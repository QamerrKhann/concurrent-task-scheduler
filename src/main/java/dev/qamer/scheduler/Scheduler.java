package dev.qamer.scheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Runs a {@link TaskGraph} on a fixed pool of worker threads.
 *
 * <p>Design: a single coordinator thread (the caller of {@link #run}) owns all
 * scheduling state -- the ready queue, remaining-dependency counts and results --
 * so none of it needs locking. Workers only execute task bodies and hand back an
 * immutable {@code Completion} through a {@link CompletionService}. This is
 * Kahn's topological sort, run incrementally: a task enters the ready queue when
 * its last prerequisite succeeds.
 *
 * <p>Complexity: O((V + E) + V log V) scheduling work for V tasks and E edges.
 */
public final class Scheduler {
    private record Completion(String id, String output, String error, int attempts, Instant start, Instant end) {
        boolean success() { return error == null; }
    }

    private final int workers;

    public Scheduler(int workers) {
        if (workers <= 0) throw new IllegalArgumentException("workers must be positive");
        this.workers = workers;
    }

    /** Runs every task and returns one result per task, in the graph's insertion order. */
    public Map<String, TaskResult> run(TaskGraph graph) throws InterruptedException {
        graph.topologicalOrder(); // Fail fast on cycles before executing anything.
        Map<String, Task> tasks = graph.tasks();
        Map<String, Set<String>> deps = graph.dependencies();

        Map<String, Set<String>> children = new HashMap<>();
        Map<String, Integer> remaining = new HashMap<>();
        Map<String, Integer> insertion = new HashMap<>();
        int seq = 0;
        for (String id : tasks.keySet()) {
            children.put(id, new LinkedHashSet<>());
            remaining.put(id, deps.get(id).size());
            insertion.put(id, seq++);
        }
        deps.forEach((id, prerequisites) -> prerequisites.forEach(dep -> children.get(dep).add(id)));

        // Highest priority first; ties go to whichever task was added to the graph first.
        Comparator<String> order = Comparator.<String>comparingInt(id -> tasks.get(id).priority()).reversed()
                .thenComparingInt(insertion::get);
        PriorityQueue<String> ready = new PriorityQueue<>(order);
        tasks.keySet().forEach(id -> { if (remaining.get(id) == 0) ready.add(id); });

        Map<String, TaskResult> results = new HashMap<>();
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        CompletionService<Completion> completions = new ExecutorCompletionService<>(pool);
        int running = 0;
        try {
            while (true) {
                while (running < workers && !ready.isEmpty()) {
                    Task task = tasks.get(ready.remove());
                    completions.submit(() -> execute(task));
                    running++;
                }
                if (running == 0) break; // Nothing in flight and nothing ready: done.

                Completion c;
                try {
                    c = completions.take().get();
                } catch (ExecutionException ex) {
                    throw new IllegalStateException("worker crashed", ex.getCause());
                }
                running--;
                TaskResult.Status status = c.success() ? TaskResult.Status.SUCCESS : TaskResult.Status.FAILED;
                results.put(c.id(), new TaskResult(c.id(), status, c.attempts(), c.output(), c.error(),
                        Duration.between(c.start(), c.end())));
                if (c.success()) {
                    for (String child : children.get(c.id())) {
                        if (remaining.merge(child, -1, Integer::sum) == 0) ready.add(child);
                    }
                }
            }
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(5, TimeUnit.SECONDS);
        }

        // Anything that never ran sits downstream of a failure. Walk in
        // topological order so each blocked task can name its root cause.
        Map<String, String> rootCause = new HashMap<>();
        for (String id : graph.topologicalOrder()) {
            if (results.containsKey(id)) continue;
            List<String> causes = new ArrayList<>();
            for (String dep : deps.get(id)) {
                TaskResult r = results.get(dep);
                if (r != null && r.status() == TaskResult.Status.FAILED) causes.add(dep);
                else if (rootCause.containsKey(dep)) causes.add(rootCause.get(dep));
            }
            String cause = String.join(", ", new LinkedHashSet<>(causes));
            rootCause.put(id, cause);
            results.put(id, new TaskResult(id, TaskResult.Status.BLOCKED, 0, null,
                    "blocked by failed task(s): " + cause, Duration.ZERO));
        }

        Map<String, TaskResult> ordered = new LinkedHashMap<>();
        tasks.keySet().forEach(id -> ordered.put(id, results.get(id)));
        return Collections.unmodifiableMap(ordered);
    }

    /** Runs on a worker thread. Touches only its own locals and the task. */
    private Completion execute(Task task) throws InterruptedException {
        Instant start = Instant.now();
        String output = null;
        String error = null;
        int attempts = 0;
        long delayMs = task.retryDelay().toMillis();
        while (attempts <= task.maxRetries()) {
            if (attempts > 0 && delayMs > 0) {
                Thread.sleep(delayMs);  // exponential backoff between attempts
                delayMs *= 2;
            }
            attempts++;
            try {
                output = task.work().call();
                error = null;
                break;
            } catch (InterruptedException ex) {
                throw ex;
            } catch (Exception ex) {
                error = ex.getClass().getSimpleName() + ": " + ex.getMessage();
            }
        }
        return new Completion(task.id(), output, error, attempts, start, Instant.now());
    }
}
