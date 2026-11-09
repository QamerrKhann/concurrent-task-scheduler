package dev.qamer.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class SchedulerTest {

    private static Task ok(String id) { return new Task(id, 1, () -> id.toUpperCase(), 0); }

    @Test
    void runsDependencyGraph() throws InterruptedException {
        TaskGraph g = new TaskGraph();
        g.addTask(ok("a")); g.addTask(ok("b")); g.addTask(ok("c"));
        g.addDependency("c", "a"); g.addDependency("c", "b");
        Map<String, TaskResult> r = new Scheduler(2).run(g);
        assertEquals(TaskResult.Status.SUCCESS, r.get("c").status());
        assertEquals("C", r.get("c").output());
        assertEquals(List.of("a", "b", "c"), new ArrayList<>(r.keySet())); // insertion order
    }

    @Test
    void independentTasksReallyRunInParallel() throws InterruptedException {
        // Both tasks wait at a 2-party barrier. That only succeeds if they are
        // running at the same moment; on one thread the barrier would time out.
        CyclicBarrier barrier = new CyclicBarrier(2);
        TaskGraph g = new TaskGraph();
        for (String id : List.of("x", "y")) {
            g.addTask(new Task(id, 1, () -> { barrier.await(2, TimeUnit.SECONDS); return id; }, 0));
        }
        Map<String, TaskResult> r = new Scheduler(2).run(g);
        assertEquals(TaskResult.Status.SUCCESS, r.get("x").status());
        assertEquals(TaskResult.Status.SUCCESS, r.get("y").status());
    }

    @Test
    void neverExceedsWorkerCount() throws InterruptedException {
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        TaskGraph g = new TaskGraph();
        for (int i = 0; i < 20; i++) {
            g.addTask(new Task("t" + i, 1, () -> {
                int now = inFlight.incrementAndGet();
                peak.accumulateAndGet(now, Math::max);
                Thread.sleep(10);
                inFlight.decrementAndGet();
                return "done";
            }, 0));
        }
        new Scheduler(3).run(g);
        assertTrue(peak.get() <= 3, "peak concurrency was " + peak.get());
        assertEquals(3, peak.get(), "should use every worker when work is available");
    }

    @Test
    void higherPriorityRunsFirstAndTiesKeepInsertionOrder() throws InterruptedException {
        List<String> started = Collections.synchronizedList(new ArrayList<>());
        TaskGraph g = new TaskGraph();
        int[][] spec = {{1}, {5}, {3}, {5}};
        String[] ids = {"low", "high1", "mid", "high2"};
        for (int i = 0; i < ids.length; i++) {
            String id = ids[i];
            g.addTask(new Task(id, spec[i][0], () -> { started.add(id); return id; }, 0));
        }
        new Scheduler(1).run(g);
        assertEquals(List.of("high1", "high2", "mid", "low"), started);
    }

    @Test
    void retriesTransientFailure() throws InterruptedException {
        AtomicInteger n = new AtomicInteger();
        TaskGraph g = new TaskGraph();
        g.addTask(new Task("retry", 1, () -> {
            if (n.incrementAndGet() == 1) throw new IllegalStateException("temporary");
            return "ok";
        }, 1));
        TaskResult r = new Scheduler(1).run(g).get("retry");
        assertEquals(TaskResult.Status.SUCCESS, r.status());
        assertEquals(2, r.attempts());
    }

    @Test
    void givesUpAfterMaxRetriesWithBackoff() throws InterruptedException {
        AtomicInteger calls = new AtomicInteger();
        TaskGraph g = new TaskGraph();
        g.addTask(new Task("flaky", 1, () -> {
            calls.incrementAndGet();
            throw new IllegalStateException("always");
        }, 2, Duration.ofMillis(40)));
        TaskResult r = new Scheduler(1).run(g).get("flaky");
        assertEquals(TaskResult.Status.FAILED, r.status());
        assertEquals(3, r.attempts());
        assertEquals(3, calls.get());
        assertTrue(r.error().contains("always"));
        // Backoff waits 40ms then 80ms between the three attempts.
        assertTrue(r.duration().toMillis() >= 120, "duration " + r.duration().toMillis());
    }

    @Test
    void blocksDependentsTransitivelyButUnrelatedWorkContinues() throws InterruptedException {
        AtomicInteger childRan = new AtomicInteger();
        TaskGraph g = new TaskGraph();
        g.addTask(new Task("bad", 1, () -> { throw new IllegalStateException("boom"); }, 0));
        g.addTask(new Task("child", 1, () -> { childRan.incrementAndGet(); return "no"; }, 0));
        g.addTask(new Task("grandchild", 1, () -> { childRan.incrementAndGet(); return "no"; }, 0));
        g.addTask(ok("unrelated"));
        g.addDependency("child", "bad");
        g.addDependency("grandchild", "child");
        Map<String, TaskResult> r = new Scheduler(1).run(g);
        assertEquals(TaskResult.Status.FAILED, r.get("bad").status());
        assertEquals(TaskResult.Status.BLOCKED, r.get("child").status());
        assertEquals(TaskResult.Status.BLOCKED, r.get("grandchild").status());
        assertTrue(r.get("grandchild").error().contains("bad"), r.get("grandchild").error());
        assertEquals(TaskResult.Status.SUCCESS, r.get("unrelated").status());
        assertEquals(0, childRan.get(), "blocked tasks must never execute");
    }

    @Test
    void respectsDependenciesOnLargeRandomDag() throws InterruptedException {
        // Random DAG: edges only go from lower to higher index, so no cycles.
        Random rng = new Random(7);
        int n = 300;
        AtomicLong clock = new AtomicLong();
        Map<String, Long> startedAt = new ConcurrentHashMap<>();
        Map<String, Long> finishedAt = new ConcurrentHashMap<>();
        TaskGraph g = new TaskGraph();
        for (int i = 0; i < n; i++) {
            String id = "t" + i;
            g.addTask(new Task(id, rng.nextInt(5), () -> {
                startedAt.put(id, clock.incrementAndGet());
                if (rng.nextInt(10) == 0) Thread.sleep(1);
                finishedAt.put(id, clock.incrementAndGet());
                return id;
            }, 0));
        }
        for (int i = 1; i < n; i++) {
            for (int k = 0; k < 3; k++) {
                int dep = rng.nextInt(i);
                if (rng.nextBoolean()) g.addDependency("t" + i, "t" + dep);
            }
        }
        Map<String, TaskResult> r = new Scheduler(4).run(g);
        assertEquals(n, r.size());
        r.values().forEach(res -> assertEquals(TaskResult.Status.SUCCESS, res.status()));
        g.dependencies().forEach((task, deps) -> deps.forEach(dep ->
                assertTrue(finishedAt.get(dep) < startedAt.get(task),
                        task + " started before its prerequisite " + dep + " finished")));
    }

    @Test
    void rejectsCycles() {
        TaskGraph g = new TaskGraph();
        g.addTask(ok("a")); g.addTask(ok("b")); g.addTask(ok("c"));
        g.addDependency("a", "b"); g.addDependency("b", "a");
        IllegalStateException ex = assertThrows(IllegalStateException.class, g::topologicalOrder);
        assertTrue(ex.getMessage().contains("a") && ex.getMessage().contains("b"));
        assertThrows(IllegalStateException.class, () -> new Scheduler(1).run(g));
    }

    @Test
    void validatesGraphConstruction() {
        TaskGraph g = new TaskGraph();
        g.addTask(ok("a"));
        assertThrows(IllegalArgumentException.class, () -> g.addDependency("a", "missing"));
        assertThrows(IllegalArgumentException.class, () -> g.addDependency("a", "a"));
        assertThrows(IllegalArgumentException.class, () -> g.addTask(ok("a")));
        assertThrows(IllegalArgumentException.class, () -> new Task(" ", 1, () -> "", 0));
        assertThrows(IllegalArgumentException.class, () -> new Task("x", 1, () -> "", -1));
        assertThrows(IllegalArgumentException.class, () -> new Scheduler(0));
    }

    @Test
    void emptyGraphReturnsNoResults() throws InterruptedException {
        assertTrue(new Scheduler(2).run(new TaskGraph()).isEmpty());
    }
}
