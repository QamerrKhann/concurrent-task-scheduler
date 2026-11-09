package dev.qamer.scheduler;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;

/** A directed acyclic graph of tasks. Not thread-safe: build it, then hand it to a {@link Scheduler}. */
public final class TaskGraph {
    private final Map<String, Task> tasks = new LinkedHashMap<>();
    private final Map<String, Set<String>> dependencies = new LinkedHashMap<>();

    public void addTask(Task task) {
        Objects.requireNonNull(task);
        if (tasks.putIfAbsent(task.id(), task) != null)
            throw new IllegalArgumentException("duplicate task: " + task.id());
        dependencies.put(task.id(), new LinkedHashSet<>());
    }

    /** Declares that {@code taskId} may only start after {@code prerequisiteId} succeeds. */
    public void addDependency(String taskId, String prerequisiteId) {
        require(taskId);
        require(prerequisiteId);
        if (taskId.equals(prerequisiteId)) throw new IllegalArgumentException("self dependency: " + taskId);
        dependencies.get(taskId).add(prerequisiteId);
    }

    public Map<String, Task> tasks() { return Collections.unmodifiableMap(tasks); }

    public Map<String, Set<String>> dependencies() {
        Map<String, Set<String>> copy = new LinkedHashMap<>();
        dependencies.forEach((k, v) -> copy.put(k, Collections.unmodifiableSet(new LinkedHashSet<>(v))));
        return Collections.unmodifiableMap(copy);
    }

    /**
     * Kahn's algorithm, O(V + E). Throws if the graph has a cycle, naming the
     * tasks that could not be ordered (those on, or downstream of, a cycle).
     */
    public List<String> topologicalOrder() {
        Map<String, Integer> degree = new LinkedHashMap<>();
        Map<String, List<String>> next = new HashMap<>();
        tasks.keySet().forEach(id -> {
            degree.put(id, dependencies.get(id).size());
            next.put(id, new ArrayList<>());
        });
        dependencies.forEach((task, deps) -> deps.forEach(dep -> next.get(dep).add(task)));
        Queue<String> q = new ArrayDeque<>();
        degree.forEach((id, d) -> { if (d == 0) q.add(id); });
        List<String> order = new ArrayList<>();
        while (!q.isEmpty()) {
            String id = q.remove();
            order.add(id);
            for (String child : next.get(id)) if (degree.merge(child, -1, Integer::sum) == 0) q.add(child);
        }
        if (order.size() != tasks.size()) {
            List<String> stuck = new ArrayList<>(tasks.keySet());
            stuck.removeAll(order);
            throw new IllegalStateException("dependency graph contains a cycle among: " + stuck);
        }
        return order;
    }

    private void require(String id) {
        if (!tasks.containsKey(id)) throw new IllegalArgumentException("unknown task: " + id);
    }
}
