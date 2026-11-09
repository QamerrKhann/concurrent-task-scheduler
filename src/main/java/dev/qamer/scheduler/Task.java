package dev.qamer.scheduler;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * A unit of work in the graph.
 *
 * @param id          unique name
 * @param priority    higher runs first among tasks that are ready at the same time
 * @param work        the job; throwing an exception counts as a failed attempt
 * @param maxRetries  extra attempts after the first failure (0 = run once)
 * @param retryDelay  wait before the first retry; doubles on each later retry
 */
public record Task(String id, int priority, Callable<String> work, int maxRetries, Duration retryDelay) {
    public Task {
        if (id == null || id.isBlank()) throw new IllegalArgumentException("task id required");
        Objects.requireNonNull(work, "work");
        Objects.requireNonNull(retryDelay, "retryDelay");
        if (maxRetries < 0) throw new IllegalArgumentException("maxRetries must be non-negative");
        if (retryDelay.isNegative()) throw new IllegalArgumentException("retryDelay must be non-negative");
    }

    /** Task that retries immediately, with no backoff. */
    public Task(String id, int priority, Callable<String> work, int maxRetries) {
        this(id, priority, work, maxRetries, Duration.ZERO);
    }
}
