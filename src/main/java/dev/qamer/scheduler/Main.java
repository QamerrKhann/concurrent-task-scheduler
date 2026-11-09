package dev.qamer.scheduler;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Demo: a small data pipeline with a flaky step, a parallel branch and a failure. */
public final class Main {
    public static void main(String[] args) throws InterruptedException {
        AtomicInteger fetchCalls = new AtomicInteger();
        TaskGraph g = new TaskGraph();
        g.addTask(new Task("fetch", 2, () -> {
            if (fetchCalls.incrementAndGet() == 1) throw new IllegalStateException("network blip");
            return "data fetched";
        }, 2, Duration.ofMillis(50)));
        g.addTask(new Task("clean", 3, () -> work("data cleaned", 100), 0));
        g.addTask(new Task("features", 2, () -> work("features built", 150), 0));
        g.addTask(new Task("fit", 5, () -> work("model fitted", 100), 0));
        g.addTask(new Task("upload", 1, () -> { throw new IllegalStateException("bucket missing"); }, 0));
        g.addTask(new Task("notify", 1, () -> "sent", 0));
        g.addTask(new Task("report", 1, () -> "report written", 0));

        g.addDependency("clean", "fetch");
        g.addDependency("features", "fetch");  // clean and features run in parallel
        g.addDependency("fit", "clean");
        g.addDependency("fit", "features");
        g.addDependency("report", "fit");
        g.addDependency("notify", "upload");   // blocked: upload always fails

        Map<String, TaskResult> result = new Scheduler(2).run(g);
        result.values().forEach(r -> System.out.printf("%-9s %-8s attempts=%d %5dms  %s%n",
                r.taskId(), r.status(), r.attempts(), r.duration().toMillis(),
                r.output() != null ? r.output() : r.error()));
    }

    private static String work(String out, long millis) throws InterruptedException {
        Thread.sleep(millis);
        return out;
    }
}
