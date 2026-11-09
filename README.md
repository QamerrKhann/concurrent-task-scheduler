# Concurrent Task Scheduler (Java 17)

An in-process scheduler for a dependency graph (DAG) of tasks. It checks the graph for cycles, runs ready tasks on a fixed pool of worker threads in priority order, retries failures with exponential backoff, and reports any task blocked by a failed prerequisite, naming the root cause.

## Build, test, run
Requires Java 17+ and Maven 3.8+.
```bash
mvn test
mvn package
java -jar target/concurrent-task-scheduler-1.0.0.jar
```

Demo output (a pipeline with a flaky fetch, a parallel branch, and a task that fails):
```
fetch     SUCCESS  attempts=2    58ms  data fetched
clean     SUCCESS  attempts=1   100ms  data cleaned
features  SUCCESS  attempts=1   150ms  features built
fit       SUCCESS  attempts=1   100ms  model fitted
upload    FAILED   attempts=1     6ms  IllegalStateException: bucket missing
notify    BLOCKED  attempts=0     0ms  blocked by failed task(s): upload
report    SUCCESS  attempts=1     0ms  report written
```

## Usage
```java
TaskGraph g = new TaskGraph();
g.addTask(new Task("fetch", 2, () -> download(), 3, Duration.ofMillis(100))); // 3 retries, 100/200/400 ms backoff
g.addTask(new Task("clean", 1, () -> clean(), 0));
g.addDependency("clean", "fetch");                     // clean waits for fetch to succeed
Map<String, TaskResult> results = new Scheduler(4).run(g);
```

## Design
- **One coordinator thread owns all scheduling state.** The thread that calls `run()` owns the ready queue, the counts of unfinished dependencies, and the results. Worker threads only run task bodies and return an immutable `Completion` through an `ExecutorCompletionService`. There is no shared mutable state, so no locks are needed, and the coordinator reacts to whichever task finishes first.
- **Kahn's algorithm, run incrementally.** A task joins the ready queue when its last prerequisite succeeds. The same algorithm in `TaskGraph.topologicalOrder()` rejects cycles before anything runs and names the tasks involved.
- **Priority:** the ready queue is a `PriorityQueue` ordered by priority (higher first). Ties go to the task that was added to the graph first, so runs are deterministic.
- **Failure handling:** when a task runs out of retries it is marked FAILED. Its dependents are never run, and each is reported as BLOCKED, with the failed task(s) traced back through the graph. Unrelated branches keep running.
- **Complexity:** O(V + E + V log V) scheduling work for V tasks and E dependencies.

## Tests (JUnit 5)
| Test | What it shows |
|---|---|
| `independentTasksReallyRunInParallel` | Two tasks meet at a `CyclicBarrier`. This only succeeds if they run at the same time. |
| `neverExceedsWorkerCount` | Peak concurrency equals the pool size and never exceeds it |
| `higherPriorityRunsFirstAndTiesKeepInsertionOrder` | The order in which ready tasks are started |
| `respectsDependenciesOnLargeRandomDag` | 300 tasks with random dependencies on 4 workers. Every task starts after all its prerequisites have finished. |
| `blocksDependentsTransitivelyButUnrelatedWorkContinues` | Blocking spreads through the graph, blocked tasks never run, and unrelated work still finishes |
| `givesUpAfterMaxRetriesWithBackoff` | The attempt count and the backoff timing |
| plus | Cycles, validation, retry success, empty graph |

## Limitations
- In-process only. There is no durable state, crash recovery, distribution, or exactly-once guarantee.
- No timeout per task. A task that hangs keeps its worker thread busy. Adding one would mean wrapping each attempt in a `Future` and calling `get(timeout)`.
- Retries happen on the worker thread, so a task sleeping in backoff occupies a worker. A refinement would be to re-queue the task with a delay, using a `ScheduledExecutorService`.
- Failure is based on exceptions only. A thrown `Error` stops the whole run.

## Interview prep
**Why not use `CompletableFuture.allOf` chains?** You could. The explicit ready queue is what makes priority ordering, a hard cap on concurrency, and accurate BLOCKED reporting easy. Chained futures start dependents as soon as they become eligible, with no global ordering.

**Where are the race conditions?** By design there are none in the scheduler: only the coordinator thread changes the scheduling state. The only things shared across threads are the task bodies, which are the user's responsibility, and the `CompletionService` queue, which is thread-safe.

**What happens on interrupt?** `run()` throws `InterruptedException`. The `finally` block calls `shutdownNow()` on the pool, which interrupts the workers.

**How would you distribute it?** Persist task state in a database or log, have workers pull work through leases with heartbeats, make tasks idempotent (since delivery is at-least-once), and move the coordinator logic behind leader election. This is roughly how Airflow and Temporal work.
