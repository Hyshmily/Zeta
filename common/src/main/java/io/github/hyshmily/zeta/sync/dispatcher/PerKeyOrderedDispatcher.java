/*
 * Copyright 2026 Hyshmily. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.hyshmily.zeta.sync.dispatcher;

import io.github.hyshmily.zeta.exception.ZetaExceptionHandler;
import io.github.hyshmily.zeta.util.Internal;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

/**
 * Per-key FIFO dispatcher that hands each granted batch to one consumer.
 *
 * <p>Tasks of the same key run strictly in submission order and never concurrently; tasks
 * of different keys run in parallel. A key yields the pool after
 * {@code maxTasksPerCycle} tasks, so a busy key cannot monopolise it.
 *
 * <p><b>The batch is the unit of work.</b> The consumer receives the whole granted list,
 * so work a later task of the same batch would repeat — a loader call per queued REFRESH,
 * a table sweep per queued clear, an overwritten value applied per queued PUT — is done
 * once, and there is no cross-task state to smuggle through a context object. The batch
 * boundary is best effort: a task granted alone is a batch of one, and a submission racing
 * the batch simply starts a follow-up cycle.
 *
 * <p><b>Atomic per-key queue.</b> Each key is at most one {@link KeyWorker} in the map.
 * Submitting and draining both run through one atomic {@code compute}: the worker is
 * created together with its first task (deciding atomically whether it must be started),
 * and an idle worker removes itself. No worker can be left orphaned.
 *
 * <p><b>Backpressure.</b> Each key's queue is bounded by {@code maxQueuePerKey}; the total
 * weight pending across all keys is bounded by {@code maxGlobalPendingUnits} (one unit ≈
 * 1 KB of payload). Control-plane submissions — a COOL decision, a clear — are O(1) and
 * skip the global budget: a value storm must not lock out the message that repairs the
 * state it created.
 *
 * <p><b>Start jitter.</b> A new worker's first cycle is parked on the executor's scheduler
 * for a random delay instead of sleeping inside a pool thread, spreading same-instant
 * loads (a broadcast fan-out) without shrinking throughput.
 *
 * <p>Thread-safe.
 */
@Slf4j
@Internal
public class PerKeyOrderedDispatcher<T> implements AutoCloseable {

  public static final int DEFAULT_MAX_GLOBAL_PENDING_UNITS = 50_000;
  public static final int DEFAULT_MAX_QUEUE_PER_KEY = 1024;
  public static final int DEFAULT_MAX_TASKS_PER_CYCLE = 64;

  private final ConcurrentHashMap<Object, KeyWorker> queues = new ConcurrentHashMap<>();
  /**
   * Aggregate weight pending across all keys. {@link LongAdder} rather than
   * {@code AtomicLong}: every submit increments it, and a striped adder keeps concurrent
   * submitters off one cache line. Reads are weak-consistent, which is acceptable for a
   * soft budget (see {@link #submit}).
   */
  private final LongAdder globalPendingUnits = new LongAdder();
  private final ScheduledExecutorService executor;
  private final Consumer<List<T>> consumer;
  private final long maxGlobalPendingUnits;
  private final int maxTasksPerCycle;
  private final int maxQueuePerKey;
  private final long jitterMs;
  private final String name;

  private volatile boolean closed = false;

  /**
   * @param executor              executor for the per-key cycles and the start-jitter park
   * @param name                  dispatcher name used in log lines
   * @param consumer              runs one granted batch to completion
   * @param maxQueuePerKey        per-key pending-queue bound
   * @param maxTasksPerCycle      tasks consumed per executor submission
   * @param maxGlobalPendingUnits aggregate weighted-unit budget across all keys
   * @param jitterMs              upper bound of the random first-cycle park; {@code <= 0} disables it
   */
  public PerKeyOrderedDispatcher(
    ScheduledExecutorService executor,
    String name,
    Consumer<List<T>> consumer,
    int maxQueuePerKey,
    int maxTasksPerCycle,
    long maxGlobalPendingUnits,
    long jitterMs
  ) {
    this.name = name;
    this.executor = executor;
    this.consumer = consumer;
    this.maxQueuePerKey = maxQueuePerKey;
    this.jitterMs = Math.max(0, jitterMs);
    this.maxTasksPerCycle = maxTasksPerCycle;
    this.maxGlobalPendingUnits = maxGlobalPendingUnits;
  }

  /**
   * Submit a task under its ordering key.
   *
   * @param key    the routing key (same key ⇒ FIFO)
   * @param task   the task to enqueue
   * @param weight its weight in global pending units (clamped to ≥ 1)
   * @param jitter {@code true} draws the start-jitter park when the key has no worker yet;
   *               pass {@code false} for tasks that perform no I/O
   * @return {@code true} when the task was enqueued
   */
  public boolean submit(Object key, T task, int weight, boolean jitter) {
    return submit(key, task, weight, jitter, false);
  }

  /**
   * Submit a control-plane task: same ordering and per-key bound, but exempt from the
   * global memory budget (a clear or a COOL is O(1) and must survive a payload storm).
   *
   * @param key  the routing key
   * @param task the control task
   * @return {@code true} when the task was enqueued
   */
  public boolean submitControl(Object key, T task) {
    return submit(key, task, 1, false, true);
  }

  private boolean submit(Object key, T task, int weight, boolean jitter, boolean control) {
    if (closed) {
      return false;
    }

    // Weight floors at 1: a zero/negative caller must not create a task that occupies a
    // queue slot but consumes no global budget, which would let a misbehaving caller grow
    // the queue without ever tripping the global backpressure check.
    int effectiveWeight = Math.max(1, weight);
    if (!control && globalPendingUnits.sum() + effectiveWeight > maxGlobalPendingUnits) {
      log.debug("[{}] Global pending units exceeded ({}), dropping task for key={}", name, maxGlobalPendingUnits, key);
      return false;
    }

    // One atomic compute decides both "does this key already have a worker" and "is the
    // task accepted", so a worker can never end up in the map without a task to grant.
    // The bin lock also serializes firstTask/queue mutation: KeyWorker is not thread-safe
    // and relies on this lock for every enqueue. The outcome travels in a one-element
    // array because compute has no second return value: 0 = refused, 1 = created the
    // worker, 2 = enqueued behind an existing one.
    int[] outcome = new int[1];
    KeyWorker worker = queues.compute(key, (k, existing) -> {
      // A close() that raced the entry check must not leave a worker created after
      // queues.clear(): re-read the flag under the bin lock.
      if (closed) {
        return existing;
      }

      if (existing == null) {
        outcome[0] = 1;
        // The first task is stored inline (task + positive weight): the common single-task
        // burst allocates neither a PendingTask carrier nor a deque.
        return new KeyWorker(key, task, effectiveWeight);
      }

      if (!existing.enqueue(task, effectiveWeight)) {
        return existing;
      }
      outcome[0] = 2;
      return existing;
    });

    if (outcome[0] == 0) {
      // A close() during the compute is shutdown, not backpressure; only a live dispatcher
      // reports the per-key bound.
      if (!closed) {
        log.debug("[{}] Task queue full for key {}. Task rejected.", name, key);
      }
      return false;
    }

    // close() may have cleared the map after admission. A worker this call created never
    // started, so refusing it is accurate and the caller may take its local fallback. A task
    // appended to an existing worker may already have been granted and executed, so the false
    // return would make the caller replay it as an INVALIDATE and erase the value it just
    // applied; shutdown is lossy, and this call reports acceptance.
    if (closed) {
      if (outcome[0] == 1) {
        queues.remove(key, worker);
        return false;
      }
      return true;
    }

    // LongAdder mutation is not transactional: adjust the budget only after the compute
    // has committed the decision.
    globalPendingUnits.add(effectiveWeight);

    // The creator of the worker grants the first cycle; every later cycle is re-driven by
    // runCycle after it settles.
    if (outcome[0] == 1) {
      startWorker(worker, jitter);
    }
    return true;
  }

  /**
   * Schedule a new worker's first cycle, applying the optional start-jitter park. The random
   * delay is drawn here, outside the map's bin lock, so a submission never holds the bucket
   * lock across the generator. A rejected park falls through to an immediate submission,
   * whose own rejection path retires the worker.
   *
   * @param worker the newly created worker
   * @param jitter whether this submission opted into the start-jitter park
   */
  private void startWorker(KeyWorker worker, boolean jitter) {
    if (jitter && jitterMs > 0L) {
      long delayMs = ThreadLocalRandom.current().nextLong(jitterMs);
      if (delayMs > 0L) {
        try {
          executor.schedule(worker, delayMs, TimeUnit.MILLISECONDS);
          return;
        } catch (RejectedExecutionException ignored) {
          // Fall through to immediate execution, whose rejection path retires the worker.
        }
      }
    }
    scheduleWorker(worker);
  }

  /** Submit the worker's next cycle; a rejection retires it and releases its budget. */
  private void scheduleWorker(KeyWorker worker) {
    try {
      executor.execute(worker);
    } catch (RejectedExecutionException e) {
      abandon(worker);
    }
  }

  /**
   * Remove a worker the executor will not run and discharge the weight its dropped tasks
   * held — the last-resort cleanup when a shut-down executor rejects the re-drive.
   */
  private void abandon(KeyWorker worker) {
    // The worker is retiring, so no other thread observes its grant field: reuse it to carry
    // the released-units total out of the compute.
    worker.grantedUnits = 0L;
    queues.compute(worker.key, (k, current) -> {
      if (current != worker) {
        return current;
      }

      long units = worker.firstWeight;
      if (worker.queue != null) {
        for (PendingTask<T> pending : worker.queue) {
          units += pending.weight;
        }
      }
      worker.grantedUnits = units;
      return null;
    });
    long released = worker.grantedUnits;
    if (released > 0L) {
      globalPendingUnits.add(-released);
    }
    log.warn(
      "[{}] Executor refused every submission for key={}; dropped {} pending unit(s) so the key can be rescheduled",
      name,
      worker.key,
      released
    );
  }

  /**
   * Grant one batch for the worker and hand it to the consumer, then either resubmit the
   * worker for the next batch or retire it. A consumer failure is reported through the
   * exception-handler chain and never kills the worker thread.
   */
  private void runCycle(KeyWorker worker) {
    // Fast-exit for a worker that was already removed (close() clears the map) — its
    // granted tasks (none yet) and budget (already reset by close()) need no settlement.
    if (closed) {
      queues.remove(worker.key, worker);
      return;
    }

    List<T> tasks = grantBatch(worker);
    if (tasks == null) {
      return; // replaced or removed concurrently
    }

    // grantBatch published the batch weight on the worker under the bin lock; this thread
    // reads it immediately after the compute returns, on the same thread that took the
    // lock. Clearing it here keeps the field clear for the next grant.
    long batchUnits = worker.grantedUnits;
    worker.grantedUnits = 0L;
    try {
      // Second closed check: close() may have run between grantBatch and here. The
      // granted tasks were taken out of the queue and must NOT run after close; the
      // finally below still settles their weight so the counter does not drift.
      if (closed) {
        queues.remove(worker.key, worker);
        return;
      }

      consumer.accept(tasks);
    } catch (Throwable t) {
      ZetaExceptionHandler.handleException("[" + name + "] Batch execution failed for key " + worker.key, t);
    } finally {
      // Settlement must run even when the consumer throws: a leaked batch weight
      // permanently consumes global budget until the dispatcher is closed.
      globalPendingUnits.add(-batchUnits);
    }

    // compute's return value answers "still has work": this worker means yes, anything
    // else (a replaced worker, or null from the retirement) means no.
    boolean pending =
      queues.compute(worker.key, (k, current) -> {
        if (current != worker) {
          return current;
        }

        if (worker.firstTask != null || (worker.queue != null && !worker.queue.isEmpty())) {
          return worker;
        }
        return null;
      }) ==
      worker;
    if (pending) {
      scheduleWorker(worker);
    }
  }

  /**
   * Atomically take up to {@code maxTasksPerCycle} tasks from the worker and unwrap them
   * into the consumer's batch. The worker stays in the map while the batch runs, so a
   * submission racing the batch enqueues behind it — which preserves FIFO order and the
   * per-key bound; it is removed only when a grant finds nothing to run.
   *
   * <p>A single-task grant uses an immutable one-element list: the common burst needs no
   * queue and no second list to unwrap into.
   *
   * @return the granted tasks, or {@code null} when the worker was idle (and removed) or
   *         replaced
   */
  private List<T> grantBatch(KeyWorker worker) {
    boolean granted =
      queues.compute(worker.key, (k, current) -> {
        if (current != worker) {
          return current;
        }

        T first = worker.firstTask;
        int firstWeight = worker.firstWeight;
        worker.firstTask = null;
        worker.firstWeight = 0;
        boolean queueEmpty = worker.queue == null || worker.queue.isEmpty();
        if (first == null && queueEmpty) {
          return null; // idle: remove so a later submission starts a fresh worker
        }

        if (first != null && queueEmpty) {
          // Common case: exactly one task pending. Skip the ArrayList and the poll loop;
          // List.of(first) is an immutable singleton with no defensive copy.
          worker.grantedTasks = List.of(first);
          worker.grantedUnits = firstWeight;
          return worker;
        }

        int queueSize = worker.queue.size();
        List<T> tasks = new ArrayList<>(Math.min(maxTasksPerCycle, queueSize + 1));
        long units = 0L;
        if (first != null) {
          tasks.add(first);
          units += firstWeight;
        }

        while (!worker.queue.isEmpty() && tasks.size() < maxTasksPerCycle) {
          PendingTask<T> next = worker.queue.poll();
          tasks.add(next.task);
          units += next.weight;
        }
        // Publish the grant on the worker: runCycle reads it immediately after this
        // compute returns, on the same thread that took the bin lock.
        worker.grantedTasks = tasks;
        worker.grantedUnits = units;
        return worker;
      }) ==
      worker;
    return granted ? worker.grantedTasks : null;
  }

  /**
   * Stop accepting submissions and drop every queued task. In-flight batches finish on
   * their own thread; the loss is bounded by the next periodic broadcast or write.
   */
  @Override
  public void close() {
    closed = true;
    // Clearing the map drops every queued task: no new worker can be created (closed is
    // true) and any in-flight runCycle that reaches grantBatch finds current != worker
    // (the map entry is gone) and returns null. globalPendingUnits is reset because the
    // tasks that held the weight are gone with the map.
    queues.clear();
    globalPendingUnits.reset();
  }

  /**
   * One key's queue. The first task and its weight are stored inline, so the common
   * single-task burst allocates neither a {@link PendingTask} carrier nor an
   * {@link ArrayDeque}. {@code firstTask}/{@code firstWeight}/{@code queue} are only
   * touched under the map's bin lock (inside {@code compute}) or during construction.
   *
   * <p>Runs its own cycle so a submission never allocates a {@code Runnable} wrapper.
   */
  private final class KeyWorker implements Runnable {

    private static final int INITIAL_QUEUE_SIZE = 8;

    final Object key;

    ArrayDeque<PendingTask<T>> queue;
    /**
     * The weight of the batch granted by the cycle currently running. Written under the bin
     * lock by {@code grantBatch}, read once by {@code runCycle} on the same thread, and
     * cleared before the next schedule; a retired worker reuses it to carry the units it
     * discharges.
     */
    long grantedUnits;
    /**
     * Tasks granted by the cycle currently running, unwrapped from their
     * {@link PendingTask} carriers. Written under the bin lock by {@code grantBatch}
     * and read once by the same cycle; the worker is rescheduled only after the
     * batch settles, so no other cycle can overwrite them meanwhile.
     */
    List<T> grantedTasks;
    int firstWeight;
    T firstTask;

    KeyWorker(Object key, T firstTask, int firstWeight) {
      this.key = key;
      this.firstTask = firstTask;
      this.firstWeight = firstWeight;
    }

    @Override
    public void run() {
      runCycle(this);
    }

    boolean enqueue(T task, int weight) {
      if (queue == null) {
        queue = new ArrayDeque<>(INITIAL_QUEUE_SIZE);
      }

      // firstTask is pending until the first grant; counting only the deque would admit
      // maxQueuePerKey + 1 tasks while the start-jitter park holds that first task back.
      int pendingCount = queue.size() + (firstTask != null ? 1 : 0);
      if (pendingCount >= maxQueuePerKey) {
        return false;
      }
      queue.addLast(new PendingTask<>(task, weight));
      return true;
    }
  }

  /** A queued task with its positive weight. */
  private record PendingTask<T>(T task, int weight) {}
}
