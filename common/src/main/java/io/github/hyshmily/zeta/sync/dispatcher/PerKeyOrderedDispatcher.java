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

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.exception.ZetaExceptionHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-key FIFO ordered dispatcher.
 *
 * <p>Ensures that tasks for the same key are executed strictly in submission order
 * and never concurrently, while tasks for different keys can execute in parallel.
 *
 * <p>Designed to be used as the inner layer of a two-layer dispatch pattern where
 * an outer jitter (e.g. {@link io.github.hyshmily.zeta.util.DelayUtil#floatTimeDelay})
 * handles cross-instance staggering and this dispatcher handles same-instance ordering:
 *
 * <pre>{@code
 * DelayUtil.floatTimeDelay(
 *     () -> dispatcher.submit(key, task),
 *     jitterMs,
 *     scheduler
 * );
 * }</pre>
 *
 * <p><b>Batched consumption.</b> Tasks of the same key are consumed in batches of at most
 * {@code maxTasksPerCycle} per underlying-executor submission. A burst of same-key traffic
 * (e.g. a batch of broadcast messages) therefore occupies a single pool slot instead of one
 * submission per task. Once a key has consumed {@code maxTasksPerCycle} tasks in a row it
 * yields back to the executor so that other keys can progress — a busy key can never
 * monopolise the pool.
 *
 * <p><b>Atomic single-queue design.</b> Each key is represented by at most one {@link KeyWorker}
 * in the map. Submitting and consuming both go through a single atomic
 * {@link ConcurrentHashMap#compute(Object, java.util.function.BiFunction)} call: the worker is
 * created together with its first task (atomically deciding whether it must be started), and an
 * idle worker removes itself by returning {@code null}. Because the start decision is part of
 * the atomic insertion, there is no window in which a worker can be orphaned — eliminating the
 * running-flag re-verification and retry loop of earlier designs.
 *
 * <p><b>Backpressure:</b> Each key's queue is bounded by {@code maxQueuePerKey}.
 * When the limit is reached, excess submissions are silently counted as rejected
 * (exposed via Micrometer). In addition, the total weight of tasks pending
 * across <em>all</em> keys is bounded by {@code maxGlobalPendingUnits}: once the
 * budget is exhausted, new submissions are dropped without being enqueued
 * (ADR-0032). Submissions carry a weight — default 1 per task, larger for heavy
 * payloads (e.g. batch sync messages charge ~1 unit per KB of body). This caps
 * the dispatcher's aggregate memory under broadcast storms, where the number of
 * distinct keys is unbounded even though each key's queue is bounded.
 *
 * <p><b>Executor rejection recovery:</b> If the underlying executor rejects a task
 * via {@link RejectedExecutionException}, the tasks remain queued in their {@link KeyWorker}
 * (the batch is only granted while executing on the executor thread, so a rejected submission
 * never loses tasks), and a delayed retry re-drives the worker — self-healing without
 * depending on future submissions.
 *
 * <p>This class is thread-safe.
 */
@Slf4j
@Internal
public class PerKeyOrderedDispatcher implements AutoCloseable {

  public static final int DEFAULT_MAX_QUEUE_PER_KEY = 1024;

  /**
   * Maximum number of tasks a single key may consume before the worker yields back to the
   * executor, giving other keys a chance to run. Trades throughput (larger batches, fewer pool
   * submissions) against fairness (a busy key must eventually let other keys progress).
   */
  public static final int DEFAULT_MAX_TASKS_PER_CYCLE = 64;

  /**
   * Default aggregate budget (in weighted units) of tasks pending across all keys. A unit
   * approximates 1 KB of payload; see {@link #submitWithWeight(Object, Runnable, int)}.
   */
  public static final int DEFAULT_MAX_GLOBAL_PENDING_UNITS = 50_000;

  /** Delay before re-driving a worker whose submission was rejected by the executor. */
  private static final long REJECTION_RETRY_DELAY_MS = 200;

  /** Log only every {@code 1 << DROP_LOG_SHIFT}-th drop to keep the hot path quiet. */
  private static final int DROP_LOG_SHIFT = 10;

  private final ConcurrentHashMap<Object, KeyWorker> queues = new ConcurrentHashMap<>();
  private final ScheduledExecutorService executor;
  private final String name;
  private final int maxQueuePerKey;
  private final int maxTasksPerCycle;

  /**
   * Aggregate budget (in weighted units) of tasks pending across all keys; when exceeded,
   * new submissions are dropped (see {@link #submitWithWeight(Object, Runnable, int)}).
   */
  private final long maxGlobalPendingUnits;

  /** Total weight of tasks currently pending (enqueued or granted-but-not-yet-executed). */
  private final AtomicLong globalPendingUnits = new AtomicLong();

  /** Number of tasks dropped by the global budget gate; used for throttled WARN logging. */
  private final AtomicLong dropCounter = new AtomicLong();

  private volatile boolean closed = false;

  public PerKeyOrderedDispatcher(ScheduledExecutorService executor, String name) {
    this(executor, name, DEFAULT_MAX_QUEUE_PER_KEY, DEFAULT_MAX_TASKS_PER_CYCLE, DEFAULT_MAX_GLOBAL_PENDING_UNITS);
  }

  public PerKeyOrderedDispatcher(ScheduledExecutorService executor, String name, int maxQueuePerKey) {
    this(executor, name, maxQueuePerKey, DEFAULT_MAX_TASKS_PER_CYCLE, DEFAULT_MAX_GLOBAL_PENDING_UNITS);
  }

  public PerKeyOrderedDispatcher(
    ScheduledExecutorService executor,
    String name,
    int maxQueuePerKey,
    int maxTasksPerCycle
  ) {
    this(executor, name, maxQueuePerKey, maxTasksPerCycle, DEFAULT_MAX_GLOBAL_PENDING_UNITS);
  }

  public PerKeyOrderedDispatcher(
    ScheduledExecutorService executor,
    String name,
    int maxQueuePerKey,
    int maxTasksPerCycle,
    long maxGlobalPendingUnits
  ) {
    this.executor = executor;
    this.name = name;
    this.maxQueuePerKey = maxQueuePerKey;
    this.maxTasksPerCycle = maxTasksPerCycle;
    this.maxGlobalPendingUnits = maxGlobalPendingUnits;
  }

  /**
   * Submit a task for a given key with an optional initial delay.
   * <p>
   * If delayMs > 0, the task is scheduled to be submitted after the delay,
   * freeing the caller thread immediately. The per-key FIFO ordering is preserved:
   * even if multiple delayed submissions for the same key are scheduled, they will
   * be submitted (and thus executed) in the order their delays expire.
   *
   * @param key     the routing key
   * @param task    the task to execute
   * @param delayMs the initial delay in milliseconds; 0 means immediate submission
   */
  public void submit(Object key, Runnable task, long delayMs) {
    submitWithWeight(key, task, 1, delayMs);
  }

  /**
   * Submit a task with an explicit weight for the global pending budget.
   *
   * @param key     the routing key
   * @param task    the task to execute
   * @param weight  the task's weight in global pending units (clamped to ≥ 1);
   *                charge ~1 unit per KB of payload so the budget tracks bytes,
   *                not message count
   * @param delayMs the initial delay in milliseconds; 0 means immediate submission
   */
  public void submitWithWeight(Object key, Runnable task, int weight, long delayMs) {
    if (delayMs <= 0) {
      submitWithWeight(key, task, weight);
      return;
    }

    if (executor.isShutdown() || executor.isTerminated()) {
      // Fallback: scheduled executor is shutdown or terminated, we cannot schedule the delayed submission.
      // For simplicity, we just log a warning and submit immediately.
      log.debug("[{}] Executor is shut down, dropping delayed task for key {}", name, key);
      return;
    }
    // Use the executor's internal scheduler to delay the actual submission.
    // Note: The executor must be a ScheduledExecutorService; we assume it is.
    executor.schedule(() -> submitWithWeight(key, task, weight), delayMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Submit a task for a given key. Tasks for the same key are executed
   * in FIFO order. If the dispatcher is closed, the task is silently dropped.
   * If the key's pending queue is full, the task is rejected (dropped without execution).
   * <p>If the underlying executor rejects a task via {@link RejectedExecutionException},
   * the task remains queued and a delayed retry re-drives the worker
   * (self-healing, does not depend on future submissions).</p>
   */
  public void submit(Object key, Runnable task) {
    submitWithWeight(key, task, 1);
  }

  /**
   * Submit a task with an explicit weight for the global pending budget.
   *
   * <p><b>Global budget gate:</b> the total weight of tasks pending across all keys is
   * bounded by {@code maxGlobalPendingUnits}. When the budget is exhausted, the task is
   * dropped without being enqueued (a lost sync message — acceptable by design, see
   * ADR-0032). The gate is a <em>soft</em> bound: the check-then-act race with concurrent
   * submitters may overshoot by a bounded amount, and a single heavy task can push the
   * budget past the cap.
   *
   * @param key    the routing key
   * @param task   the task to execute
   * @param weight the task's weight in global pending units (clamped to ≥ 1); charge
   *               ~1 unit per KB of payload so the budget tracks bytes, not message count
   */
  public void submitWithWeight(Object key, Runnable task, int weight) {
    if (closed) {
      return;
    }

    if (weight < 1) {
      weight = 1;
    }
    final int effectiveWeight = weight;

    // Global budget gate: drop before enqueue so a dropped task never occupies the queue.
    if (globalPendingUnits.get() + effectiveWeight > maxGlobalPendingUnits) {
      long drops = dropCounter.incrementAndGet();
      if ((drops & ((1 << DROP_LOG_SHIFT) - 1)) == 0) {
        log.warn(
          "[{}] Global pending units exceeded ({}), dropping task for key={}, totalDrops={}",
          name,
          maxGlobalPendingUnits,
          key,
          drops
        );
      }
      return;
    }

    // Single atomic compute: creates the worker together with its first task, or enqueues into
    // the existing worker. The start decision is made atomically with the insertion, so there
    // is no window in which the worker can be orphaned (a queue whose owner never re-runs it) —
    // the running-flag re-verification and retry loop of earlier designs is not needed.
    //
    // The remapping function stays side-effect free apart from the local outcome holder:
    // CHM may invoke it more than once under bin-lock contention, and the final invocation
    // is the one whose result is committed — so the counter is adjusted exactly once per
    // submission, after compute returns.
    int[] outcome = new int[1];
    KeyWorker worker = queues.compute(key, (k, existing) -> {
      if (existing == null) {
        outcome[0] = OUTCOME_CREATED;
        return new KeyWorker(key, new PendingTask(task, effectiveWeight));
      }

      outcome[0] = existing.enqueue(task, effectiveWeight, maxQueuePerKey) ? OUTCOME_ENQUEUED : OUTCOME_REJECTED;
      return existing;
    });

    if (outcome[0] != OUTCOME_REJECTED) {
      globalPendingUnits.addAndGet(effectiveWeight);
    } else {
      log.warn("[{}] Task queue full for key {}. Task rejected.", name, key);
    }

    // Grant the first executor submission exactly once per worker incarnation.
    // The start decision is moved here via CAS: executeWorker must run outside of
    // compute (the executor call may reject or block, and the map bin lock must not
    // be held while invoking it). runCycle's continuation calls executeWorker
    // directly and is not affected by this guard.
    if (worker.scheduled.compareAndSet(false, true)) {
      executeWorker(worker);
    }
  }

  /**
   * Submits the worker to the underlying executor. The worker's next batch is only granted
   * once it runs on an executor thread, so if this submission is rejected the tasks are still
   * safely held by the worker — nothing is lost.
   * <p>If the executor rejects the submission, a delayed retry re-drives the same worker
   * (self-healing, does not depend on future submissions).</p>
   */
  private void executeWorker(KeyWorker worker) {
    try {
      executor.execute(() -> runCycle(worker));
    } catch (RejectedExecutionException e) {
      // Self-healing retry: do not rely on future submissions to re-trigger.
      try {
        executor.schedule(
          () -> {
            if (!closed) {
              executeWorker(worker);
            }
          },
          REJECTION_RETRY_DELAY_MS,
          TimeUnit.MILLISECONDS
        );
      } catch (RejectedExecutionException ignored) {
        // Executor is shutting down; close() handles cleanup.
      }
    }
  }

  /**
   * Runs one batch of tasks for a worker on the executor thread: grant the batch atomically,
   * execute every task (an exception in one task must not kill the batch), then submit the
   * worker again for the next batch — a single pool slot per {@code maxTasksPerCycle} tasks.
   */
  @SuppressWarnings("java:S1181")
  // catching Throwable is deliberate: an Error in one task must not strand
  // the remaining tasks of the key (the JDK would let it kill the worker thread)
  private void runCycle(KeyWorker worker) {
    if (closed) {
      queues.remove(worker.key, worker);
      return;
    }
    List<PendingTask> batch = grantBatch(worker);
    if (batch.isEmpty()) {
      // The worker was replaced or removed concurrently (e.g. close() cleared the map while
      // the batch was being granted); nothing to run.
      return;
    }
    for (PendingTask pending : batch) {
      if (closed) {
        // Dispatcher was closed mid-batch; remaining tasks must never run.
        queues.remove(worker.key, worker);
        return;
      }
      try {
        pending.task().run();
      } catch (Throwable t) {
        // Route through the injectable exception-handler chain (WARN log by default) and keep
        // consuming the batch — one failing task must not strand the remaining tasks of the key.
        ZetaExceptionHandler.handleException("[" + name + "] Task execution failed for key " + worker.key, t);
      }
      // Discharge the budget unit for each executed task, keeping the counter paired with
      // the actual number of pending tasks. After close() the counter was reset to 0, so
      // in-flight batches discharging here may drive it transiently negative — harmless,
      // as no further submissions are accepted once closed.
      globalPendingUnits.addAndGet(-pending.weight());
    }
    // Continuation: submit the next batch. If rejected (shutting down), executeWorker schedules
    // a retry or lets close() clean up.
    executeWorker(worker);
  }

  /**
   * Atomically grants the next batch of tasks for the worker — up to {@code maxTasksPerCycle}
   * of its first task plus queued tasks. If the worker has nothing left, it is removed from the
   * map (returning {@code null} from {@code compute}) so that a future submission creates a
   * fresh worker; if the map entry no longer belongs to this worker, the grant is skipped.
   *
   * @param worker the worker to grant tasks from
   * @return the granted batch; empty if the worker was replaced or removed concurrently
   */
  private List<PendingTask> grantBatch(KeyWorker worker) {
    List<PendingTask> batch = new ArrayList<>(maxTasksPerCycle);
    queues.compute(worker.key, (k, v) -> {
      // Only touch our own worker. If the entry was replaced or removed (e.g. by close()),
      // leave the batch empty and do not disturb the current entry.
      if (v != worker) {
        return v;
      }
      PendingTask first = worker.firstTask;
      worker.firstTask = null;
      if (first == null && (worker.queue == null || worker.queue.isEmpty())) {
        // No more tasks; the worker is idle and must be removed from the map so a future
        // submission creates a fresh worker instead of enqueueing into a dead one.
        return null;
      }
      if (first != null) {
        batch.add(first);
      }
      while (worker.queue != null && !worker.queue.isEmpty() && batch.size() < maxTasksPerCycle) {
        batch.add(worker.queue.poll());
      }
      return worker;
    });
    return batch;
  }

  @Override
  public void close() {
    closed = true;
    // Clearing the map makes every in-flight runCycle see an empty grant (or a per-task
    // closed check) and self-remove; queued tasks are dropped without executing.
    queues.clear();
    // The dropped tasks were charged to the budget; reset it. In-flight runCycle batches
    // discharging after this point may drive the counter transiently negative — harmless,
    // as no further submissions are accepted once closed.
    globalPendingUnits.set(0);
  }

  /** Submit outcome constants written by the compute remapping into the local holder. */
  private static final int OUTCOME_CREATED = 0;
  private static final int OUTCOME_ENQUEUED = 1;
  private static final int OUTCOME_REJECTED = -1;

  /**
   * Internal state of a single key. At most one worker exists per key in the map, and it is
   * the map entry itself that encodes "running": while the worker is in the map, submissions
   * enqueue into it; when it has consumed everything, it removes itself.
   *
   * <p>The first task is stored separately from the queue so that the common single-task case
   * never allocates the {@link ArrayDeque} at all.
   */
  private static final class KeyWorker {

    private static final int INITIAL_QUEUE_SIZE = 8;

    final Object key;
    /**
     * Set exactly once per worker incarnation, via CAS, by the submit path to
     * grant the first executor submission. runCycle's continuation does not
     * consult it. Reset implicitly when the worker is removed from the map and
     * a fresh worker is created.
     */
    final AtomicBoolean scheduled = new AtomicBoolean(false);
    /**
     * First task, stored separately from the queue so that the common single-task case never
     * allocates the {@link ArrayDeque} at all. Accessed only while holding the map bin lock
     * (i.e. inside {@code queues.compute(...)}) or during construction (safe publication via
     * the {@code ConcurrentHashMap}), so it deliberately needs no {@code volatile}.
     */
    PendingTask firstTask;
    /**
     * Queued tasks behind {@link #firstTask}. Only accessed while holding the map bin lock
     * (i.e. inside {@code queues.compute(...)}), so no additional synchronization is needed.
     */
    ArrayDeque<PendingTask> queue;

    KeyWorker(Object key, PendingTask firstTask) {
      this.key = key;
      this.firstTask = firstTask;
    }

    /**
     * Enqueues a task behind the current {@link #firstTask}. Must only be called while holding
     * the map bin lock (inside {@code compute}). The capacity check counts only queued tasks —
     * the task currently being executed (or waiting as {@code firstTask}) does not count
     * against {@code maxSize}.
     *
     * @param task    the task to enqueue
     * @param weight  the task's weight in global pending units
     * @param maxSize maximum allowed queue size
     * @return true if the task was enqueued, false if the queue is full
     */
    boolean enqueue(Runnable task, int weight, int maxSize) {
      if (queue == null) {
        queue = new ArrayDeque<>(INITIAL_QUEUE_SIZE);
      }
      if (queue.size() >= maxSize) {
        return false;
      }
      queue.addLast(new PendingTask(task, weight));
      return true;
    }
  }

  /**
   * A submitted task together with its weight in global pending units. The weight is charged
   * to {@link #globalPendingUnits} at submission and discharged when the task executes, so
   * the budget tracks the aggregate memory held by pending tasks.
   */
  private record PendingTask(Runnable task, int weight) {}
}
