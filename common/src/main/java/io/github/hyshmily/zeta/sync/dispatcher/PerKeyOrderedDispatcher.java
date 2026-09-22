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
import io.github.hyshmily.zeta.util.LogThrottle;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;

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
 * <p><b>Batch-end callback (Disruptor-style, ADR-0071).</b> A task submitted as a
 * {@link BatchAwareTask} receives {@code endOfBatch == true} on the last task of the granted
 * batch and {@code false} while further tasks of the same batch follow — the same hint LMAX
 * Disruptor's {@code BatchEventProcessor} gives its consumers. Expensive per-task work that a
 * later task in the same batch would repeat (a Redis load per queued REFRESH message, for
 * example) can then be amortized to the batch tail. The signal is best-effort: a task granted
 * alone always sees {@code true}, and a submission racing the batch simply starts a follow-up
 * cycle whose last task gets the signal again. Plain {@link Runnable} submissions keep the
 * zero-overhead path.
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
 * <p><b>Start jitter (park, not sleep).</b> When a positive {@code jitterMs} is configured,
 * each newly created worker draws a random delay in {@code [0, jitterMs)} and its FIRST cycle
 * is parked on the executor's scheduler for that delay instead of occupying a pool thread with
 * a {@code Thread.sleep}. The park is one random draw per worker incarnation (per key burst),
 * not per task: same-key tasks submitted while the worker is parked enqueue behind it and run
 * after it. Per-key FIFO ordering is unaffected — ordering is fixed by the atomic submission
 * into the map, and the parked worker still holds the key exclusively (the {@code scheduled}
 * flag is set, so no second cycle can start). The point is throughput: a sleeping task holds a
 * scheduler thread for the whole jitter window, so the pool's ceiling was
 * {@code poolSize / (jitterMs + taskTime)}; a parked worker releases the thread and the ceiling
 * becomes {@code poolSize / taskTime}.
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

  /**
   * Upper bound (ms) of the random start-jitter park applied to each newly created worker;
   * {@code 0} disables the park entirely (the common case — both production listeners default
   * to no jitter or delegate their own jitter property, which may itself be {@code 0}).
   */
  private final long jitterMs;

  /**
   * Total weight of tasks currently pending (enqueued or granted-but-not-yet-executed).
   * A {@link LongAdder} because the gate below is a documented <em>soft</em> bound — the
   * slightly stale {@code sum()} it reads is no worse than the check-then-act race already
   * accepted — while the add-side contention of an {@link AtomicLong} is real under
   * broadcast storms with several consumer threads.
   */
  private final LongAdder globalPendingUnits = new LongAdder();

  /** Number of tasks dropped by the global budget gate; used for throttled WARN logging. */
  private final AtomicLong dropCounter = new AtomicLong();

  /** Number of tasks rejected because the key's queue was full; used for throttled WARN logging. */
  private final AtomicLong rejectedPerKeyCounter = new AtomicLong();

  /**
   * Rate-limits the global-budget-drop WARN to one per
   * {@value LogThrottle#DEFAULT_WINDOW_MS}ms window (ADR-0037 one-per-window
   * convention; the counter above keeps the real cumulative total for the log).
   */
  private final LogThrottle globalDropLogThrottle = LogThrottle.perDefaultWindow();

  /** Rate-limits the per-key queue-full WARN to one per standard window (ADR-0037 convention). */
  private final LogThrottle queueFullLogThrottle = LogThrottle.perDefaultWindow();

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
    this(executor, name, maxQueuePerKey, maxTasksPerCycle, maxGlobalPendingUnits, 0);
  }

  public PerKeyOrderedDispatcher(
    ScheduledExecutorService executor,
    String name,
    int maxQueuePerKey,
    int maxTasksPerCycle,
    long maxGlobalPendingUnits,
    long jitterMs
  ) {
    this.executor = executor;
    this.name = name;
    this.maxQueuePerKey = maxQueuePerKey;
    this.maxTasksPerCycle = maxTasksPerCycle;
    this.maxGlobalPendingUnits = maxGlobalPendingUnits;
    this.jitterMs = Math.max(0, jitterMs);
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
    submitCore(key, task, 1, delayMs);
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
  /**
   * Submit a batch-aware task with an explicit weight for the global pending budget. Behaves
   * exactly like {@link #submitWithWeight(Object, Runnable, int, long)} except that the task
   * receives the {@code endOfBatch} hint of its key's granted batch — see
   * {@link BatchAwareTask} (ADR-0071).
   *
   * @param key     the routing key
   * @param task    the batch-aware task to execute
   * @param weight  the task's weight in global pending units (clamped to ≥ 1)
   * @param delayMs the initial delay in milliseconds; 0 means immediate submission
   */
  public void submitWithWeight(Object key, BatchAwareTask task, int weight, long delayMs) {
    submitCore(key, task, weight, delayMs);
  }

  /**
   * Submit a batch-aware task with an explicit weight (immediate, no delay) — see
   * {@link #submitWithWeight(Object, BatchAwareTask, int, long)}.
   *
   * @param key    the routing key
   * @param task   the batch-aware task to execute
   * @param weight the task's weight in global pending units (clamped to ≥ 1); charge
   *               ~1 unit per KB of payload so the budget tracks bytes, not message count
   */
  public void submitWithWeight(Object key, BatchAwareTask task, int weight) {
    submitCore(key, task, weight, 0);
  }

  @SuppressWarnings("all")
  private void submitCore(Object key, Object task, int weight, long delayMs) {
    if (delayMs <= 0) {
      submitCore(key, task, weight);
      return;
    }

    if (executor.isShutdown() || executor.isTerminated()) {
      // The scheduled executor is already shut down or terminated — the delayed
      // submission cannot be scheduled, so the task is dropped. This mirrors the
      // other shutdown drop paths (close() clears the queues, delayed submissions
      // still pending on the scheduler are dropped on arrival): the shutdown
      // window is intentionally lossy, and lost messages are bounded by the next
      // periodic broadcast / application write.
      log.debug("[{}] Executor is shut down, dropping delayed task for key {}", name, key);
      return;
    }
    // Use the executor's internal scheduler to delay the actual submission.
    // Note: The executor must be a ScheduledExecutorService; we assume it is.
    // The isShutdown pre-check above is best-effort only (TOCTOU): the executor
    // may shut down between the check and this schedule() call, which throws
    // RejectedExecutionException — catch it and drop the task so a shutdown
    // race never propagates to the caller (consistent with the drop path above).
    try {
      executor.schedule(() -> submitCore(key, task, weight), delayMs, TimeUnit.MILLISECONDS);
    } catch (RejectedExecutionException e) {
      log.debug("[{}] Executor rejected delayed task for key {} (shutting down), dropping", name, key);
    }
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
  @SuppressWarnings("all")
  public void submitWithWeight(Object key, Runnable task, int weight) {
    submitCore(key, task, weight);
  }

  /**
   * Submit a task with an explicit weight and an optional initial delay.
   *
   * @param key     the routing key
   * @param task    the task to execute
   * @param weight  the task's weight in global pending units (clamped to ≥ 1)
   * @param delayMs the initial delay in milliseconds; 0 means immediate submission
   */
  @SuppressWarnings("all")
  public void submitWithWeight(Object key, Runnable task, int weight, long delayMs) {
    submitCore(key, task, weight, delayMs);
  }

  /**
   * Core submission path shared by the {@link Runnable} and {@link BatchAwareTask} entry
   * points; {@code task} is either a {@link Runnable} (zero-overhead legacy path) or a
   * {@link BatchAwareTask} (receives the batch-end hint in {@link PendingTask#execute}).
   *
   * <p><b>Global budget gate:</b> the total weight of tasks pending across all keys is
   * bounded by {@code maxGlobalPendingUnits}. When the budget is exhausted, the task is
   * dropped without being enqueued (a lost sync message — acceptable by design, see
   * ADR-0032). The gate is a <em>soft</em> bound: the check-then-act race with concurrent
   * submitters may overshoot by a bounded amount, and a single heavy task can push the
   * budget past the cap.
   */
  @SuppressWarnings("all")
  private void submitCore(Object key, Object task, int weight) {
    if (closed) {
      return;
    }

    if (weight < 1) {
      weight = 1;
    }
    final int effectiveWeight = weight;

    // Global budget gate: drop before enqueue so a dropped task never occupies the queue.
    if (globalPendingUnits.sum() + effectiveWeight > maxGlobalPendingUnits) {
      long drops = dropCounter.incrementAndGet();
      if (globalDropLogThrottle.tryAcquire()) {
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
    // The remapping function is invoked exactly once per compute call — per the JDK contract
    // for ConcurrentHashMap.compute, the entire method invocation is performed atomically.
    // enqueue() deliberately mutates the worker's queue and submit-outcome field inside the
    // lambda in reliance on that single-invocation guarantee — a re-invocation would add the
    // task twice — so this is a documented reliance on the JDK spec, not on observed behavior.
    // The outcome is carried on the returned worker itself (a field written by this thread's
    // lambda and read by this thread after compute returns — no cross-thread publication
    // involved), so a submission allocates no per-call holder array.
    KeyWorker worker = queues.compute(key, (k, existing) -> {
      if (existing == null) {
        KeyWorker created = new KeyWorker(key, new PendingTask(task, effectiveWeight));
        created.lastSubmitOutcome = OUTCOME_CREATED;
        // Park jitter is drawn once per worker incarnation, inside the same atomic
        // insertion that fixes the key's ordering — a ThreadLocalRandom draw is
        // cheap enough for the bin lock.
        if (jitterMs > 0) {
          created.startDelayNanos = TimeUnit.MILLISECONDS.toNanos(ThreadLocalRandom.current().nextLong(jitterMs));
        }
        return created;
      }

      existing.lastSubmitOutcome = existing.enqueue(task, effectiveWeight, maxQueuePerKey)
        ? OUTCOME_ENQUEUED
        : OUTCOME_REJECTED;
      return existing;
    });

    if (worker.lastSubmitOutcome != OUTCOME_REJECTED) {
      globalPendingUnits.add(effectiveWeight);
    } else {
      // Rate-limited: one WARN per window (ADR-0037 one-per-window convention) —
      // a burst of same-key submissions must not flood the log.
      long rejected = rejectedPerKeyCounter.incrementAndGet();
      if (queueFullLogThrottle.tryAcquire()) {
        log.warn("[{}] Task queue full for key {}. Task rejected ({} total rejected).", name, key, rejected);
      }
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
   *
   * <p>If the worker still carries an unparked start-jitter (drawn at creation), the first
   * cycle is scheduled with that delay instead of running immediately — the jitter park
   * releases the pool thread for the whole delay rather than sleeping inside it. The delay is
   * consumed exactly once per worker incarnation: continuations and rejection retries observe
   * {@code 0} and submit immediately. If the scheduler rejects the park (shutting down), the
   * immediate-submission path below takes over with its own rejection retry.
   *
   * <p>If the executor rejects the submission, a delayed retry re-drives the same worker
   * (self-healing, does not depend on future submissions).</p>
   */
  private void executeWorker(KeyWorker worker) {
    long parkNanos = worker.startDelayNanos;
    if (parkNanos > 0) {
      worker.startDelayNanos = 0;
      try {
        executor.schedule(() -> runCycle(worker), parkNanos, TimeUnit.NANOSECONDS);
        return;
      } catch (RejectedExecutionException e) {
        // Shutting down — fall through to the immediate submission, whose own
        // rejection retry keeps the worker self-healing or lets close() clean up.
      }
    }
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
   * worker again for the next batch: a single pool slot per {@code maxTasksPerCycle} tasks.
   * A worker whose grant left nothing queued was removed from the map by that (empty) grant,
   * and the continuation cycle simply discovers its own absence and returns.
   *
   * <p>The <b>last</b> task of the granted batch is executed with {@code endOfBatch == true}
   * (ADR-0071): the Disruptor {@code BatchEventProcessor} batching hint. All earlier tasks of
   * the batch see {@code false}. The flag is derived from the grant's extent — a submission
   * that lands behind the running batch simply becomes the tail of a later cycle.
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
    int lastIndex = batch.size() - 1;
    for (int i = 0; i <= lastIndex; i++) {
      PendingTask pending = batch.get(i);
      if (closed) {
        // Dispatcher was closed mid-batch; remaining tasks must never run.
        queues.remove(worker.key, worker);
        return;
      }
      try {
        pending.execute(i == lastIndex);
      } catch (Throwable t) {
        // Route through the injectable exception-handler chain (WARN log by default) and keep
        // consuming the batch — one failing task must not strand the remaining tasks of the key.
        ZetaExceptionHandler.handleException("[" + name + "] Task execution failed for key " + worker.key, t);
      }
      // Discharge the budget unit for each executed task, keeping the counter paired with
      // the actual number of pending tasks. After close() the counter was reset, so
      // in-flight batches discharging here may drive it transiently negative — harmless,
      // as no further submissions are accepted once closed.
      globalPendingUnits.add(-pending.weight());
    }
    // Continuation: submit the next batch. The grant loop re-drives the worker:
    // a cycle whose grant left queued work runs the next batch; a drained worker
    // was removed from the map by that (empty) grant and the cycle simply ends —
    // the one extra executor round-trip per drained worker is the price of keeping
    // the worker in the map while its batch executes (see grantBatch).
    executeWorker(worker);
  }

  /**
   * Atomically grants the next batch of tasks for the worker — up to {@code maxTasksPerCycle}
   * of its first task plus queued tasks. The worker <b>stays in the map</b> while the granted
   * batch executes: a submission racing the batch enqueues behind it, which keeps per-key FIFO
   * ordering, the never-concurrent guarantee, and the {@code maxQueuePerKey} bound all
   * observable for tasks submitted while an earlier batch is still running. The worker is
   * removed from the map only when a grant finds nothing to run (the idle case), so a future
   * submission creates a fresh worker instead of enqueueing into a dead one; if the map entry
   * no longer belongs to this worker, the grant is skipped. Any submission racing this compute
   * either enqueues into this worker before its removal (the task is picked up by the next
   * grant or by this worker's continuation) or, after the removal, creates a fresh worker via
   * the atomic insertion — no worker in the map is ever without tasks to grant.
   *
   * @param worker the worker to grant tasks from
   * @return the granted batch; empty if the worker was idle (and removed), replaced or removed
   *         concurrently
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
        // Nothing left: the worker is idle and must be removed from the map so a
        // future submission creates a fresh worker instead of enqueueing into a
        // dead one. The batch stays empty and the running cycle returns.
        return null;
      }

      if (first != null) {
        batch.add(first);
      }
      while (worker.queue != null && !worker.queue.isEmpty() && batch.size() < maxTasksPerCycle) {
        batch.add(worker.queue.poll());
      }
      // Keep the worker in the map for the duration of the batch: removing it here
      // (at grant time) would orphan the running batch — submissions during execution
      // would create a fresh worker whose cycle runs concurrently with this batch and
      // bypasses the queue bound entirely (the fullQueue_rejectsOverflow regression).
      return worker;
    });
    return batch;
  }

  @Override
  public void close() {
    closed = true;
    // Clearing the map makes every in-flight runCycle see an empty grant (or a per-task
    // closed check) and self-remove; queued tasks are dropped without executing.
    // Delayed submissions still pending on the shared scheduler are dropped on arrival
    // (closed check in submitWithWeight) — the shutdown window is intentionally lossy;
    // lost messages are bounded by the next periodic broadcast / application write.
    queues.clear();
    // The dropped tasks were charged to the budget; reset it. In-flight runCycle batches
    // discharging after this point may drive it transiently negative — harmless,
    // as no further submissions are accepted once closed.
    globalPendingUnits.reset();
  }

  /**
   * Snapshot of the dispatcher's overload gate, backlog and drop counters (ADR-0072 D-1).
   *
   * <p>Without this surface the three drop points — the global weighted budget
   * ({@link #maxGlobalPendingUnits}), the per-key queue bound ({@link #maxQueuePerKey}) and the
   * shutdown/close window — are observable only through their throttled WARN logs, i.e. only
   * <em>after</em> the gate has closed. Exposing them lets a caller (Actuator endpoint, Micrometer
   * gauge, health indicator) tell "no traffic" apart from "gate saturated", which is exactly the
   * distinction LMAX Disruptor's {@code Sequencer#remainingCapacity()} and
   * {@code ConsumerRepository#hasBacklog} make available to its operators.
   *
   * <p>Cost is one {@link LongAdder#sum()} (the same traversal the submission gate already pays),
   * one {@link ConcurrentHashMap#size()} estimate and two counter reads — the method is intended
   * for periodic scraping, not for the submission path.
   *
   * @return an instant-indicative snapshot of the dispatcher's gate state; never {@code null}
   */
  public DispatcherStats stats() {
    return new DispatcherStats(
      globalPendingUnits.sum(),
      maxGlobalPendingUnits,
      queues.size(),
      dropCounter.get(),
      rejectedPerKeyCounter.get()
    );
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
     * Submit outcome of the most recent {@code queues.compute} on the submitting thread:
     * {@code OUTCOME_CREATED} for a fresh worker, or {@code OUTCOME_ENQUEUED}/
     * {@code OUTCOME_REJECTED} written by {@link #enqueue}. Written inside the compute lambda
     * on the submitting thread and read by that same thread right after compute returns —
     * no cross-thread publication is involved, so no {@code volatile} is needed.
     */
    int lastSubmitOutcome = OUTCOME_CREATED;
    /**
     * Start-jitter park (nanos) drawn at creation; consumed exactly once by the first
     * {@code executeWorker} call, which schedules the first cycle with this delay instead of
     * running it immediately (releasing the pool thread for the duration). Volatile because
     * the consuming write and the continuation reads happen on executor threads, not on the
     * submitting thread.
     */
    volatile long startDelayNanos;
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
     * @param task    the task to enqueue ({@link Runnable} or {@link BatchAwareTask})
     * @param weight  the task's weight in global pending units
     * @param maxSize maximum allowed queue size
     * @return true if the task was enqueued, false if the queue is full
     */
    boolean enqueue(Object task, int weight, int maxSize) {
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
   *
   * <p>{@code task} is a {@link Runnable} (the common, zero-overhead submission path) or a
   * {@link BatchAwareTask} (receives the {@code endOfBatch} hint at execution time — the flag
   * is only known when the granted batch's extent is, i.e. inside {@code runCycle}). Stored
   * as {@code Object} so a plain submission allocates exactly one {@code PendingTask} and no
   * adapter; the {@code instanceof} in {@link #execute} is a predictably-taken branch on a
   * final field.
   */
  private record PendingTask(Object task, int weight) {
    void execute(boolean endOfBatch) {
      if (task instanceof BatchAwareTask batchAware) {
        batchAware.run(endOfBatch);
      } else {
        ((Runnable) task).run();
      }
    }
  }
}
