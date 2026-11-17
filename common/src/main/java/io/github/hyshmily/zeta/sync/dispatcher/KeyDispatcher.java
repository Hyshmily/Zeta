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
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * Per-key FIFO dispatcher that hands each granted batch to one consumer.
 *
 * <p>Tasks of the same key run strictly in submission order and never concurrently; tasks of
 * different keys run in parallel. A key yields the pool after {@code maxTasksPerCycle} tasks,
 * so a busy key cannot monopolise it. The consumer receives the whole granted batch, so work
 * a later task would repeat — a loader call, a table sweep, an overwritten value — is done
 * once; the boundary is best effort, and a submission racing a batch starts a follow-up cycle.
 *
 * <p>Each key has at most one {@link KeyWorker} in the map, created with its first task already
 * granted; later batches are drained and resubmitted inside the retire compute. The executor
 * must not run a submitted command inline: no {@link ThreadPoolExecutor.CallerRunsPolicy} and
 * no direct executor, because both submission points hold the key's bin lock and an inline run
 * would re-enter that key's {@code compute}.
 *
 * <p>See design decisions: buffering and ordering.
 *
 * <p>Thread-safe.
 */
@Slf4j
@Internal
public class KeyDispatcher<T> implements AutoCloseable {

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
   * Creates a dispatcher with the default per-key queue bound and per-cycle task cap.
   *
   * @param executor              executor for the per-key cycles and the start-jitter park
   * @param name                  dispatcher name used in log lines
   * @param consumer              runs one granted batch to completion
   * @param maxGlobalPendingUnits aggregate weighted-unit budget across all keys
   * @param jitterMs              upper bound of the random first-cycle park; {@code <= 0} disables it
   */
  public KeyDispatcher(
    ScheduledExecutorService executor,
    String name,
    Consumer<List<T>> consumer,
    long maxGlobalPendingUnits,
    long jitterMs
  ) {
    this(
      executor,
      name,
      consumer,
      DEFAULT_MAX_QUEUE_PER_KEY,
      DEFAULT_MAX_TASKS_PER_CYCLE,
      maxGlobalPendingUnits,
      jitterMs
    );
  }

  /**
   * @param executor              executor for the per-key cycles and the start-jitter park
   * @param name                  dispatcher name used in log lines
   * @param consumer              runs one granted batch to completion
   * @param maxQueuePerKey        per-key pending-queue bound; must be {@code >= 1}
   * @param maxTasksPerCycle      tasks consumed per executor submission; must be {@code >= 1}
   * @param maxGlobalPendingUnits aggregate weighted-unit budget across all keys; must be {@code >= 1}
   * @param jitterMs              upper bound of the random first-cycle park; {@code <= 0} disables it
   * @throws IllegalArgumentException when a per-key queue bound, per-cycle task cap or
   *                                  global pending budget is below one
   * @throws NullPointerException     when {@code executor}, {@code name} or {@code consumer} is null
   */
  public KeyDispatcher(
    ScheduledExecutorService executor,
    String name,
    Consumer<List<T>> consumer,
    int maxQueuePerKey,
    int maxTasksPerCycle,
    long maxGlobalPendingUnits,
    long jitterMs
  ) {
    this.name = Objects.requireNonNull(name, "name must not be null");
    this.executor = Objects.requireNonNull(executor, "executor must not be null");
    this.consumer = Objects.requireNonNull(consumer, "consumer must not be null");
    if (maxQueuePerKey < 1) {
      throw new IllegalArgumentException("maxQueuePerKey must be >= 1, got " + maxQueuePerKey);
    }
    if (maxTasksPerCycle < 1) {
      throw new IllegalArgumentException("maxTasksPerCycle must be >= 1, got " + maxTasksPerCycle);
    }
    if (maxGlobalPendingUnits < 1) {
      throw new IllegalArgumentException("maxGlobalPendingUnits must be >= 1, got " + maxGlobalPendingUnits);
    }
    // A cancelled park must leave the delay queue at once so it cannot pin its payload.
    if (executor instanceof ScheduledThreadPoolExecutor scheduled) {
      scheduled.setRemoveOnCancelPolicy(true);
    }
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
   * @return {@code true} when the task was accepted; {@code false} when the dispatcher is
   *         closed, a bound refused it, or the executor refused the first cycle, so the caller
   *         may apply its local fallback
   * @throws NullPointerException when {@code key} or {@code task} is null
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
   * @return {@code true} when the task was accepted; {@code false} when the dispatcher is
   *         closed, the per-key bound refused it, or the executor refused the first cycle
   * @throws NullPointerException when {@code key} or {@code task} is null
   */
  public boolean submitControl(Object key, T task) {
    return submit(key, task, 1, false, true);
  }

  @SuppressWarnings("all")
  private boolean submit(Object key, T task, int weight, boolean jitter, boolean control) {
    Objects.requireNonNull(key, "key must not be null");
    Objects.requireNonNull(task, "task must not be null");
    if (closed) {
      return false;
    }

    int effectiveWeight = Math.max(1, weight);
    // Subtraction form: sum() + weight could overflow near Long.MAX_VALUE.
    if (!control && globalPendingUnits.sum() > maxGlobalPendingUnits - effectiveWeight) {
      log.debug("[{}] Global pending units exceeded ({}), dropping task for key={}", name, maxGlobalPendingUnits, key);
      return false;
    }

    int[] outcome = new int[1];
    boolean[] queueFull = new boolean[1];
    Object[] createdWorker = new Object[1];
    queues.compute(key, (k, existing) -> {
      // close() may have cleared the map after the entry check above.
      if (closed) {
        return existing;
      }

      if (existing == null) {
        KeyWorker created = new KeyWorker(key, task, effectiveWeight);
        globalPendingUnits.add(effectiveWeight);
        boolean started;
        try {
          started = startWorker(created, jitter);
        } catch (RuntimeException | Error failure) {
          globalPendingUnits.add(-effectiveWeight);
          throw failure;
        }
        if (!started) {
          globalPendingUnits.add(-effectiveWeight);
          return null;
        }
        createdWorker[0] = created;
        outcome[0] = 1;
        return created;
      }

      if (!existing.enqueue(task, effectiveWeight)) {
        queueFull[0] = true;
        return existing;
      }

      globalPendingUnits.add(effectiveWeight);
      outcome[0] = 2;
      return existing;
    });

    if (outcome[0] == 1 && closed) {
      KeyWorker created = (KeyWorker) createdWorker[0];
      queues.remove(key, created);
      created.cancelJitter();
      outcome[0] = 0;
    }

    if (outcome[0] == 0) {
      if (!closed && queueFull[0]) {
        log.debug("[{}] Task queue full for key {}. Task rejected.", name, key);
      }
      return false;
    }
    return true;
  }

  /**
   * Schedules a new worker's first cycle, applying the optional start-jitter park. Runs under
   * the key's bin lock, so the worker is published only after the executor accepted the cycle;
   * the lock is held across a thread-local delay draw and a scheduler offer, neither of which
   * performs I/O.
   *
   * @param worker the newly created worker
   * @param jitter whether this submission opted into the start-jitter park
   * @return {@code true} when the cycle was accepted; {@code false} when the dispatcher is
   *         closed (including a close that landed while the executor accepted the cycle) or
   *         the executor refused both the park and the immediate submission
   */
  private boolean startWorker(KeyWorker worker, boolean jitter) {
    if (closed) {
      return false;
    }
    if (jitter && jitterMs > 0L) {
      long delayMs = ThreadLocalRandom.current().nextLong(jitterMs);
      if (delayMs > 0L) {
        try {
          ScheduledFuture<?> future = executor.schedule(worker, delayMs, TimeUnit.MILLISECONDS);
          worker.jitterFuture = future;
          if (closed) {
            future.cancel(false);
            return false;
          }
          return true;
        } catch (RejectedExecutionException ignored) {}
      }
    }
    if (closed) {
      return false;
    }
    try {
      executor.execute(worker);
      return !closed;
    } catch (RejectedExecutionException ignored) {
      return false;
    }
  }

  /**
   * Submits a worker's next cycle from inside its retire compute, under the same bin lock that
   * publishes the drained grant.
   *
   * @param worker the worker to drive
   * @return {@code true} when the executor accepted the cycle; {@code false} when rejected
   */
  private boolean submitCycle(KeyWorker worker) {
    try {
      executor.execute(worker);
      return true;
    } catch (RejectedExecutionException ignored) {
      return false;
    }
  }

  /**
   * Executes one pre-granted batch, then either drains and resubmits the worker for the next
   * batch or retires it. A consumer failure is reported through the exception-handler chain;
   * a {@link RuntimeException} never kills the worker thread, while a fatal {@link Error}
   * retires the key and then propagates.
   */
  @SuppressWarnings({ "unchecked", "java:S1181", "java:S3776" })
  private void run(KeyWorker worker) {
    worker.jitterFuture = null;
    Grant<T> grant = worker.grant;
    if (closed || grant == null) {
      worker.grant = null;
      queues.remove(worker.key, worker);
      return;
    }

    // A burst that arrived while the start-jitter park held the first task is folded into
    // that first batch under the bin lock. A lone first task pays no compute; a burst
    // coalesces into one consumer call, one sweep, one loader read.
    if (worker.first && worker.hasQueued) {
      Grant<T>[] merged = new Grant[1];
      KeyWorker kept = queues.compute(worker.key, (k, current) -> {
        if (current != worker) {
          return current;
        }
        if (closed) {
          return null;
        }
        merged[0] = worker.mergeFirst(maxTasksPerCycle);
        return worker;
      });
      if (kept != worker) {
        return;
      }
      if (merged[0] != null) {
        grant = merged[0];
      }
    }
    worker.grant = null;
    worker.first = false;

    try {
      if (closed) {
        queues.remove(worker.key, worker);
        return;
      }
      consumer.accept(grant.tasks());
    } catch (Throwable t) {
      restoreInterrupt(t);
      ZetaExceptionHandler.handleException("[" + name + "] Batch execution failed for key " + worker.key, t);
      if (t instanceof Error error) {
        retire(worker);
        throw error;
      }
    } finally {
      if (!closed) {
        globalPendingUnits.add(-grant.units());
      }
    }

    // One compute at the end: empty queue retires the worker; a non-empty queue drains the
    // next batch under the same bin lock and resubmits. Submitting inside the compute keeps
    // a concurrent submit from appending to a worker whose re-drive the executor refused.
    long[] dropped = { 0L };
    boolean[] refused = { false };
    queues.compute(worker.key, (k, current) -> {
      if (current != worker) {
        return current;
      }
      if (closed) {
        return null;
      }
      if (!worker.drainNext(maxTasksPerCycle)) {
        return null;
      }
      if (submitCycle(worker)) {
        return worker;
      }
      refused[0] = true;
      dropped[0] = worker.discard();
      return null;
    });
    if (refused[0]) {
      if (!closed && dropped[0] > 0L) {
        globalPendingUnits.add(-dropped[0]);
      }
      log.warn(
        "[{}] Executor refused the next cycle for key={}; dropped {} pending unit(s) so the key can be rescheduled",
        name,
        worker.key,
        dropped[0]
      );
    }
  }

  /**
   * Removes a worker that will not be driven again and discharges the weight of the tasks it
   * still holds. The grant already taken by the current cycle is settled by its own finally.
   *
   * @param worker the worker to retire
   */
  private void retire(KeyWorker worker) {
    long[] released = { 0L };
    queues.compute(worker.key, (k, current) -> {
      if (current != worker) {
        return current;
      }
      released[0] = worker.discard();
      return null;
    });
    if (released[0] > 0L && !closed) {
      globalPendingUnits.add(-released[0]);
    }
  }

  /**
   * Restores the interrupt flag when the failure chain carries an interrupt. A
   * {@link Consumer} cannot declare a checked exception, so the interrupt normally arrives
   * wrapped; the bounded walk covers both shapes without looping on a cyclic cause chain.
   *
   * @param failure the consumer failure
   */
  private static void restoreInterrupt(Throwable failure) {
    Throwable cause = failure;
    for (int depth = 0; cause != null && depth < 8; depth++, cause = cause.getCause()) {
      if (cause instanceof InterruptedException) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  /**
   * Stops accepting submissions and drops every queued task. In-flight batches finish on
   * their own thread; a parked first cycle is cancelled so its payload is not pinned in the
   * scheduler's delay queue. The loss is bounded by the next periodic broadcast or write.
   *
   * <p>The pending counter is reset with the map that held the queued weight. Only
   * {@link #submit} reads it, and no submit can be admitted after the dispatcher is closed,
   * so a settlement racing the reset cannot affect behaviour.
   */
  @Override
  public void close() {
    closed = true;
    for (KeyWorker worker : queues.values()) {
      worker.cancelJitter();
    }
    queues.clear();
    globalPendingUnits.reset();
    if (executor instanceof ThreadPoolExecutor pool) {
      pool.purge();
    }
  }

  /**
   * One key's queue. The current batch is pre-granted at construction or at the end of the
   * previous cycle; {@code queue} holds only tasks that have not been granted yet.
   * {@code grant}/{@code queue} are mutated under the map's bin lock, while {@code grant} is
   * read and cleared by the cycle on its own thread (volatile for that hand-off).
   */
  private final class KeyWorker implements Runnable {

    private static final int INITIAL_QUEUE_SIZE = 8;

    final Object key;

    ArrayDeque<PendingTask<T>> queue;
    /**
     * The batch this worker will execute next, or {@code null} while it is executing one.
     * Written under the bin lock by construction or {@link #drainNext(int)}, read and cleared
     * by {@link #run(KeyWorker)} on the worker thread.
     */
    volatile Grant<T> grant;
    /**
     * Whether the pre-granted first batch has not started yet. The first task counts against
     * the per-key bound while the start-jitter park holds it.
     */
    volatile boolean first;
    /**
     * Whether {@link #queue} holds at least one task. Written under the bin lock, read by
     * {@link #run(KeyWorker)} outside the lock as a merge hint; the deque itself is never
     * accessed outside the lock.
     */
    volatile boolean hasQueued;
    /**
     * The parked first cycle when start jitter deferred it, or {@code null} when the first
     * cycle ran or was submitted immediately. {@link #close()} cancels it.
     */
    volatile ScheduledFuture<?> jitterFuture;

    KeyWorker(Object key, T firstTask, int firstWeight) {
      this.key = key;
      this.grant = new Grant<>(List.of(firstTask), firstWeight);
      this.first = true;
    }

    @Override
    public void run() {
      KeyDispatcher.this.run(this);
    }

    /** Cancel the parked first cycle, if this worker still has one. */
    void cancelJitter() {
      ScheduledFuture<?> future = jitterFuture;
      if (future != null) {
        future.cancel(false);
      }
    }

    boolean enqueue(T task, int weight) {
      if (queue == null) {
        queue = new ArrayDeque<>(INITIAL_QUEUE_SIZE);
      }

      // The pre-granted first task is pending until its cycle starts; counting only the deque
      // would admit maxQueuePerKey + 1 tasks while the start-jitter park holds it back.
      if (pendingCount() >= maxQueuePerKey) {
        return false;
      }
      queue.addLast(new PendingTask<>(task, weight));
      hasQueued = true;
      return true;
    }

    /**
     * Pending tasks for this key: the pre-granted first batch counts until its cycle starts,
     * the deque counts after it.
     *
     * @return the number of admitted tasks not yet granted
     */
    int pendingCount() {
      return (queue == null ? 0 : queue.size()) + (first ? 1 : 0);
    }

    /**
     * Folds the queued tail into the pre-granted first batch, up to {@code maxTasks} total.
     * Runs under the bin lock before the first cycle starts.
     *
     * @return the merged grant, or {@code null} when there is nothing to fold
     */
    Grant<T> mergeFirst(int maxTasks) {
      first = false;
      Grant<T> current = grant;
      if (current == null || queue == null || queue.isEmpty()) {
        updateHasQueued();
        return null;
      }
      int room = maxTasks - current.tasks().size();
      if (room <= 0) {
        hasQueued = true;
        return null;
      }

      int limit = Math.min(room, queue.size());
      List<T> tasks = new ArrayList<>(current.tasks().size() + limit);
      tasks.addAll(current.tasks());
      long units = drainInto(limit, tasks, current.units());

      grant = new Grant<>(tasks, units);
      updateHasQueued();
      return grant;
    }

    /**
     * Takes up to {@code maxTasks} queued tasks into the next pre-granted batch, under the bin
     * lock. The next cycle reads the grant through its volatile hand-off.
     *
     * @return {@code false} when the worker has nothing to grant
     */
    boolean drainNext(int maxTasks) {
      if (queue == null || queue.isEmpty()) {
        hasQueued = false;
        return false;
      }
      // Fast path: construct directly for a single element to avoid ArrayList overhead
      if (queue.size() == 1) {
        PendingTask<T> single = queue.poll();
        grant = new Grant<>(List.of(single.task()), single.weight());
        hasQueued = false;
        return true;
      }

      int limit = Math.min(maxTasks, queue.size());
      List<T> tasks = new ArrayList<>(limit);
      long units = drainInto(limit, tasks, 0L);

      grant = new Grant<>(tasks, units);
      updateHasQueued();
      return true;
    }

    /**
     * Drains up to {@code limit} tasks from the queue, appends them to the target list and
     * accumulates their weights.
     *
     * @param limit        the number of tasks to drain; must not exceed {@code queue.size()}
     * @param tasks        the target list, which may already hold the first batch's tasks
     * @param initialUnits the weight already accumulated by those tasks
     * @return the total accumulated weight after draining
     */
    private long drainInto(int limit, List<T> tasks, long initialUnits) {
      long units = initialUnits;
      for (int i = 0; i < limit; i++) {
        PendingTask<T> pending = queue.poll();
        if (pending != null) {
          tasks.add(pending.task());
          units += pending.weight();
        }
      }
      return units;
    }

    /** Re-derives the lock-free merge hint after the deque was drained or partially read. */
    private void updateHasQueued() {
      hasQueued = queue != null && !queue.isEmpty();
    }

    /**
     * Drops every queued task and the pre-granted batch, returning the weight they held. Runs
     * under the bin lock; a batch already executing is settled by {@link #run(KeyWorker)}.
     *
     * @return the released pending units
     */
    long discard() {
      Grant<T> pending = grant;
      grant = null;
      long units = pending != null ? pending.units() : 0L;
      if (queue != null) {
        for (PendingTask<T> task : queue) {
          units += task.weight();
        }
        queue.clear();
      }
      hasQueued = false;
      return units;
    }
  }

  /** One pre-granted batch and the global-budget weight it holds. */
  private record Grant<T>(List<T> tasks, long units) {}

  /** A queued task with its positive weight. */
  private record PendingTask<T>(T task, int weight) {}
}
