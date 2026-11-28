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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * Per-key FIFO batch dispatcher executing tasks of the same key sequentially while running
 * distinct keys in parallel.
 *
 * <p>Each key yields the executor after at most {@code maxTasksPerCycle} tasks so hot keys cannot
 * starve other keys. The {@link Consumer} receives the entire granted batch to coalesce redundant
 * work (such as repeated reloads or sweeps).
 *
 * <p>At most one {@link KeyWorker} exists per key in {@link #queues}, created with its first batch
 * pre-granted and retired or re-driven atomically inside the key's map bin lock. The supplied
 * {@link ScheduledExecutorService} must not execute tasks inline (no caller-runs or direct
 * executor), as submission runs under the key's bin lock.
 *
 * <p>Thread-safe.
 *
 * @param <T> the dispatched task type
 */
@Slf4j
@Internal
public class KeyDispatcher<T> implements AutoCloseable {

  public static final int DEFAULT_MAX_GLOBAL_PENDING_UNITS = 50_000;
  public static final int DEFAULT_MAX_QUEUE_PER_KEY = 1024;
  public static final int DEFAULT_MAX_TASKS_PER_CYCLE = 64;

  private static final int MAX_CAUSE_CHAIN_DEPTH = 8;

  private final ConcurrentHashMap<Object, KeyWorker> queues = new ConcurrentHashMap<>();

  /** Striped pending-weight counter for the soft global memory budget. */
  private final LongAdder globalPendingUnits = new LongAdder();

  private final ScheduledExecutorService executor;
  private final Consumer<List<T>> consumer;
  private final long maxGlobalPendingUnits;
  private final int maxTasksPerCycle;
  private final int maxQueuePerKey;
  private final long jitterMs;
  private final String name;

  private volatile boolean closed;

  /**
   * Creates a dispatcher with default per-key queue capacity and per-cycle task cap.
   *
   * @param executor              executor running per-key cycles and optional start-jitter delays
   * @param name                  dispatcher name used in log messages
   * @param consumer              batch consumer invoked sequentially per key
   * @param maxGlobalPendingUnits aggregate pending-weight budget across all keys; must be {@code >= 1}
   * @param jitterMs              upper bound in milliseconds for random first-cycle delay
   *                              ({@code <= 0} disables jitter)
   * @throws IllegalArgumentException if {@code maxGlobalPendingUnits < 1}
   * @throws NullPointerException     if {@code executor}, {@code name}, or {@code consumer} is {@code null}
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
   * Creates a dispatcher with explicit queue, cycle, budget, and jitter limits.
   *
   * @param executor              executor running per-key cycles and optional start-jitter delays
   * @param name                  dispatcher name used in log messages
   * @param consumer              batch consumer invoked sequentially per key
   * @param maxQueuePerKey        per-key pending queue bound; must be {@code >= 1}
   * @param maxTasksPerCycle      maximum tasks drained per executor cycle; must be {@code >= 1}
   * @param maxGlobalPendingUnits aggregate pending-weight budget across all keys; must be {@code >= 1}
   * @param jitterMs              upper bound in milliseconds for random first-cycle delay
   *                              ({@code <= 0} disables jitter)
   * @throws IllegalArgumentException if any capacity or budget bound is less than {@code 1}
   * @throws NullPointerException     if {@code executor}, {@code name}, or {@code consumer} is {@code null}
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
    if (maxGlobalPendingUnits < 1L) {
      throw new IllegalArgumentException("maxGlobalPendingUnits must be >= 1, got " + maxGlobalPendingUnits);
    }
    // Ensure cancelled jitter futures leave the delay queue promptly instead of pinning payloads.
    if (executor instanceof ScheduledThreadPoolExecutor scheduled) {
      scheduled.setRemoveOnCancelPolicy(true);
    }
    this.maxQueuePerKey = maxQueuePerKey;
    this.maxTasksPerCycle = maxTasksPerCycle;
    this.maxGlobalPendingUnits = maxGlobalPendingUnits;
    this.jitterMs = Math.max(0L, jitterMs);
  }

  /**
   * Submits a data-plane task under its FIFO ordering key, subject to both per-key and global
   * pending-weight limits.
   *
   * @param key    the ordering key (same key executes FIFO)
   * @param task   the task to enqueue
   * @param weight task weight in global pending units (clamped to {@code >= 1})
   * @param jitter {@code true} to apply start-jitter delay when spawning a new key worker
   * @return {@code true} if accepted; {@code false} if closed, rejected by capacity limits, or
   *         refused by the executor
   * @throws NullPointerException if {@code key} or {@code task} is {@code null}
   */
  public boolean submit(Object key, T task, int weight, boolean jitter) {
    return submit(key, task, weight, jitter, false);
  }

  /**
   * Submits a control-plane task exempt from the global pending-weight budget (still subject to
   * per-key ordering and capacity) without start jitter.
   *
   * @param key  the ordering key
   * @param task the control task to enqueue
   * @return {@code true} if accepted; {@code false} if closed, per-key queue full, or refused by
   *         the executor
   * @throws NullPointerException if {@code key} or {@code task} is {@code null}
   */
  public boolean submitControl(Object key, T task) {
    return submit(key, task, 1, false, true);
  }

  /**
   * Stops accepting new submissions, cancels parked jitter tasks, drops all queued entries, and
   * resets pending weight tracking. In-flight batches complete on their own worker threads.
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
   * Enqueues {@code task} under {@code key}'s bin lock, spawning and scheduling a {@link KeyWorker}
   * when none is active.
   *
   * @param key     the ordering key
   * @param task    the task to enqueue
   * @param weight  raw task weight
   * @param jitter  whether a newly created worker may park for start jitter
   * @param control {@code true} to bypass the global pending-weight budget
   * @return {@code true} if accepted
   */
  private boolean submit(Object key, T task, int weight, boolean jitter, boolean control) {
    Objects.requireNonNull(key, "key must not be null");
    Objects.requireNonNull(task, "task must not be null");
    if (closed) {
      return false;
    }

    int effectiveWeight = Math.max(1, weight);
    // Subtraction avoids overflow near Long.MAX_VALUE.
    if (!control && globalPendingUnits.sum() > maxGlobalPendingUnits - effectiveWeight) {
      return false;
    }

    SubmitOutcome<KeyWorker> outcome = new SubmitOutcome<>();
    queues.compute(key, (k, existing) -> {
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

        outcome.created = created;
        return created;
      }
      if (!existing.enqueue(task, effectiveWeight)) {
        return existing;
      }

      globalPendingUnits.add(effectiveWeight);
      outcome.accepted = true;
      return existing;
    });

    if (outcome.created != null) {
      if (!closed) {
        return true;
      }
      // Refuse only a first cycle no worker thread has claimed: after the claim the batch will run
      // (or already has), and the caller must not apply a fallback over a write that landed.
      if (!outcome.created.abortFirst()) {
        return true;
      }
      queues.remove(key, outcome.created);
      outcome.created.cancelJitter();
      return false;
    }
    return outcome.accepted;
  }

  /**
   * Schedules a new worker's first cycle under the key's bin lock, applying optional start jitter
   * and falling back to immediate execution if delayed scheduling is rejected.
   *
   * <p>When {@link #close()} wins the publication race the cycle is refused only if no worker
   * thread has claimed it yet. A claimed cycle is reported as accepted: it may already have run its
   * batch, and a caller that treats it as refused would apply an inline removal over the value that
   * batch just wrote.
   *
   * @param worker the newly created worker
   * @param jitter whether start jitter is enabled for this submission
   * @return {@code true} if the cycle was dispatched and not refused by a concurrent close
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
          if (closed && worker.abortFirst()) {
            future.cancel(false);
            return false;
          }
          return true;
        } catch (RejectedExecutionException ignored) {
          // Fall through to immediate execution below.
        }
      }
    }
    if (closed) {
      return false;
    }
    try {
      executor.execute(worker);
      return !(closed && worker.abortFirst());
    } catch (RejectedExecutionException ignored) {
      return false;
    }
  }

  /**
   * Submits {@code worker}'s next cycle from inside its retire-bin lock. Holding that lock while
   * submitting keeps the retire decision and the resubmission atomic: the worker cannot retire
   * between the pending-count check and the hand-off to the executor. A rejection is returned to
   * the caller so the worker can be retired instead of left parked.
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
   * Executes one pre-granted batch for {@code worker}, handles failures via
   * {@link ZetaExceptionHandler}, releases batch weight, and atomically either schedules the next
   * cycle or retires the worker.
   *
   * <p>The first cycle runs only for the thread that wins {@link KeyWorker#claimFirst()}: a
   * concurrent {@link #close()} that already refused the submission must not also let the batch
   * run, and a batch that did start must not be reported as refused. Every exit above that claim
   * leaves it unclaimed, so the refusing caller observes {@code false} truthfully.
   *
   * @param worker the active key worker
   */
  @SuppressWarnings("java:S1181")
  private void run(KeyWorker worker) {
    worker.jitterFuture = null;
    List<T> tasks = worker.grantTasks;
    long units = worker.grantUnits;
    if (closed || tasks == null) {
      worker.grantTasks = null;
      queues.remove(worker.key, worker);
      return;
    }

    boolean firstCycle = worker.first;
    if (firstCycle && worker.hasQueued) {
      if (!coalesceFirstBatch(worker)) {
        return;
      }
      tasks = worker.grantTasks;
      units = worker.grantUnits;
    }
    worker.grantTasks = null;
    worker.first = false;
    if (closed || tasks == null || (firstCycle && !worker.claimFirst())) {
      queues.remove(worker.key, worker);
      return;
    }

    try {
      consumer.accept(tasks);
    } catch (Throwable t) {
      restoreInterrupt(t);
      ZetaExceptionHandler.handleException("[" + name + "] Batch execution failed for key " + worker.key, t);
      if (t instanceof Error error) {
        retire(worker);
        throw error;
      }
    } finally {
      if (!closed) {
        globalPendingUnits.add(-units);
      }
    }

    scheduleNextCycleOrRetire(worker);
  }

  /**
   * Folds tasks that arrived during a first-cycle jitter park into {@code worker}'s initial batch
   * under the key's bin lock.
   *
   * @param worker the worker starting its first cycle
   * @return {@code true} if {@code worker} remains active and should proceed with execution
   */
  private boolean coalesceFirstBatch(KeyWorker worker) {
    KeyWorker kept = queues.compute(worker.key, (k, current) -> {
      if (current != worker) {
        return current;
      }
      if (closed) {
        return null;
      }
      worker.mergeFirst(maxTasksPerCycle);
      return worker;
    });
    return kept == worker;
  }

  /**
   * Drains the next batch and resubmits {@code worker} under its bin lock, or retires the worker
   * if the queue is empty, closed, or rejected by the executor.
   *
   * @param worker the worker finishing a cycle
   */
  private void scheduleNextCycleOrRetire(KeyWorker worker) {
    long[] dropped = { -1L };
    Throwable[] failure = new Throwable[1];
    queues.compute(worker.key, (k, current) -> {
      if (current != worker) {
        return current;
      }
      if (closed || !worker.drainNext(maxTasksPerCycle)) {
        return null;
      }
      try {
        if (submitCycle(worker)) {
          return worker;
        }
      } catch (RuntimeException | Error e) {
        // A submission failure other than a plain rejection must still retire the worker: leaving
        // it in RUNNING with a drained batch would swallow every later event for this key.
        failure[0] = e;
      }
      dropped[0] = worker.discard();
      return null;
    });
    if (dropped[0] >= 0L) {
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
    if (failure[0] != null) {
      if (failure[0] instanceof RuntimeException runtime) {
        throw runtime;
      }
      throw (Error) failure[0];
    }
  }

  /**
   * Removes {@code worker} after a fatal error and releases the pending weight of its remaining
   * queued tasks.
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

  /** Restores the thread interrupt flag if an {@link InterruptedException} appears in the cause chain. */
  private static void restoreInterrupt(Throwable failure) {
    Throwable cause = failure;
    for (int depth = 0; cause != null && depth < MAX_CAUSE_CHAIN_DEPTH; depth++, cause = cause.getCause()) {
      if (cause instanceof InterruptedException) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  /**
   * Per-key queue and cycle runner. {@link #queue} and {@link #grantTasks} mutations run strictly
   * under {@link #queues}'s bin lock, with volatile hand-off of {@link #grantTasks} to the worker
   * thread executing {@link #run()}.
   */
  private final class KeyWorker implements Runnable {

    private static final int INITIAL_QUEUE_SIZE = 8;

    /** No thread owns the pre-granted first cycle yet. */
    private static final int FIRST_PENDING = 0;

    /** A worker thread is running (or has run) the pre-granted first cycle. */
    private static final int FIRST_CLAIMED = 1;

    /** {@link #close()} refused the first cycle before a worker thread could claim it. */
    private static final int FIRST_ABORTED = 2;

    private static final VarHandle FIRST_STATE;

    static {
      try {
        FIRST_STATE = MethodHandles.lookup().findVarHandle(KeyWorker.class, "firstState", int.class);
      } catch (ReflectiveOperationException e) {
        throw new ExceptionInInitializerError(e);
      }
    }

    final Object key;

    @Nullable
    ArrayDeque<PendingTask<T>> queue;

    long grantUnits;
    volatile @Nullable List<T> grantTasks;
    volatile boolean first;
    volatile boolean hasQueued;
    volatile @Nullable ScheduledFuture<?> jitterFuture;

    /**
     * Ownership of the pre-granted first cycle, so {@link #close()} either prevents the batch from
     * running or reports it as accepted — never both.
     */
    private volatile int firstState = FIRST_PENDING;

    /** Creates a worker with its initial single-task batch pre-granted. */
    private KeyWorker(Object key, T firstTask, int firstWeight) {
      this.key = key;
      this.grantUnits = firstWeight;
      this.grantTasks = List.of(firstTask);
      this.first = true;
    }

    @Override
    public void run() {
      KeyDispatcher.this.run(this);
    }

    /**
     * Claims the right to run the pre-granted first cycle.
     *
     * @return {@code false} when {@link #close()} already refused this cycle
     */
    private boolean claimFirst() {
      return FIRST_STATE.compareAndSet(this, FIRST_PENDING, FIRST_CLAIMED);
    }

    /**
     * Refuses the pre-granted first cycle unless a worker thread already claimed it.
     *
     * @return {@code true} when this call owns the refusal and the batch will not run
     */
    private boolean abortFirst() {
      return FIRST_STATE.compareAndSet(this, FIRST_PENDING, FIRST_ABORTED);
    }

    /** Cancels a parked first-cycle jitter future if still present. */
    private void cancelJitter() {
      ScheduledFuture<?> future = jitterFuture;
      if (future != null) {
        future.cancel(false);
      }
    }

    /**
     * Appends {@code task} to this key's pending deque under the bin lock if within
     * {@link #maxQueuePerKey}.
     *
     * @return {@code true} if enqueued, {@code false} if the per-key queue is full
     */
    private boolean enqueue(T task, int weight) {
      ArrayDeque<PendingTask<T>> deque = queue;
      if (deque == null) {
        deque = new ArrayDeque<>(INITIAL_QUEUE_SIZE);
        queue = deque;
      }

      if (pendingCount() >= maxQueuePerKey) {
        return false;
      }
      deque.addLast(new PendingTask<>(task, weight));
      hasQueued = true;
      return true;
    }

    /** Returns pending tasks for this key, counting the pre-granted first task until it starts. */
    private int pendingCount() {
      return (queue == null ? 0 : queue.size()) + (first ? 1 : 0);
    }

    /**
     * Coalesces queued tasks into the pre-granted initial batch up to {@code maxTasks} total;
     * called under the bin lock.
     */
    private void mergeFirst(int maxTasks) {
      first = false;
      jitterFuture = null;
      List<T> current = grantTasks;
      ArrayDeque<PendingTask<T>> deque = queue;
      if (current == null || deque == null || deque.isEmpty()) {
        updateHasQueued();
        return;
      }

      int room = maxTasks - current.size();
      if (room <= 0) {
        hasQueued = true;
        return;
      }

      int limit = Math.min(room, deque.size());
      List<T> tasks = new ArrayList<>(current.size() + limit);
      tasks.addAll(current);
      grantUnits = drainInto(deque, limit, tasks, grantUnits);
      grantTasks = tasks;
      updateHasQueued();
    }

    /**
     * Drains up to {@code maxTasks} queued entries into the next pre-granted batch under the bin
     * lock.
     *
     * @return {@code true} if a non-empty batch was granted
     */
    private boolean drainNext(int maxTasks) {
      jitterFuture = null;
      ArrayDeque<PendingTask<T>> deque = queue;
      if (deque == null || deque.isEmpty()) {
        hasQueued = false;
        return false;
      }

      if (deque.size() == 1) {
        PendingTask<T> single = deque.removeFirst();
        grantUnits = single.weight();
        grantTasks = List.of(single.task());
        hasQueued = false;
        return true;
      }

      int limit = Math.min(maxTasks, deque.size());
      List<T> tasks = new ArrayList<>(limit);
      grantUnits = drainInto(deque, limit, tasks, 0L);
      grantTasks = tasks;
      updateHasQueued();
      return true;
    }

    /** Drains {@code limit} entries from {@code deque} into {@code tasks} and sums their weights. */
    private long drainInto(ArrayDeque<PendingTask<T>> deque, int limit, List<T> tasks, long initialUnits) {
      long units = initialUnits;
      for (int i = 0; i < limit; i++) {
        PendingTask<T> pending = deque.poll();
        if (pending != null) {
          tasks.add(pending.task());
          units += pending.weight();
        }
      }
      return units;
    }

    /** Refreshes the lock-free {@link #hasQueued} hint from {@link #queue}. */
    private void updateHasQueued() {
      hasQueued = queue != null && !queue.isEmpty();
    }

    /**
     * Clears any unexecuted pre-granted batch and queued tasks under the bin lock and returns
     * their total released weight.
     */
    private long discard() {
      long units = grantTasks != null ? grantUnits : 0L;
      grantTasks = null;
      grantUnits = 0L;
      ArrayDeque<PendingTask<T>> deque = queue;
      if (deque != null) {
        for (PendingTask<T> task : deque) {
          units += task.weight();
        }
        deque.clear();
      }
      hasQueued = false;
      return units;
    }
  }

  /** Queued task paired with its positive pending weight. */
  private record PendingTask<T>(T task, int weight) {}

  /** Mutable outcome carrier populated inside {@link #queues}'s {@code compute} lambda. */
  private static final class SubmitOutcome<W> {

    boolean accepted;

    @Nullable
    W created;
  }
}
