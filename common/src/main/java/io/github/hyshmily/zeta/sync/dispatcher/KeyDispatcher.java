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
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

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
 * <p><b>Pre-granted first batch.</b> A worker is created with its first task already
 * granted, and every later batch is drained at the end of the previous cycle. The cycle
 * therefore executes a batch without taking the bin lock first: a single-task burst costs
 * one submit compute plus one retire compute, and a sustained key costs one compute per
 * cycle instead of two.
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
   * @param maxQueuePerKey        per-key pending-queue bound
   * @param maxTasksPerCycle      tasks consumed per executor submission
   * @param maxGlobalPendingUnits aggregate weighted-unit budget across all keys
   * @param jitterMs              upper bound of the random first-cycle park; {@code <= 0} disables it
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
        // The first task is pre-granted: the common single-task burst needs no deque and
        // no grant compute before its first consumer call.
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
    log.warn(
      "[{}] Executor refused every submission for key={}; dropped {} pending unit(s) so the key can be rescheduled",
      name,
      worker.key,
      released[0]
    );
  }

  /**
   * Execute one pre-granted batch, then either drain and resubmit the worker for the next
   * batch or retire it. A consumer failure is reported through the exception-handler chain
   * and never kills the worker thread.
   */
  @SuppressWarnings("all")
  private void run(KeyWorker worker) {
    Grant<T> grant = worker.grant;
    if (closed || grant == null) {
      worker.grant = null;
      queues.remove(worker.key, worker);
      return;
    }

    // A burst that arrived while the start-jitter park held the first task is folded into
    // that first batch under the bin lock. A lone first task pays no compute; a burst
    // coalesces into one consumer call, one sweep, one loader read.
    if (worker.first && worker.hasQueued()) {
      Grant<T>[] merged = new Grant[1];
      KeyWorker kept = queues.compute(worker.key, (k, current) -> {
        if (closed || current != worker) {
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
      // close() may have run between the schedule and here. The granted tasks were taken
      // out of the queue and must NOT run after close.
      if (closed) {
        queues.remove(worker.key, worker);
        return;
      }
      consumer.accept(grant.tasks());
    } catch (Throwable t) {
      ZetaExceptionHandler.handleException("[" + name + "] Batch execution failed for key " + worker.key, t);
    } finally {
      // close() already reset the counter together with the map that held the weight;
      // subtracting after that reset would drive it negative.
      if (!closed) {
        globalPendingUnits.add(-grant.units());
      }
    }

    // One compute at the end: empty queue retires the worker; a non-empty queue drains the
    // next batch under the same bin lock and resubmits.
    KeyWorker kept = queues.compute(worker.key, (k, current) -> {
      if (closed || current != worker) {
        return null;
      }
      return worker.drainNext(maxTasksPerCycle) ? worker : null;
    });
    if (kept == worker) {
      scheduleWorker(worker);
    }
  }

  /**
   * Stop accepting submissions and drop every queued task. In-flight batches finish on
   * their own thread; the loss is bounded by the next periodic broadcast or write.
   */
  @Override
  public void close() {
    closed = true;
    // Clearing the map drops every queued task: no new worker can be created (closed is
    // true) and any in-flight run that reaches the end compute finds current != worker
    // (the map entry is gone) and returns null. globalPendingUnits is reset because the
    // tasks that held the weight are gone with the map.
    queues.clear();
    globalPendingUnits.reset();
  }

  /**
   * One key's queue. The current batch is pre-granted at construction or at the end of the
   * previous cycle; {@code queue} holds only tasks that have not been granted yet.
   * {@code grant}/{@code queue} are mutated under the map's bin lock, while {@code grant} is
   * read and cleared by the cycle on its own thread (volatile for that hand-off).
   *
   * <p>Runs its own cycle so a submission never allocates a {@code Runnable} wrapper.
   */
  private final class KeyWorker implements Runnable {

    private static final int INITIAL_QUEUE_SIZE = 8;

    final Object key;

    ArrayDeque<PendingTask<T>> queue;
    /**
     * The batch this worker will execute next, or {@code null} while it is executing one.
     * Written under the bin lock by construction or {@link #drainNext(int)}, read and
     * cleared by {@link #run(KeyWorker)} on the worker thread.
     */
    volatile Grant<T> grant;
    /**
     * Whether the pre-granted first batch has not started yet. The first task counts
     * against the per-key bound while the start-jitter park holds it; a later drained batch
     * is already granted and does not.
     */
    volatile boolean first;

    KeyWorker(Object key, T firstTask, int firstWeight) {
      this.key = key;
      this.grant = new Grant<>(List.of(firstTask), firstWeight);
      this.first = true;
    }

    @Override
    public void run() {
      KeyDispatcher.this.run(this);
    }

    boolean enqueue(T task, int weight) {
      if (queue == null) {
        queue = new ArrayDeque<>(INITIAL_QUEUE_SIZE);
      }

      // The first task is pending until its cycle starts; counting only the deque would
      // admit maxQueuePerKey + 1 tasks while the start-jitter park holds that first task
      // back. A later pre-granted batch is already admitted and must not be counted twice.
      int pendingCount = queue.size() + (first ? 1 : 0);
      if (pendingCount >= maxQueuePerKey) {
        return false;
      }
      queue.addLast(new PendingTask<>(task, weight));
      return true;
    }

    /** Whether a task is waiting behind the pre-granted first batch. */
    boolean hasQueued() {
      return queue != null && !queue.isEmpty();
    }

    /**
     * Fold the queued tail into the pre-granted first batch, up to {@code maxTasks} total.
     * Runs under the bin lock before the first cycle starts.
     *
     * @return the merged grant, or {@code null} when there is nothing to fold
     */
    Grant<T> mergeFirst(int maxTasks) {
      Grant<T> current = grant;
      if (current == null || queue == null || queue.isEmpty()) {
        return null;
      }
      int room = maxTasks - current.tasks().size();
      if (room <= 0) {
        return null;
      }

      int limit = Math.min(room, queue.size());
      List<T> tasks = new ArrayList<>(current.tasks().size() + limit);
      tasks.addAll(current.tasks());
      long units = drainInto(limit, tasks, current.units());

      grant = new Grant<>(tasks, units);
      return grant;
    }

    /**
     * Take up to {@code maxTasks} queued tasks into the next pre-granted batch. Runs under
     * the bin lock; the next cycle reads the grant through its volatile hand-off.
     *
     * @return {@code false} when the worker has nothing to grant
     */
    boolean drainNext(int maxTasks) {
      if (queue == null || queue.isEmpty()) {
        return false;
      }
      // Fast path: construct directly for a single element to avoid ArrayList overhead
      if (queue.size() == 1) {
        PendingTask<T> single = queue.poll();
        grant = new Grant<>(List.of(single.task()), single.weight());
        return true;
      }

      int limit = Math.min(maxTasks, queue.size());
      List<T> tasks = new ArrayList<>(limit);
      long units = drainInto(limit, tasks, 0L);

      grant = new Grant<>(tasks, units);
      return true;
    }

    /**
     * Drains up to {@code limit} tasks from the queue, appends them to the target
     * list, and accumulates their weights.
     *
     * @param limit        the maximum number of tasks to drain
     * @param tasks        the target list to append tasks to (may already contain
     *                     initial tasks, as in the merge scenario of {@code mergeFirst})
     * @param initialUnits the initial accumulated weight (e.g., from the existing
     *                     grant in {@code mergeFirst})
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

    /**
     * Drop every queued task and the pre-granted batch, returning the weight they held.
     * Runs under the bin lock when the worker retires; a batch already executing is owned by
     * {@link #run(KeyWorker)}, which settles its weight in its own {@code finally}.
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
      return units;
    }
  }

  /** One pre-granted batch and the global-budget weight it holds. */
  private record Grant<T>(List<T> tasks, long units) {}

  /** A queued task with its positive weight. */
  private record PendingTask<T>(T task, int weight) {}
}
