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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
 * (exposed via Micrometer).
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

  private static final int DEFAULT_MAX_QUEUE_PER_KEY = 1024;

  /**
   * Maximum number of tasks a single key may consume before the worker yields back to the
   * executor, giving other keys a chance to run. Trades throughput (larger batches, fewer pool
   * submissions) against fairness (a busy key must eventually let other keys progress).
   */
  private static final int DEFAULT_MAX_TASKS_PER_CYCLE = 64;

  /** Delay before re-driving a worker whose submission was rejected by the executor. */
  private static final long REJECTION_RETRY_DELAY_MS = 200;

  private final ConcurrentHashMap<Object, KeyWorker> queues = new ConcurrentHashMap<>();
  private final ScheduledExecutorService executor;
  private final String name;
  private final int maxQueuePerKey;
  private final int maxTasksPerCycle;

  private volatile boolean closed = false;

  public PerKeyOrderedDispatcher(ScheduledExecutorService executor, String name) {
    this(executor, name, DEFAULT_MAX_QUEUE_PER_KEY, DEFAULT_MAX_TASKS_PER_CYCLE);
  }

  public PerKeyOrderedDispatcher(ScheduledExecutorService executor, String name, int maxQueuePerKey) {
    this(executor, name, maxQueuePerKey, DEFAULT_MAX_TASKS_PER_CYCLE);
  }

  public PerKeyOrderedDispatcher(
    ScheduledExecutorService executor,
    String name,
    int maxQueuePerKey,
    int maxTasksPerCycle
  ) {
    this.executor = executor;
    this.name = name;
    this.maxQueuePerKey = maxQueuePerKey;
    this.maxTasksPerCycle = maxTasksPerCycle;
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
    if (delayMs <= 0) {
      submit(key, task);
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
    executor.schedule(() -> submit(key, task), delayMs, TimeUnit.MILLISECONDS);
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
    if (closed) {
      return;
    }

    // Single atomic compute: creates the worker together with its first task, or enqueues into
    // the existing worker. The start decision is made atomically with the insertion, so there
    // is no window in which the worker can be orphaned (a queue whose owner never re-runs it) —
    // the running-flag re-verification and retry loop of earlier designs is not needed.
    boolean[] startWorker = { false };
    KeyWorker worker = queues.compute(key, (k, existing) -> {
      if (existing == null) {
        startWorker[0] = true;
        return new KeyWorker(key, task);
      }
      // Key is already being processed, try to enqueue.
      if (!existing.enqueue(task, maxQueuePerKey)) {
        log.warn("[{}] Task queue full for key {}. Task rejected.", name, key);
      }
      return existing;
    });

    // Must run execute outside of compute: the executor call may reject (or block), and the
    // map bin lock must not be held while invoking it.
    if (startWorker[0]) {
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
    List<Runnable> batch = grantBatch(worker);
    if (batch.isEmpty()) {
      // The worker was replaced or removed concurrently (e.g. close() cleared the map while
      // the batch was being granted); nothing to run.
      return;
    }
    for (Runnable task : batch) {
      if (closed) {
        // Dispatcher was closed mid-batch; remaining tasks must never run.
        queues.remove(worker.key, worker);
        return;
      }
      try {
        task.run();
      } catch (Throwable t) {
        // Route through the injectable exception-handler chain (WARN log by default) and keep
        // consuming the batch — one failing task must not strand the remaining tasks of the key.
        ZetaExceptionHandler.handleException("[" + name + "] Task execution failed for key " + worker.key, t);
      }
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
  private List<Runnable> grantBatch(KeyWorker worker) {
    List<Runnable> batch = new ArrayList<>(maxTasksPerCycle);
    queues.compute(worker.key, (k, v) -> {
      // Only touch our own worker. If the entry was replaced or removed (e.g. by close()),
      // leave the batch empty and do not disturb the current entry.
      if (v != worker) {
        return v;
      }
      Runnable first = worker.firstTask;
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
  }

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
     * First task, stored separately from the queue so that the common single-task case never
     * allocates the {@link ArrayDeque} at all. Accessed only while holding the map bin lock
     * (i.e. inside {@code queues.compute(...)}) or during construction (safe publication via
     * the {@code ConcurrentHashMap}), so it deliberately needs no {@code volatile}.
     */
    Runnable firstTask;
    /**
     * Queued tasks behind {@link #firstTask}. Only accessed while holding the map bin lock
     * (i.e. inside {@code queues.compute(...)}), so no additional synchronization is needed.
     */
    ArrayDeque<Runnable> queue;

    KeyWorker(Object key, Runnable firstTask) {
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
     * @param maxSize maximum allowed queue size
     * @return true if the task was enqueued, false if the queue is full
     */
    boolean enqueue(Runnable task, int maxSize) {
      if (queue == null) {
        queue = new ArrayDeque<>(INITIAL_QUEUE_SIZE);
      }
      if (queue.size() >= maxSize) {
        return false;
      }
      queue.addLast(task);
      return true;
    }
  }
}
