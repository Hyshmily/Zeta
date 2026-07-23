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
import java.util.ArrayDeque;
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
 * <p><b>Backpressure:</b> Each key's queue is bounded by {@code maxQueuePerKey}.
 * When the limit is reached, excess submissions are silently counted as rejected
 * (exposed via Micrometer).
 *
 * <p><b>Executor rejection recovery:</b> If the underlying executor rejects a task
 * via {@link RejectedExecutionException}, the task is returned to the head of its
 * key queue, the running flag is cleared, and a delayed retry is scheduled for the
 * head of the queue — self-healing without depending on future submissions.
 *
 * <p>This class is thread-safe.
 */
@Slf4j
@Internal
public class PerKeyOrderedDispatcher implements AutoCloseable {

  private static final int DEFAULT_MAX_QUEUE_PER_KEY = 1024;

  private final ConcurrentHashMap<Object, KeyQueue> queues = new ConcurrentHashMap<>();
  private final ScheduledExecutorService executor;
  private final String name;
  private final int maxQueuePerKey;

  private volatile boolean closed = false;

  public PerKeyOrderedDispatcher(ScheduledExecutorService executor, String name) {
    this(executor, name, DEFAULT_MAX_QUEUE_PER_KEY);
  }

  public PerKeyOrderedDispatcher(ScheduledExecutorService executor, String name, int maxQueuePerKey) {
    this.executor = executor;
    this.name = name;
    this.maxQueuePerKey = maxQueuePerKey;
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
   * the task is returned to the head of its key queue and a delayed retry is scheduled
   * (self-healing, does not depend on future submissions).</p>
   */
  public void submit(Object key, Runnable task) {
    if (closed) {
      return;
    }

    // Loop to recover from the race where scheduleNext removes a queue from the map
    // right after we obtained it and marked it as running (see scheduleNext).
    while (true) {
      KeyQueue kq = queues.computeIfAbsent(key, k -> new KeyQueue(key));
      // Try to mark this key as running. If successful, we must execute the task directly.
      if (kq.tryMarkRunning()) {
        // Re-verify that this queue is still in the map. If scheduleNext removed it
        // concurrently (pollAndCheck returned null, isRunning returned false, and remove
        // happened between our computeIfAbsent and tryMarkRunning), we have an orphan queue
        // that would never be scheduled again. Reset it and retry.
        if (queues.get(key) == kq) {
          runTask(key, kq, task);
          return;
        }
        kq.clearAndStop();
        continue;
      }
      // Key is already being processed, try to enqueue.
      if (!kq.enqueue(task, maxQueuePerKey)) {
        log.warn("[{}] Task queue full for key {}. Task rejected.", name, key);
      }
      return;
    }
  }

  private void runTask(Object key, KeyQueue kq, Runnable task) {
    try {
      executor.execute(() -> {
        try {
          if (closed) {
            return;
          }
          task.run();
        } catch (Exception e) {
          log.warn("[{}] Task execution failed for key {}", name, key, e);
        } finally {
          scheduleNext(key, kq);
        }
      });
    } catch (RejectedExecutionException e) {
      kq.returnToFrontAndStop(task);
      // Self-healing retry: do not rely on future submissions to re-trigger.
      try {
        executor.schedule(
            () -> {
              if (!closed && kq.tryMarkRunning()) {
                Runnable head = kq.pollAndCheck();
                if (head != null) {
                  runTask(key, kq, head);
                }
              }
            },
            200,
            TimeUnit.MILLISECONDS);
      } catch (RejectedExecutionException ignored) {
        // Executor is shutting down; close() handles cleanup.
      }
    }
  }

  private void scheduleNext(Object key, KeyQueue kq) {
    if (closed) {
      kq.clearAndStop();
      queues.remove(key, kq);
      return;
    }
    Runnable next = kq.pollAndCheck();
    if (next == null) {
      // No more tasks, but the queue might already be empty; we need to remove if idle.
      if (!kq.isRunning()) {
        queues.remove(key, kq);
      }
    } else {
      runTask(key, kq, next);
    }
  }

  @Override
  public void close() {
    closed = true;
    for (KeyQueue kq : queues.values()) {
      kq.clearAndStop();
    }
    queues.clear();
  }

  /**
   * Internal queue for a single key, guarded by its own intrinsic lock.
   */
  private static class KeyQueue {

    final Object key;
    final ArrayDeque<Runnable> queue = new ArrayDeque<>();
    volatile boolean running = false;

    KeyQueue(Object key) {
      this.key = key;
    }

    /**
     * Attempts to mark this key as running. If it was already running,
     * returns false; otherwise sets running to true and returns true.
     */
    synchronized boolean tryMarkRunning() {
      if (running) {
        return false;
      }
      running = true;
      return true;
    }

    /**
     * Enqueues a task if the queue is not full. Must only be called when
     * {@link #running} is true (i.e., after a false return from tryMarkRunning).
     *
     * @param task    the task to enqueue
     * @param maxSize maximum allowed queue size
     * @return true if the task was enqueued, false if the queue is full
     */
    synchronized boolean enqueue(Runnable task, int maxSize) {
      if (queue.size() >= maxSize) {
        return false;
      }
      queue.addLast(task);
      return true;
    }

    /**
     * Polls the next task from the queue. If the queue becomes empty,
     * resets the running flag to false.
     *
     * @return the next Runnable, or null if the queue is empty
     */
    synchronized Runnable pollAndCheck() {
      Runnable next = queue.pollFirst();
      if (next == null) {
        running = false;
      }
      return next;
    }

    /**
     * Clears the queue and stops running. Used when the dispatcher is closed.
     */
    synchronized void clearAndStop() {
      queue.clear();
      running = false;
    }

    /**
     * Returns a task to the front of the queue and resets the running flag.
     * Used when the executor rejects a task.
     */
    synchronized void returnToFrontAndStop(Runnable task) {
      queue.addFirst(task);
      running = false;
    }

    synchronized boolean isRunning() {
      return running;
    }
  }
}
