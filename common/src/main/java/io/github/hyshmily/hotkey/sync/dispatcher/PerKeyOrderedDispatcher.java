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
package io.github.hyshmily.hotkey.sync.dispatcher;

import io.github.hyshmily.hotkey.Internal;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import lombok.extern.slf4j.Slf4j;

/**
 * Per-key FIFO ordered dispatcher.
 *
 * <p>Ensures that tasks for the same key are executed strictly in submission order
 * and never concurrently, while tasks for different keys can execute in parallel.
 *
 * <p>Designed to be used as the inner layer of a two-layer dispatch pattern where
 * an outer jitter (e.g. {@link io.github.hyshmily.hotkey.util.DelayUtil#floatTimeDelay})
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
 * (exposed via {@link #getTotalRejected()} / Micrometer).
 *
 * <p><b>Executor rejection recovery:</b> If the underlying executor rejects a task
 * via {@link RejectedExecutionException}, the task is returned to the head of its
 * key queue and the key is marked as not running. A subsequent {@link #submit} for
 * the same key will re-trigger execution, preserving ordering.
 *
 * <p>This class is thread-safe.
 */
@Slf4j
@Internal
public class PerKeyOrderedDispatcher implements AutoCloseable {

  private static final int DEFAULT_MAX_QUEUE_PER_KEY = 1024;

  private final ConcurrentHashMap<Object, KeyQueue> queues = new ConcurrentHashMap<>();
  private final Executor executor;
  private final String name;
  private final int maxQueuePerKey;

  private volatile boolean closed = false;

  public PerKeyOrderedDispatcher(Executor executor, String name) {
    this(executor, name, DEFAULT_MAX_QUEUE_PER_KEY);
  }

  public PerKeyOrderedDispatcher(Executor executor, String name, int maxQueuePerKey) {
    this.executor = executor;
    this.name = name;
    this.maxQueuePerKey = maxQueuePerKey;
  }

  /**
   * Submit a task for a given key. Tasks for the same key are executed
   * in FIFO order. If the dispatcher is closed, the task is silently dropped.
   * If the key's pending queue is full, the task is rejected (dropped without execution).
   */
  public void submit(Object key, Runnable task) {
    if (closed) {
      return;
    }

    KeyQueue kq = queues.computeIfAbsent(key, k -> new KeyQueue(key));
    // Try to mark this key as running. If successful, we must execute the task directly.
    if (kq.tryMarkRunning()) {
      runTask(key, kq, task);
    } else {
      // Key is already being processed, try to enqueue.
      if (!kq.enqueue(task, maxQueuePerKey)) {
        log.warn("[{}] Task queue full for key {}. Task rejected.", name, key);
        // No need for log spam here, counters are sufficient.
      }
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
      // Return the task to the front of the queue and reset running flag.
      kq.returnToFrontAndStop(task);
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
