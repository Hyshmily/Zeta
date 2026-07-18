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

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link PerKeyOrderedDispatcher} covering basic execution, per-key FIFO ordering, parallel execution for
 * different keys, delayed submission, graceful close, backpressure rejection, executor rejection recovery, and key
 * cleanup.
 */
class PerKeyOrderedDispatcherTest {

  private ScheduledExecutorService executor;
  private PerKeyOrderedDispatcher dispatcher;

  @BeforeEach
  void setUp() {
    executor = Executors.newScheduledThreadPool(4);
    dispatcher = new PerKeyOrderedDispatcher(executor, "test");
  }

  @AfterEach
  void tearDown() {
    dispatcher.close();
    executor.shutdownNow();
  }

  /**
   * Verifies that a submitted task is executed at least once.
   */
  @Test
  void submit_basic_shouldExecuteTask() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    dispatcher.submit("key", latch::countDown);
    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
  }

  /**
   * Verifies that two tasks submitted for the same key execute in FIFO order (the first task completes before the
   * second starts).
   */
  @Test
  void submit_sameKey_shouldExecuteInFifoOrder() throws InterruptedException {
    AtomicInteger order = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(2);

    dispatcher.submit("key", () -> {
      assertThat(order.getAndIncrement()).isEqualTo(0);
      latch.countDown();
    });

    dispatcher.submit("key", () -> {
      assertThat(order.getAndIncrement()).isEqualTo(1);
      latch.countDown();
    });

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
  }

  /**
   * Verifies that tasks for different keys can execute concurrently. A blocking task for key1 should not prevent a
   * task for key2 from running.
   */
  @Test
  void submit_differentKeys_shouldExecuteInParallel() throws InterruptedException {
    CountDownLatch task1Running = new CountDownLatch(1);
    CountDownLatch task1Block = new CountDownLatch(1);
    CountDownLatch task2Executed = new CountDownLatch(1);

    dispatcher.submit("key1", () -> {
      task1Running.countDown();
      try {
        assertThat(task1Block.await(10, TimeUnit.SECONDS)).isTrue();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });

    assertThat(task1Running.await(5, TimeUnit.SECONDS)).isTrue();

    dispatcher.submit("key2", task2Executed::countDown);

    assertThat(task2Executed.await(5, TimeUnit.SECONDS)).isTrue();

    task1Block.countDown();
  }

  /**
   * Verifies that a task submitted with a positive delay does not execute immediately.
   */
  @Test
  void submit_withDelay_shouldNotExecuteImmediately() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);

    dispatcher.submit("key", latch::countDown, 200);

    assertThat(latch.await(100, TimeUnit.MILLISECONDS)).isFalse();
    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
  }

  /**
   * Verifies that submitting a task after {@code close()} is called silently drops the task.
   */
  @Test
  void submit_afterClose_shouldDropTask() throws InterruptedException {
    dispatcher.close();

    CountDownLatch latch = new CountDownLatch(1);
    dispatcher.submit("key", latch::countDown);

    assertThat(latch.await(500, TimeUnit.MILLISECONDS)).isFalse();
  }

  /**
   * Verifies that when a key's pending queue reaches the maximum capacity, excess submissions are silently rejected
   * without throwing.
   */
  @Test
  void submit_keyQueueFull_shouldRejectTask() throws InterruptedException {
    dispatcher = new PerKeyOrderedDispatcher(executor, "test", 1);

    CountDownLatch blockLatch = new CountDownLatch(1);
    CountDownLatch task1Started = new CountDownLatch(1);
    CountDownLatch task2Ran = new CountDownLatch(1);
    CountDownLatch task3Ran = new CountDownLatch(1);

    // Submit task1 for key1 — starts running immediately
    dispatcher.submit("key1", () -> {
      task1Started.countDown();
      try {
        assertThat(blockLatch.await(10, TimeUnit.SECONDS)).isTrue();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });

    assertThat(task1Started.await(5, TimeUnit.SECONDS)).isTrue();

    // Submit task2 — queued (queue now full, maxQueuePerKey=1)
    dispatcher.submit("key1", task2Ran::countDown);

    // Submit task3 — rejected silently (queue full)
    dispatcher.submit("key1", task3Ran::countDown);

    // Release task1, which allows task2 to run
    blockLatch.countDown();

    assertThat(task2Ran.await(5, TimeUnit.SECONDS)).isTrue();
    // Task3 should not have run
    assertThat(task3Ran.await(500, TimeUnit.MILLISECONDS)).isFalse();
  }

  /**
   * Verifies that a delayed submission is dropped when the executor has been shut down.
   */
  @Test
  void submit_withDelay_executorShutdown_shouldDropScheduled() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);

    executor.shutdown();

    dispatcher.submit("key", latch::countDown, 50);

    assertThat(latch.await(500, TimeUnit.MILLISECONDS)).isFalse();
  }

  /**
   * Verifies that after {@code close()}, no pending tasks execute even if they were already submitted to the
   * executor's internal queue.
   */
  @Test
  void close_shouldStopAllQueues() throws InterruptedException {
    CountDownLatch blockLatch = new CountDownLatch(1);
    CountDownLatch task1Started = new CountDownLatch(1);
    CountDownLatch task2Ran = new CountDownLatch(1);

    // Submit a blocking task for key1
    dispatcher.submit("key1", () -> {
      task1Started.countDown();
      try {
        assertThat(blockLatch.await(10, TimeUnit.SECONDS)).isTrue();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });

    assertThat(task1Started.await(5, TimeUnit.SECONDS)).isTrue();

    // Submit task2 for key2 — its lambda is queued on the executor
    dispatcher.submit("key2", task2Ran::countDown);

    // Close the dispatcher while task1 is still blocking
    dispatcher.close();

    // Release task1 — executor thread picks up task2's lambda but sees closed=true
    blockLatch.countDown();

    // Task2 should never have run
    assertThat(task2Ran.await(1, TimeUnit.SECONDS)).isFalse();
  }

  /**
   * Verifies that when the executor throws {@link RejectedExecutionException}, the task is returned to the front of
   * the queue and the key is no longer marked as running, allowing a subsequent submission to execute it.
   */
  @Test
  void runTask_withRejectedExecution_shouldReturnToFront() throws InterruptedException {
    ScheduledExecutorService rejectingExec = new SingleShotRejectingExecutor();
    PerKeyOrderedDispatcher rejectingDispatcher = new PerKeyOrderedDispatcher(rejectingExec, "rejecting");

    CountDownLatch task1Ran = new CountDownLatch(1);
    CountDownLatch task2Ran = new CountDownLatch(1);

    // Submit task1 — will be rejected and returned to front
    rejectingDispatcher.submit("key", task1Ran::countDown);

    // Submit task2 — should type execution (tryMarkRunning succeeds after rejection reset)
    rejectingDispatcher.submit("key", task2Ran::countDown);

    // Both tasks should eventually complete
    assertThat(task1Ran.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(task2Ran.await(5, TimeUnit.SECONDS)).isTrue();

    rejectingDispatcher.close();
    rejectingExec.shutdownNow();
  }

  /**
   * Verifies that after a single task completes and the queue is empty, the key is removed from the internal map.
   */
  @Test
  void scheduleNext_whenQueueEmpty_shouldRemoveKey() throws InterruptedException, Exception {
    CountDownLatch latch = new CountDownLatch(1);

    dispatcher.submit("key1", latch::countDown);
    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

    // Give scheduleNext time to run and remove the key
    Thread.sleep(100);

    Field queuesField = PerKeyOrderedDispatcher.class.getDeclaredField("queues");
    queuesField.setAccessible(true);
    @SuppressWarnings("unchecked")
    ConcurrentHashMap<Object, ?> queues = (ConcurrentHashMap<Object, ?>) queuesField.get(dispatcher);

    assertThat(queues).doesNotContainKey("key1");
  }

  // ── Helper classes ──────────────────────────────────────────

  /**
   * A {@link ScheduledThreadPoolExecutor} that throws {@link RejectedExecutionException} on its first
   * {@link #execute(Runnable)} call, then delegates normally for all subsequent calls.
   */
  private static class SingleShotRejectingExecutor extends ScheduledThreadPoolExecutor {

    private boolean rejectNext = true;

    SingleShotRejectingExecutor() {
      super(1);
    }

    @Override
    public void execute(Runnable command) {
      if (rejectNext) {
        rejectNext = false;
        throw new RejectedExecutionException("Simulated rejection for testing");
      }
      super.execute(command);
    }
  }
}
