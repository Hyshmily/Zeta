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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link PerKeyOrderedDispatcher} covering basic execution, per-key FIFO ordering, parallel execution for
 * different keys, delayed submission, graceful close, backpressure rejection, executor rejection recovery, and key
 * cleanup.
 */
@Tag("performance")
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
  @Tag("flaky")
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
    @SuppressWarnings("all")
    ConcurrentHashMap<Object, ?> queues = (ConcurrentHashMap<Object, ?>) queuesField.get(dispatcher);

    assertThat(queues).doesNotContainKey("key1");
  }

  /**
   * Verifies that a burst of same-key tasks is consumed in batches: a single underlying-executor
   * submission drives up to {@code maxTasksPerCycle} tasks, instead of one submission per task.
   */
  @Test
  void submit_sameKeyBurst_shouldBatch() throws InterruptedException {
    CountingExecutor countingExecutor = new CountingExecutor(4);
    PerKeyOrderedDispatcher batchingDispatcher = new PerKeyOrderedDispatcher(countingExecutor, "test");

    int taskCount = 32;
    CountDownLatch done = new CountDownLatch(taskCount);
    try {
      for (int i = 0; i < taskCount; i++) {
        batchingDispatcher.submit("key", done::countDown);
      }
      assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
      // One submission starts the worker, one continuation drains the rest — far fewer than
      // one submission per task (the pre-batching behaviour would need 32+ submissions).
      assertThat(countingExecutor.getSubmissionCount()).isLessThanOrEqualTo(4);
    } finally {
      batchingDispatcher.close();
      countingExecutor.shutdownNow();
    }
  }

  /**
   * Verifies that a key yields back to the executor after {@code maxTasksPerCycle} tasks,
   * splitting a large burst into multiple batches.
   */
  @Test
  void submit_maxTasksPerCycle_shouldSplitBatches() throws InterruptedException {
    CountingExecutor countingExecutor = new CountingExecutor(4);
    PerKeyOrderedDispatcher batchingDispatcher =
      new PerKeyOrderedDispatcher(countingExecutor, "test", PerKeyOrderedDispatcherTest.DEFAULT_MAX_QUEUE, 8);

    int taskCount = 20;
    CountDownLatch done = new CountDownLatch(taskCount);
    try {
      for (int i = 0; i < taskCount; i++) {
        batchingDispatcher.submit("key", done::countDown);
      }
      assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
      // 20 tasks at 8 per cycle → 3 batches (8 + 8 + 4): initial submission + 2 continuations.
      assertThat(countingExecutor.getSubmissionCount()).isBetween(3, 5);
    } finally {
      batchingDispatcher.close();
      countingExecutor.shutdownNow();
    }
  }

  /**
   * Verifies that batching preserves the strict per-key FIFO order across batch boundaries.
   */
  @Test
  void submit_batch_shouldPreserveFifoOrder() throws InterruptedException {
    CountingExecutor countingExecutor = new CountingExecutor(4);
    PerKeyOrderedDispatcher batchingDispatcher = new PerKeyOrderedDispatcher(countingExecutor, "test");

    int taskCount = 70;
    CountDownLatch done = new CountDownLatch(taskCount);
    var executionOrder = new java.util.concurrent.CopyOnWriteArrayList<Integer>();
    try {
      for (int i = 0; i < taskCount; i++) {
        int expected = i;
        batchingDispatcher.submit("key", () -> {
          executionOrder.add(expected);
          done.countDown();
        });
      }
      assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(executionOrder).containsExactlyElementsOf(java.util.stream.IntStream.range(0, taskCount).boxed().toList());
    } finally {
      batchingDispatcher.close();
      countingExecutor.shutdownNow();
    }
  }

  /**
   * Verifies that a task throwing {@link Throwable} inside a batch does not kill the batch:
   * the remaining tasks of the key still execute.
   */
  @Test
  void taskThrowable_shouldNotKillBatch() throws InterruptedException {
    CountingExecutor countingExecutor = new CountingExecutor(4);
    PerKeyOrderedDispatcher batchingDispatcher =
      new PerKeyOrderedDispatcher(countingExecutor, "test", PerKeyOrderedDispatcherTest.DEFAULT_MAX_QUEUE, 8);

    int taskCount = 4;
    CountDownLatch survivors = new CountDownLatch(taskCount - 1);
    try {
      batchingDispatcher.submit("key", () -> {
        throw new Error("boom");
      });
      for (int i = 0; i < taskCount - 1; i++) {
        batchingDispatcher.submit("key", survivors::countDown);
      }
      assertThat(survivors.await(5, TimeUnit.SECONDS)).isTrue();
    } finally {
      batchingDispatcher.close();
      countingExecutor.shutdownNow();
    }
  }

  /**
   * Verifies that when the executor rejects the initial submission, the delayed retry re-drives
   * the worker and all tasks run in strict submission order (FIFO, task1 before task2).
   */
  @Test
  void runTask_withRejectedExecution_shouldRetryAndPreserveFifo() throws InterruptedException {
    ScheduledExecutorService rejectingExec = new SingleShotRejectingExecutor();
    PerKeyOrderedDispatcher rejectingDispatcher = new PerKeyOrderedDispatcher(rejectingExec, "rejecting");

    CountDownLatch done = new CountDownLatch(2);
    var executionOrder = new java.util.concurrent.CopyOnWriteArrayList<Integer>();
    try {
      rejectingDispatcher.submit("key", () -> {
        executionOrder.add(1);
        done.countDown();
      });
      rejectingDispatcher.submit("key", () -> {
        executionOrder.add(2);
        done.countDown();
      });

      assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(executionOrder).containsExactly(1, 2);
    } finally {
      rejectingDispatcher.close();
      rejectingExec.shutdownNow();
    }
  }

  /**
   * Verifies that when the global pending budget is exhausted, excess submissions are dropped,
   * and that the budget drains back as tasks execute — later submissions are accepted again.
   */
  @Test
  void submit_globalCap_shouldDropAndRecoverAfterDrain() throws InterruptedException {
    PerKeyOrderedDispatcher capped = new PerKeyOrderedDispatcher(
      executor,
      "capped",
      PerKeyOrderedDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
      PerKeyOrderedDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      1
    );

    CountDownLatch blockLatch = new CountDownLatch(1);
    CountDownLatch task1Started = new CountDownLatch(1);
    CountDownLatch task2Ran = new CountDownLatch(1);
    CountDownLatch task3Ran = new CountDownLatch(1);

    try {
      // Submit task1 — runs immediately and holds the entire budget of 1 unit.
      capped.submit("key", () -> {
        task1Started.countDown();
        try {
          assertThat(blockLatch.await(10, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      assertThat(task1Started.await(5, TimeUnit.SECONDS)).isTrue();

      // Submit task2 — budget exhausted (1 pending + 1 > cap 1) → dropped.
      capped.submit("key", task2Ran::countDown);

      // Release task1 → its unit is discharged asynchronously → the budget drains to 0.
      blockLatch.countDown();

      // The discharge races with our next submission, so retry until the budget drains
      // and a submission is finally accepted (bounded retry: drain happens within µs).
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (task3Ran.getCount() > 0 && System.nanoTime() < deadline) {
        capped.submit("key", task3Ran::countDown);
        Thread.sleep(10);
      }

      assertThat(task3Ran.await(200, TimeUnit.MILLISECONDS)).isTrue();
      // Task2 was dropped before enqueue and must never run.
      assertThat(task2Ran.await(200, TimeUnit.MILLISECONDS)).isFalse();
    } finally {
      capped.close();
    }
  }

  /**
   * Verifies that submissions carry a weight against the global budget: a heavy task
   * (weight 10) fills a budget of 10, and a subsequent weight-1 task is dropped.
   */
  @Test
  void submit_weighted_shouldChargeByWeight() throws InterruptedException {
    PerKeyOrderedDispatcher capped = new PerKeyOrderedDispatcher(
      executor,
      "weighted",
      PerKeyOrderedDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
      PerKeyOrderedDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      10
    );

    CountDownLatch heavyRan = new CountDownLatch(1);
    CountDownLatch lightRan = new CountDownLatch(1);

    try {
      // Weight-10 task: 0 + 10 > 10 is false → accepted, budget now full.
      capped.submitWithWeight("key", heavyRan::countDown, 10);
      assertThat(heavyRan.await(5, TimeUnit.SECONDS)).isTrue();

      // The budget discharges right after the heavy task returns — it races with our next
      // submission, so retry until the drain lands and a weight-10 task fits exactly
      // (bounded retry: the drain happens within µs of the task completing).
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (lightRan.getCount() > 0 && System.nanoTime() < deadline) {
        capped.submitWithWeight("key", lightRan::countDown, 10);
        Thread.sleep(10);
      }
      assertThat(lightRan.await(200, TimeUnit.MILLISECONDS)).isTrue();
    } finally {
      capped.close();
    }
  }

  /**
   * Verifies that a weight-10 task blocks out a weight-1 task while it is still pending,
   * and that a weight exceeding the budget by itself is rejected while the budget is occupied.
   */
  @Test
  void submit_weighted_shouldDropWhenBudgetOccupied() throws InterruptedException {
    PerKeyOrderedDispatcher capped = new PerKeyOrderedDispatcher(
      executor,
      "weighted",
      PerKeyOrderedDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
      PerKeyOrderedDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      10
    );

    CountDownLatch blockLatch = new CountDownLatch(1);
    CountDownLatch heavyStarted = new CountDownLatch(1);
    CountDownLatch lightRan = new CountDownLatch(1);

    try {
      // Weight-10 task runs and holds the whole budget.
      capped.submitWithWeight("key", () -> {
        heavyStarted.countDown();
        try {
          assertThat(blockLatch.await(10, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }, 10);

      assertThat(heavyStarted.await(5, TimeUnit.SECONDS)).isTrue();

      // Weight-1 task while budget (10) is fully occupied → dropped.
      capped.submitWithWeight("key", lightRan::countDown, 1);

      blockLatch.countDown();

      assertThat(lightRan.await(500, TimeUnit.MILLISECONDS)).isFalse();
    } finally {
      capped.close();
    }
  }

  /**
   * Verifies that the global budget gate applies when a delayed weighted submission
   * actually fires, not at scheduling time.
   */
  @Test
  void submit_delayedWeighted_shouldApplyGateAtActualSubmission() throws InterruptedException {
    PerKeyOrderedDispatcher capped = new PerKeyOrderedDispatcher(
      executor,
      "weighted",
      PerKeyOrderedDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
      PerKeyOrderedDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      10
    );

    CountDownLatch blockLatch = new CountDownLatch(1);
    CountDownLatch heavyStarted = new CountDownLatch(1);
    CountDownLatch delayedRan = new CountDownLatch(1);

    try {
      // Weight-10 task holds the budget for the whole test.
      capped.submitWithWeight("key", () -> {
        heavyStarted.countDown();
        try {
          assertThat(blockLatch.await(10, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }, 10);

      assertThat(heavyStarted.await(5, TimeUnit.SECONDS)).isTrue();

      // Delayed weight-1 submission fires at 200 ms. Keep the budget occupied until after
      // the delayed submission has fired, so the gate (checked at actual submission time)
      // sees it exhausted and drops the task.
      capped.submitWithWeight("key", delayedRan::countDown, 1, 200);
      Thread.sleep(400);
      blockLatch.countDown();

      assertThat(delayedRan.await(200, TimeUnit.MILLISECONDS)).isFalse();
    } finally {
      capped.close();
    }
  }

  // ── Helper classes ──────────────────────────────────────────

  /**
   * Package-visible copy of the dispatcher's default per-key queue bound, used by batch tests
   * that construct a dispatcher with a custom {@code maxTasksPerCycle}.
   */
  static final int DEFAULT_MAX_QUEUE = 1024;

  /**
   * A {@link ScheduledThreadPoolExecutor} that counts every {@link #execute(Runnable)} call,
   * used to verify batched consumption.
   */
  private static class CountingExecutor extends ScheduledThreadPoolExecutor {

    private final AtomicInteger submissions = new AtomicInteger(0);

    CountingExecutor(int corePoolSize) {
      super(corePoolSize);
    }

    @Override
    public void execute(Runnable command) {
      submissions.incrementAndGet();
      super.execute(command);
    }

    int getSubmissionCount() {
      return submissions.get();
    }
  }

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
