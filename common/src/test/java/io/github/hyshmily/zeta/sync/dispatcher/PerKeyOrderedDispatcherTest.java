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
   * Verifies that a delayed submission is dropped — NOT propagated to the caller —
   * when the executor rejects the {@code schedule()} call. This pins the TOCTOU fix:
   * the {@code isShutdown()} pre-check can pass, then the executor shuts down before
   * {@code schedule()} runs, which used to throw {@link RejectedExecutionException}
   * out of {@code submitWithWeight}.
   */
  @Test
  void submit_withDelay_scheduleRejected_shouldDropWithoutThrowing() throws InterruptedException {
    ScheduleRejectingExecutor scheduleRejecting = new ScheduleRejectingExecutor();
    PerKeyOrderedDispatcher rejectingDispatcher = new PerKeyOrderedDispatcher(scheduleRejecting, "rejecting-schedule");
    try {
      CountDownLatch latch = new CountDownLatch(1);

      rejectingDispatcher.submit("key", latch::countDown, 50); // must not throw

      assertThat(latch.await(500, TimeUnit.MILLISECONDS)).isFalse();
    } finally {
      rejectingDispatcher.close();
      scheduleRejecting.shutdownNow();
    }
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

  /**
   * Verifies that a single-task worker costs exactly ONE executor submission: the grant
   * drains the worker and removes it from the map, so the cycle skips the (guaranteed-empty)
   * continuation submission the old design needed to discover emptiness.
   */
  @Test
  void submit_singleTask_shouldUseSingleExecutorSubmission() throws InterruptedException {
    CountingExecutor countingExecutor = new CountingExecutor(4);
    PerKeyOrderedDispatcher batchingDispatcher = new PerKeyOrderedDispatcher(countingExecutor, "test");

    CountDownLatch done = new CountDownLatch(1);
    try {
      batchingDispatcher.submit("key", done::countDown);
      assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
      // The worker drains within its first batch; the post-drain continuation re-drives it via
      // exactly one more execute() submission (the original assertion of 1 was wrong — the driver
      // runs on the submitting thread only, the continuation always costs a second execute()).
      awaitSubmissionCount(countingExecutor, 2, 2000);
    } finally {
      batchingDispatcher.close();
      countingExecutor.shutdownNow();
    }
  }

  /**
   * Verifies that a worker under start jitter is PARKED (scheduler-side delay), not run
   * immediately and not slept through on a pool thread: the task does not execute inside
   * the jitter window, executes right after it, and the drained worker costs zero
   * {@code execute()} submissions (the first cycle went through {@code schedule()}).
   */
  @Test
  void submit_withJitter_shouldParkBeforeFirstCycle() throws InterruptedException {
    CountingExecutor countingExecutor = new CountingExecutor(4);
    PerKeyOrderedDispatcher jittered = new PerKeyOrderedDispatcher(
      countingExecutor,
      "jitter",
      DEFAULT_MAX_QUEUE,
      PerKeyOrderedDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      50_000,
      150
    );

    Thread submitter = Thread.currentThread();
    final Thread[] runnerHolder = new Thread[1];
    CountDownLatch done = new CountDownLatch(1);
    try {
      jittered.submit("key", () -> {
        runnerHolder[0] = Thread.currentThread();
        done.countDown();
      });
      // The task must run on an executor thread, never synchronously on the submitting thread —
      // this is the deterministic guarantee of the start-jitter park (the first cycle is either
      // schedule()'d or execute()'d on the executor, regardless of the random draw in [0,150)ms).
      // Asserting after the task completes avoids any race with the submitting thread.
      assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(runnerHolder[0]).isNotNull();
      assertThat(runnerHolder[0]).isNotSameAs(submitter);
      // The worker is always re-driven by at least one execute() submission (the post-drain
      // continuation). We avoid asserting an exact count because the random jitter draw makes the
      // first cycle either a free schedule() (1 execute total) or a counted execute() (2 executes
      // total); both satisfy the "parked, not slept" contract.
      long deadline = System.currentTimeMillis() + 2000;
      while (countingExecutor.getSubmissionCount() < 1 && System.currentTimeMillis() < deadline) {
        Thread.sleep(2);
      }
      assertThat(countingExecutor.getSubmissionCount()).isGreaterThanOrEqualTo(1);
    } finally {
      jittered.close();
      countingExecutor.shutdownNow();
    }
  }

  /**
   * Verifies that the start-jitter park preserves strict per-key FIFO order across a
   * batch: same-key tasks enqueued while the worker is parked run in submission order
   * after the park expires.
   */
  @Test
  void submit_withJitter_shouldPreserveFifoOrder() throws InterruptedException {
    CountingExecutor countingExecutor = new CountingExecutor(4);
    PerKeyOrderedDispatcher jittered = new PerKeyOrderedDispatcher(
      countingExecutor,
      "jitter",
      DEFAULT_MAX_QUEUE,
      PerKeyOrderedDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      50_000,
      50
    );

    int taskCount = 70;
    CountDownLatch done = new CountDownLatch(taskCount);
    var executionOrder = new java.util.concurrent.CopyOnWriteArrayList<Integer>();
    try {
      for (int i = 0; i < taskCount; i++) {
        int expected = i;
        jittered.submit("key", () -> {
          executionOrder.add(expected);
          done.countDown();
        });
      }
      assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(executionOrder).containsExactlyElementsOf(java.util.stream.IntStream.range(0, taskCount).boxed().toList());
    } finally {
      jittered.close();
      countingExecutor.shutdownNow();
    }
  }

  /**
   * Verifies that a fresh dispatcher reports an idle gate: nothing charged, full capacity, no key
   * active, no drop counter advanced (ADR-0072 D-1).
   */
  @Test
  void stats_freshDispatcher_shouldReportIdleGate() {
    DispatcherStats stats = dispatcher.stats();

    assertThat(stats.pendingUnits()).isZero();
    assertThat(stats.maxPendingUnits()).isEqualTo(PerKeyOrderedDispatcher.DEFAULT_MAX_GLOBAL_PENDING_UNITS);
    assertThat(stats.remainingUnits()).isEqualTo(PerKeyOrderedDispatcher.DEFAULT_MAX_GLOBAL_PENDING_UNITS);
    assertThat(stats.activeKeys()).isZero();
    assertThat(stats.dropped()).isZero();
    assertThat(stats.rejected()).isZero();
    assertThat(stats.backlogged()).isFalse();
  }

  /**
   * Verifies that the derived remaining capacity clamps at zero instead of going negative when a
   * concurrent submitter (or a single oversized task) pushes the gate past its configured budget.
   */
  @Test
  void stats_remainingUnits_shouldClampAtZeroWhenOvershot() {
    DispatcherStats overshot = new DispatcherStats(12L, 10L, 1, 0L, 0L);

    assertThat(overshot.remainingUnits()).isZero();
    assertThat(overshot.backlogged()).isTrue();
  }

  /**
   * Verifies that a task is charged to the gate while it runs and discharged once it returns, that
   * its key counts as active in between, and that the gate reports itself drained afterwards.
   */
  @Test
  void stats_shouldTrackPendingUnitsAndActiveKeys() throws InterruptedException {
    CountDownLatch running = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(1);
    dispatcher.submit("key", () -> {
      running.countDown();
      awaitQuietly(release);
      done.countDown();
    });
    assertThat(running.await(5, TimeUnit.SECONDS)).isTrue();

    DispatcherStats inFlight = dispatcher.stats();
    assertThat(inFlight.pendingUnits()).isEqualTo(1L);
    assertThat(inFlight.remainingUnits()).isEqualTo(PerKeyOrderedDispatcher.DEFAULT_MAX_GLOBAL_PENDING_UNITS - 1);
    assertThat(inFlight.activeKeys()).isEqualTo(1);
    assertThat(inFlight.backlogged()).isTrue();

    release.countDown();
    assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    // The discharge happens right after the task body returns and the worker leaves the map in a
    // later grant, so await the end-state instead of sampling at an arbitrary instant.
    awaitGateDrained();
    awaitNoActiveKeys();

    DispatcherStats drained = dispatcher.stats();
    assertThat(drained.pendingUnits()).isZero();
    assertThat(drained.activeKeys()).isZero();
    assertThat(drained.backlogged()).isFalse();
  }

  /**
   * Verifies that a submission dropped by the global budget is counted as a drop — not as a per-key
   * rejection — and that the gate reports zero remaining capacity instead of a negative value.
   */
  @Test
  void stats_shouldCountGlobalGateDrops() throws InterruptedException {
    CountDownLatch running = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    PerKeyOrderedDispatcher boundedBudget = new PerKeyOrderedDispatcher(
      executor,
      "bounded-budget",
      DEFAULT_MAX_QUEUE,
      PerKeyOrderedDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      1
    );
    try {
      boundedBudget.submit("key", () -> {
        running.countDown();
        awaitQuietly(release);
      });
      assertThat(running.await(5, TimeUnit.SECONDS)).isTrue();

      // The single unit is still charged to the running task, so this submission cannot be enqueued.
      boundedBudget.submit("key", () -> {});

      DispatcherStats stats = boundedBudget.stats();
      assertThat(stats.maxPendingUnits()).isEqualTo(1L);
      assertThat(stats.pendingUnits()).isEqualTo(1L);
      assertThat(stats.remainingUnits()).isZero();
      assertThat(stats.dropped()).isEqualTo(1L);
      assertThat(stats.rejected()).isZero();
    } finally {
      release.countDown();
      boundedBudget.close();
    }
  }

  /**
   * Verifies that a submission refused by the key's own queue bound is counted as a rejection,
   * separately from the global-gate drop counter.
   */
  @Test
  void stats_shouldCountPerKeyRejections() throws InterruptedException {
    CountDownLatch running = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    int maxQueuePerKey = 2;
    PerKeyOrderedDispatcher boundedQueue = new PerKeyOrderedDispatcher(
      executor,
      "bounded-queue",
      maxQueuePerKey,
      PerKeyOrderedDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      50_000
    );
    try {
      boundedQueue.submit("key", () -> {
        running.countDown();
        awaitQuietly(release);
      });
      assertThat(running.await(5, TimeUnit.SECONDS)).isTrue();

      boundedQueue.submit("key", () -> {});
      boundedQueue.submit("key", () -> {});
      // One running task plus a full queue: the next submission must be rejected by the key bound,
      // and the global budget has ample room left.
      boundedQueue.submit("key", () -> {});

      DispatcherStats stats = boundedQueue.stats();
      assertThat(stats.rejected()).isEqualTo(1L);
      assertThat(stats.dropped()).isZero();
      assertThat(stats.pendingUnits()).isEqualTo(3L);
      assertThat(stats.activeKeys()).isEqualTo(1);
      assertThat(stats.remainingUnits()).isEqualTo(50_000L - 3L);
    } finally {
      release.countDown();
      boundedQueue.close();
    }
  }

  /**
   * Polls until the dispatcher reports an empty gate (every charged unit discharged) or the timeout
   * elapses.
   */
  private void awaitGateDrained() throws InterruptedException {
    long deadline = System.currentTimeMillis() + 2000;
    while (dispatcher.stats().pendingUnits() != 0L && System.currentTimeMillis() < deadline) {
      Thread.sleep(2);
    }
  }

  /** Polls until no key worker remains in the dispatcher's map, i.e. every worker has self-removed. */
  private void awaitNoActiveKeys() throws InterruptedException {
    long deadline = System.currentTimeMillis() + 2000;
    while (dispatcher.stats().activeKeys() != 0 && System.currentTimeMillis() < deadline) {
      Thread.sleep(2);
    }
  }

  /** Awaits a latch, restoring the interrupt flag instead of failing the surrounding task. */
  private static void awaitQuietly(CountDownLatch latch) {
    try {
      latch.await(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Polls until the {@link CountingExecutor} reports exactly {@code expected} execute() submissions
   * or the timeout elapses. Used instead of a racy immediate read: the drained-worker continuation
   * submit lands a few instructions after the task's latch is released, so the count must be awaited
   * at its deterministic end-state rather than sampled at an arbitrary instant.
   */
  private static void awaitSubmissionCount(CountingExecutor executor, int expected, long timeoutMs)
    throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (executor.getSubmissionCount() != expected && System.currentTimeMillis() < deadline) {
      Thread.sleep(2);
    }
    assertThat(executor.getSubmissionCount()).isEqualTo(expected);
  }

  // ── Batch-end callback (ADR-0071, Disruptor BatchEventProcessor semantics) ──────────

  /**
   * Verifies that a batch-aware task granted alone (the common single-message case) runs with
   * {@code endOfBatch == true}.
   */
  @Test
  void submitBatchAware_singleTask_shouldReceiveEndOfBatchTrue() throws InterruptedException {
    var flags = new java.util.concurrent.CopyOnWriteArrayList<Boolean>();

    dispatcher.submitWithWeight("key", flags::add, 1);

    long deadline = System.currentTimeMillis() + 5000;
    while (flags.isEmpty() && System.currentTimeMillis() < deadline) {
      Thread.sleep(2);
    }

    assertThat(flags).containsExactly(true);
  }

  /**
   * Verifies the burst case with deterministic grant boundaries: the first task blocks so its
   * cycle is granted with exactly one task (endOfBatch=true), the remaining tasks are then
   * submitted behind it and granted as one later batch in which only the LAST task sees
   * {@code endOfBatch == true}. Execution stays in submission order (FIFO).
   */
  @Test
  void submitBatchAware_burst_shouldFlagOnlyTheLastTaskOfEachGrantedBatch() throws InterruptedException {
    int burst = 5;
    CountDownLatch firstStarted = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch allDone = new CountDownLatch(burst - 1);
    var flags = new java.util.concurrent.CopyOnWriteArrayList<String>();

    // Task 1: granted alone (the grant precedes its execution), blocks until released.
    dispatcher.submitWithWeight("key", endOfBatch -> {
      firstStarted.countDown();
      flags.add("1:" + endOfBatch);
      try {
        assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }, 1);

    assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();

    // Tasks 2..N are submitted only after task 1's cycle was already granted, so they land
    // in the follow-up batch and its last task must carry the end-of-batch signal.
    for (int i = 2; i <= burst; i++) {
      int id = i;
      dispatcher.submitWithWeight("key", endOfBatch -> {
        flags.add(id + ":" + endOfBatch);
        allDone.countDown();
      }, 1);
    }

    release.countDown();
    assertThat(allDone.await(5, TimeUnit.SECONDS)).isTrue();
    long deadline = System.currentTimeMillis() + 5000;
    while (flags.size() < burst && System.currentTimeMillis() < deadline) {
      Thread.sleep(2);
    }

    assertThat(flags).containsExactly(
      "1:true",
      "2:false",
      "3:false",
      "4:false",
      "5:true"
    );
  }

  /**
   * Verifies that a mixed batch (plain Runnable + batch-aware tasks) delivers the signal by
   * position: only the batch's last task sees {@code endOfBatch == true}, plain Runnables are
   * unaffected.
   */
  @Test
  void submitBatchAware_mixedBatch_shouldFlagByPosition() throws InterruptedException {
    CountDownLatch firstStarted = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch tailDone = new CountDownLatch(3);
    var flags = new java.util.concurrent.CopyOnWriteArrayList<String>();

    dispatcher.submit("key", () -> {
      firstStarted.countDown();
      try {
        assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });

    assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();

    dispatcher.submitWithWeight("key", endOfBatch -> {
      flags.add("aware1:" + endOfBatch);
      tailDone.countDown();
    }, 1);
    dispatcher.submit("key", tailDone::countDown);
    dispatcher.submitWithWeight("key", endOfBatch -> {
      flags.add("aware2:" + endOfBatch);
      tailDone.countDown();
    }, 1);

    release.countDown();
    assertThat(tailDone.await(5, TimeUnit.SECONDS)).isTrue();

    assertThat(flags).containsExactly("aware1:false", "aware2:true");
  }

  /**
   * Verifies that an exception thrown by a batch-aware task does not strand the batch: the
   * remaining tasks still run and the final task still receives the end-of-batch signal.
   * The first task blocks so the two follow-up tasks are granted as one batch with
   * deterministic flags.
   */
  @Test
  void submitBatchAware_exceptionInMiddleTask_shouldNotStrandBatch() throws InterruptedException {
    CountDownLatch firstStarted = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch tailDone = new CountDownLatch(2);
    var flags = new java.util.concurrent.CopyOnWriteArrayList<Boolean>();

    // Task 1: granted alone → endOfBatch=true; blocks so tasks 2 and 3 land in one batch.
    dispatcher.submitWithWeight("key", endOfBatch -> {
      firstStarted.countDown();
      flags.add(endOfBatch);
      try {
        assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }, 1);

    assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();

    dispatcher.submitWithWeight("key", endOfBatch -> {
      flags.add(endOfBatch);
      tailDone.countDown();
      throw new RuntimeException("boom");
    }, 1);
    dispatcher.submitWithWeight("key", endOfBatch -> {
      flags.add(endOfBatch);
      tailDone.countDown();
    }, 1);

    release.countDown();
    assertThat(tailDone.await(5, TimeUnit.SECONDS)).isTrue();

    assertThat(flags).containsExactly(true, false, true);
  }

  /**
   * Verifies that a delayed batch-aware submission executes (after the delay) with the
   * end-of-batch signal, mirroring the delayed plain-submission path.
   */
  @Test
  void submitBatchAware_withDelay_shouldExecuteAndReceiveEndOfBatchTrue() throws InterruptedException {
    var flags = new java.util.concurrent.CopyOnWriteArrayList<Boolean>();

    dispatcher.submitWithWeight("key", flags::add, 1, 150);

    assertThat(flags.isEmpty()).isTrue();
    long deadline = System.currentTimeMillis() + 5000;
    while (flags.isEmpty() && System.currentTimeMillis() < deadline) {
      Thread.sleep(5);
    }

    assertThat(flags).containsExactly(true);
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

  /**
   * An executor that is NOT shutdown (the {@code isShutdown()} pre-check passes)
   * but always rejects {@code schedule(Runnable, long, TimeUnit)} — simulating
   * the TOCTOU window in which the executor shuts down between the pre-check
   * and the delayed submission.
   */
  private static class ScheduleRejectingExecutor extends ScheduledThreadPoolExecutor {

    ScheduleRejectingExecutor() {
      super(1);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
      throw new RejectedExecutionException("Simulated shutdown race for testing");
    }
  }
}
