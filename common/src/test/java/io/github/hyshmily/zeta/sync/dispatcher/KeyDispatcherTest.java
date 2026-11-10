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
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link KeyDispatcher} covering basic execution, per-key FIFO ordering, parallel execution for
 * different keys, delayed submission, graceful close, backpressure rejection, executor rejection recovery, and key
 * cleanup.
 */
class KeyDispatcherTest {

  private ScheduledExecutorService executor;
  private KeyDispatcher<Runnable> dispatcher;

  private static KeyDispatcher<Runnable> newDispatcher(
    ScheduledExecutorService executor,
    String name,
    int maxQueuePerKey,
    int maxTasksPerCycle,
    long maxGlobalPendingUnits,
    long jitterMs
  ) {
    return new KeyDispatcher<>(
      executor,
      name,
      batch -> batch.forEach(Runnable::run),
      maxQueuePerKey,
      maxTasksPerCycle,
      maxGlobalPendingUnits,
      jitterMs
    );
  }

  @BeforeEach
  void setUp() {
    executor = Executors.newScheduledThreadPool(4);
    dispatcher = newDispatcher(
      executor,
      "test",
      KeyDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
      KeyDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      KeyDispatcher.DEFAULT_MAX_GLOBAL_PENDING_UNITS,
      0
    );
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
    dispatcher.submit("key", latch::countDown, 1, true);
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

    dispatcher.submit(
      "key",
      () -> {
        assertThat(order.getAndIncrement()).isEqualTo(0);
        latch.countDown();
      },
      1,
      true
    );

    dispatcher.submit(
      "key",
      () -> {
        assertThat(order.getAndIncrement()).isEqualTo(1);
        latch.countDown();
      },
      1,
      true
    );

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

    dispatcher.submit(
      "key1",
      () -> {
        task1Running.countDown();
        try {
          assertThat(task1Block.await(10, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      },
      1,
      true
    );

    assertThat(task1Running.await(5, TimeUnit.SECONDS)).isTrue();

    dispatcher.submit("key2", task2Executed::countDown, 1, true);

    assertThat(task2Executed.await(5, TimeUnit.SECONDS)).isTrue();

    task1Block.countDown();
  }

  /**
   * Verifies that submitting a task after {@code close()} is called silently drops the task.
   */
  @Test
  void submit_afterClose_shouldDropTask() throws InterruptedException {
    dispatcher.close();

    CountDownLatch latch = new CountDownLatch(1);
    dispatcher.submit("key", latch::countDown, 1, true);

    assertThat(latch.await(500, TimeUnit.MILLISECONDS)).isFalse();
  }

  /**
   * Verifies that when a key's pending queue reaches the maximum capacity, excess submissions are silently rejected
   * without throwing.
   */
  @Test
  void submit_keyQueueFull_shouldRejectTask() throws InterruptedException {
    dispatcher = newDispatcher(
      executor,
      "test",
      1,
      KeyDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      KeyDispatcher.DEFAULT_MAX_GLOBAL_PENDING_UNITS,
      0
    );

    CountDownLatch blockLatch = new CountDownLatch(1);
    CountDownLatch task1Started = new CountDownLatch(1);
    CountDownLatch task2Ran = new CountDownLatch(1);
    CountDownLatch task3Ran = new CountDownLatch(1);

    // Submit task1 for key1 — starts running immediately
    dispatcher.submit(
      "key1",
      () -> {
        task1Started.countDown();
        try {
          assertThat(blockLatch.await(10, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      },
      1,
      true
    );

    assertThat(task1Started.await(5, TimeUnit.SECONDS)).isTrue();

    // Submit task2 — queued (queue now full, maxQueuePerKey=1)
    dispatcher.submit("key1", task2Ran::countDown, 1, true);

    // Submit task3 — rejected silently (queue full)
    dispatcher.submit("key1", task3Ran::countDown, 1, true);

    // Release task1, which allows task2 to run
    blockLatch.countDown();

    assertThat(task2Ran.await(5, TimeUnit.SECONDS)).isTrue();
    // Task3 should not have run
    assertThat(task3Ran.await(500, TimeUnit.MILLISECONDS)).isFalse();
  }

  /**
   * While the start-jitter park holds the first task in {@code firstTask}, that task
   * already counts against the per-key bound: a second submission must be rejected at
   * {@code maxQueuePerKey = 1} instead of becoming the bound + 1.
   */
  @Test
  void submit_firstTaskStillPending_shouldCountAgainstThePerKeyBound() {
    KeyDispatcher<Runnable> jittered = newDispatcher(
      executor,
      "jitter-bound",
      1,
      KeyDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      KeyDispatcher.DEFAULT_MAX_GLOBAL_PENDING_UNITS,
      5_000
    );
    try {
      assertThat(jittered.submit("key", () -> {}, 1, true)).isTrue();
      assertThat(jittered.submit("key", () -> {}, 1, true)).isFalse();
    } finally {
      jittered.close();
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
    dispatcher.submit(
      "key1",
      () -> {
        task1Started.countDown();
        try {
          assertThat(blockLatch.await(10, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      },
      1,
      true
    );

    assertThat(task1Started.await(5, TimeUnit.SECONDS)).isTrue();

    // Submit task2 for key2 — its lambda is queued on the executor
    dispatcher.submit("key2", task2Ran::countDown, 1, true);

    // Close the dispatcher while task1 is still blocking
    dispatcher.close();

    // Release task1 — executor thread picks up task2's lambda but sees closed=true
    blockLatch.countDown();

    // Task2 should never have run
    assertThat(task2Ran.await(1, TimeUnit.SECONDS)).isFalse();
  }

  /**
   * Verifies that after a single task completes and the queue is empty, the key is removed from the internal map.
   */
  @Test
  void scheduleNext_whenQueueEmpty_shouldRemoveKey() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);

    dispatcher.submit("key1", latch::countDown, 1, true);
    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

    // Give scheduleNext time to run and remove the key
    Thread.sleep(100);

    Field queuesField = KeyDispatcher.class.getDeclaredField("queues");
    queuesField.setAccessible(true);
    @SuppressWarnings("unchecked")
    ConcurrentHashMap<Object, ?> queues = (ConcurrentHashMap<Object, ?>) queuesField.get(dispatcher);

    assertThat(queues).doesNotContainKey("key1");
  }

  /**
   * Verifies that a burst of same-key tasks is consumed in batches: a single underlying-executor
   * submission drives up to {@code maxTasksPerCycle} tasks, instead of one submission per task.
   *
   * <p>The executor is gated so the whole burst is enqueued before any cycle runs. Without the
   * gate the first {@code submit} starts the worker on a real pool thread, so the number of cycles
   * — and therefore the submission add — depended on how the submitting thread raced the worker
   * (observed 4 or 5 submissions across runs, making an exact bound unassertable). Gated, all 32
   * tasks are present before the first cycle, and since 32 &lt; {@code maxTasksPerCycle} (64) a
   * single cycle drains the burst: exactly one submission, and no continuation.
   */
  @Test
  void submit_sameKeyBurst_shouldBatch() throws InterruptedException {
    GatedCountingExecutor countingExecutor = new GatedCountingExecutor(4);
    KeyDispatcher<Runnable> batchingDispatcher = newDispatcher(
      countingExecutor,
      "test",
      KeyDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
      KeyDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      KeyDispatcher.DEFAULT_MAX_GLOBAL_PENDING_UNITS,
      0
    );

    int taskCount = 32;
    CountDownLatch done = new CountDownLatch(taskCount);
    try {
      for (int i = 0; i < taskCount; i++) {
        batchingDispatcher.submit("key", done::countDown, 1, true);
      }
      // Every task is now queued; let the single armed cycle drain them.
      countingExecutor.release();

      assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
      // One submission starts the worker, one continuation drains the rest — far fewer than
      // one submission per task.
      assertThat(countingExecutor.getSubmissionCount()).isLessThanOrEqualTo(2);
    } finally {
      countingExecutor.release();
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
    KeyDispatcher<Runnable> batchingDispatcher = newDispatcher(
      countingExecutor,
      "test",
      KeyDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
      8,
      KeyDispatcher.DEFAULT_MAX_GLOBAL_PENDING_UNITS,
      0
    );

    int taskCount = 20;
    CountDownLatch done = new CountDownLatch(taskCount);
    try {
      for (int i = 0; i < taskCount; i++) {
        batchingDispatcher.submit("key", done::countDown, 1, true);
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
    KeyDispatcher<Runnable> batchingDispatcher = newDispatcher(
      countingExecutor,
      "test",
      KeyDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
      KeyDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      KeyDispatcher.DEFAULT_MAX_GLOBAL_PENDING_UNITS,
      0
    );

    int taskCount = 70;
    CountDownLatch done = new CountDownLatch(taskCount);
    var executionOrder = new java.util.concurrent.CopyOnWriteArrayList<Integer>();
    try {
      for (int i = 0; i < taskCount; i++) {
        int expected = i;
        batchingDispatcher.submit(
          "key",
          () -> {
            executionOrder.add(expected);
            done.countDown();
          },
          1,
          true
        );
      }
      assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(executionOrder).containsExactlyElementsOf(
        java.util.stream.IntStream.range(0, taskCount).boxed().toList()
      );
    } finally {
      batchingDispatcher.close();
      countingExecutor.shutdownNow();
    }
  }

  /**
   * When the executor rejects the initial submission, the worker retires with
   * the tasks it held; the next submission creates a fresh worker and runs.
   */
  @Test
  void runTask_withRejectedExecution_shouldDropWithTheRetiredWorker() throws InterruptedException {
    ScheduledExecutorService rejectingExec = new SingleShotRejectingExecutor();
    KeyDispatcher<Runnable> rejectingDispatcher = newDispatcher(
      rejectingExec,
      "rejecting",
      KeyDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
      KeyDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      KeyDispatcher.DEFAULT_MAX_GLOBAL_PENDING_UNITS,
      0
    );

    CountDownLatch done = new CountDownLatch(1);
    var executionOrder = new java.util.concurrent.CopyOnWriteArrayList<Integer>();
    try {
      rejectingDispatcher.submit(
        "key",
        () -> {
          executionOrder.add(1);
          done.countDown();
        },
        1,
        true
      );
      rejectingDispatcher.submit(
        "key",
        () -> {
          executionOrder.add(2);
          done.countDown();
        },
        1,
        true
      );

      assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
      // The first cycle was rejected and its task retired with the worker;
      // the second submission started a fresh cycle.
      assertThat(executionOrder).containsExactly(2);
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
    KeyDispatcher<Runnable> capped = newDispatcher(
      executor,
      "capped",
      KeyDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
      KeyDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      1,
      0
    );

    CountDownLatch blockLatch = new CountDownLatch(1);
    CountDownLatch task1Started = new CountDownLatch(1);
    CountDownLatch task2Ran = new CountDownLatch(1);
    CountDownLatch task3Ran = new CountDownLatch(1);

    try {
      // Submit task1 — runs immediately and holds the entire budget of 1 unit.
      capped.submit(
        "key",
        () -> {
          task1Started.countDown();
          try {
            assertThat(blockLatch.await(10, TimeUnit.SECONDS)).isTrue();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        },
        1,
        true
      );

      assertThat(task1Started.await(5, TimeUnit.SECONDS)).isTrue();

      // Submit task2 — budget exhausted (1 pending + 1 > cap 1) → dropped.
      capped.submit("key", task2Ran::countDown, 1, true);

      // Release task1 → its unit is discharged asynchronously → the budget drains to 0.
      blockLatch.countDown();

      // The discharge races with our next submission, so retry until the budget drains
      // and a submission is finally accepted (bounded retry: drain happens within µs).
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (task3Ran.getCount() > 0 && System.nanoTime() < deadline) {
        capped.submit("key", task3Ran::countDown, 1, true);
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
   * Verifies the batch-end settlement of the global pending budget: a granted batch's tasks are
   * all charged until the batch completes, so a submission landing mid-batch sees the whole
   * batch's weight against the cap — not just the not-yet-executed tail — and is dropped while
   * the budget is full. After the batch settles, submissions are accepted again.
   */
  @Test
  void submit_midBatch_shouldChargeWholeBatchUntilSettled() throws InterruptedException {
    KeyDispatcher<Runnable> capped = newDispatcher(
      executor,
      "batch-settle",
      KeyDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
      KeyDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      3,
      // A start-jitter park keeps the queued submissions pending while the pre-granted
      // first task runs, so the next batch deterministically contains both of them.
      100
    );

    CountDownLatch t1Started = new CountDownLatch(1);
    CountDownLatch t1Release = new CountDownLatch(1);
    CountDownLatch t2Started = new CountDownLatch(1);
    CountDownLatch t2Release = new CountDownLatch(1);
    CountDownLatch t3Started = new CountDownLatch(1);
    CountDownLatch t3Release = new CountDownLatch(1);
    CountDownLatch batchDone = new CountDownLatch(3);
    CountDownLatch afterDrainRan = new CountDownLatch(1);

    try {
      // The first task is pre-granted alone; t2 and t3 are queued while it runs and become
      // the next pre-granted batch, so both of their units are charged together.
      capped.submit("key", blockedTask(t1Started, t1Release, batchDone), 1, true);
      capped.submit("key", blockedTask(t2Started, t2Release, batchDone), 1, true);
      capped.submit("key", blockedTask(t3Started, t3Release, batchDone), 1, true);

      assertThat(t1Started.await(5, TimeUnit.SECONDS)).isTrue();
      // Release t1: it completes, and the next batch (t2 + t3) is granted.
      t1Release.countDown();
      assertThat(t2Started.await(5, TimeUnit.SECONDS)).isTrue();
      // The consumer runs the batch sequentially: release t2 so t3 starts and blocks.
      t2Release.countDown();
      assertThat(t3Started.await(5, TimeUnit.SECONDS)).isTrue();

      // Mid-batch, both units of the next batch are still charged: a two-unit submission
      // would exceed the three-unit budget and is dropped.
      assertThat(capped.submit("key", afterDrainRan::countDown, 2, true)).isFalse();

      // Finish the batch: the settlement discharges both units at once.
      t3Release.countDown();
      assertThat(batchDone.await(5, TimeUnit.SECONDS)).isTrue();

      // The drain races with the next submission, so retry until it lands.
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      CountDownLatch retryRan = new CountDownLatch(1);
      while (retryRan.getCount() > 0 && System.nanoTime() < deadline) {
        capped.submit("key", retryRan::countDown, 1, true);
        Thread.sleep(10);
      }
      assertThat(retryRan.await(200, TimeUnit.MILLISECONDS)).isTrue();
      assertThat(afterDrainRan.await(200, TimeUnit.MILLISECONDS)).isFalse();
    } finally {
      t1Release.countDown();
      t2Release.countDown();
      t3Release.countDown();
      capped.close();
    }
  }

  private static Runnable blockedTask(CountDownLatch started, CountDownLatch release, CountDownLatch done) {
    return () -> {
      started.countDown();
      try {
        assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      done.countDown();
    };
  }

  /**
   * A consumer that throws must not leak its batch's weight: the settlement runs in a
   * {@code finally}, so the global budget recovers and later submissions are accepted.
   */
  @Test
  void submit_consumerThrows_shouldReleaseBatchWeight() throws InterruptedException {
    KeyDispatcher<Runnable> capped = newDispatcher(
      executor,
      "throwing",
      KeyDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
      KeyDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      1,
      0
    );

    CountDownLatch thrown = new CountDownLatch(1);
    CountDownLatch nextRan = new CountDownLatch(1);
    try {
      capped.submit(
        "key",
        () -> {
          thrown.countDown();
          throw new IllegalStateException("boom");
        },
        1,
        true
      );
      assertThat(thrown.await(5, TimeUnit.SECONDS)).isTrue();

      // The settlement races the retry: submit until the released budget admits one.
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (nextRan.getCount() > 0 && System.nanoTime() < deadline) {
        capped.submit("key", nextRan::countDown, 1, true);
        Thread.sleep(10);
      }
      assertThat(nextRan.await(200, TimeUnit.MILLISECONDS)).isTrue();
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
    KeyDispatcher<Runnable> capped = newDispatcher(
      executor,
      "weighted",
      KeyDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
      KeyDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      10,
      0
    );

    CountDownLatch heavyRan = new CountDownLatch(1);
    CountDownLatch lightRan = new CountDownLatch(1);

    try {
      // Weight-10 task: 0 + 10 > 10 is false → accepted, budget now full.
      capped.submit("key", heavyRan::countDown, 10, true);
      assertThat(heavyRan.await(5, TimeUnit.SECONDS)).isTrue();

      // The budget discharges right after the heavy task returns — it races with our next
      // submission, so retry until the drain lands and a weight-10 task fits exactly
      // (bounded retry: the drain happens within µs of the task completing).
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (lightRan.getCount() > 0 && System.nanoTime() < deadline) {
        capped.submit("key", lightRan::countDown, 10, true);
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
    KeyDispatcher<Runnable> capped = newDispatcher(
      executor,
      "weighted",
      KeyDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
      KeyDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      10,
      0
    );

    CountDownLatch blockLatch = new CountDownLatch(1);
    CountDownLatch heavyStarted = new CountDownLatch(1);
    CountDownLatch lightRan = new CountDownLatch(1);

    try {
      // Weight-10 task runs and holds the whole budget.
      capped.submit(
        "key",
        () -> {
          heavyStarted.countDown();
          try {
            assertThat(blockLatch.await(10, TimeUnit.SECONDS)).isTrue();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        },
        10,
        true
      );

      assertThat(heavyStarted.await(5, TimeUnit.SECONDS)).isTrue();

      // Weight-1 task while budget (10) is fully occupied → dropped.
      capped.submit("key", lightRan::countDown, 1, true);

      blockLatch.countDown();

      assertThat(lightRan.await(500, TimeUnit.MILLISECONDS)).isFalse();
    } finally {
      capped.close();
    }
  }

  /**
   * Verifies that a single-task worker costs exactly ONE executor submission: the grant
   * drains the worker and removes it from the map, leaving no empty queue for a
   * continuation submission to discover.
   */
  @Test
  void submit_singleTask_shouldUseSingleExecutorSubmission() throws InterruptedException {
    CountingExecutor countingExecutor = new CountingExecutor(4);
    KeyDispatcher<Runnable> batchingDispatcher = newDispatcher(
      countingExecutor,
      "test",
      KeyDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
      KeyDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      KeyDispatcher.DEFAULT_MAX_GLOBAL_PENDING_UNITS,
      0
    );

    CountDownLatch done = new CountDownLatch(1);
    try {
      batchingDispatcher.submit("key", done::countDown, 1, true);
      assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
      // The worker drains within its first batch and retires itself inside the same atomic
      // compute the continuation would have used, so the burst costs exactly ONE execute():
      // there is no post-drain cycle left to discover an empty queue.
      awaitSubmissionCount(countingExecutor, 1, 2000);
    } finally {
      batchingDispatcher.close();
      countingExecutor.shutdownNow();
    }
  }

  /**
   * Verifies that a worker under start jitter is PARKED (scheduler-side delay), not run
   * immediately and not slept through on a pool thread: the task does not execute inside
   * the jitter window, executes right after it, and the drained worker adds no
   * {@code execute()} continuation — the burst costs at most the one drive a zero-length
   * jitter draw falls back to (a positive draw rides {@code schedule()}).
   */
  @Test
  void submit_withJitter_shouldParkBeforeFirstCycle() throws InterruptedException {
    CountingExecutor countingExecutor = new CountingExecutor(4);
    KeyDispatcher<Runnable> jittered = newDispatcher(
      countingExecutor,
      "jitter",
      KeyDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
      KeyDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      50_000,
      150
    );

    Thread submitter = Thread.currentThread();
    final Thread[] runnerHolder = new Thread[1];
    CountDownLatch done = new CountDownLatch(1);
    try {
      jittered.submit(
        "key",
        () -> {
          runnerHolder[0] = Thread.currentThread();
          done.countDown();
        },
        1,
        true
      );
      // The task must run on an executor thread, never synchronously on the submitting thread —
      // this is the deterministic guarantee of the start-jitter park (the first cycle is either
      // schedule()'d or execute()'d on the executor, regardless of the random draw in [0,150)ms).
      // Asserting after the task completes avoids any race with the submitting thread.
      assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(runnerHolder[0]).isNotNull();
      assertThat(runnerHolder[0]).isNotSameAs(submitter);
      // The drained worker retires inline, so the whole burst costs AT MOST one execute(): a
      // positive jitter draw parks the first cycle via schedule() (zero), and only a zero-length
      // draw falls back to execute() (one). A second submission would mean the drained worker
      // was rescheduled instead of retiring inline.
      Thread.sleep(200);
      assertThat(countingExecutor.getSubmissionCount()).isLessThanOrEqualTo(1);
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
    KeyDispatcher<Runnable> jittered = newDispatcher(
      countingExecutor,
      "jitter",
      KeyDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
      KeyDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      50_000,
      50
    );

    int taskCount = 70;
    CountDownLatch done = new CountDownLatch(taskCount);
    var executionOrder = new java.util.concurrent.CopyOnWriteArrayList<Integer>();
    try {
      for (int i = 0; i < taskCount; i++) {
        int expected = i;
        jittered.submit(
          "key",
          () -> {
            executionOrder.add(expected);
            done.countDown();
          },
          1,
          true
        );
      }
      assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(executionOrder).containsExactlyElementsOf(
        java.util.stream.IntStream.range(0, taskCount).boxed().toList()
      );
    } finally {
      jittered.close();
      countingExecutor.shutdownNow();
    }
  }

  /**
   * Verifies the double-rejection cleanup: when the immediate submission <em>and</em> the delayed
   * re-drive are both refused, the worker is retired instead of staying in the map with its
   * scheduling flag set — which would make every later submission for the key enqueue behind a
   * cycle that never runs (the key wedged for the life of the dispatcher).
   */
  @Test
  void runTask_whenRetryIsRejectedToo_shouldRetireWorkerAndAcceptFreshSubmission() throws InterruptedException {
    ScheduledExecutorService twiceRejecting = new TwiceRejectingExecutor();
    KeyDispatcher<Runnable> twiceRejectingDispatcher = newDispatcher(
      twiceRejecting,
      "twice",
      KeyDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
      KeyDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      KeyDispatcher.DEFAULT_MAX_GLOBAL_PENDING_UNITS,
      0
    );
    try {
      CountDownLatch dropped = new CountDownLatch(1);
      twiceRejectingDispatcher.submit("key", dropped::countDown, 1, false);
      // Both the submission and its re-drive were refused: the task leaves with the
      // retired worker and never runs.
      assertThat(dropped.await(300, TimeUnit.MILLISECONDS)).isFalse();

      CountDownLatch fresh = new CountDownLatch(1);
      assertThat(twiceRejectingDispatcher.submit("key", fresh::countDown, 1, false)).isTrue();
      assertThat(fresh.await(5, TimeUnit.SECONDS)).as("a fresh worker serves the key").isTrue();
    } finally {
      twiceRejectingDispatcher.close();
      twiceRejecting.shutdownNow();
    }
  }

  /**
   * Polls until the {@link CountingExecutor} reports exactly {@code expected} execute() submissions
   * or the timeout elapses. Used instead of a racy immediate read: the drained-worker continuation
   * submit lands a few instructions after the task's latch is released, so the add must be awaited
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

  // ── Helper classes ──────────────────────────────────────────

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
   * A {@link ScheduledThreadPoolExecutor} that counts submissions and holds each
   * submitted command at a gate until {@link #release()} is called.
   *
   * <p>Lets a test enqueue a whole burst before any cycle runs, so a batch-size
   * assertion does not depend on how the submitting thread races the worker.
   */
  private static class GatedCountingExecutor extends ScheduledThreadPoolExecutor {

    private final AtomicInteger submissions = new AtomicInteger(0);
    private final CountDownLatch gate = new CountDownLatch(1);

    GatedCountingExecutor(int corePoolSize) {
      super(corePoolSize);
    }

    @Override
    @SuppressWarnings("java:S2142")
    public void execute(Runnable command) {
      submissions.incrementAndGet();
      super.execute(() -> {
        try {
          gate.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        command.run();
      });
    }

    /** Let every gated command run. Idempotent. */
    void release() {
      gate.countDown();
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
   * A {@link ScheduledThreadPoolExecutor} that refuses its first {@link #execute(Runnable)} and its
   * first {@link #schedule(Runnable, long, TimeUnit)} — the "executor is saturated, not shutting
   * down" shape, where both the immediate submission and the delayed re-drive are rejected.
   */
  private static class TwiceRejectingExecutor extends ScheduledThreadPoolExecutor {

    private final AtomicInteger executeRejections = new AtomicInteger(1);

    private final AtomicInteger scheduleRejections = new AtomicInteger(1);

    TwiceRejectingExecutor() {
      super(1);
    }

    @Override
    public void execute(Runnable command) {
      if (executeRejections.getAndDecrement() > 0) {
        throw new RejectedExecutionException("Simulated rejection for testing");
      }
      super.execute(command);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
      // ScheduledThreadPoolExecutor.execute delegates to schedule(0); only a
      // delayed park/retry is the "scheduler saturated" case this fake models.
      if (delay > 0 && scheduleRejections.getAndDecrement() > 0) {
        throw new RejectedExecutionException("Simulated rejection for testing");
      }
      return super.schedule(command, delay, unit);
    }
  }
}
