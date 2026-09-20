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
package io.github.hyshmily.zeta.util.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link StandardThreadExecutor}.
 *
 * <p>Verifies the Tomcat-style execution ordering (core → max → queue → reject),
 * the {@code submittedTasksCount} accounting, the {@link StandardExecutorQueue#force}
 * fallback, and the rejection path.
 */
class StandardThreadExecutorTest {

  private StandardThreadExecutor newExecutor(int core, int max, int queueCapacity) {
    return newExecutor(core, max, queueCapacity, (r, e) -> {
      throw new RejectedExecutionException("Test rejection");
    });
  }

  private StandardThreadExecutor newExecutor(int core, int max, int queueCapacity, RejectedExecutionHandler handler) {
    return new StandardThreadExecutor(
      core,
      max,
      60L,
      TimeUnit.SECONDS,
      queueCapacity,
      r -> {
        Thread t = new Thread(r, "test-standard-executor");
        t.setDaemon(true);
        return t;
      },
      handler
    );
  }

  @Test
  void firstTask_shouldCreateThreadImmediately() throws Exception {
    // submittedCount(1) > poolSize(0) → queue.offer() returns false → worker created.
    var executor = newExecutor(1, 4, 100);
    var started = new CountDownLatch(1);
    try {
      executor.execute(() -> started.countDown());
      assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(executor.getPoolSize()).isGreaterThanOrEqualTo(1);
      assertThat(executor.getQueue()).isEmpty();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void threadCreation_precedesQueueing_untilMaxPool() throws Exception {
    // core=1, max=4: the second task must create a new thread instead of queuing.
    var executor = newExecutor(1, 4, 100);
    var started = new CountDownLatch(2);
    var release = new CountDownLatch(1);
    Runnable blocking = () -> {
      started.countDown();
      try {
        release.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };
    try {
      executor.execute(blocking);
      executor.execute(blocking);
      assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(executor.getPoolSize()).isGreaterThanOrEqualTo(2);
      assertThat(executor.getQueue()).isEmpty();
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void tasksQueued_whenPoolAtMax() throws Exception {
    // poolSize == maxPoolSize → queue.offer() returns true → task is queued.
    var executor = newExecutor(1, 2, 100);
    var blockingStarted = new CountDownLatch(2);
    var release = new CountDownLatch(1);
    var queuedRan = new CountDownLatch(1);
    Runnable blocking = () -> {
      blockingStarted.countDown();
      try {
        release.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };
    try {
      executor.execute(blocking);
      executor.execute(blocking);
      assertThat(blockingStarted.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(executor.getPoolSize()).isEqualTo(2);

      executor.execute(queuedRan::countDown);
      assertThat(executor.getQueue()).hasSize(1);

      release.countDown();
      assertThat(queuedRan.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(executor.getQueue()).isEmpty();
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void rejects_whenSubmittedExceedsMax() throws Exception {
    // cap = queueCapacity(1) + maxThreads(1) = 2; third submission is rejected.
    var executor = newExecutor(1, 1, 1);
    var firstStarted = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    Runnable blocking = () -> {
      firstStarted.countDown();
      try {
        release.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };
    try {
      executor.execute(blocking);
      assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();
      executor.execute(() -> {}); // queued — in-flight = 2, within cap
      assertThat(executor.getQueue()).hasSize(1);

      assertThatThrownBy(() -> executor.execute(() -> {})).isInstanceOf(RejectedExecutionException.class);
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void rejectionHandler_shouldBeInvoked() throws Exception {
    var rejected = new AtomicReference<Runnable>();
    var executor = newExecutor(1, 1, 1, (r, e) -> rejected.set(r));
    var firstStarted = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    Runnable blocking = () -> {
      firstStarted.countDown();
      try {
        release.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };
    Runnable rejectedTask = () -> {};
    try {
      executor.execute(blocking);
      assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();
      executor.execute(() -> {}); // queued
      executor.execute(rejectedTask); // over cap → handler
      assertThat(rejected.get()).isSameAs(rejectedTask);
      assertThat(executor.getSubmittedTasksCount()).isEqualTo(2);
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void submittedCount_returnsToZero_afterCompletion() throws Exception {
    // cap = queueCapacity(20) + maxThreads(4) = 24 > 20 submitted tasks.
    var executor = newExecutor(2, 4, 20);
    var tasks = new AtomicInteger(0);
    var done = new CountDownLatch(20);
    try {
      for (int i = 0; i < 20; i++) {
        executor.execute(() -> {
          tasks.incrementAndGet();
          done.countDown();
        });
      }
      assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(tasks.get()).isEqualTo(20);
      assertThat(executor.getSubmittedTasksCount()).isZero();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void force_afterShutdown_shouldThrow() throws Exception {
    var executor = newExecutor(1, 2, 10);
    StandardExecutorQueue queue = (StandardExecutorQueue) executor.getQueue();
    executor.shutdownNow();
    assertThatThrownBy(() -> queue.force(() -> {})).isInstanceOf(RejectedExecutionException.class);
  }

  @Test
  void force_duringShutdownTransition_shouldThrow() throws Exception {
    // Pins the defensive termination guard: while the pool is in the STOP transition (a task
    // still running, workers exiting), force() must reject instead of queueing a task that
    // may never be drained.
    var executor = newExecutor(1, 2, 10);
    var firstStarted = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    Runnable blocking = () -> {
      firstStarted.countDown();
      try {
        release.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };
    StandardExecutorQueue queue = (StandardExecutorQueue) executor.getQueue();
    try {
      executor.execute(blocking);
      assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();
      executor.shutdownNow();
      assertThatThrownBy(() -> queue.force(() -> {})).isInstanceOf(RejectedExecutionException.class);
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void tasksQueuedBeforeShutdown_shouldStillRun() throws Exception {
    // Pins the graceful-shutdown guarantee Zeta relies on: ThreadPoolExecutor in SHUTDOWN
    // state keeps draining its queue, so tasks accepted before shutdown() must all complete.
    var executor = newExecutor(2, 4, 1000);
    var executed = new AtomicInteger(0);
    var allDone = new CountDownLatch(20);
    try {
      for (int i = 0; i < 20; i++) {
        executor.execute(() -> {
          executed.incrementAndGet();
          allDone.countDown();
        });
      }
      executor.shutdown();
      assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
      assertThat(executed.get()).isEqualTo(20);
      assertThat(executor.getSubmittedTasksCount()).isZero();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void rejectionAfterShutdown_shouldNotLeakSubmittedCount() throws Exception {
    // Pins the symmetric accounting on the force()-throws path: a submission rejected
    // because the pool is shutting down never triggers afterExecute, so execute() must
    // compensate the increment itself. Without the compensation the counter would drift
    // upward by one per rejected submission, polluting the in-flight accounting.
    var executor = newExecutor(1, 2, 10);
    try {
      executor.shutdown();
      assertThatThrownBy(() -> executor.execute(() -> {})).isInstanceOf(RejectedExecutionException.class);
      assertThat(executor.getSubmittedTasksCount()).isZero();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void shutdownNow_shouldRecountDrainedTasks() throws Exception {
    // Pins the symmetric accounting on the shutdownNow() path: tasks returned by
    // shutdownNow() never execute, so afterExecute never fires and the drained batch
    // must compensate their increments. Without the compensation the counter would
    // stay at the number of drained tasks forever.
    var executor = newExecutor(1, 1, 100);
    var firstStarted = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    Runnable blocking = () -> {
      firstStarted.countDown();
      try {
        release.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };
    try {
      executor.execute(blocking);
      assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();
      executor.execute(() -> {}); // queued — the only worker is busy with the blocker
      executor.execute(() -> {}); // queued
      executor.shutdownNow(); // drains the two queued tasks
      assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
      assertThat(executor.getSubmittedTasksCount()).isZero();
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void force_shouldAcceptTask_whenRunning() throws Exception {
    var executor = newExecutor(1, 1, 10);
    var firstStarted = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var forcedRan = new CountDownLatch(1);
    Runnable blocking = () -> {
      firstStarted.countDown();
      try {
        release.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };
    StandardExecutorQueue queue = (StandardExecutorQueue) executor.getQueue();
    try {
      executor.execute(blocking);
      assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(queue.force(forcedRan::countDown)).isTrue();
      release.countDown();
      assertThat(forcedRan.await(5, TimeUnit.SECONDS)).isTrue();
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void maxSubmittedTaskCount_shouldBeQueuePlusMaxThreads() {
    var executor = newExecutor(2, 8, 500);
    try {
      assertThat(executor.getMaxSubmittedTaskCount()).isEqualTo(508);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void callerRunsPolicy_onPoolInternalReject_shouldNotLeakSubmittedCount() throws Exception {
    // Pins the accounting wrapper for NON-throwing rejection handlers: the pool itself
    // rejects (shutdown) and CallerRunsPolicy returns without throwing, so
    // super.execute() returns normally and afterExecute never fires. Without the
    // wrapper the submit-time increment would leak permanently, eventually wedging the
    // executor into permanent rejection despite a free queue.
    var executor = newExecutor(1, 2, 10, new ThreadPoolExecutor.CallerRunsPolicy());
    try {
      executor.shutdown();
      executor.execute(() -> {}); // pool-internal reject → handler returns → compensate
      assertThat(executor.getSubmittedTasksCount()).isZero();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void discardPolicy_onPoolInternalReject_shouldNotLeakSubmittedCount() throws Exception {
    // Same compensation contract for a silently-dropping handler: the task is discarded
    // (never queued, never executed, afterExecute never fires), so the increment must
    // be rolled back by the accounting wrapper.
    var executor = newExecutor(1, 2, 10, new ThreadPoolExecutor.DiscardPolicy());
    try {
      executor.shutdown();
      executor.execute(() -> {});
      assertThat(executor.getSubmittedTasksCount()).isZero();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void callerRunsPolicy_overCapInlineRun_shouldRestoreCounterToPriorValue() throws Exception {
    // Over-cap rejection with a non-throwing handler: the handler runs the task inline
    // in the submitting thread and the counter must return to its prior (pre-submission)
    // value — the inline run never reaches afterExecute.
    var executor = newExecutor(1, 1, 1, new ThreadPoolExecutor.CallerRunsPolicy());
    var firstStarted = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var inlineRuns = new AtomicInteger(0);
    try {
      executor.execute(() -> {
        firstStarted.countDown();
        try {
          release.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();
      executor.execute(() -> {}); // queued — in-flight = 2 = cap (queue 1 + max 1)
      int before = executor.getSubmittedTasksCount();
      executor.execute(inlineRuns::incrementAndGet); // over cap → CallerRunsPolicy runs inline
      assertThat(inlineRuns.get()).isEqualTo(1);
      assertThat(executor.getSubmittedTasksCount()).isEqualTo(before);
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void rejectedExecutionHandler_accessors_shouldBypassAccountingWrapper() {
    // getRejectedExecutionHandler() must return the user's handler, not the internal
    // accounting wrapper; setRejectedExecutionHandler() must swap the user handler
    // while the wrapper stays installed.
    var original = new ThreadPoolExecutor.DiscardPolicy();
    var executor = newExecutor(1, 1, 10, original);
    try {
      assertThat(executor.getRejectedExecutionHandler()).isSameAs(original);
      var replacement = new ThreadPoolExecutor.CallerRunsPolicy();
      executor.setRejectedExecutionHandler(replacement);
      assertThat(executor.getRejectedExecutionHandler()).isSameAs(replacement);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void queue_attachNullExecutor_shouldThrowNpe() {
    // Locks in the attach contract: a null executor is rejected eagerly instead of
    // surfacing later as an NPE from offer().
    var queue = new StandardExecutorQueue();
    assertThatThrownBy(() -> queue.setStandardThreadExecutor(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void queue_reattach_shouldThrowIllegalState() {
    // Locks in the single-use attach contract: maxPoolSize is snapshotted at attach,
    // so silently re-attaching to another executor would mix two executors' states.
    var executor = newExecutor(1, 2, 10);
    try {
      var queue = (StandardExecutorQueue) executor.getQueue();
      var other = newExecutor(1, 2, 10);
      try {
        assertThatThrownBy(() -> queue.setStandardThreadExecutor(other)).isInstanceOf(IllegalStateException.class);
      } finally {
        other.shutdownNow();
      }
    } finally {
      executor.shutdownNow();
    }
  }
}
