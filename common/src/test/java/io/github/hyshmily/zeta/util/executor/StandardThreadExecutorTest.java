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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link StandardThreadExecutor}.
 *
 * <p>Verifies the Tomcat-style execution ordering (core → max → queue → reject),
 * the {@code submittedTasksCount} accounting, the {@link StandardExecutorQueue#force}
 * fallback, and the rejection path.
 */
@Tag("performance")
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
}
