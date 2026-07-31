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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SafeScheduledExecutorService}.
 */
class SafeScheduledExecutorServiceTest {

  private SafeScheduledExecutorService newExecutor() {
    var executor = new SafeScheduledExecutorService(2, r -> {
      Thread t = new Thread(r, "test-safe-scheduler");
      t.setDaemon(true);
      return t;
    });
    executor.setKeepAliveTime(1, TimeUnit.SECONDS);
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  @Test
  void fixedRate_shouldNotOverlap_whenTaskExceedsPeriod() throws Exception {
    var executor = newExecutor();
    var starts = new CopyOnWriteArrayList<Long>();
    var inFlight = new AtomicInteger(0);
    var maxInFlight = new AtomicInteger(0);
    var done = new CountDownLatch(3);

    var task = (Runnable) () -> {
      int cur = inFlight.incrementAndGet();
      maxInFlight.accumulateAndGet(cur, Math::max);
      try {
        starts.add(System.currentTimeMillis());
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        inFlight.decrementAndGet();
        done.countDown();
      }
    };

    var future = executor.scheduleAtFixedRate(task, 0, 20, TimeUnit.MILLISECONDS);
    try {
      assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(maxInFlight.get()).isEqualTo(1);
      assertThat(starts).hasSize(3);
      long gap = starts.get(1) - starts.get(0);
      assertThat(gap).isGreaterThanOrEqualTo(90);
    } finally {
      future.cancel(true);
      executor.shutdownNow();
    }
  }

  @Test
  void fixedRate_shouldContinueAfterTaskThrows() throws Exception {
    var executor = newExecutor();
    var executions = new AtomicInteger(0);
    var done = new CountDownLatch(3);

    var task = (Runnable) () -> {
      int n = executions.incrementAndGet();
      done.countDown();
      if (n == 1) {
        throw new IllegalStateException("boom");
      }
    };

    var future = executor.scheduleAtFixedRate(task, 0, 10, TimeUnit.MILLISECONDS);
    try {
      assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(executions.get()).isEqualTo(3);
    } finally {
      future.cancel(true);
      executor.shutdownNow();
    }
  }

  @Test
  void fixedDelay_shouldContinueAfterTaskThrows() throws Exception {
    var executor = newExecutor();
    var executions = new AtomicInteger(0);
    var done = new CountDownLatch(2);

    var task = (Runnable) () -> {
      executions.incrementAndGet();
      done.countDown();
      throw new IllegalStateException("boom");
    };

    var future = executor.scheduleWithFixedDelay(task, 0, 10, TimeUnit.MILLISECONDS);
    try {
      assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(executions.get()).isEqualTo(2);
    } finally {
      future.cancel(true);
      executor.shutdownNow();
    }
  }

  @Test
  void cancel_shouldStopTheChain() throws Exception {
    var executor = newExecutor();
    var executions = new AtomicInteger(0);
    var started = new CountDownLatch(1);
    var release = new CountDownLatch(1);

    var task = (Runnable) () -> {
      executions.incrementAndGet();
      started.countDown();
      try {
        release.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    var future = executor.scheduleAtFixedRate(task, 0, 10, TimeUnit.MILLISECONDS);
    try {
      assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(future.cancel(false)).isTrue();
      assertThat(future.isCancelled()).isTrue();
      release.countDown();
      Thread.sleep(60);
      assertThat(executions.get()).isEqualTo(1);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void shutdown_shouldStopTheChainSilently() throws Exception {
    var executor = newExecutor();
    var executions = new AtomicInteger(0);
    var started = new CountDownLatch(1);
    var release = new CountDownLatch(1);

    var task = (Runnable) () -> {
      executions.incrementAndGet();
      started.countDown();
      try {
        release.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    var future = executor.scheduleAtFixedRate(task, 0, 10, TimeUnit.MILLISECONDS);
    assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
    executor.shutdown();
    release.countDown();
    assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    assertThat(executions.get()).isEqualTo(1);
    assertThat(future.isDone()).isTrue();
  }

  @Test
  void schedule_shouldKeepJdkOneShotSemantics() throws Exception {
    var executor = newExecutor();
    var ran = new CountDownLatch(1);
    ScheduledFuture<?> future = executor.schedule(ran::countDown, 10, TimeUnit.MILLISECONDS);
    try {
      assertThat(ran.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(future.get(1, TimeUnit.SECONDS)).isNull();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void scheduleWithFixedDelay_shouldNotOverlap() throws Exception {
    var executor = newExecutor();
    var inFlight = new AtomicInteger(0);
    var maxInFlight = new AtomicInteger(0);
    var done = new CountDownLatch(3);

    var task = (Runnable) () -> {
      int cur = inFlight.incrementAndGet();
      maxInFlight.accumulateAndGet(cur, Math::max);
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        inFlight.decrementAndGet();
        done.countDown();
      }
    };

    var future = executor.scheduleWithFixedDelay(task, 0, 10, TimeUnit.MILLISECONDS);
    try {
      assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(maxInFlight.get()).isEqualTo(1);
    } finally {
      future.cancel(true);
      executor.shutdownNow();
    }
  }

  @Test
  void getBeforeStart_shouldNotBeDone() throws Exception {
    var executor = newExecutor();
    var ran = new CountDownLatch(1);
    var future = executor.scheduleAtFixedRate(ran::countDown, 60, 10, TimeUnit.MILLISECONDS);
    try {
      assertThat(future.isDone()).isFalse();
      assertThat(ran.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(future.cancel(true)).isTrue();
      assertThat(future.isCancelled()).isTrue();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void constructorVariants_shouldBeUsable() {
    var a = new SafeScheduledExecutorService(1);
    var b = new SafeScheduledExecutorService(1, r -> new Thread(r));
    var c = new SafeScheduledExecutorService(1, r -> new Thread(r), (r, e) -> {});
    assertThat(List.of(a, b, c)).allMatch(e -> e.getPoolSize() == 0);
    a.shutdownNow();
    b.shutdownNow();
    c.shutdownNow();
  }
}
