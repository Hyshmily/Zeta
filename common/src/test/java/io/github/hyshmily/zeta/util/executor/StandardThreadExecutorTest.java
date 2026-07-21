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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link StandardThreadExecutor}.
 */
class StandardThreadExecutorTest {

  @Test
  void shouldExecuteTask() throws Exception {
    var executor = new StandardThreadExecutor(1, 1, 1);
    var result = new AtomicInteger(0);
    executor.execute(() -> result.set(42));
    executor.shutdown();
    assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    assertThat(result.get()).isEqualTo(42);
  }

  @Test
  void shouldExpandToMaxThreads() throws Exception {
    var executor = new StandardThreadExecutor(1, 4, 4);
    var latch = new CountDownLatch(1);
    for (int i = 0; i < 4; i++) {
      executor.execute(() -> {
        try {
          latch.await();
        } catch (InterruptedException ignored) {
          Thread.currentThread().interrupt();
        }
      });
    }
    assertThat(executor.getPoolSize()).isEqualTo(4);
    latch.countDown();
    executor.shutdown();
    assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  void shouldQueueWhenAtMaxThreads() throws Exception {
    var executor = new StandardThreadExecutor(1, 2, 4);
    var block = new CountDownLatch(1);
    for (int i = 0; i < 2; i++) {
      executor.execute(() -> {
        try {
          block.await();
        } catch (InterruptedException ignored) {
          Thread.currentThread().interrupt();
        }
      });
    }
    // pool should be at max (2)
    assertThat(executor.getPoolSize()).isEqualTo(2);
    // this should queue, not reject
    var queued = new AtomicInteger(0);
    executor.execute(() -> queued.set(1));
    assertThat(queued.get()).isZero();
    block.countDown();
    executor.shutdown();
    assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    assertThat(queued.get()).isOne();
  }

  @Test
  void shouldRejectWhenOverMaxSubmitted() {
    var executor = new StandardThreadExecutor(1, 1, 1);
    var block = new CountDownLatch(1);
    // fill 1 active + 1 queued = 2 submitted = max
    executor.execute(() -> {
      try {
        block.await();
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
    });
    executor.execute(() -> {
      try {
        block.await();
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
    });
    assertThat(executor.getSubmittedTasksCount()).isEqualTo(2);
    assertThatThrownBy(() -> executor.execute(() -> {}))
        .isInstanceOf(RejectedExecutionException.class);
    block.countDown();
    executor.shutdown();
  }

  @Test
  void submittedTasksCountShouldReflectActiveTasks() throws Exception {
    var executor = new StandardThreadExecutor(2, 4, 4);
    var block = new CountDownLatch(1);
    for (int i = 0; i < 3; i++) {
      executor.execute(() -> {
        try {
          block.await();
        } catch (InterruptedException ignored) {
          Thread.currentThread().interrupt();
        }
      });
    }
    assertThat(executor.getSubmittedTasksCount()).isEqualTo(3);
    block.countDown();
    executor.shutdown();
    assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    assertThat(executor.getSubmittedTasksCount()).isZero();
  }

  @Test
  void shouldUseProvidedThreadFactory() {
    var named = new java.util.concurrent.atomic.AtomicReference<String>();
    var executor = new StandardThreadExecutor(
        1, 1, 60L, TimeUnit.SECONDS, 1, r -> {
          var t = new Thread(r, "custom-test-thread");
          named.set(t.getName());
          return t;
        });
    executor.execute(() -> {});
    executor.shutdown();
    assertThat(named.get()).contains("custom-test-thread");
  }

  @Test
  void shouldAllowCoreThreadTimeOut() {
    var executor = new StandardThreadExecutor(2, 4, 4);
    executor.allowCoreThreadTimeOut(true);
    assertThat(executor.allowsCoreThreadTimeOut()).isTrue();
    executor.shutdown();
  }

  @Test
  void shouldReportMaxSubmittedTaskCount() {
    var executor = new StandardThreadExecutor(2, 4, 10);
    assertThat(executor.getMaxSubmittedTaskCount()).isEqualTo(14);
    executor.shutdown();
  }
}
