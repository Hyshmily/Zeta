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
package io.github.hyshmily.hotkey.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.hyshmily.hotkey.util.DelayUtil;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DelayUtil} verifying immediate execution for zero/negative jitter and eventual
 * execution for positive jitter values.
 */
class DelayUtilTest {

  /**
   * Verifies that a zero jitter value causes the task to execute via the scheduler.
   */
  @Test
  void floatTimeDelay_zeroJitter_shouldRunImmediately() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    DelayUtil.floatTimeDelay(latch::countDown, 0, scheduler);
    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    scheduler.shutdown();
  }

  /**
   * Verifies that a negative jitter value causes the task to execute via the scheduler.
   */
  @Test
  void floatTimeDelay_negativeJitter_shouldRunImmediately() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    DelayUtil.floatTimeDelay(latch::countDown, -1, scheduler);
    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    scheduler.shutdown();
  }

  /**
   * Verifies that a positive jitter value schedules the task for deferred execution.
   */
  @Test
  void floatTimeDelay_positiveJitter_shouldExecuteEventually() throws InterruptedException {
    AtomicBoolean executed = new AtomicBoolean(false);
    CountDownLatch latch = new CountDownLatch(1);
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    DelayUtil.floatTimeDelay(() -> {
      executed.set(true);
      latch.countDown();
    }, 100, scheduler);

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(executed).isTrue();
    scheduler.shutdown();
  }

  /**
   * Verifies that a very large jitter value still schedules the task.
   */
  @Test
  void floatTimeDelay_largeJitter_shouldExecuteEventually() throws InterruptedException {
    AtomicBoolean executed = new AtomicBoolean(false);
    CountDownLatch latch = new CountDownLatch(1);
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    DelayUtil.floatTimeDelay(() -> {
      executed.set(true);
      latch.countDown();
    }, 3_000, scheduler);

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(executed).isTrue();
    scheduler.shutdown();
  }

  /**
   * Verifies that a jitter value of 1 causes the task to execute via the scheduler (delay =
   * nextLong(1) = 0).
   */
  @Test
  void floatTimeDelay_jitterOfOne_shouldRunImmediately() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    DelayUtil.floatTimeDelay(latch::countDown, 1, scheduler);
    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    scheduler.shutdown();
  }

  /**
   * Verifies that a null scheduler with zero jitter throws NullPointerException (the method always
   * delegates to the scheduler).
   */
  @Test
  void floatTimeDelay_zeroJitter_nullScheduler_shouldThrow() {
    assertThatCode(() -> DelayUtil.floatTimeDelay(() -> {}, 0, null))
        .isInstanceOf(NullPointerException.class);
  }

  /**
   * Verifies that a null scheduler with negative jitter throws NullPointerException (the method
   * always delegates to the scheduler).
   */
  @Test
  void floatTimeDelay_negativeJitter_nullScheduler_shouldThrow() {
    assertThatCode(() -> DelayUtil.floatTimeDelay(() -> {}, -5, null))
        .isInstanceOf(NullPointerException.class);
  }

  /**
   * Verifies that a zero jitter value with a real scheduler executes the task eventually via the
   * scheduler.
   */
  @Test
  void floatTimeDelay_zeroJitter_scheduler_shouldExecuteEventually() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    DelayUtil.floatTimeDelay(latch::countDown, 0, scheduler);
    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    scheduler.shutdown();
  }

  /**
   * Verifies that a null scheduler with positive jitter throws NullPointerException (the method
   * always delegates to the scheduler regardless of jitter value).
   */
  @Test
  void floatTimeDelay_nullScheduler_withPositiveJitter_shouldThrow() {
    assertThatCode(() -> DelayUtil.floatTimeDelay(() -> {}, 100, null))
        .isInstanceOf(NullPointerException.class);
  }
}
