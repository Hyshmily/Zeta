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
package io.github.hyshmily.hotkey.broadcast;

import static org.assertj.core.api.Assertions.assertThat;

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

  @Test
  void floatTimeDelay_zeroJitter_shouldRunImmediately() {
    AtomicBoolean executed = new AtomicBoolean(false);
    DelayUtil.floatTimeDelay(() -> executed.set(true), 0, Executors.newSingleThreadScheduledExecutor());
    assertThat(executed).isTrue();
  }

  @Test
  void floatTimeDelay_negativeJitter_shouldRunImmediately() {
    AtomicBoolean executed = new AtomicBoolean(false);
    DelayUtil.floatTimeDelay(() -> executed.set(true), -1, Executors.newSingleThreadScheduledExecutor());
    assertThat(executed).isTrue();
  }

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
}
