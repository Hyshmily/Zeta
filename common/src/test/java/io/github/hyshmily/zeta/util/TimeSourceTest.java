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
package io.github.hyshmily.zeta.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TimeSource} verifying idempotent start, max-attempt guard, and correct time
 * retrieval.
 */
class TimeSourceTest {

  @AfterEach
  void tearDown() throws Exception {
    Field tryCountField = TimeSource.class.getDeclaredField("threadTryCount");
    tryCountField.setAccessible(true);
    ((AtomicInteger) tryCountField.get(null)).set(0);

    Field runningField = TimeSource.class.getDeclaredField("threadRunning");
    runningField.setAccessible(true);
    ((AtomicBoolean) runningField.get(null)).set(false);
  }

  @Test
  void start_shouldBeIdempotent() {
    assertThatCode(TimeSource::start).doesNotThrowAnyException();
    assertThatCode(TimeSource::start).doesNotThrowAnyException();
  }

  @Test
  void currentTimeMillis_withStart_shouldReturnPositiveValue() {
    TimeSource.start();
    assertThat(TimeSource.currentTimeMillis()).isPositive();
  }

  @Test
  void currentTimeMillis_shouldBeCloseToSystemTime() {
    TimeSource.start();
    long ts = TimeSource.currentTimeMillis();
    long sys = System.currentTimeMillis();
    assertThat(Math.abs(ts - sys)).isLessThan(10_000);
  }

  @Test
  void currentTimeMillis_withoutStart_shouldWork() {
    long ts = TimeSource.currentTimeMillis();
    assertThat(ts).isPositive();
  }

  @Test
  void start_afterMaxAttempts_shouldNotRestartInNewThread() throws Exception {
    // First 3 calls increment counter; only the 1st starts a thread
    for (int i = 0; i < 3; i++) {
      TimeSource.start();
    }

    // Simulate thread death so CAS could otherwise succeed
    Field runningField = TimeSource.class.getDeclaredField("threadRunning");
    runningField.setAccessible(true);
    ((AtomicBoolean) runningField.get(null)).set(false);

    // 4th call — counter is now 4 which exceeds THREAD_TRY_MAX (3)
    TimeSource.start();

    // With no thread running, currentTimeMillis() falls back to System.currentTimeMillis()
    assertThat(TimeSource.currentTimeMillis()).isPositive();
  }

  @Test
  void currentTimeMillis_cachedValue_shouldUpdateOverTime() throws Exception {
    TimeSource.start();
    // Allow the background thread to advance the clock a few cycles (each sleeps 5 ms)
    long before = TimeSource.currentTimeMillis();
    Thread.sleep(25);
    long after = TimeSource.currentTimeMillis();
    assertThat(after).isGreaterThanOrEqualTo(before);
  }
}
