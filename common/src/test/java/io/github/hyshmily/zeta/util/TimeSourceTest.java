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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TimeSource} verifying idempotent start, restart after the
 * clock-cache thread is marked dead, and correct time retrieval.
 */
class TimeSourceTest {

  @AfterEach
  void tearDown() throws Exception {
    Field attemptsField = TimeSource.class.getDeclaredField("startAttempts");
    attemptsField.setAccessible(true);
    attemptsField.setInt(null, 0);

    // The ticker lifecycle is the sign of currentMillis: a negative value is
    // the stopped encoding (~rawFloor), not a separate boolean.
    Field currentField = TimeSource.class.getDeclaredField("currentMillis");
    currentField.setAccessible(true);
    currentField.setLong(null, ~System.currentTimeMillis());
    Field threadField = TimeSource.class.getDeclaredField("clockThread");
    threadField.setAccessible(true);
    threadField.set(null, null);
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
  void start_afterThreadMarkedDead_shouldRestartAndKeepReading() throws Exception {
    // First 3 calls are all idempotent no-ops after the first one starts the thread.
    for (int i = 0; i < 3; i++) {
      TimeSource.start();
    }

    // Simulate thread death so start() can install a replacement: the stopped
    // encoding is a negative currentMillis plus no installed thread.
    Field currentField = TimeSource.class.getDeclaredField("currentMillis");
    currentField.setAccessible(true);
    currentField.setLong(null, ~System.currentTimeMillis());
    Field threadField = TimeSource.class.getDeclaredField("clockThread");
    threadField.setAccessible(true);
    threadField.set(null, null);

    // start() is restart-capable: a replacement clock thread is started, and reads work
    // either way (the cached clock falls back to System.currentTimeMillis()).
    TimeSource.start();

    assertThat(TimeSource.currentTimeMillis()).isPositive();
  }

  @Test
  void start_duringRestartBackoff_shouldNotInstallASecondTicker() throws Exception {
    Thread fakeTicker = new Thread(() -> {}, "fake-timesource");
    fakeTicker.start();

    Field currentField = TimeSource.class.getDeclaredField("currentMillis");
    currentField.setAccessible(true);
    currentField.setLong(null, ~System.currentTimeMillis());
    Field threadField = TimeSource.class.getDeclaredField("clockThread");
    threadField.setAccessible(true);
    threadField.set(null, fakeTicker);
    try {
      TimeSource.start();

      assertThat(threadField.get(null)).as("the in-backoff ticker keeps ownership").isSameAs(fakeTicker);
    } finally {
      fakeTicker.interrupt();
      fakeTicker.join(1_000L);
    }
  }

  /**
   * A ticker that has been superseded must not enter the one-second backoff. The stopper's
   * bounded join would otherwise burn its full timeout waiting for a thread that can never
   * resurrect the slot.
   */
  @Test
  void resumeAfterBackoff_whenSuperseded_shouldReturnWithoutSleeping() throws Exception {
    Field currentField = TimeSource.class.getDeclaredField("currentMillis");
    currentField.setAccessible(true);
    currentField.setLong(null, ~System.currentTimeMillis());
    Field threadField = TimeSource.class.getDeclaredField("clockThread");
    threadField.setAccessible(true);
    threadField.set(null, new Thread(() -> {}, "superseded-timesource"));

    var resumeAfterBackoff = TimeSource.class.getDeclaredMethod("resumeAfterBackoff");
    resumeAfterBackoff.setAccessible(true);

    long start = System.nanoTime();
    boolean resumed = (boolean) resumeAfterBackoff.invoke(null);
    long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

    assertThat(resumed).isFalse();
    assertThat(elapsedMs).as("a superseded ticker must not pay the restart backoff").isLessThan(250L);
  }

  @Test
  void monotonicSecondsSinceBoot_shouldNeverReturnTheUnknownSentinel() {
    // 0 is CacheEntry.getBirthStampSec()'s "unknown" marker; a value loaded in the
    // first boot second must still receive a real max-lease-eligible stamp.
    assertThat(TimeSource.monotonicSecondsSinceBoot()).isPositive();
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
