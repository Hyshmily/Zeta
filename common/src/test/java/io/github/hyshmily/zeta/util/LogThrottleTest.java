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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link LogThrottle}. The monotonic axis is advanced with
 * {@link TimeSource#setTimeOffsetForTest} instead of {@code Thread.sleep}, so the
 * window logic is verified deterministically rather than by racing wall time.
 */
class LogThrottleTest {

  private static final long WINDOW_MS = 10_000L;

  @BeforeEach
  @AfterEach
  void resetClock() {
    TimeSource.setTimeOffsetForTest(0L, 0L);
  }

  @Test
  void tryAcquire_firstCall_shouldWin() {
    LogThrottle throttle = new LogThrottle(WINDOW_MS);
    assertThat(throttle.tryAcquire()).as("first call must always log").isTrue();
  }

  @Test
  void tryAcquire_withinWindow_shouldBeSuppressed() {
    LogThrottle throttle = new LogThrottle(WINDOW_MS);
    assertThat(throttle.tryAcquire()).isTrue();
    assertThat(throttle.tryAcquire()).as("second call inside the window").isFalse();
    assertThat(throttle.tryAcquire()).isFalse();
  }

  @Test
  void tryAcquire_afterWindow_shouldWinAgain() {
    LogThrottle throttle = new LogThrottle(WINDOW_MS);
    assertThat(throttle.tryAcquire()).isTrue();

    TimeSource.setTimeOffsetForTest(0L, WINDOW_MS);
    assertThat(throttle.tryAcquire()).as("window elapsed exactly").isTrue();

    TimeSource.setTimeOffsetForTest(0L, 2L * WINDOW_MS - 1L);
    assertThat(throttle.tryAcquire()).as("one millisecond short of the window").isFalse();
  }

  /**
   * Regression test for the check-then-act race this class replaces: with a
   * {@code volatile} stamp, every thread released at the same instant read the stale
   * value, passed the window check and logged. Compare-and-set must admit exactly one.
   */
  @Test
  void tryAcquire_concurrentBurst_shouldAdmitExactlyOne() throws Exception {
    LogThrottle throttle = new LogThrottle(WINDOW_MS);
    int threads = 16;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);
    AtomicInteger admitted = new AtomicInteger();
    try {
      for (int i = 0; i < threads; i++) {
        pool.submit(() -> {
          try {
            start.await();
            if (throttle.tryAcquire()) {
              admitted.incrementAndGet();
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            done.countDown();
          }
        });
      }
      start.countDown();
      assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
    } finally {
      pool.shutdownNow();
    }
    assertThat(admitted.get()).as("exactly one winner per window").isEqualTo(1);
  }

  @Test
  void constructor_shouldRejectNonPositiveWindow() {
    assertThatThrownBy(() -> new LogThrottle(0L))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("windowMs must be positive");
    assertThatThrownBy(() -> new LogThrottle(-1L)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void instances_shouldHaveIndependentWindows() {
    LogThrottle first = new LogThrottle(WINDOW_MS);
    LogThrottle second = new LogThrottle(WINDOW_MS);
    assertThat(first.tryAcquire()).isTrue();
    assertThat(second.tryAcquire()).as("a second alarm has its own window").isTrue();
  }
}
