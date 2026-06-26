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
package io.github.hyshmily.hotkey.util.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SreRateLimiterTest {

  @Test
  void shouldAllowBelowMinSamples() {
    SreRateLimiter limiter = new SreRateLimiter(1000, 10, 1.67, 10);
    for (int i = 0; i < 5; i++) {
      assertThat(limiter.tryAcquire()).as("should allow below minSamples").isTrue();
      limiter.onSuccess();
    }
  }

  @Test
  void highSuccessRate_shouldAllowAlmostAll() {
    SreRateLimiter limiter = new SreRateLimiter(3000, 10, 1.67, 20);
    int allowed = 0;
    for (int i = 0; i < 200; i++) {
      if (limiter.tryAcquire()) {
        allowed++;
        limiter.onSuccess();
      } else {
        limiter.onFailed();
      }
    }
    assertThat((double) allowed / 200).as("allow rate").isGreaterThanOrEqualTo(0.90);
  }

  @Test
  void zeroSuccess_shouldDropAlmostAll() {
    SreRateLimiter limiter = new SreRateLimiter(3000, 10, 1.67, 20);
    for (int i = 0; i < 20; i++) {
      assertThat(limiter.tryAcquire()).isTrue();
      limiter.onFailed();
    }
    int allowed = 0;
    for (int i = 0; i < 180; i++) {
      if (limiter.tryAcquire()) {
        allowed++;
      }
      limiter.onFailed();
    }
    assertThat(allowed).as("allowed count, expected <=10 but was " + allowed).isLessThanOrEqualTo(10);
  }

  @Test
  void with50PercentSuccess_shouldAllowAroundHalf() {
    SreRateLimiter limiter = new SreRateLimiter(5000, 10, 1, 20);
    int allowed = 0;
    int N = 1000;
    for (int i = 0; i < N; i++) {
      if (limiter.tryAcquire()) {
        allowed++;
      }
      if (i % 2 == 0) {
        limiter.onSuccess();
      } else {
        limiter.onFailed();
      }
    }
    assertThat((double) allowed / N).isGreaterThan(0.30).isLessThan(0.70);
  }

  @Test
  void with100PercentSuccess_shouldApproach100Percent() {
    SreRateLimiter limiter = new SreRateLimiter(3000, 10, 1.67, 20);
    int allowed = 0;
    for (int i = 0; i < 200; i++) {
      if (limiter.tryAcquire()) {
        allowed++;
      }
      limiter.onSuccess();
    }
    assertThat((double) allowed / 200).isGreaterThan(0.95);
  }

  @Test
  void withRecovery_shouldAllowMoreAfterRecovery() {
    SreRateLimiter limiter = new SreRateLimiter(5000, 10, 1.67, 10);
    for (int i = 0; i < 50; i++) {
      limiter.tryAcquire();
      limiter.onFailed();
    }
    int phase1Allowed = 0;
    for (int i = 0; i < 200; i++) {
      if (limiter.tryAcquire()) {
        phase1Allowed++;
      }
      limiter.onFailed();
    }
    for (int i = 0; i < 200; i++) {
      limiter.tryAcquire();
      limiter.onSuccess();
    }
    int phase2Allowed = 0;
    for (int i = 0; i < 200; i++) {
      if (limiter.tryAcquire()) {
        phase2Allowed++;
      }
      limiter.onSuccess();
    }
    assertThat(phase2Allowed).isGreaterThan(phase1Allowed);
  }

  @Test
  void withExactMinSamples_boundary() {
    SreRateLimiter limiter = new SreRateLimiter(1000, 10, 1.67, 10);
    for (int i = 0; i < 10; i++) {
      assertThat(limiter.tryAcquire()).isTrue();
      limiter.onFailed();
    }
    int allowed = 0;
    for (int i = 0; i < 100; i++) {
      if (limiter.tryAcquire()) {
        allowed++;
      }
      limiter.onFailed();
    }
    assertThat(allowed).isLessThan(20);
  }

  @Test
  void withVeryLowK() {
    SreRateLimiter limiter = new SreRateLimiter(5000, 10, 0.5, 20);
    int allowed = 0;
    int N = 500;
    for (int i = 0; i < N; i++) {
      if (limiter.tryAcquire()) {
        allowed++;
        limiter.onSuccess();
      } else {
        limiter.onFailed();
      }
    }
    double rate = (double) allowed / N;
    assertThat(rate).isPositive().isLessThan(0.5);
  }

  @Test
  void withVeryHighK() {
    SreRateLimiter limiter = new SreRateLimiter(5000, 10, 10, 20);
    int allowed = 0;
    int N = 500;
    for (int i = 0; i < N; i++) {
      if (limiter.tryAcquire()) {
        allowed++;
      }
      if (i % 5 == 0) {
        limiter.onSuccess();
      } else {
        limiter.onFailed();
      }
    }
    assertThat((double) allowed / N).isGreaterThan(0.95);
  }

  @Test
  void zeroK_shouldAllowOnlyBeforeMinSamples() {
    SreRateLimiter limiter = new SreRateLimiter(1000, 10, 0, 5);
    // With k=0, maxRequests=0; only requests below minSamples(5) pass
    for (int i = 0; i < 5; i++) {
      assertThat(limiter.tryAcquire()).isTrue();
      limiter.onFailed();
    }
    // After minSamples reached, probabilistic drop kicks in
    int allowed = 0;
    for (int i = 0; i < 100; i++) {
      if (limiter.tryAcquire()) {
        allowed++;
      }
      limiter.onFailed();
    }
    assertThat(allowed).isLessThan(30);
  }

  @Test
  void negativeK_shouldTriggerDegenerateGuard() {
    SreRateLimiter limiter = new SreRateLimiter(1000, 10, -1, 5);
    for (int i = 0; i < 10; i++) {
      assertThat(limiter.tryAcquire()).isTrue();
      limiter.onSuccess();
    }
    // Negative k → maxRequests negative → guard returns true
    for (int i = 0; i < 50; i++) {
      assertThat(limiter.tryAcquire()).isTrue();
      limiter.onSuccess();
    }
  }

  @Test
  void zeroMinSamples_shouldActivateImmediately() {
    SreRateLimiter limiter = new SreRateLimiter(1000, 10, 1, 0);
    // First call should still be allowed (total < minSamples → true, minSamples=0)
    assertThat(limiter.tryAcquire()).isTrue();
  }

  @Test
  void onSuccessAndOnFailed_withoutTryAcquire_shouldNotThrow() {
    SreRateLimiter limiter = new SreRateLimiter(1000, 10, 1.67, 5);
    limiter.onSuccess();
    limiter.onFailed();
  }

  @Test
  void negativeMaxRequests_shouldAlwaysAllow() {
    SreRateLimiter limiter = new SreRateLimiter(1000, 10, -1, 0);
    for (int i = 0; i < 20; i++) {
      limiter.tryAcquire();
      limiter.onSuccess();
    }
    // k = -1, accepts > 0 → maxRequests < 0 → guard always returns true
    for (int i = 0; i < 50; i++) {
      assertThat(limiter.tryAcquire()).isTrue();
    }
  }

  @Test
  void minSamplesZero_shouldApplyBudgetImmediately() {
    SreRateLimiter limiter = new SreRateLimiter(1000, 10, 1.5, 0);
    limiter.onFailed();
    int allowed = 0;
    for (int i = 0; i < 100; i++) {
      if (limiter.tryAcquire()) allowed++;
      limiter.onFailed();
    }
    assertThat(allowed).isLessThan(30);
  }

  @Test
  void windowRollover_shouldNotBreakLimiter() throws InterruptedException {
    SreRateLimiter limiter = new SreRateLimiter(100, 5, 1.67, 5);
    for (int i = 0; i < 20; i++) {
      limiter.tryAcquire();
      limiter.onSuccess();
    }
    // Wait for window to fully roll over (2x window duration)
    Thread.sleep(250);
    // After rollover, windows should be empty → below minSamples → allow
    assertThat(limiter.tryAcquire()).isTrue();
  }
}
