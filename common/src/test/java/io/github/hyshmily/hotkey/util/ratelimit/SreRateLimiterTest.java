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

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SreRateLimiterTest {

  @Test
  void shouldAllowBelowMinSamples() {
    SreRateLimiter limiter = new SreRateLimiter(1000, 10, 1.67, 10);
    for (int i = 0; i < 5; i++) {
      assertTrue(limiter.tryAcquire(), "should allow below minSamples");
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
    double rate = (double) allowed / 200;
    assertTrue(rate >= 0.90, "allow rate=" + rate);
  }

  @Test
  void zeroSuccess_shouldDropAlmostAll() {
    SreRateLimiter limiter = new SreRateLimiter(3000, 10, 1.67, 20);
    for (int i = 0; i < 20; i++) {
      assertTrue(limiter.tryAcquire());
      limiter.onFailed();
    }
    int allowed = 0;
    for (int i = 0; i < 180; i++) {
      if (limiter.tryAcquire()) {
        allowed++;
      }
      limiter.onFailed();
    }
    assertTrue(allowed <= 10, "allowed=" + allowed + " (expected ≤10)");
  }
}
