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

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DelayUtil} verifying TTL-jitter computation across the
 * zero/negative, positive, maximum-ratio, and overflow cases.
 */
class DelayUtilTest {

  // ── computeTtlJitter ──

  @Test
  void computeTtlJitter_withZeroRatio_shouldReturnZero() {
    assertThat(DelayUtil.computeTtlJitter(10_000, 0.0)).isZero();
  }

  @Test
  void computeTtlJitter_withZeroTtl_shouldReturnZero() {
    assertThat(DelayUtil.computeTtlJitter(0, 0.1)).isZero();
  }

  @Test
  void computeTtlJitter_withNegativeTtl_shouldHandleGracefully() {
    long jitter = DelayUtil.computeTtlJitter(-1000, 0.1);
    assertThat(jitter).isBetween(-100L, 99L);
  }

  @Test
  void computeTtlJitter_withPositiveRatio_shouldBeWithinRange() {
    long ttl = 10_000;
    double ratio = 0.1;
    for (int i = 0; i < 100; i++) {
      long jitter = DelayUtil.computeTtlJitter(ttl, ratio);
      assertThat(jitter).isBetween(-(long) (ttl * ratio), (long) (ttl * ratio) - 1);
    }
  }

  @Test
  void computeTtlJitter_withMaxRatio_shouldBeWithinRange() {
    long ttl = 10_000;
    for (int i = 0; i < 100; i++) {
      long jitter = DelayUtil.computeTtlJitter(ttl, 1.0);
      assertThat(jitter).isBetween(-10_000L, 9_999L);
    }
  }

  @Test
  void computeTtlJitter_withLargeTtl_shouldHandleOverflow() {
    long jitter = DelayUtil.computeTtlJitter(Long.MAX_VALUE, 1.0);
    assertThat(jitter).isBetween(Long.MIN_VALUE, Long.MAX_VALUE);
  }
}
