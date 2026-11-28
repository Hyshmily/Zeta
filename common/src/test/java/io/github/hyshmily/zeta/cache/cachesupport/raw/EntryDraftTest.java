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
package io.github.hyshmily.zeta.cache.cachesupport.raw;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.util.TimeSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@link EntryDraft.SYSTEM} deadline arithmetic. The saturation decision belongs to the clock
 * sample, not the sum: the monotonic base is negative under a test clock or a negative-offset base,
 * and a finite TTL still has to produce a finite future instant there.
 */
class EntryDraftTest {

  @AfterEach
  void resetClock() {
    TimeSource.setTimeOffsetForTest(0L, 0L);
  }

  @Test
  void aFiniteTtlUnderANegativeClockStaysFinite() {
    // Shift the monotonic base to -10s: a 1s TTL expires at -9s, far inside the long range.
    shiftMonotonicBaseTo(-10_000L);

    CacheEntry entry = EntryDraft.blank(null).value("v").version(1L).ttl(1_000L, 0L).build();

    assertThat(entry.getHardExpireAtMs())
      .as("a negative deadline is a valid instant, not an overflow")
      .isNegative()
      .isNotEqualTo(Long.MAX_VALUE);
    assertThat(entry.getHardTtlMs()).isEqualTo(1_000L);
  }

  @Test
  void aTtlThatWouldOverflowSaturatesAtPermanent() {
    // Within one 10s TTL of the ceiling, with headroom so clock drift cannot wrap the base itself.
    shiftMonotonicBaseTo(Long.MAX_VALUE - 5_000L);

    CacheEntry entry = EntryDraft.blank(null).value("v").version(1L).ttl(10_000L, 0L).build();

    assertThat(entry.getHardExpireAtMs()).isEqualTo(Long.MAX_VALUE);
    assertThat(entry.getHardTtlMs()).isEqualTo(10_000L);
  }

  private static void shiftMonotonicBaseTo(long target) {
    TimeSource.setTimeOffsetForTest(0L, target - TimeSource.monotonicMillis());
  }
}
