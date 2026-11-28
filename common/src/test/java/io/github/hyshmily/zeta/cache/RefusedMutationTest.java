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
package io.github.hyshmily.zeta.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.StalePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.scheduler.WarmScheduler;
import io.github.hyshmily.zeta.util.clock.StampClock;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A refused L1 mutation must not run the write tail. The tail — dedup invalidation, renewal
 * re-arm, broadcast — belongs to a write that landed, and a refused write leaves the resident
 * instance in place, so only the mutation's post-image answers "did this apply?".
 *
 * <p>The version number cannot answer it: a resident may already carry the incoming stamp (two
 * nodes minting the same stamp in one millisecond after a node-id collision, or a tombstone
 * recorded at that stamp), and then the refusal looks exactly like the write it refused.
 */
class RefusedMutationTest {

  private static GetPolicy options() {
    return GetPolicy.of(0L, 0L, false, StalePolicy.SOFT_REFRESH);
  }

  /** A clock returning {@code 6, 7, 7, 8, ...}: the caller fixes the stamp under test. */
  private static StampClock clockAt(long stamp) {
    AtomicLong sequence = new AtomicLong(stamp - 1L);
    StampClock clock = mock(StampClock.class);
    when(clock.nextStamp()).thenAnswer(invocation -> sequence.incrementAndGet());
    when(clock.nextStamps(anyInt())).thenAnswer(invocation -> {
      int count = invocation.getArgument(0);
      return sequence.addAndGet(count) - count + 1;
    });
    return clock;
  }

  @Test
  @DisplayName("putThrough refused by a same-stamp tombstone does not retire the key's dedup entry")
  void putThrough_refusedBySameStampTombstone_shouldNotTouchDedup() {
    try (StampTestSupport support = new StampTestSupport(clockAt(7L))) {
      support.l1.put("k", StampTestSupport.entry("resident", 7L));
      support.lifecycle.recordInvalidation("k", 7L);

      support.cache.putThrough("k", "refused", 0L, 0L);

      CacheEntry resident = support.l1.getIfPresent("k");
      assertThat(resident).isNotNull();
      assertThat(resident.getValue()).as("the tombstoned write must not land").isEqualTo("resident");
      verify(support.singleFlight, never()).invalidate(anyString());
    }
  }

  @Test
  @DisplayName("a load fill refused by a same-version resident does not re-arm renewal")
  void loadFill_refusedBySameVersionResident_shouldNotRearmRenewal() {
    WarmScheduler warmScheduler = mock(WarmScheduler.class);
    try (StampTestSupport support = new StampTestSupport(clockAt(7L))) {
      support.cache.setWarmScheduler(warmScheduler);

      String served = support.cache.get("k", () -> {
        // A same-stamp resident lands while the reader runs: the fill below it must lose.
        support.l1.put("k", StampTestSupport.entry("raced", 7L));
        return "loaded";
      }, options());

      assertThat(served).as("the caller still gets the value it read").isEqualTo("loaded");
      CacheEntry resident = support.l1.getIfPresent("k");
      assertThat(resident).isNotNull();
      assertThat(resident.getValue()).as("the equal-version fill must not overwrite").isEqualTo("raced");
      verify(warmScheduler, never()).scheduleIfEligible(anyString(), any(), any());
    }
  }
}
