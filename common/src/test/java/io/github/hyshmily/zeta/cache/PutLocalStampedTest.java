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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.util.NodeIdentity;
import io.github.hyshmily.zeta.util.clock.MonotonicStampClock;
import io.github.hyshmily.zeta.util.clock.StampClock;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * design decisions: version stamps invariant I4: {@code putLocal} is a write and must carry a real
 * stamp; it must never push an entry back into the unknown state.
 */
class PutLocalStampedTest {

  @Test
  @DisplayName("putLocal stamps a created entry")
  void putLocal_stampsCreatedEntry() {
    try (
      StampTestSupport support = new StampTestSupport(
        new MonotonicStampClock(NodeIdentity.getNodeId(), MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS)
      )
    ) {
      support.cache.putLocal("k", "local", 0L, 0L);

      CacheEntry entry = (CacheEntry) support.l1.getIfPresent("k");
      assertThat(entry).isNotNull();
      assertThat(entry.getDataVersion()).isPositive();
    }
  }

  @Test
  @DisplayName("putLocal stamps an updated entry")
  void putLocal_stampsUpdatedEntry() {
    try (
      StampTestSupport support = new StampTestSupport(
        new MonotonicStampClock(NodeIdentity.getNodeId(), MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS)
      )
    ) {
      support.l1.put("k", StampTestSupport.entry("old", 5L));

      support.cache.putLocal("k", "new", 0L, 0L);

      CacheEntry entry = (CacheEntry) support.l1.getIfPresent("k");
      assertThat(entry.getValue()).isEqualTo("new");
      assertThat(entry.getDataVersion()).isGreaterThan(5L);
    }
  }

  @Test
  @DisplayName("putLocal yields to a resident written at a higher stamp")
  void putLocal_yieldsToHigherResidentStamp() {
    // The write allocates 11; the resident already holds 100. A putLocal that
    // was descheduled between nextStamp() and compute must not rewind it.
    AtomicLong sequence = new AtomicLong(10L);
    StampClock clock = mock(StampClock.class);
    when(clock.nextStamp()).thenAnswer(inv -> sequence.incrementAndGet());

    try (StampTestSupport support = new StampTestSupport(clock)) {
      support.l1.put("k", StampTestSupport.entry("newer", 100L));

      support.cache.putLocal("k", "stale", 0L, 0L);

      CacheEntry entry = (CacheEntry) support.l1.getIfPresent("k");
      assertThat(entry.getValue()).isEqualTo("newer");
      assertThat(entry.getDataVersion()).isEqualTo(100L);
    }
  }

  @Test
  @DisplayName("putLocal is refused when the key was invalidated at a higher stamp")
  void putLocal_refusedAfterInvalidation() {
    AtomicLong sequence = new AtomicLong(10L);
    StampClock clock = mock(StampClock.class);
    when(clock.nextStamp()).thenAnswer(inv -> sequence.incrementAndGet());

    try (StampTestSupport support = new StampTestSupport(clock)) {
      // The invalidation outranks the stamp this putLocal will allocate.
      support.lifecycle.recordInvalidation("k", 100L);

      support.cache.putLocal("k", "resurrected", 0L, 0L);

      assertThat(support.l1.getIfPresent("k")).isNull();
    }
  }
}
