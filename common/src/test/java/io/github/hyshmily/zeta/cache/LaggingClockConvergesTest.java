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

import io.github.hyshmily.zeta.util.NodeIdentity;
import io.github.hyshmily.zeta.util.clock.MonotonicStampClock;
import io.github.hyshmily.zeta.util.clock.StampClock;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * design decisions: version stamps invariant I3: a node whose wall clock lags still outranks every
 * stamp it has observed, because the clock merges the observed floor.
 */
class LaggingClockConvergesTest {

  @Test
  @DisplayName("a lagging node's next write outranks the stamp it observed")
  void observedFloor_raisesTheNextStamp() {
    MonotonicStampClock clock = new MonotonicStampClock(
      NodeIdentity.getNodeId(),
      MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS
    );
    // A peer 30s ahead of local time: inside the 60s anti-drift bound, so the
    // floor still merges. A stamp beyond the bound is refused by contract.
    long observedTs = System.currentTimeMillis() - StampClock.EPOCH + 30_000L;
    long observed = (observedTs << StampClock.TIMESTAMP_SHIFT) | (1L << StampClock.SEQUENCE_BITS_FOR_RANGE);
    clock.observe(observed);

    long next = clock.nextStamp();

    assertThat(next).isGreaterThan(observed);
    // A peer that holds the observed stamp accepts this write (no rollback).
    assertThat(VersionGuard.shouldSkipForRefresh(StampTestSupport.entry("peer", observed), next)).isFalse();
  }
}
