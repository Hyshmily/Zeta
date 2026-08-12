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
package io.github.hyshmily.zeta.hotkeydetector.doublebuffer;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Density-refined promotion boundary and incumbent-first promotion
 * (ADR-0042): when the log2-boundary bucket overflows the remaining
 * hotLimit slots, a linear sub-histogram refines the threshold to the
 * actual ~k-th largest count (keys below the refined boundary are
 * excluded instead of admitted in HashMap iteration order), and when the
 * refined boundary still cuts a tie-band wider than the remaining slots,
 * renewing members are re-promoted before newcomers — the hot set stays
 * stable under flat distributions instead of rotating a random sample
 * every scan tide.
 */
class WaveCounterDensityBoundaryTest {

  private WaveCounter counter;

  @BeforeEach
  void setUp() {
    counter = new WaveCounter(ignored -> {});
  }

  /**
   * The refined boundary excludes keys below the ~k-th largest count: 500
   * @30 + 524 @25 fill hotLimit exactly; the 1000 keys @20 must NOT be
   * promoted — the power-of-two boundary (16) would admit them all and let
   * the iteration-order break pick the winners.  The refined threshold
   * lands at 25.  (Unpromoted keys still read as members via the beacon's
   * ~0.37% false-positive rate — asserted with a 2% tolerance.)
   */
  @Test
  void refinement_excludesKeysBelowRefinedBoundary() throws Exception {
    countKeys("hi-", 500, 30);
    countKeys("mid-", 524, 25);
    countKeys("lo-", 1000, 20);
    invokeDeliver(counter);

    assertThat(allMembers("hi-", 500)).as("keys above the boundary promoted").isTrue();
    assertThat(allMembers("mid-", 524)).as("keys at the refined boundary promoted").isTrue();
    assertThat(memberCount("lo-", 1000)).as("keys below the refined boundary excluded").isLessThanOrEqualTo(20);
    assertThat(activeBeaconSize(counter)).as("promotion capped at hotLimit").isLessThanOrEqualTo(1024);
  }

  /**
   * Incumbent-first stability under a FLAT distribution: 3000 keys at the
   * same count — every promotion is a coin flip in iteration order, so
   * without the renewal-first pass the scan tide re-promotes a NEW random
   * sample (churn); with it, the same 1024 keys renew forever.  Tides:
   * 1 promotes the first sample, 2 is the saturated skip (decay 2→1), 3 is
   * the scan tide — the member set must be unchanged.
   */
  @Test
  void flatDistribution_hotSetStaysStableAcrossScanTides() throws Exception {
    int n = 3000;
    countKeys("k-", n, 25);
    invokeDeliver(counter); // tide 1: promote 1024 (iteration order)
    assertThat(activeBeaconSize(counter)).isLessThanOrEqualTo(1024);
    Set<String> afterTide1 = members("k-", n);

    countKeys("k-", n, 25);
    invokeDeliver(counter); // tide 2: saturated skip tide
    countKeys("k-", n, 25);
    invokeDeliver(counter); // tide 3: scan tide (renewal-first)

    Set<String> afterTide3 = members("k-", n);
    assertThat(afterTide3).as("renewals before newcomers — no rotation").isEqualTo(afterTide1);
    assertThat(activeBeaconSize(counter)).as("no capacity ratchet across tides").isLessThanOrEqualTo(1024);
  }

  /**
   * A fallen incumbent yields its slot: after the old set drops below the
   * refined boundary, the new set takes over.  Tide 1 promotes the old set;
   * tide 2 is the saturated skip (old evidence decays 2→1); tide 3 scans:
   * the old keys (now below the threshold) are NOT renewed and decay out,
   * the new keys enter (500 @30 + 524 of the 600 @25 — the break cuts the
   * @25 tie-band; promoted keys read as members with no false negatives).
   */
  @Test
  void fallenIncumbents_yieldSlots_toNewcomers() throws Exception {
    countKeys("old-", 500, 30);
    countKeys("old2-", 524, 25);
    invokeDeliver(counter); // tide 1: old set promoted

    countKeys("old-", 500, 20);
    countKeys("old2-", 524, 20);
    countKeys("new-", 500, 30);
    countKeys("new2-", 600, 25);
    invokeDeliver(counter); // tide 2: saturated skip (old evidence 2→1)

    countKeys("old-", 500, 20);
    countKeys("old2-", 524, 20);
    countKeys("new-", 500, 30);
    countKeys("new2-", 600, 25);
    invokeDeliver(counter); // tide 3: scan — old falls, new enters

    assertThat(memberCount("old-", 500)).as("fallen count-20 keys must leave").isLessThanOrEqualTo(10);
    assertThat(memberCount("old2-", 524)).as("fallen count-20 keys must leave").isLessThanOrEqualTo(12);
    assertThat(allMembers("new-", 500)).as("risen keys enter").isTrue();
    assertThat(memberCount("new2-", 600)).as("524 of the 600 @25 ties enter").isBetween(524, 600);
    assertThat(activeBeaconSize(counter)).isLessThanOrEqualTo(1024);
  }

  /**
   * Non-overflow path is unchanged: a skewed universe whose boundary bucket
   * fits exactly in the remaining slots takes the single-pass scan — the
   * top 1024 keys promote, floor-excluded noise keys never do (the 2000
   * noise keys read as members only via the beacon's ~0.37% false-positive
   * rate — 2% tolerance).
   */
  @Test
  void nonOverflow_skewed_singlePassPreserved() throws Exception {
    countKeys("a-", 300, 1000);
    countKeys("b-", 724, 600);
    countKeys("noise-", 2000, 10);
    invokeDeliver(counter);

    assertThat(allMembers("a-", 300)).isTrue();
    assertThat(allMembers("b-", 724)).isTrue();
    assertThat(memberCount("noise-", 2000)).as("floor-excluded noise never promoted").isLessThanOrEqualTo(40);
    assertThat(activeBeaconSize(counter)).isLessThanOrEqualTo(1024);
  }

  /**
   * Floor-dominance gate: when the floor sits ABOVE the histogram boundary
   * (all traffic in one low bucket), refinement is skipped and the single
   * pass promotes at the floor — the flat @12 pool fills hotLimit, nothing
   * exceeds it.
   */
  @Test
  void floorDominance_skipsRefinement_behaviorPreserved() throws Exception {
    countKeys("f-", 1100, 12);
    invokeDeliver(counter);

    assertThat(activeBeaconSize(counter)).as("promotion at the floor, capped").isLessThanOrEqualTo(1024);
    assertThat(activeBeaconSize(counter)).isGreaterThan(0);
  }

  // ---------------- helpers ----------------

  private static void countKeys(WaveCounter c, String prefix, int n, long delta) {
    for (int i = 0; i < n; i++) {
      c.count(prefix + i, delta);
    }
  }

  private void countKeys(String prefix, int n, long delta) {
    countKeys(counter, prefix, n, delta);
  }

  private static void invokeDeliver(WaveCounter c) {
    try {
      Method m = WaveCounter.class.getDeclaredMethod("tide");
      m.setAccessible(true);
      m.invoke(c);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean isBeaconMember(WaveCounter c, String key) throws Exception {
    Field f = WaveCounter.class.getDeclaredField("beacon");
    f.setAccessible(true);
    long[] beacon = (long[]) f.get(c);
    Field m = WaveCounter.class.getDeclaredField("beaconMask");
    m.setAccessible(true);
    int mask = (int) m.get(c);
    int h = mixHash(key.hashCode());
    int bit1 = h & mask;
    int count = (int) ((beacon[bit1 >>> 4] >>> ((bit1 & 15) << 2)) & 0x3);
    if (count == 0) {
      return false;
    }
    int bit2 = rehash(h) & mask;
    return ((beacon[bit2 >>> 4] >>> (((bit2 & 15) << 2) + 2)) & 0x3) >= 1;
  }

  private static int activeBeaconSize(WaveCounter c) throws Exception {
    Field f = WaveCounter.class.getDeclaredField("beacon");
    f.setAccessible(true);
    long[] beacon = (long[]) f.get(c);
    int active = 0;
    for (int i = 0; i < beacon.length; i++) {
      long word = beacon[i];
      if (word == 0) {
        continue;
      }
      for (int r = 0; r < 16; r++) {
        if (((word >>> (r << 2)) & 0x3) != 0) {
          active++;
        }
      }
    }
    return active;
  }

  private boolean allMembers(String prefix, int n) throws Exception {
    for (int i = 0; i < n; i++) {
      if (!isBeaconMember(counter, prefix + i)) {
        return false;
      }
    }
    return true;
  }

  private int memberCount(String prefix, int n) throws Exception {
    int count = 0;
    for (int i = 0; i < n; i++) {
      if (isBeaconMember(counter, prefix + i)) {
        count++;
      }
    }
    return count;
  }

  private Set<String> members(String prefix, int n) throws Exception {
    Set<String> set = new HashSet<>();
    for (int i = 0; i < n; i++) {
      String key = prefix + i;
      if (isBeaconMember(counter, key)) {
        set.add(key);
      }
    }
    return set;
  }

  private static int mixHash(int h) {
    h ^= h >>> 16;
    h *= 0x85ebca6b;
    h ^= h >>> 13;
    h *= 0xc2b2ae35;
    h ^= h >>> 16;
    return h;
  }

  private static int rehash(int h) {
    h *= 0x31848bab;
    h ^= h >>> 14;
    return h;
  }
}
