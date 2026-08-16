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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Beacon decay sweep period (ADR-0049): the halving decay runs on every
 * other promoted tide, so the hot-set memory is 4 tides.  Periodic earners
 * (earn/miss/miss patterns) stay hot-routed on their earn tides (the
 * sandbox-validated headline), stable keys never leave, drifted keys still
 * expire within 4 non-empty tides, the skip-tide active count matches the
 * ground-truth room scan, and clear() resets the sweep phase.
 */
class WaveCounterDecayPeriodTest {

  private List<Map<String, Long>> batches;
  private Consumer<Map<String, Long>> consumer;
  private WaveCounter counter;

  @BeforeEach
  void setUp() {
    batches = new ArrayList<>();
    consumer = batches::add;
    counter = new WaveCounter(consumer);
  }

  @AfterEach
  void tearDown() {
    counter.destroy();
  }

  /**
   * The 4-tide memory contract: a promoted key survives up to three quiet
   * tides (still a member after the third) and is gone by the fifth — the
   * decay sweep runs at tides 3 and 5 of the quiet run (tide 1 promotes).
   */
  @Test
  void quietKey_survivesThreeTides_leavesByFifth() throws Exception {
    counter.count("top", 80);
    for (int i = 0; i < 1000; i++) {
      counter.count("k" + i, 5);
    }
    invokeDeliver(counter); // tide 1: decay sweep (nothing to decay) + promote
    assertThat(isBeaconMember(counter, "top")).as("promoted at tide 1").isTrue();
    for (int t = 2; t <= 4; t++) {
      quietTide();
      assertThat(isBeaconMember(counter, "top"))
        .as("4-tide memory: still a member after " + (t - 1) + " quiet tides")
        .isTrue();
    }
    quietTide(); // tide 5: the second decay sweep of the quiet run -> 1 -> 0
    assertThat(isBeaconMember(counter, "top")).as("gone by tide 5").isFalse();
  }

  /**
   * A key re-promoted every tide must never be evicted by the parity: on a
   * decay tide the sweep halves 2 -> 1 but the same scan's renewal re-seeds
   * 1 -> 2, so the evidence never reaches 0 on either phase.
   */
  @Test
  void stableHotKey_renewsThroughBothParities() throws Exception {
    for (int t = 0; t < 6; t++) {
      counter.count("top", 80);
      for (int i = 0; i < 1000; i++) {
        counter.count("k" + i, 5);
      }
      invokeDeliver(counter);
      assertThat(isBeaconMember(counter, "top")).as("stable hot key at tide " + (t + 1)).isTrue();
    }
  }

  /**
   * ADR-0049 headline behavior: an earn/miss/miss periodic earner stays
   * hot-routed.  With the legacy every-tide decay it was decayed out by its
   * second quiet tide and cold-routed on EVERY earn tide (the promotion at
   * the earn tide's end serves the NEXT tide, which is quiet — the sandbox
   * measured 0% hot coverage for this pattern, mem_adapt_exp.py).  The
   * every-other-tide sweep keeps the evidence alive across the gap:
   * 2 -> (skip) 2 -> (decay) 1 -> member on the earn tide.
   */
  @Test
  void sawtoothEarner_earnMissMiss_staysHot_thenDriftsOut() throws Exception {
    for (int cycle = 0; cycle < 3; cycle++) {
      counter.count("top", 80);
      for (int i = 0; i < 1000; i++) {
        counter.count("k" + i, 5);
      }
      invokeDeliver(counter); // earn tide
      assertThat(isBeaconMember(counter, "top")).as("member on earn tide " + cycle).isTrue();
      quietTide(); // miss 1
      quietTide(); // miss 2
      assertThat(isBeaconMember(counter, "top"))
        .as("earn1/miss2: still a member before the next earn tide")
        .isTrue();
    }
    // once the pattern stops, the 4-tide bound still applies
    for (int t = 0; t < 5; t++) {
      quietTide();
    }
    assertThat(isBeaconMember(counter, "top")).as("drifted earner leaves within 4 quiet tides").isFalse();
  }

  /**
   * The skip-tide active count must equal the ground-truth room scan in
   * every evidence state — including evidence 2 (bit1 set, bit0 clear), the
   * case a naive bit0 popcount would miss: the SWAR identity
   * {@code (word | word >>> 1) & ACTIVE_LANE_MASK} tests exactly
   * "evidence >= 1".
   */
  @Test
  void countActive_matchesManualRoomScan() throws Exception {
    markHot(counter, "aa");
    markHot(counter, "bb");
    markHot(counter, "cc");
    assertActiveMatchesManual();
    decayCounts(counter); // 2 -> 1 (still active)
    assertActiveMatchesManual();
    decayCounts(counter); // 1 -> 0
    assertActiveMatchesManual();
    markHot(counter, "aa"); // mixed: fresh + dead
    markHot(counter, "bb");
    assertActiveMatchesManual();
    decayCounts(counter); // 2 -> 1: evidence-1 lanes must stay counted
    assertActiveMatchesManual();
  }

  /**
   * clear() must reset the sweep clock (blank-slate contract): the clock
   * starts at 1 (the next promoted tide runs the decay), one scan runs the
   * decay and restarts the clock at DECAY_PERIOD, the next scan is the
   * skip tide and counts the clock down, and clear() restores 1 so the
   * next promoted tide decays again.
   */
  @Test
  void clear_resetsDecayPhase() throws Exception {
    assertThat(sweepCountdownOf(counter)).as("first promoted tide decays").isEqualTo(1);
    quietTide(); // scan 1: the decay runs, the clock restarts at DECAY_PERIOD
    assertThat(sweepCountdownOf(counter)).isEqualTo(2);
    quietTide(); // scan 2: the skip tide — evidence freezes, the clock counts down
    assertThat(sweepCountdownOf(counter)).isEqualTo(1);
    quietTide(); // scan 3: the decay runs again (the 2-tide cycle)
    assertThat(sweepCountdownOf(counter)).isEqualTo(2);
    counter.clear();
    assertThat(sweepCountdownOf(counter)).as("clear resets the sweep clock").isEqualTo(1);
  }

  // ---------------- helpers ----------------

  /** A quiet tide: >= MIN_PROMOTION_KEYS distinct keys so the decay sweep runs. */
  private void quietTide() {
    for (int i = 0; i < 20; i++) {
      counter.count("k" + i, 1);
    }
    invokeDeliver(counter);
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

  /** Write seed 2 to both role evidences of the key's two rooms. */
  private static void markHot(WaveCounter c, String key) throws Exception {
    Field m = WaveCounter.class.getDeclaredField("beaconMask");
    m.setAccessible(true);
    int mask = (int) m.get(c);
    int h = mixHash(key.hashCode());
    setRole(c, h & mask, 0, 2);
    setRole(c, rehash(h) & mask, 2, 2);
  }

  /** Write a 2-bit role evidence (0-3) at the given offset of the room. */
  private static void setRole(WaveCounter c, int bit, int offset, int value) throws Exception {
    Field f = WaveCounter.class.getDeclaredField("beacon");
    f.setAccessible(true);
    long[] beacon = (long[]) f.get(c);
    int idx = bit >>> 4;
    int shift = ((bit & 15) << 2) + offset;
    beacon[idx] = (beacon[idx] & ~(0x3L << shift)) | ((long) value << shift);
  }

  private static int countActive(WaveCounter c) throws Exception {
    Method m = WaveCounter.class.getDeclaredMethod("countActive", int.class);
    m.setAccessible(true);
    return (int) m.invoke(c, 1);
  }

  private static int decayCounts(WaveCounter c) throws Exception {
    Method m = WaveCounter.class.getDeclaredMethod("decayCounts");
    m.setAccessible(true);
    return (int) m.invoke(c);
  }

  /** Ground truth: manual scan of the count lanes (mirrors decayCounts' consumers). */
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

  private void assertActiveMatchesManual() throws Exception {
    assertThat(countActive(counter))
      .as("SWAR active count equals the manual room scan")
      .isEqualTo(activeBeaconSize(counter));
  }

  private static int sweepCountdownOf(WaveCounter c) throws Exception {
    Field f = WaveCounter.class.getDeclaredField("sweepCountdown");
    f.setAccessible(true);
    return f.getInt(c);
  }

  // Hash helpers mirroring WaveCounter.mixHash/rehash (the production methods
  // are private; each test class carries its own copy, see the other
  // WaveCounter tests).
  private static int mixHash(int h) {
    h ^= h >>> 17;
    h *= 0xed5ad4bb;
    h ^= h >>> 11;
    h *= 0xac4c1b51;
    h ^= h >>> 15;
    return h;
  }

  private static int rehash(int h) {
    h *= 0x31848bab;
    h ^= h >>> 14;
    return h;
  }
}
