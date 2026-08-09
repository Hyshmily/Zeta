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
import java.util.Random;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * WaveCounter simulator (2026-08-08, Caffeine's {@code simulator} module
 * discipline): drives the REAL WaveCounter with synthetic workloads —
 * a stable Zipfian universe, alternating burst/quiet phases, and rotating
 * drift phases — and records the adaptive controls' trajectories (tide
 * delay, promotion floor, renewal rate) per tide.  The assertions are
 * loose invariants (bounds and direction, never exact constants): the
 * simulator is the evidence harness for the TidePacer / MoonsTidalForce
 * constants, not a golden-output test.
 */
class WaveCounterSimulatorTest {

  private static final int HOT_LIMIT = 1024;
  private static final int BURST_KEYS = 25_000; // > EARLY_TIDE_THRESHOLD_KEYS (20k) → 50ms
  private static final int NOISE_KEYS = 1_000;

  /** Precomputed-CDF Zipfian generator (rank-based, alpha = skew). */
  private static final class Zipf {
    private final double[] cdf;
    private final Random random = new Random(42);

    Zipf(int items, double alpha) {
      double[] weights = new double[items];
      double sum = 0;
      for (int i = 1; i <= items; i++) {
        weights[i - 1] = 1.0 / Math.pow(i, alpha);
        sum += weights[i - 1];
      }
      cdf = new double[items];
      double acc = 0;
      for (int i = 0; i < items; i++) {
        acc += weights[i] / sum;
        cdf[i] = acc;
      }
    }

    /** Draw a rank in [0, items). */
    int next() {
      double u = random.nextDouble();
      int lo = 0;
      int hi = cdf.length - 1;
      while (lo < hi) {
        int mid = (lo + hi) >>> 1;
        if (cdf[mid] < u) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
      return lo;
    }
  }

  /** Per-tide observation of the adaptive controls. */
  private record Obs(int tide, String phase, int keys, long delayMs, int floor, double renewal) {}

  private WaveCounter counter;
  private List<Map<String, Long>> batches;

  private void newCounter() {
    batches = new ArrayList<>();
    counter = new WaveCounter(batches::add);
  }

  // ---------------- workload 1: stable Zipfian universe ----------------

  /**
   * A stable skewed universe (alpha = 1) must keep the hot set healthy:
   * renewal stays at/above the target, the promotion floor never moves
   * off 10, the tide cadence stays within the base bounds, and the top
   * key stays a member tide after tide.
   */
  @Test
  void stableZipf_keepsHealthy() throws Exception {
    newCounter();
    Zipf zipf = new Zipf(2_000, 1.0);
    List<Obs> obs = new ArrayList<>();
    for (int t = 0; t < 40; t++) {
      for (int i = 0; i < 2_000; i++) {
        counter.count("k" + zipf.next(), 1);
      }
      invokeDeliver(counter);
      obs.add(observe("zipf", t));
      assertThat(isBeaconMember(counter, "k0")).as("zipf top key stays hot at tide " + t).isTrue();
    }
    double avgRenewal = obs.stream().mapToDouble(Obs::renewal).average().orElse(0);
    System.out.println("stableZipf: avgRenewal=" + String.format("%.2f", avgRenewal));
    for (Obs o : obs) {
      assertThat(o.delayMs()).as("delay within [50, 500]").isBetween(50L, 500L);
      assertThat(o.floor()).as("floor never moves off 10 on a healthy set").isEqualTo(10);
    }
    assertThat(avgRenewal).as("healthy universe keeps renewal at/above target").isGreaterThanOrEqualTo(0.5);
  }

  // ---------------- workload 2: burst / quiet phase shifts ----------------

  /**
   * Burst-quiet alternation must show the pacer's regimes: a burst
   * attacks to 50ms and holds it while sustained; once the burst stops,
   * non-empty quiet tides release the fast cadence gradually (never a
   * snap to 500ms); sustained EMPTY tides stretch to the 2x ladder cap;
   * a new burst attacks again.
   */
  @Test
  void burstQuiet_attacksAndReleases() throws Exception {
    newCounter();
    List<Long> burst = new ArrayList<>();
    List<Long> release = new ArrayList<>();
    long stretched = 0;
    for (int round = 0; round < 2; round++) {
      for (int t = 0; t < 4; t++) { // burst: 25k distinct keys per tide
        for (int i = 0; i < BURST_KEYS; i++) {
          counter.count("b" + i, 1);
        }
        invokeDeliver(counter);
        burst.add(observe("burst", t).delayMs());
      }
      for (int t = 0; t < 5; t++) { // quiet but non-empty: the release
        for (int i = 0; i < 30; i++) {
          counter.count("q" + i, 1);
        }
        invokeDeliver(counter);
        release.add(observe("release", t).delayMs());
      }
      for (int t = 0; t < 3; t++) { // truly empty tides: the ladder
        invokeDeliver(counter);
        stretched = observe("empty", t).delayMs();
      }
    }
    System.out.println(
      "burstQuiet: burst=" + burst.subList(0, 4) + " release=" + release.subList(0, 5) + " stretched=" + stretched
    );
    assertThat(burst.get(0)).as("burst attacks to 50ms on the very next tide").isEqualTo(50);
    assertThat(burst.get(3)).as("sustained burst keeps the fast cadence").isEqualTo(50);
    for (long d : release) {
      assertThat(d).as("release stays within the ramp bounds").isBetween(50L, 500L);
    }
    assertThat(release.get(1)).as("release is gradual, not a snap back").isGreaterThan(release.get(0));
    assertThat(release.get(4)).as("release keeps rising toward the base").isGreaterThan(release.get(1));
    assertThat(release.get(4)).as("release has not reached the base yet").isLessThan(500L);
    assertThat(stretched).as("empty-tide ladder stretches to the 2x cap").isEqualTo(1_000);
    assertThat(burst.get(4)).as("each new burst re-attacks to 50ms").isEqualTo(50);
  }

  // ---------------- workload 3: rotating drift phases ----------------

  /**
   * Rotating hot sets with quiet GAP tides: each gap (previous leaders
   * quiet, only noise counted) distresses the governor, but the admit
   * keeps the floor at the histogram boundary — the floor never leaves
   * the base across the whole rotation, and every phase leader is
   * promoted by the end of its phase.
   */
  @Test
  void driftRotation_governorStaysAtBase() throws Exception {
    newCounter();
    int minFloor = Integer.MAX_VALUE;
    int maxFloor = Integer.MIN_VALUE;
    for (int phase = 0; phase < 6; phase++) {
      for (int i = 0; i < NOISE_KEYS; i++) { // gap: only noise keys
        counter.count("k" + i, 5);
      }
      invokeDeliver(counter);
      Obs gap = observe("gap", 0);
      minFloor = Math.min(minFloor, gap.floor());
      maxFloor = Math.max(maxFloor, gap.floor());
      for (int t = 0; t < 10; t++) {
        for (int i = 0; i < 20; i++) { // the current phase's hot set
          counter.count("p" + phase + "-" + i, 80);
        }
        for (int i = 0; i < NOISE_KEYS; i++) {
          counter.count("k" + i, 5);
        }
        invokeDeliver(counter);
        Obs o = observe("healthy", t);
        minFloor = Math.min(minFloor, o.floor());
        maxFloor = Math.max(maxFloor, o.floor());
      }
      assertThat(isBeaconMember(counter, "p" + phase + "-0"))
        .as("phase leader must be promoted by the end of its phase")
        .isTrue();
    }
    System.out.println("driftRotation: floor range [" + minFloor + ", " + maxFloor + "]");
    assertThat(minFloor).as("floor never leaves the base").isEqualTo(10);
    assertThat(maxFloor).as("no ratchet — the admit holds the floor at the boundary").isEqualTo(10);
  }

  // ---------------- observation helpers ----------------

  /** Records the adaptive controls after a delivered tide (non-mutating). */
  private Obs observe(String phase, int tide) throws Exception {
    Map<String, Long> snapshot = batches.isEmpty() ? Map.of() : batches.get(batches.size() - 1);
    return new Obs(tide, phase, snapshot.size(), delayOf(counter), floorOf(counter), renewalOf(counter, snapshot));
  }

  /**
   * The delay the pacer would command for the NEXT tide from its current
   * (post-tide) state: the empty-tide stretch if the ladder is armed,
   * else the ramp at the smoothed reference.  Reads only — never folds,
   * so observing cannot perturb the simulation.
   */
  private static long delayOf(WaveCounter c) throws Exception {
    Object pacer = fieldOf(c, "pacer");
    int emptyStreak = (int) pacerField(pacer, "emptyStreak");
    if (emptyStreak > 0) {
      long stretched = 500L << Math.min(emptyStreak - 1, 1);
      return Math.min(stretched, 1_000L);
    }
    double smoothed = (double) pacerField(pacer, "smoothed");
    return c.computeNextTideDelayMs((int) smoothed);
  }

  private static Object pacerField(Object pacer, String name) throws Exception {
    Field f = pacer.getClass().getDeclaredField(name);
    f.setAccessible(true);
    return f.get(pacer);
  }

  /** Renewal = snapshot keys that are beacon members / active slots (post-tide view). */
  private double renewalOf(WaveCounter c, Map<String, Long> snapshot) throws Exception {
    int memberKeys = 0;
    for (String k : snapshot.keySet()) {
      if (isBeaconMember(c, k)) {
        memberKeys++;
      }
    }
    int slots = activeBeaconSize(c);
    return slots == 0 ? 0.0 : (double) memberKeys / slots;
  }

  private static int floorOf(WaveCounter c) throws Exception {
    Object governor = fieldOf(c, "moonsTidalForce");
    Method m = governor.getClass().getDeclaredMethod("floor");
    m.setAccessible(true);
    return (int) m.invoke(governor);
  }

  private static Object fieldOf(WaveCounter c, String name) throws Exception {
    Field f = WaveCounter.class.getDeclaredField(name);
    f.setAccessible(true);
    return f.get(c);
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
    long[] beacon = (long[]) fieldOf(c, "beacon");
    int mask = (int) fieldOf(c, "beaconMask");
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
    long[] beacon = (long[]) fieldOf(c, "beacon");
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
