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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The ADR-0056 kth-shortcut: the promotion sweep collects every key at or
 * above the PREVIOUS tide's exact boundary into a filtered list, and
 * {@code selectBoundary} takes the exact selection over that list instead of
 * the boundary-bucket view whenever the list provably holds this cycle's
 * k-th largest.  These tests pin the shortcut to the reference (bucket-view)
 * path: byte-identical pass outputs on random and adversarial snapshots,
 * exactness against an independently computed k-th largest, the fire gate
 * (which must exclude the bucket-fits corner, whose boundary is the bucket
 * MINIMUM, not the k-th largest), and the drift/floor-dominance/stale-seed
 * edges.
 */
class WaveCounterKthShortcutTest {

  private static final int HOT_LIMIT = 1024;
  private static final int FLOOR = 10;

  private WaveCounter counter;

  @BeforeEach
  void setUp() {
    counter = new WaveCounter(ignored -> {});
  }

  // ---------------- the shortcut vs the reference path ----------------

  /**
   * Byte-identical parity on random snapshots: for every tide the shortcut
   * pass (seeded with the previous tide's boundary) and the reference pass
   * (no previous boundary) must produce identical boundary/threshold/
   * overflow/blockedKeys/above/boundaryBucket — the full governor input
   * contract.  The seed follows the honest tide sequence (each tide's
   * boundary feeds the next tide's filter).  The boundary is additionally
   * asserted equal to an independently computed k-th largest on every tide
   * (the shipped boundary is exact on every reachable path — the ADR-0054
   * quickselect semantics plus the crossing invariant).
   */
  @Test
  void shortcut_outputs_byteIdenticalToReference() throws Exception {
    Random rng = new Random(0x5EED);
    long lastKth = -1;
    int fired = 0;
    for (int t = 0; t < 200; t++) {
      Map<String, Long> snapshot = randomSnapshot(rng);
      setLastKth(counter, lastKth);
      Object shortcut = runPass(counter, snapshot, FLOOR);
      if (fired(shortcut)) {
        fired++;
      }
      setLastKth(counter, -1);
      Object reference = runPass(counter, snapshot, FLOOR);
      assertSamePassOutputs(shortcut, reference, "tide " + t);
      // the shipped boundary is the exact hotLimit-th largest on every
      // reachable path (ADR-0054 semantics + the crossing invariant)
      assertThat(passLong(reference, "boundary"))
        .as("tide %d: boundary is the true k-th largest", t)
        .isEqualTo(trueKth(snapshot, HOT_LIMIT));
      // the next tide's filter seed: THIS tide's boundary (the pass armed it)
      lastKth = passLong(reference, "boundary");
    }
    assertThat(fired).as("the shortcut branch must fire at least once").isGreaterThan(0);
  }

  /**
   * The stable flat workload — the pathological case the shortcut exists
   * for: 3000 keys in ONE log2 bucket, the whole snapshot qualifies the
   * filter (the previous boundary is the common count), so the selection
   * runs over the 3000-key list instead of the full-bucket view — and
   * stays exact (boundary 25, overflow, incumbent split inputs unchanged).
   */
  @Test
  void stableFlatWorkload_shortcutFiresAndIsExact() throws Exception {
    Map<String, Long> snapshot = flatSnapshot(3000, 25);
    setLastKth(counter, -1);
    Object tide1 = runPass(counter, snapshot, FLOOR);
    assertThat(passLong(tide1, "boundary")).as("tide 1 seeds the shortcut").isEqualTo(25);
    assertThat(getLastKth(counter)).as("the pass arms the filter seed").isEqualTo(25);

    Object tide2 = runPass(counter, snapshot, FLOOR);
    assertThat(passInt(tide2, "aboveKthSize")).as("the whole snapshot qualifies the filter").isEqualTo(3000);
    assertThat(fired(tide2)).as("the shortcut fires on the flat worst case").isTrue();
    assertThat(passLong(tide2, "boundary")).isEqualTo(25);
    assertThat(passBool(tide2, "overflow")).as("3000 ties over 1024 slots").isTrue();
    assertThat(passLong(tide2, "boundary")).isEqualTo(trueKth(snapshot, HOT_LIMIT));

    setLastKth(counter, -1);
    Object reference = runPass(counter, snapshot, FLOOR);
    assertSamePassOutputs(tide2, reference, "flat 3000@25");
  }

  /**
   * The bucket-fits corner: the boundary bucket holds exactly {@code need}
   * keys (the crossing invariant — the top-down accumulation stops at the
   * first bucket that reaches {@code hotLimit} — makes
   * {@code bucketSize < need} unreachable), so the shipped boundary IS the
   * bucket minimum AND the true k-th largest; the shortcut fires there too
   * and must reproduce it.  (A fitting bucket holds at most
   * {@code need <= hotLimit} keys, so this corner is cheap either way —
   * the shortcut's value is the overflowing bucket.)
   */
  @Test
  void fitsCorner_shortcutStaysExact() throws Exception {
    // 900@1000 + 124@16: the boundary bucket [16,32) holds exactly
    // need = 124 keys; the 5000@8 below must not move the boundary.
    Map<String, Long> fits = new HashMap<>();
    for (int i = 0; i < 900; i++) {
      fits.put("a" + i, 1000L);
    }
    for (int i = 0; i < 124; i++) {
      fits.put("b" + i, 16L);
    }
    for (int i = 0; i < 5000; i++) {
      fits.put("c" + i, 8L);
    }
    setLastKth(counter, 16);
    Object shortcut = runPass(counter, fits, FLOOR);
    assertThat(passInt(shortcut, "aboveKthSize")).isEqualTo(1024);
    assertThat(fired(shortcut)).as("the list holds hotLimit keys").isTrue();
    assertThat(passLong(shortcut, "boundary")).isEqualTo(16);
    setLastKth(counter, -1);
    Object reference = runPass(counter, fits, FLOOR);
    assertSamePassOutputs(shortcut, reference, "fits corner");
    assertThat(passLong(reference, "boundary"))
      .as("the fits corner's bucket min IS the k-th largest (crossing invariant)")
      .isEqualTo(16);
    assertThat(trueKth(fits, HOT_LIMIT)).as("sanity: the true kth coincides").isEqualTo(16);

    // Off-bottom corner: the snapshot holds FEWER than hotLimit keys — the
    // boundary is the minimum positive count; the list cannot reach
    // hotLimit, so the reference path takes over unchanged.
    Map<String, Long> small = new HashMap<>();
    for (int i = 0; i < 900; i++) {
      small.put("a" + i, 1000L);
    }
    for (int i = 0; i < 100; i++) {
      small.put("b" + i, 16L);
    }
    setLastKth(counter, 8); // below the whole distribution
    Object sc2 = runPass(counter, small, FLOOR);
    assertThat(passInt(sc2, "aboveKthSize")).isEqualTo(1000);
    assertThat(fired(sc2)).as("1000 < hotLimit: the reference path takes over").isFalse();
    assertThat(passLong(sc2, "boundary")).isEqualTo(16);
    setLastKth(counter, -1);
    Object ref2 = runPass(counter, small, FLOOR);
    assertSamePassOutputs(sc2, ref2, "off-bottom corner");
    assertThat(trueKth(small, HOT_LIMIT)).isEqualTo(16);
  }

  /**
   * The multi-bucket overflow corner the shortcut must handle: the boundary
   * bucket overflows AND keys sit in higher buckets (above > 0).  The list
   * selection must land on the hotLimit-th largest of the SNAPSHOT (50),
   * not the need-th largest (1000) — the order statistic the shortcut
   * selects is the k-th largest of the whole snapshot, `above` included.
   */
  @Test
  void multiBucketOverflow_shortcutSelectsSnapshotKth() throws Exception {
    Map<String, Long> snapshot = new HashMap<>();
    for (int i = 0; i < 900; i++) {
      snapshot.put("hi" + i, 1000L);
    }
    for (int i = 0; i < 200; i++) {
      snapshot.put("mid" + i, 50L);
    }
    for (int i = 0; i < 5000; i++) {
      snapshot.put("lo" + i, 25L);
    }
    setLastKth(counter, 50); // the previous boundary: mid-level
    Object shortcut = runPass(counter, snapshot, FLOOR);
    assertThat(fired(shortcut)).as("bucket 200 > need 124, list 1100 >= 1024").isTrue();
    assertThat(passLong(shortcut, "boundary")).as("the hotLimit-th largest of the snapshot").isEqualTo(50);
    setLastKth(counter, -1);
    Object reference = runPass(counter, snapshot, FLOOR);
    assertSamePassOutputs(shortcut, reference, "multi-bucket overflow");
    assertThat(passLong(reference, "boundary")).isEqualTo(trueKth(snapshot, HOT_LIMIT));
  }

  /**
   * The tie-band overflow: 500@32 above 600@16, the boundary bucket holds
   * 600 > need 524 — the shortcut fires and must reproduce the reference's
   * overflow (1100 ties > 1024 slots) and blocked band.
   */
  @Test
  void tieBandOverflow_shortcutMatchesReference() throws Exception {
    Map<String, Long> snapshot = new HashMap<>();
    for (int i = 0; i < 500; i++) {
      snapshot.put("a" + i, 32L);
    }
    for (int i = 0; i < 600; i++) {
      snapshot.put("b" + i, 16L);
    }
    setLastKth(counter, 16);
    Object shortcut = runPass(counter, snapshot, FLOOR);
    assertThat(fired(shortcut)).isTrue();
    setLastKth(counter, -1);
    Object reference = runPass(counter, snapshot, FLOOR);
    assertSamePassOutputs(shortcut, reference, "tie band");
    assertThat(passBool(reference, "overflow")).isTrue();
    assertThat(passLong(reference, "boundary")).isEqualTo(16);
  }

  /**
   * A STALE filter seed stays exact: a seed far below the current kth
   * widens the list (the shortcut fires over a bigger list and still
   * selects exactly); a seed far above empties the list and the reference
   * path takes over — never a wrong boundary.
   */
  @Test
  void staleSeed_shortcutStaysExactOrFallsBack() throws Exception {
    Map<String, Long> snapshot = flatSnapshot(3000, 100);

    setLastKth(counter, 1); // far below the kth: the list is the whole snapshot
    Object wide = runPass(counter, snapshot, FLOOR);
    assertThat(passInt(wide, "aboveKthSize")).isEqualTo(3000);
    assertThat(fired(wide)).isTrue();
    assertThat(passLong(wide, "boundary")).isEqualTo(100);

    setLastKth(counter, 1000); // far above the kth: nothing qualifies
    Object narrow = runPass(counter, snapshot, FLOOR);
    assertThat(passInt(narrow, "aboveKthSize")).isZero();
    assertThat(fired(narrow)).isFalse();
    assertThat(passLong(narrow, "boundary")).isEqualTo(100);

    setLastKth(counter, -1);
    Object reference = runPass(counter, snapshot, FLOOR);
    assertSamePassOutputs(wide, reference, "wide seed");
    assertSamePassOutputs(narrow, reference, "narrow seed");
    assertThat(passLong(reference, "boundary")).isEqualTo(trueKth(snapshot, HOT_LIMIT));
  }

  /**
   * Floor-dominance parity: the floor sits above the kth (threshold = the
   * floor, blocked band = the floor-excluded qualifiers) — the shortcut
   * path must reproduce the reference's band exactly.
   */
  @Test
  void floorDominance_shortcutMatchesReference() throws Exception {
    Map<String, Long> snapshot = new HashMap<>();
    for (int i = 0; i < 500; i++) {
      snapshot.put("a" + i, 32L);
    }
    for (int i = 0; i < 600; i++) {
      snapshot.put("b" + i, 16L);
    }
    setLastKth(counter, 16); // the previous kth, below the raised floor
    Object shortcut = runPass(counter, snapshot, 20);
    assertThat(fired(shortcut)).isTrue();
    setLastKth(counter, -1);
    Object reference = runPass(counter, snapshot, 20);
    assertSamePassOutputs(shortcut, reference, "floor dominance");
    assertThat(passLong(shortcut, "threshold")).as("the floor dominates the threshold").isEqualTo(20);
    assertThat(passInt(shortcut, "blockedKeys"))
      .as("qualifying 1100 - candSize 500 (keys >= floor 20)")
      .isEqualTo(600);
  }

  /**
   * The first scan tide has no previous boundary: the filter is off (empty
   * list, no extra collection) and the reference path arms the seed.
   */
  @Test
  void firstTide_shortcutListEmpty_andArmsSeed() throws Exception {
    setLastKth(counter, -1);
    Object pass = runPass(counter, flatSnapshot(2000, 25), FLOOR);
    assertThat(passInt(pass, "aboveKthSize")).as("no filter on the first tide").isZero();
    assertThat(passLong(pass, "boundary")).isEqualTo(25);
    assertThat(getLastKth(counter)).as("the boundary arms the next tide's filter").isEqualTo(25);
  }

  // ---------------- reflection plumbing ----------------

  private static Class<?> passType() throws Exception {
    return Class.forName("io.github.hyshmily.zeta.hotkeydetector.doublebuffer.WaveCounter$PromotionPass");
  }

  private static Object newPass() throws Exception {
    Constructor<?> ctor = passType().getDeclaredConstructor();
    ctor.setAccessible(true);
    return ctor.newInstance();
  }

  private static int passInt(Object pass, String name) throws Exception {
    Field f = passType().getDeclaredField(name);
    f.setAccessible(true);
    return f.getInt(pass);
  }

  private static long passLong(Object pass, String name) throws Exception {
    Field f = passType().getDeclaredField(name);
    f.setAccessible(true);
    return f.getLong(pass);
  }

  private static boolean passBool(Object pass, String name) throws Exception {
    Field f = passType().getDeclaredField(name);
    f.setAccessible(true);
    return f.getBoolean(pass);
  }

  /** Mirrors {@code promote()}: sweep -> estimate -> select with the given floor. */
  private static Object runPass(WaveCounter c, Map<String, Long> snapshot, int floor) throws Exception {
    Object pass = newPass();
    passIntField(pass, "floor", floor);
    passIntField(pass, "threshold", floor); // promote() pre-sets threshold = floor
    Method sweep = WaveCounter.class.getDeclaredMethod("sweepHistogram", Map.class, passType());
    sweep.setAccessible(true);
    sweep.invoke(c, snapshot, pass);
    Method estimate = WaveCounter.class.getDeclaredMethod("estimateBoundary", passType());
    estimate.setAccessible(true);
    estimate.invoke(c, pass);
    Method select = WaveCounter.class.getDeclaredMethod("selectBoundary", passType());
    select.setAccessible(true);
    select.invoke(c, pass);
    return pass;
  }

  private static void passIntField(Object pass, String name, int value) throws Exception {
    Field f = passType().getDeclaredField(name);
    f.setAccessible(true);
    f.setInt(pass, value);
  }

  private static void setLastKth(WaveCounter c, long v) throws Exception {
    Field f = WaveCounter.class.getDeclaredField("lastKth");
    f.setAccessible(true);
    f.setLong(c, v);
  }

  private static long getLastKth(WaveCounter c) throws Exception {
    Field f = WaveCounter.class.getDeclaredField("lastKth");
    f.setAccessible(true);
    return f.getLong(c);
  }

  /** Whether the shortcut branch fired for the pass (mirrors the production gate). */
  private static boolean fired(Object pass) throws Exception {
    return passInt(pass, "aboveKthSize") >= HOT_LIMIT;
  }

  /** The governor-facing pass outputs the shortcut must reproduce byte-for-byte. */
  private static void assertSamePassOutputs(Object a, Object b, String label) throws Exception {
    assertThat(passLong(a, "boundary")).as(label + ": boundary").isEqualTo(passLong(b, "boundary"));
    assertThat(passLong(a, "threshold")).as(label + ": threshold").isEqualTo(passLong(b, "threshold"));
    assertThat(passBool(a, "overflow")).as(label + ": overflow").isEqualTo(passBool(b, "overflow"));
    assertThat(passInt(a, "blockedKeys")).as(label + ": blockedKeys").isEqualTo(passInt(b, "blockedKeys"));
    assertThat(passLong(a, "above")).as(label + ": above").isEqualTo(passLong(b, "above"));
    assertThat(passInt(a, "boundaryBucket")).as(label + ": boundaryBucket").isEqualTo(passInt(b, "boundaryBucket"));
  }

  // ---------------- snapshot factories ----------------

  private static Map<String, Long> flatSnapshot(int keys, long count) {
    Map<String, Long> m = new HashMap<>();
    for (int i = 0; i < keys; i++) {
      m.put("k" + i, count);
    }
    return m;
  }

  /** Random snapshot: floor noise, boundary-region values, bucket edges, long tail. */
  private static Map<String, Long> randomSnapshot(Random rng) {
    int keys = 512 + rng.nextInt(3000);
    Map<String, Long> m = new HashMap<>();
    for (int i = 0; i < keys; i++) {
      long v;
      int mode = rng.nextInt(4);
      switch (mode) {
        case 0:
          v = 1 + rng.nextInt(9); // floor noise
          break;
        case 1:
          v = 16 + rng.nextInt(48); // boundary region [16, 64)
          break;
        case 2:
          v = 1L << rng.nextInt(14); // bucket edges (powers of two)
          break;
        default:
          v = 1 + rng.nextInt(2000); // long tail
          break;
      }
      m.put("r" + i, v);
    }
    return m;
  }

  /** The hotLimit-th largest count of the snapshot (independent of the pipeline). */
  private static long trueKth(Map<String, Long> snapshot, int k) {
    long[] values = snapshot.values().stream().filter(v -> v > 0).mapToLong(Long::longValue).sorted().toArray();
    if (values.length == 0) {
      return 0;
    }
    return values[Math.max(0, values.length - k)];
  }
}
