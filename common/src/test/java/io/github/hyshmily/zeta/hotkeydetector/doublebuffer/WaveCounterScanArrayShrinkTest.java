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
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The reused scan arrays' shrink pass ({@code shrinkScanArrays}): a past
 * cardinality spike must not pin the peak footprint forever.  Arrays
 * holding at least 4x the current tide's need shrink to the need (floored
 * at the initial capacity), tides inside the hysteresis band resize
 * nothing, a steady state at or below the initial capacity never resizes,
 * and the pass outputs stay exact across grow/shrink/regrow cycles — the
 * shrink is pure lifecycle, never semantics.
 */
class WaveCounterScanArrayShrinkTest {

  private static final int FLOOR = 10;
  private static final int INITIAL = 1024;

  private WaveCounter counter;

  @BeforeEach
  void setUp() {
    counter = new WaveCounter(ignored -> {});
  }

  /**
   * Spike → quiet → regrow: a 20000-key spike tide grows every array (the
   * packed pair, the candidate and bucket views, and — via the overflow's
   * incumbent split and the shortcut's filter — the kth/incumbent/newcomer
   * views), a 100-key tide shrinks them ALL back to the initial capacity
   * (the floor), and a further spike tide regrows and reproduces the exact
   * boundary.
   */
  @Test
  void spike_thenQuiet_shrinksToFloor_andRegrowsExact() throws Exception {
    Map<String, Long> spike = flatSnapshot(20_000, 25);

    // tide A: the reference path (no seed) — grows the packed pair and the
    // candidate/bucket views; the boundary is exact
    setLastKth(counter, -1);
    Object a = runPass(counter, spike, FLOOR);
    assertThat(passLong(a, "boundary")).isEqualTo(25);
    assertThat(length("allValues")).isEqualTo(32_768); // pow2 growth of 20000
    assertThat(length("allHashes")).isEqualTo(32_768);
    assertThat(length("candidateIdx")).isEqualTo(20_000);
    assertThat(length("bucketIdx")).isEqualTo(20_000);

    // tide B: the shortcut fires and overflows — the kth filter and the
    // incumbent split grow their views to the snapshot size
    setLastKth(counter, 25);
    Object b = runPass(counter, spike, FLOOR);
    assertThat(passBool(b, "overflow")).isTrue();
    assertThat(passLong(b, "boundary")).isEqualTo(25);
    assertThat(length("aboveKthIdx")).isEqualTo(20_000);
    assertThat(length("incumbentIdx")).isEqualTo(20_000);
    assertThat(length("newcomerIdx")).isEqualTo(20_000);

    // tide C: the quiet tail — every array is >= 4x the need, all shrink
    // to the floor, and the pass output stays exact
    Object c = runPass(counter, flatSnapshot(100, 25), FLOOR);
    assertThat(passLong(c, "boundary")).isEqualTo(25);
    assertAllAtInitial();

    // tide D: regrow — the shrink never changed the semantics
    setLastKth(counter, -1);
    Object d = runPass(counter, spike, FLOOR);
    assertThat(passLong(d, "boundary")).isEqualTo(25);
    assertThat(length("allValues")).isEqualTo(32_768);
    assertThat(length("candidateIdx")).isEqualTo(20_000);
  }

  /**
   * Hysteresis: a tide whose need is within the band (the arrays under 4x
   * it) resizes nothing — an oscillating cardinality regrows into the
   * retained slack instead of paying a copy per tide.  8192 is exactly 2x
   * the 4096 need: inside the band from BOTH directions.
   */
  @Test
  void withinBand_noResizePaid() throws Exception {
    setLastKth(counter, -1);
    Object first = runPass(counter, flatSnapshot(8192, 25), FLOOR);
    assertThat(passLong(first, "boundary")).isEqualTo(25);
    assertThat(length("allValues")).isEqualTo(8192);
    assertThat(length("candidateIdx")).isEqualTo(8192);

    // 8192 < 4 x 4096: inside the band — no shrink, no growth
    setLastKth(counter, -1);
    Object second = runPass(counter, flatSnapshot(4096, 25), FLOOR);
    assertThat(passLong(second, "boundary")).isEqualTo(25);
    assertThat(length("allValues")).isEqualTo(8192);
    assertThat(length("allHashes")).isEqualTo(8192);
    assertThat(length("candidateIdx")).isEqualTo(8192);
    assertThat(length("bucketIdx")).isEqualTo(8192);
    assertThat(length("aboveKthIdx")).isEqualTo(INITIAL); // no seed: never touched
    assertThat(length("incumbentIdx")).isEqualTo(8192);
    assertThat(length("newcomerIdx")).isEqualTo(8192);
  }

  /**
   * A workload whose steady state sits below the initial capacity never
   * resizes the arrays at all (the floor's purpose — the pre-change
   * footprint), across repeated tides and both selection paths.
   */
  @Test
  void smallSteadyState_arraysStayAtInitialCapacity() throws Exception {
    setLastKth(counter, -1);
    Object first = runPass(counter, flatSnapshot(500, 25), FLOOR);
    assertThat(passLong(first, "boundary")).isEqualTo(25); // the bucket-fits corner
    assertAllAtInitial();

    // second tide, shortcut-seeded: the filter pre-size check must not
    // grow anything either (500 < 1024)
    setLastKth(counter, 25);
    Object second = runPass(counter, flatSnapshot(500, 25), FLOOR);
    assertThat(passLong(second, "boundary")).isEqualTo(25);
    assertAllAtInitial();
  }

  private void assertAllAtInitial() throws Exception {
    assertThat(length("allValues")).isEqualTo(INITIAL);
    assertThat(length("allHashes")).isEqualTo(INITIAL);
    assertThat(length("candidateIdx")).isEqualTo(INITIAL);
    assertThat(length("bucketIdx")).isEqualTo(INITIAL);
    assertThat(length("aboveKthIdx")).isEqualTo(INITIAL);
    assertThat(length("incumbentIdx")).isEqualTo(INITIAL);
    assertThat(length("newcomerIdx")).isEqualTo(INITIAL);
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

  private static void passIntField(Object pass, String name, int value) throws Exception {
    Field f = passType().getDeclaredField(name);
    f.setAccessible(true);
    f.setInt(pass, value);
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

  /** Mirrors {@code promote()}: sweep (with the shrink pass) -> estimate -> select. */
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

  private static void setLastKth(WaveCounter c, long v) throws Exception {
    Field f = WaveCounter.class.getDeclaredField("lastKth");
    f.setAccessible(true);
    f.setLong(c, v);
  }

  /** The current length of one of the counter's reused scan arrays (int[] or long[]). */
  private int length(String arrayField) throws Exception {
    Field f = WaveCounter.class.getDeclaredField(arrayField);
    f.setAccessible(true);
    return java.lang.reflect.Array.getLength(f.get(counter));
  }

  private static Map<String, Long> flatSnapshot(int keys, long count) {
    Map<String, Long> m = new HashMap<>();
    for (int i = 0; i < keys; i++) {
      m.put("k" + i, count);
    }
    return m;
  }
}
