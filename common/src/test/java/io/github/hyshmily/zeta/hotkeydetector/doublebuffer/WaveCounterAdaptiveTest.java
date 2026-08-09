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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Adaptive controls (WindowClimber borrowings): the TidePacer
 * (fast-attack / slow-release EWMA cadence with a still band, and the
 * empty-tide stretch ladder) and the MoonsTidalForce promotion floor
 * (renewal-signal admit-on-block floor with idle collapse, veto-return
 * and audit release as defense).  The design decisions were validated
 * first on simulated sequences in a temporary probe test; these tests pin
 * the real implementations.
 */
class WaveCounterAdaptiveTest {

  private WaveCounter counter;

  @BeforeEach
  void setUp() {
    counter = new WaveCounter(ignored -> {});
  }

  // ---------------- TidePacer ----------------

  private static Object pacerOf(WaveCounter c) throws Exception {
    Field f = WaveCounter.class.getDeclaredField("pacer");
    f.setAccessible(true);
    return f.get(c);
  }

  private static long pressureOf(Object pacer, WaveCounter c, int raw) throws Exception {
    Method m = pacer.getClass().getDeclaredMethod("pressure", int.class);
    m.setAccessible(true);
    return c.computeNextTideDelayMs((int) m.invoke(pacer, raw));
  }

  private static long emptyDelayOf(Object pacer, long baseMs) throws Exception {
    Method m = pacer.getClass().getDeclaredMethod("emptyDelay", long.class);
    m.setAccessible(true);
    return (long) m.invoke(pacer, baseMs);
  }

  /**
   * A burst folds into the smoothed reference FULLY at once (unchanged
   * burst latency — 50ms on the next tide), quiet tides release at the
   * EMA rate (140ms, not a snap back to 500ms), and in-band jitter
   * (within the 1000-key still band) moves nothing.
   */
  @Test
  void pacer_fastAttack_slowRelease_stillBand() throws Exception {
    Object pacer = pacerOf(counter);
    assertThat(pressureOf(pacer, counter, 20_000)).as("burst attacks instantly").isEqualTo(50);
    long released = pressureOf(pacer, counter, 0);
    assertThat(released).as("release is gradual, not a snap").isBetween(51L, 499L);
    assertThat(pressureOf(pacer, counter, 15_000)).as("in-band jitter below is still").isEqualTo(released);
    assertThat(pressureOf(pacer, counter, 17_000)).as("in-band jitter above is still").isEqualTo(released);
  }

  /**
   * The empty-tide ladder stretches the cadence per consecutive empty
   * tide, caps at 2x the base, and resets on any non-empty tide.
   */
  @Test
  void pacer_emptyLadder_stretchesAndResets() throws Exception {
    Object pacer = pacerOf(counter);
    assertThat(emptyDelayOf(pacer, 500)).isEqualTo(500);
    assertThat(emptyDelayOf(pacer, 500)).isEqualTo(1_000);
    assertThat(emptyDelayOf(pacer, 500)).as("capped at 2x").isEqualTo(1_000);
    pressureOf(pacer, counter, 1); // any non-empty tide is the confirm
    assertThat(emptyDelayOf(pacer, 500)).as("ladder reset").isEqualTo(500);
  }

  // ---------------- MoonsTidalForce (promotion floor governor) ----------------

  private static Object governorOf(WaveCounter c) throws Exception {
    Field f = WaveCounter.class.getDeclaredField("moonsTidalForce");
    f.setAccessible(true);
    return f.get(c);
  }

  private static int floorOf(WaveCounter c) throws Exception {
    Object governor = governorOf(c);
    Method m = governor.getClass().getDeclaredMethod("floor");
    m.setAccessible(true);
    return (int) m.invoke(governor);
  }

  private static void onTide(WaveCounter c, double renewal, int activeSlots, int hotLimit, int blockedKeys, long boundary)
    throws Exception {
    Object governor = governorOf(c);
    Method m = governor.getClass().getDeclaredMethod("onTide", double.class, int.class, int.class, int.class, long.class);
    m.setAccessible(true);
    m.invoke(governor, renewal, activeSlots, hotLimit, blockedKeys, boundary);
  }

  /** A healthy hot set (renewal at/above target, not saturated) never moves the floor. */
  @Test
  void governor_healthy_shouldNotMove() throws Exception {
    for (int i = 0; i < 10; i++) {
      onTide(counter, 0.85, 100, 1024, 0, 4);
    }
    assertThat(floorOf(counter)).isEqualTo(10);
  }

  /**
   * Distress with keys blocked behind the floor ADMITS them: the floor
   * drops to the boundary in one move, so the renewing keys qualify from
   * the next tide on.  A raised floor holds while health recovers.
   */
  @Test
  void governor_distress_admitsBlockedKeys() throws Exception {
    onTide(counter, 0.4, 100, 1024, 5, 26);
    assertThat(floorOf(counter)).isEqualTo(26);
    onTide(counter, 0.3, 100, 1024, 5, 16);
    assertThat(floorOf(counter)).as("boundary collapsed: floor drops to it, admitting the blocked keys").isEqualTo(16);
    onTide(counter, 0.9, 100, 1024, 5, 16);
    assertThat(floorOf(counter)).as("recovery holds the admitted floor (ratchet)").isEqualTo(16);
  }

  /** Distress with NOTHING blocked must hold — the floor is not over-filtering. */
  @Test
  void governor_distress_withoutBlocked_shouldHold() throws Exception {
    for (int i = 0; i < 12; i++) {
      onTide(counter, 0.1, 100, 1024, 0, 4);
    }
    assertThat(floorOf(counter)).as("nothing blocked — floor never moves, no retreat churn").isEqualTo(10);
  }

  /** An unresolved admit retreats to where it started (veto-return). */
  @Test
  void governor_unresolvedDistress_shouldRetreat() throws Exception {
    for (int i = 0; i < 5; i++) {
      onTide(counter, 0.1, 100, 1024, 5, 56);
    }
    assertThat(floorOf(counter)).isEqualTo(56);
    int guard = 0;
    while (floorOf(counter) != 10 && guard++ < 20) {
      onTide(counter, 0.1, 100, 1024, 5, 56);
    }
    assertThat(floorOf(counter)).as("retreat settles back at the probe base").isEqualTo(10);
    for (int i = 0; i < 10; i++) {
      onTide(counter, 0.1, 100, 1024, 5, 56);
    }
    assertThat(floorOf(counter)).as("re-probe churn is bounded by the veto-return cycle").isBetween(10, 56);
  }

  /**
   * Sustained health audits a raised floor back down (ratchet release).
   * The audit fires when health has been still for GOVERNOR_AUDIT_WAIT
   * tides — i.e. on the (WAIT + 1)-th healthy tide — and one step per
   * wait thereafter.
   */
  @Test
  void governor_audit_releasesRatchet() throws Exception {
    onTide(counter, 0.4, 100, 1024, 5, 56);
    onTide(counter, 0.3, 100, 1024, 5, 56);
    onTide(counter, 0.4, 100, 1024, 5, 56);
    assertThat(floorOf(counter)).isEqualTo(56);
    for (int i = 0; i < 9; i++) {
      onTide(counter, 0.85, 100, 1024, 0, 4);
    }
    assertThat(floorOf(counter)).as("audit released one step").isEqualTo(40);
    for (int i = 0; i < 18; i++) {
      onTide(counter, 0.85, 100, 1024, 0, 4);
    }
    assertThat(floorOf(counter)).as("audit released to the base").isEqualTo(10);
  }

  /** Saturation (healthy but full) lowers a raised floor faster than the audit. */
  @Test
  void governor_saturation_lowersRaisedFloor() throws Exception {
    onTide(counter, 0.4, 100, 1024, 5, 56);
    onTide(counter, 0.3, 100, 1024, 5, 56);
    onTide(counter, 0.4, 100, 1024, 5, 56);
    assertThat(floorOf(counter)).isEqualTo(56);
    for (int i = 0; i < 4; i++) {
      onTide(counter, 0.9, 950, 1024, 0, 4);
    }
    assertThat(floorOf(counter)).as("saturated health lowers toward the base").isEqualTo(10);
  }

  /** The floor never leaves [10, 256]. */
  @Test
  void governor_clamps() throws Exception {
    onTide(counter, 0.0, 100, 1024, 5, 300);
    assertThat(floorOf(counter)).as("upper clamp").isEqualTo(256);
    for (int i = 0; i < 200; i++) {
      onTide(counter, 0.0, 100, 1024, 5, 300);
    }
    assertThat(floorOf(counter)).isBetween(10, 256);
    for (int i = 0; i < 200; i++) {
      onTide(counter, 0.9, 1024, 1024, 0, 1);
    }
    assertThat(floorOf(counter)).as("lower clamp after release walk").isEqualTo(10);
  }

  // ---------------- integration: real tides ----------------

  /**
   * End-to-end: a stable hot key keeps the floor at 10; when it drifts
   * for one tide (renewal 0 with 500 noise keys blocked behind the seed
   * floor) the floor must NOT rise — the admit keeps it at the boundary —
   * and the key re-promotes on return.
   */
  @Test
  void integration_drift_floorHolds_topRepromotes() throws Exception {
    markHot(counter, "top");
    counter.count("top", 80);
    for (int i = 0; i < 500; i++) {
      counter.count("k" + i, 5);
    }
    invokeDeliver(counter);
    assertThat(floorOf(counter)).isEqualTo(10);
    assertThat(isBeaconMember(counter, "top")).isTrue();

    for (int i = 0; i < 500; i++) {
      counter.count("k" + i, 5); // top quiet this tide
    }
    invokeDeliver(counter);
    assertThat(floorOf(counter)).as("drift cannot raise the floor").isEqualTo(10);

    for (int t = 0; t < 9; t++) {
      counter.count("top", 80);
      for (int i = 0; i < 500; i++) {
        counter.count("k" + i, 5);
      }
      invokeDeliver(counter);
      assertThat(isBeaconMember(counter, "top")).as("re-promoted after the drift").isTrue();
    }
    assertThat(floorOf(counter)).as("floor never left the base").isEqualTo(10);
  }

  // ---------------- helpers ----------------

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

  private static void markHot(WaveCounter c, String key) throws Exception {
    Field f = WaveCounter.class.getDeclaredField("beacon");
    f.setAccessible(true);
    long[] beacon = (long[]) f.get(c);
    Field m = WaveCounter.class.getDeclaredField("beaconMask");
    m.setAccessible(true);
    int mask = (int) m.get(c);
    int h = mixHash(key.hashCode());
    int bit1 = h & mask;
    beacon[bit1 >>> 4] = beacon[bit1 >>> 4] | (2L << ((bit1 & 15) << 2));
    int bit2 = rehash(h) & mask;
    beacon[bit2 >>> 4] = beacon[bit2 >>> 4] | (2L << (((bit2 & 15) << 2) + 2));
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
