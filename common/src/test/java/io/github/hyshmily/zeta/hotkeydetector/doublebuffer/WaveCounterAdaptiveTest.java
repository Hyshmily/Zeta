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
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Adaptive controls (WindowClimber borrowings): the TidePacer
 * (fast-attack / slow-release EWMA cadence with a still band, and the
 * empty-tide stretch ladder) and the MoonsTidalForce promotion floor
 * (renewal-disambiguated governor: under-earning distress arms bounded
 * raise-walks, healthy tides admit blocked keys by dropping toward the
 * boundary, with anchor-memory crash bars, noise-adaptive veto margins,
 * idle collapse, veto-return and audit release as defense — ADR-0045).
 * The design decisions were validated first on simulated sequences in a
 * desktop sandbox; these tests pin the real implementations.
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

  private static void onTide(
    WaveCounter c,
    double renewal,
    int activeSlots,
    int hotLimit,
    int blockedKeys,
    long boundary,
    double ratio
  ) throws Exception {
    onTide(c, renewal, activeSlots, hotLimit, blockedKeys, boundary, ratio, 2_000L, 100, 500L);
  }

  /** Drives the governor with explicit volume/scale signals (ADR-0045 §III). */
  private static void onTide(
    WaveCounter c,
    double renewal,
    int activeSlots,
    int hotLimit,
    int blockedKeys,
    long boundary,
    double ratio,
    long volumeSeed,
    int distinct,
    long intervalMs
  ) throws Exception {
    Object governor = governorOf(c);
    // Mirror the production call order (promote() folds the volume EWMA
    // BEFORE invoking onTide — the same sample must never be folded twice,
    // which applied weight 2α−α² instead of α and distorted the P1/P2
    // regime gates). Without the explicit fold here, vol stays at its seed
    // and every tide reads as "quiet".  The volume is deliberately NOT part
    // of the TideReading (ADR-0055): the reading carries only the signals
    // onTide consumes; the volume channel is onVolume.
    Method fold = governor.getClass().getDeclaredMethod("onVolume", long.class);
    fold.setAccessible(true);
    fold.invoke(governor, volumeSeed);
    Method m = governor.getClass().getDeclaredMethod("onTide", tideReadingType());
    m.setAccessible(true);
    m.invoke(
      governor,
      newTideReading(renewal, activeSlots, hotLimit, blockedKeys, boundary, ratio, distinct, intervalMs)
    );
  }

  /**
   * The private {@code WaveCounter.TideReading} record type, resolved by
   * name — a private nested class is not nameable from this test, even in
   * the same package.
   */
  private static Class<?> tideReadingType() throws Exception {
    return Class.forName("io.github.hyshmily.zeta.hotkeydetector.doublebuffer.WaveCounter$TideReading");
  }

  /** Reflectively constructs a {@code TideReading} from the raw signals. */
  private static Object newTideReading(
    double renewal,
    int activeSlots,
    int hotLimit,
    int blockedKeys,
    long boundary,
    double ratio
  ) throws Exception {
    // Defaults that keep the P1/P2 regime switches inert for the classic
    // governor tests (the volume seed itself is folded by the onTide
    // overload above — see its Javadoc).
    return newTideReading(renewal, activeSlots, hotLimit, blockedKeys, boundary, ratio, 100, 500L);
  }

  /** Reflectively constructs a {@code TideReading} with explicit scale signals. */
  private static Object newTideReading(
    double renewal,
    int activeSlots,
    int hotLimit,
    int blockedKeys,
    long boundary,
    double ratio,
    int distinct,
    long intervalMs
  ) throws Exception {
    Constructor<?> ctor = tideReadingType()
      .getDeclaredConstructor(
        double.class,
        int.class,
        int.class,
        int.class,
        long.class,
        double.class,
        int.class,
        long.class
      );
    ctor.setAccessible(true);
    return ctor.newInstance(renewal, activeSlots, hotLimit, blockedKeys, boundary, ratio, distinct, intervalMs);
  }

  /** A healthy hot set (renewal at/above target, not saturated) never moves the floor. */
  @Test
  void governor_healthy_shouldNotMove() throws Exception {
    for (int i = 0; i < 10; i++) {
      onTide(counter, 0.85, 100, 1024, 0, 4, 1.5);
    }
    assertThat(floorOf(counter)).isEqualTo(10);
  }

  /** Distress with NOTHING blocked must hold — the floor is not over-filtering. */
  @Test
  void governor_distress_withoutBlocked_shouldHold() throws Exception {
    for (int i = 0; i < 12; i++) {
      onTide(counter, 0.1, 100, 1024, 0, 4, 1.5);
    }
    assertThat(floorOf(counter)).as("nothing blocked — floor never moves, no retreat churn").isEqualTo(10);
  }

  /**
   * Distress with blocked keys but an OVER-EARNING hot set (hotColdRatio
   * >= 1) must hold: the blocked band is the stale tail of the 4-tide
   * membership memory, which self-heals by decay — a drop would re-admit
   * pollution, a raise cannot reach it.
   */
  @Test
  void governor_distress_overEarning_shouldHold() throws Exception {
    for (int i = 0; i < 3; i++) {
      onTide(counter, 0.2, 16, 16, 10, 2, 2.0);
    }
    assertThat(floorOf(counter)).as("over-earning hot set — the stale tail must not move the floor").isEqualTo(10);
  }

  /**
   * ADR-0045 §II probe hygiene: arming a raise-walk freezes the base but does
   * NOT take the first step.  A single noisy distress sample therefore
   * cannot move the floor; the first step happens only on the next
   * distressed tide that still survives the evidence gates.  This removes
   * the premature 10→18 jump and is what keeps the walk from outrunning
   * short/noisy workloads before the verdict has a second sample.
   */
  @Test
  void governor_raiseWalk_armDefersFirstStepUntilSecondDistressedTide() throws Exception {
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // under-earning distress: arm only
    assertThat(floorOf(counter)).as("arming never steps the floor").isEqualTo(10);
    assertThat(walkOf(counter)).as("the walk is in flight").isNotNull();
    assertThat(walkIntField(counter, "samples")).as("the arm tide is not yet a walk sample").isZero();

    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // second distressed tide: first step
    assertThat(floorOf(counter)).as("the second distressed tide takes the first step").isEqualTo(18);
    assertThat(walkIntField(counter, "samples")).isEqualTo(1);
  }

  /**
   * Distress with an UNDER-EARNING hot set (hotColdRatio < 1 — occupied
   * hot slots earn less per slot than cold keys earn per key) arms a
   * bounded raise-walk: the floor steps up, confirms after the set holds
   * the target across the crash persistence, and is later released by the
   * healthy-branch admit-on-block (anti-ratchet).
   */
  @Test
  void governor_distress_underEarning_armsRaiseWalk_admitReleases() throws Exception {
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // arm: the first step is deferred one tide
    assertThat(floorOf(counter)).as("arming alone never moves the floor").isEqualTo(10);
    assertThat(walkOf(counter)).as("a raise-walk is in flight").isNotNull();
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // the next distressed tide takes the first step
    assertThat(floorOf(counter)).isEqualTo(18);
    for (int i = 0; i < 3; i++) {
      onTide(counter, 1.0, 3, 16, 0, 2, 1.5); // at-target tides: durable confirm
    }
    assertThat(floorOf(counter)).isEqualTo(18);
    onTide(counter, 1.0, 3, 16, 36, 2, 1.5); // healthy + blocked below the raised floor
    assertThat(floorOf(counter)).as("admit-on-block releases the raised floor toward the boundary").isEqualTo(10);
  }

  /**
   * A raise-walk that cannot recover the renewal signal crashes: the floor
   * returns in budgeted strides to the base frozen at the arm (the position
   * the experiment left from), and the retry ladder arms a backoff.
   */
  @Test
  void governor_raiseWalk_crashes_undoesToFrozenBase() throws Exception {
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // arm: first step deferred
    assertThat(floorOf(counter)).isEqualTo(10);
    onTide(counter, 0.2, 3, 16, 5, 2, 0.5); // first distressed step after arm
    assertThat(floorOf(counter)).isEqualTo(18);
    onTide(counter, 0.2, 3, 16, 5, 2, 0.5); // second distressed step
    onTide(counter, 0.2, 3, 16, 5, 2, 0.5); // third consecutive: crash
    assertThat(ladderField(counter, "raiseLadder", "left")).as("crash armed the retry backoff").isGreaterThan(0);
    int guard = 0;
    while (floorOf(counter) != 10 && guard++ < 20) {
      onTide(counter, 1.0, 3, 16, 0, 2, 1.5);
    }
    assertThat(floorOf(counter)).as("crash undid the raise to the frozen base").isEqualTo(10);
  }

  /**
   * A raise-walk that confirms and then re-distresses retreats to the
   * veto anchor — the position the confirmed raise left from (veto-return),
   * priced by the anchor memory: the current renewal must be worse than
   * the anchor's reference renewal minus the noise-aware margin.  The
   * anchor is planted at the confirmation, NOT re-frozen at the distress
   * episode start (the former episode-start freeze re-anchored at the
   * raised floor and made the retreat unreachable).
   */
  @Test
  void governor_raiseConfirmedThenRedistress_shouldRetreatToAnchor() throws Exception {
    onTide(counter, 0.25, 16, 16, 10, 2, 0.5); // under-earning distress: arm, first step deferred
    assertThat(floorOf(counter)).isEqualTo(10);
    onTide(counter, 0.25, 16, 16, 10, 2, 0.5); // the next distressed tide steps
    assertThat(floorOf(counter)).isEqualTo(18);
    for (int i = 0; i < 3; i++) {
      onTide(counter, 1.0, 3, 16, 0, 2, 1.5); // at-target: durable confirm, anchor (10, 0.25)
    }
    assertThat(floorOf(counter)).isEqualTo(18);
    // Re-distress worse than the anchor reference minus the margin, with
    // nothing blocked so the raise-walk cannot re-arm and mask the veto.
    // The noise band must wash the confirmation tides out of the deviation
    // EMA before the evidence gap (0.25 - 0.1 = 0.15) admits renewal 0.1.
    // The EMA decay (0.8/tide) converges asymptotically, so the veto needs
    // ~14 distress tides instead of the 8-tide ring wash-out.
    for (int i = 0; i < 16; i++) {
      onTide(counter, 0.1, 100, 1024, 0, 4, 1.5);
    }
    assertThat(governorField(counter, "retreatTarget")).as("veto armed the retreat to the anchor").isEqualTo(10);
    int guard = 0;
    while (floorOf(counter) != 10 && guard++ < 20) {
      onTide(counter, 0.1, 100, 1024, 0, 4, 1.5);
    }
    assertThat(floorOf(counter)).as("retreat settles back at the anchor").isEqualTo(10);
  }

  /**
   * The healthy path admits blocked keys: with a raised floor and keys
   * blocked behind it (the renewing keys the boundary would admit), the
   * floor drops to the boundary in one move so they qualify from the next
   * tide on.  The drop is clamped at the seed.
   */
  @Test
  void governor_healthy_admitsBlockedKeys() throws Exception {
    setGovernorField(counter, "floor", 26);
    onTide(counter, 0.9, 100, 1024, 5, 16, 1.5);
    assertThat(floorOf(counter)).isEqualTo(16);
    onTide(counter, 0.9, 100, 1024, 5, 16, 1.5);
    assertThat(floorOf(counter)).as("admitted floor holds at the boundary").isEqualTo(16);
    onTide(counter, 0.9, 100, 1024, 5, 4, 1.5);
    assertThat(floorOf(counter)).as("boundary below the seed clamps the drop").isEqualTo(10);
  }

  /**
   * Sustained health audits a raised floor back down (ratchet release).
   * The audit fires when health has been still for GOVERNOR_AUDIT_WAIT
   * tides — i.e. on the (WAIT + 1)-th healthy tide — and one step per
   * wait thereafter.
   */
  @Test
  void governor_audit_releasesRatchet() throws Exception {
    setGovernorField(counter, "floor", 56);
    for (int i = 0; i < 9; i++) {
      onTide(counter, 0.85, 100, 1024, 0, 4, 1.5);
    }
    assertThat(floorOf(counter)).isEqualTo(48);
    for (int i = 0; i < 18; i++) {
      onTide(counter, 0.85, 100, 1024, 0, 4, 1.5);
    }
    assertThat(floorOf(counter)).as("audit released to the base").isEqualTo(10);
  }

  /** Saturation (healthy but full) lowers a raised floor faster than the audit. */
  @Test
  void governor_saturation_lowersRaisedFloor() throws Exception {
    setGovernorField(counter, "floor", 56);
    for (int i = 0; i < 8; i++) {
      onTide(counter, 0.9, 950, 1024, 0, 4, 1.5);
    }
    assertThat(floorOf(counter)).as("saturated health lowers toward the base").isEqualTo(10);
  }

  /** The floor never leaves [10, 256]. */
  @Test
  void governor_clamps() throws Exception {
    setGovernorField(counter, "floor", 250);
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // under-earning distress arms the raise-walk
    assertThat(floorOf(counter)).as("arming defers the first step").isEqualTo(250);
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // first distressed step: clamped at the ceiling
    assertThat(floorOf(counter)).as("upper clamp").isEqualTo(256);
    for (int i = 0; i < 200; i++) {
      onTide(counter, 0.2, 3, 16, 5, 2, 0.5); // persistent distress: walk crashes, returns, re-arms
    }
    assertThat(floorOf(counter)).isBetween(10, 256);
    for (int i = 0; i < 200; i++) {
      onTide(counter, 0.9, 1024, 1024, 0, 1, 1.5);
    }
    assertThat(floorOf(counter)).as("lower clamp after release walk").isEqualTo(10);
  }

  // ---------------- release WALK (WindowClimber borrowings) ----------------

  private static int governorField(WaveCounter c, String name) throws Exception {
    Object governor = governorOf(c);
    Field f = governor.getClass().getDeclaredField(name);
    f.setAccessible(true);
    return f.getInt(governor);
  }

  /** The governor's in-flight {@code Walk}, or {@code null} when parked. */
  private static Object walkOf(WaveCounter c) throws Exception {
    Object governor = governorOf(c);
    Field f = governor.getClass().getDeclaredField("walk");
    f.setAccessible(true);
    return f.get(governor);
  }

  /** An int field of the in-flight {@code Walk} (only while a walk is active). */
  private static int walkIntField(WaveCounter c, String name) throws Exception {
    Field f = walkOf(c).getClass().getDeclaredField(name);
    f.setAccessible(true);
    return f.getInt(walkOf(c));
  }

  private static void setGovernorField(WaveCounter c, String name, int value) throws Exception {
    Object governor = governorOf(c);
    Field f = governor.getClass().getDeclaredField(name);
    f.setAccessible(true);
    f.setInt(governor, value);
  }

  /** A double field of the governor (e.g. anchorRenewal). */
  private static void setGovernorDoubleField(WaveCounter c, String name, double value) throws Exception {
    Object governor = governorOf(c);
    Field f = governor.getClass().getDeclaredField(name);
    f.setAccessible(true);
    f.setDouble(governor, value);
  }

  private static int ladderField(WaveCounter c, String ladderName, String name) throws Exception {
    Object governor = governorOf(c);
    Field f = governor.getClass().getDeclaredField(ladderName);
    f.setAccessible(true);
    Object ladder = f.get(governor);
    Field g = ladder.getClass().getDeclaredField(name);
    g.setAccessible(true);
    return g.getInt(ladder);
  }

  private static void setLadderField(WaveCounter c, String ladderName, String name, int value) throws Exception {
    Object governor = governorOf(c);
    Field f = governor.getClass().getDeclaredField(ladderName);
    f.setAccessible(true);
    Object ladder = f.get(governor);
    Field g = ladder.getClass().getDeclaredField(name);
    g.setAccessible(true);
    g.setInt(ladder, value);
  }

  /** A boolean field of one of the governor's ladders. */
  private static boolean ladderBoolField(WaveCounter c, String ladderName, String name) throws Exception {
    Object governor = governorOf(c);
    Field f = governor.getClass().getDeclaredField(ladderName);
    f.setAccessible(true);
    Object ladder = f.get(governor);
    Field g = ladder.getClass().getDeclaredField(name);
    g.setAccessible(true);
    return g.getBoolean(ladder);
  }

  /** A double field of the governor. */
  private static double governorDoubleField(WaveCounter c, String name) throws Exception {
    Object governor = governorOf(c);
    Field f = governor.getClass().getDeclaredField(name);
    f.setAccessible(true);
    return f.getDouble(governor);
  }

  /** The governor's audit due-wait (the R2 escalation law). */
  private static int auditWaitOf(WaveCounter c) throws Exception {
    Object governor = governorOf(c);
    Method m = governor.getClass().getDeclaredMethod("auditWait");
    m.setAccessible(true);
    return (int) m.invoke(governor);
  }

  /**
   * A release WALK that crashes (persistent renewal below the anchor bar —
   * the base renewal frozen at the arm minus the noise-aware margin)
   * undoes itself: the floor returns in budgeted strides to the base
   * frozen at the arm, and the retry ladder arms a backoff.
   */
  @Test
  void governor_walk_crashes_undoesToFrozenBase() throws Exception {
    setGovernorField(counter, "floor", 56);
    for (int i = 0; i < 9; i++) {
      onTide(counter, 0.85, 100, 1024, 0, 4, 1.5); // audit-due tide arms the walk
    }
    assertThat(floorOf(counter)).as("walk armed at the base and took its first step").isEqualTo(48);
    for (int i = 0; i < 3; i++) {
      onTide(counter, 0.1, 100, 1024, 0, 4, 1.5); // persistent below-bar renewal
    }
    assertThat(ladderField(counter, "releaseLadder", "left")).as("crash armed the retry backoff").isGreaterThan(0);
    int guard = 0;
    while (floorOf(counter) != 56 && guard++ < 20) {
      onTide(counter, 0.85, 100, 1024, 0, 4, 1.5);
    }
    assertThat(floorOf(counter)).as("crash undid the walk to the frozen base").isEqualTo(56);
  }

  /**
   * The release walk's crash bar is the ANCHOR memory, not the fixed goal
   * target: a renewal of 0.6 (healthy under the old fixed 0.5 target)
   * reads as a below-bar sample against the base reference 0.85 minus the
   * margin, and three such tides crash the walk — the descended position
   * must not earn measurably less than the position it left.
   */
  @Test
  void governor_releaseWalk_crashBar_anchorMemory() throws Exception {
    setGovernorField(counter, "floor", 56);
    for (int i = 0; i < 9; i++) {
      onTide(counter, 0.85, 100, 1024, 0, 4, 1.5); // audit-due arms the walk (base renewal 0.85)
    }
    assertThat(floorOf(counter)).isEqualTo(48);
    onTide(counter, 0.6, 100, 1024, 0, 4, 1.5); // below the anchor bar (~0.63-0.75): streak 1
    onTide(counter, 0.6, 100, 1024, 0, 4, 1.5); // streak 2 — the walk keeps stepping
    // The stride law prices the descent from the EMA-smoothed renewal
    // against the crash bar: below-bar tides creep instead of plunging,
    // so the descent over the pre-crash window is bounded (the old bold
    // driver reached the seed here).
    assertThat(floorOf(counter)).as("below-bar tides creep, not plunge").isBetween(11, 44);
    onTide(counter, 0.6, 100, 1024, 0, 4, 1.5); // streak 3: crash
    assertThat(ladderField(counter, "releaseLadder", "left"))
      .as("third below-bar tide crashes the walk")
      .isGreaterThan(0);
    int guard = 0;
    while (floorOf(counter) != 56 && guard++ < 20) {
      onTide(counter, 0.85, 100, 1024, 0, 4, 1.5);
    }
    assertThat(floorOf(counter)).as("anchor crash undid to the frozen base").isEqualTo(56);
  }

  /**
   * The release stride law prices the step from the smoothed (ring-mean)
   * renewal against the walk's crash bar: a set comfortably healthier
   * than the bar strides at the ceiling (2x the initial step), so the
   * descent completes within the walk budget instead of crawling.
   */
  @Test
  void governor_releaseWalk_stride_bolderWhenHealthier() throws Exception {
    setGovernorField(counter, "floor", 250);
    for (int i = 0; i < 9; i++) {
      onTide(counter, 0.85, 100, 1024, 0, 4, 1.5); // audit-due arms the walk (arm stride 8)
    }
    assertThat(floorOf(counter)).isEqualTo(242);
    for (int i = 0; i < 9; i++) {
      onTide(counter, 1.0, 100, 1024, 0, 4, 1.5); // fully healthy: ceiling strides
    }
    assertThat(floorOf(counter)).as("full health descends toward the base").isBetween(10, 242);
  }

  /**
   * The stride law self-converges in the decision zone: renewal pinned at
   * the crash bar shrinks the stride toward 1, so the walk confirms at a
   * position above the seed (the old bold driver plunged to the seed
   * within the budget).  The verdict samples the boundary at fine
   * granularity instead of committing to a full descent.
   */
  @Test
  void governor_releaseWalk_stride_convergesInDecisionZone() throws Exception {
    setGovernorField(counter, "floor", 100);
    for (int i = 0; i < 9; i++) {
      onTide(counter, 0.85, 100, 1024, 0, 4, 1.5); // audit-due arms the walk (arm stride 16)
    }
    assertThat(floorOf(counter)).isEqualTo(92);
    for (int i = 0; i < 16; i++) {
      onTide(counter, 0.75, 100, 1024, 0, 4, 1.5); // pinned at the crash bar
    }
    assertThat(floorOf(counter))
      .as("at-bar renewal converges above the seed; the walk confirms in the decision zone")
      .isGreaterThan(10);
    assertThat(floorOf(counter)).as("the walk still descended").isLessThan(92);
  }

  /**
   * A release WALK that stays healthy through the whole budget confirms:
   * the descended position is kept — no undo, and the ladder is rewarded
   * (the next release arms freely).
   */
  @Test
  void governor_walk_confirms_keepsDescendedPosition() throws Exception {
    setGovernorField(counter, "floor", 56);
    for (int i = 0; i < 20; i++) {
      onTide(counter, 0.9, 950, 1024, 0, 4, 1.5);
    }
    assertThat(floorOf(counter)).as("confirmed walk keeps the released floor").isEqualTo(10);
    for (int i = 0; i < 10; i++) {
      onTide(counter, 0.9, 950, 1024, 0, 4, 1.5);
    }
    assertThat(floorOf(counter)).as("no undo after confirmation").isEqualTo(10);
  }

  /**
   * Movement DECAYS the audit run instead of zeroing it (Caffeine's
   * AuditClock.tick): one floor move per wait can no longer suppress
   * audits forever.
   */
  @Test
  void governor_auditTides_decaysOnMovement() throws Exception {
    for (int i = 0; i < 5; i++) {
      onTide(counter, 0.85, 100, 1024, 0, 4, 1.5);
    }
    assertThat(governorField(counter, "auditTides")).isEqualTo(5);
    onTide(counter, 0.1, 100, 1024, 0, 4, 1.5);
    assertThat(governorField(counter, "auditTides")).as("decayed, not reset").isEqualTo(4);
  }

  /**
   * Refractory: while the post-crash backoff is unpaid, a saturated tide
   * HOLDS the position instead of arming a fresh release in the
   * just-failed direction.
   */
  @Test
  void governor_walk_backoff_holdsPosition() throws Exception {
    setGovernorField(counter, "floor", 56);
    setLadderField(counter, "releaseLadder", "left", 3);
    onTide(counter, 0.9, 950, 1024, 0, 4, 1.5);
    assertThat(floorOf(counter)).as("backoff holds the position").isEqualTo(56);
    onTide(counter, 0.9, 950, 1024, 0, 4, 1.5);
    assertThat(floorOf(counter)).as("backoff still holds").isEqualTo(56);
    onTide(counter, 0.9, 950, 1024, 0, 4, 1.5);
    assertThat(floorOf(counter)).as("backoff expired: a fresh walk arms").isEqualTo(48);
  }

  /**
   * The retry ledgers are per-direction (Caffeine's Ladder): a crashed
   * raise's backoff must NOT delay the corrective release — the release
   * is exactly the anti-ratchet channel a failed raise needs most.
   */
  @Test
  void governor_raiseBackoff_doesNotBlockRelease() throws Exception {
    setGovernorField(counter, "floor", 56);
    setLadderField(counter, "raiseLadder", "left", 3); // raise backoff unpaid
    onTide(counter, 0.9, 950, 1024, 0, 4, 1.5); // healthy saturated: release arms
    assertThat(floorOf(counter)).as("release walks despite the raise ladder's backoff").isEqualTo(48);
  }

  /** ... and a crashed release's backoff must not block the re-probe. */
  @Test
  void governor_releaseBackoff_doesNotBlockRaise() throws Exception {
    setGovernorField(counter, "floor", 26);
    setLadderField(counter, "releaseLadder", "left", 3); // release backoff unpaid
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // under-earning distress: raise arms
    assertThat(floorOf(counter)).as("raise arms despite the release ladder's backoff").isEqualTo(26);
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // the next distressed tide takes the first step
    assertThat(floorOf(counter)).as("raise arms despite the release ladder's backoff").isEqualTo(34);
  }

  /**
   * The ladder's crash pricing (Caffeine's PROBE_CRASH_ESCALATION): a
   * single crashed walk holds the rung — the re-probe waits the initial
   * backoff, not a double — and only a consecutive crash run doubles it.
   */
  @Test
  void governor_ladder_singleCrashHoldsRung_consecutiveDoubles() throws Exception {
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // arm raise #1 (deferred step), then 3 distressed tides: crash #1
    for (int i = 0; i < 3; i++) {
      onTide(counter, 0.2, 3, 16, 5, 2, 0.5); // three consecutive below-target: crash #1
    }
    assertThat(ladderField(counter, "raiseLadder", "left")).as("first crash waits the initial backoff").isEqualTo(4);
    assertThat(ladderField(counter, "raiseLadder", "rung")).as("a single crash holds the rung").isEqualTo(1);
    int guard = 0;
    while (ladderField(counter, "raiseLadder", "rung") == 1 && guard++ < 30) {
      onTide(counter, 0.2, 3, 16, 5, 2, 0.5); // return, backoff, re-arm, crash #2
    }
    assertThat(ladderField(counter, "raiseLadder", "rung")).as("a consecutive crash run doubles").isEqualTo(4);
    assertThat(ladderField(counter, "raiseLadder", "left")).isEqualTo(4);
    guard = 0;
    while (ladderField(counter, "raiseLadder", "rung") == 4 && guard++ < 30) {
      onTide(counter, 0.2, 3, 16, 5, 2, 0.5); // return, backoff, re-arm, crash #3
    }
    assertThat(ladderField(counter, "raiseLadder", "rung")).as("the run keeps doubling").isEqualTo(8);
  }

  /**
   * A raise walk that oscillates without a verdict spends its budget:
   * priced as a completed, failed experiment — the rung doubles and the
   * crash run resets — unlike a crash, which holds the rung until
   * crashes run (Caffeine's FAILED ending).
   */
  @Test
  void governor_raiseWalk_budgetSpent_pricesAsFailedExperiment() throws Exception {
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // arm: first step deferred
    assertThat(floorOf(counter)).isEqualTo(10);
    // Alternate healthy and below-target: neither streak reaches persistence.
    for (int i = 0; i < 16; i++) {
      onTide(counter, (i % 2 == 0) ? 1.0 : 0.2, 3, 16, 5, 2, 0.5);
    }
    assertThat(ladderField(counter, "raiseLadder", "rung"))
      .as("budget-spent prices as a failed experiment")
      .isEqualTo(4);
    assertThat(ladderField(counter, "raiseLadder", "crashStreak")).as("failure resets the crash run").isZero();
    assertThat(governorField(counter, "retreatTarget")).as("the budgeted return targets the frozen base").isEqualTo(10);
  }

  /**
   * A 1s workload shift at the base cadence is a 2-tide square wave (two
   * 500ms tides per period — 20 tides per period at the 50ms burst
   * cadence).  The renewal signal alternates distressed/healthy every
   * tide, so the raise-walk can never collect the 3-consecutive
   * confirmation window (both streaks cap at 1): the governor probes
   * (the walk arms and climbs), prices the budget-spent walk as FAILED,
   * undoes it to the frozen base, and never plants the veto anchor —
   * the floor does not ratchet on a workload it cannot outrun.
   */
  @Test
  void governor_secondSquareWave_oscillation_cannotConfirm_noRatchet() throws Exception {
    onTide(counter, 0.1, 3, 16, 10, 2, 0.5); // tide 1 (distress): arm, first step deferred
    assertThat(floorOf(counter)).as("the probe responds to the evidence").isEqualTo(10);
    assertThat(walkOf(counter)).as("the walk is armed").isNotNull();
    // Tides 2-17: strict 1:1 alternation (distress on odd tides).  At
    // tide 17 (distressed) the walk has spent its 16-tide budget without
    // a verdict -> FAILED, the rung doubles, and the budgeted return
    // to the frozen base begins.
    for (int i = 2; i <= 17; i++) {
      boolean distressed = (i % 2 == 1);
      onTide(counter, distressed ? 0.1 : 1.0, 3, 16, distressed ? 10 : 0, 2, distressed ? 0.5 : 1.5);
    }
    assertThat(governorField(counter, "anchorFloor")).as("oscillation can never confirm a raise").isZero();
    assertThat(ladderField(counter, "raiseLadder", "rung"))
      .as("priced as a failed experiment, not a confirmation")
      .isEqualTo(4);
    int guard = 0;
    while (floorOf(counter) != 10 && guard++ < 12) {
      onTide(counter, 1.0, 3, 16, 0, 2, 1.5); // healthy: the failed walk returns in budgeted strides
    }
    assertThat(floorOf(counter)).as("the failed walk undid itself — no ratchet").isEqualTo(10);
    assertThat(governorField(counter, "anchorFloor")).as("still no confirmation on the way down").isZero();
    for (int i = 0; i < 10; i++) {
      onTide(counter, 1.0, 3, 16, 0, 2, 1.5);
    }
    assertThat(floorOf(counter)).as("sustained health holds the base").isEqualTo(10);
  }

  /**
   * The 3-consecutive window is both necessary and sufficient: two
   * consecutive at-target tides (the most a 2-tide square wave can ever
   * produce) leave the walk in flight, a single distressed tide breaks
   * the streak, and only the third consecutive at-target tide confirms —
   * planting the veto anchor at the base the raise left from and
   * rewarding the ladder.  The floor the walk stepped to is kept.
   */
  @Test
  void governor_secondSquareWave_threeConsecutiveAtTarget_confirms() throws Exception {
    onTide(counter, 0.1, 3, 16, 10, 2, 0.5); // tide 1 (distress): arm, first step deferred
    assertThat(floorOf(counter)).isEqualTo(10);
    onTide(counter, 0.1, 3, 16, 10, 2, 0.5); // first distressed step after arm
    assertThat(floorOf(counter)).isEqualTo(18);
    onTide(counter, 1.0, 3, 16, 0, 2, 1.5); // at-target 1
    onTide(counter, 1.0, 3, 16, 0, 2, 1.5); // at-target 2
    assertThat(walkOf(counter)).as("two consecutive at-target tides are not enough").isNotNull();
    assertThat(walkIntField(counter, "healthyStreak")).isEqualTo(2);
    assertThat(floorOf(counter)).as("at-target tides hold, they do not step").isEqualTo(18);
    onTide(counter, 0.2, 3, 16, 5, 2, 0.5); // one distressed tide breaks the streak
    assertThat(walkIntField(counter, "healthyStreak")).as("a single distressed tide breaks the window").isZero();
    onTide(counter, 1.0, 3, 16, 0, 2, 1.5); // at-target 1 (fresh window)
    onTide(counter, 1.0, 3, 16, 0, 2, 1.5); // at-target 2
    assertThat(walkOf(counter)).as("two consecutive at-target tides still not enough").isNotNull();
    onTide(counter, 1.0, 3, 16, 0, 2, 1.5); // at-target 3: the durable window confirms
    assertThat(walkOf(counter)).as("three consecutive at-target tides confirm the raise").isNull();
    assertThat(floorOf(counter)).as("the confirmed position is kept").isEqualTo(26);
    assertThat(governorField(counter, "anchorFloor")).as("anchor planted at the base the raise left from").isEqualTo(10);
    assertThat(ladderField(counter, "raiseLadder", "rung")).as("confirmation rewards the ladder").isEqualTo(1);
  }

  /**
   * The empty-hot-set collapse cancels any in-flight walk: without it the
   * stale walk would crash (renewal is 0 with an empty set) and its undo
   * would drag the floor back up toward the frozen base, defeating the
   * collapse — the same applies to a budgeted return (oscillating around
   * the seed, then snapping to the stale target).
   */
  @Test
  void governor_emptySet_collapseCancelsInFlightWalk() throws Exception {
    setGovernorField(counter, "floor", 56);
    for (int i = 0; i < 9; i++) {
      onTide(counter, 0.85, 100, 1024, 0, 4, 1.5); // audit-due tide arms the walk
    }
    assertThat(floorOf(counter)).isEqualTo(48);
    onTide(counter, 0.1, 0, 1024, 0, 4, 1.5); // empty hot set: collapse
    assertThat(floorOf(counter)).as("collapsed to the seed").isEqualTo(10);
    for (int i = 0; i < 6; i++) {
      onTide(counter, 0.1, 0, 1024, 0, 4, 1.5); // would crash the stale walk → re-raise
    }
    assertThat(floorOf(counter)).as("no stale-walk undo re-raises the floor").isEqualTo(10);
  }

  // ---------------- ADR-0051: probe-machine bounds ----------------

  /**
   * ADR-0051 confirm-admit: a raise-walk CONFIRM that lands with a
   * non-empty blocked band (floor above the boundary while the set is
   * healthy) is over-filtering — the boundary admits the band, the floor
   * excludes it.  The parked branch would admit next tide; the
   * confirmation corrects to the boundary immediately, and the veto
   * anchor plants at the corrected position (a veto back below it changes
   * nothing — the threshold is max(floor, boundary) — and costs flicker).
   * The walk still gets its durable 3-tide confirmation.
   */
  @Test
  void governor_raiseConfirm_withBlockedBand_admitsAtConfirmation() throws Exception {
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // under-earning distress: arm, first step deferred
    assertThat(floorOf(counter)).isEqualTo(10);
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // the next distressed tide steps
    assertThat(floorOf(counter)).isEqualTo(18);
    // At-target tides with a non-empty band: the raise would over-filter.
    for (int i = 0; i < 2; i++) {
      onTide(counter, 1.0, 3, 16, 4, 16, 1.5); // healthy + blocked, boundary 16
      assertThat(walkOf(counter)).as("the durable window still needs 3 tides").isNotNull();
    }
    onTide(counter, 1.0, 3, 16, 4, 16, 1.5); // third consecutive at-target: CONFIRMED
    assertThat(walkOf(counter)).as("the raise confirmed").isNull();
    assertThat(floorOf(counter))
      .as("the confirmation admits the blocked band to the boundary in the same tide")
      .isEqualTo(16);
    assertThat(governorField(counter, "anchorFloor"))
      .as("the anchor plants at the corrected position")
      .isEqualTo(16);
  }

  /**
   * ADR-0051 confirm shield: a CONFIRMED raise leaves the RAISE ladder
   * refractory for CONFIRM_SHIELD tides (rung untouched) — the machine
   * cannot re-arm into an immediate arm-confirm cycle, the
   * alternating-workload ratchet that climbed the floor step by step
   * until the empty-set collapse undid the probe.  The release direction
   * is unaffected (its ladder is separate).
   */
  @Test
  void governor_raiseConfirm_armsConfirmShield() throws Exception {
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // arm, first step deferred
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // step
    assertThat(floorOf(counter)).isEqualTo(18);
    for (int i = 0; i < 3; i++) {
      onTide(counter, 1.0, 3, 16, 0, 2, 1.5); // durable confirm
    }
    assertThat(walkOf(counter)).isNull();
    assertThat(ladderField(counter, "raiseLadder", "left"))
      .as("the confirm leaves the raise ladder refractory")
      .isGreaterThan(0);
    assertThat(ladderField(counter, "raiseLadder", "rung"))
      .as("the shield is not an escalation — the rung stays rewarded")
      .isEqualTo(1);
    // Immediate re-distress with blocked keys must NOT re-arm while the
    // shield holds (the stale tail self-heals by decay instead).
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5);
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5);
    assertThat(walkOf(counter)).as("the shield suppresses the immediate re-arm").isNull();
    assertThat(floorOf(counter)).as("the floor holds at the confirmed position").isEqualTo(18);
  }

  /**
   * ADR-0051 hard budget: the raise walk's TIDAL_WALK_BUDGET binds on BOTH
   * verdict branches.  The legacy check lived only on the below-target
   * branch, so an alternating walk (at/below cycles that never reach 3
   * consecutive of either) outlived the documented budget by up to ~1.5x
   * — holding the floor hostage in alternating regimes.  An at-target
   * tide at the budget now ends the walk as FAILED (undo + backoff).
   */
  @Test
  void governor_raiseWalk_atTargetAtBudget_fails() throws Exception {
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // arm, first step deferred
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // step: floor 18, samples 1
    // Strict 1:1 alternation — at-target runs of 1 never confirm, below
    // runs of 1 never crash; the budget is the only possible ending.  The
    // below-target tides step (the gates are open), so the walk is
    // climbing; samples at tide N = N-1.
    for (int i = 0; i < 14; i++) {
      boolean atTarget = (i % 2 == 0);
      onTide(counter, atTarget ? 1.0 : 0.2, 3, 16, atTarget ? 0 : 5, 2, atTarget ? 1.5 : 0.5);
      assertThat(walkOf(counter)).as("tide %d: no verdict below the budget", 3 + i).isNotNull();
    }
    // The loop ended at samples 15 (tide 17).  The next tide (18) is an
    // AT-TARGET tide at samples 16: the hard budget ends the walk there —
    // the legacy at-target branch never checked the budget and the walk
    // survived to sample 17+.
    onTide(counter, 1.0, 3, 16, 0, 2, 1.5); // at-target at samples 16: hard budget FAILED
    assertThat(walkOf(counter)).as("the hard budget ends the walk on an at-target tide").isNull();
    assertThat(ladderField(counter, "raiseLadder", "left"))
      .as("FAILED priced the backoff")
      .isGreaterThan(0);
    int guard = 0;
    while (floorOf(counter) != 10 && guard++ < 20) {
      onTide(counter, 1.0, 3, 16, 0, 2, 1.5);
    }
    assertThat(floorOf(counter)).as("the failed walk undid to the frozen base").isEqualTo(10);
  }

  /**
   * The empty-hot-set collapse is a full regime change: the distress/veto
   * history, the audit run, the anchor and the step are reset too — a
   * stale distressTides with a stale anchor would otherwise make the first
   * post-recovery veto retreat to the OLD regime's anchor instead of the
   * fresh one.  The new regime's distress then arms a clean raise-walk
   * from the seed.
   */
  @Test
  void governor_emptySet_regimeChangeResetsFullState() throws Exception {
    setGovernorField(counter, "floor", 56);
    onTide(counter, 0.1, 100, 1024, 0, 4, 1.5); // distress 1: distressTides 1
    for (int i = 0; i < 5; i++) {
      onTide(counter, 0.85, 100, 1024, 0, 4, 1.5); // healthy: auditTides 5
    }
    assertThat(governorField(counter, "auditTides")).isEqualTo(5);
    onTide(counter, 0.1, 100, 1024, 0, 4, 1.5); // distress again: distressTides 1
    assertThat(governorField(counter, "distressTides")).isEqualTo(1);
    onTide(counter, 0.1, 0, 1024, 0, 4, 1.5); // empty hot set: collapse
    assertThat(floorOf(counter)).isEqualTo(10);
    assertThat(governorField(counter, "distressTides")).as("distress history reset").isZero();
    assertThat(governorField(counter, "auditTides")).as("audit run reset").isZero();
    assertThat(governorField(counter, "anchorFloor")).as("anchor reset").isZero();
    // New regime: under-earning distress with blocked keys arms a clean
    // raise-walk from the seed — never the stale regime's base.
    onTide(counter, 0.1, 100, 1024, 5, 2, 0.5);
    assertThat(floorOf(counter)).as("fresh regime: raise-walk arms from the seed without stepping").isEqualTo(10);
    assertThat(walkOf(counter)).as("a fresh raise-walk is in flight").isNotNull();
  }

  // ---------------- ADR-0045 §II: oscillation probe hygiene ----------------

  /**
   * ADR-0045 §II: the raise-walk's bold driver steps only while SOME member
   * still earns the threshold (0 &lt; renewal &lt; target) — a renewal of 0
   * means the set is quiet or dead, and climbing the floor cannot help it.
   * The un-gated step outran the earners on oscillating workloads (every
   * distressed tide stepped, the floor passed the hot set's earnings, the
   * evidence decayed and the empty-set collapse had to undo the probe);
   * with the gate the walk holds position and ends in a priced crash
   * verdict instead of a ladder-resetting collapse.
   */
  @Test
  void governor_raiseWalk_renewalZero_doesNotStep() throws Exception {
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // arm: first step deferred
    assertThat(floorOf(counter)).isEqualTo(10);
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // control: a live under-earner steps
    assertThat(floorOf(counter)).isEqualTo(18);
    onTide(counter, 1.0, 16, 16, 10, 2, 1.5); // at-target resets the crash streak and holds
    onTide(counter, 0.0, 16, 16, 10, 2, 0.5); // quiet set: renewal 0 -> held
    assertThat(floorOf(counter)).as("a renewal of 0 never steps the floor").isEqualTo(18);
    onTide(counter, 0.0, 16, 16, 10, 2, 0.5); // second consecutive below-target
    onTide(counter, 0.0, 16, 16, 10, 2, 0.5); // third consecutive distressed: crash
    assertThat(ladderField(counter, "raiseLadder", "left")).as("crash priced the backoff").isGreaterThan(0);
    int guard = 0;
    while (floorOf(counter) != 10 && guard++ < 20) {
      onTide(counter, 1.0, 3, 16, 0, 2, 1.5);
    }
    assertThat(floorOf(counter)).as("the crash undid the raise to the frozen base").isEqualTo(10);
  }

  /**
   * ADR-0045 §II: an empty-set collapse that kills an in-flight walk prices the
   * walk's OWN ladder (the backoff survives the reset) so the oscillation
   * probe loop — ARM → climb → COLLAPSE → ladder reset → ARM, repeating
   * forever with the throttle defeated — is throttled; ANY priced ladder
   * state survives the reset (a crash/fail price from a walk that already
   * ended, e.g. during the post-verdict retreat, is preserved too); only a
   * collapse with NO walk AND no priced ladder (a genuine regime change)
   * gets the full ladder reset.
   */
  @Test
  void governor_emptySet_collapse_pricesInFlightWalk() throws Exception {
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // arm: first step deferred
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // take the first step so the walk is above the seed
    assertThat(floorOf(counter)).isEqualTo(18);
    onTide(counter, 0.1, 0, 1024, 0, 4, 1.5); // empty hot set: collapse kills the walk
    assertThat(floorOf(counter)).isEqualTo(10);
    assertThat(ladderField(counter, "raiseLadder", "left"))
      .as("the walk-inflicted collapse priced the backoff")
      .isGreaterThan(0);
    // the unpaid price holds the re-probe: under-earning distress with
    // blocked keys cannot re-arm while the backoff is in force
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5);
    assertThat(floorOf(counter)).as("backoff holds the position").isEqualTo(10);
    // a priced ladder with NO walk in flight (a crash price landing before
    // the collapse, e.g. during the post-verdict retreat) is preserved too —
    // the collapse tide ticks the ladder first (left 4 -> 3), then preserves
    setGovernorField(counter, "floor", 56);
    setLadderField(counter, "raiseLadder", "left", 4);
    onTide(counter, 0.1, 0, 1024, 0, 4, 1.5);
    assertThat(ladderField(counter, "raiseLadder", "left"))
      .as("a priced ladder survives the collapse (post-tick)")
      .isEqualTo(3);
    // and an unpriced no-walk collapse still resets the ladder fully
    setLadderField(counter, "raiseLadder", "left", 0);
    onTide(counter, 0.1, 0, 1024, 0, 4, 1.5);
    assertThat(ladderField(counter, "raiseLadder", "left"))
      .as("regime-change collapse with a clean ladder resets")
      .isZero();
  }

  /**
   * ADR-0045 §II: the raise-walk's bold driver steps only while the hot slots
   * UNDER-EARN the cold reservoir (hotColdRatio &lt; 1) — a ratio at or above
   * 1 means the members are genuinely earning, and a step would push the
   * threshold past the marginal earners and eat the walk's own confirmation
   * (the self-eating step, e.g. a 0.43 renewal tide stepping 41→56 and
   * excluding the 55-56-count earners).
   */
  @Test
  void governor_raiseWalk_ratioGate_holdsWhenSetEarns() throws Exception {
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // arm: first step deferred
    assertThat(floorOf(counter)).isEqualTo(10);
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // the next distressed tide steps
    assertThat(floorOf(counter)).isEqualTo(18);
    onTide(counter, 0.4, 16, 16, 10, 2, 1.5); // distressed but OVER-earning: held
    assertThat(floorOf(counter)).as("ratio >= 1 never steps the floor").isEqualTo(18);
    onTide(counter, 0.8, 16, 16, 10, 2, 1.5); // at-target: hold (streak resets the crash count)
    onTide(counter, 0.4, 16, 16, 10, 2, 0.5); // under-earning again: steps
    assertThat(floorOf(counter)).isEqualTo(26);
    onTide(counter, 0.8, 16, 16, 10, 2, 1.5); // at-target: hold
    onTide(counter, 0.4, 16, 16, 10, 2, 1.5); // distressed but OVER-earning: held again
    assertThat(floorOf(counter)).as("ratio >= 1 holds at the raised position").isEqualTo(26);
    onTide(counter, 0.4, 16, 16, 10, 2, 0.5); // under-earning: steps
    assertThat(floorOf(counter)).isEqualTo(34);
  }

  /**
   * ADR-0053: the raise stride is priced by the hot/cold density ratio
   * (WindowClimber's {@code DensityClimber.steer} analog): a ratio near
   * the 1.0 step gate creeps by one count (the walk samples the decision
   * zone finely), a ratio of 0.5 strides the full initial step, and a
   * deeper shortfall saturates at {@code STEP_INITIAL}.  The stride is a
   * pure function of the tide's ratio — no step-state decay, so a long
   * healthy pause between walks never re-seeds it.
   */
  @Test
  void governor_raiseWalk_densityPricedStride() throws Exception {
    onTide(counter, 0.2, 16, 16, 10, 2, 0.9); // arm: first step deferred
    assertThat(floorOf(counter)).isEqualTo(10);
    onTide(counter, 0.2, 16, 16, 10, 2, 0.9); // -log2(0.9) ~ 0.15 -> stride 1
    assertThat(floorOf(counter)).as("a ratio near the gate creeps by one").isEqualTo(11);
    onTide(counter, 1.0, 16, 16, 10, 2, 1.5); // at-target: resets the crash streak, holds
    assertThat(floorOf(counter)).isEqualTo(11);
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // -log2(0.5) = 1 -> full initial step
    assertThat(floorOf(counter)).as("a ratio of 0.5 strides the full initial step").isEqualTo(19);
    onTide(counter, 1.0, 16, 16, 10, 2, 1.5); // at-target: holds
    onTide(counter, 0.2, 16, 16, 10, 2, 0.25); // -log2(0.25) = 2 -> saturated at the ceiling
    assertThat(floorOf(counter)).as("a deep shortfall saturates at STEP_INITIAL").isEqualTo(27);
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

  // ---------------- P1/P2: volume-gated regime switches (ADR-0045 §III) ----------------

  /**
   * P1 quiet bypass: a low-volume regime with an empty hot set drops the
   * floor to 1 after the wall-clock confirm (4 x 500ms tides), so any key
   * routes hot; the seed floor re-engages once the volume crosses the
   * hysteresis threshold.
   */
  @Test
  void governor_quietBypass_dropsFloorToOneAndReengages() throws Exception {
    // quiet: 30 counts/tide over 20 distinct keys, empty hot set
    for (int i = 0; i < 4; i++) {
      onTide(counter, 0.0, 0, 1024, 0, 1, Double.MAX_VALUE, 30L, 20, 500L);
    }
    assertThat(floorOf(counter)).as("4 x 500ms of quiet -> bypass floor").isEqualTo(1);
    // sustained quiet keeps the bypass (no churn back and forth)
    onTide(counter, 0.0, 0, 1024, 0, 1, Double.MAX_VALUE, 30L, 20, 500L);
    assertThat(floorOf(counter)).as("bypass is sticky while quiet").isEqualTo(1);
    // burst: the volume crosses the re-engage threshold -> seed floor
    onTide(counter, 1.0, 300, 1024, 0, 1, 2.0, 10_000L, 300, 500L);
    assertThat(floorOf(counter)).as("volume up -> seed floor re-engages").isEqualTo(10);
  }

  /**
   * P2 flood collapse: a RAISED floor on which NO key earns the threshold
   * at high volume is stale from another regime — one FLOOD tide collapses
   * it to the seed (instead of the parked raise-walk plus the slow
   * decay-driven collapse), and the flood lock suppresses the ARM while the
   * flood window persists (a stray earner must not re-raise the floor into
   * an instant collapse).
   */
  @Test
  void governor_floodCollapse_collapsesStaleFloorAndLocksArm() throws Exception {
    // raise the floor first: distress (renewal 0.2, ratio < 1, blocked)
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5, 2_000L, 100, 500L); // arm (no step yet)
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5, 2_000L, 100, 500L); // first distressed step: 10 -> 18
    assertThat(floorOf(counter)).as("raise-walk stepped up").isEqualTo(18);
    // flood: high volume, nothing earns the threshold, floor above the boundary
    onTide(counter, 0.0, 10, 1024, 40, 4, 0.0, 10_000L, 300, 500L);
    assertThat(floorOf(counter)).as("flood collapses the stale floor in one tide").isEqualTo(10);
    // flood lock: a stray earner must NOT re-arm while the volume is at
    // flood rate (both the ladder backoff and the lock hold the floor)
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5, 10_000L, 300, 500L);
    assertThat(floorOf(counter)).as("flood lock suppresses the re-arm").isEqualTo(10);
    // the volume drops below the flood rate -> the lock clears and the
    // machinery resumes (the volume EWMA needs a few low tides)
    for (int i = 0; i < 4; i++) {
      onTide(counter, 0.2, 16, 16, 10, 2, 0.5, 1_000L, 100, 500L);
    }
    assertThat(floorOf(counter)).as("machinery resumes after the flood window").isEqualTo(18);
  }

  /**
   * P2 negative: a high-volume quiet window under the SEED floor is the
   * noise filter's designed job — nothing earns, the floor stays at the
   * seed, no FLOOD (the collapse gate requires floor > seed) and no ARM
   * (the flood signature suppresses the pointless raise-walk).
   */
  @Test
  void governor_seedFloor_highVolumeQuietWindowDoesNotFlood() throws Exception {
    for (int i = 0; i < 5; i++) {
      onTide(counter, 0.0, 0, 1024, 40, 4, 0.0, 10_000L, 300, 500L);
    }
    assertThat(floorOf(counter))
      .as("seed floor under a high-volume quiet window is designed behavior")
      .isEqualTo(10);
  }

  /**
   * R1 (anchor band, WindowClimber's stableBand): the veto requires the
   * floor to stand MEASURABLY above the last confirmed anchor — a floor
   * within {@code ANCHOR_BAND} (5 counts) of it never retreats (the move
   * would change nothing and costs flicker), while a floor beyond the
   * band retreats once the noise margin admits the evidence.
   */
  @Test
  void governor_veto_requiresMeasurableDistanceFromAnchor() throws Exception {
    onTide(counter, 0.25, 16, 16, 10, 2, 0.5); // under-earning distress: arm, first step deferred
    onTide(counter, 0.25, 16, 16, 10, 2, 0.5); // the next distressed tide steps: floor 18
    for (int i = 0; i < 3; i++) {
      onTide(counter, 1.0, 3, 16, 0, 2, 1.5); // durable confirm: anchor (10, 0.25)
    }
    assertThat(floorOf(counter)).isEqualTo(18);
    setGovernorField(counter, "floor", 13); // within the band of the anchor
    onTide(counter, 0.1, 100, 1024, 0, 4, 1.5); // the first distress tide is a shift: it discards the in-band anchor (R3)
    setGovernorField(counter, "anchorFloor", 10); // re-plant the anchor for the band test
    setGovernorDoubleField(counter, "anchorRenewal", 0.25);
    for (int i = 0; i < 19; i++) {
      onTide(counter, 0.1, 100, 1024, 0, 4, 1.5); // persistent distress below the reference
    }
    assertThat(governorField(counter, "retreatTarget"))
      .as("a floor within the anchor band never retreats into itself")
      .isZero();
    assertThat(floorOf(counter)).as("the in-band floor is untouched").isEqualTo(13);
    setGovernorField(counter, "floor", 18); // measurably above the anchor
    for (int i = 0; i < 3; i++) {
      onTide(counter, 0.1, 100, 1024, 0, 4, 1.5);
    }
    assertThat(governorField(counter, "retreatTarget"))
      .as("a floor beyond the band retreats to the anchor")
      .isEqualTo(10);
  }

  /**
   * R2 (audit-clock escalation, WindowClimber's reschedule): a completed
   * FAILED release walk at the deepest ladder rung plants the deep-fail
   * mark — the next audit re-test of the release direction waits the
   * doubled interval (AUDIT_WAIT_MAX = 64) instead of the standard 8.
   */
  @Test
  void governor_deepFailedRelease_doublesAuditWait() throws Exception {
    setGovernorField(counter, "floor", 56);
    setLadderField(counter, "releaseLadder", "rung", 32); // deepest rung
    for (int i = 0; i < 9; i++) {
      onTide(counter, 0.85, 100, 1024, 0, 4, 1.5); // audit-due arms the walk (base 0.85)
    }
    assertThat(floorOf(counter)).isEqualTo(48);
    // 16 tides above the crash bar (0.75) but below the base (0.85):
    // the R5 beatBase test never passes, so the budget expires FAILED.
    for (int i = 0; i < 16; i++) {
      onTide(counter, 0.8, 100, 1024, 0, 4, 1.5);
    }
    assertThat(ladderField(counter, "releaseLadder", "rung")).as("fail doubles, clamped at max").isEqualTo(32);
    assertThat(ladderBoolField(counter, "releaseLadder", "deepFail"))
      .as("deepest-rung FAILED plants the audit reschedule mark")
      .isTrue();
    assertThat(auditWaitOf(counter)).as("the next audit waits the doubled interval").isEqualTo(64);
    assertThat(governorField(counter, "retreatTarget"))
      .as("the FAILED release undoes to its frozen base")
      .isEqualTo(56);
  }

  /**
   * R3 (workload-shift stand-down, WindowClimber's standDown + rates
   * reset): a single-tide goal-metric move >= RESTART_THRESHOLD in the
   * parked state discards the renewal references and the distress streak,
   * but the floor stays put, the AUDIT clock survives (position
   * stillness is orthogonal to the rate), and the shift tide's sample
   * re-seeds the references (Caffeine's reset-then-update ordering).
   */
  @Test
  void governor_workloadShift_standsDownReferences() throws Exception {
    for (int i = 0; i < 5; i++) {
      onTide(counter, 0.85, 100, 1024, 0, 4, 1.5); // healthy: audit run accumulates
    }
    assertThat(governorField(counter, "auditTides")).isEqualTo(5);
    onTide(counter, 0.1, 100, 1024, 0, 4, 1.5); // |0.85 - 0.1| >= 0.05: shift
    assertThat(governorDoubleField(counter, "smoothedRenewal"))
      .as("the shift tide's sample seeds the re-learned reference")
      .isEqualTo(0.1);
    assertThat(governorDoubleField(counter, "renewalDeviation"))
      .as("the deviation re-seeds wide (no scatter yet)")
      .isEqualTo(0.05);
    assertThat(governorField(counter, "auditTides"))
      .as("the audit clock survives the shift (decayed, not reset)")
      .isEqualTo(4);
    assertThat(floorOf(counter)).as("the floor is deliberately untouched").isEqualTo(10);
  }

  /** R3 anchor handling: discarded when the shift lands at the anchor, kept otherwise. */
  @Test
  void governor_workloadShift_anchorDiscardIsPositional() throws Exception {
    onTide(counter, 0.25, 16, 16, 10, 2, 0.5); // arm
    onTide(counter, 0.25, 16, 16, 10, 2, 0.5); // step: floor 18
    for (int i = 0; i < 3; i++) {
      onTide(counter, 1.0, 3, 16, 0, 2, 1.5); // confirm: anchor (10, 0.25)
    }
    setGovernorField(counter, "floor", 13); // within the band of the anchor
    onTide(counter, 0.5, 100, 1024, 0, 4, 1.5); // shift at the anchor position
    assertThat(governorField(counter, "anchorFloor"))
      .as("a claim tested at its own position and found wrong is discarded")
      .isZero();
    // re-confirm and shift AWAY from the anchor
    for (int i = 0; i < 9; i++) {
      onTide(counter, 0.85, 100, 1024, 0, 4, 1.5); // audit-due arms + walk
    }
    for (int i = 0; i < 20; i++) {
      onTide(counter, 0.9, 100, 1024, 0, 4, 1.5);
    }
    // release walk confirmed: anchor at the descended position
    int anchor = governorField(counter, "anchorFloor");
    assertThat(anchor).isGreaterThan(0);
    setGovernorField(counter, "floor", Math.min(anchor + 10, 250)); // away from the anchor
    onTide(counter, 0.5, 100, 1024, 0, 4, 1.5); // shift away from the anchor
    assertThat(governorField(counter, "anchorFloor"))
      .as("a shift far from the anchor leaves the reference in place")
      .isEqualTo(anchor);
  }

  /**
   * R5 (beatBase, Caffeine's audit confirm): a release walk whose budget
   * expires entirely below the arming renewal is a confirm against a
   * colder reference — it FAILS (undo + ladder escalation) instead of
   * confirming the descended position.
   */
  @Test
  void governor_releaseWalk_budgetBelowBase_failsInsteadOfConfirming() throws Exception {
    setGovernorField(counter, "floor", 56);
    for (int i = 0; i < 9; i++) {
      onTide(counter, 0.85, 100, 1024, 0, 4, 1.5); // audit-due arms the walk (base 0.85)
    }
    assertThat(floorOf(counter)).isEqualTo(48);
    for (int i = 0; i < 16; i++) {
      onTide(counter, 0.8, 100, 1024, 0, 4, 1.5); // above the bar, below the base
    }
    assertThat(governorField(counter, "retreatTarget"))
      .as("budget below the base is a failed experiment, not a confirm")
      .isEqualTo(56);
    assertThat(ladderField(counter, "releaseLadder", "rung"))
      .as("FAILED doubles the rung, floored at the initial backoff")
      .isEqualTo(4);
    int guard = 0;
    while (floorOf(counter) != 56 && guard++ < 20) {
      onTide(counter, 0.85, 100, 1024, 0, 4, 1.5);
    }
    assertThat(floorOf(counter)).as("the failed release undid to the base").isEqualTo(56);
  }

  /** R5 control: a walk whose goal metric matched its start confirms normally. */
  @Test
  void governor_releaseWalk_matchingBase_confirms() throws Exception {
    setGovernorField(counter, "floor", 56);
    for (int i = 0; i < 9; i++) {
      onTide(counter, 0.85, 100, 1024, 0, 4, 1.5); // audit-due arms the walk (base 0.85)
    }
    assertThat(floorOf(counter)).isEqualTo(48);
    for (int i = 0; i < 16; i++) {
      onTide(counter, 0.9, 100, 1024, 0, 4, 1.5); // at/above the base: beatBase
    }
    assertThat(walkOf(counter)).as("confirmed: no walk in flight").isNull();
    assertThat(ladderField(counter, "releaseLadder", "rung"))
      .as("a confirmed walk rewards the ladder")
      .isEqualTo(1);
    assertThat(floorOf(counter)).as("the descended position is kept").isEqualTo(10);
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
