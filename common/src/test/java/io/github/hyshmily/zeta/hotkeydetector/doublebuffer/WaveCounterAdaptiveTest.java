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
    Object governor = governorOf(c);
    Method m = governor.getClass().getDeclaredMethod("onTide", tideReadingType());
    m.setAccessible(true);
    m.invoke(governor, newTideReading(renewal, activeSlots, hotLimit, blockedKeys, boundary, ratio));
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
    Constructor<?> ctor = tideReadingType()
      .getDeclaredConstructor(double.class, int.class, int.class, int.class, long.class, double.class);
    ctor.setAccessible(true);
    return ctor.newInstance(renewal, activeSlots, hotLimit, blockedKeys, boundary, ratio);
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
   * >= 1) must hold: the blocked band is the stale tail of the 2-tide
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
   * Distress with an UNDER-EARNING hot set (hotColdRatio < 1 — occupied
   * hot slots earn less per slot than cold keys earn per key) arms a
   * bounded raise-walk: the floor steps up, confirms after the set holds
   * the target across the crash persistence, and is later released by the
   * healthy-branch admit-on-block (anti-ratchet).
   */
  @Test
  void governor_distress_underEarning_armsRaiseWalk_admitReleases() throws Exception {
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // arm: floor 10 -> 26
    assertThat(floorOf(counter)).isEqualTo(26);
    for (int i = 0; i < 3; i++) {
      onTide(counter, 1.0, 3, 16, 0, 2, 1.5); // at-target tides: durable confirm
    }
    assertThat(floorOf(counter)).isEqualTo(26);
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
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // arm: floor 10 -> 26
    assertThat(floorOf(counter)).isEqualTo(26);
    onTide(counter, 0.2, 3, 16, 5, 2, 0.5); // distressed: bold driver steps up
    onTide(counter, 0.2, 3, 16, 5, 2, 0.5);
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
    onTide(counter, 0.25, 16, 16, 10, 2, 0.5); // under-earning distress: arm, floor 10 -> 26
    assertThat(floorOf(counter)).isEqualTo(26);
    for (int i = 0; i < 3; i++) {
      onTide(counter, 1.0, 3, 16, 0, 2, 1.5); // at-target: durable confirm, anchor (10, 0.25)
    }
    assertThat(floorOf(counter)).isEqualTo(26);
    // Re-distress worse than the anchor reference minus the margin, with
    // nothing blocked so the raise-walk cannot re-arm and mask the veto.
    // The noise band must wash the confirmation tides out of the ring
    // before the evidence gap (0.25 - 0.1 = 0.15) admits renewal 0.1.
    for (int i = 0; i < 8; i++) {
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
    assertThat(floorOf(counter)).isEqualTo(40);
    for (int i = 0; i < 18; i++) {
      onTide(counter, 0.85, 100, 1024, 0, 4, 1.5);
    }
    assertThat(floorOf(counter)).as("audit released to the base").isEqualTo(10);
  }

  /** Saturation (healthy but full) lowers a raised floor faster than the audit. */
  @Test
  void governor_saturation_lowersRaisedFloor() throws Exception {
    setGovernorField(counter, "floor", 56);
    for (int i = 0; i < 4; i++) {
      onTide(counter, 0.9, 950, 1024, 0, 4, 1.5);
    }
    assertThat(floorOf(counter)).as("saturated health lowers toward the base").isEqualTo(10);
  }

  /** The floor never leaves [10, 256]. */
  @Test
  void governor_clamps() throws Exception {
    setGovernorField(counter, "floor", 250);
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // under-earning distress arms the raise-walk
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

  private static void setGovernorField(WaveCounter c, String name, int value) throws Exception {
    Object governor = governorOf(c);
    Field f = governor.getClass().getDeclaredField(name);
    f.setAccessible(true);
    f.setInt(governor, value);
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
    assertThat(floorOf(counter)).as("walk armed at the base and took its first step").isEqualTo(40);
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
    assertThat(floorOf(counter)).isEqualTo(40);
    onTide(counter, 0.6, 100, 1024, 0, 4, 1.5); // below the anchor bar (~0.63-0.75): streak 1
    onTide(counter, 0.6, 100, 1024, 0, 4, 1.5); // streak 2 — the walk keeps stepping
    // The stride law prices the descent from the ring-mean renewal
    // against the crash bar: below-bar tides creep instead of plunging,
    // so the descent over the pre-crash window is bounded (the old bold
    // driver reached the seed here).
    assertThat(floorOf(counter)).as("below-bar tides creep, not plunge").isBetween(11, 39);
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
      onTide(counter, 0.85, 100, 1024, 0, 4, 1.5); // audit-due arms the walk (arm stride 16)
    }
    assertThat(floorOf(counter)).isEqualTo(234);
    for (int i = 0; i < 9; i++) {
      onTide(counter, 1.0, 100, 1024, 0, 4, 1.5); // fully healthy: ceiling strides
    }
    assertThat(floorOf(counter)).as("full health descends at the stride ceiling").isEqualTo(10);
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
    assertThat(floorOf(counter)).isEqualTo(84);
    for (int i = 0; i < 16; i++) {
      onTide(counter, 0.75, 100, 1024, 0, 4, 1.5); // pinned at the crash bar
    }
    assertThat(floorOf(counter))
      .as("at-bar renewal converges above the seed; the walk confirms in the decision zone")
      .isGreaterThan(10);
    assertThat(floorOf(counter)).as("the walk still descended").isLessThan(84);
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
    assertThat(floorOf(counter)).as("backoff expired: a fresh walk arms").isEqualTo(40);
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
    assertThat(floorOf(counter)).as("release walks despite the raise ladder's backoff").isEqualTo(40);
  }

  /** ... and a crashed release's backoff must not block the re-probe. */
  @Test
  void governor_releaseBackoff_doesNotBlockRaise() throws Exception {
    setGovernorField(counter, "floor", 26);
    setLadderField(counter, "releaseLadder", "left", 3); // release backoff unpaid
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // under-earning distress: raise arms
    assertThat(floorOf(counter)).as("raise arms despite the release ladder's backoff").isEqualTo(42);
  }

  /**
   * The ladder's crash pricing (Caffeine's PROBE_CRASH_ESCALATION): a
   * single crashed walk holds the rung — the re-probe waits the initial
   * backoff, not a double — and only a consecutive crash run doubles it.
   */
  @Test
  void governor_ladder_singleCrashHoldsRung_consecutiveDoubles() throws Exception {
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // arm raise #1: floor 10 -> 26
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
    onTide(counter, 0.2, 16, 16, 10, 2, 0.5); // arm: floor 10 -> 26
    assertThat(floorOf(counter)).isEqualTo(26);
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
    assertThat(floorOf(counter)).isEqualTo(40);
    onTide(counter, 0.1, 0, 1024, 0, 4, 1.5); // empty hot set: collapse
    assertThat(floorOf(counter)).as("collapsed to the seed").isEqualTo(10);
    for (int i = 0; i < 6; i++) {
      onTide(counter, 0.1, 0, 1024, 0, 4, 1.5); // would crash the stale walk → re-raise
    }
    assertThat(floorOf(counter)).as("no stale-walk undo re-raises the floor").isEqualTo(10);
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
    assertThat(floorOf(counter)).as("fresh regime: raise-walk arms from the seed").isEqualTo(26);
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
