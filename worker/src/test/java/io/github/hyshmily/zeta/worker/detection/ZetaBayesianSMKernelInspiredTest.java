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
package io.github.hyshmily.zeta.worker.detection;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.model.EvaluationContext;
import io.github.hyshmily.zeta.model.StateSnapshot;
import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.model.ZetaDecision.DecisionType;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.worker.confidence.BayesianConfidenceEstimator;
import io.github.hyshmily.zeta.worker.confidence.ConfidenceEvaluator;
import io.github.hyshmily.zeta.worker.detection.impl.ZetaBayesianSM;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the three implemented sub-items of the kernel-inspired
 * optimizations doc §6 (CUBIC fast_convergence / HyStart / BBR full_bw,
 * all verified against the local 7.3-rc4 kernel sources):
 *
 * <ul>
 *   <li><b>§6.1 demotion hysteresis (Schmitt refactor):</b> a key demoted
 *       with a COOL broadcast faces a re-promotion bar of threshold × 1.25
 *       until it re-promotes; the gate rides the state snapshot for rollback.</li>
 *   <li><b>§6.3 full_bw stall exit:</b> 3 consecutive hot windows with gain
 *       below 1.25× during the COLD observation phase consult the Bayesian
 *       gate immediately (confirmCount > 4 only).</li>
 *   <li><b>§6.4 idle-epoch shift:</b> silence between evaluations credits the
 *       missed windows into coolStreak for CONFIRMED_HOT / PRE_COOLING keys,
 *       so a sparse resume after a long gap emits the overdue COOL instead of
 *       re-earning coolCount from zero.</li>
 * </ul>
 *
 * <p>All tests use the always-permissive {@code BayesianConfidenceEstimator}
 * configuration from {@code ZetaBayesianSMTest} (priorStd 2.0) so streak
 * gates dominate: observations ≥ ~24× threshold read as HIGH, window sums
 * of 1 read as LOW. Constructed via the 7-arg constructor with a 50 ms
 * counter window (the production default from
 * {@code smDurationMs/smSlices}); the legacy 6-arg constructor keeps idle
 * crediting disabled and is covered by {@code legacyConstructor_disablesIdleCredit}.
 */
class ZetaBayesianSMKernelInspiredTest {

  private static final ConfidenceEvaluator EVAL = new BayesianConfidenceEstimator(
    BayesianConfidenceEstimator.PRIOR_MEAN, 2.0, 0.5
  );

  /** Hot window: sum 100 vs threshold 10 → HIGH confidence. */
  private static final EvaluationContext HOT_CTX = new EvaluationContext(100L, 100L, 10L, Double.NaN, 0.0);

  /** Cold window: sum 1 vs threshold 10 → LOW confidence. */
  private static final EvaluationContext COLD_CTX = new EvaluationContext(1L, 1L, 10L, Double.NaN, 0.0);

  /** Sub-band window: sum 11 crosses the plain threshold 10 but not the raised 12.5 bar. */
  private static final EvaluationContext SUB_BAND_CTX = new EvaluationContext(11L, 11L, 10L, Double.NaN, 0.0);

  /** Mid-band window: sum 15 passes the plain-threshold verdict but lands MEDIUM (CANDIDATE). */
  private static final EvaluationContext MID_BAND_CTX = new EvaluationContext(15L, 15L, 10L, Double.NaN, 0.0);

  /** Raised-band window: sum 30 crosses both the 12.5 bar and the HIGH-confidence bar. */
  private static final EvaluationContext RAISED_CTX = new EvaluationContext(30L, 30L, 10L, Double.NaN, 0.0);

  private static final long COUNTER_WINDOW_MS = 50L;

  private ZetaBayesianSM machine;

  @BeforeEach
  void setUp() {
    // confirmCount=3, coolCount=10, preCoolGrace=4 — same triple as ZetaBayesianSMTest,
    // plus the 50 ms counter window that enables the idle-epoch shift.
    machine = new ZetaBayesianSM(3, 10, 4, EVAL, BayesianConfidenceEstimator.PRIOR_MEAN, 10_000L, COUNTER_WINDOW_MS);
  }

  @AfterEach
  void tearDown() {
    TimeSource.setTimeOffsetForTest(0, 0);
  }

  /** Promotes the key to CONFIRMED_HOT (3 hot windows, HIGH confidence). */
  private void promote(String key) {
    ZetaDecision last = null;
    for (int i = 0; i < 3; i++) {
      last = machine.evaluate(key, true, false, HOT_CTX);
    }
    assertThat(last.type()).isEqualTo(DecisionType.HOT);
  }

  /** Fully demotes the key; returns the COOL decision (10th cold window). */
  private ZetaDecision demote(String key) {
    ZetaDecision last = null;
    for (int i = 0; i < 9; i++) {
      last = machine.evaluate(key, false, false, COLD_CTX);
      assertThat(last.type()).isEqualTo(DecisionType.NONE);
    }
    last = machine.evaluate(key, false, false, COLD_CTX);
    assertThat(last.type()).isEqualTo(DecisionType.COOL);
    return last;
  }

  // ------------------------------------------------------------------
  // §6.1 demotion hysteresis (Schmitt re-promotion gate)
  // ------------------------------------------------------------------

  @Test
  void demotion_armsGate_subBandWindowStaysCold() {
    promote("k");
    demote("k");
    assertThat(machine.getStateSnapshot("k").demoteHysteresisActive()).isTrue();

    // sum 11 crosses the plain threshold but not the ×1.25 bar: the hot verdict
    // is refused even though the Evaluator's isHotThisWindow was true.
    assertThat(machine.evaluate("k", true, false, SUB_BAND_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine.getStateSnapshot("k").currentState()).isEqualTo("COLD");
  }

  @Test
  void demotion_raisedBandRePromotes_andDisarmsGate() {
    promote("k");
    demote("k");

    assertThat(machine.evaluate("k", true, false, RAISED_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine.evaluate("k", true, false, RAISED_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine.evaluate("k", true, false, RAISED_CTX).type()).isEqualTo(DecisionType.HOT);

    StateSnapshot snap = machine.getStateSnapshot("k");
    assertThat(snap.currentState()).isEqualTo("CONFIRMED_HOT");
    assertThat(snap.demoteHysteresisActive()).isFalse();
  }

  @Test
  void coolRollback_restoresPreDecisionGateState() {
    promote("k");
    ZetaDecision cool = demote("k");

    // The snapshot carried by the COOL decision was taken pre-mutation: the
    // gate was not armed yet. Rolling the failed broadcast back must undo it.
    assertThat(cool.snapShot().demoteHysteresisActive()).isFalse();
    assertThat(machine.getStateSnapshot("k").demoteHysteresisActive()).isTrue();

    machine.rollbackToPreviousState(cool.snapShot());
    StateSnapshot restored = machine.getStateSnapshot("k");
    assertThat(restored.currentState()).isEqualTo("PRE_COOLING");
    assertThat(restored.demoteHysteresisActive()).isFalse();
  }

  @Test
  void fastlane_disarmsGate() {
    promote("k");
    demote("k");
    assertThat(machine.getStateSnapshot("k").demoteHysteresisActive()).isTrue();

    ZetaDecision d = machine.evaluate("k", true, true, EvaluationContext.FASTLANE, () -> 0L);
    assertThat(d.type()).isEqualTo(DecisionType.HOT);
    assertThat(machine.getStateSnapshot("k").demoteHysteresisActive()).isFalse();
  }

  @Test
  void freshKeysAreNeverGated() {
    // No demotion ever happened: a plain-threshold window above the HIGH bar
    // promotes normally through the raised-band context (the gate is inert).
    assertThat(machine.evaluate("fresh", true, false, MID_BAND_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine.evaluate("fresh", true, false, MID_BAND_CTX).type()).isEqualTo(DecisionType.NONE);
    // sum 15 vs threshold 10 lands MEDIUM — proving the hot verdict passed at
    // a sum (15) that sits inside the ×1.25 band a demoted key would refuse.
    assertThat(machine.evaluate("fresh", true, false, MID_BAND_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine.getStateSnapshot("fresh").currentState()).isEqualTo("CANDIDATE_HOT");
  }

  // ------------------------------------------------------------------
  // §6.3 full_bw stall exit (confirmCount > 4 only)
  // ------------------------------------------------------------------

  @Test
  void plateauedGrowth_promotesOneWindowEarly() {
    ZetaBayesianSM five = new ZetaBayesianSM(5, 10, 4, EVAL, BayesianConfidenceEstimator.PRIOR_MEAN, 10_000L, COUNTER_WINDOW_MS);

    // Plateau: 100 → 110 → 115 → 118 — three consecutive gains below 1.25×.
    assertThat(five.evaluate("k", true, false, ctx(100)).type()).isEqualTo(DecisionType.NONE);
    assertThat(five.evaluate("k", true, false, ctx(110)).type()).isEqualTo(DecisionType.NONE);
    assertThat(five.evaluate("k", true, false, ctx(115)).type()).isEqualTo(DecisionType.NONE);
    // 4th window: stallCount reaches 3 → Bayesian consulted immediately.
    assertThat(five.evaluate("k", true, false, ctx(118)).type()).isEqualTo(DecisionType.HOT);
  }

  @Test
  void growingTraffic_promotesOnFullStreak() {
    ZetaBayesianSM five = new ZetaBayesianSM(5, 10, 4, EVAL, BayesianConfidenceEstimator.PRIOR_MEAN, 10_000L, COUNTER_WINDOW_MS);

    // Real growth (≥ 1.25× per window) keeps resetting the stall counter.
    assertThat(five.evaluate("k", true, false, ctx(100)).type()).isEqualTo(DecisionType.NONE);
    assertThat(five.evaluate("k", true, false, ctx(200)).type()).isEqualTo(DecisionType.NONE);
    assertThat(five.evaluate("k", true, false, ctx(300)).type()).isEqualTo(DecisionType.NONE);
    assertThat(five.evaluate("k", true, false, ctx(400)).type()).isEqualTo(DecisionType.NONE);
    assertThat(five.evaluate("k", true, false, ctx(500)).type()).isEqualTo(DecisionType.HOT);
  }

  @Test
  void coldWindow_resetsStallBaseline() {
    ZetaBayesianSM five = new ZetaBayesianSM(5, 10, 4, EVAL, BayesianConfidenceEstimator.PRIOR_MEAN, 10_000L, COUNTER_WINDOW_MS);

    assertThat(five.evaluate("k", true, false, ctx(100)).type()).isEqualTo(DecisionType.NONE);
    assertThat(five.evaluate("k", true, false, ctx(110)).type()).isEqualTo(DecisionType.NONE);
    // Cold window interrupts the observation epoch...
    assertThat(five.evaluate("k", false, false, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
    // ...so the plateau counter starts over: no early promotion within the
    // next three windows despite three more sub-1.25× gains after re-baselining.
    assertThat(five.evaluate("k", true, false, ctx(100)).type()).isEqualTo(DecisionType.NONE);
    assertThat(five.evaluate("k", true, false, ctx(110)).type()).isEqualTo(DecisionType.NONE);
    assertThat(five.evaluate("k", true, false, ctx(115)).type()).isEqualTo(DecisionType.NONE);
    assertThat(five.evaluate("k", true, false, ctx(118)).type()).isEqualTo(DecisionType.HOT);
  }

  /** Default confirmCount=1 never enters the observation phase — stall tracking is inert. */
  @Test
  void defaultConfirmCount_bypassesStallTracking() {
    ZetaBayesianSM one = new ZetaBayesianSM(1, 10, 4, EVAL, BayesianConfidenceEstimator.PRIOR_MEAN, 10_000L, COUNTER_WINDOW_MS);
    assertThat(one.evaluate("k", true, false, HOT_CTX).type()).isEqualTo(DecisionType.HOT);
  }

  private static EvaluationContext ctx(long sum) {
    return new EvaluationContext(sum, sum, 10L, Double.NaN, 0.0);
  }

  // ------------------------------------------------------------------
  // §6.4 idle-epoch shift
  // ------------------------------------------------------------------

  @Test
  void longSilence_sparseResume_emitsOverdueCoolImmediately() {
    promote("k");
    // 6 cold windows enter PRE_COOLING (coolStreak 6 of coolCount 10).
    for (int i = 0; i < 6; i++) {
      assertThat(machine.evaluate("k", false, false, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
    }

    // 10 s of silence = 200 missed 50 ms windows (capped at coolCount + 1).
    TimeSource.setTimeOffsetForTest(0, 10_000);
    // Sparse resume (one small report): the credited windows push coolStreak
    // past coolCount, so the Bayesian gate runs on the tiny observation and
    // emits the COOL that the silence already earned.
    assertThat(machine.evaluate("k", false, false, COLD_CTX).type()).isEqualTo(DecisionType.COOL);
  }

  @Test
  void longSilence_duringHot_sparseColdResume_emitsCoolInOneStep() {
    promote("k");
    // Still CONFIRMED_HOT (no cold windows evaluated) when silence begins.
    TimeSource.setTimeOffsetForTest(0, 10_000);
    // Credit jumps coolStreak past both the grace and the full cool-down.
    assertThat(machine.evaluate("k", false, false, COLD_CTX).type()).isEqualTo(DecisionType.COOL);
  }

  @Test
  void longSilence_hotResume_revivesSilentlyWithoutCool() {
    promote("k");
    for (int i = 0; i < 6; i++) {
      machine.evaluate("k", false, false, COLD_CTX);
    }
    TimeSource.setTimeOffsetForTest(0, 10_000);
    // Traffic returns hot: silent revive — the credited cooling progress is
    // discarded and no COOL broadcast is owed.
    assertThat(machine.evaluate("k", true, false, HOT_CTX).type()).isEqualTo(DecisionType.NONE);
    StateSnapshot snap = machine.getStateSnapshot("k");
    assertThat(snap.currentState()).isEqualTo("CONFIRMED_HOT");
    assertThat(snap.coolStreak()).isEqualTo(0);
  }

  /** Sub-window jitter must not credit anything (credit = full missed windows only). */
  @Test
  void subWindowJitter_doesNotAdvanceCooling() {
    promote("k");
    for (int i = 0; i < 6; i++) {
      machine.evaluate("k", false, false, COLD_CTX);
    }
    // 75 ms gap = 1 window boundary crossed, minus the current window → 0 credit.
    TimeSource.setTimeOffsetForTest(0, 75);
    assertThat(machine.evaluate("k", false, false, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine.getStateSnapshot("k").coolStreak()).isEqualTo(7);
  }

  /** The legacy 6-arg constructor keeps idle crediting disabled (existing tests unaffected). */
  @Test
  void legacyConstructor_disablesIdleCredit() {
    ZetaBayesianSM legacy = new ZetaBayesianSM(3, 10, 4, EVAL, BayesianConfidenceEstimator.PRIOR_MEAN);
    for (int i = 0; i < 3; i++) {
      legacy.evaluate("k", true, false, HOT_CTX);
    }
    for (int i = 0; i < 6; i++) {
      legacy.evaluate("k", false, false, COLD_CTX);
    }
    TimeSource.setTimeOffsetForTest(0, 10_000);
    // No credit: coolStreak only advances by the evaluation itself (7 < 10).
    assertThat(legacy.evaluate("k", false, false, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(legacy.getStateSnapshot("k").coolStreak()).isEqualTo(7);
  }
}
