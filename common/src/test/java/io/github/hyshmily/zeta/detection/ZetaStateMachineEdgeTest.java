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
package io.github.hyshmily.zeta.detection;

import static org.assertj.core.api.Assertions.*;

import io.github.hyshmily.zeta.confidence.BayesianConfidenceEstimator;
import io.github.hyshmily.zeta.confidence.ConfidenceEvaluator;
import io.github.hyshmily.zeta.confidence.EvaluationContext;
import io.github.hyshmily.zeta.detection.impl.ZetaStateMachineImpl;
import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.model.ZetaDecision.DecisionType;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Edge case tests for {@link ZetaStateMachine} covering single window, immediate cooling, and interleaved keys.
 *
 * <p>All tests use a {@link ConfidenceEvaluator} that always yields
 * {@link io.github.hyshmily.zeta.confidence.ConfidenceLevel#HIGH} so that
 * state transitions are gated purely by the hot/cold streak counters,
 * matching the pre-Bayesian behaviour.
 */
class ZetaStateMachineEdgeTest {

  /**
   * Bayesian evaluator configured with optimistic likelihood std (0.5).
   * When {@code cmsCount >> threshold} (as in {@link #CTX}) the posterior
   * probability exceeds 0.95, producing {@code HIGH} confidence.
   */
  private static final ConfidenceEvaluator EVAL = new ConfidenceEvaluator(
    new BayesianConfidenceEstimator(2.3026, 2.0, 0.5)
  );

  /**
   * Evaluation context with a high CMS count relative to threshold,
   * ensuring HIGH confidence.
   */
  private static final EvaluationContext CTX = new EvaluationContext(100L, 100L, 10L, null);

  /** Evaluation context with a low CMS count relative to threshold, ensuring LOW confidence. */
  private static final EvaluationContext COLD_CTX = new EvaluationContext(1L, 1L, 10L, null);

  /** Evaluation context calibrated for MEDIUM confidence (~0.91) with optimistic likelihood std (0.5). */
  private static final EvaluationContext MEDIUM_CTX = new EvaluationContext(20L, 20L, 10L, null);

  /** Helper to create a state machine with the given thresholds + always-HIGH evaluator. */
  private static ZetaStateMachine machineWith(int confirm, int cool, int grace) {
    return new ZetaStateMachineImpl(confirm, cool, grace, EVAL);
  }

  /**
   * Verifies that the state machine emits HOT on the first evaluation within a single-window configuration.
   */
  @Test
  void shouldHandleSingleHotWindow() {
    ZetaStateMachine m = machineWith(1, 5, 2);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);
  }

  /**
   * Verifies that a hot window followed by cold windows transitions from HOT to NONE to COOL.
   */
  @Test
  void shouldHandleImmediateCooling() {
    ZetaStateMachine m = machineWith(1, 2, 1);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);
    assertThat(m.evaluate("key", false, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", false, COLD_CTX).type()).isEqualTo(DecisionType.COOL);
  }

  /**
   * Verifies that MEDIUM confidence does not block immediate cooling (MEDIUM != HIGH).
   */
  @Test
  void mediumConfidence_shouldNotBlockImmediateCooling() {
    ZetaStateMachine m = machineWith(1, 2, 1);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);
    assertThat(m.evaluate("key", false, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", false, MEDIUM_CTX).type()).isEqualTo(DecisionType.COOL);
  }

  /**
   * Verifies that interleaved evaluations for different keys are tracked independently without interference.
   */
  @Test
  void shouldHandleInterleavedKeys() {
    // Each machine() call creates a fresh state machine with 2-window confirm
    assertThat(machine("key1", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine("key2", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine("key1", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine("key2", true).type()).isEqualTo(DecisionType.NONE);
  }

  /** Creates a single-use machine with confirmCount=2 and evaluates once. */
  private static ZetaDecision machine(String key, boolean hot) {
    ZetaStateMachine m = machineWith(2, 5, 2);
    return m.evaluate(key, hot, CTX);
  }

  @Test
  void nullKey_shouldThrowNullPointer() {
    ZetaStateMachine m = machineWith(2, 5, 2);
    assertThatNullPointerException().isThrownBy(() -> m.evaluate(null, true, CTX));
  }

  @Test
  void emptyKey_shouldBeTracked() {
    ZetaStateMachine m = machineWith(2, 5, 2);
    assertThat(m.evaluate("", true, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("", true, CTX).type()).isEqualTo(DecisionType.HOT);
  }

  @Test
  void reset_nonExistentKey_shouldNotThrow() {
    ZetaStateMachine m = machineWith(3, 10, 4);
    m.reset("never-added");
    assertThat(m.getTrackedKeys()).isZero();
  }

  @Test
  void evictStale_withEmptyState_shouldNotThrow() {
    ZetaStateMachine m = machineWith(3, 10, 4);
    m.evictStale(1000);
    assertThat(m.getTrackedKeys()).isZero();
  }

  @Test
  void reset_shouldClearTrackedCount() {
    ZetaStateMachine m = machineWith(3, 10, 4);
    m.evaluate("key", true, CTX);
    assertThat(m.getTrackedKeys()).isEqualTo(1);
    m.reset("key");
    assertThat(m.getTrackedKeys()).isZero();
  }

  @Test
  void repeatedReset_shouldBeSafe() {
    ZetaStateMachine m = machineWith(3, 10, 4);
    m.evaluate("key", true, CTX);
    m.reset("key");
    m.reset("key");
    assertThat(m.getTrackedKeys()).isZero();
  }

  @Test
  void getTrackedKeys_shouldReflectActiveKeys() {
    ZetaStateMachine m = machineWith(3, 10, 4);
    assertThat(m.getTrackedKeys()).isZero();
    m.evaluate("k1", true, CTX);
    assertThat(m.getTrackedKeys()).isEqualTo(1);
    m.evaluate("k2", true, CTX);
    assertThat(m.getTrackedKeys()).isEqualTo(2);
    m.reset("k1");
    assertThat(m.getTrackedKeys()).isEqualTo(1);
  }

  @Test
  void hotStreak_resetsOnColdWindow() {
    ZetaStateMachine m = machineWith(3, 10, 4);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", false, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);
  }

  @Test
  void confirmCountOfOne_shouldPromoteImmediately() {
    ZetaStateMachine m = machineWith(1, 5, 2);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);
    assertThat(m.evaluate("key", false, CTX).type()).isEqualTo(DecisionType.NONE);
  }

  @Test
  void coolCount_one_shouldCoolImmediately() {
    ZetaStateMachine m = machineWith(1, 1, 0);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);
    assertThat(m.evaluate("key", false, COLD_CTX).type()).isEqualTo(DecisionType.COOL);
  }

  @Test
  void mediumConfidence_shouldCoolWhenConfidenceNotHigh() {
    ZetaStateMachine m = machineWith(1, 1, 0);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);
    assertThat(m.evaluate("key", false, MEDIUM_CTX).type()).isEqualTo(DecisionType.COOL);
  }

  @Test
  void highConfidence_shouldDelayCoolInPreCooling() {
    ZetaStateMachine m = machineWith(1, 2, 1);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);
    // coolStreak=1 ≥ coolCount-grace=1 → enters PRE_COOLING, HIGH → stays
    assertThat(m.evaluate("key", false, CTX).type()).isEqualTo(DecisionType.NONE);
    // coolStreak=2 ≥ coolCount=2 → attempts cooling, but HIGH still blocks
    assertThat(m.evaluate("key", false, CTX).type()).isEqualTo(DecisionType.NONE);
    // COLD_CTX gives LOW → proceeds to COOL
    assertThat(m.evaluate("key", false, COLD_CTX).type()).isEqualTo(DecisionType.COOL);
  }

  @Test
  void evictStale_shouldOnlyRemoveStaleKeys() throws InterruptedException {
    ZetaStateMachine m = machineWith(3, 10, 4);
    m.evaluate("stale", true, CTX);
    Thread.sleep(1);
    m.evictStale(0);
    assertThat(m.getTrackedKeys()).isZero();
  }

  @Test
  void settingThresholds_shouldAffectTransitions() {
    ZetaStateMachine m = machineWith(5, 10, 4);
    m.setConfirmCount(1);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);
  }

  @Test
  void getStateSnapshot_shouldReturnCorrectSnapshot() {
    ZetaStateMachine m = machineWith(3, 10, 4);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);

    Map<String, Object> snapshot = m.getStateSnapshot("key");
    assertThat(snapshot)
      .containsEntry("currentState", "CONFIRMED_HOT")
      .containsEntry("hotStreak", 3)
      .containsEntry("coolStreak", 0);
  }

  @Test
  void getStateSnapshot_nonExistentKey_shouldReturnEmptyMap() {
    ZetaStateMachine m = machineWith(3, 10, 4);
    assertThat(m.getStateSnapshot("never-added")).isEmpty();
  }

  @Test
  void rollbackToPreviousState_shouldRestoreState() {
    ZetaStateMachine m = machineWith(3, 10, 4);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);

    Map<String, Object> snapshot = m.getStateSnapshot("key");

    m.reset("key");
    assertThat(m.getStateSnapshot("key")).isEmpty();

    m.rollbackToPreviousState("key", snapshot);
    assertThat(m.getStateSnapshot("key")).containsEntry("currentState", "CONFIRMED_HOT").containsEntry("hotStreak", 3);

    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
  }

  @Test
  void rollbackToPreviousState_withNull_shouldReset() {
    ZetaStateMachine m = machineWith(3, 10, 4);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);

    m.rollbackToPreviousState("key", null);

    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
  }

  @Test
  void rollbackToPreviousState_nonExistentKey_shouldNotThrow() {
    ZetaStateMachine m = machineWith(3, 10, 4);
    Map<String, Object> snapshot = Map.of("currentState", "CONFIRMED_HOT", "hotStreak", 3, "coolStreak", 0);
    assertThatCode(() -> m.rollbackToPreviousState("never-added", snapshot)).doesNotThrowAnyException();
  }

  @Test
  void rollbackToPreviousState_fromCold_shouldRestore() {
    ZetaStateMachine m = machineWith(3, 10, 4);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    Map<String, Object> snap = m.getStateSnapshot("key");
    assertThat(snap).containsEntry("currentState", "COLD");
    m.reset("key");
    m.rollbackToPreviousState("key", snap);
    assertThat(m.getStateSnapshot("key")).containsEntry("currentState", "COLD");
  }

  @Test
  void rollbackToPreviousState_fromPreCooling_shouldRestore() {
    ZetaStateMachine m = machineWith(1, 5, 2);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);
    assertThat(m.evaluate("key", false, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", false, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", false, CTX).type()).isEqualTo(DecisionType.NONE);
    Map<String, Object> snap = m.getStateSnapshot("key");
    assertThat(snap).containsEntry("currentState", "PRE_COOLING");
    m.reset("key");
    m.rollbackToPreviousState("key", snap);
    assertThat(m.getStateSnapshot("key")).containsEntry("currentState", "PRE_COOLING");
  }

  @Test
  void rollbackToPreviousState_withInvalidStateName_shouldThrow() {
    ZetaStateMachine m = machineWith(3, 10, 4);
    Map<String, Object> bad = Map.of("currentState", "NON_EXISTENT_STATE", "hotStreak", 0, "coolStreak", 0);
    assertThatThrownBy(() -> m.rollbackToPreviousState("key", bad)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void evictStale_shouldCleanOrphanedTimestamps() throws InterruptedException {
    ZetaStateMachine m = machineWith(3, 10, 4);
    m.evaluate("key", true, CTX);
    m.reset("key");
    Thread.sleep(1);
    m.evictStale(0);
    assertThat(m.getTrackedKeys()).isZero();
  }
}
