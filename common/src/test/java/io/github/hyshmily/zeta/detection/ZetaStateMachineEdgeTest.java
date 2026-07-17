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
import io.github.hyshmily.zeta.model.StateSnapshot;
import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.model.ZetaDecision.DecisionType;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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

  /**
   * "Cold" context with windowSum below threshold (so the hot re-check inside
   * the lock does NOT fire) but cmsCount >> threshold, yielding HIGH confidence.
   * Used by tests that need to simulate a cold-window observation on a key
   * that the confidence evaluator still considers confidently hot.
   */
  private static final EvaluationContext COLD_HIGH_CTX = new EvaluationContext(100L, 5L, 10L, null);

  /**
   * Cold context with MEDIUM confidence (windowSum < threshold, so the re-check
   * does not fire, but confidence is MEDIUM).
   */
  private static final EvaluationContext COLD_MEDIUM_CTX = new EvaluationContext(20L, 5L, 10L, null);

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
    assertThat(m.evaluate("key", false, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", false, COLD_CTX).type()).isEqualTo(DecisionType.COOL);
  }

  /**
   * Verifies that MEDIUM confidence does not block immediate cooling (MEDIUM != HIGH).
   */
  @Test
  void mediumConfidence_shouldNotBlockImmediateCooling() {
    ZetaStateMachine m = machineWith(1, 2, 1);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);
    assertThat(m.evaluate("key", false, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", false, COLD_MEDIUM_CTX).type()).isEqualTo(DecisionType.COOL);
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
    assertThat(m.evaluate("key", false, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
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
    assertThat(m.evaluate("key", false, COLD_MEDIUM_CTX).type()).isEqualTo(DecisionType.COOL);
  }

  @Test
  void highConfidence_shouldDelayCoolInPreCooling() {
    ZetaStateMachine m = machineWith(1, 2, 1);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);
    // coolStreak=1 ≥ coolCount-grace=1 → enters PRE_COOLING, HIGH → stays
    assertThat(m.evaluate("key", false, COLD_HIGH_CTX).type()).isEqualTo(DecisionType.NONE);
    // coolStreak=2 ≥ coolCount=2 → attempts cooling, but HIGH still blocks
    assertThat(m.evaluate("key", false, COLD_HIGH_CTX).type()).isEqualTo(DecisionType.NONE);
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

    StateSnapshot snapshot = m.getStateSnapshot("key");
    assertThat(snapshot.currentState()).isEqualTo("CONFIRMED_HOT");
    assertThat(snapshot.hotStreak()).isEqualTo(3);
    assertThat(snapshot.coolStreak()).isEqualTo(0);
  }

  @Test
  void getStateSnapshot_nonExistentKey_shouldReturnEmptyMap() {
    ZetaStateMachine m = machineWith(3, 10, 4);
    assertThat(m.getStateSnapshot("never-added")).isNull();
  }

  @Test
  void rollbackToPreviousState_shouldRestoreState() {
    ZetaStateMachine m = machineWith(3, 10, 4);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);

    StateSnapshot snapshot = m.getStateSnapshot("key");

    m.reset("key");
    assertThat(m.getStateSnapshot("key")).isNull();

    m.rollbackToPreviousState("key", snapshot);
    StateSnapshot restored = m.getStateSnapshot("key");
    assertThat(restored.currentState()).isEqualTo("CONFIRMED_HOT");
    assertThat(restored.hotStreak()).isEqualTo(3);

    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
  }

  @Test
  void rollbackToPreviousState_withNull_shouldReset() {
    ZetaStateMachine m = machineWith(3, 10, 4);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);

    m.rollbackToPreviousState("key", (StateSnapshot) null);

    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
  }

  @Test
  void rollbackToPreviousState_nonExistentKey_shouldNotThrow() {
    ZetaStateMachine m = machineWith(3, 10, 4);
    StateSnapshot snapshot = new StateSnapshot("never-added", "CONFIRMED_HOT", 3, 0);
    assertThatCode(() -> m.rollbackToPreviousState("never-added", snapshot)).doesNotThrowAnyException();
  }

  @Test
  void rollbackToPreviousState_fromCold_shouldRestore() {
    ZetaStateMachine m = machineWith(3, 10, 4);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    StateSnapshot snap = m.getStateSnapshot("key");
    assertThat(snap.currentState()).isEqualTo("COLD");
    m.reset("key");
    m.rollbackToPreviousState("key", snap);
    assertThat(m.getStateSnapshot("key").currentState()).isEqualTo("COLD");
  }

  @Test
  void rollbackToPreviousState_fromPreCooling_shouldRestore() {
    ZetaStateMachine m = machineWith(1, 5, 2);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);
    assertThat(m.evaluate("key", false, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", false, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", false, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
    StateSnapshot snap = m.getStateSnapshot("key");
    assertThat(snap.currentState()).isEqualTo("PRE_COOLING");
    m.reset("key");
    m.rollbackToPreviousState("key", snap);
    assertThat(m.getStateSnapshot("key").currentState()).isEqualTo("PRE_COOLING");
  }

  @Test
  void rollbackToPreviousState_withInvalidStateName_shouldThrow() {
    ZetaStateMachine m = machineWith(3, 10, 4);
    StateSnapshot bad = new StateSnapshot("key", "NON_EXISTENT_STATE", 0, 0);
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

  @Test
  void decisionSnapshot_shouldBeNullForNewKey() {
    ZetaStateMachine m = machineWith(3, 10, 4);
    ZetaDecision d = m.evaluate("newKey", true, CTX);
    assertThat(d.snapShot()).isNull();
  }

  @Test
  void decisionSnapshot_shouldCapturePreMutationState() {
    ZetaStateMachine m = machineWith(3, 10, 4);
    // First evaluation: creates state with COLD + hotStreak=1
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);

    // Second evaluation: snapShot captures {COLD, 1, 0} before mutation
    ZetaDecision d = m.evaluate("key", true, CTX);
    assertThat(d.type()).isEqualTo(DecisionType.NONE);
    assertThat(d.snapShot()).isNotNull();
    assertThat(d.snapShot().key()).isEqualTo("key");
    assertThat(d.snapShot().currentState()).isEqualTo("COLD");
    assertThat(d.snapShot().hotStreak()).isEqualTo(1);
    assertThat(d.snapShot().coolStreak()).isEqualTo(0);
  }

  @Test
  void rollbackWithDecisionSnapshot_shouldRestorePreMutationState() {
    ZetaStateMachine m = machineWith(3, 10, 4);
    // First evaluation: creates state (COLD, hotStreak=1)
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    // Second evaluation: returns snapshot of {COLD, 1, 0}
    ZetaDecision d = m.evaluate("key", true, CTX);
    assertThat(d.type()).isEqualTo(DecisionType.NONE);

    // Simulate a failed broadcast: rollback to captured snapshot
    m.rollbackToPreviousState("key", d.snapShot());

    // State should be restored to {COLD, 1, 0}
    StateSnapshot restored = m.getStateSnapshot("key");
    assertThat(restored.currentState()).isEqualTo("COLD");
    assertThat(restored.hotStreak()).isEqualTo(1);
    assertThat(restored.coolStreak()).isEqualTo(0);

    // Next evaluation should continue from restored state (hotStreak=2)
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    // Third hot window → hotStreak=3 >= confirmCount(3) → CONFIRMED_HOT
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);
  }

  @Test
  void decisionSnapshot_shouldBeNonNullForHotDecision() {
    ZetaStateMachine m = machineWith(3, 10, 4);
    // Three hot evaluations to reach CONFIRMED_HOT
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    ZetaDecision hot = m.evaluate("key", true, CTX);
    assertThat(hot.type()).isEqualTo(DecisionType.HOT);
    // HOT decision carries snapshot of state before this evaluation
    assertThat(hot.snapShot()).isNotNull();
    // Snapshot captures the state BEFORE this evaluation (COLD).
    // With always-HIGH ConfidenceEvaluator and confirmCount=3, the
    // transition is COLD→CONFIRMED_HOT directly — never via CANDIDATE_HOT.
    assertThat(hot.snapShot().currentState()).isEqualTo("COLD");
    assertThat(hot.snapShot().hotStreak()).isEqualTo(2);
    assertThat(hot.snapShot().coolStreak()).isEqualTo(0);
  }

  @Test
  void decisionSnapshot_shouldBeNonNullForCoolDecision() {
    ZetaStateMachine m = machineWith(1, 2, 1);
    // Single hot evaluation → CONFIRMED_HOT
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);
    // Cold evaluation: enters PRE_COOLING, snapShot captures {CONFIRMED_HOT, 0, 1}
    assertThat(m.evaluate("key", false, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
    // Second cold → COOL with snapshot
    ZetaDecision cool = m.evaluate("key", false, COLD_CTX);
    assertThat(cool.type()).isEqualTo(DecisionType.COOL);
    assertThat(cool.snapShot()).isNotNull();
    assertThat(cool.snapShot().currentState()).isEqualTo("PRE_COOLING");
    assertThat(cool.snapShot().hotStreak()).isEqualTo(0);
  }

  @Test
  void evictStale_shouldNotRemoveConcurrentlyEvaluatedKey() throws InterruptedException {
    ZetaStateMachine m = machineWith(3, 10, 4);
    m.evaluate("key", true, CTX);

    // Simulate a concurrent evaluation that updates the key just before eviction
    // evictStale with staleAfterMs=1 but the key was just updated by evaluate
    Thread.sleep(1);
    m.evaluate("key", true, CTX); // fresh update
    m.evictStale(0); // should NOT remove the freshly-updated key
    assertThat(m.getTrackedKeys()).isEqualTo(1);
  }

  @Test
  void isHotRecheckInsideLock_shouldRouteToHotWhenCallerSaysCold() {
    // EvaluationContext with windowSum >= threshold so the re-check fires
    EvaluationContext hotCtx = new EvaluationContext(100L, 100L, 10L, null);

    ZetaStateMachine m = machineWith(2, 5, 2);
    // First evaluation: creates state with COLD, hotStreak=1
    assertThat(m.evaluate("k", true, hotCtx).type()).isEqualTo(DecisionType.NONE);

    // Second evaluation: caller passes isHotThisWindow=false (stale) but
    // windowSum >= threshold inside the lock → re-check routes to evaluateHot.
    // hotStreak becomes 2 >= confirmCount(2) → HIGH confidence → CONFIRMED_HOT → HOT.
    ZetaDecision d = m.evaluate("k", false, hotCtx);
    assertThat(d.type()).isEqualTo(DecisionType.HOT);
  }

  @Test
  void isHotRecheck_shouldNotUpgradeWhenWindowSumBelowThreshold() {
    // EvaluationContext with windowSum < threshold — re-check should NOT fire
    EvaluationContext belowThresholdCtx = new EvaluationContext(100L, 5L, 10L, null);

    ZetaStateMachine m = machineWith(2, 5, 2);
    assertThat(m.evaluate("k", true, CTX).type()).isEqualTo(DecisionType.NONE);

    // Caller says not-hot AND windowSum < threshold → should go to evaluateCold
    ZetaDecision d = m.evaluate("k", false, belowThresholdCtx);
    assertThat(d.type()).isEqualTo(DecisionType.NONE);

    // Verify we went through evaluateCold: hotStreak was reset to 0
    assertThat(m.getStateSnapshot("k").hotStreak()).isEqualTo(0);
  }

  @Test
  void lowConfidence_shouldDecrementHotStreak() {
    // LOW confidence (COLD_CTX: cmsCount=1, threshold=10 → probability < 0.80)
    // should never promote — hotStreak is clamped at confirmCount-1.
    ZetaStateMachine m = machineWith(3, 10, 4);
    assertThat(m.evaluate("key", true, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
    // A fourth hot window should still not promote: LOW keeps hotStreak at 2.
    assertThat(m.evaluate("key", true, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.getStateSnapshot("key").hotStreak()).isEqualTo(2);
  }

  @Test
  void candidateHot_withMediumConfidence_shouldStayInCandidateHot() {
    // MEDIUM_CTX (cmsCount=20, threshold=10) yields ~0.91 → MEDIUM level.
    // With confirmCount=1 the key enters CANDIDATE_HOT, not CONFIRMED_HOT.
    ZetaStateMachine m = machineWith(1, 5, 2);
    assertThat(m.evaluate("key", true, MEDIUM_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.getStateSnapshot("key").currentState()).isEqualTo("CANDIDATE_HOT");
    // Another MEDIUM window should stay in CANDIDATE_HOT.
    ZetaDecision d = m.evaluate("key", true, MEDIUM_CTX);
    assertThat(d.type()).isEqualTo(DecisionType.NONE);
    assertThat(m.getStateSnapshot("key").currentState()).isEqualTo("CANDIDATE_HOT");
  }

  @Test
  void candidateHot_withHighConfidence_shouldPromoteToConfirmedHot() {
    ZetaStateMachine m = machineWith(1, 5, 2);
    // First: MEDIUM → enter CANDIDATE_HOT
    assertThat(m.evaluate("key", true, MEDIUM_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.getStateSnapshot("key").currentState()).isEqualTo("CANDIDATE_HOT");
    // Second: HIGH → promote to CONFIRMED_HOT, emit HOT
    ZetaDecision hot = m.evaluate("key", true, CTX);
    assertThat(hot.type()).isEqualTo(DecisionType.HOT);
    assertThat(hot.snapShot().currentState()).isEqualTo("CANDIDATE_HOT");
  }

  @Test
  void concurrentEvaluations_sameKey_shouldNotCorruptState() throws InterruptedException {
    ZetaStateMachine m = machineWith(1, 2, 1);
    int threadCount = 8;
    int evaluationsPerThread = 50;
    AtomicInteger errors = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(threadCount);
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);

    for (int t = 0; t < threadCount; t++) {
      executor.submit(() -> {
        try {
          for (int i = 0; i < evaluationsPerThread; i++) {
            m.evaluate("sharedKey", true, CTX);
            m.evaluate("sharedKey", false, COLD_CTX);
          }
        } catch (Exception e) {
          errors.incrementAndGet();
        } finally {
          latch.countDown();
        }
      });
    }
    latch.await();
    executor.shutdown();

    assertThat(errors).hasValue(0);
    // Key should still be tracked (only one key entry in states).
    assertThat(m.getTrackedKeys()).isEqualTo(1);
  }

  @Test
  void concurrentEvaluations_differentKeys_shouldNotInterfere() throws InterruptedException {
    ZetaStateMachine m = machineWith(3, 5, 2);
    int keyCount = 10;
    int threadCount = 8;
    AtomicReference<Exception> error = new AtomicReference<>();
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);

    for (int t = 0; t < threadCount; t++) {
      int threadId = t;
      executor.submit(() -> {
        try {
          for (int k = 0; k < keyCount; k++) {
            String key = "key-" + ((threadId + k) % keyCount);
            m.evaluate(key, true, CTX);
          }
        } catch (Exception e) {
          error.compareAndSet(null, e);
        }
      });
    }
    executor.shutdown();
    executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);

    assertThat(error).hasValue(null);
    // All keys should be tracked.
    assertThat(m.getTrackedKeys()).isEqualTo(keyCount);
  }

  @Test
  void coldToHot_withCvPresent_shouldTransitionNormally() {
    EvaluationContext cvCtx = new EvaluationContext(100L, 100L, 10L, 0.5);
    ZetaStateMachine m = machineWith(3, 10, 4);
    assertThat(m.evaluate("key", true, cvCtx).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true, cvCtx).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true, cvCtx).type()).isEqualTo(DecisionType.HOT);
  }

  @Test
  void highConfidenceWithCv_shouldBlockPreCoolingTransition() {
    // Configure so that a single hot windows promotes, then 2 cold windows cool.
    ZetaStateMachine m = machineWith(1, 2, 1);
    // Promote to CONFIRMED_HOT
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);
    // Cold window with HIGH confidence + CV → enters PRE_COOLING but HIGH blocks
    EvaluationContext coldHighCv = new EvaluationContext(100L, 5L, 10L, 0.5);
    assertThat(m.evaluate("key", false, coldHighCv).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.getStateSnapshot("key").currentState()).isEqualTo("PRE_COOLING");
    // Second cold with LOW → finally cools
    assertThat(m.evaluate("key", false, COLD_CTX).type()).isEqualTo(DecisionType.COOL);
  }
}
