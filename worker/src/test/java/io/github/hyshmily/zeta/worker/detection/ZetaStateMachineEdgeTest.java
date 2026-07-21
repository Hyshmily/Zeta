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

import static org.assertj.core.api.Assertions.*;

import io.github.hyshmily.zeta.detection.ZetaStateMachine;
import io.github.hyshmily.zeta.model.EvaluationContext;
import io.github.hyshmily.zeta.model.StateSnapshot;
import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.model.ZetaDecision.DecisionType;
import io.github.hyshmily.zeta.worker.confidence.BayesianConfidenceEstimator;
import io.github.hyshmily.zeta.worker.confidence.ConfidenceEvaluator;
import io.github.hyshmily.zeta.worker.detection.impl.ZetaStateMachineImpl;
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
 * {@link io.github.hyshmily.zeta.worker.confidence.ConfidenceLevel#HIGH} so that
 * state transitions are gated purely by the hot/cold streak counters,
 * matching the pre-Bayesian behaviour.
 */
class ZetaStateMachineEdgeTest {

  private static final ConfidenceEvaluator EVAL = new ConfidenceEvaluator(
    new BayesianConfidenceEstimator(2.3026, 2.0, 0.5)
  );

  private static final EvaluationContext CTX = new EvaluationContext(100L, 100L, 10L, null);

  private static final EvaluationContext COLD_CTX = new EvaluationContext(1L, 1L, 10L, null);

  private static final EvaluationContext MEDIUM_CTX = new EvaluationContext(20L, 20L, 10L, null);

  private static final EvaluationContext COLD_HIGH_CTX = new EvaluationContext(100L, 5L, 10L, null);

  private static final EvaluationContext COLD_MEDIUM_CTX = new EvaluationContext(20L, 5L, 10L, null);

  private static ZetaStateMachine machineWith(int confirm, int cool, int grace) {
    return new ZetaStateMachineImpl(confirm, cool, grace, EVAL);
  }

  @Test
  void shouldHandleSingleHotWindow() {
    ZetaStateMachine m = machineWith(1, 5, 2);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);
  }

  @Test
  void shouldHandleImmediateCooling() {
    ZetaStateMachine m = machineWith(1, 2, 1);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);
    assertThat(m.evaluate("key", false, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", false, COLD_CTX).type()).isEqualTo(DecisionType.COOL);
  }

  @Test
  void mediumConfidence_shouldNotBlockImmediateCooling() {
    ZetaStateMachine m = machineWith(1, 2, 1);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);
    assertThat(m.evaluate("key", false, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", false, COLD_MEDIUM_CTX).type()).isEqualTo(DecisionType.COOL);
  }

  @Test
  void shouldHandleInterleavedKeys() {
    assertThat(machine("key1", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine("key2", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine("key1", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine("key2", true).type()).isEqualTo(DecisionType.NONE);
  }

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
    m.evictStale(1000, k -> {});
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
    assertThat(m.evaluate("key", false, COLD_HIGH_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", false, COLD_HIGH_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", false, COLD_CTX).type()).isEqualTo(DecisionType.COOL);
  }

  @Test
  void evictStale_shouldOnlyRemoveStaleKeys() throws InterruptedException {
    ZetaStateMachine m = machineWith(3, 10, 4);
    m.evaluate("stale", true, CTX);
    Thread.sleep(1);
    m.evictStale(0, k -> {});
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
    m.evictStale(0, k -> {});
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
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);

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
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    ZetaDecision d = m.evaluate("key", true, CTX);
    assertThat(d.type()).isEqualTo(DecisionType.NONE);

    m.rollbackToPreviousState("key", d.snapShot());

    StateSnapshot restored = m.getStateSnapshot("key");
    assertThat(restored.currentState()).isEqualTo("COLD");
    assertThat(restored.hotStreak()).isEqualTo(1);
    assertThat(restored.coolStreak()).isEqualTo(0);

    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);
  }

  @Test
  void decisionSnapshot_shouldBeNonNullForHotDecision() {
    ZetaStateMachine m = machineWith(3, 10, 4);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    ZetaDecision hot = m.evaluate("key", true, CTX);
    assertThat(hot.type()).isEqualTo(DecisionType.HOT);
    assertThat(hot.snapShot()).isNotNull();
    assertThat(hot.snapShot().currentState()).isEqualTo("COLD");
    assertThat(hot.snapShot().hotStreak()).isEqualTo(2);
    assertThat(hot.snapShot().coolStreak()).isEqualTo(0);
  }

  @Test
  void decisionSnapshot_shouldBeNonNullForCoolDecision() {
    ZetaStateMachine m = machineWith(1, 2, 1);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);
    assertThat(m.evaluate("key", false, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
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

    Thread.sleep(1);
    m.evaluate("key", true, CTX);
    m.evictStale(1, k -> {});
    assertThat(m.getTrackedKeys()).isEqualTo(1);
  }

  @Test
  void isHotRecheckInsideLock_shouldRouteToHotWhenCallerSaysCold() {
    EvaluationContext hotCtx = new EvaluationContext(100L, 100L, 10L, null);

    ZetaStateMachine m = machineWith(2, 5, 2);
    assertThat(m.evaluate("k", true, hotCtx).type()).isEqualTo(DecisionType.NONE);

    ZetaDecision d = m.evaluate("k", false, hotCtx);
    assertThat(d.type()).isEqualTo(DecisionType.HOT);
  }

  @Test
  void isHotRecheck_shouldNotUpgradeWhenWindowSumBelowThreshold() {
    EvaluationContext belowThresholdCtx = new EvaluationContext(100L, 5L, 10L, null);

    ZetaStateMachine m = machineWith(2, 5, 2);
    assertThat(m.evaluate("k", true, CTX).type()).isEqualTo(DecisionType.NONE);

    ZetaDecision d = m.evaluate("k", false, belowThresholdCtx);
    assertThat(d.type()).isEqualTo(DecisionType.NONE);

    assertThat(m.getStateSnapshot("k").hotStreak()).isEqualTo(0);
  }

  @Test
  void lowConfidence_shouldDecrementHotStreak() {
    ZetaStateMachine m = machineWith(3, 10, 4);
    assertThat(m.evaluate("key", true, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.getStateSnapshot("key").hotStreak()).isEqualTo(2);
  }

  @Test
  void candidateHot_withMediumConfidence_shouldStayInCandidateHot() {
    ZetaStateMachine m = machineWith(1, 5, 2);
    assertThat(m.evaluate("key", true, MEDIUM_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.getStateSnapshot("key").currentState()).isEqualTo("CANDIDATE_HOT");
    ZetaDecision d = m.evaluate("key", true, MEDIUM_CTX);
    assertThat(d.type()).isEqualTo(DecisionType.NONE);
    assertThat(m.getStateSnapshot("key").currentState()).isEqualTo("CANDIDATE_HOT");
  }

  @Test
  void candidateHot_withHighConfidence_shouldPromoteToConfirmedHot() {
    ZetaStateMachine m = machineWith(1, 5, 2);
    assertThat(m.evaluate("key", true, MEDIUM_CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.getStateSnapshot("key").currentState()).isEqualTo("CANDIDATE_HOT");
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
    ZetaStateMachine m = machineWith(1, 2, 1);
    assertThat(m.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);
    EvaluationContext coldHighCv = new EvaluationContext(100L, 5L, 10L, 0.5);
    assertThat(m.evaluate("key", false, coldHighCv).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.getStateSnapshot("key").currentState()).isEqualTo("PRE_COOLING");
    assertThat(m.evaluate("key", false, COLD_CTX).type()).isEqualTo(DecisionType.COOL);
  }
}
