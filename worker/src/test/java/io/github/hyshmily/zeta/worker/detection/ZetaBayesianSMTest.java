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
 * Tests for {@link io.github.hyshmily.zeta.detection.ZetaBayesianSM} covering cold-to-hot, hot-to-cool, pre-cooling revive, reset, and eviction
 * transitions.
 *
 * <p>All tests use a {@link ConfidenceEvaluator} that always yields
 * {@link io.github.hyshmily.zeta.worker.confidence.ConfidenceLevel#HIGH} so that
 * state transitions are gated purely by the hot/cold streak counters,
 * matching the pre-Bayesian behaviour.
 */
class ZetaBayesianSMTest {

  private static final ConfidenceEvaluator EVAL = new ConfidenceEvaluator(
    new BayesianConfidenceEstimator(BayesianConfidenceEstimator.PRIOR_MEAN, 2.0, 0.5)
  );

  private static final EvaluationContext CTX = new EvaluationContext(100L, 100L, 10L, Double.NaN, 0.0);

  private static final EvaluationContext COLD_CTX = new EvaluationContext(1L, 1L, 10L, Double.NaN, 0.0);

  private static final EvaluationContext MEDIUM_CTX = new EvaluationContext(20L, 20L, 10L, Double.NaN, 0.0);

  private static final EvaluationContext COLD_MEDIUM_CTX = new EvaluationContext(20L, 5L, 10L, Double.NaN, 0.0);

  private ZetaBayesianSM machine;

  @BeforeEach
  void setUp() {
    machine = new ZetaBayesianSM(3, 10, 4, EVAL, BayesianConfidenceEstimator.PRIOR_MEAN);
  }

  @AfterEach
  void tearDown() {
    TimeSource.setTimeOffsetForTest(0, 0);
  }

  @Test
  void defaultEvaluateOverload_shouldDelegate() {
    io.github.hyshmily.zeta.detection.ZetaBayesianSM iface = machine;
    assertThat(iface.evaluate("key", true, false, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(iface.evaluate("key", true, false, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(iface.evaluate("key", true, false, CTX).type()).isEqualTo(DecisionType.HOT);
  }

  @Test
  void coldToHot_requiresConfirmCountConsecutiveHotWindows() {
    assertThat(machine.evaluate("key", false, false, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine.evaluate("key", true, false, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine.evaluate("key", true, false, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine.evaluate("key", true, false, CTX).type()).isEqualTo(DecisionType.HOT);
  }

  @Test
  void hotToCool_requiresCoolCountConsecutiveColdWindows() {
    ZetaDecision last = null;
    for (int i = 0; i < 3; i++) {
      last = machine.evaluate("key", true, false, CTX);
    }
    assertThat(last.type()).isEqualTo(DecisionType.HOT);

    for (int i = 0; i < 5; i++) {
      assertThat(machine.evaluate("key", false, false, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
    }
    assertThat(machine.evaluate("key", false, false, COLD_CTX).type()).isEqualTo(DecisionType.NONE);

    for (int i = 0; i < 3; i++) {
      assertThat(machine.evaluate("key", false, false, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
    }
    assertThat(machine.evaluate("key", false, false, COLD_CTX).type()).isEqualTo(DecisionType.COOL);
  }

  @Test
  void preCooling_toHot_shouldReviveWithoutOscillation() {
    ZetaDecision last = null;
    for (int i = 0; i < 3; i++) {
      last = machine.evaluate("key", true, false, CTX);
    }
    assertThat(last.type()).isEqualTo(DecisionType.HOT);

    for (int i = 0; i < 6; i++) {
      machine.evaluate("key", false, false, CTX);
    }

    assertThat(machine.evaluate("key", true, false, CTX).type()).isEqualTo(DecisionType.NONE);
  }

  @Test
  void reset_shouldClearState() {
    ZetaDecision last = null;
    for (int i = 0; i < 3; i++) {
      last = machine.evaluate("key", true, false, CTX);
    }
    assertThat(last.type()).isEqualTo(DecisionType.HOT);
    machine.reset("key");
    assertThat(machine.evaluate("key", false, false, CTX).type()).isEqualTo(DecisionType.NONE);
  }

  @Test
  void mediumConfidence_shouldStillAllowCooling() {
    ZetaDecision last = null;
    for (int i = 0; i < 3; i++) {
      last = machine.evaluate("key", true, false, CTX);
    }
    assertThat(last.type()).isEqualTo(DecisionType.HOT);

    for (int i = 0; i < 9; i++) {
      machine.evaluate("key", false, false, COLD_CTX);
    }
    assertThat(machine.evaluate("key", false, false, COLD_MEDIUM_CTX).type()).isEqualTo(DecisionType.COOL);
  }

  @Test
  void evictStale_shouldRemoveOldKeys() {
    machine.evaluate("staleKey", true, false, CTX);
    TimeSource.setTimeOffsetForTest(0, 60_000);
    machine.evictStale(10, k -> {});
    assertThat(machine.getStateSnapshot("staleKey")).isNull();
  }

  /**
   * Tiered staleness (ADR-0061 companion): a COLD-state key is evicted at the
   * short threshold while a CONFIRMED_HOT key survives it — COLD keys carry
   * no broadcast obligation, HOT keys keep the long retention that guards the
   * COOL broadcast.
   */
  @Test
  void evictStale_shouldUseColdTierForColdStateAndHotTierForHotState() {
    // keyCold: evaluated once with a cold window → stays COLD (state created
    // only for a hot window, so first give it a hot window then a cold one).
    machine.evaluate("keyCold", true, false, CTX);
    machine.evaluate("keyCold", false, false, CTX); // CANDIDATE? no: hotStreak(1) < confirm(3) → still COLD, coolStreak=1
    // Force it to COLD-with-state: it already is COLD (promotion needs 3 hot
    // windows). keyHot: promoted to CONFIRMED_HOT.
    ZetaDecision last = null;
    for (int i = 0; i < 3; i++) {
      last = machine.evaluate("keyHot", true, false, CTX);
    }
    assertThat(last.type()).isEqualTo(DecisionType.HOT);
    assertThat(machine.getStateSnapshot("keyCold")).isNotNull();

    TimeSource.setTimeOffsetForTest(0, 60_000);
    java.util.List<String> cooled = new java.util.ArrayList<>();
    // coldStaleAfterMs = 1_000: the COLD key (60s idle) is evicted; the HOT
    // key (same idle time) needs staleAfterMs = 120_000 and survives.
    machine.evictStale(120_000, 1_000, cooled::add);

    assertThat(machine.getStateSnapshot("keyCold")).as("COLD key evicted at the cold tier").isNull();
    assertThat(machine.getStateSnapshot("keyHot")).as("HOT key kept at the hot tier").isNotNull();
    assertThat(cooled).as("no COOL for the early COLD eviction").isEmpty();
  }

  /**
   * The tiered eviction keeps the full-threshold safety net: a CONFIRMED_HOT
   * key evoked with coldStale == staleAfterMs (2-arg overload) is evicted and
   * its COOL broadcast obligation discharged exactly as before tiering.
   */
  @Test
  void evictStale_twoArgOverload_shouldPreserveLegacyBehavior() {
    ZetaDecision last = null;
    for (int i = 0; i < 3; i++) {
      last = machine.evaluate("hotKey", true, false, CTX);
    }
    assertThat(last.type()).isEqualTo(DecisionType.HOT);

    TimeSource.setTimeOffsetForTest(0, 60_000);
    java.util.List<String> cooled = new java.util.ArrayList<>();
    machine.evictStale(10, cooled::add);

    assertThat(cooled).containsExactly("hotKey");
    assertThat(machine.getStateSnapshot("hotKey")).isNull();
  }

  @Test
  void evictStale_shouldBroadcastCoolForEvictedHotKey() {
    ZetaDecision last = null;
    for (int i = 0; i < 3; i++) {
      last = machine.evaluate("hotKey", true, false, CTX);
    }
    assertThat(last.type()).isEqualTo(DecisionType.HOT);

    TimeSource.setTimeOffsetForTest(0, 60_000);
    java.util.List<String> cooled = new java.util.ArrayList<>();
    machine.evictStale(10, cooled::add);

    assertThat(cooled).containsExactly("hotKey");
    assertThat(machine.getStateSnapshot("hotKey")).isNull();
  }

  /**
   * Pins the phase-3 liveness re-verification contract: a key whose state was
   * re-created after phase-2 removal (here, re-evaluated from an earlier key's
   * COOL callback — the re-check runs under the per-key lock) must NOT receive
   * an eviction COOL, which would clobber its fresh HOT.
   */
  @Test
  void evictStale_shouldSkipCoolForKeyRecreatedAfterEviction() {
    // Exactly confirmCount(3) hot windows: the third emits HOT; a fourth
    // evaluation would hit the ADR-0024 rebroadcast debounce and return NONE.
    ZetaDecision lastA = null;
    ZetaDecision lastB = null;
    for (int i = 0; i < 3; i++) {
      lastA = machine.evaluate("keyA", true, false, CTX);
      lastB = machine.evaluate("keyB", true, false, CTX);
    }
    assertThat(lastA.type()).isEqualTo(DecisionType.HOT);
    assertThat(lastB.type()).isEqualTo(DecisionType.HOT);

    TimeSource.setTimeOffsetForTest(0, 60_000);
    java.util.List<String> cooled = new java.util.ArrayList<>();
    machine.evictStale(10, key -> {
      cooled.add(key);
      if ("keyA".equals(key)) {
        // Re-create keyB in the window between phase-2 removal and the
        // phase-3 liveness check (evaluations are serialized per key, so
        // this lands before keyB's own re-check in the phase-3 loop).
        machine.evaluate("keyB", true, false, CTX);
      }
    });

    assertThat(cooled).as("keyB was live again — its COOL must be skipped").containsExactly("keyA");
    assertThat(machine.getStateSnapshot("keyB")).isNotNull();
  }
}
