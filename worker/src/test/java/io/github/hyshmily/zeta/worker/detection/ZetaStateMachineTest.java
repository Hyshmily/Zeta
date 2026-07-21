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
import io.github.hyshmily.zeta.worker.confidence.BayesianConfidenceEstimator;
import io.github.hyshmily.zeta.worker.confidence.ConfidenceEvaluator;
import io.github.hyshmily.zeta.worker.detection.impl.ZetaStateMachineImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link io.github.hyshmily.zeta.detection.ZetaStateMachine} covering cold-to-hot, hot-to-cool, pre-cooling revive, reset, and eviction
 * transitions.
 *
 * <p>All tests use a {@link ConfidenceEvaluator} that always yields
 * {@link io.github.hyshmily.zeta.worker.confidence.ConfidenceLevel#HIGH} so that
 * state transitions are gated purely by the hot/cold streak counters,
 * matching the pre-Bayesian behaviour.
 */
class ZetaStateMachineTest {

  private static final ConfidenceEvaluator EVAL = new ConfidenceEvaluator(
    new BayesianConfidenceEstimator(2.3026, 2.0, 0.5)
  );

  private static final EvaluationContext CTX = new EvaluationContext(100L, 100L, 10L, null);

  private static final EvaluationContext COLD_CTX = new EvaluationContext(1L, 1L, 10L, null);

  private static final EvaluationContext MEDIUM_CTX = new EvaluationContext(20L, 20L, 10L, null);

  private static final EvaluationContext COLD_MEDIUM_CTX = new EvaluationContext(20L, 5L, 10L, null);

  private ZetaStateMachineImpl machine;

  @BeforeEach
  void setUp() {
    machine = new ZetaStateMachineImpl(3, 10, 4, EVAL);
  }

  @Test
  void coldToHot_requiresConfirmCountConsecutiveHotWindows() {
    assertThat(machine.evaluate("key", false, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.HOT);
  }

  @Test
  void hotToCool_requiresCoolCountConsecutiveColdWindows() {
    ZetaDecision last = null;
    for (int i = 0; i < 3; i++) {
      last = machine.evaluate("key", true, CTX);
    }
    assertThat(last.type()).isEqualTo(DecisionType.HOT);

    for (int i = 0; i < 5; i++) {
      assertThat(machine.evaluate("key", false, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
    }
    assertThat(machine.evaluate("key", false, COLD_CTX).type()).isEqualTo(DecisionType.NONE);

    for (int i = 0; i < 3; i++) {
      assertThat(machine.evaluate("key", false, COLD_CTX).type()).isEqualTo(DecisionType.NONE);
    }
    assertThat(machine.evaluate("key", false, COLD_CTX).type()).isEqualTo(DecisionType.COOL);
  }

  @Test
  void preCooling_toHot_shouldReviveWithoutOscillation() {
    ZetaDecision last = null;
    for (int i = 0; i < 3; i++) {
      last = machine.evaluate("key", true, CTX);
    }
    assertThat(last.type()).isEqualTo(DecisionType.HOT);

    for (int i = 0; i < 6; i++) {
      machine.evaluate("key", false, CTX);
    }

    assertThat(machine.evaluate("key", true, CTX).type()).isEqualTo(DecisionType.NONE);
  }

  @Test
  void reset_shouldClearState() {
    ZetaDecision last = null;
    for (int i = 0; i < 3; i++) {
      last = machine.evaluate("key", true, CTX);
    }
    assertThat(last.type()).isEqualTo(DecisionType.HOT);
    machine.reset("key");
    assertThat(machine.evaluate("key", false, CTX).type()).isEqualTo(DecisionType.NONE);
  }

  @Test
  void mediumConfidence_shouldStillAllowCooling() {
    ZetaDecision last = null;
    for (int i = 0; i < 3; i++) {
      last = machine.evaluate("key", true, CTX);
    }
    assertThat(last.type()).isEqualTo(DecisionType.HOT);

    for (int i = 0; i < 9; i++) {
      machine.evaluate("key", false, COLD_CTX);
    }
    assertThat(machine.evaluate("key", false, COLD_MEDIUM_CTX).type()).isEqualTo(DecisionType.COOL);
  }

  @Test
  void evictStale_shouldRemoveOldKeys() throws InterruptedException {
    machine.evaluate("staleKey", true, CTX);
    Thread.sleep(50);
    machine.evictStale(10, k -> {});
    assertThat(machine.evaluate("staleKey", false, CTX).type()).isEqualTo(DecisionType.NONE);
  }
}
