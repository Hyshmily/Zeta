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

import io.github.hyshmily.zeta.detection.ZetaBayesianSM;
import io.github.hyshmily.zeta.model.EvaluationContext;
import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.model.ZetaDecision.DecisionType;
import io.github.hyshmily.zeta.worker.confidence.BayesianConfidenceEstimator;
import io.github.hyshmily.zeta.worker.confidence.ConfidenceEvaluator;
import org.junit.jupiter.api.Test;

/**
 * Minimal reproduction for P0-1: a steady {@code NONE} evaluation bumps {@code mutationSeq},
 * so the async drain-thread rollback of a failed HOT broadcast is skipped.
 *
 * <p>Timeline under {@code WorkerBroadcastBuffer}: T1 evaluates to HOT (snapshot seq N,
 * send queued), T2 evaluates the same key 50ms later (steady NONE, seq N+1), then the
 * drain thread fails the send and calls {@code rollbackToPreviousState(snapshot seq N)}
 * which is skipped because the live seq already advanced.
 */
class ZetaBayesianSMAsyncRollbackReproTest {

  private static final ConfidenceEvaluator EVAL = new BayesianConfidenceEstimator(
    BayesianConfidenceEstimator.PRIOR_MEAN, 2.0, 0.5, 0.95, 0.76);

  private static final EvaluationContext CTX = new EvaluationContext(100L, 100L, 10L, Double.NaN, 0.0);

  /**
   * Builds a state machine with rebroadcast disabled (interval MAX) so the steady
   * post-HOT evaluation returns NONE while still bumping the epoch on current code.
   *
   * @return a state machine with confirm=3
   */
  private static ZetaBayesianSM machineWithConfirm3() {
    return new io.github.hyshmily.zeta.worker.detection.impl.ZetaBayesianSM(
      3, 10, 4, EVAL, BayesianConfidenceEstimator.PRIOR_MEAN, Long.MAX_VALUE, 0L);
  }

  /**
   * Drives COLD to HOT, interleaves one steady NONE, then rolls back the HOT snapshot.
   *
   * <p>Expected (fixed): rollback restores the pre-HOT snapshot (COLD) so the next
   * evaluation can re-emit HOT immediately. Actual (buggy): rollback is skipped and
   * the key stays CONFIRMED_HOT with a stamped {@code lastBroadcastAt}.
   */
  @Test
  void steadyNone_shouldNotBlockHotRollback() {
    ZetaBayesianSM machine = machineWithConfirm3();

    assertThat(machine.evaluate("key", true, false, CTX).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine.evaluate("key", true, false, CTX).type()).isEqualTo(DecisionType.NONE);
    ZetaDecision hot = machine.evaluate("key", true, false, CTX);
    assertThat(hot.type()).isEqualTo(DecisionType.HOT);
    assertThat(hot.snapshot()).isNotNull();

    ZetaDecision steady = machine.evaluate("key", true, false, CTX);
    assertThat(steady.type()).isEqualTo(DecisionType.NONE);

    machine.rollbackToPreviousState("key", hot.snapshot());

    assertThat(machine.getStateSnapshot("key").currentState()).isEqualTo("COLD");
  }
}
