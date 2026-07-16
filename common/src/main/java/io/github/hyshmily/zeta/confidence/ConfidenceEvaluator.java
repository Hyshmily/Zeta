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
package io.github.hyshmily.zeta.confidence;

import io.github.hyshmily.zeta.Internal;
import lombok.RequiredArgsConstructor;

/**
 * Facade over {@link BayesianConfidenceEstimator} for the state machine.
 *
 * <p>This thin wrapper exists to keep the state machine
 * ({@link io.github.hyshmily.zeta.detection.impl.ZetaStateMachineImpl})
 * decoupled from the specific estimator implementation. The state machine
 * injects a {@code ConfidenceEvaluator} and calls {@link #evaluate} without
 * knowing whether the underlying engine is Bayesian, frequentist, or
 * heuristic — enabling future swap-outs without touching the state machine.
 *
 * <p>The three parameters (CMS count, threshold, CV) mirror the three
 * dimensions of evidence available at decision time:
 * <ol>
 *   <li><b>CMS count</b> — global frequency estimate from the
 *       HeavyKeeper sketch (multi-instance)</li>
 *   <li><b>Threshold</b> — the hot threshold the sliding window uses</li>
 *   <li><b>CV</b> — coefficient of variation for dynamic likelihood
 *       std adjustment (traffic stability signal)</li>
 * </ol>
 */
@Internal
@RequiredArgsConstructor
public class ConfidenceEvaluator {

  /** The underlying Bayesian estimator. */
  private final BayesianConfidenceEstimator estimator;

  /**
   * Evaluates the Bayesian posterior probability that this key is hot.
   *
   * @param cmsCount  HeavyKeeper frequency estimate for the key
   * @param threshold the hot threshold (raw count)
   * @param cv        coefficient of variation (may be {@code null})
   * @return a {@link ProbabilityResult} with the posterior probability
   *         and confidence level
   */
  public ProbabilityResult evaluate(long cmsCount, long threshold, Double cv) {
    return estimator.evaluate(cmsCount, threshold, cv);
  }
}
