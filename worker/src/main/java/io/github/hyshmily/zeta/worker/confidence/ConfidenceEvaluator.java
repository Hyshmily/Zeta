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
package io.github.hyshmily.zeta.worker.confidence;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.detection.ZetaBayesianSM;
import lombok.RequiredArgsConstructor;

/**
 * Facade over {@link BayesianConfidenceEstimator} for the state machine.
 *
 * <p>This thin wrapper exists to keep the state machine
 * ({@link ZetaBayesianSM})
 * decoupled from the specific estimator implementation.
 *
 * <p>The three parameters (observed count, threshold, CV) mirror the three
 * dimensions of evidence available at decision time:
 * <ol>
 *   <li><b>Observed count</b> — the raw count observed for the key in the
 *       current sliding window. Callers pass the (trend-scaled) window sum
 *       from {@link io.github.hyshmily.zeta.model.EvaluationContext#windowSum()};
 *       the momentum EMA in {@code cmsCount} feeds the adjusted threshold
 *       instead. The Worker never sees a HeavyKeeper sketch, which lives
 *       only in the App-side detector.</li>
 *   <li><b>Threshold</b> — the hot threshold the sliding window uses</li>
 *   <li><b>CV</b> — coefficient of variation for dynamic likelihood
 *       std adjustment (traffic stability signal); {@code Double.NaN}
 *       when no window history is available</li>
 * </ol>
 */
@Internal
@RequiredArgsConstructor
public class ConfidenceEvaluator {

  private final BayesianConfidenceEstimator estimator;

  public ProbabilityResult evaluate(long cmsCount, double logThreshold, double cv) {
    return estimator.evaluate(cmsCount, logThreshold, cv);
  }

  /**
   * Evaluates with per-key accumulated prior.
   *
   * @param observedCount    current window raw count
   * @param logThreshold     hot threshold in log space
   * @param cv               coefficient of variation ({@code Double.NaN} when
   *                         no window history is available)
   * @param accumulatedMean  key's posterior mean from previous evaluation
   * @param accumulatedPrec  key's accumulated precision from previous evaluations
   * @return updated {@link ProbabilityResult} with new accumulatedPrecision
   * @see BayesianConfidenceEstimator#evaluateWithAccumulatedPrior
   */
  public ProbabilityResult evaluateWithAccumulatedPrior(
    long observedCount,
    double logThreshold,
    double cv,
    double accumulatedMean,
    double accumulatedPrec
  ) {
    return estimator.evaluateWithAccumulatedPrior(observedCount, logThreshold, cv, accumulatedMean, accumulatedPrec);
  }
}
