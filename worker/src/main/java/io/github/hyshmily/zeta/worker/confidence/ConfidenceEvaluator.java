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

/**
 * Strategy interface for the hotness-confidence model consumed by the
 * Worker state machine ({@code ZetaBayesianSM}): given the per-key evidence
 * available at decision time, produce a posterior probability and its
 * {@link ConfidenceLevel} classification.
 *
 * <p>The three parameters mirror the three dimensions of evidence available
 * at decision time:
 * <ol>
 *   <li><b>Observed count</b> — the raw count observed for the key in the
 *       current sliding window. Callers pass the (trend-scaled) window sum
 *       from {@link io.github.hyshmily.zeta.model.EvaluationContext#windowSum()};
 *       the momentum EMA in {@code cmsCount} feeds the adjusted threshold
 *       instead. The Worker never sees a HeavyKeeper sketch, which lives
 *       only in the App-side detector.</li>
 *   <li><b>Threshold</b> — the hot threshold the sliding window uses
 *       (log space)</li>
 *   <li><b>CV</b> — coefficient of variation for dynamic likelihood std
 *       adjustment (traffic stability signal); {@code Double.NaN}
 *       when no window history is available</li>
 * </ol>
 *
 * <h3>Replacing the default model</h3>
 * The default implementation, {@link BayesianConfidenceEstimator}, is a
 * Normal-Normal conjugate Bayesian model over log-frequency. Implement this
 * interface and register it as a Spring Bean to swap the confidence model
 * (frequentist z-score, EWMA scoring, ML-backed, ...) without touching the
 * state machine; the {@code hotKeyStateMachine} bean injects this interface.
 * Implementations must be thread-safe.
 *
 * <p>Historical note: this type used to be a concrete facade delegating to
 * {@link BayesianConfidenceEstimator} — the delegation carried no value, so
 * the interface is now the type itself and the estimator is its default
 * implementation (ADR-0070 convergence applied on the Worker side).
 */
@Internal
public interface ConfidenceEvaluator {

  /**
   * Evaluates the posterior hotness confidence for a key.
   *
   * @param cmsCount     observed count in the current sliding window
   * @param logThreshold hot threshold in log space (natural log of raw count)
   * @param cv           coefficient of variation ({@code Double.NaN} when no
   *                     window history is available)
   * @return a non-null {@link ProbabilityResult} with the posterior
   *         probability, confidence level, and distribution parameters
   */
  ProbabilityResult evaluate(long cmsCount, double logThreshold, double cv);

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
   */
  ProbabilityResult evaluateWithAccumulatedPrior(
    long observedCount,
    double logThreshold,
    double cv,
    double accumulatedMean,
    double accumulatedPrec
  );
}
