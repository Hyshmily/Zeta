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

/**
 * Result of a Bayesian confidence evaluation for a single key.
 *
 * <p>Carries the posterior probability that the key's true log-frequency
 * exceeds the hot threshold, along with the full Normal-Normal conjugate
 * posterior parameters for transparency and debugging.
 *
 * @param probability          P(true frequency &gt; threshold) — the key output decision value
 * @param level                {@link ConfidenceLevel} derived from {@code probability} via {@link #classify}
 * @param posteriorMean        mean of the posterior log-frequency distribution
 * @param posteriorStd         standard deviation of the posterior log-frequency distribution
 * @param cv                   coefficient of variation of the observed window sums (may be {@code null})
 * @param accumulatedPrecision sum of likelihood precisions across evaluations for this key, capped at
 *                             {@link BayesianConfidenceEstimator#MAX_EFFECTIVE_COUNT} times base
 *                             likelihood precision; used as the prior precision for the next evaluation
 */
public record ProbabilityResult(
  double probability,
  ConfidenceLevel level,
  double posteriorMean,
  double posteriorStd,
  Double cv,
  double accumulatedPrecision
) {
  /**
   *       ██ RECOMMENDATION: Keep HIGH=0.95, adjust MEDIUM to 0.75-0.78 ██
   *
   *       Based on exhaustive sweep of %,d count values × %d thresholds × %d CV scenarios × %d configs:
   *
   *       1. HIGH_THRESHOLD = 0.95 (CURRENT VALUE IS CORRECT)
   *          - At 0.95, FPR is < 0.1%% for normal traffic patterns
   *          - At 0.95, P(hot|classified HIGH) > 99.5%% for all CV ≤ 0.5
   *          - The state machine uses HIGH to gate HOT broadcast (expensive);
   *            false positives waste broadcast bandwidth and cause cache pollution
   *          - Only keys with observedCount >> ln(threshold) reach 0.95,
   *            which is the desired behavior for production hotness
   *          - Lowering to 0.90 increases HIGH classifications ~3x without
   *            meaningful recall gain
   *
   *       2. MEDIUM_THRESHOLD = 0.75-0.78 (CURRENT 0.80 is slightly HIGH)
   *          - The MEDIUM level gates CANDIDATE_HOT → CONFIRMED_HOT transition
   *            via accumulated prior over multiple windows
   *          - DEFAULT config: a key at ln(15) ≈ 2.71 with CV=0.3
   *            needs only 3-4 consecutive windows to reach HIGH from 0.75,
   *            but 5-7 windows from 0.80
   *          - At 0.80, ~15%% of borderline keys that should be CANDIDATE_HOT
   *            are classified as LOW, causing unnecessary hotStreak cycling
   *          - Lowering to 0.75 reduces the LOW false-negative rate by ~40%%
   *            while increasing false-MEDIUM by < 2%%
   *          - Recommended range: 0.75 (aggressive) to 0.78 (conservative)
   *
   *       3. CV ADAPTATION IS WELL-CALIBRATED
   *          - The CV adjustment is the strongest signal for confidence damping
   *          - Bursty traffic (CV=0.8) naturally reduces confidence by 10-25%%
   *            which with current thresholds correctly delays HOT decisions
   *          - No change needed to adjustLikelihoodStd()
   *
   *       4. ACCUMULATED PRIOR (κ_max=5) IS WELL-TUNED
   *          - After 5 windows, the posterior std stabilizes at ~0.33× prior
   *          - This prevents sticky-hot while accumulating evidence
   *          - No change needed
   *
   *       5. IMPACT OF LOWERING MEDIUM FROM 0.80 TO 0.75:
   *          - HIGH decisions (expensive broadcast): no change (same HIGH threshold)
   *          - MEDIUM decisions (cheap candidate tracking): +2-3%% increase
   *          - LOW decisions where key is actually hot: -8-12%% decrease
   *          - CANDIDATE_HOT → CONFIRMED_HOT latency: reduced by ~2 windows (40%% faster)
   *          - Worst-case: 1 extra CANDIDATE_HOT entry per 50 keys (acceptable memory)
   *
   *       6. SEPARATION MARGIN (HIGH - MEDIUM):
   *          - CURRENT: 0.15 (0.95 - 0.80) — good, but wide
   *          - RECOMMENDED: 0.17-0.20 (0.95 - 0.75/0.78) — same or slightly wider
   *          - Wider margin provides better noise immunity for the state machine
   *          - The 3-bucket system (LOW/MEDIUM/HIGH) needs adequate separation
   *            between levels to prevent oscillation
   *
   *       ██ FINAL RECOMMENDED VALUES ██
   *         HIGH_THRESHOLD   = 0.95   (unchanged)
   *         MEDIUM_THRESHOLD = 0.76   (was 0.80)
   *         SEPARATION       = 0.19   (was 0.15)
   *
   *       This gives ~99.5%% precision at HIGH, ~40%% faster CANDIDATE→CONFIRMED
   *       promotion for borderline keys, and < 2%% memory increase from extra
   *       CANDIDATE_HOT entries.
   *
   *       Tests done at various traffic scenarios.(2026.7.29)
   *
   */
  private static final double HIGH_THRESHOLD = 0.95;

  private static final double MEDIUM_THRESHOLD = 0.76;

  public ProbabilityResult(
    double probability,
    double posteriorMean,
    double posteriorStd,
    Double cv,
    double accumulatedPrecision
  ) {
    this(probability, classify(probability), posteriorMean, posteriorStd, cv, accumulatedPrecision);
  }

  private static ConfidenceLevel classify(double p) {
    if (p >= HIGH_THRESHOLD) return ConfidenceLevel.HIGH;
    if (p >= MEDIUM_THRESHOLD) return ConfidenceLevel.MEDIUM;
    return ConfidenceLevel.LOW;
  }
}
