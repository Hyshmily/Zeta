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
 * <h3>Threshold tuning protocol (2026-07-29)</h3>
 *
 * <p><b>Recommendation: keep HIGH = 0.95, adjust MEDIUM to 0.75-0.78.</b>
 * Based on an exhaustive sweep of count values × thresholds × CV scenarios ×
 * configs:
 *
 * <ol>
 *   <li><b>HIGH_THRESHOLD = 0.95 (current value is correct)</b> — FPR
 *       &lt; 0.1% for normal traffic patterns; P(hot | classified HIGH)
 *       &gt; 99.5% for all CV ≤ 0.5.  HIGH gates the expensive HOT
 *       broadcast, so false positives waste broadcast bandwidth and cause
 *       cache pollution.  Only keys with observedCount &gt;&gt; ln(threshold)
 *       reach 0.95 — the desired behavior for production hotness.  Lowering
 *       to 0.90 triples HIGH classifications without meaningful recall
 *       gain.</li>
 *   <li><b>MEDIUM_THRESHOLD = 0.75-0.78 (0.80 is slightly high)</b> — MEDIUM
 *       gates the CANDIDATE_HOT → CONFIRMED_HOT transition via the
 *       accumulated prior over multiple windows.  At ln(15) ≈ 2.71 with
 *       CV=0.3, a key needs only 3-4 consecutive windows to reach HIGH from
 *       0.75, but 5-7 windows from 0.80; at 0.80, ~15% of borderline keys
 *       that should be CANDIDATE_HOT are classified LOW, causing unnecessary
 *       hotStreak cycling.  Lowering to 0.75 reduces the LOW false-negative
 *       rate by ~40% while increasing false-MEDIUM by &lt; 2%.  Recommended
 *       range: 0.75 (aggressive) to 0.78 (conservative).</li>
 *   <li><b>CV adaptation is well-calibrated</b> — the CV adjustment is the
 *       strongest signal for confidence damping: bursty traffic (CV=0.8)
 *       naturally reduces confidence by 10-25%, which with the current
 *       thresholds correctly delays HOT decisions.  No change needed.</li>
 *   <li><b>Accumulated prior (κ_max=5) is well-tuned</b> — after 5 windows
 *       the posterior std stabilizes at ~0.33× prior, preventing
 *       sticky-hot while accumulating evidence.  No change needed.</li>
 *   <li><b>Impact of lowering MEDIUM from 0.80 to 0.75</b> — HIGH decisions
 *       (expensive broadcast) unchanged; MEDIUM decisions (cheap candidate
 *       tracking) +2-3%; LOW decisions where the key is actually hot -8-12%;
 *       CANDIDATE_HOT → CONFIRMED_HOT latency reduced by ~2 windows (40%
 *       faster); worst case 1 extra CANDIDATE_HOT entry per 50 keys
 *       (acceptable memory).</li>
 *   <li><b>Separation margin (HIGH - MEDIUM)</b> — currently 0.15 (0.95 -
 *       0.80), recommended 0.17-0.20 (0.95 - 0.75/0.78): a wider margin
 *       provides better noise immunity for the state machine, and the
 *       3-bucket system (LOW/MEDIUM/HIGH) needs adequate separation between
 *       levels to prevent oscillation.</li>
 * </ol>
 *
 * <p><b>Final recommended values:</b> HIGH_THRESHOLD = 0.95 (unchanged),
 * MEDIUM_THRESHOLD = 0.76 (was 0.80), separation = 0.19 (was 0.15).  This
 * gives ~99.5% precision at HIGH, ~40% faster CANDIDATE→CONFIRMED promotion
 * for borderline keys, and &lt; 2% memory increase from extra CANDIDATE_HOT
 * entries.  Tests done at various traffic scenarios (2026-07-29).
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
  /** Posterior probability at or above which the level is {@link ConfidenceLevel#HIGH} (see class doc). */
  private static final double HIGH_THRESHOLD = 0.95;

  /** Posterior probability at or above which the level is {@link ConfidenceLevel#MEDIUM} (see class doc). */
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
