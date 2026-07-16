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

/**
 * Result of a Bayesian confidence evaluation for a single key.
 *
 * <p>Carries the posterior probability that the key's true log-frequency
 * exceeds the hot threshold, along with the full Normal-Normal conjugate
 * posterior parameters for transparency and debugging.
 *
 * @param probability  P(true frequency &gt; threshold) — the key output decision value
 * @param level        {@link ConfidenceLevel} derived from {@code probability} via {@link #classify}
 * @param posteriorMean  mean of the posterior log-frequency distribution
 * @param posteriorStd   standard deviation of the posterior log-frequency distribution
 * @param cv           coefficient of variation of the observed window sums (may be {@code null})
 */
public record ProbabilityResult(
  double probability,
  ConfidenceLevel level,
  double posteriorMean,
  double posteriorStd,
  Double cv
) {
  /** Probability above which a result is considered {@link ConfidenceLevel#HIGH}. */
  private static final double HIGH_THRESHOLD = 0.95;

  /** Probability above which a result is considered {@link ConfidenceLevel#MEDIUM}. */
  private static final double MEDIUM_THRESHOLD = 0.80;

  /**
   * Compact constructor that derives {@link #level} from {@code probability}.
   *
   * @param probability   the posterior probability value
   * @param posteriorMean posterior mean of log-frequency
   * @param posteriorStd  posterior standard deviation of log-frequency
   * @param cv            coefficient of variation (may be {@code null})
   */
  public ProbabilityResult(double probability, double posteriorMean, double posteriorStd, Double cv) {
    this(probability, classify(probability), posteriorMean, posteriorStd, cv);
  }

  /**
   * Maps a raw probability to a three-tier {@link ConfidenceLevel}.
   *
   * @param p the posterior probability
   * @return {@link ConfidenceLevel#HIGH} if p &#x2265; 0.95,
   *         {@link ConfidenceLevel#MEDIUM} if p &#x2265; 0.80,
   *         {@link ConfidenceLevel#LOW} otherwise
   */
  private static ConfidenceLevel classify(double p) {
    if (p >= HIGH_THRESHOLD) return ConfidenceLevel.HIGH;
    if (p >= MEDIUM_THRESHOLD) return ConfidenceLevel.MEDIUM;
    return ConfidenceLevel.LOW;
  }
}
