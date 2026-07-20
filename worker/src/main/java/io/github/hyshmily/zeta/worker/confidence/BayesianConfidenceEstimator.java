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
import lombok.Getter;

/**
 * Normal-Normal conjugate Bayesian estimator for log-frequency hotness.
 *
 * <p>Models the log of per-key access count as a Normal observation with
 * unknown mean and known variance. The prior represents our belief before
 * seeing any data; the likelihood is the observed log-count from the
 * sliding window. The posterior is another Normal whose mean is a
 * precision-weighted average of prior and observation.
 *
 * <h3>Model</h3>
 * <pre>
 *   Prior:      &#x03BC; ~ N(priorMean, priorStd<sup>2</sup>)
 *   Likelihood:  y | &#x03BC; ~ N(&#x03BC;, &#x03C3;<sup>2</sup>)
 *   Posterior:   &#x03BC; | y ~ N(&#x03BC;<sub>n</sub>, &#x03C3;<sub>n</sub><sup>2</sup>)
 * </pre>
 * where y = log(max(observedCount, 1)).
 *
 * <p>The posterior probability that the key is hot is:
 * <pre>
 *   P(&#x03BC; &gt; log(threshold)) = 1 - &#x03A6;((log(threshold) - &#x03BC;<sub>n</sub>) / &#x03C3;<sub>n</sub>)
 * </pre>
 *
 * <p>The likelihood standard deviation can be adjusted dynamically based
 * on the coefficient of variation (CV) of the window sums. A low CV
 * (stable traffic) reduces &#x03C3;, increasing confidence; a high CV
 * (bursty traffic) increases &#x03C3;, dampening confidence. This makes
 * the estimator robust to traffic pattern changes without manual tuning.
 *
 * <h3>Prior calibration</h3>
 * The default prior mean of ln(10) &asymp; 2.3026 was chosen so that a key
 * with 10 observed accesses in a window is neutral (posterior mean = prior
 * mean). A key needs consistently more than 10 accesses per window to
 * shift the posterior above the hot threshold. Configured via
 * {@code zeta.worker.bayesian.*} properties.
 */
@Internal
@SuppressWarnings("all")
public class BayesianConfidenceEstimator {

  /** Prior mean of the log-frequency distribution. */
  @Getter
  private final double priorMean;

  /** Prior standard deviation of the log-frequency distribution. */
  @Getter
  private final double priorStd;

  /** Base likelihood standard deviation. Adjusted by CV when available. */
  @Getter
  private final double likelihoodStd;

  /**
   * Constructs the estimator with the given Normal-Normal conjugate parameters.
   *
   * @param priorMean     prior mean (log scale)
   * @param priorStd      prior standard deviation (log scale)
   * @param likelihoodStd base likelihood standard deviation (log scale);
   *                      adjusted dynamically when CV is provided
   */
  public BayesianConfidenceEstimator(double priorMean, double priorStd, double likelihoodStd) {
    this.priorMean = priorMean;
    this.priorStd = priorStd;
    this.likelihoodStd = likelihoodStd;
  }

  /**
   * Computes the posterior probability that the key's true log-frequency
   * exceeds the hot threshold.
   *
   * <p>If {@code cv} is non-null, the likelihood standard deviation is
   * scaled via {@link #adjustLikelihoodStd} to account for traffic
   * variability. A null CV uses the base likelihoodStd directly.
   *
   * @param observedCount the raw count observed for this key in the
   *                      current sliding window
   * @param logThreshold  the hot threshold in log space (natural log of raw count)
   * @param cv            coefficient of variation of the per-key
   *                      sliding-window sums (may be {@code null})
   * @return a {@link ProbabilityResult} with the posterior probability,
   *         confidence level, and distribution parameters
   */
  public ProbabilityResult evaluate(long observedCount, double logThreshold, Double cv) {
    double y = Math.log(Math.max(observedCount, 1.0));
    double sigma = (cv != null) ? adjustLikelihoodStd(likelihoodStd, cv) : likelihoodStd;

    double priorPrecision = 1.0 / (priorStd * priorStd);
    double likelihoodPrecision = 1.0 / (sigma * sigma);
    double posteriorPrecision = priorPrecision + likelihoodPrecision;

    double posteriorMean = (priorMean * priorPrecision + y * likelihoodPrecision) / posteriorPrecision;
    double posteriorStd = Math.sqrt(1.0 / posteriorPrecision);

    double z = (logThreshold - posteriorMean) / posteriorStd;
    double hotProbability = 1.0 - NormalCdfTable.phi(z);

    return new ProbabilityResult(hotProbability, posteriorMean, posteriorStd, cv);
  }

  private static double adjustLikelihoodStd(double baseStd, double cv) {
    if (cv < 0.2) {
      return baseStd * (0.5 + (cv / 0.2) * 0.5);
    }
    if (cv > 0.5) {
      return baseStd * (1.0 + Math.min((cv - 0.5) / 0.5, 2.0));
    }
    return baseStd;
  }
}
