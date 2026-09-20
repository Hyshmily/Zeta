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
import org.springframework.util.Assert;

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
 * A non-finite CV (including {@code Double.NaN}, the "no window history"
 * sentinel) falls back to the base likelihoodStd.
 *
 * <h3>Prior calibration</h3>
 * The default prior mean of ln(10) &asymp; 2.3026 was chosen so that a key
 * with 10 observed accesses in a window is neutral (posterior mean = prior
 * mean). A key needs consistently more than 10 accesses per window to
 * shift the posterior above the hot threshold. Configured via
 * {@code zeta.worker.bayesian.*} properties.
 *
 * <h3>Confidence classification</h3>
 * The posterior probability is classified into the three
 * {@link ConfidenceLevel} tiers. The split thresholds are configurable
 * ({@code zeta.worker.bayesian.high-confidence-threshold} /
 * {@code medium-confidence-threshold}); the tuning protocol behind the
 * defaults is documented on {@link ProbabilityResult}.
 *
 * <p>This class is the default {@link ConfidenceEvaluator} strategy — register
 * a different {@code ConfidenceEvaluator} Bean to swap the confidence model,
 * or override this Bean to retune the Normal-Normal conjugate parameters.
 */
@Internal
public class BayesianConfidenceEstimator implements ConfidenceEvaluator {

  /**
   * Default prior mean: ln(10) ≈ 2.302585. A key with 10 observed accesses in a
   * window is neutral (posterior mean = prior mean); consistently more than 10
   * accesses per window are needed to shift the posterior above the hot threshold.
   */
  public static final double PRIOR_MEAN = Math.log(10);

  /**
   * Maximum effective sample size per key (κ_max). Once a key's accumulated
   * precision reaches this many base-likelihood-equivalent observations,
   * further observations contribute only to the posterior mean while the
   * precision (and therefore posterior std) stabilises.
   */
  static final int MAX_EFFECTIVE_COUNT = 5;

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
   * Posterior probability at or above which a decision is {@link ConfidenceLevel#HIGH}
   * (gates the expensive HOT broadcast). Configured via
   * {@code zeta.worker.bayesian.high-confidence-threshold}.
   */
  @Getter
  private final double highConfidenceThreshold;

  /**
   * Posterior probability at or above which a decision is {@link ConfidenceLevel#MEDIUM}
   * (gates CANDIDATE_HOT tracking). Configured via
   * {@code zeta.worker.bayesian.medium-confidence-threshold}.
   */
  @Getter
  private final double mediumConfidenceThreshold;

  /** Prior precision = 1 / priorStd² (invariant — precomputed once). */
  private final double priorPrecision;

  /** Maximum accumulated precision = MAX_EFFECTIVE_COUNT × baseLikelihoodPrecision. */
  private final double maxAccumulatedPrecision;

  /**
   * Constructs the estimator with the default confidence-classification
   * thresholds documented on {@link ProbabilityResult}.
   *
   * @param priorMean     prior mean (log scale)
   * @param priorStd      prior standard deviation (log scale)
   * @param likelihoodStd base likelihood standard deviation (log scale);
   *                      adjusted dynamically when a finite CV is provided
   */
  public BayesianConfidenceEstimator(double priorMean, double priorStd, double likelihoodStd) {
    this(priorMean, priorStd, likelihoodStd, ProbabilityResult.HIGH_THRESHOLD, ProbabilityResult.MEDIUM_THRESHOLD);
  }

  /**
   * Constructs the estimator with the given Normal-Normal conjugate parameters
   * and confidence-classification thresholds.
   *
   * @param priorMean                prior mean (log scale)
   * @param priorStd                 prior standard deviation (log scale)
   * @param likelihoodStd            base likelihood standard deviation (log scale);
   *                                 adjusted dynamically when a finite CV is provided
   * @param highConfidenceThreshold  posterior probability at or above which a decision is
   *                                 {@link ConfidenceLevel#HIGH}; must be in (0, 1) and
   *                                 strictly above {@code mediumConfidenceThreshold}
   * @param mediumConfidenceThreshold posterior probability at or above which a decision is
   *                                 {@link ConfidenceLevel#MEDIUM}; must be in (0, 1)
   */
  public BayesianConfidenceEstimator(
    double priorMean,
    double priorStd,
    double likelihoodStd,
    double highConfidenceThreshold,
    double mediumConfidenceThreshold
  ) {
    Assert.isTrue(priorStd > 0, "priorStd must be positive, got " + priorStd);
    Assert.isTrue(likelihoodStd > 0, "likelihoodStd must be positive, got " + likelihoodStd);
    Assert.isTrue(
      highConfidenceThreshold > 0.0 && highConfidenceThreshold < 1.0,
      "highConfidenceThreshold must be in (0, 1), got " + highConfidenceThreshold
    );
    Assert.isTrue(
      mediumConfidenceThreshold > 0.0 && mediumConfidenceThreshold < 1.0,
      "mediumConfidenceThreshold must be in (0, 1), got " + mediumConfidenceThreshold
    );
    Assert.isTrue(
      mediumConfidenceThreshold < highConfidenceThreshold,
      "mediumConfidenceThreshold (" +
        mediumConfidenceThreshold +
        ") must be strictly below highConfidenceThreshold (" +
        highConfidenceThreshold +
        ")"
    );
    this.priorMean = priorMean;
    this.priorStd = priorStd;
    this.likelihoodStd = likelihoodStd;
    this.highConfidenceThreshold = highConfidenceThreshold;
    this.mediumConfidenceThreshold = mediumConfidenceThreshold;
    this.priorPrecision = 1.0 / (priorStd * priorStd);
    this.maxAccumulatedPrecision = MAX_EFFECTIVE_COUNT * (1.0 / (likelihoodStd * likelihoodStd));
  }

  /**
   * Computes the posterior probability that the key's true log-frequency
   * exceeds the hot threshold, using the fixed global prior (no accumulated
   * per-key evidence).
   *
   * <p>If {@code cv} is finite, the likelihood standard deviation is scaled
   * via {@link #adjustLikelihoodStd} to account for traffic variability.
   * A non-finite CV (including the {@code Double.NaN} "no history" sentinel)
   * uses the base likelihoodStd directly.
   *
   * @param observedCount the raw count observed for this key in the
   *                      current sliding window
   * @param logThreshold  the hot threshold in log space (natural log of raw count)
   * @param cv            coefficient of variation of the per-key
   *                      sliding-window sums ({@code Double.NaN} when no
   *                      window history is available)
   * @return a {@link ProbabilityResult} with the posterior probability,
   *         confidence level, and distribution parameters
   */
  public ProbabilityResult evaluate(long observedCount, double logThreshold, double cv) {
    return evaluateWithAccumulatedPrior(observedCount, logThreshold, cv, priorMean, 0.0);
  }

  /**
   * Computes posterior probability with per-key accumulated prior.
   *
   * <p>Unlike {@link #evaluate(long, double, double)} which always uses the
   * fixed global prior, this overload accepts a key-specific accumulated
   * posterior from previous evaluations. The estimator combines it with the
   * current observation via the Normal-Normal conjugate update, caps the
   * accumulated precision at {@code MAX_EFFECTIVE_COUNT × baseLikelihoodPrecision},
   * and returns the updated precision so the caller can store it for the next
   * evaluation.
   *
   * <p>This enables per-key Bayesian evidence accumulation: a key observed
   * consistently over many windows gains higher posterior precision, while
   * the cap (κ_max = {@value #MAX_EFFECTIVE_COUNT}) prevents the model from
   * becoming too sticky to detect regime changes.
   *
   * @param observedCount    the raw count observed for this key in the
   *                         current sliding window
   * @param logThreshold     the hot threshold in log space (natural log of raw count)
   * @param cv               coefficient of variation of the per-key
   *                         sliding-window sums ({@code Double.NaN} when no
   *                         window history is available)
   * @param accumulatedMean  the key's accumulated posterior mean from
   *                         previous evaluations (initially {@link #priorMean})
   * @param accumulatedPrec  the key's accumulated precision from previous
   *                         evaluations (initially {@code 0.0})
   * @return a {@link ProbabilityResult} with the updated posterior
   *         parameters and the new accumulated precision
   */
  public ProbabilityResult evaluateWithAccumulatedPrior(
    long observedCount,
    double logThreshold,
    double cv,
    double accumulatedMean,
    double accumulatedPrec
  ) {
    double y = Math.log(Math.max(observedCount, 1.0));

    double sigma = Double.isFinite(cv) ? adjustLikelihoodStd(likelihoodStd, cv) : likelihoodStd;

    double likelihoodPrecision = 1.0 / (sigma * sigma);

    double totalPriorPrecision = priorPrecision + accumulatedPrec;
    double posteriorPrecision = totalPriorPrecision + likelihoodPrecision;
    double posteriorMean = (totalPriorPrecision * accumulatedMean + likelihoodPrecision * y) / posteriorPrecision;
    double posteriorStd = Math.sqrt(1.0 / posteriorPrecision);

    double newAccPrec = Math.min(accumulatedPrec + likelihoodPrecision, maxAccumulatedPrecision);
    double z = (logThreshold - posteriorMean) / posteriorStd;
    double hotProbability = 1.0 - NormalCdfTable.phi(z);
    return new ProbabilityResult(
      hotProbability,
      ProbabilityResult.classify(hotProbability, highConfidenceThreshold, mediumConfidenceThreshold),
      posteriorMean,
      posteriorStd,
      newAccPrec
    );
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
