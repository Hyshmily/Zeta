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

/**
 * Pre-computed lookup table for the standard Normal CDF &#x03A6;(z).
 *
 * <p>The table covers the range z &#x2208; [-6, 6] with a step size of 0.001
 * (12,001 entries). Values outside this range are clamped: z &le; -6 returns
 * 0.0, z &ge; 6 returns 1.0. Within the range, linear interpolation is used
 * between adjacent entries.
 *
 * <p>Each entry is computed via the Abramowitz &amp; Stegun approximation
 * (26.2.17), which has a maximum absolute error of 7.5&times;10<sup>-8</sup>.
 * The combined pre-computation + interpolation error stays well below
 * 1&times;10<sup>-6</sup>, which is negligible for the Bayesian confidence
 * estimator's use (where probabilities are classified into three coarse
 * bins separated by 0.15).
 *
 * <p>The table is loaded once at class initialization and is thread-safe
 * for concurrent reads.
 */
@Internal
public final class NormalCdfTable {

  /** Lower bound of the z-range. */
  private static final double MIN_Z = -6.0;

  /** Upper bound of the z-range. */
  private static final double MAX_Z = 6.0;

  /** Step size between consecutive z-values. */
  private static final double STEP = 0.001;

  /** Number of table entries = (6 - (-6)) / 0.001 + 1. */
  private static final int SIZE = (int) ((MAX_Z - MIN_Z) / STEP) + 1;

  /** Pre-computed &#x03A6;(z) values. */
  private static final double[] TABLE = new double[SIZE];

  static {
    for (int i = 0; i < SIZE; i++) {
      double z = MIN_Z + i * STEP;
      TABLE[i] = abramowitzStegun(z);
    }
  }

  private NormalCdfTable() {}

  /**
   * Returns &#x03A6;(z) = P(Z &le; z) for Z ~ N(0, 1).
   *
   * <p>Clamps extreme z-values: z &le; -6 returns 0.0, z &ge; 6 returns 1.0.
   * Within bounds, performs linear interpolation on the pre-computed table.
   *
   * @param z the z-score
   * @return the cumulative probability
   */
  public static double phi(double z) {
    if (z <= MIN_Z) return 0.0;
    if (z >= MAX_Z) return 1.0;
    double idx = (z - MIN_Z) / STEP;
    int lo = (int) idx;
    int hi = Math.min(lo + 1, SIZE - 1);
    double frac = idx - lo;
    return TABLE[lo] + frac * (TABLE[hi] - TABLE[lo]);
  }

  /**
   * Abramowitz &amp; Stegun approximation 26.2.17 for the standard Normal CDF.
   *
   * <p>For z &ge; 0:
   * <pre>
   *   &#x03A6;(z) = 1 - &#x03C6;(z) &middot; (b<sub>1</sub>t + b<sub>2</sub>t<sup>2</sup> + b<sub>3</sub>t<sup>3</sup> + b<sub>4</sub>t<sup>4</sup> + b<sub>5</sub>t<sup>5</sup>)
   *   t = 1 / (1 + pz)
   * </pre>
   * where p = 0.2316419 and &#x03C6;(z) is the standard Normal PDF.
   * Maximum absolute error: 7.5&times;10<sup>-8</sup>.
   *
   * @param z the z-score (sign-normalized internally for z &lt; 0)
   * @return approximated cumulative probability
   */
  private static double abramowitzStegun(double z) {
    if (z < 0) return 1.0 - abramowitzStegun(-z);
    double t = 1.0 / (1.0 + 0.2316419 * z);
    double p =
      1.0 -
      0.3989422804014327 *
      Math.exp((-z * z) / 2.0) *
      t *
      (0.319381530 + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))));
    return Math.min(1.0, Math.max(0.0, p));
  }
}
