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
package io.github.hyshmily.zeta.model;

import io.github.hyshmily.zeta.detection.ZetaBayesianSM;

/**
 * Aggregated observation data fed into Bayesian confidence evaluation.
 *
 * <p>Carries all the per-key metrics needed by the state machine to compute
 * a Bayesian posterior: the sliding-window sum (recent frequency, primary
 * observation), the HeavyKeeper sketch estimate (global frequency, secondary),
 * the hot threshold, and the coefficient of variation for dynamic likelihood
 * adjustment.
 *
 * <p>Created by the worker {@code KeyEvaluator}
 * before each call to
 * {@link ZetaBayesianSM#evaluate(String, boolean, EvaluationContext)}.
 *
 * @param cmsCount    HeavyKeeper frequency estimate for this key (global, cross-instance)
 * @param windowSum   total access count in the current sliding window (local);
 *                    used as the primary {@code observedCount} in the Bayesian model
 * @param threshold   the hot threshold (raw count) that the {@code windowSum} is
 *                    compared against for the binary is-hot flag
 * @param cv          coefficient of variation of the per-key sliding-window sums
 *                    over recent windows (may be {@code null} if not enough data)
 * @param logThreshold the hot threshold in log space (natural log of raw count)
 */
public record EvaluationContext(long cmsCount, long windowSum, long threshold, Double cv, double logThreshold) {
  public EvaluationContext(long cmsCount, long windowSum, long threshold, Double cv) {
    this(cmsCount, windowSum, threshold, cv, Math.log(Math.max(threshold, 1.0)));
  }
}
