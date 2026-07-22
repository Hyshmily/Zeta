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

import io.github.hyshmily.zeta.detection.ZetaBayesianSM;

/**
 * Three-tier confidence classification for Bayesian posterior probabilities.
 *
 * <p>These levels gate the state machine's HOT/COOL broadcast decisions:
 * <ul>
 *   <li>{@link #HIGH} (p &#x2265; 0.95) — emit broadcast immediately</li>
 *   <li>{@link #MEDIUM} (0.80 &#x2264; p &lt; 0.95) — defer to
 *       {@link ZetaBayesianSM.State#CANDIDATE_HOT}</li>
 *   <li>{@link #LOW} (p &lt; 0.80) — suppress broadcast, continue accumulating
 *       evidence</li>
 * </ul>
 *
 * <p>Threshold constants are defined in {@link ProbabilityResult}. The split
 * at 0.80/0.95 was chosen empirically to balance precision (avoiding false
 * broadcasts) against recall (not missing genuine hot keys that need
 * promotion).
 */
public enum ConfidenceLevel {
  HIGH,
  MEDIUM,
  LOW,
}
