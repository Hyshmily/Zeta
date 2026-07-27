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
package io.github.hyshmily.zeta.worker.detection;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.model.ZetaDecision;

/**
 * Strategy interface for evaluating per-key access reports and producing
 * HOT / COOL / NONE decisions on the Worker side.
 *
 * <p>The default implementation ({@link DefaultEvaluator}) supports a two-path
 * pipeline:
 * <ul>
 *   <li><b>Fast-lane path:</b> keys matching a configured fast-lane rule with
 *       sufficient window sum are promoted immediately — no Bayesian gating.</li>
 *   <li><b>Bayesian path:</b> sliding-window sum, CV history, EMA momentum,
 *       and a Bayesian confidence-gated state machine.</li>
 * </ul>
 *
 * <p>Implement this interface and expose it as a {@code @Bean} with
 * {@code @ConditionalOnMissingBean(Evaluator.class)} to replace the default
 * evaluation logic entirely. Custom implementations need not be aware of
 * sliding windows, CV, EMA, Bayesian confidence, or any other internal detail.
 */
@Internal
public interface Evaluator {

  /**
   * Evaluate a single key access report and return the action to take.
   *
   * @param key   the cache key being reported
   * @param count the access count in this report batch
   * @return a non-null {@link ZetaDecision} — {@code HOT}, {@code COOL},
   *         or {@code NONE}
   */
  ZetaDecision evaluate(String key, long count);

  /**
   * Evict stale per-key state. No-op by default — only relevant for
   * implementations that maintain per-key tracking (e.g. {@link DefaultEvaluator}
   * with its CV history and EMA counts).
   *
   * @param staleAfterMs maximum idle time in milliseconds before an entry
   *                     is considered stale and removed
   */
  default void evictStale(long staleAfterMs) {}
}
