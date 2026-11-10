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
package io.github.hyshmily.zeta.sync.worker;

import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;

/**
 * Callback hook for Worker HOT/COOL decision outcomes.
 *
 * <p>Implement this interface and expose it as a Spring {@code @Bean} to observe or react to
 * decision outcomes without changing the promotion policy. Multiple hooks are supported and
 * invoked in iteration order.
 *
 * <p>The framework fires exactly one terminal callback per decision it routes through the
 * policy and the guarded context, or through the COOL-run reduction:
 * {@code afterHotPromotion}/{@code onHotSkipped} for HOT and
 * {@code afterCoolDowngrade}/{@code onCoolSkipped} for COOL. A policy must not fire them.
 * The framework-only inline COOL fallback reports none: the AMQP consumer thread must not
 * wait on user code.
 *
 * <p>All methods are {@code default} no-ops — implement only the ones you need. Exceptions
 * thrown by a hook are caught and logged; they never propagate to the caller or interrupt
 * other hooks.
 */
public interface WorkerDecisionHook {
  /**
   * Called after a key has been successfully promoted to
   * {@link io.github.hyshmily.zeta.model.KeyState#HOT}.
   *
   * @param decision the HOT decision that triggered the promotion
   * @param entry    the newly written entry (never null)
   */
  default void afterHotPromotion(WorkerDecision decision, CacheEntry entry) {}

  /**
   * Called after a key has been downgraded to
   * {@link io.github.hyshmily.zeta.model.KeyState#COOL}.
   *
   * @param decision the COOL decision that triggered the downgrade
   * @param entry    the updated entry (never null)
   */
  default void afterCoolDowngrade(WorkerDecision decision, CacheEntry entry) {}

  /**
   * Called when a HOT decision was skipped for the given reason.
   *
   * @param decision the HOT decision that would have triggered the promotion
   * @param reason   why the promotion was skipped
   */
  default void onHotSkipped(WorkerDecision decision, HotSkipReason reason) {}

  /**
   * Called when a COOL decision was skipped — no entry exists in L1, a newer/equal decision
   * is already stamped, the COOL is a stale cross-Worker ghost, or the entry is hard-expired
   * and must not be resurrected.
   *
   * @param decision the COOL decision that would have triggered the downgrade
   */
  default void onCoolSkipped(WorkerDecision decision) {}
}
