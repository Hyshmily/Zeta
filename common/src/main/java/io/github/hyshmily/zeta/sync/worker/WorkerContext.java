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

import io.github.hyshmily.zeta.cache.EntryView;
import org.jspecify.annotations.Nullable;

/**
 * The framework-owned guarded write path behind {@link WorkerPolicy}: version comparison,
 * invalidation tombstones, ghost-COOL ownership, TTL arithmetic and terminal hook dispatch.
 *
 * <p>Every HOT prepass is settled by {@link #settleHot}; {@link #resolveHot} is a lock-free
 * pre-check plus value resolution, while the atomic guard runs again inside
 * {@link #settleHot}'s compute. The listener owns both calls, so a policy cannot apply a
 * value that lost a race or leave a decision without its one terminal hook.
 */
interface WorkerContext {
  /**
   * Pre-check the decision and resolve the value it would promote.
   *
   * @param decision the HOT decision to prepare; must not be null and must be HOT
   * @return the action to settle; never {@code null}
   */
  HotAction resolveHot(WorkerDecision decision);

  /**
   * Apply a prepared HOT action atomically and report its terminal hook.
   *
   * @param decision the HOT decision being settled; must not be null and must be HOT
   * @param action   the action to settle; must not be null
   */
  void settleHot(WorkerDecision decision, HotAction action);

  /**
   * Snapshot the resident for a policy decision.
   *
   * @param cacheKey the key to inspect; must not be null
   * @return the read-only snapshot, or {@code null} when the key is absent
   */
  @Nullable
  EntryView view(String cacheKey);

  /**
   * Pre-check a COOL decision before the policy sees it: a missing/expired entry, a stale decision
   * and a ghost COOL are decided here, so the policy is never called for an invalid event and its
   * {@link CoolAction.Skip} cannot mask the real reason.
   *
   * @param decision the COOL decision to check; must not be null and must be COOL
   * @return the skip reason, or {@code null} when the policy may decide
   */
  @Nullable
  CoolSkipReason preCheckCool(WorkerDecision decision);

  /**
   * Apply a COOL action atomically and report its terminal hook.
   *
   * @param decision the COOL decision being settled; must not be null and must be COOL
   * @param action   the action to settle; must not be null
   */
  void settleCool(WorkerDecision decision, CoolAction action);
}
