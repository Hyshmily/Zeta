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
package io.github.hyshmily.zeta.sync.local;

import io.github.hyshmily.zeta.cache.EntryView;
import org.jspecify.annotations.Nullable;

/**
 * Strategy interface for inbound cache-sync events.
 *
 * <p>A policy is a pure decision layer: it never receives a write context, a batch or a
 * cache entry it can mutate. The framework owns per-key ordering, batch reduction, the
 * version guard, invalidation tombstones, clear watermarks, dedup-flight kills, value
 * decoding/loading and exactly one terminal {@link SyncHook} callback per event.
 *
 * <p><b>Call order.</b> The framework reduces the granted batch first and calls the policy
 * only for the effective winner; a covered PUT/REFRESH/INVALIDATE is reported to the hook as
 * {@link SyncSkipReason#COVERED_BY_NEWER_EVENT} without reaching the policy. The winner is
 * then admitted by {@link #admit}; an admitted value is resolved (PUT decode or REFRESH
 * load) and passed to {@link #settle}. A value that cannot be resolved never
 * reaches the policy; the framework reports the matching skip and applies its versioned
 * local invalidation fallback. A resolution that raced a newer resident or an invalidation
 * is settled as a skip before {@code settle} is called.
 *
 * <p>{@link #admit} runs on the cache-sync ordered dispatcher. For a REFRESH, {@link #settle}
 * runs on the dedicated refresh executor after the loader returned; a PUT settles on the
 * dispatcher. Neither runs on the AMQP consumer thread. The {@link EntryView} is a read-only
 * snapshot; the framework re-checks every version and tombstone when it applies the action.
 *
 * <p>Implement this interface and expose it as a Spring {@code @Bean} to replace the
 * default behaviour; {@code @ConditionalOnMissingBean} wires it.
 */
public interface SyncPolicy {
  /**
   * Decide whether the effective event should continue.
   *
   * <p>Called after the framework's cheap version/tombstone pre-check, so an event that is
   * already stale never reaches the policy. {@link SyncAdmission#INVALIDATE_LOCAL} drops a value
   * path: on a PUT/REFRESH it is settled as a versioned local invalidation, while on an
   * INVALIDATE or a clear it is treated as {@link SyncAdmission#PROCEED} because those already
   * remove the local copy. Returning an admission that does not fit the event never leaves a
   * stale entry behind.
   *
   * @param event   the effective event; never {@code null}
   * @param current the resident snapshot for a key event, or {@code null} for an absent key
   *                or a clear
   * @return the admission decision; never {@code null}
   */
  default SyncAdmission admit(SyncEvent event, @Nullable EntryView current) {
    return SyncAdmission.PROCEED;
  }

  /**
   * Settle the value a PUT payload decoded or a REFRESH loader resolved: apply it, invalidate
   * locally, or skip.
   *
   * <p>The framework re-checks the loader's {@code isCacheable} on an {@link
   * SyncValueAction.Apply} value, so a policy cannot bypass admission. The event's version
   * is the only version used for the write; a policy cannot mint a new stamp.
   *
   * @param event         the effective key event; never {@code null}
   * @param resolvedValue the decoded PUT value or loaded REFRESH value; never {@code null}
   * @param current       the resident snapshot observed after the value was resolved, or
   *                      {@code null}
   * @return the value action; never {@code null}
   */
  default SyncValueAction settle(SyncEvent event, Object resolvedValue, @Nullable EntryView current) {
    return new SyncValueAction.Apply(resolvedValue);
  }
}
