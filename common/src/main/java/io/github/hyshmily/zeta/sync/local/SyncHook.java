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
import java.util.List;

/**
 * Callback hook for cache-sync lifecycle events (PUT, REFRESH, INVALIDATE and the clear
 * family).
 *
 * <p>Implement this interface and expose it as a Spring {@code @Bean} to observe
 * cross-instance sync outcomes. Multiple hooks are supported and invoked in iteration
 * order.
 *
 * <p><b>Terminal contract.</b> Every event the framework applies through the dispatcher
 * reaches exactly one terminal method: {@link #afterValueApplied} for a written PUT/REFRESH
 * value, {@link #afterValueDegraded} for a value event the framework applied as a local
 * invalidation, {@link #afterInvalidate} for an applied peer INVALIDATE, {@link #afterClear}
 * for a granted clear batch, or {@link #onSkipped} with the reason. A covered batch message
 * reaches {@link #onSkipped} with {@link SyncSkipReason#COVERED_BY_NEWER_EVENT}. A policy that
 * refuses or throws is settled as a skip.
 *
 * <p>Hooks run only on the cache-sync ordered dispatcher, never on the AMQP consumer
 * thread. A message the dispatcher refuses at submit time is applied by the no-hook
 * rejection fallback, so a slow hook can never delay the broker ack.
 *
 * <p>All methods are {@code default} no-ops — implement only the ones you need.
 * Exceptions thrown by a hook are caught and logged; they never propagate.
 */
public interface SyncHook {
  /**
   * Called after a peer value has been written to L1 — a REFRESH reload or a value-carrying
   * PUT.
   *
   * @param event the applied key event; {@code event.type()} distinguishes PUT and REFRESH
   * @param entry the written entry snapshot; never {@code null}
   */
  default void afterValueApplied(SyncEvent event, EntryView entry) {}

  /**
   * Called after a peer value event was applied as a local invalidation because it had no
   * writable value or the policy declined it.
   *
   * @param event  the value event that was degraded
   * @param reason why the value was not written ({@code VALUE_NOT_FOUND},
   *               {@code PAYLOAD_UNREADABLE}, {@code VALUE_DECLINED_BY_LOADER} or
   *               {@code DEGRADED_TO_INVALIDATE})
   */
  default void afterValueDegraded(SyncEvent event, SyncSkipReason reason) {}

  /**
   * Called after a peer INVALIDATE was applied.
   *
   * @param event the applied invalidation event
   */
  default void afterInvalidate(SyncEvent event) {}

  /**
   * Called once for a granted clear batch after its one sweep. The list carries every
   * effective clear event of the batch, each at the watermark the sweep applied (a local
   * replacement when the peer stamp was refused), so a per-prefix hook can attribute the batch
   * count itself; a skipped clear is reported through {@link #onSkipped} and is not in the list.
   *
   * @param clears          the effective clear events; never empty
   * @param affectedEntries the number of resident entries the sweep removed
   */
  default void afterClear(List<SyncEvent> clears, int affectedEntries) {}

  /**
   * Called when an event produced no applied effect.
   *
   * @param event  the refused or superseded event
   * @param reason why it produced no effect
   */
  default void onSkipped(SyncEvent event, SyncSkipReason reason) {}
}
