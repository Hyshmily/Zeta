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

import java.util.List;

/**
 * Strategy interface for processing cache-sync messages (REFRESH, INVALIDATE, PUT,
 * clears) received from peer application instances.
 *
 * <p>Implement this interface and expose it as a Spring {@code @Bean} to replace the
 * default cache-sync processing logic; {@code @ConditionalOnMissingBean} wires it.
 *
 * <p><b>The batch is the unit of work.</b> The ordered dispatcher grants all queued
 * messages of one cache key at once, so a handler receives them together and can reduce
 * the batch before touching L1: only the highest-version message of a key changes the
 * final state, and the messages it covers must not each pay a loader call, a payload
 * deserialization or a cache-bin lock. A single message arrives as a batch of one.
 */
public interface SyncDecisionHandler {
  /**
   * Applies one granted batch. All messages in the list share a dispatch key: the same
   * cache key for a per-key batch, or the clear channel for a batch of clears (whose
   * prefixes may differ).
   *
   * <p>Implementations apply at most one effective action per key — the batch has already
   * been ordered by the dispatcher, so a lower-version message can be dropped rather than
   * applied and overwritten. A REFRESH or PUT that produced no effect is reported through
   * {@link SyncHook#onRefreshSkipped} by the default implementation; the contract here is
   * only "the batch's final state is applied".
   *
   * @param batch the granted batch, in submission order (never empty)
   */
  void handleBatch(List<SyncMessage> batch);

  /**
   * Applies one message outside a dispatcher batch (the drop fallback on the consumer
   * thread, a direct call).
   *
   * @param syncMessage the message to apply
   */
  default void handle(SyncMessage syncMessage) {
    handleBatch(List.of(syncMessage));
  }
}
