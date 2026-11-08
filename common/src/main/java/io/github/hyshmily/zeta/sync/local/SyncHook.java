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

import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;

/**
 * Callback hook for cache-sync lifecycle events (REFRESH, INVALIDATE, PUT).
 *
 * <p>Implement this interface and expose it as a Spring {@code @Bean} to observe
 * cross-instance sync outcomes. Multiple hooks are supported and will be invoked
 * in iteration order.
 *
 * <p>All methods are {@code default} no-ops — implement only the ones you need.
 * Exceptions thrown by a hook are caught and logged; they never propagate.
 */
public interface SyncHook {
  /**
   * Called after a peer value has been applied to L1 — a REFRESH reload or a
   * value-carrying PUT (the same hook surface; {@code syncMessage.type()} distinguishes
   * them).
   *
   * @param cacheKey the refreshed cache key
   * @param syncMessage       the sync message that triggered the refresh
   * @param entry    the updated {@link CacheEntry} (never null)
   */
  default void afterRefresh(String cacheKey, SyncMessage syncMessage, CacheEntry entry) {}

  /**
   * Called after a key has been removed from the local cache via an INVALIDATE sync message.
   *
   * @param cacheKey the invalidated cache key
   * @param syncMessage       the sync message that triggered the invalidation
   */
  default void afterInvalidate(String cacheKey, SyncMessage syncMessage) {}

  /**
   * Called when a REFRESH or PUT sync message produced no effect: the local entry already
   * held a strictly newer dataVersion, the value was not found, the value could not be read,
   * the apply was rejected by the invalidation watermark, or a higher-version message in the
   * same dispatch batch covered it before it could apply.
   *
   * <p>Every REFRESH/PUT therefore receives exactly one terminal callback: this one, or
   * {@link #afterRefresh}.
   *
   * @param cacheKey the cache key whose refresh was skipped
   * @param syncMessage       the sync message that would have triggered the refresh
   */
  default void onRefreshSkipped(String cacheKey, SyncMessage syncMessage) {}
}
