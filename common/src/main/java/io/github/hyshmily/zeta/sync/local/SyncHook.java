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

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.model.CacheEntry;

/**
 * Callback hook for cache-sync lifecycle events (REFRESH, INVALIDATE).
 *
 * <p>Implement this interface and expose it as a Spring {@code @Bean} to observe
 * cross-instance sync outcomes. Multiple hooks are supported and will be invoked
 * in iteration order.
 *
 * <p>All methods are {@code default} no-ops — implement only the ones you need.
 * Exceptions thrown by a hook are caught and logged; they never propagate.
 */
@Internal
public interface SyncHook {

  /**
   * Called after a key has been refreshed from the data store via a REFRESH sync message.
   *
   * @param cacheKey the refreshed cache key
   * @param sm       the sync message that triggered the refresh
   * @param entry    the updated {@link CacheEntry} (never null)
   */
  default void afterRefresh(String cacheKey, SyncMessage sm, CacheEntry entry) {}

  /**
   * Called after a key has been removed from the local cache via an INVALIDATE sync message.
   *
   * @param cacheKey the invalidated cache key
   * @param sm       the sync message that triggered the invalidation
   */
  default void afterInvalidate(String cacheKey, SyncMessage sm) {}

  /**
   * Called when a REFRESH sync message was skipped because the local version
   * is already up to date or the value was not found.
   *
   * @param cacheKey the cache key whose refresh was skipped
   * @param sm       the sync message that would have triggered the refresh
   */
  default void onRefreshSkipped(String cacheKey, SyncMessage sm) {}
}
