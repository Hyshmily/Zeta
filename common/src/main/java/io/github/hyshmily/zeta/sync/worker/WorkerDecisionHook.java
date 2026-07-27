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

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.model.CacheEntry;

/**
 * Callback hook for Worker HOT/COOL decision processing lifecycle events.
 *
 * <p>Implement this interface and expose it as a Spring {@code @Bean} to observe
 * or react to Worker decision outcomes without modifying the core promotion logic.
 * Multiple hooks are supported and will be invoked in iteration order.
 *
 * <p>All methods are {@code default} no-ops — implement only the ones you need.
 * Exceptions thrown by a hook are caught and logged; they never propagate to
 * the caller or interrupt other hooks.
 */
@Internal
public interface WorkerDecisionHook {

  /**
   * Called after a key has been successfully promoted to {@link io.github.hyshmily.zeta.model.KeyState#HOT}.
   *
   * @param cacheKey the promoted cache key
   * @param wm       the Worker message that triggered the promotion
   * @param entry    the newly written {@link CacheEntry} (never null)
   */
  default void afterHotPromotion(String cacheKey, WorkerMessage wm, CacheEntry entry) {}

  /**
   * Called after a key has been downgraded to {@link io.github.hyshmily.zeta.model.KeyState#COOL}.
   *
   * @param cacheKey the downgraded cache key
   * @param wm       the Worker message that triggered the downgrade
   * @param entry    the updated {@link CacheEntry} (never null)
   */
  default void afterCoolDowngrade(String cacheKey, WorkerMessage wm, CacheEntry entry) {}

  /**
   * Called when a HOT decision was skipped for the given reason.
   *
   * @param cacheKey the cache key whose HOT promotion was skipped
   * @param wm       the Worker message that would have triggered the promotion
   * @param reason   why the promotion was skipped
   */
  default void onHotSkipped(String cacheKey, WorkerMessage wm, HotSkipReason reason) {}

  /**
   * Called when a COOL decision was skipped because no entry exists in L1.
   *
   * @param cacheKey the cache key whose COOL downgrade was skipped
   * @param wm       the Worker message that would have triggered the downgrade
   */
  default void onCoolSkipped(String cacheKey, WorkerMessage wm) {}
}
