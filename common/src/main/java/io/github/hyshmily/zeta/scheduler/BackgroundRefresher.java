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
package io.github.hyshmily.zeta.scheduler;

import io.github.hyshmily.zeta.cache.cachesupport.raw.VersionedValue;
import io.github.hyshmily.zeta.util.Internal;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

/**
 * Background soft-expire refresh executor for the L1 cache — the scheduling
 * half of the entry lifecycle.
 *
 * <p>When a read serves a soft-expired (stale-while-revalidate) entry, the
 * read path calls {@link #triggerBackgroundRefresh}: the fresh value is loaded
 * on a bounded executor, version-guarded against superseding writes, and
 * merged into L1 without blocking the caller. Failures keep the stale entry
 * servable through the lease-on-failure extension (design decisions: expiry and refresh).
 *
 * <p>Implementations expose the refresh limiter semaphore through
 * {@link #getRefreshLimiter()}; monitoring surfaces (actuator endpoint,
 * Micrometer gauges) read it there.
 */
@Internal
public interface BackgroundRefresher {
  /**
   * Triggers an asynchronous background refresh for the given cache key. The
   * caller (the read path) has already returned the stale value to the
   * client, so this method executes entirely in the background without
   * blocking the caller.
   *
   * <p>The {@code reader} must return a
   * {@link VersionedValue} with a non-null value —
   * the implementation only applies that shape (anything else is treated as "no
   * new value"). The production read path wraps the caller's supplier with a
   * post-read version probe (design decisions: version stamps); a bare value is therefore silently not
   * applied.
   *
   * @param cacheKey  the key whose value should be refreshed
   * @param reader    the data-source supplier
   * @param softTtlMs the soft TTL to set on the refreshed entry (milliseconds)
   */
  void triggerBackgroundRefresh(String cacheKey, Supplier<?> reader, long softTtlMs);

  /** Expose the refresh limiter semaphore for monitoring purposes. */
  Semaphore getRefreshLimiter();

  /**
   * Cumulative background-refresh failures that were granted a lease
   * (stale entry kept servable, design decisions: expiry and refresh). {@code 0} for implementations
   * without lease accounting.
   *
   * @return lifetime count of leases granted
   */
  default long getRefreshFailureLeased() {
    return 0L;
  }

  /**
   * Oldest currently-leased keys for the {@code staleDebt} diagnostic view:
   * entries whose last refresh failed and whose lease is still holding them
   * servable, ordered oldest-first.
   *
   * @param limit maximum entries to return; a non-positive value returns an
   *              empty map without copying the lease table
   * @return map of key to staleness milliseconds (time since the first lease
   *         of the current episode), never {@code null}
   */
  default Map<String, Long> leasedStaleness(int limit) {
    return Map.of();
  }

  /**
   * O(1) count of currently tracked lease episodes for gauges.
   *
   * <p>Prefer this over {@code leasedStaleness(Integer.MAX_VALUE).size()} on hot
   * metric paths: the view copy materialises the full map per scrape, while the
   * count is a single map size read. May include entries whose L1 value already left
   * (lazily reaped on the next view read).
   *
   * @return the number of tracked lease episodes
   */
  default int leasedKeyCount() {
    return 0;
  }
}
