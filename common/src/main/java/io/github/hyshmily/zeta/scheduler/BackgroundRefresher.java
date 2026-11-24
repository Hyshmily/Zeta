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

import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.VersionedValue;
import io.github.hyshmily.zeta.util.Internal;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;

/**
 * Background refresh executor for the L1 cache.
 *
 * <p>Two operations share one per-key dedup ticket and one limiter:
 * <ul>
 *   <li><b>soft refresh</b> — replaces the value and re-arms only the soft window, preserving the
 *       caller's absolute hard bound;</li>
 *   <li><b>hard renewal</b> — replaces the value and re-arms both declared windows, exactly like
 *       a natural expiry-and-reload, but earlier.</li>
 * </ul>
 *
 * <p>A hard renewal that arrives while a soft refresh is in flight upgrades that ticket instead
 * of issuing a second load. A failed soft refresh closes the retry window at the existing hard
 * deadline; a failed pure renewal leaves every deadline untouched.
 */
@Internal
public interface BackgroundRefresher {
  /**
   * Triggers a soft-expire refresh: the value is replaced and only the soft window re-arms, so
   * the caller's hard bound remains absolute. A refresh already in flight for the key dedups
   * this request; a resident that is absent, hard-expired or newer than the load is left
   * untouched. A failed or empty refresh closes the retry window at the existing hard deadline
   * so every stale read does not re-hit the source.
   *
   * @param cacheKey  the key to refresh
   * @param reader    the loader; it must return a {@code VersionedValue} with a non-null value
   * @param softTtlMs the soft TTL to apply
   */
  void triggerBackgroundRefresh(String cacheKey, Supplier<VersionedValue> reader, long softTtlMs);

  /**
   * Triggers a hard renewal: a fresh value starts a new full lifetime from the entry's own stored
   * hard/soft durations. If a refresh is already in flight for the key, the returned future is
   * that ticket's future: an upgraded soft ticket settles as this renewal, and a ticket already
   * writing back its soft settlement completes it only after that write-back has landed. A caller
   * that reschedules from the resident therefore observes the settled entry, never a half-applied
   * one.
   *
   * <p>The returned future always completes, including the limiter-rejected and executor-rejected
   * cases, so the caller can safely reschedule from it. A missing resident is never rebuilt from
   * the snapshot: a renewal may replace a resident, not resurrect an evicted or invalidated one.
   * A resident that crosses its hard deadline while the load runs is still the resident this
   * renewal was reserved for, and the fresh value re-arms both windows on it.
   *
   * @param cacheKey the key to renew
   * @param reader   the stamped loader
   * @return a future that always completes with the outcome
   */
  CompletableFuture<RenewOutcome> renewHard(String cacheKey, Supplier<VersionedValue> reader);

  /** @return the refresh limiter semaphore for monitoring */
  Semaphore getRefreshLimiter();

  /** The result of one renewal. */
  sealed interface RenewOutcome permits RenewOutcome.Applied, RenewOutcome.Skipped, RenewOutcome.Failed {
    /**
     * The renewal applied; the entry carries the fresh value and re-armed deadlines.
     *
     * @param entry the newly stored entry
     */
    record Applied(CacheEntry entry) implements RenewOutcome {}

    /**
     * No renewal was applied; the resident, if any, is untouched. A caller may act on the reason,
     * so the closed set is the value itself rather than a diagnostic string:
     * {@link #SOFT_REFRESH} and {@link #NEWER_VALUE} leave the key without a renewal and are the
     * reasons a scheduler re-arms from the resident.
     */
    enum Skipped implements RenewOutcome {
      /** The refresh limiter had no free permit. */
      LIMITER,
      /** The refresh executor refused the task. */
      EXECUTOR_REJECTED,
      /** The in-flight ticket settled a soft refresh instead, which does not re-arm the hard window. */
      SOFT_REFRESH,
      /** An invalidation tombstone outranks the loaded value. */
      INVALIDATED,
      /** The resident is gone, or a soft settlement found its hard deadline already passed. */
      ABSENT_OR_EXPIRED,
      /** The resident is newer than the value this ticket loaded. */
      NEWER_VALUE,
    }

    /**
     * The load failed, timed out or returned no value.
     *
     * @param cause the failure, or {@code null} for an empty answer
     */
    record Failed(@Nullable Throwable cause) implements RenewOutcome {
      public static final Failed EMPTY = new Failed(null);
    }
  }
}
