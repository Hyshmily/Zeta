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
package io.github.hyshmily.zeta.cache.cachesupport;

import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.KeyState;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;

/**
 * Manages the stateful side of the {@link CacheEntry} lifecycle: background
 * refresh scheduling, TOCTOU invalidation guards, expiry extension, and the
 * entry factory.
 *
 * <p>All stateless TTL arithmetic (resolve / compute / getEffective /
 * timestamp conversion with jitter / expiry predicates / entry-level TTL
 * transforms) lives in {@link TtlPolicy}, exposed here via
 * {@link #ttlPolicy()}.
 *
 * <p>Hard TTL controls Caffeine eviction; soft TTL controls stale-while-revalidate background refresh.
 * Each has a normal-key and hot-key variant, with an optional override taking precedence over the default.
 */
public interface ExpireManager {
  /**
   * The pure TTL and expiry policy backing this manager: every stateless
   * lifecycle computation (resolve vs default, expire timestamps, jitter,
   * predicates, TTL transforms) is performed through this module.
   *
   * @return the TTL policy; never null
   */
  TtlPolicy ttlPolicy();

  /**
   * Check whether the given raw cache value is a logically expired {@link CacheEntry}
   * and, if so, invalidate it and return {@code true}.
   */
  boolean invalidateIfIsLogicallyExpired(String cacheKey, Object raw);

  /**
   * The version stamp carried by a cache entry: the {@code dataVersion} used
   * for cross-instance sync ordering, plus whether it was produced in
   * degraded (local fallback) mode.
   *
   * @param dataVersion       the data version for cross-instance sync
   * @param isVersionDegraded whether the data version is degraded (local fallback)
   */
  record VersionStamp(long dataVersion, boolean isVersionDegraded) {}

  /**
   * The Worker decision stamp carried by a cache entry. Only present when the
   * entry has a Worker origin (HOT/COOL broadcasts); local entries carry no
   * decision metadata.
   *
   * @param decisionVersion the Worker decision version
   * @param decisionNodeId  the Worker node ID that produced the decision
   * @param decisionEpoch   the epoch (restart counter) of the decision Worker
   */
  record DecisionStamp(long decisionVersion, @Nullable String decisionNodeId, long decisionEpoch) {}

  /**
   * The TTL durations for an entry: the current hard/soft TTLs and the
   * normal (non-hot) baseline TTLs used for state reversion.
   *
   * @param hardTtlMs       hard TTL duration in milliseconds
   * @param softTtlMs       soft TTL duration in milliseconds
   * @param normalHardTtlMs normal (non-hot) hard TTL for state reversion
   * @param normalSoftTtlMs normal (non-hot) soft TTL for state reversion
   */
  record TtlSpec(long hardTtlMs, long softTtlMs, long normalHardTtlMs, long normalSoftTtlMs) {}

  /**
   * Pre-computed expire timestamps, supplied when the caller already knows
   * the exact expiry baseline (e.g. when copying from an existing entry).
   * When {@code null}, expire timestamps are computed automatically via
   * {@link TtlPolicy#applyTtl}.
   *
   * @param hardExpireAtMs pre-computed hard expiry absolute timestamp
   * @param softExpireAtMs pre-computed soft expiry absolute timestamp
   */
  record ExpiryAt(long hardExpireAtMs, long softExpireAtMs) {}

  /**
   * Build a {@link CacheEntry} from resolved fields.
   *
   * <p>Replaces the previous four-parameter-combination overloads: the
   * {@code decision} stamp is present only for Worker-sourced entries
   * (HOT/COOL broadcasts), and {@code expiryAt} is supplied only when the
   * caller has pre-computed timestamps — otherwise they are computed via
   * {@link TtlPolicy#applyTtl} from the TTL spec. Normal TTL is applied via
   * {@link TtlPolicy#applyNormalTtl} after construction.
   *
   * @param value     the cached value
   * @param version   the data version stamp (sync ordering + degraded flag)
   * @param decision  the Worker decision stamp, or {@code null} for local entries
   * @param ttl       the current and normal TTL durations
   * @param expiryAt  pre-computed expire timestamps, or {@code null} to compute
   * @param keyState  the initial key state (NORMAL, HOT, COOL)
   * @return a new {@link CacheEntry} with all fields set
   */
  CacheEntry createBuilder(
    Object value,
    VersionStamp version,
    @Nullable DecisionStamp decision,
    TtlSpec ttl,
    @Nullable ExpiryAt expiryAt,
    KeyState keyState
  );

  /**
   * Create a copy of the entry with a new (wrapped) value, preserving all other metadata.
   */
  CacheEntry replaceEntryValue(CacheEntry entry, @Nullable Object newValue);

  /**
   * Wrap a raw value using the configured compressor, without allocating a new CacheEntry.
   * Used by callers that need the compressed value for direct CacheEntry construction.
   */
  Object wrapValue(@Nullable Object rawValue);

  /** Expose the refresh limiter semaphore for monitoring purposes. */
  Semaphore getRefreshLimiter();

  /**
   * Triggers an asynchronous background refresh for the given cache key if the
   * current entry has reached its soft expiry threshold.
   */
  void triggerBackgroundRefresh(String cacheKey, Supplier<?> reader, long softTtlMs);

  /** Extend both the hard and soft expiry for a cache entry. */
  void extendExpiry(String cacheKey, long hardTtlMs, long softTtlMs);

  /** Extend only the hard expiry for a cache entry, leaving the soft expiry unchanged. */
  void extendHardExpiry(String cacheKey, long hardTtlMs);

  /** Extend only the soft expiry for a cache entry, leaving the hard expiry unchanged. */
  void extendSoftExpiry(String cacheKey, long softTtlMs);
}
