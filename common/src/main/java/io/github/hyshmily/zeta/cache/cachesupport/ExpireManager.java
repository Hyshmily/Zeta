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
 * Manages hard and soft TTL computation for {@link CacheEntry} instances.
 * <p>
 * Hard TTL controls Caffeine eviction; soft TTL controls stale-while-revalidate background refresh.
 * Each has a normal-key and hot-key variant, with an optional override taking precedence over the default.
 */
public interface ExpireManager {
  /** Whether any soft TTL is configured (normal or hot). */
  boolean isSoftExpireEnabled();

  /**
   * Check whether a {@link CacheEntry} has logically expired based on its
   * {@code hardExpireAtMs}.  Entries with {@code hardExpireAtMs == Long.MAX_VALUE}
   * are treated as permanent (never logically expire).
   */
  boolean isLogicallyExpired(CacheEntry entry);

  /**
   * Check whether the given raw cache value is a logically expired {@link CacheEntry}
   * and, if so, invalidate it and return {@code true}.
   */
  boolean invalidateIfIsLogicallyExpired(String cacheKey, Object raw);

  long computeNullExpireAt(long nullTtlMs);

  /** Hard expire timestamp from an explicit TTL duration. */
  long computeHardExpireAt(long hardTtlMs);

  /** Soft expire timestamp from an explicit TTL duration. */
  long computeSoftExpireAt(long softTtlMs);

  /** Effective hard TTL for normal keys. */
  long getEffectiveHardTtlMs();

  /** Hard expire timestamp for hot keys, returns {@code Long.MAX_VALUE} if hot hard expire is disabled. */
  long computeHotHardExpireAt();

  /** Soft expire timestamp for hot keys, returns 0 if disabled. */
  long computeHotSoftExpireAt();

  /** Effective hard TTL for hot keys. */
  long getEffectiveHotHardTtlMs();

  /** Effective soft TTL for normal keys. */
  long getEffectiveSoftTtlMs();

  /** Effective soft TTL for hot keys. */
  long getEffectiveHotSoftTtlMs();

  long resolveEffectiveHardTtl(long hardTtlMs);

  long resolveEffectiveHotHard(long hardTtlMs);

  long resolveEffectiveSoftTtl(long softTtlMs);

  long resolveEffectiveHotSoft(long softTtlMs);

  CacheEntry createBuilder(
    Object value,
    long dataVersion,
    boolean isVersionDegraded,
    long decisionVersion,
    String decisionNodeId,
    long decisionEpoch,
    long hardTtlMs,
    long softTtlMs,
    long hardExpireAtMs,
    long softExpireAtMs,
    long normalHardTtlMs,
    long normalSoftTtlMs,
    KeyState keyState
  );

  CacheEntry createBuilder(
    Object value,
    long dataVersion,
    boolean isVersionDegraded,
    long decisionVersion,
    String decisionNodeId,
    long decisionEpoch,
    long hardTtlMs,
    long softTtlMs,
    long normalHardTtlMs,
    long normalSoftTtlMs,
    KeyState keyState
  );

  CacheEntry createBuilder(
    Object value,
    long dataVersion,
    boolean isVersionDegraded,
    long decisionVersion,
    long hardTtlMs,
    long softTtlMs,
    long hardExpireAtMs,
    long softExpireAtMs,
    long normalHardTtlMs,
    long normalSoftTtlMs,
    KeyState keyState
  );

  CacheEntry createBuilder(
    Object value,
    long dataVersion,
    boolean isVersionDegraded,
    long decisionVersion,
    long hardTtlMs,
    long softTtlMs,
    long normalHardTtlMs,
    long normalSoftTtlMs,
    KeyState keyState
  );

  CacheEntry applyNormalTtl(CacheEntry original, long hardTtlMs, long softTtlMs);

  CacheEntry applyTtl(CacheEntry original, long hardTtlMs, long softTtlMs);

  CacheEntry applyHardTtl(CacheEntry original, long hardTtlMs);

  CacheEntry applySoftTtl(CacheEntry original, long softTtlMs);

  /**
   * Create a copy of the entry with a new (wrapped) value, preserving all other metadata.
   */
  CacheEntry replaceEntryValue(CacheEntry entry, @Nullable Object newValue);

  /** Convert a TTL duration (ms) to an absolute epoch-ms expiration timestamp using the configured default jitter ratio. */
  long toHardExpireTimestamp(long hardTtlMs);

  /** Convert a TTL duration (ms) to an absolute epoch-ms expiration timestamp using the given jitter ratio. */
  long toHardExpireTimestamp(long hardTtlMs, double ttlJitterRatio);

  /** Convert a soft TTL duration (ms) to an absolute epoch-ms expiration timestamp using the configured default jitter ratio. */
  long toSoftExpireTimestamp(long softTtlMs);

  /** Convert a soft TTL duration (ms) to an absolute epoch-ms expiration timestamp using the given jitter ratio. */
  long toSoftExpireTimestamp(long softTtlMs, double ttlJitterRatio);

  /**
   * Check whether the given key's soft TTL has expired.
   *
   * @return {@code true} if the entry's soft TTL has expired or the entry is absent
   * @throws IllegalStateException if soft expire is disabled
   */
  boolean isSoftExpired(Object cacheEntry);

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
