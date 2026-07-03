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
package io.github.hyshmily.hotkey.cache.cachesupport;

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.VERSION_DEFAULT;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.Internal;
import io.github.hyshmily.hotkey.autoconfigure.HotKeyProperties;
import io.github.hyshmily.hotkey.model.CacheEntry;
import io.github.hyshmily.hotkey.model.KeyState;
import io.github.hyshmily.hotkey.util.DelayUtil;
import io.github.hyshmily.hotkey.util.TimeSource;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages hard and soft TTL computation for {@link CacheEntry} instances.
 * <p>
 * Hard TTL controls Caffeine eviction; soft TTL controls stale-while-revalidate background refresh.
 * Each has a normal-key and hot-key variant, with an optional override taking precedence over the default.
 */
@Getter
@Slf4j
@Internal
public class CacheExpireManager {

  /** The underlying L1 Caffeine cache instance. */
  private final Cache<String, Object> caffeineCache;
  /** Async executor for background refresh tasks. */
  private final Executor executor;
  /** TTL configuration providing normal and hot-key TTL values. */
  private final HotKeyProperties ttlConfig;
  /** Semaphore limiting concurrent background refresh operations (null if soft expire disabled). */
  // null when soft expire is disabled; always guarded by isSoftExpireEnabled() check
  private final Semaphore refreshLimiter;
  /** Per-key dedup for background refreshes — prevents concurrent refresh for the same key. */
  private final ConcurrentHashMap<String, CompletableFuture<?>> pendingRefreshes = new ConcurrentHashMap<>();
  /** Jitter ratio applied to TTLs to prevent cache stampedes (from config, default 0.05 = ±5%). */
  private final double defaultTtlJitterRatio;

  private static final long refreshTimeoutSeconds = 30;

  private static final int BLOCKED_TAG = -1;

  /**
   * Creates a CacheExpireManager with the given Caffeine cache, executor, and TTL config.
   *
   * @param caffeineCache   the underlying L1 Caffeine cache
   * @param executor        async executor for background refresh
   * @param ttlConfig       TTL configuration (normal and hot-key variants)
   * @param refreshMaxPools maximum concurrent background refreshes (capped at 100)
   */
  public CacheExpireManager(
    Cache<String, Object> caffeineCache,
    Executor executor,
    HotKeyProperties ttlConfig,
    int refreshMaxPools
  ) {
    this.caffeineCache = caffeineCache;
    this.executor = executor;
    this.ttlConfig = ttlConfig;
    this.refreshLimiter = ttlConfig.isSoftExpireEnabled()
      ? new Semaphore(refreshMaxPools > 0 ? refreshMaxPools : 100)
      : null;
    this.defaultTtlJitterRatio = ttlConfig.getTtlJitterRatio();
  }

  /**
   * Create a CacheExpireManager with explicit jitter ratio (for testing).
   */
  CacheExpireManager(
    Cache<String, Object> caffeineCache,
    Executor executor,
    HotKeyProperties ttlConfig,
    int refreshMaxPools,
    double defaultTtlJitterRatio
  ) {
    this.caffeineCache = caffeineCache;
    this.executor = executor;
    this.ttlConfig = ttlConfig;
    this.refreshLimiter = ttlConfig.isSoftExpireEnabled()
      ? new Semaphore(refreshMaxPools > 0 ? refreshMaxPools : 100)
      : null;
    this.defaultTtlJitterRatio = defaultTtlJitterRatio;
  }

  /**
   * Whether any soft TTL is configured (normal or hot).
   *
   * @return {@code true} if soft expire is enabled in the configuration
   */
  public boolean isSoftExpireEnabled() {
    return ttlConfig.isSoftExpireEnabled();
  }

  /**
   * Check whether a {@link CacheEntry} has logically expired based on its
   * {@code hardExpireAtMs}.  Entries with {@code hardExpireAtMs == Long.MAX_VALUE}
   * are treated as permanent (never logically expire).
   *
   * @param entry the cache entry to inspect
   * @return {@code true} if the entry has logically expired
   */
  public boolean isLogicallyExpired(CacheEntry entry) {
    return entry.getHardExpireAtMs() != Long.MAX_VALUE && TimeSource.currentTimeMillis() >= entry.getHardExpireAtMs();
  }

  /**
   * Check whether the given raw cache value is a logically expired {@link CacheEntry}
   * and, if so, invalidate it and return {@code true}.
   * <p>Eliminates code duplication between {@link io.github.hyshmily.hotkey.cache.HotKeyCache#get}
   * and {@link io.github.hyshmily.hotkey.cache.HotKeyCache#getWithSoftExpire},
   * which both perform this check before and after side effects (TOCTOU guard).
   *
   * @param cacheKey the cache key to invalidate if expired
   * @param raw      the raw value from the Caffeine cache
   * @return {@code true} if the entry was expired and has been invalidated
   */
  public boolean invalidateIfIsLogicallyExpired(String cacheKey, Object raw) {
    if (raw instanceof CacheEntry ce && isLogicallyExpired(ce)) {
      caffeineCache.invalidate(cacheKey);
      log.debug("Cache entry logically expired during processing, reloading: {}", cacheKey);
      return true;
    }
    return false;
  }

  public long computeNullExpireAt(long nullTtlMs) {
    long effective = nullTtlMs > 0 ? nullTtlMs : ttlConfig.effectiveNullTtlMs();
    return toHardExpireTimestamp(effective);
  }

  /**
   * Hard expire timestamp from an explicit TTL duration.
   * Falls back to the normal-key default if {@code hardTtlMs <= 0}.
   *
   * @param hardTtlMs the hard TTL duration in milliseconds (&lt;= 0 uses configured default)
   * @return absolute epoch-ms timestamp for hard expiry
   */
  public long computeHardExpireAt(long hardTtlMs) {
    return toHardExpireTimestamp(resolveEffectiveHard(hardTtlMs));
  }

  /**
   * Hard expire timestamp for hot keys, using {@code default-hot-hard-ttl} / {@code hot-hard-ttl}.
   * Returns {@code Long.MAX_VALUE} if hot hard expire is disabled (TTL &lt;= 0).
   *
   * @return absolute epoch-ms timestamp for hot-key hard expiry
   */
  public long computeHotHardExpireAt() {
    return toHardExpireTimestamp(ttlConfig.effectiveHotHardTtlMs());
  }

  /**
   * Soft expire timestamp for hot keys, using {@code default-hot-soft-ttl} / {@code hot-soft-ttl}.
   *
   * @return absolute epoch-ms timestamp for hot-key soft expiry, or 0 if disabled
   */
  public long computeHotSoftExpireAt() {
    return toSoftExpireTimestamp(ttlConfig.effectiveHotSoftTtlMs());
  }

  /**
   * Soft expire timestamp from an explicit TTL duration.
   * Falls back to the normal-key default if {@code softTtlMs <= 0}. Returns 0 if soft expire is disabled.
   *
   * @param softTtlMs the soft TTL duration in milliseconds (&lt;= 0 uses configured default)
   * @return absolute epoch-ms timestamp for soft expiry, or 0 if disabled
   */
  public long computeSoftExpireAt(long softTtlMs) {
    if (!isSoftExpireEnabled()) {
      return 0L;
    }
    return toSoftExpireTimestamp(resolveEffectiveSoft(softTtlMs));
  }

  /**
   * Effective hard TTL for normal keys (override > default).
   *
   * @return effective hard TTL duration in milliseconds
   */
  public long getEffectiveHardTtlMs() {
    return ttlConfig.effectiveHardTtlMs();
  }

  /**
   * Resolve effective hard TTL for normal keys: use the override value if
   * positive, otherwise fall back to the configured default.
   *
   * @param hardTtlMs hard TTL override ({@code 0} or negative uses default)
   * @return effective hard TTL duration in milliseconds
   */
  public long resolveEffectiveHard(long hardTtlMs) {
    return hardTtlMs > 0 ? hardTtlMs : getEffectiveHardTtlMs();
  }

  /**
   * Effective hard TTL for hot keys (override > default).
   *
   * @return effective hot hard TTL duration in milliseconds
   */
  public long getEffectiveHotHardTtlMs() {
    return ttlConfig.effectiveHotHardTtlMs();
  }

  /**
   * Resolve effective hard TTL for hot keys: use the override value if
   * positive, otherwise fall back to the configured hot-key hard TTL.
   *
   * @param hardTtlMs hard TTL override ({@code 0} or negative uses default)
   * @return effective hot-key hard TTL duration in milliseconds
   */
  public long resolveEffectiveHotHard(long hardTtlMs) {
    return hardTtlMs > 0 ? hardTtlMs : getEffectiveHotHardTtlMs();
  }

  /**
   * Effective soft TTL for normal keys (override > default).
   *
   * @return effective soft TTL duration in milliseconds
   */
  public long getEffectiveSoftTtlMs() {
    return ttlConfig.effectiveSoftTtlMs();
  }

  /**
   * Resolve effective soft TTL for normal keys: use the override value if
   * positive, otherwise fall back to the configured default.
   *
   * @param softTtlMs soft TTL override ({@code 0} or negative uses default)
   * @return effective soft TTL duration in milliseconds
   */
  public long resolveEffectiveSoft(long softTtlMs) {
    return softTtlMs > 0 ? softTtlMs : getEffectiveSoftTtlMs();
  }

  /**
   * Effective soft TTL for hot keys (override > default).
   *
   * @return effective hot soft TTL duration in milliseconds
   */
  public long getEffectiveHotSoftTtlMs() {
    return ttlConfig.effectiveHotSoftTtlMs();
  }

  /**
   * Resolve effective soft TTL for hot keys: use the override value if
   * positive, otherwise fall back to the configured hot-key soft TTL.
   *
   * @param softTtlMs soft TTL override ({@code 0} or negative uses default)
   * @return effective hot-key soft TTL duration in milliseconds
   */
  public long resolveEffectiveHotSoft(long softTtlMs) {
    return softTtlMs > 0 ? softTtlMs : getEffectiveHotSoftTtlMs();
  }

  /**
   * Create a new {@link CacheEntry} with updated TTL fields, preserving all
   * other metadata from the supplied original entry.
   *
   * <p>Sets {@code hardTtlMs}, {@code softTtlMs},
   * {@code hardExpireAtMs} (via {@link #computeHardExpireAt}),
   * and {@code softExpireAtMs} (via {@link #computeSoftExpireAt}).
   *
   * @param original   an existing {@link CacheEntry} whose metadata should be preserved;
   *                   must not be null
   * @param hardTtlMs  hard TTL duration in milliseconds
   * @param softTtlMs  soft TTL duration in milliseconds
   * @return a new {@link CacheEntry} with the updated TTL timestamps,
   *         while keeping all version, state, and normal TTL fields unchanged
   */
  public CacheEntry applyTtl(CacheEntry original, long hardTtlMs, long softTtlMs) {
    return original
      .toBuilder()
      .hardTtlMs(hardTtlMs)
      .softTtlMs(softTtlMs)
      .hardExpireAtMs(computeHardExpireAt(hardTtlMs))
      .softExpireAtMs(computeSoftExpireAt(softTtlMs))
      .build();
  }

  /**
   * Convert a TTL duration (ms) to an absolute epoch-ms expiration timestamp
   * using the configured default jitter ratio.
   * Propagates {@link Long#MAX_VALUE} unchanged — used to signal permanent entries
   * (pure logical expiry with no hard TTL eviction).
   */
  public long toHardExpireTimestamp(long hardTtlMs) {
    return toHardExpireTimestamp(hardTtlMs, defaultTtlJitterRatio);
  }

  /**
   * Convert a TTL duration (ms) to an absolute epoch-ms expiration timestamp
   * using the given jitter ratio instead of the configured default.
   * Propagates {@link Long#MAX_VALUE} unchanged.
   *
   * @param hardTtlMs      the hard TTL duration in milliseconds
   * @param ttlJitterRatio the jitter ratio to apply (0.0–1.0)
   * @return absolute epoch-ms timestamp for hard expiry
   */
  public long toHardExpireTimestamp(long hardTtlMs, double ttlJitterRatio) {
    if (hardTtlMs == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    long jitter = DelayUtil.computeTtlJitter(hardTtlMs, ttlJitterRatio);

    return hardTtlMs > 0 ? TimeSource.currentTimeMillis() + Math.max(1, hardTtlMs + jitter) : Long.MAX_VALUE;
  }

  /**
   * Convert a soft TTL duration (ms) to an absolute epoch-ms expiration timestamp.
   * Applies configurable jitter (default ±5%) to prevent cache stampedes.
   * Returns 0 if soft expire is disabled or the TTL is non-positive.
   *
   * @param softTtlMs the soft TTL duration in milliseconds
   * @return absolute epoch-ms timestamp for soft expiry, or 0 if disabled
   */
  public long toSoftExpireTimestamp(long softTtlMs) {
    return toSoftExpireTimestamp(softTtlMs, defaultTtlJitterRatio);
  }

  /**
   * Convert a soft TTL duration (ms) to an absolute epoch-ms expiration timestamp
   * using the given jitter ratio instead of the configured default.
   * Returns 0 if soft expire is disabled. Propagates {@link Long#MAX_VALUE} unchanged.
   *
   * @param softTtlMs      the soft TTL duration in milliseconds
   * @param ttlJitterRatio the jitter ratio to apply (0.0–1.0)
   * @return absolute epoch-ms timestamp for soft expiry, or 0 if disabled
   */
  public long toSoftExpireTimestamp(long softTtlMs, double ttlJitterRatio) {
    if (!isSoftExpireEnabled() || softTtlMs <= 0) {
      return 0L;
    }
    if (softTtlMs == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    long jitter = DelayUtil.computeTtlJitter(softTtlMs, ttlJitterRatio);

    return TimeSource.currentTimeMillis() + Math.max(1, softTtlMs + jitter);
  }

  /**
   * Check whether the given key's soft TTL has expired.
   *
   * @return {@code true} if the entry's soft TTL has expired or the entry is absent
   * @throws IllegalStateException if soft expire is disabled
   */
  public boolean isSoftExpired(Object cacheEntry) {
    if (!isSoftExpireEnabled()) {
      throw new IllegalStateException(
        "CacheExpireManager soft expire is disabled, isSoftExpired() should not be called"
      );
    }
    if (cacheEntry instanceof CacheEntry ce) {
      long expireAt = ce.getSoftExpireAtMs();
      return expireAt <= 0 || expireAt < TimeSource.currentTimeMillis();
    }
    return true;
  }

  /**
   * Triggers an asynchronous background refresh for the given cache key if the
   * current entry has reached its soft expiry threshold. The caller (typically
   * {@link io.github.hyshmily.hotkey.cache.HotKeyCache#getWithSoftExpire HotKeyCache.getWithSoftExpire}) has already returned the stale value to the
   * client, so this method executes entirely in the background without blocking
   * the caller.
   *
   * <p><b>Concurrency:</b> uses {@link ConcurrentHashMap#compute} on
   * {@code pendingRefreshes} to atomically decide whether a new refresh task
   * should be launched. This eliminates the earlier DCL (double-checked locking)
   * pattern that required manual clean-up of a placeholder
   * {@link CompletableFuture} when the limiter rejected the task or the executor
   * was saturated.
   *
   * @param cacheKey  the key whose value should be refreshed
   * @param reader    the data-source supplier
   * @param softTtlMs the soft TTL to set on the refreshed entry (milliseconds)
   */
  public void triggerBackgroundRefresh(String cacheKey, Supplier<?> reader, long softTtlMs) {
    if (!isSoftExpireEnabled()) {
      return;
    }
    pendingRefreshes.compute(cacheKey, (k, existing) -> {
      // If there is already an in-flight refresh for this key, keep the
      // existing future and do nothing.
      if (existing != null && !existing.isDone()) {
        return existing;
      }

      // Try to acquire a permit from the global refresh limiter.
      if (!refreshLimiter.tryAcquire()) {
        log.debug("Refresh limiter blocked, skip background refresh: {}", cacheKey);
        // Returning null removes any previous entry, leaving no stale marker.
        return null;
      }

      // Snapshot the current data version so that we can detect whether a
      // newer write has superseded the cache entry while this refresh was
      // in-flight.
      long refreshStartDataVersion = Optional.ofNullable(caffeineCache.getIfPresent(cacheKey))
        .filter(CacheEntry.class::isInstance)
        .map(CacheEntry.class::cast)
        .map(CacheEntry::getDataVersion)
        .orElse(VERSION_DEFAULT);

      // Build the async refresh task with timeout protection.
      CompletableFuture<?> task;
      try {
        task = CompletableFuture.supplyAsync(reader, executor).orTimeout(refreshTimeoutSeconds, TimeUnit.SECONDS);
      } catch (RejectedExecutionException e) {
        // Executor saturated – release the limiter permit and leave no
        // pending marker so the next read can retry immediately.
        refreshLimiter.release();
        log.warn("Background refresh rejected by executor (saturated), key={}", cacheKey);
        return null;
      }

      // When the refresh completes (success, failure or timeout), update the
      // cache entry if the value is still applicable, and always release the
      // resources.
      task.whenComplete((value, error) -> {
        try {
          if (error != null) {
            if (error instanceof TimeoutException) {
              log.warn("Background soft refresh timed out after {}s: {}", refreshTimeoutSeconds, cacheKey);
            } else {
              log.warn("Background soft refresh failed: {}", cacheKey, error);
            }
            return;
          }
          if (value != null) {
            caffeineCache
              .asMap()
              .compute(cacheKey, (key, existingEntry) ->
                Optional.ofNullable(existingEntry)
                  .filter(CacheEntry.class::isInstance)
                  .map(CacheEntry.class::cast)
                  .map(entry -> {
                    // Version guard: if a newer write has
                    // arrived while we were refreshing,
                    // discard the stale refresh result.
                    if (entry.getDataVersion() > refreshStartDataVersion) {
                      log.debug("Async refresh discarded: newer version exists: {}", cacheKey);
                      return entry;
                    }
                    return entry
                      .toBuilder()
                      .value(value)
                      .softTtlMs(softTtlMs)
                      .softExpireAtMs(computeSoftExpireAt(softTtlMs))
                      .build();
                  })
                  .orElseGet(() ->
                    CacheEntry.builder()
                      .value(value)
                      .dataVersion(VERSION_DEFAULT)
                      .isVersionDegraded(false)
                      .decisionVersion(0L)
                      .hardTtlMs(0L)
                      .hardExpireAtMs(Long.MAX_VALUE)
                      .softTtlMs(softTtlMs)
                      .softExpireAtMs(computeSoftExpireAt(softTtlMs))
                      .keyState(KeyState.NORMAL)
                      .normalHardTtlMs(0L)
                      .normalSoftTtlMs(0L)
                      .build()
                  )
              );
          }
        } finally {
          // Always release the limiter permit and remove the in-flight
          // marker so that a future refresh can be scheduled.
          refreshLimiter.release();
          pendingRefreshes.remove(cacheKey);
        }
      });

      // Return the new task; it will be stored in the map and visible to
      // any concurrent caller for this key.
      return task;
    });
  }

  /**
   * Extend both the hard and soft expiry for a cache entry.
   *
   * <p>If the caller passes {@code 0} for either TTL, the configured default
   * hot TTL ({@link #getEffectiveHotHardTtlMs()} / {@link #getEffectiveHotSoftTtlMs()})
   * is used.
   *
   * @param cacheKey  the key whose expiry should be extended
   * @param hardTtlMs new hard TTL in milliseconds; {@code 0} to use the
   *                  configured hot hard TTL
   * @param softTtlMs new soft TTL in milliseconds; {@code 0} to use the
   *                  configured hot soft TTL
   */
  public void extendExpiry(String cacheKey, long hardTtlMs, long softTtlMs) {
    long hard = resolveEffectiveHotHard(hardTtlMs);
    long soft = resolveEffectiveHotSoft(softTtlMs);
    updateExpiry(cacheKey, hard, soft, true, true);
  }

  /**
   * Extend only the hard expiry for a cache entry, leaving the soft expiry
   * unchanged. Useful when promoting a NORMAL or COOL entry to HOT — the
   * hard TTL must be lengthened to the hot‑key value, but the existing soft
   * expiry (if any) should be preserved because it reflects a more recent
   * refresh cycle.
   *
   * <p>If the caller passes {@code 0} the configured default hot hard TTL
   * ({@link #getEffectiveHotHardTtlMs()}) is used.
   *
   * @param cacheKey  the key whose hard expiry should be extended
   * @param hardTtlMs new hard TTL in milliseconds; {@code 0} to use the
   *                  configured hot hard TTL
   */
  public void extendHardExpiry(String cacheKey, long hardTtlMs) {
    long hard = resolveEffectiveHotHard(hardTtlMs);
    updateExpiry(cacheKey, hard, 0, true, false);
  }

  /**
   * Extend only the soft expiry for a cache entry, leaving the hard expiry
   * unchanged. Useful when a background refresh has completed and the caller
   * wants to reset the soft TTL without affecting the hard TTL.
   *
   * <p>If the caller passes {@code 0} the configured default hot soft TTL
   * ({@link #getEffectiveHotSoftTtlMs()}) is used.
   *
   * @param cacheKey  the key whose soft expiry should be extended
   * @param softTtlMs new soft TTL in milliseconds; {@code 0} to use the
   *                  configured hot soft TTL
   */
  public void extendSoftExpiry(String cacheKey, long softTtlMs) {
    long soft = resolveEffectiveHotSoft(softTtlMs);
    updateExpiry(cacheKey, 0, soft, false, true);
  }

  /**
   * Atomically update the expiry timestamps of an existing cache entry.
   *
   * @param cacheKey    the key whose expiry should be extended
   * @param hardTtlMs   new hard TTL in milliseconds (ignored if {@code updateHard} is false)
   * @param softTtlMs   new soft TTL in milliseconds (ignored if {@code updateSoft} is false)
   * @param updateHard  whether to update the hard expiry timestamp
   * @param updateSoft  whether to update the soft expiry timestamp
   */
  private void updateExpiry(String cacheKey, long hardTtlMs, long softTtlMs, boolean updateHard, boolean updateSoft) {
    long hardExpire = updateHard ? computeHardExpireAt(hardTtlMs) : 0;
    long softExpire = updateSoft ? computeSoftExpireAt(softTtlMs) : 0;

    caffeineCache
      .asMap()
      .computeIfPresent(cacheKey, (k, existing) -> {
        if (existing instanceof CacheEntry entry) {
          if (updateHard) {
            entry = entry.toBuilder().hardTtlMs(hardTtlMs).hardExpireAtMs(hardExpire).build();
          }
          if (updateSoft) {
            entry = entry.toBuilder().softTtlMs(softTtlMs).softExpireAtMs(softExpire).build();
          }
          return entry;
        }
        return existing;
      });
  }
}
