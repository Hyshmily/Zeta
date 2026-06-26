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
package io.github.hyshmily.hotkey.cache;

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.VERSION_DEFAULT;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.autoconfigure.HotKeyProperties;
import io.github.hyshmily.hotkey.model.CacheEntry;
import io.github.hyshmily.hotkey.model.KeyState;
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
  private final ConcurrentHashMap<String, CompletableFuture<Void>> pendingRefreshes = new ConcurrentHashMap<>();
  /** Whether TTL jitter is enabled (from config). */
  private final boolean ttlJitterEnabled;
  /** Jitter ratio applied to TTLs to prevent cache stampedes (from config, e.g. 0.1 = ±10%). */
  private final double ttlJitterRatio;

  private static final long refreshTimeoutSeconds = 30;

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
    this.ttlJitterEnabled = ttlConfig.isTtlJitterEnabled();
    this.ttlJitterRatio = ttlConfig.getTtlJitterRatio();
  }

  /**
   * Create a CacheExpireManager with explicit jitter control (for testing).
   */
  CacheExpireManager(
    Cache<String, Object> caffeineCache,
    Executor executor,
    HotKeyProperties ttlConfig,
    int refreshMaxPools,
    boolean ttlJitterEnabled,
    double ttlJitterRatio
  ) {
    this.caffeineCache = caffeineCache;
    this.executor = executor;
    this.ttlConfig = ttlConfig;
    this.refreshLimiter = ttlConfig.isSoftExpireEnabled()
      ? new Semaphore(refreshMaxPools > 0 ? refreshMaxPools : 100)
      : null;
    this.ttlJitterEnabled = ttlJitterEnabled;
    this.ttlJitterRatio = ttlJitterRatio;
  }

  /**
   * Whether any soft TTL is configured (normal or hot).
   *
   * @return {@code true} if soft expire is enabled in the configuration
   */
  public boolean isSoftExpireEnabled() {
    return ttlConfig.isSoftExpireEnabled();
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
    long effective = hardTtlMs > 0 ? hardTtlMs : ttlConfig.effectiveHardTtlMs();
    return toHardExpireTimestamp(effective);
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
    long effective = softTtlMs > 0 ? softTtlMs : ttlConfig.effectiveSoftTtlMs();
    return toSoftExpireTimestamp(effective);
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
   * Effective hard TTL for hot keys (override > default).
   *
   * @return effective hot hard TTL duration in milliseconds
   */
  public long getEffectiveHotHardTtlMs() {
    return ttlConfig.effectiveHotHardTtlMs();
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
   * Effective soft TTL for hot keys (override > default).
   *
   * @return effective hot soft TTL duration in milliseconds
   */
  public long getEffectiveHotSoftTtlMs() {
    return ttlConfig.effectiveHotSoftTtlMs();
  }

  /**
   * Convert a TTL duration (ms) to an absolute epoch-ms expiration timestamp.
   * Propagates {@link Long#MAX_VALUE} unchanged — used to signal permanent entries
   * (pure logical expiry with no hard TTL eviction).
   */
  private long toHardExpireTimestamp(long hardTtlMs) {
    if (hardTtlMs == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    long jitter = ttlJitterEnabled
      ? (long) (hardTtlMs * ttlJitterRatio * ThreadLocalRandom.current().nextDouble(-1.0, 1.0))
      : 0L;

    return hardTtlMs > 0 ? System.currentTimeMillis() + Math.max(1, hardTtlMs + jitter) : Long.MAX_VALUE;
  }

  /**
   * Convert a soft TTL duration (ms) to an absolute epoch-ms expiration timestamp.
   * Applies configurable jitter (default ±10%) to prevent cache stampedes.
   * Returns 0 if soft expire is disabled or the TTL is non-positive.
   *
   * @param softTtlMs the soft TTL duration in milliseconds
   * @return absolute epoch-ms timestamp for soft expiry, or 0 if disabled
   */
  private long toSoftExpireTimestamp(long softTtlMs) {
    if (!isSoftExpireEnabled() || softTtlMs <= 0) {
      return 0L;
    }
    if (softTtlMs == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    long jitter = ttlJitterEnabled
      ? (long) (softTtlMs * ttlJitterRatio * ThreadLocalRandom.current().nextDouble(-1.0, 1.0))
      : 0L;

    return System.currentTimeMillis() + Math.max(1, softTtlMs + jitter);
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
      return expireAt <= 0 || expireAt < System.currentTimeMillis();
    }
    return true;
  }

  /**
   * Trigger an async background refresh for the given key.
   * The refreshed entry preserves the existing {@code dataVersion},
   * {@code isVersionDegraded}, and {@code decisionVersion} to avoid
   * overwriting newer data or Worker decisions.
   * <p>Per-key dedup via {@link #pendingRefreshes} prevents concurrent refreshes
   * for the same key (TOCTOU stampede protection).  Rate-limited by a global
   * semaphore to bound total concurrent refreshes across all keys.
   * Skipped silently if the refresh limiter is exhausted or soft expire is disabled.
   *
   * @param cacheKey  the key to refresh in the background
   * @param reader    the value supplier for generating the refreshed value
   * @param softTtlMs the soft TTL duration in milliseconds applied to the refreshed entry
   */
  public void triggerBackgroundRefresh(String cacheKey, Supplier<?> reader, long softTtlMs) {
    if (!isSoftExpireEnabled()) {
      return;
    }

    // Per-key dedup: fast path — skip if a refresh is already in-flight for this key
    CompletableFuture<Void> inflight = pendingRefreshes.get(cacheKey);
    if (inflight != null && !inflight.isDone()) {
      return;
    }

    // Pre-check pendingRefreshes again under the imminent putIfAbsent race window
    // to avoid wasting a semaphore permit on a loser of per-key dedup.
    CompletableFuture<Void> marker = new CompletableFuture<>();
    CompletableFuture<Void> race = pendingRefreshes.putIfAbsent(cacheKey, marker);
    if (race != null) {
      return;
    }

    // refreshLimiter is null when soft expire is disabled; isSoftExpireEnabled() above
    // guarantees it is non-null here.
    if (!refreshLimiter.tryAcquire()) {
      pendingRefreshes.remove(cacheKey);
      marker.complete(null);
      log.debug("Refresh limiter blocked, skip background refresh: {}", cacheKey);
      return;
    }

    long refreshStartDataVersion = Optional.ofNullable(caffeineCache.getIfPresent(cacheKey))
      .filter(CacheEntry.class::isInstance)
      .map(CacheEntry.class::cast)
      .map(CacheEntry::getDataVersion)
      .orElse(VERSION_DEFAULT);

    // Wrap the async refresh with a timeout guard so a stuck supplier never leaks
    // a pending refresh marker. The orTimeout future is always wrapped because the
    // orTimeout call itself may throw RejectedExecutionException when the executor
    // is saturated — we catch that separately below.
    CompletableFuture<?> completableFuture = new CompletableFuture<>();

    try {
      completableFuture = CompletableFuture.supplyAsync(reader, executor).orTimeout(
        refreshTimeoutSeconds,
        TimeUnit.SECONDS
      );
    } catch (RejectedExecutionException e) {
      // Executor saturated — release the limiter permit and the pending-refresh
      // slot so the next read can retry immediately instead of being blocked forever.
      refreshLimiter.release();
      pendingRefreshes.remove(cacheKey);
      marker.complete(null);
      log.warn("Background refresh rejected by executor (saturated), key={}", cacheKey);
    }

    completableFuture.whenComplete((value, error) -> {
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
            .compute(cacheKey, (k, existing) ->
              Optional.ofNullable(existing)
                .filter(CacheEntry.class::isInstance)
                .map(CacheEntry.class::cast)
                .map(cacheEntry -> {
                  if (cacheEntry.getDataVersion() > refreshStartDataVersion) {
                    log.debug("Async refresh discarded: newer version exists: {}", cacheKey);
                    return cacheEntry;
                  }
                  return cacheEntry
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
        refreshLimiter.release();
        pendingRefreshes.remove(cacheKey);
        marker.complete(null);
      }
    });
  }
}
