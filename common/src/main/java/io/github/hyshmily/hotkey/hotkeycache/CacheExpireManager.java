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
package io.github.hyshmily.hotkey.hotkeycache;

import static io.github.hyshmily.hotkey.constant.HotKeyConstants.VERSION_DEFAULT;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.entity.CacheEntry;
import io.github.hyshmily.hotkey.entity.KeyState;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages hard and soft TTL computation for {@link CacheEntry} instances.
 * <p>
 * Hard TTL controls Caffeine eviction; soft TTL controls stale-while-revalidate background refresh.
 * Each has a normal-key and hot-key variant, with an optional override taking precedence over the default.
 */
@Slf4j
@Getter
public class CacheExpireManager {

  private final Cache<String, Object> caffeineCache;
  private final Executor executor;
  private final HotKeyProperties ttlConfig;
  private final Semaphore refreshLimiter;

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
  }

  /** Whether any soft TTL is configured (normal or hot). */
  public boolean isSoftExpireEnabled() {
    return ttlConfig.isSoftExpireEnabled();
  }

  /**
   * Hard expire timestamp from an explicit TTL duration.
   * Falls back to the normal-key default if {@code hardTtlMs <= 0}.
   */
  public long computeHardExpireAt(long hardTtlMs) {
    long effective = hardTtlMs > 0 ? hardTtlMs : ttlConfig.effectiveHardTtlMs();
    return toExpireTimestamp(effective);
  }

  /**
   * Hard expire timestamp for hot keys, using {@code default-hot-hard-ttl} / {@code hot-hard-ttl}.
   * Returns {@code Long.MAX_VALUE} if hot hard expire is disabled (TTL &lt;= 0).
   */
  public long computeHotHardExpireAt() {
    return toExpireTimestamp(ttlConfig.effectiveHotHardTtlMs());
  }

  /** Soft expire timestamp for hot keys, using {@code default-hot-soft-ttl} / {@code hot-soft-ttl}. Returns 0 if disabled. */
  public long computeHotSoftExpireAt() {
    return toSoftExpireTimestamp(ttlConfig.effectiveHotSoftTtlMs());
  }

  /**
   * Soft expire timestamp from an explicit TTL duration.
   * Falls back to the normal-key default if {@code softTtlMs <= 0}. Returns 0 if soft expire is disabled.
   */
  public long computeSoftExpireAt(long softTtlMs) {
    if (!isSoftExpireEnabled()) {
      return 0L;
    }
    long effective = softTtlMs > 0 ? softTtlMs : ttlConfig.effectiveSoftTtlMs();
    return toSoftExpireTimestamp(effective);
  }

  /** Effective hard TTL for normal keys (override > default). */
  public long getEffectiveHardTtlMs() {
    return ttlConfig.effectiveHardTtlMs();
  }

  /** Effective hard TTL for hot keys (override > default). */
  public long getEffectiveHotHardTtlMs() {
    return ttlConfig.effectiveHotHardTtlMs();
  }

  /** Effective soft TTL for normal keys (override > default). */
  public long getEffectiveSoftTtlMs() {
    return ttlConfig.effectiveSoftTtlMs();
  }

  /** Effective soft TTL for hot keys (override > default). */
  public long getEffectiveHotSoftTtlMs() {
    return ttlConfig.effectiveHotSoftTtlMs();
  }

  static long toExpireTimestamp(long ttlMs) {
    if (ttlMs == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    return ttlMs > 0 ? System.currentTimeMillis() + ttlMs : Long.MAX_VALUE;
  }

  private long toSoftExpireTimestamp(long softTtlMs) {
    if (!isSoftExpireEnabled() || softTtlMs <= 0) {
      return 0L;
    }
    if (softTtlMs == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    return System.currentTimeMillis() + softTtlMs;
  }

  /**
   * Check whether the given key's soft TTL has expired.
   *
   * @throws IllegalStateException if soft expire is disabled
   */
  public boolean isSoftExpired(String cacheKey) {
    if (!isSoftExpireEnabled()) {
      throw new IllegalStateException(
        "CacheExpireManager soft expire is disabled, isSoftExpired() should not be called"
      );
    }
    Object cacheEntry = caffeineCache.getIfPresent(cacheKey);
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
   * Acquired permits are released after the refresh completes (success or failure).
   * Skipped silently if the refresh limiter is exhausted or soft expire is disabled.
   */
  public void triggerBackgroundRefresh(String cacheKey, Supplier<?> reader, long softTtlMs) {
    if (!isSoftExpireEnabled() || !refreshLimiter.tryAcquire()) {
      if (isSoftExpireEnabled()) {
        log.debug("Refresh limiter blocked, skip background refresh: {}", cacheKey);
      }
      return;
    }

    long refreshStartDataVersion = Optional.ofNullable(caffeineCache.getIfPresent(cacheKey))
      .filter(CacheEntry.class::isInstance)
      .map(CacheEntry.class::cast)
      .map(CacheEntry::getDataVersion)
      .orElse(VERSION_DEFAULT);

    CompletableFuture.supplyAsync(reader, executor).whenComplete((value, error) -> {
      try {
        if (error != null) {
          log.warn("Background soft refresh failed: {}", cacheKey, error);
          return;
        }
        if (value != null) {
          caffeineCache
            .asMap()
            .compute(cacheKey, (_, existing) ->
              Optional.ofNullable(existing)
                .filter(CacheEntry.class::isInstance)
                .map(CacheEntry.class::cast)
                .map(cacheEntry -> {
                  if (cacheEntry.getDataVersion() > refreshStartDataVersion) {
                    log.debug("Async refresh discarded: newer version exists: {}", cacheKey);
                    return cacheEntry;
                  }

                  return CacheEntry.builder()
                    .value(value)
                    .dataVersion(cacheEntry.getDataVersion())
                    .isVersionDegraded(cacheEntry.isVersionDegraded())
                    .decisionVersion(cacheEntry.getDecisionVersion())
                    .hardTtlMs(cacheEntry.getHardTtlMs())
                    .hardExpireAtMs(cacheEntry.getHardExpireAtMs())
                    .softTtlMs(softTtlMs)
                    .softExpireAtMs(computeSoftExpireAt(softTtlMs))
                    .keyState(cacheEntry.getKeyState())
                    .normalHardTtlMs(cacheEntry.getNormalHardTtlMs())
                    .normalSoftTtlMs(cacheEntry.getNormalSoftTtlMs())
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
      }
    });
  }
}
