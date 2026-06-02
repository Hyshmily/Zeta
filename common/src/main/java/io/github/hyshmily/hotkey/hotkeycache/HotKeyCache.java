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

import static io.github.hyshmily.hotkey.hotkeycache.CacheKeysPolicy.invalidCacheKey;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.broadcast.CacheSyncPublisher;
import io.github.hyshmily.hotkey.constant.HotKeyConstants;
import io.github.hyshmily.hotkey.entity.CacheEntry;
import io.github.hyshmily.hotkey.entity.KeyState;
import io.github.hyshmily.hotkey.report.HotKeyReporter;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

/**
 * Core orchestration class for hot-key caching.
 * <p>
 * Manages L1 (Caffeine) operations with hot-key awareness, dual version tracking
 * ({@code dataVersion} for data mutation ordering, {@code decisionVersion} for
 * Worker HOT/COOL decision ordering),
 * SingleFlight deduplication, soft-expire (stale-while-revalidate), and
 * cross-instance synchronization via {@link CacheSyncPublisher}.
 * All write operations are transaction-aware via {@link TransactionSupport}.
 */
@Slf4j
@RequiredArgsConstructor
public class HotKeyCache {

  private final TopK hotKeyDetector;
  private final Cache<String, Object> caffeineCache;
  private final SingleFlight singleFlight;
  private final CacheExpireManager expireManager;
  private final Executor hotKeyExecutor;
  private final Optional<CacheSyncPublisher> syncPublisher;
  private final Optional<StringRedisTemplate> redisTemplate;
  private final int versionKeyTtlMinutes;
  private final Optional<HotKeyReporter> hotKeyReporter;
  private final Set<String> blacklist = ConcurrentHashMap.newKeySet();

  /**
   * Monotonically increasing counter for fallback versions.
   * Only used when {@link #redisTemplate} is empty or Redis INCR fails.
   */
  private final AtomicLong fallbackVersionCounter = new AtomicLong(0);

  private static final String NO_SYNC_PUBLISHER = HotKeyConstants.NO_SYNC_PUBLISHER;

  /**
   * Check whether a {@link CacheEntry} has logically expired based on its
   * {@code hardExpireAtMs}.  Entries with {@code hardExpireAtMs == Long.MAX_VALUE}
   * are treated as permanent (never logically expire).
   *
   * @param entry the cache entry to inspect
   * @return {@code true} if the entry has logically expired
   */
  static boolean isLogicallyExpired(CacheEntry entry) {
    return entry.getHardExpireAtMs() != Long.MAX_VALUE && System.currentTimeMillis() >= entry.getHardExpireAtMs();
  }

  /**
   * Check whether a key is currently tracked as a hot key in L1.
   *
   * @param cacheKey the key to inspect
   * @return {@code true} if the key exists in L1 with {@link KeyState#HOT}
   */
  public boolean isHotKey(String cacheKey) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("isHotKey: invalid cacheKey");
      return false;
    }
    Object entry = caffeineCache.getIfPresent(cacheKey);
    if (entry instanceof CacheEntry ce) {
      if (isLogicallyExpired(ce)) {
        return false;
      }
      return KeyState.HOT == ce.getKeyState();
    }
    return false;
  }

  /**
   * Look up a cached value without loading or triggering hot-key detection.
   * Unlike {@link #get}, this method never invokes the reader or SingleFlight.
   *
   * @param cacheKey the key to inspect
   * @return an {@link Optional} containing the raw value if present
   */
  @SuppressWarnings("unchecked")
  public <T> Optional<T> peek(String cacheKey) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("peek: invalid cacheKey");
      return Optional.empty();
    }
    return Optional.ofNullable(caffeineCache.getIfPresent(cacheKey)).map(raw ->
      raw instanceof CacheEntry vv ? (T) vv.getValue() : (T) raw
    );
  }

  /**
   * Get a value from L1 or load it via the reader.
   * Hot keys are promoted to L1 with configured hot TTLs; normal keys use default TTLs.
   */
  public <T> Optional<T> get(String cacheKey, Supplier<T> reader) {
    return get(cacheKey, reader, 0L, 0L);
  }

  /**
   * Get with explicit TTL overrides.
   * Pass 0 to use the configured default for that TTL type.
   *
   * @param hardTtlMs hard TTL override (0 = use configured default)
   * @param softTtlMs soft TTL override (0 = use configured default)
   */
  @SuppressWarnings("unchecked")
  public <T> Optional<T> get(String cacheKey, Supplier<T> reader, long hardTtlMs, long softTtlMs) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("get: invalid cacheKey");
      return Optional.empty();
    }

    return Optional.ofNullable(caffeineCache.getIfPresent(cacheKey))
      .flatMap(raw -> {
        if (raw instanceof CacheEntry ce && isLogicallyExpired(ce)) {
          caffeineCache.invalidate(cacheKey);
          log.debug("get: logically expired, reloading: {}", cacheKey);
          return Optional.empty();
        }
        T val = raw instanceof CacheEntry vv ? (T) vv.getValue() : (T) raw;

        hotKeyDetector.add(cacheKey, HotKeyConstants.TOPK_INCR);
        hotKeyReporter.ifPresent(r -> r.record(cacheKey));

        return Optional.of(val);
      })
      .or(() -> loadAndCache(cacheKey, reader, hardTtlMs, softTtlMs));
  }

  /**
   * Get with soft-expire (stale-while-revalidate). Returns cached value immediately
   * even if soft TTL expired, while triggering async refresh in background.
   * Only HOT and COOL entries are subject to soft expire.
   */
  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader) {
    return getWithSoftExpire(cacheKey, reader, 0L, 0L);
  }

  /**
   * Get with soft-expire and explicit soft TTL override.
   *
   * @param softTtlMs soft TTL override (0 = use configured default)
   */
  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader, long softTtlMs) {
    return getWithSoftExpire(cacheKey, reader, 0L, softTtlMs);
  }

  /**
   * Get with soft-expire and explicit hard/soft TTL overrides.
   *
   * @param hardTtlMs hard TTL override (0 = use configured default)
   * @param softTtlMs soft TTL override (0 = use configured default)
   */
  @SuppressWarnings("unchecked")
  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader, long hardTtlMs, long softTtlMs) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("getWithSoftExpire: invalid cacheKey");
      return Optional.empty();
    }
    if (!expireManager.isSoftExpireEnabled()) {
      log.debug("getWithSoftExpire: soft expire not enabled, fallback to get()");
      return get(cacheKey, reader, hardTtlMs, softTtlMs);
    }
    Object raw = caffeineCache.getIfPresent(cacheKey);
    return Optional.ofNullable(raw)
      .flatMap(v -> {
        if (v instanceof CacheEntry ce && isLogicallyExpired(ce)) {
          caffeineCache.invalidate(cacheKey);
          log.debug("getWithSoftExpire: logically expired, reloading: {}", cacheKey);
          return Optional.empty();
        }

        T cached = v instanceof CacheEntry vv ? (T) vv.getValue() : (T) v;

        if (
          v instanceof CacheEntry cacheEntry &&
          (KeyState.HOT == cacheEntry.getKeyState() || KeyState.COOL == cacheEntry.getKeyState())
        ) {
          if (expireManager.isSoftExpired(cacheKey)) {
            long effectiveSoft =
              softTtlMs > 0
                ? softTtlMs
                : (KeyState.HOT == cacheEntry.getKeyState()
                    ? expireManager.getEffectiveHotSoftTtlMs()
                    : cacheEntry.getNormalSoftTtlMs() > 0
                      ? cacheEntry.getNormalSoftTtlMs()
                      : expireManager.getEffectiveSoftTtlMs());

            expireManager.triggerBackgroundRefresh(cacheKey, reader, effectiveSoft);
          }
        }
        hotKeyDetector.add(cacheKey, HotKeyConstants.TOPK_INCR);
        hotKeyReporter.ifPresent(r -> r.record(cacheKey));

        return Optional.of(cached);
      })
      .or(() -> loadAndCache(cacheKey, reader, hardTtlMs, softTtlMs));
  }

  /**
   * Load via SingleFlight, detect hot key, cache with HOT or NORMAL TTL, and return value.
   *
   * @param cacheKey  the key to load
   * @param reader    the value supplier
   * @param hardTtlMs hard TTL override (0 = use configured default)
   * @param softTtlMs soft TTL override (0 = use configured default)
   */
  private <T> Optional<T> loadAndCache(String cacheKey, Supplier<T> reader, long hardTtlMs, long softTtlMs) {
    return singleFlight
      .load(cacheKey, reader)
      .map(value -> {
        long effectiveHard = hardTtlMs > 0 ? hardTtlMs : expireManager.getEffectiveHardTtlMs();
        long effectiveSoft = softTtlMs > 0 ? softTtlMs : expireManager.getEffectiveSoftTtlMs();

        if (hotKeyDetector.add(cacheKey, HotKeyConstants.TOPK_INCR).isHotKey()) {
          long hotHard = hardTtlMs > 0 ? hardTtlMs : expireManager.getEffectiveHotHardTtlMs();
          long hotSoft = softTtlMs > 0 ? softTtlMs : expireManager.getEffectiveHotSoftTtlMs();

          caffeineCache.put(
            cacheKey,
            CacheEntry.builder()
              .value(value)
              .dataVersion(HotKeyConstants.VERSION_DEFAULT)
              .isVersionDegraded(false)
              .decisionVersion(0L)
              .hardTtlMs(hotHard)
              .hardExpireAtMs(expireManager.computeHardExpireAt(hotHard))
              .softTtlMs(hotSoft)
              .softExpireAtMs(expireManager.computeSoftExpireAt(hotSoft))
              .keyState(KeyState.HOT)
              .normalHardTtlMs(effectiveHard)
              .normalSoftTtlMs(effectiveSoft)
              .build()
          );
          hotKeyReporter.ifPresent(r -> r.record(cacheKey));
          log.debug("HotKey detected, promoted to L1 and reported: {}", cacheKey);
        } else {
          caffeineCache.put(
            cacheKey,
            CacheEntry.builder()
              .value(value)
              .dataVersion(HotKeyConstants.VERSION_DEFAULT)
              .isVersionDegraded(false)
              .decisionVersion(0L)
              .hardTtlMs(effectiveHard)
              .hardExpireAtMs(expireManager.computeHardExpireAt(effectiveHard))
              .softTtlMs(effectiveSoft)
              .softExpireAtMs(expireManager.computeSoftExpireAt(effectiveSoft))
              .keyState(KeyState.NORMAL)
              .normalHardTtlMs(effectiveHard)
              .normalSoftTtlMs(effectiveSoft)
              .build()
          );
          hotKeyReporter.ifPresent(r -> r.record(cacheKey));
          log.debug("Normal key, cached with configured TTL: {}", cacheKey);
        }
        return value;
      });
  }

  /**
   * Invalidate a single key from L1 and broadcast REFRESH to peers,
   * so they reload the latest value from Redis.
   * The next {@link #get} will re-fetch from the reader.
   *
   * @param cacheKey the key to invalidate
   */
  public void invalidate(String cacheKey) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("invalidate: invalid cacheKey");
      return;
    }
    TransactionSupport.runNowOrAfterCommit(() -> {
      VersionResult vr = nextVersion(cacheKey);
      caffeineCache.invalidate(cacheKey);
      syncPublisher.ifPresentOrElse(
        p -> p.broadcastRefresh(cacheKey, vr.dataVersion(), vr.degraded()),
        () -> log.debug("invalidate: " + NO_SYNC_PUBLISHER)
      );
    });
  }

  /**
   * Invalidate all given keys from L1 and broadcast INVALIDATE for each.
   * Invalid keys (null or blank) are silently skipped.
   *
   * @param cacheKeys the keys to invalidate
   */
  public void invalidateAll(Collection<String> cacheKeys) {
    List<String> validKeys = cacheKeys
      .stream()
      .filter(k -> !invalidCacheKey(k))
      .toList();
    if (validKeys.isEmpty()) {
      log.debug("invalidateAll: all cacheKeys are invalid");
      return;
    }
    TransactionSupport.runNowOrAfterCommit(() -> {
      caffeineCache.invalidateAll(validKeys);
      if (syncPublisher.isPresent()) {
        CacheSyncPublisher p = syncPublisher.get();
        validKeys.forEach(key -> p.broadcastInvalidate(key, 0L, false));
        log.debug("invalidateAll: broadcast {} keys", validKeys.size());
      } else {
        log.debug("No sync publisher found, skip broadcast for {} keys", validKeys.size());
      }
    });
  }

  /**
   * Write-through: execute the writer, then update L1 and broadcast.
   * Uses effective hard/soft TTL from configuration.
   */
  public <T> void putThrough(String cacheKey, T value, Runnable writer) {
    putThrough(cacheKey, value, writer, 0L, 0L);
  }

  /**
   * Write-through with explicit TTL overrides.
   * Pass 0 to use the configured default for that TTL type.
   *
   * @param hardTtlMs hard TTL override (0 = use configured default)
   * @param softTtlMs soft TTL override (0 = use configured default, 0 if disabled)
   */
  public <T> void putThrough(String cacheKey, T value, Runnable writer, long hardTtlMs, long softTtlMs) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("putThrough: invalid cacheKey");
      return;
    }
    TransactionSupport.runAsyncAfterCommit(
      () -> {
        writer.run();
        VersionResult vr = nextVersion(cacheKey);

        long effectiveHardTtl = hardTtlMs > 0 ? hardTtlMs : expireManager.getEffectiveHardTtlMs();
        long effectiveSoftTtl = softTtlMs > 0 ? softTtlMs : expireManager.getEffectiveSoftTtlMs();

        caffeineCache.put(
          cacheKey,
          CacheEntry.builder()
            .value(value)
            .dataVersion(vr.dataVersion())
            .isVersionDegraded(vr.degraded())
            .decisionVersion(0L)
            .hardTtlMs(effectiveHardTtl)
            .hardExpireAtMs(expireManager.computeHardExpireAt(effectiveHardTtl))
            .softTtlMs(effectiveSoftTtl)
            .softExpireAtMs(expireManager.computeSoftExpireAt(effectiveSoftTtl))
            .keyState(KeyState.NORMAL)
            .normalHardTtlMs(effectiveHardTtl)
            .normalSoftTtlMs(effectiveSoftTtl)
            .build()
        );

        syncPublisher.ifPresentOrElse(
          p -> p.broadcastRefresh(cacheKey, vr.dataVersion(), vr.degraded()),
          () -> log.debug("putThrough: {}", NO_SYNC_PUBLISHER)
        );
      },
      hotKeyExecutor
    );
  }

  /**
   * Execute a mutation, then invalidate L1 and broadcast.
   * Next {@link #get} will re-fetch from the reader.
   */
  public void putBeforeInvalidate(String cacheKey, Runnable mutation) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("putBeforeInvalidate: invalid cacheKey");
      return;
    }
    TransactionSupport.runNowOrAfterCommit(() -> {
      try {
        mutation.run();
      } catch (Exception e) {
        log.error("putBeforeInvalidate failed, skip local invalidate and broadcast: {}", cacheKey, e);
        return;
      }
      VersionResult vr = nextVersion(cacheKey);
      caffeineCache.invalidate(cacheKey);

      syncPublisher.ifPresentOrElse(
        p -> p.broadcastInvalidate(cacheKey, vr.dataVersion(), vr.degraded()),
        () -> log.debug("putBeforeInvalidate: {}", NO_SYNC_PUBLISHER)
      );
    });
  }

  private record VersionResult(long dataVersion, boolean degraded) {}

  private VersionResult nextVersion(String cacheKey) {
    return redisTemplate
      .map(t -> {
        try {
          String script =
            "local v = redis.call('INCR', KEYS[1]) " +
            "if tonumber(ARGV[1]) > 0 then " +
            "    redis.call('EXPIRE', KEYS[1], ARGV[1]) " +
            "end " +
            "return v";

          Long v = t.execute(
            new DefaultRedisScript<>(script, Long.class),
            List.of(HotKeyConstants.REDIS_VERSION_KEY_PREFIX + cacheKey),
            String.valueOf(versionKeyTtlMinutes * 60L)
          );
          return new VersionResult(v, false);
        } catch (Exception e) {
          log.warn("Redis version increment failed, fallback to local counter: {}", cacheKey, e);
          return fallbackVersion();
        }
      })
      .orElse(fallbackVersion());
  }

  /**
   * Build a degraded version in negative {@code long} space so that all
   * degraded versions sort below any normal (positive) Redis INCR version.
   * This guarantees the {@code sendDeduped} numeric comparison in
   * {@link io.github.hyshmily.hotkey.broadcast.CacheSyncPublisher} correctly
   * prefers normal broadcasts over degraded ones without flag-aware logic.
   */
  private VersionResult fallbackVersion() {
    long version = Long.MIN_VALUE + fallbackVersionCounter.incrementAndGet();
    return new VersionResult(version, true);
  }
}
