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

import static io.github.hyshmily.hotkey.cache.CacheKeysPolicy.invalidCacheKey;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.github.hyshmily.hotkey.constants.HotKeyConstants;
import io.github.hyshmily.hotkey.exception.HotKeyBlockedException;
import io.github.hyshmily.hotkey.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.hotkey.model.CacheEntry;
import io.github.hyshmily.hotkey.model.HotKeyCacheStats;
import io.github.hyshmily.hotkey.model.KeyState;
import io.github.hyshmily.hotkey.reporting.HotKeyReporter;
import io.github.hyshmily.hotkey.rule.Rule;
import io.github.hyshmily.hotkey.rule.Rule.RuleAction;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import io.github.hyshmily.hotkey.sharding.RingManager;
import io.github.hyshmily.hotkey.sync.CacheSyncPublisher;
import io.github.hyshmily.hotkey.sync.ClusterHealthView;
import io.github.hyshmily.hotkey.sync.VersionController;
import jakarta.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

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
@SuppressWarnings("DuplicatedCode")
@RequiredArgsConstructor
@Slf4j
public class HotKeyCache {

  /** Local TopK detector (HotKeyDetector) for identifying hot keys. */
  private final HotKeyDetector hotKeyDetector;
  /** Underlying L1 Caffeine cache storing {@link CacheEntry} or raw values. */
  private final Cache<String, Object> caffeineCache;
  /** Deduplicator preventing concurrent in-flight loads for the same key. */
  private final SingleFlight singleFlight;
  /** Manages hard and soft TTL computation for cache entries. */
  private final CacheExpireManager expireManager;
  /** Executor for async cache operations (promotion, soft refresh). */
  private final Executor hotKeyExecutor;
  /** Optional publisher for cross-instance cache synchronization. */
  private final Optional<CacheSyncPublisher> cacheSyncPublisher;
  /** Optional reporter for app-to-Worker hot key reporting. */
  private final Optional<HotKeyReporter> hotKeyReporter;
  /** Matches cache keys against blacklist/whitelist rules. */
  private final RuleMatcher ruleMatcher;
  /** Manages data version generation for mutation ordering. */
  private final VersionController versionController;
  /** Monitors Worker cluster health for graceful degradation decisions. */
  private final RingManager ringManager;

  /** Cached view of Worker cluster health, used for COOL promotion decisions. */
  private final ClusterHealthView healthView;

  /** Log message constant when no sync publisher is available. */
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
   * Check whether an existing cache entry is managed by the Worker (HOT or COOL).
   * Worker-managed entries preserve their original normal TTLs through writes.
   *
   * @param existing the existing cache entry (maybe {@code null} or a raw value)
   * @return {@code true} if the entry is a {@link CacheEntry} with state HOT or COOL
   */
  private static boolean isWorkerManagedEntry(Object existing) {
    return (
      existing instanceof CacheEntry entry &&
      (entry.getKeyState() == KeyState.HOT || entry.getKeyState() == KeyState.COOL)
    );
  }

  /**
   * Check whether a key is currently tracked as a local hot key in L1.
   *
   * @param cacheKey the key to inspect
   * @return {@code true} if the key exists in L1 with {@link KeyState#HOT}
   */
  public boolean isLocalHotKey(String cacheKey) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("isLocalHotKey: invalid cacheKey");
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
   * Whether the given {@link KeyState} is eligible for local promotion to HOT.
   * <p>
   * {@link KeyState#NORMAL} entries are always eligible: when the local TopK
   * detects them as hot, they get promoted to HOT with longer TTLs.
   * <p>
   * {@link KeyState#COOL} entries are only eligible when no Worker shard is
   * alive ( returns {@code false}).
   * This provides graceful degradation — when the Worker cluster is unavailable,
   * the local TopK drives TTL decisions instead of preserving stale Worker verdicts.
   * Once a Worker comes back online and broadcasts a new decision, it overrides
   * the local promotion via {@code decisionVersion} comparison.
   * <p>
   * {@link KeyState#HOT} entries are never eligible — they already have the
   * longest TTLs.
   *
   * @param state the current key state of the cache entry
   * @return {@code true} if the entry may be promoted by local TopK
   */
  private boolean isPromotableState(KeyState state) {
    return state == KeyState.NORMAL || (state == KeyState.COOL && !healthView.isClusterHealthy());
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

    if (ruleMatcher.evaluateRule(cacheKey) == RuleAction.BLOCK) {
      throw new HotKeyBlockedException("HotKeyCache", cacheKey);
    }

    return Optional.ofNullable(caffeineCache.getIfPresent(cacheKey)).map(raw ->
      raw instanceof CacheEntry vv ? (T) unwrapNull(vv.getValue()) : (T) raw
    );
  }

  /**
   * Get a value from L1 or load it via the reader.
   * Hot keys are promoted to L1 with configured hot TTLs; normal keys use default TTLs.
   *
   * @param cacheKey the key to retrieve
   * @param reader   the value supplier for cache misses
   * @param <T>      the value type
   * @return an {@link Optional} containing the cached or loaded value
   * @throws HotKeyBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> get(String cacheKey, Supplier<T> reader) {
    return get(cacheKey, reader, 0L, 0L);
  }

  /**
   * Get with explicit TTL overrides.
   * Pass 0 to use the configured default for that TTL type.
   *
   * @param cacheKey  the key to retrieve
   * @param reader    the value supplier for cache misses
   * @param hardTtlMs hard TTL override (0 = use configured default; {@link Long#MAX_VALUE} for permanent entry — no hard TTL eviction)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param <T>       the value type
   * @return an {@link Optional} containing the cached or loaded value
   * @throws HotKeyBlockedException when the key matches a blacklist rule
   */
  @SuppressWarnings("unchecked")
  public <T> Optional<T> get(String cacheKey, Supplier<T> reader, long hardTtlMs, long softTtlMs) {
    if (invalidCacheKey(cacheKey)) {
      throw new IllegalArgumentException("Invalid cacheKey: " + cacheKey);
    }

    RuleAction action = ruleMatcher.evaluateRule(cacheKey);
    if (action == RuleAction.BLOCK) {
      throw new HotKeyBlockedException("HotKeyCache", cacheKey);
    }
    boolean skipReport = action == RuleAction.ALLOW_NO_REPORT;

    return Optional.ofNullable(caffeineCache.getIfPresent(cacheKey))
      .flatMap(raw -> {
        if (raw instanceof CacheEntry ce && isLogicallyExpired(ce)) {
          caffeineCache.invalidate(cacheKey);
          log.debug("get: logically expired, reloading: {}", cacheKey);
          return Optional.empty();
        }
        T val = raw instanceof CacheEntry vv ? (T) unwrapNull(vv.getValue()) : (T) raw;

        promoteLocalHotkeyIfNeeded(cacheKey, raw, val, hardTtlMs, softTtlMs);
        if (!skipReport) {
          hotKeyReporter.ifPresent(r -> r.record(cacheKey));
        }

        return Optional.ofNullable(val);
      })
      .or(() -> loadAndCache(cacheKey, reader, hardTtlMs, softTtlMs, skipReport));
  }

  /**
   * Get with soft-expire (stale-while-revalidate). Returns cached value immediately
   * even if soft TTL expired, while triggering async refresh in background.
   * Only HOT and COOL entries are subject to soft expire.
   *
   * @param cacheKey the key to retrieve
   * @param reader   the value supplier for cache misses / refreshes
   * @param <T>      the value type
   * @return an {@link Optional} containing the cached (possibly stale) or loaded value
   * @throws HotKeyBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader) {
    return getWithSoftExpire(cacheKey, reader, 0L, 0L);
  }

  /**
   * Get with soft-expire and explicit soft TTL override.
   *
   * @param cacheKey  the key to retrieve
   * @param reader    the value supplier for cache misses / refreshes
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param <T>       the value type
   * @return an {@link Optional} containing the cached (possibly stale) or loaded value
   * @throws HotKeyBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader, long softTtlMs) {
    return getWithSoftExpire(cacheKey, reader, 0L, softTtlMs);
  }

  /**
   * Get with soft-expire and explicit hard/soft TTL overrides.
   *
   * @param cacheKey  the key to retrieve
   * @param reader    the value supplier for cache misses / refreshes
   * @param hardTtlMs hard TTL override (0 = use configured default; {@link Long#MAX_VALUE} for pure logical expiry — entry never hard-evicted, only soft-expire or Caffeine {@code maximumSize})
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param <T>       the value type
   * @return an {@link Optional} containing the cached (possibly stale) or loaded value
   * @throws HotKeyBlockedException when the key matches a blacklist rule
   */
  @SuppressWarnings("unchecked")
  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader, long hardTtlMs, long softTtlMs) {
    if (invalidCacheKey(cacheKey)) {
      throw new IllegalArgumentException("Invalid cacheKey: " + cacheKey);
    }

    RuleAction action = ruleMatcher.evaluateRule(cacheKey);
    if (action == RuleAction.BLOCK) {
      throw new HotKeyBlockedException("HotKeyCache", cacheKey);
    }
    boolean skipReport = action == RuleAction.ALLOW_NO_REPORT;

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

        T cached = v instanceof CacheEntry vv ? (T) unwrapNull(vv.getValue()) : (T) v;

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

        promoteLocalHotkeyIfNeeded(cacheKey, raw, cached, hardTtlMs, softTtlMs);
        if (!skipReport) {
          hotKeyReporter.ifPresent(r -> r.record(cacheKey));
        }

        return Optional.ofNullable(cached);
      })
      .or(() -> loadAndCache(cacheKey, reader, hardTtlMs, softTtlMs, skipReport));
  }

  /**
   * Load via SingleFlight, detect hot key, cache with HOT or NORMAL TTL, and return value.
   *
   * @param cacheKey   the key to load
   * @param reader     the value supplier
   * @param hardTtlMs  hard TTL override (0 = use configured default)
   * @param softTtlMs  soft TTL override (0 = use configured default)
   * @param skipReport if {@code true}, skip reporting to Worker
   */
  private <T> Optional<T> loadAndCache(
    String cacheKey,
    Supplier<T> reader,
    long hardTtlMs,
    long softTtlMs,
    boolean skipReport
  ) {
    // Secondary check: rule may have been added after the entry check in get/getWithSoftExpire
    if (ruleMatcher.evaluateRule(cacheKey) == RuleAction.BLOCK) {
      throw new HotKeyBlockedException("HotKeyCache", cacheKey);
    }
    return singleFlight
      .load(cacheKey, reader)
      .map(value -> {
        if (ruleMatcher.evaluateRule(cacheKey) == RuleAction.BLOCK) {
          throw new HotKeyBlockedException("HotKeyCache", cacheKey);
        }

        long effectiveHard = hardTtlMs > 0 ? hardTtlMs : expireManager.getEffectiveHardTtlMs();
        long effectiveSoft = softTtlMs > 0 ? softTtlMs : expireManager.getEffectiveSoftTtlMs();

        hotKeyDetector.add(cacheKey, HotKeyConstants.TOPK_INCR);
        if (hotKeyDetector.contains(cacheKey)) {
          long hotHard = hardTtlMs > 0 ? hardTtlMs : expireManager.getEffectiveHotHardTtlMs();
          long hotSoft = softTtlMs > 0 ? softTtlMs : expireManager.getEffectiveHotSoftTtlMs();

          caffeineCache
            .asMap()
            .compute(cacheKey, (k, existing) -> {
              if (isWorkerManagedEntry(existing)) {
                return existing;
              }
              return CacheEntry.builder()
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
                .build();
            });

          if (!skipReport) {
            hotKeyReporter.ifPresent(r -> r.record(cacheKey));
          }
          log.debug("HotKey detected, promoted to L1{}: {}", skipReport ? " (no report)" : " and reported", cacheKey);
        } else {
          caffeineCache
            .asMap()
            .compute(cacheKey, (k, existing) -> {
              if (isWorkerManagedEntry(existing)) {
                return existing;
              }
              return CacheEntry.builder()
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
                .build();
            });

          if (!skipReport) {
            hotKeyReporter.ifPresent(r -> r.record(cacheKey));
          }
          log.debug("Normal key, cached with configured TTL: {}", cacheKey);
        }
        return value;
      });
  }

  /**
   * Promotes a non-hot entry to HOT state in L1 if the local TopK detector
   * now considers it a hot key. Preserves existing version and TTL metadata.
   * {@link KeyState#NORMAL} entries are always eligible; {@link KeyState#COOL}
   * entries are only eligible when all Workers are dead (graceful fallback).
   *
   * @param cacheKey  the key to promote
   * @param raw       the raw cached value (maybe a {@link CacheEntry} or a bare object)
   * @param val       the extracted value from the cache entry
   * @param hardTtlMs hard TTL override (0 = use configured hot hard TTL)
   * @param softTtlMs soft TTL override (0 = use configured hot soft TTL)
   */
  private void promoteLocalHotkeyIfNeeded(String cacheKey, Object raw, Object val, long hardTtlMs, long softTtlMs) {
    if (raw instanceof CacheEntry ce && isPromotableState(ce.getKeyState())) {
      hotKeyDetector.add(cacheKey, HotKeyConstants.TOPK_INCR);
      if (!hotKeyDetector.contains(cacheKey)) {
        return;
      }
      long hotHard = hardTtlMs > 0 ? hardTtlMs : expireManager.getEffectiveHotHardTtlMs();
      long hotSoft = softTtlMs > 0 ? softTtlMs : expireManager.getEffectiveHotSoftTtlMs();

      caffeineCache
        .asMap()
        .compute(cacheKey, (k, existing) -> {
          if (existing instanceof CacheEntry entry) {
            if (!isPromotableState(entry.getKeyState())) {
              return existing;
            }

            return entry
              .toBuilder()
              .hardTtlMs(hotHard)
              .softTtlMs(hotSoft)
              .hardExpireAtMs(expireManager.computeHardExpireAt(hotHard))
              .softExpireAtMs(expireManager.computeSoftExpireAt(hotSoft))
              .keyState(KeyState.HOT)
              .build();
          }

          return CacheEntry.builder()
            .value(val)
            .dataVersion(ce.getDataVersion())
            .isVersionDegraded(ce.isVersionDegraded())
            .decisionVersion(ce.getDecisionVersion())
            .hardTtlMs(hotHard)
            .hardExpireAtMs(expireManager.computeHardExpireAt(hotHard))
            .softTtlMs(hotSoft)
            .softExpireAtMs(expireManager.computeSoftExpireAt(hotSoft))
            .keyState(KeyState.HOT)
            .normalHardTtlMs(ce.getNormalHardTtlMs())
            .normalSoftTtlMs(ce.getNormalSoftTtlMs())
            .build();
        });
    }
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
      var vr = versionController.nextVersion(cacheKey);
      caffeineCache.invalidate(cacheKey);
      cacheSyncPublisher.ifPresentOrElse(
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
      cacheSyncPublisher.ifPresentOrElse(
        p -> p.broadcastLocalInvalidateAll(validKeys),
        () -> log.debug("No sync publisher found, skip broadcast for {} keys", validKeys.size())
      );
    });
  }

  /**
   * Write-through: execute the writer, then update L1 and broadcast.
   * Uses effective hard/soft TTL from configuration.
   *
   * @param cacheKey the key to write
   * @param value    the value to cache
   * @param writer   the data-source mutation to execute before caching
   * @param <T>      the value type
   * @throws HotKeyBlockedException when the key matches a blacklist rule
   */
  public <T> void putThrough(String cacheKey, T value, Runnable writer) {
    putThrough(cacheKey, value, writer, 0L, 0L);
  }

  /**
   * Write-through with explicit TTL overrides.
   * Pass 0 to use the configured default for that TTL type.
   *
   * @param cacheKey  the key to write
   * @param value     the value to cache
   * @param writer    the data-source mutation to execute before caching
   * @param hardTtlMs hard TTL override (0 = use configured default; {@link Long#MAX_VALUE} for permanent entry — no hard TTL eviction)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param <T>       the value type
   */
  public <T> void putThrough(String cacheKey, T value, Runnable writer, long hardTtlMs, long softTtlMs) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("putThrough: invalid cacheKey");
      return;
    }
    if (ruleMatcher.evaluateRule(cacheKey) == RuleAction.BLOCK) {
      log.debug("putThrough: blocked by rule: {}", cacheKey);
      return;
    }
    TransactionSupport.runAsyncAfterCommit(
      () -> {
        writer.run();
        var vr = versionController.nextVersion(cacheKey);

        long effectiveHardTtl = hardTtlMs > 0 ? hardTtlMs : expireManager.getEffectiveHardTtlMs();
        long effectiveSoftTtl = softTtlMs > 0 ? softTtlMs : expireManager.getEffectiveSoftTtlMs();

        caffeineCache
          .asMap()
          .compute(cacheKey, (k, existing) ->
            buildPutThroughEntry(existing, value, vr, effectiveHardTtl, effectiveSoftTtl)
          );

        cacheSyncPublisher.ifPresentOrElse(
          p -> p.broadcastRefresh(cacheKey, vr.dataVersion(), vr.degraded()),
          () -> log.debug("putThrough: {}", NO_SYNC_PUBLISHER)
        );
      },
      hotKeyExecutor
    );
  }

  /**
   * Builds a {@link CacheEntry} for a write-through operation, preserving
   * Worker-managed state and version information from the existing entry.
   *
   * @param existing          the previous cache entry (maybe {@code null} or a raw value)
   * @param value             the new value to cache
   * @param vr                the version result from the version controller
   * @param effectiveHardTtl  the effective hard TTL to use
   * @param effectiveSoftTtl  the effective soft TTL to use
   * @param <T>               the value type
   * @return a new {@link CacheEntry} with the supplied value and metadata
   */
  private <T> CacheEntry buildPutThroughEntry(
    Object existing,
    T value,
    VersionController.VersionResult vr,
    long effectiveHardTtl,
    long effectiveSoftTtl
  ) {
    KeyState state = (existing instanceof CacheEntry entry) ? entry.getKeyState() : KeyState.NORMAL;

    boolean isWorkerManaged = isWorkerManagedEntry(existing);

    long normalHardTtl = 0;
    long normalSoftTtl = 0;
    long decisionVersion = (existing instanceof CacheEntry entry) ? entry.getDecisionVersion() : 0L;

    if (existing instanceof CacheEntry) {
      normalHardTtl = isWorkerManaged ? ((CacheEntry) existing).getNormalHardTtlMs() : effectiveHardTtl;
    }
    if (existing instanceof CacheEntry) {
      normalSoftTtl = isWorkerManaged ? ((CacheEntry) existing).getNormalSoftTtlMs() : effectiveSoftTtl;
    }

    return CacheEntry.builder()
      .value(value != null ? value : NullValue.INSTANCE)
      .dataVersion(vr.dataVersion())
      .isVersionDegraded(vr.degraded())
      .decisionVersion(decisionVersion)
      .hardTtlMs(state == KeyState.HOT ? expireManager.getEffectiveHotHardTtlMs() : effectiveHardTtl)
      .hardExpireAtMs(
        state == KeyState.HOT
          ? expireManager.computeHotHardExpireAt()
          : expireManager.computeHardExpireAt(effectiveHardTtl)
      )
      .softTtlMs(state == KeyState.HOT ? expireManager.getEffectiveHotSoftTtlMs() : effectiveSoftTtl)
      .softExpireAtMs(
        state == KeyState.HOT
          ? expireManager.computeHotSoftExpireAt()
          : expireManager.computeSoftExpireAt(effectiveSoftTtl)
      )
      .keyState(state)
      .normalHardTtlMs(normalHardTtl)
      .normalSoftTtlMs(normalSoftTtl)
      .build();
  }

  /**
   * Execute a mutation, then invalidate L1 and broadcast.
   * Next {@link #get} will re-fetch from the reader.
   *
   * @param cacheKey the key to invalidate after mutation
   * @param mutation the mutation to execute
   */
  public void putBeforeInvalidate(String cacheKey, Runnable mutation) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("putBeforeInvalidate: invalid cacheKey");
      return;
    }
    if (ruleMatcher.evaluateRule(cacheKey) == RuleAction.BLOCK) {
      log.debug("putBeforeInvalidate: blocked by rule: {}", cacheKey);
      return;
    }
    TransactionSupport.runNowOrAfterCommit(() -> {
      try {
        mutation.run();
      } catch (Exception e) {
        log.error("putBeforeInvalidate failed, skip local invalidate and broadcast: {}", cacheKey, e);
        return;
      }
      var vr = versionController.nextVersion(cacheKey);
      caffeineCache.invalidate(cacheKey);

      cacheSyncPublisher.ifPresentOrElse(
        p -> p.broadcastLocalInvalidate(cacheKey, vr.dataVersion(), vr.degraded()),
        () -> log.debug("putBeforeInvalidate: {}", NO_SYNC_PUBLISHER)
      );
    });
  }

  /**
   * Add a key pattern to the blacklist.
   * Subsequent accesses to matching keys will throw {@link HotKeyBlockedException}.
   *
   * @param cacheKey the key pattern to blacklist
   */
  public void addBlacklist(String cacheKey) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("blacklist: invalid cacheKey");
      return;
    }
    TransactionSupport.runNowOrAfterCommit(() -> ruleMatcher.addRule(RuleMatcher.of(cacheKey, RuleAction.BLOCK)));
  }

  /**
   * Add a key pattern to the whitelist.
   * Matching keys are allowed but bypass app-to-Worker reporting.
   *
   * @param cacheKey the key pattern to whitelist
   */
  public void addWhitelist(String cacheKey) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("whitelist: invalid cacheKey");
      return;
    }
    TransactionSupport.runNowOrAfterCommit(() ->
      ruleMatcher.addRule(RuleMatcher.of(cacheKey, RuleAction.ALLOW_NO_REPORT))
    );
  }

  /**
   * Remove a key pattern from the blacklist.
   *
   * @param cacheKey the key pattern to remove from the blacklist
   */
  public void unBlacklist(String cacheKey) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("unblacklist: invalid cacheKey");
      return;
    }
    TransactionSupport.runNowOrAfterCommit(() -> ruleMatcher.removeRule(cacheKey, RuleAction.BLOCK));
  }

  /**
   * Remove a key pattern from the whitelist.
   *
   * @param cacheKey the key pattern to remove from the whitelist
   */
  public void unWhitelist(String cacheKey) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("unwhitelist: invalid cacheKey");
      return;
    }
    TransactionSupport.runNowOrAfterCommit(() -> ruleMatcher.removeRule(cacheKey, RuleAction.ALLOW_NO_REPORT));
  }

  /**
   * Evaluate all rules against the given key and return the first matching action.
   *
   * @param cacheKey the key to evaluate
   * @return the matching {@link RuleAction}, or {@code ALLOW} if no rule matches
   */
  public RuleAction evaluateRule(String cacheKey) {
    return ruleMatcher.evaluateRule(cacheKey);
  }

  /**
   * Return a snapshot of all current rules in evaluation order.
   *
   * @return list of rules (immutable snapshot)
   */
  public List<Rule> getAllRules() {
    return ruleMatcher.getAllRules();
  }

  /**
   * Remove all blacklist and whitelist rules.
   */
  public void clearAllRules() {
    TransactionSupport.runNowOrAfterCommit(ruleMatcher::clearRules);
  }

  /**
   * Broadcast all local rules to peer instances via the sync exchange.
   * Useful for initial synchronization when a new instance joins the cluster.
   */
  public void broadcastAllLocalRulesManually() {
    ruleMatcher.broadcastAllLocalRulesManually();
  }

  /**
   * Estimated number of entries currently in the L1 cache.
   *
   * @return best-effort estimate of the current entry count
   */
  public long estimatedSize() {
    return caffeineCache.estimatedSize();
  }

  /**
   * Return a snapshot of basic L1 cache statistics.
   * <p>
   * Hit/miss/eviction counters are populated only when Caffeine's
   * {@code recordStats()} is enabled.  {@code estimatedSize} is always
   * available.
   *
   * @return a {@link HotKeyCacheStats} record; hit/miss counters are {@code 0}
   *         if stats recording is not enabled
   */
  public HotKeyCacheStats stats() {
    CacheStats cs = caffeineCache.stats();
    return new HotKeyCacheStats(
      cs.hitCount(),
      cs.missCount(),
      cs.hitRate(),
      cs.evictionCount(),
      caffeineCache.estimatedSize()
    );
  }

  /**
   * Check whether the given key is blacklisted.
   *
   * @param cacheKey the key to check
   * @return {@code true} if a blacklist rule matches the key
   */

  public boolean isBlacklisted(String cacheKey) {
    return ruleMatcher.evaluateRule(cacheKey) == RuleAction.BLOCK;
  }

  /**
   * Check whether the given key is whitelisted (skips Worker reporting).
   *
   * @param cacheKey the key to check
   * @return {@code true} if a whitelist rule matches the key
   */
  public boolean isWhitelisted(String cacheKey) {
    return ruleMatcher.evaluateRule(cacheKey) == RuleAction.ALLOW_NO_REPORT;
  }

  /**
   * Invalidate all entries from the L1 cache without broadcasting.
   * <p>
   * This is an emergency flush — all cached values are removed immediately.
   * No cross-instance sync messages are sent.
   */
  public void invalidateAll() {
    caffeineCache.invalidateAll();
  }

  /**
   * Return the underlying Caffeine cache for direct access.
   *
   * <p>This provides access to Caffeine-specific operations such as
   * {@code asMap()}, {@code policy()}, and {@code cleanUp()}. Use with
   * caution — bypassing the HotKey orchestration layer (version tracking,
   * broadcast, expiry management) can lead to inconsistent state.
   *
   * @return the raw Caffeine {@link Cache} instance
   */
  public Cache<String, Object> getLocalCache() {
    return caffeineCache;
  }

  /**
   * Unwrap a {@link NullValue} sentinel back to {@code null}.
   * <p>
   * When called from the extraction paths of {@link #get}, {@link #getWithSoftExpire},
   * and {@link #peek}, this ensures that null values stored via the sentinel are
   * transparently returned as {@code null} to callers.
   *
   * @param value the raw value from a {@link CacheEntry}
   * @return {@code null} if the sentinel, otherwise the original value
   */
  @Nullable
  private static Object unwrapNull(@Nullable Object value) {
    return value == NullValue.INSTANCE ? null : value;
  }
}
