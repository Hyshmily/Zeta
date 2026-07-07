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

import static io.github.hyshmily.hotkey.cache.cachesupport.CacheKeysPolicy.invalidCacheKey;
import static io.github.hyshmily.hotkey.constants.HotKeyConstants.VERSION_DEFAULT;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.github.hyshmily.hotkey.Internal;
import io.github.hyshmily.hotkey.autoconfigure.HotKeyProperties;
import io.github.hyshmily.hotkey.cache.annotationsupporter.NullValue;
import io.github.hyshmily.hotkey.cache.cachesupport.ExpireManager;
import io.github.hyshmily.hotkey.cache.cachesupport.SingleFlight;
import io.github.hyshmily.hotkey.cache.cachesupport.TransactionSupport;
import io.github.hyshmily.hotkey.cache.codec.CacheCompressor;
import io.github.hyshmily.hotkey.constants.HotKeyConstants;
import io.github.hyshmily.hotkey.exception.HotKeyBlockedException;
import io.github.hyshmily.hotkey.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.hotkey.model.CacheEntry;
import io.github.hyshmily.hotkey.model.HotKeyCacheStats;
import io.github.hyshmily.hotkey.model.KeyState;
import io.github.hyshmily.hotkey.reporting.KeyReporter;
import io.github.hyshmily.hotkey.rule.Rule;
import io.github.hyshmily.hotkey.rule.Rule.RuleAction;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import io.github.hyshmily.hotkey.sharding.HealthView;
import io.github.hyshmily.hotkey.sync.local.CacheSyncPublisher;
import io.github.hyshmily.hotkey.util.TimeSource;
import io.github.hyshmily.hotkey.util.version.VersionController;
import io.github.hyshmily.hotkey.util.version.VersionGuard;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
@RequiredArgsConstructor
@Slf4j
@Internal
public class HotKeyCache {

  /** Local TopK detector (HotKeyDetector) for identifying hot keys. */
  private final HotKeyDetector hotKeyDetector;
  /** Underlying L1 Caffeine cache storing {@link CacheEntry} or raw values. */
  private final Cache<String, Object> caffeineCache;
  /** Deduplicator preventing concurrent in-flight loads for the same key. */
  private final SingleFlight singleFlight;
  /** Manages hard and soft TTL computation for cache entries. */
  private final ExpireManager expireManager;
  /** Executor for async cache operations (promotion, soft refresh). */
  private final Executor hotKeyExecutor;

  /** Optional publisher for cross-instance cache synchronization. */
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private final Optional<CacheSyncPublisher> cacheSyncPublisher;

  /** Optional reporter for app-to-Worker hot key reporting. */
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private final Optional<KeyReporter> hotKeyReporter;

  /** Matches cache keys against blacklist/whitelist rules. */
  private final RuleMatcher ruleMatcher;
  /** Manages data version generation for mutation ordering. */
  private final VersionController versionController;

  /** HotKey configuration properties (TTL overrides, null-value TTL). */
  private final HotKeyProperties hotKeyProperties;

  /** Cached view of Worker cluster health, used for COOL promotion decisions. */
  private final HealthView healthView;

  /** Compressor for L1 cache values. */
  private final CacheCompressor compressor;

  /** Log message constant when no sync publisher is available. */
  private static final String NO_SYNC_PUBLISHER = HotKeyConstants.NO_SYNC_PUBLISHER;

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
      if (expireManager.isLogicallyExpired(ce)) {
        return false;
      }
      // Also check HeavyKeeper for NORMAL entries promoted by Worker broadcast detection
      return KeyState.HOT == ce.getKeyState() || hotKeyDetector.contains(cacheKey);
    }
    // Fallback to HeavyKeeper for keys that are hot in the detection engine but not yet
    // promoted to L1 with a CacheEntry wrapper (e.g., during the first detection window).
    return hotKeyDetector.contains(cacheKey);
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

    try {
      return Optional.ofNullable(caffeineCache.getIfPresent(cacheKey)).map(raw ->
        raw instanceof CacheEntry vv ? (T) unwrapValue(vv.getValue()) : (T) raw
      );
    } catch (HotKeyBlockedException e) {
      throw e;
    } catch (RuntimeException e) {
      log.error("HotKeyCache.peek internal error for key={}, returning empty", cacheKey, e);
      return Optional.empty();
    }
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
    // Graceful degradation: invalid cache key is a caller-side issue, not a HotKey internal
    // failure. Return empty rather than throwing to keep callers operational.
    if (invalidCacheKey(cacheKey)) {
      log.debug("get: invalid cacheKey");
      return Optional.empty();
    }

    RuleAction action = ruleMatcher.evaluateRule(cacheKey);
    if (action == RuleAction.BLOCK) {
      throw new HotKeyBlockedException("HotKeyCache", cacheKey);
    }
    boolean skipReport = action == RuleAction.ALLOW_NO_REPORT;

    try {
      return Optional.ofNullable(caffeineCache.getIfPresent(cacheKey))
        .flatMap(raw -> {
          if (expireManager.invalidateIfIsLogicallyExpired(cacheKey, raw)) {
            return Optional.empty();
          }

          T val = raw instanceof CacheEntry vv ? (T) unwrapValue(vv.getValue()) : (T) raw;

          return processHitAndValidate(cacheKey, raw, val, hardTtlMs, softTtlMs, skipReport);
        })
        .or(() -> loadAndCache(cacheKey, reader, hardTtlMs, softTtlMs, skipReport));
    } catch (HotKeyBlockedException e) {
      throw e;
    } catch (RuntimeException e) {
      log.error("HotKeyCache.get internal error for key={}, returning empty to keep caller operational", cacheKey, e);
      return Optional.empty();
    }
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
  @SuppressWarnings("all")
  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader, long hardTtlMs, long softTtlMs) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("getWithSoftExpire: invalid cacheKey");
      return Optional.empty();
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
    try {
      return Optional.ofNullable(raw)
        .flatMap(v -> {
          if (expireManager.invalidateIfIsLogicallyExpired(cacheKey, v)) {
            return Optional.empty();
          }

          T cached = v instanceof CacheEntry vv ? (T) unwrapValue(vv.getValue()) : (T) v;

          if (isWorkerManagedEntry(v)) {
            CacheEntry ce = (CacheEntry) v;
            if (expireManager.isSoftExpired(ce)) {
              long effectiveSoft =
                softTtlMs > 0
                  ? softTtlMs
                  : (KeyState.HOT == ce.getKeyState()
                      ? expireManager.getEffectiveHotSoftTtlMs()
                      : ce.getNormalSoftTtlMs() > 0
                        ? ce.getNormalSoftTtlMs()
                        : expireManager.getEffectiveSoftTtlMs());

              expireManager.triggerBackgroundRefresh(cacheKey, reader, effectiveSoft);
            }
          }

          return processHitAndValidate(cacheKey, raw, cached, hardTtlMs, softTtlMs, skipReport);
        })
        .or(() -> loadAndCache(cacheKey, reader, hardTtlMs, softTtlMs, skipReport));
    } catch (HotKeyBlockedException e) {
      throw e;
    } catch (RuntimeException e) {
      log.error(
        "HotKeyCache.getWithSoftExpire internal error for key={}, returning empty to keep caller operational",
        cacheKey,
        e
      );
      return Optional.empty();
    }
  }

  /**
   * Process a cache hit: trigger local hot-key promotion/renewal and report
   * to Worker, then re-check logical expiry (TOCTOU guard) after side effects.
   * <p>Extracted from {@link #get} and {@link #getWithSoftExpire} to eliminate
   * code duplication.
   *
   * @param cacheKey  the cache key
   * @param raw       the raw value from Caffeine (may be {@link CacheEntry} or bare)
   * @param cached    the unwrapped cached value
   * @param hardTtlMs hard TTL override
   * @param softTtlMs soft TTL override
   * @param skipReport if {@code true}, skip app-to-Worker reporting
   * @param <T>       the value type
   * @return an {@link Optional} containing the cached value, or empty if logically expired
   */
  private <T> Optional<T> processHitAndValidate(
    String cacheKey,
    Object raw,
    T cached,
    long hardTtlMs,
    long softTtlMs,
    boolean skipReport
  ) {
    boolean wasProcessed = processLocalHotkeyIfNeeded(cacheKey, raw, cached, hardTtlMs, softTtlMs);
    if (!skipReport) {
      hotKeyReporter.ifPresent(r -> r.record(cacheKey));
    }

    // TOCTOU: re-read from cache after side effects (promote/record may have taken time)
    if (wasProcessed || !skipReport) {
      Object currentRaw = caffeineCache.getIfPresent(cacheKey);
      if (expireManager.invalidateIfIsLogicallyExpired(cacheKey, currentRaw)) {
        return Optional.empty();
      }
    }

    return Optional.ofNullable(cached);
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
    Optional<T> result = singleFlight.load(cacheKey, reader);

    if (result.isEmpty()) {
      return handleEmptyLoadResult(cacheKey);
    }
    T value = result.get();
    return Optional.of(mapLoadedValue(cacheKey, value, hardTtlMs, softTtlMs, skipReport));
  }

  /**
   * Handle the case when SingleFlight returns empty: either return a stale
   * entry when the circuit breaker is open, or cache a {@link NullValue}
   * sentinel with a short TTL and return {@link Optional#empty()}.
   *
   * @param cacheKey the key that was loaded (resulted in a null value)
   * @param <T>      the expected value type
   * @return a stale value if the circuit breaker is open and a cached entry
   *         exists; {@link Optional#empty()} otherwise (a NullValue sentinel
   *         is written to L1 in both cases)
   */
  @SuppressWarnings("unchecked")
  private <T> Optional<T> handleEmptyLoadResult(String cacheKey) {
    if (singleFlight.isBreakerOpen()) {
      Object stale = caffeineCache.getIfPresent(cacheKey);

      if (stale != null) {
        T val = stale instanceof CacheEntry ce ? (T) unwrapValue(ce.getValue()) : (T) stale;
        log.debug("CB open, returning stale entry for key={}", cacheKey);
        return Optional.ofNullable(val);
      }
    }
    long nullTtlMs = TimeUnit.SECONDS.toMillis(hotKeyProperties.getNullValueTtlSeconds());
    long nullExpireAtMs = expireManager.computeNullExpireAt(nullTtlMs);

    caffeineCache.put(
      cacheKey,
      expireManager.createBuilder(
        NullValue.INSTANCE,
        VERSION_DEFAULT,
        false,
        0L,
        nullTtlMs,
        0L,
        nullExpireAtMs,
        0L,
        0L,
        0L,
        KeyState.NORMAL
      )
    );
    return Optional.empty();
  }

  /**
   * Process a successfully loaded value: re-check blacklist, detect hot key via
   * HeavyKeeper, store in L1 with HOT or NORMAL TTL, and optionally report to Worker.
   *
   * @param cacheKey   the key to cache
   * @param value      the loaded value (must not be null at this point)
   * @param hardTtlMs  hard TTL override (0 = use configured default)
   * @param softTtlMs  soft TTL override (0 = use configured default)
   * @param skipReport if {@code true}, skip reporting to Worker
   * @param <T>        the value type
   * @return the loaded value (unchanged)
   */
  private <T> T mapLoadedValue(String cacheKey, T value, long hardTtlMs, long softTtlMs, boolean skipReport) {
    if (ruleMatcher.evaluateRule(cacheKey) == RuleAction.BLOCK) {
      throw new HotKeyBlockedException("HotKeyCache", cacheKey);
    }

    long effectiveHard = expireManager.resolveEffectiveHardTtl(hardTtlMs);
    long effectiveSoft = expireManager.resolveEffectiveSoftTtl(softTtlMs);

    hotKeyDetector.add(cacheKey, HotKeyConstants.TOPK_INCR);
    if (hotKeyDetector.contains(cacheKey)) {
      long hotHard = expireManager.resolveEffectiveHotHard(hardTtlMs);
      long hotSoft = expireManager.resolveEffectiveHotSoft(softTtlMs);

      storeCacheEntry(cacheKey, value, hotHard, hotSoft, KeyState.HOT, effectiveHard, effectiveSoft);
      recordIfReporting(skipReport, cacheKey);
      log.debug("HotKey detected, promoted to L1{}: {}", skipReport ? " (no report)" : " and reported", cacheKey);
    } else {
      storeCacheEntry(cacheKey, value, effectiveHard, effectiveSoft, KeyState.NORMAL, effectiveHard, effectiveSoft);
      recordIfReporting(skipReport, cacheKey);
      log.debug("Normal key, cached with configured TTL: {}", cacheKey);
    }
    return value;
  }

  /**
   * Atomically insert or update the entry in L1 via {@code compute}, skipping
   * the update if the existing entry is Worker-managed (HOT or COOL).
   *
   * @param cacheKey      the key to store
   * @param value         the value to cache
   * @param hardTtlMs     the hard TTL for this entry
   * @param softTtlMs     the soft TTL for this entry
   * @param state         the key state to assign (HOT or NORMAL)
   * @param normalHardTtl the normal (non-hot) hard TTL to preserve for demotion
   * @param normalSoftTtl the normal (non-hot) soft TTL to preserve for demotion
   */
  private void storeCacheEntry(
    String cacheKey,
    Object value,
    long hardTtlMs,
    long softTtlMs,
    KeyState state,
    long normalHardTtl,
    long normalSoftTtl
  ) {
    caffeineCache
      .asMap()
      .compute(cacheKey, (k, existing) -> {
        if (isWorkerManagedEntry(existing)) {
          return existing;
        }
        return buildEntry(value, hardTtlMs, softTtlMs, state, normalHardTtl, normalSoftTtl);
      });
  }

  /**
   * Record a key access to the {@link KeyReporter} unless reporting is
   * explicitly skipped.
   *
   * @param skipReport if {@code true}, no-op
   * @param cacheKey   the key accessed
   */
  private void recordIfReporting(boolean skipReport, String cacheKey) {
    if (!skipReport) {
      hotKeyReporter.ifPresent(r -> r.record(cacheKey));
    }
  }

  private <T> CacheEntry buildEntry(
    T value,
    long hardTtlMs,
    long softTtlMs,
    KeyState state,
    long normalHardTtlMs,
    long normalSoftTtlMs
  ) {
    long hardTtlExpireAtMs = expireManager.computeHardExpireAt(hardTtlMs);
    long softTtlExpireAtMs = softTtlMs > 0 ? expireManager.computeSoftExpireAt(softTtlMs) : 0L;

    return expireManager.createBuilder(
      value,
      VERSION_DEFAULT,
      false,
      VERSION_DEFAULT,
      hardTtlMs,
      softTtlMs,
      hardTtlExpireAtMs,
      softTtlExpireAtMs,
      normalHardTtlMs,
      normalSoftTtlMs,
      state
    );
  }

  /**
   * Process a cache hit for local hot-key management: if the entry is already
   * HOT and more than half its TTL has elapsed, extend its expiry window;
   * otherwise promote eligible non-hot entries (NORMAL or COOL-when-all-dead)
   * to HOT if the local TopK now considers them hot.
   * <p>
   * HOT entries that are still within their first half are left untouched —
   * no need to re-insert the same state.
   *
   * @param cacheKey  the key to promote
   * @param raw       the raw cached value (maybe a {@link CacheEntry} or a bare object)
   * @param val       the extracted value from the cache entry
   * @param hardTtlMs hard TTL override (0 = use configured hot hard TTL)
   * @param softTtlMs soft TTL override (0 = use configured hot soft TTL)
   * @return {@code true} if a local promotion or expiry extension occurred
   */
  private boolean processLocalHotkeyIfNeeded(String cacheKey, Object raw, Object val, long hardTtlMs, long softTtlMs) {
    if (raw instanceof CacheEntry ce && ce.getKeyState() == KeyState.HOT && ce.getHardExpireAtMs() != Long.MAX_VALUE) {
      long remainingTtl = ce.getHardExpireAtMs() - TimeSource.currentTimeMillis();
      long totalTtl = ce.getHardTtlMs();

      if (totalTtl > 0 && remainingTtl < totalTtl / 2) {
        expireManager.extendExpiry(cacheKey, hardTtlMs, softTtlMs);
        return true;
      }
      return false;
    }

    if (raw instanceof CacheEntry ce && isPromotableState(ce.getKeyState())) {
      hotKeyDetector.add(cacheKey, HotKeyConstants.TOPK_INCR);
      if (!hotKeyDetector.contains(cacheKey)) {
        return false;
      }
      long hotHard = expireManager.resolveEffectiveHotHard(hardTtlMs);
      long hotSoft = expireManager.resolveEffectiveHotSoft(softTtlMs);

      promoteEntryInCache(cacheKey, val, ce, hotHard, hotSoft);
      return true;
    }
    return false;
  }

  /**
   * Atomically promote a cache entry to HOT state in L1, respecting the
   * Worker-managed guard (entries in HOT/COOL state from the Worker are
   * left untouched).
   *
   * @param cacheKey the key to promote
   * @param val      the underlying cached value
   * @param ce       the existing {@link CacheEntry} from which metadata
   *                 (version, normal TTLs) is preserved
   * @param hotHard  the hot-entry hard TTL
   * @param hotSoft  the hot-entry soft TTL
   */
  private void promoteEntryInCache(String cacheKey, Object val, CacheEntry ce, long hotHard, long hotSoft) {
    caffeineCache
      .asMap()
      .compute(cacheKey, (k, existing) -> {
        if (existing instanceof CacheEntry entry) {
          if (!isPromotableState(entry.getKeyState())) {
            return existing;
          }

          return expireManager.applyTtl(entry, hotHard, hotSoft).toBuilder().keyState(KeyState.HOT).build();
        }

        return expireManager.createBuilder(
          val,
          ce.getDataVersion(),
          ce.isVersionDegraded(),
          ce.getDecisionVersion(),
          hotHard,
          hotSoft,
          ce.getNormalHardTtlMs(),
          ce.getNormalSoftTtlMs(),
          KeyState.HOT
        );
      });
  }

  /**
   * Invalidate a single key from L1 and broadcast INVALIDATE to peers,
   * so they remove their local copy and re-fetch on next {@link #get}.
   * The next local {@link #get} will re-fetch from the reader.
   *
   * @param cacheKey the key to invalidate
   */
  public void invalidate(String cacheKey) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("invalidate: invalid cacheKey");
      return;
    }

    TransactionSupport.runNowOrAfterCommit(() -> {
      // Local L1 must be dropped synchronously so subsequent reads see the
      // invalidation immediately, even if the executor is saturated.
      caffeineCache.invalidate(cacheKey);

      // Version bump (Redis INCR) + broadcast happen asynchronously. If
      // hotKeyExecutor rejects or Redis fails, VersionController.nextVersion
      // already falls back to a degraded local counter (ADR-0009), so the
      // broadcast still propagates with isVersionDegraded=true and peers
      // honor it via the 4-case VersionGuard comparison.
      try {
        hotKeyExecutor.execute(() -> {
          try {
            var vr = versionController.nextVersion(cacheKey);
            cacheSyncPublisher.ifPresentOrElse(
              p -> p.broadcastLocalInvalidate(cacheKey, vr.dataVersion(), vr.degraded()),
              () -> log.debug("invalidate async: " + NO_SYNC_PUBLISHER)
            );
          } catch (Exception ex) {
            log.error("invalidate async broadcast failed for key={}", cacheKey, ex);
          }
        });
      } catch (RejectedExecutionException ree) {
        log.warn(
          "invalidate executor rejected for key={}, peer invalidation deferred to next cycle: {}",
          cacheKey,
          ree.getMessage()
        );
      }
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
      log.debug("invalidateAllLocal: all cacheKeys are invalid");
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
   * Evict keys from the local cache only, without broadcasting to other
   * instances and without bumping version numbers.
   *
   * <p>Useful for emergency local cleanup, testing, or when a module is
   * taken offline and only the current node needs to be cleared.
   *
   * @param cacheKeys the keys to evict locally
   */
  public void evictLocal(Collection<String> cacheKeys) {
    List<String> validKeys = cacheKeys
      .stream()
      .filter(k -> !invalidCacheKey(k))
      .toList();
    if (validKeys.isEmpty()) {
      log.debug("evictLocal: all cacheKeys are invalid");
      return;
    }
    caffeineCache.invalidateAll(validKeys);
    log.debug("evictLocal: evicted {} keys locally", validKeys.size());
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
      throw new HotKeyBlockedException("HotKeyCache", cacheKey);
    }

    AtomicBoolean writerOk = new AtomicBoolean(false);

    TransactionSupport.runAsyncAfterCommit(
      () -> {
        try {
          writer.run();
          writerOk.compareAndSet(false, true);
        } catch (Exception e) {
          // Writer failed — do NOT cache, do NOT broadcast, do NOT bump version.
          log.error("putThrough writer failed for key={}, cache update skipped: {}", cacheKey, e.getMessage(), e);
          return;
        }

        long effectiveHardTtl = expireManager.resolveEffectiveHardTtl(hardTtlMs);
        long effectiveSoftTtl = expireManager.resolveEffectiveSoftTtl(softTtlMs);

        try {
          var vr = versionController.nextVersion(cacheKey);

          caffeineCache
            .asMap()
            .compute(cacheKey, (k, existing) ->
              buildPutThroughEntry(existing, value, vr, effectiveHardTtl, effectiveSoftTtl)
            );

          cacheSyncPublisher.ifPresentOrElse(
            p -> p.broadcastRefresh(cacheKey, vr.dataVersion(), vr.degraded()),
            () -> log.debug("putThrough: {}", NO_SYNC_PUBLISHER)
          );
        } catch (RejectedExecutionException ree) {
          // hotKeyExecutor saturated mid-body (nextVersion/Redis did run, but
          // we cannot distinguish here — VersionController.nextVersion already
          // has its own Redis-degraded fallback path). Cache and broadcast are
          // skipped; the next read or the next periodic Worker cycle will
          // reconcile. Logged at ERROR so operators can detect silent drops.
          log.error(
            "putThrough executor rejected mid-flight for key={} (local cache NOT updated, broadcast NOT sent)",
            cacheKey,
            ree
          );
          throw ree; // CompletableFuture.exceptionally downstream decides policy
        } catch (Exception e) {
          // Redis-relayed exception or unexpected error. The local L1 is still
          // updated using a degraded version so the cache remains coherent with
          // the mutation that already succeeded on the writer.
          log.error("putThrough cache/broadcast failed for key={} (applying degraded local update)", cacheKey, e);
          var vr = versionController.fallbackVersion();

          caffeineCache
            .asMap()
            .compute(cacheKey, (k, existing) ->
              buildPutThroughEntry(existing, value, vr, effectiveHardTtl, effectiveSoftTtl)
            );
          cacheSyncPublisher.ifPresent(p -> p.broadcastRefresh(cacheKey, vr.dataVersion(), vr.degraded()));
        }
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
  @SuppressWarnings("all")
  private <T> CacheEntry buildPutThroughEntry(
    Object existing,
    T value,
    VersionController.VersionResult vr,
    long effectiveHardTtl,
    long effectiveSoftTtl
  ) {
    KeyState state = (existing instanceof CacheEntry entry) ? entry.getKeyState() : KeyState.NORMAL;
    if (existing instanceof CacheEntry ce && VersionGuard.shouldSkipForSync(ce, vr.dataVersion(), vr.degraded())) {
      return ce;
    }

    boolean isWorkerManaged = isWorkerManagedEntry(existing);

    long decisionVersion = 0L;
    String decisionNodeId = null;
    long decisionEpoch = 0L;
    long normalHardTtl = effectiveHardTtl;
    long normalSoftTtl = effectiveSoftTtl;

    if (existing instanceof CacheEntry entry) {
      decisionVersion = entry.getDecisionVersion();
      decisionNodeId = entry.getDecisionNodeId();
      decisionEpoch = entry.getDecisionEpoch();
      if (isWorkerManaged) {
        normalHardTtl = entry.getNormalHardTtlMs();
        normalSoftTtl = entry.getNormalSoftTtlMs();
      }
    }

    long hardTtl = effectiveHardTtl;
    long softTtl = effectiveSoftTtl;
    if (state == KeyState.HOT) {
      hardTtl = expireManager.resolveEffectiveHotHard(hardTtl);
      softTtl = expireManager.resolveEffectiveHotSoft(softTtl);
    }

    Object wrapValue = value != null ? value : NullValue.INSTANCE;
    return expireManager.createBuilder(
      wrapValue,
      vr.dataVersion(),
      vr.degraded(),
      decisionVersion,
      decisionNodeId,
      decisionEpoch,
      hardTtl,
      softTtl,
      normalHardTtl,
      normalSoftTtl,
      state
    );
  }

  /**
   * Write a value directly into the local L1 cache without version bump,
   * without broadcast, without hot-key detection, and without reporting.
   * <p>
   * Existing entry metadata is preserved.  If no entry exists, a fresh
   * {@link CacheEntry} is created with {@link KeyState#NORMAL}.
   * <p>
   * Pass {@code 0} for either TTL to use the configured default.
   *
   * @param cacheKey  the key to store
   * @param value     the value to cache
   * @param hardTtlMs hard TTL override (0 = use configured default)
   * @param softTtlMs soft TTL override (0 = use configured default)
   */
  public void putLocal(String cacheKey, Object value, long hardTtlMs, long softTtlMs) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("putLocal: invalid cacheKey");
      return;
    }
    if (ruleMatcher.evaluateRule(cacheKey) == RuleAction.BLOCK) {
      log.debug("putLocal: blocked by rule: {}", cacheKey);
      throw new HotKeyBlockedException("HotKeyCache", cacheKey);
    }

    long hardTtl = expireManager.resolveEffectiveHardTtl(hardTtlMs);
    long softTtl = expireManager.resolveEffectiveSoftTtl(softTtlMs);

    caffeineCache
      .asMap()
      .compute(cacheKey, (k, existing) -> {
        if (existing instanceof CacheEntry ce) {
          return expireManager.applyTtl(expireManager.replaceEntryValue(ce, value), hardTtl, softTtl);
        }
        return expireManager.createBuilder(
          value,
          VERSION_DEFAULT,
          false,
          0L,
          hardTtl,
          softTtl,
          hardTtl,
          softTtl,
          KeyState.NORMAL
        );
      });
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
      throw new HotKeyBlockedException("HotKeyCache", cacheKey);
    }
    TransactionSupport.runNowOrAfterCommit(() -> {
      try {
        mutation.run();
      } catch (Exception e) {
        log.error("putBeforeInvalidate mutation failed, skip invalidate and broadcast: {}", cacheKey, e);
        return;
      }
      // Drop local L1 immediately — subsequent reads on this instance must
      // re-fetch the mutated value via the reader.
      caffeineCache.invalidate(cacheKey);

      try {
        hotKeyExecutor.execute(() -> {
          try {
            var vr = versionController.nextVersion(cacheKey);
            cacheSyncPublisher.ifPresentOrElse(
              p -> p.broadcastLocalInvalidate(cacheKey, vr.dataVersion(), vr.degraded()),
              () -> log.debug("putBeforeInvalidate async: {}", NO_SYNC_PUBLISHER)
            );
          } catch (Exception ex) {
            log.error("putBeforeInvalidate async broadcast failed for key={}", cacheKey, ex);
          }
        });
      } catch (RejectedExecutionException ree) {
        log.warn(
          "putBeforeInvalidate executor rejected for key={}, peer invalidation deferred: {}",
          cacheKey,
          ree.getMessage()
        );
      }
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
      log.debug("addBlacklist: invalid cacheKey '{}'", cacheKey);
      return;
    }
    TransactionSupport.runNowOrAfterCommit(() -> {
      ruleMatcher.addRule(RuleMatcher.of(cacheKey, RuleAction.BLOCK));
      log.info("Blacklist added: '{}'", cacheKey);
    });
  }

  /**
   * Add a key pattern to the whitelist.
   * Matching keys are allowed but bypass app-to-Worker reporting.
   *
   * @param cacheKey the key pattern to whitelist
   */
  public void addWhitelist(String cacheKey) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("addWhitelist: invalid cacheKey '{}'", cacheKey);
      return;
    }
    TransactionSupport.runNowOrAfterCommit(() -> {
      ruleMatcher.addRule(RuleMatcher.of(cacheKey, RuleAction.ALLOW_NO_REPORT));
      log.info("Whitelist added: '{}'", cacheKey);
    });
  }

  /**
   * Remove a key pattern from the blacklist.
   *
   * @param cacheKey the key pattern to remove from the blacklist
   */
  public void unBlacklist(String cacheKey) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("unBlacklist: invalid cacheKey '{}'", cacheKey);
      return;
    }
    TransactionSupport.runNowOrAfterCommit(() -> {
      if (ruleMatcher.removeRule(cacheKey, RuleAction.BLOCK)) {
        log.info("Blacklist removed: '{}'", cacheKey);
      }
    });
  }

  /**
   * Remove a key pattern from the whitelist.
   *
   * @param cacheKey the key pattern to remove from the whitelist
   */
  public void unWhitelist(String cacheKey) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("unWhitelist: invalid cacheKey '{}'", cacheKey);
      return;
    }
    TransactionSupport.runNowOrAfterCommit(() -> {
      if (ruleMatcher.removeRule(cacheKey, RuleAction.ALLOW_NO_REPORT)) {
        log.info("Whitelist removed: '{}'", cacheKey);
      }
    });
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
   * {@code recordStats()} is enabled.  {@code estimatedSizeOfKeysCount} is always
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
  public void invalidateAllLocal() {
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
   * @param stored the raw value from a {@link CacheEntry}
   * @return {@code null} if the sentinel, otherwise the original value
   */
  @Nullable
  private Object unwrapValue(@Nullable Object stored) {
    try {
      Object val = compressor.unwrap(stored);
      return val == NullValue.INSTANCE ? null : val;
    } catch (IOException e) {
      log.warn("Failed to decompress cache value", e);
      return null;
    }
  }
}
