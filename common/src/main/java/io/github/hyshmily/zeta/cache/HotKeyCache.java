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
package io.github.hyshmily.zeta.cache;

import static io.github.hyshmily.zeta.cache.cachesupport.CacheKeysPolicy.invalidCacheKey;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Version.VERSION_DEFAULT;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.annotationsupporter.NullValue;
import io.github.hyshmily.zeta.cache.cachesupport.CacheKeysPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.ExpireManager;
import io.github.hyshmily.zeta.cache.cachesupport.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.TransactionSupport;
import io.github.hyshmily.zeta.cache.codec.CacheCompressor;
import io.github.hyshmily.zeta.exception.ZetaBlockedException;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.model.ZetaCacheStats;
import io.github.hyshmily.zeta.rule.Rule;
import io.github.hyshmily.zeta.rule.Rule.RuleAction;
import io.github.hyshmily.zeta.rule.RuleMatcher;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.version.VersionController;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.Builder;
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

  /** Central dispatcher for all external communication (reportToWorker + send). */
  private final CentralDispatcher dispatcher;

  /** Matches cache keys against blacklist/whitelist rules. */
  private final RuleMatcher ruleMatcher;
  /** Manages data version generation for mutation ordering. */
  private final VersionController versionController;

  /** HotKey configuration properties (TTL overrides, null-value TTL). */
  private final ZetaProperties zetaProperties;

  /** Cached view of Worker cluster health, used for COOL promotion decisions. */
  private final HealthView healthView;

  /** Compressor for L1 cache values. */
  private final CacheCompressor compressor;

  /**
   * Resolve the TTL for {@link NullValue} sentinels.
   * Extracted to a method to fix field-initialization ordering: the
   * original inline expression referenced {@code zetaProperties} before
   * the constructor had set it.
   *
   * @return null-value TTL in milliseconds
   */
  private long nullTtlMs() {
    return TimeUnit.SECONDS.toMillis(zetaProperties.getNullValueTtlSeconds());
  }

  /**
   * Whether soft-expire (stale-while-revalidate) is globally disabled.
   * Extracted to a method for the same field-ordering reason as {@link #nullTtlMs}.
   *
   * @return {@code true} when soft expire is disabled
   */
  private boolean isSoftExpireUnenabled() {
    return !expireManager.isSoftExpireEnabled();
  }

  @Builder
  static class GuardReport {

    RuleAction action;
    boolean isSkipReport;
  }

  /**
   * Normalize a cache key if query-param stripping is enabled.
   * Delegates to {@link CacheKeysPolicy#normalizeKey} only when the
   * feature is configured; otherwise returns the key unchanged (zero overhead).
   */
  private String normalize(String cacheKey) {
    if (cacheKey != null && zetaProperties.getCacheKey().isStripQuery()) {
      return CacheKeysPolicy.normalizeKey(cacheKey);
    }
    return cacheKey;
  }

  /**
   * Guard against invalid cache keys and blocked keys.
   *
   * @param cacheKey the cache key to check
   * @return the {@link RuleAction} for a valid key, or {@code null} if the key is invalid
   * @throws ZetaBlockedException when the key matches a blocklist rule
   */
  @Nullable
  private GuardReport preGuard(String cacheKey) {
    if (invalidCacheKey(cacheKey)) {
      return null;
    }
    RuleAction action = ruleMatcher.evaluateRule(cacheKey);
    if (action == RuleAction.BLOCK) {
      throw new ZetaBlockedException("HotKeyCache", cacheKey);
    }
    return GuardReport.builder().action(action).isSkipReport(action == RuleAction.ALLOW_NO_REPORT).build();
  }

  /**
   * Secondary BLOCK check (TOCTOU guard for paths where the key was already validated).
   *
   * @param cacheKey the cache key to check
   * @throws ZetaBlockedException when the key matches a blocklist rule
   */
  private void afterGuard(String cacheKey) {
    if (ruleMatcher.evaluateRule(cacheKey) == RuleAction.BLOCK) {
      throw new ZetaBlockedException("HotKeyCache", cacheKey);
    }
  }

  /**
   * Check whether an existing cache entry is managed by the Worker (HOT or COOL).
   * Worker-managed entries preserve their original normal TTLs through writes.
   *
   * @param existing the existing cache entry (maybe {@code null} or a raw value)
   * @return {@code true} if the entry is a {@link CacheEntry} with state HOT or COOL
   */
  private static boolean isWorkerManaged(Object existing) {
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
  public boolean isHot(String cacheKey) {
    cacheKey = normalize(cacheKey);
    return isHot(cacheKey, null);
  }

  /**
   * Check whether a key is locally tracked as hot, using an optional cached
   * entry to avoid a redundant Caffeine lookup.
   *
   * @param cacheKey the key to inspect
   * @param entry    an existing {@link CacheEntry} (may be {@code null}), or
   *                 {@code null} to read from L1
   * @return {@code true} if the key is a local hot key
   */
  private boolean isHot(String cacheKey, CacheEntry entry) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("isHot: invalid cacheKey");
      return false;
    }
    final CacheEntry cacheEntry = entry != null ? entry : (CacheEntry) caffeineCache.getIfPresent(cacheKey);

    return (
      (cacheEntry != null &&
        !expireManager.isLogicallyExpired(cacheEntry) &&
        cacheEntry.getKeyState() == KeyState.HOT) ||
      hotKeyDetector.contains(cacheKey)
    );
  }

  /**
   * Whether to skip Worker reporting, combining the rule-based guard decision
   * with the caller's explicit per-invocation flag.
   *
   * @param isSkipReportByGuard whether the rule engine says to skip reporting
   * @param isReportByThisTime  whether the caller explicitly requested reporting
   * @return {@code true} when the caller explicitly requested reporting, or
   *         when the rule engine decided to skip
   */
  private static boolean isSkipReport(boolean isSkipReportByGuard, boolean isReportByThisTime) {
    return isReportByThisTime || isSkipReportByGuard;
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
   * @param cacheEntry the cache entry to inspect
   * @return {@code true} if the entry may be promoted by local TopK
   */
  private boolean isPromotableState(CacheEntry cacheEntry) {
    KeyState state = cacheEntry.getKeyState();
    return state == KeyState.NORMAL || (state == KeyState.COOL && !healthView.isClusterHealthy());
  }

  /**
   * Execute a cache operation with standardized error handling: {@link ZetaBlockedException}
   * is rethrown, all other {@link RuntimeException} are logged and swallowed.
   *
   * @param cacheKey the key being accessed (used in the error log)
   * @param action   the cache operation to execute
   * @param <T>      the value type
   * @return the result of {@code action}, or {@link Optional#empty()} on error
   */
  private <T> Optional<T> execute(String cacheKey, Supplier<Optional<T>> action) {
    try {
      return action.get();
    } catch (ZetaBlockedException e) {
      throw e;
    } catch (RuntimeException e) {
      log.error("HotKeyCache internal error for key={}, returning empty to keep caller operational", cacheKey, e);
      return Optional.empty();
    }
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
    String nk = normalize(cacheKey);
    if (preGuard(nk) == null) return Optional.empty();
    return execute(nk, () ->
      Optional.ofNullable(caffeineCache.getIfPresent(nk)).map(raw ->
        raw instanceof CacheEntry vv ? (T) unwrapValue(vv.getValue()) : (T) raw
      )
    );
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
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  @SuppressWarnings("unchecked")
  public <T> Optional<T> get(
    String cacheKey,
    Supplier<T> reader,
    long hardTtlMs,
    long softTtlMs,
    boolean isReportByThisTime
  ) {
    String nk = normalize(cacheKey);
    GuardReport g = preGuard(nk);
    if (g == null) return Optional.empty();

    boolean skipReport = isSkipReport(g.isSkipReport, isReportByThisTime);

    return (Optional<T>) execute(nk, () ->
      Optional.ofNullable(caffeineCache.getIfPresent(nk))
        .flatMap(raw -> handleCacheHit(nk, raw, hardTtlMs, softTtlMs, skipReport))
        .or(() -> loadAndCache(nk, reader, hardTtlMs, softTtlMs, skipReport))
    );
  }

  /**
   * Batch variant of {@link #get(String, Supplier, long, long, boolean)}.  All keys share
   * the same explicit TTL overrides.
   *
   * @param cacheKeys  the keys to retrieve
   * @param reader     the value function for cache misses
   * @param hardTtlMs  hard TTL override (0 = use configured default;
   *                   {@link Long#MAX_VALUE} for permanent entry)
   * @param softTtlMs  soft TTL override (0 = use configured default)
   * @param <T>        the value type
   * @return a map of key → loaded or cached value (never {@code null})
   * @throws ZetaBlockedException when any key matches a blacklist rule
   */
  public <T> Map<String, Optional<T>> get(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader,
    long hardTtlMs,
    long softTtlMs,
    boolean isSkipReports
  ) {
    List<String> misses = new ArrayList<>();
    Map<String, Optional<T>> results = new LinkedHashMap<>();
    Map<String, Boolean> skipReports = new HashMap<>();

    for (String key : cacheKeys) {
      GuardReport g = preGuard(key);
      if (g == null) {
        results.put(key, Optional.empty());
        continue;
      }
      skipReports.put(key, isSkipReport(g.isSkipReport, isSkipReports));

      Optional<T> cached = execute(key, () ->
        Optional.ofNullable(caffeineCache.getIfPresent(key)).flatMap(raw ->
          handleCacheHit(key, raw, hardTtlMs, softTtlMs, skipReports.get(key))
        )
      );
      if (cached.isPresent()) {
        results.put(key, cached);
      } else {
        misses.add(key);
      }
    }
    if (misses.isEmpty()) return results;

    results.putAll(loadAndCache(misses, reader, hardTtlMs, softTtlMs, skipReports));
    return results;
  }

  /**
   * Check a cache hit: verify it is not logically expired, unwrap the value,
   * then run {@link #process} for promotion and reporting.
   * <p>Extracted from {@link #get} to name the flatMap step.
   */
  private <T> Optional<T> handleCacheHit(
    String cacheKey,
    Object raw,
    long hardTtlMs,
    long softTtlMs,
    boolean skipReport
  ) {
    if (expireManager.invalidateIfIsLogicallyExpired(cacheKey, raw)) {
      return Optional.empty();
    }
    @SuppressWarnings("unchecked")
    T val = raw instanceof CacheEntry vv ? (T) unwrapValue(vv.getValue()) : (T) raw;
    return process(cacheKey, raw, val, hardTtlMs, softTtlMs, skipReport);
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
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  @SuppressWarnings("all")
  public <T> Optional<T> getWithSoftExpire(
    String cacheKey,
    Supplier<T> reader,
    long hardTtlMs,
    long softTtlMs,
    boolean isReportByThisTime
  ) {
    String nk = normalize(cacheKey);
    GuardReport g = preGuard(nk);
    if (g == null) return Optional.empty();

    boolean skipReport = isSkipReport(g.isSkipReport, isReportByThisTime);

    if (isSoftExpireUnenabled()) {
      log.debug("getWithSoftExpire: soft expire not enabled, fallback to get()");
      return get(nk, reader, hardTtlMs, softTtlMs, isReportByThisTime);
    }
    Object raw = caffeineCache.getIfPresent(nk);
    return execute(nk, () ->
      Optional.ofNullable(raw)
        .flatMap(v -> handleSoftExpire(nk, v, reader, hardTtlMs, softTtlMs, skipReport))
        .or(() -> loadAndCache(nk, reader, hardTtlMs, softTtlMs, skipReport))
    );
  }

  /**
   * Batch variant of {@link #getWithSoftExpire(String, Supplier, long, long, boolean)}.
   * All keys share the same explicit TTL overrides.
   *
   * @param cacheKeys  the keys to retrieve
   * @param reader     the value function for cache misses / refreshes
   * @param hardTtlMs  hard TTL override (0 = use configured default;
   *                   {@link Long#MAX_VALUE} for permanent entry)
   * @param softTtlMs  soft TTL override (0 = use configured default)
   * @param <T>        the value type
   * @return a map of key → cached (possibly stale) or loaded value
   * @throws ZetaBlockedException when any key matches a blacklist rule
   */
  @SuppressWarnings("all")
  public <T> Map<String, Optional<T>> getWithSoftExpire(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader,
    long hardTtlMs,
    long softTtlMs,
    boolean isReportByThisTime
  ) {
    if (isSoftExpireUnenabled()) {
      return get(cacheKeys, reader, hardTtlMs, softTtlMs, isReportByThisTime);
    }

    List<String> misses = new ArrayList<>();
    Map<String, Optional<T>> results = new LinkedHashMap<>();
    Map<String, Boolean> skipReports = new HashMap<>();

    for (String key : cacheKeys) {
      GuardReport g = preGuard(key);
      if (g == null) {
        results.put(key, Optional.empty());
        continue;
      }

      skipReports.put(key, isSkipReport(g.isSkipReport, isReportByThisTime));
      Object raw = caffeineCache.getIfPresent(key);

      Optional<T> hit = execute(key, () ->
        Optional.ofNullable(raw).flatMap(v ->
          handleSoftExpire(key, v, () -> reader.apply(key), hardTtlMs, softTtlMs, skipReports.get(key))
        )
      );
      if (hit.isPresent()) {
        results.put(key, hit);
      } else {
        misses.add(key);
      }
    }
    if (misses.isEmpty()) return results;

    results.putAll(loadAndCache(misses, reader, hardTtlMs, softTtlMs, skipReports));
    return results;
  }

  private <T> Optional<T> handleSoftExpire(
    String cacheKey,
    Object raw,
    Supplier<T> reader,
    long hardTtlMs,
    long softTtlMs,
    boolean skipReport
  ) {
    if (expireManager.invalidateIfIsLogicallyExpired(cacheKey, raw)) {
      return Optional.empty();
    }
    @SuppressWarnings("unchecked")
    T cached = raw instanceof CacheEntry vv ? (T) unwrapValue(vv.getValue()) : (T) raw;
    refreshSoftExpire(cacheKey, raw, reader, softTtlMs);
    return process(cacheKey, raw, cached, hardTtlMs, softTtlMs, skipReport);
  }

  /**
   * Trigger a background refresh if the raw value is a worker-managed entry
   * and its soft TTL has expired.
   *
   * @param cacheKey  the cache key
   * @param raw       the raw value from Caffeine (may be {@link CacheEntry} or bare)
   * @param reader    the value supplier for the refresh
   * @param softTtlMs per-call soft TTL override (0 = use configured default)
   */
  private void refreshSoftExpire(String cacheKey, Object raw, Supplier<?> reader, long softTtlMs) {
    if (!isWorkerManaged(raw)) return;

    CacheEntry ce = (CacheEntry) raw;
    if (!expireManager.isSoftExpired(ce)) return;

    long effectiveSoft = computeSoftTtlForRefresh(softTtlMs, ce);
    expireManager.triggerBackgroundRefresh(cacheKey, reader, effectiveSoft);
  }

  /**
   * Compute the effective soft TTL to use for a background refresh.
   * <p>
   * Priority: per-call override > per-key-state hot default > per-entry normal TTL > global default.
   *
   * @param softTtlMs per-call soft TTL override (0 = use configured default)
   * @param ce        the cache entry to derive key-state and normal-soft defaults from
   * @return the effective soft TTL in milliseconds
   */
  private long computeSoftTtlForRefresh(long softTtlMs, CacheEntry ce) {
    if (softTtlMs > 0) return softTtlMs;
    if (KeyState.HOT == ce.getKeyState()) return expireManager.getEffectiveHotSoftTtlMs();
    if (ce.getNormalSoftTtlMs() > 0) return ce.getNormalSoftTtlMs();
    return expireManager.getEffectiveSoftTtlMs();
  }

  /**
   * Process a cache hit: type local hot-key promotion/renewal and reportToWorker
   * to Worker, then re-check logical expiry (TOCTOU guard) after side effects.
   * <p>Extracted from {@link #get} and {@link #getWithSoftExpire} to eliminate
   * code duplication.
   *
   * @param cacheKey  the cache key
   * @param raw       the raw value from Caffeine (maybe {@link CacheEntry} or bare)
   * @param cached    the unwrapped cached value
   * @param hardTtlMs hard TTL override
   * @param softTtlMs soft TTL override
   * @param skipReport if {@code true}, skip app-to-Worker reporting
   * @param <T>       the value type
   * @return an {@link Optional} containing the cached value, or empty if logically expired
   */
  private <T> Optional<T> process(
    String cacheKey,
    Object raw,
    T cached,
    long hardTtlMs,
    long softTtlMs,
    boolean skipReport
  ) {
    dispatcher.report(cacheKey, skipReport);

    boolean wasProcessed = processLocalHotkeyIfNeeded(cacheKey, raw, hardTtlMs, softTtlMs);

    // TOCTOU: re-read from cache after side effects (promote/reportToWorker may have taken time)
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
    afterGuard(cacheKey);
    Optional<T> result = singleFlight.load(cacheKey, reader);

    if (result.isEmpty()) {
      return mapEmpty(cacheKey);
    }
    T value = result.get();
    return Optional.of(processLoaded(cacheKey, value, hardTtlMs, softTtlMs, skipReport));
  }

  /**
   * Batch variant of {@link #loadAndCache(String, Supplier, long, long, boolean)}.
   * Submits all keys to SingleFlight in a single pass, then processes each loaded
   * value through {@link #processLoaded} and calls {@link #mapEmpty} for absent results.
   *
   * @param cacheKeys   the keys to load (typically the misses from a preceding Caffeine check)
   * @param reader      the value function for cache misses (called on executor threads)
   * @param hardTtlMs   hard TTL override (0 = use configured default)
   * @param softTtlMs   soft TTL override (0 = use configured default)
   * @param skipReports map of key → whether to skip Worker reporting
   * @param <T>         the value type
   * @return a map of key → loaded or empty result, preserving input order
   */
  private <T> Map<String, Optional<T>> loadAndCache(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader,
    long hardTtlMs,
    long softTtlMs,
    Map<String, Boolean> skipReports
  ) {
    Map<String, Optional<T>> loaded = singleFlight.load(cacheKeys, reader);
    Map<String, Optional<T>> results = new LinkedHashMap<>();

    for (String key : cacheKeys) {
      results.put(
        key,
        execute(key, () -> {
          Optional<T> opt = loaded.get(key);
          return opt
            .map(t -> processLoaded(key, t, hardTtlMs, softTtlMs, skipReports.get(key)))
            .or(() -> mapEmpty(key));
        })
      );
    }
    return results;
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
  private <T> Optional<T> mapEmpty(String cacheKey) {
    if (singleFlight.isBreakerOpen()) {
      Object stale = caffeineCache.getIfPresent(cacheKey);

      if (stale != null) {
        T val = stale instanceof CacheEntry ce ? (T) unwrapValue(ce.getValue()) : (T) stale;
        log.debug("CB open, returning stale entry for key={}", cacheKey);
        return Optional.ofNullable(val);
      }
    }
    builtNullValueEntry(cacheKey);
    return Optional.empty();
  }

  /**
   * Cache a {@link NullValue} sentinel in L1 with a short TTL so that
   * subsequent reads return empty without hitting the reader.
   *
   * @param cacheKey the key to store the null sentinel for
   */
  private void builtNullValueEntry(String cacheKey) {
    long nullExpireAtMs = expireManager.computeNullExpireAt(nullTtlMs());
    caffeineCache.put(
      cacheKey,
      expireManager.createBuilder(
        NullValue.INSTANCE,
        VERSION_DEFAULT,
        false,
        0L,
        nullTtlMs(),
        0L,
        nullExpireAtMs,
        0L,
        0L,
        0L,
        KeyState.NORMAL
      )
    );
  }

  /**
   * Process a successfully loaded value: re-check blacklist, detect hot key via
   * HeavyKeeper, store in L1 with HOT or NORMAL TTL, and optionally reportToWorker to Worker.
   *
   * @param cacheKey   the key to cache
   * @param value      the loaded value (must not be null at this point)
   * @param hardTtlMs  hard TTL override (0 = use configured default)
   * @param softTtlMs  soft TTL override (0 = use configured default)
   * @param skipReport if {@code true}, skip reporting to Worker
   * @param <T>        the value type
   * @return the loaded value (unchanged)
   */
  private <T> T processLoaded(String cacheKey, T value, long hardTtlMs, long softTtlMs, boolean skipReport) {
    afterGuard(cacheKey);

    long effectiveHard = expireManager.resolveEffectiveHardTtl(hardTtlMs);
    long effectiveSoft = expireManager.resolveEffectiveSoftTtl(softTtlMs);

    dispatcher.report(cacheKey, skipReport);
    if (hotKeyDetector.contains(cacheKey)) {
      long hotHard = expireManager.resolveEffectiveHotHard(hardTtlMs);
      long hotSoft = expireManager.resolveEffectiveHotSoft(softTtlMs);

      loadCacheEntry(cacheKey, value, hotHard, hotSoft, KeyState.HOT, effectiveHard, effectiveSoft);
    } else {
      loadCacheEntry(cacheKey, value, effectiveHard, effectiveSoft, KeyState.NORMAL, effectiveHard, effectiveSoft);
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
  private void loadCacheEntry(
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
        if (isWorkerManaged(existing)) {
          return existing;
        }
        return buildEntry(value, hardTtlMs, softTtlMs, state, normalHardTtl, normalSoftTtl);
      });
  }

  /**
   * Construct a new {@link CacheEntry} with the given value, TTLs, state, and
   * normal-TTL fallback values.
   *
   * @param value          the value to cache
   * @param hardTtlMs      the hard TTL in milliseconds
   * @param softTtlMs      the soft TTL in milliseconds (0 = no soft expire)
   * @param state          the key state to assign
   * @param normalHardTtlMs the normal (non-hot) hard TTL to preserve for demotion
   * @param normalSoftTtlMs the normal (non-hot) soft TTL to preserve for demotion
   * @param <T>            the value type
   * @return a new {@link CacheEntry}
   */
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
   * HOT and more than 20% of its TTL has elapsed, extend its expiry window;
   * otherwise promote eligible non-hot entries (NORMAL or COOL-when-all-dead)
   * to HOT if the local TopK now considers them hot.
   * <p>
   * HOT entries that are still within their first half are left untouched —
   * no need to re-insert the same state.
   *
   * @param cacheKey  the key to promote
   * @param raw       the raw cached value (maybe a {@link CacheEntry} or a bare object)
   * @param hardTtlMs hard TTL override (0 = use configured hot hard TTL)
   * @param softTtlMs soft TTL override (0 = use configured hot soft TTL)
   * @return {@code true} if a local promotion or expiry extension occurred
   */
  private boolean processLocalHotkeyIfNeeded(String cacheKey, Object raw, long hardTtlMs, long softTtlMs) {
    if (raw instanceof CacheEntry ce) {
      if (ce.getKeyState() == KeyState.HOT && ce.getHardExpireAtMs() != Long.MAX_VALUE) {
        return extendHotKeyExpiryIfNeeded(cacheKey, ce, hardTtlMs, softTtlMs);
      }
      if (isPromotableState(ce)) {
        return promoteIfLocalHot(cacheKey, hardTtlMs, softTtlMs);
      }
    }
    return false;
  }

  /**
   * Extend the expiry of a HOT entry if more than 20% of its TTL has elapsed.
   *
   * @return {@code true} if the entry was extended
   */
  private boolean extendHotKeyExpiryIfNeeded(String cacheKey, CacheEntry ce, long hardTtlMs, long softTtlMs) {
    long remainingTtl = ce.getHardExpireAtMs() - TimeSource.currentTimeMillis();
    long totalTtl = ce.getHardTtlMs();

    if (totalTtl > 0 && remainingTtl < totalTtl * 0.8) {
      expireManager.extendExpiry(cacheKey, hardTtlMs, softTtlMs);
      return true;
    }
    return false;
  }

  /**
   * Promote a NORMAL or COOL entry to HOT if the local TopK now considers it hot.
   *
   * @return {@code true} if the entry was promoted
   */
  private boolean promoteIfLocalHot(String cacheKey, long hardTtlMs, long softTtlMs) {
    long hotHard = expireManager.resolveEffectiveHotHard(hardTtlMs);
    long hotSoft = expireManager.resolveEffectiveHotSoft(softTtlMs);

    return promote(cacheKey, hotHard, hotSoft);
  }

  /**
   * Atomically promote a cache entry to HOT state in L1, respecting the
   * Worker-managed guard (entries in HOT/COOL state from the Worker are
   * left untouched).
   *
   * @param cacheKey the key to promote
   * @param hotHard  the hot-entry hard TTL
   * @param hotSoft  the hot-entry soft TTL
   */
  private boolean promote(String cacheKey, long hotHard, long hotSoft) {
    boolean[] promoted = { false };
    caffeineCache
      .asMap()
      .compute(cacheKey, (k, existing) -> {
        if (existing instanceof CacheEntry entry) {
          if (!isPromotableState(entry) || !hotKeyDetector.contains(k)) {
            return existing;
          }

          promoted[0] = true;
          return entry.withTtlAndKeyState(
            hotHard,
            hotSoft,
            expireManager.computeHardExpireAt(hotHard),
            expireManager.computeSoftExpireAt(hotSoft),
            KeyState.HOT
          );
        }
        return null;
      });
    return promoted[0];
  }

  /**
   * Invalidate a single key from L1 and send INVALIDATE to peers,
   * so they remove their local copy and re-fetch on next {@link #get}.
   * The next local {@link #get} will re-fetch from the reader.
   *
   * @param cacheKey the key to invalidate
   */
  public void invalidate(String cacheKey, boolean isBroadcastByThisTime) {
    String nk = normalize(cacheKey);
    if (invalidCacheKey(nk)) {
      log.debug("invalidate: invalid cacheKey");
      return;
    }

    TransactionSupport.runNowOrAfterCommit(() -> {
      caffeineCache.invalidate(nk);
      if (isBroadcastByThisTime) {
        bumpAndInvalidate(nk);
      }
    });
  }

  /**
   * Increment the data version asynchronously and send an INVALIDATE
   * message to all peer instances.
   * <p>Extracted from {@link #invalidate} and {@link #invalidateAfterPut} to
   * eliminate the duplicated async-send block.
   */
  private void bumpAndInvalidate(String cacheKey) {
    try {
      hotKeyExecutor.execute(() -> {
        try {
          var vr = versionController.nextVersion(cacheKey);
          dispatcher.send(cacheKey, SyncMessage.TYPE_INVALIDATE, vr.dataVersion(), vr.degraded());
        } catch (Exception ex) {
          log.error("invalidate async send failed for key={}", cacheKey, ex);
        }
      });
    } catch (RejectedExecutionException ree) {
      log.warn(
        "invalidate executor rejected for key={}, peer invalidation deferred to next cycle: {}",
        cacheKey,
        ree.getMessage()
      );
    }
  }

  /**
   * Invalidate all given keys from L1 and send INVALIDATE for each.
   * Invalid keys (null or blank) are silently skipped.
   *
   * @param cacheKeys the keys to invalidate
   * @param isBroadcastByThisTime whether to broadcast the invalidation to peers
   */
  public void invalidate(Iterable<String> cacheKeys, boolean isBroadcastByThisTime) {
    List<String> validKeys = new ArrayList<>();
    for (String key : cacheKeys) {
      String normalized = normalize(key);
      if (!invalidCacheKey(normalized)) validKeys.add(normalized);
    }
    if (validKeys.isEmpty()) {
      log.debug("invalidate: all cacheKeys are invalid");
      return;
    }

    TransactionSupport.runNowOrAfterCommit(() -> {
      caffeineCache.invalidateAll(validKeys);
      if (isBroadcastByThisTime) {
        dispatcher.send(validKeys, SyncMessage.TYPE_INVALIDATE_ALL);
      }
    });
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
  public <T> void putThrough(
    String cacheKey,
    T value,
    Runnable writer,
    long hardTtlMs,
    long softTtlMs,
    boolean isBroadcastByThisTime
  ) {
    String nk = normalize(cacheKey);
    if (preGuard(nk) == null) {
      return;
    }

    AtomicBoolean writerOk = new AtomicBoolean(false);

    TransactionSupport.runAsyncAfterCommit(
      () -> {
        try {
          writer.run();
          writerOk.compareAndSet(false, true);
        } catch (Exception e) {
          // Writer failed — do NOT cache, do NOT send, do NOT bump version.
          log.error("putThrough writer failed for key={}, cache update skipped: {}", nk, e.getMessage(), e);
          return;
        }

        long effectiveHardTtl = expireManager.resolveEffectiveHardTtl(hardTtlMs);
        long effectiveSoftTtl = expireManager.resolveEffectiveSoftTtl(softTtlMs);

        try {
          var vr = versionController.nextVersion(nk);

          caffeineCache
            .asMap()
            .compute(nk, (k, existing) ->
              buildPutThroughEntry(existing, value, vr, effectiveHardTtl, effectiveSoftTtl, false)
            );

          if (isBroadcastByThisTime) {
            dispatcher.send(nk, SyncMessage.TYPE_REFRESH, vr.dataVersion(), vr.degraded());
          }
        } catch (RejectedExecutionException ree) {
          // hotKeyExecutor saturated mid-body (nextVersion/Redis did run, but
          // we cannot distinguish here — VersionController.nextVersion already
          // has its own Redis-degraded fallback path). Cache and send are
          // skipped; the next read or the next periodic Worker cycle will
          // reconcile. Logged at ERROR so operators can detect silent drops.
          log.error(
            "putThrough executor rejected mid-flight for key={} (local cache NOT updated, send NOT sent)",
            nk,
            ree
          );
          throw ree; // CompletableFuture.exceptionally downstream decides policy
        } catch (Exception e) {
          // Redis-relayed exception or unexpected error. The local L1 is still
          // updated using a degraded version so the cache remains coherent with
          // the mutation that already succeeded on the writer.
          log.error("putThrough cache/send failed for key={} (applying degraded local update)", nk, e);
          var vr = versionController.fallbackVersion();

          caffeineCache
            .asMap()
            .compute(nk, (k, existing) ->
              buildPutThroughEntry(existing, value, vr, effectiveHardTtl, effectiveSoftTtl, true)
            );
          dispatcher.send(nk, SyncMessage.TYPE_REFRESH, vr.dataVersion(), vr.degraded());
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
    long effectiveSoftTtl,
    boolean forceUpdate
  ) {
    KeyState state = (existing instanceof CacheEntry entry) ? entry.getKeyState() : KeyState.NORMAL;
    if (
      !forceUpdate &&
      existing instanceof CacheEntry ce &&
      VersionGuard.shouldSkipForSync(ce, vr.dataVersion(), vr.degraded())
    ) {
      return ce;
    }

    boolean isWorkerManaged = isWorkerManaged(existing);

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
   * without send, without hot-key detection, and without reporting.
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
    cacheKey = normalize(cacheKey);
    if (preGuard(cacheKey) == null) return;

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
   * Execute a mutation, then invalidate L1 and send.
   * Next {@link #get} will re-fetch from the reader.
   *
   * @param cacheKey the key to invalidate after mutation
   * @param mutation the mutation to execute
   */
  public void invalidateAfterPut(String cacheKey, Runnable mutation, boolean isBroadcastByThisTime) {
    String nk = normalize(cacheKey);
    if (preGuard(nk) == null) return;

    TransactionSupport.runNowOrAfterCommit(() -> {
      try {
        mutation.run();
      } catch (Exception e) {
        log.error("invalidateAfterPut mutation failed, skip invalidate and send: {}", nk, e);
        return;
      }
      caffeineCache.invalidate(nk);
      if (isBroadcastByThisTime) {
        bumpAndInvalidate(nk);
      }
    });
  }

  /**
   * Execute mutations for multiple keys, then invalidate each from L1 and send.
   * Each key gets its own mutation. If a single mutation fails, that key is skipped
   * (invalidation and send are not performed for that key).
   *
   * @param mutations a map of key → mutation pairs
   */
  @SuppressWarnings("java:S4030")
  public void invalidateAfterPut(Map<String, ? extends Runnable> mutations, boolean isBroadcastByThisTime) {
    List<String> validKeys = new ArrayList<>();
    List<Runnable> validMutations = new ArrayList<>();

    for (var entry : mutations.entrySet()) {
      String key = normalize(entry.getKey());
      if (preGuard(key) != null) {
        validKeys.add(key);
        validMutations.add(entry.getValue());
      }
    }
    if (validKeys.isEmpty()) {
      log.debug("invalidateAfterPut: all cacheKeys are invalid");
      return;
    }

    TransactionSupport.runNowOrAfterCommit(() -> {
      for (int i = 0; i < validKeys.size(); i++) {
        String cacheKey = validKeys.get(i);
        try {
          validMutations.get(i).run();
        } catch (Exception e) {
          log.error("invalidateAfterPut mutation failed, skip invalidate and send: {}", cacheKey, e);
          continue;
        }
        caffeineCache.invalidate(cacheKey);
        if (isBroadcastByThisTime) {
          bumpAndInvalidate(cacheKey);
        }
      }
    });
  }

  /**
   * Add or remove a rule atomically within a transaction boundary.
   * <p>Extracted from the four public rule methods to eliminate the
   * identical validation-and-commit pattern.
   *
   * @param add {@code true} to add the rule, {@code false} to remove it
   */
  private void modifyRule(String cacheKey, RuleAction action, boolean add) {
    String nk = normalize(cacheKey);
    if (invalidCacheKey(nk)) {
      log.debug("modifyRule: invalid cacheKey '{}'", nk);
      return;
    }
    TransactionSupport.runNowOrAfterCommit(() -> {
      if (add) ruleMatcher.addRule(RuleMatcher.of(nk, action));
      else ruleMatcher.removeRule(nk, action);
      log.info("{} {}", add ? "Added" : "Removed", action);
    });
  }

  public void addBlacklist(String cacheKey) {
    modifyRule(cacheKey, RuleAction.BLOCK, true);
  }

  public void addWhitelist(String cacheKey) {
    modifyRule(cacheKey, RuleAction.ALLOW_NO_REPORT, true);
  }

  public void unBlacklist(String cacheKey) {
    modifyRule(cacheKey, RuleAction.BLOCK, false);
  }

  public void unWhitelist(String cacheKey) {
    modifyRule(cacheKey, RuleAction.ALLOW_NO_REPORT, false);
  }

  /**
   * Evaluate all rules against the given key and return the first matching action.
   *
   * @param cacheKey the key to evaluate
   * @return the matching {@link RuleAction}, or {@code ALLOW} if no rule matches
   */
  public RuleAction evaluateRule(String cacheKey) {
    return ruleMatcher.evaluateRule(normalize(cacheKey));
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
   * @return a {@link ZetaCacheStats} reportToWorker; hit/miss counters are {@code 0}
   *         if stats recording is not enabled
   */
  public ZetaCacheStats stats() {
    CacheStats cs = caffeineCache.stats();
    return new ZetaCacheStats(
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
    return ruleMatcher.evaluateRule(normalize(cacheKey)) == RuleAction.BLOCK;
  }

  /**
   * Check whether the given key is whitelisted (skips Worker reporting).
   *
   * @param cacheKey the key to check
   * @return {@code true} if a whitelist rule matches the key
   */
  public boolean isWhitelisted(String cacheKey) {
    return ruleMatcher.evaluateRule(normalize(cacheKey)) == RuleAction.ALLOW_NO_REPORT;
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
   * send, expiry management) can lead to inconsistent state.
   *
   * @return the raw Caffeine {@link Cache} instance
   */
  public Cache<String, Object> getLocalCache() {
    return caffeineCache;
  }

  /**
   * Resolve the effective soft TTL in milliseconds.
   * <p>
   * When the given value is {@code 0} (meaning "use configured default"),
   * returns the default soft TTL from the {@link ExpireManager}.
   * Otherwise returns the value as-is.
   *
   * @param softTtlMs the soft TTL value to resolve (0 = use configured default)
   * @return the effective soft TTL in milliseconds, always positive
   */
  public long resolveEffectiveSoftTtl(long softTtlMs) {
    return expireManager.resolveEffectiveSoftTtl(softTtlMs);
  }

  /**
   * Atomically load or compute a value, avoiding TOCTOU between the
   * Caffeine lookup and the computation.  Blocks concurrent writers for
   * the same key via Caffeine's stripe-lock on {@code asMap()}.
   * <p>
   * Hot keys detected by the local TopK are promoted to HOT TTLs;
   * normal keys use the configured effective TTLs.
   *
   * <p><b>Locking caveat:</b> The {@code reader} supplier executes inside
   * Caffeine's {@code ConcurrentHashMap.compute()} — the hash-segment lock
   * is held for the entire duration of the reader call.  Slow I/O readers
   * will block all operations on the same segment.  For I/O-heavy workloads
   * where strict per-key atomicity is not required, prefer {@link #get}
   * which uses {@link SingleFlight} deduplication without holding the
   * segment lock.
   *
   * <p><b>Reader exception safety:</b> When the reader throws, the
   * {@link SingleFlight#isBreakerOpen() circuit breaker} is consulted:
   * if open and a stale entry exists, the stale value is returned
   * (graceful degradation).  Otherwise the exception propagates to the
   * caller via the {@link #execute} error handler.
   *
   * @param cacheKey  the key to retrieve
   * @param reader    the value supplier for cache misses
   * @param hardTtlMs hard TTL override (0 = use configured default)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param isReportByThisTime whether to reportToWorker this access to the Worker
   * @param <T>       the value type
   * @return an {@link Optional} containing the cached or loaded value
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> computeIfAbsent(
    String cacheKey,
    Supplier<T> reader,
    long hardTtlMs,
    long softTtlMs,
    boolean isReportByThisTime
  ) {
    String nk = normalize(cacheKey);
    GuardReport g = preGuard(nk);
    if (g == null) return Optional.empty();

    boolean skipReport = isSkipReport(g.isSkipReport, isReportByThisTime);
    return execute(nk, () -> computeInLock(nk, reader, hardTtlMs, softTtlMs, skipReport, false));
  }

  /**
   * Atomic variant of {@link #getWithSoftExpire}.  Combines the L1 lookup,
   * value loading, and optional stale-while-revalidate into a single
   * Caffeine {@code compute()} call for stripe-level atomicity.
   * <p>
   * Delegates to {@link #computeIfAbsent} when soft-expire is disabled.
   *
   * <p><b>Locking caveat:</b> Same hash-segment lock considerations as
   * {@link #computeIfAbsent} apply — the reader runs inside the compute lock.
   *
   * @param cacheKey  the key to retrieve
   * @param reader    the value supplier for cache misses / refreshes
   * @param hardTtlMs hard TTL override (0 = use configured default)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param isReportByThisTime whether to reportToWorker this key to the Worker
   * @param <T>       the value type
   * @return an {@link Optional} containing the cached (possibly stale) or loaded value
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> computeIfAbsentWithSoftExpire(
    String cacheKey,
    Supplier<T> reader,
    long hardTtlMs,
    long softTtlMs,
    boolean isReportByThisTime
  ) {
    String nk = normalize(cacheKey);
    GuardReport g = preGuard(nk);
    if (g == null) return Optional.empty();
    if (isSoftExpireUnenabled()) return computeIfAbsent(nk, reader, hardTtlMs, softTtlMs, isReportByThisTime);

    boolean skipReport = isSkipReport(g.isSkipReport, isReportByThisTime);
    return execute(nk, () -> computeInLock(nk, reader, hardTtlMs, softTtlMs, skipReport, true));
  }

  /**
   * Shared body for {@link #computeIfAbsent} and
   * {@link #computeIfAbsentWithSoftExpire}.  Runs inside
   * Caffeine's {@code asMap().compute()} for per-key atomicity.
   * <p>
   * On a fresh miss: loads via {@code reader}, promotes to HOT if
   * the local TopK detects the key, otherwise stores as NORMAL.
   * On a hit: extends expiry when the entry is already HOT (20% threshold
   * refresh, aligned with {@link #extendHotKeyExpiryIfNeeded}) or when
   * the key is in TopK and eligible for promotion.
   * When {@code triggerRefreshOnSoftExpire} is set and the entry is
   * soft-expired, a background refresh is triggered and the stale
   * value is returned.
   *
   * <p><b>Reader exception safety:</b> If the reader throws inside the
   * compute lock, the exception is caught to avoid propagating through
   * {@link java.util.concurrent.ConcurrentHashMap#compute}.  When the
   * circuit breaker is open and a stale entry exists, the stale value
   * is returned (graceful degradation).  Otherwise, the error is
   * re-thrown after the compute completes.
   *
   * @param cacheKey                    the key to retrieve
   * @param reader                      the value supplier for cache misses / refreshes
   * @param hardTtlMs                   hard TTL override
   * @param softTtlMs                   soft TTL override
   * @param skipReport                  whether to skip Worker reporting
   * @param triggerRefreshOnSoftExpire  whether to background-refresh on soft expire
   * @param <T>                         the value type
   * @return the cached or loaded value, or empty when the reader returned {@code null}
   */
  @SuppressWarnings("all")
  private <T> Optional<T> computeInLock(
    String cacheKey,
    Supplier<T> reader,
    long hardTtlMs,
    long softTtlMs,
    boolean skipReport,
    boolean triggerRefreshOnSoftExpire
  ) {
    long effectiveHardTtl = expireManager.resolveEffectiveHardTtl(hardTtlMs);
    long effectiveSoftTtl = expireManager.resolveEffectiveSoftTtl(softTtlMs);
    long hotHardTtl = expireManager.resolveEffectiveHotHard(hardTtlMs);
    long hotSoftTtl = expireManager.resolveEffectiveHotSoft(softTtlMs);
    Object[] valueRef = { null };
    RuntimeException[] errorRef = { null };

    caffeineCache
      .asMap()
      .compute(cacheKey, (k, existing) -> {
        if (existing instanceof CacheEntry ce) {
          valueRef[0] = unwrapValue(ce.getValue());

          if (!expireManager.isLogicallyExpired(ce)) {
            if (ce.getKeyState() == KeyState.HOT && ce.getHardExpireAtMs() != Long.MAX_VALUE) {
              long remaining = ce.getHardExpireAtMs() - TimeSource.currentTimeMillis();
              long totalTtl = ce.getHardTtlMs();
              if (totalTtl > 0 && remaining < totalTtl * 0.8) {
                return expireManager.applyTtl(ce, hotHardTtl, hotSoftTtl);
              }
            } else if (isPromotableState(ce) && hotKeyDetector.contains(k)) {
              return ce.withTtlAndKeyState(
                hotHardTtl,
                hotSoftTtl,
                expireManager.computeHardExpireAt(hotHardTtl),
                expireManager.computeSoftExpireAt(hotSoftTtl),
                KeyState.HOT
              );
            }

            if (triggerRefreshOnSoftExpire && expireManager.isSoftExpired(ce)) {
              expireManager.triggerBackgroundRefresh(k, reader, computeSoftTtlForRefresh(softTtlMs, ce));
            }
            return existing;
          }
        }

        T loaded;
        try {
          loaded = reader.get();
        } catch (RuntimeException e) {
          errorRef[0] = e;
          // Circuit breaker fallback: return stale entry if available (graceful degradation)
          if (singleFlight.isBreakerOpen() && existing instanceof CacheEntry ce) {
            valueRef[0] = unwrapValue(ce.getValue());
            log.debug("CB open, returning stale entry in computeInLock for key={}", cacheKey);
            return existing;
          }
          return null; // Remove entry from cache on reader failure
        }

        if (loaded == null) {
          return buildEntry(NullValue.INSTANCE, nullTtlMs(), 0L, KeyState.NORMAL, 0L, 0L);
        }
        valueRef[0] = loaded;
        if (hotKeyDetector.contains(k)) {
          return buildEntry(loaded, hotHardTtl, hotSoftTtl, KeyState.HOT, effectiveHardTtl, effectiveSoftTtl);
        }
        return buildEntry(
          loaded,
          effectiveHardTtl,
          effectiveSoftTtl,
          KeyState.NORMAL,
          effectiveHardTtl,
          effectiveSoftTtl
        );
      });

    if (errorRef[0] != null) {
      log.error(
        "computeInLock reader failed for key={}, returning empty to keep caller operational",
        cacheKey,
        errorRef[0]
      );
      return Optional.empty();
    }

    @SuppressWarnings("unchecked")
    T value = (T) valueRef[0];
    if (value == null) return Optional.empty();

    dispatcher.report(cacheKey, skipReport);
    return Optional.of(value);
  }

  /**
   * Atomically replace the cached value for the given key only if the current
   * raw (user-facing) value equals the expected value.
   * <p>
   * Comparison is performed on the unwrapped value — if the cache holds a
   * {@link CacheEntry} its inner value is extracted first.  This makes the
   * method transparent to callers who read via {@link #peek} and write back
   * via this method: they never need to know about the {@link CacheEntry}
   * wrapper.
   * <p>
   * When the cache holds no entry and {@code expected} is {@code null},
   * the replacement <b>is not performed</b> — use {@link #putLocal} for
   * unconditional insert-absent.  This follows standard CAS semantics:
   * "replace only if present and matching".
   * <p>
   * Existing version metadata ({@code dataVersion}, {@code decisionVersion})
   * is preserved — no version bump and no cross-instance broadcast.
   *
   * @param cacheKey the key to replace
   * @param expected the expected current value ({@code null} matches only an
   *                 existing entry whose unwrapped value is {@code null},
   *                 not absence)
   * @param newValue the new value to cache (nullable)
   * @return {@code true} if the replacement was performed
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public boolean compareAndSet(String cacheKey, @Nullable Object expected, @Nullable Object newValue) {
    String nk = normalize(cacheKey);
    GuardReport g = preGuard(nk);
    if (g == null) return false;

    return execute(nk, () -> {
      boolean[] replaced = { false };
      caffeineCache
        .asMap()
        .compute(nk, (k, existing) -> {
          if (!Objects.equals(extractComparable(existing), expected)) {
            return existing;
          }
          if (!(existing instanceof CacheEntry ce)) {
            return existing;
          }
          replaced[0] = true;
          return expireManager.replaceEntryValue(ce, newValue);
        });
      return Optional.of(replaced[0]);
    }).orElse(false);
  }

  /**
   * Atomically set the cache value for the given key and return the previously
   * cached value. This is a logical equivalent of {@link java.util.concurrent.atomic.AtomicReference#getAndSet}.
   *
   * <p>If the key is absent, the new value is inserted and the method returns
   * an empty {@link Optional}. If the key already exists, its current
   * user-facing value is returned after the swap. Existing entry metadata
   * ({@code dataVersion}, {@code decisionVersion}, TTLs) is preserved exactly
   * as-is; only the value payload is replaced. No version bump or broadcast
   * occurs.
   *
   * <p>Use with caution: unlike {@link #compareAndSet}, this method does not
   * perform any concurrency control (CAS). If multiple threads call this
   * method concurrently, the final value is the last writer's. For
   * conditional swaps, prefer {@link #compareAndSet}.
   *
   * @param cacheKey  the key to update
   * @param newValue  the new value to cache (nullable)
   * @param hardTtlMs hard TTL override in milliseconds for the <b>new</b>
   *                  entry (only applies when the key was absent);
   *                  {@code 0} means use the configured default
   * @param softTtlMs soft TTL override in milliseconds (same rules as
   *                  {@code hardTtlMs})
   * @param <T>       the type of the previously cached value
   * @return the previous value, or empty if none existed
   * @throws ZetaBlockedException when the key matches a blocklist rule
   */
  @SuppressWarnings("unchecked")
  public <T> Optional<T> getAndSet(String cacheKey, Object newValue, long hardTtlMs, long softTtlMs) {
    String nk = normalize(cacheKey);
    GuardReport g = preGuard(nk);
    if (g == null) return Optional.empty();

    return execute(nk, () -> {
      @SuppressWarnings("unchecked")
      T[] oldValue = (T[]) new Object[1];
      long resolvedHardTtl = expireManager.resolveEffectiveHardTtl(hardTtlMs);
      long resolvedSoftTtl = expireManager.resolveEffectiveSoftTtl(softTtlMs);
      caffeineCache
        .asMap()
        .compute(nk, (k, existing) -> {
          if (existing instanceof CacheEntry ce) {
            oldValue[0] = (T) unwrapValue(ce.getValue());
            return expireManager.replaceEntryValue(ce, newValue);
          }
          if (existing != null) {
            oldValue[0] = (T) existing;
            return buildEntry(
              newValue,
              resolvedHardTtl,
              resolvedSoftTtl,
              KeyState.NORMAL,
              resolvedHardTtl,
              resolvedSoftTtl
            );
          }
          return buildEntry(
            newValue,
            resolvedHardTtl,
            resolvedSoftTtl,
            KeyState.NORMAL,
            resolvedHardTtl,
            resolvedSoftTtl
          );
        });
      return Optional.ofNullable(oldValue[0]);
    });
  }

  /**
   * Atomically invalidate the given key only if the current cached value
   * matches the expected value.
   * <p>
   * Comparison semantics match {@link #compareAndSet}.  No version bump or
   * broadcast is performed — the invalidation is purely local.
   *
   * @param cacheKey the key to invalidate
   * @param expected the expected value ({@code null} matches only an
   *                 existing entry whose unwrapped value is {@code null},
   *                 not absence)
   * @return {@code true} if the invalidation was performed
   */
  public boolean compareAndInvalidate(String cacheKey, @Nullable Object expected) {
    String nk = normalize(cacheKey);
    GuardReport g = preGuard(nk);
    if (g == null) return false;

    return execute(nk, () -> {
      boolean[] invalidated = { false };
      caffeineCache
        .asMap()
        .computeIfPresent(nk, (k, existing) -> {
          if (!Objects.equals(extractComparable(existing), expected)) {
            return existing;
          }
          invalidated[0] = true;
          return null;
        });
      return Optional.of(invalidated[0]);
    }).orElse(false);
  }

  /**
   * Extract the user-facing comparable value from a raw cache entry.
   * <ul>
   *   <li>{@code null} / absent → {@code null}</li>
   *   <li>Bare object → the object itself</li>
   *   <li>{@link CacheEntry} → its inner value, decompressed and
   *       {@link NullValue}-unwrapped</li>
   * </ul>
   */
  @Nullable
  private Object extractComparable(@Nullable Object raw) {
    if (raw == null) return null;
    if (raw instanceof CacheEntry ce) {
      Object inner = ce.getValue();
      try {
        inner = compressor.unwrap(inner);
      } catch (IOException e) {
        log.warn("extractComparable decompress failed", e);
        return null;
      }
      return inner == NullValue.INSTANCE ? null : inner;
    }
    return raw;
  }

  /**
   * Atomically insert the given value into the cache only if the key is not
   * already present. This is a fast, lock-free-at-key-level operation that
   * never triggers a data-source writer, version bump, or cross-instance
   * broadcast.
   *
   * <p>If the key already exists in L1 (including as a {@link NullValue}
   * sentinel), the method returns {@code false} and the existing entry is
   * left untouched. If the key is absent, a fresh {@link CacheEntry} in
   * {@link KeyState#NORMAL} is created with the supplied TTLs and the
   * method returns {@code true}.
   *
   * @param cacheKey  the key to insert
   * @param value     the value to cache (must not be {@code null}; to cache a
   *                  {@code null} result, use {@link NullValue#INSTANCE}
   *                  explicitly or rely on the {@code @NullCaching} annotation)
   * @param hardTtlMs hard TTL override in milliseconds (0 = use configured default;
   *                  {@link Long#MAX_VALUE} for permanent entry)
   * @param softTtlMs soft TTL override in milliseconds (0 = use configured default)
   * @return {@code true} if the entry was inserted, {@code false} if the key
   *         was already present
   * @throws ZetaBlockedException when the key matches a blocklist rule
   */
  public boolean putIfAbsent(String cacheKey, Object value, long hardTtlMs, long softTtlMs) {
    String nk = normalize(cacheKey);
    if (preGuard(nk) == null) return false;

    return execute(nk, () -> {
      long effectiveHard = expireManager.resolveEffectiveHardTtl(hardTtlMs);
      long effectiveSoft = expireManager.resolveEffectiveSoftTtl(softTtlMs);
      boolean[] written = { false };

      caffeineCache
        .asMap()
        .compute(nk, (k, existing) -> {
          if (existing instanceof CacheEntry ce) {
            return expireManager.applyTtl(ce, hardTtlMs, softTtlMs);
          }
          if (existing != null) {
            return existing;
          }
          written[0] = true;
          return buildEntry(value, effectiveHard, effectiveSoft, KeyState.NORMAL, effectiveHard, effectiveSoft);
        });

      return Optional.of(written[0]);
    }).orElse(false);
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
