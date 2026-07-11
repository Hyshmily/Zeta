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
package io.github.hyshmily.zeta;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.cache.fluentAPI.ZetaReadQuery;
import io.github.hyshmily.zeta.cache.fluentAPI.ZetaWriteCommand;
import io.github.hyshmily.zeta.exception.ZetaBlockedException;
import io.github.hyshmily.zeta.exception.ZetaModeException;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.model.ZetaCacheStats;
import io.github.hyshmily.zeta.rule.Rule;
import io.github.hyshmily.zeta.rule.RuleMatcher;
import io.github.hyshmily.zeta.sync.distributedlock.AutoReleaseLock;
import io.github.hyshmily.zeta.sync.distributedlock.LockProvider;
import io.github.hyshmily.zeta.util.ZetaThreadFactory;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.DisposableBean;

/**
 * Public facade for the HotKey library — the sole public API entry point.
 *
 * <p>All cache read/write/invalidation operations are dispatched through this
 * class, which delegates to {@link HotKeyCache} for L1 orchestration, version
 * tracking, and cross-instance broadcast, and to {@link TopK} (HeavyKeeper)
 * for local and cluster-wide hot-key detection queries.
 *
 * <p>Rule management (blacklist/whitelist) is also exposed here, with pattern
 * matching for exact, prefix, wildcard, and regex rules.
 *
 * <p><b>Thread safety:</b> All public methods are thread-safe. The underlying
 * Caffeine cache and HeavyKeeper sketch are designed for concurrent access.
 *
 * <p>Depending on the runtime mode, some dependencies may be absent:
 * <ul>
 *   <li><b>App-only mode</b> (default): all services available.</li>
 *   <li><b>Worker-only mode</b> ({@code zeta.worker.enabled=true}):
 *       cache and app‑side TopK are absent; only the Worker TopK is available.</li>
 * </ul>
 * Methods whose backing service is absent throw
 * {@link UnsupportedOperationException} (cache read/write) or return
 * empty / zero (TopK queries).
 */
public class Zeta implements DisposableBean {

  /**
   * Cache orchestrator that manages L1 (Caffeine), version tracking, TTL
   * enforcement, cross-instance broadcast, and rule evaluation.
   * {@code null} in Worker-only mode.
   */
  private final HotKeyCache hotKeyCache;
  /**
   * App-side local hot-key detector (HeavyKeeper). Records every cache
   * access to maintain local frequency sketches. Never {@code null} in
   * app mode.
   */
  private final HotKeyDetector appHotKeyDetector;
  /**
   * Worker-side global hot-key detector receiving aggregated reports from
   * all application instances. {@code null} when no Worker is connected.
   */
  private final TopK workerTopKAlgorithm;
  /**
   * Distributed lock provider.  {@code null} when no Redis is available
   * (graceful degradation — {@link #tryLock} returns {@code null}).
   */
  private final LockProvider lockProvider;

  /**
   * Scheduler for timed background refresh tasks (lazily initialized).
   */
  @SuppressWarnings("java:S3077") // ScheduledExecutorService is thread-safe; we manage its lifecycle
  private volatile ScheduledExecutorService refreshScheduler;

  /**
   * Per-key scheduled refresh futures, keyed by cache key.
   */
  private final ConcurrentHashMap<String, ScheduledFuture<?>> refreshFutures;

  /**
   * Create a HotKey facade with all three optional components.
   *
   * <p>Each parameter may be {@code null} depending on the deployment mode:
   * <ul>
   *   <li><b>App-only:</b> {@code hotKeyCache} and {@code appHotKeyDetector} are present,
   *       {@code workerTopKAlgorithm} is {@code null}</li>
   *   <li><b>Worker-only:</b> only {@code workerTopKAlgorithm} is present</li>
   *   <li><b>Coexistence:</b> all three are present</li>
   * </ul>
   *
   * @param hotKeyCache         the cache orchestrator (maybe {@code null} in Worker-only mode)
   * @param appHotKeyDetector   the app-side local TopK detector (maybe {@code null} in Worker-only mode)
   * @param workerTopKAlgorithm the Worker-side global TopK detector (maybe {@code null} in app-only mode)
   */
  public Zeta(HotKeyCache hotKeyCache, HotKeyDetector appHotKeyDetector, TopK workerTopKAlgorithm) {
    this(hotKeyCache, appHotKeyDetector, workerTopKAlgorithm, null);
  }

  /**
   * Create a HotKey facade with an optional distributed lock provider.
   *
   * <p>Each parameter may be {@code null} depending on the deployment mode:
   * <ul>
   *   <li><b>App-only:</b> {@code hotKeyCache} and {@code appHotKeyDetector} are present,
   *       {@code workerTopKAlgorithm} and {@code lockProvider} may be absent</li>
   *   <li><b>Worker-only:</b> only {@code workerTopKAlgorithm} is present</li>
   *   <li><b>Coexistence:</b> all four may be present</li>
   * </ul>
   *
   * @param hotKeyCache         the cache orchestrator (maybe {@code null} in Worker-only mode)
   * @param appHotKeyDetector   the app-side local TopK detector (maybe {@code null} in Worker-only mode)
   * @param workerTopKAlgorithm the Worker-side global TopK detector (maybe {@code null} in app-only mode)
   * @param lockProvider        the distributed lock provider (maybe {@code null} when no Redis)
   */
  public Zeta(
    HotKeyCache hotKeyCache,
    HotKeyDetector appHotKeyDetector,
    TopK workerTopKAlgorithm,
    LockProvider lockProvider
  ) {
    this.hotKeyCache = hotKeyCache;
    this.appHotKeyDetector = appHotKeyDetector;
    this.workerTopKAlgorithm = workerTopKAlgorithm;
    this.lockProvider = lockProvider;
    this.refreshFutures = new ConcurrentHashMap<>();
  }

  /**
   * Create a fluent read query for the given key.
   *
   * <p>Returns a {@link ZetaReadQuery} builder that lets you configure
   * the primary reader, fallback chain, cache mode, TTL overrides, null
   * caching policy, and broadcast behaviour before executing.
   *
   * <p>Useful for read-heavy call-sites that prefer a declarative style
   * over manual {@code get()}/{@code getWithSoftExpire()} orchestration.
   *
   * <p>Example:
   * <pre>
   *   Optional&lt;User&gt; user = hotKey.read("user:42")
   *       .withPrimary(userRepo::findById)
   *       .thenExecute(backupRepo::findById)
   *       .withHardTtl(30_000)
   *       .withSoftTtl(10_000)
   *       .allowBroadcast()
   *       .execute();
   * </pre>
   *
   * @param cacheKey the cache key to read
   * @param <T>      the value type
   * @return a new {@link ZetaReadQuery} instance (single-use)
   */
  public <T> ZetaReadQuery<T> read(String cacheKey) {
    return new ZetaReadQuery<>(this, cacheKey);
  }

  /**
   * Create a fluent write command for the given key.
   *
   * <p>Returns a {@link ZetaWriteCommand} builder that lets you configure
   * TTL overrides before executing a write-through, invalidate-before-write,
   * or plain invalidation.
   *
   * <p>Example:
   * <pre>
   *   hotKey.write("user:42")
   *       .withHardTtl(30_000)
   *       .putThrough(newValue, dbWriter);
   * </pre>
   *
   * @param cacheKey the cache key to operate on
   * @param <T>      the value type
   * @return a new {@link ZetaWriteCommand} instance (single-use)
   */
  public <T> ZetaWriteCommand<T> write(String cacheKey) {
    return new ZetaWriteCommand<>(this, cacheKey);
  }

  /**
   * Truly atomic computeIfAbsent — the check, load, and store are one
   * indivisible operation backed by Caffeine's {@code asMap().computeIfAbsent()}.
   * <p>
   * Unlike the old {@link #get} path which does a TOCTOU-prone
   * getIfPresent → load → compute chain, this method guarantees that
   * the loader function runs at most once per key.
   *
   * @param cacheKey the key to retrieve
   * @param loader   the value supplier for cache misses
   * @param <V>      the value type
   * @return the cached or loaded value, or {@code null} if the loader returned {@code null}
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <V> V computeIfAbsent(String cacheKey, Supplier<V> loader) {
    return computeIfAbsent(cacheKey, loader, 0L, 0L, true);
  }

  /**
   * Truly atomic computeIfAbsent with explicit hard TTL.
   *
   * @param cacheKey  the key to retrieve
   * @param loader    the value supplier for cache misses
   * @param hardTtlMs hard TTL override (0 = use configured default; {@link Long#MAX_VALUE} for permanent entry)
   * @param <V>       the value type
   * @return the cached or loaded value, or {@code null} if the loader returned {@code null}
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <V> V computeIfAbsent(String cacheKey, Supplier<V> loader, long hardTtlMs) {
    return computeIfAbsent(cacheKey, loader, hardTtlMs, 0L, true);
  }

  /**
   * Truly atomic computeIfAbsent with explicit hard and soft TTLs.
   *
   * @param cacheKey  the key to retrieve
   * @param loader    the value supplier for cache misses
   * @param hardTtlMs hard TTL override (0 = use configured default; {@link Long#MAX_VALUE} for permanent entry)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param <V>       the value type
   * @return the cached or loaded value, or {@code null} if the loader returned {@code null}
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <V> V computeIfAbsent(String cacheKey, Supplier<V> loader, long hardTtlMs, long softTtlMs) {
    return computeIfAbsent(cacheKey, loader, hardTtlMs, softTtlMs, true);
  }

  /**
   * Truly atomic computeIfAbsent with explicit hard and soft TTLs.
   *
   * @param cacheKey  the key to retrieve
   * @param loader    the value supplier for cache misses
   * @param hardTtlMs hard TTL override (0 = use configured default; {@link Long#MAX_VALUE} for permanent entry)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param <V>       the value type
   * @return the cached or loaded value, or {@code null} if the loader returned {@code null}
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <V> V computeIfAbsent(
    String cacheKey,
    Supplier<V> loader,
    long hardTtlMs,
    long softTtlMs,
    boolean allowReport
  ) {
    requireAppCache("computeIfAbsent");
    requireAppDetector("computeIfAbsent");
    return hotKeyCache.computeIfAbsent(cacheKey, loader, hardTtlMs, softTtlMs, allowReport).orElse(null);
  }

  /**
   * Truly atomic computeIfAbsent with soft-expire (stale-while-revalidate).
   * <p>
   * Returns stale data immediately when the soft TTL has expired, triggering
   * an async refresh in the background.  On miss, the load-and-store is atomic.
   *
   * @param cacheKey the key to retrieve
   * @param loader   the value supplier for cache misses / refreshes
   * @param <V>      the value type
   * @return the cached (possibly stale) or loaded value, or {@code null} if the loader returned {@code null}
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <V> V computeIfAbsentWithSoftExpire(String cacheKey, Supplier<V> loader) {
    return computeIfAbsentWithSoftExpire(cacheKey, loader, 0L, 0L, true);
  }

  /**
   * Truly atomic computeIfAbsent with soft-expire and explicit soft TTL.
   *
   * @param cacheKey  the key to retrieve
   * @param loader    the value supplier for cache misses / refreshes
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param <V>       the value type
   * @return the cached (possibly stale) or loaded value, or {@code null} if the loader returned {@code null}
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <V> V computeIfAbsentWithSoftExpire(String cacheKey, Supplier<V> loader, long softTtlMs) {
    return computeIfAbsentWithSoftExpire(cacheKey, loader, 0L, softTtlMs, true);
  }

  /**
   * Truly atomic computeIfAbsent with soft-expire and explicit hard/soft TTLs.
   *
   * @param cacheKey  the key to retrieve
   * @param loader    the value supplier for cache misses / refreshes
   * @param hardTtlMs hard TTL override (0 = use configured default; {@link Long#MAX_VALUE} for pure logical expiry)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param <V>       the value type
   * @return the cached (possibly stale) or loaded value, or {@code null} if the loader returned {@code null}
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <V> V computeIfAbsentWithSoftExpire(String cacheKey, Supplier<V> loader, long hardTtlMs, long softTtlMs) {
    return computeIfAbsentWithSoftExpire(cacheKey, loader, hardTtlMs, softTtlMs, true);
  }

  /**
   * Truly atomic computeIfAbsent with soft-expire, explicit hard/soft TTLs,
   * and reporting control.
   *
   * @param cacheKey    the key to retrieve
   * @param loader      the value supplier for cache misses / refreshes
   * @param hardTtlMs   hard TTL override (0 = use configured default;
   *                    {@link Long#MAX_VALUE} for pure logical expiry)
   * @param softTtlMs   soft TTL override (0 = use configured default)
   * @param allowReport whether to allow reporting this access to the Worker
   * @param <V>         the value type
   * @return the cached (possibly stale) or loaded value, or {@code null} if the loader returned {@code null}
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <V> V computeIfAbsentWithSoftExpire(
    String cacheKey,
    Supplier<V> loader,
    long hardTtlMs,
    long softTtlMs,
    boolean allowReport
  ) {
    requireAppCache("computeIfAbsent");
    requireAppDetector("computeIfAbsent");
    return hotKeyCache.computeIfAbsentWithSoftExpire(cacheKey, loader, hardTtlMs, softTtlMs, allowReport).orElse(null);
  }

  /**
   * Atomically replace the cached value only if the current value equals
   * the expected value.  See {@link HotKeyCache#compareAndSet} for details.
   *
   * @param cacheKey the key to replace
   * @param expected the expected current value (nullable)
   * @param newValue the new value to cache (nullable)
   * @return {@code true} if the replacement was performed
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public boolean compareAndSet(String cacheKey, @Nullable Object expected, @Nullable Object newValue) {
    requireAppCache("compareAndSet");
    return hotKeyCache.compareAndSet(cacheKey, expected, newValue);
  }

  /**
   * Atomically invalidate the cached value only if the current value equals
   * the expected value.
   * <p>
   * No version bump or broadcast is performed — the invalidation is purely
   * local.  If cross-instance invalidation is needed, pair with a subsequent
   * call to {@link #invalidate(String, boolean)}.
   *
   * @param cacheKey the key to invalidate
   * @param expected the expected value (nullable)
   * @return {@code true} if the invalidation was performed
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public boolean compareAndInvalidate(String cacheKey, @Nullable Object expected) {
    requireAppCache("compareAndInvalidate");
    return hotKeyCache.compareAndInvalidate(cacheKey, expected);
  }

  /**
   * Convenience shorthand for
   * {@link #invalidateAfterPut(String, Runnable, boolean) invalidateAfterPut(cacheKey, mutation, true)}.
   *
   * @param cacheKey the key to invalidate after mutation
   * @param mutation the mutation to execute before invalidation
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void invalidateAfterPut(String cacheKey, Runnable mutation) {
    invalidateAfterPut(cacheKey, mutation, true);
  }

  /**
   * Execute a mutation, then invalidate L1 and optionally broadcast to peers.
   * If the mutation throws, invalidation is skipped.
   *
   * @param cacheKey               the key to invalidate after mutation
   * @param mutation               the mutation to execute before invalidation
   * @param isBroadcastByThisTime  whether to broadcast the invalidation to peer instances
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void invalidateAfterPut(String cacheKey, Runnable mutation, boolean isBroadcastByThisTime) {
    requireAppCache("invalidateAfterPut");
    requireAppDetector("invalidateAfterPut");
    hotKeyCache.invalidateAfterPut(cacheKey, mutation, isBroadcastByThisTime);
  }

  /**
   * Convenience shorthand for
   * {@link #invalidateAfterPut(Map, boolean) invalidateAfterPut(mutations, true)}.
   *
   * @param mutations a map of key → mutation pairs to execute before invalidation
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void invalidateAfterPut(Map<String, ? extends Runnable> mutations) {
    invalidateAfterPut(mutations, true);
  }

  /**
   * Execute mutations for multiple keys, then invalidate each from L1 and
   * optionally broadcast. Each key gets its own mutation. If a single mutation
   * fails, that key is skipped (invalidation and send are not performed).
   *
   * @param mutations              a map of key → mutation pairs
   * @param isBroadcastByThisTime  whether to broadcast the invalidation to peer instances
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void invalidateAfterPut(Map<String, ? extends Runnable> mutations, boolean isBroadcastByThisTime) {
    requireAppCache("invalidateAfterPut");
    requireAppDetector("invalidateAfterPut");
    hotKeyCache.invalidateAfterPut(mutations, isBroadcastByThisTime);
  }

  /**
   * Look up a cached value without loading or triggering hot-key detection.
   *
   * @param cacheKey the key to look up
   * @param <T>      the value type
   * @return an {@link Optional} containing the raw value if present
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public <T> Optional<T> peek(String cacheKey) {
    requireAppCache("peek");
    return hotKeyCache.peek(cacheKey);
  }

  /**
   * Batch variant of {@link #peek(String)}. Returns a map of key → value for
   * all keys that are present in L1. Missing keys are silently omitted.
   *
   * @param cacheKeys the keys to look up
   * @return a map of present key-value pairs (never {@code null})
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public Map<String, Object> peekAll(Collection<String> cacheKeys) {
    Map<String, Object> result = new HashMap<>();
    cacheKeys.forEach(key -> {
      Optional<Object> value = peek(key);
      value.ifPresent(v -> result.put(key, v));
    });

    return result;
  }

  /**
   * Get a value from L1 or load it via the reader.
   * Hot keys are promoted to L1 with configured hot TTLs; normal keys use default TTLs.
   *
   * @param cacheKey the key to retrieve
   * @param reader   the value supplier for cache misses
   * @param <T>      the value type
   * @return an {@link Optional} containing the cached or loaded value
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> get(String cacheKey, Supplier<T> reader) {
    return get(cacheKey, reader, 0L, 0L, true);
  }

  /**
   * Convenience shorthand for {@link #get(String, Supplier, long, long, boolean) get(cacheKey, reader, ...)}
   * with explicit report control.
   *
   * @param cacheKey           the key to retrieve
   * @param reader             the value supplier for cache misses
   * @param isReportByThisTime whether to report this access to the Worker
   * @param <T>                the value type
   * @return an {@link Optional} containing the cached or loaded value
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> get(String cacheKey, Supplier<T> reader, boolean isReportByThisTime) {
    return get(cacheKey, reader, 0L, 0L, isReportByThisTime);
  }

  /**
   * Convenience shorthand for {@link #get(String, Supplier, long, long, boolean) get(cacheKey, reader, ...)}
   * with explicit hard TTL.
   *
   * @param cacheKey  the key to retrieve
   * @param reader    the value supplier for cache misses
   * @param hardTtlMs hard TTL override (0 = use configured default; {@link Long#MAX_VALUE} for permanent entry)
   * @param <T>       the value type
   * @return an {@link Optional} containing the cached or loaded value
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> get(String cacheKey, Supplier<T> reader, long hardTtlMs) {
    return get(cacheKey, reader, hardTtlMs, 0L, true);
  }

  /**
   * Convenience shorthand for {@link #get(String, Supplier, long, long, boolean) get(cacheKey, reader, ...)}
   * with explicit hard TTL and report control.
   *
   * @param cacheKey           the key to retrieve
   * @param reader             the value supplier for cache misses
   * @param hardTtlMs          hard TTL override (0 = use configured default; {@link Long#MAX_VALUE} for permanent entry)
   * @param isReportByThisTime whether to report this access to the Worker
   * @param <T>                the value type
   * @return an {@link Optional} containing the cached or loaded value
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> get(String cacheKey, Supplier<T> reader, long hardTtlMs, boolean isReportByThisTime) {
    return get(cacheKey, reader, hardTtlMs, 0L, isReportByThisTime);
  }

  /**
   * Convenience shorthand for {@link #get(String, Supplier, long, long, boolean) get(cacheKey, reader, ...)}
   * with explicit hard and soft TTLs.
   *
   * @param cacheKey  the key to retrieve
   * @param reader    the value supplier for cache misses
   * @param hardTtlMs hard TTL override (0 = use configured default; {@link Long#MAX_VALUE} for permanent entry)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param <T>       the value type
   * @return an {@link Optional} containing the cached or loaded value
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> get(String cacheKey, Supplier<T> reader, long hardTtlMs, long softTtlMs) {
    return get(cacheKey, reader, hardTtlMs, softTtlMs, true);
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
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> get(
    String cacheKey,
    Supplier<T> reader,
    long hardTtlMs,
    long softTtlMs,
    boolean isReportByThisTime
  ) {
    requireAppCache("get");
    requireAppDetector("get");
    return hotKeyCache.get(cacheKey, reader, hardTtlMs, softTtlMs, isReportByThisTime);
  }

  /**
   * Convenience shorthand for {@link #get(Iterable, Function, long, long, boolean) get(cacheKeys, reader, ...)}.
   *
   * @param cacheKeys the keys to retrieve
   * @param reader    the value function for cache misses
   * @param <T>       the value type
   * @return a map of key → loaded or cached value (never {@code null})
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when any key matches a blacklist rule
   */
  public <T> Map<String, Optional<T>> get(Iterable<String> cacheKeys, Function<? super String, ? extends T> reader) {
    return get(cacheKeys, reader, 0L, 0L, true);
  }

  /**
   * Convenience shorthand for {@link #get(Iterable, Function, long, long, boolean) get(cacheKeys, reader, ...)}
   * with explicit report control.
   *
   * @param cacheKeys          the keys to retrieve
   * @param reader             the value function for cache misses
   * @param isReportByThisTime whether to report this access to the Worker
   * @param <T>                the value type
   * @return a map of key → loaded or cached value (never {@code null})
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when any key matches a blacklist rule
   */
  public <T> Map<String, Optional<T>> get(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader,
    boolean isReportByThisTime
  ) {
    return get(cacheKeys, reader, 0L, 0L, isReportByThisTime);
  }

  /**
   * Convenience shorthand for {@link #get(Iterable, Function, long, long, boolean) get(cacheKeys, reader, ...)}
   * with explicit hard TTL.
   *
   * @param cacheKeys the keys to retrieve
   * @param reader    the value function for cache misses
   * @param hardTtlMs hard TTL override (0 = use configured default; {@link Long#MAX_VALUE} for permanent entry)
   * @param <T>       the value type
   * @return a map of key → loaded or cached value (never {@code null})
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when any key matches a blacklist rule
   */
  public <T> Map<String, Optional<T>> get(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader,
    long hardTtlMs
  ) {
    return get(cacheKeys, reader, hardTtlMs, 0L, true);
  }

  /**
   * Convenience shorthand for {@link #get(Iterable, Function, long, long, boolean) get(cacheKeys, reader, ...)}
   * with explicit hard TTL and report control.
   *
   * @param cacheKeys          the keys to retrieve
   * @param reader             the value function for cache misses
   * @param hardTtlMs          hard TTL override (0 = use configured default; {@link Long#MAX_VALUE} for permanent entry)
   * @param isReportByThisTime whether to report this access to the Worker
   * @param <T>                the value type
   * @return a map of key → loaded or cached value (never {@code null})
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when any key matches a blacklist rule
   */
  public <T> Map<String, Optional<T>> get(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader,
    long hardTtlMs,
    boolean isReportByThisTime
  ) {
    return get(cacheKeys, reader, hardTtlMs, 0L, isReportByThisTime);
  }

  /**
   * Convenience shorthand for {@link #get(Iterable, Function, long, long, boolean) get(cacheKeys, reader, ...)}
   * with explicit hard and soft TTLs.
   *
   * @param cacheKeys the keys to retrieve
   * @param reader    the value function for cache misses
   * @param hardTtlMs hard TTL override (0 = use configured default; {@link Long#MAX_VALUE} for permanent entry)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param <T>       the value type
   * @return a map of key → loaded or cached value (never {@code null})
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when any key matches a blacklist rule
   */
  public <T> Map<String, Optional<T>> get(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader,
    long hardTtlMs,
    long softTtlMs
  ) {
    return get(cacheKeys, reader, hardTtlMs, softTtlMs, true);
  }

  /**
   * Batch variant of {@link #get(String, Supplier, long, long, boolean)}.
   * All keys share the same explicit TTL overrides.
   *
   * @param cacheKeys  the keys to retrieve
   * @param reader     the value function for cache misses
   * @param hardTtlMs  hard TTL override (0 = use configured default;
   *                   {@link Long#MAX_VALUE} for permanent entry)
   * @param softTtlMs  soft TTL override (0 = use configured default)
   * @param isReportByThisTime  whether to report this access to the Worker
   * @param <T>        the value type
   * @return a map of key → loaded or cached value (never {@code null})
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when any key matches a blacklist rule
   */
  public <T> Map<String, Optional<T>> get(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader,
    long hardTtlMs,
    long softTtlMs,
    boolean isReportByThisTime
  ) {
    requireAppCache("get");
    requireAppDetector("get");
    return hotKeyCache.get(cacheKeys, reader, hardTtlMs, softTtlMs, isReportByThisTime);
  }

  /**
   * Get with soft-expire (stale-while-revalidate). Returns cached value immediately
   * even if soft TTL expired, while triggering async refresh in background.
   *
   * @param cacheKey the key to retrieve
   * @param reader   the value supplier for cache misses / refreshes
   * @param <T>      the value type
   * @return an {@link Optional} containing the cached (possibly stale) or loaded value
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader) {
    return getWithSoftExpire(cacheKey, reader, 0L, 0L, true);
  }

  /**
   * Get with soft-expire and explicit soft TTL override.
   *
   * @param cacheKey  the key to retrieve
   * @param reader    the value supplier for cache misses / refreshes
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param <T>       the value type
   * @return an {@link Optional} containing the cached (possibly stale) or loaded value
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader, long softTtlMs) {
    return getWithSoftExpire(cacheKey, reader, 0L, softTtlMs, true);
  }

  /**
   * Convenience shorthand for
   * {@link #getWithSoftExpire(String, Supplier, long, long, boolean) getWithSoftExpire(cacheKey, reader, ...)}
   * with explicit report control.
   *
   * @param cacheKey           the key to retrieve
   * @param reader             the value supplier for cache misses / refreshes
   * @param isReportByThisTime whether to report this access to the Worker
   * @param <T>                the value type
   * @return an {@link Optional} containing the cached (possibly stale) or loaded value
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader, boolean isReportByThisTime) {
    return getWithSoftExpire(cacheKey, reader, 0L, 0L, isReportByThisTime);
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
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> getWithSoftExpire(
    String cacheKey,
    Supplier<T> reader,
    long hardTtlMs,
    long softTtlMs,
    boolean isReportByThisTime
  ) {
    requireAppCache("get");
    requireAppDetector("get");
    return hotKeyCache.getWithSoftExpire(cacheKey, reader, hardTtlMs, softTtlMs, isReportByThisTime);
  }

  /**
   * Convenience shorthand for
   * {@link #getWithSoftExpire(Iterable, Function, long, long, boolean) getWithSoftExpire(cacheKeys, reader, ...)}.
   *
   * @param cacheKeys the keys to retrieve
   * @param reader    the value function for cache misses / refreshes
   * @param <T>       the value type
   * @return a map of key → cached (possibly stale) or loaded value
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when any key matches a blacklist rule
   */
  public <T> Map<String, Optional<T>> getWithSoftExpire(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader
  ) {
    return getWithSoftExpire(cacheKeys, reader, 0L, 0L, true);
  }

  /**
   * Convenience shorthand for
   * {@link #getWithSoftExpire(Iterable, Function, long, long, boolean) getWithSoftExpire(cacheKeys, reader, ...)}
   * with explicit report control.
   *
   * @param cacheKeys          the keys to retrieve
   * @param reader             the value function for cache misses / refreshes
   * @param isReportByThisTime whether to report this access to the Worker
   * @param <T>                the value type
   * @return a map of key → cached (possibly stale) or loaded value
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when any key matches a blacklist rule
   */
  public <T> Map<String, Optional<T>> getWithSoftExpire(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader,
    boolean isReportByThisTime
  ) {
    return getWithSoftExpire(cacheKeys, reader, 0L, 0L, isReportByThisTime);
  }

  /**
   * Convenience shorthand for
   * {@link #getWithSoftExpire(Iterable, Function, long, long, boolean) getWithSoftExpire(cacheKeys, reader, ...)}
   * with explicit soft TTL.
   *
   * @param cacheKeys the keys to retrieve
   * @param reader    the value function for cache misses / refreshes
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param <T>       the value type
   * @return a map of key → cached (possibly stale) or loaded value
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when any key matches a blacklist rule
   */
  public <T> Map<String, Optional<T>> getWithSoftExpire(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader,
    long softTtlMs
  ) {
    return getWithSoftExpire(cacheKeys, reader, 0L, softTtlMs, true);
  }

  /**
   * Convenience shorthand for
   * {@link #getWithSoftExpire(Iterable, Function, long, long, boolean) getWithSoftExpire(cacheKeys, reader, ...)}
   * with explicit hard and soft TTLs.
   *
   * @param cacheKeys the keys to retrieve
   * @param reader    the value function for cache misses / refreshes
   * @param hardTtlMs hard TTL override (0 = use configured default; {@link Long#MAX_VALUE} for pure logical expiry)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param <T>       the value type
   * @return a map of key → cached (possibly stale) or loaded value
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when any key matches a blacklist rule
   */
  public <T> Map<String, Optional<T>> getWithSoftExpire(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader,
    long hardTtlMs,
    long softTtlMs
  ) {
    return getWithSoftExpire(cacheKeys, reader, hardTtlMs, softTtlMs, true);
  }

  /**
   * Batch variant of {@link #getWithSoftExpire(String, Supplier, long, long, boolean)}.
   * All keys share the same explicit TTL overrides.
   *
   * @param cacheKeys  the keys to retrieve
   * @param reader     the value function for cache misses / refreshes
   * @param hardTtlMs  hard TTL override (0 = use configured default;
   *                   {@link Long#MAX_VALUE} for pure logical expiry)
   * @param softTtlMs  soft TTL override (0 = use configured default)
   * @param isReportByThisTime  whether to report this access to the Worker
   * @param <T>        the value type
   * @return a map of key → cached (possibly stale) or loaded value
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when any key matches a blacklist rule
   */
  public <T> Map<String, Optional<T>> getWithSoftExpire(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader,
    long hardTtlMs,
    long softTtlMs,
    boolean isReportByThisTime
  ) {
    requireAppCache("get");
    requireAppDetector("get");
    return hotKeyCache.getWithSoftExpire(cacheKeys, reader, hardTtlMs, softTtlMs, isReportByThisTime);
  }

  /**
   * Convenience shorthand for {@link #invalidate(String, boolean) invalidate(cacheKey, true)}.
   *
   * @param cacheKey the key to invalidate
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void invalidate(String cacheKey) {
    invalidate(cacheKey, true);
  }

  /**
   * Invalidate a single key from L1 and broadcast REFRESH to peers.
   * The next {@link #get} will re-fetch from the reader.
   *
   * @param cacheKey               the key to invalidate
   * @param isBroadcastByThisTime  whether to broadcast the invalidation to peer instances
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void invalidate(String cacheKey, boolean isBroadcastByThisTime) {
    requireAppCache("invalidate");
    hotKeyCache.invalidate(cacheKey, isBroadcastByThisTime);
  }

  /**
   * Convenience shorthand for {@link #invalidate(Collection, boolean) invalidate(cacheKeys, true)}.
   *
   * @param cacheKeys the keys to invalidate
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void invalidate(Collection<String> cacheKeys) {
    invalidate(cacheKeys, true);
  }

  /**
   * Invalidate a collection of keys from L1 and broadcast INVALIDATE for each.
   *
   * @param cacheKeys              the keys to invalidate
   * @param isBroadcastByThisTime  whether to broadcast the invalidation to peer instances
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void invalidate(Collection<String> cacheKeys, boolean isBroadcastByThisTime) {
    requireAppCache("invalidate");
    hotKeyCache.invalidate(cacheKeys, isBroadcastByThisTime);
  }

  /**
   * Write-through: execute the writer, then update L1 and broadcast.
   * Uses effective hard/soft TTL from configuration.
   *
   * @param cacheKey the key to write
   * @param value    the value to cache
   * @param writer   the data-source mutation to execute before caching
   * @param <T>      the value type
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public <T> void putThrough(String cacheKey, T value, Runnable writer) {
    putThrough(cacheKey, value, writer, 0L, 0L, true);
  }

  /**
   * Convenience shorthand for
   * {@link #putThrough(String, Object, Runnable, long, long, boolean) putThrough(cacheKey, value, writer, ...)}
   * with explicit broadcast control.
   *
   * @param cacheKey               the key to write
   * @param value                  the value to cache
   * @param writer                 the data-source mutation to execute before caching
   * @param isBroadcastByThisTime  whether to broadcast the update to peer instances
   * @param <T>                    the value type
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public <T> void putThrough(String cacheKey, T value, Runnable writer, boolean isBroadcastByThisTime) {
    putThrough(cacheKey, value, writer, 0L, 0L, isBroadcastByThisTime);
  }

  /**
   * Convenience shorthand for
   * {@link #putThrough(String, Object, Runnable, long, long, boolean) putThrough(cacheKey, value, writer, ...)}
   * with explicit hard TTL.
   *
   * @param cacheKey  the key to write
   * @param value     the value to cache
   * @param writer    the data-source mutation to execute before caching
   * @param hardTtlMs hard TTL override (0 = use configured default; {@link Long#MAX_VALUE} for permanent entry)
   * @param <T>       the value type
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public <T> void putThrough(String cacheKey, T value, Runnable writer, long hardTtlMs) {
    putThrough(cacheKey, value, writer, hardTtlMs, 0L, true);
  }

  /**
   * Convenience shorthand for
   * {@link #putThrough(String, Object, Runnable, long, long, boolean) putThrough(cacheKey, value, writer, ...)}
   * with explicit hard TTL and broadcast control.
   *
   * @param cacheKey               the key to write
   * @param value                  the value to cache
   * @param writer                 the data-source mutation to execute before caching
   * @param hardTtlMs              hard TTL override (0 = use configured default; {@link Long#MAX_VALUE} for permanent entry)
   * @param isBroadcastByThisTime  whether to broadcast the update to peer instances
   * @param <T>                    the value type
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public <T> void putThrough(String cacheKey, T value, Runnable writer, long hardTtlMs, boolean isBroadcastByThisTime) {
    putThrough(cacheKey, value, writer, hardTtlMs, 0L, isBroadcastByThisTime);
  }

  /**
   * Convenience shorthand for
   * {@link #putThrough(String, Object, Runnable, long, long, boolean) putThrough(cacheKey, value, writer, ...)}
   * with explicit hard and soft TTLs.
   *
   * @param cacheKey  the key to write
   * @param value     the value to cache
   * @param writer    the data-source mutation to execute before caching
   * @param hardTtlMs hard TTL override (0 = use configured default; {@link Long#MAX_VALUE} for permanent entry)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param <T>       the value type
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public <T> void putThrough(String cacheKey, T value, Runnable writer, long hardTtlMs, long softTtlMs) {
    putThrough(cacheKey, value, writer, hardTtlMs, softTtlMs, true);
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
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public <T> void putThrough(
    String cacheKey,
    T value,
    Runnable writer,
    long hardTtlMs,
    long softTtlMs,
    boolean isBroadcastByThisTime
  ) {
    requireAppCache("putThrough");
    hotKeyCache.putThrough(cacheKey, value, writer, hardTtlMs, softTtlMs, isBroadcastByThisTime);
  }

  /**
   * Write a value directly into the local L1 cache without version bump,
   * without broadcast, without hot-key detection, and without reporting.
   * <p>
   * Existing entry metadata is preserved.  If no entry exists, a fresh
   * {@link io.github.hyshmily.zeta.model.CacheEntry} is created with {@link io.github.hyshmily.zeta.model.KeyState#NORMAL}.
   * <p>
   * Never throws {@code UnsupportedOperationException} in Worker-only mode;
   * silently no-ops instead.
   *
   * @param cacheKey the key to store
   * @param value    the value to cache
   */
  public void putLocal(String cacheKey, Object value) {
    putLocal(cacheKey, value, 0, 0);
  }

  /**
   * Convenience shorthand for {@link #putLocal(String, Object, long, long) putLocal(cacheKey, value, hardTtlMs, ...)}
   * with explicit hard TTL. Soft TTL uses the configured default.
   *
   * <p>In Worker-only mode this method silently no-ops.
   *
   * @param cacheKey the key to store
   * @param value    the value to cache
   * @param hardTtlMs hard TTL override (0 = use configured default; {@link Long#MAX_VALUE} for permanent entry)
   */
  public void putLocal(String cacheKey, Object value, long hardTtlMs) {
    putLocal(cacheKey, value, hardTtlMs, 0);
  }

  /**
   * Write a value directly into the local L1 cache with explicit TTL overrides.
   * Delegates to {@link #putLocal(String, Object)} semantics — no version bump,
   * no broadcast, no hot-key detection, and no reporting.
   * <p>
   * Pass {@code 0} for either TTL to use the configured default.
   *
   * <p>In Worker-only mode this method silently no-ops.
   *
   * @param cacheKey  the key to store
   * @param value     the value to cache
   * @param hardTtlMs hard TTL override (0 = use configured default)
   * @param softTtlMs soft TTL override (0 = use configured default)
   */
  public void putLocal(String cacheKey, Object value, long hardTtlMs, long softTtlMs) {
    if (hotKeyCache == null) {
      return; // Worker-only mode: silently no-op
    }
    hotKeyCache.putLocal(cacheKey, value, hardTtlMs, softTtlMs);
  }

  /**
   * Estimated number of entries currently in the L1 cache.
   * <p>
   * This is a lightweight, best-effort estimate from the underlying Caffeine
   * cache, suitable for monitoring dashboards and capacity planning.
   *
   * @return estimated entry count, or {@code 0} in Worker-only mode
   */
  public long estimatedSize() {
    if (hotKeyCache == null) {
      return 0L;
    }
    return hotKeyCache.estimatedSize();
  }

  /**
   * Return a snapshot of basic L1 cache statistics.
   * <p>
   * Hit/miss/eviction counters are populated only when Caffeine's
   * {@code recordStats()} is enabled. {@code estimatedSizeOfKeysCount} is always
   * available.
   *
   * @return a {@link ZetaCacheStats} record, or {@code null} in Worker-only mode;
   *         hit/miss counters are {@code 0} if stats recording is not enabled
   */
  public ZetaCacheStats stats() {
    if (hotKeyCache == null) {
      return null;
    }
    return hotKeyCache.stats();
  }

  /**
   * Return the underlying Caffeine cache for direct access.
   *
   * <p>Useful for Caffeine-specific operations such as {@code asMap()},
   * {@code policy()}, and {@code cleanUp()}. Use with caution — bypassing
   * the HotKey orchestration layer can lead to inconsistent cache state.
   *
   * @return the raw Caffeine {@link Cache} instance
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public Cache<String, Object> getLocalCache() {
    requireAppCache("getLocalCache");
    return hotKeyCache.getLocalCache();
  }

  /**
   * Invalidate all entries from the L1 cache without broadcasting.
   * <p>
   * This is an emergency flush — all cached values are removed immediately.
   * No cross-instance sync messages are sent. Subsequent {@link #get} calls
   * will reload data from the reader.
   * <p>
   * Use with caution: flushing the local cache increases load on the backend
   * until entries are re-cached.
   *
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void invalidateAllLocal() {
    requireAppCache("invalidateAllLocal");
    hotKeyCache.invalidateAllLocal();
  }

  /**
   * Attempt to acquire a distributed lock with the provider's default
   * retry counts.
   *
   * <p>Equivalent to {@code tryLock(key, expire, unit, -1, -1, -1)}.
   *
   * <p>Usage:
   * <pre>{@code
   * try (AutoReleaseLock lock = hotKey.tryLock("order:42", 5, TimeUnit.SECONDS)) {
   *     if (lock != null) {
   *         // critical section
   *     }
   * }
   * }</pre>
   *
   * @param key    the lock key
   * @param expire the time-to-live for the lock
   * @param unit   the time unit for {@code expire}
   * @return a lock handle if acquired, or {@code null} if the lock is held
   *         by another caller or the provider is unavailable
   */
  @Nullable
  public AutoReleaseLock tryLock(String key, long expire, TimeUnit unit) {
    return lockProvider != null ? lockProvider.tryLock(key, expire, unit) : null;
  }

  /**
   * Acquire a distributed lock, execute the action, and release
   * with the provider's default retry counts.
   *
   * <p>Equivalent to {@code tryLockAndRun(key, expire, unit, action, -1, -1, -1)}.
   *
   * @param key    the lock key
   * @param expire the time-to-live for the lock
   * @param unit   the time unit for {@code expire}
   * @param action the action to execute while holding the lock
   * @return {@code true} if the lock was acquired and the action ran,
   *         {@code false} otherwise
   */
  public boolean tryLockAndRun(String key, long expire, TimeUnit unit, Runnable action) {
    Objects.requireNonNull(action, "action must not be null");
    if (lockProvider == null) {
      return false;
    }
    try (AutoReleaseLock lock = tryLock(key, expire, unit)) {
      if (lock != null) {
        action.run();
        return true;
      }
      return false;
    }
  }

  /**
   * Attempt to acquire a distributed lock with explicit retry counts.
   *
   * <p>Any negative count falls back to the provider's configured default.
   * Returns {@code null} when the lock is held by another caller or when
   * no Redis is available (graceful degradation).
   *
   * @param key                  the lock key
   * @param expire               the time-to-live for the lock
   * @param unit                 the time unit for {@code expire}
   * @param tryLockLockCount     the number of {@code SET NX} retries;
   *                             negative → default
   * @param tryLockInquiryCount  the number of {@code GET} inquiries;
   *                             negative → default
   * @param tryLockUnlockCount   the number of {@code DEL} retries;
   *                             negative → default
   * @return a lock handle if acquired, or {@code null} if the lock is held
   *         by another caller or the provider is unavailable
   */
  @Nullable
  public AutoReleaseLock tryLock(
    String key,
    long expire,
    TimeUnit unit,
    int tryLockLockCount,
    int tryLockInquiryCount,
    int tryLockUnlockCount
  ) {
    return lockProvider != null
      ? lockProvider.tryLock(key, expire, unit, tryLockLockCount, tryLockInquiryCount, tryLockUnlockCount)
      : null;
  }

  /**
   * Acquire a distributed lock with explicit retry counts, execute the
   * action, and release.
   *
   * <p>Any negative count falls back to the provider's configured default.
   *
   * @param key                  the lock key
   * @param expire               the time-to-live for the lock
   * @param unit                 the time unit for {@code expire}
   * @param action               the action to execute while holding the lock
   * @param tryLockLockCount     the number of {@code SET NX} retries;
   *                             negative → default
   * @param tryLockInquiryCount  the number of {@code GET} inquiries;
   *                             negative → default
   * @param tryLockUnlockCount   the number of {@code DEL} retries;
   *                             negative → default
   * @return {@code true} if the lock was acquired and the action ran,
   *         {@code false} otherwise
   */
  public boolean tryLockAndRun(
    String key,
    long expire,
    TimeUnit unit,
    Runnable action,
    int tryLockLockCount,
    int tryLockInquiryCount,
    int tryLockUnlockCount
  ) {
    Objects.requireNonNull(action, "action must not be null");
    try (AutoReleaseLock lock = tryLock(key, expire, unit, tryLockLockCount, tryLockInquiryCount, tryLockUnlockCount)) {
      if (lock != null) {
        action.run();
        return true;
      }
      return false;
    }
  }

  /**
   * Lazy-initialize the refresh scheduler.
   * Uses double-checked locking for thread safety.
   */
  private ScheduledExecutorService getScheduler() {
    ScheduledExecutorService s = refreshScheduler;
    if (s == null) {
      synchronized (this) {
        s = refreshScheduler;
        if (s == null) {
          s = Executors.newScheduledThreadPool(2, new ZetaThreadFactory("zeta-refresh"));
          refreshScheduler = s;
        }
      }
    }
    return s;
  }

  /**
   * Register a timed background refresh for the given key.
   * <p>
   * Schedules {@link #getWithSoftExpire} at an interval slightly longer than
   * the soft TTL ({@code softTtlMs × 1.1}) to guarantee the entry is stale
   * when the timer fires, ensuring every cycle triggers an async refresh via
   * {@code triggerBackgroundRefresh}.
   * <p>
   * The scheduler is created lazily on first registration (no threads before
   * first use). Uses a 2-thread pool to prevent a slow supplier from blocking
   * other refresh keys.
   * <p>
   * If a previous registration exists for the same key, it is cancelled and
   * replaced.
   *
   * @param key       the cache key to refresh
   * @param supplier  the value supplier for refresh
   * @param hardTtlMs hard TTL override (0 = use configured default)
   * @param softTtlMs soft TTL override (also used as the base interval)
   * @param <T>       the value type
   */
  public <T> void registerRefresh(String key, Supplier<T> supplier, long hardTtlMs, long softTtlMs) {
    long intervalMs = (long) (softTtlMs * 1.1);
    if (intervalMs < 1) {
      intervalMs = 1;
    }
    ScheduledFuture<?> prev = refreshFutures.put(
      key,
      getScheduler().scheduleWithFixedDelay(
        () -> getWithSoftExpire(key, supplier, hardTtlMs, softTtlMs, true),
        intervalMs,
        intervalMs,
        TimeUnit.MILLISECONDS
      )
    );
    if (prev != null) {
      prev.cancel(false);
    }
  }

  /**
   * Update the refresh configuration for a key at runtime.
   * <p>
   * Cancels any existing scheduled task and registers a new one with the
   * given TTLs. If no prior registration exists, this behaves identically
   * to {@link #registerRefresh}.
   *
   * @param key       the cache key
   * @param supplier  the value supplier for refresh
   * @param hardTtlMs new hard TTL override
   * @param softTtlMs new soft TTL override (also used as the base interval)
   * @param <T>       the value type
   */
  public <T> void updateRefresh(String key, Supplier<T> supplier, long hardTtlMs, long softTtlMs) {
    registerRefresh(key, supplier, hardTtlMs, softTtlMs);
  }

  /**
   * Cancel the timed refresh for the given key.
   *
   * @param key the cache key to stop refreshing
   */
  public void unregisterRefresh(String key) {
    ScheduledFuture<?> f = refreshFutures.remove(key);
    if (f != null) {
      f.cancel(false);
    }
  }

  /**
   * Cancel all timed refreshes and shut down the refresh scheduler.
   * Called automatically by Spring when this bean is destroyed.
   */
  @Override
  public void destroy() {
    refreshFutures.values().forEach(f -> f.cancel(false));
    refreshFutures.clear();
    ScheduledExecutorService s = refreshScheduler;
    if (s != null) {
      s.shutdown();
    }
  }

  //------------------------------------------------------------------------

  /**
   * Check whether a key is currently tracked as a local hot key in L1.
   *
   * @param cacheKey the key to inspect
   * @return {@code true} if the key exists in L1 with {@link io.github.hyshmily.zeta.model.KeyState#HOT}
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public boolean isLocalHotKey(String cacheKey) {
    requireAppCache("isHot");
    requireAppDetector("isHot");
    return hotKeyCache.isHot(cacheKey);
  }

  /**
   * Batch variant of {@link #isLocalHotKey(String)}. Returns a map of key →
   * hot status for all given keys.
   *
   * <p><b>Batch execution:</b> Iterates sequentially; for large batches
   * consider parallelizing in caller code.
   *
   * @param cacheKeys the keys to inspect
   * @return a map of key → whether it is a local hot key
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public Map<String, Boolean> areLocalHotKeys(Collection<String> cacheKeys) {
    Map<String, Boolean> result = new HashMap<>();
    cacheKeys.forEach(key -> result.put(key, isLocalHotKey(key)));
    return result;
  }

  /**
   * Increment the local TopK detector directly, bypassing the buffer and the
   * report-to-Worker path. Useful for bulk-loading historical access patterns
   * or correcting frequency counts.
   *
   * @param cacheKey the key to record
   * @param count    the number of accesses to add
   */
  public void notifyLocalDetectorDirect(String cacheKey, int count) {
    requireAppDetector("notifyLocalDetectorDirect");
    appHotKeyDetector.addDirect(cacheKey, count);
  }

  /**
   * Batch-increment the local TopK detector directly, bypassing buffer and reports.
   *
   * @param keyCounts a map of key → access count
   */
  public void notifyLocalDetectorDirect(Map<String, Long> keyCounts) {
    requireAppDetector("notifyLocalDetectorDirect");
    appHotKeyDetector.addDirect(keyCounts);
  }

  /**
   * Notify the local TopK detector that a key was accessed, without triggering
   * a report to the Worker. Used by {@code @Intercept} path to keep the local
   * frequency sketch accurate without flooding the Worker with reports.
   * Null keys are silently ignored.
   *
   * @param cacheKey the accessed key (maybe {@code null}, silently ignored)
   */
  public void notifyLocalDetector(String cacheKey) {
    requireAppDetector("notifyLocalDetector");
    appHotKeyDetector.add(cacheKey);
  }

  /**
   * Notify the local detector of key access with a custom delta, routing
   * through the buffered counter. Null keys are silently ignored.
   *
   * @param cacheKey the accessed key (maybe {@code null}, silently ignored)
   * @param count    the number of accesses to record
   */
  public void notifyLocalDetector(String cacheKey, long count) {
    requireAppDetector("notifyLocalDetector");
    appHotKeyDetector.add(cacheKey, count);
  }

  /**
   * Batch-notify the local detector of multiple key accesses, routing through
   * the buffered counter. Null keys in the map are silently ignored.
   *
   * @param keyCounts a map of key → access count
   */
  public void notifyLocalDetector(Map<String, Long> keyCounts) {
    requireAppDetector("notifyLocalDetector");
    appHotKeyDetector.add(keyCounts);
  }

  /**
   * Evict the given key locally, then load and cache via the supplier.
   * Uses default TTLs. No broadcast is sent for the eviction.
   *
   * @param cacheKey the key to refresh
   * @param loader   the value supplier
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void refresh(String cacheKey, Supplier<?> loader) {
    requireAppCache("refresh");
    invalidate(cacheKey, false);
    putThrough(cacheKey, loader.get(), () -> {});
  }

  /**
   * Batch variant of {@link #refresh(String, Supplier)}. Refreshes all entries
   * by evicting locally then loading and caching via the provided suppliers.
   *
   * @param loaders a map of key → value supplier
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void refreshAll(Map<String, Supplier<?>> loaders) {
    requireAppCache("refreshAll");
    loaders.forEach(this::refresh);
  }

  /**
   * Evict the given key locally, then load and cache with explicit TTL overrides.
   * No broadcast is sent for the eviction.
   *
   * @param cacheKey     the key to refresh
   * @param loader       the value supplier
   * @param hotHardTtlMs hard TTL override (0 = use configured default; {@link Long#MAX_VALUE} for permanent entry)
   * @param hotSoftTtlMs soft TTL override (0 = use configured default)
   * @param <V>          the value type
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public <V> void refresh(String cacheKey, Supplier<V> loader, long hotHardTtlMs, long hotSoftTtlMs) {
    requireAppCache("refresh");
    invalidate(cacheKey, false);
    putThrough(cacheKey, loader.get(), () -> {}, hotHardTtlMs, hotSoftTtlMs, true);
  }

  /**
   * Check whether a key is currently tracked as a cluster-wide hot key by the
   * Worker-side global detector.
   *
   * @param cacheKey the key to inspect
   * @return {@code true} if the key appears in the Worker TopK list;
   *         {@code false} if the key is {@code null} or no Worker is active
   */
  public boolean isWorkerHotKey(String cacheKey) {
    return workerTopKAlgorithm != null && cacheKey != null && workerTopKAlgorithm.contains(cacheKey);
  }

  /**
   * Batch variant of {@link #isWorkerHotKey(String)}. Returns a map of key →
   * Worker hot status for all given keys.
   *
   * <p><b>Batch execution:</b> Iterates sequentially; for large batches
   * consider parallelizing in caller code.
   *
   * @param cacheKeys the keys to inspect
   * @return a map of key → whether it is a cluster-wide hot key;
   *         all entries are {@code false} when no Worker is active
   */
  public Map<String, Boolean> areWorkerHotKeys(Collection<String> cacheKeys) {
    Map<String, Boolean> result = new HashMap<>();
    cacheKeys.forEach(key -> result.put(key, isWorkerHotKey(key)));
    return result;
  }

  /**
   * Return the top N hot keys from the local detector, ordered by frequency.
   * Useful when callers need more or fewer items than the configured TopK capacity.
   *
   * @param n the number of top keys to return
   * @return the top N items, or an empty list if no detector is available
   */
  public List<Item> returnLocalTopNHotKeys(int n) {
    return appHotKeyDetector != null ? appHotKeyDetector.listTopN(n) : List.of();
  }

  /**
   * Return a blocking queue of recently expelled hot keys from the local detector.
   * Consumers can drain this queue to react to keys that are no longer hot.
   *
   * @return the expelled queue, or an empty queue if no detector is available
   */
  public BlockingQueue<Item> returnLocalExpelledHotKeys() {
    return appHotKeyDetector != null ? appHotKeyDetector.expelled() : new LinkedBlockingQueue<>();
  }

  /**
   * Return the total number of data streams (accesses) tracked by the local detector.
   *
   * @return the total count, or {@code 0} if no detector is available
   */
  public long returnLocalTotalDataStreams() {
    return appHotKeyDetector != null ? appHotKeyDetector.total() : 0L;
  }

  /**
   * Return the current top-K hot keys (key + count) from the local detector,
   * ordered by frequency.
   *
   * @return the local top-K list, or an empty list if no detector is available;
   *         the returned list is a point-in-time snapshot
   */
  public List<Item> returnLocalHotKeys() {
    return appHotKeyDetector != null ? appHotKeyDetector.list() : List.of();
  }

  /**
   * Return the current top-K hot keys from the Worker-side global detector,
   * ordered by frequency. These keys reflect cross-instance aggregated access
   * counts and are updated via periodic reports from all application instances.
   *
   * @return the cluster-wide top-K list, or an empty list if no Worker is active;
   *         the returned list is a point-in-time snapshot
   */
  public List<Item> returnWorkerHotKeys() {
    return workerTopKAlgorithm != null ? workerTopKAlgorithm.list() : List.of();
  }

  /**
   * Return a blocking queue of recently expelled hot keys from the Worker-side
   * global detector. Consumers can drain this queue to react to keys that are
   * no longer considered cluster-wide hot.
   *
   * @return the expelled queue, or an empty queue if no Worker is active
   */
  public BlockingQueue<Item> returnWorkerExpelledHotKeys() {
    return workerTopKAlgorithm != null ? workerTopKAlgorithm.expelled() : new LinkedBlockingQueue<>();
  }

  /**
   * Return the total number of data streams tracked by the Worker-side
   * global detector.
   *
   * @return the total count, or {@code 0} if no Worker is active
   */
  public long returnWorkerTotalDataStreams() {
    return workerTopKAlgorithm != null ? workerTopKAlgorithm.total() : 0L;
  }

  //-------------------------------------------------------------------------------------

  /**
   * Add a key pattern to the blacklist. Keys matching this pattern will be
   * blocked from cache get/put operations (returns {@link Optional#empty()}).
   * <p>The pattern is auto-detected by {@link RuleMatcher#of}: exact match,
   * prefix (trailing {@code *}), wildcard (containing {@code *} or {@code ?}),
   * or regex (prefixed with {@code regex:}).
   *
   * @param keyPattern the key pattern to blacklist
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void addBlacklist(String keyPattern) {
    requireAppCache("addBlacklist");
    hotKeyCache.addBlacklist(keyPattern);
  }

  /**
   * Batch variant of {@link #addBlacklist(String)}. Adds multiple key patterns
   * to the blacklist.
   *
   * @param keyPatterns the key patterns to blacklist
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void addBlacklist(Collection<String> keyPatterns) {
    requireAppCache("addBlacklist");
    keyPatterns.forEach(hotKeyCache::addBlacklist);
  }

  /**
   * Remove a key pattern from the blacklist.
   *
   * @param keyPattern the key pattern to remove from the blacklist
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void removeBlacklist(String keyPattern) {
    requireAppCache("removeBlacklist");
    hotKeyCache.unBlacklist(keyPattern);
  }

  /**
   * Batch variant of {@link #removeBlacklist(String)}. Removes multiple key
   * patterns from the blacklist.
   *
   * @param keyPatterns the key patterns to remove from the blacklist
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void removeBlacklist(Collection<String> keyPatterns) {
    requireAppCache("removeBlacklist");
    keyPatterns.forEach(hotKeyCache::unBlacklist);
  }

  /**
   * Add a key pattern to the whitelist. Keys matching this pattern will
   * skip report recording (no Worker report sent) but still participate in
   * normal cache get/put and local hot-key detection.
   * <p>The pattern is auto-detected by {@link RuleMatcher#of}.
   *
   * @param keyPattern the key pattern to whitelist
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void addWhitelist(String keyPattern) {
    requireAppCache("addWhitelist");
    hotKeyCache.addWhitelist(keyPattern);
  }

  /**
   * Batch variant of {@link #addWhitelist(String)}. Adds multiple key patterns
   * to the whitelist.
   *
   * @param keyPatterns the key patterns to whitelist
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void addWhitelist(Collection<String> keyPatterns) {
    requireAppCache("addWhitelist");
    keyPatterns.forEach(hotKeyCache::addWhitelist);
  }

  /**
   * Remove a key pattern from the whitelist.
   *
   * @param keyPattern the key pattern to remove from the whitelist
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void removeWhitelist(String keyPattern) {
    requireAppCache("removeWhitelist");
    hotKeyCache.unWhitelist(keyPattern);
  }

  /**
   * Batch variant of {@link #removeWhitelist(String)}. Removes multiple key
   * patterns from the whitelist.
   *
   * @param keyPatterns the key patterns to remove from the whitelist
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void removeWhitelist(Collection<String> keyPatterns) {
    requireAppCache("removeWhitelist");
    keyPatterns.forEach(hotKeyCache::unWhitelist);
  }

  /**
   * Evaluate all rules against the given key and return the first matching action.
   *
   * @param cacheKey the key to evaluate
   * @return the matching {@link Rule.RuleAction}, or {@code ALLOW} if no rule matches
   *         or no cache is available (Worker-only mode)
   */
  public Rule.RuleAction evaluateRule(String cacheKey) {
    if (hotKeyCache == null) {
      return Rule.RuleAction.ALLOW;
    }
    return hotKeyCache.evaluateRule(cacheKey);
  }

  /**
   * Batch variant of {@link #evaluateRule(String)}. Evaluates all rules against
   * the given keys and returns the first matching action for each.
   *
   * @param cacheKeys the keys to evaluate
   * @return a map of key → matching {@link Rule.RuleAction} (or {@code ALLOW} if no rule matches)
   */
  public Map<String, Rule.RuleAction> evaluateRules(Collection<String> cacheKeys) {
    Map<String, Rule.RuleAction> result = new HashMap<>();
    cacheKeys.forEach(key -> {
      Rule.RuleAction action = evaluateRule(key);
      result.put(key, action);
    });

    return result;
  }

  /**
   * Quickly check whether the given key is blocked by any blacklist rule.
   * <p>
   * Equivalent to {@code evaluateRule(key) == BLOCK}, provided as a convenience
   * for guard clauses before expensive operations.
   *
   * @param cacheKey the key to check
   * @return {@code true} if a blacklist rule matches the key, {@code false} if no
   *         rule matches or no cache is available (Worker-only mode)
   */
  public boolean isBlacklisted(String cacheKey) {
    if (hotKeyCache == null) {
      return false;
    }
    return hotKeyCache.isBlacklisted(cacheKey);
  }

  /**
   * Batch variant of {@link #isBlacklisted(String)}. Checks multiple keys
   * against blacklist rules.
   *
   * @param cacheKeys the keys to check
   * @return a map of key → whether it is blocked by a blacklist rule
   */
  public Map<String, Boolean> isBlacklisted(Collection<String> cacheKeys) {
    Map<String, Boolean> result = new HashMap<>();
    cacheKeys.forEach(key -> {
      boolean isBlacklisted = isBlacklisted(key);
      result.put(key, isBlacklisted);
    });

    return result;
  }

  /**
   * Quickly check whether the given key is whitelisted (skips Worker reporting).
   * <p>
   * Equivalent to {@code evaluateRule(key) == ALLOW_NO_REPORT}, provided as a
   * convenience for debugging and monitoring.
   *
   * @param cacheKey the key to check
   * @return {@code true} if a whitelist rule matches the key, {@code false} if no
   *         rule matches or no cache is available (Worker-only mode)
   */
  public boolean isWhitelisted(String cacheKey) {
    if (hotKeyCache == null) {
      return false;
    }
    return hotKeyCache.isWhitelisted(cacheKey);
  }

  /**
   * Batch variant of {@link #isWhitelisted(String)}. Checks multiple keys
   * against whitelist rules.
   *
   * @param cacheKeys the keys to check
   * @return a map of key → whether it is whitelisted (skips Worker reporting)
   */
  public Map<String, Boolean> isWhitelisted(Collection<String> cacheKeys) {
    Map<String, Boolean> result = new HashMap<>();
    cacheKeys.forEach(key -> {
      boolean isWhitelisted = isWhitelisted(key);
      result.put(key, isWhitelisted);
    });

    return result;
  }

  /**
   * Return a snapshot of all current rules in evaluation order.
   *
   * @return list of rules, or an empty list if no cache is available;
   *         the returned list is unmodifiable
   */
  public List<Rule> getAllRules() {
    if (hotKeyCache == null) {
      return List.of();
    }
    return hotKeyCache.getAllRules();
  }

  /**
   * Remove all blacklist and whitelist rules.
   *
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void clearAllRules() {
    requireAppCache("clearAllRules");
    hotKeyCache.clearAllRules();
  }

  /**
   * Broadcast all local rules to peer instances via the sync exchange.
   * Useful for initial synchronization when a new instance joins the cluster.
   *
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void broadcastAllLocalRulesManually() {
    requireAppCache("broadcastAllLocalRulesManually");
    hotKeyCache.broadcastAllLocalRulesManually();
  }

  /**
   * Whether this instance has app-side cache available.
   *
   * <p>Returns {@code true} in App-only and Coexistence modes,
   * {@code false} in Worker-only mode. Callers can use this to guard
   * cache-dependent operations without catching {@link ZetaModeException}.
   *
   * @return {@code true} if cache-dependent APIs (get, put, invalidate, etc.) are usable
   */
  public boolean isApp() {
    return hotKeyCache != null;
  }

  /**
   * Whether this instance has Worker-side TopK available.
   *
   * <p>Returns {@code true} in Worker-only and Coexistence modes,
   * {@code false} in App-only mode.
   *
   * @return {@code true} if Worker TopK queries (returnWorkerHotKeys, etc.) are usable
   */
  public boolean isWorker() {
    return workerTopKAlgorithm != null;
  }

  /**
   * Whether this instance is in pure App-only mode (cache present, Worker TopK absent).
   *
   * @return {@code true} if App-only mode
   */
  public boolean isAppOnly() {
    return hotKeyCache != null && workerTopKAlgorithm == null;
  }

  /**
   * Whether this instance is in pure Worker-only mode (cache absent, Worker TopK present).
   *
   * @return {@code true} if Worker-only mode
   */
  public boolean isWorkerOnly() {
    return hotKeyCache == null && workerTopKAlgorithm != null;
  }

  /**
   * Returns a human-readable label of the current deployment mode based on the
   * presence/absence of the cache and Worker TopK fields.
   */
  private String currentModeLabel() {
    if (hotKeyCache != null && workerTopKAlgorithm != null) {
      return "Coexistence mode";
    }
    if (hotKeyCache != null) {
      return "App-only mode";
    }
    if (workerTopKAlgorithm != null) {
      return "Worker-only mode";
    }
    return "Uninitialized mode (no cache, no TopK)";
  }

  private void requireAppCache(String operation) {
    if (hotKeyCache == null) {
      throw new ZetaModeException(operation, currentModeLabel(), "App-mode cache");
    }
  }

  /**
   * Requires app-side TopK detector; throws {@link ZetaModeException} when
   * the app-side detector is unavailable (Worker-only mode).
   *
   * @param operation the API operation name
   */
  private void requireAppDetector(String operation) {
    if (appHotKeyDetector == null) {
      throw new ZetaModeException(operation, currentModeLabel(), "App-mode TopK detector");
    }
  }
}
