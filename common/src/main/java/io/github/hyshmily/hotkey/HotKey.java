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
package io.github.hyshmily.hotkey;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.cache.HotKeyCache;
import io.github.hyshmily.hotkey.cache.fluentAPI.HotKeyReadQuery;
import io.github.hyshmily.hotkey.cache.fluentAPI.HotKeyWriteCommand;
import io.github.hyshmily.hotkey.exception.HotKeyBlockedException;
import io.github.hyshmily.hotkey.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.Item;
import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.TopK;
import io.github.hyshmily.hotkey.model.HotKeyCacheStats;
import io.github.hyshmily.hotkey.rule.Rule;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

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
 *   <li><b>Worker-only mode</b> ({@code hotkey.worker.enabled=true}):
 *       cache and app‑side TopK are absent; only the Worker TopK is available.</li>
 * </ul>
 * Methods whose backing service is absent throw
 * {@link UnsupportedOperationException} (cache read/write) or return
 * empty / zero (TopK queries).
 */
public class HotKey {

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
  public HotKey(HotKeyCache hotKeyCache, HotKeyDetector appHotKeyDetector, TopK workerTopKAlgorithm) {
    this.hotKeyCache = hotKeyCache;
    this.appHotKeyDetector = appHotKeyDetector;
    this.workerTopKAlgorithm = workerTopKAlgorithm;
  }

  /**
   * Create a fluent read query for the given key.
   *
   * <p>Returns a {@link HotKeyReadQuery} builder that lets you configure
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
   * @return a new {@link HotKeyReadQuery} instance (single-use)
   */
  public <T> HotKeyReadQuery<T> read(String cacheKey) {
    return new HotKeyReadQuery<>(this, cacheKey);
  }

  /**
   * Create a fluent write command for the given key.
   *
   * <p>Returns a {@link HotKeyWriteCommand} builder that lets you configure
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
   * @return a new {@link HotKeyWriteCommand} instance (single-use)
   */
  public <T> HotKeyWriteCommand<T> write(String cacheKey) {
    return new HotKeyWriteCommand<>(this, cacheKey);
  }

  /**
   * Convenience shorthand for {@link #get get(cacheKey, loader).orElse(null)}.
   *
   * <p>Loads the value via the supplier on cache miss, using configured default TTLs.
   * Returns {@code null} when the loader itself returns {@code null}.
   *
   * @param cacheKey the key to retrieve
   * @param loader   the value supplier for cache misses
   * @param <V>      the value type
   * @return the cached or loaded value, or {@code null} if the loader returned {@code null}
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws HotKeyBlockedException when the key matches a blacklist rule
   */
  public <V> V computeIfAbsent(String cacheKey, Supplier<V> loader) {
    requireCache();
    return get(cacheKey, loader).orElse(null);
  }

  /**
   * Convenience shorthand with explicit hard TTL.
   *
   * @param cacheKey the key to retrieve
   * @param loader   the value supplier for cache misses
   * @param hardTtlMs hard TTL override (0 = use configured default; {@link Long#MAX_VALUE} for permanent entry)
   * @param <V>      the value type
   * @return the cached or loaded value, or {@code null} if the loader returned {@code null}
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws HotKeyBlockedException when the key matches a blacklist rule
   * @see #get(String, Supplier, long, long)
   */
  public <V> V computeIfAbsent(String cacheKey, Supplier<V> loader, long hardTtlMs) {
    requireCache();
    return get(cacheKey, loader, hardTtlMs, 0).orElse(null);
  }

  /**
   * Convenience shorthand with explicit hard and soft TTLs.
   *
   * @param cacheKey  the key to retrieve
   * @param loader    the value supplier for cache misses
   * @param hardTtlMs hard TTL override (0 = use configured default; {@link Long#MAX_VALUE} for permanent entry)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param <V>       the value type
   * @return the cached or loaded value, or {@code null} if the loader returned {@code null}
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws HotKeyBlockedException when the key matches a blacklist rule
   * @see #get(String, Supplier, long, long)
   */
  public <V> V computeIfAbsent(String cacheKey, Supplier<V> loader, long hardTtlMs, long softTtlMs) {
    requireCache();
    return get(cacheKey, loader, hardTtlMs, softTtlMs).orElse(null);
  }

  /**
   * Convenience shorthand for {@link #getWithSoftExpire getWithSoftExpire(cacheKey, loader).orElse(null)}.
   *
   * <p>Returns stale data immediately when the soft TTL has expired, triggering
   * an async refresh in the background.
   *
   * @param cacheKey the key to retrieve
   * @param loader   the value supplier for cache misses / refreshes
   * @param <V>      the value type
   * @return the cached (possibly stale) or loaded value, or {@code null} if the loader returned {@code null}
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws HotKeyBlockedException when the key matches a blacklist rule
   */
  public <V> V computeIfAbsentWithSoftExpire(String cacheKey, Supplier<V> loader) {
    requireCache();
    return getWithSoftExpire(cacheKey, loader).orElse(null);
  }

  /**
   * Convenience shorthand with explicit soft TTL.
   *
   * @param cacheKey  the key to retrieve
   * @param loader    the value supplier for cache misses / refreshes
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param <V>       the value type
   * @return the cached (possibly stale) or loaded value, or {@code null} if the loader returned {@code null}
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws HotKeyBlockedException when the key matches a blacklist rule
   */
  public <V> V computeIfAbsentWithSoftExpire(String cacheKey, Supplier<V> loader, long softTtlMs) {
    requireCache();
    return getWithSoftExpire(cacheKey, loader, softTtlMs).orElse(null);
  }

  /**
   * Convenience shorthand with explicit hard and soft TTLs for soft-expire semantics.
   *
   * @param cacheKey  the key to retrieve
   * @param loader    the value supplier for cache misses / refreshes
   * @param hardTtlMs hard TTL override (0 = use configured default; {@link Long#MAX_VALUE} for pure logical expiry)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param <V>       the value type
   * @return the cached (possibly stale) or loaded value, or {@code null} if the loader returned {@code null}
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @throws HotKeyBlockedException when the key matches a blacklist rule
   */
  public <V> V computeIfAbsentWithSoftExpire(String cacheKey, Supplier<V> loader, long hardTtlMs, long softTtlMs) {
    requireCache();
    return getWithSoftExpire(cacheKey, loader, hardTtlMs, softTtlMs).orElse(null);
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
    requireCache();
    return hotKeyCache.peek(cacheKey);
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
   * @throws HotKeyBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> get(String cacheKey, Supplier<T> reader) {
    requireCache();
    return hotKeyCache.get(cacheKey, reader);
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
   * @throws HotKeyBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> get(String cacheKey, Supplier<T> reader, long hardTtlMs, long softTtlMs) {
    requireCache();
    return hotKeyCache.get(cacheKey, reader, hardTtlMs, softTtlMs);
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
   * @throws HotKeyBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader) {
    requireCache();
    return hotKeyCache.getWithSoftExpire(cacheKey, reader);
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
   * @throws HotKeyBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader, long softTtlMs) {
    requireCache();
    return hotKeyCache.getWithSoftExpire(cacheKey, reader, softTtlMs);
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
   * @throws HotKeyBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader, long hardTtlMs, long softTtlMs) {
    requireCache();
    return hotKeyCache.getWithSoftExpire(cacheKey, reader, hardTtlMs, softTtlMs);
  }

  /**
   * Invalidate a single key from L1 and broadcast REFRESH to peers.
   * The next {@link #get} will re-fetch from the reader.
   *
   * @param cacheKey the key to invalidate
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void invalidate(String cacheKey) {
    requireCache();
    hotKeyCache.invalidate(cacheKey);
  }

  /**
   * Invalidate one or more keys from L1 and broadcast INVALIDATE for each.
   *
   * @param cacheKeys the keys to invalidate
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @see #invalidate(String)
   */
  public void invalidateAll(String... cacheKeys) {
    invalidateAll(Arrays.asList(cacheKeys));
  }

  /**
   * Invalidate a collection of keys from L1 and broadcast INVALIDATE for each.
   *
   * @param cacheKeys the keys to invalidate
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void invalidateAll(Collection<String> cacheKeys) {
    requireCache();
    hotKeyCache.invalidateAll(cacheKeys);
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
    requireCache();
    hotKeyCache.putThrough(cacheKey, value, writer);
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
  public <T> void putThrough(String cacheKey, T value, Runnable writer, long hardTtlMs, long softTtlMs) {
    requireCache();
    hotKeyCache.putThrough(cacheKey, value, writer, hardTtlMs, softTtlMs);
  }

  /**
   * Write a value directly into the local L1 cache without version bump,
   * without broadcast, without hot-key detection, and without reporting.
   * <p>
   * Existing entry metadata is preserved.  If no entry exists, a fresh
   * {@link io.github.hyshmily.hotkey.model.CacheEntry} is created with {@link io.github.hyshmily.hotkey.model.KeyState#NORMAL}.
   * <p>
   * Never throws {@code UnsupportedOperationException} in Worker-only mode;
   * silently no-ops instead.
   *
   * @param cacheKey the key to store
   * @param value    the value to cache
   */
  public void putLocal(String cacheKey, Object value) {
    if (hotKeyCache != null) {
      hotKeyCache.putLocal(cacheKey, value);
    }
  }

  /**
   * Write a value directly into the local L1 cache with explicit TTL overrides.
   * Delegates to {@link #putLocal(String, Object)} semantics — no version bump,
   * no broadcast, no hot-key detection, and no reporting.
   * <p>
   * Pass {@code 0} for either TTL to use the configured default.
   *
   * @param cacheKey  the key to store
   * @param value     the value to cache
   * @param hardTtlMs hard TTL override (0 = use configured default)
   * @param softTtlMs soft TTL override (0 = use configured default)
   */
  public void putLocal(String cacheKey, Object value, long hardTtlMs, long softTtlMs) {
    if (hotKeyCache != null) {
      hotKeyCache.putLocal(cacheKey, value, hardTtlMs, softTtlMs);
    }
  }

  /**
   * Execute a mutation, then invalidate L1 and broadcast.
   * Next {@link #get} will re-fetch from the reader.
   *
   * @param cacheKey the key to invalidate after mutation
   * @param mutation the mutation to execute
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void putBeforeInvalidate(String cacheKey, Runnable mutation) {
    requireCache();
    hotKeyCache.putBeforeInvalidate(cacheKey, mutation);
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
   * {@code recordStats()} is enabled. {@code estimatedSize} is always
   * available.
   *
   * @return a {@link HotKeyCacheStats} record, or {@code null} in Worker-only mode;
   *         hit/miss counters are {@code 0} if stats recording is not enabled
   */
  public HotKeyCacheStats stats() {
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
    requireCache();
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
  public void invalidateAll() {
    requireCache();
    hotKeyCache.invalidateAll();
  }

  //------------------------------------------------------------------------

  /**
   * Check whether a key is currently tracked as a local hot key in L1.
   *
   * @param cacheKey the key to inspect
   * @return {@code true} if the key exists in L1 with {@link io.github.hyshmily.hotkey.model.KeyState#HOT}
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public boolean isLocalHotKey(String cacheKey) {
    requireCache();
    return hotKeyCache.isLocalHotKey(cacheKey);
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
    appHotKeyDetector.addDirect(cacheKey, count);
  }

  /**
   * Batch-increment the local TopK detector directly, bypassing buffer and reports.
   *
   * @param keyCounts a map of key → access count
   */
  public void notifyLocalDetectorDirect(Map<String, Long> keyCounts) {
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
    appHotKeyDetector.add(cacheKey);
  }

  /**
   * Notify the local detector of a key access with a custom delta, routing
   * through the buffered counter. Null keys are silently ignored.
   *
   * @param cacheKey the accessed key (maybe {@code null}, silently ignored)
   * @param count    the number of accesses to record
   */
  public void notifyLocalDetector(String cacheKey, long count) {
    appHotKeyDetector.add(cacheKey, count);
  }

  /**
   * Batch-notify the local detector of multiple key accesses, routing through
   * the buffered counter. Null keys in the map are silently ignored.
   *
   * @param keyCounts a map of key → access count
   */
  public void notifyLocalDetector(Map<String, Long> keyCounts) {
    appHotKeyDetector.add(keyCounts);
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
    requireCache();
    hotKeyCache.addBlacklist(keyPattern);
  }

  /**
   * Remove a key pattern from the blacklist.
   *
   * @param keyPattern the key pattern to remove from the blacklist
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void removeBlacklist(String keyPattern) {
    requireCache();
    hotKeyCache.unBlacklist(keyPattern);
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
    requireCache();
    hotKeyCache.addWhitelist(keyPattern);
  }

  /**
   * Remove a key pattern from the whitelist.
   *
   * @param keyPattern the key pattern to remove from the whitelist
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void removeWhitelist(String keyPattern) {
    requireCache();
    hotKeyCache.unWhitelist(keyPattern);
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
    requireCache();
    hotKeyCache.clearAllRules();
  }

  /**
   * Broadcast all local rules to peer instances via the sync exchange.
   * Useful for initial synchronization when a new instance joins the cluster.
   *
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void broadcastAllLocalRulesManually() {
    requireCache();
    hotKeyCache.broadcastAllLocalRulesManually();
  }

  /**
   * Verify that the cache is available; throws {@link UnsupportedOperationException}
   * when running in Worker-only mode where no app-side cache exists.
   */
  private void requireCache() {
    if (hotKeyCache == null) {
      throw new UnsupportedOperationException(
        "HotKey cache is not available in Worker-only mode. " + "Run the Worker as a separate Spring Boot process."
      );
    }
  }
}
