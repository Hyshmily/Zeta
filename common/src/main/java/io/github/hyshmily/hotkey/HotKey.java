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
import io.github.hyshmily.hotkey.cache.distributedlock.AutoReleaseLock;
import io.github.hyshmily.hotkey.cache.distributedlock.LockProvider;
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
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;

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
   * Distributed lock provider.  {@code null} when no Redis is available
   * (graceful degradation — {@link #tryLock} returns {@code null}).
   */
  private final LockProvider lockProvider;

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
  public HotKey(
    HotKeyCache hotKeyCache,
    HotKeyDetector appHotKeyDetector,
    TopK workerTopKAlgorithm,
    LockProvider lockProvider
  ) {
    this.hotKeyCache = hotKeyCache;
    this.appHotKeyDetector = appHotKeyDetector;
    this.workerTopKAlgorithm = workerTopKAlgorithm;
    this.lockProvider = lockProvider;
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
   * Batch variant of {@link #computeIfAbsent(String, Supplier)}. Iterates over
   * the given keys and loads each one via the provided function on cache miss.
   *
   * @param cachKeys the keys to retrieve
   * @param loader   the per-key value supplier for cache misses
   * @param <V>      the value type
   * @return a map of key → loaded value (only keys whose loader returned non-null are included)
   */
  public <V> Map<String, V> computeIfAbsent(Collection<String> cachKeys, Function<String, V> loader) {
    Map<String, V> result = new HashMap<>();
    cachKeys.forEach(key -> {
      V value = computeIfAbsent(key, () -> loader.apply(key));
      result.put(key, value);
    });

    return result;
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
   * Batch variant of {@link #computeIfAbsent(String, Supplier, long)}. Iterates
   * over the given keys with an explicit hard TTL override.
   *
   * @param cachKeys  the keys to retrieve
   * @param loader    the per-key value supplier for cache misses
   * @param hardTtlMs hard TTL override (0 = use configured default; {@link Long#MAX_VALUE} for permanent entry)
   * @param <V>       the value type
   * @return a map of key → loaded value
   */
  public <V> Map<String, V> computeIfAbsent(Collection<String> cachKeys, Function<String, V> loader, long hardTtlMs) {
    Map<String, V> result = new HashMap<>();
    cachKeys.forEach(key -> {
      V value = computeIfAbsent(key, () -> loader.apply(key), hardTtlMs);
      result.put(key, value);
    });

    return result;
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
   * Batch variant of {@link #computeIfAbsent(String, Supplier, long, long)}.
   * Iterates over the given keys with explicit hard and soft TTL overrides.
   *
   * @param cachKeys  the keys to retrieve
   * @param loader    the per-key value supplier for cache misses
   * @param hardTtlMs hard TTL override (0 = use configured default; {@link Long#MAX_VALUE} for permanent entry)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param <V>       the value type
   * @return a map of key → loaded value
   */
  public <V> Map<String, V> computeIfAbsent(
    Collection<String> cachKeys,
    Function<String, V> loader,
    long hardTtlMs,
    long softTtlMs
  ) {
    Map<String, V> result = new HashMap<>();
    cachKeys.forEach(key -> {
      V value = computeIfAbsent(key, () -> loader.apply(key), hardTtlMs, softTtlMs);
      result.put(key, value);
    });

    return result;
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
   * Batch variant of {@link #computeIfAbsentWithSoftExpire(String, Supplier)}.
   * Iterates over the given keys with soft-expire semantics.
   *
   * @param cachKeys the keys to retrieve
   * @param loader   the per-key value supplier for cache misses / refreshes
   * @param <V>      the value type
   * @return a map of key → (possibly stale) loaded value
   */
  public <V> Map<String, V> computeIfAbsentWithSoftExpire(Collection<String> cachKeys, Function<String, V> loader) {
    Map<String, V> result = new HashMap<>();
    cachKeys.forEach(key -> {
      V value = computeIfAbsentWithSoftExpire(key, () -> loader.apply(key));
      result.put(key, value);
    });

    return result;
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
   * Batch variant of {@link #computeIfAbsentWithSoftExpire(String, Supplier, long)}.
   * Iterates over the given keys with explicit soft TTL override.
   *
   * @param cachKeys  the keys to retrieve
   * @param loader    the per-key value supplier for cache misses / refreshes
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param <V>       the value type
   * @return a map of key → (possibly stale) loaded value
   */
  public <V> Map<String, V> computeIfAbsentWithSoftExpire(
    Collection<String> cachKeys,
    Function<String, V> loader,
    long softTtlMs
  ) {
    Map<String, V> result = new HashMap<>();
    cachKeys.forEach(key -> {
      V value = computeIfAbsentWithSoftExpire(key, () -> loader.apply(key), softTtlMs);
      result.put(key, value);
    });

    return result;
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
   * Batch variant of {@link #computeIfAbsentWithSoftExpire(String, Supplier, long, long)}.
   * Iterates over the given keys with explicit hard and soft TTL overrides.
   *
   * @param cachKeys  the keys to retrieve
   * @param loader    the per-key value supplier for cache misses / refreshes
   * @param hardTtlMs hard TTL override (0 = use configured default; {@link Long#MAX_VALUE} for pure logical expiry)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param <V>       the value type
   * @return a map of key → (possibly stale) loaded value
   */
  public <V> Map<String, V> computeIfAbsentWithSoftExpire(
    Collection<String> cachKeys,
    Function<String, V> loader,
    long hardTtlMs,
    long softTtlMs
  ) {
    Map<String, V> result = new HashMap<>();
    cachKeys.forEach(key -> {
      V value = computeIfAbsentWithSoftExpire(key, () -> loader.apply(key), hardTtlMs, softTtlMs);
      result.put(key, value);
    });

    return result;
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
   * Convenience shorthand for {@link #evictLocal(Collection)} — evicts a single
   * key from the local cache without broadcasting.
   *
   * @param cacheKeys the key to evict locally
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void evictLocal(String cacheKeys) {
    evictLocal(Collections.singletonList(cacheKeys));
  }

  /**
   * Evict keys from the local cache only, without broadcasting to other
   * instances and without bumping version numbers.
   *
   * <p>Useful for emergency local cleanup, testing, or module offline
   * scenarios where only the current node needs to be cleared.
   *
   * @param cacheKeys the keys to evict locally
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void evictLocal(Collection<String> cacheKeys) {
    requireCache();
    hotKeyCache.evictLocal(cacheKeys);
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
    requireCache();
    putLocal(cacheKey, value, 0, 0);
  }

  /**
   * Batch variant of {@link #putLocal(String, Object)}. Writes all entries into
   * L1 without version bump, broadcast, hot-key detection, or reporting.
   *
   * @param entries a map of key → value to store locally
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void putLocal(Map<String, Object> entries) {
    requireCache();
    entries.forEach(this::putLocal);
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
    requireCache();
    hotKeyCache.putLocal(cacheKey, value, hardTtlMs, softTtlMs);
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
   * Batch variant of {@link #putBeforeInvalidate(String, Runnable)}. Executes
   * all mutations and invalidates the corresponding keys from L1 with broadcast.
   *
   * @param mutations a map of key → mutation to execute
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void putBeforeInvalidateAll(Map<String, Runnable> mutations) {
    requireCache();
    mutations.forEach(this::putBeforeInvalidate);
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
   * Batch variant of {@link #isLocalHotKey(String)}. Returns a map of key →
   * hot status for all given keys.
   *
   * @param cacheKeys the keys to inspect
   * @return a map of key → whether it is a local hot key
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public Map<String, Boolean> areLocalHotKeys(Collection<String> cacheKeys) {
    requireCache();
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
   * Notify the local detector of key access with a custom delta, routing
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
   * Evict the given key locally, then load and cache via the supplier.
   * Uses default TTLs. No broadcast is sent for the eviction.
   *
   * @param cacheKey the key to refresh
   * @param loader   the value supplier
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void refresh(String cacheKey, Supplier<?> loader) {
    requireCache();
    evictLocal(cacheKey);
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
    requireCache();
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
    requireCache();
    evictLocal(cacheKey);
    putThrough(cacheKey, loader.get(), () -> {}, hotHardTtlMs, hotSoftTtlMs);
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
    requireCache();
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
    requireCache();
    keyPatterns.forEach(hotKeyCache::addBlacklist);
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
   * Batch variant of {@link #removeBlacklist(String)}. Removes multiple key
   * patterns from the blacklist.
   *
   * @param keyPatterns the key patterns to remove from the blacklist
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void removeBlacklist(Collection<String> keyPatterns) {
    requireCache();
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
    requireCache();
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
    requireCache();
    keyPatterns.forEach(hotKeyCache::addWhitelist);
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
   * Batch variant of {@link #removeWhitelist(String)}. Removes multiple key
   * patterns from the whitelist.
   *
   * @param keyPatterns the key patterns to remove from the whitelist
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void removeWhitelist(Collection<String> keyPatterns) {
    requireCache();
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
