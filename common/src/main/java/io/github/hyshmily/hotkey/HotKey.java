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
import io.github.hyshmily.hotkey.exception.HotKeyBlockedException;
import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.Item;
import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.TopK;
import io.github.hyshmily.hotkey.model.HotKeyCacheStats;
import io.github.hyshmily.hotkey.rule.Rule;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;

/**
 * Public facade for the HotKey library.
 *
 * <p>All cache operations go through this class. Delegates to {@link HotKeyCache}
 * for L1 / version / broadcast orchestration and {@link TopK} for hot-key
 * detection queries.
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
@RequiredArgsConstructor
public class HotKey {

  /** Cache orchestrator; {@code null} in Worker-only mode. */
  private final HotKeyCache hotKeyCache;
  /** App-side local hot-key detector. */
  private final TopK topKAlgorithm;
  /** Worker-side global hot-key detector (may be {@code null} without Worker). */
  private final TopK workerTopKAlgorithm;

  /**
   * Create a HotKey with a cache and an app‑side TopK detector.
   * This constructor is used by programmatic configuration or by
   * auto‑configuration when the Worker is not active.
   *
   * @param hotKeyCache   the cache orchestrator (may be {@code null} in Worker-only mode)
   * @param topKAlgorithm the app-side local TopK detector
   */
  public HotKey(HotKeyCache hotKeyCache, TopK topKAlgorithm) {
    this(hotKeyCache, topKAlgorithm, null);
  }

  //-----------------------------------------------------------------------------

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
   * Notify the local TopK detector that a key was accessed, without triggering
   * a report to the Worker. Used by {@code @Intercept} path to keep the local
   * frequency sketch accurate without flooding the Worker with reports.
   * Null keys are silently ignored.
   *
   * @param cacheKey the accessed key (maybe {@code null}, silently ignored)
   */
  public void notifyLocalDetector(String cacheKey) {
    if (cacheKey == null) {
      return;
    }
    topKAlgorithm.addDirect(cacheKey, io.github.hyshmily.hotkey.constants.HotKeyConstants.TOPK_INCR);
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
   * Return the current top-K hot keys (key + count) from the local detector,
   * ordered by frequency.
   *
   * @return the local top-K list, or an empty list if no detector is available;
   *         the returned list is a point-in-time snapshot
   */
  public List<Item> returnLocalHotKeys() {
    return topKAlgorithm != null ? topKAlgorithm.list() : List.of();
  }

  /**
   * Return a blocking queue of recently expelled hot keys from the local detector.
   * Consumers can drain this queue to react to keys that are no longer hot.
   *
   * @return the expelled queue, or an empty queue if no detector is available
   */
  public BlockingQueue<Item> returnLocalExpelledHotKeys() {
    return topKAlgorithm != null ? topKAlgorithm.expelled() : new LinkedBlockingQueue<>();
  }

  /**
   * Return the total number of data streams (accesses) tracked by the local detector.
   *
   * @return the total count, or {@code 0} if no detector is available
   */
  public long returnLocalTotalDataStreams() {
    return topKAlgorithm != null ? topKAlgorithm.total() : 0L;
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
