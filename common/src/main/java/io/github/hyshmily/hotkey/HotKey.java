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

import io.github.hyshmily.hotkey.algorithm.Item;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyCache;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

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
public class HotKey {

  private final HotKeyCache hotKeyCache;
  private final TopK topKAlgorithm;
  private final TopK workerTopKAlgorithm;

  /**
   * Create a HotKey with a cache and an app‑side TopK detector.
   * This constructor is used by programmatic configuration or by
   * auto‑configuration when the Worker is not active.
   */
  public HotKey(HotKeyCache hotKeyCache, TopK topKAlgorithm) {
    this(hotKeyCache, topKAlgorithm, null);
  }

  /**
   * Create a HotKey that may span both app and Worker modes.
   * Any parameter may be {@code null} when the corresponding service
   * is not available in the current deployment mode.
   */
  public HotKey(HotKeyCache hotKeyCache, TopK topKAlgorithm, TopK workerTopKAlgorithm) {
    this.hotKeyCache = hotKeyCache;
    this.topKAlgorithm = topKAlgorithm;
    this.workerTopKAlgorithm = workerTopKAlgorithm;
  }


  /**
   * Check whether a key is currently tracked as a hot key.
   *
   * @return {@code true} if the key exists in L1 with {@link io.github.hyshmily.hotkey.entity.KeyState#HOT}
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public boolean isHotKey(String cacheKey) {
    requireCache();
    return hotKeyCache.isHotKey(cacheKey);
  }

  /**
   * Look up a cached value without loading or triggering hot-key detection.
   *
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
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public <T> Optional<T> get(String cacheKey, Supplier<T> reader) {
    requireCache();
    return hotKeyCache.get(cacheKey, reader);
  }

  /**
   * Get with explicit TTL overrides.
   * Pass 0 to use the configured default for that TTL type.
   *
   * @param hardTtlMs hard TTL override (0 = use configured default)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public <T> Optional<T> get(String cacheKey, Supplier<T> reader, long hardTtlMs, long softTtlMs) {
    requireCache();
    return hotKeyCache.get(cacheKey, reader, hardTtlMs, softTtlMs);
  }

  /**
   * Get with soft-expire (stale-while-revalidate). Returns cached value immediately
   * even if soft TTL expired, while triggering async refresh in background.
   *
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader) {
    requireCache();
    return hotKeyCache.getWithSoftExpire(cacheKey, reader);
  }

  /**
   * Get with soft-expire and explicit soft TTL override.
   *
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader, long softTtlMs) {
    requireCache();
    return hotKeyCache.getWithSoftExpire(cacheKey, reader, softTtlMs);
  }

  /**
   * Get with soft-expire and explicit hard/soft TTL overrides.
   *
   * @param hardTtlMs hard TTL override (0 = use configured default)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public <T> Optional<T> getWithSoftExpire(
    String cacheKey, Supplier<T> reader, long hardTtlMs, long softTtlMs
  ) {
    requireCache();
    return hotKeyCache.getWithSoftExpire(cacheKey, reader, hardTtlMs, softTtlMs);
  }

  /**
   * Invalidate a single key from L1 and broadcast REFRESH to peers.
   * The next {@link #get} will re-fetch from the reader.
   *
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void invalidate(String cacheKey) {
    requireCache();
    hotKeyCache.invalidate(cacheKey);
  }

  /**
   * Invalidate one or more keys from L1 and broadcast INVALIDATE for each.
   *
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   * @see #invalidate(String)
   */
  public void invalidateAll(String... cacheKeys) {
    invalidateAll(Arrays.asList(cacheKeys));
  }

  /**
   * Invalidate a collection of keys from L1 and broadcast INVALIDATE for each.
   *
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
   * @param hardTtlMs hard TTL override (0 = use configured default)
   * @param softTtlMs soft TTL override (0 = use configured default)
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
   * @throws UnsupportedOperationException when no cache is available (Worker-only mode)
   */
  public void putBeforeInvalidate(String cacheKey, Runnable mutation) {
    requireCache();
    hotKeyCache.putBeforeInvalidate(cacheKey, mutation);
  }


  /**
   * Return the current top-K hot keys (key + count) from the local detector,
   * ordered by frequency.
   *
   * @return the local top-K list, or an empty list if no detector is available
   */
  public List<Item> returnHotKeys() {
    return topKAlgorithm != null ? topKAlgorithm.list() : List.of();
  }

  /**
   * Return a blocking queue of recently expelled hot keys from the local detector.
   * Consumers can drain this queue to react to keys that are no longer hot.
   *
   * @return the expelled queue, or an empty queue if no detector is available
   */
  public BlockingQueue<Item> returnExpelledHotKeys() {
    return topKAlgorithm != null ? topKAlgorithm.expelled() : new LinkedBlockingQueue<>();
  }

  /**
   * Return the total number of data streams (accesses) tracked by the local detector.
   *
   * @return the total count, or {@code 0} if no detector is available
   */
  public long returnTotalDataStreams() {
    return topKAlgorithm != null ? topKAlgorithm.total() : 0L;
  }

  // ── Worker-side TopK queries (cluster-wide aggregated data) ─────────

  /**
   * Return the current top-K hot keys from the Worker-side global detector,
   * ordered by frequency. These keys reflect cross-instance aggregated access
   * counts and are updated via periodic reports from all application instances.
   *
   * @return the cluster-wide top-K list, or an empty list if no Worker is active
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
    return workerTopKAlgorithm != null
      ? workerTopKAlgorithm.expelled()
      : new LinkedBlockingQueue<>();
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


  private void requireCache() {
    if (hotKeyCache == null) {
      throw new UnsupportedOperationException(
        "HotKey cache is not available in Worker-only mode. "
           + "Run the Worker as a separate Spring Boot process."
      );
    }
  }
}
