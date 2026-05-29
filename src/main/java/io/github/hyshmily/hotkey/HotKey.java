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
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class HotKey {

  private final HotKeyCache hotKeyCache;
  private final TopK topKAlgorithm;

  /**
   * Check if the given key is currently in the Top-K hot set.
   * @since v1.0.6
   */
  public boolean isHotKey(String cacheKey) {
    return hotKeyCache.isHotKey(cacheKey);
  }

  /**
   * Return a snapshot of the current Top-K hot keys sorted by count descending.
   * @since v1.0.6
   */
  public List<Item> returnHotKeys() {
    return topKAlgorithm.list();
  }

  /**
   * Return the queue of keys recently evicted from the Top-K set.
   * @since v1.0.6
   */
  public BlockingQueue<Item> returnExpelledHotKeys() {
    return topKAlgorithm.expelled();
  }

  /**
   * Return the total number of key accesses processed by HeavyKeeper.
   * @since v1.0.6
   */
  public long returnTotalDataStreams() {
    return topKAlgorithm.total();
  }

  /**
   * Peek at the L1 cache without triggering frequency tracking or L2 reads.
   * @since v1.0.6
   */
  public <T> Optional<T> peek(String cacheKey) {
    return hotKeyCache.peek(cacheKey);
  }

  /**
   * Get a value from L1 or load it via the reader (e.g. Redis, DB, API).
   * Hot keys are automatically promoted to L1. Uses singleflight dedup for concurrent misses.
   * @since v1.0.6
   */
  public <T> Optional<T> get(String cacheKey, Supplier<T> reader) {
    return hotKeyCache.get(cacheKey, reader);
  }

  /**
   * Same as {@link #get(String, Supplier)} but with a per-entry hard TTL (milliseconds).
   * The entry is evicted from Caffeine after this TTL regardless of access patterns.
   * @since v1.0.8
   */
  public <T> Optional<T> get(String cacheKey, Supplier<T> reader, long hardTtlMs) {
    return hotKeyCache.get(cacheKey, reader, hardTtlMs);
  }

  /**
   * Get with soft-expire (stale-while-revalidate). Returns the cached value immediately
   * even if the soft TTL has expired, while triggering an async refresh in the background.
   * Falls back to singleflight load on L1 miss. Uses global softTtlMs from config.
   * @since v1.0.7
   */
  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader) {
    return hotKeyCache.getWithSoftExpire(cacheKey, reader);
  }

  /**
   * Same as {@link #getWithSoftExpire(String, Supplier)} but with a per-call soft TTL override.
   * @since v1.0.8
   */
  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader, long softTtlMs) {
    return hotKeyCache.getWithSoftExpire(cacheKey, reader, softTtlMs);
  }

  /**
   * Same as {@link #getWithSoftExpire(String, Supplier)} but with both per-entry hard TTL
   * and per-call soft TTL. Hard TTL controls Caffeine eviction; soft TTL controls async refresh.
   * @since v1.0.8-SNAPSHOT
   */
  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader, long hardTtlMs, long softTtlMs) {
    return hotKeyCache.getWithSoftExpire(cacheKey, reader, hardTtlMs, softTtlMs);
  }

  /**
   * Invalidate a single key from L1 and broadcast the invalidation to peer instances.
   * @since v1.0.7
   */
  public void invalidate(String cacheKey) {
    hotKeyCache.invalidate(cacheKey);
  }

  /**
   * Invalidate multiple keys from L1 and broadcast invalidations to peer instances.
   * @since v1.0.6
   */
  public void invalidateAll(String... cacheKeys) {
    invalidateAll(Arrays.asList(cacheKeys));
  }

  /**
   * Invalidate multiple keys from L1 and broadcast invalidations to peer instances.
   * @since v1.0.6
   */
  public void invalidateAll(Collection<String> cacheKeys) {
    hotKeyCache.invalidateAll(cacheKeys);
  }

  /**
   * Write-through: execute the writer (e.g. Redis SET), then update L1 and broadcast.
   * If called inside a Spring transaction, execution is deferred to afterCommit.
   * @since v1.0.6
   */
  public <T> void putThrough(String cacheKey, T value, Runnable writer) {
    hotKeyCache.putThrough(cacheKey, value, writer);
  }

  /**
   * Same as {@link #putThrough(String, Object, Runnable)} but with a per-entry hard TTL.
   * @since v1.0.8
   */
  public <T> void putThrough(String cacheKey, T value, Runnable writer, long hardTtlMs) {
    hotKeyCache.putThrough(cacheKey, value, writer, hardTtlMs);
  }

  /**
   * Same as {@link #putThrough(String, Object, Runnable)} but with per-entry hard TTL
   * and soft TTL. Sets both Caffeine expiry and soft-expire timestamp.
   * @since v1.0.8-SNAPSHOT
   */
  public <T> void putThrough(String cacheKey, T value, Runnable writer, long hardTtlMs, long softTtlMs) {
    hotKeyCache.putThrough(cacheKey, value, writer, hardTtlMs, softTtlMs);
  }

  /**
   * For collection mutations (LPUSH, SADD, ZADD): execute the mutation, then invalidate L1
   * and broadcast. Next {@link #get} will re-fetch the full value from the reader.
   * @since v1.0.6
   */
  public void putBeforeInvalidate(String cacheKey, Runnable mutation) {
    hotKeyCache.putBeforeInvalidate(cacheKey, mutation);
  }
}
