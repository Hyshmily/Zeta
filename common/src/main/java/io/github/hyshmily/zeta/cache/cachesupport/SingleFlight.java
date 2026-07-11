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
package io.github.hyshmily.zeta.cache.cachesupport;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Deduplicates concurrent in-flight loads for the same key.
 * <p>
 * Only the first caller executes the supplier; subsequent callers wait for
 * the same {@link java.util.concurrent.CompletableFuture}. On normal completion, the future
 * remains cached (TTL-based expiry via {@code expireAfterWrite}) so
 * late-arriving callers reuse the result without re-execution. On timeout or
 * exception, the entry is evicted immediately to allow a subsequent retry.
 * <p>
 * The internal dedup cache is bounded by {@code maxSize} (LRU eviction) and
 * entries expire after the configured {@code ttlSec} seconds from write.
 * This interface is thread-safe.
 */
public interface SingleFlight {
  /**
   * Whether the circuit breaker is currently open.
   * Used by {@code HotKeyCache} to decide whether to return stale cache on miss.
   *
   * @return {@code true} if the breaker is open
   */
  boolean isBreakerOpen();

  /**
   * Approximate number of keys currently tracked for dedup.
   * Useful for monitoring and diagnostics.
   *
   * @return the estimated number of in-flight keys
   */
  long estimatedInflightSize();

  /**
   * Load a value via the supplier, deduplicating concurrent requests for the same key.
   * Thread-safe: concurrent calls for the same key share a single future.
   *
   * @param cacheKey the key to load
   * @param reader   the value supplier (should not return {@code null})
   * @param <T>      the value type
   * @return the loaded value, or empty if the load failed or timed out
   */
  <T> Optional<T> load(String cacheKey, Supplier<T> reader);
  /**
   * Load multiple keys in parallel, deduplicating across all callers.
   * <p>
   * All reader futures are submitted to the executor in a single pass (Phase 1),
   * then results are collected in input order (Phase 2).  This avoids the
   * serial blocking that would occur from calling {@link #load(String, Supplier)}
   * in a loop.
   * <p>
   * Thread-safe: concurrent callers for the same subset of keys share the same
   * futures via the internal dedup cache.
   *
   * @param cacheKeys the keys to load
   * @param reader    function that returns a value for each key (called on executor threads)
   * @param <T>       the value type
   * @return map of key to loaded value, preserving iteration order of {@code cacheKeys};
   *         keys whose suppliers threw or timed out are absent from values
   */
  <T> Map<String, Optional<T>> load(Iterable<String> cacheKeys, Function<? super String, ? extends T> reader);
}
