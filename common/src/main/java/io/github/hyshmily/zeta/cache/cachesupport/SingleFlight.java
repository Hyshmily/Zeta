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
   * <p>
   * The default answers for the whole key space; per-prefix registries narrow
   * it per key via {@link #isBreakerOpen(String)}.
   *
   * @return {@code true} if the breaker is open
   */
  boolean isBreakerOpen();

  /**
   * Whether the given key's breaker shard is currently open.
   * <p>
   * The default implementation ignores the key for backward compatibility
   * with custom {@link SingleFlight} beans that front a single breaker.
   *
   * @param cacheKey the key whose shard to check
   * @return {@code true} if the key's breaker is open
   */
  default boolean isBreakerOpen(String cacheKey) {
    return isBreakerOpen();
  }

  /**
   * Total number of dedup loads that resolved to empty because the reader
   * future completed with a {@link java.util.concurrent.TimeoutException}
   * (single-key and batch paths alike) since startup — the Redis-degraded
   * stall signal backing {@code zeta.stall.redis_degraded.timeouts.total}.
   * <p>
   * The default implementation returns {@code 0} for backward compatibility
   * with custom {@link SingleFlight} beans that do not track timeouts.
   *
   * @return cumulative load-timeout count (monotonic, never negative)
   */
  default long getLoadTimeoutCount() {
    return 0L;
  }

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
   * <p><b>Explicit-outcome primary.</b> New code should prefer
   * {@link #loadOutcome(String, Supplier)}: it distinguishes a null source
   * ({@code SOURCE_EMPTY}) from protection-machinery suppression
   * ({@code SUPPRESSED_*}) and carries reader failures as {@code FAILED}
   * instead of throwing, so the caller (and only the caller) decides the
   * {@code failOnError} policy in one place.
   *
   * @param cacheKey the key to load
   * @param reader   the value supplier (should not return {@code null})
   * @param <T>      the value type
   * @return the explicit outcome (never {@code null}; never throws for reader
   *         failures — they surface as {@code FAILED})
   */
  <T> LoaderOutcome<T> loadOutcome(String cacheKey, Supplier<T> reader);

  /**
   * Load a value via the supplier, deduplicating concurrent requests for the same key.
   * Thread-safe: concurrent calls for the same key share a single future.
   *
   * <p>Legacy bridge over {@link #loadOutcome(String, Supplier)}: suppressed
   * and null-source outcomes resolve to empty; {@code FAILED} rethrows the
   * cause in its natural form (the historical single-key fail-fast behavior).
   * Prefer the explicit outcome for new code.
   *
   * @param cacheKey the key to load
   * @param reader   the value supplier (should not return {@code null})
   * @param <T>      the value type
   * @return the loaded value, or empty if the source was null or the load was
   *         suppressed
   * @throws RuntimeException the reader failure cause when the outcome is {@code FAILED}
   * @throws Error          the reader failure cause when it is an {@code Error}
   */
  default <T> Optional<T> load(String cacheKey, Supplier<T> reader) {
    LoaderOutcome<T> outcome = loadOutcome(cacheKey, reader);
    if (outcome.status() == LoadStatus.FAILED) {
      Throwable cause = outcome.cause();
      if (cause instanceof RuntimeException re) {
        throw re;
      }
      if (cause instanceof Error err) {
        throw err;
      }
      throw new java.util.concurrent.CompletionException(cause);
    }
    return outcome.toOptional();
  }
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

  /**
   * Batch explicit-outcome primary: per-key outcomes, never throws for reader
   * failures (they surface per key as {@code FAILED}). The caller applies its
   * own fail-fast policy over the returned map.
   *
   * @param cacheKeys the keys to load
   * @param reader    function that returns a value for each key (called on executor threads)
   * @param <T>       the value type
   * @return map of key to explicit outcome, preserving iteration order of
   *         {@code cacheKeys}
   */
  default <T> Map<String, LoaderOutcome<T>> loadOutcome(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader
  ) {
    Map<String, LoaderOutcome<T>> outcomes = new java.util.LinkedHashMap<>();
    for (String key : cacheKeys) {
      outcomes.put(key, loadOutcome(key, () -> reader.apply(key)));
    }
    return outcomes;
  }

  /**
   * Batch variant of {@link #load(Iterable, Function)} with an explicit
   * failure policy.
   * <p>
   * Legacy bridge over {@link #loadOutcome(Iterable, Function)}: per-key
   * outcomes are mapped to {@code Optional} ({@code FAILED} excluded), and
   * when {@code failOnError} is {@code true} the first {@code FAILED} cause
   * aborts the collection and propagates (already-completed results are
   * discarded). Suppressed outcomes (timeouts, breaker interception, executor
   * rejection) resolve to an empty result in both modes — they are transient
   * degradation, not failures.
   *
   * @param cacheKeys   the keys to load
   * @param reader      function that returns a value for each key (called on executor threads)
   * @param failOnError whether the first reader failure propagates instead of being swallowed
   * @param <T>         the value type
   * @return map of key to loaded value, preserving iteration order of {@code cacheKeys}
   * @throws RuntimeException the reader failure cause when {@code failOnError} is {@code true}
   */
  default <T> Map<String, Optional<T>> load(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader,
    boolean failOnError
  ) {
    Map<String, LoaderOutcome<T>> outcomes = loadOutcome(cacheKeys, reader);
    Map<String, Optional<T>> results = new java.util.LinkedHashMap<>();
    for (Map.Entry<String, LoaderOutcome<T>> e : outcomes.entrySet()) {
      LoaderOutcome<T> o = e.getValue();
      if (o.status() == LoadStatus.FAILED && failOnError) {
        Throwable cause = o.cause();
        if (cause instanceof RuntimeException re) {
          throw re;
        }
        if (cause instanceof Error err) {
          throw err;
        }
        throw new java.util.concurrent.CompletionException(cause);
      }
      results.put(e.getKey(), o.toOptional());
    }
    return results;
  }

  /**
   * Drop the dedup entry for the given key, so the next {@link #load} re-executes
   * the reader instead of replaying a completed future's result (ADR-0067).
   * <p>
   * Call whenever the key's authoritative value changes or its L1 entry is
   * removed: an in-flight or completed future older than the invalidation must
   * not be replayed onto a later miss, or the pre-invalidation value would be
   * served and re-cached. An in-flight load is not cancelled — waiters holding
   * its future still complete — but the entry leaves the dedup cache
   * immediately, so newly arriving callers start a fresh load.
   *
   * @param cacheKey the key whose dedup entry should be dropped
   */
  void invalidate(String cacheKey);

  /**
   * Drop <b>every</b> dedup entry, so the next {@link #load} for any key
   * re-executes its reader instead of replaying a completed future's result
   * (the bulk form of the ADR-0067 contract).
   * <p>
   * Called when the whole L1 is flushed locally
   * ({@code HotKeyCache.invalidateAllLocal}): every completed future older
   * than the flush is stale by definition, and replaying any of them would
   * re-cache a value the emergency flush just discarded. In-flight loads are
   * not cancelled — waiters still complete — but their results leave the
   * dedup cache immediately.
   * <p>
   * The default implementation is a no-op for backward compatibility with
   * custom {@link SingleFlight} beans supplied before this method existed;
   * such beans keep the historical replay-after-flush behavior unless they
   * override it.
   */
  default void invalidateAll() {}
}
