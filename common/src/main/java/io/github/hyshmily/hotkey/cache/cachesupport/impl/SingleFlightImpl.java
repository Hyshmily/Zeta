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
package io.github.hyshmily.hotkey.cache.cachesupport.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.hotkey.Internal;
import io.github.hyshmily.hotkey.cache.cachesupport.CircuitBreaker;
import io.github.hyshmily.hotkey.cache.cachesupport.SingleFlight;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;


/**
 * Deduplicates concurrent in-flight loads for the same key.
 * <p>
 * Only the first caller executes the supplier; subsequent callers wait for
 * the same {@link CompletableFuture}. On normal completion, the future
 * remains cached (TTL-based expiry via {@code expireAfterWrite}) so
 * late-arriving callers reuse the result without re-execution. On timeout or
 * exception, the entry is evicted immediately to allow a subsequent retry.
 * <p>
 * The internal dedup cache is bounded by {@code maxSize} (LRU eviction) and
 * entries expire after the configured {@code ttlSec} seconds from write.
 * This class is thread-safe.
 */
@Slf4j
@Internal
public class SingleFlightImpl implements SingleFlight {

  /** Caffeine cache tracking currently in-flight loads (key -> CompletableFuture). */
  private final Cache<String, CompletableFuture<Object>> inflightLoads;
  /** Async executor for running the supplier. */
  private final Executor executor;
  /** Timeout in seconds before a supplier future is completed exceptionally. */
  private final int timeoutSeconds;
  /** Maximum number of in-flight keys tracked simultaneously. */
  private final int inflightMaxSize;

  /** Circuit breaker for protecting remote calls from cascading failures. */
  private final CircuitBreaker circuitBreaker;

  /**
   * Creates a SingleFlightImpl deduplicator that prevents concurrent in-flight loads
   * for the same key.
   *
   * @param maxSize        maximum number of concurrent in-flight keys tracked
   * @param ttlSec         time-to-live for dedup entries after write
   * @param timeoutSeconds per-supplier timeout before the future is completed exceptionally
   * @param executor       async executor for supplier execution
   * @param circuitBreaker circuit breaker for protecting remote calls
   */
  public SingleFlightImpl(
    int maxSize,
    int ttlSec,
    int timeoutSeconds,
    Executor executor,
    CircuitBreaker circuitBreaker
  ) {
    this.inflightLoads = Caffeine.newBuilder().maximumSize(maxSize).expireAfterWrite(ttlSec, TimeUnit.SECONDS).build();
    this.executor = executor;
    this.timeoutSeconds = timeoutSeconds;
    this.inflightMaxSize = maxSize;
    this.circuitBreaker = circuitBreaker;
  }

  /**
   * Whether the circuit breaker is currently open.
   * Used by {@code HotKeyCache} to decide whether to return stale cache on miss.
   *
   * @return {@code true} if the breaker is open
   */
  public boolean isBreakerOpen() {
    return circuitBreaker.isOpen();
  }

  /**
   * Approximate number of keys currently tracked for dedup.
   * Useful for monitoring and diagnostics.
   *
   * @return the estimated number of in-flight keys
   */
  public long estimatedInflightSize() {
    return inflightLoads.estimatedSize();
  }

  /**
   * Load a value via the supplier, deduplicating concurrent requests for the same key.
   * Thread-safe: concurrent calls for the same key share a single future.
   *
   * @param cacheKey the key to load
   * @param reader   the value supplier (should not return {@code null})
   * @param <T>      the value type
   * @return the loaded value, or empty if the load failed or timed out
   */
  @SuppressWarnings("unchecked")
  public <T> Optional<T> load(String cacheKey, Supplier<T> reader) {
    if (!circuitBreaker.allowRequest()) {
      log.debug("CB open, skip load for key={}", cacheKey);
      return Optional.empty();
    }

    if (estimatedInflightSize() > inflightMaxSize * 0.8) {
      log.warn("SingleFlight inflight queue is high: {}/{}", estimatedInflightSize(), inflightMaxSize);
    }

    CompletableFuture<Object> future = inflightLoads
      .asMap()
      .computeIfAbsent(cacheKey, k ->
        CompletableFuture.supplyAsync(
          () -> {
            try {
              Object val = reader.get();
              circuitBreaker.onSuccess();
              return val;
            } catch (Exception e) {
              circuitBreaker.onFailure();
              throw e;
            }
          },
          executor
        ).orTimeout(timeoutSeconds, TimeUnit.SECONDS)
      );

    try {
      return Optional.ofNullable((T) future.join());
    } catch (CompletionException e) {
      log.warn("singleflight join failed: key={}", cacheKey, e);
      inflightLoads.invalidate(cacheKey);

      Throwable cause = e.getCause();

      if (cause instanceof InterruptedException) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Thread interrupted during load for key: " + cacheKey, cause);
      }

      if (cause instanceof RuntimeException re) {
        throw re;
      }

      if (cause instanceof Error err) {
        throw err;
      }

      throw new RuntimeException("Loader failed for key: " + cacheKey, cause);
    }
  }
}
