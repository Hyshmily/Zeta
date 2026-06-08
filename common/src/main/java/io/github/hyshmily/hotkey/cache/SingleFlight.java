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
package io.github.hyshmily.hotkey.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import io.github.hyshmily.hotkey.logging.DefaultLogger;
import io.github.hyshmily.hotkey.logging.HotKeyLogger;


/**
 * Deduplicates concurrent in-flight loads for the same key.
 * <p>
 * Only the first caller executes the supplier; subsequent callers wait for
 * the same {@link CompletableFuture}. Upon completion (or timeout), the
 * dedup entry is evicted so the next caller can retry.
 */
public class SingleFlight {

  private static final HotKeyLogger log = new DefaultLogger(SingleFlight.class);

  private final Cache<String, CompletableFuture<Object>> inflightLoads;
  private final Executor executor;
  private final int timeoutSeconds;
  private final int inflightMaxSize;

  /**
   * Creates a SingleFlight deduplicator that prevents concurrent in-flight loads
   * for the same key.
   *
   * @param maxSize        maximum number of concurrent in-flight keys tracked
   * @param ttlSec         time-to-live for dedup entries after write
   * @param timeoutSeconds per-supplier timeout before the future is completed exceptionally
   * @param executor       async executor for supplier execution
   */
  public SingleFlight(int maxSize, int ttlSec, int timeoutSeconds, Executor executor) {
    this.inflightLoads = Caffeine.newBuilder().maximumSize(maxSize).expireAfterWrite(ttlSec, TimeUnit.SECONDS).build();
    this.executor = executor;
    this.timeoutSeconds = timeoutSeconds;
    this.inflightMaxSize = maxSize;
  }

  /**
   * Approximate number of keys currently tracked for dedup.
   * Useful for monitoring and diagnostics.
   */
  public long estimatedInflightSize() {
    return inflightLoads.estimatedSize();
  }

  /**
   * Load a value via the supplier, deduplicating concurrent requests for the same key.
   *
   * @param cacheKey the key to load
   * @param reader   the value supplier
   * @param <T>      the value type
   * @return the loaded value, or empty if the load failed or timed out
   */
  @SuppressWarnings("unchecked")
  public <T> Optional<T> load(String cacheKey, Supplier<T> reader) {
    if (estimatedInflightSize() > inflightMaxSize * 0.8) {
      log.warn("SingleFlight inflight queue is high: {}/{}", estimatedInflightSize(), inflightMaxSize);
    }

    CompletableFuture<Object> future = inflightLoads
      .asMap()
      .computeIfAbsent(cacheKey, _ ->
        CompletableFuture.supplyAsync(() -> (Object) reader.get(), executor).orTimeout(timeoutSeconds, TimeUnit.SECONDS)
      );

    future.whenComplete((v, e) -> {
      try {
        if (e != null) {
          log.debug("singleflight load failed: key={}", cacheKey, e);
        } else if (v == null) {
          log.debug("singleflight returned null: key={}", cacheKey);
        }
      } finally {
        inflightLoads.invalidate(cacheKey);
      }
    });

    try {
      return Optional.ofNullable((T) future.join());
    } catch (Exception e) {
      log.warn("singleflight join failed: key={}", cacheKey, e);
      return Optional.empty();
    }
  }
}
