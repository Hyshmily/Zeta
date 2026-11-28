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
package io.github.hyshmily.zeta.cache.cachesupport.circuitbreaker;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.CacheKeysPolicy;
import io.github.hyshmily.zeta.util.Internal;
import java.util.concurrent.TimeUnit;

/**
 * Per-key-prefix circuit-breaker shard map.
 *
 * <p>A single global breaker lets one sick data source silence every key:
 * {@code user:} rows timing out on the DB open the breaker, and the healthy
 * {@code product:} path degrades with them. This registry shards the breaker
 * by the key prefix: an annotation key uses the cache namespace before the
 * {@linkplain CacheKeysPolicy#NAMESPACE_DELIMITER namespace delimiter}, a
 * programmatic key the segment before the first {@code ':'}, and a key with
 * neither shares the {@value #DEFAULT_BUCKET} bucket. A failure domain
 * therefore trips only its own breaker.
 *
 * <p>Bounds: at most {@code maxPrefixes} live breakers (Caffeine size
 * eviction under its Window TinyLFU policy) plus 30-minute access expiry, so
 * an unbounded key space cannot grow this map. A shard owns no thread: its
 * window is clock-driven, so shard count costs no scheduling and eviction has
 * no lifecycle to tear down. Every shard reads the same live
 * {@link ZetaProperties.CircuitBreaker}.
 *
 * <p>{@link #close()} drops the live shards — wire it as the bean destroy
 * method.
 */
@Internal
public final class CircuitBreakerRegistry implements AutoCloseable {

  /**
   * Bucket for keys without a {@code prefix:} namespace.
   */
  public static final String DEFAULT_BUCKET = "_default";

  private final Cache<String, CircuitBreaker> breakers;
  private final ZetaProperties.CircuitBreaker config;
  private final SlidingWindowBreaker.Spec spec;

  /**
   * Sharded registry: one breaker per key prefix, up to {@code maxPrefixes}.
   *
   * <p>The window validation and the exception-filter class resolution run once here, so a bad
   * window fails the context at startup and a prefix churn allocates shards without repeating
   * either.
   *
   * @param config       breaker configuration shared by every shard (never {@code null})
   * @param maxPrefixes  live-breaker cap (positive; Caffeine size eviction past it)
   */
  public CircuitBreakerRegistry(ZetaProperties.CircuitBreaker config, int maxPrefixes) {
    this.config = config;
    this.spec = new SlidingWindowBreaker.Spec(config);
    this.breakers = Caffeine.newBuilder()
      .maximumSize(Math.max(1, maxPrefixes))
      .expireAfterAccess(30, TimeUnit.MINUTES)
      .build();
  }

  /**
   * The breaker owning {@code cacheKey}'s prefix (created on first use).
   *
   * @param cacheKey the cache key (may be {@code null} → default bucket)
   * @return the responsible breaker (never {@code null})
   */
  public CircuitBreaker breakerFor(String cacheKey) {
    return breakers.get(
      CacheKeysPolicy.prefixOf(cacheKey, DEFAULT_BUCKET),
      prefix -> new SlidingWindowBreaker(config, spec)
    );
  }

  /**
   * Whether {@code cacheKey}'s breaker is currently open. Read-only: never
   * creates a breaker.
   *
   * @param cacheKey the cache key
   * @return {@code true} when the responsible breaker exists and is open
   */
  public boolean isOpen(String cacheKey) {
    CircuitBreaker breaker = breakers.getIfPresent(CacheKeysPolicy.prefixOf(cacheKey, DEFAULT_BUCKET));
    return breaker != null && breaker.isOpen();
  }

  /**
   * Whether {@code cacheKey}'s breaker is degrading loads: OPEN, or any HALF_OPEN episode. A
   * HALF_OPEN breaker still admits a probe, but a denied surplus probe or a failed probe serves
   * stale rather than throwing. Read-only: never creates a breaker.
   *
   * @param cacheKey the cache key
   * @return {@code true} when the responsible breaker exists and is in a degraded state
   */
  public boolean isDegraded(String cacheKey) {
    CircuitBreaker breaker = breakers.getIfPresent(CacheKeysPolicy.prefixOf(cacheKey, DEFAULT_BUCKET));
    return breaker != null && breaker.isDegraded();
  }

  /**
   * Whether any live breaker is degrading loads (the no-key aggregate behind the stall gauges).
   *
   * @return {@code true} when at least one shard is open or recovering
   */
  public boolean isAnyDegraded() {
    for (CircuitBreaker breaker : breakers.asMap().values()) {
      if (breaker.isDegraded()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Drops every live shard breaker. A breaker owns no thread, timer or
   * scheduler — its window is clock-driven — so clearing the map is the whole
   * of the release.
   */
  @Override
  public void close() {
    breakers.invalidateAll();
  }
}
