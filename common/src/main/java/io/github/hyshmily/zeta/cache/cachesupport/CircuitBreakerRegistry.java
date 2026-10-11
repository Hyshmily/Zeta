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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.cache.cachesupport.impl.CircuitBreakerImpl;
import io.github.hyshmily.zeta.util.executor.SafeScheduledExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Per-key-prefix circuit-breaker shard map.
 *
 * <p>A single global breaker lets one sick data source silence every key:
 * {@code user:} rows timing out on the DB open the breaker, and the healthy
 * {@code product:} Redis path degrades with them. This registry shards the
 * breaker by the key prefix (the segment before the first {@code ':'} —
 * the conventional datasource namespace; prefix-less keys share the
 * {@value #DEFAULT_BUCKET} bucket), so a failure domain trips only its own
 * breaker.
 *
 * <p>Bounds: at most {@code maxPrefixes} live breakers (Caffeine
 * size-eviction, least-recently-used; evicted breakers are closed) plus
 * 30-minute access expiry, so an unbounded key space cannot grow this map.
 * All breakers share one daemon slide thread (not one per breaker) and read
 * the same live {@link CircuitBreakerSettings}.
 *
 * <p>Shutting down ({@link #close()}) closes every live breaker and the
 * shared scheduler — wire it as the bean destroy method.
 */
@Internal
public final class CircuitBreakerRegistry implements AutoCloseable {

  /** Bucket for keys without a {@code prefix:} namespace. */
  public static final String DEFAULT_BUCKET = "_default";

  /** Default shard cap: enough for a datasource-per-prefix layout, small enough to bound threads. */
  public static final int DEFAULT_MAX_PREFIXES = 64;

  private final CircuitBreaker fixed;
  private final CircuitBreakerSettings config;
  private final Cache<String, CircuitBreaker> breakers;
  private final ScheduledExecutorService slideScheduler;

  /**
   * Sharded registry: one breaker per key prefix, up to {@code maxPrefixes}.
   *
   * @param config       breaker configuration view shared by every shard (never {@code null})
   * @param maxPrefixes  live-breaker cap (positive; least-recently-used eviction past it)
   */
  public CircuitBreakerRegistry(CircuitBreakerSettings config, int maxPrefixes) {
    this.fixed = null;
    this.config = config;
    this.slideScheduler = new SafeScheduledExecutorService(1, r -> {
      Thread t = new Thread(r, "zeta-cb-shared");
      t.setDaemon(true);
      return t;
    });
    this.breakers = Caffeine.newBuilder()
      .maximumSize(Math.max(1, maxPrefixes))
      .expireAfterAccess(30, TimeUnit.MINUTES)
      .removalListener((String key, CircuitBreaker breaker, com.github.benmanes.caffeine.cache.RemovalCause cause) -> {
        if (breaker != null) {
          try {
            breaker.close();
          } catch (Exception ignored) {
            // Close is best-effort lifecycle hygiene; never fail the eviction.
          }
        }
      })
      .build();
  }

  private CircuitBreakerRegistry(CircuitBreaker fixed) {
    this.fixed = fixed;
    this.config = null;
    this.breakers = null;
    this.slideScheduler = null;
  }

  /**
   * Single-breaker registry: every key shares {@code breaker}. Compatibility
   * bridge for direct {@code SingleFlightImpl} constructions (tests, legacy
   * wiring) — no sharding, no lifecycle (the caller's breaker stays the
   * caller's to close).
   *
   * @param breaker the shared breaker (never {@code null})
   * @return a registry routing every key to {@code breaker}
   */
  public static CircuitBreakerRegistry fixed(CircuitBreaker breaker) {
    if (breaker == null) {
      throw new IllegalArgumentException("breaker must not be null");
    }
    return new CircuitBreakerRegistry(breaker);
  }

  /**
   * The breaker owning {@code cacheKey}'s prefix (created on first use in
   * sharded mode).
   *
   * @param cacheKey the normalized cache key (may be {@code null} → default bucket)
   * @return the responsible breaker (never {@code null})
   */
  public CircuitBreaker breakerFor(String cacheKey) {
    if (fixed != null) {
      return fixed;
    }
    return breakers.get(prefixOf(cacheKey), prefix -> {
      CircuitBreakerImpl breaker = new CircuitBreakerImpl(config, slideScheduler);
      breaker.start();
      return breaker;
    });
  }

  /**
   * Whether {@code cacheKey}'s breaker is currently open. Read-only: never
   * creates a breaker.
   *
   * @param cacheKey the normalized cache key
   * @return {@code true} when the responsible breaker exists and is open
   */
  public boolean isOpen(String cacheKey) {
    if (fixed != null) {
      return fixed.isOpen();
    }
    CircuitBreaker breaker = breakers.getIfPresent(prefixOf(cacheKey));
    return breaker != null && breaker.isOpen();
  }

  /**
   * Whether any live breaker is currently open (the no-key aggregate behind
   * the stall gauges).
   *
   * @return {@code true} when at least one shard is open
   */
  public boolean isAnyOpen() {
    if (fixed != null) {
      return fixed.isOpen();
    }
    for (CircuitBreaker breaker : breakers.asMap().values()) {
      if (breaker.isOpen()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Live shard count (for endpoints and tests).
   *
   * @return the number of prefix breakers currently held
   */
  public long estimatedPrefixCount() {
    if (fixed != null) {
      return 1;
    }
    return breakers.estimatedSize();
  }

  /**
   * Extract the shard prefix: the segment before the first {@code ':'}, or
   * the default bucket for prefix-less keys.
   *
   * @param cacheKey the normalized cache key (may be {@code null})
   * @return the shard name (never {@code null})
   */
  public static String prefixOf(String cacheKey) {
    if (cacheKey == null) {
      return DEFAULT_BUCKET;
    }
    int sep = cacheKey.indexOf(':');
    return sep > 0 ? cacheKey.substring(0, sep) : DEFAULT_BUCKET;
  }

  /**
   * Closes every live shard breaker and the shared slide scheduler. A
   * {@link #fixed(CircuitBreaker)} registry closes nothing — the shared
   * breaker stays its creator's to close.
   */
  @Override
  public void close() {
    if (fixed != null) {
      return;
    }
    try {
      breakers.invalidateAll();
    } finally {
      slideScheduler.shutdownNow();
    }
  }
}
