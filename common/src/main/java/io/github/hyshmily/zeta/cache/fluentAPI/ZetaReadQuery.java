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
package io.github.hyshmily.zeta.cache.fluentAPI;

import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.cache.annotationsupporter.NullValue;
import io.github.hyshmily.zeta.exception.ZetaBlockedException;
import io.github.hyshmily.zeta.rule.Rule;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Fluent read query for the HotKey cache.
 *
 * <p>Provides a builder-pattern API for reading from the L1 cache with an
 * optional primary data-source reader, a chain of fallback readers, and
 * fine-grained control over null caching, send behaviour, and TTL
 * overrides.
 *
 * <h3>Usage</h3>
 * <pre>
 *   User user = hotKey.read("user:42")
 *       .withPrimary(userRepository::findById)
 *       .thenExecute(backupRepository::findById)
 *       .withHardTtl(30_000)
 *       .withSoftTtl(10_000)
 *       .allowBroadcast()
 *       .executeOrNull();
 * </pre>
 *
 * <p>Created via {@link Zeta#read(String)}. Instances are single-use;
 * call {@link #execute()} or {@link #executeOrNull()} exactly once.
 */
public class ZetaReadQuery<T> {

  private final Zeta zeta;
  private final String cacheKey;
  private Supplier<T> primaryReader;
  private CacheMode cacheMode = CacheMode.GET;
  private long hardTtlMs = 0;
  private long softTtlMs = 0;
  private boolean isAllowNullCaching = true;
  private boolean isAllowBroadcast = false;
  private List<Supplier<T>> fallbacks;
  private final AtomicBoolean executed = new AtomicBoolean(false);

  /**
   * Creates a new read query bound to the given cache key.
   *
   * @param zeta   the HotKey facade
   * @param cacheKey the cache key to read
   */
  public ZetaReadQuery(Zeta zeta, String cacheKey) {
    this.zeta = zeta;
    this.cacheKey = cacheKey;
  }

  /**
   * Enable cross-instance send for fallback values.
   *
   * <p>When enabled, any value resolved from a fallback reader will be
   * send to peer instances via AMQP.  When disabled (default), the
   * value is written only to the local L1 cache via {@link Zeta#putLocal}.
   *
   * @return this query instance
   */
  public ZetaReadQuery<T> allowBroadcast() {
    this.isAllowBroadcast = true;
    return this;
  }

  /**
   * Disable cross-instance send for fallback values (default).
   *
   * @return this query instance
   */
  public ZetaReadQuery<T> notAllowBroadcast() {
    this.isAllowBroadcast = false;
    return this;
  }

  /**
   * Set the primary data-source reader with the default {@link CacheMode}.
   *
   * @param reader a supplier that loads the value from the data source
   * @return this query instance
   */
  public ZetaReadQuery<T> withPrimary(Supplier<T> reader) {
    return withPrimary(reader, cacheMode);
  }

  /**
   * Set the primary data-source reader with an explicit {@link CacheMode}.
   *
   * @param reader a supplier that loads the value from the data source
   * @param mode   the cache access mode
   * @return this query instance
   */
  public ZetaReadQuery<T> withPrimary(Supplier<T> reader, CacheMode mode) {
    this.primaryReader = reader;
    this.cacheMode = mode;
    return this;
  }

  /**
   * Override the hard TTL for this query.
   *
   * @param hardTtlMs hard TTL in milliseconds (0 = use configured default)
   * @return this query instance
   */
  public ZetaReadQuery<T> withHardTtl(long hardTtlMs) {
    this.hardTtlMs = hardTtlMs;
    return this;
  }

  /**
   * Override the soft TTL for this query.
   *
   * @param softTtlMs soft TTL in milliseconds (0 = use configured default)
   * @return this query instance
   */
  public ZetaReadQuery<T> withSoftTtl(long softTtlMs) {
    this.softTtlMs = softTtlMs;
    return this;
  }

  /**
   * Override both hard and soft TTL for this query.
   *
   * @param hardTtlMs hard TTL in milliseconds (0 = use configured default)
   * @param softTtlMs soft TTL in milliseconds (0 = use configured default)
   * @return this query instance
   */
  public ZetaReadQuery<T> withTtl(long hardTtlMs, long softTtlMs) {
    this.hardTtlMs = hardTtlMs;
    this.softTtlMs = softTtlMs;
    return this;
  }

  /**
   * Disable null-value caching.
   *
   * <p>When the primary or fallback reader returns {@code null}, the value
   * will not be cached.
   *
   * @return this query instance
   */
  public ZetaReadQuery<T> notAllowNull() {
    this.isAllowNullCaching = false;
    return this;
  }

  /**
   * Enable null-value caching (default).
   *
   * <p>When the primary or fallback reader returns {@code null}, a sentinel
   * value ({@link NullValue#INSTANCE}) is cached so that subsequent reads
   * for the same key return {@link Optional#empty()} without invoking the
   * reader again.
   *
   * @return this query instance
   */
  public ZetaReadQuery<T> allowNull() {
    this.isAllowNullCaching = true;
    return this;
  }

  /**
   * Add a fallback reader to the chain.
   *
   * <p>Fallbacks are executed sequentially in registration order when the
   * primary reader does not return a value (either cache miss or reader
   * returns {@code null}).  Each non-null result is cached before being
   * returned.
   *
   * @param reader a supplier that loads the value from a fallback data source
   * @return this query instance
   */
  public ZetaReadQuery<T> thenExecute(Supplier<T> reader) {
    if (fallbacks == null) {
      fallbacks = new ArrayList<>();
    }
    fallbacks.add(reader);
    return this;
  }

  /**
   * Execute the read query and return the resolved value, or {@code null} if no
   * value is available.
   *
   * <p>Convenience terminal method that preserves compile-time type inference
   * in simple {@code return} statements, avoiding the need to unwrap an
   * {@link Optional}.
   *
   * @return the resolved value, or {@code null} if empty
   * @throws ZetaBlockedException if the key matches a block rule
   * @throws IllegalStateException if this query has already been executed
   */
  public T executeOrNull() {
    return execute().orElse(null);
  }

  /**
   * Execute the read query and return the resolved value, or the given
   * {@code defaultValue} if all readers return {@code null}.
   *
   * <p>The default value is <b>not</b> cached.  It is returned only for this
   * single invocation.
   *
   * @param defaultValue the value to return if all readers return {@code null}
   * @return the resolved value, or {@code defaultValue} if empty
   * @throws ZetaBlockedException if the key matches a block rule
   * @throws IllegalStateException if this query has already been executed
   */
  public T executeOrNull(T defaultValue) {
    return execute().orElse(defaultValue);
  }

  @SuppressWarnings("all")
  public Optional<T> execute() {
    if (!executed.compareAndSet(false, true)) {
      throw new IllegalStateException("ZetaReadQuery can only be executed once");
    }

    if (zeta.evaluateRule(cacheKey) == Rule.RuleAction.BLOCK) {
      throw new ZetaBlockedException("ZetaReadQuery", cacheKey);
    }

    Supplier<Object> wrappedPrimary = () -> {
      T val = primaryReader.get();
      if (val == null) {
        return isAllowNullCaching ? NullValue.INSTANCE : null;
      }
      return val;
    };

    Optional<Object> result = switch (cacheMode) {
      case GET -> zeta.get(cacheKey, wrappedPrimary, hardTtlMs, softTtlMs);
      case GET_WITH_SOFT_EXPIRE -> zeta.getWithSoftExpire(cacheKey, wrappedPrimary, hardTtlMs, softTtlMs, true);
    };

    if (result.isPresent()) {
      Object val = result.get();
      if (val == NullValue.INSTANCE) {
        return Optional.empty();
      }
      return Optional.of((T) val);
    }

    if (fallbacks != null) {
      for (Supplier<T> fallback : fallbacks) {
        T val = fallback.get();

        if (val != null) {
          if (isAllowBroadcast) {
            zeta.putThrough(cacheKey, val, () -> {}, hardTtlMs, softTtlMs, true);
          } else {
            zeta.putLocal(cacheKey, val, hardTtlMs, softTtlMs);
          }
          return Optional.of(val);
        }

        if (isAllowNullCaching) {
          if (isAllowBroadcast) {
            zeta.putThrough(cacheKey, NullValue.INSTANCE, () -> {}, hardTtlMs, softTtlMs, true);
          } else {
            zeta.putLocal(cacheKey, NullValue.INSTANCE, hardTtlMs, softTtlMs);
          }
        }
      }
    }

    return Optional.empty();
  }
}
