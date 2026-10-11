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
import io.github.hyshmily.zeta.annotation.annotationsupporter.NullValue;
import io.github.hyshmily.zeta.exception.ZetaBlockedException;
import io.github.hyshmily.zeta.model.ReadPolicy;
import io.github.hyshmily.zeta.model.StalePolicy;
import io.github.hyshmily.zeta.model.WritePolicy;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.springframework.util.Assert;

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
 *       .orNull();
 * </pre>
 *
 * <p>Created via {@link Zeta#read(String)}. Instances are reusable;
 * every terminal call ({@link #execute()}, {@link #orNull()} or
 * {@link #orElse(Object)}) re-runs the resolution.
 */
public class ZetaReadQuery<T> {

  /** No-op writer for fallback caching — a fallback resolves a value; it performs no data-source mutation. */
  private static final Runnable NOOP_WRITER = () -> {};

  private final Zeta zeta;
  private final String cacheKey;
  private Supplier<T> primaryReader;
  private CacheMode cacheMode = CacheMode.GET;
  private long hardTtlMs = 0;
  private long softTtlMs = 0;
  private StalePolicy stalePolicy = StalePolicy.SOFT_REFRESH;
  private boolean nullCaching = true;
  // Broadcast-by-default (I3): a fallback-resolved value is cluster state and
  // must converge — local-only is the explicit opt-out via notAllowBroadcast().
  private boolean isAllowBroadcast = true;
  // Worker-report-by-default: the primary read reports like every other read
  // path; background/probe reads opt out via report(false).
  private boolean reportEnabled = true;
  private List<Supplier<T>> fallbacks;

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
   * <p>When enabled (the default), any value resolved from a fallback reader
   * will be send to peer instances via AMQP. When disabled via
   * {@link #notAllowBroadcast()}, the value is written only to the local L1
   * cache via {@link Zeta#putLocal}.
   *
   * @return this query instance
   */
  public ZetaReadQuery<T> allowBroadcast() {
    this.isAllowBroadcast = true;
    return this;
  }

  /**
   * Disable cross-instance send for fallback values.
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
    return withPrimary(reader, CacheMode.GET);
  }

  /**
   * Set the primary data-source reader with an explicit {@link CacheMode}.
   *
   * @param reader a supplier that loads the value from the data source
   * @param mode   the cache access mode
   * @return this query instance
   */
  public ZetaReadQuery<T> withPrimary(Supplier<T> reader, CacheMode mode) {
    Assert.notNull(reader, "reader must not be null");
    Assert.notNull(mode, "mode must not be null");
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
   * Override the stale policy — what happens when the cached entry is
   * soft-expired (stale) but not yet hard-expired.
   *
   * <p>Default {@link StalePolicy#SOFT_REFRESH} (serve stale, refresh in the
   * background). Use {@link StalePolicy#RETURN} to never trigger a background
   * load, or {@link StalePolicy#REVALIDATE} to block the caller on a fresh
   * load instead of serving stale data.
   *
   * @param stalePolicy the stale policy for this query (never {@code null})
   * @return this query instance
   */
  public ZetaReadQuery<T> withStalePolicy(StalePolicy stalePolicy) {
    Assert.notNull(stalePolicy, "stalePolicy must not be null");
    this.stalePolicy = stalePolicy;
    return this;
  }

  /**
   * Set whether {@code null} reader results may be cached (default
   * {@code true}, matching {@link ReadPolicy#nullCaching()}).
   *
   * <p>{@code true}: when the primary reader or a fallback reader returns
   * {@code null}, a sentinel value ({@link NullValue#INSTANCE}) is cached with
   * a short TTL ({@code zeta.local.null-value-ttl-seconds}) so that subsequent
   * reads for the same key return {@link Optional#empty()} without invoking
   * the reader again — until the sentinel expires.
   *
   * <p>{@code false}: a {@code null} reader result creates no cache entry at
   * all, so the next query re-invokes the reader. Fallback readers are
   * likewise not cached when they return {@code null}.
   *
   * @param nullCaching whether to cache {@code null} reader results
   * @return this query instance
   */
  public ZetaReadQuery<T> nullCaching(boolean nullCaching) {
    this.nullCaching = nullCaching;
    return this;
  }

  /**
   * Set whether this query's primary read reports to the Worker (default
   * {@code true}, matching every other read path). Background/probe reads
   * that must not pollute hot-key detection pass {@code false}.
   *
   * @param reportEnabled whether to allow Worker reporting for the primary read
   * @return this query instance
   */
  public ZetaReadQuery<T> report(boolean reportEnabled) {
    this.reportEnabled = reportEnabled;
    return this;
  }

  /**
   * Add a fallback reader to the chain.
   *
   * <p>Fallbacks are executed sequentially in registration order when the
   * primary reader does not return a value (either cache miss or reader
   * returns {@code null}). A fallback that <i>throws</i> is skipped and the
   * next fallback runs ({@link ZetaBlockedException} and {@link Error} still
   * propagate — policy rejection and fatal faults are never swallowable).
   * Each non-null result is cached before being returned.
   *
   * @param reader a supplier that loads the value from a fallback data source
   * @return this query instance
   */
  public ZetaReadQuery<T> thenExecute(Supplier<T> reader) {
    Assert.notNull(reader, "reader must not be null");
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
   * <p>Queries are reusable — every terminal call re-runs the resolution.
   *
   * @return the resolved value, or {@code null} if empty
   * @throws ZetaBlockedException if the key matches a block rule
   */
  public T orNull() {
    return execute().orElse(null);
  }

  /**
   * Execute the read query and return the resolved value, or the given
   * {@code defaultValue} if empty.
   *
   * <p>The default value is <b>not</b> cached. It is returned only for this
   * single invocation.
   *
   * <p>Queries are reusable — every terminal call re-runs the resolution.
   *
   * @param defaultValue the value to return if all readers return {@code null}
   * @return the resolved value, or {@code defaultValue} if empty
   * @throws ZetaBlockedException if the key matches a block rule
   */
  public T orElse(T defaultValue) {
    return execute().orElse(defaultValue);
  }

  /**
   * Execute the read query and return the resolved value as an {@link Optional}.
   *
   * <p>Resolution order: L1 hit, then the primary reader on a miss, then the
   * fallback readers in registration order (a throwing fallback is skipped,
   * except {@link ZetaBlockedException}/{@link Error} which propagate). The
   * first non-null result is cached
   * (broadcast to peers by default; {@link #notAllowBroadcast()} opts out) and returned.
   * When every reader yields {@code null} and null caching is allowed, one
   * short-TTL {@code NullValue} sentinel is cached so subsequent reads return
   * {@link Optional#empty()} without invoking any reader again — until the
   * sentinel expires.
   *
   * <p>Queries are reusable — every terminal call re-runs the resolution.
   *
   * @return the resolved value, or an empty {@code Optional} if no reader produced a value
   * @throws ZetaBlockedException if the key matches a block rule (propagated from the cache layer)
   * @throws IllegalStateException if no reader was configured at all
   */
  @SuppressWarnings("unchecked")
  public Optional<T> execute() {
    if (primaryReader == null && (fallbacks == null || fallbacks.isEmpty())) {
      throw new IllegalStateException(
        "ZetaReadQuery has no reader: call withPrimary(...) and/or thenExecute(...) before execute()"
      );
    }

    // BLOCK-rule enforcement is centralized in the cache layer (HotKeyCache.preGuard),
    // which throws ZetaBlockedException on every read path — no duplicate check here.

    // The primary reader returns the raw value: a null result is handled
    // inside the cache layer, which stores a short-TTL NullValue sentinel
    // (when null caching is allowed) or leaves no entry at all (when
    // disallowed via nullCaching(false)).
    ReadPolicy policy = ReadPolicy.of(primaryReader, hardTtlMs, softTtlMs, nullCaching, reportEnabled, stalePolicy);

    Optional<Object> result = switch (cacheMode) {
      case GET -> zeta.get(cacheKey, policy);
      case GET_WITH_SOFT_EXPIRE -> zeta.getWithSoftExpire(cacheKey, policy);
    };

    if (result.isPresent()) {
      return Optional.of((T) result.get());
    }

    if (fallbacks != null) {
      for (Supplier<T> fallback : fallbacks) {
        final T val;
        try {
          val = fallback.get();
        } catch (ZetaBlockedException | Error e) {
          // Policy rejection and fatal faults are never swallowable — same
          // contract as the cache read paths.
          throw e;
        } catch (RuntimeException e) {
          // A failing fallback must not break the chain: its whole purpose
          // is "try the next source when the previous one is down".
          continue;
        }

        if (val != null) {
          if (isAllowBroadcast) {
            zeta.putThrough(cacheKey, val, NOOP_WRITER, WritePolicy.of(hardTtlMs, softTtlMs));
          } else {
            zeta.putLocal(cacheKey, val, WritePolicy.of(hardTtlMs, softTtlMs));
          }
          return Optional.of(val);
        }
      }

      if (nullCaching) {
        // The whole chain yielded null: cache ONE NullValue sentinel through the
        // cache layer's read path (not a raw putLocal/putThrough) — it gets the
        // short null-value TTL the nullCaching(true) contract promises (the same
        // nullTtlSeconds path the loader uses), and no version INCR / REFRESH
        // broadcast is emitted for a null. The read runs with the Worker report
        // suppressed (the primary read already reported this access); the local
        // TopK increment still applies, as it does on every cache-layer read.
        // Cached once here, after the loop: the final L1 state is identical (a
        // later successful fallback overwrites any sentinel), and the common
        // case — the primary read already stored its sentinel — pays one hit
        // instead of one full read path per null fallback.
        zeta.get(cacheKey, ReadPolicy.of(() -> null, 0L, 0L, true, false, StalePolicy.RETURN));
      }
    }

    return Optional.empty();
  }
}
