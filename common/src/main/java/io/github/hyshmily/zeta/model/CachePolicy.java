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
package io.github.hyshmily.zeta.model;

import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Immutable per-invocation cache policy, carrying the resolved storage-side
 * decisions for a single cache operation.
 *
 * <p>This record is the single transport object between the annotation layer
 * ({@code CacheExtensionAspect} → {@code ZetaCacheContext} → {@code ZetaSpringCache})
 * and the {@code Zeta} facade / {@code HotKeyCache} read paths. It replaces the
 * previous bag of loosely-coupled ThreadLocal fields.
 *
 * <h3>Lazy TTL contract</h3>
 * The TTL suppliers are evaluated <b>at most once per cache call</b> and only
 * when the value is actually needed — that is, when an entry is created on a
 * cache miss, promoted to HOT, renewed, or a soft-expire background refresh is
 * scheduled. A plain cache hit on a NORMAL entry never evaluates the suppliers,
 * so SpEL-based TTL expressions cost nothing on the hit path.
 *
 * <h3>Null-caching contract</h3>
 * When {@link #nullCaching()} is {@code true} (the default), a {@code null}
 * loader result is stored as a {@code NullValue} sentinel with a short TTL
 * ({@code zeta.local.null-value-ttl-seconds}) for cache-penetration protection.
 * When {@code false}, a {@code null} loader result leaves no cache entry at
 * all, so the next call re-invokes the loader.
 *
 * @param hardTtlMs    lazy hard TTL override in milliseconds (0 = use configured
 *                     default); evaluated at most once per cache call
 * @param softTtlMs    lazy soft TTL override in milliseconds (0 = use configured
 *                     default); evaluated at most once per cache call
 * @param nullCaching  whether {@code null} loader results may be cached as a
 *                     sentinel entry
 * @param skipBroadcast whether cross-instance sync messages are suppressed for
 *                     write/evict operations (ignored on read paths)
 * @param stalePolicy   what to do when the cached entry is soft-expired
 *                      (stale); defaults to {@link StalePolicy#SOFT_REFRESH}
 * @param reader        the value supplier for cache misses / refreshes;
 *                      {@code null} when not needed (e.g. annotation layer
 *                      carries its own loader separately)
 * @param reportEnabled whether to allow reporting this access to the Worker
 *                      for hot-key detection
 * @param failOnError   whether read-path failures (loader exceptions) propagate
 *                      to the caller instead of being swallowed as a cache miss;
 *                      {@code false} preserves the default resilient behavior
 */
public record CachePolicy(
  LongSupplier hardTtlMs,
  LongSupplier softTtlMs,
  boolean nullCaching,
  boolean skipBroadcast,
  StalePolicy stalePolicy,
  Supplier<?> reader,
  boolean reportEnabled,
  boolean failOnError
) {
  /** Shared zero supplier for "no TTL override". */
  private static final LongSupplier ZERO = () -> 0L;

  /**
   * Returns the shared {@link #ZERO} supplier for a zero (no-override) TTL,
   * avoiding a per-call capturing-lambda allocation on the common read path;
   * otherwise wraps the static value in a constant supplier.
   *
   * @param ttlMs the TTL override in milliseconds (0 = use configured default)
   * @return a supplier returning {@code ttlMs}
   */
  private static LongSupplier ttlSupplier(long ttlMs) {
    return ttlMs == 0L ? ZERO : () -> ttlMs;
  }

  /** Singleton carrying all-default semantics. */
  private static final CachePolicy DEFAULTS = new CachePolicy(
    ZERO,
    ZERO,
    true,
    false,
    StalePolicy.SOFT_REFRESH,
    null,
    true,
    false
  );

  /**
   * Compact constructor: {@code null} suppliers are normalized to a zero
   * supplier so accessors never return {@code null}.
   */
  public CachePolicy {
    if (hardTtlMs == null) hardTtlMs = ZERO;
    if (softTtlMs == null) softTtlMs = ZERO;
    if (stalePolicy == null) stalePolicy = StalePolicy.SOFT_REFRESH;
  }

  /**
   * Convenience constructor for the annotation layer and other callers that
   * manage the reader and reporting separately. Delegates to the canonical
   * constructor with {@code reader = null}, {@code reportEnabled = true} and
   * {@code failOnError = false}.
   */
  public CachePolicy(
    LongSupplier hardTtlMs,
    LongSupplier softTtlMs,
    boolean nullCaching,
    boolean skipBroadcast,
    StalePolicy stalePolicy
  ) {
    this(hardTtlMs, softTtlMs, nullCaching, skipBroadcast, stalePolicy, null, true, false);
  }

  /**
   * Builds a policy from a reader and all-default semantics: no TTL override,
   * null caching enabled, reporting enabled, failures swallowed. Combine with
   * {@link #withFailOnError()} to make read-path loader failures propagate to
   * the caller — that is the recommended way to distinguish a missing key
   * (empty result, still cached as a {@code NullValue} sentinel) from a
   * failing data source (thrown exception).
   *
   * @param reader the value supplier for cache misses / refreshes
   * @param <T>    the value type
   * @return a new policy instance
   */
  public static <T> CachePolicy of(Supplier<T> reader) {
    return new CachePolicy(ZERO, ZERO, true, false, StalePolicy.SOFT_REFRESH, reader, true, false);
  }

  /**
   * Returns a copy of this policy with {@link #failOnError()} set to
   * {@code true}: any {@link RuntimeException} on the read path (loader
   * exceptions, internal failures) propagates to the caller instead of being
   * swallowed as a cache miss. A {@code null} loader result is unaffected —
   * it is still an empty result with {@code NullValue} sentinel caching, so
   * "no data" stays distinguishable from "data source failed".
   *
   * @return a new policy instance with fail-fast semantics
   */
  public CachePolicy withFailOnError() {
    return new CachePolicy(hardTtlMs, softTtlMs, nullCaching, skipBroadcast, stalePolicy, reader, reportEnabled, true);
  }

  /**
   * Returns the shared all-defaults policy: no TTL override, null caching
   * enabled, broadcast enabled.
   *
   * @return the default policy singleton
   */
  public static CachePolicy defaults() {
    return DEFAULTS;
  }

  /**
   * Builds a policy from static TTL values. The values are wrapped into
   * constant suppliers; the lazy-evaluation contract is unaffected.
   *
   * @param hardTtlMs     hard TTL override (0 = use configured default)
   * @param softTtlMs     soft TTL override (0 = use configured default)
   * @param nullCaching   whether {@code null} loader results may be cached
   * @param skipBroadcast whether to suppress cross-instance sync messages
   * @return a new policy instance
   */
  public static CachePolicy of(long hardTtlMs, long softTtlMs, boolean nullCaching, boolean skipBroadcast) {
    return of(hardTtlMs, softTtlMs, nullCaching, skipBroadcast, StalePolicy.SOFT_REFRESH);
  }

  /**
   * Builds a policy from static TTL values and an explicit stale policy.
   * The values are wrapped into constant suppliers; the lazy-evaluation
   * contract is unaffected.
   *
   * @param hardTtlMs     hard TTL override (0 = use configured default)
   * @param softTtlMs     soft TTL override (0 = use configured default)
   * @param nullCaching   whether {@code null} loader results may be cached
   * @param skipBroadcast whether to suppress cross-instance sync messages
   * @param stalePolicy   what to do on soft-expire (stale) entries
   * @return a new policy instance
   */
  public static CachePolicy of(
    long hardTtlMs,
    long softTtlMs,
    boolean nullCaching,
    boolean skipBroadcast,
    StalePolicy stalePolicy
  ) {
    return new CachePolicy(
      ttlSupplier(hardTtlMs),
      ttlSupplier(softTtlMs),
      nullCaching,
      skipBroadcast,
      stalePolicy,
      null,
      true,
      false
    );
  }

  /**
   * Builds a full policy from a reader and all explicit cache-control knobs.
   * Intended for the direct (non-annotation) read API — the returned policy
   * carries both the data source and the behavioral decisions so that the
   * HotKeyCache deep methods can collapse to a single {@code CachePolicy}
   * parameter.
   *
   * @param reader          the value supplier for cache misses / refreshes
   * @param hardTtlMs       hard TTL override (0 = use configured default;
   *                        {@link Long#MAX_VALUE} for permanent entry)
   * @param softTtlMs       soft TTL override (0 = use configured default)
   * @param nullCaching     whether {@code null} loader results may be cached
   * @param reportEnabled   whether to allow reporting this access to the Worker
   * @param stalePolicy     what to do on soft-expire (stale) entries
   * @param <T>             the value type
   * @return a new policy instance
   */
  public static <T> CachePolicy of(
    Supplier<T> reader,
    long hardTtlMs,
    long softTtlMs,
    boolean nullCaching,
    boolean reportEnabled,
    StalePolicy stalePolicy
  ) {
    return new CachePolicy(
      ttlSupplier(hardTtlMs),
      ttlSupplier(softTtlMs),
      nullCaching,
      false,
      stalePolicy,
      reader,
      reportEnabled,
      false
    );
  }
}
