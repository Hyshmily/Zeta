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
 * Immutable per-invocation read policy, carrying the resolved storage-side
 * decisions for a single cache <em>read</em> operation (ADR-0088).
 *
 * <p>One of three closed policy records replacing the former single
 * {@code CachePolicy}: each record carries exactly the knobs its call family
 * honors, so passing a read knob to a write is a compile error instead of a
 * silent no-op. The write side is {@link WritePolicy}, the invalidation side
 * {@link InvalidatePolicy}.
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
 * @param stalePolicy  what to do when the cached entry is soft-expired
 *                     (stale); defaults to {@link StalePolicy#SOFT_REFRESH}
 * @param reader       the value supplier for cache misses / refreshes;
 *                     {@code null} when not needed (the annotation layer
 *                     injects its loader separately via {@link #withReader})
 * @param reportEnabled whether to allow reporting this access to the Worker
 *                     for hot-key detection
 * @param failOnError  whether read-path failures (loader exceptions) propagate
 *                     to the caller instead of being swallowed as a cache miss;
 *                     {@code false} preserves the default resilient behavior
 */
public record ReadPolicy(
  LongSupplier hardTtlMs,
  LongSupplier softTtlMs,
  boolean nullCaching,
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
  private static final ReadPolicy DEFAULTS = new ReadPolicy(
    ZERO,
    ZERO,
    true,
    StalePolicy.SOFT_REFRESH,
    null,
    true,
    false
  );

  /**
   * Compact constructor: {@code null} suppliers are normalized to a zero
   * supplier so accessors never return {@code null}.
   */
  public ReadPolicy {
    if (hardTtlMs == null) hardTtlMs = ZERO;
    if (softTtlMs == null) softTtlMs = ZERO;
    if (stalePolicy == null) stalePolicy = StalePolicy.SOFT_REFRESH;
  }

  /**
   * Builds a policy from a reader and all-default semantics: no TTL override,
   * null caching enabled, reporting enabled, failures swallowed. Combine with
   * {@link #withFailOnError()} to make read-path loader failures propagate to
   * the caller — that is the recommended way to distinguish a missing key
   * (empty result, still cached as a {@code NullValue} sentinel) from a
   * failing data source (thrown exception).
   *
   * <p>If a loader spec is registered for this key's prefix, prefer deriving
   * the policy from it via
   * {@link io.github.hyshmily.zeta.cache.loader.ZetaLoadingSpec#toReadPolicy(String)}
   * (or the no-reader facade overloads): a hand-built reader policy bypasses
   * the registry and silently drops the spec's TTL overrides and failure
   * semantics.
   *
   * @param reader the value supplier for cache misses / refreshes
   * @param <T>    the value type
   * @return a new policy instance
   */
  public static <T> ReadPolicy of(Supplier<T> reader) {
    return new ReadPolicy(ZERO, ZERO, true, StalePolicy.SOFT_REFRESH, reader, true, false);
  }

  /**
   * Builds a policy from a reader and static TTL overrides with all-default
   * semantics otherwise (null caching enabled, reporting enabled, failures
   * swallowed, {@link StalePolicy#SOFT_REFRESH}).
   *
   * @param reader    the value supplier for cache misses / refreshes
   * @param hardTtlMs hard TTL override (0 = use configured default;
   *                  {@link Long#MAX_VALUE} for permanent entry)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param <T>       the value type
   * @return a new policy instance
   */
  public static <T> ReadPolicy of(Supplier<T> reader, long hardTtlMs, long softTtlMs) {
    return new ReadPolicy(
      ttlSupplier(hardTtlMs),
      ttlSupplier(softTtlMs),
      true,
      StalePolicy.SOFT_REFRESH,
      reader,
      true,
      false
    );
  }

  /**
   * Builds a policy from static TTL values with all-default semantics (null
   * caching enabled, reporting enabled, {@link StalePolicy#SOFT_REFRESH}, no
   * reader). Intended for read templates whose data source is injected later
   * (e.g. batch paths building per-key readers, the annotation layer via
   * {@link #withReader}).
   *
   * @param hardTtlMs hard TTL override (0 = use configured default)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @return a new policy instance
   */
  public static ReadPolicy of(long hardTtlMs, long softTtlMs) {
    return new ReadPolicy(
      ttlSupplier(hardTtlMs),
      ttlSupplier(softTtlMs),
      true,
      StalePolicy.SOFT_REFRESH,
      null,
      true,
      false
    );
  }

  /**
   * Builds a full policy from a reader and all explicit cache-control knobs.
   * Intended for the direct (non-annotation) read API and the registry spec
   * path — the returned policy carries both the data source and the behavioral
   * decisions.
   *
   * @param reader        the value supplier for cache misses / refreshes
   * @param hardTtlMs     hard TTL override (0 = use configured default;
   *                      {@link Long#MAX_VALUE} for permanent entry)
   * @param softTtlMs     soft TTL override (0 = use configured default)
   * @param nullCaching   whether {@code null} loader results may be cached
   * @param reportEnabled whether to allow reporting this access to the Worker
   * @param stalePolicy   what to do on soft-expire (stale) entries
   * @param <T>           the value type
   * @return a new policy instance
   */
  public static <T> ReadPolicy of(
    Supplier<T> reader,
    long hardTtlMs,
    long softTtlMs,
    boolean nullCaching,
    boolean reportEnabled,
    StalePolicy stalePolicy
  ) {
    return new ReadPolicy(
      ttlSupplier(hardTtlMs),
      ttlSupplier(softTtlMs),
      nullCaching,
      stalePolicy,
      reader,
      reportEnabled,
      false
    );
  }

  /**
   * Returns the shared all-defaults policy: no TTL override, null caching
   * enabled, reporting enabled.
   *
   * @return the default policy singleton
   */
  public static ReadPolicy defaults() {
    return DEFAULTS;
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
  public ReadPolicy withFailOnError() {
    return new ReadPolicy(hardTtlMs, softTtlMs, nullCaching, stalePolicy, reader, reportEnabled, true);
  }

  /**
   * Returns a copy of this policy with the value supplier replaced: all other
   * components (lazy TTLs, null-caching, stale-policy, reporting, failure
   * semantics) are carried over unchanged. The annotation layer's sync
   * read uses this to inject Spring's {@code valueLoader} into the
   * thread-bound policy, which carries the storage-side decisions but no
   * reader of its own.
   *
   * @param reader the value supplier for cache misses / refreshes ({@code null}
   *               allowed, meaning no reader)
   * @return a new policy instance with the given reader
   */
  public ReadPolicy withReader(Supplier<?> reader) {
    return new ReadPolicy(
      hardTtlMs, softTtlMs, nullCaching, stalePolicy, reader, reportEnabled, failOnError
    );
  }

  /**
   * Returns a copy of this policy with a static hard TTL override. The value
   * is wrapped into a constant supplier; the lazy-evaluation contract is
   * unaffected for suppliers carried over from the source policy.
   *
   * @param hardTtlMs hard TTL override (0 = use configured default)
   * @return a new policy instance
   */
  public ReadPolicy withHardTtl(long hardTtlMs) {
    return new ReadPolicy(
      ttlSupplier(hardTtlMs), softTtlMs, nullCaching, stalePolicy, reader, reportEnabled, failOnError
    );
  }

  /**
   * Returns a copy of this policy with a static soft TTL override. The value
   * is wrapped into a constant supplier; the lazy-evaluation contract is
   * unaffected for suppliers carried over from the source policy.
   *
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @return a new policy instance
   */
  public ReadPolicy withSoftTtl(long softTtlMs) {
    return new ReadPolicy(
      hardTtlMs, ttlSupplier(softTtlMs), nullCaching, stalePolicy, reader, reportEnabled, failOnError
    );
  }

  /**
   * Returns a copy of this policy with the null-caching decision replaced.
   *
   * @param nullCaching whether {@code null} loader results may be cached as a
   *                    sentinel entry
   * @return a new policy instance
   */
  public ReadPolicy withNullCaching(boolean nullCaching) {
    return new ReadPolicy(
      hardTtlMs, softTtlMs, nullCaching, stalePolicy, reader, reportEnabled, failOnError
    );
  }
}
