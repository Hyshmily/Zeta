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
package io.github.hyshmily.zeta.cache.cachesupport.policy;

import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import java.util.function.LongSupplier;

/**
 * Immutable per-invocation read options: pure data carrying the resolved
 * storage-side decisions for a single cache <em>read</em> operation.
 *
 * <p>One of three closed option records: each record carries exactly the knobs
 * its call family honors, so passing a read knob to a write is a compile error
 * instead of a silent no-op. The write side is {@link PutPolicy}, the
 * invalidation side {@link InvalidatePolicy}.
 *
 * <p>The data source travels <b>outside</b> this record, as an explicit
 * call parameter: either a plain {@link java.util.function.Supplier} reader
 * (everything non-{@code null} is cached) or a
 * {@link CacheLoader} (whose
 * {@code isCacheable} gates L1 storage per value). A per-call caller can
 * never smuggle an admission decision through the options — there is no
 * filter component at all.
 *
 * <h2>Lazy TTL contract</h2>
 * The TTL suppliers are evaluated <b>at most once per cache call</b> and only
 * when the value is actually needed — that is, when an entry is created on a
 * cache miss, renewed, or a soft-expire background refresh is scheduled. A
 * plain cache hit never evaluates the suppliers,
 * so SpEL-based TTL expressions cost nothing on the hit path.
 *
 * <h2>Null contract</h2>
 * A {@code null} loader result is never cached: no entry is created, the call
 * returns {@code null}, and the next read re-invokes the loader. There is no
 * opt-out — use {@link CacheLoader#isCacheable}
 * to decline caching a non-{@code null} value while still serving it.
 *
 * @param hardTtlMs     lazy hard TTL override in milliseconds ({@code 0}/{@code -1} = use the
 *                      configured default); evaluated at most once per cache call
 * @param softTtlMs     lazy soft TTL override in milliseconds ({@code 0} = use the configured
 *                      default, {@code -1} = disabled); evaluated at most once per cache call
 * @param stalePolicy   what to do when the cached entry is soft-expired
 *                      (stale); {@code null} means <b>unspecified</b> and the
 *                      engine applies {@link StalePolicy#SOFT_REFRESH}. The
 *                      tri-state ({@code null} = inherit, vs. an explicit
 *                      {@link StalePolicy#SOFT_REFRESH}) is what lets the
 *                      enclosing advice's resolved policy win over a per-call default.
 * @param report whether to allow reporting this access to the Worker
 *                      for hot-key detection
 * @param callerRunsLoader whether the loader must execute on the calling thread
 *                      while SingleFlight still deduplicates same-key loads;
 *                      {@code false} lets the implementation run it on the load
 *                      executor. Set only where thread affinity is part of the
 *                      caller's contract (annotation reads: the business body
 *                      must keep its transaction, security context and MDC).
 */
public record GetPolicy(
  LongSupplier hardTtlMs,
  LongSupplier softTtlMs,
  StalePolicy stalePolicy,
  boolean report,
  boolean callerRunsLoader
) {
  /** Shared zero supplier for "no TTL override". */
  static final LongSupplier ZERO = () -> 0L;
  /** Singleton carrying all-default semantics. */
  private static final GetPolicy DEFAULTS = new GetPolicy(ZERO, ZERO, StalePolicy.SOFT_REFRESH, true, false);

  /**
   * Wraps a static hard TTL override in a constant supplier, sharing the singleton
   * zero supplier for the "no override" case. The chokepoint for every static
   * TTL that reaches this record, so the declaration rule is enforced once:
   * {@code 0} and {@code -1} both mean "configured default".
   *
   * @param ttlMs the TTL override in milliseconds ({@code 0}/{@code -1} = configured default)
   * @return a supplier returning the override, or the zero supplier for a default
   * @throws IllegalArgumentException when {@code ttlMs} is negative other than {@code -1}, or
   *                                  exceeds the packed maximum other than
   *                                  {@link Long#MAX_VALUE} — the option boundary rejects what
   *                                  the entry layer could not encode
   */
  static LongSupplier ttlSupplier(long ttlMs) {
    return constantSupplier(ttlMs, false);
  }

  /**
   * Wraps a soft TTL override: {@code -1} is the explicit "disable background refresh" the
   * declaration layer accepts, {@code 0} inherits the configured default, and every positive
   * value is an ordinary duration.
   *
   * @param ttlMs the soft TTL override in milliseconds ({@code -1} = disabled)
   * @return a supplier returning {@code ttlMs}
   * @throws IllegalArgumentException when the value is negative other than {@code -1}, or above
   *                                  the packed maximum other than {@link Long#MAX_VALUE}
   */
  static LongSupplier softTtlSupplier(long ttlMs) {
    return constantSupplier(ttlMs, true);
  }

  /**
   * The one supplier rule for a declared static TTL: validate the declaration vocabulary once,
   * then share the zero supplier for the "inherit" value. A soft TTL preserves {@code -1} because
   * it is a real instruction ("no window"); a hard TTL has no off state, so {@code -1} inherits
   * like {@code 0}.
   *
   * @param ttlMs        the declared TTL in milliseconds
   * @param allowDisable whether {@code -1} is the disable sentinel rather than an inherit value
   * @return the constant supplier, or the shared zero supplier
   */
  private static LongSupplier constantSupplier(long ttlMs, boolean allowDisable) {
    CacheEntry.requireDeclaredTtl(ttlMs, "TTL override");
    return (allowDisable ? ttlMs == 0L : ttlMs <= 0L) ? ZERO : () -> ttlMs;
  }

  /**
   * Compact constructor: {@code null} TTL suppliers are normalized to a zero
   * supplier so accessors never return {@code null}. {@code stalePolicy} is
   * deliberately <b>not</b> normalized — {@code null} carries the distinct
   * meaning "unspecified, let the enclosing advice decide", and the engine
   * applies the default at its own boundary.
   */
  public GetPolicy {
    if (hardTtlMs == null) hardTtlMs = ZERO;
    if (softTtlMs == null) softTtlMs = ZERO;
  }

  /**
   * Builds full options from static TTL values and all explicit
   * cache-control knobs.
   *
   * @param hardTtlMs     hard TTL override ({@code 0}/{@code -1} = configured default;
   *                      {@link Long#MAX_VALUE} for permanent entry)
   * @param softTtlMs     soft TTL override ({@code 0} = configured default, {@code -1} = disabled)
   * @param reportEnabled whether to allow reporting this access to the Worker
   * @param stalePolicy   what to do on soft-expire (stale) entries;
   *                      {@code null} = unspecified (engine default)
   * @return a new options instance
   */
  public static GetPolicy of(long hardTtlMs, long softTtlMs, boolean reportEnabled, StalePolicy stalePolicy) {
    return new GetPolicy(ttlSupplier(hardTtlMs), softTtlSupplier(softTtlMs), stalePolicy, reportEnabled, false);
  }

  /**
   * Returns the shared all-defaults options: no TTL override, reporting
   * enabled, and the engine's own stale policy ({@link StalePolicy#SOFT_REFRESH})
   * materialized. This is the concrete default a facade call uses; {@code null}
   * remains available on {@link #of} for an advice layer that wants to merge a
   * caller-unspecified policy with its own configuration.
   *
   * @return the default options singleton
   */
  public static GetPolicy defaults() {
    return DEFAULTS;
  }
}
