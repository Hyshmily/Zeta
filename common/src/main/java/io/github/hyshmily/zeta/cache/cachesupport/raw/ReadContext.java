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
package io.github.hyshmily.zeta.cache.cachesupport.raw;

import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.StalePolicy;
import io.github.hyshmily.zeta.util.Internal;
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;

/**
 * Per-call read context for the options-based read paths: the resolved
 * {@link GetPolicy} knobs plus the derived {@code report} flag and
 * the lazily memoized TTL overrides.
 *
 * <p>
 * The TTL suppliers are memoized on first use so the documented
 * at-most-once-per-call contract of {@link GetPolicy} holds structurally,
 * even when several {@link HotKeyCache} methods consume the same override
 * within one call (e.g. the SOFT_REFRESH branch feeds both the background
 * refresh and the promotion probe). The memo state is two primitive fields, so
 * a call that never consumes a TTL — a plain NORMAL-entry hit, per the
 * {@link GetPolicy} contract — allocates nothing extra.
 *
 * <p>
 * One source abstraction: a typed {@link CacheLoader} or a plain supplier
 * adapted to one. The context holds no per-call key; {@link #loadValue(String)}
 * and {@link #refreshValue(String)} receive it from the caller, so a loader
 * call binds the key once instead of storing it in every context. The adapter
 * for a plain supplier is created lazily by {@link HotKeyCache} only when a
 * miss or a reconciliation actually needs a context; a plain hit allocates
 * neither the context nor the adapter.
 *
 * <p>
 * One instance per read call. The mutable memoization is confined to the
 * calling thread; a scheduled background refresh consumes only the reload source
 * and the immutable loader admission predicate, never the memoized TTLs (which
 * are resolved before scheduling).
 */
@Internal
public final class ReadContext {

  /**
   * A resolved TTL is any value except {@link #UNRESOLVED}; the sentinel keeps
   * one field per side instead of a value plus a resolved flag.
   */
  private static final long UNRESOLVED = Long.MIN_VALUE;

  /**
   * The lazy TTL suppliers. {@code null} only on the batch path, whose factory
   * pre-seeds both resolved values, so the lazy branch is never reached.
   */
  private final @Nullable LongSupplier rawHardTtlMs;
  private final @Nullable LongSupplier rawSoftTtlMs;

  /** The single value source, or {@code null} when this call carries no source. */
  private final @Nullable CacheLoader<String, Object> loader;

  private final StalePolicy stalePolicy;
  private final boolean callerRunsLoader;
  private final boolean report;

  private long hardTtl = UNRESOLVED;
  private long softTtl = UNRESOLVED;

  private ReadContext(
    @Nullable CacheLoader<String, Object> loader,
    StalePolicy stalePolicy,
    boolean report,
    boolean callerRunsLoader,
    @Nullable LongSupplier rawHardTtlMs,
    @Nullable LongSupplier rawSoftTtlMs
  ) {
    this.loader = loader;
    this.report = report;
    this.rawHardTtlMs = rawHardTtlMs;
    this.rawSoftTtlMs = rawSoftTtlMs;
    this.callerRunsLoader = callerRunsLoader;
    // Engine boundary for the tri-state policy: an unspecified (null) policy
    // becomes the default here, as in the read path's hit classification. The
    // annotation layer resolves the merge upstream, so the engine below this
    // point only sees a concrete value.
    this.stalePolicy = StalePolicy.orDefault(stalePolicy);
  }

  /**
   * Single-key read context: the TTL suppliers stay lazy and are memoized once
   * per call, so SpEL-backed expressions are evaluated at most once regardless
   * of how many downstream methods consume them. The loader is the single
   * source; a plain supplier has already been adapted by the caller.
   *
   * @param loader  the value source (may be {@code null} = no load)
   * @param options the resolved per-invocation read options
   * @return the per-call context
   */
  @SuppressWarnings("unchecked")
  public static ReadContext of(@Nullable CacheLoader<String, ?> loader, GetPolicy options) {
    return new ReadContext(
      (CacheLoader<String, Object>) loader,
      options.stalePolicy(),
      options.report(),
      options.callerRunsLoader(),
      options.hardTtlMs(),
      options.softTtlMs()
    );
  }

  /**
   * Batch read helpers: the TTL overrides are already-resolved constants
   * carried by the caller, so the memo is pre-seeded directly — no supplier
   * object and no lazy indirection is allocated per key. The batch path never
   * asks for caller-runs, so its readers run on the load executor.
   *
   * <p>The batch path has no separate refresh source and no admission filter:
   * the same loader serves a soft-expire background refresh, and every
   * non-{@code null} value is stored when the adapter's {@code isCacheable}
   * defaults to true.
   *
   * @param loader     the refresh source (may be {@code null} when the policy
   *                   has no refresh branch)
   * @param hardTtlMs  the batch-resolved hard TTL override
   * @param softTtlMs  the batch-resolved soft TTL override
   * @param report     whether this batch feeds Worker reporting
   * @param stalePolicy the batch-resolved stale policy
   * @return the per-call context
   */
  @SuppressWarnings("unchecked")
  public static ReadContext ofBatch(
    @Nullable CacheLoader<String, ?> loader,
    long hardTtlMs,
    long softTtlMs,
    boolean report,
    StalePolicy stalePolicy
  ) {
    ReadContext ctx = new ReadContext((CacheLoader<String, Object>) loader, stalePolicy, report, false, null, null);
    ctx.hardTtl = hardTtlMs;
    ctx.softTtl = softTtlMs;
    return ctx;
  }

  /** Whether this call has a value source. */
  public boolean hasSource() {
    return loader != null;
  }

  /**
   * Loads the foreground miss value through the source.
   *
   * @param cacheKey the key to load
   * @return the loaded value; {@code null} when the source produced none
   */
  public @Nullable Object loadValue(String cacheKey) {
    return loader != null ? loader.load(cacheKey) : null;
  }

  /**
   * Loads a background refresh value through the source's reload seam — a
   * refresh may not resume caller-bound state.
   *
   * @param cacheKey the key to reload
   * @return the refreshed value; {@code null} when the source produced none
   */
  public @Nullable Object refreshValue(String cacheKey) {
    return loader != null ? loader.reload(cacheKey) : null;
  }

  /**
   * Whether the given non-{@code null} loaded value may be stored in L1.
   * A {@code null} value is never admitted (never cached regardless of the
   * loader). A call with no source, or a plain-supplier adapter whose
   * {@code isCacheable} keeps its default {@code true}, admits every non-null
   * value.
   *
   * @param value the loaded value
   * @return {@code true} when the value may take L1 residency
   */
  public boolean admits(Object value) {
    return value != null && (loader == null || loader.isCacheable(value));
  }

  public StalePolicy stalePolicy() {
    return stalePolicy;
  }

  /** @return whether this read feeds the Worker detection/reporting plane */
  public boolean report() {
    return report;
  }

  /**
   * Whether the loader must stay on the calling thread. The cache miss path
   * honours this through SingleFlight's caller-runs election; a plain hit and
   * the background refresh path never consult it.
   */
  public boolean callerRunsLoader() {
    return callerRunsLoader;
  }

  /**
   * Memoized lazy hard TTL override (0 = use configured default): the
   * underlying (possibly SpEL-backed) supplier evaluates at most once per call.
   * This is the boundary a lazy override is validated at: a negative result is
   * rejected here, because the value does not exist until the supplier runs.
   *
   * @return the non-negative hard TTL override
   * @throws IllegalArgumentException when the supplier returns a negative value
   */
  public long hardTtlMs() {
    long resolved = hardTtl;
    if (resolved == UNRESOLVED && rawHardTtlMs != null) {
      hardTtl = resolved = requireNonNegative(rawHardTtlMs.getAsLong(), "hardTtlMs");
    }
    return resolved;
  }

  /**
   * Memoized lazy soft TTL override (0 = use configured default), same contract
   * and validation as {@link #hardTtlMs()}.
   *
   * @return the non-negative soft TTL override
   * @throws IllegalArgumentException when the supplier returns a negative value
   */
  public long softTtlMs() {
    long resolved = softTtl;
    if (resolved == UNRESOLVED && rawSoftTtlMs != null) {
      softTtl = resolved = requireNonNegative(rawSoftTtlMs.getAsLong(), "softTtlMs");
    }
    return resolved;
  }

  private static long requireNonNegative(long ttlMs, String name) {
    if (ttlMs < 0) {
      throw new IllegalArgumentException(name + " must not be negative");
    }
    return ttlMs;
  }
}
