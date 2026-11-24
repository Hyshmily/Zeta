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
 * within one call (the SOFT_REFRESH branch reads the override once for the
 * background refresh and once for the entry build). The memo state is two
 * primitive fields, so a call that never consumes a TTL — a plain hit, per the
 * {@link GetPolicy} contract — allocates nothing extra.
 *
 * <p>
 * One source abstraction: a typed {@link CacheLoader} or a plain supplier
 * adapted to one. The context holds no per-call key; {@link #loadValue(String)}
 * receives it from the caller, so a loader call binds the key once instead of
 * storing it in every context. The adapter for a plain supplier is created
 * lazily by {@link HotKeyCache} only when a miss actually needs a context; a
 * plain hit allocates neither the context nor the adapter.
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
   * The lazy TTL suppliers, memoized once per call. {@link GetPolicy} replaces a
   * {@code null} supplier with a constant zero, so both are always present.
   */
  private final LongSupplier rawHardTtlMs;
  private final LongSupplier rawSoftTtlMs;

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
    LongSupplier rawHardTtlMs,
    LongSupplier rawSoftTtlMs
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

  /** Whether this call has a value source. */
  public boolean hasSource() {
    return loader != null;
  }

  /**
   * The single value source, or {@code null} when this call carries none.
   *
   * <p>The seam a background refresh captures: the scheduled task takes this
   * reference instead of the context, so a queued refresh cannot pin the call's
   * memoized TTL suppliers or the SpEL scope behind them.
   *
   * @return the loader/reader adapter, or {@code null}
   */
  public @Nullable CacheLoader<String, Object> loader() {
    return loader;
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
   * Memoized lazy hard TTL override ({@code 0}/{@code -1} = configured default): the
   * underlying (possibly SpEL-backed) supplier evaluates at most once per call.
   * This is the boundary a lazy override is validated at: the value does not
   * exist until the supplier runs, so the same encodability rule the static
   * {@link GetPolicy} boundary applies must be applied here.
   *
   * @return the declared hard TTL override
   * @throws IllegalArgumentException when the supplier returns a negative value other than
   *                                  {@code -1}, or one above the packed maximum other than
   *                                  {@link Long#MAX_VALUE}
   */
  public long hardTtlMs() {
    long resolved = hardTtl;
    if (resolved == UNRESOLVED) {
      long value = rawHardTtlMs.getAsLong();
      CacheEntry.requireDeclaredTtl(value, "hardTtlMs");
      hardTtl = resolved = value;
    }
    return resolved;
  }

  /**
   * Memoized lazy soft TTL override ({@code 0} = configured default, {@code -1} = disabled), same
   * contract and validation as {@link #hardTtlMs()}.
   *
   * @return the declared soft TTL override
   * @throws IllegalArgumentException when the supplier returns a negative value other than
   *                                  {@code -1}, or one above the packed maximum other than
   *                                  {@link Long#MAX_VALUE}
   */
  public long softTtlMs() {
    long resolved = softTtl;
    if (resolved == UNRESOLVED) {
      long value = rawSoftTtlMs.getAsLong();
      CacheEntry.requireDeclaredTtl(value, "softTtlMs");
      softTtl = resolved = value;
    }
    return resolved;
  }
}
