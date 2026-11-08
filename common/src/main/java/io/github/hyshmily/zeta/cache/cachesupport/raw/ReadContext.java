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
import java.util.function.Predicate;
import java.util.function.Supplier;
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
 * One instance per read call. The mutable memoization is confined to the
 * calling thread; a scheduled background refresh consumes only the
 * {@link #refreshReader() refresh reader} and the immutable admission
 * predicate, never the memoized TTLs (which are resolved before scheduling).
 */
@Internal
public final class ReadContext {

  /**
   * A resolved TTL is any value except {@link #UNRESOLVED}; the sentinel keeps
   * one field per side instead of a value plus a resolved flag. It is
   * {@link Long#MIN_VALUE}, not {@code -1}, so an invalid negative result from a
   * supplier is still memoized once; the TTL policy resolves any non-positive
   * value to the configured default.
   */
  private static final long UNRESOLVED = Long.MIN_VALUE;

  /**
   * The lazy TTL suppliers. {@code null} only on the batch path, whose factory
   * pre-seeds both resolved values, so the lazy branch is never reached.
   */
  private final @Nullable LongSupplier rawHardTtlMs;
  private final @Nullable LongSupplier rawSoftTtlMs;
  /** The background-refresh reader, or {@code null} for "same reader" ({@link #refreshReader()}). */
  private final @Nullable Supplier<?> refreshReader;
  private final @Nullable Predicate<Object> cacheFilter;
  private final StalePolicy stalePolicy;
  private final Supplier<?> reader;

  private final boolean callerRunsLoader;
  private final boolean report;

  private long hardTtl = UNRESOLVED;
  private long softTtl = UNRESOLVED;

  public ReadContext(
    Supplier<?> reader,
    @Nullable Supplier<?> refreshReader,
    @Nullable Predicate<Object> cacheFilter,
    StalePolicy stalePolicy,
    boolean report,
    boolean callerRunsLoader,
    @Nullable LongSupplier rawHardTtlMs,
    @Nullable LongSupplier rawSoftTtlMs
  ) {
    this.reader = reader;
    this.report = report;
    this.cacheFilter = cacheFilter;
    this.rawHardTtlMs = rawHardTtlMs;
    this.rawSoftTtlMs = rawSoftTtlMs;
    this.refreshReader = refreshReader;
    this.callerRunsLoader = callerRunsLoader;
    // Engine boundary for the tri-state policy: an unspecified (null) policy
    // becomes the default here, as in the read path's hit classification. The
    // annotation layer resolves the merge upstream, so the engine below this
    // point only sees a concrete value.
    this.stalePolicy = StalePolicy.orDefault(stalePolicy);
  }

  /**
   * Single-key read path with a plain reader: the TTL suppliers stay lazy and are memoized once
   * per call, so SpEL-backed expressions are evaluated at most once regardless of how many
   * downstream methods consume them. A plain supplier serves both the foreground load and the
   * background refresh, and admits every non-{@code null} value.
   *
   * @param reader  the foreground miss reader (may be {@code null} = no load)
   * @param options the resolved per-invocation read options
   * @return the per-call context
   */
  public static ReadContext of(@Nullable Supplier<?> reader, GetPolicy options) {
    return new ReadContext(
      reader,
      null,
      null,
      options.stalePolicy(),
      options.report(),
      options.callerRunsLoader(),
      options.hardTtlMs(),
      options.softTtlMs()
    );
  }

  /**
   * Loader-backed single-key read: the two readers and the admission predicate are derived
   * from the loader when — and only when — this context is actually built, so a plain hit
   * allocates none of them.
   *
   * @param loader   the typed loader carried by the call
   * @param cacheKey the key the loader is bound to
   * @param options  the resolved per-invocation read options
   * @return the per-call context
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static ReadContext ofLoader(CacheLoader<String, ?> loader, String cacheKey, GetPolicy options) {
    CacheLoader<String, Object> typed = (CacheLoader) loader;
    return new ReadContext(
      () -> typed.load(cacheKey),
      () -> typed.reload(cacheKey),
      typed::isCacheable,
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
   * <p>The batch path has no separate refresh reader and no admission filter:
   * the same reader serves a soft-expire background refresh, and every
   * non-{@code null} value is stored.
   */
  public static ReadContext ofBatch(
    Supplier<?> reader,
    long hardTtlMs,
    long softTtlMs,
    boolean report,
    StalePolicy stalePolicy
  ) {
    ReadContext ctx = new ReadContext(reader, reader, null, stalePolicy, report, false, null, null);
    ctx.hardTtl = hardTtlMs;
    ctx.softTtl = softTtlMs;
    return ctx;
  }

  public Supplier<?> reader() {
    return reader;
  }

  /**
   * The reader the engine uses when arming a background refresh. It is
   * separate from {@link #reader()} because a loader's refresh path may not
   * resume caller-bound state (a one-shot invocation chain, a thread-confined
   * transaction or security context); a {@code null} refresh reader means the
   * plain reader serves both paths.
   */
  public Supplier<?> refreshReader() {
    return refreshReader != null ? refreshReader : reader;
  }

  /**
   * Admission predicate for non-{@code null} loader results ({@code null} =
   * admit all): derived from the effective loader's {@code isCacheable}, never
   * settable per call.
   */
  public @Nullable Predicate<Object> cacheFilter() {
    return cacheFilter;
  }

  /**
   * Whether the given non-{@code null} loaded value may be stored in L1.
   * A {@code null} value is never admitted (never cached regardless of the
   * filter); a {@code null} filter admits everything.
   */
  public boolean admits(Object value) {
    return value != null && (cacheFilter == null || cacheFilter.test(value));
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
   */
  public long hardTtlMs() {
    long resolved = hardTtl;
    if (resolved == UNRESOLVED && rawHardTtlMs != null) {
      hardTtl = resolved = rawHardTtlMs.getAsLong();
    }
    return resolved;
  }

  /**
   * Memoized lazy soft TTL override (0 = use configured default), same contract
   * as {@link #hardTtlMs()}.
   */
  public long softTtlMs() {
    long resolved = softTtl;
    if (resolved == UNRESOLVED && rawSoftTtlMs != null) {
      softTtl = resolved = rawSoftTtlMs.getAsLong();
    }
    return resolved;
  }
}
