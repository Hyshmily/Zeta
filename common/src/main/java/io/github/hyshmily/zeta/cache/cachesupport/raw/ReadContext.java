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
 * A loader-backed call keeps the loader and its key by reference instead of
 * building per-call {@code load}/{@code reload}/{@code isCacheable} adapters, so
 * the miss path allocates only the context. A plain or batch call keeps its
 * reader, which serves both the foreground load and the background refresh.
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

  /** The loader and its bound key, or both {@code null} for a plain/batch reader. */
  private final @Nullable CacheLoader<String, Object> loader;
  private final @Nullable String loaderKey;

  private final StalePolicy stalePolicy;

  /** The plain reader, or {@code null} when the call is loader-backed. */
  private final @Nullable Supplier<?> reader;

  private final boolean callerRunsLoader;
  private final boolean report;

  private long hardTtl = UNRESOLVED;
  private long softTtl = UNRESOLVED;

  private ReadContext(
    @Nullable Supplier<?> reader,
    @Nullable CacheLoader<String, Object> loader,
    @Nullable String loaderKey,
    StalePolicy stalePolicy,
    boolean report,
    boolean callerRunsLoader,
    @Nullable LongSupplier rawHardTtlMs,
    @Nullable LongSupplier rawSoftTtlMs
  ) {
    this.reader = reader;
    this.loader = loader;
    this.loaderKey = loaderKey;
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
   * Single-key read path with a plain reader: the TTL suppliers stay lazy and are memoized once
   * per call, so SpEL-backed expressions are evaluated at most once regardless of how many
   * downstream methods consume them. The reader serves both the foreground load and the
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
   * Loader-backed single-key read: the loader and the key it is bound to travel
   * by reference, so a miss allocates only the context.
   *
   * @param loader   the typed loader carried by the call
   * @param cacheKey the key the loader is bound to
   * @param options  the resolved per-invocation read options
   * @return the per-call context
   */
  @SuppressWarnings("unchecked")
  public static ReadContext ofLoader(CacheLoader<String, ?> loader, String cacheKey, GetPolicy options) {
    return new ReadContext(
      null,
      (CacheLoader<String, Object>) loader,
      cacheKey,
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
   * the same reader serves a soft-expire background refresh, and every
   * non-{@code null} value is stored.
   */
  public static ReadContext ofBatch(
    @Nullable Supplier<?> reader,
    long hardTtlMs,
    long softTtlMs,
    boolean report,
    StalePolicy stalePolicy
  ) {
    ReadContext ctx = new ReadContext(reader, null, null, stalePolicy, report, false, null, null);
    ctx.hardTtl = hardTtlMs;
    ctx.softTtl = softTtlMs;
    return ctx;
  }

  /** Whether this call has a value source (a loader or a plain reader). */
  public boolean hasSource() {
    return loader != null || reader != null;
  }

  /**
   * Loads the foreground miss value: {@code loader.load(key)} for a
   * loader-backed call, the plain reader otherwise.
   *
   * @return the loaded value; {@code null} when the source produced none
   */
  public @Nullable Object loadValue() {
    return loader != null ? loader.load(loaderKey) : reader != null ? reader.get() : null;
  }

  /**
   * Loads a background refresh value: {@code loader.reload(key)} for a
   * loader-backed call — a refresh may not resume caller-bound state — and the
   * plain reader otherwise.
   *
   * @return the refreshed value; {@code null} when the source produced none
   */
  public @Nullable Object refreshValue() {
    return loader != null ? loader.reload(loaderKey) : reader != null ? reader.get() : null;
  }

  /**
   * Whether the given non-{@code null} loaded value may be stored in L1.
   * A {@code null} value is never admitted (never cached regardless of the
   * loader); a call with no loader admits everything.
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
