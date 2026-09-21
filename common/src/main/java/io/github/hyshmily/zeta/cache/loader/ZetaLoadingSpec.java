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
package io.github.hyshmily.zeta.cache.loader;

import io.github.hyshmily.zeta.model.CachePolicy;
import io.github.hyshmily.zeta.model.StalePolicy;
import java.util.Objects;
import java.util.function.Supplier;
import org.springframework.util.Assert;

/**
 * Immutable, per-prefix loading specification: pairs a {@link CacheLoader}
 * with the read policy applied whenever the registry routes a key to it.
 *
 * <p>Semantic mapping to Caffeine's builder vocabulary:
 * <ul>
 *   <li>{@code hardTtlMs} — the {@code expireAfterWrite} equivalent (0 = use the
 *       configured {@code zeta.local} default);</li>
 *   <li>{@code softTtlMs} — the {@code refreshAfterWrite} equivalent (stale-while-revalidate
 *       window; 0 = use the configured default);</li>
 *   <li>{@code stalePolicy} — what to serve when the soft TTL has elapsed
 *       ({@link StalePolicy#SOFT_REFRESH} default).</li>
 * </ul>
 *
 * <p>The TTL / null-caching knobs follow the {@link CachePolicy} contract: the
 * spec values are per-namespace overrides, {@code 0} means "use the configured
 * global default", and a plain hit never evaluates anything.
 *
 * @param <V> the value type produced by the loader
 * @see ZetaLoaderRegistry#register(String, ZetaLoadingSpec)
 */
public final class ZetaLoadingSpec<V> {

  private final CacheLoader<V> loader;
  private final long hardTtlMs;
  private final long softTtlMs;
  private final StalePolicy stalePolicy;
  private final boolean nullCaching;
  private final boolean reportEnabled;
  private final boolean failOnError;

  private ZetaLoadingSpec(Builder<V> builder) {
    // Single validation point: guards of(null) as well as build() without a loader,
    // so a spec can never exist with a null loader (which would NPE only on first load).
    Assert.notNull(builder.loader, "loader must be set");
    this.loader = builder.loader;
    this.hardTtlMs = builder.hardTtlMs;
    this.softTtlMs = builder.softTtlMs;
    this.stalePolicy = builder.stalePolicy;
    this.nullCaching = builder.nullCaching;
    this.reportEnabled = builder.reportEnabled;
    this.failOnError = builder.failOnError;
  }

  /**
   * Create a spec with all-default semantics: no TTL override, SOFT_REFRESH,
   * null caching enabled, reporting enabled, failures swallowed.
   *
   * @param loader the data-source loader (never {@code null})
   * @param <V>    the value type
   * @return a default spec
   */
  public static <V> ZetaLoadingSpec<V> of(CacheLoader<V> loader) {
    return new ZetaLoadingSpec<>(new Builder<>(loader));
  }

  /** Returns a new builder; {@link Builder#build()} requires a loader to be set. */
  public static <V> Builder<V> builder() {
    return new Builder<>(null);
  }

  public CacheLoader<V> loader() {
    return loader;
  }

  /** Hard TTL override in ms (0 = use configured default). */
  public long hardTtlMs() {
    return hardTtlMs;
  }

  /** Soft TTL override in ms (0 = use configured default). */
  public long softTtlMs() {
    return softTtlMs;
  }

  public StalePolicy stalePolicy() {
    return stalePolicy;
  }

  public boolean nullCaching() {
    return nullCaching;
  }

  public boolean reportEnabled() {
    return reportEnabled;
  }

  public boolean failOnError() {
    return failOnError;
  }

  /**
   * Returns a copy of this spec with the loader replaced and every other knob
   * (TTL overrides, stale policy, null-caching, reporting, failure semantics)
   * carried over unchanged — the explicit "inherit this spec's policy, swap
   * only the data source" form. The source spec is never mutated.
   *
   * <p>
   * This is the intended companion for call sites that need a different data
   * source for keys governed by this spec: derive a policy from the copy via
   * {@link #toPolicy(String)} (optionally with a {@link StalePolicy}
   * override) and hand it to the {@link CachePolicy}-taking facade overload.
   * Passing a bare reader to a facade overload instead would bypass the
   * registry entirely and fall back to global defaults, silently dropping
   * this spec's TTL overrides and {@code failOnError}.
   *
   * @param loader the replacement data-source loader (never {@code null})
   * @return a new immutable spec sharing this spec's configuration
   */
  public ZetaLoadingSpec<V> withLoader(CacheLoader<V> loader) {
    Builder<V> copy = new Builder<>(loader)
      .hardTtl(hardTtlMs)
      .softTtl(softTtlMs)
      .stalePolicy(stalePolicy)
      .nullCaching(nullCaching);
    if (!reportEnabled) {
      copy.skipReport();
    }
    if (failOnError) {
      copy.failOnError();
    }
    return copy.build();
  }

  /**
   * Wrap this spec's loader into a {@link CachePolicy} for the given cache key,
   * carrying the spec's TTL overrides, null-caching, reporting, stale policy,
   * and failure semantics. The returned policy is what feeds the existing
   * {@code Zeta.get(key, policy)} chain, so SingleFlight deduplication, circuit
   * breaking, background refresh, and Worker reporting all apply unchanged.
   *
   * @param cacheKey the key being read (captured by the reader lambda)
   * @return a policy that loads through this spec
   */
  public CachePolicy toPolicy(String cacheKey) {
    return toPolicy(cacheKey, stalePolicy);
  }

  /**
   * Variant of {@link #toPolicy(String)} with a per-call {@link StalePolicy}
   * override.
   *
   * @param cacheKey       the key being read
   * @param stalePolicyOverride the stale policy to apply instead of the spec's
   * @return a policy that loads through this spec
   */
  public CachePolicy toPolicy(String cacheKey, StalePolicy stalePolicyOverride) {
    Objects.requireNonNull(stalePolicyOverride, "stalePolicyOverride must not be null");
    Supplier<Object> reader = () -> loader.load(cacheKey);
    CachePolicy policy = CachePolicy.of(reader, hardTtlMs, softTtlMs, nullCaching, reportEnabled, stalePolicyOverride);
    return failOnError ? policy.withFailOnError() : policy;
  }

  /** Fluent builder for {@link ZetaLoadingSpec}. */
  public static final class Builder<V> {

    private CacheLoader<V> loader;
    private long hardTtlMs;
    private long softTtlMs;
    private StalePolicy stalePolicy = StalePolicy.SOFT_REFRESH;
    private boolean nullCaching = true;
    private boolean reportEnabled = true;
    private boolean failOnError = false;

    private Builder(CacheLoader<V> loader) {
      this.loader = loader;
    }

    /**
     * Set the data-source loader (required).
     *
     * @param loader the loader invoked on misses and background refreshes
     * @return this builder
     */
    public Builder<V> loader(CacheLoader<V> loader) {
      Assert.notNull(loader, "loader must not be null");
      this.loader = loader;
      return this;
    }

    /**
     * Hard TTL override — the {@code expireAfterWrite} equivalent.
     *
     * @param hardTtlMs hard TTL in ms (0 = use configured default; {@link Long#MAX_VALUE} for permanent entry)
     * @return this builder
     */
    public Builder<V> hardTtl(long hardTtlMs) {
      Assert.isTrue(hardTtlMs >= 0, "hardTtlMs must not be negative");
      this.hardTtlMs = hardTtlMs;
      return this;
    }

    /**
     * Soft TTL override — the {@code refreshAfterWrite} equivalent.
     *
     * @param softTtlMs soft TTL in ms (0 = use configured default)
     * @return this builder
     */
    public Builder<V> softTtl(long softTtlMs) {
      Assert.isTrue(softTtlMs >= 0, "softTtlMs must not be negative");
      this.softTtlMs = softTtlMs;
      return this;
    }

    /**
     * Set what to serve when the soft TTL has elapsed.
     *
     * @param stalePolicy the stale policy (default {@link StalePolicy#SOFT_REFRESH})
     * @return this builder
     */
    public Builder<V> stalePolicy(StalePolicy stalePolicy) {
      Assert.notNull(stalePolicy, "stalePolicy must not be null");
      this.stalePolicy = stalePolicy;
      return this;
    }

    /**
     * Set whether {@code null} loader results may be cached (default
     * {@code true}, matching {@link CachePolicy#nullCaching()}).
     *
     * <p>{@code true}: a {@code null} loader result is stored as a short-TTL
     * {@code NullValue} sentinel (cache-penetration protection). {@code false}:
     * a {@code null} loader result leaves no cache entry, so the next read
     * re-invokes the loader.
     *
     * @param nullCaching whether to cache {@code null} loader results
     * @return this builder
     */
    public Builder<V> nullCaching(boolean nullCaching) {
      this.nullCaching = nullCaching;
      return this;
    }

    /**
     * Disable Worker reporting for reads routed through this spec.
     *
     * @return this builder
     */
    public Builder<V> skipReport() {
      this.reportEnabled = false;
      return this;
    }

    /**
     * Propagate loader exceptions to the caller instead of swallowing them as
     * misses (distinguishes a failing data source from an absent key).
     *
     * @return this builder
     */
    public Builder<V> failOnError() {
      this.failOnError = true;
      return this;
    }

    /** Validate and build. @return an immutable spec */
    public ZetaLoadingSpec<V> build() {
      Assert.notNull(loader, "loader must be set before build()");
      return new ZetaLoadingSpec<>(this);
    }
  }
}
