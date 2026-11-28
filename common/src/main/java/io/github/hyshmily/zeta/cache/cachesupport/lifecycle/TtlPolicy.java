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
package io.github.hyshmily.zeta.cache.cachesupport.lifecycle;

import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import org.jspecify.annotations.Nullable;

/**
 * Pure TTL resolution and expiry arithmetic for {@link CacheEntry}.
 *
 * <p>TTL resolution occurs once at the configuration boundary so entries store only resolved
 * durations and deadlines. Freshness jitter is strictly one-sided (subtractive), guaranteeing that
 * an entry never outlives its declared {@code hardTtl}.
 *
 * <p><b>Deadlines are monotonic instants.</b> Every deadline is minted from and judged against
 * {@link TimeSource#monotonicMillis()}, the per-JVM clock anchored near the wall clock at startup.
 * A deadline never leaves the node that computed it (entries are process-local; no wire message
 * carries one), so the wall clock is never needed for TTL arithmetic — and keeping it out means an
 * NTP step can neither expire a live entry nor extend an expired one.
 */
@Internal
public final class TtlPolicy implements EntryDraft.ExpiryArithmetic {

  /** Sentinel declared TTL meaning "inherit the configured default". */
  public static final long INHERIT = 0L;

  /** Upper cap on the random jitter bound so {@code bound + 1} never overflows {@code long}. */
  private static final long MAX_JITTER_MS = Long.MAX_VALUE / 2;

  private final ZetaProperties ttlConfig;

  /**
   * Creates a TTL policy backed by live configuration properties.
   *
   * @param ttlConfig the live configuration supplying global TTL and jitter defaults
   */
  public TtlPolicy(ZetaProperties ttlConfig) {
    this.ttlConfig = Objects.requireNonNull(ttlConfig, "ttlConfig must not be null");
  }

  /**
   * Checks whether {@code entry}'s hard expiry deadline has passed as of the current monotonic
   * instant.
   *
   * @param entry the cache entry to inspect
   * @return {@code true} if the hard deadline is finite and has elapsed
   */
  public boolean isLogicallyExpired(CacheEntry entry) {
    return isLogicallyExpired(entry, TimeSource.monotonicMillis());
  }

  /**
   * Checks whether {@code entry}'s hard expiry deadline has passed at the given timestamp.
   *
   * @param entry the cache entry to inspect
   * @param now   the per-call monotonic-millisecond timestamp
   * @return {@code true} if the hard deadline is finite and {@code now >= hardExpireAtMs}
   */
  public boolean isLogicallyExpired(CacheEntry entry, long now) {
    Objects.requireNonNull(entry, "entry must not be null");
    long hardExpireAtMs = entry.getHardExpireAtMs();
    return hardExpireAtMs != Long.MAX_VALUE && now >= hardExpireAtMs;
  }

  /**
   * Whether the entry is servable now: present and not past its hard deadline. The null-safe pair
   * every plane needs before renewing, snapshotting or keeping a resident; {@link
   * io.github.hyshmily.zeta.cache.EntryView#isLive} is the snapshot-side form of the same rule.
   *
   * @param entry the resident entry, or {@code null} for an absent key
   * @return whether the entry is servable
   */
  public boolean isLive(@Nullable CacheEntry entry) {
    return entry != null && !isLogicallyExpired(entry);
  }

  /**
   * Checks whether {@code cacheEntry} is absent or its soft-refresh window has expired as of the
   * current monotonic instant.
   *
   * @param cacheEntry the cache entry to inspect, or {@code null}
   * @return {@code true} if {@code cacheEntry} is {@code null} or its soft window is enabled and
   *         has elapsed
   */
  public boolean isSoftExpired(@Nullable CacheEntry cacheEntry) {
    return isSoftExpired(cacheEntry, TimeSource.monotonicMillis());
  }

  /**
   * Checks whether {@code cacheEntry} is absent or its soft-refresh window has expired at {@code now}.
   *
   * @param cacheEntry the cache entry to inspect, or {@code null}
   * @param now        the per-call monotonic-millisecond timestamp
   * @return {@code true} if {@code cacheEntry} is {@code null} or its soft window is enabled and
   *         {@code softExpireAtMs <= now}
   */
  public boolean isSoftExpired(@Nullable CacheEntry cacheEntry, long now) {
    if (cacheEntry == null) {
      return true;
    }
    long softExpireAtMs = cacheEntry.getSoftExpireAtMs();
    return softExpireAtMs > 0L && softExpireAtMs <= now;
  }

  /**
   * Returns the configured default hard TTL in milliseconds.
   *
   * @return the positive default hard TTL in milliseconds
   */
  public long effectiveHardTtlMs() {
    return ttlConfig.getHardTtlMs();
  }

  /**
   * Resolves a per-call hard TTL override against this policy's configured default.
   *
   * @param hardTtlMs the declared hard TTL in milliseconds ({@code <= 0} inherits the default)
   * @return the resolved hard TTL in milliseconds
   */
  public long resolveHardTtl(long hardTtlMs) {
    return resolveHardTtl(hardTtlMs, effectiveHardTtlMs());
  }

  /**
   * Resolves a declared hard TTL against an explicit configured default.
   *
   * @param declared   the declared hard TTL in milliseconds ({@code <= 0} inherits {@code configured})
   * @param configured the fallback default hard TTL in milliseconds
   * @return the resolved hard TTL in milliseconds
   */
  public static long resolveHardTtl(long declared, long configured) {
    return declared > 0L ? declared : configured;
  }

  /**
   * Returns the configured default soft TTL in milliseconds.
   *
   * @return the default soft TTL in milliseconds, or {@code 0} when soft expiry is disabled
   */
  public long effectiveSoftTtlMs() {
    return ttlConfig.getSoftTtlMs();
  }

  /**
   * Resolves a per-call soft TTL override against this policy's configured default.
   *
   * @param softTtlMs the declared soft TTL in milliseconds ({@code < 0} disables, {@code 0}
   *                  inherits the default)
   * @return the resolved soft TTL in milliseconds, or {@code 0} when disabled
   */
  public long resolveSoftTtl(long softTtlMs) {
    return resolveSoftTtl(softTtlMs, effectiveSoftTtlMs());
  }

  /**
   * Resolves a declared soft TTL against an explicit configured default.
   *
   * @param declared   the declared soft TTL in milliseconds ({@code < 0} disables, {@code 0}
   *                   inherits {@code configured})
   * @param configured the fallback default soft TTL in milliseconds
   * @return the resolved soft TTL in milliseconds, or {@code 0} when disabled
   */
  public static long resolveSoftTtl(long declared, long configured) {
    if (declared < 0L) {
      return 0L;
    }
    return declared > 0L ? declared : configured;
  }

  /**
   * Resolves the soft TTL for a background refresh: an explicit per-call override takes precedence,
   * otherwise the entry's own stored soft TTL is preserved verbatim (including {@code 0} when
   * disabled, never falling back to the global default).
   *
   * @param entry             the existing entry being refreshed
   * @param softTtlOverrideMs the per-call override ({@code < 0} disables, {@code 0} derives from
   *                          {@code entry})
   * @return the effective soft TTL in milliseconds, or {@code 0} when disabled
   */
  public long resolveRefreshSoftTtl(CacheEntry entry, long softTtlOverrideMs) {
    Objects.requireNonNull(entry, "entry must not be null");
    if (softTtlOverrideMs != INHERIT) {
      return resolveSoftTtl(softTtlOverrideMs);
    }
    return entry.getSoftTtlMs();
  }

  /**
   * Converts a resolved hard TTL duration into an absolute monotonic-millisecond deadline with
   * one-sided subtractive jitter.
   *
   * @param hardTtlMs the resolved hard TTL in milliseconds (positive, or {@link Long#MAX_VALUE}
   *                  for permanent entries)
   * @return the absolute monotonic-millisecond hard deadline
   */
  @Override
  public long hardExpireAt(long hardTtlMs) {
    if (hardTtlMs == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    return expireAtFromNow(hardTtlMs, jitterOffset(hardTtlMs));
  }

  /**
   * Converts a resolved soft TTL duration into an absolute monotonic-millisecond deadline with
   * one-sided subtractive jitter.
   *
   * @param softTtlMs the resolved soft TTL in milliseconds ({@code <= 0} means disabled)
   * @return the absolute monotonic-millisecond soft deadline, or {@code 0} when disabled
   */
  @Override
  public long softExpireAt(long softTtlMs) {
    if (softTtlMs <= 0L) {
      return 0L;
    }
    if (softTtlMs == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    return expireAtFromNow(softTtlMs, jitterOffset(softTtlMs));
  }

  /**
   * Computes a saturating deadline {@code now + max(1, ttlMs + jitterMs)} so arithmetic never
   * wraps past {@link Long#MAX_VALUE}.
   *
   * @param ttlMs    the resolved TTL duration in milliseconds
   * @param jitterMs the non-positive jitter offset in milliseconds
   * @return the saturated monotonic-millisecond deadline
   */
  private static long expireAtFromNow(long ttlMs, long jitterMs) {
    long now = TimeSource.monotonicMillis();
    long delta = Math.max(1L, ttlMs + jitterMs);
    return now > Long.MAX_VALUE - delta ? Long.MAX_VALUE : now + delta;
  }

  /**
   * Samples a non-positive random jitter offset in {@code [-(ttlMs * ratio), 0]} to desynchronize
   * simultaneous expirations without ever extending a declared TTL.
   *
   * @param ttlMs the resolved TTL duration in milliseconds
   * @return a non-positive offset in milliseconds
   */
  private long jitterOffset(long ttlMs) {
    double ratio = ttlConfig.getTtlJitterRatio();
    if (ttlMs <= 0L || !(ratio > 0.0d)) {
      return 0L;
    }
    long baseJitter = (long) (ttlMs * ratio);
    if (baseJitter <= 0L) {
      return 0L;
    }
    long cappedJitter = Math.min(baseJitter, MAX_JITTER_MS);
    return -ThreadLocalRandom.current().nextLong(cappedJitter + 1L);
  }
}
