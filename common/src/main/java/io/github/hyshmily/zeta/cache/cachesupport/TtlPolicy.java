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
package io.github.hyshmily.zeta.cache.cachesupport;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.EntryDraft;
import io.github.hyshmily.zeta.util.DelayUtil;
import io.github.hyshmily.zeta.util.TimeSource;
import org.jspecify.annotations.Nullable;

/**
 * Pure TTL and expiry policy for {@link io.github.hyshmily.zeta.model.CacheEntry}
 * lifecycle arithmetic.
 *
 * <p>Deep module extracted from {@link ExpireManager}: every stateless
 * TTL computation — resolve (override vs default), compute (absolute
 * expire timestamps), getEffective (configured defaults), timestamp
 * conversion with jitter, and expiry predicates — lives here behind one
 * small interface. {@link ExpireManager} keeps only the stateful parts
 * (refresh scheduling, TOCTOU guards, entry factory) and exposes this
 * policy via {@code ttlPolicy()}.
 *
 * <p>The class also implements {@link EntryDraft.ExpiryArithmetic}: the draft
 * API ({@code ExpireManager.newEntry/editEntry}) delegates its computed
 * expire timestamps to these {@code to*ExpireTimestamp} conversions — the
 * single duration-to-timestamp rule for the whole entry pipeline. The former
 * {@code applyXxx} entry transforms are folded into
 * {@link EntryDraft#ttl}/{@link EntryDraft#rearmExpiry}.
 *
 * <p>Instances are cheap and hold no mutable state of their own: the
 * underlying {@link ZetaProperties} is read on every call (same as the
 * pre-extraction behaviour), so runtime configuration updates keep
 * working. All methods are thread-safe and side-effect free.
 */
@Internal
public final class TtlPolicy implements EntryDraft.ExpiryArithmetic {

  /** TTL configuration providing normal and hot-key TTL values. */
  private final ZetaProperties ttlConfig;

  /** Jitter ratio applied to TTLs to prevent cache stampedes (from config, default 0.05 = ±5%). */
  private final double defaultTtlJitterRatio;

  /**
   * Create a TTL policy backed by the given configuration.
   *
   * @param ttlConfig            TTL configuration (normal and hot-key variants)
   * @param defaultTtlJitterRatio default jitter ratio for TTL stampede prevention
   */
  public TtlPolicy(ZetaProperties ttlConfig, double defaultTtlJitterRatio) {
    this.ttlConfig = ttlConfig;
    this.defaultTtlJitterRatio = defaultTtlJitterRatio;
  }

  /**
   * Check whether a {@link CacheEntry} has logically expired based on its
   * {@code hardExpireAtMs}.  Entries with {@code hardExpireAtMs == Long.MAX_VALUE}
   * are treated as permanent (never logically expire).
   *
   * @param entry the cache entry to inspect
   * @return {@code true} if the entry has logically expired
   */
  public boolean isLogicallyExpired(CacheEntry entry) {
    return entry.getHardExpireAtMs() != Long.MAX_VALUE && TimeSource.currentTimeMillis() >= entry.getHardExpireAtMs();
  }

  public long computeNullExpireAt(long nullTtlMs) {
    long effective = nullTtlMs > 0 ? nullTtlMs : ttlConfig.effectiveNullTtlMs();
    return toHardExpireTimestamp(effective);
  }

  /**
   * Hard expire timestamp from an explicit TTL duration.
   * Falls back to the normal-key default if {@code hardTtlMs <= 0}.
   *
   * @param hardTtlMs the hard TTL duration in milliseconds (&lt;= 0 uses configured default)
   * @return absolute epoch-ms timestamp for hard expiry
   */
  public long computeHardExpireAt(long hardTtlMs) {
    return toHardExpireTimestamp(resolveEffectiveHardTtl(hardTtlMs));
  }

  /**
   * Hard expire timestamp for hot keys, using {@code default-hot-hard-ttl} / {@code hot-hard-ttl}.
   * Returns {@code Long.MAX_VALUE} if hot hard expire is disabled (TTL &lt;= 0).
   *
   * @return absolute epoch-ms timestamp for hot-key hard expiry
   */
  public long computeHotHardExpireAt() {
    return toHardExpireTimestamp(ttlConfig.effectiveHotHardTtlMs());
  }

  /**
   * Soft expire timestamp for hot keys, using {@code default-hot-soft-ttl} / {@code hot-soft-ttl}.
   *
   * @return absolute epoch-ms timestamp for hot-key soft expiry, or 0 if disabled
   */
  public long computeHotSoftExpireAt() {
    return toSoftExpireTimestamp(ttlConfig.effectiveHotSoftTtlMs());
  }

  /**
   * Soft expire timestamp from an explicit TTL duration.
   * Falls back to the normal-key default if {@code softTtlMs <= 0}.
   *
   * @param softTtlMs the soft TTL duration in milliseconds (&lt;= 0 uses configured default)
   * @return absolute epoch-ms timestamp for soft expiry, or 0 if TTL is non-positive
   */
  public long computeSoftExpireAt(long softTtlMs) {
    return toSoftExpireTimestamp(resolveEffectiveSoftTtl(softTtlMs));
  }

  /**
   * Effective hard TTL for normal keys (override > default).
   *
   * @return effective hard TTL duration in milliseconds
   */
  public long getEffectiveHardTtlMs() {
    return ttlConfig.effectiveHardTtlMs();
  }

  /**
   * Resolve effective hard TTL for normal keys: use the override value if
   * positive, otherwise fall back to the configured default.
   *
   * @param hardTtlMs hard TTL override ({@code 0} or negative uses default)
   * @return effective hard TTL duration in milliseconds
   */
  public long resolveEffectiveHardTtl(long hardTtlMs) {
    return hardTtlMs > 0 ? hardTtlMs : getEffectiveHardTtlMs();
  }

  /**
   * Effective hard TTL for hot keys (override > default).
   *
   * @return effective hot hard TTL duration in milliseconds
   */
  public long getEffectiveHotHardTtlMs() {
    return ttlConfig.effectiveHotHardTtlMs();
  }

  /**
   * Resolve effective hard TTL for hot keys: the configured hot-key TTL is
   * the floor — a positive override may only raise it
   * ({@code max(override, hotDefault)}), so promotion to HOT never shortens
   * an entry's lifetime.
   *
   * @param hardTtlMs hard TTL override ({@code 0} or negative uses the hot default)
   * @return effective hot-key hard TTL duration in milliseconds
   */
  public long resolveEffectiveHotHard(long hardTtlMs) {
    return hardTtlMs > 0 ? Math.max(hardTtlMs, getEffectiveHotHardTtlMs()) : getEffectiveHotHardTtlMs();
  }

  /**
   * Effective soft TTL for normal keys (override > default).
   *
   * @return effective soft TTL duration in milliseconds
   */
  public long getEffectiveSoftTtlMs() {
    return ttlConfig.effectiveSoftTtlMs();
  }

  /**
   * Resolve effective soft TTL for normal keys: use the override value if
   * positive, otherwise fall back to the configured default.
   *
   * @param softTtlMs soft TTL override ({@code 0} or negative uses default)
   * @return effective soft TTL duration in milliseconds
   */
  public long resolveEffectiveSoftTtl(long softTtlMs) {
    return softTtlMs > 0 ? softTtlMs : getEffectiveSoftTtlMs();
  }

  /**
   * Effective soft TTL for hot keys (override > default).
   *
   * @return effective hot soft TTL duration in milliseconds
   */
  public long getEffectiveHotSoftTtlMs() {
    return ttlConfig.effectiveHotSoftTtlMs();
  }

  /**
   * Resolve effective soft TTL for hot keys: the configured hot-key TTL is
   * the floor — a positive override may only raise it
   * ({@code max(override, hotDefault)}), so promotion to HOT never shortens
   * an entry's lifetime.
   *
   * @param softTtlMs soft TTL override ({@code 0} or negative uses the hot default)
   * @return effective hot-key soft TTL duration in milliseconds
   */
  public long resolveEffectiveHotSoft(long softTtlMs) {
    return softTtlMs > 0 ? Math.max(softTtlMs, getEffectiveHotSoftTtlMs()) : getEffectiveHotSoftTtlMs();
  }

  /**
   * Convert a TTL duration (ms) to an absolute epoch-ms expiration timestamp
   * using the configured default jitter ratio.
   * Propagates {@link Long#MAX_VALUE} unchanged — used to signal permanent entries
   * (pure logical expiry with no hard TTL eviction).
   */
  public long toHardExpireTimestamp(long hardTtlMs) {
    return toHardExpireTimestamp(hardTtlMs, defaultTtlJitterRatio);
  }

  /**
   * Convert a TTL duration (ms) to an absolute epoch-ms expiration timestamp
   * using the given jitter ratio instead of the configured default.
   * Propagates {@link Long#MAX_VALUE} unchanged.
   *
   * @param hardTtlMs      the hard TTL duration in milliseconds
   * @param ttlJitterRatio the jitter ratio to apply (0.0–1.0)
   * @return absolute epoch-ms timestamp for hard expiry
   */
  public long toHardExpireTimestamp(long hardTtlMs, double ttlJitterRatio) {
    if (hardTtlMs == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    long jitter = DelayUtil.computeTtlJitter(hardTtlMs, ttlJitterRatio);

    return hardTtlMs > 0 ? TimeSource.currentTimeMillis() + Math.max(1, hardTtlMs + jitter) : Long.MAX_VALUE;
  }

  /**
   * Convert a soft TTL duration (ms) to an absolute epoch-ms expiration timestamp.
   * Applies configurable jitter (default ±5%) to prevent cache stampedes.
   * Returns 0 if the TTL is non-positive.
   *
   * @param softTtlMs the soft TTL duration in milliseconds
   * @return absolute epoch-ms timestamp for soft expiry, or 0 if TTL is non-positive
   */
  public long toSoftExpireTimestamp(long softTtlMs) {
    return toSoftExpireTimestamp(softTtlMs, defaultTtlJitterRatio);
  }

  /**
   * Convert a soft TTL duration (ms) to an absolute epoch-ms expiration timestamp
   * using the given jitter ratio instead of the configured default.
   * Returns 0 if the TTL is non-positive. Propagates {@link Long#MAX_VALUE} unchanged.
   *
   * @param softTtlMs      the soft TTL duration in milliseconds
   * @param ttlJitterRatio the jitter ratio to apply (0.0–1.0)
   * @return absolute epoch-ms timestamp for soft expiry, or 0 if TTL is non-positive
   */
  public long toSoftExpireTimestamp(long softTtlMs, double ttlJitterRatio) {
    if (softTtlMs <= 0) {
      return 0L;
    }
    if (softTtlMs == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    long jitter = DelayUtil.computeTtlJitter(softTtlMs, ttlJitterRatio);

    return TimeSource.currentTimeMillis() + Math.max(1, softTtlMs + jitter);
  }

  /**
   * Check whether the given key's soft TTL has expired.
   *
   * <p>{@code softExpireAtMs == 0} is the documented "disabled" value
   * ({@link #toSoftExpireTimestamp(long)} returns 0 for a non-positive soft
   * TTL): a disabled soft TTL never goes stale, so no stale-policy branch
   * (SOFT_REFRESH / REVALIDATE) may fire for such an entry — the former
   * {@code <= 0} reading armed a background refresh (reader + Redis version
   * probe) on <b>every</b> hit and never healed, the exact load storm the
   * "disabled" contract exists to prevent. {@code NullValue} sentinels carry
   * 0 as well; their short HARD TTL remains the penetration guard, so they
   * are unaffected.
   *
   * @return {@code true} if the entry's soft TTL is enabled and has expired,
   *         or the entry is absent
   */
  public boolean isSoftExpired(@Nullable Object cacheEntry) {
    if (cacheEntry instanceof CacheEntry ce) {
      long expireAt = ce.getSoftExpireAtMs();
      return expireAt > 0 && expireAt < TimeSource.currentTimeMillis();
    }
    return true;
  }

  /**
   * {@link EntryDraft.ExpiryArithmetic} adaptation for the draft API: the
   * {@code to*ExpireTimestamp} contract verbatim (hard {@code <= 0} =
   * permanent, {@code Long.MAX_VALUE} propagates).
   */
  @Override
  public long hardExpireAt(long hardTtlMs) {
    return toHardExpireTimestamp(hardTtlMs);
  }

  /**
   * {@link EntryDraft.ExpiryArithmetic} adaptation for the draft API: the
   * {@code to*ExpireTimestamp} contract verbatim (soft {@code <= 0} =
   * disabled / {@code 0}, {@code Long.MAX_VALUE} propagates).
   */
  @Override
  public long softExpireAt(long softTtlMs) {
    return toSoftExpireTimestamp(softTtlMs);
  }
}
