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
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import java.util.concurrent.ThreadLocalRandom;
import org.jspecify.annotations.Nullable;

/**
 * Pure TTL and expiry policy for {@link CacheEntry}
 * lifecycle arithmetic.
 *
 * <p>
 * The stateless half of the entry lifecycle: every TTL computation — resolve
 * (override vs default), effective (configured values), timestamp conversion to
 * absolute expire instants with jitter, and expiry predicates — lives here
 * behind one small interface. {@link EntryLifecycle} keeps the stateful parts
 * (tombstone and clear watermarks, Decision-Validity demotion, entry factory)
 * and exposes this policy via {@code ttlPolicy()}.
 *
 * <p>
 * The class also implements {@link EntryDraft.ExpiryArithmetic}: the draft
 * API ({@code EntryLifecycle.newEntry/editEntry}) delegates its computed
 * expire timestamps to these {@code hardExpireAt}/{@code softExpireAt}
 * conversions — the single duration-to-timestamp rule for the whole entry
 * pipeline. {@link EntryDraft#ttl}/{@link EntryDraft#rearmExpiry} apply the
 * result.
 *
 * <p>
 * Instances are cheap and hold no mutable state of their own: the
 * underlying {@link ZetaProperties} is read on every call, so runtime
 * configuration updates keep working. All methods are thread-safe and
 * side-effect free.
 */
@Internal
public final class TtlPolicy implements EntryDraft.ExpiryArithmetic {

  /** Fallback hard TTL for a COOL entry whose normal baseline is unknown. */
  private static final long COOL_DEFAULT_PROTECTION_HARD_TTL_MS = 120_000L;
  /** Jitter ratio for the COOL protection window. */
  private static final double COOL_PROTECTION_JITTER_RATIO = 0.2;

  /** TTL configuration providing normal and hot-key TTL values. */
  private final ZetaProperties ttlConfig;

  /**
   * Create a TTL policy backed by the given configuration.
   *
   * @param ttlConfig TTL configuration (normal and hot-key variants)
   */
  public TtlPolicy(ZetaProperties ttlConfig) {
    this.ttlConfig = ttlConfig;
  }

  /**
   * Check whether a {@link CacheEntry} has logically expired based on its
   * {@code hardExpireAtMs}. Entries with {@code hardExpireAtMs == Long.MAX_VALUE}
   * are treated as permanent (never logically expire).
   *
   * @param entry the cache entry to inspect
   * @return {@code true} if the entry has logically expired
   */
  public boolean isLogicallyExpired(CacheEntry entry) {
    return isLogicallyExpired(entry, TimeSource.currentTimeMillis());
  }

  /**
   * Timestamp-threading form of {@link #isLogicallyExpired(CacheEntry)}: judges
   * expiry against a caller-supplied {@code now} so a single read path can pay
   * one clock read for several predicates. The judgement is identical to the
   * clock-reading form whenever {@code now} was sampled on the same call.
   *
   * @param entry the cache entry to inspect
   * @param now   the current epoch-millis (a single per-call sample)
   * @return {@code true} if the entry has logically expired as of {@code now}
   */
  public boolean isLogicallyExpired(CacheEntry entry, long now) {
    return entry.getHardExpireAtMs() != Long.MAX_VALUE && now >= entry.getHardExpireAtMs();
  }

  /**
   * Effective hard TTL for normal keys as configured. {@code 0} means
   * permanent (no hard expiry).
   *
   * @return effective hard TTL duration in milliseconds
   */
  public long effectiveHardTtlMs() {
    return ttlConfig.getHardTtlMs();
  }

  /**
   * Resolve effective hard TTL for normal keys: use the per-call value if
   * positive, otherwise fall back to the configured TTL.
   *
   * @param hardTtlMs hard TTL override ({@code 0} or negative uses the configured
   *                  TTL)
   * @return effective hard TTL duration in milliseconds
   */
  public long resolveEffectiveHardTtl(long hardTtlMs) {
    return hardTtlMs > 0 ? hardTtlMs : effectiveHardTtlMs();
  }

  /**
   * Effective hard TTL for hot keys as configured. {@code 0} means permanent
   * (no hard expiry).
   *
   * @return effective hot hard TTL duration in milliseconds
   */
  public long effectiveHotHardTtlMs() {
    return ttlConfig.getHotHardTtlMs();
  }

  /**
   * Resolve effective hard TTL for hot keys from the entry's resolved normal hard TTL. The hot
   * default is the floor — a positive value may only raise it ({@code max(normal, hotDefault)}) —
   * so promotion to HOT never shortens an entry's lifetime. {@code 0} on either side means
   * permanent and wins: a permanent normal entry stays permanent, and a permanent hot policy can
   * never turn a finite normal entry into a bounded one.
   *
   * <p>This method is called with an already-resolved value: {@code 0} here is the permanent
   * sentinel, not "use the configured default". The raw override boundary resolves that sentinel
   * through {@link #resolveEffectiveHardTtl(long)} before the entry layer sees it.
   *
   * @param normalHardTtlMs the resolved normal hard TTL ({@code 0} = permanent)
   * @return effective hot-key hard TTL duration in milliseconds ({@code 0} = permanent)
   */
  public long resolveEffectiveHotHard(long normalHardTtlMs) {
    long hotDefault = effectiveHotHardTtlMs();
    if (normalHardTtlMs <= 0L || hotDefault == 0L) {
      return 0L;
    }
    return Math.max(normalHardTtlMs, hotDefault);
  }

  /**
   * Effective soft TTL for normal keys as configured. {@code 0} disables
   * soft expiry.
   *
   * @return effective soft TTL duration in milliseconds
   */
  public long effectiveSoftTtlMs() {
    return ttlConfig.getSoftTtlMs();
  }

  /**
   * Resolve effective soft TTL for normal keys: use the per-call value if
   * positive, otherwise fall back to the configured TTL.
   *
   * @param softTtlMs soft TTL override ({@code 0} or negative uses the configured
   *                  TTL)
   * @return effective soft TTL duration in milliseconds
   */
  public long resolveEffectiveSoftTtl(long softTtlMs) {
    return softTtlMs > 0 ? softTtlMs : effectiveSoftTtlMs();
  }

  /**
   * Effective soft TTL for hot keys as configured. {@code 0} disables soft
   * expiry.
   *
   * @return effective hot soft TTL duration in milliseconds
   */
  public long effectiveHotSoftTtlMs() {
    return ttlConfig.getHotSoftTtlMs();
  }

  /**
   * Resolve effective soft TTL for hot keys from the entry's resolved normal soft TTL. The hot
   * default is the floor — a positive value may only raise it ({@code max(normal, hotDefault)}) —
   * so promotion to HOT never shortens an entry's soft window. A hot default of {@code 0}
   * disables soft expiry for hot entries and is not overridden by a normal soft TTL.
   *
   * <p>This method is called with an already-resolved value: {@code 0} here is the disabled
   * sentinel, not "use the configured default". The raw override boundary resolves that sentinel
   * through {@link #resolveEffectiveSoftTtl(long)} before the entry layer sees it.
   *
   * @param normalSoftTtlMs the resolved normal soft TTL ({@code 0} = disabled)
   * @return effective hot-key soft TTL duration in milliseconds ({@code 0} = disabled)
   */
  public long resolveEffectiveHotSoft(long normalSoftTtlMs) {
    long hotDefault = effectiveHotSoftTtlMs();
    if (hotDefault == 0L) {
      return 0L;
    }
    return normalSoftTtlMs > 0L ? Math.max(normalSoftTtlMs, hotDefault) : hotDefault;
  }

  /**
   * Effective soft TTL for a background refresh: the per-call override wins,
   * otherwise a HOT entry keeps the hot cadence and a NORMAL/COOL entry reuses
   * its own stored normal soft TTL or the configured default.
   *
   * @param entry             the entry being refreshed
   * @param softTtlOverrideMs per-call soft TTL override ({@code 0} = derive)
   * @return the effective soft TTL in milliseconds, {@code 0} when disabled
   */
  public long resolveRefreshSoftTtl(CacheEntry entry, long softTtlOverrideMs) {
    return softTtlOverrideMs > 0
      ? softTtlOverrideMs
      : (entry.getKeyState() == KeyState.HOT
          ? effectiveHotSoftTtlMs()
          : (entry.getNormalSoftTtlMs() > 0 ? entry.getNormalSoftTtlMs() : effectiveSoftTtlMs()));
  }

  /**
   * Normal-state hard baseline stored on an entry. {@code 0} is the
   * draft-level "permanent" value at the boundaries that resolve it; the entry
   * stores the explicit {@link Long#MAX_VALUE} sentinel so a later
   * COOL/demotion cannot mistake it for an unknown baseline and fall back to
   * the protection floor.
   *
   * @param hardTtlMs the resolved normal hard TTL ({@code 0} = permanent)
   * @return the baseline to store
   */
  public long normalHardBaseline(long hardTtlMs) {
    return hardTtlMs == 0L ? Long.MAX_VALUE : hardTtlMs;
  }

  /**
   * Hard TTL for a COOL rewrite: the entry's own normal baseline when it has
   * one, otherwise the protection floor. COOL starts a fresh protection window
   * rather than preserving the remaining normal lifetime.
   *
   * @param entry the entry being cooled
   * @return the COOL hard TTL in milliseconds
   */
  public long coolHardTtlMs(CacheEntry entry) {
    long normal = entry.getNormalHardTtlMs();
    return normal > 0 ? normal : COOL_DEFAULT_PROTECTION_HARD_TTL_MS;
  }

  /**
   * Soft TTL for a COOL rewrite: the entry's own normal baseline. A baseline
   * of {@code 0} means soft expiry is disabled and stays disabled through the
   * COOL rewrite.
   *
   * @param entry the entry being cooled
   * @return the COOL soft TTL in milliseconds, or {@code 0} when disabled
   */
  public long coolSoftTtlMs(CacheEntry entry) {
    return Math.max(0L, entry.getNormalSoftTtlMs());
  }

  /**
   * Absolute hard deadline for a COOL rewrite, with the COOL jitter ratio.
   *
   * @param coolHardTtlMs the COOL hard TTL already resolved from the entry
   * @return the absolute epoch-millis hard deadline
   */
  public long coolHardExpireAt(long coolHardTtlMs) {
    return hardExpireAt(coolHardTtlMs, COOL_PROTECTION_JITTER_RATIO);
  }

  /**
   * Absolute soft deadline for a COOL rewrite, with the COOL jitter ratio.
   *
   * @param coolSoftTtlMs the COOL soft TTL already resolved from the entry
   * @return the absolute epoch-millis soft deadline
   */
  public long coolSoftExpireAt(long coolSoftTtlMs) {
    return softExpireAt(coolSoftTtlMs, COOL_PROTECTION_JITTER_RATIO);
  }

  /**
   * Convert a TTL duration (ms) to an absolute epoch-ms expiration timestamp using the configured
   * default jitter ratio. Propagates {@link Long#MAX_VALUE} unchanged — used to signal permanent
   * entries (pure logical expiry with no hard TTL eviction).
   *
   * <p>This is the {@link EntryDraft.ExpiryArithmetic} adaptation for the draft API: hard
   * {@code <= 0} = permanent, {@code Long.MAX_VALUE} propagates.
   */
  @Override
  public long hardExpireAt(long hardTtlMs) {
    return hardExpireAt(hardTtlMs, ttlConfig.getTtlJitterRatio());
  }

  /**
   * Convert a TTL duration (ms) to an absolute epoch-ms expiration timestamp using the given jitter
   * ratio instead of the configured default. Propagates {@link Long#MAX_VALUE} unchanged.
   *
   * @param hardTtlMs      the hard TTL duration in milliseconds
   * @param ttlJitterRatio the jitter ratio to apply (0.0–1.0)
   * @return absolute epoch-ms timestamp for hard expiry
   */
  public long hardExpireAt(long hardTtlMs, double ttlJitterRatio) {
    if (hardTtlMs == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    return hardTtlMs > 0 ? expireAtFromNow(hardTtlMs, computeTtlJitter(hardTtlMs, ttlJitterRatio)) : Long.MAX_VALUE;
  }

  /**
   * Convert a soft TTL duration (ms) to an absolute epoch-ms expiration timestamp. Applies
   * configurable jitter (default ±5%) to prevent cache stampedes. Returns 0 if the TTL is
   * non-positive.
   *
   * <p>This is the {@link EntryDraft.ExpiryArithmetic} adaptation for the draft API: soft
   * {@code <= 0} = disabled / {@code 0}, {@code Long.MAX_VALUE} propagates.
   *
   * @param softTtlMs the soft TTL duration in milliseconds
   * @return absolute epoch-ms timestamp for soft expiry, or 0 if TTL is non-positive
   */
  @Override
  public long softExpireAt(long softTtlMs) {
    return softExpireAt(softTtlMs, ttlConfig.getTtlJitterRatio());
  }

  /**
   * Convert a soft TTL duration (ms) to an absolute epoch-ms expiration timestamp using the given
   * jitter ratio instead of the configured default. Returns 0 if the TTL is non-positive.
   * Propagates {@link Long#MAX_VALUE} unchanged.
   *
   * @param softTtlMs      the soft TTL duration in milliseconds
   * @param ttlJitterRatio the jitter ratio to apply (0.0–1.0)
   * @return absolute epoch-ms timestamp for soft expiry, or 0 if TTL is non-positive
   */
  public long softExpireAt(long softTtlMs, double ttlJitterRatio) {
    if (softTtlMs <= 0) {
      return 0L;
    }
    if (softTtlMs == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    return expireAtFromNow(softTtlMs, computeTtlJitter(softTtlMs, ttlJitterRatio));
  }

  /**
   * {@code now + max(1, ttl + jitter)}, saturating instead of wrapping. A TTL
   * near
   * {@link Long#MAX_VALUE} (a caller-supplied override of "effectively forever")
   * must
   * yield a far-future deadline: the plain addition wrapped negative, and every
   * expiry
   * predicate reads a negative deadline as "expired since forever", so the entry
   * died
   * on the write that created it.
   *
   * @param ttlMs    the resolved TTL duration (positive)
   * @param jitterMs the signed jitter offset to apply
   * @return the absolute epoch-ms deadline, never earlier than {@code now + 1}
   */
  private static long expireAtFromNow(long ttlMs, long jitterMs) {
    long delta = jitterMs > 0L && ttlMs > Long.MAX_VALUE - jitterMs ? Long.MAX_VALUE : Math.max(1L, ttlMs + jitterMs);
    long now = TimeSource.currentTimeMillis();
    return delta == Long.MAX_VALUE || now > Long.MAX_VALUE - delta ? Long.MAX_VALUE : now + delta;
  }

  /**
   * Check whether the given key's soft TTL has expired.
   *
   * <p>
   * {@code softExpireAtMs == 0} is the documented "disabled" value
   * ({@link #softExpireAt(long)} returns 0 for a non-positive soft
   * TTL): a disabled soft TTL never goes stale, so no stale-policy branch
   * (SOFT_REFRESH / REVALIDATE) may fire for such an entry. Treating {@code <= 0}
   * as expired would arm a background refresh (reader + version stamp) on
   * <b>every</b> hit and never heal — the exact load storm the "disabled"
   * contract exists to prevent.
   *
   * @return {@code true} if the entry's soft TTL is enabled and has expired,
   *         or the entry is absent
   */
  public boolean isSoftExpired(@Nullable CacheEntry cacheEntry) {
    return isSoftExpired(cacheEntry, TimeSource.currentTimeMillis());
  }

  /**
   * Timestamp-threading form of {@link #isSoftExpired(CacheEntry)}: judges
   * soft expiry against a caller-supplied {@code now} so a single read path
   * can pay one clock read for several predicates (see
   * {@link #isLogicallyExpired(CacheEntry, long)}).
   *
   * @param cacheEntry the cache entry to inspect (may be {@code null})
   * @param now        the current epoch-millis (a single per-call sample)
   * @return {@code true} if soft TTL is enabled and expired as of
   *         {@code now}, or the entry is absent
   */
  public boolean isSoftExpired(@Nullable CacheEntry cacheEntry, long now) {
    if (cacheEntry != null) {
      long expireAt = cacheEntry.getSoftExpireAtMs();
      // Same inclusive boundary as isLogicallyExpired: at the deadline millisecond an
      // entry is expired, whichever of the two deadlines it is.
      return expireAt > 0 && expireAt <= now;
    }
    return true;
  }

  /**
   * Uniform random jitter in
   * {@code [-(long) (ttlMs * ratio), (long) (ttlMs * ratio)]} milliseconds — a
   * symmetric closed range, so the mean offset is zero; a half-open range would
   * bias every expiry earlier and give no positive offset for a 1 ms base.
   * Returns 0 for non-positive inputs or a sub-millisecond base jitter; clamps
   * the base so neither {@code 2 * base} nor {@code base + 1} can overflow.
   */
  private static long computeTtlJitter(long ttlMs, double ttlJitterRatio) {
    if (ttlMs <= 0 || ttlJitterRatio <= 0) {
      return 0;
    }

    long baseJitter = (long) (ttlMs * ttlJitterRatio);
    if (baseJitter <= 0) {
      return 0;
    }

    if (baseJitter > Long.MAX_VALUE / 2) {
      baseJitter = Long.MAX_VALUE / 2;
    }

    return ThreadLocalRandom.current().nextLong(-baseJitter, baseJitter + 1);
  }
}
