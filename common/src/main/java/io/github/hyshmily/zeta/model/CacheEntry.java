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

import io.github.hyshmily.zeta.util.version.VersionGuard;
import jakarta.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * A value stored in the L1 cache together with its version metadata,
 * TTL information, and hot-key state.
 *
 * <p>Each {@code CacheEntry} carries two orthogonal version spaces (see ADR-0008):
 * <ul>
 *   <li><b>{@code dataVersion}</b> — monotonically increasing counter obtained
 *       from Redis INCR (normal) or the degraded Snowflake fallback (degraded,
 *       see ADR-0019). Used by
 *       the cache-sync send to resolve concurrent updates across instances.
 *       Degraded versions always live in the negative long space
 *       ({@code Long.MIN_VALUE | snowflakeId}), so {@link #isVersionDegraded()}
 *       is derived from the sign bit rather than stored separately.</li>
 *   <li><b>{@code decisionVersion}</b> — tracks Worker HOT/COOL decisions and is
 *       always monotonically increasing (never degraded). Orthogonal to
 *       {@code dataVersion}; used solely for ordering Worker decision broadcasts.</li>
 * </ul>
 *
 * <p>The normal-state TTLs ({@code normalHardTtlMs}, {@code normalSoftTtlMs}) are
 * recorded at entry creation and preserved across hot-key state transitions (HOT
 * extends TTL, COOL reverts to normal). This ensures the original expiry baseline
 * is never lost when the key's state changes.
 *
 * <p><b>Compact metadata.</b> The {@code decisionEpoch} (56 bits) and
 * {@code keyState} (2-bit code) are packed into a single {@code long}
 * ({@code packedState}). The four TTL durations are packed into single
 * {@code int}s as a 2-bit unit plus a 30-bit mantissa (see {@link #encodeTtl}):
 * TTLs up to 2^30 − 1 ms (≈ 12.4 days) round-trip exactly in milliseconds;
 * longer TTLs are stored in seconds, minutes, or hours with sub-unit error
 * (< 0.001% relative). {@code Long.MAX_VALUE} (permanent) is a dedicated
 * sentinel. Negative TTLs are rejected. The absolute expire-at timestamps
 * ({@code hardExpireAtMs}/{@code softExpireAtMs}) stay full {@code long}s and
 * are the actual expiry enforcement point — the stored TTLs are nominal values
 * used only when rebuilding an entry after a state transition.
 *
 * <p>Uses Lombok {@code @Builder} on the private constructor for initial
 * construction; the builder surface matches the logical fields
 * ({@code isVersionDegraded}, {@code decisionEpoch}, {@code keyState} are
 * accepted and packed). {@link #toBuilder()} is handwritten because the packed
 * fields no longer exist under their logical names. For modified copies, prefer
 * the {@code withXxx()} family of methods (e.g. {@link #withValue},
 * {@link #withTtl}) which allocate a single new instance directly — avoiding
 * the intermediate Builder object created by {@code toBuilder().field(v).build()}.
 */
@Getter
@ToString
@EqualsAndHashCode
public class CacheEntry {

  /** Number of bits reserved for the decision epoch within {@link #packedState}. */
  private static final int EPOCH_BITS = 56;
  /** Bit shift of the decision epoch within {@link #packedState} (above the 2-bit state code). */
  private static final int EPOCH_SHIFT = 2;
  /** Mask covering the epoch bits of {@link #packedState}. */
  private static final long EPOCH_MASK = ((1L << EPOCH_BITS) - 1) << EPOCH_SHIFT;
  /** Highest representable decision epoch (2^56 − 1 ≈ 7.2e16). */
  private static final long MAX_DECISION_EPOCH = (1L << EPOCH_BITS) - 1;
  /** Two-bit key-state code mask — the state sits in the low bits for zero-shift hot-path decode and at-a-glance debugging. */
  private static final int STATE_CODE_MASK = 0x3;
  /** Key-state codes — explicit, not enum ordinals (HOT/COOL/NORMAL order is not stable). */
  private static final int STATE_CODE_NULL = 0;
  private static final int STATE_CODE_NORMAL = 1;
  private static final int STATE_CODE_COOL = 2;
  private static final int STATE_CODE_HOT = 3;

  /** TTL encoding units (2-bit selector in the packed {@code int}). */
  private static final int TTL_UNIT_MS = 0;
  private static final int TTL_UNIT_SECOND = 1;
  private static final int TTL_UNIT_MINUTE = 2;
  private static final int TTL_UNIT_HOUR = 3;
  /** Number of mantissa bits in the packed TTL. */
  private static final int TTL_MANTISSA_BITS = 30;
  /** Number of unit-selector bits in the packed TTL. */
  private static final int TTL_UNIT_BITS = 2;
  /** Largest mantissa (2^30 − 1). */
  private static final long TTL_MANTISSA_MAX = (1L << TTL_MANTISSA_BITS) - 1;
  /** Milliseconds per unit, indexed by unit. */
  private static final long[] TTL_UNIT_FACTORS = { 1L, 1_000L, 60_000L, 3_600_000L };
  /**
   * Largest representable TTL below the infinite sentinel. The hour tier's
   * maximum mantissa (2^30 − 1 hours) would collide with the
   * {@link #TTL_ENCODE_INFINITE} encoding, so the cap is one millisecond below
   * it (≈ 122k years).
   */
  private static final long TTL_MAX_MS = TTL_MANTISSA_MAX * TTL_UNIT_FACTORS[TTL_UNIT_HOUR];
  /** Packed sentinel for {@code Long.MAX_VALUE} (permanent entry). */
  private static final int TTL_ENCODE_INFINITE = 0xFFFF_FFFF;

  /**
   * The cached value. May be {@code null} if the entry represents a tombstone
   * (invalidated) or a placeholder for a cache miss.
   */
  private final Object value;
  /**
   * Monotonically increasing version obtained from Redis INCR (normal path)
   * or the node-local Snowflake fallback (degraded path, negative space).
   * Used by the cache-sync send to resolve concurrent write conflicts across
   * instances.
   */
  private final long dataVersion;
  /**
   * Monotonically increasing version from Worker HOT/COOL decisions.
   * Never degraded (always originates from the Worker's {@code AtomicLong}).
   * Orthogonal to {@link #dataVersion} — used solely for ordering Worker
   * decision broadcasts (see ADR-0008).
   */
  private final long decisionVersion;
  /**
   * The Worker node ID that produced the {@link #decisionVersion}.
   * Used for per-Worker version partitioning in {@link VersionGuard#shouldSkipForWorker}.
   * May be {@code null} for entries created by local promotion (no Worker origin).
   */
  private final String decisionNodeId;

  /**
   * Packed decision metadata: bits [0, 2) hold the 2-bit key-state code
   * (low bits for zero-shift hot-path reads and at-a-glance debugging), bits
   * [2, 58) hold the decision epoch, bits [58, 64) are reserved.
   * Decoded via {@link #getDecisionEpoch()} and {@link #getKeyState()}.
   */
  @ToString.Exclude
  private final long packedState;

  /**
   * Packed hard TTL duration (2-bit unit + 30-bit mantissa). Decoded via
   * {@link #getHardTtlMs()}; the actual eviction timestamp is
   * {@link #hardExpireAtMs}.
   */
  @ToString.Exclude
  private final int hardTtlMs;

  /**
   * Absolute epoch-millis timestamp at which the entry should be evicted
   * (hard expiry). Compared against {@code System.currentTimeMillis()} on
   * each read.
   */
  private final long hardExpireAtMs;

  /**
   * Packed soft TTL duration (2-bit unit + 30-bit mantissa). Decoded via
   * {@link #getSoftTtlMs()}; {@code 0} means no soft expire.
   */
  @ToString.Exclude
  private final int softTtlMs;

  /**
   * Absolute epoch-millis timestamp at which the entry becomes stale
   * (soft expiry). Before this point the entry is considered fresh.
   */
  private final long softExpireAtMs;

  /**
   * Packed normal-state hard TTL baseline (2-bit unit + 30-bit mantissa).
   * Decoded via {@link #getNormalHardTtlMs()}.
   */
  @ToString.Exclude
  private final int normalHardTtlMs;

  /**
   * Packed normal-state soft TTL baseline (2-bit unit + 30-bit mantissa).
   * Decoded via {@link #getNormalSoftTtlMs()}.
   */
  @ToString.Exclude
  private final int normalSoftTtlMs;

  /**
   * Creates a new entry, packing {@code decisionEpoch} + {@code keyState}
   * into {@link #packedState} and the four TTLs into compact encodings.
   *
   * <p>The degraded flag is <b>not stored</b> — it is derived from the sign
   * bit of {@code dataVersion} (see ADR-0019). The constructor enforces the
   * invariant {@code isVersionDegraded == (dataVersion < 0)} and rejects
   * inconsistent arguments, so callers must pass versions from the correct
   * space (positive for Redis INCR, negative for the degraded fallback).
   *
   * @param value             the cached value
   * @param dataVersion       the data version; must be negative iff
   *                          {@code isVersionDegraded}
   * @param isVersionDegraded whether {@code dataVersion} is a degraded fallback
   * @param decisionVersion   the Worker decision version
   * @param decisionNodeId    the originating Worker node ID, may be null
   * @param decisionEpoch     the Worker epoch; must be in [0, 2^56 − 1]
   * @param hardTtlMs         the hard TTL duration in milliseconds
   * @param hardExpireAtMs    the absolute hard expiry timestamp
   * @param softTtlMs         the soft TTL duration in milliseconds
   * @param softExpireAtMs    the absolute soft expiry timestamp
   * @param keyState          the hot-key state, may be null
   * @param normalHardTtlMs   the normal-state hard TTL baseline
   * @param normalSoftTtlMs   the normal-state soft TTL baseline
   * @throws IllegalArgumentException if the degraded flag contradicts the sign
   *         of {@code dataVersion}, {@code decisionEpoch} exceeds 56 bits, or a
   *         TTL is negative or larger than the hour-tier maximum (except
   *         {@code Long.MAX_VALUE})
   */
  @Builder
  private CacheEntry(
    @Nullable Object value,
    long dataVersion,
    boolean isVersionDegraded,
    long decisionVersion,
    @Nullable String decisionNodeId,
    long decisionEpoch,
    long hardTtlMs,
    long hardExpireAtMs,
    long softTtlMs,
    long softExpireAtMs,
    @Nullable KeyState keyState,
    long normalHardTtlMs,
    long normalSoftTtlMs
  ) {
    if (isVersionDegraded != (dataVersion < 0)) {
      throw new IllegalArgumentException(
        "isVersionDegraded(" + isVersionDegraded + ") must equal (dataVersion < 0) for dataVersion=" + dataVersion
      );
    }
    if (decisionEpoch < 0 || decisionEpoch > MAX_DECISION_EPOCH) {
      throw new IllegalArgumentException(
        "decisionEpoch out of range [0, " + MAX_DECISION_EPOCH + "]: " + decisionEpoch
      );
    }
    this.value = value;
    this.dataVersion = dataVersion;
    this.decisionVersion = decisionVersion;
    this.decisionNodeId = decisionNodeId;
    this.packedState = (decisionEpoch << EPOCH_SHIFT) | stateCode(keyState);
    this.hardTtlMs = encodeTtl(hardTtlMs, "hardTtlMs");
    this.hardExpireAtMs = hardExpireAtMs;
    this.softTtlMs = encodeTtl(softTtlMs, "softTtlMs");
    this.softExpireAtMs = softExpireAtMs;
    this.normalHardTtlMs = encodeTtl(normalHardTtlMs, "normalHardTtlMs");
    this.normalSoftTtlMs = encodeTtl(normalSoftTtlMs, "normalSoftTtlMs");
  }

  private static int stateCode(@Nullable KeyState keyState) {
    if (keyState == null) {
      return STATE_CODE_NULL;
    }
    return switch (keyState) {
      case NORMAL -> STATE_CODE_NORMAL;
      case COOL -> STATE_CODE_COOL;
      case HOT -> STATE_CODE_HOT;
    };
  }

  private static KeyState stateFromCode(int stateCode) {
    return switch (stateCode) {
      case STATE_CODE_NORMAL -> KeyState.NORMAL;
      case STATE_CODE_COOL -> KeyState.COOL;
      case STATE_CODE_HOT -> KeyState.HOT;
      default -> null;
    };
  }

  /**
   * Packs a TTL duration into a 2-bit unit + 30-bit mantissa {@code int}.
   *
   * <p>Units are chosen so the mantissa fits: milliseconds up to 2^30 − 1 ms
   * (≈ 12.4 days) round-trip exactly, then seconds (up to 34 years), minutes,
   * and hours (up to ≈ 122k years). {@code Long.MAX_VALUE} (permanent) maps to
   * the {@link #TTL_ENCODE_INFINITE} sentinel. Negative values and values at
   * or above {@link #TTL_MAX_MS} — the hour-tier mantissa maximum would collide
   * with the sentinel encoding — are rejected.
   *
   * @param ttlMs    the TTL duration in milliseconds
   * @param fieldName the field name for the error message
   * @return the packed encoding
   * @throws IllegalArgumentException for negative or out-of-range values
   */
  private static int encodeTtl(long ttlMs, String fieldName) {
    if (ttlMs == Long.MAX_VALUE) {
      return TTL_ENCODE_INFINITE;
    }
    if (ttlMs < 0 || ttlMs >= TTL_MAX_MS) {
      throw new IllegalArgumentException(
        fieldName + " out of range [0, " + TTL_MAX_MS + ") or Long.MAX_VALUE: " + ttlMs
      );
    }

    int unit = TTL_UNIT_MS;
    long scaled = ttlMs;

    if (scaled > TTL_MANTISSA_MAX) {
      scaled /= TTL_UNIT_FACTORS[TTL_UNIT_SECOND];
      unit = TTL_UNIT_SECOND;
    }

    if (scaled > TTL_MANTISSA_MAX) {
      scaled /= 60;
      unit = TTL_UNIT_MINUTE;
    }

    if (scaled > TTL_MANTISSA_MAX) {
      scaled /= 60;
      unit = TTL_UNIT_HOUR;
    }
    return (int) ((scaled << TTL_UNIT_BITS) | unit);
  }

  /**
   * Decodes a TTL packed by {@link #encodeTtl} back to milliseconds.
   *
   * @param encoded the packed TTL
   * @return the TTL duration in milliseconds
   */
  private static long decodeTtl(int encoded) {
    if (encoded == TTL_ENCODE_INFINITE) {
      return Long.MAX_VALUE;
    }
    return (encoded >>> 2) * TTL_UNIT_FACTORS[encoded & 0x3];
  }

  /**
   * Whether {@link #getDataVersion()} was obtained from the local fallback
   * (node-local Snowflake) instead of Redis.
   *
   * <p>Derived from the sign bit rather than stored: degraded versions always
   * live in the negative long space ({@code Long.MIN_VALUE | snowflakeId},
   * ADR-0019), while Redis INCR can never wrap into the negative space within
   * the 7-day version-key TTL (ADR-0022).
   *
   * @return {@code true} iff {@code dataVersion < 0}
   */
  @ToString.Include
  public boolean isVersionDegraded() {
    return dataVersion < 0;
  }

  /**
   * The epoch (restart counter) of the Worker that produced the {@link #decisionVersion}.
   * A higher epoch indicates a Worker restart; {@link VersionGuard} unconditionally
   * accepts decisions from a higher epoch (see ADR-0010).
   * May be {@code 0} for entries created by local promotion.
   *
   * @return the packed decision epoch, decoded from {@link #packedState}
   */
  @ToString.Include(name = "decisionEpoch")
  public long getDecisionEpoch() {
    return (packedState & EPOCH_MASK) >>> EPOCH_SHIFT;
  }

  /**
   * Current hot-key state of this entry. Determines which TTL values
   * are active: {@link KeyState#HOT} uses extended TTLs,
   * {@link KeyState#COOL} reverts to normal TTLs.
   *
   * @return the key state, or {@code null} if not set
   */
  @ToString.Include(name = "keyState")
  public KeyState getKeyState() {
    return stateFromCode((int) (packedState & STATE_CODE_MASK));
  }

  /**
   * Hard TTL duration in milliseconds for this entry. The entry is evicted
   * unconditionally when {@link #hardExpireAtMs} is reached, regardless of
   * access patterns.
   *
   * <p>Decoded from the packed storage: values up to 2^30 − 1 ms (≈ 12.4 days)
   * round-trip exactly; longer values are second/minute/hour-aligned with a
   * sub-unit error. {@code Long.MAX_VALUE} denotes a permanent entry.
   *
   * @return the hard TTL in milliseconds
   */
  @ToString.Include(name = "hardTtlMs")
  public long getHardTtlMs() {
    return decodeTtl(hardTtlMs);
  }

  /**
   * Soft TTL duration in milliseconds for stale-while-revalidate behaviour.
   * After {@link #softExpireAtMs} the entry is considered stale; reads may
   * still return the stale value while a background refresh is triggered.
   * {@code 0} means no soft expire.
   *
   * @return the soft TTL in milliseconds, or {@code 0} for none
   */
  @ToString.Include(name = "softTtlMs")
  public long getSoftTtlMs() {
    return decodeTtl(softTtlMs);
  }

  /**
   * Normal-state hard TTL recorded at initial entry creation. Preserved
   * across HOT/COOL state transitions so the original hard expiry baseline
   * is always recoverable when the key returns to NORMAL state.
   *
   * @return the normal hard TTL in milliseconds
   */
  @ToString.Include(name = "normalHardTtlMs")
  public long getNormalHardTtlMs() {
    return decodeTtl(normalHardTtlMs);
  }

  /**
   * Normal-state soft TTL recorded at initial entry creation. Preserved
   * across HOT/COOL state transitions so the original soft expiry baseline
   * is always recoverable when the key returns to NORMAL state.
   *
   * @return the normal soft TTL in milliseconds
   */
  @ToString.Include(name = "normalSoftTtlMs")
  public long getNormalSoftTtlMs() {
    return decodeTtl(normalSoftTtlMs);
  }

  /** Return a copy with a different {@link #value}. */
  public CacheEntry withValue(@Nullable Object value) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded(),
      decisionVersion,
      decisionNodeId,
      getDecisionEpoch(),
      getHardTtlMs(),
      hardExpireAtMs,
      getSoftTtlMs(),
      softExpireAtMs,
      getKeyState(),
      getNormalHardTtlMs(),
      getNormalSoftTtlMs()
    );
  }

  /** Return a copy with a different {@link #dataVersion}. The degraded flag follows the new sign. */
  public CacheEntry withDataVersion(long dataVersion) {
    return new CacheEntry(
      value,
      dataVersion,
      dataVersion < 0,
      decisionVersion,
      decisionNodeId,
      getDecisionEpoch(),
      getHardTtlMs(),
      hardExpireAtMs,
      getSoftTtlMs(),
      softExpireAtMs,
      getKeyState(),
      getNormalHardTtlMs(),
      getNormalSoftTtlMs()
    );
  }

  /** Return a copy with a different {@link #decisionVersion}. */
  public CacheEntry withDecisionVersion(long decisionVersion) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded(),
      decisionVersion,
      decisionNodeId,
      getDecisionEpoch(),
      getHardTtlMs(),
      hardExpireAtMs,
      getSoftTtlMs(),
      softExpireAtMs,
      getKeyState(),
      getNormalHardTtlMs(),
      getNormalSoftTtlMs()
    );
  }

  /** Return a copy with a different {@link #decisionNodeId}. */
  public CacheEntry withDecisionNodeId(@Nullable String decisionNodeId) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded(),
      decisionVersion,
      decisionNodeId,
      getDecisionEpoch(),
      getHardTtlMs(),
      hardExpireAtMs,
      getSoftTtlMs(),
      softExpireAtMs,
      getKeyState(),
      getNormalHardTtlMs(),
      getNormalSoftTtlMs()
    );
  }

  /** Return a copy with a different {@link #getDecisionEpoch decision epoch}. */
  public CacheEntry withDecisionEpoch(long decisionEpoch) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded(),
      decisionVersion,
      decisionNodeId,
      decisionEpoch,
      getHardTtlMs(),
      hardExpireAtMs,
      getSoftTtlMs(),
      softExpireAtMs,
      getKeyState(),
      getNormalHardTtlMs(),
      getNormalSoftTtlMs()
    );
  }

  /** Return a copy with a different {@link #getHardTtlMs() hard TTL}. */
  public CacheEntry withHardTtlMs(long hardTtlMs) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded(),
      decisionVersion,
      decisionNodeId,
      getDecisionEpoch(),
      hardTtlMs,
      hardExpireAtMs,
      getSoftTtlMs(),
      softExpireAtMs,
      getKeyState(),
      getNormalHardTtlMs(),
      getNormalSoftTtlMs()
    );
  }

  /** Return a copy with a different {@link #hardExpireAtMs}. */
  public CacheEntry withHardExpireAtMs(long hardExpireAtMs) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded(),
      decisionVersion,
      decisionNodeId,
      getDecisionEpoch(),
      getHardTtlMs(),
      hardExpireAtMs,
      getSoftTtlMs(),
      softExpireAtMs,
      getKeyState(),
      getNormalHardTtlMs(),
      getNormalSoftTtlMs()
    );
  }

  /** Return a copy with a different {@link #getSoftTtlMs() soft TTL}. */
  public CacheEntry withSoftTtlMs(long softTtlMs) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded(),
      decisionVersion,
      decisionNodeId,
      getDecisionEpoch(),
      getHardTtlMs(),
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      getKeyState(),
      getNormalHardTtlMs(),
      getNormalSoftTtlMs()
    );
  }

  /** Return a copy with a different {@link #softExpireAtMs}. */
  public CacheEntry withSoftExpireAtMs(long softExpireAtMs) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded(),
      decisionVersion,
      decisionNodeId,
      getDecisionEpoch(),
      getHardTtlMs(),
      hardExpireAtMs,
      getSoftTtlMs(),
      softExpireAtMs,
      getKeyState(),
      getNormalHardTtlMs(),
      getNormalSoftTtlMs()
    );
  }

  /** Return a copy with a different {@link #getKeyState() key state}. */
  public CacheEntry withKeyState(KeyState keyState) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded(),
      decisionVersion,
      decisionNodeId,
      getDecisionEpoch(),
      getHardTtlMs(),
      hardExpireAtMs,
      getSoftTtlMs(),
      softExpireAtMs,
      keyState,
      getNormalHardTtlMs(),
      getNormalSoftTtlMs()
    );
  }

  /** Return a copy with a different {@link #getNormalHardTtlMs() normal hard TTL}. */
  public CacheEntry withNormalHardTtlMs(long normalHardTtlMs) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded(),
      decisionVersion,
      decisionNodeId,
      getDecisionEpoch(),
      getHardTtlMs(),
      hardExpireAtMs,
      getSoftTtlMs(),
      softExpireAtMs,
      getKeyState(),
      normalHardTtlMs,
      getNormalSoftTtlMs()
    );
  }

  /** Return a copy with a different {@link #getNormalSoftTtlMs() normal soft TTL}. */
  public CacheEntry withNormalSoftTtlMs(long normalSoftTtlMs) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded(),
      decisionVersion,
      decisionNodeId,
      getDecisionEpoch(),
      getHardTtlMs(),
      hardExpireAtMs,
      getSoftTtlMs(),
      softExpireAtMs,
      getKeyState(),
      getNormalHardTtlMs(),
      normalSoftTtlMs
    );
  }

  /** Return a copy with all four TTL fields updated at once. */
  public CacheEntry withTtl(long hardTtlMs, long softTtlMs, long hardExpireAtMs, long softExpireAtMs) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded(),
      decisionVersion,
      decisionNodeId,
      getDecisionEpoch(),
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      getKeyState(),
      getNormalHardTtlMs(),
      getNormalSoftTtlMs()
    );
  }

  /** Return a copy with hard TTL and hard expire-at updated together. */
  public CacheEntry withHardTtl(long hardTtlMs, long hardExpireAtMs) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded(),
      decisionVersion,
      decisionNodeId,
      getDecisionEpoch(),
      hardTtlMs,
      hardExpireAtMs,
      getSoftTtlMs(),
      softExpireAtMs,
      getKeyState(),
      getNormalHardTtlMs(),
      getNormalSoftTtlMs()
    );
  }

  /** Return a copy with soft TTL and soft expire-at updated together. */
  public CacheEntry withSoftTtl(long softTtlMs, long softExpireAtMs) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded(),
      decisionVersion,
      decisionNodeId,
      getDecisionEpoch(),
      getHardTtlMs(),
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      getKeyState(),
      getNormalHardTtlMs(),
      getNormalSoftTtlMs()
    );
  }

  /** Return a copy with both normal TTL fields updated together. */
  public CacheEntry withNormalTtl(long normalHardTtlMs, long normalSoftTtlMs) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded(),
      decisionVersion,
      decisionNodeId,
      getDecisionEpoch(),
      getHardTtlMs(),
      hardExpireAtMs,
      getSoftTtlMs(),
      softExpireAtMs,
      getKeyState(),
      normalHardTtlMs,
      normalSoftTtlMs
    );
  }

  /** Return a copy with all four TTL fields and keyState updated at once. */
  public CacheEntry withTtlAndKeyState(
    long hardTtlMs,
    long softTtlMs,
    long hardExpireAtMs,
    long softExpireAtMs,
    KeyState keyState
  ) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded(),
      decisionVersion,
      decisionNodeId,
      getDecisionEpoch(),
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      keyState,
      getNormalHardTtlMs(),
      getNormalSoftTtlMs()
    );
  }

  /**
   * Return a copy with decision metadata, TTL fields, and keyState updated
   * at once — the Worker COOL-decision pattern.
   */
  public CacheEntry withDecisionAndTtlAndState(
    long decisionVersion,
    String decisionNodeId,
    long decisionEpoch,
    long hardTtlMs,
    long softTtlMs,
    long hardExpireAtMs,
    long softExpireAtMs,
    KeyState keyState
  ) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded(),
      decisionVersion,
      decisionNodeId,
      decisionEpoch,
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      keyState,
      getNormalHardTtlMs(),
      getNormalSoftTtlMs()
    );
  }

  /**
   * Return a copy with value, version metadata, and expire-at timestamps
   * updated at once — the cache-sync refresh pattern.
   */
  public CacheEntry withValueAndRefreshMeta(
    Object value,
    long dataVersion,
    boolean isVersionDegraded,
    long hardExpireAtMs,
    long softExpireAtMs
  ) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded,
      decisionVersion,
      decisionNodeId,
      getDecisionEpoch(),
      getHardTtlMs(),
      hardExpireAtMs,
      getSoftTtlMs(),
      softExpireAtMs,
      getKeyState(),
      getNormalHardTtlMs(),
      getNormalSoftTtlMs()
    );
  }

  /** Return a copy with value and soft TTL fields updated at once — the refresh-task pattern. */
  public CacheEntry withValueAndSoftTtl(Object value, long softTtlMs, long softExpireAtMs) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded(),
      decisionVersion,
      decisionNodeId,
      getDecisionEpoch(),
      getHardTtlMs(),
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      getKeyState(),
      getNormalHardTtlMs(),
      getNormalSoftTtlMs()
    );
  }

  /**
   * Copy builder seeded from this entry.
   *
   * <p>Written by hand because Lombok's generated {@code toBuilder()} reads
   * constructor parameters by matching field names, but {@code decisionEpoch},
   * {@code keyState}, and the four TTLs no longer exist as fields — the
   * derived/decode getters feed the builder instead.
   *
   * @return a builder prefilled with this entry's logical fields
   */
  public CacheEntryBuilder toBuilder() {
    return new CacheEntryBuilder()
      .value(value)
      .dataVersion(dataVersion)
      .isVersionDegraded(isVersionDegraded())
      .decisionVersion(decisionVersion)
      .decisionNodeId(decisionNodeId)
      .decisionEpoch(getDecisionEpoch())
      .hardTtlMs(getHardTtlMs())
      .hardExpireAtMs(hardExpireAtMs)
      .softTtlMs(getSoftTtlMs())
      .softExpireAtMs(softExpireAtMs)
      .keyState(getKeyState())
      .normalHardTtlMs(getNormalHardTtlMs())
      .normalSoftTtlMs(getNormalSoftTtlMs());
  }
}
