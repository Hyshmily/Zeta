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
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.springframework.util.Assert;

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
 * (&lt; 0.001% relative). {@code Long.MAX_VALUE} (permanent) is a dedicated
 * sentinel. Negative TTLs are rejected. The absolute expire-at timestamps
 * ({@code hardExpireAtMs}/{@code softExpireAtMs}) stay full {@code long}s and
 * are the actual expiry enforcement point — the stored TTLs are nominal values
 * used only when rebuilding an entry after a state transition.
 *
 * <p><b>Construction and modification.</b> The entry is immutable. Every
 * creation and every copy-on-write modification flows through the single
 * {@link EntryDraft} API: production code obtains drafts from
 * {@code ExpireManager.newEntry()} / {@code ExpireManager.editEntry(entry)}
 * (wired to the TTL arithmetic), tests and explicit-timestamp callers use
 * {@link EntryDraft#of(CacheEntry)}. The Lombok {@code @Builder} remains on the
 * package-private constructor for direct low-level construction (the generated
 * {@link #builder()} is the flat 13-field surface tests rely on); the former
 * {@code withXxx()} copy family, {@code toBuilder()}, and the
 * {@code TtlPolicy.applyXxx()} transforms are all replaced by the draft.
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
   *
   * <p>{@code @Getter(AccessLevel.NONE)} is deliberate: the packed {@code int}
   * must never be observable. Without it Lombok would fall back to generating
   * {@code int getHardTtlMs()} the moment the hand-written {@code long} getter
   * is renamed or removed — and because an {@code int} widens silently into a
   * {@code long}, callers would receive the packed encoding instead of a
   * duration with nothing failing to compile. The same guard is applied to the
   * other three packed TTL fields.
   */
  @Getter(AccessLevel.NONE)
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
   * {@link #getSoftTtlMs()}; {@code 0} means no soft expire. See
   * {@link #hardTtlMs} for why the packed field is not exposed as a getter.
   */
  @Getter(AccessLevel.NONE)
  @ToString.Exclude
  private final int softTtlMs;

  /**
   * Absolute epoch-millis timestamp at which the entry becomes stale
   * (soft expiry). Before this point the entry is considered fresh.
   */
  private final long softExpireAtMs;

  /**
   * Packed normal-state hard TTL baseline (2-bit unit + 30-bit mantissa).
   * Decoded via {@link #getNormalHardTtlMs()}. See {@link #hardTtlMs} for why
   * the packed field is not exposed as a getter.
   */
  @Getter(AccessLevel.NONE)
  @ToString.Exclude
  private final int normalHardTtlMs;

  /**
   * Packed normal-state soft TTL baseline (2-bit unit + 30-bit mantissa).
   * Decoded via {@link #getNormalSoftTtlMs()}. See {@link #hardTtlMs} for why
   * the packed field is not exposed as a getter.
   */
  @Getter(AccessLevel.NONE)
  @ToString.Exclude
  private final int normalSoftTtlMs;

  /**
   * Creates a new entry, packing {@code decisionEpoch} + {@code keyState}
   * into {@link #packedState} and the four TTLs into compact encodings.
   *
   * <p>Package-private: {@link EntryDraft} (same package) is the single
   * high-level construction and modification API and calls this constructor
   * directly — zero extra allocation. The Lombok {@code @Builder} keeps the
   * flat 13-field surface available to tests and low-level callers.
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
  CacheEntry(
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
    Assert.isTrue(
      isVersionDegraded == (dataVersion < 0),
      () -> "isVersionDegraded(" + isVersionDegraded + ") must equal (dataVersion < 0) for dataVersion=" + dataVersion
    );
    Assert.isTrue(
      decisionEpoch >= 0 && decisionEpoch <= MAX_DECISION_EPOCH,
      () -> "decisionEpoch out of range [0, " + MAX_DECISION_EPOCH + "]: " + decisionEpoch
    );
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
   * The Worker decision stamp carried by this entry, or {@code null} for a
   * local origin (no {@code decisionNodeId} — local promotion, cleared
   * demotion). The single extraction shape for decision metadata:
   * {@code ExpireManager.decisionOf} delegates here, and callers that pass
   * the result to {@link EntryDraft#decision} round-trip the fields exactly.
   *
   * @return the decision stamp, or {@code null} when the entry has no Worker origin
   */
  @Nullable
  public DecisionStamp decisionStamp() {
    return decisionNodeId == null ? null : new DecisionStamp(decisionVersion, decisionNodeId, getDecisionEpoch());
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
}
