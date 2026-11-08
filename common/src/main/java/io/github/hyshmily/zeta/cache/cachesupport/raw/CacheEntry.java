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

import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.jspecify.annotations.Nullable;

/**
 * A value stored in the L1 cache together with its version metadata,
 * TTL information, and hot-key state.
 *
 * <p>Each {@code CacheEntry} carries two orthogonal version spaces (see design decisions: version stamps):
 * <ul>
 *   <li><b>{@code dataVersion}</b> — a positive stamp from the node-local
 *       {@code StampClock}, used by the cache-sync send to resolve concurrent
 *       updates across instances. {@code dataVersion > 0} is a real stamp;
 *       {@code 0} means unknown and makes the weakest possible claim: an
 *       unknown entry accepts any real stamp.</li>
 *   <li><b>{@code decisionVersion}</b> — tracks Worker HOT/COOL decisions and is
 *       always monotonically increasing. Orthogonal to {@code dataVersion};
 *       used solely for ordering Worker decision broadcasts.</li>
 * </ul>
 *
 * <p>The normal-state TTLs ({@code normalHardTtlMs}, {@code normalSoftTtlMs}) are
 * recorded at entry creation and preserved across hot-key state transitions (HOT
 * extends TTL, COOL reverts to normal). This ensures the original expiry baseline
 * is never lost when the key's state changes.
 *
 * <p><b>Compact metadata.</b> The nullable {@link DecisionStamp} is one reference
 * for the whole Worker decision; a local entry carries {@code null}. The key-state
 * code, the value-birth second and the hard TTL
 * duration share one {@code long} ({@code packedMeta}): state in the low two
 * bits, birth in the next 30 bits (≈ 34 years of monotonic uptime before the
 * lease-staleness bound would see a wrapped stamp), hard TTL in the high 32
 * bits (see {@link #encodeTtl}). The remaining TTL durations are packed into
 * single {@code int}s as a unit bit plus a 31-bit magnitude: TTLs up to
 * 2^31 − 1 ms (≈ 24.8 days) round-trip exactly in milliseconds; longer TTLs are
 * stored in whole seconds, rounded up, with sub-second error (at most 1 s,
 * &lt; 0.001% relative for any TTL above a day). {@code Long.MAX_VALUE}
 * (permanent) is a dedicated sentinel. Negative TTLs are rejected. The absolute
 * expire-at timestamps ({@code hardExpireAtMs}/{@code softExpireAtMs}) stay full
 * {@code long}s and are the actual expiry enforcement point; the stored TTLs are
 * nominal values used to rebuild an entry after a state transition and to size
 * the refresh-failure lease.
 *
 * <p>The nine fields total 64 bytes with compressed class pointers (12-byte
 * header, four {@code long}s, three {@code int}s, two references). See design
 * decisions: entry model.
 *
 * <p><b>Construction and modification.</b> The entry is immutable; every creation and every
 * copy-on-write modification flows through {@link EntryDraft}. Production code obtains drafts
 * from {@code EntryLifecycle.newEntry()} / {@code EntryLifecycle.editEntry(entry)} (wired to
 * the TTL arithmetic); explicit-timestamp callers use {@link EntryDraft#of(CacheEntry)}.
 */
@Getter
@ToString
@EqualsAndHashCode
public class CacheEntry {

  /** Number of bits reserved for the value-birth second within {@link #packedMeta}. */
  private static final int BIRTH_BITS = 30;
  /** Bit shift of the value-birth second within {@link #packedMeta} (above the 2-bit state code). */
  private static final int BIRTH_SHIFT = 2;
  /** Mask covering the birth bits of {@link #packedMeta} (≈ 34 years of seconds). */
  private static final long BIRTH_MASK = (1L << BIRTH_BITS) - 1;
  /** Bit shift of the encoded hard TTL within {@link #packedMeta} (above state + birth). */
  private static final int HARD_TTL_SHIFT = BIRTH_SHIFT + BIRTH_BITS;
  /** Two-bit key-state code mask — the state sits in the low bits for zero-shift hot-path decode and at-a-glance debugging. */
  private static final int STATE_CODE_MASK = 0x3;
  /** Key-state codes — explicit, not enum ordinals (HOT/COOL/NORMAL order is not stable). */
  private static final int STATE_CODE_NULL = 0;
  private static final int STATE_CODE_NORMAL = 1;
  private static final int STATE_CODE_COOL = 2;
  private static final int STATE_CODE_HOT = 3;
  /**
   * Maximum staleness of a lease-kept entry in seconds (30 min). A background
   * refresh failure never extends an entry older than this — the lease is a
   * provisional keep-alive, not indefinite life support. Second precision
   * suffices because the bound is minute-scale.
   */
  public static final int MAX_LEASE_STALE_SEC = 30 * 60;

  /**
   * TTL storage units. A packed TTL is one {@code int}: bit 31 selects the unit
   * (0 = milliseconds, 1 = seconds), bits [0, 31) carry the magnitude.
   * {@link #TTL_ENCODE_INFINITE} is the dedicated permanent sentinel — a
   * seconds-encoded value always has a non-zero magnitude, so it can never
   * collide with a real duration.
   */
  private static final int TTL_UNIT_SECONDS = 0x8000_0000;
  /** Largest magnitude the packed form can carry in either unit. */
  private static final long TTL_MAGNITUDE_MAX = Integer.MAX_VALUE;
  /**
   * Largest representable TTL: {@code 2^31 − 1} seconds (≈ 68 years). Durations
   * up to {@code 2^31 − 1} ms (≈ 24.8 days) round-trip exactly in milliseconds;
   * longer ones are stored in whole seconds, rounded up, with sub-second error.
   */
  private static final long TTL_MAX_MS = TTL_MAGNITUDE_MAX * 1_000L;
  /** Packed sentinel for {@code Long.MAX_VALUE} (permanent entry). */
  private static final int TTL_ENCODE_INFINITE = Integer.MIN_VALUE;

  /**
   * The cached value. Production read/write paths never cache {@code null}
   * (a null reader result writes no entry), so a {@code null} value here is
   * only reachable through direct low-level construction (tests, placeholders).
   *
   * <p>Excluded from {@link #toString()}: a debug log must not materialise a
   * large business object's collection graph or leak PII. The type is not
   * printed either — the key and version metadata identify the entry.
   */
  @ToString.Exclude
  private final Object value;

  /**
   * A positive data stamp allocated by the node-local {@code StampClock}
   * (design decisions: version stamps). Used by the cache-sync plane to resolve concurrent write
   * conflicts across instances. {@code > 0} is a real stamp; {@code 0} means
   * unknown (weakest claim — it accepts any real stamp).
   */
  private final long dataVersion;

  /**
   * The Worker decision lease carried by this entry, or {@code null} for a local
   * entry. Immutable and stored by reference, so a seeded draft and a
   * Worker-managed entry share the record rather than copying its fields.
   *
   * <p>The lease carries its own kind. It can outlive the effective
   * {@link #getKeyState()} (a COOL lease on a NORMAL or locally overridden HOT
   * entry), so {@link #isWorkerManaged()} checks state and kind together.
   * Orthogonal to {@link #dataVersion}; ordering is per Worker incarnation
   * (see design decisions: version stamps).
   */
  @Getter(AccessLevel.NONE)
  @ToString.Exclude
  private final @Nullable DecisionStamp decision;

  /**
   * Packed per-entry metadata:
   * <ul>
   *   <li>bits [0, 2): the 2-bit key-state code, in the low bits for a
   *       zero-shift hot-path decode and at-a-glance debugging;</li>
   *   <li>bits [2, 32): the value-birth second, {@code 0} = unknown;</li>
   *   <li>bits [32, 64): the encoded hard TTL duration.</li>
   * </ul>
   * Decoded via {@link #getKeyState()}, {@link #getBirthStampSec()} and
   * {@link #getHardTtlMs()}. The packed representation is an internal encoding
   * detail and has no generated getter.
   */
  @Getter(AccessLevel.NONE)
  @ToString.Exclude
  private final long packedMeta;

  /**
   * Absolute epoch-millis timestamp at which the entry should be evicted
   * (hard expiry). Compared against the cache clock
   * ({@code TimeSource.currentTimeMillis()}) on each read.
   */
  private final long hardExpireAtMs;

  /**
   * Packed soft TTL duration (unit bit + 31-bit magnitude). Decoded via
   * {@link #getSoftTtlMs()}; {@code 0} means no soft expire.
   *
   * <p>{@code @Getter(AccessLevel.NONE)} is deliberate: the packed {@code int}
   * must never be observable. Without it Lombok would generate
   * {@code int getSoftTtlMs()} if the hand-written {@code long} getter were
   * renamed or removed — and an {@code int} widens silently into a {@code long},
   * so callers would receive the packed encoding instead of a duration with
   * nothing failing to compile. The same guard is applied to the other packed
   * TTL fields.
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
   * Packed normal-state hard TTL baseline (unit bit + 31-bit magnitude).
   * Decoded via {@link #getNormalHardTtlMs()}. See {@link #softTtlMs} for why
   * the packed field is not exposed as a getter.
   */
  @Getter(AccessLevel.NONE)
  @ToString.Exclude
  private final int normalHardTtlMs;

  /**
   * Packed normal-state soft TTL baseline (unit bit + 31-bit magnitude).
   * Decoded via {@link #getNormalSoftTtlMs()}. See {@link #softTtlMs} for why
   * the packed field is not exposed as a getter.
   */
  @Getter(AccessLevel.NONE)
  @ToString.Exclude
  private final int normalSoftTtlMs;

  /**
   * Creates a new entry, packing the key state, the value-birth second and the
   * hard TTL into {@link #packedMeta} and the remaining TTLs into compact
   * encodings.
   *
   * <p>Package-private on purpose: {@link EntryDraft} is the only construction and
   * modification API, and calls this constructor directly — zero extra allocation.
   *
   * <p>{@code dataVersion > 0} is a real stamp allocated by the node-local
   * stamp clock; {@code 0} means unknown and carries the weakest claim.
   *
   * @param value             the cached value
   * @param dataVersion       the data stamp; {@code > 0} for a real stamp,
   *                          {@code 0} for unknown
   * @param decision          the Worker decision stamp, or {@code null} for a
   *                          local entry
   * @param hardTtlMs         the hard TTL duration in milliseconds
   * @param hardExpireAtMs    the absolute hard expiry timestamp
   * @param softTtlMs         the soft TTL duration in milliseconds
   * @param softExpireAtMs    the absolute soft expiry timestamp
   * @param keyState          the hot-key state, may be null
   * @param normalHardTtlMs   the normal-state hard TTL baseline
   * @param normalSoftTtlMs   the normal-state soft TTL baseline
   * @param birthStampSec     the monotonic birth second, {@code 0} = unknown
   * @throws IllegalArgumentException if a TTL is negative or larger than
   *         {@code 2^31 - 1} seconds (except {@code Long.MAX_VALUE})
   */
  CacheEntry(
    @Nullable Object value,
    long dataVersion,
    @Nullable DecisionStamp decision,
    long hardTtlMs,
    long hardExpireAtMs,
    long softTtlMs,
    long softExpireAtMs,
    @Nullable KeyState keyState,
    long normalHardTtlMs,
    long normalSoftTtlMs,
    int birthStampSec
  ) {
    this.value = value;
    this.dataVersion = dataVersion;
    this.decision = decision;
    this.packedMeta =
      ((long) encodeTtl(hardTtlMs, "hardTtlMs") << HARD_TTL_SHIFT) |
      ((Math.max(0, birthStampSec) & BIRTH_MASK) << BIRTH_SHIFT) |
      stateCode(keyState);
    this.hardExpireAtMs = hardExpireAtMs;
    this.softTtlMs = encodeTtl(softTtlMs, "softTtlMs");
    this.softExpireAtMs = softExpireAtMs;
    this.normalHardTtlMs = encodeTtl(normalHardTtlMs, "normalHardTtlMs");
    this.normalSoftTtlMs = encodeTtl(normalSoftTtlMs, "normalSoftTtlMs");
  }

  /**
   * Whether this entry currently carries an effective heat state at all.
   *
   * <p>This is the TTL-regime question, not the authority question: a locally
   * promoted HOT entry is elevated without being Worker-managed. Value writes
   * use this predicate to preserve the active regime.
   *
   * @return {@code true} for HOT or COOL
   */
  public boolean isElevated() {
    KeyState state = getKeyState();
    return state == KeyState.HOT || state == KeyState.COOL;
  }

  /**
   * Whether the last Worker decision still owns the effective heat state.
   *
   * <p>A COOL entry is Worker-managed while its carrier still exists. A HOT
   * entry is Worker-managed only when that carrier is a HOT decision: a local
   * promotion keeps a COOL lease for ordering, but the effective HOT state
   * belongs to the app.
   *
   * @return {@code true} when the effective state is the one the Worker
   *         decision established
   */
  public boolean isWorkerManaged() {
    if (decision == null) {
      return false;
    }
    KeyState state = getKeyState();
    if (state == KeyState.COOL) {
      return true;
    }
    return state == KeyState.HOT && decision.isHot();
  }

  /**
   * Whether a local promotion lifted a Worker-COOL entry to HOT while the
   * cluster health gate was failed. The COOL lease is retained for ordering
   * and for the read-path reassertion on recovery; it does not own the
   * effective state.
   *
   * @return {@code true} for a HOT entry carrying a COOL lease
   */
  public boolean isLocalOverrideHot() {
    return getKeyState() == KeyState.HOT && decision != null && decision.isCool();
  }

  /**
   * Whether this entry is too old for another lease-on-failure extension:
   * entries born more than {@link #MAX_LEASE_STALE_SEC} ago (on the monotonic
   * axis) must hard-expire instead of being renewed. Unknown birth
   * ({@code 0}) is never capped.
   *
   * @param nowSec the current monotonic seconds-since-boot
   * @return {@code true} when the entry exceeded the staleness cap
   */
  public boolean isLeaseStaleCapped(int nowSec) {
    int birthStampSec = getBirthStampSec();
    return birthStampSec != 0 && nowSec - birthStampSec > MAX_LEASE_STALE_SEC;
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
   * Packs a TTL duration into one {@code int}: bit 31 selects seconds, bits
   * [0, 31) carry the magnitude.
   *
   * <p>Durations up to {@code 2^31 − 1} ms (≈ 24.8 days) are stored verbatim in
   * milliseconds. Longer ones are converted to seconds with ceiling division,
   * so the decoded value never falls below the request — the safe direction for
   * a nominal TTL — and decoding then re-encoding is a no-op (the decoded
   * duration is an exact second count, which lands in the same unit again).
   * {@code Long.MAX_VALUE} (permanent) maps to {@link #TTL_ENCODE_INFINITE}.
   * Negative values and values above {@link #TTL_MAX_MS} are rejected.
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
    if (ttlMs < 0 || ttlMs > TTL_MAX_MS) {
      throw new IllegalArgumentException(
        fieldName + " out of range [0, " + TTL_MAX_MS + "] or Long.MAX_VALUE: " + ttlMs
      );
    }
    if (ttlMs <= TTL_MAGNITUDE_MAX) {
      return (int) ttlMs;
    }
    return (int) (TTL_UNIT_SECONDS | ceilDiv(ttlMs, 1_000L));
  }

  /** Ceiling division for the positive TTL magnitudes encoded here. */
  private static long ceilDiv(long value, long divisor) {
    return value / divisor + (value % divisor == 0 ? 0 : 1);
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
    return encoded >= 0 ? encoded : (long) (encoded & 0x7FFF_FFFF) * 1_000L;
  }

  /**
   * The Worker decision stamp carried by this entry, or {@code null} for a local
   * origin (local promotion, cleared demotion). The instance is the immutable
   * record the entry was built from, so this accessor allocates nothing; callers
   * may pass the result straight back to {@link EntryDraft#decision}.
   *
   * @return the decision stamp, or {@code null} when the entry has no Worker origin
   */
  @Nullable
  public DecisionStamp decision() {
    return decision;
  }

  /**
   * The epoch (restart counter) of the Worker that produced this entry's
   * decision. A higher epoch indicates a Worker restart; {@link VersionGuard}
   * unconditionally accepts decisions from a higher epoch (see design decisions:
   * heartbeat and health). {@code 0} for a local entry.
   *
   * @return the decision epoch, or {@code 0} without a decision stamp
   */
  @ToString.Include(name = "decisionEpoch")
  public long getDecisionEpoch() {
    return decision == null ? 0L : decision.decisionEpoch();
  }

  /**
   * The Worker decision version carried by this entry. Orthogonal to
   * {@link #dataVersion}; used solely for ordering Worker decision broadcasts
   * (see design decisions: version stamps).
   *
   * @return the decision version, or {@code 0} without a decision stamp
   */
  @ToString.Include(name = "decisionVersion")
  public long getDecisionVersion() {
    return decision == null ? 0L : decision.decisionVersion();
  }

  /**
   * The Worker node ID that produced this entry's decision, or {@code null} for a
   * local entry.
   *
   * @return the originating Worker node ID, or {@code null}
   */
  @ToString.Include(name = "decisionNodeId")
  @Nullable
  public String getDecisionNodeId() {
    return decision == null ? null : decision.decisionNodeId();
  }

  /**
   * Current hot-key state of this entry. Determines which TTL values
   * are active: {@link KeyState#HOT} uses extended TTLs,
   * {@link KeyState#COOL} reverts to normal TTLs.
   *
   * @return the key state, or {@code null} if not set
   */
  @ToString.Include(name = "keyState")
  @Nullable
  public KeyState getKeyState() {
    return stateFromCode((int) (packedMeta & STATE_CODE_MASK));
  }

  /**
   * Monotonic seconds-since-boot when this entry's value was born (fresh load
   * or successful refresh). {@code 0} means unknown (arithmetic-free drafts) and
   * always admits a lease. Production stamps are always {@code >= 1}:
   * {@code TimeSource} clamps the first boot second so a value loaded at JVM
   * start cannot masquerade as "unknown".
   *
   * @return the value-birth second, or {@code 0} when unknown
   */
  public int getBirthStampSec() {
    return (int) ((packedMeta >>> BIRTH_SHIFT) & BIRTH_MASK);
  }

  /**
   * Hard TTL duration in milliseconds for this entry. The entry is evicted
   * unconditionally when {@link #hardExpireAtMs} is reached, regardless of
   * access patterns.
   *
   * <p>Decoded from the packed storage: values up to 2^31 − 1 ms (≈ 24.8 days)
   * round-trip exactly; longer values are second-aligned (rounded up, sub-second
   * error). {@code Long.MAX_VALUE} denotes a permanent entry.
   *
   * @return the hard TTL in milliseconds
   */
  @ToString.Include(name = "hardTtlMs")
  public long getHardTtlMs() {
    return decodeTtl((int) (packedMeta >>> HARD_TTL_SHIFT));
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
