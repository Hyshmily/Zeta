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

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.jspecify.annotations.Nullable;

/**
 * One L1 value and the only metadata the data plane owns: its data stamp and the caller's TTL
 * declaration.
 *
 * <p>The caller's declared TTL is the entry's only lifetime. A key being hot is a runtime
 * observation and cluster leases live in the heat plane; neither supplies or rewrites a duration
 * here. That separation is what keeps the freshness contract a property of the data rather than of
 * the traffic it happens to receive.
 *
 * <p><b>Resolved durations only.</b> A hard TTL is positive ({@link Long#MAX_VALUE} = permanent)
 * and a soft TTL is {@code 0} (disabled) or positive. The declaration vocabulary ({@code 0} =
 * inherit, {@code -1} = disabled) ends at the TTL resolution boundary.
 *
 * <p><b>Compact layout.</b> The two durations share one {@code int} each as a unit bit plus a
 * 31-bit magnitude: durations up to {@code 2^31 - 1} ms round-trip exactly; longer ones are stored
 * in whole seconds, rounded up, and {@link Long#MAX_VALUE} has a dedicated sentinel. The absolute
 * deadlines stay full {@code long}s and are the enforcement point.
 */
@Getter
@ToString
@EqualsAndHashCode
public class CacheEntry {

  /**
   * TTL storage units. A packed TTL is one {@code int}: bit 31 selects the unit
   * (0 = milliseconds, 1 = seconds), bits [0, 31) carry the magnitude.
   * {@link #TTL_ENCODE_INFINITE} is the permanent sentinel.
   */
  private static final int TTL_UNIT_SECONDS = 0x8000_0000;
  private static final long TTL_MAGNITUDE_MAX = Integer.MAX_VALUE;
  private static final long TTL_MAX_MS = TTL_MAGNITUDE_MAX * 1_000L;
  private static final int TTL_ENCODE_INFINITE = Integer.MIN_VALUE;

  /**
   * The cached value. Production read/write paths never cache {@code null}; a null here is only
   * reachable through direct low-level construction.
   */
  @ToString.Exclude
  private final Object value;

  /**
   * A positive data stamp allocated by the node-local {@code StampClock}. {@code > 0} is a real
   * stamp; {@code 0} means unknown and accepts any real stamp.
   */
  private final long dataVersion;

  /** Packed hard TTL duration; decoded through {@link #getHardTtlMs()}. */
  @Getter(AccessLevel.NONE)
  @ToString.Exclude
  private final int hardTtlMs;

  /** Absolute epoch-millis hard deadline: the entry is never served at or after it. */
  private final long hardExpireAtMs;

  /** Packed soft TTL duration; decoded through {@link #getSoftTtlMs()}. {@code 0} = disabled. */
  @Getter(AccessLevel.NONE)
  @ToString.Exclude
  private final int softTtlMs;

  /** Absolute epoch-millis soft deadline; {@code 0} means the window is disabled. */
  private final long softExpireAtMs;

  /**
   * Creates a new entry, packing both durations into compact fields and re-checking every
   * invariant once at the storage boundary.
   *
   * <p>Package-private on purpose: {@link EntryDraft} is the only construction and modification
   * API, and it calls this constructor directly with zero extra allocation.
   *
   * <p>{@code dataVersion > 0} is a real stamp allocated by the node-local stamp clock; {@code 0}
   * means unknown and carries the weakest claim. A hard duration is positive (or the permanent
   * sentinel); a soft duration is {@code 0} (disabled) or positive. Negative, unresolved and
   * unencodable values fail here rather than inside an L1 compute or an after-commit hook.
   *
   * @param value           the cached value
   * @param dataVersion     the data stamp ({@code > 0} real, {@code 0} unknown)
   * @param hardTtlMs       the resolved hard TTL (positive or {@link Long#MAX_VALUE})
   * @param hardExpireAtMs  the absolute hard deadline
   * @param softTtlMs       the resolved soft TTL ({@code 0} = disabled)
   * @param softExpireAtMs  the absolute soft deadline
   * @throws IllegalArgumentException when a duration is unresolved, negative or unencodable
   */
  CacheEntry(
    @Nullable Object value,
    long dataVersion,
    long hardTtlMs,
    long hardExpireAtMs,
    long softTtlMs,
    long softExpireAtMs
  ) {
    requireResolvedHardTtl(hardTtlMs, "hardTtlMs");
    requireEncodableTtl(softTtlMs, "softTtlMs");
    this.value = value;
    this.dataVersion = dataVersion;
    this.hardTtlMs = encodeTtl(hardTtlMs, "hardTtlMs");
    this.hardExpireAtMs = hardExpireAtMs;
    this.softTtlMs = encodeTtl(softTtlMs, "softTtlMs");
    this.softExpireAtMs = softExpireAtMs;
  }

  /**
   * Packs a TTL duration into one {@code int}: bit 31 selects seconds, bits [0, 31) carry the
   * magnitude.
   *
   * <p>Durations up to {@code 2^31 - 1} ms (≈ 24.8 days) are stored verbatim in milliseconds.
   * Longer ones are converted to whole seconds with ceiling division, so the decoded value never
   * falls below the request — the safe direction for a nominal TTL — and decoding then
   * re-encoding is a no-op. {@link Long#MAX_VALUE} maps to the permanent sentinel.
   *
   * @param ttlMs     the TTL duration in milliseconds
   * @param fieldName the field name for an error message
   * @return the packed encoding
   * @throws IllegalArgumentException for a negative or out-of-range value
   */
  @SuppressWarnings("all")
  private static int encodeTtl(long ttlMs, String fieldName) {
    requireEncodableTtl(ttlMs, fieldName);
    if (ttlMs == Long.MAX_VALUE) {
      return TTL_ENCODE_INFINITE;
    }
    if (ttlMs <= TTL_MAGNITUDE_MAX) {
      return (int) ttlMs;
    }
    return (int) (TTL_UNIT_SECONDS | ceilDiv(ttlMs, 1_000L));
  }

  /**
   * Validates a declared TTL at a public boundary. The declaration vocabulary is shared by every
   * layer: {@code -1} disables a soft window (for a hard TTL it inherits, because a hard bound has
   * no "off" state), {@code 0} inherits, and every positive duration is used exactly as written,
   * including {@link Long#MAX_VALUE}. Any other negative value is a typo and fails where it was
   * written.
   *
   * @param ttlMs     the declared TTL in milliseconds
   * @param fieldName the field name for an error message
   * @throws IllegalArgumentException when the value is negative other than {@code -1}, or above
   *                                  the packed maximum other than {@link Long#MAX_VALUE}
   */
  public static void requireDeclaredTtl(long ttlMs, String fieldName) {
    if (ttlMs == -1L) {
      return;
    }
    requireEncodableTtl(ttlMs, fieldName);
  }

  /**
   * Validates a resolved duration before it reaches the packed entry fields. Every public TTL
   * boundary calls this, so an unrepresentable duration fails where the caller supplied it
   * instead of during an L1 compute or a transaction's after-commit hook.
   *
   * @param ttlMs     the resolved TTL duration in milliseconds
   * @param fieldName the field name for an error message
   * @throws IllegalArgumentException when the value is negative or above the packed maximum
   *                                  other than {@link Long#MAX_VALUE}
   */
  public static void requireEncodableTtl(long ttlMs, String fieldName) {
    if (!isEncodableTtl(ttlMs)) {
      throw new IllegalArgumentException(
        fieldName + " out of range [0, " + TTL_MAX_MS + "] or Long.MAX_VALUE: " + ttlMs
      );
    }
  }

  /**
   * Whether a resolved duration fits the packed field: {@code [0, TTL_MAX_MS]} or the permanent
   * sentinel. The predicate form lets a caller branch on encodability (a runtime TTL expression
   * degrades to the default) without using the exception as control flow.
   *
   * @param ttlMs the resolved TTL duration in milliseconds
   * @return {@code true} when {@link #requireEncodableTtl(long, String)} would accept it
   */
  public static boolean isEncodableTtl(long ttlMs) {
    return ttlMs == Long.MAX_VALUE || (ttlMs >= 0L && ttlMs <= TTL_MAX_MS);
  }

  /**
   * Validates a resolved hard duration: positive or the permanent sentinel.
   *
   * <p>There is no "unset" hard TTL inside the entry — {@code 0} and {@code -1} are declaration
   * values that resolution has already replaced, so seeing one here is a bug at the caller, not a
   * request for a default.
   *
   * @param ttlMs     the resolved hard TTL in milliseconds
   * @param fieldName the field name for an error message
   * @throws IllegalArgumentException when the value is {@code 0}, negative, or unencodable
   */
  public static void requireResolvedHardTtl(long ttlMs, String fieldName) {
    if (ttlMs <= 0L) {
      throw new IllegalArgumentException(
        fieldName + " must be resolved before it reaches an entry (positive or Long.MAX_VALUE): " + ttlMs
      );
    }
    requireEncodableTtl(ttlMs, fieldName);
  }

  /**
   * Positive-integer ceiling division. Packing a millisecond TTL into whole seconds rounds the
   * decoded duration up, so it never falls below the lifetime the caller requested.
   */
  @SuppressWarnings("all")
  private static long ceilDiv(long value, long divisor) {
    return value / divisor + (value % divisor == 0 ? 0 : 1);
  }

  /**
   * Reverses {@link #encodeTtl(long, String)}. Non-negative encodings are millisecond magnitudes;
   * the high bit marks a second-based magnitude, and the sentinel decodes back to
   * {@link Long#MAX_VALUE}. Round-tripping an encodable duration through both methods is lossless
   * at millisecond precision or rounds up to the next whole second.
   */
  private static long decodeTtl(int encoded) {
    if (encoded == TTL_ENCODE_INFINITE) {
      return Long.MAX_VALUE;
    }
    return encoded >= 0 ? encoded : (long) (encoded & 0x7FFF_FFFF) * 1_000L;
  }

  /**
   * Hard TTL duration in milliseconds. The entry is unconditionally unavailable when its hard
   * deadline is reached, regardless of access patterns.
   *
   * <p>Decoded from the packed storage: values up to {@code 2^31 - 1} ms round-trip exactly;
   * longer values are second-aligned (rounded up). The value is the caller's resolved duration —
   * positive, or {@link Long#MAX_VALUE} for a permanent entry.
   *
   * @return the hard TTL in milliseconds ({@link Long#MAX_VALUE} = permanent)
   */
  @ToString.Include(name = "hardTtlMs")
  public long getHardTtlMs() {
    return decodeTtl(hardTtlMs);
  }

  /**
   * Soft TTL duration in milliseconds. After its soft deadline the entry is stale and a read may
   * serve it while a background refresh runs. {@code 0} is a decision, not a gap: the
   * declaration disabled the window, and no consumer may re-resolve a configured default over it.
   *
   * @return the soft TTL in milliseconds, {@code 0} when disabled
   */
  @ToString.Include(name = "softTtlMs")
  public long getSoftTtlMs() {
    return decodeTtl(softTtlMs);
  }
}
