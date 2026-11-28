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

import io.github.hyshmily.zeta.util.TimeSource;
import org.jspecify.annotations.Nullable;

/**
 * The mutable intermediate representation through which every {@link CacheEntry} is created and
 * modified: shape it, {@link #build()} it, discard it. One draft and one entry per modification;
 * the packed TTL fields decode and re-encode exactly once.
 *
 * <p><b>Entry points.</b> {@code EntryLifecycle.newEntry()} (blank draft wired to the manager's
 * jitter-aware arithmetic), {@code EntryLifecycle.editEntry(entry)} (seeded copy-on-write with
 * the same arithmetic), and {@link #of(CacheEntry)} / {@link #blank(ExpiryArithmetic)} for
 * callers without a manager.
 *
 * <p><b>Value discipline.</b> {@link #value} stores the value as passed (no codec), so building
 * inside a {@code compute} callback is always lock-safe.
 *
 * <p><b>TTL semantics.</b> Durations passed to {@link #ttl}/{@link #softTtl} are already resolved
 * to their final intent — hard is positive or {@link Long#MAX_VALUE} (permanent), soft is
 * {@code 0} (disabled) or positive ({@link Long#MAX_VALUE} = never stale). A {@code 0} hard TTL
 * is a caller bug and is rejected here: the declaration vocabulary ({@code 0} = inherit,
 * {@code -1} = disabled) ends at the TTL resolution boundary. Callers that need an explicit
 * deadline set the durations first and override with {@link #hardExpiryAt} /
 * {@link #softExpiryAt} / {@link #expiryAt}.
 *
 * <p>A draft that never declares a TTL is permanent: its blank defaults are
 * {@link Long#MAX_VALUE} for the hard duration and {@code 0} for the soft one, so a built entry
 * is never in an "unset" state.
 *
 * <p>The draft is single-threaded by contract: create, shape, build on one thread.
 */
public final class EntryDraft {

  /**
   * Duration-to-timestamp arithmetic. {@link #SYSTEM} is the plain {@code now + ttl} form;
   * production drafts carry the cache's jitter-aware policy. Both mint absolute instants on the
   * monotonic axis ({@code TimeSource.monotonicMillis()}), the axis every deadline comparison uses.
   */
  public interface ExpiryArithmetic {
    /**
     * @param hardTtlMs the resolved hard TTL duration (positive)
     * @return absolute monotonic-ms hard expiry
     */
    long hardExpireAt(long hardTtlMs);

    /**
     * @param softTtlMs the resolved soft TTL duration ({@code 0} = disabled)
     * @return absolute monotonic-ms soft expiry, or {@code 0}
     */
    long softExpireAt(long softTtlMs);
  }

  /** Plain {@code now + ttl}, no jitter, saturating at {@link Long#MAX_VALUE}. */
  public static final ExpiryArithmetic SYSTEM = new ExpiryArithmetic() {
    @Override
    public long hardExpireAt(long hardTtlMs) {
      return hardTtlMs == Long.MAX_VALUE ? Long.MAX_VALUE : fromNow(hardTtlMs);
    }

    @Override
    public long softExpireAt(long softTtlMs) {
      if (softTtlMs == Long.MAX_VALUE) {
        return Long.MAX_VALUE;
      }
      return softTtlMs > 0 ? fromNow(softTtlMs) : 0L;
    }

    /**
     * Adds a resolved TTL to the current monotonic clock sample, saturating at
     * {@link Long#MAX_VALUE} so an entry reads as permanent rather than as an instant in the past.
     * The saturation is decided on the {@code now} side of the addition: the clock sample itself may
     * be negative (a negative-offset base, a test clock), and a sum that stays negative is a valid
     * future deadline, not an overflow.
     */
    private long fromNow(long ttlMs) {
      long now = TimeSource.monotonicMillis();
      return now > Long.MAX_VALUE - ttlMs ? Long.MAX_VALUE : now + ttlMs;
    }
  };

  private final ExpiryArithmetic arithmetic;

  private @Nullable Object value;
  private long hardExpireAtMs = Long.MAX_VALUE;
  private long softExpireAtMs;
  private long dataVersion;
  private long hardTtlMs = Long.MAX_VALUE;
  private long softTtlMs;

  private EntryDraft(ExpiryArithmetic arithmetic) {
    this.arithmetic = arithmetic;
  }

  /**
   * @param arithmetic expiry arithmetic, or {@code null} for {@link #SYSTEM}
   * @return a blank draft
   */
  public static EntryDraft blank(@Nullable ExpiryArithmetic arithmetic) {
    return new EntryDraft(arithmetic != null ? arithmetic : SYSTEM);
  }

  /**
   * @param source the entry to copy
   * @return a seeded draft using {@link #SYSTEM}
   */
  public static EntryDraft of(CacheEntry source) {
    return of(source, SYSTEM);
  }

  /**
   * @param source     the entry to copy
   * @param arithmetic expiry arithmetic, or {@code null} for {@link #SYSTEM}
   * @return a seeded draft
   */
  public static EntryDraft of(CacheEntry source, @Nullable ExpiryArithmetic arithmetic) {
    EntryDraft draft = new EntryDraft(arithmetic != null ? arithmetic : SYSTEM);
    draft.value = source.getValue();
    draft.dataVersion = source.getDataVersion();
    draft.hardTtlMs = source.getHardTtlMs();
    draft.hardExpireAtMs = source.getHardExpireAtMs();
    draft.softTtlMs = source.getSoftTtlMs();
    draft.softExpireAtMs = source.getSoftExpireAtMs();
    return draft;
  }

  /**
   * @param storedValue the value in stored form
   * @return this draft
   */
  public EntryDraft value(@Nullable Object storedValue) {
    this.value = storedValue;
    return this;
  }

  /**
   * @param dataVersion the positive data stamp, or {@code 0} for unknown
   * @return this draft
   */
  public EntryDraft version(long dataVersion) {
    this.dataVersion = dataVersion;
    return this;
  }

  /**
   * Sets both active durations and computes their deadlines through the injected arithmetic.
   *
   * <p>A soft TTL longer than the hard one is accepted, not rejected: the hard expiry evicts the
   * entry before the soft window opens, so the soft deadline simply never triggers. Rejecting it
   * would forbid the legitimate single-field update ({@link #softTtl}) that extends one side of a
   * live entry.
   *
   * @param hardTtlMs the resolved hard TTL (positive, or {@link Long#MAX_VALUE})
   * @param softTtlMs the resolved soft TTL ({@code 0} = disabled)
   * @return this draft
   * @throws IllegalArgumentException when the hard duration is unresolved, or a duration is
   *                                  negative or unencodable
   */
  public EntryDraft ttl(long hardTtlMs, long softTtlMs) {
    requireResolvedTtls(hardTtlMs, softTtlMs);
    this.hardTtlMs = hardTtlMs;
    this.softTtlMs = softTtlMs;
    this.hardExpireAtMs = computedHardExpiryAt(hardTtlMs);
    this.softExpireAtMs = computedSoftExpiryAt(softTtlMs);
    return this;
  }

  /**
   * Validate one resolved hard/soft duration pair.
   *
   * @param hardTtlMs the resolved hard TTL
   * @param softTtlMs the resolved soft TTL
   */
  static void requireResolvedTtls(long hardTtlMs, long softTtlMs) {
    CacheEntry.requireResolvedHardTtl(hardTtlMs, "hardTtlMs");
    CacheEntry.requireEncodableTtl(softTtlMs, "softTtlMs");
  }

  /**
   * Sets only the soft duration and computes its deadline — the soft-refresh pattern. The hard
   * deadline and duration are untouched, which is what keeps a caller's hard bound absolute
   * across a stale-while-revalidate refresh.
   *
   * @param softTtlMs the resolved soft TTL ({@code 0} = disabled, {@link Long#MAX_VALUE} = never
   *                  stale)
   * @return this draft
   * @throws IllegalArgumentException when the value is negative or unencodable
   */
  public EntryDraft softTtl(long softTtlMs) {
    CacheEntry.requireEncodableTtl(softTtlMs, "softTtlMs");
    this.softTtlMs = softTtlMs;
    this.softExpireAtMs = computedSoftExpiryAt(softTtlMs);
    return this;
  }

  /**
   * @param hardExpireAtMs the absolute hard deadline
   * @return this draft
   */
  public EntryDraft hardExpiryAt(long hardExpireAtMs) {
    this.hardExpireAtMs = hardExpireAtMs;
    return this;
  }

  /**
   * @param softExpireAtMs the absolute soft deadline
   * @return this draft
   */
  public EntryDraft softExpiryAt(long softExpireAtMs) {
    this.softExpireAtMs = softExpireAtMs;
    return this;
  }

  /**
   * @param hardExpireAtMs the absolute hard deadline
   * @param softExpireAtMs the absolute soft deadline
   * @return this draft
   */
  public EntryDraft expiryAt(long hardExpireAtMs, long softExpireAtMs) {
    this.hardExpireAtMs = hardExpireAtMs;
    this.softExpireAtMs = softExpireAtMs;
    return this;
  }

  /**
   * Recomputes both deadlines from the current durations, leaving the durations themselves
   * untouched — the hard-renewal pattern: a fresh value starts its own lifetime under exactly
   * the cadence the caller declared.
   *
   * @return this draft
   */
  public EntryDraft rearmExpiry() {
    this.hardExpireAtMs = computedHardExpiryAt(hardTtlMs);
    this.softExpireAtMs = computedSoftExpiryAt(softTtlMs);
    return this;
  }

  /**
   * Materializes the draft as an immutable {@link CacheEntry}. The draft has already rejected
   * negative durations at the setters that accept them; the entry constructor owns the packed
   * invariants and re-checks them once here.
   *
   * @return a new immutable entry with the draft's logical fields
   */
  public CacheEntry build() {
    return new CacheEntry(value, dataVersion, hardTtlMs, hardExpireAtMs, softTtlMs, softExpireAtMs);
  }

  private long computedHardExpiryAt(long hardTtlMs) {
    return arithmetic.hardExpireAt(hardTtlMs);
  }

  private long computedSoftExpiryAt(long softTtlMs) {
    return arithmetic.softExpireAt(softTtlMs);
  }
}
