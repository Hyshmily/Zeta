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
import io.github.hyshmily.zeta.util.TimeSource;
import org.jspecify.annotations.Nullable;

/**
 * The mutable intermediate representation through which every {@link CacheEntry} is created
 * and modified: shape it, {@link #build()} it, discard it. One draft and one entry per
 * modification; the packed TTL/state fields decode and re-encode exactly once.
 *
 * <p><b>Entry points.</b> {@code EntryLifecycle.newEntry()} (blank draft wired to the
 * manager's jitter-aware TTL arithmetic), {@code EntryLifecycle.editEntry(entry)} (seeded
 * copy-on-write with the same arithmetic), and {@link #of(CacheEntry)} /
 * {@link #blank(ExpiryArithmetic)} for callers without a manager.
 *
 * <p><b>Value discipline.</b> {@link #value} stores the value as passed (no codec), so
 * building inside a {@code compute} callback is always lock-safe.
 *
 * <p><b>TTL semantics.</b> Durations passed to {@link #ttl}/{@link #softTtl} are already
 * resolved to their final intent; the computed expiry follows the {@link ExpiryArithmetic}
 * contract — hard {@code 0} or {@code Long.MAX_VALUE} is permanent, soft {@code <= 0} is
 * disabled. Callers that need a custom jitter ratio or an explicit deadline set the durations
 * first and override with {@link #hardExpiryAt}/{@link #softExpiryAt}/{@link #expiryAt}.
 *
 * <p>The draft is single-threaded by contract: create, shape, build on one thread.
 */
public final class EntryDraft {

  /**
   * Duration-to-timestamp arithmetic. {@link #SYSTEM} is the plain {@code now + ttl} form
   * used by drafts with no manager (tests, explicit-timestamp rewrites); production drafts
   * carry the cache's jitter-aware policy.
   */
  public interface ExpiryArithmetic {
    /**
     * The absolute hard-expiry timestamp for a hard TTL duration. {@code <= 0} and
     * {@link Long#MAX_VALUE} mean permanent.
     *
     * @param hardTtlMs the hard TTL duration in milliseconds (already resolved)
     * @return absolute epoch-ms hard expiry timestamp
     */
    long hardExpireAt(long hardTtlMs);

    /**
     * The absolute soft-expiry timestamp for a soft TTL duration. {@code <= 0} means
     * disabled ({@code 0}).
     *
     * @param softTtlMs the soft TTL duration in milliseconds (already resolved)
     * @return absolute epoch-ms soft expiry timestamp, or {@code 0} for disabled
     */
    long softExpireAt(long softTtlMs);
  }

  /** Plain {@code now + ttl}, no jitter, saturating at {@link Long#MAX_VALUE}. */
  public static final ExpiryArithmetic SYSTEM = new ExpiryArithmetic() {
    @Override
    public long hardExpireAt(long hardTtlMs) {
      return hardTtlMs > 0 && hardTtlMs != Long.MAX_VALUE ? fromNow(hardTtlMs) : Long.MAX_VALUE;
    }

    @Override
    public long softExpireAt(long softTtlMs) {
      if (softTtlMs == Long.MAX_VALUE) {
        return Long.MAX_VALUE;
      }
      return softTtlMs > 0 ? fromNow(softTtlMs) : 0L;
    }

    private long fromNow(long ttlMs) {
      long at = TimeSource.currentTimeMillis() + ttlMs;
      return at < 0L ? Long.MAX_VALUE : at;
    }
  };

  private final ExpiryArithmetic arithmetic;

  private @Nullable DecisionStamp decision;
  private @Nullable KeyState keyState;
  private @Nullable Object value;
  private long normalHardTtlMs;
  private long normalSoftTtlMs;
  private long hardExpireAtMs;
  private long softExpireAtMs;
  /**
   * Value-birth stamp (monotonic seconds-since-boot) — carried over from the
   * source entry on a seeded draft; blank drafts start at {@code 0}
   * (unknown ⇒ lease admits) unless shaped explicitly.
   */
  private int birthStampSec;
  private long dataVersion;
  private long hardTtlMs;
  private long softTtlMs;

  private EntryDraft(ExpiryArithmetic arithmetic) {
    this.arithmetic = arithmetic;
  }

  /**
   * A blank draft for creating a fresh entry. Production code goes through
   * {@code EntryLifecycle.newEntry()} instead, which wires the TTL arithmetic.
   *
   * @param arithmetic the expiry arithmetic, or {@code null} for {@link #SYSTEM}
   * @return a blank draft with all-logical-zero fields
   */
  public static EntryDraft blank(@Nullable ExpiryArithmetic arithmetic) {
    return new EntryDraft(arithmetic != null ? arithmetic : SYSTEM);
  }

  /**
   * A draft seeded from an existing entry using {@link #SYSTEM} arithmetic: every field
   * starts as the source entry's. The manager-backed variant
   * ({@code EntryLifecycle.editEntry}) is the production modification path and carries the
   * jitter-aware policy instead.
   *
   * @param source the entry whose fields to seed from (not modified)
   * @return a seeded draft
   */
  public static EntryDraft of(CacheEntry source) {
    return of(source, SYSTEM);
  }

  /**
   * A draft seeded from an existing entry with expiry arithmetic.
   *
   * @param source    the entry whose fields to seed from (not modified)
   * @param arithmetic the expiry arithmetic, or {@code null} for {@link #SYSTEM}
   * @return a seeded draft
   */
  public static EntryDraft of(CacheEntry source, @Nullable ExpiryArithmetic arithmetic) {
    EntryDraft draft = new EntryDraft(arithmetic != null ? arithmetic : SYSTEM);
    draft.value = source.getValue();
    draft.dataVersion = source.getDataVersion();
    draft.decision = source.decision();
    draft.keyState = source.getKeyState();
    draft.hardTtlMs = source.getHardTtlMs();
    draft.hardExpireAtMs = source.getHardExpireAtMs();
    draft.softTtlMs = source.getSoftTtlMs();
    draft.softExpireAtMs = source.getSoftExpireAtMs();
    draft.normalHardTtlMs = source.getNormalHardTtlMs();
    draft.normalSoftTtlMs = source.getNormalSoftTtlMs();
    draft.birthStampSec = source.getBirthStampSec();
    return draft;
  }

  /**
   * Set the cached value (stored as-is, no codec) — see the
   * class Javadoc's value discipline.
   *
   * @param storedValue the value in final stored form (may be {@code null})
   * @return this draft
   */
  public EntryDraft value(@Nullable Object storedValue) {
    this.value = storedValue;
    return this;
  }

  /**
   * Set the data stamp. Stamps are allocated by the node-local
   * {@code StampClock} (design decisions: version stamps); {@code 0} is reserved for an unstamped
   * entry ("unknown") and must never be used for a real write.
   *
   * @param dataVersion the positive data stamp for cross-instance sync ordering
   * @return this draft
   */
  public EntryDraft version(long dataVersion) {
    this.dataVersion = dataVersion;
    return this;
  }

  /**
   * Set the Worker decision stamp. A {@code null} stamp is the local origin
   * and equivalent to {@link #clearDecision()}. The immutable record is stored
   * by reference, so a seeded draft carries the resident's stamp without copying
   * its fields and a Worker decision costs one reference, not three fields.
   *
   * @param stamp the decision stamp, or {@code null} for a local entry
   * @return this draft
   */
  public EntryDraft decision(@Nullable DecisionStamp stamp) {
    this.decision = stamp;
    return this;
  }

  /**
   * Clear the Worker decision stamp to the local origin — the demotion pattern.
   * The lease belongs to the decision, not the entry, so clearing the decision
   * drops it too.
   *
   * @return this draft
   */
  public EntryDraft clearDecision() {
    this.decision = null;
    return this;
  }

  /**
   * Set the key state (NORMAL, HOT, COOL), or {@code null} for no state.
   *
   * @param state the key state
   * @return this draft
   */
  public EntryDraft keyState(@Nullable KeyState state) {
    this.keyState = state;
    return this;
  }

  /**
   * Set the active hard/soft TTL durations and compute their expire timestamps
   * via the injected arithmetic, leaving the normal-state baseline untouched.
   * Durations must be resolved to their final intent (see class Javadoc).
   *
   * <p>A soft TTL longer than the hard one is accepted, not rejected: the hard
   * expiry evicts the entry before the soft window opens, so the soft deadline
   * simply never triggers. Rejecting it would forbid the legitimate single-field
   * update ({@link #softTtl}) that extends one side of a live entry.
   *
   * @param hardTtlMs the hard TTL ({@code 0} = permanent, stored as
   *                  {@link Long#MAX_VALUE})
   * @param softTtlMs the soft TTL ({@code 0} = disabled)
   * @return this draft
   * @throws IllegalArgumentException when a duration is negative
   */
  public EntryDraft ttl(long hardTtlMs, long softTtlMs) {
    requireNonNegativeTtls(hardTtlMs, softTtlMs);
    this.hardTtlMs = normalizeHardTtl(hardTtlMs);
    this.softTtlMs = softTtlMs;
    this.hardExpireAtMs = computedHardExpiryAt(this.hardTtlMs);
    this.softExpireAtMs = computedSoftExpiryAt(softTtlMs);
    return this;
  }

  /**
   * Set the active TTL durations without computing expire timestamps — for a rewrite that
   * supplies its own deadlines (the demotion cap). Durations must already be resolved.
   *
   * @param hardTtlMs the active hard TTL ({@code 0} = permanent, stored as
   *                  {@link Long#MAX_VALUE})
   * @param softTtlMs the active soft TTL
   * @return this draft
   * @throws IllegalArgumentException when a duration is negative
   */
  public EntryDraft ttlDurations(long hardTtlMs, long softTtlMs) {
    requireNonNegativeTtls(hardTtlMs, softTtlMs);
    this.hardTtlMs = normalizeHardTtl(hardTtlMs);
    this.softTtlMs = softTtlMs;
    return this;
  }

  /**
   * Validate one resolved hard/soft duration pair. Every TTL pair a draft sets
   * passes through here, so a nonsensical pair cannot reach L1 even when it came
   * from a lazily-evaluated TTL supplier or a SpEL expression rather than a
   * literal.
   *
   * @param hardTtlMs the resolved hard TTL
   * @param softTtlMs the resolved soft TTL
   * @throws IllegalArgumentException when a duration is negative
   */
  static void requireNonNegativeTtls(long hardTtlMs, long softTtlMs) {
    if (hardTtlMs < 0L || softTtlMs < 0L) {
      throw new IllegalArgumentException(
        "TTL must not be negative: hard=" + hardTtlMs + "ms, soft=" + softTtlMs + "ms"
      );
    }
  }

  /**
   * Set all four TTL fields at once — the active hard/soft durations with
   * computed expire timestamps, plus the normal-state baseline recorded for
   * HOT-to-NORMAL reversion. Durations must be resolved to their final intent.
   *
   * @param hardTtlMs       the hard TTL ({@code 0} = permanent, stored as
   *                        {@link Long#MAX_VALUE})
   * @param softTtlMs       the soft TTL ({@code 0} = disabled)
   * @param normalHardTtlMs the normal-state hard TTL baseline ({@code 0} = unset; a later
   *                        transition resolves it to the configured default)
   * @param normalSoftTtlMs the normal-state soft TTL baseline
   * @return this draft
   * @throws IllegalArgumentException when a duration is negative
   */
  public EntryDraft ttl(long hardTtlMs, long softTtlMs, long normalHardTtlMs, long normalSoftTtlMs) {
    requireNonNegativeTtls(normalHardTtlMs, normalSoftTtlMs);
    ttl(hardTtlMs, softTtlMs);
    this.normalHardTtlMs = normalHardTtlMs;
    this.normalSoftTtlMs = normalSoftTtlMs;
    return this;
  }

  /**
   * Set only the soft TTL duration and compute its expire timestamp — the
   * background-refresh pattern (value + soft cadence, hard bound untouched).
   *
   * @param softTtlMs the soft TTL ({@code 0} = disabled)
   * @return this draft
   * @throws IllegalArgumentException when {@code softTtlMs} is negative
   */
  public EntryDraft softTtl(long softTtlMs) {
    requireNonNegativeTtls(0L, softTtlMs);
    this.softTtlMs = softTtlMs;
    this.softExpireAtMs = computedSoftExpiryAt(softTtlMs);
    return this;
  }

  /**
   * Set the value-birth stamp (monotonic seconds-since-boot, see
   * {@link io.github.hyshmily.zeta.util.TimeSource#monotonicSecondsSinceBoot}).
   * Seeded drafts carry the source entry's stamp over (lease preservation);
   * fresh-value construction sites (loads, successful refreshes) stamp
   * {@code now}.
   *
   * @param birthStampSec the birth stamp
   * @return this draft
   */
  public EntryDraft birthStampSec(int birthStampSec) {
    this.birthStampSec = birthStampSec;
    return this;
  }

  /**
   * Override the hard expire timestamp explicitly (custom jitter ratio, lease, or
   * bound-capped scenarios). Call after {@link #ttl} to replace its computed value; the
   * hard TTL duration is unaffected.
   *
   * @param hardExpireAtMs the absolute hard expiry timestamp
   * @return this draft
   */
  public EntryDraft hardExpiryAt(long hardExpireAtMs) {
    this.hardExpireAtMs = hardExpireAtMs;
    return this;
  }

  /**
   * Override the soft expire timestamp explicitly. Call after {@link #ttl} or
   * {@link #softTtl} to replace the computed value; the soft TTL duration is unaffected.
   *
   * @param softExpireAtMs the absolute soft expiry timestamp
   * @return this draft
   */
  public EntryDraft softExpiryAt(long softExpireAtMs) {
    this.softExpireAtMs = softExpireAtMs;
    return this;
  }

  /**
   * Set both expire timestamps explicitly — see {@link #hardExpiryAt} and
   * {@link #softExpiryAt}.
   *
   * @param hardExpireAtMs the absolute hard expiry timestamp
   * @param softExpireAtMs the absolute soft expiry timestamp
   * @return this draft
   */
  public EntryDraft expiryAt(long hardExpireAtMs, long softExpireAtMs) {
    this.hardExpireAtMs = hardExpireAtMs;
    this.softExpireAtMs = softExpireAtMs;
    return this;
  }

  /**
   * Recompute both expire timestamps from the current TTL durations, leaving
   * the durations themselves untouched — the sync-refresh pattern (fresh
   * value, same cadence, re-armed expiry).
   *
   * @return this draft
   */
  public EntryDraft rearmExpiry() {
    this.hardExpireAtMs = computedHardExpiryAt(this.hardTtlMs);
    this.softExpireAtMs = computedSoftExpiryAt(this.softTtlMs);
    return this;
  }

  /**
   * Materialize the draft as an immutable {@link CacheEntry}. The draft has
   * already rejected negative durations at the setters that accept them; the
   * entry constructor owns the packed-field invariants (TTL range) and re-checks
   * them once here.
   *
   * @return a new {@link CacheEntry} with the draft's logical fields
   */
  public CacheEntry build() {
    return new CacheEntry(
      value,
      dataVersion,
      decision,
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      keyState,
      normalHardTtlMs,
      normalSoftTtlMs,
      birthStampSec
    );
  }

  /**
   * The draft-level permanent sentinel: every hard TTL setter stores
   * {@link Long#MAX_VALUE} for {@code 0}, so a later rebuild that resolves the
   * duration again cannot silently turn a permanent entry into a default-TTL one.
   *
   * @param hardTtlMs the resolved hard TTL ({@code 0} = permanent)
   * @return the duration to store
   */
  private static long normalizeHardTtl(long hardTtlMs) {
    return hardTtlMs == 0L ? Long.MAX_VALUE : hardTtlMs;
  }

  private long computedHardExpiryAt(long hardTtlMs) {
    return arithmetic.hardExpireAt(hardTtlMs);
  }

  private long computedSoftExpiryAt(long softTtlMs) {
    return arithmetic.softExpireAt(softTtlMs);
  }
}
