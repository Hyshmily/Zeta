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

import jakarta.annotation.Nullable;
import org.springframework.util.Assert;

/**
 * The single mutable intermediate representation through which every
 * {@link CacheEntry} is created and modified — one API replacing the former
 * {@code withXxx()} copy family, the hand-written {@code toBuilder()}, the
 * {@code TtlPolicy.applyXxx()} transforms, and the
 * {@code ExpireManager.buildEntry/buildWrappedEntry} factories.
 *
 * <p><b>Entry points.</b>
 * <ul>
 *   <li>{@code ExpireManager.newEntry()} — creation: a blank draft wired to
 *       the manager's TTL arithmetic (production path).</li>
 *   <li>{@code ExpireManager.editEntry(entry)} — modification: a draft seeded
 *       from an existing entry with the same arithmetic (production path). All
 *       untouched fields carry over.</li>
 *   <li>{@link #of(CacheEntry)} — arithmetic-free seeded draft for callers
 *       without a manager (tests, explicit-timestamp rewrites). Explicit
 *       timestamps ({@link #hardExpiryAt}/{@link #softExpiryAt}) always work.
 *       Computed expiry ({@link #ttl}/{@link #softTtl}/{@link #rearmExpiry})
 *       without arithmetic stays provisional: deterministic sentinels
 *       (non-positive hard = permanent, non-positive soft = disabled) apply
 *       immediately, and a positive duration stays pending until
 *       {@link #build()} — which fails fast unless an explicit timestamp
 *       resolved it first (the set-duration-then-override pattern).</li>
 * </ul>
 *
 * <p><b>Value discipline (ADR-0030).</b> {@link #value} takes the value in its
 * <b>stored</b> (already compressed) form. Compression is linear in value size
 * and must run <i>outside</i> the Caffeine bin lock — wrap first via
 * {@code ExpireManager.wrapValue}, then hand the wrapped value to the draft.
 * The draft never compresses, so building inside a {@code compute} callback is
 * always lock-safe.
 *
 * <p><b>TTL semantics.</b> Durations passed to {@link #ttl}/{@link #softTtl}
 * must already be <b>resolved to their final intent</b> (override vs
 * configured default is the caller's business, via {@code TtlPolicy.resolve*}).
 * The computed expiry timestamps follow the {@code to*ExpireTimestamp}
 * contract: a hard TTL of {@code <= 0} means permanent
 * ({@code Long.MAX_VALUE}), a soft TTL of {@code <= 0} means disabled
 * ({@code 0}), and {@code Long.MAX_VALUE} propagates. Callers that need a
 * custom jitter ratio or lease-style timestamps set durations first and then
 * override with the explicit {@link #hardExpiryAt}/{@link #softExpiryAt}.
 *
 * <p><b>Allocation.</b> One draft + one entry per modification (the previous
 * chained {@code withA().withB()...} style allocated one entry per link), and
 * the packed TTL/state fields decode and re-encode exactly once. The draft is
 * single-threaded by design: create it, shape it, {@link #build} it, discard
 * it — typically all inside one Caffeine {@code compute} callback.
 */
public final class EntryDraft {

  /**
   * Duration-to-timestamp arithmetic injected into a draft. Implemented by the
   * cache's TTL policy (jitter-aware, clock-backed); a {@code null} arithmetic
   * disables computed expiry and leaves only explicit timestamps.
   */
  public interface ExpiryArithmetic {
    /**
     * The absolute hard-expiry timestamp for a hard TTL duration.
     * {@code <= 0} means permanent ({@code Long.MAX_VALUE}).
     *
     * @param hardTtlMs the hard TTL duration in milliseconds (already resolved)
     * @return absolute epoch-ms hard expiry timestamp
     */
    long hardExpireAt(long hardTtlMs);

    /**
     * The absolute soft-expiry timestamp for a soft TTL duration.
     * {@code <= 0} means disabled ({@code 0}).
     *
     * @param softTtlMs the soft TTL duration in milliseconds (already resolved)
     * @return absolute epoch-ms soft expiry timestamp, or {@code 0} for disabled
     */
    long softExpireAt(long softTtlMs);
  }

  @Nullable
  private final ExpiryArithmetic arithmetic;

  private @Nullable Object value;
  private long dataVersion;
  private long decisionVersion;
  private @Nullable String decisionNodeId;
  private long decisionEpoch;
  private @Nullable KeyState keyState;
  private long hardTtlMs;
  private long hardExpireAtMs;
  private long softTtlMs;
  private long softExpireAtMs;
  private long normalHardTtlMs;
  private long normalSoftTtlMs;
  /**
   * Provisional computed expiry on an arithmetic-free draft: {@code ttl}/
   * {@code hardTtl}/{@code softTtl}/{@code rearmExpiry} set the flag for the
   * side they could not compute (positive duration, no arithmetic), and an
   * explicit {@code hardExpiryAt}/{@code softExpiryAt}/{@code expiryAt}
   * resolves it. {@code build()} fails fast while a flag is still set — the
   * seeded timestamp must not leak into the entry.
   */
  private boolean hardExpiryPending;
  private boolean softExpiryPending;

  private EntryDraft(@Nullable ExpiryArithmetic arithmetic) {
    this.arithmetic = arithmetic;
  }

  /**
   * A blank draft for creating a fresh entry. Production code goes through
   * {@code ExpireManager.newEntry()} instead, which wires the TTL arithmetic.
   *
   * @param arithmetic the expiry arithmetic, or {@code null} to allow only
   *                   explicit timestamps
   * @return a blank draft with all-logical-zero fields
   */
  public static EntryDraft blank(@Nullable ExpiryArithmetic arithmetic) {
    return new EntryDraft(arithmetic);
  }

  /**
   * A draft seeded from an existing entry, without expiry arithmetic: every
   * field starts as the source entry's, TTL durations may be set, and computed
   * expiry ({@link #ttl}/{@link #softTtl}/{@link #rearmExpiry}) is rejected.
   * The manager-backed variant ({@code ExpireManager.editEntry}) is the
   * production modification path.
   *
   * @param source the entry whose fields to seed from (not modified)
   * @return a seeded draft
   */
  public static EntryDraft of(CacheEntry source) {
    return of(source, null);
  }

  /**
   * A draft seeded from an existing entry with expiry arithmetic.
   *
   * @param source    the entry whose fields to seed from (not modified)
   * @param arithmetic the expiry arithmetic, or {@code null} for explicit
   *                   timestamps only
   * @return a seeded draft
   */
  public static EntryDraft of(CacheEntry source, @Nullable ExpiryArithmetic arithmetic) {
    EntryDraft draft = new EntryDraft(arithmetic);
    draft.value = source.getValue();
    draft.dataVersion = source.getDataVersion();
    draft.decisionVersion = source.getDecisionVersion();
    draft.decisionNodeId = source.getDecisionNodeId();
    draft.decisionEpoch = source.getDecisionEpoch();
    draft.keyState = source.getKeyState();
    draft.hardTtlMs = source.getHardTtlMs();
    draft.hardExpireAtMs = source.getHardExpireAtMs();
    draft.softTtlMs = source.getSoftTtlMs();
    draft.softExpireAtMs = source.getSoftExpireAtMs();
    draft.normalHardTtlMs = source.getNormalHardTtlMs();
    draft.normalSoftTtlMs = source.getNormalSoftTtlMs();
    return draft;
  }

  /**
   * Set the cached value, in its stored (already compressed) form — see the
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
   * Set the data version. The degraded flag is derived from the sign bit
   * (ADR-0019): negative versions are the Snowflake fallback space, so pass a
   * value from the correct space.
   *
   * @param dataVersion the data version for cross-instance sync ordering
   * @return this draft
   */
  public EntryDraft version(long dataVersion) {
    this.dataVersion = dataVersion;
    return this;
  }

  /**
   * Set the Worker decision stamp. A {@code null} stamp is the local origin
   * and equivalent to {@link #clearDecision()}.
   *
   * @param stamp the decision stamp, or {@code null} for a local entry
   * @return this draft
   */
  public EntryDraft decision(@Nullable DecisionStamp stamp) {
    if (stamp == null) {
      return clearDecision();
    }
    this.decisionVersion = stamp.decisionVersion();
    this.decisionNodeId = stamp.decisionNodeId();
    this.decisionEpoch = stamp.decisionEpoch();
    return this;
  }

  /**
   * Clear the Worker decision stamp to the local origin
   * ({@code VERSION_DEFAULT}, no node, epoch 0) — the demotion pattern.
   *
   * @return this draft
   */
  public EntryDraft clearDecision() {
    this.decisionVersion = 0L;
    this.decisionNodeId = null;
    this.decisionEpoch = 0L;
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
   * @param hardTtlMs the hard TTL ({@code <= 0} = permanent)
   * @param softTtlMs the soft TTL ({@code <= 0} = disabled)
   * @return this draft
   */
  public EntryDraft ttl(long hardTtlMs, long softTtlMs) {
    this.hardTtlMs = hardTtlMs;
    this.softTtlMs = softTtlMs;
    this.hardExpireAtMs = computedHardExpiryAt(hardTtlMs);
    this.softExpireAtMs = computedSoftExpiryAt(softTtlMs);
    return this;
  }

  /**
   * Set all four TTL fields at once — the active hard/soft durations with
   * computed expire timestamps, plus the normal-state baseline recorded for
   * HOT-to-NORMAL reversion. Durations must be resolved to their final intent.
   *
   * @param hardTtlMs       the hard TTL ({@code <= 0} = permanent)
   * @param softTtlMs       the soft TTL ({@code <= 0} = disabled)
   * @param normalHardTtlMs the normal-state hard TTL baseline
   * @param normalSoftTtlMs the normal-state soft TTL baseline
   * @return this draft
   */
  public EntryDraft ttl(long hardTtlMs, long softTtlMs, long normalHardTtlMs, long normalSoftTtlMs) {
    ttl(hardTtlMs, softTtlMs);
    this.normalHardTtlMs = normalHardTtlMs;
    this.normalSoftTtlMs = normalSoftTtlMs;
    return this;
  }

  /**
   * Set only the hard TTL duration and compute its expire timestamp — the
   * hard-bound extension pattern (soft cadence untouched).
   *
   * @param hardTtlMs the hard TTL ({@code <= 0} = permanent)
   * @return this draft
   */
  public EntryDraft hardTtl(long hardTtlMs) {
    this.hardTtlMs = hardTtlMs;
    this.hardExpireAtMs = computedHardExpiryAt(hardTtlMs);
    return this;
  }

  /**
   * Set only the soft TTL duration and compute its expire timestamp — the
   * background-refresh pattern (value + soft cadence, hard bound untouched).
   *
   * @param softTtlMs the soft TTL ({@code <= 0} = disabled)
   * @return this draft
   */
  public EntryDraft softTtl(long softTtlMs) {
    this.softTtlMs = softTtlMs;
    this.softExpireAtMs = computedSoftExpiryAt(softTtlMs);
    return this;
  }

  /**
   * Set only the normal-state TTL baseline, leaving the active TTLs and
   * timestamps untouched.
   *
   * @param normalHardTtlMs the normal-state hard TTL baseline
   * @param normalSoftTtlMs the normal-state soft TTL baseline
   * @return this draft
   */
  public EntryDraft normalTtl(long normalHardTtlMs, long normalSoftTtlMs) {
    this.normalHardTtlMs = normalHardTtlMs;
    this.normalSoftTtlMs = normalSoftTtlMs;
    return this;
  }

  /**
   * Override the hard expire timestamp explicitly (custom jitter ratio, lease,
   * or bound-capped scenarios). Call after {@link #ttl} to replace its computed
   * value; the hard TTL duration is unaffected. On an arithmetic-free draft
   * this also resolves a pending computed hard expiry (see {@link #hardTtl}).
   *
   * @param hardExpireAtMs the absolute hard expiry timestamp
   * @return this draft
   */
  public EntryDraft hardExpiryAt(long hardExpireAtMs) {
    this.hardExpireAtMs = hardExpireAtMs;
    this.hardExpiryPending = false;
    return this;
  }

  /**
   * Override the soft expire timestamp explicitly. Call after {@link #ttl} or
   * {@link #softTtl} to replace the computed value; the soft TTL duration is
   * unaffected. On an arithmetic-free draft this also resolves a pending
   * computed soft expiry (see {@link #softTtl}).
   *
   * @param softExpireAtMs the absolute soft expiry timestamp
   * @return this draft
   */
  public EntryDraft softExpiryAt(long softExpireAtMs) {
    this.softExpireAtMs = softExpireAtMs;
    this.softExpiryPending = false;
    return this;
  }

  /**
   * Set both expire timestamps explicitly — see {@link #hardExpiryAt} and
   * {@link #softExpiryAt}. Resolves both pending computed expiries on an
   * arithmetic-free draft.
   *
   * @param hardExpireAtMs the absolute hard expiry timestamp
   * @param softExpireAtMs the absolute soft expiry timestamp
   * @return this draft
   */
  public EntryDraft expiryAt(long hardExpireAtMs, long softExpireAtMs) {
    this.hardExpireAtMs = hardExpireAtMs;
    this.softExpireAtMs = softExpireAtMs;
    this.hardExpiryPending = false;
    this.softExpiryPending = false;
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
   * Materialize the draft as an immutable {@link CacheEntry}. Fails fast while
   * a computed-expiry timestamp is still pending on an arithmetic-free draft
   * (see the pending-field Javadoc); the packed-field invariants
   * (degraded-flag/sign agreement, epoch range, TTL range) are re-validated by
   * the entry constructor.
   *
   * @return a new {@link CacheEntry} with the draft's logical fields
   */
  public CacheEntry build() {
    Assert.state(
      !hardExpiryPending && !softExpiryPending,
      "Computed expiry requires an ExpiryArithmetic; use ExpireManager.newEntry/editEntry or set explicit timestamps"
    );
    return new CacheEntry(
      value,
      dataVersion,
      dataVersion < 0,
      decisionVersion,
      decisionNodeId,
      decisionEpoch,
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      keyState,
      normalHardTtlMs,
      normalSoftTtlMs
    );
  }

  private long computedHardExpiryAt(long hardTtlMs) {
    ExpiryArithmetic arith = arithmetic;
    if (arith != null) {
      return arith.hardExpireAt(hardTtlMs);
    }
    // Deterministic without arithmetic: a non-positive hard TTL is permanent.
    if (hardTtlMs <= 0) {
      return Long.MAX_VALUE;
    }
    // Positive duration, no arithmetic: the timestamp stays provisional — an
    // explicit hardExpiryAt/expiryAt before build() resolves it, otherwise
    // build() fails fast (see the pending-field Javadoc).
    this.hardExpiryPending = true;
    return hardExpireAtMs;
  }

  private long computedSoftExpiryAt(long softTtlMs) {
    ExpiryArithmetic arith = arithmetic;
    if (arith != null) {
      return arith.softExpireAt(softTtlMs);
    }
    // Deterministic without arithmetic: a non-positive soft TTL is disabled.
    if (softTtlMs <= 0) {
      return 0L;
    }
    this.softExpiryPending = true;
    return softExpireAtMs;
  }
}
