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
package io.github.hyshmily.zeta.worker.detection.lifecycle;

import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.worker.detection.rule.HotKeyRule;
import io.github.hyshmily.zeta.worker.detection.rule.RuleBinding;

/**
 * One HOT seat.
 *
 * <p><b>Membership is state.</b> An entry's presence in the registry's map is the whole of "this
 * key is HOT"; there is no {@code state} enum and no pending/active transition field. Cooling
 * removes the entry, and removal is the single point at which the seat is released.
 *
 * <p><b>The entry is also the cool-down timer's node.</b> Its wheel links, link state and
 * {@code nextCheckAt} deadline belong to its {@link TimerWheel} (design decisions: worker
 * cooling).
 *
 * <p>Fields are written under the key's stripe lock except {@link #promotionSubmitted}, which the
 * submitting thread sets after the dispatcher accepts the promotion; that volatile write is the
 * hand-off a cool-down reads under the stripe. {@link #belowSince} and {@link #nextCheckAt} are
 * additionally {@code volatile} so the endpoint can read them lock-free. The wheel links are
 * guarded by the owning wheel's lock; {@link #wheelState} is written under that lock too, except
 * the owner's final hand-off in {@link TimerWheel#pollDetached()}, which is {@code volatile} for
 * the reason stated there. A stale read of the volatile fields costs a misleading diagnostic, never
 * a wrong decision.
 */
public final class HE {

  /** Sentinel for "not below the LOW_QPS band"; no monotonic clock reaches it. */
  public static final long NOT_BELOW = Long.MAX_VALUE;

  /** Sentinel for "removed from the registry"; no monotonic clock reaches it. */
  static final long RETIRED = Long.MIN_VALUE;

  /** The decision version carried by this promotion. */
  private final long promotedVersion;

  /**
   * Set once the dispatcher has accepted this promotion. A cool-down reads it to record whether the
   * COOL may demote a HOT the cluster never saw; a promotion that cools before its submission is
   * represented by an elided COOL instead of a send.
   */
  private volatile boolean promotionSubmitted;

  /**
   * The binding captured at promotion: the rule that judged the key and the counters of its id.
   * Keeping it in the entry makes a cool-down or an abort two field reads instead of a second
   * lookup through the published rule set.
   */
  private final RuleBinding binding;

  /** Promotion instant on the monotonic axis. {@code COOL_FOR_EXPIRE} counts from here. */
  private final long promotedAt;

  /** The cache key this entry was promoted for; the wheel and the map identity guard both use it. */
  private final String key;

  /** Deadline of the scheduled check, and the timer's variable time. Monotonic milliseconds. */
  volatile long nextCheckAt;

  /**
   * The LOW_QPS dwell start, written under the key's stripe lock and read by the endpoint.
   * {@link #NOT_BELOW} means the key is hot but at or above its cool band, {@link #RETIRED} means
   * the entry left the registry; every other value is the instant it first dropped below the band.
   * Both sentinels are outside any monotonic instant, so zero and negative timestamps stay real.
   */
  volatile long belowSince = NOT_BELOW;

  /** Previous node in the owning wheel's bucket (or pending) list; null when unscheduled. */
  HE wheelPrev;

  /** Next node in the owning wheel's bucket (or pending) list; null when unscheduled. */
  HE wheelNext;

  /**
   * The node's wheel relation: {@link TimerWheel#UNSCHEDULED}, {@link TimerWheel#SCHEDULED} or
   * {@link TimerWheel#DETACHED}.
   *
   * <p>Written under the wheel lock, except the owner's {@code DETACHED -> UNSCHEDULED} hand-off in
   * {@link TimerWheel#pollDetached()} after the node has left every list. {@code volatile} publishes
   * that single unlocked transition to a concurrent {@code reschedule}/{@code deschedule}.
   */
  volatile int wheelState;

  HE(String key, RuleBinding binding, long promotedAt, long promotedVersion) {
    this.key = key;
    this.binding = binding;
    this.promotedAt = promotedAt;
    this.promotedVersion = promotedVersion;
    this.nextCheckAt = binding.rule().firstDue(promotedAt);
  }

  /** The bucket and pending sentinels: a self-linked node that is never scheduled or evaluated. */
  private HE() {
    this.key = null;
    this.binding = null;
    this.promotedAt = 0L;
    this.promotedVersion = 0L;
    this.wheelNext = this;
    this.wheelPrev = this;
  }

  /** Creates the wheel's self-linked bucket/pending sentinel; it is never scheduled or evaluated. */
  static HE sentinel() {
    return new HE();
  }

  /** @return the cache key this entry was promoted for */
  public String key() {
    return key;
  }

  /** @return the rule this key was promoted under */
  public HotKeyRule rule() {
    return binding.rule();
  }

  /** @return the rule and counters this key was promoted under */
  public RuleBinding binding() {
    return binding;
  }

  /** @return promotion instant, monotonic milliseconds */
  public long promotedAt() {
    return promotedAt;
  }

  /** @return the promotion's decision version */
  public long promotedVersion() {
    return promotedVersion;
  }

  /** @return whether the dispatcher has accepted this promotion */
  boolean promotionSubmitted() {
    return promotionSubmitted;
  }

  /**
   * The HOT decision this promotion announces.
   *
   * <p>Built from the entry's own key and version, so a caller that holds the promotion does not
   * re-read either through the registry's map.
   *
   * @return the HOT decision for this entry
   */
  public ZetaDecision hotDecision() {
    return ZetaDecision.hot(key, promotedVersion);
  }

  /**
   * Records that the dispatcher accepted {@link #hotDecision()}.
   *
   * <p>The write lands on this entry object, not on whichever entry the key holds now: a key that
   * cooled (or cooled and re-promoted) during the submission is a different entry, so a late
   * success can never mark a successor as submitted.
   */
  public void markPromotionSubmitted() {
    promotionSubmitted = true;
  }

  /**
   * Withdraws the submitted mark when the dispatcher accepted the promotion but could not enqueue
   * its HOT before shutdown or a failed offer. Only the submitting thread writes this flag.
   */
  public void clearPromotionSubmitted() {
    promotionSubmitted = false;
  }

  /**
   * The LOW_QPS dwell start as exposed to readers.
   *
   * <p>{@link #RETIRED} is an internal tombstone, not a clock reading: a retired entry reports
   * {@link #NOT_BELOW}, the same answer a live entry above its cool band gives. Keeping the
   * translation here lets a caller make one volatile read and subtract without guarding a second
   * sentinel; every other raw value is a real monotonic instant, including zero and negatives.
   *
   * @return the LOW_QPS dwell start, or {@link #NOT_BELOW} when the key is not below the band or the
   *         entry has been retired
   */
  public long belowSince() {
    long since = belowSince;
    return since == RETIRED ? NOT_BELOW : since;
  }

  /** @return whether this entry has been removed from the registry and can only be discarded */
  public boolean retired() {
    return belowSince == RETIRED;
  }

  /**
   * Marks this entry removed. Called under the key's stripe lock, after the identity-matched map
   * removal; the tombstone is what lets a fired event tell a live successor from its own retired
   * reference without a second map lookup.
   */
  void retire() {
    belowSince = RETIRED;
  }

  /** @return the due time of the registered check, monotonic milliseconds */
  public long nextCheckAt() {
    return nextCheckAt;
  }
}
