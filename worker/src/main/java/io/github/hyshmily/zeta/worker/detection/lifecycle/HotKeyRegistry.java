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
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.worker.detection.rule.HotKeyRule;
import io.github.hyshmily.zeta.worker.detection.rule.RuleBinding;
import io.github.hyshmily.zeta.worker.metrics.WorkerDetectionMetrics;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;
import org.jspecify.annotations.Nullable;

/**
 * The authoritative HOT set: who is hot, for how long, and under which rule.
 *
 * <p>
 * Three invariants hold here and nowhere else (design decisions: worker
 * pipeline):
 *
 * <ol>
 * <li><b>Seat conservation.</b>
 * {@code permits.availablePermits() + hotKeys.size() == capacity} at every
 * quiescent
 * point. A promotion acquires before it inserts; a cool-down releases only when
 * {@code hotKeys.remove(key, entry)}
 * reports that <em>this</em> thread removed <em>that</em> entry. The
 * two-argument remove is the single commit point — a
 * one-argument remove would let a swept check and a fired check each release
 * the same seat, because both can hold the
 * entry reference and enter the stripe lock one after the other.</li>
 * <li><b>Event identity.</b> The wheel schedules the entry object itself; a due
 * event carries that object. Every
 * removal retires it ({@link HE#RETIRED}) under the stripe lock, so an
 * evaluation reads the entry's own
 * tombstone instead of hashing the key for a second map lookup. A
 * cooled-and-re-promoted key has a different entry, so
 * a stale event is discarded without a token and without a second release.</li>
 * <li><b>Version causality.</b> The decision version is allocated inside the
 * lock that performs the transition, so
 * version order equals transition order even though the send happens later on
 * another thread.</li>
 * </ol>
 *
 * <p>
 * Both mutations and promotion take a per-key stripe lock; {@link #isHot} and
 * {@link #isCurrent} are lock-free map
 * reads. All writers of the map take the same stripe, which is what makes
 * "check then insert" and "check then remove"
 * safe without a second structure. A stripe holder may then take the owning
 * wheel's lock to schedule or deschedule the
 * entry (stripe → wheel); the wheel never takes a stripe while holding its own
 * lock, so the order cannot invert.
 */
public class HotKeyRegistry {

  /**
   * A cool-down transition: the decision to deliver and whether the HOT promotion
   * it replaced had
   * not yet reached the dispatcher when the COOL was created.
   *
   * <p>
   * The flag is necessary because a promotion and its submission are separate
   * steps: a cool-down
   * can remove the entry before the report thread submits the HOT. Such a COOL
   * would demote a key
   * the cluster was never told was hot, so the funnel may elide it when it has no
   * accepted record
   * for the key.
   *
   * @param decision           the COOL decision
   * @param replacedPendingHot whether the replaced promotion's HOT never entered
   *                           the dispatcher
   */
  public record CoolDecision(ZetaDecision decision, boolean replacedPendingHot) {
  }

  /**
   * How far past its deadline a check may be before the repair sweep treats it as
   * lost.
   *
   * <p>
   * A check that reaches its deadline is normally taken by the owning wheel
   * thread within the hand-off latency.
   * Repairing inside that window would force a schedule that is merely late and
   * report the cool-down as
   * {@link WorkerDetectionMetrics.CoolReason#SWEEP} instead of its real cause. A
   * genuinely lost check is late by one
   * sweep interval either way, so the grace adds nothing beyond the delay the
   * sweep cadence already allows.
   */
  private static final long REPAIR_GRACE_MS = 1_000L;

  /** What one due check decided; the caller owns the mutation and the metrics. */
  private enum DueVerdict {
    /** The rule cools on its fixed lifetime. */
    EXPIRE,
    /**
     * The window rose back to the cool band; the dwell clock resets and the check
     * re-arms.
     */
    RECOVERED,
    /** The key stayed below the band for the whole dwell. */
    COOL,
    /** Still below the band, but the dwell has not elapsed. */
    REARM,
  }

  /** Stripe bounds: a power of two so the index is a mask. */
  private static final int MIN_STRIPES = 256;
  private static final int MAX_STRIPES = 4_096;

  private final ToLongFunction<String> windowSum;
  private final WorkerDetectionMetrics metrics;
  private final AtomicLong decisionVersion;
  private final CoolingWheels wheels;

  private final ConcurrentHashMap<String, HE> hotKeys = new ConcurrentHashMap<>();
  private final LongAdder rejected = new LongAdder();
  private final ReentrantLock[] stripes;
  private final Semaphore permits;
  private final int stripeMask;
  private final int capacity;

  /**
   * Creates the registry.
   *
   * @param capacity        HOT seats ({@code >= 1})
   * @param metrics         detection meters (a meter-less instance is a no-op)
   * @param decisionVersion Worker-wide causal version allocator
   * @param windowSum       read-only window probe, normally
   *                        {@code detector::windowSum}
   * @param wheels          the cool-down wheels the entries are scheduled on
   */
  public HotKeyRegistry(
      int capacity,
      WorkerDetectionMetrics metrics,
      AtomicLong decisionVersion,
      ToLongFunction<String> windowSum,
      CoolingWheels wheels) {
    if (capacity < 1) {
      throw new IllegalArgumentException("max-active-hot-keys must be >= 1");
    }
    this.wheels = wheels;
    this.metrics = metrics;
    this.capacity = capacity;
    this.windowSum = windowSum;
    this.decisionVersion = decisionVersion;
    this.permits = new Semaphore(capacity, false);
    // Size the stripe array to the seat count instead of leaving a 10k-seat
    // registry on 256 stripes (~39 keys per
    // stripe): the lock is held across the LOW_QPS window read and the wheel
    // re-arm, so a collision serializes
    // unrelated keys. The bounds keep a tiny deployment from allocating 4k locks
    // and a huge one from allocating a lock
    // array larger than it can use.
    int wanted = Math.min(MAX_STRIPES, Math.max(MIN_STRIPES, capacity));
    int stripeCount = Integer.highestOneBit(wanted - 1) << 1;
    this.stripeMask = stripeCount - 1;
    this.stripes = new ReentrantLock[stripeCount];
    for (int i = 0; i < stripeCount; i++) {
      stripes[i] = new ReentrantLock();
    }
  }

  /**
   * Whether the key currently holds a seat. Lock-free.
   *
   * @param key the cache key
   * @return {@code true} when an entry exists
   */
  public boolean isHot(String key) {
    return hotKeys.containsKey(key);
  }

  /**
   * Whether the registry still holds the promotion identified by {@code version}.
   *
   * <p>
   * A decision is produced under the stripe lock that performed its transition
   * but submitted
   * after the lock is released, so the key can cool — or cool and re-promote —
   * before the decision
   * reaches the delivery path. This is the authoritative test for such a
   * decision: the promotion
   * stands exactly while its entry is still the one in the map. Lock-free; a call
   * that races the
   * removal sees either the live entry or its absence.
   *
   * @param key     the cache key
   * @param version the promotion's decision version
   * @return {@code true} while this exact promotion owns the key's seat
   */
  public boolean isCurrent(String key, long version) {
    HE entry = hotKeys.get(key);
    return entry != null && entry.promotedVersion() == version;
  }

  /**
   * Promotes a key to HOT if a seat is free, and schedules its first check.
   *
   * <p>
   * The binding is the one {@code RuleResolver.resolveIfEligible} already checked
   * against the governing rule's
   * {@code hotCount}; the registry therefore takes only the seat/lifecycle
   * decision and never re-resolves or re-binds a
   * rule.
   *
   * <p>
   * The returned entry is the promotion: its identity is the seat's identity, and
   * a caller that holds it marks the
   * promotion submitted and builds the HOT decision without a second map lookup.
   * A key that is already hot, or a
   * promotion the seat bound refuses, returns {@code null}.
   *
   * @param key     the cache key
   * @param binding the eligible rule binding the key was resolved under
   * @param now     the monotonic sample the report batch was evaluated at
   * @return the committed entry, or {@code null} when the key is already hot or
   *         no seat is available
   */
  @Nullable
  public HE promoteEntry(String key, RuleBinding binding, long now) {
    int hash = spread(key);
    ReentrantLock lock = stripe(hash);
    lock.lock();
    try {
      return promoteLocked(key, binding, now, hash);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Promotes under the key's stripe lock: takes the seat, publishes the entry and
   * schedules its
   * first check. A failure after the seat is taken returns the seat and retires
   * the entry before
   * propagating.
   *
   * @param key     the cache key
   * @param binding the eligible rule binding
   * @param now     the monotonic sample
   * @param hash    the key's spread hash, reused for the wheel selection
   * @return the committed entry, or {@code null} when the key is already hot or
   *         no seat is available
   */
  @Nullable
  private HE promoteLocked(String key, RuleBinding binding, long now, int hash) {
    if (hotKeys.containsKey(key)) {
      return null;
    }
    if (!permits.tryAcquire()) {
      rejected.increment();
      return null;
    }

    HE entry = null;
    try {
      long version = decisionVersion.incrementAndGet();
      entry = new HE(key, binding, now, version);
      hotKeys.put(key, entry);
      // Resolved after the seat is committed: a refused promotion must not pay for
      // the wheel
      // lookup, and the failure path re-selects the same wheel from the same hash.
      wheels.select(hash).schedule(entry);
      binding.recordPromotion();
      return entry;
    } catch (RuntimeException | Error e) {
      // The seat was taken but the entry never became visible: give it back before
      // propagating.
      // Retire the reference as well, so a schedule that linked before throwing
      // cannot leave a
      // live-looking node behind. The whole post-acquire block is covered, including
      // the
      // allocation, because Error is part of the contract.
      if (entry != null) {
        hotKeys.remove(key, entry);
        entry.retire();
        wheels.select(hash).deschedule(entry);
      }
      permits.release();
      throw e;
    }
  }

  /**
   * Undoes a promotion whose HOT decision never reached the broker.
   *
   * <p>
   * Called for a synchronous enqueue refusal and for every terminal drop on the
   * delivery path (residency expiry, failed
   * publish, supersession, shutdown drain). The HOT never reached any App, so
   * this is not a rollback of a published
   * state — it is "the promotion never happened". Without it a dropped HOT would
   * hold its seat until the key cooled on
   * its own, and under {@code COOL_FOR_LOW_QPS} a key that stays hot never cools:
   * enough drops would fill the seat cap
   * and silently disable detection. A HOT that did reach the broker is not undone
   * by a caller-side failure; the next
   * report's HOT carries a newer version.
   *
   * <p>
   * Identity-guarded: the entry is removed only when it is still the one this
   * promotion created, so a concurrent
   * cool-and-re-promote cannot be undone by a stale failure. The promotion is
   * recorded as aborted, which keeps
   * {@code promotions == cools + aborted + active} checkable.
   *
   * @param key     the cache key
   * @param version the {@code promotedVersion} of the promotion to undo
   */
  public void abortPromotion(String key, long version) {
    int hash = spread(key);
    ReentrantLock lock = stripe(hash);
    lock.lock();
    try {
      HE entry = hotKeys.get(key);
      if (entry != null && entry.promotedVersion() == version && removeAndRelease(entry, wheels.select(hash))) {
        entry.binding().recordAbort();
        metrics.countAbortedPromotion();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Evaluates one due check, applying its verdict.
   *
   * <p>
   * The only transition point besides {@link #promoteEntry} and
   * {@link #abortPromotion}. The fired entry is re-checked
   * under the lock: a cool or an abort since it was scheduled retired it, and the
   * tombstone discards the event without
   * a second map lookup. A {@code forced} evaluation is the sweep's repair pass,
   * but it does not skip the deadline:
   * the sweep's outer check is only a hint, and a wheel owner or an earlier
   * repair may have re-armed the entry before
   * this call took the stripe. Re-checking under the lock is what keeps one due
   * check from being evaluated twice —
   * the second evaluation would re-read the window and move {@code nextCheckAt}
   * again.
   *
   * <p>
   * The re-arm and the cool-down both move the entry's wheel node inside this
   * critical section. That is what makes
   * the wheel's deadline, the map membership and the seat permit one atomic
   * transition; the wheel's advance path
   * detaches under its own lock and evaluates after releasing it, so the two
   * locks never nest in the opposite order.
   *
   * <p>
   * A wheel-fired check ({@code forced == false}) records its lateness against
   * the deadline this call is about to
   * replace. The repair sweep is excluded: its one-second grace is deliberate.
   *
   * @param fired  the entry the wheel fired; must have come from this registry
   * @param forced {@code true} for the sweep's repair pass
   * @return the cool-down decision when this call released the seat, otherwise
   *         {@code null}
   */
  @Nullable
  public CoolDecision evaluate(HE fired, boolean forced) {
    int hash = spread(fired.key());
    ReentrantLock lock = stripe(hash);
    TimerWheel wheel = wheels.select(hash);
    lock.lock();
    try {
      if (fired.retired()) {
        return null;
      }

      long now = TimeSource.monotonicMillis();
      if (now < fired.nextCheckAt) {
        wheel.reschedule(fired);
        return null;
      }

      if (!forced) {
        // The check is due and this call owns it. Sample the lateness against the
        // deadline that is about to be
        // replaced; a concurrent repair cannot move it under the same stripe lock. The
        // repair sweep's grace is
        // deliberate, so only a wheel-fired check reports lateness.
        metrics.recordCoolingLateness(Math.max(0L, now - fired.nextCheckAt));
      }

      RuleBinding binding = fired.binding();
      return switch (classifyDue(fired, binding, now)) {
        case EXPIRE -> coolLocked(
            fired,
            wheel,
            forced ? WorkerDetectionMetrics.CoolReason.SWEEP : WorkerDetectionMetrics.CoolReason.EXPIRE);
        case RECOVERED -> {
          fired.belowSince = HE.NOT_BELOW;
          rearm(wheel, fired, binding.rule().firstDue(now));
          metrics.countRecheck();
          yield null;
        }
        case COOL -> coolLocked(
            fired,
            wheel,
            forced ? WorkerDetectionMetrics.CoolReason.SWEEP : WorkerDetectionMetrics.CoolReason.LOW_QPS);
        case REARM -> {
          if (fired.belowSince == HE.NOT_BELOW) {
            fired.belowSince = now;
          }
          rearm(wheel, fired, binding.rule().nextDue(now, fired.belowSince));
          yield null;
        }
      };
    } finally {
      lock.unlock();
    }
  }

  /**
   * Decides what a due check should do without mutating the entry. EXPIRE needs
   * no window read; a LOW_QPS check reads
   * the window once, and an unset dwell start is treated as "starts now" so the
   * caller persists it only when the key
   * actually stays hot.
   *
   * @param entry   the due entry
   * @param binding the binding captured at promotion
   * @param now     the monotonic sample
   * @return the verdict the caller applies
   */
  private DueVerdict classifyDue(HE entry, RuleBinding binding, long now) {
    HotKeyRule rule = binding.rule();
    if (rule.isExpire()) {
      return DueVerdict.EXPIRE;
    }

    long sum = windowSum.applyAsLong(entry.key());
    if (sum >= rule.coolCount()) {
      return DueVerdict.RECOVERED;
    }

    long belowSince = entry.belowSince == HE.NOT_BELOW ? now : entry.belowSince;
    return now - belowSince >= rule.lowQpsForMs() ? DueVerdict.COOL : DueVerdict.REARM;
  }

  /**
   * Re-evaluates every entry whose registered check is overdue by more than
   * {@link #REPAIR_GRACE_MS}.
   *
   * <p>
   * A stalled or failed wheel advance leaves {@code nextCheckAt} in the past;
   * this pass is what bounds the damage to one
   * sweep interval. Entries with a healthy long timer ({@code cool-after: 10m})
   * are skipped by one volatile comparison.
   * The grace keeps the pass off a check that only just reached its deadline:
   * that one is still owned by a wheel
   * thread, and forcing it would misreport the cool-down's cause as
   * {@code sweep}. The scan is only a hint: {@link
   * #evaluate} re-checks {@code nextCheckAt} under the stripe lock, so a check
   * the wheel owner already handled between
   * the scan and the lock is left alone.
   *
   * @param decisions sink for the cool-down decisions this pass produces
   * @return the number of entries the scan selected as overdue; a concurrent
   *         re-arm can still make an individual
   *         evaluation a no-op
   */
  public int repairOverdue(Consumer<CoolDecision> decisions) {
    long now = TimeSource.monotonicMillis();
    int repaired = 0;
    for (HE candidate : hotKeys.values()) {
      // Subtract from the clock rather than adding to the deadline: a saturated
      // far-future
      // deadline must stay future instead of wrapping negative and looking overdue.
      if (candidate.nextCheckAt > now - REPAIR_GRACE_MS) {
        continue;
      }

      repaired++;
      CoolDecision decision = evaluate(candidate, true);
      if (decision != null) {
        decisions.accept(decision);
      }
    }
    return repaired;
  }

  /** @return the number of keys currently holding a seat */
  public int activeCount() {
    return hotKeys.size();
  }

  /** @return the seat capacity */
  public int capacity() {
    return capacity;
  }

  /** @return cumulative promotions refused because every seat was taken */
  public long rejectedCount() {
    return rejected.sum();
  }

  /**
   * Free seats. With {@link #activeCount()} this must always sum to
   * {@link #capacity()}.
   *
   * @return the semaphore's available permits
   */
  int availableSeats() {
    return permits.availablePermits();
  }

  /**
   * The entry for a key, for diagnostics.
   *
   * @param key the cache key
   * @return the entry, or {@code null} when the key is not hot
   */
  @Nullable
  public HE entry(String key) {
    return hotKeys.get(key);
  }

  /**
   * Registers the next check for an entry that stays hot.
   *
   * <p>
   * Called under the key lock. The wheel node moves under its own lock, so the
   * new deadline and the entry's map
   * membership commit together; a concurrent wheel advance either has not
   * detached the entry yet (and sees the new
   * deadline) or has already detached it (and the evaluation is a duplicate that
   * this re-arm makes harmless).
   *
   * @param wheel the entry's wheel
   * @param entry the entry
   * @param due   the next deadline, monotonic milliseconds
   */
  private void rearm(TimerWheel wheel, HE entry, long due) {
    entry.nextCheckAt = due;
    wheel.reschedule(entry);
  }

  /**
   * Releases the seat, if this call is the one that removes the entry.
   *
   * <p>
   * The two-argument remove is the whole guard: {@code false} means another
   * thread (a sweep, a competing due check)
   * already committed the cool-down, and this call must not release a second
   * permit. Called under the key lock.
   *
   * @return the cool-down decision, or {@code null} when the entry was already
   *         gone
   */
  @Nullable
  private CoolDecision coolLocked(HE entry, TimerWheel wheel, WorkerDetectionMetrics.CoolReason reason) {
    if (!removeAndRelease(entry, wheel)) {
      return null;
    }
    entry.binding().recordCool();
    metrics.countCoolReason(reason);
    return new CoolDecision(
        ZetaDecision.cool(entry.key(), decisionVersion.incrementAndGet()),
        !entry.promotionSubmitted());
  }

  /**
   * The one seat-release protocol: unlink the entry from the map, retire it,
   * return its permit and
   * deschedule its wheel node. The identity-matched map removal is the commit
   * point — a concurrent
   * cool-down or abort that lost it must not release a second permit.
   *
   * <p>
   * Called under the key's stripe lock. The wheel node is removed after the
   * permit so every
   * caller releases the seat exactly once, whatever later step throws.
   *
   * @param entry the entry this call believes it owns
   * @param wheel the wheel the entry is scheduled on
   * @return {@code true} when this call removed the entry and released its seat
   */
  private boolean removeAndRelease(HE entry, TimerWheel wheel) {
    if (!hotKeys.remove(entry.key(), entry)) {
      return false;
    }
    entry.retire();
    permits.release();
    wheel.deschedule(entry);
    return true;
  }

  /** The spread of a key's hash, shared by the stripe and wheel selections. */
  private static int spread(String key) {
    int hash = key.hashCode();
    return hash ^ (hash >>> 16);
  }

  private ReentrantLock stripe(int spreadHash) {
    return stripes[spreadHash & stripeMask];
  }
}
