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
import java.util.function.ToLongFunction;
import org.jspecify.annotations.Nullable;

/**
 * The authoritative HOT set: who is hot, for how long, and under which rule.
 *
 * <p>
 * Three invariants hold here (design decisions: worker pipeline):
 *
 * <ol>
 * <li><b>Seat conservation.</b> {@code permits.availablePermits() + hotKeys.size() == capacity} at
 * every quiescent point. A promotion acquires before it inserts; a cool-down releases only when
 * {@code hotKeys.remove(key, entry)} reports that <em>this</em> thread removed <em>that</em> entry.
 * The two-argument remove is the single commit point: a one-argument remove would let a swept check
 * and a fired check each release the same seat.</li>
 * <li><b>Event identity.</b> The wheel schedules the entry object itself. Every removal retires it
 * ({@link HE#RETIRED}) under the stripe lock, so an evaluation reads the entry's own tombstone
 * instead of hashing the key for a second map lookup. A cooled-and-re-promoted key has a different
 * entry, so a stale event is discarded without a token and without a second release.</li>
 * <li><b>Version causality.</b> The decision version is allocated inside the lock that performs the
 * transition, so version order equals transition order even though the send happens later on
 * another thread.</li>
 * </ol>
 *
 * <p>
 * Both mutations and promotion take a per-key stripe lock; {@link #isHot} and {@link #isCurrent} are
 * lock-free map reads. All writers of the map take the same stripe, which makes "check then insert"
 * and "check then remove" safe without a second structure. A stripe holder may then take the owning
 * wheel's lock (stripe → wheel); the wheel never takes a stripe while holding its own lock, so the
 * order cannot invert.
 */
public class HotKeyRegistry {

  /** What one due check decided; the caller owns the mutation and the metrics. */
  private enum KeyStateTransition {
    /** The rule cools on its fixed lifetime. */
    TTL_EXPIRED,
    /**
     * The window rose back to the cool band; the dwell clock resets and the check
     * re-arms.
     */
    QPS_RECOVERED,
    /** The key stayed below the band for the whole dwell. */
    QPS_EXHAUSTED,
    /** Still below the band, but the dwell has not elapsed. */
    PROBATION,
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
    CoolingWheels wheels
  ) {
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
   * Undoes a promotion whose HOT decision never reached the broker.
   *
   * <p>
   * A decision is produced under the stripe lock that performed its transition but submitted after
   * the lock is released, so the key can cool — or cool and re-promote — before the decision reaches
   * the delivery path. The promotion therefore stands exactly while its entry is still the one in the
   * map; lock-free, so a call that races the removal sees either the live entry or its absence.
   *
   * @param key     the cache key
   * @param version the promotion's decision version
   * @return {@code true} while this exact promotion owns the key's seat
   */
  public boolean isCurrent(String key, long version) {
    HE entry = hotKeys.get(key);
    return entry != null && entry.version() == version;
  }

  /**
   * Promotes a key to HOT if a seat is free, and schedules its first check.
   *
   * <p>
   * The binding is the one {@code RuleResolver.resolveIfEligible} already checked against the
   * governing rule's {@code hotCount}; the registry takes only the seat/lifecycle decision and never
   * re-resolves or re-binds a rule. The returned entry is the promotion: a caller that holds it marks
   * the promotion submitted and builds the HOT decision without a second map lookup.
   *
   * @param key     the cache key
   * @param binding the eligible rule binding the key was resolved under
   * @param now     the monotonic sample the report batch was evaluated at
   * @return the committed entry, or {@code null} when the key is already hot or no seat is available
   */
  @Nullable
  public HE promote(String key, RuleBinding binding, long now) {
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
   * Promotes under the key's stripe lock: takes the seat, schedules the
   * entry's first check and then publishes the entry. A failure after the seat is
   * taken returns the seat and retires the entry before propagating.
   *
   * <p>
   * The schedule precedes the map publication, so an entry visible to the
   * lock-free repair scan already has a wheel owner.
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
    TimerWheel wheel = wheels.select(hash);
    try {
      long version = decisionVersion.incrementAndGet();
      entry = new HE(key, binding, now, version);
      wheel.schedule(entry);
      hotKeys.put(key, entry);
      binding.recordPromotion();
      return entry;
    } catch (RuntimeException | Error e) {
      if (entry != null) {
        hotKeys.remove(key, entry);
        entry.retire();
      }

      permits.release();
      if (entry != null) {
        wheel.deschedule(entry);
      }
      throw e;
    }
  }

  /**
   * Undoes a promotion whose HOT decision never reached the broker.
   *
   * <p>
   * Called for a synchronous enqueue refusal and for every terminal drop on the delivery path. The
   * HOT never reached any App, so this is not a rollback of a published state — it is "the promotion
   * never happened". Without it a dropped HOT would hold its seat until the key cooled on its own,
   * and under {@code COOL_FOR_LOW_QPS} a key that stays hot never cools: enough drops would fill the
   * seat cap and disable detection. Identity-guarded, so a concurrent cool-and-re-promote cannot be
   * undone by a stale failure; the promotion is recorded as aborted, keeping
   * {@code promotions == cools + aborted + active} checkable.
   *
   * <p>The lock-free pre-check is exact, not a heuristic: decision versions are monotonic, an entry's
   * version is final, and a version's insert is visible to the delivery path before any abort of that
   * version can be observed, so a version that is not current can never become current again — the
   * same argument {@code TimerWheel.deschedule} relies on. Most delivery-path aborts are no-ops for a
   * superseded or already-cooled key, and those no longer take the stripe lock.
   *
   * @param key     the cache key
   * @param version the decision version of the promotion to undo
   */
  public void abort(String key, long version) {
    if (!isCurrent(key, version)) {
      return;
    }
    int hash = spread(key);
    ReentrantLock lock = stripe(hash);
    lock.lock();
    try {
      HE entry = hotKeys.get(key);
      if (entry != null && entry.version() == version && removeAndRelease(entry, wheels.select(hash))) {
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
   * The fired entry is re-checked under the lock: a cool or an abort since it was scheduled retired
   * it, and the tombstone discards the event without a second map lookup. A concurrent re-arm that
   * moved {@code nextCheckAt} forward makes this evaluation re-link the fresh deadline instead of
   * reading the window. The owner also clears the node's {@code DETACHED} hand-off here, so a repair
   * that lost the stripe race sees a decided entry instead of re-linking it behind this evaluation.
   *
   * <p>
   * The re-arm and the cool-down both move the entry's wheel node inside this critical section, which
   * makes the wheel's deadline, the map membership and the seat permit one atomic transition. The
   * check records its lateness against the deadline this call is about to replace.
   *
   * @param fired the entry the wheel fired; must have come from this registry
   * @return the COOL decision when this call released the seat, otherwise {@code null}
   */
  @Nullable
  public ZetaDecision evaluate(HE fired) {
    int hash = spread(fired.key());
    ReentrantLock lock = stripe(hash);
    TimerWheel wheel = wheels.select(hash);
    lock.lock();
    try {
      // The wheel handed the node over as DETACHED and left it that way: a concurrent repair must
      // still defer to this evaluation. The stripe lock now serializes the two, so the owner can
      // clear the state before re-linking or classifying the entry.
      if (fired.wheelState == TimerWheel.DETACHED) {
        fired.wheelState = TimerWheel.UNSCHEDULED;
      }
      if (fired.retired()) {
        return null;
      }

      long now = TimeSource.monotonicMillis();
      if (now < fired.nextCheckAt) {
        // A concurrent re-arm moved the deadline after the wheel detached the node. Re-link the
        // fresh deadline; the node is UNSCHEDULED, so this is the schedule, not a move.
        wheel.reschedule(fired);
        return null;
      }

      // The check is due and this call owns it. Sample the lateness against the deadline that is
      // about to be replaced; a concurrent repair cannot move it under the same stripe lock.
      metrics.recordCoolingLateness(Math.max(0L, now - fired.nextCheckAt));

      RuleBinding binding = fired.binding();
      return switch (nextTransition(fired, binding, now)) {
        case TTL_EXPIRED -> markTerminal(fired, wheel, WorkerDetectionMetrics.CoolReason.EXPIRE);
        case QPS_RECOVERED -> {
          rearmNextProbe(wheel, fired, binding.rule().firstDue(now), HE.NOT_BELOW);
          metrics.countRecheck();
          yield null;
        }
        case QPS_EXHAUSTED -> markTerminal(fired, wheel, WorkerDetectionMetrics.CoolReason.LOW_QPS);
        case PROBATION -> {
          long belowSince = fired.belowSince == HE.NOT_BELOW ? now : fired.belowSince;
          rearmNextProbe(wheel, fired, binding.rule().nextDue(now, belowSince), belowSince);
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
  private KeyStateTransition nextTransition(HE entry, RuleBinding binding, long now) {
    HotKeyRule rule = binding.rule();
    if (rule.isExpire()) {
      return KeyStateTransition.TTL_EXPIRED;
    }

    long sum = windowSum.applyAsLong(entry.key());
    if (sum >= rule.coolCount()) {
      return KeyStateTransition.QPS_RECOVERED;
    }

    long belowSince = entry.belowSince == HE.NOT_BELOW ? now : entry.belowSince;
    return now - belowSince >= rule.lowQpsForMs() ? KeyStateTransition.QPS_EXHAUSTED : KeyStateTransition.PROBATION;
  }

  /**
   * Re-links every entry whose registered check is missing.
   *
   * <p>
   * An unscheduled node has no wheel owner, so the scan re-links it on the first sweep. A node still
   * linked in a wheel is only late — moving it would take every stripe and wheel lock again while
   * the wheel thread is already catching up, amplifying exactly the pause that made it late. A
   * detached node is already in the owner's batch and is left alone. The scan is only a hint: the
   * stripe lock re-checks the predicate, so a check the owner handled between scan and lock is left
   * alone. The wheel stays the only evaluator; a dead wheel owner cannot be replaced here, and the
   * App's hot hard TTL bounds the keys it held.
   *
   * @return the number of lost checks the scan re-linked
   */
  public int repair() {
    int repaired = 0;
    for (HE candidate : hotKeys.values()) {
      if (!needsRepair(candidate)) {
        continue;
      }

      int hash = spread(candidate.key());
      ReentrantLock lock = stripe(hash);
      lock.lock();
      try {
        if (candidate.retired() || !needsRepair(candidate)) {
          continue;
        }
        // The wheel is the only evaluator: re-link the lost node and wake its owner. A node the
        // owner detached between the check and the wheel lock is a no-op, not a repair.
        if (wheels.select(hash).reschedule(candidate)) {
          repaired++;
        }
      } finally {
        lock.unlock();
      }
    }
    // A late wheel may have missed its own wake while it was busy; one signal per sweep bounds
    // that latency without re-linking a single still-owned node.
    wheels.signalAll();
    return repaired;
  }

  /**
   * Whether the entry's check is lost. Only {@link TimerWheel#UNSCHEDULED} means no owner: a node
   * linked in a bucket or detached into the owner's batch is still the wheel's responsibility.
   *
   * @param entry the candidate entry
   * @return {@code true} when the scan should re-link the entry
   */
  @SuppressWarnings("all")
  private static boolean needsRepair(HE entry) {
    return entry.wheelState == TimerWheel.UNSCHEDULED;
  }

  /** @return the number of keys currently holding a seat */
  public int size() {
    return hotKeys.size();
  }

  /** @return the seat capacity */
  public int capacity() {
    return capacity;
  }

  /** @return cumulative promotions refused because every seat was taken */
  public long rejected() {
    return rejected.sum();
  }

  /**
   * Free seats. With {@link #size()} this must always sum to
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
   * Called under the key lock. The deadline and dwell start commit only when the wheel links the
   * node; a failed move restores both fields, so the entry stays unscheduled and overdue for the
   * repair pass instead of hiding behind a future deadline.
   *
   * @param wheel      the entry's wheel
   * @param entry      the entry
   * @param due        the next deadline, monotonic milliseconds
   * @param belowSince the dwell start to publish, or {@link HE#NOT_BELOW}
   */
  private void rearmNextProbe(TimerWheel wheel, HE entry, long due, long belowSince) {
    long previousDue = entry.nextCheckAt;
    long previousBelowSince = entry.belowSince;
    entry.nextCheckAt = due;
    entry.belowSince = belowSince;
    try {
      wheel.reschedule(entry);
    } catch (RuntimeException | Error e) {
      entry.nextCheckAt = previousDue;
      entry.belowSince = previousBelowSince;
      throw e;
    }
  }

  /**
   * Releases the seat, if this call is the one that removes the entry. The two-argument remove is
   * the whole guard: {@code false} means a competing due check already committed the cool-down, and
   * this call must not release a second permit. Called under the key lock.
   *
   * @return the cool-down decision, or {@code null} when the entry was already gone
   */
  @Nullable
  private ZetaDecision markTerminal(HE entry, TimerWheel wheel, WorkerDetectionMetrics.CoolReason reason) {
    if (!removeAndRelease(entry, wheel)) {
      return null;
    }
    entry.binding().recordCool();
    metrics.countCoolReason(reason);
    return ZetaDecision.cool(entry.key(), decisionVersion.incrementAndGet());
  }

  /**
   * The one seat-release protocol: unlink the entry from the map, retire it, return its permit and
   * deschedule its wheel node. The identity-matched map removal is the commit point — a concurrent
   * cool-down or abort that lost it must not release a second permit. Called under the key's stripe
   * lock; the wheel node is removed after the permit so the seat is released exactly once whatever
   * later step throws.
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
