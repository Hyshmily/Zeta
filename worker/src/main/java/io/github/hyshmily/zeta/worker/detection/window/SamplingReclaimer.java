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
package io.github.hyshmily.zeta.worker.detection.window;

import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;

/**
 * The admission bound for the detector's window map.
 *
 * <p>
 * One locked sampling pass decides admissions at the soft cap: {@link #tryAdmit} takes the sampler
 * lock with {@code tryLock} only when a new key reaches the cap, walks up to {@code sampleSize}
 * entries, and applies the caller's evictor inside that section. The evictor reports a removal
 * through its return value only; it must not touch this ledger. The lock order is reclaimer → entry
 * monitor for the guarded removal, with the map's bin lock taken after the entry monitor is
 * released; no path takes the reclaimer lock from inside a bin lock.
 *
 * <p>
 * The walk is a Clock-style sweep hand that resumes where the previous pass stopped. A
 * {@link ConcurrentHashMap} iterator pins the table it was created over, so the periodic sweep
 * drops the hand through {@link #releaseHand()} without taking the sampler lock.
 *
 * <p>
 * The soft cap is approximate; the hard bound is a seat ledger. A completed pass that removes
 * nothing opens a short saturation window, so the microsecond-scale pass cannot be outrun by
 * admissions that miss its lock. At the hard bound one uncontended pass may transfer its first
 * reclaimed seat to its own admission.
 *
 * <p>
 * design decisions: worker pipeline
 *
 * @param <V> the map's value type
 */
@Internal
@Slf4j
public final class SamplingReclaimer<V> {

  /**
   * How long a completed pass that found no reclaimable entry keeps the table saturated. The window bounds the bypass
   * to the admissions that race one pass, instead of letting every admission through for a whole sweep interval.
   */
  private static final long SATURATION_COOLDOWN_MS = 50L;

  /**
   * Headroom above the soft cap before an admission is refused outright. One percent keeps the bound useful for small
   * tables; the floor covers a tiny configured cap. The seat ledger makes this a true total bound, not a pre-check
   * against a weakly consistent size.
   */
  private static final long MIN_HARD_HEADROOM = 8L;

  private final ConcurrentHashMap<String, V> map;

  /** Human-readable component name used in the rejection log line. */
  private final String componentName;

  /** Set when an admission at the cap found nothing idle to reclaim. */
  private final AtomicBoolean reliefRequested = new AtomicBoolean();

  /**
   * Monotonic millisecond deadline until which a completed "nothing idle" pass suppresses admissions at the cap;
   * {@link Long#MIN_VALUE} when the last pass released a seat or no pass has run.
   */
  private final AtomicLong saturatedUntilMs = new AtomicLong(Long.MIN_VALUE);

  /** Cumulative idle entries reclaimed to admit a new key at the cap. */
  private final LongAdder capacityEvictions = new LongAdder();

  /**
   * Cumulative new-key admissions rejected by the cap. A key flood rejects from every report thread at once, while
   * readers are only the monitor scrape and the endpoint; {@link LongAdder} spreads those writes across cells instead
   * of bouncing one cache line.
   */
  private final LongAdder rejectedNewKeys = new LongAdder();

  private final ReentrantLock lock = new ReentrantLock();

  /**
   * Live seat count, including seats reserved by admissions still in flight. One window owns one seat; creation
   * reserves a seat atomically, removal releases it. Initialized from the map because callers and tests may hand a
   * prefilled table to the constructor.
   */
  private final AtomicLong usedSeats;

  /**
   * Soft admission cap; {@code <= 0} disables the bound. Fixed after construction.
   */
  private final long maxTrackedKeys;

  /** Soft cap plus headroom, or {@code 0} when the cap is disabled. */
  private final long hardLimit;

  private final int sampleSize;

  /**
   * The live hand; {@code null} before the first sampling pass, after the tail, or after
   * {@link #releaseHand()}. The sampler keeps the iterator in a local while it walks, and only it
   * calls {@link Iterator#next()}; the sweep clears this reference from any thread, so the write is
   * volatile and never touches the iterator itself.
   */
  private volatile Iterator<Map.Entry<String, V>> cursor;

  /**
   * The component-specific idle test the sampling pass applies. It receives the pass's clock
   * sample as a primitive so a caller can pass a stateless method reference instead of a
   * capturing lambda, and returns {@code true} only when this call tombstoned the entry and
   * removed it. The reclaimer owns the seat accounting for that verdict; the evictor must not
   * call {@link #releaseSeat()}.
   *
   * @param <V> the map's value type
   */
  @FunctionalInterface
  public interface IdleEvictor<V> {
    /**
     * @param entry    the sampled entry
     * @param nowSlice the pass's absolute time slice
     * @return {@code true} only when this call tombstoned and removed the entry
     */
    boolean evictIfIdle(Map.Entry<String, V> entry, long nowSlice);
  }

  /** @return cumulative new-key admissions rejected by the cap */
  public long rejectedNewKeys() {
    return rejectedNewKeys.sum();
  }

  /** @return cumulative idle entries reclaimed to admit a new key at the cap */
  public long capacityEvictions() {
    return capacityEvictions.sum();
  }

  /**
   * Creates a reclaimer over the given map.
   *
   * @param componentName  human-readable component name for the rejection WARN
   * @param map            the map to sample
   * @param sampleSize     the maximum number of entries visited per pass
   * @param maxTrackedKeys the admission cap; {@code <= 0} disables it
   */
  public SamplingReclaimer(
    String componentName,
    ConcurrentHashMap<String, V> map,
    int sampleSize,
    long maxTrackedKeys
  ) {
    this.map = map;
    // The first pass creates the hand over the table as it is then, not over the empty table this constructor sees;
    // an iterator created here is exhausted before the first admission.
    this.cursor = null;
    this.sampleSize = sampleSize;
    this.componentName = componentName;
    this.maxTrackedKeys = maxTrackedKeys;
    this.hardLimit = maxTrackedKeys <= 0L ? 0L : hardLimit(maxTrackedKeys);
    this.usedSeats = new AtomicLong(map.size());
  }

  /** The soft cap plus its headroom, saturating instead of overflowing. */
  private static long hardLimit(long cap) {
    long headroom = Math.max(MIN_HARD_HEADROOM, cap / 100L);
    return cap > Long.MAX_VALUE - headroom ? Long.MAX_VALUE : cap + headroom;
  }

  /**
   * Consumes the "an admission at the cap found nothing idle" signal. The periodic scan uses it to run once with the
   * accelerated threshold.
   *
   * @return {@code true} if relief was requested since the previous call
   */
  public boolean consumeReliefRequest() {
    // The periodic sweep calls this once per interval whether or not a rejection happened. Reading first keeps the
    // common empty case from issuing an atomic write; a rejection that lands between the read and the getAndSet is
    // already covered by the sweep currently running with the relief threshold, and the next rejection re-arms it for
    // the following sweep.
    return reliefRequested.getOpaque() && reliefRequested.getAndSet(false);
  }

  /**
   * Decides whether a new key may enter, reserving a seat when it may.
   *
   * <p>
   * A {@code true} verdict owns one seat, not a hint: creation consumes it, and a race that finds a
   * live window already published must return it with {@link #releaseSeat()}. The atomic ledger is
   * the only size pre-check on the fast path, which reserves below the soft cap only; an arrival
   * that loses the last below-cap seat falls through to the sampling path instead of consuming hard
   * headroom without walking the table. The evictor runs under the sampler lock and must not acquire
   * a lock that can reach back into this reclaimer.
   *
   * @param key      the key being admitted; used only for the DEBUG line
   * @param nowSlice the pass's clock sample, passed through to {@code evict}
   * @param evict    removes the candidate when it is still idle under its own guard; returns
   *                 {@code true} only when this call tombstoned and removed the entry
   * @return {@code true} when a seat is now reserved for the caller; creation consumes it, and an
   *         abandoned or failed creation returns it with {@link #releaseSeat()}
   */
  public boolean tryAdmit(String key, long nowSlice, IdleEvictor<V> evict) {
    long cap = maxTrackedKeys;
    if (cap <= 0L) {
      return true;
    }

    if (tryReserveIfBelowCap(cap)) {
      return true;
    }
    if (isSaturated()) {
      return reject(key);
    }
    // Reserve before touching the sampler lock: a contender can take the last hard-headroom seat at any time, and
    // only the thread that already owns a seat is allowed to spend the walk.
    if (reserveSeatIfBelowHardLimit()) {
      if (!lock.tryLock()) {
        // Another admission is sampling; the reservation is this admission's seat. The hard ledger still bounds
        // the bypass.
        return true;
      }
      try {
        // A pass that reached the lock after another admission opened the saturation window skips the walk:
        // the window is the newest evidence, and this reservation already owns the seat the caller consumes.
        if (!isSaturated()) {
          tryRecycleAndAcquireSeat(nowSlice, evict, true);
        }
        return true;
      } finally {
        lock.unlock();
      }
    }
    // The ledger is at the hard bound. One uncontended pass may still reclaim an idle seat; a contender that
    // loses the lock rejects, and a pass that finds nothing opens the saturation window.
    if (!lock.tryLock()) {
      return reject(key);
    }
    try {
      if (isSaturated()) {
        return reject(key);
      }
      return tryRecycleAndAcquireSeat(nowSlice, evict, false) || reserveSeatIfBelowHardLimit() || reject(key);
    } finally {
      lock.unlock();
    }
  }

  /**
   * One locked sampling pass: walk at most {@code sampleSize} entries, apply the caller's evictor,
   * and account for every reclaimed seat. When the admission already owns a reservation, each
   * reclaimed seat returns to the ledger; at the hard bound the first reclaimed seat is transferred
   * to its admission and never exposed to a lock-free reservation while the pass runs. A pass that
   * reclaims nothing opens the saturation window while the table is still at the cap.
   *
   * @param nowSlice the pass's clock sample, passed through to the evictor
   * @param evict    removes the candidate when it is still idle under its own guard
   * @param hasSeat  whether the caller reserved a seat before the pass
   * @return {@code true} when the caller owns a seat after the pass
   */
  private boolean tryRecycleAndAcquireSeat(long nowSlice, IdleEvictor<V> evict, boolean hasSeat) {
    Iterator<Map.Entry<String, V>> it = cursor;
    int steps = Math.min(sampleSize, map.size());
    boolean seated = hasSeat;
    boolean reclaimed = false;
    try {
      for (int i = 0; i < steps; i++) {
        if (it == null || !it.hasNext()) {
          it = map.entrySet().iterator();
          if (!it.hasNext()) {
            cursor = null;
            break;
          }
          cursor = it;
        }
        Map.Entry<String, V> entry = it.next();
        if (evict.evictIfIdle(entry, nowSlice)) {
          capacityEvictions.increment();
          reclaimed = true;
          if (seated) {
            usedSeats.decrementAndGet();
          } else {
            seated = true;
          }
        }
      }

      if (reclaimed) {
        // Both writes are usually no-ops: the saturation window is normally closed, and the table is only still at
        // the cap during a flood.
        if (saturatedUntilMs.getOpaque() != Long.MIN_VALUE) {
          saturatedUntilMs.set(Long.MIN_VALUE);
        }
        if (usedSeats.get() >= maxTrackedKeys) {
          requestRelief();
        }
      } else if (usedSeats.get() >= maxTrackedKeys) {
        saturatedUntilMs.set(TimeSource.monotonicMillis() + SATURATION_COOLDOWN_MS);
        requestRelief();
      }
      return seated;
    } catch (RuntimeException | Error e) {
      if (seated) {
        releaseSeat();
      }
      throw e;
    }
  }

  /**
   * Reserves one seat for a key that already holds a HOT registry seat. The key is not new tracked
   * traffic, so the admission bound does not apply: losing its count would make a live hot key look
   * quiet to {@code COOL_FOR_LOW_QPS}. The overshoot is bounded by the HOT seat count.
   */
  public void reserveHotSeat() {
    if (maxTrackedKeys > 0L) {
      usedSeats.incrementAndGet();
    }
  }

  /**
   * Releases one seat: a removed window's seat, or a reservation that lost the creation race. Must
   * stay lock-free, because callers hold the entry monitor and sometimes the map's bin lock; taking
   * the sampler lock here would invert the reclaimer → entry order. The tombstone or live-window
   * re-check under that monitor makes the release exactly-once.
   */
  public void releaseSeat() {
    if (maxTrackedKeys > 0L) {
      usedSeats.decrementAndGet();
    }
  }

  /**
   * Current live seat count, for diagnostics and tests.
   *
   * @return the number of reserved seats
   */
  public long usedSeats() {
    return maxTrackedKeys > 0L ? usedSeats.get() : map.size();
  }

  /**
   * Reserves one seat from the hard headroom. The soft cap only decides when a sampling pass runs;
   * the below-cap fast path uses {@link #tryReserveIfBelowCap(long)}, so this method never spends
   * headroom on an admission the soft cap could have served.
   *
   * @return {@code true} when this call added one reserved seat
   */
  private boolean reserveSeatIfBelowHardLimit() {
    for (;;) {
      long current = usedSeats.get();
      if (current >= hardLimit) {
        return false;
      }
      if (usedSeats.compareAndSet(current, current + 1)) {
        return true;
      }
    }
  }

  /**
   * Reserves one seat only while the ledger is below the soft cap. Bounding the CAS here keeps a
   * burst at the cap boundary from spending the sampling headroom without running a pass: only the
   * arrivals that actually reach the sampling path may reserve above the cap.
   *
   * @param cap the soft cap
   * @return {@code true} when this call added one reserved seat below the cap
   */
  private boolean tryReserveIfBelowCap(long cap) {
    for (;;) {
      long current = usedSeats.get();
      if (current >= cap) {
        return false;
      }
      if (usedSeats.compareAndSet(current, current + 1)) {
        return true;
      }
    }
  }

  /**
   * Whether a completed pass recently found nothing idle. While {@code true}, an admission at the
   * cap rejects without taking the sampler lock: repeating the walk would only burn a scan to learn
   * the same answer.
   *
   * @return {@code true} while the saturation window is open
   */
  private boolean isSaturated() {
    long until = saturatedUntilMs.get();
    return until != Long.MIN_VALUE && TimeSource.monotonicMillis() < until;
  }

  /**
   * Arms the periodic relief scan; the read-before-write avoids a needless cache-line bounce.
   */
  private void requestRelief() {
    if (!reliefRequested.getOpaque()) {
      reliefRequested.set(true);
    }
  }

  /**
   * Records one cap-rejected new key, arms the accelerated scan and surfaces the flood signal at
   * DEBUG. A sustained rejection means the report cardinality exceeds
   * {@code zeta.worker.max-tracked-keys}; the Worker keeps running either way.
   *
   * @param key the rejected key (used only for the DEBUG line)
   * @return always {@code false}
   */
  @SuppressWarnings("all")
  private boolean reject(String key) {
    requestRelief();
    rejectedNewKeys.increment();
    if (log.isDebugEnabled()) {
      log.debug(
      "New key rejected in {}: tracked-keys cap reached (maxTrackedKeys={}, cumulativeRejected={}, key={})",
      componentName,
      maxTrackedKeys,
      rejectedNewKeys.sum(),
      key
    );
    }
    return false;
  }

  /**
   * Drops the retained sampling hand; the next {@link #tryAdmit} restarts at the map's head.
   *
   * <p>
   * The clear is unconditional, not a {@code tryLock}: the sweep runs exactly when admissions
   * contend the sampler lock, so a {@code tryLock} there would fail exactly when the release is
   * needed and leave the old table pinned for the whole sweep interval. A pass in progress keeps its
   * own local iterator, so clearing the field never touches a running walk.
   */
  public void releaseHand() {
    cursor = null;
  }
}
