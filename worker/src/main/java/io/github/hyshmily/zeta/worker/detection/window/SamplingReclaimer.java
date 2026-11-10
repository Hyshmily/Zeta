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

import com.google.errorprone.annotations.concurrent.GuardedBy;
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
 * Owns the admission-bound state in one place: the configured cap, the rejected/evicted counters, the "relief
 * requested" signal that arms the accelerated periodic scan, the short saturation window that follows a pass finding
 * nothing idle, and the rejection log line.
 *
 * <p>
 * <b>One locked pass.</b> {@link #tryAdmit} takes the sampler lock with {@code tryLock} only when a new key reaches the
 * soft cap, walks up to {@code sampleSize} entries, and applies the caller's evictor inside that section.
 * The evictor re-checks the candidate under the guard that serializes it with mutations and removes it when idle, so
 * one pass decides and reclaims; the caller supplies only the component-specific removal test. The evictor reports a
 * removal through its return value only; it must not touch this ledger. The lock order is reclaimer → entry monitor
 * for the guarded removal, with the map's bin lock taken after the entry monitor is released; no path takes the
 * reclaimer lock from inside a bin lock. Because two samples cannot overlap, the pass's verdict is the current one.
 *
 * <p>
 * The walk is a Clock-style sweep hand that resumes where the previous pass stopped, so a saturated table is covered
 * round-robin instead of re-sampling the same head buckets. It is bounded by {@code min(sampleSize, map.size())}; an
 * exhausted hand is rebuilt at the head. A {@link ConcurrentHashMap} iterator pins the table array it was created
 * over, so the periodic sweep drops the hand through {@link #releaseHand()}.
 *
 * <p>
 * The soft cap is approximate; the hard bound is a seat ledger. A {@code tryLock} failure means another admission is
 * sampling: the holder may be releasing the very seats this arrival needs, so the arrival may proceed, but only by
 * atomically reserving a seat below the hard limit. A fast-path admission reserves only <b>below</b> the cap; once the
 * ledger reaches the cap, an arrival must take the sampling path or wait out the saturation window, so the hard headroom
 * is spent only by admissions that reach that path. A completed pass that removes nothing opens a short saturation
 * window ({@value #SATURATION_COOLDOWN_MS} ms); while it lasts, other new keys at the cap are rejected without
 * another sample. Without that window the microsecond-scale sample could be outrun by nanosecond-scale admissions and the soft
 * cap would become a sustained bypass. The pass's own admission reserves one hard-headroom seat <b>before</b> the
 * walk: the thread that paid for the walk is not the one admission the ledger should refuse while its contenders pass,
 * and a contender cannot take the seat during the scan. Every seat the pass reclaims is returned to the ledger; the
 * reservation is consumed by the admission itself. The verdict is taken only while the ledger is still at the cap: the
 * periodic sweep does not take this lock, so it can release seats during the walk, and an admission must not be
 * rejected on a saturation that no longer holds.
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
   * The live hand; {@code null} before the first sample, after the tail, or after {@link #releaseHand()}.
   */
  @GuardedBy("lock")
  private Iterator<Map.Entry<String, V>> cursor;

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
   * A {@code true} verdict owns one seat, not a hint: creation consumes it, and a race that finds a live window
   * already published must return it with {@link #releaseSeat()}. The atomic ledger is the only size pre-check on the
   * admission fast path; {@code map.size()} is read only by a completed sampling pass, which needs the walk bound and
   * re-checks the table after it. The fast path reserves below the soft cap only; an arrival that loses the last
   * below-cap seat falls through to the sampling path instead of consuming the hard headroom on an admission that
   * never walks the table.
   *
   * <p>
   * The sampling admission reserves one hard-headroom seat before the walk, so a contended admission bypassing the
   * lock cannot take the seat this pass needs and leave the thread that paid for the scan rejected. Every reclaimed
   * seat is returned to the ledger. A pass that removes nothing opens the saturation window for the arrivals behind
   * it but still consumes the reservation it took before sampling. At the hard bound no reservation exists, so one
   * uncontended pass runs without one; it admits only after reclaiming a seat, and a contender that loses the lock
   * rejects.
   *
   * <p>
   * The evictor runs while the sampler lock is held, so it must perform only the component's own guarded removal and
   * must not acquire a lock that can reach back into this reclaimer. It returns {@code true} only when it actually
   * removed the candidate; the reclaimer accounts for that seat itself, so the evictor must not call
   * {@link #releaseSeat()}.
   *
   * @param key      the key being admitted; used only for the DEBUG line
   * @param nowSlice the pass's clock sample, passed through to {@code evict}
   * @param evict    removes the candidate when it is still idle under its own guard; returns {@code true} only when
   *                 this call tombstoned and removed the entry, never for a concurrent removal
   * @return {@code true} when a seat is now reserved for the caller; creation consumes it, and an abandoned admission
   *         returns it with {@link #releaseSeat()}
   */
  public boolean tryAdmit(String key, long nowSlice, IdleEvictor<V> evict) {
    long cap = maxTrackedKeys;
    if (cap <= 0L) {
      return true;
    }

    if (tryReserveBelowCap(cap)) {
      return true;
    }
    if (isSaturated()) {
      return reject(key);
    }
    // Reserve before touching the sample lock: a contender can take the last hard-headroom seat at any time, and
    // only the thread that already owns a seat is allowed to spend the walk.
    if (reserveSeat()) {
      if (!lock.tryLock()) {
        // Another admission is sampling; the reservation is this admission's seat. The hard ledger still bounds
        // the bypass.
        return true;
      }
      try {
        sample(nowSlice, evict);
        return true;
      } catch (RuntimeException | Error e) {
        // Every reclaimed seat was already returned inside the pass; release the admission's reservation too.
        releaseSeat();
        throw e;
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
      sample(nowSlice, evict);
      return reserveSeat() || reject(key);
    } finally {
      lock.unlock();
    }
  }

  /**
   * One locked sampling pass: walk at most {@code sampleSize} entries, apply the caller's evictor, and return every
   * reclaimed seat to the ledger. When the admission could reserve, its own seat was taken before the walk, so it
   * does not compete for a reclaimed seat; at the hard bound it reserves only after the walk. A pass that reclaims
   * nothing opens the saturation window while the table is still at the cap, so the arrivals behind it reject
   * without paying another walk.
   *
   * @param nowSlice the pass's clock sample, passed through to the evictor
   * @param evict    removes the candidate when it is still idle under its own guard
   */
  private void sample(long nowSlice, IdleEvictor<V> evict) {
    boolean reclaimed = false;
    int steps = Math.min(sampleSize, map.size());
    for (int i = 0; i < steps; i++) {
      Map.Entry<String, V> entry = next();
      if (entry == null) {
        break;
      }
      if (evict.evictIfIdle(entry, nowSlice)) {
        capacityEvictions.increment();
        usedSeats.decrementAndGet();
        reclaimed = true;
      }
    }

    if (reclaimed) {
      // Both writes are usually no-ops: the saturation window is normally closed, and the table is only still at the
      // cap during a flood.
      if (saturatedUntilMs.getOpaque() != Long.MIN_VALUE) {
        saturatedUntilMs.set(Long.MIN_VALUE);
      }
      if (usedSeats.get() >= maxTrackedKeys) {
        requestRelief();
      }
    } else if (usedSeats.get() >= maxTrackedKeys) {
      // Nothing was reclaimed and the table is still at the cap: the last completed pass is the current evidence,
      // so the arrivals behind it are rejected without another walk for one short window.
      saturatedUntilMs.set(TimeSource.monotonicMillis() + SATURATION_COOLDOWN_MS);
      // A pass that found nothing idle is the trigger for the accelerated sweep: without the signal, a steady
      // trickle of new keys would keep spending the hard headroom while the periodic scan stays at its slow cadence.
      requestRelief();
    }
  }

  /**
   * Reserves one seat for a key that already holds a HOT registry seat.
   *
   * <p>The key is not new tracked traffic, so the admission bound does not apply: losing its count would make a live
   * hot key look quiet to {@code COOL_FOR_LOW_QPS}. The overshoot above the hard limit is bounded by the HOT seat
   * count, and the seat is still counted so removal can return it.
   */
  public void reserveHotSeat() {
    if (maxTrackedKeys > 0L) {
      usedSeats.incrementAndGet();
    }
  }

  /**
   * Releases one seat: a removed window's seat, or a reservation that lost the creation race.
   *
   * <p>Must stay lock-free. Callers invoke it under the entry monitor and sometimes the map's bin lock, so taking the
   * sampler lock here would invert the reclaimer → entry order. The tombstone or live-window re-check under that same
   * monitor is what makes the release exactly-once.
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
   * Reserves one seat from the hard headroom. The CAS loop is the admission bound shared by every
   * report thread; the soft cap only decides when a sampling pass runs. The below-cap fast path
   * uses {@link #tryReserveBelowCap(long)}, so this method never spends headroom on an admission
   * that could have been served by the soft cap.
   *
   * @return {@code true} when this call added one reserved seat
   */
  private boolean reserveSeat() {
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
  private boolean tryReserveBelowCap(long cap) {
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
   * Whether a completed pass recently found nothing idle. While {@code true}, an admission at the cap rejects without
   * taking the sample lock: the last completed pass is the current evidence, and repeating the walk would only burn a
   * scan to learn the same answer.
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
   * Records one cap-rejected new key, arms the accelerated scan and surfaces the flood signal at DEBUG. A
   * sustained rejection means the report cardinality exceeds the configured bound — the operator lifts
   * {@code zeta.worker.max-tracked-keys} or fixes the key shape; the Worker keeps running either way.
   *
   * @param key the rejected key (used only for the DEBUG line)
   * @return always {@code false}
   */
  private boolean reject(String key) {
    requestRelief();
    rejectedNewKeys.increment();
    if (!log.isDebugEnabled()) {
      // The adder is spread across cells and a key flood rejects from every report thread; a
      // disabled line must not pay the sum() walk and the Long boxing.
      return false;
    }
    log.debug(
      "New key rejected in {}: tracked-keys cap reached (maxTrackedKeys={}, cumulativeRejected={}, key={})",
      componentName,
      maxTrackedKeys,
      rejectedNewKeys.sum(),
      key
    );
    return false;
  }

  /**
   * Drops the retained sampling hand; the next {@link #tryAdmit} restarts at the map's head.
   *
   * <p>
   * A contended {@code tryLock} leaves the hand in place, and the next pass resumes it.
   */
  public void releaseHand() {
    if (lock.tryLock()) {
      try {
        cursor = null;
      } finally {
        lock.unlock();
      }
    }
  }

  /**
   * Returns the next entry of the sweep. An exhausted hand is rebuilt at the head, so the tail boundary left by the
   * previous pass is not mistaken for an empty table. The caller bounds the walk to {@code map.size()}, which keeps a
   * rebuild inside one pass from revisiting an entry.
   *
   * @return the next entry, or {@code null} when the map is empty
   */
  private Map.Entry<String, V> next() {
    if (cursor != null && cursor.hasNext()) {
      return cursor.next();
    }
    Iterator<Map.Entry<String, V>> it = map.entrySet().iterator();
    if (!it.hasNext()) {
      cursor = null;
      return null;
    }
    cursor = it;
    return it.next();
  }
}
