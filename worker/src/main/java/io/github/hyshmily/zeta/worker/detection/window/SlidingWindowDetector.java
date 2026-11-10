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

import io.github.hyshmily.zeta.util.TimeSource;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.jspecify.annotations.Nullable;

/**
 * A high‑performance sliding‑window detector for real‑time hot‑key
 * identification.
 *
 * <h2>Why a sliding window?</h2>
 * A fixed‑window counter (e.g. reset every second) suffers from boundary
 * problems: a burst that straddles two windows can be split and go undetected.
 * The sliding window continuously aggregates the last {@code windowSize} time
 * slices, giving a smooth, instantaneous view of traffic for every key.
 *
 * <h2>Data structure</h2>
 * Each key owns a single {@link KeyWindow} entry — a doubled {@code long[]} of length
 * {@code 2 * windowSize} plus the key's last written <b>absolute slice number</b>, stored together
 * in one map entry. Every slot packs its absolute slice tag in the high 32 bits and its saturating
 * count in the low 32 bits, so a late write from a previous rotation can never be read as traffic
 * in the current window: the sum skips a slot whose tag is not the slice it expects. Element access
 * goes through one static {@link VarHandle}; the packed word makes the tag and count one atomic
 * fact (design decisions: worker detection).
 *
 * <h2>Concurrency</h2>
 * <ul>
 * <li>The {@link VarHandle} provides atomic access to each packed slot. Reads and the sum use
 * opaque mode; a slice is an independent statistical counter, so a summation that races an add
 * sees either the previous or the new word. The add is a CAS loop because the tag and the count
 * must change together: a slot whose tag belongs to another rotation restarts at zero instead of
 * accumulating onto stale traffic.</li>
 * <li>The slice-number read‑modify‑write runs under the {@link KeyWindow} monitor. Eviction uses
 * that monitor as a leaf lock: it re-checks the idle bound, writes the {@code lastSlice}
 * tombstone, releases the monitor, and only then removes the mapping with an identity-matched
 * {@code windows.remove}. Creation still runs its remapping function under the CHM bin lock and
 * may take the entry monitor, but eviction never holds the monitor while taking the bin lock, so
 * the two orders cannot deadlock. The tombstone is the seat state: a window whose
 * {@code lastSlice} is {@code EVICTED} owns no seat, so no second flag records it. The
 * cap-relief path adds the reclaimer lock outside every map lock (reclaimer → entry monitor,
 * then the bin lock after the monitor is released); no path takes the reclaimer lock from inside
 * a bin lock. The slot add and the window-sum read run lock-free outside the monitor.</li>
 * <li>The tag closes the pause-after-map-read race without putting the add under the entry
 * monitor. A thread that samples an old slice, stalls through a later writer's window rotation,
 * and then lands on a recycled slot sees the newer tag on the CAS retry and drops its add. A write
 * that still lands with an old tag is ignored by every reader. A window whose tag is missing or
 * different is never summed, so a stale count cannot be re-attributed to a newer slice.</li>
 * <li>The aggregated sum is a statistical signal for hotspot detection,
 * where an occasional missed or double‑counted access does not
 * materially affect the HOT/COOL decision.</li>
 * <li><b>Slice-gated fast path:</b> {@link #addCountAtSlice} skips the slice-number update entirely when
 * the key's slice is not behind the caller's sample — the slice number is refreshed at most once
 * per slice. The no-lost-add guarantee against eviction holds while the eviction staleness
 * threshold is at least one slice (any real configuration: minutes vs tens of ms) — a window old
 * enough for the evictor to target routes every arriving add through the timestamp-refreshing
 * slow path. A thread that stalls for longer than the eviction threshold between its map read and
 * its slot write can still land on an entry the evictor removed; the add is then lost, never
 * attributed to another key.</li>
 * </ul>
 *
 * <h2>Memory management</h2>
 * The {@link #evictStale(long)} method must be called periodically (e.g. on the detection sweep's
 * cadence) by a scheduler. It removes any key that has not been accessed
 * within the given timeout, freeing the associated buffer and map entry.
 *
 * <h2>Window span</h2>
 * A write sums the {@code windowSize} slices ending at the slice it just wrote, plus <b>one</b>
 * guard slice. The guard makes the covered time span {@code [W, W + slice)}, so the same traffic
 * does not cross a threshold differently depending on the millisecond it was sampled (design
 * decisions: worker detection).
 *
 * <p>A read-only sum starts at the slice of the key's last write instead of at the current slice.
 * Slices after the last write hold no data, and the ring slot at the current index can still carry
 * a write from two windows ago once the index has wrapped; starting at the last write skips both.
 * The guard slot is read only while it lies inside the range that ends at the last write.
 *
 * @see io.github.hyshmily.zeta.worker.detection.DetectionPipeline#sweep()
 */
public class SlidingWindowDetector {

  /**
   * Element access for every {@link KeyWindow#slices}. One static handle instead of one
   * {@code AtomicLongArray} per key keeps the per-key footprint to the array itself. Reads use
   * opaque mode: the slots are independent statistical counters, so the summation needs per-slot
   * atomicity and coherence, not a volatile barrier per element.
   *
   * <p>A slot is one {@code long}: high 32 bits are the absolute slice's low 32 bits (the tag) and
   * low 32 bits the count. The tag is what lets the add stay lock-free: a write that lands after a
   * rotation still carries its old slice, the reader ignores it instead of summing it as current
   * traffic, and an add that finds a newer tag on its CAS retry drops itself instead of replacing
   * that newer count. The count saturates at {@link Integer#MAX_VALUE}. The tag aliases only after
   * {@code 2^32} slices, roughly 49.7 days at a 1 ms slice.
   */
  private static final VarHandle SLICES = MethodHandles.arrayElementVarHandle(long[].class);

  /**
   * Number of live entries sampled when a new key arrives at the capacity cap. Fixed so the
   * relief path costs a bounded handful of hash-bin visits; a sample that finds nothing idle means
   * the tracked set is genuinely active, and the key waits for the accelerated scan instead of
   * pushing a live entry out. Every eligible entry in the sample is reclaimed, so one walk can
   * release several seats for the admissions queued behind it.
   */
  private static final int EVICTION_SAMPLE = 32;
  private final ConcurrentHashMap<String, KeyWindow> windows = new ConcurrentHashMap<>();

  /**
   * Round-robin sampler for the admission bound; sampling happens only at the cap and spans
   * {@link #EVICTION_SAMPLE} entries, so concurrent admissions either reuse the seats one walk
   * releases or, when they lose the sample, admit without rejecting.
   */
  private final SamplingReclaimer<KeyWindow> reclaimer;

  /**
   * The stateless idle test the reclaimer's sampling pass applies. Held as one instance so a
   * new-key admission allocates neither a capturing lambda nor an out-parameter array.
   */
  private final SamplingReclaimer.IdleEvictor<KeyWindow> sampledEvictor = this::evictSampledIfStale;

  private final long timeMillisPerSlice;

  private final int windowSize;

  /**
   * Bitmask for circular buffer index (length - 1, where length is a power of
   * two).
   */
  private final int lengthMask;

  /** One key's window state: the doubled ring and the absolute slice of its last write. */
  private static final class KeyWindow {

    /**
     * Tombstone: the entry has been removed from the map and may only be replaced, never reused.
     * No timestamp this process can observe divides down to {@link Long#MIN_VALUE}, so the value is
     * distinguishable from every real slice.
     */
    private static final long EVICTED = Long.MIN_VALUE;

    /**
     * Doubled circular buffer of packed per-slice slots ({@code 2 * windowSize} slots), accessed
     * through {@link #SLICES}. The doubling keeps every slot the {@code windowSize + 1} sum covers
     * from being overwritten by the next rotation; a slot is reused only after {@code 2 * windowSize}
     * slices. The packed tag lets a late write detect that reuse and drop itself instead of
     * replacing the newer count.
     */
    final long[] slices;

    /**
     * Absolute slice number ({@code floorDiv(monotonicMillis, sliceMs)}) of the last write, written
     * only under this entry's monitor — under the slice-gated fast path at most once per slice (a
     * same-slice add leaves the slice alone). Eviction sets {@link #EVICTED} under the same
     * monitor before removing the mapping, so an add holding this reference can tell that the entry
     * is gone. {@code volatile} makes the lock-free eviction scan and diagnostics read harmless:
     * the re-check under the monitor is authoritative.
     *
     * <p>The tombstone is also the admission-seat state: {@code EVICTED} means the entry has
     * returned its seat, any other value means it owns one.
     */
    volatile long lastSlice;

    KeyWindow(int capacity, long nowSlice) {
      this.slices = new long[capacity];
      this.lastSlice = nowSlice;
    }
  }

  /**
   * Constructs a detector over an already-aligned window geometry.
   *
   * @param geometry       the aligned window (slice count, slice length, effective length)
   * @param maxTrackedKeys soft admission cap on tracked keys ({@code <= 0} = unlimited)
   */
  public SlidingWindowDetector(WindowGeometry geometry, long maxTrackedKeys) {
    this.windowSize = geometry.alignedSlices();
    this.timeMillisPerSlice = geometry.sliceMs();
    this.lengthMask = (geometry.alignedSlices() << 1) - 1;
    this.reclaimer = new SamplingReclaimer<>("detector", windows, EVICTION_SAMPLE, maxTrackedKeys);
  }

  /**
   * Cumulative new-key admissions rejected by the cap.
   *
   * @return the cumulative rejection counter
   */
  public long rejectedNewKeys() {
    return reclaimer.rejectedNewKeys();
  }

  /**
   * Cumulative idle windows reclaimed to admit a new key at the cap.
   *
   * @return the cumulative relief-eviction counter
   */
  public long capacityEvictions() {
    return reclaimer.capacityEvictions();
  }

  /**
   * Records an access count for the given key at the caller's slice and returns the sum of the
   * current sliding window.
   *
   * <p>
   * If this is the first access for the key, a new window entry is created
   * atomically. The slice-number write of a resident entry runs under its monitor;
   * eviction removes through {@code windows.compute} and re-checks under the same
   * monitor (see the class Javadoc). The tagged slot add and the window-sum read run
   * lock-free outside it.
   *
   * <p><b>Slice-gated fast path:</b> when the key already has a window and its sampled slice is
   * not ahead of the window's slice, the compute is skipped entirely — within the same slice the
   * timestamp would not move, and a thread whose sample is older than a concurrent adder's has
   * nothing to refresh either. Eviction thresholds are configured
   * in minutes while a slice is tens of ms, so every window old enough for the evictor to target
   * routes its arriving adds through the slice-refreshing slow path. Widely-hot keys reported
   * by many App instances within one slice then pay one lock-free map read instead of the entry
   * monitor on most calls. The return sum walks the O(windowSize) loop over the
   * circular buffer for an existing window: concurrent adders from many App instances share the
   * same window, so a cached sum would go stale between adds. Callers that do not consume the sum
   * use {@link #recordCountAtSlice(String, long, long)}.
   *
   * <p><b>Admission at the cap:</b> a brand-new key arriving while the admission cap is reached
   * asks the reclaimer to reclaim idle windows in one bounded pass; the add is dropped only when
   * the hard ledger is also exhausted. A pass another admission holds yields an admission, not
   * a rejection, but only by reserving a seat from the hard ledger. A completed pass that finds
   * nothing opens the reclaimer's saturation window for the arrivals behind it, while its own
   * admission still uses the hard headroom; other new keys are rejected without another pass: the
   * scan's microsecond-scale hold must not be outrun by admissions that miss its
   * lock by nanoseconds. The hard ledger is the true total bound; the soft cap only gates when a
   * sampling pass runs.
   *
   * @param key      the cache key; must not be {@code null}
   * @param count    the number of accesses to record (typically the batched add reported by an
   *                 application instance); non-positive values are ignored, and the returned sum is
   *                 then taken at {@code nowSlice}
   * @param nowSlice the batch's absolute slice number, from {@link #sliceIndex(long)}
   * @return the sum of the last {@link #windowSize} slices after adding {@code count}
   * @throws NullPointerException if {@code key} is {@code null}
   */
  public long addCountAtSlice(String key, long count, long nowSlice) {
    if (count <= 0) {
      return windowSumAtSlice(key, nowSlice);
    }
    KeyWindow window = recordCount(key, count, nowSlice, false);
    if (window == null) {
      return 0L;
    }
    // A concurrent writer may have advanced the window while this add was in flight. Summing to the
    // later slice keeps the returned sum current; an add that lost its slot to a newer rotation is
    // simply not part of it.
    return sumRange(window.slices, Math.max(nowSlice, window.lastSlice), windowSize + 1, lengthMask);
  }

  /**
   * Records an access at the caller's slice without computing the window sum.
   *
   * <p>The already-hot report path needs the count in the window for the cool-down, but its
   * promotion decision is already settled; walking {@code windowSize + 1} slots there only to
   * discard the result would put the hot traffic on the most expensive read of the window.
   *
   * <p>The key already holds a registry seat, so its window is admitted even at or above the
   * tracked-key cap: losing the count would make a live hot key look quiet to
   * {@code COOL_FOR_LOW_QPS}. The overshoot beyond the hard ledger is bounded by the HOT seat count,
   * and the cap already admits an approximate size.
   *
   * @param key      the cache key; must not be {@code null}
   * @param count    the number of accesses to record; non-positive values are ignored
   * @param nowSlice the batch's absolute slice number, from {@link #sliceIndex(long)}
   * @throws NullPointerException if {@code key} is {@code null}
   */
  public void recordCountAtSlice(String key, long count, long nowSlice) {
    if (count > 0) {
      recordCount(key, count, nowSlice, true);
    }
  }

  /**
   * Admits or advances the key's window and writes {@code count} into its slot — the shared body of
   * both report paths.
   *
   * <p>A null mapping and a tombstone both need a window. The tombstone is already dead (its seat
   * was returned when it was set), so it never takes its own monitor; both cases enter the
   * admission path, which reserves a seat before the bin lock and releases it when a concurrent
   * admission published a live window first.
   *
   * @param key        the cache key
   * @param count      the positive count to add
   * @param nowSlice   the caller's absolute slice number
   * @param alreadyHot whether this report belongs to a key that already holds a HOT seat
   * @return the live window, or {@code null} when the cap refused the admission
   */
  @Nullable
  private KeyWindow recordCount(String key, long count, long nowSlice, boolean alreadyHot) {
    // Fast path: the window's slice is not behind this thread's sample. An older sample (a
    // concurrent adder crossed the boundary first) must not take the entry monitor only to discover
    // it has nothing to refresh — the add keeps its own slice below. The comparison is on
    // SLICE NUMBER, not raw elapsed time: `now - lastWrite >= sliceMs` disagrees with the bucket
    // arithmetic around boundaries (lastWrite=90 and now=180 with a 100ms slice are one bucket
    // apart, yet only 90ms elapsed).
    KeyWindow window = windows.get(key);
    long lastSlice = window == null ? KeyWindow.EVICTED : window.lastSlice;

    if (lastSlice == KeyWindow.EVICTED) {
      window = admitAndCreate(key, nowSlice, alreadyHot);
    } else if (lastSlice < nowSlice) {
      window = advance(window, nowSlice);
      if (window == null) {
        window = admitAndCreate(key, nowSlice, alreadyHot);
      }
    }
    if (window == null) {
      return null;
    }

    // Keep the add at its own slice. `nowSlice` was sampled before the monitor and a concurrent
    // adder may have moved the window ahead, but re-attributing this count to the newer slice
    // would inject traffic older than the window into the current one. A batch that fell behind
    // the whole window is dropped; one still inside the span keeps its honest slice tag.
    long latestSlice = window.lastSlice;
    if (latestSlice == KeyWindow.EVICTED) {
      return null;
    }
    if (latestSlice - nowSlice <= windowSize) {
      addToSlot((int) (nowSlice & lengthMask), (int) Math.min(count, Integer.MAX_VALUE), window.slices, nowSlice);
    }
    return window;
  }

  /**
   * Adds {@code delta} to the slot for {@code addSlice}. The tag and the count change together, so
   * a slot left by a previous rotation is replaced from zero rather than accumulated onto. A
   * {@code getAndAdd} plus MAX repair is not atomic: a concurrent adder can observe a wrapped
   * negative count before the repair lands.
   *
   * <p>A slot already owned by a newer rotation is left untouched. The add is then older than
   * every readable slice, so dropping it loses no visible traffic; replacing the newer word would
   * erase a write the current window still sums.
   *
   * @param index    the slot index, {@code addSlice & lengthMask}
   * @param delta    the positive count to add
   * @param slots    the doubled ring
   * @param addSlice the absolute slice of this add
   */
  static void addToSlot(int index, int delta, long[] slots, long addSlice) {
    long current = (long) SLICES.getOpaque(slots, index);
    for (;;) {
      int base = 0;
      if (current != 0L) {
        // An all-zero word is an unwritten slot, not a slice-0 write: without this branch a
        // negative add slice would make the empty tag look like a newer rotation.
        int ahead = tag(current) - (int) addSlice;
        if (ahead > 0) {
          return;
        }
        if (ahead == 0) {
          base = count(current);
        }
      }
      int next = base >= Integer.MAX_VALUE - delta ? Integer.MAX_VALUE : base + delta;
      long witness = (long) SLICES.compareAndExchange(slots, index, current, pack(addSlice, next));
      if (witness == current) {
        return;
      }

      current = witness;
    }
  }

  /** @return the slice tag stored in a packed slot */
  private static int tag(long packed) {
    return (int) (packed >>> 32);
  }

  /** @return the saturating count stored in a packed slot */
  private static int count(long packed) {
    return (int) packed;
  }

  /**
   * Packs one slot. The tag is the absolute slice's low 32 bits; the count occupies the low word so
   * a stale write can be recognized without a parallel array.
   */
  static long pack(long slice, int count) {
    return ((long) (int) slice << 32) | (count & 0xffff_ffffL);
  }

  /**
   * Reserves one admission seat and publishes the key's window, or advances the window a concurrent
   * admission published first.
   *
   * <p>The admission gate runs before {@code windows.compute}: a sampling pass removes a different
   * key, and a remapping function must not modify the map it is remapping. A new key and a
   * tombstone both sample when the soft cap is reached; only a hot-key report takes a hot seat
   * without sampling. The eviction predicate runs under the reclaimer's lock but before any bin or
   * entry lock is taken here, so the order stays reclaimer → CHM bin → entry monitor. The lock-free
   * staleness test comes first: a sample over active keys must not take 32 bin locks and 32 entry
   * monitors only to find every candidate live.
   *
   * @param key             the key being admitted
   * @param nowSlice        the caller's absolute slice number
   * @param alreadyHot      whether this report belongs to a key that already holds a HOT seat and
   *                        is admitted regardless of the tracked-key bound
   * @return the live window, or {@code null} when the cap refused the seat
   */
  @Nullable
  private KeyWindow admitAndCreate(String key, long nowSlice, boolean alreadyHot) {
    if (alreadyHot) {
      // The key already holds a registry seat, so it is not new tracked traffic and the admission
      // bound does not apply. A tombstone left by a concurrent eviction is replaced under the same
      // exemption: the key's count still feeds its cool-down.
      reclaimer.reserveHotSeat();
      return computeWindow(key, nowSlice);
    }
    // A sampled tombstone already returned its seat when it was set, so the replacement is a new
    // admission: it must pass the same soft-cap sample and saturation gate as a first touch.
    return reclaimer.tryAdmit(key, nowSlice, sampledEvictor) ? computeWindow(key, nowSlice) : null;
  }

  /**
   * The reclaimer's per-candidate test: reclaim the sampled window when this pass is the one that
   * tombstoned it.
   *
   * <p>The lock-free staleness pre-check keeps a sample over active keys from taking entry monitors
   * only to find every candidate live. The re-check under the monitor is authoritative, and the
   * return value is {@code true} only for the call that wrote the tombstone; a concurrent periodic
   * sweep's tombstone leaves this call returning {@code false}. The reclaimer accounts for the
   * reclaimed seat from that verdict, so this method does not release it.
   *
   * @param entry    the sampled entry
   * @param nowSlice the pass's absolute slice number
   * @return {@code true} when this call tombstoned and removed the entry
   */
  private boolean evictSampledIfStale(Map.Entry<String, KeyWindow> entry, long nowSlice) {
    return removeIfStale(entry.getKey(), entry.getValue(), nowSlice, windowSize, false);
  }

  /**
   * Advances a live resident window across a slice boundary under its own monitor.
   *
   * <p>A resident, live entry is updated under its own monitor: the slice number belongs to the
   * entry, and no map mutation happens, so re-entering the CHM compute would only rehash the key and
   * take the bin lock once per slice.
   *
   * @param window   the sampled live window
   * @param nowSlice the caller's absolute slice number
   * @return the live window, or {@code null} when the evictor tombstoned the sampled entry first
   */
  @Nullable
  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  private static KeyWindow advance(KeyWindow window, long nowSlice) {
    synchronized (window) {
      long lastSlice = window.lastSlice;
      if (lastSlice == KeyWindow.EVICTED) {
        return null;
      }
      if (lastSlice < nowSlice) {
        window.lastSlice = nowSlice;
      }
      return window;
    }
  }

  /**
   * Creates the mapping or advances the window already published by a concurrent admission. The
   * caller holds a reserved seat: the mapping consumes it when it creates a window and returns it
   * when it finds a live one.
   *
   * <p>{@code ConcurrentHashMap.compute} runs this remapping function exactly once for the key
   * under its bin lock, so the seat the caller already reserved is consumed by returning a new
   * window, or returned with {@code releaseSeat()} when a concurrent admission published a live
   * one. No reservation object crosses the call.
   *
   * @param key      the key being admitted
   * @param nowSlice the caller's absolute slice number
   * @return the live window
   */
  private KeyWindow computeWindow(String key, long nowSlice) {
    return windows.compute(key, (k, existing) -> {
      if (existing == null) {
        return new KeyWindow(windowSize * 2, nowSlice);
      }

      KeyWindow advanced = advance(existing, nowSlice);
      if (advanced == null) {
        // The evictor tombstoned the mapping and is about to remove it; replacing it here keeps
        // the key live and consumes the caller's reserved seat. The tombstone's seat was already
        // returned (periodic sweep) or kept for the sampling admission that set it.
        return new KeyWindow(windowSize * 2, nowSlice);
      }

      // A concurrent admission published the mapping: this thread's seat is surplus.
      reclaimer.releaseSeat();
      return advanced;
    });
  }

  /**
   * Whether a window's last write is old enough to contribute zero to every reader. The boundary
   * is the same slice boundary {@link #windowSum(String)} uses, so a capacity reclaim can never
   * remove a guard slice a reader still covers. The lock-free read is a pre-check only: the
   * authoritative test is repeated under the entry monitor.
   *
   * @param window     the candidate
   * @param nowSlice   current absolute slice number
   * @param idleSlices the idle bound in slices
   * @return {@code true} when the window is outside every reader's summation range
   */
  private static boolean isStale(KeyWindow window, long nowSlice, long idleSlices) {
    long lastSlice = window.lastSlice;
    return lastSlice != KeyWindow.EVICTED && nowSlice - lastSlice > idleSlices;
  }

  /**
   * Re-checks one sampled victim under its own monitor, tombstones it when it is still idle, and
   * removes the mapping with an identity-matched remove.
   *
   * <p>The monitor is a leaf lock: it is acquired first and released before the map's bin lock is
   * taken. Only the call that writes {@code lastSlice = EVICTED} reports {@code true}; a concurrent
   * periodic sweep that tombstoned the same window first makes this call observe the tombstone and
   * return {@code false}. The periodic sweep returns the reclaimed seat through
   * {@link SamplingReclaimer#releaseSeat()}; the capacity sample leaves the seat to its own
   * admission, which {@code SamplingReclaimer} accounts for from this return value.
   *
   * @param key         the candidate key
   * @param window      the candidate window
   * @param nowSlice    current absolute slice number
   * @param idleSlices  the idle bound in slices
   * @param releaseSeat {@code true} for the periodic sweep, {@code false} for the capacity sample
   * @return {@code true} only when this call tombstoned a live window and removed its mapping
   */
  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  private boolean removeIfStale(String key, KeyWindow window, long nowSlice, long idleSlices, boolean releaseSeat) {
    if (!isStale(window, nowSlice, idleSlices)) {
      return false;
    }
    synchronized (window) {
      long lastSlice = window.lastSlice;
      if (lastSlice == KeyWindow.EVICTED || nowSlice - lastSlice <= idleSlices) {
        return false;
      }
      window.lastSlice = KeyWindow.EVICTED;
      if (releaseSeat) {
        reclaimer.releaseSeat();
      }
    }
    windows.remove(key, window);
    return true;
  }

  /**
   * Consume the "an admission at the cap found nothing idle" signal.
   *
   * <p>The periodic scan uses it to run once with the window-aligned idle
   * boundary instead of the configured retention, bounding how long a key can
   * stay locked out by an unlucky sample.
   *
   * @return {@code true} if relief was requested since the previous call
   */
  public boolean consumeReliefRequest() {
    return reclaimer.consumeReliefRequest();
  }

  /**
   * Sums the slots for the absolute slices {@code [endSlice - length + 1, endSlice]}. A slot is
   * included only when its tag equals the slice it would represent, so a late write from a previous
   * rotation is ignored rather than re-attributed. The doubled ring guarantees the range never
   * covers the same physical slot twice.
   *
   * @param slices    the doubled ring
   * @param endSlice  the newest absolute slice in the range
   * @param slotCount the number of slices, at most {@code slices.length}
   * @param lengthMask bitmask for the doubled buffer index
   * @return the range's sum
   */
  static long sumRange(long[] slices, long endSlice, int slotCount, int lengthMask) {
    long sum = 0L;
    for (int i = 0; i < slotCount; i++) {
      long slice = endSlice - i;
      long packed = (long) SLICES.getOpaque(slices, (int) (slice & lengthMask));
      if (tag(packed) == (int) slice) {
        sum += count(packed);
      }
    }
    return sum;
  }

  /**
   * Read-only window sum for the given key, computed from the circular buffer without any side
   * effects.
   *
   * <p>The sum starts at the slice of the last write rather than at the current slice: slices
   * after the last write are known empty, and the ring slot at the current index can still hold a
   * write from two windows ago after the index has wrapped. Starting at the last write skips both,
   * so a full window of silence reads only that write. Beyond one full window the key contributes
   * zero and the buffer is not walked at all.
   *
   * <p>The cool-down path's only window read: {@code COOL_FOR_LOW_QPS} re-reads the sum under the
   * key lock instead of reusing the sample a report carried, so a decision is never made on an
   * observation taken outside the lock. {@code addCountAtSlice} already returns the same quantity for the
   * report it is judging, so the hot path does not call this.
   *
   * @param key the cache key
   * @return the sliding-window sum at the current time, or {@code 0}
   */
  public long windowSum(String key) {
    // Sample the clock before reading the window's slice. A write that lands after this sample
    // pushes lastSlice past nowSlice, so the full range ends at that newer write instead of being
    // mistaken for a window of silence; the reverse order could pair an old slice with a new clock
    // reading and return zero for a key that is still receiving traffic.
    return windowSumAtSlice(key, sliceIndex(TimeSource.monotonicMillis()));
  }

  /**
   * The shared read-only sum behind {@link #windowSum(String)} and a non-positive add: the same
   * last-write-anchored range, taken at the caller's slice without re-sampling the clock.
   *
   * @param key      the cache key
   * @param nowSlice the absolute slice the sum is taken at
   * @return the sliding-window sum at {@code nowSlice}, or {@code 0}
   */
  private long windowSumAtSlice(String key, long nowSlice) {
    KeyWindow window = windows.get(key);
    if (window == null) {
      return 0L;
    }

    long lastSlice = window.lastSlice;
    if (lastSlice == KeyWindow.EVICTED) {
      // The evictor tombstoned the entry before removing the mapping; a reader that raced the
      // removal must not sum a buffer the entry no longer owns.
      return 0L;
    }

    long elapsedSlices = nowSlice - lastSlice;
    if (elapsedSlices > windowSize) {
      return 0L;
    }
    // Slices after the last write are known empty, so the walk starts there rather than at the
    // current slice: the covered range is [nowSlice - windowSize, lastSlice], and its guard slot
    // drops out one slice at a time as the silence grows.
    long silenceSlices = Math.max(0L, elapsedSlices);
    return sumRange(window.slices, lastSlice, (int) (windowSize + 1 - silenceSlices), lengthMask);
  }

  /**
   * The absolute slice a monotonic timestamp belongs to — the quantity the circular index is
   * derived from.
   *
   * <p>{@code floorDiv}, not {@code /}: a test can shift the monotonic axis negative, and
   * truncation toward zero would put instants on opposite sides of the epoch into the same slice.
   *
   * @param timestampMillis monotonic milliseconds
   * @return the absolute slice number
   */
  public long sliceIndex(long timestampMillis) {
    return Math.floorDiv(timestampMillis, timeMillisPerSlice);
  }

  /**
   * Evicts every window whose last write is outside the summation range every reader
   * covers — the slice-aligned "contributes zero" boundary used by capacity relief.
   *
   * <p>This is the same predicate {@link #isStale(KeyWindow, long, long)} uses, so a relief pass
   * can never reclaim a guard slice a concurrent read still sums.
   */
  public void evictIdle() {
    evict(windowSize);
  }

  /**
   * Evicts stale tracking entries for keys that have not been accessed within the given timeout.
   *
   * <p>This is the explicit retention bound: the caller decides how long an idle entry is kept,
   * independently of the summation range. The cap-relief path uses {@link #evictIdle()} for
   * the slice-aligned "contributes zero" test.
   *
   * <p>Scan-then-recheck: a lock-free pass visits the entries, then each candidate is re-checked
   * under its entry monitor and removed with an identity-matched map removal. Because the
   * consumer's {@link #addCountAtSlice} refreshes the slice under the <em>same</em> monitor, the two
   * operations are fully serialized per key: if {@code addCountAtSlice} ran first, the refreshed slice
   * fails the re-check and the entry is kept; if the eviction ran first, it tombstones the entry
   * and the concurrent {@code addCountAtSlice} re-creates a fresh window through the admission path, so
   * no add is lost.
   *
   * <p>The retention threshold is a duration, so it is quantized to whole slices once per scan:
   * an entry is dropped on the first slice boundary strictly more than {@code staleAfterMs} after
   * the slice of its last write. The quantization is at most one slice, while every real retention
   * is orders of magnitude larger.
   *
   * <p>Must be called periodically (e.g. via
   * {@link io.github.hyshmily.zeta.worker.detection.DetectionPipeline#sweep()}) to prevent
   * unbounded memory growth from keys that are no longer accessed.
   *
   * @param staleAfterMs maximum idle time in milliseconds before a key is considered stale and
   *                     evicted; must be non-negative
   * @throws IllegalArgumentException when {@code staleAfterMs} is negative
   */
  public void evictStale(long staleAfterMs) {
    if (staleAfterMs < 0L) {
      throw new IllegalArgumentException("staleAfterMs must be non-negative, got " + staleAfterMs);
    }
    evict(staleAfterMs / timeMillisPerSlice);
  }

  /** Evicts every window outside the given slice-aligned idle bound. */
  private void evict(long staleSlices) {
    long nowSlice = sliceIndex(TimeSource.monotonicMillis());

    // ConcurrentHashMap.forEach is weakly consistent, so removing an entry from the iteration is
    // safe. The monitor re-check still guards against a concurrent add refreshing the slice,
    // and the sweep allocates no candidate snapshot list.
    try {
      windows.forEach((key, window) -> removeIfStale(key, window, nowSlice, staleSlices, true));
    } finally {
      // Drop the hand after the walk, not before: a sample that starts during the sweep can
      // otherwise park a fresh iterator on a table sized for the burst the sweep just drained.
      reclaimer.releaseHand();
    }
  }

  /**
   * Returns the number of keys currently being tracked by this detector.
   *
   * <p>
   * This includes all keys with allocated circular buffers, regardless
   * of whether they have expired but have not yet been evicted.
   *
   * @return the number of active keys in the windows map
   */
  public int activeWindows() {
    return windows.size();
  }
}
