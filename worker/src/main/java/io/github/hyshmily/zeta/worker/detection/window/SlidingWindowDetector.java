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
 * A sliding-window detector for real-time hot-key identification.
 *
 * <p>
 * Each key owns one {@link KeyWindow}: a doubled {@code long[]} of {@code 2 * windowSize} slots plus
 * the absolute slice of its last write. A slot packs its absolute slice tag in the high 32 bits and
 * its saturating count in the low 32 bits, so a late write from a previous rotation is skipped
 * instead of being summed as current traffic. Every slot is accessed through one static
 * {@link VarHandle}; reads and the sum use opaque mode, and the add is a CAS loop because the tag
 * and the count must change together.
 *
 * <p>
 * The slice number is written under the {@link KeyWindow} monitor. Eviction writes the tombstone
 * under that monitor and removes the mapping only after releasing it, with an identity-matched
 * {@code windows.remove}. Creation installs a fresh window with an atomic {@code putIfAbsent} and
 * advances an existing one outside the bin lock, so no path takes the bin lock while holding a
 * monitor; the only nesting is entry monitor → bin lock, and creation holds no monitor at all. The
 * cap-relief path takes the reclaimer lock outside every map lock. The slot add and the window-sum
 * read stay lock-free.
 *
 * <p>
 * A write sums the {@code windowSize} slices ending at the slice it wrote plus one guard slice, so
 * the covered span is {@code [W, W + slice)} and the same traffic crosses a threshold the same way
 * whatever millisecond it is sampled. A read-only sum starts at the key's last write: later slices
 * are empty, and the current ring slot may still hold a write from two windows ago.
 *
 * <p>
 * {@link #evictStale(long)} must run periodically (the detection sweep) to bound memory.
 *
 * <p>
 * design decisions: worker detection
 */
public class SlidingWindowDetector {

  /**
   * Element access for every {@link KeyWindow#slices}. One static handle instead of one
   * {@code AtomicLongArray} per key keeps the per-key footprint to the array itself. A slot is one
   * {@code long}: high 32 bits the absolute slice tag, low 32 bits the count, saturating at
   * {@link Integer#MAX_VALUE}. The tag aliases only after {@code 2^32} slices, roughly 49.7 days at
   * a 1 ms slice.
   */
  private static final VarHandle SLICES = MethodHandles.arrayElementVarHandle(long[].class);

  /**
   * Number of live entries sampled when a new key arrives at the capacity cap. The relief path is
   * bounded to a handful of hash-bin visits; every eligible entry in the sample is reclaimed, so one
   * walk can release several seats for the admissions queued behind it.
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

  /** Bitmask for the circular buffer index ({@code 2 * windowSize - 1}). */
  private final int lengthMask;

  /** One key's window state: the doubled ring and the absolute slice of its last write. */
  static final class KeyWindow {

    /**
     * Tombstone: the entry has been removed from the map and may only be replaced. No timestamp
     * this process can observe divides down to {@link Long#MIN_VALUE}.
     */
    private static final long EVICTED = Long.MIN_VALUE;

    /**
     * Doubled circular buffer of packed per-slice slots ({@code 2 * windowSize}), accessed through
     * {@link #SLICES}. A slot is reused only after {@code 2 * windowSize} slices, so the packed tag
     * lets a late write detect the reuse and drop itself instead of replacing the newer count.
     */
    final long[] slices;

    /**
     * Absolute slice number ({@code floorDiv(monotonicMillis, sliceMs)}) of the last write, written
     * under this entry's monitor — at most once per slice on the fast path. Eviction sets
     * {@link #EVICTED} under the same monitor before removing the mapping. {@code volatile} makes
     * the lock-free eviction scan and diagnostics read harmless; the re-check under the monitor is
     * authoritative.
     *
     * <p>The tombstone is also the admission-seat state: {@code EVICTED} means the entry has
     * returned its seat, any other value means it owns one.
     */
    volatile long lastSlice;

    /**
     * Creates a window whose first slot already holds {@code initialCount} at {@code nowSlice}.
     *
     * <p>The slot is written before the window is published, so every thread that observes the
     * window observes its first count and the install needs no post-publication CAS.
     *
     * @param capacity     the doubled ring length, {@code 2 * windowSize}
     * @param nowSlice     the absolute slice of the initial count
     * @param slotIndex    the ring slot for {@code nowSlice}, {@code nowSlice & lengthMask}
     * @param initialCount the positive count already recorded at {@code nowSlice}
     */
    KeyWindow(int capacity, long nowSlice, int slotIndex, int initialCount) {
      this.slices = new long[capacity];
      this.slices[slotIndex] = pack(nowSlice, initialCount);
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
   * A first access creates the window through the admission gate. The slice-gated fast path skips
   * the entry monitor when the key's slice is not behind the caller's sample; eviction thresholds
   * are minutes against a slice of tens of milliseconds, so a window old enough for the evictor
   * always takes the slice-refreshing slow path. A first access returns the count that created the
   * window; an existing window walks the ring because concurrent adders share it, so a cached sum
   * would go stale.
   *
   * <p>
   * A brand-new key at the cap asks the reclaimer for one bounded reclaim pass; the add is dropped
   * only when the hard ledger is also exhausted. Callers that do not consume the sum use
   * {@link #recordCountAtSlice(String, long, long)}.
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
    return recordCount(key, (int) Math.min(count, Integer.MAX_VALUE), nowSlice, false, true);
  }

  /**
   * Records an access at the caller's slice without computing the window sum. The already-hot report
   * path needs the count for the cool-down but has no promotion decision pending, so walking the
   * ring only to discard the result would put hot traffic on the most expensive read.
   *
   * <p>The key already holds a registry seat, so its window is admitted even at or above the
   * tracked-key cap: losing the count would make a live hot key look quiet to
   * {@code COOL_FOR_LOW_QPS}. The overshoot is bounded by the HOT seat count.
   *
   * @param key      the cache key; must not be {@code null}
   * @param count    the number of accesses to record; non-positive values are ignored
   * @param nowSlice the batch's absolute slice number, from {@link #sliceIndex(long)}
   * @throws NullPointerException if {@code key} is {@code null}
   */
  public void recordCountAtSlice(String key, long count, long nowSlice) {
    if (count > 0) {
      recordCount(key, (int) Math.min(count, Integer.MAX_VALUE), nowSlice, true, false);
    }
  }

  /**
   * Writes {@code delta} into the key's window and returns the sliding-window sum when requested.
   *
   * <p>A null mapping and a tombstone both need a window. The tombstone is already dead (its seat
   * was returned when it was set), so it never takes its own monitor; both cases enter the
   * admission path, which reserves a seat before the map operation and releases it when a
   * concurrent admission published a live window first.
   *
   * <p>A resident window costs one slice read: a window at or ahead of the caller's sample takes the
   * fast path, and a window behind it advances under its own monitor. The comparison is on SLICE
   * NUMBER, not raw elapsed time: {@code now - lastWrite >= sliceMs} disagrees with the bucket
   * arithmetic around boundaries (lastWrite=90 and now=180 with a 100ms slice are one bucket apart,
   * yet only 90ms elapsed). The add keeps the caller's slice; re-attributing it to a newer slice
   * would inject traffic older than the window into the current one. A batch that fell behind the
   * whole window is dropped by the span test, while one still inside the span keeps its honest slice
   * tag.
   *
   * @param key        the cache key
   * @param delta      the positive count to add
   * @param nowSlice   the caller's absolute slice number
   * @param alreadyHot whether this report belongs to a key that already holds a HOT seat
   * @param needSum    whether the caller needs the sliding-window sum
   * @return the sum over the last {@code windowSize} slices, or {@code 0} when the admission was
   *         refused or {@code needSum} is {@code false}
   */
  private long recordCount(String key, int delta, long nowSlice, boolean alreadyHot, boolean needSum) {
    KeyWindow window = windows.get(key);
    long lastSlice = window == null ? KeyWindow.EVICTED : window.lastSlice;

    if (lastSlice == KeyWindow.EVICTED) {
      return admitAndRecord(key, delta, nowSlice, alreadyHot, needSum);
    }
    if (lastSlice < nowSlice) {
      lastSlice = advance(window, nowSlice);
      if (lastSlice == KeyWindow.EVICTED) {
        return admitAndRecord(key, delta, nowSlice, alreadyHot, needSum);
      }
    }
    return addToExisting(window, lastSlice, delta, nowSlice, needSum);
  }

  /**
   * Adds {@code delta} at the caller's slice when it is still inside the window, then sums the range
   * when requested. The span test uses the slice the caller observed; a slot a newer rotation reused
   * is still rejected by its tag inside {@link #addToSlot}.
   *
   * @param window    the key's live window
   * @param lastSlice the window's last-write slice observed by the caller
   * @param delta     the positive count to add
   * @param nowSlice  the caller's absolute slice number
   * @param needSum   whether to return the sliding-window sum
   * @return the sum over the last {@code windowSize} slices, or {@code 0} when {@code needSum} is
   *         {@code false}
   */
  private long addToExisting(KeyWindow window, long lastSlice, int delta, long nowSlice, boolean needSum) {
    if (lastSlice - nowSlice <= windowSize) {
      addToSlot(window, (int) (nowSlice & lengthMask), delta, nowSlice);
    }
    if (!needSum) {
      return 0L;
    }
    // A concurrent writer may have advanced the window while this add was in flight. Summing to the
    // later slice keeps the returned sum current; an add that lost its slot to a newer rotation is
    // simply not part of it. An eviction that won the race leaves no window this sum may claim.
    long latest = window.lastSlice;
    if (latest == KeyWindow.EVICTED) {
      return 0L;
    }
    return sumRange(window.slices, Math.max(nowSlice, latest), windowSize + 1, lengthMask);
  }

  /**
   * Reserves one admission seat and installs a window whose first slot already holds {@code delta},
   * or advances the window a concurrent admission published first.
   *
   * <p>The admission gate runs before the mapping is touched, because the sampling pass a refused
   * seat triggers removes a different key and must not run inside a mapping replacement. The lock
   * order stays reclaimer → entry monitor → bin lock. A fresh window is installed already carrying
   * {@code delta}, so the call returns it directly; a live window found by a lost race receives the
   * add once the surplus seat is returned.
   *
   * @param key        the key being admitted
   * @param delta      the positive count to record
   * @param nowSlice   the caller's absolute slice number
   * @param alreadyHot whether this report belongs to a key that already holds a HOT seat and is
   *                   admitted regardless of the tracked-key bound
   * @param needSum    whether to return the sliding-window sum
   * @return the sliding-window sum, or {@code 0} when the cap refused the admission or
   *         {@code needSum} is {@code false}
   */
  private long admitAndRecord(String key, int delta, long nowSlice, boolean alreadyHot, boolean needSum) {
    if (alreadyHot) {
      // The key already holds a registry seat, so it is not new tracked traffic and the admission
      // bound does not apply. A tombstone left by a concurrent eviction is replaced under the same
      // exemption: the key's count still feeds its cool-down.
      reclaimer.reserveHotSeat();
    } else if (!reclaimer.tryAdmit(key, nowSlice, sampledEvictor)) {
      // A sampled tombstone already returned its seat when it was set, so the replacement is a new
      // admission: it must pass the same soft-cap sample and saturation gate as a first touch.
      return 0L;
    }
    KeyWindow existing;
    try {
      existing = publishOrGetExisting(key, delta, nowSlice);
    } catch (RuntimeException | Error e) {
      // The reservation is this call's alone until the publication consumes it. A failed
      // allocation must return it; otherwise every failed admission permanently shrinks the ledger.
      reclaimer.releaseSeat();
      throw e;
    }
    if (existing == null) {
      // The fresh window was installed with `delta` in its initial slot; linearizable at publication.
      return needSum ? delta : 0L;
    }
    long latest = existing.lastSlice;
    // A concurrent eviction between the lost race and this read returned the window's seat and
    // removed the mapping; there is no live window left to count against.
    return latest == KeyWindow.EVICTED ? 0L : addToExisting(existing, latest, delta, nowSlice, needSum);
  }

  /**
   * Adds {@code delta} to the slot for {@code addSlice}. The tag and the count change together, so
   * a slot left by a previous rotation is replaced from zero rather than accumulated onto; a
   * {@code getAndAdd} plus MAX repair is not atomic, and a concurrent adder could observe a wrapped
   * negative count before the repair lands. A slot already owned by a newer rotation is left
   * untouched: the add is outside every readable range, while replacing the newer word would erase
   * a write the current window still sums.
   *
   * <p>A positive signed tag difference also matches a tag written between {@code 2^31} and
   * {@code 2^32} slices ago, so the cold path confirms it against the window's 64-bit last write: a
   * newer rotation is at least {@code 2 * windowSize} slices ahead and its writer advanced
   * {@code lastSlice} before the slot CAS, while a last write still within {@code windowSize} of
   * the add proves no newer rotation can have reused the slot. The acquire fence makes the newer
   * writer's {@code lastSlice} advance visible to the slot read that observed the new tag.
   *
   * @param window   the key's window, the source of the 64-bit last write
   * @param index    the slot index, {@code addSlice & lengthMask}
   * @param delta    the positive count to add
   * @param addSlice the absolute slice of this add
   */
  void addToSlot(KeyWindow window, int index, int delta, long addSlice) {
    long[] slots = window.slices;
    long current = (long) SLICES.getOpaque(slots, index);
    for (;;) {
      int base = 0;
      if (current != 0L) {
        // An all-zero word is an unwritten slot, not a slice-0 write: without this branch a
        // negative add slice would make the empty tag look like a newer rotation.
        int ahead = tag(current) - (int) addSlice;
        if (ahead > 0) {
          VarHandle.acquireFence();
          long latest = window.lastSlice;
          if (latest == KeyWindow.EVICTED || latest - addSlice > windowSize) {
            return;
          }
        } else if (ahead == 0) {
          base = count(current);
          if (base == Integer.MAX_VALUE) {
            // The slice is already saturated: the packed word would be identical, and the CAS would
            // only make every consumer contend for the same cache line to write it back.
            return;
          }
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
   * The reclaimer's per-candidate test: reclaim the sampled window when this pass is the one that
   * tombstoned it. The re-check under the monitor is authoritative, so a concurrent periodic sweep
   * makes this call return {@code false}; the reclaimer accounts for the reclaimed seat from that
   * verdict.
   *
   * @param entry    the sampled entry
   * @param nowSlice the pass's absolute slice number
   * @return {@code true} when this call tombstoned and removed the entry
   */
  private boolean evictSampledIfStale(Map.Entry<String, KeyWindow> entry, long nowSlice) {
    return removeIfStale(entry.getKey(), entry.getValue(), nowSlice, windowSize, false);
  }

  /**
   * Advances a live resident window across a slice boundary under its own monitor. No map mutation
   * happens, so a slice roll-over costs one monitor entry and no map operation.
   *
   * @param window   the sampled live window
   * @param nowSlice the caller's absolute slice number
   * @return the window's last-write slice after the call, or {@link KeyWindow#EVICTED} when the
   *         evictor tombstoned the sampled entry first
   */
  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  private static long advance(KeyWindow window, long nowSlice) {
    synchronized (window) {
      long lastSlice = window.lastSlice;
      if (lastSlice == KeyWindow.EVICTED) {
        return KeyWindow.EVICTED;
      }
      if (lastSlice < nowSlice) {
        window.lastSlice = nowSlice;
        return nowSlice;
      }
      return lastSlice;
    }
  }

  /**
   * Publishes a fresh pre-filled window, or advances the one a concurrent admission published first.
   * The caller holds a reserved seat: the mapping consumes it when this call installs a window and
   * returns it with {@code releaseSeat()} when it finds a live one.
   *
   * <p>An empty bin takes {@link ConcurrentHashMap#putIfAbsent}'s lock-free CAS path, so the common
   * first-touch allocates neither the map's reservation node nor a capturing lambda. The ring
   * allocation and the slice advance then happen outside the bin lock, which leaves the create path
   * with no nested monitor at all. A tombstoned mapping is swapped atomically by
   * {@link ConcurrentHashMap#replace}; when it is gone already, the next {@code putIfAbsent} retries
   * as a plain creation.
   *
   * @param key      the key being admitted
   * @param delta    the positive count to place in a fresh window's first slot
   * @param nowSlice the caller's absolute slice number
   * @return {@code null} when the fresh window was installed, or the live window that won the race
   */
  @Nullable
  private KeyWindow publishOrGetExisting(String key, int delta, long nowSlice) {
    KeyWindow fresh = new KeyWindow(windowSize * 2, nowSlice, (int) (nowSlice & lengthMask), delta);
    for (;;) {
      KeyWindow existing = windows.putIfAbsent(key, fresh);
      if (existing == null) {
        return null;
      }
      if (advance(existing, nowSlice) != KeyWindow.EVICTED) {
        // A concurrent admission published the mapping: this thread's seat is surplus.
        reclaimer.releaseSeat();
        return existing;
      }
      if (windows.replace(key, existing, fresh)) {
        // The evictor tombstoned this mapping and is about to remove it; installing the fresh window
        // keeps the key live and consumes the caller's reserved seat.
        return null;
      }
    }
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
   * removes the mapping with an identity-matched remove. The monitor is released before the map's
   * bin lock is taken. Only the call that writes the tombstone reports {@code true}; the periodic
   * sweep returns the seat through {@link SamplingReclaimer#releaseSeat()}, the capacity sample
   * leaves it to its own admission.
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
   * Consume the "an admission at the cap found nothing idle" signal, so the periodic scan can run
   * once with the window-aligned idle boundary and a key is locked out for at most one sweep.
   *
   * @return {@code true} if relief was requested since the previous call
   */
  public boolean consumeReliefRequest() {
    return reclaimer.consumeReliefRequest();
  }

  /**
   * Sums the slots for the absolute slices {@code [endSlice - slotCount + 1, endSlice]}. A slot is
   * included only when its tag equals the slice it would represent, so a late write from a previous
   * rotation is ignored rather than re-attributed. The walk is split at the ring's end into at most
   * two forward runs, which keeps ascending indexes for the hardware prefetcher and pays no mask per
   * slot.
   *
   * @param slices     the doubled ring
   * @param endSlice   the newest absolute slice in the range
   * @param slotCount  the number of slices, at most {@code slices.length}
   * @param lengthMask bitmask for the doubled buffer index
   * @return the range's sum
   */
  static long sumRange(long[] slices, long endSlice, int slotCount, int lengthMask) {
    if (slotCount <= 0) {
      return 0L;
    }
    long sum = 0L;
    long slice = endSlice - slotCount + 1L;
    int start = (int) (slice & lengthMask);
    int capacity = lengthMask + 1;
    int firstRun = Math.min(slotCount, capacity - start);
    for (int i = 0; i < firstRun; i++, slice++) {
      long packed = (long) SLICES.getOpaque(slices, start + i);
      if (tag(packed) == (int) slice) {
        sum += count(packed);
      }
    }
    int remaining = slotCount - firstRun;
    for (int i = 0; i < remaining; i++, slice++) {
      long packed = (long) SLICES.getOpaque(slices, i);
      if (tag(packed) == (int) slice) {
        sum += count(packed);
      }
    }
    return sum;
  }

  /**
   * Read-only window sum for the given key.
   *
   * <p>The sum starts at the slice of the last write: later slices are empty, and the ring slot at
   * the current index can still hold a write from two windows ago after the index wraps. Beyond one
   * full window the key contributes zero and the buffer is not walked.
   *
   * <p>The cool-down path's only window read: {@code COOL_FOR_LOW_QPS} re-reads the sum under the key
   * lock instead of reusing the sample a report carried. {@code addCountAtSlice} already returns the
   * same quantity for the report it is judging, so the hot path does not call this.
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
   * The absolute slice a monotonic timestamp belongs to. {@code floorDiv}, not {@code /}: a negative
   * monotonic axis must not put instants on opposite sides of the epoch into the same slice.
   *
   * @param timestampMillis monotonic milliseconds
   * @return the absolute slice number
   */
  public long sliceIndex(long timestampMillis) {
    return Math.floorDiv(timestampMillis, timeMillisPerSlice);
  }

  /**
   * Evicts every window outside the slice-aligned "contributes zero" boundary, the predicate a
   * capacity reclaim and a concurrent read share.
   */
  public void evictIdle() {
    evict(windowSize);
  }

  /**
   * Evicts stale tracking entries for keys idle longer than the given timeout.
   *
   * <p>The retention bound is the caller's explicit memory bound, independent of the summation
   * range. Scan-then-recheck: a lock-free pass visits the entries, then each candidate is re-checked
   * and removed under its entry monitor with an identity-matched map removal. Because
   * {@link #addCountAtSlice} refreshes the slice under the same monitor, the two operations are
   * serialized per key and no add is lost.
   *
   * <p>The duration is quantized to whole slices once per scan (at most one slice of error) and
   * floored at one slice: the slice a fast-path writer may still be adding to must never be
   * reclaimed as idle.
   *
   * @param staleAfterMs maximum idle time in milliseconds before a key is considered stale and
   *                     evicted; must be non-negative, and at least one slice in effect
   * @throws IllegalArgumentException when {@code staleAfterMs} is negative
   */
  public void evictStale(long staleAfterMs) {
    if (staleAfterMs < 0L) {
      throw new IllegalArgumentException("staleAfterMs must be non-negative, got " + staleAfterMs);
    }
    evict(Math.max(1L, staleAfterMs / timeMillisPerSlice));
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
   * @return the number of tracked keys, including entries not yet evicted after going idle
   */
  public int activeWindows() {
    return windows.size();
  }
}
