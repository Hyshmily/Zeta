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
package io.github.hyshmily.zeta.hotkeydetector.heavykeeper;

import io.github.hyshmily.zeta.util.Hashes;
import io.github.hyshmily.zeta.util.Internal;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.util.Assert;

/**
 * HeavyKeeper — a Count-Min Sketch variant for approximate Top‑K tracking of frequently accessed
 * keys.
 *
 * <p>Each key hashes into one bucket per row. A matching fingerprint increments the bucket; a
 * collision is resolved by a strictly larger batch taking the slot over, or by probabilistic
 * decay of the occupant. A per-slot ring of {@link #windowCount} windows is rotated by
 * {@link #fading()}, so an estimate covers the most recent
 * {@code windowCount × decayInterval}.
 *
 * <p>Thread-safe: stripe monitors guard one bucket's compound update, member counts use a
 * lock-free {@link VarHandle} max-raise, and {@link #admissionLock} guards admission and the
 * decay sweep. The sketch stripes and {@code admissionLock} are never held together, so the
 * acquisition order cannot invert.
 *
 * <p>design decisions: sketch.
 *
 * @see <a href="https://github.com/Hyshmily/Zeta/blob/master/docs/design-decisions.md">design decisions: sketch</a>
 */
@Slf4j
@Internal
public class HeavyKeeper implements TopK {

  /**
   * Release/acquire handle for a slot word: the fingerprint ({@code base}) and the running
   * sum ({@code base + 1}). A writer publishes the fingerprint last, after the windows and the
   * running sum; the lock-free {@link #probeSketch} reads it first with acquire ordering and
   * re-checks it after reading the sum.
   */
  private static final VarHandle SLOT = MethodHandles.arrayElementVarHandle(int[].class);

  /**
   * {@code cur} threshold above which stochastic sampling is replaced by direct
   * expected-value rounding: the Binomial variance is negligible relative to the
   * expectation, so the extra random-number generation buys nothing.
   */
  private static final long DIRECT_DECAY_THRESHOLD = 1 << 20;

  /**
   * Log every Nth "Failed to offer" warning (both offer sites share the counter).
   * A power of two so the rate check is a bitmask AND.
   */
  private static final int EXPELLED_LOG_INTERVAL = 1024;

  /**
   * Slot-sum threshold above which per-collision decay is capped at
   * {@code cur * MAX_DECAY_RATIO}, so one large cold batch cannot disproportionately
   * degrade an established hot slot.
   */
  private static final long PROTECTION_THRESHOLD = 16;

  /**
   * Minimum {@code cur} for the Gaussian batch-decay approximation; below it the
   * exact {@link #sampleBinomial} sampler is used.
   */
  private static final int BATCH_DECAY_THRESHOLD = 64;

  /** Maximum fraction of a hot slot's count decayed by one collision above {@link #PROTECTION_THRESHOLD}. */
  private static final double MAX_DECAY_RATIO = 0.25;

  /**
   * Floor of the {@link #raiseCount} deadband: {@code max(this, cur / 32)} keeps a
   * low-count member from paying a locked CAS on every hit.
   */
  private static final int RAISE_DEADBAND_FLOOR = 16;

  /** CAS handle for a member's {@code count}; the field is a plain volatile int. */
  private static final VarHandle MEMBER_COUNT;

  /**
   * Sentinel from {@link #expelIfBelowThreshold(String)}: the member was kept, so
   * there is no departure to report.
   */
  private static final long KEPT = -1L;

  /**
   * Membership tombstone written into a member's count when the node leaves the map.
   * Counts are non-negative, so a fast-path raise that observes it must take the
   * locked admission path instead of reporting the node hot.
   */
  private static final int DETACHED = -1;

  static {
    try {
      MEMBER_COUNT = MethodHandles.privateLookupIn(Node.class, MethodHandles.lookup()).findVarHandle(
        Node.class,
        "count",
        int.class
      );
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /** Counts failed expelled-queue offers; the WARN is logged once per {@link #EXPELLED_LOG_INTERVAL}. */
  private final AtomicInteger expelledLogCounter = new AtomicInteger();

  /**
   * Authoritative TopK membership map. Key → {@link Node} with a volatile
   * count supporting atomic max-raise on the hot path and atomic decay. Size is bounded by {@link #k} and
   * enforced by {@link #admissionLock}. Reads and writes of the count on
   * existing members are lock-free.
   */
  private final ConcurrentHashMap<String, Node> members;

  /** Bounded blocking queue receiving expelled (evicted) key-count items for downstream consumption. */
  private final BlockingQueue<Item> expelledQueue;

  /** Decayed running total of recorded increments, exposed by {@link #total()}. */
  private final LongAdder total = new LongAdder();

  /**
   * Short, non-fair lock guarding the compound admission/eviction step on
   * {@link #members} (read-min, insert, evict) and the decay sweep that
   * re-synchronises and drops members. The hot max-raise path does not acquire
   * this lock.
   */
  private final ReentrantLock admissionLock;

  /** Striped lock objects for fine-grained concurrency on sketch slot updates. */
  private final Object[] lockStripes;

  /** Constant per-unit survival probability ({@code decay}) for collision decay. */
  private final double survivalProb;

  /**
   * Membership count below which a member is dropped by {@link #decayMembership()}:
   * {@code max(1, minCount)}. A member at zero has no live estimate and only starves
   * a better candidate; the floor of 1 keeps a {@code minCount == 0} sketch expelling.
   */
  private final int expelBelowCount;

  /** Number of time windows per sketch slot (ring buffer depth). */
  private final int windowCount;

  /** Ints per slot in {@link #table}: fingerprint + running sum + the window ring. */
  private final int slotStride;

  /** Slot count ({@code depth * width}) — the bound for whole-table sweeps. */
  private final int totalSlots;

  /** Bitmask for window index when windowCount is a power of two ({@code windowCount - 1}). */
  private final int windowMask;

  /** Bitmask for bucket index when {@link #width} is a power of two ({@code width - 1}). */
  private final int widthMask;

  /** Minimum sketch estimate a key needs before it may enter the TopK set. */
  private final int minCount;

  /** Bitmask for mapping bucket index to lock stripe (stripe count must be power of two). */
  private final int lockMask;

  /**
   * Flattened per-slot state: {@code slotStride = 2 + windowCount} ints per slot,
   * {@code [fingerprint, slotSum, w0 .. w(windowCount-1)]}. Adjacent ints keep every
   * row probe within one cache line; {@code int} caps the footprint. The saturation
   * guard in {@link #applyIncrement} keeps values at or below
   * {@link Integer#MAX_VALUE}.
   */
  private final int[] table;

  private final int width;

  private final int depth;

  private final int k;

  /** {@code true} when {@link #width} is a power of two — enables mask-based bucket lookup. */
  private final boolean widthIsPow2;

  /**
   * Optional admission observer. Invoked once for every key that newly enters
   * the TopK set, under {@link #admissionLock}; the listener must not block,
   * because it runs on the admitting thread — a {@code DETECT_ONLY} add runs it
   * on the read-path caller.
   */
  private volatile Consumer<String> admissionListener;

  /**
   * Lowest member count across the TopK, maintained by the admission path and
   * the decay sweep. Read as an O(1) fast-reject threshold by {@link #admit}.
   */
  private volatile long minMemberCount;

  /** Active sketch window index, advanced once per {@link #fading()}. */
  private volatile int activeWindow;

  /**
   * Construct a HeavyKeeper instance with full configuration.
   *
   * @param k                     maximum number of hot keys to track
   * @param width                 width of the Count-Min Sketch (number of columns per row)
   * @param depth                 depth of the Count-Min Sketch (number of rows / hash functions)
   * @param decay                 probabilistic decay factor (0.0–1.0); higher values preserve counts longer
   * @param minCount              minimum count threshold before a key can enter the TopK set
   * @param expelledQueueCapacity capacity of the bounded blocking queue for expelled items
   * @param windowCount           number of time windows per sketch slot (ring buffer depth, at
   *                              least 2; rounded up to a power of two when the bean is created)
   * @param autoAlignWidth        when {@code true} and width is not a power of two, automatically
   *                              round up to the nearest power of two for faster bucket indexing
   */
  public HeavyKeeper(
    int k,
    int width,
    int depth,
    double decay,
    int minCount,
    int expelledQueueCapacity,
    int windowCount,
    boolean autoAlignWidth
  ) {
    Assert.isTrue(k > 0, "k must be > 0, but got: " + k);
    Assert.isTrue(width > 0, "width must be > 0, but got: " + width);
    Assert.isTrue(depth > 0, "depth must be > 0, but got: " + depth);
    Assert.isTrue(decay >= 0.0 && decay <= 1.0, "decay must be in [0.0, 1.0], but got: " + decay);
    Assert.isTrue(minCount >= 0, "minCount must be >= 0, but got: " + minCount);
    Assert.isTrue(windowCount >= 2, "windowCount must be >= 2, but got: " + windowCount);
    Assert.isTrue(expelledQueueCapacity > 0, "expelledQueueCapacity must be > 0, but got: " + expelledQueueCapacity);

    if (autoAlignWidth && (width & (width - 1)) != 0) {
      // highestOneBit(width - 1) << 1 overflows into the sign bit above 2^30.
      Assert.isTrue(
        width <= (1 << 30),
        "width must be <= 1073741824 when autoAlignWidth is enabled, but got: " + width
      );
      int original = width;
      width = Integer.highestOneBit(width - 1) << 1;
      log.info("Auto-aligned width from {} to {} to enable bitmask optimization", original, width);
    }

    Assert.isTrue(windowCount <= (1 << 20), "windowCount must be <= 1048576, but got: " + windowCount);
    if ((windowCount & (windowCount - 1)) != 0) {
      int original = windowCount;
      windowCount = Integer.highestOneBit(windowCount - 1) << 1;
      log.info("Auto-aligned windowCount from {} to {} to enable bitmask optimization", original, windowCount);
    }

    this.k = k;
    this.width = width;
    this.depth = depth;
    this.minCount = minCount;
    this.windowCount = windowCount;
    this.windowMask = windowCount - 1;
    this.expelBelowCount = Math.max(1, minCount);

    this.survivalProb = decay;

    int totalSlots;
    int tableLength;
    try {
      totalSlots = Math.multiplyExact(depth, width);
      tableLength = Math.multiplyExact(totalSlots, 2 + windowCount);
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(
        "sketch geometry too large: depth=" + depth + ", width=" + width + ", windowCount=" + windowCount,
        e
      );
    }
    int stripes = Math.min(2048, Math.max(64, totalSlots >> 4));
    if ((stripes & (stripes - 1)) != 0) {
      stripes = Integer.highestOneBit(stripes - 1) << 1;
    }
    this.totalSlots = totalSlots;
    this.slotStride = 2 + windowCount;
    this.table = new int[tableLength];
    this.lockStripes = new Object[stripes];
    for (int i = 0; i < stripes; i++) {
      lockStripes[i] = new Object();
    }
    this.lockMask = stripes - 1;
    this.widthIsPow2 = width > 0 && (width & (width - 1)) == 0;
    if (!widthIsPow2) {
      log.warn(
        "Width {} is not a power of two; bucket index will use the FastRange mapping " +
          "(RocksDB fastrange.h). Recommended: use a power of two for bitmask performance.",
        width
      );
    }
    this.widthMask = width - 1;

    this.admissionLock = new ReentrantLock();
    this.members = new ConcurrentHashMap<>(k);
    this.expelledQueue = new ArrayBlockingQueue<>(expelledQueueCapacity);
  }

  /**
   * Compute the bucket index for row {@code i} given the key fingerprint,
   * using a fast bit-mask when {@link #width} is a power of two and a
   * FastRange mapping otherwise (adapted from RocksDB
   * {@code util/fastrange.h}): the high half of the {@code width × uint32}
   * product consumes the full 32-bit fingerprint entropy and replaces the
   * division with a single multiplication.
   */
  private int bucketIndex(int itemFingerprint, int row) {
    int hash = itemFingerprint ^ (int) (row * 0x9e3779b97f4a7c15L);
    return widthIsPow2 ? (hash & widthMask) : (int) (((hash & 0xFFFFFFFFL) * width) >>> 32);
  }

  /**
   * 32-bit fingerprint for sketch slot indexing: the key's cached
   * {@link String#hashCode()} passed through the Murmur3 finalizer. {@code 0} is the
   * switching sentinel, so a real owner never carries it.
   */
  private static int fingerprint(String key) {
    int fingerprint = Hashes.mix32(key.hashCode());
    return fingerprint == 0 ? 1 : fingerprint;
  }

  /**
   * Record {@code delta} accesses for {@code key} and return the TopK membership
   * decision: the sketch update followed by {@link #admit}.
   *
   * <p>A non-positive increment is ignored ({@link AddResult#cold()}): the sketch has
   * no decrement protocol, so a negative value would corrupt windows and slot sums.
   */
  @Override
  public AddResult addDirect(String key, long delta) {
    if (key == null) {
      // Strict kernel: a null key is a caller bug. HotKeyDetector maps invalid keys.
      throw new NullPointerException("key must not be null");
    }
    if (delta <= 0) {
      return AddResult.cold();
    }

    long increment = saturateIncrement(delta);
    total.add(increment);
    return addOne(key, increment);
  }

  /**
   * Records one counted access: sketch update followed by the membership decision.
   * Both entry points own the key, non-positive-delta and saturation guards and the
   * {@link #total} accounting, so a batch can fold its increments into one update.
   *
   * <p>The membership lookup resolves the member's captured fingerprint, so it serves
   * both the fingerprint and the admission decision in one {@link ConcurrentHashMap#get}.
   *
   * @param key   the key to record (non-null; the empty string is a legitimate key)
   * @param delta the positive, {@code int}-saturated increment to record
   */
  private AddResult addOne(String key, long delta) {
    Node member = members.get(key);
    int itemFingerprint = member != null ? member.fp : fingerprint(key);
    long maxCount = addToSketch(itemFingerprint, delta);
    return admit(key, maxCount, itemFingerprint, member);
  }

  /**
   * Ingest a flushed counting batch without allocating a result list: the caller
   * discards every {@link AddResult}. A null key or non-positive delta is skipped so
   * one bad element cannot discard the whole flush.
   *
   * @param keyCounts map of keys to their access counts
   */
  public void ingestBatch(Map<String, Long> keyCounts) {
    if (keyCounts == null || keyCounts.isEmpty()) {
      return;
    }
    long batchTotal = 0;
    for (Map.Entry<String, Long> entry : keyCounts.entrySet()) {
      String key = entry.getKey();
      Long delta = entry.getValue();
      if (key == null || delta == null || delta <= 0L) {
        continue;
      }
      long increment = saturateIncrement(delta);
      batchTotal += increment;
      addOne(key, increment);
    }
    // One LongAdder update per flush keeps the cost constant instead of linear in the batch.
    if (batchTotal > 0) {
      total.add(batchTotal);
    }
  }

  /**
   * Register the admission observer. The listener runs under
   * {@link #admissionLock} on the admitting thread and must only hand the key
   * to a bounded queue (or perform an equally cheap non-blocking action).
   *
   * @param listener the observer, or {@code null} to detach
   */
  public void setAdmissionListener(@Nullable Consumer<String> listener) {
    this.admissionListener = listener;
  }

  /**
   * Return the current TopK list sorted by frequency descending.
   *
   * <p>Delegates to {@link #listTopN(int)} with {@link #k} as the limit,
   * equivalent to requesting all tracked hot keys.
   *
   * @return list of {@link Item} entries, never {@code null}
   */
  @Override
  public List<Item> list() {
    return listTopN(k);
  }

  /**
   * Return the top {@code limit} hot keys sorted by frequency (descending),
   * with ties broken by key name (ascending).
   *
   * <p>Takes a lock-free snapshot of the current membership, sorts it, and
   * returns at most {@code limit} entries. Each {@link Item} carries the count
   * captured when the snapshot pair was taken (see
   * {@link #snapshotItemsSorted}), so the ordering and the reported counts are
   * mutually consistent even under concurrent membership mutation. A negative
   * {@code limit} is rejected.
   *
   * @param limit maximum number of keys to return
   * @return list of at most {@code limit} {@link Item} entries
   * @throws IllegalArgumentException if {@code limit} is negative
   */
  @Override
  public List<Item> listTopN(int limit) {
    Assert.isTrue(limit >= 0, "limit must be non-negative, but got: " + limit);
    if (limit == 0 || members.isEmpty()) {
      return Collections.emptyList();
    }
    return snapshotItemsSorted(limit);
  }

  /**
   * Check whether the given key is currently in the TopK set.
   *
   * <p>This is an O(1) lookup — a direct {@link ConcurrentHashMap#containsKey}
   * call instead of a sorted-list scan.
   *
   * @param key the key to check
   * @return {@code true} if the key is a current TopK member
   */
  @Override
  public boolean contains(String key) {
    return members.containsKey(key);
  }

  /**
   * Return the bounded blocking queue of expelled (evicted) key-count items.
   *
   * <p>Consumers should drain this queue periodically for asynchronous
   * processing (e.g. monitoring, logging, or follow-up eviction actions).
   * When the queue is full, further departure events are dropped and the WARN
   * is rate-limited per {@link #EXPELLED_LOG_INTERVAL} failures, so the
   * admission path is never blocked by a slow consumer.
   *
   * @return the {@link BlockingQueue} of evicted {@link Item entries}
   */
  @Override
  public BlockingQueue<Item> expelled() {
    return expelledQueue;
  }

  /**
   * Rotate the sliding window: advance the active window and zero the stale one for
   * every sketch slot, re-synchronise membership counts under {@link #admissionLock},
   * and enqueue members that fall below {@link #expelBelowCount} to
   * {@link #expelled()}.
   *
   * <p>Guarded by the instance monitor: two concurrent calls would advance the window
   * and halve {@link #total} twice in one interval. The next window is zeroed
   * <em>before</em> it is published as active, so a concurrent {@link #addToSketch}
   * still sees the previous window and never writes into a window being zeroed.
   *
   * <p>{@code total.sumThenReset()} can drop a delta landing between sum and reset,
   * and only half the observed sum is restored; both are bounded, self-correcting
   * drifts that never feed a TopK decision.
   */
  @Override
  public synchronized void fading() {
    int nextAw = (activeWindow + 1) & windowMask;
    rotateSketchWindows(nextAw);
    activeWindow = nextAw;
    decayMembership();

    long half = total.sumThenReset() >> 1;
    if (half > 0) {
      total.add(half);
    }
  }

  /**
   * Return the approximate number of distinct keys currently tracked in the
   * TopK set. This is an O(1) call backed by {@link ConcurrentHashMap#size()}.
   *
   * @return number of keys in the hot set (at most {@link #k})
   */
  @Override
  public int estimatedSize() {
    return members.size();
  }

  @Override
  public long total() {
    return total.sum();
  }

  /**
   * Lock-free read of the sketch's current windowed estimate for a fingerprint: the
   * largest running slot sum among the rows that still carry it, or {@code -1} when
   * every row has been taken over.
   *
   * <p>{@code 0} and {@code -1} differ: a row that still carries the fingerprint with a
   * zero sum means the key's traffic rolled out of the window and its seat should be
   * released; {@code -1} means the sketch is blind to a possibly busy key, so the
   * caller must decay conservatively. Taking the maximum matches
   * {@link #addToSketch}'s over-estimate contract.
   *
   * <p>Deliberately lock-free: {@link #decayMembership()} calls this under
   * {@code admissionLock}, and taking a stripe lock here would invert the class's
   * stripe → admission order. The read is a seqlock: the fingerprint is read with acquire
   * ordering, the sum with acquire ordering, and the fingerprint is re-read. A takeover
   * clears the fingerprint before it rewrites the sum, so a matching second read means the
   * payload belongs to this key — an older owner's fingerprint can never be paired with the
   * incoming key's count.
   *
   * @param itemFingerprint the member's captured fingerprint
   * @return the current estimate (possibly 0), or -1 when no row carries the fingerprint
   */
  private long probeSketch(int itemFingerprint) {
    long max = 0;
    boolean present = false;
    for (int i = 0; i < depth; i++) {
      int base = (i * width + bucketIndex(itemFingerprint, i)) * slotStride;
      if ((int) SLOT.getAcquire(table, base) != itemFingerprint) {
        continue;
      }
      long sum = (int) SLOT.getAcquire(table, base + 1);
      if ((int) SLOT.getAcquire(table, base) != itemFingerprint) {
        // The slot changed hands after the first read: its new payload is not this key's.
        continue;
      }
      present = true;
      if (sum > max) {
        max = sum;
      }
    }
    return present ? max : -1L;
  }

  /**
   * Clamp an external delta to the sketch's {@code int} counter range. Entry points
   * reject non-positive values, so only the upper bound needs clamping.
   */
  private static long saturateIncrement(long delta) {
    return Math.min(delta, Integer.MAX_VALUE);
  }

  /**
   * Apply {@code delta} to the sketch for the key whose fingerprint is
   * {@code itemFingerprint} and return the maximum cross-row slot sum observed.
   * Dispatch: empty slot, matching fingerprint, size-aware takeover, or collision
   * decay. The row's stripe lock is released before the next row. Callers pass a
   * positive delta.
   */
  private long addToSketch(int itemFingerprint, long delta) {
    long maxCount = 0;

    for (int i = 0; i < depth; i++) {
      int index = i * width + bucketIndex(itemFingerprint, i);
      Object lock = lockStripes[index & lockMask];
      int base = index * slotStride;

      //noinspection SynchronizationOnLocalVariableOrMethodParameter
      synchronized (lock) {
        // Re-read the active window inside the stripe lock. fading() zeroes the
        // next window BEFORE publishing it as active, so this read always
        // targets a window that rotateSketchWindows has already passed (or has
        // not yet reached), making the two operations disjoint per window.
        int active = activeWindow;

        long cur = table[base + 1];
        if (cur == 0) {
          maxCount = updateEmptySlot(base, active, itemFingerprint, delta, maxCount);
        } else if (table[base] == itemFingerprint) {
          maxCount = applyIncrement(base, active, delta, maxCount);
        } else if (delta > cur) {
          maxCount = takeoverSlot(base, active, itemFingerprint, delta, maxCount);
        } else {
          maxCount = decayCollisionSlot(base, active, itemFingerprint, delta, cur, maxCount);
        }
      }
    }
    return maxCount;
  }

  /**
   * Initialise an empty sketch slot: detach the previous owner, then write the window and sum,
   * and publish the fingerprint last.
   */
  private long updateEmptySlot(int base, int active, int itemFingerprint, long delta, long maxCount) {
    // The sum reads 0 but the slot may still carry the previous owner's fingerprint: park it
    // on the switching sentinel before the payload moves, so a concurrent probe cannot pair
    // the old fingerprint with this key's count.
    SLOT.setVolatile(table, base, 0);
    long next = applyIncrement(base, active, delta, maxCount);
    // Ownership is published last: a lock-free probe that observes this fingerprint must also
    // observe the window and the running sum written before it.
    SLOT.setRelease(table, base, itemFingerprint);
    return next;
  }

  /**
   * Shared fast-path tail of {@link #updateEmptySlot} and the matching-fingerprint
   * branch of {@link #addToSketch}: add {@code delta} to the active window and mirror
   * it into the slot sum.
   *
   * <p>The increment is clamped against the slot sum's remaining headroom, so the
   * window and the sum stay in lockstep at {@link Integer#MAX_VALUE}. Saturation is a
   * stop-condition, not steady state; callers pass a positive delta.
   */
  private long applyIncrement(int base, int active, long delta, long maxCount) {
    int curSum = table[base + 1];
    int actualDelta = (int) Math.min(delta, (long) Integer.MAX_VALUE - curSum);
    int sum = curSum + actualDelta;
    table[base + 2 + active] += actualDelta;
    SLOT.setRelease(table, base + 1, sum);
    return Math.max(maxCount, sum);
  }

  /**
   * Hand an occupied slot to the incoming fingerprint: swap the fingerprint, wipe all
   * windows, and replay {@code delta} into the active window. Both handover paths
   * share this body so they leave identical state. Callers hold the stripe lock.
   */
  private long takeoverSlot(int base, int active, int itemFingerprint, long delta, long maxCount) {
    // Detach the previous owner before the payload moves: a probe that read the old
    // fingerprint then sees the sentinel (or the new owner) on its second read and discards
    // the sum it observed in between.
    SLOT.setVolatile(table, base, 0);
    int winBase = base + 2;
    Arrays.fill(table, winBase, winBase + windowCount, 0);
    table[winBase + active] = (int) delta;
    SLOT.setRelease(table, base + 1, (int) delta);
    // Ownership last, for the same acquire/release reason as updateEmptySlot.
    SLOT.setRelease(table, base, itemFingerprint);
    return Math.max(maxCount, delta);
  }

  /**
   * Collision-with-different-fingerprint path: sample the survivors from
   * Binomial({@code cur}, {@code decay}) and either hand the slot over to the incoming
   * fingerprint (full reset) or proportionally decay every window.
   *
   * <p>A collision contributes zero to the caller's {@code maxCount} unless the slot
   * changes hands: a row still owned by another key must not inflate this key's
   * estimate. The occupant's decay is still applied.
   */
  @SuppressWarnings({ "null", "squid:S2245" })
  private long decayCollisionSlot(int base, int active, int itemFingerprint, long delta, long cur, long maxCount) {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    // Constant per-unit survival probability: each of the cur existing units
    // independently survives the collision with probability decay, so the
    // number of surviving units is Binomial(cur, decay).
    double prob = survivalProb;

    long survivals;
    if (cur > DIRECT_DECAY_THRESHOLD) {
      survivals = Math.round(cur * prob);
    } else if (cur > BATCH_DECAY_THRESHOLD) {
      double expected = cur * prob;
      double variance = expected * (1.0 - prob);
      double noise = Math.sqrt(variance) * random.nextGaussian();
      survivals = Math.round(expected + noise);
      survivals = Math.max(0, Math.min(survivals, cur));
    } else {
      survivals = sampleBinomial(cur, prob, random);
    }
    long decays = cur - survivals;

    if (cur > PROTECTION_THRESHOLD) {
      long maxDecays = Math.max(1, (long) (cur * MAX_DECAY_RATIO));
      decays = Math.min(decays, maxDecays);
    }

    if (decays == 0) {
      // The survival draw kept every unit: the proportional pass below would compute
      // sub = 0 for every window and write each value back unchanged. Callers reach here
      // with cur > 0, so 0 can never satisfy the full-displacement test.
      return maxCount;
    }

    if (decays >= cur) {
      // Full-displacement lottery win: same handover as the size-aware path.
      return takeoverSlot(base, active, itemFingerprint, delta, maxCount);
    }
    int winBase = base + 2;
    // Decrement each window proportionally, keeping the slot sum in sync. The
    // remainder is carried across windows (Bresenham), so the shares sum to exactly
    // `decays` whenever the windows total `cur`.
    //
    // Long arithmetic is required but does not overflow: both factors are at most
    // 2^31, so the product is at most 2^62, and the carry stays below `cur`.
    long sumBefore = table[base + 1];
    long totalSubtracted = 0;
    long carry = 0;

    for (int w = 0; w < windowCount; w++) {
      int off = winBase + w;
      long wv = table[off];
      if (wv > 0) {
        long num = wv * decays + carry;
        long sub = num / cur;
        carry = num % cur;
        long newVal = Math.max(0, wv - sub);
        totalSubtracted += wv - newVal;
        table[off] = (int) newVal;
      }
    }
    SLOT.setRelease(table, base + 1, (int) Math.max(0, sumBefore - totalSubtracted));
    // The slot still belongs to the occupant: the incoming key contributed
    // nothing to it, so it must contribute nothing to the caller's estimate.
    // Returning the occupant's decayed slotSum here would let any cold key
    // sharing a slot inherit a hot key's count — a delta=1 collision could
    // admit it to the TopK and evict the real member.
    return maxCount;
  }

  /**
   * Zero the now-stale window ({@code aw}) for every sketch slot under its
   * owning stripe lock, keeping each slot's running sum in sync.
   */
  private void rotateSketchWindows(int aw) {
    for (int stripe = 0; stripe < lockStripes.length; stripe++) {
      synchronized (lockStripes[stripe]) {
        for (int i = stripe; i < totalSlots; i += lockStripes.length) {
          int base = i * slotStride;
          int off = base + 2 + aw;
          int oldWindow = table[off];
          if (oldWindow != 0) {
            table[off] = 0;
            int sum = table[base + 1] - oldWindow;
            if (sum < 0) {
              // Defensive clamp: the window sum can only exceed the running sum after
              // the two saturation guards diverge, never from a concurrent update
              // (the stripe lock serialises every writer of this slot).
              sum = 0;
            }
            SLOT.setRelease(table, base + 1, sum);
          }
        }
      }
    }
  }

  /**
   * Re-synchronise every TopK membership count with the sketch under
   * {@link #admissionLock}, and drop members whose count falls below
   * {@link #expelBelowCount}.
   *
   * <p>The sketch is the single source of truth: a member's count is re-read from its
   * slot sums, so a key whose traffic stopped reaches zero exactly when the window
   * rolls past it. When every row has been taken over, {@link #probeSketch} returns
   * {@code -1} — the sketch is blind, not the key cold — and the member halves instead
   * of dropping.
   *
   * <p>A member below {@code minCount} can never be re-admitted and only depresses
   * {@link #minMemberCount}, so it is dropped. A lock-free raise landing between the
   * final read and the removal is re-admitted on its next add.
   */
  private void decayMembership() {
    admissionLock.lock();
    try {
      List<String> dropped = null;
      for (Node n : members.values()) {
        // One CAS loop; the value is re-derived on every retry so a concurrent raise
        // is judged by the same rule instead of being overwritten.
        int current = synchronizeMemberCount(n);
        if (current < expelBelowCount) {
          // Double-check: admit()'s lock-free fast path may have raised the count
          // between our CAS and here. If so, keep the member.
          if (n.count >= expelBelowCount) {
            continue;
          }
          if (dropped == null) {
            dropped = new ArrayList<>();
          }
          dropped.add(n.key);
        }
      }
      if (dropped != null) {
        for (String key : dropped) {
          long departedAt = expelIfBelowThreshold(key);
          if (departedAt != KEPT) {
            // Same loss accounting as the admission path in evictMinAndPut:
            // a full expelledQueue silently drops the decay-drop, and the
            // shared expelledLogCounter rate-limits the WARN (one per
            // EXPELLED_LOG_INTERVAL failures across both offer sites).
            offerExpelled(key, departedAt, "decay-dropped key");
          }
        }
      }
      Node min = findMinMember();
      minMemberCount = min == null ? 0L : min.count;
    } finally {
      admissionLock.unlock();
    }
  }

  /**
   * Re-synchronise one member's count with the sketch, retrying against a concurrent
   * raise or decay. A move that would not change the value returns without a CAS.
   *
   * @param node the member to synchronise
   * @return the value in the field after the call
   */
  private int synchronizeMemberCount(Node node) {
    for (;;) {
      int prev = node.count;
      long observed = probeSketch(node.fp);
      // observed >= 0: a row still carries the fingerprint, so the value is
      // authoritative even at 0. -1: every row was taken over — halve, costing one
      // cycle rather than the seat.
      int next = observed >= 0 ? (int) Math.min(observed, Integer.MAX_VALUE) : prev >> 1;
      if (next == prev) {
        return prev;
      }
      int witness = (int) MEMBER_COUNT.compareAndExchange(node, prev, next);
      if (witness == prev) {
        return next;
      }
    }
  }

  /**
   * Remove one member unless its count was raised back to {@link #expelBelowCount}
   * since {@link #decayMembership()}'s first pass.
   *
   * <p>The decision is re-made atomically inside the map's per-key critical section,
   * so a concurrent revive is never lost; the departure count is captured in the same
   * section.
   *
   * @param key the candidate member
   * @return the count the member held at departure, or {@link #KEPT} when it survived
   */
  private long expelIfBelowThreshold(String key) {
    long[] departedAt = { KEPT };
    members.compute(key, (k, node) -> {
      if (node == null) {
        return null; // already departed — whoever removed it reported it
      }
      for (;;) {
        int count = node.count;
        if (count >= expelBelowCount) {
          return node;
        }
        // Tombstone the departing node atomically: a lock-free raise that won the race is
        // captured in the departure count instead of being overwritten by the removal.
        if ((int) MEMBER_COUNT.compareAndExchange(node, count, DETACHED) == count) {
          departedAt[0] = count;
          return null;
        }
      }
    });
    return departedAt[0];
  }

  /**
   * Apply the TopK membership decision for a key whose sketch estimate is
   * {@code maxCount}.
   *
   * <p>An existing member is max-raised lock-free; a concurrent eviction tombstones
   * its node, so the failed raise falls through to the locked admission path and
   * re-admits the key. A key below {@code minCount} is rejected without locking.
   * The admission path takes {@link #admissionLock}, re-checks membership, applies the
   * O(1) minimum fast-reject, then inserts or evicts.
   *
   * @param key             the key to admit or refresh
   * @param maxCount        the sketch estimate backing the decision
   * @param itemFingerprint the key's resolved sketch fingerprint
   * @param resolvedMember  the member {@link #addOne} already resolved, or {@code null};
   *                        passing it removes a second {@link ConcurrentHashMap#get}
   */
  private AddResult admit(String key, long maxCount, int itemFingerprint, @Nullable Node resolvedMember) {
    if (maxCount < minCount) {
      return AddResult.cold();
    }
    // Fast path: an existing member is max-raised without the lock. A concurrent
    // eviction marks the node DETACHED; the failed raise falls through to the locked
    // path, which re-admits the key and fires the admission observer.
    if (resolvedMember != null && raiseCount(resolvedMember, (int) maxCount)) {
      return new AddResult(null, true, key);
    }
    // Admission path: brand-new candidate, may enter or evict.
    admissionLock.lock();
    try {
      // Double-check under lock — another thread may have admitted this key.
      Node existing = members.get(key);
      if (existing != null) {
        raiseCount(existing, (int) maxCount);
        return new AddResult(null, true, key);
      }
      // O(1) fast reject: can't beat the current minimum member.
      if (members.size() >= k && maxCount < minMemberCount) {
        return AddResult.cold();
      }
      return admitOrEvict(key, maxCount, itemFingerprint);
    } finally {
      admissionLock.unlock();
    }
  }

  /**
   * Raise a member's count to {@code maxCount} when the estimate has grown past it by
   * enough to be worth publishing.
   *
   * <p>The deadband ({@code max(16, cur / 32)}) avoids a locked CAS on every hit: the
   * stored count only feeds TopK ordering and the eviction floor, both of which
   * tolerate a few percent of lag, while a member near {@link #minMemberCount} still
   * raises eagerly because there the lag decides its seat. Observing the
   * {@link #DETACHED} tombstone returns {@code false} so the caller re-admits.
   *
   * @param node     the member to raise
   * @param maxCount the sketch's current estimate for the key
   * @return {@code true} when the node is still a live member
   */
  private boolean raiseCount(Node node, int maxCount) {
    int cur = node.count;
    if (cur < 0) {
      return false;
    }
    if (maxCount <= cur) {
      return true;
    }
    int step = Math.max(RAISE_DEADBAND_FLOOR, cur >>> 5);
    if (maxCount - cur < step && cur > minMemberCount + step) {
      return true;
    }
    for (;;) {
      if (cur < 0) {
        return false;
      }
      if (maxCount <= cur) {
        return true;
      }
      int witness = (int) MEMBER_COUNT.compareAndExchange(node, cur, maxCount);
      if (witness == cur) {
        return true;
      }
      cur = witness;
    }
  }

  /**
   * Offer a departing key to {@link #expelledQueue}. A full queue drops the departure
   * (tolerated by design); the shared {@link #expelledLogCounter} rate-limits the WARN
   * across both offer sites.
   *
   * @param key       the departing key
   * @param count     the key's count at departure
   * @param siteLabel WARN text fragment identifying the offer site
   */
  private void offerExpelled(String key, long count, String siteLabel) {
    if (expelledQueue.offer(new Item(key, count))) {
      return;
    }
    long failures = expelledLogCounter.getAndIncrement();
    if ((failures & (EXPELLED_LOG_INTERVAL - 1)) == 0L) {
      if (failures == 0L) {
        log.warn("Failed to offer {}: {}", siteLabel, key);
      } else {
        log.warn("Failed to offer {}: {} ({} suppressed since last log)", siteLabel, key, EXPELLED_LOG_INTERVAL - 1L);
      }
    }
  }

  /**
   * Tell the admission observer that {@code key} just entered the TopK set.
   * Runs under {@link #admissionLock}; a listener failure must never break
   * admission (the event is an optimization, the TopK state is authoritative).
   */
  private void notifyAdmitted(String key) {
    Consumer<String> listener = admissionListener;
    if (listener == null) {
      return;
    }

    try {
      listener.accept(key);
    } catch (RuntimeException e) {
      log.debug("Admission listener failed for key={}: {}", key, e.toString());
    }
  }

  /**
   * Admit or evict under {@link #admissionLock}. Called only when the key is not yet a
   * member and the O(1) fast-reject has passed. The O(k) scan is reused for the victim
   * and the runner-up, so the closing {@code minMemberCount} refresh never walks the
   * map again.
   */
  private AddResult admitOrEvict(String key, long maxCount, int itemFingerprint) {
    if (members.size() < k) {
      members.put(key, new Node(key, maxCount, itemFingerprint));
      if (maxCount < minMemberCount || members.size() == 1) {
        minMemberCount = maxCount;
      }
      notifyAdmitted(key);

      return new AddResult(null, true, key);
    }

    // Full — must evict the minimum member (O(k) scan). The same pass also
    // resolves the runner-up, which is the new minimum after the eviction; a
    // second scan would double the O(k) work inside the admission lock for a
    // value one comparison carries.
    MinPair pair = findTwoSmallest();
    if (maxCount < pair.minCount()) {
      // maxCount loses against the scan's snapshot, and member counts only grow between
      // scans, so the snapshot is a valid lower bound for the fast-reject gate. Reading the
      // node's live count here instead can put the gate above members it should admit.
      minMemberCount = pair.minCount();
      return AddResult.cold();
    }
    // Staleness guard: raiseCount mutates member counts without the admission
    // lock, so the minimum resolved a moment ago may since have been lifted
    // above the newcomer — or above the runner-up, which would evict a hotter
    // key. Re-resolve in that (rare) case.
    if (isStaleMinimum(pair, maxCount)) {
      pair = findTwoSmallest();
      if (maxCount < pair.minCount()) {
        minMemberCount = pair.minCount();
        return AddResult.cold();
      }
    }

    String expelledKey = evictMinAndPut(key, maxCount, itemFingerprint, pair.min());
    // The evicted member was the smallest and the newcomer takes its place at
    // maxCount, so the new minimum is the smaller of the runner-up and the
    // newcomer. Written before the admission callback runs, so an observer never
    // sees the new member with the previous gate.
    minMemberCount = Math.min(maxCount, pair.secondCount());
    notifyAdmitted(key);
    return new AddResult(expelledKey, true, key);
  }

  /**
   * Whether the pair's minimum has been overtaken since it was resolved: a lock-free
   * raise pushed its count above the newcomer's or above the runner-up, so evicting it
   * would swap out a hotter key. Every removal runs under {@link #admissionLock}, so
   * the node cannot detach while this check runs.
   *
   * @param pair     the minimum and runner-up resolved by {@link #findTwoSmallest}
   * @param maxCount the newcomer's count
   * @return {@code true} when the pair must be re-resolved before evicting
   */
  private boolean isStaleMinimum(MinPair pair, long maxCount) {
    long liveMinCount = pair.min().count;
    return liveMinCount > pair.secondCount() || liveMinCount > maxCount;
  }

  /**
   * Evict {@code min} and insert {@code key} at {@code count}. Every membership
   * departure is reported to {@link #expelledQueue} so the
   * {@link AddResult#expelledKey()} contract holds. Expects
   * {@link #admissionLock} and a verified winner.
   *
   * @param min the minimum member to evict, or {@code null} when nothing is evicted
   * @return the key evicted to make room, or {@code null}
   */
  private String evictMinAndPut(String key, long count, int itemFingerprint, @Nullable Node min) {
    String expelledKey = null;
    if (min != null) {
      // Tombstone first: a lock-free raise that observes it falls through to the admission
      // path, and one that already won is captured in the departure count.
      int finalCount = (int) MEMBER_COUNT.getAndSet(min, DETACHED);
      if (members.remove(min.key, min)) {
        expelledKey = min.key;
        offerExpelled(expelledKey, Math.max(0, finalCount), "expelled key");
      }
    }
    members.put(key, new Node(key, count, itemFingerprint));
    return expelledKey;
  }

  /**
   * Whether {@code (count, key)} beats the current scan minimum under the
   * eviction ordering: lowest count first, ties broken on the key's natural
   * ordering, so eviction is deterministic. One owner — {@link #findMinMember}
   * and {@link #findTwoSmallest} must apply the same rule or the victim pick
   * and the runner-up bookkeeping disagree.
   */
  private static boolean beatsAsMin(long count, String key, long minCount, String minKey) {
    return count < minCount || (count == minCount && (minKey == null || key.compareTo(minKey) < 0));
  }

  /**
   * Find the current minimum member, breaking ties on the key's natural
   * ordering (lowest count, then lowest key) so eviction is deterministic.
   *
   * <p>Delegates to {@link #findTwoSmallest()} so the victim pick and the
   * runner-up bookkeeping cannot drift apart.
   *
   * @return the minimum node, or {@code null} when no members exist
   */
  private Node findMinMember() {
    return findTwoSmallest().min();
  }

  /**
   * Resolve the smallest member and the second-smallest count in one pass. The
   * minimum picks the victim, its scanned count is the sound comparison against the
   * newcomer, and the runner-up is the new minimum after the eviction — so no second
   * O(k) walk is needed.
   *
   * <p>{@code secondCount} stays {@link Long#MAX_VALUE} when fewer than two members
   * exist. Ties resolve on the key as in {@link #beatsAsMin}.
   *
   * @return the minimum candidate, its scanned count and the second-smallest count
   */
  private MinPair findTwoSmallest() {
    long minCount = Long.MAX_VALUE;
    long secondCount = Long.MAX_VALUE;
    String minKey = null;
    Node min = null;

    for (Node n : members.values()) {
      long c = n.count;
      if (beatsAsMin(c, n.key, minCount, minKey)) {
        // The previous minimum becomes the runner-up candidate. When the new
        // value ties the old minimum, secondCount simply keeps that same value.
        secondCount = minCount;
        minCount = c;
        minKey = n.key;
        min = n;
      } else if (c < secondCount) {
        secondCount = c;
      }
    }
    return new MinPair(min, minCount, secondCount);
  }

  /**
   * Snapshot the current membership as immutable {@link Item}s, sorted by count
   * descending (ties on key ascending), limited to {@code limit} entries.
   *
   * <p>Each count is read once, before the sort. Member counts are mutated
   * concurrently, so a comparator that re-read the counter could feed TimSort an
   * inconsistent order and abort with "Comparison method violates its general
   * contract!". Sorting the immutable snapshot is consistent by construction.
   */
  List<Item> snapshotItemsSorted(int limit) {
    if (members.isEmpty()) {
      return Collections.emptyList();
    }

    // Snapshot phase: one count read per member — the comparator never touches
    // a mutable counter, so concurrent mutations cannot destabilize the sort.
    // Weakly consistent iteration: a member admitted mid-snapshot may or may
    // not appear.
    List<Item> snapshot = new ArrayList<>(members.size());
    for (Node n : members.values()) {
      // A node tombstoned immediately before its map removal is not a member any more; the
      // weakly consistent walk must not publish the negative sentinel as a count.
      snapshot.add(new Item(n.key, Math.max(0L, n.count)));
    }
    snapshot.sort((a, b) -> {
      int c = Long.compare(b.count(), a.count());
      return c != 0 ? c : a.key().compareTo(b.key());
    });
    // Truncation returns a detached list: the snapshot is a private working
    // buffer, and a caller must not observe it change or retain its capacity.
    return snapshot.size() > limit ? new ArrayList<>(snapshot.subList(0, limit)) : snapshot;
  }

  /**
   * A key-count pair in the TopK membership set. {@link #count} is a volatile
   * {@code int} mutated through {@link #MEMBER_COUNT}: max-raise on the hot path and
   * sketch re-synchronisation during decay. The negative {@link #DETACHED} value is
   * written under {@code admissionLock} when the node leaves the membership. Counts are
   * {@code int}-capped; {@link Integer#MAX_VALUE} is a stop-condition.
   */
  @SuppressWarnings("ClassCanBeRecord")
  private static final class Node {

    /** The cache key. */
    final String key;
    /** Mixed 32-bit fingerprint, captured once at admission — a pure function of {@link #key}. */
    final int fp;

    /** Current estimated count, or {@link #DETACHED} once the node has left the membership. */
    volatile int count;

    Node(String key, long count, int fp) {
      this.fp = fp;
      this.key = key;
      this.count = (int) Math.min(count, Integer.MAX_VALUE);
    }
  }

  /**
   * The two smallest members, resolved by {@link #findTwoSmallest()}: the
   * minimum node, its count at scan time and the second-smallest count.
   */
  private record MinPair(Node min, long minCount, long secondCount) {}

  /**
   * Sample from a Binomial(n, p) distribution in O(1) expected time.
   *
   * <p>Direct simulation for small n (≤10), a normal approximation for
   * {@code np(1-p) > 5.0}, and a Poisson approximation otherwise. The {@code p > 0.5}
   * mirror case recurses once on the complement.
   */
  private static long sampleBinomial(long n, double p, ThreadLocalRandom random) {
    if (n <= 0) {
      return 0;
    }
    if (p >= 1.0) {
      return n;
    }
    if (p <= 0.0) {
      return 0;
    }

    if (p > 0.5) {
      // Complement trick: add failures instead — the recursive call runs
      // with p' = 1 - p <= 0.5, so it never recurses a second level.
      return n - sampleBinomial(n, 1.0 - p, random);
    }

    double q = 1.0 - p;

    if (n <= 10) {
      long k = 0;
      for (long i = 0; i < n; i++) {
        if (random.nextDouble() < p) {
          k++;
        }
      }
      return k;
    }

    double np = n * p;
    double npq = np * q;

    if (npq > 5.0) {
      long k = Math.round(np + Math.sqrt(npq) * random.nextGaussian());
      return Math.max(0, Math.min(n, k));
    }

    double limit = Math.exp(-np);
    long k = 0;
    double prod = 1.0;
    do {
      k++;
      prod *= random.nextDouble();
    } while (prod > limit);
    return Math.min(k - 1, n);
  }
}
