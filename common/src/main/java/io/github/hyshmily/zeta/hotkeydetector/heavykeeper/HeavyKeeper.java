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
 * HeavyKeeper — a Count-Min Sketch variant for approximate Top‑K tracking
 * of frequently accessed keys.
 *
 * <p><b>Algorithm overview:</b> Uses a 2D counter array ({@code depth × width})
 * with per-slot fingerprint verification and probabilistic decay to estimate
 * the most frequent keys using bounded memory. Each key is hashed into one
 * bucket per row; if the stored fingerprint matches, the counter is
 * incremented; otherwise a probabilistic decay (sampled from a Binomial
 * distribution) determines whether the existing counter survives or is
 * replaced. A batch that strictly outweighs the occupant takes the slot
 * outright (size-aware takeover) — the lottery only adjudicates ties and
 * smaller claimants. This design excels at filtering out low-frequency items while
 * preserving high-frequency key rankings with low error rates.
 *
 * <p><b>Sliding-window decay:</b> each sketch slot maintains a ring buffer of
 * {@link #windowCount} time windows. {@link #fading()} advances the active window
 * and zeros the oldest one, so an estimate covers the most recent
 * {@code windowCount × decayInterval} of data and a slowly rising key is not held
 * back by stale high counts (design decisions: sketch).
 *
 * <p><b>Concurrency model:</b> Thread-safe with three tiers:
 * <ol>
 *   <li>Fine-grained striped monitors (up to 2048 stripes) guard one bucket's compound update
 *       (fingerprint, running sum and window). The critical section is a handful of {@code int}
 *       writes, which a monitor handles as cheaply as an explicit lock.</li>
 *   <li>Lock-free TopK membership updates: an existing member refreshes its count with a
 *       {@link VarHandle#compareAndExchange} max-raise, and the decay pass re-synchronises it
 *       with the same CAS. A plain volatile {@code int} keeps the member footprint smaller than
 *       an {@link AtomicInteger} and still supports both the raise and the reset atomically.</li>
 *   <li>A short, non-fair {@link ReentrantLock} ({@link #admissionLock}) guards admission and the
 *       decay sweep: admission when a new key crosses {@link #minCount} and may enter or evict
 *       from the bounded TopK, the sweep when member counts are re-synchronised and dropped. The
 *       admission path is O(k) and never touches the sketch; the sweep probes it lock-free.</li>
 * </ol>
 *
 * <p><b>Lock ordering</b> (design decisions: sketch): the sketch stripes and {@code admissionLock}
 * are never held together. {@link #fading} releases each stripe before taking the admission lock,
 * and {@link #addDirect}'s sketch update releases its stripes before {@code admit} takes the lock.
 * No callee acquires either lock, so the order cannot invert.
 *
 * <p><b>Memory and accuracy choices:</b>
 * <ul>
 *   <li>Decay samples a Binomial({@code cur}, {@code survivalProb}); a large slot uses a Gaussian
 *       approximation and a huge one its expectation directly. The per-collision work is
 *       constant, not proportional to the count.</li>
 *   <li>TopK members carry their fingerprint on the {@link Node}, captured at admission; a
 *       non-member computes it directly. No per-key fingerprint cache retains keys.</li>
 *   <li>Each slot keeps its running sum, so an increment never walks the window ring.</li>
 *   <li>Slot state is one flattened {@code int[]}: {@code [fingerprint, slotSum, w0 .. wN]}.
 *       Counts saturate at {@link Integer#MAX_VALUE}; an {@code int} write is atomic, and the
 *       stripe lock serialises the compound update rather than protecting tearing.</li>
 *   <li>A volatile {@code int} for {@link Node#count} with a {@link VarHandle} CAS, because
 *       counts are {@code int}-capped; the field avoids a separate atomic object per member.</li>
 * </ul>
 *
 * @see <a href="https://github.com/Hyshmily/Zeta/blob/master/docs/design-decisions.md">design decisions: sketch</a>
 */
@Slf4j
@Internal
public class HeavyKeeper implements TopK {

  /**
   * Release/acquire handle for a slot's fingerprint. The fingerprint is the slot's ownership tag:
   * writers publish it last, after the windows and the running sum, and the lock-free
   * {@link #probeSketch} reads it first with acquire ordering.
   */
  private static final VarHandle FINGERPRINT = MethodHandles.arrayElementVarHandle(int[].class);

  /**
   * {@code cur} threshold above which stochastic sampling is replaced by direct
   * expected-value rounding. When {@code cur > DIRECT_DECAY_THRESHOLD}
   * (2²⁰ ≈ 1 million), the Binomial variance is negligible relative to the
   * expectation — the random fluctuation is &lt;0.1% of the survival count — so
   * {@code Math.round(cur * survivalProb)} is used directly,
   * eliminating random-number generation overhead on huge slot sums.
   */
  private static final long DIRECT_DECAY_THRESHOLD = 1 << 20;

  /**
   * Log every Nth "Failed to offer" warning (admission-evicted and
   * decay-dropped keys share this counter) to avoid log flooding.
   * Kept a power of two so the rate check is a bitmask AND.
   */
  private static final int EXPELLED_LOG_INTERVAL = 1024;

  /**
   * Slot sum threshold above which soft heavy protection kicks in.
   * Slots whose running sum exceeds {@code PROTECTION_THRESHOLD} have their
   * per-collision decay capped to at most {@code cur * MAX_DECAY_RATIO},
   * preventing a single large cold-batch from disproportionately degrading
   * an established hot slot.
   */
  private static final long PROTECTION_THRESHOLD = 16;

  /**
   * Minimum {@code cur} threshold for the batch-decay fast path. When
   * {@code cur > BATCH_DECAY_THRESHOLD}, the survival count is
   * approximated via Gaussian (Binomial expectation and variance) instead
   * of invoking {@link #sampleBinomial}, saving the exact-sampling overhead.
   */
  private static final int BATCH_DECAY_THRESHOLD = 64;

  /**
   * Maximum fraction of a hot slot's count that can be decayed in a single
   * collision event. Only applies when {@code cur > PROTECTION_THRESHOLD}.
   */
  private static final double MAX_DECAY_RATIO = 0.25;

  /**
   * Floor of the {@link #raiseCount} deadband. The band is
   * {@code max(this, cur / 32)} — a few percent of the current estimate, but
   * never so small that a low-count member pays a locked CAS on every hit.
   */
  private static final int RAISE_DEADBAND_FLOOR = 16;

  /** CAS handle for a member's {@code count}; the field is a plain volatile int. */
  private static final VarHandle MEMBER_COUNT;

  /**
   * Sentinel from {@link #expelIfBelowThreshold(String)}: the member was kept
   * (its count was raised back to the threshold, or it was already gone), so
   * there is no departure to report.
   */
  private static final long KEPT = -1L;

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
   * Membership count below which a member is not worth its TopK seat, dropped
   * by {@link #decayMembership()}. Equal to {@code minCount} but with a floor
   * of {@code 1}: a member whose synchronised count reaches {@code 0} holds a
   * seat with no live estimate at all — every positively counted candidate
   * outranks it, and keeping it only starves a better candidate. The floor also
   * keeps a {@code minCount == 0} sketch expelling at all: with a zero
   * threshold a plain {@code < minCount} comparison would never fire.
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
   * Flattened per-slot state: {@code slotStride = 2 + windowCount} ints per
   * slot, laid out as {@code [fingerprint, slotSum, w0 .. w(windowCount-1)]}.
   *
   * <p><b>One slot, one cache line.</b> The fingerprint, the running sum and the
   * window a bucket update touches sit in adjacent ints, so every probe of a row
   * stays within one cache line instead of reaching three unrelated addresses.
   * With {@code width = 50_000}, {@code depth = 5} and four windows a single
   * array is several megabytes, so separate arrays would turn every row of every
   * add into an L2/L3 miss.
   *
   * <p>{@code int} storage halves the sketch footprint versus {@code long}.
   * Values are bounded: every window is zeroed on rotation (every 20 s, the
   * scheduled decay cadence) and decayed on collision, and the saturation guard
   * in {@link #applyIncrement} caps at {@link Integer#MAX_VALUE} — reaching the
   * cap requires &gt;10⁸ increments/s on a single slot, beyond any single-JVM
   * read path.
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
   * {@link String#hashCode()} passed through the Murmur3 finalizer.
   */
  private static int fingerprint(String key) {
    return Hashes.mix32(key.hashCode());
  }

  /**
   * Record {@code delta} accesses for {@code key} and return the TopK
   * membership decision.
   *
   * <p>This is the single-key entry point. Delegates to {@link #addOne} — the
   * Count-Min Sketch update ({@link #addToSketch}) followed by the TopK
   * membership decision ({@link #admit}: cold reject, lock-free refresh, or
   * lock-guarded admission/eviction).
   *
   * <p>Non-positive increments are malformed for an add-only sketch and are
   * ignored ({@link AddResult#cold()}), matching the {@code WaveCounter.add}
   * guard — the sketch has no decrement protocol, so a negative value would
   * otherwise corrupt windows and slot sums.
   */
  @Override
  public AddResult addDirect(String key, long delta) {
    if (key == null) {
      // Strict kernel: a null key is a caller bug, not a cold key. The
      // lenient null/blank mapping lives on HotKeyDetector, the cache-side edge.
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
   * Records one counted access for one key: sketch update followed by the
   * membership decision. Shared body of {@link #addDirect} and
   * {@link #ingestBatch} — the callers own the key and non-positive delta
   * guards, the saturation clamp, and the {@link #total} accounting, so a batch
   * can fold its per-key increments into one {@code total} update.
   *
   * <p><b>One map lookup:</b> the membership lookup that resolves the
   * member's captured fingerprint ({@link Node#fp}) is the same lookup the
   * membership decision needs, so both are served by a single
   * {@link ConcurrentHashMap#get} and the resolved {@link Node} travels
   * straight into {@link #admit}. Non-members compute the mixed fingerprint
   * directly (the upstream counter batch already amortises one hash per key
   * per flush).
   *
   * @param key   the key to record (non-null — the entry points reject a null
   *              key; the empty string is a legitimate key)
   * @param delta the positive, {@code int}-saturated increment to record
   */
  private AddResult addOne(String key, long delta) {
    Node member = members.get(key);
    int itemFingerprint = member != null ? member.fp : fingerprint(key);
    long maxCount = addToSketch(itemFingerprint, delta);
    return admit(key, maxCount, itemFingerprint, member);
  }

  /**
   * Ingest a flushed counting batch without allocating a <em>result list</em>.
   *
   * <p>The buffered counting path ({@code AccessCounter} tide) feeds every key
   * only to sustain the sketch — its {@link AddResult}s are discarded by the
   * caller, so building an {@code ArrayList} per flush is pure garbage. This
   * entry point runs the same per-key {@code addOne} work while skipping that
   * list (the admission path still builds its per-key {@link AddResult}, as on
   * every direct add). A null key is skipped rather than rejected: one bad
   * element must not discard the whole flush. Non-positive deltas are skipped.
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
    addBatchTotal(batchTotal);
  }

  /**
   * Fold one batch's increments into {@link #total} in a single
   * {@link LongAdder} update. {@code LongAdder} spreads writes across cells,
   * but every batch still pays one CAS (and a contended cell's retry) per key;
   * one update per flush keeps the per-batch cost constant instead of linear
   * in the batch size. Same guard shape as {@link #fading()}'s restore.
   *
   * @param batchTotal the sum of the batch's recorded increments
   */
  private void addBatchTotal(long batchTotal) {
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
   * Rotate the sliding window: advance the active window and zero the now-stale
   * window for every sketch slot, so an estimate covers the most recent
   * {@code windowCount × decayInterval} of data (design decisions: sketch).
   *
   * <p>The TopK membership counts are then re-synchronised from the sketch under
   * the {@link #admissionLock} via {@link #decayMembership()}, and members whose
   * synchronised count falls below {@link #expelBelowCount} are enqueued to
   * {@link #expelled()} alongside admission-evicted entries, so consumers of
   * {@link #expelled()} have a complete view of all TopK departures. The sketch
   * stripes and the admission lock are never held together, so no deadlock is
   * possible with concurrent {@link #addDirect} callers.
   *
   * <p>This operation is invoked automatically by the scheduler in
   * {@code ZetaSchedulingConfiguration} at a fixed 20-second interval.
   *
   * <p><b>Concurrency:</b> the whole method is {@code synchronized} on the
   * instance. Two concurrent {@code fading()} calls would both advance the
   * active window, re-synchronise membership and halve the total twice in one
   * interval (and skip a window rotation); the scheduler normally serializes
   * this, but the method is public API and must be safe under direct concurrent
   * use. The critical section is short (one sketch sweep + one membership sweep)
   * and runs at most once per decay interval, so the lock has no hot-path cost.
   *
   * <p><b>Total accounting:</b> {@code total.sumThenReset()} can drop a delta
   * landing on a cell between its sum and reset, and the restore below
   * intentionally re-adds only half of the observed sum. Both are bounded
   * transient drifts, self-correcting by the next cycle and never part of any
   * TopK decision.
   *
   * <p>The next window is zeroed <em>before</em> it is published as active, so
   * concurrent {@link #addToSketch} callers still see the previous window and
   * write elsewhere — eliminating the race where a concurrent
   * {@code addToSketch} writes into the window that
   * {@code rotateSketchWindows} then zeroes.
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
   * Lock-free read of the sketch's current windowed estimate for a fingerprint:
   * the largest running slot sum among the rows whose slot still carries it,
   * or {@code -1} when every row has been taken over.
   *
   * <p><b>-1 vs 0 is the load-bearing distinction.</b> A row that still carries
   * the fingerprint with a zero sum means the key's traffic has rolled out of
   * the window — the key is genuinely cold and its seat should be released. A
   * row whose fingerprint was replaced means the sketch can no longer see the
   * key at all, which a collision can do to a still-busy key. Only the first
   * justifies dropping the seat; the second must decay conservatively.
   *
   * <p>Deliberately lock-free. {@link #decayMembership()} calls this while
   * holding {@code admissionLock}; taking a stripe lock from there would be the
   * only path that acquires admission before a stripe. The fingerprint is
   * read with acquire ordering (writers publish it last), so an observed
   * fingerprint always comes with the window and sum written before it. A probe
   * that still sees the previous fingerprint may pair it with the newer payload —
   * an over-estimate that delays one eviction by at most one cycle, the safe
   * direction for a maintenance pass that runs once per decay interval.
   *
   * <p>Takes the maximum across rows, matching {@link #addToSketch}'s estimate
   * contract: an over-estimate delays eviction by at most one cycle, an
   * under-estimate drops a hot key.
   *
   * @param itemFingerprint the member's captured fingerprint
   * @return the current estimate (possibly 0), or -1 when no row carries the fingerprint
   */
  private long probeSketch(int itemFingerprint) {
    long max = 0;
    boolean present = false;
    for (int i = 0; i < depth; i++) {
      int base = (i * width + bucketIndex(itemFingerprint, i)) * slotStride;
      if ((int) FINGERPRINT.getAcquire(table, base) == itemFingerprint) {
        present = true;
        long sum = table[base + 1];
        if (sum > max) {
          max = sum;
        }
      }
    }
    return present ? max : -1L;
  }

  /**
   * Clamp an external delta to the sketch's {@code int} counter range.
   *
   * <p>Real increments are per-flush batch counts (thousands at most); a
   * value at or above 2³¹ is a malformed input and saturates like any other
   * counter — same stop-condition semantics as {@link #applyIncrement}'s
   * guard. Every entry point ({@link #addDirect}, {@link #ingestBatch})
   * rejects non-positive increments before reaching this point, so only the
   * upper bound needs clamping.
   */
  private static long saturateIncrement(long delta) {
    return Math.min(delta, Integer.MAX_VALUE);
  }

  /**
   * Apply {@code delta} to the sketch for the key whose fingerprint is
   * {@code itemFingerprint} and return the maximum cross-row slot sum
   * observed. The top-level loop dispatches to {@link #updateEmptySlot},
   * the matching-fingerprint fast path ({@link #applyIncrement}), the
   * size-aware takeover ({@link #takeoverSlot}, when the incoming batch
   * outweighs the occupant), or {@link #decayCollisionSlot} depending on the
   * slot's populated state and fingerprint match. The row's stripe lock is taken
   * here and released before the next row. Callers must pass a positive delta —
   * every entry point rejects non-positive values before reaching here.
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
   * Initialise an empty sketch slot: store the fingerprint, write the
   * {@code delta} into the active window, and reflect it in
   * the slot sum. Returns the running {@code maxCount} (largest slot
   * sum seen so far across all rows for this add call).
   */
  private long updateEmptySlot(int base, int active, int itemFingerprint, long delta, long maxCount) {
    long next = applyIncrement(base, active, delta, maxCount);
    // Ownership is published last: a lock-free probe that observes this fingerprint must also
    // observe the window and the running sum written before it.
    FINGERPRINT.setRelease(table, base, itemFingerprint);
    return next;
  }

  /**
   * Shared fast-path tail of {@link #updateEmptySlot} and the
   * matching-fingerprint branch of {@link #addToSketch}: add
   * {@code delta} to the active window
   * and mirror it into the slot sum, returning the running
   * {@code maxCount}.
   *
   * <p><b>Saturation guard:</b> both counters saturate at
   * {@link Integer#MAX_VALUE} instead of wrapping negative. A wrapped window
   * would look "already decayed" to {@link #decayCollisionSlot}'s survivor
   * check and be skipped by it; a wrapped slot sum would misrank the slot.
   * Each guard is one comparison on the hot path, and saturation is a
   * stop-condition rather than steady state — {@link #fading()} zeroes
   * windows on rotation and {@code decayCollisionSlot} decays slot sums, so
   * a slot can only reach the cap under extreme continuous traffic.
   *
   * <p>Callers pass a positive {@code delta} (all entry points reject
   * non-positive values), so the guards only need the overflow comparison.
   */
  private long applyIncrement(int base, int active, long delta, long maxCount) {
    int off = base + 2 + active;
    int window = table[off] + (int) delta;
    if (window < 0) {
      window = Integer.MAX_VALUE; // saturation guard: an overflowing window would decay-skip
    }
    table[off] = window;
    int sum = table[base + 1] + (int) delta;
    if (sum < 0) {
      sum = Integer.MAX_VALUE;
    }
    table[base + 1] = sum;
    return Math.max(maxCount, sum);
  }

  /**
   * Hand an occupied slot to the incoming fingerprint: swap the fingerprint, wipe
   * all windows, and replay the {@code delta} into the active window.
   *
   * <p>Shared by the two handover paths — the size-aware takeover in
   * {@link #addToSketch} (a strictly bigger batch displaces the occupant outright)
   * and the full-displacement lottery win inside {@link #decayCollisionSlot}.
   * Keeping one body guarantees both paths leave the slot in the identical state.
   * Callers must hold the slot's stripe lock; {@code delta} is already
   * saturated to the {@code int} range by both public entry points.
   */
  private long takeoverSlot(int base, int active, int itemFingerprint, long delta, long maxCount) {
    int winBase = base + 2;
    Arrays.fill(table, winBase, winBase + windowCount, 0);
    table[winBase + active] = (int) delta;
    table[base + 1] = (int) delta;
    // Ownership last, for the same acquire/release reason as updateEmptySlot.
    FINGERPRINT.setRelease(table, base, itemFingerprint);
    return Math.max(maxCount, delta);
  }

  /**
   * Collision-with-different-fingerprint path: sample the number of survivors
   * from a Binomial({@code cur}, {@code decay}) — each of the occupant's
   * existing units survives with the constant per-unit probability
   * {@code decay} (design decisions: sketch) — and either hand the slot over to the incoming
   * fingerprint (full reset) or proportionally decay every window. Returns the
   * running {@code maxCount}.
   *
   * <p>This is reached only when the incoming batch does <em>not</em> outweigh
   * the occupant ({@code delta <= cur}) — a strictly bigger newcomer takes
   * the slot deterministically via {@link #takeoverSlot} before sampling, so
   * the lottery here only ever adjudicates ties and smaller claimants.
   *
   * <p><b>Estimate contract:</b> a collision contributes <b>zero</b> to the
   * caller's {@code maxCount} unless the slot changes hands. The estimate
   * aggregates rows whose fingerprint <em>matched</em> this key, so a row
   * still owned by another key can never inflate it (see design decisions: sketch). The decay
   * itself is still applied — the occupant's window is a real cost of the
   * collision, it just is not this key's mass.
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
    // Decrement each window proportionally, keep the slot sum in sync.
    //
    // The remainder is carried across windows (Bresenham): each window's share is
    // floor((wv * decays + carry) / cur) and the modulo becomes the next window's
    // carry, so the shares sum to exactly `decays` whenever the windows' total
    // equals `cur`. Independent per-window flooring dropped the whole decrement
    // when `decays` was smaller than the number of non-empty windows — with
    // decays=1 spread over three windows every share floored to zero and the
    // collision decayed nothing at all.
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
    table[base + 1] = (int) Math.max(0, sumBefore - totalSubtracted);
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
            table[base + 1] = sum;
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
   * <p><b>The sketch is the single source of truth.</b> Its slot sums are a true
   * sliding window: a key whose traffic stopped reaches zero exactly when the
   * window rolls past it, so {@link #probeSketch} makes the seat expire with
   * the window (design decisions: sketch).
   *
   * <p><b>Fallback when the sketch has no trace:</b> if every row has taken the
   * fingerprint over, {@code probeSketch} returns {@code -1}. That means "the
   * sketch cannot see this key", not "the key is cold" — collisions can evict
   * all copies of a still-busy key. Those members halve rather than drop, so a
   * collision costs one cycle instead of the seat.
   *
   * <p><b>Why the threshold is {@code minCount}, not zero:</b> a member below
   * {@code minCount} can never be admitted again — {@link #admit} rejects
   * anything with {@code maxCount < minCount} — so its seat is pure
   * starvation for a better candidate, and its presence keeps
   * {@link #minMemberCount} (the O(1) fast-reject threshold) artificially low.
   * See {@link #expelBelowCount} for the floor that keeps a {@code minCount == 0}
   * sketch expelling at all.
   *
   * <p><b>Bounded residual race:</b> the first-loop double-check keeps the
   * dropped list small, and the final decision is re-made atomically at
   * removal time (see below). A lock-free fast-path bump landing between the
   * final count read and the mapping removal (a sub-microsecond window) is
   * still possible; such a key is re-admitted on its next {@link #addDirect}
   * — a self-healing, bounded transient, not a permanent loss.
   */
  private void decayMembership() {
    admissionLock.lock();
    try {
      List<String> dropped = null;
      for (Node n : members.values()) {
        // One CAS loop, re-derived on every retry so a concurrent raise from
        // admit()'s lock-free fast path is judged by the same rule instead of
        // being overwritten. probeSketch is deliberately lock-free: taking a
        // stripe lock here would invert the class's stripe → admission order.
        int current = synchronizeMemberCount(n);
        if (current < expelBelowCount) {
          // Double-check: admit()'s lock-free fast path may have raised the
          // count between our CAS and here. If so, keep the member — don't lose
          // a hot key to a TOCTOU race.
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
   * Re-synchronise one member's count with the sketch, retrying against a
   * concurrent raise or decay. A move that would not change the value returns
   * without a CAS.
   *
   * @param node the member to synchronise
   * @return the value in the field after the call
   */
  private int synchronizeMemberCount(Node node) {
    for (;;) {
      int prev = node.count;
      long observed = probeSketch(node.fp);
      // observed >= 0 means a row still carries the fingerprint: the value is
      // authoritative even when it is 0 (the window rolled past this key). -1
      // means every row was taken over — halve, so a collision costs one cycle
      // rather than the seat.
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
   * Remove one member unless its count was raised back to
   * {@link #expelBelowCount} since {@link #decayMembership()}'s first pass.
   *
   * <p>The decision is re-made atomically inside the map's per-key critical
   * section because the lock-free {@link #admit} fast path can revive a count
   * at any moment; a revive is never lost to a stale decision. The departure
   * count is captured in the same critical section so {@link #offerExpelled}
   * reports what the member actually held rather than a hard-coded zero.
   *
   * @param key the candidate member
   * @return the count the member held at departure, or {@link #KEPT} when it
   *         survived (or had already been removed by another path)
   */
  private long expelIfBelowThreshold(String key) {
    long[] departedAt = { KEPT };
    members.compute(key, (k, node) -> {
      if (node == null) {
        return null; // already departed — whoever removed it reported it
      }
      long count = node.count;
      if (count >= expelBelowCount) {
        return node;
      }
      departedAt[0] = count;
      return null;
    });
    return departedAt[0];
  }

  /**
   * Apply the TopK membership decision for a key whose sketch estimate is
   * {@code maxCount}.
   *
   * <p>Hot path (key already a member): a lock-free
   * {@link VarHandle#compareAndExchange} max-raise lifts the member's observed
   * count to {@code maxCount} — no monitor is taken. The raise is preceded by a plain read: when the
   * sketch estimate has not grown past the current count (the common case for
   * an established hot key between decay cycles), the locked CAS is skipped
   * entirely; a raise skipped because a concurrent {@link #decayMembership}
   * sync landed first is re-applied by the next flush.
   * Below-threshold path (any key whose sketch estimate is under
   * {@code minCount}): returns {@link AddResult#cold()} without locking — a
   * member in this state is left untouched and re-lifted by a later flush whose
   * estimate clears the threshold. Admission path (key not yet a member but
   * {@code maxCount >= minCount}):
   * takes {@link #admissionLock}, resolves the minimum and runner-up via
   * {@link #findTwoSmallest()}, and either inserts the new key (evicting the
   * minimum when the set is full and the new count qualifies) or rejects it.
   * Eviction populates {@link #expelledQueue} on the write path to preserve
   * the {@link AddResult#expelledKey()} contract.
   *
   * @param key             the key to admit or refresh
   * @param maxCount        the sketch estimate backing the decision
   * @param itemFingerprint the key's resolved sketch fingerprint
   * @param resolvedMember  the member {@link #addOne} already resolved for
   *                        this key, or {@code null} when it is not a member.
   *                        Passing it removes the second
   *                        {@link ConcurrentHashMap#get}: the caller had to
   *                        resolve membership anyway to source the member's
   *                        captured fingerprint.
   */
  private AddResult admit(String key, long maxCount, int itemFingerprint, @Nullable Node resolvedMember) {
    if (maxCount < minCount) {
      return AddResult.cold();
    }
    // Fast path: existing member — atomic max-raise, no lock. The raise targets
    // the node resolved before the sketch update; if a concurrent eviction
    // detached it in that window the raise is simply lost, and the next flush
    // re-admits the key on its still-qualifying sketch estimate — the same
    // self-healing bounded transient decayMembership's removal race documents.
    if (resolvedMember != null) {
      raiseCount(resolvedMember, (int) maxCount);
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
   * Raise a member's count to {@code maxCount} when the estimate has grown past
   * it by enough to be worth publishing.
   *
   * <p><b>Why a deadband.</b> A hit adds its batch to the sketch before this
   * runs, so {@code maxCount} exceeds the stored count on every hit of a hot
   * key; a strict compare-and-raise would execute a locked CAS on
   * <em>every</em> hit, taking the node's cache line exclusive each time.
   * Borrowing {@code percpu_counter}'s batch threshold, the write is skipped
   * until the growth reaches {@code max(16, cur / 32)}: the stored count only
   * feeds TopK ordering and the eviction floor, both of which tolerate a few
   * percent of lag. A member near {@link #minMemberCount} still raises eagerly —
   * there the lag would decide whether it keeps its seat.
   *
   * <p>A max-raise skipped because a {@link #decayMembership} sync landed first
   * is a bounded transient re-applied by the next qualifying hit.
   *
   * @param node     the member to raise
   * @param maxCount the sketch's current estimate for the key
   */
  private void raiseCount(Node node, int maxCount) {
    int cur = node.count;
    if (maxCount <= cur) {
      return;
    }
    int step = Math.max(RAISE_DEADBAND_FLOOR, cur >>> 5);
    if (maxCount - cur < step && cur > minMemberCount + step) {
      return;
    }
    for (;;) {
      if (maxCount <= cur) {
        return;
      }
      int witness = (int) MEMBER_COUNT.compareAndExchange(node, cur, maxCount);
      if (witness == cur) {
        return;
      }
      cur = witness;
    }
  }

  /**
   * Offer a departing key to {@link #expelledQueue}, shared by both offer
   * sites (write-path eviction in {@link #evictMinAndPut} and the
   * {@link #decayMembership} drop). A full queue drops the departure
   * (tolerated by design), and the shared {@link #expelledLogCounter}
   * rate-limits the WARN to one log per {@link #EXPELLED_LOG_INTERVAL}
   * failures across both sites.
   *
   * @param key          the departing key
   * @param count        the key's count at departure time
   * @param siteLabel    WARN text fragment identifying the offer site
   *                     ({@code "expelled key"} / {@code "decay-dropped key"})
   */
  private void offerExpelled(String key, long count, String siteLabel) {
    if (expelledQueue.offer(new Item(key, count))) {
      return;
    }
    if ((expelledLogCounter.getAndIncrement() & (EXPELLED_LOG_INTERVAL - 1)) == 0) {
      log.warn("Failed to offer {}: {} ({} suppressed since last log)", siteLabel, key, EXPELLED_LOG_INTERVAL);
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
   * Admit or evict under {@link #admissionLock}. Only called when the key is
   * not yet a member and the O(1) fast-reject ({@link #minMemberCount}) has
   * passed. Expects caller to hold the lock. The O(k) scan of
   * {@link #findTwoSmallest()} runs once on the full path — twice only when the
   * resolved minimum turns out stale — and its candidate is handed straight to
   * {@link #evictMinAndPut}, which does not re-scan the membership; the closing
   * {@code minMemberCount} refresh reuses that scan's runner-up instead of
   * walking the map again.
   */
  private AddResult admitOrEvict(String key, long maxCount, int itemFingerprint) {
    if (members.size() < k) {
      members.put(key, new Node(key, maxCount, itemFingerprint));
      notifyAdmitted(key);
      if (maxCount < minMemberCount || members.size() == 1) {
        minMemberCount = maxCount;
      }

      return new AddResult(null, true, key);
    }

    // Full — must evict the minimum member (O(k) scan). The same pass also
    // resolves the runner-up, which is the new minimum after the eviction; a
    // second scan would double the O(k) work inside the admission lock for a
    // value one comparison carries.
    MinPair pair = findTwoSmallest();
    Node min = pair.min();
    if (maxCount < min.count) {
      minMemberCount = min.count;
      return AddResult.cold();
    }
    // Staleness guard: raiseCount mutates member counts without the admission
    // lock, so the minimum read a moment ago may since have been lifted above
    // the newcomer. Evicting it would swap a hotter key out for a colder one,
    // so re-resolve in that (rare) case.
    if (isStaleMinimum(min, maxCount)) {
      pair = findTwoSmallest();
      min = pair.min();
      if (maxCount < min.count) {
        minMemberCount = min.count;
        return AddResult.cold();
      }
    }

    String expelledKey = evictMinAndPut(key, maxCount, itemFingerprint, min);
    // The evicted member was the smallest and the newcomer takes its place at
    // maxCount, so the new minimum is the smaller of the runner-up and the
    // newcomer.
    minMemberCount = Math.min(maxCount, pair.secondCount());
    return new AddResult(expelledKey, true, key);
  }

  /**
   * Whether {@code min} has been overtaken since it was resolved: a lock-free
   * {@link #raiseCount} pushed its count above the newcomer's. Every membership
   * removal runs under {@link #admissionLock}, so the node reference cannot be
   * detached while this check runs.
   *
   * @param min      the minimum node as resolved by {@link #findTwoSmallest}
   * @param maxCount the newcomer's count
   * @return {@code true} when {@code min} must be re-resolved before evicting
   */
  private boolean isStaleMinimum(Node min, long maxCount) {
    return min.count > maxCount;
  }

  /**
   * Evict {@code min} and insert {@code key} at {@code count}. Called from the
   * write path ({@link #admitOrEvict}, which passes the minimum it already
   * resolved and whose staleness it has already ruled out) so that
   * <b>every</b> membership departure is reported to
   * {@link #expelledQueue}: the {@link AddResult#expelledKey()} contract
   * requires all departures to be visible, and a bare map removal would drop
   * the evicted snapshot key silently.
   *
   * <p>Expects caller to hold {@link #admissionLock} and to have already
   * verified the candidate beats the current minimum.
   *
   * @param min the minimum member to evict, or {@code null} when nothing is to
   *             be evicted
   * @return the key evicted to make room, or {@code null} when nothing was evicted
   */
  private String evictMinAndPut(String key, long count, int itemFingerprint, @Nullable Node min) {
    String expelledKey = null;
    if (min != null && members.remove(min.key, min)) {
      expelledKey = min.key;
      // The count is read here, not from the scan snapshot: this is the
      // member's departure value, and a lock-free raise may have moved it.
      offerExpelled(expelledKey, min.count, "expelled key");
    }
    members.put(key, new Node(key, count, itemFingerprint));
    notifyAdmitted(key);
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
   * Resolve the smallest member and the second-smallest count in one pass.
   *
   * <p>Eviction needs both: the minimum picks the victim, and the runner-up is
   * what the minimum becomes after that victim is replaced by the newcomer. One
   * pass resolves both, so the runner-up costs no second O(k) walk under
   * {@code admissionLock}.
   *
   * <p>{@code secondCount} stays {@link Long#MAX_VALUE} when fewer than two
   * members exist, so {@code min(newcomer, secondCount)} still yields the
   * newcomer. Ties are resolved on the key the same way as
   * {@link #findMinMember()}.
   *
   * @return the minimum candidate plus the second-smallest count
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
    return new MinPair(min, secondCount);
  }

  /**
   * Snapshot the current membership as immutable {@link Item}s, sorted by count
   * descending (ties broken on key ascending), limited to at most {@code limit}
   * entries. The snapshot behind {@link #listTopN(int)}, and therefore
   * {@link #list()}.
   *
   * <p><b>Snapshot-before-sort:</b> each member's count is read <b>once</b>
   * into the {@link Item} <em>before</em> the sort runs. Member counts are
   * concurrently mutated (lock-free max-raise in {@link #admit}, CAS
   * re-synchronisation in {@link #decayMembership}), and a comparator that
   * re-reads a mutable counter mid-sort can feed TimSort an inconsistent total
   * order — it then aborts with
   * {@code "Comparison method violates its general contract!"} (reachable once
   * the membership exceeds TimSort's MIN_MERGE of 32 and the merge passes run,
   * e.g. k=100 introspection from {@code ZetaEndpoint}). Sorting the immutable
   * snapshot is total-order-consistent by construction, and the reported counts
   * are the same values the ordering was computed from.
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
      snapshot.add(new Item(n.key, n.count));
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
   * A key-count pair used as an entry in the TopK membership set.
   * {@link #count} is a volatile {@code int} mutated through
   * {@link #MEMBER_COUNT}: max-raise on the hot path and sketch
   * re-synchronisation during periodic decay (halving only when the sketch no
   * longer holds the key).
   *
   * <p>{@code int} is safe: member counts are raised to the sketch estimate
   * (int-capped, see {@link #addToSketch}) and re-synchronised from the
   * sketch every decay period; {@link Integer#MAX_VALUE} is a stop-condition,
   * not a working value.
   */
  @SuppressWarnings("ClassCanBeRecord")
  private static final class Node {

    /** The cache key. */
    final String key;
    /** Mixed 32-bit fingerprint, captured once at admission — a pure function of {@link #key}. */
    final int fp;

    /** Current estimated count — support for CAS raise and decay. */
    volatile int count;

    Node(String key, long count, int fp) {
      this.fp = fp;
      this.key = key;
      this.count = (int) Math.min(count, Integer.MAX_VALUE);
    }
  }

  /** The two smallest members, resolved by {@link #findTwoSmallest()}. */
  private record MinPair(Node min, long secondCount) {}

  /**
   * Sample from a Binomial(n, p) distribution in O(1) expected time.
   *
   * <p>Uses direct simulation for small n (≤10), a normal approximation for
   * {@code np(1-p) > 5.0}, and a Poisson approximation for the remaining
   * small-λ case. The Poisson approximation uses Knuth's algorithm with
   * O(λ) expected iterations where λ = np. The {@code p > 0.5} mirror case
   * recurses once on the complement, so it never nests deeper than one level.
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
