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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.util.Assert;

/**
 * HeavyKeeper — a Count-Min Sketch variant for approximate Top-K tracking of frequently accessed
 * keys.
 *
 * <p>Each key hashes into one bucket per row. A matching fingerprint increments the bucket; a
 * collision is resolved by a strictly larger batch taking over the slot, or by probabilistic
 * occupant decay with per-unit probability {@code decay^count}, exponentially protecting hot
 * keys from cold churn. A per-slot ring of {@link #windowCount} windows rotates on every
 * {@link #fading()} call so estimates reflect the most recent
 * {@code windowCount * decayInterval}.
 *
 * <p><b>Thread safety.</b> Striped monitors guard individual sketch slot updates, member counts
 * use lock-free {@link VarHandle} max-raises, and {@link #admissionLock} serializes Top-K
 * admission, eviction, and membership decay. Sketch stripes and {@link #admissionLock} are never
 * held simultaneously, preventing lock-order inversion.
 *
 * @see <a href="https://github.com/Hyshmily/Zeta/blob/master/docs/design-decisions.md">design decisions: sketch</a>
 */
@Slf4j
@Internal
public class HeavyKeeper implements TopK {

  /**
   * Acquire/release {@link VarHandle} over {@link #table} words. Writers publish the slot
   * fingerprint last (after window and running-sum updates); lock-free {@link #probeSketch}
   * reads the fingerprint before and after the sum as a seqlock.
   */
  private static final VarHandle SLOT = MethodHandles.arrayElementVarHandle(int[].class);

  /** CAS {@link VarHandle} over {@link Node#count}. */
  private static final VarHandle MEMBER_COUNT;

  /** Slot word offset of the running window sum relative to a slot base. */
  private static final int SLOT_SUM_OFFSET = 1;

  /** Slot word offset where the per-slot window ring begins relative to a slot base. */
  private static final int SLOT_WINDOWS_OFFSET = 2;

  /** Fixed header words per slot: fingerprint + running sum. */
  private static final int SLOT_HEADER_SIZE = SLOT_WINDOWS_OFFSET;

  /** Number of precomputed {@code decay^count} probabilities for the common small-count path. */
  private static final int DECAY_TABLE_SIZE = 256;

  /** Probabilities below this cutoff are treated as zero (expected decrement < 1 for any int). */
  private static final double MIN_DECAY_PROBABILITY = 1e-12d;

  /** Power-of-two warning rate limit for dropped {@link #expelledQueue} offers. */
  private static final int EXPELLED_LOG_INTERVAL = 1024;

  /** Minimum deadband in {@link #raiseCount} to avoid locked CAS traffic on every hot hit. */
  private static final int RAISE_DEADBAND_FLOOR = 16;

  /** Sentinel returned by {@link #expelIfBelowThreshold} when a member survives removal. */
  private static final long KEPT = -1L;

  /** Tombstone written into {@link Node#count} when a member leaves {@link #members}. */
  private static final int DETACHED = -1;

  /** Golden-ratio 32-bit mixing constant used to derive independent row hashes. */
  private static final int ROW_MIX_CONSTANT = 0x9e3779b9;

  /** Maximum sketch width that can be rounded up to a power of two without int overflow. */
  private static final int MAX_AUTO_ALIGN_WIDTH = 1 << 30;

  /** Maximum supported per-slot window ring depth. */
  private static final int MAX_WINDOW_COUNT = 1 << 20;

  /** Lower and upper bounds on striped sketch lock count (power of two). */
  private static final int MIN_LOCK_STRIPES = 64;
  private static final int MAX_LOCK_STRIPES = 2048;

  /** Thresholds governing direct vs. normal vs. Poisson sampling in {@link #sampleBinomial}. */
  private static final long BINOMIAL_DIRECT_LIMIT = 10L;
  private static final double BINOMIAL_NORMAL_VARIANCE_THRESHOLD = 5.0d;

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

  private final int k;
  private final int width;
  private final int depth;
  private final int expelBelowCount;
  private final int windowCount;
  private final int windowMask;
  private final int slotStride;
  private final int totalSlots;
  private final int widthMask;
  private final boolean widthIsPow2;
  private final double decayBase;
  private final double[] decayPowers;

  /** Flattened per-slot state: {@code [fingerprint, slotSum, w0..w(windowCount-1)]}. */
  private final int[] table;

  /** Power-of-two striped monitors serializing compound updates to individual sketch slots. */
  private final Object[] lockStripes;
  private final int lockMask;

  /** Authoritative Top-K membership map (size bounded by {@link #k} under {@link #admissionLock}). */
  private final ConcurrentHashMap<String, Node> members;

  /** Non-fair lock guarding compound admission, eviction, and decay sweeps on {@link #members}. */
  private final ReentrantLock admissionLock;

  /** Bounded queue receiving evicted or decay-dropped {@link Item} entries. */
  private final BlockingQueue<Item> expelledQueue;

  /** Decayed running total of recorded increments, exposed by {@link #total()}. */
  private final LongAdder total = new LongAdder();

  /** Cumulative failed {@link #expelledQueue} offers across eviction and decay sites. */
  private final AtomicLong expelledLogCounter = new AtomicLong();

  /** Optional non-blocking observer notified outside {@link #admissionLock} on new admissions. */
  private volatile @Nullable Consumer<String> admissionListener;

  /** O(1) lower-bound fast-reject threshold, zero while the Top-K set is not full. */
  private volatile long minMemberCount;

  /** Currently active window ring index, advanced once per {@link #fading()} cycle. */
  private volatile int activeWindow;

  /**
   * Constructs a {@link HeavyKeeper} sketch with full geometry and decay configuration.
   *
   * @param k                     maximum number of hot keys tracked
   * @param width                 sketch width (buckets per row)
   * @param depth                 sketch depth (number of rows / hash functions)
   * @param decay                 collision-decay base in {@code [0.0, 1.0]}
   * @param minCount              minimum sketch estimate required for Top-K admission; the
   *                              admission and decay gates use {@code max(1, minCount)}
   * @param expelledQueueCapacity capacity of the bounded queue receiving expelled items
   * @param windowCount           time windows per slot (at least 2; rounded up to a power of two)
   * @param autoAlignWidth        {@code true} to round {@code width} up to a power of two for
   *                              bitmask indexing
   * @throws IllegalArgumentException if any parameter violates bounds or geometry overflows
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
    validateConfig(k, width, depth, decay, minCount, expelledQueueCapacity, windowCount);

    if (autoAlignWidth && !isPowerOfTwo(width)) {
      Assert.isTrue(
        width <= MAX_AUTO_ALIGN_WIDTH,
        "width must be <= 1073741824 when autoAlignWidth is enabled, but got: " + width
      );
      int original = width;
      width = nextPowerOfTwo(width);
      log.info("Auto-aligned width from {} to {} to enable bitmask optimization", original, width);
    }

    Assert.isTrue(windowCount <= MAX_WINDOW_COUNT, "windowCount must be <= 1048576, but got: " + windowCount);
    if (!isPowerOfTwo(windowCount)) {
      int original = windowCount;
      windowCount = nextPowerOfTwo(windowCount);
      log.info("Auto-aligned windowCount from {} to {} to enable bitmask optimization", original, windowCount);
    }

    int computedSlots;
    int tableLength;
    try {
      computedSlots = Math.multiplyExact(depth, width);
      tableLength = Math.multiplyExact(computedSlots, SLOT_HEADER_SIZE + windowCount);
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(
        "sketch geometry too large: depth=" + depth + ", width=" + width + ", windowCount=" + windowCount,
        e
      );
    }

    this.k = k;
    this.width = width;
    this.depth = depth;
    this.expelBelowCount = Math.max(1, minCount);
    this.windowCount = windowCount;
    this.windowMask = windowCount - 1;
    this.slotStride = SLOT_HEADER_SIZE + windowCount;
    this.totalSlots = computedSlots;
    this.widthIsPow2 = isPowerOfTwo(width);
    this.widthMask = width - 1;
    if (!widthIsPow2) {
      log.warn(
        "Width {} is not a power of two; bucket index will use the FastRange mapping " +
          "(RocksDB fastrange.h). Recommended: use a power of two for bitmask performance.",
        width
      );
    }

    this.decayBase = decay;
    this.decayPowers = buildDecayPowers(decay);
    this.table = new int[tableLength];
    this.lockStripes = createLockStripes(computedSlots);
    this.lockMask = lockStripes.length - 1;
    this.admissionLock = new ReentrantLock();
    this.members = new ConcurrentHashMap<>(k);
    this.expelledQueue = new ArrayBlockingQueue<>(expelledQueueCapacity);
  }

  /**
   * Records {@code delta} accesses for {@code key} and returns the Top-K membership decision.
   * Non-positive increments are ignored and return {@link AddResult#cold()}.
   *
   * @param key   the non-null key to record
   * @param delta the access count to add
   * @return the {@link AddResult} describing hot-key status and any evicted key
   * @throws NullPointerException if {@code key} is {@code null}
   */
  @Override
  public AddResult addDirect(String key, long delta) {
    if (key == null) {
      throw new NullPointerException("key must not be null");
    }
    if (delta <= 0L) {
      return AddResult.cold();
    }
    long increment = saturateIncrement(delta);
    total.add(increment);
    return addOne(key, increment, true);
  }

  /**
   * Ingests a batch of key counts without allocating per-key result lists, folding the batch sum
   * into a single {@link #total} update. Null keys and non-positive deltas are skipped.
   *
   * @param keyCounts map from key to positive access count, or {@code null}
   */
  public void ingestBatch(@Nullable Map<String, Long> keyCounts) {
    if (keyCounts == null || keyCounts.isEmpty()) {
      return;
    }
    long batchTotal = 0L;
    for (Map.Entry<String, Long> entry : keyCounts.entrySet()) {
      String key = entry.getKey();
      Long delta = entry.getValue();
      if (key == null || delta == null || delta <= 0L) {
        continue;
      }
      long increment = saturateIncrement(delta);
      batchTotal += increment;
      addOne(key, increment, false);
    }
    if (batchTotal > 0L) {
      total.add(batchTotal);
    }
  }

  /**
   * Registers or detaches the observer invoked outside {@link #admissionLock} whenever a key newly
   * enters the Top-K set. The listener must be fast and non-blocking.
   *
   * @param listener the admission listener, or {@code null} to detach
   */
  public void setAdmissionListener(@Nullable Consumer<String> listener) {
    this.admissionListener = listener;
  }

  /**
   * Returns all tracked Top-K items sorted by count descending, with ties broken by key ascending.
   *
   * @return sorted list of up to {@link #k} {@link Item} entries
   */
  @Override
  public List<Item> list() {
    return listTopN(k);
  }

  /**
   * Returns up to {@code limit} Top-K items sorted by count descending (ties by key ascending).
   *
   * @param limit maximum number of entries to return; must be non-negative
   * @return detached list of at most {@code limit} {@link Item} entries
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
   * Checks in O(1) time whether {@code key} is currently a member of the Top-K set.
   *
   * @param key the key to check
   * @return {@code true} if {@code key} is currently tracked as hot
   */
  @Override
  public boolean contains(String key) {
    return members.containsKey(key);
  }

  /**
   * Returns the bounded queue receiving evicted and decay-dropped {@link Item} entries.
   *
   * @return the expelled-items {@link BlockingQueue}
   */
  @Override
  public BlockingQueue<Item> expelled() {
    return expelledQueue;
  }

  /**
   * Rotates the sliding window ring: zeroes the expiring window across all sketch slots before
   * publishing it as active, re-synchronizes and prunes Top-K membership under
   * {@link #admissionLock}, and halves {@link #total}.
   */
  @Override
  public synchronized void fading() {
    int nextAw = (activeWindow + 1) & windowMask;
    rotateSketchWindows(nextAw);
    activeWindow = nextAw;
    decayMembership();

    long decrement = total.sum();
    if (decrement > 0L) {
      // Difference-decrement instead of sumThenReset: a sumThenReset would wipe concurrent adds
      // that landed between the two steps and briefly publish zero.
      total.add(-(decrement - (decrement >> 1)));
    }
  }

  /**
   * Returns the approximate number of keys currently in the Top-K set (at most {@link #k}).
   *
   * @return current membership size
   */
  @Override
  public int estimatedSize() {
    return members.size();
  }

  /**
   * Returns the decayed running total of recorded increments.
   *
   * @return decayed total count
   */
  @Override
  public long total() {
    return total.sum();
  }

  /**
   * Captures a consistent snapshot of {@link #members}, sorts by count descending and key
   * ascending, and truncates to at most {@code limit} entries.
   *
   * @param limit maximum number of entries to return
   * @return sorted, detached list of {@link Item} entries
   */
  List<Item> snapshotItemsSorted(int limit) {
    if (members.isEmpty()) {
      return Collections.emptyList();
    }
    List<Item> snapshot = new ArrayList<>(members.size());
    for (Node node : members.values()) {
      // Clamp detached tombstones (-1) observed during weakly consistent iteration to 0.
      snapshot.add(new Item(node.key, Math.max(0L, node.count)));
    }
    snapshot.sort((a, b) -> {
      int cmp = Long.compare(b.count(), a.count());
      return cmp != 0 ? cmp : a.key().compareTo(b.key());
    });
    return snapshot.size() > limit ? new ArrayList<>(snapshot.subList(0, limit)) : snapshot;
  }

  /**
   * Updates the sketch for a single validated key/increment pair and executes Top-K admission.
   *
   * @param key   the non-null key
   * @param delta the positive, {@code int}-saturated increment
   * @param needResult {@code false} to skip the {@link AddResult} allocation on a batch ingest
   * @return the membership {@link AddResult}
   */
  private AddResult addOne(String key, long delta, boolean needResult) {
    Node member = members.get(key);
    int itemFingerprint = member != null ? member.fp : fingerprint(key);
    long maxCount = addToSketch(itemFingerprint, delta);
    return admit(key, maxCount, itemFingerprint, member, needResult);
  }

  /**
   * Applies {@code delta} across all {@link #depth} sketch rows under per-slot striped locks and
   * returns the maximum matching slot sum observed.
   *
   * @param itemFingerprint the key's 32-bit non-zero fingerprint
   * @param delta           the positive, {@code int}-saturated increment
   * @return the maximum cross-row slot estimate observed for {@code itemFingerprint}
   */
  private long addToSketch(int itemFingerprint, long delta) {
    long maxCount = 0L;
    for (int row = 0; row < depth; row++) {
      int index = row * width + bucketIndex(itemFingerprint, row);
      int base = index * slotStride;
      synchronized (lockStripes[index & lockMask]) {
        int active = activeWindow;
        long cur = table[base + SLOT_SUM_OFFSET];
        if (cur == 0L) {
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
   * Claims an empty sketch slot: clears any stale fingerprint sentinel first, records {@code delta}
   * into the active window and running sum, and publishes {@code itemFingerprint} last with release
   * semantics.
   *
   * @param base            the slot's starting index in {@link #table}
   * @param active          the active window index
   * @param itemFingerprint the incoming key's fingerprint
   * @param delta           the positive increment
   * @param maxCount        the running cross-row maximum
   * @return the updated cross-row maximum
   */
  private long updateEmptySlot(int base, int active, int itemFingerprint, long delta, long maxCount) {
    SLOT.setVolatile(table, base, 0);
    long next = applyIncrement(base, active, delta, maxCount);
    SLOT.setRelease(table, base, itemFingerprint);
    return next;
  }

  /**
   * Adds {@code delta} (saturated at {@link Integer#MAX_VALUE}) to both the active window word and
   * the slot's running sum.
   *
   * @param base     the slot's starting index in {@link #table}
   * @param active   the active window index
   * @param delta    the positive increment
   * @param maxCount the running cross-row maximum
   * @return the updated cross-row maximum
   */
  private long applyIncrement(int base, int active, long delta, long maxCount) {
    int curSum = table[base + SLOT_SUM_OFFSET];
    int actualDelta = (int) Math.min(delta, (long) Integer.MAX_VALUE - curSum);
    int sum = curSum + actualDelta;
    table[base + SLOT_WINDOWS_OFFSET + active] += actualDelta;
    SLOT.setRelease(table, base + SLOT_SUM_OFFSET, sum);
    return Math.max(maxCount, sum);
  }

  /**
   * Transfers ownership of an occupied slot to {@code itemFingerprint}: detaches the prior owner
   * via sentinel {@code 0}, resets all windows, writes {@code delta} into the active window and
   * sum, and publishes the new fingerprint last.
   *
   * @param base            the slot's starting index in {@link #table}
   * @param active          the active window index
   * @param itemFingerprint the incoming key's fingerprint
   * @param delta           the incoming positive increment
   * @param maxCount        the running cross-row maximum
   * @return the updated cross-row maximum
   */
  private long takeoverSlot(int base, int active, int itemFingerprint, long delta, long maxCount) {
    SLOT.setVolatile(table, base, 0);
    int winBase = base + SLOT_WINDOWS_OFFSET;
    Arrays.fill(table, winBase, winBase + windowCount, 0);
    table[winBase + active] = (int) delta;
    SLOT.setRelease(table, base + SLOT_SUM_OFFSET, (int) delta);
    SLOT.setRelease(table, base, itemFingerprint);
    return Math.max(maxCount, delta);
  }

  /**
   * Resolves a fingerprint collision by sampling probabilistic decay units; takes over the slot if
   * the occupant is fully depleted, otherwise proportionally decrements each window using
   * Bresenham remainder carry while contributing nothing to the colliding caller's estimate.
   *
   * @param base            the slot's starting index in {@link #table}
   * @param active          the active window index
   * @param itemFingerprint the colliding key's fingerprint
   * @param delta           the colliding positive increment
   * @param cur             the occupant's current positive slot sum
   * @param maxCount        the caller's running cross-row maximum
   * @return the updated cross-row maximum
   */
  private long decayCollisionSlot(int base, int active, int itemFingerprint, long delta, long cur, long maxCount) {
    long decays = collisionDecays(cur, delta);
    if (decays == 0L) {
      return maxCount;
    }
    if (decays >= cur) {
      return takeoverSlot(base, active, itemFingerprint, delta, maxCount);
    }

    int winBase = base + SLOT_WINDOWS_OFFSET;
    long sumBefore = table[base + SLOT_SUM_OFFSET];
    long totalSubtracted = 0L;
    long carry = 0L;

    for (int w = 0; w < windowCount; w++) {
      int off = winBase + w;
      long windowVal = table[off];
      if (windowVal > 0L) {
        long numerator = windowVal * decays + carry;
        long sub = numerator / cur;
        carry = numerator % cur;
        long newVal = Math.max(0L, windowVal - sub);
        totalSubtracted += windowVal - newVal;
        table[off] = (int) newVal;
      }
    }
    SLOT.setRelease(table, base + SLOT_SUM_OFFSET, (int) Math.max(0L, sumBefore - totalSubtracted));
    return maxCount;
  }

  /**
   * Lock-free seqlock probe returning the maximum running slot sum across rows currently owned by
   * {@code itemFingerprint}, or {@code -1} if every row has been taken over by other keys.
   *
   * @param itemFingerprint the member's captured fingerprint
   * @return the current non-negative estimate, or {@code -1} when no row carries the fingerprint
   */
  private long probeSketch(int itemFingerprint) {
    long max = 0L;
    boolean present = false;
    for (int row = 0; row < depth; row++) {
      int base = (row * width + bucketIndex(itemFingerprint, row)) * slotStride;
      if ((int) SLOT.getAcquire(table, base) != itemFingerprint) {
        continue;
      }
      long sum = (int) SLOT.getAcquire(table, base + SLOT_SUM_OFFSET);
      if ((int) SLOT.getAcquire(table, base) != itemFingerprint) {
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
   * Zeroes window {@code windowIndex} across every sketch slot under each owning stripe monitor
   * and subtracts the expired contribution from each slot's running sum.
   *
   * @param windowIndex the expiring window ring index to clear
   */
  private void rotateSketchWindows(int windowIndex) {
    for (int stripe = 0; stripe < lockStripes.length; stripe++) {
      synchronized (lockStripes[stripe]) {
        for (int slot = stripe; slot < totalSlots; slot += lockStripes.length) {
          int base = slot * slotStride;
          int off = base + SLOT_WINDOWS_OFFSET + windowIndex;
          int oldWindow = table[off];
          if (oldWindow != 0) {
            table[off] = 0;
            int sum = Math.max(0, table[base + SLOT_SUM_OFFSET] - oldWindow);
            SLOT.setRelease(table, base + SLOT_SUM_OFFSET, sum);
          }
        }
      }
    }
  }

  /**
   * Samples the number of occupant units removed by a colliding batch of size {@code delta}
   * against an occupant count {@code cur}, capped at {@code min(delta, cur)}.
   *
   * @param cur   the occupant's current positive count
   * @param delta the colliding batch size
   * @return the number of units to decay in {@code [0, min(delta, cur)]}
   */
  @SuppressWarnings("squid:S2245")
  private long collisionDecays(long cur, long delta) {
    double probability = decayProbability(cur);
    if (probability <= 0.0d) {
      return 0L;
    }
    long decays = probability >= 1.0d ? delta : sampleBinomial(delta, probability, ThreadLocalRandom.current());
    return Math.min(decays, Math.min(delta, cur));
  }

  /**
   * Returns the per-unit collision-decay probability {@code decayBase^cur}, using the precomputed
   * lookup table for small counts and clamping negligible probabilities to zero.
   *
   * @param cur the occupant's current slot count
   * @return the decay probability in {@code [0.0, 1.0]}
   */
  private double decayProbability(long cur) {
    if (cur < DECAY_TABLE_SIZE) {
      return decayPowers[(int) cur];
    }
    double probability = Math.pow(decayBase, cur);
    return probability < MIN_DECAY_PROBABILITY ? 0.0d : probability;
  }

  /**
   * Computes the column index for {@code itemFingerprint} on {@code row}, using bitmasking when
   * {@link #width} is a power of two and FastRange mapping otherwise.
   *
   * @param itemFingerprint the key's 32-bit fingerprint
   * @param row             the zero-based row index
   * @return the bucket column index in {@code [0, width)}
   */
  private int bucketIndex(int itemFingerprint, int row) {
    int hash = row == 0 ? itemFingerprint : Hashes.mix32(itemFingerprint + row * ROW_MIX_CONSTANT);
    return widthIsPow2 ? (hash & widthMask) : (int) (((hash & 0xFFFFFFFFL) * width) >>> 32);
  }

  /**
   * Computes a non-zero 32-bit Murmur3-mixed fingerprint for {@code key} ({@code 0} is reserved as
   * the slot-transition sentinel).
   *
   * @param key the non-null key
   * @return a non-zero 32-bit fingerprint
   */
  private static int fingerprint(String key) {
    int fp = Hashes.mix32(key.hashCode());
    return fp == 0 ? 1 : fp;
  }

  /** Sentinel result for a batch ingest that discards the outcome but must preserve the admission notification. */
  private static final AddResult ADMITTED_SILENT = new AddResult(null, true, "");

  /**
   * Evaluates Top-K membership for {@code key} given its sketch estimate {@code maxCount}: existing
   * members max-raise lock-free, while new candidates or detached nodes take {@link #admissionLock}.
   *
   * @param key             the candidate key
   * @param maxCount        the current sketch estimate
   * @param itemFingerprint the key's 32-bit fingerprint
   * @param resolvedMember  previously looked-up {@link Node}, or {@code null}
   * @param needResult      {@code false} for a batch ingest that discards the result
   * @return the {@link AddResult}, or {@link #ADMITTED_SILENT} when the result is not requested
   */
  private AddResult admit(
    String key,
    long maxCount,
    int itemFingerprint,
    @Nullable Node resolvedMember,
    boolean needResult
  ) {
    if (maxCount < expelBelowCount) {
      return AddResult.cold();
    }
    if (resolvedMember != null && raiseCount(resolvedMember, (int) maxCount)) {
      return needResult ? new AddResult(null, true, key) : ADMITTED_SILENT;
    }
    // Lock-free fast reject: minMemberCount is zero while the set is not full, so this can only
    // refuse a candidate that cannot displace the current minimum.
    if (resolvedMember == null && maxCount <= minMemberCount) {
      return AddResult.cold();
    }

    AddResult result;
    boolean newlyAdmitted;
    admissionLock.lock();
    try {
      Node existing = members.get(key);
      if (existing != null) {
        raiseCount(existing, (int) maxCount);
        return needResult ? new AddResult(null, true, key) : ADMITTED_SILENT;
      }
      if (members.size() >= k && maxCount <= minMemberCount) {
        return AddResult.cold();
      }
      result = admitOrEvict(key, maxCount, itemFingerprint);
      newlyAdmitted = result.isHotKey();
    } finally {
      admissionLock.unlock();
    }
    if (newlyAdmitted) {
      notifyAdmitted(key);
    }
    return needResult || !newlyAdmitted ? result : ADMITTED_SILENT;
  }

  /**
   * Inserts a new member or evicts the minimum member when {@link #members} is full; caller holds
   * {@link #admissionLock}. Reuses one O(k) scan to resolve both the victim and runner-up floor.
   *
   * @param key             the incoming key
   * @param maxCount        the incoming key's sketch estimate
   * @param itemFingerprint the incoming key's fingerprint
   * @return the resulting {@link AddResult}
   */
  private AddResult admitOrEvict(String key, long maxCount, int itemFingerprint) {
    if (members.size() < k) {
      members.put(key, new Node(key, maxCount, itemFingerprint));
      if (members.size() >= k) {
        // This insert filled the set: the fast-reject floor becomes meaningful now.
        Node min = findMinMember();
        minMemberCount = min == null ? 0L : min.count;
      } else {
        // The set is not full: every fresh candidate is admissible, so the floor is 0.
        minMemberCount = 0L;
      }
      return new AddResult(null, true, key);
    }

    MinPair pair = findTwoSmallest();
    if (maxCount <= pair.minCount()) {
      minMemberCount = pair.minCount();
      return AddResult.cold();
    }
    if (isStaleMinimum(pair, maxCount)) {
      pair = findTwoSmallest();
      if (maxCount <= pair.minCount()) {
        minMemberCount = pair.minCount();
        return AddResult.cold();
      }
    }

    String expelledKey = evictMinAndPut(key, maxCount, itemFingerprint, pair.min());
    minMemberCount = Math.min(maxCount, pair.secondCount());
    return new AddResult(expelledKey, true, key);
  }

  /**
   * Checks whether a lock-free count raise pushed {@code pair.min()} above either the runner-up or
   * the newcomer since the scan completed.
   *
   * @param pair     the scanned minimum and runner-up
   * @param maxCount the newcomer's estimate
   * @return {@code true} if the minimum must be re-resolved before eviction
   */
  private boolean isStaleMinimum(MinPair pair, long maxCount) {
    Node min = pair.min();
    if (min == null) {
      return false;
    }
    long liveMinCount = min.count;
    return liveMinCount > pair.secondCount() || liveMinCount >= maxCount;
  }

  /**
   * Tombstones and removes {@code min} from {@link #members}, offers it to {@link #expelledQueue},
   * and inserts the newcomer {@code key}. Caller holds {@link #admissionLock}.
   *
   * @param key             the incoming key
   * @param count           the incoming key's count
   * @param itemFingerprint the incoming key's fingerprint
   * @param min             the victim node to evict, or {@code null}
   * @return the evicted key, or {@code null} if none was evicted
   */
  private @Nullable String evictMinAndPut(String key, long count, int itemFingerprint, @Nullable Node min) {
    String expelledKey = null;
    if (min != null) {
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
   * Lock-free max-raise of an existing member's count with a deadband of {@code max(16, cur / 32)}
   * for safe interior members while raising eagerly near {@link #minMemberCount}.
   *
   * @param node     the member node
   * @param maxCount the new sketch estimate
   * @return {@code true} if {@code node} is still attached; {@code false} if {@link #DETACHED}
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
   * Re-synchronizes every member's count against the sketch under {@link #admissionLock}, drops
   * members below {@link #expelBelowCount}, and refreshes {@link #minMemberCount}.
   */
  private void decayMembership() {
    admissionLock.lock();
    try {
      // The sweep may drop members below k; the fast-reject floor is only meaningful on a full set.
      minMemberCount = 0L;
      List<String> dropped = null;
      for (Node node : members.values()) {
        int current = synchronizeMemberCount(node);
        if (current < expelBelowCount && node.count < expelBelowCount) {
          if (dropped == null) {
            dropped = new ArrayList<>();
          }
          dropped.add(node.key);
        }
      }
      if (dropped != null) {
        for (String key : dropped) {
          long departedAt = expelIfBelowThreshold(key);
          if (departedAt != KEPT) {
            offerExpelled(key, departedAt, "decay-dropped key");
          }
        }
      }
      if (members.size() >= k) {
        Node min = findMinMember();
        minMemberCount = min == null ? 0L : min.count;
      }
    } finally {
      admissionLock.unlock();
    }
  }

  /**
   * Synchronizes one member's count with {@link #probeSketch}: adopts the authoritative sketch
   * estimate when present, or halves the count if all sketch rows were taken over.
   *
   * @param node the member node to synchronize
   * @return the resulting count stored in {@code node}
   */
  private int synchronizeMemberCount(Node node) {
    for (;;) {
      int prev = node.count;
      long observed = probeSketch(node.fp);
      int next = observed >= 0L ? (int) Math.min(observed, Integer.MAX_VALUE) : prev >> 1;
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
   * Atomically tombstones and removes {@code key} inside its map bin if its live count remains
   * below {@link #expelBelowCount}, capturing the exact departure count.
   *
   * @param key the candidate member key
   * @return the member's departure count, or {@link #KEPT} if revived concurrently
   */
  private long expelIfBelowThreshold(String key) {
    long[] departedAt = { KEPT };
    members.compute(key, (k, node) -> {
      if (node == null) {
        return null;
      }
      for (;;) {
        int count = node.count;
        if (count >= expelBelowCount) {
          return node;
        }
        if ((int) MEMBER_COUNT.compareAndExchange(node, count, DETACHED) == count) {
          departedAt[0] = count;
          return null;
        }
      }
    });
    return departedAt[0];
  }

  /**
   * Returns the minimum member under deterministic eviction ordering, or {@code null} when empty.
   *
   * @return the minimum {@link Node}, or {@code null}
   */
  private @Nullable Node findMinMember() {
    return findTwoSmallest().min();
  }

  /**
   * Scans {@link #members} once to find the smallest member (victim) and the second-smallest count
   * (new floor after eviction).
   *
   * @return {@link MinPair} containing the victim, its scanned count, and the runner-up count
   */
  private MinPair findTwoSmallest() {
    long minVal = Long.MAX_VALUE;
    long secondVal = Long.MAX_VALUE;
    String minKey = null;
    Node min = null;

    for (Node node : members.values()) {
      long count = node.count;
      if (beatsAsMin(count, node.key, minVal, minKey)) {
        secondVal = minVal;
        minVal = count;
        minKey = node.key;
        min = node;
      } else if (count < secondVal) {
        secondVal = count;
      }
    }
    return new MinPair(min, minVal, secondVal);
  }

  /**
   * Compares a candidate {@code (count, key)} against the current minimum under deterministic
   * eviction order: smaller count wins, with ties broken by lexicographically smaller key.
   *
   * @param count    the candidate count
   * @param key      the candidate key
   * @param minCount the current minimum count
   * @param minKey   the current minimum key, or {@code null} before the first element
   * @return {@code true} if the candidate is strictly smaller under eviction order
   */
  private static boolean beatsAsMin(long count, String key, long minCount, @Nullable String minKey) {
    return count < minCount || (count == minCount && (minKey == null || key.compareTo(minKey) < 0));
  }

  /**
   * Offers a departing item to {@link #expelledQueue}, dropping silently when full and
   * rate-limiting warnings to once every {@link #EXPELLED_LOG_INTERVAL} failures.
   *
   * @param key       the departing key
   * @param count     the count at departure
   * @param siteLabel descriptive label identifying eviction vs. decay drop
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
   * Notifies {@link #admissionListener} of a newly admitted hot key, isolating runtime exceptions
   * so listener failures never disrupt Top-K accounting.
   *
   * @param key the newly admitted key
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
   * Validates raw constructor parameters before alignment and allocation.
   *
   * @param k                     Top-K capacity
   * @param width                 sketch width
   * @param depth                 sketch depth
   * @param decay                 collision-decay base
   * @param minCount              admission floor
   * @param expelledQueueCapacity expelled queue capacity
   * @param windowCount           window ring depth
   * @throws IllegalArgumentException if any parameter is out of range
   */
  private static void validateConfig(
    int k,
    int width,
    int depth,
    double decay,
    int minCount,
    int expelledQueueCapacity,
    int windowCount
  ) {
    Assert.isTrue(k > 0, "k must be > 0, but got: " + k);
    Assert.isTrue(width > 0, "width must be > 0, but got: " + width);
    Assert.isTrue(depth > 0, "depth must be > 0, but got: " + depth);
    Assert.isTrue(decay >= 0.0d && decay <= 1.0d, "decay must be in [0.0, 1.0], but got: " + decay);
    Assert.isTrue(minCount >= 0, "minCount must be >= 0, but got: " + minCount);
    Assert.isTrue(windowCount >= 2, "windowCount must be >= 2, but got: " + windowCount);
    Assert.isTrue(expelledQueueCapacity > 0, "expelledQueueCapacity must be > 0, but got: " + expelledQueueCapacity);
  }

  /**
   * Precomputes {@code decay^i} for {@code 0 <= i < DECAY_TABLE_SIZE}.
   *
   * @param decay the collision-decay base
   * @return the lookup table of decay powers
   */
  private static double[] buildDecayPowers(double decay) {
    double[] powers = new double[DECAY_TABLE_SIZE];
    double probability = 1.0d;
    for (int i = 0; i < DECAY_TABLE_SIZE; i++) {
      powers[i] = probability;
      probability *= decay;
    }
    return powers;
  }

  /**
   * Allocates a power-of-two array of striped monitor objects sized proportionally to
   * {@code totalSlots} within {@code [MIN_LOCK_STRIPES, MAX_LOCK_STRIPES]}.
   *
   * @param totalSlots total number of sketch slots ({@code depth * width})
   * @return the lock-stripe array
   */
  private static Object[] createLockStripes(int totalSlots) {
    int stripes = Math.min(MAX_LOCK_STRIPES, Math.max(MIN_LOCK_STRIPES, totalSlots >> 4));
    if (!isPowerOfTwo(stripes)) {
      stripes = nextPowerOfTwo(stripes);
    }
    Object[] lockArray = new Object[stripes];
    for (int i = 0; i < stripes; i++) {
      lockArray[i] = new Object();
    }
    return lockArray;
  }

  /**
   * Returns whether a positive integer {@code value} is a power of two.
   *
   * @param value the value to check
   * @return {@code true} if {@code value > 0} and has a single set bit
   */
  private static boolean isPowerOfTwo(int value) {
    return value > 0 && (value & (value - 1)) == 0;
  }

  /**
   * Rounds a non-power-of-two positive integer up to the next power of two.
   *
   * @param value a positive integer at most {@code 1 << 30}
   * @return the smallest power of two strictly greater than {@code value - 1}
   */
  private static int nextPowerOfTwo(int value) {
    return Integer.highestOneBit(value - 1) << 1;
  }

  /**
   * Clamps a positive external increment to the sketch's {@code int} counter range.
   *
   * @param delta a positive increment
   * @return {@code min(delta, Integer.MAX_VALUE)}
   */
  private static long saturateIncrement(long delta) {
    return Math.min(delta, Integer.MAX_VALUE);
  }

  /**
   * Samples from {@code Binomial(n, p)} in O(1) expected time using direct Bernoulli trials for
   * small {@code n}, normal approximation when variance exceeds 5, and Knuth's Poisson algorithm
   * otherwise. Mirrors {@code p > 0.5} via one complement call.
   *
   * @param n      number of trials
   * @param p      per-trial success probability
   * @param random thread-local PRNG source
   * @return sampled integer in {@code [0, n]}
   */
  private static long sampleBinomial(long n, double p, ThreadLocalRandom random) {
    if (n <= 0L || p <= 0.0d) {
      return 0L;
    }
    if (p >= 1.0d) {
      return n;
    }
    if (p > 0.5d) {
      return n - sampleBinomial(n, 1.0d - p, random);
    }

    double q = 1.0d - p;
    if (n <= BINOMIAL_DIRECT_LIMIT) {
      long successes = 0L;
      for (long i = 0L; i < n; i++) {
        if (random.nextDouble() < p) {
          successes++;
        }
      }
      return successes;
    }

    double np = n * p;
    double npq = np * q;
    if (npq > BINOMIAL_NORMAL_VARIANCE_THRESHOLD) {
      long sampled = Math.round(np + Math.sqrt(npq) * random.nextGaussian());
      return Math.max(0L, Math.min(n, sampled));
    }

    double limit = Math.exp(-np);
    long k = 0L;
    double prod = 1.0d;
    do {
      k++;
      prod *= random.nextDouble();
    } while (prod > limit);
    return Math.min(k - 1L, n);
  }

  /**
   * Mutable Top-K membership entry whose {@link #count} is updated atomically via
   * {@link #MEMBER_COUNT} and set to {@link #DETACHED} upon removal.
   */
  private static final class Node {

    /** The tracked cache key. */
    final String key;

    /** Precomputed 32-bit sketch fingerprint for {@link #key}. */
    final int fp;

    /** Current estimated count, or {@link #DETACHED} once removed from {@link #members}. */
    volatile int count;

    /**
     * Creates an attached membership node.
     *
     * @param key   the tracked key
     * @param count the initial count (saturated at {@link Integer#MAX_VALUE})
     * @param fp    the key's 32-bit sketch fingerprint
     */
    private Node(String key, long count, int fp) {
      this.key = key;
      this.fp = fp;
      this.count = (int) Math.min(count, Integer.MAX_VALUE);
    }
  }

  /**
   * Snapshot of the smallest and second-smallest members produced by {@link #findTwoSmallest()}.
   *
   * @param min         the victim node with the lowest count, or {@code null} when empty
   * @param minCount    the scanned count of {@code min}, or {@link Long#MAX_VALUE} when empty
   * @param secondCount the second-smallest scanned count, or {@link Long#MAX_VALUE} if fewer than 2
   */
  private record MinPair(@Nullable Node min, long minCount, long secondCount) {}
}
