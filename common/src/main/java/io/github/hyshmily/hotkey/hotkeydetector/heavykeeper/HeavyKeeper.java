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
package io.github.hyshmily.hotkey.hotkeydetector.heavykeeper;

import com.google.common.hash.Hashing;
import io.github.hyshmily.hotkey.Internal;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * HeavyKeeper — a Count-Min Sketch variant for approximate Top‑K tracking
 * of frequently accessed keys.
 *
 * <p><b>Algorithm overview:</b> Uses a 2D count array ({@code depth × width})
 * with per-slot fingerprint verification and probabilistic decay to estimate
 * the most frequent keys using bounded memory. Each key is hashed into one
 * bucket per row; if the stored fingerprint matches, the counter is
 * incremented; otherwise a probabilistic decay (sampled from a Binomial
 * distribution) determines whether the existing counter survives or is
 * replaced. This design excels at filtering out low-frequency items while
 * preserving high-frequency key rankings with low error rates.
 *
 * <p><b>Sliding-window decay:</b> Instead of binary halving, each sketch slot
 * maintains a ring buffer of {@link #windowCount} time windows. {@link #fading()}
 * advances the epoch and zeros the oldest window, preserving the most recent
 * {@code windowCount × decayInterval} of data. This eliminates the "hot key
 * drift" problem — slowly rising keys are not disadvantaged by stale high counts.
 *
 * <p><b>Concurrency model:</b> Thread-safe with three tiers:
 * <ol>
 *   <li>Fine-grained striped synchronization (up to 4096 stripes)
 *       on individual sketch buckets for low-contention sketch updates.
 *       {@code synchronized(Object[])} was retained over {@link ReentrantLock}
 *       and {@link java.util.concurrent.locks.StampedLock} after a
 *       micro-benchmark (see {@code HeavyKeeperBenchmark}) showed biased /
 *       thin-lock optimisations win on the small uniform critical sections used
 *       here (synchronized 1.35×–1.7× faster than StampedLock write path).
 *       No lock-striping replacement yields a measurable win, so the simpler
 *       monitor was kept.</li>
 *   <li>Striped accumulator TopK membership updates: existing hot keys are
 *       refreshed via a {@link LongAccumulator} {@code (Long::max, 0)} on
 *       {@link Node#count}. The {@code LongAccumulator} uses per-CPU-cell
 *       striping (the same {@code Striped64} engine behind {@link LongAdder}),
 *       so concurrent reporters of the <i>same</i> hot key — exactly the
 *       worst-case stress pattern for top-K detection — do not contend on a
 *       single CAS word. The merger {@code Long::max} preserves the original
 *       AtomicLong monotonic-max semantics: the membership count never goes
 *       backward even if the sketch's {@code slotSums} fluctuates on
 *       collisions. Under same-key 16-thread contention this is ~2.6×
 *       faster than {@link java.util.concurrent.atomic.AtomicLong#getAndSet}
 *       CAS spin; in mixed workloads it is statistically indistinguishable.</li>
 *   <li>A short, non-fair {@link ReentrantLock} ({@link #admissionLock}) guards
 *       only the relatively rare <i>admission</i> path — when a brand-new key
 *       crosses the {@link #minCount} threshold and may enter or evict from the
 *       bounded TopK set. The locked section is O(k) (a single min-scan of
 *       members via {@link #findMinMember}) and never touches the sketch.</li>
 * </ol>
 *
 * <p><b>Memory-for-accuracy/performance trade-offs applied here:</b>
 * <ul>
 *   <li>Enlarged decay lookup table ({@link #LOOKUP_TABLE_SIZE} = 65 536) so
 *       hot keys with large counters no longer fall through to the clamped
 *       {@code decay^255} entry — decay probabilities stay accurate up to
 *       65k counts, with a {@link Math#pow} fallback beyond.</li>
 *   <li>Per-key {@link SlotLoc} cache ({@link #locCache}) memoises the
 *       Murmur3 fingerprint and pre-computed bucket indices per key, eliminating
 *       the hash + UTF-8 encode + {@code floorMod} cost on the hot path for
 *       repeatedly-accessed keys.</li>
 *   <li>Per-slot {@link #slotSums} array maintains the running window sum in
 *       O(1), removing the {@code windowCount}-length loop from the locked
 *       sketch-update path.</li>
 *   <li>Lock stripes raised from 256 to up to 4096 to further
 *       reduceCollision contention on the sketch.</li>
 *   <li>Window ring buffer flattened from {@code long[][] windows} to a single
 *       1D {@code long[] windows} (indexed {@code slot * windowCount + w}).
 *       One allocation, contiguous cache lines, no per-slot array header
 *       pointer-chasing.</li>
 *   <li>{@link LongAccumulator} adapter on {@link Node#count} — see "Concurrency
 *       model" above. Uses ~16× more memory per Node than a bare long but
 *       eliminates single-word CAS contention on same-key writes.</li>
 * </ul>
 *
 * @see <a href="../../../../../../../docs/adr/0014-heavykeeper-concurrency-choices.md">ADR-0014: HeavyKeeper concurrency data-structure choices</a>
 */
@Slf4j
@Internal
public class HeavyKeeper implements TopK {

  /** Pre-computed decay probability lookup table size ({@value}). */
  private static final int LOOKUP_TABLE_SIZE = 65536;

  @Getter
  private final int k;

  @Getter
  private final int width;

  @Getter
  private final int depth;

  /** Pre-computed decay probabilities: {@code decay^i} for {@code i in [0, LOOKUP_TABLE_SIZE)}. */
  private final double[] lookupTable;
  /** Per-slot fingerprint values for collision verification in the Count-Min Sketch. */
  private final long[] fingerprints;
  /**
   * Flattened per-slot sliding-window counters — a single
   * {@code long[totalSlots * windowCount]} array. Window {@code w} of slot
   * {@code s} lives at {@code windows[s * windowCount + w]}.
   * Window {@code activeEpoch()} (i.e. {@code windows[s * windowCount + (epoch % windowCount)]})
   * is the most recent window.
   */
  private final long[] windows;
  /**
   * Per-slot running sum across all {@link #windowCount} windows, maintained in O(1) on every
   * update and zero. Replaces the {@code windowCount}-length {@code slotSum} loop on the hot path.
   */
  private final long[] slotSums;
  /** {@code totalSlots * windowCount} — precomputed stride used for window indexing. */
  private final int windowStride;

  /** Number of time windows per sketch slot (ring buffer depth). */
  @Getter
  private final int windowCount;

  /**
   * Epoch counter, incremented each {@link #fading()}. Atomic so that concurrent
   * {@link #fading()} calls never lose an increment. The read is performed
   * <i>inside</i> the per-stripe lock to guarantee a writing thread never
   * targets a stale window that {@link #fading()} has just zeroed.
   */
  private final AtomicLong epoch = new AtomicLong(0);
  /** Striped lock objects for fine-grained concurrency on sketch slot updates. */
  private final Object[] lockStripes;
  /** Bitmask for mapping bucket index to lock stripe (stripe count must be power of two). */
  private final int lockMask;
  /** {@code true} when {@link #width} is a power of two — enables mask-based bucket lookup. */
  private final boolean widthIsPow2;
  /** Bitmask for bucket index when {@link #width} is a power of two ({@code width - 1}). */
  private final int widthMask;

  /**
   * Per-key fingerprint cache, sized to match the TopK membership set exactly.
   * Entries are added on TopK admission and removed on eviction or decay drop,
   * so the cache size never exceeds {@link #k} (default 100). Non-member keys
   * compute their fingerprint on the fly in {@link #locate} and are never cached.
   */
  private final ConcurrentHashMap<String, SlotLoc> locCache;

  /**
   * Authoritative TopK membership map. Key → {@link Node} whose count is a
   * {@link LongAccumulator} with {@code Long::max} merger. Size is bounded by
   * {@link #k} and enforced by {@link #admissionLock}. Reads and writes of
   * the count on existing members are lock-free — the accumulator stripes
   * writes across CPU cells.
   */
  private final ConcurrentHashMap<String, Node> members;
  /**
   * Short, non-fair lock guarding only the compound admission/eviction
   * operation on {@link #members} (read-min, insert, evict). The hot
   * increment path does not acquire this lock.
   */
  private final ReentrantLock admissionLock;
  /** Bounded blocking queue receiving expelled (evicted) key-count items for downstream consumption. */
  private final BlockingQueue<Item> expelledQueue;
  /** Running total of all tracked data streams since startup or last {@link #fading()}. */
  private final LongAdder total;

  /**
   * Cached minimum {@link Node#count} among all current members, used for
   * O(1) fast rejection in {@link #admit}. Written only under
   * {@link #admissionLock} and {@code volatile} for lock-free reads.
   */
  private volatile long minPqCount;

  @Getter
  private final int minCount;

  /**
   * Construct a HeavyKeeper instance.
   *
   * @param k        maximum number of hot keys to track
   * @param width    width of the Count-Min Sketch (number of columns per row)
   * @param depth    depth of the Count-Min Sketch (number of rows / hash functions)
   * @param decay    probabilistic decay factor (0.0–1.0); higher values preserve counts longer
   * @param minCount minimum count threshold before a key can enter the TopK set
   */
  public HeavyKeeper(int k, int width, int depth, double decay, int minCount) {
    this(k, width, depth, decay, minCount, 50_000, 3);
  }

  /**
   * Construct a HeavyKeeper instance with a custom expelled-queue capacity.
   *
   * @param k                     maximum number of hot keys to track
   * @param width                 width of the Count-Min Sketch (number of columns per row)
   * @param depth                 depth of the Count-Min Sketch (number of rows / hash functions)
   * @param decay                 probabilistic decay factor (0.0–1.0); higher values preserve counts longer
   * @param minCount              minimum count threshold before a key can enter the TopK set
   * @param expelledQueueCapacity capacity of the bounded blocking queue for expelled items
   */
  public HeavyKeeper(int k, int width, int depth, double decay, int minCount, int expelledQueueCapacity) {
    this(k, width, depth, decay, minCount, expelledQueueCapacity, 3);
  }

  /**
   * Construct a HeavyKeeper instance with sliding-window configuration.
   *
   * @param k                     maximum number of hot keys to track
   * @param width                 width of the Count-Min Sketch (number of columns per row)
   * @param depth                 depth of the Count-Min Sketch (number of rows / hash functions)
   * @param decay                 probabilistic decay factor (0.0–1.0); higher values preserve counts longer
   * @param minCount              minimum count threshold before a key can enter the TopK set
   * @param expelledQueueCapacity capacity of the bounded blocking queue for expelled items
   * @param windowCount           number of time windows per sketch slot (ring buffer depth, default 3)
   */
  public HeavyKeeper(
    int k,
    int width,
    int depth,
    double decay,
    int minCount,
    int expelledQueueCapacity,
    int windowCount
  ) {
    if (k <= 0) {
      throw new IllegalArgumentException("TopK must be greater than 0, but got: " + k);
    }
    this.k = k;
    this.width = width;
    this.depth = depth;
    this.minCount = minCount;
    this.windowCount = windowCount;
    this.windowStride = windowCount;

    this.lookupTable = new double[LOOKUP_TABLE_SIZE];
    for (int i = 0; i < LOOKUP_TABLE_SIZE; i++) {
      lookupTable[i] = Math.pow(decay, i);
    }

    int totalSlots = depth * width;
    int stripes = 1;
    if (totalSlots <= 4096) {
      while (stripes < totalSlots) {
        stripes <<= 1;
      }
    } else {
      while (stripes < totalSlots / 2) {
        stripes <<= 1;
      }
      if (stripes > 4096) {
        stripes = 4096;
      }
    }
    this.fingerprints = new long[totalSlots];
    this.windows = new long[totalSlots * windowCount];
    this.slotSums = new long[totalSlots];
    this.lockStripes = new Object[stripes];
    for (int i = 0; i < stripes; i++) {
      lockStripes[i] = new Object();
    }
    this.lockMask = stripes - 1;
    this.widthIsPow2 = width > 0 && (width & (width - 1)) == 0;
    if (!widthIsPow2) {
      log.warn(
        "Width {} is not a power of two; bucket index will use slow modulo. " +
          "Recommended: use a power of two for optimal performance.",
        width
      );
    }
    this.widthMask = width - 1;

    this.locCache = new ConcurrentHashMap<>();
    this.members = new ConcurrentHashMap<>(k);
    this.admissionLock = new ReentrantLock();
    this.expelledQueue = new ArrayBlockingQueue<>(expelledQueueCapacity);
    this.total = new LongAdder();
  }

  /**
   * Compute the bucket index for row {@code i} given the key fingerprint,
   * using a fast bit-mask when {@link #width} is a power of two and a
   * sign-stripped modulo otherwise (cheaper than {@link Math#floorMod}).
   */
  private int bucketIndex(long itemFingerprint, int row) {
    int hash = (int) (itemFingerprint ^ (row * 0x9e3779b97f4a7c15L));
    return widthIsPow2 ? (hash & widthMask) : ((hash & 0x7FFFFFFF) % width);
  }

  /** 64-bit Murmur3 fingerprint (lower half of 128-bit hash) for sketch slot indexing. */
  private static long fingerprint(String key) {
    return Hashing.murmur3_128().hashString(key, StandardCharsets.UTF_8).asLong();
  }

  /** Returns cached {@link SlotLoc} for TopK members; computes on the fly for non-members. */
  private SlotLoc locate(String key) {
    SlotLoc cached = locCache.get(key);
    if (cached != null) return cached;
    return new SlotLoc(fingerprint(key));
  }

  /**
   * Compute and cache the {@link SlotLoc} for a key that has just been
   * admitted into the TopK membership set. Must be called under
   * {@link #admissionLock} alongside {@link #members} mutations.
   */
  private void cacheLoc(String key) {
    locCache.put(key, new SlotLoc(fingerprint(key)));
  }

  /**
   * Record {@code increment} accesses for {@code key} and return the TopK
   * membership decision.
   *
   * <p>This is the single-key entry point. Delegates to {@link #addToSketch}
   * for the Count-Min Sketch update and to {@link #admit} for the TopK
   * membership decision (cold reject, lock-free refresh, or lock-guarded
   * admission/eviction).
   */
  @Override
  public AddResult addDirect(String key, int increment) {
    long maxCount = addToSketch(key, increment);
    return admit(key, maxCount);
  }

  /**
   * Record accesses for multiple keys in batch.
   *
   * <p>More efficient than repeated {@link #addDirect(String, int)} calls
   * because the sketch update and admission decision are bundled in a single
   * pass. Returns results only for keys that actually entered the TopK set
   * (cold keys are filtered out), reducing downstream noise.
   *
   * @param keyCounts map of keys to their access counts
   * @return list of {@link AddResult} for keys that entered the TopK set
   */
  @Override
  public List<AddResult> addDirect(Map<String, Long> keyCounts) {
    List<AddResult> results = new ArrayList<>(keyCounts.size());

    for (Map.Entry<String, Long> entry : keyCounts.entrySet()) {
      String key = entry.getKey();
      long maxCount = addToSketch(key, entry.getValue());
      AddResult r = admit(key, maxCount);
      if (r.isHotKey()) {
        results.add(r);
      }
    }
    return results;
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
   * Return the top {@code n} hot keys sorted by frequency (descending),
   * with ties broken by key name (ascending).
   *
   * <p>Takes a lock-free snapshot of the current membership, sorts it, and
   * returns at most {@code n} entries. A negative {@code n} is rejected.
   *
   * @param n maximum number of keys to return
   * @return list of at most {@code n} {@link Item} entries
   * @throws IllegalArgumentException if {@code n} is negative
   */
  @Override
  public List<Item> listTopN(int n) {
    if (n < 0) {
      throw new IllegalArgumentException("n must be non-negative, but got: " + n);
    }
    if (n == 0 || members.isEmpty()) {
      return Collections.emptyList();
    }

    List<Node> sorted = snapshotMembersSorted(n);
    List<Item> result = new ArrayList<>(sorted.size());

    for (Node node : sorted) {
      result.add(new Item(node.key, node.count.get()));
    }
    return result;
  }

  /**
   * Check whether the given key is currently in the TopK set.
   *
   * <p>This is an O(1) lookup — a direct {@link ConcurrentHashMap#containsKey}
   * call, significantly faster than the default {@link TopK#contains} which
   * materialises the full sorted list.
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
   * When the queue is full, further eviction events are silently dropped
   * (logged at WARN) so the admission path is never blocked by a slow
   * consumer.
   *
   * @return the {@link BlockingQueue} of evicted {@link Item entries}
   */
  @Override
  public BlockingQueue<Item> expelled() {
    return expelledQueue;
  }

  /**
   * Rotate the sliding window: advance the epoch and zero the now-stale
   * window for every sketch slot.  Unlike the traditional binary-halving
   * approach, this preserves the most recent {@code windowCount × decayInterval}
   * of data and eliminates the "hot key drift" problem — slowly rising keys
   * are not penalised by stale high counts from previous windows.
   *
   * <p>The TopK membership counts are then halved (binary decay) under the
   * {@link #admissionLock} via {@link #decayMembership()}, and members whose
   * halved count drops to zero are dropped silently (they are not reported to
   * {@link #expelled()}). This mirrors the original heap semantics and keeps
   * the downstream expulsion stream consistent. Lock order is
   * <i>sketch stripes → admissionLock</i>, identical to the admission path,
   * so no deadlock is possible with concurrent {@link #addDirect} callers.
   *
   * <p>This operation is invoked automatically by a scheduler at a
   * configured interval (typically 20 seconds).
   */
  @Override
  public void fading() {
    long e = epoch.incrementAndGet();
    int aw = Math.floorMod(e, windowCount);

    rotateSketchWindows(aw);
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
   * Apply {@code increment} to the sketch for {@code key} and return the
   * maximum cross-row slot sum observed. The top-level loop dispatches to
   * {@link #updateEmptySlot}, {@link #updateMatchingSlot}, or
   * {@link #decayCollisionSlot} depending on the slot's populated state and
   * fingerprint match. Per-row lock acquisition is delegated to those
   * sub-routines via the {@code synchronized(lockStripes[...])} enclosing block.
   */
  @SuppressWarnings("null")
  private long addToSketch(String key, long increment) {
    SlotLoc loc = locate(key);
    long itemFingerprint = loc.fp;
    long maxCount = 0;

    for (int i = 0; i < depth; i++) {
      int index = i * width + bucketIndex(loc.fp, i);
      Object lock = lockStripes[index & lockMask];

      //noinspection SynchronizationOnLocalVariableOrMethodParameter
      synchronized (lock) {
        // Re-read epoch inside the stripe lock so a concurrent fading() cannot
        // zero the window we are about to write to underneath us.
        int active = Math.floorMod(epoch.get(), windowCount);

        long cur = slotSums[index];
        if (cur == 0) {
          maxCount = updateEmptySlot(index, active, itemFingerprint, increment, maxCount);
        } else if (fingerprints[index] == itemFingerprint) {
          maxCount = updateMatchingSlot(index, active, increment, maxCount);
        } else {
          maxCount = decayCollisionSlot(index, active, itemFingerprint, increment, cur, maxCount);
        }
      }
    }
    total.add(increment);
    return maxCount;
  }

  /**
   * Initialise an empty sketch slot: store the fingerprint, write the
   * {@code increment} into the active window, and reflect it in
   * {@link #slotSums}. Returns the running {@code maxCount} (largest slot
   * sum seen so far across all rows for this add call).
   */
  private long updateEmptySlot(int index, int active, long itemFingerprint, long increment, long maxCount) {
    fingerprints[index] = itemFingerprint;
    windows[index * windowStride + active] += increment;
    slotSums[index] += increment;
    return Math.max(maxCount, slotSums[index]);
  }

  /**
   * Matching-fingerprint fast path: increment the active window and
   * {@link #slotSums} by {@code increment}.
   */
  private long updateMatchingSlot(int index, int active, long increment, long maxCount) {
    windows[index * windowStride + active] += increment;
    slotSums[index] += increment;
    return Math.max(maxCount, slotSums[index]);
  }

  /**
   * Collision-with-different-fingerprint path: sample the number of
   * survivors from a Binomial({@code increment}, {@code decayProb}) and
   * either hand the slot over to the incoming fingerprint (full reset) or
   * proportionally decay every window. Returns the running {@code maxCount}.
   */
  @SuppressWarnings({ "null", "squid:S2245" })
  private long decayCollisionSlot(
    int index,
    int active,
    long itemFingerprint,
    long increment,
    long cur,
    long maxCount
  ) {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    double decayProb = (cur < LOOKUP_TABLE_SIZE)
      ? lookupTable[(int) cur]
      : Math.pow(lookupTable[LOOKUP_TABLE_SIZE - 1], cur / (LOOKUP_TABLE_SIZE - 1.0)) *
        lookupTable[(int) (cur % (LOOKUP_TABLE_SIZE - 1))];
    int decays = sampleBinomial((int) Math.min(increment, Integer.MAX_VALUE), decayProb, rng);

    if (decays >= cur) {
      // Replace the slot: fingerprint swap, wipe all windows, replay increment.
      fingerprints[index] = itemFingerprint;
      Arrays.fill(windows, index * windowStride, index * windowStride + windowCount, 0);
      windows[index * windowStride + active] = increment;
      slotSums[index] = increment;
      return Math.max(maxCount, increment);
    }
    // Decrement each window proportionally, keep slotSums in sync.
    long sumBefore = slotSums[index];
    long totalSubtracted = 0;
    int base = index * windowStride;

    for (int w = 0; w < windowCount; w++) {
      int off = base + w;
      long wv = windows[off];
      if (wv > 0) {
        long sub = (decays * wv) / cur;
        long newVal = Math.max(0, wv - sub);
        totalSubtracted += wv - newVal;
        windows[off] = newVal;
      }
    }
    slotSums[index] = Math.max(0, sumBefore - totalSubtracted);
    return Math.max(maxCount, slotSums[index]);
  }

  /**
   * Zero the now-stale window ({@code aw}) for every sketch slot under its
   * owning stripe lock, keeping {@link #slotSums} in sync.
   */
  private void rotateSketchWindows(int aw) {
    for (int stripe = 0; stripe < lockStripes.length; stripe++) {
      synchronized (lockStripes[stripe]) {
        for (int i = stripe; i < slotSums.length; i += lockStripes.length) {
          long oldWindow = windows[i * windowStride + aw];
          if (oldWindow != 0) {
            windows[i * windowStride + aw] = 0;
            slotSums[i] -= oldWindow;
            if (slotSums[i] < 0) {
              slotSums[i] = 0; // guard against long underflow races
            }
          }
        }
      }
    }
  }

  /**
   * Halve every TopK membership count under {@link #admissionLock} and drop
   * members whose halved count falls to zero. Each {@link Node#count} is a
   * {@link LongAccumulator} with {@code Long::max} merger, so lowering the
   * stored value requires a {@code reset()} followed by {@code accumulate()}
   * (this is safe because the {@code admissionLock} blocks all concurrent
   * writes). Reads outside fading are not affected since the read path
   * uses {@code get()}, which combines with {@code Long::max}.
   */
  private void decayMembership() {
    admissionLock.lock();
    try {
      List<String> dropped = null;
      for (Node n : members.values()) {
        long halved = n.count.get() >> 1;
        if (halved > 0) {
          n.count.reset();
          n.count.accumulate(halved);
        } else {
          if (dropped == null) {
            dropped = new ArrayList<>();
          }
          dropped.add(n.key);
        }
      }
      if (dropped != null) {
        for (String key : dropped) {
          members.remove(key);
          locCache.remove(key); // decay-dropped — evict fingerprint cache
        }
      }
      minPqCount = members.isEmpty() ? 0L : findMinMember().count();
    } finally {
      admissionLock.unlock();
    }
  }

  /**
   * Apply the TopK membership decision for a key whose sketch estimate is
   * {@code maxCount}.
   *
   * <p>Hot path (key already a member): a lock-free {@link LongAccumulator#accumulate}
   * raises the member's observed count to {@code maxCount} via the {@code Long::max}
   * merger — no monitor is taken. Cold path (key not a member and
   * {@code maxCount < minCount}): returns {@link AddResult#cold()} without
   * locking. Admission path (key not yet a member but {@code maxCount >= minCount}):
   * takes {@link #admissionLock}, scans {@link #members} via {@link #findMinMember()}
   * for the current minimum, and either inserts the new key (evicting the
   * minimum when the set is full and the new count qualifies) or rejects it.
   * Eviction populates {@link #expelledQueue} on the write path to preserve
   * the {@link AddResult#expelledKey()} contract.
   */
  private AddResult admit(String key, long maxCount) {
    if (maxCount < minCount) {
      return AddResult.cold();
    }
    // Fast path: existing member — accumulator.max lift, no lock.
    Node member = members.get(key);
    if (member != null) {
      member.count.accumulate(maxCount);
      return new AddResult(null, true, key);
    }
    // Admission path: brand-new candidate, may enter or evict.
    admissionLock.lock();
    try {
      // Double-check under lock — another thread may have admitted this key.
      Node existing = members.get(key);
      if (existing != null) {
        existing.count.accumulate(maxCount);
        return new AddResult(null, true, key);
      }
      // O(1) fast reject: can't beat the current minimum member.
      if (members.size() >= k && maxCount < minPqCount) {
        return new AddResult(null, false, key);
      }
      return admitOrEvict(key, maxCount);
    } finally {
      admissionLock.unlock();
    }
  }

  /**
   * Admit or evict under {@link #admissionLock}. Only called when the key is
   * not yet a member and the O(1) fast-reject ({@link #minPqCount}) has
   * passed. Expects caller to hold the lock. The O(k) scan of
   * {@link #findMinMember} only happens on actual eviction.
   */
  private AddResult admitOrEvict(String key, long maxCount) {
    String expelledKey = null;

    if (members.size() < k) {
      members.put(key, new Node(key, maxCount));
      cacheLoc(key); // cache fingerprint for the new member
      if (maxCount < minPqCount || members.size() == 1) {
        minPqCount = maxCount;
      }
      return new AddResult(null, true, key);
    }

    // Full — must evict the minimum member (O(k) scan).
    MemberCandidate min = findMinMember();
    if (maxCount < min.count()) {
      minPqCount = min.count();
      return new AddResult(null, false, key);
    }

    Node removed = members.remove(min.key());
    if (removed != null) {
      expelledKey = removed.key;
      locCache.remove(expelledKey); // evicted — no longer needs cached fingerprint
      if (!expelledQueue.offer(new Item(removed.key, removed.count.get()))) {
        log.warn("Expelled queue full, dropping key: {}", removed.key);
      }
    }
    members.put(key, new Node(key, maxCount));
    cacheLoc(key); // cache fingerprint for the replacement member
    minPqCount = findMinMember().count();
    return new AddResult(expelledKey, true, key);
  }

  /**
   * Find the current minimum member, breaking ties on the key's natural
   * ordering to match the original {@link java.util.concurrent.ConcurrentSkipListMap}
   * semantics (lowest count, then lowest key) so eviction is deterministic.
   * Returns a sentinel (Long.MAX_VALUE, null key) when no members exist.
   */
  private MemberCandidate findMinMember() {
    long minMemberCount = Long.MAX_VALUE;
    String minKey = null;

    for (Node n : members.values()) {
      long c = n.count.get();
      if (c < minMemberCount || (c == minMemberCount && (minKey == null || n.key.compareTo(minKey) < 0))) {
        minMemberCount = c;
        minKey = n.key;
      }
    }
    return new MemberCandidate(minKey, minMemberCount);
  }

  /**
   * Snapshot the current membership, sorted by count descending (ties broken
   * on key ascending — same ordering used by the original
   * {@link java.util.concurrent.ConcurrentSkipListMap}). Limited to at most
   * {@code limit} entries. Used by both {@link #list()} and {@link #listTopN(int)}.
   */
  private List<Node> snapshotMembersSorted(int limit) {
    if (members.isEmpty()) {
      return Collections.emptyList();
    }

    List<Node> snapshot = new ArrayList<>(members.values());
    snapshot.sort((a, b) -> {
      int c = Long.compare(b.count.get(), a.count.get());
      return c != 0 ? c : a.key.compareTo(b.key);
    });
    if (snapshot.size() > limit) {
      return new ArrayList<>(snapshot.subList(0, limit));
    }
    return snapshot;
  }

  /** Cached Murmur3 fingerprint for a single key. */
  private record SlotLoc(long fp) {}

  /**
   * A key-count pair used as an entry in the TopK membership set. The count
   * is a {@link LongAccumulator} with {@code Long::max} merger, so writes
   * under same-key contention never serialise on a single CAS word.
   */
  @SuppressWarnings("ClassCanBeRecord")
  private static final class Node {

    /** The cache key. */
    final String key;
    /** Current estimated count — striped accumulator with {@code Long::max} merger. */
    final LongAccumulator count;

    Node(String key, long count) {
      this.key = key;
      this.count = new LongAccumulator(Long::max, 0);
      this.count.accumulate(count);
    }
  }

  /** Immutable minimummember snapshot returned by {@link #findMinMember()}. */
  private record MemberCandidate(String key, long count) {}

  /**
   * Sample from a Binomial(n, p) distribution in O(1) expected time.
   *
   * <p>Uses direct simulation for small n (≤10), normal approximation for
   * {@code np(1-p) > 5.0} (slightly tightened from 4.0 for improved
   * tail accuracy), and a Poisson approximation for the remaining
   * small-λ case.  The Poisson approximation uses Knuth's algorithm with
   * O(λ) expected iterations where λ = np.  The {@code p > 0.5} mirror
   * case is handled iteratively (via the complement) instead of recursing.
   */
  private static int sampleBinomial(int n, double p, ThreadLocalRandom rng) {
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
      // Iterative complement instead of recursion to avoid stack overhead.
      return n - sampleBinomial(n, 1.0 - p, rng);
    }

    double q = 1.0 - p;

    if (n <= 10) {
      int k = 0;
      for (int i = 0; i < n; i++) {
        if (rng.nextDouble() < p) {
          k++;
        }
      }
      return k;
    }

    double np = n * p;
    double npq = np * q;

    if (npq > 5.0) {
      int k = (int) Math.round(np + Math.sqrt(npq) * rng.nextGaussian());
      return Math.max(0, Math.min(n, k));
    }

    double limit = Math.exp(-np);
    int k = 0;
    double prod = 1.0;
    do {
      k++;
      prod *= rng.nextDouble();
    } while (prod > limit);
    return Math.min(k - 1, n);
  }
}
