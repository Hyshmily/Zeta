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
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
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
 * <p><b>Concurrency model:</b> Thread-safe with two lock tiers:
 * <ol>
 *   <li>Fine-grained striped synchronization ({@link #LOCK_STRIPES} stripes)
 *       on individual sketch buckets for low-contention sketch updates.</li>
 *   <li>A single coarse-grained lock on the sorted TopK heap
 *       ({@code sortedTopK}) for heap mutations and reads.</li>
 * </ol>
 * The separation ensures that sketch writes (the hot path) rarely contend
 * with heap operations (which happen only when a key crosses a threshold).
 *
 * <p><b>Decay:</b> {@link #fading()} halves all counters periodically to
 * age out historical data, keeping the TopK set reflective of recent access
 * patterns.
 */
@Slf4j
public class HeavyKeeper implements TopK {

  /** Pre-computed decay probability lookup table size ({@value}). */
  private static final int LOOKUP_TABLE_SIZE = 256;
  /** Number of lock stripes for fine-grained concurrency ({@value}). Must be a power of two. */
  private static final int LOCK_STRIPES = 256;

  /**
   * -- GETTER --
   * Maximum number of hot keys tracked.
   */
  @Getter
  private final int k;

  /**
   * -- GETTER --
   * Width of the Count-Min Sketch (columns per row).
   */
  @Getter
  private final int width;

  /**
   * -- GETTER --
   * Depth of the Count-Min Sketch (rows / hash functions).
   */
  @Getter
  private final int depth;

  /** Pre-computed decay probabilities: {@code decay^i} for {@code i in [0, LOOKUP_TABLE_SIZE)}. */
  private final double[] lookupTable;
  /** Per-slot fingerprint values for collision verification in the Count-Min Sketch. */
  private final long[] fingerprints;
  /** Per-slot frequency counters for the Count-Min Sketch. */
  private final long[] counts;
  /** Striped lock objects for fine-grained concurrency on sketch slot updates. */
  private final Object[] lockStripes;
  /** Bitmask for mapping bucket index to lock stripe (stripe count must be power of two). */
  private final int lockMask;
  /** Sorted map of current TopK entries, ordered by count ascending then key lexicographically.
   * Boolean.TRUE is a structural placeholder.  A TreeSet would not safely remove a Node
   * whose count had changed because the Comparator includes the count; TreeMap.remove()
   * uses the comparator (not identity — Node lacks equals/hashCode), but since Node. Count
   * is final the comparator result for a given Node instance is stable and removal works. */
  private final TreeMap<Node, Boolean> sortedTopK;
  /** Reverse index from key to its {@link Node} in the sorted map, for O(1) lookups. */
  private final Map<String, Node> heapIndex;
  /** Bounded blocking queue receiving expelled (evicted) key-count items for downstream consumption. */
  private final BlockingQueue<Item> expelledQueue;
  /** Running total of all tracked data streams since startup or last {@link #fading()}. */
  private final LongAdder total;

  /**
   * -- GETTER --
   * Minimum count threshold before a key can enter the TopK set.
   */
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
    this(k, width, depth, decay, minCount, 50_000);
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
    if (k <= 0) {
      throw new IllegalArgumentException("TopK must be greater than 0, but got: " + k);
    }
    this.k = k;
    this.width = width;
    this.depth = depth;
    this.minCount = minCount;

    this.lookupTable = new double[LOOKUP_TABLE_SIZE];
    for (int i = 0; i < LOOKUP_TABLE_SIZE; i++) {
      lookupTable[i] = Math.pow(decay, i);
    }

    int totalSlots = depth * width;
    this.fingerprints = new long[totalSlots];
    this.counts = new long[totalSlots];
    this.lockStripes = new Object[LOCK_STRIPES];
    for (int i = 0; i < LOCK_STRIPES; i++) {
      lockStripes[i] = new Object();
    }
    this.lockMask = LOCK_STRIPES - 1;

    this.sortedTopK = new TreeMap<>(Comparator.comparingLong((Node a) -> a.count).thenComparing(a -> a.key));
    this.heapIndex = new ConcurrentHashMap<>();
    this.expelledQueue = new ArrayBlockingQueue<>(expelledQueueCapacity);
    this.total = new LongAdder();
  }

  /**
   * Record access to the given key.
   *
   * <p>The returned {@link AddResult} carries an {@code expelledKey} when the key
   * entered the TopK set by displacing a previous member (non-null expelledKey and
   * {@code isHot == true}).  When the key was already in the TopK set or failed to
   * meet the minimum count threshold, {@code expelledKey} is {@code null}.
   *
   * @param key       the cache key being accessed
   * @param increment the frequency increment (typically 1)
   * @return an {@link AddResult} with expelled key (or null), hot status, and the input key
   */
  @Override
  public AddResult addDirect(String key, int increment) {
    long maxCount = addToSketch(key, increment);
    return updateHeap(key, maxCount);
  }

  /**
   * Record accesses for multiple keys.
   *
   * <p>Updates sketch counters for all keys, then updates the TopK heap with
   * keys whose estimated count meets the minimum threshold.  Returns results
   * only for keys that entered the TopK set (possibly displacing others).
   *
   * @param keyCounts map of keys to their access counts
   * @return list of {@link AddResult} for keys that entered the TopK set
   */
  @Override
  public List<AddResult> addDirect(Map<String, Long> keyCounts) {
    // First pass: update sketch counters and collect maxCounts (no heap lock).
    Map<String, Long> maxCounts = new HashMap<>(keyCounts.size());
    for (var entry : keyCounts.entrySet()) {
      long max = addToSketch(entry.getKey(), entry.getValue());
      maxCounts.put(entry.getKey(), max);
    }

    // Second pass: only lock the heap for keys that meet minCount,
    // reducing heap-lock hold time from O(all keys) to O(candidates).
    List<AddResult> results = new ArrayList<>();
    List<Map.Entry<String, Long>> candidates = new ArrayList<>(maxCounts.size());
    for (var entry : maxCounts.entrySet()) {
      if (entry.getValue() >= minCount) {
        candidates.add(entry);
      }
    }

    if (candidates.isEmpty()) {
      return results;
    }

    synchronized (sortedTopK) {
      for (var entry : candidates) {
        String key = entry.getKey();
        long maxCount = entry.getValue();

        Node existing = heapIndex.remove(key);
        if (existing != null) {
          sortedTopK.remove(existing);
        }

        boolean shouldInsert = sortedTopK.size() < k || maxCount >= sortedTopK.firstKey().count;
        if (shouldInsert) {
          Node newNode = new Node(key, maxCount);
          String expelledKey = null;
          if (sortedTopK.size() >= k) {
            Node removed = sortedTopK.pollFirstEntry().getKey();
            expelledKey = removed.key;
            heapIndex.remove(removed.key);
            if (!expelledQueue.offer(new Item(removed.key, removed.count))) {
              log.warn("Expelled queue full, dropping key: {}", removed.key);
            }
          }
          sortedTopK.put(newNode, Boolean.TRUE);
          heapIndex.put(key, newNode);
          results.add(new AddResult(expelledKey, true, key));
        }
      }
    }
    return results;
  }

  /**
   * Update the TopK heap with the estimated count for a single key.
   * If the key's count meets {@link #minCount} and is high enough
   * to enter (or remain in) the TopK set, the heap is updated under
   * the shared heap lock.
   *
   * @param key      the cache key
   * @param maxCount the estimated count from the sketch
   * @return an {@link AddResult} describing whether the key entered the TopK set
   */
  private AddResult updateHeap(String key, long maxCount) {
    if (maxCount < minCount) {
      return AddResult.cold();
    }
    synchronized (sortedTopK) {
      Node existing = heapIndex.remove(key);
      if (existing != null) {
        sortedTopK.remove(existing);
      }

      if (sortedTopK.size() < k || maxCount >= sortedTopK.firstKey().count) {
        Node newNode = new Node(key, maxCount);
        String expelled = null;
        if (sortedTopK.size() >= k) {
          Node removed = sortedTopK.pollFirstEntry().getKey();
          expelled = removed.key;
          heapIndex.remove(removed.key);
          if (!expelledQueue.offer(new Item(expelled, removed.count))) {
            log.warn("Expelled queue full, dropping key: {}", expelled);
          }
        }
        sortedTopK.put(newNode, Boolean.TRUE);
        heapIndex.put(key, newNode);
        return new AddResult(expelled, true, key);
      }
      return new AddResult(null, false, key);
    }
  }

  @SuppressWarnings("null")
  private long addToSketch(String key, long increment) {
    long itemFingerprint = Hashing.murmur3_32_fixed().hashString(key, StandardCharsets.UTF_8).padToLong() & 0xFFFFFFFFL;
    long maxCount = 0;

    for (int i = 0; i < depth; i++) {
      int hash = (int) (itemFingerprint ^ (i * 0x9e3779b97f4a7c15L));
      int bucketIndex = Math.floorMod(hash, width);
      int index = i * width + bucketIndex;
      Object lock = lockStripes[index & lockMask];

      synchronized (lock) {
        if (counts[index] == 0) {
          fingerprints[index] = itemFingerprint;
          counts[index] = increment;

          maxCount = Math.max(maxCount, increment);
        } else if (fingerprints[index] == itemFingerprint) {
          counts[index] += increment;

          maxCount = Math.max(maxCount, counts[index]);
        } else {
          ThreadLocalRandom rng = ThreadLocalRandom.current();
          double decayProb = (counts[index] < LOOKUP_TABLE_SIZE)
            ? lookupTable[(int) counts[index]]
            : lookupTable[LOOKUP_TABLE_SIZE - 1];
          int decays = sampleBinomial((int) Math.min(increment, Integer.MAX_VALUE), decayProb, rng);

          if (decays >= counts[index]) {
            fingerprints[index] = itemFingerprint;
            counts[index] = increment;
          } else {
            counts[index] -= decays;
          }
          maxCount = Math.max(maxCount, counts[index]);
        }
      }
    }
    total.add(increment);
    return maxCount;
  }

  /**
   * Return all keys currently in the TopK set, sorted by estimated count
   * descending (highest frequency first).
   *
   * <p>The returned list is a point-in-time snapshot: it is safe to iterate
   * after the lock is released but may be stale immediately.
   *
   * @return a point-in-time list of {@link Item} entries, ordered from highest
   *         to lowest estimated count; never {@code null}
   */
  @Override
  public List<Item> list() {
    synchronized (sortedTopK) {
      List<Item> result = new ArrayList<>(sortedTopK.size());
      for (Node node : sortedTopK.descendingKeySet()) {
        result.add(new Item(node.key, node.count));
      }
      return result;
    }
  }

  /**
   * Check whether a key is currently in the TopK set.
   *
   * @param key the cache key to test
   * @return {@code true} if the key is in the TopK set, {@code false} otherwise
   */
  @Override
  public boolean contains(String key) {
    return heapIndex.containsKey(key);
  }

  /**
   * Return the blocking queue holding items that have been evicted from the TopK set.
   * Consumers should drain this queue periodically for asynchronous processing.
   *
   * @return a blocking queue of evicted items
   */
  @Override
  public BlockingQueue<Item> expelled() {
    return expelledQueue;
  }

  /**
   * Halve all frequency counters in the sketch and the sorted TopK heap.
   *
   * <p>Entries whose count drops to zero after halving are removed from
   * the heap entirely. The running total is also halved. This periodic
   * decay prevents the sketch from saturating with stale historical data
   * and ensures that the TopK ranking reflects recent access patterns
   * rather than cumulative lifetime counts.
   *
   * <p>This operation is invoked automatically by a scheduler at a
   * configured interval (typically 30 seconds). Calling it concurrently
   * with {@link #addDirect} is safe — sketch counters are halved under
   * per-stripe locks and the heap is rebuilt atomically under the shared
   * heap lock.
   */
  @Override
  public void fading() {
    // Halve all sketch counters using per-stripe locking — one lock acquire
    // per stripe instead of per-slot, reducing ~250k lock ops to 256.
    for (int stripe = 0; stripe < LOCK_STRIPES; stripe++) {
      synchronized (lockStripes[stripe]) {
        for (int i = stripe; i < counts.length; i += LOCK_STRIPES) {
          counts[i] >>= 1;
        }
      }
    }

    // Rebuild the TopK set: halve all counts and discard entries that drop to 0.
    synchronized (sortedTopK) {
      TreeMap<Node, Boolean> newMap = new TreeMap<>(sortedTopK.comparator());
      heapIndex.clear();
      for (Node node : sortedTopK.keySet()) {
        long half = node.count >> 1;
        if (half > 0) {
          Node newNode = new Node(node.key, half);
          newMap.put(newNode, Boolean.TRUE);
          heapIndex.put(node.key, newNode);
        }
      }
      sortedTopK.clear();
      sortedTopK.putAll(newMap);

      long half = total.sumThenReset() >> 1;
      if (half > 0) {
        total.add(half);
      }
    }
  }

  /**
   * Return the total number of data streams tracked since startup or last reset.
   *
   * @return total access count
   */
  @Override
  public long total() {
    return total.sum();
  }

  /**
   * Return the top {@code n} hot keys, ordered by estimated count descending.
   *
   * <p>If fewer than {@code n} keys are currently tracked, the returned list
   * contains all available keys. The result is a point-in-time snapshot.
   *
   * @param n maximum number of keys to return (must be non-negative)
   * @return list of at most {@code n} {@link Item} entries, never {@code null};
   *         empty if no keys are tracked or {@code n == 0}
   */
  @Override
  public List<Item> listTopN(int n) {
    synchronized (sortedTopK) {
      return sortedTopK
        .descendingKeySet()
        .stream()
        .limit(n)
        .map(node -> new Item(node.key, node.count))
        .collect(Collectors.toList());
    }
  }

  /** A key-count pair used as an entry in the sorted TopK tree. */
  @AllArgsConstructor
  private static class Node {

    /** The cache key. */
    final String key;
    /** Its current estimated count. */
    final long count;
  }

  /**
   * Sample from a Binomial(n, p) distribution in O(1) expected time.
   *
   * <p>Uses direct simulation for small n (≤10), normal approximation for
   * {@code np(1-p) > 4.0} (lowered from 9.0 for broader coverage), and a
   * Poisson approximation for the remaining small-λ case.  The Poisson
   * approximation uses Knuth's algorithm with O(λ) expected iterations
   * where λ = np.  After the symmetry reduction (p → 1-p when p > 0.5),
   * λ ≤ 8 whenever the normal approximation is not applicable, so the
   * pathological O(n) fallback is eliminated entirely.
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

    // Use symmetry: Binomial(n, p) = n - Binomial(n, 1-p)
    // to keep p in [0, 0.5] so λ = np is bounded when npq is small.
    if (p > 0.5) {
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

    // Normal approximation — lowered from 9.0 to 4.0.
    // Acceptable for a probabilistic sketch where small errors are inherent.
    if (npq > 4.0) {
      int k = (int) Math.round(np + Math.sqrt(npq) * rng.nextGaussian());
      return Math.max(0, Math.min(n, k));
    }

    // Poisson approximation Binomial(n, p) ≈ Poisson(λ) with λ = np.
    // After symmetry reduction p ≤ 0.5, so λ = np ≤ npq / q ≤ 4 / 0.5 = 8.
    // Knuth's algorithm runs in O(λ) expected iterations.
    double L = Math.exp(-np);
    int k = 0;
    double prod = 1.0;
    do {
      k++;
      prod *= rng.nextDouble();
    } while (prod > L);
    return Math.min(k - 1, n);
  }
}
