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
package io.github.hyshmily.hotkey.hotkeydetector.heavykepper;

import com.google.common.hash.Hashing;
import io.github.hyshmily.hotkey.logging.DefaultLogger;
import io.github.hyshmily.hotkey.logging.HotKeyLogger;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * HeavyKeeper — a Count-Min Sketch variant for approximate Top‑K tracking.
 *
 * <p>Uses a 2D count array with fingerprint verification and probabilistic
 * decay to estimate the most frequent keys with bounded memory.  The
 * algorithm excels at filtering out low-frequency items while preserving
 * high-frequency key rankings with low error rates.
 *
 * <p>This implementation is thread-safe: per-bucket {@code synchronized}
 * blocks for sketch updates and a shared lock for the sorted TopK heap.
 */
public class HeavyKeeper implements TopK {

  /** Class logger. */
  private static final HotKeyLogger log = new DefaultLogger(HeavyKeeper.class);

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
   * whose count has changed because the Comparator includes the count; TreeMap.remove()
   * uses identity equality (Node lacks equals/hashCode), so the heapIndex reference works. */
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
    this.heapIndex = new HashMap<>();
    this.expelledQueue = new ArrayBlockingQueue<>(expelledQueueCapacity);
    this.total = new LongAdder();
  }

  /**
   * Record an access to the given key.
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
    /* Compute fingerprint with Guava Murmur3_32 (fixed seed ensures same key -> same fingerprint) */
    long itemFingerprint = Hashing.murmur3_32_fixed().hashString(key, StandardCharsets.UTF_8).padToLong() & 0xFFFFFFFFL;
    long maxCount = 0L;

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
          // HeavyKeeper probabilistic decay: sample the decay count from a
          // Binomial(increment, decayProb) distribution in O(1) instead of
          // looping increment times.  This keeps lock hold time bounded even
          // when the Worker-side batch increment is large.
          ThreadLocalRandom rng = ThreadLocalRandom.current();
          double decayProb = (counts[index] < LOOKUP_TABLE_SIZE)
            ? lookupTable[(int) counts[index]]
            : lookupTable[LOOKUP_TABLE_SIZE - 1];

          int decays = sampleBinomial(increment, decayProb, rng);
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

    if (maxCount < minCount) {
      return new AddResult(null, false, key);
    }

    synchronized (sortedTopK) {
      Node existing = heapIndex.remove(key);
      if (existing != null) {
        sortedTopK.remove(existing);
      }

      boolean isHot = false;
      String expelled = null;

      if (sortedTopK.size() < k || maxCount >= sortedTopK.firstKey().count) {
        Node newNode = new Node(key, maxCount);
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
        isHot = true;
      }
      return new AddResult(expelled, isHot, key);

    }
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
  public List<AddResult> add(Map<String, Long> keyCounts) {
    Map<String, Long> maxCounts = new HashMap<>(keyCounts.size());
    for (var entry : keyCounts.entrySet()) {
      long max = addToSketch(entry.getKey(), entry.getValue());
      maxCounts.put(entry.getKey(), max);
    }

    List<AddResult> results = new ArrayList<>();

    synchronized (sortedTopK) {
      for (var entry : keyCounts.entrySet()) {
        String key = entry.getKey();
        long maxCount = maxCounts.get(key);
        if (maxCount < minCount) {
            continue;
        }

        Node existing = heapIndex.remove(key);
        if (existing != null) {
            sortedTopK.remove(existing);
        }

        boolean shouldInsert = sortedTopK.size() < k
                || maxCount >= sortedTopK.firstKey().count;
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

  private long addToSketch(String key, long increment) {
    long itemFingerprint = Hashing.murmur3_32_fixed()
            .hashString(key, StandardCharsets.UTF_8).padToLong() & 0xFFFFFFFFL;
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
   * Return all keys currently in the TopK set, sorted by estimated count descending.
   *
   * @return an unmodifiable-style list of {@link Item} entries, from highest to lowest count
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
    synchronized (sortedTopK) {
      return heapIndex.containsKey(key);
    }
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
   * Halve all frequency counters in the sketch and the sorted heap,
   * removing entries whose count drops to zero.  Also halves the
   * running total.
   */
  @Override
  public void fading() {
    // Halve all sketch counters. long writes are NOT atomic per JLS 17.7,
    // so concurrent add() may observe a torn value during fading. This is
    // an acceptable precision trade-off for ~200K fewer locks.
    for (int i = 0; i < counts.length; i++) {
      counts[i] >>= 1;
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
   * Return the top {@code n} hot keys.
   *
   * @param n maximum number of keys to return
   * @return list of at most {@code n} {@link Item} entries
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
   * <p>Uses direct simulation for small n (≤10) and normal approximation
   * for larger n when np(1-p) > 9.  Falls back to direct simulation for
   * moderate n.
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

    if (npq > 9.0) {
      int k = (int) Math.round(np + Math.sqrt(npq) * rng.nextGaussian());
      return Math.max(0, Math.min(n, k));
    }

    int k = 0;
    for (int i = 0; i < n; i++) {
      if (rng.nextDouble() < p) {
          k++;
      }
    }
    return k;
  }
}
