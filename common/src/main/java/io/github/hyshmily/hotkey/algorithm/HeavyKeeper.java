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
package io.github.hyshmily.hotkey.algorithm;

import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

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
@Slf4j
public class HeavyKeeper implements TopK {

  private static final int LOOKUP_TABLE_SIZE = 256;
  private static final int LOCK_STRIPES = 256;

  private final int k;
  private final int width;
  private final int depth;
  private final double[] lookupTable;
  private final long[] fingerprints;
  private final int[] counts;
  private final Object[] lockStripes;
  private final int lockMask;
  private final TreeMap<Node, Boolean> sortedTopK;
  private final Map<String, Node> heapIndex;
  private final BlockingQueue<Item> expelledQueue;
  private final LongAdder total;
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
    this.counts = new int[totalSlots];
    this.lockStripes = new Object[LOCK_STRIPES];
    for (int i = 0; i < LOCK_STRIPES; i++) {
      lockStripes[i] = new Object();
    }
    this.lockMask = LOCK_STRIPES - 1;

    this.sortedTopK = new TreeMap<>(Comparator.comparingInt((Node a) -> a.count).thenComparing(a -> a.key));
    this.heapIndex = new HashMap<>();
    this.expelledQueue = new ArrayBlockingQueue<>(10_000);
    this.total = new LongAdder();
  }

  @Override
  public AddResult add(String key, int increment) {
    /* Compute fingerprint with Guava Murmur3_32 (fixed seed ensures same key -> same fingerprint) */
    long itemFingerprint = Hashing.murmur3_32_fixed().hashString(key, StandardCharsets.UTF_8).padToLong() & 0xFFFFFFFFL;
    int maxCount = 0;

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
          // HeavyKeeper probabilistic decay: each of the `increment` steps
          // has a `decayProb` chance of decrementing the existing counter.
          // decayProb grows with the counter value (via the pre-computed lookup
          // table), so larger counters are more likely to be decremented.
          // When the counter reaches 0, the slot is claimed for the new item.
          for (int j = 0; j < increment; j++) {
            double decayProb = (counts[index] < LOOKUP_TABLE_SIZE)
              ? lookupTable[counts[index]]
              : lookupTable[LOOKUP_TABLE_SIZE - 1];

            if (ThreadLocalRandom.current().nextDouble() < decayProb) {
              counts[index]--;
              if (counts[index] == 0) {
                fingerprints[index] = itemFingerprint;
                counts[index] = increment - j;
                maxCount = Math.max(maxCount, counts[index]);
                break;
              }
            }
          }
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

  @Override
  public boolean contains(String key) {
    synchronized (sortedTopK) {
      return heapIndex.containsKey(key);
    }
  }

  /**
   * Return the blocking queue holding items that have been evicted from the TopK set.
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
    for (int i = 0; i < depth; i++) {
      for (int j = 0; j < width; j++) {
        int index = i * width + j;
        Object lock = lockStripes[index & lockMask];
        synchronized (lock) {
          counts[index] >>= 1;
        }
      }
    }

    // Rebuild the TopK set: halve all counts and discard entries that drop to 0.
    // Halving (rather than decaying proportionally) keeps the relative ordering
    // while giving new keys a chance to enter the TopK set.
    synchronized (sortedTopK) {
      TreeMap<Node, Boolean> newMap = new TreeMap<>(sortedTopK.comparator());
      heapIndex.clear();
      for (Node node : sortedTopK.keySet()) {
        int half = node.count >> 1;
        if (half > 0) {
          Node newNode = new Node(node.key, half);
          newMap.put(newNode, Boolean.TRUE);
          heapIndex.put(node.key, newNode);
        }
      }
      sortedTopK.clear();
      sortedTopK.putAll(newMap);

      // Also halve the global stream counter so newly emitted keys are not
      // immediately disadvantaged against the historical total.
      long half = total.sumThenReset() >> 1;
      if (half > 0) {
        total.add(half);
      }
    }
  }

  /**
   * Return the total number of data streams tracked since startup or last reset.
   */
  @Override
  public long total() {
    return total.sum();
  }

  /**
   * Return the top {@code n} hot keys.
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
    final int count;
  }
}
