package io.github.hyshmily.hotkey.algorithm;

import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;

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

  public HeavyKeeper(int k, int width, int depth, double decay, int minCount) {
    if (k <= 0) {
      throw new IllegalArgumentException("topK must be greater than 0, but got: " + k);
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
    /*
      Compute fingerprint with Guava Murmur3_32 (fixed seed ensures same key ->same
      fingerprint)
     */
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
          expelledQueue.offer(new Item(expelled, removed.count));
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
  public BlockingQueue<Item> expelled() {
    return expelledQueue;
  }

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

      long half = total.sumThenReset() >> 1;
      if (half > 0) {
        total.add(half);
      }
    }
  }

  @Override
  public long total() {
    return total.sum();
  }

  private static class Node {

    final String key;
    final int count;

    Node(String key, int count) {
      this.key = key;
      this.count = count;
    }
  }
}
