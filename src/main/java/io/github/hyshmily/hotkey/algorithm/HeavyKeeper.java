package io.github.hyshmily.hotkey.algorithm;

import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class HeavyKeeper implements TopK {

  private static final int LOOKUP_TABLE_SIZE = 256;

  private final int k;
  private final int width;
  private final int depth;
  private final double[] lookupTable;
  private final Bucket[][] buckets;
  private final PriorityQueue<Node> minHeap;
  private final BlockingQueue<Item> expelledQueue;
  private final AtomicLong total;
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

    this.buckets = new Bucket[depth][width];
    for (int i = 0; i < depth; i++) {
      for (int j = 0; j < width; j++) {
        buckets[i][j] = new Bucket();
      }
    }

    this.minHeap = new PriorityQueue<>(Comparator.comparingInt(n -> n.count));
    this.expelledQueue = new ArrayBlockingQueue<>(10_000);
    this.total = new AtomicLong(0);
  }

  @Override
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public AddResult add(String key, int increment) {
    // 使用 Guava Murmur3_32 计算指纹（固定种子，保证相同 key 得到相同指纹）
    /*
      Compute fingerprint with Guava Murmur3_32 (fixed seed ensures same key ->same
      fingerprint)
     */
    long itemFingerprint = Hashing.murmur3_32_fixed().hashString(key, StandardCharsets.UTF_8).padToLong() & 0xFFFFFFFFL;
    int maxCount = 0;
    Bucket[] touched = new Bucket[depth];

    for (int i = 0; i < depth; i++) {
      // 每行使用不同种子计算桶索引，增加分散性
      // Each row uses a different seed for bucket index to improve dispersion
      int hash = (int) (itemFingerprint ^ (i * 0x9e3779b97f4a7c15L));
      int bucketIndex = Math.floorMod(hash, width);
      Bucket bucket = buckets[i][bucketIndex];
      touched[i] = bucket;

      synchronized (bucket) {
        if (bucket.count == 0) {
          bucket.fingerprint = itemFingerprint;
          bucket.count = increment;
          maxCount = Math.max(maxCount, increment);
        } else if (bucket.fingerprint == itemFingerprint) {
          bucket.count += increment;
          maxCount = Math.max(maxCount, bucket.count);
        } else {
          // 冲突衰减
          // Conflict decay
          for (int j = 0; j < increment; j++) {
            double decayProb = (bucket.count < LOOKUP_TABLE_SIZE)
                ? lookupTable[bucket.count]
                : lookupTable[LOOKUP_TABLE_SIZE - 1];
            if (ThreadLocalRandom.current().nextDouble() < decayProb) {
              bucket.count--;
              if (bucket.count == 0) {
                bucket.fingerprint = itemFingerprint;
                bucket.count = increment - j;
                maxCount = Math.max(maxCount, bucket.count);
                break;
              }
            }
          }
        }
      }
    }

    total.addAndGet(increment);

    int actualMax = 0;
    for (int i = 0; i < depth; i++) {
      synchronized (touched[i]) {
        actualMax = Math.max(actualMax, touched[i].count);
      }
    }
    maxCount = actualMax;

    if (maxCount < minCount) {
      return new AddResult(null, false, key);
    }

    synchronized (minHeap) {
      minHeap.removeIf(n -> n.key.equals(key));
      boolean isHot = false;
      String expelled = null;

      if (minHeap.size() < k || maxCount >= Objects.requireNonNull(minHeap.peek()).count) {
        Node newNode = new Node(key, maxCount);
        if (minHeap.size() >= k) {
          Node removed = minHeap.poll();
          if (removed != null) {
            expelled = removed.key;
            expelledQueue.offer(new Item(expelled, removed.count));
          }
        }
        minHeap.add(newNode);
        isHot = true;
      }
      return new AddResult(expelled, isHot, key);
    }
  }

  @Override
  public List<Item> list() {
    synchronized (minHeap) {
      List<Item> result = new ArrayList<>(minHeap.size());
      for (Node node : minHeap) {
        result.add(new Item(node.key, node.count));
      }
      result.sort((a, b) -> Integer.compare(b.count(), a.count()));
      return result;
    }
  }

  @Override
  public BlockingQueue<Item> expelled() {
    return expelledQueue;
  }

  @Override
  public void fading() {
    for (Bucket[] row : buckets) {
      for (Bucket bucket : row) {
        synchronized (bucket) {
          bucket.count >>= 1;
        }
      }
    }

    synchronized (minHeap) {
      PriorityQueue<Node> newHeap = new PriorityQueue<>(Comparator.comparingInt(n -> n.count));
      for (Node node : minHeap) {
        int half = node.count >> 1;
        if (half > 0) {
          newHeap.add(new Node(node.key, half));
        }
      }
      minHeap.clear();
      minHeap.addAll(newHeap);
      total.updateAndGet(v -> v >> 1);
    }
  }

  @Override
  public long total() {
    return total.get();
  }

  private static class Bucket {

    long fingerprint;
    int count;
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
