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
package io.github.hyshmily.zeta.sharding;

import com.google.common.hash.Hashing;
import io.github.hyshmily.zeta.Internal;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Consistent hash ring backed by sorted arrays for O(log n + k) lookups.
 *
 * <p>Each physical node is replicated across the ring with {@code virtualNodeCount}
 * virtual nodes, all mapped to the same physical {@code nodeId}.  The entire ring
 * is rebuilt atomically via {@link #rebuild(Set)} — no partial mutations, safe for
 * lock-free reads on the hot path.
 *
 * <p>Compared to the previous {@link TreeMap}-based implementation, this version
 * uses sorted {@code int[]} and {@code String[]} arrays to provide:
 * <ul>
 *   <li>O(log n) binary search to find the starting point</li>
 *   <li>O(1) sequential scan for each probe (no tree successor traversal)</li>
 *   <li>Improved CPU cache locality and reduced GC pressure</li>
 * </ul>
 */
@Internal
@RequiredArgsConstructor
@Slf4j
public class ConsistentHashRing {

  /**
   * Immutable snapshot of the ring at a point in time.
   * <p>
   * {@code equals}, {@code hashCode}, and {@code toString} are overridden
   * to consider the content of the arrays rather than their references.
   */
  private record RingState(
    /* Sorted hash values of all virtual nodes. */
    int[] hashRing,
    /* Physical node IDs aligned with {@code hashRing}. */
    String[] nodeRing,
    /* The set of live physical node IDs (for quick size/lookup). */
    Set<String> liveNodes
  ) {
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof RingState that)) return false;
      return (
        Arrays.equals(hashRing, that.hashRing) &&
        Arrays.equals(nodeRing, that.nodeRing) &&
        liveNodes.equals(that.liveNodes)
      );
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(hashRing);
      result = 31 * result + Arrays.hashCode(nodeRing);
      result = 31 * result + liveNodes.hashCode();
      return result;
    }

    @Override
    @SuppressWarnings("all")
    public String toString() {
      return (
        "RingState{" +
        "hashRing=" +
        Arrays.toString(hashRing) +
        ", nodeRing=" +
        Arrays.toString(nodeRing) +
        ", liveNodes=" +
        liveNodes +
        '}'
      );
    }
  }

  /** Number of virtual copies created per physical node on the ring. */
  private final int virtualNodeCount;

  /** Current immutable ring snapshot, thread-safe via {@link AtomicReference}. */
  private final AtomicReference<RingState> currentState = new AtomicReference<>(
    new RingState(new int[0], new String[0], Collections.emptySet())
  );

  /** Safety limit to prevent infinite-loop when the ring is corrupt or all nodes are dead. */
  private static final int MAX_PROBES = 512;

  /**
   * Atomically replace the ring with one built from the given live nodes.
   * Each node is replicated {@code virtualNodeCount} times on the ring.
   *
   * @param liveNodes the set of physical node IDs to place on the ring;
   *                  must not be {@code null}
   */
  public void rebuild(Set<String> liveNodes) {
    // 1. Collect all virtual nodes in a TreeMap for temporary sorting.
    TreeMap<Integer, String> sortedVirtualNodes = new TreeMap<>();
    for (String nodeId : liveNodes) {
      for (int i = 0; i < virtualNodeCount; i++) {
        sortedVirtualNodes.put(hash(nodeId + ":" + i), nodeId);
      }
    }

    // 2. Convert to sorted arrays for fast lock-free access.
    int size = sortedVirtualNodes.size();
    int[] hashes = new int[size];
    String[] nodes = new String[size];
    int idx = 0;
    for (Map.Entry<Integer, String> entry : sortedVirtualNodes.entrySet()) {
      hashes[idx] = entry.getKey();
      nodes[idx] = entry.getValue();
      idx++;
    }

    // 3. Publish new snapshot atomically.
    currentState.set(new RingState(hashes, nodes, Set.copyOf(liveNodes)));
  }

  /**
   * Return the physical node responsible for the given business key, skipping nodes
   * that fail the liveness predicate.
   *
   * <p>Performs a binary search to locate the starting virtual node and then walks
   * the sorted array circularly until a live physical node is found.
   *
   * @param key     the business key to route; must not be {@code null}
   * @param isAlive predicate that returns {@code true} if a node is considered alive;
   *                must not be {@code null}
   * @return the physical node ID, or {@code null} if the ring is empty or all nodes are dead
   */
  public String locateNode(String key, Predicate<String> isAlive) {
    RingState state = currentState.get();
    int[] hashes = state.hashRing;
    String[] nodes = state.nodeRing;
    int len = hashes.length;

    if (len == 0) {
      return null;
    }

    int hashKey = hash(key);
    // Find the first index where hashes[i] >= hashKey (insertion point).
    int startIdx = binarySearch(hashes, hashKey);
    if (startIdx < 0) {
      startIdx = -(startIdx + 1);
      // Wraparound to the beginning of the ring.
      if (startIdx == len) {
        startIdx = 0;
      }
    }

    int idx = startIdx;
    int probes = 0;
    do {
      String physicalNode = nodes[idx];
      if (isAlive.test(physicalNode)) {
        return physicalNode;
      }
      idx = (idx + 1) % len; // circular advancement
      if (++probes > MAX_PROBES) {
        log.warn(
          "Exhausted {} probes in consistent hash ring for key '{}', " +
            "all workers appear dead or ring is corrupt. Discarding this key.",
          MAX_PROBES,
          key
        );
        break;
      }
    } while (idx != startIdx);

    return null;
  }

  /**
   * Return the set of live physical node IDs.
   *
   * @return an unmodifiable set of node IDs (never {@code null})
   */
  public Set<String> getNodes() {
    return currentState.get().liveNodes;
  }

  /**
   * Return whether the ring contains no virtual nodes (i.e. no live nodes).
   *
   * @return {@code true} if the ring is empty
   */
  public boolean isEmpty() {
    return currentState.get().hashRing.length == 0;
  }

  /**
   * Return the number of live physical nodes in the ring.
   *
   * @return the node count (zero if no live nodes)
   */
  public int nodeCount() {
    return currentState.get().liveNodes.size();
  }

  /**
   * Compute a 32-bit Murmur3 hash for the given string using Guava's
   * {@code murmur3_32_fixed()} implementation.
   *
   * @param key the string to hash; must not be {@code null}
   * @return the hash value (may be negative)
   */
  private static int hash(String key) {
    return Hashing.murmur3_32_fixed().hashString(key, StandardCharsets.UTF_8).asInt();
  }

  /**
   * Performs a standard binary search on a sorted {@code int[]} array.
   *
   * @param a      the array to search (must be sorted in ascending order)
   * @param target the value to search for
   * @return index of the target if found; otherwise, {@code -(insertion point) - 1}
   *         (same contract as {@link Arrays#binarySearch(int[], int)})
   */
  private static int binarySearch(int[] a, int target) {
    int low = 0;
    int high = a.length - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      int midVal = a[mid];
      if (midVal < target) {
        low = mid + 1;
      } else if (midVal > target) {
        high = mid - 1;
      } else {
        return mid; // exact match
      }
    }
    return -(low + 1); // insertion point
  }
}
