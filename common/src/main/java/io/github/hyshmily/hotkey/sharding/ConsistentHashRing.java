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
package io.github.hyshmily.hotkey.sharding;

import com.google.common.hash.Hashing;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Predicate;

/**
 * Consistent hash ring backed by a {@link TreeMap}.
 *
 * <p>Each physical node is replicated across the ring with {@code virtualNodeCount}
 * virtual nodes, all mapped to the same physical {@code nodeId}.  The entire ring
 * is rebuilt atomically via {@link #rebuild(Set)} — no partial mutations, safe for
 * lock-free reads on the hot path.
 */
@RequiredArgsConstructor
@Slf4j
public class ConsistentHashRing {

  /**
   * Immutable snapshot of the ring at a point in time.
   *
   * @param ring      the virtual-node hash-to-nodeId mapping
   * @param liveNodes the set of live physical node IDs
   */
  private record RingState(NavigableMap<Integer, String> ring, Set<String> liveNodes) {}

  /** Number of virtual copies created per physical node on the ring. */
  private final int virtualNodeCount;
  /** Current immutable ring snapshot; read lock-free, replaced atomically via {@link #rebuild(Set)}. */
  private volatile RingState currentState = new RingState(Collections.emptyNavigableMap(), Collections.emptySet());

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
    NavigableMap<Integer, String> ring = new TreeMap<>();

    for (String nodeId : liveNodes) {
      for (int i = 0; i < virtualNodeCount; i++) {
        ring.put(hash(nodeId + ":" + i), nodeId);
      }
    }
    currentState = new RingState(Collections.unmodifiableNavigableMap(ring), Set.copyOf(liveNodes));
  }

  /**
   * Return the physical node responsible for the given business key, skipping nodes
   * that fail the liveness predicate.
   *
   * @param key     the business key to route; must not be {@code null}
   * @param isAlive predicate that returns {@code true} if a node is considered alive;
   *                must not be {@code null}
   * @return the physical node ID, or {@code null} if the ring is empty or all nodes are dead
   */
  public String locateNode(String key, Predicate<String> isAlive) {
    NavigableMap<Integer, String> currentRing = currentState.ring;

    if (currentRing.isEmpty()) {
      return null;
    }

    int hashKey = hash(key);
    Map.Entry<Integer, String> entry = currentRing.ceilingEntry(hashKey);
    if (entry == null) {
      entry = currentRing.firstEntry();
    }

    int startHash = entry.getKey();
    int currentHash = startHash;

    int probes=0;

    do {
      String physicalNode = currentRing.get(currentHash);
      if (physicalNode != null && isAlive.test(physicalNode)) {
        return physicalNode;
      }

      Map.Entry<Integer, String> next = currentRing.higherEntry(currentHash);
      if (next == null) {
        next = currentRing.firstEntry();
      }

      currentHash = next.getKey();

      if (++probes > MAX_PROBES) {
        log.warn("Exhausted {} probes in consistent hash ring for key '{}', all workers appear dead or ring is corrupt. Discarding this key.", MAX_PROBES, key);
        break;
      }
    } while (currentHash != startHash);

    return null;
  }

  /**
   * Return the set of live physical node IDs.
   *
   * @return an unmodifiable set of node IDs (never {@code null})
   */
  public Set<String> getNodes() {
    return currentState.liveNodes;
  }

  /**
   * Return whether the ring contains no virtual nodes (i.e. no live nodes).
   *
   * @return {@code true} if the ring is empty
   */
  public boolean isEmpty() {
    return currentState.ring.isEmpty();
  }

  /**
   * Return the number of live physical nodes in the ring.
   *
   * @return the node count (zero if no live nodes)
   */
  public int nodeCount() {
    return currentState.liveNodes.size();
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
}
