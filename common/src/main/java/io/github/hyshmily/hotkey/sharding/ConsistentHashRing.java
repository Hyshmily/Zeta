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
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Consistent hash ring backed by a {@link TreeMap}.
 *
 * <p>Each physical node is replicated across the ring with {@code virtualNodeCount}
 * virtual nodes, all mapped to the same physical {@code nodeId}.  The entire ring
 * is rebuilt atomically via {@link #rebuild(Set)} — no partial mutations, safe for
 * lock-free reads on the hot path.
 */
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

  /**
   * @param virtualNodeCount virtual copies per physical node
   */
  public ConsistentHashRing(int virtualNodeCount) {
    this.virtualNodeCount = virtualNodeCount;
  }

  /**
   * Atomically replace the ring with one built from the given live nodes.
   * Each node is replicated {@code virtualNodeCount} times on the ring.
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
   * Return the node responsible for the given key, or {@code null} if the ring is empty.
   */
  public String getNode(String key) {
    RingState state = currentState;
    if (state.ring.isEmpty()) {
      return null;
    }

    int h = hash(key);
    Map.Entry<Integer, String> entry = state.ring.ceilingEntry(h);

    return (entry != null ? entry : state.ring.firstEntry()).getValue();
  }

  /**
   * Return the set of live physical node IDs.
   *
   * @return an unmodifiable set of node IDs
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
   * @return the node count
   */
  public int nodeCount() {
    return currentState.liveNodes.size();
  }

  /**
   * Compute a 32-bit Murmur3 hash for the given string.
   *
   * @param key the string to hash
   * @return the hash value
   */
  private static int hash(String key) {
    return Hashing.murmur3_32_fixed().hashString(key, StandardCharsets.UTF_8).asInt();
  }
}
