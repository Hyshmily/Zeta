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

import java.util.Set;
import java.util.function.IntConsumer;
import lombok.Getter;
import lombok.Setter;

/**
 * Manages the consistent-hash ring for Worker shard routing.
 *
 * <p>The ring is rebuilt automatically from the live Worker set reported
 * by {@link ClusterHealthView} on each reconciliation cycle.
 */
public class RingManager {

  @Getter
  private final ConsistentHashRing ring;

  @Getter
  private final int virtualNodeCount;

  @Setter
  private IntConsumer onRingReconciled;

  /**
   * Creates a ring manager with the given virtual-node count.
   *
   * @param virtualNodeCount virtual copies per physical shard on the ring
   */
  public RingManager(int virtualNodeCount) {
    this.virtualNodeCount = virtualNodeCount;
    this.ring = new ConsistentHashRing(virtualNodeCount);
  }

  /**
   * Rebuild the ring from the current cluster health view.
   *
   * @param healthView the current cluster health view; must not be {@code null}
   * @throws NullPointerException if {@code healthView} is {@code null}
   */
  public synchronized void reconcileFromHealthView(ClusterHealthView healthView) {
    Set<String> alive = healthView.getAliveWorkerIds();
    if (!alive.equals(ring.getNodes())) {
      ring.rebuild(alive);
      if (onRingReconciled != null) {
        onRingReconciled.accept(alive.size());
      }
    }
  }

  /**
   * Return the current set of nodes on the ring.
   *
   * @return the set of live node identifiers
   */
  public Set<String> getCurrentNodes() {
    return ring.getNodes();
  }

  /**
   * Return the number of physical nodes currently on the ring.
   *
   * @return the node count
   */
  public int nodeCount() {
    return ring.nodeCount();
  }

  /**
   * Route a key to the responsible Worker node.
   *
   * @param key        the cache key to route; must not be {@code null}
   * @param healthView the current cluster health view; must not be {@code null}
   * @return the node identifier that owns the key, or {@code null} if no node is available
   * @throws NullPointerException if {@code key} or {@code healthView} is {@code null}
   */
  public String routeNode(String key, ClusterHealthView healthView) {
    Set<String> alive = healthView.getAliveWorkerIds();
    return ring.locateNode(key, alive::contains);
  }

  /**
   * Route a key to its target Worker node, using a pre-snapshotted alive-set
   * supplied by the caller. This avoids re-fetching {@link
   * ClusterHealthView#getAliveWorkerIds()} per key inside hot loops.
   *
   * @param key        the cache key to route
   * @param aliveNodes the already-snapshotted set of alive Worker ids; must not
   *                   be {@code null} or modified concurrently
   * @return the target Worker node id, or {@code null} if no alive nodes
   */
  public String routeNode(String key, Set<String> aliveNodes) {
    if (aliveNodes == null || aliveNodes.isEmpty()) {
      return null;
    }
    return ring.locateNode(key, aliveNodes::contains);
  }
}
