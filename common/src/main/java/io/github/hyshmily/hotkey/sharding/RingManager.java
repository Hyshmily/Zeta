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

import io.github.hyshmily.hotkey.sync.ClusterHealthView;
import java.util.HashSet;
import java.util.Set;
import lombok.Getter;

/**
 * Manages the consistent-hash ring for Worker shard routing.
 *
 * <p>Operates in two modes:
 * <ul>
 *   <li><b>Auto mode</b> (default) — the ring is rebuilt automatically from
 *       the live Worker set reported by {@link ClusterHealthView}.</li>
 *   <li><b>Manual mode</b> — an operator has pinned the ring to a specific
 *       set of nodes via {@link #addNode} / {@link #removeNode}.</li>
 * </ul>
 */
public class RingManager {

  @Getter
  private final ConsistentHashRing ring;

  @Getter
  private final int virtualNodeCount;

  private volatile Set<String> overrideNodes; // null=auto mode

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
   * Rebuild the ring from the health view (auto mode) or the override set (manual mode).
   *
   * @param healthView the current cluster health view
   */
  public synchronized void reconcileFromHealthView(ClusterHealthView healthView) {
    if (overrideNodes != null) {
      if (!overrideNodes.equals(ring.getNodes())) {
        ring.rebuild(overrideNodes);
      }
      return;
    }
    Set<String> alive = healthView.getAliveWorkerIds();
    if (!alive.equals(ring.getNodes())) {
      ring.rebuild(alive);
    }
  }

  /**
   * Add a physical node to the ring. Switches to manual mode.
   *
   * @param nodeId the node identifier to add
   */
  public synchronized void addNode(String nodeId) {
    if (overrideNodes == null) {
      overrideNodes = new HashSet<>(ring.getNodes());
    }
    overrideNodes.add(nodeId);
    ring.rebuild(overrideNodes);
  }

  /**
   * Remove a physical node from the ring. Switches to manual mode.
   *
   * @param nodeId the node identifier to remove
   */
  public synchronized void removeNode(String nodeId) {
    if (overrideNodes == null) {
      overrideNodes = new HashSet<>(ring.getNodes());
    }
    overrideNodes.remove(nodeId);
    ring.rebuild(overrideNodes);
  }

  /**
   * Reset to auto mode — the ring will be rebuilt from the health view on next reconciliation.
   */
  public synchronized void resetToAuto() {
    overrideNodes = null;
  }

  /**
   * Whether the ring is in manual mode (operator-pinned node set).
   *
   * @return {@code true} if in manual mode
   */
  public boolean isManualMode() {
    return overrideNodes != null;
  }

  /**
   * Return the current set of nodes on the ring.
   *
   * @return the set of live (or pinned) node identifiers
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
   * @param key        the cache key to route
   * @param healthView the current cluster health view
   * @return the node identifier that owns the key, or {@code null} if no node is available
   */
  public String routeNode(String key, ClusterHealthView healthView) {
    Set<String> alive = healthView.getAliveWorkerIds();
    return ring.locateNode(key, alive::contains);
  }
}
