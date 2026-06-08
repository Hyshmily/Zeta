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

import lombok.Getter;

import java.util.HashSet;
import java.util.Set;

/**
 * Orchestrates the {@link ConsistentHashRing} with manual-override support.
 *
 * <p>Two modes:
 * <ul>
 *   <li><b>Auto</b> ({@code overrideNodes == null}): ring is rebuilt from heartbeat data
 *       each time {@link #reconcile(Set)} is called.</li>
 *   <li><b>Manual</b> ({@code overrideNodes != null}): ring is rebuilt from the override
 *       set; heartbeat changes are ignored until {@link #resetToAuto()} is called.</li>
 * </ul>
 *
 * <p>Thread-safe: all mutation is {@code synchronized}; ring reads are lock-free.
 */
public class RingManager {

  private final ConsistentHashRing ring;
  @Getter
  private final int virtualNodeCount;
  private volatile Set<String> overrideNodes;

  public RingManager(int virtualNodeCount) {
    this.virtualNodeCount = virtualNodeCount;
    this.ring = new ConsistentHashRing(virtualNodeCount);
  }

  /**
   * Rebuild the ring from heartbeat nodes (auto mode) or from the override set
   * (manual mode).  Safe to call on every flush — does nothing if the node set
   * hasn't changed.
   */
  public synchronized void reconcile(Set<String> heartbeatNodes) {
    Set<String> nodes = overrideNodes != null ? overrideNodes : heartbeatNodes;
    if (!nodes.equals(ring.getNodes())) {
      ring.rebuild(nodes);
    }
  }

  /**
   * Return the node responsible for the given key.
   * Lock-free read against the current ring.
   */
  public String getNode(String key) {
    return ring.getNode(key);
  }

  public synchronized void addNode(String nodeId) {
    if (overrideNodes == null) {
      overrideNodes = new HashSet<>(ring.getNodes());
    }
    overrideNodes.add(nodeId);
    ring.rebuild(overrideNodes);
  }

  public synchronized void removeNode(String nodeId) {
    if (overrideNodes == null) {
      overrideNodes = new HashSet<>(ring.getNodes());
    }
    overrideNodes.remove(nodeId);
    ring.rebuild(overrideNodes);
  }

  /** Switch back to auto mode — ring will be rebuilt from heartbeats on next reconcile. */
  public synchronized void resetToAuto() {
    overrideNodes = null;
  }


  public boolean isManualMode() {
    return overrideNodes != null;
  }

  public Set<String> getCurrentNodes() {
    return ring.getNodes();
  }

  public int nodeCount() {
    return ring.nodeCount();
  }
}
