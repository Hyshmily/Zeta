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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;

/**
 * Orchestrates the {@link ConsistentHashRing} with manual-override support
 * and Worker node liveness tracking.
 *
 * <p>Two modes:
 * <ul>
 *   <li><b>Auto</b> ({@code overrideNodes == null}): ring is rebuilt from heartbeat data
 *       each time {@link #reconcile()} is called.</li>
 *   <li><b>Manual</b> ({@code overrideNodes != null}): ring is rebuilt from the override
 *       set; heartbeat changes are ignored until {@link #resetToAuto()} is called.</li>
 * </ul>
 *
 * <p>Heartbeat tracking:
 * <ul>
 *   <li>{@link #onHeartbeat(String, long)} records Worker node heartbeats.</li>
 *   <li>{@link #isAnyWorkerAlive()} / {@link #isAlive(String)} check liveness.</li>
 *   <li>{@link #reconcile()} uses the alive node set to rebuild the hash ring.</li>
 * </ul>
 *
 * <p>Thread-safe: all mutation is {@code synchronized}; ring reads are lock-free.
 */
public class RingManager {

  /** The underlying consistent hash ring; reads are lock-free. */
  @Getter
  private final ConsistentHashRing ring;

  @Getter
  /* Virtual-node replication factor per physical node. */
  private final int virtualNodeCount;

  /** Latest heartbeat timestamp per node. */
  private final Map<String, Long> nodeLastHeartbeat = new ConcurrentHashMap<>();

  /** Cumulative heartbeat count per node. */
  private final Map<String, Long> nodeTotalHeartbeats = new ConcurrentHashMap<>();

  /** Heartbeat expiry threshold in milliseconds. */
  private final long timeoutMs;

  /** Manual node override set; {@code null} when in auto mode. */
  private volatile Set<String> overrideNodes;

  /**
   * Create a RingManager with the given virtual-node replication factor
   * and default 5-second heartbeat timeout.
   *
   * @param virtualNodeCount virtual copies per physical node on the ring
   */
  public RingManager(int virtualNodeCount) {
    this(virtualNodeCount, 5_000);
  }

  /**
   * Create a RingManager with the given virtual-node replication factor
   * and custom heartbeat timeout.
   *
   * @param virtualNodeCount virtual copies per physical node on the ring
   * @param timeoutMs        heartbeat expiry threshold in milliseconds
   */
  public RingManager(int virtualNodeCount, long timeoutMs) {
    this.virtualNodeCount = virtualNodeCount;
    this.timeoutMs = timeoutMs;
    this.ring = new ConsistentHashRing(virtualNodeCount);
  }

  /**
   * Records a heartbeat for the given Worker nodeId.
   *
   * @param nodeId    the Worker node identifier
   * @param timestamp the heartbeat timestamp (epoch millis)
   */
  public void onHeartbeat(String nodeId, long timestamp) {
    nodeLastHeartbeat.put(nodeId, timestamp);
    nodeTotalHeartbeats.merge(nodeId, 1L, Long::sum);
  }

  /**
   * Returns whether at least one node has sent a heartbeat within the timeout window.
   *
   * @return true if any worker is alive
   */
  public boolean isAnyWorkerAlive() {
    long now = System.currentTimeMillis();

    return nodeLastHeartbeat
      .values()
      .stream()
      .anyMatch(ts -> now - ts < timeoutMs);
  }

  /**
   * Return the alive node responsible for the given key.
   * Lock-free read against the current ring; dead nodes are skipped.
   *
   * @param key the cache key to route
   * @return the physical node ID, or {@code null} if the ring is empty or no node is alive
   */
  public String routeNode(String key) {
    return ring.locateNode(key, this::isAlive);
  }

  /**
   * Returns the set of alive Worker node IDs.
   *
   * @return set of nodeIds that have heartbeated within the timeout window
   */
  public Set<String> getAliveNodeIds() {
    long now = System.currentTimeMillis();
    Set<String> alive = new HashSet<>();
    nodeLastHeartbeat.forEach((nodeId, ts) -> {
      if (now - ts < timeoutMs) {
        alive.add(nodeId);
      }
    });
    return alive;
  }

  /**
   * Returns whether a specific Worker node is alive.
   *
   * <p>Nodes with no heartbeat record (e.g. manually added) are considered alive.
   *
   * @param physicalNodeId the Worker node identifier
   * @return true if the node has heartbeated within the timeout window, or if no heartbeat has been recorded
   */
  public boolean isAlive(String physicalNodeId) {
    Long ts = nodeLastHeartbeat.get(physicalNodeId);
    return ts == null || System.currentTimeMillis() - ts < timeoutMs;
  }

  /**
   * Returns a snapshot of health metadata for all tracked Worker nodes.
   *
   * @return map containing alive node IDs and per-node heartbeat counts
   */
  public Map<String, Object> getWorkerHealth() {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("aliveNodes", getAliveNodeIds());
    result.put("nodeTotalHeartbeats", new LinkedHashMap<>(nodeTotalHeartbeats));
    return result;
  }

  /**
   * Rebuild the ring from live heartbeat nodes (auto mode) or from the override set
   * (manual mode).  Safe to call on every flush — does nothing if the node set
   * hasn't changed.
   */
  public synchronized void reconcile() {
    Set<String> nodes = overrideNodes != null ? overrideNodes : getAliveNodeIds();
    if (!nodes.equals(ring.getNodes())) {
      ring.rebuild(nodes);
    }
  }

  /**
   * Add a physical node to the ring.  Switches to manual mode if not already active.
   *
   * @param nodeId the ID of the node to add
   */
  public synchronized void addNode(String nodeId) {
    if (overrideNodes == null) {
      overrideNodes = new HashSet<>(ring.getNodes());
    }
    overrideNodes.add(nodeId);
    ring.rebuild(overrideNodes);
  }

  /**
   * Remove a physical node from the ring.  Switches to manual mode if not already active.
   *
   * @param nodeId the ID of the node to remove
   */
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

  /**
   * Return whether the ring is in manual override mode.
   *
   * @return {@code true} if manual mode is active
   */
  public boolean isManualMode() {
    return overrideNodes != null;
  }

  /**
   * Return the current set of live (or manually overridden) node IDs.
   *
   * @return an unmodifiable set of node IDs
   */
  public Set<String> getCurrentNodes() {
    return ring.getNodes();
  }

  /**
   * Return the number of physical nodes currently in the ring.
   *
   * @return the node count
   */
  public int nodeCount() {
    return ring.nodeCount();
  }
}
