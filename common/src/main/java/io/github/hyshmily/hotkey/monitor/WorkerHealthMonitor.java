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
package io.github.hyshmily.hotkey.monitor;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Monitors Worker node liveness by tracking heartbeat pings.
 *
 * <p>Each Worker node reports heartbeats via {@link #onHeartbeat(String, long)}, and the
 * monitor determines whether any or a specific node is alive based on a configurable
 * timeout.  This class also exposes aggregate health metadata for diagnostics.
 */
public class WorkerHealthMonitor {

  /** Latest heartbeat timestamp per node (consistent-hashing mode). */
  private final Map<String, Long> nodeLastHeartbeat = new ConcurrentHashMap<>();
  /** Cumulative heartbeat count per node (consistent-hashing mode). */
  private final Map<String, Long> nodeTotalHeartbeats = new ConcurrentHashMap<>();
  /** Heartbeat expiry threshold in milliseconds. */
  private final long timeoutMs;

  /** Creates a monitor with the default 5-second timeout. */
  public WorkerHealthMonitor() {
    this(5_000);
  }

  /**
   * Creates a monitor with a custom timeout.
   *
   * @param timeoutMs heartbeat expiry threshold in milliseconds
   */
  public WorkerHealthMonitor(long timeoutMs) {
    this.timeoutMs = timeoutMs;
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
   * Returns whether a specific Worker node is alive.
   *
   * @param nodeId the Worker node identifier
   * @return true if the node has heartbeated within the timeout window
   */
  public boolean isAlive(String nodeId) {
    Long ts = nodeLastHeartbeat.get(nodeId);
    return ts != null && System.currentTimeMillis() - ts < timeoutMs;
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
}
