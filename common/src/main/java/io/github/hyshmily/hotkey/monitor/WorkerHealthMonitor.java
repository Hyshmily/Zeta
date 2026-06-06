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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Monitors the liveness of worker shards by tracking heartbeats.
 *
 * <p>Each shard reports heartbeats via {@link #onHeartbeat(int, long)}, and the monitor
 * determines whether any or a specific shard is alive based on a configurable timeout.
 * This class also exposes aggregate health metadata for diagnostics.
 */
public class WorkerHealthMonitor {

  private final Map<Integer, Long> lastHeartbeat = new ConcurrentHashMap<>();
  private final Map<Integer, Long> totalHeartbeats = new ConcurrentHashMap<>();
  private final Map<Integer, Long> firstHeartbeatAt = new ConcurrentHashMap<>();
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
   * Records a heartbeat for the given shard.
   *
   * @param shardIndex the shard that reported the heartbeat
   * @param timestamp  the heartbeat timestamp (epoch millis)
   */
  public void onHeartbeat(int shardIndex, long timestamp) {
    lastHeartbeat.put(shardIndex, timestamp);
    totalHeartbeats.merge(shardIndex, 1L, Long::sum);
    firstHeartbeatAt.putIfAbsent(shardIndex, timestamp);
  }

  /**
   * Returns whether at least one shard has sent a heartbeat within the timeout window.
   *
   * @return true if any shard is alive
   */
  public boolean isAnyWorkerAlive() {
    long now = System.currentTimeMillis();
    return lastHeartbeat
      .values()
      .stream()
      .anyMatch(ts -> now - ts < timeoutMs);
  }

  /**
   * Returns whether a specific shard is alive.
   *
   * @param shardIndex the shard to check
   * @return true if the shard has heartbeated within the timeout window
   */
  public boolean isAlive(int shardIndex) {
    Long ts = lastHeartbeat.get(shardIndex);
    return ts != null && System.currentTimeMillis() - ts < timeoutMs;
  }

  /**
   * Returns a snapshot of health metadata for all tracked shards.
   *
   * <p>The returned map is keyed by shard index; each value contains the shard's liveness
   * status, last heartbeat age, raw timestamp, total heartbeat count, and first heartbeat.
   *
   * @return ordered map of shard health info
   */
  public Map<Integer, Map<String, Object>> getWorkerHealth() {
    long now = System.currentTimeMillis();
    Map<Integer, Map<String, Object>> result = new LinkedHashMap<>();
    lastHeartbeat.forEach((shard, ts) -> {
      Map<String, Object> info = new LinkedHashMap<>();
      long age = now - ts;
      info.put("alive", age < timeoutMs);
      info.put("lastHeartbeatAgeMs", age);
      info.put("lastHeartbeatAt", ts);
      info.put("totalHeartbeats", totalHeartbeats.getOrDefault(shard, 0L));
      info.put("firstHeartbeatAt", firstHeartbeatAt.getOrDefault(shard, 0L));
      result.put(shard, info);
    });
    return result;
  }
}
