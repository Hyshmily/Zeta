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
package io.github.hyshmily.hotkey.sync;

import io.github.hyshmily.hotkey.logging.DefaultLogger;
import io.github.hyshmily.hotkey.logging.HotKeyLogger;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Cluster-wide Worker health view, updated by incoming heartbeats.
 *
 * <p>Tracks every known Worker's epoch, heartbeat timestamps, readyToServe
 * flag, and decisionVersionHwm. Provides majority-based health judgment
 * and detects Worker restarts via epoch changes.
 */
@RequiredArgsConstructor
public class ClusterHealthView {

  private static final HotKeyLogger log = new DefaultLogger(ClusterHealthView.class);

  private final ConcurrentMap<String, WorkerHealthRecord> records = new ConcurrentHashMap<>();
  private final int knownWorkerCount;
  private final long heartbeatTimeoutMs;
  private final int degradeAfterFailures;

  @Setter
  @Getter
  private volatile boolean degraded;

  @Getter
  private volatile long lastAnyHeartbeatTime;

  /**
   * Processes an incoming heartbeat from a Worker.
   *
   * <p>Creates a new record on first sighting or after an epoch change (Worker restart).
   * Updates the existing record with the latest heartbeat timestamp, decision version
   * watermark, load factor, and readiness flag for known Workers.
   * The {@code lastAnyHeartbeatTime} is updated unconditionally after every heartbeat.
   *
   * @param hb the incoming heartbeat message; must not be null
   */
  public void onHeartbeat(WorkerHeartbeatMessage hb) {
    records.compute(hb.workerId(), (id, existing) -> {
      long now = System.currentTimeMillis();

      if (existing == null || hb.epoch() > existing.epoch) {
        WorkerHealthRecord r = new WorkerHealthRecord();

        r.workerId = hb.workerId();
        r.epoch = hb.epoch();
        r.lastHeartbeatTime = now;
        r.firstHeartbeatTime = now;
        r.readyToServe = hb.readyToServe();
        return r;
      }

      if (hb.epoch() < existing.epoch) {
        return existing;
      }

      existing.lastHeartbeatTime = now;
      existing.decisionVersionHwm = Math.max(existing.decisionVersionHwm, hb.decisionVersionHwm());
      existing.loadFactor = hb.loadFactor();
      existing.readyToServe = hb.readyToServe();
      existing.stale = false;
      existing.restarted = false;
      return existing;
    });

    lastAnyHeartbeatTime = System.currentTimeMillis();
  }

  /**
   * Records a successful verification response (pong) from a Worker, resetting its
   * verification failure count and updating its heartbeat timestamp.
   * <p>
   * If the Worker ID is not present in the health view, this call is a no-op.
   *
   * @param workerId the Worker that responded; must not be null
   */
  public void recordPong(String workerId) {
    records.computeIfPresent(workerId, (id, r) -> {
      r.lastHeartbeatTime = System.currentTimeMillis();
      r.verifyFailures = 0;
      return r;
    });
  }

  /**
   * Increments the verification failure count for a Worker.
   *
   * <p>When the failure count reaches {@code degradeAfterFailures}, the Worker is
   * marked as stale ({@code stale = true}) and excluded from health checks
   * (i.e. {@link #isClusterHealthy()} and {@link #getAliveWorkerIds()} will ignore it).
   * <p>
   * If the Worker ID is not present in the health view, this call is a no-op.
   *
   * @param workerId the Worker that failed verification; must not be null
   */
  public void markVerificationFailed(String workerId) {
    records.computeIfPresent(workerId, (id, r) -> {
      r.verifyFailures++;
      if (r.verifyFailures >= degradeAfterFailures) {
        r.stale = true;
      }
      return r;
    });
  }

  /**
   * Returns whether the cluster is considered healthy.
   *
   * <p>The cluster is healthy when a majority (&#x3e; {@code knownWorkerCount / 2})
   * of known Workers are marked as ready and have sent a heartbeat within
   * {@code heartbeatTimeoutMs}.
   * <p>
   * When {@code knownWorkerCount == 0} (no Workers configured), the cluster is
   * always considered unhealthy.
   *
   * @return {@code true} if the majority of Workers are alive and ready;
   *         {@code false} if no Workers are configured or too few are alive
   */
  public boolean isClusterHealthy() {
    if (knownWorkerCount == 0) {
        return false;
    }
    long aliveCount = records
      .values()
      .stream()
      .filter(r -> r.isAlive(heartbeatTimeoutMs))
      .count();
    return aliveCount >= knownWorkerCount / 2 + 1;
  }

  /**
   * Returns the set of Worker IDs that are currently alive (ready and within heartbeat timeout).
   *
   * @return a set of alive Worker IDs, never {@code null}
   */
  public Set<String> getAliveWorkerIds() {
    return records
      .values()
      .stream()
      .filter(r -> r.isAlive(heartbeatTimeoutMs))
      .map(WorkerHealthRecord::getWorkerId)
      .collect(Collectors.toSet());
  }

  /**
   * Returns the set of all Worker IDs that have ever been seen by this health view.
   *
   * @return a set of all known Worker IDs, never {@code null}
   */
  public Set<String> getAllWorkerIds() {
    return records.values().stream().map(WorkerHealthRecord::getWorkerId).collect(Collectors.toSet());
  }

  @Getter
  public static class WorkerHealthRecord {

    String workerId;
    long epoch;
    long lastHeartbeatTime;
    long firstHeartbeatTime;
    long decisionVersionHwm;
    double loadFactor;
    boolean readyToServe;
    boolean stale;
    boolean restarted;
    int verifyFailures;

    /**
     * Returns whether this Worker is currently considered alive.
     * <p>
     * A Worker is alive when all of the following hold:
     * <ul>
     *   <li>{@code readyToServe} is {@code true} (Worker has completed startup)</li>
     *   <li>{@code stale} is {@code false} (Worker has not exceeded failure threshold)</li>
     *   <li>The elapsed time since {@code lastHeartbeatTime} is less than {@code timeoutMs}</li>
     * </ul>
     *
     * @param timeoutMs the heartbeat timeout in milliseconds
     * @return {@code true} if the Worker is ready, not stale, and has sent a heartbeat
     *         within the timeout window
     */
    public boolean isAlive(long timeoutMs) {
      return readyToServe && !stale && System.currentTimeMillis() - lastHeartbeatTime < timeoutMs;
    }
  }
}
