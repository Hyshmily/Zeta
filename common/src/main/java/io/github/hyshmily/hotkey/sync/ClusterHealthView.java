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

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Cluster-wide view of Worker health, maintained by consuming periodic
 * {@link WorkerHeartbeatMessage} broadcasts from all Worker nodes.
 *
 * <p>This is the central health authority for the local app instance. It tracks
 * every known Worker by ID with its epoch (restart counter), heartbeat timestamps,
 * readiness flag, decision version high-water mark, and verification failure count.
 *
 * <p><b>Health judgment:</b> The cluster is considered healthy when a strict majority
 * ({@code > knownWorkerCount / 2}) of Workers are alive (ready, not stale, and within
 * the heartbeat timeout window). When the cluster becomes unhealthy, the system may
 * enter graceful degradation mode (see ADR-0009).
 *
 * <p><b>Restart detection:</b> When a Worker's epoch in a new heartbeat exceeds the
 * stored epoch, a new {@link WorkerHealthRecord} is created — all state from the
 * previous incarnation is discarded. This prevents stale health state from surviving
 * a Worker restart.
 *
 * <p><b>Thread safety:</b> All record mutations use {@link ConcurrentHashMap#compute}
 * and {@code computeIfPresent} for atomic per-Worker updates. The {@code degraded}
 * flag and {@code lastAnyHeartbeatTime} are {@code volatile} fields safe for
 * concurrent read/write.
 *
 * @see WorkerHeartbeatMessage
 * @see WorkerHeartbeatVerifier
 */
@RequiredArgsConstructor
@Slf4j
public class ClusterHealthView {

  /** Per-Worker health records keyed by Worker ID. Thread-safe via {@link ConcurrentHashMap}. */
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
   * Processes an incoming {@link WorkerHeartbeatMessage} from a Worker node and
   * updates the cluster health state accordingly.
   *
   * <p><b>New or restarted Worker:</b> If this is the first heartbeat from a Worker,
   * or if the heartbeat epoch exceeds the stored epoch (indicating a Worker restart),
   * a new {@link WorkerHealthRecord} is created with fresh timestamps and the old
   * decision watermark is discarded.
   *
   * <p><b>Known Worker:</b> Updates the last heartbeat timestamp, decision version
   * watermark (taking the max), load factor, and readiness flag. Clears any stale
   * or restarted flags.
   *
   * <p><b>Cluster recovery:</b> If the cluster was in degraded state and this heartbeat
   * brings the majority back to health, the degraded flag is automatically cleared.
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
    if (degraded && isClusterHealthy()) {
      log.info(
        "Cluster has recovered from degraded state. Alive workers: {}/{}, clearing degraded flag.",
        getAliveWorkerIds().size(),
        knownWorkerCount
      );
      this.degraded = false;
    }
  }

  /**
   * Records a successful PONG response from a Worker during active verification
   * ({@link WorkerHeartbeatVerifier#verifySuspectedWorkers}).
   *
   * <p>Resets the Worker's verification failure count to zero and updates its
   * heartbeat timestamp to now, effectively restoring it to the alive set.
   * If the cluster is currently degraded and this PONG brings it back to health,
   * the degraded flag is cleared.
   *
   * @param workerId the Worker that responded with a PONG; must not be null
   */
  public void recordPong(String workerId) {
    records.computeIfPresent(workerId, (id, r) -> {
      r.lastHeartbeatTime = System.currentTimeMillis();
      r.verifyFailures = 0;
      return r;
    });

    if (degraded && isClusterHealthy()) {
      log.info(
        "Cluster has recovered from degraded state via pong. Alive workers: {}/{}, clearing degraded flag.",
        getAliveWorkerIds().size(),
        knownWorkerCount
      );
      this.degraded = false;
    }
  }

  /**
   * Increments the verification failure count for a Worker and marks it stale
   * if the threshold is reached.
   *
   * <p>When the cumulative failure count reaches {@code degradeAfterFailures},
   * the Worker is marked as stale ({@code stale = true}). Stale Workers are
   * excluded from {@link #isClusterHealthy()} and {@link #getAliveWorkerIds()}
   * — they are effectively considered dead even if they were previously alive.
   *
   * <p>If the Worker ID is not present in the health view (never seen, or already
   * removed), this call is a no-op.
   *
   * @param workerId the Worker that failed active verification; must not be null
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
   * Returns whether the cluster is currently considered healthy based on majority
   * of known Workers being alive.
   *
   * <p>A Worker is considered alive when all three conditions hold:
   * <ul>
   *   <li>{@code readyToServe == true} (Worker completed initialization)</li>
   *   <li>{@code stale == false} (Worker has not exceeded verification failure threshold)</li>
   *   <li>Time since last heartbeat {@code < heartbeatTimeoutMs}</li>
   * </ul>
   *
   * <p>The cluster is healthy when the count of alive Workers is strictly greater
   * than {@code knownWorkerCount / 2} (simple majority).
   *
   * <p>When {@code knownWorkerCount == 0} (no Workers configured), the cluster is
   * always considered unhealthy. This prevents undefined behaviour in deployments
   * without a Worker cluster.
   *
   * @return {@code true} if a strict majority of known Workers are alive and ready;
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

  /**
   * Per-Worker health state tracked within the cluster health view.
   *
   * <p>Each record captures the Worker's current epoch, heartbeat timing,
   * readiness, load, and verification failure state. Records are created
   * on first heartbeat and updated (or replaced on epoch change) via
   * atomic {@code ConcurrentHashMap.compute} operations.
   *
   * <p>A Worker transitions through these states:
   * <ul>
   *   <li><b>Startup:</b> {@code readyToServe = false} until first detection cycle completes</li>
   *   <li><b>Healthy:</b> {@code readyToServe = true, stale = false}, heartbeats arriving within timeout</li>
   *   <li><b>Suspected:</b> Heartbeats timeout; {@link WorkerHeartbeatVerifier} probes actively</li>
   *   <li><b>Stale:</b> Verification failures reach threshold; excluded from health majority</li>
   *   <li><b>Restarted:</b> New epoch detected; old record replaced entirely</li>
   * </ul>
   */
  @Getter
  public static class WorkerHealthRecord {

    volatile String workerId;
    volatile long epoch;
    volatile long lastHeartbeatTime;
    volatile long firstHeartbeatTime;
    volatile long decisionVersionHwm;
    volatile double loadFactor;
    volatile boolean readyToServe;
    volatile boolean stale;
    volatile boolean restarted;
    volatile int verifyFailures;

    /**
     * Returns whether this Worker is currently considered alive for health-majority
     * calculations.
     *
     * <p>All three conditions must hold:
     * <ul>
     *   <li>{@link #readyToServe} is {@code true} — the Worker has completed its
     *       initial detection cycle and is accepting requests</li>
     *   <li>{@link #stale} is {@code false} — the Worker has not exceeded the
     *       configured verification failure threshold</li>
     *   <li>The elapsed wall-clock time since {@link #lastHeartbeatTime} is less
     *       than {@code timeoutMs} — heartbeats are arriving within the expected
     *       interval</li>
     * </ul>
     *
     * @param timeoutMs the heartbeat timeout window in milliseconds; must be positive
     * @return {@code true} if this Worker is ready, not stale, and has sent a heartbeat
     *         within the timeout window
     */
    public boolean isAlive(long timeoutMs) {
      return readyToServe && !stale && System.currentTimeMillis() - lastHeartbeatTime < timeoutMs;
    }
  }
}
