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
package io.github.hyshmily.zeta.sharding;

import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatMessage;
import java.util.Set;

/**
 * Cluster-wide view of Worker health, maintained by consuming periodic
 * {@link WorkerHeartbeatMessage} broadcasts from all Worker nodes.
 */
public interface HealthView {
  /** Sentinel returned by {@link #epochOf} for Workers with no health record. */
  long UNKNOWN_EPOCH = -1L;

  /**
   * Get the {@link io.github.hyshmily.zeta.util.TimeSource#monotonicMillis()
   * monotonic} timestamp (milliseconds since boot, not wall-clock) of the last
   * heartbeat received from any Worker; {@code 0} when none has arrived yet.
   */
  long getLastAnyHeartbeatTime();

  /** Record a heartbeat from a Worker. */
  void onHeartbeat(WorkerHeartbeatMessage hb);

  /** Remove all records for a Worker. */
  void removeRecord(String workerId);

  /**
   * Set the minimum number of alive workers for cluster health.
   * When {@code <= 0}, the threshold is one third of the observed Worker count
   * (rounded up, minimum 1) — see {@link #isClusterHealthy()}.
   */
  void setMinAliveWorkers(int minAliveWorkers);

  /**
   * Whether the cluster is considered healthy (at least the minimum alive threshold;
   * by default one third of observed Workers, minimum 1).
   */
  boolean isClusterHealthy();

  /** Get the IDs of Workers currently considered alive. */
  Set<String> getAliveWorkerIds();

  /** Get the IDs of all known Workers. */
  Set<String> getAllWorkerIds();

  /**
   * Whether the given Worker is currently considered alive (ready and inside its
   * effective liveness window) — the same liveness judgment the ring
   * and report path use. Unknown Workers are never alive.
   *
   * @param workerId the Worker node ID
   * @return {@code true} if the Worker is alive
   */
  boolean isAlive(String workerId);

  /**
   * Timestamp-threading form of {@link #isAlive(String)}: judges the Worker against a
   * caller-supplied {@link io.github.hyshmily.zeta.util.TimeSource#monotonicMillis()
   * monotonic} instant. A walk over several Workers samples the clock once instead of
   * once per probe. Implementations without a cheaper form delegate to
   * {@link #isAlive(String)}.
   *
   * @param workerId the Worker node ID
   * @param now      the monotonic instant to judge against
   * @return {@code true} if the Worker is alive as of {@code now}
   */
  default boolean isAlive(String workerId, long now) {
    return isAlive(workerId);
  }

  /**
   * The current epoch (restart incarnation) of the given Worker, or
   * {@link #UNKNOWN_EPOCH} when the Worker has never been seen (or its record
   * was removed after confirmation of death).
   *
   * @param workerId the Worker node ID
   * @return the Worker's current epoch, or {@link #UNKNOWN_EPOCH}
   */
  long epochOf(String workerId);

  /**
   * Whether the given Worker is <b>the current incarnation</b> and alive:
   * a health record exists, its epoch equals {@code epoch} (no restart since
   * the stamp was issued), and the Worker passes the same liveness judgment
   * as {@link #isAlive(String)}.
   *
   * <p>This is the fused Decision-Validity probe (design decisions: hot state) for the
   * Worker-HOT read path: one map lookup answers "alive AND same
   * incarnation" instead of the two lookups {@code isAlive(id) &&
   * epochOf(id) == epoch} needs. Implementations must keep the two forms
   * observationally equivalent.
   *
   * @param workerId the Worker node ID that issued the stamp (never {@code null})
   * @param epoch    the epoch captured in the stamp
   * @return {@code true} if the Worker is alive and has not restarted since
   */
  default boolean isCurrentIncarnation(String workerId, long epoch) {
    return isAlive(workerId) && epochOf(workerId) == epoch;
  }
}
