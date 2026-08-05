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
  /** Get the timestamp of the last heartbeat from any Worker. */
  long getLastAnyHeartbeatTime();

  /** Record a heartbeat from a Worker. */
  void onHeartbeat(WorkerHeartbeatMessage hb);

  /** Record a pong response from a Worker. */
  void recordPong(String workerId);

  /** Mark a Worker as having failed verification. */
  void markVerificationFailed(String workerId);

  /** Get the verification failure count for a Worker. */
  int getVerifyFailures(String workerId);

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
   * Whether the given Worker is currently considered alive (ready, not stale,
   * and within the heartbeat timeout) — the same liveness judgment the ring
   * and report path use. Unknown Workers are never alive.
   *
   * @param workerId the Worker node ID
   * @return {@code true} if the Worker is alive
   */
  boolean isAlive(String workerId);

  /**
   * The current epoch (restart incarnation) of the given Worker, or
   * {@link #UNKNOWN_EPOCH} when the Worker has never been seen (or its record
   * was removed after confirmation of death).
   *
   * @param workerId the Worker node ID
   * @return the Worker's current epoch, or {@link #UNKNOWN_EPOCH}
   */
  long epochOf(String workerId);

  /** Sentinel returned by {@link #epochOf} for Workers with no health record. */
  long UNKNOWN_EPOCH = -1L;
}
