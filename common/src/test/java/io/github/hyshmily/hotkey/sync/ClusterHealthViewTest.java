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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ClusterHealthViewTest {

  private ClusterHealthView view;

  @BeforeEach
  void setUp() {
    view = new ClusterHealthView(3, 5000, 2);
  }

  private static WorkerHeartbeatMessage hb(String workerId, long epoch, boolean ready) {
    return new WorkerHeartbeatMessage(workerId, epoch, System.currentTimeMillis(), 0, 0.0, ready, 0, 0, 0, 0, 0);
  }

  private static WorkerHeartbeatMessage hbWithHwm(String workerId, long epoch, boolean ready, long hwm) {
    return new WorkerHeartbeatMessage(workerId, epoch, System.currentTimeMillis(), hwm, 0.0, ready, 0, 0, 0, 0, 0);
  }

  // ── onHeartbeat ──

  @Test
  void shouldCreateRecordForNewWorker() {
    view.onHeartbeat(hb("w1", 1, true));
    assertThat(view.getAllWorkerIds()).containsExactly("w1");
  }

  @Test
  void shouldRejectHeartbeatWithLowerEpoch() {
    view.onHeartbeat(hb("w1", 2, true));
    view.onHeartbeat(hb("w1", 1, false));
    assertThat(view.getAliveWorkerIds()).contains("w1");
  }

  @Test
  void shouldAcceptHeartbeatWithSameEpoch() {
    view.onHeartbeat(hb("w1", 1, true));
    view.onHeartbeat(hb("w1", 1, false));
    assertThat(view.getAliveWorkerIds()).doesNotContain("w1");
  }

  @Test
  void shouldReplaceRecordOnHigherEpoch() {
    view.onHeartbeat(hb("w1", 1, true));
    view.onHeartbeat(hb("w1", 3, false));
    assertThat(view.getAliveWorkerIds()).doesNotContain("w1");
  }

  @Test
  void shouldUpdateReadyToServe() {
    view.onHeartbeat(hb("w1", 1, false));
    assertThat(view.getAliveWorkerIds()).doesNotContain("w1");
    view.onHeartbeat(hb("w1", 1, true));
    assertThat(view.getAliveWorkerIds()).contains("w1");
  }

  @Test
  void shouldUpdateLastAnyHeartbeatTime() {
    assertThat(view.getLastAnyHeartbeatTime()).isZero();
    view.onHeartbeat(hb("w1", 1, true));
    long t1 = view.getLastAnyHeartbeatTime();
    assertThat(t1).isPositive();
    view.onHeartbeat(hb("w2", 1, true));
    assertThat(view.getLastAnyHeartbeatTime()).isGreaterThanOrEqualTo(t1);
  }

  @Test
  void shouldResetStaleOnHeartbeat() {
    view.onHeartbeat(hb("w1", 1, true));
    view.markVerificationFailed("w1");
    view.markVerificationFailed("w1");
    assertThat(view.getAliveWorkerIds()).doesNotContain("w1");
    view.onHeartbeat(hb("w1", 1, true));
    assertThat(view.getAliveWorkerIds()).contains("w1");
  }

  // ── recordPong ──

  @Test
  void shouldResetVerifyFailuresOnPong() {
    view.onHeartbeat(hb("w1", 1, true));
    view.markVerificationFailed("w1");
    view.recordPong("w1");
    view.markVerificationFailed("w1");
    assertThat(view.getAliveWorkerIds()).contains("w1");
  }

  @Test
  void shouldIgnorePongForUnknownWorker() {
    view.recordPong("unknown");
  }

  // ── markVerificationFailed ──

  @Test
  void shouldMarkWorkerStaleAfterThresholdFailures() {
    view.onHeartbeat(hb("w1", 1, true));
    view.markVerificationFailed("w1");
    assertThat(view.getAliveWorkerIds()).contains("w1");
    view.markVerificationFailed("w1");
    assertThat(view.getAliveWorkerIds()).doesNotContain("w1");
  }

  @Test
  void shouldIgnoreMarkVerificationFailedForUnknownWorker() {
    view.markVerificationFailed("unknown");
  }

  // ── isClusterHealthy ──

  @Test
  void shouldReturnFalseWhenKnownWorkerCountIsZero() {
    ClusterHealthView emptyView = new ClusterHealthView(0, 5000, 2);
    assertThat(emptyView.isClusterHealthy()).isFalse();
  }

  @Test
  void shouldReturnTrueWhenMajorityIsAlive() {
    view.onHeartbeat(hb("w1", 1, true));
    view.onHeartbeat(hb("w2", 1, true));
    assertThat(view.isClusterHealthy()).isTrue();
  }

  @Test
  void shouldReturnFalseWhenOnlyMinorityIsAlive() {
    view.onHeartbeat(hb("w1", 1, true));
    assertThat(view.isClusterHealthy()).isFalse();
  }

  @Test
  void shouldExcludeStaleWorkersFromHealthCheck() {
    view.onHeartbeat(hb("w1", 1, true));
    view.onHeartbeat(hb("w2", 1, true));
    view.onHeartbeat(hb("w3", 1, true));
    view.markVerificationFailed("w1");
    view.markVerificationFailed("w1");
    assertThat(view.isClusterHealthy()).isTrue();
    view.markVerificationFailed("w2");
    view.markVerificationFailed("w2");
    assertThat(view.isClusterHealthy()).isFalse();
  }

  // ── getAliveWorkerIds ──

  @Test
  void shouldReturnOnlyAliveWorkers() {
    view.onHeartbeat(hb("w1", 1, true));
    view.onHeartbeat(hb("w2", 1, false));
    view.onHeartbeat(hb("w3", 1, true));
    view.markVerificationFailed("w3");
    view.markVerificationFailed("w3");
    assertThat(view.getAliveWorkerIds()).containsExactly("w1");
  }

  // ── getAllWorkerIds ──

  @Test
  void shouldReturnAllKnownWorkers() {
    view.onHeartbeat(hb("w1", 1, true));
    view.onHeartbeat(hb("w2", 1, false));
    assertThat(view.getAllWorkerIds()).containsExactlyInAnyOrder("w1", "w2");
  }

  @Test
  void shouldReturnEmptyWhenNoWorkers() {
    assertThat(view.getAllWorkerIds()).isEmpty();
  }

  // ── WorkerHealthRecord.isAlive ──

  @Test
  void isAliveShouldReturnTrueWhenReadyAndNotStaleAndWithinTimeout() {
    ClusterHealthView.WorkerHealthRecord r = new ClusterHealthView.WorkerHealthRecord();
    r.readyToServe = true;
    r.stale = false;
    r.lastHeartbeatTime = System.currentTimeMillis();
    assertThat(r.isAlive(5000)).isTrue();
  }

  @Test
  void isAliveShouldReturnFalseWhenNotReady() {
    ClusterHealthView.WorkerHealthRecord r = new ClusterHealthView.WorkerHealthRecord();
    r.readyToServe = false;
    r.stale = false;
    r.lastHeartbeatTime = System.currentTimeMillis();
    assertThat(r.isAlive(5000)).isFalse();
  }

  @Test
  void isAliveShouldReturnFalseWhenStale() {
    ClusterHealthView.WorkerHealthRecord r = new ClusterHealthView.WorkerHealthRecord();
    r.readyToServe = true;
    r.stale = true;
    r.lastHeartbeatTime = System.currentTimeMillis();
    assertThat(r.isAlive(5000)).isFalse();
  }

  @Test
  void isAliveShouldReturnFalseWhenTimedOut() {
    ClusterHealthView.WorkerHealthRecord r = new ClusterHealthView.WorkerHealthRecord();
    r.readyToServe = true;
    r.stale = false;
    r.lastHeartbeatTime = System.currentTimeMillis() - 100;
    assertThat(r.isAlive(50)).isFalse();
  }

  // ── Concurrent ──

  @Test
  void shouldHandleConcurrentHeartbeats() throws Exception {
    int threadCount = 5;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);
    for (int i = 0; i < threadCount; i++) {
      String wid = "w" + i;
      executor.submit(() -> {
        view.onHeartbeat(hb(wid, 1, true));
        latch.countDown();
      });
    }
    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(view.getAllWorkerIds()).hasSize(threadCount);
    executor.shutdown();
  }

  @Test
  void shouldHandleConcurrentHeartbeatsToSameWorker() throws Exception {
    int threadCount = 5;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);
    for (int i = 0; i < threadCount; i++) {
      executor.submit(() -> {
        view.onHeartbeat(hb("w1", 1, true));
        latch.countDown();
      });
    }
    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(view.getAllWorkerIds()).containsExactly("w1");
    executor.shutdown();
  }

  /**
   * Verifies that onHeartbeat clears the degraded flag when cluster recovers.
   */
  @Test
  void onHeartbeat_withDegradedFlag_shouldRecover() {
    view.setDegraded(true);
    view.onHeartbeat(hb("w1", 1, true));
    view.onHeartbeat(hb("w2", 1, true));
    assertThat(view.isDegraded()).isFalse();
  }

  /**
   * Verifies that recordPong clears the degraded flag when cluster recovers.
   */
  @Test
  void recordPong_withDegradedFlag_shouldRecover() {
    view.onHeartbeat(hb("w1", 1, true));
    view.onHeartbeat(hb("w2", 1, true));
    view.setDegraded(true);
    view.recordPong("w3");
    assertThat(view.isDegraded()).isFalse();
  }

  /**
   * Verifies that onHeartbeat updates the decision version HWM to the max.
   */
  @Test
  void onHeartbeat_shouldUpdateDecisionVersionHwm() {
    view.onHeartbeat(hbWithHwm("w1", 1, true, 10));
    view.onHeartbeat(hbWithHwm("w1", 1, true, 20));
    view.onHeartbeat(hbWithHwm("w1", 1, true, 5));
    assertThat(view.getAliveWorkerIds()).contains("w1");
  }

  /**
   * Verifies that an epoch change resets the record including decisionVersionHwm.
   */
  @Test
  void onHeartbeat_higherEpoch_shouldReplaceRecord() {
    view.onHeartbeat(hbWithHwm("w1", 1, true, 100));
    view.onHeartbeat(hbWithHwm("w1", 2, false, 0));
    assertThat(view.getAliveWorkerIds()).doesNotContain("w1");
  }

  /**
   * Verifies that isAlive returns false when exactly at the timeout boundary (elapsed == timeoutMs).
   */
  @Test
  void isAlive_exactTimeoutBoundary_shouldReturnFalse() {
    ClusterHealthView.WorkerHealthRecord r = new ClusterHealthView.WorkerHealthRecord();
    r.readyToServe = true;
    r.stale = false;
    r.lastHeartbeatTime = System.currentTimeMillis() - 5000;
    assertThat(r.isAlive(5000)).isFalse();
  }

  /**
   * Verifies that knownWorkerCount of 0 excludes all workers from healthy check.
   */
  @Test
  void isClusterHealthy_withZeroKnownWorkers_shouldReturnFalseEvenWithRecords() {
    ClusterHealthView empty = new ClusterHealthView(0, 5000, 2);
    empty.onHeartbeat(hb("w1", 1, true));
    empty.onHeartbeat(hb("w2", 1, true));
    assertThat(empty.isClusterHealthy()).isFalse();
  }

  /**
   * Verifies that isClusterHealthy with exactly majority alive returns true.
   */
  @Test
  void isClusterHealthy_exactMajority_shouldReturnTrue() {
    view.onHeartbeat(hb("w1", 1, true));
    view.onHeartbeat(hb("w2", 1, true));
    assertThat(view.isClusterHealthy()).isTrue();
  }

  /** An alternative to the private hbWithHwm method in this file. */
  @Test
  void shouldHandleHeartbeatForSingleWorker() {
    view.onHeartbeat(hb("w1", 1, true));
    assertThat(view.getAliveWorkerIds()).containsExactly("w1");
    assertThat(view.getAllWorkerIds()).containsExactly("w1");
  }

  // ── Integration ──

  @Test
  void shouldChainHeartbeatVerificationAndHealthCheck() {
    view.onHeartbeat(hb("w1", 1, true));
    view.onHeartbeat(hb("w2", 1, true));
    view.onHeartbeat(hb("w3", 1, true));
    assertThat(view.isClusterHealthy()).isTrue();
    view.markVerificationFailed("w1");
    view.markVerificationFailed("w1");
    assertThat(view.isClusterHealthy()).isTrue();
    view.markVerificationFailed("w2");
    view.markVerificationFailed("w2");
    assertThat(view.isClusterHealthy()).isFalse();
    view.recordPong("w1");
    view.onHeartbeat(hb("w1", 1, true));
    assertThat(view.getAliveWorkerIds()).contains("w1");
    assertThat(view.isClusterHealthy()).isTrue();
  }
}
