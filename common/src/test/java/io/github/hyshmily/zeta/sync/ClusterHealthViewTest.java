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
package io.github.hyshmily.zeta.sync;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sharding.impl.HealthViewImpl;
import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatMessage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ClusterHealthViewTest {

  private HealthView view;

  @BeforeEach
  void setUp() {
    view = new HealthViewImpl(5000, 2);
  }

  private static WorkerHeartbeatMessage hb(String workerId, long epoch, boolean ready) {
    return new WorkerHeartbeatMessage(0L, workerId, epoch, 0.0, ready, 0, 0, 0, 0);
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

  /**
   * A heartbeat is stronger liveness evidence than probe failures: the same-epoch
   * heartbeat clears the verification failure count, so strikes never accumulate
   * across a recovered partition episode.
   */
  @Test
  void shouldResetVerifyFailuresOnHeartbeat() {
    view.onHeartbeat(hb("w1", 1, true));
    view.markVerificationFailed("w1");
    view.markVerificationFailed("w1");
    assertThat(view.getVerifyFailures("w1")).isEqualTo(2);
    view.onHeartbeat(hb("w1", 1, true));
    assertThat(view.getVerifyFailures("w1")).isZero();
    view.markVerificationFailed("w1");
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
  void shouldReturnFalseWhenNoWorkersObserved() {
    assertThat(view.isClusterHealthy()).isFalse();
  }

  @Test
  void shouldReturnTrueWhenObservedWorkersAreAlive() {
    view.onHeartbeat(hb("w1", 1, true));
    view.onHeartbeat(hb("w2", 1, true));
    assertThat(view.isClusterHealthy()).isTrue();
  }

  /**
   * Verifies the core ADR-0028 semantics: with 3 observed Workers, a single
   * surviving Worker is considered a healthy cluster (one third, minimum 1).
   */
  @Test
  void shouldReturnTrueWhenSingleWorkerOfThreeIsAlive() {
    view.onHeartbeat(hb("w1", 1, true));
    view.onHeartbeat(hb("w2", 1, true));
    view.onHeartbeat(hb("w3", 1, true));
    view.markVerificationFailed("w2");
    view.markVerificationFailed("w2");
    view.markVerificationFailed("w3");
    view.markVerificationFailed("w3");
    assertThat(view.getAliveWorkerIds()).containsExactly("w1");
    assertThat(view.isClusterHealthy()).isTrue();
  }

  @Test
  void shouldReturnFalseWhenNoObservedWorkerIsAlive() {
    view.onHeartbeat(hb("w1", 1, true));
    view.markVerificationFailed("w1");
    view.markVerificationFailed("w1");
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
    // 1 of 3 alive = one third (floor 1) → still healthy
    assertThat(view.isClusterHealthy()).isTrue();
    view.markVerificationFailed("w3");
    view.markVerificationFailed("w3");
    assertThat(view.isClusterHealthy()).isFalse();
  }

  /**
   * Verifies the one-third threshold is rounded up: with 7 observed Workers the
   * threshold is ceil(7/3) = 3, so 3 alive is healthy and 2 alive is not.
   */
  @Test
  void isClusterHealthy_oneThirdThresholdRoundedUp() {
    for (int i = 1; i <= 7; i++) {
      view.onHeartbeat(hb("w" + i, 1, true));
    }
    for (int i = 4; i <= 7; i++) {
      view.markVerificationFailed("w" + i);
      view.markVerificationFailed("w" + i);
    }
    assertThat(view.getAliveWorkerIds()).hasSize(3);
    assertThat(view.isClusterHealthy()).isTrue();
    view.markVerificationFailed("w3");
    view.markVerificationFailed("w3");
    assertThat(view.getAliveWorkerIds()).hasSize(2);
    assertThat(view.isClusterHealthy()).isFalse();
  }

  /**
   * Verifies that minAliveWorkers overrides the derived one-third threshold.
   */
  @Test
  void minAliveWorkers_shouldOverrideDerivedThreshold() {
    view.setMinAliveWorkers(3);
    view.onHeartbeat(hb("w1", 1, true));
    view.onHeartbeat(hb("w2", 1, true));
    assertThat(view.isClusterHealthy()).isFalse();
    view.onHeartbeat(hb("w3", 1, true));
    assertThat(view.isClusterHealthy()).isTrue();
  }

  /**
   * Verifies that with observed alive Workers the cluster is healthy
   * (records-derived threshold).
   */
  @Test
  void isClusterHealthy_withAliveWorkersObserved_shouldReturnTrue() {
    HealthView empty = new HealthViewImpl(5000, 2);
    empty.onHeartbeat(hb("w1", 1, true));
    empty.onHeartbeat(hb("w2", 1, true));
    assertThat(empty.isClusterHealthy()).isTrue();
  }

  /**
   * Verifies that with NO observed alive Workers the cluster is unhealthy.
   */
  @Test
  void isClusterHealthy_withNoObservedAliveWorkers_shouldReturnFalse() {
    HealthView empty = new HealthViewImpl(5000, 2);
    assertThat(empty.isClusterHealthy()).isFalse();
  }

  /**
   * Verifies the TTL safety net: a state change that does not go through a
   * mutation hook (heartbeat timeout is time-driven) is reflected once the
   * cached judgment's TTL elapses. Uses a small heartbeat timeout and cache
   * TTL to keep the test fast.
   */
  @Test
  void isClusterHealthy_shouldRefreshCacheAfterTtl() throws InterruptedException {
    HealthView fast = new HealthViewImpl(300, 2, 100);
    fast.onHeartbeat(hb("w1", 1, true));
    assertThat(fast.isClusterHealthy()).isTrue();

    // w1's heartbeat expires (300ms); the TTL safety net (100ms) forces a
    // recomputation that sees the timeout.
    Thread.sleep(500);
    assertThat(fast.isClusterHealthy()).isFalse();
  }

  /**
   * Verifies that mutation hooks invalidate the cached judgment immediately:
   * a state change is reflected without waiting for the cache TTL.
   */
  @Test
  void isClusterHealthy_mutationHook_shouldReflectChangeImmediately() {
    HealthView fast = new HealthViewImpl(300_000, 2, 100_000);
    fast.onHeartbeat(hb("w1", 1, true));
    assertThat(fast.isClusterHealthy()).isTrue();
    // Same TTL window, but the removal hook invalidates the cache.
    fast.removeRecord("w1");
    assertThat(fast.isClusterHealthy()).isFalse();
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

  // ── isAlive / epochOf (Decision-Validity accessors, ADR-0035) ──

  @Test
  void isAliveShouldReflectAliveSet() {
    view.onHeartbeat(hb("w1", 1, true));
    assertThat(view.isAlive("w1")).isTrue();
    assertThat(view.isAlive("w2")).isFalse();
  }

  @Test
  void isAliveShouldReturnFalseForDeadWorker() {
    view.onHeartbeat(hb("w1", 1, true));
    view.markVerificationFailed("w1");
    view.markVerificationFailed("w1");
    assertThat(view.isAlive("w1")).isFalse();
  }

  @Test
  void epochOfShouldReturnStoredEpoch() {
    view.onHeartbeat(hb("w1", 7, true));
    assertThat(view.epochOf("w1")).isEqualTo(7);
  }

  @Test
  void epochOfShouldReturnUnknownForUnseenWorker() {
    assertThat(view.epochOf("w1")).isEqualTo(HealthView.UNKNOWN_EPOCH);
  }

  @Test
  void epochOfShouldReturnUnknownForRemovedWorker() {
    view.onHeartbeat(hb("w1", 7, true));
    view.removeRecord("w1");
    assertThat(view.epochOf("w1")).isEqualTo(HealthView.UNKNOWN_EPOCH);
  }

  @Test
  void epochOfShouldReturnNewEpochAfterRestart() {
    view.onHeartbeat(hb("w1", 1, true));
    view.onHeartbeat(hb("w1", 2, true));
    assertThat(view.epochOf("w1")).isEqualTo(2);
  }

  // ── WorkerHealthRecord.isAlive ──

  @Test
  void isAliveShouldReturnTrueWhenReadyAndNotStaleAndWithinTimeout() {
    HealthViewImpl.WorkerHealthRecord r = new HealthViewImpl.WorkerHealthRecord();
    r.readyToServe = true;
    r.stale = false;
    r.lastAliveEvidenceTime = TimeSource.monotonicMillis();
    assertThat(r.isAlive(5000)).isTrue();
  }

  @Test
  void isAliveShouldReturnFalseWhenNotReady() {
    HealthViewImpl.WorkerHealthRecord r = new HealthViewImpl.WorkerHealthRecord();
    r.readyToServe = false;
    r.stale = false;
    r.lastAliveEvidenceTime = TimeSource.monotonicMillis();
    assertThat(r.isAlive(5000)).isFalse();
  }

  @Test
  void isAliveShouldReturnFalseWhenStale() {
    HealthViewImpl.WorkerHealthRecord r = new HealthViewImpl.WorkerHealthRecord();
    r.readyToServe = true;
    r.stale = true;
    r.lastAliveEvidenceTime = TimeSource.monotonicMillis();
    assertThat(r.isAlive(5000)).isFalse();
  }

  @Test
  void isAliveShouldReturnFalseWhenTimedOut() {
    HealthViewImpl.WorkerHealthRecord r = new HealthViewImpl.WorkerHealthRecord();
    r.readyToServe = true;
    r.stale = false;
    r.lastAliveEvidenceTime = TimeSource.monotonicMillis() - 100;
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
   * Verifies that an epoch change replaces the per-Worker record entirely.
   */
  @Test
  void onHeartbeat_higherEpoch_shouldReplaceRecord() {
    view.onHeartbeat(hb("w1", 1, true));
    view.onHeartbeat(hb("w1", 2, false));
    assertThat(view.getAliveWorkerIds()).doesNotContain("w1");
  }

  /**
   * Verifies that isAlive returns false when exactly at the timeout boundary (elapsed == timeoutMs).
   */
  @Test
  void isAlive_exactTimeoutBoundary_shouldReturnFalse() {
    HealthViewImpl.WorkerHealthRecord r = new HealthViewImpl.WorkerHealthRecord();
    r.readyToServe = true;
    r.stale = false;
    r.lastAliveEvidenceTime = TimeSource.monotonicMillis() - 5000;
    assertThat(r.isAlive(5000)).isFalse();
  }

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
    // 1 of 3 alive = one third (floor 1) → still healthy
    assertThat(view.isClusterHealthy()).isTrue();
    view.markVerificationFailed("w3");
    view.markVerificationFailed("w3");
    assertThat(view.isClusterHealthy()).isFalse();
    view.recordPong("w1");
    view.onHeartbeat(hb("w1", 1, true));
    assertThat(view.getAliveWorkerIds()).contains("w1");
    assertThat(view.isClusterHealthy()).isTrue();
  }
}
