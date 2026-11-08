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

import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sharding.HealthViewImpl;
import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatMessage;
import io.github.hyshmily.zeta.util.TimeSource;
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
    view = new HealthViewImpl(5000);
  }

  private static WorkerHeartbeatMessage hb(String workerId, long epoch, boolean ready) {
    return new WorkerHeartbeatMessage(0L, workerId, epoch, ready, null);
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
   * Verifies the core design decisions: heartbeat and health semantics: with 3 observed Workers, a single
   * surviving Worker is considered a healthy cluster (one third, minimum 1).
   */
  @Test
  void shouldReturnTrueWhenSingleWorkerOfThreeIsAlive() {
    view.onHeartbeat(hb("w1", 1, true));
    view.onHeartbeat(hb("w2", 1, false));
    view.onHeartbeat(hb("w3", 1, false));
    assertThat(view.getAliveWorkerIds()).containsExactly("w1");
    assertThat(view.isClusterHealthy()).isTrue();
  }

  @Test
  void shouldReturnFalseWhenNoObservedWorkerIsAlive() {
    view.onHeartbeat(hb("w1", 1, false));
    assertThat(view.isClusterHealthy()).isFalse();
  }

  @Test
  void shouldExcludeNotReadyWorkersFromHealthCheck() {
    view.onHeartbeat(hb("w1", 1, true));
    view.onHeartbeat(hb("w2", 1, true));
    view.onHeartbeat(hb("w3", 1, true));
    view.onHeartbeat(hb("w1", 1, false));
    assertThat(view.isClusterHealthy()).isTrue();
    view.onHeartbeat(hb("w2", 1, false));
    // 1 of 3 alive = one third (floor 1) → still healthy
    assertThat(view.isClusterHealthy()).isTrue();
    view.onHeartbeat(hb("w3", 1, false));
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
      view.onHeartbeat(hb("w" + i, 1, false));
    }
    assertThat(view.getAliveWorkerIds()).hasSize(3);
    assertThat(view.isClusterHealthy()).isTrue();
    view.onHeartbeat(hb("w3", 1, false));
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
    HealthView empty = new HealthViewImpl(5000);
    empty.onHeartbeat(hb("w1", 1, true));
    empty.onHeartbeat(hb("w2", 1, true));
    assertThat(empty.isClusterHealthy()).isTrue();
  }

  /**
   * Verifies that with NO observed alive Workers the cluster is unhealthy.
   */
  @Test
  void isClusterHealthy_withNoObservedAliveWorkers_shouldReturnFalse() {
    HealthView empty = new HealthViewImpl(5000);
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
    HealthView fast = new HealthViewImpl(300, 100, false, 0);
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
    HealthView fast = new HealthViewImpl(300_000, 100_000, false, 0);
    fast.onHeartbeat(hb("w1", 1, true));
    assertThat(fast.isClusterHealthy()).isTrue();
    // Same TTL window, but the removal hook invalidates the cache.
    fast.removeRecord("w1");
    assertThat(fast.isClusterHealthy()).isFalse();
  }

  /**
   * A same-epoch heartbeat that flips the Worker's readiness must invalidate the cached
   * verdict inside the same TTL window; only a heartbeat that leaves liveness unchanged
   * may keep the cached judgment.
   */
  @Test
  void isClusterHealthy_heartbeatLivenessTransition_shouldInvalidateImmediately() {
    HealthView fast = new HealthViewImpl(300_000, 100_000, false, 0);
    fast.onHeartbeat(hb("w1", 1, true));
    assertThat(fast.isClusterHealthy()).isTrue();

    fast.onHeartbeat(hb("w1", 1, false));

    assertThat(fast.isClusterHealthy()).isFalse();
  }

  // ── getAliveWorkerIds ──

  @Test
  void shouldReturnOnlyAliveWorkers() {
    view.onHeartbeat(hb("w1", 1, true));
    view.onHeartbeat(hb("w2", 1, false));
    view.onHeartbeat(hb("w3", 1, false));
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

  // ── isAlive / epochOf (Decision-Validity accessors, design decisions: hot state) ──

  @Test
  void isAliveShouldReflectAliveSet() {
    view.onHeartbeat(hb("w1", 1, true));
    assertThat(view.isAlive("w1")).isTrue();
    assertThat(view.isAlive("w2")).isFalse();
  }

  @Test
  void isAliveShouldReturnFalseForDeadWorker() {
    view.onHeartbeat(hb("w1", 1, false));
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
  void isAliveShouldReturnTrueWhenReadyAndWithinTimeout() {
    HealthViewImpl.WorkerHealthRecord r = new HealthViewImpl.WorkerHealthRecord(
      "w1",
      1L,
      TimeSource.monotonicMillis(),
      true
    );
    assertThat(r.isAlive(TimeSource.monotonicMillis(), 5000, 0L)).isTrue();
  }

  @Test
  void isAliveShouldReturnFalseWhenNotReady() {
    HealthViewImpl.WorkerHealthRecord r = new HealthViewImpl.WorkerHealthRecord(
      "w1",
      1L,
      TimeSource.monotonicMillis(),
      false
    );
    assertThat(r.isAlive(TimeSource.monotonicMillis(), 5000, 0L)).isFalse();
  }

  @Test
  void isAliveShouldReturnFalseWhenTimedOut() {
    HealthViewImpl.WorkerHealthRecord r = new HealthViewImpl.WorkerHealthRecord(
      "w1",
      1L,
      TimeSource.monotonicMillis() - 100,
      true
    );
    assertThat(r.isAlive(TimeSource.monotonicMillis(), 50, 0L)).isFalse();
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
    HealthViewImpl.WorkerHealthRecord r = new HealthViewImpl.WorkerHealthRecord(
      "w1",
      1L,
      TimeSource.monotonicMillis() - 5000,
      true
    );
    assertThat(r.isAlive(TimeSource.monotonicMillis(), 5000, 0L)).isFalse();
  }

  @Test
  void shouldHandleHeartbeatForSingleWorker() {
    view.onHeartbeat(hb("w1", 1, true));
    assertThat(view.getAliveWorkerIds()).containsExactly("w1");
    assertThat(view.getAllWorkerIds()).containsExactly("w1");
  }

  // ── Integration ──

  @Test
  void shouldChainHeartbeatReadinessAndHealthCheck() {
    view.onHeartbeat(hb("w1", 1, true));
    view.onHeartbeat(hb("w2", 1, true));
    view.onHeartbeat(hb("w3", 1, true));
    assertThat(view.isClusterHealthy()).isTrue();
    view.onHeartbeat(hb("w1", 1, false));
    assertThat(view.isClusterHealthy()).isTrue();
    view.onHeartbeat(hb("w2", 1, false));
    // 1 of 3 alive = one third (floor 1) → still healthy
    assertThat(view.isClusterHealthy()).isTrue();
    view.onHeartbeat(hb("w3", 1, false));
    assertThat(view.isClusterHealthy()).isFalse();
    view.onHeartbeat(hb("w1", 1, true));
    assertThat(view.getAliveWorkerIds()).contains("w1");
    assertThat(view.isClusterHealthy()).isTrue();
  }
}
