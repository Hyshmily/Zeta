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
package io.github.hyshmily.zeta.tests;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sharding.impl.HealthViewImpl;
import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatMessage;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.window.RollingWindow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for wall-clock jump resilience (I27): NTP steps and container
 * clock drift must not break delta computations. All delta-based components
 * (heartbeat timeouts, sliding windows) use {@link TimeSource#monotonicMillis()},
 * which ignores wall-clock jumps; absolute expiry timestamps still use the wall
 * clock by design.
 */
class TimeJumpTest {

  @AfterEach
  void tearDown() {
    TimeSource.setTimeOffsetForTest(0, 0);
  }

  private static WorkerHeartbeatMessage heartbeat(String workerId, long epoch) {
    return new WorkerHeartbeatMessage(0L, workerId, epoch, 0.0, true, 0, 0, 0, 0);
  }

  // ── TimeSource semantics ──

  /**
   * Verifies that a wall-clock offset does not move the monotonic axis — this is
   * the core guarantee all delta computations rely on.
   */
  @Test
  void monotonicMillis_shouldIgnoreWallJump() throws InterruptedException {
    TimeSource.setTimeOffsetForTest(60_000, 0);
    long before = TimeSource.monotonicMillis();
    Thread.sleep(20);
    long after = TimeSource.monotonicMillis();

    assertThat(after).isGreaterThanOrEqualTo(before);
    assertThat(Math.abs(after - before)).isLessThan(1_000);
  }

  /**
   * Verifies that the wall-clock axis still moves with a forward jump — TTL
   * expiry semantics are intentionally wall-clock based.
   */
  @Test
  void currentTimeMillis_shouldReflectWallJump() {
    TimeSource.setTimeOffsetForTest(60_000, 0);
    long ts = TimeSource.currentTimeMillis();
    assertThat(Math.abs(ts - System.currentTimeMillis())).isGreaterThan(50_000);
  }

  // ── Heartbeat health ──

  /**
   * Verifies that a +60s NTP forward jump does not kill alive Workers: the
   * heartbeat timeout uses monotonic time, so only real elapsed time matters.
   */
  @Test
  void forwardWallJump_shouldNotKillAliveWorkers() {
    HealthView view = new HealthViewImpl(5000, 2);
    view.onHeartbeat(heartbeat("w1", 1L));

    TimeSource.setTimeOffsetForTest(60_000, 0);

    assertThat(view.getAliveWorkerIds()).contains("w1");
    assertThat(view.isClusterHealthy()).isTrue();
  }

  /**
   * Verifies that a -60s NTP backward jump does not revive dead Workers: once
   * real (monotonic) time has exceeded the timeout, a wall-clock step backwards
   * must not resurrect the record.
   */
  @Test
  void backwardWallJump_shouldNotReviveDeadWorkers() {
    HealthView view = new HealthViewImpl(5000, 2);
    view.onHeartbeat(heartbeat("w1", 1L));

    TimeSource.setTimeOffsetForTest(0, 10_000);
    assertThat(view.getAliveWorkerIds()).doesNotContain("w1");

    TimeSource.setTimeOffsetForTest(-60_000, 10_000);
    assertThat(view.getAliveWorkerIds()).doesNotContain("w1");
  }

  // ── Rolling window ──

  /**
   * Verifies that a +60s NTP forward jump does not rotate a RollingWindow:
   * bucket rotation is driven by monotonic elapsed time, not the wall clock.
   */
  @Test
  void forwardWallJump_shouldNotRotateRollingWindow() {
    RollingWindow window = new RollingWindow(10, 1000);
    window.add(100);

    TimeSource.setTimeOffsetForTest(60_000, 0);

    assertThat(window.sum()).isEqualTo(100);
  }
}
