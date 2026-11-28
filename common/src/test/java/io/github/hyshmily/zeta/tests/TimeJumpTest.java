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

import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.TtlPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.sharding.ClusterHealth;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatMessage;
import io.github.hyshmily.zeta.util.TimeSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for wall-clock jump resilience (I27): NTP steps and container
 * clock drift must not break delta or deadline computations. Every duration-based
 * component — heartbeat timeouts, TTL deadlines, keep-warm windows — runs on
 * {@link TimeSource#monotonicMillis()}, which ignores wall-clock jumps; the cached wall
 * clock is left to time-sortable stamps and trace IDs.
 */
class TimeJumpTest {

  @AfterEach
  void tearDown() {
    TimeSource.setTimeOffsetForTest(0, 0);
  }

  private static WorkerHeartbeatMessage heartbeat(String workerId, long epoch) {
    return new WorkerHeartbeatMessage(0L, workerId, epoch, true, null);
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
   * Verifies that the wall-clock axis still moves with a forward jump — it is the
   * axis stamps and trace IDs need, and the one no TTL deadline depends on.
   */
  @Test
  void currentTimeMillis_shouldReflectWallJump() {
    TimeSource.setTimeOffsetForTest(60_000, 0);
    long ts = TimeSource.currentTimeMillis();
    assertThat(Math.abs(ts - System.currentTimeMillis())).isGreaterThan(50_000);
  }

  // ── TTL deadlines ──

  /**
   * Verifies that a deadline is minted on the monotonic axis: with a +60 s wall offset already in
   * place, a 30 s deadline must still land inside the monotonic window, not 60 s later on the wall
   * clock. A wall-clock mint here is the silent half of a mixed-axis regression — every deadline
   * would be offset by the machine's uptime relative to the wall clock.
   */
  @Test
  void ttlDeadline_shouldBeMintedOnTheMonotonicAxis() {
    TtlPolicy ttlPolicy = new TtlPolicy(new ZetaProperties());
    TimeSource.setTimeOffsetForTest(60_000, 0);
    long mono = TimeSource.monotonicMillis();

    CacheEntry entry = EntryDraft.blank(ttlPolicy).value("v").version(1L).ttl(30_000L, 0L).build();

    assertThat(entry.getHardExpireAtMs()).isBetween(mono, mono + 30_000L);
    assertThat(entry.getHardExpireAtMs()).isLessThan(TimeSource.currentTimeMillis());
  }

  /**
   * Verifies that a wall step after the entry exists cannot expire it: a +60 s jump puts the 30 s
   * deadline behind the wall clock, and the entry must still be live. A wall-clock judgment here is
   * the other half of the regression — an NTP step silently rewriting every resident's lifetime.
   */
  @Test
  void wallJumpAfterTheBuild_shouldNotExpireAStoredEntry() {
    TtlPolicy ttlPolicy = new TtlPolicy(new ZetaProperties());
    CacheEntry entry = EntryDraft.blank(ttlPolicy).value("v").version(1L).ttl(30_000L, 0L).build();

    TimeSource.setTimeOffsetForTest(60_000, 0);

    assertThat(entry.getHardExpireAtMs()).isLessThan(TimeSource.currentTimeMillis());
    assertThat(ttlPolicy.isLogicallyExpired(entry)).isFalse();
    assertThat(ttlPolicy.isLive(entry)).isTrue();
  }

  // ── Heartbeat health ──

  /**
   * Verifies that a +60s NTP forward jump does not kill alive Workers: the
   * heartbeat timeout uses monotonic time, so only real elapsed time matters.
   */
  @Test
  void forwardWallJump_shouldNotKillAliveWorkers() {
    HealthView view = new ClusterHealth(5000);
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
    HealthView view = new ClusterHealth(5000);
    view.onHeartbeat(heartbeat("w1", 1L));

    TimeSource.setTimeOffsetForTest(0, 10_000);
    assertThat(view.getAliveWorkerIds()).doesNotContain("w1");

    TimeSource.setTimeOffsetForTest(-60_000, 10_000);
    assertThat(view.getAliveWorkerIds()).doesNotContain("w1");
  }
}
