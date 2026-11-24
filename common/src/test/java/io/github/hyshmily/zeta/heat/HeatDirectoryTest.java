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
package io.github.hyshmily.zeta.heat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.sharding.ClusterHealth;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.worker.DecisionSkipReason;
import io.github.hyshmily.zeta.sync.worker.WorkerDecision;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import org.junit.jupiter.api.Test;

/**
 * Pins the cluster lease contract: version and incarnation ordering on grant, the COOL tombstone
 * that keeps a late HOT from resurrecting a revoked lease, and the business signal that must fire
 * only for a lease that was actually hot.
 */
class HeatDirectoryTest {

  private static final String KEY = "quotes\u0000q-1";
  private static final String NODE = "worker-1";

  private static HeatDirectory directory(Executor executor) {
    return new HeatDirectory(1_000, 60_000L, executor, null, null);
  }

  private static WorkerDecision hot(long version, long epoch) {
    return new WorkerDecision(version, KEY, DecisionStamp.hot(version, NODE, epoch));
  }

  private static WorkerDecision cool(long version, long epoch) {
    return new WorkerDecision(version, KEY, DecisionStamp.cool(version, NODE, epoch));
  }

  @Test
  void grant_shouldReportNewlyHotOnlyForTheFirstLease() {
    HeatDirectory heat = directory(Runnable::run);

    assertThat(heat.grant(hot(1L, 1L)).newlyHot()).isTrue();
    assertThat(heat.grant(hot(2L, 1L)).newlyHot()).isFalse();
    assertThat(heat.isClusterHot(KEY)).isTrue();
    assertThat(heat.liveStampOf(KEY).decisionVersion()).isEqualTo(2L);
  }

  @Test
  void grant_shouldRejectAnOlderDecisionFromTheSameIncarnation() {
    HeatDirectory heat = directory(Runnable::run);
    heat.grant(hot(2L, 1L));

    HeatDirectory.LeaseOutcome result = heat.grant(hot(1L, 1L));

    assertThat(result.applied()).isFalse();
    assertThat(result.skipReason()).isEqualTo(DecisionSkipReason.VERSION_STALE);
    assertThat(heat.liveStampOf(KEY).decisionVersion()).isEqualTo(2L);
  }

  @Test
  void grant_shouldReturnTheDeadLeaseItReplaces() {
    HeatDirectory heat = directory(Runnable::run);
    heat.grant(hot(9L, 1L));

    HeatDirectory.LeaseOutcome result = heat.grant(hot(1L, 2L));

    assertThat(result.applied()).isTrue();
    assertThat(result.newlyHot()).isTrue();
    assertThat(heat.liveStampOf(KEY).decisionEpoch()).isEqualTo(2L);
    // The restarted Worker's lease is gone, and the replacement is the only record that could
    // report its end: the caller pairs the business onHot for incarnation 1 before the new grant.
    assertThat(result.endedLease()).isNotNull();
    assertThat(result.endedLease().stamp().decisionEpoch()).isEqualTo(1L);
  }

  @Test
  void grant_shouldSkipADecisionWhoseOwnIncarnationIsGone() {
    HeatDirectory heat = new HeatDirectory(1_000, 60_000L, Runnable::run, new ClusterHealth(300_000L), null);

    HeatDirectory.LeaseOutcome result = heat.grant(hot(1L, 1L));

    assertThat(result.applied()).isFalse();
    assertThat(result.skipReason()).isEqualTo(DecisionSkipReason.INCARNATION_DEAD);
    assertThat(heat.liveStampOf(KEY)).isNull();
  }

  @Test
  void revoke_shouldReportTheEndedLeaseAndStopReportingTheKeyHot() {
    HeatDirectory heat = directory(Runnable::run);
    heat.grant(hot(1L, 1L));

    HeatDirectory.LeaseOutcome result = heat.revoke(cool(2L, 1L));

    assertThat(result.applied()).isTrue();
    assertThat(result.endReason()).isEqualTo(HeatLease.EndReason.COOL);
    assertThat(result.endedLease()).isNotNull();
    assertThat(result.endedLease().isHot()).isTrue();
    assertThat(heat.isClusterHot(KEY)).isFalse();
    assertThat(heat.liveStampOf(KEY).isCool()).isTrue();
  }

  @Test
  void revoke_shouldWriteATombstoneForAColdKeyWithoutTheBusinessSignal() {
    HeatDirectory heat = directory(Runnable::run);

    HeatDirectory.LeaseOutcome result = heat.revoke(cool(1L, 1L));

    assertThat(result.applied()).isTrue();
    assertThat(result.endedLease()).isNull();
    assertThat(result.endReason()).isNull();
    assertThat(heat.liveStampOf(KEY).isCool()).isTrue();
  }

  @Test
  void grant_shouldRejectALateHotBelowTheCoolTombstone() {
    HeatDirectory heat = directory(Runnable::run);
    heat.grant(hot(5L, 1L));
    heat.revoke(cool(6L, 1L));

    HeatDirectory.LeaseOutcome late = heat.grant(hot(6L, 1L));
    HeatDirectory.LeaseOutcome newer = heat.grant(hot(7L, 1L));

    assertThat(late.applied()).isFalse();
    assertThat(late.skipReason()).isEqualTo(DecisionSkipReason.VERSION_STALE);
    assertThat(newer.applied()).isTrue();
    assertThat(heat.isClusterHot(KEY)).isTrue();
  }

  @Test
  void revoke_shouldEndAHotLeaseWhoseIncarnationIsGone() {
    HealthView health = mock(HealthView.class);
    when(health.isCurrentIncarnation(NODE, 1L)).thenReturn(true);
    HeatDirectory heat = new HeatDirectory(1_000, 60_000L, Runnable::run, health, null);
    heat.grant(hot(1L, 1L));
    when(health.isCurrentIncarnation(NODE, 1L)).thenReturn(false);

    HeatDirectory.LeaseOutcome result = heat.revoke(cool(2L, 1L));

    assertThat(result.applied()).isTrue();
    assertThat(result.endReason()).isEqualTo(HeatLease.EndReason.INCARNATION_DEAD);
    assertThat(result.endedLease()).isNotNull();
    assertThat(result.endedLease().key()).isEqualTo(KEY);
  }

  @Test
  void revoke_shouldTreatASameNodeNewerEpochAsADeadIncarnation() {
    HeatDirectory heat = directory(Runnable::run);
    heat.grant(hot(1L, 1L));

    HeatDirectory.LeaseOutcome result = heat.revoke(cool(2L, 2L));

    assertThat(result.applied()).isTrue();
    assertThat(result.endReason()).isEqualTo(HeatLease.EndReason.INCARNATION_DEAD);
    assertThat(result.endedLease()).isNotNull();
    assertThat(result.endedLease().stamp().decisionEpoch()).isEqualTo(1L);
  }

  @Test
  void setPassiveEndSink_shouldAcceptNullToDetachTheCallback() {
    List<String> ended = new ArrayList<>();
    HeatDirectory heat = new HeatDirectory(1_000, 60_000L, Runnable::run, new ClusterHealth(300_000L), null);
    heat.setPassiveEndSink((lease, reason) -> ended.add(reason + ":" + lease.key()));
    heat.setPassiveEndSink(null);
    heat.grant(hot(1L, 1L));

    assertThat(heat.isClusterHot(KEY)).isFalse();
    assertThat(ended).isEmpty();
  }

  @Test
  void isClusterHot_shouldEndALeaseWhoseIncarnationIsGone() {
    List<String> ended = new ArrayList<>();
    HealthView health = mock(HealthView.class);
    when(health.isCurrentIncarnation(NODE, 1L)).thenReturn(true);
    HeatDirectory heat = new HeatDirectory(1_000, 60_000L, Runnable::run, health, null);
    heat.setPassiveEndSink((lease, reason) -> ended.add(reason + ":" + lease.key()));
    heat.grant(hot(1L, 1L));
    when(health.isCurrentIncarnation(NODE, 1L)).thenReturn(false);

    assertThat(heat.isClusterHot(KEY)).isFalse();
    assertThat(ended).containsExactly("INCARNATION_DEAD:" + KEY);
  }
}
