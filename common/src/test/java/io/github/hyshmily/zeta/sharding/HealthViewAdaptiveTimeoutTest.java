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

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.sharding.HealthViewImpl.WorkerHealthRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Adaptive per-Worker liveness timeouts (design decisions: heartbeat and health): the
 * Jacobson/Karels estimator over heartbeat inter-arrival, the warm-up/static
 * window, and the [floor, configured] clamp. The static {@code timeoutMs}
 * stops being a flat threshold and becomes the warm-up window and the ceiling.
 */
class HealthViewAdaptiveTimeoutTest {

  private static WorkerHealthRecord recordAt(long now, boolean ready) {
    return new WorkerHealthRecord("w1", 1L, now, ready);
  }

  @Test
  @DisplayName("estimator: steady 1s heartbeats collapse to the floor clamp, jitter widens, the ceiling holds")
  void estimator_convergesAndClamps() {
    long now = 1_000_000L;
    WorkerHealthRecord record = recordAt(now, true);
    assertThat(record.effectiveTimeoutMs(10_000, 1))
      .as("no estimate before warm-up: the static window stands")
      .isEqualTo(10_000);

    for (int i = 0; i < WorkerHealthRecord.WARMUP_SAMPLES; i++) {
      now += 1_000L;
      record.onHeartbeatSample(now, true);
    }
    assertThat(record.effectiveTimeoutMs(10_000, 1)).as("warm-up publishes the estimate").isBetween(1_000L, 3_000L);
    // SRTT ≈ 1000, RTTVAR decays onto its 200 ms floor → 1800 → clamped to floor.
    assertThat(record.effectiveTimeoutMs(10_000, 3_000)).isEqualTo(3_000);

    // Inter-arrival jitter widens the window past the floor: the estimator
    // must widen with observed variance instead of pinning the floor.
    for (int i = 0; i < 8; i++) {
      now += 6_000L;
      record.onHeartbeatSample(now, true);
    }
    assertThat(record.effectiveTimeoutMs(10_000, 3_000)).isGreaterThan(3_000);

    // Sustained slow heartbeats push the raw estimate past the ceiling: the
    // configured static window remains the upper bound.
    for (int i = 0; i < 50; i++) {
      now += 60_000L;
      record.onHeartbeatSample(now, true);
    }
    assertThat(record.effectiveTimeoutMs(10_000, 3_000)).isEqualTo(10_000);
  }

  @Test
  @DisplayName("estimator: non-positive deltas (out-of-order arrival) are ignored")
  void estimator_ignoresNonPositiveDeltas() {
    long now = 1_000_000L;
    WorkerHealthRecord record = recordAt(now, true);
    record.onHeartbeatSample(now, true);
    record.onHeartbeatSample(now, true);
    // Only one real sample: still below warm-up, so the static window is returned.
    assertThat(record.effectiveTimeoutMs(10_000, 1)).isEqualTo(10_000);
    // Fill the warm-up with the same delta: the rejected samples must not have skewed it,
    // so the window stays anchored on the real 1 s inter-arrival (SRTT 1 s plus a decayed
    // deviation) instead of drifting toward the bogus deltas.
    for (int i = 0; i < WorkerHealthRecord.WARMUP_SAMPLES; i++) {
      now += 1_000L;
      record.onHeartbeatSample(now, true);
    }
    assertThat(record.effectiveTimeoutMs(Long.MAX_VALUE, 1)).isBetween(1_000L, 3_000L);
  }

  @Test
  @DisplayName("record: static window until warm-up, adaptive after, static again when disabled")
  void record_effectiveTimeoutFollowsWarmupAndSwitch() {
    long now = 1_000_000L;
    WorkerHealthRecord record = recordAt(now, true);
    for (int i = 0; i < 3; i++) {
      now += 1_000L;
      record.onHeartbeatSample(now, true);
    }
    assertThat(record.effectiveTimeoutMs(10_000, 3_000))
      .as("below warm-up the static window applies")
      .isEqualTo(10_000);

    for (int i = 0; i < 6; i++) {
      now += 1_000L;
      record.onHeartbeatSample(now, true);
    }
    assertThat(record.effectiveTimeoutMs(10_000, 3_000))
      .as("warmed up: clamped estimate, not the flat window")
      .isEqualTo(3_000);
    assertThat(record.effectiveTimeoutMs(10_000, 0))
      .as("adaptive disabled: static window regardless of warm-up")
      .isEqualTo(10_000);
  }

  @Test
  @DisplayName(
    "record: isAlive follows the adaptive window — 3.5s-old evidence is dead at floor 3s, alive at static 10s"
  )
  void record_isAliveUsesAdaptiveWindow() {
    long now = 2_000_000L;
    WorkerHealthRecord record = recordAt(now, true);
    for (int i = 0; i < 9; i++) {
      now += 1_000L;
      record.onHeartbeatSample(now, true);
    }

    record.onHeartbeatSample(now - 3_500L, true);
    assertThat(record.isAlive(now, 10_000, 3_000)).as("evidence older than the adaptive floor is dead").isFalse();
    assertThat(record.isAlive(now, 10_000, 0)).as("the same evidence is well inside the static window").isTrue();

    record.onHeartbeatSample(now - 2_000L, true);
    assertThat(record.isAlive(now, 10_000, 3_000)).isTrue();
  }
}
