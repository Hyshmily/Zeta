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
package io.github.hyshmily.zeta.reporting.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hyshmily.zeta.reporting.impl.ReportFeedLoop.Mode;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link ReportFeedLoop} tuner: seed clamping, two-window
 * score averaging, the sparse-grows / dense-shrinks control direction, and the
 * caller-policy {@code [min, max]} clamp. The feed arithmetic itself is pinned
 * by {@code FeedLoopTest}; these tests pin the tuning policy around it.
 */
class ReportFeedLoopTest {

  @Test
  void seedInterval_isClampedIntoThePolicyWindow() {
    assertThat(new ReportFeedLoop(Mode.ON, 512, 50, 1000, 50).intervalMs()).isEqualTo(50);
    assertThat(new ReportFeedLoop(Mode.ON, 512, 50, 1000, 2000).intervalMs()).isEqualTo(1000);
    assertThat(new ReportFeedLoop(Mode.ON, 512, 100, 1000, 50).intervalMs()).isEqualTo(100);
  }

  @Test
  void constructor_rejectsInvalidPolicy() {
    assertThatThrownBy(() -> new ReportFeedLoop(Mode.ON, 0, 50, 1000, 50))
      .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new ReportFeedLoop(Mode.ON, 512, 0, 1000, 50))
      .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new ReportFeedLoop(Mode.ON, 512, 1000, 50, 50))
      .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new ReportFeedLoop(null, 512, 50, 1000, 50))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void batchOnTarget_keepsTheSeedInterval() {
    ReportFeedLoop loop = new ReportFeedLoop(Mode.ON, 512, 50, 1000, 200);
    // First sample has no previous window: avg = 512 = target -> score 10000.
    assertThat(loop.onFlushCompleted(512)).isEqualTo(200);
    assertThat(loop.lastScoreBp()).isEqualTo(10_000);
    // Second window averages to the target again.
    assertThat(loop.onFlushCompleted(512)).isEqualTo(200);
  }

  @Test
  void sparseBatches_growTheInterval_towardMax() {
    ReportFeedLoop loop = new ReportFeedLoop(Mode.ON, 512, 50, 1000, 50);
    long previous = 50;
    for (int i = 0; i < 8; i++) {
      long next = loop.onFlushCompleted(1); // batch of 1 vs target 512: deeply sparse
      assertThat(next).isGreaterThanOrEqualTo(previous); // never shrinks while sparse
      previous = next;
    }
    assertThat(previous).isEqualTo(1000); // clamped at the detection-lag ceiling
    assertThat(loop.lastAvgBatchSize()).isEqualTo(1);
  }

  @Test
  void denseBatches_shrinkTheInterval_towardMin() {
    ReportFeedLoop loop = new ReportFeedLoop(Mode.ON, 512, 50, 1000, 200);
    // Two windows averaging 20x the target: score >= 2x goal -> overshoot cliff.
    loop.onFlushCompleted(1_000_000 >> 1); // first window (no prev): huge batch
    long next = loop.onFlushCompleted(10_240); // second window: avg still ~20x target
    assertThat(next).isEqualTo(50);
    assertThat(loop.lastScoreBp()).isGreaterThanOrEqualTo(20_000);
  }

  @Test
  void twoWindowAverage_scoresTheAverage_notTheLastWindow() {
    ReportFeedLoop loop = new ReportFeedLoop(Mode.ON, 512, 50, 1000, 100);
    loop.onFlushCompleted(512); // avg = 512 (no prev) -> on target
    // One burst window of 4096: the score is computed from the two-window
    // average (512 + 4096) / 2 = 2304 -> 45000 bp — NOT from the raw last
    // window (80000 bp). The averaging, not the burst, is what the loop sees.
    long next = loop.onFlushCompleted(4096);
    assertThat(loop.lastAvgBatchSize()).isEqualTo(2304);
    assertThat(loop.lastScoreBp()).isEqualTo(45_000);
    assertThat(next).isEqualTo(50); // 4.5x target: past the 2x cliff, lands on the floor
  }

  @Test
  void intervalAlwaysStaysWithinThePolicyWindow() {
    ReportFeedLoop loop = new ReportFeedLoop(Mode.ON, 512, 50, 400, 50);
    for (int batch = 0; batch <= 2048; batch += 137) {
      long next = loop.onFlushCompleted(batch);
      assertThat(next).isBetween(50L, 400L);
    }
  }

  @Test
  void shadowMode_isComputeOnly_semantics() {
    // The mode flag is opaque to the tuner itself (application is the
    // caller's duty — KeyReporterImpl applies only in ON); pin the enum
    // contract so a rename cannot silently flip the default behavior.
    assertThat(Mode.valueOf("SHADOW")).isNotEqualTo(Mode.ON);
  }
}
