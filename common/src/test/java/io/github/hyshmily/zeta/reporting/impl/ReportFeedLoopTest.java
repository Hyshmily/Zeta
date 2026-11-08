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
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link ReportFeedLoop} tuner: seed clamping, the plant
 * inverse law ({@code next = last × goal / score}) with its deadband, minimum
 * step and {@code [0.5x, 2x]} rate limit, and the wall-clock low-pass that
 * smooths the arrival rate. The tuner is exercised in rate space — a batch is
 * only ever read together with the measured window it accumulated over — so the
 * filter and the projection tests drive the package-private clock seam.
 */
class ReportFeedLoopTest {

  /** Deterministic monotonic clock for the low-pass tests. */
  private final AtomicLong now = new AtomicLong();

  private ReportFeedLoop loop(Mode mode, long target, long min, long max, long seed) {
    return new ReportFeedLoop(mode, target, min, max, seed, now::get);
  }

  @Test
  void seedInterval_isClampedIntoThePolicyWindow() {
    assertThat(loop(Mode.ON, 512, 50, 1000, 50).intervalMs()).isEqualTo(50);
    assertThat(loop(Mode.ON, 512, 50, 1000, 2000).intervalMs()).isEqualTo(1000);
    assertThat(loop(Mode.ON, 512, 100, 1000, 50).intervalMs()).isEqualTo(100);
  }

  @Test
  void constructor_rejectsInvalidPolicy() {
    assertThatThrownBy(() -> loop(Mode.ON, 0, 50, 1000, 50)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> loop(Mode.ON, 512, 0, 1000, 50)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> loop(Mode.ON, 512, 1000, 50, 50)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> loop(null, 512, 50, 1000, 50)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void firstSample_seedsTheRateAndProjectsOntoTheSeedInterval() {
    ReportFeedLoop loop = loop(Mode.ON, 512, 50, 1000, 200);
    ReportFeedLoop.State state = loop.onFlushCompleted(512, 200);
    // 512 keys over 200ms = 2.56 keys/ms; projected at the 200ms input = 512.
    assertThat(state).isEqualTo(new ReportFeedLoop.State(200, 512, 10_000));
    assertThat(loop.intervalMs()).isEqualTo(state.intervalMs());
    assertThat(loop.projectedBatchSize()).isEqualTo(state.batchSize());
    assertThat(loop.lastScoreBp()).isEqualTo(state.scoreBp());
  }

  @Test
  void onTarget_keepsTheSeedInterval() {
    ReportFeedLoop loop = loop(Mode.ON, 512, 50, 1000, 200);
    assertThat(loop.onFlushCompleted(512, 200).intervalMs()).isEqualTo(200);
    assertThat(loop.onFlushCompleted(512, 200).intervalMs()).isEqualTo(200);
  }

  @Test
  void shadowProjection_doesNotIntegrateAnInputThatWasNeverApplied() {
    // SHADOW never applies its interval, so the measured window stays 50ms
    // while the loop believes the cadence is its own input. Scoring the raw
    // batch would wind the integral up (each 512-key window would look 10x at
    // a 500ms input); scoring the RATE projected onto the input holds the
    // loop at its seed — the faithful prediction ON would have produced.
    ReportFeedLoop loop = loop(Mode.SHADOW, 512, 50, 1000, 50);
    for (int i = 0; i < 20; i++) {
      loop.onFlushCompleted(512, 50);
    }
    assertThat(loop.intervalMs()).isEqualTo(50);
    assertThat(loop.projectedBatchSize()).isEqualTo(512);
    assertThat(loop.lastScoreBp()).isEqualTo(10_000);
  }

  @Test
  void applyMode_windowEqualToInput_collapsesTheProjectionToTheObservedBatch() {
    ReportFeedLoop loop = loop(Mode.ON, 512, 50, 1000, 200);
    // Window == interval in ON, so the projection is exactly the batch.
    ReportFeedLoop.State state = loop.onFlushCompleted(1024, 200);
    assertThat(state.batchSize()).isEqualTo(1024);
    assertThat(state.scoreBp()).isEqualTo(20_000);
  }

  @Test
  void sparseBatches_growTheInterval_towardMax() {
    ReportFeedLoop loop = loop(Mode.ON, 512, 50, 1000, 50);
    long previous = 50;
    for (int i = 0; i < 8; i++) {
      // One key per window: at the seed the window is 50ms, so the projected
      // batch is 1 and the loop doubles the interval until the ceiling.
      long next = loop.onFlushCompleted(1, previous).intervalMs();
      assertThat(next).isGreaterThanOrEqualTo(previous);
      previous = next;
    }
    assertThat(previous).isEqualTo(1000);
    assertThat(loop.projectedBatchSize()).isEqualTo(1); // 1 key per 1000ms window
  }

  @Test
  void denseBatches_shrinkTheInterval_towardMin() {
    ReportFeedLoop loop = loop(Mode.ON, 512, 50, 1000, 1000);
    long next = loop.onFlushCompleted(1_000_000, 1000).intervalMs();
    // 1M keys at the 1s input: the ideal inverse would be sub-ms, so the
    // per-step rate limit lands on half the input, then the floor.
    assertThat(next).isEqualTo(500);
    assertThat(loop.lastScoreBp()).isGreaterThanOrEqualTo(19_000_000);
    for (int i = 0; i < 8; i++) {
      next = loop.onFlushCompleted(1_000_000, next).intervalMs();
    }
    assertThat(next).isEqualTo(50);
  }

  @Test
  void shortWindow_getsProportionallyLittleWeight_inTheRateFilter() {
    ReportFeedLoop loop = loop(Mode.ON, 1000, 10, 10_000, 100);
    loop.onFlushCompleted(1000, 100); // 10 keys/ms at t = 0
    now.set(100);
    ReportFeedLoop.State state = loop.onFlushCompleted(2000, 100); // 20 keys/ms

    // alpha = 1 - e^(-100/250) = 0.32968, so the rate is 10 + 0.32968 × 10 =
    // 13.297 keys/ms — a burst of twice the rate moves the estimate by a third,
    // not by half. The projected batch at 100ms is 1329.7 (13297 bp); the
    // unrounded score keeps the fractional signal, and the exact inverse asks for
    // 75ms.
    assertThat(state.batchSize()).isEqualTo(1330);
    assertThat(state.scoreBp()).isEqualTo(13_297);
    assertThat(state.intervalMs()).isEqualTo(75);
  }

  @Test
  void longCensoredGap_discardsTheStaleEstimate() {
    ReportFeedLoop loop = loop(Mode.ON, 1000, 10, 10_000, 100);
    loop.onFlushCompleted(1000, 100); // 10 keys/ms at t = 0
    // Empty tides are censored: ten seconds pass before the next accepted
    // sample. alpha = 1 - e^(-10000/250) ≈ 1, so the old rate is discarded
    // instead of dominating the first post-idle window.
    now.set(10_000);
    ReportFeedLoop.State state = loop.onFlushCompleted(1, 1);
    assertThat(state.batchSize()).isEqualTo(100); // 1 key/ms × 100ms
    assertThat(state.intervalMs()).isEqualTo(200); // 10x sparse, rate-limited 2x
  }

  @Test
  void scoreInsideTheDeadband_holdsTheInput() {
    // 1020 keys at the 100ms input: 200 bp over the goal, inside ±500.
    assertThat(loop(Mode.ON, 1000, 10, 10_000, 100).onFlushCompleted(1020, 100).intervalMs()).isEqualTo(100);
    // Exactly on the band edge (|500| <= 500) still holds.
    assertThat(loop(Mode.ON, 1000, 10, 10_000, 100).onFlushCompleted(1050, 100).intervalMs()).isEqualTo(100);
  }

  /**
   * An idle window with a target of one must not floor the projection to one: that looks
   * exactly on target and pins the interval to the seed instead of backing off toward the
   * maximum.
   */
  @Test
  void idleWindowWithTargetOne_backsOffInsteadOfLookingOnTarget() {
    ReportFeedLoop loop = loop(Mode.ON, 1, 10, 10_000, 100);

    ReportFeedLoop.State first = loop.onFlushCompleted(0, 100);
    assertThat(first.batchSize()).isZero();
    assertThat(first.scoreBp()).isEqualTo(1);
    assertThat(first.intervalMs()).isEqualTo(200);

    now.set(300);
    assertThat(loop.onFlushCompleted(0, 100).intervalMs()).isEqualTo(400);
  }

  /**
   * A fractional projection just outside the deadband must still move a small target; rounding
   * 4.6 to 5 before scoring makes it look exactly on target and freezes the loop.
   */
  @Test
  void fractionalProjectionOutsideTheDeadbandStillMovesASmallTarget() {
    ReportFeedLoop loop = loop(Mode.ON, 5, 10, 10_000, 100);

    ReportFeedLoop.State state = loop.onFlushCompleted(23, 500);

    assertThat(state.batchSize()).isEqualTo(5);
    assertThat(state.scoreBp()).isEqualTo(9_200);
    assertThat(state.intervalMs()).isEqualTo(109);
  }

  @Test
  void outsideTheDeadband_movesProportionally() {
    ReportFeedLoop loop = loop(Mode.ON, 1000, 10, 10_000, 1000);
    // 1051 keys at 1000ms: 510 bp over — outside the band; the exact inverse
    // asks for 951ms.
    assertThat(loop.onFlushCompleted(1051, 1000).intervalMs()).isEqualTo(951);
  }

  @Test
  void intervalAlwaysStaysWithinThePolicyWindow() {
    ReportFeedLoop loop = loop(Mode.ON, 512, 50, 400, 50);
    for (int batch = 0; batch <= 2048; batch += 137) {
      long next = loop.onFlushCompleted(batch, loop.intervalMs()).intervalMs();
      assertThat(next).isBetween(50L, 400L);
    }
  }

  @Test
  void shadowMode_isComputeOnly_semantics() {
    // The mode flag is opaque to the tuner itself (application is the caller's
    // duty — KeyReporterImpl applies only in ON); pin the enum contract so a
    // rename cannot silently flip the default behavior.
    assertThat(Mode.valueOf("SHADOW")).isNotEqualTo(Mode.ON);
  }

  @Test
  void scoreInsideTheDeadband_returnsLastInputUnchanged() {
    assertThat(ReportFeedLoop.nextInput(50, 10_000, 10)).isEqualTo(50);
    assertThat(ReportFeedLoop.nextInput(1234, 10_000, 1)).isEqualTo(1234);
    assertThat(ReportFeedLoop.nextInput(1000, 10_000, 5000)).isEqualTo(1000);
    assertThat(ReportFeedLoop.nextInput(50, 9_500, 500)).isEqualTo(50);
    assertThat(ReportFeedLoop.nextInput(50, 10_500, 500)).isEqualTo(50);
  }

  @Test
  void underAchieving_growsProportionally() {
    // Half the goal: the ideal inverse is 2x the input, and the rate limit
    // allows exactly that.
    assertThat(ReportFeedLoop.nextInput(50, 5_000, 1)).isEqualTo(100);
    // Quarter of the goal: the ideal is 4x, rate-limited to 2x.
    assertThat(ReportFeedLoop.nextInput(50, 2_500, 1)).isEqualTo(100);
    // Zero/negative scores are treated as 1: rate-limited to 2x.
    assertThat(ReportFeedLoop.nextInput(50, 0, 1)).isEqualTo(100);
    // Just below the goal (2 bp, outside the 1 bp band): the ideal rounds back
    // to the input, so the 1ms minimum step keeps the loop moving (one
    // millisecond, not a stall).
    assertThat(ReportFeedLoop.nextInput(1000, 9_998, 1)).isEqualTo(1001);
  }

  @Test
  void overAchieving_shrinksProportionally() {
    // 1.2x the goal: 100 / 1.2 = 83ms.
    assertThat(ReportFeedLoop.nextInput(100, 12_000, 1)).isEqualTo(83);
    // 1.5x: 67ms.
    assertThat(ReportFeedLoop.nextInput(100, 15_000, 1)).isEqualTo(67);
    // 2x: exactly half the input.
    assertThat(ReportFeedLoop.nextInput(100, 20_000, 1)).isEqualTo(50);
    // 10x: the ideal is a tenth, but one step never crosses 0.5x.
    assertThat(ReportFeedLoop.nextInput(100, 100_000, 1)).isEqualTo(50);
    // Just above the goal (2 bp, outside the 1 bp band): the ideal rounds back
    // to the input, so the 1ms minimum step moves it.
    assertThat(ReportFeedLoop.nextInput(1000, 10_002, 1)).isEqualTo(999);
  }

  @Test
  void negativeScore_isTreatedAsZero() {
    assertThat(ReportFeedLoop.nextInput(50, -5, 1)).isEqualTo(ReportFeedLoop.nextInput(50, 0, 1));
    assertThat(ReportFeedLoop.nextInput(50, Long.MIN_VALUE, 1)).isEqualTo(100);
  }

  @Test
  void hugeInput_saturatesInsteadOfWrapping() {
    long huge = Long.MAX_VALUE / 2;
    // The ideal inverse overflows a long: the double product saturates, the
    // 2x cap holds, and the result stays positive.
    long next = ReportFeedLoop.nextInput(huge, 0, 1);
    assertThat(next).isPositive().isGreaterThan(huge);
    assertThat(ReportFeedLoop.nextInput(Long.MAX_VALUE, 0, 1)).isEqualTo(Long.MAX_VALUE);
    // Over-achieving at the same scale only shrinks proportionally (2/3 of
    // the input), never wraps or jumps below the rate-limit floor.
    long shrunk = ReportFeedLoop.nextInput(huge, 15_000, 7);
    assertThat(shrunk).isPositive().isLessThan(huge).isGreaterThan(huge / 2);
  }

  @Test
  void singleStepChange_isBoundedByTwoX() {
    for (long last = 1; last <= 4096; last *= 2) {
      for (long score = 0; score <= 2_000_000; score += 111_111) {
        long next = ReportFeedLoop.nextInput(last, score, 1);
        assertThat(next).isBetween(1L, Math.max(2 * last, 2L));
        assertThat(next).isGreaterThanOrEqualTo(Math.max(1L, last / 2));
      }
    }
  }

  @Test
  void nextInput_neverReturnsLessThanOne() {
    assertThat(ReportFeedLoop.nextInput(1, 1, 0)).isEqualTo(2);
    assertThat(ReportFeedLoop.nextInput(1, 1_000_000, 0)).isEqualTo(1);
  }
}
