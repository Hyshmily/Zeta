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
package io.github.hyshmily.zeta.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link FeedLoop}, the {@code damon_feed_loop_next_input} port.
 *
 * <p>
 * Expected values are hand-computed from the kernel formula
 * ({@code mm/damon/core.c:2751-2780}): {@code next = last ± last × |score − goal| / goal},
 * with the {@code score ≥ 2 × goal} cliff landing on {@code minInput}.
 */
class FeedLoopTest {

  @Test
  void scoreOnTarget_returnsLastInputUnchanged() {
    assertThat(FeedLoop.nextInput(50, 10_000, 10)).isEqualTo(50);
    assertThat(FeedLoop.nextInput(1234, 10_000, 1)).isEqualTo(1234);
    assertThat(FeedLoop.nextInput(1000, 10_000, 5000)).isEqualTo(1000);
  }

  @Test
  void underAchieving_growsProportionally() {
    // half the goal: comp = 50 * 5000 / 10000 = 25 -> 75
    assertThat(FeedLoop.nextInput(50, 5_000, 1)).isEqualTo(75);
    // quarter of the goal: comp = 50 * 7500 / 10000 = 37 (integer division) -> 87
    assertThat(FeedLoop.nextInput(50, 2_500, 1)).isEqualTo(87);
    // zero score: comp = last -> doubles
    assertThat(FeedLoop.nextInput(50, 0, 1)).isEqualTo(100);
    // just below the goal: tiny step
    assertThat(FeedLoop.nextInput(50, 9_999, 1)).isEqualTo(50); // comp = 50*1/10000 = 0
  }

  @Test
  void overAchieving_shrinksProportionally() {
    // 1.5x the goal: comp = 50 * 5000 / 10000 = 25 -> 25
    assertThat(FeedLoop.nextInput(50, 15_000, 1)).isEqualTo(25);
    // 1.2x: comp = 100 * 2000 / 10000 = 20 -> 80
    assertThat(FeedLoop.nextInput(100, 12_000, 1)).isEqualTo(80);
    // just above the goal: comp = 0 -> unchanged
    assertThat(FeedLoop.nextInput(50, 10_001, 1)).isEqualTo(50);
    // one step below the overshoot cliff: comp = 50 * 9999 / 10000 = 49 -> 1
    assertThat(FeedLoop.nextInput(50, 19_999, 1)).isEqualTo(1);
  }

  @Test
  void overshootCliff_landsOnMinInput() {
    assertThat(FeedLoop.nextInput(50, 20_000, 10)).isEqualTo(10);
    assertThat(FeedLoop.nextInput(500, 999_999, 10)).isEqualTo(10);
    assertThat(FeedLoop.nextInput(100_000, 1_000_000, 25)).isEqualTo(25);
  }

  @Test
  void minInput_floorsOverAchievingShrink() {
    // 80 above the floor: unchanged by the floor
    assertThat(FeedLoop.nextInput(100, 12_000, 60)).isEqualTo(80);
    // exactly at the floor
    assertThat(FeedLoop.nextInput(100, 14_000, 60)).isEqualTo(60);
    // below the floor: floored
    assertThat(FeedLoop.nextInput(100, 16_000, 60)).isEqualTo(60);
  }

  @Test
  void negativeScore_isTreatedAsZero() {
    assertThat(FeedLoop.nextInput(50, -5, 1)).isEqualTo(FeedLoop.nextInput(50, 0, 1));
    assertThat(FeedLoop.nextInput(50, Long.MIN_VALUE, 1)).isEqualTo(100);
  }

  @Test
  void hugeInput_overflowGuardPreventsWraparound() {
    // Direct product would overflow: lastInput * diff > Long.MAX_VALUE.
    // The guarded branch reorders to (last / goal) * diff — result stays positive.
    long huge = Long.MAX_VALUE / 2;
    long next = FeedLoop.nextInput(huge, 0, 1);
    assertThat(next).isPositive().isGreaterThan(huge);
    // Saturation guard: last + compensation would exceed Long.MAX_VALUE.
    assertThat(FeedLoop.nextInput(Long.MAX_VALUE, 0, 1)).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void hugeInput_overAchieving_shrinksSanely() {
    long huge = Long.MAX_VALUE / 2;
    // Below the cliff the compensation is a FRACTION of the input (score 15000
    // -> diff = goal/2 -> comp ~= last/2 via the reordered product), so the
    // result stays positive and strictly below lastInput — no wraparound.
    long next = FeedLoop.nextInput(huge, 15_000, 7);
    assertThat(next).isPositive().isLessThan(huge);
    // At the cliff the kernel resets straight to minInput regardless of size.
    assertThat(FeedLoop.nextInput(huge, 20_000, 7)).isEqualTo(7);
  }

  /**
   * Cross-checks against the kernel's own consumption pattern
   * ({@code kdamond_tune_intervals}): adaptation_bp = nextInput(100_000_000,
   * score) / 10_000, then [1, 10_000] rescaled to [5_000, 10_000] via
   * {@code 5000 + bp/2}. These pin the exact values the rescaling relies on.
   */
  @Test
  void kernelAdaptationBp_nominalInput() {
    long nominal = 100_000_000;
    // score = 5000 (half goal): comp = 5e7 -> 1.5e8 -> bp 15000 -> sample x1.5
    assertThat(FeedLoop.nextInput(nominal, 5_000, 10_000)).isEqualTo(150_000_000);
    // score = 15000 (1.5x goal): comp = 5e7 -> 5e7 -> bp 5000 -> remap 7500 -> sample x0.75
    assertThat(FeedLoop.nextInput(nominal, 15_000, 10_000)).isEqualTo(50_000_000);
    // score >= 2x goal: min_input -> bp 1 -> remap 5000 -> sample x0.5
    assertThat(FeedLoop.nextInput(nominal, 20_000, 10_000)).isEqualTo(10_000);
  }

  @Test
  void singleStepChange_isBoundedByTwoX_belowTheCliff() {
    // The kernel's effective [0.5x, 2x] per-step bound: compensation is at most
    // lastInput in either direction (score in (0, 2x goal)).
    for (long last = 1; last <= 4096; last *= 2) {
      for (long score = 0; score <= 19_999; score += 1_111) {
        long next = FeedLoop.nextInput(last, score, 1);
        assertThat(next).isBetween(1L, Math.max(2 * last, 2L));
      }
    }
  }
}
