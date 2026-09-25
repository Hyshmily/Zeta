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

import io.github.hyshmily.zeta.Internal;

/**
 * Port of the kernel's monitoring-intervals feedback loop
 * ({@code damon_feed_loop_next_input}, {@code mm/damon/core.c}) — the 20-line
 * controller DAMON uses to answer "how intense should monitoring be".
 *
 * <p>
 * The loop assumes the plant input and the observed score are positively
 * related and steers the input toward a target score. Both are measured in
 * basis points against the goal: {@code score = 10000} means exactly on
 * target, {@code score < 10000} under-achieving, {@code score > 10000}
 * over-achieving. Per adjustment the input moves proportionally to its
 * relative distance from the goal — the further off target, the larger the
 * compensation; on target it does not move at all:
 *
 * <ul>
 * <li>{@code score == goal} → unchanged (the loop's fixed point);</li>
 * <li>under-achieving → {@code last + last × (goal − score) / goal}
 * (input grows, so the observed score grows back toward the goal);</li>
 * <li>over-achieving → {@code max(last − last × (score − goal) / goal, minInput)}</li>
 * <li>{@code score ≥ 2 × goal} → {@code minInput} directly (the overshoot
 * cliff: compensation would exceed the whole input, so the kernel resets to
 * the floor instead of dividing by a vanishing remainder).</li>
 * </ul>
 *
 * <p>
 * {@code minInput} keeps the loop alive: with a tiny {@code lastInput} the
 * compensation itself degenerates to zero and the loop stalls. The kernel
 * hardcodes {@code min_input = 10000} — a <em>microsecond</em> floor for its
 * sampling interval. Zeta callers pass intervals in milliseconds and must
 * re-derive the floor from their own scheduler bound (the kernel value taken
 * literally would be a 10-second floor) — hence it is a parameter here, not a
 * constant.
 *
 * <p>
 * The kernel additionally re-scales each adjustment via
 * {@code kdamond_tune_intervals} (adaptation bp clamped so a single step moves
 * the interval by at most {@code [0.5x, 2x]}). The single-step change of this
 * function is already bounded by construction — the compensation is at most
 * {@code lastInput} in either direction, except at the overshoot cliff which
 * lands exactly on {@code minInput} — so callers get the kernel's effective
 * bound by clamping the result into their own {@code [min, max]} window.
 *
 * <p>
 * Pure function, no state, no clock — unit-testable against hand-computed
 * kernel values (see {@code FeedLoopTest}).
 */
@Internal
public final class FeedLoop {

  /** Goal score in bp; scores are normalized so on-target reads exactly this. */
  private static final long GOAL = 10_000;

  private FeedLoop() {}

  /**
   * Compute the next plant input from the previous one and the observed score.
   *
   * @param lastInput previous input (same unit the caller wants back — ms for
   *                  Zeta intervals); must be positive
   * @param scoreBp   observed score in basis points of the goal ({@code 10000}
   *                  == on target); negative scores are treated as {@code 0}
   *                  (maximum stretch) rather than corrupting the compensation
   * @param minInput  floor for the returned input (see the class Javadoc for
   *                  why this is a parameter, not a constant)
   * @return the next input, never below {@code minInput}, never above
   *         {@code Long.MAX_VALUE}
   */
  public static long nextInput(long lastInput, long scoreBp, long minInput) {
    // (defensive-normalize): kernel inputs are unsigned; a negative score is a
    // caller bug (counts are non-negative). Clamping to 0 gives the maximum
    // stretch response — the sane degradation — instead of a compensation
    // larger than the goal itself.
    long score = Math.max(0, scoreBp);

    if (score == GOAL) {
      return lastInput;
    }
    if (score >= GOAL << 1) {
      return minInput;
    }

    boolean overAchieving = score > GOAL;
    long scoreGoalDiff = overAchieving ? score - GOAL : GOAL - score;

    // compensation = lastInput * scoreGoalDiff / goal, overflow-guarded exactly
    // like the kernel: reorder to (lastInput / goal) * diff only when the
    // direct product would wrap (kernel core.c:2770-2773).
    long compensation;
    if (lastInput < Long.MAX_VALUE / scoreGoalDiff) {
      compensation = (lastInput * scoreGoalDiff) / GOAL;
    } else {
      compensation = (lastInput / GOAL) * scoreGoalDiff;
    }

    if (overAchieving) {
      return Math.max(lastInput - compensation, minInput);
    }
    // Kernel: last_input + compensation with an ULONG_MAX saturation guard
    // (core.c:2777-2779).
    return lastInput > Long.MAX_VALUE - compensation ? Long.MAX_VALUE : lastInput + compensation;
  }
}
