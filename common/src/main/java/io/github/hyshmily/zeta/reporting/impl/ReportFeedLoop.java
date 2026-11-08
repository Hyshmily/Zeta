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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import java.util.function.LongSupplier;
import org.springframework.util.Assert;

/**
 * Feed-loop interval tuner for the reporter flush cadence (design decisions: reporting): each
 * completed flush is scored into the next flush interval, with
 * {@code report-interval-target-batch} keys per window as the target.
 *
 * <p>The plant input is the flush interval in ms; the observable is the arrival rate
 * {@code batchSize / windowMs}. The score is that rate projected onto the interval in force for
 * the measured window, in basis points against the target ({@link #GOAL_BP} is on target).
 * {@link Mode#SHADOW} computes and exposes every value and applies nothing; {@link Mode#ON}
 * hands each interval to the caller, which applies it to the tide base.
 *
 * <p>Thread-safety: {@link #onFlushCompleted} is synchronized; the published values are one
 * immutable {@link State} behind a single volatile write, so every getter reads one coherent
 * snapshot and a caller that needs the interval, batch and score together uses the returned
 * {@link State}.
 */
@Internal
public final class ReportFeedLoop {

  /** Tuning mode: compute-only ({@code SHADOW}, default) or apply ({@code ON}). */
  public enum Mode {
    /** Compute and expose the trajectory; never apply (deploy-first default). */
    SHADOW,

    /** Apply each computed interval to the WaveCounter tide base. */
    ON,
  }

  /**
   * Low-pass time constant of the arrival-rate estimate, in ms: a sample is weighted by
   * {@code 1 − e^(−Δt/τ)}.
   */
  private static final double RATE_TAU_MS = 250.0;

  /** Deadband around {@link #GOAL_BP}: a score inside it holds the input unchanged (500 bp = ±5%). */
  private static final long DEADBAND_BP = 500L;

  /** The score of a flush whose projected batch equals the target: {@code 10000} basis points. */
  private static final long GOAL_BP = 10_000L;

  /** Minimum move outside the deadband, so millisecond truncation cannot stall the loop. */
  private static final long MIN_STEP_MS = 1L;

  /**
   * One atomically published observation plus the decision it produced.
   *
   * @param intervalMs the base interval the loop has chosen for the next
   *                   window (what {@link Mode#ON} applies)
   * @param batchSize  the smoothed arrival rate projected onto the interval
   *                   that was in force for the measured window — the evidence
   *                   the decision was made from, not the next interval's
   *                   projection. In apply mode this equals the observed
   *                   batch's smoothed equivalent.
   * @param scoreBp    {@code batchSize} against the target in bp
   *                   ({@code -1} before the first sample)
   */
  public record State(long intervalMs, long batchSize, long scoreBp) {}

  private final long minIntervalMs;
  private final long maxIntervalMs;
  private final LongSupplier clock;
  private final long targetBatch;
  private final Mode mode;

  /** The single atomically published view (interval, projected batch, score). */
  private volatile State state;

  /** Smoothed arrival rate in keys/ms; negative = no sample yet. */
  @GuardedBy("this")
  private double ratePerMs = -1.0;

  /** Monotonic time of the last accepted sample. */
  @GuardedBy("this")
  private long lastSampleAtMs;

  /**
   * Create a tuner.
   *
   * @param mode            {@link Mode#SHADOW} computes only, {@link Mode#ON}
   *                        applies
   * @param targetBatch     goal keys per flush ({@code N}, must be positive)
   * @param minIntervalMs   interval floor (scheduler cadence bound, positive)
   * @param maxIntervalMs   interval ceiling (detection-lag bound), ≥
   *                        {@code minIntervalMs}
   * @param seedIntervalMs  initial interval (the configured
   *                        {@code report-interval-ms}), clamped into
   *                        {@code [min, max]}
   */
  public ReportFeedLoop(Mode mode, long targetBatch, long minIntervalMs, long maxIntervalMs, long seedIntervalMs) {
    this(mode, targetBatch, minIntervalMs, maxIntervalMs, seedIntervalMs, TimeSource::monotonicMillis);
  }

  /** Test seam over the monotonic clock (the low-pass weight is wall-clock based). */
  ReportFeedLoop(
    Mode mode,
    long targetBatch,
    long minIntervalMs,
    long maxIntervalMs,
    long seedIntervalMs,
    LongSupplier clock
  ) {
    Assert.notNull(mode, "mode must not be null");
    Assert.isTrue(targetBatch > 0, "report-interval-target-batch must be positive");
    Assert.isTrue(minIntervalMs > 0, "report-interval-min-ms must be positive");
    Assert.isTrue(maxIntervalMs >= minIntervalMs, "report-interval-max-ms must be >= report-interval-min-ms");
    Assert.notNull(clock, "clock must not be null");
    this.mode = mode;
    this.clock = clock;
    this.targetBatch = targetBatch;
    this.minIntervalMs = minIntervalMs;
    this.maxIntervalMs = maxIntervalMs;
    long seed = Math.min(Math.max(seedIntervalMs, minIntervalMs), maxIntervalMs);
    this.state = new State(seed, -1, -1);
  }

  /**
   * Feed one completed flush and compute the next base interval.
   *
   * <p>Call exactly once per flush that happened. An empty snapshot is a
   * zero-rate observation and is sampled with {@code batchSize = 0}; censored
   * cycles (no alive Worker) must return upstream before this
   * point. The caller guarantees {@code batchSize >= 0} and
   * {@code windowMs > 0} (the contract lives on
   * {@code KeyReporter#publishBatch}).
   *
   * @param batchSize distinct keys delivered by the flush; {@code 0} for an empty window
   * @param windowMs  measured monotonic window the batch accumulated over
   * @return the next atomically published state
   */
  public synchronized State onFlushCompleted(int batchSize, long windowMs) {
    long now = clock.getAsLong();
    double sampleRate = (double) batchSize / windowMs;

    if (ratePerMs < 0.0) {
      ratePerMs = sampleRate;
    } else {
      // Wall-clock since the last sample, not just this window: a long delivery-censored
      // gap must age the old estimate out instead of letting it dominate the first sample
      // after the gap.
      long gapMs = Math.max(windowMs, now - lastSampleAtMs);
      double alpha = -Math.expm1(-(double) gapMs / RATE_TAU_MS);
      ratePerMs += alpha * (sampleRate - ratePerMs);
    }
    lastSampleAtMs = now;

    State previous = state;
    // The evidence belongs to the input that was in force for the measured
    // window; the published interval is the decision that evidence produced.
    // Score the unrounded projection: rounding before the ratio loses the whole
    // signal for a small target (4.6 vs 5 with target 5 looks exact).
    double rawProjected = ratePerMs * previous.intervalMs();
    long projected = Math.round(rawProjected);
    long scoreBp = scoreBp(rawProjected);
    long next = nextInput(previous.intervalMs(), scoreBp, DEADBAND_BP);
    next = Math.min(Math.max(next, minIntervalMs), maxIntervalMs);

    State nextState = new State(next, projected, scoreBp);
    state = nextState;
    return nextState;
  }

  /** Return the tuning mode. */
  public Mode mode() {
    return mode;
  }

  /** Return the current base interval in ms (what {@link Mode#ON} would apply). */
  public long intervalMs() {
    return state.intervalMs();
  }

  /**
   * Return the projected batch at the interval in force for the measured window ({@code -1} before
   * the first sample).
   */
  public long projectedBatchSize() {
    return state.batchSize();
  }

  /** Return the last computed score in bp ({@code -1} before the first sample). */
  public long lastScoreBp() {
    return state.scoreBp();
  }

  /**
   * Projected batch against the target in bp, computed from the unrounded projection so a
   * fractional batch still moves the loop, saturated at {@code Long.MAX_VALUE}. A zero
   * projection (an idle window) scores the floor, which pushes the cadence toward
   * {@code maxIntervalMs} instead of looking exactly on target.
   */
  private long scoreBp(double projectedBatch) {
    return Math.max(1L, Math.round((projectedBatch * (double) GOAL_BP) / targetBatch));
  }

  /**
   * Next plant input from the previous one and the observed score — the
   * {@code damon_feed_loop_next_input} port ({@code mm/damon/core.c}),
   * rewritten as the plant's exact inverse.
   *
   * <p>
   * Scores are basis points against {@link #GOAL_BP}. Inside
   * {@code ±deadbandBp} the input holds. Outside it, {@code last × goal / score}
   * is the interval at which the projected batch would equal the target, so a
   * sparse and a dense window move the input by the same law and there is no
   * overshoot cliff. The move is rate limited to {@code [0.5x, 2x]} per step
   * (one noisy window cannot vault the cadence), and a move that rounds back to
   * {@code lastInput} is forced to at least {@link #MIN_STEP_MS} in the error
   * direction so integer truncation cannot stall the loop. The floor is the
   * caller's policy; this function never returns less than 1.
   *
   * @param lastInput   previous input in ms, positive
   * @param scoreBp     observed score in bp; non-positive is treated as 1
   * @param deadbandBp  hold band around {@link #GOAL_BP}, non-negative
   * @return the next input, saturated at {@code Long.MAX_VALUE}
   */
  static long nextInput(long lastInput, long scoreBp, long deadbandBp) {
    long score = Math.max(1L, scoreBp);
    if (Math.abs(score - GOAL_BP) <= deadbandBp) {
      return lastInput;
    }

    long ideal = Math.round(((double) lastInput * GOAL_BP) / score);
    long lower = Math.max(1L, lastInput >> 1);
    long upper = lastInput > (Long.MAX_VALUE >> 1) ? Long.MAX_VALUE : lastInput << 1;
    long next = Math.min(Math.max(ideal, lower), upper);

    if (next == lastInput) {
      // Integer truncation rounded the ideal move back to the input: force the
      // smallest legal step in the error's direction. The upward branch saturates
      // instead of overflowing at Long.MAX_VALUE.
      if (score > GOAL_BP) {
        next = Math.max(1L, lastInput - MIN_STEP_MS);
      } else if (lastInput < Long.MAX_VALUE) {
        next = lastInput + MIN_STEP_MS;
      }
    }
    return Math.max(1L, next);
  }
}
