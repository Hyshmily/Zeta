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

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.util.FeedLoop;
import org.springframework.util.Assert;

/**
 * Feed-loop interval tuner for the reporter flush cadence (ADR-0078) — the
 * {@code damon_feed_loop_next_input} controller applied to "how often should
 * this instance flush its report batches".
 *
 * <p>
 * <b>Plant and score.</b> The input is the flush interval (the WaveCounter
 * tide-loop base); the score is the delivered batch size against the target
 * {@code N} keys per flush ({@code zeta.local.report-interval-target-batch}),
 * normalized to basis points so the goal reads {@code 10000}. Sparse batches
 * lengthen the interval (fewer, fuller AMQP messages — the per-message
 * overhead dominates at low traffic); dense batches shorten it (message size
 * and detection lag stay bounded at high traffic). The plant is convergent by
 * construction: interval ↑ → batch ↑ → score ↑, so a negative feedback step
 * always moves the score back toward the goal.
 *
 * <p>
 * <b>Noise handling.</b> Two refinements the kernel applies to its own loop:
 * the score is the <em>two-window average</em> of the last two completed batch
 * sizes (raw per-window scores on bursty traffic would oscillate the
 * interval), and the result is clamped into {@code [minIntervalMs,
 * maxIntervalMs]} — the ceiling doubles as the detection-lag bound, the floor
 * keeps the scheduler's minimum cadence. The feed function's own single-step
 * change is bounded by construction ({@code ≤ 2x} below the overshoot cliff),
 * which is the kernel's effective {@code [0.5x, 2x]} step limit.
 *
 * <p>
 * <b>Censoring is the caller's duty.</b> Only flushes that actually happened
 * may be fed here: empty snapshots, no-alive-Worker drops and BBR gate drops
 * must return before sampling (the kernel has no analog — its sampler always
 * completes its round). Feeding a censored cycle would teach the loop from
 * data that never reached the broker.
 *
 * <p>
 * <b>Shadow mode.</b> {@link Mode#SHADOW} computes and exposes everything
 * (interval trajectory, score, batch size) but the caller must not apply the
 * result — the gauge {@code zeta.reporter.feedloop.interval} shows on real
 * traffic what {@link Mode#ON} would do, before the operator flips the switch.
 *
 * <p>
 * Thread-safety: the sampling state is monitor-guarded (one call per flush;
 * the flush callback may run on a shared scheduler pool). Gauges read through
 * the same monitor or volatile fields.
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

  /** Goal score in bp — batch sizes are normalized so on-target reads this. */
  private static final long GOAL_BP = 10_000L;

  private final Mode mode;
  private final long targetBatch;
  private final long minIntervalMs;
  private final long maxIntervalMs;

  /** lastInput mirror: the base interval the NEXT batch accumulates under. */
  private long intervalMs;

  /** Previous window's batch size; {@code -1} = no completed window yet. */
  private long prevBatch = -1;

  /** Last two-window-averaged batch size ({@code -1} until the first sample). */
  private long lastAvgBatch = -1;

  /** Last computed score in bp ({@code -1} until the first sample). */
  private volatile long lastScoreBp = -1;

  /**
   * Create a tuner.
   *
   * @param mode            {@link Mode#SHADOW} computes only, {@link Mode#ON}
   *                        applies
   * @param targetBatch     goal keys per flush ({@code N}, must be positive)
   * @param minIntervalMs   interval floor (scheduler cadence bound)
   * @param maxIntervalMs   interval ceiling (detection-lag bound), ≥
   *                        {@code minIntervalMs}
   * @param seedIntervalMs  initial interval (the configured
   *                        {@code report-interval-ms}), clamped into
   *                        {@code [min, max]}
   */
  public ReportFeedLoop(Mode mode, long targetBatch, long minIntervalMs, long maxIntervalMs, long seedIntervalMs) {
    Assert.notNull(mode, "mode must not be null");
    Assert.isTrue(targetBatch > 0, "report-interval-target-batch must be positive");
    Assert.isTrue(minIntervalMs > 0, "report-interval-min-ms must be positive");
    Assert.isTrue(maxIntervalMs >= minIntervalMs, "report-interval-max-ms must be >= report-interval-min-ms");
    this.mode = mode;
    this.targetBatch = targetBatch;
    this.minIntervalMs = minIntervalMs;
    this.maxIntervalMs = maxIntervalMs;
    this.intervalMs = Math.min(Math.max(seedIntervalMs, minIntervalMs), maxIntervalMs);
  }

  /**
   * Feed one completed flush and compute the next base interval.
   *
   * <p>
   * Call exactly once per flush that actually reached the routing stage —
   * censored cycles (empty snapshot, no alive Worker, BBR gate drop) must
   * return upstream before this point.
   *
   * @param batchSize distinct keys delivered by the flush
   * @return the next base interval in ms, clamped into
   *         {@code [minIntervalMs, maxIntervalMs]}
   */
  public synchronized long onFlushCompleted(int batchSize) {
    // Two-window average (kernel's noise filter): a single burst window must
    // not move the interval; the first sample has no previous window.
    long avg = prevBatch < 0 ? batchSize : (prevBatch + batchSize) >> 1;
    prevBatch = batchSize;
    lastAvgBatch = avg;

    // score = avg / targetBatch in bp; targetBatch >= 1 and avg is bounded by
    // the reservoir cap (~100k), so the product cannot overflow.
    long scoreBp = (avg * GOAL_BP) / targetBatch;
    lastScoreBp = scoreBp;

    long next = FeedLoop.nextInput(intervalMs, scoreBp, minIntervalMs);
    // Caller-policy window: the feed function floors at minInput already, but
    // the ceiling (detection-lag bound) lives here.
    next = Math.min(Math.max(next, minIntervalMs), maxIntervalMs);
    intervalMs = next;
    return next;
  }

  /** Return the tuning mode. */
  public Mode mode() {
    return mode;
  }

  /** Return the current base interval in ms (what {@link Mode#ON} would apply). */
  public synchronized long intervalMs() {
    return intervalMs;
  }

  /** Return the last two-window-averaged batch size ({@code -1} before the first sample). */
  public synchronized long lastAvgBatchSize() {
    return lastAvgBatch;
  }

  /** Return the last computed score in bp ({@code -1} before the first sample). */
  public long lastScoreBp() {
    return lastScoreBp;
  }
}
