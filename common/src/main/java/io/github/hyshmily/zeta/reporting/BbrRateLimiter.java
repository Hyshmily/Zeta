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
package io.github.hyshmily.zeta.reporting;

/**
 * Damped adaptive rate limiter (BBR-flavored), modeled on the Linux writeback
 * throttle in {@code mm/page-writeback.c}.
 *
 * <p>The concurrency budget is a slow-moving damped baseline (adjusted every
 * 200 ms through a direction gate with step-size decay) scaled by a cubic
 * position ratio around the setpoint. CPU utilisation is a continuous derating
 * signal over a ±20 pp ramp around the configured threshold — not a two-state
 * hard switch.
 */
public interface BbrRateLimiter {
  /** Check whether the current flush cycle is allowed to proceed. */
  boolean tryAcquire();

  /** Record a unit of in-flight work (batch enqueued). */
  void onEnqueue();

  /** Record a successful publish and its round-trip time. */
  void onSuccess(long rtMs);

  /** Record a consumer drop (stale/failed batch). */
  void onConsumerDrop();

  /**
   * Cause of a consumer-side batch drop, used to attribute downstream-yield
   * evidence (kernel {@code bbr_lt_bw} sampling, tcp_bbr.c:659-758).
   *
   * <p>Only {@link #PUBLISH_FAILED} and {@link #STALE} are evidence of
   * downstream saturation: the batch actually reached the publish path and
   * failed there, or waited past the staleness bound under backpressure.
   * {@link #DEAD_TARGET} (the target Worker disappeared) and
   * {@link #LOCAL_REJECTED} (local executor saturation / shutdown drain) are
   * not downstream evidence — lowering the damped baseline for them would
   * react to the wrong bottleneck.
   */
  enum ConsumerDropCause {
    /** Publish threw or timed out — the downstream pipeline is the bottleneck. */
    PUBLISH_FAILED,
    /** Batch waited past the staleness bound in the dispatch queue — backpressure evidence. */
    STALE,
    /** Target Worker no longer alive at consumption time — routing event, not saturation. */
    DEAD_TARGET,
    /** Local rejection (publish executor saturated) or shutdown drain — local, not downstream. */
    LOCAL_REJECTED,
  }

  /**
   * Record a consumer drop with its cause. Causes classified as downstream
   * saturation ({@link ConsumerDropCause#PUBLISH_FAILED},
   * {@link ConsumerDropCause#STALE}) feed the downstream-yield confirmation in
   * {@code BbrRateLimiterImpl}; other causes only record the drop and cooldown.
   *
   * <p>
   * Default delegates to {@link #onConsumerDrop()} (cause ignored) so
   * implementations compiled against earlier releases stay compatible.
   *
   * @param cause why the batch was dropped on the consumer side
   */
  default void onConsumerDrop(ConsumerDropCause cause) {
    onConsumerDrop();
  }

  /**
   * Total number of downstream-yield steps applied to the damped baseline
   * (each step reduces it by 1/8, kernel {@code bbr_lt_bw_ratio}).
   *
   * @return cumulative yield step count, or {@code -1} if not tracked
   */
  default long getTotalDownstreamYields() {
    return -1;
  }

  /** Record a gate drop (tryAcquire failed). */
  void onGateDrop();

  /** Total number of batches that passed the limiter (counted once per completed publish, not per flush cycle). */
  long getTotalPassed();

  /** Total number of batches dropped by the limiter (gate drops + consumer drops, counted per batch). */
  long getTotalDropped();

  /** Current number of in-flight batches. */
  long getInFlight();

  /** Dynamically set the minimum concurrency floor. */
  void setMinInFlight(int count);

  /**
   * Current effective admission budget (damped baseline × position ratio,
   * CPU-derated). This is what {@link #tryAcquire} enforces above the freerun band.
   *
   * <p>
   * <b>Not side-effect-free:</b> the read may consume the rate-limited
   * baseline-update slot (one per {@code BASELINE_INTERVAL_MS}) exactly as
   * {@link #tryAcquire} does — the port folds its lazy {@code dirty_ratelimit}
   * update into the shared read path. Under active traffic the control loop
   * wins the slot and scraping is a no-op; scraping an idle limiter lets the
   * estimate decay as designed. For a guaranteed-passive curve use
   * {@link #getBalancedInFlight()}.
   */
  long getCurrentMaxInFlight();

  /** Current damped budget baseline (kernel {@code wb->dirty_ratelimit} analog), for observability. */
  long getBalancedInFlight();

  /**
   * Current sliding-window max pass per bucket, for observability.
   *
   * <p>
   * <b>Not side-effect-free:</b> reads lazily smooth/decay the cached
   * maxPass estimate (kernel-style decay-on-read — the mechanism that loosens
   * the estimate under sustained gate drops), so scrape frequency is a minor
   * input to the estimate trajectory.
   */
  long getCurrentMaxPass();

  /**
   * Current sliding-window min average RT in ms, for observability.
   *
   * <p>
   * Reads lazily smooth the cached minRt estimate (decay-on-read, see
   * {@link #getCurrentMaxPass()}).
   */
  long getCurrentMinRt();
}
