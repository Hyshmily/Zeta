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

import io.github.hyshmily.zeta.hotkeydetector.doublebuffer.WaveCounter;

/**
 * Periodically aggregates per-key access counts and publishes them
 * to the Worker via {@link ReportPublisher}.
 *
 * <p>Uses a {@link WaveCounter} as a temporary counter store.
 * Burst absorption and backpressure are provided by a bounded
 * {@link java.util.concurrent.LinkedBlockingQueue} between the flush callback and the
 * RabbitMQ publisher.
 */
public interface KeyReporter {
  /**
   * Record one access for the given cache key.
   *
   * @param cacheKey the accessed key
   */
  @SuppressWarnings("all")
  void reportToWorker(String cacheKey);

  /**
   * Start the periodic flush scheduler and the reportToWorker dispatcher.
   * Idempotent — subsequent calls are silently ignored.
   */
  void start();

  /**
   * Gracefully shut down the reportToWorker dispatcher.
   * Idempotent — safe to call multiple times.
   */
  void stop();

  /**
   * Return the current number of batches waiting in the dispatcher work queue.
   *
   * @return queue depth, or {@code -1} if the dispatcher has not been started
   */
  int dispatcherDepth();

  /**
   * Return the maximum capacity of the dispatcher work queue.
   *
   * @return queue capacity, or {@code -1} if the dispatcher has not been started
   */
  int dispatcherCapacity();

  /**
   * Return the total number of batches discarded by the consumer — this
   * counter <b>conflates two discard causes</b>: batches whose target Worker
   * was no longer alive at consumption time, and batches that waited longer
   * than 5 seconds in the dispatcher queue (staleness expiry). The two
   * halves are exposed separately by {@link #dispatcherExpiredDeadTarget()}
   * and {@link #dispatcherExpiredStale()}; this sum is kept for metric
   * continuity.
   *
   * @return total expired count since startup, or {@code -1} if the dispatcher has not been started
   */
  long dispatcherExpired();

  /**
   * Return the total number of discarded batches whose target Worker was no
   * longer alive at consumption time (the worker-partition stall signal
   * behind {@code zeta.stall.worker_partition.stopped}).
   *
   * @return total dead-target discard count since startup, or {@code -1} if the dispatcher has not been started
   */
  long dispatcherExpiredDeadTarget();

  /**
   * Return the total number of batches discarded because they waited longer
   * than 5 seconds in the dispatcher queue (staleness expiry under
   * backpressure — the report-backpressure stall signal).
   *
   * @return total stale-discard count since startup, or {@code -1} if the dispatcher has not been started
   */
  long dispatcherExpiredStale();

  /**
   * Return the total number of batches rejected because the dispatcher queue was full.
   *
   * @return total dropped count since startup, or {@code -1} if the dispatcher has not been started
   */
  long dispatcherDropped();

  /**
   * Return the approximate number of unique keys currently buffered.
   *
   * @return estimated number of unique keys with pending access counts
   */
  long getPendingKeyCount();

  /**
   * Return the total number of flush cycles permitted by the BBR rate limiter.
   *
   * @return total passed count, or {@code -1} if BBR is disabled
   */
  long bbrPassed();

  /**
   * Return the total number of flush cycles dropped by the BBR rate limiter.
   *
   * @return total dropped count, or {@code -1} if BBR is disabled
   */
  long bbrDropped();

  /**
   * Return the current number of in-flight batches tracked by the BBR rate limiter.
   *
   * @return current in-flight count, or {@code -1} if BBR is disabled
   */
  long bbrInFlight();

  /**
   * Return the BBR-computed maximum concurrency budget (max in-flight).
   *
   * @return computed max in-flight, or {@code -1} if BBR is disabled
   */
  long bbrMaxInFlight();

  /**
   * Return the current damped budget baseline of the BBR rate limiter — the
   * slow-moving budget that tracks the Little-Law estimate through a direction
   * gate and step-size decay (kernel {@code wb->dirty_ratelimit} analog).
   *
   * <p>Compare with {@link #bbrMaxInFlight()}: the budget is the baseline scaled
   * by the CPU-derated position ratio, so the two curves together show how much
   * of the current budget comes from measured throughput versus position/CPU
   * pressure.
   *
   * @return current damped baseline, or {@code -1} if BBR is disabled
   */
  long bbrBalancedInFlight();

  /**
   * Whether the ADR-0078 feed-loop interval tuner is configured for the flush
   * cadence ({@code zeta.local.report-interval-tuning} != off).
   *
   * @return {@code true} when the tuner exists (shadow or apply mode)
   */
  default boolean feedLoopEnabled() {
    return false;
  }

  /**
   * Return the feed-loop's current base flush interval in ms — in shadow mode
   * this is the trajectory the tuner <em>would</em> apply (kernel
   * {@code damon_feed_loop_next_input} output), in apply mode the live
   * WaveCounter tide base.
   *
   * @return current base interval, or {@code -1} if the tuner is disabled
   */
  default long feedLoopIntervalMs() {
    return -1;
  }

  /**
   * Return the last feed-loop score in basis points of the goal — the
   * two-window-averaged batch size against
   * {@code zeta.local.report-interval-target-batch} ({@code 10000} == on
   * target).
   *
   * @return last score in bp, or {@code -1} if the tuner is disabled or no
   *         flush has been sampled yet
   */
  default long feedLoopScoreBp() {
    return -1;
  }

  /**
   * Return the last two-window-averaged batch size (distinct keys per
   * completed flush) the tuner scored.
   *
   * @return last averaged batch size, or {@code -1} if the tuner is disabled
   *         or no flush has been sampled yet
   */
  default long feedLoopBatchSize() {
    return -1;
  }
}
