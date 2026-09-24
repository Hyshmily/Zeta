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
   */
  long getCurrentMaxInFlight();

  /** Current damped budget baseline (kernel {@code wb->dirty_ratelimit} analog), for observability. */
  long getBalancedInFlight();

  /** Current sliding-window max pass per bucket, for observability. */
  long getCurrentMaxPass();

  /** Current sliding-window min average RT in ms, for observability. */
  long getCurrentMinRt();
}
