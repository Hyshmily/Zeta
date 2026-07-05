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
package io.github.hyshmily.hotkey.reporting;

/**
 * CPU‑driven BBR (Bottleneck Bandwidth and Round-trip) adaptive rate limiter.
 *
 * <p>BBR computes a dynamic concurrency budget (max in-flight batches) from
 * a sliding-window measurement of peak throughput and minimum request
 * round-trip time.  CPU utilisation is used as an additional pressure signal
 * — when CPU crosses the configured threshold, the limiter stops granting
 * capacity above the current in-flight floor (the number of active Worker
 * nodes), preventing overload from driving latency higher.
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

  /** Total number of flush cycles that passed the limiter. */
  long getTotalPassed();

  /** Total number of flush cycles dropped by the limiter. */
  long getTotalDropped();

  /** Current number of in-flight batches. */
  long getInFlight();

  /** Dynamically set the minimum concurrency floor. */
  void setMinInFlight(int count);

  /** Current computed max concurrency budget. */
  long getCurrentMaxInFlight();
}
