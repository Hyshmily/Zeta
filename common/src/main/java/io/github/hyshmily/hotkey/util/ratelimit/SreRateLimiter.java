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
package io.github.hyshmily.hotkey.util.ratelimit;

import io.github.hyshmily.hotkey.util.window.RollingWindow;

/**
 * Google SRE-inspired adaptive rate limiter.
 *
 * <p>Uses the formula {@code requests = K * accepts} where
 * {@code K = 1 / successThreshold}.  If the total number of requests in
 * the window is below the computed budget the request is let through;
 * otherwise it is dropped with probability {@code (total - requests) / (total + 1)}.
 *
 * <p>Internally tracks two sliding windows — {@code success} and {@code total}
 * — driven by {@link RollingWindow}.  The caller reports outcomes via
 * {@link #onSuccess()} and {@link #onFailed()}.
 */
public class SreRateLimiter {

  private final RollingWindow successWindow;
  private final RollingWindow totalWindow;
  private final double k;
  private final int minSamples;

  /**
   * Creates an SRE adaptive rate limiter.
   *
   * @param windowMs   duration of the sliding window in milliseconds
   * @param buckets    number of buckets within the sliding window
   * @param k          multiplier derived from 1 / successThreshold; higher values relax the limit
   * @param minSamples minimum number of total requests in the window before the limiter activates
   */
  public SreRateLimiter(long windowMs, int buckets, double k, int minSamples) {
    this.successWindow = new RollingWindow(buckets, windowMs);
    this.totalWindow = new RollingWindow(buckets, windowMs);
    this.k = k;
    this.minSamples = minSamples;
  }

  /**
   * Check whether the current request should be allowed.
   * <p>
   * This method is read-only — it does not modify window state.
   *
   * @return {@code true} if the request is within the adaptive budget
   */
  public boolean tryAcquire() {
    long total = totalWindow.sum();
    if (total < minSamples) {
        return true;
    }

    long maxRequests = (long) (k * successWindow.sum());
    // Guard against degenerate rollover
    if (maxRequests < 0) {
        return true;
    }

    if (total < maxRequests) {
        return true;
    }

    // Probabilistic drop
    return !trueOnProbability((double) (total - maxRequests) / (total + 1));
  }

  /** Record a successful request. */
  public void onSuccess() {
    successWindow.add(1);
    totalWindow.add(1);
  }

  /**
   * Record a failed request outcome.
   *
   * <p>Increments only the total window, not the success window. Must be called
   * after a request that passed {@link #tryAcquire()} completed with a failure
   * (e.g. timeout, error response). This reduces the effective success rate and
   * tightens the adaptive budget for subsequent requests.
    */
  public void onFailed() {
    totalWindow.add(1);
  }


  private static boolean trueOnProbability(double p) {
    return Math.random() < p;
  }
}
