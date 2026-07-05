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

/**
 * SRE (Service Reliability Engineering) adaptive rate limiter based on
 * Google's "The Tail at Scale" client-side throttling algorithm.
 *
 * <p>Uses a sliding window to track total requests and successful requests,
 * deriving a dynamic max-requests-per-window ratio.  When the request volume
 * exceeds the adaptive limit, requests are probabilistically dropped with a
 * probability proportional to how far the system is over capacity.
 */
public interface SreRateLimiter {

  /** Check whether the current request should be allowed. */
  boolean tryAcquire();

  /** Record a successful request. */
  void onSuccess();

  /** Record a failed request. */
  void onFailed();
}
