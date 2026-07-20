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
package io.github.hyshmily.zeta.cache.cachesupport;

/**
 * Sliding-window circuit breaker for protecting remote calls from cascading failures.
 *
 * <p>State machine: CLOSED → OPEN → HALF_OPEN → CLOSED.
 * CLOSED: sliding-window failure rate tracking.
 * OPEN:  fast-fail, blocks all requests.
 * HALF_OPEN: allows limited probe requests; requires N consecutive
 * successes to close, reverts to OPEN on any failure.
 *
 * <p>Supports exception filtering — exceptions matching exclude/include
 * rules are treated as success and do not trip the breaker.
 */
public interface CircuitBreaker extends AutoCloseable {
  /**
   * Check whether a request is allowed through.
   * When closed: always allowed. When open: allowed only for half-open probe.
   *
   * @return {@code true} if the request may proceed
   */
  boolean allowRequest();

  /** Record a successful call. If the breaker was open, attempt to close it. */
  void onSuccess();

  /** Record a failed call. */
  void onFailure();

  /**
   * Record a failed call with exception context.
   * The exception is checked against include/exclude rules — if ignored,
   * the failure is treated as a success and does not trip the breaker.
   */
  void onFailure(Throwable t);

  /** Whether the breaker is currently open. */
  boolean isOpen();

  /** Current state of the circuit breaker. */
  CircuitBreakerState getState();
}
