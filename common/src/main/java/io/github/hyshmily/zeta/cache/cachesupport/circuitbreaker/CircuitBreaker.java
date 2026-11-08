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
package io.github.hyshmily.zeta.cache.cachesupport.circuitbreaker;

/**
 * Sliding-window circuit breaker for protecting remote calls from cascading failures.
 *
 * <p>State machine: CLOSED → OPEN → HALF_OPEN → CLOSED.
 * CLOSED: sliding-window failure rate tracking.
 * OPEN:  fast-fail; after {@code singleTestIntervalMs} a single probe is
 * admitted into HALF_OPEN.
 * HALF_OPEN: allows limited probe requests; requires N consecutive
 * successes to close, reverts to OPEN on any failure.
 *
 * <p>Supports exception filtering — exceptions matching exclude/include
 * rules are treated as success and do not trip the breaker.
 *
 * <p>Admission is explicit-stamp only: {@link #tryAcquireStamp()} binds the
 * probe reservation to the returned value instead of a thread-local slot,
 * so batch (one thread, many keys) and nested cache calls can never
 * cross-consume each other's probes. The caller passes the stamp back to
 * exactly one of {@link #onSuccess(long)}, {@link #onFailure(Throwable, long)}
 * or {@link #onAbandoned(long)}.
 *
 * <p>The failure window is advanced by the clock on the write path, so an
 * instance has no background task and no lifecycle to close.
 */
public interface CircuitBreaker {
  /**
   * Rejected admission stamp: {@link #tryAcquireStamp()} returns this when
   * the call must not proceed (OPEN with no probe budget).
   */
  long STAMP_REJECTED = -1L;

  /**
   * CLOSED-path admission stamp: no probe quota was consumed, so settlement
   * is window-accounting only. Never confused with a HALF_OPEN generation
   * (generations are strictly positive).
   */
  long STAMP_CLOSED = 0L;

  /**
   * Explicit-stamp admission.
   *
   * @return {@link #STAMP_REJECTED} when denied, {@link #STAMP_CLOSED} on the
   *         CLOSED path, otherwise the HALF_OPEN probe generation (strictly positive)
   */
  long tryAcquireStamp();

  /**
   * Settle a stamp acquired via {@link #tryAcquireStamp()} as success.
   *
   * @param stamp the stamp from {@link #tryAcquireStamp()}
   */
  void onSuccess(long stamp);

  /**
   * Settle a stamp acquired via {@link #tryAcquireStamp()} as failure.
   * The exception is checked against include/exclude rules — if ignored,
   * the failure is treated as a success and does not trip the breaker.
   *
   * @param error the failure cause (may be {@code null})
   * @param stamp the stamp from {@link #tryAcquireStamp()}
   */
  void onFailure(Throwable error, long stamp);

  /**
   * Release a stamp acquired via {@link #tryAcquireStamp()} without an
   * outcome — e.g. the executor rejected the task before any data-source
   * call was attempted. Neither success nor failure statistics are affected.
   *
   * @param stamp the stamp from {@link #tryAcquireStamp()}
   */
  void onAbandoned(long stamp);

  /** Whether the breaker is currently open. */
  boolean isOpen();

  /**
   * Whether the breaker is in a recovery state: OPEN, or any HALF_OPEN episode. The read path
   * uses this instead of {@link #isOpen()} for its degraded-mode decision, because a load denied
   * a probe, or a probe that fails, must fall back to a stale value rather than throw. An
   * admitted HALF_OPEN probe is still allowed to run; the state is reported as degraded so its
   * failure path serves stale.
   *
   * @return {@code true} when a load may be refused by the breaker
   */
  default boolean isDegraded() {
    return isOpen();
  }
}
