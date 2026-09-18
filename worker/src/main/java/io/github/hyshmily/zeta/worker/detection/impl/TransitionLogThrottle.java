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
package io.github.hyshmily.zeta.worker.detection.impl;

import io.github.hyshmily.zeta.util.LogThrottle;

/**
 * Counts per-key state transitions and tells the caller when a rate-limited
 * aggregate summary is due — at most one per {@value #WINDOW_MS}ms window,
 * the ADR-0037 log-throttling convention.
 *
 * <p>Per-key transitions are logged at DEBUG by {@code ZetaBayesianSM}; this
 * throttle bounds the accompanying INFO visibility line so a mass-heat event
 * (thousands of keys transitioning in one report batch) emits a single
 * summary instead of one log per key — the design principle "never INFO on
 * the hot path".
 *
 * <p>The window/admission logic lives in {@link LogThrottle.Accumulator}, the
 * shared log-throttling utility; this adapter keeps the state machine's call
 * sites and synthetic-clock tests unchanged. Package-private: an internal
 * detail of the state machine's logging.
 */
final class TransitionLogThrottle {

  /** Window between aggregate transition summaries ({@value #WINDOW_MS}ms). */
  public static final long WINDOW_MS = 10_000;

  /** Shared accumulating throttle; owns the count and window state. */
  private final LogThrottle.Accumulator delegate = new LogThrottle.Accumulator(WINDOW_MS);

  /**
   * Records one transition and reports whether the summary window has
   * elapsed.
   *
   * @param now current monotonic millis
   * @return the number of transitions since the last summary when a summary
   *         is due, {@code -1} otherwise. Every recorded transition is
   *         accounted into exactly one returned count, so the sum of all
   *         returned counts equals the total number of records.
   */
  long record(long now) {
    return delegate.record(now);
  }
}
