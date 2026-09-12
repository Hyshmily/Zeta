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

/**
 * Counts per-key state transitions and tells the caller when a rate-limited
 * aggregate summary is due — at most one per {@value #WINDOW_MS}ms window,
 * the ADR-0037 log-throttling convention.
 *
 * <p>Per-key transitions are logged at DEBUG by {@code ZetaBayesianSM}; this
 * throttle bounds the accompanying INFO visibility line so a mass-heat event
 * (thousands of keys transitioning in one report batch) emits a single
 * summary instead of one log per key — the design principle "never INFO on
 * the hot path". Package-private: an internal detail of the state machine's
 * logging, unit-tested directly with synthetic clocks.
 */
final class TransitionLogThrottle {

  /** Window between aggregate transition summaries ({@value #WINDOW_MS}ms). */
  static final long WINDOW_MS = 10_000;

  /** Transitions recorded since the last emitted summary. Guarded by {@code this}. */
  private long countSinceSummary = 0;

  /**
   * Monotonic timestamp of the last summary. Initialized to {@code -WINDOW_MS}
   * (the ReportConsumer broadcast-failure WARN convention) so the very first
   * transition always emits a summary.
   */
  private long lastSummaryAtMs = -WINDOW_MS;

  /**
   * Records one transition and reports whether the summary window has
   * elapsed. Synchronized: report consumers evaluate keys concurrently, and
   * transitions must neither lose counts nor emit two summaries for one
   * window.
   *
   * @param now current monotonic millis
   * @return the number of transitions since the last summary when a summary
   *         is due, {@code -1} otherwise. Every recorded transition is
   *         accounted into exactly one returned count, so the sum of all
   *         returned counts equals the total number of records.
   */
  @SuppressWarnings("java:S6213")
  synchronized long record(long now) {
    countSinceSummary++;
    if (now - lastSummaryAtMs < WINDOW_MS) {
      return -1;
    }

    long due = countSinceSummary;
    countSinceSummary = 0;
    lastSummaryAtMs = now;
    return due;
  }
}
