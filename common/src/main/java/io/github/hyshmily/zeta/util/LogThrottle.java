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
package io.github.hyshmily.zeta.util;

import io.github.hyshmily.zeta.Internal;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.util.Assert;

/**
 * Window-based log throttle: admits <b>at most one</b> caller per {@code windowMs},
 * so a hot path or a failing dependency cannot flood the log.
 *
 * <p><b>Why this class exists.</b> This decision used to be hand-rolled at every
 * call site as {@code private volatile long lastXxxLoggedAtMs} plus
 * {@code if (now - lastXxxLoggedAtMs < WINDOW) return false; lastXxxLoggedAtMs = now;}
 * — a check-then-act pair on a {@code volatile} field. A {@code volatile} field
 * gives <em>visibility</em>, not <em>atomicity</em>: every thread that reads the
 * stale stamp before the first writer commits passes the check and logs, so the
 * throttle degrades to "one line per racing thread" exactly when the log is already
 * under load. The idiom had been copied across the codebase, which is why the
 * decision now lives here instead of in a dozen private helper methods.
 *
 * <p><b>Rate limiting is strict, not approximate.</b> {@code tryAcquire()} uses
 * compare-and-set: only the winner of the window is admitted, and losers are
 * suppressed without retrying (no spin). A caller that wants a suppressed-count
 * tally for the next emitted line keeps its own counter — that is a separate
 * concern from the admission decision.
 *
 * <p><b>Clock choice.</b> The window is measured with
 * {@link TimeSource#monotonicMillis()} — an elapsed-time computation, which is
 * exactly what the monotonic axis is for (see {@link TimeSource}'s class doc). A
 * raw {@link System#currentTimeMillis()} would additionally cost a JNI call per
 * invocation, and a backward NTP step would make {@code now - last} negative and
 * <em>silently suppress the alarm that an incident most needs</em>.
 *
 * <p><b>First call always wins.</b> The stamp is seeded to {@code -windowMs} rather
 * than {@code 0}: a zero seed reads as "inside the window" on a clock whose origin is
 * near zero (a fresh JVM, or a test that shifts the monotonic axis), which would
 * swallow the very first failure — the one carrying the most diagnostic value.
 *
 * <p>Thread-safe. Each instance is a single independent window; give each distinct
 * alarm its own instance.
 *
 * @see TimeSource#monotonicMillis()
 */
@Internal
public final class LogThrottle {

  /** Length of the admission window, in milliseconds. */
  private final long windowMs;

  /**
   * Monotonic timestamp of the last admitted call, or {@code -windowMs} before the
   * first one. An {@link AtomicLong} claimed by compare-and-set — see the class doc
   * for why a plain {@code volatile long} is not sufficient here.
   */
  private final AtomicLong lastAcquiredAtMs;

  /**
   * Creates a throttle admitting one caller per {@code windowMs}.
   *
   * @param windowMs the admission window length in milliseconds; must be positive
   */
  public LogThrottle(long windowMs) {
    Assert.isTrue(windowMs > 0, "windowMs must be positive: " + windowMs);
    this.windowMs = windowMs;
    this.lastAcquiredAtMs = new AtomicLong(-windowMs);
  }

  /**
   * Tries to claim the current window.
   *
   * <p>Returns {@code true} for exactly one caller per {@code windowMs}; every other
   * caller — whether it arrived inside an open window or lost the compare-and-set to a
   * thread that arrived at the same instant — gets {@code false} and should log at a
   * lower level (or skip logging) instead.
   *
   * @return {@code true} if this caller may emit the throttled log line now
   */
  public boolean tryAcquire() {
    long now = TimeSource.monotonicMillis();
    long last = lastAcquiredAtMs.get();
    return now - last >= windowMs && lastAcquiredAtMs.compareAndSet(last, now);
  }

  /**
   * The standard window for throttled alarms across the codebase (10 seconds) —
   * the single definition of the value every local {@code *_LOG_WINDOW_MS}
   * constant used to restate, so a global re-tune is a one-line change here.
   */
  public static final long DEFAULT_WINDOW_MS = 10_000;

  /**
   * Creates a throttle with the standard {@link #DEFAULT_WINDOW_MS} window.
   * Preferred over {@code new LogThrottle(10_000)} at call sites so the window
   * length is named exactly once, here.
   */
  public static LogThrottle perDefaultWindow() {
    return new LogThrottle(DEFAULT_WINDOW_MS);
  }

  /**
   * Counting variant for the "one full WARN per window, tally the rest" idiom:
   * the per-window admission decision of {@link LogThrottle} plus an
   * {@link AtomicLong} tally of the occurrences suppressed since the last
   * admitted line, so the next emitted WARN can carry the suppressed count and
   * a suppressed caller can surface a low-level DEBUG with the running tally.
   *
   * <p>This used to be copied at every failure-reporting site as a
   * {@code LogThrottle} field plus a sibling {@code AtomicLong} counter and a
   * three-branch increment / {@code getAndSet(0)} dance; the decision now lives
   * here. Typical use:
   *
   * <pre>{@code
   * Counting.Attempt attempt = loadFailureThrottle.record();
   * if (!attempt.admitted()) {
   *   log.debug("load failed for key={} ({} failures in current window)", key, attempt.count());
   *   return;
   * }
   * if (attempt.count() > 0) {
   *   log.warn("load failed for key={} ({} suppressed in the last {}ms)", key, attempt.count(), window, e);
   * } else {
   *   log.warn("load failed for key={}", key, e);
   * }
   * }</pre>
   *
   * <p>Thread-safe.
   */
  public static final class Counting {

    /**
     * Immutable outcome of one {@link Counting#record()} call.
     *
     * @param admitted {@code true} when this caller may emit the full WARN line
     * @param count    when {@code admitted}, the occurrences suppressed since the
     *                 previous admitted line (the current one excluded); when
     *                 suppressed, the occurrences in the current window so far
     */
    public record Attempt(boolean admitted, long count) {}

    private final LogThrottle throttle;

    /** Occurrences since the last admitted line. */
    private final AtomicLong sinceLastLog = new AtomicLong(0);

    /** Creates a counting throttle with the standard {@link #DEFAULT_WINDOW_MS} window. */
    public Counting() {
      this(DEFAULT_WINDOW_MS);
    }

    /**
     * Creates a counting throttle with an explicit window.
     *
     * @param windowMs the admission window length in milliseconds; must be positive
     */
    public Counting(long windowMs) {
      this.throttle = new LogThrottle(windowMs);
    }

    /**
     * Records one occurrence and decides whether it may be logged in full.
     *
     * @return an {@link Attempt} as described in its documentation
     */
    public Attempt record() {
      long failures = sinceLastLog.incrementAndGet();
      if (!throttle.tryAcquire()) {
        return new Attempt(false, failures);
      }
      return new Attempt(true, sinceLastLog.getAndSet(0) - 1);
    }
  }

  /**
   * Accumulating variant for aggregate summaries: records occurrences and, at
   * most once per window, reports the exact number recorded since the last
   * summary — so a mass event (thousands of occurrences in one report batch)
   * emits a single summary line instead of one log line per occurrence. Every
   * recorded occurrence is accounted into exactly one returned count: the sum
   * of all returned counts equals the total number of records.
   *
   * <p>Synchronized rather than CAS-based because the accounting (increment +
   * window check + reset) must be atomic as a whole; it runs once per occurrence
   * on report-batch-granular paths, not per-element hot loops.
   *
   * <p>Thread-safe.
   */
  public static final class Accumulator {

    private final long windowMs;

    /** Occurrences recorded since the last emitted summary. Guarded by {@code this}. */
    private long countSinceSummary;

    /**
     * Monotonic timestamp of the last summary, seeded to {@code -windowMs} so the
     * very first record emits a summary (first call always wins — see the
     * enclosing class doc).
     */
    private long lastSummaryAtMs;

    /**
     * Creates an accumulator emitting at most one summary per {@code windowMs}.
     *
     * @param windowMs the summary window length in milliseconds; must be positive
     */
    public Accumulator(long windowMs) {
      Assert.isTrue(windowMs > 0, "windowMs must be positive: " + windowMs);
      this.windowMs = windowMs;
      this.lastSummaryAtMs = -windowMs;
    }

    /**
     * Records one occurrence against the current monotonic clock.
     *
     * @return the number of occurrences since the last summary when a summary is
     *         due, {@code -1} otherwise
     */
    public long record() {
      return record(TimeSource.monotonicMillis());
    }

    /**
     * Records one occurrence at an explicit monotonic timestamp — the seam for
     * synthetic-clock tests.
     *
     * @param now current monotonic millis
     * @return the number of occurrences since the last summary when a summary is
     *         due, {@code -1} otherwise
     */
    public synchronized long record(long now) {
      countSinceSummary++;
      if (now - lastSummaryAtMs < windowMs) {
        return -1;
      }
      long due = countSinceSummary;
      countSinceSummary = 0;
      lastSummaryAtMs = now;
      return due;
    }
  }
}
