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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

/**
 * Performance-optimized clock source that caches {@link System#currentTimeMillis()}
 * on a low-frequency background thread (every 5ms) to avoid the cost of a
 * {@code native} call on every read via {@link #currentTimeMillis()}.
 * <p>
 * The background daemon thread is started once via {@link #start()} and runs
 * for the lifetime of the JVM.  If the thread is interrupted it silently falls
 * back to calling {@link System#currentTimeMillis()} directly.
 * <p>
 * All time-sensitive components in the hot-path (expiry, rate limiting, circuit
 * breaker) use this source instead of {@code System.currentTimeMillis()}.
 */
@Internal
@Slf4j
public final class TimeSource {

  private static volatile long currentMillis = System.currentTimeMillis();
  private static final AtomicBoolean threadRunning = new AtomicBoolean(false);
  private static final AtomicInteger threadTryCount = new AtomicInteger(0);
  private static final int THREAD_TRY_MAX = 3;

  /** Monotonic baseline: wall clock at JVM start, used to render monotonic values near wall time. */
  private static final long BOOT_WALL_MS = System.currentTimeMillis();
  private static final long BOOT_NANO = System.nanoTime();

  /** Wall-clock offset applied by tests to simulate NTP jumps; 0 in production. */
  private static volatile long wallOffsetMs = 0L;

  /** Monotonic offset applied by tests; 0 in production. */
  private static volatile long monoOffsetMs = 0L;

  /**
   * Start the background clock-cache thread. Idempotent after the thread is
   * running.  If the thread dies unexpectedly it will be restarted up to
   * {@link #THREAD_TRY_MAX} times with a 1-second delay between attempts.
   * Called automatically during {@code ZetaFacadeAutoConfiguration}
   * initialisation.
   */
  @SuppressWarnings("BusyWait")
  public static void start() {
    if (threadTryCount.incrementAndGet() <= THREAD_TRY_MAX && threadRunning.compareAndSet(false, true)) {
      Thread t = new ZetaThreadFactory("TimeSource").newThread(() -> {
        while (threadRunning.get()) {
          currentMillis = System.currentTimeMillis();
          try {
            Thread.sleep(5);
          } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
            threadRunning.set(false);
          }
        }
      });
      t.setUncaughtExceptionHandler((th, ex) -> {
        log.error("TimeSource thread terminated unexpectedly, will restart after 1s.", ex);
        threadRunning.set(false);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ignored) {
          Thread.currentThread().interrupt();
        }
        start();
      });
      t.start();
    }
  }

  /**
   * Returns the current time in milliseconds since the epoch.
   * <p>
   * When the background thread is running, this returns the cached value
   * updated every 5ms — avoiding a {@code native} JNI call on every read.
   * Falls back to {@link System#currentTimeMillis()} if the thread has not
   * been started or was interrupted.
   *
   * @return current time in milliseconds (epoch-based, wall clock)
   */
  public static long currentTimeMillis() {
    return (threadRunning.get() ? currentMillis : System.currentTimeMillis()) + wallOffsetMs;
  }

  /**
   * Returns the current time in milliseconds on a monotonic axis that never
   * moves backwards, anchored to the wall clock at JVM start.
   * <p>
   * Implemented as {@code bootWallMs + (System.nanoTime() - bootNano) / 1e6},
   * so wall-clock jumps (NTP steps, container clock drift) do not affect
   * elapsed-time computations. Use this source for <em>all delta
   * computations</em> (heartbeat timeouts, sliding windows, backoff, EMA
   * decay intervals, uptime). Keep {@link #currentTimeMillis()} for absolute
   * expiry timestamps, epoch/version stamping, and display — those semantics
   * inherently require the wall clock.
   * <p>
   * The returned value approximates the wall clock (deviation grows only
   * with wall-clock jumps), which keeps logs and diagnostics readable.
   *
   * @return current time in milliseconds on a monotonic axis
   */
  public static long monotonicMillis() {
    return BOOT_WALL_MS + (System.nanoTime() - BOOT_NANO) / 1_000_000L + monoOffsetMs;
  }

  /**
   * Test-only hook to simulate wall-clock jumps (e.g. NTP steps) for
   * {@code TimeJumpTest}-style regression tests. A positive {@code wallOffsetMs}
   * simulates a forward jump, a negative one a backward jump; the monotonic
   * axis is offset independently so tests can model "wall clock jumped,
   * monotonic did not". Reset with {@code setTimeOffsetForTest(0, 0)}.
   * No effect in production (defaults are zero).
   *
   * @param wallOffsetMs offset applied to {@link #currentTimeMillis()}
   * @param monoOffsetMs offset applied to {@link #monotonicMillis()}
   */
  public static void setTimeOffsetForTest(long wallOffsetMs, long monoOffsetMs) {
    TimeSource.wallOffsetMs = wallOffsetMs;
    TimeSource.monoOffsetMs = monoOffsetMs;
  }

  private TimeSource() {}
}
