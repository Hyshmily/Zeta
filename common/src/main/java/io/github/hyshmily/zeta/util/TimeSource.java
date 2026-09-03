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
 * for the lifetime of the JVM.  If the thread is interrupted or dies
 * unexpectedly it is restarted with a 1-second backoff; reads fall back to
 * calling {@link System#currentTimeMillis()} directly until the replacement
 * thread is up.
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

  /** Monotonic baseline: wall clock at JVM start, used to render monotonic values near wall time. */
  private static final long BOOT_WALL_MS = System.currentTimeMillis();
  private static final long BOOT_NANO = System.nanoTime();

  /** Wall-clock offset applied by tests to simulate NTP jumps; 0 in production. */
  private static volatile long wallOffsetMs = 0L;

  /** Monotonic offset applied by tests; 0 in production. */
  private static volatile long monoOffsetMs = 0L;

  /**
   * Start the background clock-cache thread. Idempotent after the thread is
   * running. If the thread dies unexpectedly or is interrupted it is restarted
   * with a 1-second delay between attempts, indefinitely — a dying clock-cache
   * thread is a rare fault and restarting costs at most one thread per second,
   * while the reads fall back to {@link System#currentTimeMillis()} meanwhile.
   * Called automatically during {@code ZetaFacadeAutoConfiguration}
   * initialisation.
   */
  @SuppressWarnings("all")
  public static void start() {
    if (threadRunning.compareAndSet(false, true)) {
      if (threadTryCount.incrementAndGet() == 1) {
        log.info("TimeSource clock-cache thread starting");
      } else {
        log.info("TimeSource clock-cache thread restarted (attempt #{})", threadTryCount.get());
      }
      Thread t = new ZetaThreadFactory("TimeSource").newThread(() -> {
        while (threadRunning.get()) {
          // Monotonic floor: a backward NTP step must never make the cached
          // wall clock jump backwards. Consumers compute deltas like
          // hardExpireAt - now and Snowflake's clock-backwards check against
          // this source; a backwards step would render those negative (early
          // expiry, spurious clock-moved-backwards exceptions). Clamping to
          // the last observed value keeps the cached clock monotonic; it
          // catches up naturally once the wall clock moves past the floor.
          currentMillis = Math.max(currentMillis, System.currentTimeMillis());
          try {
            Thread.sleep(5);
          } catch (InterruptedException ignored) {
            // An interrupted clock thread must not degrade every read to a native
            // call forever — take the same 1s-backoff restart as an unexpected death.
            restartWithBackoff();
            return;
          }
        }
      });
      t.setUncaughtExceptionHandler((th, ex) -> {
        log.error("TimeSource thread terminated unexpectedly, will restart after 1s.", ex);
        restartWithBackoff();
      });
      t.start();
    }
  }

  /**
   * Marks the clock-cache thread dead, waits the 1-second restart backoff
   * (best-effort: an interrupted backoff still proceeds to the restart), then
   * starts a replacement via {@link #start()}. Runs on the dying thread itself;
   * a concurrent {@code start()} winning the CAS during the backoff makes the
   * final {@code start()} a no-op.
   */
  private static void restartWithBackoff() {
    threadRunning.set(false);
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    start();
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
