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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.github.hyshmily.zeta.util.executor.ZetaThreadFactory;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * High-throughput clock source caching {@link System#currentTimeMillis()} on a background daemon
 * thread (5 ms cadence) to avoid per-read JNI calls on hot paths.
 *
 * <p><b>Lifecycle &amp; fallback.</b> Started via {@link #start()} or
 * {@link #registerLifecycleUser()} and stopped once the last lifecycle owner calls
 * {@link #releaseLifecycleUser()}. If the ticker is interrupted or dies unexpectedly, it backs off
 * for 1 second before resuming or restarting; reads seamlessly fall back to
 * {@link System#currentTimeMillis()} clamped to the monotonic floor so time never steps backwards.
 *
 * <p><b>Clock axes.</b> Use {@link #currentTimeMillis()} for absolute wall-clock deadlines and
 * stamps, and {@link #monotonicMillis()} for all elapsed-time and delta computations.
 */
@Internal
@Slf4j
public final class TimeSource {

  private static final long NANOS_PER_MILLI = 1_000_000L;
  private static final long TICK_MS = 5L;
  private static final long RESTART_BACKOFF_MS = 1_000L;
  private static final long STOP_JOIN_TIMEOUT_MS = 200L;

  /**
   * Largest forward wall-clock jump (beyond elapsed monotonic time) adopted verbatim; larger leaps
   * are slewed at twice real-time speed until the cached clock catches up.
   */
  private static final long MAX_FORWARD_JUMP_MS = 5_000L;

  /** Wall-clock and monotonic anchors captured at class load so monotonic values stay readable. */
  private static final long BOOT_WALL_MS = System.currentTimeMillis();
  private static final long BOOT_NANO = System.nanoTime();

  /** CAS handle for the sign-encoded {@link #currentMillis} clock word. */
  private static final VarHandle CURRENT_MILLIS;

  static {
    try {
      CURRENT_MILLIS = MethodHandles.lookup().findStaticVarHandle(TimeSource.class, "currentMillis", long.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Sign-encoded cached wall clock and ticker state:
   * <ul>
   *   <li>{@code > 0} (running): cached wall clock including {@link #wallOffsetMs} (one volatile read).</li>
   *   <li>{@code <= 0} (stopped): {@code ~rawFloor}, where {@code rawFloor} is the last unshifted
   *       wall-clock floor; reads fall back to {@code max(rawFloor, System.currentTimeMillis()) + wallOffsetMs}.</li>
   * </ul>
   */
  private static volatile long currentMillis = ~System.currentTimeMillis();

  /** Synthetic wall-clock offset for tests; always {@code 0} in production. */
  private static volatile long wallOffsetMs = 0L;

  /** Synthetic monotonic offset for tests; always {@code 0} in production. */
  private static volatile long monoOffsetMs = 0L;

  /** Currently installed ticker thread, or {@code null} when stopped. */
  private static volatile @Nullable Thread clockThread;

  /** Number of active lifecycle owners (Spring contexts) sharing the JVM-global ticker. */
  @GuardedBy("TimeSource.class")
  private static int lifecycleUsers = 0;

  /** Cumulative ticker start attempts for diagnostics. */
  @GuardedBy("TimeSource.class")
  private static int startAttempts = 0;

  /** Prevents instantiation of this static utility class. */
  private TimeSource() {
    throw new AssertionError("No TimeSource instances");
  }

  /**
   * Returns the current wall-clock time in epoch milliseconds.
   *
   * <p>Reads a single volatile field when the ticker is running; falls back to
   * {@link System#currentTimeMillis()} clamped to the monotonic floor when stopped or during a
   * restart backoff.
   *
   * @return epoch milliseconds (wall clock, monotonically non-decreasing)
   */
  public static long currentTimeMillis() {
    long cached = currentMillis;
    if (cached > 0L) {
      return cached;
    }
    // Stopped encoding: ~cached holds the raw floor before applying any test offset.
    return Math.max(~cached, System.currentTimeMillis()) + wallOffsetMs;
  }

  /**
   * Returns a monotonic millisecond timestamp immune to wall-clock/NTP jumps, anchored near the
   * wall clock at JVM startup. Use for all elapsed-time and interval calculations.
   *
   * @return monotonic timestamp in milliseconds
   */
  public static long monotonicMillis() {
    return BOOT_WALL_MS + (System.nanoTime() - BOOT_NANO) / NANOS_PER_MILLI + monoOffsetMs;
  }

  /**
   * Starts the background clock-cache daemon thread if not already running or in restart backoff.
   * Idempotent and thread-safe.
   */
  @SuppressWarnings("java:S1181")
  public static synchronized void start() {
    // clockThread != null also covers the 1s backoff while a dying ticker still owns the slot.
    if (currentMillis > 0L || clockThread != null) {
      return;
    }
    if (++startAttempts == 1) {
      log.info("TimeSource clock-cache thread starting");
    } else {
      log.info("TimeSource clock-cache thread restarted (attempt #{})", startAttempts);
    }
    try {
      Thread ticker = new ZetaThreadFactory("TimeSource").newThread(TimeSource::runTicker);
      ticker.setUncaughtExceptionHandler((thread, failure) -> {
        log.error("TimeSource thread terminated unexpectedly, will restart after 1s.", failure);
        restartWithBackoff();
      });
      clockThread = ticker;
      // Publish a fresh running timestamp synchronously before starting the thread so callers
      // never observe a stale boot/pre-backoff value before the first tick.
      publishRunning();
      ticker.start();
    } catch (Throwable e) {
      clockThread = null;
      publishStopped();
      throw e;
    }
  }

  /**
   * Registers a lifecycle owner (e.g., an application context) and ensures the ticker is running.
   * Paired with {@link #releaseLifecycleUser()}.
   */
  public static void registerLifecycleUser() {
    synchronized (TimeSource.class) {
      lifecycleUsers++;
      start();
    }
  }

  /**
   * Releases a lifecycle owner and stops the ticker once the last owner departs. Runs the thread
   * interrupt and bounded join outside the class monitor to avoid blocking a recovering ticker.
   */
  public static void releaseLifecycleUser() {
    boolean lastOwner;
    synchronized (TimeSource.class) {
      // Ignore unmatched releases so a directly started ticker is not stopped unexpectedly.
      if (lifecycleUsers <= 0) {
        return;
      }
      lastOwner = --lifecycleUsers == 0;
    }
    if (lastOwner) {
      stopTicker();
    }
  }

  /**
   * Test-only hook applying independent offsets to {@link #currentTimeMillis()} and
   * {@link #monotonicMillis()} to simulate NTP steps. Reset with {@code (0, 0)}.
   *
   * @param wallOffsetMs synthetic offset applied to the wall clock
   * @param monoOffsetMs synthetic offset applied to the monotonic clock
   */
  public static synchronized void setTimeOffsetForTest(long wallOffsetMs, long monoOffsetMs) {
    TimeSource.monoOffsetMs = monoOffsetMs;
    long delta = wallOffsetMs - TimeSource.wallOffsetMs;
    // Publish wallOffsetMs before shifting currentMillis so readers never pair new value + old offset.
    TimeSource.wallOffsetMs = wallOffsetMs;
    if (delta == 0L) {
      return;
    }
    for (;;) {
      long cached = currentMillis;
      if (cached <= 0L) {
        break;
      }
      // Keep the running sign positive even under a synthetic negative offset.
      long next = Math.max(1L, cached + delta);
      if (CURRENT_MILLIS.compareAndSet(cached, next)) {
        break;
      }
    }
  }

  /**
   * Main daemon ticker loop updating {@link #currentMillis} every {@link #TICK_MS} ms, recovering
   * in place from unexpected interrupts after a backoff.
   */
  @SuppressWarnings("all")
  private static void runTicker() {
    long lastTickNanos = System.nanoTime();
    while (clockThread == Thread.currentThread()) {
      long cached = currentMillis;
      if (cached <= 0L) {
        return;
      }
      lastTickNanos = advanceTick(cached, lastTickNanos);
      try {
        Thread.sleep(TICK_MS);
      } catch (InterruptedException ignored) {
        if (currentMillis <= 0L || clockThread != Thread.currentThread()) {
          return;
        }
        // Unexpected interrupt: fall back during backoff and re-arm this thread in place.
        if (!resumeAfterBackoff()) {
          return;
        }
        // Reset anchor so the backoff wait is not counted as elapsed tick time.
        lastTickNanos = System.nanoTime();
      }
    }
  }

  /**
   * Computes one cached-clock tick with monotonic clamping against backward NTP jumps and
   * 2x-realtime slewing for excessive forward leaps, publishing via CAS so it never overwrites a
   * concurrent stop transition.
   *
   * @param cached        the positive cached value read at the start of the iteration
   * @param lastTickNanos the monotonic anchor carrying sub-millisecond remainder from prior ticks
   * @return the advanced monotonic anchor
   */
  private static long advanceTick(long cached, long lastTickNanos) {
    // Sample wall clock first so any pause between reads inflates elapsedMs equally and does not
    // misclassify a real pause as an implausible forward leap.
    long wallNow = System.currentTimeMillis();
    long nowNanos = System.nanoTime();
    long elapsedMs = Math.max(0L, (nowNanos - lastTickNanos) / NANOS_PER_MILLI);
    // Preserve sub-millisecond remainder so 5ms ticks do not systematically under-measure elapsed time.
    long nextAnchorNanos = lastTickNanos + elapsedMs * NANOS_PER_MILLI;

    long offset = wallOffsetMs;
    long cachedWall = cached - offset;
    if (wallNow > cachedWall) {
      long delta = wallNow - cachedWall;
      long plausible = MAX_FORWARD_JUMP_MS + elapsedMs;
      long next;
      if (delta <= plausible) {
        next = wallNow + offset;
      } else {
        long slewMs = Math.max(1L, elapsedMs) * 2L;
        next = Math.min(wallNow + offset, cached + slewMs);
        log.debug(
          "Rejected forward wall-clock leap of {}ms (max plausible {}ms); slewing the cached clock at twice real time",
          delta,
          plausible
        );
      }
      // Keep running sign positive; CAS prevents resurrecting a concurrently stopped clock.
      CURRENT_MILLIS.compareAndSet(cached, Math.max(1L, next));
    }
    return nextAnchorNanos;
  }

  /**
   * Transitions an unexpectedly terminating ticker to stopped, waits {@link #RESTART_BACKOFF_MS},
   * and installs a replacement via {@link #start()} if this thread still owns {@link #clockThread}.
   */
  @SuppressWarnings("java:S2142")
  private static void restartWithBackoff() {
    if (!markStopped()) {
      return;
    }
    // Clear any residual interrupt flag from the uncaught failure so the backoff sleep actually runs.
    if (Thread.interrupted()) {
      log.debug("Cleared residual interrupt flag before TimeSource restart backoff");
    }
    try {
      Thread.sleep(RESTART_BACKOFF_MS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    synchronized (TimeSource.class) {
      if (clockThread == Thread.currentThread()) {
        clockThread = null;
        start();
      }
    }
  }

  /**
   * Suspends the ticker into wall-clock fallback for {@link #RESTART_BACKOFF_MS} after an
   * unexpected interrupt, then re-publishes running state if still owned by this thread.
   *
   * @return {@code true} if resumed; {@code false} if stopped or superseded during backoff
   */
  private static boolean resumeAfterBackoff() {
    if (!markStopped()) {
      return false;
    }
    try {
      Thread.sleep(RESTART_BACKOFF_MS);
    } catch (InterruptedException ignored) {
      // stopTicker() interrupts to cut backoff short; ownership check below decides exit.
    }
    synchronized (TimeSource.class) {
      if (clockThread != Thread.currentThread()) {
        return false;
      }
      publishRunning();
      log.info("TimeSource clock-cache thread resumed after an unexpected interrupt");
      return true;
    }
  }

  /**
   * Stops the active ticker when no lifecycle owners remain, interrupting it and performing a
   * bounded join outside the class monitor.
   */
  private static void stopTicker() {
    Thread ticker;
    synchronized (TimeSource.class) {
      if (lifecycleUsers > 0) {
        return;
      }
      ticker = clockThread;
      clockThread = null;
      publishStopped();
    }
    if (ticker == null) {
      return;
    }
    ticker.interrupt();
    if (ticker != Thread.currentThread()) {
      try {
        ticker.join(STOP_JOIN_TIMEOUT_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /** Atomically encodes the stopped state if the calling thread still owns {@link #clockThread}. */
  @SuppressWarnings("all")
  private static synchronized boolean markStopped() {
    if (clockThread != Thread.currentThread()) {
      return false;
    }
    publishStopped();
    return true;
  }

  /** Extracts the unshifted monotonic wall-clock floor from either sign encoding of {@code cached}. */
  private static long rawFloor(long cached) {
    return cached > 0L ? cached - wallOffsetMs : ~cached;
  }

  /** Publishes the positive running state under the class monitor. */
  @GuardedBy("TimeSource.class")
  @SuppressWarnings("all")
  private static void publishRunning() {
    currentMillis = Math.max(1L, Math.max(rawFloor(currentMillis), System.currentTimeMillis()) + wallOffsetMs);
  }

  /** Publishes the bitwise-complemented stopped state while preserving the raw monotonic floor. */
  @GuardedBy("TimeSource.class")
  @SuppressWarnings("all")
  private static void publishStopped() {
    currentMillis = ~rawFloor(currentMillis);
  }
}
