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
package io.github.hyshmily.zeta.util.impl;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.util.SystemLoadMonitor;
import io.github.hyshmily.zeta.util.ZetaThreadFactory;
import io.github.hyshmily.zeta.util.executor.SafeScheduledExecutorService;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

/**
 * Monitors system CPU load using the JDK platform MXBean with EMA smoothing.
 *
 * <p>Uses {@link com.sun.management.OperatingSystemMXBean#getCpuLoad()} to
 * read the CPU load of the JVM process. The value is smoothed with an
 * exponential moving average ({@code decay=0.95}) to filter out transient
 * spikes.
 * <p>Polling interval defaults to 500 ms. The monitor is started and stopped
 * explicitly via {@link #start()} / {@link #stop()}.
 * <p>A failed or {@code NaN} MXBean read never poisons the EMA: the raw read
 * falls back to the last good value (0.0 before the first success), so consumers
 * such as the BBR/SRE rate limiters never observe {@code NaN} (which would flip
 * them into strict enforcement permanently). The first failure logs one WARN
 * with the stack trace; subsequent failures are logged at DEBUG only — the
 * poller ticks every 500 ms and a recurring full-stacktrace WARN would flood
 * the log forever.
 */
@Internal
@Slf4j
public class SystemLoadMonitorImpl implements SystemLoadMonitor {

  private static final double DEFAULT_DECAY = 0.95;
  private static final long DEFAULT_POLL_MS = 500;

  private final long pollIntervalMs;
  private final double decay;
  private final ScheduledExecutorService scheduler;
  private final boolean ownsScheduler;

  private final AtomicLong emaCpuLoadBits = new AtomicLong(Double.doubleToLongBits(0.0));
  private final AtomicBoolean running = new AtomicBoolean(false);
  /** Whether the one-time WARN for a failed/NaN MXBean read has been emitted. */
  private final AtomicBoolean readFailureWarned = new AtomicBoolean(false);
  /** Last successful raw CPU load, returned on failed/NaN reads (0.0 before the first success). */
  private volatile double lastGoodRawLoad = 0.0;
  private ScheduledFuture<?> flushTask;

  /**
   * Creates a CPU load monitor with the given polling interval and EMA decay factor.
   * Creates its own scheduler (deprecated, prefer shared scheduler).
   *
   * @param pollIntervalMs polling interval in milliseconds; must be positive, otherwise the
   *                       default (500 ms) is used
   * @param decay          EMA decay factor in (0, 1); higher values smooth more aggressively,
   *                       values outside (0, 1) fall back to the default (0.95)
   */
  public SystemLoadMonitorImpl(long pollIntervalMs, double decay) {
    this(new SafeScheduledExecutorService(1, new ZetaThreadFactory("zeta-cpu-monitor")), pollIntervalMs, decay, true);
  }

  /**
   * Creates a CPU load monitor with a shared external scheduler.
   *
   * @param scheduler      the shared scheduler (not shut down on stop)
   * @param pollIntervalMs polling interval in milliseconds
   * @param decay          EMA decay factor in (0, 1)
   */
  public SystemLoadMonitorImpl(ScheduledExecutorService scheduler, long pollIntervalMs, double decay) {
    this(scheduler, pollIntervalMs, decay, false);
  }

  private SystemLoadMonitorImpl(
    ScheduledExecutorService scheduler,
    long pollIntervalMs,
    double decay,
    boolean ownsScheduler
  ) {
    this.pollIntervalMs = pollIntervalMs > 0 ? pollIntervalMs : DEFAULT_POLL_MS;
    this.decay = (decay > 0 && decay < 1) ? decay : DEFAULT_DECAY;
    this.scheduler = scheduler;
    this.ownsScheduler = ownsScheduler;
  }

  /** Start periodic CPU sampling. Idempotent. */
  public void start() {
    if (!running.compareAndSet(false, true)) {
      return;
    }
    try {
      flushTask = scheduler.scheduleAtFixedRate(this::sample, 0, pollIntervalMs, TimeUnit.MILLISECONDS);
      log.debug("SystemLoadMonitor started: pollIntervalMs={}, decay={}", pollIntervalMs, decay);
    } catch (Exception e) {
      log.error(
        "Failed to start SystemLoadMonitor sampler; CPU-based BBR backpressure " +
          "will fall back to 0 CPU load (permissive).",
        e
      );
    }
  }

  /** Stop the background sampler. */
  public void stop() {
    running.set(false);
    if (flushTask != null) {
      flushTask.cancel(false);
    }
    if (ownsScheduler) {
      scheduler.shutdown();
      try {
        if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
          scheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        scheduler.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Returns the EMA-smoothed CPU load (0.0 – 1.0).
   * Returns 0.0 if no sample has been taken yet.
   */
  public double getCpuLoadEMA() {
    return Double.longBitsToDouble(emaCpuLoadBits.get());
  }

  /**
   * Returns the raw (non-smoothed) CPU load (0.0 – 1.0).
   * Reads directly from the MXBean each call.
   *
   * <p><b>Failure handling:</b> a failed or {@code NaN} MXBean read is treated the
   * same — the last successful raw value is returned (0.0 before the first success),
   * so the EMA can never be poisoned (see class Javadoc). The first failure logs one
   * WARN with the stack trace; afterwards failures stay at DEBUG.
   */
  public double getCpuLoadRaw() {
    try {
      double raw = readCpuLoadFromOsBean();
      if (!Double.isNaN(raw)) {
        // Math.min/Math.max do not filter NaN — that is handled above as a failure.
        double clamped = Math.min(1.0, Math.max(0.0, raw));
        lastGoodRawLoad = clamped;
        return clamped;
      }
      return onCpuLoadReadFailed(null);
    } catch (Exception e) {
      return onCpuLoadReadFailed(e);
    }
  }

  /**
   * Reads the process CPU load from the platform MXBean. Protected seam: tests
   * subclass and override this to simulate MXBean failures and {@code NaN} reads.
   *
   * @return the raw CPU load, possibly {@code NaN} when the platform cannot report it
   */
  protected double readCpuLoadFromOsBean() {
    OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
    if (osBean instanceof com.sun.management.OperatingSystemMXBean sunOsBean) {
      return sunOsBean.getCpuLoad();
    }
    return 0.0;
  }

  /**
   * Fallback for a failed/NaN MXBean read: one WARN (first failure only, with the
   * stack trace), then DEBUG, returning the last good raw value.
   */
  private double onCpuLoadReadFailed(Exception cause) {
    if (readFailureWarned.compareAndSet(false, true)) {
      log.warn("Failed to read CPU load from MXBean; returning the last good value. "
        + "Further failures are logged at DEBUG.", cause);
    } else if (cause == null) {
      log.debug("CPU load MXBean returned NaN; returning the last good value.");
    } else {
      log.debug("Failed to read CPU load from MXBean; returning the last good value.", cause);
    }
    return lastGoodRawLoad;
  }

  private void sample() {
    try {
      double raw = getCpuLoadRaw();
      double current = getCpuLoadEMA();
      if (current == 0.0 && raw > 0) {
        emaCpuLoadBits.set(Double.doubleToLongBits(raw));
      } else {
        double next = current * decay + raw * (1.0 - decay);
        emaCpuLoadBits.set(Double.doubleToLongBits(next));
      }
      log.trace("SystemLoadMonitor tick: raw={}, ema={}", raw, getCpuLoadEMA());
    } catch (Exception e) {
      log.warn("Failed to sample CPU load", e);
    }
  }
}
