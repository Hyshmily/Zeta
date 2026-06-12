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
package io.github.hyshmily.hotkey.util;

import io.github.hyshmily.hotkey.logging.DefaultLogger;
import io.github.hyshmily.hotkey.logging.HotKeyLogger;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Monitors system CPU load using the JDK platform MXBean with EMA smoothing.
 *
 * <p>Uses {@link com.sun.management.OperatingSystemMXBean#getCpuLoad()} to
 * read the CPU load of the JVM process. The value is smoothed with an
 * exponential moving average ({@code decay=0.95}) to filter out transient
 * spikes.
 * <p>Polling interval defaults to 500 ms. The monitor is started and stopped
 * explicitly via {@link #start()} / {@link #stop()}.
 */
public class SystemLoadMonitor {

  private static final HotKeyLogger log = new DefaultLogger(SystemLoadMonitor.class);

  private static final double DEFAULT_DECAY = 0.95;
  private static final long DEFAULT_POLL_MS = 500;

  private final long pollIntervalMs;
  private final double decay;
  private final ScheduledExecutorService scheduler;

  private final AtomicLong emaCpuLoadBits = new AtomicLong(Double.doubleToLongBits(0.0));
  private final AtomicBoolean running = new AtomicBoolean(false);

  /** @deprecated Use {@link #SystemLoadMonitor(long, double)} for explicit configuration. */
  @Deprecated
  public SystemLoadMonitor() {
    this(DEFAULT_POLL_MS, DEFAULT_DECAY);
  }

  /**
   * Creates a CPU load monitor with the given polling interval and EMA decay factor.
   *
   * @param pollIntervalMs polling interval in milliseconds; must be positive, otherwise the
   *                       default (500 ms) is used
   * @param decay          EMA decay factor in (0, 1); higher values smooth more aggressively,
   *                       values outside (0, 1) fall back to the default (0.95)
   */
  public SystemLoadMonitor(long pollIntervalMs, double decay) {
    this.pollIntervalMs = pollIntervalMs > 0 ? pollIntervalMs : DEFAULT_POLL_MS;
    this.decay = (decay > 0 && decay < 1) ? decay : DEFAULT_DECAY;
    this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "cpu-monitor");
      t.setDaemon(true);
      return t;
    });
  }

  /** Start periodic CPU sampling. Idempotent. */
  public void start() {
    if (!running.compareAndSet(false, true)) {
      return;
    }
    scheduler.scheduleAtFixedRate(this::sample, 0, pollIntervalMs, TimeUnit.MILLISECONDS);
    log.debug("SystemLoadMonitor started: pollIntervalMs={}, decay={}", pollIntervalMs, decay);
  }

  /** Stop the background sampler. */
  public void stop() {
    running.set(false);
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
   */
  public double getCpuLoadRaw() {
    try {
      OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
      if (osBean instanceof com.sun.management.OperatingSystemMXBean sunOsBean) {
        return Math.min(1.0, Math.max(0.0, sunOsBean.getCpuLoad()));
      }
    } catch (Exception ignored) {
    }
    return 0.0;
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
    } catch (Exception e) {
      log.warn("Failed to sample CPU load", e);
    }
  }
}
