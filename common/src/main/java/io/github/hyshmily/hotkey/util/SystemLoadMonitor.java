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

/**
 * Monitors system CPU load and provides an exponentially weighted moving
 * average (EMA) of the recent CPU utilisation.
 *
 * <p>Uses {@link java.lang.management.OperatingSystemMXBean#getCpuLoad()} (or its
 * platform-specific equivalent) to sample the CPU load at a fixed interval
 * and smooths the raw values with an EMA filter.
 */
public interface SystemLoadMonitor {

  /** Start the periodic CPU load sampling. Idempotent. */
  void start();

  /** Stop the background sampler. If this instance owns the scheduler it will be shut down. */
  void stop();

  /**
   * Return the EMA-smoothed CPU load (0.0 – 1.0).
   *
   * @return the smoothed CPU load, or 0.0 if not yet sampled
   */
  double getCpuLoadEMA();

  /**
   * Return the raw (unsmoothed) CPU load.
   *
   * @return the raw CPU load
   */
  double getCpuLoadRaw();
}
