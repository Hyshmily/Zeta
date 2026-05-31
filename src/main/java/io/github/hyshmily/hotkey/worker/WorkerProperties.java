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
package io.github.hyshmily.hotkey.worker;

import jakarta.validation.constraints.Min;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Master configuration for a Worker node, bound from {@code application.yml}
 * using the {@code hotkey.worker} prefix.
 *
 * <p>Groups: basic switch / messaging, shard routing, sliding window,
 * state machine, dynamic threshold learning, and Top‑K cross‑validation.
 */
@Data
@ConfigurationProperties(prefix = "hotkey.worker")
public class WorkerProperties {

  /** Master switch – set to {@code true} to activate the Worker. */
  private boolean enabled = false;

  /**
   * When {@code true} (default), Worker mode excludes App‑side beans
   * ({@code hotKeyDetector}, etc.). Set to {@code false} to let Worker
   * and App beans coexist in the same JVM for development or debugging.
   */
  private boolean exclusiveMode = true;

  /** Logical application name used as a tenant discriminator. */
  private String appName = "default";

  /** RabbitMQ exchange where clients publish report messages. */
  private String reportExchange = "hotkey.report.exchange";

  /** RabbitMQ exchange used for HOT/COOL broadcasts. */
  private String broadcastExchange = "hotkey.broadcast.exchange";

  /** Total number of shards. Clients route via {@code abs(hash) % shardCount}. */
  private int shardCount = 1;

  /**
   * Zero‑based index of the shard this Worker consumes.
   * The Worker binds to the queue {@code hotkey.report.{appName}.{shardIndex}}.
   */
  private int shardIndex = 0;

  /** Duration of the sliding window in milliseconds. */
  private long windowDurationMs = 1000;

  /**
   * Number of time slices within one window.
   * Higher values give finer granularity but use more memory per key.
   */
  @Min(1)
  private int windowSlices = 10;

  /**
   * Absolute hot‑key threshold. A positive value overrides the ratio‑based
   * threshold. {@code -1} means the ratio threshold is used.
   */
  private long hotThreshold = -1;

  /**
   * Hot‑key threshold as a fraction of estimated global QPS.
   * Used only when {@link #hotThreshold} is ≤ 0. Default 0.01 (1%).
   */
  private double hotThresholdRatio = 0.01;

  /**
   * Duration a key must stay above the threshold to be confirmed HOT.
   * Converted to window count by {@link #getConfirmWindows()}.
   */
  private long confirmDurationMs = 2000;

  /**
   * Duration a key must stay below the threshold to be considered fully COLD.
   * Converted to window count by {@link #getCoolWindows()}.
   */
  private long coolDurationMs = 15000;

  /**
   * Additional grace period at the end of cool‑down during which the key
   * enters PRE_COOLING and may silently revive to HOT without a broadcast.
   * Converted to window count by {@link #getPreCoolGraceWindows()}.
   */
  private long preCoolGraceMs = 5000;

  /**
   * Initial learning period after startup (milliseconds). No HOT/COOL
   * broadcasts are sent during this time while baseline traffic is gathered.
   */
  private long learningPeriodMs = 30000;

  /** Interval at which the dynamic threshold is recalculated (milliseconds). */
  private long recalculateIntervalMs = 60000;

  /**
   * Tolerance for QPS changes before the threshold is updated.
   * {@code 0.5} means a ±50% change is required to trigger an update.
   */
  private double qpsChangeTolerance = 0.5;

  /** Interval between Top‑K cross‑validation runs (milliseconds). */
  private long topkValidateIntervalMs = 60000;

  /** Number of top‑ranked keys eligible for pre‑warming or rescue. */
  private int topkPreWarmCount = 5;

  /** Minimum number of consecutive Top‑K appearances required before a key is confirmed for pre‑warming. */
  private int topkPreWarmMinAppearances = 2;


  /** Top‑K capacity for the Worker‑side HeavyKeeper. */
  private int workerTopK = 100;

  /** Width of the Worker‑side Count‑Min Sketch. */
  private int workerWidth = 20000;

  /** Depth of the Worker‑side Count‑Min Sketch. */
  private int workerDepth = 10;

  /** Decay factor for the Worker‑side HeavyKeeper. */
  private double workerDecay = 0.9;

  /** Minimum count threshold for the Worker‑side HeavyKeeper. */
  private int workerMinCount = 10;

  public int getConfirmWindows() {
    double sliceMs = (double) windowDurationMs / windowSlices;
    return (int) Math.ceil(confirmDurationMs / sliceMs);
  }

  public int getCoolWindows() {
    double sliceMs = (double) windowDurationMs / windowSlices;
    return (int) Math.ceil(coolDurationMs / sliceMs);
  }

  public int getPreCoolGraceWindows() {
    double sliceMs = (double) windowDurationMs / windowSlices;
    return (int) Math.ceil(preCoolGraceMs / sliceMs);
  }
}
