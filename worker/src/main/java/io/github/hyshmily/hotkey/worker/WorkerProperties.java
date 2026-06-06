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
 * Configuration for the standalone HotKey Worker server.
 *
 * <p>Prefix: {@code hotkey.worker}.
 *
 * <p>Groups cover routing and sharding, AMQP exchange names, sliding-window
 * parameters, threshold settings, state-machine timings, dynamic threshold
 * adaptation, TopK pre-warm validation, and HeavyKeeper algorithm tuning.
 */
@Data
@ConfigurationProperties(prefix = "hotkey.worker")
public class WorkerProperties {

  /** Routing configuration for sharding app-level report queues. */
  @Data
  public static class Routing {

    private String appName = "default";
    private int shardCount = 1;
    private int shardIndex = 0;
  }

  /** AMQP exchange names for report and broadcast messages. */
  @Data
  public static class Messaging {

    private String reportExchange = "hotkey.report.exchange";
    private String broadcastExchange = "hotkey.broadcast.exchange";
  }

  /** Sliding-window parameters for local QPS tracking. */
  @Data
  public static class SlidingWindow {

    private long durationMs = 1000;

    @Min(1)
    private int slices = 10;
  }

  /** Absolute and ratio-based thresholds for HOT key classification. */
  @Data
  public static class Threshold {

    private long hotThreshold = 1000;
    private double hotThresholdRatio = 0.01;
  }

  /** State-machine timing for HOT/COOL decision transitions. */
  @Data
  public static class StateMachine {

    private long confirmDurationMs = 2000;
    private long coolDurationMs = 15000;
    private long preCoolGraceMs = 5000;
  }

  /** Dynamic threshold adaptation based on global QPS changes. */
  @Data
  public static class GlobalQpsDynamicThreshold {

    private double qpsChangeTolerance = 0.5;
    private long learningPeriodMs = 30_000;
    private double hotThresholdRatio = 0.01;
    private long recalculateIntervalMs = 60_000;
  }

  /** Pre-warm validation before emitting HOT decisions. */
  @Data
  public static class TopKValidation {

    private long validateIntervalMs = 60000;
    private int preWarmCount = 5;
    private int preWarmMinAppearances = 2;
  }

  /** HeavyKeeper TopK algorithm parameters for worker-side hot key detection. */
  @Data
  public static class HeavyKeeper {

    private int topK = 100;
    private int width = 20_000;
    private int depth = 10;
    private double decay = 0.9;
    private int minCount = 10;
  }

  @Data
  public static class Heartbeat {

    private int pingIntervalMs = 1_000;
  }

  private boolean enabled = false;
  private Routing routing = new Routing();
  private Messaging messaging = new Messaging();
  private SlidingWindow slidingWindow = new SlidingWindow();
  private Threshold threshold = new Threshold();
  private StateMachine stateMachine = new StateMachine();
  private GlobalQpsDynamicThreshold globalQpsDynamicThreshold = new GlobalQpsDynamicThreshold();
  private TopKValidation topKValidation = new TopKValidation();
  private HeavyKeeper heavyKeeper = new HeavyKeeper();
  private Heartbeat heartbeat = new Heartbeat();

  /**
   * Number of sliding-window slices that fit within the CONFIRM duration.
   *
   * @return window count for the confirm phase
   */
  public int getConfirmWindows() {
    double sliceMs = (double) slidingWindow.getDurationMs() / slidingWindow.getSlices();
    return (int) Math.ceil(stateMachine.getConfirmDurationMs() / sliceMs);
  }

  /**
   * Number of sliding-window slices that fit within the COOL duration.
   *
   * @return window count for the cool phase
   */
  public int getCoolWindows() {
    double sliceMs = (double) slidingWindow.getDurationMs() / slidingWindow.getSlices();
    return (int) Math.ceil(stateMachine.getCoolDurationMs() / sliceMs);
  }

  /**
   * Number of sliding-window slices that fit within the pre-cool grace period.
   *
   * @return window count for the pre-cool grace phase
   */
  public int getPreCoolGraceWindows() {
    double sliceMs = (double) slidingWindow.getDurationMs() / slidingWindow.getSlices();
    return (int) Math.ceil(stateMachine.getPreCoolGraceMs() / sliceMs);
  }
}
