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
package io.github.hyshmily.zeta.worker.config;

import io.github.hyshmily.zeta.constants.ZetaConstants;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Configuration for the standalone HotKey Worker server.
 *
 * <p>Prefix: {@code zeta.worker}.
 *
 * <p>Groups cover routing, AMQP exchange names, sliding-window parameters,
 * threshold settings, state-machine timings, dynamic threshold adaptation,
 * TopK pre-warm validation, and HeavyKeeper algorithm tuning.
 *
 * Default constructor.
 */
@Data
@Validated
@ConfigurationProperties(prefix = "zeta.worker")
public class WorkerProperties {

  /** Routing configuration for app-level report queue naming. Default constructor. */
  @Data
  public static class Routing {

    private String appName = "default";
  }

  /** AMQP exchange names for report and send messages. Default constructor. */
  @Data
  public static class Messaging {

    private String reportExchange = ZetaConstants.Exchange.REPORT;
    private String broadcastExchange = ZetaConstants.Exchange.BROADCAST;
    private String heartbeatExchange = ZetaConstants.Exchange.HEARTBEAT;
  }

  /** Container tuning for the report-message RabbitMQ listener. Default constructor. */
  @Data
  public static class ReportConsumer {

    @Min(1)
    private int concurrentConsumers = 8;

    private int prefetchCount = 50;
  }

  /** Sliding-window parameters for local QPS tracking. Default constructor. */
  @Data
  public static class SlidingWindow {

    private long durationMs = 1000;

    @Min(1)
    private int slices = 10;
  }

  /** Absolute and ratio-based thresholds for HOT key classification. Default constructor. */
  @Data
  public static class Threshold {

    private long hotThreshold = 1000;
    private double hotThresholdRatio = 0.01;
  }

  /** State-machine timing for HOT/COOL decision transitions. Default constructor. */
  @Data
  public static class StateMachine {

    private long smDurationMs = 500;

    @Min(1)
    private int smSlices = 10;

    private long confirmDurationMs = 50;

    private long coolDurationMs = 600_000;
    private long preCoolGraceMs = 60_000;
    /** Interval for evicting stale sliding-window and state-machine state. */
    private long evictIntervalMs = 30_000;
  }

  /** Dynamic threshold adaptation based on global QPS changes. Default constructor. */
  @Data
  public static class GlobalQpsDynamicThreshold {

    private double qpsChangeTolerance = 0.5;
    private long learningPeriodMs = 30_000;
    private double hotThresholdRatio = 0.01;
    private long recalculateIntervalMs = 60_000;
  }

  /** Pre-warm validation before emitting HOT decisions. Default constructor. */
  @Data
  public static class TopKValidation {

    private long validateIntervalMs = 60000;
    private int preWarmCount = 5;
    private int preWarmMinAppearances = 2;
  }

  /** HeavyKeeper TopK algorithm parameters for worker-side hot key detection. Default constructor. */
  @Data
  public static class HeavyKeeper {

    private int topK = 100;
    private int width = 20_000;
    private int depth = 10;
    private double decay = 0.9;
    private int minCount = 10;
  }

  /** Heartbeat (ping) interval configuration for worker-to-worker health signalling and config sync. Default constructor. */
  @Data
  public static class Heartbeat {

    private int pingIntervalMs = 1_000;
  }

  /** Bayesian confidence estimation for hot-key decisions. Default constructor. */
  @Data
  public static class Bayesian {

    private double priorMean = 2.3026;
    private double priorStd = 2.0;
    private double likelihoodStd = 0.8;
  }

  /** TopK persistence to Redis for warm-start after Worker restart. Default constructor. */
  @Data
  public static class Persistence {

    /** Whether persistence is enabled. */
    private boolean enabled = false;

    /** Interval (ms) between periodic TopK snapshots. */
    private long persistIntervalMs = 30_000;

    /** Number of top keys to persist per snapshot. */
    private int topKCount = 100;

    /** Redis key prefix; final key = prefix + appName + ":" + nodeId. */
    private String redisKeyPrefix = "zeta:topk:worker:";

    /** TTL (days) for persisted TopK data in Redis. */
    private int ttlDays = 3;
  }

  private boolean enabled = false;

  @Valid
  private Routing routing = new Routing();

  @Valid
  private Messaging messaging = new Messaging();

  @Valid
  private ReportConsumer reportConsumer = new ReportConsumer();

  @Valid
  private SlidingWindow slidingWindow = new SlidingWindow();

  @Valid
  private Threshold threshold = new Threshold();

  @Valid
  private StateMachine stateMachine = new StateMachine();

  @Valid
  private GlobalQpsDynamicThreshold globalQpsDynamicThreshold = new GlobalQpsDynamicThreshold();

  @Valid
  private TopKValidation topKValidation = new TopKValidation();

  @Valid
  private HeavyKeeper heavyKeeper = new HeavyKeeper();

  @Valid
  private Heartbeat heartbeat = new Heartbeat();

  @Valid
  private Bayesian bayesian = new Bayesian();

  @Valid
  private Persistence persistence = new Persistence();

  /**
   * Number of state-machine time slices that fit within the CONFIRM duration.
   * Uses {@code stateMachine.smDurationMs / stateMachine.smSlices} as the slice size,
   * independent of the sliding-window detector's own slice timing.
   *
   * @return window count for the confirmation phase
   */
  public int getConfirmWindows() {
    double sliceMs = (double) stateMachine.getSmDurationMs() / stateMachine.getSmSlices();
    return (int) Math.ceil(stateMachine.getConfirmDurationMs() / sliceMs);
  }

  /**
   * Number of sliding-window slices that fit within the COOL duration.
   *
   * @return window count for the cool phase
   */
  public int getCoolWindows() {
    double sliceMs = (double) stateMachine.getSmDurationMs() / stateMachine.getSmSlices();
    return (int) Math.ceil(stateMachine.getCoolDurationMs() / sliceMs);
  }

  /**
   * Number of sliding-window slices that fit within the pre-cool grace period.
   *
   * @return window count for the pre-cool grace phase
   */
  public int getPreCoolGraceWindows() {
    double sliceMs = (double) stateMachine.getSmDurationMs() / stateMachine.getSmSlices();
    return (int) Math.ceil(stateMachine.getPreCoolGraceMs() / sliceMs);
  }
}
