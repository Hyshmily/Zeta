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
import io.github.hyshmily.zeta.worker.confidence.BayesianConfidenceEstimator;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Configuration for the standalone HotKey Worker server.
 *
 * <p>Prefix: {@code zeta.worker}.
 *
 * <p>Groups cover routing, AMQP exchange names, sliding-window parameters,
 * threshold settings, state-machine timings, and dynamic threshold adaptation.
 *
 * Default constructor.
 */
@Data
@Validated
@ConfigurationProperties(prefix = "zeta.worker")
public class WorkerProperties {

  /**
   * Dedicated send buffer for HOT/COOL decision broadcasts (ADR-0061). Default constructor.
   *
   * <p>When enabled, {@code ReportConsumer} hands decision sends to a bounded
   * single-threaded executor instead of publishing synchronously on the AMQP
   * consumer thread, so a mass-heat event (thousands of keys transitioning in
   * one batch) cannot stall report consumption behind serial AMQP publishes.
   * Same ADR-0037 conventions as the App-side broadcast flush: drop-on-saturation
   * and one aggregated WARN per 10s window.
   */
  @Data
  public static class Broadcast {

    /**
     * Whether decision broadcasts are sent from the dedicated buffered
     * executor. {@code false} restores the legacy synchronous send on the
     * AMQP consumer thread.
     */
    private boolean bufferEnabled = true;

    /**
     * Capacity of the pending-decision queue. A decision arriving when the
     * queue is full is dropped immediately and its state rolled back on the
     * caller thread (the next evaluation re-emits it) — a lost decision
     * fails lenient per ADR-0007/ADR-0024.
     */
    @Min(1)
    private int bufferCapacity = 10_000;
  }

  /** Routing configuration for app-level reportToWorker queue naming. Default constructor. */
  @Data
  public static class Routing {

    private String appName = "default";
  }

  /** AMQP exchange names for reportToWorker and send messages. Default constructor. */
  @Data
  public static class Messaging {

    private String reportExchange = ZetaConstants.Exchange.REPORT;
    private String broadcastExchange = ZetaConstants.Exchange.BROADCAST;
    private String heartbeatExchange = ZetaConstants.Exchange.HEARTBEAT;
  }

  /** Container tuning for the reportToWorker-message RabbitMQ listener. Default constructor. */
  @Data
  public static class ReportConsumer {

    @Min(1)
    private int concurrentConsumers = 8;

    private int prefetchCount = 50;

    /**
     * Optional consumer-side staleness filter (ms). {@code 0} disables the
     * filter — staleness is then bounded by the queue's {@code x-message-ttl}
     * ({@link ReportQueue#messageTtlMs}) instead, avoiding a cross-host
     * wall-clock comparison between App and Worker (an App clock lag beyond
     * the threshold would otherwise silently blind that instance). When
     * enabled, only a positive age ({@code now - timestamp}) beyond the
     * threshold drops a report; a negative age (reporter clock ahead) always
     * passes.
     */
    @Min(0)
    private long stalenessThresholdMs = 0L;
  }

  /**
   * Broker-side backlog guardrails for the per-shard report queue.
   *
   * <p>The queue is durable; without a length cap a stalled consumer (GC storm,
   * lock contention) lets reports pile up unboundedly and pressures broker memory.
   * Lost reports are tolerated by design (ADR-0007), so the queue drops the
   * <em>oldest</em> messages first, keeping the freshest statistical signal.
   *
   * <p><b>Deployment note:</b> {@code x-*} queue arguments cannot be altered on an
   * existing queue. Upgrading from a version without these arguments requires
   * deleting the old queue first (drain traffic → delete queue → start new version),
   * otherwise RabbitAdmin re-declaration fails with PRECONDITION_FAILED.
   */
  @Data
  public static class ReportQueue {

    /** Maximum number of messages buffered in the report queue before drop-head applies. */
    @Min(1)
    private long maxLength = 10_000;

    /**
     * Per-message TTL (ms) inside the report queue. Reports sitting unconsumed
     * for this long are discarded by the broker — they are stale anyway (the
     * consumer-side staleness filter is disabled by default, see
     * {@link io.github.hyshmily.zeta.worker.ingest.ReportConsumer#stalenessThresholdMs}).
     */
    @Min(1000)
    private long messageTtlMs = 60_000;
  }

  /** Sliding-window parameters for local qps tracking. Default constructor. */
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

    /**
     * State-machine base window duration (ms). Each slice is
     * {@code smDurationMs / smSlices}; the confirm/cool/pre-cool window counts
     * derive from this slice size, so {@code 0} would make every window count
     * degenerate ({@code ceil(x/0.0) = Integer.MAX_VALUE}).
     */
    @Min(1)
    private long smDurationMs = 500;

    @Min(1)
    private int smSlices = 10;

    /** Total duration (ms) for HOT confirmation. */
    @Min(1)
    private long confirmDurationMs = 50;

    /** Duration (ms) a key must stay below threshold to be considered COLD. */
    @Min(1)
    private long coolDurationMs = 600_000;

    /** Grace period (ms) at the end of cool-down during which a key can silently revive. */
    @Min(1)
    private long preCoolGraceMs = 60_000;

    /**
     * Staleness threshold (ms): a key is evicted after this many milliseconds
     * without any report. Default = 2 × coolDurationMs = 20 minutes.
     *
     * <p>This property is <b>only</b> the staleness threshold — the cadence of
     * the periodic eviction scan is {@link #evictScanIntervalMs} (ADR-0060).
     */
    private long evictIntervalMs = 1_200_000;

    /**
     * Tiered staleness threshold (ms) for COLD-state keys: a COLD key that has
     * not been reported for this long is evicted on the periodic scan, while
     * CONFIRMED_HOT / PRE_COOLING keys keep the full {@link #evictIntervalMs}
     * retention (their eviction is what discharges the COOL broadcast
     * obligation). A COLD key's retained state only matters while it is still
     * being reported — a resumed key re-evaluates from scratch exactly as it
     * would after the full-threshold eviction — so the short tier bounds the
     * state map's memory under high key cardinality without touching any
     * broadcast semantics. Default 300_000 (5 min); a value
     * {@code >= evict-interval-ms} disables the tiering.
     */
    @Min(1)
    private long coldEvictIntervalMs = 300_000;

    /**
     * Interval (ms) between periodic eviction SCANs over the detector, state
     * machine, and evaluator maps (ADR-0060). Deliberately a separate knob from
     * {@link #evictIntervalMs} (the staleness threshold): the scan cadence is a
     * housekeeping-cost decision, the threshold a correctness decision (it must
     * stay &gt;= 2 × coolDurationMs so cooling keys are not evicted prematurely).
     * Default 1_200_000 (20 min) — equal to the staleness default so a stale key
     * is collected on the first scan after crossing the threshold.
     */
    @Min(1)
    private long evictScanIntervalMs = 1_200_000;

    /**
     * Minimum interval (ms) between periodic HOT rebroadcasts for a key that
     * stays in {@code CONFIRMED_HOT}. Recovers lost HOT broadcasts (ADR-0007
     * fire-and-forget) and caps fast-lane steady-state emission to one HOT
     * decision per interval (ADR-0024). Default 10 s; range [1s, 60s].
     *
     * <p>Bounds are correctness constraints (ADR-0068 addendum): below 1s the
     * rebroadcast degenerates into a per-report broadcast storm for every
     * continuously-hot key; above 60s a lost HOT decision — the whole pipeline
     * is fire-and-forget — can leave new instances un-prewarmed for minutes.
     * Enforced at binding time via {@code @Min}/{@code @Max}.
     */
    @Min(1000)
    @Max(60_000)
    private long rebroadcastIntervalMs = 10_000;
  }

  /** Dynamic threshold adaptation based on global qps changes. Default constructor. */
  @Data
  public static class GlobalQpsDynamicThreshold {

    private double qpsChangeTolerance = 0.5;
    private long learningPeriodMs = 30_000;
    private double hotThresholdRatio = 0.01;
    private long recalculateIntervalMs = 60_000;
  }

  /** Heartbeat (ping) interval configuration for worker-to-worker health signalling and config sync. Default constructor. */
  @Data
  public static class Heartbeat {

    private int pingIntervalMs = 1_000;
  }

  /** Bayesian confidence estimation for hot-key decisions. Default constructor. */
  @Data
  public static class Bayesian {

    private double priorMean = BayesianConfidenceEstimator.PRIOR_MEAN;
    private double priorStd = 2.0;
    private double likelihoodStd = 0.8;

    /**
     * Posterior probability at or above which a decision is HIGH confidence
     * (gates the expensive HOT broadcast); must be in (0, 1) and strictly
     * above {@link #mediumConfidenceThreshold}. The tuning protocol behind
     * the default is documented on
     * {@link io.github.hyshmily.zeta.worker.confidence.ProbabilityResult}.
     */
    private double highConfidenceThreshold = 0.95;

    /**
     * Posterior probability at or above which a decision is MEDIUM confidence
     * (gates CANDIDATE_HOT tracking); must be in (0, 1). The tuning protocol
     * behind the default is documented on
     * {@link io.github.hyshmily.zeta.worker.confidence.ProbabilityResult}.
     */
    private double mediumConfidenceThreshold = 0.76;
  }

  /** Fast-lane rules — bypass Bayesian state machine, broadcast on sliding-window threshold only. */
  @Data
  public static class FastLane {

    private boolean enabled = false;

    private List<FastLaneRule> rules = new ArrayList<>();

    /**
     * Interval (ms) between periodic full-set rule gossip broadcasts to peer
     * Workers (ADR-0025). A fresh or partitioned Worker converges within one
     * interval; local mutations additionally broadcast immediately. Default 60 s.
     */
    @Min(1000)
    private long gossipIntervalMs = 60_000;
  }

  /** A single fast-lane rule: key pattern + threshold. */
  @Data
  public static class FastLaneRule {

    private String keyPattern = "";
    private long threshold = 100;
  }

  private boolean enabled = false;

  @Valid
  private Routing routing = new Routing();

  @Valid
  private Messaging messaging = new Messaging();

  @Valid
  private Broadcast broadcast = new Broadcast();

  @Valid
  private ReportConsumer reportConsumer = new ReportConsumer();

  @Valid
  private ReportQueue reportQueue = new ReportQueue();

  @Valid
  private SlidingWindow slidingWindow = new SlidingWindow();

  @Valid
  private Threshold threshold = new Threshold();

  @Valid
  private StateMachine stateMachine = new StateMachine();

  @Valid
  private GlobalQpsDynamicThreshold globalQpsDynamicThreshold = new GlobalQpsDynamicThreshold();

  @Valid
  private Heartbeat heartbeat = new Heartbeat();

  @Valid
  private Bayesian bayesian = new Bayesian();

  @Valid
  private FastLane fastLane = new FastLane();

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
