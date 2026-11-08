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
import io.github.hyshmily.zeta.worker.detection.rule.CoolStrategy;
import io.github.hyshmily.zeta.worker.detection.rule.MatchType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.time.Duration;
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
 * <p>The detection half is one {@code detection} block: one shared window, one required default
 * rule, an ordered rule list, the cool-down scheduler's cadences, and the per-Worker capacity and
 * broadcast-rate limits. Thresholds are expressed in <b>QPS</b> and converted to window counts
 * once at startup, so changing the window changes every rule's meaning in one place.
 *
 * <p>Fields that participate in "this must be configured" are boxed ({@link Integer},
 * {@link Duration}) rather than primitives with defaults: a default value cannot be told apart
 * from an omitted one, so the startup validator could not enforce completeness.
 */
@Data
@Validated
@ConfigurationProperties(prefix = "zeta.worker")
public class WorkerProperties {

  /** Whether this process is a Worker. */
  private boolean enabled = false;

  /**
   * Soft admission cap on the number of <b>distinct keys</b> the window table tracks at once. A
   * key already tracked keeps counting; a brand-new key at the cap first reclaims an idle window
   * and is rejected only when a bounded sample finds none. A hard headroom above the cap bounds
   * the overshoot from contended admissions. {@code 0} disables the cap.
   */
  @Min(0)
  private long maxTrackedKeys = 1_000_000L;

  /**
   * Epoch file for restart detection. Must survive restarts: the Worker increments the stored
   * value on every start, and Apps use the epoch to accept decisions from a newer incarnation.
   * Empty means {@code ${java.io.tmpdir}/zeta-epoch-<workerId>} (with filesystem-significant
   * characters in the id replaced by {@code _}), which a container restart wipes.
   */
  private String epochFile = "";

  @Valid
  private Messaging messaging = new Messaging();

  @Valid
  private Broadcast broadcast = new Broadcast();

  @Valid
  private ReportConsumer reportConsumer = new ReportConsumer();

  @Valid
  private ReportQueue reportQueue = new ReportQueue();

  @Valid
  private Heartbeat heartbeat = new Heartbeat();

  @Valid
  private Detection detection = new Detection();

  /** AMQP exchange names for reports, decisions and heartbeats. */
  @Data
  public static class Messaging {

    private String reportExchange = ZetaConstants.Exchange.REPORT;
    private String broadcastExchange = ZetaConstants.Exchange.BROADCAST;
    private String heartbeatExchange = ZetaConstants.Exchange.HEARTBEAT;
  }

  /**
   * Decision delivery: two bounded queues drained by one thread under token buckets
   * (design decisions: decision delivery).
   */
  @Data
  public static class Broadcast {

    /** Pending HOT decisions. Full means the HOT is dropped and its promotion rolled back. */
    @Min(1)
    private int hotQueueCapacity = 10_000;

    /**
     * Pending COOL decisions before drop-on-saturation. A COOL is emitted after its seat is
     * released and the same key can cool again before the first COOL drains, so this is a burst
     * buffer, not a per-seat guarantee; a dropped COOL converges through the App's hard TTL.
     */
    @Min(1)
    private int coolQueueCapacity = 10_000;

    /** Most COOL decisions sent in one drain round, before yielding to HOT. */
    @Min(1)
    private int coolBurst = 32;

    /** Most HOT decisions sent in one drain round; COOL is capped first, so every round reaches HOT too. */
    @Min(1)
    private int hotBurst = 8;

    /**
     * Maximum time a decision may wait in its queue before the drain drops it as expired. The
     * broadcaster retains a cold accepted record for twice this value and a hot record until a COOL
     * is accepted, so the decision age is the only ordering bound an operator has to reason about
     * for cold keys.
     */
    @Min(1)
    @Max(3_600_000L)
    private long maxDecisionAgeMs = 30_000L;
  }

  /** Container tuning for the report listener. */
  @Data
  public static class ReportConsumer {

    @Min(1)
    private int concurrentConsumers = 8;

    @Min(0)
    private int prefetchCount = 50;

    /**
     * Optional consumer-side staleness filter (ms). {@code 0} disables it — staleness is then
     * bounded by the queue's {@code x-message-ttl}, avoiding a cross-host wall-clock comparison
     * that would silently blind an App whose clock lags.
     */
    @Min(0)
    private long stalenessThresholdMs = 0L;
  }

  /**
   * Broker-side guardrails for the per-shard report queue.
   *
   * <p><b>Deployment note:</b> {@code x-*} queue arguments cannot be altered on an existing
   * queue; changing these requires deleting the old queue first.
   */
  @Data
  public static class ReportQueue {

    @Min(1)
    private long maxLength = 10_000;

    @Min(1000)
    private long messageTtlMs = 60_000;
  }

  /** Worker-to-Worker heartbeat. */
  @Data
  public static class Heartbeat {

    @Min(100)
    private int pingIntervalMs = 1_000;

    /**
     * Grace period before the Worker reports ready with an empty HOT registry. Only gates the
     * ring-admission signal.
     */
    @Min(0)
    private long readyGraceMs = 3_000;
  }

  /** The detection plane. */
  @Data
  public static class Detection {

    /**
     * {@code false} keeps the listener but skips every evaluation, counting the dropped reports.
     * Removing the listener instead would let the report queue back up.
     */
    private boolean enabled = true;

    @Valid
    private Window window = new Window();

    /** Rule used for keys nothing else matches. Required in full when detection is enabled. */
    @Valid
    private Defaults defaults = new Defaults();

    /** Rules in configuration order; precedence is computed at startup. */
    @Valid
    private List<Rule> rules = new ArrayList<>();

    @Valid
    private Cooling cooling = new Cooling();

    @Valid
    private Limits limits = new Limits();
  }

  /** The one sliding window every rule shares. */
  @Data
  public static class Window {

    /** Configured window length; the effective length is {@code alignedSlices * sliceMs}. */
    private Duration duration = Duration.ofSeconds(1);

    /** Slice count; rounded up to a power of two. */
    @Min(1)
    private int slices = 16;

    /** Idle-window reclamation threshold. Memory only: an idle window sums to zero anyway. */
    private Duration retention = Duration.ofMinutes(2);
  }

  /** The mandatory fallback rule. */
  @Data
  public static class Defaults {

    private Integer hotQps;

    @Valid
    private Cool cool;
  }

  /** One rule. Every rule is complete; nothing is inherited from {@link Defaults}. */
  @Data
  public static class Rule {

    private String id;

    @Valid
    private Match match;

    /** Higher wins; the only knob an operator needs to override type specificity. */
    private int priority;

    private Integer hotQps;

    @Valid
    private Cool cool;
  }

  /** How a rule selects keys. */
  @Data
  public static class Match {

    private MatchType type;

    private String key;
  }

  /**
   * Cool-down parameters.
   *
   * <p>The fields used depend on {@link #strategy}: {@code COOL_FOR_LOW_QPS} uses
   * {@code coolQps} + {@code lowQpsFor}, {@code COOL_FOR_EXPIRE} uses {@code coolAfter}. Setting
   * a field the selected strategy does not read is a startup failure, not a silent no-op.
   */
  @Data
  public static class Cool {

    private CoolStrategy strategy;

    private Integer coolQps;

    private Duration lowQpsFor;

    private Duration coolAfter;
  }

  /** Cool-down scheduler cadences. */
  @Data
  public static class Cooling {

    /**
     * LOW_QPS re-check period. Upper bound is the window length (a slower re-check cannot see the
     * window it is judging); lower bound is one slice — it sets the per-key check rate
     * {@code max-active-hot-keys / interval}.
     */
    private Duration lowQpsCheckInterval = Duration.ofSeconds(1);

    /** Period of the timer-repair scan; a lost check is late by at most this interval plus the repair grace. */
    private Duration sweepInterval = Duration.ofSeconds(30);

    /**
     * Number of striped cool-down wheels, one advancing thread each. The wheel's links and clock
     * are single-owner, so a key space split across wheels is what parallelizes the checks.
     */
    @Min(1)
    private int threads = 2;
  }

  /** Per-Worker capacity and broadcast budget. */
  @Data
  public static class Limits {

    /** HOT seats. A promotion takes one before the entry becomes visible; a cool-down returns it after the removal. */
    @Min(1)
    private int maxActiveHotKeys = 10_000;

    @Min(1)
    private int maxHotBroadcastsPerSecond = 500;

    @Min(1)
    private int maxCoolBroadcastsPerSecond = 500;
  }
}
