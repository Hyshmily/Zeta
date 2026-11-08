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
package io.github.hyshmily.zeta.autoconfigure;

import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.util.clock.MonotonicStampClock;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Configuration properties bound from the {@code zeta.*} prefix.
 *
 * <p>Groups: TopK algorithm parameters (capacity, sketch width/depth,
 * decay, minimum count), L1 Caffeine cache limits, SingleFlight
 * deduplication settings, executor thread pool, TTL overrides (hard
 * and soft, for normal and hot keys), reporting cadence and queueing.
 *
 * <p>The concrete configuration bean is consumed directly by the cache core,
 * the circuit breaker and the endpoint. TTL, refresh and breaker reads are live;
 * construction-time fields (sketch geometry, pool sizes, queue capacities) are
 * read when their bean is created and do not follow a later rebind.
 */
@Data
@Validated
@ConfigurationProperties(prefix = "zeta.local")
public class ZetaProperties {

  private static final int CACHED_PROCESSORS = Runtime.getRuntime().availableProcessors();

  /** Number of top hot keys to track. */
  @Min(1)
  private int topK = 100;

  /** Width of the HeavyKeeper Count-Min Sketch (number of columns). */
  @Min(1)
  @Max(200_000)
  private int width = 50_000;

  /** Depth of the HeavyKeeper Count-Min Sketch (number of hash functions). */
  @Min(1)
  @Max(10)
  private int depth = 5;

  /** Decay factor for HeavyKeeper counter fading. */
  @Positive
  private double decay = 0.92;

  /** Minimum access count before a key is considered hot. */
  @Min(1)
  private int minCount = 10;

  /** Maximum number of in-flight deduplication entries. */
  @Min(1)
  private int inFlightMaxSize = 50_000;

  /** TTL in seconds for in-flight deduplication entries. */
  @Min(1)
  private int inFlightTtlSeconds = 5;

  /** Execution timeout in seconds for an in-flight cache load; queue wait is not counted. */
  @Min(1)
  private int inFlightTimeoutSeconds = 3;

  /** Core pool size for the HotKey async executor. */
  @Min(1)
  private int executorCorePoolSize = 8;

  /** Maximum pool size for the HotKey async executor. */
  @Min(1)
  private int executorMaxPoolSize = 32;

  /** Queue capacity for the HotKey async executor before rejection. */
  @Min(1)
  private int executorQueueCapacity = 2000;

  /**
   * Rejection policy when a pool's queue and max size are saturated (design decisions: api surface).
   * {@code ABORT} (default) throws {@link java.util.concurrent.RejectedExecutionException};
   * {@code CALLER_RUNS} runs the task on the submitting thread and back-pressures the caller.
   * Only the refresh fan-out pool honors {@code CALLER_RUNS}: the load and send pools always abort,
   * and a load-pool rejection is a suppressed read, never a miss.
   */
  private ExecutorRejection executorRejection = ExecutorRejection.ABORT;

  /**
   * The executor pool must be able to grow to its configured maximum: a core
   * size above the maximum makes {@link java.util.concurrent.ThreadPoolExecutor}
   * reject the construction.
   *
   * @return whether {@code executor-max-pool-size >= executor-core-pool-size}
   */
  @AssertTrue(message = "zeta.local.executor-max-pool-size must be >= zeta.local.executor-core-pool-size")
  public boolean isExecutorPoolSizeValid() {
    return executorMaxPoolSize >= executorCorePoolSize;
  }

  /** Rejection policy options for the HotKey async executor. */
  public enum ExecutorRejection {
    /** Throw {@link java.util.concurrent.RejectedExecutionException} (default). */
    ABORT,

    /** Run the rejected task on the submitting thread (back-pressure). */
    CALLER_RUNS,
  }

  /** Pool size for the shared HotKey scheduler (periodic tasks). */
  @Min(1)
  private int schedulerPoolSize = 8;

  /** Capacity of the expelled-key queue in HeavyKeeper. */
  @Min(1)
  private int expelledQueueCapacity = 10_000;

  /** Number of sliding time windows per sketch slot (power of two, rounded up). */
  @Min(2)
  @Max(10)
  private int sketchWindowCount = 4;

  /** Hard TTL (ms) for normal keys. {@code 0} means permanent (no hard expiry). */
  @Min(0)
  private long hardTtlMs = 300_000;

  /** Hard TTL (ms) for hot keys. {@code 0} means permanent (no hard expiry). */
  @Min(0)
  private long hotHardTtlMs = 3_600_000;

  /** Soft TTL (ms) for normal keys. {@code 0} disables soft expiry. */
  @Min(0)
  private long softTtlMs = 30_000;

  /** Soft TTL (ms) for hot keys. {@code 0} disables soft expiry. */
  @Min(0)
  private long hotSoftTtlMs = 300_000;

  /** Jitter ratio (0.0–1.0) applied to TTLs — e.g. 0.1 means ±10% random offset. */
  @Min(0)
  @Max(1)
  private double ttlJitterRatio = 0.05;

  /**
   * Maximum number of concurrent background refreshes for soft-expiry
   * management.
   *
   * <p>{@code 0} (the default) derives the bound from
   * {@link #executorMaxPoolSize} as half the pool, at least 1. Background
   * refreshes share {@code interruptibleLoadExecutor} with read-miss loads, so
   * the semaphore must stay below the pool size or it stops bounding anything:
   * a limit above the pool lets refreshes occupy every thread and park reads in
   * the queue. A positive value is clamped to {@link #executorMaxPoolSize}; a
   * configured value above it is logged at startup.
   */
  @Min(0)
  private int refreshMaxPools = 0;

  /**
   * Effective background-refresh concurrency bound: the configured value when
   * positive, otherwise half of {@link #executorMaxPoolSize} (at least 1),
   * clamped to {@link #executorMaxPoolSize} — the shared pool is the real bound.
   *
   * @return the effective refresh permit count, always {@code >= 1} and
   *         {@code <= executorMaxPoolSize}
   */
  public int effectiveRefreshMaxPools() {
    int maximum = refreshMaxPools > 0 ? refreshMaxPools : Math.max(1, executorMaxPoolSize / 2);
    return Math.min(maximum, executorMaxPoolSize);
  }

  /** Exchange name for app-to-Worker reportToWorker routing. */
  private String reportExchange = ZetaConstants.Exchange.REPORT;

  /** Interval in ms at which the reporter flushes batches to RabbitMQ. */
  private long reportIntervalMs = 50;

  /**
   * Maximum elapsed time (ms) between two sketch ingests on the shared access
   * counter (design decisions: access counting): reporter-sized tides merge into an accumulator and the
   * merged batch lands in the HeavyKeeper sketch whenever this cadence has
   * elapsed — the detector keeps its own 500 ms rhythm regardless of the
   * (usually faster) report tide. Also the tide base when no reporter is wired.
   */
  private long detectIntervalMs = 500;

  /**
   * design decisions: reporting feed-loop tuning of the reporter flush cadence (kernel
   * {@code damon_feed_loop_next_input} port). {@code off} disables the tuner;
   * {@code shadow} (default) computes and exposes the interval trajectory via
   * the {@code zeta.reporter.feedloop.*} gauges without applying it;
   * {@code on} applies each computed interval to the WaveCounter tide base —
   * sparse batches lengthen the interval (fewer, fuller AMQP messages), dense
   * batches shorten it (message size and detection lag stay bounded).
   */
  @NotNull
  private ReportIntervalTuning reportIntervalTuning = ReportIntervalTuning.SHADOW;

  /**
   * Goal keys per flush for the feed-loop tuner ({@code N}); the loop settles
   * the interval at which the smoothed arrival rate projects to this batch
   * size. Only meaningful when {@code report-interval-tuning} != off.
   */
  @Min(1)
  private long reportIntervalTargetBatch = 512;

  /**
   * Flush-interval floor for the feed-loop tuner (the scheduler cadence
   * bound; the WaveCounter tide floor is 50ms, so values below 50 are
   * effectively 50). Only meaningful when {@code report-interval-tuning} !=
   * off.
   */
  @Min(1)
  private long reportIntervalMinMs = 50;

  /**
   * Flush-interval ceiling for the feed-loop tuner — the detection-lag bound:
   * a new hot key waits at most this long before its first report reaches the
   * Worker. The empty-tide ladder stretches to 2x the (tuned) base, so the
   * effective idle worst case is 2x this value. Only meaningful when
   * {@code report-interval-tuning} != off.
   */
  @Min(1)
  private long reportIntervalMaxMs = 1000;

  /**
   * The feed-loop clamp must not be inverted: a ceiling below the floor would
   * make every computed interval resolve to the floor. Only checked when the
   * tuner is enabled — the two values are inert at {@code off}.
   *
   * @return whether the interval range is consistent
   */
  @AssertTrue(message = "zeta.local.report-interval-max-ms must be >= zeta.local.report-interval-min-ms")
  public boolean isReportIntervalRangeValid() {
    return reportIntervalTuning == ReportIntervalTuning.OFF || reportIntervalMaxMs >= reportIntervalMinMs;
  }

  /** Tuning modes for the reporter flush-cadence feed loop (design decisions: reporting). */
  public enum ReportIntervalTuning {
    /** No tuner: the flush cadence stays at {@code report-interval-ms}. */
    OFF,

    /** Compute and expose the trajectory via gauges; never apply (default). */
    SHADOW,

    /** Apply each computed interval to the WaveCounter tide base. */
    ON,
  }

  /**
   * Capacity of the reporter dispatch work queue, in batches. Consumers blocked on a
   * publish cannot drain, so this is the bound on a stalled broker's backlog; 1024
   * batches covers one 5 s staleness window at up to ~200 batches/s.
   */
  @Min(1)
  private int queueCapacity = 1_024;

  /** Timeout in ms for offering new entries to the reportToWorker queue. */
  @Min(1)
  private int queueOfferTimeoutMs = 100;

  /** Number of consumer threads for reportToWorker processing. 0 means auto-compute. */
  @Min(0)
  private int consumerCount = 0;

  /**
   * Effective consumer thread count: configured value, or max(4, availableProcessors/2).
   * Uses available CPU cores as a baseline — scales with machine capacity without
   * requiring static shard configuration in a dynamic topology.
   * A floor of 4 ensures adequate parallelism for reportToWorker dispatch even on small instances.
   *
   * @return the effective number of consumer threads
   */
  public int effectiveConsumerCount() {
    return consumerCount > 0 ? consumerCount : Math.max(4, CACHED_PROCESSORS / 2);
  }

  /** L1 cache sizing configuration. */
  @Data
  public static class CacheConfig {

    /**
     * Maximum number of entries in the L1 table — the process-wide bound.
     */
    private int maxSize = 100_000;

    /**
     * Maximum number of per-key invalidation tombstones
     * ({@code EntryLifecycle}). Bounds the anti-resurrection state under a
     * mass-invalidation burst; the tombstone only has to outlive in-flight
     * reads, so the bound is workload-tunable rather than fixed.
     */
    private int tombstoneMaximumSize = 10_000;

    /**
     * Per-key invalidation tombstone retention in seconds. Only has to outlive
     * an in-flight read/load (seconds in practice); past it a pre-invalidation
     * fill becomes admissible again.
     */
    private int tombstoneTtlSeconds = 600;
  }

  /** L1 cache configuration. */
  @Valid
  private CacheConfig cache = new CacheConfig();

  /**
   * Consistent-hash ring configuration for reportToWorker routing.
   *
   * <p>The app always routes reports through the consistent-hash ring
   * ({@link #virtualNodes} sizes it), so the same key maps to the same Worker
   * shard even when the shard set changes. {@link #enabled} is <b>not</b> a
   * routing switch — it only gates creation of the {@code /actuator/hotkeyring}
   * inspection endpoint (default {@code true}).
   */
  @Data
  public static class ConsistentHashing {

    /** Whether the {@code /actuator/hotkeyring} inspection endpoint is created (routing always uses the ring). */
    private boolean enabled = true;

    /** Number of virtual nodes per physical shard on the hash ring. */
    @Min(1)
    private int virtualNodes = 500;
  }

  /** Consistent-hash ring configuration for reportToWorker routing (see {@link ConsistentHashing}). */
  @Valid
  private ConsistentHashing consistentHashing = new ConsistentHashing();

  /**
   * Explicit instance ID for queue naming and node-id derivation.
   * Falls back to {@code server.port-HOSTNAME} (or {@code server.port-UUID} if {@code HOSTNAME} is unset) if empty.
   */
  private String instanceId = "";

  /**
   * Explicit 10-bit dataVersion node id (0..1023), embedded in every stamp the
   * stamp clock mints.
   *
   * <p>When unset it is derived from {@link #instanceId} (or the auto-detected
   * instance id). Two instances that resolve to the same node id can mint
   * equal stamps in the same millisecond; in a multi-instance deployment set
   * either a unique {@code node-id} per instance or unique instance ids.
   */
  @Min(0)
  @Max(1023)
  private Integer nodeId;

  /**
   * Largest forward distance (ms) an observed peer stamp may lead local
   * physical time before the stamp clock refuses to raise its floor.
   *
   * <p>Without a bound, one peer with a broken clock drags every node's
   * version floor into the future and keeps it there until physical time
   * catches up (design decisions: version stamps). The default is deliberately
   * looser than an HLC's 500 ms anti-drift delta because Zeta's LWW contract
   * already tolerates bounded clock skew: this rejects clearly broken clocks,
   * not ordinary NTP skew. Rejecting the floor bump does not reject the
   * message the stamp belongs to.
   */
  @Min(0)
  private long clockMaxForwardDriftMs = MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS;

  /**
   * Configuration for Worker heartbeat exchange and liveliness detection.
   * <p>
   * Controls the exchange name and the heartbeat reception timeout.
   */
  @Data
  public static class Heartbeat {

    /** Heartbeat exchange name. */
    private String exchangeName = ZetaConstants.Exchange.HEARTBEAT;

    /** Heartbeat timeout (ms). */
    @Min(1)
    private int timeoutMs = 10000;

    /**
     * Minimum alive workers for cluster health.
     * <p>0 (default) = derived threshold: one third of the observed Worker count,
     * rounded up, with a floor of 1 (e.g. 3 Workers observed → 1 alive is healthy;
     * 5 observed → 2 alive; 9 observed → 3 alive). A single surviving Worker is
     * intentionally accepted as a healthy cluster — see design decisions: heartbeat and health.
     * <p>Set to a positive value to override the derived threshold with an
     * absolute alive count.
     */
    @Min(0)
    private int minAliveWorkers;

    /**
     * Per-Worker adaptive liveness timeout (design decisions: heartbeat and health): each Worker's
     * effective timeout becomes the Jacobson/Karels estimate over its heartbeat
     * inter-arrival ({@code SRTT + 4×RTTVAR}, the Linux TCP RTO shape), clamped
     * into [{@link #adaptiveFloorMs}, {@link #timeoutMs}]. The static
     * {@code timeoutMs} stops being a flat threshold and becomes the warm-up
     * value and the ceiling. This tightens dead-Worker detection from the full
     * static window down to a few heartbeat intervals while widening
     * automatically under observed jitter.
     */
    private boolean adaptiveTimeout = true;

    /**
     * Lower clamp (ms) for the adaptive liveness timeout. Must stay above a few
     * heartbeat intervals (the Worker's default cadence is 1 s) so a single
     * lost beat or a short GC pause does not flap the Worker dead.
     * Default 3 000.
     */
    @Min(1)
    private long adaptiveFloorMs = 3_000;

    /**
     * The adaptive floor is the lower clamp and {@link #timeoutMs} the upper
     * clamp; an inverted pair would make the timeout stop being a ceiling.
     *
     * @return whether the adaptive window bounds are ordered
     */
    @AssertTrue(message = "zeta.local.heartbeat.adaptive-floor-ms must be <= zeta.local.heartbeat.timeout-ms")
    public boolean isAdaptiveWindowValid() {
      return !adaptiveTimeout || adaptiveFloorMs <= timeoutMs;
    }
  }

  /** Worker heartbeat and liveliness detection configuration. */
  @Valid
  private Heartbeat heartbeat = new Heartbeat();

  /**
   * Configuration for the sliding-window circuit breaker.
   *
   * <p>When enabled, protects remote calls (reader suppliers) from cascading failures.
   * Default is disabled — users should only enable when their cache-load suppliers
   * (e.g. database queries, remote API calls) are prone to timeout or error cascades.
   *
   * <p>The breaker reads every decision through this live configuration
   * (design decisions: load protection).
   */
  @Data
  public static class CircuitBreaker {

    /** Whether circuit breaker is enabled. Default {@code false}. */
    private boolean enabled = false;

    /** Sliding window duration in milliseconds. */
    @Min(1)
    private long windowTimeMs = 10_000;

    /** Number of buckets in the sliding window. */
    @Min(1)
    private int windowBuckets = 10;

    /** Failure rate threshold (0.0–1.0) — opens when exceeded. */
    @DecimalMin("0.0")
    @DecimalMax("1.0")
    private double failThreshold = 0.5;

    /** Minimum total requests in the window before the breaker evaluates the failure rate. */
    @Min(0)
    private long requestVolumeThreshold = 20;

    /**
     * How long to wait (ms) before allowing a half-open probe request. Also bounds a
     * HALF_OPEN episode: an episode with no probe activity (episode start, settlement
     * or abandonment) for this long is force-retired, so a hung probe cannot pin the
     * breaker in HALF_OPEN.
     */
    @Min(1)
    private long singleTestIntervalMs = 5_000;

    /** Whether to log state transitions. */
    private boolean logEnabled = true;

    /** Maximum number of probe requests allowed in the HALF_OPEN state. */
    @Min(1)
    private int halfOpenMaxProbes = 3;

    /**
     * Consecutive successes in HALF_OPEN state required to close the breaker.
     * Higher values prevent flapping from single lucky probe requests.
     * Default 3 balances fast recovery with stability.
     */
    @Min(1)
    private int consecutiveSuccessThreshold = 3;

    /**
     * Maximum live per-prefix breaker shards. An annotated key uses the cache
     * namespace before {@code CacheKeysPolicy.NAMESPACE_DELIMITER} ({@code U+0000});
     * a programmatic key uses the segment before the first {@code ':'}; a key with
     * neither shares one bucket. Bounds memory when the key space is unbounded
     * (Caffeine Window TinyLFU eviction past the cap, 30-minute access expiry). One
     * sick datasource trips only its own shard instead of silencing every key.
     */
    @Min(1)
    private int maxPrefixes = 64;

    /**
     * Exception class names (fully qualified) that should NOT trip the breaker.
     * For example, business-level exceptions like {@code IllegalArgumentException}
     * should not open the circuit — these are client errors, not system failures.
     * When non-empty and the thrown exception (or its cause) matches, it is
     * treated as a success for breaker accounting.
     */
    private List<String> excludeExceptions = new ArrayList<>();

    /**
     * Exception class names (fully qualified) that SHOULD trip the breaker.
     * When non-empty, ONLY these exceptions are counted as failures; all others
     * are treated as successes. When empty (default), all exceptions are counted.
     * Mutually exclusive with {@code excludeExceptions} in intent, but both
     * can be set — exclude checks run first.
     */
    private List<String> includeExceptions = new ArrayList<>();
  }

  /** Circuit breaker configuration. */
  @Valid
  private CircuitBreaker circuitBreaker = new CircuitBreaker();
}
