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

import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.policy.HeatTrigger;
import io.github.hyshmily.zeta.policy.KeyFormat;
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
 * deduplication settings, executor thread pool, TTL defaults (hard and
 * soft), background warming, reporting cadence and queueing.
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

  /**
   * Built-in hard TTL for normal keys. A non-positive {@code zeta.local.hard-ttl-ms} resolves to
   * this value; {@link Long#MAX_VALUE} is the permanent opt-in.
   */
  public static final long DEFAULT_HARD_TTL_MS = 300_000L;

  private static final int CACHED_PROCESSORS = Runtime.getRuntime().availableProcessors();

  /**
   * Physical key format for annotation namespaces that do not declare one. {@code TYPED} keeps
   * the type-tagged, collision-safe encoding; {@code PLAIN} produces {@code namespace:value} and
   * is available only to single-scalar namespaces.
   */
  @NotNull
  private KeyFormat keyFormat = KeyFormat.TYPED;

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

  /**
   * Base of the exponential collision-decay probability: a colliding unit removes one occupant
   * unit with probability {@code decay^count}. A larger value decays faster; a smaller value
   * protects an established count longer. Range {@code (0.0, 1.0]}.
   */
  @Positive
  @DecimalMax("1.0")
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

  /** Shared default core pool size for the refresh fan-out and both dedicated pools. */
  @Min(1)
  private int executorCorePoolSize = 8;

  /** Shared default maximum pool size for the refresh fan-out and both dedicated pools. */
  @Min(1)
  private int executorMaxPoolSize = 32;

  /** Shared default queue capacity for the refresh fan-out and both dedicated pools. */
  @Min(1)
  private int executorQueueCapacity = 2000;

  /**
   * Core size of the interruptible load pool; {@code 0} inherits
   * {@link #executorCorePoolSize}. A slow data source can widen this pool without
   * resizing the broadcast-send pool.
   */
  @Min(0)
  private int loadExecutorCorePoolSize;

  /** Core size of the broadcast-send pool; {@code 0} inherits {@link #executorCorePoolSize}. */
  @Min(0)
  private int sendExecutorCorePoolSize;

  /** Maximum size of the interruptible load pool; {@code 0} inherits {@link #executorMaxPoolSize}. */
  @Min(0)
  private int loadExecutorMaxPoolSize;

  /** Maximum size of the broadcast-send pool; {@code 0} inherits {@link #executorMaxPoolSize}. */
  @Min(0)
  private int sendExecutorMaxPoolSize;

  /** Queue capacity of the interruptible load pool; {@code 0} inherits {@link #executorQueueCapacity}. */
  @Min(0)
  private int loadExecutorQueueCapacity;

  /** Queue capacity of the broadcast-send pool; {@code 0} inherits {@link #executorQueueCapacity}. */
  @Min(0)
  private int sendExecutorQueueCapacity;

  /**
   * The effective load-pool core size: the load-specific value when set, otherwise
   * the shared default.
   *
   * @return the configured or inherited core size
   */
  public int effectiveLoadCorePoolSize() {
    return fallback(loadExecutorCorePoolSize, executorCorePoolSize);
  }

  /**
   * The effective load-pool maximum size: the load-specific value when set,
   * otherwise the shared default.
   *
   * @return the configured or inherited maximum size
   */
  public int effectiveLoadMaxPoolSize() {
    return fallback(loadExecutorMaxPoolSize, executorMaxPoolSize);
  }

  /**
   * The effective load-pool queue capacity: the load-specific value when set,
   * otherwise the shared default.
   *
   * @return the configured or inherited queue capacity
   */
  public int effectiveLoadQueueCapacity() {
    return fallback(loadExecutorQueueCapacity, executorQueueCapacity);
  }

  /**
   * The effective send-pool core size: the send-specific value when set, otherwise
   * the shared default.
   *
   * @return the configured or inherited core size
   */
  public int effectiveSendCorePoolSize() {
    return fallback(sendExecutorCorePoolSize, executorCorePoolSize);
  }

  /**
   * The effective send-pool maximum size: the send-specific value when set,
   * otherwise the shared default.
   *
   * @return the configured or inherited maximum size
   */
  public int effectiveSendMaxPoolSize() {
    return fallback(sendExecutorMaxPoolSize, executorMaxPoolSize);
  }

  /**
   * The effective send-pool queue capacity: the send-specific value when set,
   * otherwise the shared default.
   *
   * @return the configured or inherited queue capacity
   */
  public int effectiveSendQueueCapacity() {
    return fallback(sendExecutorQueueCapacity, executorQueueCapacity);
  }

  /** A positive override wins; otherwise the shared default applies. */
  private static int fallback(int value, int defaultValue) {
    return value > 0 ? value : defaultValue;
  }

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

  /**
   * Each pool must be able to grow to its own effective maximum.
   *
   * @return whether both effective maximums are at least their effective core sizes
   */
  @AssertTrue(
    message = "the effective load and send executor max pool sizes must be >= their effective core pool sizes"
  )
  public boolean isDedicatedPoolSizeValid() {
    return (
      effectiveLoadMaxPoolSize() >= effectiveLoadCorePoolSize() &&
      effectiveSendMaxPoolSize() >= effectiveSendCorePoolSize()
    );
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

  /**
   * Hard TTL (ms) for normal keys: {@code 0} and {@code -1} mean "the built-in default"
   * ({@link #DEFAULT_HARD_TTL_MS}), and {@link Long#MAX_VALUE} means permanent (no hard expiry).
   * A hard TTL has no "off" state, so neither {@code 0} nor {@code -1} can disable it.
   *
   * <p>One TTL vocabulary everywhere: {@code 0}/{@code -1} inherit, every positive value is used
   * exactly as written, {@link Long#MAX_VALUE} never expires.
   */
  @Min(-1)
  private long hardTtlMs = DEFAULT_HARD_TTL_MS;

  /**
   * Soft TTL (ms) for normal keys: {@code 0} and {@code -1} mean "no background refresh", which is
   * the built-in default; a positive value is the soft window, and {@link Long#MAX_VALUE} means an
   * entry never becomes stale.
   *
   * <p>Off by default because a background refresh runs the loader outside the caller's context —
   * for an annotated read that is a reflective call without the transaction, security or MDC of
   * the invocation that missed. A method whose body depends on request-scoped state must not be
   * given a soft TTL; enabling it globally is an explicit deployment decision.
   */
  @Min(-1)
  private long softTtlMs = 0;

  /** Jitter ratio (0.0–1.0) applied to TTLs — e.g. 0.1 means ±10% random offset. */
  @DecimalMin("0.0")
  @DecimalMax("1.0")
  private double ttlJitterRatio = 0.05;

  /**
   * The resolved normal hard TTL. This getter is the single place a non-positive configuration
   * becomes the built-in default, so every consumer reads a duration it can act on: positive, or
   * {@link Long#MAX_VALUE} for permanent.
   *
   * @return the normal hard TTL in milliseconds, always positive
   */
  public long getHardTtlMs() {
    return hardTtlMs > 0L ? hardTtlMs : DEFAULT_HARD_TTL_MS;
  }

  /**
   * The resolved normal soft TTL. Non-positive means "no soft window", which is the built-in
   * default.
   *
   * @return the normal soft TTL in milliseconds, {@code 0} when disabled
   */
  public long getSoftTtlMs() {
    return softTtlMs > 0L ? softTtlMs : 0L;
  }

  /**
   * {@code zeta.local.hard-ttl-ms} must be a duration the packed entry model can store.
   *
   * <p>The four TTL properties are bound at startup but only reach
   * {@link io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry} on the first write, so an
   * out-of-range value would otherwise start cleanly and fail on a request thread with an
   * {@link IllegalArgumentException}. Validating here fails the context instead.
   *
   * @return whether the value is {@code -1} (inherit), in {@code [0, TTL_MAX_MS]}, or
   *         {@link Long#MAX_VALUE} (permanent)
   */
  @AssertTrue(
    message = "zeta.local.hard-ttl-ms must be -1 or 0 (inherit), a duration up to 2147483647000 ms, or Long.MAX_VALUE (permanent)"
  )
  public boolean isHardTtlEncodable() {
    return isDeclaredTtlEncodable(hardTtlMs);
  }

  /**
   * {@code zeta.local.soft-ttl-ms} must be a duration the packed entry model can store.
   *
   * @return whether the value is {@code -1} (inherit), in {@code [0, TTL_MAX_MS]}, or
   *         {@link Long#MAX_VALUE} (never stale)
   */
  @AssertTrue(
    message = "zeta.local.soft-ttl-ms must be -1 or 0 (inherit), a duration up to 2147483647000 ms, or Long.MAX_VALUE (never stale)"
  )
  public boolean isSoftTtlEncodable() {
    return isDeclaredTtlEncodable(softTtlMs);
  }

  /** One TTL property against the entry model's range, with {@code -1} = inherit allowed. */
  private static boolean isDeclaredTtlEncodable(long ttlMs) {
    return ttlMs == -1L || CacheEntry.isEncodableTtl(ttlMs);
  }

  /**
   * Maximum number of concurrent background refreshes for soft-expiry
   * management.
   *
   * <p>{@code 0} (the default) derives the bound from
   * {@link #effectiveLoadMaxPoolSize()} as half the pool, at least 1. Background
   * refreshes share {@code interruptibleLoadExecutor} with read-miss loads, so
   * the semaphore must stay below the pool size or it stops bounding anything:
   * a limit above the pool lets refreshes occupy every thread and park reads in
   * the queue. A positive value is clamped to the effective load-pool maximum; a
   * configured value above it is logged at startup.
   */
  @Min(0)
  private int refreshMaxPools = 0;

  /**
   * Effective background-refresh concurrency bound: the configured value when
   * positive, otherwise half of {@link #effectiveLoadMaxPoolSize()} (at least 1),
   * clamped to the effective load-pool maximum — that pool is the real bound.
   *
   * @return the effective refresh permit count, always {@code >= 1} and
   *         {@code <= effectiveLoadMaxPoolSize()}
   */
  public int effectiveRefreshMaxPools() {
    int loadMax = effectiveLoadMaxPoolSize();
    int maximum = refreshMaxPools > 0 ? refreshMaxPools : Math.max(1, loadMax / 2);
    return Math.min(maximum, loadMax);
  }

  /**
   * Execution budget of one background soft refresh or hard renewal. The refresh permit and the
   * key's dedup slot are held for the whole budget, so a stuck source blocks that key's next
   * refresh for this long; keep it below the shortest hard TTL the application declares.
   */
  @Min(1)
  private long refreshTimeoutMs = 30_000L;

  /** Exchange name for app-to-Worker reportToWorker routing. */
  private String reportExchange = ZetaConstants.Exchange.REPORT;

  /** Interval in ms at which the reporter flushes batches to RabbitMQ. */
  @Min(1)
  private long reportIntervalMs = 50;

  /**
   * Maximum elapsed time (ms) between two sketch ingests on the shared access
   * counter (design decisions: access counting): reporter-sized tides merge into an accumulator and the
   * merged batch lands in the HeavyKeeper sketch whenever this cadence has
   * elapsed — the detector keeps its own 500 ms rhythm regardless of the
   * (usually faster) report tide. Also the tide base when no reporter is wired.
   */
  @Min(1)
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
    @Min(1)
    private int maxSize = 100_000;

    /**
     * Maximum number of per-key invalidation tombstones
     * ({@code EntryLifecycle}). Bounds the anti-resurrection state under a
     * mass-invalidation burst; the tombstone only has to outlive in-flight
     * reads, so the bound is workload-tunable rather than fixed.
     */
    @Min(1)
    private int tombstoneMaximumSize = 10_000;

    /**
     * Per-key invalidation tombstone retention in seconds. Only has to outlive
     * an in-flight read/load (seconds in practice); past it a pre-invalidation
     * fill becomes admissible again.
     */
    @Min(1)
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

    /**
     * Every bucket must span at least one millisecond: {@code windowTimeMs} is divided by
     * {@code windowBuckets} when the breaker shards are built, and an inverted pair otherwise
     * fails at the first {@link io.github.hyshmily.zeta.cache.cachesupport.circuitbreaker.SlidingWindowBreaker}
     * construction instead of at binding time.
     *
     * @return whether the window geometry is consistent
     */
    @AssertTrue(
      message = "zeta.local.circuit-breaker.window-time-ms must be >= zeta.local.circuit-breaker.window-buckets"
    )
    public boolean isWindowGeometryValid() {
      return windowTimeMs >= windowBuckets;
    }

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
     * A name that cannot be loaded matches nothing, so a list whose every name
     * fails counts no exception as a failure.
     * Mutually exclusive with {@code excludeExceptions} in intent, but both
     * can be set — exclude checks run first.
     */
    private List<String> includeExceptions = new ArrayList<>();
  }

  /** Circuit breaker configuration. */
  @Valid
  private CircuitBreaker circuitBreaker = new CircuitBreaker();

  /**
   * Background warming configuration: whether the framework may spend a background load on a
   * key, and under which heat signal.
   */
  @Data
  public static class Warm {

    /** Global kill switch for prewarm and keep-warm. */
    private boolean enabled = true;

    /** Upper bound on cluster lease records retained by {@code HeatDirectory}. */
    @Min(1)
    private int maxClusterLeases = 50_000;

    /**
     * Retention of a COOL tombstone. It must outlive the Worker's in-flight decision window so a
     * late HOT below the tombstone's version cannot resurrect the lease.
     */
    @Min(1_000)
    private long coolTombstoneMs = 60_000;

    /** Prewarm a missing entry when the configured heat signal fires. */
    @Valid
    private Prewarm prewarm = new Prewarm();

    /** Renew a resident entry before its soft or hard deadline. */
    @Valid
    private KeepWarm keepWarm = new KeepWarm();

    /** Operator trigger overrides, keyed by annotation cache name or programmatic prefix. */
    @Valid
    private Overrides overrides = new Overrides();

    /** Cold-load configuration. */
    @Data
    public static class Prewarm {

      /** Which signal may prewarm. {@code OFF}: a namespace must opt in explicitly. */
      @NotNull
      private HeatTrigger trigger = HeatTrigger.OFF;

      /** Maximum random delay before a prewarm, staggering nodes that receive one broadcast. */
      @Min(0)
      private long jitterMs = 50;
    }

    /** Resident-renewal configuration. */
    @Data
    public static class KeepWarm {

      /**
       * Which signal may renew a resident entry. {@code OFF}: registering a loader does not opt a
       * namespace into spending background loads; the namespace must set the trigger explicitly.
       */
      @NotNull
      private HeatTrigger trigger = HeatTrigger.OFF;

      /** Upper bound on scheduled warm tasks; excess admissions are dropped. */
      @Min(1)
      private int maxTasks = 100_000;

      /** TTL windows shorter than this are not worth a background renewal. */
      @Min(1)
      private long minWindowMs = 2_000;

      /** Maximum lead time before the deadline. */
      @Min(1)
      private long leadMaxMs = 30_000;
    }

    /** Operator overrides. Only triggers are overridable; TTLs and loaders stay single-owned. */
    @Data
    public static class Overrides {

      /** Annotation cache name to trigger override. */
      private java.util.Map<String, TriggerOverride> caches = new java.util.LinkedHashMap<>();

      /** Programmatic key prefix to trigger override. */
      private java.util.Map<String, TriggerOverride> prefixes = new java.util.LinkedHashMap<>();
    }

    /** One namespace's trigger override; a null field leaves the declared value in place. */
    @Data
    public static class TriggerOverride {

      /** Prewarm trigger override, or {@code null} to keep the declaration. */
      private HeatTrigger prewarm;

      /** Keep-warm trigger override, or {@code null} to keep the declaration. */
      private HeatTrigger keepWarm;
    }
  }

  /** Background warming configuration. */
  @Valid
  private Warm warm = new Warm();
}
