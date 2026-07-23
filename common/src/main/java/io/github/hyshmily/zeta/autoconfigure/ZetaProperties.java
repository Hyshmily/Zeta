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
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Positive;
import java.util.ArrayList;
import java.util.List;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Configuration properties bound from the {@code zeta.*} prefix.
 *
 * <p>Groups: TopK algorithm parameters (capacity, sketch width/depth,
 * decay, minimum count), L1 Caffeine cache limits, SingleFlight
 * deduplication settings, executor thread pool, TTL overrides (hard
 * and soft, for normal and hot keys), and reporting shard topology.
 */
@Data
@Validated
@ConfigurationProperties(prefix = "zeta.local")
public class ZetaProperties {

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

  /** Maximum number of entries in the L1 Caffeine cache. */
  @Min(1)
  private int localCacheMaxSize = 1000;

  /** Default TTL in minutes for non-hot entries in the L1 cache. */
  @Min(1)
  private int localCacheTtlMinutes = 5;

  /** Maximum number of in-flight deduplication entries. */
  @Min(1)
  private int inflightMaxSize = 50_000;

  /** TTL in seconds for in-flight deduplication entries. */
  @Min(1)
  private int inflightTtlSeconds = 5;

  /** Timeout in seconds for waiting on an in-flight cache load. */
  @Min(1)
  private int inflightTimeoutSeconds = 3;

  /** Core pool size for the HotKey async executor. */
  @Min(1)
  private int executorCorePoolSize = 8;

  /** Maximum pool size for the HotKey async executor. */
  @Min(1)
  private int executorMaxPoolSize = 32;

  /** Queue capacity for the HotKey async executor before rejection. */
  @Min(1)
  private int executorQueueCapacity = 2000;

  /** Pool size for the shared HotKey scheduler (periodic tasks). */
  @Min(1)
  private int schedulerPoolSize = 8;

  /**
   * Expected number of Worker nodes in the cluster.
   * <p>Used by {@link ClusterHealthView} for majority-quorum health checks.
   * If set to 0 (default), the cluster is always considered unhealthy until
   * Worker heartbeats dynamically update the count.
   * <p>For production deployments with a fixed Worker count, set this to
   * the expected number of Worker instances for accurate health detection.
   */
  @Min(0)
  private int expectedWorkerCount = 0;

  /** Capacity of the expelled-key queue in HeavyKeeper. */
  @Min(1)
  private int expelledQueueCapacity = 50_000;

  /** Number of sliding time windows per sketch slot (ring buffer depth). Default 3. */
  @Min(1)
  @Max(10)
  private int sketchWindowCount = 3;

  /** Default hard TTL (ms) for normal keys — fallback when {@link #hardTtlMs} is not set. */
  private long defaultHardTtlMs = 300_000;

  /** Override hard TTL (ms) for normal keys. 0 means use {@link #defaultHardTtlMs}. */
  private long hardTtlMs;

  /** Default hard TTL (ms) for hot keys — fallback when {@link #hotHardTtlMs} is not set. */
  private long defaultHotHardTtlMs = 3_600_000;

  /** Override hard TTL (ms) for hot keys. 0 means use {@link #defaultHotHardTtlMs}. */
  private long hotHardTtlMs;

  /** Default soft TTL (ms) for normal keys — fallback when {@link #softTtlMs} is not set. */
  private long defaultSoftTtlMs = 30_000;

  /** Override soft TTL (ms) for normal keys. 0 means use {@link #defaultSoftTtlMs}. */
  private long softTtlMs;

  /** Default soft TTL (ms) for hot keys — fallback when {@link #hotSoftTtlMs} is not set. */
  private long defaultHotSoftTtlMs = 300_000;

  /** Override soft TTL (ms) for hot keys. 0 means use {@link #defaultHotSoftTtlMs}. */
  private long hotSoftTtlMs;

  /** TTL (seconds) for null/cache-miss entries. Kept short to avoid caching negative results. */
  private int nullValueTtlSeconds = 10;

  public long effectiveNullTtlMs() {
    return nullValueTtlSeconds > 0 ? nullValueTtlSeconds * 1000L : Long.MAX_VALUE;
  }

  /**
   * Effective hard TTL for normal keys.
   * Returns the override value if set, otherwise the default.
   *
   * @return the effective hard TTL in milliseconds
   */
  public long effectiveHardTtlMs() {
    return hardTtlMs > 0 ? hardTtlMs : defaultHardTtlMs;
  }

  /**
   * Effective hard TTL for hot keys.
   * Returns the override value if set, otherwise the default.
   *
   * @return the effective hot-key hard TTL in milliseconds
   */
  public long effectiveHotHardTtlMs() {
    return hotHardTtlMs > 0 ? hotHardTtlMs : defaultHotHardTtlMs;
  }

  /**
   * Effective soft TTL for normal keys.
   * Returns the override value if set, otherwise the default. Returns 0 if disabled.
   *
   * @return the effective soft TTL in milliseconds, or 0 if disabled
   */
  public long effectiveSoftTtlMs() {
    return softTtlMs > 0 ? softTtlMs : defaultSoftTtlMs;
  }

  /**
   * Effective soft TTL for hot keys.
   * Returns the override value if set, otherwise the default. Returns 0 if disabled.
   *
   * @return the effective hot-key soft TTL in milliseconds, or 0 if disabled
   */
  public long effectiveHotSoftTtlMs() {
    return hotSoftTtlMs > 0 ? hotSoftTtlMs : defaultHotSoftTtlMs;
  }

  /**
   * Whether any soft TTL is configured (normal or hot).
   *
   * @return {@code true} if either normal or hot key soft TTL is enabled
   */
  public boolean isSoftExpireEnabled() {
    return effectiveSoftTtlMs() > 0 || effectiveHotSoftTtlMs() > 0;
  }

  /** Jitter ratio (0.0–1.0) applied to TTLs — e.g. 0.1 means ±10% random offset. */
  @Min(0)
  @Max(1)
  private double ttlJitterRatio = 0.05;

  /** Maximum number of concurrent refresh tasks for soft-expiry management. */
  @Min(1)
  private int refreshMaxPools = 100;

  /**
   * TTL in minutes for Redis version keys used in stale-detection.
   * <p>MUST be far greater than the maximum lifetime of any L1 entry for the same key.
   * Setting this too low risks version key expiry and silent dataVersion wraparound
   * (INCR restarts from 1), causing {@link io.github.hyshmily.zeta.util.version.VersionGuard#shouldSkipForSync}
   * to reject legitimate updates because the new version is numerically lower than
   * the existing version cached on peer instances.
   * <p>Default 10080 minutes (7 days). Do not reduce unless you have verified that
   * no L1 entry in your deployment can outlive this TTL.
   */
  @Min(1)
  private int versionKeyTtlMinutes = 10080;

  /** Application name used for queue naming and routing keys. */
  private String appName = "default";

  /** Exchange name for app-to-Worker reportToWorker routing. */
  private String reportExchange = ZetaConstants.Exchange.REPORT;

  /** Interval in ms at which the reporter flushes batches to RabbitMQ. */
  private long reportIntervalMs = 50;

  /** Number of shards for reportToWorker partitioning (only used when consistent-hashing is disabled). */
  private int shardCount = 1;

  /** Capacity of the per-shard reportToWorker queue. */
  @Min(1)
  private int queueCapacity = 10_000;

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

  private static final int CACHED_PROCESSORS = Runtime.getRuntime().availableProcessors();

  /**
   * L1 cache sizing configuration.
   */
  @Data
  public static class CacheConfig {

    /** Maximum number of entries (ignored when {@link #maxWeight} > 0). */
    private int maxSize = 100_000;

    /** Memory weight limit (e.g. 268435456 for 256 MB). 0 = use {@link #maxSize}. */
    private long maxWeight;

    /** Single value size limit (bytes). 0 = unlimited. */
    @Min(0)
    private long maxValueSize;
  }

  /** L1 cache configuration. */
  @Valid
  private CacheConfig cache = new CacheConfig();

  /**
   * Configuration for consistent-hashing based reportToWorker routing.
   * <p>
   * When enabled, the app uses a consistent-hash ring to select the target
   * shard for each key, ensuring the same key always maps to the same Worker
   * shard even when the shard set changes.
   */
  @Data
  public static class ConsistentHashing {

    /** Whether consistent-hashing mode is enabled. */
    private boolean enabled = true;

    /** Number of virtual nodes per physical shard on the hash ring. */
    @Min(1)
    private int virtualNodes = 500;
  }

  /** Consistent-hashing configuration for reportToWorker routing. */
  @Valid
  private ConsistentHashing consistentHashing = new ConsistentHashing();

  /** Reporting BBR adaptive rate-limiter and CPU monitor configuration. */
  @Valid
  private ReporterLimiter reporter = new ReporterLimiter();

  /**
   * Configuration for the BBR adaptive rate-limiter and CPU monitor.
   * <p>
   * Controls CPU threshold, polling interval, EMA decay factor, BBR sliding
   * window parameters, and the cooldown period after a rate-limit drop.
   */
  @Data
  public static class ReporterLimiter {

    /** Whether BBR adaptive rate-limiting is enabled. */
    @Getter(AccessLevel.NONE)
    private boolean enabled = true;

    /** CPU threshold (0–1000, default 800 = 80 %). */
    @Min(0)
    @Max(1000)
    private int cpuThreshold = 800;

    /** CPU polling interval in milliseconds. */
    @Min(100)
    private long cpuPollIntervalMs = 500;

    /** EMA decay factor for CPU smoothing (0.0 – 1.0). */
    private double cpuDecay = 0.95;

    /** BBR sliding window size in milliseconds. */
    @Min(100)
    private long bbrWindowMs = 10_000;

    /** Number of buckets in the BBR sliding window. */
    @Min(2)
    private int bbrWindowBuckets = 100;

    /** Cooldown period in ms after a drop before allowing again. */
    @Min(0)
    private long bbrCooldownMs = 1_000;
  }

  /**
   * Explicit instance ID for queue naming.
   * Falls back to {@code server.port-HOSTNAME} (or {@code server.port-UUID} if {@code HOSTNAME} is unset) if empty.
   */
  private String instanceId = "";

  /**
   * Configuration for Worker heartbeat exchange and liveliness detection.
   * <p>
   * Controls the exchange name, timeout intervals for heartbeat reception,
   * PING verification, and the number of consecutive failures before
   * graceful degradation is triggered.
   */
  @Data
  public static class Heartbeat {

    /** Heartbeat exchange name. */
    private String exchangeName = ZetaConstants.Exchange.HEARTBEAT;
    /** Heartbeat timeout (ms). */
    private int timeoutMs = 10000;
    /** Verify interval (ms). */
    private long verifyIntervalMs = 5000;
    /** PING timeout (ms). */
    private long pingTimeoutMs = 3000;
    /** Degrade after consecutive failures. */
    private int degradeAfterFailures = 3;
    /** Max backoff (ms) for per-Worker exponential backoff between verification probes. */
    private long verifyMaxBackoffMs = 600_000;
    /**
     * Minimum alive workers for cluster health. 0 = use majority formula (knownWorkerCount / 2 + 1).
     * Set to a positive value to override the default majority threshold.
     */
    private int minAliveWorkers;
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
   */
  @Data
  public static class CircuitBreaker {

    /** Whether circuit breaker is enabled. Default {@code false}. */
    private boolean enabled = false;

    /** Sliding window duration in milliseconds. */
    private long windowTimeMs = 10_000;

    /** Number of buckets in the sliding window. */
    private int windowBuckets = 10;

    /** Failure rate threshold (0.0–1.0) — opens when exceeded. */
    private double failThreshold = 0.5;

    /** Minimum total requests in the window before the breaker evaluates the failure rate. */
    private long requestVolumeThreshold = 20;

    /** How long to wait (ms) before allowing a half-open probe request. */
    private long singleTestIntervalMs = 5_000;

    /** Whether to log state transitions. */
    private boolean logEnabled = true;

    /** Maximum number of probe requests allowed in the HALF_OPEN state. */
    private int halfOpenMaxProbes = 3;

    /**
     * Consecutive successes in HALF_OPEN state required to close the breaker.
     * Higher values prevent flapping from single lucky probe requests.
     * Default 3 balances fast recovery with stability.
     */
    private int consecutiveSuccessThreshold = 3;

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

  /**
   * Configuration for Spring {@code @Cacheable} / {@code @CachePut} / {@code @CacheEvict} integration.
   *
   * <p>When enabled, HotKey detects hot keys on all Spring Cache reads and applies
   * dynamic TTL via Caffeine L1.
   */
  @Data
  public static class SpringCache {

    /** Separator between cache name and key in the internal cache key string. */
    private String keySeparator = "::";
  }

  @Data
  public static class Sync {

    /** Delay (ms) before pending records are flushed to the sync publisher. */
    private long flushDelayMs = 500;

    /**
     * Maximum deferral (ms) before a pending flush is forced to fire, even
     * when new records keep arriving within the {@link #flushDelayMs} window.
     * Prevents indefinite debounce starvation under continuous writes.
     */
    private long maxDeferMs = 2_000;
  }

  @Data
  public static class CacheKey {

    /** Strip query parameters (?...) from cache keys for normalization. */
    private boolean stripQuery = false;
  }

  /** Cache sync (broadcast) configuration. */
  @Valid
  private Sync sync = new Sync();

  /** Cache key normalization configuration. */
  @Valid
  private CacheKey cacheKey = new CacheKey();

  /** Spring Cache integration configuration. */
  @Valid
  private SpringCache springCache = new SpringCache();

  /** Number of {@code SET NX} retries for distributed lock acquisition. */
  private int tryLockLockCount = 2;

  /** Number of {@code GET} inquiries after a transient {@code SET NX} failure. */
  private int tryLockInquiryCount = 1;

  /** Number of {@code DEL} retries for distributed lock release. */
  private int tryLockUnlockCount = 2;
}
