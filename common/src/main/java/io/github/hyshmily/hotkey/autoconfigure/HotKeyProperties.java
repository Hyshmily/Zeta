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
package io.github.hyshmily.hotkey.autoconfigure;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Positive;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Configuration properties bound from the {@code hotkey.*} prefix.
 *
 * <p>Groups: TopK algorithm parameters (capacity, sketch width/depth,
 * decay, minimum count), L1 Caffeine cache limits, SingleFlight
 * deduplication settings, executor thread pool, TTL overrides (hard
 * and soft, for normal and hot keys), and reporting shard topology.
 */
@Data
@Validated
@ConfigurationProperties(prefix = "hotkey.local")
public class HotKeyProperties {

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

  /** Access-based TTL in minutes for the L1 cache (0 disables). */
  @Min(0)
  private int localCacheAccessTtlMinutes = 0;

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

  /** Capacity of the expelled-key queue in HeavyKeeper. */
  @Min(1)
  private int expelledQueueCapacity = 50_000;

  /** @deprecated No longer used; decay is driven by {@link io.github.hyshmily.hotkey.autoconfigure.HotKeySchedulingConfiguration#cleanHotKeys()} with a configurable fixed delay. */
  @Deprecated
  private int decayPeriod = 20;

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

  /**
   * Effective hard TTL for normal keys.
   * Returns the override value if set, otherwise the default.
   */
  public long effectiveHardTtlMs() {
    return hardTtlMs > 0 ? hardTtlMs : defaultHardTtlMs;
  }

  /**
   * Effective hard TTL for hot keys.
   * Returns the override value if set, otherwise the default.
   */
  public long effectiveHotHardTtlMs() {
    return hotHardTtlMs > 0 ? hotHardTtlMs : defaultHotHardTtlMs;
  }

  /**
   * Effective soft TTL for normal keys.
   * Returns the override value if set, otherwise the default. Returns 0 if disabled.
   */
  public long effectiveSoftTtlMs() {
    return softTtlMs > 0 ? softTtlMs : defaultSoftTtlMs;
  }

  /**
   * Effective soft TTL for hot keys.
   * Returns the override value if set, otherwise the default. Returns 0 if disabled.
   */
  public long effectiveHotSoftTtlMs() {
    return hotSoftTtlMs > 0 ? hotSoftTtlMs : defaultHotSoftTtlMs;
  }

  /** Whether any soft TTL is configured (normal or hot). */
  public boolean isSoftExpireEnabled() {
    return effectiveSoftTtlMs() > 0 || effectiveHotSoftTtlMs() > 0;
  }

  /** Maximum number of concurrent refresh tasks for soft-expiry management. */
  @Min(1)
  private int refreshMaxPools = 100;

  /** TTL in minutes for Redis version keys used in stale-detection. */
  @Min(1)
  private int versionKeyTtlMinutes = 60;

  /** Application name used for queue naming and routing keys. */
  private String appName = "default";

  /** Exchange name for app-to-Worker report routing. */
  private String reportExchange = "hotkey.report.exchange";

  /** Interval in ms at which the reporter flushes batches to RabbitMQ. */
  private long reportIntervalMs = 100;

  /** Number of shards for report partitioning. */
  private int shardCount = 1;

  /** Capacity of the per-shard report queue. */
  @Min(1)
  private int queueCapacity = 10_000;

  /** Timeout in ms for offering new entries to the report queue. */
  @Min(1)
  private int queueOfferTimeoutMs = 100;

  /** Number of consumer threads for report processing. 0 means auto-compute. */
  @Min(0)
  private int consumerCount = 0;

  /**
   * Effective consumer thread count: configured value, or max(1, shardCount/2).
   *
   * @return the effective number of consumer threads
   */
  public int effectiveConsumerCount() {
    return consumerCount > 0 ? consumerCount : Math.max(1, shardCount / 2);
  }

  /**
   * Configuration for consistent-hashing based report routing.
   * <p>
   * When enabled, the app uses a consistent-hash ring to select the target
   * shard for each key, ensuring the same key always maps to the same Worker
   * shard even when the shard set changes.
   */
  @Setter
  @Getter
  public static class ConsistentHashing {

    /** Whether consistent-hashing mode is enabled. */
    private boolean enabled = false;

    /** Number of virtual nodes per physical shard on the hash ring. */
    @Min(1)
    private int virtualNodes = 150;
  }

  /** Consistent-hashing configuration for report routing. */
  @Valid
  private ConsistentHashing consistentHashing = new ConsistentHashing();

  /**
   * Explicit instance ID for queue naming.
   * Falls back to {@code server.port-HOSTNAME} (or {@code server.port-UUID} if {@code HOSTNAME} is unset) if empty.
   */
  private String instanceId = "";
}
