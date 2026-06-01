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
package io.github.hyshmily.hotkey.hotkeycache;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Positive;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties bound from the {@code hotkey.*} prefix.
 *
 * <p>Groups: TopK algorithm parameters (capacity, sketch width/depth,
 * decay, minimum count), L1 Caffeine cache limits, SingleFlight
 * deduplication settings, executor thread pool, TTL overrides (hard
 * and soft, for normal and hot keys), and reporting shard topology.
 */
@Data
@ConfigurationProperties(prefix = "hotkey.local")
public class HotKeyProperties {

  @Min(1)
  private int topK = 100;

  @Min(1)
  @Max(200_000)
  private int width = 50_000;

  @Min(1)
  @Max(10)
  private int depth = 5;

  @Positive
  private double decay = 0.92;

  @Min(1)
  private int minCount = 10;

  @Min(1)
  private int localCacheMaxSize = 1000;

  @Min(1)
  private int localCacheTtlMinutes = 5;

  @Min(0)
  private int localCacheAccessTtlMinutes = 0;

  @Min(1)
  private int inflightMaxSize = 50_000;

  @Min(1)
  private int inflightTtlSeconds = 5;

  @Min(1)
  private int inflightTimeoutSeconds = 3;

  @Min(1)
  private int executorCorePoolSize = 8;

  @Min(1)
  private int executorMaxPoolSize = 32;

  @Min(1)
  private int executorQueueCapacity = 2000;

  @Deprecated
  private int decayPeriod = 20;

  // Hard TTL — normal keys
  private long defaultHardTtlMs = 300_000;   // 5min — fallback when hardTtlMs not set
  private long hardTtlMs;                     // 0    — override for normal keys

  // Hard TTL — hot keys
  private long defaultHotHardTtlMs = 3_600_000; // 1h — fallback when hotHardTtlMs not set
  private long hotHardTtlMs;                     // 0  — override for hot keys

  // Soft TTL — normal keys
  private long defaultSoftTtlMs = 30_000;   // 30s — fallback when softTtlMs not set
  private long softTtlMs;                    // 0   — override for normal keys

  // Soft TTL — hot keys
  private long defaultHotSoftTtlMs = 300_000; // 5min — fallback when hotSoftTtlMs not set
  private long hotSoftTtlMs;                   // 0    — override for hot keys

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

  @Min(1)
  private int refreshMaxPools = 100;

  @Min(1)
  private int versionKeyTtlMinutes = 60;

  private String appName = "default";
  private String reportExchange = "hotkey.report.exchange";
  private long reportIntervalMs = 100;
  private int shardCount = 1;

  /** Explicit instance ID for queue naming. Falls back to {@code server.port-HOSTNAME} (or {@code server.port-UUID} if {@code HOSTNAME} is unset) if empty. */
  private String instanceId = "";
}
