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
package io.github.hyshmily.zeta.sync.local;

import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.sync.AmqpConsumerProperties;
import io.github.hyshmily.zeta.sync.dispatcher.KeyDispatcher;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.NodeIdentity;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Configuration properties for the instance-to-instance cache synchronization
 * mechanism (INVALIDATE / REFRESH / PUT and the clear family).
 *
 * <p>Binds to the {@code zeta.sync} prefix in Spring configuration sources.
 * Each application instance creates a durable queue named
 * {@code {queuePrefix}:{instanceId}} bound to the configured FanoutExchange.
 *
 * <p>Controls the deduplication window for redundant broadcasts, the jitter
 * window to spread cluster-loader load, consumer concurrency, and prefetch settings.
 *
 * @see CacheSyncPublisher
 * @see CacheSyncListener
 */
@Data
@Validated
@ConfigurationProperties(prefix = "zeta.sync")
@Internal
public class CacheSyncProperties implements AmqpConsumerProperties {

  /** Whether instance-to-instance cache synchronization is enabled.
   * Set to {@code true} when multiple application instances share the same data
   * source and should be notified of each other's data mutations. */
  private boolean enabled = false;

  /**
   * Delay (ms) before pending sync records are flushed to the sync publisher.
   * Combines multiple writes into one batch. Deliberately small (50ms): the
   * window is the sync plane's propagation latency, so it trades merge
   * efficiency for freshness — peers see a write within ~50ms.
   */
  @PositiveOrZero
  private long flushDelayMs = 50;

  /**
   * Maximum deferral (ms) before a pending flush is forced to fire, even
   * when new records keep arriving within the {@link #flushDelayMs} window.
   * Prevents indefinite debounce starvation under continuous writes.
   */
  @Positive
  private long maxDeferMs = 500;

  /**
   * Upper bound (bytes) for a single PUT broadcast payload (the JSON-serialized
   * put value). A write whose serialized value exceeds this bound is degraded to
   * a value-less REFRESH (same buffer, peer reloads through the cluster loader) — big values
   * would otherwise dominate the AMQP message size, the sync dispatcher's KB-based
   * pending budget, and the buffer's memory footprint.
   */
  @PositiveOrZero
  private int putMaxBytes = 32_768;

  /**
   * Extra package prefixes allowed in the deserialization whitelist of the
   * value-carrying {@code PUT} broadcast (design decisions: invalidation). The built-in whitelist
   * ({@code java.util}, {@code java.time}, {@code java.math} value types plus
   * {@code io.github.hyshmily.zeta.*}) never
   * admits business DTOs — a business value outside these prefixes fails
   * closed on the receiver and degrades to a versioned INVALIDATE (safe but
   * value-less). Configure your application's own DTO packages here, e.g.
   * {@code zeta.sync.put-allowed-packages: com.example.dto} — trust the list:
   * any class under a listed prefix becomes instantiable from a broker-writable
   * sender, so keep it as narrow as the values you actually put.
   */
  private List<String> putAllowedPackages = new ArrayList<>();

  /** FanoutExchange name for broadcasting the single-key, batch and clear messages. */
  @NotBlank
  private String exchangeName = ZetaConstants.Exchange.SYNC;

  /** Prefix for the per-instance queue name, suffixed with the instance ID
   * from {@link NodeIdentity}. Final queue: {@code {queuePrefix}:{instanceId}}. */
  @NotBlank
  private String queuePrefix = "zeta.sync";

  /** Whether the RabbitMQ listener container starts automatically with the application context. */
  private boolean autoStartup = true;

  /** Time window (seconds) for deduplicating repeated broadcasts of one cache key.
   * Within this window, a send that does not outrank the recorded claim is silently
   * dropped; an equal-version INVALIDATE still replaces a REFRESH claim. */
  @Positive
  private int dedupWindowSeconds = 10;

  /** Maximum number of entries in the in-memory deduplication cache.
   * When full, Caffeine's Window TinyLFU policy evicts entries. */
  @Positive
  private int dedupMaxSize = 10_000;

  /** Maximum random start-jitter (milliseconds) parked before a key's first sync cache
   * update cycle. The draw happens once per key-dispatch burst, not per message: it
   * spreads the cluster-loader reads of peers processing the same REFRESH instead of
   * letting them all hit the data source at once. */
  @PositiveOrZero
  private int jitterMs = 50;

  /** Number of concurrent RabbitMQ consumers for the sync queue.
   * Higher values increase throughput under load at the cost of more broker connections. */
  @Positive
  private int concurrentConsumers = 3;

  /** Minimum pool size for the scheduled executor that runs jittered sync tasks.
   * The effective pool is {@code max(this, concurrentConsumers × 2)}, so every
   * consumer thread can run a delayed follow-up task without starving the others. */
  @Positive
  private int schedulerPoolSize = 4;

  /** AMQP prefetch count per consumer. Controls how many unacknowledged messages
   * each consumer can hold at once. */
  @Positive
  private int prefetchCount = 5;

  /**
   * Global cap (in weighted units) on tasks pending in the per-key sync dispatcher
   * ({@link KeyDispatcher}). One unit approximates 1 KB of AMQP payload:
   * single-key messages charge 1 unit, payload messages (PUT) charge by body
   * size ({@code 1 + bodyBytes/1024}). When the budget is exhausted, new per-key
   * tasks are dropped — the message was already acked (at-most-once, design
   * decisions: sync messages), so a dropped invalidation is re-sent by the next
   * application-level write or bounded by hard TTL. The clear family is a
   * control-plane submission and bypasses this budget.
   */
  @Positive
  private int maxPendingUnits = 50_000;
}
