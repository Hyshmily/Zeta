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
import io.github.hyshmily.zeta.sync.AbstractAmqpConsumerProperties;
import io.github.hyshmily.zeta.util.Internal;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
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
@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "zeta.sync")
@Internal
public class CacheSyncProperties extends AbstractAmqpConsumerProperties {

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
   * value-carrying {@code PUT} broadcast (design decisions: sync messages). The built-in whitelist
   * (JDK value types, {@code java.util} Collection/Map implementations, {@code java.time},
   * {@code java.math} plus {@code io.github.hyshmily.zeta.model.*}) never
   * admits business DTOs — a business value outside these prefixes fails
   * closed on the receiver and degrades to a versioned INVALIDATE (safe but
   * value-less). Configure your application's own DTO packages here, e.g.
   * {@code zeta.sync.put-allowed-packages: com.example.dto} (a trailing {@code .}
   * is added when absent, so a sibling package is not admitted) — trust the list:
   * any class under a listed prefix becomes instantiable from a broker-writable
   * sender, so keep it as narrow as the values you actually put.
   */
  private List<String> putAllowedPackages = new ArrayList<>();

  /** Time window (seconds) for deduplicating repeated broadcasts of one cache key.
   * Within this window, a send that does not outrank the recorded claim is silently
   * dropped; an equal-version INVALIDATE still replaces a REFRESH claim. */
  @Positive
  private int dedupWindowSeconds = 10;

  /** Maximum number of entries in the in-memory deduplication cache.
   * When full, Caffeine's Window TinyLFU policy evicts entries. */
  @Positive
  private int dedupMaxSize = 10_000;

  public CacheSyncProperties() {
    setExchangeName(ZetaConstants.Exchange.SYNC);
    setQueuePrefix("zeta.sync");
    setJitterMs(50);
    setConcurrentConsumers(3);
    setMaxPendingUnits(50_000);
  }
}
