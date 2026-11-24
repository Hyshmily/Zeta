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
package io.github.hyshmily.zeta.sync;

import io.github.hyshmily.zeta.util.Internal;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import lombok.Getter;
import lombok.Setter;

/**
 * The consumer-plane fields shared by the cache-sync and Worker-decision listeners: enablement,
 * the exchange and per-instance queue prefix, consumer concurrency, prefetch, the jitter-scheduler
 * pool size, the pending-unit budget and the auto-startup flag.
 *
 * <p>Each concrete {@code @ConfigurationProperties} class supplies its own exchange, queue prefix
 * and defaults in its constructor; binding and Bean Validation walk the inherited bean properties
 * exactly as they do the subclass fields.
 */
@Getter
@Setter
@Internal
public abstract class AmqpConsumerProperties {

  /** Whether this listener is enabled. */
  private boolean enabled = false;

  /** The exchange the listener's queue is bound to. */
  @NotBlank
  private String exchangeName;

  /** Prefix for the per-instance queue name; the auto-configuration appends the instance id. */
  @NotBlank
  private String queuePrefix;

  /** Whether the RabbitMQ listener container starts automatically with the application context. */
  private boolean autoStartup = true;

  /**
   * Maximum random start-jitter (milliseconds) parked before a key's first dispatcher
   * cycle; {@code 0} disables the park.
   */
  @PositiveOrZero
  private int jitterMs;

  /** Number of concurrent AMQP consumers. */
  @Positive
  private int concurrentConsumers;

  /**
   * Minimum pool size for the scheduled executor that runs jittered tasks. The effective
   * pool is {@code max(this, concurrentConsumers × 2)}.
   */
  @Positive
  private int schedulerPoolSize = 4;

  /**
   * Maximum unacknowledged messages per consumer. Both listeners do O(1) non-blocking work on the
   * AMQP thread before acknowledging, so the window bounds in-flight work rather than processing
   * time. A subclass overrides it for its own message size: the sync listener holds fewer because a
   * delivery can carry a whole PUT payload.
   */
  @Positive
  private int prefetchCount = 64;

  /** Global cap (in weighted units) on tasks pending in the per-key ordered dispatcher. */
  @Positive
  private int maxPendingUnits;
}
