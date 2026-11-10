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
package io.github.hyshmily.zeta.sync.worker;

import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.sync.AmqpConsumerProperties;
import io.github.hyshmily.zeta.sync.dispatcher.KeyDispatcher;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.NodeIdentity;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Configuration properties for the {@link WorkerListener} that receives hot/cool
 * decisions from the Worker cluster.
 *
 * <p>Binds to the {@code zeta.worker-listener} prefix in Spring configuration sources.
 * Each application instance creates a durable queue named
 * {@code {queuePrefix}:{instanceId}} bound to the configured direct exchange under the
 * {@code send.<appName>} routing key. The
 * exchange name must match the Worker-side {@code zeta.worker.messaging.broadcast-exchange}
 * property.
 *
 *
 * @see WorkerListener
 */
@Data
@Validated
@ConfigurationProperties(prefix = "zeta.worker-listener")
@Internal
public class WorkerListenerProperties implements AmqpConsumerProperties {

  /** Whether the Worker decision listener is enabled. Set to {@code true} when
   * a Worker cluster is deployed and the application should consume Worker decisions. */
  private boolean enabled = false;

  /** Decision exchange name for receiving Worker HOT/COOL decisions. Heartbeats travel on the
   * separate control-plane heartbeat exchange ({@code zeta.heartbeat.exchange}, see design decisions: heartbeat and health).
   * Must match the {@code zeta.worker.messaging.broadcast-exchange} on the Worker side. */
  @NotBlank
  private String exchangeName = ZetaConstants.Exchange.BROADCAST;

  /** Prefix for the per-instance queue name, suffixed with the instance ID
   * from {@link NodeIdentity}. Final queue: {@code {queuePrefix}:{instanceId}}. */
  @NotBlank
  private String queuePrefix = "zeta.worker";

  /** Whether the RabbitMQ listener container starts automatically with the application context. */
  private boolean autoStartup = true;

  /** Maximum random start-jitter (milliseconds) applied to a cache key's first dispatcher
   * cycle after a Worker broadcast. When non-zero, spreads cluster-loader reads across a small time
   * window to avoid thundering herds when the Worker broadcasts a decision to many app
   * instances simultaneously. The jitter is a dispatcher-level park, not a sleep inside the
   * update task. Default 0 — no jitter; the mutation is submitted without a start-jitter
   * park. */
  @PositiveOrZero
  private int jitterMs = 0;

  /** Number of concurrent RabbitMQ consumers for the Worker decision queue.
   * Higher values increase throughput under load at the cost of more broker connections. */
  @Positive
  private int concurrentConsumers = 2;

  /** Minimum pool size for the scheduled executor that runs jittered Worker cache-update
   * tasks. The effective pool is {@code max(this, concurrentConsumers × 2)} so every consumer
   * thread can run a delayed follow-up task without starving the others. */
  @Positive
  private int schedulerPoolSize = 4;

  /** AMQP prefetch count per consumer. Controls how many unacknowledged messages
   * each consumer can hold at once. */
  @Positive
  private int prefetchCount = 5;

  /**
   * Global cap (in weighted units) on tasks pending in the per-key Worker-decision
   * dispatcher ({@link KeyDispatcher}). Worker decisions carry small payloads
   * (weight 1 each). The budget is kept higher than the cache-sync budget because a
   * dropped HOT/COOL decision has real cost and nothing re-sends it, so this budget trades memory
   * against decision loss.
   */
  @Positive
  private int maxPendingUnits = 200_000;
}
