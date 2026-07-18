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

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.util.InstanceIdGenerator;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Configuration properties for the {@link WorkerListener} that receives hot/cool
 * decisions from the Worker cluster.
 *
 * <p>Binds to the {@code zeta.worker-listener} prefix in Spring configuration sources.
 * Each application instance creates an anonymous queue named
 * {@code zeta.worker:{instanceId}} bound to the configured FanoutExchange. The
 * exchange name must match the Worker-side {@code zeta.worker.messaging.send-exchange}
 * property.
 *
 * <p>Includes an inner {@link Sre} configuration for adaptive rate limiting of HOT
 * promotions, providing backpressure when downstream resources (Redis, CPU) are saturated.
 *
 * @see WorkerListener
 * @see io.github.hyshmily.zeta.util.ratelimit.SreRateLimiter
 */
@Data
@Validated
@ConfigurationProperties(prefix = "zeta.worker-listener")
@Internal
public class WorkerListenerProperties {

  /** Whether the Worker decision listener is enabled. Set to {@code true} when
   * a Worker cluster is deployed and the application should consume Worker decisions. */
  private boolean enabled = false;

  /** FanoutExchange name for receiving Worker HOT/COOL decisions and heartbeats.
   * Must match the {@code zeta.worker.messaging.send-exchange} on the Worker side. */
  private String exchangeName = ZetaConstants.Exchange.BROADCAST;

  /** Prefix for the per-instance queue name, suffixed with the instance ID
   * from {@link InstanceIdGenerator}. Final queue: {@code {queuePrefix}:{instanceId}}. */
  private String queuePrefix = "zeta.worker";

  /** Whether the RabbitMQ listener container starts automatically with the application context. */
  private boolean autoStartup = true;

  /** Maximum random jitter (milliseconds) added before each Worker decision cache update.
   * Spreads Redis reads across a small time window to avoid thundering herds
   * when the Worker broadcasts a decision to many app instances simultaneously. */
  private int warmupJitterMs = 50;

  /** Number of concurrent RabbitMQ consumers for the Worker decision queue.
   * Higher values increase throughput under load at the cost of more Redis connections. */
  private int concurrentConsumers = 2;

  /** Pool size for the scheduled executor that runs jittered Worker cache-update tasks.
   * Should be at least as large as the number of concurrent consumers. */
  private int schedulerPoolSize = 2;

  /** AMQP prefetch count per consumer. Controls how many unacknowledged messages
   * each consumer can hold at once. */
  private int prefetchCount = 5;

  /** Configuration for the SRE adaptive rate limiter on HOT decision processing.
   * Provides backpressure when downstream resources are saturated. */
  private Sre sre = new Sre();

  /**
   * SRE adaptive rate limiter settings for HOT decision processing.
   *
   * <p>Uses the Google SRE formula ({@code requests = K × accepts}) to
   * probabilistically drop HOT promotions when the success rate falls below
   * {@link #successThreshold}.  This provides backpressure when downstream
   * resources (Redis, CPU) are saturated.
   */
  @Data
  public static class Sre {

    /** Whether the SRE rate limiter is enabled for HOT decision processing.
     * When enabled, HOT promotions are probabilistically dropped when the
     * success rate falls below {@link #successThreshold}. */
    private boolean enabled = true;

    /** Sliding window duration in milliseconds over which request outcomes are tracked. */
    private long windowMs = 3000;

    /** Number of time buckets within the sliding window for granular tracking. */
    private int buckets = 10;

    /** Minimum total samples (accepts + rejects) required before the rate limiter
     * begins actively throttling. Prevents premature throttling during warm-up. */
    private int minSamples = 20;

    /** Success ratio threshold (0.0–1.0). The multiplier K is computed as
     * {@code K = 1 / successThreshold}. When the observed success rate drops below
     * this value, the limiter probabilistically drops requests. */
    private double successThreshold = 0.6;
  }

  /**
   * Returns the unique per-instance queue name for Worker decision listener.
   *
   * @return {@code {queuePrefix}:{instanceId}}
   */
  public String getQueueName() {
    return queuePrefix + ":" + InstanceIdGenerator.get();
  }
}
