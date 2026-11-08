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
import io.github.hyshmily.zeta.util.NodeIdentity;

/**
 * The consumer-plane configuration shared by the cache-sync and Worker-decision
 * AMQP listeners: exchange and per-instance queue names, consumer concurrency,
 * prefetch, the jitter-scheduler pool size and the auto-startup flag.
 *
 * <p>Implemented by {@link io.github.hyshmily.zeta.sync.local.CacheSyncProperties}
 * and {@link io.github.hyshmily.zeta.sync.worker.WorkerListenerProperties} so the
 * auto-configuration builds both listener containers and their schedulers from
 * one shape.
 */
@Internal
public interface AmqpConsumerProperties {
  /**
   * The exchange the listener's queue is bound to.
   *
   * @return the exchange name (never {@code null})
   */
  String getExchangeName();

  /**
   * Prefix for the per-instance queue name.
   *
   * @return the queue-name prefix (never {@code null})
   */
  String getQueuePrefix();

  /**
   * The per-instance queue the listener container consumes, formed as
   * {@code {queuePrefix}:{instanceId}}.
   *
   * @return the queue name (never {@code null})
   */
  default String getQueueName() {
    return getQueuePrefix() + ":" + NodeIdentity.getInstanceId();
  }

  /**
   * Number of concurrent AMQP consumers.
   *
   * @return the configured consumer count (positive)
   */
  int getConcurrentConsumers();

  /**
   * Maximum unacknowledged messages per consumer.
   *
   * @return the configured prefetch count (positive)
   */
  int getPrefetchCount();

  /**
   * Configured minimum size of the jitter-task scheduler pool.
   *
   * @return the configured pool size (positive)
   */
  int getSchedulerPoolSize();

  /**
   * Whether the listener container starts with the application context.
   *
   * @return {@code true} when the container starts automatically
   */
  boolean isAutoStartup();
}
