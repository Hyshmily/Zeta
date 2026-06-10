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
package io.github.hyshmily.hotkey.sync;

import io.github.hyshmily.hotkey.util.InstanceIdGenerator;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Configuration for listening to Worker hot/cool decisions.
 * <p>
 * Exchange: {@code hotkey.broadcast.exchange} (FanoutExchange) — must match
 * {@code hotkey.worker.messaging.broadcast-exchange} on the Worker side.
 * Queue: {@code hotkey.worker:{instanceId}}
 */
@Data
@Validated
@ConfigurationProperties(prefix = "hotkey.worker-listener")
public class WorkerListenerProperties {

  /** Whether the Worker decision listener is enabled. */
  private boolean enabled = false;

  /** FanoutExchange name for receiving Worker HOT/COOL decisions and heartbeats. */
  private String exchangeName = "hotkey.broadcast.exchange";

  /** Prefix for the per-instance queue name (suffixed with instance ID). */
  private String queuePrefix = "hotkey.worker";

  /** Whether the RabbitMQ listener container starts automatically. */
  private boolean autoStartup = true;

  /** Maximum random jitter (ms) added before each Worker decision update to spread Redis load. */
  private int warmupJitterMs = 100;

  /** Number of concurrent RabbitMQ consumers for the Worker decision queue. */
  private int concurrentConsumers = 2;

  /** Pool size for the scheduled executor that runs jittered Worker tasks. */
  private int schedulerPoolSize = 2;

  /** AMQP prefetch count per consumer. */
  private int prefetchCount = 5;

  /**
   * Returns the unique per-instance queue name for Worker decision listener.
   *
   * @return {@code {queuePrefix}:{instanceId}}
   */
  public String getQueueName() {
    return queuePrefix + ":" + InstanceIdGenerator.get();
  }
}
