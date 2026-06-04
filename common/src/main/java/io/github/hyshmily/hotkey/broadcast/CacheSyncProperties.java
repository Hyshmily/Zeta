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
package io.github.hyshmily.hotkey.broadcast;

import io.github.hyshmily.hotkey.hotkeycache.InstanceIdGenerator;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for instance-to-instance cache synchronization (INVALIDATE / REFRESH).
 * <p>
 * Exchange: {@code hotkey.sync.exchange} (FanoutExchange)
 * Queue: {@code hotkey.sync:{instanceId}}
 */
@Data
@ConfigurationProperties(prefix = "hotkey.sync")
public class CacheSyncProperties {

  private boolean enabled = false;
  private String exchangeName = "hotkey.sync.exchange";
  private String queuePrefix = "hotkey.sync";

  private boolean autoStartup = true;
  private int dedupWindowSeconds = 10;
  private int dedupMaxSize = 10_000;
  private int warmupJitterMs = 100;
  private int concurrentConsumers = 3;
  private int schedulerPoolSize = 4;

  /**
   * Returns the unique per-instance queue name.
   *
   * @return {@code {queuePrefix}:{instanceId}}
   */
  public String getQueueName() {
    return queuePrefix + ":" + InstanceIdGenerator.get();
  }
}
