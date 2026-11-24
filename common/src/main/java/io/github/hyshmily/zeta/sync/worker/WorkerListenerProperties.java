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
import io.github.hyshmily.zeta.util.Internal;
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
 * @see WorkerListener
 */
@Validated
@ConfigurationProperties(prefix = "zeta.worker-listener")
@Internal
public class WorkerListenerProperties extends AmqpConsumerProperties {

  public WorkerListenerProperties() {
    setExchangeName(ZetaConstants.Exchange.BROADCAST);
    setQueuePrefix("zeta.worker");
    setJitterMs(50);
    setConcurrentConsumers(2);
    setPrefetchCount(100);
    setMaxPendingUnits(200_000);
  }
}
