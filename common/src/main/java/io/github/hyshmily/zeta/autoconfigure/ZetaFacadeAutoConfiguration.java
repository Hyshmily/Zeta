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
package io.github.hyshmily.zeta.autoconfigure;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.endpoint.ZetaEndpoint;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sharding.impl.HealthViewImpl;
import io.github.hyshmily.zeta.sync.distributedlock.LockProvider;
import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatVerifier;
import io.github.hyshmily.zeta.util.InstanceIdGenerator;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.ZetaThreadFactory;
import jakarta.annotation.PostConstruct;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Always-active auto-configuration that creates a single {@link Zeta} facade bean
 * and initializes the instance ID for queue naming.
 *
 * <p>All dependencies are injected via {@link ObjectProvider} so that the bean can
 * be created in any deployment mode:
 * <ul>
 *   <li><b>App-only</b> — receives a {@link HotKeyCache} and app-side {@link TopK}</li>
 *   <li><b>Worker-only</b> — receives only the Worker-side {@link TopK}</li>
 *   <li><b>Coexistence</b> — receives all three</li>
 * </ul>
 *
 * <p>Registered first in {@code AutoConfiguration.imports} so the
 * {@code HotKey} facade singleton is available early and the instance ID
 * override is set before any queue-creating auto-configuration runs.
 */
@Internal
@AutoConfiguration
@RequiredArgsConstructor
public class ZetaFacadeAutoConfiguration {

  private final ZetaProperties properties;

  /**
   * Shared scheduler for all periodic tasks (flush, monitor, heartbeat, persist, etc.).
   * Pool size configurable via {@code zeta.scheduler-pool-size} (default 4).
   */
  @Bean("hotKeyScheduler")
  @ConditionalOnMissingBean(name = "hotKeyScheduler")
  public ScheduledExecutorService hotKeyScheduler() {
    return Executors.newScheduledThreadPool(
      properties.getSchedulerPoolSize(),
      new ZetaThreadFactory(ZetaConstants.THREAD_PREFIX_SCHEDULER)
    );
  }

  /**
   * Initialize the explicit instance ID override, if configured, so that
   * {@link InstanceIdGenerator#get()} returns the configured value before
   * any downstream auto-configuration creates queues.
   */
  @PostConstruct
  void initInstanceId() {
    String id = properties.getInstanceId();
    if (id != null && !id.isBlank()) {
      InstanceIdGenerator.setOverride(id);
    }
    TimeSource.start();
  }

  /**
   * Create the shared {@link HealthView} for tracking Worker cluster health.
   *
   * <p>Declared here (the always-active, first-registered auto-configuration) so that
   * a single singleton is available to all consumers — {@link HotKeyCache},
   * {@link ZetaEndpoint}, {@link KeyReporter}, {@link WorkerHeartbeatVerifier},
   * and the heartbeat listener container — eliminating the risk of multiple
   * inconsistent instances with independent state.
   *
   * @param properties the HotKey configuration properties
   * @return a new {@link HealthViewImpl} instance
   */
  @Bean
  @ConditionalOnMissingBean
  public HealthView clusterHealthView(ZetaProperties properties) {
    HealthView view = new HealthViewImpl(
      properties.getExpectedWorkerCount(),
      properties.getHeartbeat().getTimeoutMs(),
      properties.getHeartbeat().getDegradeAfterFailures()
    );
    view.setMinAliveWorkers(properties.getHeartbeat().getMinAliveWorkers());
    return view;
  }

  /**
   * Create the {@link Zeta} facade bean. Each dependency is optional — the bean
   * adapts to whatever TopK / cache resources are available in the current deployment
   * mode.
   *
   * @param hotKeyCacheProvider provider for the HotKeyCache (app-only or coexistence mode)
   * @param appTopKProvider     provider for the app-side TopK detector
   * @param workerTopKProvider  provider for the Worker-side TopK detector
   * @param lockProvider        provider for distributed locks (absent when no Redis)
   * @return a new {@link Zeta} facade instance
   */
  @Bean
  @ConditionalOnMissingBean
  public Zeta hotKey(
    ObjectProvider<HotKeyCache> hotKeyCacheProvider,
    @Qualifier("hotKeyDetector") ObjectProvider<HotKeyDetector> appTopKProvider,
    @Qualifier("workerTopK") ObjectProvider<TopK> workerTopKProvider,
    ObjectProvider<LockProvider> lockProvider
  ) {
    return new Zeta(
      hotKeyCacheProvider.getIfAvailable(),
      appTopKProvider.getIfAvailable(),
      workerTopKProvider.getIfAvailable(),
      lockProvider.getIfAvailable()
    );
  }
}
