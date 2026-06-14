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
package io.github.hyshmily.hotkey.autoconfigure;

import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.cache.HotKeyCache;
import io.github.hyshmily.hotkey.constants.HotKeyConstants;
import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.TopK;
import io.github.hyshmily.hotkey.util.InstanceIdGenerator;
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
 * Always-active auto-configuration that creates a single {@link HotKey} facade bean
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
@AutoConfiguration
@RequiredArgsConstructor
public class HotKeyFacadeAutoConfiguration {

  private final HotKeyProperties properties;

  /**
   * Shared scheduler for all periodic tasks (flush, monitor, heartbeat, persist, etc.).
   * Pool size configurable via {@code hotkey.scheduler-pool-size} (default 4).
   */
  @Bean("hotKeyScheduler")
  @ConditionalOnMissingBean(name = "hotKeyScheduler")
  public ScheduledExecutorService hotKeyScheduler() {
    return Executors.newScheduledThreadPool(properties.getSchedulerPoolSize(), r -> {
      Thread t = new Thread(r, HotKeyConstants.THREAD_PREFIX_SCHEDULER);
      t.setDaemon(true);
      return t;
    });
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
  }

  /**
   * Create the {@link HotKey} facade bean. Each dependency is optional — the bean
   * adapts to whatever TopK / cache resources are available in the current deployment
   * mode.
   *
   * @param hotKeyCacheProvider provider for the HotKeyCache (app-only or coexistence mode)
   * @param appTopKProvider     provider for the app-side TopK detector
   * @param workerTopKProvider  provider for the Worker-side TopK detector
   * @return a new {@link HotKey} facade instance
   */
  @Bean
  @ConditionalOnMissingBean
  public HotKey hotKey(
    ObjectProvider<HotKeyCache> hotKeyCacheProvider,
    @Qualifier("hotKeyDetector") ObjectProvider<TopK> appTopKProvider,
    @Qualifier("workerTopK") ObjectProvider<TopK> workerTopKProvider
  ) {
    return new HotKey(
      hotKeyCacheProvider.getIfAvailable(),
      appTopKProvider.getIfAvailable(),
      workerTopKProvider.getIfAvailable()
    );
  }
}
