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
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.cache.HotKeyCache;
import io.github.hyshmily.hotkey.autoconfigure.HotKeyProperties;
import io.github.hyshmily.hotkey.util.InstanceIdGenerator;
import jakarta.annotation.PostConstruct;
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
   * Create the {@link HotKey} facade bean.  Each dependency is optional — the bean
   * adapts to whatever TopK / cache resources are available in the current deployment
   * mode.
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
