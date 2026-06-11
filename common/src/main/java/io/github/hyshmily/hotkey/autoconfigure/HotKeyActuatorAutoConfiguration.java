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

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.cache.CacheExpireManager;
import io.github.hyshmily.hotkey.cache.SingleFlight;
import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.endpoint.HotKeyEndpoint;
import io.github.hyshmily.hotkey.endpoint.RingEndpoint;
import io.github.hyshmily.hotkey.endpoint.StateMachineEndpoint;
import io.github.hyshmily.hotkey.reporting.HotKeyReporter;
import io.github.hyshmily.hotkey.sharding.RingManager;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import io.github.hyshmily.hotkey.sharding.RingManager;
import io.github.hyshmily.hotkey.sync.CacheSyncPublisher;
import io.github.hyshmily.hotkey.sync.VersionController;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration for the {@link HotKeyEndpoint} Actuator endpoint.
 *
 * <p>All dependencies are injected via {@link ObjectProvider} so that the
 * endpoint can be created in any deployment mode:
 * <ul>
 *   <li><b>App-only</b> — app-side TopK, cache, and SingleFlight available</li>
 *   <li><b>Worker-only</b> — only Worker-side TopK is available</li>
 *   <li><b>Coexistence</b> — both TopK instances, cache, and SingleFlight</li>
 * </ul>
 * Missing dependencies are silently passed as {@code null} and guarded
 * inside {@link HotKeyEndpoint}.
 */
@AutoConfiguration
@ConditionalOnClass(Endpoint.class)
@EnableConfigurationProperties(HotKeyProperties.class)
public class HotKeyActuatorAutoConfiguration {

  /**
   * Create the Actuator endpoint.  All dependencies are optional via {@link ObjectProvider}
   * so the endpoint works in any deployment mode.
   */
  @Bean
  @ConditionalOnMissingBean
  public HotKeyEndpoint hotKeyEndpoint(
    @Qualifier("hotKeyDetector") ObjectProvider<TopK> hotKeyDetectorProvider,
    @Qualifier("workerTopK") ObjectProvider<TopK> workerTopKProvider,
    ObjectProvider<Cache<String, Object>> hotLocalCacheProvider,
    ObjectProvider<SingleFlight> singleFlightProvider,
    ObjectProvider<HotKeyReporter> hotKeyReporterProvider,
    ObjectProvider<RuleMatcher> ruleMatcherProvider,
    ObjectProvider<RingManager> ringManagerProvider,
    ObjectProvider<CacheExpireManager> expireManagerProvider,
    ObjectProvider<VersionController> versionControllerProvider,
    ObjectProvider<CacheSyncPublisher> cacheSyncPublisherProvider,
    ObjectProvider<HotKeyStateMachine> stateMachineProvider,
    HotKeyProperties properties
  ) {
    return new HotKeyEndpoint(
      hotKeyDetectorProvider.getIfAvailable(),
      workerTopKProvider.getIfAvailable(),
      hotLocalCacheProvider.getIfAvailable(),
      singleFlightProvider.getIfAvailable(),
      properties,
      hotKeyReporterProvider.getIfAvailable(),
      ruleMatcherProvider.getIfAvailable(),
      ringManagerProvider.getIfAvailable(),
      expireManagerProvider.getIfAvailable(),
      versionControllerProvider.getIfAvailable(),
      cacheSyncPublisherProvider.getIfAvailable(),
      stateMachineProvider.getIfAvailable()
    );
  }

  /**
   * Create the RingEndpoint for consistent-hash ring CRUD.
   * Only active when {@code hotkey.local.consistent-hashing.enabled=true}
   * and Spring MVC (RestController) is on the classpath.
   *
   * @param ringManager the ring manager for consistent-hash topology
   */
  @Bean
  @ConditionalOnClass(name = "org.springframework.web.bind.annotation.RestController")
  @ConditionalOnProperty(prefix = "hotkey.local.consistent-hashing", name = "enabled", havingValue = "true")
  @ConditionalOnMissingBean
  public RingEndpoint ringEndpoint(RingManager ringManager) {
    return new RingEndpoint(ringManager);
  }

  /**
   * Create the StateMachineEndpoint for reading and updating the Worker's
   * state-machine configuration at runtime.
   *
   * <p>Only active when a {@link HotKeyStateMachine} bean is present
   * (i.e. in Worker mode) and Spring MVC is on the classpath.
   *
   * @param stateMachine              the worker's state machine
   * @param configTimestampCounterProvider optional config-change timestamp counter
   * @return a new {@link StateMachineEndpoint} instance
   */
  @Bean
  @ConditionalOnClass(name = "org.springframework.web.bind.annotation.RestController")
  @ConditionalOnBean(HotKeyStateMachine.class)
  @ConditionalOnMissingBean
  public StateMachineEndpoint stateMachineEndpoint(
    HotKeyStateMachine stateMachine,
    ObjectProvider<java.util.concurrent.atomic.AtomicLong> configTimestampCounterProvider
  ) {
    return new StateMachineEndpoint(stateMachine, configTimestampCounterProvider);
  }
}
