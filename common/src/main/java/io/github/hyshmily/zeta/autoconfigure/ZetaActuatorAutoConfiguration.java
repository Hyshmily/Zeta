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

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.cache.cachesupport.ExpireManager;
import io.github.hyshmily.zeta.cache.cachesupport.SingleFlight;
import io.github.hyshmily.zeta.detection.ZetaStateMachine;
import io.github.hyshmily.zeta.endpoint.RingEndpoint;
import io.github.hyshmily.zeta.endpoint.StateMachineEndpoint;
import io.github.hyshmily.zeta.endpoint.ZetaEndpoint;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.rule.RuleMatcher;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sharding.RingManager;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.util.version.VersionController;
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
 * Auto-configuration for Actuator endpoints exposing HotKey runtime diagnostics.
 *
 * <p>Creates three endpoint beans:
 * <ul>
 *   <li>{@link ZetaEndpoint} &mdash; {@code /actuator/zeta}: comprehensive
 *       diagnostics including TopK rankings, cache metrics, SingleFlight, reporter
 *       stats, rules, TTLs, version tracking, and cluster health.</li>
 *   <li>{@link RingEndpoint} &mdash; {@code /actuator/hotkeyring}: consistent-hash
 *       ring CRUD (requires MVC and consistent-hashing enabled).</li>
 *   <li>{@link StateMachineEndpoint} &mdash; {@code /actuator/zeta/worker/state}:
 *       Worker state-machine configuration (requires Worker mode and MVC).</li>
 * </ul>
 *
 * <p>All dependencies for {@link ZetaEndpoint} are injected via
 * {@link ObjectProvider} so that the endpoint can be created in any deployment mode:
 * <ul>
 *   <li><b>App-only</b> &mdash; app-side TopK, cache, and SingleFlight available</li>
 *   <li><b>Worker-only</b> &mdash; only Worker-side TopK is available</li>
 *   <li><b>Coexistence</b> &mdash; both TopK instances, cache, and SingleFlight</li>
 * </ul>
 * Missing dependencies are silently passed as {@code null} and guarded
 * inside each endpoint.
 */
@Internal
@AutoConfiguration
@ConditionalOnClass(Endpoint.class)
@EnableConfigurationProperties(ZetaProperties.class)
public class ZetaActuatorAutoConfiguration {

  /**
   * Create the {@link ZetaEndpoint} Actuator endpoint for comprehensive diagnostics.
   *
   * <p>All dependencies are optional via {@link ObjectProvider} so the endpoint
   * works in any deployment mode. Missing components result in empty sub-sections
   * in the endpoint response rather than null-pointer errors.
   *
   * @param hotKeyDetectorProvider      provider for the app-side TopK detector (may be absent)
   * @param workerTopKProvider          provider for the Worker-side TopK detector (may be absent)
   * @param hotLocalCacheProvider       provider for the L1 Caffeine cache (may be absent)
   * @param singleFlightProvider        provider for the SingleFlight dedup layer (may be absent)
   * @param hotKeyReporterProvider      provider for the HotKey reporter (may be absent)
   * @param ruleMatcherProvider         provider for the rule matcher (may be absent)
   * @param expireManagerProvider       provider for the cache expiry manager (may be absent)
   * @param versionControllerProvider   provider for the version controller (may be absent)
   * @param cacheSyncPublisherProvider  provider for the cache sync publisher (may be absent)
   * @param stateMachineProvider        provider for the Worker state machine (may be absent)
   * @param healthViewProvider          provider for the cluster health view (may be absent)
   * @param properties                  the HotKey configuration properties (never {@code null})
   * @return a new {@link ZetaEndpoint} instance
   */
  @Bean
  @ConditionalOnMissingBean
  public ZetaEndpoint hotKeyEndpoint(
    @Qualifier("hotKeyDetector") ObjectProvider<TopK> hotKeyDetectorProvider,
    @Qualifier("workerTopK") ObjectProvider<TopK> workerTopKProvider,
    ObjectProvider<Cache<String, Object>> hotLocalCacheProvider,
    ObjectProvider<SingleFlight> singleFlightProvider,
    ObjectProvider<KeyReporter> hotKeyReporterProvider,
    ObjectProvider<RuleMatcher> ruleMatcherProvider,
    ObjectProvider<ExpireManager> expireManagerProvider,
    ObjectProvider<VersionController> versionControllerProvider,
    ObjectProvider<CacheSyncPublisher> cacheSyncPublisherProvider,
    ObjectProvider<ZetaStateMachine> stateMachineProvider,
    ObjectProvider<HealthView> healthViewProvider,
    ZetaProperties properties
  ) {
    return ZetaEndpoint.builder()
      .hotKeyDetector(hotKeyDetectorProvider.getIfAvailable())
      .workerTopK(workerTopKProvider.getIfAvailable())
      .caffeineCache(hotLocalCacheProvider.getIfAvailable())
      .singleFlight(singleFlightProvider.getIfAvailable())
      .properties(properties)
      .hotKeyReporter(hotKeyReporterProvider.getIfAvailable())
      .ruleMatcher(ruleMatcherProvider.getIfAvailable())
      .expireManager(expireManagerProvider.getIfAvailable())
      .versionController(versionControllerProvider.getIfAvailable())
      .cacheSyncPublisher(cacheSyncPublisherProvider.getIfAvailable())
      .zetaStateMachine(stateMachineProvider.getIfAvailable())
      .healthView(healthViewProvider.getIfAvailable())
      .build();
  }

  /**
   * Create the {@link RingEndpoint} for consistent-hash ring CRUD operations.
   *
   * <p>Only active when {@code zeta.local.consistent-hashing.enabled=true}
   * and Spring MVC ({@code RestController}) is on the classpath.
   * Provides REST endpoints at {@code /actuator/hotkeyring} for viewing and
   * modifying the consistent-hash ring topology.
   *
   * @param ringManagerProvider        the ring manager for consistent-hash topology (never {@code null})
   * @param healthViewProvider optional provider for the cluster health view (may be absent)
   * @return a new {@link RingEndpoint} instance
   */
  @Bean
  @ConditionalOnClass(name = "org.springframework.web.bind.annotation.RestController")
  @ConditionalOnProperty(prefix = "zeta.local.consistent-hashing", name = "enabled", havingValue = "true")
  @ConditionalOnMissingBean
  public RingEndpoint ringEndpoint(
    ObjectProvider<RingManager> ringManagerProvider,
    ObjectProvider<HealthView> healthViewProvider
  ) {
    return new RingEndpoint(ringManagerProvider.getIfAvailable(), healthViewProvider);
  }

  /**
   * Create the {@link StateMachineEndpoint} for reading and updating the Worker's
   * state-machine configuration at runtime.
   *
   * <p>Only active when a {@link ZetaStateMachine} bean is present
   * (i.e. in Worker mode) and Spring MVC is on the classpath.
   * Exposes REST endpoints at {@code /actuator/zeta/worker/state}
   * for GET (read config) and POST (update config) operations.
   * Configuration changes propagate to peer Workers via heartbeat send.
   *
   * @param stateMachine                    the Worker state machine (never {@code null})
   * @param configTimestampCounterProvider  optional provider for the config-change timestamp
   *                                        counter; bumped on each config change to trigger
   *                                        heartbeat send (may be absent)
   * @return a new {@link StateMachineEndpoint} instance
   */
  @Bean
  @ConditionalOnClass(name = "org.springframework.web.bind.annotation.RestController")
  @ConditionalOnBean(ZetaStateMachine.class)
  @ConditionalOnMissingBean
  public StateMachineEndpoint stateMachineEndpoint(
    ZetaStateMachine stateMachine,
    ObjectProvider<java.util.concurrent.atomic.AtomicLong> configTimestampCounterProvider
  ) {
    return new StateMachineEndpoint(stateMachine, configTimestampCounterProvider);
  }
}
