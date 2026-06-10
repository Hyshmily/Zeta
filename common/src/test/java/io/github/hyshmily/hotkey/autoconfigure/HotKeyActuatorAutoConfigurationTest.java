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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.endpoint.HotKeyEndpoint;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.sync.CacheSyncPublisher;
import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.cache.CacheExpireManager;
import io.github.hyshmily.hotkey.autoconfigure.HotKeyProperties;
import io.github.hyshmily.hotkey.cache.SingleFlight;
import io.github.hyshmily.hotkey.sync.VersionController;
import io.github.hyshmily.hotkey.monitor.WorkerHealthMonitor;
import io.github.hyshmily.hotkey.reporting.HotKeyReporter;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Tests for {@link HotKeyActuatorAutoConfiguration}.
 */
@ExtendWith(MockitoExtension.class)
class HotKeyActuatorAutoConfigurationTest {

  private final ApplicationContextRunner runner = new ApplicationContextRunner().withConfiguration(
    AutoConfigurations.of(HotKeyActuatorAutoConfiguration.class)
  );

  /**
   * Verifies that the actuator auto-configuration loads when the Actuator Endpoint class is on the classpath.
   */
  @Test
  void configLoadsWhenEndpointIsOnClasspath() {
    runner.run(ctx -> {
      assertThat(ctx).hasSingleBean(HotKeyProperties.class);
      // HotKeyEndpoint requires Endpoint class on classpath (condition met).
      // ObjectProvider parameters are satisfied automatically by Spring.
    });
  }

  /**
   * Verifies that the HotKeyEndpoint bean is created with all required dependencies provided via ObjectProviders.
   */
  @Test
  void hotKeyEndpointIsCreatedWithAllDependencies() {
    TopK hotKeyDetector = mock(TopK.class);
    TopK workerTopK = mock(TopK.class);
    Cache<String, Object> localCache = mock(Cache.class);
    SingleFlight singleFlight = mock(SingleFlight.class);
    HotKeyReporter reporter = mock(HotKeyReporter.class);
    HotKeyProperties properties = new HotKeyProperties();

    ObjectProvider<TopK> detectorProvider = mock(ObjectProvider.class);
    ObjectProvider<TopK> workerProvider = mock(ObjectProvider.class);
    ObjectProvider<Cache<String, Object>> cacheProvider = mock(ObjectProvider.class);
    ObjectProvider<SingleFlight> sfProvider = mock(ObjectProvider.class);
    ObjectProvider<HotKeyReporter> reporterProvider = mock(ObjectProvider.class);
    ObjectProvider<RuleMatcher> ruleMatcherProvider = mock(ObjectProvider.class);
    ObjectProvider<WorkerHealthMonitor> healthProvider = mock(ObjectProvider.class);
    ObjectProvider<CacheExpireManager> expireManagerProvider = mock(ObjectProvider.class);
    ObjectProvider<VersionController> versionControllerProvider = mock(ObjectProvider.class);
    ObjectProvider<CacheSyncPublisher> cacheSyncPublisherProvider = mock(ObjectProvider.class);
    ObjectProvider<HotKeyStateMachine> stateMachineProvider = mock(ObjectProvider.class);

    doReturn(hotKeyDetector).when(detectorProvider).getIfAvailable();
    doReturn(workerTopK).when(workerProvider).getIfAvailable();
    doReturn(localCache).when(cacheProvider).getIfAvailable();
    doReturn(singleFlight).when(sfProvider).getIfAvailable();
    doReturn(reporter).when(reporterProvider).getIfAvailable();
    doReturn(null).when(ruleMatcherProvider).getIfAvailable();
    doReturn(null).when(healthProvider).getIfAvailable();
    doReturn(null).when(expireManagerProvider).getIfAvailable();
    doReturn(null).when(versionControllerProvider).getIfAvailable();
    doReturn(null).when(cacheSyncPublisherProvider).getIfAvailable();
    doReturn(null).when(stateMachineProvider).getIfAvailable();

    HotKeyActuatorAutoConfiguration config = new HotKeyActuatorAutoConfiguration();
    HotKeyEndpoint endpoint = config.hotKeyEndpoint(
      detectorProvider,
      workerProvider,
      cacheProvider,
      sfProvider,
      reporterProvider,
      ruleMatcherProvider,
      healthProvider,
      expireManagerProvider,
      versionControllerProvider,
      cacheSyncPublisherProvider,
      stateMachineProvider,
      properties
    );

    assertThat(endpoint).isNotNull();
  }

  /**
   * Verifies that the HotKeyEndpoint is still created when all optional ObjectProvider dependencies return null.
   */
  @Test
  void hotKeyEndpointHandlesMissingDependenciesAsNull() {
    ObjectProvider<TopK> detectorProvider = mock(ObjectProvider.class);
    ObjectProvider<TopK> workerProvider = mock(ObjectProvider.class);
    ObjectProvider<Cache<String, Object>> cacheProvider = mock(ObjectProvider.class);
    ObjectProvider<SingleFlight> sfProvider = mock(ObjectProvider.class);
    ObjectProvider<HotKeyReporter> reporterProvider = mock(ObjectProvider.class);
    ObjectProvider<RuleMatcher> ruleMatcherProvider = mock(ObjectProvider.class);
    ObjectProvider<WorkerHealthMonitor> healthProvider = mock(ObjectProvider.class);
    ObjectProvider<CacheExpireManager> expireManagerProvider = mock(ObjectProvider.class);
    ObjectProvider<VersionController> versionControllerProvider = mock(ObjectProvider.class);
    ObjectProvider<CacheSyncPublisher> cacheSyncPublisherProvider = mock(ObjectProvider.class);
    ObjectProvider<HotKeyStateMachine> stateMachineProvider = mock(ObjectProvider.class);
    HotKeyProperties properties = new HotKeyProperties();

    doReturn(null).when(detectorProvider).getIfAvailable();
    doReturn(null).when(workerProvider).getIfAvailable();
    doReturn(null).when(cacheProvider).getIfAvailable();
    doReturn(null).when(sfProvider).getIfAvailable();
    doReturn(null).when(reporterProvider).getIfAvailable();
    doReturn(null).when(ruleMatcherProvider).getIfAvailable();
    doReturn(null).when(healthProvider).getIfAvailable();
    doReturn(null).when(expireManagerProvider).getIfAvailable();
    doReturn(null).when(versionControllerProvider).getIfAvailable();
    doReturn(null).when(cacheSyncPublisherProvider).getIfAvailable();
    doReturn(null).when(stateMachineProvider).getIfAvailable();

    HotKeyActuatorAutoConfiguration config = new HotKeyActuatorAutoConfiguration();
    HotKeyEndpoint endpoint = config.hotKeyEndpoint(
      detectorProvider,
      workerProvider,
      cacheProvider,
      sfProvider,
      reporterProvider,
      ruleMatcherProvider,
      healthProvider,
      expireManagerProvider,
      versionControllerProvider,
      cacheSyncPublisherProvider,
      stateMachineProvider,
      properties
    );

    assertThat(endpoint).isNotNull();
  }

  /**
   * Verifies that the HotKeyEndpoint is created with only the app-level hotKeyDetector TopK present.
   */
  @Test
  void hotKeyEndpointAcceptsOnlyAppTopK() {
    TopK hotKeyDetector = mock(TopK.class);
    ObjectProvider<TopK> detectorProvider = mock(ObjectProvider.class);
    ObjectProvider<TopK> workerProvider = mock(ObjectProvider.class);
    ObjectProvider<Cache<String, Object>> cacheProvider = mock(ObjectProvider.class);
    ObjectProvider<SingleFlight> sfProvider = mock(ObjectProvider.class);
    ObjectProvider<HotKeyReporter> reporterProvider = mock(ObjectProvider.class);
    ObjectProvider<RuleMatcher> ruleMatcherProvider = mock(ObjectProvider.class);
    ObjectProvider<WorkerHealthMonitor> healthProvider = mock(ObjectProvider.class);
    ObjectProvider<CacheExpireManager> expireManagerProvider = mock(ObjectProvider.class);
    ObjectProvider<VersionController> versionControllerProvider = mock(ObjectProvider.class);
    ObjectProvider<CacheSyncPublisher> cacheSyncPublisherProvider = mock(ObjectProvider.class);
    ObjectProvider<HotKeyStateMachine> stateMachineProvider = mock(ObjectProvider.class);
    HotKeyProperties properties = new HotKeyProperties();

    doReturn(hotKeyDetector).when(detectorProvider).getIfAvailable();
    doReturn(null).when(workerProvider).getIfAvailable();
    doReturn(null).when(cacheProvider).getIfAvailable();
    doReturn(null).when(sfProvider).getIfAvailable();
    doReturn(null).when(reporterProvider).getIfAvailable();
    doReturn(null).when(ruleMatcherProvider).getIfAvailable();
    doReturn(null).when(healthProvider).getIfAvailable();
    doReturn(null).when(expireManagerProvider).getIfAvailable();
    doReturn(null).when(versionControllerProvider).getIfAvailable();
    doReturn(null).when(cacheSyncPublisherProvider).getIfAvailable();
    doReturn(null).when(stateMachineProvider).getIfAvailable();

    HotKeyActuatorAutoConfiguration config = new HotKeyActuatorAutoConfiguration();
    HotKeyEndpoint endpoint = config.hotKeyEndpoint(
      detectorProvider,
      workerProvider,
      cacheProvider,
      sfProvider,
      reporterProvider,
      ruleMatcherProvider,
      healthProvider,
      expireManagerProvider,
      versionControllerProvider,
      cacheSyncPublisherProvider,
      stateMachineProvider,
      properties
    );

    assertThat(endpoint).isNotNull();
  }
}
