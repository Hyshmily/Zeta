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
import io.github.hyshmily.hotkey.actuator.HotKeyEndpoint;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyProperties;
import io.github.hyshmily.hotkey.hotkeycache.SingleFlight;
import io.github.hyshmily.hotkey.report.HotKeyReporter;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.util.TestPropertyValues;

/**
 * Tests for {@link HotKeyActuatorAutoConfiguration}.
 */
@ExtendWith(MockitoExtension.class)
class HotKeyActuatorAutoConfigurationTest {

  private final ApplicationContextRunner runner = new ApplicationContextRunner()
    .withConfiguration(AutoConfigurations.of(HotKeyActuatorAutoConfiguration.class));

  @Test
  void configLoadsWhenEndpointIsOnClasspath() {
    runner.run(ctx -> {
      assertThat(ctx).hasSingleBean(HotKeyProperties.class);
      // HotKeyEndpoint requires Endpoint class on classpath (condition met).
      // ObjectProvider parameters are satisfied automatically by Spring.
    });
  }

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

    doReturn(hotKeyDetector).when(detectorProvider).getIfAvailable();
    doReturn(workerTopK).when(workerProvider).getIfAvailable();
    doReturn(localCache).when(cacheProvider).getIfAvailable();
    doReturn(singleFlight).when(sfProvider).getIfAvailable();
    doReturn(reporter).when(reporterProvider).getIfAvailable();
    doReturn(null).when(ruleMatcherProvider).getIfAvailable();

    HotKeyActuatorAutoConfiguration config = new HotKeyActuatorAutoConfiguration();
    HotKeyEndpoint endpoint = config.hotKeyEndpoint(
      detectorProvider, workerProvider, cacheProvider, sfProvider, reporterProvider, ruleMatcherProvider, properties
    );

    assertThat(endpoint).isNotNull();
  }

  @Test
  void hotKeyEndpointHandlesMissingDependenciesAsNull() {
    ObjectProvider<TopK> detectorProvider = mock(ObjectProvider.class);
    ObjectProvider<TopK> workerProvider = mock(ObjectProvider.class);
    ObjectProvider<Cache<String, Object>> cacheProvider = mock(ObjectProvider.class);
    ObjectProvider<SingleFlight> sfProvider = mock(ObjectProvider.class);
    ObjectProvider<HotKeyReporter> reporterProvider = mock(ObjectProvider.class);
    ObjectProvider<RuleMatcher> ruleMatcherProvider = mock(ObjectProvider.class);
    HotKeyProperties properties = new HotKeyProperties();

    doReturn(null).when(detectorProvider).getIfAvailable();
    doReturn(null).when(workerProvider).getIfAvailable();
    doReturn(null).when(cacheProvider).getIfAvailable();
    doReturn(null).when(sfProvider).getIfAvailable();
    doReturn(null).when(reporterProvider).getIfAvailable();
    doReturn(null).when(ruleMatcherProvider).getIfAvailable();

    HotKeyActuatorAutoConfiguration config = new HotKeyActuatorAutoConfiguration();
    HotKeyEndpoint endpoint = config.hotKeyEndpoint(
      detectorProvider, workerProvider, cacheProvider, sfProvider, reporterProvider, ruleMatcherProvider, properties
    );

    assertThat(endpoint).isNotNull();
  }

  @Test
  void hotKeyEndpointAcceptsOnlyAppTopK() {
    TopK hotKeyDetector = mock(TopK.class);
    ObjectProvider<TopK> detectorProvider = mock(ObjectProvider.class);
    ObjectProvider<TopK> workerProvider = mock(ObjectProvider.class);
    ObjectProvider<Cache<String, Object>> cacheProvider = mock(ObjectProvider.class);
    ObjectProvider<SingleFlight> sfProvider = mock(ObjectProvider.class);
    ObjectProvider<HotKeyReporter> reporterProvider = mock(ObjectProvider.class);
    ObjectProvider<RuleMatcher> ruleMatcherProvider = mock(ObjectProvider.class);
    HotKeyProperties properties = new HotKeyProperties();

    doReturn(hotKeyDetector).when(detectorProvider).getIfAvailable();
    doReturn(null).when(workerProvider).getIfAvailable();
    doReturn(null).when(cacheProvider).getIfAvailable();
    doReturn(null).when(sfProvider).getIfAvailable();
    doReturn(null).when(reporterProvider).getIfAvailable();
    doReturn(null).when(ruleMatcherProvider).getIfAvailable();

    HotKeyActuatorAutoConfiguration config = new HotKeyActuatorAutoConfiguration();
    HotKeyEndpoint endpoint = config.hotKeyEndpoint(
      detectorProvider, workerProvider, cacheProvider, sfProvider, reporterProvider, ruleMatcherProvider, properties
    );

    assertThat(endpoint).isNotNull();
  }
}
