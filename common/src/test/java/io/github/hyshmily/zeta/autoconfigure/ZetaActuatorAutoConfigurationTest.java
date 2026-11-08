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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.endpoint.RingEndpoint;
import io.github.hyshmily.zeta.endpoint.ZetaEndpoint;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.scheduler.BackgroundRefresher;
import io.github.hyshmily.zeta.sharding.ConsistentHashRing;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.local.CacheSyncListener;
import io.github.hyshmily.zeta.sync.worker.WorkerListener;
import io.github.hyshmily.zeta.util.clock.StampClock;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Tests for {@link ZetaActuatorAutoConfiguration}.
 */
@ExtendWith(MockitoExtension.class)
class ZetaActuatorAutoConfigurationTest {

  private final ApplicationContextRunner runner = new ApplicationContextRunner().withConfiguration(
    AutoConfigurations.of(ZetaActuatorAutoConfiguration.class)
  );

  /**
   * Verifies that the actuator auto-configuration loads when the Actuator Endpoint class is on the classpath.
   */
  @Test
  void configLoadsWhenEndpointIsOnClasspath() {
    runner.run(ctx -> {
      assertThat(ctx).hasSingleBean(ZetaEndpoint.class);
      // ZetaEndpoint requires Endpoint class on classpath (condition met).
      // ObjectProvider parameters are satisfied automatically by Spring.
    });
  }

  /**
   * Verifies that the ZetaEndpoint bean is created with all required dependencies provided via ObjectProviders.
   */
  @Test
  void hotKeyEndpointIsCreatedWithAllDependencies() {
    TopK hotKeyDetector = mock(TopK.class);
    Cache<String, CacheEntry> localCache = mock(Cache.class);
    SingleFlight singleFlight = mock(SingleFlight.class);
    KeyReporter reporter = mock(KeyReporter.class);

    ObjectProvider<TopK> detectorProvider = mock(ObjectProvider.class);
    ObjectProvider<Cache<String, CacheEntry>> cacheProvider = mock(ObjectProvider.class);
    ObjectProvider<SingleFlight> sfProvider = mock(ObjectProvider.class);
    ObjectProvider<KeyReporter> reporterProvider = mock(ObjectProvider.class);
    ObjectProvider<BackgroundRefresher> backgroundRefresherProvider = mock(ObjectProvider.class);
    ObjectProvider<StampClock> stampClockProvider = mock(ObjectProvider.class);
    ObjectProvider<HealthView> healthViewProvider = mock(ObjectProvider.class);
    ObjectProvider<CacheSyncListener> syncListenerProvider = mock(ObjectProvider.class);
    ObjectProvider<WorkerListener> workerListenerProvider = mock(ObjectProvider.class);

    doReturn(hotKeyDetector).when(detectorProvider).getIfAvailable();
    doReturn(localCache).when(cacheProvider).getIfAvailable();
    doReturn(singleFlight).when(sfProvider).getIfAvailable();
    doReturn(reporter).when(reporterProvider).getIfAvailable();
    doReturn(null).when(backgroundRefresherProvider).getIfAvailable();
    doReturn(null).when(stampClockProvider).getIfAvailable();

    ZetaActuatorAutoConfiguration config = new ZetaActuatorAutoConfiguration();
    ZetaEndpoint endpoint = config.hotKeyEndpoint(
      detectorProvider,
      cacheProvider,
      sfProvider,
      reporterProvider,
      mock(ObjectProvider.class),
      backgroundRefresherProvider,
      stampClockProvider,
      healthViewProvider,
      syncListenerProvider,
      workerListenerProvider
    );

    assertThat(endpoint).isNotNull();
  }

  /**
   * Verifies that the ZetaEndpoint is still created when all optional ObjectProvider dependencies return null.
   */
  @Test
  void hotKeyEndpointHandlesMissingDependenciesAsNull() {
    ObjectProvider<TopK> detectorProvider = mock(ObjectProvider.class);
    ObjectProvider<Cache<String, CacheEntry>> cacheProvider = mock(ObjectProvider.class);
    ObjectProvider<SingleFlight> sfProvider = mock(ObjectProvider.class);
    ObjectProvider<KeyReporter> reporterProvider = mock(ObjectProvider.class);
    ObjectProvider<BackgroundRefresher> backgroundRefresherProvider = mock(ObjectProvider.class);
    ObjectProvider<StampClock> stampClockProvider = mock(ObjectProvider.class);
    ObjectProvider<HealthView> healthViewProvider = mock(ObjectProvider.class);
    ObjectProvider<CacheSyncListener> syncListenerProvider = mock(ObjectProvider.class);
    ObjectProvider<WorkerListener> workerListenerProvider = mock(ObjectProvider.class);

    doReturn(null).when(detectorProvider).getIfAvailable();
    doReturn(null).when(cacheProvider).getIfAvailable();
    doReturn(null).when(sfProvider).getIfAvailable();
    doReturn(null).when(reporterProvider).getIfAvailable();
    doReturn(null).when(backgroundRefresherProvider).getIfAvailable();
    doReturn(null).when(stampClockProvider).getIfAvailable();

    ZetaActuatorAutoConfiguration config = new ZetaActuatorAutoConfiguration();
    ZetaEndpoint endpoint = config.hotKeyEndpoint(
      detectorProvider,
      cacheProvider,
      sfProvider,
      reporterProvider,
      mock(ObjectProvider.class),
      backgroundRefresherProvider,
      stampClockProvider,
      healthViewProvider,
      syncListenerProvider,
      workerListenerProvider
    );

    assertThat(endpoint).isNotNull();
  }

  /**
   * Verifies that ringEndpoint is NOT created when consistent-hashing property is disabled.
   */
  @Test
  void ringEndpoint_shouldNotBeCreatedWhenConsistentHashingDisabled() {
    new ApplicationContextRunner()
      .withBean(ConsistentHashRing.class, () -> mock(ConsistentHashRing.class))
      .withConfiguration(AutoConfigurations.of(ZetaActuatorAutoConfiguration.class))
      .withPropertyValues("zeta.local.consistent-hashing.enabled=false")
      .run(ctx -> assertThat(ctx).doesNotHaveBean(RingEndpoint.class));
  }

  /**
   * Verifies that the ring inspection endpoint is created by default: the
   * {@code zeta.local.consistent-hashing.enabled} property defaults to {@code true}, so the
   * condition must match when the property is absent (it gates inspection only — report
   * routing always uses the ring).
   */
  @Test
  void ringEndpoint_shouldBeCreatedByDefault() {
    new ApplicationContextRunner()
      .withBean(ConsistentHashRing.class, () -> mock(ConsistentHashRing.class))
      .withConfiguration(AutoConfigurations.of(ZetaActuatorAutoConfiguration.class))
      .run(ctx -> assertThat(ctx).hasSingleBean(RingEndpoint.class));
  }

  /**
   * Verifies that the ZetaEndpoint is created with only the app-level hotKeyDetector TopK present.
   */
  @Test
  void hotKeyEndpointAcceptsOnlyAppTopK() {
    TopK hotKeyDetector = mock(TopK.class);
    ObjectProvider<TopK> detectorProvider = mock(ObjectProvider.class);
    ObjectProvider<Cache<String, CacheEntry>> cacheProvider = mock(ObjectProvider.class);
    ObjectProvider<SingleFlight> sfProvider = mock(ObjectProvider.class);
    ObjectProvider<KeyReporter> reporterProvider = mock(ObjectProvider.class);
    ObjectProvider<BackgroundRefresher> backgroundRefresherProvider = mock(ObjectProvider.class);
    ObjectProvider<StampClock> stampClockProvider = mock(ObjectProvider.class);
    ObjectProvider<HealthView> healthViewProvider = mock(ObjectProvider.class);
    ObjectProvider<CacheSyncListener> syncListenerProvider = mock(ObjectProvider.class);
    ObjectProvider<WorkerListener> workerListenerProvider = mock(ObjectProvider.class);

    doReturn(hotKeyDetector).when(detectorProvider).getIfAvailable();
    doReturn(null).when(cacheProvider).getIfAvailable();
    doReturn(null).when(sfProvider).getIfAvailable();
    doReturn(null).when(reporterProvider).getIfAvailable();
    doReturn(null).when(backgroundRefresherProvider).getIfAvailable();
    doReturn(null).when(stampClockProvider).getIfAvailable();

    ZetaActuatorAutoConfiguration config = new ZetaActuatorAutoConfiguration();
    ZetaEndpoint endpoint = config.hotKeyEndpoint(
      detectorProvider,
      cacheProvider,
      sfProvider,
      reporterProvider,
      mock(ObjectProvider.class),
      backgroundRefresherProvider,
      stampClockProvider,
      healthViewProvider,
      syncListenerProvider,
      workerListenerProvider
    );

    assertThat(endpoint).isNotNull();
  }

  /**
   * Pins the management-plane migration: both diagnostics endpoints must
   * be standard Actuator {@link Endpoint}s (port, exposure, and roles honored
   * via {@code management.*}) — never plain MVC controllers on the
   * application port — with stable ids and operation shapes.
   */
  @Test
  void endpointsAreActuatorEndpointsWithStableIdsAndOperations() throws Exception {
    assertThat(ZetaEndpoint.class.getAnnotation(Endpoint.class).id()).isEqualTo("hotkey");
    assertThat(RingEndpoint.class.getAnnotation(Endpoint.class).id()).isEqualTo("hotkeyring");

    assertThat(ZetaEndpoint.class.getMethod("hotKeyInfo", Integer.class)).satisfies(m ->
      assertThat(m.getAnnotation(ReadOperation.class)).isNotNull()
    );
    assertThat(RingEndpoint.class.getMethod("ringInfo")).satisfies(m ->
      assertThat(m.getAnnotation(ReadOperation.class)).isNotNull()
    );
    assertThat(
      RingEndpoint.class.getMethod("keyMapping", String[].class).getParameters()[0].getAnnotation(Selector.class)
    ).isNotNull();
  }
}
