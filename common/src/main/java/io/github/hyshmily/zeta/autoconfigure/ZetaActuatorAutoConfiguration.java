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
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.clock.StampClock;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration for Actuator endpoints exposing HotKey runtime diagnostics.
 *
 * <p>Creates two endpoint beans:
 * <ul>
 *   <li>{@link ZetaEndpoint} &mdash; {@code /actuator/hotkey}: runtime
 *       diagnostics including the TopK ranking, cache and SingleFlight state,
 *       reporter queue state, refresh leases, version-clock skew, identity,
 *       cluster health and isolation counters.</li>
 *   <li>{@link RingEndpoint} &mdash; {@code /actuator/hotkeyring}: consistent-hash
 *       ring inspection (gated by {@code zeta.local.consistent-hashing.enabled},
 *       default enabled; inspection only — routing always uses the ring).</li>
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
@ConditionalOnClass(name = "org.springframework.boot.actuate.endpoint.annotation.Endpoint")
public class ZetaActuatorAutoConfiguration {

  /**
   * Create the {@link ZetaEndpoint} Actuator endpoint for comprehensive diagnostics.
   *
   * <p>All dependencies are optional via {@link ObjectProvider} so the endpoint
   * works in any deployment mode. Missing components result in empty sub-sections
   * in the endpoint response rather than null-pointer errors.
   *
   * @param hotKeyDetectorProvider      provider for the app-side TopK detector (may be absent)
   * @param hotLocalCacheProvider       provider for the L1 Caffeine cache (may be absent)
   * @param singleFlightProvider        provider for the SingleFlight dedup layer (may be absent)
   * @param hotKeyReporterProvider      provider for the HotKey reporter (may be absent)
   * @param accessCounterProvider       provider for the shared access counter (may be absent)
   * @param backgroundRefresherProvider provider for the background refresh executor (may be absent)
   * @param stampClockProvider         provider for the data-stamp clock (may be absent)
   * @param healthViewProvider          provider for the cluster health view (may be absent)
   * @param syncListenerProvider        provider for the sync-plane listener whose ordered
   *                                    dispatcher gate is reported (may be absent)
   * @param workerListenerProvider      provider for the decision-plane listener whose ordered
   *                                    dispatcher gate is reported (may be absent)
   * @return a new {@link ZetaEndpoint} instance
   */
  @Bean
  @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
  @ConditionalOnMissingBean
  public ZetaEndpoint hotKeyEndpoint(
    @Qualifier("heavyKeeper") ObjectProvider<TopK> hotKeyDetectorProvider,
    ObjectProvider<Cache<String, CacheEntry>> hotLocalCacheProvider,
    ObjectProvider<SingleFlight> singleFlightProvider,
    ObjectProvider<KeyReporter> hotKeyReporterProvider,
    ObjectProvider<io.github.hyshmily.zeta.cache.AccessCounter> accessCounterProvider,
    ObjectProvider<BackgroundRefresher> backgroundRefresherProvider,
    ObjectProvider<StampClock> stampClockProvider,
    ObjectProvider<HealthView> healthViewProvider,
    ObjectProvider<CacheSyncListener> syncListenerProvider,
    ObjectProvider<WorkerListener> workerListenerProvider
  ) {
    return ZetaEndpoint.builder()
      .hotKeyDetector(hotKeyDetectorProvider.getIfAvailable())
      .caffeineCache(hotLocalCacheProvider.getIfAvailable())
      .singleFlight(singleFlightProvider.getIfAvailable())
      .hotKeyReporter(hotKeyReporterProvider.getIfAvailable())
      .accessCounter(accessCounterProvider.getIfAvailable())
      .backgroundRefresher(backgroundRefresherProvider.getIfAvailable())
      .stampClock(stampClockProvider.getIfAvailable())
      .healthView(healthViewProvider.getIfAvailable())
      .syncListener(syncListenerProvider.getIfAvailable())
      .workerListener(workerListenerProvider.getIfAvailable())
      .build();
  }

  /**
   * Create the {@link RingEndpoint} for consistent-hash ring inspection.
   *
   * <p>Created by default (the {@code zeta.local.consistent-hashing.enabled}
   * property defaults to {@code true}); set it to {@code false} to suppress the
   * endpoint. It gates <b>inspection only</b> — report routing always uses the
   * consistent-hash ring. Also requires the Actuator endpoint infrastructure on
   * the classpath. Provides operations at {@code /actuator/hotkeyring} for
   * viewing the ring topology and querying the node responsible for a key.
   *
   * @param ringProvider       optional provider for the consistent-hash ring; absent when
   *                           the report plane (which creates it) is not wired
   * @param healthViewProvider optional provider for the cluster health view (may be absent)
   * @return a new {@link RingEndpoint} instance
   */
  @Bean
  @ConditionalOnProperty(
    prefix = "zeta.local.consistent-hashing",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true
  )
  @ConditionalOnMissingBean
  public RingEndpoint ringEndpoint(
    ObjectProvider<ConsistentHashRing> ringProvider,
    ObjectProvider<HealthView> healthViewProvider
  ) {
    return new RingEndpoint(ringProvider.getIfAvailable(), healthViewProvider.getIfAvailable());
  }
}
