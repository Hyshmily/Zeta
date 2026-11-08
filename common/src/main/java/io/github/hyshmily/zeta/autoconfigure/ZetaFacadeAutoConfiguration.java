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

import io.github.hyshmily.zeta.DefaultZeta;
import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.endpoint.ZetaEndpoint;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sharding.HealthViewImpl;
import io.github.hyshmily.zeta.sync.local.CacheSyncProperties;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.NodeIdentity;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.clock.MonotonicStampClock;
import io.github.hyshmily.zeta.util.clock.StampClock;
import io.github.hyshmily.zeta.util.executor.SafeScheduledExecutorService;
import io.github.hyshmily.zeta.util.executor.ZetaThreadFactory;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Always-active auto-configuration that creates a single {@link Zeta} facade bean
 * and initializes the instance ID for queue naming.
 *
 * <p>The facade's optional dependencies are injected via {@link ObjectProvider} so the
 * bean can be created in any deployment mode:
 * <ul>
 *   <li><b>App-only</b> — receives the {@link HotKeyCache} and the app-side
 *       {@link TopK} sketch</li>
 *   <li><b>Worker-only</b> — cache and app detector are absent; the facade answers
 *       queries from the Worker-side components and rejects data-plane calls with
 *       {@code ZetaModeException}</li>
 *   <li><b>Coexistence</b> — receives everything the context provides</li>
 * </ul>
 *
 * <p>Registered first in {@code AutoConfiguration.imports} so the
 * {@code HotKey} facade singleton is available early and the instance ID
 * override is set before any queue-creating auto-configuration runs.
 */
@Internal
@AutoConfiguration
@EnableConfigurationProperties({ ZetaProperties.class, CacheSyncProperties.class })
@Slf4j
public class ZetaFacadeAutoConfiguration {

  private final ZetaProperties properties;

  /**
   * Bind the deployment identity before any bean of this configuration (or any
   * later auto-configuration) can create a queue or a stamp clock. Constructor
   * injection runs before {@code @PostConstruct}, closing the window in which
   * another component could read the auto-detected identity first.
   *
   * <p>Both overrides are JVM-global, so every context start sets them: a context
   * that leaves {@code instance-id} empty clears an override left by a previously
   * started context instead of inheriting it. {@link NodeIdentity#setInstanceOverride}
   * owns the null/blank normalization.
   *
   * @param properties the HotKey configuration properties
   */
  public ZetaFacadeAutoConfiguration(ZetaProperties properties) {
    this.properties = properties;
    NodeIdentity.setInstanceOverride(properties.getInstanceId());
    // Set after the instance override: clearing node-id re-derives from the
    // (possibly newly configured) instance id.
    Integer nodeId = properties.getNodeId();
    NodeIdentity.setNodeOverride(nodeId == null ? null : nodeId.longValue());
  }

  /**
   * Shared scheduler for Zeta's periodic work: the access-counter tide, the
   * broadcast buffer's deferred flush, and the TopK/expiry maintenance ticks
   * (design decisions: scheduling). Pool size configurable via
   * {@code zeta.local.scheduler-pool-size} (default 8).
   * Wrapped in a {@link SafeScheduledExecutorService} so a throwing periodic task never
   * kills the cadence of the shared scheduler.
   */
  @Bean(name = "hotKeyScheduler", destroyMethod = "shutdownNow")
  @ConditionalOnMissingBean(name = "hotKeyScheduler")
  public ScheduledExecutorService hotKeyScheduler() {
    return new SafeScheduledExecutorService(
      properties.getSchedulerPoolSize(),
      new ZetaThreadFactory(ZetaConstants.Threads.PREFIX_SCHEDULER)
    );
  }

  /**
   * Starts the cached {@link TimeSource} clock used by every TTL/monotonic
   * read. The identity overrides are already applied by the constructor.
   */
  @PostConstruct
  void registerTimeSourceUser() {
    // Reference-counted ownership: the ticker is JVM-global, so it must not be
    // stopped by the first context that closes while another still runs.
    TimeSource.registerLifecycleUser();
  }

  /**
   * Releases this context's claim on the shared clock ticker. The thread stops
   * only when the last context in the JVM is destroyed.
   */
  @PreDestroy
  void shutdownTimeSource() {
    TimeSource.releaseLifecycleUser();
  }

  /**
   * Create the shared {@link HealthView} for tracking Worker cluster health.
   *
   * <p>Declared here (the always-active, first-registered auto-configuration) so that
   * a single singleton is available to all consumers — {@link HotKeyCache},
   * {@link ZetaEndpoint}, {@link KeyReporter} and the heartbeat listener
   * container — eliminating the risk of multiple
   * inconsistent instances with independent state. Consumers inject this bean
   * directly; a locally built fallback would never receive heartbeat updates.
   *
   * @return a new {@link HealthViewImpl} instance
   */
  @Bean
  @ConditionalOnMissingBean
  public HealthView clusterHealthView() {
    HealthViewImpl view = defaultHealthView(properties);
    if (properties.getHeartbeat().getMinAliveWorkers() <= 0) {
      log.warn(
        "zeta.local.heartbeat.min-alive-workers is not configured; cluster health threshold defaults to one third of " +
          "observed Workers (minimum 1). A single surviving Worker is treated as a healthy cluster — see design decisions: heartbeat and health."
      );
    }
    return view;
  }

  /**
   * The default {@link HealthViewImpl} from the heartbeat properties — the one
   * assembly behind the shared bean, so a new heartbeat setting is threaded in
   * once and every consumer observes the same policy. The configured
   * {@code min-alive-workers} threshold is part of the assembly.
   *
   * @param properties the HotKey configuration properties (default source)
   * @return a new {@link HealthViewImpl}
   */
  static HealthViewImpl defaultHealthView(ZetaProperties properties) {
    HealthViewImpl view = new HealthViewImpl(
      properties.getHeartbeat().getTimeoutMs(),
      HealthViewImpl.DEFAULT_HEALTH_CACHE_TTL_MS,
      properties.getHeartbeat().isAdaptiveTimeout(),
      properties.getHeartbeat().getAdaptiveFloorMs()
    );
    view.setMinAliveWorkers(properties.getHeartbeat().getMinAliveWorkers());
    return view;
  }

  /**
   * Shared singleton {@link StampClock}: the single source of {@code dataVersion}
   * stamps for this node. Message trace IDs come from the cached
   * {@link io.github.hyshmily.zeta.util.TimeSource} wall clock instead
   * (design decisions: version stamps).
   *
   * <p>Declared in the always-active facade configuration, not in
   * {@link ZetaAutoConfiguration}, so the Worker (where the app-side
   * configuration is skipped) still receives it.
   *
   * <p>The implementation is the lock-free {@link MonotonicStampClock}: one CAS
   * variable carrying the shared stamp layout, so stamps stay decodable via
   * {@link StampClock#timestampOf(long)}.
   */
  @Bean
  @ConditionalOnMissingBean
  public StampClock stampClock() {
    return new MonotonicStampClock(NodeIdentity.getNodeId(), properties.getClockMaxForwardDriftMs());
  }

  /**
   * Create the {@link Zeta} facade bean. Each dependency is optional — the bean
   * adapts to whatever TopK / cache resources are available in the current deployment
   * mode.
   *
   * <p>The bulk refresh fan-out ({@code Zeta.refreshAll}) runs on the dedicated
   * {@code hotKeyExecutor} pool: each refresh task blocks on stamp allocation plus
   * business IO, so a bulk refresh of many keys must not park threads of the
   * interruptible load pool that serves SingleFlight reads and background refreshes.
   *
   * @param hotKeyCacheProvider     provider for the HotKeyCache (app-only or coexistence mode)
   * @param appTopKProvider         provider for the app-side TopK detector
   * @param refreshExecutorProvider provider for the dedicated refresh pool used by the
   *                                bulk refresh fan-out (absent in minimal contexts); the
   *                                pool is shared and never shut down here
   * @return a new {@link Zeta} facade instance
   */
  @Bean
  @ConditionalOnMissingBean
  @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
  public Zeta hotKey(
    ObjectProvider<HotKeyCache> hotKeyCacheProvider,
    @Qualifier("heavyKeeper") ObjectProvider<TopK> appTopKProvider,
    @Qualifier("hotKeyExecutor") ObjectProvider<Executor> refreshExecutorProvider
  ) {
    DefaultZeta facade = new DefaultZeta(hotKeyCacheProvider.getIfAvailable(), appTopKProvider.getIfAvailable());
    Executor refreshExecutor = refreshExecutorProvider.getIfAvailable();
    if (refreshExecutor != null) {
      facade.setRefreshExecutor(refreshExecutor);
    }
    return facade;
  }
}
