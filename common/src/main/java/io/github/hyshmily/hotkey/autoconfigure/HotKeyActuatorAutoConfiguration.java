package io.github.hyshmily.hotkey.autoconfigure;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.cache.CacheExpireManager;
import io.github.hyshmily.hotkey.cache.SingleFlight;
import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.endpoint.HotKeyEndpoint;
import io.github.hyshmily.hotkey.endpoint.RingEndpoint;
import io.github.hyshmily.hotkey.monitor.WorkerHealthMonitor;
import io.github.hyshmily.hotkey.reporting.HotKeyReporter;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import io.github.hyshmily.hotkey.sharding.RingManager;
import io.github.hyshmily.hotkey.sync.CacheSyncPublisher;
import io.github.hyshmily.hotkey.sync.VersionController;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.autoconfigure.AutoConfiguration;
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
    ObjectProvider<WorkerHealthMonitor> workerHealthMonitorProvider,
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
      workerHealthMonitorProvider.getIfAvailable(),
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
   */
  @Bean
  @ConditionalOnClass(name = "org.springframework.web.bind.annotation.RestController")
  @ConditionalOnProperty(prefix = "hotkey.local.consistent-hashing", name = "enabled", havingValue = "true")
  @ConditionalOnMissingBean
  public RingEndpoint ringEndpoint(RingManager ringManager) {
    return new RingEndpoint(ringManager);
  }
}
