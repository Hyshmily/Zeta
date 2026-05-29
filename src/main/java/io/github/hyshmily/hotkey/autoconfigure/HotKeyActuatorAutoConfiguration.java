package io.github.hyshmily.hotkey.autoconfigure;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.actuator.HotKeyEndpoint;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyProperties;
import io.github.hyshmily.hotkey.hotkeycache.SingleFlight;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@ConditionalOnClass(Endpoint.class)
public class HotKeyActuatorAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public HotKeyEndpoint hotKeyEndpoint(
      TopK hotKeyDetector,
      Cache<String, Object> hotLocalCache,
      SingleFlight singleFlight,
      HotKeyProperties properties) {
    return new HotKeyEndpoint(hotKeyDetector, hotLocalCache, singleFlight, properties);
  }
}
