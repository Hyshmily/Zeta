package io.github.hyshmily.hotkey.actuator;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyProperties;
import io.github.hyshmily.hotkey.algorithm.TopK;
import java.util.concurrent.CompletableFuture;
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
      Cache<String, CompletableFuture<Object>> inflightLoads,
      HotKeyProperties properties) {
    return new HotKeyEndpoint(hotKeyDetector, hotLocalCache, inflightLoads, properties);
  }
}
