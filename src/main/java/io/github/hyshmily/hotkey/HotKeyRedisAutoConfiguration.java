package io.github.hyshmily.hotkey;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.broadcast.BroadcastPublisher;
import java.util.Optional;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.RedisTemplate;

@AutoConfiguration(after = HotKeyAutoConfiguration.class)
@ConditionalOnClass(RedisTemplate.class)
@EnableConfigurationProperties(HotKeyProperties.class)
public class HotKeyRedisAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public HotKeyCache hotKeyCache(
    TopK hotKeyDetector,
    Cache<String, Object> hotLocalCache,
    Optional<BroadcastPublisher> broadcastPublisher
  ) {
    return new HotKeyCache(hotKeyDetector, hotLocalCache, broadcastPublisher);
  }
}
