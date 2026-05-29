package io.github.hyshmily.hotkey.autoconfigure;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.broadcast.BroadcastPublisher;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyCache;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyProperties;
import io.github.hyshmily.hotkey.hotkeycache.SingleFlight;
import io.github.hyshmily.hotkey.hotkeycache.SoftExpireManager;
import java.util.Optional;
import java.util.concurrent.Executor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;

@AutoConfiguration(after = {HotKeyAutoConfiguration.class, RedisAutoConfiguration.class})
@ConditionalOnClass(name = "org.springframework.data.redis.core.RedisTemplate")
@EnableConfigurationProperties(HotKeyProperties.class)
public class HotKeyRedisAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnBean(StringRedisTemplate.class)
  public HotKeyCache hotKeyCache(
    TopK hotKeyDetector,
    Cache<String, Object> hotLocalCache,
    SingleFlight singleFlight,
    SoftExpireManager softExpireManager,
    Optional<BroadcastPublisher> broadcastPublisher,
    @Qualifier("hotKeyExecutor") Executor hotKeyExecutor,
    StringRedisTemplate redisTemplate,
    HotKeyProperties properties
  ) {
    return new HotKeyCache(
      hotKeyDetector,
      hotLocalCache,
      singleFlight,
      softExpireManager,
      broadcastPublisher,
      hotKeyExecutor,
      Optional.of(redisTemplate),
      properties.getVersionKeyTtlMinutes()
    );
  }

  @Bean
  @ConditionalOnMissingBean
  public HotKey hotKey(HotKeyCache hotKeyCache, TopK hotKeyDetector) {
    return new HotKey(hotKeyCache, hotKeyDetector);
  }
}
