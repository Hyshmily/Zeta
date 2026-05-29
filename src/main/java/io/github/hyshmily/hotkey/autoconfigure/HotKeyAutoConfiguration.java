package io.github.hyshmily.hotkey.autoconfigure;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.algorithm.HeavyKeeper;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.broadcast.BroadcastPublisher;
import io.github.hyshmily.hotkey.entity.CacheEntry;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyCache;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyProperties;
import io.github.hyshmily.hotkey.hotkeycache.SingleFlight;
import io.github.hyshmily.hotkey.hotkeycache.SoftExpireManager;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Slf4j
@AutoConfiguration
@EnableConfigurationProperties(HotKeyProperties.class)
public class HotKeyAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public TopK hotKeyDetector(HotKeyProperties properties) {
    return new HeavyKeeper(
      properties.getTopK(),
      properties.getWidth(),
      properties.getDepth(),
      properties.getDecay(),
      properties.getMinCount()
    );
  }

  @Bean
  @ConditionalOnMissingBean
  public Cache<String, Object> hotLocalCache(HotKeyProperties properties) {
    long defaultTtlNanos = TimeUnit.MINUTES.toNanos(properties.getLocalCacheTtlMinutes());
    Caffeine<Object, Object> builder = Caffeine.newBuilder()
      .maximumSize(properties.getLocalCacheMaxSize())
      .expireAfter(
        new Expiry<Object, Object>() {
          @Override
          public long expireAfterCreate(Object key, Object value, long currentTimeNanos) {
            if (value instanceof CacheEntry entry) {
              if (entry.getExpireAtMs() == Long.MAX_VALUE) {
                return defaultTtlNanos;
              }
              long remaining = TimeUnit.MILLISECONDS.toNanos(entry.getExpireAtMs() - System.currentTimeMillis());
              return Math.max(1, remaining);
            }
            return defaultTtlNanos;
          }

          @Override
          public long expireAfterUpdate(Object key, Object value, long currentTimeNanos, long currentDuration) {
            return expireAfterCreate(key, value, currentTimeNanos);
          }

          @Override
          public long expireAfterRead(Object key, Object value, long currentTimeNanos, long currentDuration) {
            return currentDuration;
          }
        }
      );
    if (properties.getLocalCacheAccessTtlMinutes() > 0) {
      builder.expireAfterAccess(properties.getLocalCacheAccessTtlMinutes(), TimeUnit.MINUTES);
    }
    return builder.build();
  }

  @Bean
  @ConditionalOnMissingBean
  public SingleFlight singleFlight(HotKeyProperties properties, @Qualifier("hotKeyExecutor") Executor hotKeyExecutor) {
    return new SingleFlight(
      properties.getInflightMaxSize(),
      properties.getInflightTtlSeconds(),
      properties.getInflightTimeoutSeconds(),
      hotKeyExecutor
    );
  }

  @Bean
  @ConditionalOnMissingBean
  public SoftExpireManager softExpireManager(
    Cache<String, Object> hotLocalCache,
    @Qualifier("hotKeyExecutor") Executor hotKeyExecutor,
    HotKeyProperties properties
  ) {
    return new SoftExpireManager(
      hotLocalCache,
      hotKeyExecutor,
      properties.getSoftTtlMs(),
      properties.getRefreshConcurrency(),
      properties.getSoftExpireMaxSize(),
      properties.getSoftExpireTtlMinutes()
    );
  }

  @Bean("hotKeyExecutor")
  @ConditionalOnMissingBean(name = "hotKeyExecutor")
  public Executor hotKeyExecutor(HotKeyProperties properties) {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(properties.getExecutorCorePoolSize());
    executor.setMaxPoolSize(properties.getExecutorMaxPoolSize());
    executor.setQueueCapacity(properties.getExecutorQueueCapacity());
    executor.setThreadNamePrefix("hotkey-");
    executor.setRejectedExecutionHandler((r, e) -> {
      log.warn(
        "HotKey executor task rejected: corePool={}, maxPool={}, queueCapacity={}",
        properties.getExecutorCorePoolSize(),
        properties.getExecutorMaxPoolSize(),
        properties.getExecutorQueueCapacity()
      );
      throw new RejectedExecutionException("HotKey executor queue full");
    });
    executor.initialize();
    return executor;
  }

  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnBean(HotKeyCache.class)
  public HotKey hotKey(HotKeyCache hotKeyCache, TopK hotKeyDetector) {
    return new HotKey(hotKeyCache, hotKeyDetector);
  }

  @Bean
  @ConditionalOnMissingBean(type = "org.springframework.data.redis.core.RedisTemplate")
  public HotKeyCache hotKeyCache(
    TopK hotKeyDetector,
    Cache<String, Object> hotLocalCache,
    SingleFlight singleFlight,
    SoftExpireManager softExpireManager,
    Optional<BroadcastPublisher> broadcastPublisher,
    @Qualifier("hotKeyExecutor") Executor hotKeyExecutor,
    HotKeyProperties properties
  ) {
    return new HotKeyCache(
      hotKeyDetector,
      hotLocalCache,
      singleFlight,
      softExpireManager,
      broadcastPublisher,
      hotKeyExecutor,
      Optional.empty(),
      properties.getVersionKeyTtlMinutes()
    );
  }
}
