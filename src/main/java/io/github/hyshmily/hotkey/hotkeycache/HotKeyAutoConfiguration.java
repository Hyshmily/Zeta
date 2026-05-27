package io.github.hyshmily.hotkey.hotkeycache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.algorithm.HeavyKeeper;
import io.github.hyshmily.hotkey.algorithm.TopK;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
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
        properties.getMinCount());
  }

  @Bean
  @ConditionalOnMissingBean
  public Cache<String, Object> hotLocalCache(HotKeyProperties properties) {
    Caffeine<Object, Object> builder = Caffeine.newBuilder()
        .maximumSize(properties.getLocalCacheMaxSize())
        .expireAfterWrite(properties.getLocalCacheTtlMinutes(), TimeUnit.MINUTES);
    if (properties.getLocalCacheAccessTtlMinutes() > 0) {
      builder.expireAfterAccess(properties.getLocalCacheAccessTtlMinutes(), TimeUnit.MINUTES);
    }
    return builder.build();
  }

  @Bean
  @ConditionalOnMissingBean
  public Cache<String, CompletableFuture<Object>> inflightLoads(HotKeyProperties properties) {
    return Caffeine.newBuilder()
        .maximumSize(properties.getInflightMaxSize())
        .expireAfterWrite(properties.getInflightTtlSeconds(), TimeUnit.SECONDS)
        .build();
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
      log.warn("HotKey executor task rejected: corePool={}, maxPool={}, queueCapacity={}",
        properties.getExecutorCorePoolSize(), properties.getExecutorMaxPoolSize(), properties.getExecutorQueueCapacity());
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
      Cache<String, CompletableFuture<Object>> inflightLoads,
      Optional<io.github.hyshmily.hotkey.broadcast.BroadcastPublisher> broadcastPublisher,
      @Qualifier("hotKeyExecutor") Executor hotKeyExecutor,
      HotKeyProperties properties) {
    return new HotKeyCache(
        hotKeyDetector, hotLocalCache, inflightLoads, broadcastPublisher, hotKeyExecutor,
        Optional.empty(),
        properties.getInflightTimeoutSeconds(),
        properties.getSoftTtlMs(),
        properties.getRefreshConcurrency(),
        properties.getSoftExpireMaxSize(),
        properties.getSoftExpireTtlMinutes(),
        properties.getVersionKeyTtlMinutes());
  }

}
