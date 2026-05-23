package io.github.hyshmily.hotkey;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.algorithm.AddResult;
import io.github.hyshmily.hotkey.algorithm.TopK;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

public class HotKeyCache {

  private static final Logger log = LoggerFactory.getLogger(HotKeyCache.class);

  private final TopK hotKeyDetector;
  private final Cache<String, Object> caffeineCache;
  private final RedisTemplate<String, Object> redisTemplate;
  private final Optional<HotKeyBroadcaster> broadcaster;

  public HotKeyCache(
    TopK hotKeyDetector,
    Cache<String, Object> caffeineCache,
    RedisTemplate<String, Object> redisTemplate,
    Optional<HotKeyBroadcaster> broadcaster
  ) {
    this.hotKeyDetector = hotKeyDetector;
    this.caffeineCache = caffeineCache;
    this.redisTemplate = redisTemplate;
    this.broadcaster = broadcaster;
  }

  private String cacheKey(String redisHashKey, String fieldKey) {
    return redisHashKey + ":" + fieldKey;
  }

  // 穿透 Caffeine、Redis，返回 null（可接受以便于查询 DataBase）
  // Penetrate Caffeine and Redis, return null (acceptable for DB fallback)
  public Object get(String redisHashKey, String fieldKey) {
    String cacheKey = cacheKey(redisHashKey, fieldKey);

    return Optional.ofNullable(caffeineCache.getIfPresent(cacheKey))
      // 1. 查 Caffeine L1 缓存
      // 1. Check Caffeine L1 cache
      .map(value -> {
        log.debug("Caffeine L1 hit: key={}", cacheKey);
        AddResult result = hotKeyDetector.add(cacheKey, 1);
        if (result.isHotKey()) {
          caffeineCache.put(cacheKey, value);
          log.debug("HotKey access, refresh local caffeine cache expiration time: {}", cacheKey);
        }
        return value;
      })
      .orElseGet(() -> {
        // 2. 查 Redis L2
        // 2. Check Redis L2
        Object redisValue = redisTemplate.opsForHash().get(redisHashKey, fieldKey);

        return Optional.ofNullable(redisValue)
          .map(value -> {
            AddResult addResult = hotKeyDetector.add(cacheKey, 1);
            if (addResult.isHotKey()) {
              caffeineCache.put(cacheKey, value);
      broadcaster.ifPresentOrElse(
        p -> p.broadcastHotKey(redisHashKey, fieldKey),
        () -> log.debug("No broadcast publisher found,please enable Broadcast ")
      );
              log.debug("HotKey detected and added to local caffeine cache: {}", cacheKey);
            }
            return redisValue;
          })
          .orElseGet(() -> {
            log.debug("Caffeine L1 miss,Redis L2 miss: key={}", cacheKey);
            return null;
          });
      });
  }

  public void putAndBroadcast(String redisHashKey, String fieldKey, Object value) {
    String cacheKey = cacheKey(redisHashKey, fieldKey);
    runAfterCommit(() -> {
      redisTemplate.opsForHash().put(redisHashKey, fieldKey, value);
      caffeineCache.put(cacheKey, value);
      // 需要开启 Broadcast 才会有广播功能
      // Broadcast requires the Broadcast module to be enabled
      broadcaster.ifPresentOrElse(
        p -> p.broadcastHotKey(redisHashKey, fieldKey),
        () -> log.debug("No broadcast publisher found,please enable Broadcast ")
      );
    });
  }

  private void runAfterCommit(Runnable task) {
    if (TransactionSynchronizationManager.isSynchronizationActive()) {
      TransactionSynchronizationManager.registerSynchronization(
        new TransactionSynchronization() {
          @Override
          public void afterCommit() {
            task.run();
          }
        }
      );
      return;
    }
    task.run();
  }

  @Scheduled(fixedDelayString = "${hotkey.decay-period:20}", timeUnit = TimeUnit.SECONDS)
  public void cleanHotKeys() {
    hotKeyDetector.fading();
    log.debug("HeavyKeeper count has decayed");
  }
}
