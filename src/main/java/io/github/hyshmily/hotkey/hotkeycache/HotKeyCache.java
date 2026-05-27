package io.github.hyshmily.hotkey.hotkeycache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.broadcast.BroadcastPublisher;
import io.github.hyshmily.hotkey.entity.VersionedValue;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Slf4j
public class HotKeyCache {

  private final TopK hotKeyDetector;
  private final Cache<String, Object> caffeineCache;
  private final Cache<String, CompletableFuture<Object>> inflightLoads;
  private final Optional<BroadcastPublisher> broadcastPublisher;
  private final Executor hotKeyExecutor;
  private final Optional<StringRedisTemplate> redisTemplate;
  private final int inflightTimeoutSeconds;

  // Soft expire
  private final Cache<String, Long> softExpireAt;
  private final long softTtlMs;
  private final Semaphore refreshLimiter;
  private final int versionKeyTtlMinutes;

  public HotKeyCache(
    TopK hotKeyDetector,
    Cache<String, Object> caffeineCache,
    Cache<String, CompletableFuture<Object>> inflightLoads,
    Optional<BroadcastPublisher> broadcastPublisher,
    Executor hotKeyExecutor,
    Optional<StringRedisTemplate> redisTemplate
  ) {
    this(
      hotKeyDetector,
      caffeineCache,
      inflightLoads,
      broadcastPublisher,
      hotKeyExecutor,
      redisTemplate,
      0,
      0,
      0,
      0,
      0,
      0
    );
  }

  public HotKeyCache(
    TopK hotKeyDetector,
    Cache<String, Object> caffeineCache,
    Cache<String, CompletableFuture<Object>> inflightLoads,
    Optional<BroadcastPublisher> broadcastPublisher,
    Executor hotKeyExecutor,
    Optional<StringRedisTemplate> redisTemplate,
    int inflightTimeoutSeconds,
    long softTtlMs,
    int refreshConcurrency,
    int softExpireMaxSize,
    int softExpireTtlMinutes,
    int versionKeyTtlMinutes
  ) {
    this.hotKeyDetector = hotKeyDetector;
    this.caffeineCache = caffeineCache;
    this.inflightLoads = inflightLoads;
    this.broadcastPublisher = broadcastPublisher;
    this.hotKeyExecutor = hotKeyExecutor;
    this.redisTemplate = redisTemplate;
    this.inflightTimeoutSeconds = inflightTimeoutSeconds;
    this.softTtlMs = softTtlMs;
    this.versionKeyTtlMinutes = versionKeyTtlMinutes;
    if (softTtlMs > 0) {
      this.softExpireAt = Caffeine.newBuilder()
        .maximumSize(softExpireMaxSize > 0 ? softExpireMaxSize : 50_000)
        .expireAfterWrite(softExpireTtlMinutes > 0 ? softExpireTtlMinutes : 60, TimeUnit.MINUTES)
        .build();
      this.refreshLimiter = new Semaphore(refreshConcurrency > 0 ? refreshConcurrency : 100);
    } else {
      this.softExpireAt = null;
      this.refreshLimiter = null;
    }
  }

  public static boolean invalidCacheKey(String cacheKey) {
    return cacheKey == null || cacheKey.isBlank();
  }

  public static boolean invalidTypeKey(String cacheKey) {
    return cacheKey == null || cacheKey.isBlank();
  }

  public boolean isHotKey(String cacheKey) {
    if (invalidCacheKey(cacheKey)) {
      log.warn("isHotKey: invalid cacheKey");
      return false;
    }
    return hotKeyDetector.contains(cacheKey);
  }

  @SuppressWarnings("unchecked")
  public <T> Optional<T> peek(String cacheKey) {
    if (invalidCacheKey(cacheKey)) {
      log.warn("peek: invalid cacheKey");
      return Optional.empty();
    }

    return Optional.ofNullable(caffeineCache.getIfPresent(cacheKey)).map(raw ->
      raw instanceof VersionedValue vv ? (T) vv.getValue() : (T) raw
    );
  }

  @SuppressWarnings("unchecked")
  public <T> Optional<T> get(String cacheKey, Supplier<T> redisReader) {
    if (invalidCacheKey(cacheKey)) {
      log.warn("get: invalid cacheKey");
      return Optional.empty();
    }

    return Optional.ofNullable(caffeineCache.getIfPresent(cacheKey))
      .map(raw -> {
        T val = raw instanceof VersionedValue vv ? (T) vv.getValue() : (T) raw;
        hotKeyDetector.add(cacheKey, 1);

        return val;
      })
      .or(() -> loadSingleflight(cacheKey, redisReader));
  }

  @SuppressWarnings("unchecked")
  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> redisReader) {
    if (invalidCacheKey(cacheKey)) {
      log.warn("getWithStale: invalid cacheKey");
      return Optional.empty();
    }
    if (softExpireAt == null) {
      log.warn("Soft expire not enabled (softTtlMs=0), fallback to get()");
      return get(cacheKey, redisReader);
    }

    return Optional.ofNullable(caffeineCache.getIfPresent(cacheKey))
      .map(raw -> {
        T cached = raw instanceof VersionedValue vv ? (T) vv.getValue() : (T) raw;
        Long expireAt = softExpireAt.getIfPresent(cacheKey);

        if (expireAt == null || expireAt < System.currentTimeMillis()) {
          triggerAsyncRefresh(cacheKey, redisReader);
        }

        hotKeyDetector.add(cacheKey, 1);

        return cached;
      })
      .or(() -> loadSingleflight(cacheKey, redisReader));
  }

  public void invalidate(String cacheKey) {
    if (invalidCacheKey(cacheKey)) {
      log.warn("invalidate: invalid cacheKey");
      return;
    }
    Runnable task = () -> {
      long version = nextVersion(cacheKey);
      caffeineCache.invalidate(cacheKey);
      broadcastPublisher.ifPresentOrElse(
        p -> p.broadcastHotKeyWithVersion(cacheKey, version),
        () -> log.debug("No broadcast publisher found, please enable Broadcast")
      );
    };
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

  public void invalidateAll(Collection<String> cacheKeys) {
    List<String> validKeys = cacheKeys
      .stream()
      .filter(k -> !invalidCacheKey(k))
      .toList();
    if (validKeys.isEmpty()) {
      log.warn("invalidateAll: all cacheKeys are invalid");
      return;
    }
    Runnable task = () -> {
      caffeineCache.invalidateAll(validKeys);
      validKeys.forEach(key ->
        broadcastPublisher.ifPresentOrElse(
          p -> p.invalidateHotKey(key),
          () -> log.debug("No broadcast publisher found, please enable Broadcast")
        )
      );
    };
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

  public <T> void putThrough(String cacheKey, T value, Runnable redisWriter) {
    if (invalidCacheKey(cacheKey)) {
      log.warn("putThrough: invalid cacheKey");
      return;
    }
    runAfterCommit(() -> {
      redisWriter.run();
      long version = nextVersion(cacheKey);
      caffeineCache.put(cacheKey, new VersionedValue(value, version));
      broadcastPublisher.ifPresentOrElse(
        p -> p.broadcastHotKeyWithVersion(cacheKey, version),
        () -> log.debug("No broadcast publisher found, please enable Broadcast")
      );
    });
  }

  public void putInvalidate(String cacheKey, Runnable redisMutation) {
    if (invalidCacheKey(cacheKey)) {
      log.warn("putInvalidate: invalid cacheKey");
      return;
    }
    Runnable task = () -> {
      try {
        redisMutation.run();
      } catch (Exception e) {
        log.error("putInvalidate failed, skip local invalidate and broadcast: {}", cacheKey, e);
        return;
      }
      long version = nextVersion(cacheKey);
      caffeineCache.invalidate(cacheKey);
      broadcastPublisher.ifPresentOrElse(
        p -> p.broadcastHotKeyWithVersion(cacheKey, version),
        () -> log.debug("No broadcast publisher found, please enable Broadcast")
      );
    };
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

  @SuppressWarnings("unchecked")
  private <T> Optional<T> loadSingleflight(String cacheKey, Supplier<T> redisReader) {
    CompletableFuture<Object> loadFuture = inflightLoads
      .asMap()
      .computeIfAbsent(cacheKey, key ->
        CompletableFuture.supplyAsync(() -> (Object) redisReader.get(), hotKeyExecutor)
          .orTimeout(inflightTimeoutSeconds, TimeUnit.SECONDS)
          .whenComplete((_, error) -> {
            inflightLoads.invalidate(key);
            if (error != null) {
              log.warn("singleflight load failed: key={}, error={}", key, error.getMessage());
            }
          })
      );

    try {
      return Optional.ofNullable((T) loadFuture.join()).map(value -> {
        if (hotKeyDetector.add(cacheKey, 1).isHotKey()) {
          caffeineCache.put(cacheKey, new VersionedValue(value, 0L));
          if (softExpireAt != null) {
            softExpireAt.put(cacheKey, System.currentTimeMillis() + softTtlMs);
          }
          // Broadcast requires the Broadcast module to be enabled
          broadcastPublisher.ifPresent(p -> p.broadcastHotKey(cacheKey));
          log.debug("HotKey detected and loaded into local caffeine cache: {}", cacheKey);
        }
        return value;
      });
    } catch (Exception e) {
      inflightLoads.invalidate(cacheKey);
      return Optional.empty();
    }
  }

  private <T> void triggerAsyncRefresh(String cacheKey, Supplier<T> redisReader) {
    if (!refreshLimiter.tryAcquire()) {
      log.debug("Refresh limiter blocked, skip async refresh: {}", cacheKey);
      return;
    }

    CompletableFuture.supplyAsync(() -> (Object) redisReader.get(), hotKeyExecutor).whenComplete((value, error) -> {
      try {
        if (error != null) {
          log.warn("Async soft refresh failed: {}", cacheKey, error);
          return;
        }
        if (value != null) {
          caffeineCache.put(cacheKey, new VersionedValue(value, 0L));
          softExpireAt.put(cacheKey, System.currentTimeMillis() + softTtlMs);
        }
      } finally {
        refreshLimiter.release();
      }
    });
  }

  private long nextVersion(String cacheKey) {
    return redisTemplate
      .map(t -> {
        try {
          String script =
            "local v = redis.call('INCR', KEYS[1]) " +
            "if tonumber(ARGV[1]) > 0 then " +
            "    redis.call('EXPIRE', KEYS[1], ARGV[1]) " +
            "end " +
            "return v";

          return t.execute(
            new DefaultRedisScript<>(script, Long.class),
            List.of("hotkey:ver:" + cacheKey),
            String.valueOf(versionKeyTtlMinutes * 60L)
          );
        } catch (Exception e) {
          log.warn("Redis version increment failed, fallback to nanoTime: {}", cacheKey, e);
          return System.nanoTime();
        }
      })
      .orElse(System.nanoTime());
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
    log.warn("putThrough called outside transaction, submitting to async executor");
    CompletableFuture.runAsync(task, hotKeyExecutor).exceptionally(e -> {
      log.error("Async Redis write failed after non-transactional putThrough", e);
      return null;
    });
  }
}
