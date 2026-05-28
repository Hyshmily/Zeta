package io.github.hyshmily.hotkey.hotkeycache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.broadcast.BroadcastPublisher;
import io.github.hyshmily.hotkey.entity.CacheEntry;
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

  private static long expireAt(long ttlMs) {
    return ttlMs > 0 ? System.currentTimeMillis() + ttlMs : Long.MAX_VALUE;
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
      raw instanceof CacheEntry vv ? (T) vv.getValue() : (T) raw
    );
  }

  public <T> Optional<T> get(String cacheKey, Supplier<T> reader) {
    return get(cacheKey, reader, Long.MAX_VALUE);
  }

  @SuppressWarnings("unchecked")
  public <T> Optional<T> get(String cacheKey, Supplier<T> reader, long ttlMs) {
    if (invalidCacheKey(cacheKey)) {
      log.warn("get: invalid cacheKey");
      return Optional.empty();
    }

    return Optional.ofNullable(caffeineCache.getIfPresent(cacheKey))
      .map(raw -> {
        T val = raw instanceof CacheEntry vv ? (T) vv.getValue() : (T) raw;
        hotKeyDetector.add(cacheKey, 1);

        return val;
      })
      .or(() -> loadSingleflight(cacheKey, reader, ttlMs));
  }

  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader) {
    return getWithSoftExpire(cacheKey, reader, this.softTtlMs);
  }

  @SuppressWarnings("unchecked")
  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader, long softTtlMs) {
    if (invalidCacheKey(cacheKey)) {
      log.warn("getWithSoftExpire: invalid cacheKey");
      return Optional.empty();
    }
    if (softExpireAt == null) {
      log.debug("Soft expire not enabled (softTtlMs=0), fallback to get()");
      return get(cacheKey, reader);
    }

    return Optional.ofNullable(caffeineCache.getIfPresent(cacheKey))
      .map(raw -> {
        T cached = raw instanceof CacheEntry vv ? (T) vv.getValue() : (T) raw;
        Long expireAt = softExpireAt.getIfPresent(cacheKey);

        if (expireAt == null || expireAt < System.currentTimeMillis()) {
          triggerAsyncRefresh(cacheKey, reader, softTtlMs);
        }

        hotKeyDetector.add(cacheKey, 1);

        return cached;
      })
      .or(() -> loadSingleflight(cacheKey, reader));
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
      log.debug("invalidateAll: all cacheKeys are invalid");
      return;
    }
    Runnable task = () -> {
      caffeineCache.invalidateAll(validKeys);
      if (broadcastPublisher.isPresent()) {
        BroadcastPublisher p = broadcastPublisher.get();
        validKeys.forEach(p::invalidateHotKey);
      } else {
        log.debug("No broadcast publisher found, skip broadcast for {} keys", validKeys.size());
      }
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
    putThrough(cacheKey, value, redisWriter, Long.MAX_VALUE);
  }

  public <T> void putThrough(String cacheKey, T value, Runnable redisWriter, long ttlMs) {
    if (invalidCacheKey(cacheKey)) {
      log.warn("putThrough: invalid cacheKey");
      return;
    }
    runAfterCommit(() -> {
      redisWriter.run();
      long version = nextVersion(cacheKey);
      caffeineCache.put(cacheKey, new CacheEntry(value, version, expireAt(ttlMs)));
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

  private <T> Optional<T> loadSingleflight(String cacheKey, Supplier<T> reader) {
    return loadSingleflight(cacheKey, reader, Long.MAX_VALUE);
  }

  @SuppressWarnings("unchecked")
  private <T> Optional<T> loadSingleflight(String cacheKey, Supplier<T> reader, long ttlMs) {
    CompletableFuture<Object> loadFuture = inflightLoads
      .asMap()
      .computeIfAbsent(cacheKey, key ->
        CompletableFuture.supplyAsync(() -> (Object) reader.get(), hotKeyExecutor)
          .orTimeout(inflightTimeoutSeconds, TimeUnit.SECONDS)
          .whenComplete((value, error) -> {
            inflightLoads.invalidate(key);
            if (error != null) {
              log.debug("singleflight load failed: key={}", key, error);
            } else if (value == null) {
              log.debug("singleflight returned null: key={}", key);
            }
          })
      );

    try {
      return Optional.ofNullable((T) loadFuture.join()).map(value -> {
        if (hotKeyDetector.add(cacheKey, 1).isHotKey()) {
          caffeineCache.put(cacheKey, new CacheEntry(value, 0L, expireAt(ttlMs)));
          if (softExpireAt != null) {
            softExpireAt.put(cacheKey, System.currentTimeMillis() + softTtlMs);
          }
          broadcastPublisher.ifPresent(p -> p.broadcastHotKey(cacheKey));
          log.debug("HotKey detected and loaded into local caffeine cache: {}", cacheKey);
        }
        return value;
      });
    } catch (Exception e) {
      inflightLoads.invalidate(cacheKey);
      log.warn("singleflight join failed: key={}", cacheKey, e);
      return Optional.empty();
    }
  }

  private <T> void triggerAsyncRefresh(String cacheKey, Supplier<T> reader, long softTtlMs) {
    if (!refreshLimiter.tryAcquire()) {
      log.debug("Refresh limiter blocked, skip async refresh: {}", cacheKey);
      return;
    }

    CompletableFuture.supplyAsync(() -> (Object) reader.get(), hotKeyExecutor).whenComplete((value, error) -> {
      try {
        if (error != null) {
          log.warn("Async soft refresh failed: {}", cacheKey, error);
          return;
        }
        if (value != null) {
          Object existing = caffeineCache.getIfPresent(cacheKey);
          long keepExpireAt = (existing instanceof CacheEntry ce) ? ce.getExpireAtMs() : Long.MAX_VALUE;
          caffeineCache.put(cacheKey, new CacheEntry(value, 0L, keepExpireAt));
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
    log.debug("putThrough called outside transaction, submitting to async executor");
    CompletableFuture.runAsync(task, hotKeyExecutor).exceptionally(e -> {
      log.error("Async Redis write failed after non-transactional putThrough", e);
      return null;
    });
  }
}
