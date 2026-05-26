package io.github.hyshmily.hotkey.hotkeycache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.hotkey.algorithm.Item;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.broadcast.BroadcastPublisher;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

public class HotKeyCache {

  private static final Logger log = LoggerFactory.getLogger(HotKeyCache.class);

  private final TopK hotKeyDetector;
  private final Cache<String, Object> caffeineCache;
  private final Cache<String, CompletableFuture<Object>> inflightLoads;
  private final Optional<BroadcastPublisher> broadcastPublisher;
  private final Executor hotKeyExecutor;
  private final int inflightTimeoutSeconds;

  // Soft expire
  private final Cache<String, Long> softExpireAt;
  private final long softTtlMs;
  private final Semaphore refreshLimiter;

  public HotKeyCache(
      TopK hotKeyDetector,
      Cache<String, Object> caffeineCache,
      Cache<String, CompletableFuture<Object>> inflightLoads,
      Optional<BroadcastPublisher> broadcastPublisher,
      Executor hotKeyExecutor) {
    this(hotKeyDetector, caffeineCache, inflightLoads, broadcastPublisher, hotKeyExecutor, 0, 0, 0, 0, 0);
  }

  public HotKeyCache(
      TopK hotKeyDetector,
      Cache<String, Object> caffeineCache,
      Cache<String, CompletableFuture<Object>> inflightLoads,
      Optional<BroadcastPublisher> broadcastPublisher,
      Executor hotKeyExecutor,
      int inflightTimeoutSeconds,
      long softTtlMs,
      int refreshConcurrency,
      int softExpireMaxSize,
      int softExpireTtlMinutes) {
    this.hotKeyDetector = hotKeyDetector;
    this.caffeineCache = caffeineCache;
    this.inflightLoads = inflightLoads;
    this.broadcastPublisher = broadcastPublisher;
    this.hotKeyExecutor = hotKeyExecutor;
    this.inflightTimeoutSeconds = inflightTimeoutSeconds;
    this.softTtlMs = softTtlMs;
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
    return hotKeyDetector.list().stream().anyMatch(item -> item.key().equals(cacheKey));
  }

  public <T> Optional<T> peek(String cacheKey) {
    if (invalidCacheKey(cacheKey)) {
      log.warn("peek: invalid cacheKey");
      return Optional.empty();
    }
    return Optional.ofNullable((T) caffeineCache.getIfPresent(cacheKey));
  }

  @SuppressWarnings("unchecked")
  public <T> Optional<T> get(String cacheKey, Supplier<T> redisReader) {
    if (invalidCacheKey(cacheKey)) {
      log.warn("get: invalid cacheKey");
      return Optional.empty();
    }
    Optional<T> cached = Optional.ofNullable((T) caffeineCache.getIfPresent(cacheKey));
    if (cached.isPresent()) {
      T val = cached.get();
      if (hotKeyDetector.add(cacheKey, 1).isHotKey()) {
        caffeineCache.put(cacheKey, val);
        log.debug("HotKey access, refresh local caffeine cache: {}", cacheKey);
      }
      return cached;
    }
    return loadSingleflight(cacheKey, redisReader);
  }

  @SuppressWarnings("unchecked")
  public <T> Optional<T> getWithStale(String cacheKey, Supplier<T> redisReader) {
    if (invalidCacheKey(cacheKey)) {
      log.warn("getWithStale: invalid cacheKey");
      return Optional.empty();
    }
    if (softExpireAt == null) {
      log.warn("Soft expire not enabled (softTtlMs=0), fallback to get()");
      return get(cacheKey, redisReader);
    }

    T cached = (T) caffeineCache.getIfPresent(cacheKey);
    if (cached != null) {
      Long expireAt = softExpireAt.getIfPresent(cacheKey);
      if (expireAt == null || expireAt < System.currentTimeMillis()) {
        triggerAsyncRefresh(cacheKey, redisReader);
      }
      if (hotKeyDetector.add(cacheKey, 1).isHotKey()) {
        caffeineCache.put(cacheKey, cached);
        log.debug("HotKey access, refresh local caffeine cache: {}", cacheKey);
      }
      return Optional.of(cached);
    }

    return loadSingleflight(cacheKey, redisReader);
  }
  public void invalidateOrUpdate(String cacheKey) {
      putInvalidate(cacheKey, () -> {
        // No-op Redis mutation since we're only invalidating local cache
      });
  }

  public void invalidateAll(Collection<String> cacheKeys) {
    List<String> validKeys = cacheKeys.stream()
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
                      () -> log.debug("No broadcast publisher found, please enable Broadcast")));
    };
    if (TransactionSynchronizationManager.isSynchronizationActive()) {
      TransactionSynchronizationManager.registerSynchronization(
              new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                  task.run();
                }
              });
      return;
    }
    task.run();
  }

  public void putThrough(String cacheKey, Object value, Runnable redisWriter) {
    if (invalidCacheKey(cacheKey)) {
      log.warn("putThrough: invalid cacheKey");
      return;
    }
    runAfterCommit(() -> {
      redisWriter.run();
      caffeineCache.put(cacheKey, value);
      broadcastPublisher.ifPresentOrElse(
          p -> p.invalidateHotKey(cacheKey),
          () -> log.debug("No broadcast publisher found, please enable Broadcast"));
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
      caffeineCache.invalidate(cacheKey);
      broadcastPublisher.ifPresentOrElse(
          p -> p.invalidateHotKey(cacheKey),
          () -> log.debug("No broadcast publisher found, please enable Broadcast"));
    };
    if (TransactionSynchronizationManager.isSynchronizationActive()) {
      TransactionSynchronizationManager.registerSynchronization(
          new TransactionSynchronization() {
            @Override
            public void afterCommit() {
              task.run();
            }
          });
      return;
    }
    task.run();
  }

  @SuppressWarnings("unchecked")
  private <T> Optional<T> loadSingleflight(String cacheKey, Supplier<T> redisReader) {
    CompletableFuture<Object> loadFuture = inflightLoads
        .asMap()
        .computeIfAbsent(cacheKey,
            key -> CompletableFuture.supplyAsync(() -> (Object) redisReader.get(), hotKeyExecutor)
                .orTimeout(inflightTimeoutSeconds, TimeUnit.SECONDS)
                .whenComplete((_, _) -> inflightLoads.invalidate(key)));

    try {
      return Optional.ofNullable((T) loadFuture.join()).map(value -> {
        if (hotKeyDetector.add(cacheKey, 1).isHotKey()) {
          caffeineCache.put(cacheKey, value);
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

    CompletableFuture
        .supplyAsync(() -> (Object) redisReader.get(), hotKeyExecutor)
        .whenComplete((value, error) -> {
          try {
            if (error != null) {
              log.warn("Async soft refresh failed: {}", cacheKey, error);
              return;
            }
            if (value != null) {
              caffeineCache.put(cacheKey, value);
              softExpireAt.put(cacheKey, System.currentTimeMillis() + softTtlMs);
            }
          } finally {
            refreshLimiter.release();
          }
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
          });
      return;
    }
    log.warn("putThrough called outside transaction, submitting to async executor");
    CompletableFuture.runAsync(task, hotKeyExecutor).exceptionally(e -> {
      log.error("Async Redis write failed after non-transactional putThrough", e);
      return null;
    });
  }

  @Scheduled(fixedDelayString = "${hotkey.decay-period:20}", timeUnit = TimeUnit.SECONDS)
  private void cleanHotKeys() {
    hotKeyDetector.fading();
    log.debug("HeavyKeeper count has decayed");
  }

  @Scheduled(fixedDelay = 60_000)
  private void drainExpelled() {
    List<Item> items = new ArrayList<>();
    hotKeyDetector.expelled().drainTo(items, 1000);
    if (!items.isEmpty()) {
      log.info("Drained {} expelled hot keys: {}", items.size(),
          items.stream().map(Item::key).collect(Collectors.joining(",")));
    }
  }
}