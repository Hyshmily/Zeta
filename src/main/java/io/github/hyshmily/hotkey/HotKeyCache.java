package io.github.hyshmily.hotkey;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.algorithm.Item;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.broadcast.BroadcastPublisher;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
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
  private final Optional<BroadcastPublisher> broadcastPublisher;

  public HotKeyCache(
    TopK hotKeyDetector,
    Cache<String, Object> caffeineCache,
    Optional<BroadcastPublisher> broadcastPublisher
  ) {
    this.hotKeyDetector = hotKeyDetector;
    this.caffeineCache = caffeineCache;
    this.broadcastPublisher = broadcastPublisher;
  }

  public static boolean invalidCacheKey(String cacheKey) {
    return cacheKey == null || cacheKey.isBlank();
  }

  public <T> Optional<T> get(String cacheKey) {
    return get(cacheKey, () -> null);
  }

  @SuppressWarnings("unchecked")
  public <T> Optional<T> get(String cacheKey, Supplier<T> redisReader) {
    if (invalidCacheKey(cacheKey)) {
      log.warn("get: invalid cacheKey");
      return Optional.empty();
    }
    Optional<T> cached = Optional.ofNullable((T) caffeineCache.getIfPresent(cacheKey));
    if (cached.isPresent()) {
      log.debug("Caffeine L1 hit: key={}", cacheKey);
      T val = cached.get();
      if (hotKeyDetector.add(cacheKey, 1).isHotKey()) {
        caffeineCache.put(cacheKey, val);
        log.debug("HotKey access, refresh local caffeine cache: {}", cacheKey);
      }
      return cached;
    }

    return Optional.ofNullable(redisReader.get())
      .map(value -> {
        if (hotKeyDetector.add(cacheKey, 1).isHotKey()) {
          caffeineCache.put(cacheKey, value);
          broadcastPublisher.ifPresent(p -> p.broadcastHotKey(cacheKey));
          log.debug("HotKey detected and loaded into local caffeine cache: {}", cacheKey);
        }
        return value;
      });
  }

  public void putAndBroadcast(String cacheKey, Object value, Runnable redisWriter) {
    if (invalidCacheKey(cacheKey)) {
      log.warn("putAndBroadcast: invalid cacheKey");
      return;
    }
    runAfterCommit(() -> {
      redisWriter.run();
      caffeineCache.put(cacheKey, value);
      broadcastPublisher.ifPresentOrElse(
        p -> p.invalidateHotKey(cacheKey),
        () -> log.debug("No broadcast publisher found, please enable Broadcast")
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
    log.warn("putAndBroadcast called outside transaction, submitting to async executor");
    CompletableFuture.runAsync(task).exceptionally(e -> {
      log.error("Async Redis write failed after non-transactional putAndBroadcast", e);
      return null;
    });
  }

  @Scheduled(fixedDelayString = "${hotkey.decay-period:20}", timeUnit = TimeUnit.SECONDS)
  public void cleanHotKeys() {
    hotKeyDetector.fading();
    log.debug("HeavyKeeper count has decayed");
  }

  @Scheduled(fixedDelay = 60_000)
  public void drainExpelled() {
    List<Item> items = new ArrayList<>();
    hotKeyDetector.expelled().drainTo(items);
    if (!items.isEmpty()) {
      log.info("Drained {} expelled hot keys: {}", items.size(),
        items.stream().map(Item::key).collect(Collectors.joining(",")));
    }
  }
}
