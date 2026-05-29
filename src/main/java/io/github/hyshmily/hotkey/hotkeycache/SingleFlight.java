package io.github.hyshmily.hotkey.hotkeycache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SingleFlight {

  private final Cache<String, CompletableFuture<Object>> inflightLoads;
  private final Executor executor;
  private final int timeoutSeconds;

  public SingleFlight(int maxSize, int ttlSeconds, int timeoutSeconds, Executor executor) {
    this.inflightLoads = Caffeine.newBuilder()
      .maximumSize(maxSize)
      .expireAfterWrite(ttlSeconds, TimeUnit.SECONDS)
      .build();
    this.executor = executor;
    this.timeoutSeconds = timeoutSeconds;
  }

  public long estimatedInflightSize() {
    return inflightLoads.estimatedSize();
  }

  @SuppressWarnings("unchecked")
  public <T> Optional<T> load(String cacheKey, Supplier<T> reader) {
    CompletableFuture<Object> future = inflightLoads
      .asMap()
      .computeIfAbsent(cacheKey, key ->
        CompletableFuture.supplyAsync(() -> (Object) reader.get(), executor)
          .orTimeout(timeoutSeconds, TimeUnit.SECONDS)
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
      return Optional.ofNullable((T) future.join());
    } catch (Exception e) {
      inflightLoads.invalidate(cacheKey);
      log.warn("singleflight join failed: key={}", cacheKey, e);
      return Optional.empty();
    }
  }
}
