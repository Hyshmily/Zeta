package io.github.hyshmily.hotkey.broadcast;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.hotkey.HotKeyBroadcaster;
import jakarta.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

public class BroadcastPublisher implements HotKeyBroadcaster {

  private static final Logger log = LoggerFactory.getLogger(BroadcastPublisher.class);

  private final RabbitTemplate rabbitTemplate;
  private final BroadcastProperties properties;

  private Cache<String, Boolean> recentBroadcasts;

  public BroadcastPublisher(
    RabbitTemplate rabbitTemplate,
    BroadcastProperties properties
  ) {
    this.rabbitTemplate = rabbitTemplate;
    this.properties = properties;
  }

  @PostConstruct
  public void init() {
    this.recentBroadcasts = Caffeine.newBuilder()
      .expireAfterWrite(properties.getDedupWindowSeconds(), TimeUnit.SECONDS)
      .maximumSize(500)
      .build();
  }

  // 基础校验 CacheKey
  // Basic validation of cache key
  private Optional<String> validateCacheKey(String redisHashKey, String fieldKey) {
    if (redisHashKey == null || redisHashKey.isBlank() || fieldKey == null || fieldKey.isBlank()) {
      log.warn("Invalid args: hk={}, fk={}", redisHashKey, fieldKey);
      return Optional.empty();
    }
    return Optional.of(redisHashKey + ":" + fieldKey);
  }

  @Override
  public void broadcastHotKey(String redisHashKey, String fieldKey) {
    validateCacheKey(redisHashKey, fieldKey).ifPresent(cacheKey ->
      publishHotKey(redisHashKey, fieldKey, cacheKey)
    );
  }

  private void publishHotKey(String redisHashKey, String fieldKey, String cacheKey) {
    Optional.ofNullable(recentBroadcasts.getIfPresent(cacheKey)).ifPresentOrElse(
      _ -> log.debug("Skip duplicate broadcast: {}", cacheKey),
      () ->
        Optional.ofNullable(recentBroadcasts.asMap().putIfAbsent(cacheKey, Boolean.TRUE)).ifPresentOrElse(
          _ -> log.debug("Concurrent broadcast already handled by another thread, skip: {}", cacheKey),
          () -> {
            try {
              MessageProperties props = new MessageProperties();
              props.setHeader("hk", redisHashKey);
              props.setHeader("fk", fieldKey);
              Message msg = new Message(cacheKey.getBytes(StandardCharsets.UTF_8), props);
              rabbitTemplate.send(properties.getExchangeName(), "", msg);
              log.debug("HotKey broadcast: {}", cacheKey);
            } catch (Exception e) {
              recentBroadcasts.invalidate(cacheKey);
              log.error("Failed to broadcast hot key: {}", cacheKey, e);
            }
          }
        )
    );
  }
}
