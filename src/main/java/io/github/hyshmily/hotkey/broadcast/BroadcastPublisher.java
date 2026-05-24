package io.github.hyshmily.hotkey.broadcast;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.hotkey.HotKeyCache;
import jakarta.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

public class BroadcastPublisher {

  private static final Logger log = LoggerFactory.getLogger(BroadcastPublisher.class);

  static final String TYPE_INVALIDATE = "INVALIDATE";

  private final RabbitTemplate rabbitTemplate;
  private final BroadcastProperties properties;

  private Cache<String, Boolean> recentBroadcasts;

  public BroadcastPublisher(RabbitTemplate rabbitTemplate, BroadcastProperties properties) {
    this.rabbitTemplate = rabbitTemplate;
    this.properties = properties;
  }

  @PostConstruct
  public void init() {
    this.recentBroadcasts = Caffeine.newBuilder()
      .expireAfterWrite(properties.getDedupWindowSeconds(), TimeUnit.SECONDS)
      .maximumSize(properties.getDedupMaxSize())
      .build();
  }

  public void broadcastHotKey(String cacheKey) {
    sendDeduped(cacheKey, null);
  }

  public void invalidateHotKey(String cacheKey) {
    sendDeduped(cacheKey, TYPE_INVALIDATE);
  }

  private void sendDeduped(String cacheKey, String type) {
    if (HotKeyCache.invalidCacheKey(cacheKey)) {
      log.warn("Invalid cacheKey: {}", cacheKey);
      return;
    }

    Optional.ofNullable(recentBroadcasts.getIfPresent(cacheKey)).ifPresentOrElse(
      _ -> log.debug("Skip duplicate broadcast: {}", cacheKey),
      () ->
        Optional.ofNullable(recentBroadcasts.asMap().putIfAbsent(cacheKey, Boolean.TRUE)).ifPresentOrElse(
          _ -> log.debug("Concurrent broadcast already handled by another thread, skip: {}", cacheKey),
          () -> {
            try {
              Message msg = buildMessage(cacheKey, type);
              rabbitTemplate.send(properties.getExchangeName(), "", msg);
              log.debug("HotKey broadcast: {} type={}", cacheKey, type);
            } catch (Exception e) {
              recentBroadcasts.invalidate(cacheKey);
              log.error("Failed to broadcast hot key: {}", cacheKey, e);
            }
          }
        )
    );
  }

  private static Message buildMessage(String cacheKey, String type) {
    if (type == null) {
      return new Message(cacheKey.getBytes(StandardCharsets.UTF_8));
    }
    MessageProperties props = new MessageProperties();
    props.setHeader("type", type);
    return new Message(cacheKey.getBytes(StandardCharsets.UTF_8), props);
  }
}
