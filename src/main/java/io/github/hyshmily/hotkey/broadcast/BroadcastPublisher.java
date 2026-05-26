package io.github.hyshmily.hotkey.broadcast;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import static io.github.hyshmily.hotkey.hotkeycache.HotKeyCache.invalidCacheKey;
import static io.github.hyshmily.hotkey.hotkeycache.HotKeyCache.invalidTypeKey;
import static io.github.hyshmily.hotkey.broadcast.BroadcastProperties.TYPE_HOT;
import static io.github.hyshmily.hotkey.broadcast.BroadcastProperties.TYPE_INVALIDATE;

import jakarta.annotation.PostConstruct;
import java.nio.charset.Charset;
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
    sendDeduped(cacheKey, TYPE_HOT);
  }

  public void invalidateHotKey(String cacheKey) {
    sendDeduped(cacheKey, TYPE_INVALIDATE);
  }

  private void sendDeduped(String cacheKey, String type) {
    if (invalidCacheKey(cacheKey) || invalidTypeKey(type)) {
      log.warn("Invalid cacheKey or type, skip broadcast: cacheKey={}, type={}", cacheKey, type);
      return;
    }
    String compositeCacheKey = type + ":" + cacheKey;

    Optional.ofNullable(recentBroadcasts.getIfPresent(compositeCacheKey)).ifPresentOrElse(
      _ -> log.debug("Skip duplicate broadcast: {}", cacheKey),
      () ->
        Optional.ofNullable(recentBroadcasts.asMap().putIfAbsent(compositeCacheKey, Boolean.TRUE)).ifPresentOrElse(
          _ -> log.debug("Concurrent broadcast already handled by another thread, skip: {}", cacheKey),
          () -> {
            try {
              Message msg = buildMessage(cacheKey, type);
              rabbitTemplate.send(properties.getExchangeName(), "", msg);
              log.debug("HotKey broadcast: {} type={}", cacheKey, type);
            } catch (Exception e) {
              recentBroadcasts.invalidate(compositeCacheKey);
              log.error("Failed to broadcast hot key: {}", cacheKey, e);
            }
          }
        )
    );
  }

  private static Message buildMessage(String cacheKey, String type) {
    MessageProperties props = new MessageProperties();
    props.setHeader("type", type);
    return new Message(cacheKey.getBytes((Charset) StandardCharsets.UTF_8), props);
  }
}
