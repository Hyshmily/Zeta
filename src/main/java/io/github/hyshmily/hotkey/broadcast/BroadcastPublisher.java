package io.github.hyshmily.hotkey.broadcast;

import static io.github.hyshmily.hotkey.broadcast.BroadcastProperties.*;
import static io.github.hyshmily.hotkey.hotkeycache.HotKeyCache.invalidCacheKey;
import static io.github.hyshmily.hotkey.hotkeycache.HotKeyCache.invalidTypeKey;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import jakarta.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

@Slf4j
@RequiredArgsConstructor
public class BroadcastPublisher {

  private static final long VERSIONED_HOT_KEY_DEFAULT_VERSION = 0L;

  private final RabbitTemplate rabbitTemplate;
  private final BroadcastProperties properties;
  private Cache<String, Long> recentBroadcasts;

  @PostConstruct
  public void init() {
    this.recentBroadcasts = Caffeine.newBuilder()
      .expireAfterWrite(properties.getDedupWindowSeconds(), TimeUnit.SECONDS)
      .maximumSize(properties.getDedupMaxSize())
      .build();
  }

  public void broadcastHotKey(String cacheKey) {
    sendDeduped(cacheKey, TYPE_HOT, VERSIONED_HOT_KEY_DEFAULT_VERSION);
  }

  public void invalidateHotKey(String cacheKey) {
    sendDeduped(cacheKey, TYPE_INVALIDATE, VERSIONED_HOT_KEY_DEFAULT_VERSION);
  }

  public void broadcastHotKeyWithVersion(String cacheKey, long version) {
    sendDeduped(cacheKey, TYPE_HOT, version);
  }

  private void sendDeduped(String cacheKey, String type, long version) {
    if (invalidCacheKey(cacheKey) || invalidTypeKey(type)) {
      log.warn("Invalid cacheKey or type, skip broadcast: cacheKey={}, type={}", cacheKey, type);
      return;
    }
    String compositeKey = type + ":" + cacheKey;
    Long old = recentBroadcasts.asMap().putIfAbsent(compositeKey, version);

    Optional.ofNullable(old)
      .filter(existing -> existing >= version)
      .ifPresentOrElse(
        existing ->
          log.info(
            "Skip broadcast due to recent broadcast with same or newer version: cacheKey={}, type={}, existingVersion={}, newVersion={}",
            cacheKey,
            type,
            existing,
            version
          ),
        () -> {
          if (old != null) {
            recentBroadcasts.put(compositeKey, version);
          }

          Message message = buildVersionedMessage(cacheKey, type, version);
          rabbitTemplate.send(properties.getExchangeName(), "", message);
        }
      );
  }

  private static Message buildVersionedMessage(String cacheKey, String type, long version) {
    MessageProperties props = new MessageProperties();
    props.setHeader("type", type);
    props.setHeader("version", version);
    return new Message(cacheKey.getBytes(StandardCharsets.UTF_8), props);
  }
}
