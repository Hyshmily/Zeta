package io.github.hyshmily.hotkey.broadcast;

import static io.github.hyshmily.hotkey.broadcast.BroadcastProperties.TYPE_HOT;
import static io.github.hyshmily.hotkey.broadcast.BroadcastProperties.TYPE_INVALIDATE;

import com.github.benmanes.caffeine.cache.Cache;
import com.rabbitmq.client.Channel;
import io.github.hyshmily.hotkey.entity.VersionedValue;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;

@Slf4j
@RequiredArgsConstructor
public class BroadcastListener {

  private final Cache<String, Object> caffeineCache;
  private final Function<String, Object> redisLoader;
  private final BroadcastProperties properties;

  @RabbitListener(queues = "#{@broadcastProperties.queueName}", ackMode = "MANUAL")
  public void handleHotKeyMessage(Channel channel, Message msg) throws IOException {
    long tag = msg.getMessageProperties().getDeliveryTag();
    try {
      processBroadcast(msg);
      channel.basicAck(tag, false);
    } catch (Exception e) {
      log.error("HotKey broadcast processing failed: body={}", new String(msg.getBody()), e);
      channel.basicNack(tag, false, false);
    }
  }

  private void processBroadcast(Message msg) {
    byte[] body = msg.getBody();
    if (body == null || body.length == 0) {
      log.warn("Received broadcast message with empty body");
      return;
    }

    String cacheKey = new String(body, StandardCharsets.UTF_8);
    String type = msg.getMessageProperties().getHeader("type");

    switch (type) {
      case TYPE_HOT -> {
        Object verObj = msg.getMessageProperties().getHeader("version");
        long version = verObj instanceof Number n ? n.longValue() : 0L;
        handleVersionedHotKey(cacheKey, version);
      }
      case TYPE_INVALIDATE -> handleInvalidateCacheKey(cacheKey);
      case null, default -> log.warn("Unknown broadcast type: {}, cacheKey: {}", type, cacheKey);
    }
  }

  private void handleInvalidateCacheKey(String cacheKey) {
    floatTimeDelay();
    caffeineCache.invalidate(cacheKey);
    log.debug("HotKey invalidated by broadcast: {}", cacheKey);
  }

  private void handleVersionedHotKey(String cacheKey, long version) {
    floatTimeDelay();

    Object existing = caffeineCache.getIfPresent(cacheKey);
    if (existing instanceof VersionedValue vv && vv.getVersion() >= version) {
      return;
    }

    Optional.ofNullable(redisLoader.apply(cacheKey)).ifPresentOrElse(
      value -> {
        caffeineCache.put(cacheKey, new VersionedValue(value, version));
        log.debug("Versioned broadcast loaded: {} version={}", cacheKey, version);
      },
      () -> log.warn("Versioned broadcast key not in Redis: {}", cacheKey)
    );
  }

  private void floatTimeDelay() {
    long jitterMs = properties.getWarmupJitterMs();
    if (jitterMs > 0) {
      long delay = ThreadLocalRandom.current().nextLong(jitterMs);
      if (delay > 0) {
        try {
          Thread.sleep(delay);
        } catch (InterruptedException e) {
          log.error("HotKey broadcast processing interrupted during jitter delay", e);
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
