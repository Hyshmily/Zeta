package io.github.hyshmily.hotkey.broadcast;

import com.github.benmanes.caffeine.cache.Cache;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;

public class BroadcastListener {

  private static final Logger log = LoggerFactory.getLogger(BroadcastListener.class);

  private final Cache<String, Object> caffeineCache;
  private final Function<String, Object> redisLoader;

  public BroadcastListener(
    Cache<String, Object> caffeineCache,
    Function<String, Object> redisLoader
  ) {
    this.caffeineCache = caffeineCache;
    this.redisLoader = redisLoader;
  }

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

    if (BroadcastPublisher.TYPE_INVALIDATE.equals(type)) {
      caffeineCache.invalidate(cacheKey);
      log.debug("HotKey invalidated by broadcast: {}", cacheKey);
      return;
    }

    if (caffeineCache.getIfPresent(cacheKey) != null) {
      log.debug("HotKey broadcast skipped: local caffeine cache already exists: {}", cacheKey);
      return;
    }

    Optional.ofNullable(redisLoader.apply(cacheKey)).ifPresentOrElse(
      value -> {
        caffeineCache.put(cacheKey, value);
        log.debug("HotKey broadcast loaded into local caffeine cache: {}", cacheKey);
      },
      () -> log.warn("The broadcast HotKey does not exist in Redis: {}", cacheKey)
    );
  }
}
