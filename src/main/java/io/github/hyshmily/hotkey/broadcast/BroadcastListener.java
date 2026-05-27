package io.github.hyshmily.hotkey.broadcast;

import static io.github.hyshmily.hotkey.broadcast.BroadcastProperties.TYPE_HOT;
import static io.github.hyshmily.hotkey.broadcast.BroadcastProperties.TYPE_INVALIDATE;

import com.github.benmanes.caffeine.cache.Cache;
import com.rabbitmq.client.Channel;
import io.github.hyshmily.hotkey.entity.VersionedValue;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;

@Slf4j
@RequiredArgsConstructor
public class BroadcastListener {

  private final Cache<String, Object> caffeineCache;
  private final Function<String, Object> redisLoader;
  private final BroadcastProperties properties;
  private final ScheduledExecutorService scheduler;

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
    long version = msg.getMessageProperties().getHeader("version") instanceof Number n ? n.longValue() : 0L;

    Runnable task = switch (type) {
      case TYPE_HOT -> () -> handleVersionedHotKey(cacheKey, version);
      case TYPE_INVALIDATE -> () -> handleInvalidateCacheKey(cacheKey);
      default -> () -> log.warn("Unknown broadcast type: {}, cacheKey: {}", type, cacheKey);
    };

    floatTimeDelay(task);
  }

  private void handleInvalidateCacheKey(String cacheKey) {
    caffeineCache.invalidate(cacheKey);
    log.debug("HotKey invalidated by broadcast: {}", cacheKey);
  }

  private void handleVersionedHotKey(String cacheKey, long version) {
    Object existing = caffeineCache.getIfPresent(cacheKey);
    if (existing instanceof VersionedValue vv && vv.getVersion() >= version) {
      return;
    }

    Object value = redisLoader.apply(cacheKey);
    if (value == null) {
      log.warn("Versioned broadcast key not in Redis: {}", cacheKey);
      return;
    }

    caffeineCache
      .asMap()
      .compute(cacheKey, (_, cur) -> {
        if (cur instanceof VersionedValue vv && vv.getVersion() >= version) {
          return cur;
        }
        return new VersionedValue(value, version);
      });
  }

  private void floatTimeDelay(Runnable task) {
    long jitterMs = properties.getWarmupJitterMs();
    if (jitterMs > 0) {
      long delay = ThreadLocalRandom.current().nextLong(jitterMs);
      if (delay > 0) {
        scheduler.schedule(task, delay, TimeUnit.MILLISECONDS);
        return;
      }
    }
    task.run();
  }
}
