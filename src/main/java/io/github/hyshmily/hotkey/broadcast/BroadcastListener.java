/*
 * Copyright 2026 Hyshmily. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.hyshmily.hotkey.broadcast;

import static io.github.hyshmily.hotkey.broadcast.BroadcastProperties.TYPE_HOT;
import static io.github.hyshmily.hotkey.broadcast.BroadcastProperties.TYPE_INVALIDATE;

import com.github.benmanes.caffeine.cache.Cache;
import com.rabbitmq.client.Channel;
import io.github.hyshmily.hotkey.entity.CacheEntry;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
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
    BroadcastMessage bm = BroadcastMessage.from(msg);
    if (bm == null) {
      log.debug("Received broadcast message with empty body");
      return;
    }

    Runnable task = () -> broadcastMessageTypeRouter(bm);
    floatTimeDelay(task);
  }

  private void broadcastMessageTypeRouter(BroadcastMessage msg) {
    switch (msg.type()) {
      case TYPE_HOT -> handleVersionedHotKey(msg.cacheKey(), msg.version(), msg.isVersionDegraded());
      case TYPE_INVALIDATE -> handleInvalidateCacheKey(msg.cacheKey());
      default -> log.warn("Unknown broadcast type: {}, cacheKey: {}", msg.type(), msg.cacheKey());
    }
  }

  private void handleInvalidateCacheKey(String cacheKey) {
    caffeineCache.invalidate(cacheKey);
    log.debug("HotKey invalidated by broadcast: {}", cacheKey);
  }

  private void handleVersionedHotKey(String cacheKey, long version, boolean degraded) {
    if (broadcastVersionGradeGuard(cacheKey, version, degraded) != null) {
      log.debug("Versioned broadcast key already up-to-date in Caffeine: {}", cacheKey);
      return;
    }

    Object value = redisLoader.apply(cacheKey);
    if (value == null) {
      log.debug("Versioned broadcast key not in Redis: {}", cacheKey);
      return;
    }

    final boolean degradedFinal = degraded;
    final long versionFinal = version;
    caffeineCache
      .asMap()
      .compute(cacheKey, (_, existingCacheEntry) -> {
        if (broadcastVersionGradeGuard(cacheKey, versionFinal, degradedFinal) != null) {
          log.debug("Versioned broadcast key already up-to-date in Caffeine during compute: {}", cacheKey);
          return existingCacheEntry;
        }

        long keepExpireAt = (existingCacheEntry instanceof CacheEntry ce) ? ce.getExpireAtMs() : Long.MAX_VALUE;
        return new CacheEntry(value, versionFinal, degradedFinal, keepExpireAt);
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

  private @Nullable Object broadcastVersionGradeGuard(String cacheKey, long version, boolean degraded) {
    Object existingCacheEntry = caffeineCache.getIfPresent(cacheKey);

    if (existingCacheEntry instanceof CacheEntry cacheEntry) {
      boolean existingDegraded = cacheEntry.isVersionDegraded();

      if (!existingDegraded && !degraded) {
        if (cacheEntry.getVersion() >= version) {
          return existingCacheEntry;
        }
      }
      if (!existingDegraded && degraded) {
        return existingCacheEntry;
      }

      if (existingDegraded && degraded) {
        if (cacheEntry.getVersion() >= version) {
          return existingCacheEntry;
        }
      }
    }
    return null;
  }
}
