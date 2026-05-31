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

import static io.github.hyshmily.hotkey.broadcast.SyncMessage.TYPE_INVALIDATE;
import static io.github.hyshmily.hotkey.broadcast.SyncMessage.TYPE_REFRESH;

import com.github.benmanes.caffeine.cache.Cache;
import com.rabbitmq.client.Channel;
import io.github.hyshmily.hotkey.entity.CacheEntry;
import io.github.hyshmily.hotkey.hotkeycache.CacheExpireManager;
import io.github.hyshmily.hotkey.hotkeycache.VersionGuard;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;

/**
 * Listens for cache synchronization messages (INVALIDATE / REFRESH) from peer instances
 * via {@code hotkey.sync.exchange} (FanoutExchange).
 * <p>
 * Unlike {@link WorkerListener} which handles hot/cool lifecycle decisions, this listener
 * handles direct cache invalidation or refresh triggered by application-level data changes.
 * <p>
 * On INVALIDATE: atomically removes the entry from the local cache.
 * On REFRESH: reloads the value from Redis and updates the local cache, preserving existing
 * metadata (hard/soft TTLs, key state, versioning) except for value and version.
 * <p>
 * Ack is decoupled from the cache mutation (the mutation is jittered) to prevent message
 * redelivery and to avoid thundering herds on Redis.
 */
@Slf4j
@RequiredArgsConstructor
public class CacheSyncListener {

  private final Cache<String, Object> caffeineCache;
  private final Function<String, Object> redisLoader;
  private final CacheSyncProperties properties;
  private final ScheduledExecutorService scheduler;
  private final CacheExpireManager expireManager;

  /**
   * RabbitMQ message callback for sync messages. Acknowledges immediately after parsing;
   * the actual cache update is executed asynchronously with random jitter.
   *
   * @param channel the AMQP channel
   * @param msg     the raw message
   */
  public void handleSyncMessage(Channel channel, Message msg) throws IOException {
    long tag = msg.getMessageProperties().getDeliveryTag();
    try {
      processSync(msg);
      channel.basicAck(tag, false);
    } catch (Exception e) {
      log.error("CacheSync processing failed: body={}", new String(msg.getBody()), e);
      channel.basicNack(tag, false, false);
    }
  }

  /**
   * Decodes the message and schedules the appropriate handler with a random delay
   * to distribute Redis reads evenly.
   */
  private void processSync(Message msg) {
    SyncMessage sm = SyncMessage.from(msg);
    if (sm == null) {
      log.debug("Received sync message with empty or invalid body");
      return;
    }

    Runnable task = () -> syncMessageRouter(sm);

    // Random jitter spreads out Redis fetches, reducing hotspot load during batch sync.
    DelayUtil.floatTimeDelay(task, properties.getWarmupJitterMs(), scheduler);
  }

  /**
   * Routes the message to the correct handler based on its type.
   */
  private void syncMessageRouter(SyncMessage msg) {
    if (msg.type() == null) {
      log.debug("Received sync with null type, skipping");
      return;
    }
    switch (msg.type()) {
      case TYPE_INVALIDATE -> handleInvalidate(msg);
      case TYPE_REFRESH -> handleRefresh(msg);
      default -> log.warn("Unknown sync type: {}, cacheKey: {}", msg.type(), msg.cacheKey());
    }
  }

  /**
   * Atomically removes the specified key from the local cache.
   */
  private void handleInvalidate(SyncMessage sm) {
    caffeineCache.asMap().compute(sm.cacheKey(), (_, _) -> null);
    log.debug("Invalidated by sync: {}", sm.cacheKey());
  }

  /**
   * Refreshes a cache entry with the latest value from Redis.
   * <p>
   * Uses double-checked locking (DCL): a fast version guard before the Redis fetch,
   * and a second guard inside the atomic {@code compute} to prevent overwriting a
   * newer version that arrived concurrently.
   * <p>
   * The refreshed entry retains the original metadata (hard/soft TTLs, normal TTLs,
   * key state, degradation flag) except for the value and version which are taken
   * from the incoming message and Redis.
   */
  private void handleRefresh(SyncMessage sm) {
    // DCL first check – cheap, outside the compute lock
    if (VersionGuard.shouldSkipForSync(caffeineCache, sm.cacheKey(), sm.version(), sm.isVersionDegraded()) != null) {
      log.debug("Stale refresh ignored: key={}, incomingVersion={}", sm.cacheKey(), sm.version());
      return;
    }

    Object value = redisLoader.apply(sm.cacheKey());
    if (value == null) {
      log.debug("Refresh value not found in Redis: {}", sm.cacheKey());
      return;
    }

    caffeineCache
      .asMap()
      .compute(sm.cacheKey(), (key, existing) -> {
        // DCL second check – atomic with to write
        if (
          VersionGuard.shouldSkipForSync(caffeineCache, key, sm.version(), sm.isVersionDegraded()) != null ||
          !(existing instanceof CacheEntry cacheEntry)
        ) {
          return existing; // keep existing if guard rejects or if not a CacheEntry
        }

        long hardTtlMs = cacheEntry.getHardTtlMs();
        long softTtlMs = cacheEntry.getSoftTtlMs();

        // Preserve all metadata except value and version which come from the message/Redis
        return new CacheEntry(
          value,
          sm.version(),
          sm.isVersionDegraded(),
          hardTtlMs,
          expireManager.computeHardExpireAt(hardTtlMs),
          softTtlMs,
          expireManager.computeSoftExpireAt(softTtlMs),
          cacheEntry.getKeyState(),
          cacheEntry.getNormalHardTtlMs(),
          cacheEntry.getNormalSoftTtlMs()
        );
      });
    log.debug("Refreshed by sync: {}", sm.cacheKey());
  }
}
