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

import static io.github.hyshmily.hotkey.broadcast.SyncMessage.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.rabbitmq.client.Channel;
import io.github.hyshmily.hotkey.entity.CacheEntry;
import io.github.hyshmily.hotkey.hotkeycache.CacheExpireManager;
import io.github.hyshmily.hotkey.hotkeycache.VersionGuard;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import java.io.IOException;
import java.util.List;
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
 * metadata (hard/soft TTLs, key state, versioning, decision version) except for value and
 * data version.
 * <p>
 * Ack is decoupled from the cache mutation (the mutation is jittered) to prevent message
 * redelivery and to avoid thundering herds on Redis.
 */
@Slf4j
@RequiredArgsConstructor
public class CacheSyncListener {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final Cache<String, Object> caffeineCache;
  private final Function<String, Object> redisLoader;
  private final CacheSyncProperties properties;
  private final ScheduledExecutorService scheduler;
  private final CacheExpireManager expireManager;
  private final RuleMatcher ruleMatcher;

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
      case TYPE_INVALIDATE -> handleLocalInvalidate(msg);
      case TYPE_INVALIDATE_ALL -> handleLocalInvalidateAll(msg);
      case TYPE_REFRESH -> handleRefresh(msg);
      case TYPE_RULES_SYNC -> handleRulesSync(msg);
      default -> log.warn("Unknown sync type: {}, cacheKey: {}", msg.type(), msg.cacheKey());
    }
  }

  /**
   * Atomically removes the specified key from the local cache.
   * <p>
   * Uses the same version guard as {@link #handleRefresh}: when the existing
   * entry is normal and the incoming INVALIDATE is degraded, the invalidation
   * is skipped (case 2).  An unconditional path
   * ({@code version == 0L && !isVersionDegraded}) bypasses the guard for
   * bulk invalidations from {@code invalidateAll}.
   * <p>
   * A second DCL check inside the atomic {@code compute} body prevents a
   * concurrent refresh from being wiped by a stale degraded INVALIDATE.
   */
  private void handleLocalInvalidate(SyncMessage sm) {
    boolean unconditional = sm.version() == 0L && !sm.isVersionDegraded();

    if (
      !unconditional &&
      VersionGuard.shouldSkipForSync(caffeineCache, sm.cacheKey(), sm.version(), sm.isVersionDegraded())
    ) {
      log.debug("Stale invalidate ignored: key={}, incomingVersion={}", sm.cacheKey(), sm.version());
      return;
    }

    caffeineCache
      .asMap()
      .compute(sm.cacheKey(), (key, existing) -> {
        if (
          !unconditional && VersionGuard.shouldSkipForSync(caffeineCache, key, sm.version(), sm.isVersionDegraded())
        ) {
          return existing;
        }
        return null;
      });
    log.debug("Invalidated by sync: {}", sm.cacheKey());
  }

  /**
   * Batch-invalidate all keys contained in the JSON-array body.
   * This method bypasses version guards intentionally — the publisher
   * from {@code invalidateAll} sends clean (version=0L, not degraded)
   * and all keys in the batch are invalidated unconditionally.
   */
  private void handleLocalInvalidateAll(SyncMessage sm) {
    try {
      List<String> keys = OBJECT_MAPPER.readValue(sm.cacheKey(), new TypeReference<>() {});
      caffeineCache.invalidateAll(keys);
      log.debug("Batch invalidated {} keys", keys.size());
    } catch (Exception e) {
      log.error("Failed to deserialize batch invalidate keys", e);
    }
  }

  /**
   * Replace the entire rule set with the incoming JSON payload.
   */
  private void handleRulesSync(SyncMessage sm) {
    ruleMatcher.syncRules(sm.cacheKey());
  }

  /**
   * Refreshes a cache entry with the latest value from Redis.
   * <p>
   * Uses double-checked locking (DCL): a fast version guard before the Redis fetch,
   * and a second guard inside the atomic {@code compute} to prevent overwriting a
   * newer dataVersion that arrived concurrently.
   * <p>
   * The refreshed entry retains the original metadata (hard/soft TTLs, normal TTLs,
   * key state, decision version, degradation flag) except for the value and data
   * version which are taken from the incoming message and Redis.
   */
  private void handleRefresh(SyncMessage sm) {
    // DCL first check – cheap, outside the compute lock
    if (VersionGuard.shouldSkipForSync(caffeineCache, sm.cacheKey(), sm.version(), sm.isVersionDegraded())) {
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
        if (VersionGuard.shouldSkipForSync(caffeineCache, key, sm.version(), sm.isVersionDegraded())) {
          return existing; // keep existing if guard rejects
        }
        if (!(existing instanceof CacheEntry cacheEntry)) {
          return existing;
        }

        long hardTtlMs = cacheEntry.getHardTtlMs();
        long softTtlMs = cacheEntry.getSoftTtlMs();

        return CacheEntry.builder()
          .value(value)
          .dataVersion(sm.version())
          .isVersionDegraded(sm.isVersionDegraded())
          .decisionVersion(cacheEntry.getDecisionVersion())
          .hardTtlMs(hardTtlMs)
          .hardExpireAtMs(expireManager.computeHardExpireAt(hardTtlMs))
          .softTtlMs(softTtlMs)
          .softExpireAtMs(expireManager.computeSoftExpireAt(softTtlMs))
          .keyState(cacheEntry.getKeyState())
          .normalHardTtlMs(cacheEntry.getNormalHardTtlMs())
          .normalSoftTtlMs(cacheEntry.getNormalSoftTtlMs())
          .build();
      });
    log.debug("Refreshed by sync: {}", sm.cacheKey());
  }
}
