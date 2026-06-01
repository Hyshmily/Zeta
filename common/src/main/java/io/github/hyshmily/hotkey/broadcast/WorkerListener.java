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

import static io.github.hyshmily.hotkey.broadcast.WorkerMessage.TYPE_COOL;
import static io.github.hyshmily.hotkey.broadcast.WorkerMessage.TYPE_HOT;

import com.github.benmanes.caffeine.cache.Cache;
import com.rabbitmq.client.Channel;
import io.github.hyshmily.hotkey.entity.CacheEntry;
import io.github.hyshmily.hotkey.entity.KeyState;
import io.github.hyshmily.hotkey.hotkeycache.CacheExpireManager;
import io.github.hyshmily.hotkey.hotkeycache.VersionGuard;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;

/**
 * Listens for Worker hot/cool decisions via {@code hotkey.worker.exchange} (FanoutExchange).
 * <p>
 * On HOT: loads the current value from Redis, promotes the L1 cache entry to
 * {@link KeyState#HOT} with extended hard TTL and active soft expiration.
 * On COOL: downgrades an existing entry to {@link KeyState#COOL}, disabling soft
 * expiration, resetting the hard TTL to the normal value, and retaining the
 * cached data to avoid thundering herds.
 * <p>
 * Acknowledge is sent before the cache update (the update is jittered to spread
 * Redis load), so ack and data mutation are decoupled.
 */
@Slf4j
@RequiredArgsConstructor
public class WorkerListener {

  private final Cache<String, Object> caffeineCache;
  private final Function<String, Object> redisLoader;
  private final WorkerListenerProperties properties;
  private final ScheduledExecutorService scheduler;
  private final CacheExpireManager expireManager;

  /**
   * RabbitMQ message callback.  Acknowledges the message immediately after parsing;
   * the actual cache update is executed asynchronously with a random jitter
   * (see {@link #processWorker}).
   *
   * @param channel the AMQP channel
   * @param msg     the raw message
   */
  public void handleWorkerMessage(Channel channel, Message msg) throws IOException {
    long tag = msg.getMessageProperties().getDeliveryTag();
    try {
      processWorker(msg);
      channel.basicAck(tag, false); // ack before cache write
    } catch (Exception e) {
      log.error("Worker message processing failed: body={}", new String(msg.getBody()), e);
      channel.basicNack(tag, false, false); // do not requeue
    }
  }

  /**
   * Decodes the AMQP message into a {@link WorkerMessage} and schedules the
   * appropriate handler with a random jitter to avoid thundering herds.
   */
  private void processWorker(Message msg) {
    WorkerMessage wm = WorkerMessage.from(msg);
    if (wm == null) {
      log.debug("Received worker message with empty body");
      return;
    }

    Runnable task = () -> workerMessageRouter(wm);

    // Random jitter spreads Redis reads across a small time window,
    // preventing all nodes from fetching the same key simultaneously.
    DelayUtil.floatTimeDelay(task, properties.getWarmupJitterMs(), scheduler);
  }

  /**
   * Routes the message to the correct handler based on {@link WorkerMessage#type()}.
   */
  private void workerMessageRouter(WorkerMessage msg) {
    if (msg.type() == null) {
      log.debug("Received worker message with null type, skipping");
      return;
    }
    switch (msg.type()) {
      case TYPE_HOT -> handleHot(msg);
      case TYPE_COOL -> handleCool(msg);
      default -> log.warn("Unknown worker message type: {}, cacheKey: {}", msg.type(), msg.cacheKey());
    }
  }

  /**
   * Promotes a key to {@link KeyState#HOT}.
   * <p>
   * Preserves the existing {@code dataVersion} and {@code isVersionDegraded}
   * from the cached entry (if any) and stores the Worker's decision version
   * as {@code decisionVersion}.  If no entry exists, defaults are used for
   * the data version fields.
   * <p>
   * Uses a double-checked locking (DCL) pattern: a fast-path version guard before
   * the Redis fetch, and a second guard inside the atomic {@code compute} block,
   * ensuring we never overwrite a newer decision that arrived concurrently.
   */
  private void handleHot(WorkerMessage wm) {
    // DCL first check – cheap, outside the compute lock
    if (VersionGuard.shouldSkipForWorker(caffeineCache, wm.cacheKey(), wm.decisionVersion())) {
      log.debug("handleHot: HotKey already up-to-date in L1: {}", wm.cacheKey());
      return;
    }

    Object rawValue = redisLoader.apply(wm.cacheKey());

    if (rawValue == null) {
      // Redis unavailable — promote a degraded L1 entry if one exists,
      // so the Worker's HOT decision is not permanently lost.
      Object existing = caffeineCache.getIfPresent(wm.cacheKey());
      if (existing instanceof CacheEntry ce && ce.isVersionDegraded()) {
        rawValue = ce.getValue();
        log.debug("handleHot: Promoting degraded L1 entry for key: {}", wm.cacheKey());
      }
    }
    if (rawValue == null) {
      log.debug("handleHot: HotKey value not found in Redis: {}", wm.cacheKey());
      return;
    }

    Object value = rawValue;

    caffeineCache
      .asMap()
      .compute(wm.cacheKey(), (key, existing) -> {
        // DCL second check – atomic with to write
        if (VersionGuard.shouldSkipForWorker(caffeineCache, key, wm.decisionVersion())) {
          return existing;
        }

        long dataVersion = 0;
        boolean isVersionDegraded = false;
        long normalHardTtlMs = expireManager.getEffectiveHardTtlMs();
        long normalSoftTtlMs = expireManager.getEffectiveSoftTtlMs();

        if (existing instanceof CacheEntry cacheEntry) {
          dataVersion = cacheEntry.getDataVersion();
          isVersionDegraded = cacheEntry.isVersionDegraded();
          normalHardTtlMs = cacheEntry.getNormalHardTtlMs();
          normalSoftTtlMs = cacheEntry.getNormalSoftTtlMs();
        }

        return CacheEntry.builder()
          .value(value)
          .dataVersion(dataVersion)
          .isVersionDegraded(isVersionDegraded)
          .decisionVersion(wm.decisionVersion())
          .hardTtlMs(expireManager.getEffectiveHotHardTtlMs())
          .hardExpireAtMs(expireManager.computeHotHardExpireAt())
          .softTtlMs(expireManager.getEffectiveHotSoftTtlMs())
          .softExpireAtMs(expireManager.computeHotSoftExpireAt())
          .keyState(KeyState.HOT)
          .normalHardTtlMs(normalHardTtlMs)
          .normalSoftTtlMs(normalSoftTtlMs)
          .build();
      });
    log.debug("HotKey promoted by Worker: {}", wm.cacheKey());
  }

  /**
   * Downgrades a key from {@link KeyState#HOT} to {@link KeyState#COOL}.
   * <p>
   * The existing cached value, data version, degradation flag, and decision
   * version are all preserved.  The hard TTL is reset to the normal value
   * and soft expiration is fully disabled (both softTtlMs and softExpireAtMs
   * set to 0) so that the entry stops being proactively refreshed.  The data
   * remains in the cache and will be evicted naturally by Caffeine's capacity
   * policy or a subsequent invalidation.
   */
  private void handleCool(WorkerMessage wm) {
    caffeineCache
      .asMap()
      .compute(wm.cacheKey(), (key, existing) -> {
        if (VersionGuard.shouldSkipForWorker(caffeineCache, key, wm.decisionVersion())) {
          return existing;
        }

        if (existing instanceof CacheEntry cacheEntry) {
          long normalHardTtlMs = cacheEntry.getNormalHardTtlMs();
          long normalSoftTtlMs = cacheEntry.getNormalSoftTtlMs();

          return CacheEntry.builder()
            .value(cacheEntry.getValue())
            .dataVersion(cacheEntry.getDataVersion())
            .isVersionDegraded(cacheEntry.isVersionDegraded())
            .decisionVersion(cacheEntry.getDecisionVersion())
            .hardTtlMs(cacheEntry.getNormalHardTtlMs())
            .hardExpireAtMs(expireManager.computeHardExpireAt(cacheEntry.getNormalHardTtlMs()))
            .softTtlMs(0L)
            .softExpireAtMs(0L)
            .keyState(KeyState.COOL)
            .normalHardTtlMs(normalHardTtlMs)
            .normalSoftTtlMs(normalSoftTtlMs)
            .build();
        }

        // No existing entry – nothing to cool
        return existing;
      });
    log.debug("HotKey cooled by Worker: {}", wm.cacheKey());
  }
}
