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
package io.github.hyshmily.zeta.sync.local;

import static io.github.hyshmily.zeta.sync.local.SyncMessage.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.rabbitmq.client.Channel;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.cache.cachesupport.ExpireManager;
import io.github.hyshmily.zeta.cache.loader.CacheLoader;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.rule.RuleMatcher;
import io.github.hyshmily.zeta.sync.dispatcher.PerKeyOrderedDispatcher;
import io.github.hyshmily.zeta.sync.worker.WorkerListener;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;

/**
 * Listens for cache synchronization messages (INVALIDATE / REFRESH / RULES_SYNC) from
 * peer application instances via the {@code zeta.sync.exchange} FanoutExchange.
 *
 * <p>This is the inbound half of the instance-to-instance cache coherence protocol.
 * The outbound half is {@link CacheSyncPublisher}. Together they ensure that a data
 * mutation on one instance is propagated to all peers.
 *
 * <p><b>Message routing:</b>
 * <ul>
 *   <li><b>INVALIDATE</b> ({@link SyncMessage#TYPE_INVALIDATE}): Atomically removes
 *       the specified key from the local Caffeine cache. Uses the 4-case
 *       {@link VersionGuard#shouldSkipForSync} guard to prevent stale degraded
 *       invalidations from wiping out a healthy entry.</li>
 *   <li><b>INVALIDATE_ALL</b> ({@link SyncMessage#TYPE_INVALIDATE_ALL}): Batch-removes
 *       all keys in the JSON-array payload via {@code caffeineCache.invalidateAllLocal()}.
 *       Bypasses version guards — these are unconditional operations.</li>
 *   <li><b>REFRESH</b> ({@link SyncMessage#TYPE_REFRESH}): Loads the latest value from
 *       Redis and updates the local cache entry. Preserves existing metadata (TTLs,
 *       key state, decision version, degradation flag) except for the value and
 *       data version. Uses double-checked locking (DCL) with {@link VersionGuard}
 *       to prevent stale overwrites.</li>
 *   <li><b>RULES_SYNC</b> ({@link SyncMessage#TYPE_RULES_SYNC}): Merges the incoming
 *       rule set into the local {@link RuleMatcher}, guarded by {@code rulesVersion}.</li>
 * </ul>
 *
 * <p><b>Thread safety:</b> All cache mutations use
 * {@link com.github.benmanes.caffeine.cache.Cache#asMap()}{@code .compute()} for
 * atomic per-key updates. The AMQP ack is sent before the cache mutation
 * (ack-before-update pattern, see ADR-0004); the mutation is scheduled with random
 * jitter to spread Redis load across instances.
 *
 * @see CacheSyncPublisher
 * @see SyncMessage
 * @see WorkerListener
 */
@RequiredArgsConstructor
@Slf4j
@Internal
public class CacheSyncListener {

  /** Shared Jackson {@link ObjectMapper} for deserializing batch-invalidation key lists from JSON. */
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /** Local Caffeine L1 cache — target of invalidation and refresh operations.
   * Accessed atomically via {@code asMap().compute()} for thread-safe updates. */
  private final Cache<String, Object> caffeineCache;

  /** Loads the current value from Redis given a cache key.
   * Used during REFRESH to fetch the authoritative value before writing to L1. */
  private final CacheLoader redisLoader;

  /** Configuration for sync exchange name, jitter settings, and consumer concurrency. */
  private final CacheSyncProperties properties;

  /** Scheduler for running jitter-delayed cache update tasks, spreading Redis load.
   * Supplied externally to allow shared-pool reuse across listeners. */
  private final ScheduledExecutorService scheduler;

  /** Computes hard and soft expiry timestamps for refreshed entries. */
  private final ExpireManager expireManager;

  /** Hot-key rule matcher whose rule set is updated when a RULES_SYNC message arrives. */
  private final RuleMatcher ruleMatcher;

  /** Per-key FIFO dispatcher for ordered cache mutation execution. */
  private PerKeyOrderedDispatcher dispatcher;

  /**
   * Tracks the highest INVALIDATE version per key, preventing stale REFRESH
   * messages (from before the INVALIDATE) from recreating the entry.
   */
  private final Cache<String, Long> recentInvalidated = Caffeine.newBuilder()
    .maximumSize(10_000)
    .expireAfterWrite(10, TimeUnit.MINUTES)
    .build();

  @PostConstruct
  public void init() {
    this.dispatcher = new PerKeyOrderedDispatcher(scheduler, "cache-sync");
  }

  @PreDestroy
  public void destroy() {
    if (dispatcher != null) {
      dispatcher.close();
    }
  }

  /**
   * RabbitMQ message callback for incoming sync messages. Acknowledges the message
   * immediately after parsing (ack-before-update), then schedules the actual cache
   * mutation asynchronously with a random jitter to spread Redis load across peers.
   *
   * <p>On success, the message is acknowledged via {@link Channel#basicAck}. On any
   * processing exception (parse failure, routing failure), the message is negatively
   * acknowledged with {@code requeue=false} to prevent poison-message loops. The next
   * application-level write will re-send the operation.
   *
   * @param channel the AMQP channel used for ack/nack operations
   * @param msg     the raw AMQP message whose body and headers carry the sync payload;
   *                must not be null
   * @throws IOException if the channel's basicAck or basicNack call fails
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
   * Decodes the raw AMQP message into a {@link SyncMessage} and schedules the
   * appropriate handler to run after a random delay within
   * {@link CacheSyncProperties#getWarmupJitterMs()}. The jitter spreads Redis
   * reads when multiple peers process the same sync send simultaneously.
   *
   * @param msg the raw AMQP message; if the body is null, empty, or cannot be
   *            parsed into a valid {@link SyncMessage}, the message is silently
   *            dropped without scheduling any task
   */
  private void processSync(Message msg) {
    SyncMessage sm = SyncMessage.from(msg);
    if (sm == null) {
      log.debug("Received sync message with empty or invalid body");
      return;
    }

    Runnable task = () -> {
      try {
        syncMessageRouter(sm);
      } catch (Exception e) {
        log.error("Async sync task failed: type={}, key={}, version={}", sm.type(), sm.cacheKey(), sm.version(), e);
      }
    };

    long jitterMs = properties.getWarmupJitterMs();
    dispatcher.submit(sm.cacheKey(), task, jitterMs);
  }

  /**
   * Routes the deserialized {@link SyncMessage} to the appropriate handler based
   * on its type field. Delegates to {@link #handleLocalInvalidate},
   * {@link #handleLocalInvalidateAll}, {@link #handleRefresh}, or
   * {@link #handleRulesSync} accordingly.
   *
   * @param msg the deserialized sync message to route; must not be null
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
   * Atomically removes the specified key from the local cache in response to
   * an INVALIDATE sync message from a peer instance.
   *
   * <p><b>Version guard logic:</b>
   * <ul>
   *   <li><b>Unconditional path:</b> When {@code version == 0L && !isVersionDegraded}
   *       (clean invalidation from {@code invalidateAllLocal}), the guard is bypassed
   *       entirely — the entry is always removed.</li>
   *   <li><b>Guarded path:</b> Uses {@link VersionGuard#shouldSkipForSync} with the
   *       4-case degraded comparison. Case 2 (existing normal, incoming degraded)
   *       prevents a stale degraded INVALIDATE from wiping a healthy entry.</li>
   * </ul>
   *
   * <p>Double-checked locking (DCL): a fast version guard before the atomic
   * {@code compute} (first pass), and a second guard inside the {@code compute}
   * body (second pass) to prevent a concurrent REFRESH from being wiped by a
   * stale invalidate that arrived after the refresh.
   *
   * @param sm the sync message containing the key to invalidate; if the key
   *           is null or invalid, the invalidation is silently skipped
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
          !unconditional &&
          existing instanceof CacheEntry ce &&
          VersionGuard.shouldSkipForSync(ce, sm.version(), sm.isVersionDegraded())
        ) {
          return existing;
        }
        return null;
      });
    log.debug("Invalidated by sync: {}", sm.cacheKey());
    recordInvalidation(sm.cacheKey(), sm.version());
  }

  /**
   * Batch-invalidates all keys contained in the JSON-array body of the sync message.
   *
   * <p>This method intentionally bypasses version guards. The publisher
   * ({@link CacheSyncPublisher#broadcastLocalInvalidateAll}) always sends clean
   * messages (version=0L, not degraded) and all keys are removed unconditionally.
   * This is more efficient than sending individual INVALIDATE messages for each key.
   *
   * <p>Deserialization failures (malformed JSON) are logged at ERROR level and
   * do not propagate.
   *
   * @param sm the sync message whose {@code cacheKey} field contains the JSON-array
   *           of keys to invalidate; must not be null
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
   * Merges the incoming rule set from a RULES_SYNC message into the local
   * {@link RuleMatcher}, guarded by the message's {@code rulesVersion}.
   * <p>
   * Delegates to {@link RuleMatcher#syncRules}, which handles the actual
   * merge logic and version conflict resolution.
   *
   * @param sm the sync message whose {@code cacheKey} field contains the
   *           serialized rule-set JSON and whose {@code rulesVersion} field
   *           carries the version for conflict resolution; must not be null
   */
  private void handleRulesSync(SyncMessage sm) {
    ruleMatcher.syncRules(sm.cacheKey(), sm.rulesVersion());
  }

  /**
   * Refreshes a cache entry with the latest value from Redis in response to a
   * REFRESH sync message from a peer instance.
   *
   * <p><b>Refresh flow:</b>
   * <ol>
   *   <li><b>DCL check 1:</b> Fast-path version guard ({@link VersionGuard#shouldSkipForSync})
   *       against the existing L1 entry. If a newer dataVersion is already present,
   *       the refresh is skipped.</li>
   *   <li><b>Redis fetch:</b> Loads the authoritative value from Redis.</li>
   *   <li><b>DCL check 2:</b> Second version guard inside the atomic {@code compute}
   *       to prevent overwriting a newer version that arrived during the Redis fetch.</li>
   *   <li><b>Write:</b> Replaces the value and dataVersion while preserving the existing
   *       entry's metadata (hard/soft TTLs, normal TTLs, key state, decision version,
   *       degradation flag). If no entry existed in L1 before the refresh, a fresh
   *       {@link CacheEntry} is created with default metadata and {@code KeyState.NORMAL}.</li>
   * </ol>
   *
   * <p>If the key is absent from L1 and Redis returns null (key does not exist),
   * the refresh is aborted — there is nothing to cache.
   *
   * @param sm the sync message containing the key and version to refresh;
   *           must not be null
   */
  private void handleRefresh(SyncMessage sm) {
    // DCL first check – cheap, outside the compute lock
    if (VersionGuard.shouldSkipForSync(caffeineCache, sm.cacheKey(), sm.version(), sm.isVersionDegraded())) {
      log.debug("Stale refresh ignored: key={}, incomingVersion={}", sm.cacheKey(), sm.version());
      return;
    }

    Object value = loadFromRedis(sm);
    if (value == null) {
      log.warn("Refresh failed to load value from Redis for key={}", sm.cacheKey());
      return;
    }

    caffeineCache
      .asMap()
      .compute(sm.cacheKey(), (key, existing) -> {
        // DCL second check – atomic with to write
        if (
          existing instanceof CacheEntry ce && VersionGuard.shouldSkipForSync(ce, sm.version(), sm.isVersionDegraded())
        ) {
          return existing;
        }
        // Atomically check invalidation record to prevent stale refresh after invalidate
        if (isInvalidation(key, sm.version())) {
          log.debug("Refresh skipped due to recent invalidation: key={}", key);
          return existing; // preserve whatever is currently in cache (may be null)
        }

        if (existing instanceof CacheEntry cacheEntry) {
          long hardExpireAt = expireManager.computeHardExpireAt(cacheEntry.getHardTtlMs());
          long softExpireAt = expireManager.computeSoftExpireAt(cacheEntry.getSoftTtlMs());
          return cacheEntry.withValueAndRefreshMeta(
            expireManager.wrapValue(value),
            sm.version(),
            sm.isVersionDegraded(),
            hardExpireAt,
            softExpireAt
          );
        }
        long defaultHardTtlMs = expireManager.getEffectiveHardTtlMs();
        long defaultSoftTtlMs = expireManager.getEffectiveSoftTtlMs();
        return expireManager.createBuilder(
          value,
          sm.version(),
          sm.isVersionDegraded(),
          0L,
          defaultHardTtlMs,
          defaultSoftTtlMs,
          defaultHardTtlMs,
          defaultSoftTtlMs,
          KeyState.NORMAL
        );
      });
    clearInvalidation(sm.cacheKey());
    log.debug("Refreshed by sync: {}", sm.cacheKey());
  }

  /**
   * Loads the current value from Redis for the key carried in the sync message.
   * <p>
   * Any exception thrown by the {@code redisLoader} (connection timeout, Redis
   * outage, serialization error) is caught and logged at WARN level. The caller
   * should handle a {@code null} return by aborting the refresh.
   *
   * @param sm the sync message containing the cache key to load; must not be null
   * @return the value from Redis, or {@code null} if the key is absent in Redis
   *         or the load failed with an exception
   */
  private Object loadFromRedis(SyncMessage sm) {
    try {
      return redisLoader.load(sm.cacheKey());
    } catch (Exception e) {
      log.warn("handleRefresh: Redis load failed for key={}", sm.cacheKey(), e);
      return null;
    }
  }

  /**
   * Record the highest INVALIDATE version for a key.
   * Used to reject stale REFRESH messages that arrive after the INVALIDATE.
   */
  private void recordInvalidation(String key, long version) {
    if (version != 0L) {
      recentInvalidated.asMap().merge(key, version, Math::max);
    }
  }

  /**
   * Check whether a refresh should be skipped because the key was
   * invalidated at a version >= the refresh version.
   */
  private boolean isInvalidation(String key, long refreshVersion) {
    Long highWater = recentInvalidated.getIfPresent(key);
    return highWater != null && refreshVersion <= highWater;
  }

  /**
   * Remove the invalidation reportToWorker after a successful refresh,
   * allowing future refreshes for this key to proceed normally.
   */
  private void clearInvalidation(String key) {
    recentInvalidated.invalidate(key);
  }
}
