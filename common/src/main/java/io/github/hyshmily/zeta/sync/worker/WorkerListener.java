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
package io.github.hyshmily.zeta.sync.worker;

import static io.github.hyshmily.zeta.sync.worker.WorkerMessage.TYPE_COOL;
import static io.github.hyshmily.zeta.sync.worker.WorkerMessage.TYPE_HOT;

import com.github.benmanes.caffeine.cache.Cache;
import com.rabbitmq.client.Channel;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.cache.cachesupport.ExpireManager;
import io.github.hyshmily.zeta.cache.loader.CacheLoader;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.sync.dispatcher.PerKeyOrderedDispatcher;
import io.github.hyshmily.zeta.sync.local.CacheSyncListener;
import io.github.hyshmily.zeta.util.ratelimit.SreRateLimiter;
import io.github.hyshmily.zeta.util.ratelimit.impl.SreRateLimiterImpl;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;

/**
 * Listens for Worker hot/cool decisions via the {@code zeta.worker.exchange}
 * FanoutExchange and applies them to the local Caffeine L1 cache.
 *
 * <p>This is the consumer-side counterpart of the Worker's {@code WorkerBroadcaster}.
 * Incoming AMQP messages are deserialized into {@link WorkerMessage} records and
 * routed by decision type:
 * <ul>
 *   <li><b>HOT</b> ({@link WorkerMessage#TYPE_HOT}): Loads the current value from Redis,
 *       then promotes the L1 cache entry to {@link KeyState#HOT} with an extended hard TTL
 *       and active soft expiration (proactive refresh). Uses double-checked locking (DCL)
 *       with {@link VersionGuard#shouldSkipForWorker} to prevent overwriting a concurrently
 *       received newer decision.</li>
 *   <li><b>COOL</b> ({@link WorkerMessage#TYPE_COOL}): Downgrades an existing entry to
 *       {@link KeyState#COOL}, disabling soft expiration and resetting the hard TTL to
 *       the normal value. The cached data is retained to avoid thundering herds on the
 *       next read.</li>
 * </ul>
 *
 * <p><b>Ack-before-update pattern:</b> The AMQP message is acknowledged immediately after
 * parsing (see {@link #handleWorkerMessage}). The actual cache mutation is scheduled
 * asynchronously with a random jitter (see {@link WorkerListenerProperties#warmupJitterMs}).
 * This decoupling provides at-most-once delivery semantics — if the application crashes
 * after ack but before the cache write, the decision is lost and will be re-driven by the
 * next Worker heartbeat cycle (see ADR-0004).
 *
 * <p><b>Backpressure:</b> HOT promotions can be throttled by an optional
 * {@link SreRateLimiter}. When the limiter drops a request, the entry retains its current
 * state; the next Worker heartbeat or a subsequent HOT send will re-attempt the
 * promotion.
 *
 * @see WorkerMessage
 * @see WorkerHeartbeatVerifier
 * @see CacheSyncListener
 */
@RequiredArgsConstructor
@Slf4j
@Internal
public class WorkerListener {

  /** Local Caffeine L1 cache — target for HOT promotion and COOL downgrade operations.
   * Accessed atomically via {@code asMap().compute()} for thread-safe updates. */
  private final Cache<String, Object> caffeineCache;

  /** Loads the current value from Redis given a cache key.
   * Used during HOT promotion to fetch the authoritative value before writing to L1. */
  private final CacheLoader redisLoader;

  /** Configuration for Worker exchange name, queue prefix, jitter settings, and rate limiter. */
  private final WorkerListenerProperties properties;

  /** Scheduler for running jitter-delayed cache update tasks, spreading Redis load.
   * Supplied externally to allow shared-pool reuse across listeners. */
  private final ScheduledExecutorService scheduler;

  /** Computes hard and soft expiry timestamps for HOT-promoted and default-TTL entries. */
  private final ExpireManager expireManager;

  /** Optional SRE adaptive rate limiter for HOT decision processing.
   * When non-null, HOT promotions are probabilistically dropped during overload.
   * {@code null} disables rate limiting. */
  private final SreRateLimiterImpl sreRateLimiter;

  /** Per-key FIFO dispatcher for ordered cache mutation execution. */
  private PerKeyOrderedDispatcher dispatcher;

  /** Fallback hard TTL (seconds) for COOL entries when no normal TTL is configured on the existing entry. */
  private static final long COOL_DEFAULT_PROTECTION_HARDTTL_TIME = 120;
  /** Fallback soft TTL (seconds) for COOL entries when no normal TTL is configured on the existing entry. */
  private static final long COOL_DEFAULT_PROTECTION_SOFTTTL_TIME = 60;
  /** Jitter ratio for COOL fallback hard TTL (±20%). */
  private static final double COOL_DEFAULT_PROTECTION_HARDTTL_TIME_RATIO = 0.2;
  /** Jitter ratio for COOL fallback soft TTL (±20%). */
  private static final double COOL_DEFAULT_PROTECTION_SOFTTTL_TIME_RATIO = 0.2;

  @PostConstruct
  public void init() {
    this.dispatcher = new PerKeyOrderedDispatcher(scheduler, "worker-listener");
  }

  @PreDestroy
  public void destroy() {
    if (dispatcher != null) {
      dispatcher.close();
    }
  }

  /**
   * RabbitMQ message callback for incoming Worker decisions. Acknowledges the message
   * immediately after parsing (ack-before-update), then schedules the actual cache
   * mutation asynchronously with a random jitter to spread Redis load.
   *
   * <p>If processing fails with an exception, the message is negatively acknowledged
   * with {@code requeue=false} to prevent poison-message loops. The decision will be
   * re-driven by the next Worker heartbeat cycle.
   *
   * @param channel the AMQP channel used for ack/nack operations
   * @param msg     the raw AMQP message containing the Worker decision; must not be null
   * @throws IOException if the channel's basicAck or basicNack call fails
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
   * Decodes the raw AMQP message into a {@link WorkerMessage} and schedules the
   * appropriate handler to run after a random delay within
   * {@link WorkerListenerProperties#getWarmupJitterMs()}. The jitter spreads Redis
   * reads across a small time window when the Worker broadcasts to many instances.
   *
   * @param msg the raw AMQP message; may have null or empty body, in which case
   *            the message is silently dropped
   */
  private void processWorker(Message msg) {
    WorkerMessage wm = WorkerMessage.from(msg);
    if (wm == null) {
      log.debug("Received worker message with empty body");
      return;
    }

    Runnable task = () -> {
      try {
        workerMessageRouter(wm);
      } catch (Exception e) {
        log.error(
          "Error processing WorkerMessage: cacheKey={}, type={}, decisionVersion={}, nodeId={}, epoch={}",
          wm.cacheKey(),
          wm.type(),
          wm.decisionVersion(),
          wm.nodeId(),
          wm.epoch(),
          e
        );
      }
    };

    dispatcher.submit(wm.cacheKey(), () -> {
      try {
        if (properties.getWarmupJitterMs() > 0) {
          long jitter = ThreadLocalRandom.current().nextLong(properties.getWarmupJitterMs());
          TimeUnit.MILLISECONDS.sleep(jitter);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.warn("WorkerListener : Jitter sleep interrupted for key={}", wm.cacheKey());
        return;
      }
      task.run();
    });
  }

  /**
   * Routes the deserialized {@link WorkerMessage} to the appropriate handler based
   * on its type field. Delegates to {@link #handleHot} for {@code TYPE_HOT} and
   * {@link #handleCool} for {@code TYPE_COOL}. Messages with a null or unknown type
   * are logged and dropped.
   *
   * @param msg the deserialized Worker message to route; must not be null
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
   * Promotes a cache key to {@link KeyState#HOT} with extended TTL and active soft
   * expiration, following a Worker HOT decision.
   *
   * <p><b>Promotion flow:</b>
   * <ol>
   *   <li><b>SRE gate:</b> If the rate limiter drops this request, the promotion is
   *       skipped entirely — backpressure from downstream saturation.</li>
   *   <li><b>DCL check 1:</b> Fast-path version guard ({@link VersionGuard#shouldSkipForWorker})
   *       against the existing L1 entry. If a newer decision is already present, this is a no-op.</li>
   *   <li><b>Redis fetch:</b> Loads the authoritative value from Redis. If Redis is
   *       unavailable, falls back to the existing degraded L1 entry value (if any).</li>
   *   <li><b>DCL check 2:</b> Second version guard inside the atomic {@code compute} to
   *       prevent overwriting a newer decision that arrived during the Redis fetch.</li>
   *   <li><b>Write:</b> Replaces the entry with a new {@link CacheEntry} in
   *       {@code KeyState.HOT}, preserving data-version fields, setting HOT-specific
   *       TTLs, and recording the Worker's {@code decisionVersion}.</li>
   * </ol>
   *
   * <p>If no value is available from Redis <em>and</em> no degraded entry exists in L1,
   * the promotion is aborted — there is nothing to cache.
   *
   * @param wm the Worker message containing the HOT decision; must not be null
   */
  private void handleHot(WorkerMessage wm) {
    // SRE rate limiter gate — skip HOT promotion when the system is overloaded
    if (sreRateLimiter != null && !sreRateLimiter.tryAcquire()) {
      sreRateLimiter.onFailed();
      log.debug("SRE throttled HOT promotion for key={}", wm.cacheKey());
      return;
    }

    // DCL first check – cheap, outside the compute lock
    if (VersionGuard.shouldSkipForWorker(caffeineCache, wm.cacheKey(), wm.decisionVersion(), wm.nodeId(), wm.epoch())) {
      log.debug("handleHot: HotKey already up-to-date in L1: {}", wm.cacheKey());
      return;
    }

    Object value = Optional.ofNullable(loadFromRedis(wm))
      .or(() ->
        Optional.ofNullable(caffeineCache.getIfPresent(wm.cacheKey()))
          .filter(ce -> ce instanceof CacheEntry c && c.isVersionDegraded())
          .map(ce -> ((CacheEntry) ce).getValue())
      )
      .orElse(null);

    if (value == null) {
      log.debug("handleHot: HotKey value not found in Redis and no degraded entry: {}", wm.cacheKey());
      return;
    }

    caffeineCache
      .asMap()
      .compute(wm.cacheKey(), (key, existing) -> {
        long defultHotHardTtl = expireManager.getEffectiveHotHardTtlMs();
        long defultHotSoftTtl = expireManager.getEffectiveHotSoftTtlMs();

        // DCL second check – atomic with to write
        if (existing instanceof CacheEntry ce) {
          if (VersionGuard.shouldSkipForWorker(ce, wm.decisionVersion(), wm.nodeId(), wm.epoch())) {
            return existing;
          }

          return expireManager.applyTtl(
            expireManager
              .replaceEntryValue(ce, value)
              .toBuilder()
              .decisionVersion(wm.decisionVersion())
              .decisionNodeId(wm.nodeId())
              .decisionEpoch(wm.epoch())
              .keyState(KeyState.HOT)
              .build(),
            defultHotHardTtl,
            defultHotSoftTtl
          );
        }

        return expireManager.createBuilder(
          value,
          0,
          false,
          wm.decisionVersion(),
          defultHotHardTtl,
          defultHotSoftTtl,
          expireManager.getEffectiveHardTtlMs(),
          expireManager.getEffectiveSoftTtlMs(),
          KeyState.HOT
        );
      });
    log.debug("HotKey promoted by Worker: {}", wm.cacheKey());
    if (sreRateLimiter != null) {
      sreRateLimiter.onSuccess();
    }
  }

  /**
   * Downgrades a cache key from {@link KeyState#HOT} to {@link KeyState#COOL},
   * following a Worker COOL decision.
   *
   * <p>This effectively reverts the HOT promotion:
   * <ul>
   *   <li>The cached value, {@code dataVersion}, degradation flag, and
   *       {@code decisionVersion} are all preserved — the data remains available.</li>
   *   <li>The hard TTL is reset to the normal value (from
   *       {@link CacheEntry#getNormalHardTtlMs()}).</li>
   *   <li>Soft expiration is fully disabled (both {@code softTtlMs} and
   *       {@code softExpireAtMs} set to {@code 0L}), so the entry stops being
   *       proactively refreshed.</li>
   * </ul>
   *
   * <p>If no existing entry is present in L1, the COOL decision is a no-op —
   * there is nothing to downgrade. The entry will be evicted naturally by
   * Caffeine's capacity policy or a subsequent invalidation.
   *
   * @param wm the Worker message containing the COOL decision; must not be null
   */
  private void handleCool(WorkerMessage wm) {
    caffeineCache
      .asMap()
      .compute(wm.cacheKey(), (key, existing) -> {
        if (
          existing instanceof CacheEntry ce &&
          VersionGuard.shouldSkipForWorker(ce, wm.decisionVersion(), wm.nodeId(), wm.epoch())
        ) {
          return existing;
        }

        if (existing instanceof CacheEntry cacheEntry) {
          long normalHardTtlMs = cacheEntry.getNormalHardTtlMs();
          long normalSoftTtlMs = cacheEntry.getNormalSoftTtlMs();

          long hardTtlMsIfZero = normalHardTtlMs > 0 ? normalHardTtlMs : COOL_DEFAULT_PROTECTION_HARDTTL_TIME;
          long softTtlMsIfZero = normalSoftTtlMs > 0 ? normalSoftTtlMs : COOL_DEFAULT_PROTECTION_SOFTTTL_TIME;

          long hardTtlExpireAtMs = expireManager.toHardExpireTimestamp(
            hardTtlMsIfZero,
            COOL_DEFAULT_PROTECTION_HARDTTL_TIME_RATIO
          );
          long softTtlExpireAtMs = expireManager.toSoftExpireTimestamp(
            softTtlMsIfZero,
            COOL_DEFAULT_PROTECTION_SOFTTTL_TIME_RATIO
          );

          return cacheEntry
            .toBuilder()
            .value(cacheEntry.getValue())
            .dataVersion(cacheEntry.getDataVersion())
            .decisionVersion(wm.decisionVersion())
            .decisionNodeId(wm.nodeId())
            .decisionEpoch(wm.epoch())
            .hardTtlMs(hardTtlMsIfZero)
            .hardExpireAtMs(hardTtlExpireAtMs)
            .softTtlMs(softTtlMsIfZero)
            .softExpireAtMs(softTtlExpireAtMs)
            .keyState(KeyState.COOL)
            .build();
        }

        // No existing entry – nothing to cool
        return existing;
      });
    log.debug("HotKey cooled by Worker: {}", wm.cacheKey());
  }

  /**
   * Loads the current value from Redis for the key carried in the Worker message.
   * <p>
   * Any exception thrown by the {@code redisLoader} (connection timeout, Redis
   * outage, serialization error) is caught and logged at WARN level. The caller
   * is responsible for falling back to the degraded entry value when this method
   * returns {@code null}.
   *
   * @param wm the Worker message containing the cache key to load; must not be null
   * @return the value from Redis, or {@code null} if the key is absent or the
   *         load failed with an exception
   */
  private Object loadFromRedis(WorkerMessage wm) {
    try {
      return redisLoader.load(wm.cacheKey());
    } catch (Exception e) {
      log.warn("handleHot: Redis load failed for key={}, trying degraded entry", wm.cacheKey(), e);
      if (sreRateLimiter != null) {
        sreRateLimiter.onFailed();
      }
      return null;
    }
  }
}
