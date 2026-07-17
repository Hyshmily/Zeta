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

import static io.github.hyshmily.zeta.cache.cachesupport.CacheKeysPolicy.invalidCacheKey;
import static io.github.hyshmily.zeta.constants.ZetaConstants.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.sync.worker.WorkerListener;
import io.github.hyshmily.zeta.util.version.VersionController;
import jakarta.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * Publishes cache synchronization messages (INVALIDATE / REFRESH / RULES_SYNC)
 * to all
 * peer application instances via the {@code zeta.sync.exchange}
 * FanoutExchange.
 *
 * <p>
 * This is the outbound half of the instance-to-instance cache coherence
 * protocol.
 * The inbound half is {@link CacheSyncListener}. Together they ensure that a
 * data
 * mutation on one instance (write, evict, rule change) is propagated to all
 * other
 * instances within the same deployment.
 *
 * <p>
 * <b>Responsibility boundary:</b> This publisher handles
 * <em>application-level</em>
 * data changes only (e.g. {@code @CachePut}, {@code @CacheEvict},
 * {@code invalidateAllLocal}).
 * HOT/COOL lifecycle decisions from the Worker cluster are handled separately
 * by the
 * Worker's {@code WorkerBroadcaster} and received by {@link WorkerListener}.
 *
 * <p>
 * <b>Deduplication:</b> A {@link Caffeine} dedup cache keyed by
 * {@code "{type}:{cacheKey}"} prevents redundant broadcasts of the same
 * type+key
 * with a stale version within a configurable time window (see
 * {@link CacheSyncProperties#dedupWindowSeconds}).
 *
 * <p>
 * <b>Batch operations:</b> Large invalidations are split into chunks of at most
 * {@value #BATCH_SIZE} keys per AMQP message to stay within reasonable message
 * size limits.
 *
 * @see CacheSyncListener
 * @see SyncMessage
 */
@RequiredArgsConstructor
@Slf4j
@Internal
public class CacheSyncPublisher {

  /** RabbitMQ template for publishing messages to the sync FanoutExchange. */
  private final RabbitTemplate rabbitTemplate;

  /**
   * Configuration for sync exchange name, dedup window, and other sync settings.
   */
  private final CacheSyncProperties properties;

  /**
   * Dedup cache keyed by {@code "type:cacheKey"} → dataVersion. Prevents
   * redundant
   * broadcasts of the same type+key with a stale version within the configured
   * window.
   * Initialized in {@link #init()} via {@link PostConstruct}.
   */
  private Cache<String, Long> recentBroadcasts;

  /**
   * Shared Jackson {@link ObjectMapper} for serializing batch-invalidation key
   * lists to JSON.
   */
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Maximum number of keys per single batch-invalidation AMQP message ({@value}).
   * Batches larger than this are split into multiple messages.
   */
  private static final int BATCH_SIZE = 1000;

  /**
   * Initializes the deduplication cache after bean construction.
   * <p>
   * Creates a {@link Caffeine} cache with:
   * <ul>
   * <li>Expire-after-write of {@link CacheSyncProperties#dedupWindowSeconds}
   * seconds</li>
   * <li>Maximum size of {@link CacheSyncProperties#dedupMaxSize} entries</li>
   * </ul>
   * Called automatically by the Spring container after all dependencies are
   * injected.
   */
  @PostConstruct
  public void init() {
    this.recentBroadcasts = Caffeine.newBuilder()
      .expireAfterWrite(properties.getDedupWindowSeconds(), TimeUnit.SECONDS)
      .maximumSize(properties.getDedupMaxSize())
      .build();
  }

  /**
   * Returns the current estimated size of the deduplication cache.
   * <p>
   * This is a Caffeine {@code estimatedSizeOfKeysCount()} — it is an approximation and
   * should
   * not be relied upon for exact accounting.
   *
   * @return estimated number of entries in the dedup cache; {@code 0} if
   *         {@link #init()} has not been called yet
   */
  public long getDedupCacheSize() {
    return recentBroadcasts == null ? 0L : recentBroadcasts.estimatedSize();
  }

  /**
   * Broadcasts a REFRESH sync message to all peer instances, triggering them
   * to reload the value for the given key from Redis.
   *
   * <p>
   * Deduplicated: if a REFRESH for the same key with a higher or equal
   * {@code dataVersion} was send within the dedup window, this call is
   * silently skipped.
   *
   * @param cacheKey the affected cache key; must not be null or empty
   * @param version  the {@code dataVersion} at which the operation occurred;
   *                 see {@link VersionController#nextVersion}
   * @param degraded {@code true} if the version was obtained from the local
   *                 fallback counter (Redis unavailable)
   */
  public void broadcastRefresh(String cacheKey, long version, boolean degraded) {
    sendDeduped(cacheKey, SyncMessage.TYPE_REFRESH, version, degraded);
  }

  /**
   * Broadcasts an INVALIDATE sync message to all peer instances, requesting them
   * to remove the specified key from their local cache.
   *
   * <p>
   * Deduplicated: if an INVALIDATE for the same key with a higher or equal
   * {@code dataVersion} was send within the dedup window, this call is
   * silently skipped.
   *
   * @param cacheKey the affected cache key; must not be null or empty
   * @param version  the {@code dataVersion} at which the invalidation occurred
   * @param degraded {@code true} if the version was obtained from the local
   *                 fallback counter (Redis unavailable)
   */
  public void broadcastLocalInvalidate(String cacheKey, long version, boolean degraded) {
    sendDeduped(cacheKey, SyncMessage.TYPE_INVALIDATE, version, degraded);
  }

  /**
   * Batch-invalidates multiple keys in a single AMQP message (or multiple
   * messages
   * for very large batches).
   *
   * <p>
   * The body of each message is a JSON array of key strings. The receiver
   * ({@link CacheSyncListener}) calls {@code caffeineCache.invalidateAllLocal()}
   * once per batch, which is more efficient
   * than sending individual INVALIDATE messages for each key.
   *
   * <p>
   * Large collections are automatically split into chunks of at most
   * {@value #BATCH_SIZE} keys per AMQP message to avoid exceeding broker
   * message size limits. Serialization or send failures are logged at ERROR
   * level and do not propagate to the caller.
   *
   * <p>
   * Note: this method does <em>not</em> go through the version-based dedup
   * cache. All keys are unconditionally send.
   *
   * @param keys the keys to invalidate; if null or empty the call is a silent
   *             no-op
   */
  public void broadcastLocalInvalidateAll(Collection<String> keys) {
    if (keys == null || keys.isEmpty()) {
      return;
    }
    List<String> keyList = new ArrayList<>(keys);
    for (int i = 0; i < keyList.size(); i += BATCH_SIZE) {
      int end = Math.min(i + BATCH_SIZE, keyList.size());
      List<String> batch = keyList.subList(i, end);

      try {
        String json = OBJECT_MAPPER.writeValueAsString(batch);
        MessageProperties props = new MessageProperties();
        props.setHeader(AMQP_HEADER_TYPE, SyncMessage.TYPE_INVALIDATE_ALL);
        Message message = new Message(json.getBytes(StandardCharsets.UTF_8), props);

        rabbitTemplate.send(properties.getExchangeName(), "", message);
      } catch (AmqpException | JsonProcessingException e) {
        log.error("Failed to serialize batch invalidate keys", e);
      }
    }
  }

  /**
   * Broadcasts the full rule-set JSON to all peer instances for cross-instance
   * rule synchronization.
   *
   * <p>
   * Each receiver's {@link io.github.hyshmily.zeta.rule.RuleMatcher#syncRules}
   * merges the incoming rules with the local set, guarded by the
   * {@code rulesVersion}
   * to prevent stale overwrites (newer versions win).
   *
   * <p>
   * If the payload is null or blank, the call is a silent no-op. AMQP publish
   * failures are logged at ERROR level and do not propagate.
   *
   * @param rulesJson    the serialized rule-set JSON (new format with version
   *                     wrapper);
   *                     must be a valid JSON string
   * @param rulesVersion the current rules version at the time of serialization;
   *                     receivers use this for conflict resolution
   */
  public void broadcastAllLocalRules(String rulesJson, long rulesVersion) {
    if (rulesJson == null || rulesJson.isBlank()) {
      return;
    }
    try {
      MessageProperties props = new MessageProperties();
      props.setHeader(AMQP_HEADER_TYPE, SyncMessage.TYPE_RULES_SYNC);
      props.setHeader(AMQP_HEADER_RULES_VERSION, rulesVersion);
      Message message = new Message(rulesJson.getBytes(StandardCharsets.UTF_8), props);

      rabbitTemplate.send(properties.getExchangeName(), "", message);
    } catch (AmqpException e) {
      log.error("Failed to send rules sync message", e);
    }
  }

  /**
   * Core deduplicated send implementation: only publishes if no recent send
   * of the same {@code type:cacheKey} composite with a greater or equal
   * {@code dataVersion} exists in the dedup window.
   *
   * <p>
   * <b>Dedup algorithm:</b>
   * <ol>
   * <li>Check the dedup cache for an existing entry with a version &ge; the
   * incoming version. If found, the send is skipped.</li>
   * <li>Otherwise, the message is published <em>outside</em> the cache lock
   * and the dedup cache is updated to the new version <em>only on
   * success</em>. A publish failure does not advance the dedup state, so a
   * retry within the same window is still possible.</li>
   * </ol>
   *
   * <p>
   * Concurrent callers with the same key may briefly race past the check and
   * produce duplicate broadcasts. This is acceptable: the system uses
   * at-most-once delivery and self-healing via periodic cycles.
   *
   * <p>
   * The dedup cache entry expires after
   * {@link CacheSyncProperties#getDedupWindowSeconds}
   * seconds, after which a subsequent call with any version will be accepted.
   *
   * @param cacheKey the affected cache key; must not be null or empty
   * @param type     the sync message type (e.g.
   *                 {@link SyncMessage#TYPE_INVALIDATE},
   *                 {@link SyncMessage#TYPE_REFRESH})
   * @param version  the {@code dataVersion} of the operation
   * @param degraded whether the version was obtained in degraded mode; passed
   *                 through
   *                 to the message headers; also prefixes the dedup compositeKey
   *                 with "D:"
   *                 to prevent degraded broadcasts from being blocked by normal
   *                 ones
   */
  private void sendDeduped(String cacheKey, String type, long version, boolean degraded) {
    if (invalidCacheKey(cacheKey) || invalidCacheKey(type)) {
      log.debug("Invalid cacheKey or type, skip sync: cacheKey={}, type={}", cacheKey, type);
      return;
    }
    String compositeKey = degraded ? "D:" + type + ":" + cacheKey : type + ":" + cacheKey;

    Long current = recentBroadcasts.getIfPresent(compositeKey);
    if (current != null && current >= version) {
      log.debug(
        "Skip sync due to recent send with same or newer version: compositeKey={}, oldVersion={}, newVersion={}",
        compositeKey,
        current,
        version
      );
      return;
    }

    if (doSend(cacheKey, type, version, degraded)) {
      recentBroadcasts.put(compositeKey, version);
    }
  }

  private boolean doSend(String cacheKey, String type, long version, boolean degraded) {
    try {
      MessageProperties props = new MessageProperties();

      props.setHeader(AMQP_HEADER_TYPE, type);
      props.setHeader(AMQP_HEADER_VERSION, version);
      props.setHeader(AMQP_HEADER_IS_VERSION_DEGRADED, degraded);

      Message message = new Message(cacheKey.getBytes(StandardCharsets.UTF_8), props);

      rabbitTemplate.send(properties.getExchangeName(), "", message);
      return true;
    } catch (AmqpException e) {
      log.error("Failed to send sync message", e);
      return false;
    }
  }
}
