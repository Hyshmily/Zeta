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
package io.github.hyshmily.hotkey.sync;

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.*;
import static io.github.hyshmily.hotkey.cache.CacheKeysPolicy.invalidCacheKey;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import jakarta.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.RequiredArgsConstructor;
import io.github.hyshmily.hotkey.logging.DefaultLogger;
import io.github.hyshmily.hotkey.logging.HotKeyLogger;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * Publishes cache synchronization messages (INVALIDATE / REFRESH) to peer instances
 * via {@code hotkey.sync.exchange} (FanoutExchange).
 * <p>
 * Does NOT handle HOT/COOL — those are Worker's responsibility via {@code WorkerBroadcaster}.
 */
@RequiredArgsConstructor
public class CacheSyncPublisher {

  private static final HotKeyLogger log = new DefaultLogger(CacheSyncPublisher.class);

  private final RabbitTemplate rabbitTemplate;
  private final CacheSyncProperties properties;
  private Cache<String, Long> recentBroadcasts;

  /**
   * Initializes the dedup cache. Called by Spring after construction.
   */
  @PostConstruct
  public void init() {
    this.recentBroadcasts = Caffeine.newBuilder()
      .expireAfterWrite(properties.getDedupWindowSeconds(), TimeUnit.SECONDS)
      .maximumSize(properties.getDedupMaxSize())
      .build();
  }

  /** Current size of the deduplication cache. */
  public long getDedupCacheSize() {
    return recentBroadcasts == null ? 0L : recentBroadcasts.estimatedSize();
  }

  /**
   * Send a REFRESH sync message to all peers,
   * deduplicating against recent broadcasts of the same type+key with a higher version.
   *
   * @param version the {@code dataVersion} at which the operation occurred
   */
  public void broadcastRefresh(String cacheKey, long version, boolean degraded) {
    sendDeduped(cacheKey, SyncMessage.TYPE_REFRESH, version, degraded);
  }

  /**
   * Send an INVALIDATE sync message to all peers,
   * deduplicating against recent broadcasts of the same type+key with a higher version.
   *
   * @param version the {@code dataVersion} at which the operation occurred
   */
  public void broadcastLocalInvalidate(String cacheKey, long version, boolean degraded) {
    sendDeduped(cacheKey, SyncMessage.TYPE_INVALIDATE, version, degraded);
  }

  /**
   * Batch-invalidate multiple keys in a single AMQP message.
   * The body is a JSON array of key strings; the receiver calls
   * {@code caffeineCache.invalidateAll()} once.
   */
  public void broadcastLocalInvalidateAll(Collection<String> keys) {
    if (keys == null || keys.isEmpty()) {
      return;
    }
    try {
      String json = new ObjectMapper().writeValueAsString(keys);
      MessageProperties props = new MessageProperties();
      props.setHeader(AMQP_HEADER_TYPE, SyncMessage.TYPE_INVALIDATE_ALL);
      Message message = new Message(json.getBytes(StandardCharsets.UTF_8), props);

      rabbitTemplate.send(properties.getExchangeName(), "", message);
    } catch (JsonProcessingException e) {
      log.error("Failed to serialize keys for batch invalidate", e);
    }
  }

  /**
   * Broadcast a full rules-JSON payload to all peers.
   * Receivers replace their entire rule set to keep cross-instance consistency.
   */
  public void broadcastAllLocalRules(String rulesJson) {
    if (rulesJson == null || rulesJson.isBlank()) {
      return;
    }
    MessageProperties props = new MessageProperties();
    props.setHeader(AMQP_HEADER_TYPE, SyncMessage.TYPE_RULES_SYNC);
    Message message = new Message(rulesJson.getBytes(StandardCharsets.UTF_8), props);
    rabbitTemplate.send(properties.getExchangeName(), "", message);
  }

  /**
   * Deduplicated send: only publishes if no recent broadcast of the same type+key with a greater {@code dataVersion} exists.
   * <p>
   * Uses a Caffeine cache keyed by {@code type:cacheKey} with the dataVersion as value.
   * Within the dedup window ({@link CacheSyncProperties#getDedupWindowSeconds}),
   * subsequent calls with a stale dataVersion are silently dropped.
   */
  private void sendDeduped(String cacheKey, String type, long version, boolean degraded) {
    if (invalidCacheKey(cacheKey) || invalidCacheKey(type)) {
      log.debug("Invalid cacheKey or type, skip sync: cacheKey={}, type={}", cacheKey, type);
      return;
    }
    String compositeKey = type + ":" + cacheKey;

    AtomicBoolean skipped = new AtomicBoolean(false);
    recentBroadcasts
      .asMap()
      .compute(compositeKey, (_, oldVersion) -> {
        if (oldVersion != null && oldVersion >= version) {
          log.debug(
            "Skip sync due to recent broadcast with same or newer version: compositeKey={}, oldVersion={}, newVersion={}",
            compositeKey,
            oldVersion,
            version
          );
          skipped.set(true);
          return oldVersion;
        }
        return version;
      });

    if (!skipped.get()) {
      MessageProperties props = new MessageProperties();

      props.setHeader(AMQP_HEADER_TYPE, type);
      props.setHeader(AMQP_HEADER_VERSION, version);
      props.setHeader(AMQP_HEADER_IS_VERSION_DEGRADED, degraded);

      Message message = new Message(cacheKey.getBytes(StandardCharsets.UTF_8), props);

      rabbitTemplate.send(properties.getExchangeName(), "", message);
    }
  }
}
