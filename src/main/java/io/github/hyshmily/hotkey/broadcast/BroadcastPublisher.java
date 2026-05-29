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
import static io.github.hyshmily.hotkey.hotkeycache.CacheKeysPolicy.invalidCacheKey;
import static io.github.hyshmily.hotkey.hotkeycache.CacheKeysPolicy.invalidTypeKey;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import jakarta.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

@Slf4j
@RequiredArgsConstructor
public class BroadcastPublisher {

  private static final long VERSIONED_HOT_KEY_DEFAULT_VERSION = 0L;

  private final RabbitTemplate rabbitTemplate;
  private final BroadcastProperties properties;
  private Cache<String, Long> recentBroadcasts;

  @PostConstruct
  public void init() {
    this.recentBroadcasts = Caffeine.newBuilder()
      .expireAfterWrite(properties.getDedupWindowSeconds(), TimeUnit.SECONDS)
      .maximumSize(properties.getDedupMaxSize())
      .build();
  }

  public void broadcastHotKey(String cacheKey) {
    sendDeduped(cacheKey, TYPE_HOT, VERSIONED_HOT_KEY_DEFAULT_VERSION);
  }

  public void invalidateHotKey(String cacheKey) {
    sendDeduped(cacheKey, TYPE_INVALIDATE, VERSIONED_HOT_KEY_DEFAULT_VERSION);
  }

  public void broadcastHotKeyWithVersion(String cacheKey, long version) {
    sendDeduped(cacheKey, TYPE_HOT, version, false);
  }

  public void broadcastHotKeyWithVersion(String cacheKey, long version, boolean degraded) {
    sendDeduped(cacheKey, TYPE_HOT, version, degraded);
  }

  private void sendDeduped(String cacheKey, String type, long version) {
    sendDeduped(cacheKey, type, version, false);
  }

  private void sendDeduped(String cacheKey, String type, long version, boolean degraded) {
    if (invalidCacheKey(cacheKey) || invalidTypeKey(type)) {
      log.debug("Invalid cacheKey or type, skip broadcast: cacheKey={}, type={}", cacheKey, type);
      return;
    }
    String compositeKey = type + ":" + cacheKey;

    boolean shouldSend =
      recentBroadcasts
        .asMap()
        .compute(compositeKey, (_, oldVersion) -> {
          if (oldVersion != null && oldVersion >= version) {
            log.debug(
              "Skip broadcast due to recent broadcast with same or newer version: compositeKey={}, oldVersion={}, newVersion={}",
              compositeKey,
              oldVersion,
              version
            );
            return oldVersion;
          }
          return version;
        }) ==
      version;

    if (shouldSend) {
      Message message = buildVersionedMessage(cacheKey, type, version, degraded);
      rabbitTemplate.send(properties.getExchangeName(), "", message);
    }
  }

  private static Message buildVersionedMessage(String cacheKey, String type, long version, boolean degraded) {
    MessageProperties props = new MessageProperties();
    props.setHeader("type", type);
    props.setHeader("version", version);
    props.setHeader("isVersionDegraded", degraded);
    return new Message(cacheKey.getBytes(StandardCharsets.UTF_8), props);
  }
}
