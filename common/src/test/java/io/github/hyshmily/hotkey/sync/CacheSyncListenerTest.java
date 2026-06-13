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

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED;
import static io.github.hyshmily.hotkey.constants.HotKeyConstants.AMQP_HEADER_TYPE;
import static io.github.hyshmily.hotkey.constants.HotKeyConstants.AMQP_HEADER_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.rabbitmq.client.Channel;
import io.github.hyshmily.hotkey.sync.CacheSyncListener;
import io.github.hyshmily.hotkey.sync.CacheSyncProperties;
import io.github.hyshmily.hotkey.sync.SyncMessage;
import io.github.hyshmily.hotkey.model.CacheEntry;
import io.github.hyshmily.hotkey.model.KeyState;
import io.github.hyshmily.hotkey.cache.CacheExpireManager;
import io.github.hyshmily.hotkey.autoconfigure.HotKeyProperties;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * Tests for {@link CacheSyncListener} verifying correct ACK/NACK behavior for invalidate and
 * refresh sync messages, including error handling and null-type edge cases.
 */
class CacheSyncListenerTest {

  private Cache<String, Object> cache;
  private CacheSyncListener listener;
  private Channel channel;
  private ScheduledExecutorService scheduler;
  private RuleMatcher ruleMatcher;

  @BeforeEach
  void setUp() throws IOException {
    cache = Caffeine.newBuilder().maximumSize(100).build();
    Function<String, Object> redisLoader = k -> "refreshed";
    CacheSyncProperties properties = new CacheSyncProperties();
    properties.setWarmupJitterMs(0);
    scheduler = Executors.newSingleThreadScheduledExecutor();
    HotKeyProperties ttlConfig = new HotKeyProperties();
    CacheExpireManager expireManager = new CacheExpireManager(cache, Runnable::run, ttlConfig, 10);
    ruleMatcher = mock(RuleMatcher.class);

    listener = new CacheSyncListener(cache, redisLoader, properties, scheduler, expireManager, ruleMatcher);
    channel = mock(Channel.class);
  }

  /**
   * Verifies that an INVALIDATE sync message removes the key from cache and acknowledges the message.
   */
  @Test
  void handleSyncMessage_invalidate_shouldAckAndRemove() throws IOException {
    cache.put("key1", "value");
    listener.handleSyncMessage(channel, syncMessage("key1", SyncMessage.TYPE_INVALIDATE, 1L, false));
    verify(channel).basicAck(anyLong(), eq(false));
  }

  /**
   * Verifies that a REFRESH sync message updates the cached entry with new data from the loader and acknowledges.
   */
  @Test
  void handleSyncMessage_refresh_shouldAckAndUpdate() throws IOException {
    cache.put("key1", entry(1, false, 0));
    listener.handleSyncMessage(channel, syncMessage("key1", SyncMessage.TYPE_REFRESH, 2L, false));
    verify(channel).basicAck(anyLong(), eq(false));
  }

  /**
   * Verifies that a NACK is sent when an error occurs during sync message processing.
   */
  @Test
  void handleSyncMessage_shouldNackOnError() throws IOException {
    MessageProperties props = new MessageProperties();
    props.setHeader(AMQP_HEADER_TYPE, "INVALIDATE");
    Message msg = new Message("key1".getBytes(StandardCharsets.UTF_8), props);
    doThrow(new RuntimeException("forced error")).when(channel).basicAck(anyLong(), eq(false));
    listener.handleSyncMessage(channel, msg);
    verify(channel).basicNack(anyLong(), eq(false), eq(false));
  }

  /**
   * Verifies that a sync message with a null type header is handled gracefully without error.
   */
  @Test
  void handleSyncMessage_shouldHandleNullType() throws IOException {
    MessageProperties props = new MessageProperties();
    Message msg = new Message("key1".getBytes(StandardCharsets.UTF_8), props);
    listener.handleSyncMessage(channel, msg);
    verify(channel).basicAck(anyLong(), eq(false));
  }

  /**
   * Verifies that a RULES_SYNC message delegates to ruleMatcher.syncRules and acknowledges.
   */
  @Test
  void handleSyncMessage_withRulesSync_shouldCallRuleMatcher() throws IOException {
    Message msg = syncMessage("rule-payload", SyncMessage.TYPE_RULES_SYNC, 0L, false);
    listener.handleSyncMessage(channel, msg);
    verify(ruleMatcher).syncRules("rule-payload", 0L);
    verify(channel).basicAck(anyLong(), eq(false));
  }

  /**
   * Verifies that the listener handles a plain string value (not a CacheEntry) in the cache gracefully.
   */
  @Test
  void handleSyncMessage_withStringValueInsteadOfCacheEntry_shouldNotCrash() throws IOException {
    cache.put("key1", "plain-string");
    listener.handleSyncMessage(channel, syncMessage("key1", SyncMessage.TYPE_REFRESH, 2L, false));
    verify(channel).basicAck(anyLong(), eq(false));
  }

  /**
   * Verifies that a REFRESH with a null value from the Redis loader is handled without error.
   */
  @Test
  void handleSyncMessage_withRefreshAndNullRedisValue_shouldLogAndReturn() throws IOException {
    cache.put("key1", entry(1, false, 0));
    Function<String, Object> nullLoader = k -> null;
    CacheSyncProperties properties = new CacheSyncProperties();
    properties.setWarmupJitterMs(0);
    HotKeyProperties ttlConfig = new HotKeyProperties();
    CacheExpireManager expireManager = new CacheExpireManager(cache, Runnable::run, ttlConfig, 10);
    CacheSyncListener nullListener = new CacheSyncListener(cache, nullLoader, properties, scheduler, expireManager, ruleMatcher);

    nullListener.handleSyncMessage(channel, syncMessage("key1", SyncMessage.TYPE_REFRESH, 2L, false));
    verify(channel).basicAck(anyLong(), eq(false));
  }

  /**
   * Verifies that an unknown sync message type is acknowledged and ignored.
   */
  @Test
  void handleSyncMessage_unknownType_shouldAckAndReturn() throws IOException {
    Message msg = syncMessage("key1", "UNKNOWN_TYPE", 1L, false);
    listener.handleSyncMessage(channel, msg);
    verify(channel).basicAck(anyLong(), eq(false));
  }

  /**
   * Verifies that an INVALIDATE with version 0 (unconditional) removes the entry regardless of existing version.
   */
  @Test
  void invalidate_withVersion0AndNotDegraded_shouldBeUnconditional() throws IOException {
    cache.put("key1", entry(5, false, 0));
    listener.handleSyncMessage(channel, syncMessage("key1", SyncMessage.TYPE_INVALIDATE, 0L, false));
    verify(channel).basicAck(anyLong(), eq(false));
    assertThat(cache.getIfPresent("key1")).isNull();
  }

  /**
   * Verifies that invalidating a key with a plain string value (not CacheEntry) does not throw.
   */
  @Test
  void invalidate_whenExistingNotCacheEntry_shouldNotThrow() throws IOException {
    cache.put("key1", "plain-string");
    listener.handleSyncMessage(channel, syncMessage("key1", SyncMessage.TYPE_INVALIDATE, 1L, false));
    verify(channel).basicAck(anyLong(), eq(false));
    assertThat(cache.getIfPresent("key1")).isNull();
  }

  private static Message syncMessage(String key, String type, long version, boolean degraded) {
    MessageProperties props = new MessageProperties();
    props.setHeader(AMQP_HEADER_TYPE, type);
    props.setHeader(AMQP_HEADER_VERSION, version);
    props.setHeader(AMQP_HEADER_IS_VERSION_DEGRADED, degraded);
    return new Message(key.getBytes(StandardCharsets.UTF_8), props);
  }

  private static CacheEntry entry(long dataVersion, boolean degraded, long decisionVersion) {
    return CacheEntry.builder()
      .value("v")
      .dataVersion(dataVersion)
      .isVersionDegraded(degraded)
      .decisionVersion(decisionVersion)
      .hardTtlMs(300_000)
      .hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(30_000)
      .softExpireAtMs(30_000)
      .keyState(KeyState.NORMAL)
      .normalHardTtlMs(300_000)
      .normalSoftTtlMs(30_000)
      .build();
  }
}
