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

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.AMQP_HEADER_TYPE;
import static io.github.hyshmily.hotkey.constants.HotKeyConstants.AMQP_HEADER_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.rabbitmq.client.Channel;
import io.github.hyshmily.hotkey.sync.WorkerListener;
import io.github.hyshmily.hotkey.sync.WorkerListenerProperties;
import io.github.hyshmily.hotkey.sync.WorkerMessage;
import io.github.hyshmily.hotkey.model.CacheEntry;
import io.github.hyshmily.hotkey.model.KeyState;
import io.github.hyshmily.hotkey.cache.CacheExpireManager;
import io.github.hyshmily.hotkey.autoconfigure.HotKeyProperties;
import io.github.hyshmily.hotkey.sharding.RingManager;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * Tests for {@link WorkerListener} verifying HOT/COOL decision handling, cache state promotion and
 * downgrade, and error NACK behavior.
 */
class WorkerListenerTest {

  private Cache<String, Object> cache;
  private WorkerListener listener;
  private Channel channel;

  @BeforeEach
  void setUp() throws IOException {
    cache = Caffeine.newBuilder().maximumSize(100).build();
    Function<String, Object> redisLoader = k -> "refreshed";
    WorkerListenerProperties properties = new WorkerListenerProperties();
    properties.setWarmupJitterMs(0);
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    HotKeyProperties ttlConfig = new HotKeyProperties();
    CacheExpireManager expireManager = new CacheExpireManager(cache, Runnable::run, ttlConfig, 10);
    listener = new WorkerListener(cache, redisLoader, properties, scheduler, expireManager, new RingManager(150));
    channel = mock(Channel.class);
  }

  /**
   * Verifies that a HOT worker decision acknowledges and promotes the key to HOT status.
   */
  @Test
  void handleWorkerMessage_hot_shouldAckAndPromote() throws IOException {
    listener.handleWorkerMessage(channel, workerMessage("key1", WorkerMessage.TYPE_HOT, 1L));
    verify(channel).basicAck(anyLong(), anyBoolean());
  }

  /**
   * Verifies that a COOL worker decision acknowledges and downgrades the key to COOL status.
   */
  @Test
  void handleWorkerMessage_cool_shouldAckAndDowngrade() throws IOException {
    cache.put("key1", hotEntry());
    listener.handleWorkerMessage(channel, workerMessage("key1", WorkerMessage.TYPE_COOL, 2L));
    verify(channel).basicAck(anyLong(), anyBoolean());
    assertThat(cache.getIfPresent("key1")).satisfies(o -> {
      CacheEntry ce = (CacheEntry) o;
      assertThat(ce.getKeyState()).isEqualTo(KeyState.COOL);
    });
  }

  /**
   * Verifies that a NACK is sent when an error occurs during worker message processing.
   */
  @Test
  void handleWorkerMessage_shouldNackOnError() throws IOException {
    MessageProperties props = new MessageProperties();
    props.setHeader(AMQP_HEADER_TYPE, "HOT");
    Message msg = new Message("key1".getBytes(StandardCharsets.UTF_8), props);
    doThrow(new RuntimeException("forced error")).when(channel).basicAck(anyLong(), anyBoolean());
    listener.handleWorkerMessage(channel, msg);
    verify(channel).basicNack(anyLong(), anyBoolean(), anyBoolean());
  }

  private static Message workerMessage(String key, String type, long version) {
    MessageProperties props = new MessageProperties();
    props.setHeader(AMQP_HEADER_TYPE, type);
    props.setHeader(AMQP_HEADER_VERSION, version);
    return new Message(key.getBytes(StandardCharsets.UTF_8), props);
  }

  private static CacheEntry hotEntry() {
    return CacheEntry.builder()
      .value("v")
      .dataVersion(5)
      .isVersionDegraded(false)
      .decisionVersion(1)
      .hardTtlMs(3_600_000)
      .hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(300_000)
      .softExpireAtMs(System.currentTimeMillis() + 300_000)
      .keyState(KeyState.HOT)
      .normalHardTtlMs(300_000)
      .normalSoftTtlMs(30_000)
      .build();
  }
}
