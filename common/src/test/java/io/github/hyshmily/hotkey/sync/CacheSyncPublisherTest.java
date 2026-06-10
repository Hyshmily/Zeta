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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.github.hyshmily.hotkey.sync.CacheSyncProperties;
import io.github.hyshmily.hotkey.sync.CacheSyncPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * Tests for {@link CacheSyncPublisher} covering message sending, blank-key skipping, and
 * deduplication of broadcast refresh and invalidate messages.
 */
class CacheSyncPublisherTest {

  private RabbitTemplate rabbitTemplate;
  private CacheSyncProperties properties;
  private CacheSyncPublisher publisher;

  @BeforeEach
  void setUp() {
    rabbitTemplate = mock(RabbitTemplate.class);
    properties = new CacheSyncProperties();
    publisher = new CacheSyncPublisher(rabbitTemplate, properties);
    publisher.init();
  }

  /**
   * Verifies that broadcastRefresh sends a message via RabbitTemplate.
   */
  @Test
  void broadcastRefresh_shouldSendMessage() {
    publisher.broadcastRefresh("key1", 1L, false);
    verify(rabbitTemplate).send(anyString(), anyString(), any());
  }

  /**
   * Verifies that broadcastLocalInvalidate sends a message via RabbitTemplate.
   */
  @Test
  void broadcastLocalInvalidate_shouldSendMessage() {
    publisher.broadcastLocalInvalidate("key1", 1L, false);
    verify(rabbitTemplate).send(anyString(), anyString(), any());
  }

  /**
   * Verifies that broadcastRefresh skips sending when the key is blank.
   */
  @Test
  void broadcastRefresh_shouldSkipForBlankKey() {
    publisher.broadcastRefresh("", 1L, false);
    verify(rabbitTemplate, never()).send(anyString(), anyString(), any());
  }

  /**
   * Verifies that broadcastLocalInvalidate deduplicates sends so only one message is sent per key regardless of call count.
   */
  @Test
  void broadcastLocalInvalidate_shouldDeduplicate() {
    publisher.broadcastLocalInvalidate("key1", 5L, false);
    publisher.broadcastLocalInvalidate("key1", 3L, false);
    publisher.broadcastLocalInvalidate("key1", 5L, false);
    verify(rabbitTemplate).send(anyString(), anyString(), any());
  }
}
