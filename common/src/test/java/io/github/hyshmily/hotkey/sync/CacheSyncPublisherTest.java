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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import io.github.hyshmily.hotkey.sync.local.CacheSyncProperties;
import io.github.hyshmily.hotkey.sync.local.CacheSyncPublisher;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * Tests for {@link CacheSyncPublisher} covering message sending, blank-key skipping, and
 * deduplication of send refresh and invalidate messages.
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

  /**
   * Verifies that broadcastLocalInvalidateAll with null keys is a no-op.
   */
  @Test
  void broadcastLocalInvalidateAll_withNullKeys_shouldBeNoOp() {
    publisher.broadcastLocalInvalidateAll(null);
    verify(rabbitTemplate, never()).send(anyString(), anyString(), any());
  }

  /**
   * Verifies that broadcastLocalInvalidateAll with empty keys is a no-op.
   */
  @Test
  void broadcastLocalInvalidateAll_withEmptyKeys_shouldBeNoOp() {
    publisher.broadcastLocalInvalidateAll(List.of());
    verify(rabbitTemplate, never()).send(anyString(), anyString(), any());
  }

  /**
   * Verifies that broadcastLocalInvalidateAll sends a message for non-empty keys.
   */
  @Test
  void broadcastLocalInvalidateAll_withKeys_shouldSendMessage() {
    publisher.broadcastLocalInvalidateAll(List.of("k1", "k2"));
    verify(rabbitTemplate).send(anyString(), anyString(), any());
  }

  /**
   * Verifies that broadcastAllLocalRules with null JSON is a no-op.
   */
  @Test
  void broadcastAllLocalRules_withNullJson_shouldBeNoOp() {
    publisher.broadcastAllLocalRules(null, 1L);
    verify(rabbitTemplate, never()).send(anyString(), anyString(), any());
  }

  /**
   * Verifies that broadcastAllLocalRules with blank JSON is a no-op.
   */
  @Test
  void broadcastAllLocalRules_withBlankJson_shouldBeNoOp() {
    publisher.broadcastAllLocalRules("   ", 1L);
    verify(rabbitTemplate, never()).send(anyString(), anyString(), any());
  }

  /**
   * Verifies that broadcastAllLocalRules sends a message for valid JSON.
   */
  @Test
  void broadcastAllLocalRules_withValidJson_shouldSendMessage() {
    publisher.broadcastAllLocalRules("{\"rules\":[]}", 1L);
    verify(rabbitTemplate).send(anyString(), anyString(), any());
  }

  /**
   * Verifies that broadcastRefresh with null key is skipped.
   */
  @Test
  void broadcastRefresh_withNullKey_shouldSkip() {
    publisher.broadcastRefresh(null, 1L, false);
    verify(rabbitTemplate, never()).send(anyString(), anyString(), any());
  }

  /**
   * Verifies that a newer version is not deduplicated by an older one.
   */
  @Test
  void broadcastLocalInvalidate_newerVersion_shouldOverrideOld() {
    publisher.broadcastLocalInvalidate("key1", 3L, false);
    publisher.broadcastLocalInvalidate("key1", 5L, false);
    verify(rabbitTemplate, times(2)).send(anyString(), anyString(), any());
  }

  /**
   * Verifies that a normal send does not dedup-block a subsequent degraded
   * send for the same key+type (different compositeKey due to "D:" prefix).
   */
  @Test
  void degradedAfterNormal_shouldNotBeDeduplicated() {
    publisher.broadcastRefresh("deg-key", 5L, false);
    publisher.broadcastRefresh("deg-key", Long.MIN_VALUE + 1, true);
    verify(rabbitTemplate, times(2)).send(anyString(), anyString(), any());
  }

  /**
   * Verifies that a degraded send does not dedup-block a subsequent normal
   * send for the same key+type (different compositeKey without "D:" prefix).
   */
  @Test
  void normalAfterDegraded_shouldNotBeDeduplicated() {
    publisher.broadcastRefresh("deg-key", Long.MIN_VALUE + 1, true);
    publisher.broadcastRefresh("deg-key", 5L, false);
    verify(rabbitTemplate, times(2)).send(anyString(), anyString(), any());
  }

  /**
   * Verifies that two degraded broadcasts for the same key+type are correctly
   * deduplicated when the second has a lower version (same "D:" compositeKey).
   */
  @Test
  void degradedDuplicate_shouldBeDeduplicated() {
    publisher.broadcastRefresh("deg-key", 3L, true);
    publisher.broadcastRefresh("deg-key", 1L, true);
    verify(rabbitTemplate, times(1)).send(anyString(), anyString(), any());
  }

  /**
   * Verifies that getDedupCacheSize returns 0 before init.
   */
  @Test
  void getDedupCacheSize_beforeInit_shouldReturnZero() {
    CacheSyncPublisher p = new CacheSyncPublisher(rabbitTemplate, properties);
    assertThat(p.getDedupCacheSize()).isZero();
  }

  /**
   * Verifies that getDedupCacheSize returns a positive value after broadcasts.
   */
  @Test
  void getDedupCacheSize_afterBroadcast_shouldReturnPositive() {
    publisher.broadcastRefresh("key1", 1L, false);
    assertThat(publisher.getDedupCacheSize()).isPositive();
  }
}
