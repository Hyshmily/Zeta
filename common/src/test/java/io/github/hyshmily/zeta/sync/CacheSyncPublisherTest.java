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
package io.github.hyshmily.zeta.sync;

import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.HEADER_APP_NAME;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.HEADER_ORIGIN_NODE_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import io.github.hyshmily.zeta.sync.local.CacheSyncProperties;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.sync.local.PutPayloadCodec;
import io.github.hyshmily.zeta.util.NodeIdentity;
import io.github.hyshmily.zeta.util.clock.StampClock;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.core.Message;
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
    publisher = new CacheSyncPublisher(rabbitTemplate, properties, new PutPayloadCodec(properties), null);
  }

  /**
   * Verifies that broadcastRefresh sends a message via RabbitTemplate.
   */
  @Test
  void broadcastRefresh_shouldSendMessage() {
    publisher.broadcastRefresh("key1", 1L);
    verify(rabbitTemplate).send(anyString(), anyString(), any());
  }

  /**
   * Verifies that broadcastInvalidate sends a message via RabbitTemplate.
   */
  @Test
  void broadcastInvalidate_shouldSendMessage() {
    publisher.broadcastInvalidate("key1", 1L);
    verify(rabbitTemplate).send(anyString(), anyString(), any());
  }

  /**
   * The origin node id lets a receiver detect that a peer shares its 10-bit
   * dataVersion identity — the configuration in which equal stamps are skipped
   * in both directions and the peers never converge.
   */
  @Test
  void broadcastRefresh_shouldStampOriginNodeIdHeader() {
    publisher.broadcastRefresh("key1", 1L);

    assertThat((Long) captureSentMessage().getMessageProperties().getHeader(HEADER_ORIGIN_NODE_ID)).isEqualTo(
      NodeIdentity.getNodeId()
    );
  }

  /**
   * Verifies that broadcastRefresh skips sending when the key is blank.
   */
  @Test
  void broadcastRefresh_shouldSkipForBlankKey() {
    publisher.broadcastRefresh("", 1L);
    verify(rabbitTemplate, never()).send(anyString(), anyString(), any());
  }

  /**
   * Verifies that broadcastInvalidate deduplicates sends so only one message is sent per key regardless of call add.
   */
  @Test
  void broadcastInvalidate_shouldDeduplicate() {
    publisher.broadcastInvalidate("key1", 5L);
    publisher.broadcastInvalidate("key1", 3L);
    publisher.broadcastInvalidate("key1", 5L);
    verify(rabbitTemplate).send(anyString(), anyString(), any());
  }

  /**
   * Verifies that broadcastRefresh with null key is skipped.
   */
  @Test
  void broadcastRefresh_withNullKey_shouldSkip() {
    publisher.broadcastRefresh(null, 1L);
    verify(rabbitTemplate, never()).send(anyString(), anyString(), any());
  }

  /**
   * Verifies that a newer version is not deduplicated by an older one.
   */
  @Test
  void broadcastInvalidate_newerVersion_shouldOverrideOld() {
    publisher.broadcastInvalidate("key1", 3L);
    publisher.broadcastInvalidate("key1", 5L);
    verify(rabbitTemplate, times(2)).send(anyString(), anyString(), any());
  }

  /**
   * One key's INVALIDATE and REFRESH draw from the same dataVersion space: a slow
   * REFRESH at an older version must be suppressed once the INVALIDATE claim exists,
   * instead of shipping a cross-type stale broadcast.
   */
  @Test
  void staleRefreshAfterInvalidate_shouldBeSuppressedByTheSameClaim() {
    publisher.broadcastInvalidate("key1", 10L);
    publisher.broadcastRefresh("key1", 9L);

    verify(rabbitTemplate, times(1)).send(anyString(), anyString(), any());
  }

  @Test
  void newerRefreshAfterInvalidate_shouldStillSend() {
    publisher.broadcastInvalidate("key1", 10L);
    publisher.broadcastRefresh("key1", 11L);

    verify(rabbitTemplate, times(2)).send(anyString(), anyString(), any());
  }

  @Test
  void equalVersionInvalidate_shouldOutrankARefreshClaim() {
    publisher.broadcastRefresh("key1", 10L);
    publisher.broadcastInvalidate("key1", 10L);

    verify(rabbitTemplate, times(2)).send(anyString(), anyString(), any());
  }

  // ── Application stamping (design decisions: sync messages pattern on the sync plane) ──

  @Test
  void broadcastRefresh_withAppName_shouldStampAppNameHeader() {
    CacheSyncPublisher stamped = stampedPublisher("appA");

    stamped.broadcastRefresh("key1", 1L);

    assertThat((String) captureSentMessage().getMessageProperties().getHeader(HEADER_APP_NAME)).isEqualTo("appA");
  }

  @Test
  void broadcastInvalidate_withAppName_shouldStampAppNameHeader() {
    CacheSyncPublisher stamped = stampedPublisher("appA");

    stamped.broadcastInvalidate("key1", 1L);

    assertThat((String) captureSentMessage().getMessageProperties().getHeader(HEADER_APP_NAME)).isEqualTo("appA");
  }

  @Test
  void withoutAppName_shouldOmitTheHeaderForBackwardCompatibility() {
    // The setUp publisher is built without an application name (null appName).
    publisher.broadcastRefresh("key1", 1L);

    assertThat((String) captureSentMessage().getMessageProperties().getHeader(HEADER_APP_NAME)).isNull();
  }

  /**
   * Verifies the O(1) oversize short-circuit in {@code serializePutPayload}: a
   * payload that provably cannot fit under put-max-bytes (byte[] length, or a
   * String whose char add lower-bounds its UTF-8 wire form) degrades to a
   * value-less REFRESH without paying the full Jackson pass. Payloads under
   * the cap still serialize.
   */
  @Test
  void serializePutPayload_oversizeSelfSizingValue_shouldShortCircuitToNull() {
    properties.setPutMaxBytes(64);
    // The PUT mapper snapshots the cap at construction, so a publisher must be
    // built after the property change (there is no re-init seam).
    CacheSyncPublisher smallCap = new CacheSyncPublisher(
      rabbitTemplate,
      properties,
      new PutPayloadCodec(properties),
      null
    );
    assertThat(smallCap.serializePutPayload(new byte[65])).isNull();
    assertThat(smallCap.serializePutPayload("a".repeat(65))).isNull();
    // Small payloads survive the full pipeline: byte[] inflates 4/3× in base64
    // and a String gains its quotes, so the under-cap samples stay well clear
    // of the exact wire-length check.
    assertThat(smallCap.serializePutPayload(new byte[10])).isNotNull();
    assertThat(smallCap.serializePutPayload("a".repeat(10))).isNotNull();
  }

  private CacheSyncPublisher stampedPublisher(String appName) {
    return new CacheSyncPublisher(rabbitTemplate, properties, new PutPayloadCodec(properties), appName);
  }

  private Message captureSentMessage() {
    ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
    verify(rabbitTemplate).send(anyString(), anyString(), captor.capture());
    return captor.getValue();
  }
}
