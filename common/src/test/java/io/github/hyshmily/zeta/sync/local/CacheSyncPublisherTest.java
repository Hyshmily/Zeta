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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

class CacheSyncPublisherTest {

  private RabbitTemplate rabbitTemplate;
  private CacheSyncPublisher publisher;

  @BeforeEach
  void setUp() {
    rabbitTemplate = org.mockito.Mockito.mock(RabbitTemplate.class);
    CacheSyncProperties properties = new CacheSyncProperties();
    publisher = new CacheSyncPublisher(
      rabbitTemplate,
      properties,
      new PutPayloadCodec(properties),
      null,
      new io.github.hyshmily.zeta.util.NodeIdentity("test", 1L)
    );
  }

  @Test
  void anOversizedBatchIsChunkedInsteadOfDropped() {
    List<SyncMessage.Entry> entries = new ArrayList<>();
    for (int i = 0; i < SyncMessage.MAX_BATCH_ENTRIES * 2 + 1; i++) {
      entries.add(new SyncMessage.Entry("key-" + i, i + 1L));
    }

    publisher.broadcastInvalidateBatch(entries);

    ArgumentCaptor<Message> sent = ArgumentCaptor.forClass(Message.class);
    verify(rabbitTemplate, times(3)).send(anyString(), anyString(), sent.capture());
    int decoded = 0;
    for (Message message : sent.getAllValues()) {
      SyncMessage.ParsedBatch batch = SyncMessage.parseBatch(message);
      assertThat(batch).isNotNull();
      assertThat(batch.entries()).hasSizeLessThanOrEqualTo(SyncMessage.MAX_BATCH_ENTRIES);
      decoded += batch.entries().size();
    }
    assertThat(decoded).isEqualTo(entries.size());
  }

  @Test
  void invalidEntriesAreDroppedAtTheWireBoundary() {
    publisher.broadcastInvalidateBatch(
      List.of(
        new SyncMessage.Entry("", 1L),
        new SyncMessage.Entry("blank-version", 0L),
        new SyncMessage.Entry("kept", 2L)
      )
    );

    ArgumentCaptor<Message> sent = ArgumentCaptor.forClass(Message.class);
    verify(rabbitTemplate).send(anyString(), anyString(), sent.capture());
    SyncMessage.ParsedBatch batch = SyncMessage.parseBatch(sent.getValue());
    assertThat(batch).isNotNull();
    assertThat(batch.entries()).extracting(SyncMessage::cacheKey).containsExactly("kept");
  }

  @Test
  void aPutClaimsTheDedupSlotAndSuppressesAnEqualVersionRefresh() {
    byte[] payload = "value".getBytes(StandardCharsets.UTF_8);
    publisher.broadcastPut("key-1", 10L, payload);
    publisher.broadcastRefresh("key-1", 10L);

    verify(rabbitTemplate, times(1)).send(anyString(), anyString(), any(Message.class));
  }

  @Test
  void anEqualVersionInvalidateStillReplacesThePutClaim() {
    byte[] payload = "value".getBytes(StandardCharsets.UTF_8);
    publisher.broadcastPut("key-1", 10L, payload);
    publisher.broadcastInvalidate("key-1", 10L);

    verify(rabbitTemplate, times(2)).send(anyString(), anyString(), any(Message.class));
  }

  @Test
  void aFailedPutReleasesTheClaimForTheNextRefresh() {
    doThrow(new RuntimeException("broker down"))
      .doNothing()
      .when(rabbitTemplate)
      .send(anyString(), anyString(), any(Message.class));

    publisher.broadcastPut("key-1", 10L, "value".getBytes(StandardCharsets.UTF_8));
    publisher.broadcastRefresh("key-1", 10L);

    verify(rabbitTemplate, times(2)).send(anyString(), anyString(), any(Message.class));
  }

  @Test
  void aNonPositivePutVersionIsNotSent() {
    publisher.broadcastPut("key-1", 0L, "value".getBytes(StandardCharsets.UTF_8));

    verify(rabbitTemplate, never()).send(anyString(), anyString(), any(Message.class));
  }

  /** A non-positive single-key stamp must not occupy the dedup slot either. */
  @Test
  void aNonPositiveRefreshOrInvalidateIsNeitherSentNorClaimed() {
    publisher.broadcastRefresh("key-1", 0L);
    publisher.broadcastInvalidate("key-1", -1L);

    verify(rabbitTemplate, never()).send(anyString(), anyString(), any(Message.class));

    publisher.broadcastRefresh("key-1", 1L);

    verify(rabbitTemplate).send(anyString(), anyString(), any(Message.class));
  }
}
