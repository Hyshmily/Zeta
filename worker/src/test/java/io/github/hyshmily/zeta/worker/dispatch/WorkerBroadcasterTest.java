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
package io.github.hyshmily.zeta.worker.dispatch;

import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.*;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Exchange.*;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Routing.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.sync.worker.WorkerType;
import io.github.hyshmily.zeta.worker.dispatch.connection.WorkerIdentity;
import io.github.hyshmily.zeta.worker.metrics.WorkerDetectionMetrics;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * Tests for {@link WorkerBroadcaster}.
 */
@ExtendWith(MockitoExtension.class)
class WorkerBroadcasterTest {

  @Mock
  private RabbitTemplate rabbitTemplate;

  @Captor
  private ArgumentCaptor<Message> messageCaptor;

  private static final long EPOCH = 7L;

  private WorkerBroadcaster broadcaster;

  @BeforeEach
  void setUp() {
    broadcaster = new WorkerBroadcaster(
      rabbitTemplate,
      BROADCAST,
      "testApp",
      new WorkerIdentity("test-node", EPOCH),
      new WorkerDetectionMetrics(null),
      30_000L
    );
  }

  /**
   * Verifies that a HOT send sends a message with the correct routing key and AMQP headers.
   */
  @Test
  void shouldSendHotWithCorrectRoutingKeyAndHeaders() {
    broadcaster.send(ZetaDecision.hot("myKey", 1L), Long.MAX_VALUE);
    verify(rabbitTemplate).send(eq(BROADCAST), eq(PREFIX_DECISION + "testApp"), messageCaptor.capture());
    Message sent = messageCaptor.getValue();
    assertThat(new String(sent.getBody())).isEqualTo("myKey");
    assertThat(sent.getMessageProperties().getHeaders().get(HEADER_TYPE)).isEqualTo(WorkerType.HOT.code());
    assertThat(sent.getMessageProperties().getHeaders().get(HEADER_APP_NAME)).isEqualTo("testApp");
    assertThat((Long) sent.getMessageProperties().getHeaders().get(HEADER_VERSION)).isPositive();
  }

  /**
   * Verifies that a COOL send sends a message with the correct routing key and AMQP headers.
   */
  @Test
  void shouldSendCoolWithCorrectRoutingKeyAndHeaders() {
    broadcaster.send(ZetaDecision.cool("myKey", 2L), Long.MAX_VALUE);
    verify(rabbitTemplate).send(eq(BROADCAST), eq(PREFIX_DECISION + "testApp"), messageCaptor.capture());
    Message sent = messageCaptor.getValue();
    assertThat(new String(sent.getBody())).isEqualTo("myKey");
    assertThat(sent.getMessageProperties().getHeaders().get(HEADER_TYPE)).isEqualTo(WorkerType.COOL.code());
    assertThat(sent.getMessageProperties().getHeaders().get(HEADER_APP_NAME)).isEqualTo("testApp");
    assertThat((Long) sent.getMessageProperties().getHeaders().get(HEADER_VERSION)).isPositive();
  }

  /** The funnel carries the version allocated by the transition; it never mints one. */
  @Test
  void carriesTheUpstreamDecisionVersion() {
    broadcaster.send(ZetaDecision.cool("k0", 3L), Long.MAX_VALUE);
    broadcaster.send(ZetaDecision.hot("k1", 4L), Long.MAX_VALUE);
    broadcaster.send(ZetaDecision.hot("k2", 5L), Long.MAX_VALUE);
    broadcaster.send(ZetaDecision.hot("k3", 6L), Long.MAX_VALUE);

    verify(rabbitTemplate, times(4)).send(any(), eq(PREFIX_DECISION + "testApp"), messageCaptor.capture());
    List<Message> messages = messageCaptor.getAllValues();
    assertThat(messages).hasSize(4);
    for (int i = 0; i < messages.size(); i++) {
      assertThat((Long) messages.get(i).getMessageProperties().getHeaders().get(HEADER_VERSION))
        .as("message %d", i)
        .isEqualTo(3L + i);
    }
  }

  /**
   * Verifies that broadcasting with an empty key string does not fail.
   */
  @Test
  void shouldHandleEmptyKey() {
    broadcaster.send(ZetaDecision.hot("", 7L), Long.MAX_VALUE);
    verify(rabbitTemplate).send(any(), any(), messageCaptor.capture());
    assertThat(new String(messageCaptor.getValue().getBody())).isEmpty();
  }

  /**
   * Verifies that a HOT send sets the {@link ZetaConstants.Amqp#HEADER_NODE_ID}
   * header on the sent message.
   */
  @Test
  void sendBroadcast_shouldIncludeNodeIdHeader() {
    broadcaster.send(ZetaDecision.hot("key", 11L), Long.MAX_VALUE);
    verify(rabbitTemplate).send(any(), any(), messageCaptor.capture());
    assertThat(messageCaptor.getValue().getMessageProperties().getHeaders()).containsEntry(HEADER_NODE_ID, "test-node");
  }

  /** The epoch is fixed for the process, so every send carries the same value. */
  @Test
  void sendBroadcast_shouldIncludeTheWorkerEpoch() {
    broadcaster.send(ZetaDecision.hot("key", 12L), Long.MAX_VALUE);
    broadcaster.send(ZetaDecision.hot("key2", 13L), Long.MAX_VALUE);

    verify(rabbitTemplate, times(2)).send(any(), any(), messageCaptor.capture());
    assertThat(messageCaptor.getAllValues()).hasSize(2);
    assertThat(
      (Long) messageCaptor.getAllValues().get(0).getMessageProperties().getHeaders().get(HEADER_EPOCH)
    ).isEqualTo(EPOCH);
    assertThat(
      (Long) messageCaptor.getAllValues().get(1).getMessageProperties().getHeaders().get(HEADER_EPOCH)
    ).isEqualTo(EPOCH);
  }

  /**
   * Verifies that when {@link RabbitTemplate#send} throws, the error is silently
   * logged and does NOT propagate (fire-and-forget per design decisions: sync messages).
   */
  @Test
  void sendBroadcast_whenRabbitTemplateThrows_shouldSwallow() {
    doThrow(new RuntimeException("connection lost"))
      .when(rabbitTemplate)
      .send(any(String.class), any(String.class), any(Message.class));

    // Should not throw — design decisions: sync messages fire-and-forget
    assertThat(broadcaster.send(ZetaDecision.hot("key", 14L), Long.MAX_VALUE)).isEqualTo(
      WorkerBroadcaster.Delivery.FAILED
    );
  }

  /**
   * Verifies that a COOL send also swallows AMQP send failures.
   */
  @Test
  void broadcastCool_whenRabbitTemplateThrows_shouldSwallow() {
    doThrow(new RuntimeException("connection lost"))
      .when(rabbitTemplate)
      .send(any(String.class), any(String.class), any(Message.class));

    broadcaster.send(ZetaDecision.cool("key", 15L), Long.MAX_VALUE);
  }

  /**
   * A COOL is sent exactly once: no repeat chain, no scheduler interaction (design decisions:
   * sync messages).
   */
  @Test
  void broadcastCool_shouldSendExactlyOnce() {
    assertThat(broadcaster.send(ZetaDecision.cool("k", 16L), Long.MAX_VALUE)).isEqualTo(
      WorkerBroadcaster.Delivery.SENT
    );
    verify(rabbitTemplate, times(1)).send(any(), any(), any(Message.class));
  }

  /**
   * A decision older than the version the funnel already accepted describes a state every App has
   * moved past: it is dropped and reported as superseded, so the caller does not roll the promotion
   * back over a merely obsolete decision (design decisions: decision delivery).
   */
  @Test
  void broadcast_olderVersionThanAlreadySent_shouldBeDropped() {
    assertThat(broadcaster.send(ZetaDecision.hot("k", 5L), Long.MAX_VALUE)).isEqualTo(WorkerBroadcaster.Delivery.SENT);

    assertThat(broadcaster.send(ZetaDecision.cool("k", 3L), Long.MAX_VALUE))
      .as("superseded decision reports success so no rollback is applied")
      .isEqualTo(WorkerBroadcaster.Delivery.SUPERSEDED);

    verify(rabbitTemplate, times(1)).send(any(), any(), messageCaptor.capture());
    Message sent = messageCaptor.getValue();
    assertThat(sent.getMessageProperties().getHeaders().get(HEADER_VERSION)).isEqualTo(5L);
    assertThat(sent.getMessageProperties().getHeaders().get(HEADER_TYPE)).isEqualTo(WorkerType.HOT.code());
  }

  /**
   * A decision whose version is already the accepted one for the key describes a state the cluster
   * has, so it must not reach the wire — whatever its type, and even outside the 100 ms debounce
   * window (the accepted record, not the debounce, is what makes a replay a no-op).
   */
  @Test
  void broadcast_versionAlreadyAccepted_shouldBeDroppedRegardlessOfType() {
    assertThat(broadcaster.send(ZetaDecision.hot("k", 7L), Long.MAX_VALUE)).isEqualTo(WorkerBroadcaster.Delivery.SENT);

    assertThat(broadcaster.send(ZetaDecision.cool("k", 7L), Long.MAX_VALUE))
      .as("the same version is already the state this Worker holds")
      .isEqualTo(WorkerBroadcaster.Delivery.ELIDED);

    verify(rabbitTemplate, times(1)).send(any(), any(), messageCaptor.capture());
    assertThat(messageCaptor.getValue().getMessageProperties().getHeaders().get(HEADER_TYPE)).isEqualTo(
      WorkerType.HOT.code()
    );
  }

  /**
   * A COOL whose lifecycle never reached the cluster carries no state change: the last accepted
   * decision for the key is already a COOL, so the cluster is cold. It is elided, and its version
   * still becomes the accepted record — keeping the never-sent HOT from being accepted afterwards — so
   * not a token and not a network round trip are spent on the demotion.
   */
  @Test
  void broadcast_coolAfterAnAcceptedCool_shouldBeElidedWithoutSending() {
    assertThat(broadcaster.send(ZetaDecision.cool("k", 20L), Long.MAX_VALUE)).isEqualTo(
      WorkerBroadcaster.Delivery.SENT
    );

    assertThat(broadcaster.send(ZetaDecision.cool("k", 21L), Long.MAX_VALUE))
      .as("the cluster is already cold for this key")
      .isEqualTo(WorkerBroadcaster.Delivery.ELIDED);

    assertThat(broadcaster.acceptedVersion("k")).as("the elided COOL is the state this Worker holds").isEqualTo(21L);

    assertThat(broadcaster.send(ZetaDecision.hot("k", 22L), Long.MAX_VALUE)).isEqualTo(WorkerBroadcaster.Delivery.SENT);
    verify(rabbitTemplate, times(2)).send(any(), any(), messageCaptor.capture());
    assertThat(messageCaptor.getAllValues()).hasSize(2);
    assertThat(messageCaptor.getAllValues().get(0).getMessageProperties().getHeaders().get(HEADER_TYPE)).isEqualTo(
      WorkerType.COOL.code()
    );
    assertThat(messageCaptor.getAllValues().get(1).getMessageProperties().getHeaders().get(HEADER_TYPE)).isEqualTo(
      WorkerType.HOT.code()
    );
  }

  /** A HOT record outlives the queue-age bound, so a long hot episode still orders its COOL. */
  @Test
  void aHotAcceptedRecordOutlivesTheQueueAgeBound() throws InterruptedException {
    broadcaster = new WorkerBroadcaster(
      rabbitTemplate,
      BROADCAST,
      "testApp",
      new WorkerIdentity("test-node", EPOCH),
      new WorkerDetectionMetrics(null),
      1L
    );

    assertThat(broadcaster.send(ZetaDecision.hot("k", 1L), Long.MAX_VALUE)).isEqualTo(WorkerBroadcaster.Delivery.SENT);
    Thread.sleep(20L);
    assertThat(broadcaster.send(ZetaDecision.cool("k", 2L), Long.MAX_VALUE))
      .as("the cluster still holds the HOT, so the demotion must travel")
      .isEqualTo(WorkerBroadcaster.Delivery.SENT);

    verify(rabbitTemplate, times(2)).send(any(), any(), any(Message.class));
  }

  /**
   * A failed HOT clears only the cold knowledge. The accepted COOL's version stays the watermark,
   * so a stale HOT below it is still superseded instead of being re-sent after the record regressed
   * to version zero.
   */
  @Test
  void forgettingColdKeepsTheAcceptedVersionWatermark() {
    assertThat(broadcaster.send(ZetaDecision.cool("k", 5L), Long.MAX_VALUE)).isEqualTo(WorkerBroadcaster.Delivery.SENT);

    broadcaster.forgetCold("k", 6L);

    assertThat(broadcaster.acceptedVersion("k")).as("the failed HOT did not lower the watermark").isEqualTo(5L);
    assertThat(broadcaster.send(ZetaDecision.hot("k", 4L), Long.MAX_VALUE))
      .as("a stale HOT below the accepted COOL stays superseded")
      .isEqualTo(WorkerBroadcaster.Delivery.SUPERSEDED);
    assertThat(broadcaster.send(ZetaDecision.cool("k", 7L), Long.MAX_VALUE))
      .as("the cold knowledge was cleared, so the correction travels")
      .isEqualTo(WorkerBroadcaster.Delivery.SENT);

    verify(rabbitTemplate, times(2)).send(any(), any(), any(Message.class));
  }

  /**
   * A superseded HOT knows the cluster was not told hot, but it must not turn an UNKNOWN record
   * cold again: a newer failed HOT still owes a corrective COOL. The version watermark stays.
   */
  @Test
  void rememberColdDoesNotRestoreColdKnowledgeClearedByANewerHot() {
    assertThat(broadcaster.send(ZetaDecision.cool("k", 5L), Long.MAX_VALUE)).isEqualTo(WorkerBroadcaster.Delivery.SENT);

    broadcaster.forgetCold("k", 6L);
    broadcaster.rememberCold("k");

    assertThat(broadcaster.acceptedVersion("k")).isEqualTo(5L);
    assertThat(broadcaster.send(ZetaDecision.cool("k", 7L), Long.MAX_VALUE))
      .as("the failed newer HOT still needs its corrective COOL")
      .isEqualTo(WorkerBroadcaster.Delivery.SENT);
    assertThat(broadcaster.acceptedVersion("k")).isEqualTo(7L);

    verify(rabbitTemplate, times(2)).send(any(), any(), any(Message.class));
  }

  /** An existing cold record already covers the lifecycle, so rememberCold leaves it untouched. */
  @Test
  void rememberColdKeepsAnExistingColdRecordAndItsVersion() {
    assertThat(broadcaster.send(ZetaDecision.cool("k", 5L), Long.MAX_VALUE)).isEqualTo(WorkerBroadcaster.Delivery.SENT);

    broadcaster.rememberCold("k");

    assertThat(broadcaster.acceptedVersion("k")).isEqualTo(5L);
    assertThat(broadcaster.send(ZetaDecision.cool("k", 7L), Long.MAX_VALUE))
      .as("the cluster is already cold for this key")
      .isEqualTo(WorkerBroadcaster.Delivery.ELIDED);
    assertThat(broadcaster.acceptedVersion("k")).isEqualTo(7L);

    verify(rabbitTemplate, times(1)).send(any(), any(), any(Message.class));
  }

  /** A failed or expired HOT clears the cold record so its corrective COOL is still sent. */
  @Test
  void forgettingAColdTombstoneLetsTheCorrectiveCoolTravel() {
    broadcaster.rememberCold("elided");
    assertThat(broadcaster.send(ZetaDecision.cool("elided", 3L), Long.MAX_VALUE)).isEqualTo(
      WorkerBroadcaster.Delivery.ELIDED
    );

    broadcaster.rememberCold("corrected");
    broadcaster.forgetCold("corrected", 3L);
    assertThat(broadcaster.send(ZetaDecision.cool("corrected", 4L), Long.MAX_VALUE)).isEqualTo(
      WorkerBroadcaster.Delivery.SENT
    );
  }

  /** The accepted-record cache has no size cap: a maximum would evict a live ordering predecessor. */
  @Test
  void acceptedRecordsHaveNoMaximumSize() throws Exception {
    var field = WorkerBroadcaster.class.getDeclaredField("accepted");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    Cache<String, ?> cache = (Cache<String, ?>) field.get(broadcaster);

    assertThat(cache.policy().eviction()).as("no maximumSize").isEmpty();
  }

  /** The accepted-version probe answers the same question the funnel's supersede check does. */
  @Test
  void acceptedVersionTracksTheFunnelWatermark() {
    assertThat(broadcaster.acceptedVersion("k")).isZero();

    broadcaster.send(ZetaDecision.hot("k", 5L), Long.MAX_VALUE);

    assertThat(broadcaster.acceptedVersion("k")).isEqualTo(5L);
    assertThat(broadcaster.acceptedVersion("other")).isZero();
  }

  /**
   * The debounce cache keys on the decision type, so a HOT that arrives inside the 100 ms
   * window of a previous HOT is elided. It still has to mark its version as accepted:
   * otherwise an older COOL queued ahead of it — the flip-flop the dispatcher's drain can
   * reorder — passes the supersede check and demotes a key the Worker has since re-promoted.
   * The split then survives until the next promotion.
   */
  @Test
  void broadcast_elidedDuplicate_shouldStillAdvanceTheVersionWatermark() {
    assertThat(broadcaster.send(ZetaDecision.hot("flip", 10L), Long.MAX_VALUE)).isEqualTo(
      WorkerBroadcaster.Delivery.SENT
    );

    // Same type inside the debounce window: nothing is sent, but version 12 is the
    // decision this Worker now holds for the key.
    assertThat(broadcaster.send(ZetaDecision.hot("flip", 12L), Long.MAX_VALUE)).isEqualTo(
      WorkerBroadcaster.Delivery.ELIDED
    );

    // The older COOL is obsolete and must not reach the cluster.
    assertThat(broadcaster.send(ZetaDecision.cool("flip", 11L), Long.MAX_VALUE))
      .as("a COOL older than an accepted decision reports success without sending")
      .isEqualTo(WorkerBroadcaster.Delivery.SUPERSEDED);

    verify(rabbitTemplate, times(1)).send(any(), any(), messageCaptor.capture());
    assertThat(messageCaptor.getValue().getMessageProperties().getHeaders().get(HEADER_VERSION)).isEqualTo(10L);
  }

  /**
   * A COOL with no accepted record is the only demotion signal a locally promoted App can receive,
   * so it is sent; the send establishes the cold watermark that orders a late HOT.
   */
  @Test
  void aCoolWithoutAnAcceptedRecordIsSentAndEstablishesTheColdWatermark() {
    assertThat(broadcaster.send(ZetaDecision.cool("k", 4L), Long.MAX_VALUE)).isEqualTo(WorkerBroadcaster.Delivery.SENT);
    assertThat(broadcaster.acceptedVersion("k")).isEqualTo(4L);

    assertThat(broadcaster.send(ZetaDecision.hot("k", 1L), Long.MAX_VALUE))
      .as("the stale HOT is below the cold state the COOL established")
      .isEqualTo(WorkerBroadcaster.Delivery.SUPERSEDED);

    verify(rabbitTemplate, times(1)).send(any(), any(), any(Message.class));
  }

  /** The token-starved head's committed elision advances the same cold watermark as the send path. */
  @Test
  void committedColdCoolAdvancesTheWatermark() {
    assertThat(broadcaster.send(ZetaDecision.cool("k", 4L), Long.MAX_VALUE)).isEqualTo(WorkerBroadcaster.Delivery.SENT);

    ZetaDecision cool = ZetaDecision.cool("k", 5L);
    WorkerBroadcaster.Verdict verdict = broadcaster.classify(cool, Long.MAX_VALUE);
    assertThat(verdict).isNotNull();
    assertThat(verdict.outcome()).isEqualTo(WorkerBroadcaster.Delivery.ELIDED);
    assertThat(broadcaster.commit(verdict, cool)).isEqualTo(WorkerBroadcaster.Delivery.ELIDED);

    assertThat(broadcaster.acceptedVersion("k")).isEqualTo(5L);
    assertThat(broadcaster.send(ZetaDecision.hot("k", 1L), Long.MAX_VALUE)).isEqualTo(
      WorkerBroadcaster.Delivery.SUPERSEDED
    );

    verify(rabbitTemplate, times(1)).send(any(), any(), any(Message.class));
  }
}
