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

import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.model.DecisionKind;
import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.worker.dispatch.connection.WorkerIdentity;
import io.github.hyshmily.zeta.worker.metrics.WorkerDetectionMetrics;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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
      30_000L,
      65_536L
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
    assertThat(sent.getMessageProperties().getHeaders().get(HEADER_TYPE)).isEqualTo(DecisionKind.HOT.code());
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
    assertThat(sent.getMessageProperties().getHeaders().get(HEADER_TYPE)).isEqualTo(DecisionKind.COOL.code());
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
    assertThat(sent.getMessageProperties().getHeaders().get(HEADER_TYPE)).isEqualTo(DecisionKind.HOT.code());
  }

  /**
   * A decision whose version is already the watermark is a replay and never reaches the wire,
   * whatever its type. An ELIDED decision does not change the recorded wire state: the later COOL
   * at a newer version is a type change and must be sent.
   */
  @Test
  void broadcast_versionAlreadyAccepted_shouldBeDroppedRegardlessOfType() {
    assertThat(broadcaster.send(ZetaDecision.hot("k", 7L), Long.MAX_VALUE)).isEqualTo(WorkerBroadcaster.Delivery.SENT);

    assertThat(broadcaster.send(ZetaDecision.cool("k", 7L), Long.MAX_VALUE))
      .as("the same version is already the state this Worker holds")
      .isEqualTo(WorkerBroadcaster.Delivery.ELIDED);

    assertThat(broadcaster.send(ZetaDecision.cool("k", 8L), Long.MAX_VALUE))
      .as("the elided COOL did not turn the recorded wire state COOL")
      .isEqualTo(WorkerBroadcaster.Delivery.SENT);

    verify(rabbitTemplate, times(2)).send(any(), any(), messageCaptor.capture());
    assertThat(messageCaptor.getAllValues().get(0).getMessageProperties().getHeaders().get(HEADER_TYPE)).isEqualTo(
      DecisionKind.HOT.code()
    );
    assertThat(messageCaptor.getAllValues().get(1).getMessageProperties().getHeaders().get(HEADER_TYPE)).isEqualTo(
      DecisionKind.COOL.code()
    );
  }

  /**
   * A second COOL inside the 100 ms same-type window is a duplicate of the send that just happened;
   * its version advances the watermark and it does not reach the wire.
   */
  @Test
  void broadcast_coolInsideTheDebounceWindowIsElided() {
    assertThat(broadcaster.send(ZetaDecision.cool("k", 20L), Long.MAX_VALUE)).isEqualTo(
      WorkerBroadcaster.Delivery.SENT
    );

    assertThat(broadcaster.send(ZetaDecision.cool("k", 21L), Long.MAX_VALUE))
      .as("the same type was sent inside the 100 ms window")
      .isEqualTo(WorkerBroadcaster.Delivery.ELIDED);

    assertThat(broadcaster.acceptedVersion("k")).as("the elided COOL advances only the version").isEqualTo(21L);

    assertThat(broadcaster.send(ZetaDecision.hot("k", 22L), Long.MAX_VALUE)).isEqualTo(WorkerBroadcaster.Delivery.SENT);
    verify(rabbitTemplate, times(2)).send(any(), any(), messageCaptor.capture());
    assertThat(messageCaptor.getAllValues()).hasSize(2);
    assertThat(messageCaptor.getAllValues().get(0).getMessageProperties().getHeaders().get(HEADER_TYPE)).isEqualTo(
      DecisionKind.COOL.code()
    );
    assertThat(messageCaptor.getAllValues().get(1).getMessageProperties().getHeaders().get(HEADER_TYPE)).isEqualTo(
      DecisionKind.HOT.code()
    );
  }

  /**
   * The state machine classifies before the age bound: a decision already past its expiry instant
   * but inside the same-type debounce is elided, not expired. That order keeps the expired meter
   * free of no-op decisions.
   */
  @Test
  void aDebouncedDecisionPastItsAgeBoundIsElidedNotExpired() {
    assertThat(broadcaster.send(ZetaDecision.cool("k", 40L), Long.MAX_VALUE)).isEqualTo(
      WorkerBroadcaster.Delivery.SENT
    );

    assertThat(broadcaster.send(ZetaDecision.cool("k", 41L), 0L))
      .as("the same-type debounce wins over the already-passed age bound")
      .isEqualTo(WorkerBroadcaster.Delivery.ELIDED);
  }

  /** A COOL after a COOL outside the debounce window is a new lifecycle and must travel. */
  @Test
  void aCoolAfterACoolOutsideTheDebounceWindowIsSent() throws InterruptedException {
    assertThat(broadcaster.send(ZetaDecision.cool("k", 30L), Long.MAX_VALUE)).isEqualTo(
      WorkerBroadcaster.Delivery.SENT
    );
    Thread.sleep(150L);
    assertThat(broadcaster.send(ZetaDecision.cool("k", 31L), Long.MAX_VALUE))
      .as("the App may have promoted locally after the previous COOL")
      .isEqualTo(WorkerBroadcaster.Delivery.SENT);

    verify(rabbitTemplate, times(2)).send(any(), any(), any(Message.class));
  }

  /**
   * A failed send keeps the last confirmed version as the ordering watermark, but clears the
   * debounce anchor: a stale replay stays suppressed, while a newer same-type decision travels
   * immediately instead of inheriting a send the broker may never have received.
   */
  @Test
  void aFailedHotKeepsTheConfirmedWatermarkAndClearsTheDebounceAnchor() throws InterruptedException {
    AtomicInteger sends = new AtomicInteger();
    doAnswer(invocation -> {
      if (sends.incrementAndGet() == 2) {
        throw new RuntimeException("broker down");
      }
      return null;
    })
      .when(rabbitTemplate)
      .send(any(String.class), any(String.class), any(Message.class));

    assertThat(broadcaster.send(ZetaDecision.hot("k", 1L), Long.MAX_VALUE)).isEqualTo(WorkerBroadcaster.Delivery.SENT);
    Thread.sleep(150L);
    assertThat(broadcaster.send(ZetaDecision.hot("k", 2L), Long.MAX_VALUE)).isEqualTo(
      WorkerBroadcaster.Delivery.FAILED
    );
    assertThat(broadcaster.acceptedVersion("k")).as("the last confirmed version survives the failure").isEqualTo(1L);

    assertThat(broadcaster.send(ZetaDecision.hot("k", 1L), Long.MAX_VALUE))
      .as("a replay at the confirmed watermark is still elided")
      .isEqualTo(WorkerBroadcaster.Delivery.ELIDED);

    assertThat(broadcaster.send(ZetaDecision.hot("k", 3L), Long.MAX_VALUE))
      .as("the failed send must not anchor the same-type debounce")
      .isEqualTo(WorkerBroadcaster.Delivery.SENT);

    assertThat(broadcaster.send(ZetaDecision.cool("k", 4L), Long.MAX_VALUE)).isEqualTo(WorkerBroadcaster.Delivery.SENT);
    verify(rabbitTemplate, times(4)).send(any(), any(), any(Message.class));
  }

  /**
   * A failed COOL after a confirmed HOT must not erase the HOT watermark: an older queued COOL
   * below it would otherwise travel after the newer transition and regress the cluster.
   */
  @Test
  void aFailedCoolKeepsTheHotWatermarkAndSuppressesAStaleCool() {
    AtomicInteger sends = new AtomicInteger();
    doAnswer(invocation -> {
      if (sends.incrementAndGet() == 2) {
        throw new RuntimeException("broker down");
      }
      return null;
    })
      .when(rabbitTemplate)
      .send(any(String.class), any(String.class), any(Message.class));

    assertThat(broadcaster.send(ZetaDecision.hot("k", 10L), Long.MAX_VALUE)).isEqualTo(WorkerBroadcaster.Delivery.SENT);
    assertThat(broadcaster.send(ZetaDecision.cool("k", 11L), Long.MAX_VALUE)).isEqualTo(
      WorkerBroadcaster.Delivery.FAILED
    );

    assertThat(broadcaster.acceptedVersion("k")).as("the failed COOL does not erase version 10").isEqualTo(10L);
    assertThat(broadcaster.send(ZetaDecision.cool("k", 9L), Long.MAX_VALUE))
      .as("the older COOL is below the surviving watermark")
      .isEqualTo(WorkerBroadcaster.Delivery.SUPERSEDED);

    verify(rabbitTemplate, times(2)).send(any(), any(), any(Message.class));
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
      1L,
      65_536L
    );

    assertThat(broadcaster.send(ZetaDecision.hot("k", 1L), Long.MAX_VALUE)).isEqualTo(WorkerBroadcaster.Delivery.SENT);
    Thread.sleep(20L);
    assertThat(broadcaster.send(ZetaDecision.cool("k", 2L), Long.MAX_VALUE))
      .as("the cluster still holds the HOT, so the demotion must travel")
      .isEqualTo(WorkerBroadcaster.Delivery.SENT);

    verify(rabbitTemplate, times(2)).send(any(), any(), any(Message.class));
  }

  /**
   * The cool tier's cached lower bound must survive the removal of the old head: the entry that
   * becomes the head still expires on its own write instant, not the instant the bound remembered.
   */
  @Test
  void aCoolEntryBehindARemovedHeadStillExpiresOnTime() {
    broadcaster = new WorkerBroadcaster(
      rabbitTemplate,
      BROADCAST,
      "testApp",
      new WorkerIdentity("test-node", EPOCH),
      new WorkerDetectionMetrics(null),
      500L,
      65_536L
    );
    long base = TimeSource.monotonicMillis();
    TimeSource.setTimeOffsetForTest(0L, -base);
    try {
      assertThat(broadcaster.send(ZetaDecision.cool("oldest", 1L), Long.MAX_VALUE)).isEqualTo(
        WorkerBroadcaster.Delivery.SENT
      );
      assertThat(broadcaster.send(ZetaDecision.cool("second", 2L), Long.MAX_VALUE)).isEqualTo(
        WorkerBroadcaster.Delivery.SENT
      );

      TimeSource.setTimeOffsetForTest(0L, -base + 250L);
      assertThat(broadcaster.send(ZetaDecision.cool("oldest", 3L), Long.MAX_VALUE)).isEqualTo(
        WorkerBroadcaster.Delivery.SENT
      );

      TimeSource.setTimeOffsetForTest(0L, -base + 1_100L);
      assertThat(broadcaster.acceptedVersion("second"))
        .as("the new head expired on its own instant, not the removed head's bound")
        .isZero();
      assertThat(broadcaster.acceptedVersion("oldest")).isEqualTo(3L);
    } finally {
      TimeSource.setTimeOffsetForTest(0L, 0L);
    }
  }

  /**
   * A COOL that expires before it can be sent must not leave its key's HOT record with the long
   * retention: the Worker no longer holds the key hot, so the record keeps only the queue-residency
   * bound and expires with it.
   */
  @Test
  void anExpiredCoolGivesUpTheHotRecordsLongRetention() throws InterruptedException {
    broadcaster = new WorkerBroadcaster(
      rabbitTemplate,
      BROADCAST,
      "testApp",
      new WorkerIdentity("test-node", EPOCH),
      new WorkerDetectionMetrics(null),
      20L,
      65_536L
    );
    assertThat(broadcaster.send(ZetaDecision.hot("k", 1L), Long.MAX_VALUE)).isEqualTo(WorkerBroadcaster.Delivery.SENT);
    assertThat(broadcaster.send(ZetaDecision.cool("k", 2L), 0L)).isEqualTo(WorkerBroadcaster.Delivery.EXPIRED);

    Thread.sleep(120L);

    assertThat(broadcaster.acceptedVersion("k"))
      .as("the record expired on the twice-the-queue-age bound, not the long HOT one")
      .isZero();
  }

  /**
   * A HOT record keeps its version watermark for the whole hot episode, however long the COOL bound
   * is: a COOL arriving after it must still be ordered against it.
   */
  @Test
  void aHotRecordOutlivesTheCoolRetention() throws InterruptedException {
    broadcaster = new WorkerBroadcaster(
      rabbitTemplate,
      BROADCAST,
      "testApp",
      new WorkerIdentity("test-node", EPOCH),
      new WorkerDetectionMetrics(null),
      20L,
      65_536L
    );
    assertThat(broadcaster.send(ZetaDecision.hot("k", 1L), Long.MAX_VALUE)).isEqualTo(WorkerBroadcaster.Delivery.SENT);

    Thread.sleep(120L);

    assertThat(broadcaster.acceptedVersion("k")).as("the long HOT retention has no time bound").isEqualTo(1L);
  }

  /**
   * The record ledger is size-bounded: a COOL dropped before the funnel (queue saturation) never
   * updates its HOT record, so without the cap one-off keys would pin their records forever. The cap
   * evicts the oldest COOL record first, so the evidence that a long-lived key was told to the
   * cluster survives a flood of short-lived ones; evicting a live COOL record only costs a redundant
   * send, which the App's version guard absorbs.
   */
  @Test
  void theCapEvictsTheOldestCoolRecordBeforeAnyHotRecord() {
    broadcaster = new WorkerBroadcaster(
      rabbitTemplate,
      BROADCAST,
      "testApp",
      new WorkerIdentity("test-node", EPOCH),
      new WorkerDetectionMetrics(null),
      30_000L,
      2L
    );

    assertThat(broadcaster.send(ZetaDecision.hot("keep", 1L), Long.MAX_VALUE)).isEqualTo(
      WorkerBroadcaster.Delivery.SENT
    );
    assertThat(broadcaster.send(ZetaDecision.cool("a", 1L), Long.MAX_VALUE)).isEqualTo(WorkerBroadcaster.Delivery.SENT);
    assertThat(broadcaster.send(ZetaDecision.cool("b", 1L), Long.MAX_VALUE)).isEqualTo(WorkerBroadcaster.Delivery.SENT);

    assertThat(broadcaster.acceptedVersion("keep")).as("the HOT record is not the cap's victim").isEqualTo(1L);
    assertThat(broadcaster.send(ZetaDecision.cool("a", 1L), Long.MAX_VALUE))
      .as("the oldest COOL record was evicted, so its replay is a new send")
      .isEqualTo(WorkerBroadcaster.Delivery.SENT);
    assertThat(broadcaster.send(ZetaDecision.hot("keep", 1L), Long.MAX_VALUE))
      .as("the surviving HOT record still suppresses its own replay")
      .isEqualTo(WorkerBroadcaster.Delivery.ELIDED);
  }

  /**
   * A COOL flood cannot displace the HOT evidence: at the cap the ledger takes the oldest COOL record
   * first, so every silent long-lived HOT key keeps its watermark however many short-lived keys
   * follow it. Losing one would answer {@code acceptedVersion == 0} and let the next pre-funnel
   * rollback abort a live HOT seat on every App.
   */
  @Test
  void aCoolFloodCannotEvictASilentHotRecord() {
    int hotKeys = 100;
    int coolKeys = 50_000;
    broadcaster = new WorkerBroadcaster(
      new RabbitTemplate() {
        @Override
        public void send(String exchange, String routingKey, Message message) {
          // The ledger is what this test measures; the broker is not under test.
        }
      },
      BROADCAST,
      "testApp",
      new WorkerIdentity("test-node", EPOCH),
      new WorkerDetectionMetrics(null),
      30_000L,
      1_000L
    );

    for (int i = 0; i < hotKeys; i++) {
      assertThat(broadcaster.send(ZetaDecision.hot("hot-" + i, 1L), Long.MAX_VALUE)).isEqualTo(
        WorkerBroadcaster.Delivery.SENT
      );
    }
    for (int i = 0; i < coolKeys; i++) {
      broadcaster.send(ZetaDecision.cool("cool-" + i, 1L), Long.MAX_VALUE);
    }

    for (int i = 0; i < hotKeys; i++) {
      assertThat(broadcaster.acceptedVersion("hot-" + i)).as("hot-%d survived the flood", i).isEqualTo(1L);
    }
    assertThat(broadcaster.acceptedVersion("cool-0")).as("the oldest COOL records left instead").isZero();
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
   * so it is sent; the send establishes the version watermark that orders a late HOT.
   */
  @Test
  void aCoolWithoutAnAcceptedRecordIsSentAndEstablishesTheVersionWatermark() {
    assertThat(broadcaster.send(ZetaDecision.cool("k", 4L), Long.MAX_VALUE)).isEqualTo(WorkerBroadcaster.Delivery.SENT);
    assertThat(broadcaster.acceptedVersion("k")).isEqualTo(4L);

    assertThat(broadcaster.send(ZetaDecision.hot("k", 1L), Long.MAX_VALUE))
      .as("the stale HOT is below the COOL state the COOL established")
      .isEqualTo(WorkerBroadcaster.Delivery.SUPERSEDED);

    verify(rabbitTemplate, times(1)).send(any(), any(), any(Message.class));
  }

  /** The token-starved head's committed elision advances the same version watermark as the send path. */
  @Test
  void committedElidedCoolAdvancesTheWatermark() {
    // The 100 ms debounce starts inside send(), so everything between it and classify() must be
    // warm: the first Mockito invocation and the first AssertJ assertion can each take long enough
    // to expire the window on a cold test JVM.
    assertThat(0).isZero();
    broadcaster.send(ZetaDecision.cool("warmup", 1L), Long.MAX_VALUE);
    clearInvocations(rabbitTemplate);

    assertThat(broadcaster.send(ZetaDecision.cool("k", 4L), Long.MAX_VALUE)).isEqualTo(WorkerBroadcaster.Delivery.SENT);
    assertThat(broadcaster.acceptedVersion("k")).isEqualTo(4L);

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
