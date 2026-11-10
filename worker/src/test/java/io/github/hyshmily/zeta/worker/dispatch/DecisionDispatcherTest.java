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

import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.HEADER_TYPE;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.HEADER_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.worker.detection.lifecycle.HE;
import io.github.hyshmily.zeta.worker.detection.lifecycle.HotKeyRegistry;
import io.github.hyshmily.zeta.worker.dispatch.connection.WorkerIdentity;
import io.github.hyshmily.zeta.worker.metrics.WorkerDetectionMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/** Delivery routing, rate limiting and the maximum-residency bound. */
@ExtendWith(MockitoExtension.class)
class DecisionDispatcherTest {

  @Mock
  private RabbitTemplate rabbitTemplate;

  private SimpleMeterRegistry meterRegistry;
  private WorkerDetectionMetrics metrics;
  private WorkerBroadcaster broadcaster;
  private DecisionDispatcher dispatcher;

  @BeforeEach
  void setUp() {
    meterRegistry = new SimpleMeterRegistry();
    metrics = new WorkerDetectionMetrics(meterRegistry);
    broadcaster = new WorkerBroadcaster(rabbitTemplate, "ex", "app", new WorkerIdentity("node", 0L), metrics, 30_000L);
  }

  @AfterEach
  void tearDown() {
    if (dispatcher != null) {
      dispatcher.shutdown();
    }
    meterRegistry.close();
  }

  private DecisionDispatcher dispatcher(int hotRate, int coolRate, int hotBurst, int coolBurst, long maxAgeMs) {
    return dispatcher(hotRate, coolRate, hotBurst, coolBurst, maxAgeMs, DecisionDispatcher.Rollback.NOOP);
  }

  private DecisionDispatcher dispatcher(
    int hotRate,
    int coolRate,
    int hotBurst,
    int coolBurst,
    long maxAgeMs,
    DecisionDispatcher.Rollback abort
  ) {
    return new DecisionDispatcher(
      1_000,
      1_000,
      hotRate,
      coolRate,
      hotBurst,
      coolBurst,
      maxAgeMs,
      broadcaster,
      metrics,
      abort,
      DecisionDispatcher.Probe.ALWAYS
    );
  }

  @Test
  void rejectsNonPositiveLaneConfigurationBeforeStartingTheDrainThread() {
    assertThatThrownBy(() ->
      new DecisionDispatcher(
        0,
        1_000,
        1_000,
        1_000,
        8,
        32,
        30_000,
        broadcaster,
        metrics,
        DecisionDispatcher.Rollback.NOOP,
        DecisionDispatcher.Probe.ALWAYS
      )
    )
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("capacity");
    assertThatThrownBy(() ->
      new DecisionDispatcher(
        1_000,
        1_000,
        0,
        1_000,
        8,
        32,
        30_000,
        broadcaster,
        metrics,
        DecisionDispatcher.Rollback.NOOP,
        DecisionDispatcher.Probe.ALWAYS
      )
    )
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("rate");
    assertThatThrownBy(() ->
      new DecisionDispatcher(
        1_000,
        1_000,
        1_000,
        1_000,
        0,
        32,
        30_000,
        broadcaster,
        metrics,
        DecisionDispatcher.Rollback.NOOP,
        DecisionDispatcher.Probe.ALWAYS
      )
    )
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("burst");
    assertThatThrownBy(() ->
      new DecisionDispatcher(
        1_000,
        1_000,
        1_000,
        1_000,
        8,
        32,
        0L,
        broadcaster,
        metrics,
        DecisionDispatcher.Rollback.NOOP,
        DecisionDispatcher.Probe.ALWAYS
      )
    )
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("maxAgeMs");
  }

  @Test
  void routesBothDecisionTypesToTheFunnel() {
    dispatcher = dispatcher(1_000, 1_000, 8, 32, 30_000);

    assertThat(dispatcher.submit(ZetaDecision.hot("h", 1L))).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.cool("c", 2L))).isTrue();

    await(
      () ->
        meterRegistry.get(WorkerDetectionMetrics.METRIC_DECISIONS_HOT).counter().count() == 1.0d &&
        meterRegistry.get(WorkerDetectionMetrics.METRIC_DECISIONS_COOL).counter().count() == 1.0d,
      2_000
    );
  }

  @Test
  void refusesSubmissionsAfterShutdown() {
    dispatcher = dispatcher(1_000, 1_000, 8, 32, 30_000);
    dispatcher.shutdown();

    assertThat(dispatcher.submit(ZetaDecision.hot("h", 1L))).isFalse();
    assertThat(counter(WorkerDetectionMetrics.METRIC_DECISIONS_DROPPED, "hot")).isEqualTo(1.0d);
  }

  /** A promotion is delivered through the same single submission point as any other decision. */
  @Test
  void submitPromotedEntryDelivers() {
    dispatcher = dispatcher(1_000, 1_000, 8, 32, 30_000);
    HE promoted = mock(HE.class);
    when(promoted.decision()).thenReturn(ZetaDecision.hot("p", 1L));

    dispatcher.submit(promoted);

    await(() -> untagged(WorkerDetectionMetrics.METRIC_DECISIONS_HOT) == 1.0d, 2_000);
  }

  /** A synchronous refusal is the dispatcher's rollback, not the caller's. */
  @Test
  void submitPromotedEntryReturnsTheSeatWhenTheQueueRefuses() {
    List<String> aborted = new CopyOnWriteArrayList<>();
    dispatcher = dispatcher(1_000, 1_000, 8, 32, 30_000, (key, version) -> aborted.add(key + "@" + version));
    dispatcher.shutdown();
    HE promoted = mock(HE.class);
    when(promoted.decision()).thenReturn(ZetaDecision.hot("p", 1L));
    when(promoted.key()).thenReturn("p");
    when(promoted.version()).thenReturn(1L);

    dispatcher.submit(promoted);

    assertThat(aborted).containsExactly("p@1");
  }

  /** A saturation refusal returns the promotion's seat; the queue never accepted it. */
  @Test
  void aSaturatedPromotionOfferReturnsTheSeat() throws InterruptedException {
    WorkerBroadcaster funnel = mock(WorkerBroadcaster.class);
    CountDownLatch firstSendStarted = new CountDownLatch(1);
    CountDownLatch releaseFirstSend = new CountDownLatch(1);
    AtomicInteger broadcasts = new AtomicInteger();
    when(funnel.send(any(), anyLong())).thenAnswer(invocation -> {
      if (broadcasts.incrementAndGet() == 1) {
        firstSendStarted.countDown();
        releaseFirstSend.await(3, TimeUnit.SECONDS);
      }
      return WorkerBroadcaster.Delivery.SENT;
    });
    List<String> aborted = new CopyOnWriteArrayList<>();
    dispatcher = new DecisionDispatcher(
      1,
      8,
      1_000_000,
      1_000_000,
      8,
      8,
      30_000,
      funnel,
      metrics,
      (key, version) -> aborted.add(key + "@" + version),
      DecisionDispatcher.Probe.ALWAYS
    );
    HE promoted = mock(HE.class);
    when(promoted.decision()).thenReturn(ZetaDecision.hot("refused", 3L));
    when(promoted.key()).thenReturn("refused");
    when(promoted.version()).thenReturn(3L);
    try {
      assertThat(dispatcher.submit(ZetaDecision.hot("blocker", 1L))).isTrue();
      assertThat(firstSendStarted.await(3, TimeUnit.SECONDS)).isTrue();
      assertThat(dispatcher.submit(ZetaDecision.hot("queued", 2L))).isTrue();

      dispatcher.submit(promoted);

      assertThat(aborted).containsExactly("refused@3");
    } finally {
      releaseFirstSend.countDown();
    }
  }

  @Test
  void capsTheSendRateAtTheTokenBucket() {
    // 50/s: in 150ms at most ~8 tokens exist, so 20 submissions cannot all reach the wire.
    dispatcher = dispatcher(50, 50, 1, 1, 30_000);
    for (int i = 0; i < 20; i++) {
      dispatcher.submit(ZetaDecision.hot("k" + i, i + 1L));
    }

    await(() -> meterRegistry.get(WorkerDetectionMetrics.METRIC_DECISIONS_HOT).counter().count() >= 1.0d, 2_000);
    double sent = meterRegistry.get(WorkerDetectionMetrics.METRIC_DECISIONS_HOT).counter().count();
    assertThat(sent).isBetween(1.0d, 12.0d);
    verify(rabbitTemplate, org.mockito.Mockito.atLeastOnce()).send(anyString(), anyString(), any());
  }

  /**
   * The split decision sequence must converge: a queued HOT must not suppress the COOL of the
   * previous lifecycle. When the HOT fails before it reaches the broker, the Worker aborts the
   * promotion and is cold; the Apps must have received the COOL instead of staying hot until the
   * hard TTL (design decisions: decision delivery).
   */
  @Test
  void aFailedHotDoesNotEraseTheCoolThatWasAlreadySent() throws InterruptedException {
    CountDownLatch fillerStarted = new CountDownLatch(1);
    CountDownLatch releaseFiller = new CountDownLatch(1);
    List<String> sent = new CopyOnWriteArrayList<>();
    List<String> aborted = new CopyOnWriteArrayList<>();
    doAnswer(invocation -> {
      Message message = invocation.getArgument(2);
      String body = new String(message.getBody(), StandardCharsets.UTF_8);
      String type = (String) message.getMessageProperties().getHeader(HEADER_TYPE);
      if ("flip".equals(body) && "HOT".equals(type)) {
        throw new RuntimeException("broker down");
      }
      sent.add(body + ":" + type);
      if ("filler".equals(body)) {
        fillerStarted.countDown();
        releaseFiller.await(3, TimeUnit.SECONDS);
      }
      return null;
    })
      .when(rabbitTemplate)
      .send(any(String.class), any(String.class), any(Message.class));
    dispatcher = dispatcher(1_000_000, 1_000_000, 8, 8, 30_000, (key, version) -> aborted.add(key + "@" + version));

    assertThat(dispatcher.submit(ZetaDecision.cool("filler", 1L))).isTrue();
    assertThat(fillerStarted.await(3, TimeUnit.SECONDS)).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.cool("flip", 2L))).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.hot("flip", 3L))).isTrue();
    releaseFiller.countDown();

    await(() -> aborted.contains("flip@3") && sent.contains("flip:COOL"), 2_000);
    assertThat(sent).as("the COOL reached the cluster before the HOT failed").contains("flip:COOL");
    assertThat(sent).as("the failed HOT never reached the cluster").doesNotContain("flip:HOT");
    assertThat(aborted).containsExactly("flip@3");
  }

  /**
   * Expired decisions are terminal in-memory outcomes; they must not wait for the token bucket to
   * refill before being dropped and rolling back their HOT seat.
   */
  @Test
  void dropsExpiredDecisionsWithoutWaitingForAToken() {
    dispatcher = dispatcher(1, 1, 1, 1, 10);
    for (int i = 0; i < 10; i++) {
      dispatcher.submit(ZetaDecision.hot("k" + i, i + 1L));
    }

    await(() -> counter(WorkerDetectionMetrics.METRIC_DECISIONS_EXPIRED, "hot") == 10.0d, 700);
    assertThat(counter(WorkerDetectionMetrics.METRIC_DECISIONS_EXPIRED, "hot")).isEqualTo(10.0d);
  }

  /**
   * A round that only clears terminal decisions is progress. A backlog larger than one lane's work
   * budget must continue immediately instead of waiting for a token those decisions never spend.
   */
  @Test
  void aTerminalBacklogLargerThanTheLaneBudgetDoesNotWaitForAToken() {
    // The first COOL establishes the cold accepted record; the rest are elided in memory. One token
    // per second and a one-item burst leave the bucket empty while the backlog drains.
    dispatcher = new DecisionDispatcher(
      2_000,
      2_000,
      1,
      1,
      1,
      1,
      60_000,
      broadcaster,
      metrics,
      DecisionDispatcher.Rollback.NOOP,
      DecisionDispatcher.Probe.ALWAYS
    );
    assertThat(dispatcher.submit(ZetaDecision.cool("k", 1L))).isTrue();
    await(() -> untagged(WorkerDetectionMetrics.METRIC_DECISIONS_COOL) == 1.0d, 2_000);

    for (int version = 2; version <= 1_502; version++) {
      assertThat(dispatcher.submit(ZetaDecision.cool("k", version))).isTrue();
    }

    await(() -> counter(WorkerDetectionMetrics.METRIC_DECISIONS_ELIDED, "cool") == 1_501.0d, 900);
    assertThat(counter(WorkerDetectionMetrics.METRIC_DECISIONS_ELIDED, "cool")).isEqualTo(1_501.0d);
    assertThat(dispatcher.coolDepth()).isZero();
  }

  /**
   * Shutdown spends the tokens that are already available and then hands the remainder to the
   * final drop path; it must not sleep through the join timeout waiting for a refill.
   */
  @Test
  void shutdownDoesNotWaitForTokenRefill() {
    dispatcher = dispatcher(1, 1, 1, 1, 30_000);
    for (int i = 0; i < 10; i++) {
      assertThat(dispatcher.submit(ZetaDecision.hot("k" + i, i + 1L))).isTrue();
    }

    long start = System.nanoTime();
    dispatcher.shutdown();
    long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

    assertThat(elapsedMs).as("shutdown must not wait for the next token").isLessThan(1_000L);
    assertThat(
      untagged(WorkerDetectionMetrics.METRIC_DECISIONS_HOT) +
        counter(WorkerDetectionMetrics.METRIC_DECISIONS_DROPPED, "hot")
    )
      .as("every queued HOT is either sent or counted as dropped")
      .isEqualTo(10.0d);
  }

  /**
   * A drain blocked in a send must not refill its bucket during the blocked interval: the
   * shutdown contract spends only the tokens that existed when it began, leaving the backlog for
   * the final drop path instead of sending it for the whole join timeout.
   */
  @Test
  void shutdownDoesNotRefillAfterTheDrainBlocksInASend() throws InterruptedException {
    CountDownLatch firstSendStarted = new CountDownLatch(1);
    CountDownLatch releaseFirstSend = new CountDownLatch(1);
    doAnswer(invocation -> {
      firstSendStarted.countDown();
      releaseFirstSend.await(5, TimeUnit.SECONDS);
      return null;
    })
      .when(rabbitTemplate)
      .send(any(String.class), any(String.class), any(Message.class));
    // One token per second, bucket capped at one: a blocked interval longer than a second is
    // exactly one refill, so the guard's effect is observable.
    dispatcher = dispatcher(1, 1, 1, 1, 30_000);

    assertThat(dispatcher.submit(ZetaDecision.hot("first", 1L))).isTrue();
    assertThat(firstSendStarted.await(3, TimeUnit.SECONDS)).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.hot("queued", 2L))).isTrue();

    Thread releaser = new Thread(() -> {
      try {
        Thread.sleep(1_200L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      releaseFirstSend.countDown();
    });
    releaser.start();
    dispatcher.shutdown();
    releaser.join(3_000L);

    assertThat(untagged(WorkerDetectionMetrics.METRIC_DECISIONS_HOT))
      .as("only the token held when shutdown began may be spent")
      .isEqualTo(1.0d);
    assertThat(counter(WorkerDetectionMetrics.METRIC_DECISIONS_DROPPED, "hot"))
      .as("the blocked interval must not mint a replacement token")
      .isEqualTo(1.0d);
  }

  @Test
  void dropsDecisionsThatOutliveTheirMaximumResidency() {
    // 5/s with a 20ms age bound: the first send drains the bucket, the next token arrives 200ms
    // later, by which time everything queued behind it is already too old to send.
    dispatcher = dispatcher(5, 5, 1, 1, 20);
    for (int i = 0; i < 10; i++) {
      dispatcher.submit(ZetaDecision.hot("k" + i, i + 1L));
    }

    await(() -> counter(WorkerDetectionMetrics.METRIC_DECISIONS_EXPIRED, "hot") >= 5.0d, 3_000);
  }

  @Test
  void anExpiredHotDecisionRollsBackItsPromotion() throws InterruptedException {
    List<String> aborted = new CopyOnWriteArrayList<>();
    CountDownLatch sendStarted = new CountDownLatch(1);
    // The first send holds the drain long enough for the decision queued behind it to outlive its
    // 100ms bound; the latch makes "first is already in flight" deterministic.
    doAnswer(invocation -> {
      sendStarted.countDown();
      Thread.sleep(200L);
      return null;
    })
      .when(rabbitTemplate)
      .send(any(String.class), any(String.class), any(Message.class));
    dispatcher = dispatcher(1_000_000, 1_000_000, 8, 8, 100, (key, version) -> aborted.add(key + "@" + version));

    assertThat(dispatcher.submit(ZetaDecision.hot("first", 1L))).isTrue();
    assertThat(sendStarted.await(3, TimeUnit.SECONDS)).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.hot("late", 2L))).isTrue();

    await(() -> aborted.contains("late@2"), 3_000);
    assertThat(aborted)
      .as("the sent decision keeps its seat; only the expired one is undone")
      .containsExactly("late@2");
  }

  @Test
  void shutdownStopsRetryingAFailingBrokerAndRollsBackTheRemainder() {
    List<String> aborted = new CopyOnWriteArrayList<>();
    doThrow(new RuntimeException("broker down"))
      .when(rabbitTemplate)
      .send(any(String.class), any(String.class), any(Message.class));
    dispatcher = dispatcher(1_000, 1_000, 8, 32, 30_000, (key, version) -> aborted.add(key));
    for (int i = 0; i < 20; i++) {
      assertThat(dispatcher.submit(ZetaDecision.hot("h" + i, i + 1L))).isTrue();
    }
    await(() -> !aborted.isEmpty(), 1_000);

    long start = System.nanoTime();
    dispatcher.shutdown();
    long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

    assertThat(elapsedMs).as("a failing send during shutdown must not burn the join timeout").isLessThan(1_000L);
    assertThat(counter(WorkerDetectionMetrics.METRIC_DECISIONS_DROPPED, "hot"))
      .as("one drop per queued decision: attempted-and-failed or discarded by the final drain")
      .isEqualTo(20.0d);
    assertThat(aborted).as("every HOT that never reached the broker returned its seat").hasSize(20);
  }

  /**
   * A COOL that overtakes a still-queued HOT is still sent: the queued HOT may fail, and the
   * demotion is the conservative correction for whatever state the cluster already holds. The
   * obsolete HOT leaves its queue at the head without reaching the wire.
   */
  @Test
  void dropsAnOvertakenHotAndStillSendsTheCool() throws InterruptedException {
    CountDownLatch firstSendStarted = new CountDownLatch(1);
    CountDownLatch releaseFirstSend = new CountDownLatch(1);
    doAnswer(invocation -> {
      firstSendStarted.countDown();
      releaseFirstSend.await(3, TimeUnit.SECONDS);
      return null;
    })
      .when(rabbitTemplate)
      .send(any(String.class), any(String.class), any(Message.class));
    dispatcher = dispatcher(1, 1_000_000, 1, 32, 30_000);

    assertThat(dispatcher.submit(ZetaDecision.cool("filler", 1L))).isTrue();
    assertThat(firstSendStarted.await(3, TimeUnit.SECONDS)).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.hot("k", 2L))).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.cool("k", 3L))).isTrue();
    releaseFirstSend.countDown();

    await(() -> counter(WorkerDetectionMetrics.METRIC_DECISIONS_SUPERSEDED, "hot") == 1.0d, 2_000);
    await(() -> untagged(WorkerDetectionMetrics.METRIC_DECISIONS_COOL) == 2.0d, 2_000);
    assertThat(dispatcher.hotDepth()).as("the obsolete head left the queue").isZero();
    assertThat(counter(WorkerDetectionMetrics.METRIC_DECISIONS_ELIDED, "cool")).isZero();
    assertThat(untagged(WorkerDetectionMetrics.METRIC_DECISIONS_HOT)).as("the HOT never reached the wire").isZero();
    assertThat(untagged(WorkerDetectionMetrics.METRIC_DECISIONS_COOL))
      .as("the filler and the corrective COOL were sent")
      .isEqualTo(2.0d);
    verify(rabbitTemplate, times(2)).send(any(String.class), any(String.class), any(Message.class));
  }

  /**
   * A COOL for a key whose promotion never reached the cluster is a no-op when the key's last
   * accepted decision is already a COOL: the cluster is cold, so the drain elides it instead of
   * spending a cool token and a network round trip.
   */
  @Test
  void elidesACoolWhoseLifecycleNeverReachedTheCluster() throws InterruptedException {
    CountDownLatch fillerSendStarted = new CountDownLatch(1);
    CountDownLatch releaseFillerSend = new CountDownLatch(1);
    dispatcher = dispatcher(1, 1_000_000, 1, 32, 30_000);

    // The previous lifecycle's COOL reached the cluster: from here on the key is cold there.
    assertThat(dispatcher.submit(ZetaDecision.cool("k", 1L))).isTrue();
    await(() -> untagged(WorkerDetectionMetrics.METRIC_DECISIONS_COOL) == 1.0d, 2_000);

    doAnswer(invocation -> {
      fillerSendStarted.countDown();
      releaseFillerSend.await(3, TimeUnit.SECONDS);
      return null;
    })
      .when(rabbitTemplate)
      .send(any(String.class), any(String.class), any(Message.class));

    // Hold the drain inside a COOL send so the HOT and the next COOL are both queued before the
    // round reaches them.
    assertThat(dispatcher.submit(ZetaDecision.cool("filler", 2L))).isTrue();
    assertThat(fillerSendStarted.await(3, TimeUnit.SECONDS)).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.hot("k", 3L))).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.cool("k", 4L))).isTrue();
    releaseFillerSend.countDown();

    // The HOT is dropped after the COOL is elided in the same round, so awaiting the drop means
    // both outcomes are settled.
    await(() -> counter(WorkerDetectionMetrics.METRIC_DECISIONS_SUPERSEDED, "hot") == 1.0d, 2_000);
    assertThat(counter(WorkerDetectionMetrics.METRIC_DECISIONS_ELIDED, "cool")).isEqualTo(1.0d);
    assertThat(counter(WorkerDetectionMetrics.METRIC_DECISIONS_SUPERSEDED, "hot")).isEqualTo(1.0d);
    assertThat(untagged(WorkerDetectionMetrics.METRIC_DECISIONS_COOL))
      .as("only k's first COOL and the filler")
      .isEqualTo(2.0d);
    assertThat(untagged(WorkerDetectionMetrics.METRIC_DECISIONS_HOT)).isZero();
    verify(rabbitTemplate, times(2)).send(any(String.class), any(String.class), any(Message.class));
  }

  /**
   * A duplicate HOT at a version the funnel already accepted describes the promotion the cluster
   * holds hot; dropping it must not return the live seat.
   */
  @Test
  void anAcceptedDuplicateHotDoesNotReturnItsSeat() {
    List<String> aborted = new CopyOnWriteArrayList<>();
    dispatcher = dispatcher(1_000_000, 1_000_000, 8, 32, 30_000, (key, version) -> aborted.add(key + "@" + version));

    assertThat(dispatcher.submit(ZetaDecision.hot("dup", 1L))).isTrue();
    await(() -> untagged(WorkerDetectionMetrics.METRIC_DECISIONS_HOT) == 1.0d, 2_000);
    assertThat(dispatcher.submit(ZetaDecision.hot("dup", 1L))).isTrue();

    await(() -> counter(WorkerDetectionMetrics.METRIC_DECISIONS_ELIDED, "hot") == 1.0d, 2_000);
    assertThat(aborted).as("the duplicate's seat is the one already accepted").isEmpty();
  }

  /**
   * A HOT the funnel rejects as superseded never changes the cluster's state, so its seat must be
   * returned even though the pending check at the pop found nothing newer.
   */
  @Test
  void aFunnelSupersededHotReturnsItsSeat() {
    WorkerBroadcaster funnel = mock(WorkerBroadcaster.class);
    when(funnel.send(any(), anyLong())).thenReturn(WorkerBroadcaster.Delivery.SUPERSEDED);
    List<String> aborted = new CopyOnWriteArrayList<>();
    dispatcher = new DecisionDispatcher(
      1_000,
      1_000,
      1_000_000,
      1_000_000,
      8,
      32,
      30_000,
      funnel,
      metrics,
      (key, version) -> aborted.add(key + "@" + version),
      DecisionDispatcher.Probe.ALWAYS
    );

    assertThat(dispatcher.submit(ZetaDecision.hot("h", 1L))).isTrue();

    await(() -> !aborted.isEmpty(), 2_000);
    assertThat(aborted).containsExactly("h@1");
  }

  /**
   * An unexpected failure in the drain round must not kill the only delivery thread: the in-flight
   * HOT returns its seat, and the next queued decision is still delivered.
   */
  @Test
  void anUnexpectedDrainFailureDoesNotKillTheDeliveryThread() {
    WorkerBroadcaster funnel = mock(WorkerBroadcaster.class);
    AtomicInteger broadcasts = new AtomicInteger();
    List<String> aborted = new CopyOnWriteArrayList<>();
    when(funnel.send(any(), anyLong())).thenAnswer(invocation -> {
      if (broadcasts.incrementAndGet() == 1) {
        throw new IllegalStateException("boom");
      }
      return WorkerBroadcaster.Delivery.SENT;
    });
    dispatcher = new DecisionDispatcher(
      1_000,
      1_000,
      1_000_000,
      1_000_000,
      8,
      32,
      30_000,
      funnel,
      metrics,
      (key, version) -> aborted.add(key + "@" + version),
      DecisionDispatcher.Probe.ALWAYS
    );

    assertThat(dispatcher.submit(ZetaDecision.hot("k1", 1L))).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.hot("k2", 2L))).isTrue();

    await(() -> broadcasts.get() >= 2, 2_000);
    assertThat(broadcasts).as("the drain thread survived the first failure").hasValue(2);
    assertThat(aborted).as("the undelivered HOT returned its seat").contains("k1@1");
    assertThat(counter(WorkerDetectionMetrics.METRIC_DECISIONS_DROPPED, "hot"))
      .as("the failed decision lands in the delivery ledger")
      .isEqualTo(1.0d);
    assertThat(dispatcher.hotDepth()).isZero();
  }

  /**
   * A refused enqueue must not publish its item or suppress the decision already waiting: the
   * queued version still reaches the funnel and the refused version never enters the queue.
   */
  @Test
  void aRefusedOfferDoesNotSuppressTheDecisionAlreadyQueued() throws InterruptedException {
    CountDownLatch firstSendStarted = new CountDownLatch(1);
    CountDownLatch releaseFirstSend = new CountDownLatch(1);
    List<String> sent = new CopyOnWriteArrayList<>();
    List<String> aborted = new CopyOnWriteArrayList<>();
    doAnswer(invocation -> {
      Message message = invocation.getArgument(2);
      String body = new String(message.getBody(), StandardCharsets.UTF_8);
      Long version = (Long) message.getMessageProperties().getHeaders().get(HEADER_VERSION);
      sent.add(body + ":" + version);
      firstSendStarted.countDown();
      releaseFirstSend.await(3, TimeUnit.SECONDS);
      return null;
    })
      .when(rabbitTemplate)
      .send(any(String.class), any(String.class), any(Message.class));
    dispatcher = new DecisionDispatcher(
      2,
      1_000,
      1_000_000,
      1_000_000,
      8,
      32,
      30_000,
      broadcaster,
      metrics,
      (key, version) -> aborted.add(key + "@" + version),
      DecisionDispatcher.Probe.ALWAYS
    );

    // The first decision is a COOL so the broadcaster's same-type debounce cannot elide the HOT
    // under test; the dispatcher's supersede rule is what the test pins.
    assertThat(dispatcher.submit(ZetaDecision.cool("head", 1L))).isTrue();
    assertThat(firstSendStarted.await(3, TimeUnit.SECONDS)).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.hot("head", 2L))).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.hot("head", 3L))).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.hot("head", 4L))).as("the queue is full").isFalse();
    releaseFirstSend.countDown();

    await(() -> sent.contains("head:3"), 2_000);
    assertThat(sent).as("the queued version survived the refused newer offer").contains("head:1", "head:3");
    assertThat(sent).as("the refused offer never entered the queue").doesNotContain("head:4");
    assertThat(aborted).as("the refused item is the caller's rollback, not the dispatcher's").doesNotContain("head@4");
  }

  /**
   * An older HOT behind a newer pending decision is terminal in memory: it must not take a bounded
   * queue slot only for the drain to drop it later, and its seat is returned through the
   * identity-guarded abort.
   */
  @Test
  void anOlderHotBehindANewerPendingDecisionDoesNotTakeAQueueSlot() throws InterruptedException {
    WorkerBroadcaster funnel = mock(WorkerBroadcaster.class);
    CountDownLatch firstSendStarted = new CountDownLatch(1);
    CountDownLatch releaseFirstSend = new CountDownLatch(1);
    AtomicInteger broadcasts = new AtomicInteger();
    when(funnel.send(any(), anyLong())).thenAnswer(invocation -> {
      if (broadcasts.incrementAndGet() == 1) {
        firstSendStarted.countDown();
        releaseFirstSend.await(3, TimeUnit.SECONDS);
      }
      return WorkerBroadcaster.Delivery.SENT;
    });
    List<String> aborted = new CopyOnWriteArrayList<>();
    dispatcher = new DecisionDispatcher(
      1_000,
      1_000,
      1_000_000,
      1_000_000,
      8,
      32,
      30_000,
      funnel,
      metrics,
      (key, version) -> aborted.add(key + "@" + version),
      DecisionDispatcher.Probe.ALWAYS
    );

    assertThat(dispatcher.submit(ZetaDecision.cool("filler", 1L))).isTrue();
    assertThat(firstSendStarted.await(3, TimeUnit.SECONDS)).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.hot("k", 5L))).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.hot("k", 4L))).as("dropped, not refused").isTrue();

    assertThat(dispatcher.hotDepth()).as("only the newer HOT is queued").isEqualTo(1);
    assertThat(counter(WorkerDetectionMetrics.METRIC_DECISIONS_SUPERSEDED, "hot")).isEqualTo(1.0d);
    assertThat(aborted).containsExactly("k@4");
    releaseFirstSend.countDown();
  }

  /**
   * A token-starved head is classified by the funnel's own state machine: a COOL the funnel would
   * elide as redundant leaves the queue instead of blocking the lane until the next token, and the
   * elision is counted in the same bucket the send path uses.
   */
  @Test
  void aTokenStarvedHeadIsElidedByTheFunnelsStateMachine() {
    dispatcher = dispatcher(1, 1, 1, 1, 30_000);
    assertThat(dispatcher.submit(ZetaDecision.cool("k", 1L))).isTrue();
    await(() -> untagged(WorkerDetectionMetrics.METRIC_DECISIONS_COOL) == 1.0d, 2_000);

    assertThat(dispatcher.submit(ZetaDecision.cool("k", 2L))).isTrue();

    await(() -> counter(WorkerDetectionMetrics.METRIC_DECISIONS_ELIDED, "cool") == 1.0d, 2_000);
    assertThat(counter(WorkerDetectionMetrics.METRIC_DECISIONS_SUPERSEDED, "cool")).isZero();
    assertThat(dispatcher.coolDepth()).isZero();
  }

  /**
   * An expired decision the funnel's state machine would elide is counted as elided, not expired:
   * its age cannot change the cluster's state, so it must not raise the expired meter (nor the
   * aggregated failure WARN). The bucket has tokens available, which is what pins the send path's
   * classification order rather than the token-starved head's.
   */
  @Test
  void anExpiredDecisionTheFunnelElidesIsNotReportedExpired() throws InterruptedException {
    // The first COOL is accepted, so the cluster is already cold for "k".
    dispatcher = dispatcher(1_000_000, 1_000_000, 8, 8, 20);
    assertThat(dispatcher.submit(ZetaDecision.cool("k", 1L))).isTrue();
    await(() -> untagged(WorkerDetectionMetrics.METRIC_DECISIONS_COOL) == 1.0d, 2_000);

    CountDownLatch fillerSendStarted = new CountDownLatch(1);
    CountDownLatch releaseFillerSend = new CountDownLatch(1);
    doAnswer(invocation -> {
      fillerSendStarted.countDown();
      releaseFillerSend.await(3, TimeUnit.SECONDS);
      return null;
    })
      .when(rabbitTemplate)
      .send(any(String.class), any(String.class), any(Message.class));

    // Hold the drain inside the filler send while the second COOL for "k" waits past its age bound.
    assertThat(dispatcher.submit(ZetaDecision.cool("filler", 2L))).isTrue();
    assertThat(fillerSendStarted.await(3, TimeUnit.SECONDS)).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.cool("k", 3L))).isTrue();
    Thread.sleep(60L);
    releaseFillerSend.countDown();

    await(() -> counter(WorkerDetectionMetrics.METRIC_DECISIONS_ELIDED, "cool") == 1.0d, 2_000);
    assertThat(counter(WorkerDetectionMetrics.METRIC_DECISIONS_ELIDED, "cool")).isEqualTo(1.0d);
    assertThat(counter(WorkerDetectionMetrics.METRIC_DECISIONS_EXPIRED, "cool"))
      .as("an elided decision is not an expiry")
      .isZero();
    assertThat(dispatcher.coolDepth()).isZero();
  }

  /**
   * A promotion the registry no longer holds is obsolete before the funnel. The report thread can
   * stall between the transition and the submission while the key cools, and the demotion itself may
   * be refused, expired or failed — neither the pending map nor the accepted record then proves the
   * HOT stale. Broadcasting it would leave every App hot for a key the Worker has released.
   */
  @Test
  void aHotWhosePromotionIsNoLongerCurrentIsNotBroadcast() {
    List<String> aborted = new CopyOnWriteArrayList<>();
    dispatcher = new DecisionDispatcher(
      1_000,
      1_000,
      1_000_000,
      1_000_000,
      8,
      32,
      30_000,
      broadcaster,
      metrics,
      (key, version) -> aborted.add(key + "@" + version),
      (key, version) -> false
    );

    assertThat(dispatcher.submit(ZetaDecision.hot("cooled", 7L))).isTrue();

    await(() -> !aborted.isEmpty(), 2_000);
    assertThat(aborted).containsExactly("cooled@7");
    assertThat(counter(WorkerDetectionMetrics.METRIC_DECISIONS_SUPERSEDED, "hot")).isEqualTo(1.0d);
    assertThat(untagged(WorkerDetectionMetrics.METRIC_DECISIONS_HOT)).isZero();
    verify(rabbitTemplate, never()).send(anyString(), anyString(), any());
  }

  /**
   * A HOT dropped before the wire leaves the cluster cold. The lifecycle's COOL must be elided even
   * though the pending map no longer holds the HOT: the cold accepted record, installed when the
   * drain dropped it, is the proof.
   */
  @Test
  void aCoolThatFollowsADroppedHotWithoutAnAcceptedRecordIsElided() {
    dispatcher = new DecisionDispatcher(
      1_000,
      1_000,
      1_000_000,
      1_000_000,
      8,
      32,
      30_000,
      broadcaster,
      metrics,
      DecisionDispatcher.Rollback.NOOP,
      (key, version) -> false
    );

    assertThat(dispatcher.submit(ZetaDecision.hot("k", 1L))).isTrue();

    await(() -> counter(WorkerDetectionMetrics.METRIC_DECISIONS_SUPERSEDED, "hot") == 1.0d, 2_000);
    assertThat(dispatcher.submit(ZetaDecision.cool("k", 2L))).isTrue();

    await(() -> counter(WorkerDetectionMetrics.METRIC_DECISIONS_ELIDED, "cool") == 1.0d, 2_000);
    assertThat(untagged(WorkerDetectionMetrics.METRIC_DECISIONS_COOL))
      .as("a COOL for a lifecycle the cluster never saw is a no-op")
      .isZero();
    verify(rabbitTemplate, never()).send(anyString(), anyString(), any());
  }

  /**
   * The registry probe applies at the drain, not only at submission: a HOT already queued when its
   * key cools must not reach the wire even though no newer decision is pending (its COOL was refused)
   * and no accepted record exists.
   */
  @Test
  void aQueuedHotIsDroppedWhenItsPromotionWasReleasedBeforeTheSend() throws InterruptedException {
    CountDownLatch fillerStarted = new CountDownLatch(1);
    CountDownLatch releaseFiller = new CountDownLatch(1);
    List<String> aborted = new CopyOnWriteArrayList<>();
    AtomicBoolean current = new AtomicBoolean(true);
    doAnswer(invocation -> {
      fillerStarted.countDown();
      releaseFiller.await(3, TimeUnit.SECONDS);
      return null;
    })
      .when(rabbitTemplate)
      .send(any(String.class), any(String.class), any(Message.class));
    dispatcher = new DecisionDispatcher(
      1_000,
      1_000,
      1_000_000,
      1_000_000,
      8,
      32,
      30_000,
      broadcaster,
      metrics,
      (key, version) -> aborted.add(key + "@" + version),
      (key, version) -> current.get()
    );

    assertThat(dispatcher.submit(ZetaDecision.cool("filler", 1L))).isTrue();
    assertThat(fillerStarted.await(3, TimeUnit.SECONDS)).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.hot("k", 2L))).isTrue();
    current.set(false);
    releaseFiller.countDown();

    await(() -> !aborted.isEmpty(), 2_000);
    assertThat(aborted).containsExactly("k@2");
    assertThat(untagged(WorkerDetectionMetrics.METRIC_DECISIONS_HOT)).isZero();
  }

  /**
   * A stalled HOT can reach the dispatcher after the COOL that already superseded it. The COOL has
   * no accepted record, so it is sent — the only demotion signal the cluster can receive — and the
   * late HOT is dropped as stale.
   */
  @Test
  void aCoolThatArrivedBeforeItsSupersededHotIsSent() throws InterruptedException {
    CountDownLatch fillerStarted = new CountDownLatch(1);
    CountDownLatch releaseFiller = new CountDownLatch(1);
    List<String> sent = new CopyOnWriteArrayList<>();
    doAnswer(invocation -> {
      Message message = invocation.getArgument(2);
      String body = new String(message.getBody(), StandardCharsets.UTF_8);
      sent.add(body);
      if ("filler".equals(body)) {
        fillerStarted.countDown();
        releaseFiller.await(3, TimeUnit.SECONDS);
      }
      return null;
    })
      .when(rabbitTemplate)
      .send(any(String.class), any(String.class), any(Message.class));
    dispatcher = dispatcher(1_000, 1_000, 8, 32, 30_000);

    assertThat(dispatcher.submit(ZetaDecision.cool("filler", 1L))).isTrue();
    assertThat(fillerStarted.await(3, TimeUnit.SECONDS)).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.cool("k", 6L))).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.hot("k", 5L))).isTrue();
    releaseFiller.countDown();

    await(() -> untagged(WorkerDetectionMetrics.METRIC_DECISIONS_COOL) == 2.0d, 2_000);
    assertThat(sent).containsExactly("filler", "k");
    assertThat(counter(WorkerDetectionMetrics.METRIC_DECISIONS_ELIDED, "cool")).isZero();
    assertThat(untagged(WorkerDetectionMetrics.METRIC_DECISIONS_HOT)).isZero();
  }

  /**
   * A COOL with no accepted record is the only demotion signal a locally promoted App can receive,
   * so it is sent rather than elided as a ghost.
   */
  @Test
  void aCoolWithoutAnAcceptedRecordIsSent() {
    dispatcher = dispatcher(1_000, 1_000, 8, 32, 30_000);

    assertThat(dispatcher.submit(ZetaDecision.cool("k", 2L))).isTrue();

    await(() -> untagged(WorkerDetectionMetrics.METRIC_DECISIONS_COOL) == 1.0d, 2_000);
    assertThat(counter(WorkerDetectionMetrics.METRIC_DECISIONS_ELIDED, "cool")).isZero();
    verify(rabbitTemplate, times(1)).send(anyString(), anyString(), any());
  }

  /**
   * A COOL behind a newer HOT yields the round, but a failed HOT must not erase it: the next round
   * still attempts the demotion, which is the correction for whatever state the cluster holds.
   */
  @Test
  void aCoolBehindAFailedHotIsStillAttempted() {
    AtomicInteger sendAttempts = new AtomicInteger();
    doAnswer(invocation -> {
      sendAttempts.incrementAndGet();
      throw new RuntimeException("broker down");
    })
      .when(rabbitTemplate)
      .send(anyString(), anyString(), any());
    dispatcher = dispatcher(1_000, 1_000, 8, 32, 30_000);

    assertThat(dispatcher.submit(ZetaDecision.hot("k", 11L))).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.cool("k", 10L))).isTrue();

    await(() -> sendAttempts.get() == 2, 2_000);
    assertThat(sendAttempts).as("the HOT failed, so the COOL is the corrective send").hasValue(2);
    assertThat(counter(WorkerDetectionMetrics.METRIC_DECISIONS_ELIDED, "cool")).isZero();
  }

  /**
   * A COOL already superseded by a pending HOT yields the round to the HOT lane when it can send:
   * the HOT is accepted first, and the next pass drops the demotion as superseded. The opposite
   * order demotes and re-promotes every App for a state that never changed.
   */
  @Test
  void aSupersededCoolYieldsTheRoundToItsPendingHot() throws InterruptedException {
    CountDownLatch fillerSendStarted = new CountDownLatch(1);
    CountDownLatch releaseFillerSend = new CountDownLatch(1);
    List<String> sent = new CopyOnWriteArrayList<>();
    doAnswer(invocation -> {
      Message message = invocation.getArgument(2);
      String body = new String(message.getBody(), StandardCharsets.UTF_8);
      String type = (String) message.getMessageProperties().getHeader(HEADER_TYPE);
      sent.add(body + ":" + type);
      if ("filler".equals(body)) {
        fillerSendStarted.countDown();
        releaseFillerSend.await(3, TimeUnit.SECONDS);
      }
      return null;
    })
      .when(rabbitTemplate)
      .send(any(String.class), any(String.class), any(Message.class));
    dispatcher = dispatcher(1_000_000, 1_000_000, 8, 32, 30_000);

    assertThat(dispatcher.submit(ZetaDecision.cool("filler", 1L))).isTrue();
    assertThat(fillerSendStarted.await(3, TimeUnit.SECONDS)).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.hot("k", 11L))).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.cool("k", 10L))).isTrue();
    releaseFillerSend.countDown();

    await(() -> sent.size() == 2 && counter(WorkerDetectionMetrics.METRIC_DECISIONS_SUPERSEDED, "cool") == 1.0d, 2_000);
    assertThat(sent).containsExactly("filler:COOL", "k:HOT");
    assertThat(untagged(WorkerDetectionMetrics.METRIC_DECISIONS_COOL))
      .as("the obsolete demotion never reached the wire")
      .isEqualTo(1.0d);
  }

  /**
   * A superseded COOL yields at most one round. When the newer HOT sits deeper than one hot-lane
   * burst, an unbounded yield would hold every unrelated demotion behind that single key; the
   * second round sends the COOL so the cool lane keeps releasing seats.
   */
  @Test
  void aSupersededCoolYieldsOnlyOneRoundToADeepHot() throws InterruptedException {
    CountDownLatch fillerSendStarted = new CountDownLatch(1);
    CountDownLatch releaseFillerSend = new CountDownLatch(1);
    List<String> sent = new CopyOnWriteArrayList<>();
    doAnswer(invocation -> {
      Message message = invocation.getArgument(2);
      String body = new String(message.getBody(), StandardCharsets.UTF_8);
      String type = (String) message.getMessageProperties().getHeader(HEADER_TYPE);
      sent.add(body + ":" + type);
      if ("filler".equals(body)) {
        fillerSendStarted.countDown();
        releaseFillerSend.await(3, TimeUnit.SECONDS);
      }
      return null;
    })
      .when(rabbitTemplate)
      .send(any(String.class), any(String.class), any(Message.class));
    dispatcher = dispatcher(1_000_000, 1_000_000, 1, 2, 30_000);

    assertThat(dispatcher.submit(ZetaDecision.cool("filler", 1L))).isTrue();
    assertThat(fillerSendStarted.await(3, TimeUnit.SECONDS)).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.cool("k", 2L))).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.cool("u", 3L))).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.hot("h1", 4L))).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.hot("h2", 5L))).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.hot("k", 6L))).isTrue();
    releaseFillerSend.countDown();

    await(() -> sent.contains("k:HOT") && sent.contains("u:COOL"), 2_000);
    assertThat(sent.indexOf("u:COOL"))
      .as("an unrelated demotion must not wait for every HOT ahead of its key")
      .isLessThan(sent.indexOf("k:HOT"));
  }

  /**
   * A failed HOT must not inherit the cold tombstone an earlier superseded HOT installed: the
   * conservative correction sends its COOL anyway, because the cluster may hold hot state from an
   * earlier lifecycle whose COOL was lost.
   */
  @Test
  void aFailedHotClearsTheColdTombstoneSoItsCoolIsStillSent() {
    List<String> sent = new CopyOnWriteArrayList<>();
    doAnswer(invocation -> {
      Message message = invocation.getArgument(2);
      String type = (String) message.getMessageProperties().getHeader(HEADER_TYPE);
      String body = new String(message.getBody(), StandardCharsets.UTF_8);
      if ("HOT".equals(type)) {
        throw new RuntimeException("broker down");
      }
      sent.add(body + ":" + type);
      return null;
    })
      .when(rabbitTemplate)
      .send(any(String.class), any(String.class), any(Message.class));
    dispatcher = new DecisionDispatcher(
      1_000,
      1_000,
      1_000_000,
      1_000_000,
      8,
      32,
      30_000,
      broadcaster,
      metrics,
      DecisionDispatcher.Rollback.NOOP,
      (key, version) -> version != 1L
    );

    // v1 has no accepted record and is superseded: the funnel installs its cold tombstone.
    assertThat(dispatcher.submit(ZetaDecision.hot("k", 1L))).isTrue();
    await(() -> counter(WorkerDetectionMetrics.METRIC_DECISIONS_SUPERSEDED, "hot") == 1.0d, 2_000);

    // v3 is a new lifecycle whose publish fails. Its COOL must still travel.
    assertThat(dispatcher.submit(ZetaDecision.hot("k", 3L))).isTrue();
    await(() -> counter(WorkerDetectionMetrics.METRIC_DECISIONS_DROPPED, "hot") == 1.0d, 2_000);
    assertThat(dispatcher.submit(ZetaDecision.cool("k", 4L))).isTrue();

    await(() -> sent.contains("k:COOL"), 2_000);
    assertThat(sent).contains("k:COOL");
    assertThat(counter(WorkerDetectionMetrics.METRIC_DECISIONS_ELIDED, "cool"))
      .as("the tombstone from the superseded HOT must not elide the corrective COOL")
      .isZero();
  }

  /**
   * An older HOT never suppresses a COOL from a newer lifecycle: the COOL's version already
   * outranks it, so the demotion travels and the HOT is dropped.
   */
  @Test
  void anOlderHotDoesNotSuppressANewerCool() throws InterruptedException {
    CountDownLatch fillerStarted = new CountDownLatch(1);
    CountDownLatch releaseFiller = new CountDownLatch(1);
    List<String> sent = new CopyOnWriteArrayList<>();
    doAnswer(invocation -> {
      Message message = invocation.getArgument(2);
      String body = new String(message.getBody(), StandardCharsets.UTF_8);
      sent.add(body);
      if ("filler".equals(body)) {
        fillerStarted.countDown();
        releaseFiller.await(3, TimeUnit.SECONDS);
      }
      return null;
    })
      .when(rabbitTemplate)
      .send(any(String.class), any(String.class), any(Message.class));
    dispatcher = dispatcher(1_000_000, 1_000_000, 8, 32, 30_000);

    assertThat(dispatcher.submit(ZetaDecision.cool("filler", 1L))).isTrue();
    assertThat(fillerStarted.await(3, TimeUnit.SECONDS)).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.cool("k", 13L))).isTrue();
    assertThat(dispatcher.submit(ZetaDecision.hot("k", 10L))).as("dropped, not refused").isTrue();
    releaseFiller.countDown();

    await(() -> sent.contains("k"), 2_000);
    assertThat(sent).as("the COOL belongs to a newer lifecycle and must travel").contains("k");
    assertThat(counter(WorkerDetectionMetrics.METRIC_DECISIONS_ELIDED, "cool")).isZero();
  }

  /**
   * A HOT that becomes terminal while its lane is token-starved leaves without waiting for the next
   * token. The probe flips on its first call, simulating the registry releasing the promotion after
   * the drain's own peek found it current.
   */
  @Test
  void aHotThatGoesStaleWhileTokenStarvedLeavesWithoutAToken() {
    AtomicInteger probes = new AtomicInteger();
    List<String> aborted = new CopyOnWriteArrayList<>();
    dispatcher = new DecisionDispatcher(
      1_000,
      1_000,
      1,
      1_000_000,
      8,
      32,
      30_000,
      broadcaster,
      metrics,
      (key, version) -> aborted.add(key + "@" + version),
      (key, version) -> probes.getAndIncrement() == 0
    );

    assertThat(dispatcher.submit(ZetaDecision.hot("stale", 1L))).isTrue();

    await(() -> aborted.contains("stale@1"), 2_000);
    assertThat(aborted).containsExactly("stale@1");
    assertThat(dispatcher.hotDepth()).isZero();
  }

  private double counter(String name, String type) {
    return meterRegistry.get(name).tag("type", type).counter().count();
  }

  private double untagged(String name) {
    return meterRegistry.get(name).counter().count();
  }

  private static void await(BooleanSupplier condition, long timeoutMs) {
    long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      try {
        Thread.sleep(10L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }
}
