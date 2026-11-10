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

import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.rabbitmq.client.Channel;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycleImpl;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.sync.worker.DefaultWorkerContext;
import io.github.hyshmily.zeta.sync.worker.HotAction;
import io.github.hyshmily.zeta.sync.worker.WorkerDecision;
import io.github.hyshmily.zeta.sync.worker.WorkerListener;
import io.github.hyshmily.zeta.sync.worker.WorkerListenerProperties;
import io.github.hyshmily.zeta.sync.worker.WorkerPolicy;
import io.github.hyshmily.zeta.sync.worker.WorkerType;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.clock.StampClock;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * Tests for {@link WorkerListener} verifying HOT/COOL decision handling, cache state promotion and
 * downgrade, and error NACK behavior.
 */
class WorkerListenerTest {

  private Cache<String, CacheEntry> cache;
  private WorkerListener listener;
  private Channel channel;
  private ScheduledExecutorService scheduler;
  private EntryLifecycleImpl entryLifecycle;
  private StampClock stampClock;
  private DefaultWorkerContext context;

  @BeforeEach
  void setUp() throws IOException {
    cache = Caffeine.newBuilder().maximumSize(100).build();
    WorkerListenerProperties properties = new WorkerListenerProperties();
    properties.setJitterMs(0);
    scheduler = Executors.newSingleThreadScheduledExecutor();
    ZetaProperties ttlConfig = new ZetaProperties();
    entryLifecycle = new EntryLifecycleImpl(cache, ttlConfig, null);
    stampClock = org.mockito.Mockito.mock(StampClock.class);
    org.mockito.Mockito.when(stampClock.nextStamp()).thenReturn(1L);
    org.mockito.Mockito.when(stampClock.nextStamps(org.mockito.ArgumentMatchers.anyInt())).thenReturn(1L);
    WorkerPolicy policy = newPolicy(k -> "refreshed");
    listener = new WorkerListener(properties, scheduler, policy, context, null);
    channel = mock(Channel.class);
  }

  @AfterEach
  void resetClock() {
    // Clear any leaked TimeSource offset so another test class cannot skew this listener's
    // absolute-expiry arithmetic.
    TimeSource.setTimeOffsetForTest(0, 0);
  }

  private WorkerPolicy newPolicy(CacheLoader loader) {
    context = new DefaultWorkerContext(cache, loader, entryLifecycle, stampClock, Collections.emptyList());
    return new WorkerPolicy() {};
  }

  private void awaitWorkerTasks() throws InterruptedException {
    awaitWorkerTasks(scheduler);
  }

  /**
   * Waits for two tasks to drain on the given scheduler. The appName-isolated tests build their
   * listener on a dedicated {@code sched} executor, so they must wait on {@code sched} — not the
   * shared {@code scheduler} field — otherwise the wait returns before the listener's dispatcher
   * task (running on {@code sched}) has actually mutated the cache (race → NPE on a null entry).
   */
  private void awaitWorkerTasks(ScheduledExecutorService sched) throws InterruptedException {
    CountDownLatch phase1 = new CountDownLatch(1);
    CountDownLatch phase2 = new CountDownLatch(1);
    sched.execute(() -> phase1.countDown());
    assertThat(phase1.await(5, TimeUnit.SECONDS)).isTrue();
    sched.execute(() -> phase2.countDown());
    assertThat(phase2.await(5, TimeUnit.SECONDS)).isTrue();
  }

  /**
   * Verifies that a HOT worker decision acknowledges and promotes the key to HOT status.
   */
  @Test
  void onMessage_hot_shouldAckAndPromote() throws IOException, InterruptedException {
    listener.onMessage(channel, wireMessage("key1", WorkerType.HOT.code(), 1L));
    verify(channel).basicAck(anyLong(), anyBoolean());
    awaitWorkerTasks();
    assertThat(cache.getIfPresent("key1")).satisfies(o -> {
      CacheEntry ce = (CacheEntry) o;
      assertThat(ce.getKeyState()).isEqualTo(KeyState.HOT);
      assertThat(ce.getDecisionVersion()).isEqualTo(1L);
      assertThat(ce.getValue()).isEqualTo("refreshed");
      assertThat(ce.getSoftExpireAtMs()).isPositive();
    });
  }

  /**
   * Verifies that a COOL worker decision acknowledges and downgrades the key to COOL status.
   */
  @Test
  void onMessage_cool_shouldAckAndDowngrade() throws IOException, InterruptedException {
    cache.put("key1", hotEntry());
    listener.onMessage(channel, wireMessage("key1", WorkerType.COOL.code(), 2L));
    verify(channel).basicAck(anyLong(), anyBoolean());
    awaitWorkerTasks();
    assertThat(cache.getIfPresent("key1")).satisfies(o -> {
      CacheEntry ce = (CacheEntry) o;
      assertThat(ce.getKeyState()).isEqualTo(KeyState.COOL);
      assertThat(ce.getDecisionVersion()).isEqualTo(2L);
      assertThat(ce.getSoftExpireAtMs()).isPositive();
    });
  }

  /**
   * A consecutive COOL run is reduced to the decision that survives the version guard; the
   * policy sees one effective COOL, not one call per queued message.
   */
  @Test
  void dispatchBatch_shouldReduceACoolRunToItsEffectiveDecision() throws Exception {
    WorkerPolicy batchPolicy = mock(WorkerPolicy.class);
    CountDownLatch coolSeen = new CountDownLatch(1);
    AtomicReference<WorkerDecision> captured = new AtomicReference<>();
    doAnswer(invocation -> {
      captured.set(invocation.getArgument(0));
      coolSeen.countDown();
      return true;
    })
      .when(batchPolicy)
      .cool(any());

    WorkerListenerProperties properties = new WorkerListenerProperties();
    properties.setJitterMs(0);
    WorkerListener batchListener = new WorkerListener(properties, scheduler, batchPolicy, context, null);

    // Keep the single scheduler thread busy so both submissions land in the same key worker
    // before its first cycle grants; this makes the run deterministic instead of timing-based.
    CountDownLatch release = new CountDownLatch(1);
    scheduler.execute(() -> {
      try {
        release.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    Message cool1Wire = wireMessage("key1", WorkerType.COOL.code(), 2L);
    Message cool2Wire = wireMessage("key1", WorkerType.COOL.code(), 3L);
    WorkerDecision cool1 = WorkerDecision.from(cool1Wire);
    WorkerDecision cool2 = WorkerDecision.from(cool2Wire);
    batchListener.onMessage(channel, cool1Wire);
    batchListener.onMessage(channel, cool2Wire);
    release.countDown();

    assertThat(coolSeen.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(captured.get()).isEqualTo(cool2);
    verify(batchPolicy).cool(eq(cool2));
    verify(batchPolicy, never()).cool(eq(cool1));
    verify(batchPolicy, never()).hot(any(), any());
  }

  /**
   * A COOL run followed by a HOT must still deliver the HOT after the run; the listener
   * must advance past the run instead of dropping the tail of the batch.
   */
  @Test
  void dispatchBatch_shouldContinueWithHotAfterCoolRun() throws Exception {
    WorkerPolicy batchPolicy = mock(WorkerPolicy.class);
    CountDownLatch bothCalls = new CountDownLatch(2);
    AtomicReference<WorkerDecision> capturedCool = new AtomicReference<>();
    doAnswer(invocation -> {
      capturedCool.set(invocation.getArgument(0));
      bothCalls.countDown();
      return true;
    })
      .when(batchPolicy)
      .cool(any());
    doAnswer(invocation -> {
      bothCalls.countDown();
      return new HotAction.Promote(invocation.getArgument(1));
    })
      .when(batchPolicy)
      .hot(any(), any());

    WorkerListenerProperties properties = new WorkerListenerProperties();
    properties.setJitterMs(0);
    WorkerListener batchListener = new WorkerListener(properties, scheduler, batchPolicy, context, null);

    CountDownLatch release = new CountDownLatch(1);
    scheduler.execute(() -> {
      try {
        release.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    Message cool1Wire = wireMessage("key1", WorkerType.COOL.code(), 2L);
    Message cool2Wire = wireMessage("key1", WorkerType.COOL.code(), 3L);
    Message hotWire = wireMessage("key1", WorkerType.HOT.code(), 4L);
    WorkerDecision cool1 = WorkerDecision.from(cool1Wire);
    WorkerDecision cool2 = WorkerDecision.from(cool2Wire);
    WorkerDecision hot = WorkerDecision.from(hotWire);
    batchListener.onMessage(channel, cool1Wire);
    batchListener.onMessage(channel, cool2Wire);
    batchListener.onMessage(channel, hotWire);
    release.countDown();

    assertThat(bothCalls.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(capturedCool.get()).isEqualTo(cool2);
    verify(batchPolicy).hot(eq(hot), any());
    verify(batchPolicy).cool(eq(cool2));
    verify(batchPolicy, never()).cool(eq(cool1));
  }

  /**
   * An ACK failure is not a processing failure: the decision WAS handled, the
   * channel is broken. The exception propagates to the container and no NACK is
   * sent on the same already-closed channel.
   */
  @Test
  void onMessage_ackFailure_shouldPropagateWithoutNack() throws IOException {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, "HOT");
    Message msg = new Message("key1".getBytes(StandardCharsets.UTF_8), props);
    doThrow(new RuntimeException("forced error")).when(channel).basicAck(anyLong(), anyBoolean());
    assertThatThrownBy(() -> listener.onMessage(channel, msg))
      .isInstanceOf(RuntimeException.class)
      .hasMessageContaining("forced error");
    verify(channel, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
  }

  private static Message wireMessage(String key, String type, long version) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, type);
    props.setHeader(HEADER_VERSION, version);
    return new Message(key.getBytes(StandardCharsets.UTF_8), props);
  }

  /**
   * Same shape as {@link #wireMessage}, but the type header carries a fresh String
   * instance instead of the compile-time constant — what Spring AMQP hands the
   * listener for a message that came off the wire.
   */
  private static Message workerMessageOffWire(String key, String type, long version) {
    return wireMessage(key, new String(type.toCharArray()), version);
  }

  /**
   * Verifies that a HOT decision with no source value and no existing entry returns without promoting.
   */
  @Test
  void onMessage_hot_withNoSourceValueAndNoEntry_shouldReturn() throws IOException {
    WorkerListenerProperties props = new WorkerListenerProperties();
    props.setJitterMs(0);
    ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
    WorkerPolicy h = newPolicy(k -> null);
    WorkerListener nullLoader = new WorkerListener(props, sched, h, context, null);

    nullLoader.onMessage(channel, wireMessage("missing", WorkerType.HOT.code(), 1L));
    verify(channel).basicAck(anyLong(), anyBoolean());
    assertThat(cache.getIfPresent("missing")).isNull();
  }

  /**
   * Verifies that a HOT decision with a source exception and an existing entry falls back to its value.
   */
  @Test
  void onMessage_hot_withSourceExceptionAndExistingEntry_shouldUseIt() throws IOException, InterruptedException {
    cache.put(
      "key1",
      EntryDraft.blank(null)
        .value("fallback-val")
        .version(10)
        .decision(DecisionStamp.hot(1, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 30_000)
        .keyState(KeyState.NORMAL)
        .build()
    );

    WorkerListenerProperties props = new WorkerListenerProperties();
    props.setJitterMs(0);
    ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
    WorkerPolicy h = newPolicy(k -> {
      throw new RuntimeException("source down");
    });
    WorkerListener failingLoader = new WorkerListener(props, sched, h, context, null);

    failingLoader.onMessage(channel, wireMessage("key1", WorkerType.HOT.code(), 2L));
    verify(channel).basicAck(anyLong(), anyBoolean());
    awaitWorkerTasks(sched);
    sched.shutdown();
    assertThat(cache.getIfPresent("key1")).satisfies(o -> {
      CacheEntry ce = (CacheEntry) o;
      assertThat(ce.getValue()).isEqualTo("fallback-val");
      assertThat(ce.getKeyState()).isEqualTo(KeyState.HOT);
    });
  }

  /**
   * Verifies that a HOT decision with stale decision version is skipped via version guard.
   */
  @Test
  void onMessage_hot_withStaleDecisionVersion_shouldSkip() throws IOException, InterruptedException {
    cache.put("key1", hotEntry());
    listener.onMessage(channel, wireMessage("key1", WorkerType.HOT.code(), 1L));
    verify(channel).basicAck(anyLong(), anyBoolean());
    awaitWorkerTasks();
    assertThat(((CacheEntry) cache.getIfPresent("key1")).getDecisionVersion()).isEqualTo(1);
  }

  /**
   * Verifies that a COOL decision with no existing entry is a no-op.
   */
  @Test
  void onMessage_cool_withNoExistingEntry_shouldBeNoOp() throws IOException, InterruptedException {
    listener.onMessage(channel, wireMessage("nonexistent", WorkerType.COOL.code(), 2L));
    verify(channel).basicAck(anyLong(), anyBoolean());
    awaitWorkerTasks();
    assertThat(cache.getIfPresent("nonexistent")).isNull();
  }

  /**
   * Verifies that a COOL decision with stale decision version is skipped.
   */
  @Test
  void onMessage_cool_withStaleDecisionVersion_shouldSkip() throws IOException, InterruptedException {
    cache.put("key1", hotEntry());
    listener.onMessage(channel, wireMessage("key1", WorkerType.COOL.code(), 0L));
    verify(channel).basicAck(anyLong(), anyBoolean());
    awaitWorkerTasks();
    assertThat(((CacheEntry) cache.getIfPresent("key1")).getKeyState()).isEqualTo(KeyState.HOT);
  }

  /**
   * Verifies the dispatch-rejection fallback: a COOL the ordered dispatcher
   * refuses is applied instead of being lost — the entry still downgrades to COOL,
   * and the rejection is counted. (Closing the dispatcher via
   * {@link WorkerListener#destroy} makes every submit refused; the inline apply is
   * synchronous, so no scheduler await is needed.)
   *
   * <p>The type header is a <em>fresh</em> String, not the {@code TYPE_COOL}
   * constant: that is what a message deserialized off the wire carries (AMQP
   * headers are never interned).
   */
  @Test
  void onMessage_cool_dispatchRejected_shouldApplyInline() throws IOException {
    cache.put("key1", hotEntry());
    listener.destroy();
    listener.onMessage(channel, workerMessageOffWire("key1", WorkerType.COOL.code(), 2L));
    verify(channel).basicAck(anyLong(), anyBoolean());
    assertThat(cache.getIfPresent("key1")).satisfies(o -> {
      CacheEntry ce = (CacheEntry) o;
      assertThat(ce.getKeyState()).isEqualTo(KeyState.COOL);
      assertThat(ce.getDecisionVersion()).isEqualTo(2L);
    });
    assertThat(listener.coolDispatchRejections()).isEqualTo(1L);
  }

  /**
   * A COOL is a control signal: it bypasses the global pending budget, so a
   * PUT/REFRESH storm that exhausted the payload budget cannot lock the
   * decision out. The per-key queue still orders it.
   */
  @Test
  void onMessage_cool_budgetExhausted_shouldBypassTheGlobalBudget() throws Exception {
    WorkerListenerProperties props = new WorkerListenerProperties();
    props.setJitterMs(0);
    props.setMaxPendingUnits(0);
    ScheduledExecutorService sched = Executors.newScheduledThreadPool(2);
    WorkerListener starving = new WorkerListener(props, sched, newPolicy(k -> "v"), context, null);

    cache.put("key1", hotEntry());
    starving.onMessage(channel, workerMessageOffWire("key1", WorkerType.COOL.code(), 2L));
    verify(channel).basicAck(anyLong(), anyBoolean());

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      CacheEntry current = (CacheEntry) cache.getIfPresent("key1");
      if (current != null && current.getKeyState() == KeyState.COOL) {
        break;
      }
      Thread.sleep(20);
    }
    assertThat(((CacheEntry) cache.getIfPresent("key1")).getKeyState())
      .as("the control signal bypasses the exhausted payload budget")
      .isEqualTo(KeyState.COOL);
    assertThat(starving.coolDispatchRejections()).as("accepted, not refused").isZero();
    sched.shutdownNow();
  }

  /**
   * Verifies that a COOL decision on an existing entry is accepted and downgrades.
   */
  @Test
  void onMessage_cool_onExistingEntry_shouldDowngrade() throws IOException, InterruptedException {
    cache.put(
      "key1",
      EntryDraft.blank(null)
        .value("v")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 30_000)
        .keyState(KeyState.NORMAL)
        .build()
    );
    listener.onMessage(channel, wireMessage("key1", WorkerType.COOL.code(), 1L));
    verify(channel).basicAck(anyLong(), anyBoolean());
    awaitWorkerTasks();
    assertThat(cache.getIfPresent("key1")).satisfies(o -> {
      CacheEntry ce = (CacheEntry) o;
      assertThat(ce.getKeyState()).isEqualTo(KeyState.COOL);
      assertThat(ce.getDecisionVersion()).isEqualTo(1L);
      assertThat(ce.getSoftExpireAtMs()).isPositive();
    });
  }

  /**
   * Verifies that an empty body worker message is acknowledged without cache interaction.
   */
  @Test
  void onMessage_emptyBody_shouldAck() throws IOException {
    Message msg = new Message(new byte[0], new MessageProperties());
    listener.onMessage(channel, msg);
    verify(channel).basicAck(anyLong(), anyBoolean());
  }

  /**
   * Verifies that a null type header is handled gracefully.
   */
  @Test
  void onMessage_nullType_shouldAck() throws IOException {
    MessageProperties props = new MessageProperties();
    Message msg = new Message("key1".getBytes(StandardCharsets.UTF_8), props);
    listener.onMessage(channel, msg);
    verify(channel).basicAck(anyLong(), anyBoolean());
  }

  /**
   * Verifies that an unknown worker message type is acknowledged and ignored.
   */
  @Test
  void onMessage_unknownType_shouldAck() throws IOException {
    Message msg = wireMessage("key1", "UNKNOWN", 1L);
    listener.onMessage(channel, msg);
    verify(channel).basicAck(anyLong(), anyBoolean());
  }

  // ── P0-2: decisionNodeId/decisionEpoch propagation ──

  /**
   * Verifies that handleHot propagates nodeId and epoch from WorkerDecision into CacheEntry.
   */
  @Test
  void onMessage_hot_shouldSetDecisionNodeIdAndEpoch() throws IOException, InterruptedException {
    cache.put("key1", normalEntry());
    listener.onMessage(channel, workerMessageWithNodeIdEpoch("key1", WorkerType.HOT.code(), 2L, "worker-A", 3L));
    verify(channel).basicAck(anyLong(), anyBoolean());
    awaitWorkerTasks();
    assertThat(cache.getIfPresent("key1")).satisfies(o -> {
      CacheEntry ce = (CacheEntry) o;
      assertThat(ce.getDecisionNodeId()).isEqualTo("worker-A");
      assertThat(ce.getDecisionEpoch()).isEqualTo(3L);
    });
  }

  /**
   * Verifies that handleCool propagates nodeId and epoch from WorkerDecision into CacheEntry.
   */
  @Test
  void onMessage_cool_shouldSetDecisionNodeIdAndEpoch() throws IOException, InterruptedException {
    cache.put("key1", hotEntry());
    listener.onMessage(channel, workerMessageWithNodeIdEpoch("key1", WorkerType.COOL.code(), 2L, "worker-B", 5L));
    verify(channel).basicAck(anyLong(), anyBoolean());
    awaitWorkerTasks();
    assertThat(cache.getIfPresent("key1")).satisfies(o -> {
      CacheEntry ce = (CacheEntry) o;
      assertThat(ce.getDecisionNodeId()).isEqualTo("worker-B");
      assertThat(ce.getDecisionEpoch()).isEqualTo(5L);
    });
  }

  // ── design decisions: sync messages: appName shared-broker isolation ──

  /**
   * Verifies that a decision carrying a foreign appName header is acknowledged but
   * never dispatched (no cache mutation) when the listener has a configured appName.
   */
  @Test
  void onMessage_foreignAppName_shouldAckButNotApply() throws IOException, InterruptedException {
    WorkerListenerProperties props = new WorkerListenerProperties();
    props.setJitterMs(0);
    ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
    WorkerListener isolated = new WorkerListener(props, sched, newPolicy(k -> "v"), context, "appA");

    isolated.onMessage(channel, workerMessageWithAppName("key1", WorkerType.HOT.code(), 2L, "appB"));
    verify(channel).basicAck(anyLong(), anyBoolean());
    awaitWorkerTasks(sched);
    assertThat(cache.getIfPresent("key1")).isNull();
    sched.shutdown();
  }

  /**
   * Verifies that a decision whose appName header matches the listener's appName is
   * applied normally.
   */
  @Test
  void onMessage_matchingAppName_shouldApply() throws IOException, InterruptedException {
    WorkerListenerProperties props = new WorkerListenerProperties();
    props.setJitterMs(0);
    ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
    WorkerListener isolated = new WorkerListener(props, sched, newPolicy(k -> "v"), context, "appA");

    isolated.onMessage(channel, workerMessageWithAppName("key1", WorkerType.HOT.code(), 2L, "appA"));
    verify(channel).basicAck(anyLong(), anyBoolean());
    awaitWorkerTasks(sched);
    assertThat(((CacheEntry) cache.getIfPresent("key1")).getKeyState()).isEqualTo(KeyState.HOT);
    sched.shutdown();
  }

  /**
   * Verifies rolling-upgrade compatibility: a decision without the appName header
   * (pre-0068 Worker) is processed even when the listener has a configured appName.
   */
  @Test
  void onMessage_absentAppNameHeader_shouldApply() throws IOException, InterruptedException {
    WorkerListenerProperties props = new WorkerListenerProperties();
    props.setJitterMs(0);
    ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
    WorkerListener isolated = new WorkerListener(props, sched, newPolicy(k -> "v"), context, "appA");

    isolated.onMessage(channel, wireMessage("key1", WorkerType.HOT.code(), 2L));
    verify(channel).basicAck(anyLong(), anyBoolean());
    awaitWorkerTasks(sched);
    assertThat(((CacheEntry) cache.getIfPresent("key1")).getKeyState()).isEqualTo(KeyState.HOT);
    sched.shutdown();
  }

  private static Message workerMessageWithAppName(String key, String type, long version, String appName) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, type);
    props.setHeader(HEADER_VERSION, version);
    props.setHeader(HEADER_APP_NAME, appName);
    return new Message(key.getBytes(StandardCharsets.UTF_8), props);
  }

  /**
   * A foreign-app drop is counted (not just DEBUG-logged): a misconfigured
   * app-name discards 100% of decisions while the process looks healthy, so
   * the mismatch needs a counter plus last-sender signal for the actuator
   * endpoint. Matching-app and headerless messages never add.
   */
  @Test
  void foreignAppDrops_shouldCountOnlyMismatches() throws IOException, InterruptedException {
    WorkerListenerProperties props = new WorkerListenerProperties();
    props.setJitterMs(0);
    ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
    WorkerListener isolated = new WorkerListener(props, sched, newPolicy(k -> "v"), context, "appA");

    assertThat(isolated.foreignAppDrops()).isZero();
    assertThat(isolated.lastForeignApp()).isNull();

    isolated.onMessage(channel, workerMessageWithAppName("k1", WorkerType.HOT.code(), 2L, "appB"));
    isolated.onMessage(channel, workerMessageWithAppName("k2", WorkerType.COOL.code(), 3L, "appB"));
    isolated.onMessage(channel, workerMessageWithAppName("k3", WorkerType.HOT.code(), 4L, "appA"));
    isolated.onMessage(channel, wireMessage("k4", WorkerType.HOT.code(), 5L));
    awaitWorkerTasks(sched);

    assertThat(isolated.foreignAppDrops()).isEqualTo(2);
    assertThat(isolated.lastForeignApp()).isEqualTo("appB");
    sched.shutdown();
  }

  private static Message workerMessageWithNodeIdEpoch(
    String key,
    String type,
    long version,
    String nodeId,
    long epoch
  ) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, type);
    props.setHeader(HEADER_VERSION, version);
    props.setHeader(HEADER_NODE_ID, nodeId);
    props.setHeader(HEADER_EPOCH, epoch);
    return new Message(key.getBytes(StandardCharsets.UTF_8), props);
  }

  private static CacheEntry hotEntry() {
    return EntryDraft.blank(null)
      .value("v")
      .version(5)
      .decision(DecisionStamp.hot(1, null, 0L))
      .ttl(3_600_000, 300_000, 300_000, 30_000)
      .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 300_000)
      .keyState(KeyState.HOT)
      .build();
  }

  private static CacheEntry normalEntry() {
    return EntryDraft.blank(null)
      .value("n")
      .version(1)
      .decision(DecisionStamp.hot(0, null, 0L))
      .ttl(300_000, 30_000, 300_000, 30_000)
      .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 30_000)
      .keyState(KeyState.NORMAL)
      .build();
  }
}
