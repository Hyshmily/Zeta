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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.rabbitmq.client.Channel;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.impl.ExpireManagerImpl;
import io.github.hyshmily.zeta.cache.loader.CacheLoader;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.sync.worker.WorkerListener;
import io.github.hyshmily.zeta.sync.worker.WorkerListenerProperties;
import io.github.hyshmily.zeta.sync.worker.WorkerMessage;
import io.github.hyshmily.zeta.util.ratelimit.impl.SreRateLimiterImpl;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
  private ScheduledExecutorService scheduler;

  @BeforeEach
  void setUp() throws IOException {
    cache = Caffeine.newBuilder().maximumSize(100).build();
    CacheLoader redisLoader = k -> "refreshed";
    WorkerListenerProperties properties = new WorkerListenerProperties();
    properties.setWarmupJitterMs(0);
    scheduler = Executors.newSingleThreadScheduledExecutor();
    ZetaProperties ttlConfig = new ZetaProperties();
    ExpireManagerImpl expireManager = new ExpireManagerImpl(cache, Runnable::run, ttlConfig, 10);
    listener = new WorkerListener(cache, redisLoader, properties, scheduler, expireManager, null);
    listener.init();
    channel = mock(Channel.class);
  }

  private void awaitWorkerTasks() throws InterruptedException {
    CountDownLatch phase1 = new CountDownLatch(1);
    CountDownLatch phase2 = new CountDownLatch(1);
    scheduler.execute(() -> phase1.countDown());
    assertThat(phase1.await(5, TimeUnit.SECONDS)).isTrue();
    scheduler.execute(() -> phase2.countDown());
    assertThat(phase2.await(5, TimeUnit.SECONDS)).isTrue();
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
  void handleWorkerMessage_cool_shouldAckAndDowngrade() throws IOException, InterruptedException {
    cache.put("key1", hotEntry());
    listener.handleWorkerMessage(channel, workerMessage("key1", WorkerMessage.TYPE_COOL, 2L));
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
   * Verifies that a NACK is sent when an error occurs during worker message processing.
   */
  @Test
  void handleWorkerMessage_shouldNackOnError() throws IOException {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, "HOT");
    Message msg = new Message("key1".getBytes(StandardCharsets.UTF_8), props);
    doThrow(new RuntimeException("forced error")).when(channel).basicAck(anyLong(), anyBoolean());
    listener.handleWorkerMessage(channel, msg);
    verify(channel).basicNack(anyLong(), anyBoolean(), anyBoolean());
  }

  private static Message workerMessage(String key, String type, long version) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, type);
    props.setHeader(HEADER_VERSION, version);
    return new Message(key.getBytes(StandardCharsets.UTF_8), props);
  }

  /**
   * Verifies that a HOT decision with SRE rate limiter rejecting is skipped.
   */
  @Test
  void handleWorkerMessage_hot_withSreThrottling_shouldSkip() throws IOException, InterruptedException {
    SreRateLimiterImpl limiter = mock(SreRateLimiterImpl.class);
    when(limiter.tryAcquire()).thenReturn(false);
    WorkerListenerProperties props = new WorkerListenerProperties();
    props.setWarmupJitterMs(0);
    ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
    ZetaProperties ttlConfig = new ZetaProperties();
    ExpireManagerImpl expireManager = new ExpireManagerImpl(cache, Runnable::run, ttlConfig, 10);
    WorkerListener throttled = new WorkerListener(cache, k -> "v", props, sched, expireManager, limiter);
    throttled.init();

    cache.put("key1", hotEntry());
    throttled.handleWorkerMessage(channel, workerMessage("key1", WorkerMessage.TYPE_HOT, 2L));
    verify(channel).basicAck(anyLong(), anyBoolean());
    {
      CountDownLatch phase1 = new CountDownLatch(1);
      CountDownLatch phase2 = new CountDownLatch(1);
      sched.execute(() -> phase1.countDown());
      assertThat(phase1.await(5, TimeUnit.SECONDS)).isTrue();
      sched.execute(() -> phase2.countDown());
      assertThat(phase2.await(5, TimeUnit.SECONDS)).isTrue();
      sched.shutdown();
    }
    verify(limiter, never()).onFailed();
    assertThat(((CacheEntry) cache.getIfPresent("key1")).getDecisionVersion()).isEqualTo(1);
  }

  /**
   * Verifies that a HOT decision with null Redis value and no degraded entry returns without promoting.
   */
  @Test
  void handleWorkerMessage_hot_withNullRedisValueNoDegradedEntry_shouldReturn() throws IOException {
    WorkerListenerProperties props = new WorkerListenerProperties();
    props.setWarmupJitterMs(0);
    ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
    ZetaProperties ttlConfig = new ZetaProperties();
    ExpireManagerImpl expireManager = new ExpireManagerImpl(cache, Runnable::run, ttlConfig, 10);
    WorkerListener nullLoader = new WorkerListener(cache, k -> null, props, sched, expireManager, null);
    nullLoader.init();

    nullLoader.handleWorkerMessage(channel, workerMessage("missing", WorkerMessage.TYPE_HOT, 1L));
    verify(channel).basicAck(anyLong(), anyBoolean());
    assertThat(cache.getIfPresent("missing")).isNull();
  }

  /**
   * Verifies that a HOT decision with Redis exception and an existing degraded entry falls back to the degraded value.
   */
  @Test
  void handleWorkerMessage_hot_withRedisExceptionAndDegradedEntry_shouldUseDegraded()
    throws IOException, InterruptedException {
    cache.put(
      "key1",
      CacheEntry.builder()
        .value("degraded-val")
        .dataVersion(-10)
        .isVersionDegraded(true)
        .decisionVersion(1)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() + 30_000)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );

    WorkerListenerProperties props = new WorkerListenerProperties();
    props.setWarmupJitterMs(0);
    ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
    ZetaProperties ttlConfig = new ZetaProperties();
    ExpireManagerImpl expireManager = new ExpireManagerImpl(cache, Runnable::run, ttlConfig, 10);
    WorkerListener failingLoader = new WorkerListener(
      cache,
      k -> {
        throw new RuntimeException("Redis down");
      },
      props,
      sched,
      expireManager,
      null
    );
    failingLoader.init();

    failingLoader.handleWorkerMessage(channel, workerMessage("key1", WorkerMessage.TYPE_HOT, 2L));
    verify(channel).basicAck(anyLong(), anyBoolean());
    {
      CountDownLatch phase1 = new CountDownLatch(1);
      CountDownLatch phase2 = new CountDownLatch(1);
      sched.execute(() -> phase1.countDown());
      assertThat(phase1.await(5, TimeUnit.SECONDS)).isTrue();
      sched.execute(() -> phase2.countDown());
      assertThat(phase2.await(5, TimeUnit.SECONDS)).isTrue();
      sched.shutdown();
    }
    assertThat(cache.getIfPresent("key1")).satisfies(o -> {
      CacheEntry ce = (CacheEntry) o;
      assertThat(ce.getValue()).isEqualTo("degraded-val");
      assertThat(ce.getKeyState()).isEqualTo(KeyState.HOT);
    });
  }

  /**
   * Verifies that a HOT decision with stale decision version is skipped via version guard.
   */
  @Test
  void handleWorkerMessage_hot_withStaleDecisionVersion_shouldSkip() throws IOException, InterruptedException {
    cache.put("key1", hotEntry());
    listener.handleWorkerMessage(channel, workerMessage("key1", WorkerMessage.TYPE_HOT, 1L));
    verify(channel).basicAck(anyLong(), anyBoolean());
    awaitWorkerTasks();
    assertThat(((CacheEntry) cache.getIfPresent("key1")).getDecisionVersion()).isEqualTo(1);
  }

  /**
   * Verifies that a COOL decision with no existing entry is a no-op.
   */
  @Test
  void handleWorkerMessage_cool_withNoExistingEntry_shouldBeNoOp() throws IOException, InterruptedException {
    listener.handleWorkerMessage(channel, workerMessage("nonexistent", WorkerMessage.TYPE_COOL, 2L));
    verify(channel).basicAck(anyLong(), anyBoolean());
    awaitWorkerTasks();
    assertThat(cache.getIfPresent("nonexistent")).isNull();
  }

  /**
   * Verifies that a COOL decision with stale decision version is skipped.
   */
  @Test
  void handleWorkerMessage_cool_withStaleDecisionVersion_shouldSkip() throws IOException, InterruptedException {
    cache.put("key1", hotEntry());
    listener.handleWorkerMessage(channel, workerMessage("key1", WorkerMessage.TYPE_COOL, 0L));
    verify(channel).basicAck(anyLong(), anyBoolean());
    awaitWorkerTasks();
    assertThat(((CacheEntry) cache.getIfPresent("key1")).getKeyState()).isEqualTo(KeyState.HOT);
  }

  /**
   * Verifies that a COOL decision on a degraded entry is accepted and downgrades.
   */
  @Test
  void handleWorkerMessage_cool_onDegradedEntry_shouldDowngrade() throws IOException, InterruptedException {
    cache.put(
      "key1",
      entry(0, true, 0)
        .toBuilder()
        .keyState(KeyState.NORMAL)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() + 30_000)
        .build()
    );
    listener.handleWorkerMessage(channel, workerMessage("key1", WorkerMessage.TYPE_COOL, 1L));
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
  void handleWorkerMessage_emptyBody_shouldAck() throws IOException {
    Message msg = new Message(new byte[0], new MessageProperties());
    listener.handleWorkerMessage(channel, msg);
    verify(channel).basicAck(anyLong(), anyBoolean());
  }

  /**
   * Verifies that a null type header is handled gracefully.
   */
  @Test
  void handleWorkerMessage_nullType_shouldAck() throws IOException {
    MessageProperties props = new MessageProperties();
    Message msg = new Message("key1".getBytes(StandardCharsets.UTF_8), props);
    listener.handleWorkerMessage(channel, msg);
    verify(channel).basicAck(anyLong(), anyBoolean());
  }

  /**
   * Verifies that an unknown worker message type is acknowledged and ignored.
   */
  @Test
  void handleWorkerMessage_unknownType_shouldAck() throws IOException {
    Message msg = workerMessage("key1", "UNKNOWN", 1L);
    listener.handleWorkerMessage(channel, msg);
    verify(channel).basicAck(anyLong(), anyBoolean());
  }

  /**
   * Verifies that a HOT decision on a normal entry promotes to HOT and calls SRE onSuccess.
   */
  @Test
  void handleWorkerMessage_hot_withSreSuccess_shouldCallOnSuccess() throws IOException, InterruptedException {
    SreRateLimiterImpl limiter = mock(SreRateLimiterImpl.class);
    when(limiter.tryAcquire()).thenReturn(true);
    WorkerListenerProperties props = new WorkerListenerProperties();
    props.setWarmupJitterMs(0);
    ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
    ZetaProperties ttlConfig = new ZetaProperties();
    ExpireManagerImpl expireManager = new ExpireManagerImpl(cache, Runnable::run, ttlConfig, 10);
    WorkerListener throttled = new WorkerListener(cache, k -> "fresh", props, sched, expireManager, limiter);
    throttled.init();

    cache.put("key1", entry(1, false, 0));
    throttled.handleWorkerMessage(channel, workerMessage("key1", WorkerMessage.TYPE_HOT, 2L));
    verify(channel).basicAck(anyLong(), anyBoolean());
    {
      CountDownLatch phase1 = new CountDownLatch(1);
      CountDownLatch phase2 = new CountDownLatch(1);
      sched.execute(() -> phase1.countDown());
      assertThat(phase1.await(5, TimeUnit.SECONDS)).isTrue();
      sched.execute(() -> phase2.countDown());
      assertThat(phase2.await(5, TimeUnit.SECONDS)).isTrue();
      sched.shutdown();
    }
    verify(limiter).onSuccess();
    assertThat(cache.getIfPresent("key1")).satisfies(o -> {
      CacheEntry ce = (CacheEntry) o;
      assertThat(ce.getKeyState()).isEqualTo(KeyState.HOT);
      assertThat(ce.getDecisionVersion()).isEqualTo(2L);
    });
  }

  // ── P0-2: decisionNodeId/decisionEpoch propagation ──

  /**
   * Verifies that handleHot propagates nodeId and epoch from WorkerMessage into CacheEntry.
   */
  @Test
  void handleWorkerMessage_hot_shouldSetDecisionNodeIdAndEpoch() throws IOException, InterruptedException {
    cache.put("key1", normalEntry());
    listener.handleWorkerMessage(
      channel,
      workerMessageWithNodeIdEpoch("key1", WorkerMessage.TYPE_HOT, 2L, "worker-A", 3L)
    );
    verify(channel).basicAck(anyLong(), anyBoolean());
    awaitWorkerTasks();
    assertThat(cache.getIfPresent("key1")).satisfies(o -> {
      CacheEntry ce = (CacheEntry) o;
      assertThat(ce.getDecisionNodeId()).isEqualTo("worker-A");
      assertThat(ce.getDecisionEpoch()).isEqualTo(3L);
    });
  }

  /**
   * Verifies that handleCool propagates nodeId and epoch from WorkerMessage into CacheEntry.
   */
  @Test
  void handleWorkerMessage_cool_shouldSetDecisionNodeIdAndEpoch() throws IOException, InterruptedException {
    cache.put("key1", hotEntry());
    listener.handleWorkerMessage(
      channel,
      workerMessageWithNodeIdEpoch("key1", WorkerMessage.TYPE_COOL, 2L, "worker-B", 5L)
    );
    verify(channel).basicAck(anyLong(), anyBoolean());
    awaitWorkerTasks();
    assertThat(cache.getIfPresent("key1")).satisfies(o -> {
      CacheEntry ce = (CacheEntry) o;
      assertThat(ce.getDecisionNodeId()).isEqualTo("worker-B");
      assertThat(ce.getDecisionEpoch()).isEqualTo(5L);
    });
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

  private static CacheEntry normalEntry() {
    return CacheEntry.builder()
      .value("n")
      .dataVersion(1)
      .isVersionDegraded(false)
      .decisionVersion(0)
      .hardTtlMs(300_000)
      .hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(30_000)
      .softExpireAtMs(System.currentTimeMillis() + 30_000)
      .keyState(KeyState.NORMAL)
      .normalHardTtlMs(300_000)
      .normalSoftTtlMs(30_000)
      .build();
  }
}
