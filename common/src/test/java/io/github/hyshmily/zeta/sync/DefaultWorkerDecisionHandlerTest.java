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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.annotation.annotationsupporter.NullValue;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.impl.ExpireManagerImpl;
import io.github.hyshmily.zeta.cache.loader.CacheLoader;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.sync.worker.DefaultWorkerDecisionHandler;
import io.github.hyshmily.zeta.sync.worker.HotSkipReason;
import io.github.hyshmily.zeta.sync.worker.WorkerDecisionHandler;
import io.github.hyshmily.zeta.sync.worker.WorkerDecisionHook;
import io.github.hyshmily.zeta.sync.worker.WorkerMessage;
import io.github.hyshmily.zeta.util.ratelimit.impl.SreRateLimiterImpl;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

class DefaultWorkerDecisionHandlerTest {

  private Cache<String, Object> cache;
  private DefaultWorkerDecisionHandler handler;
  private CacheLoader loader;
  private ExpireManagerImpl expireManager;

  @BeforeEach
  void setUp() {
    cache = Caffeine.newBuilder().maximumSize(100).build();
    ZetaProperties ttlConfig = new ZetaProperties();
    expireManager = new ExpireManagerImpl(cache, Runnable::run, ttlConfig, 10);
    loader = k -> "fresh";
    handler = new DefaultWorkerDecisionHandler(cache, loader, expireManager, null, null, Collections.emptyList());
  }

  private static WorkerMessage workerMessage(String key, String type, long dv) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, type);
    props.setHeader(HEADER_VERSION, dv);
    props.setHeader(HEADER_NODE_ID, "node");
    props.setHeader(HEADER_EPOCH, 1L);
    props.setHeader(HEADER_MESSAGE_ID, 1L);
    return WorkerMessage.from(new Message(key.getBytes(StandardCharsets.UTF_8), props));
  }

  private static CacheEntry entry(long dv, KeyState state) {
    return CacheEntry.builder()
      .value("v")
      .dataVersion(1)
      .isVersionDegraded(false)
      .decisionVersion(dv)
      .decisionNodeId("node")
      .decisionEpoch(1L)
      .hardTtlMs(300_000)
      .hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(30_000)
      .softExpireAtMs(30_000)
      .keyState(state)
      .normalHardTtlMs(300_000)
      .normalSoftTtlMs(30_000)
      .build();
  }

  @Test
  void handleHot_shouldInvokeAfterHotPromotionHook() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, loader, expireManager, null, null, List.of(hook));

    handler.handleHot(workerMessage("key1", WorkerMessage.TYPE_HOT, 2L));

    verify(hook).afterHotPromotion(eq("key1"), any(), any());
  }

  @Test
  void handleHot_sreThrottled_shouldInvokeOnHotSkippedSre() {
    SreRateLimiterImpl limiter = mock(SreRateLimiterImpl.class);
    when(limiter.tryAcquire()).thenReturn(false);
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, loader, expireManager, limiter, null, List.of(hook));

    handler.handleHot(workerMessage("key1", WorkerMessage.TYPE_HOT, 2L));

    verify(hook).onHotSkipped(eq("key1"), any(), eq(HotSkipReason.SRE_THROTTLED));
  }

  @Test
  void handleHot_staleVersion_shouldInvokeOnHotSkippedStale() {
    cache.put("key1", entry(5, KeyState.NORMAL));
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, loader, expireManager, null, null, List.of(hook));

    handler.handleHot(workerMessage("key1", WorkerMessage.TYPE_HOT, 3L));

    verify(hook).onHotSkipped(eq("key1"), any(), eq(HotSkipReason.VERSION_STALE));
  }

  @Test
  void handleHot_valueNotFound_shouldInvokeOnHotSkippedValueNotFound() {
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, k -> null, expireManager, null, null, List.of(hook));

    handler.handleHot(workerMessage("missing", WorkerMessage.TYPE_HOT, 1L));

    verify(hook).onHotSkipped(eq("missing"), any(), eq(HotSkipReason.VALUE_NOT_FOUND));
  }

  /**
   * Verifies the Redis-outage fallback (ADR-0008): when the loader fails and the
   * L1 entry is a normal (non-degraded) entry, its value is used for promotion.
   */
  @Test
  void handleHot_redisDown_shouldPromoteFromNormalL1Entry() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, k -> null, expireManager, null, null, List.of(hook));

    handler.handleHot(workerMessage("key1", WorkerMessage.TYPE_HOT, 2L));

    verify(hook).afterHotPromotion(eq("key1"), any(), any());
    CacheEntry promoted = (CacheEntry) cache.getIfPresent("key1");
    assertThat(promoted).isNotNull();
    assertThat(promoted.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(promoted.getValue()).isEqualTo("v");
  }

  /**
   * Verifies that the Redis-outage fallback still accepts degraded L1 entries.
   */
  @Test
  void handleHot_redisDown_shouldPromoteFromDegradedL1Entry() {
    CacheEntry degraded = entry(1, KeyState.NORMAL).toBuilder().isVersionDegraded(true).build();
    cache.put("key1", degraded);
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, k -> null, expireManager, null, null, List.of(hook));

    handler.handleHot(workerMessage("key1", WorkerMessage.TYPE_HOT, 2L));

    verify(hook).afterHotPromotion(eq("key1"), any(), any());
  }

  /**
   * Verifies that a {@link NullValue} sentinel is not granted the HOT TTL even
   * when Redis is down — there is nothing of value to promote.
   */
  @Test
  void handleHot_redisDown_shouldSkipNullValueSentinel() {
    CacheEntry nullEntry = entry(1, KeyState.NORMAL).toBuilder().value(NullValue.INSTANCE).build();
    cache.put("key1", nullEntry);
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, k -> null, expireManager, null, null, List.of(hook));

    handler.handleHot(workerMessage("key1", WorkerMessage.TYPE_HOT, 2L));

    verify(hook).onHotSkipped(eq("key1"), any(), eq(HotSkipReason.VALUE_NOT_FOUND));
    CacheEntry unchanged = (CacheEntry) cache.getIfPresent("key1");
    assertThat(unchanged.getKeyState()).isEqualTo(KeyState.NORMAL);
  }

  @Test
  void handleCool_shouldInvokeAfterCoolDowngradeHook() {
    cache.put("key1", entry(5, KeyState.HOT));
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, loader, expireManager, null, null, List.of(hook));

    handler.handleCool(workerMessage("key1", WorkerMessage.TYPE_COOL, 6L));

    verify(hook).afterCoolDowngrade(eq("key1"), any(), any());
  }

  @Test
  void handleCool_missingNormalTtl_shouldApplyFallbackProtectionTtlsInMs() {
    cache.put(
      "key1",
      CacheEntry.builder()
        .value("v")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(1)
        .decisionNodeId("node")
        .decisionEpoch(1L)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(30_000)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(0)
        .normalSoftTtlMs(0)
        .build()
    );

    handler.handleCool(workerMessage("key1", WorkerMessage.TYPE_COOL, 2L));

    CacheEntry ce = (CacheEntry) cache.getIfPresent("key1");
    assertThat(ce.getKeyState()).isEqualTo(KeyState.COOL);
    // Unit regression guard: the fallback protection TTLs are declared in
    // seconds (120/60) but must be applied in milliseconds (120000/60000).
    assertThat(ce.getHardTtlMs()).isEqualTo(120_000L);
    assertThat(ce.getSoftTtlMs()).isEqualTo(60_000L);
  }

  @Test
  void handleCool_noEntry_shouldInvokeOnCoolSkipped() {
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, loader, expireManager, null, null, List.of(hook));

    handler.handleCool(workerMessage("missing", WorkerMessage.TYPE_COOL, 1L));

    verify(hook).onCoolSkipped(eq("missing"), any());
  }

  @Test
  void multipleHooks_shouldAllBeInvoked() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    WorkerDecisionHook h1 = mock(WorkerDecisionHook.class);
    WorkerDecisionHook h2 = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, loader, expireManager, null, null, List.of(h1, h2));

    handler.handleHot(workerMessage("key1", WorkerMessage.TYPE_HOT, 2L));

    verify(h1).afterHotPromotion(eq("key1"), any(), any());
    verify(h2).afterHotPromotion(eq("key1"), any(), any());
  }

  @Test
  void hookException_shouldNotBreakOtherHooks() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    AtomicInteger count = new AtomicInteger(0);
    WorkerDecisionHook failingHook = new WorkerDecisionHook() {
      @Override
      public void afterHotPromotion(String k, WorkerMessage wm, CacheEntry e) {
        throw new RuntimeException("fail");
      }
    };
    WorkerDecisionHook countingHook = new WorkerDecisionHook() {
      @Override
      public void afterHotPromotion(String k, WorkerMessage wm, CacheEntry e) {
        count.incrementAndGet();
      }
    };
    handler = new DefaultWorkerDecisionHandler(
      cache,
      loader,
      expireManager,
      null,
      null,
      List.of(failingHook, countingHook)
    );

    handler.handleHot(workerMessage("key1", WorkerMessage.TYPE_HOT, 2L));

    assertThat(count.get()).isEqualTo(1);
  }

  @Test
  void customHandler_shouldReplaceDefaultBehavior() {
    WorkerDecisionHandler custom = new WorkerDecisionHandler() {
      @Override
      public void handleHot(WorkerMessage wm) {
        cache.put(wm.cacheKey(), "custom-hot");
      }

      @Override
      public void handleCool(WorkerMessage wm) {
        cache.put(wm.cacheKey(), "custom-cool");
      }
    };

    custom.handleHot(workerMessage("k", WorkerMessage.TYPE_HOT, 1L));
    assertThat(cache.getIfPresent("k")).isEqualTo("custom-hot");

    custom.handleCool(workerMessage("k", WorkerMessage.TYPE_COOL, 2L));
    assertThat(cache.getIfPresent("k")).isEqualTo("custom-cool");
  }
}
