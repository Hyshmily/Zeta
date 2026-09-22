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
import io.github.hyshmily.zeta.cache.codec.Lz4CacheCompressor;
import io.github.hyshmily.zeta.cache.loader.CacheLoader;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.sync.worker.DefaultWorkerDecisionHandler;
import io.github.hyshmily.zeta.sync.worker.HotSkipReason;
import io.github.hyshmily.zeta.sync.worker.WorkerDecisionHandler;
import io.github.hyshmily.zeta.sync.worker.WorkerDecisionHook;
import io.github.hyshmily.zeta.sync.worker.WorkerMessage;
import io.github.hyshmily.zeta.util.ratelimit.impl.SreRateLimiterImpl;
import io.github.hyshmily.zeta.model.EntryDraft;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
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

  /**
   * Verifies the create path preserves the Worker decision identity (C1): an
   * entry created by a Worker HOT decision carries nodeId/epoch, so
   * VersionGuard can compare against out-of-order replayed decisions instead
   * of accepting them unconditionally.
   */
  @Test
  void handleHot_entryAbsent_shouldPreserveDecisionIdentity() {
    handler.handleHot(workerMessage("newkey", WorkerMessage.TYPE_HOT, 1L));

    CacheEntry ce = (CacheEntry) cache.getIfPresent("newkey");
    assertThat(ce).isNotNull();
    assertThat(ce.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(ce.getDecisionNodeId()).isEqualTo("node");
    assertThat(ce.getDecisionEpoch()).isEqualTo(1L);
    assertThat(ce.getDecisionVersion()).isEqualTo(1L);
  }

  /**
   * Verifies the C1 fix end-to-end: after a Worker-HOT create, a replayed
   * older decision from the same Worker (same epoch/nodeId, lower dv) is
   * rejected by the version guard instead of overwriting the entry.
   */
  @Test
  void handleHot_olderDecisionFromSameNode_shouldNotOverwriteCreatedEntry() {
    handler.handleHot(workerMessage("newkey", WorkerMessage.TYPE_HOT, 5L));
    CacheEntry before = (CacheEntry) cache.getIfPresent("newkey");
    assertThat(before.getDecisionVersion()).isEqualTo(5L);

    handler.handleHot(workerMessage("newkey", WorkerMessage.TYPE_HOT, 3L));

    CacheEntry after = (CacheEntry) cache.getIfPresent("newkey");
    assertThat(after.getDecisionVersion()).isEqualTo(5L);
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
   * Verifies that a clean Redis GET miss (loader returns null, no exception)
   * is NOT recorded as an SRE failure: onFailed() would deflate the success
   * ratio and make the limiter probabilistically drop legitimate HOT
   * promotions while Redis is perfectly healthy.
   */
  @Test
  void handleHot_cleanRedisMiss_shouldNotRecordSreFailure() {
    SreRateLimiterImpl limiter = mock(SreRateLimiterImpl.class);
    when(limiter.tryAcquire()).thenReturn(true);
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, k -> null, expireManager, limiter, null, List.of(hook));

    handler.handleHot(workerMessage("missing", WorkerMessage.TYPE_HOT, 1L));

    verify(hook).onHotSkipped(eq("missing"), any(), eq(HotSkipReason.VALUE_NOT_FOUND));
    verify(limiter, never()).onFailed();
    verify(limiter, never()).onSuccess();
  }

  /**
   * Verifies that a REAL Redis error (the loader throws) IS recorded as an SRE
   * failure when the promotion is aborted (no L1 fallback value exists) — only
   * genuine outages may drive the adaptive throttle.
   */
  @Test
  void handleHot_redisErrorAbortsPromotion_shouldRecordSreFailure() {
    SreRateLimiterImpl limiter = mock(SreRateLimiterImpl.class);
    when(limiter.tryAcquire()).thenReturn(true);
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    CacheLoader failingLoader = k -> {
      throw new RuntimeException("Redis down");
    };
    handler = new DefaultWorkerDecisionHandler(cache, failingLoader, expireManager, limiter, null, List.of(hook));

    handler.handleHot(workerMessage("missing", WorkerMessage.TYPE_HOT, 1L));

    verify(hook).onHotSkipped(eq("missing"), any(), eq(HotSkipReason.VALUE_NOT_FOUND));
    verify(limiter).onFailed();
    verify(limiter, never()).onSuccess();
  }

  /**
   * Verifies the afterHotPromotion gate (F4): when the inner DCL guard rejects
   * the promotion as stale (a newer decision landed during the Redis fetch),
   * neither the hook nor the SRE success may fire. The loader bumps the
   * entry's decisionVersion between the outer pre-check and the atomic
   * compute, deterministically reproducing the stale-rejection interleaving.
   */
  @Test
  void handleHot_innerGuardRejectsPromotion_shouldNotFireAfterHotPromotionHook() {
    SreRateLimiterImpl limiter = mock(SreRateLimiterImpl.class);
    when(limiter.tryAcquire()).thenReturn(true);
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    cache.put("key1", entry(1, KeyState.NORMAL));
    CacheLoader bumpingLoader = k -> {
      // Simulates a concurrent newer decision landing during the Redis fetch.
      cache.put("key1", entry(10, KeyState.NORMAL));
      return "fresh";
    };
    handler = new DefaultWorkerDecisionHandler(cache, bumpingLoader, expireManager, limiter, null, List.of(hook));

    handler.handleHot(workerMessage("key1", WorkerMessage.TYPE_HOT, 2L));

    verify(hook, never()).afterHotPromotion(any(), any(), any());
    verify(limiter, never()).onSuccess();
    // The newer decision is preserved — nothing was overwritten.
    assertThat(((CacheEntry) cache.getIfPresent("key1")).getDecisionVersion()).isEqualTo(10L);
  }

  /**
   * Verifies that the afterHotPromotion hook receives the compute's own return
   * value — the promoted entry carrying the new decision — rather than a racy
   * getIfPresent re-read that could observe a foreign entry.
   */
  @Test
  void handleHot_shouldPassPromotedEntryFromComputeToHook() {
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, loader, expireManager, null, null, List.of(hook));

    handler.handleHot(workerMessage("newkey", WorkerMessage.TYPE_HOT, 2L));

    ArgumentCaptor<CacheEntry> entryCaptor = ArgumentCaptor.forClass(CacheEntry.class);
    verify(hook).afterHotPromotion(eq("newkey"), any(), entryCaptor.capture());
    assertThat(entryCaptor.getValue().getDecisionVersion()).isEqualTo(2L);
    assertThat(entryCaptor.getValue().getKeyState()).isEqualTo(KeyState.HOT);
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
   * Regression guard against double compression: the L1 fallback value arrives
   * in its ALREADY-wrapped stored form (the zeta envelope — flag byte + LZ4
   * payload), so the promotion must store it as-is. Re-wrapping it would
   * double-compress the entry: the next read unwraps exactly one layer and
   * serves the inner zeta {@code byte[]} to the application instead of the
   * original value.
   */
  @Test
  void handleHot_redisDown_shouldNotReWrapStoredFallbackValue() {
    // The setUp ExpireManagerImpl uses CacheCompressor.NONE; the envelope form
    // only exists under the real LZ4 codec (ADR-0015).
    ExpireManagerImpl lz4Manager =
        new ExpireManagerImpl(cache, Runnable::run, new ZetaProperties(), 10, new Lz4CacheCompressor());
    String original = "zeta-fallback-value-".repeat(60); // ≥256 bytes — wrapped, not stored verbatim
    Object wrapped = lz4Manager.wrapValue(original);
    assertThat(wrapped).isInstanceOf(byte[].class);
    CacheEntry l1Entry = EntryDraft.of(entry(1, KeyState.NORMAL)).value(wrapped).build();
    cache.put("key1", l1Entry);
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, k -> null, lz4Manager, null, null, List.of(hook));

    handler.handleHot(workerMessage("key1", WorkerMessage.TYPE_HOT, 2L));

    verify(hook).afterHotPromotion(eq("key1"), any(), any());
    CacheEntry promoted = (CacheEntry) cache.getIfPresent("key1");
    assertThat(promoted).isNotNull();
    assertThat(promoted.getKeyState()).isEqualTo(KeyState.HOT);
    // The stored value must be the exact wrapped instance the L1 held —
    // byte-for-byte, no second envelope.
    assertThat(promoted.getValue()).isSameAs(wrapped);
  }

  /**
   * Verifies that the Redis-outage fallback still accepts degraded L1 entries.
   */
  @Test
  void handleHot_redisDown_shouldPromoteFromDegradedL1Entry() {
    CacheEntry degraded = entry(-1, KeyState.NORMAL);
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
    CacheEntry nullEntry = EntryDraft.of(entry(1, KeyState.NORMAL)).value(NullValue.INSTANCE).build();
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

  /**
   * ADR-0066: a hard-expired entry is dead — the next read invalidates and reloads
   * it (ADR-0034, the hard TTL is the absolute bound). Cooling it must NOT rewrite
   * the stale value with fresh normal TTLs (which would resurrect the data for a
   * full normal-TTL lifetime); the entry stays untouched and expires on schedule.
   */
  @Test
  void handleCool_logicallyExpiredEntry_shouldNotResurrect() {
    CacheEntry expired = CacheEntry.builder()
      .value("stale")
      .dataVersion(1)
      .isVersionDegraded(false)
      .decisionVersion(5)
      .decisionNodeId("node")
      .decisionEpoch(1L)
      .hardTtlMs(300_000)
      // Epoch start (ms wall clock) — long past, so isLogicallyExpired is true.
      .hardExpireAtMs(1L)
      .softTtlMs(30_000)
      .softExpireAtMs(1L)
      .keyState(KeyState.HOT)
      .normalHardTtlMs(300_000)
      .normalSoftTtlMs(30_000)
      .build();
    cache.put("key1", expired);
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, loader, expireManager, null, null, List.of(hook));

    handler.handleCool(workerMessage("key1", WorkerMessage.TYPE_COOL, 6L));

    CacheEntry after = (CacheEntry) cache.getIfPresent("key1");
    assertThat(after).isSameAs(expired);
    assertThat(after.getKeyState()).isEqualTo(KeyState.HOT);
    verify(hook).onCoolSkipped(eq("key1"), any());
    verify(hook, never()).afterCoolDowngrade(eq("key1"), any(), any());
  }

  /**
   * The expiry guard must not block a live entry: a valid HOT entry still cools
   * normally (regression guard for the ADR-0066 guard placement).
   */
  @Test
  void handleCool_liveEntry_shouldStillCool() {
    cache.put("key1", entry(5, KeyState.HOT));
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, loader, expireManager, null, null, List.of(hook));

    handler.handleCool(workerMessage("key1", WorkerMessage.TYPE_COOL, 6L));

    CacheEntry ce = (CacheEntry) cache.getIfPresent("key1");
    assertThat(ce.getKeyState()).isEqualTo(KeyState.COOL);
    verify(hook).afterCoolDowngrade(eq("key1"), any(), any());
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
