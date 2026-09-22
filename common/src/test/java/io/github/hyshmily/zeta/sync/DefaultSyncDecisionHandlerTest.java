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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.CircuitBreaker;
import io.github.hyshmily.zeta.cache.cachesupport.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.impl.ExpireManagerImpl;
import io.github.hyshmily.zeta.cache.cachesupport.impl.SingleFlightImpl;
import io.github.hyshmily.zeta.cache.loader.CacheLoader;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.rule.RuleMatcher;
import io.github.hyshmily.zeta.sync.local.DefaultSyncDecisionHandler;
import io.github.hyshmily.zeta.sync.local.SyncDecisionHandler;
import io.github.hyshmily.zeta.sync.local.SyncHook;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

class DefaultSyncDecisionHandlerTest {

  private Cache<String, Object> cache;
  private DefaultSyncDecisionHandler handler;
  private CacheLoader loader;
  private ExpireManagerImpl expireManager;
  private RuleMatcher ruleMatcher;

  @BeforeEach
  void setUp() {
    cache = Caffeine.newBuilder().maximumSize(100).build();
    ZetaProperties ttlConfig = new ZetaProperties();
    expireManager = new ExpireManagerImpl(cache, Runnable::run, ttlConfig, 10);
    loader = k -> "refreshed";
    ruleMatcher = mock(RuleMatcher.class);
    handler = new DefaultSyncDecisionHandler(cache, loader, expireManager, ruleMatcher, Collections.emptyList());
  }

  private static SyncMessage syncMessage(String key, String type, long version, boolean degraded) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, type);
    props.setHeader(HEADER_VERSION, version);
    props.setHeader(HEADER_IS_VERSION_DEGRADED, degraded);
    props.setHeader(HEADER_MESSAGE_ID, 1L);
    return SyncMessage.from(new Message(key.getBytes(StandardCharsets.UTF_8), props));
  }

  private static CacheEntry entry(long dv, boolean degraded, KeyState state) {
    return CacheEntry.builder()
      .value("v")
      .dataVersion(dv)
      .isVersionDegraded(degraded)
      .decisionVersion(0)
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
  void handleRefresh_shouldInvokeAfterRefreshHook() {
    cache.put("key1", entry(1, false, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    handler = new DefaultSyncDecisionHandler(cache, loader, expireManager, ruleMatcher, List.of(hook));

    handler.handleRefresh(syncMessage("key1", SyncMessage.TYPE_REFRESH, 2L, false));

    verify(hook).afterRefresh(eq("key1"), any(), any());
  }

  /**
   * Verifies the REFRESH value-channel fallback (C2/ADR-0031): when no value
   * can be loaded from Redis (the default — nothing in Zeta writes the
   * cache-key namespace), the refresh falls back to a local invalidation so
   * the next read reloads through the type-safe application reader instead
   * of keeping a stale entry.
   */
  @Test
  void handleRefresh_noValueInRedis_shouldFallBackToLocalInvalidation() {
    cache.put("key1", entry(1, false, KeyState.NORMAL));
    CacheLoader nullLoader = k -> null;
    handler = new DefaultSyncDecisionHandler(cache, nullLoader, expireManager, ruleMatcher, Collections.emptyList());

    handler.handleRefresh(syncMessage("key1", SyncMessage.TYPE_REFRESH, 2L, false));

    assertThat(cache.getIfPresent("key1")).isNull();
  }

  /**
   * Verifies batch-tail amortization (ADR-0071): a REFRESH marked as non-final of its key's
   * dispatch batch is skipped without the Redis load, the L1 write, or the value hook — the
   * onRefreshSkipped hook fires instead so the "a REFRESH that was not applied" contract
   * stays symmetric.
   */
  @Test
  void handleRefresh_nonFinalOfBatch_shouldSkipLoadAndApply() {
    cache.put("key1", entry(1, false, KeyState.NORMAL));
    AtomicInteger loads = new AtomicInteger();
    SyncHook hook = mock(SyncHook.class);
    handler = new DefaultSyncDecisionHandler(cache, k -> {
      loads.incrementAndGet();
      return "refreshed";
    }, expireManager, ruleMatcher, List.of(hook));

    handler.handleRefresh(syncMessage("key1", SyncMessage.TYPE_REFRESH, 2L, false), false);

    assertThat(loads.get()).isZero();
    assertThat(cache.getIfPresent("key1")).isInstanceOf(CacheEntry.class);
    assertThat(((CacheEntry) cache.getIfPresent("key1")).getDataVersion()).isEqualTo(1L);
    verify(hook).onRefreshSkipped(eq("key1"), any());
    verify(hook, never()).afterRefresh(any(), any(), any());
  }

  /**
   * Verifies that the batch-final REFRESH (endOfBatch=true) runs the full load-and-apply
   * flow — the amortization must never strand the key on a stale value.
   */
  @Test
  void handleRefresh_finalOfBatch_shouldLoadAndApply() {
    cache.put("key1", entry(1, false, KeyState.NORMAL));
    AtomicInteger loads = new AtomicInteger();
    SyncHook hook = mock(SyncHook.class);
    handler = new DefaultSyncDecisionHandler(cache, k -> {
      loads.incrementAndGet();
      return "refreshed";
    }, expireManager, ruleMatcher, List.of(hook));

    handler.handleRefresh(syncMessage("key1", SyncMessage.TYPE_REFRESH, 2L, false), true);

    assertThat(loads.get()).isOne();
    assertThat(cache.getIfPresent("key1")).isInstanceOf(CacheEntry.class);
    assertThat(((CacheEntry) cache.getIfPresent("key1")).getDataVersion()).isEqualTo(2L);
    verify(hook).afterRefresh(eq("key1"), any(), any());
  }

  /**
   * Verifies that the single-argument {@code handleRefresh} entry point (used by custom
   * implementations and kept for source compatibility) always runs the full flow — it is
   * the {@code endOfBatch=true} path.
   */
  @Test
  void handleRefresh_singleArgEntry_shouldBehaveAsEndOfBatch() {
    cache.put("key1", entry(1, false, KeyState.NORMAL));
    AtomicInteger loads = new AtomicInteger();
    handler = new DefaultSyncDecisionHandler(cache, k -> {
      loads.incrementAndGet();
      return "refreshed";
    }, expireManager, ruleMatcher, Collections.emptyList());

    handler.handleRefresh(syncMessage("key1", SyncMessage.TYPE_REFRESH, 2L, false));

    assertThat(loads.get()).isOne();
    assertThat(((CacheEntry) cache.getIfPresent("key1")).getDataVersion()).isEqualTo(2L);
  }

  @Test
  void handleRefresh_staleVersion_shouldInvokeOnRefreshSkipped() {
    cache.put("key1", entry(5, false, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    handler = new DefaultSyncDecisionHandler(cache, loader, expireManager, ruleMatcher, List.of(hook));

    handler.handleRefresh(syncMessage("key1", SyncMessage.TYPE_REFRESH, 3L, false));

    verify(hook).onRefreshSkipped(eq("key1"), any());
  }

  /**
   * Verifies that a REFRESH rejected by the atomic second guard — here the invalidation
   * watermark (refresh version below a recorded INVALIDATE version) — fires
   * {@code onRefreshSkipped} and never {@code afterRefresh}. The old code read the
   * compute's return value, which on a rejection IS the existing entry, and fired the
   * success hook for a refresh that was not applied.
   */
  @Test
  void handleRefresh_blockedByInvalidationWatermark_shouldInvokeOnRefreshSkipped() {
    SyncHook hook = mock(SyncHook.class);
    handler = new DefaultSyncDecisionHandler(cache, loader, expireManager, ruleMatcher, List.of(hook));

    // INVALIDATE with version 5 records the watermark (the entry is removed).
    handler.handleLocalInvalidate(syncMessage("key1", SyncMessage.TYPE_INVALIDATE, 5L, false));
    assertThat(cache.getIfPresent("key1")).isNull();

    // REFRESH below the watermark passes the entry-absent pre-check but must be
    // rejected by the in-compute watermark guard. The invalidation watermark is
    // handler-instance state (the handler is a production singleton), so the
    // INVALIDATE and the REFRESH must run on the same instance for the guard to
    // see the watermark.
    handler.handleRefresh(syncMessage("key1", SyncMessage.TYPE_REFRESH, 3L, false));

    verify(hook).onRefreshSkipped(eq("key1"), any());
    verify(hook, never()).afterRefresh(eq("key1"), any(), any());
    assertThat(cache.getIfPresent("key1")).isNull();
  }

  @Test
  void handleRefresh_nullValue_shouldInvokeOnRefreshSkipped() {
    cache.put("key1", entry(1, false, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    handler = new DefaultSyncDecisionHandler(cache, k -> null, expireManager, ruleMatcher, List.of(hook));

    handler.handleRefresh(syncMessage("key1", SyncMessage.TYPE_REFRESH, 2L, false));

    verify(hook).onRefreshSkipped(eq("key1"), any());
  }

  @Test
  void handleLocalInvalidate_shouldInvokeAfterInvalidateHook() {
    cache.put("key1", entry(1, false, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    handler = new DefaultSyncDecisionHandler(cache, loader, expireManager, ruleMatcher, List.of(hook));

    handler.handleLocalInvalidate(syncMessage("key1", SyncMessage.TYPE_INVALIDATE, 2L, false));

    verify(hook).afterInvalidate(eq("key1"), any());
  }

  /**
   * Verifies that a version-less (unconditional) INVALIDATE still removes an
   * ordinary NORMAL entry regardless of its data version.
   */
  @Test
  void handleLocalInvalidate_unconditional_shouldRemoveNormalEntry() {
    cache.put("key1", entry(5, false, KeyState.NORMAL));

    handler.handleLocalInvalidate(syncMessage("key1", SyncMessage.TYPE_INVALIDATE, 0L, false));

    assertThat(cache.getIfPresent("key1")).isNull();
  }

  /**
   * Verifies that a version-less (unconditional) INVALIDATE cannot clear a
   * Worker-managed HOT entry — decision metadata and extended TTL survive.
   */
  @Test
  void handleLocalInvalidate_unconditional_shouldPreserveHotEntry() {
    cache.put("key1", entry(5, false, KeyState.HOT));

    handler.handleLocalInvalidate(syncMessage("key1", SyncMessage.TYPE_INVALIDATE, 0L, false));

    assertThat(cache.getIfPresent("key1")).isNotNull();
    assertThat(((CacheEntry) cache.getIfPresent("key1")).getKeyState()).isEqualTo(KeyState.HOT);
  }

  /**
   * Verifies that a version-less (unconditional) INVALIDATE cannot clear a
   * Worker-managed COOL entry.
   */
  @Test
  void handleLocalInvalidate_unconditional_shouldPreserveCoolEntry() {
    cache.put("key1", entry(5, false, KeyState.COOL));

    handler.handleLocalInvalidate(syncMessage("key1", SyncMessage.TYPE_INVALIDATE, 0L, false));

    assertThat(cache.getIfPresent("key1")).isNotNull();
    assertThat(((CacheEntry) cache.getIfPresent("key1")).getKeyState()).isEqualTo(KeyState.COOL);
  }

  /**
   * Verifies that a preserved (rejected) unconditional INVALIDATE does not fire
   * the after-invalidate hook — the entry was not actually removed, so a
   * {@code Long.MAX_VALUE} watermark must not block later REFRESH messages.
   */
  @Test
  void handleLocalInvalidate_unconditional_onWorkerManaged_shouldNotFireHook() {
    cache.put("key1", entry(5, false, KeyState.HOT));
    SyncHook hook = mock(SyncHook.class);
    handler = new DefaultSyncDecisionHandler(cache, loader, expireManager, ruleMatcher, List.of(hook));

    handler.handleLocalInvalidate(syncMessage("key1", SyncMessage.TYPE_INVALIDATE, 0L, false));

    verify(hook, never()).afterInvalidate(eq("key1"), any());
    assertThat(cache.getIfPresent("key1")).isNotNull();
  }

  /**
   * Verifies that a preserved unconditional INVALIDATE does not record the
   * invalidation watermark — a subsequent newer REFRESH must still apply.
   */
  @Test
  void handleLocalInvalidate_unconditional_preserved_shouldNotBlockLaterRefresh() {
    cache.put("key1", entry(5, false, KeyState.HOT));

    handler.handleLocalInvalidate(syncMessage("key1", SyncMessage.TYPE_INVALIDATE, 0L, false));
    handler.handleRefresh(syncMessage("key1", SyncMessage.TYPE_REFRESH, 6L, false));

    CacheEntry entry = (CacheEntry) cache.getIfPresent("key1");
    assertThat(entry).isNotNull();
    assertThat(entry.getDataVersion()).isEqualTo(6L);
    assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
  }

  @Test
  void handleRulesSync_shouldDelegateToRuleMatcher() {
    handler.handleRulesSync(syncMessage("rules-payload", SyncMessage.TYPE_RULES_SYNC, 0L, false));

    verify(ruleMatcher).syncRules("rules-payload", 0L);
  }

  /**
   * ADR-0066: an <b>equal</b>-version REFRESH must apply, not skip. ADR-0033's
   * probe-after-read can over-stamp an entry one write ahead of its data (v4 data
   * under a v5 stamp); the write's own REFRESH(5) is the only thing that heals it.
   * The plain sync matrix's {@code >=} equality skip would swallow the REFRESH and
   * pin the stale value until the next write or TTL.
   */
  @Test
  void handleRefresh_equalVersion_overstampedEntry_shouldApplyAndHeal() {
    cache.put("key1", entry(5, false, KeyState.NORMAL));
    CacheLoader freshLoader = k -> "fresh-v5";
    handler = new DefaultSyncDecisionHandler(cache, freshLoader, expireManager, ruleMatcher, Collections.emptyList());

    handler.handleRefresh(syncMessage("key1", SyncMessage.TYPE_REFRESH, 5L, false));

    CacheEntry healed = (CacheEntry) cache.getIfPresent("key1");
    assertThat(healed).isNotNull();
    assertThat(healed.getDataVersion()).isEqualTo(5L);
    assertThat(healed.getValue()).isEqualTo("fresh-v5");
  }

  /**
   * ADR-0066: the no-value fallback invalidation must not wipe a strictly newer
   * local write. A local putThrough landed v6 while a peer's REFRESH(5) was in
   * flight — the fallback (and the fast-path guard) must leave the v6 entry alone.
   */
  @Test
  void handleRefresh_fallback_strictlyNewerLocalWrite_shouldBePreserved() {
    cache.put("key1", entry(6, false, KeyState.NORMAL));
    CacheLoader nullLoader = k -> null;
    handler = new DefaultSyncDecisionHandler(cache, nullLoader, expireManager, ruleMatcher, Collections.emptyList());

    handler.handleRefresh(syncMessage("key1", SyncMessage.TYPE_REFRESH, 5L, false));

    CacheEntry preserved = (CacheEntry) cache.getIfPresent("key1");
    assertThat(preserved).isNotNull();
    assertThat(preserved.getDataVersion()).isEqualTo(6L);
  }

  /**
   * ADR-0066: the no-value fallback invalidation must not discard a Worker-managed
   * entry's decision stamp — parity with the version-less INVALIDATE path. The
   * entry expires at its hard TTL or the next Worker decision instead.
   */
  @Test
  void handleRefresh_fallback_shouldPreserveWorkerManagedEntry() {
    cache.put("key1", entry(1, false, KeyState.HOT));
    CacheLoader nullLoader = k -> null;
    handler = new DefaultSyncDecisionHandler(cache, nullLoader, expireManager, ruleMatcher, Collections.emptyList());

    handler.handleRefresh(syncMessage("key1", SyncMessage.TYPE_REFRESH, 2L, false));

    CacheEntry preserved = (CacheEntry) cache.getIfPresent("key1");
    assertThat(preserved).isNotNull();
    assertThat(preserved.getKeyState()).isEqualTo(KeyState.HOT);
  }

  /**
   * ADR-0066: the batch INVALIDATE_ALL receiver (legacy wire format) must preserve
   * Worker-managed HOT/COOL entries while removing ordinary ones — a version-less
   * batch carries no ordering information and must not discard decision metadata.
   */
  @Test
  void handleLocalInvalidateAll_shouldPreserveWorkerManagedEntries() {
    cache.put("hot", entry(5, false, KeyState.HOT));
    cache.put("cool", entry(5, false, KeyState.COOL));
    cache.put("normal", entry(5, false, KeyState.NORMAL));

    handler.handleLocalInvalidateAll(syncMessage("[\"hot\",\"cool\",\"normal\"]", SyncMessage.TYPE_INVALIDATE_ALL, 0L, false));

    assertThat(cache.getIfPresent("hot")).isNotNull();
    assertThat(((CacheEntry) cache.getIfPresent("hot")).getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(cache.getIfPresent("cool")).isNotNull();
    assertThat(((CacheEntry) cache.getIfPresent("cool")).getKeyState()).isEqualTo(KeyState.COOL);
    assertThat(cache.getIfPresent("normal")).isNull();
  }

  @Test
  void multipleHooks_shouldAllBeInvoked() {
    cache.put("key1", entry(1, false, KeyState.NORMAL));
    SyncHook h1 = mock(SyncHook.class);
    SyncHook h2 = mock(SyncHook.class);
    handler = new DefaultSyncDecisionHandler(cache, loader, expireManager, ruleMatcher, List.of(h1, h2));

    handler.handleRefresh(syncMessage("key1", SyncMessage.TYPE_REFRESH, 2L, false));

    verify(h1).afterRefresh(eq("key1"), any(), any());
    verify(h2).afterRefresh(eq("key1"), any(), any());
  }

  @Test
  void hookException_shouldNotBreakOtherHooks() {
    cache.put("key1", entry(1, false, KeyState.NORMAL));
    AtomicInteger count = new AtomicInteger(0);
    SyncHook failingHook = new SyncHook() {
      @Override public void afterRefresh(String k, SyncMessage sm, CacheEntry e) { throw new RuntimeException("fail"); }
    };
    SyncHook countingHook = new SyncHook() {
      @Override public void afterRefresh(String k, SyncMessage sm, CacheEntry e) { count.incrementAndGet(); }
    };
    handler = new DefaultSyncDecisionHandler(cache, loader, expireManager, ruleMatcher, List.of(failingHook, countingHook));

    handler.handleRefresh(syncMessage("key1", SyncMessage.TYPE_REFRESH, 2L, false));

    assertThat(count.get()).isEqualTo(1);
  }

  @Test
  void customHandler_shouldReplaceDefaultBehavior() {
    SyncDecisionHandler custom = new SyncDecisionHandler() {
      @Override public void handleRefresh(SyncMessage sm) {
        cache.put(sm.cacheKey(), "custom-refresh");
      }
      @Override public void handleLocalInvalidate(SyncMessage sm) {
        cache.put(sm.cacheKey(), "custom-invalidate");
      }
      @Override public void handleLocalInvalidateAll(SyncMessage sm) {
        cache.put("batch", "custom-batch");
      }
      @Override public void handleRulesSync(SyncMessage sm) {
        cache.put("rules", "custom-rules");
      }
    };

    custom.handleRefresh(syncMessage("k", SyncMessage.TYPE_REFRESH, 1L, false));
    assertThat(cache.getIfPresent("k")).isEqualTo("custom-refresh");

    custom.handleLocalInvalidate(syncMessage("k", SyncMessage.TYPE_INVALIDATE, 1L, false));
    assertThat(cache.getIfPresent("k")).isEqualTo("custom-invalidate");

    custom.handleLocalInvalidateAll(syncMessage("[]", SyncMessage.TYPE_INVALIDATE_ALL, 0L, false));
    assertThat(cache.getIfPresent("batch")).isEqualTo("custom-batch");

    custom.handleRulesSync(syncMessage("rules", SyncMessage.TYPE_RULES_SYNC, 0L, false));
    assertThat(cache.getIfPresent("rules")).isEqualTo("custom-rules");
  }

  /**
   * ADR-0067: an applied versioned INVALIDATE also drops the key's SingleFlight
   * dedup entry, so a post-removal miss re-invokes the reader instead of
   * replaying the completed pre-removal load result (which would re-cache the
   * pre-invalidation value).
   */
  @Test
  void handleLocalInvalidate_alsoInvalidatesDedupEntry() {
    CircuitBreaker breaker = mock(CircuitBreaker.class);
    when(breaker.isOpen()).thenReturn(false);
    when(breaker.allowRequest()).thenReturn(true);
    SingleFlight singleFlight = new SingleFlightImpl(1000, 5, 5, Runnable::run, breaker);
    handler = new DefaultSyncDecisionHandler(cache, loader, expireManager, ruleMatcher, Collections.emptyList(), singleFlight);

    AtomicInteger loads = new AtomicInteger();
    singleFlight.load("key1", () -> {
      loads.incrementAndGet();
      return "old";
    });
    cache.put("key1", entry(2, false, KeyState.NORMAL));

    handler.handleLocalInvalidate(syncMessage("key1", SyncMessage.TYPE_INVALIDATE, 5L, false));

    assertThat(cache.getIfPresent("key1")).isNull();
    java.util.Optional<Object> next = singleFlight.load("key1", () -> {
      loads.incrementAndGet();
      return "fresh";
    });
    assertThat(next).contains("fresh");
    assertThat(loads.get()).as("post-removal load must re-run the reader").isEqualTo(2);
  }

  /**
   * ADR-0067: the value-less REFRESH fallback removal also drops the dedup
   * entry — same contract as the versioned INVALIDATE path.
   */
  @Test
  void handleRefresh_valuelessFallback_alsoInvalidatesDedupEntry() {
    CircuitBreaker breaker = mock(CircuitBreaker.class);
    when(breaker.isOpen()).thenReturn(false);
    when(breaker.allowRequest()).thenReturn(true);
    SingleFlight singleFlight = new SingleFlightImpl(1000, 5, 5, Runnable::run, breaker);
    handler = new DefaultSyncDecisionHandler(cache, k -> null, expireManager, ruleMatcher, Collections.emptyList(), singleFlight);

    AtomicInteger loads = new AtomicInteger();
    singleFlight.load("key1", () -> {
      loads.incrementAndGet();
      return "old";
    });
    cache.put("key1", entry(2, false, KeyState.NORMAL));

    handler.handleRefresh(syncMessage("key1", SyncMessage.TYPE_REFRESH, 3L, false));

    assertThat(cache.getIfPresent("key1")).isNull();
    java.util.Optional<Object> next = singleFlight.load("key1", () -> {
      loads.incrementAndGet();
      return "fresh";
    });
    assertThat(next).contains("fresh");
    assertThat(loads.get()).isEqualTo(2);
  }

  /**
   * ADR-0067: a REFRESH must not re-enable a disabled soft TTL — an entry with
   * {@code softTtlMs=0} (the "no soft expire" convention) keeps
   * {@code softExpireAtMs=0} after the refresh instead of silently gaining the
   * configured default stale-while-revalidate window.
   */
  @Test
  void handleRefresh_preservesDisabledSoftTtl() {
    cache.put(
      "key1",
      CacheEntry.builder()
        .value("v")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(0)
        .softExpireAtMs(0)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build());

    handler.handleRefresh(syncMessage("key1", SyncMessage.TYPE_REFRESH, 2L, false));

    CacheEntry refreshed = (CacheEntry) cache.getIfPresent("key1");
    assertThat(refreshed).isNotNull();
    assertThat(refreshed.getDataVersion()).isEqualTo(2L);
    assertThat(refreshed.getValue()).isEqualTo("refreshed");
    assertThat(refreshed.getSoftTtlMs()).isEqualTo(0L);
    assertThat(refreshed.getSoftExpireAtMs()).isEqualTo(0L);
  }
}
