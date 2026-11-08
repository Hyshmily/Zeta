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
import static org.mockito.Mockito.*;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycleImpl;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.sharding.ConsistentHashRing;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.worker.DefaultWorkerDecisionHandler;
import io.github.hyshmily.zeta.sync.worker.HotSkipReason;
import io.github.hyshmily.zeta.sync.worker.WorkerDecisionHandler;
import io.github.hyshmily.zeta.sync.worker.WorkerDecisionHook;
import io.github.hyshmily.zeta.sync.worker.WorkerDecisionType;
import io.github.hyshmily.zeta.sync.worker.WorkerMessage;
import io.github.hyshmily.zeta.util.clock.StampClock;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

class DefaultWorkerDecisionHandlerTest {

  private Cache<String, CacheEntry> cache;
  private DefaultWorkerDecisionHandler handler;
  private CacheLoader loader;
  private EntryLifecycleImpl entryLifecycle;
  private StampClock stampClock;

  @BeforeEach
  void setUp() {
    cache = Caffeine.newBuilder().maximumSize(100).build();
    ZetaProperties ttlConfig = new ZetaProperties();
    entryLifecycle = new EntryLifecycleImpl(cache, ttlConfig, null);
    loader = k -> "fresh";
    stampClock = org.mockito.Mockito.mock(StampClock.class);
    org.mockito.Mockito.when(stampClock.nextStamp()).thenReturn(1L);
    org.mockito.Mockito.when(stampClock.nextStamps(org.mockito.ArgumentMatchers.anyInt())).thenReturn(1L);
    handler = new DefaultWorkerDecisionHandler(cache, loader, entryLifecycle, stampClock, Collections.emptyList());
  }

  private static WorkerMessage workerMessage(String key, String type, long dv) {
    return workerMessageFrom(key, type, dv, "node");
  }

  private static WorkerMessage workerMessageFrom(String key, String type, long dv, String nodeId) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, type);
    props.setHeader(HEADER_VERSION, dv);
    props.setHeader(HEADER_NODE_ID, nodeId);
    props.setHeader(HEADER_EPOCH, 1L);
    props.setHeader(HEADER_MESSAGE_ID, 1L);
    return WorkerMessage.from(new Message(key.getBytes(StandardCharsets.UTF_8), props));
  }

  private static CacheEntry hotEntryOwnedBy(String owner, long dv) {
    return EntryDraft.of(entry(dv, KeyState.HOT)).decision(DecisionStamp.hot(dv, owner, 1L)).build();
  }

  private static CacheEntry entry(long dv, KeyState state) {
    return EntryDraft.blank(null)
      .value("v")
      .version(1)
      .decision(DecisionStamp.hot(dv, "node", 1L))
      .ttl(300_000, 30_000, 300_000, 30_000)
      .expiryAt(Long.MAX_VALUE, 30_000)
      .keyState(state)
      .build();
  }

  @Test
  void handleHot_shouldInvokeAfterHotPromotionHook() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, loader, entryLifecycle, stampClock, List.of(hook));

    handler.handleHot(workerMessage("key1", WorkerDecisionType.HOT.code(), 2L));

    verify(hook).afterHotPromotion(eq("key1"), any(), any());
  }

  /**
   * A no-loader promotion writes no loaded value, so an older per-key tombstone must not
   * block the metadata-only promotion of the live resident. Regression: the tombstone probe
   * used the zero {@code hotStamp} and reported "invalidated during load" for every key that
   * had ever been invalidated, leaving the resident NORMAL forever.
   */
  @Test
  void handleHot_withoutLoaderPromotesALiveResidentEvenAfterAPerKeyTombstone() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    entryLifecycle.recordInvalidation("key1", 10L);
    handler = new DefaultWorkerDecisionHandler(cache, null, entryLifecycle, stampClock, List.of());

    handler.handleHot(workerMessage("key1", WorkerDecisionType.HOT.code(), 2L));

    CacheEntry promoted = cache.asMap().get("key1");
    assertThat(promoted).isNotNull();
    assertThat(promoted.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(promoted.getValue()).isEqualTo("v");
    assertThat(promoted.getHardTtlMs()).isEqualTo(3_600_000L);
    assertThat(promoted.getSoftTtlMs()).isEqualTo(300_000L);
  }

  /**
   * A COOL run is pure in-memory: only the decision that survives the version guard can
   * affect the final state. The covered message must still receive its terminal skip hook.
   */
  @Test
  void handleCoolBatch_keepsTheHighestDecisionAndReportsTheCoveredOne() {
    cache.put("key1", entry(1, KeyState.HOT));
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, loader, entryLifecycle, stampClock, List.of(hook));
    WorkerMessage cool10 = workerMessage("key1", WorkerDecisionType.COOL.code(), 10L);
    WorkerMessage cool7 = workerMessage("key1", WorkerDecisionType.COOL.code(), 7L);

    handler.handleCoolBatch(List.of(cool10, cool7));

    CacheEntry cooled = cache.asMap().get("key1");
    assertThat(cooled).isNotNull();
    assertThat(cooled.getKeyState()).isEqualTo(KeyState.COOL);
    assertThat(cooled.getDecisionVersion()).isEqualTo(10L);
    verify(hook).afterCoolDowngrade(eq("key1"), eq(cool10), any());
    verify(hook).onCoolSkipped(eq("key1"), eq(cool7));
  }

  /** A later, higher decision supersedes the earlier winner before either touches L1. */
  @Test
  void handleCoolBatch_laterHigherDecisionSupersedesTheEarlierWinner() {
    cache.put("key1", entry(1, KeyState.HOT));
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, loader, entryLifecycle, stampClock, List.of(hook));
    WorkerMessage cool10 = workerMessage("key1", WorkerDecisionType.COOL.code(), 10L);
    WorkerMessage cool12 = workerMessage("key1", WorkerDecisionType.COOL.code(), 12L);

    handler.handleCoolBatch(List.of(cool10, cool12));

    CacheEntry cooled = cache.asMap().get("key1");
    assertThat(cooled).isNotNull();
    assertThat(cooled.getDecisionVersion()).isEqualTo(12L);
    verify(hook).onCoolSkipped(eq("key1"), eq(cool10));
    verify(hook).afterCoolDowngrade(eq("key1"), eq(cool12), any());
  }

  /** When the resident already outranks the whole run, every message is skipped. */
  @Test
  void handleCoolBatch_reportsEveryMessageWhenTheResidentOutranksTheRun() {
    cache.put("key1", hotEntryOwnedBy("node", 100L));
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, loader, entryLifecycle, stampClock, List.of(hook));
    WorkerMessage cool10 = workerMessage("key1", WorkerDecisionType.COOL.code(), 10L);
    WorkerMessage cool7 = workerMessage("key1", WorkerDecisionType.COOL.code(), 7L);

    handler.handleCoolBatch(List.of(cool10, cool7));

    CacheEntry resident = cache.asMap().get("key1");
    assertThat(resident).isNotNull();
    assertThat(resident.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(resident.getDecisionVersion()).isEqualTo(100L);
    verify(hook).onCoolSkipped(eq("key1"), eq(cool10));
    verify(hook).onCoolSkipped(eq("key1"), eq(cool7));
    verify(hook, never()).afterCoolDowngrade(any(), any(), any());
  }

  /**
   * A same-sender COOL run is coalesced even when the ghost-COOL guard rejects it: the
   * winner is refused by {@link #handleCool}, the covered message is refused by the fold,
   * and the HOT resident survives with one terminal hook per message.
   */
  @Test
  void handleCoolBatch_sameNodeGhostRunLeavesHotAndReportsEveryMessage() {
    cache.put("key1", hotEntryOwnedBy("worker-B", 10L));
    ConsistentHashRing ring = mock(ConsistentHashRing.class);
    HealthView healthView = mock(HealthView.class);
    when(ring.locateNode(eq("key1"), any(HealthView.class))).thenReturn("worker-B");
    when(healthView.isCurrentIncarnation("worker-B", 1L)).thenReturn(true);
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(
      cache,
      loader,
      entryLifecycle,
      stampClock,
      List.of(hook),
      ring,
      healthView
    );
    WorkerMessage cool20 = workerMessageFrom("key1", WorkerDecisionType.COOL.code(), 20L, "worker-A");
    WorkerMessage cool30 = workerMessageFrom("key1", WorkerDecisionType.COOL.code(), 30L, "worker-A");

    handler.handleCoolBatch(List.of(cool20, cool30));

    CacheEntry after = cache.asMap().get("key1");
    assertThat(after).isNotNull();
    assertThat(after.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(after.getDecisionVersion()).isEqualTo(10L);
    assertThat(after.getDecisionNodeId()).isEqualTo("worker-B");
    verify(hook).onCoolSkipped(eq("key1"), eq(cool20));
    verify(hook).onCoolSkipped(eq("key1"), eq(cool30));
    verify(hook, never()).afterCoolDowngrade(any(), any(), any());
  }

  /**
   * A mixed-node COOL run falls back to sequential applies: the ghost-COOL guard depends on
   * the resident's HOT owner, so dropping an earlier same-owner COOL could change whether a
   * later cross-node COOL is rejected.
   */
  @Test
  void handleCoolBatch_mixedNodeRunFallsBackToSequential() {
    cache.put("key1", entry(1, KeyState.HOT));
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, loader, entryLifecycle, stampClock, List.of(hook));
    WorkerMessage fromA = workerMessageFrom("key1", WorkerDecisionType.COOL.code(), 10L, "worker-A");
    WorkerMessage fromB = workerMessageFrom("key1", WorkerDecisionType.COOL.code(), 9L, "worker-B");

    handler.handleCoolBatch(List.of(fromA, fromB));

    CacheEntry cooled = cache.asMap().get("key1");
    assertThat(cooled).isNotNull();
    assertThat(cooled.getDecisionNodeId()).isEqualTo("worker-B");
    assertThat(cooled.getDecisionVersion()).isEqualTo(9L);
    verify(hook).afterCoolDowngrade(eq("key1"), eq(fromA), any());
    verify(hook).afterCoolDowngrade(eq("key1"), eq(fromB), any());
    verify(hook, never()).onCoolSkipped(any(), any());
  }

  /**
   * Verifies the create path preserves the Worker decision identity (C1): an
   * entry created by a Worker HOT decision carries nodeId/epoch, so
   * VersionGuard can compare against out-of-order replayed decisions instead
   * of accepting them unconditionally.
   */
  @Test
  void handleHot_entryAbsent_shouldPreserveDecisionIdentity() {
    handler.handleHot(workerMessage("newkey", WorkerDecisionType.HOT.code(), 1L));

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
    handler.handleHot(workerMessage("newkey", WorkerDecisionType.HOT.code(), 5L));
    CacheEntry before = (CacheEntry) cache.getIfPresent("newkey");
    assertThat(before.getDecisionVersion()).isEqualTo(5L);

    handler.handleHot(workerMessage("newkey", WorkerDecisionType.HOT.code(), 3L));

    CacheEntry after = (CacheEntry) cache.getIfPresent("newkey");
    assertThat(after.getDecisionVersion()).isEqualTo(5L);
  }

  @Test
  void handleHot_staleVersion_shouldInvokeOnHotSkippedStale() {
    cache.put("key1", entry(5, KeyState.NORMAL));
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, loader, entryLifecycle, stampClock, List.of(hook));

    handler.handleHot(workerMessage("key1", WorkerDecisionType.HOT.code(), 3L));

    verify(hook).onHotSkipped(eq("key1"), any(), eq(HotSkipReason.VERSION_STALE));
  }

  @Test
  void handleHot_valueNotFound_shouldInvokeOnHotSkippedValueNotFound() {
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, k -> null, entryLifecycle, stampClock, List.of(hook));

    handler.handleHot(workerMessage("missing", WorkerDecisionType.HOT.code(), 1L));

    verify(hook).onHotSkipped(eq("missing"), any(), eq(HotSkipReason.VALUE_NOT_FOUND));
  }

  /**
   * An L1 fallback is a promotion of the resident that supplied it, never a license to recreate
   * that value. If the resident disappears between the fallback read and the compute, creating a
   * new HOT entry would resurrect it under a fresh TTL and a new birth stamp.
   */
  @Test
  @SuppressWarnings("unchecked")
  void handleHot_fallbackEntryRemovedBeforeCompute_shouldNotCreateFromFallback() {
    Cache<String, CacheEntry> mockedCache = mock(Cache.class);
    ConcurrentMap<String, CacheEntry> map = mock(ConcurrentMap.class);
    when(mockedCache.asMap()).thenReturn(map);
    CacheEntry resident = entry(1, KeyState.NORMAL);
    when(map.get("key1")).thenReturn(resident, resident);
    when(map.compute(eq("key1"), any())).thenAnswer(invocation -> {
      BiFunction<String, CacheEntry, CacheEntry> mutation = invocation.getArgument(1);
      return mutation.apply("key1", null);
    });

    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    Cache<String, CacheEntry> lifecycleCache = Caffeine.newBuilder().build();
    EntryLifecycleImpl lifecycle = new EntryLifecycleImpl(lifecycleCache, new ZetaProperties(), null);
    handler = new DefaultWorkerDecisionHandler(mockedCache, k -> null, lifecycle, stampClock, List.of(hook));

    handler.handleHot(workerMessage("key1", WorkerDecisionType.HOT.code(), 2L));

    verify(hook).onHotSkipped(eq("key1"), any(), eq(HotSkipReason.VALUE_NOT_FOUND));
    verify(hook, never()).afterHotPromotion(any(), any(), any());
  }

  /**
   * Verifies admission on the Worker plane: a value the cluster loader declines to cache is never
   * written by a HOT warm-up, even though the decision itself is valid. With no L1 entry to fall
   * back to, the HOT is answered like a missing value.
   */
  @Test
  void handleHot_admissionDeclined_shouldNotWriteEntryAndSkipHot() {
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(
      cache,
      decliningLoader("rejected"),
      entryLifecycle,
      stampClock,
      List.of(hook)
    );

    handler.handleHot(workerMessage("declined", WorkerDecisionType.HOT.code(), 1L));

    assertThat(cache.getIfPresent("declined")).as("a declined value must not enter L1").isNull();
    verify(hook).onHotSkipped(eq("declined"), any(), eq(HotSkipReason.VALUE_NOT_FOUND));
  }

  /**
   * The declined admission takes the existing source-outage fallback: the resident L1 value is
   * promoted to HOT, never replaced by the value the application refused to cache.
   */
  @Test
  void handleHot_admissionDeclined_shouldPromoteExistingEntryInsteadOfRejectedValue() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    handler = new DefaultWorkerDecisionHandler(
      cache,
      decliningLoader("rejected"),
      entryLifecycle,
      stampClock,
      Collections.emptyList()
    );

    handler.handleHot(workerMessage("key1", WorkerDecisionType.HOT.code(), 2L));

    CacheEntry promoted = cache.getIfPresent("key1");
    assertThat(promoted).isNotNull();
    assertThat(promoted.getValue()).as("the declined value must not replace the resident").isEqualTo("v");
    assertThat(promoted.getKeyState()).isEqualTo(KeyState.HOT);
  }

  /** A cluster loader whose value is always declined by the admission predicate. */
  private static CacheLoader<String, Object> decliningLoader(Object value) {
    return new CacheLoader<String, Object>() {
      @Override
      public Object load(String key) {
        return value;
      }

      @Override
      public boolean isCacheable(Object candidate) {
        return false;
      }
    };
  }

  /**
   * Verifies the afterHotPromotion gate (F4): when the inner DCL guard rejects
   * the promotion as stale (a newer decision landed during the Redis fetch),
   * the hook may not fire. The loader bumps the
   * entry's decisionVersion between the outer pre-check and the atomic
   * compute, deterministically reproducing the stale-rejection interleaving.
   */
  @Test
  void handleHot_innerGuardRejectsPromotion_shouldNotFireAfterHotPromotionHook() {
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    cache.put("key1", entry(1, KeyState.NORMAL));
    CacheLoader bumpingLoader = k -> {
      // Simulates a concurrent newer decision landing during the Redis fetch.
      cache.put("key1", entry(10, KeyState.NORMAL));
      return "fresh";
    };
    handler = new DefaultWorkerDecisionHandler(cache, bumpingLoader, entryLifecycle, stampClock, List.of(hook));

    handler.handleHot(workerMessage("key1", WorkerDecisionType.HOT.code(), 2L));

    verify(hook, never()).afterHotPromotion(any(), any(), any());
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
    handler = new DefaultWorkerDecisionHandler(cache, loader, entryLifecycle, stampClock, List.of(hook));

    handler.handleHot(workerMessage("newkey", WorkerDecisionType.HOT.code(), 2L));

    ArgumentCaptor<CacheEntry> entryCaptor = ArgumentCaptor.forClass(CacheEntry.class);
    verify(hook).afterHotPromotion(eq("newkey"), any(), entryCaptor.capture());
    assertThat(entryCaptor.getValue().getDecisionVersion()).isEqualTo(2L);
    assertThat(entryCaptor.getValue().getKeyState()).isEqualTo(KeyState.HOT);
  }

  /**
   * Verifies the Redis-outage fallback (design decisions: version stamps): when the loader fails and the
   * L1 entry is a normal entry, its value is used for promotion.
   */
  @Test
  void handleHot_redisDown_shouldPromoteFromNormalL1Entry() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, k -> null, entryLifecycle, stampClock, List.of(hook));

    handler.handleHot(workerMessage("key1", WorkerDecisionType.HOT.code(), 2L));

    verify(hook).afterHotPromotion(eq("key1"), any(), any());
    CacheEntry promoted = (CacheEntry) cache.getIfPresent("key1");
    assertThat(promoted).isNotNull();
    assertThat(promoted.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(promoted.getValue()).isEqualTo("v");
  }

  /**
   * A loader failure must not reset the resident's deadlines: the fallback value is not fresh,
   * so extending its soft window would hide the staleness for a whole hot TTL.
   */
  @Test
  void handleHot_loaderFailurePreservesTheResidentDeadlines() {
    long now = System.currentTimeMillis();
    long hardExpireAt = now + 10_000L;
    long softExpireAt = now - 1_000L;
    CacheEntry resident = EntryDraft.blank(null)
      .value("v")
      .version(1)
      .ttl(300_000, 30_000, 300_000, 30_000)
      .expiryAt(hardExpireAt, softExpireAt)
      .keyState(KeyState.NORMAL)
      .build();
    cache.put("key1", resident);
    handler = new DefaultWorkerDecisionHandler(cache, k -> null, entryLifecycle, stampClock, List.of());

    handler.handleHot(workerMessage("key1", WorkerDecisionType.HOT.code(), 2L));

    CacheEntry promoted = (CacheEntry) cache.getIfPresent("key1");
    assertThat(promoted.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(promoted.getHardExpireAtMs()).isEqualTo(hardExpireAt);
    assertThat(promoted.getSoftExpireAtMs()).isEqualTo(softExpireAt);
  }

  /**
   * Redis-outage fallback: the L1 value is stored as-is (no codec) and the
   * promotion must preserve the exact instance.
   */
  @Test
  void handleHot_redisDown_shouldPreserveStoredFallbackValue() {
    String original = "zeta-fallback-value-".repeat(60);
    CacheEntry l1Entry = EntryDraft.of(entry(1, KeyState.NORMAL)).value(original).build();
    cache.put("key1", l1Entry);
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, k -> null, entryLifecycle, stampClock, List.of(hook));

    handler.handleHot(workerMessage("key1", WorkerDecisionType.HOT.code(), 2L));

    verify(hook).afterHotPromotion(eq("key1"), any(), any());
    CacheEntry promoted = (CacheEntry) cache.getIfPresent("key1");
    assertThat(promoted).isNotNull();
    assertThat(promoted.getKeyState()).isEqualTo(KeyState.HOT);
    // The stored value must be the exact instance the L1 held.
    assertThat(promoted.getValue()).isSameAs(original);
  }

  /**
   * Verifies that a source outage fallback promotes any existing L1 entry.
   */
  @Test
  void handleHot_sourceDown_shouldPromoteFromExistingL1Entry() {
    CacheEntry existing = entry(1L, KeyState.NORMAL);
    cache.put("key1", existing);
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, k -> null, entryLifecycle, stampClock, List.of(hook));

    handler.handleHot(workerMessage("key1", WorkerDecisionType.HOT.code(), 2L));

    verify(hook).afterHotPromotion(eq("key1"), any(), any());
  }

  /**
   * Verifies that a Redis outage with no L1 fallback value aborts the HOT
   * promotion — there is nothing of value to promote.
   */
  @Test
  void handleHot_redisDown_withoutFallback_shouldAbort() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    cache.invalidate("key1");
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, k -> null, entryLifecycle, stampClock, List.of(hook));

    handler.handleHot(workerMessage("key1", WorkerDecisionType.HOT.code(), 2L));

    verify(hook).onHotSkipped(eq("key1"), any(), eq(HotSkipReason.VALUE_NOT_FOUND));
    assertThat(cache.getIfPresent("key1")).isNull();
  }

  /**
   * Verifies that a Redis outage promotes the existing L1 entry's value —
   * the outage fallback keeps the last known value (v2: no sentinel
   * exclusion; any non-null L1 value is promotable).
   */
  @Test
  void handleHot_redisDown_shouldPromoteExistingL1Value() {
    CacheEntry existing = entry(1, KeyState.NORMAL);
    cache.put("key1", existing);
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, k -> null, entryLifecycle, stampClock, List.of(hook));

    handler.handleHot(workerMessage("key1", WorkerDecisionType.HOT.code(), 2L));

    CacheEntry after = (CacheEntry) cache.getIfPresent("key1");
    assertThat(after.getKeyState()).isEqualTo(KeyState.HOT);
  }

  @Test
  void handleCool_shouldInvokeAfterCoolDowngradeHook() {
    cache.put("key1", entry(5, KeyState.HOT));
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, loader, entryLifecycle, stampClock, List.of(hook));

    handler.handleCool(workerMessage("key1", WorkerDecisionType.COOL.code(), 6L));

    verify(hook).afterCoolDowngrade(eq("key1"), any(), any());
  }

  @Test
  void handleCool_missingNormalTtl_shouldApplyHardFallbackAndKeepSoftDisabled() {
    cache.put(
      "key1",
      EntryDraft.blank(null)
        .value("v")
        .version(1)
        .decision(DecisionStamp.hot(1, "node", 1L))
        .ttl(300_000, 30_000, 0, 0)
        .expiryAt(Long.MAX_VALUE, 30_000)
        .keyState(KeyState.HOT)
        .build()
    );

    handler.handleCool(workerMessage("key1", WorkerDecisionType.COOL.code(), 2L));

    CacheEntry ce = (CacheEntry) cache.getIfPresent("key1");
    assertThat(ce.getKeyState()).isEqualTo(KeyState.COOL);
    // The hard fallback is 120s; a missing/disabled normal soft TTL stays disabled.
    assertThat(ce.getHardTtlMs()).isEqualTo(120_000L);
    assertThat(ce.getSoftTtlMs()).isZero();
  }

  @Test
  void handleCool_noEntry_shouldInvokeOnCoolSkipped() {
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, loader, entryLifecycle, stampClock, List.of(hook));

    handler.handleCool(workerMessage("missing", WorkerDecisionType.COOL.code(), 1L));

    verify(hook).onCoolSkipped(eq("missing"), any());
  }

  /**
   * design decisions: version comparison: a hard-expired entry is dead — the next read invalidates and reloads
   * it (design decisions: expiry and refresh, the hard TTL is the absolute bound). Cooling it must NOT rewrite
   * the stale value with fresh normal TTLs (which would resurrect the data for a
   * full normal-TTL lifetime); the entry stays untouched and expires on schedule.
   */
  @Test
  void handleCool_logicallyExpiredEntry_shouldNotResurrect() {
    CacheEntry expired = EntryDraft.blank(null)
      .value("stale")
      .version(1)
      .decision(DecisionStamp.hot(5, "node", 1L))
      .ttl(300_000, 30_000, 300_000, 30_000)
      .expiryAt(1L, 1L)
      .keyState(KeyState.HOT)
      .build();
    cache.put("key1", expired);
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, loader, entryLifecycle, stampClock, List.of(hook));

    handler.handleCool(workerMessage("key1", WorkerDecisionType.COOL.code(), 6L));

    CacheEntry after = (CacheEntry) cache.getIfPresent("key1");
    assertThat(after).isSameAs(expired);
    assertThat(after.getKeyState()).isEqualTo(KeyState.HOT);
    verify(hook).onCoolSkipped(eq("key1"), any());
    verify(hook, never()).afterCoolDowngrade(eq("key1"), any(), any());
  }

  /**
   * The expiry guard must not block a live entry: a valid HOT entry still cools
   * normally (regression guard for the guard placement (design decisions: version comparison)).
   */
  @Test
  void handleCool_liveEntry_shouldStillCool() {
    cache.put("key1", entry(5, KeyState.HOT));
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, loader, entryLifecycle, stampClock, List.of(hook));

    handler.handleCool(workerMessage("key1", WorkerDecisionType.COOL.code(), 6L));

    CacheEntry ce = (CacheEntry) cache.getIfPresent("key1");
    assertThat(ce.getKeyState()).isEqualTo(KeyState.COOL);
    verify(hook).afterCoolDowngrade(eq("key1"), any(), any());
  }

  /**
   * A hard-expired entry must not be rewritten with the COOL protection
   * defaults (120s/60s): it is dead, and cooling it would resurrect data the
   * hard TTL already condemned (design decisions: expiry and refresh).
   */
  @Test
  void handleCool_hardExpiredEntry_shouldNotExtendTtl() {
    CacheEntry dead = EntryDraft.blank(null)
      .value("v")
      .version(1)
      .decision(DecisionStamp.hot(1, "node", 1L))
      .ttl(1, 0, 0, 0)
      .expiryAt(1L, 0)
      .keyState(KeyState.NORMAL)
      .build();
    cache.put("key1", dead);
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, loader, entryLifecycle, stampClock, List.of(hook));

    handler.handleCool(workerMessage("key1", WorkerDecisionType.COOL.code(), 2L));

    CacheEntry after = (CacheEntry) cache.getIfPresent("key1");
    assertThat(after).isSameAs(dead);
    assertThat(after.getKeyState()).isEqualTo(KeyState.NORMAL);
    verify(hook).onCoolSkipped(eq("key1"), any());
    verify(hook, never()).afterCoolDowngrade(eq("key1"), any(), any());
  }

  @Test
  void multipleHooks_shouldAllBeInvoked() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    WorkerDecisionHook h1 = mock(WorkerDecisionHook.class);
    WorkerDecisionHook h2 = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, loader, entryLifecycle, stampClock, List.of(h1, h2));

    handler.handleHot(workerMessage("key1", WorkerDecisionType.HOT.code(), 2L));

    verify(h1).afterHotPromotion(eq("key1"), any(), any());
    verify(h2).afterHotPromotion(eq("key1"), any(), any());
  }

  @Test
  void hookException_shouldNotBreakOtherHooks() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    AtomicInteger count = new AtomicInteger(0);
    WorkerDecisionHook failingHook = new WorkerDecisionHook() {
      @Override
      public void afterHotPromotion(String k, WorkerMessage workerMessage, CacheEntry e) {
        throw new RuntimeException("fail");
      }
    };
    WorkerDecisionHook countingHook = new WorkerDecisionHook() {
      @Override
      public void afterHotPromotion(String k, WorkerMessage workerMessage, CacheEntry e) {
        count.incrementAndGet();
      }
    };
    handler = new DefaultWorkerDecisionHandler(
      cache,
      loader,
      entryLifecycle,
      stampClock,
      List.of(failingHook, countingHook)
    );

    handler.handleHot(workerMessage("key1", WorkerDecisionType.HOT.code(), 2L));

    assertThat(count.get()).isEqualTo(1);
  }

  @Test
  void customHandler_shouldReplaceDefaultBehavior() {
    WorkerDecisionHandler custom = new WorkerDecisionHandler() {
      @Override
      public void handleHot(WorkerMessage workerMessage) {
        cache.put(workerMessage.cacheKey(), valuedEntry("custom-hot"));
      }

      @Override
      public void handleCool(WorkerMessage workerMessage) {
        cache.put(workerMessage.cacheKey(), valuedEntry("custom-cool"));
      }
    };

    custom.handleHot(workerMessage("k", WorkerDecisionType.HOT.code(), 1L));
    assertThat(cache.getIfPresent("k").getValue()).isEqualTo("custom-hot");

    custom.handleCool(workerMessage("k", WorkerDecisionType.COOL.code(), 2L));
    assertThat(cache.getIfPresent("k").getValue()).isEqualTo("custom-cool");
  }

  // ── P0 scale re-balance ghost-COOL kill ──

  /**
   * Core P0 scenario: worker-B took over the shard and asserted HOT; a delayed
   * EvictStale COOL from ex-owner worker-A must be rejected while worker-B is
   * alive and the ring still routes the key to worker-B. The HOT entry is
   * preserved untouched.
   */
  @Test
  void handleCool_staleCrossNodeCoolAfterTakeover_shouldPreserveHot() {
    cache.put("key1", hotEntryOwnedBy("worker-B", 10L));
    ConsistentHashRing ring = mock(ConsistentHashRing.class);
    HealthView healthView = mock(HealthView.class);
    when(ring.locateNode(eq("key1"), any(HealthView.class))).thenReturn("worker-B");
    when(healthView.isCurrentIncarnation("worker-B", 1L)).thenReturn(true);
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(
      cache,
      loader,
      entryLifecycle,
      stampClock,
      List.of(hook),
      ring,
      healthView
    );

    handler.handleCool(workerMessageFrom("key1", WorkerDecisionType.COOL.code(), 3L, "worker-A"));

    CacheEntry after = (CacheEntry) cache.getIfPresent("key1");
    assertThat(after.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(after.getDecisionVersion()).isEqualTo(10L);
    assertThat(after.getDecisionNodeId()).isEqualTo("worker-B");
    verify(hook).onCoolSkipped(eq("key1"), any());
    verify(hook, never()).afterCoolDowngrade(eq("key1"), any(), any());
  }

  /**
   * A dead owner's HOT is legitimately coolable: the ownership guard stays out
   * of the way and the cross-Worker COOL applies.
   */
  @Test
  void handleCool_crossNodeCoolWhenOwnerDead_shouldApply() {
    cache.put("key1", hotEntryOwnedBy("worker-B", 10L));
    ConsistentHashRing ring = mock(ConsistentHashRing.class);
    HealthView healthView = mock(HealthView.class);
    when(ring.locateNode(eq("key1"), any(HealthView.class))).thenReturn("worker-B");
    when(healthView.isCurrentIncarnation("worker-B", 1L)).thenReturn(false);
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(
      cache,
      loader,
      entryLifecycle,
      stampClock,
      List.of(hook),
      ring,
      healthView
    );

    handler.handleCool(workerMessageFrom("key1", WorkerDecisionType.COOL.code(), 3L, "worker-A"));

    CacheEntry after = (CacheEntry) cache.getIfPresent("key1");
    assertThat(after.getKeyState()).isEqualTo(KeyState.COOL);
    verify(hook).afterCoolDowngrade(eq("key1"), any(), any());
  }

  /**
   * A restarted owner is no longer the incarnation that issued the HOT: the
   * ownership guard must fail open and let the cross-Worker COOL apply even while
   * the new incarnation of the same node is alive and the ring still routes the
   * key to it.
   */
  @Test
  void handleCool_crossNodeCoolWhenOwnerRestarted_shouldApply() {
    cache.put("key1", hotEntryOwnedBy("worker-B", 10L));
    ConsistentHashRing ring = mock(ConsistentHashRing.class);
    HealthView healthView = mock(HealthView.class);
    when(ring.locateNode(eq("key1"), any(HealthView.class))).thenReturn("worker-B");
    when(healthView.isCurrentIncarnation("worker-B", 1L)).thenReturn(false);
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(
      cache,
      loader,
      entryLifecycle,
      stampClock,
      List.of(hook),
      ring,
      healthView
    );

    handler.handleCool(workerMessageFrom("key1", WorkerDecisionType.COOL.code(), 3L, "worker-A"));

    CacheEntry after = (CacheEntry) cache.getIfPresent("key1");
    assertThat(after.getKeyState()).isEqualTo(KeyState.COOL);
    verify(hook).afterCoolDowngrade(eq("key1"), any(), any());
  }

  /**
   * The ring already re-pointed at the incoming sender: its COOL is authoritative
   * and must apply even though the entry still carries the old owner's stamp.
   */
  @Test
  void handleCool_crossNodeCoolWhenRingPointsAtIncoming_shouldApply() {
    cache.put("key1", hotEntryOwnedBy("worker-B", 10L));
    ConsistentHashRing ring = mock(ConsistentHashRing.class);
    HealthView healthView = mock(HealthView.class);
    when(ring.locateNode(eq("key1"), any(HealthView.class))).thenReturn("worker-A");
    when(healthView.isCurrentIncarnation("worker-B", 1L)).thenReturn(true);
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(
      cache,
      loader,
      entryLifecycle,
      stampClock,
      List.of(hook),
      ring,
      healthView
    );

    handler.handleCool(workerMessageFrom("key1", WorkerDecisionType.COOL.code(), 3L, "worker-A"));

    CacheEntry after = (CacheEntry) cache.getIfPresent("key1");
    assertThat(after.getKeyState()).isEqualTo(KeyState.COOL);
    assertThat(after.getDecisionNodeId()).isEqualTo("worker-A");
    verify(hook).afterCoolDowngrade(eq("key1"), any(), any());
  }

  /**
   * Fail-open: without the sharding beans (the 6-argument construction) a
   * cross-Worker COOL still applies exactly as before.
   */
  @Test
  void handleCool_crossNodeCoolWithoutShardingBeans_shouldApply() {
    cache.put("key1", hotEntryOwnedBy("worker-B", 10L));
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    handler = new DefaultWorkerDecisionHandler(cache, loader, entryLifecycle, stampClock, List.of(hook));

    handler.handleCool(workerMessageFrom("key1", WorkerDecisionType.COOL.code(), 3L, "worker-A"));

    CacheEntry after = (CacheEntry) cache.getIfPresent("key1");
    assertThat(after.getKeyState()).isEqualTo(KeyState.COOL);
    verify(hook).afterCoolDowngrade(eq("key1"), any(), any());
  }

  /** NORMAL entry carrying an identifiable value (F1: slots hold {@link CacheEntry} only). */
  private static CacheEntry valuedEntry(String value) {
    return EntryDraft.blank(null)
      .value(value)
      .version(1)
      .decision(DecisionStamp.hot(0, null, 0L))
      .ttl(300_000, 0, 300_000, 0)
      .expiryAt(Long.MAX_VALUE, 0)
      .keyState(KeyState.NORMAL)
      .build();
  }
}
