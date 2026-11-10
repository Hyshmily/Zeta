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
import io.github.hyshmily.zeta.sync.worker.DefaultWorkerContext;
import io.github.hyshmily.zeta.sync.worker.HotAction;
import io.github.hyshmily.zeta.sync.worker.HotSkipReason;
import io.github.hyshmily.zeta.sync.worker.HotValue;
import io.github.hyshmily.zeta.sync.worker.WorkerDecision;
import io.github.hyshmily.zeta.sync.worker.WorkerDecisionHook;
import io.github.hyshmily.zeta.sync.worker.WorkerPolicy;
import io.github.hyshmily.zeta.sync.worker.WorkerType;
import io.github.hyshmily.zeta.util.TimeSource;
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

class DefaultWorkerContextTest {

  private Cache<String, CacheEntry> cache;
  private DefaultWorkerContext context;
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
    context = new DefaultWorkerContext(cache, loader, entryLifecycle, stampClock, Collections.emptyList());
  }

  private void handleHot(WorkerDecision decision) {
    HotAction prepared = context.resolveHot(decision);
    context.settleHot(decision, prepared);
  }

  private void handleCool(WorkerDecision decision) {
    context.cool(decision);
  }

  private void handleCoolBatch(List<WorkerDecision> batch) {
    for (WorkerDecision effective : context.reduceCool(batch)) {
      handleCool(effective);
    }
  }

  private static WorkerDecision wireMessage(String key, String type, long dv) {
    return workerMessageFrom(key, type, dv, "node");
  }

  private static WorkerDecision workerMessageFrom(String key, String type, long dv, String nodeId) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, type);
    props.setHeader(HEADER_VERSION, dv);
    props.setHeader(HEADER_NODE_ID, nodeId);
    props.setHeader(HEADER_EPOCH, 1L);
    props.setHeader(HEADER_MESSAGE_ID, 1L);
    return WorkerDecision.from(new Message(key.getBytes(StandardCharsets.UTF_8), props));
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
  void handleHot_logicallyExpiredResidentDoesNotVetoAFreshLoad() {
    CacheEntry dead = EntryDraft.blank(null)
      .value("stale")
      .version(999)
      .decision(DecisionStamp.hot(1, "node", 1L))
      .ttl(300_000, 30_000, 300_000, 30_000)
      .hardExpiryAt(TimeSource.currentTimeMillis() - 1L)
      .keyState(KeyState.NORMAL)
      .build();
    cache.put("key1", dead);
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    context = new DefaultWorkerContext(cache, loader, entryLifecycle, stampClock, List.of(hook));

    handleHot(wireMessage("key1", WorkerType.HOT.code(), 2L));

    CacheEntry promoted = cache.getIfPresent("key1");
    assertThat(promoted).isNotNull();
    assertThat(promoted.getValue()).isEqualTo("fresh");
    assertThat(promoted.getKeyState()).isEqualTo(KeyState.HOT);
    verify(hook).afterHotPromotion(any(), any());
    verify(hook, never()).onHotSkipped(any(), any());
  }

  @Test
  void handleHot_shouldInvokeAfterHotPromotionHook() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    context = new DefaultWorkerContext(cache, loader, entryLifecycle, stampClock, List.of(hook));

    handleHot(wireMessage("key1", WorkerType.HOT.code(), 2L));

    verify(hook).afterHotPromotion(any(), any());
  }

  /**
   * A no-loader promotion writes no loaded value, so an older per-key tombstone must not
   * block the metadata-only promotion of the live resident: the tombstone probe belongs to
   * the value-rewrite guard only, never to the metadata-only promotion.
   */
  @Test
  void handleHot_withoutLoaderPromotesALiveResidentEvenAfterAPerKeyTombstone() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    entryLifecycle.recordInvalidation("key1", 10L);
    context = new DefaultWorkerContext(cache, null, entryLifecycle, stampClock, List.of());

    handleHot(wireMessage("key1", WorkerType.HOT.code(), 2L));

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
    context = new DefaultWorkerContext(cache, loader, entryLifecycle, stampClock, List.of(hook));
    WorkerDecision cool10 = wireMessage("key1", WorkerType.COOL.code(), 10L);
    WorkerDecision cool7 = wireMessage("key1", WorkerType.COOL.code(), 7L);

    handleCoolBatch(List.of(cool10, cool7));

    CacheEntry cooled = cache.asMap().get("key1");
    assertThat(cooled).isNotNull();
    assertThat(cooled.getKeyState()).isEqualTo(KeyState.COOL);
    assertThat(cooled.getDecisionVersion()).isEqualTo(10L);
    verify(hook).afterCoolDowngrade(eq(cool10), any());
    verify(hook).onCoolSkipped(eq(cool7));
  }

  /** A later, higher decision supersedes the earlier winner before either touches L1. */
  @Test
  void handleCoolBatch_laterHigherDecisionSupersedesTheEarlierWinner() {
    cache.put("key1", entry(1, KeyState.HOT));
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    context = new DefaultWorkerContext(cache, loader, entryLifecycle, stampClock, List.of(hook));
    WorkerDecision cool10 = wireMessage("key1", WorkerType.COOL.code(), 10L);
    WorkerDecision cool12 = wireMessage("key1", WorkerType.COOL.code(), 12L);

    handleCoolBatch(List.of(cool10, cool12));

    CacheEntry cooled = cache.asMap().get("key1");
    assertThat(cooled).isNotNull();
    assertThat(cooled.getDecisionVersion()).isEqualTo(12L);
    verify(hook).onCoolSkipped(eq(cool10));
    verify(hook).afterCoolDowngrade(eq(cool12), any());
  }

  /** When the resident already outranks the whole run, every message is skipped. */
  @Test
  void handleCoolBatch_reportsEveryMessageWhenTheResidentOutranksTheRun() {
    cache.put("key1", hotEntryOwnedBy("node", 100L));
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    context = new DefaultWorkerContext(cache, loader, entryLifecycle, stampClock, List.of(hook));
    WorkerDecision cool10 = wireMessage("key1", WorkerType.COOL.code(), 10L);
    WorkerDecision cool7 = wireMessage("key1", WorkerType.COOL.code(), 7L);

    handleCoolBatch(List.of(cool10, cool7));

    CacheEntry resident = cache.asMap().get("key1");
    assertThat(resident).isNotNull();
    assertThat(resident.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(resident.getDecisionVersion()).isEqualTo(100L);
    verify(hook).onCoolSkipped(eq(cool10));
    verify(hook).onCoolSkipped(eq(cool7));
    verify(hook, never()).afterCoolDowngrade(any(), any());
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
    context = new DefaultWorkerContext(cache, loader, entryLifecycle, stampClock, List.of(hook), ring, healthView);
    WorkerDecision cool20 = workerMessageFrom("key1", WorkerType.COOL.code(), 20L, "worker-A");
    WorkerDecision cool30 = workerMessageFrom("key1", WorkerType.COOL.code(), 30L, "worker-A");

    handleCoolBatch(List.of(cool20, cool30));

    CacheEntry after = cache.asMap().get("key1");
    assertThat(after).isNotNull();
    assertThat(after.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(after.getDecisionVersion()).isEqualTo(10L);
    assertThat(after.getDecisionNodeId()).isEqualTo("worker-B");
    verify(hook).onCoolSkipped(eq(cool20));
    verify(hook).onCoolSkipped(eq(cool30));
    verify(hook, never()).afterCoolDowngrade(any(), any());
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
    context = new DefaultWorkerContext(cache, loader, entryLifecycle, stampClock, List.of(hook));
    WorkerDecision fromA = workerMessageFrom("key1", WorkerType.COOL.code(), 10L, "worker-A");
    WorkerDecision fromB = workerMessageFrom("key1", WorkerType.COOL.code(), 9L, "worker-B");

    handleCoolBatch(List.of(fromA, fromB));

    CacheEntry cooled = cache.asMap().get("key1");
    assertThat(cooled).isNotNull();
    assertThat(cooled.getDecisionNodeId()).isEqualTo("worker-B");
    assertThat(cooled.getDecisionVersion()).isEqualTo(9L);
    verify(hook).afterCoolDowngrade(eq(fromA), any());
    verify(hook).afterCoolDowngrade(eq(fromB), any());
    verify(hook, never()).onCoolSkipped(any());
  }

  /**
   * Verifies the create path preserves the Worker decision identity (C1): an
   * entry created by a Worker HOT decision carries nodeId/epoch, so
   * VersionGuard can compare against out-of-order replayed decisions instead
   * of accepting them unconditionally.
   */
  @Test
  void handleHot_entryAbsent_shouldPreserveDecisionIdentity() {
    handleHot(wireMessage("newkey", WorkerType.HOT.code(), 1L));

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
    handleHot(wireMessage("newkey", WorkerType.HOT.code(), 5L));
    CacheEntry before = (CacheEntry) cache.getIfPresent("newkey");
    assertThat(before.getDecisionVersion()).isEqualTo(5L);

    handleHot(wireMessage("newkey", WorkerType.HOT.code(), 3L));

    CacheEntry after = (CacheEntry) cache.getIfPresent("newkey");
    assertThat(after.getDecisionVersion()).isEqualTo(5L);
  }

  @Test
  void handleHot_staleVersion_shouldInvokeOnHotSkippedStale() {
    cache.put("key1", entry(5, KeyState.NORMAL));
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    context = new DefaultWorkerContext(cache, loader, entryLifecycle, stampClock, List.of(hook));

    handleHot(wireMessage("key1", WorkerType.HOT.code(), 3L));

    verify(hook).onHotSkipped(any(), eq(HotSkipReason.VERSION_STALE));
  }

  @Test
  void handleHot_valueNotFound_shouldInvokeOnHotSkippedValueNotFound() {
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    context = new DefaultWorkerContext(cache, k -> null, entryLifecycle, stampClock, List.of(hook));

    handleHot(wireMessage("missing", WorkerType.HOT.code(), 1L));

    verify(hook).onHotSkipped(any(), eq(HotSkipReason.VALUE_NOT_FOUND));
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
    context = new DefaultWorkerContext(mockedCache, k -> null, lifecycle, stampClock, List.of(hook));

    handleHot(wireMessage("key1", WorkerType.HOT.code(), 2L));

    verify(hook).onHotSkipped(any(), eq(HotSkipReason.VALUE_NOT_FOUND));
    verify(hook, never()).afterHotPromotion(any(), any());
  }

  /**
   * Verifies admission on the Worker plane: a value the cluster loader declines to cache is never
   * written by a HOT warm-up, even though the decision itself is valid. With no L1 entry to fall
   * back to, the HOT is answered like a missing value.
   */
  @Test
  void handleHot_admissionDeclined_shouldNotWriteEntryAndSkipHot() {
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    context = new DefaultWorkerContext(cache, decliningLoader("rejected"), entryLifecycle, stampClock, List.of(hook));

    handleHot(wireMessage("declined", WorkerType.HOT.code(), 1L));

    assertThat(cache.getIfPresent("declined")).as("a declined value must not enter L1").isNull();
    verify(hook).onHotSkipped(any(), eq(HotSkipReason.VALUE_NOT_FOUND));
  }

  /**
   * The declined admission takes the existing source-outage fallback: the resident L1 value is
   * promoted to HOT, never replaced by the value the application refused to cache.
   */
  @Test
  void handleHot_admissionDeclined_shouldPromoteExistingEntryInsteadOfRejectedValue() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    context = new DefaultWorkerContext(
      cache,
      decliningLoader("rejected"),
      entryLifecycle,
      stampClock,
      Collections.emptyList()
    );

    handleHot(wireMessage("key1", WorkerType.HOT.code(), 2L));

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
   * the promotion as stale (a newer decision landed during the loader read),
   * the hook may not fire. The loader bumps the
   * entry's decisionVersion between the outer pre-check and the atomic
   * compute, deterministically reproducing the stale-rejection interleaving.
   */
  @Test
  void handleHot_innerGuardRejectsPromotion_shouldNotFireAfterHotPromotionHook() {
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    cache.put("key1", entry(1, KeyState.NORMAL));
    CacheLoader bumpingLoader = k -> {
      // Simulates a concurrent newer decision landing during the loader read.
      cache.put("key1", entry(10, KeyState.NORMAL));
      return "fresh";
    };
    context = new DefaultWorkerContext(cache, bumpingLoader, entryLifecycle, stampClock, List.of(hook));

    handleHot(wireMessage("key1", WorkerType.HOT.code(), 2L));

    verify(hook, never()).afterHotPromotion(any(), any());
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
    context = new DefaultWorkerContext(cache, loader, entryLifecycle, stampClock, List.of(hook));

    handleHot(wireMessage("newkey", WorkerType.HOT.code(), 2L));

    ArgumentCaptor<CacheEntry> entryCaptor = ArgumentCaptor.forClass(CacheEntry.class);
    verify(hook).afterHotPromotion(any(), entryCaptor.capture());
    assertThat(entryCaptor.getValue().getDecisionVersion()).isEqualTo(2L);
    assertThat(entryCaptor.getValue().getKeyState()).isEqualTo(KeyState.HOT);
  }

  /**
   * Verifies the source-outage fallback (design decisions: version stamps): when the loader fails and the
   * L1 entry is a normal entry, its value is used for promotion.
   */
  @Test
  void handleHot_loaderFailure_shouldPromoteFromNormalL1Entry() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    context = new DefaultWorkerContext(cache, k -> null, entryLifecycle, stampClock, List.of(hook));

    handleHot(wireMessage("key1", WorkerType.HOT.code(), 2L));

    verify(hook).afterHotPromotion(any(), any());
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
    context = new DefaultWorkerContext(cache, k -> null, entryLifecycle, stampClock, List.of());

    handleHot(wireMessage("key1", WorkerType.HOT.code(), 2L));

    CacheEntry promoted = (CacheEntry) cache.getIfPresent("key1");
    assertThat(promoted.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(promoted.getHardExpireAtMs()).isEqualTo(hardExpireAt);
    assertThat(promoted.getSoftExpireAtMs()).isEqualTo(softExpireAt);
  }

  /**
   * Source-outage fallback: the L1 value is stored as-is (no codec) and the
   * promotion must preserve the exact instance.
   */
  @Test
  void handleHot_loaderFailure_shouldPreserveStoredFallbackValue() {
    String original = "zeta-fallback-value-".repeat(60);
    CacheEntry l1Entry = EntryDraft.of(entry(1, KeyState.NORMAL)).value(original).build();
    cache.put("key1", l1Entry);
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    context = new DefaultWorkerContext(cache, k -> null, entryLifecycle, stampClock, List.of(hook));

    handleHot(wireMessage("key1", WorkerType.HOT.code(), 2L));

    verify(hook).afterHotPromotion(any(), any());
    CacheEntry promoted = (CacheEntry) cache.getIfPresent("key1");
    assertThat(promoted).isNotNull();
    assertThat(promoted.getKeyState()).isEqualTo(KeyState.HOT);
    // The stored value must be the exact instance the L1 held.
    assertThat(promoted.getValue()).isSameAs(original);
  }

  /**
   * Verifies that a source outage with no L1 fallback value aborts the HOT
   * promotion — there is nothing of value to promote.
   */
  @Test
  void handleHot_loaderFailure_withoutFallback_shouldAbort() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    cache.invalidate("key1");
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    context = new DefaultWorkerContext(cache, k -> null, entryLifecycle, stampClock, List.of(hook));

    handleHot(wireMessage("key1", WorkerType.HOT.code(), 2L));

    verify(hook).onHotSkipped(any(), eq(HotSkipReason.VALUE_NOT_FOUND));
    assertThat(cache.getIfPresent("key1")).isNull();
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

    handleCool(wireMessage("key1", WorkerType.COOL.code(), 2L));

    CacheEntry ce = (CacheEntry) cache.getIfPresent("key1");
    assertThat(ce.getKeyState()).isEqualTo(KeyState.COOL);
    // The hard fallback is 120s; a missing/disabled normal soft TTL stays disabled.
    assertThat(ce.getHardTtlMs()).isEqualTo(120_000L);
    assertThat(ce.getSoftTtlMs()).isZero();
  }

  @Test
  void handleCool_noEntry_shouldInvokeOnCoolSkipped() {
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    context = new DefaultWorkerContext(cache, loader, entryLifecycle, stampClock, List.of(hook));

    handleCool(wireMessage("missing", WorkerType.COOL.code(), 1L));

    verify(hook).onCoolSkipped(any());
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
    context = new DefaultWorkerContext(cache, loader, entryLifecycle, stampClock, List.of(hook));

    handleCool(wireMessage("key1", WorkerType.COOL.code(), 6L));

    CacheEntry after = (CacheEntry) cache.getIfPresent("key1");
    assertThat(after).isSameAs(expired);
    assertThat(after.getKeyState()).isEqualTo(KeyState.HOT);
    verify(hook).onCoolSkipped(any());
    verify(hook, never()).afterCoolDowngrade(any(), any());
  }

  /**
   * The expiry guard must not block a live entry: a valid HOT entry still cools
   * normally (regression guard for the guard placement (design decisions: version comparison)).
   */
  @Test
  void handleCool_liveEntry_shouldStillCool() {
    cache.put("key1", entry(5, KeyState.HOT));
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    context = new DefaultWorkerContext(cache, loader, entryLifecycle, stampClock, List.of(hook));

    handleCool(wireMessage("key1", WorkerType.COOL.code(), 6L));

    CacheEntry ce = (CacheEntry) cache.getIfPresent("key1");
    assertThat(ce.getKeyState()).isEqualTo(KeyState.COOL);
    verify(hook).afterCoolDowngrade(any(), any());
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
    context = new DefaultWorkerContext(cache, loader, entryLifecycle, stampClock, List.of(hook));

    handleCool(wireMessage("key1", WorkerType.COOL.code(), 2L));

    CacheEntry after = (CacheEntry) cache.getIfPresent("key1");
    assertThat(after).isSameAs(dead);
    assertThat(after.getKeyState()).isEqualTo(KeyState.NORMAL);
    verify(hook).onCoolSkipped(any());
    verify(hook, never()).afterCoolDowngrade(any(), any());
  }

  @Test
  void multipleHooks_shouldAllBeInvoked() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    WorkerDecisionHook h1 = mock(WorkerDecisionHook.class);
    WorkerDecisionHook h2 = mock(WorkerDecisionHook.class);
    context = new DefaultWorkerContext(cache, loader, entryLifecycle, stampClock, List.of(h1, h2));

    handleHot(wireMessage("key1", WorkerType.HOT.code(), 2L));

    verify(h1).afterHotPromotion(any(), any());
    verify(h2).afterHotPromotion(any(), any());
  }

  @Test
  void hookException_shouldNotBreakOtherHooks() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    AtomicInteger count = new AtomicInteger(0);
    WorkerDecisionHook failingHook = new WorkerDecisionHook() {
      @Override
      public void afterHotPromotion(WorkerDecision decision, CacheEntry e) {
        throw new RuntimeException("fail");
      }
    };
    WorkerDecisionHook countingHook = new WorkerDecisionHook() {
      @Override
      public void afterHotPromotion(WorkerDecision decision, CacheEntry e) {
        count.incrementAndGet();
      }
    };
    context = new DefaultWorkerContext(cache, loader, entryLifecycle, stampClock, List.of(failingHook, countingHook));

    handleHot(wireMessage("key1", WorkerType.HOT.code(), 2L));

    assertThat(count.get()).isEqualTo(1);
  }

  @Test
  void customPolicy_shouldReplaceThePromotedValueAndRefuseCool() {
    WorkerPolicy custom = new WorkerPolicy() {
      @Override
      public HotAction hot(WorkerDecision decision, HotValue value) {
        return new HotAction.Promote(HotValue.loaded("custom-hot", value.dataVersion()));
      }

      @Override
      public boolean cool(WorkerDecision decision) {
        return false;
      }
    };

    WorkerDecision hot = wireMessage("k", WorkerType.HOT.code(), 1L);
    HotAction prepared = context.resolveHot(hot);
    HotAction action =
      prepared instanceof HotAction.Skip ? prepared : custom.hot(hot, ((HotAction.Promote) prepared).value());
    context.settleHot(hot, action);

    assertThat(cache.getIfPresent("k").getValue()).isEqualTo("custom-hot");
    assertThat(custom.cool(wireMessage("k", WorkerType.COOL.code(), 2L))).isFalse();
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
    context = new DefaultWorkerContext(cache, loader, entryLifecycle, stampClock, List.of(hook), ring, healthView);

    handleCool(workerMessageFrom("key1", WorkerType.COOL.code(), 3L, "worker-A"));

    CacheEntry after = (CacheEntry) cache.getIfPresent("key1");
    assertThat(after.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(after.getDecisionVersion()).isEqualTo(10L);
    assertThat(after.getDecisionNodeId()).isEqualTo("worker-B");
    verify(hook).onCoolSkipped(any());
    verify(hook, never()).afterCoolDowngrade(any(), any());
  }

  /**
   * A dead or restarted owner's HOT is legitimately coolable: both report
   * {@code isCurrentIncarnation == false}, so the ownership guard stays out of
   * the way and the cross-Worker COOL applies.
   */
  @Test
  void handleCool_crossNodeCoolWhenOwnerDead_shouldApply() {
    cache.put("key1", hotEntryOwnedBy("worker-B", 10L));
    ConsistentHashRing ring = mock(ConsistentHashRing.class);
    HealthView healthView = mock(HealthView.class);
    when(ring.locateNode(eq("key1"), any(HealthView.class))).thenReturn("worker-B");
    when(healthView.isCurrentIncarnation("worker-B", 1L)).thenReturn(false);
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    context = new DefaultWorkerContext(cache, loader, entryLifecycle, stampClock, List.of(hook), ring, healthView);

    handleCool(workerMessageFrom("key1", WorkerType.COOL.code(), 3L, "worker-A"));

    CacheEntry after = (CacheEntry) cache.getIfPresent("key1");
    assertThat(after.getKeyState()).isEqualTo(KeyState.COOL);
    verify(hook).afterCoolDowngrade(any(), any());
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
    context = new DefaultWorkerContext(cache, loader, entryLifecycle, stampClock, List.of(hook), ring, healthView);

    handleCool(workerMessageFrom("key1", WorkerType.COOL.code(), 3L, "worker-A"));

    CacheEntry after = (CacheEntry) cache.getIfPresent("key1");
    assertThat(after.getKeyState()).isEqualTo(KeyState.COOL);
    assertThat(after.getDecisionNodeId()).isEqualTo("worker-A");
    verify(hook).afterCoolDowngrade(any(), any());
  }

  /**
   * Fail-open: without the sharding beans (the 6-argument construction) a
   * cross-Worker COOL still applies exactly as before.
   */
  @Test
  void handleCool_crossNodeCoolWithoutShardingBeans_shouldApply() {
    cache.put("key1", hotEntryOwnedBy("worker-B", 10L));
    WorkerDecisionHook hook = mock(WorkerDecisionHook.class);
    context = new DefaultWorkerContext(cache, loader, entryLifecycle, stampClock, List.of(hook));

    handleCool(workerMessageFrom("key1", WorkerType.COOL.code(), 3L, "worker-A"));

    CacheEntry after = (CacheEntry) cache.getIfPresent("key1");
    assertThat(after.getKeyState()).isEqualTo(KeyState.COOL);
    verify(hook).afterCoolDowngrade(any(), any());
  }
}
