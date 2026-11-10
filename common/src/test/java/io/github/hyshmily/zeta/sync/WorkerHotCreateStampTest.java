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

import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.HEADER_EPOCH;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.HEADER_MESSAGE_ID;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.HEADER_NODE_ID;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.HEADER_TYPE;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.HEADER_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycleImpl;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.sync.worker.DefaultWorkerContext;
import io.github.hyshmily.zeta.sync.worker.WorkerDecision;
import io.github.hyshmily.zeta.sync.worker.WorkerType;
import io.github.hyshmily.zeta.util.clock.StampClock;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * design decisions: version stamps: the Worker HOT-create stamp is taken before the authoritative
 * load, so a write that lands during the load keeps the higher stamp.
 */
class WorkerHotCreateStampTest {

  @Test
  @DisplayName("HOT create uses the pre-load stamp and does not outrank a mid-load write")
  void hotCreate_usesPreLoadStamp() {
    Cache<String, CacheEntry> cache = Caffeine.newBuilder().maximumSize(100).build();
    EntryLifecycle lifecycle = new EntryLifecycleImpl(cache, new ZetaProperties(), null);
    AtomicLong sequence = new AtomicLong();
    StampClock clock = mock(StampClock.class);
    when(clock.nextStamp()).thenAnswer(inv -> sequence.incrementAndGet());
    when(clock.nextStamps(org.mockito.ArgumentMatchers.anyInt())).thenAnswer(inv -> {
      int n = inv.getArgument(0);
      return sequence.addAndGet(n) - n + 1;
    });

    long[] midLoadWriteStamp = new long[1];
    CacheLoader<String, Object> loader = key -> {
      // A write allocated while the authoritative load is in flight.
      midLoadWriteStamp[0] = clock.nextStamp();
      return "fresh";
    };

    DefaultWorkerContext context = new DefaultWorkerContext(cache, loader, lifecycle, clock, List.of());

    WorkerDecision decision = hotMessage("k");
    context.settleHot(decision, context.resolveHot(decision));

    CacheEntry created = (CacheEntry) cache.getIfPresent("k");
    assertThat(created).isNotNull();
    assertThat(created.getValue()).isEqualTo("fresh");
    assertThat(created.getDataVersion()).isEqualTo(1L);
    assertThat(midLoadWriteStamp[0]).isEqualTo(2L);
    assertThat(created.getDataVersion()).isLessThan(midLoadWriteStamp[0]);
  }

  @Test
  @DisplayName("HOT create is refused when the key is invalidated during the authoritative load")
  void hotCreate_refusedAfterInvalidationDuringLoad() {
    Cache<String, CacheEntry> cache = Caffeine.newBuilder().maximumSize(100).build();
    EntryLifecycle lifecycle = new EntryLifecycleImpl(cache, new ZetaProperties(), null);
    AtomicLong sequence = new AtomicLong();
    StampClock clock = mock(StampClock.class);
    when(clock.nextStamp()).thenAnswer(inv -> sequence.incrementAndGet());

    CacheLoader<String, Object> loader = key -> {
      // An INVALIDATE lands while the authoritative load is in flight: the
      // absent entry has nothing to remove, and the tombstone outranks the
      // pre-read create stamp.
      lifecycle.recordInvalidation(key, clock.nextStamp());
      return "stale";
    };

    DefaultWorkerContext context = new DefaultWorkerContext(cache, loader, lifecycle, clock, List.of());
    WorkerDecision decision = hotMessage("k");
    context.settleHot(decision, context.resolveHot(decision));

    assertThat(cache.getIfPresent("k")).isNull();
  }

  @Test
  @DisplayName("HOT create is refused when a PRE-EXISTING entry is invalidated during the load")
  void hotCreate_existingEntryInvalidatedDuringLoad_isRefused() {
    Cache<String, CacheEntry> cache = Caffeine.newBuilder().maximumSize(100).build();
    EntryLifecycle lifecycle = new EntryLifecycleImpl(cache, new ZetaProperties(), null);
    AtomicLong sequence = new AtomicLong();
    StampClock clock = mock(StampClock.class);
    when(clock.nextStamp()).thenAnswer(inv -> sequence.incrementAndGet());

    // A resident exists when the DCL snapshot is taken, so the HOT carries the
    // pre-read stamp and not a commit-time one. The create must lose to the
    // tombstone recorded during the load: the pre-invalidation value must not
    // come back as a brand-new HOT entry.
    cache.put("k", lifecycle.newEntry().value("old").version(1L).ttl(60_000L, 30_000L, 60_000L, 30_000L).build());

    CacheLoader<String, Object> loader = key -> {
      // Another node invalidates while the authoritative read is in flight:
      // record the tombstone at a stamp above the one read before the load,
      // then drop the local copy.
      lifecycle.recordInvalidation(key, clock.nextStamp());
      cache.invalidate(key);
      return "fresh";
    };

    DefaultWorkerContext context = new DefaultWorkerContext(cache, loader, lifecycle, clock, List.of());
    WorkerDecision decision = hotMessage("k");
    context.settleHot(decision, context.resolveHot(decision));

    assertThat(cache.getIfPresent("k")).isNull();
  }

  @Test
  @DisplayName("Same-version HOT rebroadcast re-promotes an entry demoted to NORMAL")
  void hotRebroadcast_rePromotesLocallyDemotedEntry() {
    Cache<String, CacheEntry> cache = Caffeine.newBuilder().maximumSize(100).build();
    EntryLifecycle lifecycle = new EntryLifecycleImpl(cache, new ZetaProperties(), null);
    StampClock clock = mock(StampClock.class);
    when(clock.nextStamp()).thenReturn(1L);

    // A local lifecycle transition (e.g. a background refresh) demoted the
    // entry to NORMAL while keeping its decision stamp. The Worker's periodic
    // rebroadcast carries exactly the same (nodeId, epoch, decisionVersion), so
    // the version guard alone would skip it forever.
    CacheEntry hot = lifecycle
      .newEntry()
      .value("v")
      .version(10L)
      .decision(DecisionStamp.hot(5L, "w1", 1L))
      .keyState(KeyState.HOT)
      .ttl(60_000L, 30_000L, 10_000L, 5_000L)
      .build();
    cache.put("k", lifecycle.editEntry(hot).keyState(KeyState.NORMAL).build());

    DefaultWorkerContext context = new DefaultWorkerContext(cache, null, lifecycle, clock, List.of());
    WorkerDecision decision = hotMessage("k", 5L);
    context.settleHot(decision, context.resolveHot(decision));

    CacheEntry promoted = (CacheEntry) cache.getIfPresent("k");
    assertThat(promoted).isNotNull();
    assertThat(promoted.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(promoted.getDecisionVersion()).isEqualTo(5L);
    assertThat(promoted.getDecisionNodeId()).isEqualTo("w1");
    assertThat(promoted.getDataVersion()).isEqualTo(10L);
  }

  @Test
  @DisplayName("HOT create from an L1 fallback keeps the resident data version, not the pre-load stamp")
  @SuppressWarnings("unchecked")
  void hotCreate_fromFallback_keepsTheResidentStamp() {
    Cache<String, CacheEntry> cache = mock(Cache.class);
    ConcurrentMap<String, CacheEntry> table = mock(ConcurrentMap.class);
    when(cache.asMap()).thenReturn(table);

    EntryLifecycle lifecycle = new EntryLifecycleImpl(
      Caffeine.newBuilder().maximumSize(100).build(),
      new ZetaProperties(),
      null
    );
    CacheEntry resident = lifecycle.newEntry().value("old").version(5L).ttl(60_000L, 30_000L, 60_000L, 30_000L).build();
    when(table.get("k")).thenReturn(resident);

    // The concurrent INVALIDATE lands between the fallback read and the compute: its
    // tombstone (6) is above the resident's version (5) but below the pre-load stamp (7).
    lifecycle.recordInvalidation("k", 6L);
    AtomicReference<CacheEntry> stored = new AtomicReference<>();
    when(table.compute(eq("k"), any())).thenAnswer(invocation -> {
      BiFunction<String, CacheEntry, CacheEntry> mutation = invocation.getArgument(1);
      CacheEntry next = mutation.apply("k", null);
      stored.set(next);
      return next;
    });

    StampClock clock = mock(StampClock.class);
    when(clock.nextStamp()).thenReturn(7L);
    DefaultWorkerContext context = new DefaultWorkerContext(
      cache,
      key -> null,
      lifecycle,
      clock,
      List.of()
    );

    WorkerDecision decision = hotMessage("k");
    context.settleHot(decision, context.resolveHot(decision));

    assertThat(stored.get())
      .as("a pre-invalidation fallback value must not be re-signed with the pre-load stamp")
      .isNull();
  }

  private static WorkerDecision hotMessage(String key) {
    return hotMessage(key, 1L);
  }

  private static WorkerDecision hotMessage(String key, long decisionVersion) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, WorkerType.HOT.code());
    props.setHeader(HEADER_VERSION, decisionVersion);
    props.setHeader(HEADER_NODE_ID, "w1");
    props.setHeader(HEADER_EPOCH, 1L);
    props.setHeader(HEADER_MESSAGE_ID, 1L);
    return WorkerDecision.from(new Message(key.getBytes(StandardCharsets.UTF_8), props));
  }
}
