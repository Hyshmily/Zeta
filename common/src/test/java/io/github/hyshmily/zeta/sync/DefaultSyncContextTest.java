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
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.EntryView;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycleImpl;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.sync.local.Batch;
import io.github.hyshmily.zeta.sync.local.CacheSyncProperties;
import io.github.hyshmily.zeta.sync.local.DefaultSyncContext;
import io.github.hyshmily.zeta.sync.local.PutPayloadCodec;
import io.github.hyshmily.zeta.sync.local.SyncAdmission;
import io.github.hyshmily.zeta.sync.local.SyncEvent;
import io.github.hyshmily.zeta.sync.local.SyncHook;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.sync.local.SyncPolicy;
import io.github.hyshmily.zeta.sync.local.SyncSkipReason;
import io.github.hyshmily.zeta.sync.local.SyncType;
import io.github.hyshmily.zeta.sync.local.SyncValueAction;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.clock.StampClock;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * Contract tests for {@link DefaultSyncContext}: one granted batch in,
 * the highest-version message applied under the version/tombstone guards, one
 * terminal hook per message.
 */
class DefaultSyncContextTest {

  private Cache<String, CacheEntry> cache;
  private EntryLifecycleImpl entryLifecycle;
  private PutPayloadCodec codec;
  private DefaultSyncContext context;

  @BeforeEach
  void setUp() {
    cache = Caffeine.newBuilder().maximumSize(100).build();
    entryLifecycle = new EntryLifecycleImpl(cache, new ZetaProperties(), null);
    codec = new PutPayloadCodec(new CacheSyncProperties());
    context = newContext(k -> "refreshed", List.of());
  }

  private DefaultSyncContext newContext(CacheLoader<String, Object> loader, List<SyncHook> hooks) {
    return newContext(loader, hooks, null);
  }

  private DefaultSyncContext newContext(
    CacheLoader<String, Object> loader,
    List<SyncHook> hooks,
    StampClock stampClock
  ) {
    return new DefaultSyncContext(cache, loader, entryLifecycle, hooks, null, codec, Runnable::run, stampClock);
  }

  private final SyncPolicy policy = new SyncPolicy() {};

  private void handleBatch(List<SyncMessage> messages) {
    context.apply(Batch.reduce(messages), policy);
  }

  private static SyncMessage msg(String key, String type, long version) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, type);
    props.setHeader(HEADER_VERSION, version);
    props.setHeader(HEADER_MESSAGE_ID, 1L);
    return SyncMessage.from(new Message(key.getBytes(StandardCharsets.UTF_8), props));
  }

  private SyncMessage put(String key, Object value, long version) {
    byte[] payload = codec.encode(value);
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, SyncType.PUT.code());
    props.setHeader(HEADER_VERSION, version);
    props.setHeader(HEADER_MESSAGE_ID, 1L);
    props.setHeader(HEADER_CACHE_KEY, key);
    return SyncMessage.from(new Message(payload, props));
  }

  private static CacheEntry entry(long dataVersion, KeyState state) {
    return EntryDraft.blank(null)
      .value("v")
      .version(dataVersion)
      .decision(DecisionStamp.hot(0, null, 0L))
      .ttl(300_000, 30_000, 300_000, 30_000)
      .expiryAt(Long.MAX_VALUE, 30_000)
      .keyState(state)
      .build();
  }

  @Test
  void refresh_reloadsAndFiresTheAppliedHook() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    context = newContext(k -> "refreshed", List.of(hook));

    handleBatch(List.of(msg("key1", SyncType.REFRESH.code(), 2L)));

    CacheEntry applied = cache.getIfPresent("key1");
    assertThat(applied).isNotNull();
    assertThat(applied.getValue()).isEqualTo("refreshed");
    assertThat(applied.getDataVersion()).isEqualTo(2L);
    verify(hook).afterValueApplied(any(), any());
  }

  /** An accepted peer stamp is already orderable locally and must not spend an allocation. */
  @Test
  void acceptedPeerVersion_isNotRestamped() {
    StampClock clock = mock(StampClock.class);
    when(clock.observe(anyLong())).thenReturn(true);
    context = newContext(k -> "refreshed", List.of(), clock);

    handleBatch(List.of(msg("key1", SyncType.REFRESH.code(), 2L)));

    assertThat(cache.getIfPresent("key1").getDataVersion()).isEqualTo(2L);
    verify(clock, never()).nextStamp();
  }

  /**
   * The batch reduction compares the peer versions before any replacement stamp exists. The
   * refused winner is re-stamped once, on the per-key apply; the covered message never
   * allocates, so an older covered mutation cannot outrank the winner.
   */
  @Test
  void overDriftBatch_isReducedOnPeerVersionsBeforeOneRestamp() {
    StampClock clock = mock(StampClock.class);
    when(clock.observe(anyLong())).thenReturn(false);
    when(clock.nextStamp()).thenReturn(42L);
    context = newContext(k -> null, List.of(), clock);
    cache.put("key1", entry(1, KeyState.NORMAL));

    handleBatch(List.of(msg("key1", SyncType.INVALIDATE.code(), 900L), msg("key1", SyncType.INVALIDATE.code(), 800L)));

    assertThat(cache.getIfPresent("key1")).isNull();
    assertThat(entryLifecycle.staleAgainstInvalidation("key1", 42L)).isTrue();
    assertThat(entryLifecycle.staleAgainstInvalidation("key1", 43L)).isFalse();
    verify(clock, times(1)).nextStamp();
  }

  /** A clear hook reports the stamp the sweep applied, not the refused peer stamp. */
  @Test
  void overDriftClear_reportsTheAppliedVersionToTheHook() {
    StampClock clock = mock(StampClock.class);
    when(clock.observe(anyLong())).thenReturn(false);
    when(clock.nextStamp()).thenReturn(42L);
    SyncHook hook = mock(SyncHook.class);
    context = newContext(k -> null, List.of(hook), clock);
    cache.put("low", entry(3, KeyState.NORMAL));

    handleBatch(List.of(msg("clear", SyncType.INVALIDATE_ALL.code(), 900L)));

    assertThat(cache.getIfPresent("low")).isNull();
    verify(hook).afterClear(argThat(events -> events.size() == 1 && events.get(0).version() == 42L), eq(1));
  }

  @Test
  void refresh_staleThanResident_skipsAndReports() {
    cache.put("key1", entry(5, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    context = newContext(k -> "refreshed", List.of(hook));

    handleBatch(List.of(msg("key1", SyncType.REFRESH.code(), 3L)));

    assertThat(cache.getIfPresent("key1").getValue()).isEqualTo("v");
    verify(hook).onSkipped(any(), any());
  }

  @Test
  void refresh_withoutValue_invalidatesLocally() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    context = newContext(k -> null, List.of(hook));

    handleBatch(List.of(msg("key1", SyncType.REFRESH.code(), 2L)));

    assertThat(cache.getIfPresent("key1")).isNull();
    verify(hook).afterValueDegraded(any(), eq(SyncSkipReason.VALUE_NOT_FOUND));
    verify(hook, never()).afterInvalidate(any());
  }

  @Test
  void refreshFailureAfterExplicitInvalidate_hardRemovesWorkerManagedEntry() {
    cache.put("key1", entry(1, KeyState.HOT));
    context = newContext(k -> null, List.of());

    handleBatch(List.of(msg("key1", SyncType.INVALIDATE.code(), 10L), msg("key1", SyncType.REFRESH.code(), 11L)));

    // The covered INVALIDATE still condemns the value: the newer REFRESH's
    // no-value fallback must not soft-expire the HOT entry and keep serving it.
    assertThat(cache.getIfPresent("key1")).isNull();
  }

  @Test
  void refreshFailureWithoutExplicitInvalidate_softExpiresWorkerManagedEntry() {
    cache.put("key1", entry(1, KeyState.HOT));
    context = newContext(k -> null, List.of());

    handleBatch(List.of(msg("key1", SyncType.REFRESH.code(), 2L)));

    CacheEntry entry = cache.getIfPresent("key1");
    assertThat(entry).isNotNull();
    assertThat(entry.getSoftExpireAtMs()).isLessThan(System.currentTimeMillis());
  }

  /**
   * The closed soft window must stay on the wall-clock axis the read path uses. A backward wall
   * step makes the monotonic axis run ahead; writing the deadline from `monotonicMillis()` would
   * put it in the future of `isSoftExpired` and keep the stale entry servable.
   */
  @Test
  void refreshFailureSoftExpiryUsesTheWallClockAxis() {
    try {
      TimeSource.setTimeOffsetForTest(-60_000L, 0L);
      cache.put("key1", entry(1, KeyState.HOT));
      context = newContext(k -> null, List.of());

      handleBatch(List.of(msg("key1", SyncType.REFRESH.code(), 2L)));

      CacheEntry entry = cache.getIfPresent("key1");
      assertThat(entry).isNotNull();
      assertThat(entry.getSoftExpireAtMs()).isLessThan(TimeSource.currentTimeMillis());
      assertThat(entryLifecycle.ttlPolicy().isSoftExpired(entry)).isTrue();
    } finally {
      TimeSource.setTimeOffsetForTest(0L, 0L);
    }
  }

  @Test
  void refreshFailureOnHardExpiredWorkerManagedEntry_removesItInsteadOfRewriting() {
    CacheEntry expired = EntryDraft.of(entry(7, KeyState.HOT))
      .hardExpiryAt(TimeSource.currentTimeMillis() - 1L)
      .build();
    cache.put("key1", expired);
    SingleFlight flight = mock(SingleFlight.class);
    context = new DefaultSyncContext(cache, k -> null, entryLifecycle, List.of(), flight, codec);

    handleBatch(List.of(msg("key1", SyncType.REFRESH.code(), 6L)));

    assertThat(cache.getIfPresent("key1")).isNull();
    assertThat(entryLifecycle.staleAgainstInvalidation("key1", 6L)).isTrue();
    verify(flight).invalidate("key1");
  }

  @Test
  void refresh_declinedByLoaderAdmission_invalidatesLocally() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    context = newContext(decliningLoader(), List.of(hook));

    handleBatch(List.of(msg("key1", SyncType.REFRESH.code(), 2L)));

    assertThat(cache.getIfPresent("key1")).isNull();
    verify(hook).afterValueDegraded(any(), eq(SyncSkipReason.VALUE_DECLINED_BY_LOADER));
  }

  /**
   * The loader admission SPI is fallible: an exception on a decoded PUT value is a refusal, not
   * an applier escape. The event still gets its one terminal hook and the versioned invalidation
   * fallback.
   */
  @Test
  void put_admissionThrows_invalidatesLocallyAndFiresTheDegradedHook() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    context = newContext(
      new CacheLoader<>() {
        @Override
        public Object load(String key) {
          return "loaded";
        }

        @Override
        public boolean isCacheable(Object candidate) {
          throw new IllegalStateException("admission down");
        }
      },
      List.of(hook)
    );

    handleBatch(List.of(put("key1", "carried", 2L)));

    assertThat(cache.getIfPresent("key1")).isNull();
    verify(hook).afterValueDegraded(any(), eq(SyncSkipReason.VALUE_DECLINED_BY_LOADER));
    verify(hook, never()).afterValueApplied(any(), any());
    verify(hook, never()).onSkipped(any(), any());
  }

  @Test
  void refresh_batchKeepsOnlyTheHighestVersion() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    AtomicInteger loads = new AtomicInteger();
    context = newContext(
      k -> {
        loads.incrementAndGet();
        return "v" + loads.get();
      },
      List.of()
    );

    handleBatch(
      List.of(
        msg("key1", SyncType.REFRESH.code(), 2L),
        msg("key1", SyncType.REFRESH.code(), 4L),
        msg("key1", SyncType.REFRESH.code(), 3L)
      )
    );

    assertThat(loads).hasValue(1);
    assertThat(cache.getIfPresent("key1").getDataVersion()).isEqualTo(4L);
  }

  @Test
  void put_absentKey_createsANormalEntry() {
    handleBatch(List.of(put("key1", "carried", 5L)));

    CacheEntry applied = cache.getIfPresent("key1");
    assertThat(applied).isNotNull();
    assertThat(applied.getValue()).isEqualTo("carried");
    assertThat(applied.getDataVersion()).isEqualTo(5L);
    assertThat(applied.getKeyState()).isEqualTo(KeyState.NORMAL);
  }

  @Test
  void put_staleVersion_neverRegresses() {
    cache.put("key1", entry(7, KeyState.NORMAL));

    handleBatch(List.of(put("key1", "carried", 5L)));

    assertThat(cache.getIfPresent("key1").getValue()).isEqualTo("v");
    assertThat(cache.getIfPresent("key1").getDataVersion()).isEqualTo(7L);
  }

  /** A stale PUT is rejected before its payload is deserialized. */
  @Test
  void put_staleVersion_shouldNotDeserializeThePayload() {
    cache.put("key1", entry(7, KeyState.NORMAL));
    PutPayloadCodec parser = mock(PutPayloadCodec.class);
    context = new DefaultSyncContext(cache, k -> "refreshed", entryLifecycle, List.of(), null, parser);

    handleBatch(List.of(put("key1", "carried", 5L)));

    verifyNoInteractions(parser);
    assertThat(cache.getIfPresent("key1").getValue()).isEqualTo("v");
  }

  @Test
  void put_equalVersion_applies() {
    cache.put("key1", entry(5, KeyState.NORMAL));

    handleBatch(List.of(put("key1", "carried", 5L)));

    assertThat(cache.getIfPresent("key1").getValue()).isEqualTo("carried");
  }

  @Test
  void put_afterInvalidate_doesNotRewarm() {
    handleBatch(List.of(msg("key1", SyncType.INVALIDATE.code(), 6L)));

    handleBatch(List.of(put("key1", "carried", 5L)));

    assertThat(cache.getIfPresent("key1")).isNull();
  }

  /**
   * An unreadable PUT degrades to a versioned eviction, but the message still has exactly
   * one terminal hook: {@code afterValueDegraded}, never {@code afterInvalidate} or
   * {@code onSkipped}.
   */
  @Test
  void put_unreadablePayload_firesOnlyTheDegradedHook() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    PutPayloadCodec unreadable = mock(PutPayloadCodec.class);
    when(unreadable.decode(any())).thenReturn(null);
    context = new DefaultSyncContext(cache, k -> "refreshed", entryLifecycle, List.of(hook), null, unreadable);

    handleBatch(List.of(put("key1", "carried", 2L)));

    assertThat(cache.getIfPresent("key1")).isNull();
    verify(hook).afterValueDegraded(any(), eq(SyncSkipReason.PAYLOAD_UNREADABLE));
    verify(hook, never()).afterInvalidate(any());
    verify(hook, never()).afterValueApplied(any(), any());
    verify(hook, never()).onSkipped(any(), any());
  }

  /**
   * A REFRESH whose load yields nothing removes the resident through the internal
   * fallback, but the fallback is not a peer INVALIDATE: the message reports one terminal
   * {@code afterValueDegraded} and no {@code afterInvalidate}.
   */
  @Test
  void refresh_emptyLoad_withResident_firesOnlyTheDegradedHook() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    context = newContext(k -> null, List.of(hook));

    handleBatch(List.of(msg("key1", SyncType.REFRESH.code(), 2L)));

    assertThat(cache.getIfPresent("key1")).isNull();
    verify(hook).afterValueDegraded(any(), eq(SyncSkipReason.VALUE_NOT_FOUND));
    verify(hook, never()).afterInvalidate(any());
  }

  @Test
  void invalidate_removesAndFiresTheHook() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    context = newContext(k -> "refreshed", List.of(hook));

    handleBatch(List.of(msg("key1", SyncType.INVALIDATE.code(), 2L)));

    assertThat(cache.getIfPresent("key1")).isNull();
    verify(hook).afterInvalidate(any());
  }

  @Test
  void invalidate_staleVersion_keepsTheNewerEntry() {
    cache.put("key1", entry(7, KeyState.NORMAL));

    handleBatch(List.of(msg("key1", SyncType.INVALIDATE.code(), 3L)));

    assertThat(cache.getIfPresent("key1")).isNotNull();
  }

  /**
   * A logically expired resident is absent for the per-key guard: the invalidation must still
   * record its tombstone, or an older in-flight fill could land after the dead entry is
   * treated as absent by the write guard.
   */
  @Test
  void invalidate_removesAnExpiredResidentAndRecordsTheTombstone() {
    CacheEntry expired = EntryDraft.of(entry(7, KeyState.NORMAL))
      .hardExpiryAt(TimeSource.currentTimeMillis() - 1L)
      .build();
    cache.put("key1", expired);
    SyncHook hook = mock(SyncHook.class);
    SingleFlight flight = mock(SingleFlight.class);
    context = new DefaultSyncContext(cache, k -> "refreshed", entryLifecycle, List.of(hook), flight, codec);

    handleBatch(List.of(msg("key1", SyncType.INVALIDATE.code(), 6L)));

    assertThat(cache.getIfPresent("key1")).isNull();
    assertThat(entryLifecycle.staleAgainstInvalidation("key1", 6L)).isTrue();
    verify(hook).afterInvalidate(any());
    verify(flight).invalidate("key1");
  }

  /** The clear watermark already protects fills; the sweep also removes a dead resident. */
  @Test
  void clearAll_removesAnExpiredResidentAboveTheClearStamp() {
    CacheEntry expired = EntryDraft.of(entry(7, KeyState.NORMAL))
      .hardExpiryAt(TimeSource.currentTimeMillis() - 1L)
      .build();
    cache.put("key1", expired);

    handleBatch(List.of(msg("clear", SyncType.INVALIDATE_ALL.code(), 6L)));

    assertThat(cache.getIfPresent("key1")).isNull();
  }

  /**
   * At the same version the INVALIDATE outranks the REFRESH, matching the publisher's
   * {@code BroadcastClaim} rule; arrival order inside the batch must not flip the winner.
   */
  @Test
  void sameVersionInvalidateOutranksRefreshInEitherOrder() {
    SyncHook hook = mock(SyncHook.class);
    context = newContext(k -> "refreshed", List.of(hook));
    SyncMessage invalidate = msg("key1", SyncType.INVALIDATE.code(), 5L);
    SyncMessage refresh = msg("key1", SyncType.REFRESH.code(), 5L);

    for (List<SyncMessage> messages : List.of(List.of(invalidate, refresh), List.of(refresh, invalidate))) {
      cache.put("key1", entry(1, KeyState.NORMAL));
      handleBatch(messages);

      assertThat(cache.getIfPresent("key1")).isNull();
    }

    verify(hook, times(2)).afterInvalidate(any());
    verify(hook, never()).afterValueApplied(any(), any());
  }

  /**
   * A covered INVALIDATE of a fallback eviction stays superseded: the value event reports
   * {@code afterValueDegraded} and the covered removal only {@code onSkipped}, because the
   * covered instruction never won the batch.
   */
  @Test
  void coveredInvalidateOfAFallbackEvictionIsReportedSuperseded() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    context = newContext(k -> null, List.of(hook));

    handleBatch(List.of(msg("key1", SyncType.INVALIDATE.code(), 2L), msg("key1", SyncType.REFRESH.code(), 3L)));

    assertThat(cache.getIfPresent("key1")).isNull();
    verify(hook).afterValueDegraded(any(), eq(SyncSkipReason.VALUE_NOT_FOUND));
    verify(hook).onSkipped(any(), eq(SyncSkipReason.COVERED_BY_NEWER_EVENT));
    verify(hook, never()).afterInvalidate(any());
  }

  /**
   * A batch of two INVALIDATEs settles both messages: the winner removes the entry and the
   * covered one is reported superseded, so neither is left without a terminal callback.
   */
  @Test
  void coveredInvalidateOfAnInvalidateWinnerIsReportedSkipped() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    context = newContext(k -> "refreshed", List.of(hook));

    handleBatch(List.of(msg("key1", SyncType.INVALIDATE.code(), 5L), msg("key1", SyncType.INVALIDATE.code(), 6L)));

    assertThat(cache.getIfPresent("key1")).isNull();
    verify(hook).afterInvalidate(any());
    verify(hook).onSkipped(any(), eq(SyncSkipReason.COVERED_BY_NEWER_EVENT));
  }

  /** Every covered INVALIDATE owns a terminal callback, not just the highest one. */
  @Test
  void everyCoveredInvalidateGetsATerminalCallback() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    context = newContext(k -> "refreshed", List.of(hook));

    handleBatch(
      List.of(
        msg("key1", SyncType.INVALIDATE.code(), 8L),
        msg("key1", SyncType.INVALIDATE.code(), 9L),
        put("key1", "carried", 10L)
      )
    );

    assertThat(cache.getIfPresent("key1").getValue()).isEqualTo("carried");
    verify(hook, times(2)).onSkipped(any(), eq(SyncSkipReason.COVERED_BY_NEWER_EVENT));
    verify(hook).afterValueApplied(any(), any());
  }

  /**
   * A winning REFRESH the policy refuses must not swallow the INVALIDATE the batch already
   * carried: the covered removal still evicts the resident, while the winner reports its own
   * policy rejection.
   */
  @Test
  void rejectedValueAdmissionStillAppliesTheCoveredInvalidate() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    context = newContext(k -> "refreshed", List.of(hook));
    SyncPolicy rejecting = new SyncPolicy() {
      @Override
      public SyncAdmission admit(SyncEvent event, EntryView current) {
        return SyncAdmission.SKIP;
      }
    };

    context.apply(
      Batch.reduce(List.of(msg("key1", SyncType.INVALIDATE.code(), 2L), msg("key1", SyncType.REFRESH.code(), 3L))),
      rejecting
    );

    assertThat(cache.getIfPresent("key1")).isNull();
    verify(hook).onSkipped(any(), eq(SyncSkipReason.COVERED_BY_NEWER_EVENT));
    verify(hook).onSkipped(any(), eq(SyncSkipReason.POLICY_REJECTED));
    verify(hook, never()).afterValueApplied(any(), any());
    verify(hook, never()).afterInvalidate(any());
  }

  /** The same fallback applies when the policy refuses the resolved value. */
  @Test
  void rejectedResolvedValueStillAppliesTheCoveredInvalidate() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    context = newContext(k -> "refreshed", List.of(hook));
    SyncPolicy rejecting = new SyncPolicy() {
      @Override
      public SyncValueAction settle(SyncEvent event, Object resolvedValue, EntryView current) {
        return SyncValueAction.SKIP;
      }
    };

    context.apply(
      Batch.reduce(List.of(msg("key1", SyncType.INVALIDATE.code(), 2L), put("key1", "carried", 3L))),
      rejecting
    );

    assertThat(cache.getIfPresent("key1")).isNull();
    verify(hook).onSkipped(any(), eq(SyncSkipReason.COVERED_BY_NEWER_EVENT));
    verify(hook).onSkipped(any(), eq(SyncSkipReason.POLICY_REJECTED));
    verify(hook, never()).afterValueApplied(any(), any());
  }

  /** A null policy result is a broken policy, not an approval: it settles as POLICY_FAILED. */
  @Test
  void nullPolicyAdmissionSettlesAsPolicyFailed() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    context = newContext(k -> "refreshed", List.of(hook));
    SyncPolicy broken = new SyncPolicy() {
      @Override
      public SyncAdmission admit(SyncEvent event, EntryView current) {
        return null;
      }
    };

    context.apply(Batch.reduce(List.of(put("key1", "carried", 2L))), broken);

    assertThat(cache.getIfPresent("key1").getValue()).isEqualTo("v");
    verify(hook).onSkipped(any(), eq(SyncSkipReason.POLICY_FAILED));
    verify(hook, never()).afterValueApplied(any(), any());
  }

  /** A thrown policy failure still runs the covered-invalidate fallback exactly once. */
  @Test
  void throwingSettleSettlesAsPolicyFailedAndAppliesTheCoveredInvalidate() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    context = newContext(k -> "refreshed", List.of(hook));
    SyncPolicy broken = new SyncPolicy() {
      @Override
      public SyncValueAction settle(SyncEvent event, Object resolvedValue, EntryView current) {
        throw new IllegalStateException("policy down");
      }
    };

    context.apply(
      Batch.reduce(List.of(msg("key1", SyncType.INVALIDATE.code(), 2L), put("key1", "carried", 3L))),
      broken
    );

    assertThat(cache.getIfPresent("key1")).isNull();
    verify(hook).onSkipped(any(), eq(SyncSkipReason.COVERED_BY_NEWER_EVENT));
    verify(hook).onSkipped(any(), eq(SyncSkipReason.POLICY_FAILED));
    verify(hook, never()).afterValueApplied(any(), any());
  }

  @Test
  void clearAll_removesAtOrBelowTheStampOnly() {
    cache.put("low", entry(3, KeyState.NORMAL));
    cache.put("high", entry(12, KeyState.NORMAL));

    handleBatch(List.of(msg("clear", SyncType.INVALIDATE_ALL.code(), 9L)));

    assertThat(cache.getIfPresent("low")).isNull();
    assertThat(cache.getIfPresent("high")).isNotNull();
  }

  @Test
  void clearPrefix_removesOnlyTheNamespace() {
    cache.put("users\u0000a", entry(1, KeyState.NORMAL));
    cache.put("users\u0000b", entry(2, KeyState.NORMAL));
    cache.put("orders\u0000a", entry(1, KeyState.NORMAL));

    handleBatch(List.of(msg("users\u0000", SyncType.INVALIDATE_PREFIX.code(), 5L)));

    assertThat(cache.getIfPresent("users\u0000a")).isNull();
    assertThat(cache.getIfPresent("users\u0000b")).isNull();
    assertThat(cache.getIfPresent("orders\u0000a")).isNotNull();
  }

  @Test
  void remoteInvalidateDropsAColdKeysInFlightLoad() {
    SingleFlight flight = mock(SingleFlight.class);
    context = new DefaultSyncContext(cache, k -> "refreshed", entryLifecycle, List.of(), flight, codec);

    handleBatch(List.of(msg("cold", SyncType.INVALIDATE.code(), 5L)));

    verify(flight).invalidate("cold");
  }

  @Test
  void remoteStaleInvalidateLeavesANewerResidentsFlightAlone() {
    SingleFlight flight = mock(SingleFlight.class);
    context = new DefaultSyncContext(cache, k -> "refreshed", entryLifecycle, List.of(), flight, codec);
    cache.put("key1", entry(7, KeyState.NORMAL));

    handleBatch(List.of(msg("key1", SyncType.INVALIDATE.code(), 3L)));

    verify(flight, never()).invalidate("key1");
  }

  @Test
  void remoteClearDropsTheMatchingDedupFlights() {
    SingleFlight flight = mock(SingleFlight.class);
    context = new DefaultSyncContext(cache, k -> "refreshed", entryLifecycle, List.of(), flight, codec);

    handleBatch(
      List.of(
        msg("clear", SyncType.INVALIDATE_ALL.code(), 9L),
        msg("users\u0000", SyncType.INVALIDATE_PREFIX.code(), 7L)
      )
    );

    // The table-wide kill covers a prefix clear at a lower stamp: a second
    // per-prefix walk would scan an already-emptied table.
    verify(flight).invalidateAll();
    verify(flight, never()).invalidatePrefix("users\u0000");
  }

  @Test
  void remotePrefixClearDropsOnlyThatNamespacesFlights() {
    SingleFlight flight = mock(SingleFlight.class);
    context = new DefaultSyncContext(cache, k -> "refreshed", entryLifecycle, List.of(), flight, codec);

    handleBatch(List.of(msg("users\u0000", SyncType.INVALIDATE_PREFIX.code(), 7L)));

    verify(flight).invalidatePrefix("users\u0000");
    verify(flight, never()).invalidateAll();
  }

  /** The consumer-thread fallback applies the removal without invoking user code. */
  @Test
  void invalidateNowRemovesWithoutFiringHooks() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    context = newContext(k -> "refreshed", List.of(hook));

    context.invalidateNow(msg("key1", SyncType.INVALIDATE.code(), 2L));

    assertThat(cache.getIfPresent("key1")).isNull();
    verifyNoInteractions(hook);
  }

  /** The consumer thread records the watermark and hands the sweep to the executor. */
  @Test
  void clearNowDefersTheSweepAndSkipsHooks() {
    cache.put("low", entry(3, KeyState.NORMAL));
    cache.put("high", entry(12, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    List<Runnable> pending = new java.util.ArrayList<>();
    context = new DefaultSyncContext(cache, k -> "refreshed", entryLifecycle, List.of(hook), null, codec, pending::add);

    context.clearNow(new Batch.Clear(List.of(msg("clear", SyncType.INVALIDATE_ALL.code(), 9L))));

    assertThat(cache.getIfPresent("low")).isNotNull();
    assertThat(cache.getIfPresent("high")).isNotNull();
    assertThat(pending).hasSize(1);

    pending.forEach(Runnable::run);

    assertThat(cache.getIfPresent("low")).isNull();
    assertThat(cache.getIfPresent("high")).isNotNull();
    verifyNoInteractions(hook);
  }

  private static CacheLoader<String, Object> decliningLoader() {
    return new CacheLoader<>() {
      @Override
      public Object load(String key) {
        return "rejected";
      }

      @Override
      public boolean isCacheable(Object candidate) {
        return false;
      }
    };
  }
}
