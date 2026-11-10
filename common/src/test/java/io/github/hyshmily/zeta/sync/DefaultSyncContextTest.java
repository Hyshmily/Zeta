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
import io.github.hyshmily.zeta.sync.local.SyncHook;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.sync.local.SyncPolicy;
import io.github.hyshmily.zeta.sync.local.SyncType;
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
    return new DefaultSyncContext(cache, loader, entryLifecycle, hooks, null, codec);
  }

  private final SyncPolicy policy = new SyncPolicy() {};

  private void handleBatch(List<SyncMessage> messages) {
    Batch batch = Batch.reduce(messages);
    if (batch instanceof Batch.Value value) {
      for (SyncMessage covered : value.covered()) {
        context.skip(covered);
      }
      if (value.winner().type() == SyncType.INVALIDATE) {
        policy.invalidate(value, context);
      } else {
        policy.value(value, context);
      }
      return;
    }
    policy.clear((Batch.Clear) batch, context);
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
    verify(hook).afterRefresh(any(), any());
  }

  @Test
  void refresh_staleThanResident_skipsAndReports() {
    cache.put("key1", entry(5, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    context = newContext(k -> "refreshed", List.of(hook));

    handleBatch(List.of(msg("key1", SyncType.REFRESH.code(), 3L)));

    assertThat(cache.getIfPresent("key1").getValue()).isEqualTo("v");
    verify(hook).onRefreshSkipped(any());
  }

  @Test
  void refresh_withoutValue_invalidatesLocally() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    context = newContext(k -> null, List.of(hook));

    handleBatch(List.of(msg("key1", SyncType.REFRESH.code(), 2L)));

    assertThat(cache.getIfPresent("key1")).isNull();
    verify(hook).onRefreshSkipped(any());
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

  @Test
  void refresh_declinedByLoaderAdmission_invalidatesLocally() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    context = newContext(decliningLoader(), List.of(hook));

    handleBatch(List.of(msg("key1", SyncType.REFRESH.code(), 2L)));

    assertThat(cache.getIfPresent("key1")).isNull();
    verify(hook).onRefreshSkipped(any());
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
   * one terminal hook: {@code onRefreshSkipped}, never {@code afterInvalidate}.
   */
  @Test
  void put_unreadablePayload_firesOnlyTheSkippedHook() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    PutPayloadCodec unreadable = mock(PutPayloadCodec.class);
    when(unreadable.decode(any())).thenReturn(null);
    context = new DefaultSyncContext(cache, k -> "refreshed", entryLifecycle, List.of(hook), null, unreadable);

    handleBatch(List.of(put("key1", "carried", 2L)));

    assertThat(cache.getIfPresent("key1")).isNull();
    verify(hook).onRefreshSkipped(any());
    verify(hook, never()).afterInvalidate(any());
    verify(hook, never()).afterRefresh(any(), any());
  }

  /**
   * A REFRESH whose load yields nothing removes the resident through the internal
   * fallback, but the fallback is not a peer INVALIDATE: the message reports one terminal
   * {@code onRefreshSkipped} and no {@code afterInvalidate}.
   */
  @Test
  void refresh_emptyLoad_withResident_firesOnlyTheSkippedHook() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    context = newContext(k -> null, List.of(hook));

    handleBatch(List.of(msg("key1", SyncType.REFRESH.code(), 2L)));

    assertThat(cache.getIfPresent("key1")).isNull();
    verify(hook).onRefreshSkipped(any());
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
   * At the same version the INVALIDATE outranks the REFRESH, matching the codec's
   * {@code BroadcastClaim} rule; arrival order inside the batch must not flip the winner.
   */
  @Test
  void sameVersionInvalidateOutranksRefreshInEitherOrder() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    context = newContext(k -> "refreshed", List.of(hook));

    handleBatch(List.of(msg("key1", SyncType.INVALIDATE.code(), 5L), msg("key1", SyncType.REFRESH.code(), 5L)));

    assertThat(cache.getIfPresent("key1")).isNull();
    verify(hook).afterInvalidate(any());
    verify(hook, never()).afterRefresh(any(), any());
  }

  /**
   * A covered INVALIDATE whose value fallback evicts still owns an invalidation outcome: the
   * removal must fire {@code afterInvalidate}, not only {@code onRefreshSkipped}.
   */
  @Test
  void coveredInvalidateFiresAfterAFallbackEviction() {
    cache.put("key1", entry(1, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    context = newContext(k -> null, List.of(hook));

    handleBatch(List.of(msg("key1", SyncType.INVALIDATE.code(), 2L), msg("key1", SyncType.REFRESH.code(), 3L)));

    assertThat(cache.getIfPresent("key1")).isNull();
    verify(hook).afterInvalidate(any());
    verify(hook).onRefreshSkipped(any());
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
