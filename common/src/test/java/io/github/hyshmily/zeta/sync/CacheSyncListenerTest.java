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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
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
import io.github.hyshmily.zeta.sync.local.Batch;
import io.github.hyshmily.zeta.sync.local.CacheSyncListener;
import io.github.hyshmily.zeta.sync.local.CacheSyncProperties;
import io.github.hyshmily.zeta.sync.local.DefaultSyncContext;
import io.github.hyshmily.zeta.sync.local.SyncContext;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.sync.local.SyncPolicy;
import io.github.hyshmily.zeta.sync.local.SyncType;
import io.github.hyshmily.zeta.util.NodeIdentity;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.clock.MonotonicStampClock;
import io.github.hyshmily.zeta.util.clock.StampClock;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * Tests for {@link CacheSyncListener} verifying correct ACK/NACK behavior for invalidate and
 * refresh sync messages, including error handling and null-type edge cases.
 */
class CacheSyncListenerTest {

  private Cache<String, CacheEntry> cache;
  private CacheSyncListener listener;
  private Channel channel;
  private ScheduledExecutorService scheduler;
  private EntryLifecycleImpl entryLifecycle;
  private SyncContext syncContext;

  @BeforeEach
  void setUp() throws IOException {
    cache = Caffeine.newBuilder().maximumSize(100).build();
    CacheSyncProperties properties = new CacheSyncProperties();
    properties.setJitterMs(0);
    scheduler = Executors.newSingleThreadScheduledExecutor();
    ZetaProperties ttlConfig = new ZetaProperties();
    entryLifecycle = new EntryLifecycleImpl(cache, ttlConfig, null);

    SyncPolicy policy = newPolicy(k -> "refreshed");
    listener = new CacheSyncListener(properties, scheduler, policy, syncContext, null, null);
    channel = mock(Channel.class);
  }

  private SyncPolicy newPolicy(CacheLoader loader) {
    io.github.hyshmily.zeta.sync.local.PutPayloadCodec parser = mock(
      io.github.hyshmily.zeta.sync.local.PutPayloadCodec.class
    );
    when(parser.decode(any())).thenReturn("carried");
    syncContext = new DefaultSyncContext(cache, loader, entryLifecycle, Collections.emptyList(), null, parser);
    return new SyncPolicy() {};
  }

  private void awaitWorkerTasks() throws InterruptedException {
    CountDownLatch phase1 = new CountDownLatch(1);
    CountDownLatch phase2 = new CountDownLatch(1);
    scheduler.execute(() -> phase1.countDown());
    assertThat(phase1.await(5, TimeUnit.SECONDS)).isTrue();
    scheduler.execute(() -> phase2.countDown());
    assertThat(phase2.await(5, TimeUnit.SECONDS)).isTrue();
  }

  /** Polls until the key is gone, for work the listener defers through a retry ladder. */
  private static void awaitRemoved(Cache<String, CacheEntry> cache, String key) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (cache.getIfPresent(key) != null && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
  }

  /**
   * Verifies that an INVALIDATE sync message removes the key from cache and acknowledges the message.
   */
  @Test
  void onMessage_invalidate_shouldAckAndRemove() throws IOException, InterruptedException {
    cache.put("key1", entry(1, 0));
    listener.onMessage(channel, syncMessage("key1", SyncType.INVALIDATE.code(), 2L));
    verify(channel).basicAck(anyLong(), eq(false));
    awaitWorkerTasks();
    assertThat(cache.getIfPresent("key1")).isNull();
  }

  /**
   * Verifies that a REFRESH sync message updates the cached entry with new data from the loader and acknowledges.
   */
  @Test
  void onMessage_refresh_shouldAckAndUpdate() throws IOException, InterruptedException {
    cache.put("key1", entry(1, 0));
    listener.onMessage(channel, syncMessage("key1", SyncType.REFRESH.code(), 2L));
    verify(channel).basicAck(anyLong(), eq(false));
    awaitWorkerTasks();
    assertThat(cache.getIfPresent("key1")).satisfies(o -> {
      CacheEntry ce = (CacheEntry) o;
      assertThat(ce.getValue()).isEqualTo("refreshed");
      assertThat(ce.getDataVersion()).isEqualTo(2L);
    });
  }

  /**
   * Verifies the batch-end amortization end to end (design decisions: buffering and ordering): a burst of same-key REFRESH
   * messages costs strictly fewer cluster-loader reads than messages, and the final L1 state carries
   * the newest version regardless of how the dispatcher sliced the grant boundaries.
   *
   * <p>The blocking loader pins the first load until the whole burst has been submitted, so
   * the drain can never complete between submissions — every slicing leaves at least one
   * non-final REFRESH to amortize. The final task of the last batch is always the newest
   * message, so the end state is deterministic even though the slicing is not.
   */
  @Test
  void onMessage_sameKeyRefreshBurst_shouldAmortizeLoadsToBatchTail() throws Exception {
    cache.put("key1", entry(1, 0));
    AtomicInteger loads = new AtomicInteger();
    CountDownLatch release = new CountDownLatch(1);
    CacheLoader blockingLoader = k -> {
      try {
        release.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      loads.incrementAndGet();
      return "refreshed";
    };
    CacheSyncProperties props = new CacheSyncProperties();
    props.setJitterMs(0);
    CacheSyncListener burstListener = new CacheSyncListener(
      props,
      scheduler,
      newPolicy(blockingLoader),
      syncContext,
      null,
      null
    );

    try {
      burstListener.onMessage(channel, syncMessage("key1", SyncType.REFRESH.code(), 2L));
      burstListener.onMessage(channel, syncMessage("key1", SyncType.REFRESH.code(), 3L));
      burstListener.onMessage(channel, syncMessage("key1", SyncType.REFRESH.code(), 4L));
      release.countDown();

      long deadline = System.currentTimeMillis() + 5000;
      boolean applied = false;
      while (System.currentTimeMillis() < deadline) {
        CacheEntry current = cache.getIfPresent("key1");
        if (loads.get() > 0 && current != null && current.getDataVersion() == 4L) {
          applied = true;
          break;
        }
        Thread.sleep(5);
      }
      assertThat(applied).as("final refresh (v4) must be applied").isTrue();
    } finally {
      burstListener.destroy();
    }

    // At least one load happened (the batch tail) but strictly fewer than messages —
    // the non-final REFRESHes of the granted batch(es) were amortized away.
    assertThat(loads.get()).isBetween(1, 2);
    assertThat(cache.getIfPresent("key1")).isInstanceOf(CacheEntry.class);
    assertThat(((CacheEntry) cache.getIfPresent("key1")).getDataVersion()).isEqualTo(4L);
  }

  /**
   * An ACK failure is not a processing failure: the message was handled, the
   * channel is broken. The exception propagates to the container (which owns
   * channel recovery) and no second operation — in particular no NACK — is
   * attempted on that channel.
   */
  @Test
  void onMessage_ackFailure_shouldPropagateWithoutNack() throws IOException {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, "INVALIDATE");
    Message msg = new Message("key1".getBytes(StandardCharsets.UTF_8), props);
    doThrow(new RuntimeException("forced error")).when(channel).basicAck(anyLong(), eq(false));
    assertThatThrownBy(() -> listener.onMessage(channel, msg))
      .isInstanceOf(RuntimeException.class)
      .hasMessageContaining("forced error");
    verify(channel, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
  }

  /**
   * Verifies that a sync message with a null type header is handled gracefully without error.
   */
  @Test
  void onMessage_shouldHandleNullType() throws IOException {
    MessageProperties props = new MessageProperties();
    Message msg = new Message("key1".getBytes(StandardCharsets.UTF_8), props);
    listener.onMessage(channel, msg);
    verify(channel).basicAck(anyLong(), eq(false));
  }

  /**
   * Verifies that an unknown sync message type is acknowledged and ignored.
   */
  @Test
  void onMessage_unknownType_shouldAckAndReturn() throws IOException {
    Message msg = syncMessage("key1", "UNKNOWN_TYPE", 1L);
    listener.onMessage(channel, msg);
    verify(channel).basicAck(anyLong(), eq(false));
  }

  /**
   * Verifies that an INVALIDATE with version 0 is skipped: it carries no
   * ordering authority (the in-tree senders always allocate a version before
   * the drop — design decisions: invalidation), so the version guard rejects it.
   */
  @Test
  void invalidate_withVersion0_shouldBeSkipped() throws IOException, InterruptedException {
    cache.put("key1", entry(5, 0));
    listener.onMessage(channel, syncMessage("key1", SyncType.INVALIDATE.code(), 0L));
    verify(channel).basicAck(anyLong(), eq(false));
    awaitWorkerTasks();
    assertThat(cache.getIfPresent("key1")).isNotNull();
  }

  /**
   * A zero version is unorderable even when a live stamp clock is wired: the listener
   * must drop it instead of clamping it to a fresh local stamp, which would let a
   * corrupt message outrank the resident entry.
   */
  @Test
  void invalidate_withVersion0AndLiveClock_shouldBeDropped() throws IOException, InterruptedException {
    CacheSyncProperties props = new CacheSyncProperties();
    props.setJitterMs(0);
    MonotonicStampClock clock = new MonotonicStampClock(11L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS);
    CacheSyncListener observing = new CacheSyncListener(
      props,
      scheduler,
      newPolicy(k -> "v"),
      syncContext,
      null,
      clock
    );
    try {
      cache.put("key1", entry(5, 0));
      observing.onMessage(channel, syncMessage("key1", SyncType.INVALIDATE.code(), 0L));
      verify(channel).basicAck(anyLong(), eq(false));
      awaitWorkerTasks();
      assertThat(cache.getIfPresent("key1")).isNotNull();
    } finally {
      observing.destroy();
    }
  }

  /**
   * A batch whose stamps are beyond the forward-drift bound is not dropped: every
   * entry applies at one fresh local stamp, because persisting the peer's future
   * version would blind later local writes to those keys.
   */
  @Test
  void invalidateBatch_beyondClockDrift_shouldApplyAtALocalStamp() throws IOException, InterruptedException {
    CacheSyncProperties props = new CacheSyncProperties();
    props.setJitterMs(0);
    MonotonicStampClock clock = new MonotonicStampClock(11L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS);
    CacheSyncListener observing = new CacheSyncListener(
      props,
      scheduler,
      newPolicy(k -> "v"),
      syncContext,
      null,
      clock
    );
    try {
      cache.put("k1", entry(1, 0));
      cache.put("k2", entry(1, 0));
      long future = (System.currentTimeMillis() - StampClock.EPOCH + 120_000L) << StampClock.TIMESTAMP_SHIFT;
      byte[] body = SyncMessage.encodeBatchBody(
        List.of(new SyncMessage.Entry("k1", future), new SyncMessage.Entry("k2", future + 1L))
      );
      MessageProperties messageProperties = new MessageProperties();
      messageProperties.setHeader(HEADER_TYPE, SyncType.INVALIDATE_BATCH.code());

      observing.onMessage(channel, new Message(body, messageProperties));

      verify(channel).basicAck(anyLong(), eq(false));
      awaitRemoved(cache, "k1");
      awaitRemoved(cache, "k2");
      assertThat(cache.getIfPresent("k1")).isNull();
      assertThat(cache.getIfPresent("k2")).isNull();
    } finally {
      observing.destroy();
    }
  }

  /**
   * A REFRESH dropped by a saturated dispatcher must not leave the stale entry
   * behind: the synchronous invalidate fallback removes it on the consumer
   * thread so the next read reloads instead of serving v1 for its full TTL.
   */
  @Test
  void onMessage_dispatcherDrop_refresh_shouldFallbackInvalidate() throws IOException {
    CacheSyncProperties saturated = new CacheSyncProperties();
    saturated.setJitterMs(0);
    saturated.setMaxPendingUnits(0);
    CacheSyncListener dropListener = new CacheSyncListener(
      saturated,
      scheduler,
      newPolicy(k -> "refreshed"),
      syncContext,
      null,
      null
    );
    try {
      cache.put("key1", entry(1, 0));
      dropListener.onMessage(channel, syncMessage("key1", SyncType.REFRESH.code(), 2L));
      verify(channel).basicAck(anyLong(), eq(false));
      assertThat(cache.getIfPresent("key1")).isNull();
    } finally {
      dropListener.destroy();
    }
  }

  /**
   * Same fallback for INVALIDATE: a saturated dispatcher still removes the key
   * synchronously instead of leaving v1 resident.
   */
  @Test
  void onMessage_dispatcherDrop_invalidate_shouldFallbackInvalidate() throws IOException {
    CacheSyncProperties saturated = new CacheSyncProperties();
    saturated.setJitterMs(0);
    saturated.setMaxPendingUnits(0);
    CacheSyncListener dropListener = new CacheSyncListener(
      saturated,
      scheduler,
      newPolicy(k -> "refreshed"),
      syncContext,
      null,
      null
    );
    try {
      cache.put("key1", entry(1, 0));
      dropListener.onMessage(channel, syncMessage("key1", SyncType.INVALIDATE.code(), 2L));
      verify(channel).basicAck(anyLong(), eq(false));
      assertThat(cache.getIfPresent("key1")).isNull();
    } finally {
      dropListener.destroy();
    }
  }

  /**
   * design decisions: version stamps decision 7: every inbound stamp is merged into the local clock on
   * the consumer thread, even when the message itself is later rejected. A
   * lagging node otherwise keeps allocating stamps below every peer's floor and
   * its writes are silently skipped forever.
   */
  @Test
  void onMessage_observesPeerStampBeforeDispatch() throws IOException {
    CacheSyncProperties props = new CacheSyncProperties();
    props.setJitterMs(0);
    MonotonicStampClock clock = new MonotonicStampClock(11L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS);
    CacheSyncListener observing = new CacheSyncListener(
      props,
      scheduler,
      newPolicy(k -> "v"),
      syncContext,
      null,
      clock
    );
    try {
      long peerTs = TimeSource.currentTimeMillis() - StampClock.EPOCH + 60_000L;
      long peerStamp = (peerTs << 22) | (7L << 12);
      observing.onMessage(channel, syncMessage("key1", SyncType.INVALIDATE.code(), peerStamp));

      assertThat(clock.nextStamp()).isGreaterThan(peerStamp);
    } finally {
      observing.destroy();
    }
  }

  /**
   * A clear dropped by a saturated dispatcher must still sweep the cache — but not on the
   * consumer thread. The refusal re-enters the clear channel and sweeps inline only when the
   * retry ladder is exhausted, so the ack path never pays an O(table) scan.
   *
   * <p>The generic fallback rewrites messages to a single-key INVALIDATE; doing that to a
   * clear would delete the literal marker key (which never exists) and leave every entry
   * resident for its full TTL.
   */
  @Test
  void onMessage_dispatcherDrop_invalidateAll_shouldSweepCache() throws Exception {
    CacheSyncProperties saturated = new CacheSyncProperties();
    saturated.setJitterMs(0);
    saturated.setMaxPendingUnits(0);
    CacheSyncListener dropListener = new CacheSyncListener(
      saturated,
      scheduler,
      newPolicy(k -> "v"),
      syncContext,
      null,
      null
    );
    try {
      cache.put("a", entry(1, 1));
      cache.put("b", entry(2, 2));
      cache.put("fresh", entry(9, 3));

      dropListener.onMessage(channel, syncMessage(SyncMessage.CLEAR_ALL_BODY, SyncType.INVALIDATE_ALL.code(), 5L));

      verify(channel).basicAck(anyLong(), eq(false));
      assertThat(cache.getIfPresent("a")).as("the refusal must not sweep the table on the consumer thread").isNotNull();

      // The retry ladder ends in the inline sweep: the clear is delayed, never lost.
      awaitRemoved(cache, "a");
      assertThat(cache.getIfPresent("b")).isNull();
      assertThat(cache.getIfPresent("fresh")).isNotNull();
    } finally {
      dropListener.destroy();
    }
  }

  /**
   * A clear batch that mixes INVALIDATE_ALL and INVALIDATE_PREFIX must reach one
   * {@code clear} call: the context applies both kinds in the same sweep. Routing the mixed
   * batch through a per-kind split would leave every non-prefix key resident.
   */
  @Test
  void onMessage_mixedClearBatch_reachesClearAndAppliesBoth() throws Exception {
    CacheSyncProperties props = new CacheSyncProperties();
    props.setJitterMs(0);
    CountDownLatch swept = new CountDownLatch(1);
    SyncPolicy routing = new SyncPolicy() {
      @Override
      public void clear(Batch.Clear batch, SyncContext context) {
        SyncPolicy.super.clear(batch, context);
        swept.countDown();
      }
    };
    CacheSyncListener mixed = new CacheSyncListener(props, scheduler, routing, syncContext, null, null);
    try {
      cache.put("low", entry(3, 0));
      cache.put("users\u0000a", entry(7, 0));
      cache.put("fresh", entry(12, 0));

      // Hold the single scheduler thread so both clears land in one granted batch.
      CountDownLatch release = new CountDownLatch(1);
      scheduler.execute(() -> {
        try {
          release.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      mixed.onMessage(channel, syncMessage(SyncMessage.CLEAR_ALL_BODY, SyncType.INVALIDATE_ALL.code(), 5L));
      mixed.onMessage(channel, syncMessage("users\u0000", SyncType.INVALIDATE_PREFIX.code(), 9L));
      release.countDown();

      assertThat(swept.await(5, TimeUnit.SECONDS)).isTrue();
      // The table-wide clear (v5) removes "low"; the higher prefix clear (v9) removes
      // the users-prefixed key; "fresh" (v12) outranks both.
      assertThat(cache.getIfPresent("low")).isNull();
      assertThat(cache.getIfPresent("users\u0000a")).isNull();
      assertThat(cache.getIfPresent("fresh")).isNotNull();
    } finally {
      mixed.destroy();
    }
  }

  private static Message syncMessage(String key, String type, long version) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, type);
    props.setHeader(HEADER_VERSION, version);
    return new Message(key.getBytes(StandardCharsets.UTF_8), props);
  }

  /** Builds a PUT sync message: typed value body, real key in the cacheKey header. */
  private static Message putSyncMessage(String key, String payload, long version) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, SyncType.PUT.code());
    props.setHeader(HEADER_VERSION, version);
    props.setHeader(HEADER_CACHE_KEY, key);
    return new Message(payload.getBytes(StandardCharsets.UTF_8), props);
  }

  /**
   * Verifies end-to-end PUT handling: the payload body is applied to the L1
   * entry keyed by the cacheKey header (per-key ordering preserved), with no
   * loader involvement — the carried value IS the new value.
   */
  @Test
  void onMessage_withPut_appliesCarriedValue() throws IOException, InterruptedException {
    cache.put("key1", entry(1, 0));
    listener.onMessage(channel, putSyncMessage("key1", "[\"java.lang.String\",\"carried\"]", 2L));
    verify(channel).basicAck(anyLong(), eq(false));
    awaitWorkerTasks();
    CacheEntry applied = cache.getIfPresent("key1");
    assertThat(applied).isNotNull();
    assertThat(applied.getValue()).isEqualTo("carried");
    assertThat(applied.getDataVersion()).isEqualTo(2L);
  }

  /**
   * Verifies that a PUT without the cacheKey header is dropped (no dispatch key,
   * no per-key ordering guarantee) but still acknowledged.
   */
  @Test
  void onMessage_withPutMissingKeyHeader_shouldAckWithoutApply() throws IOException {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, SyncType.PUT.code());
    props.setHeader(HEADER_VERSION, 2L);
    Message msg = new Message("{\"key\":\"key1\",\"value\":\"carried\"}".getBytes(StandardCharsets.UTF_8), props);
    listener.onMessage(channel, msg);
    verify(channel).basicAck(anyLong(), eq(false));
    assertThat(cache.getIfPresent("key1")).isNull();
  }

  /**
   * Verifies that an empty body message is acknowledged without cache interaction.
   */
  @Test
  void onMessage_withEmptyBody_shouldAckAndReturn() throws IOException {
    MessageProperties props = new MessageProperties();
    Message msg = new Message(new byte[0], props);
    listener.onMessage(channel, msg);
    verify(channel).basicAck(anyLong(), eq(false));
  }

  /**
   * Verifies that a refresh whose cluster-loader read fails is acknowledged gracefully.
   */
  @Test
  void onMessage_withRefreshAndLoaderException_shouldAck() throws IOException {
    CacheSyncProperties props = new CacheSyncProperties();
    props.setJitterMs(0);
    SyncPolicy h = newPolicy(k -> {
      throw new RuntimeException("source down");
    });
    CacheSyncListener failingListener = new CacheSyncListener(props, scheduler, h, syncContext, null, null);

    cache.put("key1", entry(1, 0));
    failingListener.onMessage(channel, syncMessage("key1", SyncType.REFRESH.code(), 2L));
    verify(channel).basicAck(anyLong(), eq(false));
  }

  /**
   * Verifies that a refresh on a missing cache key creates a new CacheEntry from the cluster loader.
   */
  @Test
  void onMessage_withRefreshOnMissingKey_shouldCreateEntry() throws IOException, InterruptedException {
    listener.onMessage(channel, syncMessage("nonexistent", SyncType.REFRESH.code(), 2L));
    verify(channel).basicAck(anyLong(), eq(false));
    awaitWorkerTasks();
    assertThat(cache.getIfPresent("nonexistent")).isNotNull().isInstanceOf(CacheEntry.class);
    assertThat(((CacheEntry) cache.getIfPresent("nonexistent")).getValue()).isEqualTo("refreshed");
  }

  /**
   * Verifies the REFRESH value-channel fallback (C2/design decisions: sync messages): when the
   * loader returns null (nothing writes the cache-key namespace in the data source by
   * default), the entry is invalidated locally so the next read reloads
   * through the type-safe application reader — the stale copy is never kept.
   */
  @Test
  void onMessage_withRefreshOnStringValueAndNullLoaderReturn_shouldInvalidate()
    throws IOException, InterruptedException {
    CacheSyncProperties props = new CacheSyncProperties();
    props.setJitterMs(0);
    SyncPolicy h = newPolicy(k -> null);
    CacheSyncListener nullListener = new CacheSyncListener(props, scheduler, h, syncContext, null, null);

    cache.put("key1", entry(5, 0));
    nullListener.onMessage(channel, syncMessage("key1", SyncType.REFRESH.code(), 6L));
    verify(channel).basicAck(anyLong(), eq(false));
    awaitWorkerTasks();
    assertThat(cache.getIfPresent("key1")).isNull();
  }

  /**
   * Verifies that a stale INVALIDATE is skipped.
   */
  @Test
  void onMessage_withStaleInvalidateIncoming_shouldSkip() throws IOException, InterruptedException {
    cache.put("key1", entry(5, 0));
    listener.onMessage(channel, syncMessage("key1", SyncType.INVALIDATE.code(), 3L));
    verify(channel).basicAck(anyLong(), eq(false));
    awaitWorkerTasks();
    assertThat(cache.getIfPresent("key1")).isNotNull();
  }

  /**
   * design decisions: dedup lifecycle: the sender's own REFRESH broadcast is dropped before parsing —
   * the L1 entry the sender just wrote at the broadcast version must survive
   * the fanout self-delivery.
   */
  @Test
  void onMessage_ownRefresh_shouldDropAndKeepOwnEntry() throws IOException, InterruptedException {
    NodeIdentity.setInstanceOverride("self-instance");
    try {
      cache.put("key1", entry(4, 0));
      listener.onMessage(channel, syncMessageWithOrigin("key1", SyncType.REFRESH.code(), 4L, "self-instance"));
      verify(channel).basicAck(anyLong(), eq(false));
      awaitWorkerTasks();
      // Untouched: the value is still the sender's own putThrough payload, not
      // the loader's — the message never reached the sync context.
      assertThat(((CacheEntry) cache.getIfPresent("key1")).getValue()).isEqualTo("v");
    } finally {
      NodeIdentity.setInstanceOverride(null);
    }
  }

  /**
   * A peer's REFRESH (different origin instance) is processed normally — the
   * self-drop must not leak onto cross-instance traffic.
   */
  @Test
  void onMessage_peerRefresh_shouldProcess() throws IOException, InterruptedException {
    cache.put("key1", entry(4, 0));
    listener.onMessage(channel, syncMessageWithOrigin("key1", SyncType.REFRESH.code(), 6L, "peer-instance"));
    verify(channel).basicAck(anyLong(), eq(false));
    awaitWorkerTasks();
    assertThat(((CacheEntry) cache.getIfPresent("key1")).getValue()).isEqualTo("refreshed");
  }

  /**
   * A self-addressed INVALIDATE is still processed (only REFRESH is dropped):
   * it heals invalidate-vs-reload repopulations stamped with older versions.
   */
  @Test
  void onMessage_ownInvalidate_shouldStillProcess() throws IOException, InterruptedException {
    NodeIdentity.setInstanceOverride("self-instance");
    try {
      cache.put("key1", entry(2, 0));
      listener.onMessage(channel, syncMessageWithOrigin("key1", SyncType.INVALIDATE.code(), 5L, "self-instance"));
      verify(channel).basicAck(anyLong(), eq(false));
      awaitWorkerTasks();
      assertThat(cache.getIfPresent("key1")).isNull();
    } finally {
      NodeIdentity.setInstanceOverride(null);
    }
  }

  /**
   * A REFRESH without an origin header (pre-0067 sender, rolling upgrade) is
   * processed — absent origin never drops.
   */
  @Test
  void onMessage_refreshWithoutOriginHeader_shouldProcess() throws IOException, InterruptedException {
    cache.put("key1", entry(4, 0));
    listener.onMessage(channel, syncMessage("key1", SyncType.REFRESH.code(), 6L));
    verify(channel).basicAck(anyLong(), eq(false));
    awaitWorkerTasks();
    assertThat(((CacheEntry) cache.getIfPresent("key1")).getValue()).isEqualTo("refreshed");
  }

  private static Message syncMessageWithOrigin(String key, String type, long version, String origin) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, type);
    props.setHeader(HEADER_VERSION, version);
    props.setHeader(HEADER_ORIGIN_INSTANCE, origin);
    return new Message(key.getBytes(StandardCharsets.UTF_8), props);
  }

  private static CacheEntry entry(long dataVersion, long decisionVersion) {
    return EntryDraft.blank(null)
      .value("v")
      .version(dataVersion)
      .decision(DecisionStamp.hot(decisionVersion, null, 0L))
      .ttl(300_000, 30_000, 300_000, 30_000)
      .expiryAt(Long.MAX_VALUE, 30_000)
      .keyState(KeyState.NORMAL)
      .build();
  }

  // ── Application isolation (design decisions: sync messages pattern on the sync plane) ──

  /**
   * Builds a sync message that declares the sender's application name, the way
   * {@code CacheSyncPublisher} now stamps every sync message. The sync exchange is a
   * fanout with a global name, so on a shared broker these headers are the only thing
   * telling one application's traffic apart from another's.
   */
  private static Message syncMessageFromApp(String key, String type, long version, String appName) {
    Message msg = syncMessage(key, type, version);
    msg.getMessageProperties().setHeader(HEADER_APP_NAME, appName);
    return msg;
  }

  private CacheSyncListener isolatedListener(String appName) {
    // Zero start-jitter, matching setUp: with the default 50 ms park the dispatcher
    // would still be sleeping when awaitWorkerTasks() returns, so the assertion would
    // race the task rather than observe its outcome.
    CacheSyncProperties props = new CacheSyncProperties();
    props.setJitterMs(0);
    CacheSyncListener isolated = new CacheSyncListener(
      props,
      scheduler,
      newPolicy(k -> "v"),
      syncContext,
      appName,
      null
    );
    return isolated;
  }

  @Test
  void onMessage_fromForeignApp_shouldAckAndLeaveCacheUntouched() throws IOException {
    cache.put("key1", entry(5, 0));
    CacheSyncListener isolated = isolatedListener("appA");

    isolated.onMessage(channel, syncMessageFromApp("key1", SyncType.INVALIDATE.code(), 1L, "appB"));

    // A valid message that simply is not ours: ack it, do not apply it.
    verify(channel).basicAck(anyLong(), eq(false));
    assertThat(cache.getIfPresent("key1")).isNotNull();
  }

  @Test
  void onMessage_fromSameApp_shouldApply() throws IOException, InterruptedException {
    cache.put("key1", entry(5, 0));
    CacheSyncListener isolated = isolatedListener("appA");

    isolated.onMessage(channel, syncMessageFromApp("key1", SyncType.INVALIDATE.code(), 6L, "appA"));

    awaitWorkerTasks();
    assertThat(cache.getIfPresent("key1")).isNull();
  }

  @Test
  void onMessage_withoutAppNameHeader_shouldBeProcessedForRollingUpgrade() throws IOException, InterruptedException {
    cache.put("key1", entry(5, 0));
    CacheSyncListener isolated = isolatedListener("appA");

    // A pre-0068 sender declares no appName; dropping it would break rolling upgrades.
    isolated.onMessage(channel, syncMessage("key1", SyncType.INVALIDATE.code(), 6L));

    awaitWorkerTasks();
    assertThat(cache.getIfPresent("key1")).isNull();
  }

  @Test
  void onMessage_whenLocalAppNameIsBlank_shouldProcessEveryApp() throws IOException, InterruptedException {
    cache.put("key1", entry(5, 0));
    CacheSyncListener unfiltered = isolatedListener(null);

    unfiltered.onMessage(channel, syncMessageFromApp("key1", SyncType.INVALIDATE.code(), 6L, "appB"));

    awaitWorkerTasks();
    assertThat(cache.getIfPresent("key1")).isNull();
  }

  /**
   * A foreign-app drop is counted (not just DEBUG-logged): a misconfigured
   * app-name drops 100% of sync while the process looks healthy, so the
   * mismatch needs a counter plus last-sender signal for the actuator
   * endpoint. Same-app and headerless messages never add.
   */
  @Test
  void foreignAppDrops_shouldCountOnlyMismatches() throws IOException, InterruptedException {
    cache.put("key1", entry(5, 0));
    CacheSyncListener isolated = isolatedListener("appA");

    assertThat(isolated.foreignAppDrops()).isZero();
    assertThat(isolated.lastForeignApp()).isNull();

    isolated.onMessage(channel, syncMessageFromApp("key1", SyncType.INVALIDATE.code(), 1L, "appB"));
    isolated.onMessage(channel, syncMessageFromApp("key2", SyncType.REFRESH.code(), 1L, "appB"));
    isolated.onMessage(channel, syncMessageFromApp("key1", SyncType.INVALIDATE.code(), 6L, "appA"));
    awaitWorkerTasks();

    assertThat(isolated.foreignAppDrops()).isEqualTo(2);
    assertThat(isolated.lastForeignApp()).isEqualTo("appB");
  }
}
