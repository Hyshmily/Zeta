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
package io.github.hyshmily.zeta.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.AccessCounter;
import io.github.hyshmily.zeta.cache.BroadcastBuffer;
import io.github.hyshmily.zeta.cache.CentralDispatcher;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycleImpl;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.policy.InvalidatePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.scheduler.DefaultBackgroundRefresher;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.local.DefaultSyncDecisionHandler;
import io.github.hyshmily.zeta.util.clock.StampClock;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * End-to-end semantics of the design decisions: invalidation, cache table invalidation plane without a
 * broker: the sender's full sweep and single broadcast, and the receiver's
 * guarded sweep + clear watermark (older PUT/REFRESH cannot re-warm).
 */
class ClearAllInvalidationTest {

  private ScheduledExecutorService scheduler;
  private Cache<String, CacheEntry> l1;
  private StampClock clock;
  private AtomicLong stamps;
  private HotKeyCache cache;
  private SingleFlight singleFlight;
  private List<byte[]> sentBodies;
  private DefaultSyncDecisionHandler receiver;
  private Cache<String, CacheEntry> peerL1;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    scheduler = Executors.newSingleThreadScheduledExecutor();
    l1 = Caffeine.newBuilder().maximumSize(1_000).build();
    stamps = new AtomicLong();
    clock = mock(StampClock.class);
    when(clock.nextStamp()).thenAnswer(inv -> stamps.incrementAndGet());
    when(clock.nextStamps(org.mockito.ArgumentMatchers.anyInt())).thenAnswer(inv -> {
      int n = inv.getArgument(0);
      return stamps.addAndGet(n) - n + 1;
    });

    HotKeyDetector detector = mock(HotKeyDetector.class);
    when(detector.contains(anyString())).thenReturn(false);
    ZetaProperties props = new ZetaProperties();
    var lifecycle = new EntryLifecycleImpl(l1, props, mock(HealthView.class));
    var refresher = new DefaultBackgroundRefresher(l1, Runnable::run, lifecycle, 10);
    var accessCounter = new AccessCounter(detector, null, scheduler, 50L, 500L);
    var buffer = new BroadcastBuffer(scheduler, null, 5_000, 10_000, null);

    // Capture every clear/batch broadcast body the dispatcher hands to the
    // (stubbed) publisher — the shape the broker would carry.
    sentBodies = new java.util.ArrayList<>();
    var publisher = mock(io.github.hyshmily.zeta.sync.local.CacheSyncPublisher.class);
    org.mockito.Mockito.doAnswer(inv -> {
      sentBodies.add(io.github.hyshmily.zeta.sync.local.SyncMessage.CLEAR_ALL_BODY.getBytes());
      return null;
    })
      .when(publisher)
      .broadcastInvalidateAll(anyLong());

    singleFlight = mock(SingleFlight.class);
    cache = new HotKeyCache(
      detector,
      l1,
      singleFlight,
      lifecycle,
      refresher,
      new CentralDispatcher(accessCounter, publisher, buffer, Runnable::run),
      clock,
      mock(HealthView.class)
    );

    // The receiving peer: its own L1 + the default decision handler.
    peerL1 = Caffeine.newBuilder().maximumSize(1_000).build();
    when(publisher.parsePutPayload(any())).thenReturn("fresh");
    receiver = new DefaultSyncDecisionHandler(
      peerL1,
      null,
      new EntryLifecycleImpl(peerL1, props, mock(HealthView.class)),
      List.of(),
      null,
      publisher
    );
  }

  @AfterEach
  void tearDown() {
    scheduler.shutdownNow();
  }

  private static CacheEntry entry(String value, long version) {
    return EntryDraft.blank(null)
      .value(value)
      .version(version)
      .decision(DecisionStamp.hot(0, null, 0L))
      .ttl(300_000, 30_000, 300_000, 30_000)
      .expiryAt(Long.MAX_VALUE, Long.MAX_VALUE)
      .keyState(KeyState.NORMAL)
      .build();
  }

  @Test
  @DisplayName("clear sweeps the table, keeps newer writes, broadcasts one clear message")
  void clear_sweepsAndBroadcastsOnce() {
    // Prime the mock clock so the clearStamp (101) outranks the pre-seeded
    // entries below, while the "fresh" entry at 999 still survives.
    stamps.set(100);
    l1.put("a", entry("old-a", 1L));
    l1.put("b", entry("old-b", 2L));
    // A strictly newer concurrent write must survive the sweep.
    l1.put("fresh", entry("fresh", 999L));

    long clearStamp = cache.clear(new InvalidatePolicy(InvalidatePolicy.Scope.BOTH, true));

    assertThat(clearStamp).isPositive();
    assertThat(l1.getIfPresent("a")).isNull();
    assertThat(l1.getIfPresent("b")).isNull();
    assertThat(l1.getIfPresent("fresh")).isNotNull();
    // Exactly one clear message — not one per key.
    assertThat(sentBodies).hasSize(1);
  }

  @Test
  @DisplayName("local-only clear sweeps at its stamp, keeps newer writes, sends nothing")
  void localClear_sweepsWithoutBroadcast() {
    stamps.set(100);
    l1.put("a", entry("v", 1L));
    l1.put("b", entry("v", 2L));
    // The local sweep is version-guarded exactly like the sender's synced form:
    // a write allocated after the clear stamp survives instead of diverging
    // from peers that keep it.
    l1.put("fresh", entry("fresh", 999L));

    long stamp = cache.clear(new InvalidatePolicy(InvalidatePolicy.Scope.LOCAL, true));

    assertThat(stamp).isEqualTo(-1L);
    assertThat(l1.getIfPresent("a")).isNull();
    assertThat(l1.getIfPresent("b")).isNull();
    assertThat(l1.getIfPresent("fresh")).isNotNull();
    assertThat(sentBodies).isEmpty();
  }

  @Test
  @DisplayName("scoped clear kills dedup flights before sweeping the table")
  void scopedClear_killsFlightsBeforeSweep() {
    stamps.set(100);
    l1.put("ns:a", entry("v", 1L));
    java.util.concurrent.atomic.AtomicBoolean residentAtKill = new java.util.concurrent.atomic.AtomicBoolean();
    org.mockito.Mockito.doAnswer(inv -> {
      residentAtKill.set(l1.getIfPresent("ns:a") != null);
      return null;
    })
      .when(singleFlight)
      .invalidatePrefix("ns:");

    cache.clearPrefix("ns:", new InvalidatePolicy(InvalidatePolicy.Scope.LOCAL, true));

    assertThat(residentAtKill).as("the flight is killed while the entry is still resident").isTrue();
    assertThat(l1.getIfPresent("ns:a")).isNull();
  }

  @Test
  @DisplayName("receiver sweep removes entries at or below the stamp and keeps strictly newer ones")
  void receiverSweep_appliesGuardedSweep() {
    peerL1.put("a", entry("old", 5L));
    peerL1.put("fresh", entry("fresh", 11L));

    receiver.handle(
      new io.github.hyshmily.zeta.sync.local.SyncMessage(
        0L,
        io.github.hyshmily.zeta.sync.local.SyncMessage.CLEAR_ALL_BODY,
        io.github.hyshmily.zeta.sync.local.SyncType.INVALIDATE_ALL,
        10L
      )
    );

    assertThat(peerL1.getIfPresent("a")).isNull();
    assertThat(peerL1.getIfPresent("fresh")).isNotNull();
  }

  @Test
  @DisplayName("a pre-clear PUT arriving after the sweep is suppressed by the watermark")
  void receiverWatermark_suppressesStalePutReWarm() {
    // Receiver holds a pre-clear entry; the clear arrives and removes it.
    peerL1.put("a", entry("stale", 5L));
    receiver.handle(
      new io.github.hyshmily.zeta.sync.local.SyncMessage(
        0L,
        io.github.hyshmily.zeta.sync.local.SyncMessage.CLEAR_ALL_BODY,
        io.github.hyshmily.zeta.sync.local.SyncType.INVALIDATE_ALL,
        10L
      )
    );
    assertThat(peerL1.getIfPresent("a")).isNull();

    // A PUT broadcast stamped before the clear arrives late — the per-key
    // guard passes on the now-absent key, the clear watermark must stop it.
    var stalePut = new io.github.hyshmily.zeta.sync.local.SyncMessage(
      2L,
      "a",
      io.github.hyshmily.zeta.sync.local.SyncType.PUT,
      5L,
      "[\"java.lang.String\",\"stale\"]".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    );
    receiver.handle(stalePut);

    assertThat(peerL1.getIfPresent("a")).isNull();
  }

  @Test
  @DisplayName("a post-clear PUT still applies (newer than the stamp)")
  void receiverWatermark_allowsNewerPut() {
    receiver.handle(
      new io.github.hyshmily.zeta.sync.local.SyncMessage(
        0L,
        io.github.hyshmily.zeta.sync.local.SyncMessage.CLEAR_ALL_BODY,
        io.github.hyshmily.zeta.sync.local.SyncType.INVALIDATE_ALL,
        10L
      )
    );

    var freshPut = new io.github.hyshmily.zeta.sync.local.SyncMessage(
      3L,
      "b",
      io.github.hyshmily.zeta.sync.local.SyncType.PUT,
      11L,
      "[\"java.lang.String\",\"fresh\"]".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    );
    receiver.handle(freshPut);

    assertThat(peerL1.getIfPresent("b")).isNotNull();
  }
}
