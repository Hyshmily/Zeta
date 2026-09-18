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
package io.github.hyshmily.zeta.cache.cachesupport;

import static org.mockito.Mockito.*;

import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Deterministic tests for {@link BroadcastBuffer#record}'s version-ordered merge
 * (ADR-0066): concurrent writers record out of INCR order (the version bump and the
 * record are separated by the L1 apply), so within a space the buffer must keep the
 * newest allocation instead of the last record. Before the fix, an out-of-order
 * {@code record(key, 5)} after {@code record(key, 6)} regressed the pending version
 * and the v6 REFRESH was never sent — peers pinned on stale data until the next
 * write or TTL. Across the degraded boundary the newest record wins (legacy
 * last-writer-wins; the receiver-side matrix decides what applies).
 *
 * <p>Kept out of {@code BroadcastBufferTest} so the deterministic merge coverage is
 * not excluded with that class's {@code flaky} timing tests.
 */
class BroadcastBufferMergeTest {

  private ScheduledExecutorService scheduler;
  private CacheSyncPublisher publisher;

  @BeforeEach
  void setUp() {
    scheduler = Executors.newSingleThreadScheduledExecutor();
    publisher = mock(CacheSyncPublisher.class);
  }

  @AfterEach
  void tearDown() {
    scheduler.shutdownNow();
  }

  private BroadcastBuffer buffer() {
    return new BroadcastBuffer(scheduler, Optional.of(publisher), 5000L);
  }

  /**
   * The P1 regression: record(v6) followed by an out-of-order record(v5) must keep
   * v6 pending — the flush must send the newest write's REFRESH, not the regressed
   * one a peer would skip.
   */
  @Test
  void record_outOfOrderRecords_shouldKeepNewestVersion() {
    BroadcastBuffer buf = buffer();
    buf.record("key", 6L, false);
    buf.record("key", 5L, false);
    buf.flush();

    verify(publisher).broadcastRefresh("key", 6L, false);
    verifyNoMoreInteractions(publisher);
  }

  /** Ascending records keep collapsing onto the newest — the common sequential case. */
  @Test
  void record_ascendingRecords_shouldKeepNewestVersion() {
    BroadcastBuffer buf = buffer();
    buf.record("key", 5L, false);
    buf.record("key", 6L, false);
    buf.record("key", 7L, false);
    buf.flush();

    verify(publisher).broadcastRefresh("key", 7L, false);
    verifyNoMoreInteractions(publisher);
  }

  /**
   * Across the degraded boundary the newest RECORD wins (legacy last-writer-wins):
   * a normal write recorded after a degraded one replaces it, and the receiver-side
   * matrix handles whatever the peers hold.
   */
  @Test
  void record_degradedThenNormal_keepsNewestRecord() {
    BroadcastBuffer buf = buffer();
    buf.record("key", Long.MIN_VALUE + 42L, true);
    buf.record("key", 5L, false);
    buf.flush();

    verify(publisher).broadcastRefresh("key", 5L, false);
    verifyNoMoreInteractions(publisher);
  }

  /**
   * Across the degraded boundary the newest RECORD wins: a degraded write recorded
   * after a normal one is kept pending — peers without an entry must still fetch
   * the fresh value, while peers holding normal entries skip the degraded REFRESH
   * on their own (the legacy behavior the existing integration test pins).
   */
  @Test
  void record_normalThenDegraded_keepsNewestRecord() {
    BroadcastBuffer buf = buffer();
    buf.record("key", 5L, false);
    buf.record("key", Long.MIN_VALUE + 42L, true);
    buf.flush();

    verify(publisher).broadcastRefresh("key", Long.MIN_VALUE + 42L, true);
    verifyNoMoreInteractions(publisher);
  }

  /** Within the degraded space, the newer snowflake allocation wins. */
  @Test
  void record_degradedSpace_shouldKeepNewerAllocation() {
    BroadcastBuffer buf = buffer();
    buf.record("key", Long.MIN_VALUE + 100L, true);
    buf.record("key", Long.MIN_VALUE + 200L, true);
    buf.flush();

    verify(publisher).broadcastRefresh("key", Long.MIN_VALUE + 200L, true);
    verifyNoMoreInteractions(publisher);
  }

  /** Identical records stay a single entry (the reuse fast path the merge preserves). */
  @Test
  void record_identicalRecords_shouldFlushOnce() {
    BroadcastBuffer buf = buffer();
    buf.record("key", 5L, false);
    buf.record("key", 5L, false);
    buf.flush();

    verify(publisher, times(1)).broadcastRefresh("key", 5L, false);
  }
}
