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
package io.github.hyshmily.zeta.cache;

import static org.mockito.Mockito.*;

import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Deterministic tests for {@link BroadcastBuffer#record}'s version-ordered merge
 * (design decisions: version stamps, one stamp space): concurrent writers record out of allocation
 * order (the stamp allocation and the record are separated by the L1 apply),
 * so the buffer must keep the newest allocation instead of the last record.
 * An out-of-order {@code record(key, 5)} after {@code record(key, 6)} would
 * otherwise regress the pending version and the v6 REFRESH would never be
 * sent — peers pinned on stale data until the next write or TTL.
 *
 * <p>Kept separate from {@code BroadcastBufferTest} so this deterministic merge coverage
 * is not mixed with that class's timing tests.
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
    return new BroadcastBuffer(scheduler, publisher, 5000L, Math.max(5000L, 2_000), null);
  }

  /**
   * The P1 regression: record(v6) followed by an out-of-order record(v5) must keep
   * v6 pending — the flush must send the newest write's REFRESH, not the regressed
   * one a peer would skip.
   */
  @Test
  void record_outOfOrderRecords_shouldKeepNewestVersion() {
    BroadcastBuffer buf = buffer();
    buf.record("key", 6L, null, null);
    buf.record("key", 5L, null, null);
    buf.flush();

    verify(publisher).broadcastRefresh("key", 6L);
    verifyNoMoreInteractions(publisher);
  }

  /** Ascending records keep collapsing onto the newest — the common sequential case. */
  @Test
  void record_ascendingRecords_shouldKeepNewestVersion() {
    BroadcastBuffer buf = buffer();
    buf.record("key", 5L, null, null);
    buf.record("key", 6L, null, null);
    buf.record("key", 7L, null, null);
    buf.flush();

    verify(publisher).broadcastRefresh("key", 7L);
    verifyNoMoreInteractions(publisher);
  }

  /** Identical records stay a single entry (the reuse fast path the merge preserves). */
  @Test
  void record_identicalRecords_shouldFlushOnce() {
    BroadcastBuffer buf = buffer();
    buf.record("key", 5L, null, null);
    buf.record("key", 5L, null, null);
    buf.flush();

    verify(publisher, times(1)).broadcastRefresh("key", 5L);
  }

  // ------------------------------------------------------------------
  // Value-carrying PUT records
  // ------------------------------------------------------------------

  /** A PUT record flushes as broadcastPut with its payload — no broadcastRefresh on the side. */
  @Test
  void record_withPayload_flushSendsBroadcastPut() {
    BroadcastBuffer buf = buffer();
    byte[] payload = "value-bytes".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    buf.record("key", 6L, payload, null);
    buf.flush();

    verify(publisher).broadcastPut("key", 6L, payload);
    verify(publisher, never()).broadcastRefresh(anyString(), anyLong());
  }

  /** The payload travels with its version: an older version-only record cannot displace a newer PUT. */
  @Test
  void record_payloadNewerThanRefresh_keepsPayload() {
    BroadcastBuffer buf = buffer();
    byte[] payload = "p6".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    buf.record("key", 6L, payload, null);
    buf.record("key", 5L, null, null);
    buf.flush();

    verify(publisher).broadcastPut("key", 6L, payload);
    verifyNoMoreInteractions(publisher);
  }

  /** A newer version-only REFRESH displaces an older PUT — payload merges together with its version. */
  @Test
  void record_newerRefreshDisplacesOlderPut() {
    BroadcastBuffer buf = buffer();
    buf.record("key", 5L, "p5".getBytes(java.nio.charset.StandardCharsets.UTF_8), null);
    buf.record("key", 7L, null, null);
    buf.flush();

    verify(publisher).broadcastRefresh("key", 7L);
    verifyNoMoreInteractions(publisher);
  }

  // ------------------------------------------------------------------
  // Causal cancellation
  // ------------------------------------------------------------------

  /**
   * A same-key invalidation cancels only records at or below its stamp; an
   * absent key or an already-newer record is left untouched.
   */
  @Test
  void evictIfAtOrBelow_cancelsOldRecordsOnly() {
    BroadcastBuffer buf = buffer();
    buf.record("old", 5L, null, null);
    buf.record("new", 7L, null, null);

    buf.evictIfAtOrBelow("old", 5L);
    buf.evictIfAtOrBelow("absent", 10L);
    buf.evictIfAtOrBelow("new", 6L);
    buf.flush();

    verify(publisher).broadcastRefresh("new", 7L);
    verifyNoMoreInteractions(publisher);
  }

  /** A prefix clear cancels matching old records and leaves other namespaces alone. */
  @Test
  void evictPrefixIfAtOrBelow_removesOnlyMatchingOldRecords() {
    BroadcastBuffer buf = buffer();
    buf.record("ns:a", 3L, null, null);
    buf.record("ns:b", 9L, null, null);
    buf.record("other", 1L, null, null);

    buf.evictPrefixIfAtOrBelow("ns:", 5L);
    buf.flush();

    verify(publisher).broadcastRefresh("ns:b", 9L);
    verify(publisher).broadcastRefresh("other", 1L);
    verifyNoMoreInteractions(publisher);
  }

  // ------------------------------------------------------------------
  // Currentness predicate
  // ------------------------------------------------------------------

  /**
   * The serialize-then-record guard: a record its currentness predicate refuses never
   * enters the buffer, so an invalidation that landed while the payload was being built
   * cannot be re-warmed by the flush.
   */
  @Test
  void record_stillCurrentRefuses_shouldNotFlush() {
    BroadcastBuffer buf = buffer();
    byte[] payload = "p6".getBytes(java.nio.charset.StandardCharsets.UTF_8);

    buf.record("key", 6L, payload, version -> false);
    buf.flush();

    verifyNoInteractions(publisher);
  }

  /** An accepting predicate admits the record unchanged, payload included. */
  @Test
  void record_stillCurrentAccepts_shouldFlushTheRecord() {
    BroadcastBuffer buf = buffer();
    byte[] payload = "p6".getBytes(java.nio.charset.StandardCharsets.UTF_8);

    buf.record("key", 6L, payload, version -> version == 6L);
    buf.flush();

    verify(publisher).broadcastPut("key", 6L, payload);
    verifyNoMoreInteractions(publisher);
  }

  /**
   * A tombstone that lands after the pre-merge check is caught by the post-merge
   * check: the record removes itself before the flush can send it.
   */
  @Test
  void record_tombstoneAfterPreMergeCheck_shouldRemoveItself() {
    BroadcastBuffer buf = buffer();
    byte[] payload = "p6".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    AtomicBoolean current = new AtomicBoolean(true);

    buf.record("key", 6L, payload, version -> current.getAndSet(false));

    buf.flush();
    verifyNoInteractions(publisher);
  }
}
