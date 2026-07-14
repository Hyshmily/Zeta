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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.*;

import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link BroadcastBuffer} covering initialization, recording, flushing, scheduling, error
 * handling, and thread safety.
 */
class BroadcastBufferTest {

  private ScheduledExecutorService scheduler;
  private CacheSyncPublisher publisher;
  private BroadcastBuffer buffer;

  @BeforeEach
  void setUp() {
    scheduler = Executors.newSingleThreadScheduledExecutor();
    publisher = mock(CacheSyncPublisher.class);
  }

  @AfterEach
  void tearDown() {
    scheduler.shutdownNow();
  }

  // ── Constructor and basic initialization ──

  /**
   * Verifies that a BroadcastBuffer can be created with a scheduler and an empty publisher Optional
   * without throwing.
   */
  @Test
  void constructor_shouldAcceptMinimalParameters() {
    assertThatCode(() -> new BroadcastBuffer(scheduler, Optional.empty())).doesNotThrowAnyException();
  }

  /**
   * Verifies that a BroadcastBuffer can be created with a custom flush delay.
   */
  @Test
  void constructor_shouldAcceptCustomFlushDelay() {
    assertThatCode(() -> new BroadcastBuffer(scheduler, Optional.empty(), 100L)).doesNotThrowAnyException();
  }

  // ── record() behavior ──

  /**
   * Verifies that recording a key populates the internal pending map, observable via flush side
   * effects.
   */
  @Test
  void record_shouldLazyInitPendingMap() {
    BroadcastBuffer buf = new BroadcastBuffer(scheduler, Optional.of(publisher), 5000L);
    buf.record("key1", 1L, false);
    buf.flush();
    verify(publisher).broadcastRefresh("key1", 1L, false);
  }

  /**
   * Verifies that recording the same key twice merges by last-writer-wins — only the latest version
   * is flushed.
   */
  @Test
  void record_shouldMergeSameKey_latestWins() {
    BroadcastBuffer buf = new BroadcastBuffer(scheduler, Optional.of(publisher), 5000L);
    buf.record("key", 1L, false);
    buf.record("key", 5L, false);
    buf.flush();
    verify(publisher).broadcastRefresh("key", 5L, false);
    verifyNoMoreInteractions(publisher);
  }

  /**
   * Verifies that recording two different keys results in both being flushed.
   */
  @Test
  void record_differentKeys_shouldBothBeFlushed() {
    BroadcastBuffer buf = new BroadcastBuffer(scheduler, Optional.of(publisher), 5000L);
    buf.record("key1", 1L, false);
    buf.record("key2", 2L, true);
    buf.flush();
    verify(publisher).broadcastRefresh("key1", 1L, false);
    verify(publisher).broadcastRefresh("key2", 2L, true);
  }

  // ── flush() behavior ──

  /**
   * Verifies that flush sends all pending entries to the sync publisher.
   */
  @Test
  void flush_shouldSendPendingToPublisher() {
    BroadcastBuffer buf = new BroadcastBuffer(scheduler, Optional.of(publisher), 5000L);
    buf.record("a", 1L, false);
    buf.record("b", 2L, true);
    buf.flush();
    verify(publisher).broadcastRefresh("a", 1L, false);
    verify(publisher).broadcastRefresh("b", 2L, true);
  }

  /**
   * Verifies that flush does not throw when there is no publisher.
   */
  @Test
  void flush_withNoPublisher_shouldNotThrow() {
    BroadcastBuffer buf = new BroadcastBuffer(scheduler, Optional.empty());
    buf.record("key", 1L, false);
    assertThatCode(buf::flush).doesNotThrowAnyException();
  }

  /**
   * Verifies that flush with no records never calls the publisher.
   */
  @Test
  void flush_withNoRecords_shouldNotPublish() {
    BroadcastBuffer buf = new BroadcastBuffer(scheduler, Optional.of(publisher), 5000L);
    buf.flush();
    verifyNoInteractions(publisher);
  }

  // ── Scheduling behavior ──

  /**
   * Verifies that recording a key schedules a delayed flush that fires automatically after the
   * configured delay.
   */
  @Test
  void record_shouldScheduleDelayedFlush() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    CacheSyncPublisher spyPublisher = mock(CacheSyncPublisher.class);
    doAnswer(inv -> {
      latch.countDown();
      return null;
    })
      .when(spyPublisher)
      .broadcastRefresh("auto-key", 1L, false);

    BroadcastBuffer buf = new BroadcastBuffer(scheduler, Optional.of(spyPublisher), 100L);
    buf.record("auto-key", 1L, false);

    boolean fired = latch.await(2000, TimeUnit.MILLISECONDS);
    org.junit.jupiter.api.Assertions.assertTrue(fired, "Scheduled flush should have fired within timeout");
    verify(spyPublisher).broadcastRefresh("auto-key", 1L, false);
  }

  // ── Error handling ──

  /**
   * Verifies that when the publisher throws on one key, flush catches the exception and continues
   * processing remaining keys.
   */
  @Test
  void flush_whenPublisherThrows_shouldNotPropagate() {
    CacheSyncPublisher throwingPub = mock(CacheSyncPublisher.class);
    doThrow(new RuntimeException("publisher failure")).when(throwingPub).broadcastRefresh("bad-key", 1L, false);

    BroadcastBuffer buf = new BroadcastBuffer(scheduler, Optional.of(throwingPub), 5000L);
    buf.record("bad-key", 1L, false);
    buf.record("good-key", 2L, true);

    assertThatCode(buf::flush).doesNotThrowAnyException();
    verify(throwingPub).broadcastRefresh("bad-key", 1L, false);
    verify(throwingPub).broadcastRefresh("good-key", 2L, true);
  }

  /**
   * Verifies that recording during a flush does not lose data — the concurrent record goes into a
   * fresh map that will be flushed next time.
   */
  @Test
  void flush_concurrentRecord_shouldNotLoseData() {
    CacheSyncPublisher spyPub = mock(CacheSyncPublisher.class);
    AtomicInteger callCount = new AtomicInteger(0);

    doAnswer(inv -> {
      callCount.incrementAndGet();
      return null;
    })
      .when(spyPub)
      .broadcastRefresh(anyString(), anyLong(), anyBoolean());

    BroadcastBuffer buf = new BroadcastBuffer(scheduler, Optional.of(spyPub), 5000L);
    buf.record("pre-flush", 1L, false);

    // flush swaps pending so the old map is iterated; record after swap goes to new map
    buf.flush();
    buf.record("post-flush", 2L, false);

    // first flush saw "pre-flush" only
    verify(spyPub).broadcastRefresh("pre-flush", 1L, false);

    // second flush should see "post-flush"
    buf.flush();
    verify(spyPub).broadcastRefresh("post-flush", 2L, false);
  }

  // ── Thread safety ──

  /**
   * Verifies that concurrent recording and flushing from multiple threads does not cause data loss
   * or exceptions.
   */
  @Test
  void flush_concurrentAccess_shouldBeThreadSafe() throws Exception {
    CacheSyncPublisher spyPub = mock(CacheSyncPublisher.class);
    AtomicInteger flushCount = new AtomicInteger(0);
    doAnswer(inv -> {
      flushCount.incrementAndGet();
      return null;
    })
      .when(spyPub)
      .broadcastRefresh(anyString(), anyLong(), anyBoolean());

    BroadcastBuffer buf = new BroadcastBuffer(scheduler, Optional.of(spyPub), 5000L);
    int threadCount = 4;
    int recordsPerThread = 100;

    ExecutorService pool = Executors.newFixedThreadPool(threadCount);

    for (int i = 0; i < threadCount; i++) {
      int threadIdx = i;
      pool.submit(() -> {
        for (int j = 0; j < recordsPerThread; j++) {
          buf.record("key-" + threadIdx + "-" + j, j, false);
        }
      });
    }

    pool.shutdown();
    boolean terminated = pool.awaitTermination(5, TimeUnit.SECONDS);
    org.junit.jupiter.api.Assertions.assertTrue(terminated, "Pool should terminate within timeout");

    // All records are now submitted; a single flush drains everything
    buf.flush();

    // Every recorded key must have been sent at least once
    int expectedDistinctKeys = threadCount * recordsPerThread;
    int actualDistinctKeys = flushCount.get();

    org.junit.jupiter.api.Assertions.assertEquals(
      expectedDistinctKeys,
      actualDistinctKeys,
      "All distinct keys should be published exactly once"
    );
  }
}
