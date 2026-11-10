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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.*;

import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
    assertThatCode(() -> new BroadcastBuffer(scheduler, null, 500, 2_000, null)).doesNotThrowAnyException();
  }

  /**
   * Verifies that a BroadcastBuffer can be created with a custom flush delay.
   */
  @Test
  void constructor_shouldAcceptCustomFlushDelay() {
    assertThatCode(() ->
      new BroadcastBuffer(scheduler, null, 100L, Math.max(100L, 2_000), null)
    ).doesNotThrowAnyException();
  }

  // ── record() behavior ──

  /**
   * Verifies that recording a key populates the internal pending map, observable via flush side
   * effects.
   */
  @Test
  void record_shouldLazyInitPendingMap() {
    BroadcastBuffer buf = new BroadcastBuffer(scheduler, publisher, 5000L, Math.max(5000L, 2_000), null);
    buf.record("key1", 1L, null, null);
    buf.flush();
    verify(publisher).broadcastRefresh("key1", 1L);
  }

  /**
   * Verifies that recording the same key twice merges by last-writer-wins — only the latest version
   * is flushed.
   */
  @Test
  void record_shouldMergeSameKey_latestWins() {
    BroadcastBuffer buf = new BroadcastBuffer(scheduler, publisher, 5000L, Math.max(5000L, 2_000), null);
    buf.record("key", 1L, null, null);
    buf.record("key", 5L, null, null);
    buf.flush();
    verify(publisher).broadcastRefresh("key", 5L);
    verifyNoMoreInteractions(publisher);
  }

  /**
   * Verifies that recording two different keys results in both being flushed.
   */
  @Test
  void record_differentKeys_shouldBothBeFlushed() {
    BroadcastBuffer buf = new BroadcastBuffer(scheduler, publisher, 5000L, Math.max(5000L, 2_000), null);
    buf.record("key1", 1L, null, null);
    buf.record("key2", 2L, null, null);
    buf.flush();
    verify(publisher).broadcastRefresh("key1", 1L);
    verify(publisher).broadcastRefresh("key2", 2L);
  }

  // ── flush() behavior ──

  /**
   * Verifies that flush sends all pending entries to the sync publisher.
   */
  @Test
  void flush_shouldSendPendingToPublisher() {
    BroadcastBuffer buf = new BroadcastBuffer(scheduler, publisher, 5000L, Math.max(5000L, 2_000), null);
    buf.record("a", 1L, null, null);
    buf.record("b", 2L, null, null);
    buf.flush();
    verify(publisher).broadcastRefresh("a", 1L);
    verify(publisher).broadcastRefresh("b", 2L);
  }

  /**
   * Verifies that a send-executor rejection under a chunked snapshot drops only
   * the rejected chunk: the first chunk's entries still reach the publisher and
   * {@code saturationDrops} counts only the tail. (Previously one rejection
   * stranded the whole flushed snapshot — up to 100k PUT/REFRESH broadcasts,
   * leaving peers to converge only via their hard TTL.)
   */
  @Test
  void flush_chunkedSend_executorRejectsTail_shouldDeliverHeadAndCountDrops() {
    AtomicInteger submissions = new AtomicInteger();
    Executor failingTailExecutor = task -> {
      if (submissions.incrementAndGet() > 1) {
        throw new RejectedExecutionException("saturated");
      }
      task.run();
    };
    BroadcastBuffer buf = new BroadcastBuffer(scheduler, publisher, 5000L, Math.max(5000L, 2_000), failingTailExecutor);
    for (int i = 0; i < 1100; i++) {
      buf.record("k" + i, i, null, null);
    }
    buf.flush();
    // Chunk 1 (1024 entries) ran through the executor; chunk 2 (76) was refused.
    verify(publisher, times(1024)).broadcastRefresh(anyString(), anyLong());
    assertThat(buf.saturationDrops()).isEqualTo(76);
  }

  /**
   * Verifies that flush does not throw when there is no publisher.
   */
  @Test
  void flush_withNoPublisher_shouldNotThrow() {
    BroadcastBuffer buf = new BroadcastBuffer(scheduler, null, 500, 2_000, null);
    buf.record("key", 1L, null, null);
    assertThatCode(buf::flush).doesNotThrowAnyException();
  }

  /**
   * Verifies that flush with no records never calls the publisher.
   */
  @Test
  void flush_withNoRecords_shouldNotPublish() {
    BroadcastBuffer buf = new BroadcastBuffer(scheduler, publisher, 5000L, Math.max(5000L, 2_000), null);
    buf.flush();
    verifyNoInteractions(publisher);
  }

  // ── Size cap ──

  /**
   * Installs a counting answer on the publisher that releases the latch once the given number of
   * broadcasts has been observed.
   *
   * @param sent   the counter incremented per broadcast
   * @param target the add at which the latch is released
   * @return the latch released when the target add is reached
   */
  private CountDownLatch installCountingPublisher(AtomicInteger sent, int target) {
    CountDownLatch latch = new CountDownLatch(1);
    doAnswer(inv -> {
      if (sent.incrementAndGet() == target) {
        latch.countDown();
      }
      return null;
    })
      .when(publisher)
      .broadcastRefresh(anyString(), anyLong());
    return latch;
  }

  /**
   * Verifies that the pending map is capped: recording exactly the cap never flushes by itself, but
   * the first record above the cap forces an automatic flush of every buffered key.
   */
  @Test
  void record_whenExceedingCap_shouldForceFlushAutomatically() throws Exception {
    AtomicInteger sent = new AtomicInteger(0);
    CountDownLatch allSent = installCountingPublisher(sent, BroadcastBuffer.MAX_PENDING_ENTRIES + 1);

    // Long flush delay so the deferred flush cannot fire during this test
    BroadcastBuffer buf = new BroadcastBuffer(scheduler, publisher, 5000L, Math.max(5000L, 2_000), null);

    for (int i = 0; i < BroadcastBuffer.MAX_PENDING_ENTRIES; i++) {
      buf.record("key-" + i, i, null, null);
    }
    // At exactly the cap nothing is sent yet (threshold is strictly greater-than)
    Thread.sleep(300);
    assertThat(sent).hasValue(0);

    // One more record crosses the cap and forces a flush. The publisher drains
    // 100k buffered keys through a mock; under a loaded CI JVM that can take
    // several seconds, so the await window is generous while a broken forced
    // flush still fails the assertion.
    buf.record("key-cap", 1L, null, null);
    boolean flushed = allSent.await(10_000, TimeUnit.MILLISECONDS);
    org.junit.jupiter.api.Assertions.assertTrue(flushed, "Forced flush should have fired within timeout");
    org.junit.jupiter.api.Assertions.assertEquals(
      BroadcastBuffer.MAX_PENDING_ENTRIES + 1,
      sent.get(),
      "Forced flush should send every buffered key exactly once"
    );
  }

  /**
   * Verifies that a forced flush swaps to a fresh map: records made after the forced flush are
   * buffered normally and delivered by the next explicit flush.
   */
  @Test
  void record_afterForcedFlush_shouldContinueBuffering() throws Exception {
    AtomicInteger sent = new AtomicInteger(0);
    CountDownLatch allSent = installCountingPublisher(sent, BroadcastBuffer.MAX_PENDING_ENTRIES + 1);

    BroadcastBuffer buf = new BroadcastBuffer(scheduler, publisher, 5000L, Math.max(5000L, 2_000), null);

    for (int i = 0; i < BroadcastBuffer.MAX_PENDING_ENTRIES + 1; i++) {
      buf.record("key-" + i, i, null, null);
    }

    boolean flushed = allSent.await(2000, TimeUnit.MILLISECONDS);
    org.junit.jupiter.api.Assertions.assertTrue(flushed, "Forced flush should have fired within timeout");
    org.junit.jupiter.api.Assertions.assertEquals(BroadcastBuffer.MAX_PENDING_ENTRIES + 1, sent.get());

    buf.record("post-forced", 1L, null, null);
    buf.flush();
    verify(publisher).broadcastRefresh("post-forced", 1L);
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
      .broadcastRefresh("auto-key", 1L);

    BroadcastBuffer buf = new BroadcastBuffer(scheduler, spyPublisher, 100L, Math.max(100L, 2_000), null);
    buf.record("auto-key", 1L, null, null);

    boolean fired = latch.await(2000, TimeUnit.MILLISECONDS);
    org.junit.jupiter.api.Assertions.assertTrue(fired, "Scheduled flush should have fired within timeout");
    verify(spyPublisher).broadcastRefresh("auto-key", 1L);
  }

  /**
   * Records do not cancel/re-arm the pending flush: a record arriving while a
   * flush is already scheduled rides the ORIGINAL schedule — the flush swaps
   * the whole pending map at fire time, so the later record is included in the
   * same send cycle. The old per-record reschedule would have pushed the flush
   * out by another full delay on every write.
   */
  @Test
  void record_whileFlushPending_shouldRideOriginalSchedule() throws Exception {
    CountDownLatch flushed = new CountDownLatch(1);
    CacheSyncPublisher spyPublisher = mock(CacheSyncPublisher.class);
    doAnswer(inv -> {
      flushed.countDown();
      return null;
    })
      .when(spyPublisher)
      .broadcastRefresh("second", 2L);

    BroadcastBuffer buf = new BroadcastBuffer(scheduler, spyPublisher, 400L, Math.max(400L, 2_000), null);
    long firstRecordAt = System.nanoTime();
    buf.record("first", 1L, null, null);
    // Lands while the first record's flush is pending: the flush must NOT be
    // pushed out to ~second + 400ms.
    Thread.sleep(250);
    buf.record("second", 2L, null, null);

    boolean fired = flushed.await(2, TimeUnit.SECONDS);
    org.junit.jupiter.api.Assertions.assertTrue(fired, "Scheduled flush should have fired within timeout");
    long elapsedMs = (System.nanoTime() - firstRecordAt) / 1_000_000;
    org.junit.jupiter.api.Assertions.assertTrue(
      elapsedMs < 600,
      "Flush should ride the original schedule, not be rescheduled per record (fired at ~" + elapsedMs + "ms)"
    );
    // The later record is delivered by the same flush (map swapped at fire time).
    verify(spyPublisher).broadcastRefresh("first", 1L);
    verify(spyPublisher).broadcastRefresh("second", 2L);
  }

  // ── Error handling ──

  /**
   * Verifies that when the publisher throws on one key, flush catches the exception and continues
   * processing remaining keys.
   */
  @Test
  void flush_whenPublisherThrows_shouldNotPropagate() {
    CacheSyncPublisher throwingPub = mock(CacheSyncPublisher.class);
    doThrow(new RuntimeException("publisher failure")).when(throwingPub).broadcastRefresh("bad-key", 1L);

    BroadcastBuffer buf = new BroadcastBuffer(scheduler, throwingPub, 5000L, Math.max(5000L, 2_000), null);
    buf.record("bad-key", 1L, null, null);
    buf.record("good-key", 2L, null, null);

    assertThatCode(buf::flush).doesNotThrowAnyException();
    verify(throwingPub).broadcastRefresh("bad-key", 1L);
    verify(throwingPub).broadcastRefresh("good-key", 2L);
  }

  // ── Send executor isolation (design decisions: buffering and ordering) ──

  /**
   * Verifies that with a send executor, flush hands the send loop off asynchronously: the
   * publisher is invoked on the executor thread, never on the calling thread.
   */
  @Test
  void flush_withSendExecutor_shouldSendOnExecutorThread() throws Exception {
    ExecutorService sendExecutor = Executors.newSingleThreadExecutor();
    try {
      AtomicReference<String> senderThread = new AtomicReference<>();
      doAnswer(inv -> {
        senderThread.set(Thread.currentThread().getName());
        return null;
      })
        .when(publisher)
        .broadcastRefresh("key", 1L);

      BroadcastBuffer buf = new BroadcastBuffer(scheduler, publisher, 5000L, 2000L, sendExecutor);
      buf.record("key", 1L, null, null);
      buf.flush();

      sendExecutor.shutdown();
      boolean finished = sendExecutor.awaitTermination(2, TimeUnit.SECONDS);
      org.junit.jupiter.api.Assertions.assertTrue(finished, "Async send should finish within timeout");
      verify(publisher).broadcastRefresh("key", 1L);
      assertThat(senderThread.get()).isNotNull().isNotEqualTo(Thread.currentThread().getName());
    } finally {
      sendExecutor.shutdownNow();
    }
  }

  /**
   * Verifies that a saturated send executor drops the batch without throwing and without
   * invoking the publisher — the drop is deliberate backpressure (design decisions: sync messages), the next
   * flush re-sends.
   */
  @Test
  void flush_whenSendExecutorRejects_shouldDropBatchWithoutThrowing() {
    Executor rejecting = mock(Executor.class);
    doThrow(new RejectedExecutionException("saturated")).when(rejecting).execute(any(Runnable.class));

    BroadcastBuffer buf = new BroadcastBuffer(scheduler, publisher, 5000L, 2000L, rejecting);
    buf.record("key", 1L, null, null);

    assertThatCode(buf::flush).doesNotThrowAnyException();
    verifyNoInteractions(publisher);
  }

  /**
   * Verifies that a send executor throwing a non-rejection unchecked exception (caller-supplied
   * executor SPI, e.g. a shutting-down custom pool) drops the batch without throwing into the
   * caller: a deferred send is fire-and-forget and must never fail the write path that triggered
   * it (design decisions: sync messages).
   */
  @Test
  void flush_whenSendExecutorThrowsUnchecked_shouldDropBatchWithoutThrowing() {
    Executor hostile = task -> {
      throw new IllegalStateException("custom pool is shutting down");
    };

    BroadcastBuffer buf = new BroadcastBuffer(scheduler, publisher, 5000L, 2000L, hostile);
    buf.record("key", 1L, null, null);

    assertThatCode(buf::flush).doesNotThrowAnyException();
    assertThat(buf.saturationDrops()).isEqualTo(1);
    verifyNoInteractions(publisher);
  }

  /**
   * Verifies that a dropped batch is not replayed: after a saturation drop, the next flush
   * delivers only its own entries (design decisions: buffering and ordering — loss is tolerated, recovery re-sends fresh state).
   */
  @Test
  void flush_whenSendExecutorRejectsOnce_shouldDeliverNextFlush() throws Exception {
    AtomicInteger attempts = new AtomicInteger(0);
    ExecutorService sendExecutor = Executors.newSingleThreadExecutor();
    try {
      CountDownLatch sent = new CountDownLatch(1);
      doAnswer(inv -> {
        sent.countDown();
        return null;
      })
        .when(publisher)
        .broadcastRefresh("key2", 2L);

      Executor flaky = task -> {
        if (attempts.getAndIncrement() == 0) {
          throw new RejectedExecutionException("saturated");
        }
        sendExecutor.execute(task);
      };

      BroadcastBuffer buf = new BroadcastBuffer(scheduler, publisher, 5000L, 2000L, flaky);
      buf.record("key1", 1L, null, null);
      buf.flush(); // dropped

      buf.record("key2", 2L, null, null);
      buf.flush(); // delivered

      boolean delivered = sent.await(2, TimeUnit.SECONDS);
      org.junit.jupiter.api.Assertions.assertTrue(delivered, "Next flush should be delivered within timeout");
      verify(publisher).broadcastRefresh("key2", 2L);
      verifyNoMoreInteractions(publisher);
    } finally {
      sendExecutor.shutdownNow();
    }
  }

  /**
   * Verifies that when the scheduler rejects the deferred flush scheduling, record degrades to a
   * synchronous flush so the pending entry is still delivered.
   */
  @Test
  void record_whenScheduleRejected_shouldFlushSynchronously() {
    ScheduledExecutorService rejecting = mock(ScheduledExecutorService.class);
    when(rejecting.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenThrow(
      new RejectedExecutionException("saturated")
    );

    BroadcastBuffer buf = new BroadcastBuffer(rejecting, publisher, 5000L, Math.max(5000L, 2_000), null);
    buf.record("key1", 1L, null, null);

    verify(publisher).broadcastRefresh("key1", 1L);
  }

  /**
   * Verifies that crossing the pending cap does not submit one scheduler task per
   * record.
   *
   * <p>The over-the-cap branch used to call {@code scheduler.execute(flushAndReset)}
   * unconditionally, so a burst across the cap enqueued an unbounded number of
   * tasks into the shared scheduler's queue — delaying the reporter flush tick
   * and the sketch decay that share the pool, and each of which had to run
   * before the queue could be serviced. The cap path now arms (or re-arms) a
   * single task instead.
   *
   * <p>Pinned by asserting {@code execute} is never used for the forced flush
   * and that scheduling stays bounded as the burst continues.
   */
  @Test
  void record_whenCapExceeded_shouldNotSubmitOneTaskPerRecord() {
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    @SuppressWarnings("unchecked")
    ScheduledFuture<Object> armed = mock(ScheduledFuture.class);
    doReturn(armed).when(scheduler).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    // An armed task that is already due: no re-arm needed, and no new task added.
    when(armed.isDone()).thenReturn(false);
    when(armed.getDelay(any(TimeUnit.class))).thenReturn(0L);

    BroadcastBuffer buf = new BroadcastBuffer(scheduler, publisher, 5000L, Math.max(5000L, 2_000), null);

    for (int i = 0; i < BroadcastBuffer.MAX_PENDING_ENTRIES + 50; i++) {
      buf.record("key-" + i, i, null, null);
    }

    // The forced flush never uses execute() — that was the unbounded path.
    verify(scheduler, never()).execute(any(Runnable.class));
    // A burst across the cap must not schedule a task per record: one arm for the
    // deferral window plus at most one re-arm when the cap is first crossed.
    verify(scheduler, atMost(3)).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
  }

  /**
   * Control for the test above: the deferral path does arm exactly one task, so
   * the bounded-schedule assertion above is measuring the cap branch and not a
   * buffer that never schedules anything.
   */
  @Test
  void record_firstRecord_shouldArmExactlyOneFlushTask() {
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    @SuppressWarnings("unchecked")
    ScheduledFuture<Object> armed = mock(ScheduledFuture.class);
    doReturn(armed).when(scheduler).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    when(armed.isDone()).thenReturn(false);

    BroadcastBuffer buf = new BroadcastBuffer(scheduler, publisher, 5000L, Math.max(5000L, 2_000), null);
    buf.record("key1", 1L, null, null);

    verify(scheduler, times(1)).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
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
      .broadcastRefresh(anyString(), anyLong());

    BroadcastBuffer buf = new BroadcastBuffer(scheduler, spyPub, 5000L, Math.max(5000L, 2_000), null);
    buf.record("pre-flush", 1L, null, null);

    // flush swaps pending so the old map is iterated; record after swap goes to new map
    buf.flush();
    buf.record("post-flush", 2L, null, null);

    // first flush saw "pre-flush" only
    verify(spyPub).broadcastRefresh("pre-flush", 1L);

    // second flush should see "post-flush"
    buf.flush();
    verify(spyPub).broadcastRefresh("post-flush", 2L);
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
      .broadcastRefresh(anyString(), anyLong());

    BroadcastBuffer buf = new BroadcastBuffer(scheduler, spyPub, 5000L, Math.max(5000L, 2_000), null);
    int threadCount = 4;
    int recordsPerThread = 100;

    ExecutorService pool = Executors.newFixedThreadPool(threadCount);

    for (int i = 0; i < threadCount; i++) {
      int threadIdx = i;
      pool.submit(() -> {
        for (int j = 0; j < recordsPerThread; j++) {
          buf.record("key-" + threadIdx + "-" + j, j, null, null);
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

  // ── Immediate INVALIDATE family ──

  @Test
  void broadcastInvalidate_withPublisher_runsOnTheSendExecutor() throws Exception {
    ExecutorService sendExecutor = Executors.newSingleThreadExecutor();
    try {
      BroadcastBuffer buf = new BroadcastBuffer(scheduler, publisher, 5000L, 2_000L, sendExecutor);
      CountDownLatch latch = new CountDownLatch(1);
      doAnswer(inv -> {
        latch.countDown();
        return null;
      })
        .when(publisher)
        .broadcastInvalidate("key5", 1L);

      buf.invalidate("key5", 1L);

      assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
      verify(publisher).broadcastInvalidate("key5", 1L);
    } finally {
      sendExecutor.shutdownNow();
    }
  }

  @Test
  void broadcastInvalidate_withoutExecutor_runsInline() {
    BroadcastBuffer buf = new BroadcastBuffer(scheduler, publisher, 5000L, 2_000L, null);
    buf.invalidate("key5b", 1L);
    verify(publisher, timeout(1000)).broadcastInvalidate("key5b", 1L);
  }

  @Test
  void broadcastInvalidate_withoutPublisher_isNoOp() {
    BroadcastBuffer buf = new BroadcastBuffer(scheduler, null, 5000L, 2_000L, null);
    assertThatCode(() -> buf.invalidate("key6", 2L)).doesNotThrowAnyException();
  }

  @Test
  void invalidate_withRejectingExecutor_countsTheDrop() {
    Executor rejecting = command -> {
      throw new RejectedExecutionException("saturated");
    };
    BroadcastBuffer buf = new BroadcastBuffer(scheduler, publisher, 5000L, 2_000L, rejecting);

    buf.invalidate("key7", 3L);

    assertThat(buf.invalidatesDropped()).isEqualTo(1L);
  }

  @Test
  void broadcastInvalidateBatch_sendsOneChunk() {
    BroadcastBuffer buf = new BroadcastBuffer(scheduler, publisher, 5000L, 2_000L, null);
    List<SyncMessage.Entry> entries = List.of(new SyncMessage.Entry("a", 1L), new SyncMessage.Entry("b", 2L));
    buf.invalidateBatch(entries);
    verify(publisher).broadcastInvalidateBatch(entries);
  }

  @Test
  void broadcastInvalidateAll_sendsOneClear() {
    BroadcastBuffer buf = new BroadcastBuffer(scheduler, publisher, 5000L, 2_000L, null);
    buf.invalidateAll(9L);
    verify(publisher).broadcastInvalidateAll(9L);
  }

  @Test
  void broadcastPut_serializable_buffersPutRecord() {
    BroadcastBuffer buf = new BroadcastBuffer(scheduler, publisher, 5000L, 2_000L, null);
    byte[] payload = "value-bytes".getBytes(StandardCharsets.UTF_8);
    when(publisher.serializePutPayload("v")).thenReturn(payload);

    buf.put("key9", "v", 5L, null);
    buf.flush();

    verify(publisher).broadcastPut("key9", 5L, payload);
    verify(publisher, never()).broadcastRefresh(anyString(), anyLong());
  }

  @Test
  void broadcastPut_serializationFails_degradesToRefresh() {
    BroadcastBuffer buf = new BroadcastBuffer(scheduler, publisher, 5000L, 2_000L, null);
    when(publisher.serializePutPayload("v")).thenReturn(null);

    buf.put("key10", "v", 6L, null);
    buf.flush();

    verify(publisher).broadcastRefresh("key10", 6L);
    verify(publisher, never()).broadcastPut(anyString(), anyLong(), any());
  }

  @Test
  void broadcastPut_withoutPublisher_isNoOp() {
    BroadcastBuffer buf = new BroadcastBuffer(scheduler, null, 5000L, 2_000L, null);
    assertThatCode(() -> buf.put("key11", "v", 7L, null)).doesNotThrowAnyException();
  }
}
