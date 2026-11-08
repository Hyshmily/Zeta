package io.github.hyshmily.zeta.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Central-dispatch tests: the async INVALIDATE family on the send executor and the PUT/REFRESH
 * buffering. Access routing itself is covered at {@link AccessCounter}.
 */
class CentralDispatcherTest {

  private HotKeyDetector detector;
  private KeyReporter reporter;
  private CacheSyncPublisher publisher;
  private BroadcastBuffer broadcastBuffer;
  private ScheduledExecutorService scheduler;
  private AccessCounter accessCounter;
  private java.util.concurrent.ExecutorService sendExecutor;
  private CentralDispatcher dispatcher;

  @BeforeEach
  void setUp() {
    detector = mock(HotKeyDetector.class);
    reporter = mock(KeyReporter.class);
    publisher = mock(CacheSyncPublisher.class);
    scheduler = Executors.newSingleThreadScheduledExecutor();
    sendExecutor = Executors.newSingleThreadExecutor();
    broadcastBuffer = new BroadcastBuffer(scheduler, null, 500, 2_000, null);
  }

  @AfterEach
  void tearDown() {
    if (accessCounter != null) {
      accessCounter.destroy();
    }
    sendExecutor.shutdownNow();
    scheduler.shutdownNow();
  }

  private AccessCounter newCounter(KeyReporter reporterOrNull) {
    accessCounter = new AccessCounter(
      detector,
      reporterOrNull,
      scheduler,
      50L,
      AccessCounter.DEFAULT_DETECTOR_INTERVAL_MS
    );
    return accessCounter;
  }

  @Test
  void accessCounter_tide_feedsBothTheReporterAndTheSketch() throws Exception {
    CountDownLatch reporterLatch = new CountDownLatch(1);
    doAnswer(inv -> {
      // Empty idle tides also publish; only the hot-key observation is the
      // signal this test waits for.
      java.util.Map<String, Long> batch = inv.getArgument(0);
      if (Long.valueOf(1L).equals(batch.get("hot-key"))) {
        reporterLatch.countDown();
      }
      return null;
    })
      .when(reporter)
      .publishBatch(any(), anyLong());

    AccessCounter counter = newCounter(reporter);
    counter.afterPropertiesSet();
    // A 10ms tide keeps the test fast; the sketch ingest waits for the 100ms detector cadence.
    AccessCounter fast = new AccessCounter(detector, reporter, scheduler, 10L, 100L);
    try {
      fast.afterPropertiesSet();
      fast.add("hot-key");
      assertThat(reporterLatch.await(2, TimeUnit.SECONDS)).isTrue();
      // Idle tides also publish an empty batch now (the feed loop samples zero rate),
      // so the non-empty observation is verified among the calls, not as the only one.
      verify(reporter, atLeastOnce()).publishBatch(
        argThat(batch -> Long.valueOf(1L).equals(batch.get("hot-key"))),
        anyLong()
      );
      // The sketch ingest follows on the detector cadence (100ms here).
      long deadline = System.currentTimeMillis() + 2_000;
      while (System.currentTimeMillis() < deadline) {
        try {
          verify(detector).ingest(any());
          break;
        } catch (AssertionError pending) {
          Thread.sleep(20);
        }
      }
      verify(detector).ingest(any());
    } finally {
      fast.destroy();
    }
  }

  @Test
  void accessCounter_countReportOnly_publishesASingleKeyBatchWithoutLocalCounting() {
    AccessCounter counter = newCounter(reporter);
    counter.addReportOnly("tagged");
    verify(reporter).publishDirect(argThat(batch -> Long.valueOf(1L).equals(batch.get("tagged"))));
  }

  @Test
  void accessCounter_countReportOnly_withoutReporter_isANoOp() {
    AccessCounter counter = newCounter(null);
    assertThatCode(() -> counter.addReportOnly("tagged")).doesNotThrowAnyException();
  }

  @Test
  void send_invalidate_withPublisher_runsOnTheSendExecutor() throws Exception {
    dispatcher = new CentralDispatcher(newCounter(null), publisher, broadcastBuffer, sendExecutor);
    AtomicBoolean done = new AtomicBoolean(false);
    CountDownLatch latch = new CountDownLatch(1);
    doAnswer(inv -> {
      done.set(true);
      latch.countDown();
      return null;
    })
      .when(publisher)
      .broadcastLocalInvalidate("key5", 1L);

    dispatcher.broadcastInvalidate("key5", 1L);

    // The send happens asynchronously — the calling thread must not block on AMQP.
    assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
    verify(publisher).broadcastLocalInvalidate("key5", 1L);
  }

  @Test
  void send_invalidate_withoutExecutor_runsInline() {
    dispatcher = new CentralDispatcher(newCounter(null), publisher, broadcastBuffer, null);
    dispatcher.broadcastInvalidate("key5b", 1L);
    verify(publisher, timeout(1000)).broadcastLocalInvalidate("key5b", 1L);
  }

  @Test
  void send_invalidate_withoutPublisher_shouldNotFail() {
    dispatcher = new CentralDispatcher(newCounter(null), null, broadcastBuffer, sendExecutor);
    assertThatCode(() -> dispatcher.broadcastInvalidate("key6", 2L)).doesNotThrowAnyException();
  }

  @Test
  void send_invalidateBatch_withPublisher_sendsOneChunk() {
    dispatcher = new CentralDispatcher(newCounter(null), publisher, broadcastBuffer, sendExecutor);
    List<SyncMessage.Entry> entries = List.of(new SyncMessage.Entry("a", 1L), new SyncMessage.Entry("b", 2L));
    dispatcher.broadcastInvalidateBatch(entries);
    verify(publisher, timeout(1000)).broadcastInvalidateBatch(entries);
  }

  @Test
  void send_invalidateAll_withPublisher_sendsOneClear() {
    dispatcher = new CentralDispatcher(newCounter(null), publisher, broadcastBuffer, sendExecutor);
    dispatcher.broadcastInvalidateAll(9L);
    verify(publisher, timeout(1000)).broadcastInvalidateAll(9L);
  }

  // ------------------------------------------------------------------
  // sendPut — value-carrying PUT broadcast through the buffer
  // ------------------------------------------------------------------

  private BroadcastBuffer bufferWithPublisher() {
    return new BroadcastBuffer(scheduler, publisher, 5000, 2_000, null);
  }

  /** A serializable value is buffered as a PUT record; the flush sends it as broadcastPut. */
  @Test
  void sendPut_serializable_buffersPutRecord() {
    BroadcastBuffer buf = bufferWithPublisher();
    dispatcher = new CentralDispatcher(newCounter(null), publisher, buf, sendExecutor);
    byte[] payload = "value-bytes".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    when(publisher.serializePutPayload("v")).thenReturn(payload);

    dispatcher.broadcastPut("key9", "v", 5L);
    buf.flush();

    verify(publisher).broadcastPut("key9", 5L, payload);
    verify(publisher, never()).broadcastRefresh(anyString(), anyLong());
  }

  /** A value that fails serialization degrades to a value-less REFRESH record in the same buffer. */
  @Test
  void sendPut_serializationFails_degradesToRefresh() {
    BroadcastBuffer buf = bufferWithPublisher();
    dispatcher = new CentralDispatcher(newCounter(null), publisher, buf, sendExecutor);
    when(publisher.serializePutPayload("v")).thenReturn(null);

    dispatcher.broadcastPut("key10", "v", 6L);
    buf.flush();

    verify(publisher).broadcastRefresh("key10", 6L);
    verify(publisher, never()).broadcastPut(anyString(), anyLong(), any());
  }

  /** Without a sync publisher the PUT is a silent no-op (same contract as the other send types). */
  @Test
  void sendPut_withoutPublisher_shouldNotFail() {
    dispatcher = new CentralDispatcher(newCounter(null), null, bufferWithPublisher(), sendExecutor);
    assertThatCode(() -> dispatcher.broadcastPut("key11", "v", 7L)).doesNotThrowAnyException();
  }
}
