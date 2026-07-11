package io.github.hyshmily.zeta.cache;

import static io.github.hyshmily.zeta.sync.local.SyncMessage.*;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

import io.github.hyshmily.zeta.cache.cachesupport.BroadcastBuffer;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CentralDispatcherTest {

  private HotKeyDetector detector;
  private KeyReporter reporter;
  private CacheSyncPublisher publisher;
  private BroadcastBuffer broadcastBuffer;
  private ScheduledExecutorService scheduler;
  private CentralDispatcher dispatcher;

  @BeforeEach
  void setUp() {
    detector = mock(HotKeyDetector.class);
    reporter = mock(KeyReporter.class);
    publisher = mock(CacheSyncPublisher.class);
    scheduler = Executors.newSingleThreadScheduledExecutor();
    broadcastBuffer = new BroadcastBuffer(scheduler, Optional.empty());
  }

  @AfterEach
  void tearDown() {
    scheduler.shutdownNow();
  }

  @Test
  void recordAccess_shouldAddToDetectorAndReport() {
    dispatcher = new CentralDispatcher(Optional.of(reporter), Optional.empty(), broadcastBuffer, detector);
    dispatcher.recordAccess("key1", false);
    verify(detector).add("key1");
    verify(reporter).recordReport("key1");
  }

  @Test
  void recordAccess_withSkipBroadcast_shouldNotReport() {
    dispatcher = new CentralDispatcher(Optional.of(reporter), Optional.empty(), broadcastBuffer, detector);
    dispatcher.recordAccess("key2", true);
    verify(detector).add("key2");
    verifyNoInteractions(reporter);
  }

  @Test
  void recordAccess_withoutReporter_shouldNotFail() {
    dispatcher = new CentralDispatcher(Optional.empty(), Optional.empty(), broadcastBuffer, detector);
    assertThatCode(() -> dispatcher.recordAccess("key3", false)).doesNotThrowAnyException();
    verify(detector).add("key3");
  }

  @Test
  void send_invalidate_withPublisher_shouldBroadcast() {
    dispatcher = new CentralDispatcher(Optional.empty(), Optional.of(publisher), broadcastBuffer, detector);
    dispatcher.send("key4", TYPE_INVALIDATE, 1L, false);
    verify(publisher).broadcastLocalInvalidate("key4", 1L, false);
  }

  @Test
  void send_invalidate_withoutPublisher_shouldNotFail() {
    dispatcher = new CentralDispatcher(Optional.empty(), Optional.empty(), broadcastBuffer, detector);
    assertThatCode(() -> dispatcher.send("key5", TYPE_INVALIDATE, 2L, true)).doesNotThrowAnyException();
  }

  @Test
  void send_refresh_withPublisher_shouldBuffer() {
    dispatcher = new CentralDispatcher(Optional.empty(), Optional.of(publisher), broadcastBuffer, detector);
    dispatcher.send("key6", TYPE_REFRESH, 3L, false);
    // REFRESH goes to BroadcastBuffer, not directly to publisher
    verifyNoInteractions(publisher);
  }

  @Test
  void send_refresh_withoutPublisher_shouldNotFail() {
    dispatcher = new CentralDispatcher(Optional.empty(), Optional.empty(), broadcastBuffer, detector);
    assertThatCode(() -> dispatcher.send("key7", TYPE_REFRESH, 4L, true)).doesNotThrowAnyException();
  }

  @Test
  void send_unknownType_shouldThrow() {
    dispatcher = new CentralDispatcher(Optional.empty(), Optional.empty(), broadcastBuffer, detector);
    assertThatThrownBy(() -> dispatcher.send("key8", "UNKNOWN", 0L, false))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("UNKNOWN");
  }

  @Test
  void sendCollection_null_shouldNotFail() {
    dispatcher = new CentralDispatcher(Optional.empty(), Optional.of(publisher), broadcastBuffer, detector);
    assertThatCode(() -> dispatcher.send(null, TYPE_INVALIDATE_ALL)).doesNotThrowAnyException();
    verifyNoInteractions(publisher);
  }

  @Test
  void sendCollection_empty_shouldNotFail() {
    dispatcher = new CentralDispatcher(Optional.empty(), Optional.of(publisher), broadcastBuffer, detector);
    assertThatCode(() -> dispatcher.send(List.of(), TYPE_INVALIDATE_ALL)).doesNotThrowAnyException();
    verifyNoInteractions(publisher);
  }

  @Test
  void sendCollection_invalidateAll_withPublisher_shouldBroadcast() {
    dispatcher = new CentralDispatcher(Optional.empty(), Optional.of(publisher), broadcastBuffer, detector);
    dispatcher.send(List.of("k1", "k2"), TYPE_INVALIDATE_ALL);
    verify(publisher).broadcastLocalInvalidateAll(List.of("k1", "k2"));
  }

  @Test
  void sendCollection_invalidateAll_withoutPublisher_shouldNotFail() {
    dispatcher = new CentralDispatcher(Optional.empty(), Optional.empty(), broadcastBuffer, detector);
    assertThatCode(() -> dispatcher.send(List.of("k1"), TYPE_INVALIDATE_ALL)).doesNotThrowAnyException();
  }
}
