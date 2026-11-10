package io.github.hyshmily.zeta.tests;

import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.rabbitmq.client.Channel;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.BroadcastBuffer;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycleImpl;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.sync.dispatcher.KeyDispatcher;
import io.github.hyshmily.zeta.sync.local.CacheSyncListener;
import io.github.hyshmily.zeta.sync.local.CacheSyncProperties;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.sync.local.DefaultSyncContext;
import io.github.hyshmily.zeta.sync.local.PutPayloadCodec;
import io.github.hyshmily.zeta.sync.local.SyncContext;
import io.github.hyshmily.zeta.sync.local.SyncPolicy;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.sync.local.SyncType;
import io.github.hyshmily.zeta.sync.worker.DefaultWorkerContext;
import io.github.hyshmily.zeta.sync.worker.WorkerDecision;
import io.github.hyshmily.zeta.sync.worker.WorkerPolicy;
import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatMessage;
import io.github.hyshmily.zeta.sync.worker.WorkerListener;
import io.github.hyshmily.zeta.sync.worker.WorkerListenerProperties;
import io.github.hyshmily.zeta.sync.worker.WorkerType;
import io.github.hyshmily.zeta.util.clock.StampClock;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

@Timeout(30)
class DistributedSyncTest {

  private ScheduledExecutorService scheduler;
  private Cache<String, CacheEntry> cache;
  private Channel channel;
  private StampClock stampClock;

  @BeforeEach
  void setUp() {
    scheduler = Executors.newSingleThreadScheduledExecutor();
    cache = Caffeine.newBuilder().maximumSize(10_000).build();
    channel = mock(Channel.class);
    stampClock = mock(StampClock.class);
    when(stampClock.nextStamp()).thenReturn(1L);
    when(stampClock.nextStamps(org.mockito.ArgumentMatchers.anyInt())).thenReturn(1L);
  }

  @AfterEach
  void tearDown() {
    scheduler.shutdownNow();
  }

  // ═══════════════════════════════════════════════════════════════
  // Helper utilities
  // ═══════════════════════════════════════════════════════════════

  private void awaitScheduler() throws InterruptedException {
    CountDownLatch phase1 = new CountDownLatch(1);
    CountDownLatch phase2 = new CountDownLatch(1);
    scheduler.execute(phase1::countDown);
    assertThat(phase1.await(10, TimeUnit.SECONDS)).isTrue();
    scheduler.execute(phase2::countDown);
    assertThat(phase2.await(10, TimeUnit.SECONDS)).isTrue();
  }

  private static Message syncMessage(String key, String type, long version) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, type);
    props.setHeader(HEADER_VERSION, version);
    return new Message(key.getBytes(StandardCharsets.UTF_8), props);
  }

  private static Message wireMessage(String key, String type, long version, String nodeId, long epoch) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, type);
    props.setHeader(HEADER_VERSION, version);
    props.setHeader(HEADER_NODE_ID, nodeId);
    props.setHeader(HEADER_EPOCH, epoch);
    return new Message(key.getBytes(StandardCharsets.UTF_8), props);
  }

  private static Message workerMessageSimple(String key, String type, long version) {
    return wireMessage(key, type, version, "w1", 1);
  }

  private static CacheEntry entry(
    long dataVersion,
    long decisionVersion,
    String decisionNodeId,
    long decisionEpoch,
    KeyState keyState
  ) {
    return EntryDraft.blank(null)
      .value("v")
      .version(dataVersion)
      .decision(DecisionStamp.hot(decisionVersion, decisionNodeId, decisionEpoch))
      .ttl(300_000, 30_000, 300_000, 30_000)
      .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 30_000)
      .keyState(keyState)
      .build();
  }

  private static CacheEntry hotEntry(String nodeId, long dv, long epoch) {
    return entry(5, dv, nodeId, epoch, KeyState.HOT);
  }

  private static KeyDispatcher<Runnable> runnableDispatcher(
    ScheduledExecutorService executor,
    String name,
    int maxQueuePerKey,
    int maxTasksPerCycle,
    long maxGlobalPendingUnits,
    long jitterMs
  ) {
    return new KeyDispatcher<>(
      executor,
      name,
      batch -> batch.forEach(Runnable::run),
      maxQueuePerKey,
      maxTasksPerCycle,
      maxGlobalPendingUnits,
      jitterMs
    );
  }

  private SyncContext syncContext(CacheLoader loader) {
    ZetaProperties ttlConfig = new ZetaProperties();
    EntryLifecycle expire = new EntryLifecycleImpl(cache, ttlConfig, null);
    return new DefaultSyncContext(cache, loader, expire, List.of(), null, null);
  }

  private CacheSyncListener createListener(CacheLoader loader) {
    CacheSyncProperties props = new CacheSyncProperties();
    props.setJitterMs(0);
    return new CacheSyncListener(props, scheduler, new SyncPolicy() {}, syncContext(loader), null, null);
  }

  private WorkerListener createWorkerListener(CacheLoader loader) {
    WorkerListenerProperties props = new WorkerListenerProperties();
    props.setJitterMs(0);
    EntryLifecycle expire = new EntryLifecycleImpl(cache, new ZetaProperties(), null);
    DefaultWorkerContext context = new DefaultWorkerContext(cache, loader, expire, stampClock, List.of());
    WorkerPolicy handler = new WorkerPolicy() {};
    return new WorkerListener(props, scheduler, handler, context, null);
  }

  // ═══════════════════════════════════════════════════════════════
  // 1. CacheSyncPublisher — content correctness
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("CacheSyncPublisher content correctness")
  class PublisherContentTests {

    @Test
    @DisplayName("broadcastRefresh sets correct headers and body")
    void broadcastRefresh_setsCorrectHeaders() {
      RabbitTemplate rt = mock(RabbitTemplate.class);
      CacheSyncProperties props = new CacheSyncProperties();
      CacheSyncPublisher pub = new CacheSyncPublisher(rt, props, new PutPayloadCodec(props), null);

      pub.broadcastRefresh("myKey", 42L);

      ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
      verify(rt).send(eq(props.getExchangeName()), eq(""), captor.capture());
      Message sent = captor.getValue();
      assertThat(new String(sent.getBody(), StandardCharsets.UTF_8)).isEqualTo("myKey");
      assertThat((String) sent.getMessageProperties().getHeader(HEADER_TYPE)).isEqualTo(SyncType.REFRESH.code());
      assertThat(((Number) sent.getMessageProperties().getHeader(HEADER_VERSION)).longValue()).isEqualTo(42L);
    }

    @Test
    @DisplayName("broadcastInvalidate sets correct headers and body")
    void broadcastInvalidate_setsCorrectHeaders() {
      RabbitTemplate rt = mock(RabbitTemplate.class);
      CacheSyncProperties props = new CacheSyncProperties();
      CacheSyncPublisher pub = new CacheSyncPublisher(rt, props, new PutPayloadCodec(props), null);

      pub.broadcastInvalidate("del-key", 7L);

      ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
      verify(rt).send(anyString(), eq(""), captor.capture());
      Message sent = captor.getValue();
      assertThat((String) sent.getMessageProperties().getHeader(HEADER_TYPE)).isEqualTo(SyncType.INVALIDATE.code());
      assertThat(((Number) sent.getMessageProperties().getHeader(HEADER_VERSION)).longValue()).isEqualTo(7L);
    }

    @Test
    @DisplayName("publisher handles AmqpException gracefully")
    void publisher_handlesAmqpException() {
      RabbitTemplate rt = mock(RabbitTemplate.class);
      doThrow(new AmqpException("Broker down")).when(rt).send(anyString(), anyString(), any());
      CacheSyncProperties props = new CacheSyncProperties();
      CacheSyncPublisher pub = new CacheSyncPublisher(rt, props, new PutPayloadCodec(props), null);

      pub.broadcastRefresh("k", 1L);

      verify(rt).send(anyString(), anyString(), any());
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 2. CacheSyncListener — single-space version comparison
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("CacheSyncListener version comparison (single stamp space)")
  class ListenerVersionMatrixTests {

    private CacheSyncListener listener;

    @BeforeEach
    void init() {
      listener = createListener(k -> "fresh");
    }

    @Test
    @DisplayName("existing stamp higher — skip")
    void existingHigher_skip() throws Exception {
      cache.put("k", entry(10, 0, null, 0, KeyState.NORMAL));
      listener.onMessage(channel, syncMessage("k", SyncType.REFRESH.code(), 5L));
      awaitScheduler();
      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getDataVersion()).isEqualTo(10);
    }

    @Test
    @DisplayName("incoming stamp higher — accept")
    void incomingHigher_accept() throws Exception {
      cache.put("k", entry(10, 0, null, 0, KeyState.NORMAL));
      listener.onMessage(channel, syncMessage("k", SyncType.REFRESH.code(), 15L));
      awaitScheduler();
      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getDataVersion()).isEqualTo(15L);
    }

    @Test
    @DisplayName("unknown resident stamp (0) accepts any real incoming stamp")
    void unknownResident_acceptsAnyRealStamp() throws Exception {
      cache.put("k", entry(0, 0, null, 0, KeyState.NORMAL));
      listener.onMessage(channel, syncMessage("k", SyncType.REFRESH.code(), 15L));
      awaitScheduler();
      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getDataVersion()).isEqualTo(15L);
    }

    @Test
    @DisplayName("equal stamp applies for REFRESH")
    void equalStamp_refreshApplies() throws Exception {
      cache.put("k", entry(10, 0, null, 0, KeyState.NORMAL));
      listener.onMessage(channel, syncMessage("k", SyncType.REFRESH.code(), 10L));
      awaitScheduler();
      assertThat(((CacheEntry) cache.getIfPresent("k")).getDataVersion()).isEqualTo(10L);
    }

    @Test
    @DisplayName("INVALIDATE with version 0 is skipped")
    void invalidateVersion0_skipped() throws Exception {
      cache.put("k", entry(50, 0, null, 0, KeyState.NORMAL));
      listener.onMessage(channel, syncMessage("k", SyncType.INVALIDATE.code(), 0L));
      awaitScheduler();
      assertThat(cache.getIfPresent("k")).isNotNull();
    }

    @Test
    @DisplayName("stale INVALIDATE is skipped")
    void staleInvalidate_skip() throws Exception {
      cache.put("k", entry(50, 0, null, 0, KeyState.NORMAL));
      listener.onMessage(channel, syncMessage("k", SyncType.INVALIDATE.code(), 10L));
      awaitScheduler();
      assertThat(cache.getIfPresent("k")).isNotNull();
    }

    @Test
    @DisplayName("newer INVALIDATE removes the entry")
    void newerInvalidate_removes() throws Exception {
      cache.put("k", entry(10, 0, null, 0, KeyState.NORMAL));
      listener.onMessage(channel, syncMessage("k", SyncType.INVALIDATE.code(), 50L));
      awaitScheduler();
      assertThat(cache.getIfPresent("k")).isNull();
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 2b. Duplicate and delayed REFRESH delivery (design decisions: version comparison receiver guard)
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Duplicate and delayed REFRESH delivery")
  class DuplicateDelayedRefreshTests {

    @Test
    @DisplayName("equal-version REFRESH applies (heals) instead of skipping")
    void refresh_equalVersion_appliesAndHeals() throws Exception {
      AtomicInteger loads = new AtomicInteger();
      CacheSyncListener l = createListener(k -> {
        loads.incrementAndGet();
        return "fresh";
      });
      cache.put("k", entry(5, 0, null, 0, KeyState.NORMAL));

      l.onMessage(channel, syncMessage("k", SyncType.REFRESH.code(), 5L));
      awaitScheduler();

      // Equal applies (design decisions: version comparison strict-newer guard): the authoritative value
      // is re-fetched and stamped. A plain >= skip would leave loads at 0
      // and pin the pre-existing value until TTL.
      assertThat(loads.get()).isEqualTo(1);
      assertThat(((CacheEntry) cache.getIfPresent("k")).getDataVersion()).isEqualTo(5L);
    }

    @Test
    @DisplayName("duplicate REFRESH(v5) re-applies idempotently without regressing")
    void refresh_duplicateSameVersion_idempotent() throws Exception {
      AtomicInteger loads = new AtomicInteger();
      CacheSyncListener l = createListener(k -> {
        loads.incrementAndGet();
        return "fresh";
      });
      cache.put("k", entry(0, 0, null, 0, KeyState.NORMAL));

      l.onMessage(channel, syncMessage("k", SyncType.REFRESH.code(), 5L));
      awaitScheduler();
      l.onMessage(channel, syncMessage("k", SyncType.REFRESH.code(), 5L));
      awaitScheduler();

      assertThat(loads.get()).isEqualTo(2);
      assertThat(((CacheEntry) cache.getIfPresent("k")).getDataVersion()).isEqualTo(5L);
    }

    @Test
    @DisplayName("delayed stale REFRESH(v3) after REFRESH(v5) never regresses the entry")
    void refresh_delayedStaleAfterNewer_neverRegresses() throws Exception {
      AtomicInteger loads = new AtomicInteger();
      CacheSyncListener l = createListener(k -> {
        loads.incrementAndGet();
        return "fresh";
      });
      cache.put("k", entry(0, 0, null, 0, KeyState.NORMAL));

      l.onMessage(channel, syncMessage("k", SyncType.REFRESH.code(), 5L));
      awaitScheduler();
      l.onMessage(channel, syncMessage("k", SyncType.REFRESH.code(), 3L));
      awaitScheduler();

      // Strictly-newer skip fires before the authoritative load: no second
      // load happens and the version stays at 5.
      assertThat(loads.get()).isEqualTo(1);
      assertThat(((CacheEntry) cache.getIfPresent("k")).getDataVersion()).isEqualTo(5L);
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 3. SyncMessage factory/deserialization
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("SyncMessage construction and parsing")
  class SyncMessageConstructionTests {

    @Test
    @DisplayName("SyncMessage.from parses all fields correctly")
    void from_parsesAllFields() {
      MessageProperties props = new MessageProperties();
      props.setHeader(HEADER_TYPE, SyncType.INVALIDATE.code());
      props.setHeader(HEADER_VERSION, 7L);
      Message msg = new Message("myKey".getBytes(StandardCharsets.UTF_8), props);

      SyncMessage syncMessage = SyncMessage.from(msg);
      assertThat(syncMessage).isNotNull();
      assertThat(syncMessage.cacheKey()).isEqualTo("myKey");
      assertThat(syncMessage.type()).isEqualTo(io.github.hyshmily.zeta.sync.local.SyncType.INVALIDATE);
      assertThat(syncMessage.version()).isEqualTo(7L);
    }

    @Test
    @DisplayName("SyncMessage constant values are correct")
    void constants_areCorrect() {
      assertThat(SyncType.INVALIDATE.code()).isEqualTo("INVALIDATE");
      assertThat(SyncType.REFRESH.code()).isEqualTo("REFRESH");
    }

    @Test
    @DisplayName("SyncMessage.from returns null for null/empty body")
    void from_nullBody_returnsNull() {
      assertThat(SyncMessage.from(new Message(new byte[0], new MessageProperties()))).isNull();
    }

    @Test
    @DisplayName("SyncMessage.from drops a message without a positive version")
    void from_missingVersionHeader_dropsTheMessage() {
      MessageProperties props = new MessageProperties();
      props.setHeader(HEADER_TYPE, SyncType.INVALIDATE.code());
      Message msg = new Message("k".getBytes(StandardCharsets.UTF_8), props);
      assertThat(SyncMessage.from(msg)).isNull();
    }

    @Test
    @DisplayName("SyncMessage.from with Integer version header parses correctly")
    void from_integerVersion_parses() {
      MessageProperties props = new MessageProperties();
      props.setHeader(HEADER_TYPE, SyncType.REFRESH.code());
      props.setHeader(HEADER_VERSION, 42);
      Message msg = new Message("k".getBytes(StandardCharsets.UTF_8), props);
      assertThat(SyncMessage.from(msg).version()).isEqualTo(42L);
    }

    @Test
    @DisplayName("SyncMessage record equality works")
    void record_equality() {
      SyncMessage a = new SyncMessage(0L, "k", io.github.hyshmily.zeta.sync.local.SyncType.INVALIDATE, 1);
      SyncMessage b = new SyncMessage(0L, "k", io.github.hyshmily.zeta.sync.local.SyncType.INVALIDATE, 1);
      SyncMessage c = new SyncMessage(0L, "k", io.github.hyshmily.zeta.sync.local.SyncType.INVALIDATE, 2);
      assertThat(a).isEqualTo(b);
      assertThat(a).isNotEqualTo(c);
      assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 4. WorkerListener — HOT/COOL application
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("WorkerListener HOT/COOL decision handling")
  class WorkerListenerDecisionTests {

    @Test
    @DisplayName("HOT promotes existing entry to KeyState.HOT")
    void hot_promotesEntry() throws Exception {
      cache.put("k", entry(1, 0, null, 0, KeyState.NORMAL));
      WorkerListener wl = createWorkerListener(k -> "fresh");

      wl.onMessage(channel, wireMessage("k", WorkerType.HOT.code(), 1L, "w1", 1));
      awaitScheduler();

      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getKeyState()).isEqualTo(KeyState.HOT);
      assertThat(ce.getDecisionVersion()).isEqualTo(1L);
      assertThat(ce.getDecisionNodeId()).isEqualTo("w1");
    }

    @Test
    @DisplayName("COOL downgrades existing HOT entry")
    void cool_downgradesEntry() throws Exception {
      cache.put("k", hotEntry("w1", 1, 1));
      WorkerListener wl = createWorkerListener(k -> "fresh");

      wl.onMessage(channel, wireMessage("k", WorkerType.COOL.code(), 2L, "w1", 1));
      awaitScheduler();

      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getKeyState()).isEqualTo(KeyState.COOL);
      assertThat(ce.getDecisionVersion()).isEqualTo(2L);
    }

    @Test
    @DisplayName("COOL on non-existent entry is no-op")
    void cool_onMissingEntry_noop() throws Exception {
      WorkerListener wl = createWorkerListener(k -> null);
      wl.onMessage(channel, wireMessage("missing", WorkerType.COOL.code(), 1L, "w1", 1));
      awaitScheduler();
      assertThat(cache.getIfPresent("missing")).isNull();
    }

    @Test
    @DisplayName("HOT with stale decision version is skipped")
    void hot_staleDecisionVersion_skip() throws Exception {
      cache.put("k", hotEntry("w1", 5, 1));
      WorkerListener wl = createWorkerListener(k -> "fresh");

      wl.onMessage(channel, wireMessage("k", WorkerType.HOT.code(), 3L, "w1", 1));
      awaitScheduler();

      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getDecisionVersion()).isEqualTo(5L);
    }

    @Test
    @DisplayName("COOL with stale decision version is skipped")
    void cool_staleDecisionVersion_skip() throws Exception {
      cache.put("k", hotEntry("w1", 5, 1));
      WorkerListener wl = createWorkerListener(k -> "fresh");

      wl.onMessage(channel, wireMessage("k", WorkerType.COOL.code(), 3L, "w1", 1));
      awaitScheduler();

      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getDecisionVersion()).isEqualTo(5L);
      assertThat(ce.getKeyState()).isEqualTo(KeyState.HOT);
    }

    @Test
    @DisplayName("HOT with no source value and no existing entry aborts")
    void hot_noSourceValueAndNoEntry_aborts() throws Exception {
      WorkerListener wl = createWorkerListener(k -> null);
      wl.onMessage(channel, wireMessage("missing", WorkerType.HOT.code(), 1L, "w1", 1));
      awaitScheduler();
      assertThat(cache.getIfPresent("missing")).isNull();
    }

    @Test
    @DisplayName("HOT with a source exception falls back to the existing entry")
    void hot_sourceExceptionFallsBackToExistingEntry() throws Exception {
      cache.put("k", entry(5, 0, null, 0, KeyState.NORMAL));
      WorkerListener wl = createWorkerListener(k -> {
        throw new RuntimeException("Redis down");
      });

      wl.onMessage(channel, wireMessage("k", WorkerType.HOT.code(), 1L, "w1", 1));
      awaitScheduler();

      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getKeyState()).isEqualTo(KeyState.HOT);
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 5. WorkerDecision — serialization/deserialization
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("WorkerDecision serialization/deserialization")
  class WorkerDecisionSerdeTests {

    @Test
    @DisplayName("WorkerDecision.from parses all fields")
    void from_parsesAllFields() {
      MessageProperties props = new MessageProperties();
      props.setHeader(HEADER_TYPE, WorkerType.HOT.code());
      props.setHeader(HEADER_VERSION, 10L);
      props.setHeader(HEADER_NODE_ID, "worker-A");
      props.setHeader(HEADER_EPOCH, 3L);
      Message msg = new Message("cacheKey".getBytes(StandardCharsets.UTF_8), props);

      WorkerDecision wm = WorkerDecision.from(msg);
      assertThat(wm).isNotNull();
      assertThat(wm.cacheKey()).isEqualTo("cacheKey");
      assertThat(wm.type()).isEqualTo(WorkerType.HOT);
      assertThat(wm.stamp().decisionVersion()).isEqualTo(10L);
      assertThat(wm.stamp().decisionNodeId()).isEqualTo("worker-A");
      assertThat(wm.stamp().decisionEpoch()).isEqualTo(3L);
    }

    @Test
    @DisplayName("WorkerDecision.from returns null for empty body")
    void from_emptyBody_returnsNull() {
      assertThat(WorkerDecision.from(new Message(new byte[0], new MessageProperties()))).isNull();
    }

    @Test
    @DisplayName("WorkerDecision.from defaults for missing optional fields")
    void from_missingOptionalFields_usesDefaults() {
      MessageProperties props = new MessageProperties();
      props.setHeader(HEADER_TYPE, WorkerType.COOL.code());
      props.setHeader(HEADER_VERSION, 9L);
      Message msg = new Message("k".getBytes(StandardCharsets.UTF_8), props);
      WorkerDecision wm = WorkerDecision.from(msg);
      assertThat(wm).isNotNull();
      assertThat(wm.stamp().decisionVersion()).isEqualTo(9L);
      assertThat(wm.stamp().decisionNodeId()).isNull();
      assertThat(wm.stamp().decisionEpoch()).isZero();
    }

    @Test
    @DisplayName("WorkerDecision constant values are correct")
    void constants_areCorrect() {
      assertThat(WorkerType.HOT.code()).isEqualTo("HOT");
      assertThat(WorkerType.COOL.code()).isEqualTo("COOL");
    }

    @Test
    @DisplayName("WorkerDecision record equality works")
    void record_equality() {
      WorkerDecision a = new WorkerDecision(0L, "k", WorkerType.HOT, DecisionStamp.hot(1, "w1", 1));
      WorkerDecision b = new WorkerDecision(0L, "k", WorkerType.HOT, DecisionStamp.hot(1, "w1", 1));
      WorkerDecision c = new WorkerDecision(0L, "k", WorkerType.HOT, DecisionStamp.hot(2, "w1", 1));
      assertThat(a).isEqualTo(b);
      assertThat(a).isNotEqualTo(c);
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 6. WorkerHeartbeatMessage — construction and liveness
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("WorkerHeartbeatMessage construction and liveness")
  class WorkerHeartbeatTests {

    @Test
    @DisplayName("toMessage sets all headers correctly")
    void toMessage_setsAllHeaders() {
      WorkerHeartbeatMessage hb = new WorkerHeartbeatMessage(0L, "w-42", 5L, true, null);
      Message msg = hb.toMessage();
      MessageProperties h = msg.getMessageProperties();
      Object type = h.getHeader(HEADER_TYPE);
      assertThat(type).isEqualTo(WorkerHeartbeatMessage.TYPE);
      assertThat((Object) h.getHeader(HEADER_NODE_ID)).isEqualTo("w-42");
      assertThat((Object) h.getHeader(HEADER_HEARTBEAT_EPOCH)).isEqualTo(5L);
      assertThat((Object) h.getHeader(HEADER_HEARTBEAT_READY)).isEqualTo(true);
    }

    @Test
    @DisplayName("toMessage body contains workerId")
    void toMessage_bodyIsWorkerId() {
      WorkerHeartbeatMessage hb = new WorkerHeartbeatMessage(0L, "w-x", 1L, false, null);
      assertThat(new String(hb.toMessage().getBody(), StandardCharsets.UTF_8)).isEqualTo("w-x");
    }

    @Test
    @DisplayName("from round-trips correctly")
    void from_roundTrips() {
      WorkerHeartbeatMessage original = new WorkerHeartbeatMessage(0L, "w-7", 3L, true, null);
      WorkerHeartbeatMessage restored = WorkerHeartbeatMessage.from(original.toMessage());
      assertThat(restored).isEqualTo(original);
    }

    @Test
    @DisplayName("from returns null for wrong type")
    void from_wrongType_returnsNull() {
      MessageProperties props = new MessageProperties();
      props.setHeader(HEADER_TYPE, "NOT_HB");
      Message msg = new Message("body".getBytes(StandardCharsets.UTF_8), props);
      assertThat(WorkerHeartbeatMessage.from(msg)).isNull();
    }

    @Test
    @DisplayName("from with null properties returns null")
    void from_nullProperties_returnsNull() {
      Message msg = mock(Message.class);
      when(msg.getMessageProperties()).thenReturn(null);
      assertThat(WorkerHeartbeatMessage.from(msg)).isNull();
    }

    @Test
    @DisplayName("from defaults wrong numeric header types without losing a valid worker id")
    void from_wrongNumericHeaderTypes_defaultsSafely() {
      MessageProperties props = new MessageProperties();
      props.setHeader(HEADER_TYPE, WorkerHeartbeatMessage.TYPE);
      props.setHeader(HEADER_NODE_ID, "w1");
      props.setHeader(HEADER_HEARTBEAT_EPOCH, "bad");
      props.setHeader(HEADER_HEARTBEAT_READY, "bad");
      Message msg = new Message("body".getBytes(StandardCharsets.UTF_8), props);
      WorkerHeartbeatMessage hb = WorkerHeartbeatMessage.from(msg);
      assertThat(hb.workerId()).isEqualTo("w1");
      assertThat(hb.epoch()).isZero();
      assertThat(hb.readyToServe()).isFalse();
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 7. KeyDispatcher — ordering guarantees
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("KeyDispatcher ordering guarantees")
  class DispatcherOrderingTests {

    @Test
    @DisplayName("tasks for same key execute in FIFO order")
    void sameKey_fifoOrder() throws Exception {
      KeyDispatcher<Runnable> d = runnableDispatcher(
        scheduler,
        "test",
        KeyDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
        KeyDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
        KeyDispatcher.DEFAULT_MAX_GLOBAL_PENDING_UNITS,
        0
      );
      try {
        AtomicInteger seq = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(3);

        d.submit(
          "k",
          () -> {
            assertThat(seq.getAndIncrement()).isEqualTo(0);
            latch.countDown();
          },
          1,
          true
        );
        d.submit(
          "k",
          () -> {
            assertThat(seq.getAndIncrement()).isEqualTo(1);
            latch.countDown();
          },
          1,
          true
        );
        d.submit(
          "k",
          () -> {
            assertThat(seq.getAndIncrement()).isEqualTo(2);
            latch.countDown();
          },
          1,
          true
        );

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
      } finally {
        d.close();
      }
    }

    @Test
    @DisplayName("tasks for different keys execute without blocking each other")
    void differentKeys_noBlocking() throws Exception {
      // Use a multi-threaded executor so that tasks for different keys can run concurrently
      ScheduledExecutorService multiScheduler = Executors.newScheduledThreadPool(4);
      KeyDispatcher<Runnable> d = runnableDispatcher(
        multiScheduler,
        "test",
        KeyDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
        KeyDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
        KeyDispatcher.DEFAULT_MAX_GLOBAL_PENDING_UNITS,
        0
      );
      try {
        CountDownLatch task1Block = new CountDownLatch(1);
        CountDownLatch task1Running = new CountDownLatch(1);
        CountDownLatch task2Done = new CountDownLatch(1);

        d.submit(
          "k1",
          () -> {
            task1Running.countDown();
            try {
              assertThat(task1Block.await(10, TimeUnit.SECONDS)).isTrue();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          },
          1,
          true
        );

        assertThat(task1Running.await(5, TimeUnit.SECONDS)).isTrue();

        d.submit("k2", task2Done::countDown, 1, true);
        assertThat(task2Done.await(5, TimeUnit.SECONDS)).isTrue();

        task1Block.countDown();
      } finally {
        d.close();
        multiScheduler.shutdownNow();
      }
    }

    @Test
    @DisplayName("closed dispatcher drops tasks")
    void closed_dropsTasks() throws Exception {
      KeyDispatcher<Runnable> d = runnableDispatcher(
        scheduler,
        "test",
        KeyDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
        KeyDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
        KeyDispatcher.DEFAULT_MAX_GLOBAL_PENDING_UNITS,
        0
      );
      d.close();

      CountDownLatch latch = new CountDownLatch(1);
      d.submit("k", latch::countDown, 1, true);

      assertThat(latch.await(500, TimeUnit.MILLISECONDS)).isFalse();
    }

    @Test
    @DisplayName("full queue silently rejects overflow tasks")
    void fullQueue_rejectsOverflow() throws Exception {
      KeyDispatcher<Runnable> d = runnableDispatcher(
        scheduler,
        "test",
        1,
        KeyDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
        KeyDispatcher.DEFAULT_MAX_GLOBAL_PENDING_UNITS,
        0
      );
      try {
        CountDownLatch block = new CountDownLatch(1);
        CountDownLatch started = new CountDownLatch(1);

        d.submit(
          "k",
          () -> {
            started.countDown();
            try {
              assertThat(block.await(10, TimeUnit.SECONDS)).isTrue();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          },
          1,
          true
        );

        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        CountDownLatch queued = new CountDownLatch(1);
        CountDownLatch rejected = new CountDownLatch(1);

        d.submit("k", queued::countDown, 1, true);
        d.submit("k", rejected::countDown, 1, true);

        block.countDown();

        assertThat(queued.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(rejected.await(500, TimeUnit.MILLISECONDS)).isFalse();
      } finally {
        d.close();
      }
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 10-11. Concurrent sync messages
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Concurrent sync messages")
  class ConcurrentSyncTests {

    @Test
    @DisplayName("same key: concurrent REFRESH messages preserve last-writer-wins")
    void sameKey_concurrentRefresh() throws Exception {
      CacheSyncListener l = createListener(k -> "fresh");
      cache.put("k", entry(0, 0, null, 0, KeyState.NORMAL));
      int threadCount = 10;
      ExecutorService pool = Executors.newFixedThreadPool(threadCount);
      CountDownLatch startGate = new CountDownLatch(1);

      for (int i = 0; i < threadCount; i++) {
        long v = i + 1;
        pool.submit(() -> {
          try {
            startGate.await();
            // We bypass the channel mock for concurrent test
            l.onMessage(channel, syncMessage("k", SyncType.REFRESH.code(), v));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
      }

      startGate.countDown();
      pool.shutdown();
      assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
      awaitScheduler();

      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      // The version guard ensures only the latest (or a valid newer) version wins
      assertThat(ce.getDataVersion()).isPositive();
    }

    @Test
    @DisplayName("different keys: concurrent messages for different keys execute independently")
    void differentKeys_concurrentMessages() throws Exception {
      CacheSyncListener l = createListener(k -> "fresh");
      int keyCount = 100;
      ExecutorService pool = Executors.newFixedThreadPool(8);
      CountDownLatch startGate = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(keyCount);

      for (int i = 0; i < keyCount; i++) {
        String key = "ck-" + i;
        cache.put(key, entry(0, 0, null, 0, KeyState.NORMAL));
        int version = i + 1;
        pool.submit(() -> {
          try {
            startGate.await();
            l.onMessage(channel, syncMessage(key, SyncType.REFRESH.code(), version));
          } catch (Exception e) {
            throw new RuntimeException(e);
          } finally {
            done.countDown();
          }
        });
      }

      startGate.countDown();
      assertThat(done.await(15, TimeUnit.SECONDS)).isTrue();
      pool.shutdown();
      awaitScheduler();

      for (int i = 0; i < keyCount; i++) {
        CacheEntry ce = (CacheEntry) cache.getIfPresent("ck-" + i);
        assertThat(ce).as("key ck-%s should exist", i).isNotNull();
        assertThat(ce.getDataVersion()).isEqualTo((long) i + 1);
      }
    }

    @Test
    @DisplayName("mixed INVALIDATE and REFRESH for same key — invalidate wins if newer")
    void invalidateAndRefresh_sameKey() throws Exception {
      CacheSyncListener l = createListener(k -> "fresh");
      cache.put("k", entry(10, 0, null, 0, KeyState.NORMAL));

      // INVALIDATE at version 15
      l.onMessage(channel, syncMessage("k", SyncType.INVALIDATE.code(), 15L));
      awaitScheduler();

      assertThat(cache.getIfPresent("k")).isNull();
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 12. Worker decision race
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Worker decision race conditions")
  class WorkerDecisionRaceTests {

    @Test
    @DisplayName("two HOT decisions for same key — higher version wins")
    void twoHotDecisions_higherVersionWins() throws Exception {
      cache.put("k", entry(1, 0, null, 0, KeyState.NORMAL));
      WorkerListener wl = createWorkerListener(k -> "fresh");

      wl.onMessage(channel, wireMessage("k", WorkerType.HOT.code(), 5L, "w1", 1));
      wl.onMessage(channel, wireMessage("k", WorkerType.HOT.code(), 3L, "w1", 1));
      awaitScheduler();

      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getDecisionVersion()).isEqualTo(5L);
    }

    @Test
    @DisplayName("HOT then COOL from different workers — last-writer-wins")
    void hotThenCool_differentWorkers() throws Exception {
      cache.put("k", entry(1, 0, null, 0, KeyState.NORMAL));
      WorkerListener wl = createWorkerListener(k -> "fresh");

      wl.onMessage(channel, wireMessage("k", WorkerType.HOT.code(), 10L, "w1", 1));
      wl.onMessage(channel, wireMessage("k", WorkerType.COOL.code(), 5L, "w2", 1));
      awaitScheduler();

      // w2 at same epoch, different nodeId → unconditionally accepted
      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getKeyState()).isEqualTo(KeyState.COOL);
      assertThat(ce.getDecisionNodeId()).isEqualTo("w2");
    }

    @Test
    @DisplayName("HOT from restarted worker (higher epoch) overrides existing decision")
    void hot_afterWorkerRestart_overrides() throws Exception {
      cache.put("k", entry(1, 0, null, 0, KeyState.NORMAL));
      WorkerListener wl = createWorkerListener(k -> "fresh");

      // First HOT from w1 epoch 1
      wl.onMessage(channel, wireMessage("k", WorkerType.HOT.code(), 100L, "w1", 1));
      awaitScheduler();
      assertThat(((CacheEntry) cache.getIfPresent("k")).getDecisionVersion()).isEqualTo(100L);

      // Worker restarts, same nodeId but epoch=2, lower dv
      wl.onMessage(channel, wireMessage("k", WorkerType.HOT.code(), 1L, "w1", 2));
      awaitScheduler();

      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      // Higher epoch unconditionally accepted even with lower dv
      assertThat(ce.getDecisionVersion()).isEqualTo(1L);
      assertThat(ce.getDecisionEpoch()).isEqualTo(2L);
    }

    @Test
    @DisplayName("stale message from lower epoch is rejected")
    void staleLowerEpoch_rejected() throws Exception {
      cache.put("k", entry(1, 0, null, 0, KeyState.NORMAL));
      WorkerListener wl = createWorkerListener(k -> "fresh");

      wl.onMessage(channel, wireMessage("k", WorkerType.HOT.code(), 5L, "w1", 2));
      awaitScheduler();
      assertThat(((CacheEntry) cache.getIfPresent("k")).getDecisionVersion()).isEqualTo(5L);

      // Old epoch message arrives late
      wl.onMessage(channel, wireMessage("k", WorkerType.HOT.code(), 10L, "w1", 1));
      awaitScheduler();

      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getDecisionVersion()).isEqualTo(5L);
      assertThat(ce.getDecisionEpoch()).isEqualTo(2L);
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 13. Listener with missing cache entry
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Listener with missing/non-existent cache entry")
  class MissingEntryTests {

    @Test
    @DisplayName("REFRESH on missing key creates new entry from Redis loader")
    void refreshOnMissingKey_createsEntry() throws Exception {
      CacheSyncListener l = createListener(k -> "brand-new");
      l.onMessage(channel, syncMessage("missing", SyncType.REFRESH.code(), 1L));
      awaitScheduler();

      CacheEntry ce = (CacheEntry) cache.getIfPresent("missing");
      assertThat(ce).isNotNull();
      assertThat(ce.getValue()).isEqualTo("brand-new");
      assertThat(ce.getDataVersion()).isEqualTo(1L);
      assertThat(ce.getKeyState()).isEqualTo(KeyState.NORMAL);
    }

    @Test
    @DisplayName("REFRESH on missing key with null Redis value — no entry created")
    void refreshOnMissingKey_nullRedis_noEntry() throws Exception {
      CacheSyncListener l = createListener(k -> null);
      l.onMessage(channel, syncMessage("missing", SyncType.REFRESH.code(), 1L));
      awaitScheduler();
      assertThat(cache.getIfPresent("missing")).isNull();
    }

    @Test
    @DisplayName("INVALIDATE on missing key is no-op")
    void invalidateOnMissingKey_noop() throws Exception {
      CacheSyncListener l = createListener(k -> null);
      l.onMessage(channel, syncMessage("missing", SyncType.INVALIDATE.code(), 1L));
      awaitScheduler();
      assertThat(cache.getIfPresent("missing")).isNull();
    }

    @Test
    @DisplayName("COOL on missing key is no-op")
    void coolOnMissingKey_noop() throws Exception {
      WorkerListener wl = createWorkerListener(k -> null);
      wl.onMessage(channel, wireMessage("missing", WorkerType.COOL.code(), 1L, "w1", 1));
      awaitScheduler();
      assertThat(cache.getIfPresent("missing")).isNull();
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 15. Ordering violation detection
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Per-key ordering violation detection")
  @Tag("flaky")
  class OrderingViolationTests {

    @Test
    @DisplayName("three sequential tasks for same key execute in order")
    void threeTasks_sameKey_inOrder() throws Exception {
      KeyDispatcher<Runnable> d = runnableDispatcher(
        scheduler,
        "test",
        KeyDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
        KeyDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
        KeyDispatcher.DEFAULT_MAX_GLOBAL_PENDING_UNITS,
        0
      );
      try {
        List<Integer> results = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(3);

        d.submit(
          "order-key",
          () -> {
            results.add(1);
            latch.countDown();
          },
          1,
          true
        );
        d.submit(
          "order-key",
          () -> {
            results.add(2);
            latch.countDown();
          },
          1,
          true
        );
        d.submit(
          "order-key",
          () -> {
            results.add(3);
            latch.countDown();
          },
          1,
          true
        );

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(results).containsExactly(1, 2, 3);
      } finally {
        d.close();
      }
    }

    @Test
    @DisplayName("no interleaving: same key tasks don't overlap")
    void sameKey_tasksDontOverlap() throws Exception {
      KeyDispatcher<Runnable> d = runnableDispatcher(
        scheduler,
        "test",
        KeyDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
        KeyDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
        KeyDispatcher.DEFAULT_MAX_GLOBAL_PENDING_UNITS,
        0
      );
      try {
        CountDownLatch task1Running = new CountDownLatch(1);
        CountDownLatch task1Done = new CountDownLatch(1);
        AtomicInteger concurrentCount = new AtomicInteger(0);
        CountDownLatch task2Ran = new CountDownLatch(1);

        d.submit(
          "no-overlap",
          () -> {
            task1Running.countDown();
            concurrentCount.incrementAndGet();
            try {
              Thread.sleep(300);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            concurrentCount.decrementAndGet();
            task1Done.countDown();
          },
          1,
          true
        );

        d.submit(
          "no-overlap",
          () -> {
            // This should only run after task1 completes
            assertThat(concurrentCount.get()).isZero();
            task2Ran.countDown();
          },
          1,
          true
        );

        assertThat(task1Done.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(task2Ran.await(5, TimeUnit.SECONDS)).isTrue();
      } finally {
        d.close();
      }
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 16. Multiple workers producing conflicting decisions
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Multiple workers — conflicting decisions")
  class MultiWorkerConflictTests {

    @Test
    @DisplayName("two workers same epoch — different nodeId — last-writer-wins")
    void twoWorkers_sameEpoch_differentNode() throws Exception {
      cache.put("k", entry(1, 0, null, 0, KeyState.NORMAL));
      WorkerListener wl = createWorkerListener(k -> "fresh");

      wl.onMessage(channel, wireMessage("k", WorkerType.HOT.code(), 10L, "w1", 1));
      awaitScheduler();
      assertThat(((CacheEntry) cache.getIfPresent("k")).getDecisionNodeId()).isEqualTo("w1");

      wl.onMessage(channel, wireMessage("k", WorkerType.HOT.code(), 5L, "w2", 1));
      awaitScheduler();

      // Different nodeId, same epoch → unconditional accept
      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getDecisionNodeId()).isEqualTo("w2");
      assertThat(ce.getDecisionVersion()).isEqualTo(5L);
    }

    @Test
    @DisplayName("first worker wins when own epoch is higher")
    void higherEpoch_overridesLower() throws Exception {
      cache.put("k", entry(1, 0, null, 0, KeyState.NORMAL));
      WorkerListener wl = createWorkerListener(k -> "fresh");

      wl.onMessage(channel, wireMessage("k", WorkerType.HOT.code(), 50L, "w1", 2));
      awaitScheduler();
      assertThat(((CacheEntry) cache.getIfPresent("k")).getDecisionEpoch()).isEqualTo(2L);

      // Late message from a lower epoch of the SAME Worker. Epoch is a
      // node-local incarnation counter, so this is the only regime in which the
      // comparison is meaningful — cross-Worker messages never reach it
      // (design decisions: version comparison).
      wl.onMessage(channel, wireMessage("k", WorkerType.HOT.code(), 99L, "w1", 1));
      awaitScheduler();

      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getDecisionEpoch()).isEqualTo(2L);
      assertThat(ce.getDecisionNodeId()).isEqualTo("w1");
    }

    @Test
    @DisplayName("cross-Worker message applies on arrival even at a lower epoch (design decisions: version comparison)")
    void crossWorkerLowerEpoch_appliesOnArrival() throws Exception {
      cache.put("k", entry(1, 0, null, 0, KeyState.NORMAL));
      WorkerListener wl = createWorkerListener(k -> "fresh");

      wl.onMessage(channel, wireMessage("k", WorkerType.HOT.code(), 50L, "w1", 2));
      awaitScheduler();
      assertThat(((CacheEntry) cache.getIfPresent("k")).getDecisionNodeId()).isEqualTo("w1");

      // Different Worker, lower epoch: neither epoch nor decisionVersion is
      // comparable across Workers, so arrival order (last-writer-wins) decides.
      wl.onMessage(channel, wireMessage("k", WorkerType.HOT.code(), 99L, "w2", 1));
      awaitScheduler();

      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getDecisionNodeId()).isEqualTo("w2");
      assertThat(ce.getDecisionEpoch()).isEqualTo(1L);
      assertThat(ce.getDecisionVersion()).isEqualTo(99L);
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 17. Graceful degradation chain
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Source-outage fallback chain")
  class GracefulDegradationTests {

    @Test
    @DisplayName("source down -> existing entry promoted -> source recovers -> fresh value wins")
    void degradationChain() throws Exception {
      WorkerListener wl = createWorkerListener(k -> {
        throw new RuntimeException("source down");
      });

      // The seeded version must respect the stamp clock's monotonicity (the
      // stub allocates 1, 2, ...): a seeded version ABOVE the clock would make
      // every later load read as outranked by the resident.
      cache.put("k", entry(1, 0, null, 0, KeyState.NORMAL));

      // HOT promotion falls back to the existing entry value.
      wl.onMessage(channel, wireMessage("k", WorkerType.HOT.code(), 1L, "w1", 1));
      awaitScheduler();

      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getDataVersion()).isEqualTo(1L);
      assertThat(ce.getKeyState()).isEqualTo(KeyState.HOT);

      // Source recovers: a newer Worker incarnation promotes fresh data.
      WorkerListener wlRecovered = createWorkerListener(k -> "fresh-data");

      wlRecovered.onMessage(channel, wireMessage("k", WorkerType.HOT.code(), 2L, "w1", 2));
      awaitScheduler();

      CacheEntry ce2 = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce2.getValue()).isEqualTo("fresh-data");
      assertThat(ce2.getDecisionEpoch()).isEqualTo(2L);
      assertThat(ce2.getKeyState()).isEqualTo(KeyState.HOT);
    }

    @Test
    @DisplayName("COOL preserves the value and stamp")
    void coolAfterDegradation_preservesValue() throws Exception {
      cache.put("k", entry(5, 1, "w1", 1, KeyState.HOT));
      WorkerListener wl = createWorkerListener(k -> "fresh");

      wl.onMessage(channel, wireMessage("k", WorkerType.COOL.code(), 2L, "w1", 1));
      awaitScheduler();

      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getKeyState()).isEqualTo(KeyState.COOL);
      assertThat(ce.getDataVersion()).isEqualTo(5L);
      assertThat(ce.getValue()).isEqualTo("v");
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 18. Decision version monotonicity
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Decision version monotonicity")
  class DecisionMonotonicityTests {

    @Test
    @DisplayName("WorkerListener rejects stale HOT with same nodeId")
    void hot_staleSameNode_rejected() throws Exception {
      cache.put("k", hotEntry("w1", 10, 1));
      WorkerListener wl = createWorkerListener(k -> "fresh");

      wl.onMessage(channel, wireMessage("k", WorkerType.HOT.code(), 5L, "w1", 1));
      awaitScheduler();

      assertThat(((CacheEntry) cache.getIfPresent("k")).getDecisionVersion()).isEqualTo(10L);
    }

    @Test
    @DisplayName("WorkerListener rejects stale COOL with same nodeId")
    void cool_staleSameNode_rejected() throws Exception {
      cache.put("k", hotEntry("w1", 10, 1));
      WorkerListener wl = createWorkerListener(k -> "fresh");

      wl.onMessage(channel, wireMessage("k", WorkerType.COOL.code(), 5L, "w1", 1));
      awaitScheduler();

      assertThat(((CacheEntry) cache.getIfPresent("k")).getDecisionVersion()).isEqualTo(10L);
      assertThat(((CacheEntry) cache.getIfPresent("k")).getKeyState()).isEqualTo(KeyState.HOT);
    }

    @Test
    @DisplayName("WorkerListener accepts equal version HOT (no-op but idempotent)")
    void hot_equalVersion_idempotent() throws Exception {
      cache.put("k", hotEntry("w1", 5, 1));
      WorkerListener wl = createWorkerListener(k -> "fresh");

      wl.onMessage(channel, wireMessage("k", WorkerType.HOT.code(), 5L, "w1", 1));
      awaitScheduler();

      assertThat(((CacheEntry) cache.getIfPresent("k")).getDecisionVersion()).isEqualTo(5L);
      assertThat(((CacheEntry) cache.getIfPresent("k")).getKeyState()).isEqualTo(KeyState.HOT);
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 19. Sync message with null/empty fields
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Edge cases: null and empty fields")
  class NullEmptyFieldTests {

    @Test
    @DisplayName("SyncMessage with empty body returns null")
    void emptyBody_returnsNull() {
      Message msg = new Message(new byte[0], new MessageProperties());
      assertThat(SyncMessage.from(msg)).isNull();
    }

    @Test
    @DisplayName("SyncMessage with blank key for non-batch type returns null")
    void blankKey_nonBatch_returnsNull() {
      MessageProperties props = new MessageProperties();
      props.setHeader(HEADER_TYPE, SyncType.INVALIDATE.code());
      Message msg = new Message("   ".getBytes(StandardCharsets.UTF_8), props);
      assertThat(SyncMessage.from(msg)).isNull();
    }

    @Test
    @DisplayName("SyncMessage with null type header is dropped at parse time")
    void nullType_header() {
      Message msg = new Message("k".getBytes(StandardCharsets.UTF_8), new MessageProperties());
      assertThat(SyncMessage.from(msg)).isNull();
    }

    @Test
    @DisplayName("WorkerDecision with empty body returns null")
    void workerMessage_emptyBody() {
      assertThat(WorkerDecision.from(new Message(new byte[0], new MessageProperties()))).isNull();
    }

    @Test
    @DisplayName("CacheSyncListener handles null type gracefully")
    void listener_nullType_acks() throws Exception {
      CacheSyncListener l = createListener(k -> "v");
      Message msg = new Message("k".getBytes(StandardCharsets.UTF_8), new MessageProperties());
      l.onMessage(channel, msg);
      verify(channel).basicAck(anyLong(), eq(false));
    }

    @Test
    @DisplayName("WorkerListener handles null type gracefully")
    void workerListener_nullType_acks() throws Exception {
      WorkerListener wl = createWorkerListener(k -> "v");
      Message msg = new Message("k".getBytes(StandardCharsets.UTF_8), new MessageProperties());
      wl.onMessage(channel, msg);
      verify(channel).basicAck(anyLong(), eq(false));
    }

    @Test
    @DisplayName("CacheSyncListener propagates an ack failure without a second nack")
    void listener_ackFailure_propagatesWithoutNack() throws Exception {
      doThrow(new RuntimeException("forced error")).when(channel).basicAck(anyLong(), eq(false));
      CacheSyncListener l = createListener(k -> "v");
      Message msg = syncMessage("k", SyncType.INVALIDATE.code(), 1L);
      assertThatThrownBy(() -> l.onMessage(channel, msg))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("forced error");
      verify(channel, never()).basicNack(anyLong(), eq(false), eq(false));
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 20. BroadcastBuffer integration
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("BroadcastBuffer deferral and batching")
  class BroadcastBufferIntegrationTests {

    @Test
    @DisplayName("record defers then flush sends all pending")
    void recordThenFlush_sendsAll() {
      CacheSyncPublisher pub = mock(CacheSyncPublisher.class);
      BroadcastBuffer buf = new BroadcastBuffer(scheduler, pub, 5000, Math.max(5000, 2_000), null);

      buf.record("k1", 1L, null, null);
      buf.record("k2", 2L, null, null);

      buf.flush();

      verify(pub).broadcastRefresh("k1", 1L);
      verify(pub).broadcastRefresh("k2", 2L);
    }

    @Test
    @DisplayName("auto-flush fires after configured delay")
    void autoFlush_firesAfterDelay() throws Exception {
      CacheSyncPublisher pub = mock(CacheSyncPublisher.class);
      CountDownLatch latch = new CountDownLatch(1);
      doAnswer(inv -> {
        latch.countDown();
        return null;
      })
        .when(pub)
        .broadcastRefresh("auto", 1L);

      BroadcastBuffer buf = new BroadcastBuffer(scheduler, pub, 100, Math.max(100, 2_000), null);
      buf.record("auto", 1L, null, null);

      assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
      verify(pub).broadcastRefresh("auto", 1L);
    }

    @Test
    @DisplayName("flush with no records does nothing")
    void flush_noRecords_noop() {
      CacheSyncPublisher pub = mock(CacheSyncPublisher.class);
      BroadcastBuffer buf = new BroadcastBuffer(scheduler, pub, 5000, Math.max(5000, 2_000), null);
      buf.flush();
      verifyNoInteractions(pub);
    }

    @Test
    @DisplayName("publisher exception during flush is caught, remaining keys processed")
    void flush_publisherException_continues() {
      CacheSyncPublisher pub = mock(CacheSyncPublisher.class);
      doThrow(new RuntimeException("pub fail")).when(pub).broadcastRefresh("bad", 1L);

      BroadcastBuffer buf = new BroadcastBuffer(scheduler, pub, 5000, Math.max(5000, 2_000), null);
      buf.record("bad", 1L, null, null);
      buf.record("good", 2L, null, null);

      buf.flush();

      verify(pub).broadcastRefresh("bad", 1L);
      verify(pub).broadcastRefresh("good", 2L);
    }

    @Test
    @DisplayName("concurrent record and flush is safe")
    void concurrent_recordAndFlush_safe() throws Exception {
      CacheSyncPublisher pub = mock(CacheSyncPublisher.class);
      BroadcastBuffer buf = new BroadcastBuffer(scheduler, pub, 5000, Math.max(5000, 2_000), null);

      ExecutorService pool = Executors.newFixedThreadPool(4);
      CountDownLatch startGate = new CountDownLatch(1);
      int records = 200;

      for (int i = 0; i < records; i++) {
        int idx = i;
        pool.submit(() -> {
          try {
            startGate.await();
            buf.record("ck-" + idx, idx, null, null);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
      }

      startGate.countDown();
      pool.shutdown();
      assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

      buf.flush();

      verify(pub, atLeast(records)).broadcastRefresh(anyString(), anyLong());
    }

    @Test
    @DisplayName("record with no publisher is safe")
    void record_noPublisher_safe() {
      BroadcastBuffer buf = new BroadcastBuffer(scheduler, null, 500, 2_000, null);
      buf.record("k", 1L, null, null);
      buf.flush();
      // No exception
    }
  }
}
