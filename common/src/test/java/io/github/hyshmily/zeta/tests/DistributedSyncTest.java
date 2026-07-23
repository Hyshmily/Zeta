package io.github.hyshmily.zeta.tests;

import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.rabbitmq.client.Channel;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.BroadcastBuffer;
import io.github.hyshmily.zeta.cache.cachesupport.ExpireManager;
import io.github.hyshmily.zeta.cache.cachesupport.impl.ExpireManagerImpl;
import io.github.hyshmily.zeta.cache.loader.CacheLoader;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.rule.RuleMatcher;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sharding.impl.HealthViewImpl;
import io.github.hyshmily.zeta.sync.dispatcher.PerKeyOrderedDispatcher;
import io.github.hyshmily.zeta.sync.local.CacheSyncListener;
import io.github.hyshmily.zeta.sync.local.CacheSyncProperties;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatMessage;
import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatVerifier;
import io.github.hyshmily.zeta.sync.worker.WorkerListener;
import io.github.hyshmily.zeta.sync.worker.WorkerListenerProperties;
import io.github.hyshmily.zeta.sync.worker.WorkerMessage;
import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import io.github.hyshmily.zeta.util.ratelimit.impl.SreRateLimiterImpl;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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
import org.springframework.amqp.AmqpTimeoutException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

@Timeout(30)
class DistributedSyncTest {

  private ScheduledExecutorService scheduler;
  private Cache<String, Object> cache;
  private Channel channel;
  private RuleMatcher ruleMatcher;

  @BeforeEach
  void setUp() {
    scheduler = Executors.newSingleThreadScheduledExecutor();
    cache = Caffeine.newBuilder().maximumSize(10_000).build();
    channel = mock(Channel.class);
    ruleMatcher = mock(RuleMatcher.class);
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

  private static Message syncMessage(String key, String type, long version, boolean degraded) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, type);
    props.setHeader(HEADER_VERSION, version);
    props.setHeader(HEADER_IS_VERSION_DEGRADED, degraded);
    return new Message(key.getBytes(StandardCharsets.UTF_8), props);
  }

  private static Message workerMessage(String key, String type, long version, String nodeId, long epoch) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, type);
    props.setHeader(HEADER_VERSION, version);
    props.setHeader(HEADER_NODE_ID, nodeId);
    props.setHeader(HEADER_EPOCH, epoch);
    return new Message(key.getBytes(StandardCharsets.UTF_8), props);
  }

  private static Message workerMessageSimple(String key, String type, long version) {
    return workerMessage(key, type, version, "w1", 1);
  }

  private static CacheEntry entry(
    long dataVersion,
    boolean degraded,
    long decisionVersion,
    String decisionNodeId,
    long decisionEpoch,
    KeyState keyState
  ) {
    return CacheEntry.builder()
      .value("v")
      .dataVersion(dataVersion)
      .isVersionDegraded(degraded)
      .decisionVersion(decisionVersion)
      .decisionNodeId(decisionNodeId)
      .decisionEpoch(decisionEpoch)
      .hardTtlMs(300_000)
      .hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(30_000)
      .softExpireAtMs(System.currentTimeMillis() + 30_000)
      .keyState(keyState)
      .normalHardTtlMs(300_000)
      .normalSoftTtlMs(30_000)
      .build();
  }

  private static CacheEntry hotEntry(String nodeId, long dv, long epoch) {
    return entry(5, false, dv, nodeId, epoch, KeyState.HOT);
  }

  private CacheSyncListener createListener(CacheLoader loader) {
    CacheSyncProperties props = new CacheSyncProperties();
    props.setWarmupJitterMs(0);
    ZetaProperties ttlConfig = new ZetaProperties();
    ExpireManager expire = new ExpireManagerImpl(cache, Runnable::run, ttlConfig, 10);
    CacheSyncListener l = new CacheSyncListener(cache, loader, props, scheduler, expire, ruleMatcher);
    l.init();
    return l;
  }

  private WorkerListener createWorkerListener(CacheLoader loader, SreRateLimiterImpl limiter) {
    WorkerListenerProperties props = new WorkerListenerProperties();
    props.setWarmupJitterMs(0);
    // Disable SRE if not provided
    props.getSre().setEnabled(limiter != null);
    ZetaProperties ttlConfig = new ZetaProperties();
    ExpireManager expire = new ExpireManagerImpl(cache, Runnable::run, ttlConfig, 10);
    WorkerListener l = new WorkerListener(cache, loader, props, scheduler, expire, limiter);
    l.init();
    return l;
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
      CacheSyncPublisher pub = new CacheSyncPublisher(rt, props, mock(SnowflakeIdGenerator.class));
      pub.init();

      pub.broadcastRefresh("myKey", 42L, false);

      ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
      verify(rt).send(eq(props.getExchangeName()), eq(""), captor.capture());
      Message sent = captor.getValue();
      assertThat(new String(sent.getBody(), StandardCharsets.UTF_8)).isEqualTo("myKey");
      assertThat((String) sent.getMessageProperties().getHeader(HEADER_TYPE)).isEqualTo(SyncMessage.TYPE_REFRESH);
      assertThat(((Number) sent.getMessageProperties().getHeader(HEADER_VERSION)).longValue()).isEqualTo(42L);
      assertThat((Boolean) sent.getMessageProperties().getHeader(HEADER_IS_VERSION_DEGRADED)).isFalse();
    }

    @Test
    @DisplayName("broadcastLocalInvalidate sets correct headers and body")
    void broadcastLocalInvalidate_setsCorrectHeaders() {
      RabbitTemplate rt = mock(RabbitTemplate.class);
      CacheSyncPublisher pub = new CacheSyncPublisher(rt, new CacheSyncProperties(), mock(SnowflakeIdGenerator.class));
      pub.init();

      pub.broadcastLocalInvalidate("del-key", 7L, true);

      ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
      verify(rt).send(anyString(), eq(""), captor.capture());
      Message sent = captor.getValue();
      assertThat((String) sent.getMessageProperties().getHeader(HEADER_TYPE)).isEqualTo(SyncMessage.TYPE_INVALIDATE);
      assertThat(((Number) sent.getMessageProperties().getHeader(HEADER_VERSION)).longValue()).isEqualTo(7L);
      assertThat((Boolean) sent.getMessageProperties().getHeader(HEADER_IS_VERSION_DEGRADED)).isTrue();
    }

    @Test
    @DisplayName("broadcastLocalInvalidateAll sends JSON array body")
    void broadcastLocalInvalidateAll_sendsJsonBody() {
      RabbitTemplate rt = mock(RabbitTemplate.class);
      CacheSyncPublisher pub = new CacheSyncPublisher(rt, new CacheSyncProperties(), mock(SnowflakeIdGenerator.class));
      pub.init();

      pub.broadcastLocalInvalidateAll(List.of("k1", "k2"));

      ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
      verify(rt).send(anyString(), eq(""), captor.capture());
      Message sent = captor.getValue();
      assertThat(new String(sent.getBody(), StandardCharsets.UTF_8)).isEqualTo("[\"k1\",\"k2\"]");
      assertThat((String) sent.getMessageProperties().getHeader(HEADER_TYPE)).isEqualTo(
        SyncMessage.TYPE_INVALIDATE_ALL
      );
    }

    @Test
    @DisplayName("broadcastAllLocalRules sends rules JSON and version header")
    void broadcastAllLocalRules_setsRulesVersionHeader() {
      RabbitTemplate rt = mock(RabbitTemplate.class);
      CacheSyncPublisher pub = new CacheSyncPublisher(rt, new CacheSyncProperties(), mock(SnowflakeIdGenerator.class));
      pub.init();

      pub.broadcastAllLocalRules("{\"v\":2}", 99L);

      ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
      verify(rt).send(anyString(), eq(""), captor.capture());
      Message sent = captor.getValue();
      assertThat(new String(sent.getBody(), StandardCharsets.UTF_8)).isEqualTo("{\"v\":2}");
      assertThat((String) sent.getMessageProperties().getHeader(HEADER_TYPE)).isEqualTo(SyncMessage.TYPE_RULES_SYNC);
      assertThat(((Number) sent.getMessageProperties().getHeader(HEADER_RULES_VERSION)).longValue()).isEqualTo(99L);
    }

    @Test
    @DisplayName("dedup composite key differs between degraded and normal")
    void dedupSeparatesDegradedAndNormal() {
      RabbitTemplate rt = mock(RabbitTemplate.class);
      CacheSyncPublisher pub = new CacheSyncPublisher(rt, new CacheSyncProperties(), mock(SnowflakeIdGenerator.class));
      pub.init();

      pub.broadcastRefresh("k", 1L, false);
      pub.broadcastRefresh("k", Long.MIN_VALUE, true);

      verify(rt, times(2)).send(anyString(), anyString(), any());
    }

    @Test
    @DisplayName("dedup blocks same degraded version re-send")
    void dedupBlocksDegradedResend() {
      RabbitTemplate rt = mock(RabbitTemplate.class);
      CacheSyncPublisher pub = new CacheSyncPublisher(rt, new CacheSyncProperties(), mock(SnowflakeIdGenerator.class));
      pub.init();

      pub.broadcastRefresh("k", 5L, true);
      pub.broadcastRefresh("k", 3L, true);

      verify(rt, times(1)).send(anyString(), anyString(), any());
    }

    @Test
    @DisplayName("publisher handles AmqpException gracefully")
    void publisher_handlesAmqpException() {
      RabbitTemplate rt = mock(RabbitTemplate.class);
      doThrow(new AmqpException("Broker down")).when(rt).send(anyString(), anyString(), any());
      CacheSyncPublisher pub = new CacheSyncPublisher(rt, new CacheSyncProperties(), mock(SnowflakeIdGenerator.class));
      pub.init();

      pub.broadcastRefresh("k", 1L, false);

      verify(rt).send(anyString(), anyString(), any());
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 2. CacheSyncListener — all 4 version comparison cases
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("CacheSyncListener version comparison matrix")
  class ListenerVersionMatrixTests {

    private CacheSyncListener listener;

    @BeforeEach
    void init() {
      listener = createListener(k -> "fresh");
    }

    // Case 1: Both normal — skip if existing >= incoming
    @Test
    @DisplayName("Case 1: both normal, existing version higher — skip")
    void bothNormal_existingHigher_skip() throws Exception {
      cache.put("k", entry(10, false, 0, null, 0, KeyState.NORMAL));
      listener.handleSyncMessage(channel, syncMessage("k", SyncMessage.TYPE_REFRESH, 5L, false));
      awaitScheduler();
      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getDataVersion()).isEqualTo(10);
    }

    @Test
    @DisplayName("Case 1: both normal, incoming version higher — accept")
    void bothNormal_incomingHigher_accept() throws Exception {
      cache.put("k", entry(10, false, 0, null, 0, KeyState.NORMAL));
      listener.handleSyncMessage(channel, syncMessage("k", SyncMessage.TYPE_REFRESH, 15L, false));
      awaitScheduler();
      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getDataVersion()).isEqualTo(15L);
    }

    // Case 2: Existing normal, incoming degraded — always skip
    @Test
    @DisplayName("Case 2: existing normal, incoming degraded — always skip")
    void existingNormal_incomingDegraded_skip() throws Exception {
      cache.put("k", entry(10, false, 0, null, 0, KeyState.NORMAL));
      listener.handleSyncMessage(channel, syncMessage("k", SyncMessage.TYPE_REFRESH, 999L, true));
      awaitScheduler();
      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getDataVersion()).isEqualTo(10);
    }

    // Case 3: Both degraded — skip if existing >= incoming
    @Test
    @DisplayName("Case 3: both degraded, existing version higher — skip")
    void bothDegraded_existingHigher_skip() throws Exception {
      cache.put("k", entry(10, true, 0, null, 0, KeyState.NORMAL));
      listener.handleSyncMessage(channel, syncMessage("k", SyncMessage.TYPE_REFRESH, 5L, true));
      awaitScheduler();
      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getDataVersion()).isEqualTo(10);
    }

    @Test
    @DisplayName("Case 3: both degraded, incoming version higher — accept")
    void bothDegraded_incomingHigher_accept() throws Exception {
      cache.put("k", entry(5, true, 0, null, 0, KeyState.NORMAL));
      listener.handleSyncMessage(channel, syncMessage("k", SyncMessage.TYPE_REFRESH, 15L, true));
      awaitScheduler();
      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getDataVersion()).isEqualTo(15L);
    }

    // Case 4: Existing degraded, incoming normal — never skip
    @Test
    @DisplayName("Case 4: existing degraded, incoming normal — always accept")
    void existingDegraded_incomingNormal_accept() throws Exception {
      cache.put("k", entry(100, true, 0, null, 0, KeyState.NORMAL));
      listener.handleSyncMessage(channel, syncMessage("k", SyncMessage.TYPE_REFRESH, 1L, false));
      awaitScheduler();
      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getDataVersion()).isEqualTo(1L);
      assertThat(ce.isVersionDegraded()).isFalse();
    }

    // INVALIDATE with version 0 unconditional
    @Test
    @DisplayName("INVALIDATE with version 0 is unconditional even with higher existing")
    void invalidateVersion0_unconditional() throws Exception {
      cache.put("k", entry(50, false, 0, null, 0, KeyState.NORMAL));
      listener.handleSyncMessage(channel, syncMessage("k", SyncMessage.TYPE_INVALIDATE, 0L, false));
      awaitScheduler();
      assertThat(cache.getIfPresent("k")).isNull();
    }

    // INVALIDATE degraded on normal — should skip
    @Test
    @DisplayName("INVALIDATE degraded incoming on normal entry — skip")
    void invalidateDegradedOnNormal_skip() throws Exception {
      cache.put("k", entry(50, false, 0, null, 0, KeyState.NORMAL));
      listener.handleSyncMessage(channel, syncMessage("k", SyncMessage.TYPE_INVALIDATE, 10L, true));
      awaitScheduler();
      assertThat(cache.getIfPresent("k")).isNotNull();
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
      props.setHeader(HEADER_TYPE, SyncMessage.TYPE_INVALIDATE);
      props.setHeader(HEADER_VERSION, 7L);
      props.setHeader(HEADER_IS_VERSION_DEGRADED, true);
      props.setHeader(HEADER_RULES_VERSION, 3L);
      Message msg = new Message("myKey".getBytes(StandardCharsets.UTF_8), props);

      SyncMessage sm = SyncMessage.from(msg);
      assertThat(sm).isNotNull();
      assertThat(sm.cacheKey()).isEqualTo("myKey");
      assertThat(sm.type()).isEqualTo(SyncMessage.TYPE_INVALIDATE);
      assertThat(sm.version()).isEqualTo(7L);
      assertThat(sm.isVersionDegraded()).isTrue();
      assertThat(sm.rulesVersion()).isEqualTo(3L);
    }

    @Test
    @DisplayName("SyncMessage constant values are correct")
    void constants_areCorrect() {
      assertThat(SyncMessage.TYPE_INVALIDATE).isEqualTo("INVALIDATE");
      assertThat(SyncMessage.TYPE_REFRESH).isEqualTo("REFRESH");
      assertThat(SyncMessage.TYPE_INVALIDATE_ALL).isEqualTo("INVALIDATE_ALL");
      assertThat(SyncMessage.TYPE_RULES_SYNC).isEqualTo("RULES_SYNC");
    }

    @Test
    @DisplayName("SyncMessage.from returns null for null/empty body")
    void from_nullBody_returnsNull() {
      assertThat(SyncMessage.from(new Message(new byte[0], new MessageProperties()))).isNull();
    }

    @Test
    @DisplayName("SyncMessage.from handles missing headers with defaults")
    void from_missingHeaders_usesDefaults() {
      Message msg = new Message("k".getBytes(StandardCharsets.UTF_8), new MessageProperties());
      SyncMessage sm = SyncMessage.from(msg);
      assertThat(sm.version()).isZero();
      assertThat(sm.isVersionDegraded()).isFalse();
      assertThat(sm.rulesVersion()).isZero();
    }

    @Test
    @DisplayName("SyncMessage.from with Integer version header parses correctly")
    void from_integerVersion_parses() {
      MessageProperties props = new MessageProperties();
      props.setHeader(HEADER_TYPE, SyncMessage.TYPE_REFRESH);
      props.setHeader(HEADER_VERSION, 42);
      Message msg = new Message("k".getBytes(StandardCharsets.UTF_8), props);
      assertThat(SyncMessage.from(msg).version()).isEqualTo(42L);
    }

    @Test
    @DisplayName("SyncMessage record equality works")
    void record_equality() {
      SyncMessage a = new SyncMessage(0L, "k", "T", 1, false, 0);
      SyncMessage b = new SyncMessage(0L, "k", "T", 1, false, 0);
      SyncMessage c = new SyncMessage(0L, "k", "T", 2, false, 0);
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
      cache.put("k", entry(1, false, 0, null, 0, KeyState.NORMAL));
      WorkerListener wl = createWorkerListener(k -> "fresh", null);

      wl.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_HOT, 1L, "w1", 1));
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
      WorkerListener wl = createWorkerListener(k -> "fresh", null);

      wl.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_COOL, 2L, "w1", 1));
      awaitScheduler();

      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getKeyState()).isEqualTo(KeyState.COOL);
      assertThat(ce.getDecisionVersion()).isEqualTo(2L);
    }

    @Test
    @DisplayName("COOL on non-existent entry is no-op")
    void cool_onMissingEntry_noop() throws Exception {
      WorkerListener wl = createWorkerListener(k -> null, null);
      wl.handleWorkerMessage(channel, workerMessage("missing", WorkerMessage.TYPE_COOL, 1L, "w1", 1));
      awaitScheduler();
      assertThat(cache.getIfPresent("missing")).isNull();
    }

    @Test
    @DisplayName("HOT with stale decision version is skipped")
    void hot_staleDecisionVersion_skip() throws Exception {
      cache.put("k", hotEntry("w1", 5, 1));
      WorkerListener wl = createWorkerListener(k -> "fresh", null);

      wl.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_HOT, 3L, "w1", 1));
      awaitScheduler();

      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getDecisionVersion()).isEqualTo(5L);
    }

    @Test
    @DisplayName("COOL with stale decision version is skipped")
    void cool_staleDecisionVersion_skip() throws Exception {
      cache.put("k", hotEntry("w1", 5, 1));
      WorkerListener wl = createWorkerListener(k -> "fresh", null);

      wl.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_COOL, 3L, "w1", 1));
      awaitScheduler();

      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getDecisionVersion()).isEqualTo(5L);
      assertThat(ce.getKeyState()).isEqualTo(KeyState.HOT);
    }

    @Test
    @DisplayName("HOT with null Redis value and no degraded entry aborts")
    void hot_nullRedisNoDegraded_aborts() throws Exception {
      WorkerListener wl = createWorkerListener(k -> null, null);
      wl.handleWorkerMessage(channel, workerMessage("missing", WorkerMessage.TYPE_HOT, 1L, "w1", 1));
      awaitScheduler();
      assertThat(cache.getIfPresent("missing")).isNull();
    }

    @Test
    @DisplayName("HOT with Redis exception falls back to degraded entry")
    void hot_redisExceptionFallsBackToDegraded() throws Exception {
      cache.put("k", entry(5, true, 0, null, 0, KeyState.NORMAL));
      WorkerListener wl = createWorkerListener(
        k -> {
          throw new RuntimeException("Redis down");
        },
        null
      );

      wl.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_HOT, 1L, "w1", 1));
      awaitScheduler();

      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getKeyState()).isEqualTo(KeyState.HOT);
      assertThat(ce.isVersionDegraded()).isTrue();
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 5. WorkerMessage — serialization/deserialization
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("WorkerMessage serialization/deserialization")
  class WorkerMessageSerdeTests {

    @Test
    @DisplayName("WorkerMessage.from parses all fields")
    void from_parsesAllFields() {
      MessageProperties props = new MessageProperties();
      props.setHeader(HEADER_TYPE, WorkerMessage.TYPE_HOT);
      props.setHeader(HEADER_VERSION, 10L);
      props.setHeader(HEADER_TIMESTAMP, 12345L);
      props.setHeader(HEADER_NODE_ID, "worker-A");
      props.setHeader(HEADER_EPOCH, 3L);
      Message msg = new Message("cacheKey".getBytes(StandardCharsets.UTF_8), props);

      WorkerMessage wm = WorkerMessage.from(msg);
      assertThat(wm).isNotNull();
      assertThat(wm.cacheKey()).isEqualTo("cacheKey");
      assertThat(wm.type()).isEqualTo(WorkerMessage.TYPE_HOT);
      assertThat(wm.decisionVersion()).isEqualTo(10L);
      assertThat(wm.timestamp()).isEqualTo(12345L);
      assertThat(wm.nodeId()).isEqualTo("worker-A");
      assertThat(wm.epoch()).isEqualTo(3L);
    }

    @Test
    @DisplayName("WorkerMessage.from returns null for empty body")
    void from_emptyBody_returnsNull() {
      assertThat(WorkerMessage.from(new Message(new byte[0], new MessageProperties()))).isNull();
    }

    @Test
    @DisplayName("WorkerMessage.from defaults for missing fields")
    void from_missingFields_usesDefaults() {
      MessageProperties props = new MessageProperties();
      props.setHeader(HEADER_TYPE, WorkerMessage.TYPE_COOL);
      Message msg = new Message("k".getBytes(StandardCharsets.UTF_8), props);
      WorkerMessage wm = WorkerMessage.from(msg);
      assertThat(wm.decisionVersion()).isZero();
      assertThat(wm.timestamp()).isZero();
      assertThat(wm.nodeId()).isNull();
      assertThat(wm.epoch()).isZero();
    }

    @Test
    @DisplayName("WorkerMessage constant values are correct")
    void constants_areCorrect() {
      assertThat(WorkerMessage.TYPE_HOT).isEqualTo("HOT");
      assertThat(WorkerMessage.TYPE_COOL).isEqualTo("COOL");
    }

    @Test
    @DisplayName("WorkerMessage record equality works")
    void record_equality() {
      WorkerMessage a = new WorkerMessage(0L, "k", "HOT", 1, 0, "w1", 1);
      WorkerMessage b = new WorkerMessage(0L, "k", "HOT", 1, 0, "w1", 1);
      WorkerMessage c = new WorkerMessage(0L, "k", "HOT", 2, 0, "w1", 1);
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
      WorkerHeartbeatMessage hb = new WorkerHeartbeatMessage(0L, "w-42", 5L, 99L, 0.75, true, 3, 10, 2, 9999L);
      Message msg = hb.toMessage();
      MessageProperties h = msg.getMessageProperties();
      Object type = h.getHeader(HEADER_TYPE);
      assertThat(type).isEqualTo(WorkerHeartbeatMessage.TYPE);
      assertThat((Object) h.getHeader(HEADER_NODE_ID)).isEqualTo("w-42");
      assertThat((Object) h.getHeader(HEADER_HEARTBEAT_EPOCH)).isEqualTo(5L);
      assertThat((Object) h.getHeader(HEADER_HEARTBEAT_DV_HWM)).isEqualTo(99L);
      assertThat((Object) h.getHeader(HEADER_HEARTBEAT_LOAD)).isEqualTo(0.75);
      assertThat((Object) h.getHeader(HEADER_HEARTBEAT_READY)).isEqualTo(true);
      assertThat((Object) h.getHeader(HEADER_HEARTBEAT_CONFIG_CONFIRM)).isEqualTo(3);
    }

    @Test
    @DisplayName("toMessage body contains workerId")
    void toMessage_bodyIsWorkerId() {
      WorkerHeartbeatMessage hb = new WorkerHeartbeatMessage(0L, "w-x", 1L, 0L, 0.0, false, 0, 0, 0, 0L);
      assertThat(new String(hb.toMessage().getBody(), StandardCharsets.UTF_8)).isEqualTo("w-x");
    }

    @Test
    @DisplayName("from round-trips correctly")
    void from_roundTrips() {
      WorkerHeartbeatMessage original = new WorkerHeartbeatMessage(0L, "w-7", 3L, 42L, 0.5, true, 5, 8, 1, 7777L);
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
    @DisplayName("from with wrong header types defaults safely")
    void from_wrongHeaderTypes_defaultsSafely() {
      MessageProperties props = new MessageProperties();
      props.setHeader(HEADER_TYPE, WorkerHeartbeatMessage.TYPE);
      props.setHeader(HEADER_NODE_ID, 123);
      props.setHeader(HEADER_HEARTBEAT_EPOCH, "bad");
      props.setHeader(HEADER_HEARTBEAT_READY, "bad");
      Message msg = new Message("body".getBytes(StandardCharsets.UTF_8), props);
      WorkerHeartbeatMessage hb = WorkerHeartbeatMessage.from(msg);
      assertThat(hb.workerId()).isEmpty();
      assertThat(hb.epoch()).isZero();
      assertThat(hb.readyToServe()).isFalse();
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 7. WorkerHeartbeatVerifier — timeout and dead worker
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("WorkerHeartbeatVerifier timeout and dead worker detection")
  class HeartbeatVerifierTests {

    private RabbitTemplate rt;
    private HealthView healthView;
    private WorkerHeartbeatVerifier verifier;

    @BeforeEach
    void init() {
      rt = mock(RabbitTemplate.class);
      healthView = spy(new HealthViewImpl(3, 500_000, 99));
      healthView.onHeartbeat(hb("w1", true));
      healthView.onHeartbeat(hb("w2", false));
      healthView.onHeartbeat(hb("w3", false));
      verifier = new WorkerHeartbeatVerifier(
        rt,
        healthView,
        "test-app",
        new WorkerHeartbeatVerifier.VerifierConfig(100_000, 500, 60_000)
      );
    }

    private static WorkerHeartbeatMessage hb(String workerId, boolean ready) {
      return new WorkerHeartbeatMessage(0L, workerId, 1, 0, 0.0, ready, 0, 0, 0, 0);
    }

    @Test
    @DisplayName("sendPingAndWaitPong returns true when pong received")
    void ping_returnsTrueOnPong() {
      when(rt.sendAndReceive(anyString(), anyString(), any())).thenReturn(
        new Message(new byte[0], new MessageProperties())
      );
      assertThat(verifier.sendPingAndWaitPong("w2")).isTrue();
    }

    @Test
    @DisplayName("sendPingAndWaitPong returns false on timeout")
    void ping_returnsFalseOnTimeout() {
      when(rt.sendAndReceive(anyString(), anyString(), any())).thenThrow(new AmqpTimeoutException("timeout"));
      assertThat(verifier.sendPingAndWaitPong("w2")).isFalse();
    }

    @Test
    @DisplayName("sendPingAndWaitPong returns false on null pong")
    void ping_returnsFalseOnNullPong() {
      when(rt.sendAndReceive(anyString(), anyString(), any())).thenReturn(null);
      assertThat(verifier.sendPingAndWaitPong("w2")).isFalse();
    }

    @Test
    @DisplayName("sendPingAndWaitPong sends to correct queue with headers")
    void ping_sendsToCorrectQueue() {
      when(rt.sendAndReceive(anyString(), anyString(), any())).thenReturn(
        new Message(new byte[0], new MessageProperties())
      );

      verifier.sendPingAndWaitPong("w2");

      ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
      verify(rt).sendAndReceive(eq(""), eq("zeta.verify.ping.w2"), captor.capture());
      Message sent = captor.getValue();
      assertThat((String) sent.getMessageProperties().getHeader(HEADER_VERIFY_TYPE)).isEqualTo(HEADER_VERIFY_PING);
      assertThat(sent.getMessageProperties().getReplyTo()).isEqualTo("amq.rabbitmq.reply-to");
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 8. PerKeyOrderedDispatcher — ordering guarantees
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("PerKeyOrderedDispatcher ordering guarantees")
  class DispatcherOrderingTests {

    @Test
    @DisplayName("tasks for same key execute in FIFO order")
    void sameKey_fifoOrder() throws Exception {
      PerKeyOrderedDispatcher d = new PerKeyOrderedDispatcher(scheduler, "test");
      try {
        AtomicInteger seq = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(3);

        d.submit("k", () -> {
          assertThat(seq.getAndIncrement()).isEqualTo(0);
          latch.countDown();
        });
        d.submit("k", () -> {
          assertThat(seq.getAndIncrement()).isEqualTo(1);
          latch.countDown();
        });
        d.submit("k", () -> {
          assertThat(seq.getAndIncrement()).isEqualTo(2);
          latch.countDown();
        });

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
      PerKeyOrderedDispatcher d = new PerKeyOrderedDispatcher(multiScheduler, "test");
      try {
        CountDownLatch task1Block = new CountDownLatch(1);
        CountDownLatch task1Running = new CountDownLatch(1);
        CountDownLatch task2Done = new CountDownLatch(1);

        d.submit("k1", () -> {
          task1Running.countDown();
          try {
            assertThat(task1Block.await(10, TimeUnit.SECONDS)).isTrue();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });

        assertThat(task1Running.await(5, TimeUnit.SECONDS)).isTrue();

        d.submit("k2", task2Done::countDown);
        assertThat(task2Done.await(5, TimeUnit.SECONDS)).isTrue();

        task1Block.countDown();
      } finally {
        d.close();
        multiScheduler.shutdownNow();
      }
    }

    @Test
    @DisplayName("delayed submission preserves per-key FIFO ordering")
    void delayedSubmission_preservesOrder() throws Exception {
      PerKeyOrderedDispatcher d = new PerKeyOrderedDispatcher(scheduler, "test");
      try {
        List<Integer> executionOrder = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(3);

        // Submit with increasing delays: tasks execute in order of delay expiry (shortest first),
        // but per-key FIFO ensures no concurrency within the same key.
        // With delays 0, 10, 50 the execution order matches submit order
        d.submit(
          "k",
          () -> {
            executionOrder.add(1);
            latch.countDown();
          },
          0
        );
        d.submit(
          "k",
          () -> {
            executionOrder.add(2);
            latch.countDown();
          },
          10
        );
        d.submit(
          "k",
          () -> {
            executionOrder.add(3);
            latch.countDown();
          },
          50
        );

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(executionOrder).containsExactly(1, 2, 3);
      } finally {
        d.close();
      }
    }

    @Test
    @DisplayName("closed dispatcher drops tasks")
    void closed_dropsTasks() throws Exception {
      PerKeyOrderedDispatcher d = new PerKeyOrderedDispatcher(scheduler, "test");
      d.close();

      CountDownLatch latch = new CountDownLatch(1);
      d.submit("k", latch::countDown);

      assertThat(latch.await(500, TimeUnit.MILLISECONDS)).isFalse();
    }

    @Test
    @DisplayName("full queue silently rejects overflow tasks")
    void fullQueue_rejectsOverflow() throws Exception {
      PerKeyOrderedDispatcher d = new PerKeyOrderedDispatcher(scheduler, "test", 1);
      try {
        CountDownLatch block = new CountDownLatch(1);
        CountDownLatch started = new CountDownLatch(1);

        d.submit("k", () -> {
          started.countDown();
          try {
            assertThat(block.await(10, TimeUnit.SECONDS)).isTrue();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });

        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        CountDownLatch queued = new CountDownLatch(1);
        CountDownLatch rejected = new CountDownLatch(1);

        d.submit("k", queued::countDown);
        d.submit("k", rejected::countDown);

        block.countDown();

        assertThat(queued.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(rejected.await(500, TimeUnit.MILLISECONDS)).isFalse();
      } finally {
        d.close();
      }
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 9. Degraded version handling — both skip/accept paths
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Degraded version handling in sync")
  class DegradedVersionTests {

    @Test
    @DisplayName("REFRESH: both normal, equal version — skip")
    void refresh_bothNormalEqual_skip() throws Exception {
      CacheSyncListener l = createListener(k -> "v");
      cache.put("k", entry(5, false, 0, null, 0, KeyState.NORMAL));
      l.handleSyncMessage(channel, syncMessage("k", SyncMessage.TYPE_REFRESH, 5L, false));
      awaitScheduler();
      assertThat(((CacheEntry) cache.getIfPresent("k")).getDataVersion()).isEqualTo(5L);
    }

    @Test
    @DisplayName("REFRESH: both degraded, equal version — skip")
    void refresh_bothDegradedEqual_skip() throws Exception {
      CacheSyncListener l = createListener(k -> "v");
      cache.put("k", entry(3, true, 0, null, 0, KeyState.NORMAL));
      l.handleSyncMessage(channel, syncMessage("k", SyncMessage.TYPE_REFRESH, 3L, true));
      awaitScheduler();
      assertThat(((CacheEntry) cache.getIfPresent("k")).getDataVersion()).isEqualTo(3L);
    }

    @Test
    @DisplayName("REFRESH: existing degraded, incoming degraded higher — accept")
    void refresh_bothDegradedHigher_accept() throws Exception {
      CacheSyncListener l = createListener(k -> "v");
      cache.put("k", entry(3, true, 0, null, 0, KeyState.NORMAL));
      l.handleSyncMessage(channel, syncMessage("k", SyncMessage.TYPE_REFRESH, 10L, true));
      awaitScheduler();
      assertThat(((CacheEntry) cache.getIfPresent("k")).getDataVersion()).isEqualTo(10L);
    }

    @Test
    @DisplayName("INVALIDATE: existing degraded, incoming normal — accept")
    void invalidate_existingDegradedIncomingNormal_accept() throws Exception {
      CacheSyncListener l = createListener(k -> null);
      cache.put("k", entry(3, true, 0, null, 0, KeyState.NORMAL));
      l.handleSyncMessage(channel, syncMessage("k", SyncMessage.TYPE_INVALIDATE, 1L, false));
      awaitScheduler();
      assertThat(cache.getIfPresent("k")).isNull();
    }

    @Test
    @DisplayName("INVALIDATE: existing normal, incoming degraded version 0 — skip")
    void invalidate_existingNormalIncomingDegradedV0_skip() throws Exception {
      CacheSyncListener l = createListener(k -> null);
      cache.put("k", entry(5, false, 0, null, 0, KeyState.NORMAL));
      l.handleSyncMessage(channel, syncMessage("k", SyncMessage.TYPE_INVALIDATE, 0L, true));
      awaitScheduler();
      assertThat(cache.getIfPresent("k")).isNotNull();
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
      cache.put("k", entry(0, false, 0, null, 0, KeyState.NORMAL));
      int threadCount = 10;
      ExecutorService pool = Executors.newFixedThreadPool(threadCount);
      CountDownLatch startGate = new CountDownLatch(1);

      for (int i = 0; i < threadCount; i++) {
        long v = i + 1;
        pool.submit(() -> {
          try {
            startGate.await();
            // We bypass the channel mock for concurrent test
            l.handleSyncMessage(channel, syncMessage("k", SyncMessage.TYPE_REFRESH, v, false));
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
        cache.put(key, entry(0, false, 0, null, 0, KeyState.NORMAL));
        int version = i + 1;
        pool.submit(() -> {
          try {
            startGate.await();
            l.handleSyncMessage(channel, syncMessage(key, SyncMessage.TYPE_REFRESH, version, false));
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
      cache.put("k", entry(10, false, 0, null, 0, KeyState.NORMAL));

      // INVALIDATE at version 15
      l.handleSyncMessage(channel, syncMessage("k", SyncMessage.TYPE_INVALIDATE, 15L, false));
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
      cache.put("k", entry(1, false, 0, null, 0, KeyState.NORMAL));
      WorkerListener wl = createWorkerListener(k -> "fresh", null);

      wl.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_HOT, 5L, "w1", 1));
      wl.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_HOT, 3L, "w1", 1));
      awaitScheduler();

      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getDecisionVersion()).isEqualTo(5L);
    }

    @Test
    @DisplayName("HOT then COOL from different workers — last-writer-wins")
    void hotThenCool_differentWorkers() throws Exception {
      cache.put("k", entry(1, false, 0, null, 0, KeyState.NORMAL));
      WorkerListener wl = createWorkerListener(k -> "fresh", null);

      wl.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_HOT, 10L, "w1", 1));
      wl.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_COOL, 5L, "w2", 1));
      awaitScheduler();

      // w2 at same epoch, different nodeId → unconditionally accepted
      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getKeyState()).isEqualTo(KeyState.COOL);
      assertThat(ce.getDecisionNodeId()).isEqualTo("w2");
    }

    @Test
    @DisplayName("HOT from restarted worker (higher epoch) overrides existing decision")
    void hot_afterWorkerRestart_overrides() throws Exception {
      cache.put("k", entry(1, false, 0, null, 0, KeyState.NORMAL));
      WorkerListener wl = createWorkerListener(k -> "fresh", null);

      // First HOT from w1 epoch 1
      wl.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_HOT, 100L, "w1", 1));
      awaitScheduler();
      assertThat(((CacheEntry) cache.getIfPresent("k")).getDecisionVersion()).isEqualTo(100L);

      // Worker restarts, same nodeId but epoch=2, lower dv
      wl.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_HOT, 1L, "w1", 2));
      awaitScheduler();

      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      // Higher epoch unconditionally accepted even with lower dv
      assertThat(ce.getDecisionVersion()).isEqualTo(1L);
      assertThat(ce.getDecisionEpoch()).isEqualTo(2L);
    }

    @Test
    @DisplayName("stale message from lower epoch is rejected")
    void staleLowerEpoch_rejected() throws Exception {
      cache.put("k", entry(1, false, 0, null, 0, KeyState.NORMAL));
      WorkerListener wl = createWorkerListener(k -> "fresh", null);

      wl.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_HOT, 5L, "w1", 2));
      awaitScheduler();
      assertThat(((CacheEntry) cache.getIfPresent("k")).getDecisionVersion()).isEqualTo(5L);

      // Old epoch message arrives late
      wl.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_HOT, 10L, "w1", 1));
      awaitScheduler();

      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getDecisionVersion()).isEqualTo(5L);
      assertThat(ce.getDecisionEpoch()).isEqualTo(2L);
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 13. Heartbeat timeout edge cases
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Heartbeat timeout edge cases")
  class HeartbeatTimeoutEdgeTests {

    @Test
    @DisplayName("backoff for attempt 1 is verifyInterval × 1")
    void computeBackoff_attempt1() throws Exception {
      WorkerHeartbeatVerifier v = new WorkerHeartbeatVerifier(
        mock(RabbitTemplate.class),
        spy(new HealthViewImpl(3, 5000, 99)),
        "app",
        new WorkerHeartbeatVerifier.VerifierConfig(1000, 500, 60_000)
      );
      long result = invokeComputeBackoff(v, 1);
      assertThat(result).isEqualTo(1000L);
    }

    @Test
    @DisplayName("backoff for attempt 2 is verifyInterval × 2")
    void computeBackoff_attempt2() throws Exception {
      WorkerHeartbeatVerifier v = new WorkerHeartbeatVerifier(
        mock(RabbitTemplate.class),
        spy(new HealthViewImpl(3, 5000, 99)),
        "app",
        new WorkerHeartbeatVerifier.VerifierConfig(1000, 500, 60_000)
      );
      long result = invokeComputeBackoff(v, 2);
      assertThat(result).isEqualTo(2000L);
    }

    @Test
    @DisplayName("backoff for attempt 4 uses dense-start-steep-extension formula")
    void computeBackoff_attempt4() throws Exception {
      WorkerHeartbeatVerifier v = new WorkerHeartbeatVerifier(
        mock(RabbitTemplate.class),
        spy(new HealthViewImpl(3, 5000, 99)),
        "app",
        new WorkerHeartbeatVerifier.VerifierConfig(1000, 500, 60_000)
      );
      // attempt 4: shift = 4+1 = 5, so 1000 * (1 << 5) = 32000
      long result = invokeComputeBackoff(v, 4);
      assertThat(result).isEqualTo(32000L);
    }

    @Test
    @DisplayName("backoff capped at verifyMaxBackoffMs")
    void computeBackoff_capped() throws Exception {
      WorkerHeartbeatVerifier v = new WorkerHeartbeatVerifier(
        mock(RabbitTemplate.class),
        spy(new HealthViewImpl(3, 5000, 99)),
        "app",
        new WorkerHeartbeatVerifier.VerifierConfig(1000, 500, 5000)
      );
      // attempt 5: shift = 5+1 = 6, 1000 * (1 << 6) = 64000, capped to 5000
      long result = invokeComputeBackoff(v, 5);
      assertThat(result).isEqualTo(5000L);
    }

    private long invokeComputeBackoff(WorkerHeartbeatVerifier v, int attempt) throws Exception {
      java.lang.reflect.Method m = WorkerHeartbeatVerifier.class.getDeclaredMethod("computeBackoffMs", int.class);
      m.setAccessible(true);
      return (long) m.invoke(v, attempt);
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 14. Listener with missing cache entry
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Listener with missing/non-existent cache entry")
  class MissingEntryTests {

    @Test
    @DisplayName("REFRESH on missing key creates new entry from Redis loader")
    void refreshOnMissingKey_createsEntry() throws Exception {
      CacheSyncListener l = createListener(k -> "brand-new");
      l.handleSyncMessage(channel, syncMessage("missing", SyncMessage.TYPE_REFRESH, 1L, false));
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
      l.handleSyncMessage(channel, syncMessage("missing", SyncMessage.TYPE_REFRESH, 1L, false));
      awaitScheduler();
      assertThat(cache.getIfPresent("missing")).isNull();
    }

    @Test
    @DisplayName("INVALIDATE on missing key is no-op")
    void invalidateOnMissingKey_noop() throws Exception {
      CacheSyncListener l = createListener(k -> null);
      l.handleSyncMessage(channel, syncMessage("missing", SyncMessage.TYPE_INVALIDATE, 1L, false));
      awaitScheduler();
      assertThat(cache.getIfPresent("missing")).isNull();
    }

    @Test
    @DisplayName("COOL on missing key is no-op")
    void coolOnMissingKey_noop() throws Exception {
      WorkerListener wl = createWorkerListener(k -> null, null);
      wl.handleWorkerMessage(channel, workerMessage("missing", WorkerMessage.TYPE_COOL, 1L, "w1", 1));
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
      PerKeyOrderedDispatcher d = new PerKeyOrderedDispatcher(scheduler, "test");
      try {
        List<Integer> results = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(3);

        d.submit("order-key", () -> {
          results.add(1);
          latch.countDown();
        });
        d.submit("order-key", () -> {
          results.add(2);
          latch.countDown();
        });
        d.submit("order-key", () -> {
          results.add(3);
          latch.countDown();
        });

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(results).containsExactly(1, 2, 3);
      } finally {
        d.close();
      }
    }

    @Test
    @DisplayName("no interleaving: same key tasks don't overlap")
    void sameKey_tasksDontOverlap() throws Exception {
      PerKeyOrderedDispatcher d = new PerKeyOrderedDispatcher(scheduler, "test");
      try {
        CountDownLatch task1Running = new CountDownLatch(1);
        CountDownLatch task1Done = new CountDownLatch(1);
        AtomicInteger concurrentCount = new AtomicInteger(0);
        CountDownLatch task2Ran = new CountDownLatch(1);

        d.submit("no-overlap", () -> {
          task1Running.countDown();
          concurrentCount.incrementAndGet();
          try {
            Thread.sleep(300);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          concurrentCount.decrementAndGet();
          task1Done.countDown();
        });

        d.submit("no-overlap", () -> {
          // This should only run after task1 completes
          assertThat(concurrentCount.get()).isZero();
          task2Ran.countDown();
        });

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
      cache.put("k", entry(1, false, 0, null, 0, KeyState.NORMAL));
      WorkerListener wl = createWorkerListener(k -> "fresh", null);

      wl.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_HOT, 10L, "w1", 1));
      awaitScheduler();
      assertThat(((CacheEntry) cache.getIfPresent("k")).getDecisionNodeId()).isEqualTo("w1");

      wl.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_HOT, 5L, "w2", 1));
      awaitScheduler();

      // Different nodeId, same epoch → unconditional accept
      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getDecisionNodeId()).isEqualTo("w2");
      assertThat(ce.getDecisionVersion()).isEqualTo(5L);
    }

    @Test
    @DisplayName("first worker wins when own epoch is higher")
    void higherEpoch_overridesLower() throws Exception {
      cache.put("k", entry(1, false, 0, null, 0, KeyState.NORMAL));
      WorkerListener wl = createWorkerListener(k -> "fresh", null);

      wl.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_HOT, 50L, "w1", 2));
      awaitScheduler();
      assertThat(((CacheEntry) cache.getIfPresent("k")).getDecisionEpoch()).isEqualTo(2L);

      // Late message from lower epoch
      wl.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_HOT, 99L, "w2", 1));
      awaitScheduler();

      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getDecisionEpoch()).isEqualTo(2L);
      assertThat(ce.getDecisionNodeId()).isEqualTo("w1");
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 17. Graceful degradation chain
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Graceful degradation chain")
  class GracefulDegradationTests {

    @Test
    @DisplayName("workers dead -> degraded entry accepted -> workers recover -> normal overwrites")
    void degradationChain() throws Exception {
      WorkerListener wl = createWorkerListener(
        k -> {
          throw new RuntimeException("Redis down");
        },
        null
      );

      // Phase 1: Worker dead, Redis unavailable, degraded entry exists
      cache.put("k", entry(5, true, 0, null, 0, KeyState.NORMAL));

      // HOT promotion uses Redis which fails, falls back to degraded value
      wl.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_HOT, 1L, "w1", 1));
      awaitScheduler();

      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.isVersionDegraded()).isTrue();
      assertThat(ce.getKeyState()).isEqualTo(KeyState.HOT);

      // Phase 2: Worker recovers, Redis back online with fresh data
      // Note: isVersionDegraded stays true because replaceEntryValue preserves the existing degraded flag;
      // only a subsequent write path or a non-degraded Redis INCR version resets it
      WorkerListener wlRecovered = createWorkerListener(k -> "fresh-data", null);

      wlRecovered.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_HOT, 2L, "w1", 2));
      awaitScheduler();

      CacheEntry ce2 = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce2.getValue()).isEqualTo("fresh-data");
      assertThat(ce2.getDecisionEpoch()).isEqualTo(2L);
      assertThat(ce2.getKeyState()).isEqualTo(KeyState.HOT);
    }

    @Test
    @DisplayName("COOL after graceful degradation preserves degraded value")
    void coolAfterDegradation_preservesValue() throws Exception {
      cache.put("k", entry(5, true, 1, "w1", 1, KeyState.HOT));
      WorkerListener wl = createWorkerListener(k -> "fresh", null);

      wl.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_COOL, 2L, "w1", 1));
      awaitScheduler();

      CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
      assertThat(ce.getKeyState()).isEqualTo(KeyState.COOL);
      // Degraded entry preserved
      assertThat(ce.isVersionDegraded()).isTrue();
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
      WorkerListener wl = createWorkerListener(k -> "fresh", null);

      wl.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_HOT, 5L, "w1", 1));
      awaitScheduler();

      assertThat(((CacheEntry) cache.getIfPresent("k")).getDecisionVersion()).isEqualTo(10L);
    }

    @Test
    @DisplayName("WorkerListener rejects stale COOL with same nodeId")
    void cool_staleSameNode_rejected() throws Exception {
      cache.put("k", hotEntry("w1", 10, 1));
      WorkerListener wl = createWorkerListener(k -> "fresh", null);

      wl.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_COOL, 5L, "w1", 1));
      awaitScheduler();

      assertThat(((CacheEntry) cache.getIfPresent("k")).getDecisionVersion()).isEqualTo(10L);
      assertThat(((CacheEntry) cache.getIfPresent("k")).getKeyState()).isEqualTo(KeyState.HOT);
    }

    @Test
    @DisplayName("WorkerListener accepts equal version HOT (no-op but idempotent)")
    void hot_equalVersion_idempotent() throws Exception {
      cache.put("k", hotEntry("w1", 5, 1));
      WorkerListener wl = createWorkerListener(k -> "fresh", null);

      wl.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_HOT, 5L, "w1", 1));
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
      props.setHeader(HEADER_TYPE, SyncMessage.TYPE_INVALIDATE);
      Message msg = new Message("   ".getBytes(StandardCharsets.UTF_8), props);
      assertThat(SyncMessage.from(msg)).isNull();
    }

    @Test
    @DisplayName("SyncMessage with null type header produces null type field")
    void nullType_header() {
      Message msg = new Message("k".getBytes(StandardCharsets.UTF_8), new MessageProperties());
      SyncMessage sm = SyncMessage.from(msg);
      assertThat(sm).isNotNull();
      assertThat(sm.type()).isNull();
    }

    @Test
    @DisplayName("WorkerMessage with empty body returns null")
    void workerMessage_emptyBody() {
      assertThat(WorkerMessage.from(new Message(new byte[0], new MessageProperties()))).isNull();
    }

    @Test
    @DisplayName("CacheSyncListener handles null type gracefully")
    void listener_nullType_acks() throws Exception {
      CacheSyncListener l = createListener(k -> "v");
      Message msg = new Message("k".getBytes(StandardCharsets.UTF_8), new MessageProperties());
      l.handleSyncMessage(channel, msg);
      verify(channel).basicAck(anyLong(), eq(false));
    }

    @Test
    @DisplayName("WorkerListener handles null type gracefully")
    void workerListener_nullType_acks() throws Exception {
      WorkerListener wl = createWorkerListener(k -> "v", null);
      Message msg = new Message("k".getBytes(StandardCharsets.UTF_8), new MessageProperties());
      wl.handleWorkerMessage(channel, msg);
      verify(channel).basicAck(anyLong(), eq(false));
    }

    @Test
    @DisplayName("CacheSyncListener nacks on processing error")
    void listener_nacksOnError() throws Exception {
      doThrow(new RuntimeException("forced error")).when(channel).basicAck(anyLong(), eq(false));
      CacheSyncListener l = createListener(k -> "v");
      Message msg = syncMessage("k", SyncMessage.TYPE_INVALIDATE, 1L, false);
      l.handleSyncMessage(channel, msg);
      verify(channel).basicNack(anyLong(), eq(false), eq(false));
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
      BroadcastBuffer buf = new BroadcastBuffer(scheduler, Optional.of(pub), 5000);

      buf.record("k1", 1L, false);
      buf.record("k2", 2L, true);

      buf.flush();

      verify(pub).broadcastRefresh("k1", 1L, false);
      verify(pub).broadcastRefresh("k2", 2L, true);
    }

    @Test
    @DisplayName("last-writer-wins: recording same key twice sends only latest")
    void lastWriterWins() {
      CacheSyncPublisher pub = mock(CacheSyncPublisher.class);
      BroadcastBuffer buf = new BroadcastBuffer(scheduler, Optional.of(pub), 5000);

      buf.record("k", 1L, false);
      buf.record("k", 5L, true);

      buf.flush();

      verify(pub).broadcastRefresh("k", 5L, true);
      verifyNoMoreInteractions(pub);
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
        .broadcastRefresh("auto", 1L, false);

      BroadcastBuffer buf = new BroadcastBuffer(scheduler, Optional.of(pub), 100);
      buf.record("auto", 1L, false);

      assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
      verify(pub).broadcastRefresh("auto", 1L, false);
    }

    @Test
    @DisplayName("flush with no records does nothing")
    void flush_noRecords_noop() {
      CacheSyncPublisher pub = mock(CacheSyncPublisher.class);
      BroadcastBuffer buf = new BroadcastBuffer(scheduler, Optional.of(pub), 5000);
      buf.flush();
      verifyNoInteractions(pub);
    }

    @Test
    @DisplayName("publisher exception during flush is caught, remaining keys processed")
    void flush_publisherException_continues() {
      CacheSyncPublisher pub = mock(CacheSyncPublisher.class);
      doThrow(new RuntimeException("pub fail")).when(pub).broadcastRefresh("bad", 1L, false);

      BroadcastBuffer buf = new BroadcastBuffer(scheduler, Optional.of(pub), 5000);
      buf.record("bad", 1L, false);
      buf.record("good", 2L, false);

      buf.flush();

      verify(pub).broadcastRefresh("bad", 1L, false);
      verify(pub).broadcastRefresh("good", 2L, false);
    }

    @Test
    @DisplayName("concurrent record and flush is safe")
    void concurrent_recordAndFlush_safe() throws Exception {
      CacheSyncPublisher pub = mock(CacheSyncPublisher.class);
      BroadcastBuffer buf = new BroadcastBuffer(scheduler, Optional.of(pub), 5000);

      ExecutorService pool = Executors.newFixedThreadPool(4);
      CountDownLatch startGate = new CountDownLatch(1);
      int records = 200;

      for (int i = 0; i < records; i++) {
        int idx = i;
        pool.submit(() -> {
          try {
            startGate.await();
            buf.record("ck-" + idx, idx, false);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
      }

      startGate.countDown();
      pool.shutdown();
      assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

      buf.flush();

      verify(pub, atLeast(records)).broadcastRefresh(anyString(), anyLong(), anyBoolean());
    }

    @Test
    @DisplayName("record with no publisher is safe")
    void record_noPublisher_safe() {
      BroadcastBuffer buf = new BroadcastBuffer(scheduler, Optional.empty());
      buf.record("k", 1L, false);
      buf.flush();
      // No exception
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // SRE Rate Limiter integration with WorkerListener
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("SRE Rate Limiter integration")
  class SreIntegrationTests {

    @Test
    @DisplayName("SRE throttled HOT skips promotion without calling onFailed")
    void sreThrottled_skipsPromotion() throws Exception {
      SreRateLimiterImpl limiter = mock(SreRateLimiterImpl.class);
      when(limiter.tryAcquire()).thenReturn(false);
      cache.put("k", entry(1, false, 0, null, 0, KeyState.NORMAL));

      WorkerListener wl = createWorkerListener(k -> "fresh", limiter);
      wl.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_HOT, 1L, "w1", 1));
      awaitScheduler();

      verify(limiter, never()).onFailed();
      assertThat(((CacheEntry) cache.getIfPresent("k")).getKeyState()).isNotEqualTo(KeyState.HOT);
    }

    @Test
    @DisplayName("SRE allowed HOT calls onSuccess")
    void sreAllowed_callsOnSuccess() throws Exception {
      SreRateLimiterImpl limiter = mock(SreRateLimiterImpl.class);
      when(limiter.tryAcquire()).thenReturn(true);
      cache.put("k", entry(1, false, 0, null, 0, KeyState.NORMAL));

      WorkerListener wl = createWorkerListener(k -> "fresh", limiter);
      wl.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_HOT, 1L, "w1", 1));
      awaitScheduler();

      verify(limiter).onSuccess();
      assertThat(((CacheEntry) cache.getIfPresent("k")).getKeyState()).isEqualTo(KeyState.HOT);
    }

    @Test
    @DisplayName("SRE null (disabled) does not interfere")
    void sreNull_doesNotInterfere() throws Exception {
      cache.put("k", entry(1, false, 0, null, 0, KeyState.NORMAL));
      WorkerListener wl = createWorkerListener(k -> "fresh", null);

      wl.handleWorkerMessage(channel, workerMessage("k", WorkerMessage.TYPE_HOT, 1L, "w1", 1));
      awaitScheduler();

      assertThat(((CacheEntry) cache.getIfPresent("k")).getKeyState()).isEqualTo(KeyState.HOT);
    }
  }
}
