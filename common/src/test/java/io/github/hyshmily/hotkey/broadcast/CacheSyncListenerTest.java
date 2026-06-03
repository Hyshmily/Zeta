package io.github.hyshmily.hotkey.broadcast;

import static io.github.hyshmily.hotkey.constant.HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED;
import static io.github.hyshmily.hotkey.constant.HotKeyConstants.AMQP_HEADER_TYPE;
import static io.github.hyshmily.hotkey.constant.HotKeyConstants.AMQP_HEADER_VERSION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.rabbitmq.client.Channel;
import io.github.hyshmily.hotkey.entity.CacheEntry;
import io.github.hyshmily.hotkey.entity.KeyState;
import io.github.hyshmily.hotkey.hotkeycache.CacheExpireManager;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyProperties;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

class CacheSyncListenerTest {

  private Cache<String, Object> cache;
  private CacheSyncListener listener;
  private Channel channel;
  private ScheduledExecutorService scheduler;

  @BeforeEach
  void setUp() throws IOException {
    cache = Caffeine.newBuilder().maximumSize(100).build();
    Function<String, Object> redisLoader = k -> "refreshed";
    CacheSyncProperties properties = new CacheSyncProperties();
    properties.setWarmupJitterMs(0);
    scheduler = Executors.newSingleThreadScheduledExecutor();
    HotKeyProperties ttlConfig = new HotKeyProperties();
    CacheExpireManager expireManager = new CacheExpireManager(cache, Runnable::run, ttlConfig, 10);

    listener = new CacheSyncListener(cache, redisLoader, properties, scheduler, expireManager, mock(RuleMatcher.class));
    channel = mock(Channel.class);
  }

  @Test
  void handleSyncMessage_invalidate_shouldAckAndRemove() throws IOException {
    cache.put("key1", "value");
    listener.handleSyncMessage(channel, syncMessage("key1", SyncMessage.TYPE_INVALIDATE, 1L, false));
    verify(channel).basicAck(anyLong(), eq(false));
  }

  @Test
  void handleSyncMessage_refresh_shouldAckAndUpdate() throws IOException {
    cache.put("key1", entry(1, false, 0));
    listener.handleSyncMessage(channel, syncMessage("key1", SyncMessage.TYPE_REFRESH, 2L, false));
    verify(channel).basicAck(anyLong(), eq(false));
  }

  @Test
  void handleSyncMessage_shouldNackOnError() throws IOException {
    MessageProperties props = new MessageProperties();
    props.setHeader(AMQP_HEADER_TYPE, "INVALIDATE");
    Message msg = new Message("key1".getBytes(StandardCharsets.UTF_8), props);
    doThrow(new RuntimeException("forced error")).when(channel).basicAck(anyLong(), eq(false));
    listener.handleSyncMessage(channel, msg);
    verify(channel).basicNack(anyLong(), eq(false), eq(false));
  }

  @Test
  void handleSyncMessage_shouldHandleNullType() throws IOException {
    MessageProperties props = new MessageProperties();
    Message msg = new Message("key1".getBytes(StandardCharsets.UTF_8), props);
    listener.handleSyncMessage(channel, msg);
    verify(channel).basicAck(anyLong(), eq(false));
  }

  private static Message syncMessage(String key, String type, long version, boolean degraded) {
    MessageProperties props = new MessageProperties();
    props.setHeader(AMQP_HEADER_TYPE, type);
    props.setHeader(AMQP_HEADER_VERSION, version);
    props.setHeader(AMQP_HEADER_IS_VERSION_DEGRADED, degraded);
    return new Message(key.getBytes(StandardCharsets.UTF_8), props);
  }

  private static CacheEntry entry(long dataVersion, boolean degraded, long decisionVersion) {
    return CacheEntry.builder()
      .value("v")
      .dataVersion(dataVersion)
      .isVersionDegraded(degraded)
      .decisionVersion(decisionVersion)
      .hardTtlMs(300_000)
      .hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(30_000)
      .softExpireAtMs(30_000)
      .keyState(KeyState.NORMAL)
      .normalHardTtlMs(300_000)
      .normalSoftTtlMs(30_000)
      .build();
  }
}
