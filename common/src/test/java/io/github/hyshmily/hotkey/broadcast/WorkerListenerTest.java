package io.github.hyshmily.hotkey.broadcast;

import static io.github.hyshmily.hotkey.constant.HotKeyConstants.AMQP_HEADER_TYPE;
import static io.github.hyshmily.hotkey.constant.HotKeyConstants.AMQP_HEADER_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.rabbitmq.client.Channel;
import io.github.hyshmily.hotkey.entity.CacheEntry;
import io.github.hyshmily.hotkey.entity.KeyState;
import io.github.hyshmily.hotkey.hotkeycache.CacheExpireManager;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyProperties;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * Tests for {@link WorkerListener} verifying HOT/COOL decision handling, cache state promotion and
 * downgrade, and error NACK behavior.
 */
class WorkerListenerTest {

  private Cache<String, Object> cache;
  private WorkerListener listener;
  private Channel channel;

  @BeforeEach
  void setUp() throws IOException {
    cache = Caffeine.newBuilder().maximumSize(100).build();
    Function<String, Object> redisLoader = k -> "refreshed";
    WorkerListenerProperties properties = new WorkerListenerProperties();
    properties.setWarmupJitterMs(0);
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    HotKeyProperties ttlConfig = new HotKeyProperties();
    CacheExpireManager expireManager = new CacheExpireManager(cache, Runnable::run, ttlConfig, 10);
    listener = new WorkerListener(cache, redisLoader, properties, scheduler, expireManager);
    channel = mock(Channel.class);
  }

  @Test
  void handleWorkerMessage_hot_shouldAckAndPromote() throws IOException {
    listener.handleWorkerMessage(channel, workerMessage("key1", WorkerMessage.TYPE_HOT, 1L));
    verify(channel).basicAck(anyLong(), anyBoolean());
  }

  @Test
  void handleWorkerMessage_cool_shouldAckAndDowngrade() throws IOException {
    cache.put("key1", hotEntry());
    listener.handleWorkerMessage(channel, workerMessage("key1", WorkerMessage.TYPE_COOL, 2L));
    verify(channel).basicAck(anyLong(), anyBoolean());
    assertThat(cache.getIfPresent("key1")).satisfies(o -> {
      CacheEntry ce = (CacheEntry) o;
      assertThat(ce.getKeyState()).isEqualTo(KeyState.COOL);
    });
  }

  @Test
  void handleWorkerMessage_shouldNackOnError() throws IOException {
    MessageProperties props = new MessageProperties();
    props.setHeader(AMQP_HEADER_TYPE, "HOT");
    Message msg = new Message("key1".getBytes(StandardCharsets.UTF_8), props);
    doThrow(new RuntimeException("forced error")).when(channel).basicAck(anyLong(), anyBoolean());
    listener.handleWorkerMessage(channel, msg);
    verify(channel).basicNack(anyLong(), anyBoolean(), anyBoolean());
  }

  private static Message workerMessage(String key, String type, long version) {
    MessageProperties props = new MessageProperties();
    props.setHeader(AMQP_HEADER_TYPE, type);
    props.setHeader(AMQP_HEADER_VERSION, version);
    return new Message(key.getBytes(StandardCharsets.UTF_8), props);
  }

  private static CacheEntry hotEntry() {
    return CacheEntry.builder()
      .value("v")
      .dataVersion(5)
      .isVersionDegraded(false)
      .decisionVersion(1)
      .hardTtlMs(3_600_000)
      .hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(300_000)
      .softExpireAtMs(System.currentTimeMillis() + 300_000)
      .keyState(KeyState.HOT)
      .normalHardTtlMs(300_000)
      .normalSoftTtlMs(30_000)
      .build();
  }
}
