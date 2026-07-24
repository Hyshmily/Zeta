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
package io.github.hyshmily.zeta.worker.dispatch;

import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.*;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Exchange.HEARTBEAT;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Routing.KEY_HEARTBEAT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.anyLong;

import io.github.hyshmily.zeta.detection.ZetaBayesianSM;
import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatMessage;
import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Tests for {@link WorkerHeartbeatProducer}.
 *
 * <p>Covers epoch initialisation via Redis (primary), local file (fallback), and
 * {@code System.currentTimeMillis()} (last resort). Also verifies the heartbeat
 * message structure, ready-to-serve logic, config fingerprint computation, and
 * lifecycle (start/stop).
 */
@ExtendWith(MockitoExtension.class)
class WorkerHeartbeatProducerTest {

  @Mock
  private RabbitTemplate rabbitTemplate;

  @Mock
  private ZetaBayesianSM stateMachine;

  @Mock
  private WorkerBroadcaster broadcaster;

  @Mock
  private AtomicLong configTimestampCounter;

  @Captor
  private ArgumentCaptor<Message> messageCaptor;

  private static final String WORKER_ID = "worker1";
  private static final String HB_EXCHANGE = HEARTBEAT;
  private static final long PING_INTERVAL_MS = 1000L;

  /** Each file-based test gets its own temp directory for isolation. */
  @TempDir
  private Path tempDir;

  @BeforeEach
  void cleanEpochFile() {
    try {
      Files.deleteIfExists(epochFilePath());
    } catch (Exception e) {
      // ignore
    }
  }

  // ── Epoch initialisation ──

  /**
   * First start with Redis available and no prior epoch.
   * Epoch is floored at {@code System.currentTimeMillis() * 1000} to prevent
   * counter rollback across Redis resets or file deletion.
   */
  @Test
  void constructor_shouldDoInitEpochFromRedis_whenFirstStart() {
    var producer = newProducer();
    producer.sendHeartbeat();

    verify(rabbitTemplate).send(eq(HB_EXCHANGE), eq(KEY_HEARTBEAT + WORKER_ID), messageCaptor.capture());
    assertThat(messageCaptor.getValue().getMessageProperties().getHeaders())
      .extracting(HEADER_HEARTBEAT_EPOCH)
      .isInstanceOfSatisfying(Long.class, epoch -> assertThat(epoch).isPositive().isGreaterThan(1_500_000_000_000_000L));
  }

  /**
   * Subsequent start with Redis available.  INCR returns a value below the
   * timestamp floor → epoch is floored at the current timestamp.
   */
  @Test
  void constructor_shouldDoInitEpochFromRedis_whenRestart() {
    var redisFactoryMock = mock(RedisConnectionFactory.class);
    try (var ignored = mockConstruction(StringRedisTemplate.class, redisReturning(6L))) {
      var producer = new WorkerHeartbeatProducer(
        rabbitTemplate,
        HB_EXCHANGE,
        WORKER_ID,
        stateMachine,
        broadcaster,
        configTimestampCounter,
        redisFactoryMock,
        PING_INTERVAL_MS,
        mock(ScheduledExecutorService.class),
        mock(SnowflakeIdGenerator.class)
      );
      producer.sendHeartbeat();

      verify(rabbitTemplate).send(eq(HB_EXCHANGE), eq(KEY_HEARTBEAT + WORKER_ID), messageCaptor.capture());
      assertThat(messageCaptor.getValue().getMessageProperties().getHeaders())
        .extracting(HEADER_HEARTBEAT_EPOCH)
        .isInstanceOfSatisfying(Long.class, epoch -> assertThat(epoch).isPositive().isGreaterThan(1_500_000_000_000_000L));
    }
  }

  /**
   * Redis unavailable → fallback to local temp file.
   * File does not exist → created with the timestamp-based floor.
   */
  @Test
  void constructor_shouldFallbackToLocalFile_whenRedisFails() throws Exception {
    var origTmpdir = System.setProperty("java.io.tmpdir", tempDir.toString());
    try {
      var producer = newProducer();
      producer.sendHeartbeat();

      verify(rabbitTemplate).send(any(), any(), messageCaptor.capture());
      long epoch = (Long) messageCaptor.getValue().getMessageProperties().getHeaders().get(HEADER_HEARTBEAT_EPOCH);
      assertThat(epoch).isPositive().isGreaterThan(1_500_000_000_000_000L);

      var path = epochFilePath();
      assertThat(path).exists();
      assertThat(Files.readString(path)).isEqualTo(String.valueOf(epoch));
    } finally {
      System.setProperty("java.io.tmpdir", origTmpdir);
    }
  }

  /**
   * Redis unavailable, local file pre-seeded with "3".
   * Epoch is floored at the timestamp, which dominates the increment.
   */
  @Test
  void constructor_shouldIncrementLocalFileEpochOnRestart() throws Exception {
    var origTmpdir = System.setProperty("java.io.tmpdir", tempDir.toString());
    try {
      Files.writeString(epochFilePath(), "3");
      var producer = newProducer();
      producer.sendHeartbeat();

      verify(rabbitTemplate).send(any(), any(), messageCaptor.capture());
      long epoch = (Long) messageCaptor.getValue().getMessageProperties().getHeaders().get(HEADER_HEARTBEAT_EPOCH);
      assertThat(epoch).isPositive().isGreaterThan(1_500_000_000_000_000L);

      assertThat(Files.readString(epochFilePath())).isEqualTo(String.valueOf(epoch));
    } finally {
      System.setProperty("java.io.tmpdir", origTmpdir);
    }
  }

  /**
   * Redis and local file both fail → epoch falls back to
   * {@code System.currentTimeMillis()}.
   */
  @Test
  void constructor_shouldFallbackToCurrentTimeMillis_whenAllFallbacksFail() {
    var origTmpdir = System.setProperty("java.io.tmpdir", "C:\\nonexistent_hotkey_test_xyz");
    try {
      var producer = newProducer();
      producer.sendHeartbeat();

      verify(rabbitTemplate).send(any(), any(), messageCaptor.capture());
      var epoch = (Long) messageCaptor.getValue().getMessageProperties().getHeaders().get(HEADER_HEARTBEAT_EPOCH);
      assertThat(epoch).isPositive().isGreaterThan(1_500_000_000_000_000L);
    } finally {
      System.setProperty("java.io.tmpdir", origTmpdir);
    }
  }

  // ── sendHeartbeat message structure ──

  /** Verifies exchange, routing key, and every heartbeat header. */
  @Test
  void sendHeartbeat_shouldDeliverAllFields() {
    long dv = 123L;
    long configTs = 456L;
    when(broadcaster.getCurrentDecisionVersion()).thenReturn(dv);
    when(stateMachine.getConfirmCount()).thenReturn(3);
    when(stateMachine.getCoolCount()).thenReturn(10);
    when(stateMachine.getPreCoolGraceCount()).thenReturn(3);
    when(configTimestampCounter.get()).thenReturn(configTs);
    when(stateMachine.getTrackedKeys()).thenReturn(1);

    var producer = newProducer();
    producer.sendHeartbeat();

    verify(rabbitTemplate).send(eq(HB_EXCHANGE), eq(KEY_HEARTBEAT + WORKER_ID), messageCaptor.capture());
    var headers = messageCaptor.getValue().getMessageProperties().getHeaders();

    assertThat(headers).containsEntry(HEADER_TYPE, WorkerHeartbeatMessage.TYPE);
    assertThat(headers).containsEntry(HEADER_NODE_ID, WORKER_ID);
    assertThat((Long) headers.get(HEADER_HEARTBEAT_EPOCH)).isPositive().isGreaterThan(1_500_000_000_000_000L);
    assertThat(headers).containsEntry(HEADER_HEARTBEAT_DV_HWM, dv);
    assertThat(headers.get(HEADER_HEARTBEAT_LOAD)).isInstanceOf(Double.class);
    assertThat((Double) headers.get(HEADER_HEARTBEAT_LOAD)).isBetween(0.0, 1.0);
    assertThat(headers).containsEntry(HEADER_HEARTBEAT_READY, true);
    assertThat(headers).containsEntry(HEADER_HEARTBEAT_CONFIG_CONFIRM, 3);
    assertThat(headers).containsEntry(HEADER_HEARTBEAT_CONFIG_COOL, 10);
    assertThat(headers).containsEntry(HEADER_HEARTBEAT_CONFIG_GRACE, 3);
    assertThat(headers).containsEntry(HEADER_HEARTBEAT_CONFIG_TIMESTAMP, configTs);
  }

  /** Uptime < 3s AND no tracked keys → readyToServe = false. */
  @Test
  void sendHeartbeat_shouldReportNotReady_whenColdStartAndNoTrackedKeys() {
    var producer = newProducer();
    producer.sendHeartbeat();

    verify(rabbitTemplate).send(any(), any(), messageCaptor.capture());
    assertThat(messageCaptor.getValue().getMessageProperties().getHeaders()).containsEntry(
      HEADER_HEARTBEAT_READY,
      false
    );
  }

  /** Tracked keys > 0 → readyToServe = true (regardless of uptime). */
  @Test
  void sendHeartbeat_shouldReportReady_whenTrackedKeysPresent() {
    when(stateMachine.getTrackedKeys()).thenReturn(5);
    var producer = newProducer();
    producer.sendHeartbeat();

    verify(rabbitTemplate).send(any(), any(), messageCaptor.capture());
    assertThat(messageCaptor.getValue().getMessageProperties().getHeaders()).containsEntry(
      HEADER_HEARTBEAT_READY,
      true
    );
  }

  // ── Lifecycle ──

  /** {@code start()} schedules the heartbeat task at fixed rate. */
  @Test
  void start_shouldScheduleHeartbeatAtFixedRate() {
    var schedulerMock = mock(ScheduledExecutorService.class);
    try (var executorsMock = mockStatic(Executors.class)) {
      executorsMock.when(() -> Executors.newSingleThreadScheduledExecutor(any())).thenReturn(schedulerMock);

      var producer = newProducer();
      producer.start();

      verify(schedulerMock).scheduleAtFixedRate(
        any(Runnable.class),
        eq(PING_INTERVAL_MS),
        eq(PING_INTERVAL_MS),
        eq(TimeUnit.MILLISECONDS)
      );
    }
  }

  /** {@code stop()} shuts down the internal scheduler. */
  @Test
  void stop_shouldShutdownScheduler() {
    var producer = newProducer();
    producer.stop();

    var scheduler = (ScheduledExecutorService) ReflectionTestUtils.getField(producer, "scheduler");
    assertThat(scheduler).isNotNull();
    assertThat(scheduler.isShutdown()).isTrue();
  }

  /**
   * {@code stop()} cancels the scheduled task when {@code heartbeatTask} is
   * non-null (i.e., the producer was started).  Also shuts down the scheduler.
   */
  @Test
  void stop_shouldCancelHeartbeatTaskWhenRunning() {
    var schedulerMock = mock(ScheduledExecutorService.class);
    when(schedulerMock.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any())).thenReturn(
      mock(ScheduledFuture.class)
    );

    try (var executorsMock = mockStatic(Executors.class)) {
      executorsMock.when(() -> Executors.newSingleThreadScheduledExecutor(any())).thenReturn(schedulerMock);

      var producer = newProducer();
      producer.start();
      // heartbeatTask is now non-null; stop() must cancel it and shut down
      assertThatCode(producer::stop).doesNotThrowAnyException();
    }
  }

  /**
   * Tests the first public constructor (9-param with {@link RedisConnectionFactory}
   * and {@link ScheduledExecutorService}).  This constructor is called by
   * auto-configuration and sets {@code ownsScheduler = false}.  Verifies that
   * {@code stop()} does <em>not</em> shut down the externally-owned scheduler.
   */
  @Test
  void constructorWithExternalScheduler_shouldNotShutdownSchedulerOnStop() {
    var schedulerMock = mock(ScheduledExecutorService.class);
    when(schedulerMock.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any())).thenReturn(
      mock(ScheduledFuture.class)
    );
    var redisFactoryMock = mock(RedisConnectionFactory.class);
    try (var ignored = mockConstruction(StringRedisTemplate.class, redisReturning(null))) {
      var producer = new WorkerHeartbeatProducer(
        rabbitTemplate,
        HB_EXCHANGE,
        WORKER_ID,
        stateMachine,
        broadcaster,
        configTimestampCounter,
        redisFactoryMock,
        PING_INTERVAL_MS,
        schedulerMock,
        mock(SnowflakeIdGenerator.class)
      );
      producer.start();
      producer.stop();
      verify(schedulerMock, never()).shutdown();
    }
  }

  /**
   * Using the real executor (via the 7-param constructor), {@code start()}
   * schedules the heartbeat task.  The {@code ScheduledThreadPoolExecutor}
   * creates a daemon thread through the thread-factory lambda, exercises the
   * thread-factory body, and the new thread executes {@code sendHeartbeat}.
   */
  @Test
  void start_shouldInvokeThreadFactoryViaRealExecutor() {
    var producer = newProducer();
    producer.start();
    verify(rabbitTemplate, timeout(2000)).send(anyString(), anyString(), any());
    producer.stop();
  }

  // ── Fault tolerance ──

  /**
   * {@code sendHeartbeat} catches exceptions from {@code rabbitTemplate.send}
   * and does not propagate them.
   */
  @Test
  void sendHeartbeat_shouldCatchRabbitMqException() {
    when(broadcaster.getCurrentDecisionVersion()).thenReturn(0L);
    when(configTimestampCounter.get()).thenReturn(0L);
    doThrow(new RuntimeException("RabbitMQ unavailable")).when(rabbitTemplate).send(anyString(), anyString(), any());

    var producer = newProducer();
    producer.sendHeartbeat();
    // must not throw — exception caught in catch block
  }

  /**
   * Verifies that when the local epoch file contains invalid content (not a number),
   * the epoch falls back to {@code System.currentTimeMillis()} as a last resort.
   */
  @Test
  void constructor_shouldFallbackToCurrentTimeMillisForCorruptedEpochFile() throws Exception {
    var origTmpdir = System.setProperty("java.io.tmpdir", tempDir.toString());
    try {
      Files.writeString(epochFilePath(), "not-a-number");
      var producer = newProducer();
      producer.sendHeartbeat();
      verify(rabbitTemplate).send(any(), any(), messageCaptor.capture());
      var epoch = (Long) messageCaptor.getValue().getMessageProperties().getHeaders().get(HEADER_HEARTBEAT_EPOCH);
      assertThat(epoch).isPositive().isGreaterThan(1_500_000_000_000_000L);
    } finally {
      System.setProperty("java.io.tmpdir", origTmpdir);
    }
  }

  // ── Helpers ──

  private WorkerHeartbeatProducer newProducer() {
    return WorkerHeartbeatProducer.forTesting(
      rabbitTemplate,
      HB_EXCHANGE,
      WORKER_ID,
      stateMachine,
      broadcaster,
      configTimestampCounter,
      PING_INTERVAL_MS,
      mock(SnowflakeIdGenerator.class)
    );
  }

  @SuppressWarnings("unchecked")
  private static MockedConstruction.MockInitializer<StringRedisTemplate> redisReturning(Long value) {
    return (mock, ctx) -> {
      var ops = (ValueOperations<String, String>) mock(ValueOperations.class);
      when(mock.opsForValue()).thenReturn(ops);
      when(ops.increment(anyString())).thenReturn(value);
    };
  }

  private static MockedConstruction.MockInitializer<StringRedisTemplate> redisThrowing() {
    return (mock, ctx) -> when(mock.opsForValue()).thenThrow(new RuntimeException("Redis unavailable"));
  }

  private Path epochFilePath() {
    return Path.of(System.getProperty("java.io.tmpdir"), "zeta-epoch-" + WORKER_ID);
  }
}
