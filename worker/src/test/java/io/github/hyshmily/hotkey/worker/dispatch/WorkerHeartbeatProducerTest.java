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
package io.github.hyshmily.hotkey.worker.dispatch;

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.sync.WorkerHeartbeatMessage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
  private HotKeyStateMachine stateMachine;

  @Mock
  private WorkerBroadcaster broadcaster;

  @Mock
  private AtomicLong configTimestampCounter;

  @Captor
  private ArgumentCaptor<Message> messageCaptor;

  private static final String WORKER_ID = "worker1";
  private static final String HB_EXCHANGE = "hotkey.heartbeat.exchange";
  private static final long PING_INTERVAL_MS = 1000L;

  /** Each file-based test gets its own temp directory for isolation. */
  @TempDir
  private Path tempDir;

  // ── Epoch initialisation ──

  /** First start with Redis available and no prior epoch → epoch = 1. */
  @Test
  void constructor_shouldDoInitEpochFromRedis_whenFirstStart() {
    try (var ignored = mockConstruction(StringRedisTemplate.class, redisReturning(null))) {
      var producer = newProducer();
      producer.sendHeartbeat();

      verify(rabbitTemplate).send(eq(HB_EXCHANGE), eq("heartbeat." + WORKER_ID), messageCaptor.capture());
      assertThat(messageCaptor.getValue().getMessageProperties().getHeaders())
        .containsEntry(AMQP_HEADER_HEARTBEAT_EPOCH, 1L);
    }
  }

  /** Subsequent start: Redis returns previous epoch "5" → epoch = 6. */
  @Test
  void constructor_shouldDoInitEpochFromRedis_whenRestart() {
    try (var ignored = mockConstruction(StringRedisTemplate.class, redisReturning("5"))) {
      var producer = newProducer();
      producer.sendHeartbeat();

      verify(rabbitTemplate).send(eq(HB_EXCHANGE), eq("heartbeat." + WORKER_ID), messageCaptor.capture());
      assertThat(messageCaptor.getValue().getMessageProperties().getHeaders())
        .containsEntry(AMQP_HEADER_HEARTBEAT_EPOCH, 6L);
    }
  }

  /**
   * Redis unavailable → fallback to local temp file.
   * File does not exist → created with "1" → epoch = 1.
   */
  @Test
  void constructor_shouldFallbackToLocalFile_whenRedisFails() throws Exception {
    var origTmpdir = System.setProperty("java.io.tmpdir", tempDir.toString());
    try (var ignored = mockConstruction(StringRedisTemplate.class, redisThrowing())) {
      var producer = newProducer();
      producer.sendHeartbeat();

      verify(rabbitTemplate).send(any(), any(), messageCaptor.capture());
      assertThat(messageCaptor.getValue().getMessageProperties().getHeaders())
        .containsEntry(AMQP_HEADER_HEARTBEAT_EPOCH, 1L);

      var path = epochFilePath();
      assertThat(path).exists();
      assertThat(Files.readString(path)).isEqualTo("1");
    } finally {
      System.setProperty("java.io.tmpdir", origTmpdir);
    }
  }

  /**
   * Redis unavailable, local file pre-seeded with "3" → epoch = 4.
   * Also verifies the file is updated to "4".
   */
  @Test
  void constructor_shouldIncrementLocalFileEpochOnRestart() throws Exception {
    var origTmpdir = System.setProperty("java.io.tmpdir", tempDir.toString());
    try (var ignored = mockConstruction(StringRedisTemplate.class, redisThrowing())) {
      Files.writeString(epochFilePath(), "3");
      var producer = newProducer();
      producer.sendHeartbeat();

      verify(rabbitTemplate).send(any(), any(), messageCaptor.capture());
      assertThat(messageCaptor.getValue().getMessageProperties().getHeaders())
        .containsEntry(AMQP_HEADER_HEARTBEAT_EPOCH, 4L);

      assertThat(Files.readString(epochFilePath())).isEqualTo("4");
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
    try (var ignored = mockConstruction(StringRedisTemplate.class, redisThrowing())) {
      var producer = newProducer();
      producer.sendHeartbeat();

      verify(rabbitTemplate).send(any(), any(), messageCaptor.capture());
      var epoch =
        (Long)
          messageCaptor.getValue().getMessageProperties().getHeaders().get(AMQP_HEADER_HEARTBEAT_EPOCH);
      assertThat(epoch).isPositive().isGreaterThan(1_500_000_000_000L);
    } finally {
      System.setProperty("java.io.tmpdir", origTmpdir);
    }
  }

  // ── sendHeartbeat message structure ──

  /** Verifies exchange, routing key, and every heartbeat header. */
  @Test
  void sendHeartbeat_shouldDeliverAllFields() {
    try (var ignored = mockConstruction(StringRedisTemplate.class, redisReturning(null))) {
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

      verify(rabbitTemplate).send(eq(HB_EXCHANGE), eq("heartbeat." + WORKER_ID), messageCaptor.capture());
      var headers = messageCaptor.getValue().getMessageProperties().getHeaders();

      assertThat(headers).containsEntry(AMQP_HEADER_TYPE, WorkerHeartbeatMessage.TYPE);
      assertThat(headers).containsEntry(AMQP_HEADER_NODE_ID, WORKER_ID);
      assertThat(headers).containsEntry(AMQP_HEADER_HEARTBEAT_EPOCH, 1L);
      assertThat(headers).containsEntry(AMQP_HEADER_HEARTBEAT_DV_HWM, dv);
      assertThat(headers.get(AMQP_HEADER_HEARTBEAT_LOAD)).isInstanceOf(Double.class);
      assertThat((Double) headers.get(AMQP_HEADER_HEARTBEAT_LOAD)).isBetween(0.0, 1.0);
      assertThat(headers).containsEntry(AMQP_HEADER_HEARTBEAT_READY, true);
      assertThat(headers).containsEntry("hbConfigConfirm", 3);
      assertThat(headers).containsEntry("hbConfigCool", 10);
      assertThat(headers).containsEntry("hbConfigGrace", 3);
      assertThat(headers).containsEntry("hbConfigTs", configTs);
      // confirm=3, cool=10, grace=3 → fingerprint = 31*(31*3+10)+3 = 3196
      assertThat(headers).containsEntry(AMQP_HEADER_HEARTBEAT_CONFIG_FP, 3196);
    }
  }

  /** Uptime < 3s AND no tracked keys → readyToServe = false. */
  @Test
  void sendHeartbeat_shouldReportNotReady_whenColdStartAndNoTrackedKeys() {
    try (var ignored = mockConstruction(StringRedisTemplate.class, redisReturning(null))) {
      var producer = newProducer();
      producer.sendHeartbeat();

      verify(rabbitTemplate).send(any(), any(), messageCaptor.capture());
      assertThat(messageCaptor.getValue().getMessageProperties().getHeaders())
        .containsEntry(AMQP_HEADER_HEARTBEAT_READY, false);
    }
  }

  /** Tracked keys > 0 → readyToServe = true (regardless of uptime). */
  @Test
  void sendHeartbeat_shouldReportReady_whenTrackedKeysPresent() {
    try (var ignored = mockConstruction(StringRedisTemplate.class, redisReturning(null))) {
      when(stateMachine.getTrackedKeys()).thenReturn(5);
      var producer = newProducer();
      producer.sendHeartbeat();

      verify(rabbitTemplate).send(any(), any(), messageCaptor.capture());
      assertThat(messageCaptor.getValue().getMessageProperties().getHeaders())
        .containsEntry(AMQP_HEADER_HEARTBEAT_READY, true);
    }
  }

  // ── Lifecycle ──

  /** {@code start()} schedules the heartbeat task at fixed rate. */
  @Test
  void start_shouldScheduleHeartbeatAtFixedRate() {
    var schedulerMock = mock(ScheduledExecutorService.class);
    try (var executorsMock = mockStatic(Executors.class);
         var ignored = mockConstruction(StringRedisTemplate.class, redisReturning(null))) {
      executorsMock.when(() -> Executors.newSingleThreadScheduledExecutor(any())).thenReturn(schedulerMock);

      var producer = newProducer();
      producer.start();

      verify(schedulerMock)
        .scheduleAtFixedRate(any(Runnable.class), eq(0L), eq(PING_INTERVAL_MS), eq(TimeUnit.MILLISECONDS));
    }
  }

  /** {@code stop()} shuts down the internal scheduler. */
  @Test
  void stop_shouldShutdownScheduler() {
    try (var ignored = mockConstruction(StringRedisTemplate.class, redisReturning(null))) {
      var producer = newProducer();
      producer.stop();

      var scheduler = (ScheduledExecutorService) ReflectionTestUtils.getField(producer, "scheduler");
      assertThat(scheduler).isNotNull();
      assertThat(scheduler.isShutdown()).isTrue();
    }
  }

  // ── Helpers ──

  private WorkerHeartbeatProducer newProducer() {
    return new WorkerHeartbeatProducer(
      rabbitTemplate, HB_EXCHANGE, WORKER_ID,
      stateMachine, broadcaster, configTimestampCounter, PING_INTERVAL_MS);
  }

  @SuppressWarnings("unchecked")
  private static MockedConstruction.MockInitializer<StringRedisTemplate> redisReturning(String value) {
    return (mock, ctx) -> {
      var ops = (ValueOperations<String, String>) mock(ValueOperations.class);
      when(mock.opsForValue()).thenReturn(ops);
      when(ops.get(anyString())).thenReturn(value);
    };
  }

  private static MockedConstruction.MockInitializer<StringRedisTemplate> redisThrowing() {
    return (mock, ctx) -> when(mock.opsForValue()).thenThrow(new RuntimeException("Redis unavailable"));
  }

  private Path epochFilePath() {
    return Path.of(System.getProperty("java.io.tmpdir"), "hotkey-epoch-" + WORKER_ID);
  }
}
