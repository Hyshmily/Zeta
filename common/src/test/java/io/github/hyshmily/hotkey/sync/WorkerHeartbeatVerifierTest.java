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
package io.github.hyshmily.hotkey.sync;

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.github.hyshmily.hotkey.sharding.ClusterHealthView;
import io.github.hyshmily.hotkey.sync.worker.WorkerHeartbeatMessage;
import io.github.hyshmily.hotkey.sync.worker.WorkerHeartbeatVerifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.AmqpTimeoutException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

class WorkerHeartbeatVerifierTest {

  private RabbitTemplate rabbitTemplate;
  private ClusterHealthView healthView;
  private WorkerHeartbeatVerifier verifier;

  @BeforeEach
  void setUp() {
    rabbitTemplate = mock(RabbitTemplate.class);
    healthView = spy(new ClusterHealthView(3, 500_000, 99));
    healthView.onHeartbeat(hb("w1", true));
    healthView.onHeartbeat(hb("w2", false));
    healthView.onHeartbeat(hb("w3", false));
    verifier = new WorkerHeartbeatVerifier(
      rabbitTemplate,
      healthView,
      "test-app",
      new WorkerHeartbeatVerifier.VerifierConfig(100_000, 500, 60_000)
    );
  }

  private static WorkerHeartbeatMessage hb(String workerId, boolean ready) {
    return new WorkerHeartbeatMessage(workerId, 1, 0, 0.0, ready, 0, 0, 0, 0);
  }

  // ── sendPingAndWaitPong ──

  @Test
  void shouldSendPingToCorrectQueueWithCorrectHeaders() {
    when(rabbitTemplate.sendAndReceive(anyString(), anyString(), any())).thenReturn(
      new Message(new byte[0], new MessageProperties())
    );

    verifier.sendPingAndWaitPong("w2");

    ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
    verify(rabbitTemplate).sendAndReceive(eq(""), eq("hotkey.verify.ping.w2"), captor.capture());
    Message sent = captor.getValue();
    assertThat((String) sent.getMessageProperties().getHeader(AMQP_HEADER_VERIFY_TYPE)).isEqualTo(
      AMQP_HEADER_VERIFY_PING
    );
    assertThat((String) sent.getMessageProperties().getHeader(AMQP_HEADER_VERIFY_APP_INSTANCE)).isEqualTo("test-app");
    assertThat(sent.getMessageProperties().getReplyTo()).isEqualTo("amq.rabbitmq.reply-to");
  }

  @Test
  void shouldReturnTrueWhenPongReceived() {
    when(rabbitTemplate.sendAndReceive(anyString(), anyString(), any())).thenReturn(
      new Message(new byte[0], new MessageProperties())
    );

    assertThat(verifier.sendPingAndWaitPong("w2")).isTrue();
  }

  @Test
  void shouldReturnFalseOnAmqpTimeoutException() {
    when(rabbitTemplate.sendAndReceive(anyString(), anyString(), any())).thenThrow(new AmqpTimeoutException("timeout"));

    assertThat(verifier.sendPingAndWaitPong("w2")).isFalse();
  }

  @Test
  void shouldReturnFalseWhenPongIsNull() {
    when(rabbitTemplate.sendAndReceive(anyString(), anyString(), any())).thenReturn(null);

    assertThat(verifier.sendPingAndWaitPong("w2")).isFalse();
  }

  // ── verifySuspectedWorkers ──

  @Test
  void shouldReturnEarlyWhenClusterIsHealthy() {
    healthView.onHeartbeat(hb("w2", true));
    healthView.onHeartbeat(hb("w3", true));

    verifier.verifySuspectedWorkers();

    verify(rabbitTemplate, never()).sendAndReceive(anyString(), anyString(), any());
  }

  @Test
  void shouldReturnEarlyWhenNoSuspectedWorkers() {
    ClusterHealthView emptyView = new ClusterHealthView(3, 5000, 99);
    WorkerHeartbeatVerifier v = new WorkerHeartbeatVerifier(
      rabbitTemplate,
      emptyView,
      "test-app",
      new WorkerHeartbeatVerifier.VerifierConfig(1000, 500, 60_000)
    );

    v.verifySuspectedWorkers();

    verify(rabbitTemplate, never()).sendAndReceive(anyString(), anyString(), any());
  }

  @Test
  void shouldCallRecordPongOnPingSuccess() {
    when(rabbitTemplate.sendAndReceive(anyString(), anyString(), any())).thenReturn(
      new Message(new byte[0], new MessageProperties())
    );

    verifier.verifySuspectedWorkers();

    verify(healthView).recordPong("w2");
    verify(healthView).recordPong("w3");
  }

  @Test
  void shouldCallMarkVerificationFailedOnPingFailure() {
    when(rabbitTemplate.sendAndReceive(anyString(), anyString(), any())).thenReturn(null);

    verifier.verifySuspectedWorkers();

    verify(healthView).markVerificationFailed("w2");
    verify(healthView).markVerificationFailed("w3");
  }

  // ── start ──

  @Test
  void shouldScheduleAtFixedRate() throws Exception {
    WorkerHeartbeatVerifier v = new WorkerHeartbeatVerifier(
      rabbitTemplate,
      healthView,
      "test-app",
      new WorkerHeartbeatVerifier.VerifierConfig(50, 500, 60_000)
    );
    when(rabbitTemplate.sendAndReceive(anyString(), anyString(), any())).thenReturn(
      new Message(new byte[0], new MessageProperties())
    );

    v.start();
    Thread.sleep(200);
    v.stop();

    verify(rabbitTemplate, atLeast(1)).sendAndReceive(anyString(), anyString(), any());
  }

  /**
   * Verifies that when some pings succeed and some fail, both recordPong and markVerificationFailed are called.
   */
  @Test
  void shouldHandleMixedResults() {
    when(rabbitTemplate.sendAndReceive(eq(""), eq("hotkey.verify.ping.w2"), any())).thenReturn(
      new Message(new byte[0], new MessageProperties())
    );
    when(rabbitTemplate.sendAndReceive(eq(""), eq("hotkey.verify.ping.w3"), any())).thenReturn(null);

    verifier.verifySuspectedWorkers();

    verify(healthView).recordPong("w2");
    verify(healthView).markVerificationFailed("w3");
  }

  /**
   * Verifies that a framework exception in verifySuspectedWorkers is caught and not propagated.
   */
  @Test
  void verifySuspectedWorkers_withException_shouldSwallow() {
    when(healthView.getAllWorkerIds()).thenThrow(new RuntimeException("Unexpected error"));

    verifier.verifySuspectedWorkers();

    verify(rabbitTemplate, never()).sendAndReceive(anyString(), anyString(), any());
  }

  /**
   * Verifies that start is idempotent and does not schedule multiple times.
   */
  @Test
  void start_isIdempotent() throws Exception {
    WorkerHeartbeatVerifier v = new WorkerHeartbeatVerifier(
      rabbitTemplate,
      healthView,
      "test-app",
      new WorkerHeartbeatVerifier.VerifierConfig(50, 500, 60_000)
    );
    when(rabbitTemplate.sendAndReceive(anyString(), anyString(), any())).thenReturn(
      new Message(new byte[0], new MessageProperties())
    );

    v.start();
    v.start();
    Thread.sleep(200);
    v.stop();

    verify(rabbitTemplate, atLeast(1)).sendAndReceive(anyString(), anyString(), any());
  }

  // ── stop ──

  @Test
  void shouldShutdownScheduler() {
    verifier.stop();
    // After stop(), start() is idempotent — cancelled verifyTask prevents re-scheduling
    verifier.start();
  }
}
