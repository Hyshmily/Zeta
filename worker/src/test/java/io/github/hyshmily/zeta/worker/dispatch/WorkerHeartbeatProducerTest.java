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

import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.HEADER_APP_NAME;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.HEADER_HEARTBEAT_EPOCH;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.HEADER_HEARTBEAT_READY;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Routing.KEY_HEARTBEAT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import io.github.hyshmily.zeta.worker.detection.lifecycle.HotKeyRegistry;
import io.github.hyshmily.zeta.worker.dispatch.connection.WorkerHeartbeatProducer;
import io.github.hyshmily.zeta.worker.dispatch.connection.WorkerIdentity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/** Heartbeat payload: epoch and readiness. */
@ExtendWith(MockitoExtension.class)
class WorkerHeartbeatProducerTest {

  @Mock
  private RabbitTemplate rabbitTemplate;

  @Mock
  private HotKeyRegistry registry;

  @Captor
  private ArgumentCaptor<Message> messageCaptor;

  @Test
  void sendsEpochAndReadiness() {
    WorkerHeartbeatProducer producer = new WorkerHeartbeatProducer(
      rabbitTemplate,
      "hb.exchange",
      new WorkerIdentity("worker-1", 7L),
      registry,
      0L,
      "tenant-a"
    );

    producer.sendHeartbeat();

    verify(rabbitTemplate).send(eq("hb.exchange"), eq(KEY_HEARTBEAT + "worker-1"), messageCaptor.capture());
    Message message = messageCaptor.getValue();
    assertThat(message.getMessageProperties().getHeaders().get(HEADER_HEARTBEAT_EPOCH)).isEqualTo(7L);
    assertThat(message.getMessageProperties().getHeaders().get(HEADER_HEARTBEAT_READY)).isEqualTo(true);
    assertThat(message.getMessageProperties().getHeaders().get(HEADER_APP_NAME)).isEqualTo("tenant-a");
    assertThat(new String(message.getBody())).isEqualTo("worker-1");
  }

  @Test
  void reportsNotReadyUntilTheGracePeriodElapsesOrAKeyIsHot() {
    WorkerHeartbeatProducer producer = new WorkerHeartbeatProducer(
      rabbitTemplate,
      "hb.exchange",
      new WorkerIdentity("worker-1", 7L),
      registry,
      60_000L,
      null
    );

    producer.sendHeartbeat();

    verify(rabbitTemplate).send(eq("hb.exchange"), eq(KEY_HEARTBEAT + "worker-1"), messageCaptor.capture());
    assertThat(messageCaptor.getValue().getMessageProperties().getHeaders().get(HEADER_HEARTBEAT_READY)).isEqualTo(
      false
    );
  }
}
