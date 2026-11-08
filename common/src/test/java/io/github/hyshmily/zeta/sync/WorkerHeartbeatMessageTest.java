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
package io.github.hyshmily.zeta.sync;

import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatMessage;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

class WorkerHeartbeatMessageTest {

  @Test
  void toMessage_shouldSetAllHeaders() {
    WorkerHeartbeatMessage hb = new WorkerHeartbeatMessage(42L, "worker-1", 5L, true, null);
    Message msg = hb.toMessage();
    var h = msg.getMessageProperties();
    assertThat((String) h.getHeader(HEADER_TYPE)).isEqualTo(WorkerHeartbeatMessage.TYPE);
    assertThat(((Number) h.getHeader(HEADER_MESSAGE_ID)).longValue()).isEqualTo(42L);
    assertThat(((Number) h.getHeader(HEADER_HEARTBEAT_EPOCH)).longValue()).isEqualTo(5L);
    assertThat((Boolean) h.getHeader(HEADER_HEARTBEAT_READY)).isTrue();
    assertThat((String) h.getHeader(HEADER_NODE_ID)).isEqualTo("worker-1");
  }

  @Test
  void toMessage_bodyShouldBeWorkerIdBytes() {
    WorkerHeartbeatMessage hb = new WorkerHeartbeatMessage(0L, "worker-x", 1L, false, null);
    Message msg = hb.toMessage();
    assertThat(new String(msg.getBody(), StandardCharsets.UTF_8)).isEqualTo("worker-x");
  }

  @Test
  void from_shouldRoundTrip() {
    WorkerHeartbeatMessage original = new WorkerHeartbeatMessage(7L, "w-42", 7L, true, null);
    assertThat(WorkerHeartbeatMessage.from(original.toMessage())).isEqualTo(original);
  }

  @Test
  void anAppNameHeaderIsStampedAndRoundTripped() {
    WorkerHeartbeatMessage original = new WorkerHeartbeatMessage(7L, "w-42", 7L, true, "tenant-a");
    Message message = original.toMessage();

    assertThat((String) message.getMessageProperties().getHeader(HEADER_APP_NAME)).isEqualTo("tenant-a");
    assertThat(WorkerHeartbeatMessage.from(message)).isEqualTo(original);
  }

  @Test
  void anAppNameHeaderIsOptionalForOlderWorkers() {
    WorkerHeartbeatMessage original = new WorkerHeartbeatMessage(7L, "w-42", 7L, true, null);
    Message message = original.toMessage();

    assertThat((Object) message.getMessageProperties().getHeader(HEADER_APP_NAME)).isNull();
    assertThat((String) WorkerHeartbeatMessage.from(message).appName()).isNull();
  }

  @Test
  void from_nullProperties_shouldReturnNull() {
    Message msg = mock(Message.class);
    when(msg.getMessageProperties()).thenReturn(null);
    assertThat(WorkerHeartbeatMessage.from(msg)).isNull();
  }

  @Test
  void from_wrongType_shouldReturnNull() {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, "OTHER");
    Message msg = new Message("body".getBytes(StandardCharsets.UTF_8), props);
    assertThat(WorkerHeartbeatMessage.from(msg)).isNull();
  }

  @Test
  void from_missingHeaders_shouldUseDefaults() {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, WorkerHeartbeatMessage.TYPE);
    Message msg = new Message("any".getBytes(StandardCharsets.UTF_8), props);
    WorkerHeartbeatMessage hb = WorkerHeartbeatMessage.from(msg);
    assertThat(hb.id()).isZero();
    assertThat(hb.workerId()).as("the body is the identity fallback").isEqualTo("any");
    assertThat(hb.epoch()).isZero();
    assertThat(hb.readyToServe()).isFalse();
  }

  @Test
  void from_blankWorkerIdAndBlankBody_shouldReturnNull() {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, WorkerHeartbeatMessage.TYPE);
    props.setHeader(HEADER_NODE_ID, "   ");
    Message msg = new Message(new byte[0], props);
    assertThat(WorkerHeartbeatMessage.from(msg)).isNull();
  }

  @Test
  void from_wrongHeaderTypes_shouldDefaultSafely() {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, WorkerHeartbeatMessage.TYPE);
    props.setHeader(HEADER_NODE_ID, 123);
    props.setHeader(HEADER_HEARTBEAT_EPOCH, "not-a-number");
    props.setHeader(HEADER_HEARTBEAT_READY, "not-boolean");
    Message msg = new Message("body".getBytes(StandardCharsets.UTF_8), props);
    WorkerHeartbeatMessage hb = WorkerHeartbeatMessage.from(msg);
    assertThat(hb.workerId()).as("a non-String header falls back to the body").isEqualTo("body");
    assertThat(hb.epoch()).isZero();
    assertThat(hb.readyToServe()).isFalse();
  }

  @Test
  void typeConstant_shouldBeWorkHb() {
    assertThat(WorkerHeartbeatMessage.TYPE).isEqualTo("WORKER_HB");
  }

  @Test
  void constructor_shouldSetAllFields() {
    WorkerHeartbeatMessage hb = new WorkerHeartbeatMessage(3L, "w-1", 2L, true, null);
    assertThat(hb.id()).isEqualTo(3L);
    assertThat(hb.workerId()).isEqualTo("w-1");
    assertThat(hb.epoch()).isEqualTo(2L);
    assertThat(hb.readyToServe()).isTrue();
  }
}
