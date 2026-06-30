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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.hyshmily.hotkey.sync.worker.WorkerHeartbeatMessage;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

class WorkerHeartbeatMessageTest {

  @Test
  void toMessage_shouldSetAllHeaders() {
    WorkerHeartbeatMessage hb = new WorkerHeartbeatMessage("worker-1", 5L, 42L, 0.75, true, 3, 10, 2, 9999L);
    Message msg = hb.toMessage();
    var h = msg.getMessageProperties();
    assertThat((String) h.getHeader(AMQP_HEADER_TYPE)).isEqualTo(WorkerHeartbeatMessage.TYPE);
    assertThat(((Number) h.getHeader(AMQP_HEADER_HEARTBEAT_EPOCH)).longValue()).isEqualTo(5L);
    assertThat(((Number) h.getHeader(AMQP_HEADER_HEARTBEAT_DV_HWM)).longValue()).isEqualTo(42L);
    assertThat(((Number) h.getHeader(AMQP_HEADER_HEARTBEAT_LOAD)).doubleValue()).isEqualTo(0.75);
    assertThat((Boolean) h.getHeader(AMQP_HEADER_HEARTBEAT_READY)).isTrue();
    assertThat((String) h.getHeader(AMQP_HEADER_NODE_ID)).isEqualTo("worker-1");
    assertThat(((Number) h.getHeader(AMQP_HEADER_HEARTBEAT_CONFIG_CONFIRM)).intValue()).isEqualTo(3);
    assertThat(((Number) h.getHeader(AMQP_HEADER_HEARTBEAT_CONFIG_COOL)).intValue()).isEqualTo(10);
    assertThat(((Number) h.getHeader(AMQP_HEADER_HEARTBEAT_CONFIG_GRACE)).intValue()).isEqualTo(2);
    assertThat(((Number) h.getHeader(AMQP_HEADER_HEARTBEAT_CONFIG_TIMESTAMP)).longValue()).isEqualTo(9999L);
  }

  @Test
  void toMessage_bodyShouldBeWorkerIdBytes() {
    WorkerHeartbeatMessage hb = new WorkerHeartbeatMessage("worker-x", 1L, 0L, 0.0, false, 0, 0, 0, 0L);
    Message msg = hb.toMessage();
    assertThat(new String(msg.getBody(), StandardCharsets.UTF_8)).isEqualTo("worker-x");
  }

  @Test
  void from_shouldRoundTrip() {
    WorkerHeartbeatMessage original = new WorkerHeartbeatMessage("w-42", 7L, 99L, 0.5, true, 5, 8, 1, 7777L);
    Message msg = original.toMessage();
    WorkerHeartbeatMessage restored = WorkerHeartbeatMessage.from(msg);
    assertThat(restored).isEqualTo(original);
  }

  @Test
  void from_nullMessage_shouldThrow() {
    org.junit.jupiter.api.Assertions.assertThrows(NullPointerException.class, () -> WorkerHeartbeatMessage.from(null));
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
    props.setHeader(AMQP_HEADER_TYPE, "NOT_HB");
    Message msg = new Message("body".getBytes(StandardCharsets.UTF_8), props);
    assertThat(WorkerHeartbeatMessage.from(msg)).isNull();
  }

  @Test
  void from_missingHeaders_shouldUseDefaults() {
    MessageProperties props = new MessageProperties();
    props.setHeader(AMQP_HEADER_TYPE, WorkerHeartbeatMessage.TYPE);
    Message msg = new Message("any".getBytes(StandardCharsets.UTF_8), props);
    WorkerHeartbeatMessage hb = WorkerHeartbeatMessage.from(msg);
    assertThat(hb.workerId()).isEmpty();
    assertThat(hb.epoch()).isZero();
    assertThat(hb.decisionVersionHwm()).isZero();
    assertThat(hb.loadFactor()).isZero();
    assertThat(hb.readyToServe()).isFalse();
    assertThat(hb.configConfirmCount()).isZero();
    assertThat(hb.configCoolCount()).isZero();
    assertThat(hb.configGraceCount()).isZero();
    assertThat(hb.configTimestamp()).isZero();
  }

  @Test
  void from_wrongHeaderTypes_shouldDefaultSafely() {
    MessageProperties props = new MessageProperties();
    props.setHeader(AMQP_HEADER_TYPE, WorkerHeartbeatMessage.TYPE);
    props.setHeader(AMQP_HEADER_NODE_ID, 123);
    props.setHeader(AMQP_HEADER_HEARTBEAT_EPOCH, "not-a-number");
    props.setHeader(AMQP_HEADER_HEARTBEAT_DV_HWM, "bad");
    props.setHeader(AMQP_HEADER_HEARTBEAT_LOAD, "bad");
    props.setHeader(AMQP_HEADER_HEARTBEAT_READY, "not-boolean");
    props.setHeader(AMQP_HEADER_HEARTBEAT_CONFIG_CONFIRM, "bad");
    props.setHeader(AMQP_HEADER_HEARTBEAT_CONFIG_COOL, "bad");
    props.setHeader(AMQP_HEADER_HEARTBEAT_CONFIG_GRACE, "bad");
    props.setHeader(AMQP_HEADER_HEARTBEAT_CONFIG_TIMESTAMP, "bad");
    Message msg = new Message("body".getBytes(StandardCharsets.UTF_8), props);
    WorkerHeartbeatMessage hb = WorkerHeartbeatMessage.from(msg);
    assertThat(hb.workerId()).isEmpty();
    assertThat(hb.epoch()).isZero();
    assertThat(hb.decisionVersionHwm()).isZero();
    assertThat(hb.loadFactor()).isZero();
    assertThat(hb.readyToServe()).isFalse();
    assertThat(hb.configConfirmCount()).isZero();
    assertThat(hb.configCoolCount()).isZero();
    assertThat(hb.configGraceCount()).isZero();
    assertThat(hb.configTimestamp()).isZero();
  }

  @Test
  void typeConstant_shouldBeWorkHb() {
    assertThat(WorkerHeartbeatMessage.TYPE).isEqualTo("WORKER_HB");
  }

  @Test
  void constructor_shouldSetAllFields() {
    WorkerHeartbeatMessage hb = new WorkerHeartbeatMessage("w-1", 2L, 4L, 0.5, true, 6, 7, 8, 9L);
    assertThat(hb.workerId()).isEqualTo("w-1");
    assertThat(hb.epoch()).isEqualTo(2L);
    assertThat(hb.decisionVersionHwm()).isEqualTo(4L);
    assertThat(hb.loadFactor()).isEqualTo(0.5);
    assertThat(hb.readyToServe()).isTrue();
    assertThat(hb.configConfirmCount()).isEqualTo(6);
    assertThat(hb.configCoolCount()).isEqualTo(7);
    assertThat(hb.configGraceCount()).isEqualTo(8);
    assertThat(hb.configTimestamp()).isEqualTo(9L);
  }
}
