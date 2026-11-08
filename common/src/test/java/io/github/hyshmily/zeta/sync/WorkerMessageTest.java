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

import io.github.hyshmily.zeta.sync.worker.WorkerDecisionType;
import io.github.hyshmily.zeta.sync.worker.WorkerMessage;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * Tests for {@link WorkerMessage} parsing from AMQP messages, covering HOT/COOL type parsing,
 * empty body rejection, default version handling, and type constants.
 */
class WorkerMessageTest {

  /**
   * Verifies that a valid AMQP message with HOT type and version header is parsed correctly.
   */
  @Test
  void from_shouldParseHotMessage() {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, WorkerDecisionType.HOT.code());
    props.setHeader(HEADER_VERSION, 10L);
    props.setHeader(HEADER_NODE_ID, "worker-1");
    Message msg = new Message("cacheKey".getBytes(StandardCharsets.UTF_8), props);
    WorkerMessage workerMessage = WorkerMessage.from(msg);
    assertThat(workerMessage).isNotNull();
    assertThat(workerMessage.cacheKey()).isEqualTo("cacheKey");
    assertThat(workerMessage.type()).isEqualTo(WorkerDecisionType.HOT);
    assertThat(workerMessage.stamp().decisionVersion()).isEqualTo(10L);
  }

  /** The wire type and the lease kind are one contract. */
  @Test
  void from_coolMessage_shouldCarryCoolKind() {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, WorkerDecisionType.COOL.code());
    props.setHeader(HEADER_VERSION, 10L);
    Message msg = new Message("key".getBytes(StandardCharsets.UTF_8), props);

    WorkerMessage workerMessage = WorkerMessage.from(msg);

    assertThat(workerMessage).isNotNull();
    assertThat(workerMessage.stamp().isCool()).isTrue();
  }

  /**
   * Verifies that an empty message body causes WorkerMessage.from() to return null.
   */
  @Test
  void from_shouldReturnNullForEmptyBody() {
    Message msg = new Message(new byte[0], new MessageProperties());
    assertThat(WorkerMessage.from(msg)).isNull();
  }

  /** A blank body is not a cache key; it would reach the loader as an unservable key. */
  @Test
  void from_shouldReturnNullForBlankKey() {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, WorkerDecisionType.HOT.code());
    Message msg = new Message("   ".getBytes(StandardCharsets.UTF_8), props);
    assertThat(WorkerMessage.from(msg)).isNull();
  }

  /**
   * A decision without a positive version is unorderable: the wire boundary drops it
   * before it can create an entry, so the handler never sees a zero stamp.
   */
  @Test
  void from_shouldReturnNullWhenVersionMissingOrNonPositive() {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, WorkerDecisionType.COOL.code());
    Message missing = new Message("key".getBytes(StandardCharsets.UTF_8), props);
    assertThat(WorkerMessage.from(missing)).isNull();

    props.setHeader(HEADER_VERSION, 0L);
    assertThat(WorkerMessage.from(missing)).isNull();
    props.setHeader(HEADER_VERSION, -1L);
    assertThat(WorkerMessage.from(missing)).isNull();
  }

  /**
   * Verifies the WorkerMessage type constants have the expected string values.
   */
  @Test
  void shouldHaveExpectedTypeConstants() {
    assertThat(WorkerDecisionType.HOT.code()).isEqualTo("HOT");
    assertThat(WorkerDecisionType.COOL.code()).isEqualTo("COOL");
  }

  /**
   * Verifies that the nodeId header is parsed correctly and defaults to null when missing.
   */
  @Test
  void from_withNodeId_shouldParse() {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, WorkerDecisionType.HOT.code());
    props.setHeader(HEADER_VERSION, 10L);
    props.setHeader(HEADER_NODE_ID, "worker-1");
    Message msg = new Message("key".getBytes(StandardCharsets.UTF_8), props);
    WorkerMessage workerMessage = WorkerMessage.from(msg);
    assertThat(workerMessage.stamp().decisionNodeId()).isEqualTo("worker-1");
  }

  /**
   * Verifies that a missing nodeId header defaults to null.
   */
  @Test
  void from_withMissingNodeId_shouldDefault() {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, WorkerDecisionType.HOT.code());
    props.setHeader(HEADER_VERSION, 10L);
    Message msg = new Message("key".getBytes(StandardCharsets.UTF_8), props);
    WorkerMessage workerMessage = WorkerMessage.from(msg);
    assertThat(workerMessage.stamp().decisionNodeId()).isNull();
  }

  /**
   * Verifies that a version header stored as Integer (not Long) is parsed correctly.
   */
  @Test
  void from_withVersionAsInteger_shouldHandle() {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, WorkerDecisionType.HOT.code());
    props.setHeader(HEADER_VERSION, 42);
    props.setHeader(HEADER_NODE_ID, "worker-1");
    Message msg = new Message("key".getBytes(StandardCharsets.UTF_8), props);
    WorkerMessage workerMessage = WorkerMessage.from(msg);
    assertThat(workerMessage.stamp().decisionVersion()).isEqualTo(42L);
  }

  // ── P0-2: Epoch ──

  /**
   * Verifies that the epoch header is parsed correctly.
   */
  @Test
  void from_withEpoch_shouldParse() {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, WorkerDecisionType.HOT.code());
    props.setHeader(HEADER_VERSION, 10L);
    props.setHeader(HEADER_EPOCH, 3L);
    props.setHeader(HEADER_NODE_ID, "worker-1");
    Message msg = new Message("key".getBytes(StandardCharsets.UTF_8), props);
    WorkerMessage workerMessage = WorkerMessage.from(msg);
    assertThat(workerMessage.stamp().decisionEpoch()).isEqualTo(3L);
    assertThat(workerMessage.stamp().decisionNodeId()).isEqualTo("worker-1");
  }

  /**
   * Verifies that missing epoch header defaults to 0L.
   */
  @Test
  void from_withMissingEpoch_shouldDefaultToZero() {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, WorkerDecisionType.HOT.code());
    props.setHeader(HEADER_VERSION, 10L);
    props.setHeader(HEADER_NODE_ID, "worker-1");
    Message msg = new Message("key".getBytes(StandardCharsets.UTF_8), props);
    WorkerMessage workerMessage = WorkerMessage.from(msg);
    assertThat(workerMessage.stamp().decisionEpoch()).isZero();
  }

  /**
   * Verifies that epoch header stored as Integer is parsed correctly.
   */
  @Test
  void from_withEpochAsInteger_shouldHandle() {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, WorkerDecisionType.HOT.code());
    props.setHeader(HEADER_VERSION, 10L);
    props.setHeader(HEADER_EPOCH, 2);
    props.setHeader(HEADER_NODE_ID, "worker-1");
    Message msg = new Message("key".getBytes(StandardCharsets.UTF_8), props);
    WorkerMessage workerMessage = WorkerMessage.from(msg);
    assertThat(workerMessage.stamp().decisionEpoch()).isEqualTo(2L);
  }

  /** The parser switch is exhaustive over the enum's wire codes; unknown values are null. */
  @Test
  void decisionTypeRoundTripsEveryWireCode() {
    for (WorkerDecisionType type : WorkerDecisionType.values()) {
      assertThat(WorkerDecisionType.parse(type.code())).isSameAs(type);
    }
    assertThat(WorkerDecisionType.parse("UNKNOWN")).isNull();
    assertThat(WorkerDecisionType.parse(3)).isNull();
    assertThat(WorkerDecisionType.parse(null)).isNull();
  }

  /** HOT needs the start-jitter loader park; COOL is the budget-exempt control signal. */
  @Test
  void decisionTypeFlagsMatchTheAdmissionContract() {
    assertThat(WorkerDecisionType.HOT.isControl()).isFalse();
    assertThat(WorkerDecisionType.HOT.needsJitter()).isTrue();
    assertThat(WorkerDecisionType.COOL.isControl()).isTrue();
    assertThat(WorkerDecisionType.COOL.needsJitter()).isFalse();
  }
}
