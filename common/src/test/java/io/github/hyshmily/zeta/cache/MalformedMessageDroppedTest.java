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
package io.github.hyshmily.zeta.cache;

import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.HEADER_TYPE;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.HEADER_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.sync.local.SyncType;
import io.github.hyshmily.zeta.sync.worker.WorkerDecision;
import io.github.hyshmily.zeta.sync.worker.WorkerType;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * design decisions: version stamps: a message without a usable stamp is unorderable and must be
 * dropped at the wire boundary instead of being treated as "version zero".
 */
class MalformedMessageDroppedTest {

  @Test
  @DisplayName("a sync message without a positive version never reaches a receiver")
  void syncNonPositiveVersion_droppedAtParseBoundary() {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, SyncType.INVALIDATE.code());
    byte[] body = "k".getBytes(StandardCharsets.UTF_8);

    assertThat(SyncMessage.from(new Message(body, props))).as("missing header").isNull();
    props.setHeader(HEADER_VERSION, 0L);
    assertThat(SyncMessage.from(new Message(body, props))).as("zero").isNull();
    props.setHeader(HEADER_VERSION, -5L);
    assertThat(SyncMessage.from(new Message(body, props))).as("negative").isNull();
  }

  @Test
  @DisplayName("a worker decision without a positive version never reaches a handler")
  void workerNonPositiveVersion_droppedAtParseBoundary() {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, WorkerType.HOT.code());
    byte[] body = "k".getBytes(StandardCharsets.UTF_8);

    assertThat(WorkerDecision.from(new Message(body, props))).as("missing header").isNull();
    props.setHeader(HEADER_VERSION, 0L);
    assertThat(WorkerDecision.from(new Message(body, props))).as("zero").isNull();
    props.setHeader(HEADER_VERSION, -1L);
    assertThat(WorkerDecision.from(new Message(body, props))).as("negative").isNull();
  }
}
