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

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.AMQP_HEADER_TYPE;
import static io.github.hyshmily.hotkey.constants.HotKeyConstants.AMQP_HEADER_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.hotkey.sync.WorkerMessage;
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
    props.setHeader(AMQP_HEADER_TYPE, WorkerMessage.TYPE_HOT);
    props.setHeader(AMQP_HEADER_VERSION, 10L);
    Message msg = new Message("cacheKey".getBytes(StandardCharsets.UTF_8), props);
    WorkerMessage wm = WorkerMessage.from(msg);
    assertThat(wm).isNotNull();
    assertThat(wm.cacheKey()).isEqualTo("cacheKey");
    assertThat(wm.type()).isEqualTo(WorkerMessage.TYPE_HOT);
    assertThat(wm.decisionVersion()).isEqualTo(10L);
  }

  /**
   * Verifies that an empty message body causes WorkerMessage.from() to return null.
   */
  @Test
  void from_shouldReturnNullForEmptyBody() {
    Message msg = new Message(new byte[0], new MessageProperties());
    assertThat(WorkerMessage.from(msg)).isNull();
  }

  /**
   * Verifies that from() uses default version 0 when the version header is missing.
   */
  @Test
  void from_shouldUseDefaultVersionWhenMissing() {
    MessageProperties props = new MessageProperties();
    props.setHeader(AMQP_HEADER_TYPE, WorkerMessage.TYPE_COOL);
    Message msg = new Message("key".getBytes(StandardCharsets.UTF_8), props);
    WorkerMessage wm = WorkerMessage.from(msg);
    assertThat(wm.decisionVersion()).isZero();
  }

  /**
   * Verifies the WorkerMessage type constants have the expected string values.
   */
  @Test
  void shouldHaveExpectedTypeConstants() {
    assertThat(WorkerMessage.TYPE_HOT).isEqualTo("HOT");
    assertThat(WorkerMessage.TYPE_COOL).isEqualTo("COOL");
  }
}
