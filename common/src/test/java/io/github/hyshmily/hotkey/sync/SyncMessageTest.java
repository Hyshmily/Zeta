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

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED;
import static io.github.hyshmily.hotkey.constants.HotKeyConstants.AMQP_HEADER_TYPE;
import static io.github.hyshmily.hotkey.constants.HotKeyConstants.AMQP_HEADER_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.hotkey.sync.SyncMessage;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * Tests for {@link SyncMessage} parsing from AMQP messages, covering valid messages, empty/blank
 * body handling, default header values, and type constants.
 */
class SyncMessageTest {

  /**
   * Verifies that a valid AMQP message with type, version, and degraded headers is parsed correctly.
   */
  @Test
  void from_shouldParseValidMessage() {
    MessageProperties props = new MessageProperties();
    props.setHeader(AMQP_HEADER_TYPE, SyncMessage.TYPE_REFRESH);
    props.setHeader(AMQP_HEADER_VERSION, 42L);
    props.setHeader(AMQP_HEADER_IS_VERSION_DEGRADED, true);
    Message msg = new Message("cacheKey".getBytes(StandardCharsets.UTF_8), props);
    SyncMessage sm = SyncMessage.from(msg);
    assertThat(sm).isNotNull();
    assertThat(sm.cacheKey()).isEqualTo("cacheKey");
    assertThat(sm.type()).isEqualTo(SyncMessage.TYPE_REFRESH);
    assertThat(sm.version()).isEqualTo(42L);
    assertThat(sm.isVersionDegraded()).isTrue();
  }

  /**
   * Verifies that an empty message body causes from() to return null.
   */
  @Test
  void from_shouldReturnNullForEmptyBody() {
    Message msg = new Message(new byte[0], new MessageProperties());
    assertThat(SyncMessage.from(msg)).isNull();
  }

  /**
   * Verifies that a message body with only whitespace causes from() to return null.
   */
  @Test
  void from_shouldReturnNullForBlankKey() {
    MessageProperties props = new MessageProperties();
    props.setHeader(AMQP_HEADER_TYPE, SyncMessage.TYPE_REFRESH);
    Message msg = new Message("   ".getBytes(StandardCharsets.UTF_8), props);
    assertThat(SyncMessage.from(msg)).isNull();
  }

  /**
   * Verifies that from() uses default values (version 0, not degraded) when headers are missing.
   */
  @Test
  void from_shouldUseDefaultsForMissingHeaders() {
    Message msg = new Message("key".getBytes(StandardCharsets.UTF_8), new MessageProperties());
    SyncMessage sm = SyncMessage.from(msg);
    assertThat(sm.version()).isZero();
    assertThat(sm.isVersionDegraded()).isFalse();
  }

  /**
   * Verifies the SyncMessage type constants have the expected string values.
   */
  @Test
  void shouldHaveExpectedTypeConstants() {
    assertThat(SyncMessage.TYPE_REFRESH).isEqualTo("REFRESH");
    assertThat(SyncMessage.TYPE_INVALIDATE).isEqualTo("INVALIDATE");
  }
}
