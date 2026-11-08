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

import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.sync.local.SyncType;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * Tests for {@link SyncMessage} parsing from AMQP messages, covering valid messages, empty/blank
 * body handling, default header values, and the wire-type codes.
 */
class SyncMessageTest {

  /**
   * Verifies that a valid AMQP message with type and version headers is parsed correctly.
   */
  @Test
  void from_shouldParseValidMessage() {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, SyncType.REFRESH.code());
    props.setHeader(HEADER_VERSION, 42L);
    Message msg = new Message("cacheKey".getBytes(StandardCharsets.UTF_8), props);
    SyncMessage syncMessage = SyncMessage.from(msg);
    assertThat(syncMessage).isNotNull();
    assertThat(syncMessage.cacheKey()).isEqualTo("cacheKey");
    assertThat(syncMessage.type()).isEqualTo(SyncType.REFRESH);
    assertThat(syncMessage.version()).isEqualTo(42L);
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
    props.setHeader(HEADER_TYPE, SyncType.REFRESH.code());
    Message msg = new Message("   ".getBytes(StandardCharsets.UTF_8), props);
    assertThat(SyncMessage.from(msg)).isNull();
  }

  /**
   * A single-key sync message without a positive data version is unorderable: the wire
   * boundary drops it, so no receiver has to re-check the stamp.
   */
  @Test
  void from_shouldReturnNullWhenVersionMissingOrNonPositive() {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, SyncType.REFRESH.code());
    Message msg = new Message("key".getBytes(StandardCharsets.UTF_8), props);
    assertThat(SyncMessage.from(msg)).isNull();

    props.setHeader(HEADER_VERSION, 0L);
    assertThat(SyncMessage.from(msg)).isNull();
    props.setHeader(HEADER_VERSION, -3L);
    assertThat(SyncMessage.from(msg)).isNull();
  }

  /**
   * A batch derives its version from its entries, so its own header may be absent; the
   * clear-all body is the shared marker and is never decoded into a second String.
   */
  @Test
  void from_batchWithoutHeaderVersionAndClearAllMarker_shouldParse() {
    byte[] body = SyncMessage.encodeBatchBody(List.of(new SyncMessage.Entry("a", 7L)));
    MessageProperties batchProps = new MessageProperties();
    batchProps.setHeader(HEADER_TYPE, SyncType.INVALIDATE_BATCH.code());
    SyncMessage batch = SyncMessage.from(new Message(body, batchProps));
    assertThat(batch).isNotNull();
    assertThat(batch.version()).isEqualTo(7L);

    MessageProperties clearProps = new MessageProperties();
    clearProps.setHeader(HEADER_TYPE, SyncType.INVALIDATE_ALL.code());
    clearProps.setHeader(HEADER_VERSION, 11L);
    SyncMessage clear = SyncMessage.from(new Message("*".getBytes(StandardCharsets.UTF_8), clearProps));
    assertThat(clear).isNotNull();
    assertThat(clear.cacheKey()).isSameAs(SyncMessage.CLEAR_ALL_BODY);
    assertThat(clear.version()).isEqualTo(11L);
  }

  /**
   * Verifies that a version header stored as Integer (not Long) is parsed correctly.
   */
  @Test
  void from_withVersionHeaderAsInteger_shouldHandleCorrectly() {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, SyncType.REFRESH.code());
    props.setHeader(HEADER_VERSION, 42);
    Message msg = new Message("key".getBytes(StandardCharsets.UTF_8), props);
    SyncMessage syncMessage = SyncMessage.from(msg);
    assertThat(syncMessage.version()).isEqualTo(42L);
  }

  /**
   * A null or unknown type header names no operation, so the message is dropped at
   * parse time instead of entering the ordered dispatcher.
   */
  @Test
  void from_withNullTypeHeader_shouldReturnNull() {
    MessageProperties props = new MessageProperties();
    Message msg = new Message("key".getBytes(StandardCharsets.UTF_8), props);
    assertThat(SyncMessage.from(msg)).isNull();
  }

  /**
   * A PUT without the cache-key header has no key to apply under and no
   * per-key ordering anchor: the message is dropped at parse time.
   */
  @Test
  void from_withPutTypeAndNoCacheKeyHeader_shouldReturnNull() {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, SyncType.PUT.code());
    props.setHeader(HEADER_VERSION, 7L);
    Message msg = new Message("[\"java.lang.String\",\"v\"]".getBytes(StandardCharsets.UTF_8), props);
    assertThat(SyncMessage.from(msg)).isNull();
  }

  /**
   * A PUT carries the real key in the header and the typed value bytes in the
   * body verbatim — no envelope, no String round-trip on the receive path.
   */
  @Test
  void from_withPutType_shouldTakeKeyFromHeaderAndKeepPayloadBytes() {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, SyncType.PUT.code());
    props.setHeader(HEADER_VERSION, 7L);
    props.setHeader(HEADER_CACHE_KEY, "k");
    byte[] body = "[\"java.lang.String\",\"v\"]".getBytes(StandardCharsets.UTF_8);
    SyncMessage syncMessage = SyncMessage.from(new Message(body, props));
    assertThat(syncMessage).isNotNull();
    assertThat(syncMessage.type()).isEqualTo(SyncType.PUT);
    assertThat(syncMessage.cacheKey()).isEqualTo("k");
    assertThat(syncMessage.version()).isEqualTo(7L);
    assertThat(syncMessage.payload()).isEqualTo(body);
  }

  /**
   * A scoped clear is a key-carrying type: its body is the physical prefix,
   * delimiter included.
   */
  @Test
  void from_withInvalidatePrefixType_shouldCarryThePrefix() {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, SyncType.INVALIDATE_PREFIX.code());
    props.setHeader(HEADER_VERSION, 9L);
    String prefix = "users\u0000";
    SyncMessage syncMessage = SyncMessage.from(new Message(prefix.getBytes(StandardCharsets.UTF_8), props));
    assertThat(syncMessage).isNotNull();
    assertThat(syncMessage.cacheKey()).isEqualTo(prefix);
    assertThat(syncMessage.version()).isEqualTo(9L);
  }

  /**
   * A batch carries one stamp per entry; the decoded message exposes the highest
   * so the receiver can merge a single clock floor without rescanning the list.
   */
  @Test
  void from_batch_shouldExposeTheHighestEntryVersion() {
    byte[] body = SyncMessage.encodeBatchBody(List.of(new SyncMessage.Entry("a", 7L), new SyncMessage.Entry("b", 9L)));
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, SyncType.INVALIDATE_BATCH.code());
    SyncMessage syncMessage = SyncMessage.from(new Message(body, props));

    assertThat(syncMessage).isNotNull();
    assertThat(syncMessage.version()).isEqualTo(9L);
    assertThat(syncMessage.batch()).hasSize(2);
  }

  @Test
  void from_batch_skipsMalformedEntriesWithoutEatingTheRest() {
    byte[] body = "[[] , [\"a\"], [\"b\", 5], [[\"nested\"], 6], [\"c\", 7], [\"d\", 8]]".getBytes(StandardCharsets.UTF_8);
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, SyncType.INVALIDATE_BATCH.code());
    SyncMessage syncMessage = SyncMessage.from(new Message(body, props));

    assertThat(syncMessage).isNotNull();
    assertThat(syncMessage.batch()).extracting(SyncMessage::cacheKey).containsExactly("b", "c", "d");
  }

  /** The parser switch is exhaustive over the enum's wire codes; unknown values are null. */
  @Test
  void syncTypeRoundTripsEveryWireCode() {
    for (SyncType type : SyncType.values()) {
      assertThat(SyncType.parse(type.code())).isSameAs(type);
    }
    assertThat(SyncType.parse("UNKNOWN")).isNull();
    assertThat(SyncType.parse(7)).isNull();
    assertThat(SyncType.parse(null)).isNull();
  }

  /** The byte flags encode the self-ignore, clear-family and jitter contract per type. */
  @Test
  void syncTypeFlagsMatchTheWireContract() {
    assertThat(SyncType.INVALIDATE.ignoresSelf()).isFalse();
    assertThat(SyncType.INVALIDATE.isClear()).isFalse();
    assertThat(SyncType.INVALIDATE.needsJitter()).isFalse();

    assertThat(SyncType.REFRESH.ignoresSelf()).isTrue();
    assertThat(SyncType.REFRESH.isClear()).isFalse();
    assertThat(SyncType.REFRESH.needsJitter()).isTrue();

    assertThat(SyncType.PUT.ignoresSelf()).isTrue();
    assertThat(SyncType.PUT.isClear()).isFalse();

    assertThat(SyncType.INVALIDATE_ALL.ignoresSelf()).isTrue();
    assertThat(SyncType.INVALIDATE_ALL.isClear()).isTrue();
    assertThat(SyncType.INVALIDATE_PREFIX.isClear()).isTrue();
    assertThat(SyncType.INVALIDATE_BATCH.isClear()).isFalse();
  }
}
