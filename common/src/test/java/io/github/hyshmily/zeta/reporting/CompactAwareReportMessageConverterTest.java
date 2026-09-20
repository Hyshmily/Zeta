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
package io.github.hyshmily.zeta.reporting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConversionException;

/**
 * Wire-behavior tests for {@link CompactAwareReportMessageConverter}: the
 * encode side follows the configured flag, the decode side always sniffs
 * both formats (ADR-0074 rolling-upgrade contract).
 */
class CompactAwareReportMessageConverterTest {

  private static final ReportMessage MESSAGE =
      new ReportMessage(42L, "order-service", 1_758_000_000_000L, Map.of("user:1001", 3L));

  @Test
  void toMessage_compactEnabled_emitsBinaryBody() {
    CompactAwareReportMessageConverter converter = new CompactAwareReportMessageConverter(new Jackson2JsonMessageConverter(), true);
    Message message = converter.toMessage(MESSAGE, new MessageProperties());
    assertThat(ReportMessageCodec.isCompact(message.getBody())).isTrue();
    assertThat(message.getMessageProperties().getContentType()).isEqualTo(MessageProperties.CONTENT_TYPE_BYTES);
  }

  @Test
  void toMessage_compactDisabled_emitsJsonBody() {
    CompactAwareReportMessageConverter converter = new CompactAwareReportMessageConverter(new Jackson2JsonMessageConverter(), false);
    Message message = converter.toMessage(MESSAGE, new MessageProperties());
    assertThat(message.getBody()[0]).isEqualTo((byte) '{');
  }

  @Test
  void fromMessage_compactBody_decodedByCodec() {
    CompactAwareReportMessageConverter converter = new CompactAwareReportMessageConverter(new Jackson2JsonMessageConverter(), true);
    Message wire = converter.toMessage(MESSAGE, new MessageProperties());
    assertThat(converter.fromMessage(wire)).isEqualTo(MESSAGE);
  }

  @Test
  void fromMessage_jsonBody_delegatesToJackson_evenWhenCompactEnabled() {
    Jackson2JsonMessageConverter json = new Jackson2JsonMessageConverter();
    CompactAwareReportMessageConverter converter = new CompactAwareReportMessageConverter(json, true);
    Message jsonWire = json.toMessage(MESSAGE, new MessageProperties());
    assertThat(ReportMessageCodec.isCompact(jsonWire.getBody())).isFalse();
    assertThat(converter.fromMessage(jsonWire)).isEqualTo(MESSAGE);
  }

  @Test
  void fromMessage_malformedCompactBody_messageConversionException() {
    CompactAwareReportMessageConverter converter = new CompactAwareReportMessageConverter(new Jackson2JsonMessageConverter(), false);
    byte[] truncated = new byte[5];
    truncated[0] = ReportMessageCodec.MAGIC;
    truncated[1] = ReportMessageCodec.VERSION;
    Message bad = new Message(truncated, new MessageProperties());
    assertThatThrownBy(() -> converter.fromMessage(bad)).isInstanceOf(MessageConversionException.class);
  }
}
