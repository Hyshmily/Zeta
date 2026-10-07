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

import io.github.hyshmily.zeta.sync.worker.WorkerMessage;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * Wire-compatibility pins (ADR-0091): additive evolution must not break old
 * receivers, and newer binary versions must be rejected, never misparsed.
 */
class WireCompatTest {

  @Test
  void reportJson_withUnknownField_decodesAndKeepsKnownFields() {
    CompactAwareReportMessageConverter converter = CompactAwareReportMessageConverter.forwardCompatible(false);
    // Encode through the production path so the __TypeId__ header binds the
    // decode to ReportMessage exactly like the listener container does, then
    // splice in a field from a newer producer.
    Message encoded = converter.toMessage(
      new ReportMessage(7L, "shop", 123L, Map.of("k", 2L)), new MessageProperties()
    );
    String json = new String(encoded.getBody(), StandardCharsets.UTF_8);
    String evolved = json.substring(0, json.length() - 1) + ",\"futureField\":\"x\"}";
    Message evolvedMsg = new Message(evolved.getBytes(StandardCharsets.UTF_8), encoded.getMessageProperties());
    Object decoded = converter.fromMessage(evolvedMsg);
    assertThat(decoded).isInstanceOf(ReportMessage.class);
    ReportMessage report = (ReportMessage) decoded;
    assertThat(report.appName()).isEqualTo("shop");
    assertThat(report.counts()).containsEntry("k", 2L);
  }

  @Test
  void compactNewerVersion_isRejectedNotMisparsed() {
    ReportMessage message = new ReportMessage(1L, "shop", 2L, Map.of("k", 3L));
    byte[] body = ReportMessageCodec.encode(message);
    body[1] = 0x7F;
    assertThatThrownBy(() -> ReportMessageCodec.decode(body))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("unsupported version");
  }

  @Test
  void headerFramedMessages_missingHeadersFallBackToDefaults() {
    MessageProperties props = new MessageProperties();
    props.setHeader(
      io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.HEADER_TYPE,
      WorkerMessage.TYPE_HOT
    );
    Message msg = new Message("k".getBytes(StandardCharsets.UTF_8), props);
    WorkerMessage decoded = WorkerMessage.from(msg);
    assertThat(decoded).isNotNull();
    assertThat(decoded.cacheKey()).isEqualTo("k");
    assertThat(decoded.decisionVersion()).isZero();
    assertThat(decoded.nodeId()).isNull();
    assertThat(decoded.epoch()).isZero();
  }
}
