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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hyshmily.zeta.sync.worker.WorkerDecisionType;
import io.github.hyshmily.zeta.sync.worker.WorkerMessage;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;

/**
 * Wire-compatibility pins (design decisions: sync messages): additive evolution must not break old
 * receivers. Report messages are Jackson JSON only.
 */
class WireCompatTest {

  private static Jackson2JsonMessageConverter forwardCompatibleConverter() {
    ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return new Jackson2JsonMessageConverter(mapper);
  }

  @Test
  void reportJson_withUnknownField_decodesAndKeepsKnownFields() {
    Jackson2JsonMessageConverter converter = forwardCompatibleConverter();
    // Encode through the production path so the __TypeId__ header binds the
    // decode to ReportMessage exactly like the listener container does, then
    // splice in a field from a newer producer.
    Message encoded = converter.toMessage(
      new ReportMessage(7L, "shop", 123L, 0L, Map.of("k", 2L)),
      new MessageProperties()
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
  void headerFramedMessages_missingOptionalHeadersFallBackToDefaults() {
    MessageProperties props = new MessageProperties();
    props.setHeader(io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.HEADER_TYPE, WorkerDecisionType.HOT.code());
    // The version is the one required header: without a positive stamp the decision is
    // unorderable and is dropped at the wire boundary.
    Message withoutVersion = new Message("k".getBytes(StandardCharsets.UTF_8), props);
    assertThat(WorkerMessage.from(withoutVersion)).isNull();

    props.setHeader(io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.HEADER_VERSION, 7L);
    WorkerMessage decoded = WorkerMessage.from(withoutVersion);
    assertThat(decoded).isNotNull();
    assertThat(decoded.cacheKey()).isEqualTo("k");
    assertThat(decoded.stamp().decisionVersion()).isEqualTo(7L);
    assertThat(decoded.stamp().decisionNodeId()).isNull();
    assertThat(decoded.stamp().decisionEpoch()).isZero();
  }
}
