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

import static io.github.hyshmily.zeta.constants.ZetaConstants.Routing.KEY_REPORT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import io.github.hyshmily.zeta.constants.ZetaConstants;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * Tests for {@link ReportPublisher} verifying correct routing key and exchange usage.
 */
class ReportPublisherTest {

  private RabbitTemplate rabbitTemplate;
  private ReportPublisher publisher;

  @BeforeEach
  void setUp() {
    rabbitTemplate = mock(RabbitTemplate.class);
    publisher = new ReportPublisher(rabbitTemplate, ZetaConstants.Exchange.REPORT, "testApp");
  }

  /**
   * Verifies that publish sends the message to the correct exchange with a properly formatted routing key.
   */
  @Test
  void publish_shouldSendToCorrectRoutingKey() {
    ReportMessage message = new ReportMessage("testApp", 1000L, Map.of("key1", 5L));
    publisher.publish("0", message);
    verify(rabbitTemplate).convertAndSend(
      eq(ZetaConstants.Exchange.REPORT),
      eq(KEY_REPORT + "testApp.0"),
      any(ReportMessage.class)
    );
  }

  @Test
  void publish_withEmptyTarget_shouldSendCorrectly() {
    ReportMessage message = new ReportMessage("testApp", 1000L, Map.of("key1", 5L));
    publisher.publish("", message);
    verify(rabbitTemplate).convertAndSend(
      eq(ZetaConstants.Exchange.REPORT),
      eq(KEY_REPORT + "testApp."),
      any(ReportMessage.class)
    );
  }

  @Test
  void publish_withNullMessage_shouldThrow() {
    org.junit.jupiter.api.Assertions.assertThrows(NullPointerException.class, () -> publisher.publish("target", null));
  }

  @Test
  void publish_whenAmqpException_shouldThrowToCaller() {
    ReportMessage message = new ReportMessage("testApp", 1000L, Map.of("key1", 5L));
    doThrow(new AmqpException("Broker unavailable"))
      .when(rabbitTemplate)
      .convertAndSend(
        eq(ZetaConstants.Exchange.REPORT),
        eq(KEY_REPORT + "testApp.target"),
        any(ReportMessage.class)
      );
    org.junit.jupiter.api.Assertions.assertThrows(AmqpException.class, () -> publisher.publish("target", message));
  }
}
