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
package io.github.hyshmily.hotkey.report;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
    publisher = new ReportPublisher(rabbitTemplate, "hotkey.report.exchange", "testApp");
  }

  @Test
  void publish_shouldSendToCorrectRoutingKey() {
    ReportMessage message = new ReportMessage("testApp", 1000L, Map.of("key1", 5L));
    publisher.publish(0, message);
    verify(rabbitTemplate).convertAndSend(eq("hotkey.report.exchange"), eq("report.testApp.0"), any(Object.class));
  }
}
