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
