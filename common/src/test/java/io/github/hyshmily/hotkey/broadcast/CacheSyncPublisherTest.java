package io.github.hyshmily.hotkey.broadcast;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

class CacheSyncPublisherTest {

  private RabbitTemplate rabbitTemplate;
  private CacheSyncProperties properties;
  private CacheSyncPublisher publisher;

  @BeforeEach
  void setUp() {
    rabbitTemplate = mock(RabbitTemplate.class);
    properties = new CacheSyncProperties();
    publisher = new CacheSyncPublisher(rabbitTemplate, properties);
    publisher.init();
  }

  @Test
  void broadcastRefresh_shouldSendMessage() {
    publisher.broadcastRefresh("key1", 1L, false);
    verify(rabbitTemplate).send(anyString(), anyString(), any());
  }

  @Test
  void broadcastInvalidate_shouldSendMessage() {
    publisher.broadcastInvalidate("key1", 1L, false);
    verify(rabbitTemplate).send(anyString(), anyString(), any());
  }

  @Test
  void broadcastRefresh_shouldSkipForBlankKey() {
    publisher.broadcastRefresh("", 1L, false);
    verify(rabbitTemplate, never()).send(anyString(), anyString(), any());
  }

  @Test
  void broadcastInvalidate_shouldDeduplicate() {
    publisher.broadcastInvalidate("key1", 5L, false);
    publisher.broadcastInvalidate("key1", 3L, false);
    publisher.broadcastInvalidate("key1", 5L, false);
    verify(rabbitTemplate).send(anyString(), anyString(), any());
  }
}
