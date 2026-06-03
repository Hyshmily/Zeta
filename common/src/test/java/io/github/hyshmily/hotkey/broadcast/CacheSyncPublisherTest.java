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

/**
 * Tests for {@link CacheSyncPublisher} covering message sending, blank-key skipping, and
 * deduplication of broadcast refresh and invalidate messages.
 */
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
  void broadcastLocalInvalidate_shouldSendMessage() {
    publisher.broadcastLocalInvalidate("key1", 1L, false);
    verify(rabbitTemplate).send(anyString(), anyString(), any());
  }

  @Test
  void broadcastRefresh_shouldSkipForBlankKey() {
    publisher.broadcastRefresh("", 1L, false);
    verify(rabbitTemplate, never()).send(anyString(), anyString(), any());
  }

  @Test
  void broadcastLocalInvalidate_shouldDeduplicate() {
    publisher.broadcastLocalInvalidate("key1", 5L, false);
    publisher.broadcastLocalInvalidate("key1", 3L, false);
    publisher.broadcastLocalInvalidate("key1", 5L, false);
    verify(rabbitTemplate).send(anyString(), anyString(), any());
  }
}
