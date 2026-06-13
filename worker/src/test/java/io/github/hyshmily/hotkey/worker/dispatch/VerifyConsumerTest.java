package io.github.hyshmily.hotkey.worker.dispatch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class VerifyConsumerTest {

  @Mock
  private RabbitTemplate rabbitTemplate;

  @Captor
  private ArgumentCaptor<Message> messageCaptor;

  private final String workerId = "worker-1";
  private VerifyConsumer consumer;

  @BeforeEach
  void setUp() {
    consumer = new VerifyConsumer(rabbitTemplate, workerId);
  }

  private static Message createPingWithReplyTo(String replyTo) {
    MessageProperties props = new MessageProperties();
    props.setReplyTo(replyTo);
    return new Message(new byte[0], props);
  }

  private static Message createPingWithoutReplyTo() {
    return new Message(new byte[0], new MessageProperties());
  }

  @Test
  void shouldSendPongToCorrectReplyToQueue() {
    Message ping = createPingWithReplyTo("amq.rabbitmq.reply-to");

    consumer.handlePing(ping);

    verify(rabbitTemplate).send(eq(""), eq("amq.rabbitmq.reply-to"), messageCaptor.capture());
  }

  @Test
  void shouldSetCorrectTypeHeaderOnPong() {
    Message ping = createPingWithReplyTo("amq.rabbitmq.reply-to");

    consumer.handlePing(ping);

    verify(rabbitTemplate).send(eq(""), eq("amq.rabbitmq.reply-to"), messageCaptor.capture());
    Message pong = messageCaptor.getValue();
    assertThat(pong.getMessageProperties().getHeaders().get(AMQP_HEADER_VERIFY_TYPE))
        .isEqualTo(AMQP_HEADER_VERIFY_PONG);
  }

  @Test
  void shouldSetCorrectWorkerIdHeaderOnPong() {
    Message ping = createPingWithReplyTo("amq.rabbitmq.reply-to");

    consumer.handlePing(ping);

    verify(rabbitTemplate).send(eq(""), eq("amq.rabbitmq.reply-to"), messageCaptor.capture());
    Message pong = messageCaptor.getValue();
    assertThat(pong.getMessageProperties().getHeaders().get(AMQP_HEADER_VERIFY_WORKER_ID))
        .isEqualTo(workerId);
  }

  @Test
  void shouldHaveEmptyBodyOnPong() {
    Message ping = createPingWithReplyTo("amq.rabbitmq.reply-to");

    consumer.handlePing(ping);

    verify(rabbitTemplate).send(eq(""), eq("amq.rabbitmq.reply-to"), messageCaptor.capture());
    Message pong = messageCaptor.getValue();
    assertThat(pong.getBody()).isEmpty();
  }

  @Test
  void shouldNotSendAnythingWhenReplyToIsNull() {
    Message ping = createPingWithoutReplyTo();

    consumer.handlePing(ping);

    verifyNoInteractions(rabbitTemplate);
  }
}
