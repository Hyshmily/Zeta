package io.github.hyshmily.zeta.worker.rule;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import io.github.hyshmily.zeta.worker.rule.FastLaneRuleManager.FastLaneRule;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

@ExtendWith(MockitoExtension.class)
class FastLaneRulesBroadcasterTest {

  @Mock
  private RabbitTemplate rabbitTemplate;
  @Mock
  private SnowflakeIdGenerator snowflakeIdGenerator;
  @Mock
  private FastLaneRuleManager ruleManager;

  private FastLaneRulesBroadcaster broadcaster;

  @BeforeEach
  void setUp() {
    when(snowflakeIdGenerator.nextId()).thenReturn(1L);
    when(ruleManager.getRulesVersion()).thenReturn(100L);
    when(ruleManager.getRules()).thenReturn(List.of(new FastLaneRule("product:*", 500)));
    broadcaster = new FastLaneRulesBroadcaster(rabbitTemplate, "hb.exchange", "node1", ruleManager, snowflakeIdGenerator);
  }

  @Test
  void broadcastNow_shouldSendMessage() {
    broadcaster.broadcastNow();
    verify(rabbitTemplate).send(eq("hb.exchange"), eq("fastlane.rules"), any(Message.class));
  }

  @Test
  void broadcastNow_shouldHandleSendFailure() {
    doThrow(new RuntimeException("broker down")).when(rabbitTemplate).send(any(String.class), any(String.class), any(Message.class));
    broadcaster.broadcastNow();
  }
}
