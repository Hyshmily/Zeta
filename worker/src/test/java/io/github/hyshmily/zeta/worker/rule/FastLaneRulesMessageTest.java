package io.github.hyshmily.zeta.worker.rule;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.github.hyshmily.zeta.worker.rule.FastLaneRuleManager.FastLaneRule;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

class FastLaneRulesMessageTest {

  @Test
  void toMessage_shouldSerializeAllFields() {
    List<FastLaneRule> rules = List.of(new FastLaneRule("product:*", 500));
    FastLaneRulesMessage msg = new FastLaneRulesMessage(1L, "node1", 1000L, rules);
    Message amqp = msg.toMessage();

    assertThat(amqp).isNotNull();
    assertThat((Object) amqp.getMessageProperties().getHeader("type")).isEqualTo("FASTLANE_RULES");
    assertThat((Object) amqp.getMessageProperties().getHeader("nodeId")).isEqualTo("node1");
    assertThat((Object) amqp.getMessageProperties().getHeader("messageId")).isEqualTo(1L);
    assertThat((Object) amqp.getMessageProperties().getHeader("fastlaneRulesVersion")).isEqualTo(1000L);
    assertThat(amqp.getBody()).isNotEmpty();
  }

  @Test
  void from_shouldDeserializeValidMessage() {
    FastLaneRulesMessage original = new FastLaneRulesMessage(42L, "node2", 2000L, List.of(new FastLaneRule("news:*", 300)));
    Message amqp = original.toMessage();
    FastLaneRulesMessage restored = FastLaneRulesMessage.from(amqp);

    assertThat(restored).isNotNull();
    assertThat(restored.id()).isEqualTo(42L);
    assertThat(restored.nodeId()).isEqualTo("node2");
    assertThat(restored.rulesVersion()).isEqualTo(2000L);
    assertThat(restored.rules().size()).isEqualTo(1);
    assertThat(restored.rules().get(0).keyPattern()).isEqualTo("news:*");
    assertThat(restored.rules().get(0).threshold()).isEqualTo(300);
  }

  @Test
  void from_shouldReturnNullForNullMessage() {
    assertThat(FastLaneRulesMessage.from(null)).isNull();
  }

  @Test
  void from_shouldReturnNullForWrongType() {
    Message msg = new Message("test".getBytes(), new MessageProperties());
    msg.getMessageProperties().setHeader("type", "OTHER_TYPE");
    assertThat(FastLaneRulesMessage.from(msg)).isNull();
  }

  @Test
  void from_shouldHandleEmptyRules() {
    FastLaneRulesMessage original = new FastLaneRulesMessage(1L, "n", 1L, List.of());
    Message amqp = original.toMessage();
    FastLaneRulesMessage restored = FastLaneRulesMessage.from(amqp);
    assertThat(restored).isNotNull();
    assertThat(restored.rules().size()).isZero();
  }

  @Test
  void from_shouldHandleMalformedBody() {
    MessageProperties props = new MessageProperties();
    props.setHeader("type", "FASTLANE_RULES");
    Message msg = new Message("not-json".getBytes(), props);
    assertThat(FastLaneRulesMessage.from(msg)).isNull();
  }
}
