package io.github.hyshmily.zeta.worker.endpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hyshmily.zeta.worker.rule.FastLaneRuleManager;
import io.github.hyshmily.zeta.worker.rule.FastLaneRulesBroadcaster;
import io.github.hyshmily.zeta.worker.rule.impl.FastLaneRuleManagerImpl;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

@ExtendWith(MockitoExtension.class)
class FastLaneEndpointTest {

  @Mock
  private FastLaneRulesBroadcaster broadcaster;

  private FastLaneRuleManager ruleManager;
  private FastLaneEndpoint endpoint;

  @BeforeEach
  void setUp() {
    ruleManager = new FastLaneRuleManagerImpl(List.of(
      new FastLaneRuleManager.FastLaneRule("product:*", 500)
    ));
    endpoint = new FastLaneEndpoint(ruleManager, broadcaster);
  }

  @Test
  void listRules_shouldReturnRulesAndVersion() {
    Map<String, Object> result = endpoint.listRules();
    assertThat(result).containsKey("rules");
    assertThat(result).containsKey("rulesVersion");
  }

  @Test
  void putRule_shouldAddAndBroadcastWhenAbsent() {
    Map<String, Object> result = endpoint.putRule("flash:*", 200L);
    assertThat(result.get("status")).isEqualTo("added");
    assertThat(result.get("keyPattern")).isEqualTo("flash:*");
    assertThat(ruleManager.match("flash:deal")).isNotNull();
    Mockito.verify(broadcaster).broadcastNow();
  }

  @Test
  void putRule_shouldRejectNullPattern() {
    assertThatThrownBy(() -> endpoint.putRule(null, 200L))
      .isInstanceOf(ResponseStatusException.class)
      .hasFieldOrPropertyWithValue("status", HttpStatus.BAD_REQUEST);
  }

  @Test
  void putRule_shouldRejectBlankPattern() {
    assertThatThrownBy(() -> endpoint.putRule("  ", 200L))
      .isInstanceOf(ResponseStatusException.class)
      .hasFieldOrPropertyWithValue("status", HttpStatus.BAD_REQUEST);
  }

  @Test
  void putRule_shouldRejectNullThreshold() {
    assertThatThrownBy(() -> endpoint.putRule("x:*", null))
      .isInstanceOf(ResponseStatusException.class)
      .hasFieldOrPropertyWithValue("status", HttpStatus.BAD_REQUEST);
  }

  @Test
  void putRule_shouldRejectNonPositiveThreshold() {
    assertThatThrownBy(() -> endpoint.putRule("x:*", 0L))
      .isInstanceOf(ResponseStatusException.class)
      .hasFieldOrPropertyWithValue("status", HttpStatus.BAD_REQUEST);
  }

  @Test
  void putRule_shouldUpdateAndBroadcastWhenPresent() {
    Map<String, Object> result = endpoint.putRule("product:*", 999L);
    assertThat(result.get("status")).isEqualTo("updated");
    Mockito.verify(broadcaster).broadcastNow();
  }

  @Test
  void putRule_shouldBroadcastOnEveryMutation() {
    Mockito.reset(broadcaster);
    endpoint.putRule("fresh:*", 100L);
    Mockito.verify(broadcaster).broadcastNow();
  }

  @Test
  void removeRule_shouldRemoveAndBroadcastWhenFound() {
    Map<String, Object> result = endpoint.removeRule("product:*");
    assertThat(result.get("status")).isEqualTo("removed");
    assertThat(ruleManager.match("product:123")).isNull();
    Mockito.verify(broadcaster).broadcastNow();
  }

  @Test
  void removeRule_shouldNotBroadcastWhenNotFound() {
    Mockito.reset(broadcaster);
    Map<String, Object> result = endpoint.removeRule("nonexistent:*");
    assertThat(result.get("status")).isEqualTo("not-found");
    Mockito.verifyNoInteractions(broadcaster);
  }

  @Test
  void removeRule_shouldRejectEmptyPattern() {
    Map<String, Object> result = endpoint.removeRule("");
    assertThat(result.get("status")).isEqualTo("not-found");
  }
}
