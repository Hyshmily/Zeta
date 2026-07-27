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
  void addRule_shouldAddAndBroadcast() {
    Map<String, Object> result = endpoint.addRule(Map.of("keyPattern", "flash:*", "threshold", 200));
    assertThat(result.get("status")).isEqualTo("added");
    assertThat(result.get("keyPattern")).isEqualTo("flash:*");
    assertThat(ruleManager.match("flash:deal")).isNotNull();
    Mockito.verify(broadcaster).broadcastNow();
  }

  @Test
  void addRule_shouldRejectMissingPattern() {
    assertThatThrownBy(() -> endpoint.addRule(Map.of("threshold", 200)))
      .isInstanceOf(ResponseStatusException.class)
      .hasFieldOrPropertyWithValue("status", HttpStatus.BAD_REQUEST);
  }

  @Test
  void addRule_shouldRejectBlankPattern() {
    assertThatThrownBy(() -> endpoint.addRule(Map.of("keyPattern", "  ", "threshold", 200)))
      .isInstanceOf(ResponseStatusException.class)
      .hasFieldOrPropertyWithValue("status", HttpStatus.BAD_REQUEST);
  }

  @Test
  void addRule_shouldRejectMissingThreshold() {
    assertThatThrownBy(() -> endpoint.addRule(Map.of("keyPattern", "x:*")))
      .isInstanceOf(ResponseStatusException.class)
      .hasFieldOrPropertyWithValue("status", HttpStatus.BAD_REQUEST);
  }

  @Test
  void addRule_shouldRejectNonPositiveThreshold() {
    assertThatThrownBy(() -> endpoint.addRule(Map.of("keyPattern", "x:*", "threshold", 0)))
      .isInstanceOf(ResponseStatusException.class)
      .hasFieldOrPropertyWithValue("status", HttpStatus.BAD_REQUEST);
  }

  @Test
  void addRule_shouldRejectNullBody() {
    assertThatThrownBy(() -> endpoint.addRule(null))
      .isInstanceOf(ResponseStatusException.class)
      .hasFieldOrPropertyWithValue("status", HttpStatus.BAD_REQUEST);
  }

  @Test
  void updateRule_shouldUpdateAndBroadcastWhenFound() {
    Map<String, Object> result = endpoint.updateRule(Map.of("keyPattern", "product:*", "threshold", 999));
    assertThat(result.get("status")).isEqualTo("updated");
    Mockito.verify(broadcaster).broadcastNow();
  }

  @Test
  void updateRule_shouldNotBroadcastWhenNotFound() {
    Mockito.reset(broadcaster);
    Map<String, Object> result = endpoint.updateRule(Map.of("keyPattern", "nonexistent:*", "threshold", 999));
    assertThat(result.get("status")).isEqualTo("not-found");
    Mockito.verifyNoInteractions(broadcaster);
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
  void updateRule_shouldRejectNullBody() {
    assertThatThrownBy(() -> endpoint.updateRule(null))
      .isInstanceOf(ResponseStatusException.class);
  }

  @Test
  void updateRule_shouldRejectBlankPattern() {
    assertThatThrownBy(() -> endpoint.updateRule(Map.of("keyPattern", "", "threshold", 100)))
      .isInstanceOf(ResponseStatusException.class);
  }

  @Test
  void removeRule_shouldRejectEmptyPattern() {
    Map<String, Object> result = endpoint.removeRule("");
    assertThat(result.get("status")).isEqualTo("not-found");
  }
}
