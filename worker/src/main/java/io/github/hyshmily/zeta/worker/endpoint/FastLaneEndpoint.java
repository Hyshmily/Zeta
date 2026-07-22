package io.github.hyshmily.zeta.worker.endpoint;

import io.github.hyshmily.zeta.worker.rule.FastLaneRuleManager;
import java.util.Map;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("${management.endpoints.web.base-path:/actuator}/hotkey/fastlane")
public class FastLaneEndpoint {

  private final FastLaneRuleManager ruleManager;

  public FastLaneEndpoint(FastLaneRuleManager ruleManager) {
    this.ruleManager = ruleManager;
  }

  @GetMapping
  public Map<String, Object> listRules() {
    return Map.of("rules", ruleManager.getRules());
  }

  @PostMapping
  public Map<String, Object> addRule(@RequestBody Map<String, Object> body) {
    String pattern = (String) body.get("keyPattern");
    long threshold = ((Number) body.get("threshold")).longValue();
    ruleManager.addRule(pattern, threshold);
    return Map.of("status", "added", "keyPattern", pattern, "threshold", threshold);
  }

  @PutMapping
  public Map<String, Object> updateRule(@RequestBody Map<String, Object> body) {
    String pattern = (String) body.get("keyPattern");
    long threshold = ((Number) body.get("threshold")).longValue();
    boolean updated = ruleManager.updateRule(pattern, threshold);
    return Map.of("status", updated ? "updated" : "not-found", "keyPattern", pattern);
  }

  @DeleteMapping("/{pattern}")
  public Map<String, Object> removeRule(@PathVariable String pattern) {
    boolean removed = ruleManager.removeRule(pattern);
    return Map.of("status", removed ? "removed" : "not-found", "keyPattern", pattern);
  }
}
