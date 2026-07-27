package io.github.hyshmily.zeta.worker.endpoint;

import io.github.hyshmily.zeta.worker.rule.FastLaneRuleManager;
import io.github.hyshmily.zeta.worker.rule.FastLaneRulesBroadcaster;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * Runtime CRUD for fast-lane rules (ADR-0025).
 *
 * <p>Every successful mutation stamps the local rules version and triggers an
 * immediate full-set gossip broadcast to peer Workers; a periodic broadcast
 * covers lost messages. <b>Operational discipline:</b> perform rule changes
 * against a single Worker at a time — concurrent edits on different Workers
 * resolve by wall-clock LWW (see ADR-0025).
 *
 * <p><b>DELETE note:</b> the pattern travels as a URL path variable, so
 * patterns containing {@code /} cannot be deleted via this mapping — use
 * glob-safe patterns or URL-encode the path segment.
 */
@RestController
@RequestMapping("${management.endpoints.web.base-path:/actuator}/hotkey/fastlane")
public class FastLaneEndpoint {

  private final FastLaneRuleManager ruleManager;
  private final FastLaneRulesBroadcaster broadcaster;

  public FastLaneEndpoint(FastLaneRuleManager ruleManager, FastLaneRulesBroadcaster broadcaster) {
    this.ruleManager = ruleManager;
    this.broadcaster = broadcaster;
  }

  @GetMapping
  public Map<String, Object> listRules() {
    return Map.of("rules", ruleManager.getRules(), "rulesVersion", ruleManager.getRulesVersion());
  }

  @PostMapping
  public Map<String, Object> addRule(@RequestBody Map<String, Object> body) {
    String pattern = requirePattern(body);
    long threshold = requireThreshold(body);
    ruleManager.addRule(pattern, threshold);
    broadcaster.broadcastNow();
    return Map.of("status", "added", "keyPattern", pattern, "threshold", threshold);
  }

  @PutMapping
  public Map<String, Object> updateRule(@RequestBody Map<String, Object> body) {
    String pattern = requirePattern(body);
    long threshold = requireThreshold(body);
    boolean updated = ruleManager.updateRule(pattern, threshold);
    if (updated) {
      broadcaster.broadcastNow();
    }
    return Map.of("status", updated ? "updated" : "not-found", "keyPattern", pattern);
  }

  @DeleteMapping("/{pattern}")
  public Map<String, Object> removeRule(@PathVariable String pattern) {
    boolean removed = ruleManager.removeRule(pattern);
    if (removed) {
      broadcaster.broadcastNow();
    }
    return Map.of("status", removed ? "removed" : "not-found", "keyPattern", pattern);
  }

  private static String requirePattern(Map<String, Object> body) {
    if (body == null || !(body.get("keyPattern") instanceof String pattern) || pattern.isBlank()) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "keyPattern must be a non-blank string");
    }
    return pattern;
  }

  private static long requireThreshold(Map<String, Object> body) {
    if (body == null || !(body.get("threshold") instanceof Number n) || n.longValue() <= 0) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "threshold must be a positive number");
    }
    return n.longValue();
  }
}
