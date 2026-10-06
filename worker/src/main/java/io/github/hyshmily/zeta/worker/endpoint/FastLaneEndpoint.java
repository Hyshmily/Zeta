package io.github.hyshmily.zeta.worker.endpoint;

import io.github.hyshmily.zeta.worker.rule.FastLaneRuleManager;
import io.github.hyshmily.zeta.worker.rule.FastLaneRulesBroadcaster;
import java.util.Map;
import org.springframework.boot.actuate.endpoint.annotation.DeleteOperation;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

/**
 * Runtime CRUD for fast-lane rules (ADR-0025) on the management plane.
 *
 * <p>Every successful mutation stamps the local rules version and triggers an
 * immediate full-set gossip broadcast to peer Workers; a periodic broadcast
 * covers lost messages. <b>Operational discipline:</b> perform rule changes
 * against a single Worker at a time — concurrent edits on different Workers
 * resolve by wall-clock LWW (see ADR-0025).
 *
 * <p>Endpoint id {@code hotkey-fastlane} (path
 * {@code /actuator/hotkey-fastlane}): the actuator web model exposes a single
 * write operation, so the former separate add/update verbs merge into one
 * {@code putRule} upsert — {@code POST {"keyPattern": "...", "threshold": N}}
 * adds a missing pattern ({@code "added"}) or replaces an existing one
 * ({@code "updated"}); {@code DELETE .../{pattern}} removes.
 * Runs on the management plane (port, exposure, and roles honored via the
 * standard {@code management.*} configuration): rule mutation no longer sits
 * on the application port.
 *
 * <p><b>DELETE note:</b> the pattern travels as a path segment, so patterns
 * containing {@code /} cannot be deleted via this operation — use glob-safe
 * patterns or URL-encode the segment.
 */
@Endpoint(id = "hotkey-fastlane")
public class FastLaneEndpoint {

  private final FastLaneRuleManager ruleManager;
  private final FastLaneRulesBroadcaster broadcaster;

  public FastLaneEndpoint(FastLaneRuleManager ruleManager, FastLaneRulesBroadcaster broadcaster) {
    this.ruleManager = ruleManager;
    this.broadcaster = broadcaster;
  }

  @ReadOperation
  public Map<String, Object> listRules() {
    return Map.of("rules", ruleManager.getRules(), "rulesVersion", ruleManager.getRulesVersion());
  }

  /**
   * Add-or-replace a fast-lane rule, broadcasting the new full set on success.
   *
   * @param keyPattern the rule pattern ( glob, never blank)
   * @param threshold  the hot threshold (positive); absent or non-positive is
   *                   rejected before any mutation
   * @return {@code added} for a new pattern, {@code updated} for a replaced one
   */
  @WriteOperation
  public Map<String, Object> putRule(String keyPattern, Long threshold) {
    requirePattern(keyPattern);
    requireThreshold(threshold);
    boolean updated = ruleManager.updateRule(keyPattern, threshold);
    if (!updated) {
      ruleManager.addRule(keyPattern, threshold);
    }
    broadcaster.broadcastNow();
    return updated
      ? Map.of("status", "updated", "keyPattern", keyPattern)
      : Map.of("status", "added", "keyPattern", keyPattern, "threshold", threshold);
  }

  @DeleteOperation
  public Map<String, Object> removeRule(@Selector String pattern) {
    boolean removed = ruleManager.removeRule(pattern);
    if (removed) {
      broadcaster.broadcastNow();
    }
    return Map.of("status", removed ? "removed" : "not-found", "keyPattern", pattern);
  }

  private static void requirePattern(String pattern) {
    if (pattern == null || pattern.isBlank()) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "keyPattern must be a non-blank string");
    }
  }

  private static void requireThreshold(Long threshold) {
    if (threshold == null || threshold <= 0) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "threshold must be a positive number");
    }
  }
}
