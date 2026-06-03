/*
 * Copyright 2026 Hyshmily. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.hyshmily.hotkey.actuator;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.algorithm.Item;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyProperties;
import io.github.hyshmily.hotkey.hotkeycache.SingleFlight;
import io.github.hyshmily.hotkey.report.HotKeyReporter;
import io.github.hyshmily.hotkey.rule.Rule;
import io.github.hyshmily.hotkey.rule.Rule.RuleAction;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;

/**
 * Actuator {@code /actuator/hotkey} endpoint that exposes runtime diagnostics
 * and rule management operations.
 *
 * <p>The response includes both app-side and Worker-side TopK rankings, L1
 * cache metrics, SingleFlight in-flight sizes, and recently expelled keys.
 * Each section is produced only when the corresponding service is available
 * in the current deployment mode, making the endpoint safe in App-only,
 * Worker-only, and coexistence modes.
 *
 * <p>Write operations ({@code POST / DELETE /actuator/hotkey/rules}) allow
 * runtime blacklist/whitelist management without restarting the application.
 */
@Endpoint(id = "hotkey")
@RequiredArgsConstructor
public class HotKeyEndpoint {

  private final TopK hotKeyDetector;
  private final TopK workerTopK;
  private final Cache<String, Object> caffeineCache;
  private final SingleFlight singleFlight;
  private final HotKeyProperties properties;
  private final HotKeyReporter hotKeyReporter;
  private final RuleMatcher ruleMatcher;

  private int l1MaxSize() {
    return properties.getLocalCacheMaxSize();
  }

  /**
   * Collect all diagnostic metrics into a single response map.
   *
   * @return a {@link LinkedHashMap} with TopK, cache, and SingleFlight metrics
   */
  @ReadOperation
  public Map<String, Object> hotKeyInfo() {
    Map<String, Object> info = new LinkedHashMap<>();

    if (hotKeyDetector != null) {
      List<Item> topKList = hotKeyDetector.list();
      List<Map<String, Object>> topKKeys = new ArrayList<>(topKList.size());
      for (Item item : topKList) {
        topKKeys.add(Map.of("key", item.key(), "count", item.count()));
      }
      info.put("topK", topKKeys);
      info.put("topKCount", topKList.size());
      info.put("totalRequests", hotKeyDetector.total());
      info.put("recentlyExpelled",
        hotKeyDetector.expelled().stream()
          .map(Item::key)
          .limit(10)
          .toList());
    }

    if (workerTopK != null) {
      List<Item> topKList = workerTopK.list();
      List<Map<String, Object>> topKKeys = new ArrayList<>(topKList.size());
      for (Item item : topKList) {
        topKKeys.add(Map.of("key", item.key(), "count", item.count()));
      }
      info.put("workerTopK", topKKeys);
      info.put("workerTopKCount", topKList.size());
      info.put("workerTotalRequests", workerTopK.total());
      info.put("workerRecentlyExpelled",
        workerTopK.expelled().stream()
          .map(Item::key)
          .limit(10)
          .toList());
    }

    if (caffeineCache != null) {
      info.put("l1CacheSize", caffeineCache.estimatedSize());
      info.put("l1MaxSize", l1MaxSize());
    }

    if (singleFlight != null) {
      info.put("inflightSize", singleFlight.estimatedInflightSize());
    }

    if (hotKeyReporter != null) {
      info.put("reportQueueDepth", hotKeyReporter.dispatcherDepth());
      info.put("reportQueueCapacity", hotKeyReporter.dispatcherCapacity());
      info.put("reportExpiredCount", hotKeyReporter.dispatcherExpired());
      info.put("reportQueueFullCount", hotKeyReporter.dispatcherDropped());
    }

    if (ruleMatcher != null) {
      info.put("rules", ruleMatcher.getAllRules().stream().map(this::ruleToMap).toList());
    }

    return info;
  }

  /**
   * Add a blacklist or whitelist rule at runtime.
   *
   * @param pattern the key pattern (auto-detected: exact, prefix {@code *}, wildcard, or {@code regex:})
   * @param action  {@code BLOCK} or {@code ALLOW_NO_REPORT}
   * @return a map with operation status
   */
  @WriteOperation
  public Map<String, Object> addRule(String pattern, String action) {
    if (ruleMatcher == null) {
      return Map.of("error", "RuleMatcher not available");
    }
    try {
      RuleAction ruleAction = RuleAction.valueOf(action.toUpperCase());
      ruleMatcher.addRule(RuleMatcher.of(pattern, ruleAction));
      return Map.of("status", "ok", "pattern", pattern, "action", action);
    } catch (IllegalArgumentException e) {
      return Map.of("error", "Invalid action: " + action + ". Use BLOCK or ALLOW_NO_REPORT");
    }
  }

  /**
   * Remove a blacklist or whitelist rule by pattern and action.
   *
   * @param pattern the key pattern to remove
   * @param action  the rule action to match
   * @return a map with operation status
   */
  @WriteOperation
  public Map<String, Object> removeRule(String pattern, String action) {
    if (ruleMatcher == null) {
      return Map.of("error", "RuleMatcher not available");
    }
    try {
      RuleAction ruleAction = RuleAction.valueOf(action.toUpperCase());
      boolean removed = ruleMatcher.removeRule(pattern, ruleAction);
      return removed
        ? Map.of("status", "ok", "removed", true)
        : Map.of("status", "ok", "removed", false, "message", "Rule not found");
    } catch (IllegalArgumentException e) {
      return Map.of("error", "Invalid action: " + action + ". Use BLOCK or ALLOW_NO_REPORT");
    }
  }

  /**
   * Remove all rules.
   *
   * @return a map with operation status
   */
  @WriteOperation
  public Map<String, Object> clearRules() {
    if (ruleMatcher == null) {
      return Map.of("error", "RuleMatcher not available");
    }
    ruleMatcher.clearRules();
    return Map.of("status", "ok", "removed", true);
  }

  private Map<String, String> ruleToMap(Rule rule) {
    Map<String, String> m = new LinkedHashMap<>();
    m.put("type", rule.getType().name());
    m.put("pattern", rule.getPattern());
    m.put("action", rule.getAction().name());
    return m;
  }
}
