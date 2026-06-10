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
package io.github.hyshmily.hotkey.rule;

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.REDIS_KEY_RULES;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hyshmily.hotkey.sync.CacheSyncPublisher;
import io.github.hyshmily.hotkey.rule.Rule.RuleAction;
import jakarta.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import io.github.hyshmily.hotkey.logging.DefaultLogger;
import io.github.hyshmily.hotkey.logging.HotKeyLogger;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Central component for evaluating cache-key rules (blocking, reporting suppression, etc.).
 * <p>
 * Rules are maintained in a thread-safe {@link CopyOnWriteArrayList} and are
 * evaluated on every {@code get} / {@code getWithSoftExpire} call.
 * <p>
 * Persistence is optional: if a {@link StringRedisTemplate} is provided, the
 * current rule set is written to Redis on every local change and loaded back at
 * startup.  If Redis is unavailable the rules remain purely in-memory – they
 * survive as long as at least one node holds them and can be re‑broadcast
 * through the {@link CacheSyncPublisher}.
 */
@RequiredArgsConstructor
public class RuleMatcher {

  private static final HotKeyLogger log = new DefaultLogger(RuleMatcher.class);

  /** Shared Jackson mapper; ignores unknown properties for forward compatibility. */
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().configure(
    DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
    false
  );

  /** Optional Redis template for rule persistence across restarts. */
  private final Optional<StringRedisTemplate> redisTemplate;
  /** Optional publisher for broadcasting rule changes to sibling instances. */
  private final Optional<CacheSyncPublisher> cacheSyncPublisher;

  /** Thread-safe list of all active rules, evaluated in order. */
  private List<Rule> rulesList = new CopyOnWriteArrayList<>();

  /**
   * Load persisted rules from Redis (if available) at startup.
   * <p>
   * If Redis is not configured or unreachable, the rule list starts empty –
   * the application still operates normally.
   */
  @PostConstruct
  void initRules() {
    loadRulesFromRedis();
  }

  /**
   * Convenience factory that auto-detects the rule type from the pattern.
   * <pre>{@code
   * RuleMatcher.of("user:123", BLOCK)             -> EXACT
   * RuleMatcher.of("temp:*", BLOCK)              -> PREFIX (trailing '*')
   * RuleMatcher.of("order:*-detail", ALLOW_NO_REPORT) -> WILDCARD
   * RuleMatcher.of("regex:user:\\d+", BLOCK)      -> REGEX
   * }</pre>
   */
  public static Rule of(String pattern, RuleAction action) {
    if (pattern.startsWith("regex:")) {
      return new Rule(Rule.RuleType.REGEX, pattern.substring(6), action);
    }
    if (pattern.contains("*") || pattern.contains("?")) {
      // A single trailing '*' with no '?' -> PREFIX optimisation
      if (pattern.endsWith("*") && pattern.indexOf('*') == pattern.length() - 1 && !pattern.contains("?")) {
        return new Rule(Rule.RuleType.PREFIX, pattern.substring(0, pattern.length() - 1), action);
      }
      return new Rule(Rule.RuleType.WILDCARD, pattern, action);
    }
    return new Rule(Rule.RuleType.EXACT, pattern, action);
  }

  /**
   * Append a rule.  The change is immediately visible locally, persisted
   * to Redis (if available), and broadcast to sibling instances.
   */
  public void addRule(Rule rule) {
    rule.prepare();
    rulesList.add(rule);
    persistAndBroadcastRules();
  }

  /**
   * Remove the first rule matching the given pattern and action.
   *
   * @return {@code true} if at least one rule was removed
   */
  public boolean removeRule(String pattern, RuleAction action) {
    log.info("Attempting to remove rule: pattern='{}', action={}", pattern, action);
    boolean removed = rulesList.removeIf(
      existing -> existing.getPattern().equals(pattern) && existing.getAction() == action
    );
    if (removed) {
      persistAndBroadcastRules();
    }
    return removed;
  }

  /**
   * Drop every rule and propagate the empty list.
   */
  public void clearRules() {
    rulesList.clear();
    persistAndBroadcastRules();
    log.info("All rules cleared and broadcasted");
  }

  /**
   * Remove all rules with a given action.
   *
   * @return the number of rules removed
   */
  public int removeRulesByAction(RuleAction action) {
    int before = rulesList.size();
    rulesList.removeIf(r -> r.getAction() == action);
    if (rulesList.size() < before) {
      persistAndBroadcastRules();
    }
    return before - rulesList.size();
  }

  /**
   * Remove a rule by its index in the list.  Silently ignores out-of-bounds indices.
   */
  public void removeRule(int index) {
    if (index >= 0 && index < rulesList.size()) {
      rulesList.remove(index);
      persistAndBroadcastRules();
    }
  }

  /**
   * Replace the current rule set with the one received from a peer.
   * <p>
   * This method <b>does not broadcast</b> the change – it only updates
   * the local list and Redis to avoid a broadcast storm.
   */
  public void syncRules(String json) {
    try {
      List<Rule> newRules = OBJECT_MAPPER.readValue(json, new TypeReference<>() {});
      replaceRules(newRules);
      redisTemplate.ifPresent(r -> r.opsForValue().set(REDIS_KEY_RULES, json));
      log.info("Rules synced from broadcast, total: {}", newRules.size());
    } catch (Exception e) {
      log.error("Failed to sync rules from broadcast", e);
    }
  }

  /**
   * Atomically swap the rule list.  Does <b>not</b> persist or broadcast.
   * <p>
   * Primarily used internally by {@link #syncRules} and for tests.
   */
  public void replaceRules(List<Rule> newRules) {
    List<Rule> replacement = new CopyOnWriteArrayList<>();
    newRules.forEach(r -> {
      r.prepare();
      replacement.add(r);
    });
    rulesList = replacement;
  }

  /** @return a snapshot of the current rules (safe for iteration). */
  public List<Rule> getAllRules() {
    return new ArrayList<>(rulesList);
  }

  /**
   * Evaluate the rule set for the given key and return an {@link Optional}
   * that describes how the caller should behave.
   * <ul>
   *   <li>{@code Optional.empty()} – {@code BLOCK}: reject the access immediately.</li>
   *   <li>{@code Optional.of(true)} – {@code ALLOW_NO_REPORT}: allow the access
   *       but suppress the hot‑key report.</li>
   *   <li>{@code Optional.of(false)} – no matching rule or {@code ALLOW}:
   *       proceed normally with reporting.</li>
   * </ul>
   *
   * @param cacheKey  the key being accessed
   * @param operation a label used only in log messages (e.g. "get", "getWithSoftExpire")
   */
  public Optional<Boolean> isAllowNoReport(String cacheKey, String operation) {
    RuleAction action = evaluateRule(cacheKey);
    if (action == RuleAction.BLOCK) {
      log.debug("{}: blocked by rule: {}", operation, cacheKey);
      return Optional.empty();
    }
    return Optional.of(action == RuleAction.ALLOW_NO_REPORT);
  }

  /**
   * Walk the rule list in order; the first match wins.
   *
   * @param cacheKey the key to evaluate against all rules
   * @return the {@link RuleAction} of the first matching rule, or {@link RuleAction#ALLOW} if no rule matches
   */
  public RuleAction evaluateRule(String cacheKey) {
    for (Rule rule : rulesList) {
      if (rule.match(cacheKey)) {
        return rule.getAction();
      }
    }
    return RuleAction.ALLOW;
  }

  /**
   * Serialize the current rule list to JSON, push it to Redis (if available),
   * and broadcast it via the publisher.
   * <p>
   * Failures are logged but never propagated – the in‑memory copy is already
   * up‑to‑date and a single persistence miss is acceptable.
   */
  private void persistAndBroadcastRules() {
    try {
      String json = OBJECT_MAPPER.writeValueAsString(rulesList);
      redisTemplate.ifPresentOrElse(
        r -> r.opsForValue().set(REDIS_KEY_RULES, json),
        () -> cacheSyncPublisher.ifPresent(p -> p.broadcastAllLocalRules(json))
      );

      log.debug("Rules persisted, total: {}", rulesList.size());
    } catch (Exception e) {
      log.error("Failed to persist rules", e);
    }
  }

  /**
   * Explicitly broadcast the complete rule set to sibling instances.
   * <p>
   * Useful in deployments without Redis, e.g. after an operator manually
   * adjusted rules on one node and wants the whole cluster to adopt them.
   */
  public void broadcastAllLocalRulesManually() {
    loadRulesFromRedis();
    if (cacheSyncPublisher.isPresent()) {
      try {
        String json = OBJECT_MAPPER.writeValueAsString(rulesList);
        cacheSyncPublisher.get().broadcastAllLocalRules(json);

        log.info("Rules serialized ({} bytes), broadcast not yet wired", json.length());
      } catch (Exception e) {
        log.error("Failed to serialize rules", e);
      }
    }
  }

  /**
   * Load persisted rules from Redis (if available) and append them to the local
   * rule list.  Failures are logged but never propagated — the application
   * continues with an empty or partially-loaded rule set.
   */
  private void loadRulesFromRedis() {
    redisTemplate.ifPresent(r -> {
      try {
        String json = r.opsForValue().get(REDIS_KEY_RULES);
        if (json != null && !json.isEmpty()) {
          List<Rule> saved = OBJECT_MAPPER.readValue(json, new TypeReference<>() {});
          saved.forEach(Rule::prepare);
          rulesList.addAll(saved);

          log.info("Rules loaded from Redis: {} rules", saved.size());
        }
      } catch (Exception e) {
        log.error("Failed to load rules from Redis", e);
      }
    });
  }
}
