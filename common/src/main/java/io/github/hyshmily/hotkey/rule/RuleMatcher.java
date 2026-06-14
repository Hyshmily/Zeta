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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hyshmily.hotkey.logging.DefaultLogger;
import io.github.hyshmily.hotkey.logging.HotKeyLogger;
import io.github.hyshmily.hotkey.rule.Rule.RuleAction;
import io.github.hyshmily.hotkey.sync.CacheSyncPublisher;
import jakarta.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

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

  /** Lua compare-and-set script: writes only if incoming version > current Redis version. */
  private static final RedisScript<Long> RULE_CAS_SCRIPT;

  static {
    DefaultRedisScript<Long> s = new DefaultRedisScript<>();
    s.setScriptText(
      """
      local current = redis.call('GET', KEYS[1])
      if current == false then
        redis.call('SET', KEYS[1], ARGV[1])
        return 1
      end
      local ok, decoded = pcall(cjson.decode, current)
      local curVer = 0
      if ok and type(decoded) == 'table' and decoded['rulesVersion'] then
        curVer = decoded['rulesVersion']
      end
      if tonumber(ARGV[2]) > curVer then
        redis.call('SET', KEYS[1], ARGV[1])
        return 1
      end
      return 0"""
    );
    s.setResultType(Long.class);
    RULE_CAS_SCRIPT = s;
  }

  /** Optional Redis template for rule persistence across restarts. */
  private final Optional<StringRedisTemplate> redisTemplate;
  /** Optional publisher for broadcasting rule changes to sibling instances. */
  private final Optional<CacheSyncPublisher> cacheSyncPublisher;

  /** Thread-safe list of all active rules, evaluated in order. */
  private volatile List<Rule> rulesList = new CopyOnWriteArrayList<>();

  /** Monotonically increasing version for rule set changes. Never degraded. */
  private final AtomicLong rulesVersion = new AtomicLong(0L);

  /**
   * Load persisted rules from Redis (if available) at startup.
   * <p>
   * If Redis is not configured or unreachable, the rule list starts empty –
   * the application still operates normally.
   *
   * @see #loadRulesFromRedis
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
   *
   * @param pattern the key pattern; a leading {@code regex:} prefix forces REGEX type,
   *     a trailing wildcard without interior wildcards is optimized to PREFIX,
   *     patterns containing {@code *} or {@code ?} are treated as WILDCARD,
   *     otherwise the pattern is treated as EXACT
   * @param action  the action to assign to the created rule
   * @return a new {@link Rule} with an auto-detected type matching the given pattern
   */
  public static Rule of(String pattern, RuleAction action) {
    if (pattern.startsWith("regex:")) {
      return new Rule(Rule.RuleType.REGEX, pattern.substring(6), action);
    }
    if (pattern.contains("*") || pattern.contains("?")) {
      // A single trailing '*' with no '?' -> PREFIX optimization
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
   *
   * @param rule the rule to append; must not be {@code null}
   */
  public void addRule(Rule rule) {
    rule.prepare();
    rulesList.add(rule);
    rulesVersion.incrementAndGet();
    persistAndBroadcastRules();
  }

  /**
   * Remove all rules matching the given pattern and action.
   *
   * @param pattern the pattern string to match
   * @param action  the action to match
   * @return {@code true} if at least one rule was removed
   */
  public boolean removeRule(String pattern, RuleAction action) {
    log.info("Attempting to remove rule: pattern='{}', action={}", pattern, action);
    boolean removed = rulesList.removeIf(
      existing -> existing.getPattern().equals(pattern) && existing.getAction() == action
    );
    if (removed) {
      rulesVersion.incrementAndGet();

      persistAndBroadcastRules();
    }
    return removed;
  }

  /**
   * Drop every rule and propagate the empty list.
   *
   * @see #persistAndBroadcastRules
   */
  public void clearRules() {
    rulesList.clear();
    rulesVersion.incrementAndGet();

    persistAndBroadcastRules();
    log.info("All rules cleared and broadcasted");
  }

  /**
   * Remove all rules with a given action.
   *
   * @param action the action to match; rules with this action are removed
   * @return the number of rules removed
   */
  public int removeRulesByAction(RuleAction action) {
    int before = rulesList.size();
    rulesList.removeIf(r -> r.getAction() == action);

    if (rulesList.size() < before) {
      rulesVersion.incrementAndGet();

      persistAndBroadcastRules();
    }
    return before - rulesList.size();
  }

  /**
   * Remove a rule by its index in the list.  Silently ignores out-of-bounds indices.
   *
   * @param index the index of the rule to remove
   */
  public void removeRule(int index) {
    if (index >= 0 && index < rulesList.size()) {
      rulesList.remove(index);
      rulesVersion.incrementAndGet();

      persistAndBroadcastRules();
    }
  }

  /**
   * Merge the incoming rule set into the local rule set, guarded by version.
   * <p>
   * If {@code incomingVersion > 0} and {@code <= localVersion}, the sync is skipped
   * (stale broadcast). Otherwise the rules are merged by pattern (incoming overwrites
   * same-pattern entries, local-only entries are preserved), the local version is
   * bumped past the incoming version, and the result is persisted to Redis.
   * <p>
   * This method <b>does not broadcast</b> — only updates local list and Redis,
   * avoiding a broadcast storm.
   *
   * @param json            the JSON-serialized rule list (may be old-format array or
   *                        new-format wrapper)
   * @param incomingVersion the rulesVersion from the incoming message header,
   *                        or {@code 0L} for pre-version broadcasts
   */
  public void syncRules(String json, long incomingVersion) {
    if (incomingVersion > 0L && incomingVersion <= rulesVersion.get()) {
      log.debug("Stale rules sync ignored: incomingVersion={}, localVersion={}", incomingVersion, rulesVersion.get());
      return;
    }
    try {
      List<Rule> incomingRules = parseRulesJson(json);
      List<Rule> merged = mergeRules(rulesList, incomingRules);

      replaceRules(merged);

      long newVersion = rulesVersion.updateAndGet(v -> Math.max(v, incomingVersion)) + 1;

      String outJson = serializeRules(merged, newVersion);
      redisTemplate.ifPresent(r -> persistToRedis(r, outJson, newVersion));
      log.info("Rules synced from broadcast, version={}, total: {}", newVersion, merged.size());
    } catch (Exception e) {
      log.error("Failed to sync rules from broadcast", e);
    }
  }

  /**
   * Atomically swap the rule list.  Does <b>not</b> persist or broadcast.
   * <p>
   * Primarily used internally by {@link #syncRules} and for tests.
   *
   * @param newRules the new rule list; each element is {@link Rule#prepare() prepared} before
   *     insertion, must not be {@code null}
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
   * @return {@link Optional#empty()} if the rule action is {@link RuleAction#BLOCK},
   *         {@code Optional.of(true)} if the rule action is {@link RuleAction#ALLOW_NO_REPORT},
   *         {@code Optional.of(false)} if no rule matches or the action is {@link RuleAction#ALLOW}
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
   * Serialize the current rule list to the versioned JSON format, write it to Redis
   * (if available) via Lua compare-and-set, and broadcast it via AMQP (if available).
   * <p>
   * Both channels are invoked independently — the XOR pattern has been replaced
   * by both/and. Failures are logged but never propagated.
   */
  private void persistAndBroadcastRules() {
    try {
      long version = rulesVersion.get();
      String json = serializeRules(new ArrayList<>(rulesList), version);

      redisTemplate.ifPresent(r -> persistToRedis(r, json, version));
      cacheSyncPublisher.ifPresent(p -> p.broadcastAllLocalRules(json, version));

      log.debug("Rules persisted (version={}), total: {}", version, rulesList.size());
    } catch (Exception e) {
      log.error("Failed to persist rules", e);
    }
  }

  /**
   * Write the versioned JSON rule set to Redis via Lua compare-and-set.
   * Falls back to plain SET if the Lua script fails.
   */
  private void persistToRedis(StringRedisTemplate r, String json, long version) {
    try {
      r.execute(RULE_CAS_SCRIPT, List.of(REDIS_KEY_RULES), json, String.valueOf(version));
    } catch (Exception e) {
      log.warn("Lua compare-and-set failed for rules (version={}), fallback to plain set", version, e);
      try {
        r.opsForValue().set(REDIS_KEY_RULES, json);
      } catch (Exception e2) {
        log.error("Redis fallback set also failed, skipping persist", e2);
      }
    }
  }

  /**
   * Serialize a rule list into the versioned JSON wrapper format.
   *
   * <pre>{@code {"rulesVersion":5,"rules":[{"pattern":"x","action":"BLOCK"}]}}</pre>
   */
  private String serializeRules(List<Rule> rules, long version) throws JsonProcessingException {
    Map<String, Object> wrapper = new LinkedHashMap<>();
    wrapper.put("rulesVersion", version);
    wrapper.put("rules", rules);

    return OBJECT_MAPPER.writeValueAsString(wrapper);
  }

  /**
   * Merge two rule lists by pattern (key = {@code rule.getPattern()}).
   * Incoming rules overwrite same-pattern local rules at the local position;
   * incoming rules with new patterns are appended at the end.
   */
  private static List<Rule> mergeRules(List<Rule> local, List<Rule> incoming) {
    Map<String, Rule> map = new LinkedHashMap<>();
    for (Rule rule : local) {
      map.put(rule.getPattern(), rule);
    }
    for (Rule rule : incoming) {
      map.put(rule.getPattern(), rule);
    }
    return new ArrayList<>(map.values());
  }

  /**
   * Parse a JSON string into a {@code List<Rule>}, supporting both old-format
   * arrays ({@code [...]}) and new-format versioned wrappers ({@code {"rules":...}}).
   */
  private static List<Rule> parseRulesJson(String json) throws JsonProcessingException {
    String trimmed = json.trim();
    if (trimmed.startsWith("[")) {
      return OBJECT_MAPPER.readValue(json, new TypeReference<>() {});
    }

    Map<String, Object> wrapper = OBJECT_MAPPER.readValue(json, new TypeReference<>() {});
    Object raw = wrapper.get("rules");
    if (raw instanceof List<?>) {
      return OBJECT_MAPPER.convertValue(raw, new TypeReference<>() {});
    }
    return List.of();
  }

  /**
   * Explicitly broadcast the complete rule set to sibling instances.
   * <p>
   * Useful for initial cluster sync, e.g. after an operator manually
   * adjusted rules on one node and wants the whole cluster to adopt them.
   */
  public void broadcastAllLocalRulesManually() {
    loadRulesFromRedis();

    if (cacheSyncPublisher.isPresent()) {
      try {
        long version = rulesVersion.get();
        String json = serializeRules(new ArrayList<>(rulesList), version);
        cacheSyncPublisher.get().broadcastAllLocalRules(json, version);

        log.info("Rules broadcast manually (version={}), total: {}", version, rulesList.size());
      } catch (Exception e) {
        log.error("Failed to serialize rules", e);
      }
    }
  }

  /**
   * Load persisted rules from Redis (if available) and replace the local rule list
   * with the loaded rules.  Failures are logged but never propagated — the
   * application continues with the current rule set if loading fails.
   */
  private void loadRulesFromRedis() {
    redisTemplate.ifPresent(r -> {
      try {
        String json = r.opsForValue().get(REDIS_KEY_RULES);

        if (json == null || json.isEmpty()) {
          log.info("No rules found in Redis, starting with empty rule set");
          return;
        }

        List<Rule> saved = parseRulesJson(json);
        replaceRules(saved);

        log.info("Rules loaded from Redis: {} rules", saved.size());
      } catch (Exception e) {
        log.error("Failed to load rules from Redis", e);
      }
    });
  }
}
