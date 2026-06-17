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
import lombok.extern.slf4j.Slf4j;

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.REDIS_KEY_RULES;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
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
 * Central component for evaluating cache-key rules that govern blocking,
 * reporting suppression, and other access-control decisions.
 *
 * <p><b>Rule lifecycle:</b> Rules are maintained in a thread-safe
 * {@link CopyOnWriteArrayList} and evaluated in insertion order on every
 * cache {@code get} / {@code getWithSoftExpire} invocation. The first
 * matching rule determines the action.
 *
 * <p><b>Persistence and synchronization:</b> Persistence is optional. If a
 * {@link StringRedisTemplate} is available, the current rule set is written
 * to Redis on every local change (via a Lua compare-and-set script that
 * guards against stale-write races) and reloaded at startup. If Redis is
 * unavailable, rules remain purely in-memory — they survive as long as at
 * least one application node holds them and can be re-broadcast to siblings
 * through the {@link CacheSyncPublisher}.
 *
 * <p><b>Cross-instance sync:</b> When a rule is added, removed, or cleared,
 * the change is both persisted to Redis <em>and</em> broadcast via AMQP
 * (both/and, not XOR). Incoming broadcasts from sibling instances are
 * merged via pattern-based merge (incoming overwrites same-pattern local
 * rules) and guarded by a monotonically increasing version counter.
 *
 * <p>Thread-safe. The rule list is a volatile {@link CopyOnWriteArrayList};
 * all mutating methods synchronize via the list's snapshot semantics. The
 * version counter is an {@link java.util.concurrent.atomic.AtomicLong}.
 */
@RequiredArgsConstructor
@Slf4j
public class RuleMatcher {


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
   * Load persisted rules from Redis (if available) at bean initialization time.
   *
   * <p>Called automatically by the Spring container after dependency injection
   * ({@link PostConstruct}). If Redis is not configured or is unreachable,
   * the rule list starts empty and the application operates normally with
   * out-of-memory-only rules. Any errors during loading are logged but do
   * not prevent bean initialization.
   *
   * @see #loadRulesFromRedis
   */
  @PostConstruct
  void initRules() {
    loadRulesFromRedis();
  }

  /**
   * Convenience factory method that auto-detects the appropriate
   * {@link Rule.RuleType} from the pattern string.
   *
   * <p>Type auto-detection logic:
   * <pre>{@code
   * RuleMatcher.of("user:123", BLOCK)                  -> EXACT
   * RuleMatcher.of("temp:*", BLOCK)                   -> PREFIX (single trailing '*')
   * RuleMatcher.of("order:*-detail", ALLOW_NO_REPORT)  -> WILDCARD (interior wildcard)
   * RuleMatcher.of("regex:user:\\d+", BLOCK)           -> REGEX (detected via 'regex:' prefix)
   * }</pre>
   *
   * <p>The {@code regex:} prefix is stripped before creating the rule, so
   * the stored pattern is the pure regex without the prefix.
   *
   * @param pattern the key pattern string; detection rules are:
   *     <ul>
   *       <li>A leading {@code regex:} prefix forces {@link Rule.RuleType#REGEX}</li>
   *       <li>A single trailing {@code *} without interior wildcards or
   *           {@code ?} is optimized to {@link Rule.RuleType#PREFIX}</li>
   *       <li>Patterns containing {@code *} or {@code ?} are treated as
   *           {@link Rule.RuleType#WILDCARD}</li>
   *       <li>Otherwise the pattern is treated as {@link Rule.RuleType#EXACT}</li>
   *     </ul>
   * @param action  the action to assign to the created rule
   * @return a new {@link Rule} with auto-detected type, unique ID, and
   *         creation timestamp
   * @throws java.util.regex.PatternSyntaxException if the {@code regex:}
   *         prefix is used and the pattern is not a valid regular expression
   *         (propagated from the {@link Rule} constructor)
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
   * Append a rule to the end of the rule list.
   *
   * <p>The change is immediately visible in the local rule list (snapshot
   * semantics via {@link CopyOnWriteArrayList}), persisted to Redis via
   * Lua compare-and-set (if available), and broadcast to sibling
   * application instances via AMQP. The local version counter is
   * incremented atomically.
   *
   * @param rule the rule to append; must not be {@code null}. The rule is
   *             {@link Rule#prepare() prepared} before insertion to ensure
   *             its internal pattern is compiled
   */
  public void addRule(Rule rule) {
    rule.prepare();
    rulesList.add(rule);
    rulesVersion.incrementAndGet();
    persistAndBroadcastRules();
  }

  /**
   * Remove all rules whose pattern and action both match the given values.
   *
   * <p>If at least one rule is removed, the change is persisted to Redis
   * and broadcast to sibling instances. The version counter is incremented.
   *
   * <p>Matching is based on exact equality of both {@code pattern} and
   * {@code action}; rules with the same pattern but a different action
   * are <em>not</em> removed.
   *
   * @param pattern the pattern string to match ({@link Rule#pattern})
   * @param action  the action to match ({@link Rule#action})
   * @return {@code true} if at least one rule was removed; {@code false}
   *         if no matching rule was found
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
   * Remove all rules and propagate the empty rule list.
   *
   * <p>After clearing, the rule list is empty, the version counter is
   * incremented, and the empty list is persisted to Redis and broadcast
   * to sibling instances. This ensures that all nodes converge to the
   * empty state.
   */
  public void clearRules() {
    rulesList.clear();
    rulesVersion.incrementAndGet();

    persistAndBroadcastRules();
    log.info("All rules cleared and broadcasted");
  }

  /**
   * Remove all rules whose action equals the given value.
   *
   * <p>If any rules are removed, the change is persisted and broadcast.
   * The version counter is incremented with each batch removal.
   *
   * @param action the action to match; all rules with this action are
   *               removed regardless of their pattern
   * @return the number of rules removed (zero if no matching rules exist)
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
   * Remove the rule at the given index in the current rule list.
   *
   * <p>If the index is valid (non-negative and less than the current list
   * size), the rule is removed and the change is persisted and broadcast.
   * Out-of-bounds indices are silently ignored (logged at DEBUG level
   * by the caller).
   *
   * @param index the zero-based index of the rule to remove
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
   * Serialize a rule list and its associated version into the versioned JSON
   * wrapper format.
   *
   * <p>The output format is a JSON object with two top-level keys:
   * <pre>{@code {"rulesVersion":5,"rules":[{"pattern":"x","action":"BLOCK"}]}}</pre>
   *
   * <p>This wrapper format is used for both Redis persistence and AMQP
   * broadcast, enabling the receiver to apply version-gated merge logic.
   *
   * @param rules   the rule list to serialize (a snapshot, typically via
   *                {@link #getAllRules()})
   * @param version the current rules version to embed in the wrapper
   * @return the JSON string representation
   * @throws JsonProcessingException if Jackson serialization fails
   */
  private String serializeRules(List<Rule> rules, long version) throws JsonProcessingException {
    Map<String, Object> wrapper = new LinkedHashMap<>();
    wrapper.put("rulesVersion", version);
    wrapper.put("rules", rules);

    return OBJECT_MAPPER.writeValueAsString(wrapper);
  }

  /**
   * Merge two rule lists by pattern identity ({@link Rule#getPattern()}).
   *
   * <p>The merge semantics follow a "last writer wins" strategy on a per-pattern
   * basis: for each pattern in the incoming list, the incoming rule replaces
   * any local rule with the same pattern. Local rules whose patterns are not
   * present in the incoming list are preserved. The order is determined by
   * insertion order in the merged map (local rules first, then incoming rules
   * overwrite/appending at the end).
   *
   * <p>This strategy ensures that a rule removal on one node is not
   * reintroduced by a stale broadcast from another node: if the incoming set
   * does not contain a pattern, the local version is retained.
   *
   * @param local    the current local rule list
   * @param incoming the incoming rule list from a broadcast or Redis load
   * @return a new merged rule list combining both sources
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
   * Parse a JSON string into a {@code List<Rule>}, supporting both the
   * legacy array format and the current versioned wrapper format.
   *
   * <p><b>Legacy format:</b> a plain JSON array of rule objects, e.g.
   * <pre>{@code [{"pattern":"x","action":"BLOCK"}]}</pre>
   *
   * <p><b>Current format:</b> a JSON object with a {@code "rules"} key
   * containing the array, e.g.
   * <pre>{@code {"rulesVersion":5,"rules":[{"pattern":"x","action":"BLOCK"}]}}</pre>
   *
   * <p>The format is auto-detected by examining the first character:
   * {@code [} indicates legacy array format, {@code \{} indicates the
   * versioned wrapper. Backward compatibility is maintained to support
   * rolling upgrades.
   *
   * @param json the JSON string to parse; may be {@code null} or empty
   *             (in which case an empty list is returned upstream by the
   *             caller's null check)
   * @return the parsed list of rules; never {@code null} (returns an
   *         empty list if the wrapper format lacks a {@code "rules"} key
   *         or if the value is not an array)
   * @throws JsonProcessingException if the JSON is malformed
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
   * Explicitly reload rules from Redis and broadcast the full rule set to
   * all sibling application instances via AMQP.
   *
   * <p>This method is useful for operational scenarios such as:
   * <ul>
   *   <li>An operator manually adjusted rules on one node and wants the
   *       entire cluster to converge immediately</li>
   *   <li>Recovering from a missed broadcast during a temporary network
   *       partition</li>
   *   <li>Initial cluster synchronization after a new node joins</li>
   * </ul>
   *
   * <p>The rule list is first reloaded from Redis (to pick up any
   * externally written changes), then serialized with the current version
   * and broadcast. If no {@link CacheSyncPublisher} is configured, the
   * method logs a warning and returns without broadcasting.
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
   * Load persisted rules from Redis (if available) and atomically replace
   * the local rule list with the loaded rules.
   *
   * <p>If Redis is not configured, this method is a no-op. If the Redis
   * fetch or JSON parsing fails, the error is logged and the current
   * in-memory rule list is preserved — the application continues operating
   * with the existing rules.
   *
   * <p>This method is called at startup via {@link #initRules()} and can
   * also be invoked manually via {@link #broadcastAllLocalRulesManually()}
   * to refresh from Redis on demand.
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
