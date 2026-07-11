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
package io.github.hyshmily.zeta.rule;

import io.github.hyshmily.zeta.rule.Rule.RuleAction;
import java.util.List;
import java.util.Optional;

/**
 * Central component for evaluating cache-key rules that govern blocking,
 * reporting suppression, and other access-control decisions.
 */
public interface RuleMatcher {
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
  static Rule of(String pattern, RuleAction action) {
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

  /** Add a rule to the end of the rule list. */
  void addRule(Rule rule);

  /** Remove the first rule matching the given pattern and action. Returns {@code true} if removed. */
  boolean removeRule(String pattern, Rule.RuleAction action);

  /** Remove all rules. */
  void clearRules();

  /** Remove all rules with the given action. Returns the number removed. */
  int removeRulesByAction(Rule.RuleAction action);

  /** Remove a rule by its index in the list. */
  void removeRule(int index);

  /** Synchronize the rule list from a JSON string with version checking. */
  void syncRules(String json, long incomingVersion);

  /** Replace all rules with the given list. */
  void replaceRules(List<Rule> newRules);

  /** Return an unmodifiable view of all current rules. */
  List<Rule> getAllRules();

  /**
   * Check whether reporting is suppressed for the given key and operation.
   *
   * @return {@code Optional.empty()} if no matching rule; {@code Optional.of(true/false)} if matched
   */
  Optional<Boolean> isAllowNoReport(String cacheKey, String operation);

  /** Evaluate rules for the given cache key, returning the action of the first match. */
  Rule.RuleAction evaluateRule(String cacheKey);

  /** Broadcast all local rules to other instances via the sync publisher. */
  void broadcastAllLocalRulesManually();
}
