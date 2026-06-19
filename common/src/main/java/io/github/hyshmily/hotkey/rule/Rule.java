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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;
import java.util.regex.Pattern;
import lombok.Data;

/**
 * A cache access rule that defines how a key pattern should be handled
 * during cache operations.
 *
 * <p>Supports four pattern types: exact match, prefix match, wildcard glob
 * ({@code *}, {@code ?}), and regular expression. Rules are evaluated in
 * insertion order and the first matching rule determines the action taken.
 *
 * <p>Each rule carries a unique identifier and a creation timestamp for
 * tracking and cross-instance synchronization. Rules are serialized to
 * JSON for Redis persistence and AMQP broadcast.
 *
 * <p>Instances are mutable (for Jackson deserialization) but should be
 * treated as effectively immutable after construction and registration
 * with {@link RuleMatcher}.
 *
 * @see RuleMatcher
 * @see RuleAction
 * @see RuleType
 */
@Data
public class Rule {

  /**
   * Supported pattern types for matching cache keys against a rule.
   *
   * <p>The pattern type determines how the rule's {@link #pattern} string
   * is compared against candidate cache keys. Choosing the appropriate type
   * optimizes evaluation performance: EXACT and PREFIX are O(1) / O(n)
   * string operations, while WILDCARD and REGEX require regex compilation
   * and matching.
   */
  public enum RuleType {
    /** Exact string equality via {@link String#equals(Object)}. Fastest match type. */
    EXACT,
    /** Prefix (starts-with) match via {@link String#startsWith(String)}. */
    PREFIX,
    /**
     * Glob-style pattern matching.
     * Supports {@code *} (any sequence of characters) and {@code ?}
     * (any single character). The glob is converted to a regular expression
     * via {@link Rule#prepare()} before matching.
     */
    WILDCARD,
    /**
     * Full Java regular expression matching via {@link java.util.regex.Pattern}.
     * The pattern is compiled eagerly in the constructor and lazily on first
     * access via {@link Rule#prepare()}. Use the {@code regex:} prefix in
     * {@link RuleMatcher#of} to auto-detect this type.
     */
    REGEX,
  }

  /**
   * Action to take when a cache key matches a rule.
   *
   * <p>The action determines how the cache layer handles the requested
   * operation. Actions are evaluated in rule insertion order; the first
   * matching rule's action is applied.
   */
  public enum RuleAction {
    /**
     * Reject access immediately and throw
     * {@link io.github.hyshmily.hotkey.exception.HotKeyBlockedException}.
     * Used for blacklisting specific keys or patterns.
     */
    BLOCK,
    /**
     * Allow the cache access but <em>suppress</em> the app-to-Worker
     * hot-key report for this key. Useful for keys that should not
     * contribute to hot-key detection (e.g., health-check endpoints,
     * high-frequency operational keys).
     */
    ALLOW_NO_REPORT,
    /**
     * Allow the access with full processing: the key participates in
     * local hot-key detection and app-to-Worker reporting as normal.
     * This is the default action when no rule matches.
     */
    ALLOW,
  }

  /** Unique identifier for this rule. */
  private String id;
  /** Timestamp (epoch millis) when this rule was created. */
  private long createdAt;
  /** Pattern type used to match cache keys. Defaults to {@link RuleType#EXACT} for Jackson deserialization. */
  @JsonProperty(defaultValue = "EXACT")
  private RuleType type = RuleType.EXACT;
  /** Pattern string (exact value, prefix, glob, or regex). */
  private String pattern;
  /** Action to take when a key matches this rule. */
  private RuleAction action;
  /** Compiled regex for WILDCARD and REGEX types; lazily initialised. */
  private transient volatile Pattern compiledPattern;

  /**
   * No-arg constructor for frameworks (e.g. Jackson deserialisation).
   */
  public Rule() {}

  /**
   * Construct a rule with the given type, pattern, and action.
   *
   * <p>For {@link RuleType#REGEX} rules, the pattern is compiled eagerly
   * in the constructor so that invalid regex patterns are detected at
   * construction time rather than at match time. For other types,
   * compilation is deferred to {@link #prepare()}.
   *
   * <p>A unique identifier ({@link UUID}) and creation timestamp
   * ({@link System#currentTimeMillis()}) are automatically assigned.
   *
   * @param type    the pattern type — {@link RuleType#EXACT}, {@link RuleType#PREFIX},
   *                {@link RuleType#WILDCARD}, or {@link RuleType#REGEX}
   * @param pattern the pattern string to match against cache keys; for REGEX
   *                type, this must be a valid Java regular expression
   * @param action  the action to take when a cache key matches this rule
   * @throws java.util.regex.PatternSyntaxException if {@code type} is {@link RuleType#REGEX}
   *         and the given pattern is not a valid regular expression
   */
  public Rule(RuleType type, String pattern, RuleAction action) {
    this.id = UUID.randomUUID().toString();
    this.createdAt = System.currentTimeMillis();
    this.type = type;
    this.pattern = pattern;
    this.action = action;
    if (type == RuleType.REGEX) {
      this.compiledPattern = Pattern.compile(pattern);
    }
  }

  /**
   * Test whether the given cache key matches this rule's pattern.
   *
   * <p>Matching behavior depends on the rule type:
   * <ul>
   *   <li>{@code null} type — returns {@code false} (rule is effectively disabled); may
   *       occur when {@link RuleType} is absent during deserialization</li>
   *   <li>{@link RuleType#EXACT} — exact string equality via {@link String#equals}</li>
   *   <li>{@link RuleType#PREFIX} — prefix check via {@link String#startsWith}</li>
   *   <li>{@link RuleType#WILDCARD} — converts the glob to a regex (if not already
   *       compiled) and performs a full-region {@link java.util.regex.Matcher#matches}</li>
   *   <li>{@link RuleType#REGEX} — uses {@link java.util.regex.Matcher#find} for
   *       substring matching</li>
   * </ul>
   *
   * @param key the cache key to test against this rule; must not be {@code null}
   * @return {@code true} if the key matches this rule's pattern and the
   *         rule's action should be applied; {@code false} if the rule's type is
   *         {@code null} (untyped rule is treated as non-matching)
   * @throws NullPointerException if {@code key} is {@code null}
   * @throws java.util.regex.PatternSyntaxException if the underlying pattern
   *         (REGEX or WILDCARD) has not been {@link #prepare() prepared} yet
   *         and compilation fails
   */
  public boolean match(String key) {
    if (type == null) return false;
    return switch (type) {
      case EXACT -> key.equals(pattern);
      case PREFIX -> key.startsWith(pattern);
      case WILDCARD -> {
        if (compiledPattern == null) {
          prepare();
        }
        yield compiledPattern.matcher(key).matches();
      }
      case REGEX -> {
        if (compiledPattern == null) {
          prepare();
        }
        yield compiledPattern.matcher(key).find();
      }
    };
  }

  /**
   * Lazily compile the internal regex pattern if not already compiled.
   *
   * <p>For {@link RuleType#REGEX} rules, the pattern is used as-is.
   * For {@link RuleType#WILDCARD} rules, the glob metacharacters
   * ({@code *} → {@code .*}, {@code ?} → {@code .}) are converted to their
   * regex equivalents, and all other regex special characters
   * ({@code . + ^ $ [ ] \ ( ) { } |}) are escaped.
   *
   * <p>This method is safe for concurrent calls: {@link #compiledPattern}
   * is {@code volatile}, and {@link java.util.regex.Pattern#compile} is
   * idempotent (compiling the same pattern twice produces identical,
   * interchangeable objects).
   *
   * <p>Has no effect on {@link RuleType#EXACT} or {@link RuleType#PREFIX}
   * rules, which do not use regex matching.
   *
   * @throws java.util.regex.PatternSyntaxException if the type is
   *         {@link RuleType#REGEX} or {@link RuleType#WILDCARD} and the
   *         pattern cannot be compiled into a valid regular expression
   */
  public void prepare() {
    if (type == null) return;
    if (type == RuleType.REGEX) {
      compiledPattern = Pattern.compile(pattern);
    } else if (type == RuleType.WILDCARD) {
      String regex = pattern.replaceAll("([.+^$\\[\\]\\\\(){}|])", "\\\\$1").replace("*", ".*").replace("?", ".");
      compiledPattern = Pattern.compile(regex);
    }
  }
}
