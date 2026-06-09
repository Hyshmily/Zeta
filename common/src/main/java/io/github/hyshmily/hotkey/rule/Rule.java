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

import java.util.UUID;
import java.util.regex.Pattern;
import lombok.Data;

/**
 * A cache access rule that defines how a key pattern should be handled.
 * Supports exact match, prefix match, wildcard glob ( *, ? ), and regex patterns.
 * Rules are evaluated in order and the first match determines the action.
 */
@Data
public class Rule {

  /** Supported pattern types for key matching. */
  public enum RuleType {
    /** Exact string equality. */
    EXACT,
    /** Starts-with prefix match. */
    PREFIX,
    /** Glob-style pattern ({@code *} = any, {@code ?} = single character). */
    WILDCARD,
    /** Full regular expression (via {@link java.util.regex.Pattern}). */
    REGEX,
  }

  /** Action to take when a key matches a rule. */
  public enum RuleAction {
    /** Reject access and throw {@link io.github.hyshmily.hotkey.exception.HotKeyBlockedException}. */
    BLOCK,
    /** Allow access but skip app-to-Worker reporting. */
    ALLOW_NO_REPORT,
    /** Allow access with full processing (detection + reporting). */
    ALLOW,
  }

  private String id;
  private long createdAt;
  private RuleType type;
  private String pattern;
  private RuleAction action;
  /** Compiled regex for WILDCARD and REGEX types; lazily initialised. */
  private transient volatile Pattern compiledPattern;

  /** No-arg constructor for frameworks (e.g. Jackson deserialisation). */
  public Rule() {}

  /**
   * Construct a rule with the given type, pattern, and action.
   * Pre-compiles the regex immediately for REGEX type rules.
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
   * Test whether the given key matches this rule.
   *
   * @param key the cache key to test
   * @return {@code true} if the key matches this rule's pattern
   */
  public boolean match(String key) {
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
   * Lazily compile the internal pattern if not already compiled.
   * For REGEX type, the pattern is used as-is.
   * For WILDCARD type, converts glob syntax ( {@code *}, {@code ?} ) to regex.
   */
  public void prepare() {
    if (type == RuleType.REGEX) {
      compiledPattern = Pattern.compile(pattern);
    } else if (type == RuleType.WILDCARD) {
      String regex = pattern.replaceAll("([.+^$\\[\\]\\\\(){}|])", "\\\\$1").replace("*", ".*").replace("?", ".");
      compiledPattern = Pattern.compile(regex);
    }
  }
}
