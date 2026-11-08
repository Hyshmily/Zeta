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
package io.github.hyshmily.zeta.worker.detection.rule;

import java.util.regex.Pattern;

/**
 * How a rule's pattern selects keys.
 *
 * <p>Specificity is also the second sort key of a {@link RuleSet} (after {@code priority}):
 * an exact rule outranks a prefix rule, which outranks a regex rule at the same priority.
 */
public enum MatchType {
  /** {@code key.equals(pattern)}. */
  EXACT(0),
  /** {@code key.startsWith(pattern)}. */
  PREFIX(1),
  /** {@code pattern.matcher(key).matches()} — a whole-key match. */
  REGEX(2);

  private final int specificity;

  MatchType(int specificity) {
    this.specificity = specificity;
  }

  /** Lower is more specific; used only as a deterministic tiebreak inside one priority. */
  public int specificity() {
    return specificity;
  }

  /**
   * Tests one key against this rule's pattern.
   *
   * @param pattern the configured pattern; never {@code null}
   * @param regex   the compiled pattern for {@link #REGEX}, {@code null} for the other types
   * @param key     the cache key
   * @return {@code true} when the rule selects the key
   */
  public boolean matches(String pattern, Pattern regex, String key) {
    return switch (this) {
      case EXACT -> key.equals(pattern);
      case PREFIX -> key.startsWith(pattern);
      case REGEX -> regex.matcher(key).matches();
    };
  }
}
