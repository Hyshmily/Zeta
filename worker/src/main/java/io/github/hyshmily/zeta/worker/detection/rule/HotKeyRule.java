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

import java.util.Comparator;
import java.util.regex.Pattern;
import org.jspecify.annotations.NonNull;

/**
 * One detection rule, with its QPS thresholds already converted to window counts.
 *
 * <p>The cool-down policy is flattened into the rule: {@code coolCount == 0} encodes
 * {@code COOL_FOR_EXPIRE} and stores its lifetime in {@code checkIntervalMs}; {@code coolCount > 0}
 * encodes {@code COOL_FOR_LOW_QPS} and stores the dwell duration in {@code lowQpsForMs}. The
 * configuration gate rejects the fields a strategy does not use, so the sentinel cannot be produced
 * by a valid YAML. The runtime therefore reads primitive fields instead of dispatching through a
 * policy object.
 *
 * <p>A key is bound to the rule that promoted it for the rest of its HOT life: cooling reads the
 * entry's captured binding and never the current {@link RuleSet}, so replacing the rule set cannot
 * change the bar an already-hot key is measured against (its cool-down still completes against the
 * rule that promoted it).
 *
 * @param id               stable identifier used in metrics tags and the endpoint; unique within a set
 * @param type             matcher kind
 * @param pattern          exact key, prefix, or regex source
 * @param priority         higher wins; the primary sort key, so a regex rule can outrank an exact one
 * @param order            configuration order, the final tiebreak
 * @param hotCount         window-count threshold that promotes ({@code ceil(hotQps * W / 1000)})
 * @param coolCount        window-count ceiling of the LOW_QPS band; {@code 0} encodes COOL_FOR_EXPIRE
 * @param lowQpsForMs      how long the key must stay below {@code coolCount}; {@code 0} for EXPIRE
 * @param checkIntervalMs  LOW_QPS re-check period; for EXPIRE, the HOT lifetime ({@code cool-after})
 * @param regex            the compiled {@code pattern} for {@link MatchType#REGEX} (compiled here
 *                         when {@code null}), otherwise {@code null}
 */
public record HotKeyRule(
  String id,
  MatchType type,
  String pattern,
  int priority,
  int order,
  long hotCount,
  long coolCount,
  long lowQpsForMs,
  long checkIntervalMs,
  Pattern regex
) implements Comparable<HotKeyRule> {
  /**
   * The full rule order: priority descending, then match specificity, then pattern length
   * descending, then configuration order.
   */
  private static final Comparator<HotKeyRule> ORDER = Comparator.comparingInt(HotKeyRule::priority)
    .reversed()
    .thenComparingInt(rule -> rule.type().specificity())
    .thenComparing(Comparator.comparingInt((HotKeyRule rule) -> rule.pattern().length()).reversed())
    .thenComparingInt(HotKeyRule::order);

  /**
   * Compact constructor: enforces the fields every rule needs, the flattened cool-down encoding and
   * compiles the regex once.
   *
   * @throws IllegalArgumentException when a required field is missing, the cool-down encoding is
   *                                  inconsistent, or the regex does not compile
   */
  public HotKeyRule {
    if (id == null || id.isBlank()) {
      throw new IllegalArgumentException("rule id must not be blank");
    }
    if (pattern == null || pattern.isEmpty()) {
      throw new IllegalArgumentException("rule '" + id + "' has an empty pattern");
    }
    if (type == null) {
      throw new IllegalArgumentException("rule '" + id + "' has no match type");
    }
    if (hotCount < 1) {
      throw new IllegalArgumentException("rule '" + id + "' must promote on at least one access");
    }
    if (coolCount < 0) {
      throw new IllegalArgumentException("rule '" + id + "' has a negative cool count");
    }
    if (lowQpsForMs < 0) {
      throw new IllegalArgumentException("rule '" + id + "' has a negative low-QPS duration");
    }
    if (checkIntervalMs < 1) {
      throw new IllegalArgumentException("rule '" + id + "' has a non-positive cool check interval");
    }
    if (coolCount == 0 && lowQpsForMs != 0) {
      throw new IllegalArgumentException("rule '" + id + "' encodes COOL_FOR_EXPIRE but carries a low-QPS duration");
    }
    switch (type) {
      case REGEX -> {
        // A caller that already compiled the pattern (the configuration validator) hands it over so
        // the expression is parsed once; the null case keeps the record usable on its own.
        if (regex == null) {
          regex = Pattern.compile(pattern);
        }
      }
      case EXACT, PREFIX -> {
        if (regex != null) {
          throw new IllegalArgumentException("rule '" + id + "' carries a compiled regex for a non-regex match");
        }
      }
    }
  }

  /**
   * Tests one key against this rule.
   *
   * @param key the cache key
   * @return {@code true} when the rule selects the key
   */
  public boolean matches(String key) {
    return type.matches(pattern, regex, key);
  }

  /** @return whether the rule cools on a fixed lifetime instead of a low-QPS dwell */
  public boolean isExpire() {
    return coolCount == 0L;
  }

  /**
   * The first check instant for a newly promoted entry. Both strategies are the same arithmetic:
   * LOW_QPS waits one re-check interval, EXPIRE waits its configured lifetime.
   *
   * @param nowMs the promotion instant, monotonic milliseconds
   * @return the deadline, monotonic milliseconds
   */
  public long firstDue(long nowMs) {
    return saturatedAdd(nowMs, checkIntervalMs);
  }

  /**
   * Whether a window sum clears this rule's promotion threshold.
   *
   * @param windowSum the reported window sum
   * @return {@code true} when the rule is eligible
   */
  public boolean eligible(long windowSum) {
    return windowSum >= hotCount;
  }

  /**
   * The next re-check instant for a LOW_QPS entry that is still below its band: the earlier of the
   * next periodic check and the cool deadline. Both sums saturate instead of wrapping a very large
   * configured duration into a past instant, which would re-arm the wheel immediately and spin.
   *
   * @param nowMs      the current monotonic sample
   * @param belowSince the instant the entry first dropped below the band
   * @return the next deadline, monotonic milliseconds
   */
  public long nextDue(long nowMs, long belowSince) {
    return Math.min(saturatedAdd(nowMs, checkIntervalMs), saturatedAdd(belowSince, lowQpsForMs));
  }

  /**
   * The cool-down parameters in canonical form, as they participate in {@link #definition()} and
   * therefore in the rule-set fingerprint: {@code expire:<checkIntervalMs>} for
   * {@code COOL_FOR_EXPIRE}, {@code low_qps:<coolCount>:<lowQpsForMs>:<checkIntervalMs>} for
   * {@code COOL_FOR_LOW_QPS}.
   *
   * @return the canonical cool-down text
   */
  public String describeCool() {
    return isExpire()
      ? "expire:" + checkIntervalMs
      : "low_qps:" + coolCount + ":" + lowQpsForMs + ":" + checkIntervalMs;
  }

  /**
   * The canonical text of every parameter that decides this rule's behaviour: identity, matcher,
   * priority, promotion threshold and cool-down encoding. {@code order} is deliberately excluded —
   * it is only the final precedence tiebreak, and the snapshot's ordered rule list already reflects
   * it, so a {@link RuleBinding} does not depend on it.
   *
   * <p>{@link RuleSet} fingerprints this text. Every string field is length-prefixed, so an
   * operator-supplied {@code id} or {@code pattern} containing a delimiter cannot make two
   * different rules produce the same definition. {@link RuleResolver} compares fields, not this
   * text, when it decides whether a published rule is unchanged.
   *
   * @return the canonical text, without a trailing newline
   */
  String definition() {
    return String.join(
      "|",
      id.length() + ":" + id,
      type.name(),
      pattern.length() + ":" + pattern,
      Integer.toString(priority),
      Long.toString(hotCount),
      describeCool()
    );
  }

  /**
   * Two rules are equal when every behavioural field and the configuration order match. The
   * compiled {@link #regex} is deliberately excluded: it is fully determined by {@code type} and
   * {@code pattern}, and {@link Pattern} uses identity equality, so including it would make two
   * identically configured regex rules compare unequal. Comparing the fields directly keeps
   * equality allocation-free; {@link #definition()} is only the fingerprint's canonical text.
   *
   * @param other the object to compare with
   * @return {@code true} when both rules carry the same fields and order
   */
  @Override
  public boolean equals(Object other) {
    return (
      this == other ||
      (other instanceof HotKeyRule that &&
        priority == that.priority &&
        order == that.order &&
        hotCount == that.hotCount &&
        coolCount == that.coolCount &&
        lowQpsForMs == that.lowQpsForMs &&
        checkIntervalMs == that.checkIntervalMs &&
        type == that.type &&
        id.equals(that.id) &&
        pattern.equals(that.pattern))
    );
  }

  /**
   * @return a hash consistent with {@link #equals(Object)}
   */
  @Override
  public int hashCode() {
    int result = id.hashCode();
    result = 31 * result + type.hashCode();
    result = 31 * result + pattern.hashCode();
    result = 31 * result + priority;
    result = 31 * result + order;
    result = 31 * result + Long.hashCode(hotCount);
    result = 31 * result + Long.hashCode(coolCount);
    result = 31 * result + Long.hashCode(lowQpsForMs);
    result = 31 * result + Long.hashCode(checkIntervalMs);
    return result;
  }

  /**
   * Adds a positive duration to a monotonic instant, saturating at {@link Long#MAX_VALUE} instead
   * of wrapping into the past.
   *
   * @param instant    the monotonic instant
   * @param positiveMs the positive duration in milliseconds
   * @return the sum, or {@link Long#MAX_VALUE} when it would overflow
   */
  static long saturatedAdd(long instant, long positiveMs) {
    long sum = instant + positiveMs;
    return sum < instant ? Long.MAX_VALUE : sum;
  }

  @Override
  public int compareTo(@NonNull HotKeyRule other) {
    return ORDER.compare(this, other);
  }
}
