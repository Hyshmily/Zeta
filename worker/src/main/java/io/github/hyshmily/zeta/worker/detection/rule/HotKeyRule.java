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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Comparator;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * One hot-key detection rule with its QPS thresholds pre-converted into window counts.
 *
 * <p><b>Flattened cool-down encoding.</b> {@code coolCount == 0} encodes {@code COOL_FOR_EXPIRE}
 * with its fixed HOT lifetime stored in {@code checkIntervalMs}; {@code coolCount > 0} encodes
 * {@code COOL_FOR_LOW_QPS} with its dwell duration in {@code lowQpsForMs} and re-check cadence in
 * {@code checkIntervalMs}. Because the configuration gate rejects fields unused by the active
 * strategy, the runtime reads primitive fields directly instead of dispatching through a policy
 * object.
 *
 * <p><b>Immutable binding.</b> A promoted key binds to the rule that promoted it for its entire
 * HOT lifetime; cooling always reads the captured binding rather than the live {@link RuleSet},
 * so rule-set reloads never shift the cool-down bar of an already-hot key.
 *
 * @param id              stable rule identifier used in metric tags and endpoints; unique per set
 * @param type            key matcher kind
 * @param pattern         exact key, bare namespace name, prefix, or regex source
 * @param priority        higher value wins; primary sort key (allows regex to outrank exact rules)
 * @param order           declaration order used as the final tiebreaker
 * @param hotCount        window-count threshold that promotes ({@code ceil(hotQps * W / 1000)})
 * @param coolCount       window-count ceiling of the LOW_QPS band; {@code 0} encodes COOL_FOR_EXPIRE
 * @param lowQpsForMs     dwell duration below {@code coolCount} before cooling; {@code 0} for EXPIRE
 * @param checkIntervalMs LOW_QPS re-check period, or the fixed HOT lifetime for EXPIRE
 * @param regex           compiled {@code pattern} for {@link MatchType#REGEX} (compiled in the
 *                        constructor when {@code null}), or {@code null} for non-regex matchers
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
  @Nullable Pattern regex
) implements Comparable<HotKeyRule> {
  /**
   * Precedence order: priority descending, match specificity ascending, pattern length descending,
   * then configuration declaration order ascending.
   */
  private static final Comparator<HotKeyRule> ORDER = Comparator.comparingInt(HotKeyRule::priority)
    .reversed()
    .thenComparingInt(rule -> rule.type().specificity())
    .thenComparing(Comparator.comparingInt((HotKeyRule rule) -> rule.pattern().length()).reversed())
    .thenComparingInt(HotKeyRule::order);

  /**
   * Validates required fields, enforces the flattened cool-down invariants, and compiles
   * {@link MatchType#REGEX} patterns once when not pre-compiled by the caller.
   *
   * @throws IllegalArgumentException if any required field is missing/invalid, the cool-down
   *                                  encoding is inconsistent, a {@link MatchType#NAMESPACE}
   *                                  pattern contains delimiters, or the regex fails to compile
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
    if (
      type == MatchType.NAMESPACE && (pattern.isBlank() || pattern.indexOf(':') >= 0 || pattern.indexOf('\u0000') >= 0)
    ) {
      throw new IllegalArgumentException(
        "rule '" +
          id +
          "' has a NAMESPACE pattern that is not a bare cache name; " +
          "a NAMESPACE pattern is the bare cache name, without ':' or the namespace delimiter"
      );
    }
    if (hotCount < 1L) {
      throw new IllegalArgumentException("rule '" + id + "' must promote on at least one access");
    }
    if (coolCount < 0L) {
      throw new IllegalArgumentException("rule '" + id + "' has a negative cool count");
    }
    if (lowQpsForMs < 0L) {
      throw new IllegalArgumentException("rule '" + id + "' has a negative low-QPS duration");
    }
    if (checkIntervalMs < 1L) {
      throw new IllegalArgumentException("rule '" + id + "' has a non-positive cool check interval");
    }
    if (coolCount == 0L && lowQpsForMs != 0L) {
      throw new IllegalArgumentException("rule '" + id + "' encodes COOL_FOR_EXPIRE but carries a low-QPS duration");
    }
    switch (type) {
      case REGEX -> {
        // Reuse a pre-compiled Pattern from configuration validation when supplied; compile here
        // otherwise so the record remains self-contained.
        if (regex == null) {
          regex = Pattern.compile(pattern);
        }
      }
      case EXACT, NAMESPACE, PREFIX -> {
        if (regex != null) {
          throw new IllegalArgumentException("rule '" + id + "' carries a compiled regex for a non-regex match");
        }
      }
    }
  }

  /** Tests whether {@code key} is selected by this rule. */
  public boolean matches(String key) {
    return type.matches(pattern, regex, key);
  }

  /** Returns {@code true} if {@code windowSum} meets or exceeds {@link #hotCount()}. */
  public boolean eligible(long windowSum) {
    return windowSum >= hotCount;
  }

  /** Returns {@code true} if this rule cools on a fixed lifetime rather than a low-QPS dwell. */
  public boolean isExpire() {
    return coolCount == 0L;
  }

  /**
   * Computes the first cool-down check deadline for a newly promoted entry: one re-check interval
   * for {@code COOL_FOR_LOW_QPS}, or the fixed HOT lifetime for {@code COOL_FOR_EXPIRE}.
   *
   * @param nowMs promotion timestamp in monotonic milliseconds
   * @return deadline in monotonic milliseconds, saturated at {@link Long#MAX_VALUE}
   */
  public long firstDue(long nowMs) {
    return saturatedAdd(nowMs, checkIntervalMs);
  }

  /**
   * Computes the next re-check deadline for a {@code COOL_FOR_LOW_QPS} entry still below its band:
   * the earlier of the next periodic check and the dwell expiry deadline, both saturated to
   * prevent wrap-around into the past.
   *
   * @param nowMs      current monotonic timestamp in milliseconds
   * @param belowSince monotonic timestamp when the entry first dropped below {@link #coolCount()}
   * @return next deadline in monotonic milliseconds
   */
  public long nextDue(long nowMs, long belowSince) {
    return Math.min(saturatedAdd(nowMs, checkIntervalMs), saturatedAdd(belowSince, lowQpsForMs));
  }

  /**
   * Returns the canonical cool-down parameter summary used in {@link #definition()}:
   * {@code expire:<checkIntervalMs>} or
   * {@code low_qps:<coolCount>:<lowQpsForMs>:<checkIntervalMs>}.
   */
  public String describeCool() {
    return isExpire()
      ? "expire:" + checkIntervalMs
      : "low_qps:" + coolCount + ":" + lowQpsForMs + ":" + checkIntervalMs;
  }

  /**
   * Returns the length-prefixed canonical fingerprint text of all behavioural parameters
   * (excluding {@code order}, which is already encoded by the ordered list in {@link RuleSet}).
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
   * Adds a non-negative duration to a monotonic instant, saturating at {@link Long#MAX_VALUE}
   * instead of wrapping into the past.
   */
  static long saturatedAdd(long instant, long positiveMs) {
    long sum = instant + positiveMs;
    return sum < instant ? Long.MAX_VALUE : sum;
  }

  /** Compares rules by {@link #ORDER}. */
  @Override
  public int compareTo(@NonNull HotKeyRule other) {
    Objects.requireNonNull(other, "other must not be null");
    return ORDER.compare(this, other);
  }

  /**
   * Compares all behavioural fields and {@link #order()}, deliberately excluding {@link #regex()}
   * because {@link Pattern} uses reference equality and is fully determined by {@code type} and
   * {@code pattern}.
   */
  @Override
  public boolean equals(@Nullable Object other) {
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

  /** Computes a hash code consistent with {@link #equals(Object)}, excluding {@link #regex()}. */
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
}
