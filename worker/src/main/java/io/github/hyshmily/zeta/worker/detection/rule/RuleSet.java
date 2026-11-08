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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

/**
 * An immutable, totally ordered rule snapshot plus its derived promotion floor and fingerprint.
 *
 * <p>The snapshot is what the resolver publishes; replacing it publishes a new instance, so a
 * reader never sees half of an edit. The precedence order, the promotion floor and the fingerprint
 * are derived in the constructor, so they cannot disagree with the rules.
 *
 * <p>{@link #hash()} is a fingerprint of the effective rule text, computed once per publication and
 * exposed by the Worker's detection endpoint: every Worker loading the same YAML computes the same
 * value, so comparing two Workers' endpoints is the observable signal that they judge traffic
 * differently.
 */
public final class RuleSet {

  private final List<HotKeyRule> rules;
  private final HotKeyRule defaultRule;
  private final List<HotKeyRule> allRules;
  private final long minHotCount;
  private final String hash;

  /**
   * Derives every fact the snapshot exposes from the already-sorted rules. The constructor is
   * private so the order, floor and fingerprint can only be produced together; {@link #of} is the
   * single entry point.
   *
   * @param ordered     the rules in precedence order, excluding the default
   * @param defaultRule the rule unmatched keys resolve to
   */
  private RuleSet(List<HotKeyRule> ordered, HotKeyRule defaultRule) {
    this.rules = List.copyOf(ordered);
    this.defaultRule = defaultRule;
    this.allRules = allOf(this.rules, defaultRule);
    this.minHotCount = floor(this.rules, defaultRule);
    this.hash = fingerprint(canonical(this.allRules));
  }

  /**
   * Builds a snapshot from an unordered rule list: sorts the rules into precedence order, derives
   * the promotion floor, and fingerprints the canonical text, all once.
   *
   * <p>This is the only factory. It keeps the derived facts — order, floor and fingerprint —
   * consistent by construction, so a caller cannot publish a snapshot whose floor disagrees with
   * its rules or whose hash describes a temporary object.
   *
   * <p>Rule ids must be unique across {@code rules} and the default rule: the resolver keys its
   * counters and meter series by id, so a duplicate would make two rules share one counter set.
   * The configuration gate rejects a duplicate; this factory trusts that boundary.
   *
   * @param rules       the configured rules, in any order
   * @param defaultRule the rule unmatched keys resolve to; always present
   * @return the immutable snapshot
   */
  public static RuleSet of(List<HotKeyRule> rules, HotKeyRule defaultRule) {
    if (defaultRule == null) {
      throw new IllegalArgumentException("a rule set needs a default rule");
    }
    List<HotKeyRule> ordered = new ArrayList<>(rules);
    ordered.sort(null);
    return new RuleSet(ordered, defaultRule);
  }

  /** @return the configured rules in precedence order, excluding the default */
  public List<HotKeyRule> rules() {
    return rules;
  }

  /** @return the rule unmatched keys resolve to */
  public HotKeyRule defaultRule() {
    return defaultRule;
  }

  /**
   * The lowest {@code hotCount} among the rules the floor can gate (the default and non-{@code
   * EXACT} rules); an effective exact rule is indexed and checked before the floor.
   *
   * @return the promotion floor
   */
  public long minHotCount() {
    return minHotCount;
  }

  /** @return the first 16 hex characters of the SHA-256 of the canonical rule text */
  public String hash() {
    return hash;
  }

  /**
   * Every rule the snapshot exposes, the default first: the rule unmatched keys resolve to, then
   * the precedence-ordered rules. The endpoint, the resolver's binding map and the fingerprint all
   * read this one view instead of each rebuilding "default plus rules".
   *
   * @return an immutable list; the default rule is element {@code 0}
   */
  public List<HotKeyRule> allRules() {
    return allRules;
  }

  /**
   * Resolves the rule that governs a key.
   *
   * <p>Linear by design: the rule count is an operator-authored constant (tens, not thousands), and
   * the resolver caches the answer per key, so the scan runs once per distinct key.
   *
   * @param key the cache key
   * @return the first matching rule, or {@link #defaultRule()} when none matches
   */
  public HotKeyRule resolve(String key) {
    for (HotKeyRule rule : rules) {
      if (rule.matches(key)) {
        return rule;
      }
    }
    return defaultRule;
  }

  /**
   * Builds the canonical text the fingerprint is taken over.
   *
   * @return a stable, order-sensitive representation of every rule's effective parameters
   */
  public String canonical() {
    return canonical(allRules);
  }

  /**
   * Computes the fingerprint of a canonical rule text.
   *
   * @param canonical the text produced by {@link #canonical()}
   * @return 16 lowercase hex characters
   */
  public static String fingerprint(String canonical) {
    try {
      byte[] digest = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(digest, 0, 8);
    } catch (NoSuchAlgorithmException e) {
      // SHA-256 is mandatory on every Java platform.
      throw new IllegalStateException("SHA-256 unavailable", e);
    }
  }

  private static long floor(List<HotKeyRule> rules, HotKeyRule defaultRule) {
    long floor = defaultRule.hotCount();
    // An exact rule is indexed by key and checked before the floor, so one special key with a low
    // threshold cannot pull every warm key into the match cache; its own threshold gates that key.
    for (HotKeyRule rule : rules) {
      if (rule.type() != MatchType.EXACT) {
        floor = Math.min(floor, rule.hotCount());
      }
    }
    return floor;
  }

  private static List<HotKeyRule> allOf(List<HotKeyRule> rules, HotKeyRule defaultRule) {
    List<HotKeyRule> all = new ArrayList<>(rules.size() + 1);
    all.add(defaultRule);
    all.addAll(rules);
    return List.copyOf(all);
  }

  private static String canonical(List<HotKeyRule> rules) {
    StringBuilder sb = new StringBuilder(256);
    for (HotKeyRule rule : rules) {
      sb.append(rule.definition()).append('\n');
    }
    return sb.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof RuleSet that)) {
      return false;
    }
    return rules.equals(that.rules) && defaultRule.equals(that.defaultRule);
  }

  @Override
  public int hashCode() {
    return 31 * rules.hashCode() + defaultRule.hashCode();
  }

  @Override
  public String toString() {
    return "RuleSet[hash=" + hash + ", rules=" + rules.size() + "]";
  }
}
