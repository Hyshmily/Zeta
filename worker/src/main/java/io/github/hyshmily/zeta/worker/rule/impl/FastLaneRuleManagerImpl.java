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
package io.github.hyshmily.zeta.worker.rule.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.worker.rule.FastLaneRuleManager;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Concurrent, cache-backed implementation of {@link FastLaneRuleManager}.
 *
 * <p>Rules are held in an immutable {@link RuleSnapshot} published through an
 * {@link AtomicReference}. Every mutation builds a fresh snapshot (new version,
 * new rule list, new match cache) and swaps it atomically; readers load the
 * snapshot reference once and evaluate against that exact triple. There is no
 * shared mutable map, no shared match cache, and no cross-epoch backfill: a
 * slow glob scan started under the old snapshot populates only the old
 * snapshot's cache, which becomes unreachable the moment the new snapshot is
 * published.
 *
 * <p>When multiple patterns match the same key, the earliest-added rule wins
 * (insertion order of the snapshot list).
 *
 * <p>Key-to-rule lookups ({@link #match}) are accelerated by the snapshot's own
 * Caffeine cache with a 30-second TTL, ensuring repeated evaluations of the
 * same key are O(1) amortised. Negative results (no match) are cached via a
 * sentinel so non-fast-lane keys also benefit from O(1) amortised lookup. An
 * empty rule set short-circuits to {@code null} without touching the cache
 * (zero allocation on the fast path).
 *
 * <p><b>Versioning (ADR-0025):</b> the rule set carries a wall-clock version
 * ({@link TimeSource#currentTimeMillis()} of the last local mutation, made
 * <em>strictly increasing</em>; {@code 0} for YAML-loaded initial rules).
 * Mutations stamp the version; {@link #replaceAll} applies a gossiped snapshot
 * only when its version is newer-or-equal, giving last-writer-wins convergence
 * across Workers.
 *
 * <p><b>Why strictly increasing, not a bare wall clock:</b> the version is the
 * only convergence key for a replicated rule set, and a bare
 * {@code currentTimeMillis()} breaks it in two ways. (1) Two mutations inside the
 * same millisecond stamp the <em>same</em> version for <em>different</em> rule
 * sets; since {@link #replaceAll} only rejects {@code version < rulesVersion},
 * each Worker keeps whichever snapshot arrived last, so peers can settle on
 * divergent rule sets. (2) A backward wall-clock step (NTP correction, container
 * clock drift) makes a fresh local edit carry a version at or below one already
 * published; every peer then rejects the broadcast as stale and the edit
 * silently never propagates (re-sending it does not help — it carries the same
 * low version). The version mint therefore derives each version from the
 * previous one as well as the clock.
 *
 * <p>Glob matching ({@code *} / {@code ?}) is used to compare cache keys
 * against rule patterns. See {@link #matchGlob} for the exact semantics.
 *
 * <p>Thread-safe. All mutations (local CRUD and gossip replace) are
 * {@code synchronized} — they are rare (operator-driven or once per gossip
 * interval) and must mint the version and publish the snapshot atomically.
 * Reads via {@link #match} stay lock-free (one volatile snapshot load plus
 * Caffeine-internal concurrency).
 */
public class FastLaneRuleManagerImpl implements FastLaneRuleManager {

  /** Sentinel for negative lookups — Caffeine does not cache null. */
  private static final FastLaneRule NO_MATCH = new FastLaneRule("", -1);

  /**
   * Immutable, atomically published rule-set snapshot: the version, the
   * insertion-ordered rules, and the match cache that belongs to exactly this
   * epoch. Readers pin one snapshot per {@link #match} call; writers replace
   * the whole record under the intrinsic lock.
   *
   * @param version the rule-set version (ADR-0025)
   * @param rules the insertion-ordered rules (unmodifiable, never {@code null})
   * @param matchCache the per-epoch Caffeine match cache (never {@code null})
   */
  private record RuleSnapshot(long version, List<FastLaneRule> rules, Cache<String, FastLaneRule> matchCache) {}

  /** Current rule-set snapshot; swapped atomically on every mutation. */
  private final AtomicReference<RuleSnapshot> snapshot;

  /**
   * Build a fresh per-epoch match cache.
   *
   * <p>Prevents O(n) glob scanning for repeatedly-seen keys. Entries expire
   * after 30 seconds as a backstop for in-place rule-object mutation (rules
   * are records and cannot be mutated, so the TTL never fires on a legal
   * path — every legal mutation publishes a fresh cache).
   *
   * <p>The 10k {@code maximumSize} cap is deliberate: fast-lane rule sets are
   * operator-authored and typically single-digit, so a cache miss (only
   * reachable past 10k concurrently-tracked distinct keys) degrades to an
   * O(rules) glob scan — cheap at that size. The cap is what bounds the
   * cache's memory for high-cardinality key spaces; raising it trades memory
   * for hit rate with no other effect.
   *
   * @return a new empty match cache
   */
  private static Cache<String, FastLaneRule> newMatchCache() {
    return Caffeine.newBuilder().maximumSize(10_000).expireAfterWrite(30, TimeUnit.SECONDS).build();
  }

  /**
   * Mint the next rule-set version from the given current version: strictly
   * greater than every version published so far, and never below the current
   * wall clock.
   *
   * <p>Takes the clock through {@link TimeSource} rather than
   * {@code System.currentTimeMillis()} so the version also inherits TimeSource's
   * monotonic floor against a backward NTP step. The {@code +1} term is what makes
   * it <em>strictly</em> increasing: two mutations in the same millisecond — or a
   * clock that has gone backwards — still receive distinct versions, which is
   * required because {@link #replaceAll} accepts equal versions and would
   * otherwise leave peers converging on whichever snapshot arrived last.
   *
   * <p><b>Callers must hold the intrinsic lock</b> (every mutation method is
   * {@code synchronized}), so the read of the current snapshot version races
   * no other writer.
   *
   * @param current the current snapshot version
   * @return the new, strictly greater rule-set version
   */
  private static long nextRulesVersion(long current) {
    return Math.max(current + 1, TimeSource.currentTimeMillis());
  }

  /**
   * Create a manager pre-populated with the given rules.
   *
   * @param initialRules rules to load on startup; may be {@code null} or empty
   */
  public FastLaneRuleManagerImpl(List<FastLaneRule> initialRules) {
    List<FastLaneRule> rules = initialRules == null ? List.of() : List.copyOf(initialRules);
    snapshot = new AtomicReference<>(new RuleSnapshot(0L, rules, newMatchCache()));
  }

  /**
   * Add or replace a fast-lane rule.
   *
   * <p>Builds a fresh snapshot (new version, new rule list, new match cache)
   * and publishes it atomically, so subsequent {@link #match} calls reflect
   * the new rule set with no stale backfill. Stamps the gossip version
   * (ADR-0025).
   *
   * @param keyPattern the glob-style key pattern to match
   * @param threshold  the sliding-window sum threshold
   */
  @Override
  public synchronized void addRule(String keyPattern, long threshold) {
    RuleSnapshot current = snapshot.get();
    List<FastLaneRule> next = new ArrayList<>(current.rules());
    FastLaneRule rule = new FastLaneRule(keyPattern, threshold);
    boolean replaced = false;
    for (int i = 0; i < next.size(); i++) {
      if (next.get(i).keyPattern().equals(keyPattern)) {
        next.set(i, rule);
        replaced = true;
        break;
      }
    }

    if (!replaced) {
      next.add(rule);
    }
    snapshot.set(
      new RuleSnapshot(nextRulesVersion(current.version()), List.copyOf(next), newMatchCache())
    );
  }

  /**
   * Remove a fast-lane rule by its pattern.
   *
   * <p>A miss leaves the snapshot untouched (no version bump, no cache
   * turnover); a hit publishes a fresh snapshot with a fresh match cache.
   *
   * @param keyPattern the key pattern to remove
   * @return {@code true} if a rule was actually removed
   */
  @Override
  public synchronized boolean removeRule(String keyPattern) {
    RuleSnapshot current = snapshot.get();
    boolean present = false;
    for (FastLaneRule rule : current.rules()) {
      if (rule.keyPattern().equals(keyPattern)) {
        present = true;
        break;
      }
    }

    if (!present) {
      return false;
    }

    List<FastLaneRule> next = new ArrayList<>(current.rules().size());
    for (FastLaneRule rule : current.rules()) {
      if (!rule.keyPattern().equals(keyPattern)) {
        next.add(rule);
      }
    }

    snapshot.set(
      new RuleSnapshot(nextRulesVersion(current.version()), List.copyOf(next), newMatchCache())
    );
    return true;
  }

  /**
   * Atomically update the threshold of an existing rule.
   *
   * <p>A miss leaves the snapshot untouched; a hit publishes a fresh snapshot
   * with a fresh match cache.
   *
   * @param keyPattern the key pattern to update
   * @param threshold  the new threshold value
   * @return {@code true} if the rule existed and was updated
   */
  @Override
  public synchronized boolean updateRule(String keyPattern, long threshold) {
    RuleSnapshot current = snapshot.get();
    boolean present = false;
    for (FastLaneRule rule : current.rules()) {
      if (rule.keyPattern().equals(keyPattern)) {
        present = true;
        break;
      }
    }

    if (!present) {
      return false;
    }
    FastLaneRule rule = new FastLaneRule(keyPattern, threshold);
    List<FastLaneRule> next = new ArrayList<>(current.rules().size());
    for (FastLaneRule existing : current.rules()) {
      next.add(existing.keyPattern().equals(keyPattern) ? rule : existing);
    }
    snapshot.set(
      new RuleSnapshot(nextRulesVersion(current.version()), List.copyOf(next), newMatchCache())
    );
    return true;
  }

  /**
   * Current version of the rule set for gossip merge (ADR-0025).
   *
   * @return wall-clock milliseconds of the last local mutation or applied
   *         gossip snapshot; {@code 0} for never-mutated YAML-loaded rules
   */
  @Override
  public long getRulesVersion() {
    return snapshot.get().version();
  }

  /**
   * Atomically replace the entire rule set with a gossiped snapshot.
   *
   * <p>Last-writer-wins merge (ADR-0025): applied only when {@code version}
   * is newer than or equal to the current version (equal allowed for
   * idempotent re-delivery; same-version conflicts between different nodes
   * are pre-filtered by the receiver's nodeId tie-break). The incoming list
   * order is preserved so every Worker resolves overlapping patterns to the
   * same winning rule. Application publishes a fresh snapshot with a fresh
   * match cache, so no entry computed under the previous epoch can backfill.
   *
   * @param newRules the complete replacement rule set (may be empty = clear all)
   * @param version  the gossiped rule set version
   */
  @Override
  public synchronized void replaceAll(List<FastLaneRule> newRules, long version) {
    RuleSnapshot current = snapshot.get();
    if (version < current.version()) {
      return;
    }
    List<FastLaneRule> rules = newRules == null ? List.of() : List.copyOf(newRules);
    snapshot.set(new RuleSnapshot(version, rules, newMatchCache()));
  }

  /**
   * Return a snapshot of all current rules.
   *
   * @return a defensive copy of the rule collection; never {@code null}
   */
  @Override
  public List<FastLaneRule> getRules() {
    return new ArrayList<>(snapshot.get().rules());
  }

  /**
   * Find the first fast-lane rule whose pattern matches the given cache key.
   *
   * <p>Pins one snapshot per call and evaluates against that exact epoch. An
   * empty rule set returns {@code null} without touching the match cache
   * (zero allocation). Otherwise results (including negative results via
   * sentinel) are cached in the snapshot's own Caffeine, so a slow scan under
   * an old epoch can never backfill the new epoch's cache.
   *
   * @param key the cache key to test
   * @return the matching rule, or {@code null} if no rule matches
   */
  @Override
  public FastLaneRule match(String key) {
    RuleSnapshot pinned = snapshot.get();
    List<FastLaneRule> rules = pinned.rules();
    if (rules.isEmpty()) {
      return null;
    }
    FastLaneRule cached = pinned.matchCache().get(key, k -> scanSnapshot(rules, k));
    return cached == NO_MATCH ? null : cached;
  }

  /**
   * Ordered scan of one pinned snapshot; the first matching rule wins.
   *
   * @param rules the pinned snapshot rules (never {@code null})
   * @param key the cache key to test
   * @return the first matching rule, or the {@link #NO_MATCH} sentinel
   */
  private static FastLaneRule scanSnapshot(List<FastLaneRule> rules, String key) {
    for (FastLaneRule rule : rules) {
      if (matchGlob(key, rule.keyPattern())) {
        return rule;
      }
    }
    return NO_MATCH;
  }

  /**
   * Simple glob matching.
   *
   * <p>Supports two wildcards:
   * <ul>
   *   <li>{@code *} — matches any sequence of characters (including empty)</li>
   *   <li>{@code ?} — matches any single character</li>
   * </ul>
   *
   * <p>All other characters match literally. The pattern must consume the
   * entire text for a successful match (no partial matching).
   *
   * @param text    the string to test (the actual cache key)
   * @param pattern the glob pattern to match against
   * @return {@code true} if the text matches the pattern
   */
  private static boolean matchGlob(String text, String pattern) {
    int ti = 0,
      pi = 0,
      starT = -1,
      starP = -1;
    while (ti < text.length()) {
      if (pi < pattern.length() && (pattern.charAt(pi) == '?' || pattern.charAt(pi) == text.charAt(ti))) {
        ti++;
        pi++;
      } else if (pi < pattern.length() && pattern.charAt(pi) == '*') {
        starT = ti;
        starP = pi;
        pi++;
      } else if (starP >= 0) {
        ti = ++starT;
        pi = starP + 1;
      } else {
        return false;
      }
    }
    while (pi < pattern.length() && pattern.charAt(pi) == '*') pi++;
    return pi == pattern.length();
  }
}
