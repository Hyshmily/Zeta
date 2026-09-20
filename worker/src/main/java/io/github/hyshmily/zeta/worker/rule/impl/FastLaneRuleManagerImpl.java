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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Concurrent, cache-backed implementation of {@link FastLaneRuleManager}.
 *
 * <p>Rules are stored in a {@link ConcurrentHashMap} keyed by pattern string
 * and mirrored in a {@link java.util.concurrent.CopyOnWriteArrayList} for
 * insertion-order iteration. When multiple patterns match the same key, the
 * earliest-added rule wins.
 *
 * <p>Key-to-rule lookups ({@link #match}) are accelerated by a Caffeine cache
 * with a 30-second TTL, ensuring repeated evaluations of the same key are
 * O(1) amortised. Negative results (no match) are cached via a sentinel so
 * non-fast-lane keys also benefit from O(1) amortised lookup. The cache is
 * invalidated on every write operation ({@link #addRule}, {@link #removeRule},
 * {@link #updateRule}, {@link #replaceAll}) so changes are visible immediately.
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
 * low version). {@link #nextRulesVersion()} therefore derives each version from
 * the previous one as well as the clock.
 *
 * <p>Glob matching ({@code *} / {@code ?}) is used to compare cache keys
 * against rule patterns. See {@link #matchGlob} for the exact semantics.
 *
 * <p>Thread-safe. All mutations (local CRUD and gossip replace) are
 * {@code synchronized} — they are rare (operator-driven or once per gossip
 * interval) and must update the map, the ordered list, the version, and the
 * match cache atomically. Reads via {@link #match} stay Caffeine-internal
 * and lock-free.
 */
public class FastLaneRuleManagerImpl implements FastLaneRuleManager {

  /** Sentinel for negative lookups — Caffeine does not cache null. */
  private static final FastLaneRule NO_MATCH = new FastLaneRule("", -1);

  /** Registered rules, keyed by {@link FastLaneRule#keyPattern()}. */
  private final ConcurrentHashMap<String, FastLaneRule> rules = new ConcurrentHashMap<>();

  /** Insertion-ordered list for ordered iteration in {@link #match}. */
  private final CopyOnWriteArrayList<FastLaneRule> orderedRules = new CopyOnWriteArrayList<>();

  /**
   * Wall-clock version of the current rule set (ms since epoch). {@code 0}
   * for YAML-loaded initial rules. Stamped on every local mutation and on
   * every applied gossip snapshot. Written under the intrinsic lock,
   * read via {@link #getRulesVersion()} (volatile).
   */
  private volatile long rulesVersion = 0L;

  /**
   * Mint the next rule-set version: strictly greater than every version this
   * instance has published so far, and never below the current wall clock.
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
   * {@code synchronized}), so the read-modify-write on the volatile field is not
   * racing another writer.
   *
   * @return the new, strictly greater rule-set version
   */
  private long nextRulesVersion() {
    long next = Math.max(rulesVersion + 1, TimeSource.currentTimeMillis());
    rulesVersion = next;
    return next;
  }

  /**
   * Caffeine cache from evaluated cache key to the matched rule.
   *
   * <p>Prevents O(n) glob scanning for repeatedly-seen keys. Entries expire
   * after 30 seconds to pick up rule changes that bypass {@link #invalidateAll}
   * (e.g. direct mutation of a shared map).
   *
   * <p>The 10k {@code maximumSize} cap is deliberate: fast-lane rule sets are
   * operator-authored and typically single-digit, so a cache miss (only
   * reachable past 10k concurrently-tracked distinct keys) degrades to an
   * O(rules) glob scan — cheap at that size. The cap is what bounds the
   * cache's memory for high-cardinality key spaces; raising it trades memory
   * for hit rate with no other effect.
   */
  private final Cache<String, FastLaneRule> matchCache = Caffeine.newBuilder()
    .maximumSize(10_000)
    .expireAfterWrite(30, TimeUnit.SECONDS)
    .build();

  /**
   * Reusable loader for {@link #matchCache}. Hoisted to a field so repeated {@link #match}
   * calls share one lambda instance instead of allocating a capturing lambda per call —
   * the cache is consulted once per reported key per evaluation batch.
   */
  private final Function<String, FastLaneRule> matchLoader = k -> {
    for (FastLaneRule rule : orderedRules) {
      if (matchGlob(k, rule.keyPattern())) return rule;
    }
    return NO_MATCH;
  };

  /**
   * Create a manager pre-populated with the given rules.
   *
   * @param initialRules rules to load on startup; may be {@code null} or empty
   */
  public FastLaneRuleManagerImpl(List<FastLaneRule> initialRules) {
    if (initialRules != null) {
      for (FastLaneRule rule : initialRules) {
        rules.put(rule.keyPattern(), rule);
        orderedRules.add(rule);
      }
    }
  }

  /**
   * Add or replace a fast-lane rule.
   *
   * <p>The match cache is invalidated so that subsequent {@link #match} calls
   * reflect the new rule set. Stamps the gossip version (ADR-0025).
   *
   * @param keyPattern the glob-style key pattern to match
   * @param threshold  the sliding-window sum threshold
   */
  @Override
  public synchronized void addRule(String keyPattern, long threshold) {
    FastLaneRule rule = new FastLaneRule(keyPattern, threshold);
    FastLaneRule old = rules.put(keyPattern, rule);
    if (old == null) {
      orderedRules.add(rule);
    } else {
      orderedRules.replaceAll(r -> r.keyPattern().equals(keyPattern) ? rule : r);
    }
    nextRulesVersion();
    matchCache.invalidateAll();
  }

  /**
   * Remove a fast-lane rule by its pattern.
   *
   * @param keyPattern the key pattern to remove
   * @return {@code true} if a rule was actually removed
   */
  @Override
  public synchronized boolean removeRule(String keyPattern) {
    boolean removed = rules.remove(keyPattern) != null;
    if (removed) {
      orderedRules.removeIf(r -> r.keyPattern().equals(keyPattern));
      nextRulesVersion();
      matchCache.invalidateAll();
    }
    return removed;
  }

  /**
   * Atomically update the threshold of an existing rule.
   *
   * <p>Uses {@link ConcurrentHashMap#computeIfPresent} to ensure the
   * check-and-update is atomic.
   *
   * @param keyPattern the key pattern to update
   * @param threshold  the new threshold value
   * @return {@code true} if the rule existed and was updated
   */
  @Override
  public synchronized boolean updateRule(String keyPattern, long threshold) {
    boolean updated = rules.computeIfPresent(keyPattern, (k, v) -> new FastLaneRule(k, threshold)) != null;
    if (updated) {
      FastLaneRule rule = new FastLaneRule(keyPattern, threshold);
      orderedRules.replaceAll(r -> r.keyPattern().equals(keyPattern) ? rule : r);
      nextRulesVersion();
      matchCache.invalidateAll();
    }
    return updated;
  }

  /**
   * Current version of the rule set for gossip merge (ADR-0025).
   *
   * @return wall-clock milliseconds of the last local mutation or applied
   *         gossip snapshot; {@code 0} for never-mutated YAML-loaded rules
   */
  @Override
  public long getRulesVersion() {
    return rulesVersion;
  }

  /**
   * Atomically replace the entire rule set with a gossiped snapshot.
   *
   * <p>Last-writer-wins merge (ADR-0025): applied only when {@code version}
   * is newer than or equal to the current version (equal allowed for
   * idempotent re-delivery; same-version conflicts between different nodes
   * are pre-filtered by the receiver's nodeId tie-break). The incoming list
   * order is preserved so every Worker resolves overlapping patterns to the
   * same winning rule. The match cache is invalidated on application.
   *
   * @param newRules the complete replacement rule set (may be empty = clear all)
   * @param version  the gossiped rule set version
   */
  @Override
  public synchronized void replaceAll(List<FastLaneRule> newRules, long version) {
    if (version < rulesVersion) {
      return;
    }
    rules.clear();
    orderedRules.clear();
    if (newRules != null) {
      for (FastLaneRule rule : newRules) {
        rules.put(rule.keyPattern(), rule);
        orderedRules.add(rule);
      }
    }
    rulesVersion = version;
    matchCache.invalidateAll();
  }

  /**
   * Return a snapshot of all current rules.
   *
   * @return a defensive copy of the rule collection; never {@code null}
   */
  @Override
  public List<FastLaneRule> getRules() {
    return new ArrayList<>(orderedRules);
  }

  /**
   * Find the first fast-lane rule whose pattern matches the given cache key.
   *
   * <p>Results (including negative results via sentinel) are cached in
   * Caffeine for 30 seconds so repeated lookups for the same key avoid
   * O(n) glob scanning.
   *
   * @param key the cache key to test
   * @return the matching rule, or {@code null} if no rule matches
   */
  @Override
  public FastLaneRule match(String key) {
    FastLaneRule r = matchCache.get(key, matchLoader);
    return r == NO_MATCH ? null : r;
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
