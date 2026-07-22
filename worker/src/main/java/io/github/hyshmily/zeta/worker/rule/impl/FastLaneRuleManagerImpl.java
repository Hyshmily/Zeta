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
import io.github.hyshmily.zeta.worker.rule.FastLaneRuleManager;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Concurrent, cache-backed implementation of {@link FastLaneRuleManager}.
 *
 * <p>Rules are stored in a {@link ConcurrentHashMap} keyed by pattern string.
 * Key-to-rule lookups ({@link #match}) are accelerated by a Caffeine cache
 * with a 30-second TTL, ensuring repeated evaluations of the same key are
 * O(1) amortised. The cache is invalidated on every write operation
 * ({@link #addRule}, {@link #removeRule}, {@link #updateRule}) so changes
 * are visible immediately.
 *
 * <p>Glob matching ({@code *} / {@code ?}) is used to compare cache keys
 * against rule patterns. See {@link #matchGlob} for the exact semantics.
 *
 * <p>Thread-safe. All mutations are atomic; reads from the cache are
 * Caffeine-internal and lock-free.
 */
public class FastLaneRuleManagerImpl implements FastLaneRuleManager {

  /** Registered rules, keyed by {@link FastLaneRule#keyPattern()}. */
  private final ConcurrentHashMap<String, FastLaneRule> rules = new ConcurrentHashMap<>();

  /**
   * Caffeine cache from evaluated cache key to the matched rule.
   *
   * <p>Prevents O(n) glob scanning for repeatedly-seen keys. Entries expire
   * after 30 seconds to pick up rule changes that bypass {@link #invalidateAll}
   * (e.g. direct mutation of a shared map).
   */
  private final Cache<String, FastLaneRule> matchCache = Caffeine.newBuilder()
    .maximumSize(10_000)
    .expireAfterWrite(30, TimeUnit.SECONDS)
    .build();

  /**
   * Create a manager pre-populated with the given rules.
   *
   * @param initialRules rules to load on startup; may be {@code null} or empty
   */
  public FastLaneRuleManagerImpl(List<FastLaneRule> initialRules) {
    if (initialRules != null) {
      for (FastLaneRule rule : initialRules) {
        rules.put(rule.keyPattern(), rule);
      }
    }
  }

  /**
   * Add or replace a fast-lane rule.
   *
   * <p>The match cache is invalidated so that subsequent {@link #match} calls
   * reflect the new rule set.
   *
   * @param keyPattern the glob-style key pattern to match
   * @param threshold  the sliding-window sum threshold
   */
  @Override
  public void addRule(String keyPattern, long threshold) {
    rules.put(keyPattern, new FastLaneRule(keyPattern, threshold));
    matchCache.invalidateAll();
  }

  /**
   * Remove a fast-lane rule by its pattern.
   *
   * @param keyPattern the key pattern to remove
   * @return {@code true} if a rule was actually removed
   */
  @Override
  public boolean removeRule(String keyPattern) {
    boolean removed = rules.remove(keyPattern) != null;
    if (removed) matchCache.invalidateAll();
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
  public boolean updateRule(String keyPattern, long threshold) {
    boolean updated = rules.computeIfPresent(keyPattern, (k, v) -> new FastLaneRule(k, threshold)) != null;
    if (updated) matchCache.invalidateAll();
    return updated;
  }

  /**
   * Return a snapshot of all current rules.
   *
   * @return a defensive copy of the rule collection; never {@code null}
   */
  @Override
  public List<FastLaneRule> getRules() {
    return new ArrayList<>(rules.values());
  }

  /**
   * Find the first fast-lane rule whose pattern matches the given cache key.
   *
   * <p>Results are cached in Caffeine for 30 seconds so repeated lookups
   * for the same key avoid O(n) glob scanning.
   *
   * @param key the cache key to test
   * @return the matching rule, or {@code null} if no rule matches
   */
  @Override
  public FastLaneRule match(String key) {
    return matchCache.get(key, k -> {
      for (FastLaneRule rule : rules.values()) {
        if (matchGlob(k, rule.keyPattern())) return rule;
      }
      return null;
    });
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
