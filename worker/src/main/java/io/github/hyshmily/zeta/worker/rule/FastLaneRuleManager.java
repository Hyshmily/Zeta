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
package io.github.hyshmily.zeta.worker.rule;

import java.util.List;

/**
 * Manages runtime fast-lane rules that bypass Bayesian confidence gating.
 *
 * <p>Fast-lane rules allow certain key patterns (e.g. flash-sale product IDs,
 * breaking-news slugs) to be promoted to {@code CONFIRMED_HOT} immediately
 * when their sliding-window sum exceeds the rule's threshold, without waiting
 * for Bayesian confidence assessment.
 *
 * <p>Rules can be added, removed, updated, and queried at runtime — changes
 * take effect immediately via the {@link #match} method.
 *
 * <p>Thread-safe. Intended to be consumed by {@link
 * io.github.hyshmily.zeta.worker.detection.Evaluator} and exposed via
 * {@link io.github.hyshmily.zeta.worker.endpoint.FastLaneEndpoint}.
 */
public interface FastLaneRuleManager {
  /**
   * A single fast-lane rule.
   *
   * @param keyPattern glob-style key pattern (e.g. {@code "product:*"});
   *                   {@code *} matches any sequence, {@code ?} matches any char
   * @param threshold  sliding-window sum threshold; when {@code windowSum >= threshold}
   *                   the key is promoted to CONFIRMED_HOT immediately
   */
  record FastLaneRule(String keyPattern, long threshold) {}

  /**
   * Add a new fast-lane rule. If a rule with the same {@code keyPattern}
   * already exists it will be silently replaced.
   *
   * @param keyPattern the glob-style key pattern to match
   * @param threshold  the sliding-window sum threshold
   */
  void addRule(String keyPattern, long threshold);

  /**
   * Remove the fast-lane rule for the given key pattern.
   *
   * @param keyPattern the key pattern to remove
   * @return {@code true} if a rule was removed, {@code false} if no rule
   *         matched the pattern
   */
  boolean removeRule(String keyPattern);

  /**
   * Update the threshold for an existing fast-lane rule.  No-op if no rule
   * with the given pattern exists.
   *
   * @param keyPattern the key pattern to update
   * @param threshold  the new sliding-window sum threshold
   * @return {@code true} if the rule was updated, {@code false} if no rule
   *         matched the pattern
   */
  boolean updateRule(String keyPattern, long threshold);

  /**
   * Return a snapshot of all current fast-lane rules.
   *
   * @return a new list containing every registered rule; never {@code null}
   */
  List<FastLaneRule> getRules();

  /**
   * Find the first fast-lane rule whose pattern matches the given cache key.
   *
   * <p>The search order is insertion-order of the underlying map, so when
   * multiple rules could match the same key, the earliest-added rule wins.
   *
   * @param key the cache key to test against all registered patterns
   * @return the matching rule, or {@code null} if no rule matches
   */
  FastLaneRule match(String key);
}
