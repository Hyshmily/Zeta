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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.worker.metrics.WorkerDetectionMetrics;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;

/**
 * Publishes one {@link RuleSet} at a time and caches the per-key resolution.
 *
 * <p>The cache holds the resolved rule binding, default included, because the linear scan is cheap
 * but not free and the report path resolves once per key. Effective {@code EXACT} keys are served
 * from their own index and never enter this cache. It is <b>bounded</b>: a per-key cache that grows
 * with key cardinality would be a second {@code windows} table, and this design treats the
 * tracked-key cap as the survival boundary. The size follows the working set that can reach the
 * resolver at all — keys at or above {@code minHotCount} plus effective {@code EXACT} keys past
 * their own threshold — so a small multiple of the HOT capacity covers "eligible, in the band, and
 * just cooled". A miss after eviction simply re-scans.
 *
 * <p>The match counters advance once per <b>eligible report</b>: a report whose window sum clears
 * the governing rule's own {@code hotCount}. The match cache and the exact index still avoid the
 * linear scan, but neither is what decides whether a match is counted, so a prefix, regex, default
 * and exact rule all report the same quantity.
 *
 * <p>The rule set, its match cache, its exact index, its bindings and its non-{@code EXACT} scan
 * array are one {@link Snapshot} behind a single volatile reference. Its maps and array are
 * immutable. A reader therefore never sees a new rule set with an old cache (or an old exact
 * index); a resolution that raced a swap may land in the retired cache and be dropped with it, and
 * the next report recomputes.
 *
 * <p>Publication pairs each rule with a {@link RuleBinding}. A rule whose definition is unchanged
 * keeps its binding; an id that survives a rule change keeps its counters. An identical rule-set
 * fingerprint short-circuits the publication, so a configuration refresh that changes nothing keeps
 * the match cache. A removed id is retired, so a late cool-down updates counters whose meters the
 * publication already removed.
 */
public class RuleResolver {

  /** Floor for the match cache, so a tiny HOT capacity still gets a usable cache. */
  private static final int MIN_CACHE_SIZE = 1_024;

  private final WorkerDetectionMetrics metrics;
  private final long cacheSize;

  /** The current snapshot; replaced wholesale, and its maps and scan array are immutable. */
  private volatile Snapshot published;

  /**
   * Creates a resolver over an initial rule set.
   *
   * @param ruleSet          the initial snapshot
   * @param maxActiveHotKeys the HOT capacity, used to size the match cache
   * @param metrics          detection meters (a meter-less instance is a no-op)
   */
  public RuleResolver(RuleSet ruleSet, int maxActiveHotKeys, WorkerDetectionMetrics metrics) {
    this.metrics = metrics;
    this.cacheSize = Math.max((long) MIN_CACHE_SIZE, 4L * Math.max(1, maxActiveHotKeys));
    publish(ruleSet);
  }

  /**
   * Publishes a new rule set and retires the bindings of rules it removed. A key already hot under
   * a removed rule finishes its cool-down against that rule, but its late counter update lands on
   * counters whose meters the publication already removed: the endpoint reports the current rule
   * set, and the meters are bounded by it. Publication is serialized so the snapshot, the counters
   * and the meter retirement always describe the same rule set.
   *
   * <p>An unchanged fingerprint is a no-op: the published snapshot, its bindings and its match
   * cache stay in place. Otherwise a rule whose definition is unchanged keeps its binding, so a
   * threshold edit to one rule does not rebuild the rest. Definition equality is textual, so a
   * recompiled regex — the same expression in a new {@code Pattern} — also keeps its binding.
   * Meters for newly added ids are registered only after the snapshot is fully built, so a failed
   * publication leaves the previous snapshot and its meter set in place.
   *
   * @param next the new snapshot
   */
  public synchronized void publish(RuleSet next) {
    if (next == null) {
      throw new IllegalArgumentException("rule set must not be null");
    }
    Snapshot current = published;
    if (current != null && current.hash().equals(next.hash()) && current.ruleSet().equals(next)) {
      return;
    }

    Map<String, RuleBinding> previous = current == null ? Map.of() : current.bindingsById();
    List<HotKeyRule> allRules = next.allRules();
    Map<String, RuleBinding> bindingsById = new HashMap<>(allRules.size());
    List<RuleBinding> ordered = new ArrayList<>(allRules.size());

    for (HotKeyRule rule : allRules) {
      RuleBinding old = previous.get(rule.id());
      RuleBinding binding =
        old != null && old.rule().definition().equals(rule.definition())
          ? old
          : new RuleBinding(rule, old != null ? old.counters() : new RuleCounters());
      bindingsById.put(rule.id(), binding);
      ordered.add(binding);
    }

    // Build the whole snapshot before binding or retiring anything: a failure while deriving the
    // exact index or the scan array leaves the published snapshot and its meter set untouched.
    Snapshot snapshot = new Snapshot(next, ordered, bindingsById, cacheSize);

    try {
      for (RuleBinding binding : ordered) {
        // A new id is bound only now, so a snapshot that never published registers no meter.
        if (!previous.containsKey(binding.rule().id())) {
          binding.counters().bind(metrics, binding.rule().id());
        }
      }
    } catch (RuntimeException | Error e) {
      // A registration failed after the snapshot was built: remove the meters this publication
      // registered, so the previous snapshot's meter set stays intact.
      for (RuleBinding binding : ordered) {
        if (!previous.containsKey(binding.rule().id())) {
          binding.counters().retire(metrics);
        }
      }
      throw e;
    }
    for (Map.Entry<String, RuleBinding> entry : previous.entrySet()) {
      if (!bindingsById.containsKey(entry.getKey())) {
        entry.getValue().retire(metrics);
      }
    }
    published = snapshot;
  }

  /**
   * A consistent view of the current publication for the endpoint. One volatile read backs the
   * floor, the fingerprint and every rule's counters, so a concurrent {@link #publish} cannot
   * produce a mixed scrape.
   *
   * @return the snapshot
   */
  public Snapshot snapshot() {
    return published;
  }

  /**
   * Resolves the rule for one report, applying the promotion floor and the exact index against the
   * same published snapshot.
   *
   * <p>An effective exact rule is authoritative for its key, so its own threshold decides here,
   * before the match cache: a cold report for a special key must not occupy a cache slot or run the
   * scan. Every other rule is checked against its own {@code hotCount} after the cache resolves it,
   * not only against the global floor, so the caller receives a binding that is already eligible
   * and can hand it straight to {@code HotKeyRegistry.promote}.
   *
   * <p>An eligible report advances the governing rule's match counter, whichever kind of rule it
   * is. That is the single definition of {@code zeta.worker.rule.matches}: the cache and the exact
   * index decide where the rule comes from, never whether the hit is counted.
   *
   * @param key       the cache key
   * @param windowSum the reported window sum
   * @return the governing binding, or {@code null} when the report is below the promotion
   *         threshold of the rule that governs it
   */
  @Nullable
  public RuleBinding resolveIfEligible(String key, long windowSum) {
    return published.resolveIfEligible(key, windowSum);
  }

  /**
   * The current binding for one rule id, for tests and diagnostics. The endpoint reads the ordered
   * snapshot instead, because a per-id lookup in a loop can mix publications.
   *
   * @param ruleId the rule id
   * @return its binding, or {@code null} when the id is no longer published
   */
  @Nullable
  public RuleBinding binding(String ruleId) {
    return published.bindingsById().get(ruleId);
  }

  /**
   * One publication unit: the rule set, its match cache, its indexes and the bindings, behind a
   * single volatile reference. The maps and the scan array are immutable; the cache is
   * thread-safe. It is also the match cache's loader, so a resolution allocates no per-call
   * function.
   */
  public static final class Snapshot implements Function<String, RuleBinding> {

    private final Map<String, RuleBinding> bindingsById;
    private final Cache<String, RuleBinding> matchCache;
    private final Map<String, RuleBinding> exactRules;
    private final List<RuleBinding> bindings;
    private final RuleBinding[] scanBindings;
    private final RuleBinding defaultBinding;
    private final RuleSet ruleSet;

    private Snapshot(
      RuleSet ruleSet,
      List<RuleBinding> ordered,
      Map<String, RuleBinding> bindingsById,
      long cacheSize
    ) {
      this.ruleSet = ruleSet;
      this.bindings = List.copyOf(ordered);
      this.bindingsById = Map.copyOf(bindingsById);
      this.defaultBinding = this.bindings.get(0);
      this.scanBindings = buildScanBindings(this.bindings);
      this.exactRules = buildExactRules(ruleSet, this.bindingsById);
      this.matchCache = Caffeine.newBuilder().maximumSize(cacheSize).build();
    }

    /**
     * The flat scan array for a match-cache miss. The default binding is element {@code 0} of the
     * ordered view and is returned directly when nothing matches, so the scan starts at 1. An
     * {@code EXACT} rule is skipped: an effective one was already consulted through the exact index
     * before the cache, and a shadowed one can never be the first match for any key because the rule
     * that shadows it also matches its pattern.
     */
    private static RuleBinding[] buildScanBindings(List<RuleBinding> ordered) {
      return ordered
        .stream()
        .skip(1)
        .filter(binding -> binding.rule().type() != MatchType.EXACT)
        .toArray(RuleBinding[]::new);
    }

    /**
     * Indexes the {@code EXACT} rules that actually govern their key. A rule is effective exactly
     * when resolving its own literal pattern returns that same rule; an earlier rule that matches
     * the pattern would shadow it. The scan runs once per publication over operator-authored rules,
     * against the newly published rule instances: a binding retained across a publication still
     * holds the previous {@link HotKeyRule} object, so an identity check against the bindings would
     * drop an unchanged exact rule from the index after any other rule changed.
     */
    private static Map<String, RuleBinding> buildExactRules(RuleSet ruleSet, Map<String, RuleBinding> bindingsById) {
      Map<String, RuleBinding> exact = new HashMap<>();
      for (HotKeyRule rule : ruleSet.rules()) {
        if (rule.type() == MatchType.EXACT && ruleSet.resolve(rule.pattern()) == rule) {
          exact.put(rule.pattern(), bindingsById.get(rule.id()));
        }
      }
      return Map.copyOf(exact);
    }

    /**
     * Resolves the rule for one report, applying the promotion floor and the exact index against
     * this snapshot.
     *
     * <p>An effective exact rule is authoritative for its key, so its own threshold decides here,
     * before the match cache: a cold report for a special key must not occupy a cache slot or run the
     * scan. Every other rule is checked against its own {@code hotCount} after the cache resolves it,
     * not only against the global floor, so the caller receives a binding that is already eligible
     * and can hand it straight to {@code HotKeyRegistry.promote}.
     *
     * <p>An eligible report advances the governing rule's match counter, whichever kind of rule it
     * is. That is the single definition of {@code zeta.worker.rule.matches}: the cache and the exact
     * index decide where the rule comes from, never whether the hit is counted.
     *
     * @param key       the cache key
     * @param windowSum the reported window sum
     * @return the governing binding, or {@code null} when the report is below the promotion
     *         threshold of the rule that governs it
     */
    @Nullable
    private RuleBinding resolveIfEligible(String key, long windowSum) {
      RuleBinding exact = exactRules.get(key);
      if (exact != null) {
        return eligible(exact, windowSum);
      }

      if (windowSum < ruleSet.minHotCount()) {
        return null;
      }
      return eligible(matchCache.get(key, this), windowSum);
    }

    @Nullable
    private static RuleBinding eligible(RuleBinding binding, long windowSum) {
      if (!binding.rule().eligible(windowSum)) {
        return null;
      }

      binding.recordMatch();
      return binding;
    }

    /** @return the promotion floor */
    public long minHotCount() {
      return ruleSet.minHotCount();
    }

    /**
     * The published rule set. The fingerprint is only a cheap first filter: callers that need
     * equality compare this snapshot's set, so a 64-bit collision cannot suppress a real change.
     *
     * @return the published rule set
     */
    RuleSet ruleSet() {
      return ruleSet;
    }

    /** @return the rule-set fingerprint */
    public String hash() {
      return ruleSet.hash();
    }

    /** @return the default binding first, then every rule in precedence order */
    public List<RuleBinding> bindings() {
      return bindings;
    }

    /** @return every published rule id to its binding, the default included */
    Map<String, RuleBinding> bindingsById() {
      return bindingsById;
    }

    /**
     * The rule that governs one key, resolved exactly as the report path resolves it: the exact
     * index first, then the precedence scan with the default as the fallback. Eligibility and the
     * match counter are report-path concerns and stay out of this read-only view.
     *
     * @param key the cache key
     * @return the governing rule; never {@code null}
     */
    public HotKeyRule resolveRule(String key) {
      RuleBinding exact = exactRules.get(key);
      return (exact != null ? exact : apply(key)).rule();
    }

    /**
     * The match-cache loader: scans the non-{@code EXACT} rules in precedence order and falls back
     * to the default binding. Invoked only on a cache miss, once per distinct key per publication.
     *
     * @param key the cache key
     * @return the governing binding; never {@code null}
     */
    @Override
    public RuleBinding apply(String key) {
      for (RuleBinding binding : scanBindings) {
        if (binding.rule().matches(key)) {
          return binding;
        }
      }
      return defaultBinding;
    }
  }
}
