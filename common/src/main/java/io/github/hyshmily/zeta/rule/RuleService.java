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
package io.github.hyshmily.zeta.rule;

import static io.github.hyshmily.zeta.cache.cachesupport.CacheKeysPolicy.invalidCacheKey;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.cache.cachesupport.CacheCoreSettings;
import io.github.hyshmily.zeta.cache.cachesupport.CacheKeysPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.TransactionSupport;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Rule administration service — the CRUD and query surface over the
 * {@link RuleMatcher}: blacklist/whitelist mutation, rule evaluation, and
 * rule-set snapshot/broadcast.
 *
 * <p>Extracted from the former {@code HotKeyCache} rule methods so the cache
 * orchestrator only <i>reads</i> rules on its guard path
 * ({@code preGuard}/{@code afterGuard}) while rule <i>management</i> lives
 * behind its own seam. Mutation runs inside a transaction boundary
 * ({@link TransactionSupport#runNowOrAfterCommit}) and every incoming key is
 * normalized (query-param stripping per {@link CacheCoreSettings#isStripQuery()})
 * before matching.
 *
 * <p>
 * <b>Invalid keys are skipped silently</b>, never rejected: this is the
 * engine-plane half of the rule contract. Blank-key rejection is a facade-plane
 * concern and lives in {@code DefaultZeta}'s validating
 * {@link Zeta.RuleAdmin} adapter, so callers holding a {@code RuleService}
 * directly keep the lenient engine behaviour.
 */
@RequiredArgsConstructor
@Slf4j
@Internal
public class RuleService implements Zeta.RuleAdmin {

  /** Matches cache keys against blacklist/whitelist rules. */
  private final RuleMatcher ruleMatcher;

  /** Cache-core configuration view (query-param stripping). */
  private final CacheCoreSettings settings;

  /**
   * Normalize a cache key if query-param stripping is enabled.
   * Delegates to {@link CacheKeysPolicy#normalizeKey} only when the
   * feature is configured; otherwise returns the key unchanged (zero overhead).
   */
  private String normalize(String cacheKey) {
    if (cacheKey != null && settings.isStripQuery()) {
      return CacheKeysPolicy.normalizeKey(cacheKey);
    }
    return cacheKey;
  }

  /**
   * Add or remove a rule atomically within a transaction boundary.
   *
   * @param add {@code true} to add the rule, {@code false} to remove it
   */
  private void modifyRule(String cacheKey, Rule.RuleAction action, boolean add) {
    String nk = normalize(cacheKey);
    if (invalidCacheKey(nk)) {
      if (log.isDebugEnabled()) {
        log.debug("modifyRule: invalid cacheKey '{}'", nk);
      }
      return;
    }
    TransactionSupport.runNowOrAfterCommit(() -> {
      if (add) ruleMatcher.addRule(RuleMatcher.of(nk, action));
      else ruleMatcher.removeRule(nk, action);
      log.info("{} {} rule for key={}", add ? "Added" : "Removed", action, nk);
    });
  }

  /** Add a key pattern to the blacklist (blocked keys reject cache operations). */
  @Override
  public void addBlacklist(String cacheKey) {
    modifyRule(cacheKey, Rule.RuleAction.BLOCK, true);
  }

  /** Add a key pattern to the whitelist (matched keys skip Worker reporting). */
  @Override
  public void addWhitelist(String cacheKey) {
    modifyRule(cacheKey, Rule.RuleAction.ALLOW_NO_REPORT, true);
  }

  /** Remove a key pattern from the blacklist. */
  @Override
  public void removeBlacklist(String cacheKey) {
    modifyRule(cacheKey, Rule.RuleAction.BLOCK, false);
  }

  /** Remove a key pattern from the whitelist. */
  @Override
  public void removeWhitelist(String cacheKey) {
    modifyRule(cacheKey, Rule.RuleAction.ALLOW_NO_REPORT, false);
  }

  /**
   * Evaluate all rules against the given key and return the first matching
   * action.
   *
   * @param cacheKey the key to evaluate
   * @return the matching {@link Rule.RuleAction}, or {@code ALLOW} if no rule matches
   */
  @Override
  public Rule.RuleAction evaluateRule(String cacheKey) {
    return ruleMatcher.evaluateRule(normalize(cacheKey));
  }

  /**
   * Check whether the given key is blacklisted.
   *
   * @param cacheKey the key to check
   * @return {@code true} if a blacklist rule matches the key
   */
  @Override
  public boolean isBlacklisted(String cacheKey) {
    return ruleMatcher.evaluateRule(normalize(cacheKey)) == Rule.RuleAction.BLOCK;
  }

  /**
   * Check whether the given key is whitelisted (skips Worker reporting).
   *
   * @param cacheKey the key to check
   * @return {@code true} if a whitelist rule matches the key
   */
  @Override
  public boolean isWhitelisted(String cacheKey) {
    return ruleMatcher.evaluateRule(normalize(cacheKey)) == Rule.RuleAction.ALLOW_NO_REPORT;
  }

  /**
   * Return a snapshot of all current rules in evaluation order.
   *
   * @return list of rules (immutable snapshot)
   */
  @Override
  public List<Rule> getAllRules() {
    return ruleMatcher.getAllRules();
  }

  /** Remove all blacklist and whitelist rules. */
  @Override
  public void clearAllRules() {
    TransactionSupport.runNowOrAfterCommit(ruleMatcher::clearRules);
  }

  /**
   * Broadcast all local rules to peer instances via the sync exchange.
   * Useful for initial synchronization when a new instance joins the cluster.
   */
  @Override
  public void broadcastAllLocalRulesManually() {
    ruleMatcher.broadcastAllLocalRulesManually();
  }
}
