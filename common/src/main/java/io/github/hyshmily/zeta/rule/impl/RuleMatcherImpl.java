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
package io.github.hyshmily.zeta.rule.impl;

import static io.github.hyshmily.zeta.constants.ZetaConstants.Redis.KEY_RULES;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.rule.Rule;
import io.github.hyshmily.zeta.rule.Rule.RuleAction;
import io.github.hyshmily.zeta.rule.RuleMatcher;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.util.LogThrottle;
import jakarta.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.regex.PatternSyntaxException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.util.Assert;

/**
 * Central component for evaluating cache-key rules that govern blocking,
 * reporting suppression, and other access-control decisions.
 *
 * <p><b>Rule lifecycle:</b> Rules are held in a volatile immutable
 * {@code Rule[]} snapshot; every mutation rebuilds the array under the
 * instance monitor, and rules are evaluated in insertion order — the first
 * matching rule determines the action. Because the array is never mutated
 * in place, evaluation results are memoized per key and stamped with the
 * exact snapshot they were computed from (ADR-0065): the hot path is one
 * lock-free map probe, a mutation's fresh array invalidates every previous
 * entry on its next read, and an empty rule set short-circuits without
 * touching the memo.
 *
 * <p><b>Persistence and synchronization:</b> Persistence is optional. If a
 * {@link StringRedisTemplate} is available, the current rule set is written
 * to Redis on every local change (via a Lua compare-and-set script that
 * guards against stale-write races) and reloaded at startup — the persisted
 * {@code rulesVersion} is restored into the local counter so that post-restart
 * changes are neither rejected by the CAS gate nor skipped by peers. If Redis
 * is unavailable, rules remain purely in-memory — they survive as long as at
 * least one application node holds them and can be re-send to siblings
 * through the {@link CacheSyncPublisher}.
 *
 * <p><b>Cross-instance sync:</b> When a rule is added, removed, or cleared,
 * the change is both persisted to Redis <em>and</em> send via AMQP
 * (both/and, not XOR). Per ADR-0062, an incoming broadcast carrying a
 * strictly fresher version is authoritative and <b>replaces</b> the local
 * list wholesale (so deletions and clears converge); the pre-versioning
 * union merge survives only for legacy unversioned broadcasts
 * ({@code incomingVersion == 0}, rolling upgrades). The receiver adopts the
 * incoming version exactly, so the in-memory counter and the Redis wrapper
 * never diverge and a receiver's re-persist cannot poison the CAS gate.
 *
 * <p>Thread-safe. Mutations hold the instance monitor only for the snapshot
 * swap and the in-memory staging; the Redis/AMQP I/O runs after the monitor
 * is released — one slow Redis round trip must not serialize every rule
 * mutation or pin the sync dispatcher thread. A staged persist that lands
 * after a newer mutation is rejected by the versioned CAS / strictly-fresher
 * adoption (ADR-0062), so the relaxed ordering changes no convergence
 * semantics. The snapshot reference is volatile and readers stay lock-free.
 * The version counter is an {@link java.util.concurrent.atomic.AtomicLong}.
 *
 * @see io.github.hyshmily.zeta.rule.RuleMatcher
 * @see <a href="https://github.com/Hyshmily/Zeta/blob/master/docs/adr/0062-rule-sync-version-authoritative-replace.md">ADR-0062</a>
 */
@Internal
@RequiredArgsConstructor
@Slf4j
public class RuleMatcherImpl implements RuleMatcher {

  /** Shared Jackson mapper; ignores unknown properties for forward compatibility. */
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().configure(
    DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
    false
  );

  /** Shared empty snapshot; all mutations replace the reference, never mutate in place. */
  private static final Rule[] EMPTY_RULES = new Rule[0];

  /** Lua compare-and-set script holder: lazily loaded to avoid {@code NoClassDefFoundError} when Redis is absent. */
  private static class CasScriptHolder {

    static final DefaultRedisScript<Long> SCRIPT = create();

    private static DefaultRedisScript<Long> create() {
      DefaultRedisScript<Long> s = new DefaultRedisScript<>();
      s.setScriptText(
        """
        local current = redis.call('GET', KEYS[1])
        if current == false then
          redis.call('SET', KEYS[1], ARGV[1])
          return 1
        end
        local ok, decoded = pcall(cjson.decode, current)
        local curVer = 0
        if ok and type(decoded) == 'table' and decoded['rulesVersion'] then
          curVer = decoded['rulesVersion']
        end
        if tonumber(ARGV[2]) > curVer then
          redis.call('SET', KEYS[1], ARGV[1])
          return 1
        end
        return 0"""
      );
      s.setResultType(Long.class);
      return s;
    }
  }

  /** Optional Redis template for rule persistence across restarts. */
  @SuppressWarnings("all")
  private final Optional<StringRedisTemplate> redisTemplate;

  /** Optional publisher for broadcasting rule changes to sibling instances. */
  @SuppressWarnings("all")
  private final Optional<CacheSyncPublisher> cacheSyncPublisher;

  /** Immutable snapshot of the active rules, evaluated in order; swapped atomically on mutation. */
  private volatile Rule[] rulesSnapshot = EMPTY_RULES;

  /**
   * Every mutation of the rule set ({@link #addRule}, {@link #removeRule},
   * {@link #removeRulesByAction}, {@link #clearRules}, {@link #syncRules},
   * {@link #replaceRules}) holds the instance monitor ({@code synchronized}
   * block) for its snapshot swap. The snapshot reference is
   * volatile and readers ({@link #evaluateRule}) stay lock-free, but
   * mutations must not interleave: a local add/remove mutating the current
   * snapshot concurrently with a sync swap would silently lose the local
   * change, and two racing syncs could overwrite a newer rule set with an
   * older one.
   *
   * <p>The counter is restored from Redis on load (max of local and persisted)
   * and adopted exactly from a fresher broadcast, so it stays comparable with
   * the Redis wrapper's {@code rulesVersion} across restarts — without this,
   * post-restart local changes would be silently dropped by the CAS gate.
   */
  private final AtomicLong rulesVersion = new AtomicLong(0L);

  /** Rules whose evaluation threw; identity-keyed so a re-added fixed rule logs at ERROR again. */
  private final Set<Rule> brokenRules = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));

  /** Strict one-per-window admission for the CAS-rejection WARN (ADR-0037 convention). {@link LogThrottle} claims the window with a compare-and-set, so exactly one caller per window logs. */
  private final LogThrottle casRejectLogThrottle = LogThrottle.perDefaultWindow();

  /**
   * Memoized evaluation result: the exact snapshot the action was computed from, plus the
   * action. Holding the snapshot reference — never mutated in place, every mutation builds
   * a fresh array — makes the stamp self-invalidating: a mutation swaps in a new array and
   * every entry of the previous epoch fails the reference check on its next read. The one
   * shared reference, {@link #EMPTY_RULES}, can only ever be stamped with {@code ALLOW}
   * (zero rules always allow), so its reuse by a later empty set cannot resurrect a wrong
   * action.
   */
  private record MemoEntry(Rule[] snapshot, RuleAction action) {}

  /**
   * Per-key memo of the last evaluated action (ADR-0065). Turns the ordered per-get scan
   * into one lock-free map probe; a miss re-scans the current snapshot and re-stamps.
   * Bounded at 10k keys like the Worker fast-lane match cache; the 30s TTL is the backstop
   * for changes that swap no array at all — in-place {@link Rule#setPattern}/{@link Rule#setType}
   * mutation, which is discouraged (mutate through {@link RuleMatcher} instead) but possible.
   */
  private final Cache<String, MemoEntry> decisionCache = Caffeine.newBuilder()
    .maximumSize(10_000)
    .expireAfterWrite(30, TimeUnit.SECONDS)
    .build();

  /**
   * Load persisted rules from Redis (if available) at bean initialization time.
   *
   * <p>Called automatically by the Spring container after dependency injection
   * ({@link PostConstruct}). If Redis is not configured or is unreachable,
   * the rule list starts empty and the application operates normally with
   * out-of-memory-only rules. Any errors during loading are logged but do
   * not prevent bean initialization.
   *
   * @see #loadRulesFromRedis
   */
  @PostConstruct
  void initRules() {
    loadRulesFromRedis();
  }

  /**
   * Append a rule to the end of the rule list.
   *
   * <p>The change is immediately visible in the local rule list (snapshot
   * swap semantics), persisted to Redis via Lua compare-and-set (if
   * available), and send to sibling application instances via AMQP. The
   * local version counter is incremented atomically.
   *
   * @param rule the rule to append; must not be {@code null}. The rule is
   *             {@link Rule#prepare() prepared} before insertion to ensure
   *             its internal pattern is compiled
   */
  public void addRule(Rule rule) {
    Objects.requireNonNull(rule, "rule must not be null");
    Assert.hasText(rule.getPattern(), "pattern must not be null or empty");
    Assert.notNull(rule.getAction(), "action must not be null");

    StagedRules staged;
    synchronized (this) {
      rule.prepare();
      rulesSnapshot = append(rulesSnapshot, rule);
      rulesVersion.incrementAndGet();
      staged = stagePersist();

      log.info(
        "Rule added: pattern='{}', type={}, action={} (total: {}, version: {})",
        rule.getPattern(),
        rule.getType(),
        rule.getAction(),
        rulesSnapshot.length,
        rulesVersion.get()
      );
    }
    persistAndBroadcast(staged);
  }

  /**
   * Remove all rules whose pattern and action both match the given values.
   *
   * <p>If at least one rule is removed, the change is persisted to Redis
   * and send to sibling instances. The version counter is incremented.
   *
   * <p>Matching is based on the rule's stored pattern and its action; rules
   * with the same pattern but a different action are <em>not</em> removed.
   *
   * <p>The supplied pattern is first canonicalized through
   * {@link RuleMatcher#of(String, RuleAction)} — the same transform the add
   * path applies — so a rule can be removed with the exact string that added
   * it. This is required because that transform is lossy: it strips a trailing
   * {@code *} for PREFIX rules and the {@code regex:} prefix for REGEX rules.
   * Without it, {@code addBlacklist("user:*")} stores pattern {@code "user:"}
   * and {@code removeBlacklist("user:*")} would never find it. The raw string
   * is still accepted as a fallback, so callers that already pass the
   * canonical form keep working unchanged.
   *
   * @param pattern the pattern string to match ({@link Rule#pattern}), in the
   *                same form used when the rule was added
   * @param action  the action to match ({@link Rule#action})
   * @return {@code true} if at least one rule was removed; {@code false}
   *         if no matching rule was found
   */
  public boolean removeRule(String pattern, RuleAction action) {
    StagedRules staged;
    boolean removed;
    String canonical = canonicalPattern(pattern, action);
    synchronized (this) {
      removed = removeIfMatching(
        rule ->
          rule.getAction() == action &&
          (Objects.equals(rule.getPattern(), canonical) || Objects.equals(rule.getPattern(), pattern))
      );
      if (removed) {
        rulesVersion.incrementAndGet();
        staged = stagePersist();
        log.info(
          "Rule removed: pattern='{}', action={} (total: {}, version: {})",
          pattern,
          action,
          rulesSnapshot.length,
          rulesVersion.get()
        );
      } else {
        staged = null;
        log.warn("Rule not found for removal: pattern='{}', action={}", pattern, action);
      }
    }
    persistAndBroadcast(staged);
    return removed;
  }

  /**
   * Canonicalize a caller-supplied pattern the way the add path does, so the
   * two paths agree on the form that identifies a rule.
   *
   * <p>{@link RuleMatcher#of(String, RuleAction)} is lossy — it strips a
   * trailing {@code *} for PREFIX rules and the {@code regex:} prefix for
   * REGEX rules — so the stored pattern is not the string the caller passed to
   * the add method. Returning the canonical form here restores symmetry
   * between add and remove.
   *
   * @param pattern the caller-supplied pattern
   * @param action  the action of the rule being addressed
   * @return the canonical pattern, or {@code pattern} unchanged when it cannot
   *         be canonicalized (a malformed {@code regex:} pattern could never
   *         have been added either, so there is nothing to match)
   */
  private static String canonicalPattern(String pattern, RuleAction action) {
    if (pattern == null) {
      return null;
    }
    try {
      return RuleMatcher.of(pattern, action).getPattern();
    } catch (PatternSyntaxException e) {
      return pattern;
    }
  }

  /**
   * Remove all rules and propagate the empty rule list.
   *
   * <p>After clearing, the rule list is empty, the version counter is
   * incremented, and the empty list is persisted to Redis and send
   * to sibling instances. This ensures that all nodes converge to the
   * empty state.
   */
  public void clearRules() {
    StagedRules staged;
    synchronized (this) {
      int before = rulesSnapshot.length;
      rulesSnapshot = EMPTY_RULES;
      rulesVersion.incrementAndGet();

      staged = stagePersist();
      log.info("All rules cleared (removed: {}, version: {})", before, rulesVersion.get());
    }
    persistAndBroadcast(staged);
  }

  /**
   * Remove all rules whose action equals the given value.
   *
   * <p>If any rules are removed, the change is persisted and send.
   * The version counter is incremented with each batch removal.
   *
   * @param action the action to match; all rules with this action are
   *               removed regardless of their pattern
   * @return the number of rules removed (zero if no matching rules exist)
   */
  public int removeRulesByAction(RuleAction action) {
    int removed;
    StagedRules staged = null;
    synchronized (this) {
      int before = rulesSnapshot.length;
      removeIfMatching(rule -> rule.getAction() == action);
      removed = before - rulesSnapshot.length;

      if (removed > 0) {
        rulesVersion.incrementAndGet();
        staged = stagePersist();
        log.info(
          "Rules removed by action: {} (removed: {}, total: {}, version: {})",
          action,
          removed,
          rulesSnapshot.length,
          rulesVersion.get()
        );
      }
    }
    persistAndBroadcast(staged);
    return removed;
  }

  /**
   * Remove the rule at the given index in the current rule list.
   *
   * <p>If the index is valid (non-negative and less than the current list
   * size), the rule is removed and the change is persisted and send.
   * Out-of-bounds indices are silently ignored.
   *
   * @param index the zero-based index of the rule to remove
   */
  public void removeRule(int index) {
    StagedRules staged = null;
    synchronized (this) {
      if (index >= 0 && index < rulesSnapshot.length) {
        Rule removed = rulesSnapshot[index];
        Rule[] next = new Rule[rulesSnapshot.length - 1];

        System.arraycopy(rulesSnapshot, 0, next, 0, index);
        System.arraycopy(rulesSnapshot, index + 1, next, index, next.length - index);

        rulesSnapshot = next;
        rulesVersion.incrementAndGet();
        staged = stagePersist();
        log.info(
          "Rule removed by index {}: pattern='{}', action={} (total: {}, version: {})",
          index,
          removed.getPattern(),
          removed.getAction(),
          rulesSnapshot.length,
          rulesVersion.get()
        );
      }
    }
    persistAndBroadcast(staged);
  }

  /**
   * Synchronize the rule list from a broadcast message, guarded by version.
   * <p>
   * If {@code incomingVersion > 0} and {@code <= localVersion}, the sync is
   * skipped (stale send). Otherwise, per ADR-0062:
   * <ul>
   *   <li>{@code incomingVersion > 0} — the incoming set is <b>authoritative</b>:
   *       the local list is replaced wholesale and the incoming version is
   *       adopted exactly. Deletions and clears therefore converge; a local rule
   *       absent from a fresher broadcast is dropped (its own broadcast was
   *       lost — Redis persistence is the durable backstop, per ADR-0013).</li>
   *   <li>{@code incomingVersion == 0} — legacy pre-versioning broadcast
   *       (rolling upgrade): union merge by pattern, local-only entries
   *       preserved, version bumped past the merge.</li>
   * </ul>
   * <p>
   * The merged/replaced set is persisted to Redis at the adopted version but
   * <b>not</b> re-send — avoiding a send storm.
   *
   * @param json            the JSON-serialized rule list (may be old-format array or
   *                        new-format wrapper)
   * @param incomingVersion the rulesVersion from the incoming message header,
   *                        or {@code 0L} for pre-version broadcasts
   */
  public void syncRules(String json, long incomingVersion) {
    StagedRules staged;
    try {
      synchronized (this) {
        if (incomingVersion > 0L && incomingVersion <= rulesVersion.get()) {
          log.debug(
            "Stale rules sync ignored: incomingVersion={}, localVersion={}",
            incomingVersion,
            rulesVersion.get()
          );
          return;
        }
        ParsedRuleSet parsed = parseRulesJson(json);
        if (incomingVersion > 0L) {
          replaceRules(parsed.rules());
        } else {
          replaceRules(mergeRules(List.of(rulesSnapshot), parsed.rules()));
          rulesVersion.incrementAndGet();
        }
        rulesVersion.updateAndGet(v -> Math.max(v, Math.max(incomingVersion, parsed.version())));

        long effectiveVersion = rulesVersion.get();
        staged = new StagedRules(
          serializeRules(List.of(rulesSnapshot), effectiveVersion),
          effectiveVersion,
          rulesSnapshot.length
        );
      }
    } catch (Exception e) {
      log.error("Failed to sync rules from send", e);
      return;
    }
    try {
      redisTemplate.ifPresent(r -> persistToRedis(r, staged.json(), staged.version()));
      log.info("Rules synced from send, version={}, total: {}", staged.version(), staged.total());
    } catch (Exception e) {
      log.error("Failed to sync rules from send", e);
    }
  }

  /**
   * Atomically swap the rule list.  Does <b>not</b> persist or send.
   * <p>
   * Primarily used internally by {@link #syncRules} and for tests.
   *
   * @param newRules the new rule list; each element is {@link Rule#prepare() prepared} before
   *     insertion, must not be {@code null}
   */
  public synchronized void replaceRules(List<Rule> newRules) {
    int oldSize = rulesSnapshot.length;
    Rule[] replacement = new Rule[newRules.size()];
    int idx = 0;

    for (Rule r : newRules) {
      r.prepare();
      replacement[idx++] = r;
    }

    rulesSnapshot = replacement;
    log.info("Rules replaced: {} -> {} rules", oldSize, replacement.length);
  }

  /** @return an unmodifiable snapshot of the current rules (safe for iteration). */
  public List<Rule> getAllRules() {
    return List.of(rulesSnapshot);
  }

  /**
   * Evaluate the rule set for the given key and return an {@link Optional}
   * that describes how the caller should behave.
   * <ul>
   *   <li>{@code Optional.empty()} – {@code BLOCK}: reject the access immediately.</li>
   *   <li>{@code Optional.of(true)} – {@code ALLOW_NO_REPORT}: allow the access
   *       but suppress the hot‑key reportToWorker.</li>
   *   <li>{@code Optional.of(false)} – no matching rule or {@code ALLOW}:
   *       proceed normally with reporting.</li>
   * </ul>
   *
   * @param cacheKey  the key being accessed
   * @param operation a label used only in log messages (e.g. "get", "getWithSoftExpire")
   * @return {@link Optional#empty()} if the rule action is {@link RuleAction#BLOCK},
   *         {@code Optional.of(true)} if the rule action is {@link RuleAction#ALLOW_NO_REPORT},
   *         {@code Optional.of(false)} if no rule matches or the action is {@link RuleAction#ALLOW}
   */
  public Optional<Boolean> isAllowNoReport(String cacheKey, String operation) {
    RuleAction action = evaluateRule(cacheKey);
    if (action == RuleAction.BLOCK) {
      log.debug("{}: blocked by rule: {}", operation, cacheKey);
      return Optional.empty();
    }
    return Optional.of(action == RuleAction.ALLOW_NO_REPORT);
  }

  /**
   * Evaluate the rule set for the given key, returning the action of the first matching
   * rule, or {@link RuleAction#ALLOW} when nothing matches. The scan walks the immutable
   * snapshot array in insertion order — no iterator allocation.
   *
   * <p>Results are memoized per key (ADR-0065): the memo entry carries the snapshot
   * reference it was computed from, so any mutation (which always swaps in a fresh array)
   * invalidates every previous entry by reference inequality — a new, removed, or replaced
   * rule is honored on the very next evaluation with no invalidation step. An empty rule
   * set short-circuits to {@code ALLOW} without touching the memo; a {@code null} key
   * skips the memo and keeps the historical scan semantics (NPE from a matching EXACT
   * rule, {@code ALLOW} when no rules are configured).
   *
   * @param cacheKey the key to evaluate against all rules
   * @return the {@link RuleAction} of the first matching rule, or {@link RuleAction#ALLOW} if no rule matches
   */
  public RuleAction evaluateRule(String cacheKey) {
    Rule[] snapshot = rulesSnapshot;
    if (snapshot.length == 0) {
      return RuleAction.ALLOW;
    }

    if (cacheKey != null) {
      MemoEntry memo = decisionCache.getIfPresent(cacheKey);
      if (memo != null && memo.snapshot() == snapshot) {
        return memo.action();
      }
      RuleAction action = scanRules(cacheKey, snapshot);
      decisionCache.put(cacheKey, new MemoEntry(snapshot, action));
      return action;
    }
    return scanRules(cacheKey, snapshot);
  }

  /**
   * Ordered scan of one snapshot; the first matching rule's action wins. Evaluation
   * failures are logged once per rule instance and the broken rule is skipped.
   */
  private RuleAction scanRules(String cacheKey, Rule[] snapshot) {
    for (Rule rule : snapshot) {
      try {
        if (rule.match(cacheKey)) {
          return rule.getAction();
        }
      } catch (RuntimeException e) {
        if (e instanceof NullPointerException) throw e;
        logRuleFailure(rule, e);
      }
    }
    return RuleAction.ALLOW;
  }

  /**
   * Staged persist payload: the serialized rule set and its version, captured
   * atomically under the mutation monitor. The record is immutable, so the
   * I/O can safely run after the monitor is released.
   */
  private record StagedRules(String json, long version, int total) {}

  /**
   * Serialize the current rule list and version under the monitor. In-memory
   * only — the caller runs {@link #persistAndBroadcast(StagedRules)} AFTER
   * releasing the monitor, so the Redis/AMQP I/O never holds the mutation
   * lock (one slow Redis round trip must not serialize every rule mutation
   * or pin the sync dispatcher thread).
   *
   * @return the staged payload, or {@code null} when serialization failed
   *         (already logged — the caller then skips the I/O)
   */
  private StagedRules stagePersist() {
    try {
      long version = rulesVersion.get();
      String json = serializeRules(List.of(rulesSnapshot), version);
      return new StagedRules(json, version, rulesSnapshot.length);
    } catch (Exception e) {
      log.error("Failed to serialize rules", e);
      return null;
    }
  }

  /**
   * Run the staged persist outside the mutation monitor: write the versioned
   * JSON to Redis (if available) via Lua compare-and-set and send it via AMQP
   * (if available). Both channels are invoked independently — the XOR pattern
   * has been replaced by both/and. Failures are logged but never propagated.
   * <p>
   * A payload staged before a newer mutation may land after it — the versioned
   * CAS and the receivers' strictly-fresher adoption (ADR-0062) reject the
   * stale write, so the relaxed ordering changes no convergence semantics.
   *
   * @param staged the payload from {@link #stagePersist()}; {@code null} is a
   *               no-op
   */
  private void persistAndBroadcast(StagedRules staged) {
    if (staged == null) {
      return;
    }

    try {
      redisTemplate.ifPresent(r -> persistToRedis(r, staged.json(), staged.version()));
      cacheSyncPublisher.ifPresent(p -> p.broadcastAllLocalRules(staged.json(), staged.version()));

      log.debug("Rules persisted (version={}), total: {}", staged.version(), staged.total());
    } catch (Exception e) {
      log.error("Failed to persist rules", e);
    }
  }

  /**
   * Write the versioned JSON rule set to Redis via Lua compare-and-set.
   * Falls back to plain SET if the Lua script fails.
   *
   * <p>A CAS rejection (script returns 0 — Redis already holds an equal or
   * newer version) is expected after adopting a peer's broadcast at the same
   * version, and is logged at most once per {@link LogThrottle#DEFAULT_WINDOW_MS}
   * window (DEBUG in between), so a divergence between the local counter and
   * Redis's version is always observable.
   */
  private void persistToRedis(StringRedisTemplate r, String json, long version) {
    try {
      Long result = r.execute(CasScriptHolder.SCRIPT, List.of(KEY_RULES), json, String.valueOf(version));
      if (result != 1L) {
        if (casRejectLogThrottle.tryAcquire()) {
          log.warn(
            "Redis rules CAS rejected (result={}, version={}); Redis holds an equal or newer rule set",
            result,
            version
          );
        } else {
          log.debug("Redis rules CAS rejected (result={}, version={})", result, version);
        }
      }
    } catch (Exception e) {
      log.warn("Lua compare-and-set failed for rules (version={}), fallback to plain set", version, e);
      try {
        r.opsForValue().set(KEY_RULES, json);
      } catch (Exception e2) {
        log.error("Redis fallback set also failed, skipping persist", e2);
      }
    }
  }

  /**
   * Serialize a rule list and its associated version into the versioned JSON
   * wrapper format.
   *
   * <p>The output format is a JSON object with two top-level keys:
   * <pre>{@code {"rulesVersion":5,"rules":[{"pattern":"x","action":"BLOCK"}]}}</pre>
   *
   * <p>This wrapper format is used for both Redis persistence and AMQP
   * send, enabling the receiver to apply version-gated replace logic.
   *
   * @param rules   the rule list to serialize (a snapshot, typically via
   *                {@link #getAllRules()})
   * @param version the current rules version to embed in the wrapper
   * @return the JSON string representation
   * @throws JsonProcessingException if Jackson serialization fails
   */
  private String serializeRules(List<Rule> rules, long version) throws JsonProcessingException {
    Map<String, Object> wrapper = new LinkedHashMap<>();
    wrapper.put("rulesVersion", version);
    wrapper.put("rules", rules);

    return OBJECT_MAPPER.writeValueAsString(wrapper);
  }

  /**
   * Merge two rule lists by pattern identity ({@link Rule#getPattern()}).
   *
   * <p>The merge semantics follow a "last writer wins" strategy on a per-pattern
   * basis: for each pattern in the incoming list, the incoming rule replaces
   * any local rule with the same pattern. Local rules whose patterns are not
   * present in the incoming list are preserved.
   *
   * <p>Since ADR-0062 this union merge is used <b>only</b> for legacy
   * unversioned broadcasts ({@code incomingVersion == 0}, rolling upgrades);
   * versioned broadcasts are authoritative full replacements, because the
   * union merge resurrects deleted rules cluster-wide and re-persists them
   * at a higher version.
   *
   * @param local    the current local rule list
   * @param incoming the incoming rule list from a legacy send or Redis load
   * @return a new merged rule list combining both sources
   */
  private static List<Rule> mergeRules(List<Rule> local, List<Rule> incoming) {
    Map<String, Rule> map = new LinkedHashMap<>();
    for (Rule rule : local) {
      map.put(rule.getPattern(), rule);
    }
    for (Rule rule : incoming) {
      map.put(rule.getPattern(), rule);
    }
    return new ArrayList<>(map.values());
  }

  /**
   * Parse a JSON string into a {@link ParsedRuleSet}, supporting both the
   * legacy array format and the current versioned wrapper format.
   *
   * <p><b>Legacy format:</b> a plain JSON array of rule objects, e.g.
   * <pre>{@code [{"pattern":"x","action":"BLOCK"}]}</pre>
   * carries no version (reported as {@code 0L}).
   *
   * <p><b>Current format:</b> a JSON object with a {@code "rules"} key
   * containing the array, e.g.
   * <pre>{@code {"rulesVersion":5,"rules":[{"pattern":"x","action":"BLOCK"}]}}</pre>
   *
   * <p>The format is auto-detected by examining the first character:
   * {@code [} indicates legacy array format, {@code \{} indicates the
   * versioned wrapper. Backward compatibility is maintained to support
   * rolling upgrades.
   *
   * @param json the JSON string to parse; may be {@code null} or empty
   *             (in which case an empty rule set is returned upstream by the
   *             caller's null check)
   * @return the parsed rule set; never {@code null} (returns an
   *         empty rule set if the wrapper format lacks a {@code "rules"} key
   *         or if the value is not an array)
   * @throws JsonProcessingException if the JSON is malformed
   */
  private static ParsedRuleSet parseRulesJson(String json) throws JsonProcessingException {
    String trimmed = json.trim();
    if (trimmed.startsWith("[")) {
      return new ParsedRuleSet(OBJECT_MAPPER.readValue(json, new TypeReference<>() {}), 0L);
    }

    Map<String, Object> wrapper = OBJECT_MAPPER.readValue(json, new TypeReference<>() {});
    Object raw = wrapper.get("rules");
    if (raw instanceof List<?>) {
      List<Rule> rules = OBJECT_MAPPER.convertValue(raw, new TypeReference<>() {});
      Object version = wrapper.get("rulesVersion");
      long v = version instanceof Number number ? number.longValue() : 0L;
      return new ParsedRuleSet(rules, v);
    }
    return new ParsedRuleSet(List.of(), 0L);
  }

  /** Parsed payload: the rule list plus the version embedded in the wrapper ({@code 0} for legacy arrays). */
  private record ParsedRuleSet(List<Rule> rules, long version) {}

  /**
   * Explicitly reload rules from Redis and send the full rule set to
   * all sibling application instances via AMQP.
   *
   * <p>This method is useful for operational scenarios such as:
   * <ul>
   *   <li>An operator manually adjusted rules on one node and wants the
   *       entire cluster to converge immediately</li>
   *   <li>Recovering from a missed send during a temporary network
   *       partition</li>
   *   <li>Initial cluster synchronization after a new node joins</li>
   * </ul>
   *
   * <p>The rule list is first reloaded from Redis (to pick up any
   * externally written changes), then serialized with the current version
   * and send. If no {@link CacheSyncPublisher} is configured, the
   * method logs a warning and returns without broadcasting.
   */
  public void broadcastAllLocalRulesManually() {
    loadRulesFromRedis();

    if (cacheSyncPublisher.isPresent()) {
      StagedRules staged;
      synchronized (this) {
        staged = stagePersist();
      }
      if (staged == null) {
        return;
      }
      try {
        cacheSyncPublisher.get().broadcastAllLocalRules(staged.json(), staged.version());

        log.info("Rules send manually (version={}), total: {}", staged.version(), staged.total());
      } catch (Exception e) {
        log.error("Failed to broadcast rules", e);
      }
    }
  }

  /**
   * Load persisted rules from Redis (if available) and atomically replace
   * the local rule list with the loaded rules.
   *
   * <p>If Redis is not configured, this method is a no-op. If the Redis
   * fetch or JSON parsing fails, the error is logged and the current
   * in-memory rule list is preserved — the application continues operating
   * with the existing rules.
   *
   * <p>The persisted {@code rulesVersion} is restored into the local counter
   * (max of local and persisted) so that post-restart local changes are
   * neither rejected by the Redis CAS gate nor skipped by peers as stale.
   *
   * <p>This method is called at startup via {@link #initRules()} and can
   * also be invoked manually via {@link #broadcastAllLocalRulesManually()}
   * to refresh from Redis on demand.
   */
  private synchronized void loadRulesFromRedis() {
    redisTemplate.ifPresent(r -> {
      try {
        String json = r.opsForValue().get(KEY_RULES);

        if (json == null || json.isEmpty()) {
          log.info("No rules found in Redis, starting with empty rule set");
          return;
        }

        ParsedRuleSet parsed = parseRulesJson(json);
        replaceRules(parsed.rules());
        rulesVersion.updateAndGet(v -> Math.max(v, parsed.version()));

        log.info("Rules loaded from Redis: {} rules (version={})", parsed.rules().size(), rulesVersion.get());
      } catch (Exception e) {
        log.error("Failed to load rules from Redis", e);
      }
    });
  }

  /** Append a rule to a snapshot array, returning a new array (copy-on-write). */
  private static Rule[] append(Rule[] rules, Rule rule) {
    Rule[] next = Arrays.copyOf(rules, rules.length + 1);
    next[rules.length] = rule;
    return next;
  }

  /** Rebuild the snapshot without the matching rules; {@code true} iff anything was removed. */
  private boolean removeIfMatching(Predicate<Rule> predicate) {
    List<Rule> remaining = new ArrayList<>(rulesSnapshot.length);
    for (Rule rule : rulesSnapshot) {
      if (!predicate.test(rule)) {
        remaining.add(rule);
      }
    }
    if (remaining.size() == rulesSnapshot.length) {
      return false;
    }
    rulesSnapshot = remaining.toArray(EMPTY_RULES);
    return true;
  }

  /**
   * Log a rule-evaluation failure once per rule instance at ERROR and
   * subsequently at DEBUG, so a permanently broken rule cannot spam the hot
   * path (project logging principle: never log per-op on the hot path).
   */
  private void logRuleFailure(Rule rule, RuntimeException e) {
    if (brokenRules.add(rule)) {
      log.error(
        "Rule evaluation failed for pattern='{}', type={}, skipping (further failures logged at DEBUG)",
        rule.getPattern(),
        rule.getType(),
        e
      );
    } else {
      log.debug("Rule evaluation failed for pattern='{}', type={}, skipping", rule.getPattern(), rule.getType(), e);
    }
  }
}
