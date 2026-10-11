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
package io.github.hyshmily.zeta.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.annotation.annotationsupporter.NullValue;
import io.github.hyshmily.zeta.cache.cachesupport.*;
import io.github.hyshmily.zeta.cache.codec.CacheCompressor;
import io.github.hyshmily.zeta.cache.codec.DefaultWeigher;
import io.github.hyshmily.zeta.exception.ZetaBlockedException;
import io.github.hyshmily.zeta.exception.ZetaExceptionHandler;
import io.github.hyshmily.zeta.exception.ZetaSuppressedException;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.model.*;
import io.github.hyshmily.zeta.rule.Rule.RuleAction;
import io.github.hyshmily.zeta.rule.RuleMatcher;
import io.github.hyshmily.zeta.scheduler.BackgroundRefresher;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.util.LogThrottle;
import io.github.hyshmily.zeta.util.version.VersionController;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.hyshmily.zeta.cache.cachesupport.CacheKeysPolicy.invalidCacheKey;
import static io.github.hyshmily.zeta.cache.cachesupport.CacheKeysPolicy.isWorkerManaged;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Version.VERSION_DEFAULT;

/**
 * Core orchestration class for hot-key caching.
 * <p>
 * Manages L1 (Caffeine) operations with hot-key awareness, dual version
 * tracking
 * ({@code dataVersion} for data mutation ordering, {@code decisionVersion} for
 * Worker HOT/COOL decision ordering),
 * SingleFlight deduplication, soft-expire (stale-while-revalidate), and
 * cross-instance synchronization via {@link CacheSyncPublisher}.
 * All write operations are transaction-aware via {@link TransactionSupport}.
 */
@Slf4j
@Internal
public class HotKeyCache {

  /** Local TopK detector (HotKeyDetector) for identifying hot keys. */
  private final HotKeyDetector hotKeyDetector;
  /** Underlying L1 Caffeine cache storing {@link CacheEntry} slots. */
  private final Cache<String, CacheEntry> caffeineCache;
  /** Deduplicator preventing concurrent in-flight loads for the same key. */
  private final SingleFlight singleFlight;
  /** Manages entry lifecycle: guards, demotion, the draft factory, value wrapping. */
  private final EntryLifecycle entryLifecycle;

  /** Background soft-expire refresh executor (scheduler seam). */
  private final BackgroundRefresher backgroundRefresher;
  /** Executor for async cache operations (promotion, soft refresh). */
  private final Executor hotKeyExecutor;

  /**
   * Central dispatcher for all external communication (reportToWorker + send).
   */
  private final CentralDispatcher dispatcher;

  /** Matches cache keys against blacklist/whitelist rules. */
  private final RuleMatcher ruleMatcher;
  /** Manages data version generation for mutation ordering. */
  private final VersionController versionController;

  /**
   * Cache-core configuration view (null-value TTL, key normalization) —
   * implemented at the assembly layer by the live {@code zeta.local.*} bean,
   * so runtime configuration updates keep working (ADR-0082).
   */
  private final CacheCoreSettings zetaProperties;

  /** Cached view of Worker cluster health, used for COOL promotion decisions. */
  private final HealthView healthView;

  /** Compressor for L1 cache values. */
  private final CacheCompressor compressor;

  /**
   * Refault distance admission gate (ADR-0079, the {@code mm/workingset.c} port),
   * shared with the L1 cache's removal listener. Consulted before load-path
   * inserts (and replacements of dead entries) to refuse keys whose
   * inter-access distance exceeds L1 capacity. {@code null} in plain
   * constructions (tests, legacy wiring) = disabled.
   */
  @Nullable
  private final RefaultAdmission refaultAdmission;

  /**
   * Local-TopK promotion collaborator (see {@link LocalPromotion}): owns the
   * promote-if-hot rules for both the lock-free read path and the in-lock
   * fast path. Composed here — not a constructor parameter — so the facade's
   * wiring surface stays unchanged.
   */
  private final LocalPromotion promotion;

  /**
   * Request-level hit/miss counters behind {@link #stats()}: incremented once
   * per served key on the load-capable read paths ({@code readThrough},
   * {@code batchGet}, {@code computeInLock}). A key served from L1 without
   * invoking the reader counts as a hit (sentinel hits included); a key that
   * reaches the loader counts as a miss — unlike the raw Caffeine counters,
   * internal {@code asMap().compute()} maintenance writes never touch these.
   * {@code peek()}/{@code peekAndTag()} probes are excluded.
   */
  private final LongAdder requestHits = new LongAdder();
  private final LongAdder requestMisses = new LongAdder();

  /**
   * Creates the cache orchestrator. The parameter order is the historical
   * field order (previously Lombok-generated); {@link LocalPromotion} is
   * composed internally from the same collaborators.
   */ @SuppressWarnings("java:S107") // 13 params: the established wiring surface, kept stable
  public HotKeyCache(
    HotKeyDetector hotKeyDetector,
    Cache<String, CacheEntry> caffeineCache,
    SingleFlight singleFlight,
    EntryLifecycle entryLifecycle,
    BackgroundRefresher backgroundRefresher,
    Executor hotKeyExecutor,
    CentralDispatcher dispatcher,
    RuleMatcher ruleMatcher,
    VersionController versionController,
    CacheCoreSettings zetaProperties,
    HealthView healthView,
    CacheCompressor compressor,
    @Nullable RefaultAdmission refaultAdmission
  ) {
    this.hotKeyDetector = hotKeyDetector;
    this.caffeineCache = caffeineCache;
    this.singleFlight = singleFlight;
    this.entryLifecycle = entryLifecycle;
    this.backgroundRefresher = backgroundRefresher;
    this.hotKeyExecutor = hotKeyExecutor;
    this.dispatcher = dispatcher;
    this.ruleMatcher = ruleMatcher;
    this.versionController = versionController;
    this.zetaProperties = zetaProperties;
    this.healthView = healthView;
    this.compressor = compressor;
    this.refaultAdmission = refaultAdmission;
    this.promotion = new LocalPromotion(caffeineCache, hotKeyDetector, entryLifecycle, healthView);
  }

  /**
   * Resolve the TTL for {@link NullValue} sentinels.
   * Extracted to a method to fix field-initialization ordering: the
   * original inline expression referenced {@code zetaProperties} before
   * the constructor had set it.
   *
   * @return null-value TTL in milliseconds
   */
  private long nullTtlMs() {
    return TimeUnit.SECONDS.toMillis(zetaProperties.getNullValueTtlSeconds());
  }

  /**
   * Rate-limits the read-path ERROR log to one per {@value LogThrottle#DEFAULT_WINDOW_MS}ms
   * window, so a failing data source cannot flood the log from every {@link #execute} call.
   * Mirrors the ADR-0037 flush-WARN limiter pattern in {@code BroadcastBuffer}.
   * Admission is strict — {@link LogThrottle} claims the window with a
   * compare-and-set, so exactly one caller per window logs. The atomicity and the
   * monotonic clock are provided by {@link LogThrottle}.
   */
  private final LogThrottle readErrorLogThrottle = LogThrottle.perDefaultWindow();

  /**
   * Rate-limits the degraded-write-skip WARN (P0-5): under a Redis outage
   * every putThrough takes the degraded fallback and the version guard keeps
   * the healthy entry, so an unthrottled WARN would fire at write rate.
   */
  private final LogThrottle degradedSkipLogThrottle = LogThrottle.perDefaultWindow();

  /**
   * Rate-limits the oversized-bypass WARN (P2): values over
   * {@code max-value-size} are served but take no L1 residency — without a
   * visible signal the caller observes "write succeeds but every read misses".
   */
  private final LogThrottle oversizedLogThrottle = LogThrottle.perDefaultWindow();

  /**
   * Normalize a cache key if query-param stripping is enabled.
   * Delegates to {@link CacheKeysPolicy#normalizeKey} only when the
   * feature is configured; otherwise returns the key unchanged (zero overhead).
   */
  private String normalize(String cacheKey) {
    if (cacheKey != null && zetaProperties.isStripQuery()) {
      return CacheKeysPolicy.normalizeKey(cacheKey);
    }
    return cacheKey;
  }

  /**
   * Guard against invalid cache keys and blocked keys.
   *
   * @param cacheKey the cache key to check
   * @return the {@link RuleAction} for a valid key, or {@code null} if the key is
   *         invalid
   * @throws ZetaBlockedException when the key matches a blocklist rule
   */
  @Nullable
  private RuleAction preGuard(String cacheKey) {
    if (invalidCacheKey(cacheKey)) {
      return null;
    }
    RuleAction action = ruleMatcher.evaluateRule(cacheKey);
    if (action == RuleAction.BLOCK) {
      throw new ZetaBlockedException("HotKeyCache", cacheKey);
    }
    return action;
  }

  /**
   * Secondary BLOCK check (TOCTOU guard for paths where the key was already
   * validated).
   *
   * @param cacheKey the cache key to check
   * @throws ZetaBlockedException when the key matches a blocklist rule
   */
  private void afterGuard(String cacheKey) {
    if (ruleMatcher.evaluateRule(cacheKey) == RuleAction.BLOCK) {
      throw new ZetaBlockedException("HotKeyCache", cacheKey);
    }
  }

  /**
   * Check whether a key is currently tracked as a local hot key in L1.
   *
   * @param cacheKey the key to inspect
   * @return {@code true} if the key exists in L1 with {@link KeyState#HOT}
   */
  public boolean isHot(String cacheKey) {
    cacheKey = normalize(cacheKey);
    return isHot(cacheKey, null);
  }

  /**
   * Check whether a key is locally tracked as hot, using an optional cached
   * entry to avoid a redundant Caffeine lookup.
   *
   * @param cacheKey the key to inspect
   * @param entry    an existing {@link CacheEntry} (may be {@code null}), or
   *                 {@code null} to read from L1
   * @return {@code true} if the key is a local hot key
   */
  @SuppressWarnings("java:S3358")
  private boolean isHot(String cacheKey, CacheEntry entry) {
    if (invalidCacheKey(cacheKey)) {
      if (log.isDebugEnabled()) {
        log.debug("isHot: invalid cacheKey");
      }
      return false;
    }

    final CacheEntry cacheEntry = entry != null ? entry : caffeineCache.getIfPresent(cacheKey);

    return (
      (cacheEntry != null &&
        !entryLifecycle.ttlPolicy().isLogicallyExpired(cacheEntry) &&
        cacheEntry.getKeyState() == KeyState.HOT) ||
      hotKeyDetector.contains(cacheKey)
    );
  }

  /**
   * Whether to skip Worker reporting, combining the rule-based guard decision
   * with the caller's explicit per-invocation flag. Either reason alone skips:
   * the guard said {@code ALLOW_NO_REPORT}, or the caller disabled reporting
   * ({@code ReadPolicy.reportEnabled == false}).
   *
   * @param isSkipReportByGuard whether the rule engine says to skip reporting
   * @param reportEnabled   whether the caller allows reporting for this call
   * @return {@code true} when reporting must be skipped for either reason
   */
  private static boolean isSkipReport(boolean isSkipReportByGuard, boolean reportEnabled) {
    return isSkipReportByGuard || !reportEnabled;
  }

  /**
   * Execute a cache operation with standardized error handling — see
   * {@link #executeCore} for the full contract; failures resolve to
   * {@link Optional#empty()}.
   */
  private <T> Optional<T> execute(String cacheKey, boolean failOnError, Supplier<Optional<T>> action) {
    return executeCore(cacheKey, failOnError, action, Optional::empty);
  }

  /**
   * Hit-outcome variant of {@link #execute} for the batch hit checks: identical
   * error handling, resolving failures to a miss outcome (the batch loop
   * reloads) instead of an empty {@link Optional}.
   */
  private <T> HitOutcome<T> executeHit(String cacheKey, boolean failOnError, Supplier<HitOutcome<T>> action) {
    return executeCore(cacheKey, failOnError, action, HitOutcome::miss);
  }

  /**
   * Shared error-handling core for the read paths: {@link ZetaBlockedException}
   * is rethrown, all other {@link RuntimeException} are logged and swallowed —
   * unless {@code failOnError} is set, in which case they propagate to the
   * caller so a failing data source stays distinguishable from a missing key.
   * <p>
   * When no {@code failOnError} is requested and a handler is installed via
   * {@link ZetaExceptionHandler} (thread-local, inheritable, or default), the
   * failure is routed to it first with the chain's WARN fallback disabled —
   * the chain never throws, so the read path stays operational, and the
   * handler is resolved exactly once. Without a handler, the ERROR log below
   * remains the only signal; the chain's WARN fallback would duplicate it.
   *
   * @param cacheKey    the key being accessed (used in the error log)
   * @param failOnError whether {@link RuntimeException}s propagate instead of
   *                    being swallowed
   * @param action      the cache operation to execute
   * @param fallback    the outcome returned when an error is swallowed (the
   *                    caller's shape of "empty": {@link Optional#empty()} or a
   *                    {@link HitOutcome#miss()})
   * @param <R>         the outcome type
   * @return the result of {@code action}, or {@code fallback} on error
   */
  private <R> R executeCore(String cacheKey, boolean failOnError, Supplier<R> action, Supplier<R> fallback) {
    try {
      return action.get();
    } catch (ZetaBlockedException e) {
      throw e;
    } catch (RuntimeException e) {
      if (failOnError) {
        throw e;
      }
      ZetaExceptionHandler.handleException("HotKeyCache read failed for key " + cacheKey, e, false);
      if (readErrorLogThrottle.tryAcquire()) {
        log.error(
          "HotKeyCache internal error for key={}, returning empty to keep caller operational (further failures suppressed for 10s)",
          cacheKey,
          e
        );
      }
      return fallback.get();
    }
  }

  /**
   * Record a cache access as potentially hot, reporting it to the Worker
   * without performing a cache read.
   *
   * <p>
   * This is essentially a fire-and-forget hint: it updates the local
   * HeavyKeeper sketch and enqueues the key for batch reporting to the
   * Worker cluster, exactly as {@code get()} would, but without touching
   * the L1 cache or invoking any reader/loader. Useful when the caller
   * already has the value in hand (e.g. from a write path or an external
   * source) and wants the hot-key detector and Worker to know about the
   * access without a redundant cache lookup.
   *
   * @param cacheKey the key to record
   */
  public void recordAccess(String cacheKey) {
    recordAccess(cacheKey, Zeta.TagMode.RECORD);
  }

  /**
   * Record a cache access with an explicit recording mode.
   *
   * @param cacheKey the key to record
   * @param mode     how much of the pipeline to run (never {@code null})
   */
  public void recordAccess(String cacheKey, Zeta.TagMode mode) {
    cacheKey = normalize(cacheKey);
    if (invalidCacheKey(cacheKey)) {
      return;
    }
    // Rule guard: a blacklisted key never reaches the detector/reporter, but
    // unlike the read paths a tag is a fire-and-forget hint — it must not
    // break the business method, so BLOCK drops the hint instead of throwing.
    // A whitelist match (ALLOW_NO_REPORT) forces detect-only regardless of
    // the caller's mode, mirroring readThrough's isSkipReport merge.
    RuleAction action = ruleMatcher.evaluateRule(cacheKey);
    if (action == RuleAction.BLOCK) {
      if (log.isDebugEnabled()) {
        log.debug("recordAccess: dropped hint for blocked key");
      }
      return;
    }
    boolean skipReport = mode.skipReport() || action == RuleAction.ALLOW_NO_REPORT;
    dispatcher.recordAccess(cacheKey, mode.skipDetection(), skipReport);
  }

  /**
   * Look up a cached value without loading or triggering hot-key detection.
   * Unlike {@link #get}, this method never invokes the reader or SingleFlight.
   * It still evaluates the blacklist/whitelist rules (a blocked key throws)
   * and the hard-TTL bound (a logically expired entry resolves empty) — so it
   * is free of load/detection/report side effects, but not free of guards.
   *
   * @param cacheKey the key to inspect
   * @return an {@link Optional} containing the raw value if present
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  @SuppressWarnings("all")
  public <T> Optional<T> peek(String cacheKey) {
    String nk = normalize(cacheKey);
    if (preGuard(nk) == null) {
      return Optional.empty();
    }

    return execute(nk, false, () -> {
      CacheEntry raw = caffeineCache.getIfPresent(nk);
      if (raw == null || entryLifecycle.invalidateIfIsLogicallyExpired(nk, raw)) {
        return Optional.empty();
      }
      return Optional.ofNullable(this.<T>unwrapRawValue(raw, nk));
    });
  }

  /**
   * Annotation-path peek for the Spring Cache adapter
   * ({@code ZetaSpringCache.lookup}): a <b>single</b> hash lookup plus a
   * <b>single</b> rule evaluation, feeding detection/reporting for a served
   * hit with the facade read path's rule handling — a whitelist match
   * ({@code ALLOW_NO_REPORT}) detects without reporting, {@code ALLOW}
   * detects and reports, a true miss does neither, and a blocked key throws
   * via {@link #preGuard}.
   *
   * <p>
   * This is the merged form of {@link #peek} + rule-gated tagging.
   * {@code peek} discards the {@code preGuard} action and maps a cached-null
   * sentinel to an empty hit, so the adapter previously had to re-evaluate
   * the rules and re-probe the L1 to recover both — two rule matches and two
   * hash lookups per annotation-path read. This method resolves both in one
   * pass. It is <b>not</b> a drop-in {@code peek} replacement elsewhere: a
   * served hit (value or sentinel) has the side effect of counting the access
   * on the detector and the Worker reporter.
   *
   * @param cacheKey the key to look up
   * @return the unwrapped user value, {@link NullValue#INSTANCE} for a
   *         cached-null sentinel hit (the adapter maps it to Spring's marker),
   *         or {@code null} on a true miss
   */
  public Object peekAndTag(String cacheKey) {
    String nk = normalize(cacheKey);
    RuleAction action = preGuard(nk);
    if (action == null) {
      return null;
    }

    Object served = executeCore(
      nk,
      false,
      () -> {
        CacheEntry raw = caffeineCache.getIfPresent(nk);
        if (raw == null || entryLifecycle.invalidateIfIsLogicallyExpired(nk, raw)) {
          return null;
        }
        return raw.getValue() == NullValue.INSTANCE ? NullValue.INSTANCE : unwrapValue(raw.getValue(), nk);
      },
      () -> null
    );

    if (served != null) {
      dispatcher.recordAccess(nk, false, action == RuleAction.ALLOW_NO_REPORT);
    }
    return served;
  }

  /**
   * Get with an explicit {@link ReadPolicy}. The TTL suppliers are evaluated
   * lazily — at most once per call, and only when an entry is created, promoted,
   * or a soft-expire refresh is scheduled; never on a plain NORMAL-entry hit
   * (the {@link ReadPolicy} contract), so SpEL-based TTL expressions cost
   * nothing on the hit path.
   * <p>
   * A valid {@code NullValue} sentinel is served as an empty hit (penetration
   * protection — the reader is <b>not</b> re-invoked until the sentinel's
   * short TTL expires) and the access is still counted for hot-key detection.
   * When {@link ReadPolicy#nullCaching()} is {@code false}, a {@code null}
   * reader result leaves no cache entry.
   * <p>
   * {@link StalePolicy#REVALIDATE} is honored on the hit path exactly as in
   * {@link #getWithSoftExpire(String, ReadPolicy)}: a soft-expired entry is
   * dropped-and-reloaded (non-worker-managed) or kept-and-reloaded with the
   * soft expiry renewed (worker-managed, see {@link #loadCacheEntry}).
   * {@link StalePolicy#RETURN} serves the cached value without refresh
   * handling; {@link StalePolicy#SOFT_REFRESH} serves it and arms a
   * background refresh via {@link #refreshSoftExpire} (no-op when the policy
   * carries no reader).
   *
   * @param cacheKey the key to retrieve
   * @param policy   the resolved per-invocation cache policy (carries reader,
   *                 TTLs, null-caching decision, and report-allow flag)
   * @param <T>      the value type
   * @return an {@link Optional} containing the cached or loaded value
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  @SuppressWarnings("all")
  public <T> Optional<T> get(String cacheKey, ReadPolicy policy) {
    return readThrough(cacheKey, policy);
  }

  /**
   * Shared body of the two policy-based single-key read paths
   * ({@link #get(String, ReadPolicy)} and
   * {@link #getWithSoftExpire(String, ReadPolicy)}): identical guard, hit,
   * and load orchestration. The policy's {@link StalePolicy} governs a
   * soft-expired hit directly — {@code SOFT_REFRESH} arms a background
   * refresh, {@code REVALIDATE} blocks on a reload, {@code RETURN} serves
   * as-is — so the two entry points differ only in which policy the caller
   * built, not in engine behavior.
   *
   * <p>
   * The TTL suppliers are evaluated lazily — at most once per call, and only
   * when an entry is created, promoted, or a soft-expire refresh is scheduled;
   * never on a plain NORMAL-entry hit (the {@link ReadPolicy} contract), so
   * SpEL-based TTL expressions cost nothing on the hit path.
   *
   * @param cacheKey       the key to retrieve
   * @param policy         the resolved per-invocation cache policy (carries
   *                       reader, TTLs, null-caching, stale policy, and
   *                       report-allow flag)
   * @param <T>            the value type
   * @return an {@link Optional} containing the cached (possibly stale) or
   *         loaded value
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  @SuppressWarnings("all")
  private <T> Optional<T> readThrough(String cacheKey, ReadPolicy policy) {
    Objects.requireNonNull(policy, "policy must not be null");
    String nk = normalize(cacheKey);
    RuleAction action = preGuard(nk);
    if (action == null) return Optional.empty();

    boolean skipReport = isSkipReport(action == RuleAction.ALLOW_NO_REPORT, policy.reportEnabled());
    ReadContext rp = ReadContext.of(policy, skipReport);

    // The cache read lives inside the lambda, like peek(): the freshness
    // decisions are made on the freshest possible snapshot instead of a value
    // captured before execute().
    return execute(nk, policy.failOnError(), () -> {
      CacheEntry raw = caffeineCache.getIfPresent(nk);
      if (raw != null) {
        if (!entryLifecycle.invalidateIfIsLogicallyExpired(nk, raw)) {
          // A REVALIDATE soft-expiry blocks on a reload (reader invoked), so it
          // counts as a miss; every other served hit counts as a hit.
          if (rp.stalePolicy() == StalePolicy.REVALIDATE && entryLifecycle.ttlPolicy().isSoftExpired(raw)) {
            requestMisses.increment();
          } else {
            requestHits.increment();
          }
          return serveEntryHit(nk, raw, rp);
        }
      }
      requestMisses.increment();
      return loadAndCache(nk, rp);
    });
  }

  /**
   * Serve a single-key {@link CacheEntry} hit — the shared hit shape of
   * {@link #readThrough}: REVALIDATE, sentinel serving, optional SOFT_REFRESH
   * arming, then {@link #process} for state reconciliation and reporting.
   *
   * <p>
   * Returns an {@link Optional} instead of the batch paths' {@link HitOutcome}
   * record: the single-key caller consumes the outcome immediately, so the
   * record would be pure allocation churn on the hottest path of the library.
   *
   * @param nk             the normalized cache key
   * @param ce             the {@link CacheEntry} value from Caffeine (never {@code null} here)
   * @param rp             the per-call read context
   * @param <T>            the value type
   * @return the served value, or empty for a {@link NullValue} sentinel hit
   */
  private <T> Optional<T> serveEntryHit(String nk, CacheEntry ce, ReadContext rp) {
    // REVALIDATE runs before the unwrap: a soft-expired entry gets the
    // drop-and-reload (non-worker-managed) / keep-and-renew (worker-managed)
    // handling without paying the decompression first. NullValue sentinels
    // never reach the branch — they carry softExpireAtMs = 0, which the
    // disabled-soft-TTL contract of isSoftExpired reports as never stale, so
    // their short-hard-TTL penetration protection is untouched.
    if (rp.stalePolicy() == StalePolicy.REVALIDATE && entryLifecycle.ttlPolicy().isSoftExpired(ce)) {
      return revalidateSoftExpired(nk, ce, rp);
    }

    T cached;
    if (ce.getValue() == NullValue.INSTANCE) {
      // NullValue sentinel: served as an empty hit without re-invoking the
      // reader until the sentinel's short hard TTL expires (penetration
      // protection). The access is still counted for hot-key detection.
      dispatcher.recordAccess(nk, rp.skipReport());
      return Optional.empty();
    }
    cached = unwrapRawValue(ce, nk);
    if (cached == null) {
      // Decompression corruption (unwrap already invalidated the entry):
      // never conflate with a sentinel hit — no hot-key counting, reload
      // through the miss path so the source repopulates the entry.
      requestHits.decrement();
      requestMisses.increment();
      return loadAndCache(nk, rp);
    }
    if (rp.stalePolicy() == StalePolicy.SOFT_REFRESH) {
      refreshSoftExpire(nk, ce, rp);
    }
    return process(nk, ce, cached, rp);
  }

  /**
   * Batch variant of {@link #get(String, ReadPolicy)}. All keys share
   * the same explicit TTL overrides.
   *
   * @param cacheKeys the keys to retrieve
   * @param reader    the value function for cache misses
   * @param hardTtlMs hard TTL override (0 = use configured default;
   *                  {@link Long#MAX_VALUE} for permanent entry)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param nullCaching whether a {@code null} reader result leaves a
   *                  short-TTL sentinel ({@code true}) or no entry
   * @param stalePolicy what a soft-expired hit does (SOFT_REFRESH arms a
   *                  background refresh; REVALIDATE blocks on a reload)
   * @param <T>       the value type
   *                  <p>
   *                  Keys are normalized via {@link #normalize(String)} before
   *                  rule matching
   *                  and L1 access, matching the single-key paths; the result map
   *                  keeps the
   *                  caller's original keys and the reader receives them
   *                  unchanged.
   *                  <p>
   *                  A valid {@code NullValue} sentinel hit is served as an empty
   *                  value for
   *                  that key <b>without</b> invoking the reader — the same
   *                  cache-penetration
   *                  protection as the single-key path.
   *
   * @return a map of key → loaded or cached value (never {@code null})
   * @throws ZetaBlockedException when any key matches a blacklist rule
   */
  public <T> Map<String, Optional<T>> get(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader,
    long hardTtlMs,
    long softTtlMs,
    boolean skipWorkerReport,
    boolean failOnError,
    boolean nullCaching,
    StalePolicy stalePolicy
  ) {
    return batchGet(cacheKeys, reader, hardTtlMs, softTtlMs, skipWorkerReport, failOnError, nullCaching, stalePolicy);
  }

  /**
   * Shared body of the two batch read paths ({@code get(Iterable, ...)} and
   * {@code getWithSoftExpire(Iterable, ...)}): per-key guard, hit check, and
   * miss reload are identical. The per-batch {@link StalePolicy} governs a
   * soft-expired hit exactly as on the single-key path (SOFT_REFRESH arms a
   * background refresh, REVALIDATE blocks on a reload, RETURN serves as-is).
   *
   * <p>
   * Keys are normalized before rule matching and L1 access, matching the
   * single-key paths; the result map keeps the caller's original keys and the
   * reader receives them unchanged. A valid {@code NullValue} sentinel hit is
   * served as an empty value for that key without invoking the reader — the
   * same cache-penetration protection as the single-key path.
   *
   * @param cacheKeys      the keys to retrieve
   * @param reader         the value function for cache misses (and hit-phase
   *                       refreshes)
   * @param hardTtlMs      hard TTL override (0 = use configured default;
   *                       {@link Long#MAX_VALUE} for permanent entry)
   * @param softTtlMs      soft TTL override (0 = use configured default)
   * @param reportToWorker whether to report accesses to the Worker
   * @param failOnError    whether the first reader failure propagates instead
   *                       of being swallowed as a miss (see
   *                       {@link SingleFlight#load(Iterable, Function, boolean)})
   * @param nullCaching    whether a {@code null} reader result leaves a
   *                       short-TTL sentinel ({@code true}) or no entry
   * @param stalePolicy    what a soft-expired hit does
   * @param <T>            the value type
   * @return a map of key → loaded or cached value (never {@code null})
   */
  @SuppressWarnings("all")
  private <T> Map<String, Optional<T>> batchGet(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader,
    long hardTtlMs,
    long softTtlMs,
    boolean reportToWorker,
    boolean failOnError,
    boolean nullCaching,
    StalePolicy stalePolicy
  ) {
    List<String> misses = new ArrayList<>();
    List<String> missesNormalized = new ArrayList<>();
    Map<String, Optional<T>> results = new LinkedHashMap<>();
    Map<String, Boolean> skipReports = new HashMap<>();

    for (String key : cacheKeys) {
      String nk = normalize(key);
      RuleAction action = preGuard(nk);
      if (action == null) {
        results.put(key, Optional.empty());
        continue;
      }
      boolean skipReport = isSkipReport(action == RuleAction.ALLOW_NO_REPORT, reportToWorker);
      skipReports.put(nk, skipReport);

      HitOutcome<T> hit = executeHit(nk, failOnError, () -> {
        CacheEntry raw = caffeineCache.getIfPresent(nk);
        return raw == null
          ? HitOutcome.<T>miss()
          : entryHitOutcome(
              nk,
              raw,
              ReadContext.of(() -> reader.apply(key), hardTtlMs, softTtlMs, skipReport, nullCaching, stalePolicy)
            );
      });
      if (hit.value().isPresent()) {
        requestHits.increment();
        results.put(key, hit.value());
      } else if (hit.sentinelHit()) {
        // NullValue sentinel hit: served empty without a reload (penetration
        // protection) — a hit at request level.
        requestHits.increment();
        results.put(key, Optional.empty());
      } else {
        requestMisses.increment();
        // Misses keep the original key: the reader keeps its single-key parity
        // contract, while loadAndCache uses the pre-computed normalized form
        // for cache identity.
        misses.add(key);
        missesNormalized.add(nk);
      }
    }
    if (misses.isEmpty()) return results;

    results.putAll(
      loadAndCache(misses, missesNormalized, reader, hardTtlMs, softTtlMs, skipReports, failOnError, nullCaching)
    );
    return results;
  }

  /**
   * Tri-state check of a batch cache hit: verify it is not logically expired,
   * unwrap the stored value, serve a {@link NullValue} sentinel as an empty
   * hit (the access is still counted for hot-key detection), optionally arm a
   * background refresh under SOFT_REFRESH, then converge on {@link #process}
   * for state reconciliation and reporting. A logically expired entry resolves
   * to a miss outcome (the batch loop reloads it); a {@link NullValue} sentinel
   * resolves to a sentinel outcome (served as empty without a reload).
   * <p>
   * Batch-only helper: the TTL overrides are already-resolved constants, so a
   * {@link ReadContext} is assembled per key without laziness. The single-key
   * read paths inline the same shape in {@link #serveEntryHit} — their caller
   * consumes the outcome immediately, so the record would be pure allocation
   * churn on the hottest path of the library.
   *
   * @param cacheKey       the cache key
   * @param raw            the L1 entry (never {@code null} here)
   * @param rp             the per-key read context
   * @param <T>            the value type
   * @return the hit outcome — a miss for an expired entry, a null-sentinel
   *         outcome for a sentinel hit, or the {@link #process} result for a
   *         regular hit
   */
  private <T> HitOutcome<T> entryHitOutcome(String cacheKey, CacheEntry raw, ReadContext rp) {
    if (entryLifecycle.invalidateIfIsLogicallyExpired(cacheKey, raw)) {
      return HitOutcome.miss();
    }
    // REVALIDATE runs before the unwrap, mirroring serveEntryHit: a
    // soft-expired entry blocks on a reload (drop-and-reload for plain
    // entries, keep-and-renew for worker-managed ones — both inside
    // revalidateSoftExpired). Sentinels never reach the branch (softExpireAt
    // = 0 = never stale). The reloaded value wraps straight into an outcome:
    // empty collapses to a miss via HitOutcome.of.
    if (rp.stalePolicy() == StalePolicy.REVALIDATE && entryLifecycle.ttlPolicy().isSoftExpired(raw)) {
      return HitOutcome.of(revalidateSoftExpired(cacheKey, raw, rp));
    }
    if (raw.getValue() == NullValue.INSTANCE) {
      // NullValue sentinel: serve as an empty hit without a reload — sentinels
      // carry softExpireAt = 0 (disabled soft TTL) and must not be
      // dropped-and-reloaded on every access (penetration protection).
      dispatcher.recordAccess(cacheKey, rp.skipReport());
      return HitOutcome.nullSentinel();
    }
    T cached = unwrapRawValue(raw, cacheKey);
    if (cached == null) {
      // Decompression corruption (unwrap already invalidated the entry):
      // a miss, not a sentinel hit — no counting, the batch loop reloads.
      return HitOutcome.miss();
    }
    if (rp.stalePolicy() == StalePolicy.SOFT_REFRESH) {
      refreshSoftExpire(cacheKey, raw, rp);
    }
    return HitOutcome.of(process(cacheKey, raw, cached, rp));
  }

  /**
   * Tri-state outcome of a batch cache-hit check: the served value plus whether
   * the hit was a {@link NullValue} sentinel. The distinction is load-bearing
   * for the batch loops — a sentinel is an empty <b>hit</b> (served as empty
   * without re-invoking the reader, cache-penetration protection), while a miss
   * (or a logically expired entry, or a TOCTOU-expired post-process entry)
   * goes to the reload path.
   *
   * @param value       the served value; empty for both a miss and a sentinel hit
   * @param sentinelHit {@code true} when the hit resolved to a {@link NullValue}
   *                    sentinel (empty value, no reload)
   */
  private record HitOutcome<T>(Optional<T> value, boolean sentinelHit) {
    /** A true miss (key absent, entry expired, or TOCTOU-expired): reload. */
    static <T> HitOutcome<T> miss() {
      return new HitOutcome<>(Optional.empty(), false);
    }

    /** A {@link NullValue} sentinel hit: serve empty, do not reload. */
    static <T> HitOutcome<T> nullSentinel() {
      return new HitOutcome<>(Optional.empty(), true);
    }

    /**
     * A regular hit carrying a value (possibly empty via {@code process}'s TOCTOU
     * check → miss).
     */
    @SuppressWarnings("all")
    static <T> HitOutcome<T> of(Optional<T> value) {
      return value.isPresent() ? new HitOutcome<>(value, false) : miss();
    }
  }

  /**
   * Get with soft-expire and an explicit {@link ReadPolicy}. The TTL suppliers
   * are evaluated lazily — at most once per call, and only when an entry is
   * created, promoted, or a soft-expire refresh is scheduled (the
   * {@link ReadPolicy} contract); never on a plain hit.
   * <p>
   * Serves valid {@link NullValue} sentinels as empty hits without reloading,
   * exactly like {@link #get(String, ReadPolicy)}.
   * <p>
   * {@link StalePolicy#REVALIDATE} on a soft-expired entry drops-and-reloads
   * non-worker-managed entries; a worker-managed entry is <b>kept</b> (the
   * reload's storage write is refused) and its soft expiry is renewed by
   * {@link #loadCacheEntry}, so the loader is not re-invoked at read rate for
   * COOL entries the Worker never rebroadcasts. The hard TTL stays the
   * absolute bound (ADR-0034).
   *
   * @param cacheKey the key to retrieve
   * @param policy   the resolved per-invocation cache policy (carries reader,
   *                 TTLs, null-caching, stale-policy, and report-allow flag)
   * @param <T>      the value type
   * @return an {@link Optional} containing the cached (possibly stale) or loaded
   *         value
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  @SuppressWarnings("all")
  public <T> Optional<T> getWithSoftExpire(String cacheKey, ReadPolicy policy) {
    return readThrough(cacheKey, policy);
  }

  /**
   * Batch variant of {@link #getWithSoftExpire(String, ReadPolicy)}.
   * All keys share the same explicit TTL overrides.
   *
   * @param cacheKeys the keys to retrieve
   * @param reader    the value function for cache misses / refreshes
   * @param hardTtlMs hard TTL override (0 = use configured default;
   *                  {@link Long#MAX_VALUE} for permanent entry)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param nullCaching whether a {@code null} reader result leaves a
   *                  short-TTL sentinel ({@code true}) or no entry
   * @param stalePolicy what a soft-expired hit does (SOFT_REFRESH arms a
   *                  background refresh; REVALIDATE blocks on a reload)
   * @param <T>       the value type
   *                  <p>
   *                  Keys are normalized via {@link #normalize(String)} before
   *                  rule matching
   *                  and L1 access, matching the single-key paths; the result map
   *                  keeps the
   *                  caller's original keys and the reader receives them
   *                  unchanged.
   *                  <p>
   *                  A valid {@code NullValue} sentinel hit is served as an empty
   *                  value for
   *                  that key <b>without</b> invoking the reader — the same
   *                  cache-penetration
   *                  protection as the single-key path.
   *
   * @return a map of key → cached (possibly stale) or loaded value
   * @throws ZetaBlockedException when any key matches a blacklist rule
   */
  @SuppressWarnings("all")
  public <T> Map<String, Optional<T>> getWithSoftExpire(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader,
    long hardTtlMs,
    long softTtlMs,
    boolean reportToWorker,
    boolean failOnError,
    boolean nullCaching,
    StalePolicy stalePolicy
  ) {
    return batchGet(cacheKeys, reader, hardTtlMs, softTtlMs, reportToWorker, failOnError, nullCaching, stalePolicy);
  }

  /**
   * REVALIDATE handling for a soft-expired {@link CacheEntry} hit, shared by
   * {@link #get(String, ReadPolicy)} and
   * {@link #getWithSoftExpire(String, ReadPolicy)}:
   * a non-worker-managed entry is dropped and the loader re-invoked; a
   * worker-managed entry is kept (its reload storage is refused and its soft
   * expiry renewed — see {@link #loadCacheEntry}) while the fresh load result
   * still serves the caller.
   *
   * @param cacheKey the cache key
   * @param raw      the soft-expired raw value from Caffeine
   * @param rp       the per-call read context (reader, TTLs, skip-report)
   * @param <T>      the value type
   * @return the reloaded value
   */
  private <T> Optional<T> revalidateSoftExpired(String cacheKey, CacheEntry raw, ReadContext rp) {
    if (!isWorkerManaged(raw)) {
      // Conditional removal: drop the entry only if it is still the
      // exact instance examined — a concurrent refresh/broadcast may
      // have replaced it with a fresh entry that must survive.
      AtomicBoolean removed = new AtomicBoolean();
      caffeineCache
        .asMap()
        .computeIfPresent(cacheKey, (k, v) -> {
          if (v == raw) {
            removed.set(true);
            return null;
          }
          return v;
        });
      if (removed.get()) {
        // The drop-and-reload contract re-invokes the loader: drop the dedup
        // entry too (ADR-0067), or a completed SingleFlight result from before
        // the drop would replay the stale value instead of the reload.
        singleFlight.invalidate(cacheKey);
      }
    }
    return loadAndCache(cacheKey, rp);
  }

  /**
   * Trigger a background refresh if the raw value is a soft-expired
   * {@link CacheEntry}. HOT and COOL entries are refreshed — their value must
   * stay fresh while the Worker has not re-decided; a successful refresh
   * downgrades COOL to NORMAL (see {@code DefaultBackgroundRefresher#applyRefreshTask}).
   * NORMAL entries are refreshed too, mirroring {@code computeInLock}'s
   * SOFT_REFRESH branch: the refresh re-arms whenever the entry is soft-expired
   * (stale-while-revalidate semantics of {@code zeta.local.default-soft-ttl-ms})
   * and keeps the entry inside its hard-TTL bound (ADR-0034). This is what makes
   * {@code Zeta.registerRefresh} hold its documented "every cycle triggers an
   * async refresh" contract for normal keys.
   *
   * @param cacheKey the cache key
   * @param raw      the L1 entry (never {@code null} here)
   * @param rp       the per-call read context (reader {@code null} skips the
   *                 refresh — convenience policies carry no reader; the soft
   *                 TTL override is consumed lazily through the memoized
   *                 supplier)
   */
  private void refreshSoftExpire(String cacheKey, CacheEntry raw, ReadContext rp) {
    if (rp.reader() == null || !entryLifecycle.ttlPolicy().isSoftExpired(raw)) {
      return;
    }

    long effectiveSoft = computeSoftTtlForRefresh(rp.softTtlMs(), raw);
    // ADR-0033: wrap the reader so the refresh task probes dataVersion after
    // the value read and stamps the result; probe failures stay fail-open.
    backgroundRefresher.triggerBackgroundRefresh(
      cacheKey,
      () -> probeVersioned(cacheKey, rp.reader().get()),
      effectiveSoft
    );
  }

  /**
   * Compute the effective soft TTL to use for a background refresh.
   * <p>
   * Priority: per-call override > per-key-state hot default > per-entry normal
   * TTL > global default.
   *
   * @param softTtlMs per-call soft TTL override (0 = use configured default)
   * @param ce        the cache entry to derive key-state and normal-soft defaults
   *                  from
   * @return the effective soft TTL in milliseconds
   */
  @SuppressWarnings("java:S3358")
  private long computeSoftTtlForRefresh(long softTtlMs, CacheEntry ce) {
    return softTtlMs > 0
      ? softTtlMs
      : (ce.getKeyState() == KeyState.HOT
          ? entryLifecycle.ttlPolicy().getEffectiveHotSoftTtlMs()
          : (ce.getNormalSoftTtlMs() > 0
              ? ce.getNormalSoftTtlMs()
              : entryLifecycle.ttlPolicy().getEffectiveSoftTtlMs()));
  }

  /**
   * Process a cache hit: reconcile the entry state (Decision-Validity demotion
   * and local hot-key promotion), reportToWorker, then re-check logical expiry
   * (TOCTOU guard) — but only after the side effects that can rewrite the L1
   * entry (demotion, promotion). Reporting is a buffered local append that
   * never touches L1, so a plain hit pays no second Caffeine lookup; the
   * microsecond expiry race a re-read after reporting used to cover is
   * tolerated per ADR-0013 (Acceptable Race Fading) and re-checked by the
   * next read's entry-point expiry check anyway.
   * <p>
   * Extracted from {@link #get} and {@link #getWithSoftExpire} to eliminate
   * code duplication — every hit-serving path converges here, so the
   * state-reconciliation step runs exactly once per hit.
   *
   * @param cacheKey the cache key
   * @param raw      the L1 entry (never {@code null} here)
   * @param cached   the unwrapped cached value
   * @param rp       the per-call read context (TTL overrides are consumed
   *                 lazily by the promotion probe, never on a plain hit)
   * @param <T>      the value type
   * @return an {@link Optional} containing the cached value, or empty if
   *         logically expired
   */
  private <T> Optional<T> process(String cacheKey, CacheEntry raw, T cached, ReadContext rp) {
    dispatcher.recordAccess(cacheKey, rp.skipReport());

    // Entry-state corrections within one read: demotion (a dead Worker's HOT
    // stamp is reverted to NORMAL in place) and local promotion are mutually
    // exclusive here — promotion judges the pre-demotion snapshot, so an
    // orphaned entry stays NORMAL for one read and the local TopK re-promotes
    // it on the next read if still hot (ADR-0035). Both are still evaluated:
    // a demotion never short-circuits the promotion probe.
    boolean demoted = entryLifecycle.demoteIfDecisionInvalid(cacheKey, raw);
    boolean promoted = promotion.promoteIfNeeded(cacheKey, raw, rp::hardTtlMs, rp::softTtlMs);

    // TOCTOU: re-read from cache only after the state-rewriting side effects
    // (demotion/promotion replaced the entry in place; verify the replacement
    // did not land past its own expiry).
    if (demoted || promoted) {
      CacheEntry currentRaw = caffeineCache.getIfPresent(cacheKey);
      if (entryLifecycle.invalidateIfIsLogicallyExpired(cacheKey, currentRaw)) {
        return Optional.empty();
      }
    }

    return Optional.ofNullable(cached);
  }

  /**
   * Wrap a loaded value with the {@code dataVersion} probed from Redis
   * <b>after</b> the value read (ADR-0033 — Read-Path Version Stamping).
   *
   * <p>
   * The probe-after-read ordering is monotone-safe: {@code putThrough}
   * runs the user writer before the version INCR, so the stamped version is
   * never lower than the value's true version (reject-more direction).
   *
   * <p>
   * Fail-open: a failed probe withholds the stamp ({@code stamped=false})
   * and callers fall back to the legacy version handling — an entry is never
   * demoted to degraded by a failed probe. The default implementation already
   * swallows Redis errors; the outer guard covers custom
   * {@link VersionController}
   * implementations that may throw.
   *
   * @param cacheKey the cache key the value was loaded for
   * @param value    the loaded value (may be {@code null})
   * @return the value paired with the probed version
   */
  private VersionedValue probeVersioned(String cacheKey, Object value) {
    try {
      // Reuses toVersionedValue for the probe→stamp mapping: a present probe
      // stamps the entry, a missing probe is fail-open (unstamped).
      return toVersionedValue(value, versionController.currentVersion(cacheKey));
    } catch (RuntimeException e) {
      // Rate-limited: under a Redis outage this fires per miss and would
      // otherwise flood the log at miss rate (the rest of the codebase uses
      // the same 10s single-WARN convention, ADR-0037).
      if (readErrorLogThrottle.tryAcquire()) {
        log.warn(
          "Version probe failed for key={}, stamp withheld (fail-open, further failures suppressed for 10s)",
          cacheKey,
          e
        );
      } else {
        log.debug("Version probe failed for key={}, stamp withheld (fail-open)", cacheKey, e);
      }
      return new VersionedValue(value, VERSION_DEFAULT, false);
    }
  }

  /**
   * Pair a probed version (from {@code currentVersions}) with a loaded value.
   *
   * <p>
   * Fail-open: a missing or failed probe produces an unstamped
   * {@link VersionedValue}.
   *
   * @param value the loaded value (may be {@code null})
   * @param probe the per-key probe result
   * @return the value paired with the probed version, or unstamped
   */
  @SuppressWarnings("all")
  private static VersionedValue toVersionedValue(Object value, Optional<Long> probe) {
    return probe
      .map(v -> new VersionedValue(value, v, true))
      .orElseGet(() -> new VersionedValue(value, VERSION_DEFAULT, false));
  }

  /**
   * Load via SingleFlight, detect hot key, cache with HOT or NORMAL TTL, and
   * return value.
   * <p>
   * The pre-load {@code afterGuard} was removed: every caller runs
   * {@link #preGuard} immediately before this method with no user code in
   * between, and the meaningful post-reader BLOCK re-check lives in
   * {@link #processLoaded} (single source for every load path).
   *
   * @param cacheKey the key to load
   * @param rp       the per-call read context (reader, null-caching flag and
   *                 TTL overrides; the TTL suppliers are consumed exactly once
   *                 per call through the memoized {@link ReadPolicy})
   */
  private <T> Optional<T> loadAndCache(String cacheKey, ReadContext rp) {
    @SuppressWarnings("all")
    Supplier<T> reader = (Supplier<T>) rp.reader();
    if (reader == null) {
      // S2: no data source configured (e.g. a ReadPolicy without reader on a
      // miss) — fail with an actionable error instead of an NPE inside the
      // SingleFlight execution. Stale-first still applies when the breaker is
      // open; otherwise the miss resolves empty with no sentinel write.
      return resolveNoAnswerOutcome(cacheKey);
    }
    // ADR-0033: the composite supplier runs inside the SingleFlight execution
    // scope, so concurrent waiters share one (value, probedVersion) pair —
    // the version probe is coalesced exactly like the value load.
    LoaderOutcome<VersionedValue> outcome = singleFlight.loadOutcome(cacheKey, () ->
      probeVersioned(cacheKey, reader.get())
    );

    switch (outcome.status()) {
      case LOADED -> {
        VersionedValue vv = outcome.value();
        if (vv != null && vv.value() == null) {
          return resolveNullLoadOutcome(cacheKey, rp.nullCaching(), vv);
        }
        @SuppressWarnings("all")
        T value = (T) vv.value();
        return Optional.of(
          // A real load always consumes the TTL overrides (an entry is built), so
          // resolving them here costs nothing: the memoized read context evaluates
          // each at most once per call by construction.
          processLoaded(cacheKey, value, vv, rp.hardTtlMs(), rp.softTtlMs(), rp.skipReport())
        );
      }
      case SOURCE_EMPTY -> {
        return resolveNullLoadOutcome(cacheKey, rp.nullCaching());
      }
      case FAILED -> {
        // The outer execute() owns failOnError here: report every
        // closed-breaker failure and let it adjudicate (swallow resolves
        // empty with no sentinel write); a breaker-open failure degrades via
        // the stale-first no-answer fallback below (never a sentinel: the
        // source never answered null, so there is nothing to protect).
        throwIfFailFast(cacheKey, LoadStatus.FAILED, outcome.cause(), true);
        return resolveNoAnswerOutcome(cacheKey);
      }
      case SUPPRESSED_BREAKER, SUPPRESSED_TIMEOUT, SUPPRESSED_EXECUTOR -> {
        throwIfFailFast(cacheKey, outcome.status(), null, rp.failOnError());
        logSuppressedLoad(cacheKey, outcome.status());
        return resolveNoAnswerOutcome(cacheKey);
      }
      // No silent default: a future LoadStatus must fail loudly here rather
      // than resolve as a miss (exhaustiveness is load-bearing for fail-fast).
      default -> throw new AssertionError("unhandled LoadStatus: " + outcome.status());
    }
  }

  /**
   * Batch variant of {@link #loadAndCache(String, ReadPolicy)}.
   * Submits all keys to SingleFlight in a single pass, then processes each loaded
   * value through {@link #processLoaded} and calls {@link #resolveNullLoadOutcome} for absent
   * results.
   *
   * @param cacheKeys      the keys to load (typically the misses from a preceding
   *                       Caffeine check); originals, kept for reader parity and
   *                       result-map identity
   * @param normalizedKeys the cache-identity form of each key, index-aligned with
   *                       {@code cacheKeys} (pre-computed by the caller's guard
   *                       loop, so this pass does not re-normalize)
   * @param reader         the value function for cache misses (called on executor
   *                       threads)
   * @param hardTtlMs      hard TTL override (0 = use configured default)
   * @param softTtlMs      soft TTL override (0 = use configured default)
   * @param skipReports    map of key → whether to skip Worker reporting
   * @param failOnError    whether the first reader failure propagates instead of
   *                       being swallowed as a miss (see
   *                       {@link SingleFlight#load(Iterable, Function, boolean)})
   * @param nullCaching    whether a {@code null} reader result leaves a
   *                       short-TTL sentinel ({@code true}) or no entry
   * @param <T>            the value type
   * @return a map of key → loaded or empty result, preserving input order
   */
  private <T> Map<String, Optional<T>> loadAndCache(
    List<String> cacheKeys,
    List<String> normalizedKeys,
    Function<? super String, ? extends T> reader,
    long hardTtlMs,
    long softTtlMs,
    Map<String, Boolean> skipReports,
    boolean failOnError,
    boolean nullCaching
  ) {
    // Explicit outcomes per key (never throws): the loop below applies the
    // fail-fast policy itself, stale-first, so transient suppression and real
    // failures stay distinguishable per key.
    Map<String, LoaderOutcome<T>> loaded = singleFlight.loadOutcome(cacheKeys, reader);
    // Cache identity (guard, L1, report) uses the normalized key while the
    // reader keeps the caller's original key (single-key parity).
    // ADR-0033: one pipelined round trip for all version keys; per-key
    // fail-open (a failed probe leaves that key unstamped).
    Map<String, Optional<Long>> probes = versionController.currentVersions(normalizedKeys);
    Map<String, Optional<T>> results = new LinkedHashMap<>();

    for (int i = 0; i < cacheKeys.size(); i++) {
      String key = cacheKeys.get(i);
      String nk = normalizedKeys.get(i);
      results.put(
        key,
        execute(nk, failOnError, () -> {
          LoaderOutcome<T> outcome = loaded.get(key);
          Optional<Long> probe = probes.getOrDefault(nk, Optional.empty());
          switch (outcome.status()) {
            case LOADED -> {
              T t = outcome.value();
              return Optional.ofNullable(
                processLoaded(nk, t, toVersionedValue(t, probe), hardTtlMs, softTtlMs, skipReports.get(nk))
              );
            }
            case SOURCE_EMPTY -> {
              return resolveNullLoadOutcome(nk, nullCaching, toVersionedValue(null, probe));
            }
            case FAILED -> {
              // The per-key execute() owns failOnError here: report every
              // closed-breaker failure and let it adjudicate (swallow resolves
              // empty with no sentinel write).
              throwIfFailFast(nk, LoadStatus.FAILED, outcome.cause(), true);
              return resolveNoAnswerOutcome(nk);
            }
            case SUPPRESSED_BREAKER, SUPPRESSED_TIMEOUT, SUPPRESSED_EXECUTOR -> {
              throwIfFailFast(nk, outcome.status(), null, failOnError);
              logSuppressedLoad(nk, outcome.status());
              return resolveNoAnswerOutcome(nk);
            }
            // No silent default: a future LoadStatus must fail loudly here rather
            // than resolve as a miss (exhaustiveness is load-bearing for fail-fast).
            default -> throw new AssertionError("unhandled LoadStatus: " + outcome.status());
          }
        })
      );
    }
    return results;
  }

  /**
   * Handle the case when SingleFlight returns empty: either return a stale
   * entry when the circuit breaker is open, or cache a {@link NullValue}
   * sentinel with a short TTL and return {@link Optional#empty()}.
   *
   * <p>
   * Only entries whose hard TTL has <b>not</b> expired may serve as the
   * stale fallback — the hard TTL contract (see CONTEXT.md "Expire") is never
   * bypassed, even during a source outage. A hard-expired entry returns empty
   * without writing a {@link NullValue} sentinel: the source never answered
   * null, and the stale entry is evicted by the next read's L1 check.
   *
   * @param cacheKey the key that was loaded (resulted in a null value)
   * @param <T>      the expected value type
   * @return a stale value if the circuit breaker is open and a cached entry
   *         exists within its hard TTL; {@link Optional#empty()} otherwise (a
   *         NullValue sentinel is written to L1 unless a hard-expired entry
   *         was present)
   */
  @SuppressWarnings("all")
  private <T> Optional<T> resolveNullLoadOutcome(String cacheKey, boolean nullCaching) {
    return resolveNullLoadOutcome(cacheKey, nullCaching, new VersionedValue(null, VERSION_DEFAULT, false));
  }

  /**
   * Handle the case when SingleFlight returns empty or a null value: either
   * return a stale entry when the circuit breaker is open, or cache a
   * {@link NullValue} sentinel with a short TTL and return
   * {@link Optional#empty()}.
   *
   * <p>
   * ADR-0033: a {@code NullValue} sentinel is stamped with the probed
   * version like any other load, so a late stale broadcast cannot overwrite
   * a "source returned null" marker; an unstamped sentinel keeps the legacy
   * version handling (fail-open).
   *
   * @param cacheKey    the key that was loaded (resulted in a null value)
   * @param nullCaching whether a null result should be cached as a sentinel
   * @param vv          the loaded value carrier (value is {@code null} here) with
   *                    the probed version, if any
   * @param <T>         the expected value type
   * @return a stale value if the circuit breaker is open and a cached entry
   *         exists within its hard TTL; {@link Optional#empty()} otherwise (a
   *         NullValue sentinel is written to L1 unless a hard-expired entry
   *         was present)
   */
  @SuppressWarnings("all")
  private <T> Optional<T> resolveNullLoadOutcome(String cacheKey, boolean nullCaching, VersionedValue vv) {
    if (singleFlight.isBreakerOpen(cacheKey)) {
      CacheEntry stale = caffeineCache.getIfPresent(cacheKey);

      if (stale != null) {
        // Hard-expired entries are never served as stale, even during an
        // outage: the hard TTL contract (CONTEXT.md "Expire") is not bypassed.
        if (entryLifecycle.ttlPolicy().isLogicallyExpired(stale)) {
          if (log.isDebugEnabled()) {
            log.debug("CB open, discarding hard-expired entry for key={}", cacheKey);
          }
          return Optional.empty();
        }
        T val = this.<T>unwrapRawValue(stale, cacheKey);
        if (log.isDebugEnabled()) {
          log.debug("CB open, returning stale entry for key={}", cacheKey);
        }
        return Optional.ofNullable(val);
      }
    }
    if (nullCaching) {
      buildNullValueEntry(cacheKey, vv);
    }
    return Optional.empty();
  }

  /**
   * Handle a load that produced no answer (reader failure or SingleFlight
   * suppression): serve a hard-valid stale entry when the circuit breaker is
   * open, otherwise resolve empty — and <b>never</b> write a {@link NullValue}
   * sentinel. A failure or a suppression is not a null answer: the source
   * gave no authority to claim the key absent, so caching "absent" would pin
   * a fabricated miss for the whole null-value TTL. The next read retries
   * the source instead.
   *
   * @param cacheKey the key whose load produced no answer
   * @param <T>      the expected value type
   * @return a stale value if the circuit breaker is open and a cached entry
   *         exists within its hard TTL; {@link Optional#empty()} otherwise
   */
  @SuppressWarnings("all")
  private <T> Optional<T> resolveNoAnswerOutcome(String cacheKey) {
    if (singleFlight.isBreakerOpen(cacheKey)) {
      CacheEntry stale = caffeineCache.getIfPresent(cacheKey);

      if (stale != null) {
        // Hard-expired entries are never served as stale, even during an
        // outage: the hard TTL contract (CONTEXT.md "Expire") is not bypassed.
        if (entryLifecycle.ttlPolicy().isLogicallyExpired(stale)) {
          if (log.isDebugEnabled()) {
            log.debug("CB open, discarding hard-expired entry for key={}", cacheKey);
          }
          return Optional.empty();
        }
        T val = this.<T>unwrapRawValue(stale, cacheKey);
        if (log.isDebugEnabled()) {
          log.debug("CB open, returning stale entry for key={}", cacheKey);
        }
        return Optional.ofNullable(val);
      }
    }
    return Optional.empty();
  }

  /**
   * Rate-limited suppression log for the {@code loadAndCache} paths: a
   * swallowed {@code SUPPRESSED_*} outcome produces no exception, so without
   * this line the self-protection would be invisible (the FAILED path is
   * logged by the outer {@code execute()} instead). WARN, not ERROR — a
   * suppression is the guard working as designed, not a fault.
   *
   * @param cacheKey the key whose load was suppressed
   * @param status   the suppression cause (breaker, timeout, or executor)
   */
  private void logSuppressedLoad(String cacheKey, LoadStatus status) {
    if (readErrorLogThrottle.tryAcquire()) {
      log.warn(
        "HotKeyCache load suppressed for key={} (status={}, reader never ran; returning empty/stale, next read retries; further suppressions hidden for 10s)",
        cacheKey,
        status
      );
    } else if (log.isDebugEnabled()) {
      log.debug("HotKeyCache load suppressed for key={} (status={})", cacheKey, status);
    }
  }

  /**
   * Cache a {@link NullValue} sentinel in L1 with a short TTL so that
   * subsequent reads return empty without hitting the reader.
   * <p>
   * The write is version-guarded against NORMAL entries (ADR-0067): a
   * strictly newer (or equal-version) entry survives the null answer, and an
   * unstamped (fail-open) null load — which carries no version authority at
   * all — never replaces an existing entry. The sentinel therefore lands over
   * a strictly older normal entry, mirroring the REFRESH no-value fallback's
   * posture (ADR-0031/0066). A Worker-managed (HOT/COOL) entry keeps the
   * historical semantics: the source's null answer is authoritative for the
   * VALUE, so the sentinel replaces it, carrying the decision stamp forward
   * (see the stamp rationale below).
   * <p>
   * When the sentinel IS written, the existing entry's decision stamp is
   * carried into it: a null load must not erase a Worker decision (HOT/COOL
   * stamp) ordering anchor — the stamp keeps {@code decisionVersion}
   * comparable so a later Worker broadcast still orders correctly against
   * this entry.
   *
   * @param cacheKey the key to store the null sentinel for
   * @param vv       the load carrier; the sentinel is stamped with the probed
   *                 version when present (ADR-0033), otherwise it stays at
   *                 {@link io.github.hyshmily.zeta.constants.ZetaConstants.Version#VERSION_DEFAULT}
   */
  private void buildNullValueEntry(String cacheKey, VersionedValue vv) {
    long ttlMs = nullTtlMs();
    long nullExpireAtMs = entryLifecycle.ttlPolicy().computeNullExpireAt(ttlMs);
    long dataVersion = vv.stamped() ? vv.dataVersion() : VERSION_DEFAULT;
    caffeineCache
      .asMap()
      .compute(cacheKey, (k, existing) -> {
        if (
          existing != null &&
          !isWorkerManaged(existing) &&
          (!vv.stamped() || VersionGuard.shouldSkipForSync(existing, vv.dataVersion(), false))
        ) {
          // A normal entry at or above the null load's probed version (or any
          // entry against an unstamped null) must survive: the null answer
          // carries no authority over it and overwriting it would regress L1.
          return existing;
        }
        return entryLifecycle
          .newEntry()
          .value(entryLifecycle.wrapValue(NullValue.INSTANCE))
          .version(dataVersion)
          // Carry the Worker decision stamp forward: a null load must not
          // erase a HOT/COOL ordering anchor (see method Javadoc).
          .decision(EntryLifecycle.decisionOf(existing))
          // Null sentinel: hard TTL only — no soft expiry, no normal
          // baseline (a sentinel never reverts to the normal lifecycle).
          .ttl(ttlMs, 0L, 0L, 0L)
          .hardExpiryAt(nullExpireAtMs)
          .keyState(KeyState.NORMAL)
          .build();
      });
  }

  /**
   * Process a successfully loaded value: re-check blacklist, detect hot key via
   * HeavyKeeper, store in L1 with HOT or NORMAL TTL, and optionally
   * reportToWorker to Worker.
   *
   * @param cacheKey   the key to cache
   * @param value      the loaded value (must not be null at this point)
   * @param vv         the load carrier with the probed {@code dataVersion}
   *                   (ADR-0033) for stamping the L1 entry
   * @param hardTtlMs  hard TTL override (0 = use configured default)
   * @param softTtlMs  soft TTL override (0 = use configured default)
   * @param skipReport if {@code true}, skip reporting to Worker
   * @param <T>        the value type
   * @return the loaded value (unchanged)
   */
  private <T> T processLoaded(
    String cacheKey,
    T value,
    VersionedValue vv,
    long hardTtlMs,
    long softTtlMs,
    boolean skipReport
  ) {
    afterGuard(cacheKey);

    long effectiveHard = entryLifecycle.ttlPolicy().resolveEffectiveHardTtl(hardTtlMs);
    long effectiveSoft = entryLifecycle.ttlPolicy().resolveEffectiveSoftTtl(softTtlMs);

    dispatcher.recordAccess(cacheKey, skipReport);
    if (hotKeyDetector.contains(cacheKey)) {
      long hotHard = entryLifecycle.ttlPolicy().resolveEffectiveHotHard(hardTtlMs);
      long hotSoft = entryLifecycle.ttlPolicy().resolveEffectiveHotSoft(softTtlMs);

      loadCacheEntry(cacheKey, value, hotHard, hotSoft, KeyState.HOT, effectiveHard, effectiveSoft, vv);
    } else {
      loadCacheEntry(cacheKey, value, effectiveHard, effectiveSoft, KeyState.NORMAL, effectiveHard, effectiveSoft, vv);
    }
    return value;
  }

  /**
   * Single-value size gate behind {@code zeta.local.cache.max-value-size}:
   * values whose estimated size exceeds the bound are served to the caller
   * but never stored in L1. The estimate comes from the shared
   * {@link DefaultWeigher} (same pricing as the max-weight cache), so the
   * gate and the weigher can never disagree about what "big" means. A
   * non-positive bound disables the gate (the default).
   *
   * @param cacheKey the normalized key (weigher prices key + value)
   * @param value    the user value about to be stored (never {@code null} here)
   * @return {@code true} when the value must bypass L1 storage
   */
  private boolean isOversizedValue(String cacheKey, Object value) {
    long limit = zetaProperties.getMaxValueSize();
    if (limit <= 0 || value == null) {
      return false;
    }
    long estimate = DefaultWeigher.INSTANCE.weigh(cacheKey, value);
    if (estimate > limit) {
      if (log.isDebugEnabled()) {
        log.debug(
          "Value over max-value-size, bypassing L1: key={}, estimatedBytes={}, limitBytes={}",
          cacheKey,
          estimate,
          limit
        );
      }
      return true;
    }
    return false;
  }

  /**
   * Atomically insert or update the entry in L1 via {@code compute}, skipping
   * the update if the existing entry is Worker-managed (HOT or COOL).
   * <p>
   * When the refault admission gate is active (ADR-0079), a fresh insert or the
   * replacement of a logically-expired entry is first measured against L1
   * capacity: a key whose refault distance exceeds it is stored only as a
   * short-TTL solo-flight entry, keeping scan/traversal traffic from evicting
   * the true working set. Fresh resident entries (concurrent-load race) are
   * residency continuation and are never distance-refused; Worker-managed
   * entries are exempt outright.
   * <p>
   * When the storage write is refused for a Worker-managed entry, the kept
   * entry's soft expiry is renewed (see
   * {@link #renewSoftExpiryAfterRefusedLoad}):
   * a REVALIDATE keep-and-reload of a soft-expired COOL entry must not re-invoke
   * the loader at read rate until the entry's next soft window.
   *
   * @param cacheKey      the key to store
   * @param value         the value to cache
   * @param hardTtlMs     the hard TTL for this entry
   * @param softTtlMs     the soft TTL for this entry
   * @param state         the key state to assign (HOT or NORMAL)
   * @param normalHardTtl the normal (non-hot) hard TTL to preserve for demotion
   * @param normalSoftTtl the normal (non-hot) soft TTL to preserve for demotion
   * @param vv            the load carrier; when stamped (ADR-0033) the entry is
   *                      built with the probed {@code dataVersion}; when
   *                      unstamped (fail-open) an existing entry's version
   *                      identity is preserved, else {@link VERSION_DEFAULT}
   */
  @SuppressWarnings("all")
  private void loadCacheEntry(
    String cacheKey,
    Object value,
    long hardTtlMs,
    long softTtlMs,
    KeyState state,
    long normalHardTtl,
    long normalSoftTtl,
    VersionedValue vv
  ) {
    // Size gate (max-value-size): oversized values are served to the caller
    // by the load path but never take L1 residency — skip before compression
    // so the refusal costs one weigh, not a compress + bin-lock cycle.
    if (isOversizedValue(cacheKey, value)) {
      return;
    }
    // Compress before the Caffeine bin lock (write-side ADR-0030 discipline,
    // mirroring DefaultBackgroundRefresher.applyRefreshTask): the price is one wasted
    // compression when the worker-managed guard refuses the store below.
    Object wrappedValue = entryLifecycle.wrapValue(value);
    caffeineCache
      .asMap()
      .compute(cacheKey, (k, existing) -> {
        if (isWorkerManaged(existing)) {
          // Worker-managed entries are never overwritten by a local reload —
          // the Worker's decision stamp and TTLs must survive. A soft-expired
          // kept entry (REVALIDATE keep-and-reload) renews its soft expiry so
          // the loader is not re-invoked at read rate; the hard TTL stays the
          // absolute bound (ADR-0034).
          return renewSoftExpiryAfterRefusedLoad(existing, softTtlMs);
        }
        if (
          vv.stamped() &&
          existing != null &&
          !entryLifecycle.ttlPolicy().isLogicallyExpired(existing) &&
          VersionGuard.shouldSkipForSync(existing, vv.dataVersion(), false)
        ) {
          // A concurrent write/broadcast/refresh already holds a version at
          // least as fresh as this load's probe claim (ADR-0067). The load's
          // reader may predate that write — ADR-0033's probe-after-read can
          // stamp pre-write data with a post-write version — so the store is
          // refused and the newer entry's value and version identity survive.
          return existing;
        }
        // Refault distance admission (ADR-0079, the mm/workingset.c port): a
        // fresh insert or the replacement of a dead (logically expired) entry
        // is an admission decision — "would this key have survived until its
        // next access?" A fresh resident entry (concurrent-load race) is
        // residency continuation and is never distance-refused; Worker-managed
        // entries were exempted above (Worker evidence outranks distance).
        if (refaultAdmission != null && refaultAdmission.gating()) {
          boolean admissionDecision = existing == null || entryLifecycle.ttlPolicy().isLogicallyExpired(existing);
          if (admissionDecision && refaultAdmission.admit(cacheKey) == RefaultAdmission.Decision.REJECT) {
            // Solo-flight: serve this load's value through a short-TTL entry —
            // coalesces rapid/concurrent re-reads (the SingleFlight dedup only
            // covers in-flight loads) without accumulating residency. The
            // entry's EXPIRED removal re-stamps the shadow anchor, so the next
            // refault re-measures distance from this cycle's end: sustained
            // pressure keeps re-rejecting, a stopped flood re-admits.
            long soloTtl = refaultAdmission.rejectTtlMs();
            long version = vv.stamped()
              ? vv.dataVersion()
              : existing != null
                ? existing.getDataVersion()
                : VERSION_DEFAULT;
            // soloFlight=true: the entry's SIZE eviction is the gate's own churn —
            // the clock/shadow keeper re-stamps the anchor without advancing the
            // clock, so solo-flight turnover never pollutes refault distances.
            return newEntry(wrappedValue, soloTtl, 0, KeyState.NORMAL, soloTtl, 0, version, true);
          }
        }
        if (vv.stamped()) {
          return newEntry(wrappedValue, hardTtlMs, softTtlMs, state, normalHardTtl, normalSoftTtl, vv.dataVersion());
        }
        if (existing != null) {
          // Fail-open (ADR-0033): preserve the existing entry's version
          // identity instead of zeroing it on reload. The degraded flag
          // follows the sign of the carried-over version.
          return newEntry(
            wrappedValue,
            hardTtlMs,
            softTtlMs,
            state,
            normalHardTtl,
            normalSoftTtl,
            existing.getDataVersion()
          );
        }
        return newEntry(wrappedValue, hardTtlMs, softTtlMs, state, normalHardTtl, normalSoftTtl, VERSION_DEFAULT);
      });
  }

  /**
   * Build a fresh entry over an ALREADY-wrapped value via the single draft
   * factory — the shared tail of the load paths. The compression happened
   * before the Caffeine bin lock (write-side ADR-0030 discipline, mirroring
   * {@code DefaultBackgroundRefresher.applyRefreshTask}), so the draft must not
   * re-wrap — re-wrapping a stored value would corrupt the format.
   *
   * @param wrappedValue      the value in its final stored form
   * @param hardTtlMs         the hard TTL in milliseconds (already resolved)
   * @param softTtlMs         the soft TTL in milliseconds (already resolved; 0 = disabled)
   * @param state             the key state to assign
   * @param normalHardTtl     the normal (non-hot) hard TTL to preserve for demotion
   * @param normalSoftTtl     the normal (non-hot) soft TTL to preserve for demotion
   * @param dataVersion       the {@code dataVersion} to stamp (ADR-0033);
   *                          {@link VERSION_DEFAULT} when the probe was withheld
   * @return a new {@link CacheEntry}
   */
  private CacheEntry newEntry(
    Object wrappedValue,
    long hardTtlMs,
    long softTtlMs,
    KeyState state,
    long normalHardTtl,
    long normalSoftTtl,
    long dataVersion
  ) {
    return newEntry(wrappedValue, hardTtlMs, softTtlMs, state, normalHardTtl, normalSoftTtl, dataVersion, false);
  }

  /**
   * Build a fresh entry over an ALREADY-wrapped value via the single draft
   * factory — the shared tail of the load paths, with an explicit
   * solo-flight flag (ADR-0079).
   *
   * @param wrappedValue      the value in its final stored form
   * @param hardTtlMs         the hard TTL in milliseconds (already resolved)
   * @param softTtlMs         the soft TTL in milliseconds (already resolved; 0 = disabled)
   * @param state             the key state to assign
   * @param normalHardTtl     the normal (non-hot) hard TTL to preserve for demotion
   * @param normalSoftTtl     the normal (non-hot) soft TTL to preserve for demotion
   * @param dataVersion       the {@code dataVersion} to stamp (ADR-0033);
   *                          {@link VERSION_DEFAULT} when the probe was withheld
   * @param soloFlight        whether this is a refault-gate solo-flight residency —
   *                          its SIZE eviction must not advance the refault clock
   * @return a new {@link CacheEntry}
   */
  private CacheEntry newEntry(
    Object wrappedValue,
    long hardTtlMs,
    long softTtlMs,
    KeyState state,
    long normalHardTtl,
    long normalSoftTtl,
    long dataVersion,
    boolean soloFlight
  ) {
    return entryLifecycle
      .newEntry()
      .value(wrappedValue)
      .version(dataVersion)
      .ttl(hardTtlMs, softTtlMs, normalHardTtl, normalSoftTtl)
      .keyState(state)
      .soloFlight(soloFlight)
      .build();
  }

  /**
   * Renew the soft expiry of a Worker-managed entry whose reload storage was
   * refused by {@link #loadCacheEntry}, so a kept soft-expired entry (REVALIDATE
   * keep-and-reload of a COOL entry the Worker never rebroadcast) is not
   * reloaded at read rate: the renewal pushes the soft timestamp past the
   * current read, re-opening the refresh/reload window only after the entry's
   * own soft cadence.
   *
   * <p>
   * This does not violate ADR-0034 (no read-path HOT expiry extension): the
   * renewal never moves the <b>hard</b> expiry — the hard TTL remains the
   * absolute bound for every state — and a soft renewal within that bound is
   * exactly what the Worker's own periodic HOT rebroadcast (ADR-0024) already
   * provides on the broadcast path. The renewal also never shortens a fresher
   * timestamp (a concurrent broadcast that just landed wins) and only applies
   * to entries actually soft-expired — a fresh entry left by an in-flight
   * broadcast race is returned verbatim.
   *
   * @param ce                the kept Worker-managed entry
   * @param fallbackSoftTtlMs the caller-resolved soft TTL, used only when the
   *                          entry carries no soft cadence of its own
   * @return the (possibly renewed) entry to store back
   */
  private CacheEntry renewSoftExpiryAfterRefusedLoad(CacheEntry ce, long fallbackSoftTtlMs) {
    long softTtl = ce.getSoftTtlMs();
    long effectiveSoftTtl = softTtl > 0 ? softTtl : fallbackSoftTtlMs;
    if (!entryLifecycle.ttlPolicy().isSoftExpired(ce) || effectiveSoftTtl <= 0) {
      return ce;
    }

    // Hard TTL is the absolute bound (ADR-0034): never renew past it.
    long renewedAt = Math.min(entryLifecycle.ttlPolicy().toSoftExpireTimestamp(softTtl), ce.getHardExpireAtMs());
    if (renewedAt <= ce.getSoftExpireAtMs()) {
      return ce;
    }
    // Explicit soft timestamp only: durations, value, and metadata untouched.
    return entryLifecycle.editEntry(ce).softExpiryAt(renewedAt).build();
  }

  /**
   * Invalidate a single key from L1 and send INVALIDATE to peers,
   * so they remove their local copy and re-fetch on next {@link #get}.
   * The next local {@link #get} will re-fetch from the reader.
   *
   * <p><b>C3 observability:</b> returns the allocated data version so the
   * caller can correlate this invalidation against peer state and logs —
   * fire-and-forget delivery itself is unchanged (ADR-0007), but the
   * operation is no longer unobservable. Returns {@code -1} when no version
   * was allocated (local-only invalidation or invalid key).
   *
   * @param cacheKey the key to invalidate
   * @param broadcastToPeers whether to broadcast the invalidation to peers
   * @return the allocated data version, or {@code -1} when local-only/invalid
   */
  public long invalidate(String cacheKey, boolean broadcastToPeers) {
    String nk = normalize(cacheKey);
    if (invalidCacheKey(nk)) {
      if (log.isDebugEnabled()) {
        log.debug("invalidate: invalid cacheKey");
      }
      return -1L;
    }

    // ADR-0086: bump the data version BEFORE dropping L1, not after.
    //
    // The old order (delete L1 → async bump → broadcast INVALIDATE(v+1)) left a
    // window in which this instance served a miss, reloaded the pre-invalidation
    // value and cached it stamped with the OLD version v. Nothing re-checked it
    // afterwards: the INVALIDATE we broadcast is never applied locally, and the
    // local entry had no v+1 watermark to compare against, so the stale value
    // stayed pinned for its whole TTL while peers correctly dropped theirs.
    // Allocating the new version first means every reload that races the delete
    // is stamped v+1, so the next mutation's v+2 correctly supersedes it.
    //
    // The bump is synchronous here: this block already runs on the committing
    // thread (or inline outside a transaction) via TransactionSupport, and an
    // invalidate is a write-path operation that can afford one Redis RTT —
    // unlike a read, where the latency would land on every request.
    VersionController.VersionResult bumped = broadcastToPeers ? bumpVersion(nk) : null;

    TransactionSupport.runNowOrAfterCommit(() -> {
      caffeineCache.invalidate(nk);
      // The L1 removal must extend to the dedup cache (ADR-0067): a completed
      // SingleFlight result from before this invalidation would otherwise be
      // replayed onto the next miss and re-cache the pre-invalidation value.
      singleFlight.invalidate(nk);
      if (bumped != null) {
        dispatcher.send(nk, SyncMessage.TYPE_INVALIDATE, bumped.dataVersion(), bumped.degraded());
      }
    });
    return bumped != null ? bumped.dataVersion() : -1L;
  }

  /**
   * Allocate the next data version for a key, degrading to a local fallback
   * version when the version store is unreachable.
   *
   * <p>A Redis failure must never silently drop a peer invalidation: the old
   * code caught the exception, logged ERROR and returned without broadcasting,
   * leaving peers on the stale value until TTL expiry. {@code putThrough} already
   * degrades gracefully on the same failure — this path now matches it.
   *
   * @param cacheKey the normalized cache key
   * @return the allocated version result, never {@code null}
   */
  private VersionController.VersionResult bumpVersion(String cacheKey) {
    try {
      return versionController.nextVersion(cacheKey);
    } catch (Exception ex) {
      log.error("version bump failed for key={}; falling back to a degraded local version", cacheKey, ex);
      return versionController.fallbackVersion();
    }
  }

  /**
   * Drop L1 (and the dedup entry) for one key and broadcast a versioned
   * INVALIDATE.
   *
   * <p><b>The version must already be allocated</b> — call
   * {@link #bumpVersion(String)} before the L1 removal and pass the result here.
   * The single-key {@link #invalidate(String, boolean)} established that
   * ordering and this helper exists so the batch and {@code invalidateAfterPut}
   * paths cannot drift from it again:
   *
   * <p>Allocating <em>after</em> the drop reopens the window ADR-0086 closed.
   * A reader that misses after the drop reloads and stamps the value with the
   * still-old version {@code v}; the INVALIDATE that then goes out carries
   * {@code v+1}, so the plain {@code >=} matrix in {@code shouldSkipForSync}
   * treats the reloaded entry as newer and skips it. The stale value is then
   * pinned for its whole TTL with no watermark that could unpin it. Bumping
   * first makes every racing reload stamp {@code v+1}, so the mutation's
   * {@code v+1} correctly supersedes it.
   *
   * <p>Also note the previous shape sent asynchronously from {@code hotKeyExecutor}
   * and, on saturation, dropped the peer invalidation entirely — one more way to
   * lose an invalidation. The bump is now synchronous on the committing thread,
   * which is the cost {@link #invalidate(String, boolean)} already documented and
   * accepted: an invalidate is a write-path operation that can afford one Redis
   * RTT, unlike a read where the latency would land on every request.
   *
   * @param cacheKey the normalized cache key
   * @param bumped   the version allocated before the drop, or {@code null} when
   *                 peers must not be notified (broadcast disabled)
   */
  private void dropAndSendInvalidate(String cacheKey, VersionController.VersionResult bumped) {
    caffeineCache.invalidate(cacheKey);
    // The L1 removal must extend to the dedup cache (ADR-0067): a completed
    // SingleFlight result from before this invalidation would otherwise be
    // replayed onto the next miss and re-cache the pre-invalidation value.
    singleFlight.invalidate(cacheKey);
    if (bumped != null) {
      dispatcher.send(cacheKey, SyncMessage.TYPE_INVALIDATE, bumped.dataVersion(), bumped.degraded());
    }
  }

  /**
   * Invalidate all given keys from L1 and broadcast a versioned INVALIDATE for each.
   * Invalid keys (null or blank) are silently skipped.
   *
   * <p><b>Per-key versioned broadcast (ADR-0066):</b> each key is invalidated through
   * the same bump-then-drop ordering as {@link #invalidate(String, boolean)} —
   * one version bump plus one INVALIDATE message per key, exactly like
   * {@link #invalidateAfterPut(Map, boolean)}. The former single unversioned
   * INVALIDATE_ALL broadcast could not be ordered against a newer REFRESH on the same
   * key, so a delayed batch could blindly wipe entries a peer had just refreshed.
   * The cost is one version bump per key on this (rare, explicit) operation; the
   * INVALIDATE_ALL receiver is kept for rolling upgrades against older peers.
   *
   * @param cacheKeys        the keys to invalidate
   * @param broadcastToPeers whether to broadcast the invalidation to peers
   * @return per-key allocated versions (insertion-ordered, caller's key form);
   *         invalid keys map to {@code -1}
   */
  public Map<String, Long> invalidate(Iterable<String> cacheKeys, boolean broadcastToPeers) {
    List<String> validKeys = new ArrayList<>();
    List<String> originalKeys = new ArrayList<>();
    Map<String, Long> versions = new LinkedHashMap<>();
    for (String key : cacheKeys) {
      String normalized = normalize(key);
      if (!invalidCacheKey(normalized)) {
        validKeys.add(normalized);
        originalKeys.add(key);
      } else {
        // Contract (Zeta#invalidateAndGetVersions): invalid keys map to -1
        // instead of going missing, so callers can reconcile by size.
        versions.put(key, -1L);
      }
    }
    if (validKeys.isEmpty()) {
      if (log.isDebugEnabled()) {
        log.debug("invalidate: all cacheKeys are invalid");
      }
      return versions;
    }

    // Allocate every version BEFORE any drop (ADR-0086 for the whole batch),
    // synchronously so the caller observes them even though the drops defer to
    // after-commit inside a transaction.
    VersionController.VersionResult[] bumped = new VersionController.VersionResult[validKeys.size()];
    if (broadcastToPeers) {
      for (int i = 0; i < validKeys.size(); i++) {
        bumped[i] = bumpVersion(validKeys.get(i));
        versions.put(originalKeys.get(i), bumped[i].dataVersion());
      }
    } else {
      for (String original : originalKeys) {
        versions.put(original, -1L);
      }
    }

    TransactionSupport.runNowOrAfterCommit(() -> {
      // ADR-0086 for the whole batch: allocate every version BEFORE any drop, so
      // a reader racing any single key's removal is stamped v+1 and the
      // invalidation that follows supersedes it. Interleaving bump and drop per
      // key would leave the keys already dropped still on the old version.
      caffeineCache.invalidateAll(validKeys);
      for (int i = 0; i < validKeys.size(); i++) {
        dropAndSendInvalidate(validKeys.get(i), bumped[i]);
      }
    });
    return versions;
  }

  /**
   * Refresh the given key: allocate the version <b>before</b> the load, then
   * apply the loaded value through the write-through guard.
   *
   * <p><b>C2 ordering:</b> the previous shape loaded first and bumped inside
   * {@code putThrough}, so a concurrent {@code putThrough} landing during the
   * load held an <i>older</i> version than the refresh's post-load bump — the
   * refresh then overwrote fresher data with its stale load. Allocating first
   * inverts the outcome: the concurrent write holds the newer version and the
   * refresh's {@code buildPutThroughEntry} guard ({@code shouldSkipForRefresh})
   * skips the stale apply. A burned version on loader failure is a harmless
   * watermark hole (decisionVersion already documents the same semantics).
   *
   * <p>A {@code null} loader result keeps the evict-only contract (local drop,
   * no broadcast — the next read reloads). The load itself runs on the caller
   * thread; only the apply defers to after-commit inside a transaction.
   *
   * @param cacheKey         the key to refresh
   * @param loader           the value supplier (runs on the caller thread)
   * @param hardTtlMs        hard TTL override (0 = use configured default)
   * @param softTtlMs        soft TTL override (0 = use configured default)
   * @param broadcastToPeers whether to broadcast the refresh to peers
   * @param <V>              the value type
   */
  public <V> void refresh(
    String cacheKey,
    Supplier<V> loader,
    long hardTtlMs,
    long softTtlMs,
    boolean broadcastToPeers
  ) {
    String nk = normalize(cacheKey);
    if (preGuard(nk) == null) {
      return;
    }
    Objects.requireNonNull(loader, "loader must not be null");
    if (hardTtlMs == Long.MAX_VALUE && broadcastToPeers) {
      throw new IllegalArgumentException(
        "refresh with Long.MAX_VALUE hard TTL cannot broadcast: the entry outlives the Redis " +
          "version key and peers would silently reject later updates"
      );
    }
    // Bump-before-load: every reload racing the apply below stamps an older
    // version than a concurrent writer that bumped during our load, so the
    // guard skips the stale apply instead of regressing it.
    VersionController.VersionResult vr = bumpVersion(nk);
    V value = loader.get();
    TransactionSupport.runNowOrAfterCommit(() -> {
      // The committed load obsoletes prior dedup results (ADR-0067).
      singleFlight.invalidate(nk);
      if (value == null) {
        caffeineCache.invalidate(nk);
        return;
      }
      if (isOversizedValue(nk, value)) {
        if (oversizedLogThrottle.tryAcquire()) {
          log.warn(
            "refresh value over max-value-size, broadcast without local residency for key={} (further bypasses hidden for 10s)",
            nk
          );
        }
        if (broadcastToPeers) {
          dispatcher.send(nk, SyncMessage.TYPE_REFRESH, vr.dataVersion(), vr.degraded());
        }
        return;
      }
      long effectiveHardTtl = entryLifecycle.ttlPolicy().resolveEffectiveHardTtl(hardTtlMs);
      long effectiveSoftTtl = entryLifecycle.ttlPolicy().resolveEffectiveSoftTtl(softTtlMs);
      Object wrappedValue = entryLifecycle.wrapValue(value);
      caffeineCache
        .asMap()
        .compute(nk, (k, existing) ->
          buildPutThroughEntry(existing, wrappedValue, vr, effectiveHardTtl, effectiveSoftTtl)
        );
      if (broadcastToPeers) {
        dispatcher.send(nk, SyncMessage.TYPE_REFRESH, vr.dataVersion(), vr.degraded());
      }
    });
  }

  /**
   * Write-through with explicit TTL overrides.
   * Pass 0 to use the configured default for that TTL type.
   *
   * @param cacheKey  the key to write
   * @param value     the value to cache
   * @param writer    the data-source mutation to execute before caching
   * @param hardTtlMs hard TTL override (0 = use configured default;
   *                  {@link Long#MAX_VALUE} for permanent entry — no hard TTL
   *                  eviction)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param <T>       the value type
   */
  public <T> void putThrough(
    String cacheKey,
    T value,
    Runnable writer,
    long hardTtlMs,
    long softTtlMs,
    boolean broadcastToPeers
  ) {
    String nk = normalize(cacheKey);
    if (preGuard(nk) == null) {
      return;
    }
    // S3: mirror of the facade guard — permanent + broadcast outlives the
    // Redis version key and silently desyncs peers after its 7-day TTL.
    if (hardTtlMs == Long.MAX_VALUE && broadcastToPeers) {
      throw new IllegalArgumentException(
        "putThrough with Long.MAX_VALUE hard TTL cannot broadcast: the entry outlives the Redis " +
          "version key and peers would silently reject later updates"
      );
    }

    // ADR-0086: run inline when no transaction is active, defer to afterCommit
    // when one is. The previous unconditional runAsyncAfterCommit pushed the L1
    // write onto an executor even outside a transaction, where there is no
    // commit barrier to wait for — so the deferral bought nothing but a race:
    // a caller that invoked putThrough and then invalidated (the @CacheCondition
    // purge path in CacheExtensionAspect) saw the invalidate land first and the
    // deferred L1 write resurrect the just-purged entry; and a read immediately
    // following the write missed ("read-your-write" loss). Inside a transaction
    // the work still waits for commit, and because this synchronization is
    // registered before any that the caller registers afterwards, Spring's
    // registration-order afterCommit sequence keeps write-then-invalidate ordered.
    TransactionSupport.runNowOrAfterCommit(() -> {
      try {
        writer.run();
      } catch (RuntimeException e) {
        // Writer failed — do NOT cache, do NOT send, do NOT bump version.
        // Propagate so the caller observes the failed write instead of
        // assuming DB + cache were updated (outside a transaction this
        // reaches the caller synchronously; inside a transaction
        // TransactionSupport surfaces the afterCommit failure).
        log.error("putThrough writer failed for key={}, cache update skipped: {}", nk, e.getMessage(), e);
        throw e;
      } catch (Exception e) {
        log.error("putThrough writer failed for key={}, cache update skipped: {}", nk, e.getMessage(), e);
        throw new IllegalStateException("putThrough writer failed for key=" + nk, e);
      }

      // The committed write obsoletes every load result recorded before it
      // (ADR-0067): a later miss must re-invoke the reader, not replay a
      // dedup future carrying the pre-write value.
      singleFlight.invalidate(nk);

      long effectiveHardTtl = entryLifecycle.ttlPolicy().resolveEffectiveHardTtl(hardTtlMs);
      long effectiveSoftTtl = entryLifecycle.ttlPolicy().resolveEffectiveSoftTtl(softTtlMs);

      // Size gate (max-value-size): an oversized value still bumps the
      // version and broadcasts (peers apply their own gates), but takes no
      // local L1 residency — skip compression too. The caller sees a
      // successful return with no residency: every read misses until the
      // value shrinks (warned, throttled).
      boolean oversized = isOversizedValue(nk, value);
      if (oversized && oversizedLogThrottle.tryAcquire()) {
        log.warn(
          "putThrough value over max-value-size, version bumped and broadcast without local residency for key={} (further bypasses hidden for 10s)",
          nk
        );
      }

      // Compress before the Caffeine bin lock (write-side ADR-0030
      // discipline, mirroring DefaultBackgroundRefresher.applyRefreshTask); the
      // null→sentinel mapping matches the one in buildPutThroughEntry.
      Object wrappedValue = oversized ? null : entryLifecycle.wrapValue(value != null ? value : NullValue.INSTANCE);

      try {
        VersionController.VersionResult vr = versionController.nextVersion(nk);

        if (!oversized) {
          caffeineCache
            .asMap()
            .compute(nk, (k, existing) ->
              buildPutThroughEntry(existing, wrappedValue, vr, effectiveHardTtl, effectiveSoftTtl)
            );
        }

        if (broadcastToPeers) {
          dispatcher.send(nk, SyncMessage.TYPE_REFRESH, vr.dataVersion(), vr.degraded());
        }
      } catch (Exception e) {
        // Redis-relayed exception or unexpected error (the REFRESH scheduling
        // itself cannot reject: BroadcastBuffer.record degrades to a synchronous
        // flush when the scheduler is saturated). The degraded fallback rewrite
        // keeps the shouldSkipForRefresh guard: it applies only when no newer
        // normal entry holds the key — a healthy entry a concurrent writer
        // already refreshed past this point is never overwritten with a stale
        // degraded version. When the guard skips, the local L1 stays on
        // its pre-write value until the next successful write, a peer broadcast,
        // or the entry's hard TTL (graceful-degradation semantics, ADR-0067).
        log.error("putThrough cache/send failed for key={} (applying degraded local update)", nk, e);
        VersionController.VersionResult vr = versionController.fallbackVersion();

        if (!oversized) {
          caffeineCache
            .asMap()
            .compute(nk, (k, existing) ->
              buildPutThroughEntry(existing, wrappedValue, vr, effectiveHardTtl, effectiveSoftTtl)
            );
        }
        // Same broadcast opt-out as the success path above: a caller that
        // declined the broadcast must not get one from the degraded fallback.
        if (broadcastToPeers) {
          dispatcher.send(nk, SyncMessage.TYPE_REFRESH, vr.dataVersion(), vr.degraded());
        }
      }
    });
  }

  /**
   * Builds a {@link CacheEntry} for a write-through operation, preserving
   * Worker-managed state and version information from the existing entry.
   *
   * <p><b>Equal-version guard (shouldSkipForRefresh, not shouldSkipForSync):</b>
   * the writer's value is authoritative at the version it just allocated, so an
   * existing entry at the SAME version is overwritten. That equal case is exactly
   * ADR-0033's over-stamp race: the reader loaded pre-write data and its
   * probe-after-read absorbed this write's INCR, landing the stale value stamped
   * with {@code vr.dataVersion()} between the INCR and this compute. The plain
   * sync matrix's {@code >=} equality skip would keep that stale entry — and the
   * sender's own REFRESH is dropped by the origin filter (ADR-0067), pinning the
   * stale value on the writer instance until the next write or TTL. A strictly
   * NEWER existing entry still wins (a concurrent write holds fresher data), and
   * the 4-case degraded matrix is otherwise unchanged.
   *
   * @param existing         the previous cache entry (maybe {@code null} or a raw
   *                         value)
   * @param wrappedValue     the new value to cache, already wrapped/compressed by
   *                         the caller — compression must not run under the
   *                         Caffeine bin lock (write-side ADR-0030 discipline)
   * @param vr               the version result from the version controller
   * @param effectiveHardTtl the effective hard TTL to use
   * @param effectiveSoftTtl the effective soft TTL to use
   * @return a new {@link CacheEntry} with the supplied value and metadata
   */
  @SuppressWarnings("all")
  private CacheEntry buildPutThroughEntry(
    CacheEntry existing,
    Object wrappedValue,
    VersionController.VersionResult vr,
    long effectiveHardTtl,
    long effectiveSoftTtl
  ) {
    KeyState state = existing != null ? existing.getKeyState() : KeyState.NORMAL;
    if (existing != null && VersionGuard.shouldSkipForRefresh(existing, vr.dataVersion(), vr.degraded())) {
      if (vr.degraded() && degradedSkipLogThrottle.tryAcquire()) {
        log.warn(
          "putThrough degraded version skipped, local L1 keeps pre-write value until next successful write/broadcast/TTL (further skips hidden for 10s)"
        );
      } else if (vr.degraded() && log.isDebugEnabled()) {
        log.debug("putThrough degraded version skipped, local L1 keeps pre-write value");
      }
      return existing;
    }

    boolean isWorkerManaged = isWorkerManaged(existing);

    // decisionOf carries the Worker decision stamp through; for a local origin
    // it is null, which the draft stamps as VERSION_DEFAULT/0 — the same
    // all-zero fields the old hand-rolled locals produced.
    DecisionStamp decision = EntryLifecycle.decisionOf(existing);

    long normalHardTtl = effectiveHardTtl;
    long normalSoftTtl = effectiveSoftTtl;

    if (existing != null && isWorkerManaged) {
      normalHardTtl = existing.getNormalHardTtlMs();
      normalSoftTtl = existing.getNormalSoftTtlMs();
    }

    long hardTtl = effectiveHardTtl;
    long softTtl = effectiveSoftTtl;
    if (state == KeyState.HOT) {
      hardTtl = entryLifecycle.ttlPolicy().resolveEffectiveHotHard(hardTtl);
      softTtl = entryLifecycle.ttlPolicy().resolveEffectiveHotSoft(softTtl);
    }

    return entryLifecycle
      .newEntry()
      .value(wrappedValue)
      .version(vr.dataVersion())
      .decision(decision)
      .ttl(hardTtl, softTtl, normalHardTtl, normalSoftTtl)
      .keyState(state)
      .build();
  }

  /**
   * Write a value directly into the local L1 cache without version bump,
   * without send, without hot-key detection, and without reporting.
   * <p>
   * Existing entry metadata is preserved. On a Worker-managed (HOT/COOL)
   * existing entry the Worker's TTL regime is kept as well — only the value is
   * updated — because a local write must not leave a HOT/COOL entry wearing
   * normal TTLs the Worker cannot repair (its rebroadcast with the same
   * {@code decisionVersion} is skipped by the decision guard). If no entry
   * exists, a fresh {@link CacheEntry} is created with {@link KeyState#NORMAL}.
   * <p>
   * The local write obsoletes every load result recorded before it (ADR-0067,
   * same contract as {@link #putThrough}): a completed SingleFlight result from
   * before this write must not outlive it and replay the pre-write value onto
   * the next miss — putLocal carries no version bump, so dedup invalidation is
   * its only protection.
   * <p>
   * Pass {@code 0} for either TTL to use the configured default.
   *
   * @param cacheKey  the key to store
   * @param value     the value to cache
   * @param hardTtlMs hard TTL override (0 = use configured default)
   * @param softTtlMs soft TTL override (0 = use configured default)
   */
  public void putLocal(String cacheKey, Object value, long hardTtlMs, long softTtlMs) {
    String nk = normalize(cacheKey);
    if (preGuard(nk) == null) return;

    // Transaction-aware like every other write path (P0-6): inside a
    // transaction the L1 mutation waits for commit so a rollback never leaves
    // an uncommitted value in L1; outside a transaction this runs inline.
    TransactionSupport.runNowOrAfterCommit(() -> {
      // Size gate (max-value-size): oversized values take no L1 residency.
      if (isOversizedValue(nk, value)) {
        if (oversizedLogThrottle.tryAcquire()) {
          log.warn(
            "putLocal value over max-value-size, no L1 residency for key={} (further bypasses hidden for 10s)",
            nk
          );
        }
        singleFlight.invalidate(nk);
        return;
      }

      long hardTtl = entryLifecycle.ttlPolicy().resolveEffectiveHardTtl(hardTtlMs);
      long softTtl = entryLifecycle.ttlPolicy().resolveEffectiveSoftTtl(softTtlMs);

      // Compress before the Caffeine bin lock (write-side ADR-0030 discipline,
      // mirroring DefaultBackgroundRefresher.applyRefreshTask).
      Object wrappedValue = entryLifecycle.wrapValue(value);

      // The committed write obsoletes every load result recorded before it
      // (ADR-0067): a later miss must re-invoke the reader, not replay a
      // dedup future carrying the pre-write value.
      singleFlight.invalidate(nk);

      caffeineCache
        .asMap()
        .compute(nk, (k, existing) -> {
          if (existing != null) {
            if (isWorkerManaged(existing)) {
              // Worker-managed: the Worker owns the TTL regime (mirrors
              // loadCacheEntry's refusal branch and buildPutThroughEntry's
              // preservation) — update the value only.
              return entryLifecycle.editEntry(existing).value(wrappedValue).build();
            }
            return entryLifecycle.editEntry(existing).value(wrappedValue).ttl(hardTtl, softTtl).build();
          }
          return newEntry(wrappedValue, hardTtl, softTtl, KeyState.NORMAL, hardTtl, softTtl, VERSION_DEFAULT);
        });
    });
  }

  /**
   * Execute a mutation, then invalidate L1 and send.
   * Next {@link #get} will re-fetch from the reader.
   *
   * @param cacheKey the key to invalidate after mutation
   * @param mutation the mutation to execute
   */
  public void invalidateAfterPut(String cacheKey, Runnable mutation, boolean broadcastToPeers) {
    String nk = normalize(cacheKey);
    if (preGuard(nk) == null) return;

    TransactionSupport.runNowOrAfterCommit(() -> {
      try {
        mutation.run();
      } catch (RuntimeException e) {
        log.error("invalidateAfterPut mutation failed, skip invalidate and send: {}", nk, e);
        throw e;
      } catch (Exception e) {
        log.error("invalidateAfterPut mutation failed, skip invalidate and send: {}", nk, e);
        throw new IllegalStateException("invalidateAfterPut mutation failed for key=" + nk, e);
      }
      // ADR-0086: allocate the version before the drop so a reload racing this
      // removal is stamped v+1 and superseded by the INVALIDATE below.
      VersionController.VersionResult bumped = broadcastToPeers ? bumpVersion(nk) : null;
      dropAndSendInvalidate(nk, bumped);
    });
  }

  /**
   * Execute mutations for multiple keys, then invalidate each from L1 and send.
   * Each key gets its own mutation. If a single mutation fails, that key is
   * skipped
   * (invalidation and send are not performed for that key).
   *
   * <p><b>Partial failure is visible.</b> The batch used to return
   * {@code void}, so a caller could never tell that some keys were dropped —
   * the only trace was a log line. The returned set names the keys that were
   * <i>not</i> invalidated: keys rejected up front (blank, oversized, or
   * matching a rule that makes them uncacheable) and keys whose mutation
   * threw.
   *
   * <p><b>Transaction caveat:</b> inside an active Spring transaction the
   * mutations and invalidations are deferred to after-commit (see
   * {@link TransactionSupport#runNowOrAfterCommit}), so mutation outcomes are
   * not known when this method returns — such failures are only logged, and
   * the returned set holds the up-front rejections. Outside a transaction
   * everything runs on the calling thread and the set is complete.
   *
   * @param mutations a map of key → mutation pairs
   * @return the keys that were not invalidated (never {@code null}; empty
   *         when every key applied)
   */
  @SuppressWarnings("java:S4030")
  public Set<String> invalidateAfterPut(Map<String, ? extends Runnable> mutations, boolean broadcastToPeers) {
    // I8: the batch promises the complete failed-keys set, but inside a
    // transaction mutations run at afterCommit — after this method returns —
    // so failures would only be logged and the set would lie. Fail fast.
    if (TransactionSynchronizationManager.isSynchronizationActive()) {
      throw new IllegalStateException(
        "batch invalidateAfterPut(Map) is not supported inside a transaction: mutations run at " +
          "afterCommit, so the returned failed-keys set would be incomplete — use single-key " +
          "invalidateAfterPut per key instead"
      );
    }
    List<String> validKeys = new ArrayList<>();
    List<Runnable> validMutations = new ArrayList<>();
    // Concurrent: the deferred (after-commit) path can still be appending
    // failures after this method has returned the set to the caller.
    Set<String> failedKeys = ConcurrentHashMap.newKeySet();

    for (var entry : mutations.entrySet()) {
      String key = normalize(entry.getKey());
      if (preGuard(key) != null) {
        validKeys.add(key);
        validMutations.add(entry.getValue());
      } else {
        // Rejected before any mutation ran — the caller asked for a key this
        // cache will not touch, which is a partial failure of the batch.
        failedKeys.add(entry.getKey());
      }
    }
    if (validKeys.isEmpty()) {
      if (log.isDebugEnabled()) {
        log.debug("invalidateAfterPut: all cacheKeys are invalid");
      }
      return failedKeys;
    }

    TransactionSupport.runNowOrAfterCommit(() -> {
      for (int i = 0; i < validKeys.size(); i++) {
        String cacheKey = validKeys.get(i);
        try {
          validMutations.get(i).run();
        } catch (Exception e) {
          log.error("invalidateAfterPut mutation failed, skip invalidate and send: {}", cacheKey, e);
          failedKeys.add(cacheKey);
          continue;
        }
        // ADR-0086: allocate this key's version after its mutation commits but
        // before its L1 removal, so a reload racing the removal is stamped v+1
        // and superseded by the INVALIDATE. Each key is independent here, so the
        // per-key bump-in-place is sufficient — no cross-key ordering needed.
        VersionController.VersionResult bumped = broadcastToPeers ? bumpVersion(cacheKey) : null;
        dropAndSendInvalidate(cacheKey, bumped);
      }
    });
    return failedKeys;
  }

  /**
   * Estimated number of entries currently in the L1 cache.
   *
   * @return best-effort estimate of the current entry count
   */
  public long estimatedSize() {
    return caffeineCache.estimatedSize();
  }

  /**
   * Return a snapshot of request-level L1 cache statistics.
   * <p>
   * {@code hitCount}/{@code missCount}/{@code hitRate} count caller requests,
   * not Caffeine operations: one increment per served key on the load-capable
   * read paths ({@code get}, batch {@code get}, {@code computeIfAbsent}).
   * A key served from L1 without invoking the reader is a hit (sentinel hits
   * included); a key that reaches the loader — including a REVALIDATE
   * soft-expiry that blocks on a reload — is a miss. Internal
   * {@code asMap().compute()} maintenance and {@code peek()} probes never
   * touch these counters (use the Actuator cache view for storage internals).
   * {@code evictionCount} still comes from Caffeine, and both eviction and
   * size counters are {@code 0} when a custom {@code Cache} bean is supplied
   * without stats recording.
   *
   * @return a {@link ZetaCacheStats} report with request-level hit/miss
   */
  public ZetaCacheStats stats() {
    long hits = requestHits.sum();
    long misses = requestMisses.sum();
    long total = hits + misses;
    double rate = total == 0 ? 0.0 : (double) hits / total;
    CacheStats cs = caffeineCache.stats();
    return new ZetaCacheStats(hits, misses, rate, cs.evictionCount(), caffeineCache.estimatedSize());
  }

  /**
   * Emergency local flush — drops every L1 entry and every dedup future on
   * this JVM only, never broadcasting. Prefer this name over
   * {@link #invalidateAllLocal()}: the {@code Local} suffix is load-bearing
   * (one JVM, never the cluster) and {@code flush} reads as the destructive
   * operation it is.
   * <p>
   * No cross-instance sync messages are sent: peers keep serving their
   * entries until TTL expiry or their own flush. There is deliberately no
   * cluster-wide "clear everything" operation — a broadcast flush would be a
   * fire-and-forget fanout with no acknowledgement (ADR-0007), so callers
   * could never know whether peers actually cleared. Coordinate a rolling
   * restart or per-key invalidation instead.
   * <p>
   * The flush also clears the SingleFlight dedup cache (the bulk form of the
   * ADR-0067 contract): every completed load result predates the flush and
   * must not be replayed onto a post-flush miss, or the flushed values would
   * be re-cached from the dedup future within its short TTL.
   */
  public void flushLocalEmergency() {
    caffeineCache.invalidateAll();
    singleFlight.invalidateAll();
  }

  /**
   * Invalidate all entries from the L1 cache without broadcasting.
   *
   * @deprecated prefer {@link #flushLocalEmergency()} — same behavior, but the
   *             name states the single-JVM scope and the emergency nature.
   *             Kept delegating so existing callers keep compiling.
   */
  @Deprecated
  public void invalidateAllLocal() {
    flushLocalEmergency();
  }

  /**
   * Snapshot of the L1 keys carrying the given prefix — the read-only
   * replacement for the removed raw-cache handle: namespace scans (e.g.
   * annotation-cache {@code clear()}) get the key list without ever holding
   * a writable cache reference that could bypass version stamping and peer
   * broadcast.
   *
   * @param prefix the key prefix to match (never {@code null})
   * @return the matching keys at call time (point-in-time snapshot, never {@code null})
   */
  public List<String> localKeysWithPrefix(String prefix) {
    Objects.requireNonNull(prefix, "prefix must not be null");
    return caffeineCache
      .asMap()
      .keySet()
      .stream()
      .filter(k -> k.startsWith(prefix))
      .toList();
  }

  /**
   * Read-only snapshot of up to {@code limit} L1 entries, mapped to the
   * <b>user values</b> actually stored (the {@link CacheEntry} wrapper is
   * unwrapped; the null sentinel appears as {@code null} values, never dropped
   * and never throwing — the previous collector-based implementation threw
   * {@code NullPointerException} on the first sentinel).
   *
   * @param limit maximum number of entries to return; values {@code <= 0}
   *              yield an empty map
   * @return an immutable snapshot, never {@code null}
   */
  public Map<String, Object> snapshotValues(int limit) {
    if (limit <= 0) {
      return Map.of();
    }
    Map<String, Object> snap = new LinkedHashMap<>();
    for (Map.Entry<String, CacheEntry> e : caffeineCache.asMap().entrySet()) {
      if (snap.size() >= limit) {
        break;
      }
      // Null-valued sentinels stay in the map as nulls (Collections/in.Map
      // collectors reject null values); the plain loop keeps them.
      snap.put(e.getKey(), unwrapForStats(e.getValue(), compressor));
    }
    return Collections.unmodifiableMap(snap);
  }

  /**
   * Unwrap an L1 slot into the user-visible value for read-only
   * observation (used by {@code Zeta.StatsAdmin#snapshotValues(int)}).
   *
   * <p>Every L1 slot holds a {@link CacheEntry} whose payload may itself be the
   * {@link NullValue} sentinel or an LZ4-framed payload. Observation never leaks
   * the internal model and never mutates the cache: decompression failures fall
   * back to the stored payload instead of invalidating.
   *
   * @param stored the L1 slot value, may be {@code null}
   * @param compressor the value codec; {@code null} means no decompression
   * @return the user value, or {@code null} for a cached-null sentinel
   */
  public static Object unwrapForStats(@Nullable CacheEntry stored, @Nullable CacheCompressor compressor) {
    if (stored == null) {
      return null;
    }
    Object payload = stored.getValue();
    if (compressor != null) {
      try {
        Object val = compressor.unwrap(payload);
        return val == NullValue.INSTANCE ? null : val;
      } catch (IOException e) {
        return payload == NullValue.INSTANCE ? null : payload;
      }
    }
    return payload == NullValue.INSTANCE ? null : payload;
  }

  /**
   * Unwrap an L1 slot into the stored payload for read-only observation.
   *
   * <p>Legacy single-arg form: returns the stored payload without decompression,
   * so LZ4-compressed values come back as their framed {@code byte[]}. Prefer
   * {@link #unwrapForStats(CacheEntry, CacheCompressor)} for the user-visible value.
   *
   * @param stored the L1 slot value, may be {@code null}
   * @return the stored payload, or {@code null} for a cached-null sentinel
   */
  public static Object unwrapForStats(@Nullable CacheEntry stored) {
    if (stored == null) {
      return null;
    }
    Object value = stored.getValue();
    return value == NullValue.INSTANCE ? null : value;
  }

  /**
   * Resolve the effective soft TTL in milliseconds.
   * <p>
   * When the given value is {@code 0} (meaning "use configured default"),
   * returns the default soft TTL from the EntryLifecycle.
   * Otherwise returns the value as-is.
   *
   * @param softTtlMs the soft TTL value to resolve (0 = use configured default)
   * @return the effective soft TTL in milliseconds, always positive
   */
  public long resolveEffectiveSoftTtl(long softTtlMs) {
    return entryLifecycle.ttlPolicy().resolveEffectiveSoftTtl(softTtlMs);
  }

  /**
   * The policy's TTL suppliers are evaluated lazily — at most once, and only
   * when an entry is created, promoted, renewed, or a soft-expire refresh is
   * scheduled — so SpEL-based TTL expressions cost nothing on a plain hit.
   * When {@link ReadPolicy#nullCaching()} is {@code false}, a {@code null}
   * loader result leaves no cache entry.
   *
   * @param cacheKey the key to retrieve
   * @param policy   the resolved per-invocation cache policy (carries reader,
   *                 TTLs, null-caching, and report-allow flag)
   * @param <T>      the value type
   * @return an {@link Optional} containing the cached or loaded value
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  @SuppressWarnings("all")
  public <T> Optional<T> computeIfAbsent(String cacheKey, ReadPolicy policy) {
    Objects.requireNonNull(policy, "policy must not be null");
    String nk = normalize(cacheKey);
    RuleAction action = preGuard(nk);
    if (action == null) return Optional.empty();

    boolean skipReport = isSkipReport(action == RuleAction.ALLOW_NO_REPORT, policy.reportEnabled());
    return execute(nk, policy.failOnError(), () -> computeInLock(nk, policy, skipReport));
  }

  /**
   * Atomic read-through with soft-expire awareness, sharing the exact engine of
   * {@link #computeIfAbsent(String, ReadPolicy)}: the compute callback applies
   * the policy's {@link StalePolicy} (RETURN / SOFT_REFRESH / REVALIDATE) to
   * soft-expired entries, so the two entry points are behaviorally identical —
   * this method delegates and exists only as the soft-expire-named API surface.
   * Lazy TTL evaluation and the null-caching opt-out follow the same contract
   * as {@link #computeIfAbsent(String, ReadPolicy)}.
   *
   * @param cacheKey the key to retrieve
   * @param policy   the resolved per-invocation cache policy (carries reader,
   *                 TTLs, null-caching, stale-policy, and report-allow flag)
   * @param <T>      the value type
   * @return an {@link Optional} containing the cached (possibly stale) or loaded
   *         value
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  @SuppressWarnings("all")
  public <T> Optional<T> computeIfAbsentWithSoftExpire(String cacheKey, ReadPolicy policy) {
    return computeIfAbsent(cacheKey, policy);
  }

  /**
   * Mutable outcome of the in-lock read: which branch the compute callback
   * took and the entry to serve after the bin lock is released. Replaces the
   * former four parallel single-element arrays.
   */
  private static final class LockReadOutcome {

    private boolean hit;
    private boolean refresh;
    private CacheEntry refreshEntry;
    private CacheEntry served;
  }

  /**
   * Shared body for {@link #computeIfAbsent} and
   * {@link #computeIfAbsentWithSoftExpire}. Caffeine's
   * {@code asMap().compute()} is used only for the fast hit path —
   * see ADR-0030.
   * <p>
   * <b>In-lock (fast) path:</b> the compute callback performs state checks
   * and pure TTL re-computation only: promote TopK members to HOT and apply
   * the stale policy (RETURN / SOFT_REFRESH / REVALIDATE); the value unwrap
   * happens outside the lock afterwards. No user code, no I/O, and no
   * decompression run under the bin lock, so a slow reader or a large value
   * cannot stall unrelated keys in the same hash bin.
   * <p>
   * <b>Lock-free load path:</b> on a true miss or a hard-expired entry the
   * compute returns {@code null}; a REVALIDATE soft-expiry keeps the stale
   * entry in L1. Either way the reader is invoked <i>outside</i> the lock
   * via {@link SingleFlight#load}, inheriting its dedup, timeout, and
   * circuit-breaker protection, then the loaded value is stored by a short
   * second compute through {@link #processLoaded} / {@link #loadCacheEntry}
   * — the same machinery as the {@code get} read path.
   * <p>
   * <b>Reader exception safety:</b> a failing reader rethrows when
   * {@code failOnError} is set (the pre-existing entry is left untouched);
   * otherwise the error is logged and the call resolves to empty. When the
   * circuit breaker is open, a kept stale entry is still serveable
   * (graceful degradation, ADR-0021); hard-expired entries are never served.
   * <p>
   * <b>Lazy TTL:</b> the TTL suppliers are evaluated at most once per call
   * and only when an entry is created, promoted, or a soft-expire
   * refresh is scheduled — never on a plain NORMAL-entry hit (ADR-0023).
   *
   * @param cacheKey   the key to retrieve
   * @param policy     the resolved per-invocation cache policy (carries reader,
   *                   lazy TTLs, null-caching decision, stale policy)
   * @param skipReport whether to skip Worker reporting
   * @param <T>        the value type
   * @return the cached or loaded value, or empty when the reader returned
   *         {@code null}
   */
  @SuppressWarnings("all")
  private <T> Optional<T> computeInLock(String cacheKey, ReadPolicy policy, boolean skipReport) {
    @SuppressWarnings("all")
    Supplier<T> reader = (Supplier<T>) policy.reader();
    // Shared lazy read context: the (possibly SpEL-backed) TTL suppliers are
    // evaluated at most once per call and only when a branch actually consumes
    // them — never on a plain NORMAL-entry hit, and with no memoize wrapper
    // allocation on that path either.
    ReadContext rp = ReadContext.of(policy, skipReport);

    LockReadOutcome outcome = new LockReadOutcome();
    caffeineCache
      .asMap()
      .compute(cacheKey, (k, existing) -> {
        if (existing == null || entryLifecycle.ttlPolicy().isLogicallyExpired(existing)) {
          // hard-expired: remove, load outside the lock
          return null;
        }

        outcome.hit = true;
        outcome.served = existing;

        // Decision-Validity demotion (ADR-0035): a Worker-sourced HOT entry
        // whose issuing Worker died/restarted is reverted to NORMAL in
        // place. The check-and-rewrite is atomic here because the callback
        // already holds the Caffeine bin lock — the pure function must not
        // be wrapped in its own compute. Demotion and local promotion are
        // mutually exclusive within this read (mirroring process()): a
        // demoted entry stays NORMAL for one read, then the local TopK
        // re-promotes it on the next read if still hot.
        CacheEntry cur = existing;
        CacheEntry demoted = entryLifecycle.demoteIfDecisionInvalidInPlace(cur);
        if (demoted != null) {
          cur = demoted;
        } else {
          // Local promotion only (see LocalPromotion): HOT entries are left
          // untouched — the hard TTL bound governs them, and renewal is the
          // Worker's job via periodic HOT rebroadcast (ADR-0024) and
          // reload-after-expiry. The TTL suppliers stay lazy inside
          // promoteInLock: a plain NORMAL hit never triggers a SpEL
          // evaluation. A non-promoted entry falls through to the
          // sentinel/stale handling below, exactly as before.
          CacheEntry promoted = promotion.promoteInLock(cur, k, rp::hardTtlMs, rp::softTtlMs);
          if (promoted != cur) {
            return promoted;
          }
        }

        // NullValue sentinel: never reach the stale-policy switch. Sentinels
        // carry softExpireAtMs = 0, which the disabled-soft-TTL contract of
        // isSoftExpired already reports as never stale, so this guard is
        // belt-and-suspenders: even if that contract ever changed, a
        // sentinel must not enter SOFT_REFRESH (re-arming the loader at
        // read rate while the source keeps returning null) or REVALIDATE
        // (drop-and-reload) — both would defeat the short-TTL penetration
        // protection.
        if (cur.getValue() == NullValue.INSTANCE) {
          return cur;
        }

        if (entryLifecycle.ttlPolicy().isSoftExpired(cur)) {
          StalePolicy stalePolicy = rp.stalePolicy();
          switch (stalePolicy) {
            case RETURN -> {
              return cur;
            }
            case SOFT_REFRESH -> {
              // Trigger the background refresh outside the compute lock
              // (see below); the caller already receives the stale value.
              // A null reader (convenience policies) skips the refresh;
              // a successful COOL refresh downgrades the entry to NORMAL
              // (see DefaultBackgroundRefresher#applyRefreshTask).
              if (reader != null) {
                outcome.refresh = true;
                outcome.refreshEntry = cur;
              }
              return cur;
            }
            case REVALIDATE -> {
              outcome.hit = false;
              // Keep the stale entry in L1: it powers the circuit-breaker
              // fallback and lets the next call retry after a failed load.
              return cur;
            }
          }
        }
        return cur;
      });

    if (outcome.hit) {
      // Sentinel fast path: served entry is the NullValue marker — count the
      // access (penetration-prone keys can go hot) and serve empty without
      // paying decompression.
      if (outcome.served != null && outcome.served.getValue() == NullValue.INSTANCE) {
        requestHits.increment();
        if (outcome.refresh) {
          backgroundRefresher.triggerBackgroundRefresh(
            cacheKey,
            () -> probeVersioned(cacheKey, reader.get()),
            computeSoftTtlForRefresh(rp.softTtlMs(), outcome.refreshEntry)
          );
        }
        dispatcher.recordAccess(cacheKey, skipReport);
        return Optional.empty();
      }
      // The hit side is fully resolved in the compute above; only the
      // side-effectful parts (background refresh trigger, Worker report) and
      // the value unwrap run outside the lock — decompression must never
      // stall unrelated keys in the same hash bin (ADR-0030).
      T corruptCheck = this.<T>unwrapRawValue(outcome.served, cacheKey);
      if (corruptCheck != null) {
        requestHits.increment();
        if (outcome.refresh) {
          backgroundRefresher.triggerBackgroundRefresh(
            cacheKey,
            () -> probeVersioned(cacheKey, reader.get()),
            computeSoftTtlForRefresh(rp.softTtlMs(), outcome.refreshEntry)
          );
        }

        dispatcher.recordAccess(cacheKey, skipReport);
        return Optional.of(corruptCheck);
      }
      // Decompression corruption (unwrap already invalidated the entry):
      // not a sentinel hit — fall through to the miss/load path below with
      // no hot-key counting, so the source repopulates the entry.
    }

    // The in-lock compute resolved to a load (true miss, hard-expired, or
    // REVALIDATE soft-expiry): the reader runs below.
    requestMisses.increment();

    // S2: no data source — resolve empty (stale-first when the breaker is
    // open) instead of NPEing inside the SingleFlight execution.
    if (reader == null) {
      Optional<T> stale = staleIfBreakerOpen(cacheKey);
      return stale != null ? stale : resolveNoAnswerOutcome(cacheKey);
    }

    // Lock-free load path, mirroring loadAndCache: SingleFlight provides
    // dedup, timeout, and circuit-breaker protection; the reader never runs
    // under the Caffeine bin lock, so a slow upstream cannot stall unrelated
    // keys in the same hash bin (ADR-0030). The result is written by a short
    // second compute via processLoaded/loadCacheEntry.
    //
    //
    // Stale-first ordering (H): a kept entry serveable under an open breaker
    // wins even when failOnError is set — graceful degradation outranks
    // fail-fast. The pre-existing entry was left untouched by the in-lock
    // compute (REVALIDATE kept it, miss removed a hard-expired one).
    LoaderOutcome<VersionedValue> loadOutcome = singleFlight.loadOutcome(cacheKey, () ->
      probeVersioned(cacheKey, reader.get())
    );
    switch (loadOutcome.status()) {
      case LOADED -> {
        VersionedValue vv = loadOutcome.value();
        if (vv.value() == null) {
          return resolveNullLoadOutcome(cacheKey, rp.nullCaching(), vv);
        }
        @SuppressWarnings("all")
        T t = (T) vv.value();
        try {
          return Optional.of(processLoaded(cacheKey, t, vv, rp.hardTtlMs(), rp.softTtlMs(), skipReport));
        } catch (ZetaBlockedException blocked) {
          throw blocked;
        }
      }
      case SOURCE_EMPTY -> {
        return resolveNullLoadOutcome(cacheKey, rp.nullCaching());
      }
      case FAILED, SUPPRESSED_BREAKER, SUPPRESSED_TIMEOUT, SUPPRESSED_EXECUTOR -> {
        LoadStatus status = loadOutcome.status();
        throwIfFailFast(
          cacheKey,
          status,
          status == LoadStatus.FAILED ? loadOutcome.cause() : null,
          policy.failOnError()
        );
        // Degrade is path-specific (unlike the throw policy above): a stale
        // entry wins first; otherwise both a FAILED and a suppressed load
        // resolve empty WITHOUT writing a sentinel — the source never
        // answered null (a FAILED load preserves the kept REVALIDATE entry
        // via log + empty, next read retries).
        Optional<T> stale = staleIfBreakerOpen(cacheKey);
        if (stale != null) {
          return stale;
        }
        if (status == LoadStatus.FAILED) {
          logReadFailure(cacheKey, loadOutcome.cause());
          return Optional.empty();
        }
        logReadFailure(cacheKey, null);
        return resolveNoAnswerOutcome(cacheKey);
      }
      // No silent default: a future LoadStatus must fail loudly here rather
      // than resolve as a miss (exhaustiveness is load-bearing for fail-fast).
      default -> throw new AssertionError("unhandled LoadStatus: " + loadOutcome.status());
    }
  }

  /**
   * Unified fail-fast adjudication for unsuccessful loads — the single owner
   * of the throw policy behind all three read paths (single, batch,
   * {@code computeInLock}), so the FAILED/SUPPRESSED handling cannot drift
   * apart again. Throws iff fail-fast demands it:
   * <ul>
   *   <li>{@code ZetaBlockedException} and {@code Error} always propagate
   *       (policy rejection and fatal faults are never swallowable);</li>
   *   <li>an {@code InterruptedException} cause restores the interrupt bit
   *       and degrades (legacy parity: the dedup layer historically resolved
   *       interruption to empty even under fail-fast);</li>
   *   <li>otherwise throws only when the breaker is closed <i>and</i>
   *       {@code failOnError} is set — a real failure rethrows its cause, a
   *       suppression (null cause) throws {@code ZetaSuppressedException}.
   *       A breaker-open load never throws here: the caller falls through to
   *       its degraded fallback (stale-first). This also aligns
   *       {@code computeInLock} with the other two paths, which never threw
   *       while open; its former open-no-stale throw under the flag was the
   *       odd corner out.</li>
   * </ul>
   *
   * <p>Deliberately NOT covering the degrade half: what "degrade" means is
   * genuinely per-path (sentinel write vs REVALIDATE keep), so each call site
   * supplies it after this method returns. Also deliberately separate from
   * {@link #executeCore}, which guards side-effect code (detection,
   * reporting, promotion) rather than load outcomes.
   *
   * @param cacheKey    the key whose load was unsuccessful
   * @param status      the outcome status (FAILED carries {@code cause})
   * @param cause       the reader failure, or {@code null} for suppression
   * @param failOnError whether read-path failures propagate to the caller
   */
  private void throwIfFailFast(String cacheKey, LoadStatus status, @Nullable Throwable cause, boolean failOnError) {
    if (cause instanceof ZetaBlockedException blocked) {
      throw blocked;
    }
    if (cause instanceof Error err) {
      throw err;
    }
    if (cause instanceof InterruptedException) {
      Thread.currentThread().interrupt();
      return;
    }
    if (!singleFlight.isBreakerOpen(cacheKey) && failOnError) {
      if (cause != null) {
        if (cause instanceof RuntimeException re) {
          throw re;
        }
        throw new java.util.concurrent.CompletionException(cause);
      }
      throw new ZetaSuppressedException("HotKeyCache", cacheKey, status);
    }
  }

  /**
   * Stale-first fallback shared by the {@code computeInLock} load path: when
   * the breaker is open and a hard-valid entry sits in L1, serve it (graceful
   * degradation, ADR-0021). Hard-expired entries are never served — the hard
   * TTL contract is not bypassed even in the race where the kept entry
   * crosses its hard expiry while the load is in flight.
   *
   * @param cacheKey the key to serve stale for
   * @return the stale value, or {@code null} when no serveable stale entry exists
   */
  private <T> Optional<T> staleIfBreakerOpen(String cacheKey) {
    if (!singleFlight.isBreakerOpen(cacheKey)) {
      return null;
    }
    CacheEntry stale = caffeineCache.getIfPresent(cacheKey);
    if (stale != null && !entryLifecycle.ttlPolicy().isLogicallyExpired(stale)) {
      T val = this.<T>unwrapRawValue(stale, cacheKey);
      if (log.isDebugEnabled()) {
        log.debug("CB open, returning stale entry in computeInLock for key={}", cacheKey);
      }
      // A NullValue sentinel stale serves as empty (not null): a previous
      // null-answer is itself the degraded value. Only a missing or
      // hard-expired entry yields null (no stale).
      return Optional.ofNullable(val);
    }
    return null;
  }

  /**
   * Rate-limited read-failure log shared by the {@code computeInLock} load
   * path: a sustained upstream outage would otherwise log per read at miss
   * rate (same 10s convention as {@code executeCore}).
   *
   * @param cacheKey the key whose load failed
   * @param cause    the failure cause (maybe {@code null} for suppression)
   */
  private void logReadFailure(String cacheKey, Throwable cause) {
    if (readErrorLogThrottle.tryAcquire()) {
      log.error(
        "computeInLock reader failed for key={}, returning empty to keep caller operational (further failures suppressed for 10s)",
        cacheKey,
        cause
      );
    } else {
      log.debug("computeInLock reader failed for key={}, returning empty to keep caller operational", cacheKey, cause);
    }
  }

  /**
   * Unwrap an L1 entry to its user-facing form: the entry's inner value is
   * decompressed and {@link NullValue}-unwrapped via {@link #unwrapValue}.
   * The shared shape of every read-side extraction
   * ({@code peek}, the hit paths, the stale fallback).
   *
   * @param raw      the L1 entry (may be {@code null})
   * @param cacheKey the key, for the decompression-failure log/invalidation
   * @param <T>      the expected value type
   * @return the user-facing value, or {@code null} for a sentinel
   */
  @SuppressWarnings("unchecked")
  private <T> T unwrapRawValue(@Nullable CacheEntry raw, String cacheKey) {
    if (raw == null) {
      return null;
    }
    return (T) unwrapValue(raw.getValue(), cacheKey);
  }

  /**
   * Unwrap a {@link NullValue} sentinel back to {@code null}.
   * <p>
   * When called from the extraction paths of {@link #get},
   * {@link #getWithSoftExpire},
   * and {@link #peek}, this ensures that null values stored via the sentinel are
   * transparently returned as {@code null} to callers.
   *
   * @param stored the raw value from a {@link CacheEntry}
   * @return {@code null} if the sentinel, otherwise the original value
   */
  @Nullable
  private Object unwrapValue(@Nullable Object stored, String cacheKey) {
    try {
      Object val = compressor.unwrap(stored);
      return val == NullValue.INSTANCE ? null : val;
    } catch (IOException e) {
      log.warn("Failed to decompress cache value for key={}, invalidating", cacheKey, e);
      // Conditional removal: only drop the entry if it still holds the exact
      // value that failed to decompress — a concurrently written fresh entry
      // must not be destroyed by this snapshot-based decision.
      caffeineCache
        .asMap()
        .computeIfPresent(cacheKey, (k, existing) ->
          existing != null && existing.getValue() == stored ? null : existing
        );
      return null;
    }
  }
}
