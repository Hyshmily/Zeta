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
package io.github.hyshmily.zeta.annotation;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.annotation.annotationsupporter.ZetaCacheContext;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.exception.ZetaBlockedException;
import io.github.hyshmily.zeta.model.CachePolicy;
import io.github.hyshmily.zeta.model.StalePolicy;
import io.github.hyshmily.zeta.util.LogThrottle;
import io.github.hyshmily.zeta.util.TimeSource;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.interceptor.SimpleKey;
import org.springframework.cache.interceptor.SimpleKeyGenerator;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.Ordered;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.core.annotation.Order;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Companion AOP aspect for Spring {@link Cacheable @Cacheable},
 * {@link CachePut @CachePut}, and {@link CacheEvict @CacheEvict} methods.
 * <p>
 * This aspect extends the default Spring Cache behavior by injecting
 * additional cache-control metadata (TTL, interception policies, fallback
 * logic, null-caching rules, broadcast skipping, hot-key handling, and
 * conditional caching). It acts as a bridge between the annotation-driven
 * configuration and the underlying {@link Zeta} distributed cache
 * infrastructure.
 * <p>
 * The aspect is ordered at {@link Ordered#HIGHEST_PRECEDENCE} to ensure
 * it wraps the caching layer before other interceptors execute.
 */
@Internal
@Aspect
@Order(Ordered.HIGHEST_PRECEDENCE)
@Slf4j
public class CacheExtensionAspect {

  /**
   * Reference to the core Zeta cache / detection engine.
   */
  private final Zeta zeta;

  /**
   * Separator inserted between a cache name and its key, read once from
   * {@link ZetaProperties#getSpringCache()} at construction time. Every key
   * assembly site in this aspect ({@code @Cacheable}, {@code @Preload},
   * {@code @Tag}) derives its prefix from this single value, so the three
   * cannot drift apart and the per-invocation getter chain disappears.
   */
  private final String keySeparator;

  /**
   * Parser for SpEL expressions used in annotation attributes.
   */
  private final SpelExpressionParser parser = new SpelExpressionParser();

  /**
   * Discovers method parameter names at runtime, enabling SpEL references
   * like {@code #paramName}.
   */
  private final ParameterNameDiscoverer parameterNameDiscoverer = new DefaultParameterNameDiscoverer();

  /**
   * Cache of compiled SpEL expressions to avoid repeated parsing.
   */
  private final Map<String, Expression> expressionCache = new ConcurrentHashMap<>();

  /**
   * Cache of discovered parameter names per method, so the reflection-based
   * {@link ParameterNameDiscoverer} runs at most once per method instead of
   * on every SpEL evaluation (cache miss/promotion/refresh).
   */
  private final Map<Method, String[]> paramNameCache = new ConcurrentHashMap<>();

  /**
   * Cache of resolved fallback methods keyed by the original method.
   * Reduces reflection overhead on repeated fallback calls.
   */
  private final Map<Method, Method> fallbackMethodCache = new ConcurrentHashMap<>();

  /** Upper bound for the aspect's bookkeeping caches (preload dedup, QPS buckets, QPS block table). */
  private static final int BOOKKEEPING_CACHE_MAX_SIZE = 100_000;

  /**
   * Lightweight Caffeine cache that tracks which preload keys have already
   * been registered with the local detector. Prevents duplicate registrations
   * for keys that are repeatedly processed (e.g., under high concurrency).
   */
  private final Cache<String, Boolean> registeredPreloadKeys = Caffeine.newBuilder()
    .maximumSize(BOOKKEEPING_CACHE_MAX_SIZE)
    .expireAfterWrite(1, TimeUnit.HOURS)
    .build();

  /**
   * Method-level cache for {@link Preload} annotations.
   */
  private final Map<Method, Preload> preloadCache = new ConcurrentHashMap<>();

  private static final SimpleKeyGenerator SIMPLE_KEY_GENERATOR = new SimpleKeyGenerator();

  /**
   * Saturating "effectively unlimited" count for {@link Preload} when
   * {@code count()} is unset: large enough to keep a preloaded key at the top
   * of the TopK for any realistic session, yet safe to re-add after the 1h
   * {@link #registeredPreloadKeys} dedup expiry without overflowing the
   * detector's {@code long} counter ({@code Long.MAX_VALUE} would overflow on
   * the second preload).
   */
  private static final long PRELOAD_UNLIMITED_COUNT = Integer.MAX_VALUE;

  /** Expiry for the QPS bookkeeping caches — a quiet key's buckets/block entry age out. */
  private static final Duration QPS_BOOKKEEPING_EXPIRY = Duration.ofMinutes(5);



  /**
   * Rate-limiter for per-invocation WARN sites: at most one WARN per
   * {@link LogThrottle#DEFAULT_WINDOW_MS} window, with suppressed occurrences counted and
   * reported alongside the next WARN. Admission is strict — {@link LogThrottle}
   * claims the window with a compare-and-set, so exactly one caller per window
   * logs; the atomicity and the monotonic clock are provided by
   * {@link LogThrottle}. Within a window the caller logs at DEBUG instead.
   */
  private static final class RateLimitedWarn {

    private final LogThrottle throttle = LogThrottle.perDefaultWindow();
    private final AtomicLong suppressed = new AtomicLong();

    /**
     * Try to acquire the right to emit this window's WARN.
     *
     * @return {@code true} if the caller may WARN now
     */
    boolean tryAcquire() {
      if (!throttle.tryAcquire()) {
        suppressed.incrementAndGet();
        return false;
      }
      return true;
    }

    /**
     * Drain the count of WARNs suppressed since the last emitted one.
     *
     * @return the suppressed occurrence count (counter resets to 0)
     */
    long drainSuppressed() {
      return suppressed.getAndSet(0);
    }
  }

  /**
   * Per-site rate limiters (fallback, cache condition, TTL SpEL, preload SpEL).
   */
  private final RateLimitedWarn fallbackWarn = new RateLimitedWarn();
  private final RateLimitedWarn cacheConditionWarn = new RateLimitedWarn();
  private final RateLimitedWarn ttlSpelWarn = new RateLimitedWarn();
  private final RateLimitedWarn preloadSpelWarn = new RateLimitedWarn();

  /**
   * Emit a per-invocation WARN at most once per {@link LogThrottle#DEFAULT_WINDOW_MS}
   * window; occurrences inside the window are logged at DEBUG and counted.
   * When the window's WARN fires, the suppressed count is appended.
   *
   * @param limiter the per-site limiter
   * @param message the log message (SLF4J format, one placeholder per arg)
   * @param args    the message arguments
   */
  @SuppressWarnings("all")
  private void warnRateLimited(RateLimitedWarn limiter, String message, Object... args) {
    if (limiter.tryAcquire()) {
      long suppressedCount = limiter.drainSuppressed();
      if (suppressedCount > 0) {
        Object[] extended = Arrays.copyOf(args, args.length + 1);
        extended[args.length] = suppressedCount;
        log.warn("{} ({} further occurrence(s) suppressed in the last 10s)", message, extended);
      } else {
        log.warn(message, args);
      }
    } else {
      log.debug(message + " (rate-limited)", args);
    }
  }

  /**
   * Aggregates all cache-extension annotations found on a method
   * (and its declaring class) for quick access.
   */
  private record AnnotationSet(
    CacheTTL ttl,
    Intercept intercept,
    Fallback fallback,
    NullCaching nullCaching,
    SkipBroadcast skipBroadcast,
    CacheCondition cacheCondition
  ) {}

  /**
   * Method-level cache for the resolved {@link AnnotationSet}.
   */
  private final Map<Method, AnnotationSet> annotationCache = new ConcurrentHashMap<>();

  /**
   * Methods whose {@code @Cacheable} annotation combination has already been
   * validated (and warned about if problematic). Guards the once-per-method
   * WARN contract of the lazy combination validator.
   */
  private final Set<Method> validatedReadMethods = ConcurrentHashMap.newKeySet();

  /**
   * Methods whose {@code @CachePut}/{@code @CacheEvict} annotation combination
   * has already been validated. See {@link #validatedReadMethods}.
   */
  private final Set<Method> validatedWriteMethods = ConcurrentHashMap.newKeySet();

  /**
   * Token-bucket based QPS rate limiters, one per cache key. Entries idle for
   * 5 minutes expire (mirroring {@link #qpsBlockTable}'s TTL): the cardinality
   * is user-key-driven, and without a TTL a one-time burst of distinct keys
   * would pin up to {@code maximumSize} bucket instances for the process
   * lifetime — the size cap alone only trims on the boundary crossing. An
   * active key refreshes its access on every intercepted request, so the TTL
   * never disturbs live limiting; an idle key simply starts from a fresh
   * bucket, which is the correct state for a quiet key.
   */
  private final Cache<String, Bucket> qpsBuckets = Caffeine.newBuilder()
    .expireAfterAccess(QPS_BOOKKEEPING_EXPIRY)
    .maximumSize(100_000)
    .build();

  /**
   * QPS block table: cache key → absolute unblock timestamp (millis).
   * Used together with {@link Intercept#blockDurationMs()} to enforce a
   * mandatory cooling-off period after a QPS breach. The Caffeine TTL
   * (5 minutes) is a safety net — the actual block duration is driven by
   * the stored timestamp comparison.
   */
  private final Cache<String, Long> qpsBlockTable = Caffeine.newBuilder()
    .expireAfterWrite(QPS_BOOKKEEPING_EXPIRY)
    .maximumSize(100_000)
    .build();

  /**
   * Atomic counters for tracking concurrent thread usage per cache key.
   */
  private final ConcurrentHashMap<String, AtomicInteger> concurrentCounters = new ConcurrentHashMap<>();

  /**
   * Constructs the aspect with the required dependencies.
   *
   * <p>The configuration is consumed here, once: only the derived
   * {@link #keySeparator} is retained, so a later mutation of the mutable
   * {@link ZetaProperties} bean cannot make this aspect disagree with
   * {@code ZetaSpringCache} about the key layout.
   *
   * @param zeta       the core cache engine
   * @param properties the configuration properties
   */
  public CacheExtensionAspect(Zeta zeta, ZetaProperties properties) {
    this.zeta = zeta;
    this.keySeparator = properties.getSpringCache().getKeySeparator();
  }

  /**
   * Intercepts methods annotated with {@link Cacheable} to apply extended
   * cache semantics.
   * <p>
   * The advice performs the following steps:
   * <ol>
   * <li>Resolve the target method, cache name and key.</li>
   * <li>Collect all relevant extension annotations and validate their
   * combination (WARN once per method).</li>
   * <li>Register preload keys if a {@link Preload} annotation is present.</li>
   * <li>Apply interception logic ({@link Intercept}) – may return a
   * fallback value before the actual method is called. Every
   * interception feeds the local TopK detector (without reporting to
   * the Worker) so intercepted hot keys cannot flap.</li>
   * <li>Build the immutable {@link CachePolicy} (lazy TTLs, null-caching,
   * broadcast flag) and push it into {@link ZetaCacheContext}.</li>
   * <li>Proceed with the original method invocation.</li>
   * <li>Optionally invalidate the cache entry if a {@link CacheCondition}
   * is not met after the invocation (purge semantics; the invalidation
   * is broadcast unless {@link SkipBroadcast} is present).</li>
   * <li>On exception, attempt fallback via {@link Fallback} or a dedicated
   * fallback method.</li>
   * </ol>
   *
   * @param pjp the join point representing the intercepted call
   * @return the result of the cached invocation or a fallback value
   * @throws Throwable if no fallback is configured and the original invocation
   *                   fails
   */
  @Around("@annotation(org.springframework.cache.annotation.Cacheable)")
  @SuppressWarnings("all")
  public Object aroundCacheable(ProceedingJoinPoint pjp) throws Throwable {
    Method method = resolveMethod(pjp);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    if (cacheable == null) return pjp.proceed();

    String cacheName = resolveCacheName(cacheable);
    String prefixedKey = cacheName + keySeparator + resolveKey(pjp, cacheable.key(), method);

    Preload preload = resolvePreloadAnnotation(method);
    AnnotationSet ann = resolveAnnotations(method);

    CacheTTL ttl = ann.ttl();
    Intercept intercept = ann.intercept();
    Fallback fallback = ann.fallback();
    NullCaching nullCaching = ann.nullCaching();
    SkipBroadcast skipBroadcast = ann.skipBroadcast();
    CacheCondition cacheCondition = ann.cacheCondition();

    validateReadCombination(method, cacheable, ann);

    // Register preload keys so the local detector starts tracking them early.
    if (preload != null) {
      handlePreload(preload, pjp, cacheName, method);
    }

    boolean needsDecrement = false;
    if (intercept != null) {
      String interceptFallback = intercept.fallback();
      switch (intercept.type()) {
        case FORCE -> {
          // Force a fallback without even calling the original method.
          notifyDetectorOnIntercept(prefixedKey);
          return resolveInterceptFallback(pjp, fallback, interceptFallback, prefixedKey, method);
        }
        case IS_LOCAL_HOT -> {
          // If the local detector has identified the key as a hot key, fall back.
          if (zeta.isLocalHotKey(prefixedKey)) {
            notifyDetectorOnIntercept(prefixedKey);
            return resolveInterceptFallback(pjp, fallback, interceptFallback, prefixedKey, method);
          }
        }
        case QPS -> {
          int qpsThreshold = intercept.qps().threshold();
          long blockMs = intercept.qps().blockDurationMs();
          if (qpsThreshold > 0) {
            // Layer 1: block table — fast reject without consuming tokens
            if (blockMs > 0) {
              Long unblockTime = qpsBlockTable.getIfPresent(prefixedKey);
              if (unblockTime != null) {
                if (TimeSource.monotonicMillis() < unblockTime) {
                  notifyDetectorOnIntercept(prefixedKey);
                  return resolveInterceptFallback(pjp, fallback, interceptFallback, prefixedKey, method);
                }
                // Block expired — evict and fall through to tryConsume
                qpsBlockTable.invalidate(prefixedKey);
              }
            }
            // Layer 2: token bucket — normal rate limiting
            Bucket bucket = qpsBuckets.get(prefixedKey, k ->
              Bucket.builder()
                .addLimit(
                  Bandwidth.builder().capacity(qpsThreshold).refillGreedy(qpsThreshold, Duration.ofSeconds(1)).build()
                )
                .build()
            );
            if (!bucket.tryConsume(1)) {
              // First breach → enter block table if configured
              if (blockMs > 0) {
                qpsBlockTable.put(prefixedKey, TimeSource.monotonicMillis() + blockMs);
              }
              notifyDetectorOnIntercept(prefixedKey);
              return resolveInterceptFallback(pjp, fallback, interceptFallback, prefixedKey, method);
            }
          }
        }
        case CONCURRENT_THREADS -> {
          // Limit the number of concurrent threads executing the original method for this
          // key.
          int maxThreads = intercept.concurrent().threshold();
          if (maxThreads > 0) {
            AtomicInteger counter = concurrentCounters.computeIfAbsent(prefixedKey, k -> new AtomicInteger(0));
            if (counter.incrementAndGet() > maxThreads) {
              // Exceeded the limit; decrement immediately and fall back.
              concurrentCounters.computeIfPresent(prefixedKey, (k, v) -> {
                int after = v.decrementAndGet();
                return after == 0 ? null : v;
              });
              notifyDetectorOnIntercept(prefixedKey);
              return resolveInterceptFallback(pjp, fallback, interceptFallback, prefixedKey, method);
            }
            needsDecrement = true; // must decrement in finally block
          }
        }
      }
    }

    boolean nullCachingEnabled = nullCaching == null || nullCaching.value();
    boolean skipBroadcastFlag = skipBroadcast != null;

    // Save the current context so we can restore it after the invocation.
    CachePolicy prev = ZetaCacheContext.get().snapshot();
    try {
      // Push the resolved policy. TTL suppliers stay lazy: SpEL expressions
      // are evaluated at most once, and only on miss / promotion / refresh —
      // never on a plain cache hit. The policy is pushed unconditionally so
      // nested @Cacheable invocations never observe an outer method's policy.
      ZetaCacheContext.get().push(buildPolicy(ttl, nullCachingEnabled, skipBroadcastFlag, pjp, method));

      Object result = pjp.proceed();

      // @CacheCondition: purge semantics — evaluate after proceed and evict
      // the freshly stored entry (plus any stale one) when the condition
      // holds. The invalidation is broadcast unless @SkipBroadcast is present.
      if (cacheCondition != null && !cacheCondition.unless().isEmpty()) {
        boolean shouldSkip = evaluateCacheCondition(cacheCondition.unless(), pjp, method, result);
        if (shouldSkip) {
          zeta.invalidate(prefixedKey, CachePolicy.defaults().withSkipBroadcast(skipBroadcastFlag));
        }
      }

      return result;
    } catch (ZetaBlockedException blocked) {
      // A BLOCK rule is a cache-policy rejection (the key never reaches the
      // method), not a method failure — the @throws contract of every read API
      // applies, and a @Fallback must not override it (ADR-0067).
      throw blocked;
    } catch (Exception e) {
      // Deliberately Exception, not Throwable: JVM-level Errors (OOM,
      // StackOverflow) must propagate — swallowing them behind a fallback
      // hides fatal failures.
      if (fallback != null) {
        // Rate-limited: during a sustained outage this fires on every
        // invocation and would otherwise flood the log.
        warnRateLimited(
          fallbackWarn,
          "[HotKeyCacheExtension] fallback triggered for key={}, reason={}",
          prefixedKey,
          e.getMessage()
        );
        return resolveFallback(pjp, fallback, method);
      }
      throw e;
    } finally {
      // Cleanup: decrement concurrent counter if it was incremented,
      // and restore the previous thread-local context.
      if (needsDecrement) {
        concurrentCounters.computeIfPresent(prefixedKey, (k, v) -> {
          int after = v.decrementAndGet();
          return after == 0 ? null : v;
        });
      }
      ZetaCacheContext.get().restore(prev);
    }
  }

  /**
   * Build the immutable {@link CachePolicy} for this invocation. Static TTL
   * values and SpEL expressions are both wrapped as lazy suppliers; the
   * underlying cache resolves them at most once and never on a plain hit.
   *
   * @param ttl                the resolved {@link CacheTTL} (may be {@code null})
   * @param nullCachingEnabled whether {@code null} results may be cached
   * @param skipBroadcastFlag  whether cross-instance sync is suppressed
   * @param pjp                the join point (SpEL evaluation context)
   * @param method             the intercepted method
   * @return the policy for this invocation
   */
  private CachePolicy buildPolicy(
    CacheTTL ttl,
    boolean nullCachingEnabled,
    boolean skipBroadcastFlag,
    ProceedingJoinPoint pjp,
    Method method
  ) {
    LongSupplier hardSupplier =
      ttl == null ? () -> 0L : () -> resolveTtlValue(ttl.hardTtlMs(), ttl.hardTtlSpEl(), pjp, method);
    LongSupplier softSupplier =
      ttl == null ? () -> 0L : () -> resolveTtlValue(ttl.softTtlMs(), ttl.softTtlSpEl(), pjp, method);
    return new CachePolicy(hardSupplier, softSupplier, nullCachingEnabled, skipBroadcastFlag, StalePolicy.SOFT_REFRESH);
  }

  /**
   * Feed the local TopK detector when an {@link Intercept} rule triggers,
   * without reporting to the Worker. Keeps the local frequency sketch
   * accurate so an intercepted hot key sustains its hotness instead of
   * decaying out of the TopK and flapping between intercepted/non-intercepted
   * states.
   *
   * @param prefixedKey the fully qualified cache key
   */
  private void notifyDetectorOnIntercept(String prefixedKey) {
    try {
      zeta.notifyLocalDetector(prefixedKey);
    } catch (RuntimeException e) {
      log.debug("notifyLocalDetector failed for key={}: {}", prefixedKey, e.toString());
    }
  }

  /**
   * Lazy annotation-combination validator for {@code @Cacheable} methods.
   * Runs at most once per method and logs a WARN for each meaningless or
   * harmful combination (R1/R3/R4).
   *
   * @param method    the intercepted method
   * @param cacheable the Spring {@link Cacheable} annotation
   * @param ann       the aggregated extension annotations
   */
  private void validateReadCombination(Method method, Cacheable cacheable, AnnotationSet ann) {
    if (!validatedReadMethods.add(method)) {
      return;
    }
    if (
      ann.intercept() != null &&
      ann.intercept().type() == InterceptType.FORCE &&
      (ann.ttl() != null || ann.nullCaching() != null || ann.cacheCondition() != null)
    ) {
      log.warn(
        "[Zeta] {}: @Intercept(FORCE) makes @CacheTTL/@NullCaching/@CacheCondition no-ops — " +
          "the method body never executes and nothing is ever cached.",
        method
      );
    }
    if (method.getAnnotation(Tag.class) != null) {
      log.warn(
        "[Zeta] {}: @Tag and @Cacheable on the same method double-count the key in the hot-key detector " +
          "(both paths feed HeavyKeeper). Remove one of them, or set @Tag(skipDetection = true).",
        method
      );
    }
    if (
      ann.intercept() != null &&
      ann.intercept().qps().blockDurationMs() > 0 &&
      ann.intercept().type() != InterceptType.QPS
    ) {
      log.warn(
        "[Zeta] {}: @Intercept(blockDurationMs={}) only takes effect in QPS mode, current type={}",
        method,
        ann.intercept().qps().blockDurationMs(),
        ann.intercept().type()
      );
    }
    if (ann.cacheCondition() != null && !cacheable.unless().isEmpty()) {
      log.warn(
        "[Zeta] {}: both Spring @Cacheable(unless) and @CacheCondition are present — double condition " +
          "evaluation with different semantics (skip-write vs purge). Keep only one.",
        method
      );
    }
  }

  /**
   * Lazy annotation-combination validator for
   * {@code @CachePut}/{@code @CacheEvict}
   * methods. Runs at most once per method and warns when read-path
   * annotations are present but silently ignored (R2).
   *
   * @param method the intercepted method
   */
  private void validateWriteCombination(Method method) {
    if (!validatedWriteMethods.add(method)) {
      return;
    }
    boolean hasReadOnlyAnnotation =
      method.getAnnotation(CacheTTL.class) != null ||
      method.getAnnotation(Intercept.class) != null ||
      method.getAnnotation(Fallback.class) != null ||
      method.getAnnotation(NullCaching.class) != null ||
      method.getAnnotation(CacheCondition.class) != null ||
      method.getAnnotation(Preload.class) != null;
    if (hasReadOnlyAnnotation) {
      log.warn(
        "[Zeta] {}: read-path annotations (@CacheTTL/@Intercept/@Fallback/@NullCaching/@CacheCondition/@Preload) " +
          "are ignored on @CachePut/@CacheEvict methods; only @SkipBroadcast applies there.",
        method
      );
    }
  }

  /**
   * Evaluates the {@code unless} expression of {@link CacheCondition} against the
   * method invocation result. If the expression returns {@code true}, the cache
   * entry should be skipped/invalidated.
   *
   * @param unlessExpr the SpEL expression to evaluate
   * @param pjp        the join point
   * @param method     the intercepted method
   * @param result     the result of the original method invocation
   * @return {@code true} if the condition is met (cache should be skipped)
   */
  private boolean evaluateCacheCondition(String unlessExpr, ProceedingJoinPoint pjp, Method method, Object result) {
    try {
      StandardEvaluationContext ctx = buildEvaluationContext(pjp, method);
      ctx.setVariable("result", result);
      Expression expr = expressionCache.computeIfAbsent("cacheCondition_" + unlessExpr, parser::parseExpression);
      return Boolean.TRUE.equals(expr.getValue(ctx, Boolean.class));
    } catch (Exception e) {
      // Per-invocation evaluation — rate-limited so a persistently broken
      // expression cannot flood the log.
      warnRateLimited(
        cacheConditionWarn,
        "Failed to evaluate @CacheCondition unless='{}': {}",
        unlessExpr,
        e.toString()
      );
      return false;
    }
  }

  /**
   * Resolves a TTL value from either a static long or a SpEL expression.
   * The static value takes precedence if it is greater than 0.
   *
   * @param staticVal the static TTL value from the annotation
   * @param spelExpr  a SpEL expression that evaluates to a numeric value
   * @param pjp       the join point
   * @param method    the intercepted method
   * @return the resolved TTL in milliseconds, or 0 if unresolvable
   */
  private long resolveTtlValue(long staticVal, String spelExpr, ProceedingJoinPoint pjp, Method method) {
    if (staticVal > 0) return staticVal;
    if (spelExpr == null || spelExpr.isEmpty()) return 0L;
    try {
      Expression expr = expressionCache.computeIfAbsent("ttl_" + spelExpr, parser::parseExpression);
      Number val = expr.getValue(buildEvaluationContext(pjp, method), Number.class);
      return val != null ? val.longValue() : 0L;
    } catch (Exception e) {
      // Evaluated per miss/promotion/refresh — rate-limited so a persistently
      // broken expression cannot flood the log.
      warnRateLimited(ttlSpelWarn, "Failed to evaluate TTL SpEL '{}': {}", spelExpr, e.toString());
      return 0L;
    }
  }

  /**
   * Registers preload keys with the local detector so that they are
   * proactively recognized as hot (or pre-warmed) keys.
   *
   * <p>
   * <b>Per-op cost:</b> the static {@code keys} pass is deduped by the
   * {@link #registeredPreloadKeys} window, but the dynamic {@code keyExpr}
   * SpEL expression is evaluated on <b>every</b> invocation of the annotated
   * method (it is needed to compute the dedup key), so keep {@code keyExpr}
   * expressions cheap.
   *
   * @param preload   the {@link Preload} annotation instance
   * @param pjp       the join point
   * @param cacheName the cache name
   * @param method    the intercepted method
   */
  private void handlePreload(Preload preload, ProceedingJoinPoint pjp, String cacheName, Method method) {
    long preloadCount = preload.count() > 0 ? preload.count() : PRELOAD_UNLIMITED_COUNT;

    Map<String, Boolean> registeredKeys = new HashMap<>(preload.keys().length + 1);
    Map<String, Long> notifiedKeys = new HashMap<>(preload.keys().length + 1);
    // Static keys specified directly in the annotation.
    for (String staticKey : preload.keys()) {
      String fullKey = cacheName + keySeparator + staticKey;
      if (registeredPreloadKeys.getIfPresent(fullKey) == null) {
        registeredKeys.put(fullKey, Boolean.TRUE);
        notifiedKeys.put(fullKey, preloadCount);
      }
    }
    registeredPreloadKeys.putAll(registeredKeys);
    zeta.notifyLocalDetectorDirect(notifiedKeys);

    // Dynamic key resolved via SpEL expression.
    String keyExpr = preload.keyExpr();
    if (!keyExpr.isEmpty()) {
      try {
        Expression expression = expressionCache.computeIfAbsent("preload_" + keyExpr, k ->
          parser.parseExpression(keyExpr)
        );
        Object value = expression.getValue(buildEvaluationContext(pjp, method));
        if (value != null) {
          String fullKey = cacheName + keySeparator + value;
          if (registeredPreloadKeys.getIfPresent(fullKey) == null) {
            zeta.notifyLocalDetectorDirect(fullKey, preloadCount);
            registeredPreloadKeys.put(fullKey, Boolean.TRUE);
          }
        }
      } catch (Exception e) {
        // Per-invocation evaluation — rate-limited so a persistently broken
        // expression cannot flood the log.
        warnRateLimited(preloadSpelWarn, "Failed to evaluate @Preload keyExpr '{}': {}", keyExpr, e.toString());
      }
    }
  }

  /**
   * Intercepts methods annotated with {@link Tag} to feed the cache key into
   * the local HeavyKeeper sketch and optionally the Worker, without performing
   * a cache lookup.
   *
   * <p>
   * Controls detection and reporting via the {@link Tag#skipDetection} and
   * {@link Tag#skipReport} attributes respectively. When {@link Tag#cacheName}
   * is set, the resolved key is prefixed with {@code cacheName + keySeparator} so
   * it lands in the same key namespace as {@code @Cacheable} entries.
   *
   * @param pjp the join point
   * @return the result of the original method invocation
   * @throws Throwable if the underlying method throws
   */
  @Around("@annotation(io.github.hyshmily.zeta.annotation.Tag)")
  public Object aroundTag(ProceedingJoinPoint pjp) throws Throwable {
    Method method = resolveMethod(pjp);
    Tag tag = method.getAnnotation(Tag.class);
    if (tag == null) return pjp.proceed();

    String key = resolveKey(pjp, tag.value(), method);
    if (!tag.cacheName().isEmpty()) {
      key = tag.cacheName() + keySeparator + key;
    }

    zeta.tag(key, tag.skipDetection(), tag.skipReport());

    return pjp.proceed();
  }

  /**
   * Intercepts methods annotated with {@link CachePut} or {@link CacheEvict}
   * and applies a potential {@link SkipBroadcast} flag so that cache updates
   * are not propagated unnecessarily.
   *
   * @param pjp the join point
   * @return the result of the original method invocation
   * @throws Throwable if the underlying method throws
   */
  @Around(
    "@annotation(org.springframework.cache.annotation.CachePut) || @annotation(org.springframework.cache.annotation.CacheEvict)"
  )
  public Object aroundCachePutOrEvict(ProceedingJoinPoint pjp) throws Throwable {
    return setSkipBroadcast(pjp);
  }

  /**
   * Checks whether the intercepted method carries a {@link SkipBroadcast}
   * annotation, and if so, sets the corresponding flag in the thread-local
   * {@link ZetaCacheContext} for the duration of the invocation. Read-path
   * annotations on the same method are validated (WARN once) since they are
   * silently ignored here.
   *
   * @param pjp the join point
   * @return the result of the original method invocation
   * @throws Throwable if the underlying method throws
   */
  private Object setSkipBroadcast(ProceedingJoinPoint pjp) throws Throwable {
    Method method = resolveMethod(pjp);
    SkipBroadcast skipBroadcast = method.getAnnotation(SkipBroadcast.class);
    boolean skipBroadcastFlag = skipBroadcast != null;
    validateWriteCombination(method);
    CachePolicy prev = ZetaCacheContext.get().snapshot();
    try {
      ZetaCacheContext.get().push(new CachePolicy(null, null, true, skipBroadcastFlag, StalePolicy.SOFT_REFRESH));
      return pjp.proceed();
    } finally {
      ZetaCacheContext.get().restore(prev);
    }
  }

  /**
   * Extracts the first cache name from the given {@link Cacheable} annotation.
   *
   * @param cacheable the annotation instance
   * @return the cache name, or {@code "zeta"} as a fallback
   */
  private String resolveCacheName(Cacheable cacheable) {
    String[] names = cacheable.cacheNames();
    if (names.length > 0) {
      return names[0];
    }
    String[] value = cacheable.value();
    if (value.length > 0) {
      return value[0];
    }
    return "zeta";
  }

  /**
   * Resolves the cache key for the intercepted method. If no SpEL expression
   * is provided, a {@link SimpleKey} is generated from the method arguments.
   *
   * @param pjp        the join point
   * @param expression the SpEL key expression (may be empty)
   * @param method     the intercepted method
   * @return the computed cache key as a string
   */
  private String resolveKey(ProceedingJoinPoint pjp, String expression, Method method) {
    if (expression.isEmpty()) {
      Object[] args = pjp.getArgs();
      return SIMPLE_KEY_GENERATOR.generate(pjp.getTarget(), method, args).toString();
    }
    return getExpression(expression).getValue(buildEvaluationContext(pjp, method), String.class);
  }

  /**
   * Resolves the actual target method, unwinding any interface-based proxies
   * that Spring AOP might have created.
   *
   * @param pjp the join point
   * @return the concrete method instance
   */
  private Method resolveMethod(ProceedingJoinPoint pjp) {
    MethodSignature sig = (MethodSignature) pjp.getSignature();
    Method method = sig.getMethod();
    if (method.getDeclaringClass().isInterface()) {
      try {
        method = pjp.getTarget().getClass().getMethod(method.getName(), method.getParameterTypes());
      } catch (NoSuchMethodException ignored) {
        // fall back to the interface method
      }
    }
    return method;
  }

  /**
   * Builds a SpEL {@link StandardEvaluationContext} populated with method
   * parameters, using cached parameter names to avoid repeated reflection.
   *
   * <p>Argument exposure follows the {@code MethodBasedEvaluationContext}
   * contract this aspect previously relied on: every argument is registered
   * under its synthetic {@code p{i}}/{@code a{i}} aliases AND under its
   * discovered parameter name, so expressions written in any of the three
   * styles ({@code #p0}, {@code #a0}, {@code #id}) resolve. Dropping the
   * aliases would silently turn a {@code #p0} key into {@code null}.
   *
   * @param pjp    the join point
   * @param method the intercepted method
   * @return a new evaluation context with args registered as SpEL variables
   */
  private StandardEvaluationContext buildEvaluationContext(ProceedingJoinPoint pjp, Method method) {
    StandardEvaluationContext ctx = new StandardEvaluationContext(pjp.getTarget());
    Object[] args = pjp.getArgs();
    for (int i = 0; i < args.length; i++) {
      ctx.setVariable("p" + i, args[i]);
      ctx.setVariable("a" + i, args[i]);
    }
    String[] paramNames = paramNameCache.computeIfAbsent(method, parameterNameDiscoverer::getParameterNames);
    if (paramNames != null) {
      for (int i = 0; i < paramNames.length && i < args.length; i++) {
        ctx.setVariable(paramNames[i], args[i]);
      }
    }
    return ctx;
  }

  /**
   * Retrieves a compiled SpEL {@link Expression} from the cache.
   *
   * @param expressionString the raw expression string
   * @return the compiled expression
   */
  private Expression getExpression(String expressionString) {
    return expressionCache.computeIfAbsent(expressionString, parser::parseExpression);
  }

  /**
   * Handles the fallback path triggered by an {@link Intercept} rule.
   * Priorities:
   * <ol>
   * <li>Use the {@code intercept.fallback()} SpEL expression if not blank.</li>
   * <li>Fall back to the method-level {@link Fallback} annotation.</li>
   * <li>Attempt to {@link Zeta#peek(String)} the currently cached value.</li>
   * </ol>
   *
   * @param pjp               the join point
   * @param fallback          the method-level fallback annotation (may be null)
   * @param interceptFallback the SpEL expression from
   *                          {@code @Intercept.fallback()}
   * @param prefixedKey       the fully qualified cache key
   * @param method            the intercepted method
   * @return the fallback value
   * @throws Throwable if no fallback can be resolved and the peeked value is null
   */
  private Object resolveInterceptFallback(
    ProceedingJoinPoint pjp,
    Fallback fallback,
    String interceptFallback,
    String prefixedKey,
    Method method
  ) throws Throwable {
    if (!interceptFallback.isBlank()) {
      return getExpression(interceptFallback).getValue(buildEvaluationContext(pjp, method));
    }
    if (fallback != null) {
      return resolveFallback(pjp, fallback, method);
    }
    return zeta.peek(prefixedKey).orElse(null);
  }

  /**
   * Resolves a fallback value from the {@link Fallback} annotation:
   * <ul>
   * <li>If {@link Fallback#value()} is provided, it is evaluated as a SpEL
   * expression.</li>
   * <li>Otherwise, a convention-based fallback method is invoked
   * (named {@code <methodName>Fallback}).</li>
   * </ul>
   *
   * @param pjp      the join point
   * @param fallback the fallback annotation
   * @param method   the intercepted method
   * @return the fallback value
   * @throws Throwable if the fallback method throws
   */
  private Object resolveFallback(ProceedingJoinPoint pjp, Fallback fallback, Method method) throws Throwable {
    if (!fallback.value().isEmpty()) {
      return getExpression(fallback.value()).getValue(buildEvaluationContext(pjp, method));
    }
    return invokeFallbackMethod(pjp);
  }

  /**
   * Invokes a convention-based fallback method. The fallback method must have
   * the same signature as the original method and be named
   * {@code <originalMethodName>Fallback}.
   *
   * @param pjp the join point
   * @return the result of the fallback method, or {@code null} if none is found
   * @throws Throwable if the fallback method throws an exception
   */
  @SuppressWarnings("all")
  private Object invokeFallbackMethod(ProceedingJoinPoint pjp) throws Throwable {
    Method originalMethod = resolveMethod(pjp);
    Object target = pjp.getTarget();
    Object[] args = pjp.getArgs();

    Method fallbackMethod = fallbackMethodCache.computeIfAbsent(originalMethod, m ->
      findFallbackMethod(target.getClass(), m.getName() + "Fallback", m.getParameterTypes())
    );

    if (fallbackMethod == null) {
      return null;
    }

    try {
      if (!fallbackMethod.canAccess(target)) {
        fallbackMethod.setAccessible(true);
      }
      return fallbackMethod.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  /**
   * Recursively searches for a fallback method in the given class and its
   * superclasses.
   *
   * @param clazz      the class to start searching from
   * @param name       the desired method name
   * @param paramTypes the parameter types
   * @return the method, or {@code null} if not found
   */
  private Method findFallbackMethod(Class<?> clazz, String name, Class<?>... paramTypes) {
    try {
      return clazz.getMethod(name, paramTypes);
    } catch (NoSuchMethodException e) {
      Class<?> superclass = clazz.getSuperclass();
      if (superclass != null && superclass != Object.class) {
        return findFallbackMethod(superclass, name, paramTypes);
      }
      return null;
    }
  }

  /**
   * Retrieves the {@link Preload} annotation for the given method, caching
   * the result.
   *
   * @param method the method
   * @return the annotation instance, or {@code null}
   */
  private Preload resolvePreloadAnnotation(Method method) {
    return preloadCache.computeIfAbsent(method, m -> m.getAnnotation(Preload.class));
  }

  /**
   * Aggregates all cache-extension annotations present on the given method.
   * Class-level {@link CacheTTL} annotations are also
   * taken into account if not already found at the method level.
   *
   * @param method the method
   * @return an {@link AnnotationSet} containing the resolved annotations
   */
  @SuppressWarnings("all")
  private AnnotationSet resolveAnnotations(Method method) {
    return annotationCache.computeIfAbsent(method, m -> {
      CacheTTL ttl = null;
      Intercept intercept = null;
      Fallback fallback = null;
      NullCaching nullCaching = null;
      SkipBroadcast skipBroadcast = null;
      CacheCondition cacheCondition = null;

      for (Annotation a : m.getDeclaredAnnotations()) {
        if (a instanceof CacheTTL t) {
          ttl = t;
        } else if (a instanceof Intercept i) {
          intercept = i;
        } else if (a instanceof Fallback f) {
          fallback = f;
        } else if (a instanceof NullCaching n) {
          nullCaching = n;
        } else if (a instanceof SkipBroadcast s) {
          skipBroadcast = s;
        } else if (a instanceof CacheCondition c) {
          cacheCondition = c;
        }
      }

      // Fall back to class-level annotations for TTL.
      if (ttl == null) {
        ttl = m.getDeclaringClass().getAnnotation(CacheTTL.class);
      }

      return new AnnotationSet(ttl, intercept, fallback, nullCaching, skipBroadcast, cacheCondition);
    });
  }
}
