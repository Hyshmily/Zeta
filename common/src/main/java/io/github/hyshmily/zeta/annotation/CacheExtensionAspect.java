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
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.annotationsupporter.ZetaCacheContext;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.springframework.context.expression.MethodBasedEvaluationContext;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.Ordered;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.core.annotation.Order;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * Companion AOP aspect for Spring {@link Cacheable @Cacheable},
 * {@link CachePut @CachePut}, and {@link CacheEvict @CacheEvict} methods.
 * <p>
 * This aspect extends the default Spring Cache behavior by injecting
 * additional cache-control metadata (TTL, interception policies, fallback
 * logic, null-caching rules, broadcast skipping, hot-key handling, and
 * conditional caching). It acts as a bridge between the annotation-driven
 * configuration and the underlying {@link Zeta} distributed cache infrastructure.
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
   * Global configuration properties, including key separators and defaults.
   */
  private final ZetaProperties properties;

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
   * Cache of resolved fallback methods keyed by the original method.
   * Reduces reflection overhead on repeated fallback calls.
   */
  private final Map<Method, Method> fallbackMethodCache = new ConcurrentHashMap<>();

  /**
   * Lightweight Caffeine cache that tracks which preload keys have already
   * been registered with the local detector. Prevents duplicate registrations
   * for keys that are repeatedly processed (e.g., under high concurrency).
   */
  private final Cache<String, Boolean> registeredPreloadKeys = Caffeine.newBuilder()
    .maximumSize(100_000)
    .expireAfterWrite(1, TimeUnit.HOURS)
    .build();

  /**
   * Method-level cache for {@link Preload} annotations.
   */
  private final Map<Method, Preload> preloadCache = new ConcurrentHashMap<>();

  private static final SimpleKeyGenerator SIMPLE_KEY_GENERATOR = new SimpleKeyGenerator();

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
    SkipDetection skipDetection,
    HotTTL hotTtl,
    CacheCondition cacheCondition
  ) {}

  /**
   * Method-level cache for the resolved {@link AnnotationSet}.
   */
  private final Map<Method, AnnotationSet> annotationCache = new ConcurrentHashMap<>();

  /**
   * Token-bucket based QPS rate limiters, one per cache key.
   */
  private final Cache<String, Bucket> qpsBuckets = Caffeine.newBuilder().maximumSize(100_000).build();

  /**
   * Atomic counters for tracking concurrent thread usage per cache key.
   */
  private final ConcurrentHashMap<String, AtomicInteger> concurrentCounters = new ConcurrentHashMap<>();

  /**
   * Constructs the aspect with the required dependencies.
   *
   * @param zeta       the core cache engine
   * @param properties the configuration properties
   */
  public CacheExtensionAspect(Zeta zeta, ZetaProperties properties) {
    this.zeta = zeta;
    this.properties = properties;
  }

  /**
   * Intercepts methods annotated with {@link Cacheable} to apply extended
   * cache semantics.
   * <p>
   * The advice performs the following steps:
   * <ol>
   *   <li>Resolve the target method, cache name and key.</li>
   *   <li>Collect all relevant extension annotations.</li>
   *   <li>Register preload keys if a {@link Preload} annotation is present.</li>
   *   <li>Apply interception logic ({@link Intercept}) – may return a
   *       fallback value before the actual method is called.</li>
   *   <li>Compute hard/soft TTL values and hot-key TTL (static or SpEL).</li>
   *   <li>Push cache-control flags into {@link ZetaCacheContext}.</li>
   *   <li>Proceed with the original method invocation.</li>
   *   <li>Optionally invalidate the cache entry if a {@link CacheCondition}
   *       is not met after the invocation.</li>
   *   <li>On exception, attempt fallback via {@link Fallback} or a dedicated
   *       fallback method.</li>
   * </ol>
   *
   * @param pjp       the join point representing the intercepted call
   * @param cacheable the source {@code @Cacheable} annotation
   * @return the result of the cached invocation or a fallback value
   * @throws Throwable if no fallback is configured and the original invocation fails
   */
  @Around("@annotation(cacheable)")
  @SuppressWarnings("all")
  public Object aroundCacheable(ProceedingJoinPoint pjp, Cacheable cacheable) throws Throwable {
    Method method = resolveMethod(pjp);
    String cacheName = resolveCacheName(cacheable);
    String key = resolveKey(pjp, cacheable.key(), method);
    String prefixedKey = cacheName + properties.getSpringCache().getKeySeparator() + key;

    Preload preload = resolvePreloadAnnotation(method);
    AnnotationSet ann = resolveAnnotations(method);

    CacheTTL ttl = ann.ttl();
    Intercept intercept = ann.intercept();
    Fallback fallback = ann.fallback();
    NullCaching nullCaching = ann.nullCaching();
    SkipBroadcast skipBroadcast = ann.skipBroadcast();
    SkipDetection skipDetection = ann.skipDetection();
    HotTTL hotTtl = ann.hotTtl();
    CacheCondition cacheCondition = ann.cacheCondition();

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
          return resolveInterceptFallback(pjp, fallback, interceptFallback, prefixedKey, method);
        }
        case IS_LOCAL_HOT -> {
          // If the local detector has identified the key as a hot key, fall back.
          if (zeta.isLocalHotKey(prefixedKey)) {
            return resolveInterceptFallback(pjp, fallback, interceptFallback, prefixedKey, method);
          }
        }
        case QPS -> {
          // Token-bucket rate limiting per key.
          int qpsThreshold = intercept.qps();
          if (qpsThreshold > 0) {
            Bucket bucket = qpsBuckets.get(prefixedKey, k ->
              Bucket.builder()
                .addLimit(
                  Bandwidth.builder().capacity(qpsThreshold).refillGreedy(qpsThreshold, Duration.ofSeconds(1)).build()
                )
                .build()
            );
            if (!bucket.tryConsume(1)) {
              return resolveInterceptFallback(pjp, fallback, interceptFallback, prefixedKey, method);
            }
          }
        }
        case CONCURRENT_THREADS -> {
          // Limit the number of concurrent threads executing the original method for this key.
          int maxThreads = intercept.concurrentThreads();
          if (maxThreads > 0) {
            AtomicInteger counter = concurrentCounters.computeIfAbsent(prefixedKey, k -> new AtomicInteger(0));
            if (counter.incrementAndGet() > maxThreads) {
              // Exceeded the limit; decrement immediately and fall back.
              concurrentCounters.computeIfPresent(prefixedKey, (k, v) -> {
                int after = v.decrementAndGet();
                return after == 0 ? null : v;
              });
              return resolveInterceptFallback(pjp, fallback, interceptFallback, prefixedKey, method);
            }
            needsDecrement = true; // must decrement in finally block
          }
        }
      }
    }

    // Resolve TTL values: static first, then SpEL fallback
    long hardTtlMs = resolveTtlValue(
      ttl != null ? ttl.hardTtlMs() : 0L,
      ttl != null ? ttl.hardTtlSpEl() : "",
      pjp,
      method
    );
    long softTtlMs = resolveTtlValue(
      ttl != null ? ttl.softTtlMs() : 0L,
      ttl != null ? ttl.softTtlSpEl() : "",
      pjp,
      method
    );

    long hotHardTtlMs = hotTtl != null ? hotTtl.hardTtlMs() : 0L;
    long hotSoftTtlMs = hotTtl != null ? hotTtl.softTtlMs() : 0L;

    boolean allowNull = nullCaching != null && nullCaching.value();
    boolean skipBroadcastFlag = skipBroadcast != null;
    boolean skipDetectionFlag = skipDetection != null;

    // Save the current context so we can restore it after the invocation.
    ZetaCacheContext.ContextValues prev = ZetaCacheContext.get().snapshot();
    try {
      // Push the resolved cache-control metadata into the thread-local context.
      ZetaCacheContext.get().apply(
        hardTtlMs,
        softTtlMs,
        allowNull,
        skipBroadcastFlag,
        hotHardTtlMs,
        hotSoftTtlMs,
        skipDetectionFlag
      );

      Object result = pjp.proceed();

      // @CacheCondition: evaluate after proceed; invalidate the entry if condition fails.
      if (cacheCondition != null && !cacheCondition.unless().isEmpty()) {
        boolean shouldSkip = evaluateCacheCondition(cacheCondition.unless(), pjp, method, result);
        if (shouldSkip) {
          zeta.invalidate(prefixedKey, false);
        }
      }

      return result;
    } catch (Throwable e) {
      if (fallback != null) {
        log.warn("[HotKeyCacheExtension] fallback triggered for key={}, reason={}", prefixedKey, e.getMessage());
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
      MethodBasedEvaluationContext ctx = new MethodBasedEvaluationContext(
        pjp.getTarget(),
        method,
        pjp.getArgs(),
        parameterNameDiscoverer
      );
      ctx.setVariable("result", result);
      Expression expr = expressionCache.computeIfAbsent("cacheCondition_" + unlessExpr, parser::parseExpression);
      return Boolean.TRUE.equals(expr.getValue(ctx, Boolean.class));
    } catch (Exception e) {
      log.warn("Failed to evaluate @CacheCondition unless='{}': {}", unlessExpr, e.toString());
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
      log.warn("Failed to evaluate TTL SpEL '{}': {}", spelExpr, e.toString());
      return 0L;
    }
  }

  /**
   * Registers preload keys with the local detector so that they are
   * proactively recognized as hot (or pre-warmed) keys.
   *
   * @param preload   the {@link Preload} annotation instance
   * @param pjp       the join point
   * @param cacheName the cache name
   * @param method    the intercepted method
   */
  private void handlePreload(Preload preload, ProceedingJoinPoint pjp, String cacheName, Method method) {
    String separator = properties.getSpringCache().getKeySeparator();
    long preloadCount = preload.count() > 0 ? preload.count() : Long.MAX_VALUE;

    Map<String, Boolean> registeredKeys = new HashMap<>(preload.keys().length + 1);
    Map<String, Long> notifiedKeys = new HashMap<>(preload.keys().length + 1);
    // Static keys specified directly in the annotation.
    for (String staticKey : preload.keys()) {
      String fullKey = cacheName + separator + staticKey;
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
          String fullKey = cacheName + separator + value;
          if (registeredPreloadKeys.getIfPresent(fullKey) == null) {
            zeta.notifyLocalDetectorDirect(fullKey, preloadCount);
            registeredPreloadKeys.put(fullKey, Boolean.TRUE);
          }
        }
      } catch (Exception e) {
        log.warn("Failed to evaluate @Preload keyExpr '{}': {}", keyExpr, e.toString());
      }
    }
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
  @SuppressWarnings("unused")
  @Around(
    "@annotation(org.springframework.cache.annotation.CachePut) || @annotation(org.springframework.cache.annotation.CacheEvict)"
  )
  public Object aroundCachePutOrEvict(ProceedingJoinPoint pjp) throws Throwable {
    return setSkipBroadcast(pjp);
  }

  /**
   * Checks whether the intercepted method carries a {@link SkipBroadcast}
   * annotation, and if so, sets the corresponding flag in the thread-local
   * {@link ZetaCacheContext} for the duration of the invocation.
   *
   * @param pjp the join point
   * @return the result of the original method invocation
   * @throws Throwable if the underlying method throws
   */
  private Object setSkipBroadcast(ProceedingJoinPoint pjp) throws Throwable {
    Method method = resolveMethod(pjp);
    SkipBroadcast skipBroadcast = method.getAnnotation(SkipBroadcast.class);
    boolean skipBroadcastFlag = skipBroadcast != null;
    ZetaCacheContext.ContextValues prev = ZetaCacheContext.get().snapshot();
    try {
      ZetaCacheContext.get().apply(0, 0, false, skipBroadcastFlag, 0, 0, false);
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
   * Builds a SpEL {@link EvaluationContext} populated with method parameters.
   *
   * @param pjp    the join point
   * @param method the method
   * @return a new evaluation context
   */
  private EvaluationContext buildEvaluationContext(ProceedingJoinPoint pjp, Method method) {
    return new MethodBasedEvaluationContext(pjp.getTarget(), method, pjp.getArgs(), parameterNameDiscoverer);
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
   *   <li>Use the {@code intercept.fallback()} SpEL expression if not blank.</li>
   *   <li>Fall back to the method-level {@link Fallback} annotation.</li>
   *   <li>Attempt to {@link Zeta#peek(String)} the currently cached value.</li>
   * </ol>
   *
   * @param pjp               the join point
   * @param fallback          the method-level fallback annotation (may be null)
   * @param interceptFallback the SpEL expression from {@code @Intercept.fallback()}
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
   *   <li>If {@link Fallback#value()} is provided, it is evaluated as a SpEL expression.</li>
   *   <li>Otherwise, a convention-based fallback method is invoked
   *       (named {@code <methodName>Fallback}).</li>
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
   * Class-level {@link CacheTTL} and {@link HotTTL} annotations are also
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
      SkipDetection skipDetection = null;
      HotTTL hotTtl = null;
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
        } else if (a instanceof SkipDetection s) {
          skipDetection = s;
        } else if (a instanceof HotTTL h) {
          hotTtl = h;
        } else if (a instanceof CacheCondition c) {
          cacheCondition = c;
        }
      }

      // Fall back to class-level annotations for TTL and hot TTL.
      if (ttl == null) {
        ttl = m.getDeclaringClass().getAnnotation(CacheTTL.class);
      }
      if (hotTtl == null) {
        hotTtl = m.getDeclaringClass().getAnnotation(HotTTL.class);
      }

      return new AnnotationSet(
        ttl,
        intercept,
        fallback,
        nullCaching,
        skipBroadcast,
        skipDetection,
        hotTtl,
        cacheCondition
      );
    });
  }
}
