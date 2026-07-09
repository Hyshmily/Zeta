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
package io.github.hyshmily.hotkey.annotation;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.Internal;
import io.github.hyshmily.hotkey.autoconfigure.HotKeyProperties;
import io.github.hyshmily.hotkey.cache.annotationsupporter.HotKeyCacheContext;
import io.github.hyshmily.hotkey.util.window.RollingWindow;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
 *
 * <p>Runs at {@link Ordered#HIGHEST_PRECEDENCE} — before Spring's own
 * {@code CacheInterceptor} — to set up thread-bound TTL overrides,
 * null-caching mode, send suppression, and apply
 * {@link Intercept @Intercept} / {@link Fallback @Fallback} semantics.
 *
 * <p>For each {@code @Cacheable} invocation:
 * <ol>
 *   <li>Resolves the cache key from SpEL</li>
 *   <li>If {@code @Intercept} is present and the key is a local hot key,
 *       skips the method and returns the cached value or fallback</li>
 *   <li>Applies TTL from {@code @HotKeyCacheTTL}, null-caching from
 *       {@code @NullCaching}, and send suppression from
 *       {@link Broadcast @Broadcast} into {@link HotKeyCacheContext}</li>
 *   <li>Proceeds to the Spring {@code CacheInterceptor}</li>
 *   <li>Restores the context snapshot in a {@code finally} block</li>
 *   <li>If the method throws and {@code @Fallback} is present, returns the fallback</li>
 * </ol>
 *
 * <p>For {@code @CachePut} and {@code @CacheEvict}, the aspect only reads
 * {@link Broadcast @Broadcast} to set the send suppression flag. All other
 * companion annotations ({@code @HotKeyCacheTTL}, {@code @Intercept},
 * {@code @Fallback}, {@code @NullCaching}) are ignored on these methods.
 */
@Internal
@Aspect
@Order(Ordered.HIGHEST_PRECEDENCE)
@Slf4j
public class HotKeyCacheExtensionAspect {

  /** The HotKey facade for cache introspection. */
  private final HotKey hotKey;

  /** Configuration properties (for key separator). */
  private final HotKeyProperties properties;

  /** SpEL expression parser, shared across all evaluations. */
  private final SpelExpressionParser parser = new SpelExpressionParser();

  /** Discoverer for resolving method parameter names at runtime. */
  private final ParameterNameDiscoverer parameterNameDiscoverer = new DefaultParameterNameDiscoverer();

  /** Cache of parsed SpEL expressions keyed by expression string. */
  private final Map<String, Expression> expressionCache = new ConcurrentHashMap<>();

  /** Cache of resolved naming-convention fallback methods, keyed by original Method. */
  private final Map<Method, Method> fallbackMethodCache = new ConcurrentHashMap<>();

  /**
   * Tracks which keys have already been preload-registered to avoid redundant
   * {@link HotKey#notifyLocalDetectorDirect(String, int)} calls.
   * Bounded at 100k entries with 1-hour TTL to prevent unbounded growth.
   */
  private final Cache<String, Boolean> registeredPreloadKeys = Caffeine.newBuilder()
    .maximumSize(100_000)
    .expireAfterWrite(1, TimeUnit.HOURS)
    .build();

  /** Cache of resolved companion annotations keyed by Method. */
  private final Map<Method, HotKeyPreload> preloadCache = new ConcurrentHashMap<>();

  private record AnnotationSet(
    HotKeyCacheTTL ttl,
    Intercept intercept,
    Fallback fallback,
    NullCaching nullCaching,
    Broadcast broadcast
  ) {}

  private final Map<Method, AnnotationSet> annotationCache = new ConcurrentHashMap<>();

  /**
   * Per-key QPS sliding windows for {@link InterceptTrigger#QPS} interception.
   * Each key gets a 10-bucket / 1-second window.
   * Bounded at 100k entries; idle windows are evicted by Caffeine.
   */
  private final Cache<String, RollingWindow> qpsWindows = Caffeine.newBuilder().maximumSize(100_000).build();

  /**
   * Per-key concurrent thread counters for {@link InterceptTrigger#CONCURRENT_THREADS} interception.
   * Incremented before method execution, decremented in {@code finally}.
   */
  private final ConcurrentHashMap<String, AtomicInteger> concurrentCounters = new ConcurrentHashMap<>();

  /**
   * Creates a new {@code HotKeyCacheExtensionAspect}.
   *
   * @param hotKey     the HotKey facade
   * @param properties configuration properties
   */
  public HotKeyCacheExtensionAspect(HotKey hotKey, HotKeyProperties properties) {
    this.hotKey = hotKey;
    this.properties = properties;
  }

  /**
   * Intercepts {@link Cacheable @Cacheable} methods to apply TTL overrides, intercept
   * logic, and fallback handling before the method reaches Spring's {@code CacheInterceptor}.
   *
   * <p>If {@code @Intercept} is present and the key is a local hot key, the method is
   * skipped entirely. If a {@code @Fallback} is provided, it is returned instead.
   * If the method throws, the exception is caught and the fallback is returned.
   *
   * @param pjp       the join point for the intercepted method
   * @param cacheable the {@code @Cacheable} annotation on the intercepted method
   * @return the cached or method return value
   */
  @Around("@annotation(cacheable)")
  public Object aroundCacheable(ProceedingJoinPoint pjp, Cacheable cacheable) throws Throwable {
    Method method = resolveMethod(pjp);
    String cacheName = resolveCacheName(cacheable);
    String key = resolveKey(pjp, cacheable.key(), method);
    String prefixedKey = cacheName + properties.getSpringCache().getKeySeparator() + key;

    HotKeyPreload preload = resolvePreloadAnnotation(method);
    AnnotationSet ann = resolveAnnotations(method);

    HotKeyCacheTTL ttl = ann.ttl();
    Intercept intercept = ann.intercept();
    Fallback fallback = ann.fallback();
    NullCaching nullCaching = ann.nullCaching();
    Broadcast broadcast = ann.broadcast();

    // @HotKeyPreload: inflate detection counts after @Intercept (so first call loads normally)
    // but before context setup (so pjp.proceed() → CacheInterceptor sees inflated counts)
    if (preload != null) {
      handlePreload(preload, pjp, cacheName, method);
    }

    // @Intercept: skip method when key is a local hot key if necessary
    boolean needsDecrement = false;
    if (intercept != null) {
      String interceptFallback = intercept.fallback();

      switch (intercept.trigger()) {
        case FORCE -> {
          return resolveInterceptFallback(pjp, fallback, interceptFallback, prefixedKey, method);
        }
        case IS_LOCAL_HOT -> {
          if (hotKey.isLocalHotKey(prefixedKey)) {
            return resolveInterceptFallback(pjp, fallback, interceptFallback, prefixedKey, method);
          }
        }
        case QPS -> {
          int qpsThreshold = intercept.QPS();

          if (qpsThreshold > 0) {
            RollingWindow window = qpsWindows.get(prefixedKey, k -> new RollingWindow(10, 1000));
            window.add(1);
            if (window.sum() > qpsThreshold) {
              return resolveInterceptFallback(pjp, fallback, interceptFallback, prefixedKey, method);
            }
          }
        }
        case CONCURRENT_THREADS -> {
          int maxThreads = intercept.concurrentThreads();
          if (maxThreads > 0) {
            AtomicInteger counter = concurrentCounters.computeIfAbsent(prefixedKey, k -> new AtomicInteger(0));
            if (counter.incrementAndGet() > maxThreads) {
              counter.decrementAndGet();
              return resolveInterceptFallback(pjp, fallback, interceptFallback, prefixedKey, method);
            }
            needsDecrement = true;
          }
        }
      }
    }

    // Save-restore context to prevent leakage across nested @Cacheable calls
    HotKeyCacheContext.ContextValues prev = HotKeyCacheContext.get().snapshot();
    try {
      long hardTtlMs = (ttl != null && ttl.hardTtlMs() > 0) ? ttl.hardTtlMs() : 0L;
      long softTtlMs = (ttl != null && ttl.softTtlMs() > 0) ? ttl.softTtlMs() : 0L;
      boolean allowNull = nullCaching != null && nullCaching.value();
      boolean skipBroadcast = broadcast != null && !broadcast.value();

      HotKeyCacheContext.get().apply(hardTtlMs, softTtlMs, allowNull, skipBroadcast);
      return pjp.proceed();
    } catch (Throwable e) {
      if (fallback != null) {
        log.warn("[HotKeyCacheExtension] fallback triggered for key={}, reason={}", prefixedKey, e.getMessage());
        return resolveFallback(pjp, fallback, method);
      }
      throw e;
    } finally {
      if (needsDecrement) {
        AtomicInteger counter = concurrentCounters.get(prefixedKey);
        if (counter != null) {
          counter.decrementAndGet();
        }
      }
      HotKeyCacheContext.get().restore(prev);
    }
  }

  /**
   * Handle {@link HotKeyPreload @HotKeyPreload} by inflating HeavyKeeper counts
   * for static and/or dynamic cache keys.
   *
   * <p>Each key is inflated at most once, tracked by the bounded Caffeine cache
   * {@link #registeredPreloadKeys} (100k max, 1-hour TTL). This prevents
   * unbounded growth from dynamic key expressions while ensuring automatic
   * re-inflation if a key needs to be preloaded again after a long idle period.
   *
   * <p>Static keys ({@link HotKeyPreload#keys}) are inflated immediately.
   * Dynamic keys ({@link HotKeyPreload#keyExpr}) are evaluated via SpEL against
   * the current method parameters.
   *
   * @param preload   the annotation instance
   * @param pjp       the join point providing method arguments and target
   * @param cacheName the resolved cache name from {@code @Cacheable}
   */
  private void handlePreload(HotKeyPreload preload, ProceedingJoinPoint pjp, String cacheName, Method method) {
    String separator = properties.getSpringCache().getKeySeparator();
    int preloadCount = preload.count() > 0 ? preload.count() : Integer.MAX_VALUE;

    // Static keys — register once per key
    for (String staticKey : preload.keys()) {
      String fullKey = cacheName + separator + staticKey;
      if (registeredPreloadKeys.getIfPresent(fullKey) == null) {
        hotKey.notifyLocalDetectorDirect(fullKey, preloadCount);
        registeredPreloadKeys.put(fullKey, Boolean.TRUE);
      }
    }

    // Dynamic key via SpEL — register once per unique evaluated key
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
            hotKey.notifyLocalDetectorDirect(fullKey, preloadCount);
            registeredPreloadKeys.put(fullKey, Boolean.TRUE);
          }
        }
      } catch (Exception e) {
        log.warn("Failed to evaluate @HotKeyPreload keyExpr '{}': {}", keyExpr, e.toString());
      }
    }
  }

  /**
   * Intercepts both {@link CachePut @CachePut} and {@link CacheEvict @CacheEvict}
   * methods to set the send suppression flag from {@link Broadcast @Broadcast}
   * before the method reaches Spring's {@code CacheInterceptor}.
   *
   * <p>Only the {@code @Broadcast} annotation is read here. Other companion
   * annotations ({@code @HotKeyCacheTTL}, {@code @Intercept}, {@code @Fallback},
   * {@code @NullCaching}) are ignored on write/evict methods.
   *
   * @param pjp the join point for the intercepted method
   * @return the method return value
   */
  @SuppressWarnings("unused")
  @Around(
    "@annotation(org.springframework.cache.annotation.CachePut) || @annotation(org.springframework.cache.annotation.CacheEvict)"
  )
  public Object aroundCachePutOrEvict(ProceedingJoinPoint pjp) throws Throwable {
    return setBroadcast(pjp);
  }

  private Object setBroadcast(ProceedingJoinPoint pjp) throws Throwable {
    Method method = resolveMethod(pjp);
    Broadcast broadcast = method.getAnnotation(Broadcast.class);
    boolean skipBroadcast = broadcast != null && !broadcast.value();

    HotKeyCacheContext.ContextValues prev = HotKeyCacheContext.get().snapshot();
    try {
      HotKeyCacheContext.get().apply(0, 0, false, skipBroadcast);
      return pjp.proceed();
    } finally {
      HotKeyCacheContext.get().restore(prev);
    }
  }

  /**
   * Resolves the first available cache name from the {@link Cacheable @Cacheable} annotation.
   *
   * @param cacheable the annotation instance
   * @return the cache name, or {@code "hotkey"} if none is specified
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
    return "hotkey";
  }

  /**
   * Evaluates the cache key SpEL expression against method parameter variables.
   *
   * @param pjp        the join point providing method arguments
   * @param expression the SpEL expression to evaluate
   * @return the evaluated cache key string, or all arguments as string if expression is empty
   */
  private String resolveKey(ProceedingJoinPoint pjp, String expression, Method method) {
    if (expression.isEmpty()) {
      Object[] args = pjp.getArgs();
      if (args.length == 0) {
        return "empty";
      }
      if (args.length == 1 && args[0] != null && !args[0].getClass().isArray()) {
        return args[0].toString();
      }
      return new SimpleKey(args).toString();
    }
    return getExpression(expression).getValue(buildEvaluationContext(pjp, method), String.class);
  }

  /**
   * Resolves the concrete {@link Method} for the join point, unwrapping interface
   * methods to the implementation class when necessary.
   *
   * @param pjp the join point whose method to resolve
   * @return the resolved concrete method
   */
  private Method resolveMethod(ProceedingJoinPoint pjp) {
    MethodSignature sig = (MethodSignature) pjp.getSignature();
    Method method = sig.getMethod();
    if (method.getDeclaringClass().isInterface()) {
      try {
        method = pjp.getTarget().getClass().getMethod(method.getName(), method.getParameterTypes());
      } catch (NoSuchMethodException ignored) {
        // Keep the interface method
      }
    }
    return method;
  }

  /**
   * Builds an {@link EvaluationContext} with method parameters registered
   * as context variables via {@link MethodBasedEvaluationContext}.
   *
   * @param pjp the join point providing method arguments and target
   * @return a configured evaluation context with method parameters as variables
   */
  private EvaluationContext buildEvaluationContext(ProceedingJoinPoint pjp, Method method) {
    return new MethodBasedEvaluationContext(pjp.getTarget(), method, pjp.getArgs(), parameterNameDiscoverer);
  }

  /**
   * Returns a cached {@link Expression} for the given expression string, parsing it
   * on first access.
   *
   * @param expressionString the SpEL expression string to retrieve or parse
   * @return the cached or freshly parsed expression
   */
  private Expression getExpression(String expressionString) {
    return expressionCache.computeIfAbsent(expressionString, parser::parseExpression);
  }

  /**
   * Resolve the intercept fallback chain when the method call is intercepted.
   *
   * <p>Priority order:
   * <ol>
   *   <li>{@link Intercept#fallback()} — SpEL expression evaluated against
   *       method parameters</li>
   *   <li>{@link Fallback @Fallback} — naming-convention method or SpEL expression</li>
   *   <li>{@link io.github.hyshmily.hotkey.HotKey#peek(String)} — stale cached
   *       value if available, otherwise {@code null}</li>
   * </ol>
   *
   * @param pjp               the join point providing method arguments
   * @param fallback          the method-level {@code @Fallback} annotation
   *                          (may be {@code null})
   * @param interceptFallback the fallback SpEL from {@code @Intercept.fallback()}
   *                          (may be blank)
   * @param prefixedKey       the fully-qualified cache key (cache name + separator + key)
   * @return the resolved fallback value, or {@code null} if no fallback is available
   *         and the cache does not contain the key
   * @throws Throwable if the SpEL evaluation or fallback method invocation fails
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
    return hotKey.peek(prefixedKey).orElse(null);
  }

  /**
   * Resolves the fallback value from a {@link Fallback @Fallback} annotation.
   * <p>If the annotated SpEL expression is non-empty it is evaluated first;
   * otherwise the naming-convention method ({@code {methodName}Fallback}) is invoked.
   *
   * @param pjp      the join point providing method arguments
   * @param fallback the fallback annotation
   * @return the resolved fallback value
   */
  private Object resolveFallback(ProceedingJoinPoint pjp, Fallback fallback, Method method) throws Throwable {
    if (!fallback.value().isEmpty()) {
      return getExpression(fallback.value()).getValue(buildEvaluationContext(pjp, method));
    }
    return invokeFallbackMethod(pjp);
  }

  /**
   * Invokes the naming-convention fallback method on the target bean with
   * the original method arguments.
   *
   * <p>The fallback method is expected to follow the naming convention
   * {@code {originalMethodName}Fallback} with the same parameter types
   * as the original method. The resolved method is cached in
   * {@link #fallbackMethodCache} for subsequent invocations.
   *
   * <p>If no matching fallback method exists anywhere in the class
   * hierarchy, {@code null} is returned silently (no error is logged).
   *
   * @param pjp the join point providing the original method's arguments,
   *            target object, and signature metadata
   * @return the return value of the fallback method, or {@code null} if
   *         no fallback method was found
   * @throws Throwable the unwrapped cause if the fallback method throws
   */
  private Object invokeFallbackMethod(ProceedingJoinPoint pjp) throws Throwable {
    MethodSignature sig = (MethodSignature) pjp.getSignature();
    Method originalMethod = sig.getMethod();
    Object target = pjp.getTarget();
    Object[] args = pjp.getArgs();

    Method fallbackMethod = fallbackMethodCache.computeIfAbsent(originalMethod, m ->
      findFallbackMethod(target.getClass(), m.getName() + "Fallback", m.getParameterTypes())
    );
    if (fallbackMethod == null) {
      return null;
    }
    try {
      return fallbackMethod.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  /**
   * Recursively searches the class hierarchy (superclasses, not interfaces)
   * for a method matching the given name and parameter types.
   *
   * <p>The search starts at the provided class and walks up the inheritance
   * chain via {@link Class#getSuperclass()}, stopping at {@link Object}.
   * This ensures that fallback methods defined in abstract base classes
   * or superclasses are discovered.
   *
   * <p>Interface default methods are <em>not</em> searched, as the
   * annotation-based contract is that the fallback method resides in the
   * concrete bean's class hierarchy.
   *
   * @param clazz      the class to start the search from (typically the
   *                   target object's runtime class)
   * @param name       the name of the fallback method to find (e.g.,
   *                   {@code "findUserFallback"})
   * @param paramTypes the parameter types of the method to find (must
   *                   exactly match the original method's parameter types)
   * @return the matching {@link Method}, or {@code null} if no method
   *         with the given name and parameter types exists in the
   *         hierarchy
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
   * Resolve {@link HotKeyPreload @HotKeyPreload} from the given method,
   * using a local cache to avoid repeated reflection lookups.
   *
   * @param method the candidate method
   * @return the {@code @HotKeyPreload} annotation if present, or {@code null}
   */
  private HotKeyPreload resolvePreloadAnnotation(Method method) {
    return preloadCache.computeIfAbsent(method, m -> m.getAnnotation(HotKeyPreload.class));
  }

  private AnnotationSet resolveAnnotations(Method method) {
    return annotationCache.computeIfAbsent(method, m -> {
      HotKeyCacheTTL ttl = null;
      Intercept intercept = null;
      Fallback fallback = null;
      NullCaching nullCaching = null;
      Broadcast broadcast = null;

      for (Annotation a : m.getDeclaredAnnotations()) {
        if (a instanceof HotKeyCacheTTL t) {
          ttl = t;
        } else if (a instanceof Intercept i) {
          intercept = i;
        } else if (a instanceof Fallback f) {
          fallback = f;
        } else if (a instanceof NullCaching n) {
          nullCaching = n;
        } else if (a instanceof Broadcast b) {
          broadcast = b;
        }
      }
      return new AnnotationSet(ttl, intercept, fallback, nullCaching, broadcast);
    });
  }
}
