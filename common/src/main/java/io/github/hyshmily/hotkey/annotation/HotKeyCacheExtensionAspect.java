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

import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.autoconfigure.HotKeyProperties;
import io.github.hyshmily.hotkey.cache.annotationsupporter.HotKeyCacheContext;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Companion AOP aspect for Spring {@link Cacheable @Cacheable} methods.
 *
 * <p>Runs at {@link Ordered#HIGHEST_PRECEDENCE} — before Spring's own
 * {@code CacheInterceptor} — to set up thread-bound TTL overrides,
 * null-caching mode, and apply {@link Intercept @Intercept} /
 * {@link Fallback @Fallback} semantics.
 *
 * <p>For each {@code @Cacheable} invocation:
 * <ol>
 *   <li>Resolves the cache key from SpEL</li>
 *   <li>If {@code @Intercept} is present and the key is a local hot key,
 *       skips the method and returns the cached value or fallback</li>
 *   <li>Applies TTL from {@code @HotKeyCacheTTL} and null-caching from
 *       {@code @NullCaching} into {@link HotKeyCacheContext}</li>
 *   <li>Proceeds to the Spring {@code CacheInterceptor}</li>
 *   <li>Restores the context snapshot in a {@code finally} block</li>
 *   <li>If the method throws and {@code @Fallback} is present, returns the fallback</li>
 * </ol>
 */
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
    String key = resolveKey(pjp, cacheable.key());
    String prefixedKey = cacheName + properties.getSpringCache().getKeySeparator() + key;

    HotKeyCacheTTL ttl = method.getAnnotation(HotKeyCacheTTL.class);
    Intercept intercept = method.getAnnotation(Intercept.class);
    Fallback fallback = method.getAnnotation(Fallback.class);
    NullCaching nullCaching = method.getAnnotation(NullCaching.class);

    // @Intercept: skip method when key is a local hot key
    if (intercept != null && hotKey.isLocalHotKey(prefixedKey)) {
      if (fallback != null) {
        return resolveFallback(pjp, fallback);
      }
      return hotKey.peek(prefixedKey).orElse(null);
    }

    // Save-restore context to prevent leakage across nested @Cacheable calls
    HotKeyCacheContext.ContextValues prev = HotKeyCacheContext.get().snapshot();
    try {
      long hardTtlMs = (ttl != null && ttl.hardTtlMs() > 0) ? ttl.hardTtlMs() : 0L;
      long softTtlMs = (ttl != null && ttl.softTtlMs() > 0) ? ttl.softTtlMs() : 0L;
      boolean allowNull = nullCaching != null && nullCaching.value();

      HotKeyCacheContext.get().apply(hardTtlMs, softTtlMs, allowNull);
      return pjp.proceed();
    } catch (Throwable e) {
      if (fallback != null) {
        log.warn("[HotKeyCacheExtension] fallback triggered for key={}, reason={}", prefixedKey, e.getMessage());
        return resolveFallback(pjp, fallback);
      }
      throw e;
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
  private String resolveKey(ProceedingJoinPoint pjp, String expression) {
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
    return getExpression(expression).getValue(buildEvaluationContext(pjp), String.class);
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
  private EvaluationContext buildEvaluationContext(ProceedingJoinPoint pjp) {
    return new MethodBasedEvaluationContext(
      pjp.getTarget(),
      resolveMethod(pjp),
      pjp.getArgs(),
      parameterNameDiscoverer
    );
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
   * Resolves the fallback value from a {@link Fallback @Fallback} annotation.
   * <p>If the annotated SpEL expression is non-empty it is evaluated first;
   * otherwise the naming-convention method ({@code {methodName}Fallback}) is invoked.
   *
   * @param pjp      the join point providing method arguments
   * @param fallback the fallback annotation
   * @return the resolved fallback value
   */
  private Object resolveFallback(ProceedingJoinPoint pjp, Fallback fallback) throws Throwable {
    if (!fallback.value().isEmpty()) {
      return getExpression(fallback.value()).getValue(buildEvaluationContext(pjp));
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
}
