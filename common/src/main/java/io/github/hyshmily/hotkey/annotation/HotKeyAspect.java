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

import io.github.hyshmily.hotkey.exception.HotKeyBlockedException;
import io.github.hyshmily.hotkey.logging.DefaultLogger;
import io.github.hyshmily.hotkey.logging.HotKeyLogger;
import io.github.hyshmily.hotkey.rule.Rule;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * AOP aspect that intercepts methods annotated with {@link HotKey @HotKey} and applies
 * the corresponding cache operation with the following priority chain for READ:
 * <ol>
 *   <li><b>Blacklist</b> (always-on) — {@link Rule.RuleAction#BLOCK} → fallback or exception</li>
 *   <li><b>Condition</b> — SpEL {@link HotKey#condition()} false → skip cache, execute method</li>
 *   <li><b>Intercept</b> — {@link Intercept @Intercept} + local hot key → fallback or peek</li>
 *   <li><b>TTL</b> — {@link HotKeyCacheTTL @HotKeyCacheTTL} override or global default</li>
 *   <li><b>Cache</b> — {@code getWithSoftExpire} / {@code get} with fallback on exception</li>
 *   <li><b>Unless</b> — SpEL {@link HotKey#unless()} evaluated (value accepted as cached)</li>
 * </ol>
 */
@Aspect
@RequiredArgsConstructor
public class HotKeyAspect {

  private static final HotKeyLogger log = new DefaultLogger(HotKeyAspect.class);

  /** The HotKey facade for all cache operations. */
  private final io.github.hyshmily.hotkey.HotKey hotKeyFacade;
  /** SpEL expression parser, shared across all evaluations. */
  private final SpelExpressionParser parser = new SpelExpressionParser();
  /** Cache of parsed SpEL expressions keyed by expression string. */
  private final Map<String, Expression> expressionCache = new ConcurrentHashMap<>();
  /** Discoverer for resolving method parameter names at runtime. */
  private final ParameterNameDiscoverer parameterNameDiscoverer = new DefaultParameterNameDiscoverer();
  /** Cache of method parameter name arrays, keyed by Method. */
  private final Map<Method, String[]> paramNamesCache = new ConcurrentHashMap<>();
  /** Cache of resolved naming-convention fallback methods, keyed by original Method. */
  private final Map<Method, Method> fallbackMethodCache = new ConcurrentHashMap<>();

  /**
   * Intercepts methods annotated with {@link HotKey @HotKey} and dispatches to the
   * appropriate handler based on the operation type.
   *
   * @param pjp    the join point for the intercepted method
   * @param hotKey the annotation instance on the intercepted method
   * @return the cached value or the method return value
   */
  @Around("@annotation(hotKey)")
  public Object around(ProceedingJoinPoint pjp, HotKey hotKey) throws Throwable {
    String cacheKey = resolveKey(pjp, hotKey.key());

    return switch (hotKey.operation()) {
      case READ -> handleRead(pjp, cacheKey, hotKey);
      case WRITE -> handleWrite(pjp, cacheKey, hotKey);
      case INVALIDATE -> handleInvalidate(pjp, cacheKey, hotKey);
    };
  }

  /**
   * Handles a {@link HotKey.OperationType#READ} operation through the full priority chain.
   * <ol>
   *   <li>Blacklist pre-check — blocked keys trigger fallback or exception</li>
   *   <li>Condition evaluation — skip cache when SpEL condition is false</li>
   *   <li>Intercept check — return fallback or peek value for hot keys</li>
   *   <li>TTL resolution — per-annotation override or global default</li>
   *   <li>Cache lookup — {@code getWithSoftExpire} or {@code get} with loader fallback</li>
   *   <li>Unless evaluation — SpEL exclusion (value remains cached)</li>
   * </ol>
   */
  private Object handleRead(ProceedingJoinPoint pjp, String cacheKey, HotKey hotKey) throws Throwable {
    Method method = resolveMethod(pjp);
    Fallback fallback = method.getAnnotation(Fallback.class);
    Intercept intercept = method.getAnnotation(Intercept.class);
    HotKeyCacheTTL ttl = method.getAnnotation(HotKeyCacheTTL.class);

    // Blacklist pre-check (always-on, highest priority)
    if (hotKeyFacade.evaluateRule(cacheKey) == Rule.RuleAction.BLOCK) {
      if (fallback != null) {
        return resolveFallback(pjp, fallback);
      }
      throw new HotKeyBlockedException(cacheKey);
    }

    //  Condition: skip cache entirely when condition is false
    if (!hotKey.condition().isEmpty()) {
      Boolean pass = evalSpel(pjp, hotKey.condition(), Boolean.class);
      if (pass == null || !pass) {
        return pjp.proceed();
      }
    }

    //  @Intercept + isLocalHotKey: skip method, return fallback or peek
    if (intercept != null && hotKeyFacade.isLocalHotKey(cacheKey)) {
      if (fallback != null) {
        return resolveFallback(pjp, fallback);
      }
      return hotKeyFacade.peek(cacheKey).orElse(null);
    }

    // TTL resolution: @HotKeyCacheTTL override or 0 (use global default)
    long hardTtl = ttl != null ? ttl.hardTtlMs() : 0;
    long softTtl = ttl != null ? ttl.softTtlMs() : 0;

    //  Cache get
    Supplier<Object> loader = () -> {
      try {
        return pjp.proceed();
      } catch (RuntimeException | Error e) {
        throw e;
      } catch (Throwable e) {
        throw sneakyThrow(e);
      }
    };

    Optional<Object> result;
    try {
      result = hotKey.softExpire()
        ? hotKeyFacade.getWithSoftExpire(cacheKey, loader, hardTtl, softTtl)
        : hotKeyFacade.get(cacheKey, loader, hardTtl, softTtl);
    } catch (RuntimeException e) {
      if (fallback != null) {
        log.warn("[HotKey] fallback triggered for key={}, operation=READ, reason=cacheException", cacheKey);
        return resolveFallback(pjp, fallback);
      }
      throw e;
    }

    //  Unless: evaluated but value remains cached per design
    if (!hotKey.unless().isEmpty() && result.isPresent()) {
      evalSpelWithResult(pjp, hotKey.unless(), result.get(), Boolean.class);
    }

    // Return type adaptation
    MethodSignature sig = (MethodSignature) pjp.getSignature();
    Class<?> returnType = sig.getMethod().getReturnType();
    if (returnType == Optional.class) {
      return result;
    }
    return result.orElse(null);
  }

  /**
   * Handles a {@link HotKey.OperationType#WRITE} operation.
   * <p>Delegates to {@code putBeforeInvalidate} so the mutation runs inside a
   * transaction-aware boundary.  After success, L1 is invalidated and an INVALIDATE
   * broadcast is sent to peers.
   * <p><b>Note:</b> {@code softExpire} from the {@link HotKey @HotKey} annotation is
   * silently ignored — WRITE always uses {@code putBeforeInvalidate} which does not
   * accept TTL parameters.
   */
  private Object handleWrite(ProceedingJoinPoint pjp, String cacheKey, HotKey hotKey) throws Throwable {
    if (!hotKey.softExpire()) {
      log.warn("[HotKey] softExpire=false is ignored for WRITE operation — putBeforeInvalidate does not accept TTL parameters");
    }
    Object[] resultHolder = new Object[1];
    Throwable[] exceptionHolder = new Throwable[1];

    hotKeyFacade.putBeforeInvalidate(cacheKey, () -> {
      try {
        resultHolder[0] = pjp.proceed();
      } catch (Throwable t) {
        exceptionHolder[0] = t;
        if (t instanceof RuntimeException) {
          throw (RuntimeException) t;
        }
        throw sneakyThrow(t);
      }
    });

    if (exceptionHolder[0] != null) {
      throw exceptionHolder[0];
    }
    return resultHolder[0];
  }

  /**
   * Handles a {@link HotKey.OperationType#INVALIDATE} operation.
   * <p>Invalidates the key from L1, increments the version,
   * broadcasts TYPE_REFRESH to peers, then proceeds with the original method.
   * <p><b>Note:</b> {@code softExpire} from the {@link HotKey @HotKey} annotation is
   * silently ignored — INVALIDATE always calls {@code invalidate(key)} regardless.
   */
  private Object handleInvalidate(ProceedingJoinPoint pjp, String cacheKey, HotKey hotKey) throws Throwable {
    if (!hotKey.softExpire()) {
      log.warn("[HotKey] softExpire=false is ignored for INVALIDATE operation — invalidate always clears the entry");
    }
    hotKeyFacade.invalidate(cacheKey);
    return pjp.proceed();
  }

  /**
   * Resolves the fallback value from a {@link Fallback @Fallback} annotation.
   * <p>If the annotated SpEL expression is non-empty it is evaluated first;
   * otherwise the naming-convention method ({@code {methodName}Fallback}) is invoked.
   */
  private Object resolveFallback(ProceedingJoinPoint pjp, Fallback fallback) throws Throwable {
    if (!fallback.value().isEmpty()) {
      return resolveSpelFallback(pjp, fallback.value());
    }
    return invokeFallbackMethod(pjp);
  }

  /** Evaluates a SpEL fallback expression with method parameters as context variables. */
  private Object resolveSpelFallback(ProceedingJoinPoint pjp, String expression) {
    return evalSpel(pjp, expression, Object.class);
  }

  /**
   * Invokes the naming-convention fallback method ({@code {originalName}Fallback})
   * on the target bean with the original method arguments.
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
   * Recursively searches the class hierarchy for a method with the given name and parameter types.
   *
   * @param clazz      the class to search from
   * @param name       the method name to find
   * @param paramTypes the parameter types of the method to find
   * @return the matching method, or {@code null} if not found
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
   * Resolves the concrete {@link Method} for the join point, unwrapping interface
   * methods to the implementation class when necessary.
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
   * Builds a {@link StandardEvaluationContext} with method parameters registered
   * as context variables.  Parameter names are resolved once and cached per method.
   */
  private StandardEvaluationContext buildEvaluationContext(ProceedingJoinPoint pjp) {
    MethodSignature sig = (MethodSignature) pjp.getSignature();
    Method method = sig.getMethod();
    String[] names = paramNamesCache.computeIfAbsent(method, m -> {
      String[] n = parameterNameDiscoverer.getParameterNames(m);
      if (n == null) {
        n = new String[m.getParameterCount()];
        for (int i = 0; i < n.length; i++) {
          n[i] = "arg" + i;
        }
      }
      return n;
    });
    Object[] args = pjp.getArgs();
    StandardEvaluationContext ctx = new StandardEvaluationContext();
    for (int i = 0; i < args.length; i++) {
      ctx.setVariable(names[i], args[i]);
    }
    return ctx;
  }

  /**
   * Returns a cached {@link Expression} for the given expression string, parsing it
   * on first access.  Annotation-level expressions are compile-time constants so
   * caching is safe and eliminates repeated lexing/AST construction.
   */
  private Expression getExpression(String expressionString) {
    return expressionCache.computeIfAbsent(expressionString, parser::parseExpression);
  }

  /**
   * Resolves the cache key from a SpEL expression using method parameter names
   * as context variables.
   *
   * @param pjp        the join point providing method arguments
   * @param expression the SpEL expression to evaluate as the cache key
   * @return the evaluated cache key string
   */
  private String resolveKey(ProceedingJoinPoint pjp, String expression) {
    return getExpression(expression).getValue(buildEvaluationContext(pjp), String.class);
  }

  /**
   * Evaluates a SpEL expression against method parameter variables.
   *
   * @param <T>        the expected return type
   * @param expression the SpEL expression to evaluate
   * @param type       the expected result type
   * @return the evaluated value
   */
  private <T> T evalSpel(ProceedingJoinPoint pjp, String expression, Class<T> type) {
    return getExpression(expression).getValue(buildEvaluationContext(pjp), type);
  }

  /**
   * Evaluates a SpEL expression with method parameters and a {@code #result} variable.
   * <p>Used for the {@link HotKey#unless()} evaluation where the loaded cache value
   * is exposed as {@code #result}.
   *
   * @param pjp        the join point providing method arguments for context variables
   * @param expression the SpEL expression to evaluate
   * @param result     the loaded cache value to bind as {@code #result}
   * @param type       the expected result type
   */
  private void evalSpelWithResult(ProceedingJoinPoint pjp, String expression, Object result, Class<?> type) {
    StandardEvaluationContext ctx = buildEvaluationContext(pjp);
    ctx.setVariable("result", result);
    getExpression(expression).getValue(ctx, type);
  }

  /**
   * Throws a checked exception without declaring it, using type erasure.
   * <p>This avoids polluting method signatures with checked exception declarations
   * inside lambda expressions used as cache loaders.
   *
   * @param <T> the throwable type
   * @param t   the throwable to re-throw
   * @return never returns (always throws)
   */
  @SuppressWarnings("unchecked")
  private static <T extends Throwable> RuntimeException sneakyThrow(Throwable t) throws T {
    throw (T) t;
  }
}
