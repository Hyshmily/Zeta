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

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * AOP aspect that intercepts methods annotated with {@link HotKey} and applies
 * the corresponding cache operation.
 * <p>
 * The cache key is resolved dynamically via SpEL against the method arguments.
 * Three operation modes are supported:
 * <ul>
 *   <li><b>READ</b> — uses {@code getWithSoftExpire} by default, or plain
 *       {@code get} when {@link HotKey#softExpire()} is {@code false}.
 *       The method invocation acts as the value supplier.</li>
 *   <li><b>WRITE</b> — uses {@code putBeforeInvalidate} to atomically execute
 *       the mutation, increment version, invalidate L1, and broadcast INVALIDATE.</li>
 *   <li><b>INVALIDATE</b> — invalidates L1, increments version, and broadcasts
 *       TYPE_REFRESH (versioned) to peers, then proceeds with the method.</li>
 * </ul>
 * <p>
 * If the annotated method returns {@link Optional}, the aspect passes it through
 * unchanged; otherwise it unwraps via {@link Optional#orElse(Object) orElse(null)}.
 * <p>
 * Requires the {@code -parameters} compiler flag for reliable SpEL key resolution.
 * Falls back to synthetic parameter names {@code arg0, arg1, ...} when debug info
 * is absent.
 */
@Aspect
public class HotKeyAspect {

  private final io.github.hyshmily.hotkey.HotKey hotKeyFacade;
  private final SpelExpressionParser parser = new SpelExpressionParser();
  private final ParameterNameDiscoverer parameterNameDiscoverer = new DefaultParameterNameDiscoverer();
  private final Map<Method, String[]> paramNamesCache = new ConcurrentHashMap<>();

  public HotKeyAspect(io.github.hyshmily.hotkey.HotKey hotKeyFacade) {
    this.hotKeyFacade = hotKeyFacade;
  }

  /**
   * Around advice for {@link HotKey @HotKey} methods.
   * Resolves the SpEL cache key, then dispatches to the appropriate handler
   * based on the operation type.
   */
  @Around("@annotation(hotKey)")
  public Object around(ProceedingJoinPoint pjp, HotKey hotKey) throws Throwable {
    String cacheKey = resolveKey(pjp, hotKey.key());

    return switch (hotKey.operation()) {
      case READ -> handleRead(pjp, cacheKey, hotKey);
      case WRITE -> handleWrite(pjp, cacheKey);
      case INVALIDATE -> handleInvalidate(pjp, cacheKey);
    };
  }

  /**
   * Handles {@link HotKey.OperationType#READ}.
   * <p>
   * The method invocation serves as the value supplier. If the method returns
   * {@link Optional}, the result is passed through directly; otherwise the
   * optional is unwrapped.
   */
  private Object handleRead(ProceedingJoinPoint pjp, String cacheKey, HotKey hotKey) {
    Supplier<Object> loader = () -> {
      try {
        return pjp.proceed();
      } catch (RuntimeException | Error e) {
        throw e;
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    };

    Optional<Object> result;
    if (hotKey.softExpire()) {
      result = hotKeyFacade.getWithSoftExpire(cacheKey, loader, hotKey.hardTtlMs(), hotKey.softTtlMs());
    } else {
      result = hotKeyFacade.get(cacheKey, loader, hotKey.hardTtlMs(), hotKey.softTtlMs());
    }

    MethodSignature sig = (MethodSignature) pjp.getSignature();
    Class<?> returnType = sig.getMethod().getReturnType();
    if (returnType == Optional.class) {
      return result;
    }
    return result.orElse(null);
  }

  /**
   * Handles {@link HotKey.OperationType#WRITE}.
   * <p>
   * Executes the method as the mutation inside {@code putBeforeInvalidate},
   * which provides transaction-scoped mutation execution, version increment,
   * L1 invalidation, and INVALIDATE broadcast. The result of the method is
   * returned to the caller.
   */
  private Object handleWrite(ProceedingJoinPoint pjp, String cacheKey) throws Throwable {
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
        throw new RuntimeException(t);
      }
    });

    if (exceptionHolder[0] != null) {
      throw exceptionHolder[0];
    }
    return resultHolder[0];
  }

  /**
   * Handles {@link HotKey.OperationType#INVALIDATE}.
   * <p>
   * Invalidates the key from L1, increments version, and broadcasts
   * TYPE_REFRESH (versioned) to peers, then proceeds with the method.
   */
  private Object handleInvalidate(ProceedingJoinPoint pjp, String cacheKey) throws Throwable {
    hotKeyFacade.invalidate(cacheKey);
    return pjp.proceed();
  }

  /**
   * Resolves a SpEL cache key expression against the method arguments.
   * <p>
   * Parameter names are discovered via {@link DefaultParameterNameDiscoverer}
   * (requires the {@code -parameters} compiler flag) and cached in a
   * {@link ConcurrentHashMap} keyed by {@link Method}. When names cannot be
   * determined, synthetic names {@code arg0, arg1, ...} are used as fallback.
   */
  private String resolveKey(ProceedingJoinPoint pjp, String expression) {
    Method method = ((MethodSignature) pjp.getSignature()).getMethod();
    String[] paramNames = paramNamesCache.computeIfAbsent(method, m -> {
      String[] names = parameterNameDiscoverer.getParameterNames(m);
      if (names == null) {
        // Fallback to synthetic parameter names
        names = new String[m.getParameterCount()];
        for (int i = 0; i < names.length; i++) {
          names[i] = "arg" + i;
        }
      }
      return names;
    });

    Object[] args = pjp.getArgs();
    StandardEvaluationContext context = new StandardEvaluationContext();
    for (int i = 0; i < args.length; i++) {
      context.setVariable(paramNames[i], args[i]);
    }
    return parser.parseExpression(expression).getValue(context, String.class);
  }
}
