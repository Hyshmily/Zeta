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

import io.github.hyshmily.zeta.util.Internal;
import java.lang.reflect.Method;
import org.aopalliance.aop.Advice;
import org.jspecify.annotations.NonNull;
import org.springframework.aop.Pointcut;
import org.springframework.aop.support.AbstractPointcutAdvisor;
import org.springframework.aop.support.StaticMethodMatcherPointcut;
import org.springframework.core.Ordered;

/**
 * Puts {@link ZetaCacheInterceptor} in front of every method carrying one of
 * {@link ZetaCached}, {@link ZetaPut} or {@link ZetaInvalidate}.
 *
 * <p><b>Order.</b> {@link #ADVICE_ORDER} is one step outside the transaction
 * interceptor (whose default order is {@link Ordered#LOWEST_PRECEDENCE}), so
 * the cache side effects bracket the transaction: the body runs inside the
 * transaction because the read's loader proceeds into it, while a write or an
 * after-the-fact invalidation happens only after the transaction completed — a
 * rollback can never leave a cache side effect behind. {@code @ZetaInvalidate}
 * with {@code beforeInvocation = true} runs before the transaction begins,
 * which is the same fact as bypassing its after-commit deferral. A custom
 * transaction order that places the transaction outside makes the engine defer
 * the write itself (the facade is transaction-aware), so both configurations
 * stay correct.
 *
 * <p><b>Startup validation.</b> {@code matches} is where Spring's auto-proxy
 * creator asks whether a bean needs this advice. The first question about a
 * class resolves every candidate method of that class, so a malformed second
 * verb fails bean creation even when {@code AopUtils.canApply} stopped at the
 * first match and a JDK proxy would otherwise match it lazily on its first
 * call. The invocation itself then finds the plan already cached under the
 * reported method and every contributing view.
 *
 * <p><b>Registration.</b> The advisor is declared with
 * {@code ROLE_INFRASTRUCTURE} so the auto-proxy creator Spring registers for its
 * own infrastructure (transaction, caching) applies it — no {@code @EnableCaching}
 * or AspectJ weaver required.
 */
@Internal
public final class ZetaCacheAdvisor extends AbstractPointcutAdvisor {

  /** One step outside the transaction interceptor ({@link Ordered#LOWEST_PRECEDENCE}). */
  public static final int ADVICE_ORDER = Ordered.LOWEST_PRECEDENCE - 1;

  private final Pointcut pointcut;

  private final Advice advice;

  /**
   * Create the advisor.
   *
   * @param interceptor the advice to apply
   */
  public ZetaCacheAdvisor(ZetaCacheInterceptor interceptor) {
    this.advice = interceptor;
    this.pointcut = new AnnotationPointcut(interceptor);
  }

  @Override
  @NonNull
  public Pointcut getPointcut() {
    return pointcut;
  }

  @Override
  @NonNull
  public Advice getAdvice() {
    return advice;
  }

  @Override
  public int getOrder() {
    return ADVICE_ORDER;
  }

  /**
   * Answers Spring's method-match question from the interceptor's plan table.
   * The first question about a class — match or not — resolves and validates
   * every candidate method into that class's {@code ClassValue}; afterwards
   * each question about a view of the class is one lookup, so interceptor-chain
   * assembly performs no annotation scan and no most-specific-method lookup for
   * the class's remaining methods.
   */
  private static final class AnnotationPointcut extends StaticMethodMatcherPointcut {

    private final ZetaCacheInterceptor interceptor;

    private AnnotationPointcut(ZetaCacheInterceptor interceptor) {
      this.interceptor = interceptor;
    }

    @Override
    public boolean matches(@NonNull Method method, @NonNull Class<?> targetClass) {
      return interceptor.matches(method, targetClass);
    }
  }
}
