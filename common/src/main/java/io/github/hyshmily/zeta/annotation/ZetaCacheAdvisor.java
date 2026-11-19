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
import java.lang.reflect.Proxy;
import org.jspecify.annotations.NonNull;
import org.springframework.aop.ClassFilter;
import org.springframework.aop.Pointcut;
import org.springframework.aop.framework.AopInfrastructureBean;
import org.springframework.aop.support.AbstractBeanFactoryPointcutAdvisor;
import org.springframework.aop.support.StaticMethodMatcherPointcut;
import org.springframework.core.Ordered;

/**
 * Puts the cache advice in front of every method carrying one of
 * {@link ZetaCached}, {@link ZetaPut} or {@link ZetaInvalidate}.
 *
 * <p><b>Lifecycle.</b> The advisor holds only the lightweight
 * {@link PlanResolver}; the advice bean is resolved by name when the first
 * advised bean is proxied. Creating the advisor during BeanPostProcessor
 * registration therefore never instantiates {@link Zeta} or its engine
 * dependencies, so those dependencies remain eligible for every
 * BeanPostProcessor.
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
 * call. The class filter denies infrastructure packages before that scan, so
 * Spring's infrastructure and third-party beans do not each get a permanent
 * empty table; JDK proxies are admitted because their business interfaces can
 * carry the annotations. The invocation itself then finds the plan already
 * cached under the reported method and every contributing view.
 *
 * <p><b>Registration.</b> The advisor is declared with
 * {@code ROLE_INFRASTRUCTURE} so the auto-proxy creator Spring registers for its
 * own infrastructure (transaction, caching) applies it — no {@code @EnableCaching}
 * or AspectJ weaver required.
 */
@Internal
public final class ZetaCacheAdvisor extends AbstractBeanFactoryPointcutAdvisor {

  /** One step outside the transaction interceptor ({@link Ordered#LOWEST_PRECEDENCE}). */
  public static final int ADVICE_ORDER = Ordered.LOWEST_PRECEDENCE - 1;

  /** The bean name the advisor resolves lazily; the auto-configuration declares the advice under it. */
  public static final String INTERCEPTOR_BEAN_NAME = "zetaCacheInterceptor";

  private final Pointcut pointcut;

  /**
   * Create the advisor.
   *
   * @param resolver the lightweight plan source used by the pointcut
   */
  public ZetaCacheAdvisor(PlanResolver resolver) {
    this.pointcut = new AnnotationPointcut(resolver);
    setAdviceBeanName(INTERCEPTOR_BEAN_NAME);
    setOrder(ADVICE_ORDER);
  }

  @Override
  @NonNull
  public Pointcut getPointcut() {
    return pointcut;
  }

  /**
   * Answers Spring's method-match question from the resolver's plan table.
   * The first question about a class — match or not — resolves and validates
   * every candidate method into that class's table; afterwards
   * each question about a view of the class is one lookup, so interceptor-chain
   * assembly performs no annotation scan and no most-specific-method lookup for
   * the class's remaining methods.
   */
  private static final class AnnotationPointcut extends StaticMethodMatcherPointcut {

    /**
     * A fast negative for beans that cannot carry a user cache annotation. Every method of every
     * bean otherwise reaches the resolver, whose first question scans and permanently caches that
     * class's method table. A JDK proxy is always admitted: a Spring Data repository's business
     * interfaces carry the annotation even though the generated proxy class is in a framework
     * package. A CGLIB proxy keeps its user class's package name, so it is not filtered here.
     */
    private static final ClassFilter USER_TYPES = clazz -> {
      if (Proxy.isProxyClass(clazz)) {
        return true;
      }
      if (AopInfrastructureBean.class.isAssignableFrom(clazz)) {
        return false;
      }
      String name = clazz.getName();
      return (
        !name.startsWith("java.") &&
        !name.startsWith("javax.") &&
        !name.startsWith("jakarta.") &&
        !name.startsWith("org.springframework.") &&
        !name.startsWith("org.aopalliance.") &&
        !name.startsWith("sun.") &&
        !name.startsWith("com.sun.") &&
        !name.startsWith("jdk.")
      );
    };

    private final PlanResolver resolver;

    private AnnotationPointcut(PlanResolver resolver) {
      this.resolver = resolver;
    }

    @Override
    @NonNull
    public ClassFilter getClassFilter() {
      return USER_TYPES;
    }

    @Override
    public boolean matches(@NonNull Method method, @NonNull Class<?> targetClass) {
      return resolver.matches(method, targetClass);
    }
  }
}
