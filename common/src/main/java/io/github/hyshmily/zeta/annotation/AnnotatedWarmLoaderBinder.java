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

import io.github.hyshmily.zeta.annotation.support.CompiledExpression;
import io.github.hyshmily.zeta.annotation.support.KeyEncoding;
import io.github.hyshmily.zeta.annotation.support.ParameterBindings;
import io.github.hyshmily.zeta.annotation.support.ZetaEvaluationContext;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.policy.NamespacePolicyRegistry;
import io.github.hyshmily.zeta.policy.NamespacePolicyRegistry.WarmLoaderBinder;
import io.github.hyshmily.zeta.util.CollectionSizing;
import io.github.hyshmily.zeta.util.Internal;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.aopalliance.aop.Advice;
import org.jspecify.annotations.Nullable;
import org.springframework.aop.Advisor;
import org.springframework.aop.IntroductionAdvisor;
import org.springframework.aop.Pointcut;
import org.springframework.aop.PointcutAdvisor;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.core.ResolvableType;

/**
 * Collects, validates, and binds every {@code @ZetaCached(selfWarmLoader = true)} method as its
 * namespace's background prewarm and renewal loader.
 *
 * <p>Each {@link WarmSpec} is built while beans are being proxied, fixing the namespace decoder and
 * rejecting methods whose keys cannot be inverted or whose guards depend on caller state. Binding
 * runs once all singletons are instantiated, which is the earliest point where a resolved target
 * instance can be paired with its read plan and its completed advice chain.
 *
 * <p>The expected bean type is the class the plan table was scanned for rather than the method's
 * declaring class, allowing subclasses to inherit one annotated base method and distinguish
 * themselves through their own {@code @ZetaConfig}.
 */
@Internal
@Slf4j
public final class AnnotatedWarmLoaderBinder implements WarmLoaderBinder {

  private static final String SELF_WARM_ANNOTATION = "@ZetaCached(selfWarmLoader = true)";

  /**
   * Advice that neither reads caller state nor changes the value a load produces. Naming it would
   * bury the advisors that matter: {@code ExposeInvocationInterceptor} sits on every proxy an
   * AspectJ creator builds, and a repository proxy always carries its query executor.
   */
  private static final Set<String> PLUMBING = Set.of(
    "org.springframework.aop.interceptor.ExposeInvocationInterceptor",
    "org.springframework.cache.interceptor.CacheInterceptor",
    "org.springframework.dao.support.PersistenceExceptionTranslationInterceptor",
    "org.springframework.data.repository.core.support.QueryExecutorMethodInterceptor"
  );

  private final ListableBeanFactory beanFactory;
  private final PlanResolver resolver;
  private final NamespacePolicyRegistry policies;

  /**
   * Creates a binder for self-warming {@code @ZetaCached} methods.
   *
   * @param beanFactory the Spring bean factory used to look up target beans
   * @param resolver    the plan resolver supplying collected warm specifications and contexts
   * @param policies    the namespace policy registry where background loaders are registered
   */
  public AnnotatedWarmLoaderBinder(
    ListableBeanFactory beanFactory,
    PlanResolver resolver,
    NamespacePolicyRegistry policies
  ) {
    this.beanFactory = Objects.requireNonNull(beanFactory, "beanFactory must not be null");
    this.resolver = Objects.requireNonNull(resolver, "resolver must not be null");
    this.policies = Objects.requireNonNull(policies, "policies must not be null");
  }

  /**
   * Resolves the target bean for every collected {@link WarmSpec} and registers its background
   * cache loader with the namespace policy registry.
   */
  @Override
  public void bindAll() {
    AdvisedBeans advised = new AdvisedBeans();
    for (WarmSpec spec : resolver.warmSpecs()) {
      bind(spec, advised);
    }
  }

  /**
   * Unwraps the target bean for one warm specification and registers its {@link CacheLoader}.
   *
   * @param spec    the validated warm specification to bind
   * @param advised the proxy beans discovered during this {@link #bindAll()} pass
   * @throws IllegalStateException if the target bean cannot be uniquely resolved or unwrapped
   */
  private void bind(WarmSpec spec, AdvisedBeans advised) {
    Object bean = findBean(spec, advised);
    PlanResolver.ProxyChain chain = PlanResolver.proxyChain(bean, spec.specific());
    Object target = Objects.requireNonNull(chain.target(), () -> "Unable to unwrap target bean for " + spec.specific());

    Class<?> targetClass = PlanResolver.resolveTargetClass(target, spec.specific().getDeclaringClass());
    warnUnhonouredAdvice(spec, chain, target, targetClass);
    Method invocable = resolver.targetMethod(spec.specific(), target);
    policies.registerProgrammaticNamespace(
      spec.encoding().physicalPrefix(),
      null,
      null,
      new AnnotatedWarmLoader(spec, target, targetClass, invocable, resolver)
    );
  }

  /**
   * Logs every advisor the background load cannot run under.
   *
   * <p>The load invokes the unwrapped target reflectively, so an advisor above that target is
   * absent from the call; when only the proxy exposes the annotated view (a repository or a scoped
   * proxy), the call re-enters the whole chain with no caller state. Both outcomes leave the
   * foreground call untouched, so this is a warning rather than a rejection: a read-only
   * transaction or a class-level guard is an ordinary shape that still loads the right value.
   * Advice the framework cannot prove harmless is named here instead of being classified, apart
   * from the plumbing listed in {@link #PLUMBING}.
   *
   * @param spec        the warm specification being bound
   * @param chain       the bean's target and the advice layers above it
   * @param target      the invocation target the load will use
   * @param targetClass the business class the load reports, against which the advisors' class
   *                    filters are evaluated
   */
  private static void warnUnhonouredAdvice(
    WarmSpec spec,
    PlanResolver.ProxyChain chain,
    Object target,
    Class<?> targetClass
  ) {
    List<String> bypassed = new ArrayList<>(2);
    for (Advised layer : chain.layers()) {
      for (Advisor advisor : layer.getAdvisors()) {
        String advice = adviceName(advisor);
        if (advice != null && matches(advisor, spec.specific(), targetClass) && !bypassed.contains(advice)) {
          bypassed.add(advice);
        }
      }
    }
    if (bypassed.isEmpty()) {
      return;
    }
    log.warn(
      SELF_WARM_ANNOTATION +
        " on {} binds a background load that {}. These advisors cannot run for it as " +
        "they do for a caller: {}. The foreground call keeps them; register the loader with a ZetaWarmConfigurer if " +
        "the body depends on one.",
      spec.specific(),
      PlanResolver.isProxy(target) ? "re-enters the proxy with no caller state" : "runs on the unwrapped target",
      bypassed
    );
  }

  /**
   * Returns the advice class that would be recorded for one advisor, or {@code null} to ignore it.
   *
   * @param advisor the advisor on the proxy chain
   * @return the advice class name, the advisor class name when the advice cannot be resolved, or
   *         {@code null} for the cache advice itself and for AOP plumbing
   */
  private static @Nullable String adviceName(Advisor advisor) {
    Advice advice;
    try {
      advice = advisor.getAdvice();
    } catch (RuntimeException unresolved) {
      return advisor.getClass().getName();
    }
    if (advice instanceof ZetaCacheInterceptor) {
      return null;
    }
    String name = advice.getClass().getName();
    return PLUMBING.contains(name) ? null : name;
  }

  /**
   * Returns whether {@code advisor} applies to {@code method}. A runtime matcher counts as a match
   * because the arguments are unavailable here, and a pointcut that cannot be evaluated is treated
   * as matching so the warning never hides an advisor.
   *
   * @param advisor     the advisor on the proxy chain
   * @param method      the warmed method
   * @param targetClass the authoritative user class of the proxy
   * @return {@code true} if the advisor intercepts the warmed method
   */
  private static boolean matches(Advisor advisor, Method method, Class<?> targetClass) {
    if (!(advisor instanceof PointcutAdvisor pointcutAdvisor)) {
      return !(advisor instanceof IntroductionAdvisor);
    }
    try {
      Pointcut pointcut = pointcutAdvisor.getPointcut();
      return (
        pointcut.getClassFilter().matches(targetClass) && pointcut.getMethodMatcher().matches(method, targetClass)
      );
    } catch (RuntimeException unevaluable) {
      return true;
    }
  }

  /**
   * Locates the single Spring bean matching the specification's scanned bean class. A JDK proxy
   * exposes only business interfaces, so a bean whose plan was scanned for the implementation is
   * matched by its proxy's ultimate target class. A non-instantiated {@code @Lazy} singleton is
   * skipped instead of being forced into existence, and an exact target-class match wins over an
   * inherited one when parent and child beans coexist.
   *
   * @param spec    the warm specification
   * @param advised the proxy beans discovered during this {@link #bindAll()} pass
   * @return the unique matching bean instance
   * @throws IllegalStateException if zero or multiple beans match {@link WarmSpec#beanClass()}
   */
  private Object findBean(WarmSpec spec, AdvisedBeans advised) {
    Class<?> expected = spec.beanClass();
    String[] names = beanFactory.getBeanNamesForType(expected, false, false);
    if (expected.isInterface()) {
      List<String> instantiated = new ArrayList<>(names.length);
      for (String name : names) {
        if (!isUninstantiatedLazy(name)) {
          instantiated.add(name);
        }
      }
      if (instantiated.size() == 1) {
        return beanFactory.getBean(instantiated.get(0));
      }
      throw instantiated.isEmpty() ? noBean(spec) : multipleBeans(spec, instantiated);
    }

    BeanMatches matches = new BeanMatches(expected);
    Set<String> visited = new HashSet<>(CollectionSizing.capacity(names.length + 1));
    for (String name : names) {
      visited.add(name);
      if (isUninstantiatedLazy(name)) {
        continue;
      }
      matches.record(name, beanFactory.getBean(name));
    }
    if (!matches.exactMatched()) {
      // A proxy that does not advertise the scanned class -- a JDK proxy exposing only business
      // interfaces, or a bean whose declared type is a supertype -- is only identifiable from the
      // target class its proxy reports.
      for (NamedBean entry : advised.all()) {
        if (!visited.contains(entry.name())) {
          matches.record(entry.name(), entry.bean());
        }
      }
    }
    return matches.resolve(spec);
  }

  /**
   * The proxy beans of this factory, resolved once per {@link #bindAll()} pass.
   *
   * <p>Only a specification whose scanned type matched no bean consults this list, so a container
   * with none never pays for it, while several self-warming proxies pay one type lookup and one
   * {@code getBean} per proxy instead of repeating both for every specification.
   */
  private final class AdvisedBeans {

    private @Nullable List<NamedBean> cached;

    private List<NamedBean> all() {
      if (cached == null) {
        String[] names = beanFactory.getBeanNamesForType(Advised.class, false, false);
        List<NamedBean> beans = new ArrayList<>(names.length);
        for (String name : names) {
          if (!isUninstantiatedLazy(name)) {
            beans.add(new NamedBean(name, beanFactory.getBean(name)));
          }
        }
        cached = beans;
      }
      return cached;
    }
  }

  private record NamedBean(String name, Object bean) {}

  /**
   * Returns whether {@code name} is a definition owned by this factory that has not been
   * instantiated yet, which at binding time means a {@code @Lazy} bean: binding runs after every
   * non-lazy singleton exists, and {@code getBeanNamesForType(..., false, false)} never reports a
   * prototype. A name from a parent factory is left alone — this factory's singleton registry
   * cannot tell whether the parent already created it, and skipping it would hide a valid bean.
   *
   * @param name the candidate bean name
   * @return {@code true} if resolving the name would force a lazy singleton into existence
   */
  private boolean isUninstantiatedLazy(String name) {
    return (
      beanFactory instanceof ConfigurableBeanFactory cbf &&
      beanFactory.containsBeanDefinition(name) &&
      !cbf.containsSingleton(name)
    );
  }

  private static IllegalStateException multipleBeans(WarmSpec spec, List<String> names) {
    return new IllegalStateException(
      SELF_WARM_ANNOTATION + " on " + spec.specific() + " matches several beans: " + String.join(", ", names)
    );
  }

  private static IllegalStateException noBean(WarmSpec spec) {
    return new IllegalStateException(
      SELF_WARM_ANNOTATION + " on " + spec.specific() + " has no bean of type " + spec.beanClass().getName()
    );
  }

  /**
   * Candidate beans for one warm specification, split by how they match the scanned bean class: a
   * bean whose ultimate target is that class exactly, and a bean whose target merely inherits it.
   * The exact match wins so a subclass bean cannot answer for its parent's loader; only the
   * winning group's size is checked for uniqueness.
   */
  private static final class BeanMatches {

    private final Class<?> expected;
    private final List<String> exactNames = new ArrayList<>(1);
    private final List<String> inheritedNames = new ArrayList<>(1);
    private @Nullable Object exact;
    private @Nullable Object inherited;

    private BeanMatches(Class<?> expected) {
      this.expected = expected;
    }

    private void record(String name, Object candidate) {
      Class<?> targetClass = AopProxyUtils.ultimateTargetClass(candidate);
      if (targetClass == expected) {
        if (exactNames.isEmpty()) {
          exact = candidate;
        }
        exactNames.add(name);
      } else if (expected.isAssignableFrom(targetClass)) {
        if (inheritedNames.isEmpty()) {
          inherited = candidate;
        }
        inheritedNames.add(name);
      }
    }

    private boolean exactMatched() {
      return !exactNames.isEmpty();
    }

    private Object resolve(WarmSpec spec) {
      List<String> names = exactMatched() ? exactNames : inheritedNames;
      Object bean = exactMatched() ? exact : inherited;
      if (names.size() == 1) {
        return Objects.requireNonNull(bean);
      }
      throw names.isEmpty() ? noBean(spec) : multipleBeans(spec, names);
    }
  }

  /**
   * Immutable bridge between one validated self-warming read plan and its background loader.
   *
   * @param beanClass the class the plan table was scanned for, used to locate the unique bean
   * @param encoding  the namespace key encoder and physical prefix
   * @param decoder   the inverse mapping from physical keys to method arguments
   * @param plan      the resolved read plan providing method identity and guard expressions
   */
  record WarmSpec(Class<?> beanClass, KeyEncoding encoding, KeyEncoding.Decoder decoder, CachePlan.Read plan) {
    /**
     * Validates non-null components of a warm specification.
     */
    WarmSpec {
      Objects.requireNonNull(beanClass, "beanClass must not be null");
      Objects.requireNonNull(encoding, "encoding must not be null");
      Objects.requireNonNull(decoder, "decoder must not be null");
      Objects.requireNonNull(plan, "plan must not be null");
    }

    /**
     * Validates a read plan for background self-warming and constructs its {@link WarmSpec}.
     *
     * @param beanClass the class the plan table was scanned for
     * @param encoding  the namespace key encoding
     * @param plan      the resolved read plan
     * @return the validated warm specification
     * @throws IllegalStateException if the plan uses {@code condition}, references invocation
     *                               context in {@code unless}, or has a non-invertible key
     */
    static WarmSpec of(Class<?> beanClass, KeyEncoding encoding, CachePlan.Read plan) {
      Objects.requireNonNull(beanClass, "beanClass must not be null");
      Objects.requireNonNull(encoding, "encoding must not be null");
      Objects.requireNonNull(plan, "plan must not be null");

      Method specific = plan.specific();
      if (!plan.condition().absent()) {
        throw new IllegalStateException(SELF_WARM_ANNOTATION + " on " + specific + " cannot combine with condition");
      }
      if (plan.hasUnless() && plan.unless().referencesInvocationContext()) {
        throw new IllegalStateException(SELF_WARM_ANNOTATION + " on " + specific + " only allows #result in unless");
      }

      KeyEncoding.Decoder decoder = buildDecoder(beanClass, encoding, specific, plan.key());
      return new WarmSpec(beanClass, encoding, decoder, plan);
    }

    /**
     * Returns the plan's most-specific annotated method.
     *
     * @return the specific target method
     */
    Method specific() {
      return plan.specific();
    }

    /**
     * Builds the physical-key decoder for a warmable method. A method is warmable only when its
     * physical key uniquely restores its argument list: a constant key on a zero-argument method,
     * the single parameter of a one-argument method, or a reversible default argument list.
     *
     * @param beanClass the scanned bean class used to resolve generic parameter types
     * @param encoding  the namespace key encoding
     * @param method    the specific annotated method
     * @param key       the plan's key strategy
     * @return the corresponding key decoder
     * @throws IllegalStateException if the key shape cannot be inverted into method arguments
     */
    private static KeyEncoding.Decoder buildDecoder(
      Class<?> beanClass,
      KeyEncoding encoding,
      Method method,
      CachePlan.Key key
    ) {
      int parameterCount = method.getParameterCount();
      if (key instanceof CachePlan.Key.Constant constant) {
        if (parameterCount != 0) {
          throw new IllegalStateException(
            SELF_WARM_ANNOTATION + " with a constant key on " + method + " requires a zero-argument method"
          );
        }
        return encoding.constantDecoder(constant.physicalKey());
      }
      if (key instanceof CachePlan.Key.SingleArgument single) {
        if (parameterCount != 1 || single.argumentIndex() != 0) {
          throw new IllegalStateException(
            SELF_WARM_ANNOTATION +
              " on " +
              method +
              " requires the key to cover all method parameters; use a ZetaWarmConfigurer loader instead"
          );
        }
        Class<?> resolved = resolveParameterType(method, 0, beanClass, method.getParameterTypes()[0]);
        return encoding.singleDecoder(resolved);
      }
      if (key instanceof CachePlan.Key.DefaultArguments) {
        Class<?>[] declared = method.getParameterTypes();
        Class<?>[] resolved = new Class<?>[declared.length];
        for (int i = 0; i < declared.length; i++) {
          resolved[i] = resolveParameterType(method, i, beanClass, declared[i]);
        }
        return encoding.argumentsDecoder(resolved);
      }
      throw new IllegalStateException(
        SELF_WARM_ANNOTATION +
          " on " +
          method +
          " does not support non-invertible SpEL keys; use a single parameter key, the default key, or a " +
          "ZetaWarmConfigurer loader"
      );
    }

    /**
     * Resolves a potentially generic parameter type in the context of {@code beanClass}, falling
     * back to the erased declared type when resolution is indeterminate.
     *
     * @param method       the annotated method
     * @param index        the zero-based parameter index
     * @param beanClass    the concrete bean class providing type variable bindings
     * @param declaredType the erased declared parameter type
     * @return the resolved parameter class
     */
    private static Class<?> resolveParameterType(Method method, int index, Class<?> beanClass, Class<?> declaredType) {
      return ResolvableType.forMethodParameter(method, index, beanClass).resolve(declaredType);
    }
  }

  /**
   * Background {@link CacheLoader} backing one self-warming {@code @ZetaCached} method.
   *
   * <p>This loader captures no caller state: each load decodes the physical key back into method
   * arguments, invokes the unwrapped target directly while bypassing cache advice, and evaluates
   * any result-only {@code unless} guard. Because the loader is retained by the warm scheduler for
   * the full TTL window, it must never retain an invocation, transaction, or security context.
   *
   * @param spec        the validated warm specification
   * @param target      the unwrapped target bean instance
   * @param targetClass the resolved user class of {@code target}
   * @param invocable   the concrete method to invoke reflectively on {@code target}
   * @param resolver    the plan resolver used to build {@code unless} evaluation contexts
   */
  private record AnnotatedWarmLoader(
    WarmSpec spec,
    Object target,
    Class<?> targetClass,
    Method invocable,
    PlanResolver resolver
  ) implements CacheLoader<String, Object> {
    /**
     * Decodes {@code key} back into method arguments and invokes the target method directly,
     * bypassing cache interception.
     *
     * @param key the physical cache key to reload
     * @return the loaded value (unwrapped when the method returns {@link java.util.Optional}), or
     *         {@code null} if the key does not belong to this method or the method returns empty
     * @throws IllegalStateException if the target method throws a checked exception
     */
    @Override
    public @Nullable Object load(String key) {
      Objects.requireNonNull(key, "key must not be null");
      Object[] args = spec.decoder().decode(key);
      if (args == null) {
        return null;
      }

      CachePlan.Read plan = spec.plan();
      try {
        Object value = ZetaCacheInterceptor.invokeBypassingCache(target, invocable, plan, args);
        return plan.unwrapResult(value);
      } catch (Error | RuntimeException failure) {
        throw failure;
      } catch (Throwable checked) {
        throw new IllegalStateException("background warm load failed for " + spec.specific(), checked);
      }
    }

    /**
     * Determines whether a loaded value is eligible to be written into the cache by checking for
     * {@code null} and evaluating any configured {@code unless} veto guard against {@code #result}.
     *
     * @param value the candidate value returned by {@link #load(String)}
     * @return {@code true} if {@code value} is non-null and not vetoed by {@code unless}
     */
    @Override
    public boolean isCacheable(@Nullable Object value) {
      if (value == null) {
        return false;
      }
      CachePlan.Read plan = spec.plan();
      CompiledExpression unless = plan.unless();
      if (unless.absent()) {
        return true;
      }
      ZetaEvaluationContext context = unless.needsContext()
        ? resolver.createContext(plan, target, targetClass, ParameterBindings.EMPTY_ARGS).withResult(value)
        : null;
      return !unless.evalVeto(context);
    }
  }
}
