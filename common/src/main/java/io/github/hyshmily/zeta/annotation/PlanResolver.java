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

import io.github.hyshmily.zeta.annotation.AnnotatedWarmLoaderBinder.WarmSpec;
import io.github.hyshmily.zeta.annotation.support.*;
import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.InvalidatePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.PutPolicy;
import io.github.hyshmily.zeta.policy.HeatTrigger;
import io.github.hyshmily.zeta.policy.KeyFormat;
import io.github.hyshmily.zeta.policy.NamespacePolicyRegistry;
import io.github.hyshmily.zeta.util.Merge;
import java.lang.annotation.Annotation;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.springframework.aop.SpringProxy;
import org.springframework.aop.TargetClassAware;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.framework.AopInfrastructureBean;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.core.*;
import org.springframework.core.annotation.AnnotationFilter;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.core.annotation.RepeatableContainers;
import org.springframework.expression.BeanResolver;
import org.springframework.expression.spel.SpelCompilerMode;
import org.springframework.expression.spel.SpelParserConfiguration;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

/**
 * Resolves an annotated method group into the single {@link CachePlan} executed by the cache
 * interceptor.
 *
 * <p>Resolution serves as the fail-fast boundary: a blank namespace, a {@code void}/{@code Void}
 * return type, conflicting verbs, an unparsable SpEL expression, an unknown variable, or a pre-body
 * {@code #result} reference fails bean creation rather than waiting for the first request.
 * Annotation lookup reads each method's or type's own {@code DIRECT} declarations through Spring's
 * {@link MergedAnnotations}, so composed annotations and {@code @AliasFor} overrides resolve
 * uniformly while the explicit hierarchy walk decides which type contributes which attribute.
 *
 * <p>The first query against a class resolves every candidate method once; subsequent lookups are
 * served from an immutable per-class table.
 *
 * <p><b>Two-phase initialization.</b> This resolver is constructed while the auto-proxy advisor is
 * built, before {@code BeanPostProcessor} registration completes. It therefore accepts the
 * namespace contract registry and key-format registry as lazy suppliers to avoid eagerly
 * instantiating the cache engine or user beans ahead of post-processing.
 *
 * <p><b>Warm specs.</b> A method declaring {@code selfWarmLoader = true} contributes a
 * {@link WarmSpec} keyed by physical namespace. Two distinct methods claiming the same namespace
 * fail resolution so no ambiguous loader is ever published.
 */
public final class PlanResolver {

  private static final String ZETA_CACHED = "@ZetaCached";
  private static final String ZETA_PUT = "@ZetaPut";
  private static final String ZETA_INVALIDATE = "@ZetaInvalidate";

  private static final RepeatableContainers REPEATABLE_INVALIDATES = RepeatableContainers.of(
    ZetaInvalidate.class,
    ZetaInvalidates.class
  );

  /** Embedded-value resolution for {@code ${...}} in {@code cache} and TTL attributes. */
  private final @Nullable ConfigurableBeanFactory beanFactory;

  /** Late-bound namespace contract registry consulted by the first plan publishing a contract. */
  private final Supplier<@Nullable NamespacePolicyRegistry> contractsSupplier;

  /** Late-bound plan-time key-format registry consulted by the first resolved plan. */
  private final Supplier<@Nullable NamespaceKeys> keysSupplier;

  /** Whether {@link #contracts} has been resolved; written under {@code this}. */
  private volatile boolean contractsResolved;

  /** Resolved contract registry, or {@code null} if the supplier yielded none. */
  private volatile @Nullable NamespacePolicyRegistry contracts;

  /** Resolved key-format registry, falling back to a TYPED-only instance when absent. */
  private volatile @Nullable NamespaceKeys keys;

  private final @Nullable BeanResolver beanResolver;

  /**
   * SpEL parser in {@link SpelCompilerMode#MIXED} mode, bound to the bean factory's class loader
   * (or the default class loader when none is configured).
   */
  private final SpelExpressionParser parser;

  /** Reflection caches shared across evaluation phases and scoped to this resolver's lifecycle. */
  private final ZetaEvaluationContext.ExpressionSupport expressionSupport;

  private final ParameterNameDiscoverer parameterNameDiscoverer = new DefaultParameterNameDiscoverer();

  /**
   * Per-target-class plan tables. Scoped to this resolver rather than a static {@link ClassValue}
   * so cached plans and compiled SpEL do not pin application class loaders across restarts.
   */
  private final Map<Class<?>, PlanTable> classPlans = new ConcurrentHashMap<>();

  /**
   * Reflective target methods keyed by {@code (plan method, target class)}. A proxy or repository
   * hit asks the same question on every call, and the answer is a pure function of that pair.
   */
  private final Map<MethodTargetKey, Optional<Method>> invocableMethods = new ConcurrentHashMap<>();

  /**
   * Warmed methods keyed by physical namespace prefix; guarded by {@code synchronized (warmSpecs)}.
   */
  private final Map<String, WarmSpec> warmSpecs = new LinkedHashMap<>();

  /**
   * Creates a standalone resolver with no namespace registries: plans are built and validated, no
   * contracts are published, and every namespace uses {@link KeyFormat#TYPED}.
   *
   * @param beanFactory the bean factory backing {@code @beanName} SpEL and, when it is a
   *                    {@link ConfigurableBeanFactory}, {@code ${...}} placeholders; may be
   *                    {@code null} to disable both
   * @throws IllegalStateException if no class loader is available for SpEL
   */
  public PlanResolver(@Nullable BeanFactory beanFactory) {
    this(beanFactory, () -> null, () -> null);
  }

  /**
   * Creates a plan resolver with lazy namespace registries.
   *
   * @param beanFactory the bean factory backing {@code @beanName} SpEL and {@code ${...}}
   *                    placeholders; may be {@code null} to disable both
   * @param contracts   supplier returning the registry for namespace TTL and warming contracts, or
   *                    supplying {@code null} to disable contract publication
   * @param keys        supplier returning the plan-time key-format registry, or supplying
   *                    {@code null} for a {@link KeyFormat#TYPED}-only resolver
   * @throws IllegalStateException if no class loader is available for SpEL
   */
  public PlanResolver(
    @Nullable BeanFactory beanFactory,
    Supplier<@Nullable NamespacePolicyRegistry> contracts,
    Supplier<@Nullable NamespaceKeys> keys
  ) {
    this.contractsSupplier = Objects.requireNonNull(contracts, "contracts must not be null");
    this.keysSupplier = Objects.requireNonNull(keys, "keys must not be null");
    this.beanFactory = beanFactory instanceof ConfigurableBeanFactory configurable ? configurable : null;
    this.beanResolver = beanFactory == null ? null : new BeanFactoryResolver(beanFactory);
    ClassLoader classLoader = resolveClassLoader(this.beanFactory);
    this.parser = new SpelExpressionParser(new SpelParserConfiguration(SpelCompilerMode.MIXED, classLoader));
    this.expressionSupport = new ZetaEvaluationContext.ExpressionSupport(classLoader);
  }

  /**
   * Answers Spring's pointcut matching check and triggers fail-fast validation of the target class.
   *
   * <p>Any malformed cache annotation on {@code targetClass} fails here during bean creation rather
   * than on the first invocation.
   *
   * @param method      the candidate method queried by the pointcut
   * @param targetClass the target class resolved by the pointcut
   * @return {@code true} if the method is eligible and carries a resolved {@link CachePlan}
   */
  boolean matches(Method method, Class<?> targetClass) {
    return !ignored(method) && lookupPlanForMethod(method, targetClass) != null;
  }

  /**
   * Resolves the {@link CachePlan} for one method on the given target class.
   *
   * <p>The target class's table is computed once and shared between proxies and their user classes.
   * When a direct lookup misses, resolution falls back first to the method's declaring class (for
   * nested interface proxies whose target does not implement the nested view) and then to the
   * most-specific method on the user class.
   *
   * @param method      the invoked or queried method
   * @param targetClass the class against which the plan is resolved
   * @return the resolved {@link CachePlan}, or {@code null} if no view carries a cache verb
   */
  @Nullable
  CachePlan lookupPlanForMethod(Method method, Class<?> targetClass) {
    PlanTable table = lookupOrScanPlanTable(targetClass);
    CachePlan plan = table.plans().get(method);
    if (plan != null) {
      return plan;
    }
    Class<?> userClass = table.userClass();
    if (!method.getDeclaringClass().isAssignableFrom(userClass)) {
      plan = lookupOrScanPlanTable(method.getDeclaringClass()).plans().get(method);
      if (plan != null) {
        return plan;
      }
    }
    if (table.plans().isEmpty()) {
      return null;
    }
    Method specific = BridgeMethodResolver.getMostSpecificMethod(method, userClass);
    return specific == method ? null : table.plans().get(specific);
  }

  /**
   * Returns the already-cached plan table for a receiver class without triggering a scan.
   *
   * <p>Used on the interceptor's hot path to bypass proxy classification on steady-state hits.
   *
   * @param receiverClass the runtime class of the invocation receiver
   * @return the cached {@link PlanTable}, or {@code null} if the class has not been scanned
   */
  @Nullable
  PlanTable cachedTable(Class<?> receiverClass) {
    return classPlans.get(receiverClass);
  }

  /**
   * Builds a fresh SpEL evaluation context for one invocation phase.
   *
   * @param plan        the plan being executed
   * @param target      the unwrapped target instance, or {@code null}
   * @param targetClass the authoritative target class
   * @param args        the invocation arguments
   * @return a thread-confined evaluation context
   */
  ZetaEvaluationContext createContext(CachePlan plan, @Nullable Object target, Class<?> targetClass, Object[] args) {
    return ZetaEvaluationContext.create(
      target,
      targetClass,
      plan.specific(),
      args,
      plan.bindings(),
      beanResolver,
      expressionSupport
    );
  }

  /**
   * Returns an immutable snapshot of all {@link WarmSpec} entries collected during plan resolution.
   *
   * @return an immutable collection of collected warm specifications
   */
  Collection<WarmSpec> warmSpecs() {
    synchronized (warmSpecs) {
      return List.copyOf(warmSpecs.values());
    }
  }

  /**
   * Finds the most-specific non-infrastructure interface among {@code interfaces} that extends or
   * equals {@code declaring}.
   *
   * @param interfaces the candidate interfaces
   * @param declaring  the method's declaring type
   * @return the most-specific matching business interface, or {@code null} if none matches
   */
  public static @Nullable Class<?> mostSpecificInterface(Class<?>[] interfaces, Class<?> declaring) {
    Objects.requireNonNull(interfaces, "interfaces must not be null");
    Objects.requireNonNull(declaring, "declaring must not be null");
    Class<?> view = null;
    for (Class<?> candidate : interfaces) {
      if (
        !isInfrastructureClass(candidate) &&
        declaring.isAssignableFrom(candidate) &&
        (view == null || view.isAssignableFrom(candidate))
      ) {
        view = candidate;
      }
    }
    return view;
  }

  /**
   * Resolves the invocation target of {@code candidate} together with every advice layer the walk
   * passes through.
   *
   * @param candidate the invocation receiver, or {@code null}
   * @param method    the reported method the unwrapped target must still expose
   * @return the target and the proxy layers above it, outermost first
   */
  static ProxyChain proxyChain(@Nullable Object candidate, Method method) {
    List<Advised> layers = new ArrayList<>(2);
    Object target = walkToTarget(candidate, method, layers);
    return new ProxyChain(target, List.copyOf(layers));
  }

  /**
   * Unwraps nested Spring proxies down to the deepest target that still exposes every proxied
   * business view assignable to {@code method}'s declaring class.
   *
   * <p>Unrelated mixin introductions do not block unwrapping because they are not assignable to the
   * method's declaring type.
   *
   * @param candidate the invocation receiver, or {@code null}
   * @param method    the reported method the unwrapped target must still expose
   * @return the deepest valid target, or {@code null} when {@code candidate} is {@code null}
   */
  static @Nullable Object unwrapTarget(@Nullable Object candidate, Method method) {
    return walkToTarget(candidate, method, null);
  }

  /**
   * Walks {@code candidate} down its static targets, optionally recording the layers it crosses.
   *
   * @param candidate the invocation receiver, or {@code null}
   * @param method    the reported method each target must still expose
   * @param layers    receives every crossed proxy layer when non-{@code null}; a {@code null} sink
   *                  keeps the per-call unwrap free of allocation
   * @return the deepest valid target, or {@code null} when {@code candidate} is {@code null}
   */
  private static @Nullable Object walkToTarget(
    @Nullable Object candidate,
    Method method,
    @Nullable List<Advised> layers
  ) {
    Object current = candidate;
    while (current instanceof Advised advised) {
      if (layers != null) {
        layers.add(advised);
      }

      Object next = AopProxyUtils.getSingletonTarget(advised);
      if (
        next == current ||
        !method.getDeclaringClass().isInstance(next) ||
        !implementsMethodViews(advised, next, method.getDeclaringClass())
      ) {
        return current;
      }
      current = next;
    }
    return current;
  }

  /**
   * Resolves the authoritative class used to key plan tables and {@code #root.targetClass}.
   *
   * <p>For non-proxy targets and proxies whose ultimate target still implements the most-specific
   * business view, the ultimate target class is returned; otherwise the most-specific exposed
   * interface is returned (handling Spring Data repository proxies whose target is a generic base
   * implementation).
   *
   * @param target    the invocation receiver, or {@code null}
   * @param declaring the reported method's declaring type
   * @return the authoritative target class
   */
  static Class<?> resolveTargetClass(@Nullable Object target, Class<?> declaring) {
    if (target == null) {
      return declaring;
    }

    Class<?> targetType = target.getClass();
    Class<?> ultimate = AopProxyUtils.ultimateTargetClass(target);
    if (!Proxy.isProxyClass(targetType)) {
      return declaring.isAssignableFrom(ultimate) ? ultimate : declaring;
    }

    Class<?> view = mostSpecificInterface(targetType.getInterfaces(), declaring);
    if (view == null) {
      return declaring.isAssignableFrom(ultimate) ? ultimate : declaring;
    }
    return !Proxy.isProxyClass(ultimate) && view.isAssignableFrom(ultimate) ? ultimate : view;
  }

  /**
   * Returns whether {@code ifc} is a Spring AOP infrastructure interface excluded from business
   * view resolution.
   *
   * <p>Spring declares its markers as super-interfaces of one another — {@code TransactionalProxy}
   * extends {@link SpringProxy}, {@link Advised} extends {@link TargetClassAware} — and a proxy
   * exposes those subtypes next to the business interfaces. A check by identity keeps a subtype in
   * the business set, where it competes with the real view for the proxy's scan root.
   *
   * @param ifc the candidate interface
   * @return {@code true} for {@link SpringProxy}, {@link Advised}, {@link DecoratingProxy},
   *         {@link AopInfrastructureBean} and their subtypes, or {@link TargetClassAware} itself
   */
  static boolean isInfrastructureClass(Class<?> ifc) {
    return (
      SpringProxy.class.isAssignableFrom(ifc) ||
      Advised.class.isAssignableFrom(ifc) ||
      DecoratingProxy.class.isAssignableFrom(ifc) ||
      AopInfrastructureBean.class.isAssignableFrom(ifc) ||
      ifc == TargetClassAware.class
    );
  }

  /**
   * Returns whether {@code candidate} is a proxy rather than a body: a CGLIB or JDK AOP proxy, or a
   * bare JDK dynamic proxy. Invoking one re-enters an advice chain instead of running a method.
   *
   * @param candidate the object to test
   * @return {@code true} if {@code candidate} is a proxy
   */
  static boolean isProxy(Object candidate) {
    return (
      candidate instanceof Advised || candidate instanceof SpringProxy || Proxy.isProxyClass(candidate.getClass())
    );
  }

  /**
   * Resolves the concrete reflective {@link Method} to invoke on {@code target} during background
   * refresh, falling back to an erased supertype implementation when necessary, and makes it
   * accessible.
   *
   * @param specific the plan's most-specific method
   * @param target   the unwrapped invocation target
   * @return an accessible {@link Method} invocable on {@code target}
   * @throws IllegalStateException if no compatible method exists on {@code target}
   */
  Method targetMethod(Method specific, Object target) {
    Class<?> targetType = target.getClass();
    Method method = resolveInvocable(specific, targetType);
    if (method == null) {
      throw new IllegalStateException(
        "no method on " + targetType.getName() + " can invoke " + specific + " for a reflective refresh"
      );
    }
    return method;
  }

  /**
   * Returns the cached, accessible target method for one {@code (plan method, target class)} pair.
   *
   * @param specific   the plan's most-specific method
   * @param targetType the runtime class of the invocation target
   * @return the accessible method, or {@code null} if the target cannot invoke it
   */
  private @Nullable Method resolveInvocable(Method specific, Class<?> targetType) {
    return invocableMethods
      .computeIfAbsent(new MethodTargetKey(specific, targetType), key ->
        Optional.ofNullable(findInvocableMethod(key.method(), key.targetType()))
      )
      .orElse(null);
  }

  /**
   * Selects the method to invoke reflectively on {@code targetType}, preferring Spring's invocable
   * view and falling back to an erased declaration the target can actually run.
   *
   * @param specific   the plan's most-specific method
   * @param targetType the runtime class of the invocation target
   * @return the accessible method, or {@code null} if none exists
   */
  private static @Nullable Method findInvocableMethod(Method specific, Class<?> targetType) {
    Method method = null;
    try {
      method = AopUtils.selectInvocableMethod(specific, targetType);
    } catch (IllegalStateException notInvocable) {
      // Fall back to an erased implementation when the target only implements a generic supertype.
    }
    if (
      method == null ||
      !method.getDeclaringClass().isAssignableFrom(targetType) ||
      Modifier.isStatic(method.getModifiers())
    ) {
      method = findErasedMethod(specific, targetType);
    }
    if (method != null) {
      ReflectionUtils.makeAccessible(method);
    }
    return method;
  }

  /**
   * Returns the cached plan table for {@code targetClass}, scanning and publishing it on first
   * access. CGLIB proxies share their user class's table while JDK proxies scan their business
   * interfaces together.
   *
   * @param targetClass the class to look up or scan
   * @return the immutable {@link PlanTable}
   */
  private PlanTable lookupOrScanPlanTable(Class<?> targetClass) {
    PlanTable cached = classPlans.get(targetClass);
    if (cached != null) {
      return cached;
    }
    Class<?> userClass = ClassUtils.getUserClass(targetClass);
    if (userClass != targetClass) {
      return register(targetClass, userClass, lookupOrScanPlanTable(userClass).plans());
    }
    return Proxy.isProxyClass(userClass) ? scanProxy(userClass) : register(userClass, userClass, scanClass(userClass));
  }

  /**
   * Publishes a newly constructed {@link PlanTable} under {@code targetClass}, returning the
   * existing table if another thread won the race.
   *
   * @param targetClass the lookup key class
   * @param userClass   the resolved user class
   * @param plans       the immutable map of method views to plans
   * @return the published {@link PlanTable}
   */
  private PlanTable register(Class<?> targetClass, Class<?> userClass, Map<Method, CachePlan> plans) {
    PlanTable created = new PlanTable(userClass, plans);
    PlanTable existing = classPlans.putIfAbsent(targetClass, created);
    return existing != null ? existing : created;
  }

  /**
   * Builds the plan table of a JDK dynamic proxy from its business interfaces.
   *
   * <p>The generated class is an implementation detail no expression may observe, so the table is
   * published under the single business interface whenever the proxy has one. Infrastructure
   * interfaces are excluded by {@link #isInfrastructureClass}; a business interface that another
   * business interface already extends is excluded too, because {@link #collectScanClasses} reaches
   * it through the interface graph — Spring Data publishes {@code UserRepository} next to the
   * {@code Repository} marker it extends, and counting both left the proxy with no single view.
   *
   * <p>One root reuses that interface's table, so the proxy lookup and the interface lookup answer
   * with one plan instance and every expression is compiled once. Only a proxy that genuinely
   * exposes sibling business views is scanned as itself.
   *
   * @param proxyClass the generated JDK proxy class
   * @return the published plan table
   */
  private PlanTable scanProxy(Class<?> proxyClass) {
    List<Class<?>> candidates = new ArrayList<>();
    for (Class<?> candidate : proxyClass.getInterfaces()) {
      if (!isInfrastructureClass(candidate)) {
        candidates.add(candidate);
      }
    }
    if (candidates.isEmpty()) {
      return register(proxyClass, proxyClass, Map.of());
    }
    List<Class<?>> roots = maximalInterfaces(candidates);
    if (roots.size() == 1) {
      Class<?> single = roots.get(0);
      return register(proxyClass, single, lookupOrScanPlanTable(single).plans());
    }
    return register(proxyClass, proxyClass, scanRoots(new LinkedHashSet<>(roots)));
  }

  /**
   * Scans a single concrete or interface root type.
   *
   * @param root the user class to scan
   * @return the immutable plan map
   */
  private Map<Method, CachePlan> scanClass(Class<?> root) {
    return scanRoots(Set.of(root));
  }

  /**
   * Scans one or more root types together, building one {@link CachePlan} per logical method
   * signature group and publishing any validated {@link WarmSpec} declarations.
   *
   * <p>Orchestration delegates to {@link #collectScanClasses}, {@link #groupViewsBySignature},
   * {@link #buildPlansForGroups}, {@link #publishWarmSpecs} and the key-format staging commit.
   *
   * @param roots the root classes or business interfaces to scan
   * @return an immutable plan map keyed by every view of each cached method group
   * @throws IllegalStateException if a group declares conflicting verbs or two methods claim the
   *                               same self-warmed namespace
   */
  private Map<Method, CachePlan> scanRoots(Set<Class<?>> roots) {
    Set<Class<?>> classes = collectScanClasses(roots);
    if (!hasCacheVerb(classes)) {
      // Most beans carry no cache annotation; skip bridge resolution, generic signature building
      // and method grouping for them entirely.
      return Map.of();
    }
    ViewGroups groups = groupViewsBySignature(classes, roots);
    NamespaceKeys.Staging keyStaging = keys().staging();
    BuiltPlans built = buildPlansForGroups(roots, groups, keyStaging);
    // Nothing is published until every group in this scan has validated: when a later method fails,
    // an earlier method's TTL, trigger or key-format declaration must not survive in the registry.
    publishWarmSpecs(built.warmed());
    keyStaging.commit();
    for (PendingContract contract : built.contracts()) {
      registerContract(contract.prefix(), contract.hard(), contract.soft(), contract.prewarm(), contract.keepWarm());
    }
    return Map.copyOf(built.resolved());
  }

  /**
   * Returns whether any scanned class declares a cache verb method. The check runs before the
   * expensive grouping pass so an unannotated bean publishes an empty table immediately.
   *
   * @param classes every type included in the scan
   * @return {@code true} when at least one method carries a cache verb annotation
   */
  private static boolean hasCacheVerb(Set<Class<?>> classes) {
    for (Class<?> candidateClass : classes) {
      for (Method candidate : ReflectionUtils.getDeclaredMethods(candidateClass)) {
        // A method with no declared annotation cannot carry a verb, and this runs for every method
        // of every scanned type; the merged scan below is only paid for a candidate that has one.
        if (ignored(candidate) || candidate.getDeclaredAnnotations().length == 0) {
          continue;
        }
        // The same merged view `declaredVerb` reads, so a repeatable @ZetaInvalidates container and a
        // meta-present verb are detected here exactly as they are there.
        MergedAnnotations annotations = MergedAnnotations.from(
          candidate,
          MergedAnnotations.SearchStrategy.DIRECT,
          REPEATABLE_INVALIDATES,
          AnnotationFilter.PLAIN
        );
        if (
          annotations.isPresent(ZetaCached.class) ||
          annotations.isPresent(ZetaPut.class) ||
          annotations.isPresent(ZetaInvalidate.class)
        ) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Collects every root type plus its superclass chain and every transitively implemented or
   * extended interface, each exactly once.
   *
   * @param roots the initial scan roots
   * @return the ordered set of all types whose declared methods should be inspected
   */
  private static Set<Class<?>> collectScanClasses(Set<Class<?>> roots) {
    Set<Class<?>> classes = new LinkedHashSet<>();
    Deque<Class<?>> pending = new ArrayDeque<>(roots);
    while (!pending.isEmpty()) {
      Class<?> current = pending.removeFirst();
      if (current == Object.class || !classes.add(current)) {
        continue;
      }
      Class<?> superclass = current.getSuperclass();
      if (superclass != null) {
        pending.add(superclass);
      }
      Collections.addAll(pending, current.getInterfaces());
    }
    return classes;
  }

  /**
   * Groups all candidate method views by erased signature and determines each group's most-specific
   * owner method.
   *
   * <p>A child interface that specializes its parent's type variable produces no bridge method, so
   * {@code find(Long)} and {@code find(Object)} are two views of one logical method only after the
   * parent view is resolved against the child's type arguments.
   *
   * @param classes all classes and interfaces included in the scan
   * @param roots   the scan roots used to resolve each candidate to its most-specific view
   * @return the grouped method views and their canonical owners
   */
  private static ViewGroups groupViewsBySignature(Set<Class<?>> classes, Set<Class<?>> roots) {
    Map<MethodSignature, Method> specificBySignature = new LinkedHashMap<>();
    Map<MethodSignature, Set<Method>> viewsBySignature = new LinkedHashMap<>();
    for (Class<?> candidateClass : classes) {
      for (Method candidate : ReflectionUtils.getDeclaredMethods(candidateClass)) {
        if (ignored(candidate)) {
          continue;
        }
        Method specific = getSpecificMethod(candidate, roots, classes);
        MethodSignature signature = new MethodSignature(specific);
        specificBySignature.merge(signature, specific, PlanResolver::moreSpecificOwner);
        viewsBySignature.computeIfAbsent(signature, key -> new LinkedHashSet<>()).add(candidate);
      }
    }
    return new ViewGroups(specificBySignature, viewsBySignature);
  }

  /**
   * Builds one {@link CachePlan} per signature group and maps every view of the group to that plan.
   *
   * <p>The verb is resolved before the class-level defaults are read: most scanned methods carry no
   * cache annotation, and a hierarchy scan per unannotated method dominated startup.
   *
   * @param roots      the scan roots used to select each group's bean class
   * @param groups     the grouped method views and owners
   * @param keyStaging the scan's provisional key-format declarations
   * @return the resolved plan map, the collected {@link WarmSpec} entries, and the namespace
   *         contracts awaiting publication
   */
  private BuiltPlans buildPlansForGroups(Set<Class<?>> roots, ViewGroups groups, NamespaceKeys.Staging keyStaging) {
    Map<Method, CachePlan> resolved = new HashMap<>();
    List<WarmSpec> warmed = new ArrayList<>();
    List<PendingContract> contracts = new ArrayList<>();
    Map<Class<?>, Optional<ClassConfig>> directConfigs = new HashMap<>(4);
    for (Map.Entry<MethodSignature, Set<Method>> entry : groups.viewsBySignature().entrySet()) {
      Method specific = Objects.requireNonNull(
        groups.specificBySignature().get(entry.getKey()),
        "missing specific method for signature"
      );
      Set<Method> views = entry.getValue();
      Verb verb = resolveVerb(specific, views);
      if (verb == null) {
        continue;
      }

      Class<?> beanClass = selectBeanClass(roots, specific);
      ClassConfig config = resolveConfig(roots, beanClass, views, directConfigs);
      CachePlan plan = buildPlan(beanClass, specific, verb, views, config, keyStaging, warmed, contracts);
      resolved.put(specific, plan);
      for (Method view : views) {
        resolved.put(view, plan);
      }
    }
    return new BuiltPlans(resolved, warmed, contracts);
  }

  /**
   * Builds the {@link CachePlan} for one logical method group.
   *
   * @param beanClass the scanned root used for class defaults and warm bean lookup
   * @param specific  the group's most-specific method
   * @param verb      the resolved verb declared by the group
   * @param views      every method view belonging to the group
   * @param config     the class-level defaults on {@code beanClass}, or {@code null}
   * @param keyStaging the scan's provisional key-format declarations
   * @param warmed     accumulator receiving any {@link WarmSpec} declared by a read plan
   * @param contracts  accumulator receiving the namespace contract declared by a read or write plan
   * @return the built {@link CachePlan}
   */
  private CachePlan buildPlan(
    Class<?> beanClass,
    Method specific,
    Verb verb,
    Set<Method> views,
    @Nullable ClassConfig config,
    NamespaceKeys.Staging keyStaging,
    List<WarmSpec> warmed,
    List<PendingContract> contracts
  ) {
    String[] parameterNames = parameterNames(verb.view(), specific, views);
    ParameterBindings bindings = ParameterBindings.of(specific, parameterNames);
    MethodResolution resolution = new MethodResolution(beanClass, specific, bindings, config, keyStaging);

    if (verb instanceof Verb.Read read) {
      return resolution.readPlan(read.annotation(), warmed, contracts);
    } else if (verb instanceof Verb.Write write) {
      return resolution.writePlan(write.annotation(), contracts);
    } else if (verb instanceof Verb.Invalidate invalidate) {
      List<ZetaInvalidate> annotations = invalidate.annotations();
      List<CachePlan.Invalidate.Operation> operations = new ArrayList<>(annotations.size());
      for (ZetaInvalidate annotation : annotations) {
        operations.add(resolution.invalidateOperation(annotation));
      }

      return CachePlan.Invalidate.of(specific, bindings, operations, returnsOptional(beanClass, specific));
    }

    throw new IllegalArgumentException("Unsupported verb: " + verb);
  }

  /**
   * Atomically validates an entire batch of {@link WarmSpec} entries against both the batch itself
   * and previously published loaders before registering them.
   *
   * @param warmed the warm specifications collected from one scan
   * @throws IllegalStateException if two distinct loaders claim the same physical namespace
   */
  private void publishWarmSpecs(List<WarmSpec> warmed) {
    if (warmed.isEmpty()) {
      return;
    }
    synchronized (warmSpecs) {
      Map<String, WarmSpec> batch = new LinkedHashMap<>(warmed.size());
      for (WarmSpec spec : warmed) {
        String prefix = spec.encoding().physicalPrefix();
        WarmSpec existing = batch.get(prefix);
        if (existing == null) {
          existing = warmSpecs.get(prefix);
        }
        if (existing != null && !sameLoader(existing, spec)) {
          throw new IllegalStateException(
            "namespace '" +
              spec.encoding().namespace() +
              "' declares more than one selfWarmLoader method: " +
              existing.beanClass().getName() +
              " (" +
              existing.specific() +
              ") and " +
              spec.beanClass().getName() +
              " (" +
              spec.specific() +
              ")"
          );
        }
        batch.merge(prefix, spec, PlanResolver::moreSpecificWarmSpec);
      }
      batch.forEach((prefix, spec) -> warmSpecs.merge(prefix, spec, PlanResolver::moreSpecificWarmSpec));
    }
  }

  /**
   * Upgrades a published warm spec to the concrete implementation class when the same namespace is
   * first seen through an interface view and later through the bean. The loader identity is already
   * validated by {@link #sameLoader}; only the scanned bean class (used to locate the bean and its
   * class-level defaults) is refined.
   *
   * @param current   the spec already published for this namespace
   * @param candidate the newly scanned spec for the same namespace
   * @return the more specific spec
   */
  private static WarmSpec moreSpecificWarmSpec(WarmSpec current, WarmSpec candidate) {
    return current.beanClass() != candidate.beanClass() && current.beanClass().isAssignableFrom(candidate.beanClass())
      ? candidate
      : current;
  }

  /**
   * Resolves the single cache verb declared across all views of a method group, rejecting mixed
   * verb types or conflicting maximal declarations.
   *
   * @param specific the group's most-specific method
   * @param views    every method view belonging to the group
   * @return the winning {@link Verb}, or {@code null} if no view declares a cache verb
   * @throws IllegalStateException if the group mixes verb types or two maximal declarations conflict
   */
  private static @Nullable Verb resolveVerb(Method specific, Set<Method> views) {
    List<Verb> declarations = new ArrayList<>(views.size());
    for (Method view : views) {
      if (view.isBridge() && !view.equals(specific)) {
        continue;
      }
      Verb verb = declaredVerb(view);
      if (verb != null) {
        declarations.add(verb);
      }
    }
    if (declarations.isEmpty()) {
      return null;
    }

    String role = declarations.get(0).role();
    for (Verb verb : declarations) {
      if (!verb.role().equals(role)) {
        throw new IllegalStateException(
          ZETA_CACHED +
            ", " +
            ZETA_PUT +
            " and " +
            ZETA_INVALIDATE +
            " are mutually exclusive, but " +
            specific +
            " declares conflicting verbs: " +
            declarations.stream().map(Verb::role).distinct().toList()
        );
      }
    }

    // Compare only maximal views in the declaring-type partial order: an overridden declaration is
    // an attribute source rather than a competing configuration.
    List<Verb> maximal = new ArrayList<>(declarations.size());
    for (Verb candidate : declarations) {
      boolean dominated = false;
      for (Verb other : declarations) {
        if (other != candidate && dominates(other.view(), candidate.view())) {
          dominated = true;
          break;
        }
      }
      if (!dominated) {
        maximal.add(candidate);
      }
    }
    return chooseWinner(specific, maximal);
  }

  /**
   * Selects the winning maximal verb declaration (preferring the declaration on {@code specific})
   * and rejects any disagreeing maximal sibling declaration.
   *
   * @param specific the group's most-specific method
   * @param maximal  the non-empty list of maximal verb declarations
   * @return the winning {@link Verb}
   * @throws IllegalStateException if two maximal declarations disagree
   */
  private static @NonNull Verb chooseWinner(Method specific, List<Verb> maximal) {
    Verb winner = maximal.get(0);
    for (Verb candidate : maximal) {
      if (candidate.view().equals(specific)) {
        winner = candidate;
        break;
      }
    }
    for (Verb candidate : maximal) {
      if (!winner.sameDeclaration(candidate)) {
        throw new IllegalStateException(
          "conflicting " +
            winner.role() +
            " declarations for " +
            specific +
            ": " +
            winner.declaration() +
            " vs " +
            candidate.declaration() +
            "; declare one on the implementation method"
        );
      }
    }
    return winner;
  }

  /**
   * Inspects direct and meta-annotations on one method view and extracts its single cache verb.
   *
   * @param method the method view to inspect
   * @return the declared {@link Verb}, or {@code null} if none is present
   * @throws IllegalStateException if the view mixes verb types or declares conflicting duplicates
   */
  private static @Nullable Verb declaredVerb(Method method) {
    if (method.getDeclaredAnnotations().length == 0) {
      // A method with no declared annotation cannot carry a verb: a composed annotation is itself
      // declared here, and the merged view below can only add to declared annotations.
      return null;
    }
    MergedAnnotations annotations = MergedAnnotations.from(
      method,
      MergedAnnotations.SearchStrategy.DIRECT,
      REPEATABLE_INVALIDATES,
      AnnotationFilter.PLAIN
    );
    ZetaCached cached = null;
    ZetaPut put = null;
    List<ZetaInvalidate> invalidates = null;

    for (MergedAnnotation<Annotation> merged : annotations) {
      Class<? extends Annotation> type = merged.getType();
      if (type == ZetaCached.class) {
        cached = mergeAnnotation(ZETA_CACHED, cached, (ZetaCached) merged.synthesize(), method);
      } else if (type == ZetaPut.class) {
        put = mergeAnnotation(ZETA_PUT, put, (ZetaPut) merged.synthesize(), method);
      } else if (type == ZetaInvalidate.class) {
        if (invalidates == null) {
          invalidates = new ArrayList<>(2);
        }
        invalidates.add((ZetaInvalidate) merged.synthesize());
      }
    }

    int kindCount = (cached != null ? 1 : 0) + (put != null ? 1 : 0) + (invalidates != null ? 1 : 0);
    if (kindCount == 0) {
      return null;
    }
    if (kindCount > 1) {
      throw new IllegalStateException(
        ZETA_CACHED + ", " + ZETA_PUT + " and " + ZETA_INVALIDATE + " are mutually exclusive on " + method
      );
    }
    if (cached != null) {
      return new Verb.Read(method, cached);
    }
    if (put != null) {
      return new Verb.Write(method, put);
    }
    return new Verb.Invalidate(method, List.copyOf(new LinkedHashSet<>(Objects.requireNonNull(invalidates))));
  }

  /**
   * Merges a repeated declaration of the same single-value verb annotation on one method view.
   *
   * @param role      the annotation role used in failure messages
   * @param current   the previously observed declaration, or {@code null}
   * @param candidate the newly observed declaration
   * @param method    the declaring method view
   * @param <A>       the annotation type
   * @return the surviving annotation instance
   * @throws IllegalStateException if {@code current} and {@code candidate} disagree
   */
  private static <A extends Annotation> A mergeAnnotation(
    String role,
    @Nullable A current,
    A candidate,
    Method method
  ) {
    return Merge.mergeOrThrow(
      current,
      candidate,
      () -> "conflicting " + role + " declarations on " + method + ": " + current + " vs " + candidate
    );
  }

  /**
   * Discovers parameter names for a method group, prioritizing the view carrying the verb
   * annotation, then the most-specific method, and finally any non-bridge source view.
   *
   * @param annotated the view that declares the verb annotation
   * @param specific  the group's most-specific method
   * @param views     all views belonging to the group
   * @return the discovered parameter names, or {@code null} if unavailable
   */
  private String@Nullable [] parameterNames(Method annotated, Method specific, Set<Method> views) {
    String[] names = parameterNameDiscoverer.getParameterNames(annotated);
    if (names != null) {
      return names;
    }
    if (!specific.equals(annotated)) {
      names = parameterNameDiscoverer.getParameterNames(specific);
      if (names != null) {
        return names;
      }
    }
    for (Method view : views) {
      if (view.isBridge() || view.equals(specific)) {
        continue;
      }
      String[] candidate = parameterNameDiscoverer.getParameterNames(view);
      if (candidate != null) {
        return candidate;
      }
    }
    return null;
  }

  /**
   * Resolves the {@link ZetaConfig} defaults for one method group.
   *
   * <p>A class root merges its superclass chain nearest first and then only the interfaces that
   * carry the group's views, so a sibling interface that declares none of the group's methods can
   * never lend its namespace or TTLs to another view's method. A method declared on the class alone
   * has no interface view, so every implemented interface contributes as a fallback under the
   * class's own attributes.
   *
   * <p>A JDK proxy has several roots and no implementing type, so every root related to the group's
   * views competes under the same rule: the first root is only the name in the diagnostics, never
   * the sole source of the defaults. Letting {@link #selectBeanClass} collapse the roots first made
   * the result depend on the proxy's interface order — two sibling interfaces with different
   * {@code cache} attributes silently took the first one's namespace instead of failing, and a
   * first sibling without a declaration reported a blank cache name while the same proxy with the
   * order reversed started.
   *
   * @param roots         the scan roots: the bean class, or the business interfaces of a JDK proxy
   * @param beanClass     the root selected for class defaults and warm bean lookup
   * @param views         every method view belonging to the group
   * @param directConfigs per-scan cache of each type's own declaration
   * @return the merged {@link ClassConfig}, or {@code null} when no declaration applies
   */
  private static @Nullable ClassConfig resolveConfig(
    Set<Class<?>> roots,
    Class<?> beanClass,
    Set<Method> views,
    Map<Class<?>, Optional<ClassConfig>> directConfigs
  ) {
    if (beanClass.isInterface()) {
      List<Class<?>> interfaceRoots;
      if (roots.size() <= 1) {
        interfaceRoots = List.of(beanClass);
      } else {
        List<Class<?>> viewInterfaces = viewInterfaces(views);
        interfaceRoots = new ArrayList<>(roots.size());
        for (Class<?> root : roots) {
          if (root.isInterface() && (viewInterfaces == null || assignableToAny(root, viewInterfaces))) {
            interfaceRoots.add(root);
          }
        }
        if (interfaceRoots.isEmpty()) {
          interfaceRoots = List.of(beanClass);
        }
      }
      return resolveInterfaceConfig(interfaceRoots, beanClass, null, directConfigs);
    }

    ClassConfig classConfig = null;
    List<Class<?>> implemented = new ArrayList<>();
    for (Class<?> type = beanClass; type != null && type != Object.class; type = type.getSuperclass()) {
      classConfig = mergeConfig(classConfig, directConfigOn(type, directConfigs));
      for (Class<?> ifc : type.getInterfaces()) {
        if (!implemented.contains(ifc)) {
          implemented.add(ifc);
        }
      }
    }

    List<Class<?>> viewInterfaces = viewInterfaces(views);
    List<Class<?>> interfaceRoots = new ArrayList<>(implemented.size());
    for (Class<?> ifc : implemented) {
      if (viewInterfaces == null || assignableToAny(ifc, viewInterfaces)) {
        interfaceRoots.add(ifc);
      }
    }
    return mergeConfig(classConfig, resolveInterfaceConfig(interfaceRoots, beanClass, classConfig, directConfigs));
  }

  /**
   * Returns the interfaces that declare the group's views, or {@code null} when every view belongs
   * to a class.
   *
   * @param views the group's method views
   * @return the declaring interfaces, or {@code null}
   */
  private static @Nullable List<Class<?>> viewInterfaces(Set<Method> views) {
    List<Class<?>> interfaces = null;
    for (Method view : views) {
      Class<?> owner = view.getDeclaringClass();
      if (!owner.isInterface()) {
        continue;
      }
      if (interfaces == null) {
        interfaces = new ArrayList<>(2);
      }
      if (!interfaces.contains(owner)) {
        interfaces.add(owner);
      }
    }
    return interfaces;
  }

  /**
   * Returns whether {@code candidate} is one of, or a subtype of one of, {@code supertypes}.
   *
   * @param candidate  the candidate interface
   * @param supertypes the interfaces the group exposes
   * @return {@code true} when the candidate implements a view the group uses
   */
  private static boolean assignableToAny(Class<?> candidate, List<Class<?>> supertypes) {
    for (Class<?> supertype : supertypes) {
      if (supertype.isAssignableFrom(candidate)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Merges the {@link ZetaConfig} declarations reachable from {@code roots} through the interface
   * graph.
   *
   * <p>Only maximal declarations compete: a sub-interface dominates its parents, and its
   * attributes win over theirs. Several maximal declarations that resolve to different effective
   * configuration are an ambiguity rather than a scan-order decision, so they fail startup; two
   * that agree describe one configuration and merge normally.
   *
   * @param roots         the interface roots to search
   * @param owner         the bean class or interface named in conflict errors
   * @param base          the defaults resolved outside the interface graph, or {@code null}
   * @param directConfigs per-scan cache of each type's own declaration
   * @return the merged configuration, or {@code null} when no interface declares one
   * @throws IllegalStateException when two maximal interfaces disagree after {@code base} applies
   */
  private static @Nullable ClassConfig resolveInterfaceConfig(
    List<Class<?>> roots,
    Class<?> owner,
    @Nullable ClassConfig base,
    Map<Class<?>, Optional<ClassConfig>> directConfigs
  ) {
    if (roots.isEmpty()) {
      return null;
    }
    List<Class<?>> annotated = annotatedInterfaces(roots, directConfigs);
    if (annotated.isEmpty()) {
      return null;
    }
    List<Class<?>> maximal = maximalInterfaces(annotated);
    ClassConfig first = interfaceConfig(maximal.get(0), owner, base, directConfigs);
    for (int i = 1; i < maximal.size(); i++) {
      ClassConfig other = interfaceConfig(maximal.get(i), owner, base, directConfigs);
      if (!Objects.equals(mergeConfig(base, first), mergeConfig(base, other))) {
        throw new IllegalStateException(
          "conflicting @ZetaConfig declarations on sibling interfaces " +
            maximal.get(0).getName() +
            " and " +
            maximal.get(i).getName() +
            " for " +
            owner.getName() +
            "; declare @ZetaConfig on the implementing type or the attribute on the method"
        );
      }
    }
    return first;
  }

  /**
   * Merges one interface's own declaration with its parents', nearest first.
   *
   * @param iface         the interface whose declaration to resolve
   * @param owner         the bean class or interface named in conflict errors
   * @param base          the defaults already resolved outside this interface, or {@code null}
   * @param directConfigs per-scan cache of each type's own declaration
   * @return the merged configuration of this interface and its parents
   */
  private static ClassConfig interfaceConfig(
    Class<?> iface,
    Class<?> owner,
    @Nullable ClassConfig base,
    Map<Class<?>, Optional<ClassConfig>> directConfigs
  ) {
    ClassConfig direct = directConfigOn(iface, directConfigs);
    Class<?>[] parents = iface.getInterfaces();
    ClassConfig inherited = parents.length == 0
      ? null
      : resolveInterfaceConfig(List.of(parents), owner, mergeConfig(base, direct), directConfigs);
    return mergeConfig(direct, inherited);
  }

  /**
   * Breadth-first search for every interface that declares {@code @ZetaConfig} itself.
   *
   * @param roots         the interface roots to search
   * @param directConfigs per-scan cache of each type's own declaration
   * @return the declaring interfaces in discovery order
   */
  private static List<Class<?>> annotatedInterfaces(
    List<Class<?>> roots,
    Map<Class<?>, Optional<ClassConfig>> directConfigs
  ) {
    List<Class<?>> annotated = new ArrayList<>(2);
    Deque<Class<?>> pending = new ArrayDeque<>(roots);
    Set<Class<?>> visited = new HashSet<>();
    while (!pending.isEmpty()) {
      Class<?> current = pending.removeFirst();
      if (!visited.add(current)) {
        continue;
      }
      if (directConfigOn(current, directConfigs) != null) {
        annotated.add(current);
      }
      Collections.addAll(pending, current.getInterfaces());
    }
    return annotated;
  }

  /**
   * Filters interfaces to those no other one extends.
   *
   * <p>Shared by {@code @ZetaConfig} competition and by a JDK proxy's business views: in both
   * cases a super-interface is reachable from the sub-interface, so it is not an independent peer.
   *
   * @param candidates the competing interfaces
   * @return the interfaces that are not a super-interface of another one
   */
  private static List<Class<?>> maximalInterfaces(List<Class<?>> candidates) {
    List<Class<?>> maximal = new ArrayList<>(candidates.size());
    for (Class<?> candidate : candidates) {
      boolean dominated = false;
      for (Class<?> other : candidates) {
        if (other != candidate && candidate.isAssignableFrom(other)) {
          dominated = true;
          break;
        }
      }
      if (!dominated) {
        maximal.add(candidate);
      }
    }
    return maximal;
  }

  /**
   * Returns the declaration written on {@code type} itself, cached for one scan.
   *
   * @param type          the class or interface to read
   * @param directConfigs per-scan cache of each type's own declaration
   * @return the type's declaration, or {@code null}
   */
  private static @Nullable ClassConfig directConfigOn(
    Class<?> type,
    Map<Class<?>, Optional<ClassConfig>> directConfigs
  ) {
    return directConfigs.computeIfAbsent(type, PlanResolver::readDirectConfig).orElse(null);
  }

  /**
   * Reads the declaration written on {@code type} itself.
   *
   * <p>{@code DIRECT} keeps the search on this type: {@link #resolveConfig} walks the superclass
   * chain and the interface graph itself, because it must know which type contributed an attribute.
   *
   * @param type the class or interface to read
   * @return the declaration, or empty when the type carries no {@code @ZetaConfig}
   */
  private static Optional<ClassConfig> readDirectConfig(Class<?> type) {
    if (type.getDeclaredAnnotations().length == 0) {
      return Optional.empty();
    }
    MergedAnnotations annotations = MergedAnnotations.from(
      type,
      MergedAnnotations.SearchStrategy.DIRECT,
      RepeatableContainers.none(),
      AnnotationFilter.PLAIN
    );
    if (!annotations.isPresent(ZetaConfig.class)) {
      return Optional.empty();
    }
    ClassConfig merged = null;
    for (MergedAnnotation<ZetaConfig> annotation : annotations.stream(ZetaConfig.class).toList()) {
      merged = mergeConfig(merged, toClassConfig(annotation.synthesize()));
    }
    return Optional.ofNullable(merged);
  }

  private static ClassConfig toClassConfig(ZetaConfig candidate) {
    return new ClassConfig(
      candidate.cache().isBlank() ? null : candidate.cache(),
      candidate.hardTtl().isBlank() ? null : candidate.hardTtl(),
      candidate.softTtl().isBlank() ? null : candidate.softTtl(),
      candidate.prewarm().toTrigger(),
      candidate.keepWarm().toTrigger(),
      candidate.keyFormat() == KeyFormat.Option.INHERIT ? null : candidate.keyFormat()
    );
  }

  private static @Nullable ClassConfig mergeConfig(@Nullable ClassConfig primary, @Nullable ClassConfig fallback) {
    if (primary == null) {
      return fallback;
    }
    if (fallback == null) {
      return primary;
    }
    return new ClassConfig(
      primary.cache() != null ? primary.cache() : fallback.cache(),
      primary.hardTtl() != null ? primary.hardTtl() : fallback.hardTtl(),
      primary.softTtl() != null ? primary.softTtl() : fallback.softTtl(),
      primary.prewarm() != null ? primary.prewarm() : fallback.prewarm(),
      primary.keepWarm() != null ? primary.keepWarm() : fallback.keepWarm(),
      primary.keyFormat() != null ? primary.keyFormat() : fallback.keyFormat()
    );
  }

  /**
   * Lazily resolves the namespace contract registry at most once using double-checked locking.
   *
   * @return the resolved {@link NamespacePolicyRegistry}, or {@code null} if none was supplied
   */
  private @Nullable NamespacePolicyRegistry contracts() {
    if (!contractsResolved) {
      synchronized (this) {
        if (!contractsResolved) {
          contracts = contractsSupplier.get();
          contractsResolved = true;
        }
      }
    }
    return contracts;
  }

  /**
   * Lazily resolves the plan-time key-format registry, defaulting to a {@link KeyFormat#TYPED}-only
   * instance when the supplier returns {@code null}.
   *
   * @return the non-null {@link NamespaceKeys} registry
   */
  private NamespaceKeys keys() {
    NamespaceKeys resolved = keys;
    if (resolved == null) {
      synchronized (this) {
        resolved = keys;
        if (resolved == null) {
          NamespaceKeys supplied = keysSupplier.get();
          resolved = supplied != null ? supplied : new NamespaceKeys(KeyFormat.TYPED);
          keys = resolved;
        }
      }
    }
    return resolved;
  }

  /**
   * Publishes one namespace's TTL and warming contract to the registry when a registry is bound.
   *
   * @param namespace the physical namespace prefix
   * @param hard      the resolved hard TTL
   * @param soft      the resolved soft TTL
   * @param prewarm   the effective prewarm trigger, or {@code null}
   * @param keepWarm  the effective keep-warm trigger, or {@code null}
   */
  private void registerContract(
    String namespace,
    CachePlan.Ttl hard,
    CachePlan.Ttl soft,
    @Nullable HeatTrigger prewarm,
    @Nullable HeatTrigger keepWarm
  ) {
    NamespacePolicyRegistry registry = contracts();
    if (registry == null) {
      return;
    }
    boolean constant = hard.isConstant() && soft.isConstant();
    registry.registerAnnotatedNamespace(
      namespace,
      constant ? hard.evaluate(null) : 0L,
      constant ? soft.evaluate(null) : 0L,
      !constant,
      prewarm,
      keepWarm
    );
  }

  /**
   * Resolves {@code ${...}} placeholders in an annotation attribute and fails fast if resolution
   * errors or leaves any unresolved placeholder token behind.
   *
   * @param role   the attribute role used in failure messages
   * @param raw    the raw attribute value, or {@code null}
   * @param method the owning method, or {@code null} for a class-level attribute
   * @return the resolved string, or {@code null} if {@code raw} was {@code null}
   * @throws IllegalStateException if a placeholder fails to resolve or remains unresolved
   */
  private @Nullable String resolvePlaceholders(String role, @Nullable String raw, @Nullable Method method) {
    if (raw == null || !raw.contains("${")) {
      return raw;
    }
    String resolved = raw;
    if (beanFactory != null) {
      try {
        resolved = beanFactory.resolveEmbeddedValue(raw);
      } catch (RuntimeException e) {
        throw new IllegalStateException(role + " placeholder '" + raw + "' could not be resolved" + at(method), e);
      }
    }
    if (resolved != null && resolved.contains("${")) {
      throw new IllegalStateException(
        role +
          " placeholder was not resolved ('" +
          resolved +
          "')" +
          at(method) +
          "; register a placeholder resolver or replace the placeholder"
      );
    }
    return resolved;
  }

  /**
   * Resolves the class loader used for SpEL parsing and reflective access.
   *
   * @param beanFactory the configurable bean factory, or {@code null}
   * @return a non-null {@link ClassLoader}
   * @throws IllegalStateException if no class loader is available
   */
  private static ClassLoader resolveClassLoader(@Nullable ConfigurableBeanFactory beanFactory) {
    ClassLoader classLoader = beanFactory == null ? null : beanFactory.getBeanClassLoader();
    if (classLoader == null) {
      classLoader = ClassUtils.getDefaultClassLoader();
    }
    if (classLoader == null) {
      throw new IllegalStateException("no classloader available for SpEL; configure a bean classloader");
    }
    return classLoader;
  }

  /**
   * Returns whether {@code method} is excluded from pointcut matching, scanning, and interception.
   *
   * <p>Bridge methods are retained because they carry erased signatures needed for reflective
   * refresh, while non-bridge synthetic helpers are excluded.
   *
   * @param method the method to test
   * @return {@code true} if the method can never carry a cache plan
   */
  private static boolean ignored(Method method) {
    return (
      ReflectionUtils.isObjectMethod(method) ||
      Modifier.isStatic(method.getModifiers()) ||
      Modifier.isPrivate(method.getModifiers()) ||
      (method.isSynthetic() && !method.isBridge())
    );
  }

  /**
   * Resolves a candidate method to its most-specific view across {@code roots}.
   *
   * @param candidate the declared method discovered during scanning
   * @param roots     the scan roots
   * @param classes   every type included in the scan, used to specialize an interface view
   * @return the most-specific view of {@code candidate}
   */
  private static Method getSpecificMethod(Method candidate, Set<Class<?>> roots, Set<Class<?>> classes) {
    Method specific = BridgeMethodResolver.findBridgedMethod(candidate);
    for (Class<?> root : roots) {
      if (!specific.getDeclaringClass().isAssignableFrom(root)) {
        continue;
      }
      // The resolved method is the one `root` dispatches to, so its declaring class need not
      // implement the candidate's view: a body inherited from a superclass that implements nothing
      // is still the implementation. Answering with the view instead would make the plan's method
      // depend on scan order and would hide the body from the reflective refresh.
      Method resolved = BridgeMethodResolver.getMostSpecificMethod(specific, root);
      if (!resolved.equals(specific)) {
        specific = resolved;
      }
      // An interface root can expose a specialization the bytecode never bridged. It runs after
      // the most-specific lookup and outside its branch so an intermediate interface that only
      // redeclares the erased signature cannot end the walk early.
      if (root.isInterface()) {
        specific = resolveGenericInterfaceOverride(specific, root, classes);
      }
    }
    return specific;
  }

  /**
   * Maps an interface method declared with a type variable to the declaration a sub-interface
   * makes with the resolved type, when no bridge method connects them.
   *
   * <p>Only a parameter that is a type variable or a type-variable array is specialized; every
   * other method answers {@code false} from {@link #hasTypeVariableParameter} without allocating a
   * {@link ResolvableType}. A candidate that resolves back to the erased signature is skipped, so
   * an overload declared in the same interface is never folded into the parent's group.
   *
   * @param specific the current most-specific view
   * @param root     the interface root the scan resolves against
   * @param classes  every type included in the scan
   * @return the specialized declaration, or {@code specific} when none exists
   */
  private static Method resolveGenericInterfaceOverride(Method specific, Class<?> root, Set<Class<?>> classes) {
    if (!hasTypeVariableParameter(specific)) {
      return specific;
    }
    Class<?> declaring = specific.getDeclaringClass();
    Class<?>[] erased = specific.getParameterTypes();
    Method best = specific;
    for (Class<?> candidateType : classes) {
      if (
        candidateType == declaring ||
        !candidateType.isInterface() ||
        !declaring.isAssignableFrom(candidateType) ||
        !candidateType.isAssignableFrom(root)
      ) {
        continue;
      }
      Class<?>[] resolved = resolveParameterTypes(specific, candidateType, erased);
      if (Arrays.equals(resolved, erased)) {
        continue;
      }
      for (Method method : ReflectionUtils.getDeclaredMethods(candidateType)) {
        if (
          !ignored(method) &&
          method.getName().equals(specific.getName()) &&
          Arrays.equals(method.getParameterTypes(), resolved)
        ) {
          best = moreSpecificOwner(best, method);
        }
      }
    }
    return best;
  }

  /**
   * Returns whether any declared parameter is, or is an array of, a type variable.
   *
   * @param method the method to inspect
   * @return {@code true} when a type argument could specialize the signature
   */
  private static boolean hasTypeVariableParameter(Method method) {
    for (Type type : method.getGenericParameterTypes()) {
      if (containsTypeVariable(type)) {
        return true;
      }
    }
    return false;
  }

  private static boolean containsTypeVariable(Type type) {
    if (type instanceof TypeVariable<?>) {
      return true;
    }
    return type instanceof GenericArrayType array && containsTypeVariable(array.getGenericComponentType());
  }

  /**
   * Resolves each declared parameter type of {@code method} in the type-argument context of
   * {@code contextType}, falling back to the erased type when resolution is indeterminate.
   *
   * @param method      the interface method whose parameters to resolve
   * @param contextType the sub-interface supplying the type arguments
   * @param erased      the method's erased parameter types
   * @return the resolved parameter types
   */
  private static Class<?>[] resolveParameterTypes(Method method, Class<?> contextType, Class<?>[] erased) {
    Class<?>[] resolved = new Class<?>[erased.length];
    for (int i = 0; i < erased.length; i++) {
      resolved[i] = ResolvableType.forMethodParameter(method, i, contextType).resolve(erased[i]);
    }
    return resolved;
  }

  /**
   * Selects the bean lookup class for a scanned method group: the concrete class root when
   * scanning a class, or the most-specific business interface assignable from the method's
   * declaring type when scanning a JDK proxy.
   *
   * @param roots    the scan roots
   * @param specific the group's most-specific method
   * @return the bean class used for class config and warm loader lookup
   */
  private static Class<?> selectBeanClass(Set<Class<?>> roots, Method specific) {
    Class<?> declaring = specific.getDeclaringClass();
    Class<?> selected = null;
    for (Class<?> root : roots) {
      if (!root.isInterface()) {
        return root;
      }
      if (declaring.isAssignableFrom(root) && (selected == null || selected.isAssignableFrom(root))) {
        selected = root;
      }
    }
    return selected != null ? selected : declaring;
  }

  /**
   * Returns the method declared on the more specific type when folding signature group owners: a
   * sub-interface dominates its parent, and between otherwise unrelated declarations the covariant
   * (narrower) return type dominates so the plan's {@code specific} does not depend on scan order.
   *
   * @param current   the current owner method
   * @param candidate the candidate owner method
   * @return the method with the more specific declaring class or the narrower return type
   */
  private static Method moreSpecificOwner(Method current, Method candidate) {
    Class<?> currentOwner = current.getDeclaringClass();
    Class<?> candidateOwner = candidate.getDeclaringClass();
    if (candidateOwner != currentOwner) {
      if (currentOwner.isAssignableFrom(candidateOwner)) {
        return candidate;
      }
      if (candidateOwner.isAssignableFrom(currentOwner)) {
        return current;
      }
    }
    Class<?> currentReturn = current.getReturnType();
    Class<?> candidateReturn = candidate.getReturnType();
    return candidateReturn != currentReturn && currentReturn.isAssignableFrom(candidateReturn) ? candidate : current;
  }

  /**
   * Returns whether {@code candidate}'s declaring class is a strict subtype of {@code other}'s.
   *
   * @param candidate the potentially dominating method
   * @param other     the potentially dominated method
   * @return {@code true} if {@code candidate} strictly dominates {@code other}
   */
  private static boolean dominates(Method candidate, Method other) {
    Class<?> candidateClass = candidate.getDeclaringClass();
    Class<?> otherClass = other.getDeclaringClass();
    return candidateClass != otherClass && otherClass.isAssignableFrom(candidateClass);
  }

  /**
   * Returns whether two warmed specifications describe one logical loader: the same bean viewed
   * through an interface and its implementation, or one inherited body reached from two views.
   *
   * <p>A bean class here is either a scanned user class or a business interface (a JDK proxy scan
   * yields the interface); CGLIB proxies are stripped before the scan, so a non-interface bean class
   * is always the concrete bean class. Only an interface and its implementation are two views of
   * one bean; a parent and a child concrete class are two beans competing for one loader slot, even
   * though both may inherit the same annotated method, because the loader invokes one of the two
   * instances. The methods are compared in the context of the narrower bean class — see
   * {@link #sameMethod} — because a body inherited from a superclass that implements no view only
   * resolves there.
   *
   * @param first  the first warmed specification
   * @param second the second warmed specification
   * @return {@code true} if both specifications describe one loader
   */
  private static boolean sameLoader(WarmSpec first, WarmSpec second) {
    Class<?> firstBean = first.beanClass();
    Class<?> secondBean = second.beanClass();
    if (firstBean != secondBean && !firstBean.isInterface() && !secondBean.isInterface()) {
      return false;
    }
    if (!firstBean.isAssignableFrom(secondBean) && !secondBean.isAssignableFrom(firstBean)) {
      return false;
    }
    // The narrower bean class is the type both scans describe, so it is the only context in which
    // an interface view and an inherited body compare as one method.
    Class<?> beanClass = firstBean.isAssignableFrom(secondBean) ? secondBean : firstBean;
    return sameMethod(first.specific(), second.specific(), beanClass);
  }

  /**
   * Returns whether two methods are one logical method as dispatched by {@code targetClass}.
   *
   * <p>Both owners are resolved against {@code targetClass} when it can dispatch them, because an
   * interface view and the superclass body it reaches need not be related by inheritance: only the
   * bean class relates them. Without a common target the declaring-type relation is the only
   * evidence, and unrelated declarations are treated as different methods.
   *
   * @param first       the first method
   * @param second      the second method
   * @param targetClass the class that dispatches both methods, or {@code null} when unknown
   * @return {@code true} if both methods resolve to the same implementation on {@code targetClass}
   */
  static boolean sameMethod(Method first, Method second, @Nullable Class<?> targetClass) {
    if (first.equals(second)) {
      return true;
    }
    
    Class<?> firstOwner = first.getDeclaringClass();
    Class<?> secondOwner = second.getDeclaringClass();
    if (targetClass != null && firstOwner.isAssignableFrom(targetClass) && secondOwner.isAssignableFrom(targetClass)) {
      return (
        BridgeMethodResolver.getMostSpecificMethod(first, targetClass).equals(
          BridgeMethodResolver.getMostSpecificMethod(second, targetClass)
        )
      );
    }
    if (firstOwner.isAssignableFrom(secondOwner)) {
      return BridgeMethodResolver.getMostSpecificMethod(first, secondOwner).equals(second);
    }
    if (secondOwner.isAssignableFrom(firstOwner)) {
      return BridgeMethodResolver.getMostSpecificMethod(second, firstOwner).equals(first);
    }
    return false;
  }

  /**
   * Checks whether {@code next} implements every non-infrastructure proxied interface assignable to
   * {@code declaringClass}.
   *
   * @param advised        the enclosing proxy
   * @param next           the candidate unwrapped target
   * @param declaringClass the method's declaring type
   * @return {@code true} if {@code next} implements all method-carrying views
   */
  private static boolean implementsMethodViews(Advised advised, Object next, Class<?> declaringClass) {
    for (Class<?> ifc : advised.getProxiedInterfaces()) {
      if (isInfrastructureClass(ifc)) {
        continue;
      }
      if (declaringClass.isAssignableFrom(ifc) && !ifc.isInstance(next)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Performs a breadth-first search across {@code targetType}'s hierarchy to locate the narrowest
   * non-static, non-private method whose erased parameter types accept {@code specific}'s
   * parameters.
   *
   * @param specific   the plan's most-specific method
   * @param targetType the runtime class of the invocation target
   * @return the best matching erased declaration, or {@code null} if none exists
   */
  private static @Nullable Method findErasedMethod(Method specific, Class<?> targetType) {
    Class<?>[] parameterTypes = specific.getParameterTypes();
    Deque<Class<?>> pending = new ArrayDeque<>();
    Set<Class<?>> visited = new HashSet<>();
    Method best = null;
    pending.add(targetType);

    while (!pending.isEmpty()) {
      Class<?> type = pending.removeFirst();
      if (!visited.add(type)) {
        continue;
      }
      for (Method candidate : ReflectionUtils.getDeclaredMethods(type)) {
        if (
          candidate.getName().equals(specific.getName()) &&
          candidate.getParameterCount() == parameterTypes.length &&
          !Modifier.isStatic(candidate.getModifiers()) &&
          !Modifier.isPrivate(candidate.getModifiers()) &&
          accepts(candidate.getParameterTypes(), parameterTypes) &&
          (best == null || moreSpecific(candidate, best))
        ) {
          best = candidate;
        }
      }
      Class<?> superclass = type.getSuperclass();
      if (superclass != null && superclass != Object.class) {
        pending.add(superclass);
      }
      Collections.addAll(pending, type.getInterfaces());
    }
    return best;
  }

  /**
   * Returns whether {@code candidate} narrows every parameter of {@code current} and strictly
   * narrows at least one.
   *
   * @param candidate the candidate method
   * @param current   the current best method
   * @return {@code true} if {@code candidate} is strictly more specific than {@code current}
   */
  private static boolean moreSpecific(Method candidate, Method current) {
    Class<?>[] candidateTypes = candidate.getParameterTypes();
    Class<?>[] currentTypes = current.getParameterTypes();
    boolean narrower = false;
    for (int i = 0; i < candidateTypes.length; i++) {
      if (candidateTypes[i] == currentTypes[i]) {
        continue;
      }
      if (!currentTypes[i].isAssignableFrom(candidateTypes[i])) {
        return false;
      }
      narrower = true;
    }
    return narrower;
  }

  /**
   * Returns whether every declared parameter type in {@code declared} accepts the corresponding
   * actual type in {@code actual}.
   *
   * @param declared the candidate method's parameter types
   * @param actual   the plan method's parameter types
   * @return {@code true} if all parameter positions are assignable
   */
  private static boolean accepts(Class<?>[] declared, Class<?>[] actual) {
    for (int i = 0; i < declared.length; i++) {
      if (!declared[i].isAssignableFrom(actual[i])) {
        return false;
      }
    }
    return true;
  }

  /**
   * Resolves a warming trigger option, preferring the explicit method declaration over the
   * class-level default.
   *
   * @param declared     the method-level warming option
   * @param classDefault the inherited class-level trigger, or {@code null}
   * @return the effective {@link HeatTrigger}, or {@code null} when neither specifies one
   */
  private static @Nullable HeatTrigger resolveTrigger(HeatTrigger.Option declared, @Nullable HeatTrigger classDefault) {
    HeatTrigger trigger = declared.toTrigger();
    return trigger != null ? trigger : classDefault;
  }

  /**
   * Returns {@code methodValue} when non-blank, otherwise falls back to {@code classValue}.
   *
   * @param methodValue the method-level attribute value, or {@code null}
   * @param classValue  the class-level default value, or {@code null}
   * @return the effective string value, or {@code null}
   */
  private static @Nullable String inherited(@Nullable String methodValue, @Nullable String classValue) {
    return methodValue == null || methodValue.isBlank() ? classValue : methodValue;
  }

  /**
   * Resolves the declared return type against the scanned bean class, so a generic base method
   * inherited without an override is judged by its specialization ({@code Void}, {@code Optional}).
   *
   * @param beanClass the concrete bean class providing type-variable bindings
   * @param method    the method whose return type to resolve
   * @return the resolved return class
   */
  private static Class<?> resolvedReturnType(Class<?> beanClass, Method method) {
    Class<?> declared = method.getReturnType();
    return declared == Object.class
      ? ResolvableType.forMethodReturnType(method, beanClass).resolve(declared)
      : declared;
  }

  /**
   * Whether the method's resolved return type is neither {@code void} nor {@link Void}.
   *
   * @param beanClass the concrete bean class providing type-variable bindings
   * @param method    the method to inspect
   * @return {@code true} if the method returns a value
   */
  private static boolean returnsValue(Class<?> beanClass, Method method) {
    Class<?> returnType = resolvedReturnType(beanClass, method);
    return returnType != void.class && returnType != Void.class;
  }

  /**
   * Whether the method's resolved return type is exactly {@link Optional}.
   *
   * @param beanClass the concrete bean class providing type-variable bindings
   * @param method    the method to inspect
   * @return {@code true} when the resolved return type is {@link Optional}
   */
  private static boolean returnsOptional(Class<?> beanClass, Method method) {
    return resolvedReturnType(beanClass, method) == Optional.class;
  }

  /**
   * Formats the optional {@code " for <method>"} suffix for diagnostic error messages.
   *
   * @param method the owning method, or {@code null} for class-level diagnostics
   * @return the formatted suffix, or an empty string when {@code method} is {@code null}
   */
  private static String at(@Nullable Method method) {
    return method == null ? "" : " for " + method;
  }

  /**
   * Immutable plan table for one resolved class along with its authoritative user class.
   *
   * @param userClass the user class that the plans belong to
   * @param plans     the immutable map from every method view to its {@link CachePlan}
   */
  record PlanTable(Class<?> userClass, Map<Method, CachePlan> plans) {}

  /**
   * One proxied receiver resolved to its invocation target, with the advice layers it hides.
   *
   * @param target the deepest target that still exposes the method, or {@code null} when the
   *               candidate is {@code null}
   * @param layers the proxy layers above {@code target}, outermost first
   */
  record ProxyChain(@Nullable Object target, List<Advised> layers) {}

  /**
   * Cache key for a reflective target method: the plan's method and the target's runtime class.
   *
   * @param method     the plan's most-specific method
   * @param targetType the runtime class of the invocation target
   */
  private record MethodTargetKey(Method method, Class<?> targetType) {}

  /**
   * Grouped method views produced by one class scan and the canonical owner of each signature.
   *
   * @param specificBySignature map from erased signature to most-specific owner method
   * @param viewsBySignature    map from erased signature to all discovered method views
   */
  private record ViewGroups(
    Map<MethodSignature, Method> specificBySignature,
    Map<MethodSignature, Set<Method>> viewsBySignature
  ) {}

  /**
   * Output of plan construction for one scan: resolved method plans, collected warm specs, and the
   * namespace contracts waiting to be published.
   *
   * @param resolved  map from every view of each cached group to its {@link CachePlan}
   * @param warmed    list of {@link WarmSpec} entries declared by self-warming read plans
   * @param contracts list of namespace contracts declared by read and write plans
   */
  private record BuiltPlans(Map<Method, CachePlan> resolved, List<WarmSpec> warmed, List<PendingContract> contracts) {}

  /**
   * A namespace contract resolved during one scan, published only after every method in that scan
   * has validated.
   *
   * @param prefix   the physical namespace prefix
   * @param hard     the resolved hard TTL
   * @param soft     the resolved soft TTL
   * @param prewarm  the effective prewarm trigger, or {@code null}
   * @param keepWarm the effective keep-warm trigger, or {@code null}
   */
  private record PendingContract(
    String prefix,
    CachePlan.Ttl hard,
    CachePlan.Ttl soft,
    @Nullable HeatTrigger prewarm,
    @Nullable HeatTrigger keepWarm
  ) {}

  /**
   * Erased method signature used to fold sibling interface and class declarations into one group.
   *
   * @param name           the method name
   * @param parameterTypes the erased parameter types
   */
  private record MethodSignature(String name, List<Class<?>> parameterTypes) {
    /**
     * Creates a signature key from the given {@link Method}.
     *
     * @param method the method whose erased signature is captured
     */
    MethodSignature(Method method) {
      this(method.getName(), List.of(method.getParameterTypes()));
    }
  }

  /**
   * Resolved cache verb declaration attached to the method view that declares it.
   */
  private sealed interface Verb permits Verb.Read, Verb.Write, Verb.Invalidate {
    /**
     * Returns the method view declaring this verb.
     *
     * @return the declaring method view
     */
    Method view();

    /**
     * Returns the human-readable annotation role used in diagnostics.
     *
     * @return the verb role string
     */
    String role();

    /**
     * Returns the underlying annotation or list of repeatable annotations for equality comparison.
     *
     * @return the declaration payload
     */
    Object declaration();

    /**
     * Returns whether another verb carries the same role and equal declaration attributes.
     *
     * @param other the other verb to compare against
     * @return {@code true} if both verbs describe the same operation
     */
    default boolean sameDeclaration(Verb other) {
      return role().equals(other.role()) && declaration().equals(other.declaration());
    }

    /**
     * A {@link ZetaCached} read verb declaration.
     *
     * @param view       the declaring method view
     * @param annotation the synthesized {@link ZetaCached} annotation
     */
    record Read(Method view, ZetaCached annotation) implements Verb {
      @Override
      public String role() {
        return ZETA_CACHED;
      }

      @Override
      public Object declaration() {
        return annotation;
      }
    }

    /**
     * A {@link ZetaPut} write verb declaration.
     *
     * @param view       the declaring method view
     * @param annotation the synthesized {@link ZetaPut} annotation
     */
    record Write(Method view, ZetaPut annotation) implements Verb {
      @Override
      public String role() {
        return ZETA_PUT;
      }

      @Override
      public Object declaration() {
        return annotation;
      }
    }

    /**
     * One or more {@link ZetaInvalidate} declarations flattened on a single method view.
     *
     * @param view        the declaring method view
     * @param annotations the deduplicated list of synthesized {@link ZetaInvalidate} annotations
     */
    record Invalidate(Method view, List<ZetaInvalidate> annotations) implements Verb {
      @Override
      public String role() {
        return ZETA_INVALIDATE;
      }

      @Override
      public Object declaration() {
        return annotations;
      }
    }
  }

  /**
   * Attribute-wise {@link ZetaConfig} defaults merged across a type hierarchy.
   *
   * @param cache     the default cache name, or {@code null}
   * @param hardTtl   the default hard TTL expression/literal, or {@code null}
   * @param softTtl   the default soft TTL expression/literal, or {@code null}
   * @param prewarm   the default prewarm trigger, or {@code null}
   * @param keepWarm  the default keep-warm trigger, or {@code null}
   * @param keyFormat the default key format option, or {@code null}
   */
  private record ClassConfig(
    @Nullable String cache,
    @Nullable String hardTtl,
    @Nullable String softTtl,
    @Nullable HeatTrigger prewarm,
    @Nullable HeatTrigger keepWarm,
    KeyFormat.@Nullable Option keyFormat
  ) {}

  /**
   * Resolved cache namespace name, its key encoding, and whether class-level defaults applied.
   *
   * @param cache     the validated logical namespace name
   * @param encoding  the namespace's {@link KeyEncoding}
   * @param inherited {@code true} if the namespace matches or inherits the class-level default
   */
  private record NamespaceRef(String cache, KeyEncoding encoding, boolean inherited) {}

  /**
   * Pair of resolved hard and soft TTLs for a single read or write verb.
   *
   * @param hard the hard TTL specification
   * @param soft the soft TTL specification
   */
  private record TtlPair(CachePlan.Ttl hard, CachePlan.Ttl soft) {
    /**
     * Returns whether both TTLs folded to compile-time constants.
     *
     * @return {@code true} if neither TTL requires per-invocation evaluation
     */
    boolean bothConstant() {
      return hard.isConstant() && soft.isConstant();
    }

    /**
     * Builds the precomputed {@link GetPolicy} for constant read TTLs.
     *
     * @return the prebuilt read policy
     */
    GetPolicy readPolicy() {
      return CachePlan.readPolicy(hard.constantSupplier(), soft.constantSupplier());
    }

    /**
     * Builds the precomputed {@link PutPolicy} for constant write TTLs.
     *
     * @param sync whether the write synchronizes across peers
     * @return the prebuilt write policy
     */
    PutPolicy writePolicy(boolean sync) {
      return PutPolicy.of(hard.evaluate(null), soft.evaluate(null), sync);
    }
  }

  /**
   * Structural target shape of one {@link ZetaInvalidate} declaration.
   */
  private enum TargetShape {
    ALL,
    EACH_KEY,
    SINGLE;

    /**
     * Validates mutual-exclusion rules among {@code allEntries}, {@code eachKey}, and {@code key}
     * and classifies the invalidation target shape.
     *
     * @param invalidate the invalidation annotation
     * @param method     the annotated method for error diagnostics
     * @return the classified {@link TargetShape}
     * @throws IllegalStateException if the attribute combination is contradictory
     */
    static TargetShape classify(ZetaInvalidate invalidate, Method method) {
      if (invalidate.allEntries() && invalidate.eachKey()) {
        throw new IllegalStateException(
          ZETA_INVALIDATE + " on " + method + " cannot combine allEntries = true with eachKey = true"
        );
      }
      if (invalidate.allEntries() && !invalidate.key().isBlank()) {
        throw new IllegalStateException(
          ZETA_INVALIDATE +
            " on " +
            method +
            " cannot combine allEntries = true with key = \"" +
            invalidate.key() +
            "\""
        );
      }
      if (invalidate.eachKey() && invalidate.key().isBlank()) {
        throw new IllegalStateException(
          ZETA_INVALIDATE + " on " + method + " requires an explicit key when eachKey = true"
        );
      }
      if (invalidate.allEntries()) {
        return ALL;
      }
      return invalidate.eachKey() ? EACH_KEY : SINGLE;
    }
  }

  /**
   * Per-method resolution context holding the scanned bean class, most-specific method, parameter
   * slot table, class-level defaults, and the scan's provisional key-format declarations.
   */
  private final class MethodResolution {

    private final Class<?> beanClass;
    private final Method specific;
    private final ParameterBindings bindings;
    private final @Nullable ClassConfig config;
    private final NamespaceKeys.Staging keyStaging;

    /**
     * Creates a resolution context for one method group.
     *
     * @param beanClass  the scanned bean class
     * @param specific   the group's most-specific method
     * @param bindings   the method's parameter slot table
     * @param config     the effective class-level defaults, or {@code null}
     * @param keyStaging the scan's provisional key-format declarations
     */
    private MethodResolution(
      Class<?> beanClass,
      Method specific,
      ParameterBindings bindings,
      @Nullable ClassConfig config,
      NamespaceKeys.Staging keyStaging
    ) {
      this.beanClass = beanClass;
      this.specific = specific;
      this.bindings = bindings;
      this.keyStaging = keyStaging;
      this.config = config;
    }

    /**
     * Rejects {@code void}/{@code Void} methods for cache verbs that must produce a value.
     *
     * @param annotation the annotation role for error diagnostics
     * @throws IllegalStateException if the resolved return type is not a value
     */
    private void requireValueReturn(String annotation) {
      if (!returnsValue(beanClass, specific)) {
        throw new IllegalStateException(
          annotation +
            " needs a value to cache, but " +
            specific +
            " returns " +
            resolvedReturnType(beanClass, specific)
        );
      }
    }

    /**
     * Resolves a {@link ZetaCached} annotation into a {@link CachePlan.Read}, collecting any
     * {@link WarmSpec} and deferring the namespace contract to the enclosing scan.
     *
     * @param read      the synthesized {@link ZetaCached} annotation
     * @param warmed    accumulator for self-warming specifications
     * @param contracts accumulator for the namespace contract, published once the scan validates
     * @return the resolved read plan
     * @throws IllegalStateException if validation or SpEL compilation fails
     */
    private CachePlan.Read readPlan(ZetaCached read, List<WarmSpec> warmed, List<PendingContract> contracts) {
      requireValueReturn(ZETA_CACHED);

      NamespaceRef ns = namespace(read.cache(), true);
      TtlPair ttls = ttls(read.hardTtl(), read.softTtl(), false);
      CachePlan.Read plan = new CachePlan.Read(
        specific,
        bindings,
        key(ns, read.key(), false),
        compiled(read.condition(), "condition", false),
        compiled(read.unless(), "unless", true),
        ttls.hard(),
        ttls.soft(),
        ttls.bothConstant() ? ttls.readPolicy() : null,
        returnsOptional(beanClass, specific)
      );

      if (read.selfWarmLoader()) {
        warmed.add(WarmSpec.of(beanClass, ns.encoding(), plan));
      }

      HeatTrigger classPrewarm = ns.inherited() && config != null ? config.prewarm() : null;
      HeatTrigger classKeepWarm = ns.inherited() && config != null ? config.keepWarm() : null;
      contracts.add(
        new PendingContract(
          ns.encoding().physicalPrefix(),
          ttls.hard(),
          ttls.soft(),
          resolveTrigger(read.prewarm(), classPrewarm),
          resolveTrigger(read.keepWarm(), classKeepWarm)
        )
      );
      return plan;
    }

    /**
     * Resolves a {@link ZetaPut} annotation into a {@link CachePlan.Write}, rejecting synced
     * permanent entries and deferring the namespace contract to the enclosing scan.
     *
     * @param write     the synthesized {@link ZetaPut} annotation
     * @param contracts accumulator for the namespace contract, published once the scan validates
     * @return the resolved write plan
     * @throws IllegalStateException if validation or SpEL compilation fails
     */
    private CachePlan.Write writePlan(ZetaPut write, List<PendingContract> contracts) {
      requireValueReturn(ZETA_PUT);

      NamespaceRef ns = namespace(write.cache(), true);
      TtlPair ttls = ttls(write.hardTtl(), write.softTtl(), true);
      if (write.sync() && ttls.hard().isPermanent()) {
        throw new IllegalStateException(
          ZETA_PUT +
            " sync=true cannot write a permanent entry (hardTtl = Long.MAX_VALUE): peers expire entries " +
            "only through the hard TTL, so the value would stay stale on every peer: " +
            specific
        );
      }

      CachePlan.Write plan = CachePlan.Write.of(
        specific,
        ns.cache(),
        bindings,
        key(ns, write.key(), true),
        compiled(write.condition(), "condition", false),
        compiled(write.unless(), "unless", true),
        ttls.hard(),
        ttls.soft(),
        write.sync(),
        ttls.bothConstant() ? ttls.writePolicy(write.sync()) : null,
        returnsOptional(beanClass, specific)
      );

      contracts.add(new PendingContract(ns.encoding().physicalPrefix(), ttls.hard(), ttls.soft(), null, null));
      return plan;
    }

    /**
     * Resolves one {@link ZetaInvalidate} annotation into an executable invalidation operation.
     *
     * @param invalidate the synthesized {@link ZetaInvalidate} annotation
     * @return the resolved invalidation operation
     * @throws IllegalStateException if the target shape is inconsistent or compilation fails
     */
    private CachePlan.Invalidate.Operation invalidateOperation(ZetaInvalidate invalidate) {
      TargetShape shape = TargetShape.classify(invalidate, specific);
      // A table-wide clear names no key, so it must not pin the namespace to the default format:
      // another bean may declare the same namespace PLAIN or a configurer may configure it.
      NamespaceRef ns = namespace(invalidate.cache(), shape != TargetShape.ALL);
      boolean resultAllowed = !invalidate.beforeInvocation() && returnsValue(beanClass, specific);
      InvalidatePolicy policy = new InvalidatePolicy(invalidate.scope(), invalidate.beforeInvocation());
      CompiledExpression condition = compiled(invalidate.condition(), "condition", resultAllowed);
      return switch (shape) {
        case ALL -> CachePlan.Invalidate.Operation.all(ns.cache(), policy, condition);
        case EACH_KEY -> eachKeyOperation(ns, invalidate, policy, condition, resultAllowed);
        case SINGLE -> singleOperation(ns, invalidate, policy, condition, resultAllowed);
      };
    }

    /**
     * Builds an {@code eachKey} invalidation operation that expands an array or iterable into
     * individual physical keys.
     *
     * @param ns            the resolved namespace reference
     * @param invalidate    the invalidation annotation
     * @param policy        the invalidation scope and timing policy
     * @param condition     the compiled admission guard
     * @param resultAllowed whether {@code #result} is in scope
     * @return the {@code eachKey} invalidation operation
     */
    private CachePlan.Invalidate.Operation eachKeyOperation(
      NamespaceRef ns,
      ZetaInvalidate invalidate,
      InvalidatePolicy policy,
      CompiledExpression condition,
      boolean resultAllowed
    ) {
      CompiledExpression expression = compiled(invalidate.key(), "cache key", resultAllowed);
      int slot = expression.directArgumentSlot(specific, bindings, true);
      return CachePlan.Invalidate.Operation.eachKey(ns.cache(), ns.encoding(), expression, slot, policy, condition);
    }

    /**
     * Builds a single-key invalidation operation.
     *
     * @param ns            the resolved namespace reference
     * @param invalidate    the invalidation annotation
     * @param policy        the invalidation scope and timing policy
     * @param condition     the compiled admission guard
     * @param resultAllowed whether {@code #result} is in scope
     * @return the single-key invalidation operation
     */
    private CachePlan.Invalidate.Operation singleOperation(
      NamespaceRef ns,
      ZetaInvalidate invalidate,
      InvalidatePolicy policy,
      CompiledExpression condition,
      boolean resultAllowed
    ) {
      return CachePlan.Invalidate.Operation.single(
        ns.cache(),
        key(ns, invalidate.key(), resultAllowed),
        policy,
        condition
      );
    }

    /**
     * Resolves the cache namespace name, stages its key format, and determines whether class-level
     * namespace settings apply.
     *
     * @param rawCache the method's raw {@code cache} attribute, or {@code null}
     * @param pin      whether the declaration names a key format; a table-wide clear does not
     * @return the resolved {@link NamespaceRef}
     */
    private NamespaceRef namespace(@Nullable String rawCache, boolean pin) {
      String cache = cacheName(rawCache);
      boolean inherited = inheritsClassNamespace(cache, rawCache);
      return new NamespaceRef(cache, encoding(cache, inherited, pin), inherited);
    }

    /**
     * Compiles and classifies a key expression against the resolved namespace encoding.
     *
     * @param ns            the resolved namespace reference
     * @param raw           the raw key expression, or {@code null}
     * @param resultAllowed whether {@code #result} is allowed in the key expression
     * @return the resolved {@link CachePlan.Key}
     */
    private CachePlan.Key key(NamespaceRef ns, @Nullable String raw, boolean resultAllowed) {
      return CachePlan.Key.of(ns.encoding(), specific, compiled(raw, "cache key", resultAllowed), bindings);
    }

    /**
     * Compiles one SpEL expression attribute under this method's parser and parameter bindings.
     *
     * @param raw           the raw expression text, or {@code null}
     * @param role          the attribute role for diagnostics
     * @param resultAllowed whether {@code #result} is allowed in this phase
     * @return the compiled expression
     */
    private CompiledExpression compiled(@Nullable String raw, String role, boolean resultAllowed) {
      return CompiledExpression.of(
        raw,
        parser,
        expressionSupport.emptyContext(),
        role,
        specific,
        bindings,
        resultAllowed
      );
    }

    /**
     * Resolves and classifies both hard and soft TTL attributes, inheriting class defaults when
     * omitted on the method.
     *
     * @param rawHard       the method's raw {@code hardTtl} attribute
     * @param rawSoft       the method's raw {@code softTtl} attribute
     * @param resultAllowed whether {@code #result} is allowed in TTL expressions
     * @return the resolved {@link TtlPair}
     */
    private TtlPair ttls(@Nullable String rawHard, @Nullable String rawSoft, boolean resultAllowed) {
      String hardRaw = inherited(rawHard, config == null ? null : config.hardTtl());
      String softRaw = inherited(rawSoft, config == null ? null : config.softTtl());
      return new TtlPair(ttl("hardTtl", hardRaw, resultAllowed, false), ttl("softTtl", softRaw, resultAllowed, true));
    }

    /**
     * Resolves placeholders and compiles one TTL attribute.
     *
     * @param role           the attribute role ({@code hardTtl} or {@code softTtl})
     * @param raw            the effective raw attribute value, or {@code null}
     * @param resultAllowed  whether {@code #result} is allowed in the expression
     * @param disableAllowed whether explicit disabling (e.g., {@code 0}) is permitted
     * @return the resolved {@link CachePlan.Ttl}
     */
    private CachePlan.Ttl ttl(String role, @Nullable String raw, boolean resultAllowed, boolean disableAllowed) {
      return CachePlan.Ttl.of(
        role,
        resolvePlaceholders(role, raw, specific),
        parser,
        expressionSupport.emptyContext(),
        specific,
        bindings,
        resultAllowed,
        disableAllowed
      );
    }

    /**
     * Resolves and validates the logical cache name, falling back to the class-level default when
     * the method omits it.
     *
     * @param raw the raw method-level {@code cache} attribute, or {@code null}
     * @return the validated non-blank cache name
     * @throws IllegalStateException if the resolved cache name is blank or invalid
     */
    private String cacheName(@Nullable String raw) {
      String cache = resolvePlaceholders(
        "cache name",
        inherited(raw, config == null ? null : config.cache()),
        specific
      );
      if (cache == null || cache.isBlank()) {
        throw new IllegalStateException(
          "cache name must not be blank: set cache on " + specific + " or @ZetaConfig.cache on its class"
        );
      }
      NamespaceKeys.validateName(cache);
      return cache;
    }

    /**
     * Returns whether the resolved cache name inherits or matches the class-level default so
     * class-level key format and warming options apply.
     *
     * @param resolvedCache the validated cache name
     * @param rawCache      the raw method-level {@code cache} attribute, or {@code null}
     * @return {@code true} if class-level namespace settings apply
     */
    private boolean inheritsClassNamespace(String resolvedCache, @Nullable String rawCache) {
      if (config == null) {
        return false;
      }
      if (rawCache == null || rawCache.isBlank()) {
        return true;
      }
      return config.cache() != null && resolvedCache.equals(resolvePlaceholders("cache name", config.cache(), null));
    }

    /**
     * Stages the namespace's key format declaration and returns the encoder the plan inlines.
     *
     * @param cache     the validated logical namespace name
     * @param inherited whether class-level key format settings apply
     * @param pin       whether the declaration names a key format
     * @return the resolved {@link KeyEncoding}
     */
    private KeyEncoding encoding(String cache, boolean inherited, boolean pin) {
      KeyFormat.Option option = inherited && config != null ? config.keyFormat() : KeyFormat.Option.INHERIT;
      return keyStaging.stage(cache, option, specific, pin);
    }
  }
}
