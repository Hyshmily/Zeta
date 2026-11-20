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
import io.github.hyshmily.zeta.annotation.support.ParameterBindings;
import io.github.hyshmily.zeta.annotation.support.ZetaEvaluationContext;
import io.github.hyshmily.zeta.cache.CacheKeysPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.InvalidatePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.PutPolicy;
import lombok.extern.slf4j.Slf4j;
import org.aopalliance.intercept.MethodInvocation;
import org.jspecify.annotations.Nullable;
import org.springframework.aop.ProxyMethodInvocation;
import org.springframework.aop.SpringProxy;
import org.springframework.aop.TargetClassAware;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.core.BridgeMethodResolver;
import org.springframework.core.DecoratingProxy;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Resolves an annotated method group into the one {@link CachePlan} the
 * interceptor executes.
 *
 * <p>Resolution is the fail-fast boundary: a blank namespace, a void/Void return, a
 * conflicting verb, an unparsable expression, an unknown or out-of-range variable, a
 * pre-body {@code #result} or a permanent synced write fails bean creation instead of the
 * first request. The first question about a class resolves every candidate method once;
 * later questions are one table lookup.
 */
@Slf4j
public final class PlanResolver {

  private static final String ZETA_PUT = "@ZetaPut";

  private static final String ZETA_CACHED = "@ZetaCached";

  private static final String ZETA_INVALIDATE = "@ZetaInvalidate";

  private static final char NAMESPACE_DELIMITER = CacheKeysPolicy.NAMESPACE_DELIMITER;

  /** Embedded-value resolution for {@code ${...}} in {@code cache} and both TTL attributes. */
  private final @Nullable ConfigurableBeanFactory beanFactory;

  private final @Nullable BeanResolver beanResolver;

  /**
   * SpEL parser in {@link SpelCompilerMode#MIXED} mode: an expression is interpreted first
   * and compiled after the engine's evaluation threshold, so a key/TTL/unless on a
   * steady-state path stops paying interpretation overhead without paying compilation for a
   * method nobody calls. It resolves against the bean factory's classloader when one is
   * available, otherwise against the default classloader.
   */
  private final SpelExpressionParser parser;

  /**
   * The reflection machinery every evaluation phase shares. Per resolver, not static: its
   * caches must be released with the Spring context and its type locator must build the plan table
   * against the same classloader as {@link #parser}.
   */
  private final ZetaEvaluationContext.ExpressionSupport expressionSupport;

  private final ParameterNameDiscoverer parameterNameDiscoverer = new DefaultParameterNameDiscoverer();

  /**
   * The per-target-class plan table. One lookup answers both the pointcut's "is this a verb
   * method" and the invocation's plan, with no key allocation. The table is instance-scoped, not a
   * {@link ClassValue}: a plan retains application methods and compiled SpEL, and a parent-loader
   * interface key would otherwise pin the application classloader across a context restart. A
   * throwing scan installs nothing, so the class stays unvalidated and the next question retries
   * it; a successful scan publishes one immutable table atomically, and a concurrent duplicate
   * scan is harmless because the first published table wins. A JDK proxy class has no table of
   * its own: its business interfaces own the plans, because the proxy's generated methods carry
   * neither annotations nor bridge flags.
   */
  private final Map<Class<?>, Map<Method, CachePlan>> classPlans = new ConcurrentHashMap<>();

  /** The reflective target-method cache every read plan and receiver pair shares. */
  private final TargetMethodResolver targetMethods = new TargetMethodResolver();

  /**
   * The plan table for one target class, computing it on first use.
   *
   * @param targetClass the class the pointcut or invocation asks about
   * @return the immutable table keyed by every view of the class's verb groups
   */
  private Map<Method, CachePlan> lookupOrScanPlanTable(Class<?> targetClass) {
    Map<Method, CachePlan> plans = classPlans.get(targetClass);
    if (plans != null) {
      return plans;
    }

    Class<?> userClass = ClassUtils.getUserClass(targetClass);
    if (userClass != targetClass) {
      // A CGLIB proxy shares the user class's immutable table instead of scanning the generated
      // subclass.
      return registerPlanTable(targetClass, lookupOrScanPlanTable(userClass));
    }
    if (Proxy.isProxyClass(userClass)) {
      // A JDK proxy declares public final forwarding methods without the interfaces'
      // annotations or bridge flags. Resolving them against the proxy class splits a generic
      // parent method from its annotated child override, so the proxy's business interfaces
      // own the table instead.
      return registerPlanTable(targetClass, proxyPlanTable(userClass));
    }

    Set<Class<?>> classes = new LinkedHashSet<>();
    classes.add(userClass);
    classes.addAll(getMaximalInterfaces(new ArrayList<>(ClassUtils.getAllInterfacesForClassAsSet(userClass))));
    return registerPlanTable(
      targetClass,
      buildPlanTable(classes, candidate -> BridgeMethodResolver.getMostSpecificMethod(candidate, userClass))
    );
  }

  /**
   * Publish one computed table unless another thread won the race. The tables are immutable, so
   * either winner answers every lookup identically.
   */
  private Map<Method, CachePlan> registerPlanTable(Class<?> targetClass, Map<Method, CachePlan> plans) {
    Map<Method, CachePlan> existing = classPlans.putIfAbsent(targetClass, plans);
    return existing != null ? existing : plans;
  }

  /**
   * The plan table of a JDK proxy. A single maximal business interface is the table's owner, so
   * the interface and the proxy share one resolution; several maximal interfaces are scanned
   * together because a method they both override must be resolved once, against their common
   * view.
   */
  private Map<Method, CachePlan> proxyPlanTable(Class<?> proxyClass) {
    List<Class<?>> interfaces = new ArrayList<>();
    for (Class<?> candidate : proxyClass.getInterfaces()) {
      if (isInfrastructureClass(candidate)) {
        continue;
      }
      interfaces.add(candidate);
    }

    List<Class<?>> maximal = getMaximalInterfaces(interfaces);
    if (maximal.isEmpty()) {
      return Map.of();
    }
    if (maximal.size() == 1) {
      return lookupOrScanPlanTable(maximal.get(0));
    }
    // The scan walks each interface's super-interfaces; the maximal roots are the only entry points
    // the resolution needs.
    return buildPlanTable(new LinkedHashSet<>(maximal), candidate -> getSpecificInterfaceMethod(candidate, maximal));
  }

  /** Drop every interface that another candidate already extends; only the roots need a scan. */
  private static List<Class<?>> getMaximalInterfaces(List<Class<?>> interfaces) {
    List<Class<?>> maximal = new ArrayList<>(interfaces.size());
    for (Class<?> candidate : interfaces) {
      boolean dominated = false;
      for (Class<?> existing : maximal) {
        if (candidate.isAssignableFrom(existing)) {
          dominated = true;
          break;
        }
      }
      if (!dominated) {
        maximal.removeIf(existing -> existing.isAssignableFrom(candidate));
        maximal.add(candidate);
      }
    }
    return maximal;
  }

  /**
   * Resolve one interface view to the most specific method the proxied interfaces expose. Two
   * unrelated maximal interfaces overriding the same parent are folded by signature first: the
   * most specific view is kept and {@link #resolveVerb} decides whether their declarations
   * conflict, so a verb on only one sibling is not treated as an ambiguity.
   */
  private static Method getSpecificInterfaceMethod(Method candidate, List<Class<?>> interfaces) {
    Method specific = BridgeMethodResolver.findBridgedMethod(candidate);
    for (Class<?> ifc : interfaces) {
      if (!candidate.getDeclaringClass().isAssignableFrom(ifc)) {
        continue;
      }
      Method resolved = BridgeMethodResolver.getMostSpecificMethod(candidate, ifc);
      specific = specific.getDeclaringClass().isAssignableFrom(resolved.getDeclaringClass()) ? resolved : specific;
    }
    return specific;
  }

  /**
   * Group every view by its most-specific method, then build each group once. Interface
   * declarations are folded by method signature first: an interface generates no synthetic bridge,
   * so two super-interfaces that declare the same method keep two distinct {@link Method} objects
   * and would otherwise split one logical group into two independent plans. The scanned classes
   * are also the roots of the class-level {@link ZetaConfig} lookup, so a default on the bean
   * covers a method inherited from a base class or a parent interface.
   */
  private Map<Method, CachePlan> buildPlanTable(Set<Class<?>> classes, Function<Method, Method> specificOf) {
    Map<Method, Set<Method>> viewsBySpecific = new LinkedHashMap<>();
    Map<MethodSignature, Method> interfaceSpecific = new HashMap<>();
    for (Class<?> candidateClass : classes) {
      for (Method candidate : ReflectionUtils.getAllDeclaredMethods(candidateClass)) {
        if (ignored(candidate)) {
          continue;
        }

        Method specific = specificOf.apply(candidate);
        if (specific.getDeclaringClass().isInterface()) {
          MethodSignature signature = MethodSignature.of(specific);
          Method canonical = interfaceSpecific.get(signature);
          if (canonical == null) {
            interfaceSpecific.put(signature, specific);
          } else if (isMoreSpecificInterfaceMethod(specific, canonical)) {
            interfaceSpecific.put(signature, specific);
            Set<Method> views = viewsBySpecific.remove(canonical);
            if (views != null) {
              viewsBySpecific.computeIfAbsent(specific, key -> new LinkedHashSet<>()).addAll(views);
            }
          } else {
            specific = canonical;
          }
        }
        viewsBySpecific.computeIfAbsent(specific, key -> new LinkedHashSet<>()).add(candidate);
      }
    }

    Map<Method, CachePlan> resolved = new HashMap<>();
    for (Map.Entry<Method, Set<Method>> entry : viewsBySpecific.entrySet()) {
      CachePlan plan = buildPlanForGroup(classes, entry.getKey(), entry.getValue());
      if (plan == null) {
        continue;
      }
      // The grouping key is itself one of the views, so one pass stores every view and warms
      // the one a reflective refresh invokes.
      prewarmAccess(entry.getKey());
      for (Method view : entry.getValue()) {
        resolved.put(view, plan);
      }
    }
    return Map.copyOf(resolved);
  }

  /**
   * Whether {@code candidate} is the more specific declaration of one interface signature: a
   * sub-interface dominates its parent, and a covariant return type dominates the wider one. Two
   * unrelated interfaces with the same signature and return type are a tie, so the first one the
   * scan sees remains the canonical view instead of making the plan depend on their order.
   */
  private static boolean isMoreSpecificInterfaceMethod(Method candidate, Method current) {
    Class<?> candidateDeclaring = candidate.getDeclaringClass();
    Class<?> currentDeclaring = current.getDeclaringClass();
    if (candidateDeclaring != currentDeclaring) {
      if (currentDeclaring.isAssignableFrom(candidateDeclaring)) {
        return true;
      }
      if (candidateDeclaring.isAssignableFrom(currentDeclaring)) {
        return false;
      }
    }
    Class<?> candidateReturn = candidate.getReturnType();
    Class<?> currentReturn = current.getReturnType();
    return candidateReturn != currentReturn && currentReturn.isAssignableFrom(candidateReturn);
  }

  /**
   * Create the plan resolver.
   *
   * @param beanFactory the bean factory backing {@code @beanName} SpEL and, when it is a
   *                    {@link ConfigurableBeanFactory}, {@code ${...}} attribute placeholders;
   *                    {@code null} disables both
   */
  public PlanResolver(@Nullable BeanFactory beanFactory) {
    this.beanFactory = beanFactory instanceof ConfigurableBeanFactory configurable ? configurable : null;
    this.beanResolver = beanFactory == null ? null : new BeanFactoryResolver(beanFactory);
    ClassLoader classLoader = this.beanFactory == null ? null : this.beanFactory.getBeanClassLoader();
    if (classLoader == null) {
      classLoader = ClassUtils.getDefaultClassLoader();
    }
    if (classLoader == null) {
      throw new IllegalStateException("no classloader available for SpEL; configure a bean classloader");
    }
    this.parser = new SpelExpressionParser(new SpelParserConfiguration(SpelCompilerMode.MIXED, classLoader));
    this.expressionSupport = new ZetaEvaluationContext.ExpressionSupport(classLoader);
  }

  /** The pointcut decision, and the startup fail-fast boundary. */
  boolean matches(Method method, Class<?> targetClass) {
    return !ignored(method) && lookupPlanForMethod(method, targetClass) != null;
  }

  /** Methods Spring's pointcut, the class scan and the interceptor's entry check all exclude. */
  private static boolean ignored(Method method) {
    return (
      ReflectionUtils.isObjectMethod(method) ||
      Modifier.isStatic(method.getModifiers()) ||
      Modifier.isPrivate(method.getModifiers()) ||
      // A bridge method is a compiler-generated forwarding method that carries no annotation
      // and must not be resolved against its overridden child; a synthetic method is a
      // compiler-generated helper that carries no annotation and must not be resolved against its caller.
      (method.isSynthetic() && !method.isBridge())
    );
  }

  /**
   * The plan for one method looked up on a target class. A class's table is computed once
   * and keyed by the user class, so a CGLIB proxy class and its target share one resolution
   * and a JDK proxy shares its business interface's table; a method reported only by the
   * proxy is mapped to its most-specific user-class method before the lookup.
   */
  private @Nullable CachePlan lookupPlanForMethod(Method method, Class<?> targetClass) {
    Map<Method, CachePlan> plans = lookupOrScanPlanTable(targetClass);
    CachePlan plan = plans.get(method);
    if (plan != null) {
      return plan;
    }
    Class<?> userClass = ClassUtils.getUserClass(targetClass);
    if (!method.getDeclaringClass().isAssignableFrom(userClass)) {
      // The target class does not implement the reported method (a nested interface proxy).
      // The declaring type's own table is the authority.
      plan = lookupOrScanPlanTable(method.getDeclaringClass()).get(method);
      if (plan != null) {
        return plan;
      }
    }
    Method specific = BridgeMethodResolver.getMostSpecificMethod(method, userClass);
    return specific == method ? null : plans.get(specific);
  }

  /**
   * The plan for one invocation, with the unwrapped receiver and the class the plan is
   * authoritative for. One pipeline serves every receiver shape: unwrap a nested proxy to the
   * object the body should run on, replace a receiver that cannot invoke the reported erased
   * supertype declaration with the proxy that exposes the declaring view, then look the plan up on
   * the resolved class. The proxy's business interfaces are the last fallback: a JDK proxy whose
   * target implements only a parent interface still resolves the annotated child view, and that
   * view becomes {@code #root.targetClass} (design decisions: annotations).
   */
  @Nullable
  InvocationPlan resolveInvocation(MethodInvocation invocation) {
    Method method = invocation.getMethod();
    Class<?> declaring = method.getDeclaringClass();
    if (declaring == Object.class) {
      return null;
    }

    // A plain bean is the common case: it cannot be a proxy, so its class table answers the
    // plan directly. Proxy detection, target unwrapping and interface-view resolution are only
    // needed when the receiver is an AOP proxy or a JDK proxy.
    Object receiver = invocation.getThis();
    if (receiver != null) {
      Class<?> receiverClass = receiver.getClass();
      Class<?> userClass = ClassUtils.getUserClass(receiverClass);
      if (
        userClass == receiverClass &&
        !(receiver instanceof SpringProxy) &&
        !(receiver instanceof TargetClassAware) &&
        !Proxy.isProxyClass(receiverClass)
      ) {
        Map<Method, CachePlan> plans = classPlans.get(receiverClass);
        CachePlan plan = plans == null ? null : plans.get(method);
        if (plan != null) {
          return new InvocationPlan(plan, receiver, receiverClass);
        }
      }
    }

    Object target = unwrapTarget(invocation.getThis(), method);
    Object proxy = invocation instanceof ProxyMethodInvocation proxyInvocation ? proxyInvocation.getProxy() : null;
    // isInstance(proxy) is false for null, so the body only runs with a non-null proxy.
    if (!declaring.isInstance(target) && declaring.isInstance(proxy) && !targetMethods.canInvoke(target, method)) {
      // The receiver cannot invoke the reported method (a repository proxy's target implements
      // only the parent interface); the proxy that exposes the declaring view is the only layer
      // that can run the reflective refresh.
      target = proxy;
    }

    Class<?> targetClass = resolveTargetClass(target, declaring);
    CachePlan plan = lookupPlanForMethod(method, targetClass);
    if (plan != null) {
      return new InvocationPlan(plan, target, targetClass);
    }
    if (proxy != null) {
      plan = lookupPlanForMethod(method, proxy.getClass());
      if (plan != null) {
        Class<?> view = proxy instanceof Advised advised ? proxiedView(advised, declaring) : null;
        return new InvocationPlan(plan, target, view != null ? view : targetClass);
      }
    }
    return null;
  }

  /**
   * Unwrap nested proxies down to the object the method body should run on. The invocation's
   * receiver is normally the target; only a target that is itself an {@link Advised} proxy
   * needs the singleton-target unwrap, and a non-singleton {@code TargetSource} keeps the proxy
   * it cannot unwrap. The unwrap also stops when the next target does not implement a proxied
   * interface assignable to the reported method's declaring type: that target is the only
   * layer that still knows the child interface whose method carries the annotation, so
   * unwrapping through it would silently lose the plan. An introduced (mixin) interface is
   * unrelated to the method's declaring type and therefore never blocks the unwrap.
   *
   * @param candidate the invocation receiver
   * @param method    the reported method the receiver must be able to invoke
   * @return the deepest target that still exposes every method-carrying view
   */
  static @Nullable Object unwrapTarget(@Nullable Object candidate, Method method) {
    Object current = candidate;
    while (current instanceof Advised advised) {
      Object next = AopProxyUtils.getSingletonTarget(advised);
      // isInstance(next) is false for null, so a missing target returns here before
      // implementsMethodViews dereferences it.
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
   * Whether {@code next} implements every proxied interface that can carry the reported method.
   * Infrastructure interfaces and unrelated introductions are ignored: a target never implements
   * an introduced mixin, and treating that as a reason to keep the proxy makes a reflective
   * refresh re-enter the advice chain.
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
   * The class the plan table and {@code #root.targetClass} are keyed on: the target's user class
   * when it still implements the reported method, otherwise the most specific business interface
   * the proxy exposes. A Spring Data repository proxy's ultimate target is
   * {@code SimpleJpaRepository}, which implements the repository's parent interface but not the
   * child interface that carries the annotation; keying on the target would miss every plan and
   * silently bypass the cache. A JDK proxy that is not an {@link Advised} proxy has no user class
   * to name, so its most specific interface view is the class the plan is keyed on.
   */
  private static Class<?> resolveTargetClass(@Nullable Object target, Class<?> declaring) {
    if (target == null) {
      return declaring;
    }
    Class<?> targetType = target.getClass();
    Class<?> ultimate = AopProxyUtils.ultimateTargetClass(target);
    if (!Proxy.isProxyClass(targetType)) {
      return declaring.isAssignableFrom(ultimate) ? ultimate : declaring;
    }
    Class<?> view =
      target instanceof Advised advised
        ? proxiedView(advised, declaring)
        : mostSpecificInterface(targetType.getInterfaces(), declaring);
    if (view == null) {
      return declaring.isAssignableFrom(ultimate) ? ultimate : declaring;
    }
    return !Proxy.isProxyClass(ultimate) && view.isAssignableFrom(ultimate) ? ultimate : view;
  }

  /** The most specific proxied interface that extends {@code declaring}, or {@code null}. */
  private static @Nullable Class<?> proxiedView(Advised advised, Class<?> declaring) {
    return mostSpecificInterface(AopProxyUtils.proxiedUserInterfaces(advised), declaring);
  }

  /** The most specific interface among {@code interfaces} that extends {@code declaring}, or {@code null}. */
  private static @Nullable Class<?> mostSpecificInterface(Class<?>[] interfaces, Class<?> declaring) {
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

  /** Spring's infrastructure interfaces, excluded from every business-view decision. */
  private static boolean isInfrastructureClass(Class<?> ifc) {
    return ifc == SpringProxy.class || ifc == Advised.class || ifc == DecoratingProxy.class;
  }

  /**
   * Build the SpEL context for one evaluation phase. The root wraps the immutable invocation
   * inputs; the context is confined to the thread that evaluates the expression.
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

  /** The reflective target-method cache shared by every read invocation. */
  TargetMethodResolver targetMethods() {
    return targetMethods;
  }

  private @Nullable CachePlan buildPlanForGroup(Set<Class<?>> roots, Method specific, Set<Method> views) {
    Verb verb = resolveVerb(specific, views);
    if (verb == null) {
      return null;
    }
    requireInterceptable(verb, specific, views);
    String[] parameterNames = parameterNames(verb.view(), specific, views);
    ParameterBindings bindings = ParameterBindings.of(specific, parameterNames);
    MethodResolution resolution = new MethodResolution(
      specific,
      bindings,
      classConfig(roots, specific),
      parameterNames
    );
    if (verb instanceof Verb.Read read) {
      return resolution.readPlan(views, read.annotation());
    }
    if (verb instanceof Verb.Write write) {
      return resolution.writePlan(write.annotation());
    }
    List<ZetaInvalidate> annotations = ((Verb.Invalidate) verb).annotations();
    List<CachePlan.Invalidate.Operation> operations = new ArrayList<>(annotations.size());
    for (ZetaInvalidate annotation : annotations) {
      operations.add(resolution.invalidateOperation(annotation));
    }
    return CachePlan.Invalidate.of(specific, bindings, operations);
  }

  /**
   * The class-level defaults visible from the classes the plan is resolved for. A concrete class
   * root owns the whole hierarchy search, because it already covers its superclasses and every
   * interface. For a JDK proxy's unrelated interface roots only the interfaces the method's
   * declaring type is assignable from may contribute; a sibling interface that does not carry the
   * method must not lend its namespace or TTLs to another view. Two applicable roots with
   * contradictory values are an ambiguity that no runtime choice can resolve, so resolution fails
   * instead of taking the first root the scan happens to reach. The method's declaring class is the
   * fallback when no applicable root exposes a configuration.
   */
  private static @Nullable ClassConfig classConfig(Set<Class<?>> roots, Method method) {
    Class<?> declaring = method.getDeclaringClass();
    ClassConfig found = null;
    for (Class<?> root : roots) {
      if (!root.isInterface()) {
        return configOn(root);
      }
      if (!declaring.isAssignableFrom(root)) {
        continue;
      }
      ClassConfig candidate = configOn(root);
      if (candidate == null) {
        continue;
      }
      found = found == null ? candidate : found.merge(candidate, method);
    }
    return found != null ? found : configOn(declaring);
  }

  /**
   * The attribute-wise merge of every {@link ZetaConfig} on one type's hierarchy, or {@code null}.
   * The nearest declaration supplies each attribute it sets; a blank attribute inherits the next
   * declaration outward, so a subclass may override only its hard TTL without restating the
   * namespace. The search strategy returns annotations closest first.
   */
  private static @Nullable ClassConfig configOn(Class<?> type) {
    String cache = null;
    String hardTtl = null;
    String softTtl = null;
    for (MergedAnnotation<ZetaConfig> merged : MergedAnnotations.from(
      type,
      MergedAnnotations.SearchStrategy.TYPE_HIERARCHY
    )
      .stream(ZetaConfig.class)
      .toList()) {
      ZetaConfig candidate = merged.synthesize();
      if (cache == null && !candidate.cache().isBlank()) {
        cache = candidate.cache();
      }
      if (hardTtl == null && !candidate.hardTtl().isBlank()) {
        hardTtl = candidate.hardTtl();
      }
      if (softTtl == null && !candidate.softTtl().isBlank()) {
        softTtl = candidate.softTtl();
      }
      if (cache != null && hardTtl != null && softTtl != null) {
        break;
      }
    }
    return cache == null && hardTtl == null && softTtl == null ? null : new ClassConfig(cache, hardTtl, softTtl);
  }

  /**
   * Reject a verb on a final method of a concrete class when no interface view can carry it. A
   * CGLIB proxy cannot override a final method, so the advice would never run; a JDK proxy can
   * intercept the same method through an interface view, which is why that case is allowed.
   */
  private static void requireInterceptable(Verb verb, Method specific, Set<Method> views) {
    if (specific.getDeclaringClass().isInterface() || !Modifier.isFinal(specific.getModifiers())) {
      return;
    }
    for (Method view : views) {
      if (view.getDeclaringClass().isInterface()) {
        // A JDK proxy can intercept through the interface view, but a CGLIB proxy cannot override
        // the final implementation method. The proxy mode is chosen later by Spring, so the
        // annotation cannot decide here; surface the risk instead of failing a valid JDK setup.
        log.warn(
          "[Zeta] {} on final method {} is interceptable only through its interface view: a CGLIB proxy " +
            "(spring.aop.proxy-target-class=true) cannot override it and will bypass the annotation",
          verb.role(),
          specific
        );
        return;
      }
    }
    throw new IllegalStateException(
      verb.role() +
        " on " +
        specific +
        " cannot be intercepted: a final method on a concrete class cannot be overridden by a CGLIB " +
        "proxy and no interface view declares it; remove final or declare the method on an interface"
    );
  }

  /**
   * Resolve the method group's single operation shape in one pass. Every view that declares a
   * verb participates in the mutual-exclusion check. Only the maximal views of the declaring-type
   * partial order are compared: an implementation or subclass declaration dominates what it
   * overrides, while an inherited superclass method and a newly implemented interface are
   * unrelated and must agree.
   */
  private static @Nullable Verb resolveVerb(Method specific, Set<Method> views) {
    Map<Method, Verb> declarations = new LinkedHashMap<>();
    for (Method view : views) {
      if (view.isBridge() && !view.equals(specific)) {
        continue;
      }
      Verb verb = declaredVerb(view);
      if (verb != null) {
        declarations.put(view, verb);
      }
    }
    if (declarations.isEmpty()) {
      return null;
    }

    Class<? extends Annotation> type = null;
    for (Verb verb : declarations.values()) {
      Class<? extends Annotation> verbType = verb.annotationType();
      if (type == null) {
        type = verbType;
      } else if (type != verbType) {
        throw new IllegalStateException(
          ZETA_CACHED +
            ", " +
            ZETA_PUT +
            " and " +
            ZETA_INVALIDATE +
            " are mutually exclusive, but " +
            specific +
            " declares conflicting verbs: " +
            declarations.values().stream().map(Verb::role).distinct().toList()
        );
      }
    }

    Verb winner = null;
    for (Map.Entry<Method, Verb> entry : declarations.entrySet()) {
      Method view = entry.getKey();
      boolean dominated = false;
      for (Method other : declarations.keySet()) {
        if (other != view && dominates(other, view)) {
          dominated = true;
          break;
        }
      }
      if (dominated) {
        continue;
      }
      Verb candidate = entry.getValue();
      if (winner == null) {
        winner = candidate;
      } else if (!winner.sameDeclaration(candidate)) {
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
   * The verb one view declares, or {@code null}. A composed annotation is resolved through its
   * meta-annotation and {@code @AliasFor} overrides; repeated invalidations are flattened. Two
   * different read or write declarations on one view are a configuration error.
   */
  private static @Nullable Verb declaredVerb(Method method) {
    MergedAnnotations annotations = MergedAnnotations.from(
      method,
      MergedAnnotations.SearchStrategy.DIRECT,
      RepeatableContainers.of(ZetaInvalidate.class, ZetaInvalidates.class),
      AnnotationFilter.PLAIN
    );
    ZetaCached cached = null;
    ZetaPut put = null;
    List<ZetaInvalidate> invalidates = new ArrayList<>();
    for (MergedAnnotation<Annotation> merged : annotations) {
      Class<? extends Annotation> type = merged.getType();
      if (type == ZetaCached.class) {
        cached = mergeAnnotation(ZETA_CACHED, cached, (ZetaCached) merged.synthesize(), method);
      } else if (type == ZetaPut.class) {
        put = mergeAnnotation(ZETA_PUT, put, (ZetaPut) merged.synthesize(), method);
      } else if (type == ZetaInvalidate.class) {
        invalidates.add((ZetaInvalidate) merged.synthesize());
      }
    }
    if ((cached != null && (put != null || !invalidates.isEmpty())) || (put != null && !invalidates.isEmpty())) {
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
    return invalidates.isEmpty() ? null : new Verb.Invalidate(method, invalidates);
  }

  /** One verb declared twice on the same view with different attributes is a configuration error. */
  private static <A extends Annotation> A mergeAnnotation(
    String role,
    @Nullable A current,
    A candidate,
    Method method
  ) {
    if (current == null) {
      return candidate;
    }
    if (!current.equals(candidate)) {
      throw new IllegalStateException(
        "conflicting " + role + " declarations on " + method + ": " + current + " vs " + candidate
      );
    }
    return current;
  }

  /**
   * Whether {@code candidate}'s declaring type is a strict subtype of {@code other}'s: a
   * class dominates its superclass and the interfaces it implements, and a sub-interface
   * dominates its super-interface. Two sibling interfaces dominate nothing.
   */
  private static boolean dominates(Method candidate, Method other) {
    Class<?> candidateClass = candidate.getDeclaringClass();
    Class<?> otherClass = other.getDeclaringClass();
    return candidateClass != otherClass && otherClass.isAssignableFrom(candidateClass);
  }

  /**
   * One method's resolution inputs: its most-specific method, the parameter names its expressions
   * were written against, the slot table built from them, and the class-level defaults. Every
   * attribute of the method is compiled against this context, so the plan builders do not thread
   * the same four values through each helper.
   */
  private final class MethodResolution {

    private final Method specific;
    private final ParameterBindings bindings;
    private final @Nullable ClassConfig config;
    private final String@Nullable [] parameterNames;

    private MethodResolution(
      Method specific,
      ParameterBindings bindings,
      @Nullable ClassConfig config,
      String@Nullable [] parameterNames
    ) {
      this.specific = specific;
      this.bindings = bindings;
      this.config = config;
      this.parameterNames = parameterNames;
    }

    /** Resolve a {@code @ZetaCached} method into its read plan. */
    private CachePlan.Read readPlan(Set<Method> views, ZetaCached read) {
      requireValueReturn(specific, ZETA_CACHED);
      String namespace = namespace(cacheName(read.cache()));
      CachePlan.Ttls ttls = ttls(read.hardTtl(), read.softTtl(), false);
      GetPolicy prebuiltPolicy = ttls.bothConstant()
        ? CachePlan.readPolicy(ttls.hard().constantSupplier(), ttls.soft().constantSupplier())
        : null;
      return CachePlan.Read.of(
        specific,
        bindings,
        key(read.key(), false, namespace),
        condition(read.condition(), false),
        unless(read.unless()),
        ttls,
        prebuiltPolicy,
        bridgeView(specific, views)
      );
    }

    /** Resolve a {@code @ZetaPut} method into its write plan. */
    private CachePlan.Write writePlan(ZetaPut write) {
      requireValueReturn(specific, ZETA_PUT);
      String cache = cacheName(write.cache());
      String namespace = namespace(cache);
      CachePlan.Ttls ttls = ttls(write.hardTtl(), write.softTtl(), true);
      if (write.sync() && ttls.hard().isPermanent()) {
        // A constant is knowable at resolution and must not let a business side effect happen
        // before the configuration error surfaces. A dynamic hard TTL that resolves to
        // permanent is checked before the body by Write.preflightHardTtl.
        throw new IllegalStateException(
          ZETA_PUT +
            " sync=true cannot write a permanent entry (hardTtl = Long.MAX_VALUE): peers expire entries " +
            "only through the hard TTL, so the value would stay stale on every peer: " +
            specific
        );
      }
      PutPolicy prebuiltPolicy = ttls.bothConstant() ? prebuiltWritePolicy(ttls, write.sync()) : null;
      return CachePlan.Write.of(
        specific,
        cache,
        bindings,
        key(write.key(), true, namespace),
        condition(write.condition(), false),
        unless(write.unless()),
        ttls,
        write.sync(),
        prebuiltPolicy
      );
    }

    /** Resolve one {@code @ZetaInvalidate} declaration into one executable operation. */
    private CachePlan.Invalidate.Operation invalidateOperation(ZetaInvalidate invalidate) {
      boolean before = invalidate.beforeInvocation();
      boolean allEntries = invalidate.allEntries();
      boolean eachKey = invalidate.eachKey();
      if (allEntries && eachKey) {
        throw new IllegalStateException(
          ZETA_INVALIDATE + " on " + specific + " cannot combine allEntries = true with eachKey = true"
        );
      }
      if (allEntries && !invalidate.key().isBlank()) {
        throw new IllegalStateException(
          ZETA_INVALIDATE +
            " on " +
            specific +
            " cannot combine allEntries = true with key = \"" +
            invalidate.key() +
            "\""
        );
      }
      if (eachKey && invalidate.key().isBlank()) {
        throw new IllegalStateException(
          ZETA_INVALIDATE + " on " + specific + " requires an explicit key when eachKey = true"
        );
      }
      String cache = cacheName(invalidate.cache());
      boolean resultAllowed = !before && returnsValue(specific);
      CachePlan.Target target;
      if (allEntries) {
        target = new CachePlan.Target.Namespace(cache);
      } else if (eachKey) {
        CompiledExpression expression = compiledKey(invalidate.key(), resultAllowed);
        requireExpandableKey(expression);
        target = new CachePlan.Target.Keys(cache, namespace(cache), expression, specific);
      } else {
        CompiledExpression compiled = compiledKey(invalidate.key(), resultAllowed);
        warnIfCollectionKey(compiled);
        target = new CachePlan.Target.Key(
          cache,
          CachePlan.Key.of(namespace(cache), specific, compiled, parameterNames)
        );
      }
      return new CachePlan.Invalidate.Operation(
        target,
        new InvalidatePolicy(invalidate.scope(), before),
        condition(invalidate.condition(), resultAllowed)
      );
    }

    /** Compile the key expression and classify a simple single-argument form. */
    private CachePlan.Key key(@Nullable String raw, boolean resultAllowed, String namespace) {
      return CachePlan.Key.of(namespace, specific, compiledKey(raw, resultAllowed), parameterNames);
    }

    private CompiledExpression compiledKey(@Nullable String raw, boolean resultAllowed) {
      return CompiledExpression.of(raw, parser, "cache key", specific, parameterNames, resultAllowed);
    }

    /** Compile {@code condition}; the caller declares whether {@code #result} is available. */
    private CompiledExpression condition(String raw, boolean resultAllowed) {
      CompiledExpression compiled = CompiledExpression.of(
        raw,
        parser,
        "condition",
        specific,
        parameterNames,
        resultAllowed
      );
      requireBooleanConstant(compiled, "condition", specific);
      return compiled;
    }

    /** Compile {@code unless}; it is always evaluated after the body, so {@code #result} is available. */
    private CompiledExpression unless(String raw) {
      CompiledExpression compiled = CompiledExpression.of(raw, parser, "unless", specific, parameterNames, true);
      requireBooleanConstant(compiled, "unless", specific);
      return compiled;
    }

    /**
     * Classify both TTL attributes of one verb once, applying the class-level default to an
     * attribute the method left empty.
     */
    private CachePlan.Ttls ttls(String hardTtl, String softTtl, boolean resultAllowed) {
      String hard = inherited(hardTtl, config == null ? null : config.hardTtl());
      String soft = inherited(softTtl, config == null ? null : config.softTtl());
      return new CachePlan.Ttls(ttl("hardTtl", hard, resultAllowed), ttl("softTtl", soft, resultAllowed));
    }

    /** Compile one TTL attribute, resolving {@code ${...}} under the role that names it. */
    private CachePlan.Ttl ttl(String role, @Nullable String raw, boolean resultAllowed) {
      return CachePlan.Ttl.of(
        role,
        resolvePlaceholders(role, raw, specific),
        parser,
        specific,
        parameterNames,
        resultAllowed
      );
    }

    /**
     * Resolve {@code ${...}} in the cache name, then validate it. A blank or NUL-bearing name is a
     * configuration error. An empty method attribute inherits the class-level default.
     */
    private String cacheName(@Nullable String rawCache) {
      String cache = resolvePlaceholders(
        "cache name",
        inherited(rawCache, config == null ? null : config.cache()),
        specific
      );
      if (cache == null || cache.isBlank()) {
        throw new IllegalStateException(
          "cache name must not be blank: set cache on " +
            specific +
            " or @ZetaConfig.cache on its target or declaring class"
        );
      }
      if (cache.indexOf(NAMESPACE_DELIMITER) >= 0) {
        throw new IllegalStateException("cache name must not contain U+0000: " + specific);
      }
      return cache;
    }

    /**
     * Warn when an invalidation declares a whole collection/array parameter as one key. The
     * annotation is legal — a collection can be one composite key — but in a batch delete it is
     * almost always a missing {@code eachKey = true}, and the mistake leaves every per-element
     * entry stale until its TTL.
     */
    private void warnIfCollectionKey(CompiledExpression expression) {
      Class<?> type = singleParameterType(expression);
      if (type == null && (expression.absent() || expression.wholeRootArgs()) && specific.getParameterCount() == 1) {
        // An absent key (or the whole #root.args) over one collection/array parameter is still one
        // composite key, not a per-element removal; the single-variable probe cannot see it.
        type = specific.getParameterTypes()[0];
      }
      if (type == null || !isExpandable(type)) {
        return;
      }
      log.warn(
        "[Zeta] @ZetaInvalidate on {} uses collection/array parameter '{}' as one key; add eachKey = true " +
          "to invalidate each element, or declare a different key if the whole collection is intentionally one key",
        specific,
        expression.raw() == null ? "the default key" : expression.raw()
      );
    }

    /** Reject a key that is statically proven unable to expand to an array or {@link Iterable}. */
    private void requireExpandableKey(CompiledExpression expression) {
      Class<?> type = singleParameterType(expression);
      if (type == null || !canNeverExpand(type)) {
        return;
      }
      throw new IllegalStateException(
        ZETA_INVALIDATE +
          " on " +
          specific +
          " uses eachKey = true, but key '" +
          expression.raw() +
          "' is " +
          type.getName() +
          ", not an array or Iterable (a primitive or final class cannot be expanded)"
      );
    }

    /** The type of the one parameter a whole single-variable expression names, or {@code null}. */
    private @Nullable Class<?> singleParameterType(CompiledExpression expression) {
      String variable = expression.singleVariable();
      if (variable == null) {
        return null;
      }
      int index = bindings.slotOf(variable);
      return index >= 0 ? specific.getParameterTypes()[index] : null;
    }
  }

  /**
   * Discover the names the expression was written against: the view that actually declares
   * the verb annotation owns them. A key such as {@code #userId} must bind the interface's
   * parameter even when the implementation chose a different local name. Only when the
   * declaring method carries no parameter metadata does discovery fall back to the
   * group's most-specific method, then to any remaining source view, preferring a subtype
   * declaration over the one it overrides. A compiler-generated bridge is never a source
   * view: its parameter metadata is synthetic and must not outrank a real declaration.
   */
  private String@Nullable [] parameterNames(Method annotated, Method specific, Set<Method> views) {
    String[] names = parameterNameDiscoverer.getParameterNames(annotated);
    if (names != null) {
      return names;
    }
    names = parameterNameDiscoverer.getParameterNames(specific);
    if (names != null) {
      return names;
    }
    Method foundView = null;
    String[] foundNames = null;
    for (Method view : views) {
      if (view.isBridge() || view.equals(specific)) {
        continue;
      }
      String[] candidate = parameterNameDiscoverer.getParameterNames(view);
      if (candidate == null) {
        continue;
      }
      if (foundView == null || dominates(view, foundView)) {
        foundView = view;
        foundNames = candidate;
      }
    }
    return foundNames;
  }

  /**
   * Resolve {@code ${...}} in one attribute and reject an unresolved token. A leftover
   * placeholder would otherwise reach the SpEL parser, where {@code $} is an identifier and
   * {@code {my.ttl}} an inline list, producing a syntax error that names neither the
   * placeholder nor the attribute. A resolver failure is wrapped with the same role and
   * method, so a missing property points at the annotation that owns it.
   */
  private @Nullable String resolvePlaceholders(String role, @Nullable String raw, Method method) {
    if (raw == null || !raw.contains("${")) {
      return raw;
    }
    String resolved = raw;
    if (beanFactory != null) {
      try {
        resolved = beanFactory.resolveEmbeddedValue(raw);
      } catch (RuntimeException e) {
        throw new IllegalStateException(role + " placeholder '" + raw + "' could not be resolved for " + method, e);
      }
    }
    if (resolved != null && resolved.contains("${")) {
      throw new IllegalStateException(
        role +
          " placeholder was not resolved ('" +
          resolved +
          "') for " +
          method +
          "; register a placeholder resolver or replace the placeholder"
      );
    }
    return resolved;
  }

  /**
   * Reject a compile-time constant {@code condition} or {@code unless} that is not a
   * {@code Boolean}. The runtime path asks SpEL for a {@code Boolean}; a constant of another type
   * would either convert by accident or throw on every call, so the attribute must not depend on a
   * conversion rule. A constant {@code null} stays legal, because a null guard is a defined
   * "skip" answer on both attributes.
   */
  private static void requireBooleanConstant(CompiledExpression compiled, String role, Method method) {
    if (compiled.absent() || !compiled.constant()) {
      return;
    }
    Object value = compiled.evaluateConstant();
    if (value != null && !(value instanceof Boolean)) {
      throw new IllegalStateException(
        role +
          " '" +
          compiled.raw() +
          "' on " +
          method +
          " must evaluate to a boolean, got " +
          value.getClass().getName()
      );
    }
  }

  @SuppressWarnings("all")
  private static boolean isExpandable(Class<?> type) {
    return type.isArray() || Iterable.class.isAssignableFrom(type);
  }

  /**
   * Whether a declared parameter type is statically proven unable to be an array or
   * {@link Iterable}: a primitive, or a final class that implements neither. An {@code Object},
   * interface or non-final class (the erasure of a type variable included) stays eligible, because
   * the runtime value can still be a collection.
   */
  private static boolean canNeverExpand(Class<?> type) {
    return !isExpandable(type) && (type.isPrimitive() || Modifier.isFinal(type.getModifiers()));
  }

  /** The write policy for a constant pair, evaluated once at resolution. */
  private static PutPolicy prebuiltWritePolicy(CachePlan.Ttls ttls, boolean sync) {
    long hard = ttls.hard().evaluate(null);
    long soft = ttls.soft().evaluate(null);
    return hard == 0L && soft == 0L
      ? (sync ? PutPolicy.synced() : PutPolicy.defaults())
      : PutPolicy.of(hard, soft, sync);
  }

  /** The class-level value supplies a method attribute that is empty or whitespace-only. */
  private static @Nullable String inherited(@Nullable String methodValue, @Nullable String classValue) {
    return methodValue == null || methodValue.isBlank() ? classValue : methodValue;
  }

  private static String namespace(String cache) {
    return cache + NAMESPACE_DELIMITER;
  }

  private static void requireValueReturn(Method method, String annotation) {
    if (!returnsValue(method)) {
      throw new IllegalStateException(
        annotation + " needs a value to cache, but " + method + " returns " + method.getReturnType()
      );
    }
  }

  /** Whether the method returns a value that can be bound to {@code #result}. */
  private static boolean returnsValue(Method method) {
    Class<?> returnType = method.getReturnType();
    return returnType != void.class && returnType != Void.class;
  }

  /**
   * Claim reflective access once, at resolution. The call is unconditional: public visibility is
   * not enough under JPMS when the package is opened but not exported, and an accessible flag also
   * lets {@link Method#invoke} skip the per-call caller-sensitive access check. A target that
   * cannot be opened is not fatal: the first refresh reports the {@link IllegalAccessException} in
   * its own failure path.
   */
  private static void prewarmAccess(Method method) {
    method.trySetAccessible();
  }

  /**
   * The widest bridge view of a generic override from the already-scanned view set, or {@code null}.
   * A class scan collects bridges next to the method they bridge, so no separate per-class method
   * scan is needed. Several bridges can exist for one method (a three-level generic hierarchy
   * generates a {@code Number} and an {@code Object} bridge); the widest signature is the one a
   * target most reliably exposes, and choosing it keeps the selection independent of
   * {@link Class#getDeclaredMethods()} order.
   */
  private static @Nullable Method bridgeView(Method specific, Set<Method> views) {
    Class<?> declaringClass = specific.getDeclaringClass();
    Method best = specific.isBridge() ? specific : null;
    for (Method view : views) {
      if (!view.isBridge() || view.getDeclaringClass() != declaringClass) {
        continue;
      }
      if (best == null || isWider(view.getParameterTypes(), best.getParameterTypes())) {
        best = view;
      }
    }
    return best;
  }

  /** Whether {@code candidate} accepts every value {@code current} does, with a wider type somewhere. */
  private static boolean isWider(Class<?>[] candidate, Class<?>[] current) {
    if (candidate.length != current.length) {
      return false;
    }
    return TargetMethodResolver.isMoreSpecific(current, candidate);
  }

  /** The attribute-wise class defaults one type hierarchy contributes, with blank attributes absent. */
  private record ClassConfig(@Nullable String cache, @Nullable String hardTtl, @Nullable String softTtl) {
    /**
     * Merge another applicable root's defaults. Complementary attributes combine; two roots that
     * set the same attribute to different values are ambiguous and fail resolution.
     */
    private ClassConfig merge(ClassConfig other, Method method) {
      return new ClassConfig(
        mergeAttribute("cache", cache, other.cache, method),
        mergeAttribute("hardTtl", hardTtl, other.hardTtl, method),
        mergeAttribute("softTtl", softTtl, other.softTtl, method)
      );
    }

    private static @Nullable String mergeAttribute(
      String attribute,
      @Nullable String current,
      @Nullable String candidate,
      Method method
    ) {
      if (current == null) {
        return candidate;
      }
      if (candidate != null && !current.equals(candidate)) {
        throw new IllegalStateException(
          "conflicting @ZetaConfig." +
            attribute +
            " declarations for " +
            method +
            ": '" +
            current +
            "' vs '" +
            candidate +
            "'; declare one on a common parent or on the implementation class"
        );
      }
      return current;
    }
  }

  /** The plan, unwrapped receiver and authoritative target class of one invocation. */
  record InvocationPlan(CachePlan plan, @Nullable Object target, Class<?> targetClass) {}

  /** The signature that makes two interface declarations the same logical method. */
  private record MethodSignature(String name, List<Class<?>> parameterTypes) {
    static MethodSignature of(Method method) {
      return new MethodSignature(method.getName(), List.of(method.getParameterTypes()));
    }
  }

  /** The one operation shape a method group declares, resolved with its declaring view. */
  private sealed interface Verb permits Verb.Read, Verb.Write, Verb.Invalidate {
    Method view();

    String role();

    Class<? extends Annotation> annotationType();

    /** Whether two maximal declarations describe the same operation. */
    boolean sameDeclaration(Verb other);

    /** The declaration text used in a conflict message. */
    Object declaration();

    record Read(Method view, ZetaCached annotation) implements Verb {
      @Override
      public String role() {
        return ZETA_CACHED;
      }

      @Override
      public Class<? extends Annotation> annotationType() {
        return ZetaCached.class;
      }

      @Override
      public boolean sameDeclaration(Verb other) {
        return other instanceof Read read && annotation.equals(read.annotation);
      }

      @Override
      public Object declaration() {
        return annotation;
      }
    }

    record Write(Method view, ZetaPut annotation) implements Verb {
      @Override
      public String role() {
        return ZETA_PUT;
      }

      @Override
      public Class<? extends Annotation> annotationType() {
        return ZetaPut.class;
      }

      @Override
      public boolean sameDeclaration(Verb other) {
        return other instanceof Write write && annotation.equals(write.annotation);
      }

      @Override
      public Object declaration() {
        return annotation;
      }
    }

    record Invalidate(Method view, List<ZetaInvalidate> annotations) implements Verb {
      public Invalidate {
        annotations = List.copyOf(new LinkedHashSet<>(annotations));
      }

      @Override
      public String role() {
        return ZETA_INVALIDATE;
      }

      @Override
      public Class<? extends Annotation> annotationType() {
        return ZetaInvalidate.class;
      }

      @Override
      public boolean sameDeclaration(Verb other) {
        return other instanceof Invalidate invalidate && annotations.equals(invalidate.annotations);
      }

      @Override
      public Object declaration() {
        return annotations;
      }
    }
  }
}
