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

import io.github.hyshmily.zeta.annotation.ZetaCacheInterceptor.KeyPlan;
import io.github.hyshmily.zeta.annotation.ZetaCacheInterceptor.Plan;
import io.github.hyshmily.zeta.annotation.ZetaCacheInterceptor.Ttl;
import io.github.hyshmily.zeta.annotation.ZetaCacheInterceptor.Ttls;
import io.github.hyshmily.zeta.annotation.supporter.CompiledExpression;
import io.github.hyshmily.zeta.annotation.supporter.ZetaEvaluationContext;
import io.github.hyshmily.zeta.cache.CacheKeysPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.InvalidatePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.PutPolicy;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.expression.BeanResolver;
import org.springframework.expression.spel.SpelCompilerMode;
import org.springframework.expression.spel.SpelParserConfiguration;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

/**
 * Resolves an annotated method group into the one {@link Plan} the
 * interceptor executes.
 *
 * <p>Resolution is the fail-fast boundary: a blank namespace, a void/Void return, a
 * conflicting verb, an unparsable expression, an unknown or out-of-range variable, a
 * pre-body {@code #result} or a permanent synced write fails bean creation instead of the
 * first request. The first question about a class resolves every candidate method once;
 * later questions are one {@code ClassValue} lookup.
 */
final class PlanResolver {

  private static final String ZETA_PUT = "@ZetaPut";

  private static final String ZETA_CACHED = "@ZetaCached";

  private static final String ZETA_INVALIDATE = "@ZetaInvalidate";

  private static final char NAMESPACE_DELIMITER = CacheKeysPolicy.NAMESPACE_DELIMITER;

  /** Embedded-value resolution for {@code ${...}} in {@code cache} and both TTL attributes. */
  private final @Nullable ConfigurableBeanFactory beanFactory;

  private final BeanResolver beanResolver;

  /**
   * SpEL parser in {@link SpelCompilerMode#MIXED} mode: an expression is interpreted first
   * and compiled after the engine's evaluation threshold, so a key/TTL/unless on a
   * steady-state path stops paying interpretation overhead without paying compilation for a
   * method nobody calls.
   */
  private final SpelExpressionParser parser = new SpelExpressionParser(
    new SpelParserConfiguration(SpelCompilerMode.MIXED, ClassUtils.getDefaultClassLoader())
  );

  /**
   * The reflection machinery every evaluation phase shares. Per resolver, not static: its
   * caches must be released with the Spring context and its type locator must resolve
   * against the same classloader as {@link #parser}.
   */
  private final ZetaEvaluationContext.Support expressionSupport = new ZetaEvaluationContext.Support(
    ClassUtils.getDefaultClassLoader()
  );

  private final ParameterNameDiscoverer parameterNameDiscoverer = new DefaultParameterNameDiscoverer();

  /**
   * The per-target-class plan table. One lookup answers both the pointcut's "is this a verb
   * method" and the invocation's plan, with no key allocation. The scan runs inside
   * {@link ClassValue#computeValue}: a throwing scan installs nothing, so the class stays
   * unvalidated and the next question retries it; a successful scan publishes one immutable
   * table atomically.
   */
  private final ClassValue<Map<Method, Plan>> classPlans = new ClassValue<>() {
    @Override
    protected Map<Method, Plan> computeValue(@NonNull Class<?> targetClass) {
      Class<?> userClass = ClassUtils.getUserClass(targetClass);
      if (userClass != targetClass) {
        // A CGLIB proxy shares the user class's immutable table instead of scanning the
        // generated subclass.
        return classPlans.get(userClass);
      }
      Set<Class<?>> classes = new LinkedHashSet<>();
      classes.add(userClass);
      classes.addAll(ClassUtils.getAllInterfacesForClassAsSet(userClass));

      // One implementation method can be reached through the implementation, a bridge, a
      // generic interface and several unrelated interfaces. Group every view by its
      // most-specific method first, then resolve the whole group once; every view of an
      // annotated group later answers with the same plan. The interface walk reaches the
      // same method through more than one path, so the view set is deduplicated by Method
      // equality.
      Map<Method, Set<Method>> viewsBySpecific = new LinkedHashMap<>();
      for (Class<?> candidateClass : classes) {
        for (Method candidate : ReflectionUtils.getAllDeclaredMethods(candidateClass)) {
          if (ignored(candidate)) {
            continue;
          }
          Method specific = AopUtils.getMostSpecificMethod(candidate, userClass);
          viewsBySpecific.computeIfAbsent(specific, key -> new LinkedHashSet<>()).add(candidate);
        }
      }

      Map<Method, Plan> resolved = new HashMap<>();
      for (Map.Entry<Method, Set<Method>> entry : viewsBySpecific.entrySet()) {
        Method specific = entry.getKey();
        Set<Method> views = entry.getValue();
        Plan plan = planFor(specific, views);
        if (plan == null) {
          continue;
        }
        // The grouping key is itself one of the views, so one pass stores and warms every
        // view of the group.
        for (Method view : views) {
          resolved.put(view, plan);
          prewarmAccess(view);
        }
      }
      return Map.copyOf(resolved);
    }
  };

  PlanResolver(@Nullable BeanFactory beanFactory) {
    this.beanFactory = beanFactory instanceof ConfigurableBeanFactory configurable ? configurable : null;
    this.beanResolver = beanFactory == null ? null : new BeanFactoryResolver(beanFactory);
  }

  /** The pointcut decision, and the startup fail-fast boundary. */
  boolean matches(Method method, Class<?> targetClass) {
    return !ignored(method) && plan(method, targetClass) != null;
  }

  /**
   * The plan for one invocation. A class's table is computed once and keyed by the user
   * class, so a CGLIB proxy class and its target share one resolution; a method reported
   * only by the proxy is mapped to its most-specific user-class method before the lookup.
   */
  @Nullable
  Plan plan(Method method, Class<?> targetClass) {
    Map<Method, Plan> plans = classPlans.get(targetClass);
    Plan plan = plans.get(method);
    if (plan != null) {
      return plan;
    }
    Class<?> userClass = ClassUtils.getUserClass(targetClass);
    if (!method.getDeclaringClass().isAssignableFrom(userClass)) {
      // The target class does not implement the reported method (a nested interface proxy).
      // The declaring type's own table is the authority.
      plan = classPlans.get(method.getDeclaringClass()).get(method);
      if (plan != null) {
        return plan;
      }
    }
    Method specific = AopUtils.getMostSpecificMethod(method, userClass);
    return specific == method ? null : plans.get(specific);
  }

  ZetaEvaluationContext.Support expressionSupport() {
    return expressionSupport;
  }

  @Nullable
  BeanResolver beanResolver() {
    return beanResolver;
  }

  /** Methods Spring's pointcut and the class scan both exclude. */
  private static boolean ignored(Method method) {
    return (
      Modifier.isStatic(method.getModifiers()) ||
      Modifier.isPrivate(method.getModifiers()) ||
      (method.isSynthetic() && !method.isBridge())
    );
  }

  private @Nullable Plan planFor(Method specific, Set<Method> views) {
    ResolvedVerb resolved = resolveVerb(specific, views);
    if (resolved == null) {
      return null;
    }
    String[] parameterNames = parameterNames(resolved.view(), specific, views);
    Verb verb = resolved.verb();
    if (verb instanceof Verb.Read read) {
      return readPlan(specific, parameterNames, read.annotation());
    }
    if (verb instanceof Verb.Write write) {
      return writePlan(specific, parameterNames, write.annotation());
    }
    if (verb instanceof Verb.Invalidate invalidate) {
      return invalidatePlan(specific, parameterNames, invalidate.annotation());
    }
    throw new AssertionError("unknown verb: " + verb);
  }

  /**
   * Resolve the method group's single cache verb in one pass. Every view that declares a
   * verb participates in the mutual-exclusion check, so a different verb on a
   * super-interface is a conflict even when the implementation declares its own verb. Among
   * declarations of the one winning kind, the implementation method wins outright; otherwise
   * only the maximal views of the declaring-type partial order are compared.
   */
  private static @Nullable ResolvedVerb resolveVerb(Method specific, Set<Method> views) {
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

    Verb.Kind kind = null;
    for (Verb verb : declarations.values()) {
      if (kind == null) {
        kind = verb.kind();
      } else if (kind != verb.kind()) {
        throw new IllegalStateException(
          ZETA_CACHED +
            ", " +
            ZETA_PUT +
            " and " +
            ZETA_INVALIDATE +
            " are mutually exclusive, but " +
            specific +
            " declares " +
            declarations.size()
        );
      }
    }

    Verb direct = declarations.get(specific);
    if (direct != null) {
      return new ResolvedVerb(specific, direct);
    }

    ResolvedVerb winner = null;
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
        winner = new ResolvedVerb(view, candidate);
      } else if (!winner.verb().equals(candidate)) {
        throw new IllegalStateException(
          "conflicting " +
            winner.verb().role() +
            " declarations for " +
            specific +
            ": " +
            winner.verb() +
            " vs " +
            candidate +
            "; declare one on the implementation method"
        );
      }
    }
    return winner;
  }

  /** The verb one view declares, or {@code null}; two verbs on one view are a configuration error. */
  private static @Nullable Verb declaredVerb(Method method) {
    ZetaCached cached = declaredAnnotation(method, ZetaCached.class);
    ZetaPut put = declaredAnnotation(method, ZetaPut.class);
    ZetaInvalidate invalidate = declaredAnnotation(method, ZetaInvalidate.class);
    int count = (cached != null ? 1 : 0) + (put != null ? 1 : 0) + (invalidate != null ? 1 : 0);
    if (count > 1) {
      throw new IllegalStateException(
        ZETA_CACHED + ", " + ZETA_PUT + " and " + ZETA_INVALIDATE + " are mutually exclusive on " + method
      );
    }
    return cached != null
      ? new Verb.Read(cached)
      : put != null
        ? new Verb.Write(put)
        : invalidate != null
          ? new Verb.Invalidate(invalidate)
          : null;
  }

  private static <A extends Annotation> @Nullable A declaredAnnotation(Method method, Class<A> type) {
    return declaresAnnotation(method, type) ? AnnotatedElementUtils.getMergedAnnotation(method, type) : null;
  }

  /**
   * Whether the method itself declares the annotation, directly or through a composed
   * annotation. Unlike a merged lookup, this never walks overridden interface or superclass
   * methods, so an unrelated annotation cannot make an interface declaration look local.
   */
  private static boolean declaresAnnotation(Method method, Class<? extends Annotation> annotationType) {
    for (Annotation declared : method.getDeclaredAnnotations()) {
      if (
        declared.annotationType() == annotationType ||
        AnnotatedElementUtils.hasAnnotation(declared.annotationType(), annotationType)
      ) {
        return true;
      }
    }
    return false;
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

  /** Resolve a {@code @ZetaCached} method into its read plan. */
  private Plan.Read readPlan(Method specific, String@Nullable [] parameterNames, ZetaCached read) {
    requireValueReturn(specific, ZETA_CACHED);
    String namespace = namespace(specific, read.cache());
    Ttls ttls = ttls(specific, parameterNames, read.hardTtl(), read.softTtl(), false);
    GetPolicy prebuiltPolicy = ttls.bothConstant()
      ? ZetaCacheInterceptor.readPolicy(ttls.hard().constantSupplier(), ttls.soft().constantSupplier())
      : null;
    return new Plan.Read(
      specific,
      parameterNames,
      keyOf(specific, parameterNames, read.key(), false, namespace),
      unlessOf(specific, parameterNames, read.unless()),
      ttls,
      prebuiltPolicy
    );
  }

  /** Resolve a {@code @ZetaPut} method into its write plan. */
  private Plan.Write writePlan(Method specific, String@Nullable [] parameterNames, ZetaPut write) {
    requireValueReturn(specific, ZETA_PUT);
    String namespace = namespace(specific, write.cache());
    Ttls ttls = ttls(specific, parameterNames, write.hardTtl(), write.softTtl(), true);
    if (write.sync() && ttls.hard().isPermanent()) {
      // A constant is knowable at resolution and must not let a business side effect happen
      // before the configuration error surfaces. A dynamic hard TTL that resolves to
      // permanent is checked before the body by write().
      throw new IllegalStateException(
        ZETA_PUT +
          " sync=true cannot write a permanent entry (hardTtl = Long.MAX_VALUE): peers expire entries " +
          "only through the hard TTL, so the value would stay stale on every peer: " +
          specific
      );
    }
    PutPolicy prebuiltPolicy = null;
    if (ttls.bothConstant()) {
      long hard = ttls.hard().literalMs();
      long soft = ttls.soft().literalMs();
      prebuiltPolicy =
        hard == 0L && soft == 0L
          ? (write.sync() ? PutPolicy.synced() : PutPolicy.defaults())
          : PutPolicy.of(hard, soft, write.sync());
    }
    return new Plan.Write(
      specific,
      parameterNames,
      keyOf(specific, parameterNames, write.key(), true, namespace),
      unlessOf(specific, parameterNames, write.unless()),
      ttls,
      write.sync(),
      prebuiltPolicy
    );
  }

  /** Resolve a {@code @ZetaInvalidate} method into its invalidation plan. */
  private Plan.Invalidate invalidatePlan(
    Method specific,
    String@Nullable [] parameterNames,
    ZetaInvalidate invalidate
  ) {
    boolean before = invalidate.beforeInvocation();
    String namespace = namespace(specific, invalidate.cache());
    // A void/Void body has no result to bind: admitting #result here would resolve to null
    // (a silent no-op) or fail only after the business side effect already ran.
    boolean resultAllowed = !before && returnsValue(specific);
    return new Plan.Invalidate(
      specific,
      parameterNames,
      keyOf(specific, parameterNames, invalidate.key(), resultAllowed, namespace),
      new InvalidatePolicy(invalidate.scope(), before)
    );
  }

  /**
   * Discover the names the expression was written against: the view that actually declares
   * the verb annotation owns them. A key such as {@code #userId} must bind the interface's
   * parameter even when the implementation chose a different local name. Only when the
   * declaring method carries no parameter metadata does discovery fall back to the
   * most-specific implementation method, then to any remaining view, preferring a subtype
   * declaration over the one it overrides.
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
      if (view.equals(specific)) {
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

  /** Resolve {@code ${...}} in an attribute; a null value stays absent. */
  private @Nullable String resolvePlaceholders(@Nullable String raw) {
    if (raw == null || beanFactory == null) {
      return raw;
    }
    return beanFactory.resolveEmbeddedValue(raw);
  }

  /** Classify both TTL attributes of one verb once. */
  private Ttls ttls(
    Method method,
    String@Nullable [] parameterNames,
    String hardRaw,
    String softRaw,
    boolean resultAllowed
  ) {
    return new Ttls(
      Ttl.of(
        "hardTtl",
        resolveTtlPlaceholders("hardTtl", hardRaw, method),
        parser,
        method,
        parameterNames,
        resultAllowed
      ),
      Ttl.of(
        "softTtl",
        resolveTtlPlaceholders("softTtl", softRaw, method),
        parser,
        method,
        parameterNames,
        resultAllowed
      )
    );
  }

  /**
   * Resolve {@code ${...}} in one TTL attribute and reject an unresolved token. A leftover
   * placeholder would otherwise reach the SpEL parser, where {@code $} is an identifier and
   * {@code {my.ttl}} an inline list, producing a syntax error that names neither the
   * placeholder nor the missing bean factory.
   */
  private @Nullable String resolveTtlPlaceholders(String role, @Nullable String raw, Method method) {
    String resolved = resolvePlaceholders(raw);
    if (resolved != null && resolved.contains("${")) {
      throw new IllegalStateException(
        role +
          " placeholder was not resolved ('" +
          resolved +
          "'): configure a ConfigurableBeanFactory or replace the placeholder, for " +
          method
      );
    }
    return resolved;
  }

  /** Compile the key expression and classify a simple single-argument form. */
  private KeyPlan keyOf(
    Method method,
    String@Nullable [] parameterNames,
    @Nullable String raw,
    boolean resultAllowed,
    String namespace
  ) {
    CompiledExpression compiled = CompiledExpression.of(
      raw,
      parser,
      "cache key",
      method,
      parameterNames,
      resultAllowed
    );
    return KeyPlan.of(namespace, method, compiled, parameterNames);
  }

  /** Compile {@code unless}; it is always evaluated after the body, so {@code #result} is available. */
  private CompiledExpression unlessOf(Method method, String@Nullable [] parameterNames, String raw) {
    return CompiledExpression.of(raw, parser, "unless", method, parameterNames, true);
  }

  /**
   * Resolve {@code ${...}} in the cache name, then validate and encode the namespace
   * delimiter. A blank or NUL-bearing name is a configuration error.
   */
  private String namespace(Method method, @Nullable String rawCache) {
    String cache = resolvePlaceholders(rawCache);
    if (cache == null || cache.isBlank()) {
      throw new IllegalStateException("cache name must not be blank: " + method);
    }
    if (cache.contains("${")) {
      throw new IllegalStateException(
        "cache name placeholder was not resolved ('" +
          cache +
          "'): configure a ConfigurableBeanFactory or " +
          "replace the placeholder, for " +
          method
      );
    }
    if (cache.indexOf(NAMESPACE_DELIMITER) >= 0) {
      throw new IllegalStateException("cache name must not contain U+0000: " + method);
    }
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
   * Claim reflective access once, at resolution, instead of on every background refresh. The
   * call is unconditional: public visibility is not enough under JPMS when the package is
   * opened but not exported, and an accessible flag also lets {@link Method#invoke} skip the
   * per-call caller-sensitive access check. A target that cannot be opened is not fatal: the
   * first refresh reports the {@link IllegalAccessException} in its own failure path.
   */
  private static void prewarmAccess(Method method) {
    method.trySetAccessible();
  }

  /** The one verb a method group declares, resolved with its declaring view. */
  private record ResolvedVerb(Method view, Verb verb) {}

  /** The three cache verbs as a closed set; the declaration site is the annotation carrier. */
  private sealed interface Verb permits Verb.Read, Verb.Write, Verb.Invalidate {
    Kind kind();

    String role();

    record Read(ZetaCached annotation) implements Verb {
      @Override
      public Kind kind() {
        return Kind.READ;
      }

      @Override
      public String role() {
        return ZETA_CACHED;
      }
    }

    record Write(ZetaPut annotation) implements Verb {
      @Override
      public Kind kind() {
        return Kind.WRITE;
      }

      @Override
      public String role() {
        return ZETA_PUT;
      }
    }

    record Invalidate(ZetaInvalidate annotation) implements Verb {
      @Override
      public Kind kind() {
        return Kind.INVALIDATE;
      }

      @Override
      public String role() {
        return ZETA_INVALIDATE;
      }
    }

    enum Kind {
      READ,
      WRITE,
      INVALIDATE,
    }
  }
}
