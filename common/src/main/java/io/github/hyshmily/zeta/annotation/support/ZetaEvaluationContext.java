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
package io.github.hyshmily.zeta.annotation.support;

import io.github.hyshmily.zeta.util.Internal;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.springframework.expression.*;
import org.springframework.expression.spel.support.*;
import org.springframework.util.ClassUtils;

/**
 * The one SpEL context every annotation expression is evaluated in: the cache
 * key, {@code unless}, and both TTL overrides.
 *
 * <p><b>Variables resolve against the invocation.</b> {@code #root} and
 * {@code #this} are answered by SpEL from the root object and never reach this
 * context's lookup. {@code #result} is answered first, then a parameter slot from
 * the plan's {@link ParameterBindings}. An argument reference is read straight
 * from the argument array using the table built once at plan resolution: the
 * lookup is O(1) in the number of references, allocates nothing, performs no
 * reflection and writes no map entry. The context is read-only; an expression
 * assignment or increment/decrement is rejected when the expression is compiled.
 *
 * <p><b>Root object.</b> {@code #root} is a {@link Root}, not the target bean:
 * rooting at the bean would silently break every property below, and an
 * expression that fails there resolves a different key (or throws) than the one
 * the caller wrote. The root exposes exactly the properties callers rely on:
 *
 * <ul>
 *   <li>{@code #root.methodName} — the intercepted method's name;</li>
 *   <li>{@code #root.args} — a defensive copy of the invocation arguments;</li>
 *   <li>{@code #root.method} — the group's most-specific method;</li>
 *   <li>{@code #root.target} — the bean the invocation was made on;</li>
 *   <li>{@code #root.targetClass} — the class the invocation was resolved against.</li>
 * </ul>
 *
 * <p><b>Accessor names are load-bearing.</b> SpEL resolves {@code #root.x} to
 * {@code getX()}. {@code targetClass} and {@code method} use Lombok-generated
 * getters; {@code target}, {@code methodName} and {@code args} are written out
 * because the first carries the nullable contract, one derives from
 * {@code method} and the last returns a defensive copy. A missing or misnamed
 * accessor fails only at evaluation time, on the one expression that uses it.
 *
 * <p><b>One context per foreground call, reusable across its phases.</b> A context
 * is confined to the thread that builds it. The result-independent key evaluates
 * without a bound result; a post-body phase binds the body's value with
 * {@link #bindResult}, and a bound {@code null} is still a bound result. A write
 * that already built the pre-body context binds the result on it instead of
 * allocating a second context, and a read loader caches the invocation thread's
 * context until {@link #release()} clears it, so its key, TTL suppliers and
 * {@code unless} share one instance; the phases never overlap. Nothing is shared
 * with a refresh thread or a call that already returned, so a TTL supplier or an
 * {@code unless} evaluated by the engine can never observe another evaluation's
 * state.
 *
 * <p><b>Reserved bindings cannot be shadowed.</b> {@code root}, {@code this} and
 * {@code result} resolve ahead of the argument table, so a method parameter with
 * one of those names never shadows the binding and stays legal. Canonical
 * {@code pN}/{@code aN} are generated from the slot table, so a parameter with
 * one of those names is rejected when the expression is compiled.
 *
 * <p><b>Reflection caches are shared within one interceptor.</b> A standard
 * context creates a fresh reflective accessor and resolver set per instance, so a
 * context that lives for one phase never warms them. The interceptor owns one
 * {@link ExpressionSupport} instance and every context it creates returns that support's
 * thread-safe set: the property accessor's reader/writer/type-descriptor caches
 * survive the phase that first resolved a property, and the type locator,
 * converter, comparator and operator overloader are shared for the same reason.
 * The support uses the bean factory's classloader when one is available (the
 * default classloader otherwise), so its type cache is released with the Spring
 * context instead of pinning application classes from a static field. It
 * implements {@link EvaluationContext} directly and stores no per-expression
 * variable state.
 */
@Internal
public final class ZetaEvaluationContext implements EvaluationContext {

  /** Distinguishes an unbound {@code #result} from a result that is bound to {@code null}. */
  private static final Object UNBOUND = new Object();

  /**
   * The reflection machinery shared by every evaluation phase of one interceptor.
   *
   * <p>The types it creates hold class/member caches. Keeping them per interceptor
   * ties their lifetime to the Spring context, so a DevTools restart or a second
   * context can release its application classes.
   */
  public static final class ExpressionSupport {

    private final List<PropertyAccessor> propertyAccessors;
    private final List<MethodResolver> methodResolvers;
    private final List<ConstructorResolver> constructorResolvers;
    private final TypeLocator typeLocator;
    private final TypeConverter typeConverter;
    private final TypeComparator typeComparator;
    private final OperatorOverloader operatorOverloader;

    /**
     * Build one support set for the given classloader.
     *
     * @param classLoader the loader used by {@code T(...)} type resolution
     */
    public ExpressionSupport(ClassLoader classLoader) {
      this.propertyAccessors = List.of(new ReflectivePropertyAccessor(false));
      this.methodResolvers = List.of(new ReflectiveMethodResolver());
      this.constructorResolvers = List.of(new ReflectiveConstructorResolver());
      this.typeLocator = new StandardTypeLocator(Objects.requireNonNull(classLoader, "classLoader"));
      this.typeConverter = new StandardTypeConverter();
      this.typeComparator = new StandardTypeComparator();
      this.operatorOverloader = new StandardOperatorOverloader();
    }

    /**
     * A support set over Spring's default classloader.
     *
     * @return a new support set
     */
    public static ExpressionSupport defaults() {
      return new ExpressionSupport(ClassUtils.getDefaultClassLoader());
    }
  }

  private final ParameterBindings bindings;

  private final Root root;

  private final TypedValue rootValue;

  private final @Nullable BeanResolver beanResolver;

  private final ExpressionSupport support;

  private @Nullable Object result = UNBOUND;

  private ZetaEvaluationContext(
    Root root,
    ParameterBindings bindings,
    @Nullable BeanResolver beanResolver,
    ExpressionSupport support
  ) {
    this.root = root;
    this.rootValue = new TypedValue(root);
    this.bindings = bindings;
    this.beanResolver = beanResolver;
    this.support = support;
  }

  @Override
  @NonNull
  public TypedValue getRootObject() {
    return rootValue;
  }

  @Override
  @NonNull
  public List<PropertyAccessor> getPropertyAccessors() {
    return support.propertyAccessors;
  }

  @Override
  @NonNull
  public List<ConstructorResolver> getConstructorResolvers() {
    return support.constructorResolvers;
  }

  @Override
  @NonNull
  public List<MethodResolver> getMethodResolvers() {
    return support.methodResolvers;
  }

  @Override
  @Nullable
  public BeanResolver getBeanResolver() {
    return beanResolver;
  }

  @Override
  @NonNull
  public TypeLocator getTypeLocator() {
    return support.typeLocator;
  }

  @Override
  @NonNull
  public TypeConverter getTypeConverter() {
    return support.typeConverter;
  }

  @Override
  @NonNull
  public TypeComparator getTypeComparator() {
    return support.typeComparator;
  }

  @Override
  @NonNull
  public OperatorOverloader getOperatorOverloader() {
    return support.operatorOverloader;
  }

  /**
   * Build the context for one evaluation phase.
   *
   * @param target      the unwrapped target (may be {@code null})
   * @param targetClass the resolved class
   * @param method      the group's most-specific method
   * @param args        the invocation arguments
   * @param bindings    the plan's parameter slot table
   * @param beanResolver the resolver backing {@code @beanName}, or {@code null}
   * @param support     the reflection machinery shared by the interceptor
   * @return a fresh context, confined to the calling thread
   */
  public static ZetaEvaluationContext create(
    @Nullable Object target,
    Class<?> targetClass,
    Method method,
    Object[] args,
    ParameterBindings bindings,
    @Nullable BeanResolver beanResolver,
    ExpressionSupport support
  ) {
    return new ZetaEvaluationContext(
      new Root(target, targetClass, method, args),
      bindings,
      beanResolver,
      Objects.requireNonNull(support, "support")
    );
  }

  /**
   * Bind {@code #result} for this evaluation. The context must not be shared
   * across threads or evaluated concurrently. A write may bind the result on the
   * context it already built before the body returned: the phases never overlap.
   *
   * @param result the method result (may be {@code null})
   * @return {@code this}, for chaining
   */
  public ZetaEvaluationContext bindResult(@Nullable Object result) {
    this.result = result;
    return this;
  }

  /**
   * Reject a write: the context is read-only, and an expression assignment or
   * increment/decrement is rejected when the expression is compiled.
   */
  @Override
  public void setVariable(@NonNull String name, @Nullable Object value) {
    throw new UnsupportedOperationException("cache expressions are read-only: cannot assign #" + name);
  }

  /**
   * Resolve {@code #result} or an invocation argument. Called by the interpreted
   * evaluator and by compiled SpEL bytecode alike.
   */
  @Override
  public @Nullable Object lookupVariable(@NonNull String name) {
    if (result != UNBOUND && "result".equals(name)) {
      return result;
    }
    return bindings.resolve(name, root.args);
  }

  /** @return the root object (exposed for diagnostics and tests) */
  public Root root() {
    return root;
  }

  /**
   * The {@code #root} object. Every property is served by a public accessor
   * named after it: SpEL looks up {@code get<Property>}, so an accessor missing
   * here fails only when the single expression that uses it is evaluated.
   */
  public static final class Root {

    private final @Nullable Object target;

    @Getter
    private final Class<?> targetClass;

    @Getter
    private final Method method;

    private final Object[] args;

    private Root(@Nullable Object target, Class<?> targetClass, Method method, Object[] args) {
      this.target = target;
      this.targetClass = targetClass;
      this.method = method;
      this.args = args;
    }

    /** @return the unwrapped target, or {@code null} when the invocation has none */
    public @Nullable Object getTarget() {
      return target;
    }

    /** @return the method name ({@code #root.methodName}) */
    @SuppressWarnings("unused")
    public String getMethodName() {
      return method.getName();
    }

    /**
     * @return a copy of the invocation arguments ({@code #root.args}), so an
     *         expression cannot mutate the call it evaluates; an empty argument
     *         array is immutable and is returned as-is
     */
    public Object[] getArgs() {
      return args.length == 0 ? args : args.clone();
    }

    @Override
    public String toString() {
      return "Root{" + method + "}";
    }
  }
}
