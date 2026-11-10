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
package io.github.hyshmily.zeta.annotation.supporter;

import io.github.hyshmily.zeta.util.Internal;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.*;
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
 * context's lookup. {@code #result} is answered first, then {@code #pN},
 * {@code #aN} or a parameter name. An argument reference is read straight from
 * the argument array using the parameter names discovered once at plan
 * resolution: the lookup is O(1) in the number of references, allocates nothing,
 * performs no reflection and writes no map entry. The context is read-only;
 * an expression assignment is rejected when the expression is compiled.
 *
 * <p><b>Root object.</b> {@code #root} is a {@link Root}, not the target bean:
 * rooting at the bean would silently break every property below, and an
 * expression that fails there resolves a different key (or throws) than the one
 * the caller wrote. The root exposes exactly the properties callers rely on:
 *
 * <ul>
 *   <li>{@code #root.methodName} — the intercepted method's name;</li>
 *   <li>{@code #root.args} — a defensive copy of the invocation arguments;</li>
 *   <li>{@code #root.method} — the most-specific implementation method;</li>
 *   <li>{@code #root.target} — the bean the invocation was made on;</li>
 *   <li>{@code #root.targetClass} — the class the invocation was resolved against.</li>
 * </ul>
 *
 * <p><b>Accessor names are load-bearing.</b> SpEL resolves {@code #root.x} to
 * {@code getX()}. {@code target}, {@code targetClass} and {@code method} use
 * Lombok-generated getters; {@code methodName} and {@code args} are written out
 * because one derives from {@code method} and the other returns a defensive
 * copy. A missing or misnamed accessor fails only at evaluation time, on the
 * one expression that uses it.
 *
 * <p><b>One context per evaluation phase.</b> A context is confined to the
 * thread that builds it and is discarded when that phase ends. The
 * result-independent key evaluates without a bound result; a post-body phase
 * binds the body's value with {@link #withResult}, and a bound {@code null} is
 * still a bound result. Nothing is shared with a later phase, a refresh thread,
 * or a call that already returned, so a TTL supplier or an {@code unless}
 * evaluated by the engine can never observe another evaluation's state.
 *
 * <p><b>Reserved bindings cannot be shadowed.</b> {@code root}, {@code this},
 * {@code result} and canonical {@code pN}/{@code aN} are reserved; a method
 * parameter with one of those names is rejected when the expression is
 * compiled, so parameter resolution cannot overwrite the body result bound by
 * {@link #withResult}.
 *
 * <p><b>Reflection caches are shared within one interceptor.</b> A standard
 * context creates a fresh reflective accessor and resolver set per instance, so a
 * context that lives for one phase never warms them. The interceptor owns one
 * {@link Support} instance and every context it creates returns that support's
 * thread-safe set: the property accessor's reader/writer/type-descriptor caches
 * survive the phase that first resolved a property, and the type locator,
 * converter, comparator and operator overloader are shared for the same reason.
 * The support uses the interceptor's classloader, so its type cache is released
 * with the Spring context instead of pinning application classes from a static
 * field. It implements {@link EvaluationContext} directly and stores no
 * per-expression variable state.
 */
@Internal
public final class ZetaEvaluationContext implements EvaluationContext {

  /**
   * The reflection machinery shared by every evaluation phase of one interceptor.
   *
   * <p>The types it creates hold class/member caches. Keeping them per interceptor
   * ties their lifetime to the Spring context, so a DevTools restart or a second
   * context can release its application classes.
   */
  public static final class Support {

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
    public Support(ClassLoader classLoader) {
      this.propertyAccessors = List.of(new ReflectivePropertyAccessor());
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
    public static Support defaults() {
      return new Support(ClassUtils.getDefaultClassLoader());
    }
  }

  private final String@Nullable [] parameterNames;

  private final Root root;

  private final TypedValue rootValue;

  private final @Nullable BeanResolver beanResolver;

  private final Support support;

  private boolean hasResult;

  private @Nullable Object result;

  private ZetaEvaluationContext(
    Root root,
    String@Nullable [] parameterNames,
    @Nullable BeanResolver beanResolver,
    Support support
  ) {
    this.root = root;
    this.rootValue = new TypedValue(root);
    this.parameterNames = parameterNames;
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
   * <p>{@code method} must be the most-specific implementation method, not the
   * interface {@link Method} a JDK proxy reports: the parameter names were
   * discovered from that method, and an interface abstract method may carry no
   * parameter-name metadata. An expression bound against the interface method
   * would then leave {@code #name} unresolved while the implementation
   * method's parameters were available.
   *
   * @param root           the invocation root
   * @param parameterNames the method's parameter names, or {@code null}
   * @param beanResolver   the resolver backing {@code @beanName}, or {@code null}
   * @param support        the reflection machinery shared by the interceptor
   * @return a fresh context, confined to the calling thread
   */
  public static ZetaEvaluationContext of(
    Root root,
    String@Nullable [] parameterNames,
    @Nullable BeanResolver beanResolver,
    Support support
  ) {
    return new ZetaEvaluationContext(root, parameterNames, beanResolver, Objects.requireNonNull(support, "support"));
  }

  /**
   * Build a context over a fresh default support set. Use the four-argument form
   * when the interceptor already owns a support instance.
   *
   * @param root           the invocation root
   * @param parameterNames the method's parameter names, or {@code null}
   * @param beanResolver   the resolver backing {@code @beanName}, or {@code null}
   * @return a fresh context, confined to the calling thread
   */
  public static ZetaEvaluationContext of(
    Root root,
    String@Nullable [] parameterNames,
    @Nullable BeanResolver beanResolver
  ) {
    return of(root, parameterNames, beanResolver, Support.defaults());
  }

  /**
   * Bind {@code #result} for this evaluation. The context must be created for
   * the phase that produced the result: it is never shared with a pre-body
   * evaluation or another thread.
   *
   * @param result the method result (may be {@code null})
   * @return {@code this}, for chaining
   */
  public ZetaEvaluationContext withResult(@Nullable Object result) {
    this.hasResult = true;
    this.result = result;
    return this;
  }

  /**
   * Reject a write: the context is read-only, and an expression assignment is
   * rejected when the expression is compiled.
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
    if (hasResult && "result".equals(name)) {
      return result;
    }
    Object[] arguments = root.args;
    int index = CompiledExpression.argumentIndex(name, parameterNames);
    if (index < 0) {
      return null;
    }
    Method method = root.method;
    int parameterCount = method.getParameterCount();
    if (method.isVarArgs() && index == parameterCount - 1) {
      return varargsValue(method, arguments, index);
    }
    return index < arguments.length ? arguments[index] : null;
  }

  /**
   * Adapts an invocation argument array to the packed shape {@link Method#invoke} requires. A
   * varargs tail may arrive packed (the last element is already an array), expanded (several
   * elements after the fixed parameters), or as one scalar value; the packed array always has
   * exactly {@code parameterCount} elements with the final element a component-typed array.
   *
   * @param method the target method
   * @param args   the invocation arguments as reported by the proxy
   * @return the original array when already packed, otherwise a packed copy
   */
  public static Object[] packArguments(Method method, Object[] args) {
    if (!method.isVarArgs() || method.getParameterCount() == 0) {
      return args;
    }
    int last = method.getParameterCount() - 1;
    Class<?> arrayType = method.getParameterTypes()[last];
    if (args.length == method.getParameterCount() && (args[last] == null || arrayType.isInstance(args[last]))) {
      return args;
    }
    Object[] packed = Arrays.copyOf(args, method.getParameterCount());
    packed[last] = varargsValue(method, args, last);
    return packed;
  }

  /**
   * The varargs tail at {@code index} as a component-typed array. A packed tail is returned
   * unchanged; an expanded tail (including the one-vararg form, whose length equals the method's
   * parameter count) is copied into a new array of the declared component type.
   */
  private static Object varargsValue(Method method, Object[] args, int index) {
    Class<?> arrayType = method.getParameterTypes()[index];
    if (args.length == method.getParameterCount() && (args[index] == null || arrayType.isInstance(args[index]))) {
      return args[index];
    }
    Class<?> componentType = arrayType.getComponentType();
    int expanded = Math.max(0, args.length - index);
    Object varargs = Array.newInstance(componentType, expanded);
    for (int i = 0; i < expanded; i++) {
      Array.set(varargs, i, args[index + i]);
    }
    return varargs;
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

    @Getter
    private final Object target;

    @Getter
    private final Class<?> targetClass;

    @Getter
    private final Method method;

    private final Object[] args;

    private Root(Object target, Class<?> targetClass, Method method, Object[] args) {
      this.target = target;
      this.targetClass = targetClass;
      this.method = method;
      this.args = args;
    }

    /**
     * Wrap one call's immutable invocation inputs.
     *
     * @param target      the unwrapped target (may be {@code null})
     * @param targetClass the resolved class
     * @param method      the most-specific implementation method
     * @param args        the invocation arguments
     * @return the root for this call
     */
    public static Root of(@Nullable Object target, Class<?> targetClass, Method method, Object[] args) {
      return new Root(target, targetClass, method, args);
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
