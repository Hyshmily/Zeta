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
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.expression.*;
import org.springframework.expression.spel.support.*;
import org.springframework.util.ClassUtils;

/**
 * The SpEL {@link EvaluationContext} used to evaluate all cache annotation expressions: keys,
 * {@code condition}/{@code unless} guards, and TTL overrides.
 *
 * <p><b>Variable resolution.</b> {@code #result} is checked first when bound, followed by parameter
 * slots from the plan's {@link ParameterBindings}. Argument references read directly from the
 * invocation argument array via the precomputed slot table in O(1) time without allocation. The
 * context is read-only; {@link #setVariable(String, Object)} rejects assignments.
 *
 * <p><b>Root object.</b> {@code #root} is a {@link Root} rather than the target bean, exposing
 * {@code methodName}, {@code args}, {@code method}, {@code target}, and {@code targetClass}.
 * {@link Root#getArgs()} returns a defensive copy so expressions cannot mutate invocation state.
 *
 * <p><b>One context per phase.</b> Instances are immutable: the pre-body phase uses a result-free
 * context created via {@link #create}, and {@link #withResult(Object)} derives a post-body context
 * sharing the same {@link Root}, {@link TypedValue}, and {@link ExpressionSupport}. Each context is
 * confined to the thread that creates it.
 *
 * <p><b>Shared reflection caches.</b> Rather than creating fresh reflective accessors and resolvers
 * per evaluation phase, each interceptor owns one {@link ExpressionSupport} instance whose
 * thread-safe caches live for the lifetime of the Spring context, allowing DevTools restarts to
 * release application classes cleanly.
 */
@Internal
public final class ZetaEvaluationContext implements EvaluationContext {

  private static final TypeDescriptor ROOT_TYPE = TypeDescriptor.valueOf(Root.class);
  private static final Object NO_RESULT = new Object();

  private final Root root;
  private final TypedValue rootValue;
  private final ParameterBindings bindings;
  private final @Nullable BeanResolver beanResolver;
  private final ExpressionSupport support;

  /** The bound {@code #result} value, or {@link #NO_RESULT} during a pre-body phase. */
  private final @Nullable Object result;

  /**
   * Creates an immutable evaluation context for one invocation phase.
   *
   * @param root         the {@code #root} metadata object
   * @param rootValue    the pre-wrapped {@link TypedValue} for {@code root}
   * @param bindings     the plan's parameter slot table
   * @param beanResolver the resolver backing {@code @beanName}, or {@code null}
   * @param support      the reflection machinery shared by the interceptor
   * @param result       the bound {@code #result}, or {@link #NO_RESULT} when unbound
   */
  private ZetaEvaluationContext(
    Root root,
    TypedValue rootValue,
    ParameterBindings bindings,
    @Nullable BeanResolver beanResolver,
    ExpressionSupport support,
    @Nullable Object result
  ) {
    this.root = root;
    this.rootValue = rootValue;
    this.bindings = bindings;
    this.beanResolver = beanResolver;
    this.support = support;
    this.result = result;
  }

  /**
   * Creates a result-free evaluation context for a pre-body phase.
   *
   * @param target       the unwrapped target instance, or {@code null} if unavailable
   * @param targetClass  the resolved target class
   * @param method       the group's most-specific method
   * @param args         the invocation arguments
   * @param bindings     the plan's parameter slot table
   * @param beanResolver the resolver backing {@code @beanName}, or {@code null}
   * @param support      the reflection machinery shared by the interceptor
   * @return a fresh pre-body evaluation context
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
    Root root = new Root(
      target,
      Objects.requireNonNull(targetClass, "targetClass must not be null"),
      Objects.requireNonNull(method, "method must not be null"),
      Objects.requireNonNull(args, "args must not be null")
    );
    return new ZetaEvaluationContext(
      root,
      new TypedValue(root, ROOT_TYPE),
      Objects.requireNonNull(bindings, "bindings must not be null"),
      beanResolver,
      Objects.requireNonNull(support, "support must not be null"),
      NO_RESULT
    );
  }

  /**
   * Returns a derived context with {@code #result} bound to {@code result}, leaving this context
   * unchanged and reusing its {@link Root}, {@link TypedValue}, and {@link ExpressionSupport}.
   *
   * @param result the method return value, which may be {@code null}
   * @return a post-body evaluation context with {@code #result} bound
   */
  public ZetaEvaluationContext withResult(@Nullable Object result) {
    return new ZetaEvaluationContext(root, rootValue, bindings, beanResolver, support, result);
  }

  /**
   * Returns the {@link Root} object backing {@code #root} for diagnostics and testing.
   *
   * @return the root metadata object
   */
  public Root root() {
    return root;
  }

  /**
   * Returns the {@link TypedValue} wrapping {@link #root()}.
   *
   * @return the typed root object
   */
  @Override
  public @NonNull TypedValue getRootObject() {
    return rootValue;
  }

  /**
   * Returns the shared read-only property accessors.
   *
   * @return the list of property accessors
   */
  @Override
  public @NonNull List<PropertyAccessor> getPropertyAccessors() {
    return support.propertyAccessors;
  }

  /**
   * Returns the shared constructor resolvers.
   *
   * @return the list of constructor resolvers
   */
  @Override
  public @NonNull List<ConstructorResolver> getConstructorResolvers() {
    return support.constructorResolvers;
  }

  /**
   * Returns the shared method resolvers.
   *
   * @return the list of method resolvers
   */
  @Override
  public @NonNull List<MethodResolver> getMethodResolvers() {
    return support.methodResolvers;
  }

  /**
   * Returns the configured bean resolver for {@code @beanName} lookups.
   *
   * @return the bean resolver, or {@code null} if none is configured
   */
  @Override
  public @Nullable BeanResolver getBeanResolver() {
    return beanResolver;
  }

  /**
   * Returns the shared type locator used for {@code T(...)} expressions.
   *
   * @return the type locator
   */
  @Override
  public @NonNull TypeLocator getTypeLocator() {
    return support.typeLocator;
  }

  /**
   * Returns the shared type converter.
   *
   * @return the type converter
   */
  @Override
  public @NonNull TypeConverter getTypeConverter() {
    return support.typeConverter;
  }

  /**
   * Returns the shared type comparator.
   *
   * @return the type comparator
   */
  @Override
  public @NonNull TypeComparator getTypeComparator() {
    return support.typeComparator;
  }

  /**
   * Returns the shared operator overloader.
   *
   * @return the operator overloader
   */
  @Override
  public @NonNull OperatorOverloader getOperatorOverloader() {
    return support.operatorOverloader;
  }

  /**
   * Rejects variable assignment because cache expressions are pure functions of the invocation.
   *
   * @param name  the variable name
   * @param value the attempted value
   * @throws UnsupportedOperationException always
   */
  @Override
  public void setVariable(@NonNull String name, @Nullable Object value) {
    throw new UnsupportedOperationException("cache expressions are read-only: cannot assign #" + name);
  }

  /**
   * Disables SpEL assignment and increment/decrement operators before they evaluate a right-hand
   * side, so an indexer write ({@code #args[0] = ...}) cannot mutate a caller argument in place.
   *
   * @return {@code false}: cache expressions are read-only
   */
  @Override
  public boolean isAssignmentEnabled() {
    return false;
  }

  /**
   * Resolves {@code #result} (when bound) or a declared invocation parameter by name or positional
   * alias.
   *
   * @param name the variable name without the leading {@code '#'}
   * @return the resolved value, or {@code null} if unbound or if the bound value is {@code null}
   */
  @Override
  public @Nullable Object lookupVariable(@NonNull String name) {
    if (result != NO_RESULT && "result".equals(name)) {
      return result;
    }
    return bindings.resolve(name, root.args);
  }

  /**
   * Shared reflection and type-handling machinery reused across every evaluation phase of one
   * interceptor.
   *
   * <p>The components held here maintain internal class and member caches. Scoping one instance per
   * interceptor ties their lifetime to the Spring context so DevTools restarts or multiple contexts
   * can unload application classes cleanly.
   */
  public static final class ExpressionSupport {

    private final List<PropertyAccessor> propertyAccessors;
    private final List<MethodResolver> methodResolvers;
    private final List<ConstructorResolver> constructorResolvers;
    private final TypeLocator typeLocator;
    private final TypeConverter typeConverter;
    private final TypeComparator typeComparator;
    private final OperatorOverloader operatorOverloader;
    private final EvaluationContext emptyContext;

    /**
     * Builds a shared expression support set backed by the given class loader.
     *
     * @param classLoader the class loader used by {@code T(...)} type resolution
     */
    public ExpressionSupport(ClassLoader classLoader) {
      ClassLoader loader = Objects.requireNonNull(classLoader, "classLoader must not be null");
      this.propertyAccessors = List.of(new ReflectivePropertyAccessor(false));
      this.methodResolvers = List.of(new ReflectiveMethodResolver());
      this.constructorResolvers = List.of(new ReflectiveConstructorResolver());
      this.typeLocator = new StandardTypeLocator(loader);
      this.typeConverter = new StandardTypeConverter();
      this.typeComparator = new StandardTypeComparator();
      this.operatorOverloader = new StandardOperatorOverloader();
      this.emptyContext = new EmptyEvaluationContext(this);
    }

    /**
     * Returns the immutable read-only context used when an expression reads no invocation state.
     *
     * <p>It carries this support's own accessors, resolvers, converters and class loader, so a
     * context-free expression resolves {@code T(...)} against the bean factory's loader and cannot
     * write a property or a variable.
     *
     * @return the shared context scoped to this support instance
     */
    public EvaluationContext emptyContext() {
      return emptyContext;
    }

    /**
     * Builds a shared expression support set using Spring's default class loader.
     *
     * @return a default {@link ExpressionSupport} instance
     */
    public static ExpressionSupport defaults() {
      ClassLoader classLoader = ClassUtils.getDefaultClassLoader();
      if (classLoader == null) {
        classLoader = ExpressionSupport.class.getClassLoader();
      }
      if (classLoader == null) {
        classLoader = ClassLoader.getSystemClassLoader();
      }
      return new ExpressionSupport(classLoader);
    }

    /**
     * The context-free phase context: it exposes this support's components and no root, variable or
     * bean state. Every mutator rejects, so a context-free expression cannot write.
     */
    private record EmptyEvaluationContext(ExpressionSupport support) implements EvaluationContext {
      @Override
      public @NonNull TypedValue getRootObject() {
        return TypedValue.NULL;
      }

      @Override
      public @NonNull List<PropertyAccessor> getPropertyAccessors() {
        return support.propertyAccessors;
      }

      @Override
      public @NonNull List<ConstructorResolver> getConstructorResolvers() {
        return support.constructorResolvers;
      }

      @Override
      public @NonNull List<MethodResolver> getMethodResolvers() {
        return support.methodResolvers;
      }

      @Override
      public @Nullable BeanResolver getBeanResolver() {
        return null;
      }

      @Override
      public @NonNull TypeLocator getTypeLocator() {
        return support.typeLocator;
      }

      @Override
      public @NonNull TypeConverter getTypeConverter() {
        return support.typeConverter;
      }

      @Override
      public @NonNull TypeComparator getTypeComparator() {
        return support.typeComparator;
      }

      @Override
      public @NonNull OperatorOverloader getOperatorOverloader() {
        return support.operatorOverloader;
      }

      @Override
      public void setVariable(@NonNull String name, @Nullable Object value) {
        throw new UnsupportedOperationException("cache expressions are read-only: cannot assign #" + name);
      }

      @Override
      public boolean isAssignmentEnabled() {
        return false;
      }

      @Override
      public @Nullable Object lookupVariable(@NonNull String name) {
        return null;
      }
    }
  }

  /**
   * The {@code #root} object exposed to SpEL expressions.
   *
   * <p>Every property is served by a public JavaBean getter matching its name ({@code target},
   * {@code targetClass}, {@code method}, {@code methodName}, {@code args}) for reflective SpEL
   * access.
   */
  public static final class Root {

    private final @Nullable Object target;

    /**
     * -- GETTER --
     *  Returns the resolved target class (
     * ).
     *
     * @return the target class
     */
    @Getter
    private final Class<?> targetClass;

    /**
     * -- GETTER --
     *  Returns the invoked method (
     * ).
     *
     * @return the method metadata
     */
    @Getter
    private final Method method;

    private final Object[] args;

    /**
     * Creates a root metadata object for one method invocation.
     *
     * @param target      the unwrapped target instance, or {@code null}
     * @param targetClass the resolved target class
     * @param method      the invoked method
     * @param args        the invocation argument array
     */
    private Root(@Nullable Object target, Class<?> targetClass, Method method, Object[] args) {
      this.target = target;
      this.targetClass = targetClass;
      this.method = method;
      this.args = args;
    }

    /**
     * Returns the unwrapped invocation target ({@code #root.target}).
     *
     * @return the target instance, or {@code null} when the invocation has no target
     */
    public @Nullable Object getTarget() {
      return target;
    }

    /**
     * Returns the invoked method's simple name ({@code #root.methodName}).
     *
     * @return the method name
     */
    @SuppressWarnings("unused")
    public String getMethodName() {
      return method.getName();
    }

    /**
     * Returns a defensive copy of the invocation arguments ({@code #root.args}); an empty argument
     * array is immutable and returned as-is.
     *
     * @return a copy of the invocation arguments
     */
    public Object[] getArgs() {
      return args.length == 0 ? args : args.clone();
    }

    /**
     * Returns a diagnostic string identifying this root's method.
     *
     * @return a string representation of this root
     */
    @Override
    public String toString() {
      return "Root{" + method + "}";
    }
  }
}
