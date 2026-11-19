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

import io.github.hyshmily.zeta.annotation.support.InvocationArguments;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.jspecify.annotations.Nullable;
import org.springframework.core.BridgeMethodResolver;

/**
 * Resolves the reflective method a target can invoke, including the erased declaration a generic
 * supertype or interface exposes.
 *
 * <p>The walk is reflection-heavy and runs on proxied invocations and background refreshes, so the
 * per-target-class answer is cached: {@link #canInvoke} remembers one boolean per reported method,
 * and {@link #invocable} remembers the method already selected and made accessible. The caches are
 * owned by this resolver, so they are released with the Spring context; a {@code ClassValue} on the
 * target class could retain a child-loader method inside a parent-loader framework class and pin
 * that child loader across a context restart.
 */
final class TargetMethodResolver {

  private final Map<Class<?>, Map<Method, Boolean>> invokeTable = new ConcurrentHashMap<>();

  private final Map<Class<?>, Map<Object, Method>> invocableTable = new ConcurrentHashMap<>();

  /**
   * Whether {@code target} can invoke {@code method} directly or through an assignable erasure on
   * a supertype it implements. Plan resolution keeps such a receiver as the refresh target; only
   * one that cannot invoke the method is replaced by the proxy exposing the declaring view.
   *
   * @param target the unwrapped receiver, or {@code null}
   * @param method the reported method
   * @return {@code true} when the target exposes the method or an erased declaration
   */
  boolean canInvoke(@Nullable Object target, Method method) {
    if (target == null) {
      return false;
    }
    if (method.getDeclaringClass().isInstance(target)) {
      return true;
    }
    return invokeTable
      .computeIfAbsent(target.getClass(), ignored -> new ConcurrentHashMap<>())
      .computeIfAbsent(method, ignored -> canInvokeUncached(target, method));
  }

  private static boolean canInvokeUncached(Object target, Method method) {
    Method invocable = resolveCallableErasedMethod(
      method.getDeclaringClass(),
      target.getClass(),
      method.getName(),
      method.getParameterTypes()
    );
    return invocable != null;
  }

  /**
   * The method a refresh invokes on {@code target}. The plan's precomputed bridge is a signature
   * carrier: the target method is looked up on the target class, because a plan can be shared by
   * every view of a generic method. An interface never generates a bridge, so a target that
   * implements only a generic parent interface is reached through the common supertype's erased
   * declaration instead. A same-signature static helper on the target is not the body to refresh;
   * neither is a method the target cannot invoke, so both fall through to the erased declaration.
   *
   * <p>Access is claimed on the selected method before it is returned. When the plan's implementation
   * method cannot be opened — a package-private class in a module that does not open its package —
   * the reported interface method is preferred if it is itself accessible: invoking the public
   * interface method still dispatches to the implementation without reflective access to the
   * implementation class.
   *
   * @param specific     the plan's most-specific method, which owns the signature
   * @param erasedBridge the plan's widest generated bridge, or {@code null}
   * @param target       the unwrapped receiver
   * @param reported     the method the invocation reported
   * @return the method to invoke, made accessible for a refresh
   */
  Method invocable(Method specific, @Nullable Method erasedBridge, Object target, Method reported) {
    Object key = reported == specific ? specific : new InvocationKey(specific, reported);
    return invocableTable
      .computeIfAbsent(target.getClass(), ignored -> new ConcurrentHashMap<>())
      .computeIfAbsent(key, ignored -> resolveInvocable(specific, erasedBridge, target, reported));
  }

  private static Method resolveInvocable(
    Method specific,
    @Nullable Method erasedBridge,
    Object target,
    Method reported
  ) {
    Class<?> targetType = target.getClass();
    Method invocable = specific.getDeclaringClass().isInstance(target)
      ? specific
      : reported.getDeclaringClass().isInstance(target)
        ? reported
        : BridgeMethodResolver.getMostSpecificMethod(reported, targetType);
    // A same-signature static helper on the target is not the body to refresh; neither is a method
    // the target cannot invoke. Fall through to the erased declaration in both cases.
    if (!invocable.getDeclaringClass().isInstance(target) || Modifier.isStatic(invocable.getModifiers())) {
      Method erased = resolveErasedOnTarget(targetType, specific, erasedBridge);
      invocable = erased != null ? erased : reported;
    }
    invocable.trySetAccessible();
    if (!invocable.canAccess(target) && reported != invocable) {
      // The reported interface method may live in an open package even when the implementation
      // method's package is closed. Claim its access before asking, so a package-private declaring
      // type in an open package is not rejected on a visibility check it can pass.
      reported.trySetAccessible();
      boolean reportedVisible =
        reported.getDeclaringClass().isInstance(target) &&
        !Modifier.isStatic(reported.getModifiers()) &&
        reported.canAccess(target);
      if (reportedVisible) {
        return reported;
      }
    }
    return invocable;
  }

  /**
   * Resolve the target's implementation of the plan's erased signature: the precomputed bridge
   * first, then an erased declaration on a supertype the target actually implements.
   *
   * @param targetType   the runtime target class
   * @param specific     the plan's most-specific method
   * @param erasedBridge the precomputed widest bridge, or {@code null}
   * @return the method to invoke, or {@code null} when the target exposes neither shape
   */
  private static @Nullable Method resolveErasedOnTarget(
    Class<?> targetType,
    Method specific,
    @Nullable Method erasedBridge
  ) {
    if (
      erasedBridge != null &&
      erasedBridge.getDeclaringClass().isAssignableFrom(targetType) &&
      !Modifier.isStatic(erasedBridge.getModifiers())
    ) {
      // The bridge is already the signature carrier and dispatches virtually to the target's
      // implementation. Looking it up with Class.getMethod would only find a public method, while a
      // protected or package-private bridge is a legal CGLIB target.
      return erasedBridge;
    }
    return resolveCallableErasedMethod(
      specific.getDeclaringClass(),
      targetType,
      specific.getName(),
      specific.getParameterTypes()
    );
  }

  /**
   * The target's implementation of an erased supertype declaration, ready to invoke.
   *
   * @param contractType          the type that declares the specialized signature
   * @param targetType            the runtime target class
   * @param name                  the method name
   * @param specificParameterTypes the plan's specialized parameter types
   * @return the callable method, or {@code null} when no supertype offers one
   */
  private static @Nullable Method resolveCallableErasedMethod(
    Class<?> contractType,
    Class<?> targetType,
    String name,
    Class<?>[] specificParameterTypes
  ) {
    Method erased = findErasedSuperMethod(contractType, targetType, name, specificParameterTypes);
    if (erased == null) {
      return null;
    }
    Method resolved = BridgeMethodResolver.getMostSpecificMethod(erased, targetType);
    return callableOn(resolved, targetType) ? resolved : null;
  }

  /** Invoke a resolved method, unwrapping the reflective exception's cause. */
  static Object invoke(Method invocable, Object target, Object[] args) throws Throwable {
    try {
      return invocable.invoke(target, InvocationArguments.pack(invocable, args));
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      throw cause != null ? cause : e;
    }
  }

  /**
   * Find the most-specific supertype declaration of {@code name} whose parameters are assignable
   * from the plan's specialized parameters. Only a supertype the target implements is searched, so
   * the resolved method is guaranteed to be invocable on the target. A static or private helper
   * that happens to share the signature is not part of the inherited contract and is skipped; an
   * overload set that accepts the specialized types resolves to the most specific declaration, so
   * the answer does not depend on {@link Class#getDeclaredMethods()} order. Each supertype is
   * visited once, so a diamond of repository interfaces is scanned once per resolution rather than
   * once per path.
   */
  private static @Nullable Method findErasedSuperMethod(
    Class<?> type,
    Class<?> targetType,
    String name,
    Class<?>[] specificParameterTypes
  ) {
    return findErasedSuperMethod(type, targetType, name, specificParameterTypes, new HashSet<>());
  }

  private static @Nullable Method findErasedSuperMethod(
    Class<?> type,
    Class<?> targetType,
    String name,
    Class<?>[] specificParameterTypes,
    Set<Class<?>> visited
  ) {
    if (!visited.add(type)) {
      return null;
    }
    Method best = type.isAssignableFrom(targetType)
      ? mostSpecificDeclaration(type, name, specificParameterTypes)
      : null;
    Class<?> superclass = type.getSuperclass();
    if (superclass != null && superclass != Object.class) {
      best = moreSpecificMethod(
        best,
        findErasedSuperMethod(superclass, targetType, name, specificParameterTypes, visited)
      );
    }
    for (Class<?> ifc : type.getInterfaces()) {
      best = moreSpecificMethod(
        best,
        findErasedSuperMethod(ifc, targetType, name, specificParameterTypes, visited)
      );
    }
    return best;
  }

  /**
   * Choose the more specific declaration of one erased contract. A candidate with narrower
   * parameters wins; when the parameters tie, a declaration on a subtype wins over the one it
   * inherits. A genuine tie keeps the current candidate, which is equivalent for dispatch.
   */
  private static @Nullable Method moreSpecificMethod(@Nullable Method current, @Nullable Method candidate) {
    if (candidate == null) {
      return current;
    }
    if (current == null) {
      return candidate;
    }
    if (isMoreSpecific(candidate.getParameterTypes(), current.getParameterTypes())) {
      return candidate;
    }
    if (isMoreSpecific(current.getParameterTypes(), candidate.getParameterTypes())) {
      return current;
    }
    Class<?> currentDeclaring = current.getDeclaringClass();
    Class<?> candidateDeclaring = candidate.getDeclaringClass();
    if (currentDeclaring != candidateDeclaring && currentDeclaring.isAssignableFrom(candidateDeclaring)) {
      return candidate;
    }
    return current;
  }

  /**
   * The most specific non-static, non-private declaration on {@code type} whose parameters accept
   * the specialized types.
   */
  private static @Nullable Method mostSpecificDeclaration(
    Class<?> type,
    String name,
    Class<?>[] specificParameterTypes
  ) {
    Method best = null;
    for (Method candidate : type.getDeclaredMethods()) {
      int modifiers = candidate.getModifiers();
      if (
        Modifier.isStatic(modifiers) ||
        Modifier.isPrivate(modifiers) ||
        !candidate.getName().equals(name) ||
        candidate.getParameterCount() != specificParameterTypes.length ||
        !parametersAssignable(candidate.getParameterTypes(), specificParameterTypes)
      ) {
        continue;
      }
      if (best == null || isMoreSpecific(candidate.getParameterTypes(), best.getParameterTypes())) {
        best = candidate;
      }
    }
    return best;
  }

  /** Whether every declared parameter type accepts the corresponding specialized type. */
  private static boolean parametersAssignable(Class<?>[] erasedTypes, Class<?>[] specificTypes) {
    for (int i = 0; i < erasedTypes.length; i++) {
      if (!erasedTypes[i].isAssignableFrom(specificTypes[i])) {
        return false;
      }
    }
    return true;
  }

  /** Whether {@code candidate} is strictly more specific than {@code current} in every position. */
  static boolean isMoreSpecific(Class<?>[] candidate, Class<?>[] current) {
    boolean specific = false;
    for (int i = 0; i < candidate.length; i++) {
      if (!current[i].isAssignableFrom(candidate[i])) {
        return false;
      }
      specific |= candidate[i] != current[i];
    }
    return specific;
  }

  /**
   * Whether the target exposes the method as a non-static, non-private inherited member. Visibility
   * is claimed separately with {@link Method#trySetAccessible()}, so a protected or package-private
   * implementation in an open package is callable.
   */
  private static boolean callableOn(Method method, Class<?> targetType) {
    int modifiers = method.getModifiers();
    return (
      method.getDeclaringClass().isAssignableFrom(targetType) &&
      !Modifier.isStatic(modifiers) &&
      !Modifier.isPrivate(modifiers)
    );
  }

  /**
   * The plan's specific method plus the reported view when they differ. Two {@link Method}
   * references identify the plan group without hashing the plan and its compiled expressions.
   */
  private record InvocationKey(Method specific, Method reported) {}
}
