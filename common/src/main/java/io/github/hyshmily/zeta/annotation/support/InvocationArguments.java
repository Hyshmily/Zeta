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
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.IdentityHashMap;
import org.jspecify.annotations.Nullable;

/**
 * Adapts an invocation's argument array to the shapes the annotation layer needs: the packed form
 * {@link Method#invoke} requires, the array form a SpEL {@code #pN}/{@code #aN} lookup returns for a
 * varargs tail, and the detached snapshot a retained background-refresh loader reuses.
 *
 * <p>A varargs tail may arrive packed (the last element is already an array), expanded (several
 * elements after the fixed parameters), as one scalar value, or as the {@code Object[]} container a
 * generic interface's erased {@code T...} signature produces. A non-varargs generic array parameter
 * ({@code T[]}) arrives in the same erased container. The pack and tail entry points normalize those
 * shapes to a component-typed array so a {@code String[]} never leaks an {@code Object[]} into a key,
 * an {@code unless} or a reflective invocation.
 */
@Internal
public final class InvocationArguments {

  /**
   * Adapt an invocation argument array to the packed shape {@link Method#invoke} requires: exactly
   * {@code parameterCount} elements, every array parameter copied into its declared component type
   * and, for a varargs method, the final element a component-typed array.
   *
   * <p>An invocation may report fewer arguments than the method declares. A missing fixed slot is
   * left {@code null} by the copy and a missing varargs tail becomes an empty array; the fixed-slot
   * loop never reads past the reported array.
   *
   * @param method the target method
   * @param args   the invocation arguments as reported by the proxy
   * @return the original array when already packed, otherwise a packed copy
   */
  public static Object[] pack(Method method, Object[] args) {
    int count = method.getParameterCount();
    if (count == 0) {
      return args;
    }
    Class<?>[] paramTypes = method.getParameterTypes();
    if (!method.isVarArgs()) {
      return args.length == count
        ? adaptFixed(args, paramTypes, count)
        : adaptFixed(Arrays.copyOf(args, count), paramTypes, Math.min(count, args.length));
    }
    int last = count - 1;
    Class<?> arrayType = paramTypes[last];
    if (args.length == count && (args[last] == null || arrayType.isInstance(args[last]))) {
      return adaptFixed(args, paramTypes, last);
    }
    Object[] packed = Arrays.copyOf(args, count);
    int limit = Math.min(last, args.length);
    for (int i = 0; i < limit; i++) {
      packed[i] = adaptArgument(paramTypes[i], args[i]);
    }
    packed[last] = varargsTail(arrayType, count, args, last);
    return packed;
  }

  /** Adapt every parameter before {@code count}, returning the original array when nothing changed. */
  private static Object[] adaptFixed(Object[] args, Class<?>[] paramTypes, int count) {
    Object[] packed = null;
    for (int i = 0; i < count; i++) {
      Object adapted = adaptArgument(paramTypes[i], args[i]);
      if (adapted != args[i]) {
        if (packed == null) {
          packed = args.clone();
        }
        packed[i] = adapted;
      }
    }
    return packed != null ? packed : args;
  }

  /**
   * The varargs tail at {@code index} as a component-typed array. A packed tail is returned
   * unchanged; an expanded tail (including the one-vararg form, whose length equals the method's
   * parameter count) is copied into a new array of the declared component type. A generic
   * interface erases {@code T...} to {@code Object[]}, so a call through that view hands this
   * method an {@code Object[]} container even when the implementation declares {@code String...};
   * its elements are copied into the declared component type instead of being treated as one
   * standalone element. A multi-dimensional tail is rebuilt recursively, so {@code String[]...}
   * accepts an erased {@code Object[]} of rows.
   *
   * @param arrayType      the declared array type of the tail parameter
   * @param parameterCount the method's parameter count
   * @param args           the invocation arguments
   * @param index          the tail parameter's index
   * @return the tail as an array of the declared component type
   */
  static Object varargsTail(Class<?> arrayType, int parameterCount, Object[] args, int index) {
    Class<?> componentType = arrayType.getComponentType();
    if (args.length == parameterCount) {
      Object tail = args[index];
      if (tail == null || arrayType.isInstance(tail)) {
        return tail;
      }
      // A multi-dimensional tail (String[]... has component type String[]) can arrive as one
      // already-expanded element. That element is not an erased container of the next dimension:
      // adapting it would write the element's values into rows of the deeper array. A non-array
      // component (Object, Serializable, a generic erasure) keeps the container interpretation:
      // an Object[] is itself an instance of Serializable, so only an array-typed component can
      // distinguish a single row from the erased container.
      if (tail instanceof Object[] && (!componentType.isArray() || !componentType.isInstance(tail))) {
        return adaptArgument(arrayType, tail);
      }
    }
    return copyVarargs(args, componentType, index, Math.max(0, args.length - index));
  }

  /**
   * Adapt an erased {@code Object[]} to the expected array type, recursively. Returns {@code arg}
   * unchanged when it is {@code null}, the expected type is not an array, or it already implements
   * the expected type. A reference array is rebuilt with the declared component type and each
   * element is adapted in turn, so a {@code String[][]} parameter accepts an {@code Object[]} of
   * {@code Object[]} rows without an {@link ArrayStoreException}.
   *
   * @param expectedType the declared parameter type
   * @param arg          the reported argument, possibly an erased container
   * @return the argument in the declared array shape, or the argument unchanged
   */
  public static @Nullable Object adaptArgument(Class<?> expectedType, @Nullable Object arg) {
    if (arg == null || !expectedType.isArray() || expectedType.isInstance(arg)) {
      return arg;
    }
    if (!(arg instanceof Object[] erased)) {
      return arg;
    }
    Class<?> componentType = expectedType.getComponentType();
    Object adapted = Array.newInstance(componentType, erased.length);
    for (int i = 0; i < erased.length; i++) {
      Array.set(adapted, i, adaptArgument(componentType, erased[i]));
    }
    return adapted;
  }

  private static Object copyVarargs(Object[] source, Class<?> componentType, int from, int length) {
    Object varargs = Array.newInstance(componentType, length);
    for (int i = 0; i < length; i++) {
      Array.set(varargs, i, adaptArgument(componentType, source[from + i]));
    }
    return varargs;
  }

  /**
   * Snapshot the invocation arguments a retained background-refresh loader may reuse. The caller can
   * mutate only an array-valued argument after the call, so an argument list with no array returns
   * as-is and a plain L1 hit pays no copy; when an array is present, the argument array and every
   * array reachable through its array elements are copied before the caller can mutate them. A
   * non-array mutable object (including an array held inside a `List`, `Map` or POJO) is still
   * shared, so a method whose body depends on caller-mutated argument state must not declare a
   * soft TTL.
   *
   * @param args the invocation arguments, never {@code null}
   * @return the original array when no array element can be mutated, otherwise a snapshot
   */
  public static Object[] snapshot(Object[] args) {
    if (args.length == 0) {
      return args;
    }
    for (Object argument : args) {
      if (argument != null && argument.getClass().isArray()) {
        return copyArrays(args);
      }
    }
    return args;
  }

  private static Object[] copyArrays(Object[] args) {
    Object[] copy = args.clone();
    IdentityHashMap<Object, Object> seen = null;
    for (int i = 0; i < copy.length; i++) {
      Object argument = copy[i];
      if (argument != null && argument.getClass().isArray()) {
        if (argument instanceof Object[] objects && hasNestedArray(objects)) {
          if (seen == null) {
            seen = new IdentityHashMap<>();
          }
          copy[i] = cloneArrayDeep(objects, seen);
        } else {
          copy[i] = cloneFlatArray(argument);
        }
      }
    }
    return copy;
  }

  /** Whether a reference array directly holds another array. */
  private static boolean hasNestedArray(Object[] values) {
    for (Object element : values) {
      if (element != null && element.getClass().isArray()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Clone one array argument, recursing into its array elements. A reference array is inspected
   * element by element: its component type is {@code Object} even when every element is an array,
   * so the component type cannot decide whether the array is flat. Each source array is cloned
   * once and recorded in {@code seen} before its elements are visited, so a repeated array
   * resolves to its existing clone and a self-referential graph keeps its cycle instead of
   * recursing without bound. A primitive array has no elements to detach and uses its native
   * {@code clone()}.
   *
   * @param array the array argument, or an array element
   * @param seen  the source-to-clone map of this call
   * @return a detached copy with the same runtime array type
   */
  private static Object cloneArrayDeep(Object array, IdentityHashMap<Object, Object> seen) {
    if (!(array instanceof Object[] values)) {
      return cloneFlatArray(array);
    }
    Object existing = seen.get(values);
    if (existing != null) {
      return existing;
    }
    Object[] cloned = values.clone();
    seen.put(values, cloned);
    for (int i = 0; i < values.length; i++) {
      Object element = values[i];
      if (element != null && element.getClass().isArray()) {
        cloned[i] = cloneArrayDeep(element, seen);
      }
    }
    return cloned;
  }

  private static Object cloneFlatArray(Object array) {
    if (array instanceof Object[] values) {
      return values.clone();
    }
    if (array instanceof byte[] values) {
      return values.clone();
    }
    if (array instanceof short[] values) {
      return values.clone();
    }
    if (array instanceof int[] values) {
      return values.clone();
    }
    if (array instanceof long[] values) {
      return values.clone();
    }
    if (array instanceof float[] values) {
      return values.clone();
    }
    if (array instanceof double[] values) {
      return values.clone();
    }
    if (array instanceof char[] values) {
      return values.clone();
    }
    if (array instanceof boolean[] values) {
      return values.clone();
    }
    throw new AssertionError("not an array: " + array.getClass());
  }

  private InvocationArguments() {}
}
