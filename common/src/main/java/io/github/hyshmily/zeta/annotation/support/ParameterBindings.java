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

import io.github.hyshmily.zeta.util.CollectionSizing;
import io.github.hyshmily.zeta.util.Internal;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Immutable parameter slot table that a plan's SpEL lookups resolve against, mapping every legal
 * variable name to its zero-based argument index.
 *
 * <p>The table is built once per plan. A lookup is a single map read, and a varargs tail is read
 * back as the array type declared by the method so a key written against {@code #ids} sees every
 * element rather than only the first one.
 *
 * <p>The positional aliases {@code #pN}/{@code #aN} always address parameter {@code N}: a
 * discovered name that spells one is reserved and never entered in the table, so it can neither
 * move an alias to another slot nor make an out-of-range alias resolve to its own slot.
 *
 * <p>This class also provides the two argument-shape conversions required by the annotation layer
 * outside SpEL: packing arguments into the exact shape required by {@link Method#invoke}, and
 * snapshotting array arguments for retained background-refresh loaders.
 */
@Internal
public final class ParameterBindings {

  /** The shared zero-argument array used where an invocation has no arguments. */
  public static final Object[] EMPTY_ARGS = new Object[0];
  private static final ParameterBindings EMPTY = new ParameterBindings(Map.of(), 0, false, null, false);

  private final Map<String, Integer> slots;
  private final int parameterCount;
  private final boolean hasParameterNames;

  /** The declared array type of a varargs tail parameter, or {@code null} when not varargs. */
  private final @Nullable Class<?> varargsTailType;

  /** Whether a declared parameter type can receive an array argument. */
  private final boolean mayContainArray;

  /**
   * Creates an immutable parameter binding table.
   *
   * @param slots             the immutable map from variable name to zero-based slot index
   * @param parameterCount    the number of declared method parameters
   * @param hasParameterNames whether discovered parameter names were available
   * @param varargsTailType   the declared array type of the varargs tail, or {@code null}
   * @param mayContainArray   whether a declared parameter type can receive an array argument
   */
  private ParameterBindings(
    Map<String, Integer> slots,
    int parameterCount,
    boolean hasParameterNames,
    @Nullable Class<?> varargsTailType,
    boolean mayContainArray
  ) {
    this.slots = slots;
    this.parameterCount = parameterCount;
    this.hasParameterNames = hasParameterNames;
    this.varargsTailType = varargsTailType;
    this.mayContainArray = mayContainArray;
  }

  /**
   * Builds the immutable slot table for one method plan.
   *
   * <p>The declared parameter types also decide whether an argument can ever be an array: an array
   * is assignable to another array type, {@link Object}, {@link Cloneable} or {@link Serializable}
   * and to nothing else, so any other declared type rules it out. Erasure makes this exact in both
   * directions — a type variable widens to its bound (usually {@code Object}), and a varargs tail
   * is declared as its component array.
   *
   * @param method         the group's most-specific method
   * @param parameterNames the discovered parameter names, or {@code null} when unavailable
   * @return the immutable slot table
   */
  public static ParameterBindings of(Method method, String@Nullable [] parameterNames) {
    Objects.requireNonNull(method, "method must not be null");
    int count = method.getParameterCount();
    if (count == 0) {
      return EMPTY;
    }

    // Writes are at most the discovered name plus #pN and #aN for each parameter.
    Map<String, Integer> slots = new HashMap<>(CollectionSizing.capacity(count * (parameterNames != null ? 3 : 2)));
    if (parameterNames != null) {
      int limit = Math.min(count, parameterNames.length);
      for (int i = 0; i < limit; i++) {
        String name = parameterNames[i];
        // A discovered name that spells a positional alias is reserved, never entered: #pN/#aN must
        // always address parameter N, and an out-of-range alias must fail resolution with the
        // out-of-range diagnostic instead of binding this parameter's slot.
        if (name == null || isContextVariable(name) || CompiledExpression.argumentIndex(name) >= 0) {
          continue;
        }
        slots.put(name, i);
      }
    }
    for (int i = 0; i < count; i++) {
      Integer slot = i;
      slots.put("p" + i, slot);
      slots.put("a" + i, slot);
    }

    Class<?>[] parameterTypes = method.getParameterTypes();
    boolean mayContainArray = false;
    for (Class<?> type : parameterTypes) {
      if (type.isArray() || type == Object.class || type == Cloneable.class || type == Serializable.class) {
        mayContainArray = true;
        break;
      }
    }
    Class<?> tailType = method.isVarArgs() ? parameterTypes[count - 1] : null;
    return new ParameterBindings(Map.copyOf(slots), count, parameterNames != null, tailType, mayContainArray);
  }

  /**
   * Returns the method's declared parameter count.
   *
   * @return the number of declared parameters
   */
  public int parameterCount() {
    return parameterCount;
  }

  /**
   * Returns whether this slot table includes discovered parameter names in addition to
   * {@code #pN}/{@code #aN}.
   *
   * @return {@code true} if parameter names were available at resolution time
   */
  public boolean hasParameterNames() {
    return hasParameterNames;
  }

  /**
   * Returns whether any declared parameter type can receive an array argument. When {@code false},
   * the invocation's argument array holds no array and needs no defensive snapshot for a retained
   * loader.
   *
   * @return {@code true} if an argument may be an array
   */
  public boolean mayContainArray() {
    return mayContainArray;
  }

  /**
   * Resolves a variable name to its zero-based parameter slot index.
   *
   * @param variable the variable name without the leading {@code '#'}
   * @return the zero-based slot index, or {@code -1} if unbound
   */
  public int slotOf(String variable) {
    Objects.requireNonNull(variable, "variable must not be null");
    Integer slot = slots.get(variable);
    return slot != null ? slot : -1;
  }

  /**
   * Resolves a variable name to its corresponding invocation argument value.
   *
   * @param variable the variable name without the leading {@code '#'}
   * @param args     the invocation arguments
   * @return the resolved argument value, or {@code null} if unbound or absent
   */
  public @Nullable Object resolve(String variable, Object[] args) {
    Objects.requireNonNull(variable, "variable must not be null");
    Objects.requireNonNull(args, "args must not be null");
    Integer slot = slots.get(variable);
    if (slot == null) {
      return null;
    }
    
    Class<?> tailType = varargsTailType;
    if (tailType != null && slot == parameterCount - 1) {
      return varargsTail(tailType, parameterCount, args, slot);
    }
    return slot < args.length ? args[slot] : null;
  }

  /**
   * Adapts an invocation argument array to the packed shape required by {@link Method#invoke}:
   * exactly {@code parameterCount} elements, with erased array parameters converted to their
   * declared component types and any varargs tail packed into a component-typed array.
   *
   * @param method the target method
   * @param args   the invocation arguments as reported by the proxy
   * @return {@code args} unchanged when already packed, otherwise a packed copy
   */
  public static Object[] pack(Method method, Object[] args) {
    Objects.requireNonNull(method, "method must not be null");
    Objects.requireNonNull(args, "args must not be null");

    int count = method.getParameterCount();
    if (count == 0) {
      return args.length == 0 ? args : EMPTY_ARGS;
    }

    Class<?>[] types = method.getParameterTypes();
    if (!method.isVarArgs()) {
      if (args.length == count) {
        return adaptFixed(args, types, count);
      }
      Object[] packed = Arrays.copyOf(args, count);
      adaptInPlace(packed, types, Math.min(count, args.length));
      return packed;
    }

    int last = count - 1;
    Class<?> arrayType = types[last];
    if (args.length == count && (args[last] == null || arrayType.isInstance(args[last]))) {
      return adaptFixed(args, types, last);
    }

    Object[] packed = Arrays.copyOf(args, count);
    adaptInPlace(packed, types, Math.min(last, args.length));
    packed[last] = varargsTail(arrayType, count, args, last);
    return packed;
  }

  /**
   * Adapts an erased {@code Object[]} to the expected declared array type. Reference arrays are
   * rebuilt with the declared component type and nested rows are adapted recursively so parameters
   * such as {@code String[][]} accept an {@code Object[]} of rows without raising an
   * {@link ArrayStoreException}.
   *
   * @param expectedType the declared parameter or component type
   * @param arg          the reported argument, possibly an erased {@code Object[]}
   * @return the argument in the declared array shape, or {@code arg} unchanged if no adaptation applies
   */
  public static @Nullable Object adaptArgument(Class<?> expectedType, @Nullable Object arg) {
    Objects.requireNonNull(expectedType, "expectedType must not be null");
    if (arg == null || !expectedType.isArray() || expectedType.isInstance(arg) || !(arg instanceof Object[] erased)) {
      return arg;
    }

    Class<?> componentType = expectedType.getComponentType();
    if (!componentType.isPrimitive()) {
      if (!componentType.isArray()) {
        return copyReferenceArray(erased, expectedType);
      }
      Object[] adapted = (Object[]) Array.newInstance(componentType, erased.length);
      for (int i = 0; i < erased.length; i++) {
        adapted[i] = adaptArgument(componentType, erased[i]);
      }
      return adapted;
    }

    Object adapted = Array.newInstance(componentType, erased.length);
    for (int i = 0; i < erased.length; i++) {
      Array.set(adapted, i, erased[i]);
    }
    return adapted;
  }

  /**
   * Creates a defensive snapshot of invocation arguments for a retained background-refresh loader.
   * When no non-empty array argument is present, {@code args} is returned unchanged so plain cache
   * hits allocate nothing; an empty array is shared because its length is fixed and it has no
   * elements to mutate. A flat array is cloned without the identity map; only a nested reference
   * array pays for the cycle/shared-reference table.
   *
   * @param args the invocation arguments; must not be {@code null}
   * @return {@code args} unchanged when no array element is present, otherwise a defensive snapshot
   */
  public static Object[] snapshot(Object[] args) {
    Objects.requireNonNull(args, "args must not be null");
    boolean arrayPresent = false;
    boolean nested = false;
    for (Object argument : args) {
      if (argument == null || !argument.getClass().isArray() || Array.getLength(argument) == 0) {
        // An empty array cannot change: its length is fixed and it has no elements to mutate.
        continue;
      }
      arrayPresent = true;
      if (!nested && hasArrayElement(argument)) {
        nested = true;
      }
    }
    if (!arrayPresent) {
      return args;
    }

    Object[] copy = args.clone();
    if (!nested) {
      // Flat arrays need neither the identity map nor a recursive walk.
      for (int i = 0; i < args.length; i++) {
        Object argument = args[i];
        if (argument == null || !argument.getClass().isArray() || Array.getLength(argument) == 0) {
          continue;
        }
        copy[i] = argument instanceof Object[] values ? values.clone() : clonePrimitiveArray(argument);
      }
      return copy;
    }

    IdentityHashMap<Object, Object> seen = new IdentityHashMap<>();
    for (int i = 0; i < args.length; i++) {
      Object argument = args[i];
      if (argument == null || !argument.getClass().isArray() || Array.getLength(argument) == 0) {
        continue;
      }
      copy[i] = cloneArray(argument, seen);
    }
    return copy;
  }

  /**
   * Returns whether a non-empty reference array holds at least one array element; a primitive array
   * never does.
   *
   * @param array the array to inspect
   * @return {@code true} when a nested array element is present
   */
  private static boolean hasArrayElement(Object array) {
    if (!(array instanceof Object[] values)) {
      return false;
    }
    for (Object element : values) {
      if (element != null && element.getClass().isArray()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns whether {@code name} is a reserved SpEL context variable bound ahead of method
   * parameters ({@code root}, {@code this}, or {@code result}).
   *
   * @param name the unprefixed variable name
   * @return {@code true} if the name is reserved by SpEL or the evaluation phase
   */
  static boolean isContextVariable(String name) {
    return "root".equals(name) || "this".equals(name) || "result".equals(name);
  }

  /**
   * Builds or extracts the varargs tail at {@code index} as a component-typed array. A tail that is
   * already packed (or {@code null}) is returned unchanged; an expanded tail is copied into a new
   * array of the declared component type.
   *
   * <p>Only a plain {@code Object[]} counts as an erased packed tail: a reported argument of any
   * other array class is one element of the declared component type, so a single empty row of a
   * {@code String[]...} parameter stays one row instead of collapsing to zero.
   *
   * @param arrayType      the declared array type of the varargs tail parameter
   * @param parameterCount the method's total declared parameter count
   * @param args           the invocation arguments
   * @param index          the zero-based index of the varargs tail parameter
   * @return the packed tail array, or {@code null} when an explicit {@code null} tail was supplied
   */
  static @Nullable Object varargsTail(Class<?> arrayType, int parameterCount, Object[] args, int index) {
    Class<?> componentType = arrayType.getComponentType();
    if (args.length == parameterCount) {
      Object tail = args[index];
      if (tail == null || arrayType.isInstance(tail)) {
        return tail;
      }
      if (
        tail instanceof Object[] erased &&
        tail.getClass() == Object[].class &&
        (!componentType.isArray() || isPackedNestedArray(erased, componentType))
      ) {
        return adaptArgument(arrayType, tail);
      }
    }

    int tailLength = Math.max(0, args.length - index);
    Object varargs = Array.newInstance(componentType, tailLength);
    for (int i = 0; i < tailLength; i++) {
      Array.set(varargs, i, adaptArgument(componentType, args[index + i]));
    }
    return varargs;
  }

  /**
   * Returns whether an erased tail array is already packed in the declared nested component type:
   * every element is assignable to it, or is an {@code Object[]} row that can be adapted to it. An
   * {@code Object[]} element cannot be an instance of a primitive-array component type, so it can
   * only be a row of the packed tail; treating it as a single element instead copied the whole tail
   * into one component array and failed in {@code Array.set}. An empty array is a valid empty tail
   * for any reference component type.
   *
   * @param packed        the erased tail array
   * @param componentType the declared array component type (itself an array)
   * @return {@code true} when the array is a packed tail rather than a single element
   */
  private static boolean isPackedNestedArray(Object[] packed, Class<?> componentType) {
    for (Object element : packed) {
      // A null row is a legal element of the packed tail; treating it as a single element made
      // adaptArgument copy the whole tail into one component array and raise ArrayStoreException.
      if (element == null || componentType.isInstance(element) || element instanceof Object[]) {
        continue;
      }
      return false;
    }
    return true;
  }

  /**
   * Lazily copies {@code args} only if at least one fixed parameter requires array adaptation.
   *
   * @param args  the source argument array
   * @param types the declared parameter types
   * @param count the number of leading parameters to inspect
   * @return {@code args} if unchanged, otherwise a cloned array containing the adapted values
   */
  private static Object[] adaptFixed(Object[] args, Class<?>[] types, int count) {
    Object[] packed = null;
    for (int i = 0; i < count; i++) {
      Object adapted = adaptArgument(types[i], args[i]);
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
   * Adapts the leading {@code count} entries of a freshly allocated argument array in place.
   *
   * @param target the mutable target array
   * @param types  the declared parameter types
   * @param count  the number of leading entries to adapt
   */
  private static void adaptInPlace(Object[] target, Class<?>[] types, int count) {
    for (int i = 0; i < count; i++) {
      target[i] = adaptArgument(types[i], target[i]);
    }
  }

  /**
   * Copies an erased {@code Object[]} into a newly allocated array of the specified reference array
   * type.
   *
   * @param erased       the source {@code Object[]}
   * @param expectedType the target non-primitive array class
   * @return a new array instance of {@code expectedType}
   */
  private static Object[] copyReferenceArray(Object[] erased, Class<?> expectedType) {
    return Arrays.copyOf(erased, erased.length, (Class<? extends Object[]>) expectedType);
  }

  /**
   * Deep-clones an array argument, recursing into nested reference arrays while preserving cycles
   * and shared references via {@code seen}.
   *
   * @param array the array object to clone
   * @param seen  identity map tracking already cloned arrays
   * @return the cloned array, or the original instance if empty
   */
  private static Object cloneArray(Object array, IdentityHashMap<Object, Object> seen) {
    Object existing = seen.get(array);
    if (existing != null) {
      return existing;
    }
    if (!(array instanceof Object[] values)) {
      Object cloned = clonePrimitiveArray(array);
      if (cloned != array) {
        seen.put(array, cloned);
      }
      return cloned;
    }
    if (values.length == 0) {
      return values;
    }
    Object[] cloned = values.clone();
    seen.put(values, cloned);
    for (int i = 0; i < values.length; i++) {
      Object element = values[i];
      if (element != null && element.getClass().isArray()) {
        cloned[i] = cloneArray(element, seen);
      }
    }
    return cloned;
  }

  /**
   * Clones a primitive array without reflection, returning zero-length arrays unchanged.
   *
   * @param array the primitive array instance
   * @return a shallow copy of the array, or {@code array} itself when empty
   */
  private static Object clonePrimitiveArray(Object array) {
    if (array instanceof int[] values) {
      return values.length == 0 ? values : values.clone();
    }
    if (array instanceof long[] values) {
      return values.length == 0 ? values : values.clone();
    }
    if (array instanceof byte[] values) {
      return values.length == 0 ? values : values.clone();
    }
    if (array instanceof char[] values) {
      return values.length == 0 ? values : values.clone();
    }
    if (array instanceof double[] values) {
      return values.length == 0 ? values : values.clone();
    }
    if (array instanceof float[] values) {
      return values.length == 0 ? values : values.clone();
    }
    if (array instanceof short[] values) {
      return values.length == 0 ? values : values.clone();
    }
    if (array instanceof boolean[] values) {
      return values.length == 0 ? values : values.clone();
    }
    throw new AssertionError("Unexpected primitive array type: " + array.getClass().getName());
  }
}
