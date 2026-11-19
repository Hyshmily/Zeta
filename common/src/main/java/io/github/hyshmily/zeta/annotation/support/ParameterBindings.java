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
import java.util.HashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * The one immutable slot table a plan's SpEL lookups resolve against: every legal variable name
 * mapped to its argument index, plus the declared parameter types captured once.
 *
 * <p>Resolution runs on the evaluation path, so the table exists to keep it allocation-free beyond
 * the value it returns: a variable lookup is one map read instead of a scan of the discovered
 * parameter names, and a varargs or array argument is read from the captured types instead of
 * {@link Method#getParameterTypes()}, which clones the array on every call. A method with no array
 * and no varargs tail stores no type array, so its lookup is a plain slot read. The table is built
 * once per plan, at resolution, and never changes.
 */
@Internal
public final class ParameterBindings {

  private static final ParameterBindings EMPTY = new ParameterBindings(null, 0L, false, Map.of());

  /**
   * The declared parameter types, or {@code null} when no slot needs array adaptation. A method
   * with no array and no varargs tail is the common case, and its lookups skip the type check
   * entirely.
   */
  private final Class<?>@Nullable [] parameterTypes;

  /** Bit {@code i} marks slot {@code i} as an array; slots beyond {@code Long.SIZE} use the type. */
  private final long arrayMask;

  private final boolean varArgs;

  private final Map<String, Integer> slots;

  private ParameterBindings(
    Class<?>@Nullable [] parameterTypes,
    long arrayMask,
    boolean varArgs,
    Map<String, Integer> slots
  ) {
    this.parameterTypes = parameterTypes;
    this.arrayMask = arrayMask;
    this.varArgs = varArgs;
    this.slots = slots;
  }

  /**
   * Build the slot table of one method plan.
   *
   * @param method         the group's most-specific method: it supplies the parameter types and the
   *                       varargs flag
   * @param parameterNames the names the expression was written against, or {@code null} when they
   *                       were not discoverable
   * @return the immutable slot table, never {@code null}
   * @throws IllegalStateException when a parameter name would overwrite a positional slot for a
   *                               different argument
   */
  public static ParameterBindings of(Method method, String@Nullable [] parameterNames) {
    int count = method.getParameterCount();
    if (count == 0) {
      return EMPTY;
    }
    Class<?>[] declared = method.getParameterTypes();
    long arrayMask = 0L;
    boolean arrays = method.isVarArgs();
    for (int i = 0; i < count; i++) {
      if (declared[i].isArray()) {
        arrays = true;
        if (i < Long.SIZE) {
          arrayMask |= 1L << i;
        }
      }
    }
    // Three entries per parameter (pN, aN, and its discovered name); a capacity of four per
    // parameter keeps the default 0.75 load factor from rehashing on the last insertion.
    Map<String, Integer> slots = new HashMap<>(count * 4);
    for (int i = 0; i < count; i++) {
      Integer slot = i;
      slots.put("p" + i, slot);
      slots.put("a" + i, slot);
      String name = parameterNames != null && i < parameterNames.length ? parameterNames[i] : null;
      if (name == null || isContextVariable(name)) {
        continue;
      }
      int positional = CompiledExpression.argumentIndex(name);
      if (positional >= 0 && positional != i) {
        throw new IllegalStateException(
          "reserved positional parameter '" +
            name +
            "' on " +
            method +
            " would overwrite the binding for argument " +
            positional +
            "; #pN and #aN are generated from the argument slot table, so rename the parameter"
        );
      }
      slots.put(name, slot);
    }
    return new ParameterBindings(arrays ? declared : null, arrayMask, method.isVarArgs(), Map.copyOf(slots));
  }

  /** The names bound ahead of the argument table: SpEL's root/active object and the bound result. */
  static boolean isContextVariable(String name) {
    return "root".equals(name) || "this".equals(name) || "result".equals(name);
  }

  /**
   * Resolve one variable name to its argument index, or {@code -1} when it is unbound or reserved
   * as a context variable ({@code root}, {@code this}, {@code result}). Resolution-time checks use
   * this instead of scanning the parameter-name array again.
   *
   * @param variable the variable name without the leading {@code #}
   * @return the zero-based slot index, or {@code -1}
   */
  public int slotOf(String variable) {
    if (isContextVariable(variable)) {
      return -1;
    }
    Integer slot = slots.get(variable);
    return slot != null ? slot : -1;
  }

  /**
   * Resolve one variable name to its argument value. A name outside the table resolves to
   * {@code null}; expression validation rejects such a name at resolution, so this is the same
   * answer the evaluator would give for an unbound variable and allocates nothing.
   *
   * @param variable the variable name without the leading {@code #}
   * @param args     the invocation arguments
   * @return the adapted argument, or {@code null} when the name is not bound or the argument is
   *         absent
   */
  public @Nullable Object resolve(String variable, Object[] args) {
    Integer slot = slots.get(variable);
    if (slot == null) {
      return null;
    }
    int index = slot;
    Class<?>[] types = parameterTypes;
    if (types == null) {
      return index < args.length ? args[index] : null;
    }
    if (varArgs && index == types.length - 1) {
      return InvocationArguments.varargsTail(types[index], types.length, args, index);
    }
    if (index >= args.length) {
      return null;
    }
    if (index < Long.SIZE ? (arrayMask & (1L << index)) == 0 : !types[index].isArray()) {
      return args[index];
    }
    return InvocationArguments.adaptArgument(types[index], args[index]);
  }
}
