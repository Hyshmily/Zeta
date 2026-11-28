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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;
import java.lang.reflect.Method;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;

@DisplayName("ParameterBindings")
class ParameterBindingsTest {

  private final ParameterNameDiscoverer parameterNames = new DefaultParameterNameDiscoverer();

  @Test
  @DisplayName("a parameter named for its own positional slot is legal")
  void ownPositionalNameIsLegal() throws NoSuchMethodException {
    Method method = ParameterBindingsTest.class.getDeclaredMethod("ownPosition", String.class);

    ParameterBindings bindings = ParameterBindings.of(method, names(method));

    assertThat(bindings.slotOf("p0")).isZero();
    assertThat(bindings.resolve("p0", new Object[] { "v" })).isEqualTo("v");
  }

  @Test
  @DisplayName("a parameter named for another positional slot cannot move #pN")
  void positionalAliasWinsOverAParameterName() throws NoSuchMethodException {
    Method method = ParameterBindingsTest.class.getDeclaredMethod("shadowedPositions", String.class, String.class);

    ParameterBindings bindings = ParameterBindings.of(method, names(method));

    assertThat(bindings.slotOf("p0")).isZero();
    assertThat(bindings.slotOf("p1")).isEqualTo(1);
    assertThat(bindings.resolve("p0", new Object[] { "first", "second" })).isEqualTo("first");
    assertThat(bindings.resolve("p1", new Object[] { "first", "second" })).isEqualTo("second");
  }

  @Test
  @DisplayName("a name that spells an out-of-range positional alias is reserved, not bound")
  void outOfRangePositionalNameIsReserved() throws NoSuchMethodException {
    Method method = ParameterBindingsTest.class.getDeclaredMethod("conflictingPosition", String.class);

    ParameterBindings bindings = ParameterBindings.of(method, names(method));

    assertThat(bindings.slotOf("p1")).isEqualTo(-1);
    assertThat(bindings.slotOf("p0")).isZero();
  }

  @Test
  @DisplayName("reserved context names never enter the slot table")
  void contextNamesAreNotSlots() throws NoSuchMethodException {
    Method method = ParameterBindingsTest.class.getDeclaredMethod("reservedNames", String.class, String.class);
    Object[] args = { "root-value", "result-value" };

    ParameterBindings bindings = ParameterBindings.of(method, names(method));

    assertThat(bindings.slotOf("root")).isEqualTo(-1);
    assertThat(bindings.slotOf("result")).isEqualTo(-1);
    assertThat(bindings.resolve("root", args)).isNull();
    assertThat(bindings.resolve("result", args)).isNull();
  }

  @Test
  @DisplayName("a packed nested varargs tail may contain a null row")
  void packedNestedVarargsTailAcceptsANullRow() throws NoSuchMethodException {
    Method method = ParameterBindingsTest.class.getDeclaredMethod("nestedVarargs", String[][].class);
    Object[] reported = { new Object[] { new String[] { "a" }, null } };

    Object[] packed = ParameterBindings.pack(method, reported);

    assertThat(packed).hasSize(1);
    assertThat(packed[0]).isInstanceOf(String[][].class);
    assertThat((Object[]) packed[0]).containsExactly(new String[] { "a" }, null);
  }

  @Test
  @DisplayName("a single empty row of a nested varargs parameter stays one row")
  void singleEmptyNestedVarargsRowIsPreserved() throws NoSuchMethodException {
    Method method = ParameterBindingsTest.class.getDeclaredMethod("nestedVarargs", String[][].class);

    Object[] packed = ParameterBindings.pack(method, new Object[] { new String[0] });

    assertThat(packed).hasSize(1);
    assertThat((String[][]) packed[0]).hasDimensions(1, 0);
  }

  @Test
  @DisplayName("an expanded nested varargs tail is packed row by row")
  void expandedNestedVarargsTailIsPacked() throws NoSuchMethodException {
    Method method = ParameterBindingsTest.class.getDeclaredMethod("nestedVarargs", String[][].class);

    Object[] packed = ParameterBindings.pack(method, new Object[] { new Object[] { "a" }, new Object[] { "b" } });

    assertThat((Object[]) packed[0]).containsExactly(new String[] { "a" }, new String[] { "b" });
  }

  @Test
  @DisplayName("an erased packed tail of a primitive-array varargs parameter is adapted row by row")
  void erasedPackedPrimitiveVarargsTailIsAdapted() throws NoSuchMethodException {
    Method method = ParameterBindingsTest.class.getDeclaredMethod("primitiveNestedVarargs", int[][].class);

    Object[] packed = ParameterBindings.pack(method, new Object[] { new Object[] { new Object[] { 1, 2 } } });

    assertThat(packed).hasSize(1);
    assertThat(packed[0]).isInstanceOf(int[][].class);
    int[][] rows = (int[][]) packed[0];
    assertThat(rows).hasDimensions(1, 2);
    assertThat(rows[0]).containsExactly(1, 2);
  }

  @Test
  @DisplayName("an argument can be an array only when a declared type can hold one")
  void mayContainArrayFollowsTheDeclaredTypes() {
    assertThat(bindingsOf("scalarArgs").mayContainArray()).isFalse();
    assertThat(bindingsOf("arrayArg").mayContainArray()).isTrue();
    assertThat(bindingsOf("objectArg").mayContainArray()).isTrue();
    assertThat(bindingsOf("cloneableArg").mayContainArray()).isTrue();
    assertThat(bindingsOf("serializableArg").mayContainArray()).isTrue();
    assertThat(bindingsOf("nestedVarargs").mayContainArray()).as("a varargs tail is declared as its array")
      .isTrue();
  }

  private ParameterBindings bindingsOf(String name) {
    for (Method method : ParameterBindingsTest.class.getDeclaredMethods()) {
      if (method.getName().equals(name)) {
        return ParameterBindings.of(method, names(method));
      }
    }
    throw new AssertionError("no such method: " + name);
  }

  @SuppressWarnings("unused")
  private String ownPosition(String p0) {
    return p0;
  }

  @SuppressWarnings("unused")
  private String conflictingPosition(String p1) {
    return p1;
  }

  @SuppressWarnings("unused")
  private String shadowedPositions(String p1, String p0) {
    return p1 + p0;
  }

  @SuppressWarnings("unused")
  private String reservedNames(String root, String result) {
    return root + result;
  }

  @SuppressWarnings("unused")
  private void nestedVarargs(String[]... tails) {}

  @SuppressWarnings("unused")
  private void primitiveNestedVarargs(int[]... tails) {}

  @SuppressWarnings("unused")
  private void scalarArgs(String id, long version) {}

  @SuppressWarnings("unused")
  private void arrayArg(String[] ids) {}

  @SuppressWarnings("unused")
  private void objectArg(Object payload) {}

  @SuppressWarnings("unused")
  private void cloneableArg(Cloneable payload) {}

  @SuppressWarnings("unused")
  private void serializableArg(Serializable payload) {}

  private String@Nullable [] names(Method method) {
    return parameterNames.getParameterNames(method);
  }
}
