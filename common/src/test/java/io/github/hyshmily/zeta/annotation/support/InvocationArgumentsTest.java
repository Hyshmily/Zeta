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

import java.io.Serializable;
import java.lang.reflect.Method;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("InvocationArguments")
class InvocationArgumentsTest {

  static class Fixture {

    public String join(String... ids) {
      return String.join(",", ids);
    }

    public String save(String[] ids) {
      return String.join(",", ids);
    }

    public String merge(String[] left, String[] right) {
      return String.join(",", left) + "|" + String.join(",", right);
    }

    public String matrix(String[]... rows) {
      return String.join(
        ",",
        java.util.Arrays.stream(rows)
          .map(row -> String.join(",", row))
          .toList()
      );
    }

    public String grid(String[][] rows) {
      return matrix(rows);
    }

    public String echo(Serializable... values) {
      return String.join(",", java.util.Arrays.stream(values).map(Object::toString).toList());
    }

    public String accept(Serializable[] values) {
      return Integer.toString(values.length);
    }

    public String log(String tag, Object... values) {
      return tag + ":" + values.length;
    }
  }

  @Test
  @DisplayName("pack copies an erased Object[] into the declared component type")
  void packCopiesErasedVarargsToTheComponentType() throws NoSuchMethodException {
    Method join = Fixture.class.getDeclaredMethod("join", String[].class);
    Object[] erased = { new Object[] { "a", "b" } };

    Object[] packed = InvocationArguments.pack(join, erased);
    assertThat(packed).hasSize(1);
    assertThat(packed[0]).isInstanceOf(String[].class);
    assertThat((String[]) packed[0]).containsExactly("a", "b");
  }

  @Test
  @DisplayName("pack copies an erased Object[] into a non-varargs array parameter")
  void packNormalizesANonVarargsArrayParameter() throws NoSuchMethodException {
    Method save = Fixture.class.getDeclaredMethod("save", String[].class);
    Object[] erased = { new Object[] { "a", "b" } };

    Object[] packed = InvocationArguments.pack(save, erased);
    assertThat(packed[0]).isInstanceOf(String[].class);
    assertThat((String[]) packed[0]).containsExactly("a", "b");
  }

  /**
   * Every array is {@link Serializable}, so testing the component type with {@code isInstance}
   * mistakes the erased {@code Object[]} container for one element of a {@code Serializable...}
   * method.
   */
  @Test
  @DisplayName("pack treats an erased Object[] as the container under a Serializable bound")
  void packTreatsAnErasedObjectArrayAsTheContainerUnderASerializableBound() throws NoSuchMethodException {
    Method echo = Fixture.class.getDeclaredMethod("echo", Serializable[].class);
    Method accept = Fixture.class.getDeclaredMethod("accept", Serializable[].class);
    Object[] erased = { new Object[] { "a", "b" } };

    Object[] varargs = InvocationArguments.pack(echo, erased);
    assertThat(varargs[0]).isInstanceOf(Serializable[].class);
    assertThat((Serializable[]) varargs[0]).containsExactly("a", "b");

    Object[] array = InvocationArguments.pack(accept, erased);
    assertThat(array[0]).isInstanceOf(Serializable[].class);
    assertThat((Serializable[]) array[0]).containsExactly("a", "b");
  }

  @Test
  @DisplayName("pack adapts expanded, one-value and packed varargs for reflection")
  void packAdaptsEveryVarargsShape() throws NoSuchMethodException {
    Method join = Fixture.class.getDeclaredMethod("join", String[].class);

    assertThat((Object[]) InvocationArguments.pack(join, new Object[] { "a", "b" })[0]).containsExactly("a", "b");
    assertThat((Object[]) InvocationArguments.pack(join, new Object[] { "a" })[0]).containsExactly("a");
    assertThat((Object[]) InvocationArguments.pack(join, new Object[0])[0]).isEmpty();

    Object[] packed = { new String[] { "a", "b" } };
    assertThat(InvocationArguments.pack(join, packed)).isSameAs(packed);
  }

  @Test
  @DisplayName("a single String[] is one row of a String[]... tail, not an erased container")
  void packTreatsASingleRowAsOneVarargsElement() throws NoSuchMethodException {
    Method matrix = Fixture.class.getDeclaredMethod("matrix", String[][].class);
    Object[] oneRow = { new String[] { "a", "b" } };

    Object[] packed = InvocationArguments.pack(matrix, oneRow);

    assertThat(packed[0]).isInstanceOf(String[][].class);
    assertThat((String[][]) packed[0]).hasDimensions(1, 2);
    assertThat(((String[][]) packed[0])[0]).containsExactly("a", "b");
  }

  @Test
  @DisplayName("pack adapts every non-varargs array parameter, not only the last")
  void packAdaptsEveryNonVarargsArray() throws NoSuchMethodException {
    Method merge = Fixture.class.getDeclaredMethod("merge", String[].class, String[].class);
    Object[] erased = { new Object[] { "a" }, new Object[] { "b" } };

    Object[] packed = InvocationArguments.pack(merge, erased);
    assertThat(packed[0]).isInstanceOf(String[].class);
    assertThat((String[]) packed[0]).containsExactly("a");
    assertThat(packed[1]).isInstanceOf(String[].class);
    assertThat((String[]) packed[1]).containsExactly("b");
  }

  @Test
  @DisplayName("pack tolerates a short argument array for a multi-parameter varargs method")
  void packToleratesShortVarargsArguments() throws NoSuchMethodException {
    Method log = Fixture.class.getDeclaredMethod("log", String.class, Object[].class);

    Object[] empty = InvocationArguments.pack(log, new Object[0]);
    assertThat(empty).hasSize(2);
    assertThat(empty[0]).isNull();
    assertThat((Object[]) empty[1]).isEmpty();

    Object[] tagOnly = InvocationArguments.pack(log, new Object[] { "tag" });
    assertThat(tagOnly[0]).isEqualTo("tag");
    assertThat((Object[]) tagOnly[1]).isEmpty();

    Object[] expanded = InvocationArguments.pack(log, new Object[] { "tag", "a", "b" });
    assertThat(expanded[0]).isEqualTo("tag");
    assertThat((Object[]) expanded[1]).containsExactly("a", "b");
  }

  @Test
  @DisplayName("pack rebuilds a multi-dimensional varargs tail from an erased Object[]")
  void packRebuildsAMultiDimensionalVarargsTail() throws NoSuchMethodException {
    Method matrix = Fixture.class.getDeclaredMethod("matrix", String[][].class);
    Object[] rows = { new Object[] { "a" }, new Object[] { "b" } };

    Object[] packed = InvocationArguments.pack(matrix, new Object[] { rows });
    assertThat(packed[0]).isInstanceOf(String[][].class);
    String[][] matrixRows = (String[][]) packed[0];
    assertThat(matrixRows[0]).containsExactly("a");
    assertThat(matrixRows[1]).containsExactly("b");
  }

  @Test
  @DisplayName("pack rebuilds expanded multi-dimensional varargs elements")
  void packRebuildsExpandedMultiDimensionalVarargs() throws NoSuchMethodException {
    Method matrix = Fixture.class.getDeclaredMethod("matrix", String[][].class);
    Object[] expanded = { new Object[] { "a" }, new Object[] { "b" } };

    Object[] packed = InvocationArguments.pack(matrix, expanded);
    assertThat(packed[0]).isInstanceOf(String[][].class);
    String[][] matrixRows = (String[][]) packed[0];
    assertThat(matrixRows[0]).containsExactly("a");
    assertThat(matrixRows[1]).containsExactly("b");
  }

  @Test
  @DisplayName("pack rebuilds a non-varargs multi-dimensional array parameter")
  void packRebuildsANonVarargsMultiDimensionalArray() throws NoSuchMethodException {
    Method grid = Fixture.class.getDeclaredMethod("grid", String[][].class);
    Object[] erased = { new Object[] { new Object[] { "a" }, new Object[] { "b" } } };

    Object[] packed = InvocationArguments.pack(grid, erased);
    assertThat(packed[0]).isInstanceOf(String[][].class);
    String[][] matrixRows = (String[][]) packed[0];
    assertThat(matrixRows[0]).containsExactly("a");
    assertThat(matrixRows[1]).containsExactly("b");
  }
}
