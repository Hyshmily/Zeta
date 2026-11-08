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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("DefaultKey")
class DefaultKeyTest {

  @Test
  @DisplayName("no arguments encode to []")
  void noArguments() {
    assertThat(DefaultKey.encode(new Object[0])).isEqualTo("[]");
  }

  @Test
  @DisplayName("a single argument carries its value's type tag")
  void singleArgument() {
    assertThat(DefaultKey.encode(new Object[] { "x" })).isEqualTo("[s1:x]");
    assertThat(DefaultKey.encode(new Object[] { "" })).isEqualTo("[s0:]");
    assertThat(DefaultKey.encode(new Object[] { 42 })).isEqualTo("[I:42]");
    assertThat(DefaultKey.encode(new Object[] { 'a' })).isEqualTo("[c1:a]");
    assertThat(DefaultKey.encode(new Object[] { "a", 1 })).isEqualTo("[s1:a,I:1]");
  }

  @Test
  @DisplayName("String, Character and other CharSequence values never collide, even with equal text")
  void textShapesStayDistinct() {
    assertThat(DefaultKey.encode(new Object[] { "a" })).isEqualTo("[s1:a]");
    assertThat(DefaultKey.encode(new Object[] { 'a' })).isEqualTo("[c1:a]");
    assertThat(DefaultKey.encode(new Object[] { new StringBuilder("a") })).isEqualTo("[q1:a]");

    assertThat(DefaultKey.encode(new Object[] { "a" })).isNotEqualTo(DefaultKey.encode(new Object[] { 'a' }));
    assertThat(DefaultKey.encode(new Object[] { "a" })).isNotEqualTo(
      DefaultKey.encode(new Object[] { new StringBuilder("a") })
    );
  }

  @Test
  @DisplayName("length prefixes keep argument boundaries: (\"foo, bar\", \"baz\") differs from (\"foo\", \"bar, baz\")")
  void argumentBoundariesAreUnambiguous() {
    String commaInsideArgument = DefaultKey.encode(new Object[] { "foo, bar", "baz" });
    String commaInsideLastArgument = DefaultKey.encode(new Object[] { "foo", "bar, baz" });

    assertThat(commaInsideArgument).isEqualTo("[s8:foo, bar,s3:baz]");
    assertThat(commaInsideLastArgument).isEqualTo("[s3:foo,s8:bar, baz]");
    assertThat(commaInsideArgument).isNotEqualTo(commaInsideLastArgument);
  }

  @Test
  @DisplayName("the no-argument key is disjoint from every single-argument key")
  void noArgumentsDisjointFromSingleArgument() {
    Object[] singles = { "", "[]", 0, 'a', new Object[0], new String[0], new int[0] };

    for (Object single : singles) {
      assertThat(DefaultKey.encode(new Object[] { single })).isNotEqualTo("[]");
    }
  }

  @Test
  @DisplayName("arrays are encoded by content, never by identity hash")
  void arraysUseContentNotIdentity() {
    String[] first = { "a", "b" };
    String[] second = { "a", "b" };
    String[] different = { "a", "c" };

    assertThat(first).isNotSameAs(second);
    assertThat(DefaultKey.encode(new Object[] { first })).isEqualTo("[aO[s1:a,s1:b]]");
    assertThat(DefaultKey.encode(new Object[] { first })).isEqualTo(DefaultKey.encode(new Object[] { second }));
    assertThat(DefaultKey.encode(new Object[] { first })).isNotEqualTo(DefaultKey.encode(new Object[] { different }));
  }

  @Test
  @DisplayName("primitive arrays differ by their component code")
  void primitiveArraysDifferByComponentCode() {
    assertThat(DefaultKey.encode(new Object[] { new byte[] { 1 } })).isEqualTo("[aB[B:1]]");
    assertThat(DefaultKey.encode(new Object[] { new short[] { 1 } })).isEqualTo("[aS[S:1]]");
    assertThat(DefaultKey.encode(new Object[] { new int[] { 1 } })).isEqualTo("[aI[I:1]]");
    assertThat(DefaultKey.encode(new Object[] { new long[] { 1 } })).isEqualTo("[aJ[J:1]]");
    assertThat(DefaultKey.encode(new Object[] { new float[] { 1.0f } })).isEqualTo("[aF[F:1.0]]");
    assertThat(DefaultKey.encode(new Object[] { new double[] { 1.0 } })).isEqualTo("[aD[D:1.0]]");
    assertThat(DefaultKey.encode(new Object[] { new char[] { 'a' } })).isEqualTo("[aC[c1:a]]");
    assertThat(DefaultKey.encode(new Object[] { new boolean[] { true } })).isEqualTo("[aZ[Z:true]]");

    assertThat(DefaultKey.encode(new Object[] { new int[] { 1 } })).isNotEqualTo(
      DefaultKey.encode(new Object[] { new byte[] { 1 } })
    );
  }

  @Test
  @DisplayName("empty primitive arrays keep their component tag")
  void emptyPrimitiveArrays() {
    assertThat(DefaultKey.encode(new Object[] { new int[0] })).isEqualTo("[aI[]]");
    assertThat(DefaultKey.encode(new Object[] { new char[0] })).isEqualTo("[aC[]]");
  }

  @Test
  @DisplayName("nested arrays recurse with the same part rules")
  void nestedArrays() {
    Object[] nested = { "x", new int[] { 1, 2 } };

    assertThat(DefaultKey.encode(new Object[] { nested })).isEqualTo("[aO[s1:x,aI[I:1,I:2]]]");
  }

  @Test
  @DisplayName("a null array element encodes as n")
  void nullArrayElement() {
    assertThat(DefaultKey.encode(new Object[] { new Object[] { "a", null } })).isEqualTo("[aO[s1:a,n]]");
  }

  @Test
  @DisplayName("char elements that are delimiters stay unambiguous")
  void charElementsThatLookLikeDelimiters() {
    char[] commaThenBracket = { ',', ']' };
    char[] bracketThenComma = { ']', ',' };

    assertThat(DefaultKey.encode(new Object[] { commaThenBracket })).isEqualTo("[aC[c1:,,c1:]]]");
    assertThat(DefaultKey.encode(new Object[] { commaThenBracket })).isNotEqualTo(
      DefaultKey.encode(new Object[] { bracketThenComma })
    );
    assertThat(DefaultKey.encode(new Object[] { ",]" })).isNotEqualTo(
      DefaultKey.encode(new Object[] { commaThenBracket })
    );
  }

  @Test
  @DisplayName("String \"1\" and Integer 1 stay distinct")
  void stringAndIntegerStayDistinct() {
    assertThat(DefaultKey.encode(new Object[] { "1" })).isEqualTo("[s1:1]");
    assertThat(DefaultKey.encode(new Object[] { 1 })).isEqualTo("[I:1]");
    assertThat(DefaultKey.encode(new Object[] { "1" })).isNotEqualTo(DefaultKey.encode(new Object[] { 1 }));
  }

  @Test
  @DisplayName("numeric types with equal text stay distinct")
  void numericTypesStayDistinct() {
    assertThat(DefaultKey.encode(new Object[] { (byte) 1 })).isEqualTo("[B:1]");
    assertThat(DefaultKey.encode(new Object[] { (short) 1 })).isEqualTo("[S:1]");
    assertThat(DefaultKey.encode(new Object[] { 1 })).isEqualTo("[I:1]");
    assertThat(DefaultKey.encode(new Object[] { 1L })).isEqualTo("[J:1]");
    assertThat(DefaultKey.encode(new Object[] { 1.0f })).isEqualTo("[F:1.0]");
    assertThat(DefaultKey.encode(new Object[] { 1.0d })).isEqualTo("[D:1.0]");
    assertThat(DefaultKey.encode(new Object[] { new java.math.BigInteger("1") })).isEqualTo("[N:1]");
    assertThat(DefaultKey.encode(new Object[] { new java.math.BigDecimal("1") })).isEqualTo("[E:1]");
    assertThat(DefaultKey.encode(new Object[] { true })).isEqualTo("[Z:true]");

    assertThat(DefaultKey.encode(new Object[] { 1 }))
      .isNotEqualTo(DefaultKey.encode(new Object[] { 1L }))
      .isNotEqualTo(DefaultKey.encode(new Object[] { new java.math.BigDecimal("1") }))
      .isNotEqualTo(DefaultKey.encode(new Object[] { 1.0d }));
  }

  @Test
  @DisplayName("negative zero renders as zero: a numerically equal argument addresses one key")
  void negativeZeroIsNormalized() {
    assertThat(DefaultKey.encode(new Object[] { -0.0 })).isEqualTo("[D:0.0]");
    assertThat(DefaultKey.encode(new Object[] { 0.0 })).isEqualTo("[D:0.0]");
    assertThat(DefaultKey.encode(new Object[] { -0.0f })).isEqualTo("[F:0.0]");
    assertThat(DefaultKey.encode(new Object[] { 0.0f })).isEqualTo("[F:0.0]");
    assertThat(DefaultKey.encode(new Object[] { new double[] { -0.0 } })).isEqualTo("[aD[D:0.0]]");
    assertThat(DefaultKey.encode(new Object[] { new float[] { -0.0f } })).isEqualTo("[aF[F:0.0]]");
  }

  @Test
  @DisplayName("a directly self-referential array fails with a configuration error instead of overflowing the stack")
  void directSelfReferenceIsRejected() {
    Object[] self = new Object[1];
    self[0] = self;

    assertThatThrownBy(() -> DefaultKey.encode(new Object[] { self }))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("self-referential array")
      .hasMessageContaining("explicit key");
  }

  @Test
  @DisplayName("an indirect array cycle is rejected at the first repeated array")
  void indirectCycleIsRejected() {
    Object[] first = new Object[1];
    Object[] second = new Object[1];
    first[0] = second;
    second[0] = first;

    assertThatThrownBy(() -> DefaultKey.encode(new Object[] { first }))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("cyclic array")
      .hasMessageContaining("explicit key");
  }

  @Test
  @DisplayName("a three-array cycle is rejected at the repeated ancestor")
  void threeArrayCycleIsRejected() {
    Object[] first = new Object[1];
    Object[] second = new Object[1];
    Object[] third = new Object[1];
    first[0] = second;
    second[0] = third;
    third[0] = first;

    assertThatThrownBy(() -> DefaultKey.encode(new Object[] { first }))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("cyclic array")
      .hasMessageContaining("explicit key");
  }

  @Test
  @DisplayName("a branching indirect cycle is rejected instead of expanding every branch")
  void branchingIndirectCycleIsRejected() {
    Object[] first = new Object[2];
    Object[] second = new Object[] { first, first };
    first[0] = second;
    first[1] = second;

    assertThatThrownBy(() -> DefaultKey.encode(new Object[] { first }))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("cyclic array")
      .hasMessageContaining("explicit key");
  }

  @Test
  @DisplayName("a branching cycle longer than the depth cap still fails at the depth cap")
  void longBranchingCycleIsBoundedByDepth() {
    int length = 40;
    Object[][] ring = new Object[length][2];
    for (int i = 0; i < length; i++) {
      ring[i][0] = ring[(i + 1) % length];
      ring[i][1] = ring[(i + 1) % length];
    }

    assertThatThrownBy(() -> DefaultKey.encode(new Object[] { ring[0] }))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("32 nested array levels")
      .hasMessageContaining("explicit key");
  }

  @Test
  @DisplayName("a shared child array is a DAG, not a cycle, and stays encodable")
  void sharedChildArrayIsNotACycle() {
    Object[] shared = { "x" };
    Object[] root = { shared, shared };

    assertThat(DefaultKey.encode(new Object[] { root })).isEqualTo("[aO[aO[s1:x],aO[s1:x]]]");
  }

  @Test
  @DisplayName("rows of a two-dimensional reference array share one ancestor chain without false positives")
  void twoDimensionalObjectArray() {
    Object[][] rows = { { "a" }, { "b" } };

    assertThat(DefaultKey.encode(new Object[] { rows })).isEqualTo("[aO[aO[s1:a],aO[s1:b]]]");
  }

  @Test
  @DisplayName("the maximum accepted nesting depth is 32 array levels")
  void maximumNestingDepthIsAccepted() {
    Object[] nested = new Object[0];
    for (int i = 1; i < 32; i++) {
      nested = new Object[] { nested };
    }

    String encoded = DefaultKey.encode(new Object[] { nested });

    assertThat(encoded).startsWith("[aO[aO[");
    assertThat(encoded).endsWith("]]");
  }

  @Test
  @DisplayName("a 33rd array level is rejected")
  void nestingBeyondTheLimitIsRejected() {
    Object[] nested = new Object[0];
    for (int i = 0; i < 32; i++) {
      nested = new Object[] { nested };
    }
    Object[] tooDeep = nested;

    assertThatThrownBy(() -> DefaultKey.encode(new Object[] { tooDeep }))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("32 nested array levels");
  }
}
