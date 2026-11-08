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

import java.lang.reflect.Method;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.expression.spel.standard.SpelExpressionParser;

@DisplayName("CompiledExpression")
class CompiledExpressionTest {

  private final SpelExpressionParser parser = new SpelExpressionParser();

  private final ParameterNameDiscoverer parameterNames = new DefaultParameterNameDiscoverer();

  private Method method;

  @BeforeEach
  void setUp() throws NoSuchMethodException {
    method = CompiledExpressionTest.class.getDeclaredMethod("fixture", String.class);
  }

  @SuppressWarnings("unused")
  private String fixture(String id) {
    return id;
  }

  @SuppressWarnings("unused")
  private String fixtureWithNonCanonicalName(String p01) {
    return p01;
  }

  @SuppressWarnings("unused")
  private String fixtureWithPositionalName(String p1) {
    return p1;
  }

  @SuppressWarnings("unused")
  private String fixtureWithResultParameter(String result) {
    return result;
  }

  @Test
  @DisplayName("a null, empty or whitespace-only attribute is absent, not a parse failure")
  void emptyIsAbsent() {
    assertThat(CompiledExpression.of(null, parser, "key", method, names(method), true).absent()).isTrue();
    assertThat(CompiledExpression.of("", parser, "key", method, names(method), true).absent()).isTrue();
    assertThat(CompiledExpression.of("   ", parser, "unless", method, names(method), true).absent()).isTrue();
    assertThat(CompiledExpression.of("\t\n", parser, "unless", method, names(method), true).absent()).isTrue();
  }

  @Test
  @DisplayName("surrounding whitespace is stripped at this entry point")
  void whitespaceIsStripped() {
    CompiledExpression compiled = CompiledExpression.of("  #p0  ", parser, "key", method, names(method), true);

    assertThat(compiled.absent()).isFalse();
    assertThat(compiled.raw()).isEqualTo("#p0");
  }

  @Test
  @DisplayName("a malformed attribute fails resolution with the role and method")
  void malformedFailsResolution() {
    assertThatThrownBy(() -> CompiledExpression.of("#{", parser, "cache key", method, names(method), true))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("cache key")
      .hasMessageContaining("cannot be parsed")
      .hasMessageContaining("fixture");
  }

  @Test
  @DisplayName("a present expression evaluates")
  void presentEvaluates() {
    CompiledExpression compiled = CompiledExpression.of("1 + 1", parser, "key", method, names(method), true);

    assertThat(compiled.absent()).isFalse();
    assertThat(
      compiled.getValue(new org.springframework.expression.spel.support.StandardEvaluationContext())
    ).isEqualTo(2);
  }

  @Test
  @DisplayName("variables are collected from the parsed AST, not from literal text")
  void variablesComeFromTheAst() {
    CompiledExpression cached = compiled("#p0 == #result");

    assertThat(cached.variables()).containsExactlyInAnyOrder("p0", "result");
    assertThat(compiled("'#result' + \"#p0\"").variables()).isEmpty();
    assertThat(compiled("#root.args[0]").variables()).containsExactly("root");
  }

  @Test
  @DisplayName("only canonical #pN/#aN names map to an index, and a discovered parameter name maps to its position")
  void argumentIndexResolvesPositionAndName() {
    assertThat(CompiledExpression.argumentIndex("p0")).isZero();
    assertThat(CompiledExpression.argumentIndex("a7")).isEqualTo(7);
    assertThat(CompiledExpression.argumentIndex("p01")).isEqualTo(-1);
    assertThat(CompiledExpression.argumentIndex("p")).isEqualTo(-1);
    assertThat(CompiledExpression.argumentIndex("pX")).isEqualTo(-1);
    assertThat(CompiledExpression.argumentIndex("p2147483648")).isEqualTo(-1);
    assertThat(CompiledExpression.argumentIndex("result")).isEqualTo(-1);

    assertThat(CompiledExpression.argumentIndex("id", new String[] { "id" })).isZero();
    assertThat(CompiledExpression.argumentIndex("page", new String[] { "id", "page" })).isEqualTo(1);
    assertThat(CompiledExpression.argumentIndex("p1", new String[] { "id" })).isEqualTo(1);
    assertThat(CompiledExpression.argumentIndex("missing", new String[] { "id" })).isEqualTo(-1);
    assertThat(CompiledExpression.argumentIndex("id", null)).isEqualTo(-1);
  }

  @Test
  @DisplayName("an out-of-range positional reference fails resolution with its role and method")
  void outOfRangePositionalReferenceFailsResolution() {
    assertThatThrownBy(() -> CompiledExpression.of("#p1", parser, "hardTtl", method, names(method), true))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("hardTtl")
      .hasMessageContaining("addresses parameter 1")
      .hasMessageContaining("declares 1");
    assertThatThrownBy(() -> CompiledExpression.of("#a1", parser, "unless", method, names(method), true))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("unless")
      .hasMessageContaining("addresses parameter 1");
  }

  @Test
  @DisplayName("an in-range or literal positional reference is accepted")
  void inRangeAndLiteralPositionalReferencesAreAccepted() {
    assertThat(compiled("#p0").absent()).isFalse();
    assertThat(compiled("#a0").absent()).isFalse();
    assertThat(compiled("'#p1'").absent()).isFalse();
  }

  @Test
  @DisplayName("a variable the context cannot bind fails resolution when parameter names are known")
  void unknownVariableFailsResolution() {
    assertThatThrownBy(() -> compiled("#reslt"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("key")
      .hasMessageContaining("unknown variable '#reslt'")
      .hasMessageContaining("fixture");
    assertThatThrownBy(() -> compiled("#p01"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("unknown variable '#p01'");
  }

  @Test
  @DisplayName("an expression that assigns its own variable skips only the unknown-name check")
  void assignmentSkipsUnknownNameValidation() {
    assertThat(compiled("#x = 1").absent()).isFalse();
    assertThat(compiled("#x = #id").absent()).isFalse();
    assertThatThrownBy(() -> compiled("#x = #p1"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("addresses parameter 1");
  }

  @Test
  @DisplayName("a property assignment does not exempt its right-hand side from validation")
  void propertyAssignmentStillValidatesUnknownNames() {
    assertThatThrownBy(() -> compiled("#id.foo = #typo"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("unknown variable '#typo'");
    assertThatThrownBy(() -> compiled("#x = #typo"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("unknown variable '#typo'");
  }

  @Test
  @DisplayName("a pre-body #result is rejected")
  void resultIsRejectedBeforeTheBody() {
    assertThatThrownBy(() -> preBody("#result.id"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("cache key")
      .hasMessageContaining("#result")
      .hasMessageContaining("before the method body");
  }

  @Test
  @DisplayName("a parameter named result is rejected because #result is reserved")
  void resultNamedParameterIsRejected() throws NoSuchMethodException {
    Method withResult = CompiledExpressionTest.class.getDeclaredMethod("fixtureWithResultParameter", String.class);

    assertThatThrownBy(() -> CompiledExpression.of("#result", parser, "key", withResult, names(withResult), false))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("reserved parameter 'result'");
  }

  @Test
  @DisplayName("a non-canonical name is legal when a parameter actually has that name")
  void nonCanonicalParameterNameIsAllowed() throws NoSuchMethodException {
    Method methodWithP01 = CompiledExpressionTest.class.getDeclaredMethod("fixtureWithNonCanonicalName", String.class);

    assertThat(
      CompiledExpression.of("#p01", parser, "key", methodWithP01, names(methodWithP01), true).absent()
    ).isFalse();
  }

  @Test
  @DisplayName("a parameter named p1 is rejected because positional bindings are reserved")
  void positionalParameterNameIsRejected() throws NoSuchMethodException {
    Method methodWithP1 = CompiledExpressionTest.class.getDeclaredMethod("fixtureWithPositionalName", String.class);

    assertThatThrownBy(() -> CompiledExpression.of("#p1", parser, "key", methodWithP1, names(methodWithP1), true))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("reserved parameter 'p1'");
  }

  @Test
  @DisplayName("references sees a variable but not text inside a string literal")
  void referencesSkipsStringLiterals() {
    assertThat(compiled("#result.id").references("result")).isTrue();
    assertThat(compiled("#result").references("result")).isTrue();
    assertThat(compiled("#p0 == #result").references("result")).isTrue();

    assertThat(unchecked("#resultX").references("result")).isFalse();
    assertThat(compiled("#p0").references("result")).isFalse();
    assertThat(compiled("'#result'").references("result")).isFalse();
    assertThat(compiled("#p0 == 'it''s #result'").references("result")).isFalse();
    assertThat(compiled("\"#result\"").references("result")).isFalse();
    assertThat(compiled("\"it\"\"s #result\"").references("result")).isFalse();
    assertThat(compiled("'say \"hi\" #result'").references("result")).isFalse();
    assertThat(compiled("\"#result\" + #result").references("result")).isTrue();
  }

  private String@Nullable [] names(Method target) {
    return parameterNames.getParameterNames(target);
  }

  private CompiledExpression compiled(String raw) {
    return CompiledExpression.of(raw, parser, "key", method, names(method), true);
  }

  private CompiledExpression preBody(String raw) {
    return CompiledExpression.of(raw, parser, "cache key", method, names(method), false);
  }

  private CompiledExpression unchecked(String raw) {
    return new CompiledExpression(raw, parser.parseExpression(raw), Set.of("resultX"));
  }
}
