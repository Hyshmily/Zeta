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

import java.lang.reflect.Method;
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
  @DisplayName("the AST flags are collected from the parsed tree, not from literal text")
  void flagsComeFromTheAst() {
    assertThat(compiled("#p0 == #result").referencesResult()).isTrue();
    assertThat(compiled("'#result' + \"#p0\"").referencesResult()).isFalse();
    assertThat(compiled("#root.args[0]").referencesResult()).isFalse();

    assertThat(compiled("#root.args").wholeRootArgs()).isTrue();
    assertThat(compiled("#root.args[0]").wholeRootArgs()).isFalse();
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
  @DisplayName("a named variable without parameter metadata fails resolution instead of resolving to null")
  void namedVariableWithoutParameterNamesFailsResolution() {
    assertThatThrownBy(() -> CompiledExpression.of("#userId", parser, "key", method, null, true))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("parameter names are not available")
      .hasMessageContaining("-parameters")
      .hasMessageContaining("#p0");
    assertThat(CompiledExpression.of("#p0", parser, "key", method, null, true).absent()).isFalse();
    assertThat(CompiledExpression.of("#root.args[0]", parser, "key", method, null, true).absent()).isFalse();
    assertThat(CompiledExpression.of("#result", parser, "unless", method, null, true).absent()).isFalse();
  }

  @Test
  @DisplayName("a SpEL function call is rejected: the evaluation context registers no functions")
  void functionCallsAreRejected() {
    assertThatThrownBy(() -> compiled("#hash(#p0)"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("cannot call SpEL function")
      .hasMessageContaining("no functions are registered")
      .hasMessageContaining("fixture");
  }

  @Test
  @DisplayName("root-relative indexer, selection and projection are dynamic, never folded as constants")
  void rootRelativeCollectionNodesAreDynamic() {
    assertThat(compiled("[0]").constant()).isFalse();
    assertThat(compiled("?[#this != null]").constant()).isFalse();
    assertThat(compiled("![#this]").constant()).isFalse();
  }

  @Test
  @DisplayName("every assignment form is rejected: cache expressions are read-only")
  void assignmentsAreRejected() {
    assertThatThrownBy(() -> compiled("#x = 1"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("must not contain assignments")
      .hasMessageContaining("read-only");
    assertThatThrownBy(() -> compiled("#result = 1"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("must not contain assignments");
    assertThatThrownBy(() -> compiled("#id.foo = #typo"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("must not contain assignments");
    assertThatThrownBy(() -> compiled("(#x = 1) + #x"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("must not contain assignments");
    assertThatThrownBy(() -> compiled("#x++"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("must not contain assignments")
      .hasMessageContaining("read-only");
    assertThatThrownBy(() -> compiled("#x--"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("must not contain assignments");
    assertThatThrownBy(() -> compiled("#root.args[0].age++"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("must not contain assignments");
  }

  @Test
  @DisplayName("a context-free expression is marked constant; any variable or access keeps it dynamic")
  void constantExpressionsAreMarked() {
    assertThat(compiled("60000L").constant()).isTrue();
    assertThat(compiled("60 * 1000").constant()).isTrue();
    assertThat(compiled("'literal'").constant()).isTrue();
    assertThat(compiled("#p0").constant()).isFalse();
    assertThat(compiled("#p0 + 1").constant()).isFalse();
    assertThat(compiled("T(java.lang.Long).MAX_VALUE").constant()).isFalse();
    assertThat(compiled("new java.lang.String('x')").constant()).isFalse();
  }

  @Test
  @DisplayName("a context-free expression is folded once and a failure names its role and method")
  void constantExpressionsAreFoldedOnce() {
    assertThat(compiled("60 * 1000").evaluateConstant()).isEqualTo(60_000);
    assertThat(compiled("#p0").evaluateConstant()).isNull();
    assertThat(compiled("null").evaluateConstant()).isNull();
    assertThat(CompiledExpression.ABSENT.evaluateConstant()).isNull();

    assertThatThrownBy(() ->
      CompiledExpression.of("1 / 0", parser, "hardTtl", method, names(method), true)
    )
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("hardTtl")
      .hasMessageContaining("failed constant evaluation")
      .hasMessageContaining("fixture");
  }

  @Test
  @DisplayName("a pre-body #result is rejected")
  void resultIsRejectedBeforeTheBody() {
    assertThatThrownBy(() -> preBody("#result.id"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("cache key")
      .hasMessageContaining("#result")
      .hasMessageContaining("does not bind a method result");
  }

  @Test
  @DisplayName("a parameter named result is legal; #result stays the bound body result")
  void resultNamedParameterIsAllowed() throws NoSuchMethodException {
    Method withResult = CompiledExpressionTest.class.getDeclaredMethod("fixtureWithResultParameter", String.class);

    assertThat(CompiledExpression.of("#p0", parser, "key", withResult, names(withResult), false).absent()).isFalse();
    assertThat(CompiledExpression.of("#result", parser, "key", withResult, names(withResult), true).absent()).isFalse();
    assertThat(CompiledExpression.argumentIndex("result", names(withResult))).isEqualTo(-1);
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
  @DisplayName("referencesResult sees #result but not text inside a string literal")
  void referencesResultSkipsStringLiterals() {
    assertThat(compiled("#result.id").referencesResult()).isTrue();
    assertThat(compiled("#result").referencesResult()).isTrue();
    assertThat(compiled("#p0 == #result").referencesResult()).isTrue();

    assertThat(unchecked("#resultX").referencesResult()).isFalse();
    assertThat(compiled("#p0").referencesResult()).isFalse();
    assertThat(compiled("'#result'").referencesResult()).isFalse();
    assertThat(compiled("#p0 == 'it''s #result'").referencesResult()).isFalse();
    assertThat(compiled("\"#result\"").referencesResult()).isFalse();
    assertThat(compiled("\"it\"\"s #result\"").referencesResult()).isFalse();
    assertThat(compiled("'say \"hi\" #result'").referencesResult()).isFalse();
    assertThat(compiled("\"#result\" + #result").referencesResult()).isTrue();
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
    return new CompiledExpression.Present(raw, parser.parseExpression(raw), false, false, false, null);
  }
}
