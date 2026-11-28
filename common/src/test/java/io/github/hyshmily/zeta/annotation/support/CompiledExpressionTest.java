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
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;

@DisplayName("CompiledExpression")
class CompiledExpressionTest {

  private static final ZetaEvaluationContext.ExpressionSupport SUPPORT =
    ZetaEvaluationContext.ExpressionSupport.defaults();

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
    assertThat(
      CompiledExpression.of(null, parser, SUPPORT.emptyContext(), "key", method, bindings(method), true).absent()
    ).isTrue();
    assertThat(
      CompiledExpression.of("", parser, SUPPORT.emptyContext(), "key", method, bindings(method), true).absent()
    ).isTrue();
    assertThat(
      CompiledExpression.of("   ", parser, SUPPORT.emptyContext(), "unless", method, bindings(method), true).absent()
    ).isTrue();
    assertThat(
      CompiledExpression.of("\t\n", parser, SUPPORT.emptyContext(), "unless", method, bindings(method), true).absent()
    ).isTrue();
  }

  @Test
  @DisplayName("surrounding whitespace is stripped at this entry point")
  void whitespaceIsStripped() {
    CompiledExpression compiled = CompiledExpression.of(
      "  #p0  ",
      parser,
      SUPPORT.emptyContext(),
      "key",
      method,
      bindings(method),
      true
    );

    assertThat(compiled.absent()).isFalse();
    assertThat(compiled.raw()).isEqualTo("#p0");
  }

  @Test
  @DisplayName("a malformed attribute fails resolution with the role and method")
  void malformedFailsResolution() {
    assertThatThrownBy(() ->
      CompiledExpression.of("#{", parser, SUPPORT.emptyContext(), "cache key", method, bindings(method), true)
    )
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("cache key")
      .hasMessageContaining("cannot be parsed")
      .hasMessageContaining("fixture");
  }

  @Test
  @DisplayName("a present expression evaluates")
  void presentEvaluates() {
    CompiledExpression compiled = CompiledExpression.of(
      "1 + 1",
      parser,
      SUPPORT.emptyContext(),
      "key",
      method,
      bindings(method),
      true
    );

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
  @DisplayName("only canonical #pN/#aN names map to an index")
  void argumentIndexResolvesPosition() {
    assertThat(CompiledExpression.argumentIndex("p0")).isZero();
    assertThat(CompiledExpression.argumentIndex("a7")).isEqualTo(7);
    assertThat(CompiledExpression.argumentIndex("p01")).isEqualTo(-1);
    assertThat(CompiledExpression.argumentIndex("p")).isEqualTo(-1);
    assertThat(CompiledExpression.argumentIndex("pX")).isEqualTo(-1);
    assertThat(CompiledExpression.argumentIndex("p2147483648")).isEqualTo(-1);
    assertThat(CompiledExpression.argumentIndex("result")).isEqualTo(-1);
  }

  @Test
  @DisplayName("an out-of-range positional reference fails resolution with its role and method")
  void outOfRangePositionalReferenceFailsResolution() {
    assertThatThrownBy(() ->
      CompiledExpression.of("#p1", parser, SUPPORT.emptyContext(), "hardTtl", method, bindings(method), true)
    )
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("hardTtl")
      .hasMessageContaining("addresses parameter 1")
      .hasMessageContaining("declares 1");
    assertThatThrownBy(() ->
      CompiledExpression.of("#a1", parser, SUPPORT.emptyContext(), "unless", method, bindings(method), true)
    )
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
    assertThatThrownBy(() ->
      CompiledExpression.of(
        "#userId",
        parser,
        SUPPORT.emptyContext(),
        "key",
        method,
        ParameterBindings.of(method, null),
        true
      )
    )
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("parameter names are not available")
      .hasMessageContaining("-parameters")
      .hasMessageContaining("#p0");
    assertThat(
      CompiledExpression.of(
        "#p0",
        parser,
        SUPPORT.emptyContext(),
        "key",
        method,
        ParameterBindings.of(method, null),
        true
      ).absent()
    ).isFalse();
    assertThat(
      CompiledExpression.of(
        "#root.args[0]",
        parser,
        SUPPORT.emptyContext(),
        "key",
        method,
        ParameterBindings.of(method, null),
        true
      ).absent()
    ).isFalse();
    assertThat(
      CompiledExpression.of(
        "#result",
        parser,
        SUPPORT.emptyContext(),
        "unless",
        method,
        ParameterBindings.of(method, null),
        true
      ).absent()
    ).isFalse();
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

    assertThat(
      CompiledExpression.of(
        "#p0",
        parser,
        SUPPORT.emptyContext(),
        "key",
        withResult,
        bindings(withResult),
        false
      ).absent()
    ).isFalse();
    assertThat(
      CompiledExpression.of(
        "#result",
        parser,
        SUPPORT.emptyContext(),
        "key",
        withResult,
        bindings(withResult),
        true
      ).absent()
    ).isFalse();
  }

  @Test
  @DisplayName("a non-canonical name is legal when a parameter actually has that name")
  void nonCanonicalParameterNameIsAllowed() throws NoSuchMethodException {
    Method methodWithP01 = CompiledExpressionTest.class.getDeclaredMethod("fixtureWithNonCanonicalName", String.class);

    assertThat(
      CompiledExpression.of(
        "#p01",
        parser,
        SUPPORT.emptyContext(),
        "key",
        methodWithP01,
        bindings(methodWithP01),
        true
      ).absent()
    ).isFalse();
  }

  @Test
  @DisplayName("a selection/projection condition reads its element, not the invocation root")
  void elementScopedAccessIsNotAnInvocationDependency() {
    assertThat(compiled("#result.items.?[active].isEmpty()").referencesInvocationContext()).isFalse();
    assertThat(compiled("#result.?[#this.valid].isEmpty()").referencesInvocationContext()).isFalse();
    assertThat(compiled("#result.![name]").referencesInvocationContext()).isFalse();
    // The selection itself still applies to the root before it enters the element scope.
    assertThat(compiled("?[active]").referencesInvocationContext()).isTrue();
    // #root stays the invocation root inside the element scope.
    assertThat(compiled("#result.?[#root.args[0] != null]").referencesInvocationContext()).isTrue();
  }

  @Test
  @DisplayName("referencesResult sees #result but not text inside a string literal")
  void referencesResultSkipsStringLiterals() {
    assertThat(compiled("#result.id").referencesResult()).isTrue();
    assertThat(compiled("#result").referencesResult()).isTrue();
    assertThat(compiled("#p0 == #result").referencesResult()).isTrue();

    assertThat(compiled("'#resultX'").referencesResult()).isFalse();
    assertThat(compiled("#p0").referencesResult()).isFalse();
    assertThat(compiled("'#result'").referencesResult()).isFalse();
    assertThat(compiled("#p0 == 'it''s #result'").referencesResult()).isFalse();
    assertThat(compiled("\"#result\"").referencesResult()).isFalse();
    assertThat(compiled("\"it\"\"s #result\"").referencesResult()).isFalse();
    assertThat(compiled("'say \"hi\" #result'").referencesResult()).isFalse();
    assertThat(compiled("\"#result\" + #result").referencesResult()).isTrue();
  }

  @Test
  @DisplayName("only an AST of literals and operators is a pure constant")
  void pureConstantClassification() {
    assertThat(compiled("1 + 1").isPureConstant()).isTrue();
    assertThat(compiled("'v1:' + 1").isPureConstant()).isTrue();
    assertThat(compiled("true ? 'a' : 'b'").isPureConstant()).isTrue();
    assertThat(compiled("#p0").isPureConstant()).isFalse();
    assertThat(compiled("#root.args[0]").isPureConstant()).isFalse();
    assertThat(compiled("@cacheConfig").isPureConstant()).isFalse();
    assertThat(compiled("T(java.lang.Math).abs(-1)").isPureConstant()).isFalse();
    assertThat(compiled("new java.math.BigDecimal('1')").isPureConstant()).isFalse();
    assertThat(compiled("'abc'.length()").isPureConstant()).isFalse();
    assertThat(CompiledExpression.ABSENT.isPureConstant()).isFalse();
  }

  @Test
  @DisplayName("an unquoted inline-map key is a literal name, not a #root read")
  void inlineMapKeyIsNotAnInvocationDependency() {
    CompiledExpression map = compiled("{ACTIVE: false, DRAFT: true}");

    assertThat(map.referencesInvocationContext()).isFalse();
    assertThat(map.isPureConstant()).isTrue();
    assertThat(map.getValue(null)).isEqualTo(Map.of("ACTIVE", false, "DRAFT", true));
  }

  @Test
  @DisplayName("the empty context is one read-only instance per support")
  void emptyContextIsReadOnly() {
    EvaluationContext context = SUPPORT.emptyContext();

    assertThat(context).isSameAs(SUPPORT.emptyContext());
    assertThatThrownBy(() -> context.setVariable("x", 1)).isInstanceOf(UnsupportedOperationException.class);
    assertThat(context.lookupVariable("x")).isNull();
    assertThat(compiled("T(java.lang.String).valueOf(1)").getValue(null)).isEqualTo("1");
  }

  @Test
  @DisplayName("assignment and increment/decrement fail at resolution, not on the first call")
  void assignmentIsRejectedAtResolution() {
    assertThatThrownBy(() -> compiled("#id = 'x'"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("read-only");
    assertThatThrownBy(() -> compiled("#id++"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("read-only");
    assertThatThrownBy(() -> compiled("#id--"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("read-only");
  }

  private ParameterBindings bindings(Method target) {
    return ParameterBindings.of(target, parameterNames.getParameterNames(target));
  }

  private CompiledExpression compiled(String raw) {
    return CompiledExpression.of(raw, parser, SUPPORT.emptyContext(), "key", method, bindings(method), true);
  }

  private CompiledExpression preBody(String raw) {
    return CompiledExpression.of(raw, parser, SUPPORT.emptyContext(), "cache key", method, bindings(method), false);
  }
}
