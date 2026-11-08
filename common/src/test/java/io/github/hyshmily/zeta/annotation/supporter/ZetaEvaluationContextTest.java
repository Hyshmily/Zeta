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

import java.lang.reflect.Method;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

@DisplayName("ZetaEvaluationContext")
class ZetaEvaluationContextTest {

  private final SpelExpressionParser parser = new SpelExpressionParser();

  private final ParameterNameDiscoverer parameterNames = new DefaultParameterNameDiscoverer();

  private final Fixture target = new Fixture();

  private final Object[] arguments = { "ACME", 7 };

  private final ZetaEvaluationContext.Support support = ZetaEvaluationContext.Support.defaults();

  private Method method;

  @BeforeEach
  void setUp() throws NoSuchMethodException {
    method = Fixture.class.getDeclaredMethod("find", String.class, int.class);
  }

  static class Fixture {

    public String find(String id, int limit) {
      return id + ":" + limit;
    }

    public String empty() {
      return "empty";
    }

    public String join(String... ids) {
      return String.join(",", ids);
    }
  }

  private ZetaEvaluationContext context() {
    return ZetaEvaluationContext.of(
      ZetaEvaluationContext.Root.of(target, Fixture.class, method, arguments),
      parameterNames.getParameterNames(method),
      null,
      support
    );
  }

  private Object evaluate(String expression) {
    return parser.parseExpression(expression).getValue(context());
  }

  @Test
  @DisplayName("#root exposes methodName, method, args, target and targetClass")
  void rootExposesTheInvocation() {
    ZetaEvaluationContext context = context();

    assertThat(parser.parseExpression("#root.methodName").getValue(context)).isEqualTo("find");
    assertThat(parser.parseExpression("#root.method").getValue(context)).isSameAs(method);
    assertThat(parser.parseExpression("#root.args[0]").getValue(context)).isEqualTo("ACME");
    assertThat(parser.parseExpression("#root.args[1]").getValue(context)).isEqualTo(7);
    assertThat(parser.parseExpression("#root.target").getValue(context)).isSameAs(target);
    assertThat(parser.parseExpression("#root.targetClass").getValue(context)).isSameAs(Fixture.class);
    assertThat(context.root()).isSameAs(context.getRootObject().getValue());
  }

  @Test
  @DisplayName("arguments bind by parameter name and by #p0/#a0")
  void argumentsBindByNameAndPosition() {
    assertThat(evaluate("#id")).isEqualTo("ACME");
    assertThat(evaluate("#limit")).isEqualTo(7);
    assertThat(evaluate("#p0")).isEqualTo("ACME");
    assertThat(evaluate("#a0")).isEqualTo("ACME");
    assertThat(evaluate("#p1")).isEqualTo(7);
    assertThat(evaluate("#a1")).isEqualTo(7);
  }

  @Test
  @DisplayName("withResult binds #result without replacing the root")
  void withResultBindsResult() {
    ZetaEvaluationContext context = context().withResult("loaded");

    assertThat(parser.parseExpression("#result").getValue(context)).isEqualTo("loaded");
    assertThat(parser.parseExpression("#root.methodName").getValue(context)).isEqualTo("find");
  }

  @Test
  @DisplayName("withResult(null) binds a null result")
  void withResultBindsNull() {
    ZetaEvaluationContext context = context().withResult(null);

    assertThat(parser.parseExpression("#result").getValue(context)).isNull();
    assertThat(parser.parseExpression("#root.methodName").getValue(context)).isEqualTo("find");
  }

  @Test
  @DisplayName("an assigned null shadows the same-named argument")
  void assignedNullShadowsArgument() {
    ZetaEvaluationContext context = context();

    assertThat(parser.parseExpression("#id = null").getValue(context)).isNull();
    assertThat(parser.parseExpression("#id").getValue(context)).isNull();
  }

  @Test
  @DisplayName("a compiled expression evaluates against the context interface")
  void compiledExpressionEvaluates() {
    SpelExpression expression = (SpelExpression) parser.parseExpression("#p1 + 1");
    assertThat(expression.getValue(context())).isEqualTo(8);

    assertThat(expression.compileExpression()).isTrue();
    assertThat(expression.getValue(context())).isEqualTo(8);
  }

  @Test
  @DisplayName("reflection caches are shared within one support and isolated between supports")
  void reflectionAccessorsAreSharedWithinSupport() {
    assertThat(context().getPropertyAccessors()).isSameAs(context().getPropertyAccessors());
    assertThat(context().getMethodResolvers()).isSameAs(context().getMethodResolvers());

    ZetaEvaluationContext.Support other = ZetaEvaluationContext.Support.defaults();
    ZetaEvaluationContext otherContext = ZetaEvaluationContext.of(
      ZetaEvaluationContext.Root.of(target, Fixture.class, method, arguments),
      parameterNames.getParameterNames(method),
      null,
      other
    );

    assertThat(otherContext.getPropertyAccessors()).isNotSameAs(context().getPropertyAccessors());
    assertThat(otherContext.getMethodResolvers()).isNotSameAs(context().getMethodResolvers());
  }

  @Test
  @DisplayName("getArgs returns a defensive copy: an expression cannot mutate the invocation arguments")
  void getArgsReturnsACopy() {
    ZetaEvaluationContext context = context();

    Object[] exposed = context.root().getArgs();
    assertThat(exposed).isNotSameAs(arguments);

    exposed[0] = "mutated";

    assertThat(arguments[0]).isEqualTo("ACME");
    assertThat(context.root().getArgs()[0]).isEqualTo("ACME");
  }

  @Test
  @DisplayName("getArgs returns the empty argument array as-is: there is nothing to copy")
  void getArgsReusesTheEmptyArray() throws NoSuchMethodException {
    Method empty = Fixture.class.getDeclaredMethod("empty");
    Object[] noArgs = new Object[0];
    ZetaEvaluationContext context = ZetaEvaluationContext.of(
      ZetaEvaluationContext.Root.of(target, Fixture.class, empty, noArgs),
      parameterNames.getParameterNames(empty),
      null
    );

    assertThat(context.root().getArgs()).isSameAs(noArgs);
  }

  @Test
  @DisplayName("a vararg parameter binds the remaining arguments as one array")
  void varargsBindAsOneArray() throws NoSuchMethodException {
    Method join = Fixture.class.getDeclaredMethod("join", String[].class);
    ZetaEvaluationContext context = ZetaEvaluationContext.of(
      ZetaEvaluationContext.Root.of(target, Fixture.class, join, new Object[] { "a", "b" }),
      parameterNames.getParameterNames(join),
      null
    );

    assertThat((Object[]) parser.parseExpression("#ids").getValue(context)).containsExactly("a", "b");
    assertThat((Object[]) parser.parseExpression("#p0").getValue(context)).containsExactly("a", "b");
  }

  @Test
  @DisplayName("expanded varargs bind as component arrays for zero, one and many values")
  void expandedVarargsBindAsComponentArrays() throws NoSuchMethodException {
    Method join = Fixture.class.getDeclaredMethod("join", String[].class);
    String[] names = parameterNames.getParameterNames(join);

    ZetaEvaluationContext none = varargsContext(join, names, new Object[0]);
    assertThat((Object[]) parser.parseExpression("#ids").getValue(none)).isEmpty();

    ZetaEvaluationContext one = varargsContext(join, names, new Object[] { "a" });
    assertThat((Object[]) parser.parseExpression("#ids").getValue(one)).containsExactly("a");

    String[] packed = { "a", "b" };
    ZetaEvaluationContext packedContext = varargsContext(join, names, new Object[] { packed });
    assertThat(parser.parseExpression("#ids").getValue(packedContext)).isSameAs(packed);
  }

  @Test
  @DisplayName("packArguments adapts expanded, one-value and packed varargs for reflection")
  void packArgumentsAdaptsEveryVarargsShape() throws NoSuchMethodException {
    Method join = Fixture.class.getDeclaredMethod("join", String[].class);

    assertThat((Object[]) ZetaEvaluationContext.packArguments(join, new Object[] { "a", "b" })[0])
      .containsExactly("a", "b");
    assertThat((Object[]) ZetaEvaluationContext.packArguments(join, new Object[] { "a" })[0])
      .containsExactly("a");
    assertThat((Object[]) ZetaEvaluationContext.packArguments(join, new Object[0])[0]).isEmpty();

    Object[] packed = { new String[] { "a", "b" } };
    assertThat(ZetaEvaluationContext.packArguments(join, packed)).isSameAs(packed);
  }

  private ZetaEvaluationContext varargsContext(Method join, String[] names, Object[] args) {
    return ZetaEvaluationContext.of(
      ZetaEvaluationContext.Root.of(target, Fixture.class, join, args),
      names,
      null
    );
  }
}
