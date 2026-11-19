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

  private final ZetaEvaluationContext.ExpressionSupport support = ZetaEvaluationContext.ExpressionSupport.defaults();

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

    public String merge(String[] left, String[] right) {
      return String.join(",", left) + "|" + String.join(",", right);
    }

    public String echo(Serializable... values) {
      return String.join(",", java.util.Arrays.stream(values).map(Object::toString).toList());
    }
  }

  private ParameterBindings bindings(Method method) {
    return ParameterBindings.of(method, parameterNames.getParameterNames(method));
  }

  private ZetaEvaluationContext context() {
    return context(method, arguments, support);
  }

  private ZetaEvaluationContext context(Method method, Object[] args, ZetaEvaluationContext.ExpressionSupport support) {
    return ZetaEvaluationContext.create(target, Fixture.class, method, args, bindings(method), null, support);
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
  @DisplayName("bindResult binds #result without replacing the root")
  void bindResultBindsResult() {
    ZetaEvaluationContext context = context().bindResult("loaded");

    assertThat(parser.parseExpression("#result").getValue(context)).isEqualTo("loaded");
    assertThat(parser.parseExpression("#root.methodName").getValue(context)).isEqualTo("find");
  }

  @Test
  @DisplayName("bindResult(null) binds a null result")
  void bindResultBindsNull() {
    ZetaEvaluationContext context = context().bindResult(null);

    assertThat(parser.parseExpression("#result").getValue(context)).isNull();
    assertThat(parser.parseExpression("#root.methodName").getValue(context)).isEqualTo("find");
  }

  @Test
  @DisplayName("the context rejects variable writes")
  void setVariableIsRejected() {
    ZetaEvaluationContext context = context();

    assertThatThrownBy(() -> context.setVariable("id", null))
      .isInstanceOf(UnsupportedOperationException.class)
      .hasMessageContaining("read-only");
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

    ZetaEvaluationContext.ExpressionSupport other = ZetaEvaluationContext.ExpressionSupport.defaults();
    ZetaEvaluationContext otherContext = context(method, arguments, other);

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
    ZetaEvaluationContext context = context(empty, noArgs, support);

    assertThat(context.root().getArgs()).isSameAs(noArgs);
  }

  @Test
  @DisplayName("a vararg parameter binds the remaining arguments as one array")
  void varargsBindAsOneArray() throws NoSuchMethodException {
    Method join = Fixture.class.getDeclaredMethod("join", String[].class);
    ZetaEvaluationContext context = context(join, new Object[] { "a", "b" }, support);

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
  @DisplayName("a generic interface's erased Object[] varargs tail is copied to the declared component type")
  void erasedVarargsAreCopiedToTheComponentType() throws NoSuchMethodException {
    Method join = Fixture.class.getDeclaredMethod("join", String[].class);
    Object[] erased = { new Object[] { "a", "b" } };
    ZetaEvaluationContext context = varargsContext(join, parameterNames.getParameterNames(join), erased);

    Object value = parser.parseExpression("#ids").getValue(context);
    assertThat(value).isInstanceOf(String[].class);
    assertThat((String[]) value).containsExactly("a", "b");
  }

  @Test
  @DisplayName("a non-varargs array parameter binds as its declared array type")
  void erasedArrayParametersAreCopiedToTheComponentType() throws NoSuchMethodException {
    Method merge = Fixture.class.getDeclaredMethod("merge", String[].class, String[].class);
    Object[] erased = { new Object[] { "a" }, new Object[] { "b" } };
    ZetaEvaluationContext context = context(merge, erased, support);

    assertThat((String[]) parser.parseExpression("#left").getValue(context)).containsExactly("a");
    assertThat((String[]) parser.parseExpression("#right").getValue(context)).containsExactly("b");
    assertThat((String[]) parser.parseExpression("#p1").getValue(context)).containsExactly("b");
  }

  /**
   * Every array is {@link Serializable}, so testing the component type with {@code isInstance}
   * mistakes the erased {@code Object[]} container for one element of a {@code Serializable...}
   * method.
   */
  @Test
  @DisplayName("an erased Object[] varargs tail under a Serializable bound resolves to the declared array")
  void serializableBoundArraysAreNormalized() throws NoSuchMethodException {
    Method echo = Fixture.class.getDeclaredMethod("echo", Serializable[].class);
    Object[] erased = { new Object[] { "a", "b" } };

    ZetaEvaluationContext context = context(echo, erased, support);
    assertThat(parser.parseExpression("#p0").getValue(context)).isInstanceOf(Serializable[].class);
  }

  private ZetaEvaluationContext varargsContext(Method join, String[] names, Object[] args) {
    return ZetaEvaluationContext.create(
      target,
      Fixture.class,
      join,
      args,
      ParameterBindings.of(join, names),
      null,
      support
    );
  }
}
