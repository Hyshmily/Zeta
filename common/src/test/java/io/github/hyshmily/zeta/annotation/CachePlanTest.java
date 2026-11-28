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
package io.github.hyshmily.zeta.annotation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hyshmily.zeta.annotation.CachePlan.Invalidate.Operation;
import io.github.hyshmily.zeta.annotation.support.CompiledExpression;
import io.github.hyshmily.zeta.annotation.support.KeyEncoding;
import io.github.hyshmily.zeta.annotation.support.NamespaceKeys;
import io.github.hyshmily.zeta.annotation.support.ParameterBindings;
import io.github.hyshmily.zeta.annotation.support.ZetaEvaluationContext;
import io.github.hyshmily.zeta.cache.cachesupport.policy.InvalidatePolicy;
import io.github.hyshmily.zeta.policy.KeyFormat;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.expression.spel.standard.SpelExpressionParser;

@DisplayName("CachePlan")
class CachePlanTest {

  private static final ZetaEvaluationContext.ExpressionSupport SUPPORT =
    ZetaEvaluationContext.ExpressionSupport.defaults();

  private static final SpelExpressionParser PARSER = new SpelExpressionParser();

  @Test
  @DisplayName("a BigInteger below Long.MIN_VALUE fails instead of wrapping to a positive TTL")
  void negativeBigIntegerIsRejected() {
    CachePlan.Ttl ttl = ttl(
      "hardTtl",
      "new java.math.BigInteger('-18446744073709551615')",
      method("oneArg", String.class),
      false
    );

    assertThatThrownBy(() -> ttl.evaluate(null))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("exceeds the long range");
  }

  @Test
  @DisplayName("a BigInteger that maps to -1 after wrapping never reaches the disable sentinel")
  void negativeBigIntegerNeverDisablesASoftWindow() {
    CachePlan.Ttl ttl = ttl("softTtl", "new java.math.BigInteger('-18446744073709551617')", method("noArgs"), true);

    assertThatThrownBy(() -> ttl.evaluate(null))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("exceeds the long range");
  }

  @Test
  @DisplayName("a BigDecimal above Long.MAX_VALUE fails instead of keeping its low 64 bits")
  void outOfRangeBigDecimalIsRejected() {
    CachePlan.Ttl ttl = ttl("hardTtl", "new java.math.BigDecimal('18446744073709551617')", method("noArgs"), false);

    assertThatThrownBy(() -> ttl.evaluate(null))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("within the long range");
  }

  @Test
  @DisplayName("a BigDecimal with a zero fraction is still an integral TTL")
  void integralBigDecimalIsAccepted() {
    CachePlan.Ttl ttl = ttl("hardTtl", "new java.math.BigDecimal('60000.0')", method("noArgs"), false);

    assertThat(ttl.evaluate(null)).isEqualTo(60_000L);
  }

  @Test
  @DisplayName("a negative BigDecimal -1 keeps the disable sentinel it spells")
  void negativeOneBigDecimalKeepsTheSentinel() {
    CachePlan.Ttl enabled = ttl("softTtl", "new java.math.BigDecimal('-1')", method("noArgs"), true);
    CachePlan.Ttl disabled = ttl("hardTtl", "new java.math.BigDecimal('-1')", method("noArgs"), false);

    assertThat(enabled.evaluate(null)).isEqualTo(-1L);
    assertThat(disabled.evaluate(null)).isZero();
  }

  @Test
  @DisplayName("constant arithmetic folds to a millisecond constant at resolution")
  void constantArithmeticFolds() {
    CachePlan.Ttl ttl = ttl("softTtl", "60 * 1000", method("noArgs"), true);

    assertThat(ttl.isConstant()).isTrue();
    assertThat(ttl.evaluate(null)).isEqualTo(60_000L);
    assertThat(ttl.needsContext()).isFalse();
  }

  @Test
  @DisplayName("a folded constant that overflows 32-bit SpEL arithmetic fails startup")
  void overflowingConstantArithmeticFailsStartup() {
    Method method = method("noArgs");

    assertThatThrownBy(() -> ttl("softTtl", "30 * 24 * 60 * 60 * 1000", method, true))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("softTtl");
  }

  @Test
  @DisplayName("a constant expression that cannot be evaluated names its role and method")
  void constantEvaluationFailureNamesTheAttribute() {
    Method method = method("noArgs");

    assertThatThrownBy(() -> ttl("hardTtl", "1 / 0", method, false))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("hardTtl")
      .hasMessageContaining("cannot be evaluated at startup")
      .hasMessageContaining(method.toString());
  }

  @Test
  @DisplayName("a type reference is not a constant, so it stays lazily evaluated")
  void typeReferenceStaysLazy() {
    CachePlan.Ttl ttl = ttl("hardTtl", "T(java.lang.Math).abs(-60000)", method("noArgs"), false);

    assertThat(ttl.isConstant()).isFalse();
    assertThat(ttl.needsContext()).isFalse();
    assertThat(ttl.evaluate(null)).isEqualTo(60_000L);
  }

  @Test
  @DisplayName("a type-reference key needs no per-invocation context")
  void typeReferenceKeyNeedsNoContext() {
    Method method = method("noArgs");
    CachePlan.Key key = CachePlan.Key.of(
      encoding(method),
      method,
      compiled("T(java.lang.Math).max(1, 2)", method),
      bindings(method)
    );

    assertThat(key).isInstanceOf(CachePlan.Key.Expression.class);
    assertThat(key.needsContext()).isFalse();
    assertThat(key.resolve(new Object[0], null)).isEqualTo("users\u0000[I:2]");
  }

  @Test
  @DisplayName("a literal key folds to a constant on a PLAIN zero-argument method")
  void literalKeyFoldsOnPlainZeroArgumentMethod() {
    Method method = method("noArgs");
    KeyEncoding encoding = encoding(method, KeyFormat.PLAIN);
    CachePlan.Key key = CachePlan.Key.of(encoding, method, compiled("'global'", method), bindings(method));

    assertThat(key).isInstanceOf(CachePlan.Key.Constant.class);
    assertThat(((CachePlan.Key.Constant) key).physicalKey()).isEqualTo("users:global");
    assertThat(key.resolve(new Object[0], null)).isEqualTo("users:global");
  }

  @Test
  @DisplayName("a literal key folds even when the method has parameters")
  void literalKeyFoldsWithParameters() {
    Method method = method("twoArgs", String.class, String.class);
    KeyEncoding encoding = encoding(method, KeyFormat.TYPED);
    CachePlan.Key key = CachePlan.Key.of(encoding, method, compiled("'global'", method), bindings(method));

    assertThat(key).isInstanceOf(CachePlan.Key.Constant.class);
    assertThat(key.resolve(new Object[] { "a", "b" }, null)).isEqualTo("users\u0000[s6:global]");
  }

  @Test
  @DisplayName("a constant key that resolves to null fails resolution")
  void nullConstantKeyFails() {
    Method method = method("noArgs");

    assertThatThrownBy(() -> CachePlan.Key.of(encoding(method), method, compiled("null", method), bindings(method)))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("resolved to null");
  }

  @Test
  @DisplayName("the canonical argument key yields null for a null argument instead of throwing")
  void defaultArgumentsKeyReportsNull() {
    Method method = method("twoArgs", String.class, String.class);
    CachePlan.Key key = CachePlan.Key.of(encoding(method), method, CompiledExpression.ABSENT, bindings(method));

    assertThat(key).isInstanceOf(CachePlan.Key.DefaultArguments.class);
    assertThat(key.resolveOrNull(new Object[] { "a", null }, null)).isNull();
    assertThatThrownBy(() -> key.resolve(new Object[] { "a", null }, null))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("null argument 1");
  }

  @Test
  @DisplayName("eachKey accepts 1024 distinct keys followed by a duplicate or null")
  void eachKeyLimitCountsDistinctKeys() {
    Method method = method("oneArg", String.class);
    Operation operation = eachKeyOperation(method);
    List<Long> ids = new ArrayList<>();
    for (long i = 0; i < 1024; i++) {
      ids.add(i);
    }
    ids.add(0L);
    ids.add(null);

    assertThat((List<?>) operation.resolve(method, new Object[] { ids }, null, false)).hasSize(1024);
  }

  @Test
  @DisplayName("eachKey rejects 1025 distinct keys")
  void eachKeyRejectsTheNextDistinctKey() {
    Method method = method("oneArg", String.class);
    Operation operation = eachKeyOperation(method);
    List<Long> ids = new ArrayList<>();
    for (long i = 0; i <= 1024; i++) {
      ids.add(i);
    }

    assertThatThrownBy(() -> operation.resolve(method, new Object[] { ids }, null, false))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("more than 1024 keys");
  }

  private static Operation eachKeyOperation(Method method) {
    return Operation.eachKey(
      "users",
      encoding(method),
      CompiledExpression.ABSENT,
      0,
      new InvalidatePolicy(InvalidatePolicy.Scope.LOCAL, false),
      CompiledExpression.ABSENT
    );
  }

  private static CachePlan.Ttl ttl(String role, String raw, Method method, boolean disableAllowed) {
    return CachePlan.Ttl.of(role, raw, PARSER, SUPPORT.emptyContext(), method, bindings(method), true, disableAllowed);
  }

  private static CompiledExpression compiled(String raw, Method method) {
    return CompiledExpression.of(raw, PARSER, SUPPORT.emptyContext(), "cache key", method, bindings(method), true);
  }

  private static KeyEncoding encoding(Method method) {
    return encoding(method, KeyFormat.TYPED);
  }

  private static KeyEncoding encoding(Method method, KeyFormat format) {
    NamespaceKeys keys = new NamespaceKeys(format);
    NamespaceKeys.Staging staging = keys.staging();
    KeyEncoding encoding = staging.stage("users", KeyFormat.Option.INHERIT, method, true);
    staging.commit();
    return encoding;
  }

  private static ParameterBindings bindings(Method method) {
    return ParameterBindings.of(method, null);
  }

  private static Method method(String name, Class<?>... parameterTypes) {
    try {
      return Fixture.class.getDeclaredMethod(name, parameterTypes);
    } catch (NoSuchMethodException e) {
      throw new AssertionError(e);
    }
  }

  @SuppressWarnings("unused")
  private static final class Fixture {

    private void noArgs() {}

    private void oneArg(String id) {}

    private void twoArgs(String id, String name) {}
  }
}
