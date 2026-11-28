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
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.annotation.support.DefaultKey;
import io.github.hyshmily.zeta.annotation.support.NamespaceKeys;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.policy.*;
import io.github.hyshmily.zeta.policy.KeyFormat;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.aop.ProxyMethodInvocation;
import org.springframework.aop.TargetSource;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.framework.AopConfigException;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.aop.support.DefaultIntroductionAdvisor;
import org.springframework.aop.support.DefaultPointcutAdvisor;
import org.springframework.aop.support.DelegatingIntroductionInterceptor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.expression.spel.SpelEvaluationException;

@DisplayName("ZetaCacheInterceptor")
class ZetaCacheInterceptorTest {

  private static final String NS = "\u0000";

  /** The physical key an explicit single-value expression produces: one default-key argument. */
  private static String key(String cache, Object value) {
    return (cache + NS + DefaultKey.encode(new Object[] { value }));
  }

  private Zeta zeta;
  private PlanResolver resolver;
  private ZetaCacheInterceptor interceptor;

  /** What the last simulated engine read received. */
  private String lastKey;
  private CacheLoader<String, Object> lastLoader;
  private GetPolicy lastOptions;

  /** Simulated L1 state: {@code hit} serves {@code hitValue} without running the loader. */
  private boolean hit;
  private Object hitValue;

  /** What the simulated engine observed from the loader on a miss. */
  private Boolean lastShouldCache;

  /** What the simulated engine saw the loader throw, or {@code null} when it returned. */
  private Throwable lastLoadFailure;

  @BeforeEach
  void setUp() {
    zeta = mock(Zeta.class);
    resolver = new PlanResolver(null);
    interceptor = new ZetaCacheInterceptor(zeta, resolver);
    when(zeta.get(anyString(), any(CacheLoader.class), any(GetPolicy.class))).thenAnswer(invocation -> {
      lastKey = invocation.getArgument(0);
      lastLoader = invocation.getArgument(1);
      lastOptions = invocation.getArgument(2);
      if (hit) {
        return hitValue;
      }
      try {
        Object loaded = lastLoader.load(lastKey);
        lastShouldCache = lastLoader.isCacheable(loaded);
        return loaded;
      } catch (RuntimeException failure) {
        lastLoadFailure = failure;
        throw failure;
      }
    });
  }

  @SuppressWarnings("unchecked")
  private <T> T proxy(T target, MethodInterceptor... otherAdvices) {
    ProxyFactory factory = new ProxyFactory(target);
    factory.addAdvisor(cacheAdvisor());
    for (MethodInterceptor other : otherAdvices) {
      factory.addAdvisor(new DefaultPointcutAdvisor(other));
    }
    return (T) factory.getProxy();
  }

  private ZetaCacheAdvisor cacheAdvisor() {
    ZetaCacheAdvisor advisor = new ZetaCacheAdvisor(resolver);
    advisor.setAdvice(interceptor);
    return advisor;
  }

  /** A JDK dynamic proxy: the invocation reports the interface method, not the implementation. */
  @SuppressWarnings("unchecked")
  private <T> T jdkProxy(T target, Class<?>... interfaces) {
    return jdkProxyWith(target, interfaces);
  }

  /** A JDK dynamic proxy carrying extra advice behind the cache advice, in declaration order. */
  @SuppressWarnings("unchecked")
  private <T> T jdkProxyWith(T target, Class<?>[] interfaces, MethodInterceptor... advices) {
    ProxyFactory factory = new ProxyFactory();
    factory.setTarget(target);
    factory.setInterfaces(interfaces);
    factory.addAdvisor(cacheAdvisor());
    for (MethodInterceptor advice : advices) {
      factory.addAdvisor(new DefaultPointcutAdvisor(advice));
    }
    return (T) factory.getProxy();
  }

  private void missRunsBody() {
    hit = false;
    hitValue = null;
  }

  private void hitServes(Object value) {
    hit = true;
    hitValue = value;
  }

  /**
   * Proxy creation wraps pointcut validation failures in {@link AopConfigException}; assert through the
   * wrapper to the interceptor's own diagnosis.
   */
  private static void assertRejectedAtStartup(ThrowingCallable call, String... messageFragments) {
    Throwable thrown = catchThrowable(call);
    assertThat(thrown).isInstanceOf(AopConfigException.class);
    Throwable diagnosis = thrown;
    while (diagnosis != null && !(diagnosis instanceof IllegalStateException)) {
      diagnosis = diagnosis.getCause();
    }
    assertThat(diagnosis).as("startup validation diagnosis").isInstanceOf(IllegalStateException.class);
    for (String fragment : messageFragments) {
      assertThat(diagnosis.getMessage()).contains(fragment);
    }
  }

  // ---------------------------------------------------------------------
  // @ZetaCached: the read
  // ---------------------------------------------------------------------

  static class UserReader {

    final AtomicInteger calls = new AtomicInteger();

    @ZetaCached(cache = "users", key = "#p0")
    public String load(String id) {
      calls.incrementAndGet();
      return "real-" + id;
    }
  }

  @Test
  @DisplayName("a miss runs the body once and serves it under the namespaced key")
  void missRunsBodyUnderNamespacedKey() {
    UserReader target = new UserReader();
    missRunsBody();

    assertThat(proxy(target).load("42")).isEqualTo("real-42");
    assertThat(target.calls).hasValue(1);
    assertThat(lastKey).isEqualTo(key("users", "42"));
    assertThat(lastOptions.stalePolicy()).isEqualTo(StalePolicy.SOFT_REFRESH);
    assertThat(lastOptions.report()).isTrue();
    assertThat(lastOptions.callerRunsLoader()).isTrue();
    assertThat(lastOptions.hardTtlMs().getAsLong()).isZero();
    assertThat(lastOptions.softTtlMs().getAsLong()).isZero();
  }

  @Test
  @DisplayName("a hit is served without running the body")
  void hitNeverRunsBody() {
    UserReader target = new UserReader();
    hitServes("cached-42");

    assertThat(proxy(target).load("42")).isEqualTo("cached-42");
    assertThat(target.calls).hasValue(0);
    assertThat(lastKey).isEqualTo(key("users", "42"));
  }

  static class TtlService {

    @ZetaCached(cache = "users", key = "#p0", hardTtl = "#p0 == 'vip' ? 1000 : 5", softTtl = "7")
    public String load(String id) {
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("TTL attributes stay lazy: a digit literal is a constant, an expression is evaluated on demand")
  void ttlOverridesAreLazy() {
    TtlService service = proxy(new TtlService());

    missRunsBody();
    service.load("vip");
    assertThat(lastOptions.hardTtlMs().getAsLong()).isEqualTo(1000L);
    assertThat(lastOptions.softTtlMs().getAsLong()).isEqualTo(7L);

    missRunsBody();
    service.load("normal");
    assertThat(lastOptions.hardTtlMs().getAsLong()).isEqualTo(5L);
  }

  static class StaticTtlService {

    @ZetaCached(cache = "users", key = "#p0", hardTtl = "60000", softTtl = "5000")
    public String load(String id) {
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("two constant TTLs prebuild one options instance: a read allocates no TTL supplier and no record")
  void constantTtlsShareOnePrebuiltOptions() {
    StaticTtlService service = proxy(new StaticTtlService());

    missRunsBody();
    service.load("a");
    GetPolicy first = lastOptions;
    missRunsBody();
    service.load("b");

    assertThat(lastOptions).isSameAs(first);
    assertThat(lastOptions.hardTtlMs().getAsLong()).isEqualTo(60_000L);
    assertThat(lastOptions.softTtlMs().getAsLong()).isEqualTo(5_000L);
  }

  @Test
  @DisplayName("a mixed literal/expression pair reuses the prebuilt constant supplier across calls")
  void mixedTtlsReuseTheConstantSupplier() {
    TtlService service = proxy(new TtlService());

    missRunsBody();
    service.load("a");
    GetPolicy first = lastOptions;
    missRunsBody();
    service.load("b");

    assertThat(lastOptions).isNotSameAs(first);
    assertThat(lastOptions.softTtlMs()).isSameAs(first.softTtlMs());
    assertThat(lastOptions.hardTtlMs()).isNotSameAs(first.hardTtlMs());
  }

  static class DefaultKeySingleArg {

    @ZetaCached(cache = "users")
    public String load(String id) {
      return "v";
    }
  }

  static class DefaultKeyNoArgs {

    @ZetaCached(cache = "users")
    public String load() {
      return "v";
    }
  }

  static class DefaultKeyMultipleArgs {

    @ZetaCached(cache = "users")
    public String load(String a, String b) {
      return "v";
    }
  }

  @Test
  @DisplayName("an absent key attribute derives the canonical default-key encoding from the arguments")
  void defaultKeys() {
    missRunsBody();
    proxy(new DefaultKeySingleArg()).load("42");
    assertThat(lastKey).isEqualTo("users" + NS + "[s2:42]");

    missRunsBody();
    proxy(new DefaultKeyNoArgs()).load();
    assertThat(lastKey).isEqualTo("users" + NS + "[]");

    missRunsBody();
    proxy(new DefaultKeyMultipleArgs()).load("a", "b");
    assertThat(lastKey).isEqualTo("users" + NS + "[s1:a,s1:b]");
  }

  @Test
  @DisplayName("default keys are injective: argument boundaries cannot collide across calls")
  void defaultKeysDoNotCollideOnArgumentBoundaries() {
    DefaultKeyMultipleArgs service = proxy(new DefaultKeyMultipleArgs());

    missRunsBody();
    service.load("foo, bar", "baz");
    String first = lastKey;

    missRunsBody();
    service.load("foo", "bar, baz");
    String second = lastKey;

    assertThat(first).isNotEqualTo(second);
  }

  @Test
  @DisplayName("default keys are injective: a no-argument key cannot be forged by one argument")
  void defaultKeysDoNotCollideAcrossArity() {
    missRunsBody();
    proxy(new DefaultKeyNoArgs()).load();
    String noArgs = lastKey;

    missRunsBody();
    proxy(new DefaultKeySingleArg()).load("[]");
    String singleArg = lastKey;

    missRunsBody();
    proxy(new DefaultKeySingleArg()).load("SimpleKey []");
    String classic = lastKey;

    assertThat(singleArg).isNotEqualTo(noArgs);
    assertThat(classic).isNotEqualTo(noArgs);
  }

  static class LiteralKeyReader {

    @ZetaCached(cache = "users", key = "'fixed'")
    public String load(String id) {
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("a literal key resolves to the same constant physical key")
  void literalKeyIsConstant() {
    missRunsBody();
    proxy(new LiteralKeyReader()).load("42");

    assertThat(lastKey).isEqualTo(key("users", "fixed"));
  }

  static class ArithmeticKeyReader {

    @ZetaCached(cache = "users", key = "1 + 2")
    public String load(String id) {
      return "v";
    }
  }

  @Test
  @DisplayName("a context-free key expression is folded to a constant physical key")
  void contextFreeKeyIsConstant() {
    missRunsBody();
    proxy(new ArithmeticKeyReader()).load("42");

    assertThat(lastKey).isEqualTo(key("users", 3));
  }

  static class NullLiteralKeyReader {

    @ZetaCached(cache = "users", key = "null")
    public String load(String id) {
      return "v";
    }
  }

  @Test
  @DisplayName("default keys encode array content instead of the identity hash")
  void defaultKeysEncodeArrayContent() {
    ArrayArgReader service = proxy(new ArrayArgReader());

    missRunsBody();
    service.load(new byte[] { 1, 2 });
    String first = lastKey;

    missRunsBody();
    service.load(new byte[] { 1, 2 });
    String sameContent = lastKey;

    missRunsBody();
    service.load(new byte[] { 2, 1 });
    String otherContent = lastKey;

    assertThat(sameContent).isEqualTo(first);
    assertThat(otherContent).isNotEqualTo(first);
    assertThat(first).doesNotContain("@");
  }

  static class ArrayArgReader {

    @ZetaCached(cache = "users")
    public String load(byte[] id) {
      return "v";
    }
  }

  static class ExplicitArrayKeyReader {

    @ZetaCached(cache = "users", key = "#p0")
    public String load(byte[] id) {
      return "v";
    }
  }

  @Test
  @DisplayName("an explicit #p0 array key encodes content instead of the identity hash")
  void explicitArrayKeyEncodesContent() {
    ExplicitArrayKeyReader service = proxy(new ExplicitArrayKeyReader());

    missRunsBody();
    service.load(new byte[] { 1, 2 });
    String first = lastKey;

    missRunsBody();
    service.load(new byte[] { 1, 2 });
    String sameContent = lastKey;

    missRunsBody();
    service.load(new byte[] { 2, 1 });
    String otherContent = lastKey;

    assertThat(sameContent).isEqualTo(first);
    assertThat(otherContent).isNotEqualTo(first);
    assertThat(first).doesNotContain("@");
  }

  static class VarargsKeyReader {

    @ZetaCached(cache = "users", key = "#ids")
    public String load(String... ids) {
      return "v";
    }
  }

  @Test
  @DisplayName("a varargs tail binds the whole array on the explicit-key path")
  void varargsKeyBindsTheWholeArray() {
    VarargsKeyReader service = proxy(new VarargsKeyReader());

    missRunsBody();
    service.load("a", "b");
    String first = lastKey;

    missRunsBody();
    service.load(new String[] { "a", "b" });
    String sameContent = lastKey;

    missRunsBody();
    service.load("a", "c");
    String otherContent = lastKey;

    assertThat(sameContent).isEqualTo(first);
    assertThat(otherContent).isNotEqualTo(first);
    assertThat(first).doesNotContain("@");
  }

  static class ArrayArgumentReader {

    @ZetaCached(cache = "users", key = "#p0")
    public String load(String[] ids) {
      return ids[0];
    }
  }

  static class RootArgsKeyReader {

    @ZetaCached(cache = "users", key = "#root.args")
    public String load(String[] ids) {
      return "v";
    }
  }

  static class RootArgsMultiKeyReader {

    @ZetaCached(cache = "users", key = "#root.args")
    public String load(String a, String b) {
      return "v";
    }
  }

  static class ParenthesizedRootArgsMultiKeyReader {

    @ZetaCached(cache = "users", key = "(#root.args)")
    public String load(String a, String b) {
      return "v";
    }
  }

  static class RootArgsNoArgReader {

    @ZetaCached(cache = "users", key = "#root.args")
    public String load() {
      return "v";
    }
  }

  static class RootArgsNullReader {

    @ZetaCached(cache = "users", key = "#root.args")
    public String load(String a, String b) {
      return "v";
    }
  }

  @Test
  @DisplayName("#root.args and an absent key address the same multi-argument entry")
  void rootArgsKeyMatchesDefaultKey() {
    missRunsBody();
    proxy(new DefaultKeyMultipleArgs()).load("a", "b");
    String defaultKey = lastKey;

    missRunsBody();
    proxy(new RootArgsMultiKeyReader()).load("a", "b");

    assertThat(lastKey).isEqualTo(defaultKey);
  }

  @Test
  @DisplayName("a parenthesized #root.args keeps the multi-argument default encoding")
  void parenthesizedRootArgsMatchesDefaultKey() {
    missRunsBody();
    proxy(new DefaultKeyMultipleArgs()).load("a", "b");
    String defaultKey = lastKey;

    missRunsBody();
    proxy(new ParenthesizedRootArgsMultiKeyReader()).load("a", "b");

    assertThat(lastKey).isEqualTo(defaultKey);
  }

  @Test
  @DisplayName("a no-argument #root.args folds to the default no-argument key")
  void noArgumentRootArgsMatchesDefaultKey() {
    missRunsBody();
    proxy(new DefaultKeyNoArgs()).load();
    String defaultKey = lastKey;

    missRunsBody();
    proxy(new RootArgsNoArgReader()).load();

    assertThat(lastKey).isEqualTo(defaultKey);
  }

  @Test
  @DisplayName("an explicit #root.args null argument names the declared key, not a missing one")
  void explicitRootArgsNullArgumentNamesTheKey() {
    RootArgsNullReader service = proxy(new RootArgsNullReader());

    assertThatThrownBy(() -> service.load(null, "b"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("null argument 0")
      .hasMessageContaining("key = \"#root.args\"")
      .hasMessageNotContaining("declare an explicit key");
  }

  @Test
  @DisplayName("#root.args encodes an array argument by content")
  void rootArgsKeyEncodesArrayContent() {
    RootArgsKeyReader service = proxy(new RootArgsKeyReader());

    missRunsBody();
    service.load(new String[] { "a", "b" });
    String first = lastKey;

    missRunsBody();
    service.load(new String[] { "a", "b" });
    String sameContent = lastKey;

    assertThat(sameContent).isEqualTo(first);
    assertThat(first).doesNotContain("@");
  }

  static class ArrayResultWriter {

    @ZetaPut(cache = "users", key = "#result")
    public String[] save(String id) {
      return new String[] { id, "x" };
    }
  }

  @Test
  @DisplayName("an array @ZetaPut #result key is stable by content")
  void writeArrayResultKeyIsStable() {
    proxy(new ArrayResultWriter()).save("a");

    String expected = "users" + NS + DefaultKey.encode(new Object[] { new String[] { "a", "x" } });
    verify(zeta).put(eq(expected), any(), any(PutPolicy.class));
  }

  static class ObjectArgReader {

    @ZetaCached(cache = "obj")
    public String load(Object arg) {
      return "v";
    }
  }

  @Test
  @DisplayName("default keys distinguish String, Character and other CharSequence arguments")
  void defaultKeyTagsDistinguishTextShapes() {
    ObjectArgReader service = proxy(new ObjectArgReader());

    missRunsBody();
    service.load('a');
    String character = lastKey;

    missRunsBody();
    service.load("a");
    String string = lastKey;

    missRunsBody();
    service.load(new StringBuilder("a"));
    String sequence = lastKey;

    assertThat(character).isEqualTo("obj" + NS + "[c1:a]");
    assertThat(string).isEqualTo("obj" + NS + "[s1:a]");
    assertThat(sequence).isEqualTo("obj" + NS + "[q1:a]");
    assertThat(character).isNotEqualTo(string);
    assertThat(string).isNotEqualTo(sequence);
  }

  @Test
  @DisplayName("a null argument cannot silently become a default key")
  void nullArgumentIsRejected() {
    DefaultKeyMultipleArgs service = proxy(new DefaultKeyMultipleArgs());

    assertThatThrownBy(() -> service.load(null, "x"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("null argument 0");
    verify(zeta, never()).get(anyString(), any(CacheLoader.class), any(GetPolicy.class));
  }

  static class MethodNameKey {

    @ZetaCached(cache = "quotes", key = "#root.methodName + ':' + #p0")
    public String load(String symbol) {
      return "q";
    }
  }

  @Test
  @DisplayName("#root.methodName is available to the key expression")
  void rootMethodNameInKey() {
    missRunsBody();
    proxy(new MethodNameKey()).load("ACME");

    assertThat(lastKey).isEqualTo(key("quotes", "load:ACME"));
  }

  static class RootAccessorsKey {

    @ZetaCached(cache = "quotes", key = "#root.methodName + ':' + #root.args[0] + ':' + #root.targetClass.simpleName")
    public String load(String symbol) {
      return "q";
    }
  }

  @Test
  @DisplayName("#root.args and #root.targetClass are bound")
  void rootAccessorsInKey() {
    missRunsBody();
    proxy(new RootAccessorsKey()).load("ACME");

    assertThat(lastKey).isEqualTo(key("quotes", "load:ACME:RootAccessorsKey"));
  }

  static class KeyHelper {

    public String of(String id) {
      return "k-" + id;
    }
  }

  static class BeanReferenceKey {

    @ZetaCached(cache = "users", key = "@keys.of(#p0)")
    public String load(String id) {
      return "v";
    }
  }

  @Test
  @DisplayName("@beanName expressions resolve through the bean factory")
  void beanReferenceInKey() {
    StaticListableBeanFactory beanFactory = new StaticListableBeanFactory();
    beanFactory.addBean("keys", new KeyHelper());
    resolver = new PlanResolver(beanFactory);
    interceptor = new ZetaCacheInterceptor(zeta, resolver);
    missRunsBody();

    proxy(new BeanReferenceKey()).load("42");

    assertThat(lastKey).isEqualTo(key("users", "k-42"));
  }

  static class UnlessService {

    @ZetaCached(cache = "users", key = "#p0", unless = "#result == 'skip'")
    public String load(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("unless vetoes storing a result that was still served")
  void unlessVetoesStore() {
    UnlessService service = proxy(new UnlessService());
    missRunsBody();

    assertThat(service.load("skip")).isEqualTo("skip");
    assertThat(lastShouldCache).isFalse();

    missRunsBody();
    assertThat(service.load("keep")).isEqualTo("keep");
    assertThat(lastShouldCache).isTrue();
  }

  @Test
  @DisplayName("a whitespace-only unless is absent, not a parse failure")
  void blankUnlessIsAbsent() {
    BlankUnlessService service = proxy(new BlankUnlessService());
    missRunsBody();

    assertThat(service.load("v")).isEqualTo("v");
    assertThat(lastShouldCache).isTrue();
  }

  static class BlankUnlessService {

    @ZetaCached(cache = "users", key = "#p0", unless = "   ")
    public String load(String id) {
      return id;
    }
  }

  static class FailingUnlessService {

    @ZetaCached(cache = "users", key = "#p0", unless = "#p0.charAt(99) == 'x'")
    public String load(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("a runtime unless failure is thrown to the caller")
  void runtimeUnlessFailureIsThrown() {
    FailingUnlessService service = proxy(new FailingUnlessService());
    missRunsBody();

    assertThatThrownBy(() -> service.load("v")).isInstanceOf(RuntimeException.class);
  }

  static class BrokenKeyService {

    @ZetaCached(cache = "users", key = "'unclosed")
    public String load(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("a broken key expression fails at proxy creation instead of the first call")
  void brokenKeyFailsAtStartup() {
    assertRejectedAtStartup(() -> proxy(new BrokenKeyService()), "cannot be parsed");
    verify(zeta, never()).get(anyString(), any(CacheLoader.class), any(GetPolicy.class));
  }

  static class BrokenTtlService {

    @ZetaCached(cache = "users", key = "#p0", hardTtl = "'unclosed")
    public String load(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("a broken TTL expression fails at proxy creation")
  void brokenTtlFailsAtStartup() {
    assertRejectedAtStartup(() -> proxy(new BrokenTtlService()), "cannot be parsed");
  }

  static class FinalMethodReader {

    @ZetaCached(cache = "users", key = "#p0")
    public final String load(String id) {
      return "v-" + id;
    }
  }

  interface FinalMethodView {
    @ZetaCached(cache = "users", key = "#p0")
    String load(String id);
  }

  static class FinalMethodImpl implements FinalMethodView {

    @Override
    public final String load(String id) {
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("a final implementation method is allowed when an interface view can be proxied")
  void finalMethodWithInterfaceViewIsAllowed() {
    ZetaCacheAdvisor advisor = cacheAdvisor();
    assertThat(AopUtils.canApply(advisor.getPointcut(), FinalMethodImpl.class, false)).isTrue();

    missRunsBody();
    FinalMethodView service = jdkProxy(new FinalMethodImpl(), FinalMethodView.class);

    assertThat(service.load("42")).isEqualTo("v-42");
    assertThat(lastKey).isEqualTo(key("users", "42"));
  }

  static class MisspelledUnlessService {

    @ZetaCached(cache = "users", key = "#p0", unless = "#reslt != null")
    public String load(String id) {
      return id;
    }
  }

  static class MisspelledTtlService {

    @ZetaCached(cache = "users", key = "#p0", hardTtl = "#customTtl")
    public String load(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("a misspelled SpEL variable fails at proxy creation instead of evaluating to null")
  void unknownVariableFailsAtStartup() {
    assertRejectedAtStartup(() -> proxy(new MisspelledUnlessService()), "unknown variable '#reslt'");
    assertRejectedAtStartup(() -> proxy(new MisspelledTtlService()), "unknown variable '#customTtl'");
  }

  static class FailingTtlService {

    @ZetaCached(cache = "users", key = "#p0", hardTtl = "#p0.charAt(99)")
    public String load(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("a runtime TTL failure is thrown to the caller")
  void runtimeTtlFailureIsThrown() {
    FailingTtlService service = proxy(new FailingTtlService());
    missRunsBody();
    service.load("42");

    assertThatThrownBy(() -> lastOptions.hardTtlMs().getAsLong()).isInstanceOf(RuntimeException.class);
  }

  static class ThrowingReader {

    @ZetaCached(cache = "users", key = "#p0")
    public String load(String id) {
      throw new IllegalStateException("source down: " + id);
    }
  }

  @Test
  @DisplayName("a body exception reaches the caller unchanged and is a thrown load failure for the engine")
  void bodyFailurePropagates() {
    ThrowingReader service = proxy(new ThrowingReader());
    missRunsBody();

    assertThatThrownBy(() -> service.load("42"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("source down: 42");
    assertThat(lastLoadFailure)
      .as("the breaker and the degraded-read path only see a failure the loader threw")
      .isInstanceOf(IllegalStateException.class);
    assertThat(lastShouldCache).as("no value ever reached the admission filter").isNull();
  }

  static class CheckedThrowingReader {

    @ZetaCached(cache = "users", key = "#p0")
    public String load(String id) throws IOException {
      throw new IOException("disk down: " + id);
    }
  }

  @Test
  @DisplayName("a checked body exception travels as an unchecked load failure and is restored for the caller")
  void checkedBodyFailureIsWrappedForTheEngineAndRestored() {
    CheckedThrowingReader service = proxy(new CheckedThrowingReader());
    missRunsBody();

    assertThatThrownBy(() -> service.load("42"))
      .isInstanceOf(IOException.class)
      .hasMessage("disk down: 42");
    assertThat(lastLoadFailure)
      .as("the engine's failure path is unchecked-only, so the checked cause is carried")
      .isInstanceOf(RuntimeException.class)
      .hasCauseInstanceOf(IOException.class);
  }

  static class NullReturningReader {

    final AtomicInteger calls = new AtomicInteger();

    @ZetaCached(cache = "users", key = "#p0")
    public String load(String id) {
      calls.incrementAndGet();
      return null;
    }
  }

  @Test
  @DisplayName("null is never cached: every read runs the body again")
  void nullIsNeverStored() {
    NullReturningReader target = new NullReturningReader();
    NullReturningReader service = proxy(target);
    missRunsBody();

    assertThat(service.load("42")).isNull();
    assertThat(service.load("42")).isNull();
    assertThat(target.calls).hasValue(2);
  }

  static class UsersService {

    @ZetaCached(cache = "users", key = "#p0")
    public String load(String id) {
      return "u";
    }
  }

  static class OrdersService {

    @ZetaCached(cache = "orders", key = "#p0")
    public String load(String id) {
      return "o";
    }
  }

  @Test
  @DisplayName("two cache names never collide on the same logical key")
  void namespacesIsolateTheSameLogicalKey() {
    missRunsBody();
    proxy(new UsersService()).load("1");
    String usersKey = lastKey;

    missRunsBody();
    proxy(new OrdersService()).load("1");

    assertThat(usersKey).isEqualTo(key("users", "1"));
    assertThat(lastKey).isEqualTo(key("orders", "1"));
  }

  static class BackgroundRefreshService {

    final AtomicInteger calls = new AtomicInteger();

    @ZetaCached(cache = "users", key = "#p0")
    public String load(String id) {
      calls.incrementAndGet();
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("a background refresh invokes the body reflectively on another thread")
  void backgroundRefreshRunsBodyOffTheInvocationThread() throws Exception {
    BackgroundRefreshService target = new BackgroundRefreshService();
    BackgroundRefreshService service = proxy(target);
    missRunsBody();
    service.load("42");
    assertThat(target.calls).hasValue(1);

    CacheLoader<String, Object> refresh = lastLoader;
    Thread refresher = new Thread(() -> refresh.reload(key("users", "42")));
    refresher.start();
    refresher.join(5_000);

    assertThat(refresher.isAlive()).isFalse();
    assertThat(target.calls).hasValue(2);
  }

  static class ScopedRefreshReader {

    final AtomicInteger calls = new AtomicInteger();

    @ZetaCached(cache = "users", key = "#p0")
    public String load(String id) {
      calls.incrementAndGet();
      return "scoped-" + id;
    }
  }

  static class NonStaticTargetSource implements TargetSource {

    private final ScopedRefreshReader target = new ScopedRefreshReader();

    final AtomicInteger released = new AtomicInteger();

    @Override
    public Class<?> getTargetClass() {
      return ScopedRefreshReader.class;
    }

    @Override
    public boolean isStatic() {
      return false;
    }

    @Override
    public Object getTarget() {
      return target;
    }

    @Override
    public void releaseTarget(Object target) {
      released.incrementAndGet();
    }
  }

  @Test
  @DisplayName("a refresh of a non-static target source invokes the actual target without re-entering the advice")
  void nonStaticTargetSourceRefreshDoesNotReenterTheAdvice() {
    NonStaticTargetSource source = new NonStaticTargetSource();
    ProxyFactory factory = new ProxyFactory();
    factory.setTargetSource(source);
    factory.setProxyTargetClass(true);
    factory.addAdvisor(cacheAdvisor());
    ScopedRefreshReader service = (ScopedRefreshReader) factory.getProxy();

    hitServes("stale-42");
    assertThat(service.load("42")).isEqualTo("stale-42");

    CacheLoader<String, Object> refresh = lastLoader;
    assertThat(refresh.reload(key("users", "42"))).isEqualTo("scoped-42");
    assertThat(source.target.calls).hasValue(1);
    assertThat(source.released).hasValue(1);
  }

  static class NestedOptionalReader {

    @ZetaCached(cache = "users", key = "#p0")
    public Optional<Optional<String>> load(String id) {
      return Optional.of(Optional.of("v-" + id));
    }
  }

  @Test
  @DisplayName("a nested Optional is unwrapped once and rewrapped once on both paths")
  void nestedOptionalKeepsItsOuterWrapper() {
    NestedOptionalReader service = proxy(new NestedOptionalReader());

    missRunsBody();
    assertThat(service.load("42")).isEqualTo(Optional.of(Optional.of("v-42")));

    hitServes(Optional.of("v-42"));
    assertThat(service.load("42")).isEqualTo(Optional.of(Optional.of("v-42")));
  }

  static class ContextBuildingReader {

    @ZetaCached(cache = "users", key = "#p0 + ':k'")
    public String load(String id) {
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("the foreground read releases the one-shot advice chain, and reload still runs reflectively")
  void readReleasesTheAdviceChain() throws Exception {
    ContextBuildingReader service = proxy(new ContextBuildingReader());
    missRunsBody();
    service.load("42");

    CacheLoader<String, Object> refresh = lastLoader;
    Field chain = refresh.getClass().getDeclaredField("chain");
    chain.setAccessible(true);

    assertThat(chain.get(refresh)).as("the foreground read released the advice chain").isNull();
    assertThat(refresh.reload(key("users", "42"))).as("a refresh never needs the chain").isEqualTo("v-42");
    assertThat(chain.get(refresh)).as("a refresh retains no reflective state on the loader").isNull();
  }

  static class InlineRefreshReader {

    @ZetaCached(cache = "users", key = "#p0", unless = "#result == 'refreshed'")
    public String load(String id) {
      return "refreshed";
    }
  }

  @Test
  @DisplayName("an inline refresh evaluates unless against the refreshed value, never a foreground context")
  void inlineRefreshEvaluatesUnlessAgainstTheRefreshedValue() {
    InlineRefreshReader service = proxy(new InlineRefreshReader());
    AtomicReference<Boolean> admitted = new AtomicReference<>();
    when(zeta.get(anyString(), any(CacheLoader.class), any(GetPolicy.class))).thenAnswer(invocation -> {
      CacheLoader<String, Object> loader = invocation.getArgument(1);
      Object refreshed = loader.reload(invocation.getArgument(0));
      admitted.set(loader.isCacheable(refreshed));
      return "stale";
    });

    assertThat(service.load("42")).isEqualTo("stale");
    assertThat(admitted.get()).as("unless sees the refreshed value, not a stale binding").isFalse();
  }

  static class ReleasedContextReader {

    @ZetaCached(cache = "users", key = "#p0", unless = "#result == 'refreshed'")
    public String load(String id) {
      return "refreshed";
    }
  }

  static class OffThreadTtlReader {

    @ZetaCached(cache = "users", key = "#root.methodName + ':' + #p0", hardTtl = "#p0.length()")
    public String load(String id) {
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("a dynamic TTL supplier evaluated on another thread builds its own context")
  void dynamicTtlEvaluatesOffTheInvocationThread() throws Exception {
    OffThreadTtlReader service = proxy(new OffThreadTtlReader());
    missRunsBody();
    service.load("42");

    // The key already built and cached the invocation thread's context; the
    // engine may still evaluate the supplier later, on a refresh thread.
    LongSupplier hardTtl = lastOptions.hardTtlMs();
    AtomicLong evaluated = new AtomicLong(-1L);
    Thread refresher = new Thread(() -> evaluated.set(hardTtl.getAsLong()));
    refresher.start();
    refresher.join(5_000);

    assertThat(refresher.isAlive()).isFalse();
    assertThat(evaluated).hasValue(2L);
  }

  static class ThrowingRefreshReader {

    @ZetaCached(cache = "users", key = "#p0")
    public String load(String id) {
      throw new IllegalStateException("source down: " + id);
    }
  }

  @Test
  @DisplayName("a background-refresh body failure is thrown to the engine, never returned as a storable value")
  void refreshFailureIsThrownToTheEngine() {
    hitServes("stale-42");
    ThrowingRefreshReader service = proxy(new ThrowingRefreshReader());
    assertThat(service.load("42")).isEqualTo("stale-42");

    CacheLoader<String, Object> refresh = lastLoader;
    assertThatThrownBy(() -> refresh.reload(key("users", "42")))
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("source down: 42");
  }

  static class ChainObservedReader {

    final AtomicInteger calls = new AtomicInteger();

    @ZetaCached(cache = "users", key = "#p0")
    public String load(String id) {
      calls.incrementAndGet();
      return "real-" + id;
    }
  }

  @Test
  @DisplayName("the background-refresh loader entry point never resumes the one-shot advice chain")
  void reloadNeverResumesTheInvocationChain() {
    ChainObservedReader target = new ChainObservedReader();
    AtomicInteger chain = new AtomicInteger();
    MethodInterceptor counting = invocation -> {
      chain.incrementAndGet();
      return invocation.proceed();
    };
    ChainObservedReader service = proxy(target, counting);

    // A cache hit never runs the body and never enters the inner advice.
    hitServes("cached-42");
    assertThat(service.load("42")).isEqualTo("cached-42");
    assertThat(chain).hasValue(0);

    // The engine's refresh entry point (reload), even invoked inline on the
    // invocation thread, must go reflectively: the inner advice is not re-entered.
    CacheLoader<String, Object> refresh = lastLoader;
    assertThat(refresh.reload(key("users", "42"))).isEqualTo("real-42");
    assertThat(target.calls).hasValue(1);
    assertThat(chain).hasValue(0);
  }

  interface InterfaceReader {
    @ZetaCached(cache = "users", key = "#p0")
    String load(String id);
  }

  static class InterfaceReaderImpl implements InterfaceReader {

    final AtomicInteger calls = new AtomicInteger();

    @Override
    public String load(String id) {
      calls.incrementAndGet();
      return "impl-" + id;
    }
  }

  @Test
  @DisplayName("an annotation declared on the interface is honoured through a JDK proxy")
  void interfaceAnnotationIsHonoured() {
    InterfaceReaderImpl target = new InterfaceReaderImpl();
    missRunsBody();

    InterfaceReader service = jdkProxy(target, InterfaceReader.class);

    assertThat(service.getClass()).isNotSameAs(InterfaceReaderImpl.class);
    assertThat(service.load("42")).isEqualTo("impl-42");
    assertThat(target.calls).hasValue(1);
    assertThat(lastKey).isEqualTo(key("users", "42"));
  }

  interface CachedVerb {
    @ZetaCached(cache = "users", key = "#p0")
    String load(String id);
  }

  interface PutVerb {
    @ZetaPut(cache = "users", key = "#p0")
    String load(String id);
  }

  static class TwoInterfaceConflict implements CachedVerb, PutVerb {

    @Override
    public String load(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("verbs contributed by different interfaces conflict even when the implementation has none")
  void interfaceVerbConflictIsRejected() throws NoSuchMethodException {
    Method method = TwoInterfaceConflict.class.getMethod("load", String.class);

    assertThatThrownBy(() -> resolver.matches(method, TwoInterfaceConflict.class))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("mutually exclusive");
  }

  interface FirstCached {
    @ZetaCached(cache = "first", key = "#p0")
    String load(String id);
  }

  interface SecondCached {
    @ZetaCached(cache = "second", key = "#p0")
    String load(String id);
  }

  static class SameVerbConflict implements FirstCached, SecondCached {

    @Override
    public String load(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("the same verb with different attributes on two interfaces is rejected, not silently picked")
  void interfaceSameVerbConflictIsRejected() throws NoSuchMethodException {
    Method method = SameVerbConflict.class.getMethod("load", String.class);

    assertThatThrownBy(() -> resolver.matches(method, SameVerbConflict.class))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("conflicting @ZetaCached");
  }

  interface ThirdCached {
    @ZetaCached(cache = "first", key = "#p0")
    String load(String id);
  }

  static class SameVerbAgreement implements FirstCached, ThirdCached {

    @Override
    public String load(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("the same verb with equal attributes on two interfaces is one declaration")
  void interfaceSameVerbAgreementIsAccepted() throws NoSuchMethodException {
    Method method = SameVerbAgreement.class.getMethod("load", String.class);

    assertThat(resolver.matches(method, SameVerbAgreement.class)).isTrue();
  }

  interface FirstInvalidations {
    @ZetaInvalidate(cache = "first", key = "#p0")
    @ZetaInvalidate(cache = "second", key = "#p0")
    String update(String id);
  }

  interface SecondInvalidations {
    @ZetaInvalidate(cache = "second", key = "#p0")
    @ZetaInvalidate(cache = "first", key = "#p0")
    String update(String id);
  }

  static class FlippedInvalidations implements FirstInvalidations, SecondInvalidations {

    @Override
    public String update(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("two interfaces listing the same invalidations in a different order conflict")
  void invalidationDeclarationOrderIsRejected() throws NoSuchMethodException {
    Method method = FlippedInvalidations.class.getMethod("update", String.class);

    assertThatThrownBy(() -> resolver.matches(method, FlippedInvalidations.class))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("conflicting @ZetaInvalidate");
  }

  interface GenericReader<T> {
    @ZetaCached(cache = "users", key = "#p0")
    T get(T id);
  }

  static class GenericReaderImpl implements GenericReader<String> {

    @Override
    @ZetaPut(cache = "users", key = "#p0")
    public String get(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("a verb on a generic interface conflicts with a bridge-erased implementation verb")
  void genericBridgeVerbConflictIsRejected() throws NoSuchMethodException {
    Method bridge = GenericReaderImpl.class.getMethod("get", Object.class);

    assertThatThrownBy(() -> resolver.matches(bridge, GenericReaderImpl.class))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("mutually exclusive")
      .hasMessageContaining("conflicting verbs:")
      .hasMessageContaining("@ZetaCached")
      .hasMessageContaining("@ZetaPut");
  }

  interface ProxyOnlyReader {
    @ZetaCached(cache = "users", key = "#id")
    String fetch(String id);
  }

  @Test
  @DisplayName("a JDK proxy without a user-class implementation still binds parameter names")
  void jdkProxyParameterNamesFallBackToTheInterfaceMethod() {
    ProxyFactory factory = new ProxyFactory();
    factory.setInterfaces(ProxyOnlyReader.class);
    factory.addAdvisor(cacheAdvisor());
    factory.addAdvisor(
      new DefaultPointcutAdvisor((MethodInterceptor) invocation -> "proxied-" + invocation.getArguments()[0])
    );
    ProxyOnlyReader service = (ProxyOnlyReader) factory.getProxy();

    missRunsBody();
    assertThat(service.fetch("42")).isEqualTo("proxied-42");
    assertThat(lastKey).isEqualTo(key("users", "42"));
  }

  interface DefaultCachedReader {
    @ZetaCached(cache = "interface-cache", key = "#p0")
    String load(String id);
  }

  static class ImplCachedReader implements DefaultCachedReader {

    @Override
    @ZetaCached(cache = "impl-cache", key = "'fixed'")
    public String load(String id) {
      return "impl-" + id;
    }
  }

  @Test
  @DisplayName("the implementation's annotation wins over the interface's for the same verb")
  void implementationAnnotationWinsOverInterface() {
    missRunsBody();

    DefaultCachedReader service = jdkProxy(new ImplCachedReader(), DefaultCachedReader.class);

    assertThat(service.load("42")).isEqualTo("impl-42");
    assertThat(lastKey).isEqualTo(key("impl-cache", "fixed"));
  }

  static class BaseCachedReader {

    @ZetaCached(cache = "base", key = "#p0")
    public String load(String id) {
      return "base-" + id;
    }
  }

  static class OverridingCachedReader extends BaseCachedReader {

    @Override
    public String load(String id) {
      return "override-" + id;
    }
  }

  @Test
  @DisplayName("an annotation on an overridden superclass method still applies to the override")
  void superclassAnnotationAppliesToOverriddenMethod() {
    missRunsBody();

    assertThat(proxy(new OverridingCachedReader()).load("42")).isEqualTo("override-42");
    assertThat(lastKey).isEqualTo(key("base", "42"));
  }

  interface BaseApi {
    @ZetaCached(cache = "interface", key = "#p0")
    String load(String id);
  }

  static class BaseApiImpl implements BaseApi {

    @Override
    @ZetaCached(cache = "base", key = "#p0")
    public String load(String id) {
      return "base-" + id;
    }
  }

  static class AuditedApi extends BaseApiImpl {

    @Override
    public String load(String id) {
      return "audited-" + super.load(id);
    }
  }

  @Test
  @DisplayName("a grandchild override with no annotation resolves the base class over the interface")
  void baseClassAnnotationDominatesTheInterfaceThroughAGrandchild() {
    missRunsBody();

    BaseApi service = jdkProxy(new AuditedApi(), BaseApi.class);
    assertThat(service.load("42")).isEqualTo("audited-base-42");
    assertThat(lastKey).isEqualTo(key("base", "42"));
  }

  static class ParentCached {

    @ZetaCached(cache = "parent", key = "#p0")
    public String load(String id) {
      return "parent-" + id;
    }
  }

  static class ChildCached extends ParentCached {

    @Override
    @ZetaCached(cache = "child", key = "#p0")
    public String load(String id) {
      return "child-" + id;
    }
  }

  static class GrandChildCached extends ChildCached {

    @Override
    public String load(String id) {
      return "grandchild-" + super.load(id);
    }
  }

  @Test
  @DisplayName("the nearest annotated class in a three-level hierarchy wins without a conflict")
  void nearestAnnotatedClassWinsInDeepClassHierarchy() {
    missRunsBody();

    assertThat(proxy(new GrandChildCached()).load("42")).isEqualTo("grandchild-child-42");
    assertThat(lastKey).isEqualTo(key("child", "42"));
  }

  interface SuperApi {
    @ZetaCached(cache = "super", key = "#p0")
    String load(String id);
  }

  interface SubApi extends SuperApi {
    @Override
    @ZetaCached(cache = "sub", key = "#p0")
    String load(String id);
  }

  static class SubApiImpl implements SubApi {

    @Override
    public String load(String id) {
      return "impl-" + id;
    }
  }

  @Test
  @DisplayName("a sub-interface annotation overrides its super-interface without a conflict")
  void subInterfaceAnnotationDominatesTheSuperInterface() {
    missRunsBody();

    SubApi service = jdkProxy(new SubApiImpl(), SubApi.class);
    assertThat(service.load("42")).isEqualTo("impl-42");
    assertThat(lastKey).isEqualTo(key("sub", "42"));
  }

  interface DiamondSuper {
    @ZetaCached(cache = "v1")
    String load(String id);
  }

  interface DiamondSub extends DiamondSuper {
    @Override
    @ZetaCached(cache = "v2")
    String load(String id);
  }

  interface DiamondOther {
    @ZetaCached(cache = "v1")
    String load(String id);
  }

  static class DiamondImpl implements DiamondSub, DiamondOther {

    @Override
    public String load(String id) {
      return "impl-" + id;
    }
  }

  @Test
  @DisplayName("a maximal annotated view still conflicts with an unrelated interface after a dominating view")
  void maximalViewsConflictIndependentOfTraversalOrder() throws NoSuchMethodException {
    Method load = DiamondImpl.class.getMethod("load", String.class);

    assertThatThrownBy(() -> resolver.matches(load, DiamondImpl.class))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("conflicting @ZetaCached")
      .hasMessageContaining("v1")
      .hasMessageContaining("v2");
  }

  static class AnnotatedUnrelatedDiamondImpl implements DiamondSub, DiamondOther {

    @Deprecated
    @Override
    public String load(String id) {
      return "impl-" + id;
    }
  }

  @Test
  @DisplayName("an unrelated implementation annotation does not hide an interface verb conflict")
  void unrelatedImplementationAnnotationDoesNotHideAConflict() throws NoSuchMethodException {
    Method load = AnnotatedUnrelatedDiamondImpl.class.getMethod("load", String.class);

    assertThatThrownBy(() -> resolver.matches(load, AnnotatedUnrelatedDiamondImpl.class))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("conflicting @ZetaCached");
  }

  interface RenamedParameterReader {
    /** The interface declares {@code id}; the implementation declares {@code value}. */
    @ZetaCached(cache = "users", key = "#id + ':' + #root.method.declaringClass.simpleName")
    String load(String id);
  }

  static class RenamedParameterReaderImpl implements RenamedParameterReader {

    @Override
    public String load(String value) {
      return "impl-" + value;
    }
  }

  @Test
  @DisplayName("SpEL parameter names come from the method that declares the annotation")
  void parameterNamesComeFromTheAnnotatedMethod() {
    missRunsBody();

    RenamedParameterReader service = jdkProxy(new RenamedParameterReaderImpl(), RenamedParameterReader.class);

    assertThat(service.load("42")).isEqualTo("impl-42");
    assertThat(lastKey).isEqualTo(key("users", "42:RenamedParameterReaderImpl"));
  }

  static class InheritedCachedReader {

    @ZetaCached(cache = "parent", key = "#p0")
    public String load(String id) {
      return "parent-" + id;
    }
  }

  interface AddedCachedView {
    @ZetaCached(cache = "added", key = "#p0")
    String load(String id);
  }

  static class InheritedAndAddedReader extends InheritedCachedReader implements AddedCachedView {}

  @Test
  @DisplayName("an inherited superclass verb conflicts with a newly implemented interface verb")
  void inheritedVerbConflictsWithANewInterfaceVerb() throws NoSuchMethodException {
    Method load = InheritedAndAddedReader.class.getMethod("load", String.class);

    assertThatThrownBy(() -> resolver.matches(load, InheritedAndAddedReader.class))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("conflicting @ZetaCached");
  }

  interface DeprecatedRenamedParameterReader {
    @ZetaCached(cache = "users", key = "#id")
    String load(String id);
  }

  static class DeprecatedRenamedParameterReaderImpl implements DeprecatedRenamedParameterReader {

    @Deprecated
    @Override
    public String load(String value) {
      return "impl-" + value;
    }
  }

  @Test
  @DisplayName("an unrelated implementation annotation does not steal parameter names from the interface")
  void unrelatedImplementationAnnotationDoesNotStealParameterNames() {
    missRunsBody();

    DeprecatedRenamedParameterReader service = jdkProxy(
      new DeprecatedRenamedParameterReaderImpl(),
      DeprecatedRenamedParameterReader.class
    );

    assertThat(service.load("42")).isEqualTo("impl-42");
    assertThat(lastKey).isEqualTo(key("users", "42"));
  }

  /** A proxy with no advice: models a FactoryBean product or a scoped target that is itself a proxy. */
  private static <T> T plainProxy(Object target, Class<T> iface) {
    ProxyFactory factory = new ProxyFactory();
    factory.setTarget(target);
    factory.setInterfaces(iface);
    return iface.cast(factory.getProxy());
  }

  interface NestedProxyReader {
    String load(String id);
  }

  static class NestedProxyReaderImpl implements NestedProxyReader {

    final AtomicInteger calls = new AtomicInteger();

    @Override
    @ZetaCached(cache = "users", key = "#p0")
    public String load(String id) {
      calls.incrementAndGet();
      return "real-" + id;
    }
  }

  @Test
  @DisplayName("a target that is itself a proxy is unwrapped once for the context and the background refresh")
  void nestedProxyTargetIsUnwrapped() throws Throwable {
    NestedProxyReaderImpl target = new NestedProxyReaderImpl();
    NestedProxyReader nested = plainProxy(target, NestedProxyReader.class);

    Method method = NestedProxyReaderImpl.class.getMethod("load", String.class);
    MethodInvocation invocation = mock(MethodInvocation.class);
    when(invocation.getMethod()).thenReturn(method);
    when(invocation.getThis()).thenReturn(nested);
    when(invocation.getArguments()).thenReturn(new Object[] { "42" });
    when(invocation.proceed()).thenAnswer(ignored -> target.load("42"));

    missRunsBody();
    assertThat(interceptor.invoke(invocation)).isEqualTo("real-42");
    assertThat(target.calls).hasValue(1);

    // A refresh must invoke the underlying target, not the inner proxy, so the
    // inner proxy's advice chain (if any) is not re-entered.
    CacheLoader<String, Object> refresh = lastLoader;
    assertThat(refresh.reload(key("users", "42"))).isEqualTo("real-42");
    assertThat(target.calls).hasValue(2);
  }

  static class NoArgReader {

    @ZetaCached(cache = "users")
    public String load() {
      return "v";
    }
  }

  @Test
  @DisplayName("a custom invocation reporting null arguments is treated as no arguments")
  void nullArgumentsAreNormalized() throws Throwable {
    NoArgReader target = new NoArgReader();
    MethodInvocation invocation = mock(MethodInvocation.class);
    when(invocation.getMethod()).thenReturn(NoArgReader.class.getMethod("load"));
    when(invocation.getThis()).thenReturn(target);
    when(invocation.getArguments()).thenReturn(null);
    when(invocation.proceed()).thenReturn("v");

    missRunsBody();
    assertThat(interceptor.invoke(invocation)).isEqualTo("v");
  }

  interface Audited {
    String audit();
  }

  static class AuditMixin implements Audited {

    @Override
    public String audit() {
      return "audit";
    }
  }

  interface IntroducedReader {
    String load(String id);
  }

  static class IntroducedReaderImpl implements IntroducedReader {

    final AtomicInteger calls = new AtomicInteger();

    @Override
    @ZetaCached(cache = "users", key = "#p0")
    public String load(String id) {
      calls.incrementAndGet();
      return "real-" + id;
    }
  }

  @Test
  @DisplayName("an introduced (mixin) interface does not block the target unwrap or the refresh")
  void introducedInterfaceDoesNotBlockTheUnwrap() {
    IntroducedReaderImpl target = new IntroducedReaderImpl();

    // The inner proxy carries both the Zeta advice and a mixin the target does not implement.
    ProxyFactory inner = new ProxyFactory(target);
    inner.setInterfaces(IntroducedReader.class);
    inner.addAdvisor(
      new DefaultIntroductionAdvisor(new DelegatingIntroductionInterceptor(new AuditMixin()), Audited.class)
    );
    inner.addAdvisor(cacheAdvisor());
    IntroducedReader nested = (IntroducedReader) inner.getProxy();

    ProxyFactory outer = new ProxyFactory(nested);
    outer.setInterfaces(IntroducedReader.class);
    outer.addAdvisor(cacheAdvisor());
    IntroducedReader service = (IntroducedReader) outer.getProxy();

    missRunsBody();
    assertThat(service.load("42")).isEqualTo("real-42");
    assertThat(target.calls).hasValue(1);

    // A refresh must reach the real target, not the inner proxy: re-entering the advice chain
    // would serve the stale hit instead of running the body.
    hitServes("stale-42");
    CacheLoader<String, Object> refresh = lastLoader;
    assertThat(refresh.reload(key("users", "42"))).isEqualTo("real-42");
    assertThat(target.calls).hasValue(2);
  }

  static class AnnotatedObjectMethod {

    @ZetaCached(cache = "users")
    @Override
    public String toString() {
      return "annotated";
    }
  }

  @Test
  @DisplayName("an Object method never matches, including an annotated override on the bean")
  void objectMethodsNeverMatch() throws NoSuchMethodException {
    assertThat(resolver.matches(Object.class.getMethod("toString"), AnnotatedObjectMethod.class)).isFalse();
    assertThat(
      resolver.matches(AnnotatedObjectMethod.class.getMethod("toString"), AnnotatedObjectMethod.class)
    ).isFalse();
  }

  static class NestedArrayReader {

    @ZetaCached(cache = "users", key = "'k'")
    public String load(byte[][] ids) {
      return ids[0][0] + ":" + ids[1][0];
    }
  }

  @Test
  @DisplayName("a nested array argument is snapshotted deeply for a later refresh")
  void nestedArrayArgumentsAreSnapshottedDeeply() {
    NestedArrayReader service = proxy(new NestedArrayReader());
    byte[][] ids = { { 1 }, { 2 } };

    missRunsBody();
    assertThat(service.load(ids)).isEqualTo("1:2");

    CacheLoader<String, Object> refresh = lastLoader;
    ids[0][0] = 9;
    assertThat(refresh.reload(key("users", "k"))).as("the refresh sees the pre-call nested value").isEqualTo("1:2");
  }

  static class ObjectArrayReader {

    @ZetaCached(cache = "users", key = "'k'")
    public String load(Object[] payload) {
      return String.valueOf(((byte[]) payload[0])[0]);
    }
  }

  @Test
  @DisplayName("an Object[] argument is snapshotted through the arrays its elements hold")
  void objectArrayArgumentsAreSnapshottedDeeply() {
    ObjectArrayReader service = proxy(new ObjectArrayReader());
    Object[] payload = { new byte[] { 1 } };

    missRunsBody();
    assertThat(service.load(payload)).isEqualTo("1");

    CacheLoader<String, Object> refresh = lastLoader;
    ((byte[]) payload[0])[0] = 9;
    assertThat(refresh.reload(key("users", "k"))).as("the refresh sees the pre-call element value").isEqualTo("1");
  }

  static class VarargsArrayReader {

    @ZetaCached(cache = "users", key = "'k'")
    public String load(Object... payload) {
      return String.valueOf(((byte[]) payload[1])[0]);
    }
  }

  @Test
  @DisplayName("an Object... tail is snapshotted through the arrays its elements hold")
  void varargsArrayArgumentsAreSnapshottedDeeply() {
    VarargsArrayReader service = proxy(new VarargsArrayReader());
    byte[] bytes = { 1 };

    missRunsBody();
    assertThat(service.load("k", bytes)).isEqualTo("1");

    CacheLoader<String, Object> refresh = lastLoader;
    bytes[0] = 9;
    assertThat(refresh.reload(key("users", "k"))).as("the refresh sees the pre-call varargs value").isEqualTo("1");
  }

  static class CyclicArrayReader {

    @ZetaCached(cache = "users", key = "'k'")
    public String load(Object[][] matrix) {
      return String.valueOf(matrix[0] == matrix);
    }
  }

  @Test
  @DisplayName("a cyclic array argument is snapshotted once per array and keeps the cycle")
  void cyclicArrayArgumentsAreSnapshottedWithoutOverflow() {
    CyclicArrayReader service = proxy(new CyclicArrayReader());
    Object[][] matrix = new Object[1][];
    matrix[0] = matrix;

    missRunsBody();
    assertThat(service.load(matrix)).isEqualTo("true");

    CacheLoader<String, Object> refresh = lastLoader;
    matrix[0] = null;
    assertThat(refresh.reload(key("users", "k"))).as("the snapshot keeps the cycle").isEqualTo("true");
  }

  static class ArgumentViewReader {

    private Object conditionView;

    @ZetaCached(cache = "users", key = "#root.target.keyView(#p0)", condition = "#root.target.conditionView(#p0)")
    public String load(String[] ids) {
      return "body";
    }

    public boolean conditionView(String[] ids) {
      conditionView = ids;
      return true;
    }

    public String keyView(String[] ids) {
      return conditionView == ids ? "same" : "different";
    }
  }

  @Test
  @DisplayName("a read condition and key read one argument snapshot")
  void readConditionAndKeyShareOneArgumentView() {
    ArgumentViewReader service = proxy(new ArgumentViewReader());

    missRunsBody();
    assertThat(service.load(new String[] { "a" })).isEqualTo("body");
    assertThat(lastKey).isEqualTo(key("users", "same"));
  }

  interface RepositoryView {
    @ZetaCached(cache = "users", key = "#id")
    String load(String id);
  }

  /** The Spring Data shape: the interface proxy's singleton target does not implement the interface. */
  static class RepositoryTarget {

    final AtomicInteger calls = new AtomicInteger();

    String load(String id) {
      calls.incrementAndGet();
      return "real-" + id;
    }
  }

  @Test
  @DisplayName("an interface proxy whose target does not implement the interface still caches and reloads")
  void interfaceProxyTargetDoesNotBypassThePlan() throws Throwable {
    RepositoryTarget repositoryTarget = new RepositoryTarget();
    TargetSource targetSource = mock(TargetSource.class);
    when(targetSource.isStatic()).thenReturn(true);
    when(targetSource.getTarget()).thenReturn(repositoryTarget);
    RepositoryView nested = (RepositoryView) Proxy.newProxyInstance(
      RepositoryView.class.getClassLoader(),
      new Class<?>[] { RepositoryView.class, Advised.class },
      (proxy, method, args) -> {
        if ("getTargetSource".equals(method.getName())) {
          return targetSource;
        }
        if ("getTargetClass".equals(method.getName())) {
          return null;
        }
        return repositoryTarget.load((String) args[0]);
      }
    );

    Method method = RepositoryView.class.getMethod("load", String.class);
    MethodInvocation invocation = mock(MethodInvocation.class);
    when(invocation.getMethod()).thenReturn(method);
    when(invocation.getThis()).thenReturn(nested);
    when(invocation.getArguments()).thenReturn(new Object[] { "42" });
    when(invocation.proceed()).thenAnswer(ignored -> nested.load("42"));

    missRunsBody();
    assertThat(interceptor.invoke(invocation)).isEqualTo("real-42");
    assertThat(lastKey).isEqualTo(key("users", "42"));

    CacheLoader<String, Object> refresh = lastLoader;
    assertThat(refresh.reload(key("users", "42"))).isEqualTo("real-42");
    assertThat(repositoryTarget.calls).hasValue(2);
  }

  static class LoaderThreadService {

    @ZetaCached(cache = "users", key = "#id")
    public String load(String id) {
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("a retained loader releases the invocation thread before the read returns")
  void retainedLoaderReleasesTheInvocationThread() throws Throwable {
    LoaderThreadService target = new LoaderThreadService();
    Method reported = LoaderThreadService.class.getMethod("load", String.class);
    MethodInvocation invocation = mock(MethodInvocation.class);
    when(invocation.getMethod()).thenReturn(reported);
    when(invocation.getThis()).thenReturn(target);
    when(invocation.getArguments()).thenReturn(new Object[] { "42" });
    when(invocation.proceed()).thenReturn("body-42");

    missRunsBody();
    assertThat(interceptor.invoke(invocation)).isEqualTo("body-42");

    Field thread = lastLoader.getClass().getDeclaredField("invocationThread");
    thread.setAccessible(true);
    assertThat(thread.get(lastLoader)).isNull();
  }

  interface GenericRepository<T, ID> {
    T find(ID id);
  }

  interface StringRepository extends GenericRepository<String, String> {
    @Override
    @ZetaCached(cache = "users", key = "#id")
    String find(String id);
  }

  /** The Spring Data shape after generic erasure: only {@code find(Object)} exists on the target. */
  static class ErasedStringRepository<T, ID> implements GenericRepository<T, ID> {

    final AtomicInteger calls = new AtomicInteger();

    @Override
    @SuppressWarnings("unchecked")
    public T find(ID id) {
      calls.incrementAndGet();
      return (T) ("erased-" + id);
    }
  }

  @Test
  @DisplayName("a generic sub-interface refresh re-enters the proxy that owns the query")
  void genericSubInterfaceRefreshReentersTheQueryProxy() {
    ErasedStringRepository<String, String> target = new ErasedStringRepository<>();
    AtomicInteger queryCalls = new AtomicInteger();
    MethodInterceptor queryExecutor = invocation -> {
      queryCalls.incrementAndGet();
      return target.find((String) invocation.getArguments()[0]);
    };
    StringRepository service = (StringRepository) jdkProxyWith(
      target,
      new Class<?>[] { StringRepository.class },
      queryExecutor
    );

    hitServes("cached");
    assertThat(service.find("42")).isEqualTo("cached");
    assertThat(lastKey).isEqualTo(key("users", "42"));

    CacheLoader<String, Object> refresh = lastLoader;
    assertThat(refresh.reload(lastKey))
      .as("the child-interface refresh runs the query through the proxy, not the erased base method")
      .isEqualTo("erased-42");
    assertThat(queryCalls).as("the refresh re-entered the proxy chain").hasValue(1);
    assertThat(target.calls).hasValue(1);

    hitServes("cached");
    GenericRepository<String, String> parent = service;
    assertThat(parent.find("7")).isEqualTo("cached");
    assertThat(lastKey).isEqualTo(key("users", "7"));

    refresh = lastLoader;
    assertThat(refresh.reload(lastKey))
      .as("a parent-interface call dispatches through the child's bridge, so it refreshes through the proxy")
      .isEqualTo("erased-7");
    assertThat(queryCalls).as("the parent view reaches the query executor too").hasValue(2);
    assertThat(target.calls).hasValue(2);
  }

  interface ErasedCrudRepository<T, ID> {
    Optional<T> findById(ID id);
  }

  interface ErasedUserRepository extends ErasedCrudRepository<String, String> {
    @Override
    @ZetaCached(cache = "users", key = "#id")
    Optional<String> findById(String id);
  }

  /** The Spring Data shape: the target implements the parent interface only. */
  static class ErasedSimpleRepository implements ErasedCrudRepository<String, Object> {

    final AtomicInteger calls = new AtomicInteger();

    @Override
    public Optional<String> findById(Object id) {
      calls.incrementAndGet();
      return Optional.of("db-" + id);
    }
  }

  @Test
  @DisplayName("a refresh reaches a target that implements only the repository's erased parent interface")
  void refreshResolvesTheErasedSuperInterfaceMethod() throws Throwable {
    ErasedSimpleRepository target = new ErasedSimpleRepository();
    Method method = ErasedUserRepository.class.getMethod("findById", String.class);
    MethodInvocation invocation = mock(MethodInvocation.class);
    when(invocation.getMethod()).thenReturn(method);
    when(invocation.getThis()).thenReturn(target);
    when(invocation.getArguments()).thenReturn(new Object[] { "42" });
    when(invocation.proceed()).thenReturn(Optional.of("db-42"));

    missRunsBody();
    assertThat(interceptor.invoke(invocation)).isEqualTo(Optional.of("db-42"));
    assertThat(lastKey).isEqualTo(key("users", "42"));

    CacheLoader<String, Object> refresh = lastLoader;
    assertThat(refresh.reload(lastKey)).isEqualTo("db-42");
    assertThat(target.calls).hasValue(1);
  }

  interface ParentQueryView<T, ID> {
    T find(ID id);
  }

  interface ChildQueryView extends ParentQueryView<String, String> {
    @Override
    @ZetaCached(cache = "users", key = "#id")
    String find(String id);
  }

  /** The repository shape: the proxy's target implements only the erased parent view. */
  static class ParentOnlyQueryTarget implements ParentQueryView<String, Object> {

    final AtomicInteger calls = new AtomicInteger();

    @Override
    public String find(Object id) {
      calls.incrementAndGet();
      return "base-" + id;
    }
  }

  @Test
  @DisplayName("a parent-view call refreshes through the proxy that owns the query")
  void parentViewRefreshReentersTheQueryProxy() {
    ParentOnlyQueryTarget target = new ParentOnlyQueryTarget();
    AtomicInteger queryCalls = new AtomicInteger();
    MethodInterceptor queryExecutor = invocation -> {
      if ("find".equals(invocation.getMethod().getName())) {
        queryCalls.incrementAndGet();
        return "query-" + invocation.getArguments()[0];
      }
      return invocation.proceed();
    };
    ChildQueryView service = (ChildQueryView) jdkProxyWith(
      (Object) target,
      new Class<?>[] { ChildQueryView.class },
      queryExecutor
    );

    ParentQueryView<String, String> parent = service;
    missRunsBody();
    assertThat(parent.find("7")).isEqualTo("query-7");
    assertThat(lastKey).isEqualTo(key("users", "7"));

    CacheLoader<String, Object> refresh = lastLoader;
    assertThat(refresh.reload(lastKey))
      .as("the erased parent call must not become the refresh receiver")
      .isEqualTo("query-7");
    assertThat(queryCalls).as("the refresh re-entered the query executor").hasValue(2);
    assertThat(target.calls).as("the base implementation was never invoked").hasValue(0);
  }

  @Test
  @DisplayName("a child-view call publishes the plan table under the proxy class")
  void childViewCallPublishesTheProxyTable() {
    ParentOnlyQueryTarget target = new ParentOnlyQueryTarget();
    ChildQueryView service = (ChildQueryView) jdkProxyWith(
      (Object) target,
      new Class<?>[] { ChildQueryView.class },
      (MethodInterceptor) invocation -> "query-" + invocation.getArguments()[0]
    );

    missRunsBody();
    assertThat(service.find("7")).isEqualTo("query-7");

    PlanResolver.PlanTable table = resolver.cachedTable(service.getClass());
    assertThat(table).as("the next call answers from two map reads").isNotNull();
    assertThat(table.userClass()).isEqualTo(ChildQueryView.class);
  }

  interface QueryRepository {
    @ZetaCached(cache = "users", key = "#id")
    String findById(Long id);
  }

  static class MutatingArrayService {

    @ZetaCached(cache = "users", key = "#p0[0]", unless = "#p0[0] < 0")
    public String load(int[] ids) {
      ids[0] = -ids[0];
      return "body";
    }
  }

  @Test
  @DisplayName("a body that rewrites its array argument cannot change what the loader evaluates")
  void arrayArgumentSnapshotDrivesTheLoaderGuard() {
    MutatingArrayService service = proxy(new MutatingArrayService());

    missRunsBody();
    assertThat(service.load(new int[] { 7 })).isEqualTo("body");
    assertThat(lastKey).isEqualTo(key("users", 7));
    assertThat(lastShouldCache).isTrue();
  }

  interface ParentRepository<T, ID> {
    T find(ID id);
  }

  interface TargetClassRepository extends ParentRepository<String, String> {
    @Override
    @ZetaCached(cache = "users", key = "#root.targetClass.simpleName + ':' + #id")
    String find(String id);
  }

  static class ParentOnlyRepository implements ParentRepository<String, Object> {

    @Override
    public String find(Object id) {
      return "db-" + id;
    }
  }

  @Test
  @DisplayName("a proxy-interface plan reports the annotated child view as #root.targetClass")
  void proxyInterfacePlanUpdatesTargetClass() throws Throwable {
    ParentOnlyRepository target = new ParentOnlyRepository();
    Object proxy = jdkProxy(target, TargetClassRepository.class);
    Method parent = ParentRepository.class.getMethod("find", Object.class);
    ProxyMethodInvocation invocation = mock(ProxyMethodInvocation.class);
    when(invocation.getMethod()).thenReturn(parent);
    when(invocation.getThis()).thenReturn(target);
    when(invocation.getProxy()).thenReturn(proxy);
    when(invocation.getArguments()).thenReturn(new Object[] { "42" });
    when(invocation.proceed()).thenReturn("db-42");

    missRunsBody();
    assertThat(interceptor.invoke(invocation)).isEqualTo("db-42");
    assertThat(lastKey).isEqualTo(key("users", "TargetClassRepository:42"));
  }

  interface RawProxyView {
    @ZetaCached(cache = "users", key = "#root.targetClass.simpleName + ':' + #p0")
    String load(String id);
  }

  @Test
  @DisplayName("a non-Advised JDK proxy target reports its interface view as #root.targetClass")
  void rawJdkProxyTargetClassIsTheInterfaceView() {
    RawProxyView raw = (RawProxyView) Proxy.newProxyInstance(
      RawProxyView.class.getClassLoader(),
      new Class<?>[] { RawProxyView.class },
      (proxy, method, args) -> "raw-" + args[0]
    );

    missRunsBody();
    assertThat(proxy(raw).load("42")).isEqualTo("raw-42");
    assertThat(lastKey).isEqualTo(key("users", "RawProxyView:42"));
  }

  interface FallbackView {
    @ZetaCached(cache = "users", key = "#id")
    String load(String id);
  }

  /** The receiver is the underlying implementation, which does not implement the view. */
  static class FallbackTarget {

    final AtomicInteger calls = new AtomicInteger();

    String load(String id) {
      calls.incrementAndGet();
      return "target-" + id;
    }
  }

  interface ReentrantRefreshView {
    @ZetaCached(cache = "users", key = "#id")
    String load(String id);
  }

  /** The receiver cannot invoke the view, so the refresh falls back to the advice proxy itself. */
  static class ReentrantRefreshTarget {}

  @Test
  @DisplayName("a refresh that invokes the advice proxy bypasses the cache advice instead of serving stale")
  void refreshThroughTheAdviceProxyDoesNotReenterTheCacheRead() {
    // A target keeps the pointcut's target class non-null; the terminal interceptor is the body,
    // so a reflective invocation on the proxy reaches it through the advice chain exactly like
    // the repository fallback does.
    ProxyFactory factory = new ProxyFactory();
    factory.setTarget(new ReentrantRefreshTarget());
    factory.setInterfaces(ReentrantRefreshView.class);
    factory.addAdvisor(cacheAdvisor());
    factory.addAdvisor(
      new DefaultPointcutAdvisor((MethodInterceptor) invocation -> "real-" + invocation.getArguments()[0])
    );
    ReentrantRefreshView service = (ReentrantRefreshView) factory.getProxy();

    missRunsBody();
    assertThat(service.load("42")).isEqualTo("real-42");

    // The engine serves this stale value while the refresh runs. A re-entered cache read would
    // return it as the refreshed value and renew the stale entry.
    hitServes("stale-42");
    CacheLoader<String, Object> refresh = lastLoader;
    assertThat(refresh.reload(key("users", "42"))).isEqualTo("real-42");
  }

  interface StaticHelperParent {
    String load(CharSequence id);

    static String load(String id) {
      return "static-" + id;
    }
  }

  interface StaticHelperChild extends StaticHelperParent {
    @ZetaCached(cache = "users", key = "#id")
    String load(String id);
  }

  static class StaticHelperTarget implements StaticHelperParent {

    @Override
    public String load(CharSequence id) {
      return "parent-" + id;
    }
  }

  @Test
  @DisplayName("a static helper on a supertype is not the erased refresh method")
  void staticHelperOnASupertypeIsSkipped() throws Throwable {
    assertThat(refreshOf(new StaticHelperTarget(), StaticHelperChild.class)).isEqualTo("parent-42");
  }

  interface StaticCollisionParent {
    String load(CharSequence id);
  }

  interface StaticCollisionChild extends StaticCollisionParent {
    @ZetaCached(cache = "users", key = "#id")
    String load(String id);
  }

  static class StaticCollisionTarget implements StaticCollisionParent {

    public static String load(String id) {
      return "static-" + id;
    }

    @Override
    public String load(CharSequence id) {
      return "parent-" + id;
    }
  }

  @Test
  @DisplayName("a same-signature static method on the target is not the refresh body")
  void staticMethodOnTheTargetIsSkipped() throws Throwable {
    assertThat(refreshOf(new StaticCollisionTarget(), StaticCollisionChild.class)).isEqualTo("parent-42");
  }

  interface PrivateHelperParent {
    String load(CharSequence id);

    private String load(String id) {
      return "private-" + id;
    }
  }

  interface PrivateHelperChild extends PrivateHelperParent {
    @ZetaCached(cache = "users", key = "#id")
    String load(String id);
  }

  static class PrivateHelperTarget implements PrivateHelperParent {

    @Override
    public String load(CharSequence id) {
      return "parent-" + id;
    }
  }

  @Test
  @DisplayName("a private helper on a supertype is not the erased refresh method")
  void privateHelperOnASupertypeIsSkipped() throws Throwable {
    assertThat(refreshOf(new PrivateHelperTarget(), PrivateHelperChild.class)).isEqualTo("parent-42");
  }

  interface OverloadedParent {
    String load(Object id);

    String load(CharSequence id);
  }

  interface OverloadedChild extends OverloadedParent {
    @ZetaCached(cache = "users", key = "#id")
    String load(String id);
  }

  static class OverloadedTarget implements OverloadedParent {

    @Override
    public String load(Object id) {
      return "object-" + id;
    }

    @Override
    public String load(CharSequence id) {
      return "charsequence-" + id;
    }
  }

  @Test
  @DisplayName("the erased refresh method is the most specific accepting overload")
  void erasedOverloadPicksTheMostSpecificSignature() throws Throwable {
    assertThat(refreshOf(new OverloadedTarget(), OverloadedChild.class)).isEqualTo("charsequence-42");
  }

  interface GenericBridgeRoot<T> {
    String load(T id);
  }

  interface NumberBridgeRoot<U extends Number> extends GenericBridgeRoot<U> {
    @Override
    @ZetaCached(cache = "users", key = "#id")
    String load(U id);
  }

  static class IntegerBridgeRoot implements NumberBridgeRoot<Integer> {

    @Override
    public String load(Integer id) {
      return "integer-" + id;
    }
  }

  interface DiamondRoot<T> {
    @SuppressWarnings("unchecked")
    default T find(T id) {
      return (T) Long.valueOf(1L);
    }
  }

  interface DiamondNumeric<T extends Number> extends DiamondRoot<T> {
    @SuppressWarnings("unchecked")
    @Override
    default T find(T id) {
      return (T) Long.valueOf(2L);
    }
  }

  interface DiamondLongRepo extends DiamondRoot<Long>, DiamondNumeric<Long> {
    @ZetaCached(cache = "users", key = "#id")
    @Override
    Long find(Long id);
  }

  static class DiamondNumericTarget implements DiamondNumeric<Long> {}

  @Test
  @DisplayName("the erased walk compares every reachable supertype instead of returning the first")
  void erasedWalkPicksTheMostSpecificDiamondDeclaration() throws Throwable {
    DiamondNumericTarget target = new DiamondNumericTarget();
    Method reported = DiamondLongRepo.class.getMethod("find", Long.class);
    MethodInvocation invocation = mock(MethodInvocation.class);
    when(invocation.getMethod()).thenReturn(reported);
    when(invocation.getThis()).thenReturn(target);
    when(invocation.getArguments()).thenReturn(new Object[] { 42L });
    when(invocation.proceed()).thenReturn("body");

    missRunsBody();
    assertThat(interceptor.invoke(invocation)).isEqualTo("body");

    assertThat(lastLoader.reload(key("users", "42"))).isEqualTo(2L);
  }

  @Test
  @DisplayName("a generic bridge resolves to a method the target can invoke reflectively")
  void widestBridgeIsInvocable() throws Throwable {
    IntegerBridgeRoot target = new IntegerBridgeRoot();
    Method reported = NumberBridgeRoot.class.getMethod("load", Number.class);
    MethodInvocation invocation = mock(MethodInvocation.class);
    when(invocation.getMethod()).thenReturn(reported);
    when(invocation.getThis()).thenReturn(target);
    when(invocation.getArguments()).thenReturn(new Object[] { 42 });
    when(invocation.proceed()).thenReturn("body");

    PlanResolver resolver = new PlanResolver(null);
    CachePlan plan = resolver.lookupPlanForMethod(reported, target.getClass());
    assertThat(plan).isNotNull();
    CachePlan.Read read = (CachePlan.Read) plan;
    assertThat(resolver.targetMethod(read.specific(), target).getName()).isEqualTo("load");
  }

  /** Run one cached call and return what its background refresh produces. */
  private Object refreshOf(Object target, Class<?> view) throws Throwable {
    Method method = view.getMethod("load", String.class);
    MethodInvocation invocation = mock(MethodInvocation.class);
    when(invocation.getMethod()).thenReturn(method);
    when(invocation.getThis()).thenReturn(target);
    when(invocation.getArguments()).thenReturn(new Object[] { "42" });
    when(invocation.proceed()).thenReturn("body-42");

    missRunsBody();
    assertThat(interceptor.invoke(invocation)).isEqualTo("body-42");
    return lastLoader.reload(lastKey);
  }

  interface LeftView {
    @ZetaCached(cache = "shared", key = "#id")
    String load(String id);
  }

  interface RightView {
    String load(String id);
  }

  interface DiamondView extends LeftView, RightView {}

  @Test
  @DisplayName("sibling interfaces with the same method signature share one plan")
  void siblingInterfaceMethodsShareOnePlan() throws NoSuchMethodException {
    Method right = RightView.class.getMethod("load", String.class);

    assertThat(resolver.matches(right, DiamondView.class)).isTrue();
  }

  interface ConflictingLeftView {
    @ZetaCached(cache = "shared", key = "#id")
    String load(String id);
  }

  interface ConflictingRightView {
    @ZetaPut(cache = "shared", key = "#id")
    String load(String id);
  }

  interface ConflictingDiamondView extends ConflictingLeftView, ConflictingRightView {}

  @Test
  @DisplayName("sibling interfaces with conflicting verbs fail resolution instead of splitting")
  void siblingInterfaceVerbsConflict() throws NoSuchMethodException {
    Method left = ConflictingLeftView.class.getMethod("load", String.class);

    assertThatThrownBy(() -> resolver.matches(left, ConflictingDiamondView.class))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("mutually exclusive");
  }

  interface CommonView {
    String load(String id);
  }

  interface AnnotatedCommonView extends CommonView {
    @Override
    @ZetaCached(cache = "annotated", key = "#id")
    String load(String id);
  }

  interface PlainCommonView extends CommonView {
    @Override
    String load(String id);
  }

  static class CommonViewImpl implements AnnotatedCommonView, PlainCommonView {

    @Override
    public String load(String id) {
      return "impl-" + id;
    }
  }

  @Test
  @DisplayName("a verb on one of two maximal interfaces sharing a parent resolves without ambiguity")
  void oneAnnotatedSiblingUnderACommonParentResolves() throws NoSuchMethodException {
    CommonView service = jdkProxy(new CommonViewImpl(), AnnotatedCommonView.class, PlainCommonView.class);
    Method common = CommonView.class.getMethod("load", String.class);

    assertThat(resolver.matches(common, service.getClass())).isTrue();

    hitServes("cached");
    assertThat(service.load("42")).isEqualTo("cached");
    assertThat(lastKey).isEqualTo(key("annotated", "42"));
  }

  interface OtherAnnotatedCommonView extends CommonView {
    @Override
    @ZetaCached(cache = "other", key = "#id")
    String load(String id);
  }

  static class ConflictingCommonViewImpl implements AnnotatedCommonView, OtherAnnotatedCommonView {

    @Override
    public String load(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("two maximal interfaces sharing a parent still conflict when their verbs disagree")
  void twoAnnotatedSiblingsUnderACommonParentConflict() throws NoSuchMethodException {
    CommonView service = jdkProxy(
      new ConflictingCommonViewImpl(),
      AnnotatedCommonView.class,
      OtherAnnotatedCommonView.class
    );
    Method common = CommonView.class.getMethod("load", String.class);

    assertThatThrownBy(() -> resolver.matches(common, service.getClass()))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("conflicting @ZetaCached")
      .hasMessageContaining("annotated")
      .hasMessageContaining("other");
  }

  interface SameAnnotatedCommonView extends CommonView {
    @Override
    @ZetaCached(cache = "annotated", key = "#id")
    String load(String id);
  }

  static class SameCommonViewImpl implements AnnotatedCommonView, SameAnnotatedCommonView {

    @Override
    public String load(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("two maximal interfaces sharing a parent and declaring equal verbs resolve to one plan")
  void equalVerbsOnSiblingsUnderACommonParentAgree() throws NoSuchMethodException {
    CommonView service = jdkProxy(new SameCommonViewImpl(), AnnotatedCommonView.class, SameAnnotatedCommonView.class);
    Method common = CommonView.class.getMethod("load", String.class);

    assertThat(resolver.matches(common, service.getClass())).isTrue();
  }

  @Test
  @DisplayName("a JDK proxy's parent-interface method matches and serves the child interface's plan")
  void jdkProxyParentInterfaceMethodMatchesTheChildPlan() throws NoSuchMethodException {
    ErasedStringRepository<String, String> target = new ErasedStringRepository<>();
    StringRepository service = (StringRepository) jdkProxy(target, StringRepository.class);
    Method parent = GenericRepository.class.getMethod("find", Object.class);

    assertThat(resolver.matches(parent, service.getClass())).isTrue();

    hitServes("cached");
    GenericRepository<String, String> parentView = service;
    assertThat(parentView.find("7")).isEqualTo("cached");
    assertThat(lastKey).isEqualTo(key("users", "7"));
  }

  interface ParentView {
    @ZetaCached(cache = "parent", key = "#p0")
    String load(String id);
  }

  interface ChildView extends ParentView {
    @Override
    @ZetaCached(cache = "child", key = "#p0")
    String load(String id);
  }

  static class ChildViewImpl implements ChildView {

    @Override
    public String load(String id) {
      return "impl-" + id;
    }
  }

  @Test
  @DisplayName("a proxy exposing both a child and its parent interface keeps the child plan")
  void proxyKeepsTheMoreSpecificInterfacePlan() {
    ChildView service = (ChildView) jdkProxy(new ChildViewImpl(), ChildView.class, ParentView.class);

    hitServes("cached");
    assertThat(service.load("7")).isEqualTo("cached");
    assertThat(lastKey).isEqualTo(key("child", "7"));
  }

  @Test
  @DisplayName("an unwrap stops before a target that no longer exposes the child interface")
  void advisedTargetKeepsTheChildInterfacePlan() throws Throwable {
    ErasedStringRepository<String, String> target = new ErasedStringRepository<>();
    StringRepository nested = plainProxy(target, StringRepository.class);
    Method parent = GenericRepository.class.getMethod("find", Object.class);
    MethodInvocation invocation = mock(MethodInvocation.class);
    when(invocation.getMethod()).thenReturn(parent);
    when(invocation.getThis()).thenReturn(nested);
    when(invocation.getArguments()).thenReturn(new Object[] { "7" });
    when(invocation.proceed()).thenAnswer(ignored -> target.find("7"));

    hitServes("cached");
    assertThat(interceptor.invoke(invocation)).isEqualTo("cached");
    assertThat(lastKey).isEqualTo(key("users", "7"));
  }

  @Test
  @DisplayName("a hit snapshots array arguments before the caller can mutate them")
  void hitSnapshotsArrayArguments() {
    ArrayArgumentReader service = proxy(new ArrayArgumentReader());
    String[] ids = { "a", "b" };

    hitServes("cached");
    assertThat(service.load(ids)).isEqualTo("cached");

    ids[0] = "changed";
    assertThat(lastLoader.reload(lastKey)).isEqualTo("a");
  }

  static class ConflictService {

    @ZetaCached(cache = "users", key = "#p0")
    @ZetaPut(cache = "users", key = "#p0")
    public String load(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("one method may carry one verb only")
  void verbsAreMutuallyExclusive() {
    assertRejectedAtStartup(() -> proxy(new ConflictService()), "mutually exclusive");
  }

  static class VoidReader {

    @ZetaCached(cache = "users", key = "#p0")
    public void load(String id) {
      // nothing to cache
    }
  }

  @Test
  @DisplayName("@ZetaCached rejects a void method")
  void voidReadIsRejected() {
    assertRejectedAtStartup(() -> proxy(new VoidReader()), "returns void");
  }

  static class BoxedVoidReader {

    @ZetaCached(cache = "users", key = "#p0")
    public Void load(String id) {
      return null;
    }
  }

  @Test
  @DisplayName("@ZetaCached rejects a java.lang.Void method: it can only return null")
  void boxedVoidReadIsRejected() {
    assertRejectedAtStartup(() -> proxy(new BoxedVoidReader()), "Void");
  }

  static class BlankNamespace {

    @ZetaCached(cache = "   ", key = "#p0")
    public String load(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("a blank cache namespace is rejected")
  void blankNamespaceIsRejected() {
    assertRejectedAtStartup(() -> proxy(new BlankNamespace()), "must not be blank");
  }

  static class NamedKeyReader {

    @ZetaCached(cache = "users", key = "#id")
    public String load(String id) {
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("a bare parameter-name key resolves to the argument directly")
  void namedSingleArgumentKey() {
    missRunsBody();
    proxy(new NamedKeyReader()).load("42");

    assertThat(lastKey).isEqualTo(key("users", "42"));
  }

  @Test
  @DisplayName("an explicit single-argument key and the default key address the same entry")
  void explicitSingleArgumentKeyMatchesDefaultKey() {
    missRunsBody();
    proxy(new DefaultKeySingleArg()).load("42");
    String defaultKey = lastKey;

    missRunsBody();
    proxy(new NamedKeyReader()).load("42");

    assertThat(lastKey).isEqualTo(defaultKey);
  }

  static class ResultNamedParameterReader {

    @ZetaCached(cache = "users", key = "#p0")
    public String load(String result) {
      return "v-" + result;
    }
  }

  @Test
  @DisplayName("a parameter named result is legal when the key does not bind #result")
  void parameterNamedResultIsLegal() {
    missRunsBody();
    proxy(new ResultNamedParameterReader()).load("42");

    assertThat(lastKey).isEqualTo(key("users", "42"));
  }

  static class ResultParameterKeyWriter {

    @ZetaPut(cache = "users", key = "#result")
    public String save(String result) {
      return "saved-" + result;
    }
  }

  @Test
  @DisplayName("a #result key resolves to the body result even when a parameter is named result")
  void resultKeyNeverBindsTheParameterOfTheSameName() {
    proxy(new ResultParameterKeyWriter()).save("42");

    verify(zeta).put(eq(key("users", "saved-42")), eq("saved-42"), any(PutPolicy.class));
  }

  static class PrefixedParameterNames {

    @ZetaCached(cache = "users", key = "#accountId")
    public String byAccount(String accountId, int page, String p01) {
      return "a-" + accountId;
    }

    @ZetaCached(cache = "users", key = "#page")
    public String byPage(String accountId, int page, String p01) {
      return "p-" + page;
    }

    @ZetaCached(cache = "users", key = "#p01")
    public String byNonCanonical(String accountId, int page, String p01) {
      return "n-" + p01;
    }

    @ZetaCached(cache = "users", key = "#a1")
    public String byPosition(String accountId, int page, String p01) {
      return "i-" + page;
    }
  }

  @Test
  @DisplayName("a parameter name starting with a/p is a name, never a positional reference")
  void prefixedParameterNamesTakeTheNamePath() {
    PrefixedParameterNames service = proxy(new PrefixedParameterNames());

    missRunsBody();
    service.byAccount("acme", 9, "p1");
    assertThat(lastKey).isEqualTo(key("users", "acme"));

    missRunsBody();
    service.byPage("acme", 9, "p1");
    assertThat(lastKey).isEqualTo(key("users", 9));

    missRunsBody();
    service.byNonCanonical("acme", 9, "p1");
    assertThat(lastKey).isEqualTo(key("users", "p1"));

    missRunsBody();
    service.byPosition("acme", 9, "p1");
    assertThat(lastKey).isEqualTo(key("users", 9));
  }

  static class ShadowedPositionalNameReader {

    @ZetaCached(cache = "users", key = "#p0")
    public String load(String id, String p0, String a0) {
      return "v";
    }
  }

  static class ShadowedPositionalNameSpelReader {

    /** Concatenation keeps the key off the fast path, so SpEL binds {@code #p0} itself. */
    @ZetaCached(cache = "users", key = "#p0 + ''")
    public String load(String id, String p0, String a0) {
      return "v";
    }
  }

  static class OutOfRangeKeyReader {

    @ZetaCached(cache = "users", key = "#p1")
    public String load(String id) {
      return id;
    }
  }

  static class OutOfRangeAliasNameReader {

    @ZetaCached(cache = "users", key = "#p1")
    public String load(String p1) {
      return p1;
    }
  }

  static class ZeroArgumentNamedKeyReader {

    @ZetaCached(cache = "users", key = "#foo")
    public String load() {
      return "v";
    }
  }

  @Test
  @DisplayName("an out-of-range #pN fails at proxy creation")
  void outOfRangeArgumentReferenceFailsAtStartup() {
    assertRejectedAtStartup(() -> proxy(new OutOfRangeKeyReader()), "addresses parameter 1");
  }

  @Test
  @DisplayName("a parameter named like an out-of-range positional alias cannot answer for it")
  void outOfRangeAliasParameterNameIsRejected() {
    assertRejectedAtStartup(() -> proxy(new OutOfRangeAliasNameReader()), "addresses parameter 1");
  }

  @Test
  @DisplayName("a zero-argument method reports an unknown variable, not missing parameter metadata")
  void zeroArgumentUnknownVariableIsNotBlamedOnParameterMetadata() {
    assertRejectedAtStartup(() -> proxy(new ZeroArgumentNamedKeyReader()), "unknown variable '#foo'");
  }

  static class PaddedOutOfRangeKeyReader {

    @ZetaCached(cache = "users", key = "  #p1  ")
    public String load(String id) {
      return id;
    }
  }

  static class PaddedOutOfRangeTtlReader {

    @ZetaCached(cache = "users", key = "#p0", hardTtl = "  #p1  ")
    public String load(String id) {
      return id;
    }
  }

  static class PaddedOutOfRangeUnlessReader {

    @ZetaCached(cache = "users", key = "#p0", unless = "  #p1  ")
    public String load(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("surrounding whitespace does not hide an out-of-range reference in key, TTL or unless")
  void paddedOutOfRangeReferencesFailAtStartup() {
    assertRejectedAtStartup(() -> proxy(new PaddedOutOfRangeKeyReader()), "cache key", "addresses parameter 1");
    assertRejectedAtStartup(() -> proxy(new PaddedOutOfRangeTtlReader()), "hardTtl", "addresses parameter 1");
    assertRejectedAtStartup(() -> proxy(new PaddedOutOfRangeUnlessReader()), "unless", "addresses parameter 1");
  }

  static class WhitespaceTtlReader {

    @ZetaCached(cache = "users", key = "#p0", hardTtl = " 60000 ", softTtl = "\t5000\n")
    public String load(String id) {
      return "v";
    }
  }

  @Test
  @DisplayName("TTL literals are stripped and folded to constants")
  void ttlLiteralsAreStripped() {
    WhitespaceTtlReader service = proxy(new WhitespaceTtlReader());
    missRunsBody();
    service.load("42");
    GetPolicy first = lastOptions;

    missRunsBody();
    service.load("43");

    assertThat(first.hardTtlMs().getAsLong()).isEqualTo(60_000L);
    assertThat(first.softTtlMs().getAsLong()).isEqualTo(5_000L);
    assertThat(lastOptions).as("both constants prebuild one options instance").isSameAs(first);
  }

  static class SignedTtlReader {

    @ZetaCached(cache = "users", key = "#p0", hardTtl = " 0 ", softTtl = " +5000 ")
    public String load(String id) {
      return "v";
    }
  }

  @Test
  @DisplayName("a signed TTL literal is folded: zero means inherit and a leading plus is accepted")
  void signedTtlLiteralsAreFolded() {
    SignedTtlReader service = proxy(new SignedTtlReader());
    missRunsBody();
    service.load("42");
    GetPolicy first = lastOptions;

    missRunsBody();
    service.load("43");

    assertThat(first.hardTtlMs().getAsLong()).isZero();
    assertThat(first.softTtlMs().getAsLong()).isEqualTo(5_000L);
    assertThat(lastOptions).as("both signed literals stay constants").isSameAs(first);
  }

  static class DisabledSoftTtlReader {

    @ZetaCached(cache = "users", key = "#p0", softTtl = "-1")
    public String load(String id) {
      return "v";
    }
  }

  @Test
  @DisplayName("softTtl = -1 is the explicit disable sentinel and reaches the policy unchanged")
  void minusOneSoftTtlMeansDisabled() {
    DisabledSoftTtlReader service = proxy(new DisabledSoftTtlReader());
    missRunsBody();
    service.load("42");

    assertThat(lastOptions.softTtlMs().getAsLong()).isEqualTo(-1L);
  }

  static class DisabledSoftTtlLongLiteralReader {

    @ZetaCached(cache = "users", key = "#p0", softTtl = "-1L")
    public String load(String id) {
      return "v";
    }
  }

  @Test
  @DisplayName("softTtl = -1L folds as a constant and keeps the disable sentinel (not 0 = inherit)")
  void minusOneLongLiteralSoftTtlStaysDisabled() {
    DisabledSoftTtlLongLiteralReader service = proxy(new DisabledSoftTtlLongLiteralReader());
    missRunsBody();
    service.load("42");
    GetPolicy first = lastOptions;

    missRunsBody();
    service.load("43");

    assertThat(first.softTtlMs().getAsLong()).isEqualTo(-1L);
    assertThat(lastOptions).as("a suffixed sentinel is still a constant").isSameAs(first);
  }

  static class NegativeHardTtlReader {

    @ZetaCached(cache = "users", key = "#p0", hardTtl = "-5")
    public String load(String id) {
      return "v";
    }
  }

  @Test
  @DisplayName("a negative hard TTL other than -1 is a startup error, not a silent inherit")
  void negativeHardTtlFailsStartup() {
    assertRejectedAtStartup(() -> proxy(new NegativeHardTtlReader()), "hardTtl", "negative");
  }

  static class NegativeSoftTtlReader {

    @ZetaCached(cache = "users", key = "#p0", softTtl = "-2")
    public String load(String id) {
      return "v";
    }
  }

  @Test
  @DisplayName("a negative soft TTL other than -1 is a startup error")
  void otherNegativeSoftTtlFailsStartup() {
    assertRejectedAtStartup(() -> proxy(new NegativeSoftTtlReader()), "softTtl", "negative");
  }

  static class OverflowingTtlReader {

    @ZetaCached(cache = "users", key = "#p0", hardTtl = "30 * 24 * 3600 * 1000")
    public String load(String id) {
      return "v";
    }
  }

  static class SuffixedTtlReader {

    @ZetaCached(cache = "users", key = "#p0", hardTtl = "60000L", softTtl = "5000L")
    public String load(String id) {
      return "v";
    }
  }

  @Test
  @DisplayName("a numeric SpEL literal is folded like a digits-only TTL")
  void suffixedTtlLiteralsAreFolded() {
    SuffixedTtlReader service = proxy(new SuffixedTtlReader());
    missRunsBody();
    service.load("42");
    GetPolicy first = lastOptions;

    missRunsBody();
    service.load("43");

    assertThat(first.hardTtlMs().getAsLong()).isEqualTo(60_000L);
    assertThat(first.softTtlMs().getAsLong()).isEqualTo(5_000L);
    assertThat(lastOptions).as("both suffixed literals prebuild one options instance").isSameAs(first);
  }

  static class UnaryPlusTtlReader {

    @ZetaCached(cache = "users", key = "#p0", hardTtl = "+60000L", softTtl = "+5000L")
    public String load(String id) {
      return "v";
    }
  }

  @Test
  @DisplayName("unary-plus TTL literals fold to constants instead of evaluating SpEL per call")
  void unaryPlusTtlLiteralsAreFolded() {
    UnaryPlusTtlReader service = proxy(new UnaryPlusTtlReader());
    missRunsBody();
    service.load("42");
    GetPolicy first = lastOptions;

    missRunsBody();
    service.load("43");

    assertThat(first.hardTtlMs().getAsLong()).isEqualTo(60_000L);
    assertThat(first.softTtlMs().getAsLong()).isEqualTo(5_000L);
    assertThat(lastOptions).as("unary-plus literals are constants").isSameAs(first);
  }

  static class HugeRealTtlReader {

    @ZetaCached(cache = "users", key = "#p0", hardTtl = "1e20")
    public String load(String id) {
      return "v";
    }
  }

  static class ExactTwoPow63TtlReader {

    @ZetaCached(cache = "users", key = "#p0", hardTtl = "9223372036854775808e0")
    public String load(String id) {
      return "v";
    }
  }

  static class TrailingZeroBigDecimalTtlReader {

    @ZetaCached(cache = "users", key = "#p0", hardTtl = "new java.math.BigDecimal('100.00')")
    public String load(String id) {
      return "v";
    }
  }

  @Test
  @DisplayName("a BigDecimal TTL whose fraction is only trailing zeros is still integral")
  void trailingZeroBigDecimalTtlIsFolded() {
    TrailingZeroBigDecimalTtlReader service = proxy(new TrailingZeroBigDecimalTtlReader());
    missRunsBody();
    service.load("42");

    assertThat(lastOptions.hardTtlMs().getAsLong()).isEqualTo(100L);
  }

  static class ConstantTtlFailureReader {

    @ZetaCached(cache = "users", key = "#p0", hardTtl = "1 / 0")
    public String load(String id) {
      return "v";
    }
  }

  static class UnresolvedTtlPlaceholderReader {

    @ZetaCached(cache = "users", key = "#p0", hardTtl = "${users.hardTtl}")
    public String load(String id) {
      return "v";
    }
  }

  @Test
  @DisplayName("an unresolved TTL placeholder names the attribute instead of reaching the SpEL parser")
  void unresolvedTtlPlaceholderFailsStartup() {
    assertRejectedAtStartup(
      () -> proxy(new UnresolvedTtlPlaceholderReader()),
      "hardTtl",
      "placeholder was not resolved"
    );
  }

  static class UnresolvedPlaceholderReader {

    @ZetaCached(cache = "${users.cache}", key = "#p0")
    public String load(String id) {
      return "v";
    }
  }

  @Test
  @DisplayName("an unresolved cache placeholder fails startup instead of becoming a literal namespace")
  void unresolvedCachePlaceholderFailsStartup() {
    assertRejectedAtStartup(() -> proxy(new UnresolvedPlaceholderReader()), "placeholder was not resolved");
  }

  static class ThrowingPlaceholderReader {

    @ZetaCached(cache = "${users.cache}", key = "#p0")
    public String load(String id) {
      return "v";
    }
  }

  @Test
  @DisplayName("a placeholder resolver failure names the attribute role and method")
  void placeholderResolverFailureNamesTheRole() {
    DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
    beanFactory.addEmbeddedValueResolver(value -> {
      throw new IllegalArgumentException("missing property");
    });
    resolver = new PlanResolver(beanFactory);
    interceptor = new ZetaCacheInterceptor(zeta, resolver);

    assertRejectedAtStartup(() -> proxy(new ThrowingPlaceholderReader()), "cache name", "could not be resolved");
  }

  static class PlaceholderReader {

    @ZetaCached(cache = "${users.cache}", key = "#p0", hardTtl = "${users.hardTtl}", softTtl = "${users.softTtl}")
    public String load(String id) {
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("${...} placeholders resolve in cache and both TTL attributes at plan resolution")
  void placeholdersResolveInCacheAndTtls() {
    DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
    beanFactory.addEmbeddedValueResolver(value ->
      value.replace("${users.cache}", "users").replace("${users.hardTtl}", "60000").replace("${users.softTtl}", "5000")
    );
    resolver = new PlanResolver(beanFactory);
    interceptor = new ZetaCacheInterceptor(zeta, resolver);
    missRunsBody();

    proxy(new PlaceholderReader()).load("42");

    assertThat(lastKey).isEqualTo(key("users", "42"));
    assertThat(lastOptions.hardTtlMs().getAsLong()).isEqualTo(60_000L);
    assertThat(lastOptions.softTtlMs().getAsLong()).isEqualTo(5_000L);
  }

  static class ResultKeyReader {

    @ZetaCached(cache = "users", key = "#result.id")
    public Saved load(String id) {
      return new Saved(id, "ACTIVE", 1L);
    }
  }

  @Test
  @DisplayName("a read key cannot use #result")
  void readKeyCannotUseResult() {
    assertRejectedAtStartup(() -> proxy(new ResultKeyReader()), "#result", "does not bind a method result");
  }

  static class ResultTtlReader {

    @ZetaCached(cache = "users", key = "#p0", hardTtl = "#result.ttl")
    public String load(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("a read TTL cannot use #result")
  void readTtlCannotUseResult() {
    assertRejectedAtStartup(() -> proxy(new ResultTtlReader()), "hardTtl", "#result");
  }

  static class ResultSoftTtlReader {

    @ZetaCached(cache = "users", key = "#p0", softTtl = "#result.ttl")
    public String load(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("a read soft TTL cannot use #result")
  void readSoftTtlCannotUseResult() {
    assertRejectedAtStartup(() -> proxy(new ResultSoftTtlReader()), "softTtl", "#result");
  }

  static class ResultBeforeInvalidator {

    @ZetaInvalidate(cache = "users", key = "#result.id", beforeInvocation = true)
    public Saved rename(String id) {
      return new Saved(id, "ACTIVE", 1L);
    }
  }

  @Test
  @DisplayName("a beforeInvocation invalidation key cannot use #result")
  void beforeInvocationKeyCannotUseResult() {
    assertRejectedAtStartup(() -> proxy(new ResultBeforeInvalidator()), "#result", "does not bind a method result");
  }

  static class VoidResultInvalidator {

    @ZetaInvalidate(cache = "users", key = "#result")
    public void rename(String id) {
      // no result to bind
    }
  }

  @Test
  @DisplayName("a void invalidation cannot use #result, even after the body")
  void voidInvalidationKeyCannotUseResult() {
    assertRejectedAtStartup(() -> proxy(new VoidResultInvalidator()), "#result", "does not bind a method result");
  }

  static class LiteralResultKeyReader {

    @ZetaCached(cache = "users", key = "'#result'")
    public String load(String id) {
      return id;
    }
  }

  static class DoubleQuotedLiteralResultKeyReader {

    @ZetaCached(cache = "users", key = "\"#result\"")
    public String load(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("#result inside a string literal is not a variable reference")
  void resultInsideAStringLiteralIsAllowed() {
    missRunsBody();
    proxy(new LiteralResultKeyReader()).load("42");

    assertThat(lastKey).isEqualTo(key("users", "#result"));
  }

  @Test
  @DisplayName("#result inside a double-quoted string literal is not a variable reference")
  void resultInsideADoubleQuotedStringLiteralIsAllowed() {
    missRunsBody();
    proxy(new DoubleQuotedLiteralResultKeyReader()).load("42");

    assertThat(lastKey).isEqualTo(key("users", "#result"));
  }

  // ---------------------------------------------------------------------
  // @ZetaPut: the write
  // ---------------------------------------------------------------------

  static class UserWriter {

    final AtomicInteger calls = new AtomicInteger();

    @ZetaPut(cache = "users", key = "#p0")
    public String save(String id) {
      calls.incrementAndGet();
      return "saved-" + id;
    }
  }

  @Test
  @DisplayName("a write runs the body and writes its non-null result through")
  void writeThroughNonNullResult() {
    UserWriter target = new UserWriter();
    ArgumentCaptor<PutPolicy> options = ArgumentCaptor.forClass(PutPolicy.class);

    assertThat(proxy(target).save("42")).isEqualTo("saved-42");
    assertThat(target.calls).hasValue(1);
    verify(zeta).put(eq(key("users", "42")), eq("saved-42"), options.capture());
    assertThat(options.getValue().sync()).isTrue();
    assertThat(options.getValue().hardTtlMs().getAsLong()).isZero();
  }

  static class ConditionOnlyWriter {

    final AtomicInteger calls = new AtomicInteger();

    @ZetaPut(cache = "users", condition = "#p0 != null")
    public String save(String id) {
      calls.incrementAndGet();
      return "saved-" + id;
    }
  }

  @Test
  @DisplayName("a write condition that reads a parameter builds the pre-body context")
  void writeConditionReadsParameter() {
    ConditionOnlyWriter target = new ConditionOnlyWriter();

    assertThat(proxy(target).save("42")).isEqualTo("saved-42");
    assertThat(target.calls).hasValue(1);
    verify(zeta).put(eq(key("users", "42")), eq("saved-42"), any(PutPolicy.class));
  }

  static class TtlWriter {

    @ZetaPut(cache = "users", key = "#p0", hardTtl = "60000", softTtl = "5000", sync = false)
    public String save(String id) {
      return "v";
    }
  }

  @Test
  @DisplayName("write TTL overrides and sync=false reach the facade")
  void writeOverridesReachFacade() {
    ArgumentCaptor<PutPolicy> options = ArgumentCaptor.forClass(PutPolicy.class);
    proxy(new TtlWriter()).save("42");

    verify(zeta).put(eq(key("users", "42")), eq("v"), options.capture());
    assertThat(options.getValue().sync()).isFalse();
    assertThat(options.getValue().hardTtlMs().getAsLong()).isEqualTo(60_000L);
    assertThat(options.getValue().softTtlMs().getAsLong()).isEqualTo(5_000L);
  }

  @Test
  @DisplayName("a write with two constant TTLs reuses one prebuilt PutPolicy across calls")
  void constantWriteTtlsReuseOnePrebuiltOptions() {
    ArgumentCaptor<PutPolicy> options = ArgumentCaptor.forClass(PutPolicy.class);
    TtlWriter service = proxy(new TtlWriter());

    service.save("1");
    service.save("2");

    verify(zeta, times(2)).put(anyString(), any(), options.capture());
    assertThat(options.getAllValues().get(0)).isSameAs(options.getAllValues().get(1));
    assertThat(options.getValue().hardTtlMs().getAsLong()).isEqualTo(60_000L);
    assertThat(options.getValue().softTtlMs().getAsLong()).isEqualTo(5_000L);
  }

  static class DynamicTtlWriter {

    final List<String> events = new ArrayList<>();

    @ZetaPut(cache = "users", key = "#p0", hardTtl = "#root.target.nextTtl(#p0)", sync = false)
    public String save(String id) {
      events.add("body");
      return "v";
    }

    public long nextTtl(String id) {
      events.add("ttl");
      return "vip".equals(id) ? 1000L : 5L;
    }
  }

  @Test
  @DisplayName("a write TTL expression is evaluated after the body, when the write happens")
  void writeTtlExpressionIsEvaluatedAtWriteTime() {
    ArgumentCaptor<PutPolicy> options = ArgumentCaptor.forClass(PutPolicy.class);
    DynamicTtlWriter target = new DynamicTtlWriter();

    assertThat(proxy(target).save("vip")).isEqualTo("v");

    verify(zeta).put(eq(key("users", "vip")), eq("v"), options.capture());
    assertThat(target.events).containsExactly("body", "ttl");
    assertThat(options.getValue().sync()).isFalse();
    assertThat(options.getValue().hardTtlMs().getAsLong()).isEqualTo(1000L);
  }

  static class PermanentSyncedWriter {

    final AtomicInteger calls = new AtomicInteger();

    @ZetaPut(cache = "users", key = "#p0", hardTtl = "9223372036854775807")
    public String save(String id) {
      calls.incrementAndGet();
      return "v";
    }
  }

  @Test
  @DisplayName("@ZetaPut rejects a permanent synced TTL at resolution, before the body runs")
  void permanentSyncedTtlIsRejectedAtResolution() {
    PermanentSyncedWriter target = new PermanentSyncedWriter();

    assertRejectedAtStartup(() -> proxy(target), "permanent");
    assertThat(target.calls).hasValue(0);
    verify(zeta, never()).put(anyString(), any(), any(PutPolicy.class));
  }

  static class PermanentSyncedSuffixedWriter {

    @ZetaPut(cache = "users", key = "#p0", hardTtl = "9223372036854775807L")
    public String save(String id) {
      return "v";
    }
  }

  @Test
  @DisplayName("a suffixed permanent synced TTL is rejected at resolution")
  void suffixedPermanentSyncedTtlIsRejectedAtResolution() {
    assertRejectedAtStartup(() -> proxy(new PermanentSyncedSuffixedWriter()), "permanent");
  }

  static class NullWriter {

    final AtomicInteger ttlEvaluations = new AtomicInteger();

    @ZetaPut(cache = "users", key = "#p0", hardTtl = "#root.target.nextTtl()", sync = false)
    public String save(String id) {
      return null;
    }

    public long nextTtl() {
      ttlEvaluations.incrementAndGet();
      return 1000L;
    }
  }

  @Test
  @DisplayName("a null write result writes nothing and evaluates no TTL")
  void nullWriteWritesNothing() {
    NullWriter target = new NullWriter();

    assertThat(proxy(target).save("42")).isNull();
    assertThat(target.ttlEvaluations).hasValue(0);
    verify(zeta, never()).put(anyString(), any(), any(PutPolicy.class));
  }

  static class VetoedTtlWriter {

    final AtomicInteger ttlEvaluations = new AtomicInteger();

    @ZetaPut(cache = "users", key = "#p0", hardTtl = "#root.target.nextTtl()", unless = "true", sync = false)
    public String save(String id) {
      return "v-" + id;
    }

    public long nextTtl() {
      ttlEvaluations.incrementAndGet();
      return 1000L;
    }
  }

  @Test
  @DisplayName("a vetoed write evaluates no TTL")
  void vetoedWriteEvaluatesNoTtl() {
    VetoedTtlWriter target = new VetoedTtlWriter();

    assertThat(proxy(target).save("42")).isEqualTo("v-42");
    assertThat(target.ttlEvaluations).hasValue(0);
    verify(zeta, never()).put(anyString(), any(), any(PutPolicy.class));
  }

  static class ThrowingWriter {

    @ZetaPut(cache = "users", key = "#p0")
    public String save(String id) {
      throw new IllegalStateException("boom");
    }
  }

  @Test
  @DisplayName("a write body exception writes nothing and propagates")
  void writeBodyFailurePropagates() {
    ThrowingWriter service = proxy(new ThrowingWriter());

    assertThatThrownBy(() -> service.save("42"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("boom");
    verify(zeta, never()).put(anyString(), any(), any(PutPolicy.class));
  }

  static class PreflightWriter {

    final AtomicInteger calls = new AtomicInteger();

    @ZetaPut(cache = "users", key = "#p0")
    public String save(String id) {
      calls.incrementAndGet();
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("a result-independent write key fails before the body runs")
  void resultIndependentWriteKeyFailsBeforeTheBody() {
    PreflightWriter target = new PreflightWriter();

    assertThatThrownBy(() -> proxy(target).save(null))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("resolved to null");
    assertThat(target.calls).hasValue(0);
    verify(zeta, never()).put(anyString(), any(), any(PutPolicy.class));
  }

  static class DynamicPermanentWriter {

    final AtomicInteger calls = new AtomicInteger();
    final AtomicInteger ttlEvaluations = new AtomicInteger();

    @ZetaPut(cache = "users", key = "#p0", hardTtl = "#root.target.nextTtl(#p0)")
    public String save(String id) {
      calls.incrementAndGet();
      return "v-" + id;
    }

    public long nextTtl(String id) {
      ttlEvaluations.incrementAndGet();
      return "permanent".equals(id) ? Long.MAX_VALUE : 5L;
    }
  }

  @Test
  @DisplayName("a result-independent dynamic permanent TTL fails before the body runs, and its value is reused")
  void resultIndependentDynamicPermanentTtlFailsBeforeTheBody() {
    DynamicPermanentWriter target = new DynamicPermanentWriter();
    DynamicPermanentWriter service = proxy(target);

    assertThatThrownBy(() -> service.save("permanent"))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("permanent entry cannot be synced");
    assertThat(target.calls).hasValue(0);
    verify(zeta, never()).put(anyString(), any(), any(PutPolicy.class));

    assertThat(service.save("ordinary")).isEqualTo("v-ordinary");
    assertThat(target.calls).hasValue(1);
    assertThat(target.ttlEvaluations).as("each call evaluates its hard TTL exactly once").hasValue(2);
  }

  record PermanentResult(long ttl) {}

  static class ResultPermanentWriter {

    @ZetaPut(cache = "users", key = "#p0", hardTtl = "#result.ttl")
    public PermanentResult save(String id) {
      return new PermanentResult(Long.MAX_VALUE);
    }
  }

  @Test
  @DisplayName("a result-dependent permanent synced TTL propagates after the body instead of being logged away")
  void resultDependentPermanentTtlPropagates() {
    ResultPermanentWriter service = proxy(new ResultPermanentWriter());

    assertThatThrownBy(() -> service.save("42"))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("permanent entry cannot be synced");
    verify(zeta, never()).put(anyString(), any(), any(PutPolicy.class));
  }

  static class SyncedWriter {

    @ZetaPut(cache = "users", key = "#p0", sync = true)
    public String save(String id) {
      return "v-" + id;
    }
  }

  /**
   * A configuration rejection from the facade is not a cache-engine failure: it must reach the
   * caller instead of being logged away as a failed write.
   */
  @Test
  @DisplayName("a synced write's configuration rejection propagates to the caller")
  void syncedWriteConfigurationFailurePropagates() {
    doThrow(new IllegalArgumentException("A permanent entry cannot be synced"))
      .when(zeta)
      .put(anyString(), any(), any(PutPolicy.class));

    assertThatThrownBy(() -> proxy(new SyncedWriter()).save("u1"))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("permanent entry cannot be synced");
  }

  /** A Worker-only deployment is a mode error, not a cache-engine failure. */
  @Test
  @DisplayName("a Worker-only mode rejection propagates to the caller")
  void workerOnlyModeFailurePropagates() {
    doThrow(new io.github.hyshmily.zeta.exception.ZetaModeException("put", "Worker-only mode", "App-mode cache"))
      .when(zeta)
      .put(anyString(), any(), any(PutPolicy.class));

    assertThatThrownBy(() -> proxy(new SyncedWriter()).save("u1")).isInstanceOf(
      io.github.hyshmily.zeta.exception.ZetaModeException.class
    );
  }

  static class MutableEntity {

    String id;

    MutableEntity(String id) {
      this.id = id;
    }

    public String getId() {
      return id;
    }
  }

  static class MutatingWriter {

    @ZetaPut(cache = "users", key = "#p0.id")
    public MutableEntity save(MutableEntity entity) {
      entity.id = "mutated";
      return entity;
    }
  }

  @Test
  @DisplayName("a result-independent key sees the argument state of the call, not the body's mutation")
  void resultIndependentKeyIsResolvedBeforeTheBodyMutatesArguments() {
    MutableEntity entity = new MutableEntity("original");

    proxy(new MutatingWriter()).save(entity);

    verify(zeta).put(eq(key("users", "original")), eq(entity), any(PutPolicy.class));
  }

  static class HugeTtlReader {

    @ZetaCached(cache = "users", key = "#p0", hardTtl = "99999999999999999999")
    public String load(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("a digits-only TTL literal outside the long range fails resolution with a clear message")
  void hugeTtlLiteralFailsResolution() {
    assertRejectedAtStartup(() -> proxy(new HugeTtlReader()), "hardTtl", "exceeds the long range");
  }

  static class UnencodableTtlReader {

    @ZetaCached(cache = "users", key = "#p0", hardTtl = "3000000000000")
    public String load(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("a constant TTL above the packed entry range fails resolution, not the first miss")
  void unencodableConstantTtlFailsResolution() {
    assertRejectedAtStartup(() -> proxy(new UnencodableTtlReader()), "hardTtl", "out of range");
  }

  static class NumberTtlReader {

    @ZetaCached(cache = "users", key = "#p0", hardTtl = "#p1")
    public String load(String id, Number ttl) {
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("a dynamic TTL outside the long range is rejected, not wrapped")
  void hugeNumericTtlIsRejected() {
    NumberTtlReader service = proxy(new NumberTtlReader());

    missRunsBody();
    service.load("big", new BigInteger("9223372036854775808"));
    assertThatThrownBy(() -> lastOptions.hardTtlMs().getAsLong()).isInstanceOf(IllegalStateException.class);

    missRunsBody();
    service.load("decimal", new BigDecimal("9223372036854775808"));
    assertThatThrownBy(() -> lastOptions.hardTtlMs().getAsLong()).isInstanceOf(IllegalStateException.class);
  }

  @Test
  @DisplayName("a floating-point dynamic TTL is rejected")
  void floatingDynamicTtlIsRejected() {
    NumberTtlReader service = proxy(new NumberTtlReader());

    missRunsBody();
    service.load("double", 1500.5d);
    assertThatThrownBy(() -> lastOptions.hardTtlMs().getAsLong()).isInstanceOf(IllegalStateException.class);

    missRunsBody();
    service.load("float", 1500.5f);
    assertThatThrownBy(() -> lastOptions.hardTtlMs().getAsLong()).isInstanceOf(IllegalStateException.class);
  }

  @Test
  @DisplayName("a dynamic TTL outside the encodable range is rejected")
  void dynamicUnencodableTtlIsRejected() {
    NumberTtlReader service = proxy(new NumberTtlReader());

    missRunsBody();
    service.load("huge", 3_000_000_000_000L);

    assertThatThrownBy(() -> lastOptions.hardTtlMs().getAsLong()).isInstanceOf(RuntimeException.class);
  }

  /** A generated-id entity: the id only exists after the body ran. */
  public static class Saved {

    private final String id;
    private final String status;
    private final long ttl;

    Saved(String id, String status, long ttl) {
      this.id = id;
      this.status = status;
      this.ttl = ttl;
    }

    public String getId() {
      return id;
    }

    public String getStatus() {
      return status;
    }

    public long getTtl() {
      return ttl;
    }
  }

  static class ResultKeyWriter {

    @ZetaPut(cache = "users", key = "#result.id", hardTtl = "#result.ttl")
    public Saved save(String name) {
      return new Saved("id-" + name, "ACTIVE", 60_000L);
    }
  }

  @Test
  @DisplayName("#result is available to a @ZetaPut key and TTL because the body already returned")
  void writeResolvesResultInKeyAndTtl() {
    ArgumentCaptor<PutPolicy> options = ArgumentCaptor.forClass(PutPolicy.class);
    Saved saved = proxy(new ResultKeyWriter()).save("u1");

    verify(zeta).put(eq(key("users", "id-u1")), eq(saved), options.capture());
    assertThat(options.getValue().hardTtlMs().getAsLong()).isEqualTo(60_000L);
  }

  static class NullResultKeyWriter {

    @ZetaPut(cache = "users", key = "#result?.id")
    public Saved save(String name) {
      return new Saved(null, "ACTIVE", 1L);
    }
  }

  @Test
  @DisplayName("a result-dependent @ZetaPut key that resolves to null skips the write after the body")
  void nullResultWriteKeySkipsTheWrite() {
    Saved saved = proxy(new NullResultKeyWriter()).save("u1");

    assertThat(saved.getId()).isNull();
    verify(zeta, never()).put(anyString(), any(), any(PutPolicy.class));
  }

  static class WholeResultWriter {

    @ZetaPut(cache = "users", key = "#result")
    public String save(String id) {
      return "saved-" + id;
    }
  }

  @Test
  @DisplayName("a whole-result key reads the bound result directly")
  void wholeResultKeyUsesTheResult() {
    proxy(new WholeResultWriter()).save("42");

    verify(zeta).put(eq(key("users", "saved-42")), eq("saved-42"), any(PutPolicy.class));
  }

  static class ConditionalWriter {

    @ZetaPut(cache = "users", key = "#result.id", unless = "#result.status != 'ACTIVE'")
    public Saved save(String status) {
      return new Saved("id-" + status, status, 1L);
    }
  }

  @Test
  @DisplayName("@ZetaPut.unless vetoes the write but still returns the value")
  void writeUnlessVetoesTheWrite() {
    ConditionalWriter service = proxy(new ConditionalWriter());

    assertThat(service.save("ACTIVE").getId()).isEqualTo("id-ACTIVE");
    verify(zeta, times(1)).put(eq(key("users", "id-ACTIVE")), any(), any(PutPolicy.class));

    assertThat(service.save("ARCHIVED").getStatus()).isEqualTo("ARCHIVED");
    verify(zeta, times(1)).put(anyString(), any(), any(PutPolicy.class));
  }

  static class FailingUnlessWriter {

    @ZetaPut(cache = "users", key = "#p0", unless = "#p0.charAt(99) == 'x'")
    public Saved save(String id) {
      return new Saved(id, "ACTIVE", 1L);
    }
  }

  static class FailingWriteUnlessService {

    @ZetaPut(cache = "users", key = "#p0", unless = "#result.id.charAt(99) == 'x'")
    public Saved save(String id) {
      return new Saved(id, "ACTIVE", 1L);
    }
  }

  @Test
  @DisplayName("a runtime write-unless failure is thrown to the caller")
  void runtimeWriteUnlessFailureIsThrown() {
    FailingWriteUnlessService service = proxy(new FailingWriteUnlessService());

    assertThatThrownBy(() -> service.save("v")).isInstanceOf(RuntimeException.class);
  }

  static class BrokenWriteUnless {

    @ZetaPut(cache = "users", key = "#p0", unless = "#result +")
    public Saved save(String id) {
      return new Saved(id, "ACTIVE", 1L);
    }
  }

  @Test
  @DisplayName("a broken @ZetaPut.unless fails at proxy creation")
  void brokenWriteUnlessFailsAtStartup() {
    assertRejectedAtStartup(() -> proxy(new BrokenWriteUnless()), "cannot be parsed");
  }

  // ---------------------------------------------------------------------
  // @ZetaInvalidate: the removal
  // ---------------------------------------------------------------------

  static class InvalidatingService {

    final List<String> events = new ArrayList<>();

    @ZetaInvalidate(cache = "users", key = "#p0")
    public String rename(String id) {
      events.add("body");
      return "renamed-" + id;
    }
  }

  @Test
  @DisplayName("an invalidation runs after the body by default")
  void invalidationRunsAfterBody() {
    InvalidatingService target = new InvalidatingService();
    ArgumentCaptor<InvalidatePolicy> options = ArgumentCaptor.forClass(InvalidatePolicy.class);
    doAnswer(invocation -> target.events.add("invalidate"))
      .when(zeta)
      .invalidate(anyString(), any(InvalidatePolicy.class));

    assertThat(proxy(target).rename("42")).isEqualTo("renamed-42");

    assertThat(target.events).containsExactly("body", "invalidate");
    verify(zeta).invalidate(eq(key("users", "42")), options.capture());
    assertThat(options.getValue().scope()).isEqualTo(InvalidatePolicy.Scope.BOTH);
    assertThat(options.getValue().immediate()).isFalse();
  }

  static class BeforeInvocationService {

    final List<String> events = new ArrayList<>();

    @ZetaInvalidate(cache = "users", key = "#p0", beforeInvocation = true, scope = InvalidatePolicy.Scope.LOCAL)
    public String rename(String id) {
      events.add("body");
      return "renamed-" + id;
    }
  }

  @Test
  @DisplayName("beforeInvocation invalidates before the body, immediately, in the declared scope")
  void beforeInvocationRunsFirst() {
    BeforeInvocationService target = new BeforeInvocationService();
    ArgumentCaptor<InvalidatePolicy> options = ArgumentCaptor.forClass(InvalidatePolicy.class);
    doAnswer(invocation -> target.events.add("invalidate"))
      .when(zeta)
      .invalidate(anyString(), any(InvalidatePolicy.class));

    proxy(target).rename("42");

    assertThat(target.events).containsExactly("invalidate", "body");
    verify(zeta).invalidate(eq(key("users", "42")), options.capture());
    assertThat(options.getValue().scope()).isEqualTo(InvalidatePolicy.Scope.LOCAL);
    assertThat(options.getValue().immediate()).isTrue();
  }

  static class ResultInvalidator {

    @ZetaInvalidate(cache = "users", key = "#result.id")
    public Saved rename(String name) {
      return new Saved("id-" + name, "ACTIVE", 1L);
    }
  }

  @Test
  @DisplayName("an after-the-body invalidation resolves #result in its key")
  void invalidationResolvesResultInKey() {
    proxy(new ResultInvalidator()).rename("u1");

    verify(zeta).invalidate(eq(key("users", "id-u1")), any(InvalidatePolicy.class));
  }

  @Test
  @DisplayName("one plan reuses the same prebuilt InvalidatePolicy instance across calls")
  void invalidationOptionsArePrebuiltOnce() {
    InvalidatingService service = proxy(new InvalidatingService());
    ArgumentCaptor<InvalidatePolicy> options = ArgumentCaptor.forClass(InvalidatePolicy.class);

    service.rename("1");
    service.rename("2");

    verify(zeta, times(2)).invalidate(anyString(), options.capture());
    assertThat(options.getAllValues().get(0)).isSameAs(options.getAllValues().get(1));
  }

  static class NullResultInvalidator {

    @ZetaInvalidate(cache = "users", key = "#result?.id")
    public Saved rename(String name) {
      return null;
    }
  }

  @Test
  @DisplayName("a null-safe key on a null body result is a no-op removal instead of a failure")
  void nullResultSkipsNullSafeResultKey() {
    assertThat(proxy(new NullResultInvalidator()).rename("u1")).isNull();

    verify(zeta, never()).invalidate(anyString(), any(InvalidatePolicy.class));
  }

  static class DirectNullResultInvalidator {

    @ZetaInvalidate(cache = "users", key = "#result.id")
    public Saved rename(String name) {
      return null;
    }
  }

  @Test
  @DisplayName("a direct #result dereference on a null result fails; #result?.id is the null-safe form")
  void directNullResultDereferenceFails() {
    DirectNullResultInvalidator service = proxy(new DirectNullResultInvalidator());

    assertThatThrownBy(() -> service.rename("u1")).isInstanceOf(SpelEvaluationException.class);
    verify(zeta, never()).invalidate(anyString(), any(InvalidatePolicy.class));
  }

  static class FallbackInvalidator {

    @ZetaInvalidate(cache = "users", key = "#result != null ? #result.id : #p0")
    public Saved rename(String id) {
      return null;
    }
  }

  @Test
  @DisplayName("a null result still runs a result-dependent key that has its own null fallback")
  void nullResultRunsFallbackExpression() {
    assertThat(proxy(new FallbackInvalidator()).rename("u1")).isNull();

    verify(zeta).invalidate(eq(key("users", "u1")), any(InvalidatePolicy.class));
  }

  static class NullFallbackInvalidator {

    @ZetaInvalidate(cache = "users", key = "#result != null ? #result.id : #p0.id")
    public Saved rename(Saved fallback) {
      return null;
    }
  }

  @Test
  @DisplayName("a null result does not hide a failure from another null variable")
  void nullResultDoesNotHideAnotherVariableFailure() {
    NullFallbackInvalidator service = proxy(new NullFallbackInvalidator());

    assertThatThrownBy(() -> service.rename(null)).isInstanceOf(SpelEvaluationException.class);
    verify(zeta, never()).invalidate(anyString(), any(InvalidatePolicy.class));
  }

  static class NullKeyResultInvalidator {

    @ZetaInvalidate(cache = "users", key = "#result.id")
    public Saved rename(String name) {
      return new Saved(null, "ACTIVE", 1L);
    }
  }

  @Test
  @DisplayName("a result-dependent key that resolves to null is a no-op removal")
  void nullResultKeySkipsInvalidation() {
    assertThat(proxy(new NullKeyResultInvalidator()).rename("u1").getId()).isNull();

    verify(zeta, never()).invalidate(anyString(), any(InvalidatePolicy.class));
  }

  static class NullKeyInvalidator {

    @ZetaInvalidate(cache = "users", key = "#p0")
    public String rename(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("a null key from a non-result expression stays a configuration error")
  void nullNonResultKeyIsAnError() {
    NullKeyInvalidator service = proxy(new NullKeyInvalidator());

    assertThatThrownBy(() -> service.rename(null))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("resolved to null");
    verify(zeta, never()).invalidate(anyString(), any(InvalidatePolicy.class));
  }

  static class ThrowingInvalidator {

    @ZetaInvalidate(cache = "users", key = "#p0")
    public String rename(String id) {
      throw new IllegalStateException("boom");
    }
  }

  @Test
  @DisplayName("a throwing invalidation body leaves the cache untouched")
  void throwingInvalidationBodyLeavesCacheUntouched() {
    ThrowingInvalidator service = proxy(new ThrowingInvalidator());

    assertThatThrownBy(() -> service.rename("42")).isInstanceOf(IllegalStateException.class);
    verify(zeta, never()).invalidate(anyString(), any(InvalidatePolicy.class));
  }

  // ---------------------------------------------------------------------
  // PLAIN keys
  // ---------------------------------------------------------------------

  @ZetaConfig(cache = "plainUsers", keyFormat = KeyFormat.Option.PLAIN)
  static class PlainReader {

    final AtomicInteger calls = new AtomicInteger();

    @ZetaCached(key = "#p0")
    public String load(Long id) {
      calls.incrementAndGet();
      return "real-" + id;
    }
  }

  @Test
  @DisplayName("a PLAIN namespace encodes namespace:value")
  void plainKeyEncoding() {
    resolver = new PlanResolver(null, () -> null, () -> new NamespaceKeys(KeyFormat.TYPED));
    interceptor = new ZetaCacheInterceptor(zeta, resolver);
    PlainReader target = new PlainReader();
    missRunsBody();

    assertThat(proxy(target).load(42L)).isEqualTo("real-42");
    assertThat(target.calls).hasValue(1);
    assertThat(lastKey).isEqualTo("plainUsers:42");
  }

  @ZetaConfig(cache = "plainInferred", keyFormat = KeyFormat.Option.PLAIN)
  static class PlainInferredReader {

    @ZetaCached(key = "#p0")
    public String load(Long id) {
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("PLAIN infers the key type from a single-argument key")
  void plainInfersType() {
    resolver = new PlanResolver(null, () -> null, () -> new NamespaceKeys(KeyFormat.TYPED));
    interceptor = new ZetaCacheInterceptor(zeta, resolver);
    missRunsBody();

    proxy(new PlainInferredReader()).load(42L);

    assertThat(lastKey).isEqualTo("plainInferred:42");
  }

  @ZetaConfig(cache = "plainDefault", keyFormat = KeyFormat.Option.PLAIN)
  static class PlainDefaultReader {

    @ZetaCached
    public String load(Long id) {
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("a one-argument method with no key attribute uses the single-argument shape in PLAIN")
  void plainDefaultKeyUsesSingleArgumentShape() {
    resolver = new PlanResolver(null, () -> null, () -> new NamespaceKeys(KeyFormat.TYPED));
    interceptor = new ZetaCacheInterceptor(zeta, resolver);
    missRunsBody();

    proxy(new PlainDefaultReader()).load(42L);

    assertThat(lastKey).isEqualTo("plainDefault:42");
  }

  @ZetaConfig(cache = "plainRepeated", keyFormat = KeyFormat.Option.PLAIN)
  static class PlainRepeatedReader {

    @ZetaCached(cache = "plainRepeated", key = "#p0")
    public String load(Long id) {
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("a method that repeats the class cache name inherits its format and key type")
  void repeatedCacheNameInheritsTheClassFormat() {
    resolver = new PlanResolver(null, () -> null, () -> new NamespaceKeys(KeyFormat.TYPED));
    interceptor = new ZetaCacheInterceptor(zeta, resolver);
    missRunsBody();

    proxy(new PlainRepeatedReader()).load(42L);

    assertThat(lastKey).isEqualTo("plainRepeated:42");
  }

  @ZetaConfig(cache = "plainBatch", keyFormat = KeyFormat.Option.PLAIN)
  static class PlainBatchInvalidator {

    @ZetaInvalidate(key = "#ids", eachKey = true)
    public void delete(List<Long> ids) {}
  }

  @Test
  @DisplayName("eachKey in a PLAIN namespace infers the Iterable element type")
  void plainEachKeyUsesTheElementType() {
    resolver = new PlanResolver(null, () -> null, () -> new NamespaceKeys(KeyFormat.TYPED));
    interceptor = new ZetaCacheInterceptor(zeta, resolver);

    proxy(new PlainBatchInvalidator()).delete(List.of(1L, 2L));

    verify(zeta).invalidate(eq(List.of("plainBatch:1", "plainBatch:2")), any(InvalidatePolicy.class));
  }

  @ZetaConfig(cache = "plainMulti", keyFormat = KeyFormat.Option.PLAIN)
  static class PlainMultiReader {

    @ZetaCached
    public String load(Long first, Long second) {
      return first + ":" + second;
    }
  }

  @Test
  @DisplayName("PLAIN refuses a multi-argument default key at startup")
  void plainMultiArgFailsStartup() {
    resolver = new PlanResolver(null, () -> null, () -> new NamespaceKeys(KeyFormat.TYPED));
    interceptor = new ZetaCacheInterceptor(zeta, resolver);

    assertRejectedAtStartup(() -> proxy(new PlainMultiReader()), "multi-argument default key");
  }

  @ZetaConfig(cache = "plainUnknown", keyFormat = KeyFormat.Option.PLAIN)
  static class PlainUnknownReader {

    @ZetaCached(key = "#p0.id")
    public String load(Saved saved) {
      return saved.id;
    }
  }

  @Test
  @DisplayName("a PLAIN namespace with an untyped expression key is accepted")
  void plainUnknownTypeIsAccepted() {
    NamespaceKeys keys = new NamespaceKeys(KeyFormat.TYPED);
    resolver = new PlanResolver(null, () -> null, () -> keys);
    interceptor = new ZetaCacheInterceptor(zeta, resolver);
    proxy(new PlainUnknownReader());

    assertThat(keys.formats()).containsEntry("plainUnknown", KeyFormat.PLAIN);
  }
}
