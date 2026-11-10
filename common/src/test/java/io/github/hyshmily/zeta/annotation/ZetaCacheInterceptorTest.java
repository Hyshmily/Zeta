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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.policy.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
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
import org.springframework.aop.TargetSource;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.framework.AopConfigException;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.DefaultPointcutAdvisor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.expression.spel.SpelEvaluationException;

@DisplayName("ZetaCacheInterceptor")
class ZetaCacheInterceptorTest {

  private static final String NS = "\u0000";

  /** The physical key an explicit single-value expression produces: one default-key argument. */
  private static String key(String cache, Object value) {
    return (cache + NS + io.github.hyshmily.zeta.annotation.supporter.DefaultKey.encode(new Object[] { value }));
  }

  private Zeta zeta;
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

  @BeforeEach
  void setUp() {
    zeta = mock(Zeta.class);
    interceptor = new ZetaCacheInterceptor(zeta, null);
    when(zeta.get(anyString(), any(CacheLoader.class), any(GetPolicy.class))).thenAnswer(invocation -> {
      lastKey = invocation.getArgument(0);
      lastLoader = invocation.getArgument(1);
      lastOptions = invocation.getArgument(2);
      if (hit) {
        return hitValue;
      }
      Object loaded = lastLoader.load(lastKey);
      lastShouldCache = lastLoader.isCacheable(loaded);
      return loaded;
    });
  }

  @SuppressWarnings("unchecked")
  private <T> T proxy(T target, MethodInterceptor... otherAdvices) {
    ProxyFactory factory = new ProxyFactory(target);
    factory.addAdvisor(new ZetaCacheAdvisor(interceptor));
    for (MethodInterceptor other : otherAdvices) {
      factory.addAdvisor(new DefaultPointcutAdvisor(other));
    }
    return (T) factory.getProxy();
  }

  /** A JDK dynamic proxy: the invocation reports the interface method, not the implementation. */
  @SuppressWarnings("unchecked")
  private <T> T jdkProxy(T target, Class<?>... interfaces) {
    ProxyFactory factory = new ProxyFactory();
    factory.setTarget(target);
    factory.setInterfaces(interfaces);
    factory.addAdvisor(new ZetaCacheAdvisor(interceptor));
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

    String expected =
      "users" +
      NS +
      io.github.hyshmily.zeta.annotation.supporter.DefaultKey.encode(new Object[] { new String[] { "a", "x" } });
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
    interceptor = new ZetaCacheInterceptor(zeta, beanFactory);
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
  @DisplayName("a runtime unless failure fails open: the value is stored")
  void runtimeUnlessFailureFailsOpen() {
    FailingUnlessService service = proxy(new FailingUnlessService());
    missRunsBody();

    assertThat(service.load("v")).isEqualTo("v");
    assertThat(lastShouldCache).isTrue();
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
  @DisplayName("a runtime TTL evaluation failure degrades to the configured default")
  void runtimeTtlFailureDegradesToDefault() {
    FailingTtlService service = proxy(new FailingTtlService());
    missRunsBody();

    service.load("42");

    assertThat(lastOptions.hardTtlMs().getAsLong()).isZero();
  }

  static class ThrowingReader {

    @ZetaCached(cache = "users", key = "#p0")
    public String load(String id) {
      throw new IllegalStateException("source down: " + id);
    }
  }

  @Test
  @DisplayName("a body exception reaches the caller unchanged and stores nothing")
  void bodyFailurePropagates() {
    ThrowingReader service = proxy(new ThrowingReader());
    missRunsBody();

    assertThatThrownBy(() -> service.load("42"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("source down: 42");
    assertThat(lastShouldCache).isFalse();
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
    assertThat(chain.get(refresh)).as("a refresh cannot re-pin the chain").isNull();
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

    assertThatThrownBy(() -> interceptor.matches(method, TwoInterfaceConflict.class))
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

    assertThatThrownBy(() -> interceptor.matches(method, SameVerbConflict.class))
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

    assertThat(interceptor.matches(method, SameVerbAgreement.class)).isTrue();
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

    assertThatThrownBy(() -> interceptor.matches(bridge, GenericReaderImpl.class))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("mutually exclusive");
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
    factory.addAdvisor(new ZetaCacheAdvisor(interceptor));
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

    assertThatThrownBy(() -> interceptor.matches(load, DiamondImpl.class))
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

    assertThatThrownBy(() -> interceptor.matches(load, AnnotatedUnrelatedDiamondImpl.class))
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
  @SuppressWarnings("unchecked")
  private static <T> T plainProxy(T target, Class<?> iface) {
    ProxyFactory factory = new ProxyFactory();
    factory.setTarget(target);
    factory.setInterfaces(iface);
    return (T) factory.getProxy();
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
  @DisplayName("a parameter named result is rejected because #result is reserved")
  void parameterNamedResultIsRejected() {
    assertRejectedAtStartup(() -> proxy(new ResultNamedParameterReader()), "reserved parameter 'result'");
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

  @Test
  @DisplayName("a parameter named p0 or a0 is rejected because positional bindings are reserved")
  void positionalParameterNamesAreRejected() {
    assertRejectedAtStartup(() -> proxy(new ShadowedPositionalNameReader()), "reserved parameter 'p0'");
    assertRejectedAtStartup(() -> proxy(new ShadowedPositionalNameSpelReader()), "reserved parameter 'p0'");
  }

  static class OutOfRangeKeyReader {

    @ZetaCached(cache = "users", key = "#p1")
    public String load(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("an out-of-range #pN fails at proxy creation")
  void outOfRangeArgumentReferenceFailsAtStartup() {
    assertRejectedAtStartup(() -> proxy(new OutOfRangeKeyReader()), "addresses parameter 1");
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

    @ZetaCached(cache = "users", key = "#p0", hardTtl = " -5 ", softTtl = " +5000 ")
    public String load(String id) {
      return "v";
    }
  }

  @Test
  @DisplayName("a signed TTL literal is folded: negative clamps to zero, a leading plus is accepted")
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

  @Test
  @DisplayName("a floating-point TTL literal fails startup: milliseconds are discrete")
  void floatingTtlLiteralFailsStartup() {
    assertRejectedAtStartup(() -> proxy(new HugeRealTtlReader()), "hardTtl", "must be an integral number");
  }

  static class ExactTwoPow63TtlReader {

    @ZetaCached(cache = "users", key = "#p0", hardTtl = "9223372036854775808e0")
    public String load(String id) {
      return "v";
    }
  }

  @Test
  @DisplayName("a floating-point literal is rejected before any narrowing can saturate")
  void exactTwoPow63TtlLiteralFailsStartup() {
    assertRejectedAtStartup(() -> proxy(new ExactTwoPow63TtlReader()), "hardTtl", "must be an integral number");
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
    interceptor = new ZetaCacheInterceptor(zeta, beanFactory);
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

  static class NumberTtlReader {

    @ZetaCached(cache = "users", key = "#p0", hardTtl = "#p1")
    public String load(String id, Number ttl) {
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("a huge BigInteger or BigDecimal TTL clamps to the permanent sentinel instead of wrapping negative")
  void hugeNumericTtlClampsInsteadOfWrapping() {
    NumberTtlReader service = proxy(new NumberTtlReader());

    missRunsBody();
    service.load("big", new BigInteger("9223372036854775808"));
    assertThat(lastOptions.hardTtlMs().getAsLong()).isEqualTo(Long.MAX_VALUE);

    missRunsBody();
    service.load("decimal", new BigDecimal("9223372036854775808.5"));
    assertThat(lastOptions.hardTtlMs().getAsLong()).isEqualTo(Long.MAX_VALUE);

    missRunsBody();
    service.load("negative", new BigInteger("-9223372036854775809"));
    assertThat(lastOptions.hardTtlMs().getAsLong()).isZero();
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

  @Test
  @DisplayName("a runtime @ZetaPut.unless failure fails open: the write happens")
  void runtimeWriteUnlessFailureFailsOpen() {
    proxy(new FailingUnlessWriter()).save("u1");

    verify(zeta).put(eq(key("users", "u1")), any(Saved.class), any(PutPolicy.class));
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
}
