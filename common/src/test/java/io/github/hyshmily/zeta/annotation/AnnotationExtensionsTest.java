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
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.annotation.support.DefaultKey;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.InvalidatePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.PutPolicy;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.core.annotation.AliasFor;

@DisplayName("annotation extensions")
class AnnotationExtensionsTest {

  private static final String NS = "\u0000";

  private Zeta zeta;
  private PlanResolver resolver;
  private ZetaCacheInterceptor interceptor;

  @BeforeEach
  void setUp() {
    zeta = mock(Zeta.class);
    resolver = new PlanResolver(null);
    interceptor = new ZetaCacheInterceptor(zeta, resolver);
    when(zeta.get(anyString(), any(CacheLoader.class), any(GetPolicy.class))).thenAnswer(invocation -> {
      CacheLoader<String, Object> loader = invocation.getArgument(1);
      return loader.load(invocation.getArgument(0));
    });
  }

  private static String key(String cache, Object value) {
    return cache + NS + DefaultKey.encode(new Object[] { value });
  }

  @SuppressWarnings("unchecked")
  private <T> T proxy(T target) {
    return proxy(target, interceptor);
  }

  @SuppressWarnings("unchecked")
  private <T> T proxy(T target, ZetaCacheInterceptor advice) {
    ProxyFactory factory = new ProxyFactory(target);
    ZetaCacheAdvisor advisor = new ZetaCacheAdvisor(resolver);
    advisor.setAdvice(advice);
    factory.addAdvisor(advisor);
    return (T) factory.getProxy();
  }

  // ---------------------------------------------------------------------
  // condition
  // ---------------------------------------------------------------------

  static class ConditionalRead {

    final AtomicInteger calls = new AtomicInteger();

    @ZetaCached(cache = "users", key = "#p0", condition = "#p0 != null")
    public String load(String id) {
      calls.incrementAndGet();
      return id == null ? null : "v-" + id;
    }
  }

  @Test
  @DisplayName("a false read condition bypasses the cache before the null key is resolved")
  void falseReadConditionBypassesBeforeKeyResolution() {
    ConditionalRead target = new ConditionalRead();
    ConditionalRead service = proxy(target);

    assertThat(service.load(null)).isNull();
    assertThat(target.calls).hasValue(1);
    verify(zeta, never()).get(anyString(), any(CacheLoader.class), any(GetPolicy.class));

    assertThat(service.load("42")).isEqualTo("v-42");
    verify(zeta).get(eq(key("users", "42")), any(CacheLoader.class), any(GetPolicy.class));
  }

  static class ConditionalWrite {

    @ZetaPut(cache = "users", key = "#p0", condition = "#p0 != null")
    public String save(String id) {
      return id == null ? null : "s-" + id;
    }
  }

  @Test
  @DisplayName("a false write condition runs the body without resolving the key or writing")
  void falseWriteConditionSkipsTheWrite() {
    ConditionalWrite service = proxy(new ConditionalWrite());

    assertThat(service.save(null)).isNull();
    verify(zeta, never()).put(anyString(), any(), any(PutPolicy.class));

    assertThat(service.save("42")).isEqualTo("s-42");
    verify(zeta).put(eq(key("users", "42")), eq("s-42"), any(PutPolicy.class));
  }

  static class BrokenConditionReader {

    final AtomicInteger calls = new AtomicInteger();

    @ZetaCached(cache = "users", key = "#p0", condition = "#p0.charAt(99) == 'x'")
    public String load(String id) {
      calls.incrementAndGet();
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("a throwing condition fails closed: the body runs and the cache is bypassed")
  void brokenConditionBypassesTheCache() {
    BrokenConditionReader target = new BrokenConditionReader();
    BrokenConditionReader service = proxy(target);

    assertThat(service.load("42")).isEqualTo("v-42");
    assertThat(target.calls).hasValue(1);
    verify(zeta, never()).get(anyString(), any(CacheLoader.class), any(GetPolicy.class));
  }

  static class ConstantConditionReader {

    @ZetaCached(cache = "users", key = "#p0", condition = "true")
    public String load(String id) {
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("a constant condition is evaluated without an invocation context")
  void constantConditionUsesTheCache() {
    assertThat(proxy(new ConstantConditionReader()).load("42")).isEqualTo("v-42");

    verify(zeta).get(eq(key("users", "42")), any(CacheLoader.class), any(GetPolicy.class));
  }

  static class StringConstantConditionReader {

    @ZetaCached(cache = "users", condition = "'true'")
    public String load(String id) {
      return id;
    }
  }

  static class NumberConstantUnlessReader {

    @ZetaCached(cache = "users", unless = "1")
    public String load(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("a constant condition or unless that is not Boolean fails startup")
  void nonBooleanConstantGuardsFailStartup() {
    Throwable condition = catchThrowable(() -> proxy(new StringConstantConditionReader()));
    assertThat(rootCause(condition))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("condition")
      .hasMessageContaining("must evaluate to a boolean");

    Throwable unless = catchThrowable(() -> proxy(new NumberConstantUnlessReader()));
    assertThat(rootCause(unless))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("unless")
      .hasMessageContaining("must evaluate to a boolean");
  }

  record Saved(String id, String status) {}

  static class ResultConditionInvalidator {

    @ZetaInvalidate(cache = "users", key = "#result.id", condition = "#result.status == 'ACTIVE'")
    public Saved rename() {
      return new Saved("u1", "ACTIVE");
    }
  }

  static class InactiveResultConditionInvalidator {

    @ZetaInvalidate(cache = "users", key = "#result.id", condition = "#result.status == 'ACTIVE'")
    public Saved rename() {
      return new Saved("u1", "DELETED");
    }
  }

  @Test
  @DisplayName("an after-the-body invalidation condition sees the unwrapped result")
  void afterBodyConditionSeesTheResult() {
    proxy(new ResultConditionInvalidator()).rename();
    verify(zeta).invalidate(eq(key("users", "u1")), any(InvalidatePolicy.class));

    proxy(new InactiveResultConditionInvalidator()).rename();
    verify(zeta, times(1)).invalidate(anyString(), any(InvalidatePolicy.class));
  }

  static class PreBodyResultConditionInvalidator {

    @ZetaInvalidate(cache = "users", key = "#p0", condition = "#result != null", beforeInvocation = true)
    public String rename(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("a pre-invocation condition cannot read #result")
  void preBodyConditionRejectsResult() {
    Throwable thrown = catchThrowable(() -> proxy(new PreBodyResultConditionInvalidator()));

    assertThat(thrown).isNotNull();
    assertThat(rootCause(thrown)).isInstanceOf(IllegalStateException.class).hasMessageContaining("cannot use #result");
  }

  private static Throwable rootCause(Throwable error) {
    Throwable cause = error;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    return cause;
  }

  // ---------------------------------------------------------------------
  // Optional
  // ---------------------------------------------------------------------

  static class OptionalReader {

    final AtomicInteger calls = new AtomicInteger();

    @ZetaCached(cache = "users", key = "#p0")
    public Optional<String> load(String id) {
      calls.incrementAndGet();
      return Optional.of("v-" + id);
    }
  }

  @Test
  @DisplayName("an Optional read stores the domain value and rewraps it for the caller")
  void optionalReadStoresDomainValue() {
    OptionalReader target = new OptionalReader();
    OptionalReader service = proxy(target);
    AtomicReference<Object> stored = new AtomicReference<>();
    AtomicReference<Boolean> cacheable = new AtomicReference<>();
    when(zeta.get(anyString(), any(CacheLoader.class), any(GetPolicy.class))).thenAnswer(invocation -> {
      CacheLoader<String, Object> loader = invocation.getArgument(1);
      Object value = loader.load(invocation.getArgument(0));
      stored.set(value);
      cacheable.set(loader.isCacheable(value));
      return value;
    });

    assertThat(service.load("42")).contains("v-42");
    assertThat(target.calls).hasValue(1);
    assertThat(stored.get()).isEqualTo("v-42");
    assertThat(cacheable.get()).isTrue();
  }

  static class EmptyOptionalReader {

    final AtomicInteger calls = new AtomicInteger();

    @ZetaCached(cache = "users", key = "#p0")
    public Optional<String> load(String id) {
      calls.incrementAndGet();
      return Optional.empty();
    }
  }

  @Test
  @DisplayName("an empty Optional is a null result and is never stored")
  void emptyOptionalIsNotStored() {
    EmptyOptionalReader target = new EmptyOptionalReader();
    EmptyOptionalReader service = proxy(target);
    AtomicReference<Boolean> cacheable = new AtomicReference<>();
    when(zeta.get(anyString(), any(CacheLoader.class), any(GetPolicy.class))).thenAnswer(invocation -> {
      CacheLoader<String, Object> loader = invocation.getArgument(1);
      Object value = loader.load(invocation.getArgument(0));
      cacheable.set(loader.isCacheable(value));
      return value;
    });

    assertThat(service.load("42")).isEmpty();
    assertThat(service.load("42")).isEmpty();
    assertThat(target.calls).hasValue(2);
    assertThat(cacheable.get()).isFalse();
  }

  @Test
  @DisplayName("an Optional hit is rewrapped without a second Optional layer")
  void optionalHitIsRewrapped() {
    when(zeta.get(anyString(), any(CacheLoader.class), any(GetPolicy.class))).thenReturn("cached-42");

    assertThat(proxy(new OptionalReader()).load("42")).contains("cached-42");
  }

  static class OptionalWriter {

    @ZetaPut(cache = "users", key = "#p0")
    public Optional<String> save(String id) {
      return Optional.of("saved-" + id);
    }
  }

  static class EmptyOptionalWriter {

    @ZetaPut(cache = "users", key = "#p0")
    public Optional<String> save(String id) {
      return Optional.empty();
    }
  }

  @Test
  @DisplayName("an Optional write stores the domain value and returns the original wrapper")
  void optionalWriteStoresDomainValue() {
    assertThat(proxy(new OptionalWriter()).save("42")).contains("saved-42");
    verify(zeta).put(eq(key("users", "42")), eq("saved-42"), any(PutPolicy.class));

    assertThat(proxy(new EmptyOptionalWriter()).save("42")).isEmpty();
    verify(zeta, times(1)).put(anyString(), any(), any(PutPolicy.class));
  }

  // ---------------------------------------------------------------------
  // repeatable invalidations / eachKey / allEntries
  // ---------------------------------------------------------------------

  static class RepeatableInvalidator {

    @ZetaInvalidate(cache = "users", key = "#p0")
    @ZetaInvalidate(cache = "emails", key = "#p0")
    public String update(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("repeated invalidations remove every declared key")
  void repeatedInvalidationsRemoveEveryKey() {
    proxy(new RepeatableInvalidator()).update("42");

    verify(zeta).invalidate(eq(key("users", "42")), any(InvalidatePolicy.class));
    verify(zeta).invalidate(eq(key("emails", "42")), any(InvalidatePolicy.class));
  }

  static class MixedStateInvalidator {

    final AtomicInteger body = new AtomicInteger();

    @ZetaInvalidate(cache = "before", key = "#p0", beforeInvocation = true)
    @ZetaInvalidate(cache = "pre", key = "#p0")
    @ZetaInvalidate(cache = "result", key = "#result.id", condition = "#result.status == 'ACTIVE'")
    @ZetaInvalidate(cache = "skipped", key = "#p0", condition = "#p0 == null")
    public Saved rename(String id) {
      body.incrementAndGet();
      return new Saved("u1", "ACTIVE");
    }
  }

  @Test
  @DisplayName("repeated invalidations keep each phase state independent")
  void repeatedInvalidationsKeepIndependentPhaseStates() {
    MixedStateInvalidator target = new MixedStateInvalidator();
    List<String> events = new ArrayList<>();
    doAnswer(invocation -> {
      String physical = invocation.getArgument(0);
      events.add(physical.substring(0, physical.indexOf(NS)));
      return null;
    })
      .when(zeta)
      .invalidate(anyString(), any(InvalidatePolicy.class));

    assertThat(proxy(target).rename("42").status()).isEqualTo("ACTIVE");
    assertThat(target.body).hasValue(1);
    assertThat(events)
      .as("the pre-invocation removal runs first, then the body, then the deferred removals")
      .containsExactly("before", "pre", "result");
    verify(zeta).invalidate(eq(key("before", "42")), any(InvalidatePolicy.class));
    verify(zeta).invalidate(eq(key("pre", "42")), any(InvalidatePolicy.class));
    verify(zeta).invalidate(eq(key("result", "u1")), any(InvalidatePolicy.class));
    verify(zeta, never()).invalidate(eq(key("skipped", "42")), any(InvalidatePolicy.class));
  }

  static class PartiallyResolvableInvalidator {

    final AtomicInteger body = new AtomicInteger();

    @ZetaInvalidate(cache = "early", key = "#p0", beforeInvocation = true)
    @ZetaInvalidate(cache = "late")
    public void rename(String id, String unused) {
      body.incrementAndGet();
    }
  }

  @Test
  @DisplayName("a failing deferred target stops every pre-invocation removal from running")
  void batchValidationPrecedesEveryImmediateRemoval() {
    PartiallyResolvableInvalidator target = new PartiallyResolvableInvalidator();
    PartiallyResolvableInvalidator service = proxy(target);

    assertThatThrownBy(() -> service.rename("42", null))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("null argument 1");
    assertThat(target.body).hasValue(0);
    verify(zeta, never()).invalidate(anyString(), any(InvalidatePolicy.class));
  }

  static class PhaseTrackingInvalidator {

    private boolean committed;

    @ZetaInvalidate(cache = "users", key = "#p0", condition = "#root.target.committed")
    public String rename(String id) {
      committed = true;
      return id;
    }

    public boolean isCommitted() {
      return committed;
    }
  }

  @Test
  @DisplayName("a present condition is evaluated after the body, even when it does not read #result")
  void presentConditionIsEvaluatedAfterTheBody() {
    proxy(new PhaseTrackingInvalidator()).rename("42");

    verify(zeta).invalidate(eq(key("users", "42")), any(InvalidatePolicy.class));
  }

  static class NullPostKeyInvalidator {

    @ZetaInvalidate(cache = "users", key = "#p0", condition = "#result != null")
    public Saved rename(String id) {
      return new Saved("u1", "ACTIVE");
    }
  }

  @Test
  @DisplayName("a key resolved after the body treats a null as a no-op, even without #result")
  void nullKeyAfterTheBodyIsANoOp() {
    assertThat(proxy(new NullPostKeyInvalidator()).rename(null).status()).isEqualTo("ACTIVE");

    verify(zeta, never()).invalidate(anyString(), any(InvalidatePolicy.class));
  }

  static class ParentInvalidator {

    @ZetaInvalidate(cache = "parent", key = "#p0")
    public String update(String id) {
      return id;
    }
  }

  static class ChildInvalidator extends ParentInvalidator {

    @Override
    @ZetaInvalidate(cache = "child", key = "#p0")
    public String update(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("a child invalidation set overrides the parent set instead of merging")
  void childInvalidationSetOverridesParent() {
    proxy(new ChildInvalidator()).update("42");

    verify(zeta).invalidate(eq(key("child", "42")), any(InvalidatePolicy.class));
    verify(zeta, never()).invalidate(eq(key("parent", "42")), any(InvalidatePolicy.class));
  }

  static class BatchInvalidator {

    final AtomicInteger calls = new AtomicInteger();

    @ZetaInvalidate(cache = "users", key = "#p0", eachKey = true)
    public void delete(long[] ids) {
      calls.incrementAndGet();
    }
  }

  @Test
  @DisplayName("eachKey expands a primitive array once per distinct element and skips an empty batch")
  void eachKeyExpandsPrimitiveArray() {
    BatchInvalidator target = new BatchInvalidator();
    BatchInvalidator service = proxy(target);

    service.delete(new long[] { 1L, 2L, 2L });
    verify(zeta).invalidate(eq(List.of(key("users", 1L), key("users", 2L))), any(InvalidatePolicy.class));

    service.delete(new long[0]);
    verify(zeta, times(1)).invalidate(anyList(), any(InvalidatePolicy.class));
    assertThat(target.calls).hasValue(2);
  }

  @Test
  @DisplayName("a null eachKey collection fails before the body")
  void nullEachKeyCollectionFailsBeforeBody() {
    BatchInvalidator target = new BatchInvalidator();
    BatchInvalidator service = proxy(target);

    assertThatThrownBy(() -> service.delete(null))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("resolved to null");
    assertThat(target.calls).hasValue(0);
    verify(zeta, never()).invalidate(anyList(), any(InvalidatePolicy.class));
  }

  static class ConstantBatchInvalidator {

    @ZetaInvalidate(cache = "users", key = "{'a','b'}", eachKey = true)
    public void delete() {}
  }

  @Test
  @DisplayName("a constant eachKey expression expands without an invocation context")
  void constantEachKeyExpandsWithoutAContext() {
    proxy(new ConstantBatchInvalidator()).delete();

    verify(zeta).invalidate(eq(List.of(key("users", "a"), key("users", "b"))), any(InvalidatePolicy.class));
  }

  static class NamespaceClear {

    final AtomicInteger calls = new AtomicInteger();

    @ZetaInvalidate(cache = "dict", allEntries = true)
    public void reload() {
      calls.incrementAndGet();
    }
  }

  @Test
  @DisplayName("allEntries clears the namespace instead of removing one key")
  void allEntriesClearsNamespace() {
    NamespaceClear target = new NamespaceClear();
    proxy(target).reload();

    assertThat(target.calls).hasValue(1);
    verify(zeta).clearNamespace(eq("dict"), any(InvalidatePolicy.class));
    verify(zeta, never()).invalidate(anyString(), any(InvalidatePolicy.class));
  }

  // ---------------------------------------------------------------------
  // composed annotation / @ZetaConfig
  // ---------------------------------------------------------------------

  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  @ZetaCached(key = "'fixed'")
  @interface UserCache {
    @AliasFor(annotation = ZetaCached.class, attribute = "key")
    String key() default "#p0";
  }

  @ZetaConfig(cache = "users", hardTtl = "60000")
  static class ComposedConfigService {

    @UserCache
    public String load(String id) {
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("a composed annotation is discovered and @ZetaConfig supplies cache and TTL defaults")
  void composedAnnotationInheritsClassConfig() {
    ArgumentCaptor<GetPolicy> options = ArgumentCaptor.forClass(GetPolicy.class);
    proxy(new ComposedConfigService()).load("42");

    verify(zeta).get(eq(key("users", "42")), any(CacheLoader.class), options.capture());
    assertThat(options.getValue().hardTtlMs().getAsLong()).isEqualTo(60_000L);
  }

  @ZetaConfig(cache = "class-cache", hardTtl = "60000")
  static class MethodOverrideService {

    @ZetaCached(cache = "method-cache", hardTtl = "0")
    public String load(String id) {
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("a method cache/TTL wins over @ZetaConfig, and \"0\" stays an explicit default")
  void methodAttributesWinOverClassConfig() {
    ArgumentCaptor<GetPolicy> options = ArgumentCaptor.forClass(GetPolicy.class);
    proxy(new MethodOverrideService()).load("42");

    verify(zeta).get(eq(key("method-cache", "42")), any(CacheLoader.class), options.capture());
    assertThat(options.getValue().hardTtlMs().getAsLong()).isZero();
  }

  static class ConfigBaseService {

    @ZetaCached(key = "#p0")
    public String load(String id) {
      return "v-" + id;
    }
  }

  @ZetaConfig(cache = "inherited")
  static class ConfigChildService extends ConfigBaseService {}

  @Test
  @DisplayName("a @ZetaConfig on the bean class supplies an inherited method's namespace")
  void beanClassConfigSuppliesInheritedMethod() {
    ConfigChildService service = proxy(new ConfigChildService());

    assertThat(service.load("42")).isEqualTo("v-42");
    verify(zeta).get(eq(key("inherited", "42")), any(CacheLoader.class), any(GetPolicy.class));
  }

  interface ConfigParentView {
    @ZetaCached(key = "#p0")
    default String load(String id) {
      return "v-" + id;
    }
  }

  @ZetaConfig(cache = "view-cache")
  interface ConfigChildView extends ConfigParentView {}

  static class ConfigViewService implements ConfigChildView {}

  @Test
  @DisplayName("a @ZetaConfig on a child interface supplies an inherited default method's namespace")
  void childInterfaceConfigSuppliesInheritedDefaultMethod() {
    ConfigChildView service = (ConfigChildView) proxy(new ConfigViewService());

    assertThat(service.load("42")).isEqualTo("v-42");
    verify(zeta).get(eq(key("view-cache", "42")), any(CacheLoader.class), any(GetPolicy.class));
  }

  @Test
  @DisplayName("a child interface's own plan table resolves an inherited method against its @ZetaConfig")
  void childInterfaceTableUsesItsOwnConfig() throws NoSuchMethodException {
    Method method = ConfigParentView.class.getMethod("load", String.class);

    assertThat(resolver.matches(method, ConfigChildView.class)).isTrue();
  }

  static class NoCacheService {

    @ZetaCached
    public String load(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("a cache name that is still blank after @ZetaConfig fails startup")
  void blankCacheAfterFallbackFailsStartup() {
    Throwable thrown = catchThrowable(() -> proxy(new NoCacheService()));

    assertThat(thrown).isNotNull();
    assertThat(rootCause(thrown))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("cache name must not be blank");
  }

  static class BadAllEntriesInvalidator {

    @ZetaInvalidate(cache = "users", key = "#p0", allEntries = true)
    public void delete(String id) {}
  }

  static class BadEachKeyInvalidator {

    @ZetaInvalidate(cache = "users", key = "#p0", eachKey = true)
    public void delete(Long id) {}
  }

  static class ErasableEachKeyInvalidator {

    @ZetaInvalidate(cache = "users", key = "#keys", eachKey = true)
    public void delete(Object keys) {}
  }

  @Test
  @DisplayName("eachKey accepts Object and generic keys whose runtime type can be a collection")
  void eachKeyAcceptsErasableKeyTypes() {
    assertThat(catchThrowable(() -> proxy(new ErasableEachKeyInvalidator()))).isNull();
  }

  @Test
  @DisplayName("allEntries forbids a key and eachKey rejects a statically non-collection key")
  void invalidInvalidationShapesFailStartup() {
    Throwable allEntries = catchThrowable(() -> proxy(new BadAllEntriesInvalidator()));
    assertThat(allEntries).isNotNull();
    assertThat(rootCause(allEntries))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("allEntries = true");

    Throwable eachKey = catchThrowable(() -> proxy(new BadEachKeyInvalidator()));
    assertThat(eachKey).isNotNull();
    assertThat(rootCause(eachKey))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("not an array or Iterable");
  }

  static class BrokenThenGoodInvalidator {

    @ZetaInvalidate(cache = "broken", key = "#result.id")
    @ZetaInvalidate(cache = "good", key = "'k'")
    public String delete() {
      return null;
    }
  }

  @Test
  @DisplayName("one post-body invalidation failure does not suppress the remaining removals")
  void postBodyInvalidationFailureDoesNotSuppressSiblings() {
    BrokenThenGoodInvalidator service = proxy(new BrokenThenGoodInvalidator());

    Throwable thrown = catchThrowable(service::delete);

    assertThat(thrown).isNotNull();
    verify(zeta).invalidate(eq(key("good", "k")), any(InvalidatePolicy.class));
  }

  // ---------------------------------------------------------------------
  // failure policy
  // ---------------------------------------------------------------------

  static class PutFailureService {

    @ZetaPut(cache = "users", key = "#p0")
    public String save(String id) {
      return "saved-" + id;
    }
  }

  @Test
  @DisplayName("a put failure after the body is handed to the policy and the value is returned")
  void putFailureIsHandedToThePolicy() {
    doThrow(new IllegalStateException("cache down")).when(zeta).put(anyString(), any(), any(PutPolicy.class));
    AtomicReference<CacheFailure> failure = new AtomicReference<>();
    ZetaCacheInterceptor failing = new ZetaCacheInterceptor(zeta, resolver, failure::set);
    PutFailureService service = proxy(new PutFailureService(), failing);

    assertThat(service.save("42")).isEqualTo("saved-42");
    assertThat(failure.get().verb()).isEqualTo(CacheVerb.PUT);
    assertThat(failure.get().cache()).isEqualTo("users");
    assertThat(failure.get().keys()).containsExactly(key("users", "42"));
    assertThat(failure.get().value()).isEqualTo("saved-42");
  }

  static class BeforeInvalidateFailureService {

    final AtomicInteger calls = new AtomicInteger();

    @ZetaInvalidate(cache = "users", key = "#p0", beforeInvocation = true)
    public String rename(String id) {
      calls.incrementAndGet();
      return "renamed-" + id;
    }
  }

  @Test
  @DisplayName("the default policy rethrows a pre-invocation removal failure before the body")
  void preInvocationFailureStopsTheBody() {
    doThrow(new IllegalStateException("cache down")).when(zeta).invalidate(anyString(), any(InvalidatePolicy.class));
    BeforeInvalidateFailureService target = new BeforeInvalidateFailureService();
    BeforeInvalidateFailureService service = proxy(target);

    assertThatThrownBy(() -> service.rename("42"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("cache down");
    assertThat(target.calls).hasValue(0);
  }
}
