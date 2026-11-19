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
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.annotation.support.DefaultKey;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import org.aopalliance.intercept.MethodInterceptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.DefaultPointcutAdvisor;

@DisplayName("@ZetaConfig across several business interfaces")
class MultiInterfaceConfigTest {

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

  static class DummyTarget {}

  private <T> T proxy(Class<?>... interfaces) {
    ProxyFactory factory = new ProxyFactory();
    // A user-class target keeps the target class available to the pointcut; the terminal
    // interceptor is the body, so the target method is never invoked and need not implement the
    // interfaces.
    factory.setTarget(new DummyTarget());
    factory.setInterfaces(interfaces);
    ZetaCacheAdvisor advisor = new ZetaCacheAdvisor(resolver);
    advisor.setAdvice(interceptor);
    factory.addAdvisor(advisor);
    factory.addAdvisor(
      new DefaultPointcutAdvisor((MethodInterceptor) invocation -> "v-" + invocation.getArguments()[0])
    );
    @SuppressWarnings("unchecked")
    T service = (T) factory.getProxy();
    return service;
  }

  private <T> T proxyTarget(T target) {
    ProxyFactory factory = new ProxyFactory(target);
    ZetaCacheAdvisor advisor = new ZetaCacheAdvisor(resolver);
    advisor.setAdvice(interceptor);
    factory.addAdvisor(advisor);
    @SuppressWarnings("unchecked")
    T service = (T) factory.getProxy();
    return service;
  }

  @ZetaConfig(cache = "left")
  interface LeftView {
    String unrelated();
  }

  @ZetaConfig(cache = "right")
  interface RightView {
    @ZetaCached(key = "#id")
    String load(String id);
  }

  @Test
  @DisplayName("a sibling interface does not lend its namespace to another view's method")
  void siblingConfigDoesNotLeak() throws Exception {
    RightView service = proxy(LeftView.class, RightView.class);
    Method load = RightView.class.getMethod("load", String.class);

    assertThat(resolver.matches(load, service.getClass())).isTrue();
    CachePlan.Read plan = readPlan(service.getClass(), load);
    assertThat(plan.key().resolve(new Object[] { "42" }, null)).isEqualTo(key("right", "42"));
  }

  @SuppressWarnings("unchecked")
  private CachePlan.Read readPlan(Class<?> proxyClass, Method method) throws Exception {
    Field field = PlanResolver.class.getDeclaredField("classPlans");
    field.setAccessible(true);
    Map<Class<?>, Map<Method, CachePlan>> tables = (Map<Class<?>, Map<Method, CachePlan>>) field.get(resolver);
    CachePlan plan = tables.get(proxyClass).get(method);
    assertThat(plan).isInstanceOf(CachePlan.Read.class);
    return (CachePlan.Read) plan;
  }

  @ZetaConfig(cache = "inherited", hardTtl = "60000")
  static class ParentConfig {
    @ZetaCached(key = "#id")
    public String load(String id) {
      return "v-" + id;
    }
  }

  @ZetaConfig(hardTtl = "30000")
  static class ChildConfig extends ParentConfig {}

  @Test
  @DisplayName("a child config overrides one attribute and inherits the rest")
  void childConfigMergesAttributeWise() {
    ChildConfig service = proxyTarget(new ChildConfig());
    ArgumentCaptor<GetPolicy> policy = ArgumentCaptor.forClass(GetPolicy.class);

    assertThat(service.load("42")).isEqualTo("v-42");
    verify(zeta).get(eq(key("inherited", "42")), any(CacheLoader.class), policy.capture());
    assertThat(policy.getValue().hardTtlMs().getAsLong()).isEqualTo(30_000L);
  }

  interface SharedView {
    @ZetaCached(key = "#id")
    String load(String id);
  }

  @ZetaConfig(cache = "first")
  interface FirstView extends SharedView {}

  @ZetaConfig(cache = "second")
  interface SecondView extends SharedView {}

  @Test
  @DisplayName("two applicable sibling configs for one inherited method fail resolution")
  void conflictingSiblingConfigsFail() throws Exception {
    SharedView service = proxy(FirstView.class, SecondView.class, SharedView.class);
    Method load = SharedView.class.getMethod("load", String.class);

    Throwable thrown = catchThrowable(() -> resolver.matches(load, service.getClass()));

    assertThat(thrown).isNotNull();
    Throwable diagnosis = thrown;
    while (diagnosis != null && !(diagnosis instanceof IllegalStateException)) {
      diagnosis = diagnosis.getCause();
    }
    assertThat(diagnosis).isInstanceOf(IllegalStateException.class);
    assertThat(diagnosis.getMessage()).contains("conflicting @ZetaConfig").contains("load");
  }

  @ZetaConfig(cache = "merged")
  interface CacheOnlyView extends SharedView {}

  @ZetaConfig(hardTtl = "60000")
  interface TtlOnlyView extends SharedView {}

  @Test
  @DisplayName("complementary attributes from two sibling roots merge instead of conflicting")
  void complementarySiblingConfigsMerge() throws Exception {
    SharedView service = proxy(CacheOnlyView.class, TtlOnlyView.class, SharedView.class);
    Method load = SharedView.class.getMethod("load", String.class);

    assertThat(resolver.matches(load, service.getClass())).isTrue();
    CachePlan.Read plan = readPlan(service.getClass(), load);
    assertThat(plan.key().resolve(new Object[] { "42" }, null)).isEqualTo(key("merged", "42"));
    assertThat(plan.prebuiltPolicy().hardTtlMs().getAsLong()).isEqualTo(60_000L);
  }
}
