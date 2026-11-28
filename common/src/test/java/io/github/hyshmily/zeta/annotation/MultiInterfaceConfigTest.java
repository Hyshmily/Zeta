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
import java.lang.reflect.Proxy;
import java.util.Map;
import org.aopalliance.intercept.MethodInterceptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.aop.SpringProxy;
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

  private static Throwable rootCause(Throwable error) {
    Throwable cause = error;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    return cause;
  }

  private <T> T proxy(Class<?>... interfaces) {
    ProxyFactory factory = new ProxyFactory();
    // A target that implements the proxied views mirrors a real JDK proxy: the pointcut's target
    // class then exposes the business interfaces instead of an unrelated dummy. The terminal
    // interceptor below is the body, so the target method is never invoked.
    Object target = Proxy.newProxyInstance(
      MultiInterfaceConfigTest.class.getClassLoader(),
      interfaces,
      (proxy, method, args) -> {
        if (method.getDeclaringClass() != Object.class) {
          return null;
        }
        return switch (method.getName()) {
          case "equals" -> proxy == (args == null || args.length == 0 ? null : args[0]);
          case "hashCode" -> System.identityHashCode(proxy);
          case "toString" -> "target";
          default -> null;
        };
      }
    );
    factory.setTarget(target);
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
    return withCacheAdvice(factory);
  }

  /** A CGLIB proxy: the plan table is scanned for the bean class, as a class-level config is. */
  private <T> T classProxy(T target) {
    ProxyFactory factory = new ProxyFactory(target);
    factory.setProxyTargetClass(true);
    return withCacheAdvice(factory);
  }

  @SuppressWarnings("unchecked")
  private <T> T withCacheAdvice(ProxyFactory factory) {
    ZetaCacheAdvisor advisor = new ZetaCacheAdvisor(resolver);
    advisor.setAdvice(interceptor);
    factory.addAdvisor(advisor);
    return (T) factory.getProxy();
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

  @ZetaConfig(cache = "baseNS")
  interface BaseRepo<T> {
    @ZetaCached(key = "#id")
    String find(T id);
  }

  @ZetaConfig(cache = "childNS")
  interface ChildRepo extends BaseRepo<Long> {}

  @Test
  @DisplayName("the most specific interface supplies the config regardless of proxy interface order")
  void mostSpecificInterfaceSuppliesConfig() {
    BaseRepo<Long> service = proxy(BaseRepo.class, ChildRepo.class);

    service.find(42L);

    verify(zeta).get(eq(key("childNS", 42L)), any(CacheLoader.class), any(GetPolicy.class));
  }

  interface SharedView {
    @ZetaCached(key = "#id")
    String find(String id);
  }

  @ZetaConfig(cache = "orders")
  interface OrdersView extends SharedView {}

  @ZetaConfig(cache = "users")
  interface UsersView extends SharedView {}

  interface PlainView extends SharedView {}

  @Test
  @DisplayName("two sibling views with conflicting configs fail instead of taking the first one")
  void conflictingSiblingViewConfigsFailForAProxy() {
    SharedView service = proxy(OrdersView.class, UsersView.class);

    Throwable thrown = catchThrowable(() -> service.find("1"));

    assertThat(rootCause(thrown))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("conflicting @ZetaConfig")
      .hasMessageContaining(OrdersView.class.getName())
      .hasMessageContaining(UsersView.class.getName());
  }

  @Test
  @DisplayName("a proxy's single annotated view supplies the config regardless of interface order")
  void aSingleAnnotatedViewSuppliesTheConfig() {
    SharedView plainFirst = proxy(PlainView.class, UsersView.class);
    assertThat(plainFirst.find("1")).isEqualTo("v-1");
    verify(zeta).get(eq(key("users", "1")), any(CacheLoader.class), any(GetPolicy.class));

    SharedView annotatedFirst = proxy(UsersView.class, PlainView.class);
    assertThat(annotatedFirst.find("2")).isEqualTo("v-2");
    verify(zeta).get(eq(key("users", "2")), any(CacheLoader.class), any(GetPolicy.class));
  }

  interface TransactionalMarker extends SpringProxy {}

  @Test
  @DisplayName("an infrastructure sub-interface leaves the business view as the proxy's user class")
  void infrastructureSubInterfaceKeepsTheBusinessView() throws Exception {
    SharedView service = proxy(UsersView.class, TransactionalMarker.class);
    Method find = SharedView.class.getMethod("find", String.class);

    assertThat(resolver.matches(find, service.getClass())).isTrue();

    PlanResolver.PlanTable table = resolver.cachedTable(service.getClass());
    assertThat(table.userClass()).isEqualTo(UsersView.class);
    assertThat(table.plans())
      .as("the proxy shares the view's plans and their compiled expressions")
      .isSameAs(resolver.cachedTable(UsersView.class).plans());
  }

  @SuppressWarnings("unchecked")
  private CachePlan.Read readPlan(Class<?> proxyClass, Method method) throws Exception {
    Field field = PlanResolver.class.getDeclaredField("classPlans");
    field.setAccessible(true);
    Object table = ((Map<Class<?>, Object>) field.get(resolver)).get(proxyClass);
    Method plans = table.getClass().getDeclaredMethod("plans");
    plans.setAccessible(true);
    CachePlan plan = ((Map<Method, CachePlan>) plans.invoke(table)).get(method);
    assertThat(plan).isInstanceOf(CachePlan.Read.class);
    return (CachePlan.Read) plan;
  }

  @ZetaConfig(cache = "users", hardTtl = "60000")
  interface UserReader {
    @ZetaCached(key = "#id")
    String findUser(String id);
  }

  @ZetaConfig(cache = "orders", softTtl = "5000")
  interface OrderReader {
    @ZetaCached(key = "#id")
    String findOrder(String id);
  }

  static class CompositeReader implements UserReader, OrderReader {

    @Override
    public String findUser(String id) {
      return "user-" + id;
    }

    @Override
    public String findOrder(String id) {
      return "order-" + id;
    }
  }

  @Test
  @DisplayName("a class root keeps each sibling interface's namespace and TTLs on its own method")
  void concreteClassKeepsSiblingNamespacesApart() {
    CompositeReader service = classProxy(new CompositeReader());
    ArgumentCaptor<GetPolicy> policy = ArgumentCaptor.forClass(GetPolicy.class);

    assertThat(service.findOrder("7")).isEqualTo("order-7");

    verify(zeta).get(eq(key("orders", "7")), any(CacheLoader.class), policy.capture());
    assertThat(policy.getValue().hardTtlMs().getAsLong()).isZero();
    assertThat(policy.getValue().softTtlMs().getAsLong()).isEqualTo(5_000L);

    assertThat(service.findUser("8")).isEqualTo("user-8");

    verify(zeta).get(eq(key("users", "8")), any(CacheLoader.class), policy.capture());
    assertThat(policy.getValue().hardTtlMs().getAsLong()).isEqualTo(60_000L);
    assertThat(policy.getValue().softTtlMs().getAsLong()).isZero();
  }

  static class AmbiguousReader implements UserReader, OrderReader {

    @Override
    public String findUser(String id) {
      return id;
    }

    @Override
    public String findOrder(String id) {
      return id;
    }

    @ZetaCached(key = "#id")
    public String findLocal(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("a method only the class declares cannot pick one of two conflicting sibling configs")
  void conflictingSiblingConfigsFailForAClassOnlyMethod() {
    Throwable thrown = catchThrowable(() -> classProxy(new AmbiguousReader()).findLocal("1"));

    assertThat(thrown).isNotNull();
    assertThat(rootCause(thrown))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("conflicting @ZetaConfig")
      .hasMessageContaining(UserReader.class.getName())
      .hasMessageContaining(OrderReader.class.getName());
  }

  interface CrudView<T, ID> {
    T findById(ID id);
  }

  interface UserView extends CrudView<String, Long> {
    @Override
    @ZetaCached(cache = "users", key = "#id")
    String findById(Long id);
  }

  @Test
  @DisplayName("a specialized child signature serves the erased parent view from one plan")
  void specializedSignatureServesTheParentView() throws Exception {
    Method parent = CrudView.class.getMethod("findById", Object.class);
    Method child = UserView.class.getMethod("findById", Long.class);

    assertThat(resolver.matches(child, UserView.class)).isTrue();
    CachePlan plan = resolver.lookupPlanForMethod(parent, UserView.class);

    assertThat(plan).isSameAs(resolver.lookupPlanForMethod(child, UserView.class));
    assertThat(plan).isInstanceOf(CachePlan.Read.class);
    assertThat(((CachePlan.Read) plan).key().resolve(new Object[] { 42L }, null)).isEqualTo(key("users", 42L));
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
}
