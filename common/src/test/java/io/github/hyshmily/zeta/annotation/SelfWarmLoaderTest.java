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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.github.hyshmily.zeta.annotation.support.NamespaceKeys;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.policy.KeyFormat;
import io.github.hyshmily.zeta.policy.NamespacePolicyRegistry;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.interceptor.ExposeInvocationInterceptor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.beans.factory.support.StaticListableBeanFactory;

@DisplayName("selfWarmLoader")
class SelfWarmLoaderTest {

  private static PlanResolver resolver() {
    return new PlanResolver(null, () -> null, () -> new NamespaceKeys(KeyFormat.TYPED));
  }

  private static Method read(Class<?> type, String name, Class<?>... parameters) throws Exception {
    return type.getDeclaredMethod(name, parameters);
  }

  private static CacheLoader<String, Object> loader(
    PlanResolver resolver,
    Object target,
    Method method,
    String namespaceKey
  ) {
    assertThat(resolver.matches(method, target.getClass())).isTrue();
    NamespacePolicyRegistry policies = new NamespacePolicyRegistry(new ZetaProperties(), null);
    StaticListableBeanFactory beanFactory = new StaticListableBeanFactory(Map.of("target", target));
    new AnnotatedWarmLoaderBinder(beanFactory, resolver, policies).bindAll();
    CacheLoader<String, Object> loader = policies.resolve(namespaceKey).loader();
    assertThat(loader).isNotNull();
    return loader;
  }

  static class UserService {

    final AtomicInteger calls = new AtomicInteger();

    @ZetaCached(cache = "users", key = "#p0", selfWarmLoader = true)
    public String load(Long id) {
      calls.incrementAndGet();
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("a TYPED warmed method decodes the physical key and invokes the body")
  void typedLoad() throws Exception {
    UserService target = new UserService();
    PlanResolver resolver = resolver();
    CacheLoader<String, Object> loader = loader(
      resolver,
      target,
      read(UserService.class, "load", Long.class),
      "users\u0000[J:42]"
    );
    assertThat(resolver.warmSpecs().iterator().next().encoding().namespace()).isEqualTo("users");

    assertThat(loader.load("users\u0000[J:42]")).isEqualTo("v-42");
    assertThat(loader.load("users\u0000[J:43]")).isEqualTo("v-43");
    assertThat(loader.load("other\u0000[J:42]")).isNull();
    assertThat(target.calls).hasValue(2);
  }

  interface InheritedApi {
    @ZetaCached(cache = "inherited", key = "#p0", selfWarmLoader = true)
    String find(String id);
  }

  static class InheritedBase {

    final AtomicInteger calls = new AtomicInteger();

    public String find(String id) {
      calls.incrementAndGet();
      return "v-" + id;
    }
  }

  /** The body is inherited from a superclass that implements no view at all. */
  static class InheritedService extends InheritedBase implements InheritedApi {}

  @Test
  @DisplayName("an interface view and a body inherited from a non-implementing superclass are one loader")
  void inheritedBodyAndInterfaceViewShareOneLoader() throws Exception {
    PlanResolver resolver = resolver();
    Method view = InheritedApi.class.getMethod("find", String.class);

    assertThat(resolver.matches(view, InheritedApi.class)).isTrue();
    assertThat(resolver.matches(view, InheritedService.class)).isTrue();

    AnnotatedWarmLoaderBinder.WarmSpec spec = resolver.warmSpecs().iterator().next();
    assertThat(spec.specific()).isEqualTo(InheritedBase.class.getMethod("find", String.class));
    assertThat(spec.beanClass()).isEqualTo(InheritedService.class);
  }

  @ZetaConfig(cache = "plainUsers", keyFormat = KeyFormat.Option.PLAIN)
  static class PlainService {

    final AtomicInteger calls = new AtomicInteger();

    @ZetaCached(key = "#p0", selfWarmLoader = true)
    public String load(Long id) {
      calls.incrementAndGet();
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("a PLAIN warmed method decodes namespace:value")
  void plainLoad() throws Exception {
    PlainService target = new PlainService();
    PlanResolver resolver = resolver();
    CacheLoader<String, Object> loader = loader(
      resolver,
      target,
      read(PlainService.class, "load", Long.class),
      "plainUsers:42"
    );

    assertThat(loader.load("plainUsers:42")).isEqualTo("v-42");
    assertThat(loader.load("plainUsers:nope")).isNull();
    assertThat(target.calls).hasValue(1);
  }

  static class ConditionalService {

    @ZetaCached(cache = "users", key = "#p0", condition = "#p0 != null", selfWarmLoader = true)
    public String load(Long id) {
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("a warmed method cannot depend on a condition")
  void rejectsCondition() throws Exception {
    PlanResolver resolver = resolver();

    assertThatThrownBy(() ->
      resolver.matches(read(ConditionalService.class, "load", Long.class), ConditionalService.class)
    )
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("cannot combine with condition");
  }

  static class ParamUnlessService {

    @ZetaCached(cache = "users", key = "#p0", unless = "#p0 == null", selfWarmLoader = true)
    public String load(Long id) {
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("a warmed method's unless may read only #result")
  void rejectsParameterUnless() throws Exception {
    PlanResolver resolver = resolver();

    assertThatThrownBy(() ->
      resolver.matches(read(ParamUnlessService.class, "load", Long.class), ParamUnlessService.class)
    )
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("only allows #result in unless");
  }

  static class RootUnlessService {

    @ZetaCached(cache = "users", key = "#p0", unless = "args[0] == null", selfWarmLoader = true)
    public String load(Long id) {
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("a warmed method cannot read the invocation through a bare root property")
  void rejectsRootPropertyUnless() throws Exception {
    PlanResolver resolver = resolver();

    assertThatThrownBy(() ->
      resolver.matches(read(RootUnlessService.class, "load", Long.class), RootUnlessService.class)
    )
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("only allows #result in unless");
  }

  static class SelectionUnlessService {

    @ZetaCached(cache = "users", key = "#p0", unless = "#result.items.?[active].isEmpty()", selfWarmLoader = true)
    public String load(Long id) {
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("a warmed method's unless may select over #result without reading the invocation")
  void acceptsSelectionUnless() throws Exception {
    PlanResolver resolver = resolver();

    assertThat(
      resolver.matches(read(SelectionUnlessService.class, "load", Long.class), SelectionUnlessService.class)
    ).isTrue();
  }

  interface LeftView {
    String left(Long id);
  }

  interface RightView {
    @ZetaCached(cache = "users", key = "#p0", selfWarmLoader = true)
    String load(Long id);
  }

  static class BothViews implements LeftView, RightView {

    @Override
    public String left(Long id) {
      return "l-" + id;
    }

    @Override
    public String load(Long id) {
      return "v-" + id;
    }
  }

  static class LeftOnly implements LeftView {

    @Override
    public String left(Long id) {
      return "x-" + id;
    }
  }

  @Test
  @DisplayName("a multi-interface JDK proxy binds the warmed method through its declaring interface")
  void multiInterfaceProxyBindsTheDeclaringInterface() throws Exception {
    BothViews target = new BothViews();
    Object proxy = Proxy.newProxyInstance(
      getClass().getClassLoader(),
      new Class<?>[] { LeftView.class, RightView.class },
      (ignored, method, args) -> method.invoke(target, args)
    );
    PlanResolver resolver = resolver();
    Method load = RightView.class.getMethod("load", Long.class);
    assertThat(resolver.matches(load, proxy.getClass())).isTrue();

    NamespacePolicyRegistry policies = new NamespacePolicyRegistry(new ZetaProperties(), null);
    StaticListableBeanFactory beanFactory = new StaticListableBeanFactory(
      Map.of("proxy", proxy, "leftOnly", new LeftOnly())
    );
    new AnnotatedWarmLoaderBinder(beanFactory, resolver, policies).bindAll();

    assertThat(policies.resolve("users\u0000[J:42]").loader()).isNotNull();
  }

  @Test
  @DisplayName("a JDK proxy binds an implementation-scanned warm method through its ultimate target")
  void jdkProxyBindsImplementationScannedWarmMethod() throws Exception {
    WarmApiImpl target = new WarmApiImpl();
    ProxyFactory factory = new ProxyFactory(target);
    factory.setInterfaces(WarmApi.class);
    factory.setProxyTargetClass(false);
    Object proxy = factory.getProxy();
    PlanResolver resolver = resolver();
    Method load = WarmApiImpl.class.getMethod("load", Long.class);
    assertThat(resolver.matches(load, WarmApiImpl.class)).isTrue();

    NamespacePolicyRegistry policies = new NamespacePolicyRegistry(new ZetaProperties(), null);
    StaticListableBeanFactory beanFactory = new StaticListableBeanFactory(Map.of("proxy", proxy));
    new AnnotatedWarmLoaderBinder(beanFactory, resolver, policies).bindAll();

    CacheLoader<String, Object> loader = policies.resolve("proxyUsers\u0000[J:42]").loader();
    assertThat(loader).isNotNull();
    assertThat(loader.load("proxyUsers\u0000[J:42]")).isEqualTo("v-42");
    assertThat(target.calls).hasValue(1);
  }

  interface WarmApi {
    String load(Long id);
  }

  static class WarmApiImpl implements WarmApi {

    final AtomicInteger calls = new AtomicInteger();

    @Override
    @ZetaCached(cache = "proxyUsers", key = "#p0", selfWarmLoader = true)
    public String load(Long id) {
      calls.incrementAndGet();
      return "v-" + id;
    }
  }

  static class NoOpAdvice implements MethodInterceptor {

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
      return invocation.proceed();
    }
  }

  static class AdvisedWarmService {

    @ZetaCached(cache = "advisedUsers", key = "#p0", selfWarmLoader = true)
    public String load(Long id) {
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("advice the background load cannot honour is named in a WARN, not rejected")
  void warnsAboutAdviceTheBackgroundLoadCannotHonour() throws Exception {
    AdvisedWarmService target = new AdvisedWarmService();
    ProxyFactory factory = new ProxyFactory(target);
    factory.setProxyTargetClass(true);
    factory.addAdvice(ExposeInvocationInterceptor.INSTANCE);
    factory.addAdvice(new NoOpAdvice());
    Object proxy = factory.getProxy();

    PlanResolver resolver = resolver();
    Method load = AdvisedWarmService.class.getMethod("load", Long.class);
    assertThat(resolver.matches(load, AdvisedWarmService.class)).isTrue();

    NamespacePolicyRegistry policies = new NamespacePolicyRegistry(new ZetaProperties(), null);
    StaticListableBeanFactory beanFactory = new StaticListableBeanFactory(Map.of("proxy", proxy));

    Logger binderLogger = (Logger) LoggerFactory.getLogger(AnnotatedWarmLoaderBinder.class);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    binderLogger.addAppender(appender);
    try {
      new AnnotatedWarmLoaderBinder(beanFactory, resolver, policies).bindAll();
    } finally {
      binderLogger.detachAppender(appender);
    }

    assertThat(policies.resolve("advisedUsers\u0000[J:42]").loader()).isNotNull();
    assertThat(appender.list).anySatisfy(event -> {
      assertThat(event.getLevel()).isEqualTo(Level.WARN);
      assertThat(event.getFormattedMessage())
        .contains("selfWarmLoader = true")
        .contains("NoOpAdvice")
        .doesNotContain("ExposeInvocationInterceptor")
        .contains("ZetaWarmConfigurer");
    });
  }

  interface AnnotatedApi {
    @ZetaCached(cache = "viewUsers", key = "#p0", selfWarmLoader = true)
    String find(Long id);
  }

  static class AnnotatedApiImpl implements AnnotatedApi {

    @Override
    public String find(Long id) {
      return "v-" + id;
    }
  }

  @Test
  @DisplayName("the interface and implementation views of one warmed method are one declaration")
  void interfaceAndImplementationViewsAreNotDuplicates() throws Exception {
    PlanResolver resolver = resolver();
    Method implMethod = AnnotatedApiImpl.class.getMethod("find", Long.class);
    Method ifaceMethod = AnnotatedApi.class.getMethod("find", Long.class);

    assertThat(resolver.matches(implMethod, AnnotatedApiImpl.class)).isTrue();
    assertThat(resolver.matches(ifaceMethod, AnnotatedApi.class)).isTrue();

    assertThat(resolver.warmSpecs()).hasSize(1);
    assertThat(resolver.warmSpecs().iterator().next().beanClass()).isEqualTo(AnnotatedApiImpl.class);
  }

  abstract static class BaseService<T> {

    final AtomicInteger calls = new AtomicInteger();

    @ZetaCached(cache = "genericUsers", key = "#p0", selfWarmLoader = true)
    public String find(T id) {
      calls.incrementAndGet();
      return "v-" + id;
    }
  }

  static class LongService extends BaseService<Long> {}

  @Test
  @DisplayName("a warmed method inherited from a generic base decodes its resolved type argument")
  void genericBaseMethodResolvesItsTypeArgument() throws Exception {
    LongService target = new LongService();
    PlanResolver resolver = resolver();
    Method find = BaseService.class.getDeclaredMethod("find", Object.class);
    CacheLoader<String, Object> loader = loader(resolver, target, find, "genericUsers\u0000[J:42]");

    assertThat(loader.load("genericUsers\u0000[J:42]")).isEqualTo("v-42");
    assertThat(loader.load("genericUsers\u0000[J:43]")).isEqualTo("v-43");
    assertThat(target.calls).hasValue(2);
  }

  @ZetaConfig(cache = "parentWarm")
  static class ParentWarmService {

    final AtomicInteger calls = new AtomicInteger();

    @ZetaCached(key = "#p0", selfWarmLoader = true)
    public String load(Long id) {
      calls.incrementAndGet();
      return "parent-" + id;
    }
  }

  @ZetaConfig(cache = "childWarm")
  static class ChildWarmService extends ParentWarmService {

    @Override
    public String load(Long id) {
      calls.incrementAndGet();
      return "child-" + id;
    }
  }

  @Test
  @DisplayName("coexisting parent and child beans each bind the warm loader of their own namespace")
  void coexistingParentAndChildBeansBindByExactTargetClass() throws Exception {
    ParentWarmService parent = new ParentWarmService();
    ChildWarmService child = new ChildWarmService();
    PlanResolver resolver = resolver();
    Method load = read(ParentWarmService.class, "load", Long.class);
    assertThat(resolver.matches(load, ParentWarmService.class)).isTrue();
    assertThat(resolver.matches(load, ChildWarmService.class)).isTrue();

    NamespacePolicyRegistry policies = new NamespacePolicyRegistry(new ZetaProperties(), null);
    StaticListableBeanFactory beanFactory = new StaticListableBeanFactory(
      Map.of("parentWarmService", parent, "childWarmService", child)
    );
    new AnnotatedWarmLoaderBinder(beanFactory, resolver, policies).bindAll();

    String parentKey = "parentWarm\u0000[J:42]";
    String childKey = "childWarm\u0000[J:42]";
    assertThat(policies.resolve(parentKey).loader().load(parentKey)).isEqualTo("parent-42");
    assertThat(policies.resolve(childKey).loader().load(childKey)).isEqualTo("child-42");
    assertThat(parent.calls).hasValue(1);
    assertThat(child.calls).hasValue(1);
  }

  @Test
  @DisplayName("a not-yet-created @Lazy subclass bean is skipped instead of being forced into existence")
  void uninstantiatedLazySubclassIsSkipped() throws Exception {
    PlanResolver resolver = resolver();
    Method load = read(ParentWarmService.class, "load", Long.class);
    assertThat(resolver.matches(load, ParentWarmService.class)).isTrue();

    DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
    beanFactory.registerBeanDefinition("parentWarmService", new RootBeanDefinition(ParentWarmService.class));
    RootBeanDefinition lazyChild = new RootBeanDefinition(ChildWarmService.class);
    lazyChild.setLazyInit(true);
    beanFactory.registerBeanDefinition("lazyChildWarmService", lazyChild);
    beanFactory.preInstantiateSingletons();
    assertThat(beanFactory.containsSingleton("lazyChildWarmService")).isFalse();

    NamespacePolicyRegistry policies = new NamespacePolicyRegistry(new ZetaProperties(), null);
    new AnnotatedWarmLoaderBinder(beanFactory, resolver, policies).bindAll();

    assertThat(beanFactory.containsSingleton("lazyChildWarmService"))
      .as("binding must not instantiate a @Lazy bean to inspect its target class")
      .isFalse();
    String key = "parentWarm\u0000[J:42]";
    assertThat(policies.resolve(key).loader().load(key)).isEqualTo("parent-42");
  }

  static class CompositeKeyService {

    @ZetaCached(cache = "users", key = "#p0", selfWarmLoader = true)
    public String load(List<String> ids) {
      return String.join(",", ids);
    }
  }

  @Test
  @DisplayName("a warmed method needs a reversible scalar key")
  void rejectsCompositeKey() throws Exception {
    PlanResolver resolver = resolver();

    assertThatThrownBy(() ->
      resolver.matches(read(CompositeKeyService.class, "load", List.class), CompositeKeyService.class)
    )
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("reversible scalar key");
  }

  static class DuplicateWarmService {

    @ZetaCached(cache = "users", key = "#p0", selfWarmLoader = true)
    public String first(Long id) {
      return "a-" + id;
    }

    @ZetaCached(cache = "users", key = "#p0", selfWarmLoader = true)
    public String second(Long id) {
      return "b-" + id;
    }
  }

  @Test
  @DisplayName("two warmed methods in one namespace fail resolution")
  void rejectsDuplicateWarmMethod() {
    PlanResolver resolver = resolver();

    assertThatThrownBy(() ->
      resolver.matches(read(DuplicateWarmService.class, "first", Long.class), DuplicateWarmService.class)
    )
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("more than one selfWarmLoader");
  }
}
