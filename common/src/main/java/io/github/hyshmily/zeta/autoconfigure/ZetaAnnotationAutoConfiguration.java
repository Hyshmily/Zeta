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
package io.github.hyshmily.zeta.autoconfigure;

import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.annotation.AnnotatedWarmLoaderBinder;
import io.github.hyshmily.zeta.annotation.PlanResolver;
import io.github.hyshmily.zeta.annotation.ZetaCacheAdvisor;
import io.github.hyshmily.zeta.annotation.ZetaCacheInterceptor;
import io.github.hyshmily.zeta.annotation.support.NamespaceKeys;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.policy.NamespacePolicyRegistry;
import io.github.hyshmily.zeta.policy.NamespacePolicyRegistry.WarmLoaderBinder;
import io.github.hyshmily.zeta.util.Internal;
import org.aopalliance.intercept.MethodInterceptor;
import org.jspecify.annotations.NonNull;
import org.springframework.aop.config.AopConfigUtils;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Role;
import org.springframework.core.type.AnnotationMetadata;

/**
 * Auto-configuration for the annotation-driven cache verbs
 * {@link io.github.hyshmily.zeta.annotation.ZetaCached @ZetaCached},
 * {@link io.github.hyshmily.zeta.annotation.ZetaPut @ZetaPut} and
 * {@link io.github.hyshmily.zeta.annotation.ZetaInvalidate @ZetaInvalidate}.
 *
 * <p>No switch: the advice only bids for methods that carry one of the three
 * annotations, so an application that never uses them pays a pointcut check per
 * bean and nothing else. Gating on the app-side {@link HotKeyCache} bean (and
 * the {@link Zeta} facade it is addressed through) keeps the advice out of a
 * Worker-only deployment, where there is no L1 table for the verbs to address;
 * the facade exists there too, created with a {@code null} engine.
 *
 * <p><b>Auto-proxy infrastructure is part of the contract.</b> An
 * {@link org.springframework.aop.Advisor} bean is only applied if an
 * auto-proxy creator exists. Spring Boot installs one only when AspectJ is on
 * the classpath ({@code spring-boot-starter-aop}) or another {@code @Enable*}
 * feature registers one, so relying on the host application would make the
 * three annotations silently inert in a plain {@code spring-boot-starter}
 * application. {@link ProxyCreatorRegistrar} therefore registers the same
 * infrastructure-only creator Spring's own {@code @EnableCaching} /
 * {@code @EnableTransactionManagement} register, idempotently: if the
 * application (or Boot's AOP auto-configuration) already installed a creator
 * under {@link AopConfigUtils#AUTO_PROXY_CREATOR_BEAN_NAME}, it upgrades rather
 * than duplicates it.
 */
@Internal
@AutoConfiguration(after = { ZetaFacadeAutoConfiguration.class, ZetaAutoConfiguration.class })
@ConditionalOnClass(MethodInterceptor.class)
@ConditionalOnBean({ HotKeyCache.class, Zeta.class })
@Import(ZetaAnnotationAutoConfiguration.ProxyCreatorRegistrar.class)
public class ZetaAnnotationAutoConfiguration {

  /**
   * Create the lightweight plan source. It depends only on the bean factory: the two registries
   * arrive as suppliers and are resolved by the first plan that needs them.
   *
   * <p>Resolving a plan also publishes the method's namespace contract, so registration happens
   * exactly when the annotated bean is proxied — before any Worker decision can arrive for its
   * keys. The indirection is load-bearing: this bean and {@link ZetaCacheAdvisor} are
   * infrastructure, and the advisor is created while the auto-proxy creator initializes, before
   * BeanPostProcessors are registered. Resolving the registries here would instantiate the engine,
   * its properties, the namespace keys and the user's cluster loader ahead of their
   * post-processing.
   *
   * @param beanFactory the bean factory backing {@code @beanName} SpEL and
   *                    {@code ${...}} attribute placeholders
   * @param contracts   supplies the namespace contract registry, absent only in a deployment
   *                    without an L1 engine
   * @param keys        supplies the plan-time format registry
   * @return the plan resolver
   */
  @Bean
  @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
  @ConditionalOnMissingBean
  public static PlanResolver zetaPlanResolver(
    BeanFactory beanFactory,
    ObjectProvider<NamespacePolicyRegistry> contracts,
    ObjectProvider<NamespaceKeys> keys
  ) {
    return new PlanResolver(beanFactory, contracts::getIfAvailable, keys::getIfAvailable);
  }

  /**
   * Bind every {@code selfWarmLoader} method to its bean. The finalizer runs this after all
   * singletons exist, so the plan built at proxy time can be paired with the target object.
   *
   * @param beanFactory the bean factory that owns the target beans
   * @param resolver    the plan resolver holding the warmed method specs
   * @param policies    the namespace warming registry
   * @return the binder
   */
  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnBean(NamespacePolicyRegistry.class)
  public WarmLoaderBinder annotatedWarmLoaderBinder(
    ListableBeanFactory beanFactory,
    PlanResolver resolver,
    NamespacePolicyRegistry policies
  ) {
    return new AnnotatedWarmLoaderBinder(beanFactory, resolver, policies);
  }

  /**
   * Create the one advice behind the three annotations. The bean is resolved
   * lazily by {@link ZetaCacheAdvisor#INTERCEPTOR_BEAN_NAME}, so this method —
   * and the {@link Zeta} facade it needs — runs when the first advised bean is
   * proxied, not while BeanPostProcessors are being registered.
   *
   * @param zeta     the facade every verb goes through
   * @param resolver the plan source shared with the advisor
   * @return the interceptor
   */
  @Bean(name = ZetaCacheAdvisor.INTERCEPTOR_BEAN_NAME)
  @Lazy
  public ZetaCacheInterceptor zetaCacheInterceptor(Zeta zeta, PlanResolver resolver) {
    return new ZetaCacheInterceptor(zeta, resolver);
  }

  /**
   * Register the advice with the auto-proxy creator. Declared {@code static} so
   * the advisor can be created without instantiating the configuration class.
   *
   * <p>{@code ROLE_INFRASTRUCTURE} is load-bearing: the
   * {@code InfrastructureAdvisorAutoProxyCreator} registered below only considers
   * infrastructure-role advisors, so an application-role advisor would be
   * ignored whenever this configuration provides the auto-proxy infrastructure.
   *
   * @param resolver the lightweight plan source
   * @return the advisor
   */
  @Bean
  @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
  @ConditionalOnMissingBean
  public static ZetaCacheAdvisor zetaCacheAdvisor(PlanResolver resolver) {
    return new ZetaCacheAdvisor(resolver);
  }

  /**
   * Registers Spring's infrastructure-only auto-proxy creator, upgrading an
   * existing auto-proxy creator instead of duplicating it, so the three
   * annotations work without {@code spring-boot-starter-aop}. Mirrors what
   * Spring's own {@code @Enable*} selectors do through {@code AutoProxyRegistrar}.
   */
  static final class ProxyCreatorRegistrar implements ImportBeanDefinitionRegistrar {

    @Override
    public void registerBeanDefinitions(
      @NonNull AnnotationMetadata importingClassMetadata,
      @NonNull BeanDefinitionRegistry registry
    ) {
      AopConfigUtils.registerAutoProxyCreatorIfNecessary(registry);
    }
  }
}
