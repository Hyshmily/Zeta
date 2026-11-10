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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.annotation.ZetaCacheAdvisor;
import io.github.hyshmily.zeta.annotation.ZetaCacheInterceptor;
import io.github.hyshmily.zeta.annotation.ZetaCached;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.aop.config.AopConfigUtils;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@DisplayName("ZetaAnnotationAutoConfiguration")
class ZetaAnnotationAutoConfigurationTest {

  private final ApplicationContextRunner runner = new ApplicationContextRunner().withConfiguration(
    AutoConfigurations.of(ZetaAnnotationAutoConfiguration.class)
  );

  @Test
  @DisplayName("wires the interceptor, the advisor and bootstrap auto-proxy infrastructure")
  void wiresAdviceAndAutoProxyInfrastructure() {
    Zeta zeta = mock(Zeta.class);
    runner
      .withBean(Zeta.class, () -> zeta)
      .withBean(HotKeyCache.class, () -> mock(HotKeyCache.class))
      .run(context -> {
        assertThat(context).hasSingleBean(ZetaCacheInterceptor.class);
        assertThat(context).hasSingleBean(ZetaCacheAdvisor.class);
        assertThat(context.containsBean(AopConfigUtils.AUTO_PROXY_CREATOR_BEAN_NAME)).isTrue();
        ZetaCacheInterceptor interceptor = context.getBean(ZetaCacheInterceptor.class);
        assertThat(context.getBean(ZetaCacheAdvisor.class).getAdvice()).isSameAs(interceptor);
      });
  }

  @Test
  @DisplayName("an application-supplied interceptor wins and the advisor still points at it")
  void customInterceptorWins() {
    ZetaCacheInterceptor custom = new ZetaCacheInterceptor(mock(Zeta.class), null);
    runner
      .withBean(Zeta.class, () -> mock(Zeta.class))
      .withBean(HotKeyCache.class, () -> mock(HotKeyCache.class))
      .withBean(ZetaCacheInterceptor.class, () -> custom)
      .run(context -> {
        assertThat(context).hasSingleBean(ZetaCacheInterceptor.class);
        assertThat(context.getBean(ZetaCacheInterceptor.class)).isSameAs(custom);
        assertThat(context.getBean(ZetaCacheAdvisor.class).getAdvice()).isSameAs(custom);
      });
  }

  @Test
  @DisplayName("does not wire anything without an app-side cache (Worker-only mode)")
  void backsOffWithoutAppCache() {
    runner
      .withBean(Zeta.class, () -> mock(Zeta.class))
      .run(context -> {
        assertThat(context).doesNotHaveBean(ZetaCacheInterceptor.class);
        assertThat(context).doesNotHaveBean(ZetaCacheAdvisor.class);
        assertThat(context.containsBean(AopConfigUtils.AUTO_PROXY_CREATOR_BEAN_NAME)).isFalse();
      });
  }

  @Test
  @DisplayName("does not wire anything without a facade")
  void backsOffWithoutFacade() {
    runner
      .withBean(HotKeyCache.class, () -> mock(HotKeyCache.class))
      .run(context -> {
        assertThat(context).doesNotHaveBean(ZetaCacheInterceptor.class);
        assertThat(context).doesNotHaveBean(ZetaCacheAdvisor.class);
      });
  }

  static class AnnotatedService {

    final AtomicInteger calls = new AtomicInteger();

    @ZetaCached(cache = "users", key = "#p0")
    public String load(String id) {
      calls.incrementAndGet();
      return "real-" + id;
    }

    /** Non-annotated accessor: the CGLIB proxy delegates it to the target instance. */
    int calls() {
      return calls.get();
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class ServiceConfiguration {

    @Bean
    AnnotatedService annotatedService() {
      return new AnnotatedService();
    }
  }

  @Test
  @DisplayName("end to end: an annotated bean is proxied and its read goes through the facade")
  void annotatedBeanIsProxied() {
    Zeta zeta = mock(Zeta.class);
    when(zeta.get(anyString(), any(CacheLoader.class), any(GetPolicy.class))).thenAnswer(invocation -> {
      CacheLoader<String, Object> loader = invocation.getArgument(1);
      return loader.load(invocation.getArgument(0));
    });

    runner
      .withBean(Zeta.class, () -> zeta)
      .withBean(HotKeyCache.class, () -> mock(HotKeyCache.class))
      .withUserConfiguration(ServiceConfiguration.class)
      .run(context -> {
        AnnotatedService service = context.getBean(AnnotatedService.class);
        assertThat(service.getClass()).isNotSameAs(AnnotatedService.class);
        assertThat(service.load("42")).isEqualTo("real-42");
        assertThat(service.calls()).isEqualTo(1);
        verify(zeta).get(eq("users\u0000[s2:42]"), any(CacheLoader.class), any(GetPolicy.class));
      });
  }
}
