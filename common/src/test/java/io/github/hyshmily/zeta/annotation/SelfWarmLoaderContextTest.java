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
import static org.mockito.Mockito.mock;

import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.annotation.support.NamespaceKeys;
import io.github.hyshmily.zeta.autoconfigure.ZetaAnnotationAutoConfiguration;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.policy.HeatTrigger;
import io.github.hyshmily.zeta.policy.KeyFormat;
import io.github.hyshmily.zeta.policy.NamespacePolicyRegistry;
import io.github.hyshmily.zeta.policy.NamespacePolicyRegistry.WarmLoaderBinder;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@DisplayName("selfWarmLoader context")
class SelfWarmLoaderContextTest {

  static class UserService {

    final AtomicInteger calls = new AtomicInteger();

    @ZetaCached(cache = "users", key = "#p0", prewarm = HeatTrigger.Option.CLUSTER, selfWarmLoader = true)
    public String load(Long id) {
      calls.incrementAndGet();
      return "v-" + id;
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class ServiceConfiguration {

    @Bean
    UserService userService() {
      return new UserService();
    }
  }

  @Test
  @DisplayName("the registrar binds the warmed method as its namespace loader")
  void bindsTheWarmedMethod() {
    ZetaProperties properties = new ZetaProperties();
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(ZetaAnnotationAutoConfiguration.class))
      .withBean(Zeta.class, () -> mock(Zeta.class))
      .withBean(HotKeyCache.class, () -> mock(HotKeyCache.class))
      .withBean(ZetaProperties.class, () -> properties)
      .withBean(NamespacePolicyRegistry.class, () -> new NamespacePolicyRegistry(properties, null))
      .withBean(NamespaceKeys.class, () -> new NamespaceKeys(KeyFormat.TYPED))
      .withUserConfiguration(ServiceConfiguration.class)
      .run(context -> {
        PlanResolver resolver = context.getBean(PlanResolver.class);
        assertThat(resolver.warmSpecs()).isNotEmpty();
        context.getBean(WarmLoaderBinder.class).bindAll();
        NamespacePolicyRegistry policies = context.getBean(NamespacePolicyRegistry.class);
        CacheLoader<String, Object> loader = policies.resolve("users\u0000[J:42]").loader();

        assertThat(loader).isNotNull();
        assertThat(loader.load("users\u0000[J:42]")).isEqualTo("v-42");
        UserService target = org.springframework.test.util.AopTestUtils.getTargetObject(
          context.getBean(UserService.class)
        );
        assertThat(target.calls).hasValue(1);
      });
  }
}
