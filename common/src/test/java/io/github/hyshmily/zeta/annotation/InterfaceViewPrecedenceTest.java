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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import java.util.concurrent.atomic.AtomicReference;
import org.aopalliance.intercept.MethodInterceptor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.DefaultPointcutAdvisor;

/**
 * Pins the interface-view precedence of one logical method: two unrelated interfaces may declare the
 * same signature, and the more specific return type must own the plan regardless of scan order.
 */
@DisplayName("Interface view precedence")
class InterfaceViewPrecedenceTest {

  private final AtomicReference<String> lastKey = new AtomicReference<>();
  private final PlanResolver resolver = new PlanResolver(null);

  private ZetaCacheInterceptor interceptor() {
    Zeta zeta = mock(Zeta.class);
    when(zeta.get(anyString(), any(CacheLoader.class), any(GetPolicy.class))).thenAnswer(invocation -> {
      lastKey.set(invocation.getArgument(0));
      CacheLoader<String, Object> loader = invocation.getArgument(1);
      return loader.load(invocation.getArgument(0));
    });
    return new ZetaCacheInterceptor(zeta, resolver);
  }

  private String call(ZetaCacheInterceptor interceptor, Class<?> first, Class<?> second) {
    ProxyFactory factory = new ProxyFactory();
    factory.setInterfaces(first, second);
    ZetaCacheAdvisor advisor = new ZetaCacheAdvisor(resolver);
    advisor.setAdvice(interceptor);
    factory.addAdvisor(advisor);
    factory.addAdvisor(
      new DefaultPointcutAdvisor((MethodInterceptor) invocation -> "v-" + invocation.getArguments()[0])
    );
    TextView service = (TextView) factory.getProxy();
    assertThat(service.load("42")).isEqualTo("v-42");
    return lastKey.get();
  }

  @Test
  @DisplayName("the covariant declaration owns the plan when the wider interface is scanned first")
  void covariantDeclarationWinsRegardlessOfOrder() {
    ZetaCacheInterceptor interceptor = interceptor();
    assertThat(call(interceptor, ObjectView.class, TextView.class)).contains("TextView:42");

    lastKey.set(null);
    assertThat(call(interceptor, TextView.class, ObjectView.class)).contains("TextView:42");
  }

  interface ObjectView {
    Object load(String id);
  }

  interface TextView {
    @ZetaCached(cache = "users", key = "#root.method.declaringClass.simpleName + ':' + #p0")
    String load(String id);
  }
}
