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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.InvalidatePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.PutPolicy;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.aop.Advisor;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.DefaultPointcutAdvisor;
import org.springframework.core.OrderComparator;
import org.springframework.core.Ordered;

/**
 * Advice-order and pointcut contracts.
 *
 * <p>Two properties are load-bearing and both are asserted here:
 *
 * <ul>
 *   <li><b>Method security stays outer.</b> A rejection must surface to the
 *       caller and must not be masked by a cache read — otherwise an
 *       unauthenticated caller would trigger the business load.</li>
 *   <li><b>The transaction advice stays inner.</b> The cache side effect of a
 *       write or an after-the-fact invalidation must happen after the
 *       transaction completed, so a rollback can never leave a stale/partial
 *       entry behind. The body itself still runs inside the transaction
 *       because the read's loader proceeds into it.</li>
 * </ul>
 */
@DisplayName("ZetaCacheAdvisor")
class ZetaCacheAdvisorTest {

  private Zeta zeta;
  private ZetaCacheInterceptor interceptor;
  private ZetaCacheAdvisor advisor;

  @BeforeEach
  void setUp() {
    zeta = mock(Zeta.class);
    interceptor = new ZetaCacheInterceptor(zeta, null);
    advisor = new ZetaCacheAdvisor(interceptor);
  }

  /**
   * Build the proxy the way Spring's auto-proxy creator does: collect the
   * advisors, sort them by order, and register the sorted list. Manual
   * {@code addAdvice} would preserve insertion order and make the ordering
   * assertions meaningless.
   */
  @SuppressWarnings("unchecked")
  private <T> T proxy(T target, MethodInterceptor... otherAdvices) {
    List<Advisor> advisors = new ArrayList<>();
    advisors.add(advisor);
    for (MethodInterceptor other : otherAdvices) {
      DefaultPointcutAdvisor wrapper = new DefaultPointcutAdvisor(other);
      if (other instanceof Ordered ordered) {
        wrapper.setOrder(ordered.getOrder());
      }
      advisors.add(wrapper);
    }
    advisors.sort(OrderComparator.INSTANCE);
    ProxyFactory factory = new ProxyFactory(target);
    factory.addAdvisors(advisors);
    return (T) factory.getProxy();
  }

  private void readRunsBody() {
    when(zeta.get(anyString(), any(CacheLoader.class), any(GetPolicy.class))).thenAnswer(invocation -> {
      CacheLoader<String, Object> loader = invocation.getArgument(1);
      return loader.load(invocation.getArgument(0));
    });
  }

  @Test
  @DisplayName("the advice sits between method security (order 100) and the transaction interceptor")
  void adviceOrderWindow() {
    assertThat(ZetaCacheAdvisor.ADVICE_ORDER).as("inside method security").isGreaterThan(100);
    assertThat(ZetaCacheAdvisor.ADVICE_ORDER)
      .as("outside the default transaction interceptor")
      .isLessThan(Ordered.LOWEST_PRECEDENCE);
  }

  static final class DeniedException extends RuntimeException {

    DeniedException() {
      super("denied");
    }
  }

  /** Models {@code @PreAuthorize}: order 100 is Spring Security's method-security order. */
  static final class SecurityAdvice implements MethodInterceptor, Ordered {

    @Override
    public Object invoke(MethodInvocation invocation) {
      throw new DeniedException();
    }

    @Override
    public int getOrder() {
      return 100;
    }
  }

  static class SecuredService {

    @ZetaCached(cache = "orders", key = "#p0")
    public String load(String id) {
      return "real-" + id;
    }
  }

  @Test
  @DisplayName("a method-security rejection is never turned into a cache read")
  void securityRejectionRunsFirst() {
    readRunsBody();
    SecuredService service = proxy(new SecuredService(), new SecurityAdvice());

    assertThatThrownBy(() -> service.load("42")).isInstanceOf(DeniedException.class);
    verify(zeta, never()).get(anyString(), any(CacheLoader.class), any(GetPolicy.class));
  }

  /** Models {@code TransactionInterceptor}: default order, so it runs inside the cache advice. */
  static final class TransactionAdvice implements MethodInterceptor, Ordered {

    private final int[] depth;

    private final int[] bodyDepth;

    private TransactionAdvice(int[] depth, int[] bodyDepth) {
      this.depth = depth;
      this.bodyDepth = bodyDepth;
    }

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
      depth[0]++;
      try {
        bodyDepth[0] = depth[0];
        return invocation.proceed();
      } finally {
        depth[0]--;
      }
    }

    @Override
    public int getOrder() {
      return Ordered.LOWEST_PRECEDENCE;
    }
  }

  static class TransactionalReader {

    @ZetaCached(cache = "users", key = "#p0")
    public String load(String id) {
      return "v-" + id;
    }
  }

  static class TransactionalWriter {

    @ZetaPut(cache = "users", key = "#p0")
    public String save(String id) {
      return "saved-" + id;
    }
  }

  static class TransactionalInvalidator {

    @ZetaInvalidate(cache = "users", key = "#p0", beforeInvocation = true)
    public String rename(String id) {
      return "renamed-" + id;
    }
  }

  @Test
  @DisplayName("cache side effects bracket the transaction: the body runs inside it, write/invalidate outside")
  void cacheSideEffectsBracketTheTransaction() throws Throwable {
    int[] depth = { 0 };
    int[] bodyDepth = { -1 };
    TransactionAdvice tx = new TransactionAdvice(depth, bodyDepth);

    // Read: the engine call and the loader run before the transaction advice is
    // entered; the body runs inside it (depth 1), which is the contract that
    // lets a cached method still see its transaction.
    int[] engineDepth = { -1 };
    when(zeta.get(anyString(), any(CacheLoader.class), any(GetPolicy.class))).thenAnswer(invocation -> {
      engineDepth[0] = depth[0];
      CacheLoader<String, Object> loader = invocation.getArgument(1);
      return loader.load(invocation.getArgument(0));
    });
    proxy(new TransactionalReader(), tx).load("42");
    assertThat(engineDepth[0]).isZero();
    assertThat(bodyDepth[0]).isEqualTo(1);

    // Write: the body runs inside the transaction, the cache write after it.
    int[] putDepth = { -1 };
    doAnswer(invocation -> {
      putDepth[0] = depth[0];
      return null;
    })
      .when(zeta)
      .put(anyString(), any(), any(PutPolicy.class));
    proxy(new TransactionalWriter(), tx).save("42");
    assertThat(bodyDepth[0]).isEqualTo(1);
    assertThat(putDepth[0]).isZero();

    // Invalidation before the body is the same fact: it runs before the
    // transaction starts, so it cannot be deferred to after-commit.
    int[] invalidateDepth = { -1 };
    doAnswer(invocation -> {
      invalidateDepth[0] = depth[0];
      return null;
    })
      .when(zeta)
      .invalidate(anyString(), any(InvalidatePolicy.class));
    proxy(new TransactionalInvalidator(), tx).rename("42");
    assertThat(invalidateDepth[0]).isZero();
    assertThat(bodyDepth[0]).isEqualTo(1);
  }

  static class VerbService {

    @ZetaCached(cache = "users", key = "#p0")
    public String read(String id) {
      return id;
    }

    @ZetaPut(cache = "users", key = "#p0")
    public String write(String id) {
      return id;
    }

    @ZetaInvalidate(cache = "users", key = "#p0")
    public String remove(String id) {
      return id;
    }

    public String plain(String id) {
      return id;
    }

    @ZetaCached(cache = "users", key = "#p0")
    public static String staticRead(String id) {
      return id;
    }

    @ZetaCached(cache = "users", key = "#p0")
    private String privateRead(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("the pointcut matches only non-static, non-private verb methods")
  void pointcutMatchesVerbMethodsOnly() throws NoSuchMethodException {
    Class<VerbService> type = VerbService.class;
    assertThat(matches(type.getMethod("read", String.class))).isTrue();
    assertThat(matches(type.getMethod("write", String.class))).isTrue();
    assertThat(matches(type.getMethod("remove", String.class))).isTrue();
    assertThat(matches(type.getMethod("plain", String.class))).isFalse();
    assertThat(matches(type.getMethod("staticRead", String.class))).isFalse();
    assertThat(matches(type.getDeclaredMethod("privateRead", String.class))).isFalse();
  }

  static class InvalidVerbService {

    @ZetaCached(cache = "users", key = "'unclosed")
    public String load(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("a matching pointcut resolves the plan: a bad verb fails while the proxy is built")
  void pointcutValidatesAtMatchTime() throws NoSuchMethodException {
    java.lang.reflect.Method method = InvalidVerbService.class.getMethod("load", String.class);

    assertThatThrownBy(() -> matches(method))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("cannot be parsed");
    assertThatThrownBy(() -> matches(method))
      .as("a failed scan is not recorded as a validated class")
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("cannot be parsed");
  }

  static class PartiallyInvalidService {

    @ZetaCached(cache = "users", key = "#p0")
    public String good(String id) {
      return id;
    }

    @ZetaCached(cache = "users", key = "#p0")
    @ZetaPut(cache = "users", key = "#p0")
    public String bad(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("the first matching method validates every candidate method of the class")
  void firstMatchValidatesTheWholeClass() throws Exception {
    java.lang.reflect.Method good = PartiallyInvalidService.class.getMethod("good", String.class);

    assertThatThrownBy(() -> matches(good))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("mutually exclusive");
    assertThatThrownBy(() -> matches(good))
      .as("a failed scan records no plan, so the retry re-validates the whole class")
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("mutually exclusive");
  }

  interface DualKeyReader {
    String load(String id);
  }

  static class DualKeyReaderImpl implements DualKeyReader {

    @Override
    @ZetaCached(cache = "users", key = "#p0")
    public String load(String id) {
      return id;
    }
  }

  @Test
  @DisplayName("an interface method and its implementation share one cached plan")
  void interfaceAndImplementationShareThePlan() throws Exception {
    java.lang.reflect.Method implementation = DualKeyReaderImpl.class.getMethod("load", String.class);
    java.lang.reflect.Method declared = DualKeyReader.class.getMethod("load", String.class);

    assertThat(matches(implementation)).isTrue();

    Field resolverField = ZetaCacheInterceptor.class.getDeclaredField("resolver");
    resolverField.setAccessible(true);
    Object resolver = resolverField.get(interceptor);

    Field classPlans = PlanResolver.class.getDeclaredField("classPlans");
    classPlans.setAccessible(true);
    Map<?, ?> plans = (Map<?, ?>) ((ClassValue<?>) classPlans.get(resolver)).get(DualKeyReaderImpl.class);

    assertThat(plans).hasSize(2);
    assertThat(plans.get(declared))
      .as("the interface verb reuses the implementation's plan instead of compiling it twice")
      .isSameAs(plans.get(implementation));
  }

  private boolean matches(java.lang.reflect.Method method) {
    return advisor.getPointcut().getMethodMatcher().matches(method, method.getDeclaringClass());
  }
}
