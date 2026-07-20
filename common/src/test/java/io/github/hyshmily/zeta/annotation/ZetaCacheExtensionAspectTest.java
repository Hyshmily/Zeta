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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.annotationsupporter.ZetaCacheContext;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;

@DisplayName("CacheExtensionAspect tests")
class ZetaCacheExtensionAspectTest {

  private Zeta zeta;
  private ZetaProperties properties;
  private ZetaProperties.SpringCache springCache;
  private CacheExtensionAspect aspect;

  @BeforeEach
  void setUp() {
    zeta = mock(Zeta.class);
    properties = mock(ZetaProperties.class);
    springCache = new ZetaProperties.SpringCache();
    springCache.setKeySeparator("::");
    when(properties.getSpringCache()).thenReturn(springCache);
    aspect = new CacheExtensionAspect(zeta, properties);
  }

  @AfterEach
  void tearDown() {
    ZetaCacheContext.get().restore(null);
  }

  // ── Test service helpers ──

  interface TestInterface {
    String find(String id);
  }

  static class TestInterfaceImpl implements TestInterface {

    @Override
    public String find(String id) {
      return "impl-" + id;
    }
  }

  static class TestService {

    @Cacheable(cacheNames = "test", key = "#p0")
    public String find(String id) {
      return "result-" + id;
    }

    public String findFallback(String id) {
      return "fallback-" + id;
    }

    @Cacheable(cacheNames = "test", key = "#p0")
    @Intercept
    @Fallback
    public String findInterceptedWithFallback(String id) {
      return "result-" + id;
    }

    public String findInterceptedWithFallbackFallback(String id) {
      return "fallback-" + id;
    }

    @Cacheable(cacheNames = "test", key = "#p0")
    @Intercept
    public String findInterceptedNoFallback(String id) {
      return "result-" + id;
    }

    @Cacheable(cacheNames = "test", key = "#p0")
    @Intercept(type = InterceptType.FORCE)
    public String findInterceptedForceTrueNoFallback(String id) {
      return "result-" + id;
    }

    @Cacheable(cacheNames = "test", key = "#p0")
    @Intercept(type = InterceptType.FORCE)
    @Fallback
    public String findInterceptedForceTrueWithFallback(String id) {
      return "result-" + id;
    }

    public String findInterceptedForceTrueWithFallbackFallback(String id) {
      return "fallback-force-true-" + id;
    }

    @Cacheable(cacheNames = "test", key = "#p0")
    @CacheTTL(hardTtlMs = 5000)
    public String findWithTtl(String id) {
      return "result-" + id;
    }

    @Cacheable(cacheNames = "test", key = "#p0")
    @NullCaching(true)
    public String findWithNullCaching(String id) {
      return "result-" + id;
    }

    @Cacheable(cacheNames = "test", key = "#p0")
    @SkipBroadcast
    public String findBroadcastOff(String id) {
      return "result-" + id;
    }

    @CachePut(cacheNames = "test", key = "#p0")
    @SkipBroadcast
    public String putBroadcastOff(String id) {
      return "result-" + id;
    }

    @CacheEvict(cacheNames = "test", key = "#p0")
    @SkipBroadcast
    public void evictBroadcastOff(String id) {}

    @CachePut(cacheNames = "test", key = "#p0")
    public String putBroadcastDefault(String id) {
      return "result-" + id;
    }

    @Cacheable(cacheNames = "test", key = "#p0")
    @Fallback
    public String findThrowing(String id) {
      throw new RuntimeException("from-method");
    }

    public String findThrowingFallback(String id) {
      return "fallback-result";
    }

    @Cacheable(cacheNames = "test", key = "#p0")
    public String findThrowingNoFallback(String id) {
      throw new RuntimeException("from-method");
    }

    @Cacheable(cacheNames = "test", key = "#p0")
    @Fallback
    public String findThrowingNoFallbackMethod(String id) {
      throw new RuntimeException("error");
    }

    @Cacheable(cacheNames = "test", key = "#p0")
    @Fallback
    public String findThrowingFallbackMethod(String id) {
      throw new RuntimeException("from-method");
    }

    public String findThrowingFallbackMethodFallback(String id) {
      throw new IllegalArgumentException("fallback-threw");
    }

    @Cacheable(cacheNames = "test", key = "#p0")
    @Intercept(type = InterceptType.QPS, qps = 5)
    public String findQpsIntercepted(String id) {
      return "result-" + id;
    }

    @Cacheable(cacheNames = "test", key = "#p0")
    @Intercept(type = InterceptType.QPS, qps = 5, fallback = "'qps-fallback'")
    public String findQpsInterceptedWithSpelFallback(String id) {
      return "result-" + id;
    }

    @Cacheable(cacheNames = "test", key = "#p0")
    @Intercept(type = InterceptType.CONCURRENT_THREADS, concurrentThreads = 2)
    public String findConcurrentThreadsIntercepted(String id) {
      return "result-" + id;
    }

    @Cacheable(cacheNames = "test", key = "#p0")
    @Preload(keys = { "preload-key-a", "preload-key-b" })
    public String findWithStaticPreload(String id) {
      return "result-" + id;
    }

    @Cacheable(cacheNames = "test", key = "#p0")
    @Preload(keyExpr = "#id")
    public String findWithDynamicPreload(String id) {
      return "result-" + id;
    }
  }

  static class NoArgService {

    @Cacheable(cacheNames = "test")
    public String find() {
      return "no-arg";
    }
  }

  static class MultiArgService {

    @Cacheable(cacheNames = "test")
    public String find(String a, String b) {
      return a + b;
    }
  }

  static class ArrayArgService {

    @Cacheable(cacheNames = "test")
    public String find(String[] ids) {
      return "array";
    }
  }

  static class SpelFallbackService {

    @Cacheable(cacheNames = "test", key = "#p0")
    @Intercept
    @Fallback("'spel-fallback-value'")
    public String find(String id) {
      return "result-" + id;
    }
  }

  static class BaseFallbackService {

    public String findFallback(String id) {
      return "base-fallback-" + id;
    }
  }

  static class DerivedFallbackService extends BaseFallbackService {

    @Cacheable(cacheNames = "test", key = "#p0")
    @Fallback
    public String find(String id) {
      throw new RuntimeException("error");
    }
  }

  // ── @Intercept tests ──

  @Test
  @DisplayName("@Intercept with hot key and @Fallback returns fallback value")
  void interceptWithZetaAndFallback_returnsFallback() throws Throwable {
    Method method = TestService.class.getMethod("findInterceptedWithFallback", String.class);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    MethodSignature signature = mock(MethodSignature.class);
    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);

    when(signature.getMethod()).thenReturn(method);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });

    when(zeta.isLocalHotKey("test::myId")).thenReturn(true);

    Object result = aspect.aroundCacheable(pjp, cacheable);

    assertThat(result).isEqualTo("fallback-myId");
    verify(pjp, never()).proceed();
  }

  @Test
  @DisplayName("@Intercept with hot key and no @Fallback returns peek value")
  void interceptWithZetaNoFallback_returnsPeekValue() throws Throwable {
    Method method = TestService.class.getMethod("findInterceptedNoFallback", String.class);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    MethodSignature signature = mock(MethodSignature.class);
    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);

    when(signature.getMethod()).thenReturn(method);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });

    when(zeta.isLocalHotKey("test::myId")).thenReturn(true);
    when(zeta.peek("test::myId")).thenReturn(Optional.of("cached-value"));

    Object result = aspect.aroundCacheable(pjp, cacheable);

    assertThat(result).isEqualTo("cached-value");
    verify(pjp, never()).proceed();
  }

  @Test
  @DisplayName("@Intercept with non-hot key proceeds normally")
  void interceptWithNonZeta_proceedsNormally() throws Throwable {
    Method method = TestService.class.getMethod("findInterceptedNoFallback", String.class);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    MethodSignature signature = mock(MethodSignature.class);
    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);

    when(signature.getMethod()).thenReturn(method);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(pjp.proceed()).thenReturn("result-myId");

    when(zeta.isLocalHotKey("test::myId")).thenReturn(false);

    Object result = aspect.aroundCacheable(pjp, cacheable);

    assertThat(result).isEqualTo("result-myId");
    verify(pjp).proceed();
  }

  // ── @Intercept(force=true) tests ──

  @Test
  @DisplayName("@Intercept(force=true) with non-hot key and no @Fallback returns peek value")
  void interceptForceTrueNotHotNoFallback_returnsPeekValue() throws Throwable {
    Method method = TestService.class.getMethod("findInterceptedForceTrueNoFallback", String.class);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    MethodSignature signature = mock(MethodSignature.class);
    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);

    when(signature.getMethod()).thenReturn(method);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });

    when(zeta.isLocalHotKey("test::myId")).thenReturn(false);
    when(zeta.peek("test::myId")).thenReturn(Optional.of("cached-value"));

    Object result = aspect.aroundCacheable(pjp, cacheable);

    assertThat(result).isEqualTo("cached-value");
    verify(pjp, never()).proceed();
  }

  @Test
  @DisplayName("@Intercept(force=true) with non-hot key and no @Fallback and peek miss returns null")
  void interceptForceTrueNotHotNoFallback_peekMiss_returnsNull() throws Throwable {
    Method method = TestService.class.getMethod("findInterceptedForceTrueNoFallback", String.class);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    MethodSignature signature = mock(MethodSignature.class);
    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);

    when(signature.getMethod()).thenReturn(method);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });

    when(zeta.isLocalHotKey("test::myId")).thenReturn(false);
    when(zeta.peek("test::myId")).thenReturn(Optional.empty());

    Object result = aspect.aroundCacheable(pjp, cacheable);

    assertThat(result).isNull();
    verify(pjp, never()).proceed();
  }

  @Test
  @DisplayName("@Intercept(force=true) with non-hot key and @Fallback returns fallback value")
  void interceptForceTrueNotHotWithFallback_returnsFallback() throws Throwable {
    Method method = TestService.class.getMethod("findInterceptedForceTrueWithFallback", String.class);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    MethodSignature signature = mock(MethodSignature.class);
    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);

    when(signature.getMethod()).thenReturn(method);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });

    when(zeta.isLocalHotKey("test::myId")).thenReturn(false);

    Object result = aspect.aroundCacheable(pjp, cacheable);

    assertThat(result).isEqualTo("fallback-force-true-myId");
    verify(pjp, never()).proceed();
  }

  @Test
  @DisplayName("context is restored after proceed in finally block")
  void contextRestoredAfterProceed() throws Throwable {
    ZetaCacheContext.get().apply(999L, 888L, true, false, 0L, 0L, false);

    Method method = TestService.class.getMethod("find", String.class);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    MethodSignature signature = mock(MethodSignature.class);
    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);

    when(signature.getMethod()).thenReturn(method);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(pjp.proceed()).thenReturn("result-myId");

    aspect.aroundCacheable(pjp, cacheable);

    assertThat(ZetaCacheContext.get().getHardTtlMs()).isEqualTo(999L);
    assertThat(ZetaCacheContext.get().getSoftTtlMs()).isEqualTo(888L);
    assertThat(ZetaCacheContext.get().isAllowNull()).isTrue();
  }

  @Test
  @DisplayName("context is cleared after proceed when no prior context")
  void contextClearedAfterProceedWhenNoPriorContext() throws Throwable {
    ZetaCacheContext.get().restore(null);

    Method method = TestService.class.getMethod("find", String.class);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    MethodSignature signature = mock(MethodSignature.class);
    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);

    when(signature.getMethod()).thenReturn(method);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(pjp.proceed()).thenReturn("result-myId");

    aspect.aroundCacheable(pjp, cacheable);

    assertThat(ZetaCacheContext.get().snapshot()).isNull();
  }

  // ── Fallback on exception tests ──

  @Test
  @DisplayName("when method throws and @Fallback present, returns fallback")
  void methodThrowsWithFallback_returnsFallback() throws Throwable {
    Method method = TestService.class.getMethod("findThrowing", String.class);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    MethodSignature signature = mock(MethodSignature.class);
    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);

    when(signature.getMethod()).thenReturn(method);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(pjp.proceed()).thenThrow(new RuntimeException("from-method"));

    when(zeta.isLocalHotKey(anyString())).thenReturn(false);

    Object result = aspect.aroundCacheable(pjp, cacheable);

    assertThat(result).isEqualTo("fallback-result");
  }

  @Test
  @DisplayName("when method throws and no @Fallback, rethrows")
  void methodThrowsWithoutFallback_rethrows() throws Throwable {
    Method method = TestService.class.getMethod("findThrowingNoFallback", String.class);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    MethodSignature signature = mock(MethodSignature.class);
    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);

    when(signature.getMethod()).thenReturn(method);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(pjp.proceed()).thenThrow(new RuntimeException("from-method"));

    when(zeta.isLocalHotKey(anyString())).thenReturn(false);

    assertThatThrownBy(() -> aspect.aroundCacheable(pjp, cacheable))
      .isInstanceOf(RuntimeException.class)
      .hasMessage("from-method");
  }

  // ── resolveCacheName tests ──

  @Test
  @DisplayName("resolveCacheName uses cacheNames[0] when available")
  void resolveCacheName_usesCacheNames() throws Throwable {
    Method method = TestService.class.getMethod("find", String.class);
    Cacheable cacheable = mock(Cacheable.class);
    when(cacheable.cacheNames()).thenReturn(new String[] { "fromCacheNames" });
    when(cacheable.value()).thenReturn(new String[] { "fromValue" });
    when(cacheable.key()).thenReturn("");

    MethodSignature signature = mock(MethodSignature.class);
    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);

    when(signature.getMethod()).thenReturn(method);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(pjp.proceed()).thenReturn("ok");

    aspect.aroundCacheable(pjp, cacheable);
  }

  @Test
  @DisplayName("resolveCacheName falls back to value[0] when cacheNames empty")
  void resolveCacheName_fallsBackToValue() throws Throwable {
    Method method = TestService.class.getMethod("find", String.class);
    Cacheable cacheable = mock(Cacheable.class);
    when(cacheable.cacheNames()).thenReturn(new String[0]);
    when(cacheable.value()).thenReturn(new String[] { "fromValue" });
    when(cacheable.key()).thenReturn("");

    MethodSignature signature = mock(MethodSignature.class);
    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);

    when(signature.getMethod()).thenReturn(method);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(pjp.proceed()).thenReturn("ok");

    aspect.aroundCacheable(pjp, cacheable);
  }

  @Test
  @DisplayName("resolveCacheName defaults to zeta when both empty")
  void resolveCacheName_defaultsToHotkey() throws Throwable {
    Method method = TestService.class.getMethod("find", String.class);
    Cacheable cacheable = mock(Cacheable.class);
    when(cacheable.cacheNames()).thenReturn(new String[0]);
    when(cacheable.value()).thenReturn(new String[0]);
    when(cacheable.key()).thenReturn("");

    MethodSignature signature = mock(MethodSignature.class);
    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);

    when(signature.getMethod()).thenReturn(method);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(pjp.proceed()).thenReturn("ok");

    aspect.aroundCacheable(pjp, cacheable);
  }

  // ── resolveKey tests ──

  @Test
  @DisplayName("resolveKey with empty expression and single arg returns arg.toString()")
  void resolveKey_emptyExpressionWithSingleArg() throws Throwable {
    ZetaCacheContext.get().restore(null);

    Cacheable cacheable = mock(Cacheable.class);
    when(cacheable.cacheNames()).thenReturn(new String[] { "test" });
    when(cacheable.value()).thenReturn(new String[0]);
    when(cacheable.key()).thenReturn("");

    MethodSignature signature = mock(MethodSignature.class);
    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);

    when(signature.getMethod()).thenReturn(TestService.class.getMethod("find", String.class));
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(pjp.proceed()).thenReturn("result-myId");

    aspect.aroundCacheable(pjp, cacheable);
  }

  @Test
  @DisplayName("resolveKey with SpEL expression evaluates correctly")
  void resolveKey_withSpelExpression() throws Throwable {
    Method method = TestService.class.getMethod("find", String.class);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    MethodSignature signature = mock(MethodSignature.class);

    when(signature.getMethod()).thenReturn(method);

    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "spelKey" });
    when(pjp.proceed()).thenReturn("result-spelKey");

    aspect.aroundCacheable(pjp, cacheable);
  }

  // ── resolveMethod tests ──

  @Test
  @DisplayName("resolveMethod unwraps interface methods to impl class")
  void resolveMethod_unwrapsInterfaceMethod() throws Throwable {
    Method interfaceMethod = TestInterface.class.getMethod("find", String.class);
    Cacheable cacheable = mock(Cacheable.class);
    when(cacheable.cacheNames()).thenReturn(new String[] { "test" });
    when(cacheable.value()).thenReturn(new String[0]);
    when(cacheable.key()).thenReturn("");

    MethodSignature signature = mock(MethodSignature.class);
    when(signature.getMethod()).thenReturn(interfaceMethod);

    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestInterfaceImpl());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(pjp.proceed()).thenReturn("impl-myId");

    aspect.aroundCacheable(pjp, cacheable);
    verify(pjp).proceed();
  }

  // ── TTL override tests ──

  @Test
  @DisplayName("@CacheTTL applies hardTtlMs override to context")
  void ttlOverride_appliesToContext() throws Throwable {
    ZetaCacheContext.get().restore(null);

    Method method = TestService.class.getMethod("findWithTtl", String.class);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    MethodSignature signature = mock(MethodSignature.class);

    when(signature.getMethod()).thenReturn(method);

    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(pjp.proceed()).thenAnswer(invocation -> {
      assertThat(ZetaCacheContext.get().getHardTtlMs()).isEqualTo(5000L);
      return "result-myId";
    });

    when(zeta.isLocalHotKey(anyString())).thenReturn(false);

    aspect.aroundCacheable(pjp, cacheable);
  }

  // ── @SkipBroadcast tests ──

  @Test
  @DisplayName("@SkipBroadcast on @Cacheable sets skipBroadcast in context")
  void broadcastFalse_onCacheable_setsSkipBroadcast() throws Throwable {
    ZetaCacheContext.get().restore(null);

    Method method = TestService.class.getMethod("findBroadcastOff", String.class);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    MethodSignature signature = mock(MethodSignature.class);

    when(signature.getMethod()).thenReturn(method);

    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(pjp.proceed()).thenAnswer(invocation -> {
      assertThat(ZetaCacheContext.get().isSkipBroadcast()).isTrue();
      return "result-myId";
    });

    when(zeta.isLocalHotKey(anyString())).thenReturn(false);

    aspect.aroundCacheable(pjp, cacheable);
  }

  @Test
  @DisplayName("@SkipBroadcast on @CachePut sets skipBroadcast in context")
  void broadcastFalse_onCachePut_setsSkipBroadcast() throws Throwable {
    ZetaCacheContext.get().restore(null);

    Method method = TestService.class.getMethod("putBroadcastOff", String.class);
    MethodSignature signature = mock(MethodSignature.class);

    when(signature.getMethod()).thenReturn(method);

    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(pjp.proceed()).thenAnswer(invocation -> {
      assertThat(ZetaCacheContext.get().isSkipBroadcast()).isTrue();
      return "result-myId";
    });

    aspect.aroundCachePutOrEvict(pjp);
  }

  @Test
  @DisplayName("@SkipBroadcast on @CacheEvict sets skipBroadcast in context")
  void broadcastFalse_onCacheEvict_setsSkipBroadcast() throws Throwable {
    ZetaCacheContext.get().restore(null);

    Method method = TestService.class.getMethod("evictBroadcastOff", String.class);
    MethodSignature signature = mock(MethodSignature.class);

    when(signature.getMethod()).thenReturn(method);

    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(pjp.proceed()).thenAnswer(invocation -> {
      assertThat(ZetaCacheContext.get().isSkipBroadcast()).isTrue();
      return null;
    });

    aspect.aroundCachePutOrEvict(pjp);
  }

  @Test
  @DisplayName("without @SkipBroadcast on @CachePut does not set skipBroadcast")
  void broadcastDefault_onCachePut_doesNotSkip() throws Throwable {
    ZetaCacheContext.get().restore(null);

    Method method = TestService.class.getMethod("putBroadcastDefault", String.class);
    MethodSignature signature = mock(MethodSignature.class);

    when(signature.getMethod()).thenReturn(method);

    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(pjp.proceed()).thenAnswer(invocation -> {
      assertThat(ZetaCacheContext.get().isSkipBroadcast()).isFalse();
      return "result-myId";
    });

    aspect.aroundCachePutOrEvict(pjp);
  }

  // ── NullCaching tests ──

  @Test
  @DisplayName("@NullCaching(true) sets allowNull in context")
  void nullCaching_appliesAllowNull() throws Throwable {
    ZetaCacheContext.get().restore(null);

    Method method = TestService.class.getMethod("findWithNullCaching", String.class);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    MethodSignature signature = mock(MethodSignature.class);

    when(signature.getMethod()).thenReturn(method);

    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(pjp.proceed()).thenAnswer(invocation -> {
      assertThat(ZetaCacheContext.get().isAllowNull()).isTrue();
      return "result-myId";
    });

    when(zeta.isLocalHotKey(anyString())).thenReturn(false);

    aspect.aroundCacheable(pjp, cacheable);
  }

  // ── resolveKey: empty expression with 0 args ──

  @Test
  @DisplayName("resolveKey with empty expression and 0 args returns 'empty'")
  void resolveKey_emptyExpressionWithNoArgs() throws Throwable {
    Method method = TestService.class.getMethod("find", String.class);
    Cacheable cacheable = mock(Cacheable.class);
    when(cacheable.cacheNames()).thenReturn(new String[] { "test" });
    when(cacheable.value()).thenReturn(new String[0]);
    when(cacheable.key()).thenReturn("");

    MethodSignature signature = mock(MethodSignature.class);
    when(signature.getMethod()).thenReturn(method);

    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[0]);
    when(pjp.proceed()).thenReturn("ok");

    aspect.aroundCacheable(pjp, cacheable);
    verify(pjp).proceed();
  }

  @Test
  @DisplayName("resolveKey with empty expression and multiple args returns SimpleKey")
  void resolveKey_emptyExpressionWithMultipleArgs() throws Throwable {
    Method method = TestService.class.getMethod("find", String.class);
    Cacheable cacheable = mock(Cacheable.class);
    when(cacheable.cacheNames()).thenReturn(new String[] { "test" });
    when(cacheable.value()).thenReturn(new String[0]);
    when(cacheable.key()).thenReturn("");

    MethodSignature signature = mock(MethodSignature.class);
    when(signature.getMethod()).thenReturn(method);

    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "a", "b" });
    when(pjp.proceed()).thenReturn("ok");

    aspect.aroundCacheable(pjp, cacheable);
    verify(pjp).proceed();
  }

  @Test
  @DisplayName("resolveKey with empty expression and single null arg returns SimpleKey")
  void resolveKey_emptyExpressionWithSingleNullArg() throws Throwable {
    Method method = TestService.class.getMethod("find", String.class);
    Cacheable cacheable = mock(Cacheable.class);
    when(cacheable.cacheNames()).thenReturn(new String[] { "test" });
    when(cacheable.value()).thenReturn(new String[0]);
    when(cacheable.key()).thenReturn("");

    MethodSignature signature = mock(MethodSignature.class);
    when(signature.getMethod()).thenReturn(method);

    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { null });
    when(pjp.proceed()).thenReturn("ok");

    aspect.aroundCacheable(pjp, cacheable);
    verify(pjp).proceed();
  }

  @Test
  @DisplayName("resolveKey with empty expression and single array arg returns SimpleKey")
  void resolveKey_emptyExpressionWithArrayArg() throws Throwable {
    Method method = TestService.class.getMethod("find", String.class);
    Cacheable cacheable = mock(Cacheable.class);
    when(cacheable.cacheNames()).thenReturn(new String[] { "test" });
    when(cacheable.value()).thenReturn(new String[0]);
    when(cacheable.key()).thenReturn("");

    MethodSignature signature = mock(MethodSignature.class);
    when(signature.getMethod()).thenReturn(method);

    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { new String[] { "x" } });
    when(pjp.proceed()).thenReturn("ok");

    aspect.aroundCacheable(pjp, cacheable);
    verify(pjp).proceed();
  }

  // ── resolveMethod: interface method with NoSuchMethodException ──

  @Test
  @DisplayName("resolveMethod keeps interface method when getMethod throws NoSuchMethodException")
  void resolveMethod_keepsInterfaceMethodWhenNoSuchMethod() throws Throwable {
    Method interfaceMethod = TestInterface.class.getMethod("find", String.class);
    Cacheable cacheable = mock(Cacheable.class);
    when(cacheable.cacheNames()).thenReturn(new String[] { "test" });
    when(cacheable.value()).thenReturn(new String[0]);
    when(cacheable.key()).thenReturn("");

    MethodSignature signature = mock(MethodSignature.class);
    when(signature.getMethod()).thenReturn(interfaceMethod);

    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new Object());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(pjp.proceed()).thenReturn("result-myId");

    aspect.aroundCacheable(pjp, cacheable);
    verify(pjp).proceed();
  }

  // ── resolveFallback with non-empty SpEL expression ──

  @Test
  @DisplayName("@Intercept with hot key and @Fallback with SpEL expression returns evaluated fallback")
  void interceptWithZetaAndSpelFallback_returnsFallback() throws Throwable {
    Method method = SpelFallbackService.class.getMethod("find", String.class);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    MethodSignature signature = mock(MethodSignature.class);

    when(signature.getMethod()).thenReturn(method);

    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new SpelFallbackService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });

    when(zeta.isLocalHotKey("test::myId")).thenReturn(true);

    Object result = aspect.aroundCacheable(pjp, cacheable);

    assertThat(result).isEqualTo("spel-fallback-value");
    verify(pjp, never()).proceed();
  }

  // ── invokeFallbackMethod when null ──

  @Test
  @DisplayName("when method throws and @Fallback has no matching method, returns null")
  void methodThrowsWithFallbackButNoMethod_returnsNull() throws Throwable {
    Method method = TestService.class.getMethod("findThrowingNoFallbackMethod", String.class);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    MethodSignature signature = mock(MethodSignature.class);

    when(signature.getMethod()).thenReturn(method);

    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(pjp.proceed()).thenThrow(new RuntimeException("error"));

    when(zeta.isLocalHotKey(anyString())).thenReturn(false);

    Object result = aspect.aroundCacheable(pjp, cacheable);

    assertThat(result).isNull();
  }

  // ── findFallbackMethod superclass search ──

  @Test
  @DisplayName("findFallbackMethod searches superclass when method not found on target class")
  void fallbackMethod_searchesSuperclass() throws Throwable {
    Method method = DerivedFallbackService.class.getMethod("find", String.class);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    MethodSignature signature = mock(MethodSignature.class);

    when(signature.getMethod()).thenReturn(method);

    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new DerivedFallbackService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(pjp.proceed()).thenThrow(new RuntimeException("error"));

    when(zeta.isLocalHotKey(anyString())).thenReturn(false);

    Object result = aspect.aroundCacheable(pjp, cacheable);

    assertThat(result).isEqualTo("base-fallback-myId");
  }

  // ── InvocationTargetException from fallback method ──

  @Test
  @DisplayName("when fallback method itself throws, InvocationTargetException is unwrapped and propagated")
  void fallbackMethodThrowing_throwsUnwrappedCause() throws Throwable {
    Method method = TestService.class.getMethod("findThrowingFallbackMethod", String.class);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    MethodSignature signature = mock(MethodSignature.class);

    when(signature.getMethod()).thenReturn(method);

    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(pjp.proceed()).thenThrow(new RuntimeException("original-error"));

    when(zeta.isLocalHotKey(anyString())).thenReturn(false);

    assertThatThrownBy(() -> aspect.aroundCacheable(pjp, cacheable))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("fallback-threw");
  }

  // ── @Intercept(type = qps) tests ──

  @Test
  @DisplayName("@Intercept(qps=5) below threshold proceeds normally")
  void qpsBelowThreshold_proceedsNormally() throws Throwable {
    Method method = TestService.class.getMethod("findQpsIntercepted", String.class);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    MethodSignature signature = mock(MethodSignature.class);

    when(signature.getMethod()).thenReturn(method);

    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(pjp.proceed()).thenReturn("result-myId");

    when(zeta.isLocalHotKey(anyString())).thenReturn(false);

    Object result = aspect.aroundCacheable(pjp, cacheable);

    assertThat(result).isEqualTo("result-myId");
  }

  @Test
  @DisplayName("@Intercept(qps=5) above threshold returns null (no fallback, no @Fallback)")
  void qpsAboveThresholdNoFallback_returnsNull() throws Throwable {
    Method method = TestService.class.getMethod("findQpsIntercepted", String.class);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    MethodSignature signature = mock(MethodSignature.class);

    when(signature.getMethod()).thenReturn(method);

    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(pjp.proceed()).thenReturn("result-myId");

    when(zeta.isLocalHotKey(anyString())).thenReturn(false);
    when(zeta.peek(anyString())).thenReturn(Optional.empty());

    // Exceed qps by calling 6 times (threshold = 5)
    Object ignored;
    for (int i = 0; i < 5; i++) {
      ignored = aspect.aroundCacheable(pjp, cacheable);
    }
    Object result = aspect.aroundCacheable(pjp, cacheable);

    assertThat(result).isNull();
  }

  @Test
  @DisplayName("@Intercept(qps=5) above threshold with SpEL fallback returns fallback")
  void qpsAboveThresholdWithSpelFallback_returnsFallback() throws Throwable {
    Method method = TestService.class.getMethod("findQpsInterceptedWithSpelFallback", String.class);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    MethodSignature signature = mock(MethodSignature.class);

    when(signature.getMethod()).thenReturn(method);

    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(pjp.proceed()).thenReturn("result-myId");

    when(zeta.isLocalHotKey(anyString())).thenReturn(false);

    // Exceed qps by calling 6 times (threshold = 5)
    Object ignored;
    for (int i = 0; i < 5; i++) {
      ignored = aspect.aroundCacheable(pjp, cacheable);
    }
    Object result = aspect.aroundCacheable(pjp, cacheable);

    assertThat(result).isEqualTo("qps-fallback");
  }

  // ── @Intercept(CONCURRENT_THREADS) tests ──

  @Test
  @DisplayName("@Intercept(CONCURRENT_THREADS=2) allows 2 through, intercepts 3rd")
  void concurrentThreads_guardsCorrectly() throws Throwable {
    Method method = TestService.class.getMethod("findConcurrentThreadsIntercepted", String.class);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    MethodSignature signature = mock(MethodSignature.class);
    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);

    when(signature.getMethod()).thenReturn(method);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(zeta.isLocalHotKey(anyString())).thenReturn(false);
    when(zeta.peek(anyString())).thenReturn(Optional.of("cached-value"));

    CountDownLatch enterLatch = new CountDownLatch(2);
    CountDownLatch blockLatch = new CountDownLatch(1);

    when(pjp.proceed()).thenAnswer(invocation -> {
      enterLatch.countDown();
      blockLatch.await(5, TimeUnit.SECONDS);
      return "result-myId";
    });

    ExecutorService exec = Executors.newFixedThreadPool(3);
    List<Future<Object>> futures = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      futures.add(
        exec.submit(() -> {
          try {
            return aspect.aroundCacheable(pjp, cacheable);
          } catch (Throwable e) {
            throw new RuntimeException(e);
          }
        })
      );
    }

    assertThat(enterLatch.await(5, TimeUnit.SECONDS)).isTrue();

    // Small window for the 3rd thread to hit the atomic guard and be intercepted
    Thread.sleep(100);

    blockLatch.countDown();
    exec.shutdown();
    exec.awaitTermination(5, TimeUnit.SECONDS);

    verify(pjp, times(2)).proceed();
  }

  // ── @Preload tests ──

  @Test
  @DisplayName("@Preload(keys={...}) calls notifyLocalDetectorDirect for static keys")
  void staticPreload_inflatesDetector() throws Throwable {
    Method method = TestService.class.getMethod("findWithStaticPreload", String.class);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    MethodSignature signature = mock(MethodSignature.class);

    when(signature.getMethod()).thenReturn(method);

    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(pjp.proceed()).thenReturn("result-myId");

    when(zeta.isLocalHotKey(anyString())).thenReturn(false);

    aspect.aroundCacheable(pjp, cacheable);

    verify(zeta).notifyLocalDetectorDirect(
      Map.of("test::preload-key-a", Long.MAX_VALUE, "test::preload-key-b", Long.MAX_VALUE)
    );
  }

  @Test
  @DisplayName("@Preload(keyExpr=\"#id\") calls notifyLocalDetectorDirect for dynamic key")
  void dynamicPreload_inflatesDetector() throws Throwable {
    Method method = TestService.class.getMethod("findWithDynamicPreload", String.class);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    MethodSignature signature = mock(MethodSignature.class);

    when(signature.getMethod()).thenReturn(method);

    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myDynamicKey" });
    when(pjp.proceed()).thenReturn("result-myDynamicKey");

    when(zeta.isLocalHotKey(anyString())).thenReturn(false);

    aspect.aroundCacheable(pjp, cacheable);

    verify(zeta).notifyLocalDetectorDirect("test::myDynamicKey", Long.MAX_VALUE);
  }

  @Test
  @DisplayName("@Preload static keys are inflated only once")
  void staticPreload_inflatesOnce() throws Throwable {
    Method method = TestService.class.getMethod("findWithStaticPreload", String.class);
    Cacheable cacheable = method.getAnnotation(Cacheable.class);
    MethodSignature signature = mock(MethodSignature.class);

    when(signature.getMethod()).thenReturn(method);

    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    when(pjp.getSignature()).thenReturn(signature);
    when(pjp.getTarget()).thenReturn(new TestService());
    when(pjp.getArgs()).thenReturn(new Object[] { "myId" });
    when(pjp.proceed()).thenReturn("result-myId");

    when(zeta.isLocalHotKey(anyString())).thenReturn(false);

    // Call twice — notifyLocalDetectorDirect should only be called once per key
    aspect.aroundCacheable(pjp, cacheable);
    aspect.aroundCacheable(pjp, cacheable);

    verify(zeta, times(1)).notifyLocalDetectorDirect(
      Map.of("test::preload-key-a", Long.MAX_VALUE, "test::preload-key-b", Long.MAX_VALUE)
    );
  }
}
