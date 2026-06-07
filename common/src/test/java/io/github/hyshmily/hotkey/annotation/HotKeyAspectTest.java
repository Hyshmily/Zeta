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
package io.github.hyshmily.hotkey.annotation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.hyshmily.hotkey.annotation.HotKey.OperationType;
import io.github.hyshmily.hotkey.exception.HotKeyBlockedException;
import java.lang.reflect.Method;
import io.github.hyshmily.hotkey.rule.Rule.RuleAction;
import java.util.Optional;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link HotKeyAspect} covering the full priority chain:
 * Blacklist → Condition → Intercept → TTL → Cache → Unless.
 */
class HotKeyAspectTest {

  private io.github.hyshmily.hotkey.HotKey hotKeyFacade;
  private HotKeyAspect aspect;

  @BeforeEach
  void setUp() {
    hotKeyFacade = mock(io.github.hyshmily.hotkey.HotKey.class);
    aspect = new HotKeyAspect(hotKeyFacade);
  }

  // ── Annotated target methods (real annotations for reflection) ─────

  @SuppressWarnings("unused")
  @HotKey(key = "'staticKey'")
  public String sampleMethod(String arg) {
    return arg;
  }

  @SuppressWarnings("unused")
  @HotKey(key = "'staticKey'")
  @Fallback("'fallbackVal'")
  public String sampleWithFallback(String arg) {
    return arg;
  }

  @SuppressWarnings("unused")
  @HotKey(key = "'staticKey'")
  @Fallback
  public String sampleWithFallbackNaming(String arg) {
    return arg;
  }

  @SuppressWarnings("unused")
  public String sampleWithFallbackNamingFallback(String arg) {
    return "naming-convention-fb";
  }

  @SuppressWarnings("unused")
  @HotKey(key = "'staticKey'")
  @Fallback("'fallbackVal'")
  @Intercept
  public String sampleInterceptedWithFallback(String arg) {
    return arg;
  }

  @SuppressWarnings("unused")
  @HotKey(key = "'staticKey'")
  @Intercept
  public String sampleInterceptedNoFallback(String arg) {
    return arg;
  }

  @SuppressWarnings("unused")
  @HotKey(key = "'staticKey'")
  @Fallback("'ttlVal'")
  @Intercept
  @HotKeyCacheTTL(hardTtlMs = 5000, softTtlMs = 60000)
  public String sampleWithFullAnnotations(String arg) {
    return arg;
  }

  @SuppressWarnings("unused")
  @HotKey(key = "'staticKey'", condition = "#arg != null")
  public String sampleWithCondition(String arg) {
    return "executed-" + arg;
  }

  @SuppressWarnings("unused")
  @HotKey(key = "'staticKey'", unless = "#result == null")
  public String sampleWithUnless(String arg) {
    return arg;
  }

  @SuppressWarnings("unused")
  @HotKey(key = "'staticKey'")
  @Fallback("'hotFallback'")
  public String sampleWithFallbackNoInterceptHot(String arg) {
    return arg;
  }

  @SuppressWarnings("unused")
  @HotKey(key = "'staticKey'")
  @HotKeyCacheTTL(hardTtlMs = 7777, softTtlMs = 8888)
  public String sampleWithTtl(String arg) {
    return arg;
  }

  // ── Helper ─────────────────────────────────────────────────────────

  private ProceedingJoinPoint mockPjp(String methodName, Object[] args) throws Exception {
    Method method = getClass().getDeclaredMethod(methodName, String.class);
    MethodSignature sig = mock(MethodSignature.class);
    when(sig.getMethod()).thenReturn(method);

    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    when(pjp.getSignature()).thenReturn(sig);
    when(pjp.getArgs()).thenReturn(args);
    when(pjp.getTarget()).thenReturn(this);
    return pjp;
  }

  private HotKey defaultAnn() {
    HotKey ann = mock(HotKey.class);
    when(ann.operation()).thenReturn(OperationType.READ);
    when(ann.softExpire()).thenReturn(true);
    when(ann.condition()).thenReturn("");
    when(ann.unless()).thenReturn("");
    return ann;
  }

  // ── READ: basic path ───────────────────────────────────────────────

  @Test
  void read_shouldUseGetWithSoftExpireByDefault() throws Throwable {
    ProceedingJoinPoint pjp = mockPjp("sampleMethod", new Object[]{"testArg"});
    HotKey ann = defaultAnn();
    when(ann.key()).thenReturn("'staticKey'");

    when(hotKeyFacade.evaluateRule("staticKey")).thenReturn(RuleAction.ALLOW);
    when(hotKeyFacade.isLocalHotKey("staticKey")).thenReturn(false);
    when(hotKeyFacade.getWithSoftExpire(anyString(), any(), anyLong(), anyLong()))
      .thenReturn(Optional.of("cached"));

    Object result = aspect.around(pjp, ann);

    assertThat(result).isEqualTo("cached");
    verify(hotKeyFacade).getWithSoftExpire(anyString(), any(), anyLong(), anyLong());
  }

  @Test
  void read_withSoftExpireFalse_shouldUseGet() throws Throwable {
    ProceedingJoinPoint pjp = mockPjp("sampleMethod", new Object[]{"testArg"});
    HotKey ann = defaultAnn();
    when(ann.key()).thenReturn("'staticKey'");
    when(ann.softExpire()).thenReturn(false);

    when(hotKeyFacade.evaluateRule("staticKey")).thenReturn(RuleAction.ALLOW);
    when(hotKeyFacade.isLocalHotKey("staticKey")).thenReturn(false);
    when(hotKeyFacade.get(anyString(), any(), anyLong(), anyLong()))
      .thenReturn(Optional.of("cached"));

    assertThat(aspect.around(pjp, ann)).isEqualTo("cached");
    verify(hotKeyFacade).get(anyString(), any(), anyLong(), anyLong());
  }

  // ── Step ①: Blacklist ──────────────────────────────────────────────

  @Test
  void blacklist_withoutFallback_shouldThrowBlockedException() throws Throwable {
    ProceedingJoinPoint pjp = mockPjp("sampleMethod", new Object[]{"testArg"});
    HotKey ann = defaultAnn();
    when(ann.key()).thenReturn("'staticKey'");

    when(hotKeyFacade.evaluateRule("staticKey")).thenReturn(RuleAction.BLOCK);

    assertThatThrownBy(() -> aspect.around(pjp, ann))
      .isInstanceOf(HotKeyBlockedException.class)
      .hasMessageContaining("staticKey");
    verify(pjp, never()).proceed();
  }

  @Test
  void blacklist_withFallback_shouldReturnFallbackValue() throws Throwable {
    ProceedingJoinPoint pjp = mockPjp("sampleWithFallback", new Object[]{"testArg"});
    HotKey ann = defaultAnn();
    when(ann.key()).thenReturn("'staticKey'");

    when(hotKeyFacade.evaluateRule("staticKey")).thenReturn(RuleAction.BLOCK);

    Object result = aspect.around(pjp, ann);
    assertThat(result).isEqualTo("fallbackVal");
    verify(pjp, never()).proceed();
  }

  // ── Step ②: Condition ──────────────────────────────────────────────

  @Test
  void conditionFalse_shouldSkipCacheAndExecuteMethod() throws Throwable {
    ProceedingJoinPoint pjp = mockPjp("sampleWithCondition", new Object[]{"testArg"});
    HotKey ann = defaultAnn();
    when(ann.key()).thenReturn("'staticKey'");

    // The method has condition="#arg != null", but we override the annotation mock
    when(ann.condition()).thenReturn("false");

    when(pjp.proceed()).thenReturn("direct-exec");

    Object result = aspect.around(pjp, ann);

    assertThat(result).isEqualTo("direct-exec");
    verify(pjp).proceed();
    verify(hotKeyFacade, never()).getWithSoftExpire(anyString(), any(), anyLong(), anyLong());
  }

  @Test
  void conditionTrue_shouldGoThroughCache() throws Throwable {
    ProceedingJoinPoint pjp = mockPjp("sampleWithCondition", new Object[]{"testArg"});
    HotKey ann = defaultAnn();
    when(ann.key()).thenReturn("'staticKey'");
    when(ann.condition()).thenReturn("true");

    when(hotKeyFacade.evaluateRule("staticKey")).thenReturn(RuleAction.ALLOW);
    when(hotKeyFacade.isLocalHotKey("staticKey")).thenReturn(false);
    when(hotKeyFacade.getWithSoftExpire(anyString(), any(), anyLong(), anyLong()))
      .thenReturn(Optional.of("cached"));

    assertThat(aspect.around(pjp, ann)).isEqualTo("cached");
  }

  // ── Step ③: Intercept ──────────────────────────────────────────────

  @Test
  void intercept_hotKey_withFallback_shouldReturnFallbackValue() throws Throwable {
    ProceedingJoinPoint pjp = mockPjp("sampleInterceptedWithFallback", new Object[]{"testArg"});
    HotKey ann = defaultAnn();
    when(ann.key()).thenReturn("'staticKey'");

    when(hotKeyFacade.evaluateRule("staticKey")).thenReturn(RuleAction.ALLOW);
    when(hotKeyFacade.isLocalHotKey("staticKey")).thenReturn(true);

    Object result = aspect.around(pjp, ann);

    assertThat(result).isEqualTo("fallbackVal");
    verify(pjp, never()).proceed();
  }

  @Test
  void intercept_hotKey_withoutFallback_shouldPeek() throws Throwable {
    ProceedingJoinPoint pjp = mockPjp("sampleInterceptedNoFallback", new Object[]{"testArg"});
    HotKey ann = defaultAnn();
    when(ann.key()).thenReturn("'staticKey'");

    when(hotKeyFacade.evaluateRule("staticKey")).thenReturn(RuleAction.ALLOW);
    when(hotKeyFacade.isLocalHotKey("staticKey")).thenReturn(true);
    when(hotKeyFacade.peek("staticKey")).thenReturn(Optional.of("peeked"));

    Object result = aspect.around(pjp, ann);

    assertThat(result).isEqualTo("peeked");
    verify(pjp, never()).proceed();
    verify(hotKeyFacade).peek("staticKey");
  }

  @Test
  void intercept_hotKey_withoutFallback_peekEmpty_shouldReturnNull() throws Throwable {
    ProceedingJoinPoint pjp = mockPjp("sampleInterceptedNoFallback", new Object[]{"testArg"});
    HotKey ann = defaultAnn();
    when(ann.key()).thenReturn("'staticKey'");

    when(hotKeyFacade.evaluateRule("staticKey")).thenReturn(RuleAction.ALLOW);
    when(hotKeyFacade.isLocalHotKey("staticKey")).thenReturn(true);
    when(hotKeyFacade.peek("staticKey")).thenReturn(Optional.empty());

    Object result = aspect.around(pjp, ann);

    assertThat(result).isNull();
    verify(pjp, never()).proceed();
  }

  @Test
  void intercept_notHot_shouldGoThroughCache() throws Throwable {
    ProceedingJoinPoint pjp = mockPjp("sampleInterceptedNoFallback", new Object[]{"testArg"});
    HotKey ann = defaultAnn();
    when(ann.key()).thenReturn("'staticKey'");

    when(hotKeyFacade.evaluateRule("staticKey")).thenReturn(RuleAction.ALLOW);
    when(hotKeyFacade.isLocalHotKey("staticKey")).thenReturn(false);
    when(hotKeyFacade.getWithSoftExpire(anyString(), any(), anyLong(), anyLong()))
      .thenReturn(Optional.of("cached"));

    assertThat(aspect.around(pjp, ann)).isEqualTo("cached");
  }

  // ── @Fallback alone should NOT imply @Intercept ────────────────────

  @Test
  void fallbackAlone_withHotKey_shouldNotIntercept() throws Throwable {
    ProceedingJoinPoint pjp = mockPjp("sampleWithFallback", new Object[]{"testArg"});
    HotKey ann = defaultAnn();
    when(ann.key()).thenReturn("'staticKey'");

    // isLocalHotKey is true, but since there's no @Intercept, the method should NOT be skipped
    when(hotKeyFacade.evaluateRule("staticKey")).thenReturn(RuleAction.ALLOW);
    when(hotKeyFacade.isLocalHotKey("staticKey")).thenReturn(true);
    when(hotKeyFacade.getWithSoftExpire(anyString(), any(), anyLong(), anyLong()))
      .thenReturn(Optional.of("cached"));

    Object result = aspect.around(pjp, ann);

    // Should go through cache, not fallback
    assertThat(result).isEqualTo("cached");
    verify(hotKeyFacade).getWithSoftExpire(anyString(), any(), anyLong(), anyLong());
  }

  // ── Step ④: TTL override ──────────────────────────────────────────

  @Test
  void ttlAnnotation_shouldOverrideTimer() throws Throwable {
    ProceedingJoinPoint pjp = mockPjp("sampleWithTtl", new Object[]{"testArg"});
    HotKey ann = defaultAnn();
    when(ann.key()).thenReturn("'staticKey'");

    when(hotKeyFacade.evaluateRule("staticKey")).thenReturn(RuleAction.ALLOW);
    when(hotKeyFacade.isLocalHotKey("staticKey")).thenReturn(false);
    when(hotKeyFacade.getWithSoftExpire(eq("staticKey"), any(), eq(7777L), eq(8888L)))
      .thenReturn(Optional.of("ttl-cached"));

    assertThat(aspect.around(pjp, ann)).isEqualTo("ttl-cached");
    verify(hotKeyFacade).getWithSoftExpire(eq("staticKey"), any(), eq(7777L), eq(8888L));
  }

  // ── Step ⑤: Cache exception ───────────────────────────────────────

  @Test
  void cacheException_withFallback_shouldReturnFallback() throws Throwable {
    ProceedingJoinPoint pjp = mockPjp("sampleWithFallback", new Object[]{"testArg"});
    HotKey ann = defaultAnn();
    when(ann.key()).thenReturn("'staticKey'");

    when(hotKeyFacade.evaluateRule("staticKey")).thenReturn(RuleAction.ALLOW);
    when(hotKeyFacade.isLocalHotKey("staticKey")).thenReturn(false);
    when(hotKeyFacade.getWithSoftExpire(anyString(), any(), anyLong(), anyLong()))
      .thenThrow(new RuntimeException("redis down"));

    Object result = aspect.around(pjp, ann);

    assertThat(result).isEqualTo("fallbackVal");
  }

  @Test
  void cacheException_withoutFallback_shouldRethrow() throws Throwable {
    ProceedingJoinPoint pjp = mockPjp("sampleMethod", new Object[]{"testArg"});
    HotKey ann = defaultAnn();
    when(ann.key()).thenReturn("'staticKey'");

    when(hotKeyFacade.evaluateRule("staticKey")).thenReturn(RuleAction.ALLOW);
    when(hotKeyFacade.isLocalHotKey("staticKey")).thenReturn(false);
    when(hotKeyFacade.getWithSoftExpire(anyString(), any(), anyLong(), anyLong()))
      .thenThrow(new RuntimeException("redis down"));

    assertThatThrownBy(() -> aspect.around(pjp, ann))
      .isInstanceOf(RuntimeException.class)
      .hasMessageContaining("redis down");
  }

  // ── Step ⑥: Unless ─────────────────────────────────────────────────

  @Test
  void unless_shouldEvaluateButNotPreventCaching() throws Throwable {
    ProceedingJoinPoint pjp = mockPjp("sampleWithUnless", new Object[]{"testArg"});
    HotKey ann = defaultAnn();
    when(ann.key()).thenReturn("'staticKey'");
    when(ann.unless()).thenReturn("true");

    when(hotKeyFacade.evaluateRule("staticKey")).thenReturn(RuleAction.ALLOW);
    when(hotKeyFacade.isLocalHotKey("staticKey")).thenReturn(false);
    when(hotKeyFacade.getWithSoftExpire(anyString(), any(), anyLong(), anyLong()))
      .thenReturn(Optional.of("cached"));

    // unless=true — value is already cached, user accepts this
    Object result = aspect.around(pjp, ann);

    assertThat(result).isEqualTo("cached");
    verify(hotKeyFacade).getWithSoftExpire(anyString(), any(), anyLong(), anyLong());
  }

  // ── Fallback naming convention ─────────────────────────────────────

  @Test
  void fallbackNamingConvention_shouldInvokeMethod() throws Throwable {
    ProceedingJoinPoint pjp = mockPjp("sampleWithFallbackNaming", new Object[]{"testArg"});
    HotKey ann = defaultAnn();
    when(ann.key()).thenReturn("'staticKey'");

    when(hotKeyFacade.evaluateRule("staticKey")).thenReturn(RuleAction.BLOCK);

    Object result = aspect.around(pjp, ann);

    assertThat(result).isEqualTo("naming-convention-fb");
  }

  // ── WRITE ──────────────────────────────────────────────────────────

  @Test
  void write_shouldUsePutBeforeInvalidate() throws Throwable {
    ProceedingJoinPoint pjp = mockPjp("sampleMethod", new Object[]{"testArg"});
    HotKey ann = defaultAnn();
    when(ann.key()).thenReturn("'writeKey'");
    when(ann.operation()).thenReturn(OperationType.WRITE);

    doAnswer(invocation -> {
      Runnable action = invocation.getArgument(1);
      action.run();
      return null;
    }).when(hotKeyFacade).putBeforeInvalidate(anyString(), any());

    when(pjp.proceed()).thenReturn("writeResult");

    Object result = aspect.around(pjp, ann);
    assertThat(result).isEqualTo("writeResult");
    verify(hotKeyFacade).putBeforeInvalidate(eq("writeKey"), any());
  }

  // ── INVALIDATE ─────────────────────────────────────────────────────

  @Test
  void invalidate_shouldCallInvalidateAndProceed() throws Throwable {
    ProceedingJoinPoint pjp = mockPjp("sampleMethod", new Object[]{"testArg"});
    HotKey ann = defaultAnn();
    when(ann.key()).thenReturn("'invalidateKey'");
    when(ann.operation()).thenReturn(OperationType.INVALIDATE);
    when(pjp.proceed()).thenReturn("afterInvalidate");

    Object result = aspect.around(pjp, ann);

    assertThat(result).isEqualTo("afterInvalidate");
    verify(hotKeyFacade).invalidate("invalidateKey");
  }

  // ── Full priority chain ────────────────────────────────────────────

  @Test
  void fullChain_allStepsPass_shouldReturnCachedValue() throws Throwable {
    ProceedingJoinPoint pjp = mockPjp("sampleWithFullAnnotations", new Object[]{"testArg"});
    HotKey ann = defaultAnn();
    when(ann.key()).thenReturn("'staticKey'");
    when(ann.condition()).thenReturn("true");
    when(ann.unless()).thenReturn("false");

    // ① Blacklist: ALLOW (pass)
    when(hotKeyFacade.evaluateRule("staticKey")).thenReturn(RuleAction.ALLOW);

    // ② Condition: true (pass)
    // ③ Intercept: not hot (pass through)
    when(hotKeyFacade.isLocalHotKey("staticKey")).thenReturn(false);

    // ④ TTL: @HotKeyCacheTTL(hard=5000, soft=60000)
    // ⑤ Cache: success
    when(hotKeyFacade.getWithSoftExpire(eq("staticKey"), any(), eq(5000L), eq(60000L)))
      .thenReturn(Optional.of("chain-result"));

    // ⑥ Unless: false (pass)

    Object result = aspect.around(pjp, ann);

    assertThat(result).isEqualTo("chain-result");
    verify(hotKeyFacade).getWithSoftExpire(eq("staticKey"), any(), eq(5000L), eq(60000L));
  }

  @Test
  void fullChain_blacklistBlocked_shouldReturnFallback() throws Throwable {
    ProceedingJoinPoint pjp = mockPjp("sampleWithFullAnnotations", new Object[]{"testArg"});
    HotKey ann = defaultAnn();
    when(ann.key()).thenReturn("'staticKey'");

    when(hotKeyFacade.evaluateRule("staticKey")).thenReturn(RuleAction.BLOCK);

    Object result = aspect.around(pjp, ann);

    assertThat(result).isEqualTo("ttlVal");
    verify(pjp, never()).proceed();
  }

  @Test
  void fullChain_intercepted_shouldReturnFallback() throws Throwable {
    ProceedingJoinPoint pjp = mockPjp("sampleWithFullAnnotations", new Object[]{"testArg"});
    HotKey ann = defaultAnn();
    when(ann.key()).thenReturn("'staticKey'");

    when(hotKeyFacade.evaluateRule("staticKey")).thenReturn(RuleAction.ALLOW);
    when(hotKeyFacade.isLocalHotKey("staticKey")).thenReturn(true);

    Object result = aspect.around(pjp, ann);

    assertThat(result).isEqualTo("ttlVal");
    verify(pjp, never()).proceed();
  }

  // ── Optional return type ───────────────────────────────────────────

  @Test
  void read_optionalReturnType_shouldPassThrough() throws Throwable {
    Method signatureMethod = getClass().getDeclaredMethod("optionalReturnMethod");
    MethodSignature sig = mock(MethodSignature.class);
    when(sig.getMethod()).thenReturn(signatureMethod);

    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    when(pjp.getSignature()).thenReturn(sig);
    when(pjp.getArgs()).thenReturn(new Object[] {});
    when(pjp.getTarget()).thenReturn(this);

    HotKey ann = defaultAnn();
    when(ann.key()).thenReturn("'optKey'");

    when(hotKeyFacade.evaluateRule("optKey")).thenReturn(RuleAction.ALLOW);
    when(hotKeyFacade.isLocalHotKey("optKey")).thenReturn(false);

    // Single-wrapped: the facade returns Optional<Object>, and handleRead returns it
    // directly for Optional return types — the result is already Optional<String>.
    when(hotKeyFacade.getWithSoftExpire(anyString(), any(), anyLong(), anyLong()))
      .thenReturn(Optional.of("optional-wrapped"));

    @SuppressWarnings("unchecked")
    Optional<String> result = (Optional<String>) aspect.around(pjp, ann);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo("optional-wrapped");
  }

  @SuppressWarnings("unused")
  @HotKey(key = "'optKey'")
  public Optional<String> optionalReturnMethod() {
    return Optional.of("from-method");
  }
}
