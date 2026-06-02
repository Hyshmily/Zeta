package io.github.hyshmily.hotkey.annotation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.hyshmily.hotkey.annotation.HotKey.OperationType;
import java.util.Optional;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HotKeyAspectTest {

  private io.github.hyshmily.hotkey.HotKey hotKeyFacade;
  private HotKeyAspect aspect;

  @BeforeEach
  void setUp() {
    hotKeyFacade = mock(io.github.hyshmily.hotkey.HotKey.class);
    aspect = new HotKeyAspect(hotKeyFacade);
  }

  @Test
  void around_read_shouldUseGetWithSoftExpire() throws Throwable {
    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    HotKey ann = mock(HotKey.class);
    MethodSignature sig = mock(MethodSignature.class);

    when(pjp.getSignature()).thenReturn(sig);
    when(sig.getMethod()).thenReturn(getClass().getDeclaredMethod("sampleMethod", String.class));
    when(pjp.getArgs()).thenReturn(new Object[]{"testKey"});
    when(pjp.proceed()).thenReturn("result");
    when(ann.key()).thenReturn("'staticKey'");
    when(ann.operation()).thenReturn(OperationType.READ);
    when(ann.softExpire()).thenReturn(true);
    when(ann.hardTtlMs()).thenReturn(0L);
    when(ann.softTtlMs()).thenReturn(0L);
    when(hotKeyFacade.getWithSoftExpire(anyString(), any(), anyLong(), anyLong()))
      .thenReturn(Optional.of("result"));

    Object result = aspect.around(pjp, ann);
    assertThat(result).isEqualTo("result");
    verify(hotKeyFacade).getWithSoftExpire(anyString(), any(), anyLong(), anyLong());
  }

  @Test
  void around_readWithoutSoftExpire_shouldUseGet() throws Throwable {
    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    HotKey ann = mock(HotKey.class);
    MethodSignature sig = mock(MethodSignature.class);

    when(pjp.getSignature()).thenReturn(sig);
    when(sig.getMethod()).thenReturn(getClass().getDeclaredMethod("sampleMethod", String.class));
    when(pjp.getArgs()).thenReturn(new Object[]{"testKey"});
    when(pjp.proceed()).thenReturn("result");
    when(ann.key()).thenReturn("'staticKey'");
    when(ann.operation()).thenReturn(OperationType.READ);
    when(ann.softExpire()).thenReturn(false);
    when(ann.hardTtlMs()).thenReturn(0L);
    when(ann.softTtlMs()).thenReturn(0L);
    when(hotKeyFacade.get(anyString(), any(), anyLong(), anyLong()))
      .thenReturn(Optional.of("result"));

    Object result = aspect.around(pjp, ann);
    assertThat(result).isEqualTo("result");
    verify(hotKeyFacade).get(anyString(), any(), anyLong(), anyLong());
  }

  @Test
  void around_write_shouldUsePutBeforeInvalidate() throws Throwable {
    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    HotKey ann = mock(HotKey.class);
    MethodSignature sig = mock(MethodSignature.class);

    when(pjp.getSignature()).thenReturn(sig);
    when(sig.getMethod()).thenReturn(getClass().getDeclaredMethod("sampleMethod", String.class));
    when(pjp.getArgs()).thenReturn(new Object[]{"testKey"});
    when(pjp.proceed()).thenReturn("writeResult");
    when(ann.key()).thenReturn("'staticKey'");
    when(ann.operation()).thenReturn(OperationType.WRITE);

    doAnswer(invocation -> {
      Runnable action = invocation.getArgument(1);
      action.run();
      return null;
    }).when(hotKeyFacade).putBeforeInvalidate(anyString(), any());

    Object result = aspect.around(pjp, ann);
    assertThat(result).isEqualTo("writeResult");
  }

  @Test
  void around_invalidate_shouldCallInvalidateAndProceed() throws Throwable {
    ProceedingJoinPoint pjp = mock(ProceedingJoinPoint.class);
    HotKey ann = mock(HotKey.class);
    MethodSignature sig = mock(MethodSignature.class);

    when(pjp.getSignature()).thenReturn(sig);
    when(sig.getMethod()).thenReturn(getClass().getDeclaredMethod("sampleMethod", String.class));
    when(pjp.getArgs()).thenReturn(new Object[]{"testKey"});
    when(pjp.proceed()).thenReturn("afterInvalidate");
    when(ann.key()).thenReturn("'staticKey'");
    when(ann.operation()).thenReturn(OperationType.INVALIDATE);

    Object result = aspect.around(pjp, ann);
    assertThat(result).isEqualTo("afterInvalidate");
    verify(hotKeyFacade).invalidate(anyString());
  }

  @SuppressWarnings("unused")
  public String sampleMethod(String arg) {
    return arg;
  }
}
