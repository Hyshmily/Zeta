package io.github.hyshmily.hotkey.endpoint;

import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class StateMachineEndpointTest {

  private HotKeyStateMachine stateMachine;
  private ObjectProvider<AtomicLong> configTimestampCounter;
  private StateMachineEndpoint endpoint;

  @BeforeEach
  void setUp() {
    stateMachine = mock(HotKeyStateMachine.class);
    configTimestampCounter = mock(ObjectProvider.class);
    endpoint = new StateMachineEndpoint(stateMachine, configTimestampCounter);
  }

  @Test
  void get_shouldReturnAllStateMachineFields() {
    when(stateMachine.getConfirmCount()).thenReturn(3);
    when(stateMachine.getCoolCount()).thenReturn(10);
    when(stateMachine.getPreCoolGraceCount()).thenReturn(4);
    when(stateMachine.getTrackedKeys()).thenReturn(7);

    Map<String, Object> result = endpoint.get();

    assertThat(result)
      .containsEntry("confirmCount", 3)
      .containsEntry("coolCount", 10)
      .containsEntry("preCoolGraceCount", 4)
      .containsEntry("trackedKeys", 7);
  }

  @Test
  void set_withConfirmCount_shouldUpdate() {
    AtomicLong counter = new AtomicLong(5);
    when(configTimestampCounter.getIfAvailable()).thenReturn(counter);

    Map<String, Object> result = endpoint.set(Map.of("confirmCount", "8"));

    assertThat(result).containsEntry("status", "ok");
    verify(stateMachine).setConfirmCount(8);
  }

  @Test
  void set_withCoolCount_shouldUpdate() {
    AtomicLong counter = new AtomicLong(5);
    when(configTimestampCounter.getIfAvailable()).thenReturn(counter);

    Map<String, Object> result = endpoint.set(Map.of("coolCount", "20"));

    assertThat(result).containsEntry("status", "ok");
    verify(stateMachine).setCoolCount(20);
  }

  @Test
  void set_withPreCoolGraceCount_shouldUpdate() {
    AtomicLong counter = new AtomicLong(5);
    when(configTimestampCounter.getIfAvailable()).thenReturn(counter);

    Map<String, Object> result = endpoint.set(Map.of("preCoolGraceCount", "8"));

    assertThat(result).containsEntry("status", "ok");
    verify(stateMachine).setPreCoolGraceCount(8);
  }

  @Test
  void set_withNoParams_shouldNotUpdateAnything() {
    AtomicLong counter = new AtomicLong(5);
    when(configTimestampCounter.getIfAvailable()).thenReturn(counter);

    Map<String, Object> result = endpoint.set(Map.of());

    assertThat(result).containsEntry("status", "ok");
    verifyNoInteractions(stateMachine);
  }

  @Test
  void set_withCounterAvailable_shouldIncrement() {
    AtomicLong counter = mock(AtomicLong.class);
    when(configTimestampCounter.getIfAvailable()).thenReturn(counter);

    endpoint.set(Map.of("confirmCount", "5"));

    verify(counter).incrementAndGet();
  }

  @Test
  void set_withCounterUnavailable_shouldNotThrow() {
    when(configTimestampCounter.getIfAvailable()).thenReturn(null);

    Map<String, Object> result = endpoint.set(Map.of("confirmCount", "3"));

    assertThat(result).containsEntry("status", "ok");
  }
}
