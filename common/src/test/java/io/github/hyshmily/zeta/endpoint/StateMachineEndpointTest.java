package io.github.hyshmily.zeta.endpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.github.hyshmily.zeta.detection.ZetaBayesianSM;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

class StateMachineEndpointTest {

  private ZetaBayesianSM stateMachine;
  private ObjectProvider<AtomicLong> configTimestampCounter;
  private StateMachineEndpoint endpoint;

  @BeforeEach
  void setUp() {
    stateMachine = mock(ZetaBayesianSM.class);
    configTimestampCounter = mock(ObjectProvider.class);
    endpoint = new StateMachineEndpoint(stateMachine, configTimestampCounter);
  }

  /**
   * Stubs the state machine's CURRENT config to a valid baseline — the
   * endpoint validates the POST-applied combination (provided fields override,
   * others keep their current values) against the same invariant the config
   * gossip layer enforces.
   */
  private void stubCurrentConfig(int confirm, int cool, int preCoolGrace) {
    when(stateMachine.getConfirmCount()).thenReturn(confirm);
    when(stateMachine.getCoolCount()).thenReturn(cool);
    when(stateMachine.getPreCoolGraceCount()).thenReturn(preCoolGrace);
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
    stubCurrentConfig(3, 10, 4);
    AtomicLong counter = new AtomicLong(5);
    when(configTimestampCounter.getIfAvailable()).thenReturn(counter);

    Map<String, Object> result = endpoint.set(8, null, null);

    assertThat(result).containsEntry("status", "ok");
    verify(stateMachine).setConfirmCount(8);
  }

  @Test
  void set_withCoolCount_shouldUpdate() {
    stubCurrentConfig(3, 10, 4);
    AtomicLong counter = new AtomicLong(5);
    when(configTimestampCounter.getIfAvailable()).thenReturn(counter);

    Map<String, Object> result = endpoint.set(null, 20, null);

    assertThat(result).containsEntry("status", "ok");
    verify(stateMachine).setCoolCount(20);
  }

  @Test
  void set_withPreCoolGraceCount_shouldUpdate() {
    stubCurrentConfig(3, 10, 4);
    AtomicLong counter = new AtomicLong(5);
    when(configTimestampCounter.getIfAvailable()).thenReturn(counter);

    Map<String, Object> result = endpoint.set(null, null, 8);

    assertThat(result).containsEntry("status", "ok");
    verify(stateMachine).setPreCoolGraceCount(8);
  }

  @Test
  void set_withNoParams_shouldNotMutateAnything() {
    stubCurrentConfig(3, 10, 4);
    AtomicLong counter = new AtomicLong(5);
    when(configTimestampCounter.getIfAvailable()).thenReturn(counter);

    Map<String, Object> result = endpoint.set(null, null, null);

    assertThat(result).containsEntry("status", "ok");
    // The current config is read for the combination validation, but nothing
    // is rewritten and the timestamp counter is not bumped.
    verify(stateMachine, never()).setConfirmCount(anyInt());
    verify(stateMachine, never()).setCoolCount(anyInt());
    verify(stateMachine, never()).setPreCoolGraceCount(anyInt());
    assertThat(counter.get()).isEqualTo(5);
  }

  @Test
  void set_withCounterAvailable_shouldIncrement() {
    stubCurrentConfig(3, 10, 4);
    AtomicLong counter = mock(AtomicLong.class);
    when(configTimestampCounter.getIfAvailable()).thenReturn(counter);

    endpoint.set(5, null, null);

    verify(counter).incrementAndGet();
  }

  @Test
  void set_withCounterUnavailable_shouldNotThrow() {
    stubCurrentConfig(3, 10, 4);
    when(configTimestampCounter.getIfAvailable()).thenReturn(null);

    Map<String, Object> result = endpoint.set(3, null, null);

    assertThat(result).containsEntry("status", "ok");
  }

  @Test
  void set_withAllParams_shouldUpdateAll() {
    stubCurrentConfig(3, 10, 4);
    AtomicLong counter = new AtomicLong(5);
    when(configTimestampCounter.getIfAvailable()).thenReturn(counter);

    Map<String, Object> result = endpoint.set(7, 15, 5);

    assertThat(result).containsEntry("status", "ok");
    verify(stateMachine).setConfirmCount(7);
    verify(stateMachine).setCoolCount(15);
    verify(stateMachine).setPreCoolGraceCount(5);
  }

  @Test
  void set_withAllParams_shouldIncrementCounterOnce() {
    stubCurrentConfig(3, 10, 4);
    AtomicLong counter = mock(AtomicLong.class);
    when(configTimestampCounter.getIfAvailable()).thenReturn(counter);

    endpoint.set(7, 15, 5);

    // Counter should be incremented once (after all params are applied)
    verify(counter, times(1)).incrementAndGet();
  }

  /**
   * Typed write parameters move malformed-input rejection into the actuator
   * framework (non-numeric JSON never dispatches — HTTP 400 before this
   * method runs). The null-means-absent contract this test pins instead: a
   * partial update keeps the other fields at their current values.
   */
  @Test
  void set_withPartialUpdate_shouldKeepOtherFields() {
    stubCurrentConfig(3, 10, 4);
    AtomicLong counter = new AtomicLong(5);
    when(configTimestampCounter.getIfAvailable()).thenReturn(counter);

    Map<String, Object> result = endpoint.set(null, 20, null);

    assertThat(result).containsEntry("status", "ok");
    verify(stateMachine).setCoolCount(20);
    verify(stateMachine, never()).setConfirmCount(anyInt());
    verify(stateMachine, never()).setPreCoolGraceCount(anyInt());
  }

  @Test
  void set_withNegativeConfirmCount_shouldReturnError() {
    stubCurrentConfig(3, 10, 4);
    Map<String, Object> result = endpoint.set(-1, null, null);
    assertThat(result).containsEntry("status", "error");
    verify(stateMachine, never()).setConfirmCount(anyInt());
  }

  @Test
  void set_withNegativeCoolCount_shouldReturnError() {
    stubCurrentConfig(3, 10, 4);
    Map<String, Object> result = endpoint.set(null, -5, null);
    assertThat(result).containsEntry("status", "error");
    verify(stateMachine, never()).setCoolCount(anyInt());
  }

  @Test
  void set_withNegativePreCoolGraceCount_shouldReturnError() {
    stubCurrentConfig(3, 10, 4);
    Map<String, Object> result = endpoint.set(null, null, -3);
    assertThat(result).containsEntry("status", "error");
    verify(stateMachine, never()).setPreCoolGraceCount(anyInt());
  }

  /**
   * Regression: a zero-valued config satisfies no gossip invariant
   * ({@code confirmCount >= 1, preCoolGraceCount >= 1, coolCount > preCoolGraceCount})
   * and used to be applied locally while every peer rejected the heartbeat —
   * permanent cluster divergence. The endpoint must reject it outright.
   */
  @Test
  void set_withZeroValues_shouldBeRejected() {
    stubCurrentConfig(3, 10, 4);
    Map<String, Object> result = endpoint.set(0, 0, 0);
    assertThat(result).containsEntry("status", "error");
    verify(stateMachine, never()).setConfirmCount(anyInt());
    verify(stateMachine, never()).setCoolCount(anyInt());
    verify(stateMachine, never()).setPreCoolGraceCount(anyInt());
  }

  /**
   * Regression: the endpoint must enforce the same coolCount &gt;
   * preCoolGraceCount invariant the config-negotiation layer enforces on
   * gossip — a locally-applied violation has no reconciliation path.
   */
  @Test
  void set_withCoolCountBelowGraceCount_shouldBeRejected() {
    stubCurrentConfig(3, 10, 4);
    Map<String, Object> result = endpoint.set(null, 2, null);
    assertThat(result).containsEntry("status", "error");
    verify(stateMachine, never()).setCoolCount(anyInt());
  }

  @Test
  void set_withCounterAvailable_shouldIncrementEachTime() {
    stubCurrentConfig(3, 10, 4);
    AtomicLong counter = mock(AtomicLong.class);
    when(configTimestampCounter.getIfAvailable()).thenReturn(counter);

    endpoint.set(5, null, null);
    endpoint.set(null, 10, null);

    verify(counter, times(2)).incrementAndGet();
  }
}
