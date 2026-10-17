package io.github.hyshmily.zeta.worker.config;

import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.detection.ZetaBayesianSM;
import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatMessage;
import io.github.hyshmily.zeta.sync.worker.SmConfigSnapshot;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

@ExtendWith(MockitoExtension.class)
class WorkerConfigNegotiatorTest {

  @Mock
  private ZetaBayesianSM stateMachine;

  private final AtomicReference<SmConfigSnapshot> configSnapshot =
    new AtomicReference<>(new SmConfigSnapshot(3, 10, 4, 0L));

  /** Resets the gossip snapshot to the zero timestamp with the default counts. */
  private void setConfigTs(long ts) {
    configSnapshot.set(new SmConfigSnapshot(3, 10, 4, ts));
  }
  private final String nodeId = "worker-1";
  private WorkerConfigNegotiator negotiator;

  @BeforeEach
  void setUp() {
    setConfigTs(0);
    negotiator = new WorkerConfigNegotiator(stateMachine, configSnapshot, nodeId, null);
  }

  private static Message createHeartbeatMessage(String workerId, long configTs) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, WorkerHeartbeatMessage.TYPE);
    props.setHeader(HEADER_NODE_ID, workerId);
    props.setHeader(HEADER_HEARTBEAT_CONFIG_TIMESTAMP, configTs);
    props.setHeader(HEADER_HEARTBEAT_CONFIG_CONFIRM, 5);
    props.setHeader(HEADER_HEARTBEAT_CONFIG_COOL, 10);
    props.setHeader(HEADER_HEARTBEAT_CONFIG_GRACE, 3);
    props.setHeader(HEADER_HEARTBEAT_EPOCH, 1L);
    props.setHeader(HEADER_TIMESTAMP, System.currentTimeMillis());
    props.setHeader(HEADER_HEARTBEAT_LOAD, 0.0);
    props.setHeader(HEADER_HEARTBEAT_READY, true);
    props.setHeader(HEADER_HEARTBEAT_CONFIG_FP, 0);
    return new Message(workerId.getBytes(), props);
  }

  private static Message createMessageWithWrongType() {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, "OTHER");
    return new Message(new byte[0], props);
  }

  @Test
  void shouldReturnEarlyWhenHeartbeatIsNull() {
    negotiator.onHeartbeat(createMessageWithWrongType());

    verifyNoInteractions(stateMachine);
    assertThat(configSnapshot.get().configTimestamp()).isZero();
  }

  @Test
  void shouldReturnEarlyWhenOwnMessage() {
    Message msg = createHeartbeatMessage(nodeId, 10);

    negotiator.onHeartbeat(msg);

    verifyNoInteractions(stateMachine);
    assertThat(configSnapshot.get().configTimestamp()).isZero();
  }

  @Test
  void shouldIgnoreStaleTimestamp() {
    setConfigTs(100);
    Message msg = createHeartbeatMessage("worker-2", 50);

    negotiator.onHeartbeat(msg);

    verifyNoInteractions(stateMachine);
    assertThat(configSnapshot.get().configTimestamp()).isEqualTo(100);
  }

  @Test
  void shouldApplyNewerConfig() {
    Message msg = createHeartbeatMessage("worker-2", 10);

    negotiator.onHeartbeat(msg);

    verify(stateMachine).setConfirmCount(5);
    verify(stateMachine).setCoolCount(10);
    verify(stateMachine).setPreCoolGraceCount(3);
    assertThat(configSnapshot.get().configTimestamp()).isEqualTo(10);
  }

  /**
   * Equal timestamps are broken by nodeId lexicographic order (mirroring the
   * fast-lane rules gossip tie-break, ADR-0025): a remote with a LOWER nodeId
   * loses the tie and is ignored — without this, two Workers configured within
   * the same millisecond would each ignore the other's heartbeat forever and
   * keep permanently divergent configs.
   */
  @Test
  void shouldIgnoreEqualTimestampFromLowerNodeId() {
    setConfigTs(10);
    Message msg = createHeartbeatMessage("worker-0", 10);

    negotiator.onHeartbeat(msg);

    verifyNoInteractions(stateMachine);
    assertThat(configSnapshot.get().configTimestamp()).isEqualTo(10);
  }

  /**
   * Equal timestamps are ignored outright (no nodeId tie-break any more): the
   * wall-clock-seeded startup snapshot plus strictly-newer-only adoption means
   * concurrent same-millisecond edits converge on the next strictly-newer
   * write instead of flapping on tie-breaks. A remote heartbeat carrying the
   * same timestamp as the local watermark must therefore leave both the state
   * machine and the snapshot untouched.
   */
  @Test
  void shouldIgnoreEqualTimestampEvenFromHigherNodeId() {
    setConfigTs(10);
    Message msg = createHeartbeatMessage("worker-2", 10);

    negotiator.onHeartbeat(msg);

    verifyNoInteractions(stateMachine);
    assertThat(configSnapshot.get().configTimestamp()).isEqualTo(10);
    assertThat(negotiator.hasReceivedConfig()).isFalse();
  }

  @Test
  void shouldReleaseStartupLatchOnFirstValidHeartbeat() {
    assertThat(negotiator.hasReceivedConfig()).isFalse();
    Message msg = createHeartbeatMessage("worker-2", 5);

    negotiator.onHeartbeat(msg);

    verify(stateMachine).setConfirmCount(5);
    assertThat(configSnapshot.get().configTimestamp()).isEqualTo(5);
    assertThat(negotiator.hasReceivedConfig()).isTrue();
  }

  /**
   * The startup check is a lock-free latch query ({@code hasReceivedConfig})
   * that the auto-configuration schedules externally — it must never throw
   * and must report "not received" before any heartbeat.
   */
  @Test
  void hasReceivedConfigShouldBeSafeBeforeAnyHeartbeat() {
    assertThatCode(() -> negotiator.hasReceivedConfig()).doesNotThrowAnyException();
    assertThat(negotiator.hasReceivedConfig()).isFalse();
  }

  /**
   * Verifies that a heartbeat with zero config timestamp is skipped when the local
   * counter is also at zero and the remote loses the nodeId tie-break
   * ({@code "worker-0" < "worker-1"}).
   */
  @Test
  void shouldSkipConfigWithEqualZeroTimestampFromLowerNodeId() {
    Message msg = createHeartbeatMessage("worker-0", 0);
    negotiator.onHeartbeat(msg);

    verifyNoInteractions(stateMachine);
    assertThat(configSnapshot.get().configTimestamp()).isEqualTo(0);
  }

  /**
   * Verifies that multiple heartbeat messages with increasing timestamps are all
   * applied, each updating the config to the latest values.
   */
  @Test
  void shouldApplyMultipleHeartbeatUpdatesInOrder() {
    Message msg1 = createHeartbeatMessage("worker-2", 10);
    negotiator.onHeartbeat(msg1);
    assertThat(configSnapshot.get().configTimestamp()).isEqualTo(10);

    Message msg2 = createHeartbeatMessage("worker-2", 20);
    negotiator.onHeartbeat(msg2);
    assertThat(configSnapshot.get().configTimestamp()).isEqualTo(20);

    Message msg3 = createHeartbeatMessage("worker-2", 30);
    negotiator.onHeartbeat(msg3);
    assertThat(configSnapshot.get().configTimestamp()).isEqualTo(30);
  }

  /**
   * Verifies that a null (malformed) heartbeat message is silently ignored.
   */
  @Test
  void shouldIgnoreMessageWithWrongHeaders() {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, WorkerHeartbeatMessage.TYPE);
    // Missing required headers (nodeId, configTs, etc.) → WorkerHeartbeatMessage.from returns null
    Message msg = new Message("worker-2".getBytes(), props);

    negotiator.onHeartbeat(msg);

    verifyNoInteractions(stateMachine);
    assertThat(configSnapshot.get().configTimestamp()).isZero();
  }

  /**
   * Reproduces the repeated-apply symptom: a peer re-broadcasts the same
   * config on every heartbeat. The first strictly-newer heartbeat advances
   * the watermark WITHOUT rewriting the state machine (the gossiped values
   * already match the local snapshot); every repeat then carries an equal
   * timestamp and is ignored outright — no re-apply, no re-log, once per
   * second forever.
   */
  @Test
  void shouldAdoptIdenticalValuesOnceForWatermarkThenDeduplicate() {
    // Local snapshot already carries the values the peer gossips (5/10/3).
    configSnapshot.set(new SmConfigSnapshot(5, 10, 3, 10L));
    Message msg = createHeartbeatMessage("worker-2", 20);

    negotiator.onHeartbeat(msg);
    negotiator.onHeartbeat(msg);
    negotiator.onHeartbeat(msg);

    verify(stateMachine, never()).setConfirmCount(anyInt());
    verify(stateMachine, never()).setCoolCount(anyInt());
    verify(stateMachine, never()).setPreCoolGraceCount(anyInt());
    assertThat(configSnapshot.get().configTimestamp()).isEqualTo(20);
    assertThat(negotiator.hasReceivedConfig()).isTrue();
  }

  /**
   * A strictly newer timestamp must still advance the local watermark even when
   * the gossiped values coincide with the local ones, otherwise this Worker
   * would keep broadcasting a stale timestamp that peers ignore.
   */
  @Test
  void shouldAdvanceWatermarkOnNewerTimestampWithIdenticalValues() {
    // Local snapshot already carries the gossiped values — the watermark must
    // still advance so this Worker stops broadcasting a timestamp peers ignore.
    configSnapshot.set(new SmConfigSnapshot(5, 10, 3, 10L));
    Message msg = createHeartbeatMessage("worker-2", 20);

    negotiator.onHeartbeat(msg);

    assertThat(configSnapshot.get().configTimestamp()).isEqualTo(20);
    verify(stateMachine, never()).setConfirmCount(anyInt());
    verify(stateMachine, never()).setCoolCount(anyInt());
    verify(stateMachine, never()).setPreCoolGraceCount(anyInt());
  }
}
