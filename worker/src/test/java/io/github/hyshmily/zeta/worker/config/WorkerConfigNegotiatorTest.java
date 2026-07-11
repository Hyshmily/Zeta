package io.github.hyshmily.zeta.worker.config;

import static io.github.hyshmily.zeta.constants.ZetaConstants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import io.github.hyshmily.zeta.detection.ZetaStateMachine;
import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatMessage;
import java.util.concurrent.atomic.AtomicLong;
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
  private ZetaStateMachine stateMachine;

  private final AtomicLong configTimestampCounter = new AtomicLong(0);
  private final String nodeId = "worker-1";
  private WorkerConfigNegotiator negotiator;

  @BeforeEach
  void setUp() {
    for (Thread t : Thread.getAllStackTraces().keySet()) {
      if ("config-sync-startup".equals(t.getName())) {
        t.interrupt();
      }
    }
    configTimestampCounter.set(0);
    negotiator = new WorkerConfigNegotiator(stateMachine, configTimestampCounter, nodeId);
  }

  private static Message createHeartbeatMessage(String workerId, long configTs) {
    MessageProperties props = new MessageProperties();
    props.setHeader(AMQP_HEADER_TYPE, WorkerHeartbeatMessage.TYPE);
    props.setHeader(AMQP_HEADER_NODE_ID, workerId);
    props.setHeader(AMQP_HEADER_HEARTBEAT_CONFIG_TIMESTAMP, configTs);
    props.setHeader(AMQP_HEADER_HEARTBEAT_CONFIG_CONFIRM, 5);
    props.setHeader(AMQP_HEADER_HEARTBEAT_CONFIG_COOL, 10);
    props.setHeader(AMQP_HEADER_HEARTBEAT_CONFIG_GRACE, 3);
    props.setHeader(AMQP_HEADER_HEARTBEAT_EPOCH, 1L);
    props.setHeader(AMQP_HEADER_TIMESTAMP, System.currentTimeMillis());
    props.setHeader(AMQP_HEADER_HEARTBEAT_DV_HWM, 0L);
    props.setHeader(AMQP_HEADER_HEARTBEAT_LOAD, 0.0);
    props.setHeader(AMQP_HEADER_HEARTBEAT_READY, true);
    props.setHeader(AMQP_HEADER_HEARTBEAT_CONFIG_FP, 0);
    return new Message(workerId.getBytes(), props);
  }

  private static Message createMessageWithWrongType() {
    MessageProperties props = new MessageProperties();
    props.setHeader(AMQP_HEADER_TYPE, "OTHER");
    return new Message(new byte[0], props);
  }

  @Test
  void shouldReturnEarlyWhenHeartbeatIsNull() {
    negotiator.onHeartbeat(createMessageWithWrongType());

    verifyNoInteractions(stateMachine);
    assertThat(configTimestampCounter.get()).isZero();
  }

  @Test
  void shouldReturnEarlyWhenOwnMessage() {
    Message msg = createHeartbeatMessage(nodeId, 10);

    negotiator.onHeartbeat(msg);

    verifyNoInteractions(stateMachine);
    assertThat(configTimestampCounter.get()).isZero();
  }

  @Test
  void shouldIgnoreStaleTimestamp() {
    configTimestampCounter.set(100);
    Message msg = createHeartbeatMessage("worker-2", 50);

    negotiator.onHeartbeat(msg);

    verifyNoInteractions(stateMachine);
    assertThat(configTimestampCounter.get()).isEqualTo(100);
  }

  @Test
  void shouldApplyNewerConfig() {
    Message msg = createHeartbeatMessage("worker-2", 10);

    negotiator.onHeartbeat(msg);

    verify(stateMachine).setConfirmCount(5);
    verify(stateMachine).setCoolCount(10);
    verify(stateMachine).setPreCoolGraceCount(3);
    assertThat(configTimestampCounter.get()).isEqualTo(10);
  }

  @Test
  void shouldIgnoreEqualTimestamp() {
    configTimestampCounter.set(10);
    Message msg = createHeartbeatMessage("worker-2", 10);

    negotiator.onHeartbeat(msg);

    verifyNoInteractions(stateMachine);
    assertThat(configTimestampCounter.get()).isEqualTo(10);
  }

  @Test
  void shouldReleaseStartupLatchOnFirstValidHeartbeat() {
    negotiator.syncOnStartup();
    Message msg = createHeartbeatMessage("worker-2", 5);

    negotiator.onHeartbeat(msg);

    verify(stateMachine).setConfirmCount(5);
    assertThat(configTimestampCounter.get()).isEqualTo(5);
  }

  @Test
  void syncOnStartupShouldNotThrowOnTimeout() {
    assertThatCode(() -> negotiator.syncOnStartup()).doesNotThrowAnyException();
  }

  @Test
  void syncOnStartupShouldStartDaemonThread() throws InterruptedException {
    negotiator.syncOnStartup();
    Message msg = createHeartbeatMessage("worker-2", 7);

    negotiator.onHeartbeat(msg);

    Thread.sleep(50);
    verify(stateMachine).setConfirmCount(5);
    assertThat(configTimestampCounter.get()).isEqualTo(7);
  }

  @Test
  void shouldHandleInterruptGracefully() {
    assertThatCode(() -> negotiator.syncOnStartup()).doesNotThrowAnyException();
  }

  @Test
  void integrationSyncOnStartupAndOnHeartbeatShouldApplyConfigAndReleaseLatch() {
    negotiator.syncOnStartup();

    Message msg = createHeartbeatMessage("worker-2", 15);
    negotiator.onHeartbeat(msg);

    verify(stateMachine).setConfirmCount(5);
    verify(stateMachine).setCoolCount(10);
    verify(stateMachine).setPreCoolGraceCount(3);
    assertThat(configTimestampCounter.get()).isEqualTo(15);
  }

  /**
   * Verifies that a heartbeat with zero config timestamp is skipped when the local
   * counter is also at zero, because {@code remoteTs <= localTs} (0 <= 0).
   */
  @Test
  void shouldSkipConfigWithEqualZeroTimestamp() {
    Message msg = createHeartbeatMessage("worker-2", 0);
    negotiator.onHeartbeat(msg);

    verifyNoInteractions(stateMachine);
    assertThat(configTimestampCounter.get()).isEqualTo(0);
  }

  /**
   * Verifies that multiple heartbeat messages with increasing timestamps are all
   * applied, each updating the config to the latest values.
   */
  @Test
  void shouldApplyMultipleHeartbeatUpdatesInOrder() {
    Message msg1 = createHeartbeatMessage("worker-2", 10);
    negotiator.onHeartbeat(msg1);
    assertThat(configTimestampCounter.get()).isEqualTo(10);

    Message msg2 = createHeartbeatMessage("worker-2", 20);
    negotiator.onHeartbeat(msg2);
    assertThat(configTimestampCounter.get()).isEqualTo(20);

    Message msg3 = createHeartbeatMessage("worker-2", 30);
    negotiator.onHeartbeat(msg3);
    assertThat(configTimestampCounter.get()).isEqualTo(30);
  }

  /**
   * Verifies that a null (malformed) heartbeat message is silently ignored.
   */
  @Test
  void shouldIgnoreMessageWithWrongHeaders() {
    MessageProperties props = new MessageProperties();
    props.setHeader(AMQP_HEADER_TYPE, WorkerHeartbeatMessage.TYPE);
    // Missing required headers (nodeId, configTs, etc.) → WorkerHeartbeatMessage.from returns null
    Message msg = new Message("worker-2".getBytes(), props);

    negotiator.onHeartbeat(msg);

    verifyNoInteractions(stateMachine);
    assertThat(configTimestampCounter.get()).isZero();
  }
}
