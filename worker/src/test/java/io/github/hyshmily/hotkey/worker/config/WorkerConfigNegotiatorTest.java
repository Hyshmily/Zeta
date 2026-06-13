package io.github.hyshmily.hotkey.worker.config;

import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.sync.WorkerHeartbeatMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

import java.util.concurrent.atomic.AtomicLong;

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class WorkerConfigNegotiatorTest {

  @Mock
  private HotKeyStateMachine stateMachine;

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
    props.setHeader("hbConfigTs", configTs);
    props.setHeader("hbConfigConfirm", 5);
    props.setHeader("hbConfigCool", 10);
    props.setHeader("hbConfigGrace", 3);
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
  void shouldHandleInterruptGracefully() throws InterruptedException {
    negotiator.syncOnStartup();

    Thread daemon = null;
    for (Thread t : Thread.getAllStackTraces().keySet()) {
      if ("config-sync-startup".equals(t.getName())) {
        daemon = t;
        break;
      }
    }
    assertThat(daemon).isNotNull();
    assertThat(daemon.isDaemon()).isTrue();

    daemon.interrupt();
    daemon.join(1000);
    assertThat(daemon.isAlive()).isFalse();
  }

  @Test
  void integrationSyncOnStartupAndOnHeartbeatShouldApplyConfigAndReleaseLatch()
      throws InterruptedException {
    negotiator.syncOnStartup();

    Thread daemon = findDaemonThread();
    assertThat(daemon.isAlive()).isTrue();

    Message msg = createHeartbeatMessage("worker-2", 15);
    negotiator.onHeartbeat(msg);

    daemon.join(1000);
    assertThat(daemon.isAlive()).isFalse();
    verify(stateMachine).setConfirmCount(5);
    verify(stateMachine).setCoolCount(10);
    verify(stateMachine).setPreCoolGraceCount(3);
    assertThat(configTimestampCounter.get()).isEqualTo(15);
  }

  private Thread findDaemonThread() {
    for (Thread t : Thread.getAllStackTraces().keySet()) {
      if ("config-sync-startup".equals(t.getName())) {
        return t;
      }
    }
    throw new AssertionError("config-sync-startup thread not found");
  }
}
