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
package io.github.hyshmily.zeta.worker.config;

import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.HEADER_TYPE;

import io.github.hyshmily.zeta.detection.ZetaBayesianSM;
import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatMessage;
import io.github.hyshmily.zeta.util.ZetaThreadFactory;
import io.github.hyshmily.zeta.worker.rule.FastLaneRuleManager;
import io.github.hyshmily.zeta.worker.rule.FastLaneRulesMessage;
import jakarta.annotation.PostConstruct;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;

/**
 * Listens for heartbeat-based config updates from peer Workers and applies them
 * if the received config timestamp is newer than the local one.
 *
 * <p>The config queue carries two message types, demultiplexed by the
 * {@code type} header:
 * <ul>
 *   <li>{@code WORKER_HB} — state-machine config gossip (confirm/cool/grace
 *       counts), applied when the embedded timestamp is newer (ADR-0003).</li>
 *   <li>{@code FASTLANE_RULES} — full fast-lane rule set with a wall-clock
 *       version, applied last-writer-wins via
 *       {@link FastLaneRuleManager#replaceAll} (ADR-0025).</li>
 * </ul>
 *
 * <p>On startup, waits up to 3 seconds for the first heartbeat to arrive.
 * If none is received, the Worker continues with the values from
 * {@link io.github.hyshmily.zeta.worker.config.WorkerProperties} — this can happen when all other Workers are down.
 */
@Slf4j
public class WorkerConfigNegotiator {

  /** State machine whose config (confirm/cool/grace counts) is updated from heartbeat messages. */
  private final ZetaBayesianSM stateMachine;
  /** Monotonically increasing counter tracking the latest config-change timestamp. */
  private final AtomicLong configTimestampCounter;
  /** Unique identifier for this Worker node, used in queue names and heartbeat identification. */
  private final String nodeId;
  /** Fast-lane rule manager receiving gossiped rule sets; may be {@code null} (rules gossip disabled). */
  private final FastLaneRuleManager fastLaneRuleManager;
  private final CountDownLatch startupLatch = new CountDownLatch(1);

  /**
   * Compatibility constructor without fast-lane rules gossip (rules messages
   * are silently skipped). Prefer {@link #WorkerConfigNegotiator(ZetaBayesianSM, AtomicLong, String, FastLaneRuleManager)}.
   *
   * @param stateMachine           the worker's state machine
   * @param configTimestampCounter the shared config-change timestamp counter
   * @param nodeId                 unique identifier for this Worker node
   */
  public WorkerConfigNegotiator(ZetaBayesianSM stateMachine, AtomicLong configTimestampCounter, String nodeId) {
    this(stateMachine, configTimestampCounter, nodeId, null);
  }

  /**
   * Full constructor with fast-lane rules gossip support.
   *
   * @param stateMachine           the worker's state machine
   * @param configTimestampCounter the shared config-change timestamp counter
   * @param nodeId                 unique identifier for this Worker node
   * @param fastLaneRuleManager    the rule manager receiving gossiped rule sets (may be {@code null})
   */
  public WorkerConfigNegotiator(
    ZetaBayesianSM stateMachine,
    AtomicLong configTimestampCounter,
    String nodeId,
    FastLaneRuleManager fastLaneRuleManager
  ) {
    this.stateMachine = stateMachine;
    this.configTimestampCounter = configTimestampCounter;
    this.nodeId = nodeId;
    this.fastLaneRuleManager = fastLaneRuleManager;
  }

  /**
   * Waits up to 3 seconds for the first heartbeat from a peer Worker.
   *
   * <p>The waiting thread simply counts down the latch when a valid config
   * heartbeat arrives via the async {@link #onHeartbeat} listener.  Config
   * is applied asynchronously by the listener; if no heartbeat arrives
   * within 3 seconds, a warning is logged and the Worker proceeds with
   * configured defaults.
   */
  @PostConstruct
  void syncOnStartup() {
    Thread waitThread = new ZetaThreadFactory("zeta-config-sync-startup").newThread(() -> {
      try {
        boolean received = startupLatch.await(3000, TimeUnit.MILLISECONDS);
        if (!received) {
          log.warn("No config heartbeat received within 3s, using WorkerProperties defaults");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    waitThread.start();
  }

  /**
   * Processes an incoming message from the config queue and dispatches on the
   * {@code type} header: heartbeat config gossip ({@code WORKER_HB}) or
   * fast-lane rules gossip ({@code FASTLANE_RULES}).
   *
   * @param msg the raw AMQP message
   */
  @RabbitListener(queues = "#{@workerConfigQueue.name}", containerFactory = "workerConfigListenerContainerFactory")
  public void onHeartbeat(Message msg) {
    try {
      doOnMessage(msg);
    } catch (Exception e) {
      log.warn("Uncaught exception in onHeartbeat config negotiation, discarding message to prevent requeue loop", e);
    }
  }

  private void doOnMessage(Message msg) {
    if (msg == null || msg.getMessageProperties() == null) {
      return;
    }
    if (FastLaneRulesMessage.TYPE.equals(msg.getMessageProperties().getHeader(HEADER_TYPE))) {
      doOnRulesMessage(msg);
      return;
    }
    doOnHeartbeat(msg);
  }

  /**
   * Applies a gossiped fast-lane rule set last-writer-wins (ADR-0025).
   *
   * <p>Same-millisecond ties are broken by nodeId lexicographic order so that
   * simultaneous edits on two Workers converge deterministically. A tie-win
   * message whose content already equals the local set is skipped to avoid
   * needlessly invalidating the match cache on every periodic rebroadcast.
   */
  private void doOnRulesMessage(Message msg) {
    if (fastLaneRuleManager == null) {
      return;
    }
    FastLaneRulesMessage rulesMsg = FastLaneRulesMessage.from(msg);
    if (rulesMsg == null || rulesMsg.nodeId().equals(nodeId)) {
      return;
    }

    long remote = rulesMsg.rulesVersion();
    long local = fastLaneRuleManager.getRulesVersion();
    boolean newer = remote > local;
    boolean tieWin = remote == local && rulesMsg.nodeId().compareTo(nodeId) > 0;
    if (newer || (tieWin && !rulesMsg.rules().equals(fastLaneRuleManager.getRules()))) {
      fastLaneRuleManager.replaceAll(rulesMsg.rules(), remote);
      log.info(
        "Applied fast-lane rules gossip from {}: version={}, rules={}",
        rulesMsg.nodeId(),
        remote,
        rulesMsg.rules().size()
      );
    }
  }

  private void doOnHeartbeat(Message msg) {
    WorkerHeartbeatMessage hb = WorkerHeartbeatMessage.from(msg);
    if (hb == null) {
      return;
    }

    if (hb.workerId().equals(nodeId)) {
      return;
    }

    long remoteTs = hb.configTimestamp();
    long localTs = configTimestampCounter.get();
    if (remoteTs <= localTs) {
      return;
    }

    int cc = hb.configConfirmCount();
    int gc = hb.configCoolCount();
    int pgc = hb.configGraceCount();
    if (cc <= 0 || pgc <= 0 || gc <= pgc) {
      log.warn(
        "Ignoring malformed config from {}: confirmCount={}, coolCount={}, preCoolGraceCount={}",
        hb.workerId(),
        cc,
        gc,
        pgc
      );
      return;
    }
    stateMachine.setConfirmCount(cc);
    stateMachine.setCoolCount(gc);
    stateMachine.setPreCoolGraceCount(pgc);

    configTimestampCounter.set(remoteTs);

    log.debug(
      "Applied newer config from {}: confirmCount={}, coolCount={}, preCoolGraceCount={}",
      hb.workerId(),
      hb.configConfirmCount(),
      hb.configCoolCount(),
      hb.configGraceCount()
    );

    if (startupLatch.getCount() > 0) {
      startupLatch.countDown();
    }
  }
}
