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
package io.github.hyshmily.zeta.worker.dispatch.connection;

import static io.github.hyshmily.zeta.constants.ZetaConstants.Routing.KEY_HEARTBEAT;

import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatMessage;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.worker.detection.lifecycle.HotKeyRegistry;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * Structured Worker heartbeat sender.
 *
 * <p>Each heartbeat carries the node id, this incarnation's epoch and the ready-to-serve flag on
 * the dedicated heartbeat exchange, plus the sender's tenant key when one is configured. The App's
 * health gate reads epoch and readiness; liveness is the heartbeat's own arrival cadence.
 *
 * <p>The epoch is node-local: {@code WorkerEpochStore} persists and increments it on each process
 * start, and the same value is stamped on this Worker's HOT/COOL decisions.
 *
 * <p>One message per call; the cadence belongs to {@code WorkerScheduling}, which owns every
 * periodic Worker task.
 */
@Slf4j
public class WorkerHeartbeatProducer {

  /**
   * First heartbeat instant (monotonic axis) anchoring the ready-to-serve grace period. Anchoring
   * at the first send, not bean construction, counts only the time the Worker has actually been
   * heartbeating; a slow context refresh before the scheduler starts cannot consume the grace.
   */
  private volatile long startTime = Long.MIN_VALUE;

  private final RabbitTemplate rabbitTemplate;
  /** The HOT set, read only for the ready-to-serve decision. */
  private final HotKeyRegistry hotRegistry;
  private final String heartbeatExchange;
  /** This process's immutable node id and epoch. */
  private final WorkerIdentity identity;
  /** Precomputed {@code heartbeat.<nodeId>}; the value is fixed for the lifetime of the bean. */
  private final String routingKey;
  private final long readyGraceMs;

  /** This application's tenant key, stamped so another App on the broker can drop the heartbeat. */
  @Nullable
  private final String appName;

  /**
   * Creates a producer that stamps its application name on every heartbeat.
   *
   * @param rabbitTemplate    the heartbeat-dedicated RabbitMQ template
   * @param heartbeatExchange the target exchange
   * @param identity          this process's immutable node id and epoch
   * @param hotRegistry       the HOT set for the ready-to-serve check
   * @param readyGraceMs      grace period before reporting ready with an empty HOT set
   * @param appName           the sending application name, or {@code null} for no stamp
   */
  public WorkerHeartbeatProducer(
    RabbitTemplate rabbitTemplate,
    String heartbeatExchange,
    WorkerIdentity identity,
    HotKeyRegistry hotRegistry,
    long readyGraceMs,
    @Nullable String appName
  ) {
    this.appName = appName;
    this.identity = identity;
    this.hotRegistry = hotRegistry;
    this.readyGraceMs = readyGraceMs;
    this.rabbitTemplate = rabbitTemplate;
    this.heartbeatExchange = heartbeatExchange;
    this.routingKey = KEY_HEARTBEAT + identity.nodeId();
  }

  /**
   * Whether this Worker is ready to serve hot-key detection.
   *
   * <p>Ready once the grace period elapsed or as soon as the HOT set is non-empty. The grace only
   * gates the ring-admission signal; it says nothing about detection state.
   *
   * @return {@code true} when the Worker considers itself ready
   */
  private boolean isReadyToServe() {
    long now = TimeSource.monotonicMillis();
    long start = startTime;
    if (start == Long.MIN_VALUE) {
      startTime = start = now;
    }
    return now - start >= readyGraceMs || hotRegistry.activeCount() > 0;
  }

  /**
   * Builds and sends one heartbeat.
   *
   * <p>Published to the heartbeat exchange with routing key {@code heartbeat.<workerId>}. A failure
   * is logged at WARN and otherwise ignored (fire-and-forget).
   */
  public void sendHeartbeat() {
    try {
      WorkerHeartbeatMessage hb = new WorkerHeartbeatMessage(
        TimeSource.currentTimeMillis(),
        identity.nodeId(),
        identity.epoch(),
        isReadyToServe(),
        appName
      );
      rabbitTemplate.send(heartbeatExchange, routingKey, hb.toMessage());
    } catch (Exception e) {
      log.warn("Scheduled sendHeartbeat failed", e);
    }
  }
}
