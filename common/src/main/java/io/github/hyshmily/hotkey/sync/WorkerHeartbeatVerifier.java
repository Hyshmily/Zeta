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
package io.github.hyshmily.hotkey.sync;

import io.github.hyshmily.hotkey.logging.DefaultLogger;
import io.github.hyshmily.hotkey.logging.HotKeyLogger;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.AmqpTimeoutException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.*;

/**
 * On-demand active verifier — only triggers when heartbeats go silent.
 *
 * <p>Periodically checks {@link ClusterHealthView} for Workers that have
 * exceeded {@code heartbeatTimeoutMs} without a heartbeat, sends a PING
 * via Direct reply-to, and updates the health view on PONG or failure.
 */
@RequiredArgsConstructor
public class WorkerHeartbeatVerifier {

  private static final HotKeyLogger log = new DefaultLogger(WorkerHeartbeatVerifier.class);

  private final RabbitTemplate rabbitTemplate;
  private final ClusterHealthView healthView;
  private final String appInstanceId;
  private final long verifyIntervalMs;
  private final long pingTimeoutMs;
  private final int degradeAfterFailures;
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
    Thread t = new Thread(r, "hb-verifier");
    t.setDaemon(true);
    return t;
  });

  private static final String QUEUE_VERIFY_PING_PREFIX = "hotkey.verify.ping.";

  /**
   * Starts the periodic heartbeat verification at a fixed rate.
   *
   * <p>Every {@code verifyIntervalMs} milliseconds, iterates over all Workers in
   * {@link ClusterHealthView} that have exceeded the heartbeat timeout and sends
   * a PING via Direct reply-to. Updates the health view on PONG or failure.
   */
  public void start() {
    scheduler.scheduleAtFixedRate(
      this::verifySuspectedWorkers,
      verifyIntervalMs,
      verifyIntervalMs,
      TimeUnit.MILLISECONDS
    );
  }

  /**
   * Gracefully shuts down the heartbeat verification scheduler.
   *
   * <p>Already-running tasks are allowed to complete; no new verification
   * cycles will be scheduled.
   */
  public void stop() {
    scheduler.shutdown();
  }

  void verifySuspectedWorkers() {
    if (healthView.isClusterHealthy()) {
      return;
    }

    Set<String> suspected = healthView
      .getAllWorkerIds()
      .stream()
      .filter(id -> !healthView.getAliveWorkerIds().contains(id))
      .collect(Collectors.toSet());

    if (suspected.isEmpty()) {
      return;
    }

    log.info("Verifying suspected workers: {}", suspected);

    int failures = 0;
    for (String workerId : suspected) {
      boolean alive = sendPingAndWaitPong(workerId);
      if (!alive) {
        failures++;
        healthView.markVerificationFailed(workerId);
        log.warn("Worker {} verification failed (failures={})", workerId, failures);
      } else {
        log.debug("Worker {} verification succeeded", workerId);
        healthView.recordPong(workerId);
      }
    }

    if (failures >= degradeAfterFailures && !healthView.isClusterHealthy()) {
      log.warn("Cluster degraded: {} workers unreachable after verification (threshold={})", failures, degradeAfterFailures);
      healthView.setDegraded(true);
    }
  }

  boolean sendPingAndWaitPong(String workerId) {
    MessageProperties props = new MessageProperties();
    props.setHeader(AMQP_HEADER_VERIFY_TYPE, AMQP_HEADER_VERIFY_PING);
    props.setHeader(AMQP_HEADER_VERIFY_APP_INSTANCE, appInstanceId);
    props.setReplyTo("amq.rabbitmq.reply-to");

    Message ping = new Message(new byte[0], props);

    try {
      rabbitTemplate.setReplyTimeout((int) pingTimeoutMs);
      Message pong = rabbitTemplate.sendAndReceive("", QUEUE_VERIFY_PING_PREFIX + workerId, ping);
      return pong != null;
    } catch (AmqpTimeoutException e) {
      return false;
    }
  }
}
