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
package io.github.hyshmily.hotkey.sync.worker;
import io.github.hyshmily.hotkey.sharding.ClusterHealthView;
import lombok.extern.slf4j.Slf4j;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.AmqpTimeoutException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.*;

/**
 * On-demand active heartbeat verifier that probes suspected-dead Workers when
 * passive heartbeat monitoring indicates a potential failure.
 *
 * <p>Under normal operation, the {@link ClusterHealthView} is updated passively
 * by incoming heartbeat messages. This verifier activates only when one or more
 * Workers have exceeded the {@code heartbeatTimeoutMs} window — it sends a
 * point-to-point PING message to each suspected Worker's dedicated verification
 * queue ({@code hotkey.verify.ping.{workerId}}) via Direct reply-to and awaits
 * a PONG response.
 *
 * <p><b>Failure escalation:</b> If cumulative failures across all suspected Workers
 * reach {@code degradeAfterFailures} and the cluster remains unhealthy, the health
 * view is marked as degraded via {@link ClusterHealthView#degraded}, triggering
 * graceful degradation behaviour (see ADR-0009). Workers that respond are restored
 * to the alive set; Workers that fail repeatedly are marked as stale and excluded
 * from majority-based health checks.
 *
 * <p><b>Thread safety:</b> The verification loop runs on a single daemon thread
 * ({@code hb-verifier}). The {@link ClusterHealthView} uses {@code ConcurrentHashMap}
 * internally, so concurrent heartbeat reception and verification are safe.
 *
 * @see ClusterHealthView
 * @see WorkerHeartbeatMessage
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class WorkerHeartbeatVerifier {


  private final RabbitTemplate rabbitTemplate;
  private final ClusterHealthView healthView;
  private final String appInstanceId;
  private final long verifyIntervalMs;
  private final long pingTimeoutMs;
  private final int degradeAfterFailures;
  private final ScheduledExecutorService scheduler;
  private final boolean ownsScheduler;
  private ScheduledFuture<?> verifyTask;

  private static final String QUEUE_VERIFY_PING_PREFIX = "hotkey.verify.ping.";

  /**
   * Creates a verifier with an internally owned single-thread scheduler.
   * <p>
   * This constructor is convenience for standalone use and tests. The internal
   * daemon thread is named {@code hb-verifier}. The caller must invoke
   * {@link #start()} to begin periodic verification and {@link #stop()} to
   * release the scheduler resources.
   *
   * @param rabbitTemplate        the RabbitMQ template for sending PING messages
   * @param healthView            the shared cluster health view to update on PONG/failure
   * @param appInstanceId         the local application instance ID, sent as PING metadata
   * @param verifyIntervalMs      fixed delay between verification cycles (milliseconds)
   * @param pingTimeoutMs         maximum time to wait for a PONG response (milliseconds)
   * @param degradeAfterFailures  number of failed verifications before marking a Worker stale
   *                              and potentially degrading the cluster
   */
  public WorkerHeartbeatVerifier(
    RabbitTemplate rabbitTemplate,
    ClusterHealthView healthView,
    String appInstanceId,
    long verifyIntervalMs,
    long pingTimeoutMs,
    int degradeAfterFailures
  ) {
    this(rabbitTemplate, healthView, appInstanceId, verifyIntervalMs, pingTimeoutMs, degradeAfterFailures,
      Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "hb-verifier");
        t.setDaemon(true);
        return t;
      }), true);
  }

  /**
   * Creates a verifier with a caller-supplied external scheduler.
   * <p>
   * The caller is responsible for managing the scheduler's lifecycle. When using
   * this constructor, {@link #stop()} will cancel the verification task but will
   * <em>not</em> shut down the scheduler. This allows sharing a single scheduler
   * across multiple listeners and verifiers.
   *
   * @param rabbitTemplate        the RabbitMQ template for sending PING messages
   * @param healthView            the shared cluster health view to update on PONG/failure
   * @param appInstanceId         the local application instance ID, sent as PING metadata
   * @param verifyIntervalMs      fixed delay between verification cycles (milliseconds)
   * @param pingTimeoutMs         maximum time to wait for a PONG response (milliseconds)
   * @param degradeAfterFailures  number of failed verifications before marking a Worker stale
   * @param scheduler             the external scheduler for running verification tasks;
   *                              will not be shut down by {@link #stop()}
   */
  public WorkerHeartbeatVerifier(
    RabbitTemplate rabbitTemplate,
    ClusterHealthView healthView,
    String appInstanceId,
    long verifyIntervalMs,
    long pingTimeoutMs,
    int degradeAfterFailures,
    ScheduledExecutorService scheduler
  ) {
    this(rabbitTemplate, healthView, appInstanceId, verifyIntervalMs, pingTimeoutMs, degradeAfterFailures,
      scheduler, false);
  }

  /**
   * Starts the periodic heartbeat verification at a fixed rate.
   *
   * <p>Every {@code verifyIntervalMs} milliseconds, iterates over all Workers in
   * {@link ClusterHealthView} that have exceeded {@code heartbeatTimeoutMs} without
   * a heartbeat and sends each a PING via Direct reply-to. On PONG, the Worker's
   * health record is restored; on timeout, the failure counter is incremented.
   * <p>
   * The verification runs on a daemon background thread. The task is idempotent:
   * subsequent calls to this method after the verifier is already running are
   * silently ignored.
   *
   * @see #verifySuspectedWorkers
   * @see #stop()
   */
  public void start() {
    if (verifyTask != null) {
      return;
    }
    try {
      verifyTask = scheduler.scheduleAtFixedRate(
        this::verifySuspectedWorkers,
        verifyIntervalMs,
        verifyIntervalMs,
        TimeUnit.MILLISECONDS
      );
    } catch (Exception e) {
      log.error("Failed to start heartbeat verifier scheduler; Worker liveness verification " +
          "will not run. Application continues but stale Workers may not be detected.", e);
    }
  }

  /**
   * Gracefully stops the periodic heartbeat verification.
   *
   * <p>Cancels the scheduled verification task and, if this verifier owns its
   * scheduler (created via the 6-argument constructor), shuts down the scheduler.
   * When an external scheduler is used (7-argument constructor), the scheduler is
   * left running for the caller to manage.
   *
   * <p>After {@code stop()}, the verifier can be restarted by calling {@link #start()}
   * again.
   */
  public void stop() {
    if (verifyTask != null) {
      verifyTask.cancel(false);
    }
    if (ownsScheduler) {
      scheduler.shutdown();
    }
  }

  /**
   * Iterates over all Workers registered in {@link ClusterHealthView} that are
   * <em>not</em> currently marked alive, sends each a PING via Direct reply-to,
   * and updates the health view based on the response.
   *
   * <p>If the cluster is healthy (majority of Workers alive), the verification
   * is skipped entirely — no need for active probing. When cumulative failures
   * across all suspected Workers reach {@code degradeAfterFailures} and the cluster
   * remains unhealthy, the cluster is marked as degraded.
   *
   * <p>This method is called periodically by the scheduled task and is also
   * safe to invoke manually for testing.
   */
  public void verifySuspectedWorkers() {
    try {
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
    } catch (Exception e) {
      log.error("Scheduled verifySuspectedWorkers failed", e);
    }
  }

  /**
   * Sends a point-to-point PING message to the given Worker's dedicated verification
   * queue ({@code hotkey.verify.ping.{workerId}}) and waits for a PONG response
   * via AMQP Direct reply-to ({@code amq.rabbitmq.reply-to}).
   *
   * <p>The PING carries the application instance ID in the
   * {@link io.github.hyshmily.hotkey.constants.HotKeyConstants#AMQP_HEADER_VERIFY_APP_INSTANCE} header so the Worker
   * can identify the requester. The reply timeout is set to {@code pingTimeoutMs}
   * on the RabbitTemplate before each call.
   *
   * @param workerId the Worker to ping; must not be null
   * @return {@code true} if a non-null PONG response was received within
   *         {@code pingTimeoutMs}; {@code false} if the request timed out
   *         ({@link AmqpTimeoutException}) or the queue is unreachable
   */
  public boolean sendPingAndWaitPong(String workerId) {
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
