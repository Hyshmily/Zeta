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

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.*;
import static io.github.hyshmily.hotkey.util.TimeSource.currentTimeMillis;

import io.github.hyshmily.hotkey.sharding.ClusterHealthView;
import io.github.hyshmily.hotkey.util.HotKeyThreadFactory;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpTimeoutException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * On-demand active heartbeat verifier that probes suspected-dead Workers when
 * passive heartbeat monitoring indicates a potential failure.
 *
 * <p>Under normal operation, the {@link ClusterHealthView} is updated passively
 * by incoming heartbeat messages. This verifier sends a point-to-point PING
 * message to each suspected Worker's dedicated verification queue
 * ({@code hotkey.verify.ping.{workerId}}) via Direct reply-to and awaits
 * a PONG response.
 *
 * <p>Workers that respond are restored to the alive set via
 * {@link ClusterHealthView#recordPong}; Workers that fail repeatedly are
 * marked as stale via {@link ClusterHealthView#markVerificationFailed} and
 * excluded from health-majority calculations.
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
  private final long verifyMaxBackoffMs;
  private final ScheduledExecutorService scheduler;
  private final ConcurrentHashMap<String, Long> nextVerifyTime = new ConcurrentHashMap<>();
  private final boolean ownsScheduler;
  private ScheduledFuture<?> verifyTask;

  private static final String QUEUE_VERIFY_PING_PREFIX = "hotkey.verify.ping.";
  private static final int MAX_BACKOFF_SHIFT = 30;
  private static final int MAX_RETRY = 5;

  /**
   * Parameter object for {@link WorkerHeartbeatVerifier} construction.
   */
  public record VerifierConfig(long verifyIntervalMs, long pingTimeoutMs, long verifyMaxBackoffMs) {}

  /**
   * Creates a verifier with an internally owned single-thread scheduler.
   * The internal daemon thread is named {@code hb-verifier}.
   */
  public WorkerHeartbeatVerifier(
    RabbitTemplate rabbitTemplate,
    ClusterHealthView healthView,
    String appInstanceId,
    VerifierConfig config
  ) {
    this(
      rabbitTemplate,
      healthView,
      appInstanceId,
      config.verifyIntervalMs,
      config.pingTimeoutMs,
      config.verifyMaxBackoffMs,
      Executors.newSingleThreadScheduledExecutor(new HotKeyThreadFactory("hotkey-hb-verifier")),
      true
    );
  }

  /**
   * Creates a verifier with a caller-supplied external scheduler.
   * The caller manages the scheduler lifecycle — {@link #stop()} cancels
   * the task but does not shut down the scheduler.
   */
  public WorkerHeartbeatVerifier(
    RabbitTemplate rabbitTemplate,
    ClusterHealthView healthView,
    String appInstanceId,
    VerifierConfig config,
    ScheduledExecutorService scheduler
  ) {
    this(
      rabbitTemplate,
      healthView,
      appInstanceId,
      config.verifyIntervalMs,
      config.pingTimeoutMs,
      config.verifyMaxBackoffMs,
      scheduler,
      false
    );
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
      verifyTask = scheduler.scheduleWithFixedDelay(
        this::verifySuspectedWorkers,
        verifyIntervalMs,
        verifyIntervalMs,
        TimeUnit.MILLISECONDS
      );
    } catch (Exception e) {
      log.error(
        "Failed to start heartbeat verifier scheduler; Worker liveness verification " +
          "will not run. Application continues but stale Workers may not be detected.",
        e
      );
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
   * <p>When cumulative failures across all suspected Workers reach
   * {@code degradeAfterFailures} and the cluster remains unhealthy, the cluster
   * is marked as degraded.
   *
   * <p>This method is called periodically by the scheduled task and is also
   * safe to invoke manually for testing.
   */
  public void verifySuspectedWorkers() {
    try {
      Set<String> suspected = healthView
        .getAllWorkerIds()
        .stream()
        .filter(id -> !healthView.getAliveWorkerIds().contains(id))
        .filter(id -> healthView.getVerifyFailures(id) < MAX_RETRY)
        .collect(Collectors.toSet());

      if (suspected.isEmpty()) {
        return;
      }

      for (String workerId : suspected) {
        Long skipUntil = nextVerifyTime.get(workerId);
        if (skipUntil != null && currentTimeMillis() < skipUntil) {
          log.trace("Worker {} in backoff, skip (remaining={}ms)", workerId, skipUntil - currentTimeMillis());
          continue;
        }

        boolean alive = sendPingAndWaitPong(workerId);
        if (!alive) {
          healthView.markVerificationFailed(workerId);

          int attempt = healthView.getVerifyFailures(workerId);
          if (attempt >= MAX_RETRY) {
            log.warn("Worker {} confirmed dead ({} failures), removing record", workerId, MAX_RETRY);
            healthView.removeRecord(workerId);
            nextVerifyTime.remove(workerId);
            continue;
          }

          long backoffMs = computeBackoffMs(attempt);

          nextVerifyTime.put(workerId, currentTimeMillis() + backoffMs);
          log.warn("Worker {} verification failed (attempt={}, backoff={}ms)", workerId, attempt, backoffMs);
        } else {
          nextVerifyTime.remove(workerId);
          healthView.recordPong(workerId);
        }

        if (!healthView.isClusterHealthy()) {
          log.warn("Cluster is unhealthy after verifying worker {}", workerId);
        }
      }
    } catch (Exception e) {
      log.error("Scheduled verifySuspectedWorkers failed", e);
    }
  }

  /**
   * <p>Uses a two-phase exponential strategy: <em>dense start, steep extension</em>.
   * First 3 attempts grow slowly (half the naive power) so early retries cluster
   * closely; attempts 4-5 grow aggressively (double the naive power) to spread
   * late retries further apart. Capped at {@code verifyMaxBackoffMs}.
   *
   * <table>
   *   <caption>Multiplier over {@code verifyIntervalMs}</caption>
   *   <tr><th>Attempt</th><th>Shift</th><th>Multiplier</th></tr>
   *   <tr><td>1</td><td>0</td><td>1×</td></tr>
   *   <tr><td>2</td><td>1</td><td>2×</td></tr>
   *   <tr><td>3</td><td>2</td><td>4×</td></tr>
   *   <tr><td>4</td><td>5</td><td>32×</td></tr>
   *   <tr><td>5</td><td>6</td><td>64×</td></tr>
   * </table>
   *
   * @param attempt the number of consecutive failures (1-based)
   * @return the backoff duration in milliseconds
   */
  protected long computeBackoffMs(int attempt) {
    int shift = attempt <= 3 ? (attempt - 1) : (attempt + 1);
    return Math.min(verifyMaxBackoffMs, verifyIntervalMs * (1L << Math.min(shift, MAX_BACKOFF_SHIFT)));
  }

  /**
   * Sends a point-to-point PING message to the given Worker's dedicated verification
   * queue ({@code hotkey.verify.ping.{workerId}}) and waits for a PONG response
   * via AMQP Direct reply-to ({@code amq.rabbitmq.reply-to}).
   *
   * <p>The PING carries the application instance ID in the
   * {@link io.github.hyshmily.hotkey.constants.HotKeyConstants#AMQP_HEADER_VERIFY_APP_INSTANCE} header so the Worker
   * can identify the requester. The reply timeout is set in the message properties.
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
