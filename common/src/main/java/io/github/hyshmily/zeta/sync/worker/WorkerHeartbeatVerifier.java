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
package io.github.hyshmily.zeta.sync.worker;

import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.*;
import static io.github.hyshmily.zeta.util.TimeSource.currentTimeMillis;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.util.ZetaThreadFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpTimeoutException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * On-demand active heartbeat verifier that probes suspected-dead Workers when
 * passive heartbeat monitoring indicates a potential failure.
 *
 * <p>Under normal operation, the {@link HealthView} is updated passively
 * by incoming heartbeat messages. This verifier sends a point-to-point PING
 * message to each suspected Worker's dedicated verification queue
 * ({@code zeta.verify.ping.{workerId}}) via Direct reply-to and awaits
 * a PONG response.
 *
 * <p>Workers that respond are restored to the alive set via
 * {@link HealthView#recordPong}; Workers that fail repeatedly are
 * marked as stale via {@link HealthView#markVerificationFailed} and
 * excluded from health-majority calculations.
 *
 * <p><b>Thread safety:</b> The verification loop runs on a single daemon thread
 * ({@code hb-verifier}). The {@link HealthView} uses {@code ConcurrentHashMap}
 * internally, so concurrent heartbeat reception and verification are safe.
 *
 * @see HealthView
 * @see WorkerHeartbeatMessage
 */
@Slf4j
@Internal
public class WorkerHeartbeatVerifier {

  private final RabbitTemplate rabbitTemplate;
  private final HealthView healthView;
  private final String appInstanceId;
  private final long verifyIntervalMs;
  private final long pingTimeoutMs;
  private final long verifyMaxBackoffMs;
  private ScheduledExecutorService scheduler;
  private final ConcurrentHashMap<String, Long> nextVerifyTime = new ConcurrentHashMap<>();
  private final boolean ownsScheduler;
  private final Executor probeExecutor;
  private ScheduledFuture<?> verifyTask;

  private static final String QUEUE_VERIFY_PING_PREFIX = "zeta.verify.ping.";
  private static final int MAX_BACKOFF_SHIFT = 30;
  private static final int MAX_RETRY = 5;

  /**
   * Parameter object for {@link WorkerHeartbeatVerifier} construction.
   */
  public record VerifierConfig(long verifyIntervalMs, long pingTimeoutMs, long verifyMaxBackoffMs) {}

  /**
   * Creates the shared probe executor for parallel Worker PINGs.
   *
   * <p>A fixed thread pool of 4 daemon threads ({@code zeta-hb-probe}) allows
   * up to 4 suspected Workers to be probed concurrently, bounding the
   * verification round duration to roughly {@code pingTimeoutMs × ceil(N / 4)}
   * instead of {@code pingTimeoutMs × N}.
   *
   * @return a new fixed thread pool for parallel probing
   */
  private static Executor createProbeExecutor() {
    return Executors.newFixedThreadPool(4, new ZetaThreadFactory("zeta-hb-probe"));
  }

  /**
   * Common constructor shared by the two public constructors.
   *
   * <p>Initialises the probe executor but does NOT start the periodic
   * verification loop — call {@link #start()} to begin.
   *
   * @param rabbitTemplate   used for sending PING messages
   * @param healthView       shared cluster health state
   * @param appInstanceId    identifies this app instance in PING headers
   * @param verifyIntervalMs interval between verification rounds
   * @param pingTimeoutMs    per-PING timeout
   * @param verifyMaxBackoffMs  max exponential-backoff delay
   * @param scheduler        executor for the verification task
   * @param ownsScheduler    whether to shut down the scheduler in {@link #stop()}
   */
  private WorkerHeartbeatVerifier(
    RabbitTemplate rabbitTemplate,
    HealthView healthView,
    String appInstanceId,
    long verifyIntervalMs,
    long pingTimeoutMs,
    long verifyMaxBackoffMs,
    ScheduledExecutorService scheduler,
    boolean ownsScheduler
  ) {
    this.rabbitTemplate = rabbitTemplate;
    this.healthView = healthView;
    this.appInstanceId = appInstanceId;
    this.verifyIntervalMs = verifyIntervalMs;
    this.pingTimeoutMs = pingTimeoutMs;
    this.verifyMaxBackoffMs = verifyMaxBackoffMs;
    this.scheduler = scheduler;
    this.ownsScheduler = ownsScheduler;
    this.probeExecutor = createProbeExecutor();
  }

  /**
   * Creates a verifier with an internally owned single-thread scheduler.
   * The internal daemon thread is named {@code hb-verifier}.
   */
  public WorkerHeartbeatVerifier(
    RabbitTemplate rabbitTemplate,
    HealthView healthView,
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
      Executors.newSingleThreadScheduledExecutor(new ZetaThreadFactory("zeta-hb-verifier")),
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
    HealthView healthView,
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
   * {@link HealthView} that have exceeded {@code heartbeatTimeoutMs} without
   * a heartbeat and sends each a PING via Direct reply-to. On PONG, the Worker's
   * health reportToWorker is restored; on timeout, the failure counter is incremented.
   * <p>
   * The verification runs on a daemon background thread. The task is idempotent:
   * subsequent calls to this method after the verifier is already running are
   * silently ignored.
   *
   * <p><b>Restart safety:</b> If the scheduler was shut down by a previous
   * {@link #stop()} call and this verifier owns its scheduler, a new scheduler
   * is created automatically. This guarantees the verifier is always
   * restartable per its lifecycle contract.
   *
   * @see #verifySuspectedWorkers
   * @see #stop()
   */
  public synchronized void start() {
    if (verifyTask != null) {
      return;
    }
    if (ownsScheduler && (scheduler == null || scheduler.isShutdown())) {
      scheduler = Executors.newSingleThreadScheduledExecutor(new ZetaThreadFactory("zeta-hb-verifier"));
      log.info("Re-created heartbeat verifier scheduler for restart");
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
   * <p>Cancels the scheduled verification task (setting {@code verifyTask = null}
   * to allow re-scheduling on restart), and, if this verifier owns its scheduler
   * (created via the 6-argument constructor), shuts down the scheduler.
   * When an external scheduler is used (7-argument constructor), the scheduler is
   * left running for the caller to manage.
   *
   * <p>After {@code stop()}, the verifier can be restarted by calling {@link #start()}
   * again. See {@link #start()} for restart-safety details.
   */
  public void stop() {
    if (verifyTask != null) {
      verifyTask.cancel(false);
      verifyTask = null;
    }
    if (ownsScheduler && scheduler != null) {
      scheduler.shutdown();
    }
  }

  /**
   * Iterates over all Workers registered in {@link HealthView} that are
   * <em>not</em> currently marked alive, sends each a PING via Direct reply-to,
   * and updates the health view based on the response.
   *
   * <p>Probes are executed <b>in parallel</b> via a dedicated thread pool
   * ({@code probeExecutor}, 4 threads) using {@link CompletableFuture#allOf}.
   * This avoids the serial bottleneck of probing N suspected Workers
   * sequentially (N × {@code pingTimeoutMs} per round).
   *
   * <p>Workers in exponential backoff (see {@link #nextVerifyTime}) are
   * skipped before being added to the probe batch.
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

      List<CompletableFuture<Void>> futures = new ArrayList<>();
      for (String workerId : suspected) {
        Long skipUntil = nextVerifyTime.get(workerId);
        if (skipUntil != null && currentTimeMillis() < skipUntil) {
          log.trace("Worker {} in backoff, skip (remaining={}ms)", workerId, skipUntil - currentTimeMillis());
          continue;
        }

        futures.add(CompletableFuture.runAsync(() -> probeWorker(workerId), probeExecutor));
      }

      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    } catch (Exception e) {
      log.error("Scheduled verifySuspectedWorkers failed", e);
    }
  }

  /**
   * Probes a single suspected Worker by sending a PING and processing the result.
   *
   * <p>On success (PONG received), the Worker is restored to the alive set
   * via {@link HealthView#recordPong}. On failure, the failure counter is
   * incremented and exponential backoff is scheduled. Workers that exceed
   * {@link #MAX_RETRY} consecutive failures are removed from the health view.
   *
   * <p>This method is designed to run on the shared {@link #probeExecutor}
   * thread pool, enabling concurrent probing of multiple Workers.
   *
   * @param workerId the Worker to probe; must not be null
   */
  private void probeWorker(String workerId) {
    try {
      boolean alive = sendPingAndWaitPong(workerId);
      if (!alive) {
        healthView.markVerificationFailed(workerId);

        int attempt = healthView.getVerifyFailures(workerId);
        if (attempt >= MAX_RETRY) {
          log.warn("Worker {} confirmed dead ({} failures), removing reportToWorker", workerId, MAX_RETRY);
          healthView.removeRecord(workerId);
          nextVerifyTime.remove(workerId);
          return;
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
    } catch (Exception e) {
      log.error("Failed to probe worker {}", workerId, e);
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
   * queue ({@code zeta.verify.ping.{workerId}}) and waits for a PONG response
   * via AMQP Direct reply-to ({@code amq.rabbitmq.reply-to}).
   *
   * <p>The PING carries the application instance ID in the
   * {@link ZetaConstants.Amqp#HEADER_VERIFY_APP_INSTANCE} header so the Worker
   * can identify the requester. The reply timeout is set in the message properties.
   *
   * @param workerId the Worker to ping; must not be null
   * @return {@code true} if a non-null PONG response was received within
   *         {@code pingTimeoutMs}; {@code false} if the request timed out
   *         ({@link AmqpTimeoutException}) or the queue is unreachable
   */
  public boolean sendPingAndWaitPong(String workerId) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_VERIFY_TYPE, HEADER_VERIFY_PING);
    props.setHeader(HEADER_VERIFY_APP_INSTANCE, appInstanceId);
    props.setReplyTo("amq.rabbitmq.reply-to");

    Message ping = new Message(new byte[0], props);

    try {
      Message pong = rabbitTemplate.sendAndReceive("", QUEUE_VERIFY_PING_PREFIX + workerId, ping);
      return pong != null;
    } catch (AmqpTimeoutException e) {
      return false;
    }
  }
}
