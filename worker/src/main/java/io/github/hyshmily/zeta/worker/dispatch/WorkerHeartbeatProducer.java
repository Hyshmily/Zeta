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
package io.github.hyshmily.zeta.worker.dispatch;

import static io.github.hyshmily.zeta.constants.ZetaConstants.Routing.KEY_HEARTBEAT;

import io.github.hyshmily.zeta.detection.ZetaBayesianSM;
import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatMessage;
import io.github.hyshmily.zeta.util.ZetaThreadFactory;
import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.DependsOn;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Enhanced Worker heartbeat sender.
 *
 * <p>Replaces the old ping-only heartbeat approach with
 * structured heartbeats containing epoch, decisionVersionHwm, loadFactor,
 * and readyToServe flags. Broadcast on the dedicated heartbeat exchange.
 *
 * <p>Epoch is persisted to Redis (fallback to local file), incremented
 * atomically on each process start for restart detection by Apps.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
@DependsOn("rabbitAdmin")
@SuppressWarnings("SpringDependsOnUnresolvedBeanInspection")
public class WorkerHeartbeatProducer {

  /** RabbitMQ template for publishing heartbeat messages. */
  private final RabbitTemplate rabbitTemplate;
  /** Target topic exchange for heartbeat messages. */
  private final String heartbeatExchange;
  /** Unique identity of this Worker node. */
  private final String workerId;
  /** State machine providing config-gossip fields (confirm/cool/grace counts). */
  private final ZetaBayesianSM stateMachine;
  /** Broadcaster for reading the current decision version watermark. */
  private final WorkerBroadcaster broadcaster;
  /** Monotonically increasing epoch, persisted across restarts. */
  private final long epoch;
  /** JVM start timestamp used for the ready-to-serve grace period. */
  private final long startTime;
  /** Scheduler for periodic heartbeat sends. */
  private final ScheduledExecutorService scheduler;
  /** Whether this instance owns the scheduler (self-created). */
  private final boolean ownsScheduler;
  /** Shared monotonic counter for config-change timestamps embedded in heartbeats. */
  private final AtomicLong configTimestampCounter;

  /** Interval between consecutive heartbeat sends (milliseconds). */
  private final long pingIntervalMs;

  private final SnowflakeIdGenerator snowflakeIdGenerator;
  /** Handle for the scheduled heartbeat task. */
  private ScheduledFuture<?> heartbeatTask;

  private static final String EPOCH_REDIS_KEY_PREFIX = "zeta:worker:epoch:";

  /**
   * Creates a new heartbeat producer with a shared external scheduler.
   *
   * @param rabbitTemplate         the RabbitMQ template for publishing heartbeat messages
   * @param heartbeatExchange      the target topic exchange for heartbeat messages
   * @param workerId               unique identity of this Worker node
   * @param stateMachine           the state machine providing config-gossip fields
   * @param broadcaster            the broadcaster for reading the current decision version watermark
   * @param configTimestampCounter the shared monotonic counter for config-change timestamps
   * @param redisConnectionFactory the Redis connection factory for epoch initialization
   * @param pingIntervalMs         interval between consecutive heartbeat sends (milliseconds)
   * @param scheduler              the shared external scheduler for periodic heartbeat sends
   */
  @SuppressWarnings("all")
  public WorkerHeartbeatProducer(
    RabbitTemplate rabbitTemplate,
    String heartbeatExchange,
    String workerId,
    ZetaBayesianSM stateMachine,
    WorkerBroadcaster broadcaster,
    AtomicLong configTimestampCounter,
    RedisConnectionFactory redisConnectionFactory,
    long pingIntervalMs,
    ScheduledExecutorService scheduler,
    SnowflakeIdGenerator snowflakeIdGenerator
  ) {
    this(
      rabbitTemplate,
      heartbeatExchange,
      workerId,
      stateMachine,
      broadcaster,
      initEpoch(workerId, redisConnectionFactory),
      System.currentTimeMillis(),
      scheduler,
      false,
      configTimestampCounter,
      pingIntervalMs,
      snowflakeIdGenerator
    );
  }

  /**
   * Creates a new heartbeat producer with an externally-computed epoch.
   * Used when the config layer already computed the epoch via {@link #initEpoch}.
   *
   * @param rabbitTemplate         the RabbitMQ template for publishing heartbeat messages
   * @param heartbeatExchange      the target topic exchange for heartbeat messages
   * @param workerId               unique identity of this Worker node
   * @param stateMachine           the state machine providing config-gossip fields
   * @param broadcaster            the broadcaster for reading the current decision version watermark
   * @param configTimestampCounter the shared monotonic counter for config-change timestamps
   * @param epoch                  the externally-computed epoch value
   * @param pingIntervalMs         interval between consecutive heartbeat sends (milliseconds)
   * @param scheduler              the shared external scheduler for periodic heartbeat sends
   */
  @SuppressWarnings("all")
  public WorkerHeartbeatProducer(
    RabbitTemplate rabbitTemplate,
    String heartbeatExchange,
    String workerId,
    ZetaBayesianSM stateMachine,
    WorkerBroadcaster broadcaster,
    AtomicLong configTimestampCounter,
    long epoch,
    long pingIntervalMs,
    ScheduledExecutorService scheduler,
    SnowflakeIdGenerator snowflakeIdGenerator
  ) {
    this(
      rabbitTemplate,
      heartbeatExchange,
      workerId,
      stateMachine,
      broadcaster,
      epoch,
      System.currentTimeMillis(),
      scheduler,
      false,
      configTimestampCounter,
      pingIntervalMs,
      snowflakeIdGenerator
    );
  }

  /**
   * Factory method for tests: creates a heartbeat producer with an internally-owned
   * scheduler and initialises the epoch from the local file fallback (no Redis
   * dependency).
   *
   * @param rabbitTemplate         the RabbitMQ template for publishing heartbeat messages
   * @param heartbeatExchange      the target topic exchange for heartbeat messages
   * @param workerId               unique identity of this Worker node
   * @param stateMachine           the state machine providing config-gossip fields
   * @param broadcaster            the broadcaster for reading the current decision version watermark
   * @param configTimestampCounter the shared monotonic counter for config-change timestamps
   * @param pingIntervalMs         interval between consecutive heartbeat sends (milliseconds)
   * @return a new heartbeat producer ready for testing
   */
  public static WorkerHeartbeatProducer forTesting(
    RabbitTemplate rabbitTemplate,
    String heartbeatExchange,
    String workerId,
    ZetaBayesianSM stateMachine,
    WorkerBroadcaster broadcaster,
    AtomicLong configTimestampCounter,
    long pingIntervalMs,
    SnowflakeIdGenerator snowflakeIdGenerator
  ) {
    long epoch = initEpochFromLocalFile(workerId);
    var scheduler = Executors.newSingleThreadScheduledExecutor(new ZetaThreadFactory("zeta-hb-producer"));
    return new WorkerHeartbeatProducer(
      rabbitTemplate,
      heartbeatExchange,
      workerId,
      stateMachine,
      broadcaster,
      epoch,
      System.currentTimeMillis(),
      scheduler,
      true,
      configTimestampCounter,
      pingIntervalMs,
      snowflakeIdGenerator
    );
  }

  public static long initEpoch(String workerId, RedisConnectionFactory redisConnectionFactory) {
    if (redisConnectionFactory == null) {
      log.info("RedisConnectionFactory is null, using local file fallback for epoch initialisation");
      return initEpochFromLocalFile(workerId);
    }
    StringRedisTemplate template = new StringRedisTemplate(redisConnectionFactory);
    template.afterPropertiesSet();
    return doInitEpoch(template, workerId);
  }

  /**
   * Initializes the epoch by atomically incrementing a Redis counter via
   * {@code INCR}.  Uses a single atomic Redis command (no read-modify-write
   * race) so concurrent Workers with the same {@code workerId} always
   * receive distinct epoch values.
   *
   * <p>Falls back to a local file if Redis is unavailable.
   *
   * @param redis the Redis template (created ad-hoc for this one-time operation)
   * @return the new epoch value for this Worker incarnation
   */
  public static long doInitEpoch(StringRedisTemplate redis, String workerId) {
    try {
      String key = EPOCH_REDIS_KEY_PREFIX + workerId;
      Long next = redis.opsForValue().increment(key);
      log.info("Epoch initialized via Redis: workerId={}, epoch={}", workerId, next);
      return next != null ? next : initEpochFromLocalFile(workerId);
    } catch (Exception e) {
      log.warn("Redis unavailable for epoch, using local file fallback", e);
      return initEpochFromLocalFile(workerId);
    }
  }

  /**
   * Fallback epoch initialization using a local temp file when Redis is
   * unavailable.  Reads the previous value, increments, and writes back.
   *
   * <p>If both Redis and local file fallback fail, falls back to
   * {@code System.currentTimeMillis()} combined with a random jitter as a
   * last resort epoch value (cross-machine collision risk is minimised by
   * the jitter).  The combined value is monotonically increasing across
   * calls within the same JVM.
   *
   * @return the new epoch value, or a timestamp-based value on complete failure
   */
  public static long initEpochFromLocalFile(String workerId) {
    try {
      Path path = Path.of(System.getProperty("java.io.tmpdir"), "zeta-epoch-" + workerId);
      long next = 1;
      if (Files.exists(path)) {
        String content = Files.readString(path);
        next = Long.parseLong(content.trim()) + 1;
      }
      Files.writeString(path, String.valueOf(next));
      return next;
    } catch (Exception e) {
      long epoch = System.currentTimeMillis() * 1000L + ThreadLocalRandom.current().nextInt(1000);
      log.error("Epoch local file fallback failed, using timestamp with jitter: epoch={}", epoch, e);
      return epoch;
    }
  }

  /**
   * Computes the current CPU load factor (0.0 – 1.0) via the platform MXBean.
   *
   * @return CPU load clamped to [0, 1], or 0.0 if unavailable
   */
  private double computeLoadFactor() {
    double cpuLoad = 0.0;
    try {
      OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
      if (osBean instanceof com.sun.management.OperatingSystemMXBean sunOsBean) {
        cpuLoad = sunOsBean.getCpuLoad();
      }
    } catch (Exception e) {
      log.warn("Failed to compute CPU load factor; defaulting to 0.0", e);
    }
    return Math.max(0.0, Math.min(1.0, cpuLoad));
  }

  /**
   * Whether this Worker is ready to serve hot-key detection.
   * Returns {@code true} after a 3-second startup grace period or as soon as
   * the state machine has tracked at least one key.
   *
   * @return {@code true} if the Worker considers itself ready
   */
  private boolean isReadyToServe() {
    long uptime = System.currentTimeMillis() - startTime;
    return uptime > 3000 || stateMachine.getTrackedKeys() > 0;
  }

  /**
   * Starts the periodic heartbeat sender at the configured ping interval.
   *
   * <p>First heartbeat is delayed by {@code pingIntervalMs} to allow the
   * RabbitMQ connection and exchange declarations (RabbitAdmin) to complete,
   * preventing channel-level NOT_FOUND errors when the heartbeat exchange
   * has not yet been declared.
   */
  @PostConstruct
  public void start() {
    try {
      heartbeatTask = scheduler.scheduleAtFixedRate(
        this::sendHeartbeat,
        pingIntervalMs,
        pingIntervalMs,
        TimeUnit.MILLISECONDS
      );
    } catch (Exception e) {
      log.error(
        "Failed to start heartbeat scheduler; Worker heartbeat will not be sent. " +
          "Application continues but App instances may mark this Worker as dead.",
        e
      );
    }
  }

  /**
   * Gracefully stops the heartbeat sender.
   *
   * <p>Cancels the scheduled task; shuts down the scheduler only if owned.
   */
  @PreDestroy
  public void stop() {
    if (heartbeatTask != null) {
      heartbeatTask.cancel(false);
    }
    if (ownsScheduler) {
      scheduler.shutdown();
    }
  }

  /**
   * Builds and sends a single heartbeat message containing epoch, decision
   * version watermark, load factor, ready-to-serve flag, config fingerprint,
   * and state-machine config gossip fields.
   *
   * <p>Published to the configured heartbeat exchange with routing key
   * {@code heartbeat.<workerId>}.  Silently drops if the channel or
   * connection is unavailable (fire-and-forget).
   */
  void sendHeartbeat() {
    try {
      WorkerHeartbeatMessage hb = new WorkerHeartbeatMessage(
        snowflakeIdGenerator.nextId(),
        workerId,
        epoch,
        broadcaster.getCurrentDecisionVersion(),
        computeLoadFactor(),
        isReadyToServe(),
        stateMachine.getConfirmCount(),
        stateMachine.getCoolCount(),
        stateMachine.getPreCoolGraceCount(),
        configTimestampCounter.get()
      );
      rabbitTemplate.send(heartbeatExchange, KEY_HEARTBEAT + workerId, hb.toMessage());
    } catch (Exception e) {
      log.error("Scheduled sendHeartbeat failed", e);
    }
  }
}
