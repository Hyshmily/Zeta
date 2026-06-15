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
package io.github.hyshmily.hotkey.worker.config;
import lombok.extern.slf4j.Slf4j;

import io.github.hyshmily.hotkey.constants.HotKeyConstants;
import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.HeavyKeeper;
import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.TopK;
import io.github.hyshmily.hotkey.util.InstanceIdGenerator;
import io.github.hyshmily.hotkey.worker.detection.GlobalQpsEstimator;
import io.github.hyshmily.hotkey.worker.detection.SlidingWindowDetector;
import io.github.hyshmily.hotkey.worker.detection.ThresholdLearner;
import io.github.hyshmily.hotkey.worker.detection.TopKValidator;
import io.github.hyshmily.hotkey.worker.dispatch.VerifyConsumer;
import io.github.hyshmily.hotkey.worker.dispatch.WorkerBroadcaster;
import io.github.hyshmily.hotkey.worker.dispatch.WorkerHeartbeatProducer;
import io.github.hyshmily.hotkey.worker.ingest.ReportConsumer;
import io.github.hyshmily.hotkey.worker.persistence.TopKPersistService;
import jakarta.annotation.PostConstruct;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Auto‑configuration for the <b>hot‑key Worker</b>.
 *
 * <p>Activates when {@code hotkey.worker.enabled=true} and RabbitMQ is on the
 * classpath.  The Worker consumes per‑key access counts reported by application
 * instances, applies a sliding‑window + state‑machine pipeline, and broadcasts
 * HOT / COOL decisions back to every instance.
 *
 * <h3>Provisioned beans</h3>
 * <ul>
 *   <li>{@link SlidingWindowDetector} – sliding‑window counter.</li>
 *   <li>{@link HotKeyStateMachine} – per‑key lifecycle state machine.</li>
 *   <li>{@link TopK} (worker‑scoped) – global frequency estimator for
 *       Top‑K cross‑validation and pre‑warming.</li>
 *   <li>{@link TopKValidator} – periodic Top‑K inspection that pre‑warms
 *       stable hot keys.</li>
 *   <li>{@link ReportConsumer} – AMQP listener that drives the pipeline.</li>
 *   <li>{@link WorkerBroadcaster} – publishes HOT / COOL broadcasts.</li>
 *   <li>RabbitMQ topology ({@code reportExchange}, shard‑specific
 *       {@code reportQueue}, binding).</li>
 *   <li>Scheduled tasks for stale‑state eviction and Top‑K validation.</li>
 * </ul>
 */
@AutoConfiguration
@ConditionalOnClass(name = "org.springframework.amqp.rabbit.core.RabbitTemplate")
@ConditionalOnProperty(prefix = "hotkey.worker", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(WorkerProperties.class)
@EnableScheduling
@RequiredArgsConstructor
@Slf4j
public class WorkerAutoConfiguration {


  private final WorkerProperties properties;

  /**
   * Worker node identity — auto-generated once per JVM.
   *
   * <p>Used in queue names ({@code hotkey.worker.config.<nodeId>}) and heartbeat
   * messages to identify this Worker instance uniquely.
   */
  private final String nodeId = InstanceIdGenerator.get();

  /**
   * Worker TopK snapshot service that persists the current hot-key list to
   * Redis and restores it on startup.
   *
   * <p>Only active when {@code hotkey.worker.persistence.enabled=true}.
   */
  @Bean
  @ConditionalOnProperty(prefix = "hotkey.worker.persistence", name = "enabled", havingValue = "true")
  public TopKPersistService topKPersistService(
    @Qualifier("workerTopK") TopK workerTopK,
    StringRedisTemplate redisTemplate,
    WorkerProperties properties
  ) {
    return new TopKPersistService(
      workerTopK,
      redisTemplate,
      properties.getRouting().getAppName(),
      nodeId,
      properties.getPersistence()
    );
  }

  /**
   * Schedules periodic TopK snapshot persistence. The returned placeholder bean
   * keeps the task alive in the context.
   */
  @Bean
  @ConditionalOnBean(TopKPersistService.class)
  public Object topKPersistTask(
    TopKPersistService service,
    WorkerProperties properties,
    @Qualifier("hotKeyScheduler") ScheduledExecutorService scheduler
  ) {
    long interval = properties.getPersistence().getPersistIntervalMs();
    scheduler.scheduleAtFixedRate(service::persistToRedis, interval, interval, TimeUnit.MILLISECONDS);
    return new Object();
  }

  /**
   * Validates that the sliding-window duration is evenly divisible by the
   * number of slices to avoid rounding inaccuracies.
   */
  @PostConstruct
  void validateWindowConfig() {
    if (properties.getSlidingWindow().getDurationMs() % properties.getSlidingWindow().getSlices() != 0) {
      log.warn(
        "windowDurationMs ({}) is not evenly divisible by windowSlices ({}). " +
          "This introduces rounding inaccuracies in window calculations.",
        properties.getSlidingWindow().getDurationMs(),
        properties.getSlidingWindow().getSlices()
      );
    }
  }

  /**
   * Creates the sliding‑window detector.
   *
   * <p>If an absolute hot threshold is configured via
   * {@code hotkey.worker.hot-threshold} it is used directly; otherwise the
   * value is set to {@code Long.MAX_VALUE} and the ratio‑based threshold
   * will be calculated later by the dynamic‑threshold logic (not shown here).
   */
  @Bean
  public SlidingWindowDetector slidingWindowDetector(WorkerProperties properties) {
    long threshold = properties.getThreshold().getHotThreshold();
    if (threshold <= 0) {
      threshold = Long.MAX_VALUE;
    }
    return new SlidingWindowDetector(
      properties.getSlidingWindow().getDurationMs(),
      properties.getSlidingWindow().getSlices(),
      threshold
    );
  }

  /**
   * Creates the per‑key lifecycle state machine.
   *
   * @param properties worker configuration properties providing confirm, cool, and
   *                   pre-cool grace window counts (converted from durations via
   *                   {@link WorkerProperties#getConfirmWindows()},
   *                   {@link WorkerProperties#getCoolWindows()}, and
   *                   {@link WorkerProperties#getPreCoolGraceWindows()})
   * @return a new {@link HotKeyStateMachine} instance
   */
  @Bean
  public HotKeyStateMachine hotKeyStateMachine(WorkerProperties properties) {
    return new HotKeyStateMachine(
      properties.getConfirmWindows(),
      properties.getCoolWindows(),
      properties.getPreCoolGraceWindows()
    );
  }

  /**
   * Worker‑scoped Top‑K instance backed by a {@link HeavyKeeper} sketch.
   *
   * <p>This is intentionally a <b>separate bean</b> qualified with
   * {@code "workerTopK"} so that it can be distinguished from any other
   * Top‑K instance that might be defined in the application context
   * (e.g. for dashboard queries).
   *
   * @param properties worker configuration providing HeavyKeeper algorithm parameters
   *                   (top-K count, width, depth, decay, minimum count)
   * @return a new {@link HeavyKeeper} instance qualified as {@code workerTopK}
   */
  @Bean
  public TopK workerTopK(WorkerProperties properties) {
    return new HeavyKeeper(
      properties.getHeavyKeeper().getTopK(),
      properties.getHeavyKeeper().getWidth(),
      properties.getHeavyKeeper().getDepth(),
      properties.getHeavyKeeper().getDecay(),
      properties.getHeavyKeeper().getMinCount()
    );
  }

  /**
   * Top‑K validator that periodically inspects the worker's Top‑K list
   * and pre‑warms keys that appear consistently.
   *
   * @param workerTopK  the worker-scoped HeavyKeeper sketch for frequency estimation
   * @param broadcaster the worker broadcaster used to emit pre-warm HOT decisions
   * @param properties  worker configuration providing TopK validation parameters
   *                    (pre-warm count and minimum consecutive appearances)
   * @return a new {@link TopKValidator} instance
   */
  @Bean
  public TopKValidator topKValidator(
    @Qualifier("workerTopK") TopK workerTopK,
    WorkerBroadcaster broadcaster,
    WorkerProperties properties
  ) {
    return new TopKValidator(
      workerTopK,
      broadcaster,
      properties.getTopKValidation().getPreWarmCount(),
      properties.getTopKValidation().getPreWarmMinAppearances()
    );
  }

  /**
   * Report consumer – the main AMQP entry point.
   *
   * <p>Injects the worker‑scoped Top‑K so that every consumed report also
   * feeds the frequency estimator.
   *
   * @param detector           the sliding-window detector for per-key access tracking
   * @param stateMachine       the per-key lifecycle state machine for HOT/COOL transitions
   * @param broadcaster        publishes HOT and COOL decisions to all application instances
   * @param topKValidator      pre-warm validator for cross-instance frequency-based confirmation
   * @param workerTopK         the worker-scoped HeavyKeeper sketch for frequency estimation
   * @param globalQpsEstimator the global QPS estimator tracking overall throughput
   * @return a new {@link ReportConsumer} instance
   */
  @Bean
  public ReportConsumer reportConsumer(
    SlidingWindowDetector detector,
    HotKeyStateMachine stateMachine,
    WorkerBroadcaster broadcaster,
    TopKValidator topKValidator,
    @Qualifier("workerTopK") TopK workerTopK,
    GlobalQpsEstimator globalQpsEstimator
  ) {
    return new ReportConsumer(detector, stateMachine, broadcaster, topKValidator, workerTopK, globalQpsEstimator);
  }

  /**
   * Broadcasts HOT / COOL decisions to all application instances.
   *
   * @param rabbitTemplate         the RabbitMQ template used to publish messages
   * @param properties             worker configuration providing exchange and routing settings
   * @param stateMachine           the state machine providing config gossip fields for heartbeats
   * @param configTimestampCounter the shared monotonic counter for config-change timestamps
   * @return a new {@link WorkerBroadcaster} instance
   */
  @Bean
  public WorkerBroadcaster workerBroadcaster(
    RabbitTemplate rabbitTemplate,
    WorkerProperties properties,
    HotKeyStateMachine stateMachine,
    @Qualifier("configTimestampCounter") AtomicLong configTimestampCounter
  ) {
    return new WorkerBroadcaster(
      rabbitTemplate,
      properties.getMessaging().getBroadcastExchange(),
      properties.getRouting().getAppName(),
      stateMachine,
      configTimestampCounter
    );
  }

  /**
   * Direct exchange to which clients publish report messages.
   * Routing keys follow the pattern {@code report.<appName>.<nodeId>}.
   *
   * @param properties worker configuration providing the report exchange name
   * @return a durable, non-auto-delete {@link DirectExchange}
   */
  @Bean
  public DirectExchange reportExchange(WorkerProperties properties) {
    return new DirectExchange(properties.getMessaging().getReportExchange(), true, false);
  }

  /**
   * Queue that this Worker binds to.
   * Queue name is {@code hotkey.report.<appName>.<nodeId>},
   * guaranteeing that a key always lands on the same Worker.
   *
   * <p>The queue has a 7-day idle expiry ({@code x-expires}) since shard queues
   * are fixed-count and long-lived.
   *
   * @param properties worker configuration providing the routing app name
   * @return a durable {@link Queue} with the shard-specific name
   */
  @Bean
  public Queue reportQueue(WorkerProperties properties) {
    String queueName = HotKeyConstants.QUEUE_PREFIX_REPORT + properties.getRouting().getAppName() + "." + nodeId;
    return QueueBuilder.durable(queueName)
        .withArgument("x-expires", 604_800_000)
        .build();
  }

  /**
   * Binds the report queue to the report exchange using the shard-specific
   * routing key {@code report.<appName>.<nodeId>}.
   *
   * @param reportQueue    the shard-specific report queue
   * @param reportExchange the report exchange
   * @param properties     worker configuration providing the routing app name
   * @return a {@link Binding} connecting the queue to the exchange
   */
  @Bean
  public Binding reportBinding(Queue reportQueue, DirectExchange reportExchange, WorkerProperties properties) {
    String routingKey = HotKeyConstants.ROUTING_KEY_REPORT + properties.getRouting().getAppName() + "." + nodeId;
    return BindingBuilder.bind(reportQueue).to(reportExchange).with(routingKey);
  }

  /**
   * Auto-delete queue for receiving heartbeat-based config updates from
   * peer Workers.
   *
   * <p>Each Worker declares its own queue named
   * {@code hotkey.worker.config.<nodeId>}, which is automatically removed
   * when the Worker disconnects.  The queue is bound to the
   * {@code heartbeatExchange} with routing key {@code heartbeat.*}.
   *
   * @return a non-durable, auto-delete {@link Queue} unique to this Worker node
   */
  @Bean
  public Queue workerConfigQueue() {
    return QueueBuilder.nonDurable("hotkey.worker.config." + nodeId).autoDelete().build();
  }

  /**
   * Binds the per-Worker config queue to the heartbeat exchange with the
   * routing key pattern {@code heartbeat.*}.
   *
   * @param workerConfigQueue the per-Worker config queue to bind
   * @param heartbeatExchange the heartbeat topic exchange
   * @return a {@link Binding} connecting the config queue to the heartbeat exchange
   */
  @Bean
  public Binding workerConfigBinding(Queue workerConfigQueue, TopicExchange heartbeatExchange) {
    return BindingBuilder.bind(workerConfigQueue).to(heartbeatExchange).with("heartbeat.*");
  }

  /**
   * Topic exchange for epoch-aware structured heartbeats from Workers.
   *
   * @param properties worker configuration providing the heartbeat exchange name
   * @return a durable, non-auto-delete {@link TopicExchange}
   */
  @Bean
  public TopicExchange heartbeatExchange(WorkerProperties properties) {
    return new TopicExchange(properties.getMessaging().getHeartbeatExchange(), true, false);
  }

  /**
   * Monotonically increasing counter for config-change timestamps.
   *
   * <p>Every {@code POST /actuator/worker/state} call increments this counter.
   * Receiving Workers compare their own counter against the incoming value;
   * only strictly newer configs are applied.
   *
   * @return a new {@link AtomicLong} initialised to {@code 0}
   */
  @Bean
  @Qualifier("configTimestampCounter")
  public AtomicLong configTimestampCounter() {
    return new AtomicLong(0);
  }

  /**
   * Periodically evicts stale keys from the sliding‑window detector and
   * the state machine.
   *
   * <p>The eviction threshold is set to {@code 2 * coolDurationMs},
   * giving keys a generous grace period after their last access before
   * their data structures are reclaimed.
   *
   * @param detector     the sliding-window detector whose idle keys will be evicted
   * @param stateMachine the state machine whose idle entries will be evicted
   * @param properties   worker configuration providing the cool duration for stale threshold
   * @return a new {@link EvictStaleTask} instance
   */
  @Bean
  public EvictStaleTask evictStaleTask(
    SlidingWindowDetector detector,
    HotKeyStateMachine stateMachine,
    WorkerProperties properties
  ) {
    return new EvictStaleTask(detector, stateMachine, properties);
  }

  /**
   * Scheduled task that evicts stale keys from the sliding-window detector
   * and state machine.
   */
  @RequiredArgsConstructor
  public static class EvictStaleTask {

    private final SlidingWindowDetector detector;
    private final HotKeyStateMachine stateMachine;
    private final WorkerProperties properties;

    /**
     * Evicts keys that have not been accessed within {@code 2 * coolDurationMs}.
     */
    @Scheduled(fixedDelayString = "${hotkey.worker.state-machine.evict-interval-ms:30000}")
    public void evictStale() {
      try {
        long staleAfterMs = properties.getStateMachine().getCoolDurationMs() * 2;
        detector.evictStale(staleAfterMs);
        stateMachine.evictStale(staleAfterMs);
        log.debug("Evicted stale Worker state: staleAfterMs={}", staleAfterMs);
      } catch (Exception e) {
        log.error("Scheduled evictStale failed", e);
      }
    }
  }

  /**
   * Periodically runs Top‑K cross‑validation to detect slow‑warming hot
   * keys that the sliding window might miss.
   *
   * <p>Interval is controlled by {@code hotkey.worker.topk-validation.validate-interval-ms}
   * (default 60 seconds).
   *
   * @param topKValidator the TopK validator whose {@link TopKValidator#validate()} will be scheduled
   * @return a new {@link TopKValidationTask} instance
   */
  @Bean
  public TopKValidationTask topKValidationTask(TopKValidator topKValidator) {
    return new TopKValidationTask(topKValidator);
  }

  /**
   * Scheduled task that inspects the Worker TopK and pre-warms stable hot keys.
   */
  @RequiredArgsConstructor
  public static class TopKValidationTask {

    private final TopKValidator topKValidator;

    /**
     * Runs TopK validation and pre-warm logic.
     */
    @Scheduled(fixedDelayString = "${hotkey.worker.topk-validation.validate-interval-ms:60000}")
    public void validate() {
      try {
        topKValidator.validate();
      } catch (Exception e) {
        log.error("Scheduled TopK validation failed", e);
      }
    }
  }

  /**
   * Global QPS estimator that tracks overall throughput across all keys in the
   * current shard using a sliding window.
   *
   * @param properties worker configuration properties (sliding-window duration
   *                   and slice count are extracted from here)
   * @return a new {@link GlobalQpsEstimator} instance
   */
  @Bean
  public GlobalQpsEstimator globalQpsEstimator(WorkerProperties properties) {
    return new GlobalQpsEstimator(
      properties.getSlidingWindow().getDurationMs(),
      properties.getSlidingWindow().getSlices()
    );
  }

  /**
   * Threshold learner that periodically recalculates the hot-key threshold
   * based on estimated global QPS and updates the sliding-window detector.
   *
   * @param estimator the global QPS estimator
   * @param detector  the sliding-window detector whose threshold will be
   *                  dynamically adjusted
   * @param properties worker configuration properties for threshold tuning
   *                   parameters (ratio, tolerance, learning period)
   * @return a new {@link ThresholdLearner} instance
   */
  @Bean
  public ThresholdLearner thresholdLearner(
    GlobalQpsEstimator estimator,
    SlidingWindowDetector detector,
    WorkerProperties properties
  ) {
    return new ThresholdLearner(estimator, detector, properties);
  }

  /**
   * Listens for heartbeat-based config updates from peer Workers and applies
   * them if the received config timestamp is newer than the local one.
   *
   * <p>On startup, waits up to 3 seconds for the first heartbeat to arrive.
   * If none is received, the Worker continues with the values from
   * {@link WorkerProperties}.
   *
   * @param stateMachine           the worker's state machine
   * @param configTimestampCounter the shared config-change timestamp counter
   * @return a new {@link WorkerConfigNegotiator} instance
   */
  @Bean
  public WorkerConfigNegotiator workerConfigNegotiator(
    HotKeyStateMachine stateMachine,
    @Qualifier("configTimestampCounter") AtomicLong configTimestampCounter
  ) {
    return new WorkerConfigNegotiator(stateMachine, configTimestampCounter, nodeId);
  }

  /**
   * Schedules periodic threshold recalculation using the {@link ThresholdLearner}.
   *
   * <p>The learner runs at the interval specified by
   * {@code hotkey.worker.global-qps-dynamic-threshold.recalculate-interval-ms}.
   * The returned placeholder bean keeps the scheduled task alive in the context.
   *
   * @param learner    the threshold learner to schedule
   * @param properties worker configuration for the recalculation interval
   * @param scheduler  the shared worker scheduler
   * @return a placeholder {@link Object} bean that keeps the scheduled task alive
   */
  @Bean
  public Object thresholdLearningTask(
    ThresholdLearner learner,
    WorkerProperties properties,
    @Qualifier("hotKeyScheduler") ScheduledExecutorService scheduler
  ) {
    scheduler.scheduleAtFixedRate(
      learner,
      properties.getGlobalQpsDynamicThreshold().getRecalculateIntervalMs(),
      properties.getGlobalQpsDynamicThreshold().getRecalculateIntervalMs(),
      TimeUnit.MILLISECONDS
    );
    return new Object(); // placeholder bean
  }

  /**
   * Auto-delete queue for on-demand verification requests (PING) from Apps.
   * Queue name: {@code hotkey.verify.ping.<nodeId>}.
   *
   * @return a non-durable, auto-delete {@link Queue} for PING/PONG verification
   */
  @Bean
  public Queue verifyPingQueue() {
    return QueueBuilder.nonDurable("hotkey.verify.ping." + nodeId).autoDelete().build();
  }

  /**
   * Handles PING requests from Apps, replies PONG via Direct reply-to.
   *
   * @param rabbitTemplate the RabbitMQ template used to send PONG responses
   * @return a new {@link VerifyConsumer} instance for this Worker node
   */
  @Bean
  public VerifyConsumer verifyConsumer(RabbitTemplate rabbitTemplate) {
    return new VerifyConsumer(rabbitTemplate, nodeId);
  }

  /**
   * Container for the verification ping queue (NONE ack, on-demand only).
   *
   * @param connectionFactory the RabbitMQ connection factory
   * @param verifyPingQueue   the auto-delete ping queue
   * @param verifyConsumer    the consumer whose {@link VerifyConsumer#handlePing} handles messages
   * @return a configured {@link SimpleMessageListenerContainer} with NONE acknowledgement mode
   */
  @Bean
  public SimpleMessageListenerContainer verifyPingContainer(
    ConnectionFactory connectionFactory,
    Queue verifyPingQueue,
    VerifyConsumer verifyConsumer
  ) {
    SimpleMessageListenerContainer c = new SimpleMessageListenerContainer(connectionFactory);
    c.setQueues(verifyPingQueue);
    c.setMessageListener(verifyConsumer::handlePing);
    c.setAcknowledgeMode(AcknowledgeMode.NONE);
    return c;
  }

  /**
   * Enhanced heartbeat producer that sends structured heartbeats with epoch,
   * load factor, decision-version watermark, and state-machine config gossip.
   *
   * <p>Replaces the old ping-only heartbeat approach. Uses its own internal scheduler.
   *
   * @param rabbitTemplate         the RabbitMQ template for publishing heartbeat messages
   * @param properties             worker configuration providing exchange and interval settings
   * @param stateMachine           the state machine providing config gossip fields
   * @param broadcaster            the broadcaster for reading the current decision version watermark
   * @param configTimestampCounter the shared monotonic counter for config-change timestamps
   * @return a new {@link WorkerHeartbeatProducer} instance
   */
  @Bean
  public WorkerHeartbeatProducer workerHeartbeatProducer(
    RabbitTemplate rabbitTemplate,
    WorkerProperties properties,
    HotKeyStateMachine stateMachine,
    WorkerBroadcaster broadcaster,
    RedisConnectionFactory redisConnectionFactory,
    @Qualifier("configTimestampCounter") AtomicLong configTimestampCounter,
    @Qualifier("hotKeyScheduler") ScheduledExecutorService scheduler
  ) {
    return new WorkerHeartbeatProducer(
      rabbitTemplate,
      properties.getMessaging().getHeartbeatExchange(),
      nodeId,
      stateMachine,
      broadcaster,
      configTimestampCounter,
      redisConnectionFactory,
      properties.getHeartbeat().getPingIntervalMs(),
      scheduler
    );
  }
}
