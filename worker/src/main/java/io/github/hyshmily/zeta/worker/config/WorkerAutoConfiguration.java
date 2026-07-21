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

import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.detection.ZetaStateMachine;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.HeavyKeeper;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.util.InstanceIdGenerator;
import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import io.github.hyshmily.zeta.worker.confidence.BayesianConfidenceEstimator;
import io.github.hyshmily.zeta.worker.confidence.ConfidenceEvaluator;
import io.github.hyshmily.zeta.worker.detection.*;
import io.github.hyshmily.zeta.worker.detection.impl.ZetaStateMachineImpl;
import io.github.hyshmily.zeta.worker.dispatch.VerifyConsumer;
import io.github.hyshmily.zeta.worker.dispatch.WorkerBroadcaster;
import io.github.hyshmily.zeta.worker.dispatch.WorkerHeartbeatProducer;
import io.github.hyshmily.zeta.worker.ingest.ReportConsumer;
import io.github.hyshmily.zeta.worker.persistence.TopKPersistService;
import jakarta.annotation.PostConstruct;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Auto‑configuration for the <b>hot‑key Worker</b>.
 *
 * <p>Activates when {@code zeta.worker.enabled=true} and RabbitMQ is on the
 * classpath.  The Worker consumes per‑key access counts reported by application
 * instances, applies a sliding‑window + state‑machine pipeline, and broadcasts
 * HOT / COOL decisions back to every instance.
 *
 * <h2>Provisioned beans</h2>
 * <ul>
 *   <li>{@link SlidingWindowDetector} – sliding‑window counter.</li>
 *   <li>{@link ZetaStateMachine} – per‑key lifecycle state machine.</li>
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
 *
 * Default constructor.
 */
@Configuration
@ConditionalOnClass(name = "org.springframework.amqp.rabbit.core.RabbitTemplate")
@ConditionalOnProperty(prefix = "zeta.worker", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(WorkerProperties.class)
@EnableScheduling
@RequiredArgsConstructor
@Slf4j
public class WorkerAutoConfiguration {

  private final WorkerProperties properties;

  /**
   * Worker node identity — auto-generated once per JVM.
   *
   * <p>Used in queue names ({@code zeta.worker.config.<nodeId>}) and heartbeat
   * messages to identify this Worker instance uniquely.
   */
  private String nodeId;

  @PostConstruct
  void initInstanceId() {
    String envId = System.getenv("INSTANCE_ID");
    if (envId != null && !envId.isBlank()) {
      InstanceIdGenerator.setOverride(envId);
    }
    this.nodeId = InstanceIdGenerator.get();
  }

  /**
   * Worker TopK snapshot service that persists the current hot-key list to
   * Redis and restores it on startup.
   *
   * <p>Only active when {@code zeta.worker.persistence.enabled=true}.
   *
   * @param workerTopK  the worker-scoped HeavyKeeper sketch to persist
   * @param redisTemplate the Redis template used for persistence
   * @param properties  worker configuration providing persistence settings
   * @return a new {@link TopKPersistService} instance
   */
  @Bean
  @ConditionalOnProperty(prefix = "zeta.worker.persistence", name = "enabled", havingValue = "true")
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
   *
   * @param service    the TopK persistence service whose {@code persistToRedis} is scheduled
   * @param properties worker configuration providing the persistence interval
   * @param scheduler  the shared worker scheduler
   * @return a placeholder {@link Object} bean that keeps the scheduled task alive
   */
  @Bean
  @ConditionalOnBean(TopKPersistService.class)
  public Object topKPersistTask(
    TopKPersistService service,
    WorkerProperties properties,
    @Qualifier("hotKeyScheduler") ScheduledExecutorService scheduler
  ) {
    long interval = properties.getPersistence().getPersistIntervalMs();
    try {
      scheduler.scheduleAtFixedRate(service::persistToRedis, interval, interval, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      log.error("Failed to schedule TopK persistence task; Worker TopK snapshots will not be persisted.", e);
    }
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
   * {@code zeta.worker.hot-threshold} it is used directly; otherwise the
   * value is set to {@code Long.MAX_VALUE} and the ratio‑based threshold
   * will be calculated later by the dynamic‑threshold logic (not shown here).
   *
   * @param properties worker configuration providing sliding-window parameters
   * @return a new {@link SlidingWindowDetector} instance
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
   * Bayesian confidence estimator using Normal-Normal conjugate model.
   *
   * @param properties worker configuration providing Bayesian prior and likelihood parameters
   * @return a new {@link BayesianConfidenceEstimator} instance
   */
  @Bean
  public BayesianConfidenceEstimator bayesianConfidenceEstimator(WorkerProperties properties) {
    WorkerProperties.Bayesian cfg = properties.getBayesian();
    return new BayesianConfidenceEstimator(cfg.getPriorMean(), cfg.getPriorStd(), cfg.getLikelihoodStd());
  }

  /**
   * Confidence evaluator facade that wraps the Bayesian estimator.
   *
   * @param estimator the Bayesian confidence estimator
   * @return a new {@link ConfidenceEvaluator} instance
   */
  @Bean
  public ConfidenceEvaluator confidenceEvaluator(BayesianConfidenceEstimator estimator) {
    return new ConfidenceEvaluator(estimator);
  }

  /**
   * Creates the per‑key lifecycle state machine with optional Bayesian confidence integration.
   *
   * @param properties        worker configuration properties providing confirm, cool, and
   *                          pre-cool grace window counts
   * @param confidenceEvaluator the Bayesian confidence evaluator injected into the state machine
   * @return a new {@link ZetaStateMachine} instance
   */
  @Bean
  public ZetaStateMachine hotKeyStateMachine(WorkerProperties properties, ConfidenceEvaluator confidenceEvaluator) {
    return new ZetaStateMachineImpl(
      properties.getConfirmWindows(),
      properties.getCoolWindows(),
      properties.getPreCoolGraceWindows(),
      confidenceEvaluator
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
   * Unified key evaluator that combines sliding-window detection, HeavyKeeper
   * frequency estimation, and Bayesian confidence evaluation into a single call.
   *
   * @param detector     the sliding-window detector
   * @param stateMachine the per-key lifecycle state machine with Bayesian integration
   * @param workerTopK   the worker-scoped HeavyKeeper sketch
   * @return a new {@link KeyEvaluator} instance
   */
  @Bean
  public KeyEvaluator keyEvaluator(
    SlidingWindowDetector detector,
    ZetaStateMachine stateMachine,
    @Qualifier("workerTopK") TopK workerTopK
  ) {
    return new KeyEvaluator(detector, stateMachine, workerTopK);
  }

  /**
   * Report consumer – the main AMQP entry point.
   *
   * <p>Injects the worker‑scoped Top‑K so that every consumed reportToWorker also
   * feeds the frequency estimator.
   *
   * @param keyEvaluator       the unified key evaluator (sliding window + Bayesian)
   * @param broadcaster        publishes HOT and COOL decisions to all application instances
   * @param topKValidator      pre-warm validator for cross-instance frequency-based confirmation
   * @param workerTopK         the worker-scoped HeavyKeeper sketch for frequency estimation
   * @param globalQpsEstimator the global qps estimator tracking overall throughput
   * @param stateMachine       the per-key lifecycle state machine (retained for TopKValidator integration)
   * @return a new {@link ReportConsumer} instance
   */
  @Bean
  public ReportConsumer reportConsumer(
    KeyEvaluator keyEvaluator,
    WorkerBroadcaster broadcaster,
    TopKValidator topKValidator,
    @Qualifier("workerTopK") TopK workerTopK,
    GlobalQpsEstimator globalQpsEstimator,
    ZetaStateMachine stateMachine
  ) {
    return new ReportConsumer(keyEvaluator, broadcaster, topKValidator, workerTopK, globalQpsEstimator, stateMachine);
  }

  /**
   * Broadcasts HOT / COOL decisions to all application instances.
   *
   * @param rabbitTemplate         the RabbitMQ template used to publish messages
   * @param properties             worker configuration providing exchange and routing settings
   * @param nodeId                 the Worker's node identity, injected via {@code @Qualifier("workerNodeId")}
   * @param epochCounter           the Worker's epoch counter, injected via {@code @Qualifier("workerEpochCounter")}
   * @return a new {@link WorkerBroadcaster} instance
   */
  @Bean
  public WorkerBroadcaster workerBroadcaster(
    RabbitTemplate rabbitTemplate,
    WorkerProperties properties,
    @Qualifier("workerNodeId") String nodeId,
    @Qualifier("workerEpochCounter") AtomicLong epochCounter,
    SnowflakeIdGenerator snowflakeIdGenerator
  ) {
    return new WorkerBroadcaster(
      rabbitTemplate,
      properties.getMessaging().getBroadcastExchange(),
      properties.getRouting().getAppName(),
      nodeId,
      epochCounter,
      snowflakeIdGenerator
    );
  }

  /**
   * Fanout exchange for broadcasting HOT/COOL decisions to all application instances.
   *
   * <p>App instances bind their per-instance queues to this exchange to receive
   * decision broadcasts. Both app and worker declare this exchange so that it
   * exists regardless of startup order.
   *
   * @param properties worker configuration providing the broadcast exchange name
   * @return a durable, non-auto-delete {@link FanoutExchange}
   */
  @Bean
  public FanoutExchange broadcastExchange(WorkerProperties properties) {
    return new FanoutExchange(properties.getMessaging().getBroadcastExchange(), true, false);
  }

  /**
   * Direct exchange to which clients publish reportToWorker messages.
   * Routing keys follow the pattern {@code reportToWorker.<appName>.<nodeId>}.
   *
   * @param properties worker configuration providing the reportToWorker exchange name
   * @return a durable, non-auto-delete {@link DirectExchange}
   */
  @Bean
  public DirectExchange reportExchange(WorkerProperties properties) {
    return new DirectExchange(properties.getMessaging().getReportExchange(), true, false);
  }

  /**
   * Queue that this Worker binds to.
   * Queue name is {@code zeta.reportToWorker.<appName>.<nodeId>},
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
    String queueName = ZetaConstants.Routing.QUEUE_PREFIX_REPORT + properties.getRouting().getAppName() + "." + nodeId;
    return QueueBuilder.durable(queueName).withArgument("x-expires", 604_800_000).build();
  }

  /**
   * Binds the reportToWorker queue to the reportToWorker exchange using the shard-specific
   * routing key {@code reportToWorker.<appName>.<nodeId>}.
   *
   * @param reportQueue    the shard-specific reportToWorker queue
   * @param reportExchange the reportToWorker exchange
   * @param properties     worker configuration providing the routing app name
   * @return a {@link Binding} connecting the queue to the exchange
   */
  @Bean
  public Binding reportBinding(Queue reportQueue, DirectExchange reportExchange, WorkerProperties properties) {
    String routingKey = ZetaConstants.Routing.KEY_REPORT + properties.getRouting().getAppName() + "." + nodeId;
    return BindingBuilder.bind(reportQueue).to(reportExchange).with(routingKey);
  }

  /**
   * Auto-delete queue for receiving heartbeat-based config updates from
   * peer Workers.
   *
   * <p>Each Worker declares its own queue named
   * {@code zeta.worker.config.<nodeId>}, which is automatically removed
   * when the Worker disconnects.  The queue is bound to the
   * {@code heartbeatExchange} with routing key {@code heartbeat.*}.
   *
   * @return a non-durable, auto-delete {@link Queue} unique to this Worker node
   */
  @Bean
  public Queue workerConfigQueue() {
    return QueueBuilder.nonDurable("zeta.worker.config." + nodeId).autoDelete().build();
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
    return BindingBuilder.bind(workerConfigQueue).to(heartbeatExchange).with(ZetaConstants.Routing.KEY_HEARTBEAT + "*");
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
   * Exposes the Worker's node ID as a Spring bean for injection.
   *
   * @return the unique node identifier for this Worker instance
   */
  @Bean
  public String workerNodeId() {
    return nodeId;
  }

  /**
   * Monotonically increasing epoch counter for this Worker instance.
   *
   * <p>The epoch is incremented on every Worker restart via Redis {@code INCR}
   * and transmitted in both heartbeat and send messages.  Receiving apps use
   * the epoch to detect Worker restarts and unconditionally accept decisions
   * from a higher epoch (ADR-0010).
   *
   * <p>Both {@link WorkerHeartbeatProducer} and {@link WorkerBroadcaster}
   * share this single bean so that heartbeat epoch and broadcast epoch are
   * guaranteed identical.
   *
   * @param workerId the unique node identifier for this Worker instance
   * @param redis    the Redis connection factory for epoch initialization
   * @return a new {@link AtomicLong} initialised to the current epoch value
   */
  @Bean
  public AtomicLong workerEpochCounter(@Qualifier("workerNodeId") String workerId, RedisConnectionFactory redis) {
    long epoch = WorkerHeartbeatProducer.initEpoch(workerId, redis);
    return new AtomicLong(epoch);
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
  public AtomicLong configTimestampCounter() {
    return new AtomicLong(0);
  }

  /**
   * Periodically evicts stale keys from the sliding‑window detector,
   * state machine, and CV history.
   *
   * <p>The eviction threshold is set to {@code 2 * coolDurationMs},
   * giving keys a generous grace period after their last access before
   * their data structures are reclaimed.
   *
   * @param detector     the sliding-window detector whose idle keys will be evicted
   * @param stateMachine the state machine whose idle entries will be evicted
   * @param keyEvaluator the key evaluator whose CV history will be evicted
   * @param properties   worker configuration providing the cool duration for stale threshold
   * @return a new {@link EvictStaleTask} instance
   */
  @Bean
  public EvictStaleTask evictStaleTask(
    SlidingWindowDetector detector,
    ZetaStateMachine stateMachine,
    KeyEvaluator keyEvaluator,
    WorkerProperties properties,
    WorkerBroadcaster broadcaster
  ) {
    return new EvictStaleTask(detector, stateMachine, keyEvaluator, properties, broadcaster);
  }

  /**
   * Scheduled task that evicts stale keys from the sliding-window detector,
   * state machine, and CV history.
   */
  @RequiredArgsConstructor
  public static class EvictStaleTask {

    private final SlidingWindowDetector detector;
    private final ZetaStateMachine stateMachine;
    private final KeyEvaluator keyEvaluator;
    private final WorkerProperties properties;
    private final WorkerBroadcaster broadcaster;

    /**
     * Evicts keys that have not been accessed within {@code 2 * coolDurationMs}.
     */
    @Scheduled(fixedDelayString = "${zeta.worker.state-machine.evict-interval-ms:30000}")
    public void evictStale() {
      try {
        long staleAfterMs = properties.getStateMachine().getCoolDurationMs() * 2;
        detector.evictStale(staleAfterMs);
        stateMachine.evictStale(staleAfterMs, key -> {
          if (!broadcaster.broadcastCool(key)) {
            log.warn("Failed to broadcast COOL for stale key: {}", key);
          }
        });
        keyEvaluator.evictStale(staleAfterMs);
        log.debug("EvictStale tick: staleAfterMs={}", staleAfterMs);
      } catch (Exception e) {
        log.error("Scheduled evictStale failed", e);
      }
    }
  }

  /**
   * Periodically runs Top‑K cross‑validation to detect slow‑warming hot
   * keys that the sliding window might miss.
   *
   * <p>Interval is controlled by {@code zeta.worker.topk-validation.validate-interval-ms}
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
   * Default constructor.
   */
  @RequiredArgsConstructor
  public static class TopKValidationTask {

    private final TopKValidator topKValidator;

    /**
     * Runs TopK validation and pre-warm logic.
     */
    @Scheduled(fixedDelayString = "${zeta.worker.topk-validation.validate-interval-ms:60000}")
    public void validate() {
      try {
        topKValidator.validate();
        log.debug("TopK validation tick completed");
      } catch (Exception e) {
        log.error("Scheduled TopK validation failed", e);
      }
    }
  }

  /**
   * Global qps estimator that tracks overall throughput across all keys in the
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
   * based on estimated global qps and updates the sliding-window detector.
   *
   * @param estimator the global qps estimator
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
   * {@link SimpleRabbitListenerContainerFactory} using {@link SimpleMessageConverter}
   * for the Worker config listener, avoiding Jackson JSON conversion of heartbeat
   * messages (which use a custom header-based format).
   *
   * @param connectionFactory the RabbitMQ connection factory
   * @return a configured {@link SimpleRabbitListenerContainerFactory} instance
   */
  @Bean
  public SimpleRabbitListenerContainerFactory workerConfigListenerContainerFactory(
    ConnectionFactory connectionFactory
  ) {
    SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
    factory.setConnectionFactory(connectionFactory);
    factory.setMessageConverter(new SimpleMessageConverter());
    return factory;
  }

  /**
   * Fallback JSON message converter for reportToWorker messages. Active only when the
   * common module's {@code reportMessageConverter} is absent (e.g. in tests).
   *
   * @return a {@link org.springframework.amqp.support.converter.Jackson2JsonMessageConverter} instance
   */
  @Bean("reportMessageConverter")
  @ConditionalOnMissingBean(name = "reportMessageConverter")
  public MessageConverter reportMessageConverter() {
    return new org.springframework.amqp.support.converter.Jackson2JsonMessageConverter();
  }

  /**
   * Container factory for the {@link ReportConsumer}'s {@code @RabbitListener}.
   * Lifts throughput above Spring Boot's default (concurrency=1) by exposing
   * concurrent-consumers and prefetch via {@code zeta.worker.reportToWorker-consumer.*}.
   *
   * @param connectionFactory the RabbitMQ connection factory
   * @param reportMessageConverter the JSON message converter for reportToWorker messages
   * @param properties worker configuration properties
   * @return a configured {@link SimpleRabbitListenerContainerFactory} instance
   */
  @Bean
  public SimpleRabbitListenerContainerFactory reportListenerContainerFactory(
    ConnectionFactory connectionFactory,
    @Qualifier("reportMessageConverter") MessageConverter reportMessageConverter,
    WorkerProperties properties
  ) {
    SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
    factory.setConnectionFactory(connectionFactory);
    factory.setMessageConverter(reportMessageConverter);
    factory.setConcurrentConsumers(properties.getReportConsumer().getConcurrentConsumers());
    factory.setMaxConcurrentConsumers(properties.getReportConsumer().getConcurrentConsumers());
    factory.setPrefetchCount(properties.getReportConsumer().getPrefetchCount());
    factory.setAcknowledgeMode(AcknowledgeMode.AUTO);
    factory.setDefaultRequeueRejected(false);
    return factory;
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
    ZetaStateMachine stateMachine,
    @Qualifier("configTimestampCounter") AtomicLong configTimestampCounter
  ) {
    return new WorkerConfigNegotiator(stateMachine, configTimestampCounter, nodeId);
  }

  /**
   * Schedules periodic threshold recalculation using the {@link ThresholdLearner}.
   *
   * <p>The learner runs at the interval specified by
   * {@code zeta.worker.global-qps-dynamic-threshold.recalculate-interval-ms}.
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
    try {
      scheduler.scheduleAtFixedRate(
        learner,
        properties.getGlobalQpsDynamicThreshold().getRecalculateIntervalMs(),
        properties.getGlobalQpsDynamicThreshold().getRecalculateIntervalMs(),
        TimeUnit.MILLISECONDS
      );
    } catch (Exception e) {
      log.error("Failed to schedule threshold learning task; dynamic threshold " + "adjustment will not run.", e);
    }
    return new Object(); // placeholder bean
  }

  /**
   * Auto-delete queue for on-demand verification requests (PING) from Apps.
   * Queue name: {@code zeta.verify.ping.<nodeId>}.
   *
   * @return a non-durable, auto-delete {@link Queue} for PING/PONG verification
   */
  @Bean
  public Queue verifyPingQueue() {
    return QueueBuilder.nonDurable("zeta.verify.ping." + nodeId).autoDelete().build();
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
    c.setErrorHandler(t -> log.warn("Verify ping listener uncaught exception (PONG will not be sent)", t));
    return c;
  }

  /**
   * Enhanced heartbeat producer that sends structured heartbeats with epoch,
   * load factor, decision-version watermark, and state-machine config gossip.
   *
   * <p>Shares the same epoch counter bean with {@link WorkerBroadcaster}
   * so that epoch values in heartbeat messages and send messages are
   * guaranteed identical.
   *
   * @param rabbitTemplate         the RabbitMQ template for publishing heartbeat messages
   * @param properties             worker configuration providing exchange and interval settings
   * @param stateMachine           the state machine providing config gossip fields
   * @param broadcaster            the broadcaster for reading the current decision version watermark
   * @param epochCounter           the shared epoch counter (initialised via Redis INCR)
   * @param configTimestampCounter the shared monotonic counter for config-change timestamps
   * @param scheduler              the shared worker scheduler for periodic heartbeat sends
   * @return a new {@link WorkerHeartbeatProducer} instance
   */
  @Bean
  public WorkerHeartbeatProducer workerHeartbeatProducer(
    RabbitTemplate rabbitTemplate,
    WorkerProperties properties,
    ZetaStateMachine stateMachine,
    WorkerBroadcaster broadcaster,
    @Qualifier("workerEpochCounter") AtomicLong epochCounter,
    @Qualifier("configTimestampCounter") AtomicLong configTimestampCounter,
    @Qualifier("hotKeyScheduler") ScheduledExecutorService scheduler,
    SnowflakeIdGenerator snowflakeIdGenerator
  ) {
    return new WorkerHeartbeatProducer(
      rabbitTemplate,
      properties.getMessaging().getHeartbeatExchange(),
      nodeId,
      stateMachine,
      broadcaster,
      configTimestampCounter,
      epochCounter.get(),
      properties.getHeartbeat().getPingIntervalMs(),
      scheduler,
      snowflakeIdGenerator
    );
  }
}
