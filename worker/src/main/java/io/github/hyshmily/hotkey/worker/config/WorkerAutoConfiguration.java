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

import io.github.hyshmily.hotkey.algorithm.HeavyKeeper;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.constants.HotKeyConstants;
import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.logging.DefaultLogger;
import io.github.hyshmily.hotkey.logging.HotKeyLogger;
import io.github.hyshmily.hotkey.util.InstanceIdGenerator;
import io.github.hyshmily.hotkey.worker.detection.GlobalQpsEstimator;
import io.github.hyshmily.hotkey.worker.detection.SlidingWindowDetector;
import io.github.hyshmily.hotkey.worker.detection.ThresholdLearner;
import io.github.hyshmily.hotkey.worker.detection.TopKValidator;
import io.github.hyshmily.hotkey.worker.dispatch.WorkerBroadcaster;
import io.github.hyshmily.hotkey.worker.ingest.ReportConsumer;
import jakarta.annotation.PostConstruct;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
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
public class WorkerAutoConfiguration {

  private static final HotKeyLogger log = new DefaultLogger(WorkerAutoConfiguration.class);

  private final WorkerProperties properties;

  /**
   * Worker node identity — auto-generated once per JVM.
   *
   * <p>Used in queue names ({@code hotkey.worker.config.<nodeId>}) and heartbeat
   * messages to identify this Worker instance uniquely.
   */
  private final String nodeId = InstanceIdGenerator.get();

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

  /** Creates the per‑key lifecycle state machine. */
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

  /** Broadcasts HOT / COOL decisions to all application instances. */
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
   * Shared scheduler for worker‑internal periodic tasks (eviction, etc.).
   * Threads are daemon so they don't block JVM shutdown.
   */
  @Bean(destroyMethod = "shutdown")
  public ScheduledExecutorService hotKeyWorkerScheduler() {
    return Executors.newScheduledThreadPool(2, r -> {
      Thread t = new Thread(r, HotKeyConstants.THREAD_PREFIX_WORKER);
      t.setDaemon(true);
      return t;
    });
  }

  /**
   * Direct exchange to which clients publish report messages.
   * Routing keys follow the pattern {@code report.<appName>.<nodeId>}.
   */
  @Bean
  public DirectExchange reportExchange(WorkerProperties properties) {
    return new DirectExchange(properties.getMessaging().getReportExchange(), true, false);
  }

  /**
   * Queue that this Worker binds to.
   * Queue name is {@code hotkey.report.<appName>.<nodeId>},
   * guaranteeing that a key always lands on the same Worker.
   */
  @Bean
  public Queue reportQueue(WorkerProperties properties) {
    String queueName = HotKeyConstants.QUEUE_PREFIX_REPORT + properties.getRouting().getAppName() + "." + nodeId;
    return QueueBuilder.durable(queueName).build();
  }

  /** Binds the queue to the report exchange with the matching routing key. */
  @Bean
  public Binding reportBinding(Queue reportQueue, DirectExchange reportExchange, WorkerProperties properties) {
    String routingKey = HotKeyConstants.ROUTING_KEY_REPORT + properties.getRouting().getAppName() + "." + nodeId;
    return BindingBuilder.bind(reportQueue).to(reportExchange).with(routingKey);
  }

  /**
   * Auto-delete queue for receiving heartbeat-based config updates from
   * other Workers.
   *
   * <p>Each Worker declares its own queue named
   * {@code hotkey.worker.config.<nodeId>}, which is automatically removed
   * when the Worker disconnects.  The queue is bound directly to the
   * {@code broadcastExchange} — the same exchange that
   * {@link WorkerBroadcaster} (and all app-side WorkerListeners) publish
   * heartbeats to.
   */
  @Bean
  public Queue workerConfigQueue() {
    return QueueBuilder.nonDurable("hotkey.worker.config." + nodeId).autoDelete().build();
  }

  /** Binds the per-Worker config queue to the shared broadcast exchange. */
  @Bean
  public Binding workerConfigBinding(Queue workerConfigQueue, WorkerProperties properties) {
    return new Binding(
      workerConfigQueue.getName(),
      Binding.DestinationType.QUEUE,
      properties.getMessaging().getBroadcastExchange(),
      "",
      null
    );
  }

  /**
   * Monotonically increasing counter for config-change timestamps.
   *
   * <p>Every {@code POST /actuator/worker/state} call increments this counter.
   * Receiving Workers compare their own counter against the incoming value;
   * only strictly newer configs are applied.
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
    @Scheduled(fixedDelayString = "${hotkey.worker.state-machine.cool-duration-ms:15000}")
    public void evictStale() {
      long staleAfterMs = properties.getStateMachine().getCoolDurationMs() * 2;
      detector.evictStale(staleAfterMs);
      stateMachine.evictStale(staleAfterMs);
      log.debug("Evicted stale Worker state: staleAfterMs={}", staleAfterMs);
    }
  }

  /**
   * Periodically runs Top‑K cross‑validation to detect slow‑warming hot
   * keys that the sliding window might miss.
   *
   * <p>Interval is controlled by {@code hotkey.worker.topk-validation.validate-interval-ms}
   * (default 60 seconds).
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
      topKValidator.validate();
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
    @Qualifier("hotKeyWorkerScheduler") ScheduledExecutorService scheduler
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
   * Schedules a periodic heartbeat ping broadcast for this worker instance.
   *
   * @param broadcaster the worker broadcaster used to send the heartbeat
   * @param properties  worker configuration properties (ping interval
   *                    is extracted from here)
   * @param scheduler   the shared worker scheduler
   * @return a placeholder {@link Object} bean that keeps the scheduler alive
   */
  @Bean
  public Object pingTask(
    WorkerBroadcaster broadcaster,
    WorkerProperties properties,
    @Qualifier("hotKeyWorkerScheduler") ScheduledExecutorService scheduler
  ) {
    int intervalMs = properties.getHeartbeat().getPingIntervalMs();
    scheduler.scheduleAtFixedRate(
      () -> broadcaster.broadcastHeartbeat(nodeId),
      0,
      intervalMs,
      TimeUnit.MILLISECONDS
    );
    return new Object(); // placeholder bean
  }
}
