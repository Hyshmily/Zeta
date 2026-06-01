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
package io.github.hyshmily.hotkey.worker;

import io.github.hyshmily.hotkey.algorithm.HeavyKeeper;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.constant.HotKeyConstants;
import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import jakarta.annotation.PostConstruct;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
@AutoConfiguration
@ConditionalOnClass(name = "org.springframework.amqp.rabbit.core.RabbitTemplate")
@ConditionalOnProperty(prefix = "hotkey.worker", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(WorkerProperties.class)
@EnableScheduling
public class WorkerAutoConfiguration {

  private final WorkerProperties properties;

  public WorkerAutoConfiguration(WorkerProperties properties) {
    this.properties = properties;
  }

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
  public WorkerBroadcaster workerBroadcaster(RabbitTemplate rabbitTemplate, WorkerProperties properties) {
    return new WorkerBroadcaster(
      rabbitTemplate,
      properties.getMessaging().getBroadcastExchange(),
      properties.getRouting().getAppName()
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
   * Routing keys follow the pattern {@code report.<appName>.<shardIndex>}.
   */
  @Bean
  public DirectExchange reportExchange(WorkerProperties properties) {
    return new DirectExchange(properties.getMessaging().getReportExchange(), true, false);
  }

  /**
   * Shard‑specific queue that this Worker binds to.
   * The queue name is {@code hotkey.report.<appName>.<shardIndex>},
   * guaranteeing that a key always lands on the same Worker.
   */
  @Bean
  public Queue reportQueue(WorkerProperties properties) {
    String queueName =
      HotKeyConstants.QUEUE_PREFIX_REPORT +
      properties.getRouting().getAppName() +
      "." +
      properties.getRouting().getShardIndex();
    return QueueBuilder.durable(queueName).build();
  }

  /** Binds the shard queue to the report exchange with the matching routing key. */
  @Bean
  public Binding reportBinding(Queue reportQueue, DirectExchange reportExchange, WorkerProperties properties) {
    String routingKey =
      HotKeyConstants.ROUTING_KEY_REPORT +
      properties.getRouting().getAppName() +
      "." +
      properties.getRouting().getShardIndex();
    return BindingBuilder.bind(reportQueue).to(reportExchange).with(routingKey);
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

  @RequiredArgsConstructor
  public static class EvictStaleTask {

    private final SlidingWindowDetector detector;
    private final HotKeyStateMachine stateMachine;
    private final WorkerProperties properties;

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

  @RequiredArgsConstructor
  public static class TopKValidationTask {

    private final TopKValidator topKValidator;

    @Scheduled(fixedDelayString = "${hotkey.worker.topk-validation.validate-interval-ms:60000}")
    public void validate() {
      topKValidator.validate();
    }
  }

  @Bean
  public GlobalQpsEstimator globalQpsEstimator(WorkerProperties properties) {
    return new GlobalQpsEstimator(
      properties.getSlidingWindow().getDurationMs(),
      properties.getSlidingWindow().getSlices()
    );
  }

  @Bean
  public ThresholdLearner thresholdLearner(
    GlobalQpsEstimator estimator,
    SlidingWindowDetector detector,
    WorkerProperties properties
  ) {
    return new ThresholdLearner(estimator, detector, properties);
  }

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
}
