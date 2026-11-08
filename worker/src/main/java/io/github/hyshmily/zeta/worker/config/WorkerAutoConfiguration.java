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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hyshmily.zeta.autoconfigure.ZetaAmqpAutoConfiguration;
import io.github.hyshmily.zeta.autoconfigure.ZetaAppProperties;
import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.util.NodeIdentity;
import io.github.hyshmily.zeta.util.executor.SafeScheduledExecutorService;
import io.github.hyshmily.zeta.util.executor.ZetaThreadFactory;
import io.github.hyshmily.zeta.worker.detection.DetectionPipeline;
import io.github.hyshmily.zeta.worker.detection.lifecycle.CoolingScheduler;
import io.github.hyshmily.zeta.worker.detection.lifecycle.CoolingWheels;
import io.github.hyshmily.zeta.worker.detection.lifecycle.HotKeyRegistry;
import io.github.hyshmily.zeta.worker.detection.rule.RuleResolver;
import io.github.hyshmily.zeta.worker.detection.rule.RuleSet;
import io.github.hyshmily.zeta.worker.detection.window.SlidingWindowDetector;
import io.github.hyshmily.zeta.worker.detection.window.WindowGeometry;
import io.github.hyshmily.zeta.worker.dispatch.DecisionDispatcher;
import io.github.hyshmily.zeta.worker.dispatch.WorkerBroadcaster;
import io.github.hyshmily.zeta.worker.dispatch.connection.WorkerEpochStore;
import io.github.hyshmily.zeta.worker.dispatch.connection.WorkerHeartbeatProducer;
import io.github.hyshmily.zeta.worker.dispatch.connection.WorkerIdentity;
import io.github.hyshmily.zeta.worker.dispatch.ingest.ReportConsumer;
import io.github.hyshmily.zeta.worker.endpoint.HotKeyEndpoint;
import io.github.hyshmily.zeta.worker.metrics.WorkerDetectionMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;

/**
 * Auto‑configuration for the <b>hot‑key Worker</b>.
 *
 * <p>Activates when {@code zeta.worker.enabled=true} and RabbitMQ is on the classpath. The Worker
 * consumes per‑key access counts reported by application instances, evaluates them against one
 * sliding window and an ordered rule set, and broadcasts HOT / COOL decisions back to every
 * instance.
 *
 * <h2>Provisioned beans</h2>
 * <ul>
 *   <li>{@link DetectionSettings} – the validated detection block, with thresholds converted to
 *       window counts.</li>
 *   <li>{@link SlidingWindowDetector} – the shared per-key window table.</li>
 *   <li>{@link RuleResolver} – the published rule snapshot and its bounded match cache.</li>
 *   <li>{@link HotKeyRegistry} – the authoritative HOT set and its seat permits.</li>
 *   <li>{@link CoolingWheels} / {@link CoolingScheduler} – the striped cool-down wheels and their
 *       advancing threads.</li>
 *   <li>{@link DetectionPipeline} – the report entry point that ties them together.</li>
 *   <li>{@link ReportConsumer} – the AMQP listener.</li>
 *   <li>{@link DecisionDispatcher} / {@link WorkerBroadcaster} – two-queue delivery.</li>
 *   <li>{@link WorkerIdentity} – this process's node id and restart epoch, stamped on every send.</li>
 *   <li>{@link WorkerDetectionMetrics} – detection-plane meters (meter-less without a registry).</li>
 *   <li>RabbitMQ topology, the heartbeat, and the repair sweep's schedule.</li>
 * </ul>
 */
@AutoConfiguration(after = ZetaAmqpAutoConfiguration.class)
@ConditionalOnClass(name = "org.springframework.amqp.rabbit.core.RabbitTemplate")
@ConditionalOnProperty(prefix = "zeta.worker", name = "enabled", havingValue = "true")
@EnableConfigurationProperties({ WorkerProperties.class, ZetaAppProperties.class })
@RequiredArgsConstructor
@Slf4j
public class WorkerAutoConfiguration {

  private final WorkerProperties properties;

  /**
   * Dedicated single-thread scheduler for the Worker <b>control plane</b>: heartbeat emission.
   *
   * <p>The heartbeat has its own connection factory so data-plane congestion cannot block it; the
   * thread pool is the other half of that isolation. The repair sweep walks the window table and
   * the HOT set and runs on the data-plane scheduler, so a long sweep cannot delay a heartbeat —
   * and an App that sees a late heartbeat drops a perfectly healthy Worker from the ring.
   *
   * @return the control-plane scheduler
   */
  @Bean(name = "workerControlScheduler", destroyMethod = "shutdownNow")
  @ConditionalOnMissingBean(name = "workerControlScheduler")
  public ScheduledExecutorService workerControlScheduler() {
    return new SafeScheduledExecutorService(1, new ZetaThreadFactory("zeta-worker-control"));
  }

  /**
   * Validates the {@code detection} block once and resolves it into runtime objects.
   *
   * <p>Every violation is collected and thrown together, so a misconfiguration fails startup with
   * the full list instead of one problem per restart.
   *
   * @return the resolved detection settings
   */
  @Bean
  @ConditionalOnMissingBean
  public DetectionSettings detectionSettings() {
    DetectionSettings settings = DetectionSettings.of(properties);
    RuleSet ruleSet = settings.ruleSet();
    log.info(
      "Worker detection: enabled={}, window={}ms/{} (slice {}ms), rules={}, rulesHash={}, minHotCount={}, maxActiveHotKeys={}",
      settings.enabled(),
      settings.window().windowMs(),
      settings.window().alignedSlices(),
      settings.window().sliceMs(),
      ruleSet.rules().size(),
      ruleSet.hash(),
      ruleSet.minHotCount(),
      settings.maxActiveHotKeys()
    );
    return settings;
  }

  /**
   * The aligned window geometry, exposed for the endpoint.
   *
   * @param settings the resolved detection settings
   * @return the geometry
   */
  @Bean
  @ConditionalOnMissingBean
  public WindowGeometry windowGeometry(DetectionSettings settings) {
    return settings.window();
  }

  /**
   * Creates the sliding-window detector.
   *
   * @param settings the resolved detection settings
   * @return the shared window table
   */
  @Bean
  @ConditionalOnMissingBean
  public SlidingWindowDetector slidingWindowDetector(DetectionSettings settings) {
    return new SlidingWindowDetector(settings.window(), properties.getMaxTrackedKeys());
  }

  /**
   * Creates the rule resolver over the validated snapshot.
   *
   * @param settings the resolved detection settings
   * @param metrics  detection meters (used for the per-rule match counters)
   * @return the resolver
   */
  @Bean
  @ConditionalOnMissingBean
  public RuleResolver ruleResolver(DetectionSettings settings, WorkerDetectionMetrics metrics) {
    return new RuleResolver(settings.ruleSet(), settings.maxActiveHotKeys(), metrics);
  }

  /**
   * Detection-plane meters.
   *
   * <p>Only the counters are created here. The gauges and the cumulative function counters are
   * bound by {@link #workerDetectionMeterBinder} once every singleton exists, because they read
   * components that themselves need these meters to count a rejection.
   *
   * @param meterRegistry provider for the registry; absent when the context holds none
   * @return the meters (meter-less without a registry)
   */
  @Bean
  @ConditionalOnMissingBean
  public WorkerDetectionMetrics workerDetectionMetrics(ObjectProvider<MeterRegistry> meterRegistry) {
    return new WorkerDetectionMetrics(meterRegistry.getIfAvailable());
  }

  /**
   * Worker-wide causal decision-version allocator.
   *
   * <p>One counter for the whole Worker, not one per component: the numbers must be comparable
   * across promotions and cool-downs, otherwise the broadcaster cannot tell "queued behind a slow
   * chunk" from "already superseded".
   *
   * @return a fresh counter
   */
  @Bean(name = "workerDecisionVersionCounter")
  @ConditionalOnMissingBean(name = "workerDecisionVersionCounter")
  public AtomicLong workerDecisionVersionCounter() {
    return new AtomicLong(0L);
  }

  /**
   * The authoritative HOT set.
   *
   * @param settings        the resolved detection settings
   * @param metrics         detection meters
   * @param decisionVersion the shared version allocator
   * @param detector        the window table, read by the LOW_QPS cool-down
   * @param wheels          the cool-down wheels the entries are scheduled on
   * @return the registry
   */
  @Bean
  @ConditionalOnMissingBean
  public HotKeyRegistry hotKeyRegistry(
    DetectionSettings settings,
    WorkerDetectionMetrics metrics,
    @Qualifier("workerDecisionVersionCounter") AtomicLong decisionVersion,
    SlidingWindowDetector detector,
    CoolingWheels wheels
  ) {
    return new HotKeyRegistry(settings.maxActiveHotKeys(), metrics, decisionVersion, detector::windowSum, wheels);
  }

  /**
   * The striped cool-down wheels: one independent timer wheel and one advancing thread per
   * configured cooling thread. A key is assigned to a wheel by its hash, so the key space — not a
   * shared queue — partitions the work.
   *
   * @param settings the resolved detection settings (wheel count)
   * @return the wheel set
   */
  @Bean
  @ConditionalOnMissingBean
  public CoolingWheels coolingWheels(DetectionSettings settings) {
    return new CoolingWheels(settings.coolingThreads());
  }

  /**
   * Single submission point for Worker decisions: two bounded queues, one drain thread.
   *
   * @param settings    the validated detection snapshot providing both send rates
   * @param broadcaster the send funnel every decision goes through
   * @param metrics     detection meters
   * @param registry    the HOT set: an undelivered HOT returns its seat, and a queued HOT whose
   *                    promotion is gone is dropped
   * @return the dispatcher
   */
  @Bean
  @ConditionalOnMissingBean
  public DecisionDispatcher decisionDispatcher(
    DetectionSettings settings,
    WorkerBroadcaster broadcaster,
    WorkerDetectionMetrics metrics,
    HotKeyRegistry registry
  ) {
    WorkerProperties.Broadcast broadcast = properties.getBroadcast();
    return new DecisionDispatcher(
      broadcast.getHotQueueCapacity(),
      broadcast.getCoolQueueCapacity(),
      settings.maxHotBroadcastsPerSecond(),
      settings.maxCoolBroadcastsPerSecond(),
      broadcast.getHotBurst(),
      broadcast.getCoolBurst(),
      broadcast.getMaxDecisionAgeMs(),
      broadcaster,
      metrics,
      registry::abortPromotion,
      registry::isCurrent
    );
  }

  /**
   * Cool-down threads over the striped wheels.
   *
   * @param wheels      the striped wheels (one advancing thread each)
   * @param registry    the HOT set the checks drive
   * @param dispatcher  the submission point for COOL decisions
   * @return the scheduler
   */
  @Bean
  @ConditionalOnMissingBean
  public CoolingScheduler coolingScheduler(
    CoolingWheels wheels,
    HotKeyRegistry registry,
    DecisionDispatcher dispatcher
  ) {
    return new CoolingScheduler(wheels, registry, dispatcher);
  }

  /**
   * Binds the gauges and cumulative function counters after every singleton exists. This is a
   * separate bean, not a side effect of {@link #coolingScheduler}, so a caller-supplied
   * {@code CoolingScheduler} does not silently lose the meters that read the detector, registry
   * and queues.
   *
   * <p>The queue-depth and wheel-count suppliers are method references created here, not fields.
   * {@link WorkerDetectionMetrics#bindMeters} therefore registers those three gauges with a strong
   * reference: Micrometer's default weak reference would otherwise let a young collection clear the
   * supplier and leave the gauge reading {@code NaN}.
   *
   * @param metrics   detection meters
   * @param detector  the window table
   * @param registry  the HOT set
   * @param dispatcher the decision queues
   * @param wheels    the cool-down wheels
   * @return a callback invoked once after singleton instantiation
   */
  @Bean
  public SmartInitializingSingleton workerDetectionMeterBinder(
    WorkerDetectionMetrics metrics,
    SlidingWindowDetector detector,
    HotKeyRegistry registry,
    DecisionDispatcher dispatcher,
    CoolingWheels wheels
  ) {
    return () ->
      metrics.bindMeters(detector, registry, dispatcher::hotDepth, dispatcher::coolDepth, wheels::scheduledCount);
  }

  /**
   * The detection entry point the report consumer calls.
   *
   * @param detector    the window table
   * @param rules       the rule resolver
   * @param registry    the HOT set
   * @param dispatcher  the decision submission point
   * @param settings    the resolved detection settings (retention threshold and enablement)
   * @return the pipeline
   */
  @Bean
  @ConditionalOnMissingBean
  public DetectionPipeline detectionPipeline(
    SlidingWindowDetector detector,
    RuleResolver rules,
    HotKeyRegistry registry,
    DecisionDispatcher dispatcher,
    DetectionSettings settings
  ) {
    return new DetectionPipeline(detector, rules, registry, dispatcher, settings.retentionMs(), settings.enabled());
  }

  /**
   * Report consumer – the main AMQP entry point.
   *
   * @param pipeline      the detection entry point
   * @param identity      this process's immutable node id and epoch
   * @param metrics       detection meters
   * @return a new {@link ReportConsumer}
   */
  @Bean
  @ConditionalOnMissingBean
  public ReportConsumer reportConsumer(
    DetectionPipeline pipeline,
    WorkerIdentity identity,
    WorkerDetectionMetrics metrics
  ) {
    return new ReportConsumer(
      pipeline,
      identity.epoch(),
      properties.getReportConsumer().getStalenessThresholdMs(),
      metrics
    );
  }

  /**
   * The repair task: reclaim idle windows, then re-evaluate overdue cool-downs.
   *
   * @param pipeline the detection pipeline
   * @return the task
   */
  @Bean
  @ConditionalOnMissingBean
  public SweepTask sweepTask(DetectionPipeline pipeline) {
    return new SweepTask(pipeline);
  }

  /** Scheduled housekeeping; the schedule lives in {@link #workerScheduling}. */
  public static final class SweepTask {

    private final DetectionPipeline pipeline;

    SweepTask(DetectionPipeline pipeline) {
      this.pipeline = pipeline;
    }

    /** Runs one sweep and logs the number of overdue checks it selected. */
    public void sweep() {
      try {
        int repaired = pipeline.sweep();
        if (repaired > 0) {
          log.debug("Detection sweep repaired {} overdue cool-down check(s)", repaired);
        }
      } catch (Exception e) {
        log.error("Scheduled detection sweep failed", e);
      }
    }
  }

  /**
   * Dedicated {@link RabbitTemplate} for HOT/COOL decision broadcasts.
   *
   * <p>The decision plane is data-plane traffic: it uses the same connection factory as report
   * consumption and worker-listener containers, never the dedicated heartbeat factory. A shared
   * template would let broker flow control on a broadcast burst block the heartbeat channel and
   * make the App drop a healthy Worker from the ring.
   *
   * @param beanFactory the bean factory used to resolve the data-plane connection factory
   * @return a new {@link RabbitTemplate} for decision broadcasts
   */
  @Bean("zetaWorkerBroadcastRabbitTemplate")
  @ConditionalOnMissingBean(name = "zetaWorkerBroadcastRabbitTemplate")
  public RabbitTemplate workerBroadcastRabbitTemplate(ListableBeanFactory beanFactory) {
    return new RabbitTemplate(ZetaAmqpAutoConfiguration.dataPlaneConnectionFactory(beanFactory));
  }

  /**
   * Publishes HOT / COOL decisions to all application instances.
   *
   * <p>One send per decision, fire-and-forget: no repeat chain and no periodic re-emission. A lost
   * decision is absorbed by the cluster's coarse backstops (the App's hard TTL, the local TopK's
   * TTL authority when the Worker is down), not repaired per key.
   *
   * @param rabbitTemplate  the data-plane template used to publish decisions
   * @param appProperties   shared {@code zeta.app-name} carried in the decision headers
   * @param identity        this process's immutable node id and epoch
   * @param metrics         detection meters
   * @return a new {@link WorkerBroadcaster}
   */
  @Bean
  @ConditionalOnMissingBean
  public WorkerBroadcaster workerBroadcaster(
    @Qualifier("zetaWorkerBroadcastRabbitTemplate") RabbitTemplate rabbitTemplate,
    ZetaAppProperties appProperties,
    WorkerIdentity identity,
    WorkerDetectionMetrics metrics
  ) {
    return new WorkerBroadcaster(
      rabbitTemplate,
      properties.getMessaging().getBroadcastExchange(),
      appProperties.getAppName(),
      identity,
      metrics,
      properties.getBroadcast().getMaxDecisionAgeMs()
    );
  }

  /**
   * Direct exchange for broadcasting HOT/COOL decisions to the instances of one application.
   *
   * <p>App instances bind their per-instance queues with the routing key {@code send.<appName>};
   * the Worker publishes under that same key. A different {@code appName} sharing the broker sees
   * nothing, because none of its queues are bound to this key.
   *
   * <p><b>Upgrade note:</b> an exchange's type cannot be changed in place — a broker that still
   * holds {@code zeta.send.exchange} as {@code fanout} answers {@code PRECONDITION_FAILED}. Delete
   * the exchange (and the app queues bound to it) before rolling out, or point the property at a
   * fresh name.
   *
   * @return a durable, non-auto-delete {@link DirectExchange}
   */
  @Bean
  @ConditionalOnMissingBean(name = "broadcastExchange")
  public DirectExchange broadcastExchange() {
    return new DirectExchange(properties.getMessaging().getBroadcastExchange(), true, false);
  }

  /**
   * Direct exchange to which clients publish report messages.
   *
   * @return a durable, non-auto-delete {@link DirectExchange}
   */
  @Bean
  @ConditionalOnMissingBean(name = "reportExchange")
  public DirectExchange reportExchange() {
    return new DirectExchange(properties.getMessaging().getReportExchange(), true, false);
  }

  /**
   * The queue this Worker binds to: {@code zeta.reportToWorker.<appName>.<nodeId>}, so a key
   * always lands on the same Worker.
   *
   * <p>A 7-day idle expiry, plus {@code x-max-length} with drop-head and {@code x-message-ttl},
   * bounds both broker memory and staleness when consumption stalls.
   *
   * <p><b>Upgrade note:</b> {@code x-*} arguments cannot be altered on an existing queue — delete
   * the old queue before rolling out a change, otherwise re-declaration fails.
   *
   * @param appProperties shared {@code zeta.app-name} used in the queue name
   * @param identity      this process's immutable identity; its node id is the shard
   * @return a durable {@link Queue}
   */
  @Bean
  @ConditionalOnMissingBean(name = "reportQueue")
  public Queue reportQueue(ZetaAppProperties appProperties, WorkerIdentity identity) {
    String queueName = ZetaConstants.Routing.QUEUE_PREFIX_REPORT + appProperties.getAppName() + "." + identity.nodeId();
    return QueueBuilder.durable(queueName)
      .withArgument("x-expires", 604_800_000)
      .withArgument("x-max-length", properties.getReportQueue().getMaxLength())
      .withArgument("x-overflow", "drop-head")
      .withArgument("x-message-ttl", properties.getReportQueue().getMessageTtlMs())
      .build();
  }

  /**
   * Binds the report queue to the report exchange with the shard-specific routing key.
   *
   * @param reportQueue    the shard-specific queue
   * @param reportExchange the report exchange
   * @param appProperties  shared {@code zeta.app-name} used in the routing key
   * @param identity       this process's immutable identity; its node id is the shard
   * @return the binding
   */
  @Bean
  @ConditionalOnMissingBean(name = "reportBinding")
  public Binding reportBinding(
    @Qualifier("reportQueue") Queue reportQueue,
    @Qualifier("reportExchange") DirectExchange reportExchange,
    ZetaAppProperties appProperties,
    WorkerIdentity identity
  ) {
    String routingKey = ZetaConstants.Routing.KEY_REPORT + appProperties.getAppName() + "." + identity.nodeId();
    return BindingBuilder.bind(reportQueue).to(reportExchange).with(routingKey);
  }

  /**
   * Starts every periodic Worker task once the context is fully refreshed, and cancels them on
   * shutdown.
   *
   * <p>Two tasks: the repair sweep on the data plane (long, in-memory walks) and the heartbeat
   * on the control plane.
   *
   * @param sweepTask         the window-reclamation and timer-repair sweep
   * @param heartbeatProducer the structured heartbeat sender
   * @param settings          the resolved detection settings (sweep cadence)
   * @param controlScheduler  the control-plane scheduler
   * @param dataPlaneScheduler the scheduler for the repair sweep
   * @return the lifecycle that owns every schedule
   */
  @Bean
  @ConditionalOnMissingBean
  public WorkerScheduling workerScheduling(
    SweepTask sweepTask,
    WorkerHeartbeatProducer heartbeatProducer,
    DetectionSettings settings,
    @Qualifier("workerControlScheduler") ScheduledExecutorService controlScheduler,
    @Qualifier("hotKeyScheduler") ScheduledExecutorService dataPlaneScheduler
  ) {
    long sweepInterval = settings.sweepIntervalMs();
    long heartbeatInterval = properties.getHeartbeat().getPingIntervalMs();
    return new WorkerScheduling(
      List.of(
        // first sweep one full interval after startup, never during it
        new WorkerScheduling.Task(dataPlaneScheduler, sweepTask::sweep, sweepInterval, sweepInterval),
        new WorkerScheduling.Task(
          controlScheduler,
          heartbeatProducer::sendHeartbeat,
          heartbeatInterval,
          heartbeatInterval
        )
      )
    );
  }

  /**
   * Owns the Worker's periodic tasks: starts them on {@link #start()} (context refreshed, every
   * bean published) and cancels them on {@link #stop()}.
   *
   * <p>{@link SmartLifecycle} rather than {@code @Scheduled}: a starter must not enable scheduling
   * in the host application, and the futures have to be cancellable.
   */
  public static final class WorkerScheduling implements SmartLifecycle {

    /**
     * One periodic task bound to the scheduler of its plane.
     *
     * @param scheduler      the pool the task runs on
     * @param body           the work
     * @param initialDelayMs delay before the first run; {@code periodMs == 0} makes it one-shot
     * @param periodMs       period between runs, {@code 0} for a one-shot task
     */
    record Task(ScheduledExecutorService scheduler, Runnable body, long initialDelayMs, long periodMs) {}

    private final List<ScheduledFuture<?>> futures = new ArrayList<>();
    private final List<Task> tasks;

    private volatile boolean running;

    WorkerScheduling(List<Task> tasks) {
      this.tasks = tasks;
    }

    @Override
    public synchronized void start() {
      if (running) {
        return;
      }

      running = true;
      for (Task task : tasks) {
        try {
          futures.add(schedule(task));
        } catch (Exception e) {
          // A Worker whose heartbeat or repair sweep cannot be scheduled is not a degraded Worker:
          // it either never joins the ring or silently leaks cool-down checks. Fail startup instead
          // of reporting isRunning=true with a dead capability.
          stop();
          throw new IllegalStateException(
            "Failed to schedule worker task (initialDelayMs=" +
              task.initialDelayMs() +
              ", periodMs=" +
              task.periodMs() +
              ")",
            e
          );
        }
      }
    }

    /** Schedules one task as repeating or one-shot; {@code periodMs == 0} means one-shot. */
    private static ScheduledFuture<?> schedule(Task task) {
      if (task.periodMs() > 0) {
        return task
          .scheduler()
          .scheduleWithFixedDelay(task.body(), task.initialDelayMs(), task.periodMs(), TimeUnit.MILLISECONDS);
      }
      return task.scheduler().schedule(task.body(), task.initialDelayMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized void stop() {
      running = false;
      for (ScheduledFuture<?> future : futures) {
        future.cancel(false);
      }
      futures.clear();
    }

    @Override
    public boolean isRunning() {
      return running;
    }
  }

  /**
   * Topic exchange for epoch-aware structured heartbeats from Workers.
   *
   * @return a durable, non-auto-delete {@link TopicExchange}
   */
  @Bean
  @ConditionalOnMissingBean(name = "heartbeatExchange")
  public TopicExchange heartbeatExchange() {
    return new TopicExchange(properties.getMessaging().getHeartbeatExchange(), true, false);
  }

  /**
   * This process's immutable identity: the node id plus the restart-detection epoch, incremented on
   * every start and persisted in {@code zeta.worker.epoch-file}.
   *
   * <p>The epoch is read once at startup and never changes, so one record carries it together with
   * the node id. That record is the single source of the node identity in the context: the report
   * queue, its binding and every sender read it instead of a separate {@code String} bean.
   *
   * <p>{@code INSTANCE_ID} is the deployment-supplied identity (Kubernetes downward API, Nomad
   * meta, ...). It is applied before the node id is read, so the override and the read are one step.
   *
   * @return the identity every sender stamps
   */
  @Bean
  @ConditionalOnMissingBean
  public WorkerIdentity workerIdentity() {
    String envId = System.getenv("INSTANCE_ID");
    if (envId != null && !envId.isBlank()) {
      NodeIdentity.setInstanceOverride(envId);
    }
    String nodeId = NodeIdentity.getInstanceId();
    String configured = properties.getEpochFile();
    Path epochFile = configured == null || configured.isBlank() ? null : Path.of(configured);
    return new WorkerIdentity(nodeId, WorkerEpochStore.initEpoch(nodeId, epochFile));
  }

  /**
   * Façade endpoint for the detection plane.
   *
   * <p>Declared explicitly (not component-scanned): {@code @Endpoint} is not a stereotype, so
   * without this bean the endpoint would silently vanish. The explicit bean name keeps it
   * distinct from the App-side {@code ZetaEndpoint} bean, which both auto-configurations would
   * otherwise register as {@code hotKeyEndpoint}. The shipped {@code application.yml} exposes
   * only {@code health}; an operator opts in by adding {@code hotkeyworker} to the web exposure
   * include and mapping its path.
   *
   * @param rules    the rule resolver
   * @param window   the effective window geometry
   * @param registry the HOT set
   * @param cooling  the cool-down scheduler
   * @param detector the window table
   * @return a new {@link HotKeyEndpoint}
   */
  @Bean("hotKeyWorkerEndpoint")
  @ConditionalOnMissingBean
  @ConditionalOnClass(name = "org.springframework.boot.actuate.endpoint.annotation.Endpoint")
  public HotKeyEndpoint hotKeyEndpoint(
    RuleResolver rules,
    WindowGeometry window,
    HotKeyRegistry registry,
    CoolingScheduler cooling,
    SlidingWindowDetector detector
  ) {
    return new HotKeyEndpoint(rules, window, registry, cooling, detector);
  }

  /**
   * Fallback report message converter. Active only when the common module's converter is absent
   * (e.g. in tests). Jackson JSON only; unknown fields are ignored.
   *
   * @return a forward-compatible Jackson JSON converter
   */
  @Bean("reportMessageConverter")
  @ConditionalOnMissingBean(name = "reportMessageConverter")
  public MessageConverter reportMessageConverter() {
    ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return new Jackson2JsonMessageConverter(mapper);
  }

  /**
   * Container factory for {@link ReportConsumer}'s {@code @RabbitListener}.
   *
   * @param beanFactory            the bean factory used to resolve the data-plane connection factory
   * @param reportMessageConverter the JSON message converter for report messages
   * @return a configured {@link SimpleRabbitListenerContainerFactory}
   */
  @Bean
  @ConditionalOnMissingBean(name = "reportListenerContainerFactory")
  public SimpleRabbitListenerContainerFactory reportListenerContainerFactory(
    ListableBeanFactory beanFactory,
    @Qualifier("reportMessageConverter") MessageConverter reportMessageConverter
  ) {
    SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
    factory.setConnectionFactory(ZetaAmqpAutoConfiguration.dataPlaneConnectionFactory(beanFactory));
    factory.setMessageConverter(reportMessageConverter);
    factory.setConcurrentConsumers(properties.getReportConsumer().getConcurrentConsumers());
    factory.setMaxConcurrentConsumers(properties.getReportConsumer().getConcurrentConsumers());
    factory.setPrefetchCount(properties.getReportConsumer().getPrefetchCount());
    factory.setAcknowledgeMode(AcknowledgeMode.AUTO);
    factory.setDefaultRequeueRejected(false);
    return factory;
  }

  /**
   * Dedicated {@link RabbitTemplate} for control-plane traffic (heartbeat), backed by
   * the dedicated heartbeat connection factory.
   *
   * <p>Pre-declares the heartbeat exchange before the template is handed to any publisher: the
   * exchange is otherwise declared lazily by {@code RabbitAdmin}, and that declaration is not
   * ordered against the control-plane connection — on an empty broker a first publish can reach a
   * not-yet-existing exchange, RabbitMQ answers {@code 404 NOT_FOUND}, closes the channel and that
   * tick's heartbeat is lost.
   *
   * <p>A failure is non-fatal: the broker may legitimately be unreachable during context refresh,
   * and {@code RabbitAdmin} declares the exchange on (re)connect.
   *
   * @param connectionFactory the dedicated heartbeat connection factory
   * @return a new {@link RabbitTemplate} for control-plane messaging
   */
  @Bean("zetaHeartbeatRabbitTemplate")
  @ConditionalOnMissingBean(name = "zetaHeartbeatRabbitTemplate")
  public RabbitTemplate heartbeatRabbitTemplate(
    @Qualifier("zetaHeartbeatConnectionFactory") ConnectionFactory connectionFactory
  ) {
    RabbitTemplate template = new RabbitTemplate(connectionFactory);
    String exchange = properties.getMessaging().getHeartbeatExchange();
    try {
      template.execute(channel -> {
        channel.exchangeDeclare(exchange, "topic", true);
        return null;
      });
    } catch (Exception e) {
      log.warn(
        "Could not pre-declare heartbeat exchange {} on the control-plane connection; " +
          "relying on RabbitAdmin to declare it on connect (early heartbeats may be dropped)",
        exchange,
        e
      );
    }
    return template;
  }

  /**
   * Structured heartbeat producer: epoch and readiness.
   *
   * <p>Reads the same immutable {@link WorkerIdentity} epoch as {@link WorkerBroadcaster}, so the
   * values in the heartbeat and decision headers are identical, and sends on the
   * heartbeat-dedicated template so a data-plane stall cannot delay it.
   *
   * @param rabbitTemplate  the heartbeat-dedicated RabbitMQ template
   * @param registry        the HOT set for the ready-to-serve check
   * @param identity        this process's immutable node id and epoch
   * @param appProperties   shared {@code zeta.app-name} stamped on the heartbeat
   * @return a new {@link WorkerHeartbeatProducer}
   */
  @Bean
  @ConditionalOnMissingBean
  public WorkerHeartbeatProducer workerHeartbeatProducer(
    @Qualifier("zetaHeartbeatRabbitTemplate") RabbitTemplate rabbitTemplate,
    HotKeyRegistry registry,
    WorkerIdentity identity,
    ZetaAppProperties appProperties
  ) {
    return new WorkerHeartbeatProducer(
      rabbitTemplate,
      properties.getMessaging().getHeartbeatExchange(),
      identity,
      registry,
      properties.getHeartbeat().getReadyGraceMs(),
      appProperties.getAppName()
    );
  }
}
