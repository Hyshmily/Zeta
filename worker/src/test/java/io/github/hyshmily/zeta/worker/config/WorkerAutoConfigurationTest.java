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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.hyshmily.zeta.detection.ZetaBayesianSM;
import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import io.github.hyshmily.zeta.worker.detection.Evaluator;
import io.github.hyshmily.zeta.worker.detection.GlobalQpsEstimator;
import io.github.hyshmily.zeta.worker.detection.SlidingWindowDetector;
import io.github.hyshmily.zeta.worker.detection.ThresholdLearner;
import io.github.hyshmily.zeta.worker.dispatch.VerifyConsumer;
import io.github.hyshmily.zeta.worker.dispatch.WorkerBroadcaster;
import io.github.hyshmily.zeta.worker.ingest.ReportConsumer;
import io.github.hyshmily.zeta.worker.persistence.TopKPersistService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Tests for {@link WorkerAutoConfiguration}.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("WorkerAutoConfiguration tests")
class WorkerAutoConfigurationTest {

  private final ApplicationContextRunner runner = new ApplicationContextRunner()
    .withPropertyValues("zeta.worker.enabled=true")
    .withUserConfiguration(MinimalMockConfiguration.class)
    .withConfiguration(AutoConfigurations.of(WorkerAutoConfiguration.class));

  /**
   * Verifies all expected worker beans are created when {@code zeta.worker.enabled=true}.
   */
  @Test
  @DisplayName("all expected worker beans are created when worker enabled")
  void allBeansAreCreatedWhenWorkerEnabled() {
    runner.run(ctx -> {
      assertThat(ctx).hasSingleBean(SlidingWindowDetector.class);
      assertThat(ctx).hasSingleBean(ZetaBayesianSM.class);
      assertThat(ctx).hasSingleBean(WorkerBroadcaster.class);
      assertThat(ctx).hasSingleBean(ReportConsumer.class);
      assertThat(ctx).hasSingleBean(GlobalQpsEstimator.class);
      assertThat(ctx).hasSingleBean(ThresholdLearner.class);
      assertThat(ctx).hasSingleBean(WorkerAutoConfiguration.EvictStaleTask.class);
    });
  }

  /**
   * Verifies no worker beans are created when {@code zeta.worker.enabled=false}.
   */
  @Test
  @DisplayName("no worker beans are created when worker disabled")
  void beansAreNotCreatedWhenWorkerDisabled() {
    new ApplicationContextRunner()
      .withPropertyValues("zeta.worker.enabled=false")
      .withUserConfiguration(MinimalMockConfiguration.class)
      .withConfiguration(AutoConfigurations.of(WorkerAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(SlidingWindowDetector.class);
        assertThat(ctx).doesNotHaveBean(ZetaBayesianSM.class);
        assertThat(ctx).doesNotHaveBean(WorkerBroadcaster.class);
        assertThat(ctx).doesNotHaveBean(ReportConsumer.class);
        assertThat(ctx).doesNotHaveBean(GlobalQpsEstimator.class);
      });
  }

  /**
   * Verifies the {@link SlidingWindowDetector} bean is created with the expected default parameters.
   */
  @Test
  @DisplayName("SlidingWindowDetector has correct default parameters")
  void slidingWindowDetectorHasCorrectDefaults() {
    runner.run(ctx -> {
      SlidingWindowDetector detector = ctx.getBean(SlidingWindowDetector.class);
      assertThat(detector.getWindowSize()).isEqualTo(10);
      assertThat(detector.getThreshold()).isEqualTo(1000);
    });
  }

  /**
   * Verifies that when the hot-threshold is set to zero, the detector uses
   * {@code Long.MAX_VALUE} as the threshold (dynamic threshold mode).
   */
  @Test
  @DisplayName("SlidingWindowDetector uses Long.MAX_VALUE when hot-threshold is zero")
  void slidingWindowDetectorUsesMaxValueWhenThresholdIsZero() {
    new ApplicationContextRunner()
      .withPropertyValues("zeta.worker.enabled=true", "zeta.worker.threshold.hot-threshold=0")
      .withUserConfiguration(MinimalMockConfiguration.class)
      .withConfiguration(AutoConfigurations.of(WorkerAutoConfiguration.class))
      .run(ctx -> {
        SlidingWindowDetector detector = ctx.getBean(SlidingWindowDetector.class);
        assertThat(detector.getThreshold()).isEqualTo(Long.MAX_VALUE);
      });
  }

  /**
   * Verifies that when the hot-threshold is set to a negative value, the detector
   * uses {@code Long.MAX_VALUE} (same as zero — dynamic threshold mode).
   */
  @Test
  @DisplayName("SlidingWindowDetector uses Long.MAX_VALUE when hot-threshold is negative")
  void slidingWindowDetectorUsesMaxValueWhenThresholdIsNegative() {
    new ApplicationContextRunner()
      .withPropertyValues("zeta.worker.enabled=true", "zeta.worker.threshold.hot-threshold=-100")
      .withUserConfiguration(MinimalMockConfiguration.class)
      .withConfiguration(AutoConfigurations.of(WorkerAutoConfiguration.class))
      .run(ctx -> {
        SlidingWindowDetector detector = ctx.getBean(SlidingWindowDetector.class);
        assertThat(detector.getThreshold()).isEqualTo(Long.MAX_VALUE);
      });
  }

  /**
   * Verifies that {@code EvictStaleTask.evictStale} does not throw when called.
   * Fault: scheduled task must never propagate exceptions.
   */
  @Test
  @DisplayName("EvictStaleTask.evictStale should catch exceptions")
  void evictStaleTaskShouldCatchExceptions() {
    runner.run(ctx -> {
      WorkerAutoConfiguration.EvictStaleTask task = ctx.getBean(WorkerAutoConfiguration.EvictStaleTask.class);
      assertThatCode(task::evictStale).doesNotThrowAnyException();
    });
  }

  /**
   * Verifies that {@code validateWindowConfig} does not throw when
   * {@code slidingWindow.durationMs} is not evenly divisible by {@code windowSlices},
   * even though it logs a warning.
   */
  @Test
  @DisplayName("validateWindowConfig should not throw when duration is not evenly divisible by slices")
  void validateWindowConfigShouldLogWarningWhenNotEvenlyDivisible() {
    new ApplicationContextRunner()
      .withPropertyValues(
        "zeta.worker.enabled=true",
        "zeta.worker.sliding-window.duration-ms=1001",
        "zeta.worker.sliding-window.slices=10"
      )
      .withUserConfiguration(MinimalMockConfiguration.class)
      .withConfiguration(AutoConfigurations.of(WorkerAutoConfiguration.class))
      .run(ctx -> {
        assertThatCode(() -> ctx.getBean(SlidingWindowDetector.class)).doesNotThrowAnyException();
      });
  }

  /**
   * Verifies the {@code workerConfigQueue} bean is created with the correct name prefix.
   */
  @Test
  @DisplayName("should create workerConfigQueue bean with correct name prefix")
  void shouldCreateWorkerConfigQueueBean() {
    runner.run(ctx -> {
      Queue queue = ctx.getBean("workerConfigQueue", Queue.class);
      assertThat(queue.getName()).startsWith("zeta.worker.config.");
    });
  }

  /**
   * Verifies the {@code heartbeatExchange} bean is a {@link TopicExchange}.
   */
  @Test
  @DisplayName("should create heartbeatExchange TopicExchange bean")
  void shouldCreateHeartbeatExchangeBean() {
    runner.run(ctx -> {
      assertThat(ctx).hasSingleBean(TopicExchange.class);
    });
  }

  /**
   * Verifies the {@code verifyPingQueue} bean is an auto-delete queue.
   */
  @Test
  @DisplayName("should create verifyPingQueue auto-delete queue bean")
  void shouldCreateVerifyPingQueueBean() {
    runner.run(ctx -> {
      Queue queue = ctx.getBean("verifyPingQueue", Queue.class);
      assertThat(queue.getName()).startsWith("zeta.verify.ping.");
      assertThat(queue.isAutoDelete()).isTrue();
    });
  }

  /**
   * Verifies the {@link VerifyConsumer} bean is created.
   */
  @Test
  @DisplayName("should create VerifyConsumer bean")
  void shouldCreateVerifyConsumerBean() {
    runner.run(ctx -> {
      assertThat(ctx).hasSingleBean(VerifyConsumer.class);
    });
  }

  /**
   * Verifies the {@code reportExchange} bean is a {@link DirectExchange}.
   */
  @Test
  @DisplayName("should create reportExchange DirectExchange bean")
  void shouldCreateReportExchangeBean() {
    runner.run(ctx -> {
      assertThat(ctx).hasSingleBean(DirectExchange.class);
    });
  }

  /**
   * Verifies the {@code configTimestampCounter} bean is an {@link AtomicLong}.
   */
  @Test
  @DisplayName("should create configTimestampCounter AtomicLong bean")
  void shouldCreateConfigTimestampCounterBean() {
    runner.run(ctx -> {
      assertThat(ctx).getBean("configTimestampCounter", AtomicLong.class).isNotNull();
    });
  }

  /**
   * Verifies the auto-configuration is skipped when {@link RabbitTemplate} is not
   * on the classpath ({@code @ConditionalOnClass}), even when
   * {@code zeta.worker.enabled=true}.
   */
  @Test
  @DisplayName("should skip WorkerAutoConfiguration when RabbitTemplate is not on classpath")
  void shouldIgnoreBeansWhenAmqpClassNotPresent() {
    new ApplicationContextRunner()
      .withClassLoader(new FilteredClassLoader(RabbitTemplate.class))
      .withPropertyValues("zeta.worker.enabled=true")
      .withConfiguration(AutoConfigurations.of(WorkerAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(SlidingWindowDetector.class);
        assertThat(ctx).doesNotHaveBean(ZetaBayesianSM.class);
        assertThat(ctx).doesNotHaveBean(WorkerBroadcaster.class);
        assertThat(ctx).doesNotHaveBean(ReportConsumer.class);
      });
  }

  /**
   * Verifies that {@code EvictStaleTask.evictStale()} calls both
   * {@link SlidingWindowDetector#evictStale(long)} and
   * {@link ZetaBayesianSM#evictStale(long)} with the expected stale threshold.
   */
  @Test
  @DisplayName("EvictStaleTask.evictStale should call evictStale on detector and stateMachine")
  void evictStaleTaskShouldCallEvictStaleOnDetectorAndStateMachine() {
    SlidingWindowDetector detector = Mockito.mock(SlidingWindowDetector.class);
    ZetaBayesianSM stateMachine = Mockito.mock(ZetaBayesianSM.class);
    WorkerBroadcaster broadcaster = Mockito.mock(WorkerBroadcaster.class);
    Evaluator evaluator = Mockito.mock(Evaluator.class);
    WorkerProperties properties = new WorkerProperties();

    WorkerAutoConfiguration.EvictStaleTask task = new WorkerAutoConfiguration.EvictStaleTask(
      detector,
      stateMachine,
      evaluator,
      properties,
      broadcaster
    );
    task.evictStale();

    long expectedStaleAfterMs = properties.getStateMachine().getCoolDurationMs() * 2;
    Mockito.verify(detector).evictStale(expectedStaleAfterMs);
    Mockito.verify(stateMachine).evictStale(Mockito.eq(expectedStaleAfterMs), Mockito.any());
  }

  /**
   * Verifies that the {@code workerTopK} HeavyKeeper bean is created.
   */
  @Test
  @DisplayName("should create workerTopK bean")
  void shouldCreateWorkerTopKBean() {
    runner.run(ctx -> assertThat(ctx).hasBean("workerTopK"));
  }

  /**
   * Verifies that the shard-specific {@code reportQueue} bean is created with the
   * correct name prefix.
   */
  @Test
  @DisplayName("should create reportQueue bean with correct name prefix")
  void shouldCreateReportQueueBean() {
    runner.run(ctx -> {
      Queue queue = ctx.getBean("reportQueue", Queue.class);
      assertThat(queue.getName()).startsWith("zeta.reportToWorker.");
    });
  }

  /**
   * Verifies that the {@code reportBinding} bean binds the reportToWorker queue to the
   * reportToWorker exchange.
   */
  @Test
  @DisplayName("should create reportBinding bean")
  void shouldCreateReportBindingBean() {
    runner.run(ctx -> assertThat(ctx).hasBean("reportBinding"));
  }

  /**
   * Verifies that the {@code workerConfigBinding} bean binds the worker config
   * queue to the heartbeat exchange.
   */
  @Test
  @DisplayName("should create workerConfigBinding bean")
  void shouldCreateWorkerConfigBindingBean() {
    runner.run(ctx -> assertThat(ctx).hasBean("workerConfigBinding"));
  }

  /**
   * Verifies the {@link WorkerConfigNegotiator} bean is created with its
   * required dependencies.
   */
  @Test
  @DisplayName("should create WorkerConfigNegotiator bean")
  void shouldCreateWorkerConfigNegotiatorBean() {
    runner.run(ctx -> assertThat(ctx).hasBean("workerConfigNegotiator"));
  }

  /**
   * Verifies the {@code verifyPingContainer} bean (a
   * {@link org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer})
   * is created for the verification PING/PONG protocol.
   */
  @Test
  @DisplayName("should create verifyPingContainer bean")
  void shouldCreateVerifyPingContainerBean() {
    runner.run(ctx -> assertThat(ctx).hasBean("verifyPingContainer"));
  }

  /**
   * Verifies that the {@code thresholdLearningTask} placeholder bean is created
   * by the auto-configuration.
   */
  @Test
  @DisplayName("should create thresholdLearningTask bean")
  void shouldCreateThresholdLearningTaskBean() {
    runner.run(ctx -> assertThat(ctx).hasBean("thresholdLearningTask"));
  }

  /**
   * Verifies the {@link io.github.hyshmily.zeta.worker.dispatch.WorkerHeartbeatProducer}
   * bean is created with all its dependencies.
   */
  @Test
  @DisplayName("should create WorkerHeartbeatProducer bean")
  void shouldCreateWorkerHeartbeatProducerBean() {
    runner.run(ctx -> assertThat(ctx).hasBean("workerHeartbeatProducer"));
  }

  /**
   * Verifies that {@code EvictStaleTask.evictStale()} does not propagate
   * exceptions thrown by the underlying {@link SlidingWindowDetector}.
   */
  @Test
  @DisplayName("EvictStaleTask.evictStale should catch exceptions from detector")
  void evictStaleTaskShouldCatchExceptionsFromDetector() {
    SlidingWindowDetector detector = Mockito.mock(SlidingWindowDetector.class);
    ZetaBayesianSM stateMachine = Mockito.mock(ZetaBayesianSM.class);
    WorkerBroadcaster broadcaster = Mockito.mock(WorkerBroadcaster.class);
    Evaluator evaluator = Mockito.mock(Evaluator.class);
    WorkerProperties properties = new WorkerProperties();
    Mockito.doThrow(new RuntimeException("eviction failed")).when(detector).evictStale(Mockito.anyLong());

    WorkerAutoConfiguration.EvictStaleTask task = new WorkerAutoConfiguration.EvictStaleTask(
      detector,
      stateMachine,
      evaluator,
      properties,
      broadcaster
    );
    assertThatCode(task::evictStale).doesNotThrowAnyException();
  }

  /**
   * Verifies that {@code validateWindowConfig} does not log a warning when
   * {@code slidingWindow.durationMs} is evenly divisible by {@code windowSlices}.
   */
  @Test
  @DisplayName("validateWindowConfig should not warn when duration is evenly divisible by slices")
  void validateWindowConfigShouldNotWarnWhenEvenlyDivisible() {
    assertThatCode(() ->
      new ApplicationContextRunner()
        .withPropertyValues(
          "zeta.worker.enabled=true",
          "zeta.worker.sliding-window.duration-ms=1000",
          "zeta.worker.sliding-window.slices=10"
        )
        .withUserConfiguration(MinimalMockConfiguration.class)
        .withConfiguration(AutoConfigurations.of(WorkerAutoConfiguration.class))
        .run(ctx -> ctx.getBean(SlidingWindowDetector.class))
    ).doesNotThrowAnyException();
  }

  /**
   * Verifies that persistence-related beans ({@link TopKPersistService} and
   * {@code topKPersistTask}) are created when
   * {@code zeta.worker.persistence.enabled=true} and
   * {@link org.springframework.data.redis.core.StringRedisTemplate} is available.
   */
  @Test
  @DisplayName("should create persistence beans when persistence enabled")
  void shouldCreatePersistenceBeansWhenEnabled() {
    new ApplicationContextRunner()
      .withPropertyValues("zeta.worker.enabled=true", "zeta.worker.persistence.enabled=true")
      .withUserConfiguration(MinimalMockConfigurationWithPersistence.class)
      .withConfiguration(AutoConfigurations.of(WorkerAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).hasSingleBean(TopKPersistService.class);
        assertThat(ctx).hasBean("topKPersistTask");
      });
  }

  /**
   * Minimal configuration providing mocked RabbitTemplate, ConnectionFactory, and
   * the shared scheduler for the context runner.
   */
  @Configuration
  static class MinimalMockConfiguration {

    @Bean
    @org.springframework.context.annotation.Primary
    RabbitTemplate rabbitTemplate() {
      return org.mockito.Mockito.mock(RabbitTemplate.class);
    }

    @Bean
    ConnectionFactory connectionFactory() {
      return org.mockito.Mockito.mock(ConnectionFactory.class);
    }

    @Bean("zetaHeartbeatConnectionFactory")
    ConnectionFactory zetaHeartbeatConnectionFactory() {
      return org.mockito.Mockito.mock(ConnectionFactory.class);
    }

    @Bean
    RedisConnectionFactory redisConnectionFactory() {
      return org.mockito.Mockito.mock(RedisConnectionFactory.class);
    }

    @Bean("hotKeyScheduler")
    ScheduledExecutorService hotKeyScheduler() {
      return Executors.newSingleThreadScheduledExecutor();
    }

    @Bean
    SnowflakeIdGenerator snowflakeIdGenerator() {
      return org.mockito.Mockito.mock(SnowflakeIdGenerator.class);
    }
  }

  /**
   * Extends {@link MinimalMockConfiguration} with a mocked
   * {@link org.springframework.data.redis.core.StringRedisTemplate} so that
   * {@link TopKPersistService} can be created when persistence is enabled.
   */
  @Configuration
  static class MinimalMockConfigurationWithPersistence extends MinimalMockConfiguration {

    @Bean
    StringRedisTemplate stringRedisTemplate() {
      return org.mockito.Mockito.mock(StringRedisTemplate.class);
    }
  }
}
