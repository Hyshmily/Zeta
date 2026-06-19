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

import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.worker.detection.GlobalQpsEstimator;
import io.github.hyshmily.hotkey.worker.detection.SlidingWindowDetector;
import io.github.hyshmily.hotkey.worker.detection.ThresholdLearner;
import io.github.hyshmily.hotkey.worker.detection.TopKValidator;
import io.github.hyshmily.hotkey.worker.dispatch.VerifyConsumer;
import io.github.hyshmily.hotkey.worker.dispatch.WorkerBroadcaster;
import io.github.hyshmily.hotkey.worker.ingest.ReportConsumer;
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
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Tests for {@link WorkerAutoConfiguration}.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("WorkerAutoConfiguration tests")
class WorkerAutoConfigurationTest {

  private final ApplicationContextRunner runner = new ApplicationContextRunner()
    .withPropertyValues("hotkey.worker.enabled=true")
    .withUserConfiguration(MinimalMockConfiguration.class)
    .withConfiguration(AutoConfigurations.of(WorkerAutoConfiguration.class));

  /**
   * Verifies all expected worker beans are created when {@code hotkey.worker.enabled=true}.
   */
  @Test
  @DisplayName("all expected worker beans are created when worker enabled")
  void allBeansAreCreatedWhenWorkerEnabled() {
    runner.run(ctx -> {
      assertThat(ctx).hasSingleBean(SlidingWindowDetector.class);
      assertThat(ctx).hasSingleBean(HotKeyStateMachine.class);
      assertThat(ctx).hasSingleBean(TopKValidator.class);
      assertThat(ctx).hasSingleBean(WorkerBroadcaster.class);
      assertThat(ctx).hasSingleBean(ReportConsumer.class);
      assertThat(ctx).hasSingleBean(GlobalQpsEstimator.class);
      assertThat(ctx).hasSingleBean(ThresholdLearner.class);
      assertThat(ctx).hasSingleBean(WorkerAutoConfiguration.EvictStaleTask.class);
      assertThat(ctx).hasSingleBean(WorkerAutoConfiguration.TopKValidationTask.class);
    });
  }

  /**
   * Verifies no worker beans are created when {@code hotkey.worker.enabled=false}.
   */
  @Test
  @DisplayName("no worker beans are created when worker disabled")
  void beansAreNotCreatedWhenWorkerDisabled() {
    new ApplicationContextRunner()
      .withPropertyValues("hotkey.worker.enabled=false")
      .withUserConfiguration(MinimalMockConfiguration.class)
      .withConfiguration(AutoConfigurations.of(WorkerAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(SlidingWindowDetector.class);
        assertThat(ctx).doesNotHaveBean(HotKeyStateMachine.class);
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
      .withPropertyValues(
        "hotkey.worker.enabled=true",
        "hotkey.worker.threshold.hot-threshold=0"
      )
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
      .withPropertyValues(
        "hotkey.worker.enabled=true",
        "hotkey.worker.threshold.hot-threshold=-100"
      )
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
        "hotkey.worker.enabled=true",
        "hotkey.worker.sliding-window.duration-ms=1001",
        "hotkey.worker.sliding-window.slices=10"
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
      assertThat(queue.getName()).startsWith("hotkey.worker.config.");
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
      assertThat(queue.getName()).startsWith("hotkey.verify.ping.");
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
      assertThat(ctx).hasSingleBean(AtomicLong.class);
    });
  }

  /**
   * Verifies the auto-configuration is skipped when {@link RabbitTemplate} is not
   * on the classpath ({@code @ConditionalOnClass}), even when
   * {@code hotkey.worker.enabled=true}.
   */
  @Test
  @DisplayName("should skip WorkerAutoConfiguration when RabbitTemplate is not on classpath")
  void shouldIgnoreBeansWhenAmqpClassNotPresent() {
    new ApplicationContextRunner()
      .withClassLoader(new FilteredClassLoader(RabbitTemplate.class))
      .withPropertyValues("hotkey.worker.enabled=true")
      .withConfiguration(AutoConfigurations.of(WorkerAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(SlidingWindowDetector.class);
        assertThat(ctx).doesNotHaveBean(HotKeyStateMachine.class);
        assertThat(ctx).doesNotHaveBean(WorkerBroadcaster.class);
        assertThat(ctx).doesNotHaveBean(ReportConsumer.class);
      });
  }

  /**
   * Verifies that {@code TopKValidationTask.validate()} does not throw.
   * Mirrors the {@code evictStaleTaskShouldCatchExceptions} pattern.
   */
  @Test
  @DisplayName("TopKValidationTask.validate should catch exceptions")
  void topKValidationTaskShouldCatchExceptions() {
    runner.run(ctx -> {
      WorkerAutoConfiguration.TopKValidationTask task = ctx.getBean(WorkerAutoConfiguration.TopKValidationTask.class);
      assertThatCode(task::validate).doesNotThrowAnyException();
    });
  }

  /**
   * Verifies that {@code EvictStaleTask.evictStale()} calls both
   * {@link SlidingWindowDetector#evictStale(long)} and
   * {@link HotKeyStateMachine#evictStale(long)} with the expected stale threshold.
   */
  @Test
  @DisplayName("EvictStaleTask.evictStale should call evictStale on detector and stateMachine")
  void evictStaleTaskShouldCallEvictStaleOnDetectorAndStateMachine() {
    SlidingWindowDetector detector = Mockito.mock(SlidingWindowDetector.class);
    HotKeyStateMachine stateMachine = Mockito.mock(HotKeyStateMachine.class);
    WorkerProperties properties = new WorkerProperties();

    WorkerAutoConfiguration.EvictStaleTask task =
      new WorkerAutoConfiguration.EvictStaleTask(detector, stateMachine, properties);
    task.evictStale();

    long expectedStaleAfterMs = properties.getStateMachine().getCoolDurationMs() * 2;
    Mockito.verify(detector).evictStale(expectedStaleAfterMs);
    Mockito.verify(stateMachine).evictStale(expectedStaleAfterMs);
  }

  /**
   * Minimal configuration providing mocked RabbitTemplate, ConnectionFactory, and
   * the shared scheduler for the context runner.
   */
  @Configuration
  static class MinimalMockConfiguration {

    @Bean
    RabbitTemplate rabbitTemplate() {
      return org.mockito.Mockito.mock(RabbitTemplate.class);
    }

    @Bean
    ConnectionFactory connectionFactory() {
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
  }
}
