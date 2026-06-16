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
import io.github.hyshmily.hotkey.worker.dispatch.WorkerBroadcaster;
import io.github.hyshmily.hotkey.worker.ingest.ReportConsumer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Tests for {@link WorkerAutoConfiguration}.
 */
@ExtendWith(MockitoExtension.class)
class WorkerAutoConfigurationTest {

  private final ApplicationContextRunner runner = new ApplicationContextRunner()
    .withPropertyValues("hotkey.worker.enabled=true")
    .withUserConfiguration(MinimalMockConfiguration.class)
    .withConfiguration(AutoConfigurations.of(WorkerAutoConfiguration.class));

  /**
   * Verifies all expected worker beans are created when {@code hotkey.worker.enabled=true}.
   */
  @Test
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
  void evictStaleTaskShouldCatchExceptions() {
    runner.run(ctx -> {
      WorkerAutoConfiguration.EvictStaleTask task = ctx.getBean(WorkerAutoConfiguration.EvictStaleTask.class);
      // should not throw — exceptions are caught internally
      assertThatCode(task::evictStale).doesNotThrowAnyException();
    });
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
