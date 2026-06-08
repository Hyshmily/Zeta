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

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.worker.detection.GlobalQpsEstimator;
import io.github.hyshmily.hotkey.worker.detection.SlidingWindowDetector;
import io.github.hyshmily.hotkey.worker.detection.ThresholdLearner;
import io.github.hyshmily.hotkey.worker.detection.TopKValidator;
import io.github.hyshmily.hotkey.worker.dispatch.WorkerBroadcaster;
import io.github.hyshmily.hotkey.worker.ingest.ReportConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Tests for {@link WorkerAutoConfiguration}.
 */
@ExtendWith(MockitoExtension.class)
class WorkerAutoConfigurationTest {

  private final ApplicationContextRunner runner = new ApplicationContextRunner()
    .withPropertyValues("hotkey.worker.enabled=true")
    .withUserConfiguration(MinimalMockConfiguration.class)
    .withConfiguration(AutoConfigurations.of(WorkerAutoConfiguration.class));

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

  @Test
  void slidingWindowDetectorHasCorrectDefaults() {
    runner.run(ctx -> {
      SlidingWindowDetector detector = ctx.getBean(SlidingWindowDetector.class);
      assertThat(detector.getWindowSize()).isEqualTo(10);
      assertThat(detector.getThreshold()).isEqualTo(1000);
    });
  }

  /**
   * Minimal configuration providing mocked RabbitTemplate for the context runner.
   */
  @Configuration
  static class MinimalMockConfiguration {

    @Bean
    RabbitTemplate rabbitTemplate() {
      return org.mockito.Mockito.mock(RabbitTemplate.class);
    }
  }
}
