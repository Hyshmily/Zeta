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

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.worker.config.WorkerAutoConfiguration;
import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.worker.detection.GlobalQpsEstimator;
import io.github.hyshmily.hotkey.worker.detection.SlidingWindowDetector;
import io.github.hyshmily.hotkey.worker.detection.ThresholdLearner;
import io.github.hyshmily.hotkey.worker.detection.TopKValidator;
import io.github.hyshmily.hotkey.worker.dispatch.WorkerBroadcaster;
import io.github.hyshmily.hotkey.worker.dispatch.WorkerHeartbeatProducer;
import io.github.hyshmily.hotkey.worker.ingest.ReportConsumer;
import java.lang.reflect.Method;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.data.redis.connection.RedisConnectionFactory;

/**
 * Tests for {@link WorkerApplication}.
 */
@SpringBootTest(
  webEnvironment = SpringBootTest.WebEnvironment.NONE,
  properties = {
    "hotkey.worker.enabled=true",
    "spring.autoconfigure.exclude="
        + "org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration,"
        + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration"
  }
)
class WorkerApplicationTest {

  @Autowired
  private ApplicationContext applicationContext;

  /**
   * Verifies that {@link WorkerApplication} declares a {@code main(String[])} method.
   */
  @Test
  void shouldHaveMainMethod() throws Exception {
    Method main = WorkerApplication.class.getMethod("main", String[].class);
    assertThat(main).isNotNull();
  }

  /**
   * Verifies that {@link WorkerApplication} is annotated with {@code @SpringBootApplication}.
   */
  @Test
  void shouldBeAnnotatedWithSpringBootApplication() {
    org.springframework.boot.autoconfigure.SpringBootApplication annotation =
      WorkerApplication.class.getAnnotation(org.springframework.boot.autoconfigure.SpringBootApplication.class);
    assertThat(annotation).isNotNull();
  }

  /**
   * Verifies that the {@link WorkerApplication} class can be loaded without errors.
   */
  @Test
  void classShouldLoad() {
    assertThat(WorkerApplication.class).isNotNull();
  }

  /**
   * Verifies that the full Spring application context starts successfully with
   * mocked RabbitMQ/Redis infrastructure when {@code hotkey.worker.enabled=true}.
   */
  @Test
  @DisplayName("the application context loads with mocked infrastructure")
  void applicationContextLoadsWhenWorkerEnabled() {
    assertThat(applicationContext).isNotNull();
  }

  /**
   * Verifies that all key Worker beans are present in the context after
   * the full Spring Boot application starts.
   */
  @Test
  @DisplayName("all key worker beans are created")
  void allWorkerBeansAreCreated() {
    assertThat(applicationContext.getBean(SlidingWindowDetector.class)).isNotNull();
    assertThat(applicationContext.getBean(HotKeyStateMachine.class)).isNotNull();
    assertThat(applicationContext.getBean(WorkerBroadcaster.class)).isNotNull();
    assertThat(applicationContext.getBean(ReportConsumer.class)).isNotNull();
    assertThat(applicationContext.getBean(GlobalQpsEstimator.class)).isNotNull();
    assertThat(applicationContext.getBean(ThresholdLearner.class)).isNotNull();
    assertThat(applicationContext.getBean(TopKValidator.class)).isNotNull();
    assertThat(applicationContext.getBean(WorkerHeartbeatProducer.class)).isNotNull();
  }

  /**
   * Verifies that the {@link HotKey} facade bean is also created by
   * {@link io.github.hyshmily.hotkey.autoconfigure.HotKeyFacadeAutoConfiguration}
   * in Worker mode (it receives only the worker-side TopK, not the cache).
   */
  @Test
  @DisplayName("the HotKey facade bean is present")
  void hotKeyFacadeBeanIsCreated() {
    assertThat(applicationContext.getBean(HotKey.class)).isNotNull();
  }

  /**
   * Minimal configuration providing mocked RabbitMQ and Redis infrastructure
   * beans so the full Spring context can start without real services.
   *
   * <p>Mirrors the pattern from
   * {@code io.github.hyshmily.hotkey.worker.config.WorkerAutoConfigurationTest.MinimalMockConfiguration}.
   */
  @TestConfiguration
  @ImportAutoConfiguration(WorkerAutoConfiguration.class)
  static class MinimalMockConfiguration {

    @Bean
    RabbitTemplate rabbitTemplate() {
      return Mockito.mock(RabbitTemplate.class);
    }

    @Bean
    ConnectionFactory connectionFactory() {
      return Mockito.mock(ConnectionFactory.class);
    }

    @Bean
    RedisConnectionFactory redisConnectionFactory() {
      return Mockito.mock(RedisConnectionFactory.class);
    }

    @Bean("hotKeyScheduler")
    ScheduledExecutorService hotKeyScheduler() {
      return Executors.newSingleThreadScheduledExecutor();
    }
  }
}
