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
import static org.mockito.Mockito.mock;

import io.github.hyshmily.zeta.autoconfigure.ZetaAmqpAutoConfiguration;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.worker.detection.lifecycle.CoolingScheduler;
import io.github.hyshmily.zeta.worker.dispatch.DecisionDispatcher;
import io.github.hyshmily.zeta.worker.dispatch.WorkerBroadcaster;
import io.github.hyshmily.zeta.worker.dispatch.ingest.ReportConsumer;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The Worker's bean graph is its delivery contract: the decision broadcaster must use the
 * data-plane connection, and enabling reporting on a Worker must not make the context fail.
 */
class WorkerContextTest {

  @TempDir
  Path tempDir;

  @Configuration(proxyBeanMethods = false)
  static class TestSchedulers {

    @Bean(name = "hotKeyScheduler", destroyMethod = "shutdownNow")
    ScheduledExecutorService hotKeyScheduler() {
      return Executors.newSingleThreadScheduledExecutor();
    }
  }

  private ApplicationContextRunner runner(boolean reportEnabled) {
    return runner(reportEnabled, "rabbitConnectionFactory");
  }

  private ApplicationContextRunner runner(boolean reportEnabled, String dataPlaneBeanName) {
    ConnectionFactory dataPlane = mock(ConnectionFactory.class);
    ConnectionFactory controlPlane = mock(ConnectionFactory.class);
    RabbitTemplate heartbeatTemplate = mock(RabbitTemplate.class);
    return new ApplicationContextRunner()
      .withUserConfiguration(WorkerAutoConfiguration.class, TestSchedulers.class)
      .withConfiguration(AutoConfigurations.of(ZetaAmqpAutoConfiguration.class))
      .withBean(dataPlaneBeanName, ConnectionFactory.class, () -> dataPlane)
      .withBean("zetaHeartbeatConnectionFactory", ConnectionFactory.class, () -> controlPlane)
      .withBean("zetaHeartbeatRabbitTemplate", RabbitTemplate.class, () -> heartbeatTemplate)
      .withPropertyValues(
        "zeta.worker.enabled=true",
        "zeta.report.enabled=" + reportEnabled,
        "zeta.worker.epoch-file=" + tempDir.resolve("epoch-" + dataPlaneBeanName + "-" + reportEnabled),
        "zeta.worker.detection.defaults.hot-qps=10",
        "zeta.worker.detection.defaults.cool.strategy=COOL_FOR_LOW_QPS",
        "zeta.worker.detection.defaults.cool.cool-qps=1",
        "zeta.worker.detection.defaults.cool.low-qps-for=2s"
      );
  }

  /**
   * A configuration class under the application's scan base package must be listed as an
   * auto-configuration, not picked up by component scanning: the scan runs before
   * {@code @EnableAutoConfiguration}'s deferred imports, so its {@code @ConditionalOnMissingBean}
   * decisions would otherwise be made before the common module's auto-configurations exist.
   */
  @Test
  void workerAutoConfigurationIsRegisteredAsAnAutoConfiguration() throws Exception {
    assertThat(WorkerAutoConfiguration.class.isAnnotationPresent(AutoConfiguration.class)).isTrue();
    try (
      InputStream imports = getClass()
        .getClassLoader()
        .getResourceAsStream("META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports")
    ) {
      assertThat(imports).isNotNull();
      assertThat(new String(imports.readAllBytes(), StandardCharsets.UTF_8))
        .contains(WorkerAutoConfiguration.class.getName());
    }
  }

  @Test
  void workerContextStartsWithReportingEnabledAndBroadcastsOnTheDataPlane() {
    runner(true).run(context -> {
      assertThat(context).hasNotFailed();
      assertThat(context).hasSingleBean(WorkerBroadcaster.class);
      assertThat(context).hasSingleBean(ReportConsumer.class);
      assertThat(context).hasSingleBean(DecisionDispatcher.class);
      assertThat(context).hasSingleBean(CoolingScheduler.class);

      RabbitTemplate broadcastTemplate = context.getBean("zetaWorkerBroadcastRabbitTemplate", RabbitTemplate.class);
      assertThat(broadcastTemplate.getConnectionFactory())
        .as("HOT/COOL must never share the heartbeat connection")
        .isSameAs(context.getBean("rabbitConnectionFactory", ConnectionFactory.class));

      assertThat(context).doesNotHaveBean("zetaReportRabbitTemplate");
      assertThat(context).doesNotHaveBean(KeyReporter.class);
    });
  }

  @Test
  void workerContextStartsWithReportingDisabled() {
    runner(false).run(context -> {
      assertThat(context).hasNotFailed();
      assertThat(context).hasSingleBean(WorkerBroadcaster.class);
      assertThat(
        context.getBean("zetaWorkerBroadcastRabbitTemplate", RabbitTemplate.class).getConnectionFactory()
      ).isSameAs(context.getBean("rabbitConnectionFactory", ConnectionFactory.class));
    });
  }

  @Test
  void workerContextResolvesACustomNamedDataPlaneFactory() {
    String beanName = "ordersRabbitConnectionFactory";
    runner(true, beanName).run(context -> {
      assertThat(context).hasNotFailed();
      assertThat(
        context.getBean("zetaWorkerBroadcastRabbitTemplate", RabbitTemplate.class).getConnectionFactory()
      ).isSameAs(context.getBean(beanName, ConnectionFactory.class));
    });
  }
}
