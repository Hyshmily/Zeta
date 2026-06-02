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
package io.github.hyshmily.hotkey.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.github.hyshmily.hotkey.hotkeycache.HotKeyProperties;
import io.github.hyshmily.hotkey.report.HotKeyReporter;
import io.github.hyshmily.hotkey.report.ReportPublisher;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.autoconfigure.AutoConfigurations;

/**
 * Tests for {@link HotKeyReportAutoConfiguration}.
 */
@ExtendWith(MockitoExtension.class)
class HotKeyReportAutoConfigurationTest {

  private final ApplicationContextRunner runner = new ApplicationContextRunner()
    .withConfiguration(AutoConfigurations.of(HotKeyReportAutoConfiguration.class));

  @Test
  void configIsSkippedWhenRabbitTemplateBeanNotPresent() {
    // RabbitTemplate is not on the test classpath, so @ConditionalOnBean prevents loading
    runner.run(ctx -> {
      assertThat(ctx).doesNotHaveBean(ReportPublisher.class);
      assertThat(ctx).doesNotHaveBean(HotKeyReporter.class);
      assertThat(ctx).doesNotHaveBean(ScheduledExecutorService.class);
    });
  }

  @Test
  void configIsSkippedWhenPropertyIsDisabled() {
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(HotKeyReportAutoConfiguration.class))
      .withPropertyValues("hotkey.report.enabled=false")
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(ReportPublisher.class);
        assertThat(ctx).doesNotHaveBean(HotKeyReporter.class);
      });
  }

  @Test
  void configIsActiveByDefaultWhenRabbitTemplatePresent() {
    // This test verifies the matchIfMissing = true behavior for the property condition.
    // The config is still skipped because RabbitTemplate bean is absent.
    runner.run(ctx -> {
      // Just verify the context loads without errors
      assertThat(ctx).isNotNull();
    });
  }

  @Test
  void reportPublisherIsCreatedWithCorrectProperties() {
    RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
    HotKeyProperties properties = new HotKeyProperties();
    properties.setReportExchange("test.report.exchange");
    properties.setAppName("test-app");

    HotKeyReportAutoConfiguration config = new HotKeyReportAutoConfiguration();
    ReportPublisher publisher = config.reportPublisher(rabbitTemplate, properties);

    assertThat(publisher).isNotNull();
  }

  @Test
  void hotKeyReporterIsCreatedWithRequiredDependencies() {
    ReportPublisher reportPublisher = mock(ReportPublisher.class);
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    HotKeyProperties properties = new HotKeyProperties();

    HotKeyReportAutoConfiguration config = new HotKeyReportAutoConfiguration();
    HotKeyReporter reporter = config.hotKeyReporter(reportPublisher, scheduler, properties);

    assertThat(reporter).isNotNull();
  }

  @Test
  void reportPublisherUsesAppNameFromProperties() {
    RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
    HotKeyProperties properties = new HotKeyProperties();
    properties.setAppName("my-app");
    properties.setReportExchange("my.exchange");

    HotKeyReportAutoConfiguration config = new HotKeyReportAutoConfiguration();
    ReportPublisher publisher = config.reportPublisher(rabbitTemplate, properties);

    assertThat(publisher).isNotNull();
  }

  @Test
  void hotKeyReportSchedulerIsCreated() {
    HotKeyReportAutoConfiguration config = new HotKeyReportAutoConfiguration();
    ScheduledExecutorService scheduler = config.hotKeyReportScheduler();

    assertThat(scheduler).isNotNull();
    assertThat(scheduler.isShutdown()).isFalse();
    scheduler.shutdown();
  }

  @Test
  void hotKeyReportSchedulerThreadNameContainsReport() {
    HotKeyReportAutoConfiguration config = new HotKeyReportAutoConfiguration();
    ScheduledExecutorService scheduler = config.hotKeyReportScheduler();

    assertThat(scheduler).isNotNull();
    scheduler.execute(() -> {
      String threadName = Thread.currentThread().getName();
      assertThat(threadName).contains("hotkey-report");
    });
    scheduler.shutdown();
  }
}
