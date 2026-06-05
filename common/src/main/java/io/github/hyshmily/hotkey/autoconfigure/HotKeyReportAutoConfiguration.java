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

import io.github.hyshmily.hotkey.constant.HotKeyConstants;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyProperties;
import io.github.hyshmily.hotkey.report.HotKeyReporter;
import io.github.hyshmily.hotkey.report.ReportPublisher;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Autoconfiguration for hot key reporting (app instance → Worker).
 * <p>
 * Activates when RabbitTemplate is present and {@code hotkey.report.enabled} is true (default).
 * Creates {@link ReportPublisher} (RabbitMQ sender) and {@link HotKeyReporter} (batch aggregator).
 */
@AutoConfiguration(after = RabbitAutoConfiguration.class)
@ConditionalOnBean(RabbitTemplate.class)
@ConditionalOnProperty(prefix = "hotkey.report", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(HotKeyProperties.class)
public class HotKeyReportAutoConfiguration {

  /**
   * Declare the DirectExchange for report routing (app → Worker).
   * Routing keys ({@code report.<appName>.<shardIndex>}) ensure each shard's
   * messages land on the correct worker queue.
   */
  @Bean
  public DirectExchange hotkeyReportExchange(HotKeyProperties properties) {
    return new DirectExchange(properties.getReportExchange(), true, false);
  }

  /**
   * Create the {@link ReportPublisher} for sending batched access-count reports to the Worker.
   */
  @Bean
  @ConditionalOnMissingBean
  public ReportPublisher reportPublisher(RabbitTemplate rabbitTemplate, HotKeyProperties properties) {
    return new ReportPublisher(rabbitTemplate, properties.getReportExchange(), properties.getAppName());
  }

  /**
   * Create the {@link HotKeyReporter} that aggregates per-key counts and flushes them
   * at the configured interval.
   */
  @Bean(initMethod = "start", destroyMethod = "stop")
  @ConditionalOnMissingBean
  public HotKeyReporter hotKeyReporter(
    ReportPublisher reportPublisher,
    ScheduledExecutorService hotKeyReportScheduler,
    HotKeyProperties properties
  ) {
    return new HotKeyReporter(
      reportPublisher,
      hotKeyReportScheduler,
      properties.getReportIntervalMs(),
      properties.getShardCount(),
      properties.getAppName(),
      properties.getQueueCapacity(),
      properties.getQueueOfferTimeoutMs(),
      properties.effectiveConsumerCount()
    );
  }

  /**
   * Dedicated scheduler for periodic report flushing.
   */
  @Bean(destroyMethod = "shutdown")
  public ScheduledExecutorService hotKeyReportScheduler() {
    return Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, HotKeyConstants.THREAD_PREFIX_REPORT);
      t.setDaemon(true);
      return t;
    });
  }
}
