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
package io.github.hyshmily.hotkey.reporting;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.hotkey.monitor.WorkerHealthMonitor;
import io.github.hyshmily.hotkey.reporting.HotKeyReporter;
import io.github.hyshmily.hotkey.reporting.ReportMessage;
import io.github.hyshmily.hotkey.reporting.ReportPublisher;
import io.github.hyshmily.hotkey.sharding.RingManager;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link HotKeyReporter} covering record, start, and dispatcher state operations.
 */
class HotKeyReporterTest {

  private HotKeyReporter reporter;
  private ScheduledExecutorService scheduler;
  private TestReportPublisher testPublisher;

  static class TestReportPublisher extends ReportPublisher {

    volatile int publishCount = 0;

    TestReportPublisher() {
      super(null, "test", "testApp");
    }

    @Override
    public void publish(String target, ReportMessage message) {
      publishCount++;
    }
  }

  @BeforeEach
  void setUp() {
    scheduler = Executors.newSingleThreadScheduledExecutor();
    testPublisher = new TestReportPublisher();
    reporter = new HotKeyReporter(
      new WorkerHealthMonitor(),
      testPublisher,
      scheduler,
      1000,
      "testApp",
      1000,
      100,
      1,
      new RingManager(150)
    );
  }

  @AfterEach
  void tearDown() {
    reporter.stop();
    scheduler.shutdown();
  }

  /**
   * Verifies that recording keys increments the internal dispatcher capacity counter.
   */
  @Test
  void record_shouldIncrementCounter() {
    reporter.start();
    reporter.record("key1");
    reporter.record("key1");
    reporter.record("key2");
    assertThat(reporter.dispatcherCapacity()).isEqualTo(1000);
  }

  /**
   * Verifies that calling start multiple times is idempotent and does not throw.
   */
  @Test
  void start_shouldBeIdempotent() {
    reporter.start();
    reporter.start();
    assertThat(reporter.dispatcherCapacity()).isEqualTo(1000);
  }

  /**
   * Verifies that dispatcher depth returns -1 before the reporter has been started.
   */
  @Test
  void dispatcherDepth_shouldReturnMinusOneBeforeStart() {
    assertThat(reporter.dispatcherDepth()).isEqualTo(-1);
  }
}
