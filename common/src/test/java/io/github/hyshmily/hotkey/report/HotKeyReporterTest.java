package io.github.hyshmily.hotkey.report;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
    public void publish(int shardIndex, ReportMessage message) {
      publishCount++;
    }
  }

  @BeforeEach
  void setUp() {
    scheduler = Executors.newSingleThreadScheduledExecutor();
    testPublisher = new TestReportPublisher();
    reporter = new HotKeyReporter(
      testPublisher, scheduler, 1000, 1, "testApp", 1000, 100, 1
    );
  }

  @AfterEach
  void tearDown() {
    reporter.stop();
    scheduler.shutdown();
  }

  @Test
  void record_shouldIncrementCounter() {
    reporter.start();
    reporter.record("key1");
    reporter.record("key1");
    reporter.record("key2");
    assertThat(reporter.dispatcherCapacity()).isEqualTo(1000);
  }

  @Test
  void start_shouldBeIdempotent() {
    reporter.start();
    reporter.start();
    assertThat(reporter.dispatcherCapacity()).isEqualTo(1000);
  }

  @Test
  void dispatcherDepth_shouldReturnMinusOneBeforeStart() {
    assertThat(reporter.dispatcherDepth()).isEqualTo(-1);
  }
}
