package io.github.hyshmily.hotkey.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class SystemLoadMonitorTest {

  private static final long DEFAULT_POLL_MS = 500;
  private static final double DEFAULT_DECAY = 0.95;

  @Test
  void deprecatedConstructor_shouldDelegateToFullConstructor() {
    SystemLoadMonitor monitor = new SystemLoadMonitor();
    assertThat(monitor.getCpuLoadEMA()).isEqualTo(0.0);
    monitor.stop();
  }

  @Test
  void constructor_shouldClampNonPositivePollInterval() throws Exception {
    Field pollField = SystemLoadMonitor.class.getDeclaredField("pollIntervalMs");
    pollField.setAccessible(true);

    SystemLoadMonitor zeroPoll = new SystemLoadMonitor(0, 0.5);
    assertThat(pollField.getLong(zeroPoll)).isEqualTo(DEFAULT_POLL_MS);

    SystemLoadMonitor negPoll = new SystemLoadMonitor(-100, 0.5);
    assertThat(pollField.getLong(negPoll)).isEqualTo(DEFAULT_POLL_MS);
  }

  @Test
  void constructor_shouldClampInvalidDecay() throws Exception {
    Field decayField = SystemLoadMonitor.class.getDeclaredField("decay");
    decayField.setAccessible(true);

    assertThat(decayField.getDouble(new SystemLoadMonitor(100, 0))).isEqualTo(DEFAULT_DECAY);
    assertThat(decayField.getDouble(new SystemLoadMonitor(100, -0.1))).isEqualTo(DEFAULT_DECAY);
    assertThat(decayField.getDouble(new SystemLoadMonitor(100, 1.0))).isEqualTo(DEFAULT_DECAY);
    assertThat(decayField.getDouble(new SystemLoadMonitor(100, 1.5))).isEqualTo(DEFAULT_DECAY);
  }

  @Test
  void getCpuLoadEMA_shouldReturnZeroBeforeAnySample() {
    SystemLoadMonitor monitor = new SystemLoadMonitor(100, 0.5);
    assertThat(monitor.getCpuLoadEMA()).isEqualTo(0.0);
    monitor.stop();
  }

  @Test
  void constructor_shouldClampNaNDecayToDefault() throws Exception {
    Field decayField = SystemLoadMonitor.class.getDeclaredField("decay");
    decayField.setAccessible(true);
    assertThat(decayField.getDouble(new SystemLoadMonitor(100, Double.NaN))).isEqualTo(DEFAULT_DECAY);
  }

  @Test
  void sharedSchedulerConstructor_shouldUseProvidedScheduler() {
    var scheduler = Executors.newSingleThreadScheduledExecutor();
    SystemLoadMonitor monitor = new SystemLoadMonitor(scheduler, 100, 0.5);
    monitor.start();
    monitor.stop();
    // shared scheduler should NOT be shut down by stop()
    assertThat(scheduler.isShutdown()).isFalse();
    scheduler.shutdown();
  }

  @Test
  void start_shouldBeIdempotent() {
    SystemLoadMonitor monitor = new SystemLoadMonitor(50, 0.5);
    monitor.start();
    monitor.start();
    monitor.stop();
  }

  @Test
  void start_shouldScheduleSampling() throws Exception {
    TestableMonitor monitor = new TestableMonitor(0.5, 0.8);
    monitor.start();
    Thread.sleep(300);
    monitor.stop();
    assertThat(monitor.getCpuLoadEMA()).isGreaterThan(0);
  }

  @Test
  void stop_shouldNotBreakGetCpuLoadEMA() {
    SystemLoadMonitor monitor = new SystemLoadMonitor(100, 0.5);
    monitor.start();
    monitor.stop();
    assertThat(monitor.getCpuLoadEMA()).isEqualTo(0.0);
  }

  @Test
  void stop_shouldBeSafeToCallMultipleTimes() {
    SystemLoadMonitor monitor = new SystemLoadMonitor(100, 0.5);
    monitor.stop();
    monitor.stop();
  }

  @Test
  void sample_whenRawAboveZero_shouldInitializeEmaDirectly() throws Exception {
    TestableMonitor monitor = new TestableMonitor(0.5, 0.8);
    Method sample = SystemLoadMonitor.class.getDeclaredMethod("sample");
    sample.setAccessible(true);
    sample.invoke(monitor);
    assertThat(monitor.getCpuLoadEMA()).isCloseTo(0.8, within(1e-12));
  }

  @Test
  void sample_whenRawIsZero_shouldNotInitializeEma() throws Exception {
    TestableMonitor monitor = new TestableMonitor(0.5, 0.0);
    Method sample = SystemLoadMonitor.class.getDeclaredMethod("sample");
    sample.setAccessible(true);
    sample.invoke(monitor);
    assertThat(monitor.getCpuLoadEMA()).isEqualTo(0.0);
  }

  @Test
  void sample_shouldApplyEmaFormulaOnSubsequentCalls() throws Exception {
    double decay = 0.6;
    TestableMonitor monitor = new TestableMonitor(decay, 1.0, 0.5, 0.0);
    Method sample = SystemLoadMonitor.class.getDeclaredMethod("sample");
    sample.setAccessible(true);

    sample.invoke(monitor);
    assertThat(monitor.getCpuLoadEMA()).isCloseTo(1.0, within(1e-12));

    sample.invoke(monitor);
    double expected2 = 1.0 * decay + 0.5 * (1.0 - decay);
    assertThat(monitor.getCpuLoadEMA()).isCloseTo(expected2, within(1e-12));

    sample.invoke(monitor);
    double expected3 = expected2 * decay + 0.0 * (1.0 - decay);
    assertThat(monitor.getCpuLoadEMA()).isCloseTo(expected3, within(1e-12));
  }

  @Test
  void getCpuLoadRaw_shouldReturnClampedValue() {
    SystemLoadMonitor monitor = new SystemLoadMonitor();
    assertThat(monitor.getCpuLoadRaw()).isBetween(0.0, 1.0);
    monitor.stop();
  }

  @Test
  void threadSafety_concurrentReads_shouldNotCorruptEma() throws Exception {
    SystemLoadMonitor monitor = new SystemLoadMonitor(10, 0.5);
    monitor.start();
    ExecutorService exec = Executors.newFixedThreadPool(4);
    CountDownLatch latch = new CountDownLatch(1);
    for (int i = 0; i < 4; i++) {
      exec.submit(() -> {
        try {
          latch.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        for (int j = 0; j < 1000; j++) {
          monitor.getCpuLoadEMA();
        }
      });
    }
    latch.countDown();
    exec.shutdown();
    assertThat(exec.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    monitor.stop();
  }

  static class TestableMonitor extends SystemLoadMonitor {
    private final double[] rawValues;
    private int index;

    TestableMonitor(double decay, double... rawValues) {
      super(100, decay);
      this.rawValues = rawValues;
    }

    @Override
    public double getCpuLoadRaw() {
      if (index < rawValues.length) {
        return rawValues[index++];
      }
      return 0.0;
    }
  }
}
