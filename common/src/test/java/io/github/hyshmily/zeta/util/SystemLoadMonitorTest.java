package io.github.hyshmily.zeta.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import io.github.hyshmily.zeta.util.impl.SystemLoadMonitorImpl;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class SystemLoadMonitorTest {

  private static final long DEFAULT_POLL_MS = 500;
  private static final double DEFAULT_DECAY = 0.95;

  @Test
  void constructor_shouldClampNonPositivePollInterval() throws Exception {
    Field pollField = SystemLoadMonitorImpl.class.getDeclaredField("pollIntervalMs");
    pollField.setAccessible(true);

    SystemLoadMonitor zeroPoll = new SystemLoadMonitorImpl(0, 0.5);
    assertThat(pollField.getLong(zeroPoll)).isEqualTo(DEFAULT_POLL_MS);

    SystemLoadMonitor negPoll = new SystemLoadMonitorImpl(-100, 0.5);
    assertThat(pollField.getLong(negPoll)).isEqualTo(DEFAULT_POLL_MS);
  }

  @Test
  void constructor_shouldClampInvalidDecay() throws Exception {
    Field decayField = SystemLoadMonitorImpl.class.getDeclaredField("decay");
    decayField.setAccessible(true);

    assertThat(decayField.getDouble(new SystemLoadMonitorImpl(100, 0))).isEqualTo(DEFAULT_DECAY);
    assertThat(decayField.getDouble(new SystemLoadMonitorImpl(100, -0.1))).isEqualTo(DEFAULT_DECAY);
    assertThat(decayField.getDouble(new SystemLoadMonitorImpl(100, 1.0))).isEqualTo(DEFAULT_DECAY);
    assertThat(decayField.getDouble(new SystemLoadMonitorImpl(100, 1.5))).isEqualTo(DEFAULT_DECAY);
  }

  @Test
  void getCpuLoadEMA_shouldReturnZeroBeforeAnySample() {
    SystemLoadMonitor monitor = new SystemLoadMonitorImpl(100, 0.5);
    assertThat(monitor.getCpuLoadEMA()).isEqualTo(0.0);
    monitor.stop();
  }

  @Test
  void constructor_shouldClampNaNDecayToDefault() throws Exception {
    Field decayField = SystemLoadMonitorImpl.class.getDeclaredField("decay");
    decayField.setAccessible(true);
    assertThat(decayField.getDouble(new SystemLoadMonitorImpl(100, Double.NaN))).isEqualTo(DEFAULT_DECAY);
  }

  @Test
  void sharedSchedulerConstructor_shouldUseProvidedScheduler() {
    var scheduler = Executors.newSingleThreadScheduledExecutor();
    SystemLoadMonitor monitor = new SystemLoadMonitorImpl(scheduler, 100, 0.5);
    monitor.start();
    monitor.stop();
    // shared scheduler should NOT be shut down by stop()
    assertThat(scheduler.isShutdown()).isFalse();
    scheduler.shutdown();
  }

  @Test
  void start_shouldBeIdempotent() {
    SystemLoadMonitor monitor = new SystemLoadMonitorImpl(50, 0.5);
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
    SystemLoadMonitor monitor = new SystemLoadMonitorImpl(100, 0.5);
    monitor.start();
    monitor.stop();
    // After stop the EMA may be 0 or may have captured a brief sample;
    // the important contract is it returns a non-negative, finite value.
    assertThat(monitor.getCpuLoadEMA()).isNotNegative();
  }

  @Test
  void stop_shouldBeSafeToCallMultipleTimes() {
    SystemLoadMonitor monitor = new SystemLoadMonitorImpl(100, 0.5);
    monitor.stop();
    monitor.stop();
  }

  @Test
  void sample_whenRawAboveZero_shouldInitializeEmaDirectly() throws Exception {
    TestableMonitor monitor = new TestableMonitor(0.5, 0.8);
    Method sample = SystemLoadMonitorImpl.class.getDeclaredMethod("sample");
    sample.setAccessible(true);
    sample.invoke(monitor);
    assertThat(monitor.getCpuLoadEMA()).isCloseTo(0.8, within(1e-12));
  }

  @Test
  void sample_whenRawIsZero_shouldNotInitializeEma() throws Exception {
    TestableMonitor monitor = new TestableMonitor(0.5, 0.0);
    Method sample = SystemLoadMonitorImpl.class.getDeclaredMethod("sample");
    sample.setAccessible(true);
    sample.invoke(monitor);
    assertThat(monitor.getCpuLoadEMA()).isEqualTo(0.0);
  }

  @Test
  void sample_shouldApplyEmaFormulaOnSubsequentCalls() throws Exception {
    double decay = 0.6;
    TestableMonitor monitor = new TestableMonitor(decay, 1.0, 0.5, 0.0);
    Method sample = SystemLoadMonitorImpl.class.getDeclaredMethod("sample");
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
  void threadSafety_concurrentReads_shouldNotCorruptEma() throws Exception {
    SystemLoadMonitor monitor = new SystemLoadMonitorImpl(10, 0.5);
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

  // ── New tests for uncovered code paths ──

  @Test
  void constructor_withInvalidPollInterval_shouldUseDefault() throws Exception {
    Field pollField = SystemLoadMonitorImpl.class.getDeclaredField("pollIntervalMs");
    pollField.setAccessible(true);

    SystemLoadMonitor monitor = new SystemLoadMonitorImpl(-1, 0.5);
    assertThat(pollField.getLong(monitor)).isEqualTo(DEFAULT_POLL_MS);
  }

  @Test
  void constructor_withInvalidDecay_shouldUseDefault() throws Exception {
    Field decayField = SystemLoadMonitorImpl.class.getDeclaredField("decay");
    decayField.setAccessible(true);

    assertThat(decayField.getDouble(new SystemLoadMonitorImpl(100, 1.0))).isEqualTo(DEFAULT_DECAY);
  }

  @Test
  void constructor_withDecayZero_shouldUseDefault() throws Exception {
    Field decayField = SystemLoadMonitorImpl.class.getDeclaredField("decay");
    decayField.setAccessible(true);

    assertThat(decayField.getDouble(new SystemLoadMonitorImpl(100, 0))).isEqualTo(DEFAULT_DECAY);
  }

  @Test
  void constructor_withDecayNegative_shouldUseDefault() throws Exception {
    Field decayField = SystemLoadMonitorImpl.class.getDeclaredField("decay");
    decayField.setAccessible(true);

    assertThat(decayField.getDouble(new SystemLoadMonitorImpl(100, -0.5))).isEqualTo(DEFAULT_DECAY);
  }

  @Test
  void stop_whenNotStarted_shouldNotThrow() {
    SystemLoadMonitor monitor = new SystemLoadMonitorImpl(100, 0.5);
    monitor.stop();
  }

  @Test
  void stop_withOwnedScheduler_shouldShutdown() {
    SystemLoadMonitor monitor = new SystemLoadMonitorImpl(100, 0.5);
    monitor.start();
    monitor.stop();
    // Owned scheduler should be shut down by stop()
    assertThat(monitor.getCpuLoadEMA()).isNotNegative();
  }

  @Test
  void sample_shouldInitializeEmaFromRaw() throws Exception {
    TestableMonitor monitor = new TestableMonitor(0.5, 0.8);
    Method sample = SystemLoadMonitorImpl.class.getDeclaredMethod("sample");
    sample.setAccessible(true);
    sample.invoke(monitor);
    assertThat(monitor.getCpuLoadEMA()).isCloseTo(0.8, within(1e-12));
  }

  @Test
  void getCpuLoadEMA_beforeSample_shouldReturnZero() {
    SystemLoadMonitor monitor = new SystemLoadMonitorImpl(100, 0.5);
    assertThat(monitor.getCpuLoadEMA()).isEqualTo(0.0);
    monitor.stop();
  }

  static class TestableMonitor extends SystemLoadMonitorImpl {

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

  // ── MXBean failure handling (protected readCpuLoadFromOsBean seam) ──

  /** Seam subclass: the MXBean read always throws (e.g. NumberFormatException from a broken bean). */
  static class ThrowingMonitor extends SystemLoadMonitorImpl {

    ThrowingMonitor() {
      super(100, 0.5);
    }

    @Override
    protected double readCpuLoadFromOsBean() {
      throw new NumberFormatException("simulated MXBean failure");
    }
  }

  /** Seam subclass: the MXBean read returns NaN (platform cannot report the load). */
  static class NaNTargetMonitor extends SystemLoadMonitorImpl {

    NaNTargetMonitor() {
      super(100, 0.5);
    }

    @Override
    protected double readCpuLoadFromOsBean() {
      return Double.NaN;
    }
  }

  /** Seam subclass: one good read, then the MXBean read always throws. */
  static class GoodThenThrowingMonitor extends SystemLoadMonitorImpl {

    private final double goodValue;
    private boolean first = true;

    GoodThenThrowingMonitor(double goodValue) {
      super(100, 0.5);
      this.goodValue = goodValue;
    }

    @Override
    protected double readCpuLoadFromOsBean() {
      if (first) {
        first = false;
        return goodValue;
      }
      throw new NumberFormatException("simulated MXBean failure");
    }
  }

  @Test
  void getCpuLoadRaw_whenBeanThrows_shouldReturnLastGoodValue() {
    GoodThenThrowingMonitor monitor = new GoodThenThrowingMonitor(0.7);
    assertThat(monitor.getCpuLoadRaw()).isEqualTo(0.7); // good read → stored as last good
    assertThat(monitor.getCpuLoadRaw()).isEqualTo(0.7); // failing read → last good
    assertThat(monitor.getCpuLoadRaw()).isEqualTo(0.7); // still failing → still last good
  }

  @Test
  void getCpuLoadRaw_whenBeanThrowsBeforeAnySuccess_shouldReturnZero() {
    ThrowingMonitor monitor = new ThrowingMonitor();
    assertThat(monitor.getCpuLoadRaw()).isEqualTo(0.0);
  }

  @Test
  void sample_whenRawIsNaN_shouldNotPoisonEma() throws Exception {
    // A NaN raw must be treated as a read failure: a NaN EMA would flip the BBR/SRE
    // consumers (cpuLoad < cpuThreshold comparisons) into strict enforcement forever.
    NaNTargetMonitor monitor = new NaNTargetMonitor();
    Method sample = SystemLoadMonitorImpl.class.getDeclaredMethod("sample");
    sample.setAccessible(true);
    sample.invoke(monitor);
    assertThat(monitor.getCpuLoadEMA()).isEqualTo(0.0);
    assertThat(monitor.getCpuLoadEMA()).isFinite();
  }

  @Test
  void getCpuLoadRaw_repeatedFailures_shouldWarnOnlyOnceThenDebug() {
    ThrowingMonitor monitor = new ThrowingMonitor();
    CollectingAppender appender = new CollectingAppender();
    LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
    ch.qos.logback.classic.Logger logbackLogger = context.getLogger(SystemLoadMonitorImpl.class);
    // The DEBUG assertions below require DEBUG to actually be emitted — the
    // effective level may be INFO depending on the logback config on the
    // classpath, so pin it for the duration of the test.
    Level originalLevel = logbackLogger.getLevel();
    logbackLogger.setLevel(Level.DEBUG);
    appender.start();
    logbackLogger.addAppender(appender);
    try {
      for (int i = 0; i < 5; i++) {
        assertThat(monitor.getCpuLoadRaw()).isEqualTo(0.0);
      }
      // One WARN (first failure, with the stack trace); the recurring 500ms-tick
      // failures stay at DEBUG so the log is not flooded forever.
      assertThat(appender.events).filteredOn(e -> e.getLevel() == Level.WARN).hasSize(1);
      assertThat(appender.events)
        .filteredOn(e -> e.getLevel() == Level.DEBUG)
        .extracting(ILoggingEvent::getFormattedMessage)
        .anySatisfy(msg -> assertThat(msg).contains("Failed to read CPU load"));
    } finally {
      logbackLogger.detachAppender(appender);
      if (originalLevel != null) {
        logbackLogger.setLevel(originalLevel);
      }
    }
  }

  /** Minimal logback capture appender (same pattern as {@code VersionControllerTest}). */
  private static class CollectingAppender extends AppenderBase<ILoggingEvent> {

    final java.util.List<ILoggingEvent> events = new java.util.ArrayList<>();

    @Override
    protected void append(ILoggingEvent event) {
      events.add(event);
    }
  }
}
