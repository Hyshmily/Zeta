package io.github.hyshmily.hotkey.broadcast;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class DelayUtilTest {

  @Test
  void floatTimeDelay_zeroJitter_shouldRunImmediately() {
    AtomicBoolean executed = new AtomicBoolean(false);
    DelayUtil.floatTimeDelay(() -> executed.set(true), 0, Executors.newSingleThreadScheduledExecutor());
    assertThat(executed).isTrue();
  }

  @Test
  void floatTimeDelay_negativeJitter_shouldRunImmediately() {
    AtomicBoolean executed = new AtomicBoolean(false);
    DelayUtil.floatTimeDelay(() -> executed.set(true), -1, Executors.newSingleThreadScheduledExecutor());
    assertThat(executed).isTrue();
  }

  @Test
  void floatTimeDelay_positiveJitter_shouldExecuteEventually() throws InterruptedException {
    AtomicBoolean executed = new AtomicBoolean(false);
    CountDownLatch latch = new CountDownLatch(1);
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    DelayUtil.floatTimeDelay(() -> {
      executed.set(true);
      latch.countDown();
    }, 100, scheduler);

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(executed).isTrue();
    scheduler.shutdown();
  }
}
