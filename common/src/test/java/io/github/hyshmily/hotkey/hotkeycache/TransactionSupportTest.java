package io.github.hyshmily.hotkey.hotkeycache;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TransactionSupport}, verifying sync and async execution outside transaction context.
 */
class TransactionSupportTest {

  @Test
  void runNowOrAfterCommit_shouldExecuteSynchronouslyOutsideTransaction() {
    AtomicBoolean executed = new AtomicBoolean(false);
    TransactionSupport.runNowOrAfterCommit(() -> executed.set(true));
    assertThat(executed).isTrue();
  }

  @Test
  void runAsyncAfterCommit_shouldExecuteAsyncOutsideTransaction() throws InterruptedException {
    AtomicBoolean executed = new AtomicBoolean(false);
    CountDownLatch latch = new CountDownLatch(1);
    Executor executor = Executors.newSingleThreadExecutor();

    TransactionSupport.runAsyncAfterCommit(() -> {
      executed.set(true);
      latch.countDown();
    }, executor);

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(executed).isTrue();
  }
}
