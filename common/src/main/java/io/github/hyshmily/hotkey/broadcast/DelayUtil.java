package io.github.hyshmily.hotkey.broadcast;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Utility for introducing random jitter before executing a task.
 * Package-private; used internally by broadcast listeners to spread
 * Redis reads evenly across instances.
 */
final class DelayUtil {

  /**
   * Execute the task after a random delay uniformly distributed
   * between {@code 0} and {@code jitterMs}.  If {@code jitterMs} is
   * {@code 0} or negative the task runs immediately.
   *
   * @param task      the task to execute
   * @param jitterMs  maximum jitter delay in milliseconds
   * @param scheduler the scheduler to use for delayed execution
   */
  static void floatTimeDelay(Runnable task, long jitterMs, ScheduledExecutorService scheduler) {
    if (jitterMs > 0) {
      long delay = ThreadLocalRandom.current().nextLong(jitterMs);
      if (delay > 0) {
        scheduler.schedule(task, delay, TimeUnit.MILLISECONDS);
        return;
      }
    }
    task.run();
  }

  private DelayUtil() {}
}
