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
package io.github.hyshmily.zeta.worker.detection.lifecycle;

import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.executor.ZetaThreadFactory;
import io.github.hyshmily.zeta.worker.dispatch.DecisionDispatcher;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * Drives the cool-down checks: one thread per striped wheel, each advancing its wheel, draining the
 * due entries, and submitting whatever COOL decision the registry returns.
 *
 * <p>The schedule's operations are O(1) and allocation-free: a key's check is its
 * {@link HE} node, so a re-arm moves that node and a cool-down unlinks it. A
 * {@link TimerWheel} is advanced by a single owner, so {@code detection.cooling.threads} splits the
 * key space into independent wheels instead of sizing a pool of pollers
 * (design decisions: worker cooling).
 */
@Slf4j
public class CoolingScheduler {

  private final DecisionDispatcher dispatcher;
  private final HotKeyRegistry registry;
  private final CoolingWheels wheels;

  private final List<Thread> threads = new ArrayList<>();

  private volatile boolean running = true;

  /**
   * Creates the scheduler and starts one advancing thread per wheel.
   *
   * @param wheels     the striped cool-down wheels
   * @param registry   the HOT set the checks drive
   * @param dispatcher the single submission point for the resulting COOL decisions
   */
  public CoolingScheduler(CoolingWheels wheels, HotKeyRegistry registry, DecisionDispatcher dispatcher) {
    this.wheels = wheels;
    this.registry = registry;
    this.dispatcher = dispatcher;
    ZetaThreadFactory factory = new ZetaThreadFactory("zeta-worker-cool");
    try {
      for (int i = 0; i < wheels.size(); i++) {
        TimerWheel wheel = wheels.wheel(i);
        Thread thread = factory.newThread(() -> loop(wheel));
        threads.add(thread);
        thread.start();
      }
    } catch (RuntimeException | Error e) {
      // A failed start must not leave the earlier owners running: this bean never reaches
      // @PreDestroy when its constructor throws.
      shutdown();
      throw e;
    }
  }

  /**
   * Advances one wheel and drains the entries it detached, evaluating each under the registry's
   * stripe lock and submitting any COOL decision. The inner loop checks {@code running} so a large
   * detached batch does not outlive shutdown; one poisoned check is logged and left to the repair
   * sweep instead of killing the owner.
   *
   * @param wheel the wheel owned by this thread
   */
  private void loop(TimerWheel wheel) {
    while (running) {
      try {
        wheel.advance(TimeSource.monotonicMillis());
        HE entry;
        while (running && (entry = wheel.pollDetached()) != null) {
          // The registry resolves the entry's own wheel from the key's hash; the evaluation must not
          // depend on which thread handed the entry over.
          dispatcher.submit(registry.evaluate(entry, false));
        }
        if (running) {
          wheel.awaitNext();
        } else {
          return;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      } catch (Exception e) {
        // One poisoned check must never kill a cooling thread: the entry is unscheduled and
        // overdue, so the repair sweep retries it.
        log.warn("Cooling check failed; the entry stays overdue for the repair sweep", e);
      }
    }
  }

  /** Stops the threads at context shutdown. Never throws. */
  @PreDestroy
  public void shutdown() {
    running = false;
    wheels.signalAll();
    for (Thread thread : threads) {
      thread.interrupt();
    }
    // An interrupt during shutdown must not skip the remaining threads: record it, finish the
    // bounded joins, then restore the flag. Returning early leaves later wheels advancing during
    // context teardown.
    boolean interrupted = false;
    for (Thread thread : threads) {
      try {
        thread.join(1_000L);
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    for (Thread thread : threads) {
      if (thread.isAlive()) {
        log.warn(
          "Cooling thread {} did not stop within the shutdown join; it will discard further checks",
          thread.getName()
        );
      }
    }
  }

  /** @return checks currently linked in the cooling wheels */
  public int scheduled() {
    return wheels.scheduledCount();
  }
}
