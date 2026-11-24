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

import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.executor.ZetaThreadFactory;
import io.github.hyshmily.zeta.worker.dispatch.DecisionDispatcher;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
public class CoolingScheduler implements SmartLifecycle {

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
   * detached batch does not outlive shutdown; one poisoned check is caught at the entry boundary, so
   * it cannot skip the rest of the batch or kill the wheel owner. A failure before the verdict
   * commits leaves the entry unscheduled for the repair pass; a failure after a cool or re-arm has
   * committed loses only that COOL or sample, which the App's hard TTL or the next check bounds. A
   * failed submission loses only that COOL.
   *
   * @param wheel the wheel owned by this thread
   */
  private void loop(TimerWheel wheel) {
    while (running) {
      try {
        wheel.advance(TimeSource.monotonicMillis());
        HE entry;
        while (running && (entry = wheel.pollAndDetached()) != null) {
          evaluate(entry);
        }
        if (!running) {
          return;
        }
        wheel.awaitNext();
      } catch (InterruptedException e) {
        if (!running) {
          Thread.currentThread().interrupt();
          return;
        }
        log.warn("Cooling thread {} was interrupted while running; continuing", Thread.currentThread().getName());
      } catch (Exception e) {
        log.warn("Cooling wheel advance/wait failed", e);
      }
    }
  }

  /**
   * Evaluates one due check and submits its COOL. The two steps fail independently: an evaluation
   * that fails before its verdict commits leaves the entry for the repair pass, while a failure
   * after a cool or re-arm has committed loses only that COOL or sample. A failed submission loses
   * only this COOL. Both catch {@link Throwable} at this boundary because a dead wheel owner leaves
   * every key on its wheel unscheduled until the App's hard TTL, and the repair pass cannot replace
   * the owner.
   *
   * @param entry the due entry the wheel handed over
   */
  @SuppressWarnings("java:S1181")
  private void evaluate(HE entry) {
    // The registry resolves the entry's own wheel from the key's hash; the evaluation must not
    // depend on which thread handed the entry over. A null decision means the check re-armed,
    // recovered or found the entry already retired.
    ZetaDecision decision;
    try {
      decision = registry.evaluate(entry);
    } catch (Throwable t) {
      log.warn("Cooling evaluation failed for key={}", entry.key(), t);
      return;
    }
    if (decision == null) {
      return;
    }
    try {
      dispatcher.submit(decision);
    } catch (Throwable t) {
      log.warn("Cooling decision submission failed for key={}", decision.cacheKey(), t);
    }
  }

  /**
   * The advancing threads are started by the constructor, so the lifecycle start has nothing to do.
   * Implemented so {@link #stop()} runs before singleton destruction; the dispatcher's final drain
   * then starts only after these threads have stopped submitting.
   */
  @Override
  public void start() {
    // The constructor starts the advancing threads.
  }

  @Override
  public void stop() {
    shutdown();
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  /**
   * Stops before the dispatcher ({@code MAX_VALUE - 20}) so no COOL is submitted during its final
   * drain.
   */
  @Override
  public int getPhase() {
    return Integer.MAX_VALUE - 10;
  }

  @PreDestroy
  public void shutdown() {
    running = false;
    wheels.signalAll();
    for (Thread thread : threads) {
      thread.interrupt();
    }
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
    boolean interrupted = false;
    for (Thread thread : threads) {
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0L) {
        break;
      }
      try {
        TimeUnit.NANOSECONDS.timedJoin(thread, remaining);
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
