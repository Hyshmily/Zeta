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
package io.github.hyshmily.zeta.worker.dispatch;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.util.LogThrottle;
import io.github.hyshmily.zeta.util.ZetaThreadFactory;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * Bounded single-threaded send buffer for Worker decision broadcasts
 * (ADR-0061, mirroring the App-side ADR-0037 conventions).
 *
 * <p>{@link ReportConsumer} hands every HOT/COOL decision to
 * {@link #submit(Supplier, Runnable)} instead of publishing synchronously on
 * the AMQP consumer thread. The dedicated executor drains the queue serially,
 * so a mass-heat event (thousands of keys transitioning within one batch)
 * never stalls report consumption behind a burst of synchronous AMQP
 * publishes — report ingestion and decision sending are decoupled the same
 * way the report and broadcast planes already are.
 *
 * <p>Failure semantics (all lenient, per ADR-0007/ADR-0024):
 *
 * <ul>
 *   <li><b>Send failure</b> (the send returns {@code false} or throws — the
 *       broadcaster logs the full stack at the send site) — the task's
 *       {@code onFailure} callback runs on the drain thread, which applies
 *       the state-machine rollback so the next evaluation re-emits the
 *       decision.</li>
 *   <li><b>Saturation</b> (the queue is full) — the decision is dropped
 *       immediately and {@code onFailure} runs on the <em>caller</em> thread:
 *       the rollback is applied synchronously, the AMQP consumer is not
 *       blocked, and the next evaluation re-emits the decision.</li>
 *   <li><b>Shutdown</b> — queued decisions are drained once ({@code shutdown()}
 *       lets pending tasks finish); a decision arriving after shutdown is
 *       dropped with its rollback applied.</li>
 * </ul>
 *
 * <p>Send failures and saturation drops are aggregated into at most one WARN
 * per {@value LogThrottle#DEFAULT_WINDOW_MS}ms window (first send site keeps its own
 * detailed log), so a RabbitMQ outage neither floods the log nor produces a
 * per-decision WARN storm.
 *
 * <p>Per-key ordering is preserved: the single drain thread executes tasks in
 * submission order, so two decisions for the same key keep their relative
 * order (the receiver's {@code decisionVersion >=} guard makes even a
 * reordering harmless).
 */
@Internal
@Slf4j
public class WorkerBroadcastBuffer {

  private final ThreadPoolExecutor sendExecutor;
  private final AtomicInteger failedSinceLastLog = new AtomicInteger();

  /**
   * Rate-limits the aggregated failure/drop WARN to one per
   * {@value LogThrottle#DEFAULT_WINDOW_MS}ms window (ADR-0037 convention).
   * Admission is strict — {@link LogThrottle} claims the window with a compare-and-set,
   * so exactly one caller per window logs. The atomicity and the monotonic
   * clock are provided by {@link LogThrottle}.
   */
  private final LogThrottle errorLogThrottle = LogThrottle.perDefaultWindow();

  /**
   * Creates the buffer with a bounded single-threaded drain executor.
   *
   * @param capacity maximum number of pending decisions before drop-on-saturation
   */
  public WorkerBroadcastBuffer(int capacity) {
    this.sendExecutor = new ThreadPoolExecutor(
      1,
      1,
      0L,
      TimeUnit.MILLISECONDS,
      new ArrayBlockingQueue<>(capacity),
      new ZetaThreadFactory("zeta-worker-broadcast"),
      new ThreadPoolExecutor.AbortPolicy()
    );
  }

  /**
   * Submits a decision send for asynchronous execution.
   *
   * @param send      the send task; returns {@code true} on success
   *                  ({@code WorkerBroadcaster} returns {@code false} and logs
   *                  the failure itself)
   * @param onFailure rollback callback, applied on the drain thread after a
   *                  failed send or on the caller thread after a saturation drop
   */
  public void submit(Supplier<Boolean> send, Runnable onFailure) {
    try {
      sendExecutor.execute(() -> doSend(send, onFailure));
    } catch (RejectedExecutionException e) {
      // Queue full or executor shutting down — drop the decision and roll its
      // state back right here so the next evaluation re-emits it (lenient per
      // ADR-0007/ADR-0024). The rollback is lock-only (no I/O), safe on the
      // AMQP consumer thread.
      onFailure.run();
      noteFailure();
    }
  }

  /**
   * Drains one decision on the single send thread: executes the send and
   * applies the rollback callback on failure. Never throws.
   */
  @SuppressWarnings("java:S4276")
  private void doSend(Supplier<Boolean> send, Runnable onFailure) {
    boolean ok;
    try {
      ok = Boolean.TRUE.equals(send.get());
    } catch (Exception e) {
      ok = false;
    }
    if (!ok) {
      onFailure.run();
      noteFailure();
    }
  }

  /**
   * Aggregates one failure (send failure or saturation drop) into the
   * rate-limited WARN: at most one log per {@value LogThrottle#DEFAULT_WINDOW_MS}ms
   * window, reporting the number of failures accumulated since the last log.
   */
  private void noteFailure() {
    int failed = failedSinceLastLog.incrementAndGet();
    if (!errorLogThrottle.tryAcquire()) {
      return;
    }
    log.warn(
      "Failed to broadcast {} decision(s) since last report (send failures and saturation drops; " +
        "states rolled back, next evaluation re-emits; further failures suppressed for {}s)",
      failed,
      LogThrottle.DEFAULT_WINDOW_MS / 1000
    );
    failedSinceLastLog.set(0);
  }

  /**
   * Stops the buffer at context shutdown. Queued decisions are allowed to
   * finish ({@code shutdown()} drains the queue); tasks submitted after
   * shutdown are rejected and their rollbacks applied on the caller thread.
   */
  @PreDestroy
  public void shutdown() {
    sendExecutor.shutdown();
  }
}
