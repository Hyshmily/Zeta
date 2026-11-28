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
package io.github.hyshmily.zeta.cache;

import static io.github.hyshmily.zeta.constants.ZetaConstants.TOPK_INCR;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.counter.WaveCounter;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.util.CollectionSizing;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * Single buffered access counter behind the read path: every served read increments one
 * {@link WaveCounter}, and each delivered tide fans out to both the local {@link HotKeyDetector}
 * and the optional {@link KeyReporter}.
 *
 * <pre>
 *   read ──► AccessCounter WaveCounter (report cadence)
 *              ├─ every tide    ──► reporter.publishBatch (route/publish)
 *              └─ every >=500ms ──► detector.ingest (merged batch)
 * </pre>
 *
 * <p>Sharing one reservoir guarantees both pipelines observe consistent volumes under one capacity
 * bound. The reporter leg tracks its configured cadence (tuned by its feedback loop), while the
 * detector leg merges tides in a bounded accumulator flushed at {@code detectorIntervalMs}.
 *
 * <p><b>Tag-mode routing.</b> {@link Zeta.TagMode#RECORD} feeds the shared {@link WaveCounter};
 * {@link Zeta.TagMode#DETECT_ONLY} updates the sketch directly via
 * {@link HotKeyDetector#addDirect}; {@link Zeta.TagMode#REPORT_ONLY} publishes directly to the
 * reporter without entering the local sketch; {@link Zeta.TagMode#SILENT} is a no-op.
 *
 * <p><b>Thread safety.</b> {@link #record(String, boolean)} is lock-free via {@link WaveCounter}.
 * Detector accumulation and scheduled flushes serialize on {@code this} so shutdown draining never
 * overlaps an active tide, while remote reporting executes outside the monitor.
 */
@Slf4j
@Internal
public class AccessCounter implements InitializingBean, DisposableBean {

  /**
   * Default cap on distinct keys buffered between two sketch ingests. Overflow flushes early
   * instead of dropping counts so detection quality is preserved under key-churn scans.
   */
  public static final int DEFAULT_MAX_ACCUMULATED_KEYS = 16_384;

  /** Default sketch ingest cadence in milliseconds when no tighter interval is configured. */
  public static final long DEFAULT_DETECTOR_INTERVAL_MS = 500L;

  /** Initial logical capacity of the reused detector accumulator map. */
  private static final int ACCUMULATOR_INITIAL_CAPACITY = 1024;

  /** Backing table size holding {@link #ACCUMULATOR_INITIAL_CAPACITY} entries without resizing. */
  private static final int ACCUMULATOR_INITIAL_TABLE_SIZE = CollectionSizing.capacity(ACCUMULATOR_INITIAL_CAPACITY);

  /**
   * Flushed batch size above which the accumulator map is replaced rather than cleared, preventing
   * a single scan burst from pinning peak heap capacity permanently.
   */
  private static final int ACCUMULATOR_SHRINK_THRESHOLD = 4096;

  /** Maximum distinct cold keys buffered per {@link WaveCounter} cycle. */
  private static final int MAX_BUFFER_SIZE = 100_000;

  private final ScheduledExecutorService scheduler;
  private final HotKeyDetector detector;
  private final long detectorIntervalMs;
  private final int maxAccumulatedKeys;
  private final @Nullable KeyReporter reporter;
  private final WaveCounter counter;

  /** Observer notified with the keys read on each tide; defaults to a no-op. */
  private volatile ReadObserver readObserver = ReadObserver.NONE;

  /**
   * Accumulator batching reporter-cadence tides until {@link #detectorIntervalMs} elapses, or
   * {@code null} when no reporter is wired: the tide then already runs at the detector cadence, so
   * it feeds the sketch directly and a second buffer would only copy it.
   */
  @GuardedBy("this")
  private @Nullable HashMap<String, Long> detectorAccumulator;

  /** Armed one-shot task flushing {@link #detectorAccumulator} even when traffic goes idle. */
  @GuardedBy("this")
  private @Nullable ScheduledFuture<?> pendingDetectorFlush;

  /** Monotonic timestamp in milliseconds of the most recent sketch ingest. */
  @GuardedBy("this")
  private long lastDetectorFlushMs;

  /** Monotonic generation token invalidating already-started flush tasks after cancellation. */
  @GuardedBy("this")
  private long detectorFlushGen;

  /**
   * Creates an {@link AccessCounter} feeding {@code detector} and optionally {@code reporter}.
   *
   * @param detector           the local Top-K hot-key detector; must not be {@code null}
   * @param reporter           the worker reporter, or {@code null} when reporting is disabled
   * @param scheduler          the shared scheduler driving tides and deferred detector flushes;
   *                           must not be {@code null}
   * @param reportIntervalMs   base tide interval in milliseconds when {@code reporter} is present
   * @param detectorIntervalMs maximum elapsed time in milliseconds between sketch ingests
   */
  public AccessCounter(
    HotKeyDetector detector,
    @Nullable KeyReporter reporter,
    ScheduledExecutorService scheduler,
    long reportIntervalMs,
    long detectorIntervalMs
  ) {
    this.detector = Objects.requireNonNull(detector, "detector must not be null");
    this.scheduler = Objects.requireNonNull(scheduler, "scheduler must not be null");
    this.reporter = reporter;
    this.maxAccumulatedKeys = DEFAULT_MAX_ACCUMULATED_KEYS;
    // Anchor at construction time so the first tide accumulates normally instead of flushing early.
    this.lastDetectorFlushMs = TimeSource.monotonicMillis();
    this.detectorIntervalMs = Math.max(1L, detectorIntervalMs);
    long tideBaseMs = reporter != null ? Math.max(1L, reportIntervalMs) : this.detectorIntervalMs;
    this.detectorAccumulator = reporter != null ? new HashMap<>(ACCUMULATOR_INITIAL_TABLE_SIZE) : null;
    this.counter = new WaveCounter(this::onTide, MAX_BUFFER_SIZE, scheduler, tideBaseMs);
  }

  /**
   * Starts the underlying {@link WaveCounter} tide loop once bean initialization completes. All
   * fields, including the observer and the reporter cadence, are final or set by then, so the first
   * tide can arrive immediately without seeing a partially constructed counter.
   */
  @Override
  public void afterPropertiesSet() {
    counter.afterPropertiesSet();
  }

  /**
   * Stops the tide loop, cancels any pending detector flush task, and drains residual accumulated
   * counts into {@link #detector}.
   */
  @Override
  public void destroy() {
    counter.destroy();
    synchronized (this) {
      cancelPendingDetectorFlush();
      flushBatch();
    }
  }

  /**
   * Records a single read access: routes to the merged {@link WaveCounter} pipeline when
   * {@code report} is {@code true}, or directly to {@link #detector} otherwise.
   *
   * @param cacheKey the accessed physical key
   * @param report   {@code true} to include this read in the worker reporting pipeline
   */
  void record(String cacheKey, boolean report) {
    if (report) {
      counter.count(cacheKey, TOPK_INCR);
    } else {
      addDetection(cacheKey, TOPK_INCR);
    }
  }

  /**
   * Records {@code count} accesses for one key according to an explicit {@link Zeta.TagMode}.
   *
   * @param cacheKey the accessed physical key
   * @param count    the positive number of accesses to record
   * @param mode     the pipeline routing mode; must not be {@code null}
   */
  public void record(String cacheKey, long count, Zeta.TagMode mode) {
    Objects.requireNonNull(mode, "mode must not be null");
    switch (mode) {
      case RECORD -> counter.count(cacheKey, count);
      case DETECT_ONLY -> addDetection(cacheKey, count);
      case REPORT_ONLY -> addReportOnly(cacheKey, count);
      case SILENT -> {
        // Intentionally ignored.
      }
    }
  }

  /**
   * Records weighted accesses for a batch of keys under one shared {@link Zeta.TagMode}.
   *
   * @param keyCounts validated map from physical key to positive access count; must not be {@code null}
   * @param mode      the pipeline routing mode; must not be {@code null}
   */
  public void recordAll(Map<String, Long> keyCounts, Zeta.TagMode mode) {
    Objects.requireNonNull(keyCounts, "keyCounts must not be null");
    Objects.requireNonNull(mode, "mode must not be null");
    switch (mode) {
      case RECORD -> keyCounts.forEach(counter::count);
      case DETECT_ONLY -> ingestSafely(keyCounts);
      case REPORT_ONLY -> addReportOnlyBatch(keyCounts);
      case SILENT -> {
        // Intentionally ignored.
      }
    }
  }

  /**
   * Installs the observer notified of keys observed on each tide.
   *
   * @param observer the read observer, or {@code null} to reset to {@link ReadObserver#NONE}
   */
  public void setReadObserver(@Nullable ReadObserver observer) {
    this.readObserver = observer != null ? observer : ReadObserver.NONE;
  }

  /**
   * Returns the approximate number of distinct keys currently buffered in the counting reservoir.
   *
   * @return estimated distinct keys with pending counts
   */
  public long pendingKeyCount() {
    return counter.estimatedSizeOfKeysCount();
  }

  /**
   * Updates the {@link WaveCounter} tide delivery interval in milliseconds.
   *
   * @param intervalMs the new tide interval in milliseconds
   */
  public void adjustDeliverIntervalMs(long intervalMs) {
    counter.adjustDeliverIntervalMs(intervalMs);
  }

  /**
   * Consumes one merged tide snapshot from {@link WaveCounter}: feeds the local detector leg first
   * (under monitor), notifies {@link #readObserver}, and publishes to {@link #reporter} outside
   * the monitor so remote backpressure cannot block detector flushes or shutdown draining.
   *
   * @param snapshot the immutable tide snapshot of key counts (may be empty)
   * @param windowMs measured monotonic accumulation window in milliseconds
   */
  void onTide(Map<String, Long> snapshot, long windowMs) {
    mergeIntoSketches(snapshot);
    try {
      readObserver.onReads(snapshot.keySet(), TimeSource.monotonicMillis());
    } catch (RuntimeException e) {
      log.debug("Read observer failed for a tide of {} keys", snapshot.size(), e);
    }
    if (reporter != null) {
      // Deliver empty snapshots too so the reporter's feed loop observes idle windows.
      reporter.publishBatch(snapshot, windowMs);
    }
  }

  /**
   * Records a detection-only access synchronously into {@link #detector} without reporting.
   *
   * @param cacheKey the accessed physical key
   * @param count    the positive access count
   */
  private void addDetection(String cacheKey, long count) {
    detector.addDirect(cacheKey, count);
  }

  /**
   * Publishes a single report-only access directly to {@link #reporter} without updating the local
   * detector sketch.
   *
   * @param cacheKey the accessed physical key
   * @param count    the positive access count
   */
  private void addReportOnly(String cacheKey, long count) {
    if (reporter == null) {
      return;
    }
    reporter.publishDirect(Map.of(cacheKey, count));
  }

  /**
   * Publishes a batch of report-only accesses directly to {@link #reporter} without updating the
   * local detector sketch.
   *
   * @param keyCounts validated map of key counts
   */
  private void addReportOnlyBatch(Map<String, Long> keyCounts) {
    if (reporter == null || keyCounts.isEmpty()) {
      return;
    }
    reporter.publishDirect(Collections.unmodifiableMap(keyCounts));
  }

  /**
   * Merges a tide snapshot into {@link #detectorAccumulator} and flushes to {@link #detector}
   * whenever {@link #detectorIntervalMs} has elapsed or {@link #maxAccumulatedKeys} is reached.
   *
   * <p>Synchronized so a final drain during {@link #destroy()} never overlaps an in-flight tide on
   * the scheduler thread.
   *
   * @param snapshot the immutable tide snapshot of key counts (may be empty)
   */
  private synchronized void mergeIntoSketches(Map<String, Long> snapshot) {
    HashMap<String, Long> accumulator = detectorAccumulator;
    if (accumulator == null) {
      // No reporter: the tide interval already is the detector interval, so nothing needs merging.
      ingestSafely(snapshot);
      return;
    }
    long now = TimeSource.monotonicMillis();
    boolean due = now - lastDetectorFlushMs >= detectorIntervalMs;

    if (snapshot.isEmpty()) {
      // Empty tides must not advance the cadence anchor, or subsequent traffic after an idle
      // period would wait a full detector interval. Drain any due residual or keep its timer armed.
      if (!accumulator.isEmpty()) {
        if (due) {
          flushNow(now);
        } else {
          scheduleDetectorFlush(now);
        }
      }
      return;
    }

    boolean oversize = snapshot.size() >= maxAccumulatedKeys;
    boolean emptyAtBoundary = due && accumulator.isEmpty();
    if (oversize || emptyAtBoundary) {
      flushNow(now);
      ingestSafely(snapshot);
      return;
    }

    snapshot.forEach((key, count) -> accumulator.merge(key, count, Long::sum));
    if (due || accumulator.size() >= maxAccumulatedKeys) {
      flushNow(now);
    } else {
      scheduleDetectorFlush(now);
    }
  }

  /**
   * Advances the detector flush anchor to {@code now}, cancels any armed flush task, and drains
   * {@link #detectorAccumulator}.
   *
   * @param now the current monotonic timestamp in milliseconds
   */
  @GuardedBy("this")
  private void flushNow(long now) {
    lastDetectorFlushMs = now;
    cancelPendingDetectorFlush();
    flushBatch();
  }

  /**
   * Hands {@link #detectorAccumulator} to {@link #detector} and resets it, replacing the map when
   * it grew past {@link #ACCUMULATOR_SHRINK_THRESHOLD} to release burst memory.
   */
  @GuardedBy("this")
  private void flushBatch() {
    HashMap<String, Long> batch = detectorAccumulator;
    if (batch == null || batch.isEmpty()) {
      return;
    }
    int size = batch.size();
    ingestSafely(batch);
    if (size > ACCUMULATOR_SHRINK_THRESHOLD) {
      detectorAccumulator = new HashMap<>(ACCUMULATOR_INITIAL_TABLE_SIZE);
    } else {
      batch.clear();
    }
  }

  /**
   * Arms a one-shot flush task for the remainder of {@link #detectorIntervalMs} if none is active.
   *
   * @param now the current monotonic timestamp in milliseconds
   */
  @GuardedBy("this")
  private void scheduleDetectorFlush(long now) {
    if (pendingDetectorFlush != null && !pendingDetectorFlush.isDone()) {
      return;
    }
    long gen = ++detectorFlushGen;
    long delayMs = Math.max(1L, detectorIntervalMs - (now - lastDetectorFlushMs));
    try {
      pendingDetectorFlush = scheduler.schedule(() -> flushDetector(gen), delayMs, TimeUnit.MILLISECONDS);
    } catch (RejectedExecutionException e) {
      log.debug("Detector flush scheduling rejected; accumulator will drain on a later tide", e);
    }
  }

  /**
   * Executes a scheduled detector flush if its generation token still matches
   * {@link #detectorFlushGen}.
   *
   * @param gen the generation token captured when the task was scheduled
   */
  private synchronized void flushDetector(long gen) {
    if (gen != detectorFlushGen) {
      return;
    }
    pendingDetectorFlush = null;
    HashMap<String, Long> batch = detectorAccumulator;
    if (batch == null || batch.isEmpty()) {
      return;
    }
    lastDetectorFlushMs = TimeSource.monotonicMillis();
    flushBatch();
  }

  /**
   * Cancels any armed detector flush task and advances {@link #detectorFlushGen} so an already
   * running callback exits without mutating state.
   */
  @GuardedBy("this")
  private void cancelPendingDetectorFlush() {
    detectorFlushGen++;
    if (pendingDetectorFlush != null) {
      pendingDetectorFlush.cancel(false);
      pendingDetectorFlush = null;
    }
  }

  /**
   * Passes a non-empty batch to {@link HotKeyDetector#ingest(Map)}, isolating runtime failures so
   * the tide loop is never disrupted.
   *
   * @param batch the batch of key counts to ingest
   */
  private void ingestSafely(Map<String, Long> batch) {
    if (batch.isEmpty()) {
      return;
    }
    try {
      detector.ingest(batch);
    } catch (RuntimeException e) {
      log.debug("Detector ingest failed for a batch of {} keys", batch.size(), e);
    }
  }
}
