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
 * The single buffered access counter behind the read path (design decisions: access counting):
 * every served read increments <b>one</b> {@link WaveCounter}, and each tide fans the merged
 * snapshot out to both aggregation legs.
 *
 * <pre>
 *   read ──► AccessCounter WaveCounter (report cadence)
 *              ├─ every tide    ──► reporter.publishBatch (route/publish)
 *              └─ every ≥500 ms ──► detector.ingest (merged batch)
 * </pre>
 *
 * <p>One add per read pays one hash, one thread-local lookup, one beacon check and one
 * cold-capacity check for both legs; the two pipelines cannot disagree because they share one
 * capacity bound and one tide task. The reporter leg keeps its configured cadence and the
 * feed-loop tuner (design decisions: reporting); the detector leg sees the same volumes on its own
 * 500 ms rhythm, batched through an accumulator flushed whenever {@code detectorIntervalMs}
 * elapses.
 *
 * <p><b>Tag-mode routing</b> (per-key granularity the merged tide cannot express):
 * {@code DETECT_ONLY} adds synchronously to the sketch ({@link HotKeyDetector#addDirect}) and
 * {@code REPORT_ONLY} publishes directly — both are rare, opt-in paths. {@code RECORD} is the
 * normal path: the read path and both tag batch forms feed {@link WaveCounter} through
 * {@link #record(String, boolean)} or {@link #record(String, long, Zeta.TagMode)}.
 *
 * <p>Without a reporter (no RabbitMQ / App-only deployment) the counter is the detector's sole
 * feed at the detector's own cadence.
 *
 * <p><b>Thread-safety:</b> {@link #record(String, boolean)} is the hot path — thread-safe through
 * the {@link WaveCounter}. {@link #onTide} is serialized by its own monitor so the shutdown-time
 * final drain cannot overlap a running tide. Lifecycle follows the Spring bean contract
 * ({@link InitializingBean}/{@link DisposableBean}).
 */
@Slf4j
@Internal
public class AccessCounter implements InitializingBean, DisposableBean {

  /**
   * Default cap on distinct keys buffered between two sketch ingests — 16× the
   * per-tide reservoir of a healthy workload, and a few MB in the worst case.
   */
  public static final int DEFAULT_MAX_ACCUMULATED_KEYS = 16_384;

  /** Initial capacity of the reused detector accumulator (a filter map, not a reservoir). */
  private static final int ACCUMULATOR_INITIAL_CAPACITY = 1024;

  /**
   * Table size that holds {@link #ACCUMULATOR_INITIAL_CAPACITY} entries without a
   * resize at the default load factor.
   */
  private static final int ACCUMULATOR_INITIAL_TABLE_SIZE = (int) (ACCUMULATOR_INITIAL_CAPACITY / 0.75f) + 1;

  /**
   * A flushed batch larger than this releases its table instead of clearing it, so
   * one scan burst cannot pin the peak capacity for the process lifetime. Below it,
   * the table is reused: steady-state cardinality must not rehash every cadence.
   */
  private static final int ACCUMULATOR_SHRINK_THRESHOLD = 4096;

  /** Default sketch ingest cadence when no tighter interval is configured (the detector's 500ms rhythm). */
  public static final long DEFAULT_DETECTOR_INTERVAL_MS = 500;

  /** Max distinct cold keys per counting cycle. */
  private static final int MAX_BUFFER_SIZE = 100_000;

  private final ScheduledExecutorService scheduler;

  private final HotKeyDetector detector;

  private final long detectorIntervalMs;

  /**
   * Key ceiling for {@link #detectorAccumulator}. The sketch itself is bounded,
   * so the accumulator must be too: a scan between two detector ticks would
   * otherwise buffer every distinct key it touched, making the <em>accumulator</em>
   * the largest structure in the process under exactly the traffic this cache
   * exists to absorb. Overflow flushes early instead of dropping counts
   * (detection quality is unaffected — only the batching cadence is).
   */
  private final int maxAccumulatedKeys;

  @Nullable
  private final KeyReporter reporter;

  private final WaveCounter counter;

  /**
   * Detector-side accumulator: counts merged from reporter-sized tides until
   * {@link #detectorIntervalMs} has elapsed, then handed to the sketch as one
   * batch. A flush only releases the map when the flushed batch exceeds the burst
   * watermark, so steady-state cardinality keeps its table and one scan burst
   * cannot pin the peak capacity for the process lifetime.
   */
  @GuardedBy("this")
  private HashMap<String, Long> detectorAccumulator = new HashMap<>(ACCUMULATOR_INITIAL_TABLE_SIZE);

  /**
   * One-shot task that flushes the accumulator at the detector cadence even
   * when traffic stops after a single burst. Tides are delivered for every
   * window, including empty ones; an empty tide never advances the detector
   * anchor.
   */
  @GuardedBy("this")
  private ScheduledFuture<?> pendingDetectorFlush;

  /** Monotonic timestamp of the last sketch ingest. */
  @GuardedBy("this")
  private long lastDetectorFlushMs;

  /**
   * Generation token of the armed detector flush. It is advanced whenever the flush is armed or
   * cancelled, so a task that already started before a cancellation can recognise that it does
   * not own the schedule and return without touching the new task or the cadence anchor.
   */
  @GuardedBy("this")
  private long detectorFlushGen;

  /**
   * Creates the counter.
   *
   * @param detector           the local TopK detector (sketch ingest sink; never {@code null})
   * @param reporter           the Worker reporter ({@code null} when reporting is disabled —
   *                           the counter then feeds the detector only)
   * @param scheduler          the shared scheduler driving the tide ({@code hotKeyScheduler};
   *                           never {@code null})
   * @param reportIntervalMs   the reporter's tide base in ms (the configured
   *                           {@code zeta.local.report-interval-ms}); ignored when
   *                           {@code reporter} is {@code null}
   * @param detectorIntervalMs max elapsed time between sketch ingests in ms
   */
  public AccessCounter(
    HotKeyDetector detector,
    @Nullable KeyReporter reporter,
    ScheduledExecutorService scheduler,
    long reportIntervalMs,
    long detectorIntervalMs
  ) {
    this.reporter = reporter;
    this.maxAccumulatedKeys = DEFAULT_MAX_ACCUMULATED_KEYS;
    // Anchor at "now" so the first, report-sized tide accumulates like every
    // other one instead of looking detector-interval-old and flushing early.
    this.lastDetectorFlushMs = TimeSource.monotonicMillis();
    this.detectorIntervalMs = Math.max(1L, detectorIntervalMs);
    this.detector = Objects.requireNonNull(detector, "detector must not be null");
    this.scheduler = Objects.requireNonNull(scheduler, "scheduler must not be null");
    long tideBaseMs = reporter != null ? Math.max(1L, reportIntervalMs) : this.detectorIntervalMs;
    this.counter = new WaveCounter(this::onTide, MAX_BUFFER_SIZE, scheduler, tideBaseMs);
  }

  /**
   * Record one access on the pipeline selected by {@code report}: the merged detection and Worker
   * reporting path when reporting is allowed, the synchronous detection-only path otherwise.
   *
   * @param cacheKey the accessed key
   * @param report   whether this access feeds the Worker reporting plane
   */
  void record(String cacheKey, boolean report) {
    if (report) {
      counter.count(cacheKey, TOPK_INCR);
    } else {
      addDetection(cacheKey, TOPK_INCR);
    }
  }

  /**
   * Record {@code count} accesses for one key under an explicit tag mode. The switch is exhaustive
   * so a new mode cannot silently fall through to the merged read path.
   *
   * @param cacheKey the accessed key
   * @param count    the positive number of accesses to add
   * @param mode     how much of the pipeline to run (never {@code null})
   */
  public void record(String cacheKey, long count, Zeta.TagMode mode) {
    switch (mode) {
      case RECORD -> counter.count(cacheKey, count);
      case DETECT_ONLY -> addDetection(cacheKey, count);
      case REPORT_ONLY -> addReportOnly(cacheKey, count);
      case SILENT -> {
      }
    }
  }

  /**
   * Record weighted accesses for a batch of keys under one shared tag mode.
   *
   * <p>{@code RECORD} iterates the shared reservoir so the batch keeps its capacity bound and
   * merges with concurrent reads; {@code DETECT_ONLY} hands the batch to the sketch in one pass;
   * {@code REPORT_ONLY} publishes it as one batch.
   *
   * @param keyCounts validated map of cacheKey to a positive access count
   * @param mode      how much of the pipeline to run (never {@code null})
   */
  public void recordAll(Map<String, Long> keyCounts, Zeta.TagMode mode) {
    switch (mode) {
      case RECORD -> keyCounts.forEach(counter::count);
      case DETECT_ONLY -> ingestSafely(keyCounts);
      case REPORT_ONLY -> addReportOnlyBatch(keyCounts);
      case SILENT -> {
      }
    }
  }

  /**
   * Record a detection-only access ({@code TagMode.DETECT_ONLY}): one synchronous sketch update,
   * no Worker report. The callers that use this are rare, so the synchronous sketch cost is
   * acceptable; the merged tide cannot express per-key mode splits.
   *
   * @param cacheKey the accessed key
   * @param count    the positive number of accesses to add
   */
  private void addDetection(String cacheKey, long count) {
    detector.addDirect(cacheKey, count);
  }

  /**
   * Record a report-only access ({@code TagMode.REPORT_ONLY}): one single-key batch published
   * directly, with no local counting at all — the key never enters the local sketch, honoring the
   * skip contract the merged tide cannot. Out-of-band by construction: the cadence tuner never
   * samples it, because a one-key report says nothing about how many keys a flush window
   * accumulates.
   *
   * @param cacheKey the accessed key
   * @param count    the positive number of accesses to add
   */
  private void addReportOnly(String cacheKey, long count) {
    if (reporter == null) {
      return;
    }
    reporter.publishDirect(Map.of(cacheKey, count));
  }

  /**
   * Batch form of {@link #addReportOnly(String, long)}: one publish for the whole map.
   *
   * @param keyCounts validated map of cacheKey to a positive access count
   */
  private void addReportOnlyBatch(Map<String, Long> keyCounts) {
    if (reporter == null) {
      return;
    }
    reporter.publishDirect(Collections.unmodifiableMap(keyCounts));
  }

  /**
   * Start the tide loop. Called by the Spring container after construction.
   */
  @Override
  public void afterPropertiesSet() {
    counter.afterPropertiesSet();
  }

  /**
   * Stop the tide loop and drain what the last tide left behind. The counter's
   * own shutdown contract drops pending counts (bounded one-tide loss), but
   * the detector accumulator is this class's state: without the drain, a final
   * tide inside the detector interval (or under the key cap) returns early and
   * silently discards both the snapshot and everything already merged.
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
   * The tide consumer — the single delivery point of the merged snapshot.
   * Merges into the sketch accumulator and flushes it when the detector cadence
   * has elapsed, then publishes to the reporter (when wired).
   *
   * <p>The snapshot must not be mutated (WaveCounter contract) — both legs
   * only read it.
   *
   * <p>The detector leg runs first: the reporting plane is remote and can block
   * for its bounded offer budget, while local detection must not fall behind a
   * stalled broker. The reporter leg runs <b>outside</b> the accumulator monitor:
   * its queue offer can block, and holding this monitor across that wait would
   * stall the tide (and {@code destroy}'s drain). The monitor covers only this
   * class's own accumulator state.
   *
   * @param snapshot the tide's merged key counts (never {@code null}, may be empty)
   * @param windowMs measured monotonic window the snapshot accumulated over,
   *                 in ms (positive; the reporter's feed loop turns the batch
   *                 into an arrival rate with it)
   */
  void onTide(Map<String, Long> snapshot, long windowMs) {
    mergeIntoSketches(snapshot);
    if (reporter != null) {
      // Empty snapshots are delivered too: the reporter's feed loop needs the
      // zero-arrival window to back the cadence off, and admit() still drops the
      // report itself when there is nothing to route.
      reporter.publishBatch(snapshot, windowMs);
    }
  }

  /**
   * The detector leg of {@link #onTide}: merges the tide into the sketch
   * accumulator and drains it when the detector cadence has elapsed.
   *
   * <p>Synchronized because the WaveCounter's shutdown path invokes the
   * consumer one last time <em>from the closing thread</em> while a tide that
   * already fired may still be draining on the scheduler thread
   * ({@code pendingTide.cancel(false)} deliberately does not interrupt a
   * running one). One monitor per tide — every 50 ms by default, never per
   * read — makes the accumulator and its flush timestamp safe for that overlap.
   *
   * @param snapshot the tide's merged key counts (never {@code null}, may be empty)
   */
  private synchronized void mergeIntoSketches(Map<String, Long> snapshot) {
    long now = TimeSource.monotonicMillis();
    boolean due = now - lastDetectorFlushMs >= detectorIntervalMs;
    // An empty tide is a reporter signal, not a detector ingest: it must not advance
    // the cadence anchor, or the first real traffic after an idle period is batched
    // behind a full detector interval. A non-empty residual at the boundary still
    // drains here; otherwise keep its flush armed, which also retries a schedule the
    // scheduler rejected at merge time.
    if (snapshot.isEmpty()) {
      if (due && !detectorAccumulator.isEmpty()) {
        flushNow(now);
      } else if (!detectorAccumulator.isEmpty()) {
        scheduleDetectorFlush(now);
      }
      return;
    }

    boolean oversize = snapshot.size() >= maxAccumulatedKeys;
    boolean emptyAtBoundary = due && detectorAccumulator.isEmpty();
    // A single tide that alone fills the accumulator can never be merged
    // without oversizing it: a key-churn scan may deliver up to MAX_BUFFER_SIZE
    // keys. An empty accumulator at the cadence boundary bypasses it for the
    // same reason: there is nothing to merge, so hand the snapshot straight to
    // the sketch. Both cases anchor the cadence and drain the residual first.
    if (oversize || emptyAtBoundary) {
      flushNow(now);
      ingestSafely(snapshot);
      return;
    }

    mergeSnapshot(snapshot);
    // Judge the MERGED size: the snapshot and the accumulator overlap on a stable
    // hot set, so summing the two views would trip the cap at roughly half of it.
    if (due || detectorAccumulator.size() >= maxAccumulatedKeys) {
      flushNow(now);
    } else {
      scheduleDetectorFlush(now);
    }
  }

  /** Merges one tide's counts into the accumulator; the caller holds the monitor. */
  @GuardedBy("this")
  private void mergeSnapshot(Map<String, Long> snapshot) {
    snapshot.forEach((key, count) -> detectorAccumulator.merge(key, count, Long::sum));
  }

  /**
   * The one "flush now" action: move the cadence anchor, cancel the armed flush, drain the
   * accumulator. The caller holds the monitor.
   */
  @GuardedBy("this")
  private void flushNow(long now) {
    lastDetectorFlushMs = now;
    cancelPendingDetectorFlush();
    flushBatch();
  }

  /**
   * Hands the accumulator to the sketch and resets it. A batch that outgrew the
   * shrink threshold is replaced instead of cleared: {@code HashMap.clear()} keeps
   * its {@code Node[]} table, so a scan burst would otherwise pin the peak capacity
   * for the process lifetime. A steady-state batch reuses the table.
   */
  @GuardedBy("this")
  private void flushBatch() {
    HashMap<String, Long> batch = detectorAccumulator;
    if (batch.isEmpty()) {
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
   * Arms the one-shot detector flush at the remaining part of the cadence.
   * A no-op while a flush is already armed.
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
      // Scheduler saturated or shutting down: a later tide or destroy()
      // still drains the accumulator.
      log.debug("Detector flush scheduling rejected; accumulator will drain on a later tide", e);
    }
  }

  /** The armed detector-cadence flush: drain the accumulator even if no tide follows. */
  private synchronized void flushDetector(long gen) {
    if (gen != detectorFlushGen) {
      // A running task cannot be interrupted by cancel(false); a cancellation or a replacement
      // schedule advanced the generation while this one waited for the monitor.
      return;
    }
    pendingDetectorFlush = null;
    if (detectorAccumulator.isEmpty()) {
      return;
    }
    lastDetectorFlushMs = TimeSource.monotonicMillis();
    flushBatch();
  }

  /** Cancels the armed detector flush and invalidates any task that already started. */
  @GuardedBy("this")
  private void cancelPendingDetectorFlush() {
    detectorFlushGen++;
    if (pendingDetectorFlush != null) {
      pendingDetectorFlush.cancel(false);
      pendingDetectorFlush = null;
    }
  }

  private void ingestSafely(Map<String, Long> batch) {
    if (batch.isEmpty()) {
      // The tide also delivers empty snapshots (the reporter's zero-arrival
      // window), and an empty detector flush reaches here: neither is an ingest.
      return;
    }
    try {
      detector.ingest(batch);
    } catch (RuntimeException e) {
      // The sketch ingest must never kill the tide (design decisions: buffering and ordering).
      log.debug("Detector ingest failed for a batch of {} keys", batch.size(), e);
    }
  }

  /**
   * Approximate number of unique keys currently buffered in the counting reservoir
   * (design decisions: access counting).
   *
   * @return estimated distinct keys with pending counts
   */
  public long pendingKeyCount() {
    return counter.estimatedSizeOfKeysCount();
  }

  /**
   * Move the tide base. The reporting feed-loop tuner computes the interval and applies it here;
   * the tuner stays in the reporter (design decisions: reporting).
   *
   * @param intervalMs the new tide base in ms
   */
  public void adjustDeliverIntervalMs(long intervalMs) {
    counter.adjustDeliverIntervalMs(intervalMs);
  }
}
