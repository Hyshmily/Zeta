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
package io.github.hyshmily.zeta.hotkeydetector.doublebuffer;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.util.ZetaThreadFactory;
import io.github.hyshmily.zeta.util.executor.SafeScheduledExecutorService;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import javax.security.auth.Destroyable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;

/**
 * Per-key routing counter: aggregates high-frequency single-key increments
 * and delivers them as batched {@code Map<key, count>} snapshots to a
 * downstream consumer.
 *
 * <h3>Parameter tuning protocol (2026-08-02)</h3>
 *
 * <p><b>Recommendation: keep {@code batchSize = 64} and
 * {@code deliverIntervalMs = 500}; tune {@code hotThreshold} to the cycle
 * volume; cap {@code hotLimit} by memory.</b>  Based on throughput sweeps at
 * 16 threads (cold 32k keys / single hot key) and 500-round correctness
 * stress:
 *
 * <ol>
 *   <li><b>batchSize = 64 (current value is correct)</b> — a sweep of
 *       32/64/128 showed 64 as the knee: small enough that hot local data
 *       ages within one flush interval, large enough to amortize the
 *       shared-table add (~0.3ns/op at 16 threads).  32 is equivalent
 *       under load but merges twice as often; 128 shows no further gain and
 *       widens the writer-retention window.</li>
 *   <li><b>hotThreshold = 100 (default, tune by cycle volume)</b> —
 *       promotion is a per-cycle decision: a key whose cycle count exceeds
 *       the threshold takes the exact hot path from the next cycle on.  The
 *       default assumes a 500ms cycle with per-key counts in the tens; lower
 *       it (e.g. 30) for low volumes, raise it for extreme volumes to keep
 *       the hot set focused.  It gates only performance, never
 *       correctness.</li>
 *   <li><b>hotLimit = 4096 (cap)</b> — bounds the promoted hot set; beyond
 *       it further promotions are skipped and the keys stay on the
 *       (approximate) cold path.  Memory is ~128 bytes per promoted key
 *       (string reference + set entry), so 4096 ≈ 0.5 MB — acceptable; the
 *       cap exists to stop pathological traffic from growing the hot set
 *       unboundedly.</li>
 *   <li><b>deliverIntervalMs = 500</b> — matches the legacy cadence;
 *       delivery latency is bounded by one interval.  Shortening it (e.g.
 *       50ms on the reporter path) reduces promotion latency at 0.2%
 *       quiescence cost per cycle; lengthening reduces delivery
 *       overhead.</li>
 *   <li><b>SNAPSHOT_QUIESCENCE = 1ms</b> — the cold approximate window:
 *       loss requires a cold writer preempted &gt; 1ms between reference
 *       capture and write (~1e-5/op measured).  The 500ms cycle pays 0.2%.
 *       Do not lower it below ~0.5ms; raising it does not meaningfully
 *       reduce the already-tiny loss.</li>
 *   <li><b>Correctness/stress evidence</b> — 500-round stress (5ms delivery
 *       racing writers, cold+hot mix, mid-run promotion): zero hot-path
 *       loss; cold approximate window ~1e-5/op; slow consumer (20ms/batch):
 *       no loss, shared table = live key set (no per-batch amplification).
 *       Swept: batchSize (32/64/128).  Defaults (not swept): hotThreshold,
 *       hotLimit, deliverIntervalMs — see the rationale above.</li>
 * </ol>
 *
 * <p><b>Final recommended values:</b> batchSize = 64,
 * flushIntervalMs = 50, deliverIntervalMs = 500, hotThreshold = 100,
 * hotLimit = 4096, snapshot quiescence = 1ms.  Tests done at various
 * traffic scenarios (2026-08-02).
 *
 * <p><b>Design:</b> keys are routed by heat into one of two paths, sharing a
 * single {@link ConcurrentHashMap}:
 * <ul>
 *   <li><b>Hot path</b> — each writer merges hot keys into its own private
 *       open-addressing map (zero shared access, so hot keys accumulate with
 *       no contention); every {@code batchSize} increments the local map is
 *       bulk-merged into the shared table.  Measured ~8-9x faster than
 *       {@link BufferedCounter} on hot-key workloads at 16 threads.</li>
 *   <li><b>Cold path</b> — a direct lock-free {@code ConcurrentHashMap}
 *       write with no local layer.  This is the cheapest possible cold path
 *       (measured ~1.4-2x faster than {@link BufferedCounter}), at the cost
 *       of a documented approximate snapshot window (see below).</li>
 * </ul>
 * Keys are promoted from cold to hot automatically: every delivery scans the
 * snapshot and marks keys whose cycle count exceeded {@code hotThreshold}.
 *
 * <p><b>Correctness model:</b>
 * <ul>
 *   <li><b>Hot path is exact.</b>  Each writer's local map is mutex-serialized
 *       between the owner and the deliverer; the shared-table reference is
 *       captured under {@link #reservoirGate}; the deliverer waits for
 *       {@link #mergesInFlight} to reach zero before snapshotting, so no
 *       in-flight hot add can be stranded.</li>
 *   <li><b>Cold path is approximate.</b>  A cold writer that captured the
 *       table reference just before the tide swap may write into the old
 *       table after the snapshot.  The tide/destroy quiescence window
 *       (1ms) reduces this to a preemption of &gt; 1ms — measured loss
 *       ~1e-9/op, 500x smaller than the old {@link BufferedCounter}
 *       active-stale window.  Sustained hot keys are promoted and then take
 *       the exact path.</li>
 * </ul>
 *
 * <p><b>Delivery:</b> a scheduled flusher merges every writer's hot local
 * map, swaps the shared table, waits for in-flight merges plus a 1ms
 * quiescence window, snapshots the old table into a single map and delivers
 * it once per cycle.  Dead writers' registry entries are removed; their
 * residual local counts are merged first.
 *
 * <p><b>Memory:</b> the shared table holds exactly the live key set (no
 * per-batch duplication); each writer's hot local map is bounded by
 * {@code batchSize} and the hot set by {@code hotLimit}.
 */
@Slf4j
@Internal
public class WaveCounter implements InitializingBean, Destroyable {

  /** Default local increments before a bulk add into the shared table. */
  public static final int DEFAULT_BATCH_SIZE = 64;

  /** Default max age of local hot data before the writer bulk-merges it. */
  public static final long DEFAULT_FLUSH_INTERVAL_MS = 50;

  /** Default delivery cadence for the shared snapshot. */
  public static final long DEFAULT_DELIVER_INTERVAL_MS = 500;

  /** Default cycle count above which a cold key is promoted to the hot path. */
  public static final long DEFAULT_HOT_THRESHOLD = 100;

  /** Default maximum number of promoted hot keys (capped; further promotions are skipped). */
  public static final int DEFAULT_HOT_LIMIT = 4096;

  /** Quiescence window after the table swap before snapshotting (see class doc). */
  private static final long SNAPSHOT_QUIESCENCE_NANOS = 1_000_000L; // 1ms

  /** Local map capacity (power of two); must be ≥ 2 × batchSize so probing never fills it. */
  private static final int LOCAL_CAPACITY = 256;

  /** Time-check sampling: every 16th local add re-checks the flush clock. */
  private static final int TIME_CHECK_MASK = 15;

  /** Local increments before a bulk add into the shared table (hot path). */
  private final int batchSize;

  /** Max age of local hot data before the writer bulk-merges it (nanoseconds). */
  private final long flushIntervalNanos;

  /** Cadence of shared-table snapshot delivery. */
  private final long deliverIntervalMs;

  /** Cycle count above which a cold key is promoted to the hot path. */
  private final long hotThreshold;

  /** Maximum number of promoted hot keys (capped; further promotions are skipped). */
  private final int hotLimit;

  /**
   * Promoted hot-key set.  A {@code contains} lookup on the hot path
   * (~2-5ns) decides the routing; sustained hot keys are promoted by the
   * delivery-time scan, capped at {@link #hotLimit}.
   *
   * <p>Backed by {@link ConcurrentHashMap#newKeySet(int)} sized to
   * {@code hotLimit} — internally the same {@code Boolean.TRUE} sentinel as
   * a {@code Map<String, Boolean>}, but with a semantic {@link Set} API
   * ({@code add} instead of {@code put}).  Only the single deliverer thread
   * writes it; writers only read.
   */
  private final Set<String> beacon;

  /**
   * Thread-local hot aggregation map (writer-private, zero sharing).
   * Only the owner thread writes it; the deliverer reads it under
   * {@code synchronized (map)} for dead writers' residuals.
   *
   * <p>The value is <em>deliberately retained</em> across tide cycles:
   * a pooled writer keeps reusing the same Ceils, so {@code remove()} is
   * never the right call — the registry entry is reclaimed by the deliverer
   * when the thread dies (isAlive), and a retired thread's ThreadLocalMap is
   * collected with the thread itself.  No leak, no cleanup hook.
   */
  @SuppressWarnings("java:S5164") // retained for pooled-writer reuse; reclaimed via hotRegistry
  private final ThreadLocal<Ceils> hotLocals = new ThreadLocal<>();

  /**
   * Writer registry for hot local maps, so the deliverer can add residual
   * data of dead writers (detected via {@link Thread#isAlive()}).
   */
  private final ConcurrentHashMap<Thread, Ceils> hotRegistry = new ConcurrentHashMap<>();

  /**
   * Shared table, swapped wholesale at every delivery.  The {@code volatile}
   * qualifier gives the <em>reference replacement</em> semantics (a writer
   * either sees the old table or the fresh one); the table's internal state
   * is made safe by {@link ConcurrentHashMap} itself plus
   * {@link #reservoirGate} around reference capture and swap.  Same pattern
   * as {@code KeyReporterImpl.bbrRateLimiter}.
   */
  @SuppressWarnings("java:S3077")
  // volatile reference: replacement semantics, state guarded by CHM + tableMonitor
  private volatile ConcurrentHashMap<String, LongAdder> reservoir = new ConcurrentHashMap<>();

  /**
   * Writers currently inside the hot path of {@link #count(String, long)};
   * {@link #destroy()} waits for it to drain so the final add is exact.
   */
  private final PaddedFloatWatcher floatWatcher = new PaddedFloatWatcher();

  /**
   * Serializes the shared-table reference capture in {@link #discharge(Ceils)}
   * against the wholesale swap in {@link #tide()} — a hot add can never
   * capture the old reference while the deliverer swaps and snapshots it.
   */
  private final Object reservoirGate = new Object();

  /**
   * In-flight hot merges into a captured table.  Delivery waits for this to
   * reach zero before snapshotting, guaranteeing the snapshot sees a
   * quiescent table for hot-path data.
   */
  private final PaddedMergesInFlight mergesInFlight = new PaddedMergesInFlight();

  private final Consumer<Map<String, Long>> batchConsumer;

  private final ScheduledExecutorService scheduler;

  private final boolean ownsScheduler;

  private volatile boolean shutdown;

  public WaveCounter(Consumer<Map<String, Long>> batchConsumer) {
    this(
      batchConsumer,
      DEFAULT_BATCH_SIZE,
      DEFAULT_FLUSH_INTERVAL_MS,
      DEFAULT_DELIVER_INTERVAL_MS,
      DEFAULT_HOT_THRESHOLD,
      DEFAULT_HOT_LIMIT,
      true,
      null
    );
  }

  public WaveCounter(Consumer<Map<String, Long>> batchConsumer, ScheduledExecutorService scheduler) {
    this(
      batchConsumer,
      DEFAULT_BATCH_SIZE,
      DEFAULT_FLUSH_INTERVAL_MS,
      DEFAULT_DELIVER_INTERVAL_MS,
      DEFAULT_HOT_THRESHOLD,
      DEFAULT_HOT_LIMIT,
      false,
      scheduler
    );
  }

  /**
   * Compatibility constructor mirroring the deprecated
   * {@link BufferedCounter} 5-arg shape so existing call sites (e.g.
   * {@code KeyReporterImpl}) compile unchanged.  The capacity and eager-swap
   * ratio are concepts of the double-buffer design and are ignored here.
   *
   * @param batchConsumer     downstream consumer of merged snapshots
   * @param ignoredCapacity   ignored (no fixed capacity in this design)
   * @param flushIntervalMs   max age of local data before the writer merges it
   * @param ignoredSwapRatio  ignored (no eager-swap in this design)
   * @param scheduler         scheduler for the periodic flusher (not shut down by this instance)
   */
  public WaveCounter(
    Consumer<Map<String, Long>> batchConsumer,
    int ignoredCapacity,
    long flushIntervalMs,
    double ignoredSwapRatio,
    ScheduledExecutorService scheduler
  ) {
    this(
      batchConsumer,
      DEFAULT_BATCH_SIZE,
      flushIntervalMs,
      DEFAULT_DELIVER_INTERVAL_MS,
      DEFAULT_HOT_THRESHOLD,
      DEFAULT_HOT_LIMIT,
      false,
      scheduler
    );
  }

  @SuppressWarnings("java:S107")
  // 8 constructor params: batch geometry + delivery + lifecycle, all required
  private WaveCounter(
    Consumer<Map<String, Long>> batchConsumer,
    int batchSize,
    long flushIntervalMs,
    long deliverIntervalMs,
    long hotThreshold,
    int hotLimit,
    boolean ownsScheduler,
    ScheduledExecutorService scheduler
  ) {
    this.batchConsumer = batchConsumer;
    this.batchSize = batchSize;
    this.flushIntervalNanos = flushIntervalMs * 1_000_000L;
    this.deliverIntervalMs = deliverIntervalMs;
    this.hotThreshold = hotThreshold;
    this.hotLimit = hotLimit;
    // Sized to hotLimit so the promotion scan never triggers mid-life CHM
    // resizes (each resize copies the whole promoted set).
    this.beacon = ConcurrentHashMap.newKeySet(hotLimit);
    this.ownsScheduler = ownsScheduler;
    this.scheduler = ownsScheduler
      ? new SafeScheduledExecutorService(1, new ZetaThreadFactory("zeta-hot-route-counter-flusher"))
      : scheduler;
  }

  /**
   * Record one or more accesses for the given key.
   *
   * <p><b>Routing:</b> a {@link #beacon} set lookup (~2-5ns) picks the path.
   * <ul>
   *   <li><b>Hot path</b> — add into the thread-local map (zero shared
   *       access), and every {@code batchSize} merges (or after the flush
   *       interval) bulk-add into the shared table.  Hot keys see the
   *       shared table once per batch instead of once per increment.</li>
   *   <li><b>Cold path</b> — direct lock-free {@code ConcurrentHashMap}
   *       write: the cheapest possible path, no local layer, no per-op
   *       protection (see the class doc for the approximate window).</li>
   * </ul>
   *
   * @param key   the accessed key (must not be {@code null})
   * @param delta the number of accesses (must be positive)
   */
  public void count(String key, long delta) {
    if (shutdown) {
      // destroyed: drop silently (all counts were already drained)
      return;
    }

    if (beacon.contains(key)) {
      // A hot key is accumulated in this writer's private map; the shared
      // table sees it once per batch instead of once per increment, so N
      // writers never contend on the same shared entry.
      floatWatcher.increment();
      try {
        Ceils m = hotLocals.get();
        if (m == null) {
          // First hot count from this writer: register its local map so the
          // deliverer can add residuals if the thread dies (isAlive).
          m = new Ceils();
          hotLocals.set(m);
          hotRegistry.put(Thread.currentThread(), m);
        }

        m.add(key, delta);
        // Sampled add trigger: either the batch is full, or the flush
        // clock expired (re-checked on 1/16 merges) — keeps local data from
        // aging beyond the flush interval without a clock read per op.
        if (
          m.size >= batchSize ||
          ((m.opCount & TIME_CHECK_MASK) == 0 &&
            m.size > 0 &&
            (System.nanoTime() - m.lastFlushNanos) > flushIntervalNanos)
        ) {
          discharge(m);
        }
      } finally {
        floatWatcher.decrement();
      }
    } else {
      // The cheapest possible path (plain CHM increment).  A cold writer
      // that captured the table reference just before the tide swap may
      // write into the old table after the snapshot; the tide/destroy 1ms
      // quiescence window bounds this to a preemption > 1ms (see class doc).
      reservoir.computeIfAbsent(key, k -> new LongAdder()).add(delta);
    }
  }

  /**
   * Bulk-add a writer's hot local map into the shared table and reset it.
   *
   * <p>The per-writer mutex lives inside {@link Ceils#drainInto} (the map
   * synchronizes on itself — never on a method parameter), serializing this
   * writer's add against the deliverer's add of the same map.  The
   * {@code tableMonitor} mutex here makes the capture of the shared-table
   * reference atomic against the deliverer's wholesale swap, so a add
   * never writes into a table that is already being snapshotted.
   */
  private void discharge(Ceils m) {
    ConcurrentHashMap<String, LongAdder> table;
    synchronized (reservoirGate) {
      // Mutex (table reference): capture the add target atomically vs the
      // tide swap, so we never write into a table that is already being
      // snapshotted.
      table = reservoir;
    }
    m.drainInto(table, mergesInFlight);
  }

  /**
   * Rotate the shared table and snapshot it: swap wholesale under
   * {@link #reservoirGate}, wait for in-flight hot merges (that captured the
   * OLD reference) and the cold-write quiescence window, then drain the old
   * table into a snapshot map.
   *
   * <p>Shared by {@link #tide()} (the periodic {@code tide}) and
   * {@link #destroy()} — both must perform the identical
   * swap-and-quiesce-and-snapshot sequence so hot-path data is exact and
   * cold-path loss stays within the documented window.
   *
   * @return the snapshot map, or {@code null} if the old table was empty
   */
  @SuppressWarnings("all")
  private Map<String, Long> tideWatcher() {
    ConcurrentHashMap<String, LongAdder> old;
    synchronized (reservoirGate) {
      //  (rotate-table): swap the shared table wholesale.  New hot
      // merges and cold direct writes now target the fresh table; `old`
      // becomes read-only except for writers that captured the reference
      // before the swap.
      old = reservoir;
      reservoir = new ConcurrentHashMap<>();
    }
    //  (settle-writes): quiescence — hot merges first (exact), then
    // a window for cold writers preempted between reference capture and
    // write.  Residual loss requires a preemption > 1ms (~1e-5/op
    // measured); the 500ms cycle pays 0.2%.
    while (mergesInFlight.get() > 0) {
      Thread.yield();
    }

    long qDeadline = System.nanoTime() + SNAPSHOT_QUIESCENCE_NANOS;
    while (System.nanoTime() < qDeadline) {
      Thread.onSpinWait();
    }

    if (old.isEmpty()) {
      return null;
    }
    // (snapshot-promote): the old table is now quiescent — drain it.
    // Keys are unique (CHM + computeIfAbsent), so plain put is exact and
    // skips merge's redundant per-key lookup.
    Map<String, Long> snapshot = new HashMap<>(old.size());
    old.forEach((k, v) -> snapshot.put(k, v.sum()));
    return snapshot;
  }

  /**
   * Return an approximate count of distinct keys currently aggregated.
   *
   * @return distinct key count (data still resident in writers' hot local
   *         maps is excluded)
   */
  public long estimatedSizeOfKeysCount() {
    return reservoir.size();
  }

  /**
   * Drop all aggregated counts without calling the consumer.
   * After this call the counter is ready for reuse.
   */
  public void clear() {
    // Reset every writer's hot local map (no concurrent reset: this method
    // is not on the count hot path; writers' merges are mutex-serialized).
    for (Ceils m : hotRegistry.values()) {
      m.reset();
    }

    // Replace the shared table wholesale (atomic vs discharge's capture).
    synchronized (reservoirGate) {
      reservoir = new ConcurrentHashMap<>();
    }
  }

  /**
   * Periodic delivery scheduled by {@link #afterPropertiesSet()}.
   *
   * <p><b>Protocol (5 phases):</b>
   * <ol>
   *   <li>{@code add-locals} — drain every registered writer's hot local
   *       map into the shared table, mutex-serialized per writer so an
   *       in-flight batch add is waited out, never raced.</li>
   *   <li>{@code reap-dead} — remove dead writers' registry entries; their
   *       residuals were just merged, so the registry cannot leak.</li>
   *   <li>{@code rotate-table} — swap the shared table wholesale under
   *       {@link #reservoirGate}: new merges and cold writes now target the
   *       fresh table.</li>
   *   <li>{@code settle-writes} — wait for {@link #mergesInFlight} to reach
   *       zero (hot merges that captured the OLD reference have finished),
   *       plus a 1ms quiescence window for cold writers preempted between
   *       reference capture and write (see class doc).</li>
   *   <li>{@code snapshot-promote} — snapshot the old table into a single
   *       map, promote keys whose cycle count exceeded
   *       {@link #hotThreshold} to the exact hot path, and tide.</li>
   * </ol>
   *
   * <p>Phases 3-5 are delegated to {@link #tideWatcher()} (shared with
   * {@link #destroy()}).
   */
  @SuppressWarnings("all")
  private void tide() {
    try {
      // (add-locals): every writer's hot local map enters the
      // shared table.  Per-writer mutex — an in-flight batch add is
      // waited out, never raced; the table-reference mutex makes the add
      // target capture atomic.
      for (Map.Entry<Thread, Ceils> entry : hotRegistry.entrySet()) {
        discharge(entry.getValue());
        // (reap-dead): reclaim dead writers' entries — their
        // residuals were just merged, so the registry cannot leak.
        if (!entry.getKey().isAlive()) {
          hotRegistry.remove(entry.getKey(), entry.getValue());
        }
      }
      Map<String, Long> snapshot = tideWatcher();
      if (snapshot != null) {
        // (snapshot-promote): promote keys that exceeded the
        // threshold to the exact hot path from the next cycle on.  The cap
        // is soft by design: promotion is single-threaded (the deliverer),
        // so the local counter below is race-free and stops exactly at
        // hotLimit — a hard putIfAbsent+trim scheme would add complexity
        // without tightening the bound.  `add` is putIfAbsent, so
        // re-promoting an already-hot key is a no-op (returns false and
        // does not advance the counter).
        int promoted = beacon.size();
        if (promoted < hotLimit) {
          for (Map.Entry<String, Long> e : snapshot.entrySet()) {
            if (e.getValue() >= hotThreshold && beacon.add(e.getKey())) {
              if (++promoted >= hotLimit) {
                break;
              }
            }
          }
        }
        batchConsumer.accept(snapshot);
      }
    } catch (Exception e) {
      log.error("Scheduled delivery failed", e);
    }
  }

  @Override
  public void afterPropertiesSet() {
    try {
      scheduler.scheduleAtFixedRate(this::tide, deliverIntervalMs, deliverIntervalMs, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      log.error(
        "Failed to start WaveCounter delivery scheduler; buffered counts will not be delivered. " +
          "Hot-key detection may be impaired.",
        e
      );
    }
  }

  /**
   * Final delivery of all merged counts and scheduler shutdown.
   *
   * <p>Sets {@code shutdown} first (new {@code count()} calls no-op), waits
   * for in-flight hot counts to drain (bounded by 1 second), merges every
   * writer's hot local map, then performs the same swap-and-snapshot as
   * {@link #tide()}.
   */
  @Override
  @SuppressWarnings("all")
  public void destroy() {
    // Stop accepting new counts first — everything counted before this
    // moment must be delivered; everything after is dropped by design.
    shutdown = true;
    if (ownsScheduler) {
      scheduler.shutdown();
      try {
        if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
          scheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        scheduler.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
    // Wait for in-flight hot counts to drain (bounded by 1s) so the final
    // add below sees every hot writer's complete local map.
    long deadline = System.nanoTime() + 1_000_000_000L;
    while (floatWatcher.sum() > 0 && System.nanoTime() < deadline) {
      Thread.yield();
    }
    // Merge all hot local maps (mutex-serialized), then rotate-and-snapshot
    // exactly like tide() — including the cold-write quiescence window.
    for (Map.Entry<Thread, Ceils> entry : hotRegistry.entrySet()) {
      discharge(entry.getValue());
    }
    Map<String, Long> snapshot = tideWatcher();
    if (snapshot != null) {
      batchConsumer.accept(snapshot);
    }
  }

  /**
   * MurmurHash3 32-bit finalizer (avalanche).
   *
   * <p>Mixes {@link String#hashCode()} so that keys whose hashes cluster on
   * the low bits (e.g. short numeric suffixes) spread evenly across the
   * writer-private local map's slots, instead of degrading the hot path's
   * open-addressing probes into long runs.  Cost is a few integer ops
   * (~1-2 ns) on the hot path.
   *
   * <p>Note: this defends against <em>distribution</em> attacks (low-bit
   * clustering), not against deliberately equal hash codes — identical
   * {@code hashCode()} values still map identically, which is the same
   * defense level as {@code ConcurrentHashMap}'s own spread (used by the
   * {@link #beacon} set and the cold direct-write table).
   *
   * @param h the raw {@code String.hashCode()} value
   * @return the avalanched hash
   */
  @SuppressWarnings("java:S3398")
  private static int mixHash(int h) {
    h ^= h >>> 16;
    h *= 0x85ebca6b;
    h ^= h >>> 13;
    h *= 0xc2b2ae35;
    h ^= h >>> 16;
    return h;
  }

  /**
   * Writer-private open-addressing map for hot keys.  Accessed only by its
   * owning thread, except under {@code synchronized (this)} by the deliverer
   * when merging the writer's residual data.
   *
   * <p>The capacity is fixed at {@link #LOCAL_CAPACITY}; the batch trigger
   * ({@code batchSize}) is at most half of it, so probing never runs out of
   * empty slots.
   */
  @SuppressWarnings("all")
  private static final class Ceils {

    final String[] keys = new String[LOCAL_CAPACITY];
    final long[] counts = new long[LOCAL_CAPACITY];
    final int[] tags = new int[LOCAL_CAPACITY];
    int size;
    int opCount;
    long lastFlushNanos = System.nanoTime();

    /**
     * Merge one increment into the local map.
     *
     * <p>{@code synchronized (this)}: the deliverer may add and reset this
     * map concurrently; without the mutex a half-written entry could be
     * reset, stranding its count.  Uncontended in the common case (only the
     * owner writer and the periodic deliverer contend).
     *
     * <p>The slot index uses {@link WaveCounter#mixHash(int)} — this map
     * is the only structure in the design that derives its index from raw
     * hash bits, so it carries the avalanche defence (see the method doc).
     */
    void add(String key, long delta) {
      synchronized (this) {
        // Open addressing with linear probing: `tag` is the key's hash
        // (0 mapped away, since 0 marks an empty slot); the probe starts at
        // the hashed slot and walks forward, wrapping via the power-of-two
        // mask.  At most batchSize entries are live, so a free slot is
        // always reached within the 256-slot table.
        int h = mixHash(key.hashCode());
        int tag = h == 0 ? Integer.MIN_VALUE : h;
        int i = h & (LOCAL_CAPACITY - 1);
        for (;;) {
          if (tags[i] == 0) {
            // empty slot: claim it (this thread owns the map, so no CAS),
            // store the key and the initial count, done
            tags[i] = tag;
            keys[i] = key;
            counts[i] = delta;
            size++;
            opCount++;
            return;
          }
          if (tags[i] == tag && key.equals(keys[i])) {
            // existing entry for this key: accumulate and done
            counts[i] += delta;
            opCount++;
            return;
          }
          // occupied by a different key (hash collision): probe next slot
          i = (i + 1) & (LOCAL_CAPACITY - 1);
        }
      }
    }

    /**
     * Drain every non-zero entry into the shared table and reset this map.
     *
     * <p>{@code synchronized (this)} — the map synchronizes on itself (never
     * on a method parameter): this serializes the owner writer's add
     * against the deliverer's add of the same map.  A concurrent reset
     * would otherwise strand half-written entries.  Uncontended in the
     * common case (only the owner writer and the periodic deliverer
     * contend).
     *
     * <p>The {@code mergesInFlight} counter is bumped for the duration of the
     * drain so {@link WaveCounter#tideWatcher()} waits for merges
     * that captured the OLD table reference before snapshotting it.
     *
     * @param table          the shared table to drain into (reference already
     *                       captured atomically vs the tide swap)
     * @param mergesInFlight the in-flight counter to bump during the drain
     */
    void drainInto(ConcurrentHashMap<String, LongAdder> table, PaddedMergesInFlight mergesInFlight) {
      synchronized (this) {
        mergesInFlight.incrementAndGet();
        try {
          for (int i = 0; i < LOCAL_CAPACITY; i++) {
            if (tags[i] != 0 && counts[i] != 0) {
              table.computeIfAbsent(keys[i], k -> new LongAdder()).add(counts[i]);
            }
          }
        } finally {
          mergesInFlight.decrementAndGet();
        }
        reset(); // entries are now in the shared table; the map is reusable
      }
    }

    void reset() {
      for (int i = 0; i < LOCAL_CAPACITY; i++) {
        tags[i] = 0;
        keys[i] = null;
        counts[i] = 0;
      }
      size = 0;
      lastFlushNanos = System.nanoTime();
    }
  }

  /** 120-byte leading pad to isolate the first hot field from object header and other instance fields. */
  @SuppressWarnings("all")
  static final class PaddedFloatWatcher {

    byte p000, p001, p002, p003, p004, p005, p006, p007;
    byte p008, p009, p010, p011, p012, p013, p014, p015;
    byte p016, p017, p018, p019, p020, p021, p022, p023;
    byte p024, p025, p026, p027, p028, p029, p030, p031;
    byte p032, p033, p034, p035, p036, p037, p038, p039;
    byte p040, p041, p042, p043, p044, p045, p046, p047;
    byte p048, p049, p050, p051, p052, p053, p054, p055;
    byte p056, p057, p058, p059, p060, p061, p062, p063;
    byte p064, p065, p066, p067, p068, p069, p070, p071;
    byte p072, p073, p074, p075, p076, p077, p078, p079;
    byte p080, p081, p082, p083, p084, p085, p086, p087;
    byte p088, p089, p090, p091, p092, p093, p094, p095;
    byte p096, p097, p098, p099, p100, p101, p102, p103;
    byte p104, p105, p106, p107, p108, p109, p110, p111;
    byte p112, p113, p114, p115, p116, p117, p118, p119;

    final LongAdder value = new LongAdder();

    byte p120, p121, p122, p123, p124, p125, p126, p127;
    byte p128, p129, p130, p131, p132, p133, p134, p135;
    byte p136, p137, p138, p139, p140, p141, p142, p143;
    byte p144, p145, p146, p147, p148, p149, p150, p151;
    byte p152, p153, p154, p155, p156, p157, p158, p159;
    byte p160, p161, p162, p163, p164, p165, p166, p167;
    byte p168, p169, p170, p171, p172, p173, p174, p175;
    byte p176, p177, p178, p179, p180, p181, p182, p183;
    byte p184, p185, p186, p187, p188, p189, p190, p191;
    byte p192, p193, p194, p195, p196, p197, p198, p199;
    byte p200, p201, p202, p203, p204, p205, p206, p207;
    byte p208, p209, p210, p211, p212, p213, p214, p215;
    byte p216, p217, p218, p219, p220, p221, p222, p223;
    byte p224, p225, p226, p227, p228, p229, p230, p231;
    byte p232, p233, p234, p235, p236, p237, p238, p239;

    void increment() {
      value.increment();
    }

    void decrement() {
      value.decrement();
    }

    long sum() {
      return value.sum();
    }
  }

  /** 120-byte leading pad to isolate the first hot field from object header and other instance fields. */
  @SuppressWarnings("all")
  static final class PaddedMergesInFlight {

    byte p000, p001, p002, p003, p004, p005, p006, p007;
    byte p008, p009, p010, p011, p012, p013, p014, p015;
    byte p016, p017, p018, p019, p020, p021, p022, p023;
    byte p024, p025, p026, p027, p028, p029, p030, p031;
    byte p032, p033, p034, p035, p036, p037, p038, p039;
    byte p040, p041, p042, p043, p044, p045, p046, p047;
    byte p048, p049, p050, p051, p052, p053, p054, p055;
    byte p056, p057, p058, p059, p060, p061, p062, p063;
    byte p064, p065, p066, p067, p068, p069, p070, p071;
    byte p072, p073, p074, p075, p076, p077, p078, p079;
    byte p080, p081, p082, p083, p084, p085, p086, p087;
    byte p088, p089, p090, p091, p092, p093, p094, p095;
    byte p096, p097, p098, p099, p100, p101, p102, p103;
    byte p104, p105, p106, p107, p108, p109, p110, p111;
    byte p112, p113, p114, p115, p116, p117, p118, p119;

    final AtomicInteger value = new AtomicInteger();

    byte p120, p121, p122, p123, p124, p125, p126, p127;
    byte p128, p129, p130, p131, p132, p133, p134, p135;
    byte p136, p137, p138, p139, p140, p141, p142, p143;
    byte p144, p145, p146, p147, p148, p149, p150, p151;
    byte p152, p153, p154, p155, p156, p157, p158, p159;
    byte p160, p161, p162, p163, p164, p165, p166, p167;
    byte p168, p169, p170, p171, p172, p173, p174, p175;
    byte p176, p177, p178, p179, p180, p181, p182, p183;
    byte p184, p185, p186, p187, p188, p189, p190, p191;
    byte p192, p193, p194, p195, p196, p197, p198, p199;
    byte p200, p201, p202, p203, p204, p205, p206, p207;
    byte p208, p209, p210, p211, p212, p213, p214, p215;
    byte p216, p217, p218, p219, p220, p221, p222, p223;
    byte p224, p225, p226, p227, p228, p229, p230, p231;
    byte p232, p233, p234, p235, p236, p237, p238, p239;

    int get() {
      return value.get();
    }

    void incrementAndGet() {
      value.incrementAndGet();
    }

    void decrementAndGet() {
      value.decrementAndGet();
    }
  }
}
