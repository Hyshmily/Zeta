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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import javax.security.auth.Destroyable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;

/**
 * Per-key routing counter: aggregates high-frequency single-key increments
 * and delivers them as batched {@code Map<key, count>} snapshots to a
 * downstream consumer.
 *
 * <h3>Design rationale</h3>
 *
 * <p><b>Problem.</b> N writer threads increment per-key counters at high
 * frequency; a periodic snapshot of distinct-key counts must be delivered
 * to a downstream consumer (a hot-key detector, a cluster reporter).  The
 * workload is skewed — a few keys carry most of the traffic — and the key
 * universe is largely stable across cycles.  The design must budget four
 * resources: aggregate throughput (multi-million ops/s at 16 threads),
 * memory (bounded, flat under churn), delivery latency (bounded), and a
 * precisely documented precision model.
 *
 * <p><b>Core idea — heat-aware routing.</b> A single shared table would
 * serialize every writer on the hot keys; per-writer buffering for every
 * key would multiply memory and delivery work.  WaveCounter routes each
 * count into one of two paths, sharing a single {@link ConcurrentHashMap}:
 * <ul>
 *   <li><b>Hot path</b> — each writer merges hot keys into its own private
 *       open-addressing map (zero shared access, no contention); every
 *       {@code opMaxCount} increments the local map is bulk-merged into
 *       the shared table.  Fixed per-op costs: the {@link ReentrantLock}
 *       guarding the local map against drain (see the lock rationale
 *       below) and the routing beacon read.</li>
 *   <li><b>Cold path</b> — a direct lock-free {@code ConcurrentHashMap}
 *       increment with no local layer: the cheapest possible write, at the
 *       cost of a documented approximate snapshot window (see the
 *       Correctness model).</li>
 * </ul>
 * Promotion from cold to hot is a per-cycle decision: every delivery
 * scans the snapshot and promotes the keys that earn the top
 * {@code hotLimit} slots, estimated from a log2-bucket histogram of the
 * cycle's counts (floored at {@link #PROMOTION_FLOOR}); the key takes the
 * exact hot path from the next cycle on.  Membership is time-sampled:
 * the counting beacon decays every tide, so a key that keeps earning the
 * boundary stays hot while a drifted-away key leaves within 2 tides —
 * the hot set follows drifting heat instead of freezing.  Promotion
 * gates only performance, never correctness.
 *
 * <p><b>Precision budget.</b> Exactness is expensive in proportion to
 * the time a writer holds a shared structure.  The design partitions the
 * budget by heat: the hot path pays for exactness (in-flight merges are
 * waited out), the cold path pays for throughput (a bounded approximate
 * window of ~1e-5/op).  Sustained hot keys are promoted and then take the
 * exact path, so the approximation applies only to traffic that does not
 * justify exactness.
 *
 * <p><b>Default parameters.</b> Each default is a measured design
 * decision, not an operator knob:
 * <ul>
 *   <li><b>opMaxCount = 128</b> (the local batch size) — the knee of a
 *       64/128 sweep at 16 threads: small enough that hot local data ages
 *       within one flush interval, large enough to amortize the
 *       shared-table add (~0.3ns/op at 16 threads).</li>
 *   <li><b>Promotion boundary, not a threshold</b> — the top
 *       {@code hotLimit} keys of the cycle earn the hot slots, estimated
 *       from a log2-bucket histogram of the snapshot (one O(n) pass with
 *       a leading-zeros log per key; the boundary lands on a power of
 *       two).  Scale-free by construction: a relative hot spot qualifies
 *       at ANY traffic volume — the legacy fixed threshold (100
 *       counts/cycle) silently failed for keys carrying most of a
 *       low-volume cycle's traffic (measured: 0 promotions at an 80-count
 *       top key).  {@link #PROMOTION_FLOOR} (10) keeps noise keys (1-9
 *       counts/cycle) out of the hot set.  A snapshot below
 *       {@link #MIN_PROMOTION_KEYS} (16) distinct keys skips promotion
 *       entirely (Caffeine's min-signal discipline — the decay reclaims
 *       any slots anyway, so this guards against wasted promotion work,
 *       not permanent pollution).</li>
 *   <li><b>hotLimit = 1024</b> — caps the active promoted hot set so
 *       pathological traffic cannot grow it unboundedly.  The routing
 *       beacon is the compact 2+2 role-evidence array of
 *       {@code hotLimit × 32} rooms (≈16 KB at 1024 — no key strings
 *       retained, so cold keys stay GC-eligible), with a ~0.37%
 *       false-positive rate at full capacity — routing-only, never
 *       correctness (a false positive merely routes a cold key onto the
 *       bounded hot path, whose counts converge in the same table).
 *       The beacon is read on EVERY count, so its size has a cache-level
 *       throughput effect — 1024 (16 KB, L1/L2-resident) measured
 *       consistently fastest on a 16-thread sweep; the "capacity starves"
 *       penalty for limits below the hot-key count did not materialize
 *       (the cold path carries un-promoted hot keys without measurable
 *       loss).  Throughput-sensitive deployments may lower it further,
 *       capacity-hungry ones may raise it back.</li>
 *   <li><b>Promotion floor, admit-on-block</b> — the absolute floor (10)
 *       is the seed of a {@link MoonsTidalForce}: the floor never
 *       filters above the histogram boundary.  Distress with keys
 *       blocked behind the floor (the renewing keys the boundary would
 *       admit) drops the floor to the boundary so they qualify; an
 *       empty hot set collapses the floor to the seed.  The former
 *       raise-on-blocked direction was procyclical — it ratcheted the
 *       floor past the whole distribution on rotating hot sets and
 *       oscillated forever under key churn.  Veto-return, audit clock
 *       and saturation release are kept as defensive machinery; the
 *       histogram boundary stays scale-free.</li>
 *   <li><b>deliverIntervalMs = 500, adaptive</b> — delivery latency is
 *       bounded by one adaptive interval: a tide that delivered ≥ 20,000
 *       distinct keys re-schedules at 50ms, scaling linearly back to the
 *       base as the backlog shrinks — trading burst detection latency
 *       against delivery overhead, self-damping as the reservoir drains.
 *       The ramp is driven by a {@link TidePacer}: the raw backlog is
 *       folded into a smoothed reference with a fast-attack (bursts
 *       shorten the cycle immediately) / slow-release (the fast cadence
 *       drains the reservoir over ~5 tides instead of snapping back)
 *       asymmetry and a still band (in-band jitter moves nothing);
 *       consecutive EMPTY tides stretch the cadence up to 2x the base
 *       (the empty-tide ladder — idle cycles stop paying the quiescence,
 *       decay and wakeup at the base rate), reset by any non-empty tide.</li>
 *   <li><b>SNAPSHOT_QUIESCENCE = 1ms</b> — the price of the cold
 *       approximate window (see the Correctness model).  Paid only when a
 *       cold writer may be in flight: the {@code coldWriteSeen} flag set
 *       by every cold first-insert (before the insert) is captured and
 *       cleared at the swap; a cycle with no cold traffic skips the window
 *       entirely (residual loss window shrinks to the ns-scale get-to-flag
 *       gap), while a cycle with cold traffic pays the same parked wait.
 *       The wait parks instead of spinning, so the cost is a yielded core
 *       for most of the window, not CPU burn.</li>
 * </ul>
 *
 * <p><b>Correctness model:</b>
 * <ul>
 *   <li><b>Hot path is exact.</b>  Each writer's local map is mutex-serialized
 *       between the owner and the deliverer; the shared-table reference is
 *       captured under {@link #reservoirGate} and the in-flight slot is
 *       reserved atomically with the capture, so the deliverer waiting for
 *       {@link #mergesInFlight} to reach zero can never snapshot a table
 *       that a captured merge still targets — no in-flight hot add can be
 *       stranded.</li>
 *   <li><b>Cold path is approximate.</b>  A cold writer that captured the
 *       table reference just before the tide swap may write into the old
 *       table after the snapshot.  The tide/destroy quiescence window
 *       (1ms) reduces this to a preemption of &gt; 1ms — measured on
 *       deliver-racing and slow-consumer stress: typical loss 0, worst
 *       observed ≈2.1e-5/op.  The window is gated by the
 *       {@code coldWriteSeen} flag: skipped entirely on cycles with no
 *       cold traffic (residual loss window = the ns-scale gap between a
 *       writer's reference read and its flag store, and a miss-path
 *       writer preempted across the swap re-targets the NEW table via
 *       its {@code computeIfAbsent} re-read), unchanged when the flag is
 *       set.  Sustained hot keys are promoted and then take the exact
 *       path.</li>
 * </ul>
 *
 * <p><b>Delivery:</b> a self-rescheduling flusher (a one-shot tide that
 * re-arms itself at a backlog-adaptive delay) merges every writer's hot
 * local map, swaps the shared table, waits for in-flight merges plus the
 * gated quiescence window, snapshots the old table into a single map and
 * delivers it once per cycle.  Dead writers' registry entries are removed;
 * their residual local counts are merged first.
 *
 * <p><b>Memory:</b> the shared table holds exactly the live key set (no
 * per-batch duplication); each writer's hot local map is bounded by
 * {@code opMaxCount} and the hot set by {@code hotLimit}.  Cold- and
 * hot-path {@link LongAdder}s are recycled across cycles from the
 * previous tide's drained table (see {@link #ebbReservoir}), so stable
 * key universes allocate no adders per cycle; the pool is bounded by one
 * cycle's key universe.
 *
 */

@Slf4j
@Internal
public class WaveCounter implements InitializingBean, Destroyable {

  /**
   * Default local increments before a bulk add into the shared table.
   *
   * <p>The knee of a 64/128 sweep on the hot path after the per-op
   * in-flight counter was removed: the larger batch measured at or above
   * the smaller across every workload.  Half of {@link #LOCAL_CAPACITY},
   * so the open-addressing probe never fills.
   */
  public static final int DEFAULT_MAX_OPCOUNT = 128;

  /** Default max age of local hot data before the writer bulk-merges it. */
  public static final long DEFAULT_FLUSH_INTERVAL_MS = 50;

  /** Default delivery cadence for the shared snapshot. */
  public static final long DEFAULT_DELIVER_INTERVAL_MS = 500;

  /**
   * Absolute floor for promotion: a key is never promoted
   * below this per-cycle count, regardless of the histogram boundary —
   * guards against noise keys (counting 1-9 per cycle) taking hot slots.
   * Kept far below the fixed 100-count threshold it replaces, so relative
   * hot spots at low traffic still qualify (the fixed threshold silently
   * failed for keys that carry most of a low-volume cycle's traffic).
   *
   * <p>This is the {@link #moonsTidalForce}'s seed and lower clamp: the promotion
   * floor is adaptive, never above {@link #GOVERNOR_FLOOR_MAX}.
   */
  private static final long PROMOTION_FLOOR = 10;

  /**
   * Minimum snapshot size for promotion (Caffeine
   * WindowClimber's min-signal discipline): a snapshot with fewer
   * distinct keys than this carries no meaningful distribution — the
   * histogram boundary would collapse to the smallest non-zero bucket
   * and promote everything.  The decay would reclaim the slots within 2
   * tides anyway, so this guards against wasted promotion work on
   * startup noise, not permanent pollution; below this size the
   * promotion pass is skipped entirely.
   */
  private static final int MIN_PROMOTION_KEYS = 16;

  /** Log2-bucket count for the promotion histogram (see the tide's promotion pass). */
  private static final int HISTOGRAM_BUCKETS = 64;

  /**
   * Default maximum number of promoted hot keys (capped; further promotions are skipped).
   *
   * <p>The counting beacon is ~24 KB at this limit vs ~80 KB at 4096, and
   * every count reads it — a 16-thread sweep across five workloads
   * measured +16..36% throughput at 1024, including an N=2000 hot-key load
   * that exceeds the capacity (the cold path carries un-promoted hot keys
   * without measurable loss, so the capacity penalty never materialized).
   * The beacon stays L2-resident.
   */
  public static final int DEFAULT_HOT_LIMIT = 1024;

  /** Quiescence window after the table swap before snapshotting (see class doc). */
  private static final long SNAPSHOT_QUIESCENCE_NANOS = 1_000_000L; // 1ms

  /**
   * Bounded spin iterations for the {@code settle-writes} wait. Hot merges
   * are µs-scale (a bounded 256-slot drainInto), so spinning briefly with
   * {@link Thread#onSpinWait()} avoids the yield syscall for the common
   * short wait; the bound guarantees a preempted writer cannot stall the
   * deliverer on a busy-spin for longer than a few µs before yielding.
   */
  private static final int SETTLE_SPIN_ITERATIONS = 1024;

  /**
   * Adaptive-delivery floor: under backlog the tide re-schedules at no less
   * than this interval, bounding burst detection latency and keeping the
   * reporter's reservoir away from its capacity cap.
   */
  private static final long EARLY_TIDE_MIN_INTERVAL_MS = 50;

  /**
   * Backlog threshold for adaptive delivery: a tide that delivered this many
   * distinct keys (≈20% of the reporter's 100k capacity cap) re-schedules at
   * {@link #EARLY_TIDE_MIN_INTERVAL_MS}; below it the delay scales linearly
   * back to {@code deliverIntervalMs}.
   */
  private static final int EARLY_TIDE_THRESHOLD_KEYS = 20_000;

  /**
   * Tide-pacer release rate (WindowClimber's {@code Rates}
   * smoothing): when the backlog falls, the smoothed reference folds the
   * new value with this EMA constant (~5-tide memory), so a burst's fast
   * cadence decays back to the base over several tides instead of
   * snapping the very next quiet tide (the ping-pong of the raw law).
   * The attack side folds the FULL burst immediately (see
   * {@link #PACER_STILL_BAND_KEYS}) so burst detection latency is
   * unchanged.
   */
  private static final double PACER_RELEASE_RATE = 0.2;

  /**
   * Tide-pacer still band (WindowClimber's {@code stableBand}
   * / AuditClock discipline): a backlog move smaller than this (5% of
   * {@link #EARLY_TIDE_THRESHOLD_KEYS}) is noise — it neither moves the
   * smoothed reference (anti-ping-pong deadband) nor counts as a burst.
   * A move within the band leaves the cadence still; movement is decayed,
   * never reset, so a single big tide cannot toggle the ramp.
   */
  private static final long PACER_STILL_BAND_KEYS = EARLY_TIDE_THRESHOLD_KEYS / 20;

  /**
   * Empty-tide ladder (WindowClimber's {@code Ladder}): each
   * consecutive EMPTY tide stretches the next cadence by a power of two,
   * capping at this multiple of {@code deliverIntervalMs} — idle cycles
   * stop paying the 1ms quiescence, the decay sweep and the scheduler
   * wakeup every 500ms.  Any non-empty tide resets the streak (the
   * ladder's confirm).  The stretch is capped at 2x so a burst is
   * detected within at most two base intervals.
   */
  private static final int EMPTY_TIDE_STRETCH_CAP_MULTIPLE = 2;

  /** Power-of-two shift per consecutive empty tide (1 = at most 2x the base). */
  private static final int EMPTY_TIDE_STRETCH_MAX_SHIFT = 1;

  /**
   * Promotion-governor renewal target (WindowClimber
   * borrowings): the share of active hot slots whose key earned traffic
   * this tide below which the governor treats the hot set as distressed.
   * Probed on real workloads: a healthy single-key set reads 1.0, a
   * two-key set with one drifter 0.5, a fully drifted set 0.0.
   */
  private static final double GOVERNOR_RENEWAL_TARGET = 0.5;

  /** Promotion-governor ceiling on the floor; the seed and lower clamp are {@link #PROMOTION_FLOOR}. */
  private static final int GOVERNOR_FLOOR_MAX = 256;

  /** Promotion-governor initial probe step, in count units. */
  private static final int GOVERNOR_STEP_INITIAL = 16;

  /** Promotion-governor step decay toward convergence (WindowClimber's {@code Step}). */
  private static final double GOVERNOR_STEP_DECAY = 0.98;

  /**
   * Promotion-governor veto streak: consecutive distress tides after
   * which a failed raise probe retreats to where it started
   * (WindowClimber's {@code Anchor} veto / probe-walk undo).
   */
  private static final int GOVERNOR_VETO_STREAK = 4;

  /** Promotion-governor budgeted retreat strides (WindowClimber's veto-return). */
  private static final int GOVERNOR_RETURN_BUDGET = 8;

  /** Per-stride cap while returning to the probe base. */
  private static final int GOVERNOR_RETURN_STEP = 8;

  /**
   * Promotion-governor audit wait: healthy-and-still tides after which a
   * raised floor is probed one step back down (WindowClimber's
   * {@code AuditClock} re-testing a still equilibrium) — releases the
   * ratchet a distress phase left behind.
   */
  private static final int GOVERNOR_AUDIT_WAIT = 8;

  /** Promotion-governor saturation fraction: the hot set is "full" at 90% of {@link #hotLimit}. */
  private static final double GOVERNOR_SATURATION_FRACTION = 0.9;

  /** Local map capacity (power of two); must be ≥ 2 × opMaxCount so probing never fills it. */
  private static final int LOCAL_CAPACITY = 256;

  /** Time-check sampling: every 16th local add re-checks the flush clock. */
  private static final int TIME_CHECK_MASK = 15;

  /** Local increments before a bulk add into the shared table (hot path). */
  private final int opMaxCount;

  /** Max age of local hot data before the writer bulk-merges it (nanoseconds). */
  private final long flushIntervalNanos;

  /** Cadence of shared-table snapshot delivery. */
  private final long deliverIntervalMs;

  /** Maximum number of promoted hot keys (capped; further promotions are skipped). */
  private final int hotLimit;

  /**
   * Promoted hot-key routing beacon.  A fast membership test on the hot
   * path (~3-10ns) decides the routing; sustained hot keys are promoted by
   * the delivery-time scan, capped at {@link #hotLimit}.
   *
   * <p><b>Compact 2+2 role evidence.</b>  The beacon is one
   * array over the room space (sized {@code hotLimit × 32} rooms, rounded
   * to a power of two), 4 bits per room (16 rooms per long): low 2 bits
   * = the bit1-role evidence, high 2 bits = the bit2-role evidence
   * (~16 KB at the default 1024 limit).
   * <ul>
   *   <li><b>Each role needs only 2 bits (saturating at 3)</b>: the
   *       first-promotion seed of 2 plus the per-tide halving sweep
   *       {@code >> 1} (see {@link #decayCounts()}) gives the 2-tide
   *       memory — a drifted-away key leaves within 2 tides, a new hot
   *       key can be promoted on any tide (the freeze-at-hotLimit problem
   *       of the old bit-only design is gone), a stable hot key
   *       re-promotes every tide and never decays out (no ping-pong).
   *       Member test: both role evidences {@code >= 1}.</li>
   *   <li><b>Role separation is the k=2 point</b>: a room serves two
   *       roles — as some key's bit1 (count evidence) and as another
   *       key's bit2 (trace evidence).  The two 2-bit fields are
   *       independent, so a promoted key always satisfies both (no false
   *       negatives), and a false positive needs both a polluted count
   *       room AND a polluted trace room.  When both of a key's hashes
   *       land in the same room (1/32768), the fields stay independent
   *       (each written to its own half).</li>
   * </ul>
   * Counting is monotonic (values decay toward zero, never wrap), so the
   * trace density is bounded by the recent promotions — no historical
   * drift, no epoch alias; the false-positive rate stays ~0.04% instead
   * of rising toward the k=1 level.  Only the single deliverer thread
   * writes the array (read-modify-write, no CAS needed).  Writers read via
   * PLAIN (non-volatile) array loads: the beacon is routing-only, so a
   * stale read is harmless — a writer that has not yet observed a
   * promotion merely routes the key down the cold path, which is always
   * correct (the same key is re-promoted next tide).  Hardware cache
   * coherence (immediate on x86 TSO, eventual on weaker architectures)
   * bounds the staleness in practice; the JMM does not guarantee when the
   * update is seen, and correctness never depends on it.  A torn 64-bit
   * load is non-tearing in practice on HotSpot for aligned accesses, and
   * would be harmless anyway — garbage evidence reads as a false negative,
   * which routes cold (correct).  See ADR-0038 (consolidated WaveCounter tide controls).
   *
   * <p><b>Accepted imperfection.</b>  The evidence value is a memory
   * mark, not a literal promotion count (first seed is 2).  Routing-only,
   * never correctness.
   *
   * <p><b>Why k=2 is deliberate.</b> The optimal hash count for a Bloom
   * filter is {@code k = (m/n)·ln2} — at our {@code m/n = 32} that is ≈22,
   * which would push the false-positive rate from 0.37% down to ~1e-7 at
   * the same memory. We deliberately do not chase that: false positives are
   * <em>free</em> here (routing-only), and k=22 would cost 22 bit tests per
   * count instead of 2. The k=2 budget is set by the hot-path latency, not
   * by the false-positive math. If k ever needs to grow, use the
   * Kirsch–Mitzenmacher combination {@code g_i = (h1 + i·h2) & beaconMask}
   * from the two base hashes — one add and mask per extra position instead
   * of an additional seeded avalanche per k.
   *
   */
  private final long[] beacon;
  /** Bit mask for the routing beacon (power-of-two size minus one). */
  private final int beaconMask;

  /**
   * Thread-local hot aggregation map (writer-private, zero sharing).
   * Only the owner thread writes it; the deliverer drains it via the
   * per-map {@link ReentrantLock} (or lock-free for dead writers —
   * see {@code drainDead}).
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
  // volatile reference: replacement semantics, state guarded by CHM + reservoirGate
  private volatile ConcurrentHashMap<String, LongAdder> reservoir = new ConcurrentHashMap<>();

  /**
   * Adder recycling pool: the previous cycle's drained table, published by
   * the deliverer (volatile) only AFTER every adder was summed — the pool
   * therefore holds only fully-consumed adders whose counts already entered
   * a snapshot.  Cold-path first inserts and hot-path batch drains
   * ({@code Ceils.waveTo}) steal one from here instead of allocating (see
   * {@link #stealRecycled}), eliminating the per-cycle LongAdder churn
   * for stable key universes.
   *
   * <p>Zeroing happens at steal time ({@link LongAdder#reset()}), not at
   * snapshot time, so the tide keeps paying only the read-only
   * {@code sum()} it always paid.  A stolen adder's residual (the previous
   * cycle's count, or a late cold write from a writer preempted across the
   * quiescence window) is reset away — lost exactly like the documented cold
   * approximate window, never carried into the next cycle.
   *
   * <p>Memory is bounded by one cycle's key universe: the pool is replaced
   * wholesale at every tide (the just-drained old table becomes the pool;
   * the previous pool is dropped).  {@link #clear()} and {@link #destroy()}
   * drop the pool reference so no recycled state survives a reset.
   */
  @SuppressWarnings("java:S3077")
  private volatile ConcurrentHashMap<String, LongAdder> ebbReservoir;

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

  /** Whether {@link #afterPropertiesSet()} started the self-rescheduling delivery chain. */
  private volatile boolean deliveryStarted;

  /**
   * Soft cap on distinct cold keys per delivery cycle ({@code 0} = unbounded).
   * Set by the compatibility constructor (e.g. the reporter's
   * {@code MAX_BUFFER_SIZE}); the detection path keeps it unbounded because
   * {@link #hotLimit} already bounds its promoted set.
   */
  private int capacity;

  /**
   * O(1) distinct-key counter for the cold-path capacity guard, replacing
   * {@code ConcurrentHashMap.size()} (which reads the CHM counter cells —
   * O(cores) volatile reads — on every miss).  Incremented inside
   * {@code computeIfAbsent}'s mapping function (exactly once per real
   * first insert, under the bin lock, so a plain atomic increment is
   * correct), and reset at every table swap.  Exactly equal to the
   * table's distinct-key count except for the swap race window (a
   * first-insert that captured the OLD table reference after the reset
   * over-counts by one until the next swap) — well within the documented
   * "approximate (racy size check)" semantics.
   */
  private final AtomicLong approximateSize = new AtomicLong();

  /**
   * Quiescence gate: set by any cold-path first-insert BEFORE
   * its write, cleared by the deliverer at table swap.  When unset, no
   * cold writer is in flight and the tide skips the 1ms quiescence window;
   * when set, the window is paid (same bound as before).  The residual
   * loss window when skipped is the ns-scale gap between a writer's
   * reference read and this flag's store; a miss-path writer preempted
   * across the swap re-targets the NEW table (its computeIfAbsent
   * re-reads the field), so the flag cannot lose a write by itself.
   *
   * <p>Set only on the cold-miss branch (the drop branch returns before
   * it — a dropped key never writes), never on the hot path or the
   * hit path (a hit implies an insert this cycle, which set the flag).
   */
  private volatile boolean coldWriteSeen;

  /**
   * Pacing state for the adaptive tide cadence: EWMA-smoothed
   * backlog (fast attack, slow release, still band) plus the empty-tide
   * stretch ladder.  Written only by the deliverer thread, so no
   * synchronization.
   */
  private final TidePacer pacer = new TidePacer();

  /**
   * Adaptive promotion floor (WindowClimber borrowings): the
   * floor probes up while keys are blocked behind it, retreats when the
   * probe fails, and audits back down under health or saturation.
   * Written only by the deliverer thread, so no synchronization.
   */
  private final MoonsTidalForce moonsTidalForce = new MoonsTidalForce();

  /**
   * Log2-bucket promotion histogram, reused across tides instead of
   * allocated per tide (the deliverer is the only writer).  Zeroed with
   * {@link Arrays#fill} at the start of each promotion pass.
   */
  private final int[] histogram = new int[HISTOGRAM_BUCKETS];

  public WaveCounter(Consumer<Map<String, Long>> batchConsumer) {
    this(
      batchConsumer,
      DEFAULT_MAX_OPCOUNT,
      DEFAULT_FLUSH_INTERVAL_MS,
      DEFAULT_DELIVER_INTERVAL_MS,
      DEFAULT_HOT_LIMIT,
      true,
      null
    );
  }

  public WaveCounter(Consumer<Map<String, Long>> batchConsumer, ScheduledExecutorService scheduler) {
    this(
      batchConsumer,
      DEFAULT_MAX_OPCOUNT,
      DEFAULT_FLUSH_INTERVAL_MS,
      DEFAULT_DELIVER_INTERVAL_MS,
      DEFAULT_HOT_LIMIT,
      false,
      scheduler
    );
  }

  /**
   * Compatibility constructor mirroring the deprecated
   * {@link BufferedCounter} 5-arg shape so existing call sites (e.g.
   * {@code KeyReporterImpl}) compile unchanged.  The eager-swap
   * ratio is a concept of the double-buffer design and is ignored here;
   * the capacity is wired as a <em>soft</em> cap on the cold-path
   * reservoir: once the shared table reaches it, <b>new</b> keys are
   * dropped (counted) while already-tracked keys keep counting. The cap is
   * approximate (racy size check) — it bounds memory, not exactness.
   *
   * @param batchConsumer     downstream consumer of merged snapshots
   * @param capacity          max distinct cold keys per delivery cycle;
   *                          {@code <= 0} means unbounded
   * @param flushIntervalMs   max age of local data before the writer merges it
   * @param ignoredSwapRatio  ignored (no eager-swap in this design)
   * @param scheduler         scheduler for the periodic flusher (not shut down by this instance)
   */
  public WaveCounter(
    Consumer<Map<String, Long>> batchConsumer,
    int capacity,
    long flushIntervalMs,
    double ignoredSwapRatio,
    ScheduledExecutorService scheduler
  ) {
    this(
      batchConsumer,
      DEFAULT_MAX_OPCOUNT,
      flushIntervalMs,
      DEFAULT_DELIVER_INTERVAL_MS,
      DEFAULT_HOT_LIMIT,
      false,
      scheduler
    );
    this.capacity = Math.max(0, capacity);
  }

  @SuppressWarnings("java:S107")
  // 8 constructor params: batch geometry + delivery + lifecycle, all required
  private WaveCounter(
    Consumer<Map<String, Long>> batchConsumer,
    int opMaxCount,
    long flushIntervalMs,
    long deliverIntervalMs,
    int hotLimit,
    boolean ownsScheduler,
    ScheduledExecutorService scheduler
  ) {
    this.batchConsumer = batchConsumer;
    this.opMaxCount = opMaxCount;
    this.flushIntervalNanos = flushIntervalMs * 1_000_000L;
    this.deliverIntervalMs = deliverIntervalMs;
    this.hotLimit = hotLimit;
    // Counting/evidence beacon, sized hotLimit × 32 rooms rounded up to a
    // power of two (~0.37% false-positive rate at full capacity; ~24 KB at
    // the default 1024 limit — the previous CHM key set retained ~0.5 MB
    // of Strings). The power-of-two size keeps the room index a mask, and
    // the size itself never changes, so promotion never resizes.
    long wantBits = Math.max(1L, hotLimit * 32L);
    int beaconBitCount = 1;
    while (beaconBitCount < wantBits && beaconBitCount > 0) {
      beaconBitCount <<= 1;
    }
    this.beaconMask = beaconBitCount - 1;
    this.beacon = new long[beaconBitCount >>> 2]; // 4 bits per room (2+2 roles)
    this.ownsScheduler = ownsScheduler;
    this.scheduler = ownsScheduler
      ? new SafeScheduledExecutorService(1, new ZetaThreadFactory("zeta-hot-route-counter-flusher"))
      : scheduler;
  }

  /**
   * Record one or more accesses for the given key.
   *
   * <p><b>Routing:</b> a {@link #isBeaconMember(int)} membership test (~3-10ns) picks the path.
   * <ul>
   *   <li><b>Hot path</b> — add into the thread-local map (zero shared
   *       access), and every {@code opMaxCount} merges (or after the flush
   *       interval) bulk-add into the shared table.  Hot keys see the
   *       shared table once per batch instead of once per increment.</li>
   *   <li><b>Cold path</b> — direct lock-free {@code ConcurrentHashMap}
   *       write: the cheapest possible path, no local layer, no per-op
   *       protection (see the class doc for the approximate window).</li>
   * </ul>
   *
   * @param key   the accessed key (must not be {@code null})
   * @param delta the number of accesses (positive; non-positive deltas
   *              and empty keys are silently ignored)
   */
  @SuppressWarnings("all")
  public void count(String key, long delta) {
    if (key.isEmpty() || delta <= 0 || shutdown) {
      // destroyed: drop silently (all counts were already drained)
      return;
    }

    // The avalanched key hash is computed once and shared by the beacon
    // routing check and the hot local map — one mixHash per count instead
    // of three.
    int h = mixHash(key.hashCode());

    if (isBeaconMember(h)) {
      // A hot key is accumulated in this writer's private map; the shared
      // table sees it once per batch instead of once per increment, so N
      // writers never contend on the same shared entry.
      Ceils m = hotLocals.get();
      if (m == null) {
        // First hot count from this writer: register its local map so the
        // deliverer can add residuals if the thread dies (isAlive).
        m = new Ceils();
        hotLocals.set(m);
        hotRegistry.put(Thread.currentThread(), m);
      }

      m.add(key, delta, h);
      // Sampled add trigger: either the batch is full, or the flush
      // clock expired (re-checked on 1/16 merges) — keeps local data from
      // aging beyond the flush interval without a clock read per op.
      if (
        m.size >= opMaxCount ||
        ((m.opCount & TIME_CHECK_MASK) == 0 &&
          m.size > 0 &&
          (System.nanoTime() - m.lastFlushNanos) > flushIntervalNanos)
      ) {
        discharge(m);
      }
    } else {
      // The cheapest possible path (plain CHM increment).  A cold writer
      // that captured the table reference just before the tide swap may
      // write into the old table after the snapshot; the tide/destroy 1ms
      // quiescence window bounds this to a preemption > 1ms (see class doc).
      LongAdder cell = reservoir.get(key);
      if (cell == null) {
        // Soft capacity guard: only NEW keys at capacity are dropped —
        // keys already tracked keep counting, so the bound limits memory
        // (key cardinality) without biasing established counters.  The
        // guard is paid only on the miss branch (get() returned null), so
        // the steady-state hit path is a single table lookup plus a
        // striped add (was: containsKey + size() + computeIfAbsent — three
        // CHM ops).  The capacity test uses the O(1) approximateSize
        // instead of ConcurrentHashMap.size() (O(cores) counter-cell reads
        // per miss).  The former containsKey re-check is dropped: it was
        // redundant with the get() above except for
        // the µs race where a key is inserted by another thread between
        // the two reads at the capacity boundary, which is inside the
        // documented "approximate (racy size check)" semantics.  Keeping
        // the guard BEFORE the flag/insert keeps the dominant
        // drop path at one get + one atomic read.
        if (capacity > 0 && approximateSize.get() >= capacity) {
          return;
        }
        // (quiescence-gate): a cold writer that may land in the CURRENT
        // table marks the flag BEFORE its insert; the tide clears it at
        // swap and skips the 1ms quiescence window on cycles where no
        // cold writer was in flight (see the field doc).  A dropped key
        // never reaches this store — only real writes mark the flag.
        coldWriteSeen = true;
        // First insert: two-phase steal + putIfAbsent instead of
        // computeIfAbsent's mapping closure — the closure escaped into CHM
        // and was allocated per real first-insert (GC pressure under key
        // churn).  The steal runs exactly once per key via the pool's
        // single conditional removal (see stealRecycled); a zeroed adder
        // that loses the putIfAbsent race to another thread is discarded
        // (a 24-byte object, cheaper than the closure it replaces).  The
        // approximate-size increment is exactly-once: putIfAbsent's winner
        // is the one real insert.
        LongAdder candidate = stealRecycled(ebbReservoir, key);
        cell = candidate != null ? candidate : new LongAdder();
        LongAdder prev = reservoir.putIfAbsent(key, cell);
        if (prev == null) {
          approximateSize.incrementAndGet();
        } else {
          cell = prev;
        }
      }
      cell.add(delta);
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
   * routing beacon bit set and the cold direct-write table).
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
   * Second-round hash for the k=2 routing beacon (Caffeine's
   * {@code FrequencySketch.rehash}): derives the second room from the
   * SAME avalanched hash with one multiply and one xor-shift (2 ops)
   * instead of a second full avalanche ({@link #mixHash} + seed, 5 ops) —
   * the previous scheme cost a full mixHash on every hot-likely
   * membership test.  The multiply by an odd constant is a bijection and
   * the xor-shift mixes the high bits, so the derived position is
   * uniformly distributed and independent of the first — the k=2
   * false-positive math is unchanged.
   *
   * @param x the avalanched key hash ({@code mixHash(key.hashCode())})
   * @return the second independent hash for the trace room
   */
  private static int rehash(int x) {
    x *= 0x31848bab;
    x ^= x >>> 14;
    return x;
  }

  // The compact beacon packs 4-bit rooms into 64-bit longs (16 rooms per
  // long): low 2 bits = bit1 role evidence, high 2 bits = bit2 role
  // evidence.  Each role needs only 2 bits (saturating at 3): the
  // first-promotion seed of 2 plus the halving decay yields the 2-tide
  // memory.  The deliverer is the only writer, so the read-modify-write
  // helpers below need no CAS.

  /** Field offset of the bit1-role evidence within a room (low 2 bits). */
  private static final int BIT1_OFFSET = 0;

  /** Field offset of the bit2-role evidence within a room (high 2 bits). */
  private static final int BIT2_OFFSET = 2;

  /**
   * Halving-decay shift for a 2-bit role evidence.  In this
   * encoding only 1 is meaningful: a shift of 2 or more zeroes every lane
   * (values 0-3 right-shifted by >= 2), and the SWAR identity in
   * {@link #decayCounts()} — a lane's high bit moved to its low position —
   * holds exactly for shift 1, because the shifted bit must land within
   * its own 2-bit lane.  A different effective decay RATE (e.g. halve
   * every other tide) is a sweep-cadence knob, not this shift.
   */
  private static final int DECAY_SHIFT = 1;

  /**
   * SWAR decay mask: the high bit of every 2-bit field
   * (positions {@code 4r+1} / {@code 4r+3}).  A halving decay of a
   * 2-bit lane is exactly "move the high bit to the low position", so
   * {@code (word & this) >>> DECAY_SHIFT} decays the whole word
   * bit-parallel.  Tied to {@link #DECAY_SHIFT} == 1 — the mask must
   * select the bit that lands at each lane's low position after the
   * shift.
   */
  private static final long DECAY_HIGH_BITS_MASK = 0xAAAAAAAAAAAAAAAAL;

  /**
   * SWAR active mask: the count-evidence lanes of the
   * DECAYED word (positions {@code 4r}).  A room is active after decay
   * iff its original count evidence was ≥ 2 (high bit set), which lands
   * on bit {@code 4r} of {@code (word & DECAY_HIGH_BITS_MASK) >>> DECAY_SHIFT}.
   * Trace lanes (positions {@code 4r+2}) are excluded — active counts
   * rooms, not fields.
   */
  private static final long ACTIVE_LANE_MASK = 0x1111111111111111L;

  /**
   * Read the 2-bit role evidence at the given offset of the room.
   *
   * <p>A plain array load (see the field doc — best-effort visibility,
   * routing-only; never correctness).
   *
   * @param bit    the global room index (0‑based, masked by {@link #beaconMask})
   * @param offset the role field offset within the room ({@link #BIT1_OFFSET}
   *               or {@link #BIT2_OFFSET})
   * @return the evidence value (0-3)
   */
  private int roleGet(int bit, int offset) {
    int idx = bit >>> 4;
    int shift = ((bit & 15) << 2) + offset;
    return (int) ((beacon[idx] >>> shift) & 0x3);
  }

  /**
   * Add {@code delta} to the 2-bit role evidence at the given offset of
   * the room (caller ensures the result stays within 0-3).
   *
   * <p>Deliverer-only (read-modify-write, no CAS needed).
   *
   * @param bit    the global room index (0‑based)
   * @param offset the role field offset within the room ({@link #BIT1_OFFSET}
   *               or {@link #BIT2_OFFSET})
   * @param delta  the signed delta to add
   */
  private void roleAdd(int bit, int offset, int delta) {
    int idx = bit >>> 4;
    int shift = ((bit & 15) << 2) + offset;
    beacon[idx] = beacon[idx] + ((long) delta << shift);
  }

  /**
   * Whether the key is a promoted hot key (hot-path routing check).
   *
   * <p>Two independent role evidences, both required: the bit1-role
   * evidence at the first hash room AND the bit2-role evidence at the
   * second hash room, each {@code >= 1}.  A promoted key always writes
   * both (no false negatives); false positives need both a polluted
   * count room AND a polluted trace room (the k=2 point), and are
   * routing-only.
   *
   * @param h   the avalanched key hash ({@code mixHash(key.hashCode())}),
   *            computed once by the caller and shared with
   *            {@code Ceils#add}
   */
  private boolean isBeaconMember(int h) {
    int bit1 = h & beaconMask;
    if (roleGet(bit1, BIT1_OFFSET) == 0) {
      return false;
    }

    int bit2 = rehash(h) & beaconMask;
    return roleGet(bit2, BIT2_OFFSET) >= 1;
  }

  /**
   * Promote a key into the routing beacon (deliverer thread only).
   *
   * <p>The ACTIVE hot set (rooms whose bit1-role evidence transitions
   * 0 → 2) gates the promotion scan, refreshed by the tide's halving
   * sweep — a decayed-away key frees its slot, so the
   * freeze-at-hotLimit problem of the old bit-only design is gone.  The
   * deliverer is the only writer, so the read-modify-write needs no CAS.
   *
   * @param key the key to promote
   * @return {@code true} if the key became newly active (count room
   *         evidence 0 → 2)
   */
  private boolean promoteToBeacon(String key) {
    int h = mixHash(key.hashCode());
    int bit1 = h & beaconMask;
    int bit2 = rehash(h) & beaconMask;
    // When both evidences fall in the same room (1/32768 collision), the
    // two 2-bit fields are still independent — each is written to its own
    // half of the 4-bit room, so a member's own promotion always
    // satisfies both (no false negatives).
    int c = roleGet(bit1, BIT1_OFFSET);
    int t = roleGet(bit2, BIT2_OFFSET);
    // Per-field saturation guard: each role evidence is
    // refreshed independently — a room saturated (3) by OTHER keys'
    // evidence must not block this key's own field.
    if (c < 3) {
      // First promotion seeds 2 (not 1): with the per-tide halving decay,
      // a seed of 1 would leave the hot set after a single quiet tide;
      // seeding 2 gives a 2-tide memory (tolerates one cycle of counting
      // fluctuation without ping-pong).  The value is a memory mark, not
      // a literal promotion count.
      roleAdd(bit1, BIT1_OFFSET, c == 0 ? 2 : 1);
    }
    if (t < 3) {
      // Trace evidence: +1 saturating at 3 (first seeds 2, like bit1 —
      // both evidences share the 2-tide memory and expire in lockstep).
      roleAdd(bit2, BIT2_OFFSET, t == 0 ? 2 : 1);
    }

    // Newly active = the count room transitioned 0 → 2 (a fresh slot in
    // the decayCounts() budget).  The guard above always writes when
    // c == 0, so the return is exact in every branch.
    return c == 0;
  }

  /**
   * Halving decay for the role evidences, run once per tide before the
   * promotion scan: every 2-bit field {@code >> DECAY_SHIFT}, and fields
   * that decay to zero free their slot.  Both roles decay on the same
   * ladder ({@link #DECAY_SHIFT} — the per-lane halving), so a member's
   * two evidences expire in lockstep (no false negatives from one
   * outliving the other).  Also returns the active hot-set size (the
   * hotLimit gate for the promotion scan).
   *
   * <p><b>SWAR .</b>  The per-room inner loop is replaced by
   * bit-parallel decay: in a 2-bit lane the decayed value is exactly the
   * lane's high bit moved to the low position, so the whole word decays
   * with one mask-and-shift, and the post-decay active count is the
   * same shifted value's lane popcount.  See
   * {@link #DECAY_HIGH_BITS_MASK} / {@link #ACTIVE_LANE_MASK} for the
   * lane layout.
   *
   * <p>Active is counted POST-decay: a room whose count evidence was 1
   * (decaying to 0) is free for the promotion scan this tide — counting
   * pre-decay values would over-report the active set and starve the
   * freed slot (a mini freeze-at-hotLimit, contradicting the "after
   * decay" contract below).
   *
   * @return the number of rooms with a live bit1-role evidence (>= 1)
   *         after decay
   */
  private int decayCounts() {
    int active = 0;
    for (int i = 0; i < beacon.length; i++) {
      long word = beacon[i];
      if (word == 0) {
        continue;
      }

      long decayed = (word & DECAY_HIGH_BITS_MASK) >>> DECAY_SHIFT;
      active += Long.bitCount(decayed & ACTIVE_LANE_MASK);
      beacon[i] = decayed;
    }
    return active;
  }

  /**
   * Recycle a fully-consumed {@link LongAdder} for the given key from the
   * adder pool, or return {@code null} if none is available.
   *
   * <p>Single conditional-removal instead of the old get + remove(key,
   * recycled): the pool is replaced wholesale per tide and published only
   * after every adder was summed, so the entry under a key cannot change
   * between a get and a remove — {@code remove(key)} alone is exact and
   * halves the CHM lookups.
   *
   * <p>The adder is {@link LongAdder#reset() reset} on steal: the pool
   * guarantees only that the value was <em>consumed</em> by the previous
   * snapshot, not that it is zero — resetting clears the residual
   * (previous cycle's count and any late cold write from a writer preempted
   * across the quiescence window), so it is lost exactly like the documented
   * cold approximate window instead of contaminating the next cycle.
   *
   * @param pool the adder pool (previous tide's drained table), possibly null
   * @param key  the key being first-inserted
   * @return a zero-valued adder to reuse, or {@code null} to allocate
   */
  private static LongAdder stealRecycled(ConcurrentHashMap<String, LongAdder> pool, String key) {
    if (pool == null) {
      return null;
    }

    LongAdder recycled = pool.remove(key);
    if (recycled == null) {
      return null;
    }
    recycled.reset();
    return recycled;
  }

  /**
   * Bulk-add a writer's hot local map into the shared table and reset it.
   *
   * <p>The per-writer mutex lives inside {@link Ceils#drainInto} (the
   * per-map {@link ReentrantLock} — never a parameter), serializing this
   * writer's add against the deliverer's drain of the same map.  The
   * {@link #reservoirGate} mutex here makes the capture of the shared-table
   * reference atomic against the deliverer's wholesale swap, so an add
   * never writes into a table that is already being snapshotted.
   */
  private void discharge(Ceils m) {
    ConcurrentHashMap<String, LongAdder> table;
    synchronized (reservoirGate) {
      // Mutex (table reference): capture the add target atomically vs the
      // tide swap, so we never write into a table that is already being
      // snapshotted.
      table = reservoir;
      // (in-flight reservation): reserve the merge slot HERE, atomically
      // with the reference capture.  A tide that swaps after this point
      // and then waits for mergesInFlight sees the reservation (monitor
      // release-acquire ordering: the tide acquired the same gate to
      // swap), so it cannot snapshot `table` before this merge lands.
      // Without the reservation a discharge preempted between the capture
      // and its own increment could land the merge in `old` after the
      // snapshot — previously masked by the always-paid 1ms quiescence
      // window, exposed once the window became gated (a hot-racing stress
      // lost 32/160k ops with the window skipped).
      mergesInFlight.increment();
    }
    try {
      m.drainInto(table, ebbReservoir);
    } finally {
      mergesInFlight.decrement();
    }
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
    boolean quiesce;
    synchronized (reservoirGate) {
      //  (rotate-table): swap the shared table wholesale.  New hot
      // merges and cold direct writes now target the fresh table; `old`
      // becomes read-only except for writers that captured the reference
      // before the swap.
      old = reservoir;
      reservoir = new ConcurrentHashMap<>();
      approximateSize.set(0);
      // (quiescence-gate): capture and clear the cold-write flag under
      // the same mutex as the swap.  A writer that observed the OLD
      // reference and set the flag before the swap is captured here and
      // the window is paid; a writer whose flag lands after the swap
      // targets the NEW table (its computeIfAbsent re-reads the field),
      // so clearing under the gate cannot lose a write.
      quiesce = coldWriteSeen;
      coldWriteSeen = false;
    }
    //  (settle-writes): quiescence — hot merges first (exact), then
    // a window for cold writers preempted between reference capture and
    // write.  The window is paid ONLY when a cold writer may be in
    // flight (coldWriteSeen was set this cycle); on cycles with no cold
    // traffic it is skipped entirely, with the residual loss window
    // shrinking to the ns-scale get-to-flag gap (see class doc).
    // Residual loss with the window paid requires a preemption > 1ms
    // (~1e-5/op measured).  The window precedes the emptiness check so
    // a writer that recovers during it can still land in the snapshot.
    int spin = SETTLE_SPIN_ITERATIONS;
    while (mergesInFlight.get() > 0) {
      if (--spin >= 0) {
        Thread.onSpinWait();
      } else {
        Thread.yield();
      }
    }

    if (quiesce) {
      long qDeadline = System.nanoTime() + SNAPSHOT_QUIESCENCE_NANOS;
      while (System.nanoTime() < qDeadline) {
        long remaining = qDeadline - System.nanoTime();
        if (remaining > 100_000) {
          // > 100µs: yield the core
          LockSupport.parkNanos(remaining >> 1);
        } else {
          Thread.onSpinWait();
        }
      }
    }

    if (old.isEmpty()) {
      // Nothing to snapshot (idle cycle, or all traffic still resident in
      // writers' local maps): the quiescence window above was still paid —
      // an empty old table cannot be told apart from "no writer in flight"
      // (the information gap), and the parked wait costs no CPU.  Only the
      // snapshot build is skipped.
      return null;
    }
    // (snapshot-promote): the old table is now quiescent — drain it.
    // Keys are unique (CHM + computeIfAbsent), so plain put is exact and
    // skips merge's redundant per-key lookup.  Every adder's value enters
    // the snapshot; the drained table is then published as the recycling
    // pool — only after the last sum, so a steal can never observe
    // unconsumed counts (see {@link #ebbReservoir}).
    Map<String, Long> snapshot = new HashMap<>(old.size());
    old.forEach((k, v) -> snapshot.put(k, v.sum()));
    ebbReservoir = old;
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
      // A clear() is a full reset — zero the distinct-key counter too:
      // a stale size would make the capacity guard drop the FIRST
      // post-clear insert (a key that just cleared at the boundary is
      // dropped although the fresh table is empty).  A writer mid-insert
      // during clear() counted against the discarded table — its
      // increment may land after this reset and is lost, which is
      // consistent with clear() dropping everything anyway.
      approximateSize.set(0);
      // ...and drop the quiescence flag too so a pre-clear cold write
      // cannot force an unnecessary window later (an in-flight writer
      // racing clear() is ns-scale; counts dropped by clear are lost
      // anyway by definition).
      coldWriteSeen = false;
    }

    // A clear() is a full reset — drop the adder pool so no recycled
    // state survives it (steals would otherwise reuse pre-clear adders).
    ebbReservoir = null;
  }

  /**
   * Periodic delivery scheduled by {@link #afterPropertiesSet()}.
   *
   * <p><b>Protocol (5 phases):</b>
   * <ol>
   *   <li>{@code add-locals} — drain every registered writer's hot local
   *       map into the shared table: live writers via {@code tryLock}
   *       (locked maps are skipped for the cycle — their flush-clock
   *       discharge delivers within {@code flushIntervalMs}, never lost),
   *       dead writers lock-free (thread death does not release locks).</li>
   *   <li>{@code reap-dead} — remove dead writers' registry entries; their
   *       residuals were just merged, so the registry cannot leak.</li>
   *   <li>{@code rotate-table} — swap the shared table wholesale under
   *       {@link #reservoirGate}: new merges and cold writes now target the
   *       fresh table.</li>
   *   <li>{@code settle-writes} — wait for {@link #mergesInFlight} to reach
   *       zero (hot merges that captured the OLD reference have finished),
   *       then the 1ms quiescence window for cold writers preempted between
   *       reference capture and write (parked, not spun — see class doc;
   *       gated by the {@code coldWriteSeen} flag: skipped when no cold
   *       writer may be in flight).
   *       Only then is the old table checked for emptiness; the window
   *       precedes the check so a writer that recovers during it can still
   *       land in the snapshot.</li>
   *   <li>{@code snapshot-promote} — snapshot the old table into a single
   *       map, estimate the top-{@code hotLimit} boundary from a log2-bucket
   *       histogram of the cycle's counts (floor {@link #PROMOTION_FLOOR}),
   *       promote the keys at or above it to the exact hot path, and tide.</li>
   * </ol>
   *
   * <p>Phases 3-5 are delegated to {@link #tideWatcher()} (shared with
   * {@link #destroy()}).
   */
  @SuppressWarnings("all")
  private void tide() {
    long nextDelayMs = deliverIntervalMs;
    try {
      // (add-locals): every writer's hot local map enters the
      // shared table.  Per-writer mutex (ReentrantLock): a live writer
      // mid-add is tried non-blockingly — if the lock is held, this
      // writer's residual is SKIPPED for this cycle (its flush-clock
      // discharge delivers it within flushIntervalMs; never lost, measured
      // 0 loss across 10 deliver-racing stress rounds).  A dead writer is
      // drained lock-free (it can never write again), which also fixes the
      // previous design's dead-writer-holds-monitor tide stall.  The
      // table reference is captured ONCE before the loop: the tide is the
      // only thread that swaps the table, and the swap happens later in
      // this same thread (tideWatcher), so a single capture is equivalent
      // to per-writer captures.
      ConcurrentHashMap<String, LongAdder> table;
      synchronized (reservoirGate) {
        table = reservoir;
      }
      for (Map.Entry<Thread, Ceils> entry : hotRegistry.entrySet()) {
        if (entry.getValue().size == 0) {
          // Empty local map: nothing to merge — skip the drain (spares the
          // mergesInFlight RMWs and the 256-slot scan).  Dead writers are
          // still reaped here so the registry cannot leak.  A writer that
          // raced an add in just before this read keeps its data in the
          // local map and merges it on its own batch trigger or the next
          // tide — delayed, never lost (the flush clock in add() bounds
          // the staleness).
          if (!entry.getKey().isAlive()) {
            hotRegistry.remove(entry.getKey(), entry.getValue());
          }
          continue;
        }

        Thread writer = entry.getKey();
        Ceils local = entry.getValue();

        if (!writer.isAlive()) {
          // Dead writer: quiescent map, no lock needed.  Reap after.
          local.drainDead(table, ebbReservoir);
          hotRegistry.remove(writer, local);
          continue;
        }

        if (!local.tryDrainInto(table, mergesInFlight, ebbReservoir)) {
          // Writer holds the map mid-add: skip this cycle — its own
          // flush-clock discharge (≤ flushIntervalMs) moves the data into
          // the shared table, so the next tide's snapshot includes it.
          continue;
        }
        // (reap-dead): reclaim dead writers' entries — their
        // residuals were just merged, so the registry cannot leak.
        if (!writer.isAlive()) {
          hotRegistry.remove(writer, local);
        }
      }

      Map<String, Long> snapshot = tideWatcher();
      if (snapshot != null) {
        // Adaptive cadence: the raw backlog is folded into a
        // smoothed reference — a burst beyond the still band attacks
        // instantly (unchanged burst latency), quiet tides release at the
        // EMA rate (no 50<->500ms ping-pong), in-band jitter moves nothing.
        nextDelayMs = computeNextTideDelayMs(pacer.pressure(snapshot.size()));
        // (snapshot-promote): estimate the boundary of the top hotLimit
        // keys from a log2-bucket histogram of the cycle's counts.  The
        // halving decay runs FIRST (frees decayed slots, refreshes the
        // active hot-set size — a drifted-away key leaves within 2 tides),
        // then the promotion scan promotes every key at or above the
        // boundary (count +1, first promotion seeds 2 for a 2-tide
        // memory).  A snapshot below {@link #MIN_PROMOTION_KEYS} distinct
        // keys is skipped: its distribution is meaningless and promoting
        // everything would burn beacon slots on startup noise.
        long threshold = moonsTidalForce.floor();
        int promoted = decayCounts();
        if (promoted < hotLimit && snapshot.size() >= MIN_PROMOTION_KEYS) {
          // One pass over the snapshot: the log2-bucket histogram AND the
          // promotion candidates (keys at or above the floor — the promote
          // condition v >= max(floor, boundary) always implies v >= floor,
          // so the candidate list is an exact filter for the second pass).
          // The second pass then iterates the candidate list instead of the
          // full snapshot; only the boundary-below-floor case (noise
          // traffic) still needs a full sweep for the blocked-keys signal.
          Arrays.fill(histogram, 0);
          List<Map.Entry<String, Long>> candidates = new ArrayList<>(Math.min(snapshot.size(), 1024));
          for (Map.Entry<String, Long> e : snapshot.entrySet()) {
            long v = e.getValue();
            if (v <= 0) {
              continue;
            }

            int bucket = (64 - Long.numberOfLeadingZeros(v)) - 1;
            if (bucket >= HISTOGRAM_BUCKETS) {
              bucket = HISTOGRAM_BUCKETS - 1;
            }

            histogram[bucket]++;
            if (v >= threshold) {
              candidates.add(e);
            }
          }

          long accumulated = 0;
          long boundary = 1; // the pure histogram boundary, before the floor
          for (int i = HISTOGRAM_BUCKETS - 1; i >= 0 && accumulated < hotLimit; i--) {
            if (histogram[i] > 0) {
              accumulated += histogram[i];
              // Clamp the shift at 62: a bucket-63 count (>= 2^63 per cycle)
              // would shift the boundary negative and make the promote test
              // admit the whole snapshot — unreachable in practice, kept
              // sane anyway.
              boundary = 1L << Math.min(i, HISTOGRAM_BUCKETS - 2);
              threshold = Math.max(moonsTidalForce.floor(), boundary);
            }
          }

          // Promotion scan.  Also gathers the governor's signals: memberKeys
          // (slots whose key earned >= threshold this tide — the renewal
          // rate) and blockedKeys (keys the floor excludes from the
          // histogram boundary — the only case where raising the floor can
          // help).  Counting memberKeys POST-promotion avoids the startup
          // artifact of a fresh beacon reading zero renewal on the very
          // tide that promotes everyone.
          int memberKeys = 0;
          int blockedKeys = 0;
          if (boundary < threshold) {
            // The blocked band [boundary, threshold) is non-empty only when
            // the floor sits above the histogram boundary (noise traffic):
            // those keys are below the floor, so they are not in the
            // candidate list — sweep the snapshot for the governor signal.
            // (A full count is fine here: it only matters when the band is
            // non-empty, and the scan breaks early only in the saturated
            // state where the governor ignores blockedKeys.)
            for (Map.Entry<String, Long> e : snapshot.entrySet()) {
              long v = e.getValue();
              if (v >= boundary && v < threshold) {
                blockedKeys++;
              }
            }
          }
          for (Map.Entry<String, Long> e : candidates) {
            long v = e.getValue();
            if (v >= threshold) {
              // Every key at the threshold is a member after this call
              // (re-promotions included) — the renewal numerator.
              memberKeys++;
              if (promoteToBeacon(e.getKey()) && ++promoted >= hotLimit) {
                break;
              }
            }
          }
          moonsTidalForce.onTide(
            memberKeys / (double) Math.max(1L, promoted),
            promoted,
            hotLimit,
            blockedKeys,
            boundary
          );
        }
        batchConsumer.accept(snapshot);
      } else {
        // Empty tide: the ladder stretches the cadence (idle power saving)
        // up to {@link #EMPTY_TIDE_STRETCH_CAP_MULTIPLE} x the base.
        nextDelayMs = pacer.emptyDelay(deliverIntervalMs);
      }
    } catch (Exception e) {
      log.error("Scheduled delivery failed", e);
    }
    // Self-rescheduling chain (one-shot schedule → tide → re-schedule).
    // Guarded by deliveryStarted so reflection-driven tides in tests never
    // arm a background chain, and by shutdown so destroy() stops it.
    if (deliveryStarted && !shutdown) {
      scheduleNextTide(nextDelayMs);
    }
  }

  /**
   * Adaptive delay until the next tide, scaled by the backlog delivered this
   * cycle.
   *
   * <p>Borrows the JVM monitor feedback idea: delivery frequency adapts to
   * pressure instead of paying a fixed cadence. A burst (many distinct keys)
   * shortens the cycle toward {@link #EARLY_TIDE_MIN_INTERVAL_MS}, bounding
   * detection latency and keeping the reporter's reservoir away from its
   * capacity cap; idle cycles stay at {@code deliverIntervalMs}. The loop is
   * self-damping: faster delivery drains the reservoir, which shrinks the
   * backlog signal and lengthens the cycle again.
   *
   * @param deliveredKeys distinct keys delivered by the just-completed tide
   * @return the delay in milliseconds, in
   *         [{@link #EARLY_TIDE_MIN_INTERVAL_MS}, {@code deliverIntervalMs}]
   */
  long computeNextTideDelayMs(int deliveredKeys) {
    if (deliveredKeys <= 0) {
      return deliverIntervalMs;
    }

    long pressure = Math.min(deliveredKeys, EARLY_TIDE_THRESHOLD_KEYS);
    long delay =
      deliverIntervalMs - ((deliverIntervalMs - EARLY_TIDE_MIN_INTERVAL_MS) * pressure) / EARLY_TIDE_THRESHOLD_KEYS;
    return Math.max(delay, EARLY_TIDE_MIN_INTERVAL_MS);
  }

  /**
   * Arm the next tide as a one-shot task. Safe to call after
   * {@link #destroy()} — the scheduler rejects and the exception is logged.
   *
   * @param delayMs delay until the next tide
   */
  private void scheduleNextTide(long delayMs) {
    try {
      scheduler.schedule(this::tide, delayMs, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      log.error("Failed to schedule WaveCounter delivery; buffered counts will be delayed.", e);
    }
  }

  @Override
  public void afterPropertiesSet() {
    deliveryStarted = true;
    try {
      scheduler.schedule(this::tide, deliverIntervalMs, TimeUnit.MILLISECONDS);
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
   * <p>Sets {@code shutdown} first (new {@code count()} calls no-op), merges
   * every registered writer's hot local map (mutex-serialized against any
   * in-flight add via the per-map lock), then performs the same
   * swap-and-snapshot as {@link #tide()}.
   *
   * <p><b>Approximate final add.</b>  The registry sweep
   * cannot enumerate a writer that is racing its FIRST hot registration
   * (or was preempted between registration and its first add), so at
   * most a few residual counts from such a writer can be left undelivered
   * — the information gap that a per-op in-flight counter (removed for
   * hot-path throughput, measured ~15ns/op) used to close.  The window
   * is ns-scale, destroy is a shutdown path, and every REGISTERED
   * writer's data stays exact (the per-map lock waits out its in-flight
   * add); accepted as the documented approximation.
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
    // Merge all hot local maps (mutex-serialized), then rotate-and-snapshot
    // exactly like tide() — including the cold-write quiescence window.
    for (Map.Entry<Thread, Ceils> entry : hotRegistry.entrySet()) {
      discharge(entry.getValue());
    }
    Map<String, Long> snapshot = tideWatcher();
    if (snapshot != null) {
      batchConsumer.accept(snapshot);
    }
    // Shut down: drop the adder pool (no one can steal after shutdown, and
    // the reference would otherwise pin the last drained table in memory).
    ebbReservoir = null;
  }

  /**
   * Writer-private open-addressing map for hot keys.  Accessed only by its
   * owning thread, except under {@link #lock} by the deliverer when merging
   * the writer's residual data.
   *
   * <p>The capacity is fixed at {@link #LOCAL_CAPACITY}; the batch trigger
   * ({@code opMaxCount}) is at most half of it, so probing never runs out of
   * empty slots.
   */
  @SuppressWarnings("all")
  private static final class Ceils {

    final String[] keys = new String[LOCAL_CAPACITY];
    final long[] counts = new long[LOCAL_CAPACITY];
    final int[] tags = new int[LOCAL_CAPACITY];
    /**
     * Occupancy bitmap: bit {@code i} set = slot {@code i}
     * claimed.  The bit is set BEFORE the slot writes in {@link #add}, so
     * the drains visit only the claimed slots (<= opMaxCount of
     * LOCAL_CAPACITY) instead of scanning the full table, and a slot whose
     * writer died mid-claim stays marked (counts == 0) for the drain to
     * reclaim.
     */
    final long[] occupied = new long[LOCAL_CAPACITY >>> 6]; // 4 longs
    long lastFlushNanos = System.nanoTime();
    int opCount;
    int size;

    /**
     * Per-map mutex.  A {@link ReentrantLock} instead of the monitor so the
     * deliverer can {@link ReentrantLock#tryLock()}: a busy writer never
     * blocks the tide — its residual is skipped for the cycle and delivered
     * by the writer's own flush-clock discharge (bounded by
     * {@code flushIntervalMs}), or by {@link #drainDead} once the writer is
     * observed dead.  Reentrant for the writer's own batch discharge.
     * Writer-side cost is within run-to-run noise of the monitor
     * (measured on the copied prototype).
     */
    final ReentrantLock lock = new ReentrantLock();

    /**
     * Merge one increment into the local map.
     *
     * <p>{@link #lock}: the deliverer may add and reset this map
     * concurrently; without the mutex a half-written entry could be reset,
     * stranding its count.  Uncontended in the common case (only the owner
     * writer and the periodic deliverer contend).
     *
     * <p>The slot index derives from the avalanched hash {@code h}, computed
     * once by {@link WaveCounter#count(String, long)} and shared with the
     * beacon routing check — the tag is the full avalanched hash (0 mapped
     * away, since 0 marks an empty slot).
     *
     * @param key   the accessed key
     * @param delta the number of accesses
     * @param h     the avalanched key hash ({@code mixHash(key.hashCode())})
     */
    public void add(String key, long delta, int h) {
      lock.lock();
      try {
        // Open addressing with linear probing: `tag` is the key's hash
        // (0 mapped away, since 0 marks an empty slot); the probe starts at
        // the hashed slot and walks forward, wrapping via the power-of-two
        // mask.  At most opMaxCount entries are live, so a free slot is
        // always reached within the 256-slot table.
        int tag = h == 0 ? Integer.MIN_VALUE : h;
        int i = h & (LOCAL_CAPACITY - 1);
        for (;;) {
          if (tags[i] == 0) {
            // empty slot: claim it (this thread owns the map, so no CAS),
            // store the key and the initial count, done
            long shift = 1L << (i & 63);
            occupied[i >>> 6] |= shift; // mark BEFORE the writes:
            tags[i] = tag; // a death mid-claim leaves a marked slot whose
            keys[i] = key; // counts == 0 the drain filters, and reclaims
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
      } finally {
        lock.unlock();
      }
    }

    /**
     * Drain every non-zero entry into the shared table and reset this map.
     *
     * <p>{@link #lock} (reentrant): serializes the owner writer's add
     * against the deliverer's drain of the same map.  A concurrent reset
     * would otherwise strand half-written entries.  Uncontended in the
     * common case (only the owner writer and the periodic deliverer
     * contend).  Blocking — used by the writer's own batch discharge and by
     * {@code destroy()}, both of which must wait out an in-flight add.
     *
     * <p>The {@code mergesInFlight} reservation is NOT bumped here: the
     * caller ({@link WaveCounter#discharge(Ceils)}) reserves the slot
     * atomically with its table-reference capture, closing the
     * capture-to-bump preemption window that the tide's settle-wait
     * cannot see .
     *
     * @param table the shared table to drain into (reference already
     *              captured atomically vs the tide swap, and reserved in
     *              mergesInFlight by the caller)
     * @param ebb   the adder-recycling pool (previous tide's drained
     *              table, possibly null) — hot keys steal from it
     *              instead of allocating
     */
    public void drainInto(ConcurrentHashMap<String, LongAdder> table, ConcurrentHashMap<String, LongAdder> ebb) {
      lock.lock();
      try {
        waveTo(table, ebb);

        size = 0;
        lastFlushNanos = System.nanoTime();
      } finally {
        lock.unlock();
      }
    }

    /**
     * Non-blocking drain for the periodic tide: {@link ReentrantLock#tryLock()}
     * — if the writer holds the map mid-add, the tide skips this writer's
     * residual for this cycle instead of blocking.  Skipped data is not
     * lost: the writer's own flush-clock discharge (every add re-checks
     * {@code lastFlushNanos}) moves it into the shared table within
     * {@code flushIntervalMs}, so a later tide's snapshot picks it up.
     *
     * @param table          the shared table to drain into
     * @param mergesInFlight the in-flight counter to bump during the drain
     * @param ebb            the adder-recycling pool (previous tide's drained
     *                       table, possibly null)
     * @return {@code true} if the drain was performed, {@code false} if the
     *         writer held the lock and the tide skipped the map
     */
    public boolean tryDrainInto(
      ConcurrentHashMap<String, LongAdder> table,
      PaddedMergesInFlight mergesInFlight,
      ConcurrentHashMap<String, LongAdder> ebb
    ) {
      if (!lock.tryLock()) {
        return false;
      }

      try {
        mergesInFlight.increment();
        try {
          waveTo(table, ebb);
        } finally {
          mergesInFlight.decrement();
        }
      } finally {
        lock.unlock();
      }
      return true;
    }

    /**
     * Lock-free drain for a DEAD writer (called only after
     * {@link Thread#isAlive()} returned {@code false}): a dead thread can
     * never write again, so the map is quiescent — no lock, no
     * {@code mergesInFlight} bump (nothing is in flight).  A writer that
     * died mid-add leaves at most one half-written slot, which the
     * {@code counts[i] != 0} filter skips (and which the sweep now
     * reclaims).  Without this, a writer that died while holding the lock
     * would stall every tide forever (Java locks are not released on
     * thread death).
     *
     * @param table the shared table to drain into
     * @param ebb   the adder-recycling pool (previous tide's drained table,
     *              possibly null)
     */
    public void drainDead(ConcurrentHashMap<String, LongAdder> table, ConcurrentHashMap<String, LongAdder> ebb) {
      waveTo(table, ebb);
      size = 0;
      lastFlushNanos = System.nanoTime();
    }

    /**
     * Drains all claimed local slots into the shared
     * {@code ConcurrentHashMap}, clearing each slot in-place so that the
     * same sweep both merges counts and reclaims the slot for future use.
     *
     * <p><b>Bitmap-driven sweep .</b>  Only the claimed slots
     * (<= opMaxCount of LOCAL_CAPACITY) are visited, instead of a full
     * 256-slot scan — the per-batch drain cost drops ~4x.  A slot whose
     * writer died mid-claim has its bit set (bits are set BEFORE the slot
     * writes in {@link #add}) but {@code counts[i] == 0}: the filter skips
     * the merge while the slot is still reclaimed here — the old full scan
     * never reset half-written slots, leaving a permanent probe-chain
     * hazard.
     *
     * <p>Each merged key recycles its adder from the pool (the previous
     * tide's drained table, already fully summed before publication) via
     * {@link WaveCounter#stealRecycled} instead of allocating — the
     * hot-path drain now shares the cold path's zero-allocation property
     * for stable key universes.
     *
     * @param table the shared accumulation table (non-null)
     * @param ebb   the adder-recycling pool (previous tide's drained table,
     *              possibly null)
     */
    private void waveTo(ConcurrentHashMap<String, LongAdder> table, ConcurrentHashMap<String, LongAdder> ebb) {
      for (int w = 0; w < occupied.length; w++) {
        long bits = occupied[w];
        while (bits != 0) {
          int j = Long.numberOfTrailingZeros(bits);
          int i = (w << 6) + j;
          if (counts[i] != 0) {
            // Two-phase steal + putIfAbsent (same shape as the cold path in
            // count()): waveTo targets the FRESH table (post-swap), so these
            // are mostly first-inserts — the steal usually wins and no
            // mapping closure is allocated per key per drain.
            LongAdder candidate = stealRecycled(ebb, keys[i]);
            LongAdder cell = candidate != null ? candidate : new LongAdder();
            LongAdder prev = table.putIfAbsent(keys[i], cell);
            cell = prev != null ? prev : cell;
            cell.add(counts[i]);
          }
          // Clear the slot inline: the entry's value has been moved into
          // the shared table (or the slot was half-written), so the slot
          // is reclaimed on the same pass — no second sweep.  The callers
          // re-arm the flush clock (lastFlushNanos = now) after the sweep.
          tags[i] = 0;
          keys[i] = null;
          counts[i] = 0;
          bits &= bits - 1;
        }
      }
      // Every set bit was visited above — wholesale clear.
      Arrays.fill(occupied, 0L);
    }

    /**
     * Reset every slot (used by {@code clear()} — a full drop without any
     * move; the drain paths reclaim slots inline instead of calling this,
     * so the map's clear semantics live in one sweep here and inline in
     * the drain sweep).
     *
     * <p>The per-map lock is taken here too: {@code clear()} can race a
     * writer's in-flight claim.  A lock-free reset that clears the
     * occupied bitmap between the writer's bit-set and its slot writes
     * would strand the entry — data in a slot with no claim bit is never
     * swept by {@link #waveTo} (which visits only claimed slots) and is
     * silently dropped by the next {@code clear()}.  Blocking here is
     * fine: {@code clear()} is off the count hot path.
     */
    void reset() {
      lock.lock();
      try {
        for (int i = 0; i < LOCAL_CAPACITY; i++) {
          tags[i] = 0;
          keys[i] = null;
          counts[i] = 0;
        }
        Arrays.fill(occupied, 0L); // drop stale claim bits
        size = 0;
        lastFlushNanos = System.nanoTime();
      } finally {
        lock.unlock();
      }
    }
  }

  /**
   * Pacing state for the adaptive tide cadence (WindowClimber
   * borrowings): an EWMA-smoothed backlog reference with a fast-attack /
   * slow-release asymmetry and a still band, plus the empty-tide stretch
   * ladder.  Written only by the deliverer thread — no synchronization.
   *
   * <p><b>Attack vs release.</b>  A backlog above the reference by more
   * than the still band folds in FULLY at once ({@link #pressure(int)}),
   * so a burst shortens the cadence to {@link #EARLY_TIDE_MIN_INTERVAL_MS}
   * on the very next tide — burst detection latency is unchanged from the
   * raw law.  A backlog BELOW the reference folds at the EMA rate
   * ({@link #PACER_RELEASE_RATE}), so the fast cadence drains the
   * reservoir for several tides instead of snapping back to the base the
   * moment one quiet tide arrives (the raw law's 50<->500ms ping-pong).
   *
   * <p><b>Still band.</b>  Moves within
   * {@link #PACER_STILL_BAND_KEYS} of the reference are noise: the
   * reference does not move (deadband), so a single jittered tide cannot
   * toggle the ramp.  Movement is decayed, never reset — a burst that
   * leaves no trace cannot suppress the next one.
   *
   * <p><b>Empty-tide ladder.</b>  A tide that delivered nothing is not a
   * workload signal (an empty old table cannot be told apart from "no
   * writer in flight"), so it never touches the reference; instead each
   * consecutive empty tide stretches the next cadence by a power of two,
   * capped at {@link #EMPTY_TIDE_STRETCH_CAP_MULTIPLE} x the base — idle
   * cycles stop paying the 1ms quiescence, the decay sweep and the
   * scheduler wakeup at the base rate.  Any non-empty tide resets the
   * streak (the ladder's confirm).
   */
  private static final class TidePacer {

    private double smoothed = -1;
    private int emptyStreak;

    /**
     * Fold this tide's raw backlog into the smoothed reference and return
     * the pressure value for the ramp.  Resets the empty-tide ladder.
     *
     * @param deliveredKeys distinct keys delivered by this tide
     * @return the smoothed backlog (fast attack, slow release, still band)
     */
    int pressure(int deliveredKeys) {
      emptyStreak = 0;
      if (smoothed < 0) {
        smoothed = deliveredKeys;
      } else if (deliveredKeys > smoothed + PACER_STILL_BAND_KEYS) {
        smoothed = deliveredKeys;
      } else if (smoothed > deliveredKeys + PACER_STILL_BAND_KEYS) {
        smoothed += PACER_RELEASE_RATE * (deliveredKeys - smoothed);
      }
      return (int) smoothed;
    }

    /**
     * Stretched delay after an empty tide: the ladder doubles per
     * consecutive empty tide, capped at
     * {@link #EMPTY_TIDE_STRETCH_CAP_MULTIPLE} x the base.
     *
     * @param baseMs the base delivery interval
     * @return the delay until the next tide
     */
    long emptyDelay(long baseMs) {
      emptyStreak++;
      long stretched = baseMs << Math.min(emptyStreak - 1, EMPTY_TIDE_STRETCH_MAX_SHIFT);
      return Math.min(stretched, EMPTY_TIDE_STRETCH_CAP_MULTIPLE * baseMs);
    }
  }

  /**
   * Adaptive promotion floor (WindowClimber borrowings): the
   * floor (the minimum per-cycle count for promotion, seeded at
   * {@link #PROMOTION_FLOOR}) is a noise filter, and the only signal it
   * can act on is the health of the hot set it admits.
   *
   * <p><b>Renewal.</b>  Each promoted tide the governor measures
   * {@code renewal} = active hot slots whose key earned at least the
   * threshold this tide, divided by the active slots.  A set below
   * {@link #GOVERNOR_RENEWAL_TARGET} (0.5) is distressed.
   *
   * <p><b>Admit on block.</b>  Distress with keys measurably blocked
   * behind the floor ({@code blockedKeys} — keys at the histogram
   * boundary but below the floor) means the floor is excluding exactly
   * the keys that would renew the hot set: the boundary admits them, the
   * floor excludes them.  The governor DROPS the floor to the boundary in
   * one move, admitting the blocked keys so renewal recovers next tide.
   * The former raise-on-blocked direction was procyclical: it excluded
   * more renewing keys, renewal stayed low, and the floor ratcheted past
   * the whole distribution — a rotating-hot-set workload ran the floor up
   * while the genuine hot keys were stranded and the hot set emptied, and
   * a churn workload oscillated the floor forever.
   *
   * <p><b>Idle collapse.</b>  A tide that leaves the hot set completely
   * empty ({@code activeSlots == 0}) proves the floor exceeds the whole
   * distribution — nothing can qualify, so filtering is meaningless.  The
   * floor collapses to the seed immediately instead of walking back
   * through the slow audit ({@link #GOVERNOR_AUDIT_WAIT}), which a
   * fast-rotating workload can outrun.
   *
   * <p><b>Retreat.</b>  Kept as defense: if distress survives
   * {@link #GOVERNOR_VETO_STREAK} tides of moving and the floor is still
   * above where the distress started, the floor returns to
   * {@code probeBase} in {@link #GOVERNOR_RETURN_BUDGET} budgeted strides.
   *
   * <p><b>Release.</b>  A raised floor is a ratchet: it comes back down
   * under saturation (the hot set at
   * {@link #GOVERNOR_SATURATION_FRACTION} of capacity while healthy — the
   * floor admits more) or under the audit (health sustained for
   * {@link #GOVERNOR_AUDIT_WAIT} still tides steps the floor one probe
   * down).  The floor is clamped to
   * [{@link #PROMOTION_FLOOR}, {@link #GOVERNOR_FLOOR_MAX}].
   */
  private static final class MoonsTidalForce {

    private int floor = (int) PROMOTION_FLOOR;
    private double step = GOVERNOR_STEP_INITIAL;
    private int distressAge;
    private int auditAge;
    private int probeBase;
    private int returnLeft;
    private int returnTarget;
    private boolean returning;

    int floor() {
      return floor;
    }

    /**
     * Advance the governor by one promoted tide.
     *
     * @param renewal     share of active hot slots whose key earned traffic
     * @param activeSlots active hot slots after this tide's promotion scan
     * @param hotLimit    the hot-set capacity (saturation signal)
     * @param blockedKeys keys the floor currently excludes from the
     *                    histogram boundary (the case where the floor is
     *                    over-filtering — admitted by dropping the floor)
     * @param boundary    the pure histogram boundary (top-{@code hotLimit}
     *                    count level, before the floor); the drop target
     *                    when keys are blocked
     */
    @SuppressWarnings("all")
    public void onTide(double renewal, int activeSlots, int hotLimit, int blockedKeys, long boundary) {
      if (activeSlots == 0 && floor > PROMOTION_FLOOR) {
        // The hot set is empty — the floor exceeds the whole distribution
        // and filters nothing.  Reset to the seed immediately; the slow
        // audit can be outrun by a fast-rotating workload.
        floor = (int) PROMOTION_FLOOR;
      }

      if (returning) {
        int stride = Math.abs(floor - returnTarget);
        if (stride <= 1 || returnLeft <= 0) {
          floor = returnTarget;
          returning = false;
        } else {
          floor += (int) Math.signum(returnTarget - floor) * Math.min(GOVERNOR_RETURN_STEP, stride / 2 + 1);
          returnLeft--;
        }
        return;
      }

      if (renewal < GOVERNOR_RENEWAL_TARGET) {
        auditAge = 0;
        if (distressAge == 0) {
          probeBase = floor;
        }

        distressAge++;
        if (distressAge > GOVERNOR_VETO_STREAK && floor > probeBase) {
          // The move failed to recover the signal — retreat to where it
          // started (floor > probeBase keeps an unmoved floor from
          // retreating into itself forever).
          returnTarget = probeBase;
          returnLeft = GOVERNOR_RETURN_BUDGET;
          returning = true;
          distressAge = 0;
        } else if (blockedKeys > 0) {
          // Admit on block: blocked keys are exactly the renewing keys
          // the boundary would admit — excluding them starves the hot
          // set.  Drop the floor to the boundary so they qualify from the
          // next tide on.  Clamped: a boundary above GOVERNOR_FLOOR_MAX
          // keeps the floor inside its documented range (inert — the
          // threshold is max(floor, boundary) anyway).
          floor = (int) Math.min(Math.max(PROMOTION_FLOOR, boundary), GOVERNOR_FLOOR_MAX);
        }
      } else {
        distressAge = 0;
        boolean saturated = activeSlots >= GOVERNOR_SATURATION_FRACTION * hotLimit;
        if ((saturated || auditAge >= GOVERNOR_AUDIT_WAIT) && floor > PROMOTION_FLOOR) {
          floor = Math.max((int) PROMOTION_FLOOR, floor - (int) step);
          step *= GOVERNOR_STEP_DECAY;
          auditAge = 0;
        } else {
          auditAge++;
          step = GOVERNOR_STEP_INITIAL;
        }
      }
    }
  }

  /**
   * 120-byte leading pad to isolate the first hot field from object header and other instance fields.
   *
   * <p>Backed by a {@link LongAdder} (not an {@code AtomicInteger}): every
   * writer's {@code drainInto} bump lands in its own striping cell, so batch
   * merges from many writers never contend on a single cache line.  The
   * deliverer's {@code get()} reads a stable {@code sum()} — exact here
   * because the adder is never reset (cells are never migrated), and the
   * settle-wait only needs "reached zero" semantics.
   */
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

    long get() {
      return value.sum();
    }

    void increment() {
      value.increment();
    }

    void decrement() {
      value.decrement();
    }
  }
}
