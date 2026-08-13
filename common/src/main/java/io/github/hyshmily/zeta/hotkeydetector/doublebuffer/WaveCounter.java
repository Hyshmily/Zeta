/*
 * Copyright 2026 Hyshmily. All Rights Reserved.
 *
 * Portions of this file are derived from Caffeine
 * (https://github.com/ben-manes/caffeine), Copyright Ben Manes, licensed
 * under the Apache License, Version 2.0.
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
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.ZetaThreadFactory;
import io.github.hyshmily.zeta.util.executor.SafeScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;

import javax.security.auth.Destroyable;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

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
 *       the shared table.  Fixed per-op costs: the routing beacon read,
 *       the lock-free fast add (a volatile {@link Ceils#drainStamp} read
 *       plus a post-check — see the lock rationale below, ADR-0043) and
 *       an atomic per-slot count add ({@code AtomicLongArray.getAndAdd},
 *       the price of the exact take-and-merge protocol).  The per-map
 *       {@link ReentrantLock} is paid only when a drain is actually in
 *       flight (µs-scale, at most once per writer per tide).</li>
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
 * the counting beacon decays every promoted tide (snapshots below
 * {@link #MIN_PROMOTION_KEYS} distinct keys and empty tides skip the
 * decay sweep entirely — see ADR-0038), so a key that keeps earning the
 * boundary stays hot while a drifted-away key leaves within 2 non-empty
 * tides — the hot set follows drifting heat instead of freezing.
 * Promotion gates only performance, never correctness.
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
 *       at ANY traffic volume (a fixed absolute threshold cannot — a
 *       low-volume cycle's top key measured 0 promotions at 80 counts).
 *       {@link #PROMOTION_FLOOR} (10) keeps noise keys (1-9
 *       counts/cycle) out of the hot set.  A snapshot below
 *       {@link #MIN_PROMOTION_KEYS} (16) distinct keys skips promotion
 *       entirely (Caffeine's min-signal discipline — the decay reclaims
 *       any slots anyway, so this guards against wasted promotion work,
 *       not permanent pollution).  The boundary is refined by density
 *       when the boundary bucket overflows the remaining slots: a linear
 *       64-sub-bucket histogram over the bucket's count range
 *       ({@link #subHistogram}) lands the threshold at the actual ~k-th
 *       largest count instead of the bucket's power-of-two floor
 *       (single-count resolution below 64), so keys below the refined
 *       boundary are excluded instead of being admitted in HashMap
 *       iteration order.  When the refined boundary still cuts a tie-band
 *       wider than the remaining slots, promotion is incumbent-first:
 *       renewing members are re-promoted before any newcomer (the
 *       pre-decay beacon state — the only last-tide membership memory —
 *       is captured in {@link #incumbentIdx} before the halving decay
 *       zeroes it), so the capacity break never evicts a renewing key and
 *       the hot set stays stable under flat distributions where every
 *       promotion would otherwise be a coin flip.  Both refinements are
 *       routing-only and cost zero per-op; the deliverer pays at most one
 *       extra O(n) pass per tide — the sub-histogram and the
 *       incumbent/newcomer split are one merged sweep, and the fill pass
 *       drops the membership re-test (the split is decided once,
 *       pre-decay) — only on overflowing tides.</li>
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
 *   <li><b>Promotion floor, renew-disambiguated governor</b> — the
 *       absolute floor (10) is the seed of a {@link MoonsTidalForce}
 *       (ADR-0045): the floor never filters above the histogram
 *       boundary.  Healthy tides with keys blocked behind the floor drop
 *       it toward the boundary so the renewing keys qualify; distress
 *       where the occupied hot slots earn less per slot than cold keys
 *       earn per key (the density-ratio evidence) arms a bounded
 *       raise-walk that filters the stale tail; an empty hot set
 *       collapses the floor to the seed.  The raise runs as an
 *       evidence-based, reversible probe (goal verdict, budgeted undo,
 *       retry backoff): an unconditional step would ratchet the floor
 *       past the whole distribution on rotating hot sets and oscillate
 *       forever under key churn.  Veto-return, audit clock and
 *       saturation release are kept as defensive machinery; the
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
 *       approximate window (see the Correctness model).  Paid when the
 *       shared table received any write this cycle: the
 *       {@code coldWriteSeen} flag is set by every insert path — cold
 *       first-inserts and every merge (read-gated, so only the first
 *       mark of a cycle pays) — and captured and cleared at the swap.
 *       Entries inserted by ANY path (including hot-path drains) are
 *       visible to cold hit-writers, whose adds need the same window,
 *       so only a cycle with NO shared-table writes skips the window
 *       entirely — and its old table is empty anyway, nothing to lose.
 *       A cycle with table writes pays the same parked wait.  The wait
 *       parks instead of spinning, so the cost is a yielded core for
 *       most of the window, not CPU burn.</li>
 * </ul>
 *
 * <p><b>Correctness model:</b>
 * <ul>
 *   <li><b>Hot path is exact.</b>  The per-slot counts are atomic
 *       ({@code AtomicLongArray}): the writer's update is a
 *       {@code getAndAdd} and every drain (tide sweep, batch discharge,
 *       tag-driven reconcile) takes the whole value with
 *       {@code getAndSet(0)} — the two are serialized per slot, so a slot
 *       is never torn and a "taken" slot reads exactly 0.  A racing
 *       update that lands on a taken slot observes the 0 return and
 *       recovers its delta exactly via {@code Ceils#recoverZero} (the
 *       prior value is provably already in the shared table), so the hot
 *       path is never lost and never double-counted — the residual is
 *       always precisely the un-merged delta.  The shared-table reference
 *       is captured under {@link #reservoirGate} and the in-flight slot
 *       is reserved atomically with the capture, so the deliverer waiting
 *       for {@link #mergesInFlight} to reach zero can never snapshot a
 *       table that a captured merge still targets — no in-flight hot add
 *       can be stranded.  (A non-atomic read-modify-write loses exactly
 *       one delta when a take lands between the read and the write-back
 *       — ~3e-6/op on the tryLockSkip stress; the atomic protocol
 *       closes it, see ADR-0043.)</li>
 *   <li><b>Cold path is approximate.</b>  A cold writer that captured the
 *       table reference just before the tide swap may write into the old
 *       table after the snapshot.  The tide/destroy quiescence window
 *       (1ms) reduces this to a preemption of &gt; 1ms — measured on
 *       deliver-racing and slow-consumer stress: typical loss 0, worst
 *       observed ≈2.1e-5/op.  The window is gated by the
 *       {@code coldWriteSeen} flag: every insert into the shared table
 *       marks it — cold first-inserts, and every merge (the
 *       writer-side drain paths mark it inside their
 *       {@link #reservoirGate} capture, so a swap racing an in-flight
 *       merge still captures it) — so the window is skipped only on
 *       cycles with NO shared-table writes, whose old table is empty
 *       anyway.  Residual loss with the window paid requires a
 *       preemption &gt; 1ms; a miss-path writer preempted across the swap
 *       re-targets the NEW table via its {@code computeIfAbsent}
 *       re-read.  Sustained hot keys are promoted and then take the
 *       exact path.</li>
 * </ul>
 *
 * <p><b>Delivery:</b> a self-rescheduling flusher (a one-shot tide that
 * re-arms itself at a backlog-adaptive delay) merges every writer's hot
 * local map, swaps the shared table, waits for in-flight merges plus the
 * gated quiescence window, snapshots the old table into a single map and
 * delivers it once per cycle.  Dead writers' registry entries are removed;
 * their residual local counts are merged first.  An external pressure
 * signal can request an earlier tide via {@link #nudgeTide()} —
 * earliest-first with tolerance coalescing, so multiple nudges merge
 * into at most one schedule (see {@link #tideScheduleGate}).
 *
 * <p><b>Memory:</b> the shared table holds exactly the live key set (no
 * per-batch duplication); each writer's hot local map is bounded by
 * {@code opMaxCount} and the hot set by {@code hotLimit}.  Cold- and
 * hot-path {@link LongAdder}s are recycled across cycles from the
 * previous tide's drained table (see {@link #ebbReservoir}), so stable
 * key universes allocate no adders per cycle; the pool is bounded by one
 * cycle's key universe.  The per-writer {@link Ceils} maps follow the
 * same recycling discipline: dead writers' drained maps are pooled for
 * the next writer (see {@link #ceilPool}, capped at
 * {@link #CEIL_POOL_CAP}), so per-request-thread deployments allocate a
 * local map at most once per pooled wave instead of once per request.
 * Memory therefore tracks the concurrent hot-writing thread count
 * (~7KB per live thread that ever counted a hot key), not the cumulative
 * churn — a large but live writer pool retains one map per thread until
 * the thread dies and the tide reaps it.
 *
 */

@Slf4j
@Internal
public class WaveCounter implements InitializingBean, Destroyable {

  /**
   * Default local increments before a bulk add into the shared table.
   *
   * <p>The knee of a 64/128 sweep on the hot path: the larger batch
   * measured at or above the smaller across every workload.  Half of
   * {@link #LOCAL_CAPACITY},
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
   * Kept low so relative hot spots at low traffic still qualify (an
   * absolute threshold silently fails for keys that carry most of a
   * low-volume cycle's traffic).
   *
   * <p>This is the {@link #moonsTidalForce}'s seed and lower clamp: the promotion
   * floor is adaptive, never above {@link #FLOOR_MAX}.
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
   * Sub-bucket count for the density refinement of the promotion boundary
   * (see {@link #subHistogram}): when the boundary bucket of the log2
   * histogram overflows the remaining {@code hotLimit} slots, the bucket's
   * count range is re-bucketed into this many LINEAR slots, refining the
   * power-of-two boundary to the actual ~k-th largest count.  64 gives
   * single-count resolution for bucket floors below 64 (the common
   * boundary region); the sub-histogram is one shift per candidate (the
   * boundary floor is structurally a power of two, so the sub-index is a
   * shift, never a division), paid only on overflowing tides by the
   * deliverer.
   */
  private static final int SUB_HISTOGRAM_BUCKETS = 64;

  /** Shift form of {@link #SUB_HISTOGRAM_BUCKETS} (power of two). */
  private static final int SUB_SHIFT = 6;

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

  /**
   * Ceiling on {@code hotLimit} (2^25 ≈ 33.5M hot keys): the beacon room
   * space is {@code hotLimit * 32} rooms, and the power-of-two sizing loop
   * must terminate on a positive {@code int} bit count — a room space at or
   * above 2^31 would overflow the loop's bit count to a negative mask and
   * mis-index (or OOM on) the beacon array.  A math guard, not a sizing
   * knob — no real hot set approaches it.
   */
  private static final int MAX_HOT_LIMIT = 1 << 25;

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
   * Check-sampling mask for the {@code settle-writes} wait: the in-flight
   * counter's {@code get()} walks its striping cells (O(cores) volatile
   * reads), so the spin loop re-checks it only every 64th iteration — the
   * common exit (counter already zero, or a µs-scale merge) lands on the
   * first check, and a sampled exit is delayed by at most one sample
   * period (sub-µs of onSpinWait).
   */
  private static final int SETTLE_CHECK_MASK = 63;

  /**
   * Adaptive-delivery floor: under backlog the tide re-schedules at no less
   * than this interval, bounding burst detection latency and keeping the
   * reporter's reservoir away from its capacity cap.
   */
  private static final long EARLY_TIDE_MIN_INTERVAL_MS = 50;

  /**
   * Tide-scheduling coalescing tolerance (Caffeine's {@code Pacer}
   * {@code TOLERANCE}, millisecond domain): a proposed fire within this
   * many ms of the pending fire is "soon enough" — the request merges
   * into the pending tide instead of churning a cancel/reschedule.
   * Roughly 2x {@link #EARLY_TIDE_MIN_INTERVAL_MS}: a 50ms nudge skips
   * only when the pending tide fires within ~150ms of now.
   */
  private static final long SCHEDULE_TOLERANCE_MS = 100;

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
  private static final double RENEWAL_TARGET = 0.5;

  /** Promotion-governor ceiling on the floor; the seed and lower clamp are {@link #PROMOTION_FLOOR}. */
  private static final int FLOOR_MAX = 256;

  /** Promotion-governor initial probe step, in count units. */
  private static final int STEP_INITIAL = 8;

  /** Promotion-governor step decay toward convergence (WindowClimber's {@code Step}). */
  private static final double STEP_DECAY = 0.98;

  /**
   * Release-walk stride ceiling.  The release law prices the stride from
   * the smoothed renewal against the walk's own crash bar, clamped to
   * [1, {@value}]; the ceiling keeps a fully healthy set from traversing
   * the whole floor domain in a single tide.  It is intentionally not
   * scaled with {@link #STEP_INITIAL}: the raise direction uses smaller,
   * more conservative steps while a healthy release may still move faster
   * back toward the seed.
   */
  private static final int STRIDE_MAX = 32;

  /**
   * Promotion-governor veto streak: consecutive distress tides after
   * which a failed raise probe retreats to where it started
   * (WindowClimber's {@code Anchor} veto / probe-walk undo).
   */
  private static final int VETO_STREAK = 4;

  /** Promotion-governor budgeted retreat strides (WindowClimber's veto-return). */
  private static final int RETURN_BUDGET = 8;

  /** Per-stride cap while returning to the probe base. */
  private static final int STEP_RETURN_MAX = 8;

  /**
   * Promotion-governor audit wait: healthy-and-still tides after which a
   * raised floor is probed one step back down (WindowClimber's
   * {@code AuditClock} re-testing a still equilibrium) — releases the
   * ratchet a distress phase left behind.
   */
  private static final int AUDIT_WAIT = 8;

  /** Promotion-governor saturation fraction: the hot set is "full" at 90% of {@link #hotLimit}. */
  private static final double SATURATION_FRACTION = 0.9;

  /** Local map capacity (power of two); must be ≥ 2 × opMaxCount so probing never fills it. */
  private static final int LOCAL_CAPACITY = 256;

  /**
   * Ceils-pool cap: the maximum number of drained per-writer hot maps
   * retained for reuse (~7KB each ≈ 1.8MB at the cap).  A burst of thread
   * deaths with no new writers must not pin the whole wave's maps forever;
   * the steady-state pool is one tide's dead-writer count anyway (the
   * {@link #ebbReservoir} bound, applied to maps).
   */
  private static final int CEIL_POOL_CAP = 256;

  /**
   * Upper bound for pre-sizing the fresh shared table at each tide swap.
   *
   * <p>The new table is sized from the cycle's approximate distinct-key
   * count so high-cardinality workloads avoid repeated {@link
   * ConcurrentHashMap} resizes.  The cap keeps a pathological burst from
   * pinning a needlessly large empty table; beyond this the table simply
   * grows on demand as before.
   */
  private static final int MAX_RESERVOIR_PREALLOC = 1 << 20;

  /** Time-check sampling: every 16th local add re-checks the flush clock. */
  private static final int TIME_CHECK_MASK = 15;

  /** Local increments before a bulk add into the shared table (hot path). */
  private final int opMaxCount;

  /** Max age of local hot data before the writer bulk-merges it (milliseconds). */
  private final long flushIntervalMillis;

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
   *       key can be promoted on any tide (no freeze at capacity), a
   *       stable hot key re-promotes every tide and never decays out (no
   *       ping-pong).
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
   * Ceils recycling pool (the {@link #ebbReservoir} pattern applied to the
   * per-writer hot maps): a DEAD writer's fully-drained map is reset and
   * returned here by the tide, and the first hot count of a NEW writer
   * claims it instead of allocating.  Virtual-thread and
   * per-request-thread deployments churn writers at request rate; without
   * the pool every request would allocate a ~7KB local map that dies with
   * its thread and is only reaped at the next tide.
   *
   * <p><b>Invariants.</b> Only the deliverer pushes (dead writers' maps,
   * post-{@code drainDead} + {@link Ceils#reset()} — the dead thread can
   * never touch the map again, so a pooled instance is single-owner
   * forever); any writer claims (first hot count per thread, off the
   * steady-state path).  The pool is capped at {@link #CEIL_POOL_CAP} so a
   * one-time wave of thread deaths cannot retain its maps permanently.
   */
  private final ConcurrentLinkedQueue<Ceils> ceilPool = new ConcurrentLinkedQueue<>();

  /** Size guard for {@link #ceilPool} (single pusher, so the count is exact). */
  private final AtomicInteger ceilPoolSize = new AtomicInteger();

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

  /**
   * Lifecycle flag: {@code true} after {@link #destroy()} — new {@code count()}
   * calls no-op.  Read on EVERY count (both the hot and the cold path), so the
   * per-op read is the cheapest access mode that preserves the destroy
   * contract: OPAQUE via {@link #SHUTDOWN}.  The contract is eventual
   * visibility, not ordering — destroy's residual loss window for a writer
   * that raced past the store is documented as ns-scale, and opaque only
   * widens it by the store-propagation latency (~40-80ns); no cross-variable
   * chain depends on this flag (unlike {@link #coldWriteSeen}).  The
   * destroy/tide-path accesses stay VOLATILE (once per destroy/tide, so the
   * barrier is free there) so the shutdown store is a proper publish point.
   */
  @SuppressWarnings("unused")
  private boolean shutdown;

  /** Access handle for {@link #shutdown} (opaque on the count path). */
  @SuppressWarnings("all")
  private static final VarHandle SHUTDOWN;

  static {
    try {
      SHUTDOWN = MethodHandles.lookup().findVarHandle(WaveCounter.class, "shutdown", boolean.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

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
   * Overshoot headroom on the cold-path capacity guard
   * ({@link #capacity} / 10): new keys are admitted until the table
   * reaches {@code capacity + capacityHeadroom} distinct keys.  The guard
   * exists to bound memory, not exactness, and the pacer keeps the
   * steady-state reservoir ~5x below the cap — the headroom absorbs the
   * remaining burst edge so the drop (whose policy biases against NEW
   * keys, the worst case for burst detection) stays near-unreachable
   * while the memory bound stays O(capacity).
   */
  private long capacityHeadroom;

  /**
   * O(1) distinct-key counter for the cold-path capacity guard —
   * {@code ConcurrentHashMap.size()} reads the CHM counter cells
   * (O(cores) volatile reads) on every miss.  Incremented exactly once
   * per real first insert into the current table: the cold miss branch in
   * {@code count()} bumps it on the {@code putIfAbsent} winner, and the
   * hot-path drains ({@code Ceils.waveTo}) bump it the same way — a
   * promoted key's first merge into the fresh table is a first insert
   * too.  Reset at every table swap.  Approximately equal to the table's
   * distinct-key count except for the swap race window (a first-insert
   * that captured the OLD table reference after the reset over-counts by
   * one until the next swap, and a drain racing a swap may bump a
   * counter that the swap already reset — a key in the discarded table,
   * correctly not counted) — well within the documented "approximate
   * (racy size check)" semantics.
   *
   * <p><b>Why not LongAdder?</b> A swap was evaluated with a desktop
   * micro-benchmark (JDK 26, tight-loop and ~200k/s paced regimes).  The
   * guard read and the increment are SAME-FREQUENCY on the cold-miss path
   * (capacity &gt; 0 only), so LongAdder trades cheap CAS increments for a
   * cell-walking {@code sum()} that measured 3-9x slower per read than
   * {@code get()}; in the realistic paced regime the whole counter cost
   * is ~2% of per-op time and the swap measured a flat 1.0x.  The only
   * LongAdder win (30% at 16 threads) appeared in a tight-loop regime
   * that WaveCounter cannot reach — increments are bounded by the
   * per-cycle distinct-key rate, not the op rate.
   *
   * <p><b>Weak access modes.</b>  The counter is stored as a plain
   * {@code long} and accessed only through the {@code APPROXIMATE_SIZE}
   * {@link VarHandle}: reads and the tide reset use opaque mode
   * ({@code getOpaque} / {@code setOpaque}), the increment uses
   * {@code getAndAddRelease} (the RMW family has no opaque variant;
   * release is the weakest available and its one-way ordering is
   * harmless here).  The field is single-variable and approximate — no
   * cross-variable ordering is ever required (a stale read falls inside
   * the documented "approximate (racy size check)" semantics), so the
   * weak modes (atomic, coherent, no full barriers) replace the
   * AtomicLong's volatile RMW with no semantic change.
   */
  @SuppressWarnings("unused")
  private long approximateSizeValue;

  /** Opaque access handle for {@link #approximateSizeValue}. */
  private static final VarHandle APPROXIMATE_SIZE;

  static {
    try {
      APPROXIMATE_SIZE = MethodHandles.lookup().findVarHandle(WaveCounter.class, "approximateSizeValue", long.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Quiescence gate: set by any shared-table insert BEFORE
   * the entry becomes visible, cleared by the deliverer at table swap.
   * When unset, no insert has happened this cycle — the old table at the
   * swap is empty and nothing can be lost — and the tide skips the 1ms
   * quiescence window; when set, the window is paid.  A miss-path writer
   * preempted across the swap re-targets
   * the NEW table (its computeIfAbsent re-reads the field), so the flag
   * cannot lose a write by itself.
   *
   * <p>Set by EVERY real insert into the shared table: the cold-miss
   * branch in {@code count} (before the insert — the drop branch returns
   * before it, so a dropped key never marks), every merge
   * ({@link #mergeKey}, before the {@code putIfAbsent} makes the entry
   * visible), and the gate-protected writer-side merge paths
   * ({@link #discharge}, {@link #reconcile}, {@link # recoverZero}, inside
   * {@link #reservoirGate}) so a swap that races an in-flight merge still
   * captures the mark — a hit on an entry inserted by ANY path (cold
   * miss, hot drain, reconcile, recovery) is a cold write too, and the
   * window is the bound it documents.
   * The store is read-gated ({@code if (!coldWriteSeen) coldWriteSeen = true;}):
   * the flag is monotonic within a cycle, so only the first mark of a
   * cycle pays (a CAS would cost a locked RMW per mark — more than the
   * store it replaces).  The read-then-store race is benign: both writers
   * store anyway, and the only ordering that matters — a store landing
   * before the tide's gate-acquire — is unchanged (a writer that observes
   * the flag already set inserts with the window already gated on).
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
   * Earliest-first tide scheduling (Caffeine's {@code Pacer} policy,
   * millisecond domain): at most one tide is ever pending; an external
   * {@link #nudgeTide()} that requests a meaningfully earlier fire
   * cancels and replaces the pending one-shot, and requests within
   * {@link #SCHEDULE_TOLERANCE_MS} of the pending fire are merged
   * (skipped).  Delays are clamped up to
   * {@link #EARLY_TIDE_MIN_INTERVAL_MS} and {@link #nextFireTimeMs}
   * keeps a {@code 0} sentinel for "unscheduled" (the sentinel is never
   * committed as a fire time — {@code scheduleAt == 0} maps to 1).
   *
   * <p><b>Why synchronized.</b>  The self-reschedule runs on the
   * deliverer thread, but a nudge may arrive from ANY thread (an
   * external pressure signal), so the pending-future state is guarded —
   * a racy double-arming would run two concurrent tides and race the
   * table swap (two {@link #tideWatcher()} rotations).  The cost is
   * paid on the scheduling path only (once per tide plus per nudge),
   * never per count.
   */
  private final Object tideScheduleGate = new Object();

  /** Fire time of the pending tide (monotonic clock, ms); {@code 0} = unscheduled. */
  private long nextFireTimeMs;

  /** The single pending one-shot tide (guarded by {@link #tideScheduleGate}). */
  private ScheduledFuture<?> pendingTide;

  /**
   * Adaptive promotion floor (WindowClimber borrowings): the
   * floor probes up via bounded raise-walks when the hot set
   * under-earns the cold reservoir (density-ratio evidence), admits
   * blocked keys by dropping toward the boundary under health, retreats
   * when a probe fails, and audits back down under health or
   * saturation — the moves run as bounded WALKS (frozen base,
   * anchor-memory goal verdict, budgeted undo on crash, retry backoff),
   * and movement decays the audit run instead of zeroing it (ADR-0045).
   * Written only by the deliverer thread, so no
   * synchronization.
   */
  private final MoonsTidalForce moonsTidalForce = new MoonsTidalForce();

  /**
   * Log2-bucket promotion histogram, reused across tides instead of
   * allocated per tide (the deliverer is the only writer).  Zeroed with
   * {@link Arrays#fill} at the start of each promotion pass.
   */
  private final int[] histogram = new int[HISTOGRAM_BUCKETS];

  /**
   * Log2-bucket histogram of the CANDIDATES only (keys at or above the
   * floor), filled in the same pass as {@link #histogram} (deliverer-only,
   * reused like it).  The difference {@code histogram[b] -
   * candidateHistogram[b]} is exactly the number of keys in the blocked
   * band {@code [2^b, floor)} — the boundary bucket is the highest
   * non-empty bucket, so no other bucket overlaps the band — the
   * governor's {@code blockedKeys} signal is one subtraction.
   */
  private final int[] candidateHistogram = new int[HISTOGRAM_BUCKETS];

  /**
   * Linear sub-histogram for the density refinement of the promotion
   * boundary (deliverer-only, reused like {@link #histogram}): when the
   * boundary bucket of the log2 histogram overflows the remaining
   * {@code hotLimit} slots, the bucket's count range {@code [2^b, 2^(b+1))}
   * is re-bucketed into {@link #SUB_HISTOGRAM_BUCKETS} linear slots and the
   * boundary is refined to the lower edge of the sub-bucket where the
   * top-down accumulation crosses the remaining slots — the actual ~k-th
   * largest count instead of a power of two.  Routing-only; paid only on
   * overflowing tides.
   */
  private final int[] subHistogram = new int[SUB_HISTOGRAM_BUCKETS];

  /**
   * Promotion candidate index list, reused across tides instead of allocated
   * per tide (the deliverer is the only writer): packed-array indices (into
   * {@link #allValues} / {@link #allHashes}) of the keys at or above the
   * floor, collected in the histogram sweep.  Cleared at the start of each
   * promotion pass and only read within the same pass, so stale entries from
   * a skipped pass are never observed.
   *
   * <p>Index views instead of entry lists: the split and the promotion scan
   * need only the value (the threshold test) and the cached hash (the beacon
   * tests) — both already packed in {@link #allValues} / {@link #allHashes} —
   * never the key itself.  Each value and hash lives in exactly one copy.
   */
  private int[] candidateIdx = new int[1024];

  /**
   * Incumbent index snapshot for the renewal-first promotion pass
   * (deliverer-only, reused like {@link #candidateIdx}): the beacon members
   * captured BEFORE the per-tide halving decay — the only memory of who was
   * hot last tide, since the decay zeroes the evidence of every member on a
   * saturated set's scan tide.  The split from the newcomers happens in the
   * same merged sweep that builds the sub-histogram (one membership test per
   * candidate).  The two-pass scan re-promotes exactly these keys first
   * (renewals before newcomers), so the capacity break can never evict a
   * renewing key and the hot set stays stable under flat distributions where
   * the boundary tie-band is wider than the remaining slots.  The indices
   * reference the packed arrays — no extra key retention beyond the tide.
   */
  private int[] incumbentIdx = new int[1024];

  /**
   * Newcomer index snapshot for the fill pass (deliverer-only, reused like
   * {@link #incumbentIdx}): the candidates that were NOT beacon members
   * before the halving decay, split from the incumbents in the same merged
   * sweep that builds the sub-histogram.  Pass 2 iterates exactly this list,
   * so no membership re-test is needed there: the split is decided once,
   * pre-decay, and pass 1 re-promotes every qualifying incumbent, so no
   * newcomer can already be a member when pass 2 runs.  Only
   * read within the same overflow pass, so stale entries from a skipped pass
   * are never observed.
   */
  private int[] newcomerIdx = new int[1024];

  /**
   * Packed per-key (value, hash) snapshot arrays, reused across tides instead
   * of allocated per tide (the deliverer is the only writer).  Filled in the
   * same single sweep as {@link #histogram} for EVERY snapshot entry —
   * including {@code v <= 0} members, which the floor filter below skips but
   * the saturated branch needs (a stale-squatting member reads 0 this tide),
   * so the packing happens BEFORE the filter.
   *
   * <p>The saturated branch (see {@link #promote(Map)}) re-visits the whole
   * snapshot to count renewals and member earnings; the packed arrays give
   * it a tight two-array scan with the pass-1 hashes — one
   * {@code mixHash} per key per tide, never a re-avalanche.  The values are
   * stored because the branch needs them per key (two sequential arrays read
   * in lockstep — better locality than dereferencing the snapshot's own
   * {@code Map.Entry} nodes).
   *
   * <p>The packed arrays are also the data mother of the index views
   * ({@link #candidateIdx} / {@link #incumbentIdx} / {@link #newcomerIdx}):
   * a view holds only packed indices, so the value and the hash are read
   * from this single copy instead of from per-view duplicates.
   *
   * <p>Deliverer-only, grown by doubling like {@link #candidateIdx},
   * cleared implicitly via the pass-local packed-size counter (stale tail
   * entries are never read).
   */
  private long[] allValues = new long[1024];

  /** Parallel to {@link #allValues}: the cached avalanched hashes. */
  private int[] allHashes = new int[1024];

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
   * reservoir: once the shared table reaches it (plus a 10% overshoot
   * headroom, see {@link #capacityHeadroom}), <b>new</b> keys are dropped
   * — their counts are lost for this cycle — while already-tracked keys
   * keep counting. The cap is approximate (racy size check) — it bounds
   * memory, not exactness.
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
    this.capacityHeadroom = this.capacity / 10;
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
    // (parameter validation): the local map's open-addressing probe must
    // never fill — the batch trigger discharges at opMaxCount, so at most
    // that many slots are claimed, and LOCAL_CAPACITY / 2 keeps the probe
    // distance bounded at a full batch (a claim for a new key past that
    // point would loop forever over occupied slots).  hotLimit is capped so
    // the beacon room space (hotLimit * 32) stays within a positive-int
    // power of two — beyond 2^30 rooms the sizing loop overflows to a
    // negative mask and mis-indexes the array.
    if (opMaxCount <= 0 || opMaxCount > LOCAL_CAPACITY / 2) {
      throw new IllegalArgumentException(
        "opMaxCount must be in (0, " +
          (LOCAL_CAPACITY / 2) +
          "] so the open-addressing probe can never fill the local map"
      );
    }
    if (hotLimit < 0 || hotLimit > MAX_HOT_LIMIT) {
      throw new IllegalArgumentException("hotLimit must be in [0, " + MAX_HOT_LIMIT + "]");
    }
    this.batchConsumer = batchConsumer;
    this.opMaxCount = opMaxCount;
    this.flushIntervalMillis = flushIntervalMs;
    this.deliverIntervalMs = deliverIntervalMs;
    this.hotLimit = hotLimit;
    // Counting/evidence beacon, sized hotLimit × 32 rooms rounded up to a
    // power of two (~0.37% false-positive rate at full capacity; ~24 KB at
    // the default 1024 limit). The power-of-two size keeps the room index
    // a mask, and the size itself never changes, so promotion never resizes.
    long wantBits = Math.max(1L, hotLimit * 32L);
    int beaconBitCount = 1;
    while (beaconBitCount < wantBits && beaconBitCount > 0) {
      beaconBitCount <<= 1;
    }
    this.beaconMask = beaconBitCount - 1;
    // hotLimit == 0 would allocate a 0-length array (beaconBitCount == 1),
    // turning every count() into an ArrayIndexOutOfBoundsException on the
    // first roleGet.  Clamp to one long: an empty beacon reads as 0 in
    // every room, so every key routes cold — the exact semantics of "no
    // hot set" (the promotion scan is gated by `promoted < hotLimit`, so
    // nothing is ever promoted either).
    this.beacon = new long[Math.max(1, beaconBitCount >>> 2)]; // 4 bits per room (2+2 roles)
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
   * @param key   the accessed key ({@code null} and empty keys are
   *              silently ignored)
   * @param delta the number of accesses (positive; non-positive deltas
   *              and empty keys are silently ignored)
   */
  @SuppressWarnings("all")
  public void count(String key, long delta) {
    if (key == null || key.isEmpty() || delta <= 0 || (boolean) SHUTDOWN.getOpaque(this)) {
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
        // deliverer can add residuals if the thread dies (isAlive).  The
        // map is claimed from the dead-writer pool when available instead
        // of allocated (see {@link #ceilPool}) — per-request-thread
        // deployments would otherwise allocate a ~7KB map per request.
        m = ceilPool.poll();
        if (m == null) {
          m = new Ceils();
        } else {
          ceilPoolSize.decrementAndGet();
        }

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
          (TimeSource.monotonicMillis() - m.lastFlushMillis) > flushIntervalMillis)
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
        // striped add.  The capacity test uses the O(1) approximateSize
        // instead of ConcurrentHashMap.size() (O(cores) counter-cell reads
        // per miss); the get() above doubles as the membership check — a
        // key inserted by another thread between the two reads at the
        // capacity boundary is inside the documented "approximate (racy
        // size check)" semantics.  Keeping the guard BEFORE the flag/insert
        // keeps the dominant drop path at one get + one atomic read.
        if (capacity > 0 && (long) APPROXIMATE_SIZE.getOpaque(this) >= capacity + capacityHeadroom) {
          return;
        }
        // (quiescence-gate): a cold writer that may land in the CURRENT
        // table marks the flag BEFORE its insert; the tide clears it at
        // swap and skips the 1ms quiescence window on cycles where no
        // cold writer was in flight (see the field doc).  A dropped key
        // never reaches this store — only real writes mark the flag.
        // Read-gated: the flag is monotonic within a cycle, so only the
        // first cold first-insert pays the store (see the field doc).
        // First insert: two-phase steal + putIfAbsent (see mergeKey) —
        // computeIfAbsent's mapping closure would allocate per real
        // first-insert (GC pressure under key churn).  A zeroed adder that
        // loses the putIfAbsent race to another thread is discarded (a
        // 24-byte object).  The approximate-size increment is exactly-once:
        // putIfAbsent's winner is the one real insert.
        cell = mergeKey(reservoir, ebbReservoir, key, delta, true);
      } else {
        cell.add(delta);
      }
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
   * instead of a second full avalanche ({@link #mixHash} + seed, 5 ops).
   * The multiply by an odd constant is a bijection and
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
   * Initial evidence value when a key is first promoted into the routing beacon.
   *
   * <p>A seed of {@code 2} (not {@code 1}) together with the per‑tide halving decay
   * yields a <em>two‑tide</em> memory window: a freshly promoted hot key is written
   * as {@code 2} in the current tide, decays to {@code 1} in the next tide (still
   * satisfies the membership test), and decays to {@code 0} in the tide after that,
   * automatically leaving the beacon.  The two‑tide hysteresis prevents ping‑pong
   * eviction/re‑promotion caused by a single quiet cycle while keeping the hot set
   * responsive to workload shifts.
   *
   * <p>This value is used only for path‑routing decisions; it is never added to
   * the actual per‑key counter.
   */
  private static final int INIT_COUNT = 2;
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
   * sweep — a decayed-away key frees its slot, so the hot set never
   * freezes at capacity.  The deliverer is the only writer, so the
   * read-modify-write needs no CAS.
   *
   * @param h   the avalanched key hash ({@code mixHash(key.hashCode())}),
   *            computed once by the promotion sweep and cached — the caller
   *            never recomputes it
   * @return {@code true} if the key became newly active (count room
   *         evidence 0 → 2)
   */
  @SuppressWarnings("java:S3358")
  private boolean promoteToBeacon(int h) {
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
    // evidence must not block this key's own field.  First promotion
    // seeds 2 (not 1): with the per-tide halving decay, a seed of 1
    // would leave the hot set after a single quiet tide; seeding 2 gives
    // a 2-tide memory (tolerates one cycle of counting fluctuation
    // without ping-pong).  The value is a memory mark, not a literal
    // promotion count.
    int delta1 = c < 3 ? (c == 0 ? INIT_COUNT : 1) : 0;
    int delta2 = t < 3 ? (t == 0 ? INIT_COUNT : 1) : 0;
    roleAdds(bit1, bit2, delta1, delta2);

    // Newly active = the count room transitioned 0 → 2 (a fresh slot in
    // the decayCounts() budget).  The guard above always writes when
    // c == 0, so the return is exact in every branch.
    return c == 0;
  }

  /**
   * Add independent deltas to the two role evidences of a promotion.
   *
   * <p><b>(merged read-modify-write).</b>  When both evidences share the
   * same long (1/16 of promotions), the word is loaded once, both fields
   * are adjusted in registers under their own saturation guards, and
   * stored once — instead of two independent read-modify-write cycles
   * (the JIT's CSE would likely merge the accesses anyway; the branch
   * makes it deterministic).  Deliverer-only, so no CAS.
   *
   * @param bit1   the bit1-role room index (masked by {@link #beaconMask})
   * @param bit2   the bit2-role room index (masked by {@link #beaconMask})
   * @param delta1 the signed delta for the bit1-role evidence (0 = no write)
   * @param delta2 the signed delta for the bit2-role evidence (0 = no write)
   */
  private void roleAdds(int bit1, int bit2, int delta1, int delta2) {
    if ((delta1 | delta2) == 0) {
      return;
    }
    int idx1 = bit1 >>> 4;
    int idx2 = bit2 >>> 4;
    int shift1 = ((bit1 & 15) << 2) + BIT1_OFFSET;
    int shift2 = ((bit2 & 15) << 2) + BIT2_OFFSET;

    if (idx1 != idx2) {
      beacon[idx1] += (long) delta1 << shift1;
      beacon[idx2] += (long) delta2 << shift2;
    } else {
      beacon[idx1] += ((long) delta1 << shift1) | ((long) delta2 << shift2);
    }
  }

  /**
   * Halving decay for the role evidences, run once per promoted tide
   * (snapshot with {@code distinctKeys >= MIN_PROMOTION_KEYS}) before the
   * promotion scan.  Tides without a scan skip the decay entirely —
   * empty tides never reach this method ({@link #tide()} calls
   * {@link #promote(Map)} only with a non-null snapshot) and sub-minimum
   * snapshots freeze it via the decay gate in {@code promote} — because
   * halving without the re-seeding scan would strip the whole hot set
   * within two tides (evidence 2→1→0) on small workloads; see ADR-0038):
   * every 2-bit field {@code >> DECAY_SHIFT}, and fields
   * that decay to zero free their slot.  Both roles decay on the same
   * ladder ({@link #DECAY_SHIFT} — the per-lane halving), so a member's
   * two evidences expire in lockstep (no false negatives from one
   * outliving the other).  Also returns the active hot-set size (the
   * hotLimit gate for the promotion scan).
   *
   * <p><b>SWAR.</b>  In a 2-bit lane the decayed value is exactly the
   * lane's high bit moved to the low position, so the whole word decays
   * with one mask-and-shift (bit-parallel, no per-room loop), and the
   * post-decay active count is the same shifted value's lane popcount.
   * See
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
   * <p>Single conditional-removal ({@code remove(key)} alone): the pool
   * is replaced wholesale per tide and published only after every adder
   * was summed, so the entry under a key cannot change between a get and
   * a remove — {@code remove(key)} is exact and halves the CHM lookups.
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
   * Return a dead writer's fully-drained local map to the pool for reuse
   * (see {@link #ceilPool}).  The map is {@link Ceils#reset() reset} first,
   * clearing any phantom claim slots a mid-claim death left behind.  Only
   * the deliverer calls this (tide phase-1, after {@code drainDead} and
   * the registry reaping), so the pool-size guard is exact.
   *
   * @param m the dead writer's local map (drained and reaped)
   */
  private void recycleCeils(Ceils m) {
    if (ceilPoolSize.get() >= CEIL_POOL_CAP) {
      return;
    }

    m.reset();
    ceilPool.offer(m);
    ceilPoolSize.incrementAndGet();
  }

  /**
   * Merge a count into the shared table: recycle the key's adder from the pool
   * when available (two-phase steal + {@code putIfAbsent} — see
   * {@link #stealRecycled}), and bump the distinct-key counter exactly once on
   * the real first insert (the {@code putIfAbsent} winner).
   *
   * <p>Shared by the cold path in {@link #count(String, long)}, the hot-path
   * drains ({@code Ceils.waveTo}), the tag-driven reconcile and the zero-return
   * recovery — one merge protocol in one place, so the exactness argument
   * (never lost, never double-counted) lives in a single code path instead of
   * four near-identical copies.
   *
   * <p>The merge also marks the {@link #coldWriteSeen} quiescence flag BEFORE
   * the {@code putIfAbsent} makes the entry visible: an entry inserted by ANY
   * path (cold miss, hot drain, reconcile, recovery) can be hit-added by a
   * cold writer, and the tide's 1ms window is gated on the flag — a hit on
   * a hot-drain-inserted entry is a cold write too and needs the same
   * window, so every insert path marks the flag before visibility.
   * The store is read-gated (the flag is monotonic within a cycle),
   * so only the first mark of a cycle pays; the writer-side paths that hold
   * {@link #reservoirGate} additionally mark inside the gate (see
   * {@link #discharge(Ceils)}), so a swap that races an in-flight merge
   * still captures the mark.
   *
   * @param table           the shared accumulation table (non-null)
   * @param ebb             the adder-recycling pool (previous tide's drained
   *                        table, possibly null)
   * @param key             the merged key
   * @param value           the count to merge (positive)
   * @param markQuiescence  whether this merge may be the first insert of
   *                        the cycle and therefore must mark
   *                        {@link #coldWriteSeen}; hot drain sweeps pass
   *                        {@code false} because their caller marks once
   *                        before the sweep
   * @return the live cell the value was added into (the {@code putIfAbsent}
   *         winner, or the stolen/allocated cell on a first insert)
   */
  private LongAdder mergeKey(
    ConcurrentHashMap<String, LongAdder> table,
    ConcurrentHashMap<String, LongAdder> ebb,
    String key,
    long value,
    boolean markQuiescence
  ) {
    // (quiescence-gate): mark the cold-write flag BEFORE the entry becomes
    // visible — any entry inserted this cycle may be hit-added by a cold
    // writer, whose add needs the window's protection (see the field doc).
    // Read-gated: only the first mark of the cycle pays a store.
    if (markQuiescence && !coldWriteSeen) {
      coldWriteSeen = true;
    }
    LongAdder candidate = stealRecycled(ebb, key);
    LongAdder cell = candidate != null ? candidate : new LongAdder();
    LongAdder prev = table.putIfAbsent(key, cell);
    if (prev == null) {
      APPROXIMATE_SIZE.getAndAddRelease(this, 1L);
    }

    cell = prev != null ? prev : cell;
    cell.add(value);
    return cell;
  }

  /**
   * Bulk-add a writer's hot local map into the shared table and reset it.
   *
   * <p>The per-writer mutex lives inside {@link Ceils#drainInto} (the
   * per-map {@link ReentrantLock} — never a parameter), serializing this
   * writer's add against the deliverer's drain of the same map.  The
   * {@link #reservoirGate} mutex here makes the capture of the shared-table
   * reference atomic against the deliverer's wholesale swap, so an add
   * never writes into a table that is already being snapshotted.  The same
   * capture marks the {@link #coldWriteSeen} quiescence flag (see the gate
   * block), so a swap racing this in-flight merge still pays the window
   * for cold hit-writers on the entries it inserts.
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
      // snapshot — the gated quiescence window does not cover it (a
      // hot-racing stress lost 32/160k ops with the window skipped).
      mergesInFlight.increment();
      // (quiescence-gate): mark the cold-write flag HERE, atomically with
      // the capture — the entries this merge inserts may land in `table`
      // after the swap (an in-flight drain), and cold hit-writers on them
      // need this cycle's window; a mergeKey-only mark would land after
      // the tide's flag capture and be lost.  Read-gated: the flag is
      // monotonic within a cycle, so only the first mark pays a store.
      if (!coldWriteSeen) {
        coldWriteSeen = true;
      }
    }
    try {
      m.drainInto(table, ebbReservoir);
    } finally {
      mergesInFlight.decrement();
    }
  }

  /**
   * Tag-driven recovery merge of a writer's local map, triggered by the
   * writer's post-check in {@code Ceils#add} when a drain raced its
   * fast add (the drain's sweep or wholesale bit clear may have missed
   * the racing entry).  Same reference-capture/reservation shape as
   * {@link #discharge}; the sweep itself is tag-driven
   * ({@link Ceils#reconcile}), so entries whose marks were wiped are
   * still recovered.  Bounded one-tide delay, never loss, never double.
   */
  private void reconcile(Ceils m) {
    ConcurrentHashMap<String, LongAdder> table;
    synchronized (reservoirGate) {
      table = reservoir;
      mergesInFlight.increment();
      // (quiescence-gate): mark the cold-write flag atomically with the
      // capture — same argument as {@link #discharge(Ceils)}: the entries
      // this sweep inserts may land after the swap, and cold hit-writers
      // on them need this cycle's window.
      if (!coldWriteSeen) {
        coldWriteSeen = true;
      }
    }

    try {
      m.reconcile(table, ebbReservoir);
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
      int nextCapacity = (int) Math.min((long) APPROXIMATE_SIZE.getOpaque(this), MAX_RESERVOIR_PREALLOC);
      reservoir = nextCapacity > 16 ? new ConcurrentHashMap<>(nextCapacity) : new ConcurrentHashMap<>();
      APPROXIMATE_SIZE.setOpaque(this, 0L);
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
    if (mergesInFlight.get() != 0) {
      for (int check = 0; ; check++) {
        // (settle-writes): the counter is re-read on a sampling cadence —
        // get() walks the striping cells, and the 64-iteration period keeps
        // the exit delay sub-µs (the counter is zero or a bounded 256-slot
        // merge in the common case; the sample cannot miss a landing zero —
        // the loop simply re-samples until it observes it).
        if ((check & SETTLE_CHECK_MASK) == 0 && mergesInFlight.get() == 0) {
          break;
        }

        if (--spin >= 0) {
          Thread.onSpinWait();
        } else {
          Thread.yield();
        }
      }
    }

    if (quiesce) {
      // (time source): this window deliberately stays on
      // System.nanoTime() — the 1ms bound is precision-critical
      // (monotonicMillis' ms quantization would widen it to 1-2ms and
      // make `remaining >> 1` degenerate into parkNanos(0)); it is a
      // pure park, not a testable behavior surface.
      long qDeadline = System.nanoTime() + SNAPSHOT_QUIESCENCE_NANOS;
      long now = System.nanoTime();

      while (now < qDeadline) {
        long remaining = qDeadline - now;
        if (remaining > 100_000) {
          // > 100µs: yield the core
          LockSupport.parkNanos(remaining >> 1);
        } else {
          Thread.onSpinWait();
        }
        now = System.nanoTime();
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
   * @return distinct key count, read from the O(1) counter (data still
   *         resident in writers' hot local maps is excluded; the swap-race
   *         approximation is the documented counter semantics)
   */
  public long estimatedSizeOfKeysCount() {
    return (long) APPROXIMATE_SIZE.getOpaque(this);
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
      APPROXIMATE_SIZE.setOpaque(this, 0L);
      // ...and drop the quiescence flag too so a pre-clear cold write
      // cannot force an unnecessary window later (an in-flight writer
      // racing clear() is ns-scale; counts dropped by clear are lost
      // anyway by definition).
      coldWriteSeen = false;
    }

    // A clear() is a full reset — drop the adder pool so no recycled
    // state survives it (steals would otherwise reuse pre-clear adders).
    ebbReservoir = null;

    // A clear() is a full reset — the routing beacon must not keep stale
    // hot routes: without this, promoted keys would keep taking the hot
    // path for up to 2 tides after the drop (the halving decay's memory).
    // Routing-only, so the stale path was never a correctness issue, but
    // the "ready for reuse" contract should start from a blank slate.
    Arrays.fill(beacon, 0L);
    // The governor floor and pacer reference are deliberately retained:
    // they are adaptive filters tuned over many tides, and clearing them
    // on a transient workload reset would throw away the adaptivity
    // (the floor self-corrects via its audit/release law within a few
    // tides of the new distribution).
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
   *       gated by the {@code coldWriteSeen} flag: skipped only when the
   *       shared table received no writes this cycle — its old table is
   *       empty anyway).
   *       Only then is the old table checked for emptiness; the window
   *       precedes the check so a writer that recovers during it can still
   *       land in the snapshot.</li>
   *   <li>{@code deliver-then-promote} — snapshot the old table into a
   *       single map, deliver it to the batch consumer, then
   *       {@link #promote(Map)} the cycle's hot keys to the exact hot path
   *       (boundary estimation, density refinement, incumbent-first scan —
   *       see the method's Javadoc) — delivery first, so the O(n)
   *       promotion work never adds to the consumer's latency; and tide.</li>
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
      // drained lock-free (it can never write again — see drainDead).  The
      // table reference is captured ONCE before the loop: the tide is the
      // only thread that swaps the table at tide time, and the swap happens
      // later in this same thread (tideWatcher), so a single capture is
      // equivalent to per-writer captures.  A concurrent clear() may swap
      // the table mid-loop: drains then land in the discarded table and
      // their counts are dropped — consistent with clear()'s full-reset
      // contract (counts racing a clear are lost by definition).
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
            // (dead-writer drain before reap): size is a plain field read
            // and can lag the claim by a few ns — a stale-zero read racing
            // the writer's death would drop a just-claimed residual (the
            // occupied bit and slot writes are committed but size was not
            // yet visible).  drainDead sweeps the occupied bitmap, which
            // is the truth: an empty map costs ~4 bit checks, so draining
            // unconditionally is free at tide frequency and closes the
            // window.
            entry.getValue().drainDead(table, ebbReservoir);
            hotRegistry.remove(entry.getKey(), entry.getValue());
            // (recycle-ceils): the drained map is dead-writer-owned and
            // empty — return it to the pool for the next writer instead
            // of letting it become garbage with the thread (see
            // {@link #ceilPool}).
            recycleCeils(entry.getValue());
          }
          continue;
        }

        Thread writer = entry.getKey();
        Ceils local = entry.getValue();

        if (!writer.isAlive()) {
          // Dead writer: quiescent map, no lock needed.  Reap after.
          local.drainDead(table, ebbReservoir);
          hotRegistry.remove(writer, local);
          recycleCeils(local);
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
          // (recycle-ceils): tryDrainInto just emptied the map and the
          // writer died before the re-check — pool it like the other
          // dead-writer paths (see {@link #ceilPool}).
          recycleCeils(local);
        }
      }

      Map<String, Long> snapshot = tideWatcher();
      if (snapshot != null) {
        // Adaptive cadence: the raw backlog is folded into a
        // smoothed reference — a burst beyond the still band attacks
        // instantly (unchanged burst latency), quiet tides release at the
        // EMA rate (no 50<->500ms ping-pong), in-band jitter moves nothing.
        int distinctKeys = snapshot.size();
        nextDelayMs = computeNextTideDelayMs(pacer.pressure(distinctKeys));
        // (deliver-before-promote): the consumer receives the snapshot
        // BEFORE the promotion pass — the O(n) promotion work (hash
        // sweep, histogram, beacon decay, governor) must not add to the
        // delivery latency the consumer sees; the beacon state it
        // produces only affects the NEXT cycle's routing.  The promote
        // still runs when the consumer throws (the batch is lost either
        // way — see the tide catch), so a failing consumer cannot also
        // freeze the routing.  The consumer must not mutate the
        // delivered map.
        try {
          batchConsumer.accept(snapshot);
        } finally {
          // (snapshot-promote): boundary estimation, density refinement
          // and the promotion scan live in {@link #promote(Map)} — see
          // its Javadoc for the phase detail (the decay ordering is
          // internal).
          promote(snapshot);
        }
      } else {
        // Empty tide: the ladder stretches the cadence (idle power saving)
        // up to {@link #EMPTY_TIDE_STRETCH_CAP_MULTIPLE} x the base.
        nextDelayMs = pacer.emptyDelay(deliverIntervalMs);
      }
    } catch (Exception e) {
      log.error("Scheduled delivery failed", e);
    }
    // Self-rescheduling chain (one-shot schedule → tide → re-schedule),
    // routed through the coalescing pacer so a pending nudge wins when
    // it is meaningfully earlier.  Guarded by deliveryStarted so
    // reflection-driven tides in tests never arm a background chain, and
    // by shutdown so destroy() stops it.
    if (deliveryStarted && !((boolean) SHUTDOWN.getVolatile(this))) {
      scheduleTide(nextDelayMs);
    }
  }

  /**
   * Promotion pass of a delivered snapshot (tide phase {@code snapshot-promote}):
   * estimate the top-{@code hotLimit} boundary from a log2-bucket histogram of
   * the cycle's counts (floor {@link #PROMOTION_FLOOR}), refine it by density
   * when the boundary bucket overflows (a linear sub-histogram lands the
   * threshold at the actual ~k-th largest count), and promote the keys at or
   * above it to the exact hot path — renewals before newcomers when the
   * refined boundary still cuts a tie-band wider than the remaining slots
   * (the incumbent/newcomer split is decided in the same merged sweep that
   * builds the sub-histogram, so the fill pass needs no membership re-test).
   * The same sweep packs every entry's (value, hash) into reused arrays
   * ({@link #allValues} / {@link #allHashes}), so the saturated branch
   * reuses the pass-1 hashes instead of re-avalanching the whole snapshot.
   * The promotion scan itself iterates packed-index views
   * ({@link #candidateIdx} / {@link #incumbentIdx} / {@link #newcomerIdx})
   * into the same packing — one copy of each value and hash, zero entry
   * references.
   * See ADR-0042.
   *
   * <p><b>Ordering contract.</b>  The histogram/candidate pass, the boundary
   * estimation and the incumbent capture run BEFORE {@link #decayCounts()}:
   * the pre-decay beacon state is the only memory of last tide's membership
   * (the halving decay zeroes every member's evidence on a saturated set's
   * scan tide), so the renewal-first pass can only identify incumbents
   * pre-decay.  The decay then frees decayed slots (a drifted-away key
   * leaves within 2 non-empty tides) and gates the scan on the active hot-set size.
   * A snapshot below {@link #MIN_PROMOTION_KEYS} distinct keys is skipped:
   * its distribution is meaningless and promoting everything would burn
   * beacon slots on startup noise.
   *
   * <p>Deliverer-only, called once per non-empty tide by {@link #tide()};
   * the only per-tide result is the beacon/governor state it mutates.
   *
   * @param snapshot the just-delivered per-key counts (non-null; empty
   *                 snapshots never reach this method)
   */
  @SuppressWarnings("all")
  private void promote(Map<String, Long> snapshot) {
    int distinctKeys = snapshot.size();
    int floor = moonsTidalForce.floor();
    long threshold = floor;
    long boundary = 1; // the pure histogram boundary, before the floor
    // The histogram's highest non-empty bucket (the boundary bucket); -1
    // when the promotion pass was skipped (no band can exist either).
    int boundaryBucket = -1;
    boolean overflow = false;
    // Total of every non-zero count in the snapshot — the density signal's
    // cold-reservoir numerator (see the onTide call below).
    long snapshotSum = 0;
    // Entries packed into {@link #allValues} / {@link #allHashes} by the
    // histogram sweep (0 when the sweep was skipped — the branches that read
    // it are gated by the same MIN_PROMOTION_KEYS check).
    int packedSize = 0;
    // Index-view sizes, parallel to {@link #candidateIdx}
    // {@link #incumbentIdx} / {@link #newcomerIdx} — method-level because
    // each view is filled in one block and consumed in another (the
    // overflow sweep fills the split, the promotion scan consumes it).
    int candSize = 0;
    int incSize = 0;
    int newSize = 0;
    // Active hot-set size after this tide's halving decay (see the decay
    // gate below); 0 on sub-minimum tides, where the scan that consumes it
    // is skipped.
    int remain = 0;

    if (distinctKeys >= MIN_PROMOTION_KEYS) {
      // One pass over the snapshot: the log2-bucket histogram AND the
      // promotion candidates (keys at or above the floor — the promote
      // condition v >= max(floor, boundary) always implies v >= floor,
      // so the candidate list is an exact filter for the second pass).
      // The second pass then iterates the candidate list instead of the
      // full snapshot; the governor's blocked-keys signal is derived
      // from the candidate histogram (histogram[b] − candidateHistogram[b],
      // see the field doc) instead of a full sweep.  Every entry's (value,
      // hash) is packed into {@link #allValues} / {@link #allHashes} in the
      // same sweep — the saturated branch reuses them instead of re-iterating
      // the snapshot and re-avalanching every key.  The pass now runs
      // BEFORE decayCounts (the incumbent capture below needs the
      // pre-decay beacon state); on gate-failing (saturated) tides it is
      // wasted work — bounded, see the class doc.
      Arrays.fill(histogram, 0);
      Arrays.fill(candidateHistogram, 0);

      // (packed snapshot): ensure the packed (value, hash) arrays cover the
      // snapshot before the sweep — every entry is packed (the saturated
      // branch needs v <= 0 members too, see {@link #allValues}), so the
      // capacity is the distinct-key count, grown by doubling like
      // {@link #candidateIdx}.
      if (allValues.length < distinctKeys) {
        int grown = allValues.length;
        while (grown < distinctKeys) {
          grown <<= 1;
        }

        allValues = Arrays.copyOf(allValues, grown);
        allHashes = Arrays.copyOf(allHashes, grown);
      }
      // The candidate view can hold at most one index per snapshot entry.
      // Pre-size it once to distinctKeys so the sweep never pays repeated
      // doubling copies on high-cardinality tides; the memory is already
      // committed by allValues/allHashes at the same cardinality.
      if (candidateIdx.length < distinctKeys) {
        candidateIdx = Arrays.copyOf(candidateIdx, distinctKeys);
      }

      for (Map.Entry<String, Long> e : snapshot.entrySet()) {
        long v = e.getValue();
        // One avalanche per key, cached for the packed arrays AND the
        // candidate index view (see {@link #candidateIdx}) — never computed
        // twice for the same key.  Packed BEFORE the floor filter: the
        // saturated branch counts v <= 0 members (the stale-squatting
        // signal) and needs their hashes too.
        int h = mixHash(e.getKey().hashCode());
        allValues[packedSize] = v;
        allHashes[packedSize] = h;
        packedSize++;
        if (v <= 0) {
          continue;
        }

        snapshotSum += v;

        int bucket = (64 - Long.numberOfLeadingZeros(v)) - 1;
        if (bucket >= HISTOGRAM_BUCKETS) {
          bucket = HISTOGRAM_BUCKETS - 1;
        }

        histogram[bucket]++;
        if (v >= threshold) {
          if (candidateIdx.length == candSize) {
            candidateIdx = Arrays.copyOf(candidateIdx, candidateIdx.length << 1);
          }
          candidateIdx[candSize++] = packedSize - 1;
          candidateHistogram[bucket]++;
        }
      }

      long accumulated = 0;
      long above = 0;
      for (int i = HISTOGRAM_BUCKETS - 1; i >= 0 && accumulated < hotLimit; i--) {
        if (histogram[i] > 0) {
          above = accumulated;
          accumulated += histogram[i];
          boundaryBucket = i;
          // Clamp the shift at 62: a bucket-63 count (>= 2^63 per cycle)
          // would shift the boundary negative and make the promote test
          // admit the whole snapshot — unreachable in practice, kept
          // sane anyway.
          boundary = 1L << Math.min(i, HISTOGRAM_BUCKETS - 2);
          threshold = Math.max(floor, boundary);
        }
      }

      // (density refinement): the boundary bucket (counts in
      // [2^b, 2^(b+1))) can hold far more keys than the remaining
      // hotLimit slots — the log2 boundary is a power of two, so
      // without refinement the scan would pick the winners in HashMap
      // iteration order (arbitrary under a flat distribution).  A linear
      // sub-histogram over the bucket refines the boundary to the actual
      // ~k-th largest count (64 sub-buckets = single-count resolution
      // below 64).  Skipped when the floor dominates the boundary (noise
      // traffic — the floor does the selection) or when the bucket fits.
      // Overflow beyond the remaining slots sets the incumbent-first
      // gate: with a tie-band wider than the capacity, renewals are
      // ordered before newcomers so the capacity break never cuts a
      // renewing key (stable hot set under flat distributions).
      int need = (int) (hotLimit - above);
      if (boundaryBucket >= 0 && histogram[boundaryBucket] > need && floor <= boundary) {
        long low = boundary;
        Arrays.fill(subHistogram, 0);
        // (incumbent split): the pre-decay beacon state is the ONLY memory
        // of who was hot last tide — the halving decay zeroes every member's
        // evidence on a saturated set's scan tide, so post-decay membership
        // is empty.  The split therefore runs in this merged sweep, BEFORE
        // decayCounts: pass 1 re-promotes exactly the incumbents regardless
        // of the iteration order the capacity break cuts, and pass 2 fills
        // from the newcomers.  One membership test per candidate, decided
        // once pre-decay: pass 1 re-promotes every qualifying incumbent,
        // so no member survives to pass 2 — the fill pass never re-tests
        // membership.
        incSize = 0;
        newSize = 0;
        // Each split view holds at most one index per candidate, so one
        // pre-allocation to candSize removes the per-candidate doubling
        // copies on overflowing tides.
        if (incumbentIdx.length < candSize) {
          incumbentIdx = Arrays.copyOf(incumbentIdx, candSize);
        }
        if (newcomerIdx.length < candSize) {
          newcomerIdx = Arrays.copyOf(newcomerIdx, candSize);
        }

        for (int cIdx = 0; cIdx < candSize; cIdx++) {
          int packedIdx = candidateIdx[cIdx];
          long v = allValues[packedIdx];
          int h = allHashes[packedIdx];
          // Bucket-relative position of the count: 0 = below the boundary
          // bucket (v < 2^b < threshold — can never earn the refined
          // threshold, so the membership test is skipped for these), 1 =
          // in it (sub-histogram + split), > 1 = above it (always
          // qualifying — split only).
          long aboveBucket = v >>> boundaryBucket;
          if (aboveBucket == 1) {
            long off = v - low;
            // (shift form of the sub-index): `low` is structurally a power of
            // two — the estimation loop only assigns power-of-two boundaries
            // (2^b, clamped at b = 62; counts above 2^62 map to bucket 62, so
            // the boundary bucket never exceeds it), so the division
            // (off << SUB_SHIFT) / low is an exact shift: buckets below 64
            // left-shift (off < 2^b, so off << (6 - b) < 64 — no overflow),
            // buckets above 64 right-shift (off >>> (b - 6) < 64 — unsigned,
            // off >= 0).  The sub-index can never reach 64, so no overflow
            // clamp is needed.
            int subIdx =
              boundaryBucket <= SUB_SHIFT
                ? (int) (off << (SUB_SHIFT - boundaryBucket))
                : (int) (off >>> (boundaryBucket - SUB_SHIFT));
            subHistogram[subIdx]++;
          }

          if (aboveBucket >= 1) {
            if (isBeaconMember(h)) {
              if (incumbentIdx.length == incSize) {
                incumbentIdx = Arrays.copyOf(incumbentIdx, incumbentIdx.length << 1);
              }
              incumbentIdx[incSize++] = packedIdx;
            } else {
              if (newcomerIdx.length == newSize) {
                newcomerIdx = Arrays.copyOf(newcomerIdx, newcomerIdx.length << 1);
              }
              newcomerIdx[newSize++] = packedIdx;
            }
          }
        }

        long subAccum = 0;
        long refinedThreshold = low;
        for (int s = SUB_HISTOGRAM_BUCKETS - 1; s >= 0 && subAccum < need; s--) {
          if (subHistogram[s] > 0) {
            subAccum += subHistogram[s];
            // Exact lower edge of sub-bucket s: the div-mod decomposition
            // avoids the overflow of s * low (s <= 63, low <= 2^62).
            refinedThreshold =
              low +
              (long) s * (low / SUB_HISTOGRAM_BUCKETS) +
              ((long) s * (low % SUB_HISTOGRAM_BUCKETS)) / SUB_HISTOGRAM_BUCKETS;
          }
        }

        if (refinedThreshold > boundary) {
          threshold = refinedThreshold;
          boundary = refinedThreshold;
        }

        overflow = subAccum > need;
      }

      // (decay gate): the halving decay runs only when the promotion
      // scan that re-seeds evidence also runs.  On a sub-minimum
      // snapshot (distinctKeys < MIN_PROMOTION_KEYS) no scan runs, so
      // an unconditional decay would strip the whole hot set within two
      // tides with nothing to renew it (evidence 2→1→0), silently
      // routing every key down the cold path for the duration of the
      // small workload — the exact regime (few hot keys, high QPS) the
      // hot path exists for.  Evidence stays frozen on such tides,
      // exactly like empty tides (ADR-0038).
      remain = decayCounts();
    }

    if (distinctKeys >= MIN_PROMOTION_KEYS) {
      // Renewal numerator, hot-set earnings and the promoted-key count are
      // computed on EVERY promoted tide — including the saturated state
      // (remain >= hotLimit), where a full hot set whose members stopped
      // earning is the governor's squatting distress signal.  Counting
      // memberKeys POST-promotion avoids the startup artifact of a fresh
      // beacon reading zero renewal on the very tide that promotes everyone.
      int memberKeys = 0;
      long memberEarnings = 0;
      int promotedCount = 0;
      // (blocked signal via histogram arithmetic): the blocked band
      // [boundary, threshold) is non-empty only when the floor sits above
      // the histogram boundary (noise traffic — the floor does the
      // selection).  The band's keys are exactly the boundary bucket's
      // keys below the floor: the boundary bucket is the highest
      // non-empty bucket, so no other bucket overlaps the band, and
      // histogram[b] − candidateHistogram[b] counts them exactly — one
      // subtraction.
      int blockedKeys =
        boundary < threshold && boundaryBucket >= 0
          ? histogram[boundaryBucket] - candidateHistogram[boundaryBucket]
          : 0;

      if (remain < hotLimit) {
        if (overflow) {
          // (renew-first): pass 1 — every captured incumbent that still
          // earns the threshold is re-remain before any newcomer, so the
          // capacity break in pass 2 can never evict a renewing key.
          // Activations (rooms zeroed by the decay) consume the hotLimit
          // budget exactly like the single pass; a saturated set renews
          // fully and leaves pass 2 nothing to promote (stable freeze, not
          // rotation).  Fallen incumbents (v < threshold) are skipped and
          // decay out, freeing their slots for pass 2.
          for (int iIdx = 0; iIdx < incSize; iIdx++) {
            long v = allValues[incumbentIdx[iIdx]];
            if (v >= threshold) {
              memberKeys++;
              memberEarnings += v;
              promotedCount++;
              if (promoteToBeacon(allHashes[incumbentIdx[iIdx]])) {
                remain++;
              }
            }
          }
          // (fill): pass 2 — newcomers take the remaining capacity; the
          // capacity check precedes the promotion, so a set that pass 1
          // refilled cannot grow past hotLimit (the single-pass break
          // would overshoot by one here).  No membership re-test: the
          // newcomer split was decided pre-decay in the merged sweep, and
          // pass 1 re-remain every qualifying incumbent, so no key here
          // can already be a member.
          for (int nIdx = 0; nIdx < newSize; nIdx++) {
            long v = allValues[newcomerIdx[nIdx]];
            if (v >= threshold) {
              memberKeys++;
              // The capacity check PRECEDES the promotion: pass 1 may have
              // already refilled the budget (renewals are activations after
              // the decay-zeroed scan tide), so an after-the-fact break
              // would still promote one newcomer per scan tide — a key that
              // then renews forever and ratchets the hot set past hotLimit.
              if (remain >= hotLimit) {
                break;
              }
              memberEarnings += v;
              promotedCount++;
              if (promoteToBeacon(allHashes[newcomerIdx[nIdx]])) {
                remain++;
              }
            }
          }
        } else {
          for (int cIdx = 0; cIdx < candSize; cIdx++) {
            long v = allValues[candidateIdx[cIdx]];
            if (v >= threshold) {
              // Every key at the threshold is a member after this call
              // (re-promotions included) — the renewal numerator.
              memberKeys++;
              memberEarnings += v;
              promotedCount++;
              if (promoteToBeacon(allHashes[candidateIdx[cIdx]]) && ++remain >= hotLimit) {
                break;
              }
            }
          }
        }
      } else {
        // (saturated): no promotion scan runs (every slot is occupied), so
        // the renewal and earnings signals are enumerated directly.  The
        // membership test reads the POST-decay beacon — a key is a member
        // iff it was promoted or renewed within the last two tides, exactly
        // the slots that occupy the set — so stale members (whose keys no
        // longer earn the threshold) are counted as occupied slots with
        // their residual earnings, which is the squatting signal.  The
        // enumeration walks the packed arrays from the histogram sweep —
        // the pass-1 hashes are reused, one {@code mixHash} per key per
        // tide even on the saturated steady state of a full hot set.
        // The renewal numerator counts ONLY beacon members that earned the
        // threshold (ADR-0045's "active hot slots whose key earned at
        // least the threshold") — snapshot keys that are NOT members are
        // cold keys waiting for a slot, and counting them would inflate
        // renewal: with 1024 stale members and 600 new earners the set
        // would read healthy (0.586 >= 0.5) and the squatting evidence
        // (hotColdRatio) would never be reached.  The self-healing stays
        // the 2-tide decay: the stale members' slots free up, and the
        // next scan promotes the earners.
        for (int i = 0; i < packedSize; i++) {
          long v = allValues[i];
          if (isBeaconMember(allHashes[i])) {
            memberEarnings += v;
            promotedCount++;
            if (v >= threshold) {
              memberKeys++;
            }
          }
        }
      }

      // (density signal, DensityClimber-style): the hot set's earnings per
      // occupied slot vs the cold reservoir's earnings per key, computed
      // within this single sample — immune to workload phases.  A ratio
      // below 1 means the occupied hot slots earn less per slot than cold
      // keys earn per key (a frozen or stale-squatting set); the governor
      // prices its raise-walk arm on it.  Structurally >= 1 whenever the
      // boundary selects the top earners and the set is refilled, so it
      // fires only in the stale window.
      long coldKeys = distinctKeys - promotedCount;
      double hotColdRatio;
      if (coldKeys <= 0 || promotedCount == 0) {
        // No cold base or no members to measure — no under-earning signal.
        hotColdRatio = Double.MAX_VALUE;
      } else {
        double hotDensity = (double) memberEarnings / Math.max(1L, remain);
        double coldDensity = (double) (snapshotSum - memberEarnings) / coldKeys;
        hotColdRatio = coldDensity > 0 ? hotDensity / coldDensity : Double.MAX_VALUE;
      }

      moonsTidalForce.onTide(
        new TideReading(
          memberKeys / (double) Math.max(1L, remain),
          remain,
          hotLimit,
          blockedKeys,
          boundary,
          hotColdRatio
        )
      );
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
   * Arm the next tide as a one-shot task (earliest-first, coalesced).
   *
   * <p><b>Pacer semantics.</b>  If a tide is already pending and its
   * fire is still in the future and within
   * {@link #SCHEDULE_TOLERANCE_MS} of the proposed fire, the request is
   * merged (skipped); a meaningfully earlier request cancels the pending
   * future and re-arms.  Degenerate delays are clamped up to
   * {@link #EARLY_TIDE_MIN_INTERVAL_MS}.  Safe to call after
   * {@link #destroy()} — the shutdown flag short-circuits, and scheduler
   * rejections are logged.
   *
   * @param delayMs requested delay until the next tide (clamped up)
   */
  private void scheduleTide(long delayMs) {
    if ((boolean) SHUTDOWN.getVolatile(this)) {
      return;
    }

    long now = TimeSource.monotonicMillis();
    long scheduleAt = now + Math.max(delayMs, EARLY_TIDE_MIN_INTERVAL_MS);
    synchronized (tideScheduleGate) {
      if (pendingTide != null) {
        // Skip if a pending fire is still soon enough; otherwise cancel
        // the future being replaced (earliest-first).
        if (((nextFireTimeMs - now) > 0L) && !pendingTide.isDone() && maySkip(scheduleAt)) {
          return;
        }

        pendingTide.cancel(false);
      }

      ScheduledFuture<?> next;
      try {
        next = scheduler.schedule(this::tide, scheduleAt - now, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        // Scheduling failed (e.g. the executor is shutting down): restore
        // the unscheduled sentinel and report once, not per request.
        nextFireTimeMs = 0L;
        log.error("Failed to schedule WaveCounter delivery; buffered counts will be delayed.", e);
        return;
      }
      // 0-sentinel: nextFireTimeMs == 0 means "unscheduled" — never
      // commit it as a fire time (unreachable with a clamped positive
      // delay, kept as a guard for the invariant).
      nextFireTimeMs = scheduleAt == 0L ? 1L : scheduleAt;
      pendingTide = next;
    }
  }

  /**
   * Whether the proposed fire is within the coalescing tolerance of the
   * pending fire (Caffeine's {@code Pacer#maySkip}).
   *
   * @param scheduleAtMs proposed fire time (monotonic clock, ms)
   * @return {@code true} when the pending fire is soon enough
   */
  private boolean maySkip(long scheduleAtMs) {
    long delta = scheduleAtMs - nextFireTimeMs;
    return delta >= -SCHEDULE_TOLERANCE_MS;
  }

  /**
   * Request an earlier delivery from any thread (an external pressure
   * signal — e.g. a downstream consumer observing a growing backlog
   * mid-interval).  Earliest-first with coalescing: the request skips
   * when the pending tide fires within the tolerance band anyway, and
   * otherwise the pending tide is cancelled and re-armed at
   * {@link #EARLY_TIDE_MIN_INTERVAL_MS} — multiple nudges merge into at
   * most one schedule.  No-op after {@link #destroy()}.
   */
  public void nudgeTide() {
    scheduleTide(EARLY_TIDE_MIN_INTERVAL_MS);
  }

  @Override
  public void afterPropertiesSet() {
    deliveryStarted = true;
    scheduleTide(deliverIntervalMs);
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
   * — the information gap a per-op in-flight counter would close at
   * ~15ns/op on the hot path; the ns-scale window is the accepted price.
   * The window
   * is ns-scale, destroy is a shutdown path, and every REGISTERED
   * writer's data stays exact: the per-map drain signal (drainStamp) is
   * flipped during the sweep so a writer's lock-free fast add takes the
   * locked path and serializes with it instead of racing it — the only
   * residual is an add whose post-check recovery lands after destroy's
   * final snapshot, the same ns-scale class as the registration gap;
   * accepted as the documented approximation.
   */
  @Override
  @SuppressWarnings("all")
  public void destroy() {
    // Stop accepting new counts first — everything counted before this
    // moment must be delivered; everything after is dropped by design.
    SHUTDOWN.setVolatile(this, true);
    // Cancel any pending tide (never interrupt a running one): without
    // this an injected scheduler's pending tide would still fire after
    // destroy().  A concurrently RUNNING tide is not affected — its own
    // reschedule guard (the shutdown flag) stops the chain.
    synchronized (tideScheduleGate) {
      if (pendingTide != null) {
        pendingTide.cancel(false);
        pendingTide = null;
        nextFireTimeMs = 0L;
      }
    }
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
    // Merge all hot local maps (tag-driven recovery sweep — a fast add
    // racing this shutdown may have been mid-flight), then
    // rotate-and-snapshot exactly like tide() — including the cold-write
    // quiescence window.
    for (Map.Entry<Thread, Ceils> entry : hotRegistry.entrySet()) {
      Thread writer = entry.getKey();
      Ceils local = entry.getValue();
      if (writer.isAlive()) {
        reconcile(local);
      } else {
        // A dead writer can never touch its map again, so a lock-free drain
        // is exact; blocking on Ceils.lock here could hang forever because
        // Java locks are not released on thread death (tide() has the same
        // rule via drainDead()).
        ConcurrentHashMap<String, LongAdder> table;
        synchronized (reservoirGate) {
          table = reservoir;
          mergesInFlight.increment();
          if (!coldWriteSeen) {
            coldWriteSeen = true;
          }
        }
        try {
          local.drainDead(table, ebbReservoir);
        } finally {
          mergesInFlight.decrement();
        }
        hotRegistry.remove(writer, local);
        recycleCeils(local);
      }
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
  private final class Ceils {

    final String[] keys = new String[LOCAL_CAPACITY];
    /**
     * Per-slot count, atomic (see the class doc): the writer's update path
     * uses {@code getAndAdd} and the drains take the whole value with
     * {@code getAndSet(0)}, so the two are serialized per slot and a
     * "taken" slot reads exactly 0 — a racing update that lands on a
     * taken slot observes the 0 return and recovers its delta via
     * {@link #recoverZero} instead of writing into a dead slot (a
     * non-atomic read-modify-write loses exactly the delta on that
     * interleaving — ~1 count per 800k ops on the tryLockSkip stress).
     */
    final AtomicLongArray counts = new AtomicLongArray(LOCAL_CAPACITY);
    final int[] tags = new int[LOCAL_CAPACITY];
    /**
     * Occupancy bitmap: bit {@code i} set = slot {@code i}
     * claimed.  In the lock-free fast add the bit is stored AFTER the
     * slot writes (release), so a drain can only ever observe a complete
     * entry; a slot whose writer died mid-claim stays unmarked and is
     * simply never swept (the {@code counts[i] != 0} filter is kept as a
     * defense for phantom bits restored by a racing read-modify-write).
     */
    final AtomicLongArray occupied = new AtomicLongArray(LOCAL_CAPACITY >>> 6); // 4 longs
    long lastFlushMillis = TimeSource.monotonicMillis();
    int opCount;
    int size;

    /**
     * Drain signal: even = quiescent, odd = a drain/reset is in flight.
     * Written by the deliverer ({@link #tryDrainInto}) and by
     * {@link #reset()}; read by the writer's fast add to choose between
     * the lock-free and the locked path.  A plain volatile long is enough
     * (no CAS): only one thread writes it per transition.
     */
    /**
     * Drain signal: even = quiescent, odd = a drain/reset is in flight.
     * Written by the deliverer ({@link #tryDrainInto}) and by
     * {@link #reset()}; read by the writer's fast add to choose between
     * the lock-free and the locked path.  A plain volatile long is enough
     * (no CAS): only one thread writes it per transition.
     *
     * <p><b>Asymmetric access modes (ADR-0046 weak-ordering).</b>  The
     * field is managed by {@link #DRAIN_STAMP}: the writer's FIRST read
     * (the fast/slow decision and the post-check baseline) uses OPAQUE —
     * a stale baseline can only ADD spurious reconciles (a drain that
     * completed before the add started reads as "moved" — the tag-driven
     * sweep is correct and the cost is a rare locked pass), it can never
     * remove a legitimate race detection, so the ADR-0043 totality
     * argument is untouched; the POST-CHECK and all writes stay VOLATILE
     * (the post-check's total-order participation is what closes the
     * escape cycle).  The opaque first read drops the per-add acquire on
     * weak memory models (AArch64: ldar -> ldr); on x86 it is a no-op.
     */
    long drainStamp;

    /** Access handle for {@link #drainStamp} (opaque first read, volatile post-check/writes). */
    private static final VarHandle DRAIN_STAMP;

    static {
      try {
        DRAIN_STAMP = MethodHandles.lookup().findVarHandle(Ceils.class, "drainStamp", long.class);
      } catch (ReflectiveOperationException e) {
        throw new ExceptionInInitializerError(e);
      }
    }

    /**
     * Slow-path mutex.  A {@link ReentrantLock} instead of the monitor so
     * the deliverer can {@link ReentrantLock#tryLock()}: a busy writer
     * never blocks the tide — its residual is skipped for the cycle and
     * delivered by the writer's own flush-clock discharge (bounded by
     * {@code flushIntervalMs}), or by {@link #drainDead} once the writer
     * is observed dead.  The writer's FAST add does not take this lock — a
     * volatile {@link #drainStamp} read plus a post-check instead — so the
     * hot path pays ~2ns instead of the lock/unlock pair; the lock is
     * paid only when a drain is actually in flight (µs-scale, at most
     * once per writer per tide).
     */
    final ReentrantLock lock = new ReentrantLock();

    /**
     * Merge one increment into the local map.
     *
     * <p><b>Lock-free fast path.</b>  The owner thread normally owns the
     * map exclusively; the only concurrent writers are the deliverer's
     * drain and {@code clear()}'s reset, both brief.  Instead of a
     * lock/unlock pair per op, the fast add reads the {@link #drainStamp}
     * (odd = a drain is in flight → take the locked slow path), writes its
     * slot with the occupied bit stored LAST (release), and re-reads the
     * stamp: if it moved, a drain raced us — the entry is either already
     * merged (slot cleared) or still resident (the sweep passed before our
     * bit-store, or the sweep's wholesale clear wiped the bit) — in the
     * latter case the whole map is merged tag-driven via
     * {@link WaveCounter#reconcile(Ceils)}, so the count lands in the
     * current table with at most a one-tide delay.  Never loss, never
     * double count.
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
      long s = (long) DRAIN_STAMP.getOpaque(this);
      if ((s & 1) == 1) {
        // A drain/reset is in flight: take the locked path so the sweep
        // never sees a half-written entry (µs-scale, at most once per
        // tide).
        addSlow(key, delta, h);
        return;
      }

      // Open addressing with linear probing: `tag` is the key's hash
      // (0 mapped away, since 0 marks an empty slot); the probe starts at
      // the hashed slot and walks forward, wrapping via the power-of-two
      // mask.  At most opMaxCount entries are live, so a free slot is
      // always reached within the 256-slot table.
      int tag = h == 0 ? Integer.MIN_VALUE : h;
      int i = h & (LOCAL_CAPACITY - 1);
      for (;;) {
        if (tags[i] == 0) {
          // empty slot: claim it — slot writes first, then the release
          // bit-store, so a drain that observes the bit observes a
          // complete entry.
          tags[i] = tag;
          keys[i] = key;
          counts.setRelease(i, delta);
          long w = i >>> 6;
          long shift = 1L << (i & 63);
          occupied.setRelease((int) w, occupied.getAcquire((int) w) | shift);
          size++;
          opCount++;
          break;
        }

        if (tags[i] == tag && keys[i] != null && key.equals(keys[i])) {
          // existing entry for this key: accumulate and done.  Atomic add:
          // a concurrent drain's getAndSet either took the slot BEFORE us
          // (prev == 0 → the whole prior value is already in the shared
          // table and only our delta is resident → recover it exactly via
          // {@link #recoverZero}) or AFTER us (it takes our delta with
          // the rest — nothing to do).  Never lost, never double.
          long prev = counts.getAndAdd(i, delta);
          opCount++;
          if (prev == 0) {
            recoverZero(i, key);
          }

          return;
        }
        // occupied by a different key (hash collision): probe next slot
        i = (i + 1) & (LOCAL_CAPACITY - 1);
      }
      // (post-check): a drain ran while this add was in flight.  If our
      // entry is still resident, the sweep either missed it (bit stored
      // after its pass) or wiped its bit (the wholesale clear) — merge
      // the whole map tag-driven so the count lands in the current table.
      // If the sweep merged it, the slot is cleared and there is nothing
      // to do.  Bounded one-tide delay, never loss, never double.
      if ((long) DRAIN_STAMP.getVolatile(this) != s && tags[i] == tag && keys[i] != null && key.equals(keys[i])) {
        WaveCounter.this.reconcile(this);
      }
    }

    /**
     * Locked fallback of {@link #add} for the drain-in-flight window:
     * serialized against the sweep, so the slot writes can never tear.
     */
    private void addSlow(String key, long delta, int h) {
      lock.lock();
      try {
        int tag = h == 0 ? Integer.MIN_VALUE : h;
        int i = h & (LOCAL_CAPACITY - 1);
        for (;;) {
          if (tags[i] == 0) {
            tags[i] = tag;
            keys[i] = key;
            counts.setRelease(i, delta);

            int w = i >>> 6;
            long shift = 1L << (i & 63);
            occupied.setRelease(w, occupied.getAcquire(w) | shift);
            size++;
            opCount++;
            return;
          }
          if (tags[i] == tag && keys[i] != null && key.equals(keys[i])) {
            // Locked: no drain can be mid-take, so the plain atomic add
            // cannot observe the 0-return recovery path.
            counts.addAndGet(i, delta);
            opCount++;
            return;
          }
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
     * cannot see.
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
        lastFlushMillis = TimeSource.monotonicMillis();
      } finally {
        lock.unlock();
      }
    }

    /**
     * Non-blocking drain for the periodic tide: {@link ReentrantLock#tryLock()}
     * — if the writer holds the map mid-add, the tide skips this writer's
     * residual for this cycle instead of blocking.  Skipped data is not
     * lost: the writer's own flush-clock discharge (every add re-checks
     * {@code lastFlushMillis}) moves it into the shared table within
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
        // (drain signal): set the stamp odd BEFORE the sweep so the
        // writer's fast adds take the locked path instead of writing
        // concurrently; restore it to a fresh even epoch after.
        long s = (long) DRAIN_STAMP.getVolatile(this);
        DRAIN_STAMP.setVolatile(this, s | 1);
        try {
          mergesInFlight.increment();
          try {
            waveTo(table, ebb);
            size = 0;
            lastFlushMillis = TimeSource.monotonicMillis();
          } finally {
            mergesInFlight.decrement();
          }
        } finally {
          DRAIN_STAMP.setVolatile(this, s + 2);
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
     * {@code counts[i] != 0} filter skips (and which the sweep
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
      lastFlushMillis = TimeSource.monotonicMillis();
    }

    /**
     * Tag-driven full sweep: merge every resident entry into the shared
     * table and reset the map.  Used by the writer's post-check in
     * {@link #add} after a racing drain — the drain's sweep may have
     * passed a slot before it was written, so the bit-driven
     * {@link #waveTo} would miss such entries; this sweep scans the tags
     * instead.  Locked: serialized against the deliverer's drains and the
     * batch discharge.  ~256 tag reads, paid at most once per writer per
     * racing tide.
     *
     * <p><b>Drain signal.</b>  The stamp is flipped odd under the lock,
     * like {@link #tryDrainInto}: {@link WaveCounter#destroy()} calls this
     * cross-thread, and a writer's lock-free fast add must take the
     * locked path instead of racing the sweep — a claim landing after the
     * sweep passed its slot would otherwise escape with no stamp change to
     * trigger the add's post-check recovery.  Same-thread callers (the
     * add's own post-check) are unaffected: the flip only steers
     * concurrent adds.
     */
    public void reconcile(ConcurrentHashMap<String, LongAdder> table, ConcurrentHashMap<String, LongAdder> ebb) {
      lock.lock();
      try {
        // (drain signal): flip the stamp odd so concurrent fast adds take
        // addSlow and serialize with this sweep (see the method Javadoc).
        long s = (long) DRAIN_STAMP.getVolatile(this);
        DRAIN_STAMP.setVolatile(this, s | 1);
        try {
          for (int i = 0; i < LOCAL_CAPACITY; i++) {
            if (tags[i] != 0) {
              // Atomic take (same protocol as waveTo): the reconcile holds
              // the lock so no drain runs concurrently, but a 0 take still
              // means the value is already in the shared table — skip.
              long v = counts.getAndSet(i, 0);
              if (v != 0) {
                mergeKey(table, ebb, keys[i], v, false);
                tags[i] = 0;
                keys[i] = null;
                clearBit(i >>> 6, i & 63);
              }
            }
          }

          size = 0;
          lastFlushMillis = TimeSource.monotonicMillis();
        } finally {
          DRAIN_STAMP.setVolatile(this, s + 2);
        }
      } finally {
        lock.unlock();
      }
    }

    /**
     * Recovery for a racing update that landed on a taken slot: the
     * atomic {@code getAndAdd} returned 0, so the slot's prior value is
     * already in the shared table and only this update's residual is
     * resident here.  The table reference is captured under
     * {@link WaveCounter#reservoirGate} and the in-flight slot reserved
     * (the same capture/reserve protocol as
     * {@link WaveCounter#discharge(Ceils)}), then the residual is taken
     * and merged into the shared table under the per-map lock and the
     * slot reclaimed.  The key is passed by the caller — the slot's
     * tag/key may have been cleared by the taking sweep.  A 0 take
     * means the sweep took the residual after our add — nothing left to
     * merge.  Lock order: reservoirGate → per-map lock, so no deadlock
     * with the tide paths.
     */
    private void recoverZero(int i, String key) {
      ConcurrentHashMap<String, LongAdder> table;
      synchronized (reservoirGate) {
        table = reservoir;
        mergesInFlight.increment();
        // (quiescence-gate): mark the cold-write flag atomically with the
        // capture — same argument as {@link WaveCounter#discharge(Ceils)}.
        if (!coldWriteSeen) {
          coldWriteSeen = true;
        }
      }

      try {
        lock.lock();
        try {
          long v = counts.getAndSet(i, 0);
          if (v != 0) {
            mergeKey(table, ebbReservoir, key, v, false);
          }

          tags[i] = 0;
          keys[i] = null;
          clearBit(i >>> 6, i & 63);
        } finally {
          lock.unlock();
        }
      } finally {
        mergesInFlight.decrement();
      }
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
     * the merge while the slot is still reclaimed here — a half-written
     * slot must not survive into a permanent probe-chain hazard.
     *
     * <p>Each merged key recycles its adder from the pool (the previous
     * tide's drained table, already fully summed before publication) via
     * {@link WaveCounter#stealRecycled} instead of allocating — the
     * hot-path drain shares the cold path's zero-allocation property
     * for stable key universes.
     *
     * <p>First inserts bump {@code approximateSize} exactly like the cold
     * path in {@code count()} (the {@code putIfAbsent} winner is the one
     * real insert).  The hot-path drains — the first merge of a promoted
     * key into the fresh table — are first inserts too: without the bump
     * the capacity guard would under-count the table by the resident hot
     * keys and weaken the cold cap by up to {@code hotLimit} keys.  The
     * race with the tide's {@code set(0)}
     * reset is the same documented swap race as the cold path (a drain
     * that captured the OLD table inserts into the drained table — its
     * bump is wiped by the reset, which is exactly right, since the key
     * is no longer in the current table).
     *
     * @param table the shared accumulation table (non-null)
     * @param ebb   the adder-recycling pool (previous tide's drained table,
     *              possibly null)
     */
    private void waveTo(ConcurrentHashMap<String, LongAdder> table, ConcurrentHashMap<String, LongAdder> ebb) {
      boolean markedQuiescence = false;
      for (int w = 0; w < occupied.length(); w++) {
        long bits = occupied.getAcquire(w);

        while (bits != 0) {
          int j = Long.numberOfTrailingZeros(bits);
          int i = (w << 6) + j;
          // Atomic take: serialized against the writer's getAndAdd.  A 0
          // take means the slot was already consumed (or is a phantom
          // mark) — skip it WITHOUT clearing the slot or the bit: a
          // racing update may land its delta right after the take, and
          // the mark is what keeps that delta visible for the next sweep
          // (or the writer's own 0-return recovery).
          long v = counts.getAndSet(i, 0);
          if (v != 0) {
            // Mark the quiescence gate once for the whole sweep, on the
            // first real merge — an empty drain must not force the tide's
            // 1ms window.  mergeKey() is called with markQuiescence=false
            // below, so this local flag is the only volatile check on the
            // hot drain path.
            if (!markedQuiescence) {
              if (!coldWriteSeen) {
                coldWriteSeen = true;
              }
              markedQuiescence = true;
            }
            // Two-phase steal + putIfAbsent (same shape as the cold path in
            // count()): waveTo targets the FRESH table (post-swap), so these
            // are mostly first-inserts — the steal usually wins and no
            // mapping closure is allocated per key per drain.
            mergeKey(table, ebb, keys[i], v, false);
            // Clear the merged slot inline: the entry's value has been
            // moved into the shared table, so the slot is reclaimed on the
            // same pass — no second sweep.  The callers re-arm the flush
            // clock (lastFlushMillis = now) after the sweep.
            tags[i] = 0;
            keys[i] = null;
            // Selective bit clear (CAS — the writer's claim may be
            // concurrently setting bits in this word): only the merged
            // slot's mark is dropped; skipped slots keep theirs.
            clearBit(w, j);
          }
          bits &= bits - 1;
        }
      }
    }

    /** Atomically clear the occupancy bit {@code j} of word {@code w}. */
    private void clearBit(int w, int j) {
      long clear = ~(1L << j);
      long cur = occupied.getAcquire(w);
      while (!occupied.compareAndSet(w, cur, cur & clear)) {
        cur = occupied.getAcquire(w);
      }
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
        // Signal fast adds to take the locked path while the map is
        // cleared, then restore a fresh even epoch.
        long s = (long) DRAIN_STAMP.getVolatile(this);
        DRAIN_STAMP.setVolatile(this, s | 1);
        try {
          for (int i = 0; i < LOCAL_CAPACITY; i++) {
            tags[i] = 0;
            keys[i] = null;
            counts.setRelease(i, 0L);
          }

          for (int w = 0; w < occupied.length(); w++) {
            occupied.setRelease(w, 0L); // drop stale claim bits
          }

          size = 0;
          lastFlushMillis = TimeSource.monotonicMillis();
        } finally {
          DRAIN_STAMP.setVolatile(this, s + 2);
        }
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
   * The immutable derived view of one promoted tide (WindowClimber's
   * {@code Reading}): the goal-metric signals {@link #promote(Map)}
   * computes from the snapshot, bundled so the governor's input contract
   * is a single typed carrier instead of six positional primitives.  The
   * record is the seam between the promotion pass and the governor: the
   * same object the deliverer constructs is the object {@code onTide}
   * consumes, so the two sides cannot drift apart (a signal added here
   * is visible in the governor's Javadoc, a signal dropped here fails
   * the compile).
   *
   * @param renewal      share of active hot slots whose key earned at
   *                     least the threshold this tide ({@code memberKeys /
   *                     max(1, remain)})
   * @param remain       active hot slots after this tide's promotion scan
   * @param hotLimit     the hot-set capacity (saturation signal)
   * @param blockedKeys  keys the floor currently excludes from the
   *                     histogram boundary (the case where the floor is
   *                     over-filtering — admitted by dropping the floor)
   * @param boundary     the pure histogram boundary (top-{@code hotLimit}
   *                     count level, before the floor)
   * @param hotColdRatio the hot set's earnings per occupied slot divided
   *                     by the cold reservoir's earnings per key, this tide
   */
  private record TideReading(
    double renewal,
    int remain,
    int hotLimit,
    int blockedKeys,
    long boundary,
    double hotColdRatio
  ) {}

  /**
   * Adaptive promotion floor (WindowClimber borrowings): the
   * floor (the minimum per-cycle count for promotion, seeded at
   * {@link #PROMOTION_FLOOR}) is a noise filter, and the only signal it
   * can act on is the health of the hot set it admits.
   *
   * <p><b>Renewal.</b>  Each promoted tide the governor measures
   * {@code renewal} = active hot slots whose key earned at least the
   * threshold this tide, divided by the active slots.  The ratio is
   * counted as keys over bit1 rooms (each {@code promoteToBeacon}
   * counts a key, each activated room counts a slot), so room
   * collisions can push it slightly above 1 — the goal-metric
   * comparisons are same-scale and unaffected.  A set below
   * {@link #RENEWAL_TARGET} (0.5) is distressed.  The signal is
   * computed on saturated tides as well (see {@code promote}): a full
   * hot set whose members stopped earning is exactly the state a scan
   * gate would hide from the governor.
   *
   * <p><b>Admit on block.</b>  Health with keys measurably blocked
   * behind the floor ({@code blockedKeys} — keys at the histogram
   * boundary but below the floor) means the floor is excluding exactly
   * the keys that would renew the hot set: the boundary admits them, the
   * floor excludes them.  The governor DROPS the floor toward the boundary
   * in one move, admitting the blocked keys so renewal recovers next tide.
   * The branch lives on the HEALTHY path: under distress the same band is
   * the stale tail of the 2-tide membership memory (keys that were hot and
   * stopped earning), which is already below the floor and self-heals by
   * decay — the drop would re-admit the pollution.  The renewal signal
   * disambiguates the two readings of {@code blockedKeys}.
   *
   * <p><b>Raise-walk (bounded up-probe).</b>  Distress with the hot set
   * under-earning the cold reservoir ({@code hotColdRatio < 1} — occupied
   * hot slots earn less per slot than cold keys earn per key, the
   * frozen-set signal) arms a bounded raise-walk: the floor steps up per
   * tide (frozen base, goal-metric verdict, budgeted undo, retry ladder).
   * The walk confirms — keeps the raised floor — only after the set holds
   * the target across the same persistence that crashes it (a single lucky
   * tide must not keep a raise) and crashes — budgeted return to the base
   * plus backoff — after persistent distress; the audit, the release walk
   * and the admit-on-block all release an over-raised floor.  Both
   * directions are evidence-based: a raise that does not recover the
   * signal is undone, never ratcheted.
   *
   * <p><b>Anchor rate memory.</b>  The goal-metric references are frozen
   * at capture: the veto anchor (the position the last confirmed walk
   * settled on, with the renewal frozen at its arm) for the veto retreat,
   * {@code walk.baseRenewal} for a release walk's crash bar.  A
   * veto or a crash fires only when the current position actually earns
   * less than the reference minus a noise-aware margin (WindowClimber's
   * anchor veto with {@code VETO_MARGIN_MIN}).
   *
   * <p><b>Noise-adaptive margin.</b>  The veto margin is priced from the
   * measured scatter of the recent renewals (a small ring buffer — the
   * WindowClimber {@code Rates} deviation): noisy workloads need a wider
   * evidence gap before a veto/undo may fire, quiet ones keep the fixed
   * margin.
   *
   * <p><b>Idle collapse.</b>  A tide that leaves the hot set completely
   * empty ({@code activeSlots == 0}) proves the floor exceeds the whole
   * distribution — nothing can qualify, so filtering is meaningless.  The
   * floor collapses to the seed immediately instead of walking back
   * through the slow audit ({@link #AUDIT_WAIT}), which a
   * fast-rotating workload can outrun.
   *
   * <p><b>Retreat.</b>  Kept as defense: if distress survives
   * {@link #VETO_STREAK} tides while the floor is above the last
   * confirmed walk's anchor and earns less than the anchor's reference
   * minus the noise margin, the floor returns to the anchor in
   * {@link #RETURN_BUDGET} budgeted strides.
   *
   * <p><b>Release.</b>  A raised floor is a ratchet: it comes back down
   * under saturation (the hot set at
   * {@link #SATURATION_FRACTION} of capacity while healthy — the
   * floor admits more), under the audit (health sustained for
   * {@link #AUDIT_WAIT} still tides steps the floor one probe
   * down), and under the admit-on-block when genuinely hot keys are
   * blocked.  The floor is clamped to
   * [{@link #PROMOTION_FLOOR}, {@link #FLOOR_MAX}].  The release
   * walk itself strides by a noise-priced law: the stride is the initial
   * probe step scaled by the smoothed (ring-mean) renewal against the
   * walk's own crash bar ({@code baseRenewal - vetoMargin()}), clamped to
   * [1, {@link #STRIDE_MAX}] — comfortably healthy sets descend
   * at up to 2x the initial step, approaching the bar the walk creeps so
   * the verdict samples the decision zone at fine granularity, and a
   * below-bar descent is bounded instead of plunging to the seed before
   * the crash verdict fires.  The bold-driver decay is retired for this
   * direction, so the release walk no longer touches the raise-owned
   * step state.
   */
  private static final class MoonsTidalForce {

    /** Tides a walk may take before it is confirmed (Caffeine's PROBE_WALK_BUDGET). */
    private static final int TIDAL_WALK_BUDGET = 16;
    /**
     * Consecutive tides a walk needs on its goal-metric side before it
     * settles, symmetric in both directions: below-target tides crash-
     * abort a walk, at-target tides confirm a raise.
     */
    private static final int TIDAL_CRASH_PERSISTENCE = 3;
    /** Retry wait after a crashed walk, in tides; doubles on repeated crashes. */
    private static final int TIDAL_BACKOFF_INITIAL = 4;
    /** Longest retry wait between walks after repeated crashes. */
    private static final int TIDAL_BACKOFF_MAX = 32;
    /**
     * Distress tides before a raise-walk may arm (a single noisy tide must
     * not arm one).  The stale tail of the 2-tide membership memory keeps
     * distress visible for at most one tide per pollution wave, so the arm
     * delay stays at its minimum and the walk's own verdict absorbs the
     * noise.
     */
    private static final int RAISE_ARM_DELAY = 1;
    /**
     * The evidence gap on the renewal goal metric before a veto or an undo
     * may fire (WindowClimber's {@code VETO_MARGIN_MIN} analog): the
     * current position must earn at least this much less than the frozen
     * reference before the machine distrusts it.  The effective margin is
     * the max of this and twice the measured renewal scatter (see
     * {@link #vetoMargin()}).
     */
    private static final double MIN_TIDAL_EVIDENCE = 0.1;
    /** Renewal ring-buffer size for the noise-adaptive veto margin. */
    private static final int LUNAR_MEMORY = 8;

    private int floor = (int) PROMOTION_FLOOR;
    private double step = STEP_INITIAL;

    private boolean retreating;
    private int retreatTarget;
    private int distressTides;
    private int retreatStepsLeft;
    /**
     * Veto anchor: the position the last confirmed walk settled on, with
     * the renewal reference frozen at its arm.  A raise-walk confirm
     * plants it at the position the raise left from (so a later veto can
     * undo the raise); a release-walk confirm plants it at the descended
     * position.  A veto retreats the floor here in budgeted strides when
     * distress survives {@link #VETO_STREAK} tides above the
     * anchor and the current renewal earns less than the reference minus
     * the noise margin.  Reset (0) by the empty-set collapse: an unplanted
     * anchor is inert — renewal is never below {@code 0 - margin}.
     */
    private int anchorFloor;
    private double anchorRenewal;
    private int auditTides;

    /** A walk in flight ({@code up} = raise, {@code down} = release), or {@code null} when parked. */
    private Walk walk;

    /**
     * Per-direction retry ledgers (Caffeine's {@code Ladder}): the raise
     * and release directions own one each, and an ending may only deepen
     * the ledger of the layer that produced it — a crashed raise must not
     * delay the corrective release (the anti-ratchet channel a failed
     * raise needs most), nor a crashed release the re-probe.
     */
    private final RetryLadder raiseLadder = new RetryLadder();
    private final RetryLadder releaseLadder = new RetryLadder();

    /** Ring buffer of the recent renewals — the noise-scatter source for {@link #vetoMargin()}. */
    private final double[] lunarMemory = new double[LUNAR_MEMORY];
    private int lunarMemoryIdx;
    private int lunarMemoryCount;

    int floor() {
      return floor;
    }

    /**
     * Advance the governor by one promoted tide.
     *
     * <p>The reading is the immutable derived view of this tide built by
     * {@link #promote(Map)} (WindowClimber's {@code Reading} pattern):
     * the goal-metric signals are bundled into one typed carrier instead
     * of six positional primitives, so the promotion pass and the
     * governor share a single documented contract.
     *
     * @param reading the tide's derived view (renewal, occupancy,
     *                saturation, blocked keys, histogram boundary and the
     *                density ratio — see {@link TideReading})
     */
    @SuppressWarnings("all")
    public void onTide(TideReading reading) {
      double renewal = reading.renewal();
      int remain = reading.remain();
      int hotLimit = reading.hotLimit();
      int blockedKeys = reading.blockedKeys();
      long boundary = reading.boundary();
      double hotColdRatio = reading.hotColdRatio();

      raiseLadder.tick();
      releaseLadder.tick();

      /**
       * Empty-set collapse: when the hot set is completely empty
       * ({@code remain == 0}) but the floor still sits above the absolute
       * seed, the floor has overshot the entire key distribution and is
       * filtering nothing.  Rather than drifting back down via the slow
       * audit (which a fast-rotating workload can outrun), the governor
       * resets its full state immediately — anchoring at the seed and
       * discarding all stale distress, audit, walk, and renewal history
       * from the previous regime.
       */
      if (remain == 0 && floor > PROMOTION_FLOOR) {
        // (collapse ladder pricing, ADR-0046): a collapse that kills an
        // in-flight walk — the raised floor outran the earners and the
        // member set decayed away — is the walk's own fault, so it is priced
        // as a failed experiment; and ANY priced ladder state (a crash/fail
        // price from a walk that already ended, e.g. during the post-verdict
        // retreat) survives the reset.  Without the price the oscillation
        // probe loop (ARM -> climb -> COLLAPSE -> ladder reset -> ARM)
        // repeats forever, the backoff throttle defeated by the reset.  A
        // genuine regime change (no walk AND no priced ladder) still gets
        // the full ladder reset.
        int savedRaiseRung = -1,
          savedRaiseLeft = -1,
          savedReleaseRung = -1,
          savedReleaseLeft = -1;
        if (walk != null) {
          RetryLadder ladder = walk.up ? raiseLadder : releaseLadder;
          ladder.fail();
        }

        if (raiseLadder.left > 0 || raiseLadder.rung > 1 || releaseLadder.left > 0 || releaseLadder.rung > 1) {
          savedRaiseRung = raiseLadder.rung;
          savedRaiseLeft = raiseLadder.left;
          savedReleaseRung = releaseLadder.rung;
          savedReleaseLeft = releaseLadder.left;
        }

        reset();
        if (savedRaiseRung > 0) {
          raiseLadder.rung = savedRaiseRung;
          raiseLadder.left = savedRaiseLeft;
          releaseLadder.rung = savedReleaseRung;
          releaseLadder.left = savedReleaseLeft;
        }
        return;
      }

      lunarMemory[lunarMemoryIdx] = renewal;
      lunarMemoryIdx = (lunarMemoryIdx + 1) % LUNAR_MEMORY;
      if (lunarMemoryCount < LUNAR_MEMORY) {
        lunarMemoryCount = Math.min(lunarMemoryCount + 1, LUNAR_MEMORY);
      }

      if (retreating) {
        int stride = Math.abs(floor - retreatTarget);
        if (stride <= 1 || retreatStepsLeft <= 0) {
          floor = retreatTarget;
          retreating = false;
          // The return is a reset to the anchor (mirrors the collapse's
          // regime change): the step has decayed through the failed probe
          // history and would otherwise leave the next arm crawling at
          // sub-stride granularity — the next probe starts fresh.
          step = STEP_INITIAL;
        } else {
          floor += (int) Math.signum(retreatTarget - floor) * Math.min(STEP_RETURN_MAX, stride / 2 + 1);
          retreatStepsLeft--;
        }
        return;
      }

      if (walk != null) {
        if (walk.up) {
          // (raise-walk): the goal metric is the health test itself.  A
          // raise confirms — keeps the raised floor — only after the set
          // holds the target across the same persistence that crashes it
          // (a single lucky tide must not keep a raise; the audit, the
          // release walk and the admit-on-block release an over-raise)
          // and crashes when distress persists through the crash
          // persistence.  The confirm plants the veto anchor at the
          // position the raise left from, so a later distress can undo a
          // raise that failed to recover the signal.  The bold driver
          // steps only while the set is still distressed.
          switch (raiseEnding(renewal)) {
            case CONFIRMED:
              anchorFloor = walk.baseFloor;
              anchorRenewal = walk.baseRenewal;
              walk = null;
              raiseLadder.reward();
              auditTides = 0;
              return;
            case CRASHED:
              undoWalk(raiseLadder, WalkEnding.CRASHED);
              return;
            case FAILED:
              undoWalk(raiseLadder, WalkEnding.FAILED);
              return;
            case WALKING:
              // The bold driver steps only while the set is still
              // distressed; at-target tides hold so the confirmation
              // streak can accumulate without moving.
              //
              // (evidence-gated step, ADR-0046): the step additionally
              // requires SOME member to still earn the threshold
              // (0 < renewal < target) AND the hot slots to still under-earn
              // the cold reservoir (hotColdRatio < 1).  A renewal of 0 means
              // the set is quiet or dead — climbing cannot help it; a ratio
              // >= 1 means the members are genuinely earning — a step would
              // push the threshold past the marginal earners and eat the
              // walk's own confirmation (the self-eating step).  The un-gated
              // step outran the earners on oscillating workloads and
              // self-inflicted the empty-set collapse; with the gates the
              // walk ends in a priced FAILED/CRASHED verdict instead of a
              // ladder-resetting collapse (validated on 400 random
              // workloads: probe burden -30%, confirms unchanged/up).
              if (renewal > 0.0 && renewal < RENEWAL_TARGET && hotColdRatio < 1.0 && floor < FLOOR_MAX) {
                computeNextRaiseStep();
              }
              return;
          }
        } else {
          // (release-walk): the verdict is the goal metric priced against the
          // anchor memory — a tide below what the base position earned minus
          // the noise-aware margin is a level test against the frozen
          // reference (WindowClimber's anchor veto).  A crash that persists
          // for PROBE_CRASH_PERSISTENCE consecutive tides undoes the walk
          // (budgeted return to the base); a walk that stays healthy for the
          // full budget confirms — the descended position is kept, becomes
          // the new veto anchor, and the ladder is rewarded.
          switch (releaseEnding(renewal)) {
            case CRASHED:
              undoWalk(releaseLadder, WalkEnding.CRASHED);
              return;
            case CONFIRMED:
              anchorFloor = floor;
              anchorRenewal = renewal;
              walk = null;
              releaseLadder.reward();
              return;
            case WALKING:
              // Stride law (smoothing in the signal): the stride is priced
              // by the RING-MEAN renewal against the walk's own crash bar
              // (baseRenewal - margin, the same reference the verdict
              // judges).  Comfortably above the bar the walk strides boldly
              // (up to the ceiling); approaching the bar the stride shrinks
              // toward 1 so the verdict samples the decision zone at fine
              // granularity; below it the walk creeps while the crash
              // persistence accumulates.  The law self-converges (gain -> 0
              // as the signal -> the bar); the bold-driver decay it
              // replaces is retired for this direction, so the release walk
              // no longer touches the raise-owned step state.
              computeNextReleaseStep();
              return;
          }
        }
      }

      if (renewal < RENEWAL_TARGET) {
        // (distress): the blocked band is the stale tail of the 2-tide
        // membership memory — its keys are already below the floor, so a
        // drop would re-admit pollution and a raise cannot reach them; the
        // tail self-heals by decay within two tides.  The only
        // evidence-based action is the bounded raise-walk, armed when the
        // hot set is genuinely under-earning the cold reservoir.
        // Movement DECAYS the audit run instead of zeroing it (Caffeine's
        // AuditClock.tick): a hard reset would let one floor move per wait
        // suppress audits forever.
        auditTides = Math.max(0, auditTides - 1);
        distressTides++;
        // The veto margin is computed only when the veto can actually
        // fire — the evidence-gap conditions are cheap, the margin is a
        // two-pass ring sweep (see {@link #vetoMargin()}): most distress
        // tides never reach it.  `renewal < anchorRenewal` is implied by
        // the margin gate (the margin is always positive) and only serves
        // as the short-circuit.
        if (
          distressTides > VETO_STREAK &&
          floor > anchorFloor &&
          renewal < anchorRenewal &&
          renewal < (anchorRenewal - vetoMargin())
        ) {
          // A raise above the last confirmed anchor failed to recover the
          // signal — retreat to the anchor on the evidence that the
          // current position earns less than the anchor's reference minus
          // the noise margin (WindowClimber's anchor veto; floor >
          // anchorFloor keeps a position at or below its anchor from
          // retreating into itself forever).
          retreatTarget = anchorFloor;
          retreatStepsLeft = RETURN_BUDGET;
          retreating = true;
          distressTides = 0;
        } else if (
          hotColdRatio < 1.0 &&
          blockedKeys > 0 &&
          floor < FLOOR_MAX &&
          !raiseLadder.isBackingOff() &&
          distressTides >= RAISE_ARM_DELAY
        ) {
          // Arm the raise-walk WITHOUT taking the first step: the base is
          // frozen at the position the experiment leaves from, and the
          // first step is taken on the next distressed tide that survives
          // the evidence gates.  This one-tide delay prevents a single
          // noisy distress sample from moving the floor before the walk
          // has produced a second sample (ADR-0046 probe hygiene).  The
          // density ratio confirms the slots are genuinely under-earning
          // before the machine spends a walk on a mixed signal.
          walk = new Walk(floor, renewal, /* up= */ true);
          distressTides = 0;
          auditTides = 0;
        }
        // else: the self-healing stale tail — no action (documented).
      } else {
        distressTides = 0;
        if (blockedKeys > 0 && floor > Math.max(PROMOTION_FLOOR, boundary)) {
          // Admit on block — only on the HEALTHY path (renewal-
          // disambiguated): blocked keys are exactly the renewing keys the
          // boundary would admit — excluding them starves the hot set.
          // Drop the floor toward the boundary so they qualify from the
          // next tide on.  Under distress the same band is the stale tail
          // and must not be admitted.  Clamped: a boundary above
          // GOVERNOR_FLOOR_MAX keeps the floor inside its documented range
          // (inert — the threshold is max(floor, boundary) anyway).
          floor = (int) Math.min(Math.max(PROMOTION_FLOOR, boundary), FLOOR_MAX);
        } else {
          boolean saturated = remain >= SATURATION_FRACTION * hotLimit;
          if ((saturated || auditTides >= AUDIT_WAIT) && floor > PROMOTION_FLOOR) {
            if (releaseLadder.isBackingOff()) {
              // Refractory: hold the position after a crashed walk — a fresh
              // release would immediately re-test the just-failed direction.
              auditTides++;
              step = STEP_INITIAL;
              return;
            }

            // Arm the release walk BEFORE the first step: the base is frozen
            // at the position the experiment leaves from.  The first step
            // uses the same stride law as the walk: at the arm the ring
            // mean is the arm renewal, so the stride is exactly the
            // initial probe step (16 = initial * margin / max(0.1, margin)),
            // deterministic and independent of the raise-owned step state.
            walk = new Walk(floor, renewal, /* up= */ false);
            computeNextReleaseStep();
            auditTides = 0;
          } else {
            auditTides++;
            step = STEP_INITIAL;
          }
        }
      }
    }

    /**
     * Full-state reset triggered when the hot set becomes empty
     * ({@code remain == 0}) while the floor is above the absolute seed.
     *
     * <p>An empty hot set proves that the floor exceeds the whole
     * distribution — no key can qualify, so filtering is meaningless.
     * The floor collapses to the seed immediately instead of walking
     * back through the slow audit, which a fast-rotating workload can
     * outrun.
     *
     * <p>All in-flight experiments are cancelled: a budgeted retreat would
     * drag the floor back toward a stale probe base over the coming tides,
     * and a crashed walk would do the same via its undo — both defeating
     * the collapse.  The distress/veto history, the audit clock, the
     * step state, the anchor, and the lunar memory (renewal ring) are
     * all reset because an empty set is a regime change.  Without this
     * wipe a stale {@code distressTides} together with a stale anchor
     * could trigger one spurious retreat after the workload recovers
     * (the anchor is recaptured by the next confirmed walk of the new
     * regime).
     *
     * <p><b>Ladder pricing on a walk-inflicted collapse (ADR-0046).</b>
     * The caller prices an in-flight walk's ladder BEFORE this reset when
     * the collapse killed that walk (the raised floor outran the earners),
     * then restores {@code rung}/{@code left} afterwards — so the backoff
     * survives the reset and throttles the oscillation probe loop.  A
     * collapse with NO walk in flight (a genuine regime change) keeps the
     * full reset documented above.
     */
    private void reset() {
      // The hot set is empty — the floor exceeds the whole distribution
      // and filters nothing.  Reset to the seed immediately; the slow
      // audit can be outrun by a fast-rotating workload.
      floor = (int) PROMOTION_FLOOR;
      // Cancel any in-flight experiment: a budgeted return would drag
      // the floor back toward its stale probe base over the coming
      // tides (oscillating around the seed, then snapping to the
      // target once returnLeft runs out), and a crashed walk would do
      // the same via its undo — both defeating the collapse.
      retreating = false;
      walk = null;
      retreatStepsLeft = 0;
      raiseLadder.reset();
      releaseLadder.reset();
      // The empty set is a regime change: the distress/veto history,
      // the audit run, the step, the anchor and the renewal ring all
      // belong to the old regime.  Reset them so the new regime starts
      // from a blank slate — a stale distressAge together with a stale
      // anchor could otherwise trigger one spurious retreat after the
      // workload recovers (the anchor is recaptured by the next
      // confirmed walk of the new regime).
      distressTides = 0;
      anchorFloor = 0;
      anchorRenewal = 0;
      auditTides = 0;
      step = STEP_INITIAL;
      retreatTarget = 0;
      lunarMemoryIdx = 0;
      lunarMemoryCount = 0;
    }

    /**
     * One bold-driver step up (raise walk): the decayed step, clamped at
     * the ceiling.  The single shared raise movement — the arm and the
     * bold driver both move the floor this way, decaying the step toward
     * convergence across the probe history.
     */
    private void computeNextRaiseStep() {
      floor = Math.min(FLOOR_MAX, floor + Math.max(1, (int) step));
      step *= STEP_DECAY;
    }

    /**
     * One noise-priced step down (release walk): the stride law, clamped
     * at the seed.  The single shared release movement — the arm and the
     * walk branch both move the floor this way, so the first step and
     * every WALKING tide are priced by the same law.
     */
    private void computeNextReleaseStep() {
      if (floor > PROMOTION_FLOOR) {
        floor = Math.max((int) PROMOTION_FLOOR, floor - releaseStride());
      }
    }

    /**
     * The evidence gap required before a veto/undo fires: the fixed
     * {@link #MIN_TIDAL_EVIDENCE}, widened by twice the measured renewal
     * scatter (WindowClimber's {@code Rates.noiseBand} pricing) so noisy
     * workloads are not hair-triggered by ordinary jitter.  With fewer
     * than two ring samples there is no measured scatter, so the gap is
     * 0 — a veto cannot fire against a single sample (the bar is then
     * the full base renewal).
     */
    private double vetoMargin() {
      if (lunarMemoryCount < 2) {
        return 0.0;
      }
      return Math.max(MIN_TIDAL_EVIDENCE, 2.0 * computeTideRenewalStats().std());
    }

    /**
     * The smoothed renewal signal and its scatter in one sweep of the
     * ring: the mean is the signal the release stride law prices against,
     * the population standard deviation is the noise source for
     * {@link #vetoMargin()}.  One shared sweep serves both consumers
     * (the previous separate mean/variance passes scanned the ring three
     * times per release stride).  An empty or single-sample ring returns
     * a zero signal and zero scatter.
     */
    private RenewalStats computeTideRenewalStats() {
      int n = lunarMemoryCount;
      double sum = 0;
      double sumSq = 0;

      for (int i = 0; i < n; i++) {
        double v = lunarMemory[i];
        sum += v;
        sumSq += v * v;
      }

      double mean = sum / n;
      double std = Math.sqrt(sumSq / n - mean * mean);
      return new RenewalStats(mean, std);
    }

    /**
     * The smoothed renewal signal (the ring mean) and its population
     * standard deviation, computed by the one shared sweep.
     */
    private record RenewalStats(double mean, double std) {}

    /**
     * The release-walk stride for the current tide, priced by the
     * smoothed renewal against the walk's own crash bar (the same
     * anchor-memory reference the verdict judges):
     * {@code clamp(round(16 * (ringMean - bar) / noise), 1, 32)} with
     * {@code bar = baseRenewal - margin} and {@code noise =
     * max(RENEWAL_VETO_MARGIN, margin)}.  At the arm the ring mean is
     * the arm renewal, so the first stride is exactly the initial probe
     * step; as the signal approaches the bar the stride self-converges
     * to 1, and a below-bar signal (the crash zone) creeps while the
     * verdict accumulates its persistence.  With no renewal history
     * (fewer than two ring samples — a walk armed on the first tide, or
     * right after the regime reset) the noise floor cannot price the
     * stride, so the walk takes the default initial step.
     */
    private int releaseStride() {
      if (lunarMemoryCount < 2) {
        return STEP_INITIAL;
      }

      RenewalStats stats = computeTideRenewalStats();
      double noise = Math.max(MIN_TIDAL_EVIDENCE, 2.0 * stats.std());
      double bar = Math.max(0.0, walk.baseRenewal - noise);

      int stride = (int) Math.round((STEP_INITIAL * (stats.mean() - bar)) / noise);
      return Math.max(1, Math.min(STRIDE_MAX, stride));
    }

    /**
     * A layer's retry ledger (Caffeine's {@code Ladder}): the refractory
     * rung that a completed, failed walk deepens, the run of consecutive
     * crash endings after which a crash stops being priced as an
     * exogenous workload shift, and the tides left to serve before the
     * same direction may arm again.  The raise and release directions own
     * one each; sharing one would let a crashed raise delay the
     * corrective release — the anti-ratchet channel a failed raise needs
     * most — and a crashed release delay the re-probe.
     */
    private static final class RetryLadder {

      /** Consecutive crash endings at which a crash stops being priced as exogenous. */
      static final int PROBE_CRASH_ESCALATION = 2;

      int rung = 1;
      int left;
      int crashStreak;

      /**
       * Records a crashed ending: the wait holds at the current rung
       * (floored at the initial backoff) — probe damage and an exogenous
       * shift are indistinguishable on one crash, so it must not be
       * priced as a failed experiment — and only a consecutive crash run
       * doubles the rung (Caffeine's {@code PROBE_CRASH_ESCALATION}).
       */
      void crash() {
        crashStreak++;
        if (crashStreak >= PROBE_CRASH_ESCALATION) {
          rung = Math.min(TIDAL_BACKOFF_MAX, Math.max(TIDAL_BACKOFF_INITIAL, rung * 2));
        }

        left = Math.max(TIDAL_BACKOFF_INITIAL, rung);
      }

      /** Records a completed, failed ending: the rung doubles and the crash run resets. */
      void fail() {
        crashStreak = 0;
        rung = Math.min(TIDAL_BACKOFF_MAX, Math.max(TIDAL_BACKOFF_INITIAL, rung * 2));
        left = rung;
      }

      /** Rewards a confirmed walk: the next arm of this direction is nearly free. */
      void reward() {
        crashStreak = 0;
        rung = 1;
      }

      /** Whether this direction's backoff is still unpaid (refractory). */
      boolean isBackingOff() {
        return left > 0;
      }

      /** Serves one tide of the backoff. */
      void tick() {
        if (left > 0) {
          left--;
        }
      }

      /** Restores the ledger to its opening state, as the collapse does. */
      void reset() {
        crashStreak = 0;
        rung = 1;
        left = 0;
      }
    }

    /**
     * How a walk ends (Caffeine's {@code ProbeEnding}): the verdict
     * methods below only DECIDE the ending — the walk branch acts on it,
     * so the decision and the mutation stay separate.
     */
    private enum WalkEnding {
      /** Below-target persistence: undo to the frozen base, deepening the layer's ladder. */
      CRASHED,
      /** Budget spent without a verdict: undo, priced as a completed, failed experiment. */
      FAILED,
      /** The walk validated its position: keep it, plant the anchor, reward the ladder. */
      CONFIRMED,
      /** No ending fired: take the next bold-driver stride. */
      WALKING,
    }

    /** Computes how a raise walk ends this tide (Caffeine's {@code probeEnding}). */
    @SuppressWarnings("java:S3358")
    private WalkEnding raiseEnding(double renewal) {
      walk.samples++;
      if (renewal >= RENEWAL_TARGET) {
        walk.crashStreak = 0;
        walk.healthyStreak++;
        return (walk.healthyStreak >= TIDAL_CRASH_PERSISTENCE) ? WalkEnding.CONFIRMED : WalkEnding.WALKING;
      } else {
        walk.healthyStreak = 0;
        walk.crashStreak++;

        return (walk.crashStreak >= TIDAL_CRASH_PERSISTENCE)
          ? WalkEnding.CRASHED
          : (walk.samples >= TIDAL_WALK_BUDGET)
            ? WalkEnding.FAILED
            : WalkEnding.WALKING;
      }
    }

    /** Computes how a release walk ends this tide (Caffeine's {@code probeEnding}). */
    private WalkEnding releaseEnding(double renewal) {
      double bar = Math.max(0.0, walk.baseRenewal - vetoMargin());
      walk.crashStreak = (renewal < bar) ? walk.crashStreak + 1 : 0;
      if (walk.crashStreak >= TIDAL_CRASH_PERSISTENCE) {
        return WalkEnding.CRASHED;
      }

      walk.samples++;
      return (walk.samples >= TIDAL_WALK_BUDGET) ? WalkEnding.CONFIRMED : WalkEnding.WALKING;
    }

    /** Undoes a walk to its frozen base: budgeted return + the layer's ladder pricing. */
    private void undoWalk(RetryLadder ladder, WalkEnding ending) {
      retreatTarget = walk.baseFloor;
      retreatStepsLeft = RETURN_BUDGET;
      retreating = true;
      walk = null;

      if (ending == WalkEnding.FAILED) {
        ladder.fail();
      } else {
        ladder.crash();
      }
    }

    /**
     * A walk in flight (Caffeine's WindowClimber {@code Walk}): the floor
     * is moved one step per tide while the goal metric stays on the
     * experiment's side.  The base (position AND renewal) is frozen at the
     * arm and is what the ending judges against: a persistent below-target
     * tide undoes the walk (budgeted return to {@link #baseFloor}), a
     * verdict of {@link #TIDAL_CRASH_PERSISTENCE} at-target tides
     * completes a raise (the position is kept and plants the veto anchor
     * at the base).
     */
    private static final class Walk {

      final int baseFloor;
      final double baseRenewal;
      final boolean up;
      int samples;
      int crashStreak;
      int healthyStreak;

      Walk(int baseFloor, double baseRenewal, boolean up) {
        this.baseFloor = baseFloor;
        this.baseRenewal = baseRenewal;
        this.up = up;
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
