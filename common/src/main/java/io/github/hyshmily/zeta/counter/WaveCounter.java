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
package io.github.hyshmily.zeta.counter;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.github.hyshmily.zeta.util.CollectionSizing;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.executor.PeriodicScheduler;
import io.github.hyshmily.zeta.util.executor.ZetaThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import javax.security.auth.Destroyable;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Per-key routing counter: aggregates high-frequency single-key increments and delivers them as
 * batched {@code Map<key, add>} snapshots, each paired with the monotonic window it accumulated
 * over (the denominator an arrival-rate controller needs).
 *
 * <p>Heat-aware routing (writer-local map for hot keys, direct shared-table increment for cold
 * keys), the exact-versus-approximate precision split, the promotion boundary and floor governor,
 * and the tide pacing are specified in {@code docs/design-decisions.md} (design decisions: counting
 * engine). This class owns the protocol: each {@code Ceils} slot has a single writer, the writer
 * publishes every increment through a monotonic sequence, a drain merges the difference against its
 * private {@code drained[]} baseline without touching the writer's slots, and the table swap is
 * serialized under {@code reservoirGate} and settled by the retired reservoir's in-flight counter.
 */

@Slf4j
@Internal
public class WaveCounter implements InitializingBean, Destroyable {

  /**
   * Tide-pacer still band (WindowClimber's {@code stableBand}
   * / AuditClock discipline): a backlog move smaller than this (5% of
   * {@link #EARLY_TIDE_THRESHOLD_KEYS}) is noise — it neither moves the
   * smoothed reference (anti-ping-pong deadband) nor counts as a burst.
   * A move within the band leaves the cadence still; movement is decayed,
   * never reset, so a single big tide cannot toggle the ramp.
   */
  private static final long PACER_STILL_BAND_KEYS = WaveCounter.EARLY_TIDE_THRESHOLD_KEYS / 20;

  /**
   * Audit-clock ceiling (WindowClimber's {@code AUDIT_WAIT_MAX} analog):
   * the audit wait may stretch to at most this many healthy tides,
   * reached only after a completed FAILED release at the deepest ladder
   * rung (2 x {@code MoonsTidalForce.TIDAL_BACKOFF_MAX}).
   */
  private static final int AUDIT_WAIT_MAX = 2 * MoonsTidalForce.TIDAL_BACKOFF_MAX;

  /**
   * Anchor stable band (WindowClimber's {@code Reading.stableBand}
   * discipline): the veto requires the floor to be measurably AWAY from
   * the last confirmed anchor — more than this many counts above
   * {@code anchorFloor}. 2% of the floor domain ({@link #FLOOR_MAX}),
   * floored at 1, so a raise that confirmed leaves the floor at least one
   * full probe step above the anchor and every legitimate veto still
   * fires; only 1-2-add jitter around the anchor is absorbed (a veto
   * would retreat to a position the floor is already at — the move
   * changes nothing and costs flicker).
   */
  private static final int ANCHOR_BAND = Math.max(1, WaveCounter.FLOOR_MAX / 50);

  /** Re-engage threshold of the quiet bypass (2x entry — hysteresis band), counts/sec. */
  private static final long QUIET_REENGAGE_RATE_PER_SEC = 2 * WaveCounter.QUIET_RATE_PER_SEC;

  /**
   * Quiescence window (1 ms) after the table swap before snapshotting: the bound on a cold writer
   * preempted between reference capture and its store.
   */
  private static final long SNAPSHOT_QUIESCENCE_NANOS = 1_000_000L;

  /**
   * Empty-tide ladder (WindowClimber's {@code Ladder}): each
   * consecutive EMPTY tide stretches the next cadence by a power of two,
   * capping at this multiple of {@code deliverIntervalMs} — idle cycles
   * stop paying the 1ms quiescence, the decay sweep and the scheduler
   * wakeup every 500ms. Any non-empty tide resets the streak (the
   * ladder's confirm). The stretch is capped at 2x so a burst is
   * detected within at most two base intervals.
   */
  private static final int EMPTY_TIDE_STRETCH_CAP_MULTIPLE = 2;

  /**
   * Initial capacity AND shrink floor of the deliverer-side reused scan
   * arrays ({@link #allValues}, {@link #allHashes}, {@link #candidateIdx},
   * {@link #aboveKthIdx}, {@link #splitIdx}, {@link #selectValues}). As the floor it bounds the
   * shrink in {@link #shrinkScanArrays(int)}: a workload whose steady state sits at
   * or below this size never resizes the arrays at all, so the shrink only ever reclaims the
   * overshoot of a past cardinality spike.
   */
  private static final int SCAN_ARRAY_INITIAL_CAPACITY = 1024;

  /**
   * Backlog threshold for adaptive delivery: a tide that delivered this many
   * distinct keys (≈20% of the reporter's 100k capacity cap) re-schedules at
   * {@link #EARLY_TIDE_MIN_INTERVAL_MS}; below it the delay scales linearly
   * back to {@code deliverIntervalMs}.
   */
  private static final int EARLY_TIDE_THRESHOLD_KEYS = 20_000;

  /** Default delivery cadence for the shared snapshot, in milliseconds. */
  public static final long DEFAULT_DELIVER_INTERVAL_MS = 500;

  /**
   * Upper bound for pre-sizing the fresh shared table at each tide swap.
   *
   * <p>
   * The new table is sized from the cycle's approximate distinct-key
   * add so high-cardinality workloads avoid repeated {@link
   * ConcurrentHashMap} resizes. The cap keeps a pathological burst from
   * pinning a needlessly large empty table; beyond this the table grows on demand.
   */
  private static final int MAX_RESERVOIR_PREALLOC = 1 << 20;

  /**
   * Adaptive-delivery floor: under backlog the tide re-schedules at no less
   * than this interval, bounding burst detection latency and keeping the
   * reporter's reservoir away from its capacity cap.
   */
  private static final long EARLY_TIDE_MIN_INTERVAL_MS = 50;

  /** Power-of-two shift per consecutive empty tide (1 = at most 2x the base). */
  private static final int EMPTY_TIDE_STRETCH_MAX_SHIFT = 1;

  /**
   * Bounded spin iterations for the {@code settle-writes} wait. Hot merges
   * are µs-scale (a bounded 256-slot drainInto), so spinning briefly with
   * {@link Thread#onSpinWait()} avoids the yield syscall for the common
   * short wait; the bound guarantees a preempted writer cannot stall the
   * deliverer on a busy-spin for longer than a few µs before yielding.
   */
  private static final int SETTLE_SPIN_ITERATIONS = 1024;

  /**
   * Promotion-governor saturation fraction: the hot set is "full" at 90% of
   * {@link #hotLimit}.
   */
  private static final double SATURATION_FRACTION = 0.9;

  /**
   * Shrink hysteresis factor for the reused scan arrays: an array is
   * downsized only when it holds at least this multiple of the current
   * tide's need (see {@link #shrinkScanArrays(int)}). The band keeps
   * cardinality oscillation from resizing on every tide — an array must
   * drift to 4x the working set before a copy is paid, and a rebounding
   * workload regrows into the retained slack for free.
   */
  private static final int SCAN_ARRAY_SHRINK_FACTOR = 4;

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
   * Flood volume rate (counts/sec) for the P2 stale-floor collapse
   * (design decisions: counting engine): only genuinely high-volume windows collapse instantly; a
   * high-volume quiet window under the SEED floor is the noise filter's
   * designed job (the seed floor is inert — threshold is
   * {@code max(floor, boundary)} anyway), and moderate-volume stale floors
   * keep the existing bounded self-healing (4-tide decay + empty-set
   * collapse).
   *
   * <p>
   * A lower gate would also fire on legitimate quiet windows at 2500-5000
   * counts/sec — a raise-walk in flight over a set that briefly reads
   * renewal 0 — flood-collapsing the walk and locking the arm on a
   * transient, not a regime change. 5000 reserves the instant collapse for
   * genuinely high-volume stale floors; moderate ones keep the bounded
   * self-healing.
   */
  private static final long FLOOD_RATE_PER_SEC = 5_000;

  /**
   * Workload-shift detection threshold (WindowClimber's
   * {@code RESTART_THRESHOLD}): a single-tide move of the goal metric
   * (renewal) at or above this announces a regime change — the governor
   * re-learns its references from here instead of smoothing across the
   * shift. The renewal is a per-tide ratio (one sample per promoted
   * tide), noisier than Caffeine's request-aggregated hit rate, so the
   * parked-state stand-down is the only consumer and the walk/retreat
   * machinery — which expects renewal swings — is untouched.
   */
  private static final double RESTART_THRESHOLD = 0.05;

  /**
   * Accumulated wall time the quiet condition must hold before the bypass
   * engages, and the high-volume condition before the floor returns: a
   * quiet REGIME lasts seconds, not 1-2 tides (at the 50ms burst cadence a
   * 4-tide confirm is only 200ms — volume swings must not bounce the floor
   * between 1 and 10).
   */
  private static final long QUIET_CONFIRM_MS = 2_000;

  /**
   * Default number of DISTINCT keys a writer's local map holds before it is bulk-added
   * into the shared table (a slot-add batch trigger, not an increment add). Half of
   * {@link #LOCAL_CAPACITY} keeps the open-addressing probe from filling.
   */
  public static final int DEFAULT_MAX_OPCOUNT = 128;

  /**
   * Default maximum number of promoted hot keys. The counting beacon stays L2-resident
   * at this limit; larger hot sets are carried by the cold path without loss.
   */
  public static final int DEFAULT_HOT_LIMIT = 1024;

  /**
   * Ceiling on {@code hotLimit} (2^25 ≈ 33.5M hot keys): the beacon room
   * space is {@code hotLimit * 32} rooms, so this cap bounds it at 2^30
   * rooms (2^26 longs, 512 MiB), the sizing loop's ceiling for a
   * power-of-two mask. A math guard, not a sizing knob — no real hot set
   * approaches it.
   */
  private static final int MAX_HOT_LIMIT = 1 << 25;

  /**
   * Promotion-governor renewal target (WindowClimber
   * borrowings): the share of active hot slots whose key earned traffic
   * this tide below which the governor treats the hot set as distressed.
   * Probed on real workloads: a healthy single-key set reads 1.0, a
   * two-key set with one drifter 0.5, a fully drifted set 0.0.
   */
  private static final double RENEWAL_TARGET = 0.5;

  /**
   * Minimum snapshot size for promotion (Caffeine
   * WindowClimber's min-signal discipline): a snapshot with fewer
   * distinct keys than this carries no meaningful distribution — the
   * histogram boundary would collapse to the smallest non-zero bucket
   * and promote everything. The decay would reclaim the slots within 4
   * tides anyway, so this guards against wasted promotion work on
   * startup noise, not permanent pollution; below this size the
   * promotion pass is skipped entirely.
   */
  private static final int MIN_PROMOTION_KEYS = 16;

  /**
   * See {@link #QUIET_CONFIRM_MS}: the re-engage confirm (fast — a burst must
   * return quickly).
   */
  private static final long NOISE_CONFIRM_MS = 500;

  /**
   * Minimum snapshot distinct keys for the flood signature (tiny snapshots are
   * noise, not floods).
   */
  private static final int FLOOD_MIN_DISTINCT = 32;

  /**
   * Hard cap on concurrently REGISTERED writer maps ({@link #hotRegistry}),
   * i.e. on the number of live threads that may own a private hot map at
   * once (~7KB each ≈ 28MB at the cap).
   *
   * <p>
   * The registry is reaped once per tide and the pool is capped at
   * {@link #CEIL_POOL_CAP}, which is enough for pooled-writer deployments
   * (a bounded thread pool reuses its maps) but NOT for per-request or
   * virtual-thread deployments, where a single tide interval can create
   * tens of thousands of carriers: past the pool, every new thread would
   * allocate a map and register itself, and the whole wave stays resident
   * until the next tide — hundreds of MB of maps plus an O(threads) walk on
   * every tide. Past this cap a new writer gets no local map and counts
   * straight into the shared table (see {@link #acquireLocal()}).
   *
   * <p>
   * 4096 keeps every realistic platform thread pool (a Tomcat executor is
   * hundreds) far below the cap, so only genuine thread churn pays the
   * cold-path fallback.
   */
  private static final int MAX_HOT_WRITERS = 4096;

  /**
   * Log2-bucket add for the promotion boundary LOCATION table (see the
   * tide's promotion pass): the top-down accumulation over this histogram
   * locates the boundary bucket in O(1) expected; the boundary VALUE is
   * the exact k-th largest add selected within that bucket (design decisions: counting engine).
   * Kept as a location table only; the boundary value selection is separate.
   *
   * <p>A positive {@code long} has at most 63 significant bits, so
   * {@code 63 - numberOfLeadingZeros(v)} lands in {@code [0, 62]} — every
   * bucket is reachable and {@code 1L << bucket} stays positive.
   */
  private static final int HISTOGRAM_BUCKETS = 63;

  /**
   * EWMA weight of the per-tide volume signal (fast — the regime switches are
   * wall-clock-debounced).
   */
  private static final double VOLUME_ALPHA = 0.5;

  /**
   * Local map capacity (power of two); must be ≥ 2 × opMaxCount so probing never
   * fills it.
   */
  private static final int LOCAL_CAPACITY = 256;

  /**
   * Absolute floor for promotion: a key is never promoted
   * below this per-cycle add, regardless of the histogram boundary —
   * guards against noise keys (counting 1-9 per cycle) taking hot slots.
   * Kept low so relative hot spots at low traffic still qualify (an
   * absolute threshold silently fails for keys that carry most of a
   * low-volume cycle's traffic).
   *
   * <p>
   * This is the {@link #moonsTidalForce}'s seed and lower clamp: the promotion
   * floor is adaptive, never above {@link #FLOOR_MAX}.
   */
  private static final int PROMOTION_FLOOR = 10;

  /** Per-stride cap while returning to the probe base. */
  private static final int STEP_RETURN_MAX = 8;

  /**
   * Ceils-pool cap: the maximum number of drained per-writer hot maps
   * retained for reuse (~9KB each with the drain baseline ≈ 2.3MB at the cap). A burst of thread
   * deaths with no new writers must not pin the whole wave's maps forever;
   * the steady-state pool is one tide's dead-writer add anyway.
   */
  private static final int CEIL_POOL_CAP = 256;

  /**
   * Confirm shield (design decisions: counting engine): tides the RAISE ladder stays refractory
   * after a raise CONFIRM, so the machine cannot re-arm into an immediate
   * arm-confirm cycle — the alternating-workload ratchet (arm at F, 3
   * at-target tides, confirm at F, immediate re-arm; each cycle that
   * passes the step gates ratchets the floor up until the empty-set
   * collapse has to undo the probe). The rung is NOT touched (only the
   * reward state), so a genuine regime shift re-arms once the shield
   * expires; the release direction is unaffected (its ladder is separate).
   * 8 keeps the re-arm cycle rare without delaying a genuine shift;
   * Caffeine's park-shield analog is 32 samples, four times this.
   */
  private static final int CONFIRM_SHIELD = 8;

  /**
   * Per-tide count threshold below which the regime is "quiet" (design decisions: counting engine):
   * with a volume this small the promotion floor's noise-filtering role is
   * vacuous (there is no noise to filter), the hot set is empty, and the
   * empty slots cost nothing — the governor bypasses the floor
   * ({@link #QUIET_FLOOR} = 1) so ANY key routes hot (the fast-add path),
   * and the {@link #MIN_PROMOTION_KEYS} scan gate is lifted so sub-minimum
   * quiet snapshots still promote (the decay freeze cannot leave a stale
   * floor behind a frozen set; design decisions: counting engine). The seed floor re-engages at
   * {@link #QUIET_REENGAGE_RATE_PER_SEC} (2x the entry — hysteresis band against
   * volume hovering at the boundary).
   */
  private static final long QUIET_RATE_PER_SEC = 100;

  /**
   * Promotion-governor budgeted retreat strides (WindowClimber's veto-return).
   */
  private static final int RETURN_BUDGET = 8;

  /**
   * Descent slope-probe persistence (design decisions: counting engine): consecutive descent tides
   * the retreat must keep under-earning its OWN start (the renewal at the
   * veto arm, refreshed by the last probe sample, minus the noise margin)
   * before the descent's direction (down toward the anchor) is falsified
   * and the retreat pivots — a mini version of the veto probe's 2-tide
   * window at the descent scale (a single tide invites noise to pivot).
   */
  private static final int SLOPE_STREAK = 2;

  /**
   * Beacon decay sweep period (design decisions: counting engine): the halving decay runs once every
   * this many PROMOTED tides; on the intervening tides the evidence freezes
   * and only the active size is counted (see {@link #countActive(int)}).
   * A period of 2 extends the 2-tide memory to 4 tides — a drifted-away key
   * survives up to three quiet tides before its slots free, which is the
   * designed cost that keeps periodic earners (earn/miss/miss patterns) on
   * the hot path. The sweep is a deliverer-side phase, never a per-op cost.
   */
  private static final int DECAY_PERIOD = 2;

  /**
   * Promotion-governor initial probe step, in add units. Dual role (design
   * decisions: sketch): the upper bound (and saturation point) of the density-priced
   * raise stride (a raise below the density gate never moves more than this
   * per tide), and the first stride of a release walk (the arm's stride is
   * exactly {@code STEP_INITIAL} — see ).
   */
  private static final int STEP_INITIAL = 8;

  /**
   * Direction-probe window (design decisions: counting engine, WindowClimber's direction probing):
   * tides the veto HOLDS the current position and samples the live
   * renewal before committing to a retreat. A recovery (at or above
   * {@link #RENEWAL_TARGET}) within the window settles here and re-plants
   * the anchor — the veto's direction hypothesis (down toward the anchor)
   * is only a hypothesis; the confirm's verified claim is the
   * counter-evidence. A persistent shortfall deepens to the veto anchor. A single
   * tide would let a noise blip settle a veto; the sampled minimum
   * mirrors the walk's own 3-tide confirm discipline at the veto scale.
   */
  private static final int PROBE_STREAK = 2;

  /**
   * Release-walk stride ceiling. The release law prices the stride from
   * the smoothed renewal against the walk's own crash bar, clamped to
   * [1, {@value}]; the ceiling keeps a fully healthy set from traversing
   * the whole floor domain in a single tide. It is intentionally not
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

  /** The bypass floor of the quiet regime: promote on the boundary alone. */
  private static final int QUIET_FLOOR = 1;

  /**
   * Promotion-governor ceiling on the floor; the seed and lower clamp are
   * {@link #PROMOTION_FLOOR}.
   */
  private static final int FLOOR_MAX = 256;

  /**
   * Promotion-governor audit wait: healthy-and-still tides after which a
   * raised floor is probed one step back down (WindowClimber's
   * {@code AuditClock} re-testing a still equilibrium) — releases the
   * ratchet a distress phase left behind.
   */
  private static final int AUDIT_WAIT = 8;

  /**
   * Access handle for {@link #coldWriteSeen} (opaque read, release write, acquire
   * read).
   */
  private static final VarHandle COLD_WRITE_SEEN;

  /** Access handle for {@link #shutdown} (opaque on the count path). */
  private static final VarHandle SHUTDOWN;

  static {
    SHUTDOWN = varHandle(WaveCounter.class, "shutdown", boolean.class);
    COLD_WRITE_SEEN = varHandle(WaveCounter.class, "coldWriteSeen", boolean.class);
  }

  /**
   * Resolves a field's {@link VarHandle}; a missing field is a programming error that must
   * fail class initialization rather than surface later on the hot path.
   *
   * @param owner the class declaring the field
   * @param name  the field name
   * @param type  the field type
   * @return the resolved handle
   */
  private static VarHandle varHandle(Class<?> owner, String name, Class<?> type) {
    try {
      return MethodHandles.lookup().findVarHandle(owner, name, type);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /** Executor for the self-rescheduling tide; shut down only when {@link #ownsScheduler}. */
  private final ScheduledExecutorService scheduler;

  /**
   * Receives every delivered snapshot; invoked on the deliverer thread, or on the caller's thread
   * for the final window of {@link #destroy()}.
   */
  private final BatchConsumer batchConsumer;

  /**
   * Distinct local keys (claimed slots) before a bulk add into the shared
   * table (hot path). A slot-add trigger, not an increment add — see
   * {@link #DEFAULT_MAX_OPCOUNT}.
   */
  private final int opMaxCount;

  /**
   * Maximum number of promoted hot keys (capped; further promotions are skipped).
   */
  private final int hotLimit;

  /**
   * Writer registry for hot local maps, so the deliverer can add residual
   * data of dead writers (detected via {@link Thread#isAlive()}).
   */
  private final ConcurrentHashMap<Thread, Ceils> hotRegistry = new ConcurrentHashMap<>();

  /**
   * Ceils recycling pool: a dead writer's fully drained map is reset and returned here
   * by the tide; the first hot add of a new writer claims it instead of allocating a
   * ~7 KB map that dies with its thread. Only the deliverer pushes (dead writers'
   * maps), any writer claims, and the size is capped at {@link #CEIL_POOL_CAP}.
   */
  private final ConcurrentLinkedQueue<Ceils> ceilPool = new ConcurrentLinkedQueue<>();

  /**
   * Adaptive promotion floor: probes up through bounded raise-walks when the hot set
   * under-earns the cold reservoir, drops toward the boundary when healthy keys are
   * blocked, and retreats or audits back down. Written only by the deliverer, so no
   * synchronization. design decisions: counting engine.
   */
  private final MoonsTidalForce moonsTidalForce = new MoonsTidalForce();

  /**
   * Thread-local hot aggregation map (writer-private). The value is deliberately
   * retained across tides; the registry entry is reclaimed when the writer dies and
   * the retired thread's ThreadLocalMap is collected with it.
   *
   * <p>{@link Ceils} must stay static: a non-static map would hold
   * {@code WaveCounter.this} through a never-removed value, so the entry's weak key
   * could never clear and a destroyed counter would stay pinned for every writer
   * thread's lifetime. The owner is passed per call instead.
   */
  @SuppressWarnings("java:S5164") // retained for pooled-writer reuse; reclaimed via hotRegistry
  private final ThreadLocal<Ceils> hotLocals = new ThreadLocal<>();

  /** Size guard for {@link #ceilPool} (single pusher, so the add is exact). */
  private final AtomicInteger ceilPoolSize = new AtomicInteger();

  /**
   * Counts the cold-path drops made by the soft capacity guard — a drop path
   * of its own, separate from the reporter's dead-worker, unroutable,
   * routing-queue and dispatcher counters. At high key cardinality the
   * Worker's view under-counts by exactly the dropped tail, so the counter
   * is the operator's signal that {@code capacity} is too small for the
   * workload. Exposed through {@link #coldCapacityDrops()}; the per-drop log
   * was removed so a high-cardinality flood cannot turn DEBUG on into a hot-path cost.
   */
  private final LongAdder coldCapacityDrops = new LongAdder();

  /**
   * Holder for the single pending one-shot tide. The gate serializes the arm in
   * {@link #scheduleTide(long)} against the cancel in {@link #destroy()}; a racy double-arm
   * would run two concurrent tides.
   */
  private final Object tideScheduleGate = new Object();

  /**
   * Serializes a whole tide body against {@link #destroy()}. The single-shot chain arms the next
   * tide from inside the running body, so without this lock a tide starting after {@code destroy()}
   * entered its final drain could overlap the table swap, the promotion state and the consumer
   * callback. The lock is outer to {@code reservoirGate}, the per-map {@code Ceils.lock} and
   * {@code tideScheduleGate}, so its acquisition order cannot cycle with them.
   */
  private final ReentrantLock tideExecutionLock = new ReentrantLock();

  /**
   * Serializes the shared-table reference capture in {@link #discharge(Ceils)}
   * against the wholesale swap in {@link #tide()} — a hot add can never
   * capture the old reference while the deliverer swaps and snapshots it.
   */
  private final Object reservoirGate = new Object();

  /**
   * Pacing state for the adaptive tide cadence: EWMA-smoothed
   * backlog (fast attack, slow release, still band) plus the empty-tide
   * stretch ladder. Written only by the deliverer thread, so no
   * synchronization.
   */
  private final TidePacer pacer = new TidePacer();

  /** Bit mask for the routing beacon (power-of-two size minus one). */
  private final int beaconMask;

  /** Whether this counter created the scheduler and therefore shuts it down in {@link #destroy()}. */
  private final boolean ownsScheduler;

  /**
   * Quiescence gate: set by every real shared-table insert BEFORE the entry becomes
   * visible, cleared by the deliverer at table swap. When unset, no insert happened this
   * cycle — the old table at the swap is empty and nothing can be lost — so the tide
   * skips the 1 ms window. The store is read-gated (opaque read, then release write):
   * the flag is monotonic within a cycle, so only the first mark pays. The deliverer
   * captures and clears it with acquire/release inside the swap's gate.
   *
   * <p>Set by the cold-miss insert, every drain/reconcile merge, and the
   * gate-protected writer-side merges. A hit on an entry inserted by ANY path is a cold
   * write and needs the same window.
   */
  @SuppressWarnings("unused")
  private boolean coldWriteSeen;

  /**
   * Lifecycle flag: {@code true} after {@link #destroy()}; new {@code count()} calls
   * no-op. Read on every count, so it uses the cheapest access mode that preserves the
   * destroy contract (OPAQUE via {@link #SHUTDOWN}). The contract is eventual
   * visibility, not ordering; destroy/tide-path accesses stay volatile.
   */
  @SuppressWarnings("unused")
  private boolean shutdown;

  /** The single pending one-shot tide. */
  @GuardedBy("tideScheduleGate")
  private ScheduledFuture<?> pendingTide;

  /**
   * Monotonic time of the last table swap (the deliverer and the teardown path;
   * guarded by {@link #reservoirGate} so the swap and its timestamp are one
   * atomic step). The delta to the next swap is the window the next snapshot
   * accumulated over — the rate denominator the feed loop needs. {@code 0} =
   * no swap yet; the first window falls back to the base.
   */
  @GuardedBy("reservoirGate")
  private long lastSnapshotAtMs;

  /**
   * O(1) distinct-key counter for the cold-path capacity guard; {@code CHM.size()}
   * walks counter cells on every miss. Incremented once per real first insert into the
   * current table (cold miss and hot-path drains), reset at every table swap. A separate
   * {@link AtomicLong} keeps the high-cardinality first-insert write off the cache line holding
   * the counter's read-only header and hot path fields; opaque reads and reset, release
   * increment. Single-variable and approximate, so no cross-variable ordering is required.
   */
  private final AtomicLong approximateSize = new AtomicLong();

  /**
   * Tides until the next beacon decay sweep (design decisions: counting engine): the sweep clock
   * counts down from {@link #DECAY_PERIOD}; the tide on which it reads
   * {@code 1} runs the halving decay and the clock restarts at
   * {@link #DECAY_PERIOD}, and each intervening tide freezes the evidence
   * and only counts the active size ({@code countActive(1)}). The
   * clock is advanced by the deliverer once per promoted tide, exactly
   * where the decay gate chooses {@link #decayCounts()} vs
   * {@code countActive(1)} — tides that skip the scan (empty,
   * sub-minimum) freeze the evidence AND do not advance the clock (the
   * decay-gate freeze extends to the sweep clock; design decisions: counting engine). Starts at
   * {@code 1} so the first promoted tide after construction runs the decay.
   */
  private int sweepCountdown = 1;

  /**
   * Admission ceiling of the cold-path capacity guard: new cold keys are
   * admitted until the table reaches this many distinct keys. For a positive
   * configured capacity it is the capacity plus a 10% overshoot headroom; an
   * unbounded counter stores {@link Long#MAX_VALUE}. The guard exists to bound
   * memory, not exactness, and the pacer keeps the steady-state reservoir ~5x
   * below the cap — the headroom absorbs the remaining burst edge so the drop
   * (whose policy biases against NEW cold keys, the worst case for burst
   * detection) stays near-unreachable while the memory bound stays O(capacity).
   * Pre-computed at construction so the miss path pays no per-admission branch
   * and addition.
   */
  private final long capacityLimit;

  /**
   * Sum of the positive counts in the snapshot built by the last
   * {@link #tideWatcher()} call (deliverer-only). The snapshot build
   * dereferences every adder for {@code v.sum()} anyway, so the governor's
   * volume signal is accumulated there for free instead of a second O(n) pass
   * over the delivered map in {@link #promote(Map, long)} (the design decisions: counting engine
   * "one extra O(n) value pass" accepted cost, eliminated). Consumed only by
   * the immediately following {@code promote} of the same non-empty tide —
   * an empty-snapshot return never reaches {@code promote}, so the
   * field is written exactly where a consumer exists. The fold still happens
   * BEFORE the scan gate in {@code promote} (the volume EWMA must fold on
   * every non-empty tide, including sub-minimum ones).
   */
  private long lastSnapshotSum;

  /**
   * Previous tide's exact promotion boundary ({@code kth}), the
   * kth-shortcut filter seed (design decisions: counting engine): the sweep collects every key at
   * or above this value into {@link #aboveKthIdx}, and
   * {@link #selectBoundary} takes the exact selection over that list when
   * it provably holds this cycle's k-th largest — stable workloads skip
   * the in-bucket quickselect entirely. {@code -1} = no previous boundary
   * yet (the first scan tide pays the full path; a stale value never
   * affects correctness — it merely widens or narrows the filter, see the
   * method's Javadoc). Deliverer-only; retained across tides like the
   * governor floor (an adaptive reference, never correctness).
   */
  private long lastKth = -1;

  /**
   * Shared table plus its in-flight merge count, swapped wholesale at every delivery. The
   * {@code volatile} qualifier gives the <em>reference replacement</em> semantics (a writer
   * either sees the old reservoir or the fresh one); the table's internal state is made safe by
   * {@link ConcurrentHashMap} itself plus {@link #reservoirGate} around reference capture and
   * swap. A rotation waits on the retired reservoir's own counter, so merges into the fresh table
   * cannot extend the wait.
   */
  @SuppressWarnings("java:S3077")
  // volatile reference: replacement semantics, state guarded by CHM +
  // reservoirGate
  private volatile Reservoir reservoir = new Reservoir();

  /**
   * Whether {@link #afterPropertiesSet()} started the self-rescheduling delivery
   * chain.
   */
  private volatile boolean deliveryStarted;

  /**
   * Whether an external controller (the reporter's feed loop, apply mode) owns
   * the cadence. Set by the first {@link #adjustDeliverIntervalMs} and never
   * cleared: backlog pressure then no longer compresses the base, because the
   * loop scores its input against a rate measured over the window that input
   * produced — two controllers on one actuator cannot both own it. The
   * empty-tide ladder still applies: it fires only when there is no sample, and
   * a rate is invariant to the window it was measured over.
   */
  private volatile boolean externalCadence;

  /**
   * Cadence of shared-table snapshot delivery, clamped up to
   * {@link #EARLY_TIDE_MIN_INTERVAL_MS} at construction. {@link #scheduleTide}
   * clamps every fire to that floor anyway, so a sub-floor configuration would
   * only desynchronize the interval the loop reports from the cadence it
   * actually runs — and an unclamped {@code 0} becomes the first window's
   * length, turning a normal add into an {@code +Infinity} counts/sec rate that
   * trivially satisfies the P2 flood signature's rate gate. The config layer
   * ({@code zeta.local.report-interval-ms})
   * does not validate the value, so the clamp lives at the consumption point.
   *
   * <p>
   * Mutable through {@link #adjustDeliverIntervalMs}: the value is the
   * BASE the cadence adaptations act around — backlog pressure compresses
   * a non-empty tide's delay from this base down to the floor, the empty-tide
   * ladder stretches an idle cycle up to 2x this base, and the feed-loop
   * interval tuner (report path only) moves the base itself. The first external
   * adjustment transfers ownership ({@link #externalCadence}): the pressure
   * compression then steps aside so the applied window is exactly the caller's
   * input. Volatile: the tide loop re-reads it on every fire, so an adjustment
   * takes effect at the next scheduled tide without re-arming anything.
   */
  private volatile long deliverIntervalMs;

  /**
   * Promotion candidate index list, reused across tides instead of allocated
   * per tide (the deliverer is the only writer): packed-array indices (into
   * {@link #allValues} / {@link #allHashes}) of the keys at or above the
   * floor, collected in the histogram sweep. Cleared at the start of each
   * promotion pass and only read within the same pass, so stale entries from
   * a skipped pass are never observed.
   *
   * <p>
   * Index views instead of entry lists: the split and the promotion scan
   * need only the value (the threshold test) and the cached hash (the beacon
   * tests) — both already packed in {@link #allValues} / {@link #allHashes} —
   * never the key itself. Each value and hash lives in exactly one copy.
   *
   * <p>
   * Grown on demand by the sweep and opportunistically shrunk by
   * {@link #shrinkScanArrays(int)} after a cardinality spike (the lifecycle
   * the other views share).
   */
  private int[] candidateIdx = new int[SCAN_ARRAY_INITIAL_CAPACITY];

  /**
   * Incumbent/newcomer split for the renewal-first promotion pass (deliverer-only, reused like
   * {@link #candidateIdx}): packed-array indices (into {@link #allValues}) of the keys at or above
   * the exact cutoff, partitioned against the PRE-decay beacon membership — the only memory of who
   * was hot last tide, since the decay zeroes every member's evidence on a saturated set's decay
   * tide.
   *
   * <p>
   * One array, two fronts: incumbents are written from index {@code 0} upward and newcomers from
   * {@code viewSize - 1} downward, and the two fronts cannot cross because every qualifying key
   * lands on exactly one side. {@code PromotionPass} carries the three cursors ({@code incSize},
   * {@code newStart}, {@code newEnd}); the promotion pass walks incumbents forward and newcomers
   * in reverse, so both sides keep their snapshot order and the capacity break stays deterministic.
   * The indices reference the packed arrays — no extra key retention beyond the tide.
   */
  private int[] splitIdx = new int[SCAN_ARRAY_INITIAL_CAPACITY];

  /**
   * Kth-shortcut candidate list (design decisions: counting engine, deliverer-only, reused like
   * {@link #candidateIdx}): packed-array indices (into {@link #allValues}) of every key at or above
   * the previous tide's exact boundary ({@link #lastKth}), collected in the histogram sweep — one
   * extra comparison per snapshot entry. When the list holds at least {@link #hotLimit} keys and
   * is not disproportionately larger than the view the bucket path would scan,
   * {@link #selectBoundary} selects the exact k-th largest over THIS list instead of the
   * boundary-bucket properties: the list provably contains every key that can rank in the top
   * {@code hotLimit} (see the method's Javadoc for the lemma), so stable workloads skip the
   * in-bucket selection whose size is the bucket, not the boundary. It also serves as the scan view
   * for the incumbent split and the promotion pass (see {@code PromotionPass#aboveKthView}), so it
   * must keep its snapshot order: selection runs on a compact copy in {@link #selectValues}.
   */
  private int[] aboveKthIdx = new int[SCAN_ARRAY_INITIAL_CAPACITY];

  /**
   * Packed per-key (value, hash) snapshot arrays, reused across tides instead
   * of allocated per tide (the deliverer is the only writer). Filled in the
   * same single sweep as {@link #histogram} for EVERY snapshot entry —
   * including {@code v <= 0} members, which the floor filter below skips but
   * the saturated branch needs (a stale-squatting member reads 0 this tide),
   * so the packing happens BEFORE the filter.
   *
   * <p>
   * The saturated branch (see {@link #promote(Map, long)}) re-visits the whole
   * snapshot to add renewals and member earnings; the packed arrays give
   * it a tight two-array scan with the pass-1 hashes — one
   * {@code mixHash} per key per tide, never a re-avalanche. The values are
   * stored because the branch needs them per key (two sequential arrays read
   * in lockstep — better locality than dereferencing the snapshot's own
   * {@code Map.Entry} nodes).
   *
   * <p>
   * The packed arrays are also the data mother of the index views
   * ({@link #candidateIdx} / {@link #aboveKthIdx} / {@link #splitIdx}):
   * a view holds only packed indices, so the value and the hash are read
   * from this single copy instead of from per-view duplicates.
   *
   * <p>
   * Deliverer-only, grown by doubling like {@link #candidateIdx},
   * opportunistically shrunk by {@link #shrinkScanArrays(int)} after a
   * cardinality spike, and cleared implicitly via the pass-local
   * packed-size counter (stale tail entries are never read).
   */
  private long[] allValues = new long[SCAN_ARRAY_INITIAL_CAPACITY];

  /**
   * Compact value buffer for the boundary selection (deliverer-only, reused like
   * {@link #candidateIdx}): the exact k-th largest is selected over a copied {@code long[]} of the
   * candidate values, never over an indirect index view and never over {@link #allValues} itself.
   *
   * <p>
   * Copying is what protects the (value, hash) pairing and the snapshot order: the three-way
   * partition permutes its input, and {@link #allValues}' order drives the incumbent split and the
   * promotion pass. A compact value array also turns the selection into a sequential scan over
   * fresh longs instead of a random-access chase through an index view, and its three-way
   * post-condition yields the qualifying count directly ({@code n - lt}), replacing the old
   * second pass over the view. Grown on demand and shrunk with the other scan arrays.
   */
  private long[] selectValues = new long[SCAN_ARRAY_INITIAL_CAPACITY];

  /** Parallel to {@link #allValues}: the cached avalanched hashes. */
  private int[] allHashes = new int[SCAN_ARRAY_INITIAL_CAPACITY];

  /**
   * Log2-bucket promotion histogram — the boundary LOCATION table only
   * (design decisions: counting engine): the top-down accumulation locates the
   * boundary bucket, and the boundary value is the exact k-th largest add
   * selected within it (quickselect over the packed values, see
   * {@link #selectBoundary}). Reused across tides instead of allocated per
   * tide (the deliverer is the only writer). Zeroed with {@link Arrays#fill}
   * at the start of each promotion pass.
   */
  private final int[] histogram = new int[HISTOGRAM_BUCKETS];

  /**
   * Reused promotion-pass state (deliverer-only): one instance per counter instead of one per
   * promoted tide. The pass is a plain field holder, so reuse keeps the promotion path free of a
   * per-tide allocation and lets the phase methods read and write the same object the JIT already
   * has in hand instead of passing nine primitives through every call.
   */
  private final PromotionPass pass = new PromotionPass();

  /**
   * Promoted hot-key routing beacon: a compact 2+2 bit role-evidence array over
   * {@code hotLimit × 32} rooms (rounded to a power of two), 4 bits per room. A key is
   * a member when both role evidences are {@code >= 1}; the deliverer is the only
   * writer, and routing readers use plain array loads — the beacon is routing-only, so
   * a stale read merely routes a key cold, which is always correct.
   *
   * <p>Mechanism, decay period and the k=2 false-positive budget: design decisions:
   * counting engine.
   */
  private final long[] beacon;

  /**
   * Receives one tide snapshot together with the window it accumulated over
   * (previous swap → this swap; the base cadence before the first swap). The window — not the
   * scheduled delay — is the denominator an arrival-rate controller needs:
   * backlog pressure and the empty-tide ladder make the actual cadence differ
   * from the base.
   */
  @FunctionalInterface
  public interface BatchConsumer {
    /**
     * @param snapshot merged per-key counts (never {@code null}; empty for an
     *                 idle window; must not be mutated)
     * @param windowMs measured accumulation window in ms, positive
     */
    void accept(Map<String, Long> snapshot, long windowMs);
  }

  public WaveCounter(BatchConsumer batchConsumer) {
    this(batchConsumer, DEFAULT_MAX_OPCOUNT, DEFAULT_DELIVER_INTERVAL_MS, DEFAULT_HOT_LIMIT, true, null, 0);
  }

  public WaveCounter(BatchConsumer batchConsumer, ScheduledExecutorService scheduler) {
    this(batchConsumer, DEFAULT_MAX_OPCOUNT, DEFAULT_DELIVER_INTERVAL_MS, DEFAULT_HOT_LIMIT, false, scheduler, 0);
  }

  /**
   * Constructor that additionally wires the delivery cadence and a soft
   * capacity cap on the cold-path reservoir.
   *
   * <p>
   * {@code deliverIntervalMs} is passed through to the tide loop, so callers
   * such as {@code KeyReporter} can honor their configured report interval
   * ({@code zeta.local.report-interval-ms}, default 50ms) instead of the
   * 500ms adaptive default. A non-empty cycle stays at
   * {@code deliverIntervalMs} unless backlog pressure shortens it; the
   * empty-tide ladder may stretch it up to 2x. Values below
   * {@link #EARLY_TIDE_MIN_INTERVAL_MS} are clamped up to it — the scheduler
   * enforces the floor on every fire regardless, so a smaller setting would
   * only desynchronize the interval the governor's rate normalization sees
   * from the cadence actually scheduled.
   *
   * <p>
   * The capacity is wired as a <em>soft</em> cap on the cold-path
   * reservoir: once the shared table reaches it (plus a 10% overshoot
   * headroom, see {@link #capacityLimit}), <b>new cold</b> keys are dropped
   * — their counts are lost for this cycle — while already-tracked keys
   * keep counting. The cap is approximate (racy size check) — it bounds
   * memory, not exactness.
   *
   * @param batchConsumer     downstream consumer of merged snapshots
   * @param capacity          soft cap on distinct keys in the shared table per
   *                          delivery cycle; {@code <= 0} means unbounded
   * @param scheduler         scheduler for the periodic flusher (not shut down by
   *                          this instance)
   * @param deliverIntervalMs base delivery cadence of the tide loop
   */
  public WaveCounter(
    BatchConsumer batchConsumer,
    int capacity,
    ScheduledExecutorService scheduler,
    long deliverIntervalMs
  ) {
    this(batchConsumer, DEFAULT_MAX_OPCOUNT, deliverIntervalMs, DEFAULT_HOT_LIMIT, false, scheduler, capacity);
  }

  @SuppressWarnings("java:S107")
  // 7 constructor params: batch geometry + delivery + lifecycle, all required
  private WaveCounter(
    BatchConsumer batchConsumer,
    int opMaxCount,
    long deliverIntervalMs,
    int hotLimit,
    boolean ownsScheduler,
    ScheduledExecutorService scheduler,
    int capacity
  ) {
    Assert.notNull(batchConsumer, "batchConsumer must not be null");
    // (parameter validation): the local map's open-addressing probe must
    // never fill — the batch trigger discharges at opMaxCount, so at most
    // that many slots are claimed, and LOCAL_CAPACITY / 2 keeps the probe
    // distance bounded at a full batch (a claim for a new key past that
    // point would loop forever over occupied slots). hotLimit is capped so
    // the beacon room space (hotLimit * 32) stays at or below 2^30 rooms:
    // MAX_HOT_LIMIT and the sizing loop both cap it there, so the mask stays
    // a positive int.
    Assert.isTrue(
      opMaxCount > 0 && opMaxCount <= LOCAL_CAPACITY / 2,
      "opMaxCount must be in (0, " +
        (LOCAL_CAPACITY / 2) +
        "] so the open-addressing probe can never fill the local map"
    );
    Assert.isTrue(hotLimit >= 0 && hotLimit <= MAX_HOT_LIMIT, "hotLimit must be in [0, " + MAX_HOT_LIMIT + "]");
    this.batchConsumer = batchConsumer;
    int cap = Math.max(0, capacity);
    this.capacityLimit = cap > 0 ? cap + (cap / 10L) : Long.MAX_VALUE;
    this.opMaxCount = opMaxCount;
    // deliverIntervalMs is clamped UP to the adaptive floor: scheduleTide
    // enforces that floor on every fire regardless, and an unclamped <= 0
    // value reaches the governor's rate normalization as a division by zero
    // (see the field doc).
    this.deliverIntervalMs = Math.max(EARLY_TIDE_MIN_INTERVAL_MS, deliverIntervalMs);
    this.hotLimit = hotLimit;
    // Counting/evidence beacon, sized hotLimit × 32 rooms rounded up to a
    // power of two (~0.37% false-positive rate at full capacity; ~16 KB at
    // the default 1024 limit). The power-of-two size keeps the room index
    // a mask, and the size itself never changes, so promotion never resizes.
    long wantBits = Math.max(1L, hotLimit * 32L);
    long beaconBitCount;
    if (wantBits <= 1) {
      beaconBitCount = 1;
    } else {
      beaconBitCount = Long.highestOneBit(wantBits - 1) << 1;
      if (beaconBitCount > (1L << 30) || beaconBitCount <= 0) {
        beaconBitCount = 1L << 30;
      }
    }
    this.beaconMask = (int) (beaconBitCount - 1);
    // hotLimit == 0 would allocate a 0-length array (beaconBitCount == 1),
    // turning every add() into an ArrayIndexOutOfBoundsException on the
    // first roleGet. Clamp to one long: an empty beacon reads as 0 in
    // every room, so every key routes cold — the exact semantics of "no
    // hot set" (the promotion scan is gated by `promoted < hotLimit`, so
    // nothing is ever promoted either).
    // 16 rooms per long (4 bits per room, 2+2 roles), so the array needs
    // exactly beaconBitCount >>> 4 longs — the mask's addressable space;
    // a larger allocation would only be swept for nothing by decayCounts().
    this.beacon = new long[Math.toIntExact(Math.max(1, beaconBitCount >>> 4))]; // 4 bits per room (2+2 roles)
    this.ownsScheduler = ownsScheduler;
    // Note: a null external scheduler is allowed at construction (a counter
    // that never starts delivery legitimately uses this shape) — the
    // lifecycle contract is enforced in afterPropertiesSet(), which is the
    // single delivery-arming entry point.
    this.scheduler = ownsScheduler
      ? new PeriodicScheduler(1, new ZetaThreadFactory("zeta-hot-route-counter-flusher"))
      : scheduler;
  }

  /**
   * Record one or more accesses for the given key.
   *
   * <p>
   * <b>Routing:</b> the writer's last-key cache (a thread-private
   * pointer test, see {@link Ceils#tryFastAdd}) is consulted first — a
   * repeated add of the same key instance is fully handled without the
   * beacon — and only a miss falls through to the
   * {@link #isBeaconMember(int, int)} membership test (~3-10ns) that picks the
   * path.
   * <ul>
   * <li><b>Hot path</b> — add into the thread-local map (zero shared
   * access), and bulk-add into the shared table once the map holds
   * {@code opMaxCount} distinct keys (a slot-add batch trigger, not an
   * increment add: low-traffic residuals are drained by each tide
   * instead of on a clock). Hot keys see the
   * shared table once per batch instead of once per increment.</li>
   * <li><b>Cold path</b> — direct lock-free {@code ConcurrentHashMap}
   * write: the cheapest possible path, no local layer, no per-op
   * protection (design decisions: counting engine).</li>
   * </ul>
   *
   * @param key   the accessed key ({@code null} and empty keys are
   *              silently ignored)
   * @param delta the number of accesses (positive; non-positive deltas
   *              and empty keys are silently ignored)
   */
  public void count(String key, long delta) {
    if (key == null || key.isEmpty() || delta <= 0 || (boolean) SHUTDOWN.getOpaque(this)) {
      // destroyed: drop silently (all counts were already drained)
      return;
    }

    // (last-key cache first): the thread-private fast add precedes the
    // beacon routing — a repeated add of the SAME key instance (the common
    // hot-key pattern, see {@link Ceils#tryFastAdd}) is handled here
    // without paying the beacon's two array loads and rehash (~3-5ns). A
    // slot never changes keys except through a claim by this same writer,
    // and every claim records the cache, so a hit can only target the key's
    // own live slot. A key whose beacon evidence decayed out of the hot set
    // still lands its counts in the shared table exactly on the next drain,
    // so the stale route is bounded by that drain and never loses or
    // double-counts. Cold-only workloads pay one ThreadLocalMap miss-get
    // instead of the beacon test (a wash); hot workloads save the beacon
    // read on every repeated-key op.
    Ceils m = hotLocals.get();
    if (m == null || !m.tryFastAdd(key, delta)) {
      // The avalanched key hash is computed once and shared by the beacon
      // routing check and the hot local map — one mixHash per add instead
      // of three.
      int h = mixHash(key.hashCode());

      // A hot key is accumulated in this writer's private map; the shared
      // table sees it once per batch instead of once per increment, so N
      // writers never contend on the same shared entry.
      // {@code acquireLocal()} returns null when the writer registry is at
      // its cap (short-lived/virtual threads — see
      // {@link #MAX_HOT_WRITERS}); the key then takes the cold path, which
      // is what a thread that counts a handful of times would have cost
      // anyway (its local data has to wait for a tide regardless).
      if (isBeaconMember(h, 1) && (m != null || (m = acquireLocal()) != null)) {
        m.add(this, key, delta, h);
      } else {
        // The cheapest possible path (plain CHM increment). A cold writer
        // that captured the table reference just before the tide swap may
        // write into the old table after the snapshot; the tide/destroy 1ms
        // quiescence window bounds this to a preemption > 1ms (design decisions: counting engine).
        // The shared-table reference is captured once into a local: the miss
        // branch's merge must target the SAME table the get() probed, and
        // a second volatile read of the field could observe the tide's swap
        // in between — landing the insert in a different table than the one
        // the membership check ran against. One volatile read instead of two
        // also drops a per-miss acquire on weak memory models (AArch64:
        // ldar → ldr for the cached local); a no-op on x86 (TSO).
        ConcurrentHashMap<String, LongAdder> table = reservoir.map;
        LongAdder cell = table.get(key);
        if (cell == null) {
          // Soft capacity guard: only NEW keys at capacity are dropped —
          // keys already tracked keep counting, so the bound limits memory
          // (key cardinality) without biasing established counters. The
          // guard is paid only on the miss branch (get() returned null), so
          // the steady-state hit path is a single table lookup plus a
          // striped add. The capacity test uses the O(1) approximateSize
          // instead of ConcurrentHashMap.size() (O(cores) counter-cell reads
          // per miss); the get() above doubles as the membership check — a
          // key inserted by another thread between the two reads at the
          // capacity boundary is inside the documented "approximate (racy
          // size check)" semantics. Keeping the guard BEFORE the flag/insert
          // keeps the dominant drop path at one get + one atomic read.
          if (approximateSize.getOpaque() >= capacityLimit) {
            coldCapacityDrops.increment();
            return;
          }
          // (quiescence-gate): only a real insert marks the flag — the capacity
          // drop above returns first. The ordering (mark before visibility) and
          // the read-gating live on markColdWrite; the flag itself on coldWriteSeen.
          markColdWrite();
          LongAdder fresh = new LongAdder();
          LongAdder prev = table.putIfAbsent(key, fresh);
          if (prev == null) {
            approximateSize.getAndAdd(1L);
            fresh.add(delta);
          } else {
            prev.add(delta);
          }
        } else {
          cell.add(delta);
        }
      }
    }
  }

  /**
   * Claim (or allocate) this writer's hot local map, registering it so the
   * deliverer can merge the residual of a thread that dies.
   *
   * <p>
   * The map is taken from the dead-writer pool when available (see
   * {@link #ceilPool}) instead of allocated — per-request-thread deployments
   * would otherwise allocate a ~7KB map per request.
   *
   * <p>
   * <b>Registry cap.</b> The pool is bounded at {@link #CEIL_POOL_CAP} and
   * dead writers are only reaped once per tide, so a deployment that churns
   * writers at request rate (per-request threads, and above all virtual
   * threads — Java 17 has no {@code Thread.isVirtual()} to test for one)
   * would register tens of thousands of live {@code Thread} keys and ~7KB
   * maps inside a single tide interval, none of which can ever reach the
   * batch that justifies a private map. Past {@link #MAX_HOT_WRITERS} the
   * new writer gets nothing and its counts go straight down the cold path:
   * a thread that is about to die would have had its local data flushed by
   * the tide anyway, so the lost aggregation is nil while the memory bound
   * becomes O(MAX_HOT_WRITERS) instead of O(requests per tide).
   *
   * @return the writer's local map, or {@code null} if the registry is at
   *         its cap (the caller then takes the cold path)
   */
  private Ceils acquireLocal() {
    // Check the registry cap before polling: a pooled map must not let the registry exceed
    // MAX_HOT_WRITERS by the pool's capacity.
    if (hotRegistry.size() >= MAX_HOT_WRITERS) {
      return null;
    }
    Ceils m = ceilPool.poll();
    if (m == null) {
      m = new Ceils();
    } else {
      ceilPoolSize.decrementAndGet();
    }

    Thread writer = Thread.currentThread();
    hotLocals.set(m);
    hotRegistry.put(writer, m);
    // count() passed its shutdown check before this registration. If destroy() cleared the
    // registry in between, this entry would pin the writer thread and its map after teardown;
    // remove it here and fall back to the cold path (counts racing destroy may be dropped).
    if ((boolean) SHUTDOWN.getVolatile(this)) {
      hotRegistry.remove(writer, m);
      hotLocals.remove();
      return null;
    }
    return m;
  }

  /**
   * Caffeine's {@code FrequencySketch.spread} — hash-prospector's {@code triple32} first
   * two rounds. Spreads keys whose {@link String#hashCode()} clusters on the low bits
   * across the local map's open-addressing slots. {@link #rehash(int)} is the third
   * round, so the pair is the complete triple32.
   *
   * @param h the raw {@code String.hashCode()} value
   * @return the avalanched hash
   */
  @SuppressWarnings("java:S3398")
  private static int mixHash(int h) {
    h ^= h >>> 17;
    h *= 0xed5ad4bb;
    h ^= h >>> 11;
    h *= 0xac4c1b51;
    h ^= h >>> 15;
    return h;
  }

  /**
   * Second hash for the k=2 routing beacon: triple32's third round, applied to the
   * same avalanched hash. The multiply is a bijection and the xor-shift mixes the high
   * bits, so the derived room is independent of the first.
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
  // evidence. Each role needs only 2 bits (saturating at 3): the
  // first-promotion seed of 2 plus the halving decay (swept on every
  // other promoted tide, design decisions: counting engine) yields the 4-tide memory. The
  // deliverer is the only writer, so the read-modify-write helpers below
  // need no CAS.

  /** Field offset of the bit1-role evidence within a room (low 2 bits). */
  private static final int BIT1_OFFSET = 0;

  /** Field offset of the bit2-role evidence within a room (high 2 bits). */
  private static final int BIT2_OFFSET = 2;

  /**
   * Halving-decay shift for a 2-bit role evidence. In this
   * encoding only 1 is meaningful: a shift of 2 or more zeroes every lane
   * (values 0-3 right-shifted by >= 2), and the SWAR identity in
   * {@link #decayCounts()} — a lane's high bit moved to its low position —
   * holds exactly for shift 1, because the shifted bit must land within
   * its own 2-bit lane. A different effective decay RATE is the sweep
   * cadence, not this shift: the sweep runs on every
   * {@link #DECAY_PERIOD}-th promoted tide instead (see
   * {@link #sweepCountdown}, design decisions: counting engine), keeping this shift and its SWAR
   * identity untouched.
   */
  private static final int DECAY_SHIFT = 1;

  /**
   * Initial evidence value when a key is first promoted into the routing beacon.
   *
   * <p>
   * A seed of {@code 2} (not {@code 1}) together with the halving decay,
   * swept on every other promoted tide (design decisions: counting engine), yields a <em>four-tide</em>
   * memory window: a freshly promoted hot key is written as {@code 2}, survives
   * two quiet tides unchanged, decays to {@code 1} (still satisfies the
   * membership test), survives one more quiet tide, and decays to {@code 0} on
   * the following sweep, automatically leaving the beacon. The hysteresis
   * prevents ping-pong eviction/re-promotion across quiet cycles — up to three
   * of them — while the 4-tide bound keeps the hot set responsive to workload
   * shifts.
   *
   * <p>
   * This value is used only for path‑routing decisions; it is never added to
   * the actual per‑key counter.
   */
  private static final int INIT_COUNT = 2;
  /**
   * SWAR decay mask: the high bit of every 2-bit field
   * (positions {@code 4r+1} / {@code 4r+3}). A halving decay of a
   * 2-bit lane is exactly "move the high bit to the low position", so
   * {@code (word & this) >>> DECAY_SHIFT} decays the whole word
   * bit-parallel. Tied to {@link #DECAY_SHIFT} == 1 — the mask must
   * select the bit that lands at each lane's low position after the
   * shift.
   */
  private static final long DECAY_HIGH_BITS_MASK = 0xAAAAAAAAAAAAAAAAL;

  /**
   * SWAR active mask: the count-evidence lanes of the
   * DECAYED word (positions {@code 4r}). A room is active after decay
   * iff its original count evidence was ≥ 2 (high bit set), which lands
   * on bit {@code 4r} of {@code (word & DECAY_HIGH_BITS_MASK) >>> DECAY_SHIFT}.
   * Trace lanes (positions {@code 4r+2}) are excluded — active counts
   * rooms, not fields.
   */
  private static final long ACTIVE_LANE_MASK = 0x1111111111111111L;

  /**
   * Read the 2-bit role evidence at the given offset of the room.
   *
   * <p>
   * A plain array load (see the field doc — best-effort visibility,
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
   * Whether the key is a promoted hot key (hot-path routing check) at the
   * given evidence floor.
   *
   * <p>
   * Two independent role evidences, both required: the bit1-role
   * evidence at the first hash room AND the bit2-role evidence at the
   * second hash room, each {@code >= minEvidence}. A promoted key always
   * writes both (no false negatives); false positives need both a
   * polluted add room AND a polluted trace room (the k=2 point), and are
   * routing-only.
   *
   * <p>
   * The floor is {@code 1} on the routing path and in the promotion
   * split (any live evidence makes a member). The design decisions: counting engine
   * phase-normalized governor reading raises it to {@code 2} — the
   * decay-equivalent membership, exactly the set a halving decay would
   * leave behind (post-decay evidence {@code >= 1} {@code <=>} pre-decay
   * evidence {@code >= 2}) — so on a sweep SKIP tide the saturated
   * enumeration reads the same membership the adjacent decay tide's
   * post-decay beacon would have shown, instead of including the
   * evidence-1 lanes the frozen evidence still reports.
   *
   * @param h           the avalanched key hash ({@code mixHash(key.hashCode())}),
   *                    computed once by the caller and shared with
   *                    {@code Ceils#add}
   * @param minEvidence the minimum role evidence (1 = routing member,
   *                    2 = decay-equivalent member, design decisions: counting engine)
   */
  private boolean isBeaconMember(int h, int minEvidence) {
    int bit1 = h & beaconMask;
    if (roleGet(bit1, BIT1_OFFSET) < minEvidence) {
      return false;
    }

    int bit2 = rehash(h) & beaconMask;
    return roleGet(bit2, BIT2_OFFSET) >= minEvidence;
  }

  /**
   * Promote a key into the routing beacon (deliverer thread only).
   *
   * <p>
   * The ACTIVE hot set (rooms whose bit1-role evidence transitions
   * 0 → 2) gates the promotion scan, refreshed by the tide's halving
   * sweep — a decayed-away key frees its slot, so the hot set never
   * freezes at capacity. The deliverer is the only writer, so the
   * read-modify-write needs no CAS.
   *
   * <p>
   * Both roles are loaded, adjusted and stored back in one fused step. A saturated role
   * ({@code delta == 0}) contributes no write at all: an unconditional {@code += 0} store would
   * take the cache line exclusive on the deliverer and invalidate it for the routing readers, for
   * no state change.
   *
   * @param h the avalanched key hash ({@code mixHash(key.hashCode())}),
   *          computed once by the promotion sweep and cached — the caller
   *          never recomputes it
   * @return {@code true} if the key became newly active (add room
   *         evidence 0 → 2)
   */
  @SuppressWarnings("java:S3358")
  private boolean promoteToBeacon(int h) {
    int bit1 = h & beaconMask;
    int bit2 = rehash(h) & beaconMask;
    int idx1 = bit1 >>> 4;
    int idx2 = bit2 >>> 4;
    int shift1 = ((bit1 & 15) << 2) + BIT1_OFFSET;
    int shift2 = ((bit2 & 15) << 2) + BIT2_OFFSET;

    // When both evidences fall in the same room (1/32768 collision), the two 2-bit fields are
    // still independent — each is written to its own half of the 4-bit room, so a member's own
    // promotion always satisfies both (no false negatives).
    long w1 = beacon[idx1];
    int c = (int) ((w1 >>> shift1) & 0x3);
    // Per-field saturation guard: each role evidence is refreshed independently — a room
    // saturated (3) by OTHER keys' evidence must not block this key's own field. First promotion
    // seeds 2 (not 1): with the every-other-tide sweep (design decisions: counting engine), a
    // seed of 1 would leave the hot set after a single quiet tide; seeding 2 gives the 4-tide
    // memory (tolerates up to three quiet cycles without ping-pong).
    int delta1 = c < 3 ? (c == 0 ? INIT_COUNT : 1) : 0;

    if (idx1 == idx2) {
      int t = (int) ((w1 >>> shift2) & 0x3);
      int delta2 = t < 3 ? (t == 0 ? INIT_COUNT : 1) : 0;
      if ((delta1 | delta2) != 0) {
        beacon[idx1] = w1 + (((long) delta1 << shift1) | ((long) delta2 << shift2));
      }
    } else {
      if (delta1 != 0) {
        beacon[idx1] = w1 + ((long) delta1 << shift1);
      }
      long w2 = beacon[idx2];
      int t = (int) ((w2 >>> shift2) & 0x3);
      int delta2 = t < 3 ? (t == 0 ? INIT_COUNT : 1) : 0;
      if (delta2 != 0) {
        beacon[idx2] = w2 + ((long) delta2 << shift2);
      }
    }
    return c == 0;
  }

  /**
   * Halving decay for the role evidences, run once per
   * {@link #DECAY_PERIOD}-th promoted tide (snapshot with
   * {@code distinctKeys >= MIN_PROMOTION_KEYS}) before the promotion
   * scan — on the intervening tides the evidence freezes and only the
   * active size is counted (see {@link #countActive(int)}, design decisions: counting engine).
   * Tides without a scan skip the decay entirely —
   * empty tides never reach this method ({@link #tide()} calls
   * {@link #promote(Map, long)} only with a non-empty snapshot) and sub-minimum
   * snapshots freeze it via the decay gate in {@code promote} — because
   * halving without the re-seeding scan would strip the whole hot set
   * within two tides (evidence 2→1→0) on small workloads; see design decisions: counting engine):
   * every 2-bit field {@code >> DECAY_SHIFT}, and fields
   * that decay to zero free their slot. Both roles decay on the same
   * ladder ({@link #DECAY_SHIFT} — the per-lane halving), so a member's
   * two evidences expire in lockstep (no false negatives from one
   * outliving the other). Also returns the active hot-set size (the
   * hotLimit gate for the promotion scan).
   *
   * <p>
   * <b>SWAR.</b> In a 2-bit lane the decayed value is exactly the
   * lane's high bit moved to the low position, so the whole word decays
   * with one mask-and-shift (bit-parallel, no per-room loop), and the
   * post-decay active add is the same shifted value's lane popcount.
   * See
   * {@link #DECAY_HIGH_BITS_MASK} / {@link #ACTIVE_LANE_MASK} for the
   * lane layout.
   *
   * <p>
   * Active is counted POST-decay: a room whose add evidence was 1
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
   * Count the active rooms at the given evidence floor, WITHOUT decaying
   * (the sweep-period skip tide (design decisions: counting engine)): the evidence stays frozen for
   * this tide — exactly like the decay gate's freeze on empty/sub-minimum
   * tides (design decisions: counting engine) — and only the active hot-set size is reported (the
   * hotLimit gate and the governor's occupancy denominator, same
   * consumers as {@link #decayCounts()}'s return).
   *
   * <p>
   * The floor is {@code 1} for the promotion scan gate (any live
   * add evidence occupies a slot — the honest occupancy). The phase-normalized governor reading
   * raises it to {@code 2} (design decisions: counting engine) —
   * the decay-equivalent basis: the post-scan evidence {@code >= 2} set,
   * exactly what a halving decay + this scan would have left
   * (re-promoted evidence-1 earners add like the decay tide's
   * activations; the saturated branch's frozen beacon IS its post-scan
   * state). The gate runs BEFORE the scan and the reading AFTER it, so
   * the two floors are paid as two separate passes — a skip tide never
   * needs both at the same beacon state.
   *
   * <p>
   * <b>SWAR, branch-free.</b> A room's add lane is active at floor 1 iff its
   * 2-bit evidence is non-zero — moving the lane's bit1 down to bit0
   * ({@code word >>> 1}) and OR-ing it back makes the lane's bit0 exactly
   * the "evidence >= 1" test; at floor 2 the shifted word alone is the
   * "evidence >= 2" test (the lane's bit1). Trace lanes shift into
   * positions the {@link #ACTIVE_LANE_MASK} masks out, so the add is
   * one or-shift plus a popcount per word — the same cost shape as
   * {@link #decayCounts()}, one pass per promoted tide on the deliverer.
   * There is deliberately no per-word {@code word != 0} shortcut: at the
   * default hot limit roughly two thirds of the words are non-zero and the
   * rate drifts with occupancy, which is the branch predictor's worst band;
   * the shift-mask-popcount sequence is a few ALU ops on a known-not-taken
   * path.
   *
   * @param minEvidence the minimum add-lane evidence (1 = honest
   *                    occupancy, 2 = decay-equivalent, design decisions: counting engine)
   * @return the number of rooms with a live bit1-role evidence
   *         {@code >= minEvidence}, undecayed
   */
  private int countActive(int minEvidence) {
    int active = 0;
    if (minEvidence == 1) {
      for (long word : beacon) {
        active += Long.bitCount((word | (word >>> 1)) & ACTIVE_LANE_MASK);
      }
    } else {
      for (long word : beacon) {
        active += Long.bitCount((word >>> 1) & ACTIVE_LANE_MASK);
      }
    }
    return active;
  }

  /**
   * Return a dead writer's fully-drained local map to the pool for reuse
   * (see {@link #ceilPool}). The map is {@link Ceils#tryReset() reset} first,
   * clearing any phantom claim slots a mid-claim death left behind; a map whose
   * lock is still held (the writer died between the lock acquisition and the
   * protected block, so the finally never ran) is discarded instead of blocking
   * the deliverer forever. Only the deliverer pushes, and the size slot is
   * reserved before the offer so a concurrent {@link #acquireLocal()} poll cannot
   * drive the counter negative (or let the pool grow past its cap).
   *
   * @param m the dead writer's local map (drained and reaped)
   */
  private void recycleCeils(Ceils m) {
    if (!m.tryReset()) {
      return;
    }
    if (ceilPoolSize.incrementAndGet() > CEIL_POOL_CAP) {
      ceilPoolSize.decrementAndGet();
      return;
    }

    ceilPool.offer(m);
  }

  /**
   * Merge a positive count into the shared table, allocating the key's adder only on a real first
   * insert.
   *
   * <p>
   * The hot-path hit check is a plain {@link ConcurrentHashMap#get} before the insert: dozens of
   * writers draining the same hot key into the fresh table would otherwise each allocate a
   * {@link LongAdder}, race {@code putIfAbsent}, and pay the bin lock — the common case after a
   * swap is that the first drainer inserted the cell and every later one only adds to it. The
   * cold path performs the same {@code get} itself and therefore calls {@code putIfAbsent} directly
   * (it already knows the key is absent), so this method's pre-check costs nothing there.
   *
   * <p>
   * Not {@code computeIfAbsent}: its mapping closure would allocate on every real first insert —
   * exactly the churn-heavy case this store exists to make cheap.
   *
   * <p>
   * The caller owns the quiescence mark for an insert: the cold path marks before this call and
   * {@link Ceils#waveDeltaTo} marks once per sweep, so this primitive stays a pure merge.
   *
   * @param table the shared accumulation table (non-null)
   * @param key   the merged key
   * @param value the count to merge (positive)
   */
  private void mergeHotKey(ConcurrentHashMap<String, LongAdder> table, String key, long value) {
    LongAdder cell = table.get(key);
    if (cell != null) {
      cell.add(value);
      return;
    }

    LongAdder fresh = new LongAdder();
    LongAdder prev = table.putIfAbsent(key, fresh);
    if (prev == null) {
      approximateSize.getAndAdd(1L);
      fresh.add(value);
    } else {
      prev.add(value);
    }
  }

  /**
   * Mark the quiescence gate: the shared table received a write this
   * cycle. Read-gated (the flag is monotonic within a cycle), so only
   * the first mark of a cycle pays a store. Opaque read + release write
   * (see {@link #coldWriteSeen} field doc).
   */
  private void markColdWrite() {
    if (!(boolean) COLD_WRITE_SEEN.getOpaque(this)) {
      COLD_WRITE_SEEN.setRelease(this, true);
    }
  }

  /**
   * Capture the current reservoir, reserve an in-flight merge slot on it and mark the quiescence
   * gate — atomically, under {@link #reservoirGate}.
   *
   * <p>
   * The mutex makes the reference capture atomic against the deliverer's swap in
   * {@link #tideWatcher()}, so the returned reservoir is never one the deliverer is already
   * snapshotting. The in-flight reservation is per reservoir: a rotation waits only for merges
   * that captured the table it retired, so traffic into the fresh table can neither extend that
   * wait nor cancel an older reservation in a shared counter. The quiescence mark is taken under
   * the same gate: the entries this merge inserts may land after the swap, and cold hit-writers
   * on them need this cycle's window.
   *
   * @return the current reservoir, reserved for one in-flight merge
   */
  private Reservoir captureAndReserve() {
    synchronized (reservoirGate) {
      Reservoir current = reservoir;
      current.inFlight.incrementAndGet();
      markColdWrite();
      return current;
    }
  }

  /**
   * Bulk-add a writer's hot local map into the shared table and reset it.
   *
   * <p>
   * The per-writer mutex lives inside {@link Ceils#drainInto} (the
   * per-map {@link ReentrantLock} — never a parameter), serializing this
   * writer's add against the deliverer's drain of the same map. The
   * {@link #reservoirGate} mutex here makes the capture of the shared-table
   * reference atomic against the deliverer's wholesale swap, so an add
   * never writes into a table that is already being snapshotted. The same
   * capture marks the {@link #coldWriteSeen} quiescence flag (see the gate
   * block), so a swap racing this in-flight merge still pays the window
   * for cold hit-writers on the entries it inserts.
   */
  private void discharge(Ceils m) {
    // (capture+reserve): the mutex makes the reference capture atomic vs
    // the tide swap, and the in-flight reservation plus the quiescence
    // mark are taken atomically with it (see {@link #captureAndReserve()}).
    Reservoir captured = captureAndReserve();
    try {
      m.dischargeInto(this, captured.map);
    } finally {
      captured.inFlight.decrementAndGet();
    }
  }

  /**
   * Rotate the shared table and snapshot it: swap wholesale under
   * {@link #reservoirGate}, wait for in-flight hot merges (that captured the
   * OLD reference) and the cold-write quiescence window, then drain the old
   * table into a snapshot map.
   *
   * <p>
   * Shared by {@link #tide()} (the periodic {@code tide}) and
   * {@link #destroy()} — both must perform the identical
   * swap-and-quiesce-and-snapshot sequence so hot-path data is exact and
   * cold-path loss stays within the documented window.
   *
   * @return the snapshot and its measured window; the snapshot is empty when the
   *         old table held no counts (an idle window)
   */
  private Snapshot tideWatcher() {
    Reservoir rotated;
    boolean quiesce;
    long windowMs;
    synchronized (reservoirGate) {
      // (measure-window): captured under the same gate as the swap, so the
      // delta is the exact accumulation window of the snapshot being rotated
      // out — not the delay the loop slept (pressure and the empty ladder
      // change that) and not the previous cadence decision.
      long now = TimeSource.monotonicMillis();
      windowMs = lastSnapshotAtMs == 0L ? deliverIntervalMs : Math.max(1L, now - lastSnapshotAtMs);
      lastSnapshotAtMs = now;
      // (rotate-table): swap the shared table wholesale. New hot
      // merges and cold direct writes now target the fresh table; the
      // retired reservoir becomes read-only except for writers that captured
      // it before the swap.
      rotated = reservoir;
      int nextCapacity = (int) Math.min(approximateSize.getOpaque(), MAX_RESERVOIR_PREALLOC);
      reservoir = nextCapacity > 16 ? new Reservoir(nextCapacity) : new Reservoir();
      approximateSize.setOpaque(0L);
      // (quiescence-gate): capture and clear the cold-write flag under
      // the same mutex as the swap. A writer that marked before the swap
      // is captured here, so the window below is paid and a writer
      // preempted in the capture-to-mark gap still lands in `old` in
      // time. A mark landing after this clear arms no window this cycle
      // — the cold path writes to the table it captured and never
      // re-reads the field (see count()) — so that write targets a drained
      // `old` and is lost; the mark merely persists into the next cycle
      // and makes it pay a spare window (idempotent, harmless).
      quiesce = (boolean) COLD_WRITE_SEEN.getAcquire(this);
      COLD_WRITE_SEEN.setRelease(this, false);
    }
    // (settle-writes): quiescence — hot merges first (exact), then
    // a window for cold writers preempted between reference capture and
    // write. The window is paid ONLY when a cold writer may be in
    // flight (coldWriteSeen was set this cycle); on cycles with no cold
    // traffic it is skipped entirely, with the residual loss window
    // shrinking to the ns-scale get-to-flag gap (design decisions: counting engine).
    // Residual loss with the window paid requires a preemption > 1ms
    // (~1e-5/op measured). The window precedes the emptiness check so
    // a writer that recovers during it can still land in the snapshot.
    AtomicInteger inFlight = rotated.inFlight;
    int spin = SETTLE_SPIN_ITERATIONS;
    while (inFlight.get() != 0) {
      if (--spin >= 0) {
        Thread.onSpinWait();
      } else {
        Thread.yield();
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

    ConcurrentHashMap<String, LongAdder> old = rotated.map;
    if (old.isEmpty()) {
      // Nothing to snapshot (idle cycle, or all traffic still resident in
      // writers' local maps): the quiescence window above was still paid —
      // an empty old table cannot be told apart from "no writer in flight"
      // (the information gap), and the parked wait costs no CPU. Only the
      // snapshot build is skipped; the empty window is still delivered so a
      // rate controller can observe zero arrivals and back the cadence off.
      return new Snapshot(Map.of(), windowMs);
    }
    // (snapshot-promote): the old table is now quiescent — drain it.
    // Keys are unique (CHM + putIfAbsent), so plain put is exact and skips
    // a redundant per-key lookup. The governor's volume signal is
    // accumulated in the same pass (the adder is already dereferenced for
    // sum()) and published via {@link #lastSnapshotSum} — no second O(n)
    // pass in promote().
    // Pre-sized past the load factor: HashMap(initialCapacity) rounds DOWN
    // to the next power of two and still resizes at 0.75x of it, so the
    // raw size would rehash mid-fill on most tides.
    Map<String, Long> snapshot = new HashMap<>(CollectionSizing.capacity(old.size()));
    long sum = 0;
    for (Map.Entry<String, LongAdder> e : old.entrySet()) {
      long s = e.getValue().sum();
      snapshot.put(e.getKey(), s);
      if (s > 0) {
        sum += s;
      }
    }
    lastSnapshotSum = sum;

    return new Snapshot(snapshot, windowMs);
  }

  /** One rotated snapshot and the measured window it accumulated over. */
  private record Snapshot(Map<String, Long> counts, long windowMs) {}

  /**
   * One delivery cycle's accumulation table plus the hot merges that captured it. The counter is
   * bound to the table it protects, so a rotation waits only for merges into the table it retired;
   * merges into the fresh table never extend that wait.
   */
  private static final class Reservoir {

    /** The shared accumulation map for one delivery cycle. */
    private final ConcurrentHashMap<String, LongAdder> map;

    /** Hot merges that captured {@link #map} and have not settled yet. */
    private final AtomicInteger inFlight = new AtomicInteger();

    /** Creates an empty reservoir with the default table capacity. */
    private Reservoir() {
      this.map = new ConcurrentHashMap<>();
    }

    /**
     * Creates an empty reservoir with a pre-sized table.
     *
     * @param initialCapacity the expected distinct-key count
     */
    private Reservoir(int initialCapacity) {
      this.map = new ConcurrentHashMap<>(initialCapacity);
    }
  }

  /**
   * Return an approximate add of distinct keys currently aggregated.
   *
   * @return distinct key add, read from the O(1) counter (data still
   *         resident in writers' hot local maps is excluded; the swap-race
   *         approximation is the documented counter semantics)
   */
  public long estimatedSizeOfKeysCount() {
    return approximateSize.getOpaque();
  }

  /**
   * Cumulative new cold keys dropped by the soft capacity guard. A growing value with a stable
   * workload means {@code capacity} is too small; the count is separate from the reporter's
   * dead-worker, unroutable, routing-queue and dispatcher counters.
   *
   * @return cumulative cold-key capacity drops
   */
  public long coldCapacityDrops() {
    return coldCapacityDrops.sum();
  }

  /**
   * Periodic delivery. Phases: drain every registered writer's hot map (live writers
   * non-blocking, dead writers lock-free), reap dead writers, rotate the shared table
   * under {@link #reservoirGate}, settle in-flight merges plus the cold-write
   * quiescence window ({@link #tideWatcher()}), then deliver the snapshot before the
   * O(n) promotion pass so promotion never adds to consumer latency.
   *
   * <p>A failure anywhere — including an {@link Error} from the consumer — is logged
   * and the chain re-arms in a {@code finally}, so a throwing consumer cannot kill
   * delivery.
   */
  private void tide() {
    tideExecutionLock.lock();
    try {
      if ((boolean) SHUTDOWN.getVolatile(this)) {
        return;
      }
      // A tide clears its own future before the body: the running future is still the pending one
      // until run() returns, so leaving it in place would make the finally re-arm over a slot that
      // still names the running task. From here on, scheduleTide() is the only writer of the field.
      synchronized (tideScheduleGate) {
        pendingTide = null;
      }
      long nextDelayMs = deliverIntervalMs;
      try {
        // (add-locals): every writer's hot local map enters the
        // shared table. Per-writer mutex (ReentrantLock): a live writer
        // mid-claim is tried non-blockingly — if the lock is held, this
        // writer's residual is SKIPPED for this cycle (it is delivered by
        // the writer's own batch discharge or the next tide's tryDrainInto;
        // never lost, measured 0 loss across 10 deliver-racing stress
        // rounds). A dead writer is drained lock-free (it can never write
        // again — see drainDead). The table reference is read once: the tide
        // holds {@code tideExecutionLock}, the only lock under which the
        // reservoir reference is replaced, so it cannot change mid-loop.
        ConcurrentHashMap<String, LongAdder> table = reservoir.map;
        for (Map.Entry<Thread, Ceils> entry : hotRegistry.entrySet()) {
          Thread writer = entry.getKey();
          Ceils local = entry.getValue();

          if (!writer.isAlive()) {
            // Dead writer: quiescent map, no lock needed. Reap after.
            // (dead-writer drain before reap): a residual may be published
            // after the last tide saw the writer alive; drainDead sweeps the
            // occupied bitmap, which is the truth, so draining
            // unconditionally is free at tide frequency and closes the
            // death-race window.
            local.drainDead(this, table);
            hotRegistry.remove(writer, local);
            // (recycle-ceils): the drained map is dead-writer-owned and
            // empty — return it to the pool for the next writer instead
            // of letting it become garbage with the thread (see
            // {@link #ceilPool}).
            recycleCeils(local);
            continue;
          }

          if (!local.hasPendingUpdates()) {
            // Nothing published since the last drain: skip the lock and the
            // bitmap scan. The sequence read is acquire and the writer
            // publishes every increment, so a claim racing this check is
            // either seen or picked up by the next tide instead of being
            // hidden behind a stale size read.
            continue;
          }

          if (!local.tryDrainInto(this, table)) {
            // Writer holds the map mid-claim: skip this cycle — its own
            // batch discharge (or the next tide's tryDrainInto) moves the
            // data into the shared table, so a later tide's snapshot
            // includes it.
            continue;
          }
          // (reap-dead): reclaim dead writers' entries — their
          // residuals were just merged, so the registry cannot leak.
          if (!writer.isAlive()) {
            // A writer that died after the liveness check above may have added a final batch
            // between the drain and this re-check; drain it again before pooling the map.
            local.drainDead(this, table);
            hotRegistry.remove(writer, local);
            // (recycle-ceils): tryDrainInto just emptied the map and the
            // writer died before the re-check — pool it like the other
            // dead-writer paths (see {@link #ceilPool}).
            recycleCeils(local);
          }
        }

        Snapshot snapshot = tideWatcher();
        Map<String, Long> counts = snapshot.counts();
        if (counts.isEmpty()) {
          // An empty window is a zero-arrival observation for consumers that
          // drive a cadence controller, but it is not a promotion input: no
          // decay/histogram pass, and the empty-tide ladder still owns the
          // delay. A consumer failure must not skip the ladder.
          try {
            batchConsumer.accept(counts, snapshot.windowMs());
          } finally {
            nextDelayMs = pacer.emptyDelay(deliverIntervalMs);
          }
        } else {
          // (deliver-before-promote): the consumer receives the snapshot
          // BEFORE the promotion pass — the O(n) promotion work (hash
          // sweep, histogram, beacon decay, governor) must not add to the
          // delivery latency the consumer sees; the beacon state it
          // produces only affects the NEXT cycle's routing. The promote
          // still runs when the consumer throws (the batch is lost either
          // way — see the tide catch), so a failing consumer cannot also
          // freeze the routing. The consumer must not mutate the
          // delivered map.
          //
          // Delivery also comes before the next-delay computation: in apply
          // mode the consumer moves the base, and the delay this tide arms must
          // be the one the loop just chose — computing it first left a whole
          // extra window running at the superseded cadence.
          try {
            batchConsumer.accept(counts, snapshot.windowMs());
          } finally {
            promote(counts, snapshot.windowMs());
          }
          // Adaptive cadence (pacer path only): the raw backlog is folded into
          // a smoothed reference — a burst beyond the still band attacks
          // instantly, quiet tides release at the EMA rate, in-band jitter moves
          // nothing. An external cadence owner skips the pressure ramp entirely
          // and uses its base verbatim.
          nextDelayMs = nextTideDelayMs(counts.size());
        }
      } catch (Throwable t) {
        // Throwable, not Exception: the tide is a ONE-SHOT schedule(), so an uncaught Error would land
        // in a Future nobody reads and the self-reschedule below would never run, killing delivery for
        // the process lifetime.
        log.error("Scheduled delivery failed", t);
      } finally {
        // Self-rescheduling chain (one-shot schedule → tide → re-schedule).
        // Guarded by deliveryStarted so reflection-driven tides in tests never
        // arm a background chain, and by shutdown so destroy() stops it.
        // Re-armed in a finally so a failed tide (including an Error) cannot
        // end the chain.
        if (deliveryStarted && !((boolean) SHUTDOWN.getVolatile(this))) {
          scheduleTide(nextDelayMs);
        }
      }
    } finally {
      tideExecutionLock.unlock();
    }
  }

  /**
   * Promotion pass of a delivered snapshot: locate the boundary bucket, select the exact
   * {@code hotLimit}-th largest add, then promote the keys at or above the threshold —
   * renewals before newcomers when the tie band exceeds the remaining slots. The same
   * sweep packs every entry's (value, hash) into reused arrays for the saturated branch
   * and the index views.
   *
   * <p>The histogram/candidate pass, boundary estimation and incumbent capture run
   * BEFORE {@link #decayCounts()}: the pre-decay beacon is the only memory of last
   * tide's membership. A snapshot below {@link #MIN_PROMOTION_KEYS} is skipped unless
   * the quiet regime lifts the gate.
   *
   * <p>Deliverer-only, called once per non-empty tide.
   *
   * @param snapshot the just-delivered per-key counts (non-null, non-empty)
   * @param windowMs the measured swap-to-swap accumulation window, positive
   */
  private void promote(Map<String, Long> snapshot, long windowMs) {
    if (hotLimit == 0) {
      // No hot set to govern: skip the O(n) sweep, the scan arrays and the governor entirely.
      return;
    }
    int distinctKeys = snapshot.size();
    // The volume signal was accumulated during the snapshot build — no
    // second O(n) pass over the delivered map here (see {@link #lastSnapshotSum}).
    long snapshotSum = lastSnapshotSum;
    moonsTidalForce.onVolume(snapshotSum, windowMs);
    // (quiet scan gate, design decisions: counting engine): the MIN_PROMOTION_KEYS gate is lifted
    // while the quiet regime holds (volumeRate < QUIET_RATE_PER_SEC): a sub-minimum
    // quiet snapshot still promotes — the gate's noise protection is
    // vacuous with no traffic, and the decay must run so a stale member
    // set can reach remain == 0 and trigger the bypass (the decay (design decisions: counting engine)
    // freeze would otherwise strand a stale floor behind a frozen set).
    boolean scan = distinctKeys >= MIN_PROMOTION_KEYS || moonsTidalForce.quietVolume();
    if (scan) {
      // (pass state): the phase outputs thread through the reused holder —
      // each phase fills the fields the next phase reads (see
      // {@link PromotionPass}), and no per-tide object is allocated.
      pass.reset(moonsTidalForce.positions.floor);
      // The histogram/candidate sweep, the boundary estimation and the
      // exact selection all run BEFORE decayCounts() — the pre-decay
      // beacon state is the only memory of last tide's membership (see
      // the method Javadoc's ordering contract).
      sweepHistogram(snapshot, pass);
      estimateBoundary(pass);
      selectBoundary(pass);
      decay(pass);
      computeReading(pass, distinctKeys, snapshotSum, windowMs);
    }
  }

  /**
   * Opportunistic shrink of the reused scan arrays ({@link #allValues} /
   * {@link #allHashes}, the index views and the selection buffer), run at the start of every
   * scan tide's sweep: an array holding at least
   * {@link #SCAN_ARRAY_SHRINK_FACTOR}x the current tide's need is
   * downsized to {@code max(distinctKeys, SCAN_ARRAY_INITIAL_CAPACITY)}.
   *
   * <p>
   * <b>Why.</b> The arrays grow by doubling/pre-sizing, so without a
   * shrink one cardinality spike (a flood of distinct keys) would pin the
   * peak footprint for the process lifetime — megabytes of dead tail at the
   * reporter's 100k-key capacity. The shrink hands that tail back while
   * keeping the reuse discipline intact.
   *
   * <p>
   * <b>Hysteresis.</b> The 4x band and the floor keep resize churn
   * bounded: a workload must drift to 4x its working set before a copy is
   * paid (oscillating cardinality regrows into the retained slack for
   * free instead of bouncing the arrays), and a steady state at or below
   * the initial capacity never resizes at all.
   * Worst case the shrink adds one O(need) copy per scan tide on the
   * deliverer, the same cost shape as the sweep itself.
   *
   * <p>
   * <b>Safety.</b> Deliverer-only (the arrays are the promotion pass's
   * exclusive state), and every consumer re-grows on demand: each pass
   * reads only its own size prefix (stale tails are never read), and the
   * sweep's growth checks run after this pass, so a shrunken array that
   * under-covers the snapshot is re-grown immediately.
   *
   * <p>
   * Only the always-full views are pre-sized elsewhere; the filtered views
   * ({@link #candidateIdx}, {@link #aboveKthIdx}) stay demand-grown on
   * purpose. Pre-sizing them to {@code distinctKeys} would commit the whole
   * snapshot's worth of memory for lists that hold ~{@code hotLimit}
   * entries in steady state, and it would defeat the 4x hysteresis the
   * shrink depends on.
   *
   * @param distinctKeys the current tide's distinct-key add — the need
   *                     every array must cover this tide
   */
  private void shrinkScanArrays(int distinctKeys) {
    int target = Math.max(distinctKeys, SCAN_ARRAY_INITIAL_CAPACITY);
    // allValues/allHashes grow in lockstep, so one check covers the pair
    if (allValues.length >= (long) SCAN_ARRAY_SHRINK_FACTOR * target) {
      // Every array below is rewritten from index 0 before it is read, so a copy of the old
      // prefix is wasted memory bandwidth; allocate the new length directly.
      allValues = new long[target];
      allHashes = new int[target];
    }
    candidateIdx = shrinkScanView(candidateIdx, target);
    aboveKthIdx = shrinkScanView(aboveKthIdx, target);
    splitIdx = shrinkScanView(splitIdx, target);
    if (selectValues.length >= (long) SCAN_ARRAY_SHRINK_FACTOR * target) {
      selectValues = new long[target];
    }
  }

  /**
   * Shrink one index view when it holds at least
   * {@link #SCAN_ARRAY_SHRINK_FACTOR}x the target, else return it as-is.
   * The comparison is long arithmetic — 4x a huge target would overflow an
   * int multiply (a math guard; the arrays cannot reach it in practice).
   *
   * @param view   the reused index view (deliverer-only)
   * @param target the current tide's need (see {@link #shrinkScanArrays(int)})
   * @return the shrunken view, or the same array when within the band
   */
  private static int[] shrinkScanView(int[] view, int target) {
    return view.length >= (long) SCAN_ARRAY_SHRINK_FACTOR * target ? new int[target] : view;
  }

  /**
   * One pass over the snapshot: the log2-bucket location histogram AND the promotion
   * candidates (keys at or above the floor — the promote condition
   * {@code v >= max(floor, boundary)} always implies {@code v >= floor}). Every entry's
   * (value, hash) is packed into {@link #allValues} / {@link #allHashes} so the saturated
   * branch reuses them; the same sweep collects the kth-shortcut list. Runs BEFORE
   * {@link #decayCounts()} because the incumbent capture needs the pre-decay beacon
   * state.
   */
  private void sweepHistogram(Map<String, Long> snapshot, PromotionPass pass) {
    int distinctKeys = snapshot.size();
    Arrays.fill(histogram, 0);

    // (array shrink): reclaim a past cardinality spike's overshoot before
    // the growth checks — the floor + 4x hysteresis band keep steady-state
    // workloads copy-free (see shrinkScanArrays).
    shrinkScanArrays(distinctKeys);

    // (packed snapshot): ensure the packed (value, hash) arrays cover the
    // snapshot before the sweep — every entry is packed (the saturated
    // branch needs v <= 0 members too, see {@link #allValues}), so the
    // capacity is the distinct-key add, grown by doubling like
    // {@link #candidateIdx}.
    if (allValues.length < distinctKeys) {
      int grown = distinctKeys <= 0 ? 1 : Integer.highestOneBit(distinctKeys - 1) << 1;

      // The packed arrays are filled from 0 in this sweep; a copy of the previous prefix would
      // only be overwritten.
      allValues = new long[grown];
      allHashes = new int[grown];
    }
    // The auxiliary index views grow on demand below. Pre-sizing them to the whole snapshot
    // committed memory for views a tide might not use (the kth-shortcut list, a
    // low-cardinality bucket) without saving a copy: the sweep still writes them from 0.

    long prevKth = lastKth;
    // A previous boundary of 1 admits almost the whole snapshot, so the list would be as large
    // as the bucket path it replaces: below 2 the filter buys nothing and is skipped.
    boolean collectAboveKth = prevKth > 1;
    int packed = 0;
    int aboveKthSize = 0;
    int candSize = 0;

    for (Map.Entry<String, Long> e : snapshot.entrySet()) {
      long v = e.getValue();
      // One avalanche per key, cached for the packed arrays AND the
      // candidate index view (see {@link #candidateIdx}) — never computed
      // twice for the same key. Packed BEFORE the floor filter: the
      // saturated branch counts v <= 0 members (the stale-squatting
      // signal) and needs their hashes too.
      allValues[packed] = v;
      allHashes[packed] = mixHash(e.getKey().hashCode());
      int idx = packed++;
      if (v <= 0) {
        continue;
      }

      // 63 - numberOfLeadingZeros(v) is floor(log2(v)) for a positive long and stays in [0, 62].
      histogram[63 - Long.numberOfLeadingZeros(v)]++;
      // (kth-shortcut filter, design decisions: counting engine): collect every key at or above
      // the previous tide's exact boundary — the list {@link #selectBoundary}
      // selects over when it provably holds this cycle's k-th largest (see
      // the method's Javadoc). One comparison per snapshot entry, paid on
      // every scan tide; the first tide (lastKth = -1) skips the branch.
      if (collectAboveKth && v >= prevKth) {
        if (aboveKthIdx.length == aboveKthSize) {
          aboveKthIdx = Arrays.copyOf(aboveKthIdx, aboveKthIdx.length << 1);
        }
        aboveKthIdx[aboveKthSize++] = idx;
      }

      if (v >= pass.floor) {
        if (candidateIdx.length == candSize) {
          candidateIdx = Arrays.copyOf(candidateIdx, candidateIdx.length << 1);
        }
        candidateIdx[candSize++] = idx;
      }
    }
    pass.packedSize = packed;
    pass.aboveKthSize = aboveKthSize;
    pass.candSize = candSize;
  }

  /**
   * Top-down boundary LOCATION: the highest non-empty histogram bucket is
   * the boundary bucket, and the boundary PROVISIONALLY lands on its
   * power-of-two floor ({@code 2^b}) — scale-free by construction (a
   * relative hot spot qualifies at ANY traffic volume).
   * {@link #selectBoundary} immediately replaces the boundary value with
   * the exact k-th largest add (design decisions: counting engine); this method only locates the
   * bucket and computes {@code above} (the keys in higher buckets), the
   * remaining-slot basis of the selection.
   */
  private void estimateBoundary(PromotionPass pass) {
    long accumulated = 0;
    long above = 0;
    for (int i = HISTOGRAM_BUCKETS - 1; i >= 0 && accumulated < hotLimit; i--) {
      int cnt = histogram[i];
      if (cnt > 0) {
        above = accumulated;
        accumulated += cnt;
        pass.boundaryBucket = i;
        pass.boundary = 1L << i;
        pass.threshold = Math.max(pass.floor, pass.boundary);
      }
    }
    pass.above = above;
  }

  /**
   * Exact selection of the promotion boundary: the boundary VALUE is the exact
   * {@code hotLimit}-th largest add of the snapshot — selected over the compact value buffer from
   * the log2-located boundary bucket, or over the kth-shortcut list when that list provably
   * contains the cutoff and is not materially larger than the view the bucket path would scan.
   * The threshold becomes {@code max(floor, kth)}; the blocked band and the
   * incumbent/newcomer overflow gate derive from the same exact cutoff.
   * design decisions: counting engine.
   */
  private void selectBoundary(PromotionPass pass) {
    int b = pass.boundaryBucket;
    if (b < 0) {
      return;
    }

    int need = (int) (hotLimit - pass.above);
    int bucketCount = histogram[b];
    long kth;
    long qualifying;

    if (b == 0) {
      // Bucket 0 is exactly the count 1: every key in it is a 1, so the cutoff and the
      // qualifying count are known without touching the buffer.
      kth = 1L;
      qualifying = pass.above + bucketCount;
    } else {
      boolean floorBelowBucketEdge = pass.floor <= pass.boundary;
      int scanLimit = floorBelowBucketEdge ? pass.candSize : pass.packedSize;
      // (cost gate): the shortcut spends O(list) where the bucket path spends O(bucket + list), so
      // it only pays when the list is not materially larger than what the bucket path scans. A
      // stale low lastKth makes the list the whole snapshot; the gate then keeps the bucket path.
      if (pass.aboveKthSize >= hotLimit && pass.aboveKthSize <= (scanLimit >>> 1) + bucketCount) {
        int n = pass.aboveKthSize;
        ensureSelectValues(n);
        for (int i = 0; i < n; i++) {
          selectValues[i] = allValues[aboveKthIdx[i]];
        }
        // The three-way post-condition yields the cutoff AND its qualifying count: everything
        // before lt is strictly below kth, everything from lt on is at or above it.
        int lt = selectKthPartition(selectValues, n, hotLimit);
        kth = selectValues[lt];
        qualifying = n - lt;
        pass.aboveKthView = true;
      } else if (bucketCount <= need) {
        // The bucket fits in the remaining slots: the cutoff is its minimum and every bucket key
        // qualifies. Non-positive counts cannot match: their unsigned shift is never 1.
        long minVal = Long.MAX_VALUE;
        if (floorBelowBucketEdge) {
          for (int i = 0; i < scanLimit; i++) {
            long v = allValues[candidateIdx[i]];
            if ((v >>> b) == 1 && v < minVal) {
              minVal = v;
            }
          }
        } else {
          for (int i = 0; i < scanLimit; i++) {
            long v = allValues[i];
            if ((v >>> b) == 1 && v < minVal) {
              minVal = v;
            }
          }
        }
        kth = minVal;
        qualifying = pass.above + bucketCount;
        pass.aboveKthView = pass.aboveKthSize >= hotLimit;
      } else {
        // (exact selection): the need-th largest within the bucket IS the k-th largest of the
        // snapshot — the `above` keys in higher buckets all qualify. Selecting over the values
        // (not an index view) keeps the scan sequential and leaves the packed pairing untouched.
        ensureSelectValues(bucketCount);
        int n = 0;
        if (floorBelowBucketEdge) {
          for (int i = 0; i < scanLimit; i++) {
            long v = allValues[candidateIdx[i]];
            if ((v >>> b) == 1) {
              selectValues[n++] = v;
            }
          }
        } else {
          for (int i = 0; i < scanLimit; i++) {
            long v = allValues[i];
            if ((v >>> b) == 1) {
              selectValues[n++] = v;
            }
          }
        }
        int lt = selectKthPartition(selectValues, n, need);
        kth = selectValues[lt];
        qualifying = pass.above + (n - lt);
        pass.aboveKthView = pass.aboveKthSize >= hotLimit;
      }
    }

    pass.boundary = kth;
    pass.threshold = Math.max(pass.floor, kth);
    // (kth-shortcut seed): the exact boundary of THIS tide becomes the next
    // tide's filter — the pass's output is the only memory the shortcut
    // needs (a skipped tide leaves the previous seed in place; stale seeds
    // only widen or narrow the filter, never break the lemma).
    lastKth = kth;
    if (kth >= pass.floor) {
      // the boundary dominates: the threshold is the exact k-th largest,
      // the blocked band is empty, and the incumbent-first gate is the
      // tie band wider than the capacity.
      pass.overflow = qualifying > hotLimit;
      pass.blockedKeys = 0;
      if (pass.overflow) {
        splitIncumbents(pass, kth);
      }
    } else {
      // the floor dominates: the threshold is the floor; the blocked band
      // is the keys the floor excludes that WOULD qualify at the top-k
      // cutoff — add(v >= kth) - add(v >= floor) = qualifying -
      // candSize (cand = v >= floor, exact by construction).
      pass.overflow = false;
      pass.blockedKeys = (int) (qualifying - pass.candSize);
    }
  }

  /** Grow the selection buffer to hold at least {@code needed} values. */
  private void ensureSelectValues(int needed) {
    if (selectValues.length < needed) {
      selectValues = new long[Math.max(selectValues.length << 1, needed)];
    }
  }

  /**
   * Pre-decay incumbent/newcomer split: the candidates at or above {@code kth}, split by
   * last-tide membership, so the capacity break never cuts a renewing key. Runs BEFORE
   * the halving decay — the pre-decay beacon state is the only memory of last tide's
   * membership. Pass 1 re-promotes every qualifying incumbent, so the fill pass never
   * re-tests membership.
   *
   * <p>
   * One array, two fronts: incumbents fill from index 0 upward, newcomers from
   * {@code viewSize - 1} downward. The fronts cannot cross — every qualifying key lands on
   * exactly one side — and each side keeps its snapshot order, so the capacity break is
   * deterministic.
   */
  private void splitIncumbents(PromotionPass pass, long kth) {
    // (shortcut view): on the kth-shortcut path every key at or above kth
    // is inside aboveKthIdx, so the split walks that view (~hotLimit
    // entries) instead of every candidate at or above the floor — under a
    // high-cardinality load (100k keys above the floor, hotLimit 1024) the
    // split would otherwise dominate the tide with an O(snapshot) scan the
    // shortcut had just eliminated from the selection.
    boolean useAbove = pass.aboveKthView && pass.aboveKthSize < pass.candSize;
    int[] view = useAbove ? aboveKthIdx : candidateIdx;
    int viewSize = useAbove ? pass.aboveKthSize : pass.candSize;
    if (splitIdx.length < viewSize) {
      splitIdx = new int[viewSize];
    }

    int inc = 0;
    int newStart = viewSize;
    for (int cIdx = 0; cIdx < viewSize; cIdx++) {
      int packedIdx = view[cIdx];
      if (allValues[packedIdx] >= kth) {
        if (isBeaconMember(allHashes[packedIdx], 1)) {
          splitIdx[inc++] = packedIdx;
        } else {
          splitIdx[--newStart] = packedIdx;
        }
      }
    }
    pass.incSize = inc;
    pass.newStart = newStart;
    pass.newEnd = viewSize;
  }

  /**
   * In-place selection of the k-th LARGEST value (1-based) of {@code a[0..n)} by three-way
   * quickselect: O(n) expected comparisons, and no O(n^2) input — a deterministic pseudo-random
   * median of three blunts adversarial orderings that defeat a fixed middle pivot. The pivot is
   * picked with an xorshift-seeded Lemire multiply-shift, so the draw costs no division.
   *
   * <p>
   * The three-way partition is what makes the tie bands cheap and the second pass unnecessary: on
   * return {@code a[0..lt)} is strictly below the cutoff and {@code a[lt..n)} is at or above it,
   * so {@code a[lt]} is the k-th largest and {@code n - lt} is the exact qualifying count. Only the
   * caller's scratch copy is permuted; the packed snapshot keeps its order.
   *
   * @param a the value buffer (permuted)
   * @param n the number of valid entries
   * @param k the 1-based rank from the top; must satisfy {@code 1 <= k <= n}
   * @return the index where the cutoff begins (the k-th largest value lives at {@code a[return]})
   */
  private static int selectKthPartition(long[] a, int n, int k) {
    int lo = 0;
    int hi = n - 1;
    int target = n - k;
    int seed = n * 0x9E3779B9 + 1;
    while (lo < hi) {
      int span = hi - lo + 1;
      long pivot;
      if (span >= 32) {
        seed ^= seed << 13;
        seed ^= seed >>> 17;
        seed ^= seed << 5;
        long x = a[lo + (int) (((seed & 0xFFFFFFFFL) * span) >>> 32)];
        seed = seed * 1664525 + 1013904223;
        long y = a[lo + (int) (((seed & 0xFFFFFFFFL) * span) >>> 32)];
        seed = seed * 1664525 + 1013904223;
        long z = a[lo + (int) (((seed & 0xFFFFFFFFL) * span) >>> 32)];
        pivot = (x <= y) ? (y <= z ? y : Math.max(x, z)) : (x <= z ? x : Math.max(y, z));
      } else {
        pivot = a[(lo + hi) >>> 1];
      }

      int lt = lo;
      int i = lo;
      int gt = hi;
      while (i <= gt) {
        long v = a[i];
        if (v < pivot) {
          a[i] = a[lt];
          a[lt] = v;
          lt++;
          i++;
        } else if (v > pivot) {
          a[i] = a[gt];
          a[gt] = v;
          gt--;
        } else {
          i++;
        }
      }
      if (target < lt) {
        hi = lt - 1;
      } else if (target > gt) {
        lo = gt + 1;
      } else {
        return lt;
      }
    }
    return lo;
  }

  /**
   * Beacon decay gate: the halving decay runs only when the promotion scan that
   * re-seeds evidence also runs. A sub-minimum snapshot would otherwise strip the whole
   * hot set with nothing to renew it; the decay also runs only every
   * {@link #DECAY_PERIOD}-th promoted tide, so periodic earners survive their quiet
   * tides. On intervening tides only the active size is counted and the evidence
   * freezes; a skipped scan freezes the sweep clock too. {@code sweepDecay} captures
   * this tide's phase for the governor's decay-equivalent reading.
   */
  private void decay(PromotionPass pass) {
    pass.sweepDecay = sweepCountdown == 1;
    if (pass.sweepDecay) {
      pass.remain = decayCounts();
      sweepCountdown = DECAY_PERIOD;
    } else {
      pass.remain = countActive(1);
      sweepCountdown--;
    }
  }

  /**
   * The governor reading of the pass: the renewal numerator, hot-set
   * earnings and the promoted-key add are computed on EVERY promoted
   * tide — including the saturated state ({@code remain >= hotLimit}),
   * where a full hot set whose members stopped earning is the governor's
   * squatting distress signal. Counting memberKeys POST-promotion avoids
   * the startup artifact of a fresh beacon reading zero renewal on the
   * very tide that promotes everyone. The signals are bundled into one
   * {@link TideReading} and delivered to {@link #moonsTidalForce}.
   */
  private void computeReading(PromotionPass pass, int distinctKeys, long snapshotSum, long windowMs) {
    int memberKeys = 0;
    long memberEarnings = 0;
    // Two hot-side counters — exactly one is populated, because the
    // saturated branch excludes the scan branches: the scan branches add
    // keys PROMOTED THIS TIDE, the saturated branch counts beacon MEMBERS
    // with live evidence (no scan ran). Both are the "hot-side" key add
    // the density signal subtracts from distinctKeys, so they fold back
    // into one value after the branches.
    int promotedThisTideCount = 0;
    int activeMemberCount = 0;
    // (blocked signal, design decisions: counting engine): the blocked band [boundary, threshold)
    // is non-empty only when the floor sits above the exact k-th largest
    // (noise traffic — the floor does the selection). The band is the
    // keys the floor excludes that WOULD qualify at the top-k cutoff,
    // computed exactly in selectBoundary (add(v >= kth) − candSize).
    int blockedKeys = pass.blockedKeys;

    // (phase-normalized reading): on a sweep SKIP tide the governor's signals are
    // measured on the DECAY-EQUIVALENT basis — the post-scan evidence >= 2 set,
    // exactly what a halving decay + this scan would have left — so renewal,
    // hotColdRatio, the saturation gate and the empty-set collapse are phase-invariant
    // instead of oscillating with the frozen evidence. The promotion scan gate keeps
    // the honest (evidence >= 1) occupancy, so no capacity oversell. The strict flag
    // gates the saturated enumeration below; the reading's remain is re-derived AFTER
    // the scan so the non-saturated branch sees the post-promotion beacon.
    boolean strictReading = !pass.sweepDecay;
    if (pass.remain < hotLimit) {
      if (pass.overflow) {
        // (renew-first): pass 1 — every captured incumbent is re-promoted before any newcomer, so
        // the capacity break in pass 2 can never evict a renewing key. Activations (rooms zeroed
        // by the decay) consume the hotLimit budget exactly like the single pass; a saturated set
        // renews fully and leaves pass 2 nothing to promote (stable freeze, not rotation). The
        // split already filtered at the exact cutoff, so no threshold test is repeated here.
        for (int iIdx = 0; iIdx < pass.incSize; iIdx++) {
          int packedIdx = splitIdx[iIdx];
          long v = allValues[packedIdx];
          memberKeys++;
          memberEarnings += v;
          promotedThisTideCount++;
          if (promoteToBeacon(allHashes[packedIdx])) {
            pass.remain++;
          }
        }
        // (fill): pass 2 — newcomers take the remaining capacity; the
        // capacity check precedes the promotion, so a set that pass 1
        // refilled cannot grow past hotLimit (the single-pass break
        // would overshoot by one here). No membership re-test: the
        // newcomer split was decided pre-decay in the merged sweep, and
        // pass 1 re-promotes every qualifying incumbent, so no key here
        // can already be a member. Newcomers were written from the top
        // down, so walking them in reverse restores snapshot order.
        for (int nIdx = pass.newEnd - 1; nIdx >= pass.newStart; nIdx--) {
          // The capacity check PRECEDES everything, including the
          // memberKeys++: pass 1 may have already refilled the budget
          // (renewals are activations after the decay-zeroed scan tide), so
          // an after-the-fact break would still promote one newcomer per
          // scan tide — a key that then renews forever and ratchets the hot
          // set past hotLimit. Counting the key that triggered the break
          // inflates the renewal numerator by one as well (it was never
          // promoted).
          if (pass.remain >= hotLimit) {
            break;
          }
          int packedIdx = splitIdx[nIdx];
          long v = allValues[packedIdx];
          memberKeys++;
          memberEarnings += v;
          promotedThisTideCount++;
          if (promoteToBeacon(allHashes[packedIdx])) {
            pass.remain++;
          }
        }
      } else {
        // (shortcut view): the smaller of the two views that provably contain the threshold band.
        // On the shortcut path every key at or above the cutoff is inside aboveKthIdx; otherwise
        // candidateIdx holds every key at or above the floor, and the threshold is at least that.
        boolean useAbove = pass.aboveKthView && pass.aboveKthSize < pass.candSize;
        int[] view = useAbove ? aboveKthIdx : candidateIdx;
        int viewSize = useAbove ? pass.aboveKthSize : pass.candSize;
        long threshold = pass.threshold;
        for (int cIdx = 0; cIdx < viewSize; cIdx++) {
          int packedIdx = view[cIdx];
          long v = allValues[packedIdx];
          if (v >= threshold) {
            // Every key at the threshold is a member after this call
            // (re-promotions included) — the renewal numerator. The hash must
            // come from the SAME packed index the value did.
            int h = allHashes[packedIdx];
            // A non-overflow selection only bounds the CURRENT tide's qualifying set; it does
            // not account for the active slots that survived the decay. Once those fill the hot
            // set, only an existing member may renew — activating a newcomer here would push
            // `remain` past hotLimit.
            if (pass.remain >= hotLimit && !isBeaconMember(h, 1)) {
              continue;
            }
            memberKeys++;
            memberEarnings += v;
            promotedThisTideCount++;
            if (promoteToBeacon(h)) {
              pass.remain++;
            }
          }
        }
      }
    } else {
      // (saturated): no promotion scan runs (every slot is occupied), so
      // the renewal and earnings signals are enumerated directly. The
      // enumeration walks the packed arrays from the histogram sweep —
      // the pass-1 hashes are reused, one {@code mixHash} per key per
      // tide even on the saturated steady state of a full hot set.
      // The membership test reads the POST-decay beacon — a key is a member
      // iff it was promoted or renewed within the last four tides (the
      // counting-engine sweep period), exactly the slots that occupy the set —
      // so stale members (whose keys no longer earn the threshold) are
      // counted as occupied slots with their residual earnings, which is
      // the squatting signal. On a sweep SKIP tide the enumeration uses
      // the decay-equivalent membership (isBeaconMember floor 2),
      // the frozen-beacon analogue of the post-decay test (design decisions: counting engine).
      // The renewal numerator counts ONLY beacon members that earned the
      // threshold (design decisions: counting engine's "active hot slots whose key earned at
      // least the threshold") — snapshot keys that are NOT members are
      // cold keys waiting for a slot, and counting them would inflate
      // renewal: with 1024 stale members and 600 new earners the set
      // would read healthy (0.586 >= 0.5) and the squatting evidence
      // (hotColdRatio) would never be reached. The self-healing stays
      // the 4-tide decay: the stale members' slots free up, and the
      // next scan promotes the earners.
      for (int i = 0; i < pass.packedSize; i++) {
        long v = allValues[i];
        if (isBeaconMember(allHashes[i], strictReading ? 2 : 1)) {
          memberEarnings += v;
          activeMemberCount++;
          if (v >= pass.threshold) {
            memberKeys++;
          }
        }
      }
    }

    // (phase-normalized reading, design decisions: counting engine): the reading's remain is the
    // decay-equivalent active set on a skip tide — the post-scan
    // evidence >= 2 add (countActive(2)), which on decay
    // tides equals `remain` (decay + activations) by construction.
    int remainReading = strictReading ? countActive(2) : pass.remain;

    // (density signal, DensityClimber-style): the hot set's earnings per
    // occupied slot vs the cold reservoir's earnings per key, computed
    // within this single sample — immune to workload phases. A ratio
    // below 1 means the occupied hot slots earn less per slot than cold
    // keys earn per key (a frozen or stale-squatting set); the governor
    // prices its raise-walk arm on it. Structurally >= 1 whenever the
    // boundary selects the top earners and the set is refilled, so it
    // fires only in the stale window.
    // Exactly one branch above ran, so the sum is the hot-side key add
    // (keys promoted this tide, or live beacon members on a saturated set)
    // — the rest of the snapshot is the cold reservoir the ratio prices.
    int hotSideCount = promotedThisTideCount + activeMemberCount;
    long coldKeys = distinctKeys - hotSideCount;
    double hotColdRatio;
    if (coldKeys <= 0 || hotSideCount == 0) {
      // No cold base or no members to measure — no under-earning signal.
      hotColdRatio = Double.MAX_VALUE;
    } else {
      double hotDensity = (double) memberEarnings / Math.max(1L, remainReading);
      double coldDensity = (double) (snapshotSum - memberEarnings) / coldKeys;
      hotColdRatio = coldDensity > 0 ? hotDensity / coldDensity : Double.MAX_VALUE;
    }

    moonsTidalForce.onTide(
      new TideReading(
        // A beacon false positive can make memberKeys exceed the active-room count; the ratio is
        // a renewal share, so it cannot exceed 1.0 (a frozen >1 base would later look like a crash
        // when the workload returns to a genuine 100%).
        Math.min(1.0, memberKeys / (double) Math.max(1L, remainReading)),
        remainReading,
        hotLimit,
        blockedKeys,
        pass.boundary,
        hotColdRatio,
        distinctKeys,
        windowMs
      )
    );
  }

  /**
   * Adaptive delay until the next tide, scaled by the backlog delivered this
   * cycle — the wave pacer path, used only while no external controller owns
   * the cadence.
   *
   * <p>
   * Borrows the JVM monitor feedback idea: delivery frequency adapts to
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
   * Next delay for a non-empty tide: the external controller's base verbatim,
   * or the wave pacer's pressure-compressed delay.
   *
   * <p>
   * While an external controller owns the cadence, its base <em>is</em> the
   * delay. Compressing it from the backlog would make the applied window differ
   * from the input the controller scored, i.e. a second controller on the same
   * actuator. The empty-tide ladder is not affected (it is chosen in the
   * {@code null}-snapshot branch and only fires when there is no sample).
   */
  private long nextTideDelayMs(int deliveredKeys) {
    if (externalCadence) {
      pacer.resetEmptyStreak();
      return deliverIntervalMs;
    }
    return computeNextTideDelayMs(pacer.pressure(deliveredKeys));
  }

  /**
   * Arm the next tide as a one-shot task.
   *
   * <p>
   * A tide clears {@link #pendingTide} under the schedule gate before its body and re-arms from
   * the {@code finally}, so at most one future exists at a time and the arm always replaces a
   * spent slot. Degenerate delays are clamped up to {@link #EARLY_TIDE_MIN_INTERVAL_MS}. Safe to
   * call after {@link #destroy()} — the shutdown flag short-circuits, and scheduler rejections
   * are logged.
   *
   * @param delayMs requested delay until the next tide (clamped up)
   */
  private void scheduleTide(long delayMs) {
    if ((boolean) SHUTDOWN.getVolatile(this)) {
      return;
    }

    long effectiveDelay = Math.max(delayMs, EARLY_TIDE_MIN_INTERVAL_MS);
    synchronized (tideScheduleGate) {
      // destroy() sets the flag before taking this gate. Re-check under the gate: a reschedule
      // racing destroy could otherwise pass the outer check, wait here, and arm a fresh future
      // after destroy canceled the pending one.
      if ((boolean) SHUTDOWN.getVolatile(this)) {
        return;
      }
      try {
        pendingTide = scheduler.schedule(this::tide, effectiveDelay, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        // Scheduling failed (e.g. the executor is shutting down): report once, not per request.
        pendingTide = null;
        log.error("Failed to schedule WaveCounter delivery; buffered counts will be delayed.", e);
      }
    }
  }

  /**
   * Adjust the base delivery cadence used by the tide loop from the next fire
   * onward (design decisions: reporting feed-loop interval tuner; report path
   * only).
   *
   * <p>
   * The <b>first</b> call transfers cadence ownership from the wave pacer to
   * the caller: from then on a non-empty tide's delay is exactly this base (the
   * empty-tide ladder still stretches an idle cycle up to
   * {@link #EMPTY_TIDE_STRETCH_CAP_MULTIPLE}x it). Backlog pressure no longer
   * compresses the delay — a controller that scores a rate onto its own input
   * needs the window it measures to be the input it applied, and two
   * controllers cannot both own the same actuator. Ownership is never returned;
   * the tuner is configured once at startup.
   *
   * <p>
   * Callers own the [floor, ceiling] policy; this method only clamps up to the
   * floor so the construction-time invariant (see the {@code deliverIntervalMs}
   * Javadoc — the governor's flood-rate normalization divides by the cadence)
   * is never broken by a later adjustment.
   *
   * <p>
   * Visibility: the fields are volatile and the tide loop re-reads them on
   * every fire, so an adjustment takes effect at the next scheduled tide. The
   * currently armed delay is intentionally left alone.
   *
   * <p>
   * Safe to call from any thread after {@link #afterPropertiesSet()};
   * no-op after {@link #destroy()}.
   *
   * @param newIntervalMs requested base cadence (clamped up to the floor)
   */
  public void adjustDeliverIntervalMs(long newIntervalMs) {
    if ((boolean) SHUTDOWN.getVolatile(this)) {
      return;
    }
    this.deliverIntervalMs = Math.max(EARLY_TIDE_MIN_INTERVAL_MS, newIntervalMs);
    this.externalCadence = true;
  }

  @Override
  public void afterPropertiesSet() {
    // Fail fast before arming delivery: a null external scheduler would
    // otherwise NPE inside scheduleTide, be caught by its guard, and log
    // "buffered counts will be delayed" while delivery would in fact never
    // start. A counter that never calls this method legitimately may still
    // be constructed with a null scheduler (pure buffered counting).
    Assert.notNull(
      scheduler,
      "scheduler must not be null when starting delivery " +
        "(construct with ownsScheduler=true or inject a real executor)"
    );
    // Seed the window clock before the first fire: an adjustDeliverIntervalMs() between startup
    // and the first tide must not make that tide report the new base as elapsed time.
    synchronized (reservoirGate) {
      if (lastSnapshotAtMs == 0L) {
        lastSnapshotAtMs = TimeSource.monotonicMillis();
      }
    }
    deliveryStarted = true;
    scheduleTide(deliverIntervalMs);
  }

  /**
   * Final delivery of all merged counts and scheduler shutdown.
   *
   * <p>
   * Sets {@code shutdown} first (new {@code count()} calls no-op), merges
   * every registered writer's hot local map (mutex-serialized against any
   * in-flight claim via the per-map lock), then performs the same
   * swap-and-snapshot as {@link #tide()}.
   *
   * <p>
   * <b>Approximate final add.</b> The registry sweep
   * cannot enumerate a writer that is racing its FIRST hot registration
   * (or was preempted between registration and its first add), so at
   * most a few residual counts from such a writer can be left undelivered
   * — the information gap a per-op in-flight counter would close at
   * ~15ns/op on the hot path; the ns-scale window is the accepted price.
   * For a registered writer the drain merges every published delta: the
   * writer's sequence is read before the slot scan, and a last-moment add
   * either lands before that read (merged now) or after it, in which case
   * it is the same ns-scale shutdown race as the registration gap.
   *
   * <p>
   * A live writer's map is not wiped after its drain: the writer may still be
   * mid-claim on the lock-free path, and a cross-thread wipe would race that
   * claim. Its slots are released with the writer thread's ThreadLocal — at
   * most {@code opMaxCount} key references, none of the counter's own state.
   */
  @Override
  public boolean isDestroyed() {
    return (boolean) SHUTDOWN.getVolatile(this);
  }

  @Override
  public void destroy() {
    // Stop accepting new counts first — everything counted before this
    // moment must be delivered; everything after is dropped by design.
    SHUTDOWN.setVolatile(this, true);
    // Cancel any pending tide (never interrupt a running one): without
    // this an injected scheduler's pending tide would still fire after
    // destroy(). A concurrently RUNNING tide is not affected — its own
    // reschedule guard (the shutdown flag) stops the chain.
    synchronized (tideScheduleGate) {
      if (pendingTide != null) {
        pendingTide.cancel(false);
        pendingTide = null;
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
    // Merge all hot local maps, then rotate-and-snapshot exactly like tide() —
    // including the cold-write quiescence window.
    tideExecutionLock.lock();
    try {
      // The lock is the only context in which the reservoir reference is replaced, so one read
      // covers the whole sweep: no per-writer capture/reserve, no reservoirGate contention and no
      // in-flight churn on the teardown path.
      ConcurrentHashMap<String, LongAdder> table = reservoir.map;
      for (Map.Entry<Thread, Ceils> entry : hotRegistry.entrySet()) {
        Thread writer = entry.getKey();
        Ceils local = entry.getValue();
        if (writer.isAlive()) {
          // Blocking drain: the map lock serializes a live writer's in-flight capacity reset, and
          // the delta baseline makes the merge exact.
          local.drainFinal(this, table);
        } else {
          // A dead writer can never touch its map again, so a lock-free drain
          // is exact; blocking on Ceils.lock here could hang forever because
          // Java locks are not released on thread death (tide() has the same
          // rule via drainDead()).
          local.drainDead(this, table);
          hotRegistry.remove(writer, local);
          // No recycle: destroy() clears the pool in the finally below, so a full reset here
          // would only write a map that becomes garbage immediately.
        }
      }
      Snapshot snapshot = tideWatcher();
      try {
        // Shutdown delivers only real counts: an empty final window carries no data,
        // and a consumer must not see a synthetic batch on close.
        if (!snapshot.counts().isEmpty()) {
          batchConsumer.accept(snapshot.counts(), snapshot.windowMs());
        }
      } finally {
        // Drop the per-writer state: every residual was merged above, so
        // the registry's {@code Thread} keys and the pooled maps are pure
        // ballast — and a destroyed counter that is still referenced (a bean
        // registry, a leaked context) would otherwise keep dead threads and up
        // to {@link #CEIL_POOL_CAP} ~7KB maps alive until the process ends.
        // {@code Ceils} is static and holds no back-reference, so live writers'
        // ThreadLocal entries are reclaimed by GC once this counter is itself
        // unreachable — this only releases what THIS object owns.
        hotRegistry.clear();
        ceilPool.clear();
        ceilPoolSize.set(0);
      }
    } finally {
      tideExecutionLock.unlock();
    }
  }

  /**
   * Writer-private open-addressing map for hot keys. Accessed only by its
   * owning thread, except for the deliverer's drains, which never write a slot's key or count.
   *
   * <p>
   * The capacity is fixed at {@link #LOCAL_CAPACITY}; the batch trigger
   * ({@code opMaxCount}) is at most half of it, so probing never runs out of
   * empty slots.
   *
   * <p>
   * <b>Single-writer monotonic protocol.</b> The owner owns {@link #keys}, {@link #tags},
   * {@link #counts}, {@link #occupied}, {@code size}, the last-key cache and {@link #writeSeq};
   * the deliverer only reads them and maintains its own {@link #drained} baseline. Every
   * increment — a fresh claim or an existing-slot add — is published by a release store to
   * {@code writeSeq} after the count store, so a drain that observes a new sequence also observes
   * every count it covers. A drain reads the sequence first, merges {@code counts[i] - drained[i]}
   * for each marked slot and advances only its private baseline: it never clears a slot, never
   * rewrites a key, and therefore never races the writer's claim. The residual is exact — a count
   * published after the sequence read is simply merged by the next drain because the sequence
   * moved again — and a count read by the drain before its sequence store is not double-counted
   * because the next round's difference is zero. Slots are recycled in bulk only in
   * {@link #dischargeInto}, which runs on the owner's own thread under the map lock.
   *
   * <p>
   * <b>Static, and the owner is never stored.</b> Instances are the values of
   * {@link #hotLocals}, a {@link ThreadLocal} the writer deliberately never
   * removes (a pooled writer keeps reusing its map). A non-static inner class
   * would hold {@code WaveCounter.this}, closing a strong reference cycle
   * {@code Thread -> ThreadLocalMap.Entry.value -> Ceils -> WaveCounter ->
   * hotLocals}: the {@code ThreadLocal} key would stay strongly reachable
   * through the value, so the map's {@code WeakReference} key could never be
   * cleared and a {@link #destroy()}'d counter would stay pinned for the
   * lifetime of every writer thread (beacon, scan arrays, registry, pool).
   * The owner is therefore passed as a PARAMETER to the few methods that
   * reach the shared table and never kept in a field — the cycle is cut and a
   * retired counter's entry is expunged like any other stale
   * {@code ThreadLocal} value.
   */
  private static final class Ceils {

    private static final VarHandle COUNTS = MethodHandles.arrayElementVarHandle(long[].class);
    private static final VarHandle OCCUPIED = MethodHandles.arrayElementVarHandle(long[].class);
    private static final VarHandle WRITE_SEQ = varHandle(Ceils.class, "writeSeq", long.class);

    /** Keys by slot; written only by the owner, cleared in bulk by {@link #tryReset()}. */
    final String[] keys = new String[LOCAL_CAPACITY];

    /**
     * Per-slot counts. Written only by the owner: a plain read-modify-write on the add path, a
     * release store so a drain that sees the slot also sees the value.
     */
    final long[] counts = new long[LOCAL_CAPACITY];

    /**
     * The drainer's per-slot baseline: the share of {@link #counts} already merged into the shared
     * table. Written only by the drainer (under {@link #lock}) and reset by the owner's discharge,
     * so the merge difference {@code counts[i] - drained[i]} is the un-merged residual. The
     * baseline is what lets a drain run without clearing slots.
     */
    final long[] drained = new long[LOCAL_CAPACITY];

    /**
     * Per-slot claim tags (the avalanched hash, 0 = empty). Same-thread only: the owner probes
     * them when claiming, and no other thread writes them.
     */
    final int[] tags = new int[LOCAL_CAPACITY];

    /**
     * Occupancy bitmap: bit {@code i} set = slot {@code i} claimed. Stored with release ordering
     * after the slot's key and count, so a drain that observes a bit also observes a complete
     * entry; the drain reads it with acquire. Cleared in bulk by the owner's discharge, which
     * cannot race a claim (both run on the owner's thread).
     */
    final long[] occupied = new long[LOCAL_CAPACITY >>> 6];

    /**
     * Slow-path mutex. A {@link ReentrantLock} instead of a monitor so the deliverer can
     * {@link ReentrantLock#tryLock()}: a busy writer never blocks the tide — its residual is
     * skipped for the cycle and delivered by the writer's own batch discharge or the next tide's
     * drain, or by {@link #drainDead} once the writer is observed dead. The lock serializes the
     * drain against the owner's capacity reset ({@link #dischargeInto}); it is not on the add path.
     */
    final ReentrantLock lock = new ReentrantLock();

    /** Number of claimed slots, at most {@code opMaxCount} between discharges. Owner-only. */
    int size;

    /**
     * Last-key cache for the fast add (thread-private: written and read only by the owner): the
     * slot of the most recent successful add. A slot never changes keys except through a claim by
     * this same writer, and every claim records the cache, so {@code key == lastKey} guarantees
     * the slot still holds this key — no probe, no {@code equals}, no beacon.
     *
     * <p>
     * The cache survives drains: the drain leaves the slot and its key in place and only advances
     * its baseline, so the next repeated add is a plain store to the same slot. A bulk discharge
     * or a reset clears it.
     */
    private String lastKey;

    /** Slot of {@link #lastKey} (meaningful only while {@code lastKey != null}). */
    private int lastSlot;

    /**
     * Monotonic count of published increments. Every claim and every add stores the incremented
     * value with release ordering AFTER the count store; a drain reads it with acquire before its
     * slot scan. {@code 0} is a valid value — {@link #drainedSeq} tracks the same sequence, so an
     * untouched map reads as drained.
     */
    @SuppressWarnings("unused")
    private long writeSeq;

    /** The sequence value the last completed drain observed; written only by the drainer. */
    private volatile long drainedSeq;

    /**
     * Whether any increment was published since the last completed drain — the deliverer's O(1)
     * skip test. The read is acquire and the writer publishes every count before its sequence, so
     * a {@code false} answer means the previous drain (which advanced the baseline after its scan)
     * already covered every visible count.
     *
     * @return {@code true} when the map holds un-merged increments
     */
    boolean hasPendingUpdates() {
      return (long) WRITE_SEQ.getAcquire(this) != drainedSeq;
    }

    /**
     * Fast add for the repeated-SAME-key pattern, callable BEFORE the beacon routing test in
     * {@link WaveCounter#count(String, long)}: a repeated add of the same key instance
     * (the common hot-key pattern) skips the probe, the {@code equals} AND the routing beacon.
     *
     * <p>
     * The cache can only reference a key that previously took the hot path, and a slot's key
     * never changes except through a claim by this writer — which always re-records the cache —
     * so a hit is a hot-key add by construction. A drain may have merged the slot's count since;
     * the add simply publishes a new sequence value and the next drain picks up the difference.
     *
     * @param key   the accessed key
     * @param delta the number of accesses
     * @return {@code true} if the increment was fully handled here
     */
    boolean tryFastAdd(String key, long delta) {
      if (key == lastKey) {
        COUNTS.setRelease(counts, lastSlot, counts[lastSlot] + delta);
        WRITE_SEQ.setRelease(this, writeSeq + 1);
        return true;
      }
      return false;
    }

    /**
     * Routed add of one increment into the local map (beacon membership already decided by the
     * caller): open addressing with linear probing.
     *
     * <p>
     * A fresh claim writes the key, the count and the tag, then publishes the occupancy bit with
     * release ordering and the sequence with release ordering — a drain that sees either observes
     * a complete entry. An existing slot is a plain count add plus a sequence publish. The batch
     * trigger runs only after a claim, so the steady-state hit path never reads {@code size}.
     *
     * @param owner the owning counter (passed per call, never stored — see the class note)
     * @param key   the accessed key
     * @param delta the number of accesses
     * @param h     the avalanched key hash ({@code mixHash(key.hashCode())})
     */
    void add(WaveCounter owner, String key, long delta, int h) {
      int tag = tagOf(h);
      int i = indexOf(h);
      for (;;) {
        int curTag = tags[i];
        if (curTag == 0) {
          keys[i] = key;
          COUNTS.setOpaque(counts, i, delta);
          tags[i] = tag;
          int w = i >>> 6;
          OCCUPIED.setRelease(occupied, w, occupied[w] | (1L << (i & 63)));
          WRITE_SEQ.setRelease(this, writeSeq + 1);
          if (++size >= owner.opMaxCount) {
            owner.discharge(this);
          } else {
            lastKey = key;
            lastSlot = i;
          }
          return;
        }
        if (curTag == tag && key.equals(keys[i])) {
          COUNTS.setRelease(counts, i, counts[i] + delta);
          WRITE_SEQ.setRelease(this, writeSeq + 1);
          lastKey = key;
          lastSlot = i;
          return;
        }
        i = (i + 1) & (LOCAL_CAPACITY - 1);
      }
    }

    /**
     * Merge every un-merged slot into the shared table and reclaim the whole map. Runs on the
     * owner's own thread when the batch trigger fires, so no writer can race the wipe; the lock
     * only excludes the deliverer's concurrent drain.
     *
     * <p>
     * The sequence is left in place (it stays monotonic) and the baseline is advanced to it, so a
     * drain that runs immediately afterwards sees nothing pending. {@code drained} is zeroed
     * together with the counts, keeping the pairing consistent.
     *
     * @param owner the owning counter (passed per call, never stored — see the class note)
     * @param table the shared table captured by the caller
     */
    void dischargeInto(WaveCounter owner, ConcurrentHashMap<String, LongAdder> table) {
      lock.lock();
      try {
        for (int w = 0; w < occupied.length; w++) {
          long bits = occupied[w];
          if (bits == 0L) {
            continue;
          }
          while (bits != 0L) {
            int i = (w << 6) + Long.numberOfTrailingZeros(bits);
            long diff = counts[i] - drained[i];
            if (diff != 0L) {
              owner.mergeHotKey(table, keys[i], diff);
            }
            keys[i] = null;
            tags[i] = 0;
            counts[i] = 0L;
            drained[i] = 0L;
            bits &= bits - 1;
          }
          OCCUPIED.setRelease(occupied, w, 0L);
        }
        size = 0;
        lastKey = null;
        drainedSeq = writeSeq;
      } finally {
        lock.unlock();
      }
    }

    /**
     * Non-blocking drain for the periodic tide: {@link ReentrantLock#tryLock()} — if the writer
     * holds the map mid-discharge, the tide skips this writer's residual for this cycle instead of
     * blocking. Skipped data is not lost: the writer's own batch discharge (or the next tide's
     * drain) moves it into the shared table.
     *
     * @param owner the owning counter (passed per call, never stored — see the class note)
     * @param table the shared table to drain into
     * @return {@code true} if the drain ran (or there was nothing to merge), {@code false} if the
     *         map was locked
     */
    boolean tryDrainInto(WaveCounter owner, ConcurrentHashMap<String, LongAdder> table) {
      if (!lock.tryLock()) {
        return false;
      }
      try {
        long seq = (long) WRITE_SEQ.getAcquire(this);
        if (seq == drainedSeq) {
          return true;
        }
        waveDeltaTo(owner, table);
        drainedSeq = seq;
        return true;
      } finally {
        lock.unlock();
      }
    }

    /**
     * Blocking final drain for {@link WaveCounter#destroy()}: a live writer's in-flight capacity
     * reset is serialized by the map lock, and every published delta is merged before the counter
     * shuts down.
     *
     * @param owner the owning counter (passed per call, never stored — see the class note)
     * @param table the shared table to drain into
     */
    void drainFinal(WaveCounter owner, ConcurrentHashMap<String, LongAdder> table) {
      lock.lock();
      try {
        long seq = (long) WRITE_SEQ.getAcquire(this);
        waveDeltaTo(owner, table);
        drainedSeq = seq;
      } finally {
        lock.unlock();
      }
    }

    /**
     * Lock-free drain for a DEAD writer (called only after {@link Thread#isAlive()} returned
     * {@code false}): a dead thread can never write again, so the map is quiescent — no lock, no
     * reservoir reservation. Without this, a writer that died while holding the lock would stall
     * every tide forever (Java locks are not released on thread death).
     *
     * @param owner the owning counter (passed per call, never stored — see the class note)
     * @param table the shared table to drain into
     */
    void drainDead(WaveCounter owner, ConcurrentHashMap<String, LongAdder> table) {
      waveDeltaTo(owner, table);
    }

    /**
     * Merge the difference between every marked slot and the drain's baseline, advancing the
     * baseline to the observed value. The writer's slots are never touched, so a claim racing this
     * scan is either seen (its bit and count were published before the bitmap read) or left for
     * the next drain via the sequence — never lost, never double-counted.
     */
    private void waveDeltaTo(WaveCounter owner, ConcurrentHashMap<String, LongAdder> table) {
      boolean markedQuiescence = false;
      for (int w = 0; w < occupied.length; w++) {
        long bits = (long) OCCUPIED.getAcquire(occupied, w);
        while (bits != 0L) {
          int i = (w << 6) + Long.numberOfTrailingZeros(bits);
          long cur = (long) COUNTS.getAcquire(counts, i);
          long diff = cur - drained[i];
          if (diff != 0L) {
            if (!markedQuiescence) {
              owner.markColdWrite();
              markedQuiescence = true;
            }
            owner.mergeHotKey(table, keys[i], diff);
            drained[i] = cur;
          }
          bits &= bits - 1;
        }
      }
    }

    /** Map the sentinel hash 0 away — 0 marks an empty tag slot. */
    private static int tagOf(int h) {
      return h == 0 ? Integer.MIN_VALUE : h;
    }

    /** Return the index of the given hash in the local table. */
    private static int indexOf(int h) {
      return h & (LOCAL_CAPACITY - 1);
    }

    /**
     * Reset every slot (used when a dead writer's map is pooled). The per-map lock is probed, not
     * blocked: a map whose owner died between the lock acquisition and the protected block can
     * never release it, and {@link WaveCounter#recycleCeils} discards such a map instead of
     * blocking the deliverer forever.
     *
     * @return {@code true} if the map was reset and can be reused
     */
    boolean tryReset() {
      if (!lock.tryLock()) {
        return false;
      }
      try {
        Arrays.fill(keys, null);
        Arrays.fill(counts, 0L);
        Arrays.fill(drained, 0L);
        Arrays.fill(tags, 0);
        Arrays.fill(occupied, 0L);
        size = 0;
        lastKey = null;
        writeSeq = 0L;
        drainedSeq = 0L;
        return true;
      } finally {
        lock.unlock();
      }
    }
  }

  /**
   * Pacing state for the adaptive tide cadence: an EWMA-smoothed backlog with fast
   * attack, slow release and a still band, plus the empty-tide stretch ladder. Written
   * only by the deliverer. The attack folds a burst in fully so detection latency does
   * not grow; the release EMA drains it over several tides instead of ping-ponging; the
   * still band ignores jitter. An empty tide never touches the reference; each
   * consecutive one stretches the next cadence, capped at
   * {@link #EMPTY_TIDE_STRETCH_CAP_MULTIPLE}x the base.
   */
  private static final class TidePacer {

    private double smoothed = -1;
    private int emptyStreak;

    /**
     * Fold this tide's raw backlog into the smoothed reference and return
     * the pressure value for the ramp. Resets the empty-tide ladder.
     *
     * @param deliveredKeys distinct keys delivered by this tide
     * @return the smoothed backlog (fast attack, slow release, still band)
     */
    int pressure(int deliveredKeys) {
      resetEmptyStreak();
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
     * Reset the empty-tide ladder: a non-empty tide is the ladder's confirm.
     * {@link #pressure(int)} does this implicitly; the external-cadence path
     * skips the pressure fold but must still break an empty streak.
     */
    void resetEmptyStreak() {
      emptyStreak = 0;
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
   * {@code Reading}): the goal-metric signals {@link #promote(Map, long)}
   * computes from the snapshot, bundled so the governor's input contract
   * is a single typed carrier instead of eight positional primitives. The
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
   *                     add level, before the floor)
   * @param hotColdRatio the hot set's earnings per occupied slot divided
   *                     by the cold reservoir's earnings per key, this tide
   * @param distinct     distinct keys of this tide's snapshot — the flood
   *                     signature's scale gate
   * @param intervalMs   the measured accumulation window this tide's snapshot
   *                     was rotated over (previous swap → this swap) — the
   *                     wall-clock debounce interval for the regime switches;
   *                     volume is already normalized to counts/sec when folded
   */
  private record TideReading(
    double renewal,
    int remain,
    int hotLimit,
    int blockedKeys,
    long boundary,
    double hotColdRatio,
    int distinct,
    long intervalMs
  ) {}

  /**
   * Mutable per-tide state of one promotion pass (deliverer-only, reused across tides): the phase
   * outputs thread through the pass methods (sweepHistogram, estimateBoundary, selectBoundary,
   * decay, computeReading) instead of nine positional primitives — each phase fills the fields the
   * next phase reads. {@link #reset(int)} establishes the invariants a fresh pass would have.
   */
  private static final class PromotionPass {

    /**
     * The promotion floor of this pass, captured once (the governor only moves it
     * in the reading).
     */
    int floor;
    /** Entries packed into allValues / allHashes by the histogram sweep. */
    int packedSize;
    /** Candidate index-view size, parallel to candidateIdx. */
    int candSize;
    /** Kth-shortcut view size, parallel to aboveKthIdx (0 while lastKth < 0). */
    int aboveKthSize;
    /**
     * Whether the kth-shortcut list provably contains this tide's threshold band
     * ({@code aboveKthSize >= hotLimit}). When it does, the exact k-th largest is
     * {@code >= lastKth}, so EVERY key at or above this tide's threshold is already inside
     * {@code aboveKthIdx} — the incumbent split and the promotion scan can walk that view
     * (~hotLimit entries) instead of {@code candidateIdx} (every key at or above the floor,
     * which under a high-cardinality load is the whole snapshot). False on the bucket path,
     * where the boundary was selected from a bucket that is not a superset of the threshold
     * band.
     */
    boolean aboveKthView;
    /** Incumbent count in splitIdx, written from index 0 upward. */
    int incSize;
    /** First newcomer slot in splitIdx (newcomers fill from newEnd - 1 downward). */
    int newStart;
    /** One past the last newcomer slot (the view size the split walked). */
    int newEnd;
    /**
     * Distinct keys accumulated above the boundary bucket (the remaining-slot
     * basis).
     */
    long above;
    /** The promotion threshold: max of the floor and the (refined) boundary. */
    long threshold;
    /**
     * The pure promotion boundary, before the floor (the exact k-th largest since
     * design decisions: counting engine).
     */
    long boundary = 1;
    /**
     * The histogram's highest non-empty bucket (the boundary bucket); -1 when the
     * pass was skipped.
     */
    int boundaryBucket = -1;
    /**
     * Whether the exact cutoff's tie band exceeded the remaining slots (the
     * incumbent-first gate).
     */
    boolean overflow;
    /**
     * Keys the floor excludes that WOULD qualify at the top-k cutoff (computed by
     * selectBoundary).
     */
    int blockedKeys;
    /**
     * Active hot-set size after this tide's halving decay (0 on sub-minimum tides).
     */
    int remain;
    /**
     * Whether THIS tide ran the halving decay (vs a sweep skip tide) — the counting-engine
     * phase.
     */
    boolean sweepDecay;

    /**
     * Reset the pass to the state a fresh instance would have, with the given floor.
     *
     * @param floor the promotion floor this pass evaluates against
     */
    void reset(int floor) {
      this.floor = floor;
      this.threshold = floor;
      this.packedSize = 0;
      this.candSize = 0;
      this.aboveKthSize = 0;
      this.aboveKthView = false;
      this.incSize = 0;
      this.newStart = 0;
      this.newEnd = 0;
      this.above = 0;
      this.boundary = 1;
      this.boundaryBucket = -1;
      this.overflow = false;
      this.blockedKeys = 0;
      this.remain = 0;
      this.sweepDecay = true;
    }
  }

  /**
   * Adaptive promotion floor: a noise filter seeded at {@link #PROMOTION_FLOOR} whose
   * only signal is the health of the hot set it admits. It drops toward the boundary
   * when healthy keys are blocked, raises through a bounded, evidence-priced walk when
   * the hot slots under-earn the cold reservoir, retreats to the last confirmed anchor
   * when a raise fails, and releases under saturation or sustained health.
   *
   * <p>Written only by the deliverer; all references (anchor, renewal EMA, ladder) are
   * per-instance state. design decisions: counting engine.
   */
  private static final class MoonsTidalForce {

    /**
     * Tides a walk may take before it is confirmed (Caffeine's PROBE_WALK_BUDGET).
     */
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
     * Consecutive crash endings at which a crash stops being priced as
     * exogenous (Caffeine's {@code PROBE_CRASH_ESCALATION}): only a
     * consecutive crash run doubles the ladder rung — probe damage and an
     * exogenous workload shift are indistinguishable on a single crash.
     * Consumed by {@link Ladder#crash(Ladder.Direction)}.
     */
    private static final int PROBE_CRASH_ESCALATION = 2;
    /**
     * Distress tides before a raise-walk may arm, so one noisy tide cannot arm it. The
     * walk's own crash/confirm persistence and budgeted undo are the noise filter.
     */
    private static final int RAISE_ARM_DELAY = 1;
    /**
     * The evidence gap on the renewal goal metric before a veto or an undo
     * may fire (WindowClimber's {@code VETO_MARGIN_MIN} analog): the
     * current position must earn at least this much less than the frozen
     * reference before the machine distrusts it. The effective margin is
     * the max of this and twice the measured renewal scatter (see
     * {@link #vetoMargin()}).
     */
    private static final double MIN_TIDAL_EVIDENCE = 0.1;
    /**
     * EMA weight of the per-tide renewal deviation (Caffeine's
     * {@code Rates.RATE_SMOOTHING}, ~5-tide memory): the noise-adaptive
     * veto margin is priced from a smoothed mean absolute deviation
     * instead of a ring population std — O(1) per tide (no ring sweep)
     * and less outlier-sensitive (a single noisy renewal cannot inflate
     * the margin for a whole ring window).
     */
    private static final double RENEWAL_SMOOTHING = 0.2;

    /**
     * Cached {@code Math.log(2.0)} for the density-priced raise stride (avoids a
     * constant recompute per raise step).
     */
    private static final double LN_2 = Math.log(2.0);

    /**
     * Cached {@code 1 / Math.log(2.0)} so the raise-stride log2 is a multiply, not
     * a divide, per step.
     */
    private static final double INV_LN_2 = 1.0 / LN_2;

    /**
     * Seed of the renewal-deviation EMA (Caffeine's {@code Rates.DEVIATION_SEED}):
     * with the {@link #MIN_TIDAL_EVIDENCE} floor the seeded margin reads
     * exactly the floor, so a cold state (no samples yet) cannot veto
     * early or return a zero margin before any scatter exists.
     */
    private static final double DEVIATION_SEED = 0.05;
    private static final Ladder.Direction RAISE = Ladder.Direction.RAISE;
    private static final Ladder.Direction RELEASE = Ladder.Direction.RELEASE;

    // The governor's scalar state lives in the {@link Positions}, {@link State},
    // {@link Rates}, {@link Ladder} and {@link Regime} wrapper classes below.
    // Each state value is a plain field, and the seed values arrive as constructor parameters:
    // {@code Positions} seeds the adaptive floor at {@link #PROMOTION_FLOOR},
    // {@link Ladder} keeps the raise and release retry ledgers as two
    // independent state groups selected by its internal {@link Ladder.Direction}
    // enum instead of a half-segmented packed word). The two P1 debounce
    // accumulators (`quietMs`, `noiseMs`) are plain ints of the {@link Regime}
    // wrapper — saturating, deliverer-only writes. The governor's
    // {@code double} renewal signals live with their owning state too: the
    // veto anchor renewal in {@link Positions}, the descent slope reference
    // in {@link State} and the smoothed-renewal EMA pair in {@link Rates}
    // (each wrapper's {@code reset()} keeps its fields' seeding consistent).
    // Every state value is written only by the deliverer thread, so the
    // accessors remain plain, non-atomic updates. The {@code lastRenewal}
    // and {@code volumeRate} signals below stay separate fields: a rolling sample
    // reference and a continuous EWMA, where fixed-point encoding would lose
    // precision on the governor's most sensitive path.

    /**
     * Position-domain state (floor, retreat target, anchors) — {@link Positions}.
     */
    private final Positions positions = new Positions(PROMOTION_FLOOR);

    /** Retreat-probe and persistence state — {@link State}. */
    private final State state = new State();

    /** P1 regime-switch debounce accumulators (wall-clock ms) — {@link Regime}. */
    private final Regime debounce = new Regime();

    /**
     * The raise and release retry ledgers (independent state per internal
     * direction) — {@link Ladder}.
     */
    private final Ladder ladder = new Ladder();

    /** Smoothed-renewal EMA pair (mean + deviation) — {@link Rates}. */
    private final Rates rates = new Rates();

    /**
     * The previous tide's raw renewal — the reference of the R3
     * workload-shift test (Caffeine's {@code Sample.previousHitRate}):
     * a single-tide move at or above {@link #RESTART_THRESHOLD}
     * announces a regime change. {@code NaN} = unseeded (no previous
     * sample yet); survives collapse and the stand-down itself — only
     * the tide flow updates it.
     */
    private double lastRenewal = Double.NaN;

    /**
     * EWMA of the per-tide snapshot volume in counts per second, folded by
     * {@link #onVolume(long, long)} on every non-empty tide — 0 is the unseeded marker (seeded
     * with the first rate sample after a reset). The unit is normalized at fold time; a cadence
     * change therefore cannot reinterpret an old window's total against a new window's length.
     * The quiet bypass and the flood collapse are rate-gated: the floor's absolute position must
     * match the regime's scale.
     */
    private double volumeRate;

    /** No retreat in progress. */
    private static final int RETREAT_STAGE_STOPPED = -1;

    /** Single-stage retreat: walk directly to the target. */
    private static final int RETREAT_STAGE_DOING = 0;

    /** Direction-probe retreat: hold and sample renewal before committing. */
    private static final int RETREAT_STAGE_PROBE = PROBE_STREAK;

    /**
     * A walk in flight ({@code up} = raise, {@code down} = release), or
     * {@code null} when parked.
     */
    private Walk walk;

    /**
     * Advance the governor by one promoted tide.
     *
     * <p>
     * The reading is the immutable derived view of this tide built by
     * {@link #promote(Map, long)} (WindowClimber's {@code Reading} pattern):
     * the goal-metric signals are bundled into one typed carrier instead
     * of eight positional primitives, so the promotion pass and the
     * governor share a single documented contract.
     *
     * <p>
     * <b>P1 (quiet bypass, design decisions: counting engine).</b> A volume-quiet regime
     * ({@code volumeRate < QUIET_RATE_PER_SEC}) with an empty hot set drops the floor to
     * {@link #QUIET_FLOOR} (1) so ANY key routes hot and the scan gate is
     * lifted (sub-minimum snapshots promote). The floor's noise role is
     * vacuous with no traffic, the slots are free, and a stale raised floor
     * must not survive into the next regime. Both switches are debounced
     * by accumulated wall time: a quiet regime lasts seconds.
     *
     * <p>
     * <b>P2 (flood collapse, design decisions: counting engine).</b> A RAISED floor on which NO
     * key of a HIGH-volume snapshot earns the threshold (the
     * {@link #floodSignature} — renewal 0, floor above the
     * boundary, band non-empty, rate at/above {@link #FLOOD_RATE_PER_SEC})
     * is stale from another regime, not a noise filter: the correct
     * response is the empty-set collapse's full reset in ONE tide instead
     * of the wrong-direction raise-walk (parked by the renewal==0 step
     * gate) plus the slow 4-tide decay + remain==0 collapse. The FLOOD
     * also plants {@link State#floodLock}, which suppresses the ARM until the
     * volume drops below the flood rate (a raise and a flood collapse are
     * contradictory responses to the same state).
     *
     * @param reading the tide's derived view (renewal, occupancy,
     *                saturation, blocked keys, histogram boundary, the
     *                density ratio and the volume/scale signals — see
     *                {@link TideReading})
     */
    public void onTide(TideReading reading) {
      double renewal = reading.renewal();
      int remain = reading.remain();
      int hotLimit = reading.hotLimit();
      int blockedKeys = reading.blockedKeys();
      long boundary = reading.boundary();
      double hotColdRatio = reading.hotColdRatio();
      int distinct = reading.distinct();
      long intervalMs = reading.intervalMs();

      ladder.tick(RAISE); // raise ladder serves one backoff tide
      ladder.tick(RELEASE); // release ladder serves one backoff tide

      // The volume EWMA is folded in promote() before onTide() is reached. Folding the
      // same sample again here would apply ~0.75 weight instead of the designed 0.5 and
      // could engage/disengage the P1/P2 regime gates a tide early.
      if (emptyCollapse(remain) || isEqQuiet(remain, intervalMs)) {
        return;
      }

      isDqQuiet(intervalMs);
      double rate = volumeRate;
      boolean floodSig = floodSignature(renewal, blockedKeys, boundary, distinct, rate);
      if (floodCollapse(floodSig, rate)) {
        return;
      }

      foldRenewal(renewal);
      if (retreat(renewal)) {
        return;
      }

      if (walk(renewal, hotColdRatio, blockedKeys, boundary)) {
        return;
      }

      if (belowHealthyRenewal(renewal)) {
        distress(renewal, blockedKeys, hotColdRatio, floodSig);
      } else {
        healthy(renewal, remain, hotLimit, blockedKeys, boundary);
      }
    }

    /**
     * Regime-change collapse: the empty-set collapse and the P2 flood collapse share one
     * full reset with the same ladder pricing. An empty hot set above the seed, or a
     * raised floor blocking the whole high-volume distribution, proves the floor is
     * stale — reset to the seed immediately instead of walking back through the slow
     * audit.
     *
     * <p>In-flight walks and retreats are cancelled so they cannot drag the floor back;
     * distress/veto history, the audit clock, the anchor and the renewal/volume
     * references belong to the old regime and are reset. A walk killed by the collapse
     * is priced FAILED before the wipe, and any already-priced ladder state survives.
     * {@code floodLock} deliberately survives (planted after this call by the FLOOD
     * site): clearing it would re-arm into the same flood.
     */
    private void collapse() {
      // (collapse ladder pricing, design decisions: counting engine): a collapse that kills an
      // in-flight walk is the walk's own fault — priced as a failed experiment; any ladder state
      // priced before the wipe survives, so the backoff throttle keeps an oscillation loop
      // bounded. ladder.fail() always leaves left = rung >= TIDAL_BACKOFF_INITIAL, so the
      // isInflight() snapshot below is guaranteed to see it after a just-killed walk.
      if (walk != null) {
        ladder.fail(walk.up ? RAISE : RELEASE);
      }
      boolean keepLadder = isInflight();

      positions.reset();
      endWalk();
      // The empty set is a regime change: the distress/veto history,
      // the audit run, the anchor and the renewal references all
      // belong to the old regime. Reset them so the new regime starts
      // from a blank slate — a stale distressTides together with a stale
      // anchor could otherwise trigger one spurious retreat after the
      // workload recovers (the anchor is recaptured by the next
      // confirmed walk of the new regime).
      // (design decisions: counting engine): the descent slope state dies with the regime.
      state.reset();
      ladder.raise.crashStreak = 0;
      ladder.release.crashStreak = 0;
      // An un-priced ladder (no walk killed, no earlier price) starts clean: deepFail is a
      // release-only audit reschedule and must not outlive the regime that earned it.
      if (!keepLadder) {
        ladder.deepFail = false;
      }
      // The volume signal and the quiet/noise accumulators belong to the
      // old regime — re-seeded by the next tide. The renewal references
      // (the smoothed signal and its deviation) belong to it too — re-seeded
      // by the next fold. floodLock deliberately survives
      // (planted by the FLOOD call site AFTER this call): a wipe
      // that cleared it would re-arm into the same flood.
      volumeRate = 0.0;
      debounce.noiseMs = debounce.quietMs = 0;
      rates.reset();
    }

    /**
     * The P2 flood signature (design decisions: counting engine): a high-volume snapshot on which NO
     * key earns the threshold while the floor does the selection. Used
     * both as the FLOOD collapse trigger and to suppress the ARM — a raise
     * and a flood collapse are contradictory responses to the same state;
     * at high volume the stale-floor interpretation wins, so the floor must
     * not be raised into an instant collapse. Conditions: no earner
     * (renewal <= 0), blocked band present, floor above boundary, sufficient
     * distinct keys, and rate at/above the flood threshold.
     *
     * @param renewal     the tide's renewal ratio (memberKeys / max(1, remain))
     * @param blockedKeys keys the floor currently excludes from the
     *                    histogram boundary (the case where the floor is
     *                    over-filtering — admitted by dropping the floor)
     * @param boundary    the pure histogram boundary (top-{@code hotLimit}
     *                    add level, before the floor)
     * @param distinct    distinct keys of this tide's snapshot — the flood
     *                    signature's scale gate
     * @param rate        the tide's volume rate (counts/sec), computed once
     *                    by {@link #onTide(TideReading)}
     * @return true when the flood signature holds
     */
    private boolean floodSignature(double renewal, int blockedKeys, long boundary, int distinct, double rate) {
      return (
        renewal <= 0.0 &&
        blockedKeys > 0 &&
        positions.floor > boundary &&
        distinct >= FLOOD_MIN_DISTINCT &&
        rate >= FLOOD_RATE_PER_SEC
      );
    }

    /**
     * Reference re-learn after a workload shift (R3; Caffeine's
     * {@code Anchor.standDown} + {@code Rates.reset}): the goal-metric
     * references, the distress streak and the renewal EMA are discarded
     * — the machine re-learns from here instead of smoothing across the
     * shift. The FLOOR is deliberately untouched (the position was
     * earned; only the references are stale), the raise stride needs no
     * re-seed (it is a pure function of each tide's density reading since
     * design decisions: counting engine), and the AUDIT clock survives (Caffeine's
     * AuditClock: "what must be still is the position, never the rate" —
     * a rate swing must not suppress the re-test of a still
     * equilibrium). The veto anchor is discarded only when the shift
     * happened AT the anchor position (within {@link #ANCHOR_BAND}): a
     * claim tested at its own position and found wrong is discarded, but
     * a shift far from the anchor is typically the controller's own
     * retreat crossing a band edge — the reference survives there.
     */
    private void standDown() {
      if (Math.abs(positions.floor - positions.anchorD) <= ANCHOR_BAND) {
        // The shift landed AT the anchor: the claim is discarded (tested at
        // its own position and found wrong), but the FLOOR is deliberately
        // untouched — the position was earned, only the references are
        // stale. (Positions.reset() would clear the floor too, so the
        // anchor fields are wiped by hand here.)
        positions.anchorD = 0;
        positions.anchorD_Renewal = 0;
      }
      state.distressTides = 0;
      // The renewal EMA pair re-seeds together (R3's reset-then-update: the
      // shift tide's sample becomes the first sample of the new regime).
      rates.reset();
    }

    /**
     * Empty-set collapse: an empty hot set while the floor still sits above the seed
     * means the floor overshot the whole distribution. Reset immediately instead of
     * drifting down through the slow audit.
     *
     * @return true when the collapse ran and the tide is handled
     */
    private boolean emptyCollapse(int remain) {
      if (remain == 0 && isAbovePromoteFloor()) {
        collapse();
        return true;
      }
      return false;
    }

    /**
     * P1 entry (quiet bypass): a volume-quiet regime with an empty hot set drops the
     * floor to {@link #QUIET_FLOOR}, so any key routes hot and sub-minimum snapshots
     * promote. Debounced by accumulated wall time. Ordered after the empty-set collapse.
     *
     * @return true when the bypass engaged and the tide is handled
     */
    private boolean isEqQuiet(int remain, long intervalMs) {
      boolean quietCond = remain == 0 && positions.floor != QUIET_FLOOR && volumeRate < QUIET_RATE_PER_SEC;
      debounce.quietMs = quietCond ? (int) Math.min(QUIET_CONFIRM_MS, debounce.quietMs + intervalMs) : 0;
      if (debounce.quietMs >= QUIET_CONFIRM_MS) {
        // A complete regime switch: the walk/retreat, the anchors, the distress history and the
        // renewal references were all measured against the old volume regime. The probes are
        // cancelled before the floor drops, and the anchors are cleared with the floor, so the
        // re-engage path starts from a clean state instead of resuming stale steps or retreating
        // to a pre-quiet target.
        endWalk();
        state.reset();
        positions.reset();
        positions.floor = QUIET_FLOOR;
        rates.reset();
        debounce.quietMs = 0;
        return true;
      }
      return false;
    }

    /**
     * P1 exit: the floor returns to the seed once the volume EWMA crosses
     * {@link #QUIET_REENGAGE_RATE_PER_SEC} (2x the entry — hysteresis band
     * against volume hovering at the boundary), debounced by
     * {@link #NOISE_CONFIRM_MS}.
     *
     * <p>
     * The exit is the same complete regime switch as the entry. Restoring only the floor would
     * leave an in-flight walk frozen against the quiet floor's base: a raise armed at
     * {@link #QUIET_FLOOR} would later crash and undo to that base, dragging the floor back
     * below {@link #PROMOTION_FLOOR}; a retreat armed there could target a sub-seed position.
     * Cancelling the probes and wiping the anchors removes both paths structurally.
     */
    private void isDqQuiet(long intervalMs) {
      boolean noiseCond = positions.floor == QUIET_FLOOR && volumeRate >= QUIET_REENGAGE_RATE_PER_SEC;
      debounce.noiseMs = noiseCond ? (int) Math.min(NOISE_CONFIRM_MS, debounce.noiseMs + intervalMs) : 0;
      if (debounce.noiseMs >= NOISE_CONFIRM_MS) {
        endWalk();
        state.reset();
        positions.reset();
        rates.reset();
        debounce.noiseMs = 0;
      }
    }

    /**
     * P2 flood collapse: a raised floor on which no key of a high-volume snapshot earns
     * the threshold ({@link #floodSignature}) is stale from another regime. Reset in one
     * tide and plant {@link State#floodLock} so the ARM stays suppressed until the volume
     * drops (a raise and a flood collapse are contradictory responses).
     *
     * @param rate the tide's volume rate (counts/sec), computed once by
     *             {@link #onTide(TideReading)}
     * @return true when the flood collapse ran and the tide is handled
     */
    private boolean floodCollapse(boolean floodSig, double rate) {
      // The lock clears when the volume drops below the flood rate (a
      // genuine regime change); the seed floor's noise role stays intact
      // (a high-volume window under the SEED floor is designed behavior —
      // the collapse gate requires floor > seed). floodSig is the flood
      // signature precomputed once in onTide; its conditions subsume
      // rate >= FLOOD_RATE_PER_SEC, so the collapse branch only adds the
      // floor-above-seed test (no recomputation on the distress path).
      if (rate < FLOOD_RATE_PER_SEC) {
        state.floodLock = false;
      } else if (isAbovePromoteFloor() && floodSig) {
        collapse();
        state.floodLock = true;
        return true;
      }
      return false;
    }

    /**
     * Renewal fold with the workload-shift stand-down (R3; Caffeine's
     * {@code Sample.previousHitRate} + {@code Anchor.standDown} +
     * {@code Rates.update}): a single-tide move of the goal metric at or
     * above {@link #RESTART_THRESHOLD} announces a regime change — the
     * parked machine discards its references (the goal-metric memory, the
     * noise references, the distress streak; the anchor only when the
     * shift happened AT the anchor position) and re-learns from here
     * instead of smoothing across the shift. The walk and retreat
     * machinery — which EXPECTS renewal swings — is untouched; the audit
     * clock survives (position stillness, not the rate). The stand-down
     * runs BEFORE the fold (Caffeine's Rates.reset-then-update ordering):
     * the shift tide's sample is the FIRST sample of the new regime and
     * seeds the re-learned references. The deviation then folds against
     * the PRE-update smoothed renewal, as one EMA pair (O(1) per tide).
     */
    private void foldRenewal(double renewal) {
      boolean workloadShift = !Double.isNaN(lastRenewal) && Math.abs(renewal - lastRenewal) >= RESTART_THRESHOLD;
      lastRenewal = renewal;
      // Stand-down is deferred to the parked state on purpose: the walk and retreat
      // verdicts use their own frozen renewal, never the live anchor. The anchor is
      // re-learned only on a confirm or a parked stand-down; an unconfirmed walk yields
      // no new anchor evidence. If a verdict ever reads the live anchor, this gate must
      // broaden to match the stand-down or a stale anchor survives the experiment.
      if (workloadShift && !isWalking() && state.retreatStage == RETREAT_STAGE_STOPPED) {
        standDown();
      }
      if (Double.isNaN(rates.smoothedRenewal)) {
        rates.smoothedRenewal = renewal;
      } else {
        rates.renewalDeviation +=
          RENEWAL_SMOOTHING * (Math.abs(renewal - rates.smoothedRenewal) - rates.renewalDeviation);
        rates.smoothedRenewal += RENEWAL_SMOOTHING * (renewal - rates.smoothedRenewal);
      }
    }

    /**
     * One budgeted retreat tide: the floor walks back to
     * {@link Positions#retreatTarget} in at most {@link #RETURN_BUDGET} convergent
     * strides.
     *
     * <p>A veto-armed retreat first HOLDS for {@link #PROBE_STREAK} tides and samples
     * the live renewal: a recovery re-plants the anchor at the held position; a
     * persistent shortfall deepens the target. The stage-0 descent reads the live renewal
     * on every stride — a rung that earns the target parks there and re-plants the
     * anchor; a descent under-earning its own start for {@link #SLOPE_STREAK} tides is
     * falsified and pivots.
     *
     * @param renewal the tide's live renewal — the probe's verdict signal
     * @return true when a retreat was in flight and the tide is handled
     */
    private boolean retreat(double renewal) {
      if (state.retreatStage == RETREAT_STAGE_STOPPED) {
        return false;
      }

      if (state.retreatStage > RETREAT_STAGE_DOING) {
        state.retreatStage--;
        if (renewal >= RENEWAL_TARGET) {
          // The veto's direction hypothesis was wrong — the confirmed
          // position still earns. Settle here and re-plant the anchor:
          // this becomes the position future vetoes back off to.
          plant(renewal, /* moveFloorToTarget= */ true, /* discardAnchor= */ false);
        } else if (state.retreatStage == RETREAT_STAGE_DOING) {
          // The hold keeps under-earning: the veto direction holds.
          // Deepen to the last verified position (the veto anchor).
          positions.retreatTarget = positions.anchorD;

          state.retreatStartRenewal = renewal;
        }
        return true;
      }

      if (state.retreatFromVeto) {
        if (renewal >= RENEWAL_TARGET) {
          // Mid-descent recovery: this rung earns the target — park here
          // (the floor is already at it) and re-plant the anchor; the
          // descent STOPS instead of walking past the first healthy rung
          // to the interval/base that was never re-measured.
          plant(renewal, /* moveFloorToTarget= */ false, /* discardAnchor= */ false);
          return true;
        }
        if (renewal < state.retreatStartRenewal - vetoMargin()) {
          state.retreatWorseCount++;
          if (state.retreatWorseCount >= SLOPE_STREAK) {
            // The descent keeps under-earning the retreat's own start — the
            // slope falsifies the down direction. Stop the retreat and
            // discard the stale claim; the parked distress re-arms the raise
            // direction (the reverse attempt) from the pivot rung.
            plant(0.0, /* moveFloorToTarget= */ false, /* discardAnchor= */ true);
            state.distressTides = 0;
            return true;
          }
        } else {
          state.retreatWorseCount = 0;
        }
      }

      int stride = Math.abs(positions.floor - positions.retreatTarget);
      if (stride <= 1 || state.retreatStepsLeft <= 0) {
        positions.floor = positions.retreatTarget;
        state.retreatStage = RETREAT_STAGE_STOPPED;
      } else {
        positions.floor = positions.floor + direction() * Math.min(STEP_RETURN_MAX, decayStride(stride));
        state.retreatStepsLeft = state.retreatStepsLeft - 1;
      }
      return true;
    }

    /**
     * Re-plant the veto anchor and stop the retreat: optionally move the floor to the
     * held {@code retreatTarget}, then either re-plant the anchor at the (new) floor or
     * discard it (the falsified-claim case), and reset the retreat machinery.
     *
     * @param renewal           the live renewal to freeze as the anchor reference (0 when discarding)
     * @param moveFloorToTarget whether the floor moves to {@code retreatTarget}
     * @param discardAnchor     whether the anchor is cleared instead of re-planted
     */
    private void plant(double renewal, boolean moveFloorToTarget, boolean discardAnchor) {
      if (moveFloorToTarget) {
        positions.floor = positions.retreatTarget;
      }

      if (discardAnchor) {
        positions.anchorD_Renewal = 0.0;
        positions.anchorD = 0;
      } else {
        positions.anchorD = positions.floor;
        positions.anchorD_Renewal = renewal;
      }

      state.retreatStage = RETREAT_STAGE_STOPPED;
      state.retreatStepsLeft = 0;
      positions.retreatTarget = 0;
      state.retreatWorseCount = 0;
      state.retreatFromVeto = false;
    }

    /**
     * One walk tide: the raise or release walk in flight advances one
     * step per tide — the goal metric is the health test itself. A
     * raise confirms — keeps the raised floor — only after the set holds
     * the target across the same persistence that crashes it (a single
     * lucky tide must not keep a raise; the audit, the release walk and
     * the admit-on-block release an over-raise), and crashes when
     * distress persists through the crash persistence. The confirm
     * plants the veto anchor at the position the raise left from (the
     * DEEP target), so a later distress probes the direction before it
     * undoes a raise that failed to recover the signal.
     * The release direction prices the goal metric against the anchor
     * memory (WindowClimber's anchor veto): a crash that persists for
     * PROBE_CRASH_PERSISTENCE consecutive tides undoes the walk (budgeted
     * return to the base); a walk that stays healthy for the full budget
     * confirms; a budget spent entirely below the arming renewal is a
     * FAILED experiment (R5's beatBase).
     *
     * @return true when a walk was in flight and the tide is handled
     */
    private boolean walk(double renewal, double hotColdRatio, int blockedKeys, long boundary) {
      if (!isWalking()) {
        return false;
      }
      if (isWalkUp()) {
        return switch (raiseEnding(renewal)) {
          case CONFIRMED -> confirmRaiseWalk(blockedKeys, boundary);
          case CRASHED -> undoWalk(RAISE, WalkEnding.CRASHED);
          case FAILED -> undoWalk(RAISE, WalkEnding.FAILED);
          case WALKING -> handleRaiseWalkWalking(renewal, hotColdRatio);
        };
      } else {
        return switch (releaseEnding(renewal)) {
          case CONFIRMED -> confirmReleaseWalk(renewal);
          case CRASHED -> undoWalk(RELEASE, WalkEnding.CRASHED);
          case FAILED -> undoWalk(RELEASE, WalkEnding.FAILED);
          case WALKING -> handleReleaseWalkWalking();
        };
      }
    }

    /**
     * The parked distress decision (renewal below the target): the
     * blocked band is the stale tail of the 4-tide membership memory —
     * its keys are already below the floor, so a drop would re-admit
     * pollution and a raise cannot reach them; the tail self-heals by
     * decay within four tides. The only evidence-based action is the
     * bounded raise-walk, armed when the hot set is genuinely
     * under-earning the cold reservoir. Movement DECAYS the audit run
     * instead of zeroing it (Caffeine's AuditClock.tick): a hard reset
     * would let one floor move per wait suppress audits forever.
     */
    private void distress(double renewal, int blockedKeys, double hotColdRatio, boolean floodSig) {
      state.auditTides = Math.max(0, state.auditTides - 1);
      state.distressTides = state.distressTides + 1;
      // The veto margin is computed only when the veto can actually
      // fire — the evidence-gap conditions are cheap, the margin is an
      // O(1) EMA read (see {@link #vetoMargin()}): most distress
      // tides never reach it.
      if (shouldRetreatToAnchor(renewal)) {
        // A raise above the last confirmed anchor failed to recover the
        // signal — but the retreat's DIRECTION (down toward the anchor) is
        // only a hypothesis (design decisions: counting engine): before committing, HOLD the veto
        // position and sample the live renewal for PROBE_STREAK tides (the
        // direction stride probe — WindowClimber's "only keep a position on
        // a clear verdict"). A recovery re-plants the anchor at the held
        // position and cancels the retreat; a persistent shortfall deepens
        // to the veto anchor. The veto evidence gates remain
        // WindowClimber's anchor veto unchanged: floor >
        // anchorD + ANCHOR_BAND keeps a position at or within a jitter
        // band of its anchor from retreating into itself — the move would
        // change nothing (the threshold is max(floor, boundary)) and costs
        // flicker.
        positions.retreatTarget = positions.floor;
        state.retreatStage = RETREAT_STAGE_PROBE;
        state.retreatStepsLeft = RETURN_BUDGET;
        state.distressTides = 0;
        // (design decisions: counting engine): the descent later slopes against the veto's own
        // sample — "worse than where the retreat started" is the
        // falsification of the down hypothesis.
        state.retreatFromVeto = true;
        state.retreatStartRenewal = renewal;
        state.retreatWorseCount = 0;
      } else if (shouldArmRaiseWalk(hotColdRatio, blockedKeys, floodSig)) {
        // Arm the raise-walk WITHOUT taking the first step: the base is
        // frozen at the position the experiment leaves from, and the
        // first step is taken on the next distressed tide that survives
        // the evidence gates. This one-tide delay prevents a single
        // noisy distress sample from moving the floor before the walk
        // has produced a second sample (design decisions: counting engine probe hygiene). The
        // density ratio confirms the slots are genuinely under-earning
        // before the machine spends a walk on a mixed signal.
        walk = new Walk(positions.floor, renewal, /* up= */ true);
        state.auditTides = state.distressTides = 0;
      }
      // else: the self-healing stale tail — no action (documented).
    }

    /**
     * The parked healthy decision: admit-on-block drops the floor toward
     * the boundary in one move when genuinely hot keys are blocked behind
     * the floor (renewal-disambiguated — under distress the same band is
     * the stale tail and must not be admitted); otherwise a saturated set
     * or a sustained healthy-and-still position (the audit clock) arms
     * the release walk one step at a time.
     */
    private void healthy(double renewal, int remain, int hotLimit, int blockedKeys, long boundary) {
      state.distressTides = 0;
      if (hasBlockedKeysAboveBoundary(blockedKeys, boundary)) {
        // Admit on block — only on the HEALTHY path (renewal-
        // disambiguated): blocked keys are exactly the renewing keys the
        // boundary would admit — excluding them starves the hot set.
        // Drop the floor toward the boundary so they qualify from the
        // next tide on. Under distress the same band is the stale tail
        // and must not be admitted. Clamped: a boundary above
        // GOVERNOR_FLOOR_MAX keeps the floor inside its documented range
        // (inert — the threshold is max(floor, boundary) anyway).
        positions.floor = effectiveBoundary(boundary);
      } else {
        boolean saturated = remain >= SATURATION_FRACTION * hotLimit;
        if ((saturated || state.auditTides >= auditWait()) && isAbovePromoteFloor()) {
          if (ladder.isBackingOff(RELEASE)) {
            // Refractory: hold the position after a crashed walk — a fresh
            // release would immediately re-test the just-failed direction.
            state.auditTides++;
            return;
          }

          // Arm the release walk BEFORE the first step: the base is frozen
          // at the position the experiment leaves from. The first step
          // uses the same stride law as the walk: at the arm the smoothed
          // signal is the arm renewal, so the stride is exactly the
          // initial probe step ({@link #STEP_INITIAL} =
          // initial * margin / max(0.1, margin)),
          // deterministic and independent of the raise direction's
          // density-priced stride (design decisions: counting engine).
          walk = new Walk(positions.floor, renewal, /* up= */ false);
          // (audit-clock reschedule, R2): the deep-fail wait has been
          // served — the fresh walk re-tests the direction at the
          // standard cadence from here.
          ladder.deepFail = false;
          positions.floor = computeNextReleaseStep();
          state.auditTides = 0;
        } else {
          state.auditTides++;
        }
      }
    }

    /**
     * One density-priced step up (raise walk): the stride is proportional to the
     * hot/cold density shortfall,
     * {@code clamp(round(STEP_INITIAL * min(1, -log2(hotColdRatio))), 1, STEP_INITIAL)}.
     * Far below the 1.0 gate the walk strides at the full initial step; as the ratio
     * approaches it the stride converges to 1, approaching the earners' capacity without
     * overshooting. A pure function of this tide's density reading.
     *
     * @param hotColdRatio the tide's hot earnings per occupied slot divided by the cold
     *                     reservoir's earnings per key (the gate requires {@code < 1})
     */
    private void computeNextRaiseStep(double hotColdRatio) {
      double ratio = Double.isFinite(hotColdRatio) && hotColdRatio > 0.0 ? hotColdRatio : 0.5;
      // log1p keeps the near-1.0 gate numerically stable.
      double magnitude = -Math.log1p(ratio - 1.0) * INV_LN_2;
      int stride = (int) Math.round(STEP_INITIAL * Math.min(1.0, magnitude));
      stride = Math.max(1, stride);
      positions.floor = Math.min(FLOOR_MAX, positions.floor + stride);
    }

    /**
     * One noise-priced step down (release walk): the stride law, clamped
     * at the seed. The single shared release movement — the arm and the
     * walk branch both move the floor this way, so the first step and
     * every WALKING tide are priced by the same law.
     *
     * @return the next floor position, clamped to the seed
     */
    private int computeNextReleaseStep() {
      return Math.max(PROMOTION_FLOOR, positions.floor - releaseStride());
    }

    /**
     * The evidence gap required before a veto/undo fires: the fixed
     * {@link #MIN_TIDAL_EVIDENCE}, widened by twice the smoothed renewal
     * deviation (WindowClimber's {@code Rates.noiseBand} pricing) so noisy
     * workloads are not hair-triggered by ordinary jitter. Every caller runs after
     * {@link #foldRenewal(double)} in the same tide, so the smoothed signal is seeded and the
     * gap always has a measured deviation behind it.
     */
    private double vetoMargin() {
      return Math.max(MIN_TIDAL_EVIDENCE, 2.0 * rates.renewalDeviation);
    }

    /**
     * The release-walk stride for the current tide, priced by the
     * smoothed renewal against the walk's own crash bar (the same
     * anchor-memory reference the verdict judges):
     * {@code clamp(round({@link #STEP_INITIAL} * (smoothed - bar) / noise), 1, {@link #STRIDE_MAX})}
     * with {@code bar = baseRenewal - margin} and {@code noise =
     * max(MIN_TIDAL_EVIDENCE, margin)}. At the arm the smoothed signal is
     * the arm renewal, so the first stride is exactly the initial probe
     * step ({@link #STEP_INITIAL}); as the signal approaches the bar
     * the stride self-converges
     * to 1, and a below-bar signal (the crash zone) creeps while the
     * verdict accumulates its persistence. The smoothed signal is always seeded here: the walk
     * branch runs after {@link #foldRenewal(double)} on the same tide.
     */
    private int releaseStride() {
      double noise = vetoMargin();
      double bar = releaseBar(noise);

      int stride = (int) Math.round((STEP_INITIAL * (rates.smoothedRenewal - bar)) / noise);
      return Math.max(1, Math.min(STRIDE_MAX, stride));
    }

    /**
     * The release-walk crash bar: the walk's arming renewal minus the noise
     * margin — the single reference both the verdict
     * ({@link #releaseEnding(double)}) and the stride law
     * ({@link #releaseStride()}) judge the live renewal against (same
     * anchor-memory reference, same floor-at-zero clamp).
     */
    private double releaseBar(double margin) {
      return Math.max(0.0, walk.baseRenewal - margin);
    }

    /*-------- Little Helper --------*/

    /**
     * Positions — plain fields: the adaptive promotion floor (seeded by the constructor
     * parameter), the retreat target, the veto anchor and its renewal reference.
     */
    private static final class Positions {

      /**
       * Veto anchor renewal: the renewal reference frozen at the confirm of the
       * last walk (see {@link #anchorD}).
       */
      double anchorD_Renewal;
      int retreatTarget;
      int anchorD;
      int floor;

      private Positions(int seedFloor) {
        floor = seedFloor;
      }

      /**
       * Full wipe: the floor returns to the seed, the anchor and the
       * retreat target clear.
       */
      void reset() {
        floor = PROMOTION_FLOOR;
        retreatTarget = 0;
        anchorD = 0;
        anchorD_Renewal = 0;
      }
    }

    /**
     * State — plain fields: the retreat probe/persistence flags, distress, audit and
     * flood state.
     */
    private static final class State {

      int retreatStage = RETREAT_STAGE_STOPPED;
      boolean retreatFromVeto;
      int retreatWorseCount;
      int retreatStepsLeft;
      boolean floodLock;
      int distressTides;
      int auditTides;

      /**
       * (design decisions: counting engine) descent slope probe: the descent's own reference — the
       * renewal where the descent started (the veto-arm sample, refreshed by
       * the last probe sample when the probe deepens). The descent direction
       * (down toward the anchor) is falsified when the live renewal keeps
       * under-earning this reference minus the noise margin for
       * {@link MoonsTidalForce#SLOPE_STREAK} tides. Kept as a plain
       * {@code double}: a renewal ratio carries no fixed-point encoding
       * without precision loss.
       */
      double retreatStartRenewal;

      /**
       * Full wipe: the distress/audit streaks, the retreat machinery and the
       * descent slope reference clear. {@code floodLock} deliberately
       * survives — a wipe that cleared it would re-arm into the same flood.
       */
      void reset() {
        auditTides = 0;
        distressTides = 0;
        retreatStepsLeft = 0;
        retreatWorseCount = 0;
        retreatFromVeto = false;
        retreatStartRenewal = 0.0;
        retreatStage = RETREAT_STAGE_STOPPED;
      }
    }

    /**
     * Rates — the smoothed-renewal EMA pair (Caffeine's {@code Rates}): the
     * renewal mean and its mean absolute deviation, folded as one EMA pair by
     * {@link MoonsTidalForce#foldRenewal(double)}. Both fields reset
     * together so the pair's seeding stays consistent (R3's reset-then-update
     * ordering).
     */
    private static final class Rates {

      /**
       * Smoothed renewal signal (Caffeine's {@code Rates.smoothed}): the EMA
       * mean of the per-tide renewals, {@code NaN} = unseeded. The release
       * stride law prices its stride against this signal; the deviation EMA
       * ({@link #renewalDeviation}) is updated against the PRE-update
       * smoothed value, as an EMA pair (Caffeine's {@code Rates.update}).
       */
      double smoothedRenewal = Double.NaN;

      /**
       * EMA of the per-tide renewal absolute deviation (Caffeine's
       * {@code Rates.deviation}) — the noise source for
       * {@link MoonsTidalForce#vetoMargin()} and the release stride law.
       * Seeded wide ({@link MoonsTidalForce#DEVIATION_SEED}) so a cold state
       * cannot veto early; only the deliverer writes it.
       */
      double renewalDeviation = DEVIATION_SEED;

      /** The R3 re-seed: unseeded mean, wide deviation. */
      void reset() {
        smoothedRenewal = Double.NaN;
        renewalDeviation = DEVIATION_SEED;
      }
    }

    /**
     * Regime — the P1 regime-switch debounce accumulators (wall-clock ms),
     * saturating and deliverer-only. (The volume EWMA itself stays a
     * governor field — {@code volumeRate}.)
     */
    private static final class Regime {

      private int quietMs;
      private int noiseMs;
    }

    /**
     * Ladder — one independent retry ledger per walk direction, selected by
     * the internal {@link Direction} enum. The pricing semantics are Caffeine's Ladder: a single
     * crash without escalation does not move the rung, a consecutive crash
     * run of {@link #PROBE_CRASH_ESCALATION} doubles it, a completed FAILED
     * experiment always doubles, a CONFIRMED walk rewards it back to rung 1,
     * and the backoff served by {@code left} gates re-arms.
     */
    private static final class Ladder {

      private enum Direction {
        RAISE,
        RELEASE,
      }

      private static final class Ledger {

        int rung = 1;
        int left;
        int crashStreak;
      }

      final Ledger raise = new Ledger();

      final Ledger release = new Ledger();

      /**
       * Release-only deep-fail flag (R2's audit reschedule): set when a
       * completed FAILED release lands on the deepest rung — the escalation
       * that stretches {@code auditWait()} past the backoff's reach. Held
       * on the Ladder itself (not the per-direction ledgers) because only
       * the release direction can ever earn it.
       */
      boolean deepFail;

      private void crash(Direction direction) {
        Ledger ledger = direction == RAISE ? raise : release;
        ledger.crashStreak++;
        if (ledger.crashStreak >= PROBE_CRASH_ESCALATION) {
          ledger.rung = Math.min(TIDAL_BACKOFF_MAX, Math.max(TIDAL_BACKOFF_INITIAL, ledger.rung * 2));
        }
        ledger.left = Math.max(TIDAL_BACKOFF_INITIAL, ledger.rung);
      }

      private void fail(Direction direction) {
        Ledger ledger = direction == RAISE ? raise : release;
        ledger.crashStreak = 0;
        ledger.left = ledger.rung = Math.min(TIDAL_BACKOFF_MAX, Math.max(TIDAL_BACKOFF_INITIAL, ledger.rung * 2));
      }

      private void reward(Direction direction) {
        Ledger ledger = direction == RAISE ? raise : release;
        ledger.crashStreak = 0;
        ledger.rung = 1;
      }

      private boolean isBackingOff(Direction direction) {
        return (direction == RAISE ? raise : release).left > 0;
      }

      private void tick(Direction direction) {
        Ledger ledger = direction == RAISE ? raise : release;
        if (ledger.left > 0) {
          ledger.left--;
        }
      }
    }

    /**
     * Folds one tide's total volume into the rate EWMA.
     *
     * @param snapshotSum the tide's total add
     * @param windowMs    the tide's measured accumulation window (positive)
     */
    private void onVolume(long snapshotSum, long windowMs) {
      double rate = (snapshotSum * 1000.0) / Math.max(1L, windowMs);
      volumeRate = (volumeRate == 0.0) ? rate : volumeRate + VOLUME_ALPHA * (rate - volumeRate);
    }

    /** Returns true when the rate EWMA is below the quiet threshold. */
    private boolean quietVolume() {
      return volumeRate < QUIET_RATE_PER_SEC;
    }

    /** Returns true if any backoff or ladder pricing state is active. */
    private boolean isInflight() {
      return (ladder.raise.left > 0 || ladder.raise.rung > 1 || ladder.release.left > 0 || ladder.release.rung > 1);
    }

    /** Returns true when the floor is above the absolute seed. */
    private boolean isAbovePromoteFloor() {
      return positions.floor > PROMOTION_FLOOR;
    }

    /** Returns the retreat direction: +1 up, -1 down. */
    private int direction() {
      return positions.retreatTarget > positions.floor ? 1 : -1;
    }

    /**
     * Returns a convergent retreat stride (half the remaining distance plus one).
     */
    private int decayStride(int stride) {
      return stride / 2 + 1;
    }

    /** Returns the boundary clamped to the governor's [seed, ceiling] range. */
    private int effectiveBoundary(long boundary) {
      return (int) Math.min(FLOOR_MAX, Math.max(boundary, PROMOTION_FLOOR));
    }

    /**
     * Returns true when blocked keys exist and the floor over-filters above the
     * boundary.
     */
    private boolean hasBlockedKeysAboveBoundary(int blockedKeys, long boundary) {
      return blockedKeys > 0 && positions.floor > effectiveBoundary(boundary);
    }

    /** Returns true when a walk is currently in flight. */
    private boolean isWalking() {
      return walk != null;
    }

    /** Returns true when the in-flight walk is a raise walk. */
    private boolean isWalkUp() {
      return isWalking() && walk.up;
    }

    private void endWalk() {
      walk = null;
    }

    /**
     * Whether the raise walk should take a step this tide (evidence-gated
     * step, design decisions: counting engine): the set must still be distressed but
     * not completely dead, the hot slots must genuinely under-earn the cold
     * reservoir, and the floor must have room to climb.
     */
    private boolean shouldStepRaiseWalk(double renewal, double hotColdRatio) {
      return renewal > 0.0 && belowHealthyRenewal(renewal) && hotColdRatio < 1.0 && positions.floor < FLOOR_MAX;
    }

    /** Returns true when the renewal is below the healthy target (distress). */
    private boolean belowHealthyRenewal(double renewal) {
      return renewal < RENEWAL_TARGET;
    }

    /**
     * Whether the current distress is severe and persistent enough to veto
     * the last confirmed raise and retreat the floor back to its anchor:
     * continuous distress exceeds the veto streak, the floor stands clearly
     * above the anchor (outside the jitter band), and the renewal is below
     * the anchor reference by more than the noise margin.
     */
    private boolean shouldRetreatToAnchor(double renewal) {
      return (
        state.distressTides > VETO_STREAK &&
        positions.floor > positions.anchorD + ANCHOR_BAND &&
        renewal < (positions.anchorD_Renewal - vetoMargin())
      );
    }

    /**
     * Whether the parked distress state should arm a new raise walk (without
     * taking its first step): the floor sits at or above the seed, the hot set is genuinely
     * under-earning the cold reservoir, there are blocked keys, the floor has room to climb, the
     * raise ladder is not backing off, distress has persisted long enough, and the flood lock /
     * signature are absent (a flood collapse would undo the raise anyway).
     */
    private boolean shouldArmRaiseWalk(double hotColdRatio, int blockedKeys, boolean floodSig) {
      return (
        positions.floor >= PROMOTION_FLOOR &&
        hotColdRatio < 1.0 &&
        blockedKeys > 0 &&
        positions.floor < FLOOR_MAX &&
        !ladder.isBackingOff(RAISE) &&
        state.distressTides >= RAISE_ARM_DELAY &&
        // (flood suppression, design decisions: counting engine): under the flood signature or the
        // flood lock the floor would be FLOOD-collapsed on this or the
        // next tide — a raise is the wrong response to a stale floor
        // blocking the whole distribution (and the empty-set collapse
        // would undo it anyway). floodSig is precomputed once in onTide.
        !state.floodLock &&
        !floodSig
      );
    }

    /**
     * How a walk ends (Caffeine's {@code ProbeEnding}): the verdict
     * methods below only DECIDE the ending — the walk branch acts on it,
     * so the decision and the mutation stay separate.
     */
    private enum WalkEnding {
      /**
       * Below-target persistence: undo to the frozen base, deepening the layer's
       * ladder.
       */
      CRASHED,
      /**
       * Budget spent without a verdict: undo, priced as a completed, failed
       * experiment.
       */
      FAILED,
      /**
       * The walk validated its position: keep it, plant the anchor, reward the
       * ladder.
       */
      CONFIRMED,
      /** No ending fired: take the next bold-driver stride. */
      WALKING,
    }

    /**
     * Computes how a raise walk ends this tide (Caffeine's {@code probeEnding}).
     */
    @SuppressWarnings("java:S3358")
    private WalkEnding raiseEnding(double renewal) {
      walk.samples++;
      if (!belowHealthyRenewal(renewal)) {
        walk.crashStreak = 0;
        if (++walk.healthyStreak >= TIDAL_CRASH_PERSISTENCE) {
          return WalkEnding.CONFIRMED;
        }
        // The budget check holds on BOTH branches, so an alternating walk cannot outlive
        // TIDAL_WALK_BUDGET; a spent budget becomes a priced FAILED verdict.
        return (walk.samples >= TIDAL_WALK_BUDGET) ? WalkEnding.FAILED : WalkEnding.WALKING;
      } else {
        walk.healthyStreak = 0;

        return (++walk.crashStreak >= TIDAL_CRASH_PERSISTENCE)
          ? WalkEnding.CRASHED
          : (walk.samples >= TIDAL_WALK_BUDGET)
            ? WalkEnding.FAILED
            : WalkEnding.WALKING;
      }
    }

    /**
     * Computes how a release walk ends this tide (Caffeine's {@code probeEnding}).
     */
    private WalkEnding releaseEnding(double renewal) {
      double bar = releaseBar(vetoMargin());
      walk.crashStreak = (renewal < bar) ? walk.crashStreak + 1 : 0;
      if (walk.crashStreak >= TIDAL_CRASH_PERSISTENCE) {
        return WalkEnding.CRASHED;
      }

      // (beatBase, R5; Caffeine's audit confirm): the confirm requires
      // the goal metric to have matched or beaten the walk's start at
      // least once — a budget spent entirely below the arming level is a
      // confirm against a colder reference (a false confirm). The test
      // is inclusive (a saturating arming sample makes any
      // strictly-greater bar unsatisfiable). An unconfirmed budget is
      // priced as a completed FAILED experiment (undo + ladder
      // escalation), exactly like the raise direction.
      walk.beatBase |= renewal >= walk.baseRenewal;
      if (++walk.samples >= TIDAL_WALK_BUDGET) {
        return walk.beatBase ? WalkEnding.CONFIRMED : WalkEnding.FAILED;
      }
      return WalkEnding.WALKING;
    }

    /**
     * Undoes a walk to its frozen base: budgeted return + the layer's ladder
     * pricing.
     */
    private boolean undoWalk(Ladder.Direction direction, WalkEnding ending) {
      // An unconfirmed walk yields NO new anchor evidence: do NOT touch
      // anchorD/anchorD_Renewal here (unlike the confirm paths). The
      // anchor is only re-learned on a confirm or a parked standDown; a
      // mid-walk workload shift is deferred to that parked re-learn by the
      // narrow R3 gate (see the invariant comment at foldRenewal). Ending the
      // walk returns the floor to its frozen base via the budgeted descent.
      positions.retreatTarget = walk.baseFloor;
      if (positions.floor != walk.baseFloor) {
        state.retreatStepsLeft = RETURN_BUDGET;
        state.retreatStage = RETREAT_STAGE_DOING;
      } else {
        // A walk that crashed or failed before moving has nothing to return to. Entering the
        // retreat state would spend a whole tide on a zero-step descent and swallow that tide's
        // distress/healthy decision.
        state.retreatStepsLeft = 0;
        state.retreatStage = RETREAT_STAGE_STOPPED;
        positions.retreatTarget = 0;
      }
      state.retreatFromVeto = false;
      state.retreatStartRenewal = 0.0;
      state.retreatWorseCount = 0;
      endWalk();

      if (ending == WalkEnding.FAILED) {
        ladder.fail(direction);
        // (audit-clock reschedule, R2): a completed FAILED release at the
        // deepest rung doubles the audit wait (WindowClimber's reschedule
        // law — the backoff's own wait covers rungs below the maximum, so
        // this is the only place the re-test cadence can stretch past it).
        // A crash is priced as a workload shift and keeps the cadence. The
        // flag is RELEASE-only — the raise direction never reads it, so the
        // dead raise-side write is gone.
        if (direction == RELEASE && ladder.release.rung >= TIDAL_BACKOFF_MAX) {
          ladder.deepFail = true;
        }
      } else {
        ladder.crash(direction);
      }
      return true;
    }

    /**
     * Completes a confirmed raise walk: ends the walk, rewards the raise
     * ladder, plants the confirm shield (design decisions: counting engine), resets the audit clock,
     * and performs confirm-admit if the confirmed position still over-filters
     * (blocked band above the boundary). Plants the veto anchor at the
     * corrected floor when the correction fires, otherwise at the raise's base.
     *
     * @return true (the tide is fully handled)
     */
    private boolean confirmRaiseWalk(int blockedKeys, long boundary) {
      // (confirm-admit, design decisions: counting engine): the raise confirmed — the set
      // held the target across the crash persistence — but a
      // confirmed position with a non-empty blocked band
      // (floor > boundary, keys the boundary would admit) is
      // over-filtering: the parked branch would admit it next
      // tide; do it at confirmation instead, so the over-filter
      // window ends here (the 3-tide confirm streak is the
      // designed minimum; the rejected mid-walk admit aborted the
      // walk on the FIRST healthy tide and collapsed the confirm
      // mechanism — design decisions: counting engine).
      int baseFloor = walk.baseFloor;
      positions.anchorD_Renewal = walk.baseRenewal;
      endWalk();
      ladder.reward(RAISE);
      // (confirm shield, design decisions: counting engine): the raise ladder stays
      // refractory after a confirm (the alternating-workload
      // ratchet: arm -> 3 at-target -> confirm -> immediate re-arm
      // climbs the floor step by step until the empty-set
      // collapse undoes the probe). The rung is not touched —
      // only the wait — and the release ladder is separate.
      ladder.raise.left = CONFIRM_SHIELD;
      state.auditTides = 0;
      if (hasBlockedKeysAboveBoundary(blockedKeys, boundary)) {
        positions.anchorD = positions.floor = effectiveBoundary(boundary);
      } else {
        positions.anchorD = baseFloor;
      }

      return true;
    }

    /**
     * Advance a raise walk in the WALKING state: the walk steps only while
     * the set is still distressed and the evidence gates are satisfied
     * (design decisions: counting engine). When the gates pass, a density-priced
     * stride raises the floor; otherwise the walk holds and the confirmation
     * or crash streak continues to accumulate.
     *
     * @return true — the tide is fully handled by the raise walk
     */
    private boolean handleRaiseWalkWalking(double renewal, double hotColdRatio) {
      // The bold driver steps only while the set is still
      // distressed; at-target tides hold so the confirmation
      // streak can accumulate without moving.
      // The step requires SOME member to still earn the threshold (0 < renewal <
      // target) AND the hot slots to still under-earn the cold reservoir
      // (hotColdRatio < 1). A renewal of 0 cannot be helped by climbing; a ratio >= 1
      // means the members earn, so a step would push the threshold past the marginal
      // earners and eat the walk's own confirmation.
      if (shouldStepRaiseWalk(renewal, hotColdRatio)) {
        computeNextRaiseStep(hotColdRatio);
      }
      return true;
    }

    /**
     * Completes a confirmed release walk: plants the veto anchor at the
     * descended floor, records the anchor renewal, ends the walk and rewards
     * the release ladder. Returns true (the tide is fully handled).
     */
    private boolean confirmReleaseWalk(double renewal) {
      positions.anchorD = positions.floor;
      positions.anchorD_Renewal = renewal;
      endWalk();
      ladder.reward(RELEASE);
      return true;
    }

    /**
     * Advances a release walk in the WALKING state: if the floor is still
     * above the seed, takes one noise-priced step down (the stride shrinks as
     * the signal approaches the crash bar). Returns true (the tide is fully
     * handled).
     */
    private boolean handleReleaseWalkWalking() {
      // The stride is priced by the smoothed renewal (the Rates EMA pair) against the
      // walk's own crash bar (baseRenewal - margin, the same reference the verdict
      // judges). Comfortably above the bar the walk strides boldly (up to the ceiling);
      // approaching the bar the stride shrinks toward 1 so the verdict samples the
      // decision zone finely; below it the walk creeps while crash persistence
      // accumulates. The raise direction prices its stride independently.
      if (isAbovePromoteFloor()) {
        positions.floor = computeNextReleaseStep();
      }
      return true;
    }

    /**
     * The audit due-wait for the release direction (WindowClimber's
     * {@code AuditClock.reschedule}): the standard {@link #AUDIT_WAIT}
     * healthy tides, stretched to {@link #AUDIT_WAIT_MAX} after a
     * completed FAILED release at the deepest ladder rung
     * ({@code ladder.deepFail}). The rung's own backoff already
     * throttles intermediate rungs (the audit run accumulates during the
     * backoff, so the inter-attempt gap is max(backoff, wait) — the wait
     * law binds exactly where the backoff cannot).
     */
    private int auditWait() {
      return ladder.deepFail ? AUDIT_WAIT_MAX : AUDIT_WAIT;
    }

    /**
     * A walk in flight (Caffeine's WindowClimber {@code Walk}): the floor
     * is moved one step per tide while the goal metric stays on the
     * experiment's side. The base (position AND renewal) is frozen at the
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
      /** R5: whether the goal metric ever matched or beat the arming renewal. */
      boolean beatBase;

      Walk(int baseFloor, double baseRenewal, boolean up) {
        this.baseFloor = baseFloor;
        this.baseRenewal = baseRenewal;
        this.up = up;
      }
    }
  }
}
