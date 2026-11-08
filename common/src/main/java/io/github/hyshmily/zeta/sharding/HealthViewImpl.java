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
package io.github.hyshmily.zeta.sharding;

import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatMessage;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Cluster-wide view of Worker health, maintained by consuming periodic
 * {@link WorkerHeartbeatMessage} broadcasts from all Worker nodes.
 *
 * <p>This is the central health authority for the local app instance. It tracks
 * every known Worker by ID with its epoch (restart counter), last heartbeat
 * timestamp and readiness flag.
 *
 * <p><b>Health judgment:</b> The cluster is considered healthy when at least one
 * third of the Workers observed via heartbeats (rounded up, minimum 1) are alive
 * (ready and within their effective liveness window). When the cluster
 * becomes unhealthy, the system may enter graceful degradation mode
 * (see design decisions: hot state and design decisions: heartbeat and health).
 *
 * <p><b>Restart detection:</b> When a Worker's epoch in a new heartbeat exceeds the
 * stored epoch, its record is replaced — all state from the previous incarnation is
 * discarded. This prevents stale health state from surviving a Worker restart.
 *
 * <p><b>Thread safety:</b> Records are read without a lock. Every mutation —
 * heartbeat, removal and record replacement — runs under {@link #recordLock}, so
 * a per-Worker update is atomic without a {@code ConcurrentHashMap.compute} bin
 * lock and without a second table. The cached health judgment is one packed
 * {@code long} republished by CAS against the generation it was computed from.
 *
 * @see WorkerHeartbeatMessage
 */
@Internal
@Slf4j
public class HealthViewImpl implements HealthView {

  private static final int HEALTH_TS_BITS = 42;
  private static final int HEALTH_GEN_SHIFT = HEALTH_TS_BITS + 1;
  private static final long HEALTH_TS_MASK = (1L << HEALTH_TS_BITS) - 1L;
  private static final long HEALTH_LOW_MASK = (1L << HEALTH_GEN_SHIFT) - 1L;

  /** Default cluster-health cache TTL: heartbeat cadence is seconds-level, 1s is ample. */
  public static final long DEFAULT_HEALTH_CACHE_TTL_MS = 1_000;

  private final long heartbeatTimeoutMs;
  private final long healthCacheTtlMs;
  /**
   * Per-Worker adaptive liveness timeouts (design decisions: heartbeat and health): a non-zero
   * value enables the mode and is the lower clamp; the record's Jacobson/Karels inter-arrival
   * estimate replaces the flat {@link #heartbeatTimeoutMs}, which stays as the warm-up window
   * and the ceiling. {@code 0} means adaptive timeouts are off.
   */
  private final long adaptiveFloorMs;

  /** Per-Worker health records. Read lock-free; every mutation runs under {@link #recordLock}. */
  private final ConcurrentMap<String, WorkerHealthRecord> records = new ConcurrentHashMap<>();

  /**
   * One packed cluster-health judgment: bit 0 is the verdict, bits 1..42 the biased
   * computation instant (zero means invalid), bits 43..63 the invalidation generation.
   * Cached for at most {@link #healthCacheTtlMs}; a mutation that can change the verdict
   * invalidates it (design decisions: heartbeat and health).
   */
  private final AtomicLong healthCache = new AtomicLong(0L);

  /**
   * Serializes every record mutation: creation, epoch replacement, heartbeat updates and
   * removal. The write rate is the heartbeat cadence (seconds-level), and no callback, I/O,
   * logging or repeated clock read runs while it is held; reads never take it.
   */
  private final Object recordLock = new Object();

  @Getter
  private volatile long lastAnyHeartbeatTime;

  private volatile int minAliveWorkers;

  public HealthViewImpl(long heartbeatTimeoutMs) {
    this(heartbeatTimeoutMs, DEFAULT_HEALTH_CACHE_TTL_MS, false, 0L);
  }

  /**
   * Creates a HealthViewImpl with adaptive per-Worker liveness timeouts.
   *
   * @param heartbeatTimeoutMs heartbeat timeout window (ms); with adaptive
   *                           timeouts this is the warm-up window and the ceiling
   * @param healthCacheTtlMs   cluster-health judgment cache TTL (ms)
   * @param adaptiveTimeout    whether liveness windows adapt per Worker
   * @param adaptiveFloorMs    lower clamp for the adaptive window; ignored when
   *                           {@code adaptiveTimeout} is off ({@code 0})
   */
  public HealthViewImpl(long heartbeatTimeoutMs, long healthCacheTtlMs, boolean adaptiveTimeout, long adaptiveFloorMs) {
    this.healthCacheTtlMs = healthCacheTtlMs;
    this.heartbeatTimeoutMs = heartbeatTimeoutMs;
    this.adaptiveFloorMs = adaptiveTimeout ? Math.max(1L, adaptiveFloorMs) : 0L;
  }

  /**
   * Override the minimum alive Worker threshold. Any change invalidates the
   * cached health judgment.
   *
   * @param minAliveWorkers minimum alive Workers for a healthy cluster; {@code <= 0}
   *                        restores the records-derived one-third threshold
   */
  public void setMinAliveWorkers(int minAliveWorkers) {
    this.minAliveWorkers = minAliveWorkers;
    invalidateHealthCache();
  }

  /**
   * Processes an incoming {@link WorkerHeartbeatMessage} from a Worker node and
   * updates the cluster health state accordingly.
   *
   * <p><b>New or restarted Worker:</b> If this is the first heartbeat from a Worker,
   * or if the heartbeat epoch exceeds the stored epoch (indicating a Worker restart),
   * its record is replaced with fresh timestamps and all state from the previous
   * incarnation is discarded.
   *
   * <p><b>Known Worker:</b> Updates the last alive evidence timestamp and readiness
   * flag. The inter-arrival sample that feeds the adaptive window is taken here, from
   * heartbeat arrivals only.
   *
   * <p><b>Replayed heartbeat:</b> An epoch below the stored one is a heartbeat from a
   * superseded incarnation. It is not evidence of anything and is dropped without a record
   * write, a last-heartbeat refresh or a cache invalidation.
   *
   * <p>The steady-state path (same epoch, the overwhelming majority of heartbeats) allocates
   * nothing, logs nothing and reads the clock once: the record is mutated in place, and the
   * cached cluster-health judgment is invalidated only when the Worker's liveness verdict
   * actually flips.
   *
   * @param hb the incoming heartbeat message; must not be null
   */
  @Override
  public void onHeartbeat(WorkerHeartbeatMessage hb) {
    boolean joined;
    boolean rejoined;
    boolean invalidate;
    long previousEpoch;
    synchronized (recordLock) {
      // Sampled under the lock: two heartbeats arriving out of scheduler order must not let the
      // older sample win the record and move lastAnyHeartbeatTime backwards.
      long now = TimeSource.monotonicMillis();
      WorkerHealthRecord record = records.get(hb.workerId());
      if (record != null && hb.epoch() < record.epoch) {
        return; // superseded incarnation: not evidence of anything
      }

      if (record == null || hb.epoch() > record.epoch) {
        records.put(hb.workerId(), new WorkerHealthRecord(hb.workerId(), hb.epoch(), now, hb.readyToServe()));
        previousEpoch = record == null ? UNKNOWN_EPOCH : record.epoch;
        joined = record == null;
        rejoined = !joined;
        invalidate = true;
      } else {
        boolean wasAlive = aliveAt(record, now);
        record.onHeartbeatSample(now, hb.readyToServe());
        invalidate = wasAlive != aliveAt(record, now);
        previousEpoch = UNKNOWN_EPOCH;
        rejoined = false;
        joined = false;
      }
      // Written under the lock so concurrent heartbeats cannot move the marker backwards.
      lastAnyHeartbeatTime = now;
    }

    // Logged after the lock so a slow appender cannot delay heartbeats. The decision was
    // made under the lock, which keeps a join/re-join report exactly-once under races.
    if (joined) {
      log.info("Worker joined cluster: {} (epoch={})", hb.workerId(), hb.epoch());
    } else if (rejoined) {
      log.info("Worker re-joined with new epoch: {} (epoch {} -> {})", hb.workerId(), previousEpoch, hb.epoch());
    }
    if (invalidate) {
      invalidateHealthCache();
    }
  }

  /**
   * Removes the health record for a Worker that has been confirmed dead.
   *
   * <p>Unknown (or already removed) Worker IDs are a no-op and leave the
   * cluster-health cache untouched.
   *
   * @param workerId the Worker to remove; must not be null
   */
  @Override
  public void removeRecord(String workerId) {
    synchronized (recordLock) {
      if (records.remove(workerId) == null) {
        return;
      }
    }
    invalidateHealthCache();
  }

  /**
   * Returns whether the cluster is currently considered healthy.
   *
   * <p>A Worker is considered alive when both conditions hold:
   * <ul>
   *   <li>{@code readyToServe == true} (Worker completed initialization)</li>
   *   <li>Time since the last heartbeat is below the effective window — the
   *       static heartbeat timeout, or the adaptive per-Worker estimate when an
   *       adaptive floor is configured</li>
   * </ul>
   *
   * <p>The cluster is healthy when the count of alive Workers is at least the
   * minimum threshold. When {@code minAliveWorkers} is not configured ({@code <= 0}),
   * the threshold is one third of the observed Worker count (rounded up, minimum 1),
   * where the observed count is the number of Workers ever seen via heartbeats and not
   * yet confirmed dead. This intentionally accepts a single surviving Worker as a
   * healthy cluster (see design decisions: heartbeat and health): degradation is triggered only when fewer than
   * one third of the observed Workers remain alive.
   *
   * @return {@code true} if the minimum alive Worker threshold is met;
   *         {@code false} otherwise
   */
  @Override
  public boolean isClusterHealthy() {
    // Cached judgment: a single volatile read on the hot path (COOL-key
    // promotion check). Recomputed when invalidated by a record mutation or
    // when the cache TTL elapsed — health is a seconds-level signal, so a
    // stale judgment inside the TTL window is never observable in practice.
    long now = TimeSource.monotonicMillis();
    long cached = healthCache.get();
    long computedMs = ((cached >>> 1) & HEALTH_TS_MASK) - 1L;
    if (computedMs >= 0L && now - computedMs < healthCacheTtlMs) {
      return (cached & 1L) != 0L;
    }

    boolean healthy = computeClusterHealthy(now);
    // Publish only against the generation this computation started from: an
    // invalidation advances the generation, so this CAS fails rather than overwriting
    // it. A failed publish still returns the freshly computed verdict.
    long next = (cached & ~HEALTH_LOW_MASK) | (((now + 1L) & HEALTH_TS_MASK) << 1) | (healthy ? 1L : 0L);
    healthCache.compareAndSet(cached, next);
    return healthy;
  }

  /**
   * Full cluster-health computation: count alive Workers against the minimum
   * threshold (see {@link #isClusterHealthy()} for the semantics).
   *
   * @param now the monotonic instant to judge every record against
   * @return {@code true} if the minimum alive Worker threshold is met
   */
  private boolean computeClusterHealthy(long now) {
    int minAlive = minAliveWorkers;
    int observed = 0;
    int aliveCount = 0;
    for (WorkerHealthRecord record : records.values()) {
      observed++;
      if (aliveAt(record, now)) {
        aliveCount++;
      }
    }
    if (minAlive <= 0) {
      minAlive = Math.max(1, (observed + 2) / 3);
    }
    return aliveCount >= minAlive;
  }

  /**
   * Mark the cached health judgment stale so the next {@link #isClusterHealthy()}
   * call recomputes. Called when a change can move the verdict; advancing the
   * generation and clearing the low bits allocates nothing.
   */
  private void invalidateHealthCache() {
    healthCache.updateAndGet(word -> ((word >>> HEALTH_GEN_SHIFT) + 1L) << HEALTH_GEN_SHIFT);
  }

  /**
   * Returns the set of Worker IDs that are currently alive: ready to serve and with
   * liveness evidence inside their effective window. The judgment is exactly
   * {@link WorkerHealthRecord#isAlive(long, long, long)}, shared with
   * {@link #isAlive(String)} and the cluster-health computation — one liveness predicate
   * for every consumer.
   *
   * @return a set of alive Worker IDs, never {@code null}
   */
  @Override
  public Set<String> getAliveWorkerIds() {
    // One clock sample for the walk (see computeClusterHealthy): every Worker is judged
    // against the same instant, and the hot liveness path pays no per-record clock read.
    long now = TimeSource.monotonicMillis();
    Set<String> alive = new HashSet<>(Math.max(16, records.size()));
    for (WorkerHealthRecord record : records.values()) {
      if (aliveAt(record, now)) {
        alive.add(record.workerId);
      }
    }
    return alive;
  }

  /**
   * Returns the set of all Worker IDs that have ever been seen by this health view
   * and not yet confirmed dead.
   *
   * @return an unmodifiable set of all known Worker IDs, never {@code null}
   */
  @Override
  public Set<String> getAllWorkerIds() {
    return Set.copyOf(records.keySet());
  }

  /**
   * Whether the given Worker is currently considered alive — the same
   * freshness-based judgment as {@link #getAliveWorkerIds()} and the ring /
   * report path: ready to serve and inside its effective liveness window.
   * Unknown Workers are never alive. O(1) per-record lookup — safe on the
   * read path.
   *
   * @param workerId the Worker node ID
   * @return {@code true} if the Worker is alive
   */
  @Override
  public boolean isAlive(String workerId) {
    return isAlive(workerId, TimeSource.monotonicMillis());
  }

  /**
   * Timestamp-threading form of {@link #isAlive(String)}: one map lookup and a
   * caller-supplied instant, so a multi-node walk samples the clock once.
   */
  @Override
  public boolean isAlive(String workerId, long now) {
    WorkerHealthRecord record = records.get(workerId);
    return record != null && aliveAt(record, now);
  }

  /**
   * The current epoch (restart incarnation) of the given Worker, or
   * {@link #UNKNOWN_EPOCH} when the Worker has never been seen or its record
   * was removed after confirmation of death.
   *
   * @param workerId the Worker node ID
   * @return the Worker's current epoch, or {@link #UNKNOWN_EPOCH}
   */
  @Override
  public long epochOf(String workerId) {
    WorkerHealthRecord record = records.get(workerId);
    return record != null ? record.epoch : UNKNOWN_EPOCH;
  }

  /**
   * Fused "alive AND same incarnation" probe (see {@link HealthView#isCurrentIncarnation}):
   * one map lookup answers both the liveness judgment and the epoch equality.
   *
   * @param workerId the Worker node ID that issued the stamp; must not be {@code null}
   * @param epoch    the epoch captured in the stamp
   * @return {@code true} if the Worker is alive and its epoch is unchanged
   */
  @Override
  public boolean isCurrentIncarnation(String workerId, long epoch) {
    WorkerHealthRecord record = records.get(workerId);
    return record != null && record.epoch == epoch && aliveAt(record, TimeSource.monotonicMillis());
  }

  /** The one liveness judgment, bound to this view's static and adaptive windows. */
  private boolean aliveAt(WorkerHealthRecord record, long now) {
    return record.isAlive(now, heartbeatTimeoutMs, adaptiveFloorMs);
  }

  /**
   * Per-Worker health state tracked within the cluster health view.
   *
   * <p>Each record captures the Worker's current epoch, liveness timing and
   * readiness. Records are created on first heartbeat, mutated in place while the
   * epoch is unchanged, and replaced on epoch change under the health view's record
   * lock.
   *
   * <p>A Worker transitions through these states:
   * <ul>
   *   <li><b>Startup:</b> not ready until the first detection cycle completes</li>
   *   <li><b>Healthy:</b> ready, heartbeats arriving within the window</li>
   *   <li><b>Restarted:</b> New epoch detected; old record replaced entirely</li>
   * </ul>
   */
  public static class WorkerHealthRecord {

    /** RTTVAR floor — the {@code TCP_RTO_MIN} position in the kernel formula. */
    private static final long MDEV_FLOOR_MS = 200L;

    /** Samples before the adaptive estimate is trusted; the static window applies until then. */
    static final int WARMUP_SAMPLES = 8;

    public final String workerId;
    public final long epoch;

    /**
     * Sign bit encodes readiness, the magnitude the last alive-evidence instant:
     * non-negative means ready and holds the monotonic timestamp; negative means
     * not ready and holds {@code ~timestamp}. One volatile write updates both, so
     * a reader can never pair a stale flag with a newer timestamp.
     */
    private volatile long aliveState;

    /** Raw estimator state; written only under the owning health view's record lock. */
    private long rttVarMs = MDEV_FLOOR_MS;
    private long srttMs = -1L;

    /**
     * Dual-lifecycle word read by the hot path. Negative: the remaining warm-up
     * samples ({@code -WARMUP_SAMPLES .. -1}); non-negative: the published
     * {@code srtt + 4 x rttVar} estimate. One volatile read answers both "is the
     * estimate trusted" and "what is it".
     */
    private volatile long estimateOrWarmup = -WARMUP_SAMPLES;

    /**
     * @param workerId the Worker node ID; immutable for the life of the record
     * @param epoch    the Worker's restart counter; immutable for the life of the record
     * @param now      the monotonic instant of the heartbeat that created the record
     * @param ready    the readiness flag carried by that heartbeat
     */
    public WorkerHealthRecord(String workerId, long epoch, long now, boolean ready) {
      this.epoch = epoch;
      this.workerId = workerId;
      this.aliveState = ready ? now : ~now;
    }

    /** The last heartbeat arrival instant, independent of the readiness flag. */
    public long getLastAliveEvidenceTime() {
      long state = aliveState;
      return state >= 0L ? state : ~state;
    }

    /**
     * Feeds one heartbeat arrival: the inter-arrival delta updates the estimator and
     * the same volatile write publishes the new evidence instant and readiness.
     * Runs under the owning health view's record lock.
     */
    void onHeartbeatSample(long now, boolean ready) {
      long intervalMs = now - getLastAliveEvidenceTime();
      if (intervalMs > 0L) {
        if (srttMs < 0L) {
          srttMs = intervalMs;
          rttVarMs = Math.max(intervalMs / 2L, MDEV_FLOOR_MS);
        } else {
          long err = intervalMs - srttMs;
          srttMs += err / 8L;
          long delta = Math.abs(err) - rttVarMs / 4L;
          if (err < 0L && delta > 0L) {
            delta /= 8L;
          }
          rttVarMs = Math.max(MDEV_FLOOR_MS, rttVarMs + delta);
        }
        long state = estimateOrWarmup;
        estimateOrWarmup = state < -1L ? state + 1L : srttMs + 4L * rttVarMs;
      }
      aliveState = ready ? now : ~now;
    }

    /**
     * Returns whether this Worker is currently considered alive — the one judgment
     * shared by the alive set, the cluster-health count, the routing predicate and the
     * Decision-Validity probe.
     *
     * <p>Both conditions must hold: the Worker is ready, and the elapsed monotonic
     * time since the last heartbeat is less than the effective liveness window. The
     * monotonic clock means NTP wall-clock jumps cannot kill workers.
     *
     * @param timeoutMs       the static heartbeat timeout window in milliseconds (the
     *                        warm-up window and the adaptive ceiling); must be positive
     * @param floorMs         lower clamp for the adaptive window; {@code 0} disables
     *                        adaptive timeouts (see {@link #effectiveTimeoutMs})
     * @return {@code true} if this Worker is ready and has provided liveness
     *         evidence within the effective timeout window
     */
    public boolean isAlive(long now, long timeoutMs, long floorMs) {
      long state = aliveState;
      return state >= 0L && now - state < effectiveTimeoutMs(timeoutMs, floorMs);
    }

    /**
     * The effective liveness window: the clamped Jacobson/Karels estimate once the
     * estimator is warmed up, the configured static window until then (and always when
     * {@code floorMs} is {@code 0}). One volatile read per call — this runs for every
     * record of every liveness walk.
     */
    public long effectiveTimeoutMs(long configuredTimeoutMs, long floorMs) {
      if (floorMs == 0L) {
        return configuredTimeoutMs;
      }
      long estimate = estimateOrWarmup;
      return estimate < 0L ? configuredTimeoutMs : Math.max(floorMs, Math.min(configuredTimeoutMs, estimate));
    }
  }
}
