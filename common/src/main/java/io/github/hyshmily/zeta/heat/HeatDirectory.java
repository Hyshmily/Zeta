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
package io.github.hyshmily.zeta.heat;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.RemovalCause;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.sharding.ConsistentHashRing;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.worker.DecisionSkipReason;
import io.github.hyshmily.zeta.sync.worker.WorkerDecision;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * The cluster heat lease table. It owns the Worker decision order — version, epoch, incarnation
 * liveness and the ghost-COOL ownership check — but no data, no TTL and no cache entry.
 *
 * <p>A COOL decision leaves a short tombstone instead of deleting the record: a late HOT below
 * the COOL's version must be rejected, and an absent record cannot compare versions. The tombstone
 * expires after the Worker's in-flight decision window; a HOT lease remains until a COOL or an
 * incarnation change ends it.
 *
 * <p>Removal callbacks driven by Caffeine run on the dedicated notification executor, never on a
 * cache write lock. A lease found dead by {@link #isClusterHot} is ended through that same
 * executor, so a listener never runs on the L1 thread that observed the dead incarnation.
 */
@Internal
public final class HeatDirectory {

  /**
   * A HOT lease never expires on its own: only a COOL or an incarnation change ends it. Caffeine
   * clamps any duration to its own horizon ({@code Long.MAX_VALUE >> 1}), so the value is the
   * documented "no expiration" form rather than a hundred-year guess that the clamp then rewrites.
   */
  private static final long HOT_RETENTION_NANOS = Long.MAX_VALUE;

  private static final BiConsumer<HeatLease, HeatLease.EndReason> NOOP_SINK = (lease, reason) -> {};

  private final Cache<String, HeatLease> clusterLeases;
  private final @Nullable ConsistentHashRing ring;
  private final @Nullable HealthView healthView;
  private final Executor notifyExecutor;
  private final long coolTombstoneNanos;
  private volatile BiConsumer<HeatLease, HeatLease.EndReason> passiveEndSink = NOOP_SINK;

  /**
   * @param maxLeases        maximum live lease/tombstone records
   * @param coolTombstoneMs how long a COOL tombstone stays for version ordering
   * @param notifyExecutor  executor for passive removal callbacks
   * @param healthView      Worker liveness/epoch view, or {@code null}
   * @param ring            consistent-hash ring for the ghost-COOL guard, or {@code null}
   */
  public HeatDirectory(
    int maxLeases,
    long coolTombstoneMs,
    Executor notifyExecutor,
    @Nullable HealthView healthView,
    @Nullable ConsistentHashRing ring
  ) {
    this.ring = ring;
    this.healthView = healthView;
    this.notifyExecutor = Objects.requireNonNull(notifyExecutor, "notifyExecutor");
    this.coolTombstoneNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(1_000L, coolTombstoneMs));
    this.clusterLeases = Caffeine.newBuilder()
      .maximumSize(maxLeases)
      // The Expiry derives a tombstone's remaining lifetime from TimeSource and the lease's own
      // grant stamp: the table must advance on that clock, not on System.nanoTime().
      .ticker(() -> TimeUnit.MILLISECONDS.toNanos(TimeSource.monotonicMillis()))
      .executor(notifyExecutor)
      .expireAfter(
        new Expiry<String, HeatLease>() {
          @Override
          public long expireAfterCreate(@NonNull String key, @NonNull HeatLease value, long currentTime) {
            return value.isHot() ? HOT_RETENTION_NANOS : coolTombstoneNanos;
          }

          @Override
          public long expireAfterUpdate(
            @NonNull String key,
            @NonNull HeatLease value,
            long currentTime,
            long currentDuration
          ) {
            if (value.isHot()) {
              return HOT_RETENTION_NANOS;
            }
            // A compute that returns the resident instance is still an update to Caffeine; keep
            // the tombstone's original deadline instead of re-arming the full window on every
            // duplicate COOL.
            long elapsedNanos = TimeUnit.MILLISECONDS.toNanos(
              Math.max(0L, TimeSource.monotonicMillis() - value.grantedAtMonoMs())
            );
            return Math.max(0L, coolTombstoneNanos - elapsedNanos);
          }

          @Override
          public long expireAfterRead(
            @NonNull String key,
            @NonNull HeatLease value,
            long currentTime,
            long currentDuration
          ) {
            return currentDuration;
          }
        }
      )
      .removalListener((String key, HeatLease lease, RemovalCause cause) -> {
        // A HOT lease has no finite lifetime: the only passive end Caffeine can cause is a
        // size eviction. An explicit remove (isClusterHot's prune) reports through notifyPassiveEnd.
        if (lease != null && lease.isHot() && cause == RemovalCause.SIZE) {
          passiveEndSink.accept(lease, HeatLease.EndReason.EVICTED);
        }
      })
      .build();
  }

  /**
   * Installs the sink for leases ended without a Worker COOL.
   *
   * @param sink the sink, or {@code null} to detach it
   */
  public void setPassiveEndSink(@Nullable BiConsumer<HeatLease, HeatLease.EndReason> sink) {
    this.passiveEndSink = sink != null ? sink : NOOP_SINK;
  }

  /**
   * Applies one HOT decision.
   *
   * <p>The compute is the single arbiter: an incoming decision below the resident stamp (same
   * Worker incarnation) is skipped; a new incarnation or a different Worker accepts on arrival,
   * because both version spaces are node-local. The lease is stored only; no cache value or TTL
   * moves. A decision whose own incarnation is already dead never installs a lease: accepting it
   * would report {@code onHot} for a lease that {@link #isClusterHot} immediately ends.
   *
   * <p>A HOT that replaces a lease whose incarnation is gone reports that lease in the result.
   * Caffeine sees the replacement as {@code REPLACED} and never fires its removal listener, so
   * the caller must emit the old lease's end before the new {@code onHot}; returning it keeps
   * that pairing on the per-key dispatcher thread instead of handing it to another executor.
   * The key was not hot while its owner was gone — {@link #isClusterHot} reports it cold — so the
   * takeover is a new grant, and a grant over a live lease (including another live Worker's,
   * after a shard moved) is not.
   *
   * @param decision the Worker decision
   * @return whether the lease was granted, whether the key was not hot before, and the lease the
   *         grant replaced
   */
  public LeaseOutcome grant(WorkerDecision decision) {
    DecisionStamp incoming = decision.stamp();
    if (!isIncarnationAlive(incoming)) {
      return LeaseOutcome.SKIPPED_DEAD;
    }
    LeaseOutcome[] result = new LeaseOutcome[1];
    clusterLeases
      .asMap()
      .compute(decision.cacheKey(), (key, existing) -> {
        boolean aliveHot = existing != null && existing.isHot() && isIncarnationAlive(existing.stamp());
        DecisionStamp resident = existing != null && (existing.isCool() || aliveHot) ? existing.stamp() : null;
        if (resident != null && VersionGuard.shouldSkipForWorker(resident, incoming)) {
          result[0] = LeaseOutcome.SKIPPED_STALE;
          return existing;
        }

        // A same-node newer epoch means the lease's owner restarted even when the health view has
        // not observed the restart yet. A different Worker keeps a live lease continuously hot.
        boolean staysHot = aliveHot && !isSameNodeNewerEpoch(existing, incoming);
        HeatLease dead = existing != null && existing.isHot() && !staysHot ? existing : null;
        // A different Worker or a restarted incarnation is a new grant even when its version is
        // below the resident one: those counters are node-local and incomparable.
        result[0] = LeaseOutcome.granted(!staysHot, dead);
        return new HeatLease(key, incoming, TimeSource.monotonicMillis());
      });
    return result[0];
  }

  /**
   * Applies one COOL decision, writing a short version tombstone.
   *
   * <p>A tombstone is required rather than a delete: a late HOT below the COOL's version must be
   * rejected, and an absent record cannot compare versions. The tombstone expires after the
   * Worker's in-flight decision window. The ghost-COOL guard rejects a cross-Worker kill while a
   * different live incarnation still owns the ring slot — including a COOL tombstone, so a delayed
   * cross-node COOL cannot overwrite another owner's tombstone and let an older HOT resurrect. The
   * ring rebuild runs outside the compute, and the liveness probe inside it is a lock-free
   * health-view read.
   *
   * <p>A COOL from an incarnation that is already gone never revokes a lease owned by a live or
   * newer incarnation. The one exception is a same-node, same-epoch HOT lease: the COOL is its last
   * chance to pair the business {@code onHot} with an {@code onCool}, so it ends that lease with
   * {@link HeatLease.EndReason#INCARNATION_DEAD}. A COOL that revokes a dead grant is the same
   * pairing; the replacement itself is invisible to Caffeine's removal listener.
   *
   * @param decision the Worker decision
   * @return whether the lease was revoked, the ended lease and why, and the skip reason when not
   */
  public LeaseOutcome revoke(WorkerDecision decision) {
    String key = decision.cacheKey();
    DecisionStamp incoming = decision.stamp();
    reconcileRingForCrossNodeCool(key, incoming);

    LeaseOutcome[] result = new LeaseOutcome[1];
    clusterLeases
      .asMap()
      .compute(key, (k, existing) -> {
        if (existing != null && VersionGuard.shouldSkipForWorker(existing.stamp(), incoming)) {
          result[0] = LeaseOutcome.SKIPPED_STALE;
          return existing;
        }

        boolean incomingAlive = isIncarnationAlive(incoming);
        boolean aliveResident = existing != null && isIncarnationAlive(existing.stamp());
        if (!incomingAlive) {
          // A delayed COOL from a dead incarnation may still pair-close the HOT it issued in the
          // same incarnation; it must never revoke a lease owned by a live or newer incarnation.
          if (
            existing != null &&
            existing.isHot() &&
            !aliveResident &&
            existing.stamp().sameNodeAs(incoming) &&
            existing.stamp().decisionEpoch() == incoming.decisionEpoch()
          ) {
            result[0] = LeaseOutcome.revoked(existing, HeatLease.EndReason.INCARNATION_DEAD);
            return null;
          }

          result[0] = LeaseOutcome.SKIPPED_DEAD;
          return existing;
        }

        if (aliveResident && isStaleCrossNodeCool(existing, key, incoming)) {
          result[0] = LeaseOutcome.SKIPPED_GHOST;
          return existing;
        }

        if (existing != null && existing.isHot()) {
          boolean sameIncarnation = aliveResident && !isSameNodeNewerEpoch(existing, incoming);
          result[0] = LeaseOutcome.revoked(
            existing,
            sameIncarnation ? HeatLease.EndReason.COOL : HeatLease.EndReason.INCARNATION_DEAD
          );
        } else {
          result[0] = LeaseOutcome.APPLIED_UNCHANGED;
        }

        return new HeatLease(k, incoming, TimeSource.monotonicMillis());
      });
    return result[0];
  }

  /**
   * Answers the cluster-heat query and lazily prunes a lease whose incarnation is gone.
   *
   * <p>The prune is identity-guarded: a newer lease installed between the read and the removal is
   * never removed. A successful prune ends the lease with
   * {@link HeatLease.EndReason#INCARNATION_DEAD} on the notification executor, so business hot
   * state does not leak when a Worker dies and a listener never runs on the observing thread.
   *
   * @param key the physical key
   * @return whether a live HOT lease currently governs the key
   */
  public boolean isClusterHot(String key) {
    HeatLease lease = clusterLeases.getIfPresent(key);
    if (lease == null || !lease.isHot()) {
      return false;
    }
    if (isIncarnationAlive(lease.stamp())) {
      return true;
    }
    if (clusterLeases.asMap().remove(key, lease)) {
      notifyPassiveEnd(lease);
    }
    return false;
  }

  /**
   * Reads the resident lease stamp the dispatcher's batch reducer may compare against, using the
   * same liveness rule as {@link #grant}: a lease whose incarnation is gone contributes no stamp,
   * because its version space ended with that incarnation.
   *
   * @param key the physical key
   * @return the live lease stamp, or {@code null} when the key is cold
   */
  public @Nullable DecisionStamp liveStampOf(String key) {
    HeatLease lease = clusterLeases.asMap().get(key);
    return lease != null && (lease.isCool() || isIncarnationAlive(lease.stamp())) ? lease.stamp() : null;
  }

  /**
   * Ends one lease without a Worker COOL. Runs on the notification executor rather than the thread
   * that observed the end, so a listener never executes under an L1 bin lock.
   */
  private void notifyPassiveEnd(HeatLease lease) {
    try {
      notifyExecutor.execute(() -> passiveEndSink.accept(lease, HeatLease.EndReason.INCARNATION_DEAD));
    } catch (RejectedExecutionException ignored) {
      // A rejected notification during shutdown has no receiver left.
    }
  }

  private boolean isIncarnationAlive(DecisionStamp stamp) {
    HealthView view = healthView;
    if (view == null) {
      return true;
    }

    String nodeId = stamp.decisionNodeId();
    return nodeId == null || view.isCurrentIncarnation(nodeId, stamp.decisionEpoch());
  }

  /**
   * Whether {@code incoming} comes from a newer incarnation of the lease's own node. A higher
   * epoch means that Worker restarted, so the lease is gone even when the health view has not
   * observed the restart yet.
   */
  @SuppressWarnings("all")
  private static boolean isSameNodeNewerEpoch(HeatLease lease, DecisionStamp incoming) {
    return lease.stamp().sameNodeAs(incoming) && incoming.decisionEpoch() > lease.stamp().decisionEpoch();
  }

  /**
   * Rebuild the ring when a cross-node COOL names a Worker the local view does not know. A new
   * Worker may take a shard before this node observes the membership change; checking only the
   * resident owner would reject that legitimate COOL as a ghost.
   */
  private void reconcileRingForCrossNodeCool(String key, DecisionStamp incoming) {
    if (ring == null || healthView == null) {
      return;
    }

    HeatLease snapshot = clusterLeases.asMap().get(key);
    if (snapshot == null) {
      return;
    }

    String owner = snapshot.stamp().decisionNodeId();
    String sender = incoming.decisionNodeId();
    if (owner != null && sender != null && !owner.equals(sender)) {
      Set<String> nodes = ring.getNodes();
      if (!nodes.contains(owner) || !nodes.contains(sender)) {
        ring.rebuildIfPossible(healthView);
      }
    }
  }

  /** A cross-node COOL is a ghost while the resident owner is alive and still owns the ring slot. */
  @SuppressWarnings("all")
  private boolean isStaleCrossNodeCool(HeatLease lease, String key, DecisionStamp incoming) {
    if (ring == null || healthView == null) {
      return false;
    }

    DecisionStamp owner = lease.stamp();
    if (!owner.hasNodeId() || !incoming.hasNodeId() || owner.sameNodeAs(incoming)) {
      return false;
    }

    return owner.decisionNodeId().equals(ring.locateNode(key, healthView));
  }

  /**
   * One HOT or COOL apply outcome.
   *
   * @param newlyHot  whether this grant changed the key from cold to hot
   * @param endedLease the lease this apply ended, or {@code null}
   * @param endReason why the ended lease stopped being hot, or {@code null}
   * @param skipReason the skip reason, or {@code null} when applied
   */
  public record LeaseOutcome(
    boolean newlyHot,
    @Nullable HeatLease endedLease,
    HeatLease.@Nullable EndReason endReason,
    @Nullable DecisionSkipReason skipReason
  ) {
    static final LeaseOutcome SKIPPED_STALE = new LeaseOutcome(false, null, null, DecisionSkipReason.VERSION_STALE);
    static final LeaseOutcome SKIPPED_DEAD = new LeaseOutcome(false, null, null, DecisionSkipReason.INCARNATION_DEAD);
    static final LeaseOutcome SKIPPED_GHOST = new LeaseOutcome(false, null, null, DecisionSkipReason.GHOST_COOL);
    static final LeaseOutcome APPLIED_UNCHANGED = new LeaseOutcome(false, null, null, null);
    static final LeaseOutcome NEWLY_HOT = new LeaseOutcome(true, null, null, null);

    public boolean applied() {
      return skipReason == null;
    }

    static LeaseOutcome granted(boolean newlyHot, @Nullable HeatLease deadLease) {
      return deadLease != null
        ? new LeaseOutcome(newlyHot, deadLease, HeatLease.EndReason.INCARNATION_DEAD, null)
        : (newlyHot ? NEWLY_HOT : APPLIED_UNCHANGED);
    }

    static LeaseOutcome revoked(HeatLease endedLease, HeatLease.EndReason endReason) {
      return new LeaseOutcome(false, endedLease, endReason, null);
    }
  }
}
