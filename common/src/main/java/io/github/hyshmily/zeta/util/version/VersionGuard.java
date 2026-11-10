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
package io.github.hyshmily.zeta.util.version;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.sync.local.CacheSyncListener;
import io.github.hyshmily.zeta.sync.worker.WorkerListener;
import io.github.hyshmily.zeta.util.Internal;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * Shared version comparison logic for the two message receivers: the Worker
 * decision listener and the instance-to-instance cache sync listener.
 *
 * <p>Two independent version spaces are compared here:
 * <ul>
 *   <li><b>Decision space</b> — {@link #shouldSkipForWorker} for COOL and
 *       {@link #shouldSkipForHot} for HOT. {@code nodeId} and {@code epoch} are
 *       node-local: a different node id accepts on arrival, while the same node
 *       compares epoch and then {@code decisionVersion}. The HOT variant also
 *       re-applies an equal same-node/same-epoch decision to a resident that a
 *       local lifecycle transition demoted to {@link KeyState#NORMAL}.</li>
 *   <li><b>Data space</b> — {@link #shouldSkipForSync} (an equal stamp skips) and
 *       {@link #shouldSkipForRefresh} (an equal stamp applies), one numeric
 *       comparison over {@code dataVersion} (design decisions: version stamps).</li>
 * </ul>
 *
 * <p>All methods are thread-safe and keep no instance state; the utility class must never
 * be instantiated.
 *
 * <p><b>Incoming stamps are validated at the wire boundary.</b> {@code WorkerDecision.from}
 * and {@code SyncMessage.from} drop a non-positive stamp, so every method here assumes a
 * positive incoming value and does not re-check it (one rule, one owner).
 *
 * @see WorkerListener
 * @see CacheSyncListener
 */
@Internal
@Slf4j
public final class VersionGuard {

  /**
   * Cumulative decisions skipped because a same-{@code nodeId} Worker sent a lower
   * epoch (stale incarnation). Persistent growth after a Worker restart is the
   * observable signature of the lost-epoch-file scenario: the new incarnation
   * seeds its epoch from the wall clock, a clock regression leaves it below the
   * old incarnation's epoch, and every decision is skipped until the resident
   * entries expire — point {@code zeta.worker.epoch-file} at a persistent volume.
   */
  private static final LongAdder EPOCH_REGRESS_SKIPS = new LongAdder();

  /**
   * Utility class — prevent instantiation.
   */
  private VersionGuard() {}

  /**
   * Cumulative number of decisions skipped for a same-nodeId epoch rollback.
   * Monotonic; safe to read from any thread.
   *
   * @return the total epoch-rollback skip count since JVM start
   */
  public static long epochRegressSkips() {
    return EPOCH_REGRESS_SKIPS.sum();
  }

  /**
   * WorkerListener guard for COOL decisions, with epoch and node-id awareness.
   * <p>Decision logic:
   * <ol>
   * <li>No existing entry → accept (return false)</li>
   *   <li><b>Different nodeId → accept unconditionally.</b> Both the epoch
   *       (a per-Worker restart counter seeded from that Worker's own wall
   *       clock at boot) and the decisionVersion (a per-Worker counter starting
   *       at 0) are <em>node-local</em> quantities; neither is comparable
   *       across Workers, so arrival order — last-writer-wins — is the only
   *       sound tie-break (design decisions: version comparison).</li>
   *   <li>Same nodeId, incoming epoch &gt; existing epoch → accept
   *       (Worker restart detected — design decisions: heartbeat and health)</li>
   *   <li>Same nodeId, incoming epoch &lt; existing epoch → skip (stale
   *       incarnation message)</li>
   *   <li>Same nodeId, same epoch → skip if existing dv &gt;= incoming dv
   *       (same counter, directly comparable)</li>
   * </ol>
   *
   * @param existing the existing cache entry; may be null
   * @param incoming the incoming Worker decision stamp; must not be null
   * @return true if the incoming message should be skipped, false if it should be applied
   */
  public static boolean shouldSkipForWorker(CacheEntry existing, DecisionStamp incoming) {
    return shouldSkipDecision(existing == null ? null : existing.decision(), incoming, false);
  }

  /**
   * COOL guard against a resident decision stamp, for batch folding where the resident
   * entry is read once and then advanced locally through the run.
   *
   * @param resident the resident decision stamp, or {@code null} when absent
   * @param incoming the incoming Worker decision stamp; must not be null
   * @return true if the incoming COOL should be skipped
   */
  public static boolean shouldSkipForWorker(@Nullable DecisionStamp resident, DecisionStamp incoming) {
    return shouldSkipDecision(resident, incoming, false);
  }

  /**
   * WorkerListener guard for <b>HOT</b> decisions: identical to
   * {@link #shouldSkipForWorker} except that an equal same-node/same-epoch
   * decision re-applies when the resident is {@link KeyState#NORMAL}.
   *
   * <p>An entry demoted to NORMAL by a local lifecycle transition (background
   * refresh or soft-expiry rebuild) keeps its decision stamp, so a redelivered
   * duplicate would compare equal and be skipped forever — the key would stay
   * NORMAL under a decision that still says HOT. An equal decision on a HOT or
   * COOL resident is still a duplicate and skips.
   *
   * @param existing the existing cache entry; may be null
   * @param incoming the incoming Worker decision stamp; must not be null
   * @return true if the incoming HOT decision should be skipped
   */
  public static boolean shouldSkipForHot(CacheEntry existing, DecisionStamp incoming) {
    return shouldSkipDecision(
      existing == null ? null : existing.decision(),
      incoming,
      existing != null && existing.getKeyState() == KeyState.NORMAL
    );
  }

  private static boolean shouldSkipDecision(
    @Nullable DecisionStamp resident,
    DecisionStamp incoming,
    boolean equalApplies
  ) {
    if (resident == null || !Objects.equals(incoming.decisionNodeId(), resident.decisionNodeId())) {
      // No resident decision, or a different Worker node: epoch and version are
      // node-local, so arrival order is the only sound comparison.
      return false;
    }

    long residentEpoch = resident.decisionEpoch();
    long incomingEpoch = incoming.decisionEpoch();
    // Same Worker: epoch is a monotone incarnation counter, so it is comparable.
    if (incomingEpoch > residentEpoch) {
      return false;
    }
    if (incomingEpoch < residentEpoch) {
      return epochRollback(incoming, residentEpoch, incomingEpoch);
    }

    long residentVersion = resident.decisionVersion();
    return equalApplies ? residentVersion > incoming.decisionVersion() : residentVersion >= incoming.decisionVersion();
  }

  /** Records one stale-incarnation skip: a same-node decision below the resident epoch. */
  private static boolean epochRollback(DecisionStamp incoming, long residentEpoch, long incomingEpoch) {
    // Stale message from an old Worker incarnation. The counter is the persistent
    // signal: a container rebuild that loses the epoch file mutes this Worker's
    // decisions until the resident entries expire, and this skip is its only signature.
    EPOCH_REGRESS_SKIPS.increment();
    log.debug(
      "Epoch rollback: existing={}, incoming={}, nodeId={} — skipping stale message; persistent skips after a " +
        "restart mean the epoch file was lost — point zeta.worker.epoch-file at a persistent volume",
      residentEpoch,
      incomingEpoch,
      incoming.decisionNodeId()
    );
    return true;
  }

  /**
   * CacheSyncListener guard for use inside an atomic {@code compute} block: the caller
   * already holds the existing entry reference.
   *
   * <p>Single stamp space (design decisions: version stamps): a resident stamp of
   * {@code 0} (unknown) accepts any incoming stamp; otherwise the comparison is numeric
   * and an equal version skips. The incoming stamp is positive — the sync wire boundary
   * dropped every non-positive one.
   *
   * @param existing             the existing cache entry; may be {@code null} (returns {@code false})
   * @param incomingDataVersion  the positive data stamp from the incoming sync message
   * @return {@code true} if the incoming update should be skipped;
   *         {@code false} if it should be applied
   */
  public static boolean shouldSkipForSync(CacheEntry existing, long incomingDataVersion) {
    return skip(existing, incomingDataVersion, false);
  }

  /**
   * CacheSyncListener guard for the <b>REFRESH receiver</b>: identical to
   * {@link #shouldSkipForSync} except that an <b>equal</b> version applies instead of
   * skipping — only a strictly newer existing version is skipped.
   *
   * <p>Rationale (design decisions: version comparison): replaying an equal-version refresh re-fetches the
   * authoritative value (or falls back to a local invalidation when no value channel
   * exists) and heals an over-stamped entry, while skipping would pin the stale value
   * until the next write for the key or its whole TTL. The extra work for a genuine
   * duplicate (same write, same version) is one reload plus an idempotent overwrite.
   *
   * @param existing            the existing cache entry; may be {@code null} (returns {@code false})
   * @param incomingDataVersion the data stamp from the incoming REFRESH message
   * @return {@code true} if the incoming refresh should be skipped (existing entry is
   *         strictly newer); {@code false} if it should be applied
   */
  public static boolean shouldSkipForRefresh(CacheEntry existing, long incomingDataVersion) {
    return skip(existing, incomingDataVersion, true);
  }

  /**
   * Single-space comparison behind {@link #shouldSkipForSync} and
   * {@link #shouldSkipForRefresh}: the two receivers differ in exactly one cell —
   * how an <b>equal</b> version is treated.
   *
   * @param entry        the existing cache entry; may be {@code null} (returns {@code false})
   * @param incoming     the data stamp from the incoming sync message
   * @param equalApplies {@code true} → an <b>equal</b> stamp applies (strictly-newer skip,
   *                     the REFRESH receiver, design decisions: version comparison); {@code false} → an equal stamp
   *                     skips
   * @return {@code true} if the incoming message should be skipped; {@code false} if it should be applied
   */
  private static boolean skip(CacheEntry entry, long incoming, boolean equalApplies) {
    if (entry == null) {
      return false;
    }

    long version = entry.getDataVersion();
    if (version <= 0L) {
      // Unknown makes the weakest claim: accept any real stamp.
      return false;
    }
    return equalApplies ? version > incoming : version >= incoming;
  }

  /**
   * REFRESH-receiver guard with a cache-level fast path: fetches the existing entry
   * from the L1 cache and delegates to the entry-level overload.
   *
   * <p>Used <em>outside</em> atomic {@code compute} blocks as a cheap first-pass
   * check (DCL pattern). A second guard inside the {@code compute} block is still
   * needed for correctness.
   *
   * <p>The lookup goes through {@link Cache#asMap()} on purpose: a sync-plane
   * pre-check is not a caller read, so it must not record a hit/miss. It does
   * still offer the node to Caffeine's read buffer and frequency sketch; a probe
   * that must not influence eviction either uses
   * {@code policy().getIfPresentQuietly}. The entry-level overload is called
   * directly, with no capturing predicate.
   *
   * @param cache               the local Caffeine L1 cache; must not be null
   * @param cacheKey            the cache key to look up; must not be null
   * @param incomingDataVersion the data stamp from the incoming REFRESH message
   * @return {@code true} if the incoming refresh should be skipped;
   *         {@code false} if it should be applied
   * @see #shouldSkipForRefresh(CacheEntry, long)
   */
  public static boolean shouldSkipForRefresh(
    Cache<String, CacheEntry> cache,
    String cacheKey,
    long incomingDataVersion
  ) {
    return shouldSkipForRefresh(cache.asMap().get(cacheKey), incomingDataVersion);
  }
}
