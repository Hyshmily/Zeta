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
package io.github.hyshmily.zeta.cache.cachesupport.lifecycle;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryMutations;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryMutations.Applied;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.clock.StampClock;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * Default {@link EntryLifecycle}: the stateful side of the {@link CacheEntry}
 * lifecycle — TOCTOU invalidation guards, Decision-Validity demotion
 * (design decisions: hot state) and the single {@link EntryDraft} factory.
 *
 * <p>Background soft-expire refresh scheduling lives behind the separate
 * {@link io.github.hyshmily.zeta.scheduler.BackgroundRefresher} seam
 * ({@code DefaultBackgroundRefresher}); this class deliberately owns no
 * executor and no scheduling state.
 */
@Slf4j
@Internal
public class EntryLifecycleImpl implements EntryLifecycle {

  /** Upper bound on live prefix watermarks before eviction/folding. */
  private static final int MAX_PREFIX_WATERMARKS = 64;

  /** The underlying L1 Caffeine cache instance. */
  private final Cache<String, CacheEntry> caffeineCache;

  /** Cache-bounded tombstone config; the prefix watermarks share its TTL bound. */
  private final ZetaProperties.CacheConfig cacheConfig;

  /**
   * Cluster health view used for the Decision-Validity check
   * (design decisions: hot state). {@code null} disables demotion (test doubles, consumers
   * without a health view).
   */
  private final HealthView healthView;

  /** Pure TTL/expiry policy — all stateless lifecycle arithmetic lives here. */
  private final TtlPolicy ttlPolicy;

  /** Highest clear stamp applied; {@code 0} = none. Written only under {@link #watermarkLock}. */
  private volatile long clearWatermark;

  /**
   * Serializes the two low-frequency watermark writers. The read path stays
   * lock-free on the volatile snapshot; only clear/clearPrefix pay this lock.
   */
  private final Object watermarkLock = new Object();

  /**
   * Per-key invalidation tombstones: the highest version an INVALIDATE removed this key at.
   * Bounded by {@code zeta.local.cache.tombstone-maximum-size} /
   * {@code zeta.local.cache.tombstone-ttl-seconds} — the tombstone only has to outlive
   * in-flight reads, not the entry's hard TTL.
   */
  private final Cache<String, Long> invalidatedAt;

  /**
   * Scoped clear watermarks (namespace prefix → highest clear stamp)
   * plus the highest live stamp, in one immutable snapshot.
   *
   * <p>Copy-on-write: the snapshot is immutable and replaced under
   * {@link #watermarkLock} by the two low-frequency clear writers. The maximum
   * lets the write path ({@link #staleAgainstInvalidation}) reject the common
   * case — a stamp above every recorded clear — with one volatile read and one
   * long compare, without walking the map or reading the clock; only a version
   * low enough to match a live prefix pays the short startsWith walk
   * (design decisions: cache table).
   *
   * <p>Bounded at {@link #MAX_PREFIX_WATERMARKS} and self-pruning: a stamp the
   * table-wide watermark covers is dropped (it can never match again), and
   * past the cap the whole live set folds into the table-wide watermark at
   * the highest stamp involved and the map resets. Folding refuses strictly
   * more writes, which is the safe direction — it can only reject stale
   * fills, never admit them. The max is recomputed from the live entries on
   * every rewrite: it gates only whether the walk runs, never whether a write
   * is refused, and a pruned high stamp must not make later writes pay for a
   * walk that cannot match.
   */
  private volatile PrefixWatermarks prefixClearWatermarks = PrefixWatermarks.EMPTY;

  /**
   * Callback fired once per successful {@link #demoteIfDecisionInvalid}
   * demotion; {@code null} disables the notification. Volatile: registered
   * once at engine construction, read on every demotion.
   */
  private volatile @Nullable Consumer<String> demotedSink;

  /**
   * Immutable prefix-watermark snapshot. {@code maxStamp} is the highest
   * <em>live</em> stamp, or {@code 0} when {@code stamps} is empty: a write
   * above it cannot match any live prefix, so the walk is skipped. Pruning
   * recomputes it, so a removed high stamp does not leave a stale fast-path
   * bound behind.
   */
  private record PrefixWatermarks(Map<String, Long> stamps, long maxStamp) {
    static final PrefixWatermarks EMPTY = new PrefixWatermarks(Map.of(), 0L);
  }

  /**
   * Creates a lifecycle manager with the given Caffeine cache, TTL config,
   * and cluster health view.
   *
   * <p>The {@link HealthView} powers the Decision-Validity demotion (design decisions: hot state):
   * Worker-sourced HOT entries whose issuing Worker incarnation is dead or
   * restarted are reverted to the NORMAL lifecycle on the read path. A
   * {@code null} health view disables demotion.
   *
   * @param caffeineCache the underlying L1 Caffeine cache
   * @param ttlConfig     TTL configuration (normal and hot-key variants)
   * @param healthView    cluster health view for the Decision-Validity check, or {@code null}
   */
  public EntryLifecycleImpl(Cache<String, CacheEntry> caffeineCache, ZetaProperties ttlConfig, HealthView healthView) {
    this.healthView = healthView;
    this.caffeineCache = caffeineCache;
    this.cacheConfig = ttlConfig.getCache();
    this.ttlPolicy = new TtlPolicy(ttlConfig);
    this.invalidatedAt = Caffeine.newBuilder()
      .maximumSize(cacheConfig.getTombstoneMaximumSize())
      .expireAfterWrite(cacheConfig.getTombstoneTtlSeconds(), TimeUnit.SECONDS)
      // Same clock family as every other deadline in this class: the monotonic
      // millisecond axis, so tests can advance it through TimeSource's offset
      // hook and an NTP step cannot drop or resurrect a tombstone.
      .ticker(() -> TimeUnit.MILLISECONDS.toNanos(TimeSource.monotonicMillis()))
      .build();
  }

  /**
   * Judges hard expiry against the caller's single clock sample and removes the
   * supplied snapshot. A concurrent replacement is judged against the same
   * sample inside the compute: the return value is either {@code null} or an
   * entry that is not logically expired at {@code now}, so callers never have
   * to re-check the replacement.
   *
   * @param cacheKey the cache key
   * @param raw      the entry the caller read, or {@code null}
   * @param now      the per-call epoch-millis sample
   * @return the surviving resident, or {@code null} when the key is now absent
   */
  @Override
  @SuppressWarnings("all")
  public @Nullable CacheEntry evictIfExpired(String cacheKey, @Nullable CacheEntry raw, long now) {
    if (raw == null || !ttlPolicy.isLogicallyExpired(raw, now)) {
      return raw;
    }

    CacheEntry live = caffeineCache
      .asMap()
      .compute(cacheKey, (key, existing) ->
        existing == null || existing == raw || ttlPolicy.isLogicallyExpired(existing, now) ? null : existing
      );
    if (live == null) {
      log.debug("Cache entry logically expired during processing, reloading: {}", cacheKey);
    }
    return live;
  }

  /**
   * Decision-Validity demotion (design decisions: hot state): revert a Worker-sourced HOT entry
   * to the NORMAL lifecycle when its issuing Worker incarnation is no longer
   * authoritative.
   *
   * <p>An entry's Worker decision is valid only while the issuing incarnation
   * is alive and its epoch is unchanged: the Worker is the sole authority for
   * cooling the key down, so a dead or restarted Worker leaves the entry with
   * HOT TTLs but no one to revoke them. On the first read after invalidity is
   * detected, the entry is rewritten in place — value preserved and still
   * served, TTLs reverted to the normal baseline, decision stamp cleared —
   * so it converges at the normal hard TTL and the local TopK re-decides on
   * the next reload.
   *
   * <p>Runs on the read path; the predicate is a cheap O(1) health-view lookup
   * and only fires for entries carrying a decision stamp. The outer check is
   * only a fast path. Inside the atomic {@code compute} the full judgment runs
   * again for <b>every</b> resident, including the unchanged reference: the
   * health record is mutable, so a Worker heartbeat may have arrived between
   * the outer check and the bin lock, and a recovered HOT lease must not be
   * clobbered by the stale outer verdict. With no health view configured,
   * demotion is disabled and reports the entry unchanged.
   *
   * @param key   the cache key
   * @param entry the L1 entry (may be {@code null})
   * @param now   the per-call epoch-millis sample
   * @return the images the bin lock observed; {@link Applied#changed()} is
   *         {@code false} when no demotion applied
   */
  @Override
  public Applied demoteIfDecisionInvalid(String key, @Nullable CacheEntry entry, long now) {
    if (entry == null || (!ttlPolicy.isLogicallyExpired(entry, now) && !needsDemotion(entry))) {
      return EntryMutations.unchanged(entry);
    }

    // Build the rewrite inside the callback: a demotion storm (a dead Worker's HOT set read
    // concurrently) would otherwise allocate one candidate per reader and discard every loser.
    // The callback is single-threaded per key, so only the winner copies the entry; the losers
    // exit on the re-judged state without touching the draft. An expired resident is removed
    // rather than rewritten: demoteToNormal keeps the earlier deadline, so a rewritten entry
    // would still be dead and the caller would have to evict it a second time.
    Applied applied = EntryMutations.apply(caffeineCache, key, resident -> {
      if (resident == null || ttlPolicy.isLogicallyExpired(resident, now)) {
        return null;
      }
      // Re-judge under the bin lock even when the reference is unchanged: the health record is
      // mutable, and a Worker heartbeat may have arrived between the outer check and this compute.
      // A recovered Worker's HOT lease must not be clobbered by the stale outer verdict.
      return needsDemotion(resident) ? demoteToNormal(resident) : resident;
    });
    // A null post-image is an expiry removal, not a demotion: no sink, no demotion log.
    if (applied.changed() && applied.current() != null) {
      if (log.isDebugEnabled()) {
        // This mutation never creates an entry, so the pre-image is present.
        CacheEntry demotedFrom = applied.previous();
        if (demotedFrom != null) {
          log.debug(
            "Decision-Validity demotion: key={} nodeId={} epoch={} reverted to NORMAL lifecycle",
            key,
            demotedFrom.getDecisionNodeId(),
            demotedFrom.getDecisionEpoch()
          );
        }
      }
      Consumer<String> sink = demotedSink;
      if (sink != null) {
        sink.accept(key);
      }
    }
    return applied;
  }

  /**
   * The Decision-Validity predicate (design decisions: hot state): an entry needs demotion when it
   * carries a Worker decision stamp on a HOT entry whose issuing incarnation is gone (no health
   * record, dead, or restarted to a new epoch). A disabled health view and local-origin entries are
   * never incarnation-invalid — the Worker is the sole authority for cooling the key down, so only
   * it can orphan a HOT stamp.
   *
   * <p>
   * The probe is allocation-free: the stamp is read through the entry's plain
   * getters, and {@code isCurrentIncarnation(nodeId, epoch)} answers liveness and
   * epoch equality in one map read.
   *
   * @param entry the L1 entry to inspect (never {@code null})
   * @return {@code true} when the entry carries a stale Worker decision
   */
  @Override
  public boolean needsDemotion(CacheEntry entry) {
    if (entry.getKeyState() != KeyState.HOT || !entry.isWorkerManaged()) {
      return false;
    }
    HealthView view = healthView;
    String nodeId = entry.getDecisionNodeId();
    if (view == null || nodeId == null) {
      return false;
    }

    // Authoritative only while the Worker is alive (same freshness judgment as
    // the ring/report path) and its epoch is unchanged (no restart).
    return !view.isCurrentIncarnation(nodeId, entry.getDecisionEpoch());
  }

  @Override
  public void setDemotedSink(@Nullable Consumer<String> demotedSink) {
    this.demotedSink = demotedSink;
  }

  /**
   * The earlier of a candidate deadline and the entry's own, where
   * {@code noDeadline} is that side's "no bound" sentinel ({@code Long.MAX_VALUE}
   * for hard, {@code 0} for a disabled soft): an absent bound defers to the other.
   */
  private static long earlierDeadline(long candidate, long current, long noDeadline) {
    return candidate == noDeadline ? current : current == noDeadline ? candidate : Math.min(candidate, current);
  }

  /**
   * Entry factory for creating new entries — the single creation path. See
   * {@link EntryLifecycle#newEntry()} for the draft contract (value discipline,
   * TTL resolution semantics). Blank drafts are born now: the value-birth
   * stamp is preset to the current monotonic second (fresh value), while
   * {@link #editEntry(CacheEntry)} preserves the source entry's stamp.
   */
  @Override
  public EntryDraft newEntry() {
    return EntryDraft.blank(ttlPolicy).birthStampSec(TimeSource.monotonicSecondsSinceBoot());
  }

  /**
   * Entry factory for modifying existing entries — the single copy-on-write
   * path. See {@link EntryLifecycle#editEntry(CacheEntry)}.
   */
  @Override
  public EntryDraft editEntry(CacheEntry source) {
    return EntryDraft.of(source, ttlPolicy);
  }

  @Override
  public CacheEntry writeValue(
    @Nullable CacheEntry entry,
    Object value,
    long version,
    long hardTtlMs,
    long softTtlMs,
    boolean promoteHot
  ) {
    CacheEntry live = entry != null && !ttlPolicy.isLogicallyExpired(entry) ? entry : null;
    if (live != null && live.isElevated()) {
      if (live.getKeyState() == KeyState.COOL && promoteHot) {
        // Degraded-mode local promotion on a value write: the write path must reach the same
        // COOL -> HOT state the read path's reconciliation would, while retaining the COOL lease
        // for the recovery reassertion.
        long hotHard = ttlPolicy.resolveHotHardTtl(hardTtlMs);
        long hotSoft = ttlPolicy.resolveHotSoftTtl(softTtlMs);
        return editEntry(live)
          .value(value)
          .version(version)
          .birthStampSec(TimeSource.monotonicSecondsSinceBoot())
          .ttlDurations(hotHard, hotSoft)
          .expiryAt(ttlPolicy.hardExpireAt(hotHard), ttlPolicy.softExpireAt(hotSoft))
          .keyState(KeyState.HOT)
          .build();
      }
      // The elevated regime owns its soft window; a stored 0 is the disabled sentinel and must
      // not be re-enabled by the caller's normal soft TTL.
      long soft = live.getSoftTtlMs();
      long softExpireAt = Math.min(ttlPolicy.softExpireAt(soft), live.getHardExpireAtMs());
      return editEntry(live)
        .value(value)
        .version(version)
        .birthStampSec(TimeSource.monotonicSecondsSinceBoot())
        .softTtl(soft)
        .softExpiryAt(softExpireAt)
        .build();
    }

    KeyState target = promoteHot ? KeyState.HOT : KeyState.NORMAL;
    long activeHard = promoteHot ? ttlPolicy.resolveHotHardTtl(hardTtlMs) : hardTtlMs;
    long activeSoft = promoteHot ? ttlPolicy.resolveHotSoftTtl(softTtlMs) : softTtlMs;
    return newEntry()
      .value(value)
      .version(version)
      .decision(live != null ? live.decision() : null)
      .ttl(activeHard, activeSoft, ttlPolicy.normalHardBaseline(hardTtlMs), softTtlMs)
      .keyState(target)
      .build();
  }

  @Override
  public CacheEntry cool(CacheEntry entry, DecisionStamp stamp) {
    long now = TimeSource.currentTimeMillis();
    long hardTtl = ttlPolicy.coolHardTtlMs(entry);
    long softTtl = ttlPolicy.coolSoftTtlMs(entry);
    // A COOL rewrite is metadata-only: an already-expired soft window stays expired so
    // the next read still arms the refresh that was due. A still-fresh window keeps the
    // COOL cadence; only a value write or a successful refresh re-arms an expired one.
    long softExpireAt = ttlPolicy.transitionSoftExpireAt(entry, softTtl, TtlPolicy.COOL_PROTECTION_JITTER_RATIO, now);
    return editEntry(entry)
      .decision(stamp)
      .ttlDurations(hardTtl, softTtl)
      .expiryAt(ttlPolicy.coolHardExpireAt(hardTtl), softExpireAt)
      .keyState(KeyState.COOL)
      .build();
  }

  /**
   * The demotion rewrite: normal TTLs, NORMAL state and the decision stamp cleared in one draft
   * copy. The caller supplies the judgment through
   * {@link #demoteIfDecisionInvalid(String, CacheEntry, long)}, which re-runs the full predicate
   * under the bin lock.
   *
   * <p>Demotion is not a refresh: the value keeps its age, so the normal-life deadline caps the
   * remaining life instead of restarting it. A HOT entry's own deadline is the (usually far longer)
   * hot TTL, so the cap shortens it; an entry already inside the normal window keeps its own
   * deadline and demotion never buys it a second lifetime. The soft cadence is clamped the same
   * way, treating {@code 0} (disabled) as "no bound".
   */
  @Override
  public CacheEntry demoteToNormal(CacheEntry entry) {
    long normalHardTtlMs = ttlPolicy.resolveEffectiveHardTtl(entry.getNormalHardTtlMs());
    long normalSoftTtlMs = ttlPolicy.resolveEffectiveSoftTtl(entry.getNormalSoftTtlMs());
    // A permanent normal baseline (0 or Long.MAX_VALUE) has no deadline to cap the entry with:
    // the demoted entry becomes permanent as well, instead of keeping the HOT deadline while its
    // stored duration says "never expire".
    long hardExpireAt =
      normalHardTtlMs == 0L || normalHardTtlMs == Long.MAX_VALUE
        ? Long.MAX_VALUE
        : earlierDeadline(ttlPolicy.hardExpireAt(normalHardTtlMs), entry.getHardExpireAtMs(), Long.MAX_VALUE);
    long softExpireAt;
    if (normalSoftTtlMs == 0L) {
      softExpireAt = 0L;
    } else if (normalSoftTtlMs == Long.MAX_VALUE) {
      softExpireAt = Long.MAX_VALUE;
    } else {
      softExpireAt = earlierDeadline(ttlPolicy.softExpireAt(normalSoftTtlMs), entry.getSoftExpireAtMs(), 0L);
    }
    return editEntry(entry)
      .ttlDurations(normalHardTtlMs, normalSoftTtlMs)
      .expiryAt(hardExpireAt, softExpireAt)
      .keyState(KeyState.NORMAL)
      .clearDecision()
      .build();
  }

  /**
   * The pure TTL and expiry policy backing this lifecycle.
   *
   * @return the TTL policy; never null
   */
  @Override
  public TtlPolicy ttlPolicy() {
    return ttlPolicy;
  }

  /**
   * Whether a write at {@code version} loses to an invalidation this node
   * applied — the anti-resurrection guard every L1 write path shares.
   *
   * <p>A version-less write ({@code 0}) carries no ordering authority: it
   * loses to any tombstone for its key, and to nothing else. A positive one
   * loses to the clear watermark and to a per-key tombstone at or below its
   * version. The per-key boundary is inclusive: the invalidation applies at
   * equality, so a delayed PUT stamped in the same millisecond as a peer's
   * INVALIDATE can never re-install the value that invalidation removed.
   *
   * @param key     the key being written
   * @param version the stamp the write would store
   * @return {@code true} when the write predates an applied invalidation
   */
  @Override
  public boolean staleAgainstInvalidation(String key, long version) {
    if (version <= 0L) {
      return invalidatedAt.getIfPresent(key) != null;
    }

    if (version <= clearWatermark || staleAgainstPrefixClear(key, version)) {
      return true;
    }

    Long tombstone = invalidatedAt.getIfPresent(key);
    return tombstone != null && version <= tombstone;
  }

  /**
   * Whether {@code version} predates a scoped clear covering {@code cacheKey}.
   *
   * <p>One volatile read plus a max-stamp compare when the write outranks every
   * recorded clear — the common case, and the only cost once a namespace clear
   * has aged past the in-flight window. A version low enough to match a live
   * prefix pays the short walk, which compares stamps before touching the key
   * (integer first, string comparison second).
   *
   * @param key     the key being written
   * @param version the stamp the write would store
   * @return {@code true} when a prefix watermark covers the key at this version
   */
  private boolean staleAgainstPrefixClear(String key, long version) {
    PrefixWatermarks snapshot = prefixClearWatermarks;
    if (version > snapshot.maxStamp() || snapshot.stamps().isEmpty()) {
      return false;
    }

    for (Map.Entry<String, Long> mark : snapshot.stamps().entrySet()) {
      if (version <= mark.getValue() && key.startsWith(mark.getKey())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Single-prefix form of {@link #recordPrefixClears(Map)}. The watermark checks
   * run lock-free against the immutable snapshot first, so a replayed or
   * already-covered clear allocates no map and takes no lock. The batch form
   * re-checks every condition under the lock, so these reads are advisory.
   *
   * @param prefix     the key prefix
   * @param clearStamp the clear's stamp
   */
  @Override
  public void recordPrefixClear(String prefix, long clearStamp) {
    if (prefix == null || prefix.isEmpty() || clearStamp <= clearWatermark) {
      return;
    }
    Long prev = prefixClearWatermarks.stamps().get(prefix);
    if (prev != null && prev >= clearStamp) {
      return;
    }
    recordPrefixClears(Map.of(prefix, clearStamp));
  }

  /**
   * Record several namespace clear watermarks as one snapshot swap; only an
   * increase is stored. Runs under {@link #watermarkLock}, so concurrent clears
   * never lose each other. Covered stamps are dropped, then past
   * {@link #MAX_PREFIX_WATERMARKS} the expired entries shed, then the live set
   * folds into the table-wide watermark. A stamp this call records or raises is
   * never age-evicted: its encoded wall clock can lag a peer's (design
   * decisions: cache table).
   *
   * @param clears merged {@code prefix -> clear stamp} entries
   */
  @Override
  public void recordPrefixClears(Map<String, Long> clears) {
    if (clears.isEmpty()) {
      return;
    }

    synchronized (watermarkLock) {
      PrefixWatermarks snapshot = prefixClearWatermarks;
      Map<String, Long> previous = snapshot.stamps();
      Map<String, Long> updated = new HashMap<>(previous);
      boolean changed = updated.values().removeIf(stamp -> stamp <= clearWatermark);

      for (Map.Entry<String, Long> clear : clears.entrySet()) {
        String prefix = clear.getKey();
        Long clearStamp = clear.getValue();
        if (prefix == null || prefix.isEmpty() || clearStamp == null || clearStamp <= clearWatermark) {
          continue;
        }
        Long existing = updated.get(prefix);
        if (existing != null && existing >= clearStamp) {
          continue;
        }
        updated.put(prefix, clearStamp);
        changed = true;
      }
      // Fast return
      if (!changed) {
        return;
      }

      //--------------------Age Eviction : memory control for cache(similar to LRU)--------------------------
      if (updated.size() > MAX_PREFIX_WATERMARKS) {
        long expireAtMs =
          TimeSource.currentTimeMillis() - TimeUnit.SECONDS.toMillis(cacheConfig.getTombstoneTtlSeconds());
        updated
          .entrySet()
          .removeIf(mark -> {
            Long recorded = clears.get(mark.getKey());
            // A stamp this call adds or raises is that call's own in-flight window:
            // its encoded wall clock may lag a peer's, so it is never age-evicted.
            boolean recordedHere =
              recorded != null && recorded > clearWatermark && recorded > previous.getOrDefault(mark.getKey(), 0L);
            return !recordedHere && StampClock.timestampOf(mark.getValue()) <= expireAtMs;
          });

        //-------------------Utimate Eviction : fold the whole live set into the table-wide watermark--------
        if (updated.size() > MAX_PREFIX_WATERMARKS) {
          // The age escape did not reclaim enough room: fold the whole live set
          // into the table-wide watermark at the highest stamp involved. Folding
          // refuses strictly more writes, which is the safe direction.
          long foldStamp = updated.values().stream().mapToLong(Long::longValue).max().orElse(clearWatermark);
          clearWatermark = Math.max(clearWatermark, foldStamp);
          prefixClearWatermarks = new PrefixWatermarks(Map.of(), 0L);
          log.debug(
            "Prefix clear watermarks saturated at {} with {} still live; folding into the table-wide watermark at stamp {}",
            MAX_PREFIX_WATERMARKS,
            updated.size(),
            foldStamp
          );
          return;
        }
      }

      long liveMax = updated.values().stream().mapToLong(Long::longValue).max().orElse(0L);
      prefixClearWatermarks = new PrefixWatermarks(updated.isEmpty() ? Map.of() : Map.copyOf(updated), liveMax);
    }
  }

  /**
   * Record a per-key invalidation tombstone. A version-less ({@code 0})
   * invalidation carries no ordering information and is therefore not
   * recorded: treating it as "invalidated at infinity" would block every
   * later fill for the tombstone's whole TTL and the key could never be
   * re-warmed by anyone.
   *
   * @param key     the invalidated key
   * @param version the invalidation's stamp
   */
  @Override
  public void recordInvalidation(String key, long version) {
    if (version > 0L) {
      // Replays and out-of-order notices are common on a broadcast plane; a
      // tombstone that already covers this version needs no write. The merge
      // stays the atomic arbiter — this read only skips the common no-op that
      // would re-box the stamp and take the bin lock.
      Long prev = invalidatedAt.getIfPresent(key);
      if (prev != null && prev >= version) {
        return;
      }
      invalidatedAt.asMap().merge(key, version, Math::max);
    }
  }

  /**
   * Record a table-wide clear watermark; only an increase is stored. Prefix
   * watermarks the new watermark subsumes are pruned — no write can reach the
   * prefix walk with a version low enough to match them, so the entries are
   * dead weight on every L1 write.
   *
   * @param clearStamp the clear's stamp
   */
  @Override
  public void recordClear(long clearStamp) {
    if (clearStamp <= 0L) {
      return;
    }

    synchronized (watermarkLock) {
      clearWatermark = Math.max(clearWatermark, clearStamp);
      PrefixWatermarks snapshot = prefixClearWatermarks;
      Map<String, Long> current = snapshot.stamps();
      if (current.isEmpty()) {
        return;
      }
      if (clearWatermark >= snapshot.maxStamp()) {
        // Every live prefix stamp is subsumed: publish the empty snapshot without
        // allocating a map or walking it.
        prefixClearWatermarks = new PrefixWatermarks(Map.of(), 0L);
        return;
      }
      Map<String, Long> pruned = new HashMap<>(current);
      if (!pruned.values().removeIf(stamp -> stamp <= clearWatermark)) {
        return;
      }
      long liveMax = pruned.values().stream().mapToLong(Long::longValue).max().orElse(0L);
      prefixClearWatermarks = new PrefixWatermarks(pruned.isEmpty() ? Map.of() : Map.copyOf(pruned), liveMax);
    }
  }
}
