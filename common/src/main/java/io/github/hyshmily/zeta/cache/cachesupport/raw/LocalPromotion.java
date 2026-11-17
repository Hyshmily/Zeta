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
package io.github.hyshmily.zeta.cache.cachesupport.raw;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.TtlPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryMutations.Applied;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Local-TopK promotion: the policy that lifts a NORMAL (or degraded COOL)
 * entry to HOT when the local TopK considers its key hot.
 *
 * <p>The policy is separate from {@link HotKeyCache} so the promotion rules
 * evolve in one place, independently of the cache mechanics around them.
 * NORMAL promotion is fed by TopK admission events and runs off the read path;
 * the read path only runs the degraded-mode COOL branch via
 * {@link #promoteDegraded}. Both issue their own Caffeine {@code compute}.
 *
 * <p>A Worker-managed HOT entry is never touched; cooling is the Worker's job
 * (a COOL decision) and reload-after-expiry. A Worker-managed COOL entry is
 * promoted back to HOT only while the cluster health gate has failed (degraded
 * mode).
 *
 * <p>Thread safety: the only mutable state is a bounded admission queue;
 * collaborators are thread-safe and every promotion is re-verified inside its
 * own Caffeine {@code compute} — residency, promotable state and logical
 * expiry. An expired resident is removed, never re-stamped with a new deadline.
 */
@Internal
public final class LocalPromotion {

  /** Bound on pending admission events; a full queue drops (loads and writes re-check membership). */
  private static final int ADMISSION_QUEUE_CAPACITY = 4_096;
  /** Maximum keys drained per scheduled tick, bounding a scheduler pause. */
  private static final int ADMISSION_DRAIN_LIMIT = 2_048;

  /** Admission residency probe: no hit/miss or eviction-policy side effect. */
  private final QuietCache<String, CacheEntry> quietCache;
  /** Underlying L1 Caffeine cache storing {@link CacheEntry} slots. */
  private final Cache<String, CacheEntry> caffeineCache;
  /** Local TopK detector (HotKeyDetector) for identifying hot keys. */
  private final HotKeyDetector hotKeyDetector;
  /** Manages entry lifecycle: guards, demotion, the draft factory, value wrapping. */
  private final EntryLifecycle entryLifecycle;
  /** Cached view of Worker cluster health, used for COOL promotion decisions. */
  private final HealthView healthView;
  /** The stateless TTL/expiry arithmetic, resolved once from the lifecycle it belongs to. */
  private final TtlPolicy ttlPolicy;

  /** Keys that just entered the local TopK; drained off the read path. */
  private final BlockingQueue<String> admittedKeys = new ArrayBlockingQueue<>(ADMISSION_QUEUE_CAPACITY);

  public LocalPromotion(
    Cache<String, CacheEntry> caffeineCache,
    HotKeyDetector hotKeyDetector,
    EntryLifecycle entryLifecycle,
    HealthView healthView
  ) {
    this.healthView = healthView;
    this.caffeineCache = caffeineCache;
    this.entryLifecycle = entryLifecycle;
    this.hotKeyDetector = hotKeyDetector;
    this.ttlPolicy = entryLifecycle.ttlPolicy();
    this.quietCache = new QuietCache<>(caffeineCache);
  }

  /**
   * Performs the degraded-mode promotion the read path already admitted through
   * {@link #isDegradedPromotable}: a COOL entry, or a NORMAL entry still carrying a COOL
   * lease, may go HOT only while the Worker cluster fails the health gate. Plain
   * NORMAL promotion stays event-driven ({@link #onAdmitted} →
   * {@link #promoteAdmitted}), so an ordinary normal hit pays no TopK membership
   * probe.
   *
   * <p>The caller has checked TopK membership and cluster health; the compute
   * re-verifies residency, logical expiry and {@link #mayPromote} under
   * the bin lock. The two override values are passed explicitly, so promotion
   * does not depend on a read context.
   *
   * @param cacheKey           the cache key
   * @param hardTtlOverrideMs the per-call hard TTL override (0 = none)
   * @param softTtlOverrideMs the per-call soft TTL override (0 = none)
   * @param now                the per-call epoch-millis sample
   * @return the images the bin lock observed ({@code current == null} means the
   *         resident was removed as hard-expired); {@link Applied#changed()} is
   *         {@code false} when the resident was absent, already promoted, or not
   *         promotable
   */
  public Applied promoteDegraded(String cacheKey, long hardTtlOverrideMs, long softTtlOverrideMs, long now) {
    return promoteIfAllowed(cacheKey, hardTtlOverrideMs, softTtlOverrideMs, now);
  }

  /**
   * Read-only degraded-promotion gate: the entry is COOL, or NORMAL carrying a
   * COOL lease, the cluster health gate has failed, and the local TopK still
   * tracks the key. The read path uses it to decide whether a hit must enter
   * reconciliation; {@link #promoteDegraded} re-checks under the promotion
   * compute.
   *
   * @param cacheKey the cache key
   * @param raw      the L1 entry just read (never {@code null})
   * @return {@code true} when the hit needs the promotion path
   */
  public boolean isDegradedPromotable(String cacheKey, CacheEntry raw) {
    KeyState state = raw.getKeyState();
    DecisionStamp decision = raw.decision();
    boolean degradedCool =
      state == KeyState.COOL || (state == KeyState.NORMAL && decision != null && decision.isCool());
    return degradedCool && !healthView.isClusterHealthy() && hotKeyDetector.contains(cacheKey);
  }

  /**
   * Whether this entry may be promoted to local HOT now, regardless of which
   * path (read, write or admission drain) asks.
   *
   * <p>A Worker-COOL lease survives a COOL-to-NORMAL refresh, so checking only
   * the state would let a healthy cluster locally override a cool decision.
   * The lease kind is therefore the gate: while the cluster is healthy a COOL
   * lease keeps the entry out of local promotion.
   *
   * @param entry the resident entry
   * @return {@code true} when local promotion is allowed
   */
  public boolean mayPromote(CacheEntry entry) {
    KeyState state = entry.getKeyState();
    if (state == KeyState.HOT) {
      return false;
    }
    if (state == KeyState.COOL) {
      return !healthView.isClusterHealthy();
    }

    DecisionStamp decision = entry.decision();
    return decision == null || decision.isHot() || !healthView.isClusterHealthy();
  }

  /**
   * Whether a read must reassert the retained COOL lease after recovery.
   *
   * @param raw the resident entry
   * @return {@code true} when the entry is a local override and the cluster is healthy
   */
  public boolean needsReassert(CacheEntry raw) {
    return raw.isLocalOverrideHot() && healthView.isClusterHealthy();
  }

  /**
   * Reasserts the retained Worker decision after the health gate recovers.
   *
   * <p>A local promotion keeps the COOL lease for ordering, but the effective
   * HOT state is the App's. When the Worker is reachable again, a live COOL
   * lease returns the entry to COOL; a dead/restarted Worker drops the lease
   * and leaves a local HOT (or returns to NORMAL when the local TopK no longer
   * tracks the key). This is the read-path convergence for a promotion that
   * happened during degradation.
   *
   * @param cacheKey the key to reconcile
   * @param raw      the resident entry read by the caller
   * @param now      the per-call epoch-millis sample
   * @return the images the bin lock observed; {@link Applied#changed()} is
   *         {@code false} when the lease needs no reassertion
   */
  public Applied reassertIfNeeded(String cacheKey, CacheEntry raw, long now) {
    if (!ttlPolicy.isLogicallyExpired(raw, now) && !needsReassert(raw)) {
      return EntryMutations.unchanged(raw);
    }

    return EntryMutations.apply(caffeineCache, cacheKey, existing -> {
      if (existing == null || ttlPolicy.isLogicallyExpired(existing, now)) {
        return null;
      }
      if (!existing.isLocalOverrideHot() || !healthView.isClusterHealthy()) {
        return existing;
      }

      DecisionStamp decision = existing.decision();
      if (decision == null) {
        return existing;
      }
      if (
        decision.decisionNodeId() != null &&
        healthView.isCurrentIncarnation(decision.decisionNodeId(), decision.decisionEpoch())
      ) {
        return entryLifecycle.cool(existing, decision);
      }
      return hotKeyDetector.contains(cacheKey)
        ? entryLifecycle.editEntry(existing).clearDecision().build()
        : entryLifecycle.demoteToNormal(existing);
    });
  }

  /**
   * Admission sink installed on {@link HotKeyDetector}: called on the detection
   * thread under the sketch's admission lock, so it only offers to a bounded
   * queue. A full queue drops the event; a load or write that finds the key
   * already a member still stamps HOT.
   */
  public void onAdmitted(String cacheKey) {
    if (cacheKey != null) {
      admittedKeys.offer(cacheKey);
    }
  }

  /**
   * Re-queues a key whose Worker decision was just invalidated and rewritten to
   * NORMAL. Its original admission event was consumed long ago, so without this
   * the entry would keep the normal TTLs until it expired and reloaded even
   * while the local TopK still considers it hot. The membership check keeps
   * genuinely cold demotions out of the queue.
   *
   * @param cacheKey the demoted key
   */
  public void onDemoted(String cacheKey) {
    if (cacheKey != null && hotKeyDetector.contains(cacheKey)) {
      admittedKeys.offer(cacheKey);
    }
  }

  /**
   * Drain admission events and promote resident NORMAL entries. Runs on the
   * shared scheduler, never on the read path. Each key is re-checked against
   * the current TopK before the atomic promote, because an eviction may have
   * happened between admission and this tick.
   *
   * @return the number of entries this call transitioned to HOT
   */
  public int promoteAdmitted() {
    int promoted = 0;
    for (int i = 0; i < ADMISSION_DRAIN_LIMIT; i++) {
      String cacheKey = admittedKeys.poll();
      if (cacheKey == null) {
        break;
      }

      if (hotKeyDetector.contains(cacheKey)) {
        Applied applied = promote(cacheKey);
        if (applied.changed() && applied.current() != null) {
          promoted++;
        }
      }
    }
    return promoted;
  }

  /**
   * Atomically promote a cache entry to HOT state in L1, respecting the
   * Worker-managed guard: a HOT entry is left untouched, and a COOL entry is
   * promotable only while the cluster health gate has failed. A logically
   * expired resident is removed instead of re-stamped: promotion must never
   * resurrect data whose hard deadline has already passed.
   *
   * <p>A lock-free {@code getIfPresent} drops absent and live already-promoted
   * entries before the bin lock is taken — the common shape of an admission
   * event whose load already stamped HOT. An expired snapshot still enters the
   * compute so it is removed. The compute re-verifies residency, expiry and
   * promotable state under the lock; TopK membership was checked by the caller
   * and is advisory, so it is not probed there.
   *
   * @param key the key to promote
   * @return the images the bin lock observed; {@link Applied#changed()} is
   *         {@code false} when nothing changed
   */
  private Applied promote(String key) {
    CacheEntry snapshot = quietCache.getIfPresent(key);
    if (snapshot == null) {
      return EntryMutations.unchanged(null);
    }
    long now = TimeSource.currentTimeMillis();
    // A live non-promotable resident (already HOT, COOL while healthy, or
    // NORMAL with a COOL lease while healthy) is the common admission duplicate
    // and must not take the bin lock. An expired resident of any state still
    // reaches the compute, which removes it.
    if (!ttlPolicy.isLogicallyExpired(snapshot, now) && !mayPromote(snapshot)) {
      return EntryMutations.unchanged(snapshot);
    }
    return promoteIfAllowed(key, 0L, 0L, now);
  }

  /**
   * The shared promotion compute: re-verifies residency, expiry and promotability under the bin
   * lock, then rewrites the entry with the caller's TTL overrides (if any) layered over the
   * entry's own normal baseline.
   *
   * @param key                the key to promote
   * @param hardTtlOverrideMs the per-call hard TTL override (0 = none)
   * @param softTtlOverrideMs the per-call soft TTL override (0 = none)
   * @param now                the per-call epoch-millis sample
   * @return the images the bin lock observed; {@link Applied#changed()} is
   *         {@code false} when nothing changed
   */
  private Applied promoteIfAllowed(String key, long hardTtlOverrideMs, long softTtlOverrideMs, long now) {
    return EntryMutations.apply(caffeineCache, key, entry -> {
      if (entry == null || ttlPolicy.isLogicallyExpired(entry, now)) {
        return null;
      }
      if (!mayPromote(entry)) {
        return entry;
      }
      return promotedEntry(entry, hardTtlOverrideMs, softTtlOverrideMs, now);
    });
  }

  /**
   * The local-promotion rewrite: hot TTLs with computed expire timestamps,
   * state → HOT; the normal baseline carries over from the source entry for
   * later demotion.
   *
   * <p>Promotion only ever extends life: the entry's normal TTL baseline is
   * the floor, and the caller's override may raise it further
   * ({@link TtlPolicy#resolveHotHardTtl(long)} /
   * {@link TtlPolicy#resolveHotSoftTtl(long)}). An entry written with a
   * longer explicit TTL therefore keeps that lifetime when it becomes HOT.
   *
   * <p>Promotion is a metadata rewrite, not a value refresh. A source entry
   * that is already soft-expired keeps its past soft deadline: moving it to
   * {@code now + hotSoftTtl} would make stale data look freshly loaded and
   * suppress its next background refresh for the whole hot window. Only a value
   * write or a successful refresh re-arms an expired soft window; a still-fresh
   * window keeps the new regime's cadence. A disabled ({@code 0}) or permanent
   * ({@code Long.MAX_VALUE}) hot soft TTL is the new regime and replaces the past
   * deadline outright: keeping it would mark the entry stale on every read.
   *
   * @param entry             the entry to promote (not modified)
   * @param hardTtlOverrideMs the caller's hard TTL override (0 = none)
   * @param softTtlOverrideMs the caller's soft TTL override (0 = none)
   * @param now               the per-call epoch-millis sample
   * @return the promoted entry to store back
   */
  private CacheEntry promotedEntry(CacheEntry entry, long hardTtlOverrideMs, long softTtlOverrideMs, long now) {
    long normalHard = entry.getNormalHardTtlMs() > 0L ? entry.getNormalHardTtlMs() : ttlPolicy.effectiveHardTtlMs();
    long normalSoft = entry.getNormalSoftTtlMs() > 0L ? entry.getNormalSoftTtlMs() : ttlPolicy.effectiveSoftTtlMs();
    // A resolved normal hard baseline of 0 is permanent: a positive override must not turn it into
    // a bounded lifetime. A soft baseline of 0 only means the normal regime was disabled — the hot
    // default may re-enable it — so the caller's override still raises the window.
    long baseHard = normalHard == 0L ? 0L : Math.max(hardTtlOverrideMs, normalHard);
    long baseSoft = Math.max(softTtlOverrideMs, normalSoft);
    long hotHardTtl = ttlPolicy.resolveHotHardTtl(baseHard);
    long hotSoftTtl = ttlPolicy.resolveHotSoftTtl(baseSoft);
    long softExpireAt = ttlPolicy.transitionSoftExpireAt(entry, hotSoftTtl, now);
    return entryLifecycle
      .editEntry(entry)
      .ttlDurations(hotHardTtl, hotSoftTtl)
      .expiryAt(ttlPolicy.hardExpireAt(hotHardTtl), softExpireAt)
      .keyState(KeyState.HOT)
      .build();
  }
}
