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
package io.github.hyshmily.zeta.cache.cachesupport.impl;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.cache.cachesupport.CacheCoreSettings;
import io.github.hyshmily.zeta.cache.cachesupport.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.TtlPolicy;
import io.github.hyshmily.zeta.cache.codec.CacheCompressor;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.EntryDraft;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.sharding.HealthView;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * Default {@link EntryLifecycle}: the stateful side of the {@link CacheEntry}
 * lifecycle — TOCTOU invalidation guards, Decision-Validity demotion
 * (ADR-0035), the single {@link EntryDraft} factory, and value wrapping.
 *
 * <p>Background soft-expire refresh scheduling lives behind the separate
 * {@link io.github.hyshmily.zeta.scheduler.BackgroundRefresher} seam
 * ({@code DefaultBackgroundRefresher}); this class deliberately owns no
 * executor and no scheduling state.
 */
@Slf4j
@Internal
public class EntryLifecycleImpl implements EntryLifecycle {

  /** The underlying L1 Caffeine cache instance. */
  private final Cache<String, CacheEntry> caffeineCache;
  /** TTL configuration providing normal and hot-key TTL values. */
  private final CacheCoreSettings ttlConfig;
  /** Pure TTL/expiry policy — all stateless lifecycle arithmetic lives here. */
  private final TtlPolicy ttlPolicy;
  /** Compressor for L1 cache values. */
  private final CacheCompressor compressor;

  /**
   * Cluster health view used for the Decision-Validity check
   * (ADR-0035). {@code null} disables demotion (test doubles, consumers
   * without a health view).
   */
  private final HealthView healthView;

  /**
   * Creates a lifecycle manager with the given Caffeine cache, TTL config,
   * compressor, and cluster health view.
   *
   * <p>The {@link HealthView} powers the Decision-Validity demotion (ADR-0035):
   * Worker-sourced HOT entries whose issuing Worker incarnation is dead or
   * restarted are reverted to the NORMAL lifecycle on the read path. A
   * {@code null} health view disables demotion.
   *
   * @param caffeineCache the underlying L1 Caffeine cache
   * @param ttlConfig     TTL configuration (normal and hot-key variants)
   * @param compressor    compressor for L1 cache values
   * @param healthView    cluster health view for the Decision-Validity check, or {@code null}
   */
  public EntryLifecycleImpl(
    Cache<String, CacheEntry> caffeineCache,
    CacheCoreSettings ttlConfig,
    CacheCompressor compressor,
    HealthView healthView
  ) {
    this(caffeineCache, ttlConfig, ttlConfig.getTtlJitterRatio(), compressor, healthView);
  }

  /**
   * Create a lifecycle manager with an explicit jitter ratio (for testing).
   *
   * <p>The single constructor body: the public constructor delegates here, so
   * the field wiring exists exactly once.
   */
  EntryLifecycleImpl(
    Cache<String, CacheEntry> caffeineCache,
    CacheCoreSettings ttlConfig,
    double defaultTtlJitterRatio,
    CacheCompressor compressor,
    HealthView healthView
  ) {
    this.caffeineCache = caffeineCache;
    this.ttlConfig = ttlConfig;
    this.compressor = compressor;
    this.healthView = healthView;
    this.ttlPolicy = new TtlPolicy(ttlConfig, defaultTtlJitterRatio);
  }

  /**
   * Check whether the given L1 entry is logically expired
   * and, if so, invalidate it and return {@code true}.
   * <p>Eliminates code duplication between the read paths of
   * {@code HotKeyCache}, which perform this check before and after side
   * effects (TOCTOU guard).
   *
   * @param cacheKey the cache key to invalidate if expired
   * @param raw      the L1 entry (may be {@code null})
   * @return {@code true} if the entry was expired and has been invalidated
   */
  @Override
  public boolean invalidateIfIsLogicallyExpired(String cacheKey, CacheEntry raw) {
    if (raw != null && ttlPolicy.isLogicallyExpired(raw)) {
      // The snapshot the caller examined is expired, so the caller must reload.
      // The removal itself is best-effort and identity-guarded: a concurrent
      // write (broadcast, putThrough, refresh) may have replaced the snapshot
      // with a fresh entry since it was read — that fresh entry must not be
      // destroyed by this snapshot-based decision (the reload path replaces it
      // with an equally fresh value instead).
      caffeineCache.asMap().computeIfPresent(cacheKey, (k, existing) -> existing == raw ? null : existing);
      log.debug("Cache entry logically expired during processing, reloading: {}", cacheKey);
      return true;
    }
    return false;
  }

  /**
   * Decision-Validity demotion (ADR-0035): revert a Worker-sourced HOT entry
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
   * and only fires for entries carrying a decision stamp. The predicate is
   * re-verified inside the atomic {@code compute}, so a concurrent fresh
   * broadcast (recovered Worker) is never clobbered. With no health view
   * configured, demotion is disabled (no-op, {@code false}).
   *
   * @param cacheKey the cache key
   * @param raw      the L1 entry (may be {@code null})
   * @return {@code true} if the entry was demoted
   */
  @Override
  public boolean demoteIfDecisionInvalid(String cacheKey, CacheEntry raw) {
    if (raw == null || !decisionIsInvalid(raw)) {
      return false;
    }
    DecisionStamp stamp = raw.decisionStamp();

    boolean[] demoted = new boolean[1];
    caffeineCache
      .asMap()
      .compute(cacheKey, (key, existing) -> {
        if (existing == null) {
          return null;
        }
        // Re-verify inside the atomic write: the entry may have been
        // re-stamped by a fresh broadcast (recovered Worker) or locally
        // re-promoted since the outer check. The pure function judges the
        // current entry independently — any now-invalid stamp is demoted.
        CacheEntry corrected = demoteIfDecisionInvalidInPlace(existing);
        if (corrected != null) {
          demoted[0] = true;
          return corrected;
        }
        return existing;
      });
    if (demoted[0] && stamp != null) {
      log.debug(
        "Decision-Validity demotion: key={} nodeId={} epoch={} reverted to NORMAL lifecycle",
        cacheKey,
        stamp.decisionNodeId(),
        stamp.decisionEpoch()
      );
    }
    return demoted[0];
  }

  /**
   * Pure Decision-Validity demotion: judge the entry and, when its issuing
   * Worker incarnation is dead or restarted, return it rewritten to NORMAL
   * (value preserved, normal TTLs, decision stamp cleared). {@code null} when
   * no demotion applies. Side-effect-free — the caller provides atomicity.
   *
   * @param entry the Worker-sourced cache entry to inspect
   * @return the demoted entry, or {@code null} if no demotion applies
   */
  @Override
  @Nullable
  public CacheEntry demoteIfDecisionInvalidInPlace(CacheEntry entry) {
    if (!decisionIsInvalid(entry)) {
      return null;
    }

    long normalHardTtlMs = ttlPolicy.resolveEffectiveHardTtl(entry.getNormalHardTtlMs());
    long normalSoftTtlMs = ttlPolicy.resolveEffectiveSoftTtl(entry.getNormalSoftTtlMs());
    // Single draft rewrite: normal TTLs + NORMAL state + decision stamp
    // cleared in one copy — formerly a four-link withXxx() chain.
    return editEntry(entry).ttl(normalHardTtlMs, normalSoftTtlMs).keyState(KeyState.NORMAL).clearDecision().build();
  }

  /**
   * The Decision-Validity predicate (ADR-0035), shared by the read-path guard
   * ({@link #demoteIfDecisionInvalid}) and the pure rewrite
   * ({@link #demoteIfDecisionInvalidInPlace}): an entry needs demotion when it
   * carries a Worker decision stamp on a HOT entry whose issuing incarnation
   * is gone (no health record, dead, or restarted to a new epoch). A disabled
   * health view and local-origin entries are never invalid — the Worker is the
   * sole authority for cooling the key down, so only it can orphan a HOT stamp.
   */
  @SuppressWarnings("all")
  private boolean decisionIsInvalid(CacheEntry entry) {
    HealthView view = healthView;
    if (view == null) {
      return false;
    }

    DecisionStamp stamp = entry.decisionStamp();
    if (stamp == null || entry.getKeyState() != KeyState.HOT) {
      return false;
    }
    String nodeId = stamp.decisionNodeId();
    // Authoritative only while the Worker is alive (same freshness judgment as
    // the ring/report path) and its epoch is unchanged (no restart).
    return !(view.isAlive(nodeId) && view.epochOf(nodeId) == stamp.decisionEpoch());
  }

  /**
   * Entry factory for creating new entries — the single creation path. See
   * {@link EntryLifecycle#newEntry()} for the draft contract (value discipline,
   * TTL resolution semantics).
   */
  @Override
  public EntryDraft newEntry() {
    return EntryDraft.blank(ttlPolicy);
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
  public Object wrapValue(@Nullable Object rawValue) {
    return compressor.wrap(rawValue);
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
}
