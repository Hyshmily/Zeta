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
package io.github.hyshmily.zeta.cache.cachesupport;

import com.github.benmanes.caffeine.cache.RemovalCause;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.model.CacheEntry;
import jakarta.annotation.Nullable;
import org.springframework.util.Assert;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Refault distance admission for the L1 load path (ADR-0079) — the
 * {@code mm/workingset.c} shadow-entry port.
 *
 * <p>
 * The gate answers a question the existing evidence chains (TinyLFU frequency,
 * HeavyKeeper TopK membership, Worker heat decisions) cannot: <b>"would this key
 * have survived in L1 until its next access?"</b> Scan/traversal traffic accumulates
 * real frequency on every cycle, so frequency-based admission legitimately beats
 * victims while the true working set is evicted underneath it — the kernel's
 * documented thrashing shape, where the only remedy is to not admit keys whose
 * inter-access distance exceeds the cache capacity.
 *
 * <h3>Mechanics (kernel mapping)</h3>
 * <ul>
 *   <li><b>Eviction clock</b> — {@code nonresident_age}: incremented once per
 *       <b>capacity</b> eviction ({@code RemovalCause.SIZE} only). Eviction count is
 *       the measure of "cache time". TTL expiries, replacements and explicit
 *       invalidations never advance it — counting them would drive distances at
 *       TTL cadence and mass-reject (the fail-closed trap the ADR calls out).</li>
 *   <li><b>Shadow table</b> — open-addressed {@code long[]} keyed by a spread hash of
 *       the cache key; slot holds {@code (clock & 0xFFFFFFFF) + 1} at the moment the
 *       key last left residency, with {@code 0} meaning "no evidence" (the cold-key
 *       rule: a never-evicted key is admitted). The {@code +1} bias reserves 0 —
 *       storing the raw clock would make the zero-initialized table read as
 *       "evicted an eternity ago" and permanently reject every cold key once the
 *       clock advanced (defect 1 in the ADR).</li>
 *   <li><b>Residency-end stamps</b> — {@code SIZE} (capacity eviction): advance the
 *       clock, then stamp. Exception: the SIZE eviction of a <em>solo-flight</em>
 *       entry (ADR-0079's short-TTL reject residue, flagged on the
 *       {@link CacheEntry}) stamps at the current clock <em>without</em> advancing
 *       — that churn is the gate's own byproduct, and counting it would feed the
 *       reject rate back into the clock every distance is measured against.
 *       {@code EXPIRED} (own TTL end): stamp <em>without</em> advancing the clock.
 *       This is the anchor refresh the kernel gets for free
 *       by re-evicting inactive pages: a rejected key's solo-flight entry ends with
 *       an EXPIRED stamp, so the next refault measures evictions since that cycle's
 *       end and the reject loop is self-correcting — sustained pressure keeps
 *       re-rejecting, a stopped flood re-admits. {@code EXPLICIT}
 *       (cross-instance invalidation): clear the slot — the kernel discards the
 *       shadow on truncate, and freshly written data deserves fresh admission.
 *       {@code REPLACED}/{@code COLLECTED}: no evidence change.</li>
 *   <li><b>Distance test</b> — {@code dist = (clock − slot) & 0xFFFFFFFF ≤ capacityEntries}
 *       → admit, else reject. Unsigned masked subtraction makes clock laps read as
 *       small distances — optimistic (kernel {@code workingset.c:495-512} accepts the
 *       same error direction). Bucket collisions overwrite a shadow with a newer
 *       stamp — also optimistic. After the ADR's two defect fixes, every residual
 *       error degrades toward "admit", i.e. toward pre-ADR behavior.</li>
 * </ul>
 *
 * <h3>Modes</h3>
 * <ul>
 *   <li>{@link Mode#OFF} — inert: no listener attached, zero allocation, the cache
 *       behaves byte-identically to pre-ADR-0079.</li>
 *   <li>{@link Mode#SHADOW} (default) — full machinery runs and every decision is
 *       counted, but {@link #admit} always returns {@link Decision#ADMIT};
 *       {@code zeta.l1.refault.reject.total} is the would-reject rate (the
 *       ADR-0078 shadow playbook).</li>
 *   <li>{@link Mode#ON} — enforced: a {@link Decision#REJECT} verdict stores the
 *       loaded value as a short-TTL solo-flight entry ({@link #rejectTtlMs()})
 *       instead of a full-TTL residency, coalescing backend load without
 *       accumulating residency.</li>
 * </ul>
 *
 * <p>
 * The capacity estimate is the static configured bound ({@code max-size}, or
 * {@code refault-capacity-entries} in weight mode) — the value Caffeine actually
 * enforces under pressure, which is the quantity the kernel compares against
 * (working-set size). A non-positive estimate is rejected at construction
 * (fail-fast): with no capacity term, every evidenced distance would exceed it
 * and the gate would mass-reject — the one direction the port must never lean.
 */
@Internal
public final class RefaultAdmission {

  /**
   * Gate mode (the autoconfigure-free core of the config enum, mapped by name
   * at the assembly layer — ADR-0082).
   */
  public enum Mode {
    /** Inert: no listener, no table, no decisions. */
    OFF,

    /** Compute and count every decision; always return ADMIT (default). */
    SHADOW,

    /** Enforce the reject verdict with a short-TTL solo-flight entry. */
    ON,
  }

  /** Gate verdict for one load-path admission decision. */
  public enum Decision {
    /** Evidence says the key fits (or no evidence at all): grant residency. */
    ADMIT,

    /** Distance exceeds capacity: the key would not have survived its own absence. */
    REJECT,
  }

  /** Working width of the clock: 32 bits, lap-safe via masked subtraction. */
  private static final long WIDTH_MASK = 0xFFFFFFFFL;
  /** Smallest shadow table (2^8 = 256 slots, 2 KiB). */
  private static final int MIN_BITS = 8;
  /** Auto-derived table ceiling (2^18 slots = 2 MiB). */
  private static final int MAX_AUTO_BITS = 18;
  /** Explicit-configuration table ceiling (2^24 slots = 128 MiB — operator's choice). */
  private static final int MAX_BITS = 24;
  /** Kernel density rule: table slots ≥ 8× capacity ({@code count_shadow_nodes}, pages &gt;&gt; 3). */
  private static final int DENSITY_FACTOR = 8;

  /** VarHandle for atomic (acquire/release) access to {@link #shadowSlots} elements. */
  private static final VarHandle SLOTS = MethodHandles.arrayElementVarHandle(long[].class);
  /** VarHandle for release/acquire access to {@link #lastDistance} — the distance gauge is best-effort. */
  private static final VarHandle LAST_DISTANCE;

  static {
    try {
      LAST_DISTANCE = MethodHandles.lookup().findVarHandle(RefaultAdmission.class, "lastDistance", long.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private final Mode mode;
  private final long rejectTtlMs;
  private final long capacityEntries;
  private final long[] shadowSlots;
  private final int slotMask;

  /** Capacity-eviction counter — the "cache time" clock. SIZE events only. */
  private final AtomicLong evictionClock = new AtomicLong();
  private final LongAdder admitCount = new LongAdder();
  private final LongAdder rejectCount = new LongAdder();
  /**
   * Distance of the most recent evidence-backed decision; -1 = decided without
   * evidence. Plain (non-final) field accessed via release/acquire
   * ({@link #LAST_DISTANCE}): the decision path runs on load threads while the
   * gauge is scraped on a Micrometer thread, and a volatile store there (an
   * mfence on x86) would put a cross-core invalidation on every miss — the
   * exact shape of a scan flood. Release/acquire keeps single-threaded
   * ordering (tests) and best-effort cross-thread visibility (the gauge),
   * without the fence.
   *
   * <p>
   * Not {@code final}: final fields reject VarHandle write access modes at
   * runtime ({@code UnsupportedOperationException}), and {@link #admit}
   * updates this field on every evidence-backed decision.
   */
  private long lastDistance = -1L;
  /** Scrape marks for the {@code clock.rate} gauge (single Micrometer scrape thread expected). */
  private final AtomicLong rateMarkClock = new AtomicLong();
  private final AtomicLong rateMarkNanos = new AtomicLong(System.nanoTime());
  private final AtomicLong rateBits = new AtomicLong(Double.doubleToLongBits(0.0));

  private RefaultAdmission(Mode mode, long rejectTtlMs, long capacityEntries, int tableBits) {
    this.mode = mode;
    this.rejectTtlMs = rejectTtlMs;
    this.capacityEntries = capacityEntries;
    this.shadowSlots = tableBits == 0 ? new long[0] : new long[1 << tableBits];
    this.slotMask = tableBits == 0 ? 0 : (1 << tableBits) - 1;
  }

  /**
   * Read-only view of the {@code zeta.local.cache.*} refault configuration
   * block consumed by {@link #from}. Implemented at the assembly layer by the
   * nested cache block of {@code ZetaProperties}; the cache packages never
   * import the autoconfigure package (ADR-0082). All reads are
   * construction-time: the gate extracts its final state once.
   */
  public interface Settings {

    /** The gate mode (already mapped to {@link Mode} by the implementation). */
    Mode mode();

    /** Hard TTL (ms) for solo-flight entries stored after a reject verdict. */
    long getRefaultRejectTtlMs();

    /** Explicit capacity estimate (entries); {@code 0} derives from {@link #getMaxSize()}. */
    long getRefaultCapacityEntries();

    /** Explicit shadow-table exponent; {@code 0} auto-derives (clamped to [2^8, 2^18]). */
    int getRefaultShadowBits();

    /** Configured entry bound (used as the capacity fallback in size mode). */
    int getMaxSize();
  }

  /**
   * Build the gate from the {@code zeta.local.cache.*} configuration.
   *
   * <p>Table sizing: an explicit {@code refault-shadow-bits} in [8, 24] wins;
   * otherwise the smallest power of two covering 8× the capacity estimate is
   * used, clamped to [2^8, 2^18]. A table smaller than 8× capacity raises the
   * collision rate, which degrades decisions optimistically (more admits) —
   * never pessimistically.
   *
   * @param cfg the L1 cache configuration block view
   * @return the gate (inert singleton-state when mode is {@link Mode#OFF})
   * @throws IllegalArgumentException when an explicit shadow-bits value is out of range,
   *         or when the capacity estimate resolves to zero or negative — in weight
   *         mode {@code max-size} is not the entry bound, so
   *         {@code refault-capacity-entries} must carry the expected steady-state
   *         entry count. A non-positive term would push every evidenced distance
   *         past it and mass-reject (pessimistic), so the gate refuses to arm.
   */
  public static RefaultAdmission from(Settings cfg) {
    Mode mode = cfg.mode();
    if (mode == Mode.OFF) {
      return new RefaultAdmission(mode, cfg.getRefaultRejectTtlMs(), 0, 0);
    }

    long capacity = cfg.getRefaultCapacityEntries() > 0 ? cfg.getRefaultCapacityEntries() : cfg.getMaxSize();
    Assert.isTrue(
      capacity > 0,
      "zeta.local.cache: refault admission needs a positive capacity estimate (max-size=" +
        cfg.getMaxSize() +
        ", refault-capacity-entries=" +
        cfg.getRefaultCapacityEntries() +
        "). In weight mode max-size is not the entry bound — set refault-capacity-entries."
    );

    int bits = cfg.getRefaultShadowBits();
    if (bits == 0) {
      long needed = Math.max(1L, capacity) * DENSITY_FACTOR;
      bits = Math.max(MIN_BITS, Math.min(MAX_AUTO_BITS, ceilLog2(needed)));
    } else if (bits < MIN_BITS || bits > MAX_BITS) {
      throw new IllegalArgumentException(
        "zeta.local.cache.refault-shadow-bits must be 0 (auto) or in [" + MIN_BITS + ", " + MAX_BITS + "]: " + bits
      );
    }
    return new RefaultAdmission(mode, cfg.getRefaultRejectTtlMs(), capacity, bits);
  }

  private static int ceilLog2(long value) {
    return 64 - Long.numberOfLeadingZeros(Math.max(1L, value - 1));
  }

  /**
   * Whether the gate machinery is running (listener must be attached, decisions
   * must be computed and counted).
   *
   * @return {@code false} only in {@link Mode#OFF}
   */
  public boolean gating() {
    return mode != Mode.OFF;
  }

  /**
   * Whether reject verdicts are enforced (solo-flight storage instead of full residency).
   *
   * @return {@code true} only in {@link Mode#ON}
   */
  public boolean enforcing() {
    return mode == Mode.ON;
  }

  /**
   * Hard TTL for solo-flight entries built on a reject verdict.
   *
   * @return the configured reject coalescing window in milliseconds
   */
  public long rejectTtlMs() {
    return rejectTtlMs;
  }

  /**
   * Value-less overload: the removal carries no solo-flight evidence, so every
   * {@code SIZE} eviction is treated as a true residency turnover. Tests and
   * callers without access to the removed value use this shape; the L1
   * wiring passes the value for the solo-flight distinction.
   *
   * @param key   the removed entry's key
   * @param cause the Caffeine removal cause
   */
  public void onRemoval(Object key, RemovalCause cause) {
    onRemoval(key, null, cause);
  }

  /**
   * Removal-notification entry point — the clock and shadow keeper. Wired as the
   * L1 Caffeine {@code removalListener} by the auto-configuration; does nothing in
   * {@link Mode#OFF}.
   *
   * <p>Cause handling: {@code SIZE} advances the clock then stamps (capacity
   * turnover) — except a solo-flight entry's SIZE eviction, which stamps without
   * advancing (the gate's own churn must not feed the reject rate back into the
   * clock every distance is measured against); {@code EXPIRED} stamps without
   * advancing (residency-end anchor refresh — the reject loop's self-correction);
   * {@code EXPLICIT} clears the slot (truncate semantics: invalidated data
   * re-admits fresh); {@code REPLACED} and {@code COLLECTED} leave evidence
   * untouched.
   *
   * <p>Ordering residual (documented, accepted): removal notifications are
   * delivered through the cache executor, and Caffeine does not preserve their
   * order across delivery threads. An {@code EXPLICIT} clear can therefore lose
   * to a same-key stamp whose eviction event predates it — the evidence
   * re-appears, a pessimistic direction that costs at most one extra reject
   * cycle before the next anchor re-stamp self-corrects. Serializing the keeper
   * would not close this: the reordering happens upstream, in the delivery
   * executor, before the keeper sees any event.
   *
   * @param key   the removed entry's key (non-String keys are ignored — the L1 is a
   *              {@code Cache<String, Object>}; the guard only defends against misuse)
   * @param value the removed entry's value; a {@link CacheEntry} flagged
   *              {@code soloFlight} marks its SIZE eviction as the gate's own
   *              churn (no clock advance). May be {@code null}
   * @param cause the Caffeine removal cause
   */
  public void onRemoval(Object key, @Nullable Object value, RemovalCause cause) {
    if (mode == Mode.OFF || !(key instanceof String cacheKey)) {
      return;
    }

    switch (cause) {
      case SIZE -> {
        if (value instanceof CacheEntry entry && entry.isSoloFlight()) {
          // The gate's own residue: re-stamp the anchor (the residency ended),
          // but the clock must not count churn the gate itself produced —
          // otherwise the reject rate inflates every distance and the gate
          // degenerates into rejecting everything it has ever seen leave.
          stamp(cacheKey, evictionClock.get());
        } else {
          long now = evictionClock.incrementAndGet();
          stamp(cacheKey, now);
        }
      }
      case EXPIRED -> stamp(cacheKey, evictionClock.get());
      case EXPLICIT -> SLOTS.setRelease(shadowSlots, hash(cacheKey), 0L);
      default -> {
        // REPLACED: the entry is still resident under the same key.
        // COLLECTED: no weak/soft refs in Zeta's L1 — cannot occur.
      }
    }
  }

  /**
   * The admission gate — called from {@code HotKeyCache.loadCacheEntry} inside the
   * Caffeine {@code compute} for a key that is absent (fresh insert) or whose entry is
   * logically expired (dead residency → re-admission decision). Fresh resident
   * entries are never passed here: their update is residency continuation, not an
   * admission decision.
   *
   * <p>In {@link Mode#SHADOW} the verdict is computed and counted but the return
   * value is always {@link Decision#ADMIT} — the caller admits unconditionally and
   * {@code zeta.l1.refault.reject.total} accumulates the would-reject rate.
   *
   * @param cacheKey the key being (re-)admitted
   * @return the verdict; enforcement is the caller's duty (see {@link #enforcing()})
   */
  public Decision admit(String cacheKey) {
    if (mode == Mode.OFF) {
      // Defensive: the caller guards with gating(); a stray call must not index
      // the (zero-length) table or record decisions the machinery never ran.
      return Decision.ADMIT;
    }

    long slot = (long) SLOTS.getAcquire(shadowSlots, hash(cacheKey));
    long dist;
    Decision verdict;
    if (slot == 0L) {
      // No evidence: the key never left residency through this slot — the cold-key
      // rule. Admitting is what the kernel does for a fault-in with no shadow entry.
      dist = -1L;
      verdict = Decision.ADMIT;
    } else {
      dist = (evictionClock.get() - (slot - 1L)) & WIDTH_MASK;
      verdict = dist <= capacityEntries ? Decision.ADMIT : Decision.REJECT;
    }

    LAST_DISTANCE.setRelease(this, dist);
    if (verdict == Decision.ADMIT) {
      admitCount.increment();
    } else {
      rejectCount.increment();
    }
    return mode == Mode.ON ? verdict : Decision.ADMIT;
  }

  private void stamp(String cacheKey, long clockValue) {
    SLOTS.setRelease(shadowSlots, hash(cacheKey), (clockValue & WIDTH_MASK) + 1L);
  }

  private int hash(String cacheKey) {
    int h = cacheKey.hashCode();
    return (h ^ (h >>> 16)) & slotMask;
  }

  /** Cumulative admit verdicts (shadow mode: includes would-admit). */
  public long admitCount() {
    return admitCount.sum();
  }

  /** Cumulative reject verdicts (shadow mode: the would-reject rate). */
  public long rejectCount() {
    return rejectCount.sum();
  }

  /**
   * Distance of the latest evidence-backed decision; {@code -1} = last decision had no evidence.
   * Best-effort across threads (release/acquire, no volatile fence): the gauge is scraped
   * while load threads update it, and a slightly stale read is acceptable — a volatile
   * store on the decision path would cost a cross-core invalidation per miss.
   */
  public long lastDistance() {
    return (long) LAST_DISTANCE.getAcquire(this);
  }

  /** The static capacity estimate (entries) distances are compared against. */
  public long capacityEntries() {
    return capacityEntries;
  }

  /** Current eviction-clock value (SIZE evictions since construction). */
  public long evictionClockValue() {
    return evictionClock.get();
  }

  /**
   * Capacity-eviction rate (evictions/sec), EWMA-smoothed (α = 1/8) over
   * Micrometer scrape deltas. Independently valuable as the scan-pressure alarm:
   * a sustained non-zero rate is capacity thrashing, whatever the gate decides.
   *
   * @return the smoothed eviction rate; 0.0 before the second scrape
   */
  public double clockRatePerSec() {
    long nowNanos = System.nanoTime();
    long nowClock = evictionClock.get();
    long prevNanos = rateMarkNanos.getAndSet(nowNanos);
    long prevClock = rateMarkClock.getAndSet(nowClock);
    double prev = Double.longBitsToDouble(rateBits.get());
    long elapsed = nowNanos - prevNanos;
    if (elapsed <= 0) {
      return prev;
    }

    double rate = ((nowClock - prevClock) * 1_000_000_000.0) / elapsed;
    double next = prev == 0.0 ? rate : prev + (rate - prev) / 8.0;
    rateBits.set(Double.doubleToLongBits(next));
    return next;
  }

  /** Test-only: pin the eviction clock to simulate lap/wrap without 2^32 events. */
  void forceEvictionClockForTest(long value) {
    evictionClock.set(value);
  }

  /** Test-only: shadow table slot count (0 in {@link Mode#OFF}). */
  int tableSlots() {
    return shadowSlots.length;
  }
}
