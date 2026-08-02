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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import javax.security.auth.Destroyable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;

/**
 * Double-buffered counter that aggregates high-frequency single-key increments
 * and flushes them in batch to a downstream consumer.
 *
 * <p><b>Design:</b> 64 hash-indexed slots, each with an active
 * {@link CounterBuffer} accepting incoming {@link #count(String, long)} calls.
 * When a buffer exceeds the eager-swap threshold the thread that detects it
 * atomically swaps in a fresh buffer and enqueues the old one for async
 * draining.  A per-slot <em>spill</em> buffer catches concurrent {@code add()}
 * calls during the swap window so no thread performs a wasted CAS — losers
 * redirect into the spill, and the winner drains it back into the new active
 * buffer.  A scheduled flusher drains all queues and active buffers every
 * {@code flushIntervalMs}.
 *
 * <p><b>Spill ticket protocol:</b> spill writers hold a per-slot ticket for
 * the duration of their {@code add()}; reclaimers (swap winner, flusher,
 * destroy) wait for tickets to drain before their final drain, so no
 * in-flight {@code add()} can be stranded in a detached spill.  Writers
 * re-check the spill reference after taking the ticket and retry via the
 * active path if the spill was recycled in between.  The hot path never
 * touches the ticket counters.
 *
 * <p><b>Safety net:</b> Every flush cycle also drains any leftover spill
 * buffers, bounding worst-case spill data delay to {@code flushIntervalMs}.
 *
 * <p><b>Bounded flush queue:</b> the async drain queue has a fixed capacity
 * ({@link #DEFAULT_FLUSH_QUEUE_CAPACITY} by default).  When a stalled
 * consumer fills it, incoming batches are dropped and counted via
 * {@link #droppedFlushBatches()} instead of growing heap unboundedly —
 * bounded memory at the price of bounded count loss (ADR-0013).
 *
 * <p>Thread-safe. All public methods can be called concurrently from
 * multiple threads.
 */
@Slf4j
@Internal
public class BufferedCounter implements InitializingBean, Destroyable {

  private static final int DEFAULT_MAX_CEIL_SIZE = 10_000;

  private static final long DEFAULT_FLUSH_INTERVAL_MS = 500;

  private static final double DEFAULT_EAGER_SWAP_RATIO = 0.75;

  /**
   * Default capacity of the bounded flush queue.
   * Bounding the queue protects against unbounded memory growth when the
   * downstream consumer stalls; overflow batches are dropped and counted
   * (see {@link #droppedFlushBatches()}).
   */
  private static final int DEFAULT_FLUSH_QUEUE_CAPACITY = 1024;

  /** Fixed slot array size — never changes, only {@link #ceilCount} limits which slots are live. */
  private static final int MAXS_CEILS = 64;

  private final int ceilMaxCapacity;

  private final long flushIntervalMs;

  /**
   * Number of live hash slots (power of 2, 8..64).
   * Only slots {@code [0, ceilCount)} receive traffic; higher indices are
   * orphaned after a shrink and must be drained by iterating up to MAXS_CEILS.
   *
   * <p>Initialized from the CPU count (capped at MAXS_CEILS) so that the hash
   * width matches the expected thread contention from the start, instead of
   * waiting for the saturation-triggered auto-tuner to expand it.
   */
  private volatile int ceilCount = Math.max(
    8,
    Integer.highestOneBit(Math.min(64, Runtime.getRuntime().availableProcessors() << 1))
  );

  /** Active buffer per slot — each is a lock-free ConcurrentHashMap-based counter. */
  private final AtomicReference<CounterBuffer>[] activeCeils;

  /**
   * Per-slot spill buffer, normally {@code null}.
   * Non-null only while a swap is in progress: the thread that sets it wins
   * the right to swap the active buffer; all other threads sees spill != null
   * and redirect their {@code add()} into it.  The winner drains the spill
   * back into the new active after the swap completes.
   */
  private final AtomicReference<CounterBuffer>[] spillCeils;

  /**
   * Per-slot in-flight spill writer count (ticket protocol).
   *
   * <p>A writer increments before {@code add()} into the spill and decrements
   * after; a reclaiming thread ({@link #trySwap(int, CounterBuffer)} winner,
   * the flusher, or {@link #destroy()}) waits for this to reach zero before
   * its final drain.  This proves no in-flight {@code add()} can be stranded
   * in a detached spill — the stale-reader window where an {@code add()}
   * lands after the reclaiming drain is closed by the writer's re-check
   * (the reference is re-read after the ticket is taken, and a recycled
   * spill redirects the writer to the active path).
   *
   * <p>Only the spill path touches this array; the hot path (no swap in
   * progress) never does.
   */
  private final AtomicIntegerArray spillCeilReaders;

  /**
   * Bounded queue of old active buffers awaiting async drain by {@link #flushStandby()}.
   * Using a queue decouples the hot-path swap from the downstream consumer
   * (which may block, e.g. on RabbitMQ publish).
   *
   * <p>The queue is bounded so a stalled consumer cannot grow heap
   * unboundedly: when the queue is full, the incoming batch is dropped and
   * counted by {@link #flushDropCounter} (consistent with the library's
   * acceptable-loss philosophy, ADR-0013).
   */
  private final ArrayBlockingQueue<CounterBuffer> flushQueue;

  /** Number of batches dropped because the flush queue was full (bounded-queue safety valve). */
  private final LongAdder flushDropCounter = new LongAdder();

  /** Downstream consumer receiving aggregated {@code Map<key, count>} snapshots. */
  private final Consumer<Map<String, Long>> batchConsumer;

  private final ScheduledExecutorService scheduler;

  /** {@code true} if this instance created its own scheduler and must shut it down on destroy. */
  private final boolean ownsScheduler;

  /** Tracks how many eager swaps occurred since the last flush. */
  private final LongAdder swapCounter = new LongAdder();

  /** Number of flush cycles accumulated in the current sampling window. */
  private int sampleWindows;

  /** Sum of swap counts across flushes in the current window. */
  private int sampleHits;

  private volatile boolean shutdown;

  private final int eagerSwapThreshold;

  public BufferedCounter(Consumer<Map<String, Long>> batchConsumer) {
    this(
      batchConsumer,
      DEFAULT_MAX_CEIL_SIZE,
      DEFAULT_FLUSH_INTERVAL_MS,
      DEFAULT_EAGER_SWAP_RATIO,
      true,
      null,
      DEFAULT_FLUSH_QUEUE_CAPACITY
    );
  }

  public BufferedCounter(Consumer<Map<String, Long>> batchConsumer, ScheduledExecutorService scheduler) {
    this(
      batchConsumer,
      DEFAULT_MAX_CEIL_SIZE,
      DEFAULT_FLUSH_INTERVAL_MS,
      DEFAULT_EAGER_SWAP_RATIO,
      false,
      scheduler,
      DEFAULT_FLUSH_QUEUE_CAPACITY
    );
  }

  public BufferedCounter(
    Consumer<Map<String, Long>> batchConsumer,
    int ceilMaxCapacity,
    long flushIntervalMs,
    double eagerSwapRatio,
    ScheduledExecutorService scheduler
  ) {
    this(
      batchConsumer,
      ceilMaxCapacity,
      flushIntervalMs,
      eagerSwapRatio,
      false,
      scheduler,
      DEFAULT_FLUSH_QUEUE_CAPACITY
    );
  }

  /**
   * Full constructor with explicit flush queue capacity.
   *
   * @param batchConsumer     downstream consumer receiving merged snapshots
   * @param ceilMaxCapacity   max distinct keys per slot before eager swap
   * @param flushIntervalMs   periodic flush interval
   * @param eagerSwapRatio    saturation ratio triggering an eager swap
   * @param scheduler         scheduler for the periodic flusher (not shut down by this instance)
   * @param flushQueueCapacity bounded capacity of the flush queue; overflow batches are
   *                          dropped and counted by {@link #droppedFlushBatches()}
   */
  public BufferedCounter(
    Consumer<Map<String, Long>> batchConsumer,
    int ceilMaxCapacity,
    long flushIntervalMs,
    double eagerSwapRatio,
    ScheduledExecutorService scheduler,
    int flushQueueCapacity
  ) {
    this(batchConsumer, ceilMaxCapacity, flushIntervalMs, eagerSwapRatio, false, scheduler, flushQueueCapacity);
  }

  @SuppressWarnings("unchecked")
  private BufferedCounter(
    Consumer<Map<String, Long>> batchConsumer,
    int ceilMaxCapacity,
    long flushIntervalMs,
    double eagerSwapRatio,
    boolean ownsScheduler,
    ScheduledExecutorService scheduler,
    int flushQueueCapacity
  ) {
    this.batchConsumer = batchConsumer;
    this.ceilMaxCapacity = ceilMaxCapacity;
    this.flushIntervalMs = flushIntervalMs;
    eagerSwapThreshold = (int) (ceilMaxCapacity * eagerSwapRatio);

    this.activeCeils = new AtomicReference[MAXS_CEILS];
    this.spillCeils = new AtomicReference[MAXS_CEILS];
    this.spillCeilReaders = new AtomicIntegerArray(MAXS_CEILS);
    for (int i = 0; i < MAXS_CEILS; i++) {
      this.activeCeils[i] = new AtomicReference<>(new CounterBuffer());
      this.spillCeils[i] = new AtomicReference<>(null);
    }
    this.flushQueue = new ArrayBlockingQueue<>(Math.max(1, flushQueueCapacity));
    this.ownsScheduler = ownsScheduler;
    this.scheduler = ownsScheduler
      ? new SafeScheduledExecutorService(1, new ZetaThreadFactory("zeta-buffered-counter-flusher"))
      : scheduler;
  }

  /**
   * Record one or more accesses for the given key.
   *
   * <p><b>Fast path (no swap in progress):</b> hash → active buffer → add.
   * Every 64th call checks {@code size()} and triggers an eager swap if
   * the buffer is near capacity.
   *
   * <p><b>Spill path (swap in progress):</b> take the slot ticket, add to
   * the spill, release the ticket.  The ticket lets a reclaiming thread
   * prove no writer is in flight before its final drain; the writer
   * re-checks the spill reference after taking the ticket and retries via
   * the active path if the spill was recycled in between — closing the
   * stale-reader window where an {@code add()} lands in a detached spill.
   *
   * @param key   the accessed key (must not be {@code null})
   * @param delta the number of accesses (must be positive)
   */
  @SuppressWarnings("all")
  public void count(String key, long delta) {
    if (shutdown) {
      return;
    }

    int idx = mixHash(key.hashCode()) & (ceilCount - 1);
    for (;;) {
      CounterBuffer spill = spillCeils[idx].get();
      if (spill == null) {
        break; // fast path
      }

      spillCeilReaders.incrementAndGet(idx);
      if (spillCeils[idx].get() != spill) {
        // recycled between the read and the ticket — retry the whole path
        spillCeilReaders.decrementAndGet(idx);
        continue;
      }

      spill.add(key, delta);
      spillCeilReaders.decrementAndGet(idx);
      return;
    }

    CounterBuffer seg = activeCeils[idx].get();
    seg.add(key, delta);

    // Sampled size check (1/64 calls) to decide if we need an eager swap
    if (seg.shouldCheckSize() && seg.size() >= eagerSwapThreshold) {
      trySwap(idx, seg);
      swapCounter.increment();
    }
  }

  /**
   * MurmurHash3 32-bit finalizer (avalanche).
   *
   * <p>Mixes {@link String#hashCode()} so that keys whose hashes cluster on
   * the low bits (e.g. short numeric suffixes) spread evenly across slots,
   * instead of collapsing the whole workload onto one slot.  Cost is a few
   * integer ops (~1-2 ns) on the hot path.
   *
   * <p>Note: this defends against <em>distribution</em> attacks (low-bit
   * clustering), not against deliberately equal hash codes — identical
   * {@code hashCode()} values still map identically, which is the same
   * defense level as {@code ConcurrentHashMap}'s own spread.
   *
   * @param h the raw {@code String.hashCode()} value
   * @return the avalanched hash
   */
  private static int mixHash(int h) {
    h ^= h >>> 16;
    h *= 0x85ebca6b;
    h ^= h >>> 13;
    h *= 0xc2b2ae35;
    h ^= h >>> 16;
    return h;
  }

  /**
   * Return an approximate count of distinct keys in the active buffer.
   *
   * @return number of distinct keys in the currently-active buffers
   */
  public long estimatedSizeOfKeysCount() {
    int count = 0;
    for (int i = 0; i < ceilCount; i++) {
      count += activeCeils[i].get().size();
    }
    return count;
  }

  /**
   * Drain all remaining counts from all buffers without calling the consumer.
   * After this call all buffers are empty and ready for reuse.
   */
  public void clear() {
    // Active buffers (live slots only — orphaned above ceilCount are empty)
    for (int i = 0; i < ceilCount; i++) {
      activeCeils[i].getAndSet(new CounterBuffer()).drain();
    }
    // Spill buffers (safety net: drain all 64 slots)
    for (int i = 0; i < MAXS_CEILS; i++) {
      CounterBuffer sp = spillCeils[i].getAndSet(null);
      if (sp != null) {
        sp.drain();
      }
    }
    // Flush queue
    CounterBuffer buf;
    while ((buf = flushQueue.poll()) != null) {
      buf.drain();
    }
  }

  /**
   * Atomically swap a saturated active buffer for a fresh one, using a
   * per-slot spill buffer so that concurrent callers redirect rather than
   * waste a CAS.
   *
   * <p><b>Protocol (5 phases):</b>
   * <ol>
   *   <li>{@code stale-check} — verify {@code buffer} is still the active
   *       reference (another thread may have already swapped).
   *   <li>{@code install-spill} — CAS the spill slot from {@code null} to a
   *       new empty buffer.  If this fails, another thread owns the swap.
   *   <li>{@code swap-active} — CAS the active slot from {@code buffer} to
   *       a fresh buffer.  If this fails, drain the spill into the winner's
   *       current active and return.
   *   <li>{@code enqueue-old} — offer the replaced active buffer to the
   *       flush queue for async consumption (dropped and counted if the
   *       bounded queue is full).
   *   <li>{@code drain-merge} — drain the spill into the new active buffer
   *       with a bounded number of passes.  If the retry limit is exceeded
   *       the spill is forwarded to the flush queue instead.  Otherwise,
   *       CAS-clear the spill reference; on success, a second drain catches
   *       any {@code add()} that raced during the window, and the spill is
   *       then forwarded to the flush queue as a safety net so that a still
   *       later {@code add()} is drained by the next flush cycle instead of
   *       being stranded in a detached buffer.
   * </ol>
   */
  private void trySwap(int idx, CounterBuffer buffer) {
    if (buffer != activeCeils[idx].get()) {
      return;
    }

    CounterBuffer spill = new CounterBuffer();
    if (!spillCeils[idx].compareAndSet(null, spill)) {
      return; // another thread owns the swap for this slot
    }

    CounterBuffer newBuf = new CounterBuffer();
    if (!activeCeils[idx].compareAndSet(buffer, newBuf)) {
      // Lost: someone else swapped active while we were setting up spill.
      // Merge spill into the winner's current active and clean up.  Writers
      // that already ticketed this spill must finish before the merge.
      waitSpillCeilReaders(idx);
      spill.drainInto(activeCeils[idx].get());
      spillCeils[idx].set(null);
      return;
    }

    enqueueOrDrop(buffer);

    if (!drainInTo(spill, newBuf)) {
      // drain failed to complete in a reasonable number of passes, so we leave the spill
      if (spillCeils[idx].compareAndSet(spill, null)) {
        //if CAS successfully cleared, we can safely offer the spill to flushQueue for async draining
        waitSpillCeilReaders(idx);
        enqueueOrDrop(spill);
      }
      return;
    }
    // CAS-clear spill to signal flusher this slot is done.
    // Only on success (we still own the spill) do we drain again to catch
    // any add() that raced in between the first drain and the CAS clear.
    // On failure the flusher has already taken ownership via getAndSet(null)
    // and will drain the buffer itself — we must not touch spill again.
    if (spillCeils[idx].compareAndSet(spill, null)) {
      // Ticket protocol: wait for in-flight spill writers to finish before
      // the final drain, proving no add() can land after it.  On timeout the
      // enqueueOrDrop fallback below still catches stragglers.
      waitSpillCeilReaders(idx);
      drainInTo(spill, newBuf);
      // Safety net: an add() that read the spill reference before the CAS
      // clear but lands after the final drain would be stranded in a detached
      // buffer.  Queueing the spill lets the flusher catch it next cycle
      // (an empty buffer is a no-op there).
      enqueueOrDrop(spill);
    }
  }

  /**
   * Bounded spin until no in-flight spill writer remains for the slot.
   *
   * <p>The spin is bounded so a reclaiming thread cannot be starved forever
   * by a continuous stream of spill writers.  On timeout the caller still
   * forwards the spill to the flush queue ({@link #enqueueOrDrop(CounterBuffer)}),
   * which the flusher drains on the next cycle — the ticket tightens the
   * window but the queue remains the ultimate fallback.
   *
   * @param idx the slot whose spill writers to wait for
   */
  private void waitSpillCeilReaders(int idx) {
    for (int spins = 0; spins < 200_000 && spillCeilReaders.get(idx) != 0; spins++) {
      Thread.onSpinWait();
    }
  }

  /**
   * Offer a batch to the bounded flush queue, dropping it (and counting the
   * drop) when the queue is full.
   *
   * <p>Dropping is the bounded-queue safety valve: a stalled consumer cannot
   * grow heap unboundedly.  The batch dropped is the <em>newest</em> one
   * (the {@code offer} that failed) — older batches already queued are
   * preserved and delivered first.  This is consistent with the library's
   * acceptable-loss philosophy (ADR-0013): bounded memory is guaranteed,
   * and the loss is observable via {@link #droppedFlushBatches()}.
   *
   * @param buf buffer awaiting async drain
   */
  private void enqueueOrDrop(CounterBuffer buf) {
    if (!flushQueue.offer(buf)) {
      flushDropCounter.increment();
    }
  }

  /**
   * Return the number of batches dropped because the flush queue was full.
   *
   * <p>A non-zero value indicates the downstream consumer is not keeping up;
   * those counts were lost (bounded-queue safety valve).  See
   * {@link #enqueueOrDrop(CounterBuffer)}.
   *
   * @return cumulative dropped batch count
   */
  public long droppedFlushBatches() {
    return flushDropCounter.sum();
  }

  /**
   * Repeatedly drain {@code source} into {@code target} using
   * {@link CounterBuffer#drainInto(CounterBuffer)}, stopping when the buffer
   * is empty or the retry limit is reached.
   *
   * <p>Multiple passes are necessary because {@link CounterBuffer#drainInto}
   * calls {@link LongAdder#sumThenReset()} — a concurrent {@code add()} that
   * arrives <em>after</em> the reset would be stranded in the next pass.
   *
   * <p>After 8 passes the winner yields the core instead of hot-spinning —
   * during a swap storm the spill is continuously refilled, and a bounded
   * backoff keeps the swap winner from burning CPU for little progress.
   *
   * @return {@code true} if the buffer was fully drained, {@code false} if
   *         the retry limit was exceeded (caller should fall back to the
   *         flush queue)
   */
  private boolean drainInTo(CounterBuffer source, CounterBuffer target) {
    int attempts = 0;
    while (source.drainInto(target)) {
      // loop until source has no non-zero counters,or reach the limit.
      if (++attempts >= ((MAXS_CEILS << 3) / ceilCount)) {
        return false;
      }
      if (attempts > 8) {
        Thread.yield(); // bounded backoff instead of hot-spinning
      }
    }
    return true;
  }

  /**
   * Periodic flush scheduled by {@link #afterPropertiesSet()}.
   *
   * <p><b>Order matters:</b>
   * <ol>
   *   <li>Drain all <b>spill</b> buffers first (safety net for swap races;
   *       see {@link #trySwap(int, CounterBuffer)} phase 5-6 window).
   *   <li>Drain the <b>flush queue</b> (old active buffers from prior swaps).
   *   <li>Sample swap events and <b>auto-tune ceilCount</b>.
   *   <li>Drain all <b>active</b> ceils (including orphaned slots after a shrink).
   * </ol>
   *
   * <p>All three sources are merged into a single snapshot delivered to the
   * consumer <b>once per cycle</b>.  Single-shot delivery keeps the consumer
   * call rate bounded (at most one call per flush interval, regardless of the
   * number of live slots) — a burst of per-buffer deliveries would otherwise
   * overflow downstream bounded queues (e.g. the reporter routing executor).
   *
   * <p>Steps 1 and 2 are repeated identically in {@link #destroy()} and
   * {@link #clear()} to ensure the same three sources are exhausted.
   */
  private void flushStandby() {
    try {
      Map<String, Long> merged = null;
      // Spills may still hold data if a swap completed between the last
      // drainInto call and the CAS clear.  getAndSet(null) atomically
      // claims the spill so no swap winner races us; new writers see null
      // and go to the active path, and we wait for already-ticketed writers.
      for (int i = 0; i < MAXS_CEILS; i++) {
        CounterBuffer sp = spillCeils[i].getAndSet(null);
        if (sp != null) {
          waitSpillCeilReaders(i);
          merged = mergeDrain(merged, sp);
        }
      }

      CounterBuffer buf;
      while ((buf = flushQueue.poll()) != null) {
        merged = mergeDrain(merged, buf);
      }

      int current = ceilCount;
      sampleWindows++;
      sampleHits += (int) swapCounter.sumThenReset();

      if (sampleWindows >= current) {
        double hitRatio = (double) sampleHits / current;
        if (hitRatio >= 0.75 && current < MAXS_CEILS) {
          ceilCount = current << 1; // expand: more than 75% of flushes had swaps
        } else if (hitRatio < 0.25 && current > 8) {
          ceilCount = current >> 1; // shrink: fewer than 25%
        }
        sampleWindows = 0;
        sampleHits = 0;
      }

      // Must iterate MAXS_CEILS, not ceilCount, because previously-active
      // indices above a shrunk ceilCount would be orphaned and leak memory.
      for (int i = 0; i < MAXS_CEILS; i++) {
        merged = mergeDrain(merged, activeCeils[i].getAndSet(new CounterBuffer()));
      }

      if (merged != null && !merged.isEmpty()) {
        batchConsumer.accept(merged);
      }
    } catch (Exception e) {
      log.error("Scheduled flushStandby failed", e);
    }
  }

  /**
   * Drain {@code buf} into the merged snapshot and return it.
   *
   * <p>The same key may legitimately appear in more than one drained buffer
   * (e.g. after a shrink re-hashes keys onto fewer slots), so values are
   * summed.
   *
   * @param merged current snapshot, or {@code null}
   * @param buf    buffer to drain (empty buffers are a no-op)
   * @return the merged snapshot, or {@code null} if no non-zero counts yet
   */
  private Map<String, Long> mergeDrain(Map<String, Long> merged, CounterBuffer buf) {
    Map<String, Long> snapshot = buf.drain();
    if (snapshot.isEmpty()) {
      return merged;
    }
    if (merged == null) {
      return snapshot;
    }
    snapshot.forEach((key, val) -> merged.merge(key, val, Long::sum));
    return merged;
  }

  /**
   * Drain a single buffer into the downstream consumer.
   * No-op if the buffer is empty.
   */
  private void drainBuffer(CounterBuffer buf) {
    if (buf.size() != 0) {
      Map<String, Long> snapshot = buf.drain();
      if (!snapshot.isEmpty()) {
        batchConsumer.accept(snapshot);
      }
    }
  }

  @Override
  public void afterPropertiesSet() {
    try {
      scheduler.scheduleAtFixedRate(this::flushStandby, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      log.error(
        "Failed to start BufferedCounter flush scheduler; buffered counts will not " +
          "be flushed to HeavyKeeper. Hot-key detection may be impaired.",
        e
      );
    }
  }

  /**
   * Final drain of all three buffer sources and scheduler shutdown.
   *
   * <p>The drain order mirrors {@link #flushStandby()}:
   * active ceils → spill ceils → flush queue.
   */
  @Override
  public void destroy() {
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

    // Active ceils (all 64, not just ceilCount — prevent leak from shrink orphans)
    // Spill ceils (safety net: drain any orphaned spill data)
    // shutdown=true already makes count() no-op, so ticketed writers are only
    // those already in flight; wait for them so no add() is stranded.
    for (int i = 0; i < MAXS_CEILS; i++) {
      drainBuffer(activeCeils[i].getAndSet(new CounterBuffer()));
      CounterBuffer sp = spillCeils[i].getAndSet(null);
      if (sp != null) {
        waitSpillCeilReaders(i);
        drainBuffer(sp);
      }
    }

    // Flush queue
    CounterBuffer buf;
    while ((buf = flushQueue.poll()) != null) {
      drainBuffer(buf);
    }
  }

  /**
   * Returns the ratio of the active buffer's current distinct-key count
   * to the total capacity across all live ceils.
   *
   * @return saturation ratio in the {@code [0, 1+)} range
   */
  public double activeBufferSaturation() {
    int totalSize = 0;
    for (int i = 0; i < ceilCount; i++) {
      totalSize += activeCeils[i].get().size();
    }
    return (double) totalSize / (ceilMaxCapacity * ceilCount);
  }

  /**
   * A simple key→counter wrapper around {@link ConcurrentHashMap}.
   *
   * <p>Not thread-safe in the general sense — instances are expected to be
   * written by {@link #add(String, long)} and then {@link #drain()}ed once by
   * a single thread (the swap winner or the flusher), after which the instance
   * is discarded.
   */
  private static class CounterBuffer {

    /** Key→counter storage.  Package-visible for spill-merge in the outer class. */
    private final ConcurrentHashMap<String, LongAdder> counters = new ConcurrentHashMap<>();

    /**
     * Loose sampling counter: incremented on every {@link #add(String, long)}.
     * Only {@link #shouldCheckSize()} reads it; the value itself is approximate
     * (wraps around at Long.MAX_VALUE harmlessly).
     *
     * <p>A {@link LongAdder} rather than an atomic integer: when a hot key
     * funnels every thread into this one buffer, a single CAS counter becomes
     * a cache-line hotspot (measured ~2x worse at 16 threads on one hot key);
     * the adder's per-thread cells stripe the RMW traffic instead.
     */
    private final LongAdder addCounter = new LongAdder();

    /** Record one or more accesses for the given key. */
    public void add(String key, long delta) {
      counters.computeIfAbsent(key, k -> new LongAdder()).add(delta);
      addCounter.increment();
    }

    /**
     * Drain all non-zero counters from this buffer directly into {@code target},
     * skipping the intermediate HashMap allocation.
     *
     * @return {@code true} if any non-zero values were transferred
     */
    boolean drainInto(CounterBuffer target) {
      boolean nonEmpty = false;

      for (var entry : counters.entrySet()) {
        long val = entry.getValue().sumThenReset();
        if (val > 0) {
          target.counters.computeIfAbsent(entry.getKey(), k -> new LongAdder()).add(val);
          nonEmpty = true;
        }
      }
      return nonEmpty;
    }

    /**
     * Returns {@code true} once every ~64 calls so that the hot-path caller
     * can skip the more expensive {@link #size()} check the other 63 times.
     */
    public boolean shouldCheckSize() {
      return (addCounter.sum() & 63) == 0;
    }

    /** Number of distinct keys held in this buffer.  Cost: O(NCPU) volatile reads. */
    public int size() {
      return counters.size();
    }

    /**
     * Atomically snapshot all non-zero counters and reset them to zero.
     *
     * <p>Keys remain in the map (and their LongAdders are reused) so that
     * concurrent {@code add()} calls do not race with structural removal.
     * The caller must discard this instance after calling {@code drain()}
     * — it is not reused in place.
     */
    public Map<String, Long> drain() {
      Map<String, Long> result = new HashMap<>(counters.size());
      if (counters.isEmpty()) {
        return result;
      }
      counters.forEach((key, adder) -> {
        long val = adder.sumThenReset();
        if (val > 0) {
          result.put(key, val);
        }
      });
      return result;
    }
  }
}
