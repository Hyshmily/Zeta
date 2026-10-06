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
package io.github.hyshmily.zeta.annotation;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * Admission gate for the {@link Intercept} annotation — the stateful half of
 * the interception check (QPS token buckets, QPS block table, concurrent
 * thread counters), extracted from {@code CacheExtensionAspect} so the aspect
 * stays an annotation-resolution and policy-assembly surface.
 *
 * <p>
 * <b>Contract:</b> {@link #check} evaluates one invocation against the
 * configured {@link Intercept} rule and returns a {@link GateResult}; the
 * caller falls back on {@link GateResult#BLOCKED} and calls
 * {@link #release(String)} in a {@code finally} block exactly when the result
 * was {@link GateResult#PROCEED_WITH_RELEASE} (a concurrent-thread slot was
 * taken and must be returned when the invocation completes).
 *
 * <p>
 * <b>State ownership:</b> the three bookkeeping structures live here and only
 * here — per-key token buckets (idle-expiring), the absolute unblock-timestamp
 * block table, and the concurrent-thread counters — so the aspect cannot drift
 * from the release protocol.
 *
 * <p>
 * The FORCE and IS_LOCAL_HOT modes are stateless and handled inline in
 * {@link #check}: FORCE is a constant block, IS_LOCAL_HOT consults the
 * caller-supplied local-hot predicate (the local TopK membership probe). The
 * detector feed on a blocked call ("keep the intercepted key hot so it cannot
 * flap") stays with the aspect — it is a cache-layer side effect, not gate
 * state.
 *
 * <p>
 * Thread safety: all public methods are thread-safe. Bookkeeping caches are
 * Caffeine instances with a 5-minute idle/write expiry and a 100k size cap —
 * the cardinality is user-key-driven and the TTL bounds memory for one-time
 * burst keys; an active key refreshes its access on every intercepted request,
 * so the expiry never disturbs live limiting.
 */
@Internal
public final class InterceptGate {

  /** Expiry for the QPS bookkeeping caches — a quiet key's buckets/block entry age out. */
  private static final Duration QPS_BOOKKEEPING_EXPIRY = Duration.ofMinutes(5);

  /** Upper bound for the gate's bookkeeping caches (QPS buckets, QPS block table). */
  private static final int BOOKKEEPING_CACHE_MAX_SIZE = 100_000;

  /**
   * Token-bucket based QPS rate limiters, one per cache key. Entries idle for
   * 5 minutes expire (mirroring {@link #qpsBlockTable}'s TTL): the cardinality
   * is user-key-driven, and without a TTL a one-time burst of distinct keys
   * would pin up to {@code maximumSize} bucket instances for the process
   * lifetime — the size cap alone only trims on the boundary crossing. An
   * active key refreshes its access on every intercepted request, so the TTL
   * never disturbs live limiting; an idle key simply starts from a fresh
   * bucket, which is the correct state for a quiet key.
   */
  private final Cache<String, Bucket> qpsBuckets = Caffeine.newBuilder()
    .expireAfterAccess(QPS_BOOKKEEPING_EXPIRY)
    .maximumSize(BOOKKEEPING_CACHE_MAX_SIZE)
    .build();

  /**
   * QPS block table: cache key → absolute unblock timestamp (millis).
   * Used together with {@link Intercept#blockDurationMs()} to enforce a
   * mandatory cooling-off period after a QPS breach. The Caffeine TTL
   * (5 minutes) is a safety net — the actual block duration is driven by
   * the stored timestamp comparison.
   */
  private final Cache<String, Long> qpsBlockTable = Caffeine.newBuilder()
    .expireAfterWrite(QPS_BOOKKEEPING_EXPIRY)
    .maximumSize(BOOKKEEPING_CACHE_MAX_SIZE)
    .build();

  /**
   * Atomic counters for tracking concurrent thread usage per cache key.
   */
  private final ConcurrentHashMap<String, AtomicInteger> concurrentCounters = new ConcurrentHashMap<>();

  /**
   * Outcome of a single admission check.
   */
  public enum GateResult {
    /**
     * Proceed with the method; no gate state was taken.
     */
    PROCEED,
    /**
     * Proceed with the method; a concurrent-thread slot was taken — the caller
     * must invoke {@link #release(String)} for this key when the invocation
     * completes (normally or exceptionally).
     */
    PROCEED_WITH_RELEASE,
    /**
     * Fall back (the interception rule fired); no gate state is held.
     */
    BLOCKED,
  }

  /**
   * Evaluate one invocation against the {@link Intercept} rule.
   *
   * <p>
   * Mode semantics, faithful to the historical aspect behaviour:
   * <ul>
   * <li>{@link InterceptType#FORCE} — always {@code BLOCKED}.</li>
   * <li>{@link InterceptType#IS_LOCAL_HOT} — {@code BLOCKED} when the
   * {@code isLocalHot} predicate accepts the key.</li>
   * <li>{@link InterceptType#QPS} — layer 1 is the block table (fast reject
   * without consuming a token; an expired entry is evicted and the call falls
   * through to the bucket), layer 2 the token bucket; a first breach enters
   * the block table when {@code blockDurationMs} is configured. A
   * non-positive {@code threshold} disables the mode entirely.</li>
   * <li>{@link InterceptType#CONCURRENT_THREADS} — a per-key counter caps
   * concurrent executions of the annotated method; an over-limit caller is
   * immediately unwound (its tentative increment is rolled back) and blocked.
   * A non-positive {@code threshold} disables the mode entirely.</li>
   * </ul>
   *
   * @param prefixedKey the fully qualified cache key
   * @param intercept   the resolved {@code @Intercept} annotation
   * @param isLocalHot  the local TopK membership probe for
   *                    {@link InterceptType#IS_LOCAL_HOT}
   * @return the admission outcome — {@link GateResult#PROCEED_WITH_RELEASE}
   *         when a concurrent-thread slot was taken and a later
   *         {@link #release(String)} is owed
   */
  public GateResult check(String prefixedKey, Intercept intercept, Predicate<String> isLocalHot) {
    return switch (intercept.type()) {
      case FORCE -> GateResult.BLOCKED;
      case IS_LOCAL_HOT -> isLocalHot.test(prefixedKey) ? GateResult.BLOCKED : GateResult.PROCEED;
      case QPS -> checkQps(prefixedKey, intercept);
      case CONCURRENT_THREADS -> checkConcurrent(prefixedKey, intercept);
    };
  }

  /**
   * Release the concurrent-thread slot taken by a
   * {@link GateResult#PROCEED_WITH_RELEASE} check. Safe to call at most once
   * per admitted invocation, in a {@code finally} block.
   *
   * @param prefixedKey the cache key whose counter to decrement
   */
  public void release(String prefixedKey) {
    concurrentCounters.computeIfPresent(prefixedKey, (k, v) -> {
      int after = v.decrementAndGet();
      return after == 0 ? null : v;
    });
  }

  /**
   * QPS admission: block-table fast reject, then the token bucket. A first
   * breach enters the block table when {@code blockDurationMs} is configured.
   * A non-positive threshold proceeds without consuming a token.
   */
  private GateResult checkQps(String prefixedKey, Intercept intercept) {
    int qpsThreshold = intercept.qps().threshold();
    long blockMs = intercept.qps().blockDurationMs();
    if (qpsThreshold <= 0) {
      return GateResult.PROCEED;
    }
    // Layer 1: block table — fast reject without consuming tokens
    if (blockMs > 0) {
      Long unblockTime = qpsBlockTable.getIfPresent(prefixedKey);
      if (unblockTime != null) {
        if (TimeSource.monotonicMillis() < unblockTime) {
          return GateResult.BLOCKED;
        }
        // Block expired — evict and fall through to tryConsume
        qpsBlockTable.invalidate(prefixedKey);
      }
    }
    // Layer 2: token bucket — normal rate limiting
    Bucket bucket = qpsBuckets.get(prefixedKey, k ->
      Bucket.builder()
        .addLimit(Bandwidth.builder().capacity(qpsThreshold).refillGreedy(qpsThreshold, Duration.ofSeconds(1)).build())
        .build()
    );
    if (!bucket.tryConsume(1)) {
      // First breach → enter block table if configured
      if (blockMs > 0) {
        qpsBlockTable.put(prefixedKey, TimeSource.monotonicMillis() + blockMs);
      }
      return GateResult.BLOCKED;
    }
    return GateResult.PROCEED;
  }

  /**
   * Concurrent-thread admission: take one slot and cap at the configured
   * threshold; an over-limit caller's tentative increment is rolled back
   * immediately and blocked. A non-positive threshold proceeds without taking
   * a slot.
   */
  private GateResult checkConcurrent(String prefixedKey, Intercept intercept) {
    int maxThreads = intercept.concurrent().threshold();
    if (maxThreads <= 0) {
      return GateResult.PROCEED;
    }
    AtomicInteger counter = concurrentCounters.computeIfAbsent(prefixedKey, k -> new AtomicInteger(0));
    if (counter.incrementAndGet() > maxThreads) {
      // Exceeded the limit; roll the tentative increment back and block.
      release(prefixedKey);
      return GateResult.BLOCKED;
    }
    return GateResult.PROCEED_WITH_RELEASE;
  }
}
