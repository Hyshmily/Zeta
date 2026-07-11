# SingleFlight: Exception-Only Invalidate

Zeta's SingleFlight invalidates completed futures **only on exception** (catch block), not on success. This differs from a naive try-finally approach that would always remove the future after completion.

## Decision

```java
// Current design: catch-only invalidate
try {
    return Optional.ofNullable((T) future.join());  // success → keep future cached
} catch (CompletionException e) {
    inflightLoads.invalidate(cacheKey);              // exception only → remove for retry
    // ... rethrow
}
```

Key insight: the dedup cache (`Caffeine<String, CompletableFuture>`, `expireAfterWrite(5s)`) has a different lifecycle from the value cache (Caffeine L1). The dedup cache exists only to coalesce concurrent in-flight requests. Once the future completes normally, late-arriving callers should **reuse the completed result** until the TTL naturally expires — not trigger a redundant supplier execution.

**try-finally (always invalidate) — produces redundant executions:**

```
T1: load("K") → computeIfAbsent → creates F1, starts supplier
T2: load("K") → computeIfAbsent → returns F1 (still cached), joins and waits
T1: supplier finishes, join returns, finally → invalidate("K")
T2: join F1 returns successfully
T3: load("K") → computeIfAbsent → F1 is gone! Creates F2, re-runs supplier  ← REDUNDANT
```

**catch-only (current design) — normal results age out naturally:**

```
T1: load("K") → computeIfAbsent → creates F1, starts supplier
T1: join returns successfully → no invalidate
T2: load("K") → computeIfAbsent → F1 still cached, joins F1, reuses result
T3: load("K") → computeIfAbsent → F1 still cached, reuses
... until expireAfterWrite(5s) naturally evicts F1
```

Caffeine's `expireAfterWrite(ttlSec)` already manages the lifecycle of completed futures. catch-only lets successful results be reused within that TTL window, avoiding unnecessary supplier re-execution. try-finally would actively destroy the cache on every success path, forcing the next caller to rebuild — which defeats the purpose of deduplication.

Exception handling is different: a failed future must be evicted immediately so the next caller can retry. Without catch-only invalidation, a transient failure would poison the cache for the entire TTL window (5s).

## Consequences

- Under heavy load, the future's TTL window absorbs burst spikes: 1000 concurrent callers for key "K" at T=0 all share one supplier execution; callers at T=1s reuse the same completed result without re-execution.
- A fast-failing supplier (e.g. network timeout) is retried on the next caller instead of being stuck for 5s.
- The 80% capacity warning log (`estimatedInflightSize > maxSize * 0.8`) provides an early signal for abnormal dedup pile-up.
- Memory overhead: each completed future stays in the dedup cache for up to 5s (~200 bytes per entry), bounded by `inflightMaxSize` (default 50k).
