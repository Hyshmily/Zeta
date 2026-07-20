# Local Promotion with Worker-Aware Fallback

`isPromotableState` governs local promotion: NORMAL entries upgrade to HOT on every L1 hit (both `get` and `getWithSoftExpire`), and COOL entries upgrade only when `HealthView.isClusterHealthy()` returns `false`.

## Decision

Two asymmetric promotion rules:

- **NORMAL → HOT unconditionally.** The local App always promotes hot keys faster than the Worker can broadcast. This does not race with Worker decisions because Workers never issue NORMAL — they emit only HOT or COOL. A local NORMAL→HOT promotion is a provisional speed-up; the Worker can still override it later via a higher `decisionVersion` broadcast.

- **COOL → HOT only when majority quorum fails** (`alive < known / 2 + 1`, or `alive == 0` when no Workers are configured). COOL means a Worker deliberately decided to cool this key down. While the Worker cluster is healthy (majority quorum satisfied), the local App defers to Worker authority. Only when the Worker cluster fails majority quorum (graceful degradation) does the local TopK assume authority and promote COOL entries. Worker recovery overrides all local promotions via `decisionVersion` comparison within one broadcast cycle.

Implementation in `HotKeyCache.java`:

```java
private boolean isPromotableState(CacheEntry cacheEntry) {
    KeyState state = cacheEntry.getKeyState();
    return state == KeyState.NORMAL || (state == KeyState.COOL && !healthView.isClusterHealthy());
}
```

## Consequences

- Local promotion never races with Worker decisions because the two version spaces are orthogonal (`dataVersion` for cache sync, `decisionVersion` for decision ordering).
- A COOL entry stays COOL for as long as the Worker cluster is healthy. This is intentional: the Worker's decision is authoritative.
- On cluster-wide Worker failure, the system degrades gracefully: local TopK drives L1 TTL, Reporter drops silently, and recovery is automatic once Workers come back.
- Latency overhead of the promotion path is one local TopK `contains()` check plus one atomic `compute()` on the Caffeine map — both sub-microsecond.
