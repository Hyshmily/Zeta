# Version Key TTL Wraparound Guard

ADR-0008 established dual version spaces (Redis INCR for normal, Snowflake-degraded for fallback). ADR-0019 replaced the degraded counter with Snowflake IDs. Neither addressed the invariant that the Redis version key's TTL must exceed the maximum lifetime of any L1 entry for the same key. When a version key expires and the next INCR restarts from 1, the new version is numerically lower than the version cached on peer instances, causing `VersionGuard.shouldSkipForSync` to silently reject writes.

**Decision:** Three-layer defense:

1. **Default TTL extended** from 60 minutes to 10080 (7 days), far exceeding any realistic L1 entry lifetime.
2. **Per-key version floor cache** (Caffeine, 10k max) in `VersionControllerImpl` that tracks the highest dataVersion seen per key and logs `ERROR` on wraparound detection.
3. **Class-level Javadoc** on `Zeta` documenting that `Long.MAX_VALUE` permanent entries can outlive the version key and must be used with awareness of this constraint.

**Alternatives considered:** Removing the EXPIRE from the Lua script entirely (version keys live forever) would prevent wraparound at the cost of unbounded Redis memory growth for abandoned keys. A periodic background sweeper adds complexity disproportionate to the risk given the 7-day TTL. Neither justifies the trade-off vs. the current approach.
