# Snowflake-Generated Degraded dataVersion

The degraded `dataVersion` fallback (`VersionControllerImpl.fallbackVersion()`) was changed from `Long.MIN_VALUE + localCounter` to `Long.MIN_VALUE + SnowflakeIdGenerator.nextId()`, making degraded versions globally comparable across instances.

## summary

When Redis is unavailable, the `dataVersion` for cache writes falls back to a degraded mode. Previously each instance used a partitioned counter (`Long.MIN_VALUE + AtomicLong`) — versions from different instances were incomparable since counters were independent. By adding a Snowflake ID (41-bit timestamp + 10-bit node identity), degraded versions now carry a globally monotonic timestamp, enabling cross-instance ordering even in degraded mode. The `Long.MIN_VALUE` offset preserves the invariant that all degraded versions sort below any positive Redis INCR version.

## Motivation

### Previously: Partitioned Counters, Incomparable

When Redis was down, each instance produced versions like:

| Instance A | Instance B |
|---|---|
| `Long.MIN_VALUE + 1` | `Long.MIN_VALUE + 1` |
| `Long.MIN_VALUE + 2` | `Long.MIN_VALUE + 2` |

In `VersionGuard.shouldSkipForSync()` case 3 (both degraded), the comparison `existing >= incoming` worked correctly only for messages from the **same** instance. Cross-instance degraded comparisons were meaningless — `A:-9223372036854775807 >= B:-9223372036854775806` might be true or false depending solely on which instance incremented faster, not on which write happened first.

### After: Snowflake-Timestamped, Globally Comparable

Each degraded version now embeds a Snowflake ID whose timestamp component provides a global ordering:

| Instance A (14:32:01.000) | Instance B (14:32:01.005) |
|---|---|
| `Long.MIN_VALUE + 6912345678000` | `Long.MIN_VALUE + 6912345678005` |
| `Long.MIN_VALUE + 6912345678001` | `Long.MIN_VALUE + 6912345678006` |

Case 3 now correctly resolves A's later write vs B's earlier write, even across instances.

## Decision

Replace the `fallbackVersion()` body:

```java
// Before
long version = Long.MIN_VALUE + fallbackVersionCounter.incrementAndGet();

// After
long version = Long.MIN_VALUE + snowflakeIdGenerator.nextId();
```

The `SnowflakeIdGenerator` is already a shared Spring bean in `ZetaFacadeAutoConfiguration`, injected into `VersionControllerImpl` alongside the existing `Optional<StringRedisTemplate>` and `versionKeyTtlMinutes`.

### Invariant Preservation

- **Degraded < Normal:** `Long.MIN_VALUE + snowflakeId ∈ [-9.22e18, -1]` while Redis INCR returns positive values → degraded always sorts below normal. Case 4 (existing degraded, incoming normal) remains correct.
- **Monotonic within instance:** Snowflake IDs from the same generator increase monotonically (timestamp + sequence) → `existing >= incoming` within a single instance works as before.
- **Monotonic across instances:** Snowflake IDs from different instances with synchronized clocks produce the correct ordering; with skewed clocks, ordering follows the clock values (an existing constraint that applies equally to normal Redis INCR ordering within a shard).

## Considered Options

- **Negation (`-snowflakeId`):** Rejected because negating an increasing positive ID produces decreasing negative values, reversing the comparison direction in `shouldSkipForSync` case 3.
- **Keep `Long.MIN_VALUE + localCounter` (status quo):** Rejected because cross-instance degraded comparisons are meaningless. During a prolonged Redis outage affecting multiple instances, the inability to order cross-instance degraded versions could cause stale cache entries to survive broadcast comparisons.
- **Unix-millis timestamp directly (`-currentTimeMillis`):** Rejected because two writes within the same millisecond would collide, and clock rewinds would break monotonicity. Snowflake's 12-bit sequence and clock-rewind tolerance (5ms) handle both.

## Consequences

1. **Clock dependency introduced to degraded versioning.** Previously the fallback was pure counter-based (no clock). Now it requires `TimeSource` (cached clock, 5ms resolution). Clock skew between instances may produce counter-intuitive ordering, though this is no different from normal-mode Redis INCR ordering across shards.
2. **SnowflakeIdGenerator must be available wherever VersionControllerImpl is used.** Already satisfied since it's a Spring bean registered in the always-active `ZetaFacadeAutoConfiguration`.
3. **getDegradedVersionCount() still works.** The counter is retained as a metric-only field, incremented alongside the Snowflake ID generation.
4. **Backward compatibility.** The `CacheEntry.getDataVersion()` and `isVersionDegraded()` contract is unchanged. The `VersionGuard` comparison logic is untouched. Existing entries in the cache with old-format degraded versions will be replaced on the next write.
5. **No impact on `decisionVersion` or `epoch`.** These remain AtomicLong and Redis INCR respectively — the Snowflake change is scoped entirely to the degraded `dataVersion` path.
