# Dual Version Space: dataVersion vs decisionVersion

**Status:** Accepted (2026-06-10)

## Context

HotKey tracks two independent version counters: `dataVersion` for data mutations (writes, invalidations) and `decisionVersion` for Worker HOT/COOL decisions. Single-version designs (e.g. a single Redis INCR) would couple these concerns: a Writer that increments the global version would also invalidate a concurrent Worker decision, and vice versa.

The two concerns have different reliability requirements:
- **Data mutations** must survive Redis failover — if Redis is down, the version must degrade gracefully to a local counter.
- **Worker decisions** are emitted by a single process (the Worker) with a strict monotonic `AtomicLong` that never degrades.

## Decision

Maintain separate version spaces:

- **dataVersion** — `long`, persisted in Redis via `INCR <key>`. Falls back to a node-local counter (`Long.MIN_VALUE + counter`) when Redis is unavailable, with `isVersionDegraded=true` flag on the broadcast message. Peers apply a 4-case comparison to prevent degraded versions from overwriting normal ones.
- **decisionVersion** — `long`, maintained by the Worker as a process-local `AtomicLong`. Never degraded because the Worker never accesses Redis during decision emission. If the Worker restarts, the AtomicLong resets to 0. To handle this, `shouldSkipForWorker()` unconditionally accepts any incoming decision when the existing entry is degraded.

## Consequences

- **Positive:** Data mutations and Worker decisions never interfere. A high-throughput write storm cannot disrupt HOT/COOL lifecycles.
- **Positive:** Redis outages do not affect Worker decision delivery. The Worker can continue broadcasting HOT/COOL decisions while Redis is recovering.
- **Positive:** The 4-case comparison (`shouldSkipForSync`) is straightforward to reason about because the degraded flag is a boolean, not a vector clock.
- **Negative:** Two versions must be carried in every `CacheEntry` (24 bytes overhead per entry plus serialization cost).
- **Negative:** The number of version comparisons may confuse readers — each path must pick the correct version space. `VersionGuard` centralises this logic but adds a class to the codebase.
