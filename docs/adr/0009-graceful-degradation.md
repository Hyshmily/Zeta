# Graceful Degradation: Local Fallback When All Workers Are Dead

**Status:** Accepted (2026-06-10)

## Context

HotKey's architecture depends on a central Worker cluster for cluster-wide HOT/COOL decisions. When the Worker cluster is entirely unreachable (e.g. network partition, all Worker processes crashed), the App must continue serving cache requests. Without Workers, no new COOL→HOT promotions happen through the broadcast path, and existing COOL entries would eventually expire with their short TTLs.

The system must decide: should COOL entries become **promotable by local TopK** when no Workers are alive? If yes, what happens when Workers recover and broadcast their (potentially stale) decisions over local promotions?

## Decision

When `WorkerHealthMonitor.isAnyWorkerAlive()` returns `false`, the App enters **graceful degradation mode**:

1. **COOL entries become locally promotable** — `isWorkerManagedEntry()` returns `false` for COOL entries when no Workers are alive. This allows `promoteLocalHotkeyIfNeeded` to upgrade COOL→HOT with local TopK authority.
2. **Reports continue to be dropped** — `HotKeyReporter` continues to accept `record()` calls but the dispatcher queue eventually backpressures. When Workers recover, the next flush cycle delivers the accumulated counters.
3. **Worker recovery overrides local decisions** — When a Worker heartbeat arrives, `isAnyWorkerAlive()` returns to `true`. Any HOT decisions broadcast by the recovered Worker override local promotions via `shouldSkipForWorker` decisionVersion comparison (existing degraded entries unconditionally accept incoming decisions).

## Consequences

- **Positive:** The system never stalls waiting for a missing Worker. Cache performance degrades to local-only but remains operational.
- **Positive:** Recovery is automatic — no manual intervention needed when Workers come back.
- **Positive:** The `WorkerHealthMonitor` heartbeat timeout (5s) is the only failure detection mechanism. No consensus protocol, no leader election.
- **Negative:** During the outage window, different App instances may have inconsistent HOT/COOL states (each runs its own local TopK). This is acceptable because: (a) without Workers, there is no global authority anyway, and (b) Worker recovery overrides all local decisions within one broadcast cycle (~300ms).
- **Negative:** The combined "Worker all dead" vs "Worker partial dead" vs "Worker healthy" states are spread across three code paths (`promoteLocalHotkeyIfNeeded`, `HotKeyReporter.flush()`, `isWorkerManagedEntry`). A holistic test suite is needed to cover all transitions.
