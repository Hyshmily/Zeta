# Rule Sync — Version-Authoritative Full Replace

Status: accepted (2026-08-29; supersedes the merge-on-receive half of ADR-0012)

`syncRules` previously merged incoming broadcasts by pattern (union: incoming overwrites same-pattern, local-only preserved — ADR-0012) and re-persisted the merged set. That combination made rule deletions and `clearRules` structurally non-convergent: a peer holding the removed rule resurrected it in the union merge, then wrote the resurrected set back to Redis at a HIGHER version, and the deleting node was then re-converted by the peer's broadcast. Two version-bookkeeping defects made it worse: `loadRulesFromRedis` discarded the persisted `rulesVersion` (a restarted node's counter reset to 0, so its next CAS write was silently rejected and its broadcasts were skipped as stale), and `syncRules` persisted `max(local, incoming) + 1` while storing `max` in memory (counter and Redis wrapper diverged permanently after the first accepted sync, silently rejecting every subsequent local change's CAS write).

Decision: a broadcast carrying a strictly fresher version (`incomingVersion > local`) is **authoritative** — the local list is replaced wholesale and the incoming version is adopted exactly (`updateAndGet(max)`, no `+1`). `loadRulesFromRedis` restores the persisted `rulesVersion` into the local counter. A CAS rejection (script returns 0 / no set) is now observable: rate-limited WARN (10 s window, the ADR-0037 log convention), DEBUG otherwise. Legacy pre-versioning broadcasts (`incomingVersion == 0`, rolling upgrades) keep the union merge.

## Considered Options

- **Tombstones / diff-based deletes** (already rejected in ADR-0012 as overkill; still rejected): needs deletion state, a wire-format change, and an expiry policy to solve exactly the window the full replace closes for free.
- **Keep union merge, stop re-persisting after sync**: the peer keeps the resurrected rule in memory indefinitely, silently diverged from Redis — worse than the disease.
- **Global version authority** (Redis INCR per rule change): correct under concurrency, but adds a synchronous Redis round-trip to every rule mutation, against the fire-and-forget philosophy.

## Consequences

- Deletions and clears propagate to live peers; previously the only reliable channel was Redis + restart.
- A local rule absent from a fresher broadcast is dropped — its own broadcast was lost, and Redis persistence (both-write) plus `broadcastAllLocalRulesManually()` are the recovery paths. Concurrent equal-version changes from two nodes still resolve last-writer-wins via the CAS gate and the loser's change is dropped with a WARN — unchanged from before and bounded by ADR-0013 (acceptable race fading).
- The receiver's re-persist after a sync is normally a benign CAS rejection (Redis already holds the sender's version, now logged at DEBUG) and heals Redis when the sender's own persist failed.
