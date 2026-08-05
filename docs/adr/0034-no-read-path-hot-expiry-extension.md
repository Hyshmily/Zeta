# No Read-Path HOT Expiry Extension

The read path previously extended the hard/soft expiry of any HOT entry past 75% of its TTL by a full hot TTL, on every read. We removed it: HOT entries are now bounded by their hard TTL exactly like every other state, and renewal is the Worker's job.

## Context

`processLocalHotkeyIfNeeded` and the inline path in `computeInLock` both implemented the same rule: for an entry in `KeyState.HOT` with a finite `hardExpireAtMs`, if remaining TTL dropped below 25% of the entry's TTL, re-apply the full hot TTL. The condition was purely read-frequency-driven — it checked neither Worker liveness nor decision freshness, and had no upper bound.

## Problem

The extension converted an absolute eviction deadline into an unbounded rolling window:

- **Worker died after a HOT broadcast → infinite HOT.** The Worker cluster's periodic HOT rebroadcast (ADR-0024, 10 s) renews worker-HOT keys while the Worker lives. Once the Worker dies without a COOL, the rebroadcast stops but the local extension kept renewing the key forever as long as it was read at least once per TTL. COOL is never rebroadcast (ADR-0024), so even a single lost COOL in a healthy cluster produced the same unbounded extension.
- **Contradicted the graceful-degradation contract.** ADR-0021/0028 state that when the Worker cluster fails the health gate, local TopK drives L1 TTL. The extension froze Worker-HOT authority locally instead, defeating the "hard TTL bounds the lenient direction" argument that ADR-0024 relies on for lost COOL messages.
- **Undermined the hard-TTL freshness backstop.** Soft-expire background refresh keeps content fresh while the entry lives; hard TTL is the last-resort exit when the reader keeps failing. The extension disabled that exit, serving stale values indefinitely.

## Decision

**Remove the read-path HOT expiry extension entirely.** HOT entries are left untouched on hits:

- `processLocalHotkeyIfNeeded` and `computeInLock` now handle local promotion (NORMAL → HOT, COOL → HOT only when the health gate fails, ADR-0021) and nothing else.
- The entire `ExpireManager` extend API (`extendExpiry`, `extendHardExpiry`, `extendSoftExpiry`) was removed — it had no production callers outside the deleted mechanism. Promotion applies hot TTLs directly via `withTtlAndKeyState`; soft-expire refresh applies its own TTL via `applyRefreshTask`.
- Renewal of still-hot keys is covered by three existing mechanisms: Worker periodic HOT rebroadcast with TTL re-application (ADR-0024) while the Worker lives; local promotion on the next read when the key is still local-hot; and `processLoaded` rebuilding a reloaded entry directly as HOT when it is still in the local TopK. The cost is at most one Redis reload per hot key per hot TTL.

## Considered Options

- **Health-gated extension** — extend only while `HealthView.isClusterHealthy()`. Rejected: closes the all-Workers-dead case but not partial failure (a dead owner Worker below the ADR-0028 one-third gate) or a lost COOL in a healthy cluster; the unbounded window survives.
- **Decaying extension TTL** — extend with a geometrically halved TTL stored in the entry's own `hardTtlMs`, converging after ~2× the initial TTL. Rejected: field-free and bounded, but redefines `hardTtlMs` from "TTL" to "remaining budget" and adds subtle semantics for a mechanism whose healthy-cluster value is redundant.
- **Extension-count bit-packing in the TTL** — rejected: fragile, harms debuggability, and achieves nothing the decaying-TTL option does not do more cleanly.
- **`dataVersion`-gated extension** — rejected: `dataVersion` advances only on writes; a read-only hot key never triggers it, killing the mechanism for its primary audience.

## Consequences

1. HOT entries expire at their hard TTL exactly like NORMAL entries; a still-hot key pays one Redis reload per hot TTL and is rebuilt as HOT by `processLoaded`.
2. The unbounded-freshness failure mode (dead Worker + failed reader = stale forever) is eliminated.
3. `ZetaCacheTest` extension tests were inverted to pin the no-extension contract; the `CacheExpireManagerTest` extend-API tests were removed with the methods.
