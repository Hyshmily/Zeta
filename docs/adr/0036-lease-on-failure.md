# Lease-on-Failure: Background Refresh Failure Keeps the Stale Entry Alive

When a background soft-expire refresh fails (reader exception or 30s timeout), the entry previously continued untouched toward its hard TTL, then expired and forced every subsequent read into a synchronous reload attempt against the failing source — a stampede that defeats the point of stale-while-revalidate. We decided: on refresh failure, rewrite the existing entry in place with both expire timestamps extended to `max(remaining/2, 120s)`, so the stale value stays servable and the next soft-expiry read re-arms the refresh. Always on, no config knob.

## Status

accepted

## Context

`ExpireManagerImpl.createRefreshTask` runs the async refresh and, on failure, only logged WARN and returned. The entry (Worker-managed HOT/COOL — background refresh only exists for those states, see `HotKeyCache.refreshSoftExpire`) kept its original expire timestamps, died at the hard TTL, and the next read became a synchronous miss load. SingleFlight dedups _concurrent_ in-flight loads but cannot stop the next read from re-attempting, so a persistently failing source was hammered at read rate while every read got an empty/error result.

Soft-expire already serves stale data during the refresh window; the lease merely extends that window when the source is down. This is the same pattern as AutoLoadCache's `RefreshTask` (rewrite old value with `expire/2`), adjusted for Zeta's two-tier TTL model.

## Decision

**On background refresh failure (exception or timeout), rewrite the existing entry in place: keep the value and all metadata (dataVersion, decision stamps, keyState, and the `hardTtlMs`/`softTtlMs` duration fields — Option B), extend only the expire timestamps:**

```
leaseTtlMs = max(120_000, max(1, hardExpireAtMs - now) / 2)
newHardExpireAtMs = now + leaseTtlMs
newSoftExpireAtMs = now + leaseTtlMs / 2
```

Consequences by design:

- **Soft exponential decay.** Each failure halves the _remaining_ budget; repeated failures converge to the 120s floor. Repeated failure is graceful degradation, not entry clearing.
- **The retry window is the second half of every lease.** The soft timestamp is the lease midpoint, so the entry is soft-expired (but still hard-valid) during the second half — the next read in that window re-arms the refresh. Retries are read-triggered, matching Zeta's existing model (there is no periodic refresh scan). `soft == hard` would close the window entirely: the read path checks hard expiry before soft expiry (`invalidateIfIsLogicallyExpired` runs before `refreshSoftExpire`), so the entry would die at lease end without ever retrying. At the floor (120s) the window is 60s, comfortably above the 30s refresh timeout.
- **Duration fields stay authoritative.** `hardTtlMs`/`softTtlMs` remain "what the last authority granted" (Worker broadcast, promotion, demotion, write). The lease is a _provisional keep-alive_, not an authoritative state transition — authoritative transitions rewrite fields, provisional keep-alives only move timestamps. Caffeine's L1 expiry is timestamp-driven anyway (`ZetaAutoConfiguration` `Expiry` reads `hardExpireAtMs`), so the lease is mechanically native.
- **Guarded like `applyRefreshTask`.** The lease applies only when `dataVersion == snapshot` **and** `hardExpireAtMs == snapshot` (snapshot gains `hardExpireAtMs`): the same logical entry that failed the refresh. A write, broadcast, or promotion that landed in-flight already granted fresh TTLs — leasing it would shorten them. An evicted/absent entry is never recreated (the failure path carries no new value).
- **Permanent entries (`hardExpireAtMs == Long.MAX_VALUE`) are never leased** — nothing to extend, and the halving arithmetic would overflow. Their soft TTL already re-arms refresh on the next read.
- **Error types are indistinguishable.** The reader is a bare `Supplier<?>`; connection failures and definitive not-found errors lease identically. Accepted (same as AutoLoadCache); classifying would require an API change not worth making.
- **Residual race is benign.** A same-version HOT rebroadcast (ADR-0024, 10s cadence) that lands between snapshot and lease is skipped by the `hardExpireAtMs` identity check; a rebroadcast that lands _after_ the lease overwrites it with full HOT TTLs on the next cycle. COOL is never rebroadcast (ADR-0024), so COOL decay is the intended behavior. A leased orphaned-HOT entry whose Worker died is demoted on the next read by ADR-0035 — demotion rewrites fields, so the lease is immediately neutralized.

## Considered Options

- **Option A — halve the `hardTtlMs` duration field itself** (`lease = max(120s, hardTtlMs / 2)` via `withTtl`, fields and timestamps in sync). Rejected: the field is already a concurrent state variable rewritten by every authoritative transition; treating it as decay-budget storage would make the decayed value persist in introspection during an outage ("TTL 1.25m" is misleading; "TTL 5m, 2.25m remaining" is honest) and adds a second lifecycle semantics on top of Caffeine's timestamp-driven one. The concurrency risk is identical to Option B — both write inside the per-key atomic `compute` — so Option A buys nothing.
- **Constant lease (AutoLoadCache's literal `expire/2` on the configured TTL every time)** — rejected: no decay, contradicts the intended graceful-degradation profile.
- **Config knob (`zeta.local.refresh.lease-on-failure.enabled`, default off)** — rejected by decision: always on, no knob. Silent behavior change for existing deployments is accepted and documented here; constants `2` and `120_000` are compile-time, like `refreshTimeoutSeconds`.

## Consequences

1. **Hard TTL is no longer the absolute exit while the reader fails.** ADR-0034 eliminated read-frequency-driven unbounded renewal; this reintroduces _failure-driven_ unbounded renewal: a persistently failing source with continuing reads keeps the stale value alive at the 120s floor indefinitely. This is the intended availability-vs-freshness trade: the alternative is per-read synchronous attempts against a down source. When the source recovers, the next refresh succeeds (`applyRefreshTask` runs) and the entry returns to the normal lifecycle; staleness is self-healing and bounded by the lease only in per-lease terms, not in total lifetime.
2. **Contract language updated**: CONTEXT.md "Expire" gains the lease; CONFIG.md/README no longer imply hard TTL is an absolute bound under reader failure.
3. Failure WARN logs remain; the lease is logged at DEBUG.
