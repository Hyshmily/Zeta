# ADR-0028: Records-Based One-Third Cluster Health Threshold

The cluster health quorum previously depended on `knownWorkerCount` — a value initialized from the `zeta.local.expected-worker-count` config and intended to be updated dynamically from ring reconciliation. The dynamic path was never wired (dead code), so with the default config the threshold degenerated to "any single alive Worker = healthy" while the docs claimed majority quorum. The fixed config path worked but required operators to know the cluster size in advance.

We replace the majority formula with a **records-derived one-third threshold**: the health check is healthy when at least `ceil(observedWorkers / 3)` Workers are alive, floored at 1, where `observedWorkers` is the count of Workers ever seen via heartbeat and not yet confirmed dead by the verification pipeline (`HealthViewImpl.records`). The explicit `zeta.local.heartbeat.min-alive-workers` config overrides the derived threshold with an absolute alive count. `expected-worker-count` and the `setKnownWorkerCount` mechanism are deleted.

## Rationale

- **Honesty:** the old default behavior was "single survivor = healthy" by accident, while docs and ADR-0021 claimed majority quorum. The new default makes the actual behavior the documented contract.
- **Self-maintaining denominator:** `records` grows on heartbeat join and shrinks only when the verifier confirms death (`MAX_RETRY` consecutive PING failures), so the threshold never collapses on a transient network blip and requires no static cluster-size config.
- **Single survivor is acceptable:** Worker-side report traffic is compressed (LZ4, BBR-bounded batching), so one surviving Worker can serve the cluster; accepting it keeps the framework running instead of degrading on every partial failure. Degradation (local COOL→HOT takeover) triggers only when fewer than one third of observed Workers remain alive.
- **Safety floor:** `max(1, …)` prevents the vacuous "empty records → healthy" state that would otherwise lock COOL entries forever after all Workers are confirmed dead.

## Considered and rejected

- **Ring-reconcile callback wiring (方案 A):** the callback delivers the *alive* count, so using it as the denominator re-codifies "any alive = healthy"; it is also traffic-dependent (fires only on report flush) and RabbitMQ-gated.
- **Static `expected-worker-count` denominator kept as an option:** operators who need a fixed, stricter threshold can already use `min-alive-workers`; keeping two knobs for the same concept was rejected for simplicity.

## Consequences

- Behavior change for unconfigured deployments: a 3-Worker cluster with 2 dead is now "healthy" by explicit design (previously accidental); a 5-Worker cluster with 3 dead is also healthy (2 alive ≥ ceil(5/3)). Operators requiring stricter thresholds must set `min-alive-workers`.
- A startup WARN is logged when `min-alive-workers` is not configured, calling out the one-third semantics.
- Supersedes the majority-quorum formulas stated in ADR-0021 (its "single surviving Worker cannot serve as a reliable global authority" principle is explicitly overridden for this health gate).
