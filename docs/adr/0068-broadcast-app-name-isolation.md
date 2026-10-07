# Broadcast App-Name Isolation & Rebroadcast-Interval Bounds

A shared-broker MQ link review found that the decision-broadcast fanout gives no per-app isolation: the fanout exchange ignores the routing key (`send.<appName>` is decorative) and neither side's default exchange name contains the appName, so app A's HOT/COOL decisions reach app B's instances and trigger useless prewarming there. The same review found the rebroadcast interval — the self-healing upper bound for a lost HOT decision under ADR-0007's fire-and-forget semantics — was bounded below but not above.

## Status

accepted (2026-09-17). **This file was written into `docs/adr/` on 2026-09-29**, from the AGENTS.md index entry, `docs/CONFIG.md:244`, `README.md:468-470` and the shipped implementation: the decision was referenced by five documents but the file was never created (the same hygiene gap that left ADR-0079 out of the index until the same pass).

## Decision

- **Every HOT/COOL decision broadcast carries an `appName` header** (`ZetaConstants.Amqp.HEADER_APP_NAME = "appName"`), set by `WorkerBroadcaster` from the Worker's `zeta.worker.routing.app-name`.
- **The App-side `WorkerListener` drops decisions whose declared appName differs from its own** `zeta.local.app-name` (`WorkerListener.processWorker`). Two compatibility rules, both required for a rolling upgrade: a message **without** the header (pre-0068 Worker) is always processed, and a listener **without** a configured appName (legacy wiring) processes everything. A mismatch between the two `app-name` values silently discards decisions — visible at DEBUG only, so the two properties must be kept identical.
- **The same header and the same three rules apply to the sync plane.** `CacheSyncPublisher` stamps it and `CacheSyncListener.isForeignApp` filters on it; the sync fanout has the same shape but a lower blast radius (a foreign INVALIDATE is a no-op for unrelated keys). *(Note: `docs/CONFIG.md:244`/`CONFIG.zh.md:235` still say the sync plane "is not filtered yet" — stale as of this writing.)*
- **`WorkerProperties.StateMachine.rebroadcastIntervalMs` gains `@Max(60_000)`** beside the pre-existing `@Min(1000)`, enforced fail-fast at properties-binding time (default unchanged at 10s). Below 1s the rebroadcast degenerates into a per-report broadcast storm for every continuously-hot key; above 60s a lost HOT can leave new instances un-prewarmed for minutes, past which a milliseconds-vs-seconds mix-up is the overwhelmingly likely cause. The bounds rationale is recorded in the ADR-0024 addendum.

## Considered Options

- **Receiver-side header filtering as the remedy for the fanout's blindness to routing keys** — the pre-existing javadoc suggested it, but it was unimplementable: broadcasts carried no appName header to filter on. Stamping the header at the sender is the precondition, which is why both halves ship together.
- **Per-app exchange names** — rejected: it does not fix the rolling-upgrade window (a peer still bound to the shared exchange keeps receiving foreign decisions) and pushes a naming convention onto every operator.

## Consequences

1. Messages without the header are processed unconditionally — the rolling-upgrade window (old Worker, new App or vice versa) degrades to the old receive-everything behavior rather than dropping decisions.
2. The isolation is silent when misconfigured: a worker/app `app-name` mismatch discards every decision, observable only at DEBUG. Operators must keep `zeta.worker.routing.app-name` equal to `zeta.local.app-name`.
3. Header-less senders keep working, so the header is an additive wire change with no ADR-0074-style format versioning.

## 2026-10-05 note — predicate extraction (no semantic change)

The two per-listener copies of the foreign-app check (`WorkerListener.processWorker`
inline, `CacheSyncListener.isForeignApp`) were extracted into the shared
`sync.AppIsolationFilter` (`isForeign` + drop counter + last-sender + log gate;
both listeners keep delegating `foreignAppDrops()` / `lastForeignApp()` so the
Actuator surface is unchanged). The three rules, the compatibility contract, and
all log wording are byte-identical — the extraction only removes the divergence
the two copies had already developed.
