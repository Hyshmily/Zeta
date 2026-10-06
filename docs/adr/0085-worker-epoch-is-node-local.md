# Worker Epoch and Decision Version Are Node-Local

`VersionGuard.shouldSkipForWorker` compared `epoch` — and, at equal epoch,
`decisionVersion` — between an incoming Worker broadcast and the entry already
in the cache, **without first checking that both came from the same Worker**.
Both quantities are per-Worker: `epoch` is a restart counter seeded from that
Worker's own wall clock at boot, and `decisionVersion` is a per-Worker counter
starting at 0. Neither has any defined relationship to the same field on another
Worker, so the comparison was not merely imprecise — it was meaningless.

The failure mode was silent and permanent. During a rolling restart, the freshly
restarted Worker boots with a higher epoch and stamps it onto the entries it
broadcasts. Every later decision from a surviving, older-epoch Worker then failed
the `incomingEpoch < existingEpoch` test and was dropped — not for a bounded
reordering window, but for the **entire remaining TTL of the entry**. A rolling
restart therefore gradually muted every Worker that had not restarted, and the
entry kept serving a decision from an incarnation that no longer owned the key.
Nothing logged the drop: the entry looked healthy and up to date.

We decided to scope both comparisons to messages whose `nodeId` equals the
entry's `decisionNodeId`, evaluated **before** the epoch comparison: a
cross-Worker message is accepted on arrival, unconditionally, and ordering
between Workers is by arrival (last-writer-wins). Arrival order converges
because the owning Worker re-broadcasts its decision (ADR-0024), so a
momentarily out-of-order decision is corrected by the next broadcast instead of
poisoning the entry for its whole TTL. Within one Worker, epoch remains a
monotone incarnation counter and `decisionVersion` a single counter, so both
stay directly comparable and the restart-detection behaviour of ADR-0010 is
unchanged.

Rejected: comparing epochs only when both are non-zero, or comparing `nodeId`
tie-breaks by `decisionVersion` magnitude. Both keep a cross-Worker ordering
that does not exist, and both preserve the mute-the-survivor failure — they just
make the window less obvious. Rejected also: rejecting cross-Worker messages
outright to force a single owner. That would need an ownership lease the Worker
cluster does not have, and would drop legitimate decisions during any ownership
transfer.

One consequence worth recording: this is an observable behaviour change, not a
pure refactor. A lower-epoch message from a *different* Worker now overwrites the
entry where it previously lost. `DistributedSyncTest` carried exactly one
cross-`nodeId` epoch assertion (`higherEpoch_overridesLower`); it was retargeted
to the same-Worker regime it was written to describe, and a new case
(`crossWorkerLowerEpoch_appliesOnArrival`) pins the cross-Worker branch.
