# Graceful Degradation: Local Fallback When All Workers Are Dead

When `ClusterHealthView.isClusterHealthy()` returns `false` (Worker cluster fails majority quorum: `alive < known / 2 + 1`), Zeta's `promoteLocalHotkeyIfNeeded` upgrades COOL→HOT with local TopK authority, `HotKeyReporter` continues accepting `record()` calls but the dispatcher backpressures, and Worker recovery overrides all local promotions via `decisionVersion` comparison within one broadcast cycle. This ensures the system never stalls waiting for Workers, recovery is automatic, and no consensus protocol or leader election is needed. Inconsistent HOT/COOL states across instances during the outage window are acceptable — without Workers there is no global authority anyway.

Note that the guard uses a **majority quorum** rather than "any alive" — a single surviving Worker does not prevent local COOL→HOT promotion, because that Worker alone cannot serve as a reliable global authority. The cluster is considered healthy only when at least half plus one Workers are responsive.

The quorum can be overridden by setting `minAliveWorkers` on `HealthViewImpl`. When set to a positive value, it replaces the `knownWorkerCount / 2 + 1` formula entirely. This is used for testing and for deployments with a fixed minimum availability requirement.
