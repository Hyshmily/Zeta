# Acceptable Race Fading: Bounded Inconsistency from Async Broadcast

HotKey accepts transient inconsistencies between instances because every state-oriented operation is broadcast periodically, and the next cycle naturally converges any missed updates. No distributed consensus, no idempotent processing, no message dedup state is required.

## Decision

All cross-instance communication falls into three categories, all of which tolerate loss:

1. **Cache sync (REFRESH/INVALIDATE):** Every application-level write publishes via `CacheSyncPublisher`. The caller's write is already applied locally, so a lost broadcast only delays the peer's view. The next write for the same key sends a fresh broadcast. There is no per-message retry, no publisher confirm, no inflight dedup across instances.

2. **Worker decisions (HOT/COOL):** Workers evaluate the sliding window periodically and broadcast decisions on every cycle. A lost HOT broadcast means the App stays at the current state for at most one evaluation window (~1s). `WorkerListener` acknowledges before updating (ADR-0004), accepting that a crash between ack and update costs one cycle.

3. **Rule synchronization:** Rules are gossiped via AMQP and persisted to Redis. Concurrent deletions may temporarily re-appear (ADR-0012). The next rule broadcast or periodic reconciliation fixes the inconsistency.

The inconsistency window is bounded by the shortest periodic interval among the affected paths: 1s (Worker heartbeat/evaluation), configurable `reportIntervalMs` (default 100ms for Reporter), or the next write transaction for sync.

We explicitly choose **not** to provide:
- Message-level idempotency (no dedup hash)
- Distributed locking (no leader election, no lease)
- Exactly-once delivery (at-most-once for sync/decisions, fire-and-forget for reports)

## Considered Options

- **Distributed consensus (Raft/Paxos):** Correct but drastically increases complexity for a hot-key cache system where convergence speed is measured in seconds, not milliseconds. The expected cost (deployment ops, latency, partition handling) exceeds the value.
- **Idempotent message processing with dedup window:** Each instance would track processed message IDs with TTL. Adds memory overhead, complex GC, and still cannot guarantee exactly-once without consensus.
- **Current approach (acceptance):** Simple, minimal overhead, proven sufficient. The K8s/rolling-update deployment model means all instances are rarely down simultaneously, and the bounded inconsistency window (<1s in practice) is acceptable for a cache warming system.

## Consequences

- A process-crash between ack and update causes a one-cycle delay in cache sync or decision application — never permanent divergence.
- Under extreme network partition (>broadcast interval), some instances may serve stale data. The next heatbeat from a live Worker re-converges all instances.
- No need for persistent message store or replay infrastructure in the library.
- The acceptable window is configurable via `hotkey.worker-listener.jitter-max-ms`, `hotkey.sync.dedup-window-sec`, etc.
