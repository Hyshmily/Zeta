# Worker Decision Broadcast Buffer (Async Send With Async Rollback)

`ReportConsumer.processReport` drained pending HOT/COOL decision broadcasts **synchronously on the AMQP consumer thread**, one `rabbitTemplate.send` per decision, with the failure rollback (`ZetaBayesianSM.rollbackToPreviousState`) applied inline from the send's return value. A mass-heat event — a cache-stampede onset, a flash-crowd burst, a bulk preload — can put thousands of keys into transition within one report batch; at that point each of the Worker's report consumers stalls behind thousands of serial AMQP publishes while its queue keeps filling (bounded only by the broker-side `x-max-length` / `x-message-ttl`, which then *drops reports* — the freshest statistical signal — to shed load). We decided to move decision sends onto a dedicated bounded single-threaded executor (drop-on-saturation), following the App-side ADR-0037 conventions, and to move the failure rollback from "inline after the failed send" to "on the drain thread after a failed send / on the caller thread after a saturation drop" — safe because the state machine's `mutationSeq` epoch already guards rollbacks against clobbering concurrently-advanced state.

## Status

accepted

## Context

The Worker's ingest plane and its broadcast plane share one thread per report consumer: evaluate → (on transition) send → rollback-on-failure, all inline. The report and broadcast *AMQP connections* are already isolated (ADR-0010 dual-queue control/data planes), but the worker-side *code path* was not. Three consequences:

1. **Head-of-line blocking under burst.** With `concurrentConsumers=8` and a batch of N transitioning keys, the consumer thread spends `N × sendLatency` inside `processReport`. Report consumption rate collapses exactly when traffic spikes — the moment detection latency matters most. Dropped head-of-line reports then starve the sliding windows of *other* keys, compounding the stall.
2. **The rollback's synchronous shape was the only reason sends were synchronous.** `processReport` needs the send's boolean to decide whether to roll the key's state back. The `mutationSeq` snapshot-rollback guard (in place since the parallel-consumer fix) makes a *deferred* rollback equally safe: it applies only while no later evaluation advanced the key, and the periodic HOT rebroadcast (ADR-0024) retries anything skipped. The synchronous dependency no longer buys anything.
3. **Precedent exists.** The App side solved the same shape in ADR-0037 (`BroadcastBuffer` flush isolation): dedicated bounded single-threaded executor, drop-on-saturation, one aggregated WARN per 10 s window. A lost decision fails lenient — HOT self-heals via ADR-0024 periodic rebroadcast, COOL via hard TTL + stale eviction (ADR-0007 / ADR-0024 semantics).

## Decision

- New `WorkerBroadcastBuffer` (worker module, `dispatch/`): a `ThreadPoolExecutor(1,1)` over an `ArrayBlockingQueue(capacity)`, `AbortPolicy` — bounded, single-threaded, drop-on-saturation. Per-key submission order is preserved (FIFO drain), which keeps same-key decision ordering even though the receiver's `decisionVersion >=` guard tolerates reordering anyway.
- `ReportConsumer` gains a nullable buffer (constructor overload; `null` keeps the legacy synchronous `processReport` — same convention as ADR-0037's `sendExecutor = null`). With a buffer present, each chunk's pending decisions are submitted as `Supplier<Boolean>` send tasks; a `false`/throw applies the rollback callback on the drain thread.
- **Saturation drop** (queue full or executor shut down): the decision is dropped *immediately* and its rollback runs on the **caller thread** (the AMQP consumer) — the rollback is lock-only (no I/O), so this is safe and gives the next evaluation the earliest possible retry. Dropped decisions count into the same aggregated WARN as send failures.
- Failure WARN aggregation: one WARN per 10 s window reporting the accumulated failure/drop count (ADR-0037 convention). The full-stack send-failure log stays at the send site in `WorkerBroadcaster` (unchanged).
- Wired by default (`zeta.worker.broadcast.buffer-enabled=true`, capacity `buffer-capacity=10000`); `@PreDestroy` drains queued decisions via `shutdown()`; submissions after shutdown are dropped with caller-thread rollback.
- `WorkerHeartbeatProducer` / `FastLaneRulesBroadcaster` / the eviction COOL callback stay synchronous: they are control-plane or 20-min-cadence paths where per-send latency is irrelevant and simplicity is worth more.

## Considered Options

- **Batch decisions into one fanout message (list of keys per message):** removes the per-key send cost without an executor, but is a wire-protocol change — every App's `WorkerListener` must learn a new message shape, and the rollback granularity becomes per-batch. Rejected for this iteration; the buffer achieves the ingestion-decoupling goal with zero App-side change.
- **Unbounded async queue:** converts broker backpressure into heap growth on the Worker under a RabbitMQ outage. Rejected — the bound and its drop semantics are the point.
- **Reuse common's `BroadcastBuffer`:** wrong shape — it merges per-key *versions* with deferred flush (last-writer-wins, 500 ms defer), while decisions are discrete ordered events needing per-item failure callbacks. Only the ADR-0037 *conventions* transfer; the implementation is worker-specific.
- **Keep sync sends, rely on broker-side report drops:** the status quo — drops reports under burst, i.e. loses detection signal to protect broadcast throughput. Rejected.

## Consequences

1. Decision broadcast latency gains one queue hop (typically sub-millisecond; bounded by the drain thread's throughput, ~tens of thousands of sends/s — far above steady-state decision rates).
2. A send failure now rolls the key's state back a few milliseconds later (drain thread) instead of inline; a saturation drop rolls back immediately on the consumer thread. Both preserve the retry contract: the next evaluation re-emits, or ADR-0024's periodic rebroadcast does.
3. Shutdown drains in-flight decisions once (`shutdown()`); a hard `shutdownNow` semantics is deliberately not used — a Worker stopping anyway fails lenient either way.
4. `zeta.worker.broadcast.buffer-enabled=false` restores the exact legacy behavior (synchronous drain, inline rollback) for deployments that need it.
