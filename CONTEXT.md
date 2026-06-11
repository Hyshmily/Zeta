# HotKey Domain Glossary

## Core Concepts

- **Hot Key** — A cache key accessed with abnormally high frequency. Ambiguous term; use **Local-Hot** (algorithm-detected) or **Worker-Hot** (`KeyState.HOT`, Worker-broadcasted) when precision matters.
- **Local-Hot** — A key whose frequency exceeds the HeavyKeeper TopK threshold on the local App instance. Triggers local L1 promotion but may differ from the cluster-wide view.
- **Worker-Hot** — A key broadcast by the Worker as HOT (`KeyState.HOT`) via AMQP. Carries a `decisionVersion` for ordering. Grants the longest L1 TTLs (1h default).
- **Worker-Cool** — A key broadcast by the Worker as COOL (`KeyState.COOL`). Signals the key is cooling down after a HOT period. Preserves original normal TTLs.

## Cache States

- **KeyState** — Enum: `NORMAL` / `HOT` / `PRE_COOL` / `COOL`. `PRE_COOL` is a transient state in the Worker state machine cooling sequence (`HOT → PRE_COOL → COOL → NORMAL`).
- **NORMAL** — Default state. No Worker involvement. Local TopK can freely promote to HOT.
- **HOT** — Worker-broadcasted hot decision OR local promotion result. Longest TTLs (hotHardTtl / hotSoftTtl). Never degraded or overwritten by local decisions.
- **COOL** — Worker-broadcasted cool decision. Preserved when at least one Worker is alive. Eligible for local promotion to HOT only when **all** Workers are dead (graceful degradation mode).

## Components

- **App** — The user's Spring Boot application that pulls `hotkey` as a dependency. Runs local L1 (Caffeine), optional L2 (Redis), local TopK, and the Reporter-to-Worker AMQP link.
- **Worker** — Standalone Spring Boot application (`hotkey-worker`). Consumes App reports via AMQP, runs its own TopK, emits HOT/COOL decisions back to Apps via broadcast. Published as a separate module, never deployed to Maven Central.
- **TopK** — Interface for the hot key detection algorithm. Implemented by HeavyKeeper (Count-Min Sketch variant). Tracks the `K` most frequent keys.
- **SingleFlight** — Deduplication mechanism that coalesces concurrent concurrent loads for the same key into a single execution. Prevents thundering herd.

## Versions

- **dataVersion** — Monotonically increasing counter (Redis INCR or node-local fallback). Orders data mutations across instances. May be **degraded** (local counter) when Redis is unavailable.
- **decisionVersion** — Monotonically increasing `AtomicLong` on the Worker. Orders HOT/COOL decisions. Never degraded. Independent of dataVersion.
- **Degraded Version** — A dataVersion produced by the node-local fallback counter (`Long.MIN_VALUE + counter`). Marked with `isVersionDegraded=true`. Never wins against a normal (Redis) version in broadcast comparisons.

## Lifecycle

- **Graceful Degradation** — When all Workers are detected as dead (`RingManager.isAnyWorkerAlive()` returns false), the App falls back to local TopK-driven TTL decisions: COOL entries become eligible for local promotion to HOT, and `isWorkerManagedEntry` checks return false. Restored automatically when a Worker heartbeat arrives.
- **Promote** — Upgrade a cache entry from NORMAL or COOL to HOT with longer TTLs. Triggered by local TopK (in `promoteLocalHotkeyIfNeeded`) or by Worker broadcast (in `handleHot`).
- **Expire** — Two-tier: **soft expire** (stale-while-revalidate, returns stale data + async refresh) and **hard expire** (absolute TTL, entry invalidated). Soft expire only applies to HOT and COOL entries.

## Operations

- **L1** — Caffeine cache, App-local. First lookup target.
- **L2** — Optional Redis cache (or any backend via the `Supplier<T>` reader). Async fallback on L1 miss.
- **Report** — Per-key frequency count sent from App to Worker via AMQP. Aggregated locally (Caffeine, 30s, 100k max) before batching.
- **Broadcast** — AMQP exchange `hotkey.broadcast.exchange` for cross-instance sync (cache values, HOT/COOL decisions) and Worker-to-App decision delivery.
- **@Intercept** — AOP annotation for read-side interception. Calls `hotKeyDetector.add()` (keeps TopK accurate) but skips `reporter.record()` (avoids flooding Worker). Does NOT trigger Worker report flow.
- **@HotKeyCacheTtl** — Per-key TTL override annotation for write operations. Replaces inline TTL parameters previously on `@HotKey`. READ TTLs remain application-config global.

## Failure Behavior

- **Worker All Dead** — All Worker shards have missed the heartbeat window (5s timeout). Activated threshold: `RingManager.isAnyWorkerAlive() == false`. Local TopK assumes authority: COOL entries become promotable, `isWorkerManagedEntry` returns false. Worker broadcast on recovery overrides local promotions via decisionVersion.
- **Worker Partial Dead** — Some shards alive, some not. Alive shards continue normally. Dead shard's keys are routed to other shards via consistent-hashing ring reconciliation.
- **Redis Degraded** — `nextVersion()` falls back to node-local counter (`Long.MIN_VALUE + counter`). Broadcast carries `isVersionDegraded=true`. Peers apply 4-case comparison to prevent degraded versions overwriting normal ones.
