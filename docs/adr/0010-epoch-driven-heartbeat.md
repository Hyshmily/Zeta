# Epoch-Driven Heartbeat with Dual Queue Isolation

The old heartbeat used a passive PING on a shared Fanout exchange, with every App running its own independent timer — no Worker restart detection, no health metadata, and heartbeat/decision traffic competing on the same queue. Zeta's heartbeats now use a dedicated TopicExchange (`zeta.heartbeat.exchange`, routing key `heartbeat.{workerId}`, NONE ack, prefetch=100) fully isolated from HOT/COOL decisions (MANUAL ack, prefetch=5). Each Worker publishes a structured 9-field `WorkerHeartbeatMessage` (epoch, decisionVersionHwm, loadFactor, readyToServe, config parameters) every 1s via AMQP headers for zero-deserialization parsing.

## Epoch Initialization

The epoch is atomically incremented on Worker startup via a single Redis `INCR` command (replacing the previous non-atomic `GET`+`SET` read-modify-write). This guarantees that concurrent Workers with the same `workerId` always receive distinct epoch values, eliminating initialization races. Falls back to a local temp file (`%TEMP%/zeta-epoch-<workerId>`), then to `System.currentTimeMillis() * 1000 + random_jitter` as a last resort with minimal collision probability.

Apps detect restarts immediately: any decision from a higher epoch is unconditionally accepted (see VersionGuard rules below).

## Shared Epoch (Heartbeat + Broadcast)

The epoch is initialized once in `WorkerAutoConfiguration.workerEpochCounter()` and stored in a shared `AtomicLong` bean. Both `WorkerHeartbeatProducer` (heartbeat path) and `WorkerBroadcaster` (HOT/COOL broadcast path) derive their epoch from this single bean. This guarantees that `AMQP_HEADER_EPOCH` in decision messages is never 0 and always matches the heartbeat epoch.

## VersionGuard Decision Rules

`shouldSkipForWorker()` applies the following ordered rules:

1. **No existing entry** → accept
2. **Existing entry degraded** → accept unconditionally (safety net)
3. **Incoming epoch &gt; existing epoch** → accept unconditionally (Worker restart)
4. **Incoming epoch &lt; existing epoch** → skip (stale incarnation)
5. **Same epoch, same nodeId** → normal ordering via `decisionVersion`
6. **Same epoch, different nodeId** → accept unconditionally (last-writer-wins)

Rule 6 reflects that `decisionVersion` counters are local per Worker and not comparable across Workers. When two Workers share the same epoch (extremely rare — only via fallback paths), cross-Worker unconditional accept converges via the next heartbeat epoch. The App's Local TopK (ADR-0001) provides a safety net during the convergence window.

## Other Details

`ClusterHealthView` uses majority quorum (`alive >= total/2 + 1`) for cluster health, `readyToServe=false` guards cold-start Workers, and on-demand verification via Direct reply-to probes only suspected Workers instead of polling. The config queue re-binds from broadcast exchange to heartbeat exchange (`heartbeat.*`), carrying configTimestamp for peer config gossip. This eliminates false-positive timeouts, provides immediate restart detection, and requires no external registry.
