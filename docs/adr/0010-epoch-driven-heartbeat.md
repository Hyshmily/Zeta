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

## 2026-07-27 Addendum: Transport Isolation Repair — `@Primary` Kept, Data Plane Explicitly Qualified

The original design called for dual-queue isolation "extended to the transport layer" (separate TCP connections for control-plane vs data-plane traffic). An audit found the isolation was never actually delivered: `zetaHeartbeatConnectionFactory` is annotated `@Primary`, so every unqualified `ConnectionFactory` injection point resolved to the heartbeat connection — including the data-plane `zetaReportRabbitTemplate`, `zetaSyncRabbitTemplate`, and the Worker's `reportListenerContainerFactory`. All traffic multiplexed over the "dedicated" control connection while Boot's `rabbitConnectionFactory` sat idle.

**Decision:** keep `@Primary` on `zetaHeartbeatConnectionFactory`, and qualify all Zeta-owned data-plane injection points explicitly for `rabbitConnectionFactory` (aligning with the pre-existing precedent in `workerListenerContainer` and `syncListenerContainer`).

Removing `@Primary` was considered and rejected: with two non-primary `ConnectionFactory` candidates, Spring Boot's `RabbitTemplate` (`@ConditionalOnSingleCandidate`) silently backs off, and downstream code injecting `RabbitTemplate` would then resolve to `@Primary zetaReportRabbitTemplate` — silently inheriting its JSON message converter for the consumer's own messages. `@Primary` on the heartbeat factory preserves single-candidate resolution for unqualified injections (no downstream breakage); explicit qualifiers route Zeta's own traffic correctly.

**Final channel mapping:**

| Plane | Connection factory | Traffic |
| ----- | ------------------ | ------- |
| Control | `zetaHeartbeatConnectionFactory` | App heartbeat consumption, verify PING/PONG, Worker heartbeat producer, Worker config gossip (incl. fast-lane rules gossip, ADR-0025) |
| Data | `rabbitConnectionFactory` (Boot) | Report publish/consume, cache-sync publish/consume, Worker decision consume, Worker HOT/COOL broadcast |

**Operational note:** `@Qualifier("rabbitConnectionFactory")` relies on Spring Boot's default bean name. If a consuming application defines its own `ConnectionFactory` bean (causing Boot to back off), these injection points fail fast at startup with an explicit `NoSuchBeanDefinitionException` — acceptable, since silent mis-routing is worse than a loud startup failure.
