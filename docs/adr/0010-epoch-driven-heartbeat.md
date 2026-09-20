# Epoch-Driven Heartbeat with Dual Queue Isolation

The old heartbeat used a passive PING on a shared Fanout exchange, with every App running its own independent timer — no Worker restart detection, no health metadata, and heartbeat/decision traffic competing on the same queue. Zeta's heartbeats now use a dedicated TopicExchange (`zeta.heartbeat.exchange`, routing key `heartbeat.{workerId}`, NONE ack, prefetch=100) fully isolated from HOT/COOL decisions (MANUAL ack, prefetch=5). Each Worker publishes a structured 8-field `WorkerHeartbeatMessage` (epoch, loadFactor, readyToServe, config parameters) every 1s via AMQP headers for zero-deserialization parsing. An earlier `decisionVersionHwm` field was removed: nothing consumed it, and the receiver-side ordering guard uses the per-message `decisionVersion` watermark instead.

## Epoch Initialization

The epoch is atomically incremented on Worker startup via a single Redis `INCR` command (replacing the previous non-atomic `GET`+`SET` read-modify-write). This guarantees that concurrent Workers with the same `workerId` always receive distinct epoch values, eliminating initialization races. Falls back to a local temp file (`%TEMP%/zeta-epoch-<workerId>`), then to `System.currentTimeMillis() * 1000 + random_jitter` as a last resort with minimal collision probability.

Apps detect restarts immediately: any decision from a higher epoch is unconditionally accepted (see VersionGuard rules below).

## Shared Epoch (Heartbeat + Broadcast)

The epoch is initialized once in `WorkerAutoConfiguration.workerEpochCounter()` and stored in a shared `AtomicLong` bean. Both `WorkerHeartbeatProducer` (heartbeat path) and `WorkerBroadcaster` (HOT/COOL broadcast path) derive their epoch from this single bean. This guarantees that `AMQP_HEADER_EPOCH` in decision messages is never 0 and always matches the heartbeat epoch.

## VersionGuard Decision Rules

`shouldSkipForWorker()` applies the following ordered rules:

1. **No existing entry** → accept
2. **Incoming epoch &gt; existing epoch** → accept unconditionally (Worker restart)
3. **Incoming epoch &lt; existing epoch** → skip (stale incarnation)
4. **Same epoch, same nodeId** → normal ordering via `decisionVersion`
5. **Same epoch, different nodeId** → accept unconditionally (last-writer-wins)

Rule 5 reflects that `decisionVersion` counters are local per Worker and not comparable across Workers. When two Workers share the same epoch (extremely rare — only via fallback paths), cross-Worker unconditional accept converges via the next heartbeat epoch. The App's Local TopK (ADR-0001) provides a safety net during the convergence window.

### Degraded Entry Handling (Removed from Worker Path)

A previous version of this ADR specified rule 2 ("existing entry degraded → accept unconditionally"). This rule was removed because it conflated the orthogonal `dataVersion` and `decisionVersion` version spaces (ADR-0008). The `isVersionDegraded` flag reflects whether the `dataVersion` was generated from a local counter during Redis outage — it has no bearing on `decisionVersion` ordering.

Rules 2–5 above already provide complete coverage for the scenarios the degraded check was meant to protect:

| Scenario | Guard |
| --------------------------------------------------- | ------------------------------ |
| Worker restarted, entry has stale decision metadata | Rule 2 (higher epoch → accept) |
| First Worker contact on a locally-written entry | Rule 5 (different/null nodeId → accept) |
| Same Worker, same incarnation | Rule 4 (`decisionVersion` comparison) |

The degraded flag is still used by `shouldSkipForSync()` (the `dataVersion` path) where its 4-case comparison matrix correctly distinguishes normal vs degraded data versions.

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

## 2026-09-17 Addendum: Control-Plane Exchange Pre-Declaration (Cold-Start `404`)

A cluster E2E run on a freshly created broker (`docker compose down -v`, so no exchanges existed) reproducibly logged two `ERROR`s from the **first** Worker to start, and none from the second:

```
ERROR o.s.a.r.c.CachingConnectionFactory - Shutdown Signal: channel error;
  protocol method: #method<channel.close>(reply-code=404,
  reply-text=NOT_FOUND - no exchange 'zeta.heartbeat.exchange' in vhost '/', class-id=60, method-id=40)
```

**Cause.** The heartbeat exchange was declared only by Spring Boot's `RabbitAdmin`, which declares lazily when a connection is created. The control-plane publishers (`WorkerHeartbeatProducer`, `FastLaneRulesBroadcaster`) open their own connection through `zetaHeartbeatConnectionFactory`, and nothing ordered the two. On an empty broker the first publish won the race, RabbitMQ closed the channel, and that tick's heartbeat plus fast-lane rule gossip were lost. Self-healing one interval later — which is exactly why it survived this long: only the first Worker of a cold cluster ever shows it.

**Decision.** Pre-declare the heartbeat exchange on the control-plane connection **before that connection is handed to a publisher** — `WorkerAutoConfiguration#heartbeatRabbitTemplate` runs an idempotent `exchangeDeclare` through the template's own `execute` while constructing the bean. Ordering then follows from dependency injection (no control-plane publish can precede the template instance that carries it) rather than from a timing assumption. A failure is logged at `WARN` and degrades to the previous behaviour, so an unreachable broker during context refresh cannot break startup. The earlier mitigation — delaying the first heartbeat by `pingIntervalMs` — is retained but is not what makes this correct.

**Rejected: registering a second `RabbitAdmin`.** This is the obvious fix and it silently destroys the cluster. Boot's admin is guarded by `@ConditionalOnMissingBean` **without attributes**, so the condition matches on the method's **return type**, `AmqpAdmin`:

```java
@Bean
@ConditionalOnSingleCandidate(ConnectionFactory.class)
@ConditionalOnBooleanProperty(name = "spring.rabbitmq.dynamic", matchIfMissing = true)
@ConditionalOnMissingBean                       // ← return type AmqpAdmin
public AmqpAdmin amqpAdmin(ConnectionFactory connectionFactory) { ... }
```

`RabbitAdmin implements AmqpAdmin`, so any admin Zeta registers would switch Boot's off and leave **every** report / broadcast / sync exchange, queue and binding undeclared — with no error pointing at the cause. Verified with `javap -v` against `spring-boot-autoconfigure:3.5.3`. Declaring from the publishing template avoids the question entirely.

**Verification.** Cold cluster (empty broker), `zeta-worker:1.1.57` rebuilt from this revision:

|                                 | Before | After |
| ------------------------------- | ------ | ----- |
| `404 NOT_FOUND` lines, worker-1 | 2      | **0** |
| `404 NOT_FOUND` lines, worker-2 | 0      | 0     |
| worker-1 startup `ERROR` count  | 2      | **0** |

Control plane confirmed intact: `rabbitmqctl list_exchanges` shows `zeta.heartbeat.exchange topic`; the App logs `Worker joined cluster: worker-1 / worker-2`; an end-to-end run still yields `state=HOT, decisionVersion=1, decisionNodeId=worker-1` on the App's L1 entry. The same cold-start race remains possible for the data-plane entities Boot's admin declares — documented as a remaining caveat under `zeta.worker-listener.*` in [CONFIG.md](../CONFIG.md).
