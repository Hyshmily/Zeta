# AGENTS.md

This file provides guidance to AI coding assistants when working with code in this repository.

**Skill system:** This project uses the **mattpocock/skills** system. Skills at `$HOME/.agents/skills/`. MUST load relevant skill(s) before any substantive operation. See Workflow Rules.

## Project Overview

HotKey is a Spring Boot Starter for automatic hot key detection and multi-level cache warming (HeavyKeeper + Caffeine L1 + optional Redis L2 + optional RabbitMQ broadcast).

**Coordinates:** `io.github.hyshmily:hotkey:1.1.4`

Two Maven modules under `hotkey-parent:1.1.4`:
- **`common/`** — Spring Boot Starter (published to Maven Central). Cache logic, algorithm, auto-configuration, sync, reporting, sharding, annotations.
- **`worker/`** — Standalone Spring Boot app (never published). Cluster-wide hot key detection via RabbitMQ.

## Build Commands

```bash
mvn clean package                    # Build all modules (JARs)
mvn clean package -P release         # Build with GPG signing for Maven Central
mvn test                             # Run all unit tests (1262 total: 1109 common + 153 worker)
mvn javadoc:jar                      # Generate Javadoc JAR (common only)
mvn source:jar                       # Generate source JAR (common only)
```

**Java version:** 21 | **Code formatting:** Prettier with `prettier-plugin-java` and `@prettier/plugin-xml` (printWidth: 120)

## Architecture

### Core Layers

| Layer | Package | Key Classes |
|---|---|---|
| `hotkeydetector/` | Detection engine | `HotKeyDetector` (facade, `implements TopK`), `HeavyKeeper` (TopK impl), `Item`, `AddResult` |
| `hotkeydetector/doublebuffer/` | Buffered counting | `BufferedCounter` (double-buffer flush to heap) |
| `cache/` | Cache orchestration | `HotKeyCache`, `SingleFlight`, `CacheExpireManager`, `TransactionSupport`, `CacheKeysPolicy` |
| `sync/` | Cross-instance sync | `CacheSyncPublisher`, `CacheSyncListener`, `SyncMessage`, `VersionGuard`, `VersionController` |
| `sync/` (broadcast) | Worker decision listener | `WorkerListener`, `WorkerMessage`, `WorkerListenerProperties` |
| `reporting/` | App→Worker reporting | `HotKeyReporter`, `ReportPublisher`, `ReportMessage`, `BbrRateLimiter` |
| `model/` | Data types | `CacheEntry` (with `dataVersion` + `isVersionDegraded` + `decisionVersion`), `KeyState`, `HotKeyDecision` |
| `detection/` | Worker state machine | `HotKeyStateMachine` |
| `sharding/` | Consistent hash ring | `ConsistentHashRing`, `RingManager` |
| `annotation/` | `@Cacheable` companions | `HotKeyCacheExtensionAspect`, `@HotKeyCacheTTL`, `@Intercept`, `@Fallback`, `@NullCaching` |
| `autoconfigure/` | Spring Boot auto-config | 8 auto-config classes |
| `endpoint/` | Actuator endpoints | `HotKeyEndpoint`, `RingEndpoint`, `StateMachineEndpoint` |
| `constants/` | Shared constants | `HotKeyConstants` (AMQP headers, thread prefixes, routing keys) |
| `rule/` | Rule management | `Rule`, `RuleMatcher` |
| `logging/` | Logging abstraction | `HotKeyLogger`, `DefaultLogger` |
| `exception/` | Custom exceptions | `HotKeyBlockedException` |
| `util/` | Utilities | `SystemLoadMonitor`, `SreRateLimiter`, `RollingWindow`, `DelayUtil`, `InstanceIdGenerator` |

Worker-only components (`worker/` module): `SlidingWindowDetector`, `TopKValidator`, `WorkerBroadcaster`, `ReportConsumer`, `WorkerConfigNegotiator`, `GlobalQpsEstimator`, `ThresholdLearner`, `WorkerProperties`, `TopKPersistService`, `VerifyConsumer`, `WorkerHeartbeatProducer`.

### Public API

`HotKey` is the sole public entry point (facade). `HotKeyCache` is internal.

- **Read:** `get()`, `getWithSoftExpire()`, `peek()` (side-effect-free)
- **Write:** `putThrough()`, `putBeforeInvalidate()`
- **Invalidate:** `invalidate()`, `invalidateAll()`
- **Introspection:** `isLocalHotKey()`, `isWorkerHotKey()`, `returnLocalHotKeys()`, `returnWorkerHotKeys()`, `returnLocalExpelledHotKeys()`, `returnWorkerExpelledHotKeys()`, `returnLocalTotalDataStreams()`, `returnWorkerTotalDataStreams()`, `estimatedSize()`, `stats()`, `getLocalCache()` (raw Caffeine — introspection only, never write through)
- **Rule management:** `addBlacklist()`, `removeBlacklist()`, `addWhitelist()`, `removeWhitelist()`, `getAllRules()`, `evaluateRule()`, `isBlacklisted()`, `isWhitelisted()`, `clearAllRules()`, `broadcastAllLocalRulesManually()`

`@Cacheable` + `@HotKeyCacheTTL` + `@Intercept` + `@Fallback` — annotation-based entry point via `HotKeyCacheExtensionAspect`.

### Auto-Configuration

Registered in `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` (common module):

| Configuration | Condition | Creates |
|---|---|---|
| `HotKeyFacadeAutoConfiguration` | Always | `HotKey` facade |
| `HotKeyAutoConfiguration` | !Worker-only | `HotKeyDetector`, Caffeine, SingleFlight, non-Redis HotKeyCache |
| `HotKeyRedisAutoConfiguration` | `RedisTemplate` present | Redis-backed HotKeyCache + version tracking |
| `HotKeyActuatorAutoConfiguration` | `Endpoint.class` on classpath | `/actuator/hotkey` + `/actuator/hotkeyring` + `/actuator/hotkey/worker/state` |
| `HotKeyMicrometerAutoConfiguration` | `MeterBinder.class` on classpath | Caffeine metrics + custom business gauges |
| `HotKeyAmqpAutoConfiguration` | `RabbitTemplate` present | Report, sync, worker-listener AMQP infrastructure |
| `HotKeySchedulingConfiguration` | HotKeyDetector bean present | Periodic decay + expelled drain |
| `HotKeySpringCacheAutoConfiguration` | `AbstractValueAdaptingCache` + `HotKey` bean + `hotkey.spring-cache.enabled=true` | `HotKeyCacheManager`, `HotKeyCacheExtensionAspect` |

Worker module has its own `WorkerAutoConfiguration` (activated by `@SpringBootApplication` scan when `hotkey.worker.enabled=true`).

### Data Flow

**Read path:** L1 hit → `hotKeyDetector.add(key)` + `hotKeyReporter.record(key)` → return CacheEntry. L1 miss → SingleFlight dedup → async supplier → same add+record → cache as CacheEntry if hot.

**Write path:** TransactionSupport deferral → run mutation → Redis INCR for `dataVersion` → update L1 (preserves `decisionVersion`) → broadcast with `dataVersion` + degraded flag.

**Broadcast reception:** `CacheSyncListener` uses 4-case degraded version comparison on `dataVersion`. `WorkerListener` compares on `decisionVersion` (degraded → unconditional accept; else `>=`).

**Report flow (App→Worker):** Every `get()`/`getWithSoftExpire()` → `HotKeyReporter.record(key)` → local aggregation (Caffeine, 30s, 100k max) → `ReportPublisher` batches to RabbitMQ → Worker `ReportConsumer` feeds `workerTopK` → Worker emits HOT/COOL decisions back via `WorkerListener`.

**Key principle:** Every read triggers both `hotKeyDetector.add()` + `hotKeyReporter.record()`. `peek()` is the sole side-effect-free exception.

### Deployment Modes

- **App-only** (`hotkey.worker.enabled=false`, default): Local TopK + HotKeyCache + Reporter.
- **Worker-only** (`hotkey.worker.enabled=true`): Own TopK, SlidingWindowDetector, Broadcaster. Cache methods throw `UnsupportedOperationException`.

## Key Patterns

- **Optional dependencies:** Redis, RabbitMQ, Actuator are `<optional>true</optional>`; `@ConditionalOnClass`/`@ConditionalOnBean` gating
- **`@ConditionalOnMissingBean`** on every bean — consumers override any component
- **Supplier/Runnable callbacks:** Library decoupled from `RedisTemplate`; callers supply their own read/write lambdas
- **Config split:** `hotkey.local.*` for app config, `hotkey.worker.*` for worker config
- **Java records** for immutable data carriers (`SyncMessage`, `ReportMessage`, `WorkerMessage`, `HotKeyDecision`)
- **Lombok** for boilerplate (`@Data`, `@RequiredArgsConstructor`, `@Slf4j`, `@Getter`, `@Builder`)
- **Dual version space:** `dataVersion` (Redis INCR, may degrade) for cache sync; `decisionVersion` (Worker `AtomicLong`, never degrades) for decision ordering. Orthogonal.

## Code Style

- **Prefer Lombok** over manual getters/setters/constructors
- **Comments:** Javadoc (`/** ... */`) on all public classes/methods. Never delete existing comments. Update comments when code changes. English only.
- **Section dividers** (`// ── Section Name ──`) acceptable in long classes

## Workflow Rules

1. **Skill loading (mandatory):** Load `grill-with-docs` at session start. Before any substantive operation, examine `$HOME/.agents/skills/` and load relevant skill(s). Matt Pocock's skills take priority. Per operation: planning → `brainstorming` + `grill-with-docs` + `improve-codebase-architecture` + `grill-me` + `zoom-out`; code mod → `code-review` + `grill-me`; debugging → `diagnose`; sub-agent → `zoom-out`.

2. **Evaluate before change.** Re-read all related source files before proposing changes. Consider: performance, memory, concurrency, distributed correctness, caller UX, edge cases, race conditions, failure modes, backward compatibility. Produce structured review (suggestion, reason, impact, source, approach) before modifying.

3. **No git without asking.** Never use destructive git commands (`checkout`, `restore`, `reset`). Ask first.

4. **Grill-with-docs before execution.** Cross-reference changes against `CONTEXT.md`, `docs/adr/`, `docs/CONFIG.md`, `README.md`, `README.zh.md`. Resolve contradictions before proceeding.

5. **ADR on architectural decisions.** Create ADR in `docs/adr/` when decision is (a) hard to reverse, (b) surprising, (c) a real trade-off. Use format from `$HOME/.agents/skills/grill-with-docs/ADR-FORMAT.md`. Number sequentially.

6. **CONTEXT.md on term clarification.** Update immediately when `grill-with-docs` resolves a fuzzy domain term. Add new domain concepts on the spot.

7. **Bilingual doc sync.** Every EN doc (`CONFIG.md`, `ANNOTATION.md`, `MONITOR.md`) has a `*.zh.md` counterpart. Both updated in same change. Semantics must match.

8. **Stress test every core change.** Changes affecting `TopK`, `SingleFlight`, `HotKeyCache`, `VersionGuard`, `CacheSyncPublisher/Listener`, `WorkerListener`, `HotKeyStateMachine`, `CacheExpireManager`, or `HotKeyReporter` must pass `HotKeyStressTest` (31 scenarios covering concurrency dedup, version races, distributed 3-node simulation). Skip only for non-functional changes (config metadata, Javadoc, formatting, build config).

9. **Doc sync on every code change.** After any code modification, skim `README.md`, `CONTEXT.md`, `docs/CONFIG.md`, `docs/ANNOTATION.md`, `docs/MONITOR.md`, and `docs/adr/` for affected references. Update if found. Complements rule 7.

## Domain Glossary & Architecture Decisions

See `CONTEXT.md` for canonical definitions of **Local-Hot**, **Worker-Hot**, **Worker-Cool**, **Graceful Degradation**, and all versioning concepts.

| ADR | Subject |
|---|---|
| `0001` | **Local Promotion with Worker-Aware Fallback** — `promoteLocalHotkeyIfNeeded` upgrades NORMAL always, COOL only when all Workers dead. |
| `0002` | **SingleFlight Exception-Only Invalidate** — catch-block only; prevents premature cleanup. |
| `0003` | **State Machine Config via Heartbeat Gossip** — AMQP heartbeat broadcasts, no config store. |
| `0004` | **Ack-Before-Update** — at-most-once delivery, self-healing via periodic broadcast. |
| `0005` | **32-bit Murmur3 for Consistent Hash Ring** — `murmur3_32_fixed()`, <3% collision at 500 vnodes × 100 Workers. |
| `0006` | **Per-Stripe Lock in HeavyKeeper Fading** — per-stripe `synchronized` guards against torn `long` reads during counter decay, avoiding `AtomicLongArray` overhead. |
| `0013` | **Acceptable Race Fading** — transient inconsistencies from async broadcast are bounded by the next periodic cycle; no distributed consensus needed for state convergence. |
| `0007` | **No Publisher Confirms** — fire-and-forget broadcast; lost messages tolerated by next cycle. |
| `0008` | **Dual Version Space** — `dataVersion` (may degrade) for sync, `decisionVersion` (never degrades) for HOT/COOL. Orthogonal. |
| `0009` | **Graceful Degradation** — when all Workers dead, local TopK assumes authority; self-heals on Worker recovery. |
| `0010` | **Epoch-Driven Heartbeat** — dual-queue isolation for control vs report traffic, epoch-based identity. |
| `0011` | **BBR + CPU Fusion for Reporter Self-Protection** — CPU-based BBR rate limiting for reporter backpressure. |
| `0012` | **Versioned Rule Merge** — rule list shipped with version metadata for cross-instance consistency. |

## Design Principles

- **Local-First Promotion:** Local App promotes faster than Worker broadcasts. Worker only overrides via newer `decisionVersion`.
- **Graceful Degradation:** All Workers dead → COOL entries promotable, Reporter drops silently, local TopK drives L1 TTL. Worker recovery reasserts authority via `decisionVersion >=`.
- **SingleFlight as Dedup, Not Cache:** Short TTL (5s default). Long-lived dedup is Caffeine's job. Prevents memory growth from stale futures.
- **Decentralized Config Gossip:** State machine parameters propagate via heartbeat broadcasts (not config store). New Worker waits ≤3s for peer heartbeat.
- **Logging:** ERROR for genuine failures, WARN for operational risks, DEBUG for expected edge cases. Never WARN/INFO on hot path. Never log inside `forEach`. Truncate unbounded `Collectors.joining()` with `.limit(N)`.

## Annotation Integration (Spring Cache)

The annotation-based entry point works through Spring's standard `@Cacheable`/`@CachePut`/`@CacheEvict` with HotKey companion annotations (`@HotKeyCacheTTL`, `@Intercept`, `@Fallback`, `@NullCaching`). Managed by `HotKeyCacheExtensionAspect` at `@Order(HIGHEST_PRECEDENCE)`. Gated by `hotkey.spring-cache.enabled=true`. Full test matrix documented in `docs/ANNOTATION.md`.

## Reference Implementation

Original hotkey v0.0.4 at `D:\hotkey-master-v0.0.4` for design reference.
