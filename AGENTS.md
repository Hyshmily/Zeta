# AGENTS.md

This file provides guidance to AI coding assistants (Claude Code, Copilot CLI, Gemini CLI, Cursor, etc.) when working with code in this repository.

**Skill system:** This project uses the **mattpocock/skills** system for specialized AI workflows. Skills are available at `$HOME/.agents/skills/`. The AI MUST load the relevant skill(s) before performing any substantive operation — planning, code modification, debugging, code review, refactoring, testing, or sub-agent dispatch. See Workflow Rules section for the per-operation skill loading matrix.

## Project Overview

HotKey is a Spring Boot Starter for automatic hot key detection and multi-level cache warming. It combines the HeavyKeeper algorithm (Count-Min Sketch variant) with Caffeine L1 caching, optional Redis L2 caching, and optional RabbitMQ broadcast for cross-instance synchronization.

**Coordinates:** `io.github.hyshmily:hotkey:1.1.3`

The project is split into 2 Maven modules under parent `hotkey-parent:1.1.3`:

- **`common/`** (`io.github.hyshmily:hotkey:1.1.3`) — Spring Boot Starter published to Maven Central; all cache logic, auto-configuration, algorithm, broadcast, report, entity types
- **`worker/`** (`io.github.hyshmily:hotkey-worker:1.1.3`) — standalone Spring Boot app (never published); cluster-wide hot key detection via RabbitMQ reports; parent `spring-boot-starter-parent:3.5.3`

## Build Commands

```bash
mvn clean package                    # Build all modules (JARs)
mvn clean package -P release         # Build with GPG signing for Maven Central
mvn javadoc:jar                      # Generate Javadoc JAR (common only)
mvn source:jar                       # Generate source JAR (common only)
```

**Worker module** is skipped from deployment — `pom.xml` sets `<maven.deploy.skip>true</maven.deploy.skip>`.

**Java version:** 25 (set in parent POM and maven-compiler-plugin)

**Code formatting:** Prettier with `prettier-plugin-java` and `@prettier/plugin-xml` (printWidth: 120)

## Architecture

### Core Layers

1. **`algorithm`** — Hot key detection engine (HeavyKeeper implementation of TopK interface)
2. **`hotkeycache`** — Cache orchestration (`HotKeyCache`, `SingleFlight`, `CacheExpireManager`, `TransactionSupport`, `CacheKeysPolicy`, `VersionGuard`, `InstanceIdGenerator`)
3. **`broadcast`** — RabbitMQ cross-instance sync (`CacheSyncPublisher`, `CacheSyncListener`, `SyncMessage`) + Worker listener (`WorkerListener`, `WorkerMessage`)
4. **`report`** — App-to-Worker reporting (`HotKeyReporter`, `ReportPublisher`, `ReportMessage`)
5. **`entity`** — Shared data types (`CacheEntry` with `dataVersion` + `isVersionDegraded` + `decisionVersion`, `KeyState`, `HotKeyDecision`)
6. **`detection`** — Worker-side state machine (`HotKeyStateMachine`)
7. **`annotation`** — `@HotKey` annotation with SpEL key, three operation types (READ/WRITE/INVALIDATE), per-annotation TTL overrides, `softExpire()` toggle (default true) to choose between `getWithSoftExpire` vs `get`
8. **`autoconfigure`** — All Spring Boot auto-configuration classes
9. **`actuator`** — `/actuator/hotkey` monitoring endpoint
10. **`constant`** — Shared constants (`HotKeyConstants`: AMQP headers, thread prefixes, routing keys)

Worker-only components live in the `worker/` module under `io.github.hyshmily.hotkey.worker`:

- `SlidingWindowDetector`, `TopKValidator`, `WorkerBroadcaster`, `ReportConsumer`, `WorkerAutoConfiguration`, `WorkerProperties`, `GlobalQpsEstimator`, `ThresholdLearner`, `WorkerApplication`

### Public API

`HotKey` is the sole public entry point (facade pattern). All cache operations go through this bean. `HotKeyCache` is internal.

Key methods: `get()`, `get(key, supplier, hardTtlMs, softTtlMs)`, `putThrough()`, `putThrough(key, value, runnable, hardTtlMs, softTtlMs)`, `putBeforeInvalidate()`, `invalidate()`, `invalidateAll()`, `isLocalHotKey()`, `isWorkerHotKey()`, `returnLocalHotKeys()`, `returnLocalExpelledHotKeys()`, `returnLocalTotalDataStreams()`, `returnWorkerHotKeys()`, `returnWorkerExpelledHotKeys()`, `returnWorkerTotalDataStreams()`, `getWithSoftExpire()`, `getWithSoftExpire(key, supplier, softTtlMs)`, `getWithSoftExpire(key, supplier, hardTtlMs, softTtlMs)`, `peek()`, `getLocalCache()` (raw Caffeine reference — ⚠️ bypasses orchestration; introspection only, never write through it), `addBlacklist()`, `removeBlacklist()`, `addWhitelist()`, `removeWhitelist()`, `getAllRules()`, `evaluateRule()`, `clearAllRules()`, `broadcastAllLocalRulesManually()`

`@HotKey` is the annotation-based entry point, handled by `HotKeyAspect` which injects the `HotKey` facade.

### Auto-Configuration

All in `autoconfigure/` package, registered in `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` (in `common/` module):

- `HotKeyFacadeAutoConfiguration` — always runs; creates `HotKey` facade via `ObjectProvider`
- `HotKeyAutoConfiguration` — runs unless Worker-only mode; creates TopK, Caffeine, SingleFlight, non-Redis HotKeyCache
- `HotKeyRedisAutoConfiguration` — activates when `RedisTemplate` bean present; `StringRedisTemplate` optional via `ObjectProvider`; version tracking
- `HotKeyActuatorAutoConfiguration` — activates when `Endpoint.class` on classpath; registers `/actuator/hotkey` diagnostic endpoint
- `HotKeyMicrometerAutoConfiguration` — activates when `MeterBinder.class` on classpath; registers Caffeine cache metrics (`hotkey.l1.*`) and custom HotKey business metrics (`hotkey.*`) as Micrometer gauges
- `HotKeyAmqpAutoConfiguration` — merged auto-config for all AMQP features (report, sync, worker-listener); gates on `@ConditionalOnClass(RabbitTemplate.class)` + inner `@ConditionalOnClass(RedisTemplate.class)` for sync/worker-listener, individual `@ConditionalOnProperty` gates
- `HotKeySchedulingConfiguration` — periodic decay + expelled drain; controlled via `hotkey.scheduling.enabled` + requires TopK bean
- `HotKeyAnnotationAutoConfiguration` — activates when `Aspect.class` on classpath AND `HotKey` bean present AND `hotkey.annotation.enabled=true`; registers `HotKeyAspect`

**Worker module** has its own `WorkerAutoConfiguration` (not in the shared imports file) — activated by `@SpringBootApplication` scan in `WorkerApplication.java` when `hotkey.worker.enabled=true`.

**Resolved:** `HotKeyRedisAutoConfiguration` now has `@AutoConfiguration(after = {HotKeyAutoConfiguration.class, RedisAutoConfiguration.class})` and its own `hotKeyCache` bean, fixing the ordering race.

### Data Flow

**Read path:** Caffeine L1 hit -> `hotKeyDetector.add(key,1)` + `hotKeyReporter.record(key)` -> return CacheEntry, OR L1 miss -> SingleFlight dedup -> async supplier (e.g., Redis) -> `hotKeyDetector.add(key,1)` + `hotKeyReporter.record(key)` -> cache as CacheEntry if hot

**Write path:** TransactionSupport deferral -> run mutation -> Redis INCR for dataVersion (VersionResult with degraded flag) -> update L1 (preserves existing decisionVersion) -> broadcast with dataVersion + degraded flag

**Broadcast reception:** CacheSyncListener uses 4-case degraded version comparison on `dataVersion` (normal-vs-normal, normal-vs-degraded, degraded-vs-normal, degraded-vs-degraded). WorkerListener uses compare on `decisionVersion` — if existing entry is degraded, incoming decision is unconditionally accepted; otherwise simple `>=` comparison. `decisionVersion` itself is always clean (never degraded).

**Report flow (App -> Worker):** Every `get()`/`getWithSoftExpire()` triggers `hotKeyReporter.record(key)` on both L1 hit and L2 miss paths -> `HotKeyReporter` aggregates locally (Caffeine, 30s, 100k max) -> `ReportPublisher` batches to RabbitMQ every `hotkey.local.report-interval-ms` -> Worker `ReportConsumer` feeds `workerTopK.add()` (separate TopK instance) -> Worker emits HOT/COOL decisions back to apps via `WorkerListener`

### Deployment Modes

- **App-only** (`hotkey.worker.enabled=false`, default): App runs local TopK + HotKeyCache + Reporter; Worker components absent
- **Worker-only** (`hotkey.worker.enabled=true`): Worker runs its own TopK, SlidingWindowDetector, Broadcaster; HotKey facade throws `UnsupportedOperationException` for cache methods; no L1/L2 cache

### Key Principle

Every read access (hit or miss, hot or not) triggers both `hotKeyDetector.add()` (local frequency tracking) and `hotKeyReporter.record()` (app-to-Worker reporting). `peek()` is the sole exception — it is intentionally side-effect-free (no add, no record, no L2 read).

## Key Patterns

- **Optional dependencies:** Redis, RabbitMQ, Actuator are `<optional>true</optional>`; auto-configuration uses `@ConditionalOnClass`/`@ConditionalOnBean`
- **`@ConditionalOnMissingBean`** on every bean — consumers can override any component
- **Supplier/Runnable callbacks:** Library decoupled from `RedisTemplate`; callers supply their own read/write lambdas
- **Config prefix split:** `hotkey.local.*` for app-side detection config (`HotKeyProperties`), `hotkey.worker.*` for worker config (`WorkerProperties`)
- **Java records** for immutable data carriers (`Item`, `AddResult`, `SyncMessage`, `ReportMessage`, `WorkerMessage`, `HotKeyDecision`)
- **Lombok** for boilerplate (`@Data`, `@RequiredArgsConstructor`, `@Slf4j`, `@Getter`)
- **Jakarta Validation** on `HotKeyProperties` fields
- **Git tag-based releases** — JitPack uses tags; Maven Central via `-P release` profile; both `hotkey-parent` and `hotkey` published; worker skipped via `<maven.deploy.skip>true</maven.deploy.skip>`
- **Two-version space design:** `dataVersion` (Redis INCR, may degrade to node-local counter) for cross-instance cache sync; `decisionVersion` (Worker `AtomicLong`, never degraded) for HOT/COOL decision ordering. Versions are orthogonal — data mutation does not affect `decisionVersion`, and Worker decision does not affect `dataVersion`.
- **Degraded version comparison:** Broadcast peers use 4-case comparison on `dataVersion` to prevent degraded versions from overwriting normal ones
- **`spring-amqp` is optional** in common module — app-only users don't pull AMQP; worker gets it via `spring-boot-starter-amqp`
- **`spring-boot-starter-aop` is optional** — only pulled when `@HotKey` annotation is used

## Code Style

- **Prefer Lombok annotations** over manual boilerplate:
  - `@Data` for POJOs with getters/setters/toString/equals/hashCode
  - `@Getter` / `@Setter` for selective field exposure
  - `@RequiredArgsConstructor` for constructor injection (use `@AllArgsConstructor` when all fields are `final`)
  - `@Slf4j` for logger injection
  - `@Builder` for complex object construction
  - Avoid writing manual getters, setters, constructors, toString, equals, hashCode when Lombok can generate them
- **Comments:**
  - Prefer Javadoc (`/** ... */`) on all public classes and methods
  - **Do NOT delete existing comments** — only add or improve them
  - **When modifying code, update the related comments accordingly** — if a method signature, behavior, or parameter changes, the comment must reflect the new state
  - All comments must be in English
  - Section dividers (e.g. `// ── Section Name ──`) are acceptable for organizing long classes

## Workflow Rules

1. **Skill usage per conversation:**
   - **Session start:** MUST load `grill-with-docs` at the beginning of EVERY session, regardless of the task. This is non-negotiable.
   - **Mandatory check:** Before any substantive operation (planning, code modification, debugging, code review, refactoring, testing, sub-agent dispatch, etc.), the AI MUST first examine the available skills located at $HOME/.agents/skills/ and load the relevant skill(s) before proceeding. When multiple skills apply, priority must be given to Matt Pocock's skills (i.e., those from the mattpocock/skills repository or any skill authored by Matt Pocock) over others, unless explicitly instructed otherwise. Consult each skill's description in its SKILL.md to determine applicability.
   - **Planning/discussion:** Load `brainstorming` + `grill-with-docs` + `improve-codebase-architecture` + `grill-me` + `zoom-out` before any planning session.
   - **Before code modification:** Load `code-review` + `grill-me`.
   - **Testing/debugging:** Load `diagnose` for bug investigation.
   - **Sub-agent dispatch:** Invoke `zoom-out` to provide broader context for sub-agent tasks.
   - **Infrastructure tasks:** Use relevant MCP tools (e.g., `mcp__mysql`, `mcp__redis`).
   - **Platform adaptation:** Non-Claude-Code AIs should substitute their platform's skill-loading mechanism (e.g., Copilot CLI `skill` tool, Gemini CLI `activate_skill` tool, Cursor rules) — the skill content is the same regardless of how it is loaded.
2. **Do not modify code lightly.** Only change code when the change does not affect HotKey's core functionality (HeavyKeeper algorithm, cache orchestration, broadcast sync, auto-configuration). If in doubt, discuss first.
3. **Re-read before change.** Before modifying code or discussing a plan, re-read all related source files. Evaluate from multiple angles: performance, memory footprint, high concurrency, distributed broadcast correctness, caller UX. Produce a structured review (suggestion, reason, impact, source location, modification approach) before proposing changes.
4. **Consider all cases before change.** Before any code modification, consider edge cases, race conditions, failure modes, backward compatibility. Verify with tests or reasoning that the change is safe. Reference rules 2 and 3.
5. **No git without asking.** Never use `git checkout`, `git restore`, `git reset`, or any destructive git commands. Ask the author first.

6. **Grill-with-docs before execution.** Before any code modification, planning session, debugging, or refactoring, load the `grill-with-docs` skill and cross-reference the planned change against `CONTEXT.md`, `docs/adr/`, `docs/ARCH.md`, `docs/CONFIG.md`, `README.md`, and `README.zh.md`. Resolve any contradiction between docs and code before proceeding. This ensures the plan is consistent with the established domain language and documented decisions.

7. **ADR on architectural decisions.** When a decision satisfies all three of (a) hard to reverse, (b) surprising without context, (c) result of a real trade-off, create an ADR in `docs/adr/`. Use the format from `$HOME/.agents/skills/grill-with-docs/ADR-FORMAT.md`. Number sequentially (`0001-`, `0002-`, ...). Don't batch — capture while the trade-off is fresh.

8. **CONTEXT.md on term clarification.** When `grill-with-docs` resolves a fuzzy domain term (`hot` vs `HOT`, `degraded` vs `normal`, etc.), update `CONTEXT.md` immediately with the canonical definition. Don't defer. If the conversation surfaces a new domain concept not yet in `CONTEXT.md`, add it on the spot.

9. **Bilingual doc sync.** Every EN doc in `docs/` (`ARCH.md`, `CONFIG.md`, `ANNOTATION.md`, `MONITOR.md`) plus `CONTEXT.md` has a `*.zh.md` counterpart. Both must be updated in the same change. The ZH file can be a translation or a re-expression — the domain semantics must match.

10. **Stress test every core change.** Any code change affecting `TopK`, `SingleFlight`, `HotKeyCache`, `VersionGuard`, `CacheSyncPublisher/Listener`, `WorkerListener`, `HotKeyStateMachine`, `CacheExpireManager`, or `HotKeyReporter` must pass `HotKeyStressTest` (31 scenarios). The stress test covers:

- **TopK**: no duplicate keys under 20-thread contention, bounded size ≤ K
- **SingleFlight**: extreme dedup (50 threads, 1 execution), no `Recursive update` race
- **HotKeyCache**: concurrent get/invalidate/peek consistency; logical expiry (B path, pure logical expiry)
- **CacheSyncPublisher**: concurrent dedup (20 threads, exactly 1 send), version ordering correctness
- **VersionGuard**: concurrent shouldSkipForSync/Worker under 10-thread load
- **HotKeyStateMachine**: independent keys + same-key concurrent evaluations
- **HotKeyReporter**: 10 threads × 50k high-frequency record
- **CacheSyncListener**: concurrent invalidate/refresh (10 threads × 100 ops each)
- **WorkerListener**: concurrent HOT/COOL decision race, final state sanity
- **Distributed**: 3 nodes, each 5 workers × 300 ops, simulated Redis + broadcast bus

Test pattern: `CountDownLatch` for synchronization, `AtomicInteger errors` for exception tracking, `CachedThreadPool`/`FixedThreadPool` for concurrency, `Optional` + `Supplier` for Cold/Hot access, `Runnable::run` executor to expose inline CompletionStage races. Assert `errors.get() == 0` and component-level invariants.

**Skip when:** Changes that do NOT touch any of the listed components (e.g., config metadata, Javadoc, non-functional formatting, build config, properties files) can skip the stress test. Use judgment — if the change affects data flow, state machine, or cache orchestration indirectly, run the tests.

11. **Doc sync on every code change.** After any code modification, cross-reference the change against all relevant documentation:
    - **`README.md` / `README.zh.md`** — check and update code samples, config examples, feature descriptions, usage instructions
    - **`CONTEXT.md`** — check and update domain term definitions
    - **`docs/`** — check `CONFIG.md`, `ARCH.md`, `ANNOTATION.md`, `MONITOR.md` and their `*.zh.md` counterparts for affected sections
    - **`docs/adr/`** — if the change invalidates or modifies a prior ADR's rationale, update or supersede it
    - The check is shallow but mandatory: skim each doc for references (property names, defaults, descriptions, diagrams, config blocks) related to the changed code. If none are found, no doc change is needed.
    - This rule complements rule 9 (bilingual sync) — rule 9 governs the language pairing, rule 11 governs the scope of doc traversal.

## Reference Implementation

Original hotkey project (v0.0.4) is available at `D:\hotkey-master-v0.0.4` for comparison and design reference.

## Domain Glossary & Architecture Decisions

See `CONTEXT.md` (project root) for the canonical domain glossary — it defines the precise meaning of **Local-Hot**, **Worker-Hot**, **Worker-Cool**, **Graceful Degradation**, and all versioning concepts (`dataVersion`, `decisionVersion`, `Degraded Version`).

Design decision records (ADRs) are in `docs/adr/`:

| ADR    | Subject                                                                                                                                                                                                                                                                                                                                                                                    |
| ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `0001` | **Local Promotion with Worker-Aware Fallback** — `promoteLocalHotkeyIfNeeded` upgrades NORMAL always, COOL only when all Workers are dead. Rationale: local App promotes faster than Worker broadcast (priority for local view); when Worker cluster is entirely dead, COOL entries don't stagnate — local TopK takes over. Worker re-override via `decisionVersion` ensures self-healing. |
| `0002` | **SingleFlight Exception-Only Invalidate** — removed `whenComplete` invalidate, only catch-block invalidates. Normal completions stay cached via Caffeine `expireAfterWrite`. Rationale: prevents T3 duplicate supplier execution from premature cleanup; follows Reactors/Hystrix standard dedup pattern.                                                                                 |
| `0003` | **State Machine Config via Heartbeat Gossip** — `HotKeyStateMachine` parameters sync via AMQP heartbeat broadcasts (not Redis/etcd). New Worker waits ≤3s for peer heartbeat. Rationale: avoid external config store dependency; rolling/k8s deployment never has all Workers down simultaneously.                                                                                         |
| `0004` | **Ack-Before-Update for AMQP Listeners** — `WorkerListener` and `CacheSyncListener` ack before cache write, trading at-most-once delivery for simpler logic and avoiding redelivery storms during jitter windows. Self-healing via periodic broadcast.                                                                                                                                     |
| `0005` | **32-bit Murmur3 for Consistent Hash Ring** — uses `murmur3_32_fixed()` instead of MD5/SHA-256. Collision probability <3% with 500 virtual nodes × 100 Workers; performance-critical hot path favors speed over theoretical collision resistance.                                                                                                                                          |

## Design Principles

### Local-First Promotion

The local App must always be able to promote a key to HOT faster than the Worker can broadcast. This ensures the local view of hot keys takes priority over centralized cluster consensus. Worker decisions only override local state when they arrive with a strictly newer `decisionVersion`.

### Graceful Degradation

When all Workers are dead (`RingManager.isAnyWorkerAlive() == false`), the system degrades gracefully:

- `isPromotableState()` returns true for COOL entries (normally only NORMAL is eligible)
- Local TopK assumes authority for drive L1 TTL decisions
- Reporter drops all batches silently (no AMQP target alive)
- On Worker recovery, broadcast decisions override local promotions via `decisionVersion >=` comparison — no explicit recovery protocol needed

### SingleFlight as Dedup, Not Cache

SingleFlight is a dedup mechanism for concurrent identical requests, not a cache. Completed futures stay in `inflightLoads` only for the TTL window (`inflightTtlSec`), and this TTL is intentionally short (5s default). This means SingleFlight only coalesces requests arriving within that window. Long-lived dedup is the job of Caffeine L1. This design limits memory growth from stale futures and prevents any single key's failure from blocking future requests indefinitely.

### Decentralized Config Gossip

Worker configuration (state machine parameters) propagates via existing heartbeat broadcasts rather than a dedicated config store. This works because the target deployment model (k8s rolling updates) never has all Workers down simultaneously. A single surviving Worker provides the latest config to newly started peers within 3 seconds. The trade-off: if all Workers restart simultaneously, config resets to `WorkerProperties` defaults — acceptable for the target deployment model but explicitly not suitable for bare-metal deployments where all Workers could go down together.

## Logging Rules

- **ERROR** (3): Only for genuine failures — broadcast processing crash, Redis mutation failure, async write failure
- **WARN** (4): Only for operational risks — executor rejection, Redis version fallback, singleflight timeout, async refresh failure
- **DEBUG** (25): Expected edge cases — invalid keys, dedup skips, missing broadcast, TTL expiry, soft expire fallback, non-transactional putThrough
- **INFO** (1): Only periodic summaries — expelled key drain (truncated to 20 keys)
- **Hot path rule:** Never log at WARN/INFO in per-request code paths. Use DEBUG or TRACE.
- **Loop rule:** Never log inside `forEach` loops — aggregate and log once after the loop
- **String rule:** Truncate unbounded `Collectors.joining()` output with `.limit(N)` + `...` suffix

## Annotation @HotKey Integration Tests

Located in `HotKeyAnnotationIntegrationIT` (Testcontainers, tagged `docker`):

| Test                                                     | Verifies                                                                                       |
| -------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `readOperation_shouldCacheViaGetWithSoftExpire`          | READ caches via `getWithSoftExpire`, invalidate forces reload                                  |
| `readOperation_withSoftExpireFalse_shouldUseGet`         | `softExpire=false` uses `get()` not `getWithSoftExpire`                                        |
| `writeOperation_shouldMutateAndInvalidate`               | WRITE + default TTLs, L1 cleared after mutation                                                |
| `invalidateOperation_shouldClearCacheAndProceed`         | INVALIDATE clears L1 then executes method                                                      |
| `readOperation_shouldUseSpelKey`                         | SpEL key resolves to correct cache key                                                         |
| `readOperation_differentSpelArgs_shouldUseDifferentKeys` | Different SpEL args → different keys                                                           |
| `writeOperation_withZeroTtl_shouldUseDefaults`           | WRITE silently ignores `hardTtlMs=0, softTtlMs=0` (no error)                                   |
| `writeOperation_withSoftExpireFalse_shouldIgnore`        | WRITE silently ignores `softExpire=false`                                                      |
| `invalidateOperation_withSoftExpireFalse_shouldIgnore`   | INVALIDATE silently ignores `softExpire=false`                                                 |
| `readOperation_withZeroTtl_shouldFallbackToDefaults`     | READ `hardTtlMs=0, softTtlMs=0` falls back to `HotKeyProperties` default TTLs, cache hit works |

**Known limitations:** `handleWrite` (line 122) ignores `HotKey.hardTtlMs()` and `HotKey.softExpire()`; `handleInvalidate` (line 150) ignores `HotKey.softExpire()`. These are by design — WRITE always uses `putBeforeInvalidate` (no TTL params), INVALIDATE always calls `invalidate(key)`.
