# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

HotKey is a Spring Boot Starter for automatic hot key detection and multi-level cache warming. It combines the HeavyKeeper algorithm (Count-Min Sketch variant) with Caffeine L1 caching, optional Redis L2 caching, and optional RabbitMQ broadcast for cross-instance synchronization.

**Coordinates:** `io.github.hyshmily:hotkey:1.1.1`

## Module Structure

The project is split into 2 Maven modules under parent `hotkey-parent:1.1.1`:

- **`common/`** (`io.github.hyshmily:hotkey:1.1.1`) — Spring Boot Starter published to Maven Central; all cache logic, auto-configuration, algorithm, broadcast, report, entity types
- **`worker/`** (`io.github.hyshmily:hotkey-worker:1.1.1`) — standalone Spring Boot app (never published); cluster-wide hot key detection via RabbitMQ reports; parent `spring-boot-starter-parent:3.5.3`

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
2. **`hotkeycache`** — Cache orchestration (`HotKeyCache`, `SingleFlight`, `CacheExpireManager`, `TransactionSupport`, `CacheKeysPolicy`, `VersionGuard`)
3. **`broadcast`** — RabbitMQ cross-instance sync (`CacheSyncPublisher`, `CacheSyncListener`, `SyncMessage`) + Worker listener (`WorkerListener`, `WorkerMessage`)
4. **`report`** — App-to-Worker reporting (`HotKeyReporter`, `ReportPublisher`, `ReportMessage`)
5. **`entity`** — Shared data types (`CacheEntry` with `dataVersion` + `isVersionDegraded` + `decisionVersion`, `KeyState`, `HotKeyDecision`)
6. **`detection`** — Worker-side state machine (`HotKeyStateMachine`)
7. **`annotation`** — `@HotCache` annotation with SpEL key, three operation types (READ/WRITE/INVALIDATE), per-annotation TTL overrides
8. **`autoconfigure`** — All Spring Boot auto-configuration classes
9. **`actuator`** — `/actuator/hotkey` monitoring endpoint

Worker-only components live in the `worker/` module under `io.github.hyshmily.hotkey.worker`:
- `SlidingWindowDetector`, `TopKValidator`, `WorkerBroadcaster`, `ReportConsumer`, `WorkerAutoConfiguration`, `WorkerProperties`

### Public API

`HotKey` is the sole public entry point (facade pattern). All cache operations go through this bean. `HotKeyCache` is internal.

Key methods: `get()`, `putThrough()`, `putBeforeInvalidate()`, `invalidate()`, `isHotKey()`, `returnHotKeys()`, `getWithSoftExpire()`, `peek()`

`@HotCache` is the annotation-based entry point, handled by `HotKeyAspect` which injects the `HotKey` facade.

### Auto-Configuration

All in `autoconfigure/` package, registered in `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` (in `common/` module):

- `HotKeyFacadeAutoConfiguration` — always runs; creates `HotKey` facade via `ObjectProvider`
- `HotKeyAutoConfiguration` — runs unless Worker-only mode; creates TopK, Caffeine, SingleFlight, non-Redis HotKeyCache
- `HotKeyRedisAutoConfiguration` — activates when `RedisTemplate` bean present; `StringRedisTemplate` optional via `ObjectProvider`; version tracking
- `HotKeyActuatorAutoConfiguration` — activates when `Endpoint.class` on classpath
- `HotKeySyncAutoConfiguration` — activates when `RabbitTemplate` + `RedisTemplate` present AND `hotkey.sync.enabled=true`
- `HotKeyWorkerListenerAutoConfiguration` — activates when `RabbitTemplate` + `RedisTemplate` present AND `hotkey.worker-listener.enabled=true`
- `HotKeyReportAutoConfiguration` — activates when `RabbitTemplate` present AND `hotkey.local.report.enabled=true` (default)
- `HotKeySchedulingConfiguration` — periodic decay + expelled drain; controlled via `hotkey.local.scheduling.enabled` + requires TopK bean
- `HotKeyAnnotationAutoConfiguration` — activates when `Aspect.class` on classpath AND `HotKey` bean present; registers `HotKeyAspect`

**Worker module** has its own `WorkerAutoConfiguration` (not in the shared imports file) — activated by `@SpringBootApplication` scan in `WorkerApplication.java` when `hotkey.worker.enabled=true`.

**Resolved:** `HotKeyRedisAutoConfiguration` now has `@AutoConfiguration(after = {HotKeyAutoConfiguration.class, RedisAutoConfiguration.class})` and its own `hotKeyCache` bean, fixing the ordering race.

### Data Flow

**Read path:** Caffeine L1 hit -> `hotKeyDetector.add(key,1)` + `hotKeyReporter.record(key)` -> return CacheEntry, OR L1 miss -> SingleFlight dedup -> async supplier (e.g., Redis) -> `hotKeyDetector.add(key,1)` + `hotKeyReporter.record(key)` -> cache as CacheEntry if hot -> optional broadcast

**Write path:** TransactionSupport deferral -> Redis INCR for dataVersion (VersionResult with degraded flag) -> run mutation -> update L1 (preserves existing decisionVersion) -> broadcast with dataVersion + degraded flag

**Broadcast reception:** CacheSyncListener uses 4-case degraded version comparison on `dataVersion` (normal-vs-normal, normal-vs-degraded, degraded-vs-normal, degraded-vs-degraded). WorkerListener uses simple compare on `decisionVersion` (no degraded logic — decisionVersion is always clean `AtomicLong`).

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
- **Java records** for immutable data carriers (`Item`, `AddResult`, `VersionResult`, `SyncMessage`, `ReportMessage`, `WorkerMessage`, `HotKeyDecision`)
- **Lombok** for boilerplate (`@Data`, `@RequiredArgsConstructor`, `@Slf4j`, `@Getter`)
- **Jakarta Validation** on `HotKeyProperties` fields
- **Git tag-based releases** — JitPack uses tags; Maven Central via `-P release` profile; both `hotkey-parent` and `hotkey` published; worker skipped via `<maven.deploy.skip>true</maven.deploy.skip>`
- **Two-version space design:** `dataVersion` (Redis INCR, may degrade to node-local counter) for cross-instance cache sync; `decisionVersion` (Worker `AtomicLong`, never degraded) for HOT/COOL decision ordering. Versions are orthogonal — data mutation does not affect `decisionVersion`, and Worker decision does not affect `dataVersion`.
- **Degraded version comparison:** Broadcast peers use 4-case comparison on `dataVersion` to prevent degraded versions from overwriting normal ones
- **`spring-amqp` is optional** in common module — app-only users don't pull AMQP; worker gets it via `spring-boot-starter-amqp`
- **`spring-boot-starter-aop` is optional** — only pulled when `@HotCache` annotation is used

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
  - English comments are preferred for new code; preserve existing Chinese comments as-is
  - Section dividers (e.g. `// ── Section Name ──`) are acceptable for organizing long classes

## Workflow Rules

1. **Skill usage per conversation:** Load `brainstorm` skill for discussion/planning. Use `grill-me` and `code-review` skills before any code modification. If task involves database, Redis, or other infrastructure, use the relevant MCP tools (e.g., `mcp__mysql`, `mcp__redis`).
2. **Do not modify code lightly.** Only change code when the change does not affect HotKey's core functionality (HeavyKeeper algorithm, cache orchestration, broadcast sync, auto-configuration). If in doubt, discuss first.
3. **Re-read before change.** Before modifying code or discussing a plan, re-read all related source files. Evaluate from multiple angles: performance, memory footprint, high concurrency, distributed broadcast correctness, caller UX. Produce a structured review (suggestion, reason, impact, source location, modification approach) before proposing changes.
4. **Consider all cases before change.** Before any code modification, consider edge cases, race conditions, failure modes, backward compatibility. Verify with tests or reasoning that the change is safe. Reference rules 2 and 3.
5. **No git without asking.** Never use `git checkout`, `git restore`, `git reset`, or any destructive git commands. Ask the author first.

## Reference Implementation

Original hotkey project (v0.0.4) is available at `D:\hotkey-master-v0.0.4` for comparison and design reference.

## Logging Rules

- **ERROR** (3): Only for genuine failures — broadcast processing crash, Redis mutation failure, async write failure
- **WARN** (4): Only for operational risks — executor rejection, Redis version fallback, singleflight timeout, async refresh failure
- **DEBUG** (25): Expected edge cases — invalid keys, dedup skips, missing broadcast, TTL expiry, soft expire fallback, non-transactional putThrough
- **INFO** (1): Only periodic summaries — expelled key drain (truncated to 20 keys)
- **Hot path rule:** Never log at WARN/INFO in per-request code paths. Use DEBUG or TRACE.
- **Loop rule:** Never log inside `forEach` loops — aggregate and log once after the loop
- **String rule:** Truncate unbounded `Collectors.joining()` output with `.limit(N)` + `...` suffix
