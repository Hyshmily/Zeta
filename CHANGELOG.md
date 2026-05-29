# Changelog

All notable changes to this project will be documented in this file.

## 1.0.9

- **Degraded Version Tracking** — `CacheEntry` now carries `isVersionDegraded` flag. `VersionResult` record replaces raw `long` return from `nextVersion()`. `BroadcastListener` uses 4-case comparison: normal-vs-normal (numeric), normal-vs-degraded (reject), degraded-vs-normal (accept), degraded-vs-degraded (numeric). `BroadcastPublisher` passes `isVersionDegraded` header in messages.
- **`BroadcastMessage` Record** — new record for type-safe message parsing, replacing inline header extraction in `BroadcastListener`.
- **Package Restructuring** — auto-configuration classes moved to `autoconfigure/` package. `AutoConfiguration.imports` updated accordingly.
- **`SingleFlight` Extraction** — singleflight dedup logic extracted from `HotKeyCache` into standalone `SingleFlight` class.
- **`SoftExpireManager` Extraction** — soft-expire tracking and async refresh extracted from `HotKeyCache` into standalone `SoftExpireManager` class.
- **`TransactionSupport` Utility** — transaction deferral logic extracted into `TransactionSupport.runAfterCommit()` and `runNowOrAfterCommit()`.
- **`CacheKeysPolicy` Utility** — `invalidCacheKey()` / `invalidTypeKey()` extracted from `HotKeyCache` into `CacheKeysPolicy`.
- **`loadSingleflight` softTtlMs Fix** — L1 miss path in `getWithSoftExpire` now correctly passes per-call `softTtlMs` to singleflight load, instead of always using the global default.
- **TTL Reference Table Fix** — corrected `putThrough(key, value, writer)` default from "falls back to `local-cache-ttl-minutes`" to `Long.MAX_VALUE` (no hard TTL override).

- **HotKey Bean Race Condition Fix** — `HotKeyRedisAutoConfiguration` now has `@AutoConfiguration(after = {HotKeyAutoConfiguration.class, RedisAutoConfiguration.class})` and its own `hotKey()` bean, ensuring `HotKey` is always created when Redis is on classpath regardless of auto-config ordering.
- **Combined Hard + Soft TTL** — new `getWithSoftExpire(key, reader, hardTtlMs, softTtlMs)` and `putThrough(key, value, writer, hardTtlMs, softTtlMs)` allow setting both Caffeine hard TTL and soft-expire TTL in a single call.
- **Parameter Rename** — `ttlMs` → `hardTtlMs`, `redisWriter` → `writer`, `redisMutation` → `mutation` for clarity.
- **`@since` Tags** — all public methods in `HotKey` facade now have `@since` Javadoc tags indicating the version they were introduced.
- **Javadoc Comments** — all public methods in `HotKey` facade now have English Javadoc comments explaining usage.
- **License Header** — all Java source files now carry Apache 2.0 copyright header (`Copyright 2026 Hyshmily`).
- **Typo Fix** — fixed `purThrough` → `putThrough` in `HotKey` facade.
- **Core Extraction Refactor** — extracted `CacheKeysPolicy`, `TransactionSupport`, `SingleFlight`, `SoftExpireManager`, `BroadcastMessage` into dedicated classes. `HotKeyCache` simplified from ~426 to ~277 lines.
- **SingleFlight Dedicated Bean** — `SingleFlight` owns the inflight dedup Cache internally; `HotKeyEndpoint` uses `SingleFlight.estimatedInflightSize()`.
- **Auto-Configuration Reorganization** — all 5 auto-configuration classes moved under `io.github.hyshmily.hotkey.autoconfigure` package.
- **BroadcastMessage Record** — new record `BroadcastMessage.from(Message)` encapsulates RabbitMQ parsing with `isVersionDegraded` header extraction.
- **Algorithms & Entity Packages Unchanged** — `HeavyKeeper`, `TopK`, `CacheEntry`, `HotKeyProperties` retain original packages.

## 1.0.8

- **Per-Entry Hard TTL** — `get(key, reader, ttlMs)` and `putThrough(key, value, writer, ttlMs)` allow setting a per-entry Caffeine hard TTL (milliseconds). Entries without a custom TTL remain governed by the global `hotkey.local-cache-ttl-minutes`.
- **Per-Call Soft TTL** — `getWithSoftExpire(key, reader, softTtlMs)` allows overriding the global `hotkey.soft-ttl-ms` on a per-call basis.
- **`VersionedValue` → `CacheEntry`** — internal cache entry type now carries `expireAtMs` alongside `value` and `version`. `VersionedValue` removed.
- **`expireAfter(Expiry)`** — Caffeine L1 cache builder switched from fixed `expireAfterWrite` to per-entry `Expiry` callback, enabling variable TTL across entries.
- **Delegation Refactor** — `get()`, `putThrough()`, `loadSingleflight()` no-arg overloads now delegate to their `ttlMs` counterparts, eliminating duplicate validation and logging.
- **Soft Expire TTL Preservation** — `triggerAsyncRefresh` now preserves the original entry's `expireAtMs` instead of resetting to `Long.MAX_VALUE`, keeping per-entry hard TTL intact across background refreshes.
- **`instance-id` Removed from YAML** — `BroadcastProperties.instanceId` has no setter (`@Setter(AccessLevel.NONE)`); the YAML property `hotkey.broadcast.instance-id` was non-functional and has been removed from config examples.
- **Broadcast TTL Preservation** — `BroadcastListener.handleVersionedHotKey` now preserves the local entry's `expireAtMs` instead of resetting to `Long.MAX_VALUE`, keeping per-entry hard TTL intact across broadcast updates.
- **`HeavyKeeper.contains()` O(1) Override** — overrode `contains()` with `heapIndex.containsKey()` inside `synchronized(sortedTopK)`, providing guaranteed O(1) `isHotKey()` lookups (previously fell through to O(K log K) default `list()` iteration).
- **`invalidate()` Added to API Docs** — single-key `invalidate(cacheKey)` now documented in the API reference table.
- **README Overhaul** — removed outdated known issue / workaround section; fixed `putInvalidate` → `putBeforeInvalidate` references; added `contains()` to architecture diagram; added TTL Reference table; rewrote soft expire section with comparison to traditional logical expiration.

## 1.0.7

- **TreeMap Top-K** — replaced `PriorityQueue` with `TreeMap` + `HashMap` in HeavyKeeper. All Top-K operations (insert, delete, evict) are now O(log K) instead of O(K), eliminating the linear `removeIf` scan on every hot key access.
- **Flat Bucket Array** — replaced `Bucket[][]` object array with flat `long[] fingerprints` + `int[] counts`, reducing per-bucket object header overhead (~50% memory reduction for the Count-Min Sketch).
- **Lock Stripping** — replaced per-bucket `synchronized` with 256 striped locks, reducing lock object count from 250,000 to 256.
- **LongAdder for Total Counter** — replaced `AtomicLong` with `LongAdder` for the global request counter, reducing CAS contention under high concurrency.
- **Atomic Broadcast Dedup** — `BroadcastPublisher.sendDeduped` now uses `compute()` instead of `putIfAbsent` + conditional update, eliminating the dedup race window.
- **Atomic Version Compare** — `BroadcastListener.handleVersionedHotKey` now uses `caffeineCache.asMap().compute()` for atomic version comparison and cache update, preventing lower-version overwrite.
- **Non-Blocking Jitter** — `BroadcastListener` no longer calls `Thread.sleep()` on the RabbitMQ consumer thread. Jitter delay is now scheduled via `ScheduledExecutorService`, and messages are ACKed before processing to avoid blocking consumption.
- **Singleflight Cleanup** — `loadSingleflight` now always invalidates the inflight entry in `whenComplete`, fixing a timing issue where `orTimeout` could leave stale futures in the dedup cache.
- **Lua Script for Version** — `nextVersion()` now uses a single Lua script (`INCR` + `EXPIRE`) instead of two separate Redis commands, halving the round-trip cost.
- **`isHotKey()` O(1)** — added `TopK.contains()` method backed by a `ConcurrentHashMap`-based hot key set, replacing the O(K log K) `list()` + stream lookup.
- **`get()` No Redundant Put** — removed the unnecessary `caffeineCache.put()` on cache hit; the `get()` path now only tracks frequency via `add()`.
- **`putThrough` Generic** — `putThrough` now accepts `<T>` for compile-time type safety.
- **`putThrough` Ordering** — `redisWriter.run()` now executes before `nextVersion()`, preventing version number gaps on write failures.
- **`invalidate()` Direct** — `invalidate()` now directly performs cache invalidation + broadcast instead of routing through `putInvalidate` with an empty Runnable.
- **`getRelaxed` → `getWithSoftExpire`** — clearer naming for the soft-expire read path.
- **Scheduling Separated** — `@EnableScheduling` removed from `HotKeyAutoConfiguration`; periodic decay and expelled drain moved to `HotKeySchedulingConfiguration`, controllable via `hotkey.scheduling.enabled`.
- **Configurable `expireAfterAccess`** — new `hotkey.local-cache-access-ttl-minutes` (default 0 = disabled) for supplementary L1 access-based expiration.
- **Broadcast Consumer Concurrency** — new `hotkey.broadcast.concurrent-consumers` (default 3) for parallel message consumption.
- **Message TTL** — broadcast queues now have a 60-second `x-message-ttl` to auto-expire stale messages on instance restart.
- **Executor Rejection Logging** — `hotKeyExecutor` now logs and throws `RejectedExecutionException` when the queue is full, instead of silently failing.
- **Actuator No Side Effect** — `/actuator/hotkey` endpoint now reads expelled keys via stream snapshot instead of `drainTo`, no longer consuming queue data.

## 1.0.6

- **Versioned Cache Invalidation** — replaces broadcast-based invalidation with Redis INCR global version tracking. `putThrough` writes L2, updates local cache, and broadcasts with a monotonic version number. Peers compare versions before refreshing, eliminating redundant Redis loads.
- **Exception-Safe Version Fallback** — `nextVersion()` catches Redis failures and falls back to `System.nanoTime()`, ensuring writes never block on version generation.
- **Version Key TTL** — new `hotkey.version-key-ttl-minutes` (default 60) auto-expires Redis version keys to prevent unbounded memory growth.
- **API Renames** — `getStale` renamed to `getRelaxed`, `putInvalidate` renamed to `putBeforeInvalidate`.
- **`peek()` Version-Aware** — automatically unwraps `VersionedValue`, transparent to callers.
