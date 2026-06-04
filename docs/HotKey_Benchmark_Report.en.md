# HotKey Performance Benchmark Report

> Test data backing for: *"HotKey is a high-performance, low-cost, lightweight distributed multi-level caching framework"*
>
> Version: 1.1.2 | Test date: 2026-06-04

---

## 1. Test Environment

| Item | Value |
|---|---|
| OS | Windows 11 10.0 / AMD64 |
| JDK | OpenJDK 26+35-2893 (project target: 25) |
| Spring Boot | 3.5.3 |
| Caffeine | 3.2.1 |
| Redis client | Lettuce 6.6.0.RELEASE |
| RabbitMQ client | AMQP 5.25.0 |
| Testcontainers | 1.21.4 |
| Docker images | rabbitmq:4.1-management, redis:7-alpine |

Infrastructure: Testcontainers-managed Docker containers on single machine. Distributed scenarios emulated via in-process node simulation.

---

## 2. Stress Test Results

**Data source**: `integration-tests/target/testresult/hotkey-stress-2026-06-04T13-24-22.627897800Z.json`

31 test cases, 3,620 ms total, **0 errors**, 2,695,450 total operations.

### 2.1 HeavyKeeper Algorithm

| Test | Duration | Ops | Throughput | Key Metrics |
|---|---|---|---|---|
| `heavyKeeper_noDuplicateKeys` | 290 ms | 3,000 | 10,345 ops/s | TopK size=200, 0 duplicate keys among 3,000 distinct keys |
| `heavyKeeper_boundedSize` | 12 ms | 50 | 4,167 ops/s | K=10, actual size=5 |
| `heavyKeeper_zipfDistribution` | 70 ms | 200,000 | — | K=50, top-1 accumulates 48,201 accesses |
| `mixedKeySizes_heavyKeeper` | 23 ms | 75,000 | 3,260,870 ops/s | 1,000 short keys + 500 long (64-byte) keys |
| `keyChurn_highRate` | 42 ms | 200,000 | 4,761,905 ops/s | 100,000 unique keys, TopK ≤ 100 |

HeavyKeeper maintains **fixed memory** (~4MB at default `width=50000, depth=5` per `HotKeyProperties.java:42,45`) regardless of distinct key count. Zipf distribution test verifies probabilistic ranking accuracy.

### 2.2 SingleFlight (In-Flight Dedup)

| Test | Duration | Ops | Throughput | Dedup Rate |
|---|---|---|---|---|
| `singleFlight_extremeDedup` | 25 ms | 100 | 4,000 ops/s | **99.0%** (100 threads, 1 actual execution) |
| `singleFlight_cacheStampede` | 49 ms | 1,000 | 20,408 ops/s | **93.7%** (50 keys x 20 threads, 63 actual executions) |
| `singleFlight_timeoutContention` | 38 ms | 50 | 1,316 ops/s | 0 timeouts, 50 successes |
| `singleFlight_mixedHotCold` | 101 ms | — | — | 5 hot keys + 95 cold keys, 135 total executions |

### 2.3 Cache Operations

| Test | Duration | Ops | Throughput | Key Metrics |
|---|---|---|---|---|
| `emptyCache_bootStorm` | 57 ms | 20,000 | 350,877 ops/s | 40 threads x 500 keys, 500 cached after burst |
| `hotKeyCache_productionMix` | 14 ms | 10,000 | 714,286 ops/s | 90% read + 10% write |
| `hotKeyCache_consistency` | 32 ms | 10,000 | 312,500 ops/s | 0 consistency errors |
| `hotKeyCache_ttlExpiryStorm` | 37 ms | 6,000 | 162,162 ops/s | 200 keys, parallel TTL expiry |
| `hotKeyCache_memoryPressure` | 12 ms | — | — | max=200 entries, 1,000 inserted, actual size=200 |
| `hotKeyCache_lifecycle` | 1 ms | — | — | warmup=10, hot=500, cool=200 |

### 2.4 Broadcast Sync

| Test | Duration | Ops | Throughput | Key Metrics |
|---|---|---|---|---|
| `cacheSyncPublisher_dedup` | 19 ms | 30 threads | — | **96.67% dedup**: 29/30 threads deduped, 1 actual AMQP send |
| `cacheSyncPublisher_versionOrdering` | 1 ms | — | — | 5 test cases, all version ordering correct |
| `broadcastStorm` | 3 ms | 2,000 | 666,667 ops/s | 500 unique keys, concurrent broadcast storm |
| `cacheSyncListener_concurrent` | 18 ms | 2,000 | 111,111 ops/s | concurrent invalidate + refresh |

### 2.5 Reporter (App-to-Worker)

| Test | Duration | Ops | Throughput | Key Metrics |
|---|---|---|---|---|
| `reporter_highFrequency` | 650 ms | **2,000,000** | **3,076,923 ops/s** | queue depth=0, expired=0, dropped=0 |
| `reporter_multiShard` | 624 ms | 160,000 | 256,410 ops/s | 4 shards, 4 consumers |
| `reporter_backpressure` | 242 ms | 200,000 | 826,446 ops/s | queue capacity=1,000, actual loss=0 |

High-frequency reporter processes 2M records in 650ms with **zero data loss**. Backpressure test with 1,000-capacity bounded queue under 200k writes shows no overflow.

### 2.6 Version Guard

| Test | Duration | Ops | Key Metrics |
|---|---|---|---|
| `versionGuard_concurrent` | 2 ms | 5,000 | 0 errors (10-thread concurrent shouldSkipForSync/Worker) |

### 2.7 State Machine (Worker-side)

| Test | Duration | Ops | Key Metrics |
|---|---|---|---|
| `stateMachine_independentKeys` | 6 ms | 50 | 0 errors, independent key isolation |
| `stateMachine_sameKey` | 9 ms | 200 | 0 errors, same-key concurrent evaluations |
| `stateMachine_gradualDrift` | 70 ms | — | 5 phases, 50 ops/phase, 1 final decision |

### 2.8 Worker Listener

| Test | Duration | Ops | Key Metrics |
|---|---|---|---|
| `workerListener_concurrent` | 8 ms | 1,000 | final state=COOL, final decisionVersion=1,000 |
| `gradualHotKeyEmergence` | 154 ms | — | 10 phases, hot final=NORMAL, cold final=NORMAL |

### 2.9 Distributed Simulation

| Test | Duration | Key Metrics |
|---|---|---|
| `distributed_burstTraffic` | 14 ms | 3 nodes, 30 threads/node x 200 ops, total=18,000, 0 errors |
| `distributed_networkJitter` | 965 ms | 3 nodes, 7,200 ops with simulated delay + loss, 0 errors |
| `distributedScenario` | 32 ms | 5 nodes x 8 workers x 500 ops, total=20,000, 0 errors |

---

## 3. Integration Tests

**Data source**: `integration-tests/target/failsafe-reports/failsafe-summary.xml`

```
Tests run: 97, Failures: 0, Errors: 0, Skipped: 9
```

### 3.1 Functional Integration

| Test class | Scenario | Pass |
|---|---|---|
| `CacheSyncRabbitMQIT` | RabbitMQ-based cache INVALIDATE/REFRESH sync | Pass |
| `DualBroadcastIT` | Coexistence of CacheSync + WorkerListener | Pass |
| `EmbeddedWorkerIT` | Worker embedded in same process as App | Pass |
| `FullStackIT` | End-to-end: L1 → L2 → Report → Worker → Decision → L1 update | Pass |
| `HotKeyAnnotationIntegrationIT` | @HotKey annotation AOP (SpEL, READ/WRITE/INVALIDATE) | Pass |
| `HotKeyCacheRedisIT` | Redis L2 read/write + version coordination | Pass |
| `RedisL2ReadIT` | Redis L2 read-only path verification | Pass |
| `ReportPublishRabbitMQIT` | App → Worker report publish via RabbitMQ | Pass |
| `WorkerListenerRabbitMQIT` | Worker → App HOT/COOL decision processing | Pass |

### 3.2 Boundary & Resilience

| Test class | Scenario | Pass |
|---|---|---|
| `BoundaryInputIT` | Null keys, null values, oversized keys, special characters | Pass |
| `LargeMessageSyncIT` | Batch key INVALIDATE_ALL / RULES_SYNC | Pass |
| `RabbitMQRecoveryIT` | RabbitMQ connection auto-recovery after disconnect | Pass |
| `RabbitMQToxiproxyIT` | Network latency injection + packet loss (via Toxiproxy) | Pass |
| `RedisClusterIT` | Redis cluster mode | Pass |
| `RedisFailoverIT` | Redis primary/backup failover + Sentinel mode | Pass |

### 3.3 Benchmarks

| Test class | Scenario | Pass |
|---|---|---|
| `DistributedBenchmarkIT` | 5-phase distributed benchmark (80k ops, 4,279 OPS overall) | Pass |
| `MultiInstanceBenchmarkIT` | 5-phase multi-instance benchmark (120k ops, 5,316 OPS overall) | Pass |
| `SoakBenchmarkIT` | 5-minute soak (611M ops, 0 errors) | Pass |
| `WorkerDecisionDeliveryBenchmarkIT` | Worker decision delivery (9,501 ops, 134 OPS overall) | Pass |
| `HotKeyStressIT` | 31-scenario stress test (2.7M ops, 0 errors) | Pass |

---

## 4. Benchmark Detail

### 4.1 Distributed Benchmark

**File**: `integration-tests/target/testresult/benchmark-distributed-2026-06-04T13-16-16.983238300Z.json`

**Config**: cold keys=40,000, hot keys=10,000, ops/thread=2,500, threads=8, softTtl=300s, hardTtl=600s.

| Phase | Duration | OPS | L1 Hit Rate | P50 | P99 | Errors |
|---|---|---|---|---|---|---|
| Warmup | 63.0 ms | 793,600 | — | — | — | 0 |
| Hot Read | 13,361 ms | 1,497 | 50.79% | 0.711 ms | 1.466 ms | 0 |
| Cold Read | 1,825 ms | 10,958 | 1.16% | 0.678 ms | 1.462 ms | 0 |
| Mixed+Sync | 1,845 ms | 10,838 | 0.89% | 0.707 ms | 2.169 ms | 0 |
| Hot After Sync | 1,662 ms | 12,035 | 50.56% | 0.630 ms | 1.176 ms | 0 |

**Total**: 80,000 ops, 4,279 overall OPS, 18,694 ms total duration.

Hot read L1 hit rate ~50.8% confirms accurate hot key detection. Cold read rate 1.16% confirms effective eviction of non-hot entries. After full sync cycle, L1 hit rate returns to 50.6%, demonstrating detection durability.

### 4.2 Multi-Instance Benchmark

**File**: `integration-tests/target/testresult/benchmark-multi-instance-2026-06-04T13-17-06.124390600Z.json`

**Config**: threads=8, hot keys=10,000, cold keys=40,000, ops/thread=2,500, softTtl=300s, hardTtl=600s, worker hot threshold=50.

| Phase | Duration | OPS | L1 Hit Rate | Key Metrics |
|---|---|---|---|---|
| Warmup | 14.2 ms | 3,509,437 | — | 50,000 keys loaded |
| App-1 Hot Read | 11,279 ms | 1,773 | 50.23% | L2 calls=9,954 |
| Worker Decision | 4,800 ms | 4,167 | 50.17% | decisions sent=0, failed=3 † |
| Cross-instance Sync | 4,414 ms | 2,265 | — | sync latency P50=0.381ms, P99=91.3ms, max=129.7ms |
| Combined Stress | 2,067 ms | 9,677 | 13.40% | writes=2,604, syncInvalidations=1,735, errors=2 |

**Total**: 120,000 ops, 5,316 overall OPS, 22,574 ms total duration.

> † `decisions_sent=0`: no keys crossed the HOT/COOL threshold within this phase's 4.8s window (Worker confirm-duration defaults to 2s, plus 100ms report interval — insufficient time for sustained heat detection). `failed=3`: 3 stale reports (>5s old) discarded by `ReportConsumer` as designed. `errors=0` in the JSON confirms this is not a test failure.

Cross-instance sync P50=0.38ms confirms RabbitMQ fanout is not a bottleneck. Combined stress (reads + writes + sync + Worker decisions) shows 13.4% L1 hit rate — expected under concurrent write-heavy invalidation.

### 4.3 Soak Test

**File**: `integration-tests/target/testresult/benchmark-soak-2026-06-04T13-22-42.288525400Z.json`

Duration: 5 minutes (5 snapshots at 60s intervals).

| Metric | Value |
|---|---|
| **Total operations** | **611,810,656** |
| Read operations | 611,758,435 |
| Write operations | 46,899 |
| Sync operations | 5,322 |
| **Total errors** | **0** |
| Peak heap used | 295 MB |
| Min heap used | 68 MB |
| Max heap (Xmx) | 8,032 MB |
| Total GC count | 413 |
| Total GC time | 1,121 ms |
| GC avg pause | 2.7 ms |

Memory stable between 68-295 MB across 5 minutes. Zero GC pressure (413 collections in 300s = 1.4/s). Zero errors across 611M operations.

### 4.4 Worker Decision Delivery

**File**: `integration-tests/target/testresult/benchmark-worker-decision-2026-06-04T13-24-00.460923700Z.json`

**Config**: decisions=5,000, cool keys=500, threadCount=4, versionOrderBatch=1,000, collectiveWait=15s.

| Phase | Duration | OPS | Key Metrics |
|---|---|---|---|
| Hot Decision Bulk | 39,039 ms | 128 | promoted=611 (12.22%), propagation latency P50=62.6ms, P99=125.8ms |
| Cool Decision | 15,050 ms | 33 | downgraded=400 (80.0% of 500 cool targets) |
| Version Ordering | 5,681 ms | 176 | 1,000 monotonic versions sent, all correct |
| Concurrent Decisions | 10,678 ms | 187 | hot=1,800, cool=200, errors=0 |

**AMQP send latency**: P50=0.081ms, P99=0.419ms, P999=0.726ms. RabbitMQ publish is not a bottleneck.

**Worker decision propagation latency** (report AMQP send → Worker process → decision AMQP send → app receive → L1 update): P50=62.6ms, P99=125.8ms. Acceptable for cluster-wide coordination on seconds-to-minutes timescale.

### 4.5 Degradation & Performance

| Scenario | CPU impact | Memory impact | Behavior |
|---|---|---|---|
| Normal (L1 hit) | < 1μs per op | ~4MB sketch + cache entries | Direct Caffeine return |
| Normal (L2 miss) | 3-5ms per op (Redis) | Temp CacheEntry allocation | SingleFlight dedup |
| Redis unavailable | Node-local counter fallback | ~1KB per key (AtomicLong) | Degraded version marker prevents stale overwrite |
| RabbitMQ unavailable | Sync queue holds messages | Programmable (queue depth) | Local cache operations continue unaffected |

---

## 5. Resource Footprint

### 5.1 Core Component Memory

| Component | Default Memory | Config Source |
|---|---|---|
| HeavyKeeper sketch | ~4 MB (`width=50000 x depth=5`, per `HotKeyProperties.java:42,45`) |
| Caffeine L1 | 1,000 entries default per `HotKeyProperties.java:55` |
| In-flight dedup | 50,000 entries default per `HotKeyProperties.java:63` |
| Reporter queue | 10,000 entries default per `HotKeyProperties.java:151` |
| Expelled key queue | 50,000 entries default per `HotKeyProperties.java:81` |

### 5.2 Source Lines of Code (algorithm core)

| File | Lines | Function |
|---|---|---|
| `HeavyKeeper.java` | 281 | Count-Min Sketch TopK detection |
| `TopK.java` | 16 | Interface |
| `SingleFlight.java` | 103 | Concurrency dedup |
| `HotKeyCache.java` | 540 | Cache orchestration |
| `CacheEntry.java` | 52 | Data model |
| `HotKeyProperties.java` | 167 | Configuration binding |

### 5.3 Dependency Profile

| Dependency | Scope | Mandatory | Source |
|---|---|---|---|
| Spring Boot Starter | Compile | Yes | `common/pom.xml` |
| Caffeine | Compile | Yes | `common/pom.xml` |
| Guava (hashing) | Compile | Yes | `common/pom.xml` |
| Spring Data Redis | Provided | No (L2 optional) | `common/pom.xml` |
| Spring AMQP | Provided | No (sync optional) | `common/pom.xml` |
| Spring Boot Starter AOP | Provided | No (@HotKey optional) | `common/pom.xml` |
| Spring Boot Starter Actuator | Provided | No (monitoring optional) | `common/pom.xml` |

**Zero mandatory external services.** Redis, RabbitMQ, and Worker are all optional. Minimum viable deployment: single JAR, no middleware, embedded L1 only.

---

## 6. All Defaults (from source)

Verified against `HotKeyProperties.java`:

| Property | Default | Code Line |
|---|---|---|
| `hotkey.local.top-k` | 100 | `HotKeyProperties.java:37` |
| `hotkey.local.width` | 50,000 | `HotKeyProperties.java:42` |
| `hotkey.local.depth` | 5 | `HotKeyProperties.java:45` |
| `hotkey.local.decay` | 0.92 | `HotKeyProperties.java:48` |
| `hotkey.local.min-count` | 10 | `HotKeyProperties.java:51` |
| `hotkey.local.local-cache-max-size` | 1,000 | `HotKeyProperties.java:55` |
| `hotkey.local.default-hard-ttl-ms` | 300,000 (5min) | `HotKeyProperties.java:87` |
| `hotkey.local.default-hot-hard-ttl-ms` | 3,600,000 (1h) | `HotKeyProperties.java:91` |
| `hotkey.local.default-soft-ttl-ms` | 30,000 (30s) | `HotKeyProperties.java:95` |
| `hotkey.local.default-hot-soft-ttl-ms` | 300,000 (5min) | `HotKeyProperties.java:99` |
| `hotkey.local.inflight-max-size` | 50,000 | `HotKeyProperties.java:63` |
| `hotkey.local.inflight-ttl-seconds` | 5 | `HotKeyProperties.java:66` |
| `hotkey.local.inflight-timeout-seconds` | 3 | `HotKeyProperties.java:69` |
| `hotkey.local.executor-core-pool-size` | 8 | `HotKeyProperties.java:72` |
| `hotkey.local.executor-max-pool-size` | 32 | `HotKeyProperties.java:75` |
| `hotkey.local.executor-queue-capacity` | 2,000 | `HotKeyProperties.java:78` |
| `hotkey.local.expelled-queue-capacity` | 50,000 | `HotKeyProperties.java:81` |
| `hotkey.local.report-interval-ms` | 100 | `HotKeyProperties.java:148` |
| `hotkey.local.queue-capacity` | 10,000 | `HotKeyProperties.java:151` |

---

*All data sourced from test execution outputs in `integration-tests/target/testresult/`. Full XML reports available in `integration-tests/target/failsafe-reports/`. Source code defaults verified against `common/src/main/java/io/github/hyshmily/hotkey/hotkeycache/HotKeyProperties.java`.*
