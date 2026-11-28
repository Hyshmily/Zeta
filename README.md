# Zeta

<p align="center">
  <a href="https://central.sonatype.com/artifact/io.github.hyshmily/zeta"><img src="https://img.shields.io/maven-central/v/io.github.hyshmily/zeta?color=blue" alt="Maven Central"></a>
  <a href="https://jitpack.io/#Hyshmily/Zeta"><img src="https://jitpack.io/v/Hyshmily/Zeta.svg" alt="JitPack"></a>
  <a href="https://github.com/Hyshmily/Zeta/releases"><img src="https://img.shields.io/github/v/release/Hyshmily/Zeta?color=brightgreen" alt="GitHub Release"></a>
  <a href="https://github.com/Hyshmily/Zeta/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/Hyshmily/Zeta/ci.yml?branch=master&label=CI&logo=github" alt="CI"></a>
  <a href="https://coveralls.io/github/Hyshmily/Zeta?branch=master"><img src="https://coveralls.io/repos/github/Hyshmily/Zeta/badge.svg" alt="Coveralls"></a>
  <a href="https://openjdk.java.net/"><img src="https://img.shields.io/badge/Java-17-orange" alt="Java"></a>
  <a href="https://spring.io/projects/spring-boot"><img src="https://img.shields.io/badge/Spring%20Boot-3.5.3-brightgreen" alt="Spring Boot"></a>
  <a href="https://github.com/Hyshmily/zeta/commits/master"><img src="https://img.shields.io/github/last-commit/Hyshmily/zeta/master" alt="Last Commit"></a>
  <a href="https://www.apache.org/licenses/LICENSE-2.0"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
  <a href="https://visitor-badge.laobi.icu/badge?page_id=Hyshmily.zeta"><img src="https://visitor-badge.laobi.icu/badge?page_id=Hyshmily.zeta" alt="Visitors"></a>
</p>

[**中文**](README.zh.md)

Zeta is a local-first, multi-instance caching and real-time hot-key management framework for Spring Boot. Requiring no external coordination stores such as Redis or ZooKeeper, it relies solely on message broadcasting to deliver end-to-end hot-key detection, prewarming and keep-warm renewal, versioned multi-instance synchronization, and graceful degradation at minimal overhead—with zero per-key pre-registration.

> [!NOTE]
>
> Zeta 1.2.0 is coming soon.

## How it works

### Local and cluster detection

```java
private final Zeta zeta;

// read-through: an L1 hit returns immediately; a miss coalesces through SingleFlight and counts as an access
User user = zeta.get("user:123", () -> userRepository.findById(123L));

// record hot-key access only (no L1 read/write; for external caches or detection-only deployments)
zeta.tag("product:123");
zeta.tagAll(Map.of("product:123", 3L, "product:456", 1L));

// pure in-memory peek: no load, no access count, no reporting side effect
Product cached = zeta.peek("product:123");
```

- **Local layer (on by default):** every instance runs a HeavyKeeper Top-K sketch. A key that enters the Top-K set becomes **local-hot**: it is its own evidence for background keep-warm renewal and it is reported as heat for observability. Local heat never rewrites the TTL the caller declared. An L1 miss coalesces concurrent requests; a soft-expired entry serves the stale value while refreshing in the background.
- **Cluster layer (optional):** per-instance counts reach RabbitMQ through the consistent-hash ring and are evaluated by a Worker cluster. The Worker keeps a sliding window per key and compares its sum with the governing rule's `hotCount` / `coolCount` (`EXACT` / `PREFIX` / `REGEX`, precedence-ordered; a matching exact rule decides on its own threshold, everything else is gated by the promotion floor). HOT/COOL decisions are broadcast back to every instance of that application through a direct exchange. They install or revoke a **cluster heat lease** — permission to spend an opt-in prewarm or renewal — and never rewrite a cache entry or its TTL.

## Multi-node cache coherence

Much like a primary/replica database syncing writes through a log-replication protocol, Zeta propagates cache changes between application instances over a RabbitMQ publish-subscribe channel:

- a cluster-synced write (`put(key, value, PutPolicy.synced())`) or an invalidation allocates a monotonic `dataVersion` from the node-local lock-free `StampClock`;
- `BroadcastBuffer` coalesces write fan-out inside a short deferral window and cancels buffered writes for older versions before an `INVALIDATE` leaves (causal cancellation);
- a synced write carries the serialized value (oversized or non-serializable values degrade to `REFRESH`); peers reduce each key's batch in order through `KeyDispatcher`, compare versions and apply directly with no read-back — one broadcast converges the whole cluster;
- version comparison, invalidation tombstones and hard TTLs deliver eventual consistency without the cost of Paxos or Raft.

Benchmarks (`HotHitBenchmark`, Intel Ultra 9 285H, JDK 26):

- `peek` **~123M ops/s** (8.1 ns/op: Caffeine lookup plus unwrapping, no side effects)
- `get` (L1 hit) **~40M ops/s** single-threaded (25.3 ns/op), **~99M ops/s** aggregated over 8 threads (80.8 ns/op; the full path including TopK and reporter)
- `get` (L1 hit, 1 KB String value) **~39M ops/s** (25.9 ns/op)

HotKey is inspired by JD's [hotkey](https://gitee.com/jd-platform-opensource/hotkey); the algorithms come from [Aegis](https://github.com/go-kratos/aegis) and [neural](https://github.com/yu120/neural/tree/master).

## Configuration

### Standalone App

Java 17 and Spring Boot 3.5.x are required.

```xml
<dependency>
  <groupId>io.github.hyshmily</groupId>
  <artifactId>zeta</artifactId>
  <version>1.2.0</version>
</dependency>
```

`zeta.report.enabled` defaults to `true`, but it is inert at this step: without `spring-boot-starter-amqp` there is no `RabbitTemplate`, so the reporter is never wired.

```yaml
zeta:
  local:
    top-k: 100 # seats in the local ranking
    min-count: 10 # minimum accesses to enter the TopK
    cache:
      max-size: 100000 # process-wide L1 ceiling
    hard-ttl-ms: 300000 # Caffeine eviction (Long.MAX_VALUE = permanent)
    soft-ttl-ms: 0 # background refresh off by default (-1 also disables it)
    key-format: TYPED # TYPED (collision-safe) | PLAIN (cache:value, one scalar type)
    warm:
      enabled: true # global kill switch for warming
      prewarm:
        trigger: OFF # OFF | LOCAL | CLUSTER | ANY; a namespace must opt in
      keep-warm:
        trigger: OFF # independent of soft-ttl; a namespace must opt in
```

Add `spring-boot-starter-actuator` to expose the endpoints:

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,hotkey,hotkeyring
```

```bash
curl -s localhost:8080/actuator/hotkey | jq '{topK: .local.topK, cacheSize: .local.cacheSize, total: .local.totalRequests}'
```

Every property, default and tuning note lives in [CONFIG.md](docs/CONFIG.md) (§2–§9); endpoint and meter fields are documented in [MONITOR.md](docs/MONITOR.md).

### Clustered App

Add the AMQP starter on top of the standalone setup:

```xml
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-amqp</artifactId>
</dependency>
```

Then point the App at the broker and enable the paths you need:

```yaml
spring:
  rabbitmq:
    host: ${RABBITMQ_HOST:localhost}
    port: 5672
    username: ${RABBITMQ_USER:zeta}
    password: ${RABBITMQ_PASSWORD:zeta}

zeta:
  app-name: order # tenancy key + decision routing key, must match the Worker; [A-Za-z0-9_-]+
  report:
    enabled: true # the default
  worker-listener:
    enabled: true # heartbeat + HOT/COOL
  sync:
    enabled: true # optional: cross-instance PUT/REFRESH/INVALIDATE
  local:
    instance-id: ${HOSTNAME}-1 # unique per instance (add replica/port when several share a host)
    node-id: 1 # unique per instance; defaults to the CRC32 of instance-id
```

Every property of each path is listed in [CONFIG.md](docs/CONFIG.md) (§6 report queue, §7 Worker listener and cluster health, §8 cache sync); endpoint fields are in [MONITOR.md](docs/MONITOR.md).

### Worker

**Option A — compose, broker included**

```bash
mvn clean package -pl worker -am # the image copies the packaged JAR
docker compose -f worker/docker-compose.yml up -d --build
```

compose reads `worker/.env` automatically. `ZETA_APP_NAME` must match the App's `zeta.app-name` — it decides which application's reports this Worker consumes, and it is the **routing key** of the decisions it broadcasts: the Worker publishes to the direct exchange `zeta.send.exchange` under `send.<app-name>`, and each App instance binds its queue under the same key.

```dotenv
ZETA_APP_NAME=myapp
ZETA_WORKER_TAG=1.2.0
RABBITMQ_USER=zeta
RABBITMQ_PASSWORD=change-me
```

**Option B — standalone container against your own broker**

```bash
docker run -d --name zeta-worker \
  -p 127.0.0.1:8080:8080 \
  -e SPRING_RABBITMQ_HOST=<rabbitmq-host> \
  -e SPRING_RABBITMQ_USERNAME=<user> \
  -e SPRING_RABBITMQ_PASSWORD=<password> \
  -e ZETA_APP_NAME=myapp \
  -e ZETA_WORKER_EPOCH_FILE=/var/lib/zeta/epoch \
  -v zeta_worker_epoch:/var/lib/zeta \
  ghcr.io/hyshmily/zeta-worker:1.2.0
```

**Option C — build your own Worker process**

```java
@SpringBootApplication(scanBasePackages = { "com.example.worker", "io.github.hyshmily.zeta.worker" })
public class MyWorkerApplication {

  public static void main(String[] args) {
    SpringApplication.run(MyWorkerApplication.class, args);
  }
}
```

The following applies to a self-built Worker or a directly run jar:

```yaml
zeta:
  app-name: order # must match the App's zeta.app-name
  report:
    enabled: false # a Worker-only node does not report
  worker:
    enabled: true
    node-id: worker-1 # stable id; queue + epoch-file names must survive restarts
    epoch-file: /var/lib/zeta/epoch # must survive restarts
    detection:
      defaults:
        hot-qps: 1000
        cool:
          strategy: COOL_FOR_LOW_QPS
          cool-qps: 100
          low-qps-for: 30s

management:
  endpoints:
    web:
      exposure:
        include: health
  endpoint:
    health:
      probes:
        enabled: true
```

The shipped Worker exposes only `health`. The read-only detection view is opt-in: add
`hotkeyworker` to `management.endpoints.web.exposure.include` and set
`management.endpoints.web.path-mapping.hotkeyworker: hotkey-worker` to publish it as
`/actuator/hotkey-worker` (Boot 3.5 requires alphanumeric endpoint ids). A self-built Worker uses
the same opt-in.

Every Worker-side property is listed in [CONFIG.md](docs/CONFIG.md) (§10 messaging ingress, §11 detection, §12 scheduling); runtime endpoints are in [MONITOR.md](docs/MONITOR.md) (§4 worker state).

### Verification

**1. Broker topology.** The Worker declares one durable report queue per shard,
`zeta.reportToWorker.<appName>.<workerNodeId>`:

```bash
docker compose -f worker/docker-compose.yml exec rabbitmq rabbitmqctl list_queues name consumers messages
docker compose -f worker/docker-compose.yml exec rabbitmq \
  rabbitmqctl list_bindings source_name destination_name routing_key
```

The queue should show `zeta.worker.report-consumer.concurrent-consumers` consumers (8 by default) and
a growing message count. Its binding must name `zeta.reportToWorker.exchange` as the source; otherwise
check the two `report-exchange` settings in [CONFIG.md §10](docs/CONFIG.md#10-worker-basics-and-messaging-ingress).

**2. The App can see the Worker:**

```bash
curl -s localhost:8080/actuator/hotkey | jq '.worker'
```

`health` should be `healthy` and `msSinceLastAnyHeartbeat` should sit near 1000. A value of `-1` means no heartbeat has ever arrived: wrong broker, wrong `app-name`, or `worker-listener.enabled=false`.

**3. The Worker can see reports** (with the opt-in `hotkeyworker` exposure enabled):

```bash
docker compose -f worker/docker-compose.yml exec worker \
  curl -fsS 'localhost:8080/actuator/hotkey-worker?key=user:42'
```

A positive `windowSum`, or `hot: true` after sustained traffic, means reports arrived. See
[MONITOR.md](docs/MONITOR.md) §4 for the fields.

## Data loaders

Zeta ships no data-access client; every value enters L1 through the loader you supply.

### SPI

`io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader<K, V>`:

| Method                     | Answers                                                                         | Default  |
| -------------------------- | ------------------------------------------------------------------------------- | -------- |
| `V load(K key)`            | where the value comes from — called on a foreground miss                        | abstract |
| `V reload(K key)`          | where a refresh reads it — never on the original call stack                     | `load`   |
| `boolean isCacheable(V v)` | whether a value the engine obtained on the loader's behalf may be written to L1 | `true`   |

```java
CacheLoader<String, User> userLoader = new CacheLoader<>() {
  @Override
  public User load(String key) {
    // Foreground miss: with callerRunsLoader = true this runs on the caller's thread and sees
    // its transaction and security context.
    return userRepository.findByKey(key);
  }

  @Override
  public User reload(String key) {
    // Background refresh / prewarm / peer REFRESH: never on the original call stack, so it must
    // be stateless and thread-safe. Returning null means "keep the current entry", which keeps
    // serving until its hardTtl expires instead of turning a servable value into a miss.
    return userReadOnlyRepository.findSnapshotByKey(key);
  }

  @Override
  public boolean isCacheable(User value) {
    // Admission before the L1 write: false still returns the value to the caller but stores
    // nothing; a peer PUT value goes through the same rule. null is never cached and never
    // reaches this method. Do not block on I/O here.
    return !value.isPlaceholder() && value.isActive();
  }
};

User user = zeta.get("user:1", userLoader);
```

| Consumer                                                    | Uses it to                                                | Without it                                                                     |
| ----------------------------------------------------------- | --------------------------------------------------------- | ------------------------------------------------------------------------------ |
| `HotKeyHandler` prewarm (`prewarmIfAbsent` / `prewarmWith`) | load a missing entry on a valid HOT lease                 | no prewarm; the HOT only reaches business `onHot`                              |
| `CacheSyncListener` → `DefaultSyncContext`                  | reload after a peer `REFRESH`                             | degrades to a local invalidation — the next read goes back through your reader |
| the same context's value-carrying `PUT`                     | validate the value carried on the wire with `isCacheable` | no loader, no validation: the carried value is applied as-is                   |

### Programmatic registration

Beyond annotations, `ZetaWarmConfigurer` registers a namespace's loader and its prewarm/keep-warm
triggers at startup; the registry freezes after every configurer has run and accepts no further
registrations.

```java
@Configuration
public class WarmConfiguration {

  @Bean
  ZetaWarmConfigurer warmConfig(UserRepository userRepo, QuoteFeed quoteFeed, GenericSource fallbackSource) {
    return (registry) -> {
      // Typed namespace: the framework decodes the physical key into the namespace's key type,
      // so the loader receives a Long.
      registry
        .namespace("users", Long.class)
        .format(KeyFormat.PLAIN) // omitted: inherit the annotation's format or the global default; a conflict fails startup
        .cacheLoader(
          new CacheLoader<Long, User>() {
            @Override
            public User load(Long id) {
              return userRepo.findById(id);
            }

            @Override
            public boolean isCacheable(User user) {
              return user.isActive();
            }
          }
        )
        .prewarm(HeatTrigger.CLUSTER) // OFF | LOCAL | CLUSTER | ANY
        .keepWarm(HeatTrigger.ANY) // independent of softTtl; opt in explicitly
        .register();

      // Raw namespace: the loader handles the complete physical key, for a PLAIN namespace with
      // several key shapes or a custom parse.
      registry
        .namespaceRaw("quotes")
        .format(KeyFormat.PLAIN)
        .loader((physicalKey) -> quoteFeed.snapshot(physicalKey.substring("quotes:".length())))
        .prewarm(HeatTrigger.ANY)
        .keepWarm(HeatTrigger.CLUSTER)
        .register();

      // Fallback loader: only for keys that belong to no registered namespace (dynamic tenant prefixes).
      registry.fallbackLoader((physicalKey) -> fallbackSource.load(physicalKey));
    };
  }
}
```

| Declaration                 | The loader receives     | Use it for                                           |
| --------------------------- | ----------------------- | ---------------------------------------------------- |
| `.namespace(name, K.class)` | the decoded logical key | one reversible key type (scalars, UUID)              |
| `.namespaceRaw(name)`       | the full physical key   | PLAIN namespaces with several shapes, custom parses  |
| `.fallbackLoader(loader)`   | the full physical key   | namespaces that are never registered (tenant prefix) |

`.loader(Function)` and `.cacheLoader(CacheLoader)` are alternatives: the first supplies values only,
the second also owns `reload` and `isCacheable`. A namespace has at most one loader; a second
registration, or a `format` that conflicts with the annotation declaration, fails startup instead of
overwriting.

## Broadcast listener and extensions

### Observe heat decisions and sync events

`HotKeyListener` and `SyncHook` are registered as `@Bean`s and may coexist; a failing callback is
isolated and logged, and never interrupts the per-key ordered dispatcher.

```java
@Configuration
public class ZetaObserverConfiguration {

  // Heat leases: the whole lifecycle a Worker grants and revokes.
  @Bean
  public HotKeyListener heatLifecycleMonitor(MeterRegistry metrics) {
    return new HotKeyListener() {
      @Override
      public void onHot(WorkerDecision decision, boolean newlyHot) {
        // newlyHot == false is a repeat broadcast that renews the same lease.
        if (newlyHot) {
          metrics.counter("zeta.hot.granted", "worker", decision.stamp().decisionNodeId()).increment();
        }
      }

      @Override
      public void onCool(HeatLease lease, HeatLease.EndReason reason) {
        // Every terminal reason: a COOL decision, a dead/restarted Worker, table eviction, expiry.
        metrics.counter("zeta.hot.ended", "reason", reason.name()).increment();
      }

      @Override
      public void onHotSkipped(WorkerDecision decision, DecisionSkipReason reason) {
        log.debug("HOT skipped key={} reason={}", decision.cacheKey(), reason);
      }

      @Override
      public void onCoolSkipped(WorkerDecision decision, DecisionSkipReason reason) {
        log.debug("COOL skipped key={} reason={}", decision.cacheKey(), reason);
      }
    };
  }

  // Sync terminal state: every event handled by the dispatcher reaches exactly one of the five.
  @Bean
  public SyncHook syncAuditHook(MeterRegistry metrics) {
    return new SyncHook() {
      @Override
      public void afterValueApplied(SyncEvent event, EntryView entry) {
        metrics.counter("zeta.sync.applied", "type", event.type().name()).increment();
      }

      @Override
      public void afterValueDegraded(SyncEvent event, SyncSkipReason reason) {
        metrics.counter("zeta.sync.degraded", "reason", reason.name()).increment();
      }

      @Override
      public void afterInvalidate(SyncEvent event) {
        metrics.counter("zeta.sync.invalidated").increment();
      }

      @Override
      public void afterClear(List<SyncEvent> clears, int affectedEntries) {
        // A granted clear batch completes its one coalesced sweep and calls back once.
        log.info("Sync clear applied: batches={}, removed={}", clears.size(), affectedEntries);
      }

      @Override
      public void onSkipped(SyncEvent event, SyncSkipReason reason) {
        metrics.counter("zeta.sync.skipped", "reason", reason.name()).increment();
      }
    };
  }
}
```

| Listener         | Method                            | Fires when                                                                                                                                                                                                                               |
| ---------------- | --------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `HotKeyListener` | `onHot(decision, newlyHot)`       | a valid HOT lease was granted (`newlyHot` is false for a repeat broadcast)                                                                                                                                                               |
| `HotKeyListener` | `onCool(lease, reason)`           | a live HOT lease ended: `COOL`, `INCARNATION_DEAD`, `EVICTED`, `EXPIRED`                                                                                                                                                                 |
| `HotKeyListener` | `onHotSkipped(decision, reason)`  | a HOT was refused (`VERSION_STALE`, `COVERED_BY_NEWER_DECISION`)                                                                                                                                                                         |
| `HotKeyListener` | `onCoolSkipped(decision, reason)` | a COOL was refused (`VERSION_STALE`, `GHOST_COOL`, `COVERED_BY_NEWER_DECISION`)                                                                                                                                                          |
| `SyncHook`       | `afterValueApplied`               | a peer `REFRESH` read-back or a value-carrying `PUT` was written                                                                                                                                                                         |
| `SyncHook`       | `afterValueDegraded`              | a value event was applied as a local invalidation (`VALUE_NOT_FOUND`, `PAYLOAD_UNREADABLE`, `VALUE_DECLINED_BY_LOADER`, `DEGRADED_TO_INVALIDATE`, `ABSENT_NOT_PREWARMABLE`, `TTL_NOT_PREWARMABLE`, `REFRESH_REJECTED`, `REFRESH_FAILED`) |
| `SyncHook`       | `afterInvalidate`                 | a peer `INVALIDATE` was applied                                                                                                                                                                                                          |
| `SyncHook`       | `afterClear`                      | a granted clear batch completed its one coalesced sweep                                                                                                                                                                                  |
| `SyncHook`       | `onSkipped`                       | an event produced no effect (`VERSION_STALE`, `INVALIDATION_TOMBSTONE`, `COVERED_BY_NEWER_EVENT`, `POLICY_REJECTED`, `POLICY_FAILED`)                                                                                                    |

### Replace the default heat action and sync policy

Expose a `HotKeyHandler` bean to change what a decision does with the data plane, and a `SyncPolicy`
bean to customize peer-sync admission; autoconfiguration backs off with `@ConditionalOnMissingBean`,
and the first bean wins.

```java
@Configuration
public class ZetaStrategyConfiguration {

  private static final String FLASH_NAMESPACE = "flash";
  private static final String USERS_TYPED = "users\u0000"; // physical prefix of a TYPED namespace
  private static final String ORDERS_TYPED = "orders\u0000";

  // Runs on the per-key ordered dispatch thread: never block on I/O here.
  @Bean
  public HotKeyHandler customHotKeyHandler(FlashSaleLoader flashSaleLoader) {
    return new HotKeyHandler() {
      @Override
      public void onHot(HotKeyContext context) {
        if (!context.isNewlyHot()) {
          return; // a repeat HOT for the same lease does nothing
        }
        // namespace() answers the bare name for both the TYPED and the PLAIN encoding.
        if (FLASH_NAMESPACE.equals(context.namespace())) {
          if (context.hasLiveEntry()) {
            context.armKeepWarm(); // a live entry starts its renewal cycle
          } else {
            // Prewarm with an explicit stateless loader: skips the namespace prewarm trigger and
            // still obeys the static TTL.
            context.prewarmWith(flashSaleLoader);
          }
          return;
        }
        // Default behaviour: prewarm an absent entry through the namespace policy, or arm keep-warm
        // for a live entry whose trigger matches.
        HotKeyHandler.super.onHot(context);
      }
      // onCool needs no override: the default drops a renewal task whose keep-warm trigger no
      // longer accepts the key, and a COOL never evicts an entry or rewrites a TTL.
    };
  }

  @Bean
  public SyncPolicy customSyncPolicy() {
    return new SyncPolicy() {
      @Override
      public SyncAdmission admit(SyncEvent event, @Nullable EntryView current) {
        // A clear event: current is always null, and prefix() == null means the whole table.
        if (event.isClear()) {
          String prefix = event.prefix();
          return prefix == null || prefix.startsWith(USERS_TYPED) || USERS_TYPED.startsWith(prefix)
            ? SyncAdmission.PROCEED
            : SyncAdmission.SKIP;
        }

        String key = event.key();
        // Turn the value path into a local invalidation: orders are reloaded from this node's own
        // store, so a peer value is refused but the stale copy must go. On an INVALIDATE the same
        // answer is just PROCEED, so the rule is safe to apply to every operation of the namespace.
        if (event.isValue() && key != null && key.startsWith(ORDERS_TYPED)) {
          return SyncAdmission.INVALIDATE_LOCAL;
        }
        return key != null && key.startsWith(USERS_TYPED) ? SyncAdmission.PROCEED : SyncAdmission.SKIP;
      }

      @Override
      public SyncValueAction settle(SyncEvent event, Object resolvedValue, @Nullable EntryView current) {
        // Called only after a PUT decoded or a REFRESH read back; the framework still re-checks the
        // version, the tombstone and isCacheable before writing.
        if (resolvedValue instanceof User user) {
          if (user.deletedAt() != null) {
            return SyncValueAction.INVALIDATE; // a soft delete degrades to a versioned local invalidation
          }
          if (!user.isComplete() && EntryView.isLive(current)) {
            return SyncValueAction.SKIP; // keep the complete live copy
          }
          return SyncValueAction.apply(user.masked());
        }
        return new SyncValueAction.Apply(resolvedValue);
      }
    };
  }
}
```

| Interface       | Methods                                                               | The framework owns                                                                         |
| --------------- | --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `HotKeyHandler` | `onHot(HotKeyContext)`, `onCool(HotKeyContext)`                       | lease ordering, the prewarm guard, one listener call per applied decision                  |
| `SyncPolicy`    | `admit(SyncEvent, EntryView)`, `settle(SyncEvent, Object, EntryView)` | batch reduction, version/tombstone/clear guards, decode/read-back, one hook call per event |

`admit` runs once per key batch on the effective event (covered messages are reported as `COVERED_BY_NEWER_EVENT`), and once per clear; the expiry/tombstone pre-check runs before the policy.

| `SyncAdmission`    | PUT / REFRESH                                                        | INVALIDATE                               | INVALIDATE_ALL / INVALIDATE_PREFIX            |
| ------------------ | -------------------------------------------------------------------- | ---------------------------------------- | --------------------------------------------- |
| `PROCEED`          | decode/read back, then `settle`                                      | evict at the event version               | record the watermark + one coalesced sweep    |
| `INVALIDATE_LOCAL` | skip decode/read back; local invalidation (`DEGRADED_TO_INVALIDATE`) | treated as `PROCEED` (already a removal) | treated as `PROCEED` (already a local sweep)  |
| `SKIP`             | entry unchanged (`POLICY_REJECTED`)                                  | entry unchanged (`POLICY_REJECTED`)      | this clear joins no sweep (`POLICY_REJECTED`) |

A clear is prefix-based (`prefix() == null` means table-wide) and its `current` is always `null`; a refused or failed clear only skips itself. A policy that throws or returns `null` settles as `POLICY_FAILED`, and every path reaches exactly one `SyncHook` callback.

| `SyncValueAction` | Effect                                                                                                        |
| ----------------- | ------------------------------------------------------------------------------------------------------------- |
| `Apply(value)`    | write at the event version after re-checking the guards and `isCacheable`; a refusal degrades to invalidation |
| `Invalidate()`    | drop the value and invalidate locally at the event version (`DEGRADED_TO_INVALIDATE`)                         |
| `Skip()`          | leave the resident entry unchanged (`POLICY_REJECTED`)                                                        |

## Facade API

The `Zeta` bean exposes `get` / `put` / `invalidate` / `refresh` and their batch forms, plus the `stats()`, `detector()` and `unsafe()` diagnostic views. Full documentation is indexed by topic:

- **Reads, writes, invalidation, refresh, TTLs, sync scope and deployment modes:** [CONFIG.md](docs/CONFIG.md)
- **Actuator endpoints, statistics and metrics:** [MONITOR.md](docs/MONITOR.md)
- **Declarative API:** [Annotations](#annotations) below
- **Extension points and callbacks:** [Broadcast listener and extensions](#broadcast-listener-and-extensions) below

## Annotations

| Annotation        | Target       | Effect                                                                                                                                                                                |
| ----------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `@ZetaCached`     | M, A         | Read-through: a hit skips the body, a miss runs it once and caches a non-`null` result. `condition` bypasses the cache before the key; `Optional<T>` stores the value.                |
| `@ZetaPut`        | M, A         | Write-through: runs the body, then caches a non-`null` result. `condition` skips the write before the key; `Optional<T>` stores the value; a `#result` key resolves after.            |
| `@ZetaInvalidate` | M, A, repeat | Removes after the body by default (`condition`/`key` may read `#result`); `beforeInvocation = true` removes first. `eachKey` expands a collection, `allEntries` clears one namespace. |

`M` = method, `A` = annotation type.

### `@ZetaConfig`

Class-level defaults for `cache`, TTLs and triggers, merged attribute by attribute from the nearest
declaration: a subclass may override only `hardTtl` and inherit the parent's namespace, and a non-blank
method attribute wins.

```java
// Class-level defaults.
@ZetaConfig(
  cache = "products",
  hardTtl = "300000",
  softTtl = "15000",
  prewarm = HeatTrigger.Option.CLUSTER,
  keepWarm = HeatTrigger.Option.ANY,
  keyFormat = KeyFormat.Option.PLAIN // applies only to methods that inherit this class's cache
)
public abstract class BaseProductService {}

// The subclass overrides hardTtl only; cache / softTtl / prewarm / keepWarm / keyFormat are inherited.
@ZetaConfig(hardTtl = "600000")
@Service
public class DetailedProductService extends BaseProductService {

  // Non-blank wins: "0" falls back to the global default and "-1" disables the soft window;
  // neither inherits the class value.
  @ZetaCached(key = "#id", hardTtl = "0", softTtl = "-1")
  public Optional<Product> findRaw(Long id) {
    return productRepository.findById(id); // Optional.empty() is never cached
  }
}
```

A method that names a different `cache` inherits neither this class's `keyFormat` nor its TTLs: those
defaults belong to another namespace.

### Composed annotations

```java
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@ZetaCached(cache = "dict", hardTtl = "3600000", softTtl = "-1")
public @interface DictCached {
  @AliasFor(annotation = ZetaCached.class, attribute = "key")
  String key() default "";
}
```

`@AliasFor` is equivalent to writing the attribute directly, and composed annotations stack.

### `@ZetaCached`

```java
// 1. Default composite key and configuration placeholders
@Service
public class CatalogService {

  // cache / hardTtl / softTtl all accept ${...} placeholders resolved once at startup (defaults allowed)
  // Omitting key encodes every argument with the DefaultKey rules (typed, sets sorted, arrays by content)
  @ZetaCached(cache = "${app.cache.catalog:catalog}", hardTtl = "${app.cache.catalog-ttl:120000}")
  public PageResult<Product> listProducts(Long categoryId, Set<String> tags, int page) {
    return productRepository.findByCategory(categoryId, tags, page);
  }

  // #root.methodName, @beanName.method(...) and the positional aliases #p0/#a0 are all available
  @ZetaCached(cache = "catalog_views", key = "#root.methodName + ':' + @tenantContext.current() + ':' + #p0")
  public ProductView view(Long productId) {
    return productRepository.findView(productId);
  }
}

// 2. Soft expiry and a dynamic TTL (stale-while-revalidate)
@Service
public class OrderReadService {

  // softTtl = 10s: a stale hit serves immediately while exactly one background recompute runs
  // hardTtl is an expression: evaluated only on a miss or a refresh, never on a plain hit
  // A background refresh invokes the method reflectively, outside the cache advice chain, so do not
  // configure softTtl on a method that depends on request state or the caller's transaction.
  @ZetaCached(cache = "orders", key = "#orderId", softTtl = "10000", hardTtl = "#isVip ? 120000 : 60000")
  public Order getOrder(String orderId, boolean isVip) {
    return orderRepository.findById(orderId);
  }
}

// 3. Bypass before the read and veto after it
@Service
public class OrderGuardService {

  // condition runs before the key: a null orderId runs the body (no L1 probe, no null-key failure)
  // unless runs after a non-null return; #result is the unwrapped domain value, and true returns it
  // without storing. A null return is never cached, so #result == null needs no check here.
  @ZetaCached(
    cache = "orders",
    key = "#orderId",
    condition = "#orderId != null && !#bypass",
    unless = "#result.status == 'PENDING'"
  )
  public Order getOrder(String orderId, boolean bypass) {
    return orderRepository.findById(orderId);
  }
}

// 4. PLAIN format plus the namespace's self-registered background loader (at most one)
@ZetaConfig(cache = "products", keyFormat = KeyFormat.Option.PLAIN)
@Service
public class ProductWarmReadService {

  // selfWarmLoader = true binds this method as the "products" background loader at startup.
  // Constraints: a reversible key (no-arg constant, one scalar, default scalars), no condition,
  // unless may read only #result, and prewarm requires a static TTL. The load runs outside the
  // caller's advice chain: a transaction or method-security advisor on the method is named in a
  // startup WARN, and a body that depends on one belongs in a ZetaWarmConfigurer loader.
  @ZetaCached(
    key = "#id",
    prewarm = HeatTrigger.Option.CLUSTER,
    keepWarm = HeatTrigger.Option.ANY,
    selfWarmLoader = true
  )
  public Product getProduct(Long id) {
    return productRepository.findById(id).orElse(null);
  }
}
```

The same namespace may bind its loader through [programmatic registration](#programmatic-registration)
instead; only one of the two may exist.

### `@ZetaPut`

```java
// 5. Write back the generated key, then decide the TTL and the veto from the result
@Service
public class AccountWriteService {

  // A key that reads #result resolves after the body, so #result.id sees the generated key.
  // unless == true skips the write and still returns; a null #result.id skips it too.
  // sync = true (default) broadcasts the value to peers; in that mode hardTtl must not resolve to Long.MAX_VALUE.
  @ZetaPut(
    cache = "accounts",
    key = "#result.id",
    unless = "#result.status != 'ACTIVE'",
    hardTtl = "#result.isVip() ? 3600000 : 600000",
    softTtl = "30000"
  )
  public Account createAccount(AccountDraft draft) {
    Account account = new Account(draft.getUsername(), draft.getEmail());
    accountRepository.insert(account); // account.id is filled in by the time this returns
    return account;
  }

  // 6. Local-only write: sync = false sends no broadcast and permits a permanent entry.
  // condition runs before the body; Optional<UserSession> stores the UserSession, an empty Optional stores nothing.
  @ZetaPut(
    cache = "user_sessions",
    key = "#token",
    condition = "#token != null",
    sync = false,
    hardTtl = "1800000",
    softTtl = "-1"
  )
  public Optional<UserSession> refreshLocalSession(String token) {
    return sessionClient.fetchRemoteSession(token);
  }
}
```

### `@ZetaInvalidate`

```java
// 7. Before-invocation removal, batch expansion, whole-namespace clear
@Service
public class InventoryWriteService {

  // beforeInvocation = true removes before the body and is not deferred to afterCommit;
  // neither key nor condition may read #result in that phase.
  @ZetaInvalidate(cache = "inventory", key = "#skuId", beforeInvocation = true, scope = InvalidatePolicy.Scope.BOTH)
  public void deductStock(Long skuId, int quantity) {
    inventoryRepository.decrease(skuId, quantity);
  }

  // eachKey = true expands an array or Iterable into one physical key each (max 1024 per call, key required)
  @ZetaInvalidate(cache = "inventory", key = "#skuIds", eachKey = true, condition = "#skuIds != null")
  public void deleteAll(long[] skuIds) {
    inventoryRepository.deleteAll(skuIds);
  }

  // allEntries = true clears the whole namespace; mutually exclusive with key / eachKey
  @ZetaInvalidate(cache = "inventory", allEntries = true)
  public void rebuildInventory() {
    inventoryRepository.rebuild();
  }
}

// 8. Repeatable annotations, post-body #result navigation, remote-only removal
@Service
public class ConfigWriteService {

  // Each @ZetaInvalidate owns its cache / key / condition / scope.
  // After-body (default): a throwing body keeps the entry; #result?.key tolerates a null return
  // (a null resolved key skips that removal silently).
  // scope = REMOTE: this node is already updated, only peers must drop their copy.
  @ZetaInvalidate(
    cache = "config",
    key = "#result?.configKey",
    condition = "#result != null && #result.published",
    scope = InvalidatePolicy.Scope.REMOTE
  )
  @ZetaInvalidate(
    cache = "config_by_group",
    key = "#result != null ? #result.groupCode : #form.groupCode",
    scope = InvalidatePolicy.Scope.BOTH
  )
  public ConfigItem updateConfig(ConfigUpdateForm form) {
    return configRepository.update(form);
  }
}
```

## Monitoring

Zeta offers two complementary monitoring surfaces: Actuator endpoints (`/actuator/hotkey`, `/actuator/hotkeyring`, `/actuator/hotkey-worker`) and Micrometer meters.

The endpoints are governed by `management.endpoints.web.exposure.include`, which exposes only `health` by default: the App side must add `hotkey,hotkeyring`; the Worker side exposes only `health` unless an operator opts in to `hotkeyworker` plus `management.endpoints.web.path-mapping.hotkeyworker: hotkey-worker` (Boot 3.5 endpoint ids must be alphanumeric, while the public URL stays hyphenated).

Full response shapes and field descriptions are in [MONITOR.md](docs/MONITOR.md).

## License

Apache License 2.0
