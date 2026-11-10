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

Zeta is a lightweight distributed cache and cache-warming framework for Spring Boot. Keys need no pre-registration: it handles hot-key discovery, decision, warming and graceful degradation. A sudden hotspot gets a cluster-wide, eventually consistent cache at minimal cost.

> [!NOTE]
>
> Zeta 1.2.0 is coming soon.

## Local and cluster detection

```java
private final Zeta zeta;

// tag the key inside the shared namespace
zeta.tag("product:123");

// read it
zeta.peek("product:123");
```

- **Local layer (on by default):** every instance runs a HeavyKeeper TopK sketch. As soon as a key enters the TopK set, its L1 Caffeine TTL is extended automatically — no Worker feedback required. An L1 miss coalesces concurrent requests; a soft-expired entry serves the stale value while refreshing in the background.
- **Cluster layer (optional):** per-instance counts are reported to RabbitMQ and evaluated by a Worker cluster. The Worker keeps a sliding window per key and compares its sum with the governing rule's `hotCount`/`coolCount` (regex, prefix or exact rules, precedence-ordered); a matching exact rule decides on its own threshold, everything else is gated by the promotion floor. HOT/COOL decisions are broadcast back to every App instance.

## Multi-node cache coherence

Much like a primary/replica database syncing writes through a log-replication protocol, Zeta propagates cache changes between application instances over a RabbitMQ publish-subscribe channel.

Whenever an instance performs a cluster-synced write (`put(key, value, PutPolicy.synced())`) or an invalidation, each node's local, lock-free `StampClock` assigns a `dataVersion` per key and broadcasts the result to every peer. A synced write carries the serialized value, so a peer compares versions and applies it directly, with no read-back to the source: one send converges the whole cluster, including instances that hold no related key at all.

Version comparison delivers eventual consistency without paying for a consensus protocol such as Paxos or Raft.

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
    hard-ttl-ms: 300000 # normal key: Caffeine eviction
    soft-ttl-ms: 30000 # normal key: stale-while-revalidate
    hot-hard-ttl-ms: 3600000 # Local-Hot key
    hot-soft-ttl-ms: 300000 # Local-Hot key
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
    instance-id: ${HOSTNAME} # queue name and node-id derive from it
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
| `V reload(K key)`          | where a background refresh reads it — never on the original call stack          | `load`   |
| `boolean isCacheable(V v)` | whether a value the engine obtained on the loader's behalf may be written to L1 | `true`   |

```java
// reader — the convenience form: anything non-null is cached
User a = zeta.get("user:1", () -> repo.findUser(1), GetPolicy.defaults());

// loader — the same call plus an admission decision
CacheLoader<String, User> loader = new CacheLoader<>() {
  @Override
  public User load(String key) {
    return repo.findUser(key);
  }

  @Override
  public boolean isCacheable(User v) {
    return !v.isPlaceholder();
  }
};

User b = zeta.get("user:1", loader, GetPolicy.defaults());
```

| Consumer                                           | Uses it to                                                 | Without it                                                                                     |
| -------------------------------------------------- | ---------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `WorkerListener` → `DefaultWorkerContext`  | load the authoritative value before promoting a key to HOT | promotes what L1 already holds; the HOT is skipped when no entry is usable (`VALUE_NOT_FOUND`) |
| `CacheSyncListener` → `DefaultSyncContext` | reload after a peer `REFRESH`                              | degrades to a local invalidation — the next read goes back through your reader                 |
| the same context's value-carrying `PUT`            | validate the value carried on the wire with `isCacheable`  | no loader, no validation: the carried value is applied as-is                                   |

**Custom per-entry TTL**

Zeta uses **differentiated TTLs**: hot keys and normal keys each have their own defaults.

| Key state | Hard TTL (Caffeine eviction) | Soft TTL (stale-while-revalidate) |
| --------- | ---------------------------- | --------------------------------- |
| Normal    | `hard-ttl-ms` (5 min)        | `soft-ttl-ms` (30 s)              |
| Hot       | `hot-hard-ttl-ms` (1 h)      | `hot-soft-ttl-ms` (5 min)         |

> [!NOTE]
> **Cache-avalanche protection:** `TtlPolicy` applies a uniformly random offset to every expiry timestamp through its internal `computeTtlJitter()` (±5% by default). A 5-minute hard TTL actually expires between 4.75 and 5.25 minutes. Controlled by `zeta.local.ttl-jitter-ratio` (a ratio, `0.05` = ±5% by default, `0` disables it).

## Worker listener

**Observe broadcast decisions through hooks**

Implement `WorkerDecisionHook` or `SyncHook` to observe how broadcasts are applied, without touching the core logic:

```java
@Bean
public WorkerDecisionHook myMonitorHook() {
  return new WorkerDecisionHook() {
    @Override
    public void afterHotPromotion(WorkerDecision decision, CacheEntry entry) {
      metrics.counter("hot.promotion").increment();
    }

    @Override
    public void onHotSkipped(WorkerDecision decision, HotSkipReason reason) {
      log.warn("HOT skipped {}: {}", decision.cacheKey(), reason);
    }
  };
}
```

| Hook                 | Method               | Fires when                                                                                                                                                        |
| -------------------- | -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WorkerDecisionHook` | `afterHotPromotion`  | a key is promoted to HOT (extended TTL)                                                                                                                           |
| `WorkerDecisionHook` | `afterCoolDowngrade` | a key returns to the normal TTL                                                                                                                                   |
| `WorkerDecisionHook` | `onHotSkipped`       | a HOT decision is skipped (`VERSION_STALE` stale version / `VALUE_NOT_FOUND` no usable value / `POLICY_REJECTED` custom refusal)                                  |
| `WorkerDecisionHook` | `onCoolSkipped`      | a COOL decision is skipped (no matching entry in L1, stale version, or covered by a newer COOL in the same run)                                                   |
| `SyncHook`           | `afterRefresh`       | a peer `REFRESH` read-back or a value-carrying `PUT` was applied                                                                                                  |
| `SyncHook`           | `afterInvalidate`    | a key was removed from L1 because of a peer broadcast                                                                                                             |
| `SyncHook`           | `onRefreshSkipped`   | a REFRESH or PUT did not take effect (stale version, missing value, rejected by the invalidation watermark, or overwritten by a higher version in the same batch) |

**Replace the default broadcast handling**

```java
@Bean
@ConditionalOnMissingBean(WorkerPolicy.class)
public WorkerPolicy myPolicy() {
  return new WorkerPolicy() {
    @Override
    public HotAction hot(WorkerDecision decision, HotValue value) {
      return myPolicy(value.value())
        ? new HotAction.Promote(value)
        : new HotAction.Skip(HotSkipReason.POLICY_REJECTED);
    }

    // cool keeps the default: return true
  };
}
```

| Interface      | Methods                                                                                                                | Framework owns                                                                                         |
| -------------- | ---------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `WorkerPolicy` | `hot(WorkerDecision, HotValue)`, `cool(WorkerDecision)` — both default to the standard pipeline                        | per-key dispatch, `resolveHot`/`settleHot`/`cool`/`skip`, version/tombstone guards, hooks              |
| `SyncPolicy`   | `value(Batch.Value, SyncContext)`, `invalidate(Batch.Value, SyncContext)`, `clear(Batch.Clear, SyncContext)`           | batch reduction, `value`/`invalidate`/`clear`/`skip`, version/tombstone guards, clear watermarks, hooks |

## Annotations

| Annotation        | Target | Effect                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| ----------------- | ------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `@ZetaCached`     | M      | Read-through: a hit returns immediately without executing the method body; a miss executes it once and stores the non-`null` result. The body is the loader, so soft expiry (stale-while-revalidate) and SingleFlight coalescing apply as well. Key and TTLs resolve before the body, so `#result` is unavailable; unknown SpEL variables, reserved parameter names (`root`, `this`, `result`, `pN`, `aN`) when the method compiles an expression, and digits-only TTLs outside `long` fail startup.  |
| `@ZetaPut`        | M      | Write-through: executes the body, then stores the non-`null` result. A key that does not read `#result` resolves before the body (a null argument or null key fails before the transaction); a synced dynamic hard TTL is pre-checked before the body for the permanent+sync rule. Other TTLs and `unless` resolve after the body, once the result is non-null and not vetoed. `sync = false` writes this instance only.                                                                              |
| `@ZetaInvalidate` | M      | Removes entries: by default after the body, so the key may use `#result`. A key that does not read `#result` resolves before the body. A key that reads `#result` is evaluated by SpEL: use `#result?.id` when the body may return `null` (a null key is a no-op), while a plain `#result.id` on a null result fails; an expression with its own null fallback still runs. `beforeInvocation = true` removes before the body (where `#result` is unavailable). `scope` selects local, remote or both. |

### `@ZetaCached`

```java
// 1. Default key and configuration placeholders (basic read)
@Service
public class ProductService {

  // cache supports ${...} placeholders (with defaults), substituted once at method resolution
  // key omitted: a composite key is generated from the canonical encoding of (categoryId, page, size)
  @ZetaCached(cache = "${app.cache.catalog-namespace:catalog}")
  public PageResult<Product> listProducts(Long categoryId, int page, int size) {
    return productRepository.findByCategory(categoryId, page, size);
  }
}

// 2. Soft expiry plus a dynamic TTL (stale-while-revalidate)
@Service
public class RankingService {

  // VIP and normal users get separate rankings: VIP 120 s, normal 60 s (milliseconds)
  // softTtl=10s: once soft-expired the old ranking is served immediately while a single
  // background task recomputes it
  // The background refresh invokes the method body directly, bypassing transaction and security
  // interceptors; do not set softTtl on methods that depend on request-scoped state
  @ZetaCached(
    cache = "rankings",
    key = "#region + ':' + #isVip",
    softTtl = "10000",
    hardTtl = "#isVip ? 120000 : 60000"
  )
  public RankList getTopRankings(String region, boolean isVip) {
    return rankingCalculator.calculateTop100(region, isVip);
  }
}

// 3. Conditional veto after the read (unless)
@Service
public class OrderService {

  // unless is evaluated after the body returns: #result is the return value; when true the value
  // is still returned to the caller but not written to the cache
  // A null return is never cached anyway; PENDING orders are additionally vetoed by unless, so
  // the next read goes through to the database again
  @ZetaCached(cache = "orders", key = "#orderId", unless = "#result == null || #result.status == 'PENDING'")
  public Order getOrder(String orderId) {
    return orderRepository.findById(orderId);
  }
}
```

### `@ZetaPut`

```java
// 4. Write back a generated primary key after insert (@ZetaPut + #result)
@Service
public class AccountService {

  // A key that reads #result (here) resolves after the body returns, so it can read the
  // database-generated id through #result.id; a key that does not read #result resolves before
  // the body and cannot fail after the transaction commits. TTLs are evaluated when the write
  // happens; a synced dynamic hard TTL is pre-checked before the body for the permanent+sync rule.
  // sync = true (the default): the written value is broadcast to peers over the messaging channel,
  // and peers apply it directly without reading back from the source
  @ZetaPut(cache = "accounts", key = "#result.id", hardTtl = "3600000")
  public Account createAccount(AccountDraft draft) {
    Account account = new Account(draft.getUsername(), draft.getEmail());
    accountRepository.insert(account); // account.id is populated on return
    return account;
  }
}

// 5. Local-only write (no cluster broadcast)
@Service
public class SessionService {

  // sync = false: only this node's L1 is updated and no sync message is sent; peers keep their
  // own copy until hardTtl expires
  @ZetaPut(cache = "user_sessions", key = "#token", sync = false, hardTtl = "1800000")
  public UserSession refreshLocalSession(String token) {
    return sessionClient.fetchRemoteSession(token);
  }
}
```

### `@ZetaInvalidate`

```java
// 6. Invalidate before the method runs (pre-invocation @ZetaInvalidate)
@Service
public class InventoryService {

  // Default is post-invocation: the cache is left untouched when the method throws.
  // beforeInvocation = true removes before the body, where the key cannot use #result
  // scope = BOTH (the default): remove the local copy and tell remote nodes to remove theirs too
  @ZetaInvalidate(cache = "inventory", key = "#skuId", beforeInvocation = true, scope = InvalidatePolicy.Scope.BOTH)
  public void deductStock(Long skuId, int quantity) {
    inventoryRepository.decrease(skuId, quantity);
  }
}

// 7. Clear only the remote copies (@ZetaInvalidate with an explicit scope)
@Service
public class BroadcastService {

  // scope = REMOTE: this node already refreshed through another path (such as binlog replay), so
  // only peers need to drop their copies
  // Post-invocation by default, so the key may use #result; write "#result?.id" when the body may
  // return null (a null key skips this removal), while a fallback expression such as
  // "#result != null ? #result.id : #fallbackId" still runs
  @ZetaInvalidate(cache = "config", key = "#result.configKey", scope = InvalidatePolicy.Scope.REMOTE)
  public ConfigItem updateRemoteCluster(ConfigUpdateForm form) {
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
