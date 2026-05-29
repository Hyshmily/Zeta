# 更新日志

所有重要变更均记录在此文件中。

## 1.0.9

- **HotKey Bean 竞态条件修复** — `HotKeyRedisAutoConfiguration` 新增 `@AutoConfiguration(after = {HotKeyAutoConfiguration.class, RedisAutoConfiguration.class})` 和 `hotKey()` bean，确保 Redis 在 classpath 时 `HotKey` 无论自动配置顺序都正常创建。
- **组合硬 + 软 TTL** — 新增 `getWithSoftExpire(key, reader, hardTtlMs, softTtlMs)` 和 `putThrough(key, value, writer, hardTtlMs, softTtlMs)`，一次调用同时设置 Caffeine 硬 TTL 和软过期 TTL。
- **参数重命名** — `ttlMs` → `hardTtlMs`、`redisWriter` → `writer`、`redisMutation` → `mutation`，语义更清晰。
- **`@since` 标签** — `HotKey` 门面所有公开方法添加 `@since` Javadoc 标签，标明引入版本。
- **Javadoc 注释** — `HotKey` 门面所有公开方法添加英文 Javadoc 注释说明用法。
- **许可证头** — 所有 Java 源文件添加 Apache 2.0 版权声明头（`Copyright 2026 Hyshmily`）。
- **拼写修复** — 修复 `HotKey` 门面中 `purThrough` → `putThrough` 拼写错误。
- **核心提取重构** — 从 `HotKeyCache` 和 `BroadcastListener` 中提取出 `CacheKeysPolicy`、`TransactionSupport`、`SingleFlight`、`SoftExpireManager`、`BroadcastMessage` 等独立类。`HotKeyCache` 从约 426 行简化为 277 行。
- **SingleFlight 独立 Bean** — `SingleFlight` 内部持有 inflight dedup Cache；`HotKeyEndpoint` 通过 `SingleFlight.estimatedInflightSize()` 监控。
- **自动配置重组** — 5 个自动配置类统一移到 `io.github.hyshmily.hotkey.autoconfigure` 包下。
- **BroadcastMessage 记录** — 新增 `BroadcastMessage.from(Message)` record 封装 RabbitMQ 消息解析，提取 `isVersionDegraded` 头部。
- **算法与实体包未变动** — `HeavyKeeper`、`TopK`、`CacheEntry`、`HotKeyProperties` 保持原包路径，消费者二进制兼容。

## 1.0.8

- **Per-entry 硬 TTL** — `get(key, reader, ttlMs)` 和 `putThrough(key, value, writer, ttlMs)` 支持为单个 entry 设置 Caffeine 硬 TTL（毫秒）。未传 ttlMs 的调用仍使用全局 `hotkey.local-cache-ttl-minutes`。
- **Per-call 软 TTL** — `getWithSoftExpire(key, reader, softTtlMs)` 支持按调用覆盖全局 `hotkey.soft-ttl-ms`。
- **`VersionedValue` → `CacheEntry`** — 内部缓存条目类型新增 `expireAtMs` 字段，与 `value`、`version` 并列。`VersionedValue` 已删除。
- **`expireAfter(Expiry)`** — Caffeine L1 缓存构建从固定 `expireAfterWrite` 改为 per-entry `Expiry` 回调，支持条目级变量 TTL。
- **委托重构** — `get()`、`putThrough()`、`loadSingleflight()` 的无参重载委托给带 `ttlMs` 的版本，消除重复校验和日志。
- **软过期 TTL 保留** — `triggerAsyncRefresh` 刷新时保留原始 entry 的 `expireAtMs`，不再重置为 `Long.MAX_VALUE`，per-entry 硬 TTL 在后台刷新中保持不变。
- **移除 `instance-id` YAML 配置** — `BroadcastProperties.instanceId` 无 setter（`@Setter(AccessLevel.NONE)`），YAML 中的 `hotkey.broadcast.instance-id` 实际无效，已从配置示例中移除。
- **广播 TTL 保留** — `BroadcastListener.handleVersionedHotKey` 现在保留本地 entry 的 `expireAtMs`，不再重置为 `Long.MAX_VALUE`，per-entry 硬 TTL 在广播更新中保持不变。
- **`HeavyKeeper.contains()` O(1) 覆盖** — 用 `heapIndex.containsKey()` 在 `synchronized(sortedTopK)` 内覆盖 `contains()`，`isHotKey()` 变为真正 O(1)（原先回退到 O(K log K) 的 `list()` 遍历）。
- **`invalidate()` 补入 API 文档** — 单 key `invalidate(cacheKey)` 现在在 API 参考表中列出。
- **README 重构** — 删除过时的已知问题 / Workaround 章节；`putInvalidate` → `putBeforeInvalidate` 修正；架构图补 `contains()`；新增 TTL 参考表；重写软失效章节对比传统逻辑过期。

## 1.0.7

- **TreeMap Top-K** — HeavyKeeper 中用 `TreeMap` + `HashMap` 替代 `PriorityQueue`。所有 Top-K 操作（插入、删除、淘汰）从 O(K) 降为 O(K log K)，消除每次热点访问时的线性 `removeIf` 扫描。
- **平坦桶数组** — 用 `long[] fingerprints` + `int[] counts` 替代 `Bucket[][]` 对象数组，减少每个桶的对象头开销（Count-Min Sketch 内存约减半）。
- **条带锁** — 用 256 个条带锁替代每桶 `synchronized`，锁对象数从 250,000 降至 256。
- **LongAdder 计数器** — 全局请求计数器从 `AtomicLong` 改为 `LongAdder`，减少高并发下的 CAS 竞争。
- **原子广播去重** — `BroadcastPublisher.sendDeduped` 改用 `compute()` 替代 `putIfAbsent` + 条件更新，消除去重竞态窗口。
- **原子版本比较** — `BroadcastListener.handleVersionedHotKey` 改用 `caffeineCache.asMap().compute()` 实现原子版本比较和缓存更新，防止低版本覆盖高版本。
- **非阻塞抖动延迟** — `BroadcastListener` 不再在 RabbitMQ 消费线程上调用 `Thread.sleep()`。抖动延迟改由 `ScheduledExecutorService` 调度，消息在处理前先 ACK，避免阻塞消费。
- **Singleflight 清理** — `loadSingleflight` 现在在 `whenComplete` 中总是清理 inflight 条目，修复 `orTimeout` 可能遗留过期 future 的时序问题。
- **Lua 脚本合并版本操作** — `nextVersion()` 改用单条 Lua 脚本（`INCR` + `EXPIRE`）替代两次 Redis 命令，往返开销减半。
- **`isHotKey()` O(1)** — 新增 `TopK.contains()` 方法，基于 `ConcurrentHashMap` 热 key 集合，替代 O(K log K) 的 `list()` + stream 查找。
- **`get()` 去除冗余 put** — 缓存命中时不再执行不必要的 `caffeineCache.put()`，仅通过 `add()` 跟踪访问频率。
- **`putThrough` 泛型化** — `putThrough` 现在接受 `<T>` 参数，编译期类型安全。
- **`putThrough` 顺序修正** — `redisWriter.run()` 现在在 `nextVersion()` 之前执行，避免写入失败时版本号空洞。
- **`invalidate()` 直接执行** — `invalidate()` 直接执行缓存失效 + 广播，不再走空 Runnable 的 `putInvalidate` 路径。
- **`getRelaxed` → `getWithSoftExpire`** — 软失效读路径命名更直观。
- **调度分离** — `@EnableScheduling` 从 `HotKeyAutoConfiguration` 移除；周期性衰减和驱逐队列清理移至 `HotKeySchedulingConfiguration`，通过 `hotkey.scheduling.enabled` 控制。
- **可配置访问过期** — 新增 `hotkey.local-cache-access-ttl-minutes`（默认 0 = 禁用），L1 补充访问过期策略。
- **广播消费并发数** — 新增 `hotkey.broadcast.concurrent-consumers`（默认 3），支持并行消费广播消息。
- **消息 TTL** — 广播队列新增 60 秒 `x-message-ttl`，实例重启后自动清理过期消息。
- **线程池拒绝日志** — `hotKeyExecutor` 队列满时记录日志并抛出 `RejectedExecutionException`，不再静默失败。
- **Actuator 无副作用** — `/actuator/hotkey` 端点改用 stream 快照读取驱逐 key，不再消耗队列数据。

## 1.0.6

- **版本化缓存失效** — 用 Redis INCR 全局版本号替代广播驱动的缓存失效。`putThrough` 写入 L2、更新本地缓存、附带单调递增版本的广播。对端节点先比较版本再决定是否回源，消除冗余 Redis 加载。
- **异常安全版本降级** — `nextVersion()` 捕获 Redis 异常后降级到 `System.nanoTime()`，写入操作不会被版本生成阻断。
- **版本 key TTL** — 新增 `hotkey.version-key-ttl-minutes`（默认 60 分钟），自动过期 Redis 版本号 key，防止版本键无限膨胀。
- **API 重命名** — `getStale` 更名为 `getRelaxed`，`putInvalidate` 更名为 `putBeforeInvalidate`。
- **`peek()` 版本感知** — 自动解包 `VersionedValue`，对调用方透明。
