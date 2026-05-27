# 更新日志

所有重要变更均记录在此文件中。

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
