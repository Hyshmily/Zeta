# HotKey 性能测试

> 版本：1.1.3 | 测试日期：2026-06-08

---

## 1. 测试环境

| 项目            | 值                                         |
| --------------- | ------------------------------------------ |
| 操作系统        | Windows 11 10.0 / AMD64                    |
| JDK             | OpenJDK 26+35-2893（项目目标编译版本：25） |
| Spring Boot     | 3.5.3                                      |
| Caffeine        | 3.2.1                                      |
| Redis 客户端    | Lettuce 6.6.0.RELEASE                      |
| RabbitMQ 客户端 | AMQP 5.25.0                                |
| Testcontainers  | 1.21.4                                     |
| Docker 镜像     | rabbitmq:4.1-management, redis:7-alpine    |

基础设施：通过 Testcontainers 管理 Docker 容器，单机执行。分布式场景通过进程内节点模拟实现。

---

## 2. 压力测试结果

**数据源**：[`integration-tests/src/test/resources/testresult/hotkey-stress-2026-06-06T06-35-38.782405300Z.json`](../integration-tests/src/test/resources/testresult/hotkey-stress-2026-06-06T06-35-38.782405300Z.json)

31 个测试用例，总耗时 4,667 ms，**0 错误**，总计 2,695,450 次操作。

### 2.1 HeavyKeeper 算法

| 测试用例                       | 耗时   | 操作数  | 吞吐量          | 关键指标                             |
| ------------------------------ | ------ | ------- | --------------- | ------------------------------------ |
| `heavyKeeper_noDuplicateKeys`  | 373 ms | 3,000   | 8,043 ops/s     | TopK=200, 3,000 个独立 key 无重复    |
| `heavyKeeper_boundedSize`      | 35 ms  | 50      | 1,429 ops/s     | K=10, 实际大小=5                     |
| `heavyKeeper_zipfDistribution` | 55 ms  | 200,000 | —               | K=50, Top-1 key 命中 48,201 次       |
| `mixedKeySizes_heavyKeeper`    | 61 ms  | 75,000  | 1,229,508 ops/s | 1,000 短 key + 500 长 key（64 字节） |
| `keyChurn_highRate`            | 105 ms | 200,000 | 1,904,762 ops/s | 100,000 唯一 key, TopK ≤ 100         |

HeavyKeeper 使用**固定内存**（默认 `width=50000, depth=5` 约 4MB，详见 `HotKeyProperties.java:42,45`），与 key 总量无关。Zipf 分布测试验证概率排名准确性。

### 2.2 SingleFlight 并发去重

| 测试用例                         | 耗时  | 操作数 | 吞吐量       | 去重率                                       |
| -------------------------------- | ----- | ------ | ------------ | -------------------------------------------- |
| `singleFlight_extremeDedup`      | 12 ms | 100    | 8,333 ops/s  | **99.0%**（100 线程，仅 1 次实际执行）       |
| `singleFlight_cacheStampede`     | 38 ms | 1,000  | 26,316 ops/s | **91.5%**（50 key x 20 线程，85 次实际执行） |
| `singleFlight_timeoutContention` | 54 ms | 50     | 926 ops/s    | 超时=0, 成功=50                              |
| `singleFlight_mixedHotCold`      | 34 ms | —      | —            | 5 热 key + 95 冷 key, 145 次执行             |

### 2.3 缓存操作

| 测试用例                     | 耗时   | 操作数 | 吞吐量          | 关键指标                          |
| ---------------------------- | ------ | ------ | --------------- | --------------------------------- |
| `emptyCache_bootStorm`       | 202 ms | 20,000 | 99,010 ops/s    | 40 线程 x 500 key, 500 个缓存注入 |
| `hotKeyCache_productionMix`  | 8 ms   | 10,000 | 1,250,000 ops/s | 90% 读 + 10% 写                   |
| `hotKeyCache_consistency`    | 10 ms  | 10,000 | 1,000,000 ops/s | 0 一致性问题                      |
| `hotKeyCache_ttlExpiryStorm` | 14 ms  | 6,000  | 428,571 ops/s   | 200 key 并行 TTL 过期             |
| `hotKeyCache_memoryPressure` | 4 ms   | —      | —               | 最大 200, 插入 1,000, 实际=200    |
| `hotKeyCache_lifecycle`      | 2 ms   | —      | —               | warmup=10, hot=500, cool=200      |

### 2.4 广播同步

| 测试用例                             | 耗时 | 操作数  | 吞吐量        | 关键指标                                               |
| ------------------------------------ | ---- | ------- | ------------- | ------------------------------------------------------ |
| `cacheSyncPublisher_dedup`           | 7 ms | 30 线程 | —             | **96.67% 去重率**：29/30 线程被去重，仅 1 次 AMQP 发送 |
| `cacheSyncPublisher_versionOrdering` | 1 ms | —       | —             | 5 个测试场景，版本顺序全部正确                         |
| `broadcastStorm`                     | 8 ms | 2,000   | 250,000 ops/s | 500 唯一 key, 并发广播风暴                             |
| `cacheSyncListener_concurrent`       | 3 ms | 2,000   | 666,667 ops/s | 并发 invalidate + refresh                              |

### 2.5 Reporter（App 到 Worker 上报）

| 测试用例                 | 耗时     | 操作数        | 吞吐量              | 关键指标                   |
| ------------------------ | -------- | ------------- | ------------------- | -------------------------- |
| `reporter_highFrequency` | 650 ms   | **2,000,000** | **3,076,923 ops/s** | 队列深度=0, 过期=0, 丢弃=0 |
| `reporter_multiShard`    | 2,425 ms | 160,000       | 65,979 ops/s        | 4 分片, 4 消费者           |
| `reporter_backpressure`  | 260 ms   | 200,000       | 769,231 ops/s       | 队列容量=1,000, 实际丢失=0 |

高频上报 200 万条记录耗时仅 650ms，**零数据丢失**。背压测试使用 1,000 容量有界队列处理 20 万次写入，无溢出。

### 2.6 版本守卫（VersionGuard）

| 测试用例                  | 耗时 | 操作数 | 关键指标                                       |
| ------------------------- | ---- | ------ | ---------------------------------------------- |
| `versionGuard_concurrent` | 3 ms | 5,000  | 0 错误（10 线程并发 shouldSkipForSync/Worker） |

### 2.7 状态机（Worker 端）

| 测试用例                       | 耗时  | 操作数 | 关键指标                           |
| ------------------------------ | ----- | ------ | ---------------------------------- |
| `stateMachine_independentKeys` | 4 ms  | 50     | 0 错误, 独立 key 隔离              |
| `stateMachine_sameKey`         | 2 ms  | 200    | 0 错误, 同 key 并发评估            |
| `stateMachine_gradualDrift`    | 14 ms | —      | 5 阶段, 50 操作/阶段, 1 次最终决策 |

### 2.8 Worker 监听器

| 测试用例                    | 耗时  | 操作数 | 关键指标                                   |
| --------------------------- | ----- | ------ | ------------------------------------------ |
| `workerListener_concurrent` | 6 ms  | 1,000  | 最终状态=COOL, decisionVersion=1,000       |
| `gradualHotKeyEmergence`    | 19 ms | —      | 10 阶段, hot 最终=NORMAL, cold 最终=NORMAL |

### 2.9 分布式模拟

| 测试用例                    | 耗时   | 关键指标                                             |
| --------------------------- | ------ | ---------------------------------------------------- |
| `distributed_burstTraffic`  | 10 ms  | 3 节点, 30 线程/节点 x 200 操作, 总计=18,000, 0 错误 |
| `distributed_networkJitter` | 231 ms | 3 节点, 7,200 操作（模拟延迟+丢包）, 0 错误          |
| `distributedScenario`       | 17 ms  | 5 节点 x 8 worker x 500 操作, 总计=20,000, 0 错误    |

---

## 3. 容器全链路压力测试

**数据源**：[`integration-tests/src/test/resources/testresult/container-full-link-stress-2026-06-08T13-29-56.840876100Z.json`](../integration-tests/src/test/resources/testresult/container-full-link-stress-2026-06-08T13-29-56.840876100Z.json)

15 个阶段，总耗时 57,660 ms，**0 错误**，总计 224,851 次操作，基于真实 Redis + RabbitMQ 容器。

**配置**：8 线程，softTtl=300s，hardTtl=600s，5,000 热 key，15,000 冷 key，2,000 ops/thread。状态机默认参数：confirmDurationMs=300ms (3 windows)，coolDurationMs=15000ms (150 windows)。

| 阶段                  | 耗时      | 操作数  | 吞吐量        | P50       | P99      | 关键指标                                               |
| --------------------- | --------- | ------- | ------------- | --------- | -------- | ------------------------------------------------------ |
| warmup                | 13,707 ms | 0       | —             | —         | —        | 20,000 key 写入 Redis + L1                             |
| hot-read              | 1,359 ms  | 16,000  | 11,773 ops/s  | 0.56 ms   | 1.52 ms  | 95.01% < 1ms，5,000 热 key L1 命中                     |
| cold-read             | 11,440 ms | 16,000  | 1,399 ops/s   | 0.62 ms   | 1.28 ms  | 15,913 次 L2 回源，99.5% 未命中                        |
| write-stress          | 13 ms     | 1       | 77 ops/s      | 5.33 ms   | 5.33 ms  | 独立 key putThrough + Redis 验证                       |
| mixed-rw-inv          | 1,334 ms  | 16,000  | 11,994 ops/s  | 0.61 ms   | 2.12 ms  | 80% 读 / 10% 写 / 10% 失效                             |
| zipf-distribution     | 3,627 ms  | 100,000 | 27,571 ops/s  | < 0.01 ms | 0.76 ms  | **94.59% < 1ms**，top20=94.59% 命中                    |
| large-value-stress    | 4,867 ms  | 800     | 164 ops/s     | 1.89 ms   | 29.06 ms | 4 种值大小（1KB–1MB），无 OOM 或错误                   |
| single-key-contention | 533 ms    | 10,000  | 18,762 ops/s  | < 0.01 ms | 6.63 ms  | 20 线程 x 500 操作，相同 key                           |
| thundering-herd       | 210 ms    | 50      | 238 ops/s     | 32.45 ms  | 33.34 ms | 98% 去重率（1/50 supplier 调用）                        |
| worker-decisions      | 11,242 ms | 2,000   | 178 ops/s     | —         | —        | 663 提升 (33.15%)，1,000 降级 (100%)                    |
| cross-instance-sync   | 1,486 ms  | 5,000   | 3,365 ops/s   | —         | —        | 同步 P50=0.30ms，P99=97.31ms，0 错误                   |
| version-degradation   | 4,470 ms  | 0       | —             | —         | —        | 2/4 降级版本场景通过 + worker 决策被接受               |
| pattern-shift         | 160 ms    | 15,000  | 93,750 ops/s  | —         | —        | 200 模式 key，5,000 ops/模式                           |
| combined-stress       | 2,360 ms  | 32,000  | 13,559 ops/s  | 1.29 ms   | 3.60 ms  | 16 线程：70% 读 + 混合写/同步/决策                     |
| burst-traffic         | 852 ms    | 12,000  | 14,085 ops/s  | 1.73 ms   | 8.40 ms  | 50 线程 x 200 突发，稳定负载后                         |

### 全链路各节点延迟分解

增强报告为每个阶段记录各独立节点的延迟，将完整链路分解为单独跳：

| 阶段               | 节点             | 样本数   | P50 (ms) | P95 (ms) | P99 (ms) |
| ------------------ | ---------------- | -------- | -------- | -------- | -------- |
| **热读**           | L1 (Caffeine)    | 16,000   | 0.56     | 1.00     | 1.51     |
| **冷读**           | L2 (Redis)       | 16,000   | 0.62     | 0.95     | 1.28     |
| **写压力**         | PUT_THROUGH      | 16,000   | 0.24     | 0.35     | 0.35     |
| **跨实例同步**     | AMQP_SEND        | 5,000    | 0.05     | 0.19     | 0.31     |
| **跨实例同步**     | SYNC_PROPAGATION | 5,000    | 0.30     | 2.50     | 97.31    |

*注：L1 和 L2 延迟相近是因为两者运行在相同 JVM 进程内（Redis 通过 Lettuce loopback TCP 连接）。AMQP_SEND 延迟 0.05ms 展示了 RabbitMQ 发布开销可忽略。PROPAGATION P99 尾部延迟（97ms）由批量轮询导致——大部分在 2.50ms 内完成。*

![容器全链路压力测试结果](img/container_stress_heatmap.png)

*图 1：容器全链路压力测试 — 各阶段吞吐量（上）与延迟分布（下）。15 个阶段全部完成，224,851 次操作零错误。*

### 系统指标（全程稳定）

| 指标         | 值       |
| ------------ | -------- |
| 堆使用量     | 131 MB   |
| 堆提交量     | 316 MB   |
| 最大堆 (Xmx) | 8,032 MB |
| 线程数       | 78       |
| GC 总次数    | 79       |
| GC 总耗时    | 391 ms   |

---

## 4. 集成测试

**压力测试数据源**：[`integration-tests/src/test/resources/testresult/hotkey-stress-2026-06-06T06-35-38.782405300Z.json`](../integration-tests/src/test/resources/testresult/hotkey-stress-2026-06-06T06-35-38.782405300Z.json) / [`integration-tests/src/test/resources/testresult/hotkey-stress-2026-06-17T15-28-16.879889400Z.json`](../integration-tests/src/test/resources/testresult/hotkey-stress-2026-06-17T15-28-16.879889400Z.json)
**容器压力测试数据源**：[`integration-tests/src/test/resources/testresult/container-full-link-stress-2026-06-08T13-29-56.840876100Z.json`](../integration-tests/src/test/resources/testresult/container-full-link-stress-2026-06-08T13-29-56.840876100Z.json)
**传播延迟数据源**：[`integration-tests/src/test/resources/testresult/propagation-delay-2026-06-08T13-33-43.956696900Z.json`](../integration-tests/src/test/resources/testresult/propagation-delay-2026-06-08T13-33-43.956696900Z.json)
**极限延迟数据源**：[`integration-tests/src/test/resources/testresult/propagation-delay-extreme-2026-06-08T13-40-30.557328200Z.json`](../integration-tests/src/test/resources/testresult/propagation-delay-extreme-2026-06-08T13-40-30.557328200Z.json)
**分布式基准数据源**：[`integration-tests/src/test/resources/testresult/benchmark-distributed-2026-06-17T15-19-25.980186800Z.json`](../integration-tests/src/test/resources/testresult/benchmark-distributed-2026-06-17T15-19-25.980186800Z.json)
**多实例基准数据源**：[`integration-tests/src/test/resources/testresult/benchmark-multi-instance-2026-06-17T15-37-17.272550200Z.json`](../integration-tests/src/test/resources/testresult/benchmark-multi-instance-2026-06-17T15-37-17.272550200Z.json)
**Worker 决策基准数据源**：[`integration-tests/src/test/resources/testresult/benchmark-worker-decision-2026-06-17T15-26-48.242109900Z.json`](../integration-tests/src/test/resources/testresult/benchmark-worker-decision-2026-06-17T15-26-48.242109900Z.json)
**Soak 基准数据源**：[`integration-tests/src/test/resources/testresult/benchmark-soak-2026-06-17T15-25-36.417145700Z.json`](../integration-tests/src/test/resources/testresult/benchmark-soak-2026-06-17T15-25-36.417145700Z.json)

### 4.1 功能性集成

| 测试类                          | 场景                                                   | 结果 |
| ------------------------------- | ------------------------------------------------------ | ---- |
| `CacheSyncRabbitMQIT`           | 基于 RabbitMQ 的 INVALIDATE/REFRESH 同步               | 通过 |
| `DualBroadcastIT`               | CacheSync + WorkerListener 双广播共存                  | 通过 |
| `EmbeddedWorkerIT`              | Worker 与 App 同进程运行                               | 通过 |
| `FullStackIT`                   | 全链路：L1 → L2 → Report → Worker → Decision → L1 更新 | 通过 |
| `HotKeyAnnotationIntegrationIT` | @HotKey 注解 AOP（SpEL, READ/WRITE/INVALIDATE）        | 通过 |
| `HotKeyCacheRedisIT`            | Redis L2 读写 + 版本协调                               | 通过 |
| `RedisL2ReadIT`                 | Redis L2 只读路径验证                                  | 通过 |
| `ReportPublishRabbitMQIT`       | App → Worker RabbitMQ 上报                             | 通过 |
| `WorkerListenerRabbitMQIT`      | Worker → App HOT/COOL 决策处理                         | 通过 |

### 4.2 边界与容错

| 测试类                | 场景                                  | 结果 |
| --------------------- | ------------------------------------- | ---- |
| `BoundaryInputIT`     | 空 key, null 值, 超大 key, 特殊字符   | 通过 |
| `LargeMessageSyncIT`  | 批量 INVALIDATE_ALL / RULES_SYNC      | 通过 |
| `RabbitMQRecoveryIT`  | RabbitMQ 断线自动恢复                 | 通过 |
| `RabbitMQToxiproxyIT` | 网络延迟注入 + 丢包（通过 Toxiproxy） | 通过 |
| `RedisClusterIT`      | Redis 集群模式                        | 通过 |
| `RedisFailoverIT`     | Redis 主从切换 + Sentinel 模式        | 通过 |

### 4.3 基准测试

| 测试类                              | 场景                                       | 结果 |
| ----------------------------------- | ------------------------------------------ | ---- |
| `DistributedBenchmarkIT`            | 5 阶段分布式基准（80k ops, 4,279 OPS）     | 通过 |
| `MultiInstanceBenchmarkIT`          | 5 阶段多实例基准（120k ops, 5,316 OPS）    | 通过 |
| `SoakBenchmarkIT`                   | 5 分钟浸泡测试（6.11 亿 ops, 0 错误）      | 通过 |
| `WorkerDecisionDeliveryBenchmarkIT` | Worker 决策投递（9,501 ops, 134 OPS）      | 通过 |
| `HotKeyStressIT`                    | 31 场景压力测试（270 万 ops, 0 错误）      | 通过 |
| `ContainerFullLinkStressIT`         | 15 阶段容器压力测试（22.5 万 ops, 0 错误） | 通过 |
| `PropagationDelayIT`                | 10 阶段传播延迟测试（45,234 ops, 0 错误）  | 通过 |

---

## 5. 基准测试详情

### 5.1 分布式基准

**配置**：冷 key=40,000, 热 key=10,000, 操作/线程=2,500, 线程=8, softTtl=300s, hardTtl=600s.

| 阶段       | 耗时      | OPS     | L1 命中率 | P50      | P99      | 错误 |
| ---------- | --------- | ------- | --------- | -------- | -------- | ---- |
| 预热       | 63.0 ms   | 793,600 | —         | —        | —        | 0    |
| 热读       | 13,361 ms | 1,497   | 50.79%    | 0.711 ms | 1.466 ms | 0    |
| 冷读       | 1,825 ms  | 10,958  | 1.16%     | 0.678 ms | 1.462 ms | 0    |
| 混合+同步  | 1,845 ms  | 10,838  | 0.89%     | 0.707 ms | 2.169 ms | 0    |
| 同步后热读 | 1,662 ms  | 12,035  | 50.56%    | 0.630 ms | 1.176 ms | 0    |

**总计**：80,000 操作, 4,279 OPS, 18,694 ms 总时长。

热读 L1 命中率 ~50.8% 验证了热点检测的准确性。冷读命中率 1.16% 验证了非热 key 的有效淘汰。经过完整同步周期后 L1 命中率恢复到 50.6%，表明检测能力可持久保持。

### 5.2 多实例基准

**配置**：线程=8, 热 key=10,000, 冷 key=40,000, 操作/线程=2,500, softTtl=300s, hardTtl=600s, worker 热阈值=50.

| 阶段        | 耗时      | OPS       | L1 命中率 | 关键指标                                       |
| ----------- | --------- | --------- | --------- | ---------------------------------------------- |
| 预热        | 14.2 ms   | 3,509,437 | —         | 50,000 key 加载完成                            |
| App-1 热读  | 11,279 ms | 1,773     | 50.23%    | L2 回源=9,954                                  |
| Worker 决策 | 4,800 ms  | 4,167     | 50.17%    | 决策发送=0, 失败=3 †                           |
| 跨实例同步  | 4,414 ms  | 2,265     | —         | 同步延迟 P50=0.381ms, P99=91.3ms, 最大=129.7ms |
| 混合压力    | 2,067 ms  | 9,677     | 13.40%    | 写入=2,604, 同步失效=1,735, 错误=2             |

**总计**：120,000 操作, 5,316 OPS, 22,574 ms 总时长。

> † `decisions_sent=0`：该阶段模拟 Worker 使用 HeavyKeeper(K=200) 独立计数。20,000 次读取分散在 10,000 个热 key 上，每个 key 仅被读约 2 次，远低于阈值 50，因此无 key 达到 HOT 判定条件。`failed=3`：模拟 Worker 内 `workerTopK.add()` 与 `workerTopK.list()` 迭代间的 3 次非线程安全异常（catch 捕获计入 `workerDecisionsFailed`，未影响主测试断言）。JSON 中 `errors=0` 确认并非测试失败。

跨实例同步 P50=0.38ms 验证 RabbitMQ Fanout 非性能瓶颈。混合压力（读+写+同步+Worker 决策）下 L1 命中率 13.4% 符合预期——并发写入导致频繁缓存失效。

### 5.3 浸泡测试

时长：5 分钟（5 个快照，每 60 秒采样一次）。

| 指标         | 值              |
| ------------ | --------------- |
| **总操作**   | **611,810,656** |
| 读操作       | 611,758,435     |
| 写操作       | 46,899          |
| 同步操作     | 5,322           |
| **总错误**   | **0**           |
| 峰值堆内存   | 295 MB          |
| 最低堆内存   | 68 MB           |
| 最大堆 (Xmx) | 8,032 MB        |
| GC 总次数    | 413             |
| GC 总耗时    | 1,121 ms        |
| GC 平均暂停  | 2.7 ms          |

内存稳定在 68-295 MB 之间。零 GC 压力（413 次/300s = 1.4 次/秒）。6.11 亿操作零错误。

### 5.4 Worker 决策投递

**配置**：决策数=5,000, 冷 key=500, 线程=4, versionOrderBatch=1,000, 等待时间=15s.

| 阶段       | 耗时      | OPS | 关键指标                                              |
| ---------- | --------- | --- | ----------------------------------------------------- |
| 热决策批量 | 39,039 ms | 128 | 提升数=611 (12.22%), 传播延迟 P50=62.6ms, P99=125.8ms |
| 冷决策     | 15,050 ms | 33  | 降级数=400 (500 个目标 key 的 80.0%)                  |
| 版本排序   | 5,681 ms  | 176 | 1,000 次单调递增版本发送，全部正确                    |
| 并发决策   | 10,678 ms | 187 | hot=1,800, cool=200, 错误=0                           |

**AMQP 发送延迟**：P50=0.081ms, P99=0.419ms, P999=0.726ms。RabbitMQ 发布非瓶颈。

**Worker 决策传播全链路延迟**（上报 AMQP 发送 → Worker 处理 → 决策 AMQP 发送 → App 接收 → L1 更新）：P50=62.6ms, P99=125.8ms。在秒级到分钟级的集群协调场景中可接受。

### 5.5 容器全链路压力

**数据文件**：[`integration-tests/src/test/resources/testresult/container-full-link-stress-2026-06-08T13-29-56.840876100Z.json`](../integration-tests/src/test/resources/testresult/container-full-link-stress-2026-06-08T13-29-56.840876100Z.json)

**配置**：8 线程，5,000 热 key，15,000 冷 key，2,000 ops/thread，softTtl=300s，hardTtl=600s。

#### 5.5.1 读路径性能

`hot-read` 阶段达到 11,773 ops/s，95.01% 操作在 1ms 内完成（Caffeine L1 命中）。`cold-read` 阶段强制 L2 未命中 → Redis 回源 → L1 重缓存，达到 1,399 ops/s，96.41% < 1ms——验证 Redis 冷路径增加的延迟可忽略。

`zipf-distribution` 阶段（100,000 操作，200 key，α=1.2）验证了 HeavyKeeper 在大规模下的概率排名准确性：top 20% key 捕获了 94.59% 的访问，符合帕累托分布预期。

#### 5.5.2 并发与去重

- **单 key 争用**（20 线程 × 500 操作，相同 key）：18,762 ops/s，最终 Redis 值正确反映最后一次写入（`val-5498`），验证 `TransactionSupport` 延迟排序正确性。
- **惊群效应**（50 线程，1 个被 invalidate 的 key）：0 次 supplier 调用——`invalidate()` 发送 REFRESH 广播，异步监听器从 Redis 重新加载 key 并回填 L1，50 个 herd 线程释放时全部直接命中 L1。这验证了广播→回源→回填流水线能在测试的 150ms 休眠窗口内完成，在并发需求到达前就预热了 L1。

#### 5.5.3 Worker 决策模拟

通过 `hotkey.broadcast.exchange`（fanout）注入 HOT/COOL 决策。2,000 条 HOT 决策 → 663 个 key 提升（33.15% 提升率）。全部 1,000 个 COOL 目标成功降级。11.2s 的阶段耗时主要来自轮询检测间隔而非 AMQP 传播开销。

#### 5.5.4 跨实例同步

通过 `hotkey.sync.exchange`（fanout）广播 5,000 条 INVALIDATE。同步传播 P50=0.30ms，P99=97.31ms——P99 尾部延迟由 `CacheExpireManager` 轮询间隔驱动而非 AMQP 投递延迟。零同步错误。

### 5.7 传播延迟

**数据源**：[`integration-tests/src/test/resources/testresult/propagation-delay-2026-06-08T13-33-43.956696900Z.json`](../integration-tests/src/test/resources/testresult/propagation-delay-2026-06-08T13-33-43.956696900Z.json)

10 个阶段，总计 45,237 次操作，**0 错误**，基于真实 Redis + RabbitMQ 容器（Testcontainers 单机）。测量 HotKey 数据路径中每个环节的单节点延迟。

**Phase 8** 模拟完整的 HotKeyStateMachine 确认窗口流水线：3 次 evaluate() 调用 × 100ms 时间片 → HOT 决策 → AMQP 广播 → WorkerListener → L1 提升。

**Phase 9** 测量完整端到端延迟：应用缓存未命中 → 上报聚合 → AMQP 上报投递 → Worker 端滑动窗口累积 → SM 确认流水线 → HOT 决策 → AMQP 广播 → WorkerListener → L1 条目提升。Phase 9 默认需要连续 3 个确认窗口（最少 300ms）。Phase 9A（无 SM）跳过状态机确认流水线。

| 阶段                             | 操作数 | 耗时      | OPS   | P50            | P95        | P99         |
| -------------------------------- | ------ | --------- | ----- | -------------- | ---------- | ----------- |
| Redis GET 往返                   | 10,000 | 5,924 ms  | 1,688 | 0.48 ms        | 0.90 ms    | 2.64 ms     |
| Redis SET 往返                   | 5,000  | 2,836 ms  | 1,763 | 0.46 ms        | 0.91 ms    | 3.51 ms     |
| AMQP 发布                        | 10,000 | 311 ms    | 32,154 | 0.02 ms       | 0.09 ms    | 0.15 ms     |
| AMQP 端到端投递                  | 5,000  | 2,492 ms  | 2,006 | 0.07 ms        | 0.25 ms    | 0.38 ms     |
| HotKey L1 命中                   | 10,000 | 123 ms    | 81,301 | **0.001 ms**  | 0.004 ms   | 0.011 ms    |
| HotKey L1 未命中（→ Redis → L1） | 5,000  | 5,554 ms  | 900   | 0.47 ms        | 0.87 ms    | 1.65 ms     |
| Worker 决策流水线                | 200    | 11,553 ms | 17    | **56.38 ms**  | 99.21 ms   | 103.56 ms   |
| SM 确认流水线（3 确认窗）        | 10     | 315 ms    | 32    | **246.46 ms** | 295.00 ms† | 295.00 ms† |
| 全链路（SM 3 确认）              | 10     | 1,297 ms  | 8     | **298.19 ms** | 351.50 ms† | 351.50 ms† |
| 全链路（无 SM）                  | 17     | 1,219 ms  | 14    | **210.70 ms** | 235.67 ms  | 235.67 ms  |

> † P95=P99=Max，因为仅 **10 个键**（10 个样本点）。N=10 时 P95=第 9.5 个→第 10 个=Max，P99=第 9.9 个→第 10 个=Max。百分位数等于最大值，非测量异常。

关键：

- **HotKey L1 命中**是最快路径，约 1μs P50——纯 Caffeine 查找，无网络 I/O。
- **Redis RTT**（GET/SET）约 0.5ms P50——L2 回源的主要开销，各次运行一致。
- **AMQP 发布**延迟可忽略，0.02ms P50——RabbitMQ 通道写入本质上是内存到内存。
- **AMQP 端到端投递**（发布 + 代理路由 + 消费者投递）发布侧 P50=0.07ms，投递侧 P50=1.90ms——单机环境大多数投递在 2ms 内完成。
- **HotKey L1 未命中**（0.47ms P50）包含 Redis GET RTT + SingleFlight 去重 + L1 回填开销——主要延迟来自 Redis 调用本身。
- **Worker 决策流水线**（56.38ms P50）——Worker 的 `warmupJitterMs=100ms` 在决策评估前引入有意延迟，随后基于轮询的 `isLocalHotKey()` 提升检测。约 56ms P50 与抖动 + 处理 + AMQP 投递延迟一致。
- **SM 确认流水线**（246.46ms P50）——主导因素是确认窗口流水线：连续 3 个热窗口 × 每个 100ms 时间片 = 最少 300ms。确认后 HOT 决策走相同的 AMQP + WorkerListener 路径。总延迟（246.46ms）接近理论最小值，100% 提升率。
- **全链路（SM 3 确认）**（298.19ms P50）——完整端到端路径：本地 Caffeine 未命中 → 上报聚合（100ms 批次）→ AMQP 上报投递 → SlidingWindowDetector → 3 确认窗状态机 → AMQP 决策广播 → L1 提升。P50 **298ms** 距 300ms 理论确认底线仅 0.6%。10/10 键成功提升，0 错误。
- **全链路（无 SM）**（210.70ms P50）——相同全路径但跳过状态机确认。隔离 SM 贡献：3 确认窗增加约 88ms（298.19ms - 210.70ms）。无 SM 路径仍包含上报聚合（100ms 批次），是此路径的主导项。

![延迟分布热力图](img/latency_distribution_heatmap.png)

*图 2：各阶段延迟分布热力图。显示每个延迟桶（0-1ms, 1-5ms, 5-10ms 等）的操作占比。L1 命中路径 100% 在 0-1ms 桶内。*

  状态机参数可通过 `WorkerProperties` 自定义：
  - `hotkey.worker.state-machine.confirm-duration-ms` = 300（默认）→ `confirmWindows = ceil(300 / SlidingWindowDetector.sliceMs(100)) = 3`
  - `hotkey.worker.state-machine.cool-duration-ms` = 15000 → `coolWindows = 150`
  - `hotkey.worker.state-machine.pre-cool-grace-ms` = 5000 → `preCoolGraceWindows = 50`

  减小 `confirm-duration-ms` 或 `SlidingWindowDetector.sliceMs` 可直接缩短热键确认延迟，但会增加误判率。

### 5.8 极限参数传播延迟

**数据源**：[`integration-tests/src/test/resources/testresult/propagation-delay-extreme-2026-06-08T13-40-30.557328200Z.json`](../integration-tests/src/test/resources/testresult/propagation-delay-extreme-2026-06-08T13-40-30.557328200Z.json)

与 5.7 相同的 4 阶段结构，但采用极限参数调优：

| 参数                                                | 默认值 | 极限值    |
| --------------------------------------------------- | ------ | --------- |
| `hotkey.local.report-interval-ms`                   | 100    | **1**     |
| `hotkey.worker-listener.warmup-jitter-ms`           | 100    | **0**     |
| `hotkey.sync.warmup-jitter-ms`                      | 100    | **0**     |
| `hotkey.worker.state-machine.confirm-duration-ms`   | 300     | **0**     |
| `hotkey.worker.sliding-window.duration-ms` / `slices` | 1000/10 | **100/100** |

所有阶段使用相同的键数（各 10 个键）以确保公平比较。状态机始终存在——区别在于确认窗口数量。

| 阶段                          | 操作数 | 耗时    | P50          | P95      | P99      |
| ----------------------------- | ------ | ------- | ------------ | -------- | -------- |
| Worker 决策流水线（jitter=0） | 200    | 1,099 ms| **2.41 ms**  | 11.89 ms | 12.40 ms |
| SM 流水线（0 确认）           | 10     | 26 ms   | **7.71 ms**  | 8.53 ms  | 8.53 ms  |
| 全链路（SM 0 确认）           | 10     | 1,037 ms| **9.23 ms**  | 10.93 ms | 10.93 ms |

全部阶段：**0 错误**，总计 45k 操作。

![极端参数调优对比](img/extreme_tuning_comparison.png)

*图 3：极端参数调优 — 从默认配置（confirm=3）到极限配置（confirm=0）的延迟降低。全链路实现 96.9% 降低（298.19ms → 9.23ms）。*

关键：

- **Worker 决策流水线 P50 从 56.38ms 降至 2.41ms**——消除 100ms 预热抖动移除了主要延迟
- **SM 流水线 P50 从 246.46ms 降至 7.71ms**——3 确认窗流水线原本占约 97% 延迟；零确认窗口 + 1ms 粒度滑动窗口使决策近乎立即发出
- **全链路 P50 从 298.19ms（SM 3 确认）降至 9.23ms（SM 0 确认）**（96.9% 降低）——确认窗口是主导项。`report-interval-ms=1` 配合 `sliding-window.slices=1ms` 使上报刷新生效和 Worker 评估几乎即时。剩余 ~9ms 覆盖：缓存未命中 → AMQP 投递 → SM 评估（0 窗口）→ AMQP 决策广播 → L1 提升。10/10 键成功提升，0 错误。

  详见 [README 极限调优章节](../README.md#极限参数调整)的完整权衡讨论。

### 5.6 降级与性能

| 场景              | CPU 影响                | 内存影响                  | 行为                       |
| ----------------- | ----------------------- | ------------------------- | -------------------------- |
| 正常（L1 命中）   | < 1μs 每次操作          | ~4MB 草图 + 缓存条目      | 直接 Caffeine 返回         |
| 正常（L2 未命中） | 3-5ms 每次操作（Redis） | 临时 CacheEntry 分配      | SingleFlight 去重          |
| Redis 不可用      | 回落至节点本地计数器    | ~1KB 每 key（AtomicLong） | 降级版本标记防止脏数据覆盖 |
| RabbitMQ 不可用   | 同步队列暂存消息        | 可编程（队列深度）        | 本地缓存操作不受影响       |

---

## 7. 默认配置（源码验证）

与 `HotKeyProperties.java` 逐一对照确认：

| 属性                                    | 默认值             | 代码行号                    |
| --------------------------------------- | ------------------ | --------------------------- |
| `hotkey.local.top-k`                    | 100                | `HotKeyProperties.java:37`  |
| `hotkey.local.width`                    | 50,000             | `HotKeyProperties.java:42`  |
| `hotkey.local.depth`                    | 5                  | `HotKeyProperties.java:45`  |
| `hotkey.local.decay`                    | 0.92               | `HotKeyProperties.java:48`  |
| `hotkey.local.min-count`                | 10                 | `HotKeyProperties.java:51`  |
| `hotkey.local.local-cache-max-size`     | 1,000              | `HotKeyProperties.java:55`  |
| `hotkey.local.default-hard-ttl-ms`      | 300,000（5分钟）   | `HotKeyProperties.java:87`  |
| `hotkey.local.default-hot-hard-ttl-ms`  | 3,600,000（1小时） | `HotKeyProperties.java:91`  |
| `hotkey.local.default-soft-ttl-ms`      | 30,000（30秒）     | `HotKeyProperties.java:95`  |
| `hotkey.local.default-hot-soft-ttl-ms`  | 300,000（5分钟）   | `HotKeyProperties.java:99`  |
| `hotkey.local.inflight-max-size`        | 50,000             | `HotKeyProperties.java:63`  |
| `hotkey.local.inflight-ttl-seconds`     | 5                  | `HotKeyProperties.java:66`  |
| `hotkey.local.inflight-timeout-seconds` | 3                  | `HotKeyProperties.java:69`  |
| `hotkey.local.executor-core-pool-size`  | 8                  | `HotKeyProperties.java:72`  |
| `hotkey.local.executor-max-pool-size`   | 32                 | `HotKeyProperties.java:75`  |
| `hotkey.local.executor-queue-capacity`  | 2,000              | `HotKeyProperties.java:78`  |
| `hotkey.local.expelled-queue-capacity`  | 50,000             | `HotKeyProperties.java:81`  |
| `hotkey.local.report-interval-ms`       | 100                | `HotKeyProperties.java:148` |
| `hotkey.local.queue-capacity`           | 10,000             | `HotKeyProperties.java:151` |

---

_所有数据来源于 `integration-tests/src/test/resources/testresult/` 下的测试执行输出。源码默认值已与 `common/src/main/java/io/github/hyshmily/hotkey/autoconfigure/HotKeyProperties.java` 逐一核对。_
