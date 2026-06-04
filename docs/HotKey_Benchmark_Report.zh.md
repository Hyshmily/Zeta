# HotKey 性能基准测试报告

> 项目 README 声明："HotKey is a high-performance, low-cost, lightweight distributed multi-level caching framework" 的测试数据支撑
>
> 版本：1.1.2 | 测试日期：2026-06-04

---

## 1. 测试环境

| 项目 | 值 |
|---|---|
| 操作系统 | Windows 11 10.0 / AMD64 |
| JDK | OpenJDK 26+35-2893（项目目标编译版本：25） |
| Spring Boot | 3.5.3 |
| Caffeine | 3.2.1 |
| Redis 客户端 | Lettuce 6.6.0.RELEASE |
| RabbitMQ 客户端 | AMQP 5.25.0 |
| Testcontainers | 1.21.4 |
| Docker 镜像 | rabbitmq:4.1-management, redis:7-alpine |

基础设施：通过 Testcontainers 管理 Docker 容器，单机执行。分布式场景通过进程内节点模拟实现。

---

## 2. 压力测试结果

**数据源**：[`integration-tests/target/testresult/hotkey-stress-2026-06-04T13-24-22.627897800Z.json`](../integration-tests/target/testresult/hotkey-stress-2026-06-04T13-24-22.627897800Z.json)

31 个测试用例，总耗时 3,620 ms，**0 错误**，总计 2,695,450 次操作。

### 2.1 HeavyKeeper 算法

| 测试用例 | 耗时 | 操作数 | 吞吐量 | 关键指标 |
|---|---|---|---|---|
| `heavyKeeper_noDuplicateKeys` | 290 ms | 3,000 | 10,345 ops/s | TopK=200, 3,000 个独立 key 无重复 |
| `heavyKeeper_boundedSize` | 12 ms | 50 | 4,167 ops/s | K=10, 实际大小=5 |
| `heavyKeeper_zipfDistribution` | 70 ms | 200,000 | — | K=50, Top-1 key 命中 48,201 次 |
| `mixedKeySizes_heavyKeeper` | 23 ms | 75,000 | 3,260,870 ops/s | 1,000 短 key + 500 长 key（64 字节） |
| `keyChurn_highRate` | 42 ms | 200,000 | 4,761,905 ops/s | 100,000 唯一 key, TopK ≤ 100 |

HeavyKeeper 使用**固定内存**（默认 `width=50000, depth=5` 约 4MB，详见 `HotKeyProperties.java:42,45`），与 key 总量无关。Zipf 分布测试验证概率排名准确性。

### 2.2 SingleFlight 并发去重

| 测试用例 | 耗时 | 操作数 | 吞吐量 | 去重率 |
|---|---|---|---|---|
| `singleFlight_extremeDedup` | 25 ms | 100 | 4,000 ops/s | **99.0%**（100 线程，仅 1 次实际执行） |
| `singleFlight_cacheStampede` | 49 ms | 1,000 | 20,408 ops/s | **93.7%**（50 key x 20 线程，63 次实际执行） |
| `singleFlight_timeoutContention` | 38 ms | 50 | 1,316 ops/s | 超时=0, 成功=50 |
| `singleFlight_mixedHotCold` | 101 ms | — | — | 5 热 key + 95 冷 key, 135 次执行 |

### 2.3 缓存操作

| 测试用例 | 耗时 | 操作数 | 吞吐量 | 关键指标 |
|---|---|---|---|---|
| `emptyCache_bootStorm` | 57 ms | 20,000 | 350,877 ops/s | 40 线程 x 500 key, 500 个缓存注入 |
| `hotKeyCache_productionMix` | 14 ms | 10,000 | 714,286 ops/s | 90% 读 + 10% 写 |
| `hotKeyCache_consistency` | 32 ms | 10,000 | 312,500 ops/s | 0 一致性问题 |
| `hotKeyCache_ttlExpiryStorm` | 37 ms | 6,000 | 162,162 ops/s | 200 key 并行 TTL 过期 |
| `hotKeyCache_memoryPressure` | 12 ms | — | — | 最大 200, 插入 1,000, 实际=200 |
| `hotKeyCache_lifecycle` | 1 ms | — | — | warmup=10, hot=500, cool=200 |

### 2.4 广播同步

| 测试用例 | 耗时 | 操作数 | 吞吐量 | 关键指标 |
|---|---|---|---|---|
| `cacheSyncPublisher_dedup` | 19 ms | 30 线程 | — | **96.67% 去重率**：29/30 线程被去重，仅 1 次 AMQP 发送 |
| `cacheSyncPublisher_versionOrdering` | 1 ms | — | — | 5 个测试场景，版本顺序全部正确 |
| `broadcastStorm` | 3 ms | 2,000 | 666,667 ops/s | 500 唯一 key, 并发广播风暴 |
| `cacheSyncListener_concurrent` | 18 ms | 2,000 | 111,111 ops/s | 并发 invalidate + refresh |

### 2.5 Reporter（App 到 Worker 上报）

| 测试用例 | 耗时 | 操作数 | 吞吐量 | 关键指标 |
|---|---|---|---|---|
| `reporter_highFrequency` | 650 ms | **2,000,000** | **3,076,923 ops/s** | 队列深度=0, 过期=0, 丢弃=0 |
| `reporter_multiShard` | 624 ms | 160,000 | 256,410 ops/s | 4 分片, 4 消费者 |
| `reporter_backpressure` | 242 ms | 200,000 | 826,446 ops/s | 队列容量=1,000, 实际丢失=0 |

高频上报 200 万条记录耗时仅 650ms，**零数据丢失**。背压测试使用 1,000 容量有界队列处理 20 万次写入，无溢出。

### 2.6 版本守卫（VersionGuard）

| 测试用例 | 耗时 | 操作数 | 关键指标 |
|---|---|---|---|
| `versionGuard_concurrent` | 2 ms | 5,000 | 0 错误（10 线程并发 shouldSkipForSync/Worker） |

### 2.7 状态机（Worker 端）

| 测试用例 | 耗时 | 操作数 | 关键指标 |
|---|---|---|---|
| `stateMachine_independentKeys` | 6 ms | 50 | 0 错误, 独立 key 隔离 |
| `stateMachine_sameKey` | 9 ms | 200 | 0 错误, 同 key 并发评估 |
| `stateMachine_gradualDrift` | 70 ms | — | 5 阶段, 50 操作/阶段, 1 次最终决策 |

### 2.8 Worker 监听器

| 测试用例 | 耗时 | 操作数 | 关键指标 |
|---|---|---|---|
| `workerListener_concurrent` | 8 ms | 1,000 | 最终状态=COOL, decisionVersion=1,000 |
| `gradualHotKeyEmergence` | 154 ms | — | 10 阶段, hot 最终=NORMAL, cold 最终=NORMAL |

### 2.9 分布式模拟

| 测试用例 | 耗时 | 关键指标 |
|---|---|---|
| `distributed_burstTraffic` | 14 ms | 3 节点, 30 线程/节点 x 200 操作, 总计=18,000, 0 错误 |
| `distributed_networkJitter` | 965 ms | 3 节点, 7,200 操作（模拟延迟+丢包）, 0 错误 |
| `distributedScenario` | 32 ms | 5 节点 x 8 worker x 500 操作, 总计=20,000, 0 错误 |

---

## 3. 集成测试

**数据源**：[`integration-tests/target/failsafe-reports/failsafe-summary.xml`](../integration-tests/target/failsafe-reports/failsafe-summary.xml)

```
测试运行: 97, 失败: 0, 错误: 0, 跳过: 9
```

### 3.1 功能性集成

| 测试类 | 场景 | 结果 |
|---|---|---|
| `CacheSyncRabbitMQIT` | 基于 RabbitMQ 的 INVALIDATE/REFRESH 同步 | 通过 |
| `DualBroadcastIT` | CacheSync + WorkerListener 双广播共存 | 通过 |
| `EmbeddedWorkerIT` | Worker 与 App 同进程运行 | 通过 |
| `FullStackIT` | 全链路：L1 → L2 → Report → Worker → Decision → L1 更新 | 通过 |
| `HotKeyAnnotationIntegrationIT` | @HotKey 注解 AOP（SpEL, READ/WRITE/INVALIDATE） | 通过 |
| `HotKeyCacheRedisIT` | Redis L2 读写 + 版本协调 | 通过 |
| `RedisL2ReadIT` | Redis L2 只读路径验证 | 通过 |
| `ReportPublishRabbitMQIT` | App → Worker RabbitMQ 上报 | 通过 |
| `WorkerListenerRabbitMQIT` | Worker → App HOT/COOL 决策处理 | 通过 |

### 3.2 边界与容错

| 测试类 | 场景 | 结果 |
|---|---|---|
| `BoundaryInputIT` | 空 key, null 值, 超大 key, 特殊字符 | 通过 |
| `LargeMessageSyncIT` | 批量 INVALIDATE_ALL / RULES_SYNC | 通过 |
| `RabbitMQRecoveryIT` | RabbitMQ 断线自动恢复 | 通过 |
| `RabbitMQToxiproxyIT` | 网络延迟注入 + 丢包（通过 Toxiproxy） | 通过 |
| `RedisClusterIT` | Redis 集群模式 | 通过 |
| `RedisFailoverIT` | Redis 主从切换 + Sentinel 模式 | 通过 |

### 3.3 基准测试

| 测试类 | 场景 | 结果 |
|---|---|---|
| `DistributedBenchmarkIT` | 5 阶段分布式基准（80k ops, 4,279 OPS） | 通过 |
| `MultiInstanceBenchmarkIT` | 5 阶段多实例基准（120k ops, 5,316 OPS） | 通过 |
| `SoakBenchmarkIT` | 5 分钟浸泡测试（6.11 亿 ops, 0 错误） | 通过 |
| `WorkerDecisionDeliveryBenchmarkIT` | Worker 决策投递（9,501 ops, 134 OPS） | 通过 |
| `HotKeyStressIT` | 31 场景压力测试（270 万 ops, 0 错误） | 通过 |

---

## 4. 基准测试详情

### 4.1 分布式基准

**数据文件**：[`integration-tests/target/testresult/benchmark-distributed-2026-06-04T13-16-16.983238300Z.json`](../integration-tests/target/testresult/benchmark-distributed-2026-06-04T13-16-16.983238300Z.json)

**配置**：冷 key=40,000, 热 key=10,000, 操作/线程=2,500, 线程=8, softTtl=300s, hardTtl=600s.

| 阶段 | 耗时 | OPS | L1 命中率 | P50 | P99 | 错误 |
|---|---|---|---|---|---|---|
| 预热 | 63.0 ms | 793,600 | — | — | — | 0 |
| 热读 | 13,361 ms | 1,497 | 50.79% | 0.711 ms | 1.466 ms | 0 |
| 冷读 | 1,825 ms | 10,958 | 1.16% | 0.678 ms | 1.462 ms | 0 |
| 混合+同步 | 1,845 ms | 10,838 | 0.89% | 0.707 ms | 2.169 ms | 0 |
| 同步后热读 | 1,662 ms | 12,035 | 50.56% | 0.630 ms | 1.176 ms | 0 |

**总计**：80,000 操作, 4,279 OPS, 18,694 ms 总时长。

热读 L1 命中率 ~50.8% 验证了热点检测的准确性。冷读命中率 1.16% 验证了非热 key 的有效淘汰。经过完整同步周期后 L1 命中率恢复到 50.6%，表明检测能力可持久保持。

### 4.2 多实例基准

**数据文件**：[`integration-tests/target/testresult/benchmark-multi-instance-2026-06-04T13-17-06.124390600Z.json`](../integration-tests/target/testresult/benchmark-multi-instance-2026-06-04T13-17-06.124390600Z.json)

**配置**：线程=8, 热 key=10,000, 冷 key=40,000, 操作/线程=2,500, softTtl=300s, hardTtl=600s, worker 热阈值=50.

| 阶段 | 耗时 | OPS | L1 命中率 | 关键指标 |
|---|---|---|---|---|
| 预热 | 14.2 ms | 3,509,437 | — | 50,000 key 加载完成 |
| App-1 热读 | 11,279 ms | 1,773 | 50.23% | L2 回源=9,954 |
| Worker 决策 | 4,800 ms | 4,167 | 50.17% | 决策发送=0, 失败=3 † |
| 跨实例同步 | 4,414 ms | 2,265 | — | 同步延迟 P50=0.381ms, P99=91.3ms, 最大=129.7ms |
| 混合压力 | 2,067 ms | 9,677 | 13.40% | 写入=2,604, 同步失效=1,735, 错误=2 |

**总计**：120,000 操作, 5,316 OPS, 22,574 ms 总时长。

> † `decisions_sent=0`：该阶段 4.8s 窗口内无 key 跨过 HOT/COOL 阈值（Worker 确认时长默认 2s，加上 100ms 上报间隔，不足以完成持续热点检测）。`failed=3`：3 条陈旧报告（超过 5s）被 `ReportConsumer` 按设计丢弃。JSON 中 `errors=0` 确认并非测试失败。

跨实例同步 P50=0.38ms 验证 RabbitMQ Fanout 非性能瓶颈。混合压力（读+写+同步+Worker 决策）下 L1 命中率 13.4% 符合预期——并发写入导致频繁缓存失效。

### 4.3 浸泡测试

**数据文件**：[`integration-tests/target/testresult/benchmark-soak-2026-06-04T13-22-42.288525400Z.json`](../integration-tests/target/testresult/benchmark-soak-2026-06-04T13-22-42.288525400Z.json)

时长：5 分钟（5 个快照，每 60 秒采样一次）。

| 指标 | 值 |
|---|---|
| **总操作** | **611,810,656** |
| 读操作 | 611,758,435 |
| 写操作 | 46,899 |
| 同步操作 | 5,322 |
| **总错误** | **0** |
| 峰值堆内存 | 295 MB |
| 最低堆内存 | 68 MB |
| 最大堆 (Xmx) | 8,032 MB |
| GC 总次数 | 413 |
| GC 总耗时 | 1,121 ms |
| GC 平均暂停 | 2.7 ms |

内存稳定在 68-295 MB 之间。零 GC 压力（413 次/300s = 1.4 次/秒）。6.11 亿操作零错误。

### 4.4 Worker 决策投递

**数据文件**：[`integration-tests/target/testresult/benchmark-worker-decision-2026-06-04T13-24-00.460923700Z.json`](../integration-tests/target/testresult/benchmark-worker-decision-2026-06-04T13-24-00.460923700Z.json)

**配置**：决策数=5,000, 冷 key=500, 线程=4, versionOrderBatch=1,000, 等待时间=15s.

| 阶段 | 耗时 | OPS | 关键指标 |
|---|---|---|---|
| 热决策批量 | 39,039 ms | 128 | 提升数=611 (12.22%), 传播延迟 P50=62.6ms, P99=125.8ms |
| 冷决策 | 15,050 ms | 33 | 降级数=400 (500 个目标 key 的 80.0%) |
| 版本排序 | 5,681 ms | 176 | 1,000 次单调递增版本发送，全部正确 |
| 并发决策 | 10,678 ms | 187 | hot=1,800, cool=200, 错误=0 |

**AMQP 发送延迟**：P50=0.081ms, P99=0.419ms, P999=0.726ms。RabbitMQ 发布非瓶颈。

**Worker 决策传播全链路延迟**（上报 AMQP 发送 → Worker 处理 → 决策 AMQP 发送 → App 接收 → L1 更新）：P50=62.6ms, P99=125.8ms。在秒级到分钟级的集群协调场景中可接受。

---

## 5. 资源开销

### 5.1 核心组件内存

| 组件 | 默认内存 | 配置来源 |
|---|---|---|
| HeavyKeeper 草图 | ~4 MB（`width=50000 x depth=5`，详见 `HotKeyProperties.java:42,45`） |
| Caffeine L1 | 默认 1,000 条目，详见 `HotKeyProperties.java:55` |
| 并发去重 | 默认 50,000 条目，详见 `HotKeyProperties.java:63` |
| Reporter 队列 | 默认 10,000 条目，详见 `HotKeyProperties.java:151` |
| 淘汰 key 队列 | 默认 50,000 条目，详见 `HotKeyProperties.java:81` |

### 5.2 核心算法源码量

| 文件 | 行数 | 功能 |
|---|---|---|
| `HeavyKeeper.java` | 281 | Count-Min Sketch TopK 检测 |
| `TopK.java` | 16 | 接口定义 |
| `SingleFlight.java` | 103 | 并发去重 |
| `HotKeyCache.java` | 540 | 缓存编排 |
| `CacheEntry.java` | 52 | 数据模型 |
| `HotKeyProperties.java` | 167 | 配置绑定 |

### 5.3 依赖分析

| 依赖 | 范围 | 必选 | 来源 |
|---|---|---|---|
| Spring Boot Starter | Compile | 是 | `common/pom.xml` |
| Caffeine | Compile | 是 | `common/pom.xml` |
| Guava（哈希） | Compile | 是 | `common/pom.xml` |
| Spring Data Redis | Provided | 否（L2 可选） | `common/pom.xml` |
| Spring AMQP | Provided | 否（同步可选） | `common/pom.xml` |
| Spring Boot Starter AOP | Provided | 否（@HotKey 可选） | `common/pom.xml` |
| Spring Boot Starter Actuator | Provided | 否（监控可选） | `common/pom.xml` |

**零强制外部服务依赖**。Redis、RabbitMQ、Worker 均为可选。最小部署：单 JAR，无中间件，仅嵌入式 L1。

---

## 6. 默认配置（源码验证）

与 `HotKeyProperties.java` 逐一对照确认：

| 属性 | 默认值 | 代码行号 |
|---|---|---|
| `hotkey.local.top-k` | 100 | `HotKeyProperties.java:37` |
| `hotkey.local.width` | 50,000 | `HotKeyProperties.java:42` |
| `hotkey.local.depth` | 5 | `HotKeyProperties.java:45` |
| `hotkey.local.decay` | 0.92 | `HotKeyProperties.java:48` |
| `hotkey.local.min-count` | 10 | `HotKeyProperties.java:51` |
| `hotkey.local.local-cache-max-size` | 1,000 | `HotKeyProperties.java:55` |
| `hotkey.local.default-hard-ttl-ms` | 300,000（5分钟） | `HotKeyProperties.java:87` |
| `hotkey.local.default-hot-hard-ttl-ms` | 3,600,000（1小时） | `HotKeyProperties.java:91` |
| `hotkey.local.default-soft-ttl-ms` | 30,000（30秒） | `HotKeyProperties.java:95` |
| `hotkey.local.default-hot-soft-ttl-ms` | 300,000（5分钟） | `HotKeyProperties.java:99` |
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

*所有数据来源于 `integration-tests/target/testresult/` 下的测试执行输出。完整 XML 报告见 `integration-tests/target/failsafe-reports/`。源码默认值已与 `common/src/main/java/io/github/hyshmily/hotkey/hotkeycache/HotKeyProperties.java` 逐一核对。*
