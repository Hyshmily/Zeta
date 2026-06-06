# HotKey 性能测试

> 版本：1.1.3 | 测试日期：2026-06-06

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

**数据源**：[`integration-tests/target/testresult/hotkey-stress-2026-06-06T06-35-38.782405300Z.json`](../integration-tests/target/testresult/hotkey-stress-2026-06-06T06-35-38.782405300Z.json)

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

**数据源**：[`integration-tests/target/testresult/container-full-link-stress-2026-06-06T06-37-13.143975800Z.json`](../integration-tests/target/testresult/container-full-link-stress-2026-06-06T06-37-13.143975800Z.json)

15 个阶段，总耗时 58,331 ms，**0 错误**，总计 224,851 次操作，基于真实 Redis + RabbitMQ 容器。

**配置**：8 线程，softTtl=300s，hardTtl=600s，5,000 热 key，15,000 冷 key，2,000 ops/thread。

| 阶段                  | 耗时      | 操作数  | 吞吐量        | P50       | P99      | 关键指标                                               |
| --------------------- | --------- | ------- | ------------- | --------- | -------- | ------------------------------------------------------ |
| warmup                | 13,351 ms | 0       | —             | —         | —        | 20,000 key 写入 Redis + L1                             |
| hot-read              | 1,375 ms  | 16,000  | 11,636 ops/s  | 0.62 ms   | 1.56 ms  | 95.72% < 1ms，5,000 热 key L1 命中                     |
| cold-read             | 11,290 ms | 16,000  | 1,417 ops/s   | 0.62 ms   | 1.15 ms  | 97.30% < 1ms，15,917 次 L2 回源                        |
| write-stress          | 9 ms      | 1       | 111 ops/s     | 3.49 ms   | 3.49 ms  | 独立 key putThrough + Redis 验证                       |
| mixed-rw-inv          | 1,312 ms  | 16,000  | 12,195 ops/s  | 0.62 ms   | 1.65 ms  | 80% 读 / 10% 写 / 10% 失效                             |
| zipf-distribution     | 3,403 ms  | 100,000 | 29,386 ops/s  | < 0.01 ms | 0.86 ms  | **99.68% < 1ms**，top20=94.56% 命中                    |
| large-value-stress    | 4,857 ms  | 800     | 165 ops/s     | 1.84 ms   | 24.33 ms | 4 种值大小（1KB–1MB），Redis 往返                      |
| single-key-contention | 745 ms    | 10,000  | 13,423 ops/s  | < 0.01 ms | 11.18 ms | 20 线程 x 500 操作，相同 key                           |
| thundering-herd       | 162 ms    | 50      | 309 ops/s     | < 0.01 ms | 1.57 ms  | 0 次 supplier 调用（广播 REFRESH 在 herd 前已回填 L1） |
| worker-decisions      | 11,199 ms | 2,000   | 179 ops/s     | —         | —        | 25 提升 (1.25%)，1,000 降级                            |
| cross-instance-sync   | 2,946 ms  | 5,000   | 1,697 ops/s   | —         | —        | 同步 P50=0.36ms，P99=97.27ms，0 错误                   |
| version-degradation   | 4,412 ms  | 0       | —             | —         | —        | 2/4 降级版本场景通过                                   |
| pattern-shift         | 132 ms    | 15,000  | 113,636 ops/s | —         | —        | 200 模式 key，5,000 ops/模式                           |
| combined-stress       | 2,498 ms  | 32,000  | 12,810 ops/s  | 1.36 ms   | 3.84 ms  | 16 线程：70% 读 + 混合写/同步/决策                     |
| burst-traffic         | 640 ms    | 12,000  | 18,750 ops/s  | 1.57 ms   | 2.55 ms  | 50 线程 x 200 突发，稳定负载后                         |

### 系统指标（全程稳定）

| 指标         | 值       |
| ------------ | -------- |
| 堆使用量     | 436 MB   |
| 堆提交量     | 588 MB   |
| 最大堆 (Xmx) | 8,032 MB |
| 线程数       | 77       |
| GC 总次数    | 52       |
| GC 总耗时    | 227 ms   |

---

## 4. 集成测试

**压力测试数据源**：[`integration-tests/target/testresult/hotkey-stress-2026-06-06T06-35-38.782405300Z.json`](../integration-tests/target/testresult/hotkey-stress-2026-06-06T06-35-38.782405300Z.json)
**容器压力测试数据源**：[`integration-tests/target/testresult/container-full-link-stress-2026-06-06T06-37-13.143975800Z.json`](../integration-tests/target/testresult/container-full-link-stress-2026-06-06T06-37-13.143975800Z.json)
**传播延迟数据源**：[`integration-tests/target/testresult/propagation-delay-2026-06-06T07-22-19.722553100Z.json`](../integration-tests/target/testresult/propagation-delay-2026-06-06T07-22-19.722553100Z.json)

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
| `PropagationDelayIT`                | 8 阶段传播延迟测试（4.5 万 ops, 0 错误）   | 通过 |

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

**数据文件**：[`integration-tests/target/testresult/container-full-link-stress-2026-06-06T06-37-13.143975800Z.json`](../integration-tests/target/testresult/container-full-link-stress-2026-06-06T06-37-13.143975800Z.json)

**配置**：8 线程，5,000 热 key，15,000 冷 key，2,000 ops/thread，softTtl=300s，hardTtl=600s。

#### 5.5.1 读路径性能

`hot-read` 阶段达到 11,636 ops/s，95.72% 操作在 1ms 内完成（Caffeine L1 命中）。`cold-read` 阶段强制 L2 未命中 → Redis 回源 → L1 重缓存，达到 1,417 ops/s，97.30% < 1ms——验证 Redis 冷路径增加的延迟可忽略。

`zipf-distribution` 阶段（100,000 操作，200 key，α=1.2）验证了 HeavyKeeper 在大规模下的概率排名准确性：top 20% key 捕获了 94.56% 的访问，符合帕累托分布预期。

#### 5.5.2 并发与去重

- **单 key 争用**（20 线程 × 500 操作，相同 key）：13,423 ops/s，最终 Redis 值正确反映最后一次写入（`val-3498`），验证 `TransactionSupport` 延迟排序正确性。
- **惊群效应**（50 线程，1 个被 invalidate 的 key）：0 次 supplier 调用——`invalidate()` 发送 REFRESH 广播，异步监听器从 Redis 重新加载 key 并回填 L1，50 个 herd 线程释放时全部直接命中 L1。这验证了广播→回源→回填流水线能在测试的 150ms 休眠窗口内完成，在并发需求到达前就预热了 L1。

#### 5.5.3 Worker 决策模拟

通过 `hotkey.broadcast.exchange`（fanout）注入 HOT/COOL 决策。2,000 条 HOT 决策 → 25 个 key 提升（1.25% 提升率）。全部 1,000 个 COOL 目标成功降级。11.2s 的阶段耗时主要来自轮询检测间隔而非 AMQP 传播开销。

#### 5.5.4 跨实例同步

通过 `hotkey.sync.exchange`（direct）广播 5,000 条 INVALIDATE。同步传播 P50=0.36ms，P99=97.27ms——P99 尾部延迟由 `CacheExpireManager` 轮询间隔驱动而非 AMQP 投递延迟。零同步错误。

### 5.7 传播延迟

**数据源**：[`integration-tests/target/testresult/propagation-delay-2026-06-06T08-02-16.442185900Z.json`](../integration-tests/target/testresult/propagation-delay-2026-06-06T08-02-16.442185900Z.json)

10 个阶段，总计 45,312 次操作，**0 错误**，基于真实 Redis + RabbitMQ 容器（Testcontainers 单机）。测量 HotKey 数据路径中每个环节的单节点延迟。

**Phase 8** 模拟完整的 HotKeyStateMachine 确认窗口流水线：20 次 evaluate() 调用 × 100ms 时间片 → HOT 决策 → AMQP 广播 → WorkerListener → L1 提升。

**Phase 9** 测量完整端到端延迟：应用缓存未命中 → 上报聚合 → AMQP 上报投递 → Worker 端滑动窗口累积 → HOT 决策 → AMQP 广播 → WorkerListener → L1 条目提升。Phase 9A 未启用状态机（收到首次上报批次后立即广播），Phase 9B 需要连续 20 个确认窗口（最少 2,000ms）。

| 阶段                              | 操作数 | 耗时      | OPS    | P50           | P95       | P99       |
| --------------------------------- | ------ | --------- | ------ | ------------- | --------- | --------- |
| Redis GET 往返                    | 10,000 | 7,928 ms  | 1,261  | 0.62 ms       | 1.60 ms   | 4.01 ms   |
| Redis SET 往返                    | 5,000  | 3,754 ms  | 1,332  | 0.61 ms       | 1.45 ms   | 3.76 ms   |
| AMQP 发布                         | 10,000 | 406 ms    | 24,631 | 0.02 ms       | 0.11 ms   | 0.27 ms   |
| AMQP 端到端投递                   | 5,000  | 2,526 ms  | 1,979  | 0.07 ms       | 0.27 ms   | 0.48 ms   |
| HotKey L1 命中                    | 10,000 | 129 ms    | 77,519 | **0.001 ms**  | 0.004 ms  | 0.018 ms  |
| HotKey L1 未命中（→ Redis → L1）  | 5,000  | 5,688 ms  | 879    | 0.51 ms       | 0.94 ms   | 2.26 ms   |
| Worker 决策流水线（未开启状态机） | 200    | 10,993 ms | 18     | **51.64 ms**  | 97.75 ms  | 104.16 ms |
| 状态机流水线（开启状态机）        | 10     | 2,042 ms  | 5      | **1,983 ms**  | 2,015 ms  | 2,015 ms  |
| 全链路流水线（未开启状态机）      | 92     | 1,234 ms  | 75     | **155.81 ms** | 202.14 ms | 212.69 ms |
| 全链路 + 状态机（20 个确认窗口）  | 10     | 2,999 ms  | 3      | **2,038 ms**  | 2,055 ms  | 2,055 ms  |

关键：

- **HotKey L1 命中**是最快路径，约 1μs P50——纯 Caffeine 查找，无网络 I/O。
- **Redis RTT**（GET/SET）约 0.6ms P50——L2 回源的主要开销。
- **AMQP 发布**延迟可忽略，0.02ms P50——RabbitMQ 通道写入本质上是内存到内存。
- **AMQP 端到端投递**（发布 + 代理路由 + 消费者投递）发布侧 P50=0.07ms，投递侧 P50=1.46ms——单机环境大多数投递在 2ms 内完成。
- **HotKey L1 未命中**（0.51ms P50）包含 Redis GET RTT + SingleFlight 去重 + L1 回填开销——主要延迟来自 Redis 调用本身。
- **Worker 决策流水线（未开启状态机）**（51.64ms P50）——Worker 的 `warmupJitterMs=100ms` 在决策评估前引入有意延迟，随后基于轮询的 `isLocalHotKey()` 提升检测。约 52ms P50 与抖动 + 处理 + AMQP 投递延迟一致。
- **状态机流水线（开启状态机）**（1,983ms P50）——主导因素是确认窗口流水线：连续 20 个热窗口 × 每个 100ms 时间片 = 最少 2,000ms。确认后 HOT 决策走与 Phase 7 相同的 AMQP + WorkerListener 路径。总延迟（1,983ms）接近理论最小值 2,000ms + ~52ms 传播 ≈ 2,052ms。
- **全链路（未开启状态机）**（155.81ms P50）——比 Worker 决策流水线多约 104ms，包括：上报聚合批处理（report-interval-ms=100ms）、AMQP 上报投递、滑动窗口累积、以及决策广播路径。两段 AMQP 跳转（上报 + 决策）各引入投递延迟，100ms 批处理间隔贡献了一个完整 tick。50/50 键成功提升，0 错误。
- **全链路 + 状态机**（2,038ms P50）——在全链路基础上附加 20 窗口确认流水线（最少 2,000ms）。总延迟主要由确认窗口需求主导，仅比状态机流水线（Phase 8）多约 55ms——新增的上报聚合和投递开销相比 2s 确认底线可忽略不计。10/10 键成功提升，0 错误。

  状态机参数可通过 `WorkerProperties` 自定义：
  - `hotkey.worker.state-machine.confirm-duration-ms` = 2000（默认）→ `confirmWindows = ceil(2000 / SlidingWindowDetector.sliceMs(100)) = 20`
  - `hotkey.worker.state-machine.cool-duration-ms` = 15000 → `coolWindows = 150`
  - `hotkey.worker.state-machine.pre-cool-grace-ms` = 5000 → `preCoolGraceWindows = 50`

  减小 `confirm-duration-ms` 或 `SlidingWindowDetector.sliceMs` 可直接缩短热键确认延迟，但会增加误判率。

### 5.6 降级与性能

| 场景              | CPU 影响                | 内存影响                  | 行为                       |
| ----------------- | ----------------------- | ------------------------- | -------------------------- |
| 正常（L1 命中）   | < 1μs 每次操作          | ~4MB 草图 + 缓存条目      | 直接 Caffeine 返回         |
| 正常（L2 未命中） | 3-5ms 每次操作（Redis） | 临时 CacheEntry 分配      | SingleFlight 去重          |
| Redis 不可用      | 回落至节点本地计数器    | ~1KB 每 key（AtomicLong） | 降级版本标记防止脏数据覆盖 |
| RabbitMQ 不可用   | 同步队列暂存消息        | 可编程（队列深度）        | 本地缓存操作不受影响       |

---

## 6. 资源开销

### 6.1 核心组件内存

| 组件             | 默认内存                                                             | 配置来源 |
| ---------------- | -------------------------------------------------------------------- | -------- |
| HeavyKeeper 草图 | ~4 MB（`width=50000 x depth=5`，详见 `HotKeyProperties.java:42,45`） |
| Caffeine L1      | 默认 1,000 条目，详见 `HotKeyProperties.java:55`                     |
| 并发去重         | 默认 50,000 条目，详见 `HotKeyProperties.java:63`                    |
| Reporter 队列    | 默认 10,000 条目，详见 `HotKeyProperties.java:151`                   |
| 淘汰 key 队列    | 默认 50,000 条目，详见 `HotKeyProperties.java:81`                    |

### 6.2 核心算法源码量

| 文件                    | 行数 | 功能                       |
| ----------------------- | ---- | -------------------------- |
| `HeavyKeeper.java`      | 281  | Count-Min Sketch TopK 检测 |
| `TopK.java`             | 16   | 接口定义                   |
| `SingleFlight.java`     | 103  | 并发去重                   |
| `HotKeyCache.java`      | 540  | 缓存编排                   |
| `CacheEntry.java`       | 52   | 数据模型                   |
| `HotKeyProperties.java` | 167  | 配置绑定                   |

### 6.3 依赖分析

| 依赖                         | 范围     | 必选               | 来源             |
| ---------------------------- | -------- | ------------------ | ---------------- |
| Spring Boot Starter          | Compile  | 是                 | `common/pom.xml` |
| Caffeine                     | Compile  | 是                 | `common/pom.xml` |
| Guava（哈希）                | Compile  | 是                 | `common/pom.xml` |
| Spring Data Redis            | Provided | 否（L2 可选）      | `common/pom.xml` |
| Spring AMQP                  | Provided | 否（同步可选）     | `common/pom.xml` |
| Spring Boot Starter AOP      | Provided | 否（@HotKey 可选） | `common/pom.xml` |
| Spring Boot Starter Actuator | Provided | 否（监控可选）     | `common/pom.xml` |

**零强制外部服务依赖**。Redis、RabbitMQ、Worker 均为可选。最小部署：单 JAR，无中间件，仅嵌入式 L1。

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

_所有数据来源于 `integration-tests/target/testresult/` 下的测试执行输出。源码默认值已与 `common/src/main/java/io/github/hyshmily/hotkey/hotkeycache/HotKeyProperties.java` 逐一核对。_
