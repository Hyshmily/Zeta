# HotKey 功能增强建议 — 基于 Camellia 的源码分析

> 本文档基于对 [Camellia](https://github.com/netease-im/camellia)（网易开源，v1.4.2）和 HotKey（`io.github.hyshmily:hotkey:1.1.53`）两个项目的完整源码对比分析，提出具体可落地的功能增强建议，包含完整的 Java 实现代码。

---

## 目录

1. [可插拔事件回调系统](#1-可插拔事件回调系统)
2. [管理控制台 Admin Console](#2-管理控制台-admin-console)
3. [熔断器 Circuit Breaker](#3-熔断器-circuit-breaker)
4. [缓存值 LZ4 压缩](#4-缓存值-lz4-压缩)
5. [动态配置框架 DynamicValueGetter](#5-动态配置框架-dynamicvaluegetter)
6. [滑动窗口精确计数器](#6-滑动窗口精确计数器)
7. [缓存命中统计回调](#7-缓存命中统计回调)
8. [按 Key 有序执行器 HashedExecutor](#8-按-key-有序执行器-hashedexecutor)
9. [Grafana 预置 Dashboard](#9-grafana-预置-dashboard)
10. [集成方式](#10-集成方式)
11. [更多可参考的设计细节](#11-更多可参考的设计细节)
    - [线程模型与队列设计](#111-线程模型与队列设计)
    - [Netty 推送通道 vs AMQP 广播](#112-netty-推送通道-vs-amqp-广播)
    - [监控指标的多格式导出](#113-监控指标的多格式导出)
    - [配置管理体系的抽象设计](#114-配置管理体系的抽象设计)
    - [TopN 统计的跨节点协调](#115-topn-统计的跨节点协调)
    - [热 key 变更通知的去重与渐进式 TTL](#116-热-key-变更通知的去重与渐进式-ttl)
    - [高性能时间缓存](#117-高性能时间缓存)
    - [回调的防抖机制](#118-回调的防抖机制)
    - [数据校验与配置变更检测](#119-数据校验与配置变更检测)
    - [命名约定与日志分类](#1110-命名约定与日志分类)
    - [枚举值映射的规范方式](#1111-枚举值映射的规范方式)
    - [CamelliaMapUtils 的 computeIfAbsent 封装](#1112-camelliamaputils-的-computeifabsent-封装)
    - [源追踪 Source Tracking](#1113-源追踪-source-tracking)
    - [设计细节总结表](#1114-设计细节总结表)
    - [对现有 HotKey 代码的具体优化建议](#1115-对现有-hotkey-代码的具体优化建议)
    - [二进制协议设计](#1116-二进制协议设计-packunpack-codec)
    - [SDK 端采集器的双缓冲设计](#1117-sdk-端采集器的双缓冲设计)
    - [BeanFactory 扩展机制](#1118-beanfactory-扩展机制)
    - [优雅上下线与状态管理](#1119-优雅上下线与状态管理)
    - [时间桶非对齐处理](#1120-时间桶非对齐处理)
    - [配置校验与转换逻辑](#1121-hotkeyconfig-校验与转换逻辑)
    - [设计模式总结](#1122-设计模式总结)
    - [Camellia 的局限性与不足](#1123-camellia-的局限性与不足)
    - [完整功能清单](#1124-camellia-值得-hotkey-借鉴的完整功能清单)

---

## 1. 可插拔事件回调系统

### 1.0 补充：客户端监听器（命名空间级）

> **审查建议**：Camellia 除了服务端回调外，还有 SDK 侧客户端监听器 `CamelliaHotKeyListener`，用于接收服务器推送的热 key 通知。HotKey 的 Worker 通过 AMQP 广播决策，但应用端没有公共 API 订阅这些决策。

**新增 API 到 `HotKey` 门面：**

```java
/**
 * 注册 Worker 侧热 key 监听器。
 * 当 Worker 广播 HOT/COOL 决策时，通过此监听器通知应用层。
 */
public void addHotKeyListener(String cacheKey, Consumer<HotKeyEvent> listener) {
    // 包装 Worker 消息解码逻辑
    // 通过 WorkerListener 内部的事件总线分发
}
```

**优先级**：P0 — 是实现自定义缓存策略、外部指标导出、告警接入的基础设施。

---

### 参考来源

Camellia 的 `com.netease.nim.camellia.hot.key.server.callback` 包定义了 4 个回调接口、1 个管理器 (`HotKeyCallbackManager`)、以及 Logging/Dummy 实现。

| 文件 | 类 | 作用 |
|------|-----|------|
| `HotKeyCallback.java` | `HotKeyCallback` | 热 key 检测回调 (`newHotKey(HotKeyInfo)`) |
| `HotKeyTopNCallback.java` | `HotKeyTopNCallback` | TopN 统计完成回调 (`topN(TopNStatsResult)`) |
| `MonitorCallback.java` | `MonitorCallback` | 服务器健康统计回调 (`serverStats(HotKeyServerStats)`) |
| `HotKeyCacheStatsCallback.java` | `HotKeyCacheStatsCallback` | 缓存命中统计回调 (`newStats(List<HotKeyCacheStats>)`) |
| `HotKeyCallbackManager.java` | `HotKeyCallbackManager` | 管理器，统一线程池 + 防抖 + 初始化 |

HotKeyCallbackManager 的核心逻辑（`HotKeyCallbackManager.java:39-53`）：
- 通过 `BeanFactory.getBean()` 实例化回调类（类名来自配置）
- 用 `NamespaceCamelliaLocalCache` 做 5s 级别防抖
- 用独立的 `ThreadPoolExecutor(DiscardPolicy)` 异步执行回调

### HotKey 现状

没有任何面向用户的回调机制。热 key 检测、TopN 变更、缓存命中统计等事件无法被外部感知。

### 新增代码

#### 1.1 回调接口定义

**包路径：`common/src/main/java/io/github/hyshmily/hotkey/callback/`**

```java
// ===== HotKeyCallback.java =====
@FunctionalInterface
public interface HotKeyCallback {
    void onHotKeyEvent(HotKeyEvent event);
    default void init(HotKeyProperties properties) {}
}

// ===== HotKeyEvent.java =====
public class HotKeyEvent {
    private final String cacheKey;
    private final long currentCount;
    private final KeyState newState;  // HOT / COOL
    private final String source;      // LOCAL / WORKER
    private final long timestamp;
    private final Set<String> sourceSet;
    // 全参构造 + getters + toString()
}

// ===== TopNCallback.java =====
@FunctionalInterface
public interface TopNCallback {
    void onTopNChanged(List<Item> topN, String source);
    default void init(HotKeyProperties properties) {}
}

// ===== MonitorCallback.java =====
@FunctionalInterface
public interface MonitorCallback {
    void onMonitor(HotKeyMonitorSnapshot snapshot);
    default void init(HotKeyProperties properties) {}
}

// ===== HotKeyMonitorSnapshot.java =====
public class HotKeyMonitorSnapshot {
    private final long localTopKSize;
    private final long workerTopKSize;
    private final long l1CacheSize;
    private final long l1HitCount;
    private final long l1MissCount;
    private final long singleFlightInflight;
    private final long reporterQueueDepth;
    private final boolean clusterHealthy;
    private final int workerAliveCount;
    // 全参构造 + getters
}
```

#### 1.2 回调管理器

```java
// ===== CallbackManager.java =====
@Slf4j
public class CallbackManager {

    private final List<HotKeyCallback> hotKeyCallbacks = new CopyOnWriteArrayList<>();
    private final List<TopNCallback> topNCallbacks = new CopyOnWriteArrayList<>();
    private final List<MonitorCallback> monitorCallbacks = new CopyOnWriteArrayList<>();

    private final ThreadPoolExecutor executor;

    public CallbackManager(int poolSize, int queueCapacity) {
        this.executor = new ThreadPoolExecutor(
            poolSize, poolSize, 0, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(queueCapacity),
            r -> { Thread t = new Thread(r, "hotkey-callback"); t.setDaemon(true); return t; },
            new ThreadPoolExecutor.DiscardPolicy()
        );
    }

    public void registerHotKeyCallback(HotKeyCallback cb) { hotKeyCallbacks.add(cb); }
    public void registerTopNCallback(TopNCallback cb) { topNCallbacks.add(cb); }
    public void registerMonitorCallback(MonitorCallback cb) { monitorCallbacks.add(cb); }

    public void fireHotKey(String key, long count, KeyState state, String src, Set<String> srcSet) {
        if (hotKeyCallbacks.isEmpty()) return;
        HotKeyEvent event = new HotKeyEvent(key, count, state, src, System.currentTimeMillis(), srcSet);
        executor.submit(() -> {
            for (HotKeyCallback cb : hotKeyCallbacks) {
                try { cb.onHotKeyEvent(event); }
                catch (Exception e) { log.error("HotKeyCallback error", e); }
            }
        });
    }

    public void fireTopN(List<Item> topN, String source) {
        if (topNCallbacks.isEmpty()) return;
        executor.submit(() -> {
            for (TopNCallback cb : topNCallbacks) {
                try { cb.onTopNChanged(topN, source); }
                catch (Exception e) { log.error("TopNCallback error", e); }
            }
        });
    }

    public void fireMonitor(HotKeyMonitorSnapshot snapshot) {
        if (monitorCallbacks.isEmpty()) return;
        executor.submit(() -> {
            for (MonitorCallback cb : monitorCallbacks) {
                try { cb.onMonitor(snapshot); }
                catch (Exception e) { log.error("MonitorCallback error", e); }
            }
        });
    }
}
```

#### 1.3 在 HotKey 门面中暴露

在 `HotKey.java` 中新增：

```java
private final CallbackManager callbackManager;

// 构造器中初始化
this.callbackManager = new CallbackManager(4, 1000);

// 新增方法
public void addHotKeyCallback(HotKeyCallback callback) { callbackManager.registerHotKeyCallback(callback); }
public void addTopNCallback(TopNCallback callback)     { callbackManager.registerTopNCallback(callback); }
public void addMonitorCallback(MonitorCallback callback) { callbackManager.registerMonitorCallback(callback); }
```

#### 1.4 在 HotKeyCache 中触发

在 `HotKeyCache.java` 中检测到热 key 的位置添加：

```java
// 在 loadAndCache() 中，检测到 hotKeyDetector.contains(cacheKey) 时：
callbackManager.fireHotKey(cacheKey, hotKeyDetector.findCount(cacheKey),
    KeyState.HOT, "LOCAL", null);

// 在 promoteLocalHotkeyIfNeeded() 中，promotion 成功时：
callbackManager.fireHotKey(cacheKey, hotKeyDetector.findCount(cacheKey),
    KeyState.HOT, "LOCAL", null);
```

---

## 2. 管理控制台 Admin Console

### 参考来源

Camellia 的 `camellia-http-console` 模块实现了一个内嵌的 Netty HTTP 服务器：

| 文件 | 功能 |
|------|------|
| `CamelliaHttpConsoleServer.java` | Netty 服务器启动，绑定端口 |
| `CamelliaHttpConsoleServerHandler.java` | HTTP 请求处理，路由到 `@ConsoleApi` 方法 |
| `ConsoleApiInvokersUtil.java` | 扫描 `@ConsoleApi` 注解并注册 |
| `ConsoleApiInvoker.java` | 通过反射调用目标方法 |
| `ConsoleApi.java` | `@ConsoleApi(uri="/path")` 注解 |
| `ConsoleResult.java` | 统一返回格式 |
| `ConsoleServiceAdaptor.java` | 实现 `/status`, `/online`, `/offline`, `/monitor`, `/topN`, `/prometheus`, `/reload` |

ConsoleServiceAdaptor 端点实现参考：

```java
// /status → ServerStatus.getStatus()
// /monitor → HotKeyServerMonitorCollector.getHotKeyServerStats() → StatsJsonConverter
// /topN → TopNMonitor.getTopNStatsResult(namespace, backtrack)
// /prometheus → StatsPrometheusConverter
// /reload → ConfReloadHolder.reload()
```

### HotKey 现状

仅依赖 Spring Boot Actuator 端点，没有独立的管理控制台。

### 新增代码

#### 2.1 Console API 框架

**包路径：`hotkey-console/src/main/java/io/github/hyshmily/hotkey/console/`**

```java
// ===== ConsoleApi.java =====
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ConsoleApi { String uri(); }

// ===== ConsoleResult.java =====
public class ConsoleResult {
    private final HttpResponseStatus code;
    private final String data;
    public ConsoleResult(HttpResponseStatus code, String data) { ... }
    public static ConsoleResult success() { return new ConsoleResult(OK, "success"); }
    public static ConsoleResult success(String data) { return new ConsoleResult(OK, data); }
    public static ConsoleResult error() { return new ConsoleResult(INTERNAL_SERVER_ERROR, "error"); }
    // getters...
}

// ===== ConsoleApiInvoker.java =====
public class ConsoleApiInvoker {
    private final String uri;
    private final boolean withParams;
    private final Object service;
    private final Method method;

    public ConsoleResult invoke(QueryStringDecoder decoder) throws Exception {
        if (withParams) return (ConsoleResult) method.invoke(service, decoder.parameters());
        else return (ConsoleResult) method.invoke(service);
    }
}

// ===== ConsoleApiInvokersUtil.java =====
public class ConsoleApiInvokersUtil {
    public static Map<String, ConsoleApiInvoker> initApiInvokers(Object service) {
        Map<String, ConsoleApiInvoker> map = new HashMap<>();
        List<Method> methods = new ArrayList<>();
        collectMethods(service.getClass(), methods);
        for (Method m : methods) {
            ConsoleApi api = m.getDeclaredAnnotation(ConsoleApi.class);
            if (api == null) continue;
            if (!ConsoleResult.class.isAssignableFrom(m.getReturnType())) continue;
            int pc = m.getParameterCount();
            if (pc == 0) { m.setAccessible(true); map.put(api.uri(), new ConsoleApiInvoker(service, m, api.uri(), false)); }
            else if (pc == 1 && Map.class.isAssignableFrom(m.getParameterTypes()[0])) {
                m.setAccessible(true); map.put(api.uri(), new ConsoleApiInvoker(service, m, api.uri(), true));
            }
        }
        return map;
    }
    private static void collectMethods(Class<?> clazz, List<Method> methods) {
        if (clazz == null) return;
        methods.addAll(Arrays.asList(clazz.getMethods()));
        collectMethods(clazz.getSuperclass(), methods);
        for (Class<?> iface : clazz.getInterfaces()) collectMethods(iface, methods);
    }
}
```

#### 2.2 Netty HTTP 服务器

```java
// ===== HotKeyConsoleConfig.java =====
public class HotKeyConsoleConfig {
    private String host = "0.0.0.0";
    private int port = 17070;
    private int bossThread = 1;
    private int workThread = Runtime.getRuntime().availableProcessors();
    private Object consoleService;
    // getters/setters...
}

// ===== HotKeyConsoleServer.java =====
@Slf4j
public class HotKeyConsoleServer {
    private final HotKeyConsoleConfig config;
    private final Map<String, ConsoleApiInvoker> invokerMap;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ChannelFuture channelFuture;

    public HotKeyConsoleServer(HotKeyConsoleConfig config) {
        this.config = config;
        this.invokerMap = ConsoleApiInvokersUtil.initApiInvokers(config.getConsoleService());
        log.info("Endpoints: {}", invokerMap.keySet());
    }

    public ChannelFuture start() {
        bossGroup = new NioEventLoopGroup(config.getBossThread(), new DefaultThreadFactory("console-boss"));
        workerGroup = new NioEventLoopGroup(config.getWorkThread(), new DefaultThreadFactory("console-worker"));
        ServerBootstrap b = new ServerBootstrap();
        b.option(ChannelOption.SO_BACKLOG, 1024)
            .group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
            .childHandler(new ChannelInitializer<Channel>() {
                @Override protected void initChannel(Channel ch) {
                    ch.pipeline().addLast(new HttpServerCodec());
                    ch.pipeline().addLast(new HttpObjectAggregator(65536));
                    ch.pipeline().addLast(new HotKeyConsoleServerHandler(invokerMap));
                }
            });
        channelFuture = b.bind(config.getHost(), config.getPort()).syncUninterruptibly();
        return channelFuture;
    }

    public void stop() {
        if (channelFuture != null) channelFuture.channel().close();
        if (bossGroup != null) bossGroup.shutdownGracefully();
        if (workerGroup != null) workerGroup.shutdownGracefully();
        log.info("Console server stopped");
    }
}

// ===== HotKeyConsoleServerHandler.java =====
@Slf4j
public class HotKeyConsoleServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private final Map<String, ConsoleApiInvoker> invokerMap;
    // channelRead0: 解析 URI → invokerMap.get(path) → invoke → 返回 HTTP 响应
    // 404 / 200 / 500 处理
    // Content-Type: application/json; charset=utf-8
}
```

#### 2.3 控制台服务实现

```java
// ===== HotKeyConsoleService.java =====
@RequiredArgsConstructor
@Slf4j
public class HotKeyConsoleService {
    private final HotKey hotKey;

    @ConsoleApi(uri = "/status")
    public ConsoleResult status() {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("mode", hotKey.isAppOnly() ? "APP_ONLY" : hotKey.isWorkerOnly() ? "WORKER_ONLY" : "COEXISTENCE");
        info.put("app", hotKey.isApp()); info.put("worker", hotKey.isWorker());
        info.put("estimatedSize", hotKey.estimatedSize());
        if (hotKey.stats() != null) {
            info.put("hitCount", hotKey.stats().hitCount());
            info.put("missCount", hotKey.stats().missCount());
            info.put("hitRate", hotKey.stats().hitRate());
        }
        return ConsoleResult.success(toJson(info));
    }

    @ConsoleApi(uri = "/hotkeys")
    public ConsoleResult hotKeys() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("localTopK", hotKey.returnLocalHotKeys().stream()
            .map(i -> Map.of("key", i.key(), "count", i.count())).toList());
        result.put("workerTopK", hotKey.returnWorkerHotKeys().stream()
            .map(i -> Map.of("key", i.key(), "count", i.count())).toList());
        result.put("localTotal", hotKey.returnLocalTotalDataStreams());
        result.put("workerTotal", hotKey.returnWorkerTotalDataStreams());
        return ConsoleResult.success(toJson(result));
    }

    @ConsoleApi(uri = "/rules")
    public ConsoleResult rules() { return ConsoleResult.success(toJson(hotKey.getAllRules())); }

    @ConsoleApi(uri = "/rules/add")
    public ConsoleResult addRule(Map<String, List<String>> params) {
        String pattern = getParam(params, "pattern");
        String action = getParam(params, "action");
        if (pattern == null || action == null) return ConsoleResult.error("missing params");
        if ("BLOCK".equalsIgnoreCase(action)) hotKey.addBlacklist(pattern);
        else if ("ALLOW_NO_REPORT".equalsIgnoreCase(action)) hotKey.addWhitelist(pattern);
        return ConsoleResult.success("ok");
    }

    @ConsoleApi(uri = "/topN")
    public ConsoleResult topN(Map<String, List<String>> params) {
        int n = params.containsKey("n") ? Integer.parseInt(params.get("n").get(0)) : 20;
        return ConsoleResult.success(toJson(hotKey.returnLocalTopNHotKeys(n)));
    }

    private String getParam(Map<String, List<String>> p, String k) {
        return p.containsKey(k) ? p.get(k).get(0) : null;
    }

    private String toJson(Object obj) {
        try { return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(obj); }
        catch (Exception e) { return obj.toString(); }
    }
}
```

#### 2.4 Spring Boot 自动配置

```java
// ===== HotKeyConsoleAutoConfiguration.java =====
@AutoConfiguration
@ConditionalOnBean(HotKey.class)
@ConditionalOnProperty(prefix = "hotkey.console", name = "enabled", havingValue = "true", matchIfMissing = false)
public class HotKeyConsoleAutoConfiguration {

    @Bean
    public HotKeyConsoleService hotKeyConsoleService(HotKey hotKey) { return new HotKeyConsoleService(hotKey); }

    @Bean(destroyMethod = "stop")
    public HotKeyConsoleServer hotKeyConsoleServer(HotKeyConsoleService svc, HotKeyProperties props) {
        HotKeyConsoleConfig cfg = new HotKeyConsoleConfig();
        cfg.setPort(props.getConsolePort());
        cfg.setConsoleService(svc);
        HotKeyConsoleServer server = new HotKeyConsoleServer(cfg);
        server.start();
        return server;
    }
}

// HotKeyConsoleServer 中新增 stop() 方法：
public void stop() {
    if (channelFuture != null) {
        channelFuture.channel().close();
    }
    if (bossGroup != null) bossGroup.shutdownGracefully();
    if (workerGroup != null) workerGroup.shutdownGracefully();
}

// HotKeyProperties 中新增：
private int consolePort = 17070;
```

---

## 3. 熔断器 Circuit Breaker

### 参考来源

Camellia 的 `CamelliaCircuitBreaker` 完整实现（`CamelliaCircuitBreaker.java:1-256`）：

- **数据结构**：`LongAdder[] successBuckets` + `LongAdder[] failBuckets`，长度为 `bucketSize`（默认 10）
- **滑动机制**：定时器 `scheduleAtFixedRate` 每 `windowTime/bucketSize` ms 滑动一次
- **打开条件**：`totalFail > 0` 且 `totalFail/(totalSuccess+totalFail) > failThresholdPercentage` 且 `totalSuccess+totalFail >= requestVolumeThreshold`
- **半开探测**：`System.currentTimeMillis() - openTimestamp > singleTestIntervalMillis` 时，通过 `CAS` 允许一个请求通过
- **关闭恢复**：`incrementSuccess()` 中检测到 `circuitBreakerOpen==true` 时，CAS 关闭并重置所有桶
- **热加载**：`enable`、`forceOpen`、`failThresholdPercentage`、`requestVolumeThreshold`、`singleTestIntervalMillis` 全部使用 `DynamicValueGetter`
- **管理**：`CamelliaCircuitBreakerManager` 用 Caffeine 缓存管理多个实例，支持自动过期清理

### HotKey 现状

没有任何熔断机制。当 DB 或远程服务在缓存 Miss 时响应变慢导致线程池积压。

### 新增代码

```java
// ===== HotKeyCircuitBreaker.java =====
@Slf4j
public class HotKeyCircuitBreaker implements Closeable {

    private static final ScheduledExecutorService SCHEDULER = new ScheduledThreadPoolExecutor(
        Runtime.getRuntime().availableProcessors(),
        r -> { Thread t = new Thread(r, "cb"); t.setDaemon(true); return t; });

    private static final AtomicLong idGen = new AtomicLong();

    private final String name;
    private final Config config;
    private final int bucketSize;
    private final LongAdder[] success;
    private final LongAdder[] fail;
    private volatile int index;
    private final AtomicBoolean open = new AtomicBoolean(false);
    private volatile long openTimestamp;
    private final AtomicLong lastHalfOpen = new AtomicLong(0L);
    private final ScheduledFuture<?> slideFuture;

    // 构造器：初始化桶数组，启动定时滑动
    public HotKeyCircuitBreaker(Config config) {
        this.config = config;
        this.name = "cb-" + idGen.incrementAndGet();
        this.bucketSize = config.statisticSlidingWindowBucketSize;
        this.success = new LongAdder[bucketSize];
        this.fail = new LongAdder[bucketSize];
        for (int i = 0; i < bucketSize; i++) { success[i] = new LongAdder(); fail[i] = new LongAdder(); }
        long slideMs = config.statisticSlidingWindowTime / bucketSize;
        this.slideFuture = SCHEDULER.scheduleAtFixedRate(this::slide, slideMs, slideMs, TimeUnit.MILLISECONDS);
    }

    public boolean allowRequest() {
        if (!config.enabled) { if (open.get()) open.set(false); return true; }
        if (config.forceOpen) return false;
        if (!open.get()) return true;
        // 半开探测 — 先将 lastHalfOpen 的快照存入局部变量，避免 TOCTOU 竞态
        long now = System.currentTimeMillis();
        long lastTest = lastHalfOpen.get();
        if (now - openTimestamp > config.singleTestIntervalMs
            && now - lastTest > config.singleTestIntervalMs
            && lastHalfOpen.compareAndSet(lastTest, now)) {
            if (config.logEnable) log.info("CB half-open: {}", name);
            return true;
        }
        return false;
    }

    public void onSuccess() {
        if (!config.enabled) return;
        success[index].increment();
        if (open.get() && open.compareAndSet(true, false)) {
            for (LongAdder a : fail) a.reset();
            for (LongAdder a : success) a.reset();
            if (config.logEnable) log.info("CB closed: {}", name);
        }
    }

    public void onFailure() { if (config.enabled) fail[index].increment(); }

    private void slide() {
        int next = (index + 1) % bucketSize;
        success[next].reset(); fail[next].reset();
        index = next;
        long ts = 0, tf = 0;
        for (LongAdder a : success) ts += a.sum();
        for (LongAdder a : fail) tf += a.sum();
        if (tf > 0 && ts + tf >= config.requestVolumeThreshold) {
            double rate = (double) tf / (ts + tf);
            if (rate > config.failThresholdPercentage && open.compareAndSet(false, true)) {
                openTimestamp = System.currentTimeMillis();
                if (config.logEnable) log.info("CB OPEN: {}", name);
            }
        }
    }

    @Override public void close() { if (slideFuture != null) slideFuture.cancel(false); }

    @Getter
    public static class Config {
        private boolean enabled = true;
        private boolean forceOpen = false;
        private long statisticSlidingWindowTime = 10_000L;
        private int statisticSlidingWindowBucketSize = 10;
        private double failThresholdPercentage = 0.5;
        private long requestVolumeThreshold = 20L;
        private long singleTestIntervalMs = 5_000L;
        private boolean logEnable = true;
    }
}
```

#### 集成到 HotKeyCache

```java
// HotKeyCache 新增字段
private final HotKeyCircuitBreaker loadBreaker;

// 构造器中初始化
this.loadBreaker = new HotKeyCircuitBreaker(new HotKeyCircuitBreaker.Config());

// 在 loadAndCache() 中：
if (!loadBreaker.allowRequest()) {
    log.warn("Circuit breaker open for key={}", cacheKey);
    return Optional.empty();  // 或返回过期数据
}
try {
    T value = reader.get();
    loadBreaker.onSuccess();
    // ... 缓存逻辑 ...
} catch (Exception e) {
    loadBreaker.onFailure();
    throw e;
}
```

---

## 4. 缓存值 LZ4 压缩

### 参考来源

`CamelliaCompressor.java` 实现（121 行）：
- 数据格式：`tag(1B) + magic(N bytes) + compressedLen(4B) + originalLen(4B) + compressedData`
- 阈值可配（默认 1024 字节），小于阈值不压缩
- 向后兼容：未压缩数据直接返回（长度检查 + magic 检查 + 长度校验）
- 集成在 `Jackson2JsonCamelliaCacheSerializer` 中作为可选项

### HotKey 现状

Redis L2 和 AMQP 消息传递原始数据，无压缩。

### 新增代码

```java
// ===== CacheValueCompressor.java =====
public class CacheValueCompressor {
    private static final String DEFAULT_MAGIC = "hotkey~z";
    private static final int DEFAULT_THRESHOLD = 1024;
    private static final byte NO_C = 0, LZ4_C = 1;
    private static final LZ4Compressor COMPRESSOR;
    private static final LZ4FastDecompressor DECOMPRESSOR;
    static {
        LZ4Factory f = LZ4Factory.fastestInstance();
        COMPRESSOR = f.fastCompressor();
        DECOMPRESSOR = f.fastDecompressor();
    }

    private final byte[] magic;
    private final int headerLen;
    private final int threshold;

    public CacheValueCompressor() { this(DEFAULT_MAGIC, DEFAULT_THRESHOLD); }
    public CacheValueCompressor(int threshold) { this(DEFAULT_MAGIC, threshold); }
    public CacheValueCompressor(String magic, int threshold) {
        this.magic = magic.getBytes(StandardCharsets.UTF_8);
        this.headerLen = 1 + this.magic.length + 4 + 4;
        this.threshold = threshold;
    }

    public byte[] compress(byte[] original) {
        if (original == null || original.length <= threshold) return original;
        int maxLen = COMPRESSOR.maxCompressedLength(original.length);
        byte[] compressed = new byte[maxLen];
        int clen = COMPRESSOR.compress(original, 0, original.length, compressed, 0, maxLen);
        if (headerLen + clen >= original.length) return original;
        byte[] result = new byte[headerLen + clen];
        System.arraycopy(compressed, 0, result, headerLen, clen);
        ByteBuffer buf = ByteBuffer.wrap(result);
        buf.put(LZ4_C); buf.put(magic); buf.putInt(result.length); buf.putInt(original.length);
        return result;
    }

    public byte[] decompress(byte[] data) {
        if (data == null || data.length <= headerLen) return data;
        ByteBuffer buf = ByteBuffer.wrap(data);
        if (buf.get() == NO_C) return data;
        byte[] m = new byte[magic.length]; buf.get(m);
        if (!Arrays.equals(m, magic)) return data;
        if (buf.getInt() != data.length) return data;
        int olen = buf.getInt();
        byte[] original = new byte[olen];
        DECOMPRESSOR.decompress(data, headerLen, original, 0, olen);
        return original;
    }
}
```

#### 集成点

Redis L2 序列化层 + AMQP 消息发布层：

```java
// 自动配置条件：hotkey.local.cache.compress=true
@Bean
@ConditionalOnProperty(prefix = "hotkey.local.cache", name = "compress", havingValue = "true")
public CacheValueCompressor cacheValueCompressor() { return new CacheValueCompressor(); }
```

---

## 5. 动态配置框架

### 参考来源

`DynamicValueGetter.java` 是一个 10 行的函数式接口：

```java
public interface DynamicValueGetter<T> { T get(); }
```

配合 `DynamicConfig` 实现从 Spring Environment、Nacos 等动态读取配置。

### HotKey 现状

所有配置通过 `HotKeyProperties` 在启动时读取，修改后需要重启。

### 新增代码

```java
// ===== DynamicValue.java =====
@FunctionalInterface
public interface DynamicValue<T> { T get(); }

// ===== SpringDynamicValue.java =====
public class SpringDynamicValue<T> implements DynamicValue<T> {
    private final Environment env;
    private final String key;
    private final Class<T> targetType;
    private final T defaultVal;
    @Override
    @SuppressWarnings("unchecked")
    public T get() {
        if (env == null) return defaultVal;
        if (targetType == String.class) return (T) env.getProperty(key, (String) defaultVal);
        return env.getProperty(key, targetType, defaultVal);
    }
}
```

#### 应用场景：HeavyKeeper 动态 minCount

```java
// HeavyKeeper 在 auto-config 中注入 DynamicValue
@Bean
public HeavyKeeper heavyKeeper(HotKeyProperties props, Environment env) {
    DynamicValue<Integer> dynMinCount = new SpringDynamicValue<>(env, "hotkey.local.min-count", Integer.class, props.getMinCount());
    return new HeavyKeeper(props.getTopK(), props.getWidth(), props.getDepth(),
        props.getDecay(), dynMinCount, ...);
}

// HeavyKeeper 构造器中：
private final DynamicValue<Integer> dynamicMinCount;
// 定时任务每 10s 刷新
public void refreshConfig() {
    this.minCount = dynamicMinCount.get();
}
```

---

## 6. 滑动窗口精确计数器

### 参考来源

`HotKeyCounter.java`（99 行）：
- 桶大小固定 100ms
- `total` 懒计算（-1 时重新求和）
- 线程不安全，由上层保证单线程

### HotKey 现状

仅使用 HeavyKeeper（概率型），在 key 数量可控时适用精确计数。

### 新增代码

```java
// ===== ExactSlidingWindowCounter.java =====
public class ExactSlidingWindowCounter {
    private final long[] buckets;
    private final int bucketCount;
    private final long bucketSizeMs;
    private int index;
    private long lastUpdateTime = System.currentTimeMillis();
    private long total = -1;

    public ExactSlidingWindowCounter(long windowMs, long bucketSizeMs) {
        this.bucketCount = (int) (windowMs / bucketSizeMs);
        this.buckets = new long[bucketCount];
        this.bucketSizeMs = bucketSizeMs;
    }

    public long update(long count) {
        long now = System.currentTimeMillis();
        int step = (int) ((now - lastUpdateTime) / bucketSizeMs);
        if (step > 0) { slide(step); total = -1; lastUpdateTime = now; }
        buckets[index] += count;
        if (total == -1) { long s = 0; for (long b : buckets) s += b; total = s; }
        else total += count;
        return total;
    }

    public long getTotal() {
        if (total == -1) { long s = 0; for (long b : buckets) s += b; total = s; }
        return total;
    }

    private void slide(int step) {
        if (step >= bucketCount) {
            for (int i = 0; i < bucketCount; i++) buckets[i] = 0;
            index = 0; return;
        }
        for (int i = 1; i <= step; i++) buckets[(index + i) % bucketCount] = 0;
        index = (index + step) % bucketCount;
    }
}
```

---

## 7. 缓存命中统计回调

### 参考来源

Camellia 的 `CamelliaHotKeyCacheSdk` 用 `NamespaceCamelliaLocalCache` 收集 `Stats(namespace, key, LongAdder)`，
定期通过 `HotKeyCacheStatsPack` 上报到服务器。

### HotKey 现状

已有 `HotKeyCacheStats` 模型类，无统计上报机制。

### 新增代码

```java
// ===== CacheStatsCallback.java =====
@FunctionalInterface
public interface CacheStatsCallback {
    void onCacheStats(HotKeyCacheStats stats);
    default void init(HotKeyProperties properties) {}
}

// CallbackManager 中增加：
private final List<CacheStatsCallback> cacheStatsCallbacks = new CopyOnWriteArrayList<>();
public void registerCacheStatsCallback(CacheStatsCallback cb) { cacheStatsCallbacks.add(cb); }

// HotKeyCache.stats() 中可选触发：
public HotKeyCacheStats stats() {
    CacheStats cs = caffeineCache.stats();
    HotKeyCacheStats s = new HotKeyCacheStats(cs.hitCount(), cs.missCount(), cs.hitRate(), cs.evictionCount(), caffeineCache.estimatedSize());
    // 通知 callback
    return s;
}
```

---

## 8. 按 Key 有序执行器 HashedExecutor

### 参考来源

`CamelliaHashedExecutor.java`（395 行）：
- `WorkThread` 内部类：每个线程持有一个 `BlockingQueue<FutureTask<?>>`
- `hashIndex(byte[])`：`Math.abs(Arrays.hashCode(hashKey)) % poolSize`
- 支持 `DynamicValueGetter<Integer>` 动态队列大小
- 内置 `AbortPolicy`、`DiscardPolicy`、`CallerRunsPolicy`

### HotKey 现状

使用 `ThreadPoolTaskExecutor` 通用线程池，无路由能力。

### 新增代码

```java
// ===== HashedExecutor.java =====
@Slf4j
public class HashedExecutor {
    private final String name;
    private final int poolSize;
    private final List<WorkThread> threads;
    private final AtomicBoolean initOk = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    public HashedExecutor(String name, int poolSize, int queueSize) {
        this.name = name; this.poolSize = poolSize;
        this.threads = new ArrayList<>(poolSize);
        synchronized (this) {
            for (int i = 0; i < poolSize; i++) {
                WorkThread wt = new WorkThread(queueSize);
                wt.start(); threads.add(wt);
            }
            initOk.set(true);
        }
    }

    public Future<Void> submit(String hashKey, Runnable task) {
        return submit(hashKey.getBytes(StandardCharsets.UTF_8), task);
    }

    public Future<Void> submit(byte[] hashKey, Runnable task) {
        int idx = Math.floorMod(Arrays.hashCode(hashKey), threads.size());
        FutureTask<Void> ft = new FutureTask<>(task, null);
        if (!threads.get(idx).submit(ft)) throw new RejectedExecutionException("Queue full");
        return ft;
    }

    public <T> Future<T> submit(String hashKey, Callable<T> task) {
        int idx = Math.floorMod(Arrays.hashCode(hashKey.getBytes(StandardCharsets.UTF_8)), threads.size());
        FutureTask<T> ft = new FutureTask<>(task);
        if (!threads.get(idx).submit(ft)) throw new RejectedExecutionException("Queue full");
        return ft;
    }

    public void shutdown() { shutdown.set(true); }

    private static class WorkThread extends Thread {
        private final BlockingQueue<FutureTask<?>> queue;
        private final AtomicBoolean active = new AtomicBoolean(false);
        private final AtomicLong completed = new AtomicLong(0);

        WorkThread(int queueSize) { this.queue = new LinkedBlockingQueue<>(queueSize); setDaemon(true); }

        boolean submit(FutureTask<?> t) { return queue.offer(t); }
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    FutureTask<?> t = queue.poll(1, TimeUnit.SECONDS);
                    if (t != null) { active.set(true); try { t.run(); } finally { active.set(false); completed.incrementAndGet(); } }
                } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
            }
        }
    }
}
```

#### 集成到 WorkerListener

```java
private final HashedExecutor keyOrderedExec = new HashedExecutor("worker-listener", 8, 10000);

void handleHot(WorkerMessage wm) {
    keyOrderedExec.submit(wm.getCacheKey(), () -> processHot(wm));
}
```

---

## 9. Grafana 预置 Dashboard

### 参考来源

Camellia 的 `StatsPrometheusConverter` 将监控数据导出为 Prometheus 格式。

### HotKey 现状

已通过 Micrometer 导出指标，无预置 Dashboard。

### 新增内容

**文件：`docs/grafana/hotkey-dashboard.json`**

```json
{
  "dashboard": {
    "title": "HotKey Cache",
    "panels": [
      { "title": "TopK Size", "targets": [{ "expr": "hotkey_topk_size" }] },
      { "title": "L1 Cache Hit Ratio", "targets": [{ "expr": "hotkey_l1_cache_hit_ratio" }] },
      { "title": "L1 Cache Size", "targets": [{ "expr": "hotkey_l1_cache_size" }] },
      { "title": "SingleFlight Inflight", "targets": [{ "expr": "hotkey_singleflight_inflight" }] },
      { "title": "Reporter Queue Depth", "targets": [{ "expr": "hotkey_reporter_queue_depth" }] },
      { "title": "Worker Alive", "targets": [{ "expr": "hotkey_worker_alive" }] },
      { "title": "Version Degraded", "targets": [{ "expr": "hotkey_version_degraded_total" }] },
      { "title": "CPU Load", "targets": [{ "expr": "hotkey_cpu_load" }] }
    ]
  }
}
```

---

## 10. 集成方式

### Maven 模块结构

```
hotkey-parent/
├── common/                          ← 核心 Starter
│   └── src/main/java/io/github/hyshmily/hotkey/
│       ├── callback/                ← 新增：事件回调系统
│       ├── util/
│       │   ├── circuitbreaker/       ← 新增：熔断器
│       │   ├── compress/            ← 新增：LZ4 压缩
│       │   ├── config/              ← 新增：动态配置
│       │   └── executor/            ← 新增：HashedExecutor
│       └── hotkeydetector/
│           └── ExactSlidingWindowCounter.java ← 新增
├── hotkey-console/                  ← 新增模块（可选依赖 netty）
├── worker/
└── demo-app/
```

### 新增配置属性

```properties
# ===== 管理控制台 =====
hotkey.console.enabled=true
hotkey.console.port=17070

# ===== 熔断器 =====
hotkey.circuit-breaker.enabled=true
hotkey.circuit-breaker.fail-threshold-percentage=0.5
hotkey.circuit-breaker.request-volume-threshold=20
hotkey.circuit-breaker.single-test-interval-ms=5000

# ===== 缓存压缩 =====
hotkey.local.cache.compress=true
hotkey.local.cache.compress-threshold=1024

# ===== 动态配置（运行时修改）=====
hotkey.local.min-count=10
hotkey.local.hard-ttl-ms=300000
hotkey.local.soft-ttl-ms=30000
```

### 实现优先级

| 优先级 | 功能 | 代码量 | 核心收益 |
|--------|------|--------|---------|
| **P0** | 事件回调系统（含客户端监听器） | ~500 行 | 扩展性，告警集成 |
| **P0** | 管理控制台（优先扩展现有 Actuator 端点） | ~500 行 | 运维可观测性 |
| **P0** | 熔断器（推荐 Resilience4j 包装或自定义实现） | ~300 行 | 系统稳定性 |
| **P1** | 管理控制台（独立 Netty HTTP 服务器备选方案） | ~800 行 | 非 Spring Boot 环境 |
| **P1** | 优雅上下线（Worker 模块） | ~200 行 | 生产可靠性 |
| **P1** | LZ4 压缩 | ~250 行 | 网络/存储成本 |
| **P1** | SDK 双缓冲收集器 | ~200 行 | 热点路径性能 |
| **P2** | 精确计数器 | ~300 行 | 补充概率型检测 |
| **P2** | TopN 跨 Worker Redis 协调 | ~400 行 | 集群 TopN 一致 |
| **P2** | Grafana Dashboard | ~100 行 JSON | 可视化 |
| **P2** | 动态配置框架 | ~150 行 | 运行时调优 |
| **P2** | 流量分类统计 | ~100 行 | 可观测性 |
| **P3** | HashedExecutor | ~400 行 | 事件顺序保证 |

---

## 11. 更多可参考的设计细节

本章分析 Camellia 项目中未在前文作为独立功能建议列出、但同样值得参考的设计模式和实现细节。
按「HotKey 可直接借鉴」到「仅做知识参考」排列。

---

### 11.1 线程模型与队列设计

#### Camellia 的做法

Camellia 的 hot-key server 使用多级线程模型：

```
Netty IO Threads (boss/worker)
    → 反序列化为 HotKeyPack
    → HotKeyPackBizServerHandler.onPushPack()
        → 按 namespace|key 哈希到 target queue (selectQueue)
            → HotKeyCalculatorQueue (每个队列一个单线程消费者)
                → HotKeyCalculator 单线程处理
                    → HotKeyCounter 更新（非线程安全，无需锁）
                    → TopNCounter 更新（非线程安全）
                    → HotKeyEventHandler 提交到 HashedExecutor
                        → 回调 + 通知发送（异步）
```

关键设计点：

1. **`HotKeyCalculatorQueue`**（`HotKeyCalculatorQueue.java:23-125`）：
   - 每个 queue 绑定一个 `HotKeyCalculator`（单线程消费者）
   - 支持 8 种队列实现（通过 `WorkQueueType` 枚举选择），包括 JCTools 的高性能无锁 MPSC 队列
   - 使用 `LongAdder pendingSize` 实现 O(1) 的 Pending 检测（比 `queue.size()` 的 O(n) 更高效）
   - 独立的调度线程每 5s 打印队列满的警告日志
   - 快速拒绝路径：`pendingSize.sum() > bizWorkQueueCapacity` 优先检查

```java
// HotKeyCalculatorQueue 的快速拒绝路径
public void push(List<KeyCounter> counters, String source) {
    int size = counters.size();
    boolean success;
    if (pendingSize.sum() > bizWorkQueueCapacity) {  // 快速检查
        success = false;
    } else {
        pendingSize.add(size);
        success = queue.offer(new Object[]{counters, source});
        if (!success) {
            pendingSize.add(size * -1);  // 回滚
        }
    }
    if (!success) {
        fail.add(size);
        discardCount.add(size);
    }
}
```

2. **`HotKeyEventHandler`**（`HotKeyEventHandler.java:19-108`）：
   - 使用 `CamelliaHashedExecutor(hot-key-event, cpuNum, 10000)` 按 namespace 路由事件
   - 热 key 通知去重：通过 `NamespaceCamelliaLocalCache` 缓存热 key，检查 TTL 是否过半决定是否重新通知
   - UPDATE/DELETE 去重：`keyUpdateCache.putIfAbsent(namespace, key, true, 100L)`，100ms 内只下发一次

```java
// HotKeyEventHandler 的热 key 通知逻辑
public void newHotKey(HotKey hotKey, Rule rule, long current, Set<String> sourceSet) {
    executor.submit(hotKey.getNamespace(), () -> {
        if (hotKey.getAction() == KeyAction.QUERY && hotKey.getExpireMillis() != null) {
            Long expireMillis = hotKeyCache.get(hotKey.getNamespace(), hotKey.getKey(), Long.class);
            boolean needNotify = false;
            if (expireMillis == null) {
                needNotify = true;  // 首次热 key
            } else {
                long ttl = hotKeyCache.ttl(hotKey.getNamespace(), hotKey.getKey());
                if (ttl < expireMillis / 2) {
                    needNotify = true;  // TTL 过半，重新通知
                }
            }
            if (needNotify) {
                hotKeyCache.put(key, expireMillis, expireMillis);
                hotKeyNotifyService.notifyHotKey(hotKey);
            }
        }
        callbackManager.newHotkey(hotKey, rule, current, sourceSet);
    });
}
```

#### 对 HotKey 的建议

| 改进点 | Camellia 做法 | HotKey 现状 | 建议 |
|--------|--------------|-------------|------|
| 计数线程模型 | 单线程消费 queue，无锁计数 | HeavyKeeper 内部用 `synchronized` 锁 | 对精确计数可增加单线程 queue 模式 |
| Pending 跟踪 | `LongAdder` 独立计数，O(1) | `queue.size()` 可能 O(n) | 队列大的场景用 `LongAdder` |
| 队列选择 | 可配置 8 种队列实现 | 固定 `LinkedBlockingQueue` | 对高性能场景可选 JCTools MPSC |
| 事件去重 | `putIfAbsent(namespace, key, TTL)` | `ConcurrentHashMap` 简单标记 | 增加带 TTL 的去重模式 |
| 事件路由 | `HashedExecutor` 按 namespace 路由 | 随机线程池 | 相同 namespace 的事件按顺序处理 |

---

### 11.2 Netty 推送通道 vs AMQP 广播

#### Camellia 的做法

Camellia 的 hot-key server 与 SDK 之间使用 **长连接 TCP（Netty）** 通信：

```
Server ←→ Client (Netty TCP 长连接)
  ├── HeartbeatPack (双向心跳)
  ├── PushPack (Client → Server: 上报访问计数)
  ├── GetConfigPack (Client → Server: 获取配置)
  ├── NotifyHotKeyPack (Server → Client: 热 key 通知)
  └── NotifyHotKeyConfigPack (Server → Client: 配置变更)
```

关键设计：

1. **`HotKeyNotifyService`**（`HotKeyNotifyService.java:23-101`）：
   - 通过 `ClientConnectHub` 维护 namespace → Channel 映射
   - `notifyHotKey()` 遍历所有订阅了该 namespace 的 channel 推送 `NotifyHotKeyPack`
   - 使用 `SeqManager`（`ConcurrentLinkedHashMap<seqId, CompletableFuture>`）异步等待 ACK

```java
// 异步推送 + 响应处理
public void notifyHotKey(HotKey hotKey) {
    ConcurrentHashMap<String, Boolean> map = ClientConnectHub.getInstance().getMap(hotKey.getNamespace());
    for (String consid : map.keySet()) {
        ChannelInfo channelInfo = ClientConnectHub.getInstance().get(consid);
        if (channelInfo != null) {
            HotKeyPack pack = HotKeyPack.newPack(HotKeyCommand.NOTIFY_HOTKEY,
                new NotifyHotKeyPack(Collections.singletonList(hotKey)));
            CompletableFuture<HotKeyPack> future = sendPack(channelInfo, pack);
            future.thenAccept(p -> { /* 处理响应 */ });
            future.exceptionally(throwable -> { /* 处理异常 */ });
        }
    }
}

private CompletableFuture<HotKeyPack> sendPack(ChannelInfo channelInfo, HotKeyPack hotKeyPack) {
    hotKeyPack.getHeader().setSeqId(channelInfo.genSeqId());
    CompletableFuture<HotKeyPack> future = channelInfo.getSeqManager().putSession(hotKeyPack);
    channelInfo.getCtx().channel().writeAndFlush(hotKeyPack);
    return future;
}
```

2. **`ClientConnectHub`**（`ClientConnectHub.java:1-53`）：
   - `ConcurrentHashMap<String, ChannelInfo>` 全局连接表
   - `ConcurrentHashMap<String, ConcurrentHashMap<String, Boolean>>` namespace → {consid} 订阅关系
   - 连接建立时注册 `InitHandler.channelActive`，断开时 `channelInactive` 清理

3. **`SeqManager`**：`ConcurrentLinkedHashMap<Long, CompletableFuture<HotKeyPack>>`
   - 每个客户端连接独立 SeqManager
   - 发送时 `putSession()`，收到 ACK 时 `complete()`
   - 超时/断开时 `clear()`

#### 对 HotKey 的启示

HotKey 使用 RabbitMQ 作为通信层，有异步、解耦的优势，但推送延迟高于 TCP 直连。
Camellia 的通道模型适合对延迟敏感的场景。

**建议**：在 HotKey 的 Worker 广播场景中，可以借鉴 Camellia 的通道分层模式：

```
HotKey App → RabbitMQ → Worker        （现有：批处理，高延迟）
HotKey App ← RabbitMQ ← Worker        （现有：广播，中等延迟）

// 可选的优化方向：
// 对延迟敏感的热 key 通知，增加直连通道
HotKey App ← Netty TCP ← Worker       （补充：低延迟推送）
```

---

### 11.3 监控指标的多格式导出

#### Camellia 的做法

Camellia 为监控数据提供了 **4 种输出格式**：

| 格式 | 类 | 用途 |
|------|-----|------|
| JSON | `StatsJsonConverter` | `/monitor` 端点，人工查看 |
| Prometheus | `StatsPrometheusConverter` | `/prometheus` 端点，Grafana 采集 |
| Metrics（全量） | `PrometheusMetrics` | `/metrics` 端点，含 JVM/系统指标 |

1. **`StatsJsonConverter`**（`StatsJsonConverter.java:12-75`）
   - 输出：info（线程数、应用名、连接数）、queueStats（每个队列的 pending/discard）、trafficStats（每个 namespace 的 RULE_NOT_MATCH/NORMAL/HOT 统计）、hotKeyInfoList（采样热 key）

```java
// TrafficStats 的 Type 枚举
public static enum Type {
    RULE_NOT_MATCH(1),  // 不匹配规则
    NORMAL(2),          // 正常流量
    HOT(3);             // 热 key 流量
}
```

2. **`StatsPrometheusConverter`**（`StatsPrometheusConverter.java:6-46`）
   - 标准 Prometheus 格式，带 `# HELP` 和 `# TYPE` 注释
   - 指标命名规范：`hot_key_server_connect_count`、`hot_key_server_traffic_detail{namespace="xxx", type="HOT"}`

3. **`PrometheusMetrics`**（`PrometheusMetrics.java:18-152`）
   - 不仅包含业务指标，还包含：JVM 内存、GC 统计、CPU、Uptime、Netty 直接内存

```java
// PrometheusMetrics 导出的部分指标
// vm_name, java_version, uptime, start_time
// free_memory, total_memory, max_memory, heap_memory_usage, netty_direct_memory
// cpu{type="cpu_num"}, cpu{type="usage"}
// gc{name="G1 Young Generation", type="count"}, gc{name=..., type="time"}
// client_connect, queue{name="0", type="pending"}, qps_detail{namespace="ns", type="HOT"}
```

#### 对 HotKey 的建议

HotKey 已有 Micrometer 集成。可以补充：

1. **在 HotKeyEndpoint 中增加流量分类统计**（参考 TrafficStats）：

```java
// 新增统计：按类型区分流量
public enum TrafficType {
    RULE_NOT_MATCH,  // 不匹配规则（直接通过）
    NORMAL,          // 正常流量
    HOT              // 热 key 流量
}

// HotKeyMonitorSnapshot 中增加
private final Map<TrafficType, Long> trafficCounts;
```

2. **在 Prometheus 导出中增加 JVM 级指标**（参考 PrometheusMetrics）：
   - 内存、GC、Uptime、CPU 等基础运维指标

---

### 11.4 配置管理体系的抽象设计

#### Camellia 的做法

Camellia 的配置管理是分层、可插拔的：

```
抽象层：
HotKeyConfigService（抽象类）
  ├── get(namespace) → HotKeyConfig
  ├── init(properties)
  ├── registerCallback(Callback)
  └── invokeUpdate(namespace)

装饰器：
CacheableHotKeyConfigService（包装，增加本地缓存 + 60s 刷新 + 变更检测）

实现类：
  ├── FileBasedHotKeyConfigService    （本地文件，60s 重载）
  ├── ApiBasedHotKeyConfigService     （HTTP API，回调通知）
  ├── NacosHotKeyConfigService        （Nacos，事件监听）
  └── EtcdHotKeyConfigService         （Etcd，watch 机制）
```

关键设计：

1. **`HotKeyConfigService`**（`HotKeyConfigService.java:13-51`）：
   - 抽象类而非接口：方便后续增加公共逻辑
   - 内置 `List<Callback>` + `synchronized registerCallback`：支持多个配置变更监听器

2. **`CacheableHotKeyConfigService`**（`CacheableHotKeyConfigService.java:19-89`）：
   - `ConcurrentHashMap<String, HotKeyConfig>` 缓存结果
   - 每 60s 全量刷新所有已缓存的 namespace
   - `HotKeyConfigUtils.isChange()` 通过 JSON 字符串比较检测是否有变化
   - `registerCallback()` 链式包装：先刷新缓存，再通知注册的回调

```java
// 链式回调注册
public void registerCallback(HotKeyConfigService.Callback callback) {
    hotKeyConfigService.registerCallback(namespace -> {
        reload(namespace, false);  // 先刷新缓存
        callback.update(namespace);  // 再通知外部
    });
}
```

3. **`ConfigInitUtil`**（`ConfigInitUtil.java:8-23`）：通过类名自动选择配置源

4. **`ConfReloadHolder`**（`ConfReloadHolder.java:6-19`）：静态 holder，控制台 `/reload` 端点触发全部重新加载

5. **Extension 模块**：Nacos 和 Etcd 作为独立 Maven 模块发布，通过 classpath 自动发现

#### 对 HotKey 的建议

HotKey 的 `RuleMatcher` 目前仅支持 API 方式添加规则。可以借鉴 Camellia 的分层设计：

```java
// 配置源抽象
public interface RuleConfigSource {
    List<Rule> loadRules();
    void addListener(RuleChangeListener listener);
}

// 内置实现
public class LocalFileRuleConfigSource implements RuleConfigSource { ... }
public class NacosRuleConfigSource implements RuleConfigSource { ... }
public class RedisRuleConfigSource implements RuleConfigSource { ... }

// 装饰器
public class CachingRuleConfigSource implements RuleConfigSource {
    private final RuleConfigSource delegate;
    private volatile List<Rule> cachedRules;
    private final ScheduledExecutorService scheduler = ...;
    // 每 30s 刷新
}
```

---

### 11.5 TopN 统计的跨节点协调

#### Camellia 的做法

Camellia 的 `TopNCounterManager` 实现了跨节点的 TopN 协调：

1. **双缓冲 + 定期收集**：
   - 每个 TopNCounter 使用 `AtomicBoolean backup` 在两组 Caffeine 缓存间切换
   - `tinyCollect()` 每 `topnTinyCollectSeconds`（默认 5s）收集一次活跃缓存
   - `collect()` 每 `topnCollectSeconds`（默认 60s）汇总并写入 Redis

2. **Redis 协调去重**：
   - 所有节点的 TopN 写入 Redis Sorted Set（score = maxQps）
   - 只有抢到 Redis 锁的节点触发 `HotKeyTopNCallback`

```java
// TopNCounterManager 的收集流程
private void scheduleCollect() {
    // 1. 收集所有 namespace 的 TopN
    List<TopNStatsResult> results = new ArrayList<>();
    for (String namespace : namespaceSet) {
        results.add(topNCounterMap.get(namespace).collect());
    }
    // 2. 写入 Redis Sorted Set
    for (TopNStatsResult result : results) {
        template.zadd(namespaceKeys, timestamp, result.getNamespace());
        String key = mergeKey(result.getNamespace(), timeKey);
        // key = camellia:hot.key:topN:{namespace}:{timeKey}
        for (TopNStats stats : result.getTopN()) {
            template.zadd(key, (double) stats.getMaxQps(), JSON.toJSONString(stats));
        }
        template.expire(key, properties.getTopnRedisExpireSeconds());
    }
    // 3. 延迟半周期，让所有节点写完
    callbackScheduler.schedule(() -> callback(timeKey),
        properties.getTopnCollectSeconds() / 2, TimeUnit.SECONDS);
}

private void callback(String timeKey) {
    // 4. 用 Redis 分布式锁确保每个 namespace 只有一个节点触发回调
    for (String namespace : namespaces) {
        CamelliaRedisLock lock = CamelliaRedisLock.newLock(template, lockKey, expire, expire);
        if (lock.tryLock()) {
            callbackManager.topN(result);  // 触发全局回调
        }
    }
}
```

3. **时间对齐**：
   - 各节点的收集周期起始时间对齐到整秒的中间位置
   - `nearestTime = (now / collectSec) * collectSec + (collectSec / 2) * 1000`

#### 对 HotKey 的建议

HotKey 的 HeavyKeeper 通过 `fading()` 周期性衰减，但没有 TopN 持久化和跨节点汇总。
可以借鉴：

1. **TopN 持久化到 Redis**：定期将 `HeavyKeeper.list()` 的结果写入 Redis
2. **跨 Worker TopN 汇总**：Worker 节点将 TopN 写入 Redis，通过锁协调回调
3. **TopN 历史回溯**：带时间戳的 TopN 可用于历史趋势分析

```java
// HotKey 可新增的 TopN 持久化逻辑
public class TopkPersistService {
    private final HeavyKeeper workerTopK;
    private final StringRedisTemplate redis;
    private final String appName;

    @Scheduled(fixedDelay = 60_000)
    public void persist() {
        String timeKey = new SimpleDateFormat("yyyy-MM-dd#HH:mm:ss")
            .format(new Date(System.currentTimeMillis() / 60_000 * 60_000));
        List<Item> topK = workerTopK.listTopN(100);
        String redisKey = "hotkey:topN:" + appName + ":" + timeKey;
        for (Item item : topK) {
            redis.opsForZSet().add(redisKey, item.key(), item.count());
        }
        redis.expire(redisKey, Duration.ofDays(3));
    }
}
```

---

### 11.6 热 key 变更通知的去重与渐进式 TTL

#### Camellia 的做法

`HotKeyEventHandler` 实现了智能的热 key 通知策略：

1. **通知去重**：通过 `NamespaceCamelliaLocalCache` 缓存 `key → expireMillis`
2. **渐进式 TTL 续期**：仅在 TTL 过半时重新通知（避免频繁推送）
3. **UPDATE/DELETE 去重**：`keyUpdateCache.putIfAbsent(namespace, key, true, 100L)`，100ms 内只推送一次

```java
// 热 key 通知条件
if (hotKey.getAction() == KeyAction.QUERY && hotKey.getExpireMillis() != null) {
    Long expireMillis = hotKeyCache.get(hotKey.getNamespace(), hotKey.getKey(), Long.class);
    if (expireMillis == null) {
        needNotify = true;  // 首次热 key
    } else {
        long ttl = hotKeyCache.ttl(hotKey.getNamespace(), hotKey.getKey());
        if (ttl < expireMillis / 2) {
            needNotify = true;  // TTL 过半续期
        }
    }
}
```

#### 对 HotKey 的建议

HotKey 的 `HotKeyCache.loadAndCache()` 中热 key 检测后直接写入 Caffeine，没有渐进式 TTL 续期的概念。
可以借鉴：

```java
// 在 promoteLocalHotkeyIfNeeded 中增加 TTL 过半检测
if (ce.getKeyState() == KeyState.HOT) {
    long ttl = ce.getHardExpireAtMs() - System.currentTimeMillis();
    long totalTtl = ce.getHardTtlMs();
    if (ttl < totalTtl / 2) {
        // TTL 过半，续期 HOT 状态
        return ce.toBuilder()
            .hardExpireAtMs(expireManager.computeHotHardExpireAt())
            .softExpireAtMs(expireManager.computeHotSoftExpireAt())
            .build();
    }
}
```

---

### 11.7 高性能时间缓存

#### Camellia 的做法

`TimeCache.java` 是一个后台线程每 5ms 更新一次的 `volatile long currentMillis`，
替代 `System.currentTimeMillis()` 的高频调用：

```java
public class TimeCache {
    public static volatile long currentMillis = System.currentTimeMillis();

    static {
        Thread t = new Thread(() -> {
            while (true) {
                currentMillis = System.currentTimeMillis();
                try { Thread.sleep(5); } catch (InterruptedException e) { break; }
            }
        }, "time-cache");
        t.setDaemon(true);
        t.start();
    }
}
```

**使用场景**：`HotKeyCounter.update()` 中每次调用都读取 `TimeCache.currentMillis`
代替 `System.currentTimeMillis()`，减少系统调用开销。

#### 对 HotKey 的建议

HotKey 的热路径（`HotKeyCache.get()` → `loadAndCache()` → `promoteLocalHotkeyIfNeeded()`）
中频繁调用 `System.currentTimeMillis()`。可以引入类似的毫秒级时间缓存：

```java
// 在 HotKeyConstants 或工具类中添加
public class TimeSource {
    private static volatile long currentMillis = System.currentTimeMillis();
    private static volatile long nanoTime = System.nanoTime();

    static {
        Thread t = new Thread(() -> {
            while (true) {
                currentMillis = System.currentTimeMillis();
                try { Thread.sleep(1); } catch (InterruptedException e) { break; }
            }
        }, "hotkey-time");
        t.setDaemon(true);
        t.start();
    }

    public static long currentTimeMillis() { return currentMillis; }
}
```

---

### 11.8 回调的防抖机制

#### Camellia 的做法

`HotKeyCallbackManager.newHotkey()` 使用 `NamespaceCamelliaLocalCache` 实现了 5s 级别的防抖：

```java
public void newHotkey(HotKey hotKey, Rule rule, long current, Set<String> sourceSet) {
    Long lastCallbackTime = callbackTimeCache.get(hotKey.getNamespace(), hotKey.getKey(), Long.class);
    if (lastCallbackTime == null) {
        boolean success = callbackTimeCache.putIfAbsent(
            hotKey.getNamespace(), hotKey.getKey(),
            TimeCache.currentMillis, properties.getHotKeyCallbackIntervalSeconds());
        if (success) {
            // 成功占位，执行回调
            executor.submit(() -> hotKeyCallback.newHotKey(hotKeyInfo));
        }
    }
    // lastCallbackTime != null → 在防抖间隔内，跳过
}
```

对同一个 key，在 `hotKeyCallbackIntervalSeconds`（默认 5s）内只触发一次回调。
使用 `putIfAbsent` + TTL 实现，比 `ConcurrentHashMap.computeIfAbsent` 更简洁。

#### 对 HotKey 的建议

HotKey 的 `promoteLocalHotkeyIfNeeded()` 在每次读路径都会检查，可以加入类似的防抖：

```java
// HotKeyCache 中新增防抖缓存
private final Cache<String, Boolean> callbackDedup = Caffeine.newBuilder()
    .expireAfterWrite(5, TimeUnit.SECONDS)
    .maximumSize(10000)
    .build();

// 在触发 HOT 回调前检查
String dedupKey = "hotkey:" + cacheKey;
if (callbackDedup.getIfPresent(dedupKey) == null) {
    callbackDedup.put(dedupKey, true);
    callbackManager.fireHotKey(cacheKey, count, KeyState.HOT, "LOCAL", null);
}
```

---

### 11.9 数据校验与配置变更检测

#### Camellia 的做法

`HotKeyConfigUtils` 提供了配置校验和变更检测：

1. **`checkAndConvert(HotKeyConfig)`**（`HotKeyConfigUtils.java`）：
   - 检查 namespace 非空
   - 检查每个 rule 的 name、type、keyConfig 非空
   - 确保 `checkMillis >= 100`，并向上取整到 100ms 的整数倍
   - 确保 `checkThreshold > 0`

2. **`isChange(HotKeyConfig old, HotKeyConfig new)`**：
   - 使用 `JSONObject.toJSONString()` 排序后比较
   - 忽略对象引用差异，只关心实际内容

```java
public static boolean isChange(HotKeyConfig oldConfig, HotKeyConfig newConfig) {
    if (oldConfig == null && newConfig == null) return false;
    if (oldConfig == null || newConfig == null) return true;
    // 使用 JSON 字符串比较，自动忽略字段顺序差异
    return !JSONObject.toJSONString(oldConfig).equals(JSONObject.toJSONString(newConfig));
}
```

#### 对 HotKey 的建议

HotKey 的 `RuleMatcher.syncRules()` 已有版本号比较。可以增加：

1. **规则数据合法性校验**：pattern 格式检查、正则编译验证
2. **配置变更检测**：在 `addRule()`/`removeRule()` 中增加变更日志输出
3. **备份快照**：修改规则前备份到 Redis

---

### 11.10 命名约定与日志分类

#### Camellia 的做法

1. **回调日志分离**：使用独立的 Logger 名称

```java
// LoggingHotKeyCallback.java
private static final Logger logger = LoggerFactory.getLogger("camellia-hot-key");

// LoggingHotKeyTopNCallback.java
private static final Logger logger = LoggerFactory.getLogger("camellia-hot-key-topn");

// LoggingMonitorCallback.java
private static final Logger logger = LoggerFactory.getLogger("camellia-monitor-collect");

// LoggingHotKeyCacheStatsCallback.java
private static final Logger logger = LoggerFactory.getLogger("camellia-hot-key-cache-stats");
```

这样可以在 logback.xml 中为不同类别配置独立的输出文件、级别和格式：

```xml
<logger name="camellia-hot-key" level="INFO" additivity="false">
    <appender-ref ref="HOTKEY_FILE"/>
</logger>
<logger name="camellia-hot-key-topn" level="INFO" additivity="false">
    <appender-ref ref="TOPN_FILE"/>
</logger>
```

2. **线程命名约定**：所有线程使用 `camellia-` 前缀

```java
// ThreadFactory 示例
new CamelliaThreadFactory("hot-key-callback")
new CamelliaThreadFactory("camellia-hot-key-topn-collect")
new CamelliaThreadFactory("hot-key-calculator-queue-full-log")
new CamelliaThreadFactory("camellia-hot-key-topn-callback-scheduler")
```

#### 对 HotKey 的建议

HotKey 已有 `HotKeyConstants.THREAD_PREFIX_HOTKEY` 等常量。
可以增加：

1. **回调日志独立 Logger**：让用户可以分别配置回调日志
2. **所有线程统一前缀**：确保 `jstack` 能一眼识别 HotKey 线程

---

### 11.11 枚举值映射的规范方式

#### Camellia 的做法

Camellia 的枚举使用 `value` + `getByValue()` 统一映射：

```java
public static enum KeyAction {
    QUERY(1),
    UPDATE(2),
    DELETE(3);

    private final int value;
    private static final Map<Integer, KeyAction> map = new HashMap<>();
    static {
        for (KeyAction action : KeyAction.values()) map.put(action.value, action);
    }

    public static KeyAction getByValue(int value) {
        return map.get(value);
    }
}
```

这种方法比 `switch` 或连续 `if-else` 更高效、更安全。

#### 对 HotKey 的建议

HotKey 现有枚举（如 `KeyState`、`RuleType` 等）可以使用相同的模式，便于在序列化/反序列化中快速查找。

---

### 11.12 CamelliaMapUtils 的 computeIfAbsent 封装

#### Camellia 的做法

```java
// CamelliaMapUtils.java
public class CamelliaMapUtils {
    public static <K, V> V computeIfAbsent(Map<K, V> map, K key, Function<? super K, ? extends V> mappingFunction) {
        V v = map.get(key);
        if (v == null) {
            v = mappingFunction.apply(key);
            V old = map.putIfAbsent(key, v);
            if (old != null) return old;
        }
        return v;
    }
}
```

为什么不用 `ConcurrentHashMap.computeIfAbsent()`？因为后者**始终加锁**，
在多读少写的场景下，先 get 再 putIfAbsent 性能更好。

#### 对 HotKey 的建议

HotKey 代码中多处使用 `ConcurrentHashMap.computeIfAbsent()`。对读多写少的场景
可以改用类似模式优化。

---

### 11.13 源追踪 (Source Tracking)

#### Camellia 的做法

`HotKeyCounter` 和 `TopNCounter.Counter` 都维护了一个 `sourceSet`：

```java
// HotKeyCounter 中
private Set<String> sourceSet;
public void update(long count, String source) {
    if (source != null) {
        if (sourceSet == null) sourceSet = new HashSet<>();
        if (sourceSet.size() >= maxHotKeySourceSetSize) sourceSet.clear();
        sourceSet.add(source);
    }
}
```

这个 `sourceSet` 记录了谁上报了这个热 key，在 `HotKeyInfo` 中随回调一起提供，
便于排查问题。

#### 对 HotKey 的建议

HotKey 的热 key 检测没有来源追踪。可以在 `HotKeyDetector.add()` 中增加来源参数，
在回调中携带，帮助定位哪个 App 实例触发了热 key 检测。

---

### 11.14 设计细节总结表

下表汇总了所有 Camellia 设计细节及其对 HotKey 的参考价值：

| # | 设计细节 | Camellia 文件 | HotKey 借鉴难度 | 参考价值 |
|---|---------|--------------|----------------|---------|
| 1 | 生产者-消费者队列 + LongAdder pending | `HotKeyCalculatorQueue.java` | 低 | 队列监控更准确 |
| 2 | 8 种可切换的队列实现 | `WorkQueueType.java` | 低 | 高性能场景可选 |
| 3 | 热 key 通知 TTL 过半续期 | `HotKeyEventHandler.java:52-64` | 低 | 减少推送频率 |
| 4 | UPDATE/DELETE 100ms 去重 | `HotKeyEventHandler.java:94` | 低 | 防止重复通知 |
| 5 | Netty CompletableFuture 异步推送 | `HotKeyNotifyService.java:95-100` | 中 | 替代同步发送 |
| 6 | 命名空间连接管理 | `ClientConnectHub.java` | 中 | 精细化通道管理 |
| 7 | 监控多格式输出 (JSON/Prometheus/Metrics) | `StatsJsonConverter.java`、`PrometheusMetrics.java` | 低 | 丰富可观测性 |
| 8 | 流量分类统计 (RULE_NOT_MATCH/NORMAL/HOT) | `TrafficStats.java` | 低 | 更细粒度的统计 |
| 9 | 配置源抽象 + 装饰器模式 | `HotKeyConfigService.java`、`CacheableHotKeyConfigService.java` | 中 | 扩展多种配置后端 |
| 10 | Nacos/Etcd 扩展模块 | `NacosHotKeyConfigService.java`、`EtcdHotKeyConfigService.java` | 中 | 企业集成 |
| 11 | TopN Redis 持久化 + 跨节点协调 | `TopNCounterManager.java` | 高 | 集群 TopN 一致 |
| 12 | 时间缓存 5ms 级 | `TimeCache.java` | 低 | 减少系统调用 |
| 13 | 回调 5s 防抖 | `HotKeyCallbackManager.java:59-80` | 低 | 防止回调风暴 |
| 14 | 配置校验 + JSON 变更检测 | `HotKeyConfigUtils.java` | 低 | 配置可靠性 |
| 15 | 日志按类别分离 | `LoggingHotKeyCallback.java` | 低 | 便于日志管理 |
| 16 | 枚举 value 映射 `getByValue()` | `KeyAction.java`、`TrafficStats.Type.java` | 低 | 序列化友好 |
| 17 | `CamelliaMapUtils` 读优化 | `CamelliaMapUtils.java` | 低 | 读多写少场景 |
| 18 | 来源追踪 sourceSet | `HotKeyCounter.java:56-64` | 低 | 问题排查 |

---

### 11.15 对现有 HotKey 代码的具体优化建议

#### 优化 1：HeavyKeeper 衰减的 LongAdder 跟踪

Camellia 使用 `LongAdder` 跟踪队列 pending 数量。HeavyKeeper 的 `fading()` 可以增加进度日志：

```java
// HeavyKeeper.java 现有方法
public void fading() {
    // ... 现有逻辑 ...
    // 新增：跟踪衰减消耗的时间
    long start = System.nanoTime();
    // ... 衰减逻辑 ...
    long elapsed = System.nanoTime() - start;
    if (elapsed > TimeUnit.MILLISECONDS.toNanos(100)) {
        log.warn("HeavyKeeper fading took too long: {}ms", TimeUnit.NANOSECONDS.toMillis(elapsed));
    }
}
```

#### 优化 2：HotKeyCache 读路径增加流量统计

Camellia 的 `TrafficStats` 区分 RULE_NOT_MATCH / NORMAL / HOT。
HotKey 的 `loadAndCache()` 可以在每个分支增加计数：

```java
// HotKeyCache 新增
private final AtomicLong hotTraffic = new AtomicLong();
private final AtomicLong normalTraffic = new AtomicLong();
private final AtomicLong blockedTraffic = new AtomicLong();

// 在 loadAndCache 各分支：
if (hotKeyDetector.contains(cacheKey)) {
    hotTraffic.incrementAndGet();
    // ... HOT 路径 ...
} else {
    normalTraffic.incrementAndGet();
    // ... NORMAL 路径 ...
}
```

#### 优化 3：使用 `ConcurrentHashMap.compute` 替代 DCL 模式

`CacheExpireManager.triggerBackgroundRefresh()` 中的 `pendingRefreshes.putIfAbsent()`
可以改用 `compute()` 实现原子操作。

#### 优化 4：回调执行使用独立线程池

Camellia 的回调使用独立 `ThreadPoolExecutor` + `DiscardPolicy`，避免影响主线程。
HotKey 如果实现回调系统，也应使用独立线程池。

---

### 11.16 二进制协议设计 (Pack/Unpack Codec)

#### Camellia 的做法

Camellia 使用自定义二进制协议而非 JSON 进行 SDK⇄Server 通信（`HotKeyPack.java`、`HotKeyPackHeader.java`、`HotKeyPackBody.java`）：

```
HotKeyPack 结构:
┌─────────────────────────────────────────────┐
│ length(4B) │ header │ body (可选, 按 command) │
└─────────────────────────────────────────────┘

Header 结构:
┌──────────────────────────┐
│ command(1B) │ seqId(8B) │ tag(1B) │
└──────────────────────────┘
  command: HEARTBEAT(1) / GET_CONFIG(2) / PUSH(3)
           NOTIFY_HOTKEY(4) / NOTIFY_CONFIG(5) / HOT_KEY_CACHE_STATS(6)
  tag: bit0=EMPTY_BODY, bit1=ACK
  seqId: 用于请求-响应关联 (SeqManager)
```

Body 使用 `camellia-codec` 的 `Props` / `ArrayMable` 序列化（`HotKeyConfigPackUtils.java`）：
- `Props`：key-value 对（String→String）
- `ArrayMable`：数组（如 `KeyCounter` 列表）

关键类：
- `HotKeyPackEncoder.java`：`MessageToMessageEncoder<HotKeyPack>`，调用 `pack.encode(allocator)`
- `HotKeyPackDecoder.java`：`ByteToMessageDecoder`，读 4B 长度前缀，最多 40MB，解码为 `HotKeyPack`
- `SeqManager.java`：`ConcurrentLinkedHashMap<seqId, CompletableFuture<HotKeyPack>>`

**与 HotKey 的对比**：HotKey 使用 RabbitMQ + Jackson JSON，消息头带 `type`/`version`/`degraded` 等属性。Camellia 的二进制协议更轻量，但 HotKey 的 AMQP 模式更适合解耦场景。

---

### 11.17 SDK 端采集器的双缓冲设计

#### Camellia 的做法

SDK 端的访问计数采集器（`IHotKeyCounterCollector.java`）使用**双缓冲 AtomicBoolean 切换**，有 3 种实现：

| 实现类 | 后端 | 适用场景 |
|--------|------|---------|
| `CaffeineCollector` | Caffeine cache | 默认，安全 |
| `ConcurrentLinkedHashMapCollector` | ConcurrentLinkedHashMap | LRU 友好 |
| `ConcurrentHashMapCollector` | ConcurrentHashMap | 超高性能，不限制容量 |

双缓冲核心逻辑：

```java
// CaffeineCollector 的双缓冲模式
private final AtomicBoolean backup = new AtomicBoolean(false);
private final Map<String, Map<String, LongAdder>> map1;
private final Map<String, Map<String, LongAdder>> map2;

public void push(String namespace, String key, int action, int count) {
    // 写入当前活跃集合
    Map<String, Map<String, LongAdder>> active = backup.get() ? map2 : map1;
    LongAdder adder = active.computeIfAbsent(ns, k -> new ConcurrentHashMap<>())
        .computeIfAbsent(uniqueKey, k -> new LongAdder());
    adder.add(count);
}

public List<KeyCounter> collect() {
    // 翻转标志 → 旧集合不再被写入
    backup.set(!backup.get());
    // 遍历旧集合, 收集 KeyCounter, 清空
    Map<String, Map<String, LongAdder>> inactive = backup.get() ? map1 : map2;
    // ... 遍历 inactive，sumThenReset()，放入 list ...
    inactive.clear();
    return list;
}
```

默认 1s 推送一次，每次最多 5000 条。

**与 HotKey 的对比**：HotKey 的 `HotKeyReporter` 使用 Caffeine 做聚合（`expireAfterAccess(30s), max 100k`），推送间隔 50ms。Camellia 的 1s 间隔 + 5k 批量更适合高吞吐场景。

---

### 11.18 BeanFactory 扩展机制

#### Camellia 的做法

Camellia 使用简单的 `BeanFactory` 接口来实现回调类和配置源的可插拔：

```java
// BeanFactory.java
public interface BeanFactory {
    Object getBean(Class<?> clazz);
}

// DefaultBeanFactory.java — 通过无参构造器创建实例
public class DefaultBeanFactory implements BeanFactory {
    private final ConcurrentHashMap<Class<?>, Object> cache = new ConcurrentHashMap<>();
    @Override
    public Object getBean(Class<?> clazz) {
        return cache.computeIfAbsent(clazz, k -> k.getConstructor().newInstance());
    }
}
```

通过类名配置切换实现（`HotKeyServerProperties.java`）：

```properties
# 回调类配置
hotKeyCallbackClassName=com.example.MyHotKeyCallback
topNCallbackClassName=com.example.MyTopNCallback
monitorCallbackClassName=com.example.MyMonitorCallback
hotKeyCacheStatsCallbackClassName=com.example.MyStatsCallback

# 配置源配置
hotKeyConfigServiceClassName=com.example.MyConfigService
```

`ConfigInitUtil.java` 中根据类名选择：

```java
public static HotKeyConfigService initHotKeyConfigService(HotKeyServerProperties properties) {
    String className = properties.getHotKeyConfigServiceClassName();
    if (className.equals(FileBasedHotKeyConfigService.class.getName())) {
        service = new FileBasedHotKeyConfigService();
    } else if (className.equals(ApiBasedHotKeyConfigService.class.getName())) {
        service = new ApiBasedHotKeyConfigService();
    } else {
        service = (HotKeyConfigService) properties.getBeanFactory().getBean(BeanInitUtils.parseClass(className));
    }
    service.init(properties);
    return service;
}
```

**对 HotKey 的参考价值**：HotKey 的 auto-configuration 使用 `@ConditionalOnMissingBean`，Spring 环境下更灵活。Camellia 的 className 方式适合非 Spring Boot 场景。

---

### 11.19 优雅上下线与状态管理

#### Camellia 的做法

Camellia 的 `ConsoleServiceAdaptor` 支持 **ONLINE/OFFLINE** 状态管理：

```java
// ServerStatus.java (来源: camellia-hot-key-server/netty/ServerStatus.java)
public enum Status { ONLINE, OFFLINE }

public class ServerStatus {
    private static volatile Status status = Status.ONLINE;
    private static volatile long lastAccessTime = System.currentTimeMillis();

    public static boolean isIdle() {
        return System.currentTimeMillis() - lastAccessTime > IDLE_CHECK_INTERVAL;
    }
}
```

控制台端点：

| 端点 | 行为 |
|------|------|
| `POST /online` | `ServerStatus.setStatus(ONLINE)`，恢复正常服务 |
| `POST /offline` | `ServerStatus.setStatus(OFFLINE)`，不再接受新请求 |
| `GET /status` | 返回 ONLINE 或 OFFLINE（OFELINE 且 idle 时返回错误码） |
| `GET /check` | 健康检查，始终返回 200 |

流程：
1. **上线**：设为 ONLINE，`ClientConnectHub` 开始接受连接
2. **下线**：设为 OFFLINE，等待现有连接处理完毕（`isIdle()` 返回 true）
3. `ConsoleServiceAdaptor.status()`：`OFFLINE 且 !isIdle()` 时返回 error，提示还有未完成请求

**对 HotKey 的建议**：HotKey 的 Worker 节点停机维护时，可以先宣告 OFFLINE，等积压的 Report 消费完再关闭进程。

---

### 11.20 时间桶非对齐处理

#### Camellia 的做法

`HotKeyCounter.slideToNextBucket()` 处理了**多步滑动**的场景——由于上层可能间隔较长时间才调用 `update()`，可能一次跳过多个桶：

```java
// HotKeyCounter.java:76-98
private void slideToNextBucket(int step) {
    if (step >= bucketSize) {
        // 跳过了整个窗口 → 全部清空
        for (int i = 0; i < bucketSize; i++) buckets[i] = 0;
        index = 0;
        return;
    }
    // 处理环形缓冲区的边界
    if (index + step < bucketSize) {
        // 简单情况：清空 (index+1 .. index+step)
        for (int i = index + 1; i <= index + step; i++) buckets[i] = 0;
        index = index + step;
    } else {
        // 跨越了缓冲区末尾
        for (int i = index + 1; i < bucketSize; i++) buckets[i] = 0;
        for (int i = 0; i <= index + step - bucketSize; i++) buckets[i] = 0;
        index = index + step - bucketSize;
    }
}
```

这个处理在 HotKey 的 HeavyKeeper 中不需要（概率型结构不按时间桶滑动），
但如果要实现 `ExactSlidingWindowCounter`，可以**直接参考**这个环形缓冲区滑动算法。

---

### 11.21 HotKeyConfig 校验与转换逻辑

#### Camellia 的做法

`HotKeyConfigUtils.checkAndConvert(HotKeyConfig)` 的完整校验逻辑（来源：`camellia-hot-key-common/utils/HotKeyConfigUtils.java`）：

```java
public static boolean checkAndConvert(HotKeyConfig config) {
    if (config == null) return false;
    String namespace = config.getNamespace();
    if (namespace == null || namespace.isEmpty()) return false;

    List<Rule> rules = config.getRules();
    if (rules == null || rules.isEmpty()) return false;

    for (Rule rule : rules) {
        if (rule.getName() == null || rule.getName().isEmpty()) return false;
        if (rule.getType() == null) return false;
        if (rule.getKeyConfig() == null || rule.getKeyConfig().isEmpty()) return false;

        // 确保 checkMillis >= 100，并向上取整到 100ms
        long checkMillis = rule.getCheckMillis();
        if (checkMillis < 100) return false;
        long mod = checkMillis % 100;
        if (mod != 0) {
            rule.setCheckMillis(checkMillis + 100 - mod); // 向上取整
        }
        // 确保 checkThreshold > 0
        if (rule.getCheckThreshold() <= 0) return false;
    }
    return true;
}
```

**与 HotKey 的对比**：HotKey 的 `Rule` 类在 `prepare()` 中做了正则编译，但缺少 `checkMillis` / `checkThreshold` 等业务关键字段的校验和规范化（如 100ms 对齐）。

---

### 11.22 设计模式总结

Camellia 在整个 hot-key 模块中使用的设计模式：

| 设计模式 | 使用位置 | 作用 |
|---------|---------|------|
| **生产者-消费者** | `HotKeyCalculatorQueue` + `HotKeyCalculator` | 解耦 IO 线程和计算线程 |
| **双缓冲 (Double Buffer)** | `TopNCounter` 的 `AtomicBoolean backup` | 无锁切换活跃/收集状态 |
| **装饰器 (Decorator)** | `CacheableHotKeyConfigService` 包装 `HotKeyConfigService` | 添加缓存和定时刷新能力 |
| **策略 (Strategy)** | `WorkQueueType` 选择 8 种队列实现 | 可替换的队列策略 |
| **模板方法 (Template Method)** | `HotKeyConfigService` 抽象类 | 统一的配置源接口 |
| **观察者 (Observer)** | `HotKeyConfigService.Callback` 列表 | 配置变更通知 |
| **单例 (Singleton)** | `ClientConnectHub.getInstance()` | 全局连接管理 |
| **外观 (Facade)** | `CamelliaHotKeyServer` | 统一的服务启动接口 |
| **适配器 (Adapter)** | `ConsoleServiceAdaptor` | 将业务逻辑适配为 HTTP API |
| **工厂 (Factory)** | `ConfigInitUtil` + `BeanFactory` | 按类名创建配置源实例 |
| **异步响应** | `SeqManager` + `CompletableFuture` | Netty 请求-响应关联 |
| **计数信号量 (Semaphore)** | `HotKeyCallbackManager` 的 `ThreadPoolExecutor` | 控制回调并发 |
| **链式注册** | `CacheableHotKeyConfigService.registerCallback()` | 先刷新缓存再通知 |
| **快速失败 (Fast Fail)** | `HotKeyCalculatorQueue.push()` 先检查 `pendingSize` | O(1) 队列满检测 |

---

### 11.23 Camellia 的局限性与不足

客观分析 Camellia hot-key 模块的设计缺陷：

| 局限性 | 影响 | HotKey 是否已有解决方案 |
|--------|------|----------------------|
| **无 Graceful Degradation**：Server 挂掉时 SDK 无法降级，所有 `push()` 失败 | 系统可用性下降 | ✅ Graceful Degradation + ClusterHealthView |
| **无版本跟踪**：无法处理并发写冲突 | 后到的通知可能覆盖新更新 | ✅ Dual Version Space (ADR-0008) |
| **精确计数内存高**：`long[]` 对百万级 key 内存消耗大 | OOM 风险 | ✅ HeavyKeeper 概率型，固定内存 |
| **无二级缓存**：SDK 侧只有本地缓存 | 缓存命中率受限 | ✅ Redis L2 + Caffeine L1 |
| **无分布式锁** | 无法协调跨进程操作 | ✅ RedisLockProvider |
| **单线程计数器瓶颈**：每个 queue 单线程处理 | 大规模集群需更多 queue | — |
| **依赖 Netty 版本**：需要升级 JDK 或 Netty 版本（Java 21+） | 兼容性受限 | — |
| **配置源类名硬编码**：`ConfigInitUtil` 中 if-else 判断 | 扩展新实现需改核心代码 | ✅ Spring Boot 自动配置 |

---

### 11.24 Camellia 值得 HotKey 借鉴的完整功能清单

| # | 功能 | Camellia | HotKey 现有状态 | 建议优先级 |
|---|------|----------|---------------|-----------|
| 1 | **事件回调** | `HotKeyCallback` 等 4 接口 + 管理器 | 无 | P0 |
| 2 | **管理控制台** | Netty HTTP 控制台，10+ 端点 | 仅 Actuator | P0 |
| 3 | **熔断器** | 滑动窗口 `LongAdder[]` + 半开探测 | 无 | P0 |
| 4 | **回调防抖** | `putIfAbsent` + TTL 防抖 (5s) | 无 | P0 |
| 5 | **配置源抽象** | 4 种实现 + 装饰器 | API only | P1 |
| 6 | **LZ4 压缩** | 自描述头，向后兼容 | 无 | P1 |
| 7 | **动态配置** | `DynamicValueGetter<T>` | 静态配置 | P1 |
| 8 | **热 key TTL 续期** | TTL 过半重新通知 | 固定 TTL | P1 |
| 9 | **优雅上下线** | ONLINE/OFFLINE | 无 | P1 |
| 10 | **精确计数器** | `long[]` 滑动窗口 (100ms 桶) | 仅概率型 HeavyKeeper | P2 |
| 11 | **TopN 持久化** | Redis SortedSet + 跨节点协调 | 仅内存 | P2 |
| 12 | **流量分类统计** | RULE_NOT_MATCH/NORMAL/HOT | 无 | P2 |
| 13 | **时间缓存** | `TimeCache` 5ms 级 | `System.currentTimeMillis()` | P2 |
| 14 | **日志分类** | 独立 Logger 名称 | 统一 Logger | P2 |
| 15 | **源追踪** | `sourceSet` 记录上报来源 | 无 | P2 |
| 16 | **双缓冲收集器** | `AtomicBoolean` 无锁切换 | Caffeine 30s 超时 | P2 |
| 17 | **HashedExecutor** | 按 key 路由线程池 | 通用线程池 | P2 |
| 18 | **枚举 value 映射** | `getByValue()` 静态 map | 无此模式 | P3 |
| 19 | **多队列支持** | 8 种队列可切换 (含 JCTools) | `LinkedBlockingQueue` | P3 |
| 20 | **BeanFactory** | 轻量可插拔 | Spring DI | P3 |

---

---
## 12. 实现注意事项

### 12.1 LZ4 依赖声明

`CacheValueCompressor` 使用 `net.jpountz.lz4:LZ4Factory`，需要在 `common/pom.xml` 中添加：

```xml
<dependency>
    <groupId>org.lz4</groupId>
    <artifactId>lz4-java</artifactId>
    <version>1.8.0</version>
    <optional>true</optional>
</dependency>
```

### 12.2 控制台模块 Netty 依赖

`hotkey-console` 模块依赖 Netty，必须在 `pom.xml` 中声明为 `<optional>true</optional>`：

```xml
<dependency>
    <groupId>io.netty</groupId>
    <artifactId>netty-all</artifactId>
    <version>4.1.100.Final</version>
    <optional>true</optional>
</dependency>
```

### 12.3 熔断器与 ADR-0002 的协调

ADR-0002（SingleFlight Exception-Only Invalidate）规定 SingleFlight 仅在异常时删除缓存条目。
熔断器打开时**不应**返回 `Optional.empty()`，应改为：

1. 如果缓存中有过期数据，返回过期数据（配合 soft-expire 机制）
2. 如果无缓存数据，抛出 `HotKeyBlockedException` 以便调用者感知

更新 ADR-0002 增加熔断器场景说明。

### 12.4 测试策略

根据工作流规则第 8 条，以下修改必须新增 `HotKeyStressTest` 场景：

- **熔断器**：`circuitBreaker_allowsHalfOpenOnSuccess` — 覆盖状态转换、并发访问
- **事件回调**：`callback_firesOnHotKeyPromotion` — 验证 fire-and-forget 语义
- **LZ4 压缩**：`lz4Compressor_roundtripWithMixedCluster` — 混合版本兼容性
- **优雅上下线**：`worker_gracefulShutdown_drainsReports` — Worker 关闭前处理完积压

---

## 附录 A：审查修复记录

| 版本 | 日期 | 修改内容 |
|------|------|---------|
| v1.0 | 2026-06-25 | 初始版本 |
| v1.1 | 2026-06-25 | 新增 §11.16-§11.24；修复 `HotKeyCircuitBreaker` 缺少 `idGen`、`now` 未定义、TOCTOU 竞态 |
| v1.1 | 2026-06-25 | 修复 `HotKeyCacheStats` record getter 命名；修复 Console 默认启用安全风险；添加 `stop()` 方法 |
| v1.1 | 2026-06-25 | 修复 `HashedExecutor` `Math.abs(Integer.MIN_VALUE)` → `Math.floorMod()` |
| v1.1 | 2026-06-25 | 新增客户端监听器功能；更新优先级排序；新增依赖声明、ADR 协调、测试策略章节 |

---

> 本文档所有代码基于 Camellia v1.4.2 和 HotKey v1.1.53 源码分析编写。
> 实际集成时需根据目标版本 API 变化做调整。
