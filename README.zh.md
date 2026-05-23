# HotKey

[**English**](README.md)

HotKey — 基于 [HeavyKeeper](https://github.com/go-kratos/aegis) 算法的 Top-K 热点 Key 检测 & Caffeine/Redis 多级缓存自动预热 Spring Boot Starter（低精度版本）


> [!Important]
> 这是作者在开发过程中总结的经验模块，不能确保生产过程中的可靠性与稳定性。如需完善的热点 key 自动监测和更高精度版本，请参考 [hotkey](https://gitee.com/jd-platform-opensource/hotkey)

## 特点

- **HeavyKeeper 算法** —  Count-Min Sketch + 指数冲突衰减的概率性 Top-K 检测
- **两级缓存** — Caffeine (L1) + Redis (L2)，自动热点缓存
- **热点广播** — 可选 RabbitMQ fanout，分布式的多实例缓存预热
- **Spring Boot 自动配置** — 引入依赖即用，零样板代码

## 快速开始

### 1. 添加依赖

```xml
<dependency>
    <groupId>io.github.hyshmily</groupId>
    <artifactId>hotkey-spring-boot-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

Redis 和 RabbitMQ 依赖均为可选——仅在使用对应功能时才需引入。

### 2. 配置

```yaml
hotkey:
  top-k: 100
  width: 100000
  depth: 5
  decay: 0.92
  min-count: 10
  local-cache-max-size: 1000
  local-cache-ttl-minutes: 5
  decay-period: 20
  broadcast:
    enabled: false
    exchange-name: hotkey.broadcast.exchange
    queue-prefix: hotkey.broadcast
    instance-id: ${server.port}
```

### 3. 使用

```java
@Autowired
private HotKeyCache hotKeyCache;

public Object getData(String hashKey, String fieldKey) {
    return hotKeyCache.get(hashKey, fieldKey);
}

public void updateData(String hashKey, String fieldKey, Object value) {
    // update database...
    hotKeyCache.updateCaffeineIfPresent(hashKey, fieldKey, value);
}
```

### 4. 启用广播（多实例）

```yaml
hotkey:
  broadcast:
    enabled: true
```

广播模式通过 RabbitMQ fanout 在所有实例间同步热点 key。

### 5. 高级

hotKeyCache.get返回类型为Object，用户可自行转换为具体类型：

```java
String value = (String) hotKeyCache.get(hashKey, fieldKey);
```

目前可通过null值判断是否击穿caffeine和Redis两级缓存以扩展业务的数据库回退操作,此逻辑尚未在库中实现（待完善），需用户自行处理。后续会增加更丰富的返回结果以区分不同情况。



## 架构

请求流程：Caffeine L1 → Redis L2 → HeavyKeeper 检测：

```
┌──────────────┐   Caffeine 命中?   ┌──────────────┐
│   Request    │ ────────────────→  │  Caffeine L1 │
│              │ ←────────────────  │   (本地)     │
└──────┬───────┘   返回值           └──────┬───────┘
       │ Caffeine 未命中?                   │ Hot key?
       ↓                                   ↓
┌──────────────┐                   ┌───────────────┐
│   Redis L2   │                   │ HeavyKeeper   │
│   (全局)     │                   │   检测器      │
└──┬───────┬───┘                   └──────┬────────┘
   │ 找到值 │ null (待完善)                │ 被提升?
   ↓       ↓                               ↓
刷新缓存  返回null ───→ DB回退    Caffeine.put + 广播
```

## 模块

| 模块 | 依赖 | 自动配置 |
|------|------|---------|
| `algorithm` | 无 | 始终生效 |
| `cache` (Redis) | `spring-boot-starter-data-redis` | `@ConditionalOnClass` |
| `broadcast` (RabbitMQ) | `spring-boot-starter-amqp` | `@ConditionalOnClass` + 属性开关 |


## 许可证

Apache License 2.0
