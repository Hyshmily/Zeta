# Spring Cache 注解集成 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bridge HotKey hot-key detection into Spring Cache annotations: `@Cacheable`/`@CachePut`/`@CacheEvict` trigger detection, and companion annotations (`@HotKeyCacheTTL`, `@Intercept`, `@Fallback`) extend Spring's cache behavior.

**Architecture:** HotKeySpringCache implements Spring `Cache` interface + TTL-aware enhancements. HotKeyCacheManager implements Spring `CacheManager`. A companion aspect (`HotKeyCacheExtensionAspect`) intercepts Spring cache annotations, reads companion annotations, and passes TTL/intercept/fallback logic around Spring's CacheInterceptor.

**Tech Stack:** Java 17, Spring Boot 3.5.3, Spring Cache, Caffeine, HotKey internals

**Design doc:** `docs/superpowers/specs/2026-06-14-spring-cache-integration-design.md`

---

### Task 1: Add dependency + AutoConfiguration.imports + properties

**Files:**
- Modify: `common/pom.xml`
- Modify: `common/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
- Modify: `common/src/main/java/io/github/hyshmily/hotkey/autoconfigure/HotKeyProperties.java`

- [ ] **Step 1: Add spring-boot-starter-cache dependency**

Add to `common/pom.xml` after the `spring-boot-starter-aop` block:

```xml
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-cache</artifactId>
      <optional>true</optional>
    </dependency>
```

- [ ] **Step 2: Add spring-cache configuration block to HotKeyProperties**

Add inner class before the closing brace:

```java
  @Valid
  private SpringCache springCache = new SpringCache();

  @Data
  public static class SpringCache {
    private boolean enabled = false;
    private String keySeparator = "::";
  }
```

- [ ] **Step 3: Append auto-config class to AutoConfiguration.imports**

```
io.github.hyshmily.hotkey.autoconfigure.HotKeySpringCacheAutoConfiguration
```

- [ ] **Step 4: Verify compilation**

Run: `mvn compile -pl common -q`

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: add spring-boot-starter-cache optional dep + config properties"
```

---

### Task 2: HotKeyCacheContext (ThreadLocal TTL holder)

**Files:**
- Create: `common/src/main/java/io/github/hyshmily/hotkey/cache/HotKeyCacheContext.java`
- Create: `common/src/test/java/io/github/hyshmily/hotkey/cache/HotKeyCacheContextTest.java`

- [ ] **Step 1: Write the failing test**

Create `HotKeyCacheContextTest.java`:

```java
package io.github.hyshmily.hotkey.cache;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class HotKeyCacheContextTest {

  @Test
  void shouldStoreAndClearTtl() {
    HotKeyCacheContext.set(1000L, 200L);
    assertThat(HotKeyCacheContext.get()).containsExactly(1000L, 200L);
    HotKeyCacheContext.clear();
    assertThat(HotKeyCacheContext.get()).isNull();
  }

  @Test
  void shouldReturnNullByDefault() {
    HotKeyCacheContext.clear();
    assertThat(HotKeyCacheContext.get()).isNull();
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl common -Dtest=HotKeyCacheContextTest`
Expected: COMPILATION ERROR

- [ ] **Step 3: Write implementation**

Create `HotKeyCacheContext.java`:

```java
package io.github.hyshmily.hotkey.cache;

public final class HotKeyCacheContext {

  private static final ThreadLocal<long[]> TTL_HOLDER = new ThreadLocal<>();

  private HotKeyCacheContext() {}

  public static void set(long hardTtlMs, long softTtlMs) {
    TTL_HOLDER.set(new long[]{hardTtlMs, softTtlMs});
  }

  public static long[] get() {
    return TTL_HOLDER.get();
  }

  public static void clear() {
    TTL_HOLDER.remove();
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl common -Dtest=HotKeyCacheContextTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: add HotKeyCacheContext for ThreadLocal TTL propagation"
```

---

### Task 3: HotKeySpringCache

**Files:**
- Create: `common/src/main/java/io/github/hyshmily/hotkey/cache/HotKeySpringCache.java`
- Create: `common/src/test/java/io/github/hyshmily/hotkey/cache/HotKeySpringCacheTest.java`

- [ ] **Step 1: Write failing test**

Create `HotKeySpringCacheTest.java` with tests:
- `getName()` returns cache name
- `get(key, Callable)` prefixes key and delegates to `hotKey.get()`
- `get(key, Callable)` with ThreadLocal TTL → delegates to `hotKey.getWithSoftExpire()`
- `put(key, value)` delegates to `hotKey.putThrough()`
- `evict(key)` delegates to `hotKey.invalidate()`
- `get(key)` without Callable → `hotKey.peek()`
- `clear()` removes entries with prefix

- [ ] **Step 2: Write implementation**

Create `HotKeySpringCache.java`:

```java
package io.github.hyshmily.hotkey.cache;

import io.github.hyshmily.hotkey.HotKey;
import java.util.concurrent.Callable;
import org.springframework.cache.support.AbstractValueAdaptingCache;

public class HotKeySpringCache extends AbstractValueAdaptingCache {

  private final String name;
  private final HotKey hotKey;
  private final String separator;

  public HotKeySpringCache(String name, HotKey hotKey, String separator) {
    super(true);
    this.name = name;
    this.hotKey = hotKey;
    this.separator = separator;
  }

  private String prefixed(Object key) { return name + separator + key; }

  @Override public String getName() { return name; }
  @Override public Object getNativeCache() { return this; }

  @Override
  protected Object lookup(Object key) {
    return hotKey.peek(prefixed(key)).orElse(null);
  }

  @Override @SuppressWarnings("unchecked")
  public <T> T get(Object key, Callable<T> valueLoader) {
    long[] ttl = HotKeyCacheContext.get();
    if (ttl != null) {
      return (T) hotKey.getWithSoftExpire(prefixed(key), (Callable<Object>) valueLoader, ttl[0], ttl[1])
        .orElse(null);
    }
    return (T) hotKey.get(prefixed(key), (Callable<Object>) valueLoader).orElse(null);
  }

  @Override
  public void put(Object key, Object value) {
    long[] ttl = HotKeyCacheContext.get();
    if (ttl != null) {
      hotKey.putThrough(prefixed(key), value, () -> {}, ttl[0], ttl[1]);
    } else {
      hotKey.putThrough(prefixed(key), value, () -> {});
    }
  }

  @Override
  public ValueWrapper putIfAbsent(Object key, Object value) {
    Object old = hotKey.getLocalCache().asMap().putIfAbsent(prefixed(key), value);
    return old != null ? toValueWrapper(old) : null;
  }

  @Override
  public void evict(Object key) { hotKey.invalidate(prefixed(key)); }

  @Override
  public void clear() {
    hotKey.getLocalCache().asMap().keySet().removeIf(k -> k.startsWith(name + separator));
  }
}
```

- [ ] **Step 3: Verify tests pass**

Run: `mvn test -pl common -Dtest=HotKeySpringCacheTest`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat: add HotKeySpringCache implementing Spring Cache interface"
```

---

### Task 4: HotKeyCacheManager

**Files:**
- Create: `common/src/main/java/io/github/hyshmily/hotkey/cache/HotKeyCacheManager.java`
- Create: `common/src/test/java/io/github/hyshmily/hotkey/cache/HotKeyCacheManagerTest.java`

- [ ] **Step 1: Write failing test** → cache creation, caching, name listing
- [ ] **Step 2: Write implementation**

```java
package io.github.hyshmily.hotkey.cache;

import io.github.hyshmily.hotkey.HotKey;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

public class HotKeyCacheManager implements CacheManager {
  private final ConcurrentMap<String, Cache> cacheMap = new ConcurrentHashMap<>();
  private final HotKey hotKey;
  private final String separator;

  public HotKeyCacheManager(HotKey hotKey, String separator) {
    this.hotKey = hotKey;
    this.separator = separator;
  }

  @Override
  public Cache getCache(String name) {
    return cacheMap.computeIfAbsent(name, n -> new HotKeySpringCache(n, hotKey, separator));
  }

  @Override
  public Collection<String> getCacheNames() {
    return Collections.unmodifiableSet(cacheMap.keySet());
  }
}
```

- [ ] **Step 3: Verify tests pass**
- [ ] **Step 4: Commit**

---

### Task 5: Auto-configuration for HotKeySpringCache

**Files:**
- Create: `common/src/main/java/io/github/hyshmily/hotkey/autoconfigure/HotKeySpringCacheAutoConfiguration.java`
- Create: `common/src/test/java/io/github/hyshmily/hotkey/autoconfigure/HotKeySpringCacheAutoConfigurationTest.java`

- [ ] **Step 1: Write failing test** → disabled→no bean, missing HotKeyCache→no bean, existing CacheManager respected
- [ ] **Step 2: Write implementation**

```java
@AutoConfiguration(before = HotKeyAnnotationAutoConfiguration.class)
@ConditionalOnBean(HotKeyCache.class)
@ConditionalOnClass({CacheManager.class, HotKey.class})
@ConditionalOnProperty(prefix = "hotkey.spring-cache", name = "enabled", havingValue = "true")
public class HotKeySpringCacheAutoConfiguration {

  @Bean @ConditionalOnMissingBean(CacheManager.class)
  public HotKeyCacheManager hotKeyCacheManager(HotKey hotKey, HotKeyProperties props) {
    return new HotKeyCacheManager(hotKey, props.getSpringCache().getKeySeparator());
  }
}
```

- [ ] **Step 3: Verify tests pass** + `mvn test -pl common` for full regression
- [ ] **Step 4: Commit**

---

### Task 6: HotKeyCacheExtensionAspect

**Files:**
- Create: `common/src/main/java/io/github/hyshmily/hotkey/annotation/HotKeyCacheExtensionAspect.java`
- Modify: `HotKeySpringCacheAutoConfiguration.java` (add aspect bean)

- [ ] **Step 1: Write implementation**

```java
package io.github.hyshmily.hotkey.annotation;

import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.cache.HotKeyCacheContext;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.RequiredArgsConstructor;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.Ordered;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.core.annotation.Order;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

@Aspect
@Order(Ordered.HIGHEST_PRECEDENCE)
@RequiredArgsConstructor
public class HotKeyCacheExtensionAspect {

  private final HotKey hotKey;
  private final SpelExpressionParser parser = new SpelExpressionParser();
  private final Map<String, Expression> expressionCache = new ConcurrentHashMap<>();
  private final ParameterNameDiscoverer parameterNameDiscoverer = new DefaultParameterNameDiscoverer();
  private final Map<Method, String[]> paramNamesCache = new ConcurrentHashMap<>();
  private final Map<Method, Method> fallbackMethodCache = new ConcurrentHashMap<>();

  @Around("@annotation(cacheable)")
  public Object handleCacheable(ProceedingJoinPoint pjp, Cacheable cacheable) throws Throwable {
    Method method = resolveMethod(pjp);
    Intercept intercept = method.getAnnotation(Intercept.class);
    Fallback fallback = method.getAnnotation(Fallback.class);
    HotKeyCacheTTL ttl = method.getAnnotation(HotKeyCacheTTL.class);

    if (intercept == null && fallback == null && ttl == null) {
      return pjp.proceed();
    }

    String key = resolveKey(pjp, cacheable.key());
    String cacheName = cacheable.value()[0];
    String prefixedKey = cacheName + "::" + key;

    if (intercept != null && hotKey.isLocalHotKey(prefixedKey)) {
      return fallback != null ? resolveFallback(pjp, fallback)
        : hotKey.peek(prefixedKey).orElse(null);
    }

    if (ttl != null) HotKeyCacheContext.set(ttl.hardTtlMs(), ttl.softTtlMs());
    try {
      return pjp.proceed();
    } catch (Throwable e) {
      if (fallback != null) return resolveFallback(pjp, fallback);
      throw e;
    } finally {
      if (ttl != null) HotKeyCacheContext.clear();
    }
  }

  // ---------- SpEL utilities (same pattern as HotKeyAspect) ----------

  private String resolveKey(ProceedingJoinPoint pjp, String expression) {
    return getExpression(expression).getValue(buildEvaluationContext(pjp), String.class);
  }

  private Expression getExpression(String expr) {
    return expressionCache.computeIfAbsent(expr, parser::parseExpression);
  }

  private StandardEvaluationContext buildEvaluationContext(ProceedingJoinPoint pjp) {
    MethodSignature sig = (MethodSignature) pjp.getSignature();
    Method method = sig.getMethod();
    String[] names = paramNamesCache.computeIfAbsent(method, m -> {
      String[] n = parameterNameDiscoverer.getParameterNames(m);
      if (n == null) { n = new String[m.getParameterCount()];
        for (int i = 0; i < n.length; i++) n[i] = "arg" + i; }
      return n;
    });
    Object[] args = pjp.getArgs();
    StandardEvaluationContext ctx = new StandardEvaluationContext();
    for (int i = 0; i < args.length; i++) ctx.setVariable(names[i], args[i]);
    return ctx;
  }

  private Method resolveMethod(ProceedingJoinPoint pjp) {
    MethodSignature sig = (MethodSignature) pjp.getSignature();
    Method method = sig.getMethod();
    if (method.getDeclaringClass().isInterface()) {
      try { method = pjp.getTarget().getClass().getMethod(method.getName(), method.getParameterTypes()); }
      catch (NoSuchMethodException ignored) {}
    }
    return method;
  }

  private Object resolveFallback(ProceedingJoinPoint pjp, Fallback fb) throws Throwable {
    if (!fb.value().isEmpty()) return getExpression(fb.value()).getValue(buildEvaluationContext(pjp));
    return invokeFallbackMethod(pjp);
  }

  private Object invokeFallbackMethod(ProceedingJoinPoint pjp) throws Throwable {
    MethodSignature sig = (MethodSignature) pjp.getSignature();
    Method m = sig.getMethod();
    Object target = pjp.getTarget();
    Object[] args = pjp.getArgs();
    Method fb = fallbackMethodCache.computeIfAbsent(m, k ->
      findFallbackMethod(target.getClass(), k.getName() + "Fallback", k.getParameterTypes()));
    if (fb == null) return null;
    try { return fb.invoke(target, args); } catch (InvocationTargetException e) { throw e.getCause(); }
  }

  private Method findFallbackMethod(Class<?> clazz, String name, Class<?>... paramTypes) {
    try { return clazz.getMethod(name, paramTypes); }
    catch (NoSuchMethodException e) {
      Class<?> s = clazz.getSuperclass();
      if (s != null && s != Object.class) return findFallbackMethod(s, name, paramTypes);
      return null;
    }
  }
}
```

- [ ] **Step 2: Add aspect bean to auto-config**

In `HotKeySpringCacheAutoConfiguration`, add:

```java
  @Bean @ConditionalOnMissingBean
  public HotKeyCacheExtensionAspect hotKeyCacheExtensionAspect(HotKey hotKey) {
    return new HotKeyCacheExtensionAspect(hotKey);
  }
```

- [ ] **Step 3: Verify compilation and full test suite**

Run: `mvn test -pl common`
Expected: All tests pass

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat: add HotKeyCacheExtensionAspect for companion annotation support"
```

---

### Task 7: Deprecate @HotKey / HotKeyAspect

**Files:**
- Modify: `annotation/HotKey.java` — add `@Deprecated` to class and javadoc
- Modify: `annotation/HotKeyAspect.java` — add `@Deprecated` to class and javadoc
- Modify: `autoconfigure/HotKeyAnnotationAutoConfiguration.java` — add `@Deprecated` to class and javadoc

- [ ] **Step 1: Run full test suite to confirm nothing broke**
- [ ] **Step 2: Commit**

```bash
git add -A
git commit -m "deprecate: mark @HotKey, HotKeyAspect, AnnotationAutoConfiguration @Deprecated"
```

---

### Task 8: Doc sync

**Files:**
- Modify: `docs/CONFIG.md` + `docs/CONFIG.zh.md`
- Modify: `docs/ANNOTATION.md` + `docs/ANNOTATION.zh.md`
- Modify: `CONTEXT.md`

- [ ] **Step 1: CONFIG.md** — add table for `hotkey.spring-cache.*`
- [ ] **Step 2: ANNOTATION.md** — mark @HotKey deprecated, add @Cacheable guide
- [ ] **Step 3: CONTEXT.md** — add HotKeySpringCache, HotKeyCacheManager, HotKeyCacheExtensionAspect
- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "docs: sync Spring Cache integration config and annotation docs"
```
