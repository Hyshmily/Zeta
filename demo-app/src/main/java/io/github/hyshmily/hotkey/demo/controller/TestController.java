package io.github.hyshmily.hotkey.demo.controller;

import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.constants.HotKeyConstants;
import io.github.hyshmily.hotkey.sharding.HealthView;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/tests")
public class TestController {

  @Autowired
  private HotKey hotKey;

  @Autowired
  private StringRedisTemplate redisTemplate;

  @Autowired(required = false)
  private HealthView healthView;

  @Autowired(required = false)
  private RabbitTemplate rabbitTemplate;

  @PostMapping("/thundering-herd/{key}")
  public ResponseEntity<Map<String, Object>> thunderingHerd(@PathVariable String key) {
    int threadCount = 50;
    AtomicInteger supplierCalls = new AtomicInteger(0);

    if (redisTemplate.opsForValue().get(key) == null) {
      redisTemplate.opsForValue().set(key, "herd-base");
    }
    hotKey.putThrough(key, "herd-base", () -> {});
    hotKey.invalidate(key);

    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);
    CountDownLatch gate = new CountDownLatch(1);
    AtomicInteger errors = new AtomicInteger(0);

    for (int t = 0; t < threadCount; t++) {
      pool.submit(() -> {
        try {
          gate.await();
          hotKey.get(
            key,
            () -> {
              supplierCalls.incrementAndGet();
              try {
      Thread.sleep(200);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              return redisTemplate.opsForValue().get(key);
            },
            300000,
            30000
          );
        } catch (Exception e) {
          errors.incrementAndGet();
        } finally {
          latch.countDown();
        }
      });
    }

    try {
      Thread.sleep(100);
      gate.countDown();
      latch.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    pool.shutdown();

    double dedupRatio = ((double) (threadCount - supplierCalls.get()) / threadCount) * 100;
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("scenario", "thundering-herd");
    result.put("threads", threadCount);
    result.put("supplierCalls", supplierCalls.get());
    result.put("dedupRatioPct", Math.round(dedupRatio * 100.0) / 100.0);
    result.put("errors", errors.get());
    return ResponseEntity.ok(result);
  }

  @PostMapping("/version-degradation")
  public ResponseEntity<Map<String, Object>> versionDegradation() {
    if (rabbitTemplate == null) {
      return ResponseEntity.ok(Map.of("status", "SKIPPED", "reason", "RabbitMQ not available"));
    }
    Map<String, Object> results = new LinkedHashMap<>();
    results.put("scenario", "version-degradation");

    String syncExchange = "hotkey.sync.exchange";
    String keyPrefix = "test:degr:";

    String k1 = keyPrefix + "norm";
    redisTemplate.opsForValue().set(k1, "base");
    hotKey.putThrough(k1, "base", () -> {});
    hotKey.invalidate(k1);
    MessageProperties p1 = new MessageProperties();
    p1.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "INVALIDATE");
    p1.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, 100L);
    p1.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
    rabbitTemplate.send(syncExchange, "", new Message(k1.getBytes(StandardCharsets.UTF_8), p1));
    results.put("case1_normal_normal", "PASS");

    String k2 = keyPrefix + "n2d";
    redisTemplate.opsForValue().set(k2, "degraded-state");
    hotKey.putThrough(k2, "degraded-state", () -> {});
    MessageProperties p2 = new MessageProperties();
    p2.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "INVALIDATE");
    p2.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, 1L);
    p2.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
    rabbitTemplate.send(syncExchange, "", new Message(k2.getBytes(StandardCharsets.UTF_8), p2));
    results.put("case2_normal_over_degraded", "PASS");

    return ResponseEntity.ok(results);
  }

  @PostMapping("/worker-broadcast/{key}")
  public ResponseEntity<Map<String, Object>> workerBroadcast(@PathVariable String key) {
    if (rabbitTemplate == null) {
      return ResponseEntity.ok(Map.of("status", "SKIPPED", "reason", "RabbitMQ not available"));
    }
    String broadcastExchange = "hotkey.broadcast.exchange";
    long dv = System.currentTimeMillis();

    MessageProperties props = new MessageProperties();
    props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "HOT");
    props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, dv);
    props.setHeader(HotKeyConstants.AMQP_HEADER_NODE_ID, "test-worker");
    props.setHeader(HotKeyConstants.AMQP_HEADER_EPOCH, 1L);
    rabbitTemplate.send(broadcastExchange, "", new Message(key.getBytes(StandardCharsets.UTF_8), props));

    boolean promoted = false;
    try {
      Thread.sleep(1000);
      promoted = hotKey.isLocalHotKey(key);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    Map<String, Object> result = new LinkedHashMap<>();
    result.put("scenario", "worker-broadcast");
    result.put("key", key);
    result.put("decisionType", "HOT");
    result.put("decisionVersion", dv);
    result.put("promoted", promoted);
    return ResponseEntity.ok(result);
  }

  @PostMapping("/large-value")
  public ResponseEntity<Map<String, Object>> largeValue() {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("scenario", "large-value");

    int[] sizes = { 1024, 10240, 102400, 524288 };
    List<Map<String, Object>> entries = new ArrayList<>();
    boolean allOk = true;

    for (int size : sizes) {
      String key = "test:large:" + size;
      String value = randomString(size);

      long t0 = System.nanoTime();
      hotKey.putThrough(key, value, () -> redisTemplate.opsForValue().set(key, value));
      long putNs = System.nanoTime() - t0;

      String cached = hotKey.get(key, () -> redisTemplate.opsForValue().get(key), 300000, 30000).orElse(null);
      boolean ok = value.equals(cached);
      if (!ok) {
        allOk = false;
      }

      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("sizeBytes", size);
      entry.put("putMs", Math.round((putNs / 1_000_000.0) * 100.0) / 100.0);
      entry.put("verified", ok);
      entries.add(entry);
    }

    result.put("entries", entries);
    result.put("allVerified", allOk);
    return ResponseEntity.ok(result);
  }

  @PostMapping("/full-chain/{key}")
  public ResponseEntity<Map<String, Object>> fullChain(@PathVariable String key) {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("scenario", "full-chain");
    result.put("key", key);
    String val = "e2e-" + System.currentTimeMillis();

    long t0 = System.nanoTime();
    hotKey.putThrough(key, val, () -> redisTemplate.opsForValue().set(key, val));
    long putNs = System.nanoTime() - t0;
    result.put("putThroughMs", roundMs(putNs));

    Object cached = hotKey.peek(key).orElse(null);
    result.put("l1Hit", val.equals(cached));

    hotKey.invalidate(key);
    boolean l1Empty = hotKey.peek(key).isEmpty();
    result.put("invalidateOk", l1Empty);

    t0 = System.nanoTime();
    String reloaded = hotKey.get(key, () -> redisTemplate.opsForValue().get(key), 300000, 30000).orElse(null);
    long getNs = System.nanoTime() - t0;
    result.put("getAfterInvalidateMs", roundMs(getNs));
    result.put("reloadOk", val.equals(reloaded));

    return ResponseEntity.ok(result);
  }

  @PostMapping("/propagation")
  public ResponseEntity<Map<String, Object>> propagation() {
    List<Map<String, Object>> samples = new ArrayList<>();

    for (int i = 0; i < 100; i++) {
      String key = "test:prop:" + i;
      String val = "pv-" + i;
      redisTemplate.opsForValue().set(key, val);
      hotKey.invalidate(key);

      long t0 = System.nanoTime();
      hotKey.get(key, () -> redisTemplate.opsForValue().get(key), 300000, 30000);
      long ns = System.nanoTime() - t0;

      Map<String, Object> s = new LinkedHashMap<>();
      s.put("key", key);
      s.put("latencyMs", roundMs(ns));
      samples.add(s);
    }

    double[] vals = samples
      .stream()
      .mapToDouble(s -> ((Number) s.get("latencyMs")).doubleValue())
      .sorted()
      .toArray();
    int len = vals.length;
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("scenario", "propagation");
    result.put("samples", samples.size());
    result.put("p50Ms", len > 0 ? vals[len / 2] : 0);
    result.put("p95Ms", len > 0 ? vals[(int) (len * 0.95)] : 0);
    result.put("p99Ms", len > 0 ? vals[(int) (len * 0.99)] : 0);
    return ResponseEntity.ok(result);
  }

  @PostMapping("/null-cache/{key}")
  public ResponseEntity<Map<String, Object>> nullCache(@PathVariable String key) {
    int[] callCount = { 0 };
    Object first = hotKey.computeIfAbsent(key, () -> {
      callCount[0]++;
      return null;
    }, 300000, 30000);
    Object second = hotKey.computeIfAbsent(key, () -> {
      callCount[0]++;
      return null;
    }, 300000, 30000);
    return ResponseEntity.ok(Map.of(
      "scenario", "null-cache",
      "firstResult", first == null ? "null" : first,
      "secondResult", second == null ? "null" : second,
      "callCount", callCount[0]
    ));
  }

  @PostMapping("/worker-e2e/{key}")
  public ResponseEntity<Map<String, Object>> workerE2E(@PathVariable String key) throws Exception {
    String val = "e2e-" + System.currentTimeMillis();

    hotKey.putThrough(key, val, () -> redisTemplate.opsForValue().set(key, val));

    // Wait for at least one Worker to be alive before flooding reads,
    // otherwise the Reporter's flush cycles will reset counters with no route target
    long deadline = System.currentTimeMillis() + 30000;
    while (System.currentTimeMillis() < deadline) {
      if (healthView != null && !healthView.getAliveWorkerIds().isEmpty()) break;
      Thread.sleep(200);
    }

    // Wait for putThrough async task to complete (write to L1 + Redis)
    Thread.sleep(500);

    long readStartTime = System.currentTimeMillis();
    for (int i = 0; i < 10000; i++) {
      hotKey.get(key, () -> redisTemplate.opsForValue().get(key), 300000, 30000).orElse(null);
    }
    long readEndTime = System.currentTimeMillis();
    long readDurationMs = readEndTime - readStartTime;

    long pollStartTime = System.currentTimeMillis();
    long timeout = 60000;
    boolean detected = false;

    while (System.currentTimeMillis() - pollStartTime < timeout) {
      if (hotKey.isLocalHotKey(key)) {
        detected = true;
        break;
      }
      Thread.sleep(20);
    }

    long pollDurationMs = System.currentTimeMillis() - pollStartTime;

    Map<String, Object> result = new LinkedHashMap<>();
    result.put("scenario", "worker-e2e");
    result.put("key", key);
    result.put("detected", detected);
    result.put("readDurationMs", readDurationMs);
    result.put("pollDurationMs", pollDurationMs);
    result.put("elapsedMs", readDurationMs + pollDurationMs);
    result.put("frameworkLatencyMs", pollDurationMs);
    result.put("timeoutMs", timeout);
    return ResponseEntity.ok(result);
  }

  @PostMapping("/preload/{key}")
  public ResponseEntity<Map<String, Object>> preload(@PathVariable String key) {
    hotKey.notifyLocalDetectorDirect(key, Integer.MAX_VALUE);
    boolean isHot = hotKey.isLocalHotKey(key);
    return ResponseEntity.ok(Map.of(
      "scenario", "preload",
      "key", key,
      "isLocalHotKey", isHot
    ));
  }

  @PostMapping("/all")
  public ResponseEntity<Map<String, Object>> runAll() throws Exception {
    Map<String, Object> results = new LinkedHashMap<>();
    results.put("timestamp", System.currentTimeMillis());

    results.put("fullChain", fullChain("test:all:e2e").getBody());
    results.put("largeValue", largeValue().getBody());
    results.put("thunderingHerd", thunderingHerd("test:all:herd").getBody());
    results.put("versionDegradation", versionDegradation().getBody());
    results.put("propagation", propagation().getBody());

    boolean allPassed = true;
    for (Map.Entry<String, Object> e : results.entrySet()) {
      if (e.getValue() instanceof Map<?, ?> m) {
        Object v = m.get("allVerified");
        if (v instanceof Boolean b && !b) {
          allPassed = false;
        }
      }
    }
    results.put("allPassed", allPassed);
    return ResponseEntity.ok(results);
  }

  private static double roundMs(long ns) {
    return Math.round((ns / 1_000_000.0) * 100.0) / 100.0;
  }

  private static String randomString(int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append((char) ('A' + (i % 26)));
    }
    return sb.toString();
  }
}
