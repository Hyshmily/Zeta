package io.github.hyshmily.hotkey.demo.controller;

import io.github.hyshmily.hotkey.HotKey;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/bench")
public class BenchController {

  @Autowired
  private HotKey hotKey;

  @Autowired
  private StringRedisTemplate redisTemplate;

  /** Peek L1 — pure Caffeine lookup, no side effects. */
  @GetMapping("/peek/{key}")
  public Optional<String> peek(@PathVariable String key) {
    return hotKey.peek(key);
  }

  /** Get with L1 hit (pre-warm before testing). Full path: TopK + Reporter. */
  @GetMapping("/get-hit/{key}")
  public Optional<String> getHit(@PathVariable String key) {
    return hotKey.get(key, () -> redisTemplate.opsForValue().get(key));
  }

  /** Get with L1 miss — loads from Redis. Full path: SingleFlight + TopK + Reporter. */
  @GetMapping("/get-miss/{key}")
  public Optional<String> getMiss(@PathVariable String key) {
    hotKey.invalidate(key);
    return hotKey.get(key, () -> redisTemplate.opsForValue().get(key));
  }

  /** Get with soft expire — stale-while-revalidate. */
  @GetMapping("/get-soft/{key}")
  public Optional<String> getSoft(@PathVariable String key) {
    return hotKey.getWithSoftExpire(key, () -> redisTemplate.opsForValue().get(key));
  }

  /** Pre-warm a key into L1 cache. */
  @PostMapping("/warm/{key}")
  public Map<String, Object> warm(@PathVariable String key) {
    String val = "bench-" + ThreadLocalRandom.current().nextLong();
    redisTemplate.opsForValue().set(key, val);
    hotKey.putThrough(key, val, () -> {});
    return Map.of("key", key, "warmed", true);
  }

  /** Pre-warm a batch of keys. */
  @PostMapping("/warm-batch/{count}")
  public Map<String, Object> warmBatch(@PathVariable int count) {
    for (int i = 0; i < count; i++) {
      String key = "bench:" + i;
      String val = "bench-" + i;
      redisTemplate.opsForValue().set(key, val);
      hotKey.putThrough(key, val, () -> {});
    }
    return Map.of("count", count, "warmed", true);
  }
}
