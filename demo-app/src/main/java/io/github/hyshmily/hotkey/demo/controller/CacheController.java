package io.github.hyshmily.hotkey.demo.controller;

import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.exception.HotKeyBlockedException;
import io.github.hyshmily.hotkey.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.hotkey.model.HotKeyCacheStats;
import io.github.hyshmily.hotkey.rule.Rule;
import io.github.hyshmily.hotkey.rule.Rule.RuleAction;
import io.github.hyshmily.hotkey.sync.distributedlock.AutoReleaseLock;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
public class CacheController {

  @Autowired
  private HotKey hotKey;

  @Autowired(required = false)
  private StringRedisTemplate redisTemplate;

  @GetMapping("/cache/{key}")
  public ResponseEntity<String> get(@PathVariable String key, @RequestParam(defaultValue = "get") String mode) {
    try {
      if ("peek".equals(mode)) {
        Optional<String> v = hotKey.peek(key);
        return v.map(ResponseEntity::ok).orElse(ResponseEntity.noContent().build());
      }
      if ("fresh".equals(mode)) {
        hotKey.invalidate(key);
      }
      if ("soft".equals(mode)) {
        Optional<String> val = hotKey.getWithSoftExpire(
          key,
          () -> {
            if (redisTemplate != null) return redisTemplate.opsForValue().get(key);
            return null;
          },
          3000
        );
        return val.map(ResponseEntity::ok).orElse(ResponseEntity.noContent().build());
      }
      Optional<String> val = hotKey.get(
        key,
        () -> {
          if (redisTemplate != null) {
            return redisTemplate.opsForValue().get(key);
          }
          return null;
        },
        300000,
        30000
      );
      return val.map(ResponseEntity::ok).orElse(ResponseEntity.noContent().build());
    } catch (HotKeyBlockedException e) {
      return ResponseEntity.status(403).body("BLOCKED: " + e.getCacheKey());
    }
  }

  @PutMapping("/cache/{key}")
  public ResponseEntity<Void> put(
    @PathVariable String key,
    @RequestBody String value,
    @RequestParam(defaultValue = "0") long softTtl
  ) {
    if (softTtl > 0) {
      hotKey.putThrough(
        key,
        value,
        () -> {
          if (redisTemplate != null) {
            redisTemplate.opsForValue().set(key, value);
          }
        },
        300000,
        softTtl
      );
    } else {
      hotKey.putThrough(key, value, () -> {
        if (redisTemplate != null) {
          redisTemplate.opsForValue().set(key, value);
        }
      });
    }
    return ResponseEntity.ok().build();
  }

  @PutMapping("/cache/local-only/{key}")
  public ResponseEntity<Void> putLocal(@PathVariable String key, @RequestBody String value) {
    hotKey.putLocal(key, value);
    if (redisTemplate != null) {
      redisTemplate.opsForValue().set(key, value);
    }
    return ResponseEntity.ok().build();
  }

  @DeleteMapping("/cache/{key}")
  public ResponseEntity<Void> invalidate(@PathVariable String key) {
    hotKey.invalidate(key);
    return ResponseEntity.ok().build();
  }

  @DeleteMapping("/cache")
  public ResponseEntity<Void> invalidateAll(@RequestParam String keys) {
    hotKey.invalidateAll(Arrays.asList(keys.split(",")));
    return ResponseEntity.ok().build();
  }

  @PostMapping("/cache/{key}/refresh")
  public ResponseEntity<Void> refresh(@PathVariable String key) {
    hotKey.refresh(key, () -> {
      if (redisTemplate != null) {
        return redisTemplate.opsForValue().get(key);
      }
      return null;
    });
    return ResponseEntity.ok().build();
  }

  @PostMapping("/lock/acquire")
  public ResponseEntity<Map<String, Object>> acquireLock(
    @RequestParam String key,
    @RequestParam(defaultValue = "5000") long timeoutMs
  ) {
    try (AutoReleaseLock lock = hotKey.tryLock("demo:" + key, timeoutMs, TimeUnit.MILLISECONDS)) {
      if (lock != null) {
        return ResponseEntity.ok(Map.of("acquired", true, "key", key));
      }
      return ResponseEntity.status(409).body(Map.of("acquired", false, "key", key));
    }
  }

  @DeleteMapping("/lock/release")
  public ResponseEntity<Void> releaseLock(@RequestParam String key) {
    AutoReleaseLock lock = hotKey.tryLock("demo:" + key, 1, TimeUnit.MILLISECONDS);
    if (lock != null) {
      lock.close();
    }
    return ResponseEntity.ok().build();
  }

  @GetMapping("/hotkeys/local")
  public List<Item> localHotKeys() {
    return hotKey.returnLocalHotKeys();
  }

  @GetMapping("/hotkeys/worker")
  public List<Item> workerHotKeys() {
    return hotKey.returnWorkerHotKeys();
  }

  @GetMapping("/rules")
  public List<Rule> rules() {
    return hotKey.getAllRules();
  }

  @PostMapping("/rules")
  public ResponseEntity<Void> addRule(@RequestParam String pattern, @RequestParam String action) {
    RuleAction ra = RuleAction.valueOf(action.toUpperCase());
    if (ra == RuleAction.BLOCK) {
      hotKey.addBlacklist(pattern);
    } else if (ra == RuleAction.ALLOW_NO_REPORT) {
      hotKey.addWhitelist(pattern);
    }
    return ResponseEntity.ok().build();
  }

  @DeleteMapping("/rules")
  public ResponseEntity<Void> removeRule(@RequestParam String pattern, @RequestParam String action) {
    RuleAction ra = RuleAction.valueOf(action.toUpperCase());
    if (ra == RuleAction.BLOCK) {
      hotKey.removeBlacklist(pattern);
    } else if (ra == RuleAction.ALLOW_NO_REPORT) {
      hotKey.removeWhitelist(pattern);
    }
    return ResponseEntity.ok().build();
  }

  @GetMapping("/stats")
  public HotKeyCacheStats stats() {
    return hotKey.stats();
  }

  @GetMapping("/health")
  public Map<String, Object> health() {
    return Map.of(
      "status",
      "UP",
      "mode",
      hotKey.isAppOnly() ? "APP_ONLY" : hotKey.isWorkerOnly() ? "WORKER_ONLY" : "COEXISTENCE",
      "redis",
      redisTemplate != null ? "CONNECTED" : "DISABLED"
    );
  }
}
