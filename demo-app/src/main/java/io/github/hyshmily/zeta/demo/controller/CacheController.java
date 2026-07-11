package io.github.hyshmily.zeta.demo.controller;

import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.exception.ZetaBlockedException;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.zeta.model.ZetaCacheStats;
import io.github.hyshmily.zeta.rule.Rule;
import io.github.hyshmily.zeta.rule.Rule.RuleAction;
import io.github.hyshmily.zeta.sync.distributedlock.AutoReleaseLock;
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
  private Zeta zeta;

  @Autowired(required = false)
  private StringRedisTemplate redisTemplate;

  @GetMapping("/cache/{key}")
  public ResponseEntity<String> get(@PathVariable String key, @RequestParam(defaultValue = "get") String mode) {
    try {
      if ("peek".equals(mode)) {
        Optional<String> v = zeta.peek(key);
        return v.map(ResponseEntity::ok).orElse(ResponseEntity.noContent().build());
      }
      if ("fresh".equals(mode)) {
        zeta.invalidate(key);
      }
      if ("soft".equals(mode)) {
        Optional<String> val = zeta.getWithSoftExpire(
          key,
          () -> {
            if (redisTemplate != null) return redisTemplate.opsForValue().get(key);
            return null;
          },
          3000
        );
        return val.map(ResponseEntity::ok).orElse(ResponseEntity.noContent().build());
      }
      Optional<String> val = zeta.get(
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
    } catch (ZetaBlockedException e) {
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
      zeta.putThrough(
        key,
        value,
        () -> {
          if (redisTemplate != null) {
            redisTemplate.opsForValue().set(key, value);
          }
        },
        300000,
        softTtl,
        true
      );
    } else {
      zeta.putThrough(key, value, () -> {
        if (redisTemplate != null) {
          redisTemplate.opsForValue().set(key, value);
        }
      });
    }
    return ResponseEntity.ok().build();
  }

  @PutMapping("/cache/local-only/{key}")
  public ResponseEntity<Void> putLocal(@PathVariable String key, @RequestBody String value) {
    zeta.putLocal(key, value);
    if (redisTemplate != null) {
      redisTemplate.opsForValue().set(key, value);
    }
    return ResponseEntity.ok().build();
  }

  @DeleteMapping("/cache/{key}")
  public ResponseEntity<Void> invalidate(@PathVariable String key) {
    zeta.invalidate(key);
    return ResponseEntity.ok().build();
  }

  @DeleteMapping("/cache")
  public ResponseEntity<Void> invalidateAll(@RequestParam String keys) {
    zeta.invalidate(Arrays.asList(keys.split(",")).toString());
    return ResponseEntity.ok().build();
  }

  @PostMapping("/cache/{key}/refresh")
  public ResponseEntity<Void> refresh(@PathVariable String key) {
    zeta.refresh(key, () -> {
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
    try (AutoReleaseLock lock = zeta.tryLock("demo:" + key, timeoutMs, TimeUnit.MILLISECONDS)) {
      if (lock != null) {
        return ResponseEntity.ok(Map.of("acquired", true, "key", key));
      }
      return ResponseEntity.status(409).body(Map.of("acquired", false, "key", key));
    }
  }

  @DeleteMapping("/lock/release")
  public ResponseEntity<Void> releaseLock(@RequestParam String key) {
    AutoReleaseLock lock = zeta.tryLock("demo:" + key, 1, TimeUnit.MILLISECONDS);
    if (lock != null) {
      lock.close();
    }
    return ResponseEntity.ok().build();
  }

  @GetMapping("/hotkeys/local")
  public List<Item> localHotKeys() {
    return zeta.returnLocalHotKeys();
  }

  @GetMapping("/hotkeys/worker")
  public List<Item> workerHotKeys() {
    return zeta.returnWorkerHotKeys();
  }

  @GetMapping("/rules")
  public List<Rule> rules() {
    return zeta.getAllRules();
  }

  @PostMapping("/rules")
  public ResponseEntity<Void> addRule(@RequestParam String pattern, @RequestParam String action) {
    RuleAction ra = RuleAction.valueOf(action.toUpperCase());
    if (ra == RuleAction.BLOCK) {
      zeta.addBlacklist(pattern);
    } else if (ra == RuleAction.ALLOW_NO_REPORT) {
      zeta.addWhitelist(pattern);
    }
    return ResponseEntity.ok().build();
  }

  @DeleteMapping("/rules")
  public ResponseEntity<Void> removeRule(@RequestParam String pattern, @RequestParam String action) {
    RuleAction ra = RuleAction.valueOf(action.toUpperCase());
    if (ra == RuleAction.BLOCK) {
      zeta.removeBlacklist(pattern);
    } else if (ra == RuleAction.ALLOW_NO_REPORT) {
      zeta.removeWhitelist(pattern);
    }
    return ResponseEntity.ok().build();
  }

  @GetMapping("/stats")
  public ZetaCacheStats stats() {
    return zeta.stats();
  }

  @GetMapping("/health")
  public Map<String, Object> health() {
    return Map.of(
      "status",
      "UP",
      "mode",
      zeta.isAppOnly() ? "APP_ONLY" : zeta.isWorkerOnly() ? "WORKER_ONLY" : "COEXISTENCE",
      "redis",
      redisTemplate != null ? "CONNECTED" : "DISABLED"
    );
  }
}
