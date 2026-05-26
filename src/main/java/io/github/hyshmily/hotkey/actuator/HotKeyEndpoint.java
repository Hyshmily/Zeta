package io.github.hyshmily.hotkey.actuator;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyProperties;
import io.github.hyshmily.hotkey.algorithm.Item;
import io.github.hyshmily.hotkey.algorithm.TopK;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;

@Endpoint(id = "hotkey")
public class HotKeyEndpoint {

  private final TopK hotKeyDetector;
  private final Cache<String, Object> caffeineCache;
  private final Cache<String, CompletableFuture<Object>> inflightLoads;
  private final int l1MaxSize;

  public HotKeyEndpoint(
      TopK hotKeyDetector,
      Cache<String, Object> caffeineCache,
      Cache<String, CompletableFuture<Object>> inflightLoads,
      HotKeyProperties properties) {
    this.hotKeyDetector = hotKeyDetector;
    this.caffeineCache = caffeineCache;
    this.inflightLoads = inflightLoads;
    this.l1MaxSize = properties.getLocalCacheMaxSize();
  }

  @ReadOperation
  public Map<String, Object> hotKeyInfo() {
    Map<String, Object> info = new LinkedHashMap<>();

    List<Item> topKList = hotKeyDetector.list();
    List<Map<String, Object>> topKKeys = new ArrayList<>(topKList.size());
    for (Item item : topKList) {
      topKKeys.add(Map.of("key", item.key(), "count", item.count()));
    }
    info.put("topK", topKKeys);
    info.put("topKCount", topKList.size());

    info.put("totalRequests", hotKeyDetector.total());

    info.put("l1CacheSize", caffeineCache.estimatedSize());
    info.put("l1MaxSize", l1MaxSize);

    info.put("inflightSize", inflightLoads.estimatedSize());

    BlockingQueue<Item> expelledQueue = hotKeyDetector.expelled();
    List<Item> expelledItems = new ArrayList<>();
    expelledQueue.drainTo(expelledItems, 10);
    info.put("recentlyExpelled", expelledItems.stream().map(Item::key).toList());

    return info;
  }
}
