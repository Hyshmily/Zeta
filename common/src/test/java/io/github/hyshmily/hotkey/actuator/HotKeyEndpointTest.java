package io.github.hyshmily.hotkey.actuator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.algorithm.Item;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyProperties;
import io.github.hyshmily.hotkey.hotkeycache.SingleFlight;
import io.github.hyshmily.hotkey.report.HotKeyReporter;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link HotKeyEndpoint} verifying monitoring endpoint structure and null-safety.
 */
class HotKeyEndpointTest {

  private TopK hotKeyDetector;
  private TopK workerTopK;
  private Cache<String, Object> caffeineCache;
  private SingleFlight singleFlight;
  private HotKeyProperties properties;
  private HotKeyReporter hotKeyReporter;
  private HotKeyEndpoint endpoint;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    hotKeyDetector = mock(TopK.class);
    workerTopK = mock(TopK.class);
    caffeineCache = mock(Cache.class);
    singleFlight = mock(SingleFlight.class);
    properties = new HotKeyProperties();
    hotKeyReporter = mock(HotKeyReporter.class);
    endpoint = new HotKeyEndpoint(hotKeyDetector, workerTopK, caffeineCache, singleFlight, properties, hotKeyReporter, new RuleMatcher(Optional.empty(), Optional.empty()));
  }

  @Test
  void hotKeyInfo_shouldIncludeAllSections() {
    when(hotKeyDetector.list()).thenReturn(List.of(new Item("k1", 10)));
    when(hotKeyDetector.total()).thenReturn(100L);
    when(hotKeyDetector.expelled()).thenReturn(new LinkedBlockingQueue<>());
    when(workerTopK.list()).thenReturn(List.of(new Item("wk1", 5)));
    when(workerTopK.total()).thenReturn(50L);
    when(workerTopK.expelled()).thenReturn(new LinkedBlockingQueue<>());
    when(caffeineCache.estimatedSize()).thenReturn(42L);
    when(singleFlight.estimatedInflightSize()).thenReturn(3L);
    when(hotKeyReporter.dispatcherDepth()).thenReturn(5);

    Map<String, Object> info = endpoint.hotKeyInfo();
    assertThat(info).containsKey("topK");
    assertThat(info).containsKey("topKCount");
    assertThat(info).containsKey("totalRequests");
    assertThat(info).containsKey("recentlyExpelled");
    assertThat(info).containsKey("workerTopK");
    assertThat(info).containsKey("workerTopKCount");
    assertThat(info).containsKey("workerTotalRequests");
    assertThat(info).containsKey("l1CacheSize");
    assertThat(info).containsKey("l1MaxSize");
    assertThat(info).containsKey("inflightSize");
    assertThat(info).containsKey("reportQueueDepth");
  }

  @Test
  void hotKeyInfo_shouldHandleNullComponents() {
    HotKeyEndpoint minimal = new HotKeyEndpoint(null, null, null, null, properties, null, null);
    Map<String, Object> info = minimal.hotKeyInfo();
    assertThat(info).doesNotContainKey("topK");
    assertThat(info).doesNotContainKey("workerTopK");
    assertThat(info).doesNotContainKey("l1CacheSize");
    assertThat(info).doesNotContainKey("inflightSize");
    assertThat(info).doesNotContainKey("reportQueueDepth");
  }
}
