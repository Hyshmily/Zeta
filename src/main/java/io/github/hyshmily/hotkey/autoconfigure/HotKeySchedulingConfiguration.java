package io.github.hyshmily.hotkey.autoconfigure;

import io.github.hyshmily.hotkey.algorithm.Item;
import io.github.hyshmily.hotkey.algorithm.TopK;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@Slf4j
@AutoConfiguration(after = HotKeyAutoConfiguration.class)
@ConditionalOnProperty(name = "hotkey.scheduling.enabled", havingValue = "true", matchIfMissing = true)
@EnableScheduling
public class HotKeySchedulingConfiguration {

  private final TopK hotKeyDetector;

  public HotKeySchedulingConfiguration(TopK hotKeyDetector) {
    this.hotKeyDetector = hotKeyDetector;
  }

  @Scheduled(fixedDelayString = "${hotkey.decay-period:20}", timeUnit = java.util.concurrent.TimeUnit.SECONDS)
  public void cleanHotKeys() {
    hotKeyDetector.fading();
    log.debug("HeavyKeeper count has decayed");
  }

  @Scheduled(fixedDelay = 60_000)
  public void drainExpelled() {
    List<Item> items = new ArrayList<>();
    hotKeyDetector.expelled().drainTo(items, 1000);
    if (!items.isEmpty()) {
      String keys = items.stream().map(Item::key).limit(20).collect(Collectors.joining(","));
      log.info(
        "Drained {} expelled hot keys: {}{}",
        items.size(),
        keys,
        items.size() > 20 ? "..." : ""
      );
    }
  }
}
