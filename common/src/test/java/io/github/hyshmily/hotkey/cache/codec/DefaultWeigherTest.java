package io.github.hyshmily.hotkey.cache.codec;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.hotkey.cache.annotationsupporter.NullValue;
import io.github.hyshmily.hotkey.model.CacheEntry;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DefaultWeigherTest {

  @Test
  void weigh_keyAndCacheEntryWithString_shouldReturnPositiveWeight() {
    CacheEntry entry = CacheEntry.builder().value("hello").dataVersion(1).build();
    int weight = DefaultWeigher.INSTANCE.weigh("myKey", entry);
    assertThat(weight).isPositive();
  }

  @Test
  void weigh_keyAndNullValue_shouldReturnPositiveWeight() {
    int weight = DefaultWeigher.INSTANCE.weigh("myKey", NullValue.INSTANCE);
    assertThat(weight).isPositive();
  }

  @Test
  void weigh_keyAndString_shouldReturnPositiveWeight() {
    int weight = DefaultWeigher.INSTANCE.weigh("myKey", "a string value");
    assertThat(weight).isPositive();
  }

  @Test
  void weigh_keyAndByteArray_shouldReturnPositiveWeight() {
    int weight = DefaultWeigher.INSTANCE.weigh("myKey", new byte[] { 1, 2, 3 });
    assertThat(weight).isPositive();
  }

  @Test
  void weigh_keyAndCollection_shouldReturnPositiveWeight() {
    int weight = DefaultWeigher.INSTANCE.weigh("myKey", List.of("a", "b", "c"));
    assertThat(weight).isPositive();
  }

  @Test
  void weigh_keyAndMap_shouldReturnPositiveWeight() {
    int weight = DefaultWeigher.INSTANCE.weigh("myKey", Map.of("k1", "v1", "k2", "v2"));
    assertThat(weight).isPositive();
  }

  @Test
  void weigh_keyAndObjectArray_shouldReturnPositiveWeight() {
    int weight = DefaultWeigher.INSTANCE.weigh("myKey", new Object[] { "a", "b", "c" });
    assertThat(weight).isPositive();
  }

  @Test
  void weigh_keyAndOtherObject_shouldReturnPositiveWeight() {
    int weight = DefaultWeigher.INSTANCE.weigh("myKey", 42);
    assertThat(weight).isPositive();
  }

  @Test
  void weigh_cacheEntryContainingNullValue_shouldReturnPositiveWeight() {
    CacheEntry entry = CacheEntry.builder().value(NullValue.INSTANCE).dataVersion(1).build();
    int weight = DefaultWeigher.INSTANCE.weigh("myKey", entry);
    assertThat(weight).isPositive();
  }

  @Test
  void weigh_cacheEntryContainingCacheEntry_shouldReturnPositiveWeight() {
    CacheEntry inner = CacheEntry.builder().value("inner").dataVersion(1).build();
    CacheEntry outer = CacheEntry.builder().value(inner).dataVersion(2).build();
    int weight = DefaultWeigher.INSTANCE.weigh("myKey", outer);
    assertThat(weight).isPositive();
  }

  @Test
  void weigh_emptyCollection_shouldReturnPositiveWeight() {
    int weight = DefaultWeigher.INSTANCE.weigh("myKey", List.of());
    assertThat(weight).isPositive();
  }

  @Test
  void weigh_emptyMap_shouldReturnPositiveWeight() {
    int weight = DefaultWeigher.INSTANCE.weigh("myKey", Map.of());
    assertThat(weight).isPositive();
  }

  @Test
  void weigh_emptyByteArray_shouldReturnPositiveWeight() {
    int weight = DefaultWeigher.INSTANCE.weigh("myKey", new byte[0]);
    assertThat(weight).isPositive();
  }
}
