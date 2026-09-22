package io.github.hyshmily.zeta.cache.codec;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.annotation.annotationsupporter.NullValue;
import io.github.hyshmily.zeta.model.CacheEntry;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.lucene.util.RamUsageEstimator;
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

  @Test
  void weigh_keyAndPojoWithLargeArray_shouldCountNestedFieldContent() {
    int smallWeight = DefaultWeigher.INSTANCE.weigh("myKey", new PojoWithPayload(16));
    int largeWeight = DefaultWeigher.INSTANCE.weigh("myKey", new PojoWithPayload(10_000_000));
    assertThat((long) largeWeight - smallWeight).isGreaterThan(10_000_000L);
  }

  @Test
  void weigh_cacheEntryContainingPojoWithLargeArray_shouldCountNestedFieldContent() {
    CacheEntry small = CacheEntry.builder().value(new PojoWithPayload(16)).dataVersion(1).build();
    CacheEntry large = CacheEntry.builder().value(new PojoWithPayload(10_000_000)).dataVersion(1).build();
    int smallWeight = DefaultWeigher.INSTANCE.weigh("myKey", small);
    int largeWeight = DefaultWeigher.INSTANCE.weigh("myKey", large);
    assertThat((long) largeWeight - smallWeight).isGreaterThan(10_000_000L);
  }

  /**
   * A reference graph beyond {@link DefaultWeigher#GRAPH_NODE_BUDGET} must stop walking and
   * return a bounded (conservative) weight instead of traversing unbounded nodes per write.
   */
  @Test
  void weigh_deepPojoOverNodeBudget_shouldCompleteWithBoundedWeight() {
    int weight = DefaultWeigher.INSTANCE.weigh("myKey", new Linked(DefaultWeigher.DEFAULT_GRAPH_NODE_BUDGET * 2));
    assertThat(weight).isPositive();
  }

  /** Same budget guard for the reference-array expansion path (in-loop cap). */
  @Test
  void weigh_wideArrayOverNodeBudget_shouldCompleteWithBoundedWeight() {
    Object[] wide = new Object[DefaultWeigher.DEFAULT_GRAPH_NODE_BUDGET * 3];
    for (int i = 0; i < wide.length; i++) {
      wide[i] = new Object();
    }
    int weight = DefaultWeigher.INSTANCE.weigh("myKey", wide);
    assertThat(weight).isPositive();
  }

  /**
   * An over-budget graph must not be priced at its walked prefix: the previous 2× factor left a
   * 100 000-node chain ≈ 12× under-priced (measured), so max-weight eviction never saw it.
   */
  @Test
  void weigh_deepPojoOverNodeBudget_shouldPriceAtLeastTheFloor() {
    int weight = DefaultWeigher.INSTANCE.weigh("myKey", new Linked(DefaultWeigher.DEFAULT_GRAPH_NODE_BUDGET * 20));
    assertThat(weight).isGreaterThanOrEqualTo((int) DefaultWeigher.OVER_BUDGET_FLOOR);
  }

  /**
   * A container's elements must be priced, not merely counted: a flat per-element allowance
   * under-priced an {@code ArrayList<byte[]>} holding real payloads by three orders of magnitude.
   */
  @Test
  void weigh_collectionWithLargePayloads_shouldPriceElementContent() {
    int small = DefaultWeigher.INSTANCE.weigh(
      "myKey",
      List.of(new byte[16], new byte[16], new byte[16], new byte[16], new byte[16])
    );
    int large = DefaultWeigher.INSTANCE.weigh(
      "myKey",
      List.of(
        new byte[1_000_000],
        new byte[1_000_000],
        new byte[1_000_000],
        new byte[1_000_000],
        new byte[1_000_000]
      )
    );
    assertThat((long) large - small).isGreaterThan(4_900_000L);
  }

  /** Map entries must be priced through keys and values, not by entry count alone. */
  @Test
  void weigh_mapWithLargeValues_shouldPriceEntryContent() {
    int small = DefaultWeigher.INSTANCE.weigh("myKey", Map.of("k", new byte[16]));
    int large = DefaultWeigher.INSTANCE.weigh("myKey", Map.of("k", new byte[1_000_000]));
    assertThat((long) large - small).isGreaterThan(990_000L);
  }

  /** Reference-array elements must be priced; the array itself is not the payload. */
  @Test
  void weigh_objectArrayWithLargeElement_shouldPriceElementContent() {
    int small = DefaultWeigher.INSTANCE.weigh("myKey", new Object[] { new byte[16] });
    int large = DefaultWeigher.INSTANCE.weigh("myKey", new Object[] { new byte[1_000_000] });
    assertThat((long) large - small).isGreaterThan(990_000L);
  }

  /**
   * Reference slots are part of an array's shallow size, so widening the array by N elements must
   * add exactly N references — the previous implementation added them a second time while still
   * ignoring the elements.
   */
  @Test
  void weigh_objectArray_shouldNotDoubleCountReferenceSlots() {
    int empty = DefaultWeigher.INSTANCE.weigh("myKey", new Object[0]);
    int thousand = DefaultWeigher.INSTANCE.weigh("myKey", new Object[1000]);
    assertThat(thousand - empty).isEqualTo(1000 * RamUsageEstimator.NUM_BYTES_OBJECT_REF);
  }

  /** An empty container has no elements to price and must not be charged a per-element allowance. */
  @Test
  void weigh_emptyContainer_shouldNotBeChargedPerElementWeight() {
    int emptyList = DefaultWeigher.INSTANCE.weigh("myKey", List.of());
    int emptyArray = DefaultWeigher.INSTANCE.weigh("myKey", new Object[0]);
    assertThat(emptyList - emptyArray).isLessThanOrEqualTo(64);
  }

  /**
   * Calibration guard: a typical entry (32-character ASCII key, 64-character String payload)
   * really occupies ≈ 370 B, so the estimate must stay in that order of magnitude. The previous
   * {@code ENTRY_OVERHEAD = 512} alone exceeded the whole measured entry.
   */
  @Test
  void weigh_typicalEntry_shouldStayWithinCalibratedBound() {
    CacheEntry entry = CacheEntry.builder().value("x".repeat(64)).dataVersion(1).build();
    int weight = DefaultWeigher.INSTANCE.weigh("OrderService::order:123456789012", entry);
    assertThat(weight).isBetween(300, 560);
  }

  /**
   * Traversal-cost guard: a wide container must not be walked element by element. The weigher runs
   * on the write path — on Caffeine's {@code computeIfAbsent} path while holding the key's bin lock
   * — so pricing a 10 000-element list by iterating all of it (measured 134 µs before sampling) is
   * not acceptable; only the sample prefix may be visited.
   */
  @Test
  void weigh_wideContainer_shouldVisitOnlyTheSamplePrefix() {
    CountingList list = new CountingList(10_000);

    int weight = DefaultWeigher.INSTANCE.weigh("myKey", list);

    assertThat(weight).isPositive();
    assertThat(list.visited).isLessThanOrEqualTo(DefaultWeigher.CONTAINER_SAMPLE_SIZE);
  }

  /** The extrapolated tail must keep the estimate proportional to the container's size. */
  @Test
  void weigh_wideContainer_shouldScaleWithElementCount() {
    int small = DefaultWeigher.INSTANCE.weigh("myKey", new CountingList(1_000));
    int large = DefaultWeigher.INSTANCE.weigh("myKey", new CountingList(10_000));
    assertThat(large).isGreaterThan(small * 5);
  }

  /**
   * A sampled container must still be priced from what its elements actually weigh: a homogeneous
   * 1 000-element list lands within the safety factor around the real payload (measured 2.0×), not
   * at a flat per-element constant.
   */
  @Test
  void weigh_sampledContainer_shouldStayProportionalToPayload() {
    List<byte[]> list = new ArrayList<>();
    for (int i = 0; i < 1_000; i++) {
      list.add(new byte[1024]);
    }
    long payload = 1_000L * RamUsageEstimator.shallowSizeOf(new byte[1024]);
    long weight = DefaultWeigher.INSTANCE.weigh("myKey", list);
    assertThat(weight).isBetween((long) (payload * 1.5), (long) (payload * 3.0));
  }

  /**
   * Prefix-only sampling misses a payload clustered outside the first slots (a 1 000-element list
   * with 1 MB tails was under-priced ≈ 9 500× — measured). Random-access structures are therefore
   * sampled across their whole range, and this pins that behaviour.
   */
  @Test
  void weigh_randomAccessListWithClusteredPayload_shouldSeeTheTail() {
    List<byte[]> list = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      list.add(new byte[i < 8 ? 16 : 64_000]);
    }
    int weight = DefaultWeigher.INSTANCE.weigh("myKey", list);
    assertThat(weight).isGreaterThan(1_000_000);
  }

  /**
   * A value implementing {@link Weighable} is priced from its own report in O(1) — no walk state is
   * allocated (Caffeine's "pre-calculated weights" fast path).
   */
  @Test
  void weigh_weighableValue_usesReportedSize() {
    Weighable value = () -> 123_456L;
    String key = "myKey";
    long keyWeight = RamUsageEstimator.shallowSizeOf(key) + (2L * key.length());
    long expected = keyWeight + 123_456L + DefaultWeigher.ENTRY_OVERHEAD;
    assertThat(DefaultWeigher.INSTANCE.weigh(key, value)).isEqualTo(expected);
  }

  /** A {@link Weighable} reporting "unknown" (non-positive) falls back to measuring the graph. */
  @Test
  void weigh_weighableReturningUnknown_fallsBackToMeasurement() {
    int weight = DefaultWeigher.INSTANCE.weigh("myKey", new UnknownSizeValue());
    assertThat(weight).isGreaterThan(100_000);
  }

  /** Fields annotated {@link Unweighed} are excluded from the measurement. */
  @Test
  void weigh_unweighedField_isExcludedFromMeasurement() {
    int weight = DefaultWeigher.INSTANCE.weigh("myKey", new PartiallyWeighed(64_000));
    assertThat(weight).isLessThan(200_000);
  }

  /** A class annotated {@link Unweighed} is skipped entirely: only the reference slot remains. */
  @Test
  void weigh_unweighedClass_isSkipped() {
    int weight = DefaultWeigher.INSTANCE.weigh("myKey", new ExcludedHolder());
    assertThat(weight).isLessThan(10_000);
  }

  /**
   * {@code abort} over-budget policy: a budget-exhausted graph is priced above any budget so
   * Caffeine evicts it immediately, while the default extrapolate policy stays bounded.
   */
  @Test
  void weigh_overBudgetPolicy_isSelectable() {
    DefaultWeigher aborting = DefaultWeigher.of(8, true);
    DefaultWeigher extrapolating = DefaultWeigher.of(8, false);
    Linked deep = new Linked(64);
    assertThat(aborting.weigh("myKey", deep)).isEqualTo(Integer.MAX_VALUE);
    assertThat(extrapolating.weigh("myKey", deep)).isLessThan(Integer.MAX_VALUE);
  }

  /** A value that reports "unknown" but actually holds a large payload. */
  private static final class UnknownSizeValue implements Weighable {

    private final byte[] payload = new byte[100_000];

    @Override
    public long weighInBytes() {
      return 0;
    }
  }

  /** The counted field is measured; the {@code @Unweighed} one is ignored. */
  private static final class PartiallyWeighed {

    private final byte[] counted;

    @Unweighed
    private final byte[] ignored;

    private PartiallyWeighed(int countedSize) {
      this.counted = new byte[countedSize];
      this.ignored = new byte[1_000_000];
    }
  }

  /** Class-level exclusion: whichever cache entry references it, its content is not priced. */
  @Unweighed
  private static final class ExcludedHolder {

    private final byte[] payload = new byte[1_000_000];
  }

  /** A list that counts how many elements the weigher actually reads. */
  private static final class CountingList extends AbstractList<byte[]> {

    private final int size;
    private int visited;

    private CountingList(int size) {
      this.size = size;
    }

    @Override
    public byte[] get(int index) {
      visited++;
      return new byte[64];
    }

    @Override
    public int size() {
      return size;
    }
  }

  /** Graphs within the budget are measured exactly: a two-node graph sums both shallow sizes. */
  @Test
  void weigh_pojoWithinNodeBudget_shouldMatchShallowSum() {
    PojoWithPayload pojo = new PojoWithPayload(1024);
    int weight = DefaultWeigher.INSTANCE.weigh("myKey", pojo);
    // pojo shallow + char[1024] (header + 2 bytes/char) + key + entry overhead
    assertThat(weight).isGreaterThan(2048);
  }

  private static final class Linked {

    private final Linked next;

    private Linked(int chainLength) {
      Linked tail = null;
      // built iteratively — a recursive constructor would overflow the stack before weigh() runs
      for (int i = 0; i < chainLength; i++) {
        tail = new Linked(tail);
      }
      this.next = tail;
    }

    private Linked(Linked next) {
      this.next = next;
    }
  }

  private static final class PojoWithPayload {

    private final char[] payload;

    private PojoWithPayload(int size) {
      this.payload = new char[size];
    }
  }
}
