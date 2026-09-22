/*
 * Copyright 2026 Hyshmily. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.hyshmily.zeta.cache.codec;

import com.github.benmanes.caffeine.cache.Weigher;
import io.github.hyshmily.zeta.model.CacheEntry;
import org.apache.lucene.util.RamUsageEstimator;
import org.jspecify.annotations.NonNull;
import org.springframework.util.Assert;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;
import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOf;

/**
 * Heap-weight estimator for {@link CacheEntry} values backed by Lucene's {@link
 * org.apache.lucene.util.RamUsageEstimator}.
 *
 * <p>Used when {@code zeta.local.cache.max-weight} is set. Object header / field / alignment
 * calculations are delegated to Lucene's {@code RamUsageEstimator} (which auto-detects compressed
 * OOPs, object alignment, and JVM pointer sizes), with per-class shallow sizes cached.
 *
 * <p><b>Fast path.</b> A value implementing {@link Weighable} reports its retained size directly —
 * O(1) with no walk. Recommended for values that already track their size (Caffeine's
 * "pre-calculated weights"); returning a non-positive size falls back to measurement.
 *
 * <p><b>Measurement model.</b> Everything else is priced as
 * {@code key + ENTRY_OVERHEAD + deep size}, where the deep size is a cycle-safe identity walk over:
 * <ul>
 *   <li>{@code String} / {@code byte[]} — payload included (text at two bytes per character, see
 *       below),</li>
 *   <li>{@link Collection} / {@link Map} / {@code Object[]} — the container's own overhead plus
 *       every element priced recursively; small containers exactly, wide ones from a sample
 *       (evenly spaced for arrays and random-access lists, first-elements otherwise) extrapolated
 *       with a safety factor,</li>
 *   <li>any other object — its shallow size plus a recursive walk over its non-static reference
 *       fields (nested {@code char[]} / {@code byte[]} payloads included).</li>
 * </ul>
 * Fields and classes annotated {@link Unweighed} are skipped — the escape hatch for shared context
 * a value reaches but does not own (Ehcache's {@code @IgnoreSizeOf}, Jamm's {@code @Unmetered}).
 *
 * <p><b>Why containers are walked through their public APIs.</b> {@code java.util} classes live
 * in a {@code java.base} package that is not open to the unnamed module, so reflective field access
 * to their backing storage fails: a field-only walk prices {@code ArrayList<byte[1MB]>} at its
 * 24-byte wrapper, i.e. four thousand times too light. Iterating the real element graph is what
 * keeps the estimate monotone in the payload — five 1 MB elements must weigh ≈ 5 MB, not "five
 * references".
 *
 * <p><b>Calibration.</b> The constants below were measured with JOL on JDK 21 / compressed OOPs
 * against Caffeine 3.2.1 and Zeta's variable-expiration node: node ≈ 110 B, hash-table slot ≈ 4 B,
 * 32-character ASCII key ≈ 72 B. {@link #ENTRY_OVERHEAD} therefore covers ≈ 114 B of Caffeine
 * bookkeeping with headroom; the original 512 B made every entry 2.25× heavier than reality
 * (measured), halving the effective budget in max-weight mode and evicting entries that fit.
 *
 * <p><b>Deliberate conservatism on text.</b> Text payloads are priced at two bytes per character.
 * Compact strings store Latin-1 text in one byte per character, so ASCII values are over-priced by
 * up to 2× — accepted, because the one-byte-per-char alternative would under-price CJK text, and an
 * over-estimate costs an early eviction while an under-estimate holds memory.
 *
 * <p><b>Cost.</b> The walk runs on the write path, and Caffeine computes the weight inside
 * {@code data.compute(...)} on its {@code computeIfAbsent} path — i.e. while holding that key's bin
 * lock — so the cost is bounded twice over: wide containers are sampled and extrapolated
 * ({@link #CONTAINER_SAMPLE_SIZE}) instead of walked element by element, and the whole walk is
 * capped at the configured node budget. Measured per call: ≈ 0.2 µs for a scalar payload, ≈ 0.6 µs
 * for a shallow POJO, ≈ 2–4 µs for a container of any size (the cost is the sample, not the element
 * count), ≈ 30 µs worst case on the reflection path. Weights are recomputed on every write (cache
 * miss / refresh), never on the read hot path.
 *
 * <p><b>Remaining under-counts.</b> A payload hidden behind unreadable fields (JDK internals such
 * as the {@code long[]} inside {@code BitSet}) or behind a container implementation's private
 * wrapper nodes ({@code LinkedList}/{@code HashSet} nodes) is priced at its container allowance
 * rather than its true size. Non-random-access containers are sampled from their first elements, so
 * a payload clustered later in iteration order can still be under-priced — a {@code HashMap}
 * iterates in hash order (effectively a random sample) while a sorted or insertion-ordered
 * container can be biased. Such values stay proportionally priced — the element payloads are
 * measured, only the wrapper is approximated — so no shape becomes free.
 */
public final class DefaultWeigher implements Weigher<String, Object> {

  /** Default node budget, also used by {@link #INSTANCE}. Package-private for tests. */
  static final int DEFAULT_GRAPH_NODE_BUDGET = 128;

  /**
   * Per-entry Caffeine bookkeeping: the variable-expiration node plus the amortized hash-table
   * slot. Measured at ≈ 114 B on JDK 21 with compressed OOPs; 128 keeps headroom for the
   * read-buffer and queue linkages while staying close enough that a max-weight budget means what
   * it says. Package-private so tests can assert the formula.
   */
  static final int ENTRY_OVERHEAD = 128;

  /**
   * Per-element bookkeeping inside a {@link Collection}: the container's own wrapper node, taken at
   * the largest common implementation ({@code HashMap.Node} = 32 B; {@code LinkedList.Node} = 24 B;
   * an {@code ArrayList} needs none). The backing-array reference slot is priced separately via
   * {@link org.apache.lucene.util.RamUsageEstimator#NUM_BYTES_OBJECT_REF}. Deliberately at the high
   * end: lists are over-priced slightly, node-backed sets are covered, and the element payload
   * itself is always measured separately.
   */
  static final int COLLECTION_ELEMENT_WEIGHT = 32;

  /**
   * Per-entry bookkeeping inside a {@link Map}: {@code HashMap.Node} (16 B header + hash + key,
   * value and next references = 32 B). The table slot is priced separately via
   * {@link org.apache.lucene.util.RamUsageEstimator#NUM_BYTES_OBJECT_REF}, at one slot per entry
   * rather than the ≈ 1.33 slots a 0.75 load factor implies — a slight under-count of the table,
   * which the node term dominates.
   */
  static final int MAP_ENTRY_WEIGHT = 32;

  /**
   * Scale applied to the walked prefix when a graph exhausts the node budget and the policy is to
   * extrapolate. The earlier 2× factor under-priced a 100 000-node chain by ≈ 12× (measured), which
   * left max-weight eviction blind to values whose size grows with node count.
   */
  static final int OVER_BUDGET_SCALE = 8;

  /**
   * Lower bound for an over-budget graph: whatever the walk managed to see before the budget
   * tripped, a structure large enough to exceed the node budget is never priced below 1 MiB, so a
   * cache cannot be filled with thousands of "cheap" unpriceable structures.
   */
  static final long OVER_BUDGET_FLOOR = 1L << 20;

  /**
   * How many elements of a wide container are measured before the rest is extrapolated. Sampling
   * turns pricing a 10 000-element list from O(10 000) into O(8) — measured 134 µs → ≈ 1 µs — at
   * the price of assuming the sample represents the tail; {@link #EXTRAPOLATION_SAFETY} covers that
   * assumption.
   */
  static final int CONTAINER_SAMPLE_SIZE = 8;

  /**
   * Safety factor on extrapolated container tails. The sample cannot cover every ordering — a
   * container ordered by size could hide larger elements behind it — so the unmeasured part is
   * doubled rather than taken at face value: an over-estimate evicts early, an under-estimate holds
   * unbounded memory.
   */
  static final int EXTRAPOLATION_SAFETY = 2;

  /**
   * The singleton instance with the default node budget and extrapolate-on-exhaustion policy, also
   * the shape historical callers use ({@code DefaultWeigher.INSTANCE}).
   */
  public static final DefaultWeigher INSTANCE = new DefaultWeigher(DEFAULT_GRAPH_NODE_BUDGET, false);

  private final int graphNodeBudget;

  /**
   * Whether a graph that exhausts the node budget is priced above any budget ({@code true}, the
   * entry is evicted immediately — Ehcache's abort semantics) or extrapolated from the walked
   * prefix ({@code false}, the default).
   */
  private final boolean abortWhenBudgetExhausted;

  private DefaultWeigher(int graphNodeBudget, boolean abortWhenBudgetExhausted) {
    this.graphNodeBudget = graphNodeBudget;
    this.abortWhenBudgetExhausted = abortWhenBudgetExhausted;
  }

  /**
   * Create a weigher with a custom node budget and over-budget policy.
   *
   * @param graphNodeBudget          max object-graph nodes visited per {@code weigh} call (≥ 1);
   *                                 bounds the per-write CPU cost — wide containers are sampled and
   *                                 never consume this budget in bulk
   * @param abortWhenBudgetExhausted {@code true} to price a budget-exhausted value above any budget
   *                                 so it is evicted immediately (Ehcache-style abort); {@code
   *                                 false} to extrapolate from the walked prefix
   * @return a weigher with the given policy
   */
  public static DefaultWeigher of(int graphNodeBudget, boolean abortWhenBudgetExhausted) {
    Assert.isTrue(graphNodeBudget >= 1, "graphNodeBudget must be ≥ 1");
    return new DefaultWeigher(graphNodeBudget, abortWhenBudgetExhausted);
  }

  /** Per-class shallow size cache: Lucene recomputes field offsets on every call. */
  private static final ClassValue<Long> SHALLOW_SIZES = new ClassValue<>() {
    @Override
    protected Long computeValue(@NonNull Class<?> type) {
      return RamUsageEstimator.shallowSizeOfInstance(type);
    }
  };

  /**
   * Object-array header size (16 B with compressed OOPs), resolved from the JVM once so the
   * container model needs no hard-coded header constant.
   */
  private static final long ARRAY_HEADER = shallowSizeOf(new Object[0]);

  /**
   * Shallow size of a node. For arrays it depends on the length, for everything else on the class
   * only — which is why the per-class cache is safe and cuts the per-node reflection cost.
   *
   * @param node the object to size
   * @return the shallow size in bytes, payload excluded
   */
  private static long shallowSizeOfObject(Object node) {
    Class<?> type = node.getClass();
    return type.isArray() ? shallowSizeOf(node) : SHALLOW_SIZES.get(type);
  }

  /**
   * Non-static, non-primitive instance fields per class, resolved once per class and kept
   * accessible. Fields annotated {@link Unweighed} are excluded, as are fields that cannot be made
   * accessible (module encapsulation) — the estimate then under-counts by exactly those fields.
   */
  private static final ClassValue<List<Field>> REFERENCE_FIELDS = new ClassValue<>() {
    @Override
    protected List<Field> computeValue(Class<?> type) {
      List<Field> fields = new ArrayList<>();
      for (Class<?> c = type; c != null && c != Object.class; c = c.getSuperclass()) {
        for (Field f : c.getDeclaredFields()) {
          if (
            Modifier.isStatic(f.getModifiers()) || f.getType().isPrimitive() || f.isAnnotationPresent(Unweighed.class)
          ) {
            continue;
          }
          if (f.trySetAccessible()) {
            fields.add(f);
          }
        }
      }
      return fields;
    }
  };

  @Override
  public int weigh(@NonNull String key, @NonNull Object value) {
    long keyWeight = shallowSizeOfObject(key) + ((long) key.length() << 1);

    // Fast path: a value that knows its own size is priced in O(1), no walk state at all.
    if (value instanceof Weighable weighable) {
      long self = weighable.weighInBytes();
      if (self > 0) {
        return (int) Math.min(keyWeight + self + ENTRY_OVERHEAD, Integer.MAX_VALUE);
      }
      // Non-positive means "unknown" — fall through and measure.
    }

    Walk walk = new Walk(graphNodeBudget);
    measureValue(walk, value);
    long total = keyWeight + (walk.exhausted ? onBudgetExhausted(walk.total) : walk.total) + ENTRY_OVERHEAD;
    return (int) Math.min(total, Integer.MAX_VALUE);
  }

  /**
   * Per-call walk state: the identity visited set (cycle safety), the remaining node budget, and
   * the running total. A fresh instance per {@code weigh} call keeps the walk free of shared
   * mutable state — the weigher is called concurrently from every writer thread.
   */
  private static final class Walk {

    private final Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    private int budget;
    private long total;
    private boolean exhausted;

    private Walk(int budget) {
      this.budget = budget;
    }
  }

  /**
   * Conservative price for a graph that exhausted the node budget under the extrapolate policy.
   *
   * <p>This is a heuristic, not a bound: the unwalked remainder is unknown by construction, so the
   * walked prefix is scaled by {@link #OVER_BUDGET_SCALE} and floored at {@link #OVER_BUDGET_FLOOR}.
   * Both error directions are then bounded in the direction that matters — an over-budget value is
   * evicted early rather than kept at a token price — while a genuinely huge graph stays
   * proportionally expensive instead of near-free.
   *
   * @param walked the size measured before the budget tripped
   * @return the conservative weight for the whole graph
   */
  private static long overBudgetWeight(long walked) {
    return Math.max(walked * OVER_BUDGET_SCALE, OVER_BUDGET_FLOOR);
  }

  /**
   * Price for a budget-exhausted graph under this weigher's policy: {@code abort} prices the value
   * above any budget so Caffeine evicts it immediately, {@code extrapolate} keeps the conservative
   * heuristic of {@link #overBudgetWeight(long)}.
   *
   * @param walked the size measured before the budget tripped
   * @return the weight for the whole graph
   */
  private long onBudgetExhausted(long walked) {
    return abortWhenBudgetExhausted ? Integer.MAX_VALUE : overBudgetWeight(walked);
  }

  /**
   * Measure {@code node} into {@code walk}, recursing into whatever it references.
   *
   * <p>Recursive rather than queued: the visit order does not matter for a size sum, recursion keeps
   * the per-node cost to one identity lookup plus one shallow-size call (no queue, no second pass
   * over the frontier), and the depth is bounded by the node budget so a pathological graph cannot
   * overflow the stack. Cycle safety comes from the identity visited set.
   */
  private void measureValue(Walk walk, Object node) {
    if (node == null || walk.exhausted || !walk.seen.add(node)) {
      return;
    }
    if (walk.budget-- <= 0) {
      walk.exhausted = true;
      return;
    }

    // Nested fast path: an element that knows its own size stops the recursion here.
    if (node instanceof Weighable weighable) {
      long self = weighable.weighInBytes();
      if (self > 0) {
        walk.total += self;
        return;
      }
      // Non-positive means "unknown" — fall through and measure.
    }

    if (node instanceof Collection<?> c) {
      // shallowSizeOf(Collection) covers the wrapper object only, never its backing array.
      walk.total +=
        shallowSizeOfObject(c) + ARRAY_HEADER + (long) c.size() * (COLLECTION_ELEMENT_WEIGHT + NUM_BYTES_OBJECT_REF);
      measureElements(walk, c, c.size());
      return;
    }
    if (node instanceof Map<?, ?> m) {
      walk.total += shallowSizeOfObject(m) + ARRAY_HEADER + (long) m.size() * (MAP_ENTRY_WEIGHT + NUM_BYTES_OBJECT_REF);
      measureEntries(walk, m);
      return;
    }
    Class<?> type = node.getClass();
    if (node instanceof String s) {
      walk.total += shallowSizeOfObject(s) + ((long) s.length() << 1);
      return;
    }
    if (type.isArray()) {
      // An Object[]'s reference slots are part of its shallow size — adding them again, as an
      // earlier revision did, double-counted them while still ignoring the elements.
      walk.total += shallowSizeOfObject(node);
      if (!type.getComponentType().isPrimitive()) {
        Object[] array = null;
        if (node instanceof Object[]) {
          array = (Object[]) node;
        }
        measureElements(walk, Arrays.asList(array), array.length);
      }
      return;
    }
    if (type.isAnnotationPresent(Unweighed.class)) {
      // Checked after the fast paths: String and array classes cannot carry the annotation, and
      // the lookup is not free on the shapes every write prices. The reference slot is already
      // priced by the parent; the content is excluded by contract.
      return;
    }
    walk.total += shallowSizeOfObject(node);
    for (Field f : REFERENCE_FIELDS.get(type)) {
      Object child;
      try {
        child = f.get(node);
      } catch (IllegalAccessException e) {
        continue;
      }
      measureValue(walk, child);
    }
  }

  /**
   * Measure a container's elements: all of them when there are few, otherwise a sampled tail
   * extrapolated from the sample.
   *
   * <p>Sampling strategy depends on what the container allows:
   * <ul>
   *   <li>{@code Object[]} and random-access {@link List} — elements are read at evenly spaced
   *       indexes across the whole range, so a payload clustered anywhere (sorted by size, appended
   *       in size order, sitting in the middle) is still seen. Measured on a 1 000-element list
   *       whose payload sits entirely outside the first 8 slots: the prefix sample under-priced it
   *       ≈ 9 500×, the spread sample landed at 1.8×.</li>
   *   <li>every other container — the first {@link #CONTAINER_SAMPLE_SIZE} elements via iteration.
   *       Without random access there is no cheaper representative sample; the iteration order
   *       decides how representative it is (a {@code HashMap} iterates in hash order, which is
   *       effectively a random sample, while a sorted or insertion-ordered container can be
   *       biased).</li>
   * </ul>
   * Iteration goes through the public API because {@code java.util} lives in a {@code java.base}
   * package that is not open to the unnamed module — its backing storage cannot be read
   * reflectively.
   *
   * @param size the container's declared size, so a sampled tail needs no second pass
   */
  private void measureElements(Walk walk, Collection<?> elements, int size) {
    if (walk.exhausted) {
      return;
    }
    if (size <= CONTAINER_SAMPLE_SIZE) {
      for (Object element : elements) {
        measureValue(walk, element);
      }
      return;
    }
    if (elements instanceof RandomAccess && elements instanceof List<?> list) {
      // Evenly spaced indexes: one get per sample, no O(size) pass, no prefix bias.
      long sampled = 0;
      int measured = 0;
      for (int i = 0; i < CONTAINER_SAMPLE_SIZE; i++) {
        long before = walk.total;
        measureValue(walk, list.get((int) (((long) i * size) / CONTAINER_SAMPLE_SIZE)));
        sampled += walk.total - before;
        measured++;
        if (walk.exhausted) {
          return;
        }
      }
      extrapolate(walk, size, measured, sampled);
      return;
    }
    // The guard runs before the fetch, so a sampled container is never read past
    // CONTAINER_SAMPLE_SIZE elements — an enhanced-for would pull one extra element before its
    // body could break.
    long sampled = 0;
    int measured = 0;
    Iterator<?> iterator = elements.iterator();
    while (measured < CONTAINER_SAMPLE_SIZE && iterator.hasNext()) {
      long before = walk.total;
      measureValue(walk, iterator.next());
      sampled += walk.total - before;
      measured++;
      if (walk.exhausted) {
        return;
      }
    }
    extrapolate(walk, size, measured, sampled);
  }

  /** Measure a map's entries, sampling the tail on the same rule as {@link #measureElements}. */
  private void measureEntries(Walk walk, Map<?, ?> map) {
    int size = map.size();
    if (walk.exhausted) {
      return;
    }
    if (size <= CONTAINER_SAMPLE_SIZE) {
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        measureValue(walk, entry.getKey());
        measureValue(walk, entry.getValue());
      }
      return;
    }
    long sampled = 0;
    int measured = 0;
    Iterator<? extends Map.Entry<?, ?>> iterator = map.entrySet().iterator();
    while (measured < CONTAINER_SAMPLE_SIZE && iterator.hasNext()) {
      Map.Entry<?, ?> entry = iterator.next();
      long before = walk.total;
      measureValue(walk, entry.getKey());
      measureValue(walk, entry.getValue());
      sampled += walk.total - before;
      measured++;
      if (walk.exhausted) {
        return;
      }
    }
    extrapolate(walk, size, measured, sampled);
  }

  /**
   * Price a sampled container's unmeasured tail from the sampled average, scaled by
   * {@link #EXTRAPOLATION_SAFETY}. The sampled part is already in {@code walk.total}, so only the
   * difference is added. A budget-exhausted walk is left alone — the verdict for the whole graph is
   * {@link #onBudgetExhausted(long)}.
   */
  private static void extrapolate(Walk walk, int size, int measured, long sampled) {
    if (walk.exhausted || measured == 0) {
      return;
    }
    long full = (long) size * (sampled / measured) * EXTRAPOLATION_SAFETY;
    walk.total += Math.max(0, full - sampled);
  }
}
