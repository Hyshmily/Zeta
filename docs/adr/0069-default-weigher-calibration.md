# ADR-0069: DefaultWeigher Calibration (max-weight mode)

- Status: Accepted
- Date: 2026-09-18
- Related: ADR-0023 (annotation three-layer responsibility model), `zeta.local.cache.max-weight` (CONFIG.md)

## Problem

`DefaultWeigher` is the sole capacity oracle when `zeta.local.cache.max-weight > 0`: its return value decides what Caffeine evicts. Its javadoc claimed "an over-estimate costs an early eviction while an under-estimate costs unbounded memory", but measurement (JOL 0.17, JDK 21, compressed oops, Caffeine 3.2.1 with Zeta's variable-expiration `Expiry`) showed both error directions at once, with the under-estimates far larger than the over-estimates:

**Over-estimates (wasted budget)**

| Item | Weigher | Measured | Error |
| --- | --- | --- | --- |
| Per-entry fixed cost (`ENTRY_OVERHEAD` + key) | 600 B | 186 B (node 110 + key 72 + table slot 4) | 3.2× |
| Empty `ArrayList` / empty `HashMap` | 224 / 398 B | 40 / 48 B | 5.6× / 8.3× |
| Synthetic typical entry (32-char key + `CacheEntry(String 64)`) | 832 B | 370 B | 2.25× |

`ENTRY_OVERHEAD = 512` alone exceeded the entire real entry, so max-weight ran at ≈ 44% of its nominal capacity.

**Under-estimates (`max-weight` blind, the dangerous direction)**

| Value | Weigher | Measured | Under |
| --- | --- | --- | --- |
| `ArrayList<byte[1MB]>`(5) | 1 024 B | 5 000 144 B | 1/4882 |
| `HashMap<String,byte[1MB]>`(1) | 398 B | 1 000 224 B | 1/2513 |
| `Object[]{String(1MB)}` | 28 B | 1 000 064 B | 1/50003 |
| `BitSet` (1 MB backing array) | 24 B | 1 000 040 B | 1/41668 |
| `Linked` chain, 100 000 nodes | 131 072 B | 1 600 016 B | 1/12.2 |

Root causes:

1. The `Collection` / `Map` branches priced by "element count × constant" without measuring element content.
2. The `Object[]` branch added the reference slots twice (`shallowSizeOf(array)` already includes `length × NUM_BYTES_OBJECT_REF`) while still ignoring the elements.
3. `graphSize` relied on reflective field reads; `java.util` / `java.time` live in non-open `java.base` packages so `trySetAccessible()` fails (measured: 0 visible reference fields for `BitSet` / `LocalDateTime` / `HashMap`) → shallow size only; the 1 MB `long[]` behind a `BitSet` was invisible.
4. On budget exhaustion the code returned `2 × walked prefix`; the prefix is only part of the graph, so a 100 000-node chain was under-priced 12.2× — the opposite of the documented "conservative" intent.

Production reachability: the compressor degrades to `CacheCompressor.NONE` only when lz4 is absent (`ZetaAutoConfiguration#cacheCompressor`), and `Lz4CacheCompressor.wrap` compresses only `String` / `byte[]` (threshold `CacheCompressor.MIN_COMPRESS_LENGTH = 256`) — **every other value type is stored as a live object graph**, so all of the above branches are on the default deployment path.

The test suite gave no protection: 15 of the 17 original `DefaultWeigherTest` cases only asserted `isPositive()`; the constants were never calibrated.

## Decision

**1. Containers are measured by content, not by count.** `Collection` / `Map` / `Object[]` elements are measured recursively through the public iteration APIs (`measureElements` / `measureEntries`), sharing one cycle-safe walk with POJO field traversal. Iteration instead of reflection is forced by module encapsulation: the private backing storage of `java.util` classes is not readable reflectively, so a field-only walk would forever price an `ArrayList<byte[1MB]>` at its 24-byte wrapper.

**2. Constants recalibrated against measurement; made package-private for test assertions.**

- `ENTRY_OVERHEAD`: 512 → **128** (measured node 110 + table slot 4 = 114, with headroom).
- `COLLECTION_ELEMENT_WEIGHT`: 200 → **32** (per-element container bookkeeping, bounded by the largest common wrapper: `HashMap.Node` 32 / `LinkedList.Node` 24 / `ArrayList` none; the backing-array slot is priced separately via `NUM_BYTES_OBJECT_REF`).
- `MAP_ENTRY_WEIGHT`: 350 → **32** (`HashMap.Node` measured at 32 B: 16-byte header + hash + key/value/next), table slot priced 1:1 per entry.
- Array headers are no longer hard-coded: `ARRAY_HEADER = shallowSizeOf(new Object[0])`.

**3. The `Object[]` double count is removed.** The array branch takes `shallowSizeOf(array)` plus measured elements — the reference slots are already inside the array's shallow size.

**4. Budget exhaustion becomes an explicit heuristic.** `overBudgetWeight(walked) = max(walked × OVER_BUDGET_SCALE(8), OVER_BUDGET_FLOOR(1 MiB))`.
- Rejected alternative: return `Integer.MAX_VALUE` on exhaustion ("refuse to price the unpriceable") — semantically clean, but it silently makes every legitimate value with more than the budget's nodes uncachable, a behaviour change for existing max-weight users.
- The adopted heuristic admits it is not a bound: it guarantees an over-budget structure is not priced at its walked prefix (possibly a few KB) while a genuinely huge graph stays proportionally expensive.
- `OVER_BUDGET_FLOOR` is 1 MiB = `GRAPH_NODE_BUDGET × 256 B` (a conservative 256 B per visited node), so thousands of unpriceable structures cannot fill a cache at token cost.
- Known cost: a graph just past the budget is over-priced 2–6× (early eviction preferred).

**5. Text remains priced at two bytes per character.** Compact strings store Latin-1 text at one byte per character, so ASCII is over-priced up to 2×. Kept deliberately: the one-byte alternative would under-price CJK text, and an over-estimate evicts early while an under-estimate holds memory.

## Traversal cost (follow-up)

The first fix priced containers correctly but turned the write path from O(1) into O(n). Measured (JDK 21, ns per call):

| Value | Before | First fix | Final |
| --- | --- | --- | --- |
| `List<String>(10)` | 340 | 2 959 | 2 407 |
| `List<String>(10 000)` | 293 | **155 183** | **2 194** |
| `Map<String,String>(10 000)` | 540 | **157 291** | **4 146** |
| `Linked` chain 4096 | 300 730 | 707 331 | **19 352** |
| `Linked` chain 100 000 | 291 871 | 788 694 | **23 050** |

155 µs per write is unacceptable beyond throughput: Caffeine computes the weight inside the `data.compute(...)` mapping function on its `computeIfAbsent` path (`BoundedLocalCache:2692-2699`), i.e. **while holding that key's bin lock** — one large value blocks every other key in the same bin for 150 µs, violating the ADR-0030 discipline of no heavy work under the lock.

Three measures:

1. **Container sampling with extrapolation**: when `size > CONTAINER_SAMPLE_SIZE(8)`, measure the sampled elements and price the tail as "sample average × element count × `EXTRAPOLATION_SAFETY(2)`". Cost becomes O(sample), independent of container size (measured 2–4 µs; a 10-element and a 10 000-element container cost the same).
2. **Node budget 4096 → 128** (`GRAPH_NODE_BUDGET`): containers no longer consume the budget in bulk, so it only bounds reflection-path depth; worst case drops from ~700 µs to ~23 µs.
3. **Recursive walk with an iterator-guarded sample**: the `ArrayDeque` and the extra frontier pass are gone; the sampling loop uses an explicit `Iterator` with `hasNext()` before `next()`, so a wide container is read exactly `CONTAINER_SAMPLE_SIZE` times — an enhanced-for pulls one extra element before the body can break (caught by the new `weigh_wideContainer_shouldVisitOnlyTheSamplePrefix` test).

Known trade-offs (documented, no longer presented as exact measurement):

- **The sample is a prefix or an evenly spaced spread; a container ordered adversarially can still hide mass from it.** `EXTRAPOLATION_SAFETY = 2` covers that assumption (prefer over-estimate).
- **POJO graphs over 128 nodes** are truncated and priced `max(walked × 8, 1 MiB)` (the first fix measured them exactly within 4096 nodes) — deep object graphs are now over-priced in exchange for a hard write-path CPU bound; `max-weight` users with such values should use `max-size`.
- Small containers (≤ 8 elements) are still measured exactly; their cost rose from ~340 ns to ~2.4 µs — the intrinsic price of actually reading element content, not a removable constant.

## Sampling accuracy (follow-up)

Sampling introduced its own error surface, measured per shape (JOL, including the ≈ 186 B per-entry overhead; SHIPPED = current implementation):

| Value | Model truth | SHIPPED | vs reality |
| --- | --- | --- | --- |
| `List<byte[1KB]>` × 8 (≤ sample) | 0.01 MB | 0.01 MB | **1.03×** |
| `List<byte[1KB]>` × 1 000 (uniform) | 1.03 MB | 2.02 MB | 2.03× |
| `List<byte[1MB]>` × 100 (uniform) | 95.37 MB | 190.74 MB | 2.00× |
| `List<String(1KB)>` × 1 000 | 2.01 MB | 3.99 MB | 3.91× |
| `Map<String,byte[1KB]>` × 1 000 | 1.06 MB | 2.08 MB | 1.92× |
| `Object[]{byte[1KB]}` × 1 000 | 1.00 MB | 1.99 MB | 2.00× |
| `List<Dto{String64,byte[512]}>` × 1 000 | 0.71 MB | 1.38 MB | 2.18× |
| **Tail-heavy**: first 8 tiny, rest 1 MB × 992 | 946.09 MB | 1 669.00 MB | **1.76×** (prefix sample: 0.00×) |
| **Head-heavy**: first 8 × 1 MB, rest 16 B | 7.69 MB | 238.51 MB | 31.12× (prefix: 248.86×) |
| **Middle-heavy**: 400–600 × 1 MB | 190.80 MB | 238.51 MB | **1.25×** (prefix: 0.00×) |
| **Map tail-heavy** 200 × 256 KB (no random access) | 46.89 MB | 0.03 MB | **0.00× (residual gap)** |

Conclusions:

- **Uniform load** sits at 1.9–2.2× over (`EXTRAPOLATION_SAFETY = 2` by design); text-heavy at 3.9×; ≤ 8 elements are exact.
- **Clustered load**: evenly spaced sampling (arrays and `RandomAccess` lists) turns the catastrophic prefix under-price (0.00×, i.e. ~1/9 500) into 1.25–1.76×; head-heavy becomes 31× over (safe direction).
- **`SAFETY = 2` must not drop to 1**: at 1 the middle-heavy case becomes 0.62× (under-estimate), violating the prefer-over-estimate principle — the data supports keeping 2.
- **Residual gap (declared)**: non-random-access containers (`Map` / `Set` / `LinkedList`) can only be prefix-sampled, so a clustered payload can still be under-priced (1/1 563 here). A `HashMap` iterates in hash order, which is effectively a random sample and mitigates this; sorted or insertion-ordered containers do not. Such workloads should use `max-size`.

## Consequences

- max-weight capacity now tracks real heap occupancy: the typical entry went from 832 B to 448 B (≈ 1.2× measured), roughly doubling effective capacity; `ArrayList<byte[]>` / `Map<String,byte[]>` / `Object[]{large}` values are priced from content instead of element counts.
- Remaining under-counts are declared in the `DefaultWeigher` javadoc ("Remaining under-counts") and are no longer presented as exact: payloads hidden behind unreadable fields (`BitSet`-style `java.base` internals), container-private wrapper nodes (`LinkedList` / `HashSet`), and clustered payloads in non-random-access containers. Element payloads are still measured in all of these — only wrappers are approximated — so no shape becomes free.
- Write-path cost is bounded: ≈ 0.2 µs scalar payloads, ≈ 0.6 µs shallow POJOs, ≈ 2–4 µs for containers of any size, ≈ 30 µs worst case on the reflection path.
- Weight semantics changed, so **existing max-weight deployments must re-plan**: the same `max-weight` now holds roughly twice as many entries (weights shrank). Set the value by target bytes, not by target entry count.
- `DefaultWeigher` constants moved from `private` to package-private (same precedent as `GRAPH_NODE_BUDGET`) for in-package test assertions; the public API is unchanged.

## Prior art (surveyed 2026-09-18)

| Project | Mechanism | What it confirms / what is worth borrowing |
| --- | --- | --- |
| Ehcache size-of engine (`net.sf.ehcache.pool`, `org.ehcache:sizeof`) | Three engines: AgentSizeOf (`Instrumentation`) → UnsafeSizeOf (`Unsafe` class layouts) → ReflectionSizeOf; `maxDepth` + `maxDepthExceededBehavior` (continue = default, logs a warning and continues / abort = give up and flag `hasAbortedSizeOf()`); `@IgnoreSizeOf` annotation + Filter SPI | Same shape as `GRAPH_NODE_BUDGET` + `overBudgetWeight`. Ehcache's default of 1 000 references has real-world performance complaints (SO 35074727), confirming the budget must be small. Worth borrowing: ① `Unsafe` class layouts instead of reflection; ② an "ignore this field" escape hatch for users |
| Spark `SizeTracker` (`SizeTrackingAppendOnlyMap`) | A single `SizeEstimator` call "can take a sizable amount of time (order of a few milliseconds)" → exponential-backoff sampling (1.1×), keep the last two samples, `bytesPerUpdate = Δsize/Δupdates`, O(1) `estimateSize()` | The official precedent for "sample and extrapolate"; different dimension (per update vs per element), same amortization pattern |
| Lucene `Accountable.ramBytesUsed()` | Data structures self-report bytes plus a `getChildResources()` tree; `RamUsageEstimator.sizeOf(Accountable[])` sums them | "Self-reporting over reflection". But Lucene #16113 shows ownership semantics left undefined cause shared memory to be double-counted (ES/Solr circuit-breaker drift), and #15026 shows `sizeOf(Accountable[])` double-counting shallow sizes. A self-reporting fast path requires ownership semantics first; this design's identity walk deduplicates by construction |
| Jamm `MemoryMeter` (Cassandra / Elasticsearch lineage) | javaagent + `Instrumentation.getObjectSize` + reflective graph crawl; IdentityHashMap against double-counting and cycles; `@Unmetered` annotation; swappable tracker | Its README measures "one million objects ≈ 5 seconds" — direct confirmation that deep walks must be budget-capped; `@Unmetered` is the same escape hatch as `@IgnoreSizeOf`; also warns about the WEB-INF/lib dual-classloader agent trap |
| Caffeine's official `Weigher` guidance | "Keep weighing logic fast and lightweight", "Never perform expensive computations in the weigher", "Avoid I/O"; weights are measured on insert/update and stay static for the entry's lifetime; recommends "static or pre-calculated weights" | Direct endorsement of a value-carried-weight fast path (e.g. `document.sizeInBytes()`, images as `w*h*4`); this design's per-character overhead matches its "account for object overhead" guidance |
| Twitter `ObjectSizeCalculator` (same source as the OpenJDK nashorn class, widely copied) | `Unsafe.objectFieldOffset`-based per-class layouts, HotSpot/OpenJDK only, "doesn't eat memory or time" | Same idea as Ehcache's UnsafeSizeOf; a candidate accelerator for the reflection path |

Conclusion: this design (node-budget cap + container sampling with extrapolation + identity dedup + conservative direction) matches mainstream practice, and no simpler existing scheme replaces it. All three borrowable candidates were adopted in this same revision — treat them as implemented, not as open TODOs: ① the value-carried-weight fast path is `Weighable` (short-circuits in `weigh()` and per node in `measureValue()`; the ownership contract — per-entry accounting, shared subgraphs counted once per entry, `0`/negative falls back to measurement — is spelled out in its Javadoc); ② per-class shallow-size caching is the `SHALLOW_SIZES` / `REFERENCE_FIELDS` `ClassValue`s (reflection amortized to once per class; `Unsafe` layouts deliberately not adopted — once cached, the per-node residual is one `Field.get`, not worth JDK-internal-API maintenance); ③ the configurable budget with abort semantics is `weigh-walk-nodes` + `WeighOverBudget.ABORT/EXTRAPOLATE`, and the `@IgnoreSizeOf`-style escape hatch is `@Unweighed` (field- and type-level, filtered once per class when the field cache is built — zero per-node cost; the type-level check runs after the `String` / array fast paths, which cannot carry the annotation).

## Verification

- `DefaultWeigherTest` grew from 17 to 29 cases; the new ones assert calibration rather than `isPositive()`:
  - `weigh_collectionWithLargePayloads_shouldPriceElementContent`: `List<byte[1MB]>` vs `List<byte[16]>` differ by more than 4.9 MB.
  - `weigh_mapWithLargeValues_shouldPriceEntryContent` and `weigh_objectArrayWithLargeElement_shouldPriceElementContent`: same for maps and arrays (> 990 KB deltas).
  - `weigh_objectArray_shouldNotDoubleCountReferenceSlots`: `new Object[1000]` minus `new Object[0]` equals exactly `1000 × NUM_BYTES_OBJECT_REF` (regression lock on the double count).
  - `weigh_emptyContainer_shouldNotBeChargedPerElementWeight`: an empty container costs at most 64 B more than an empty array (the old code charged 200 B).
  - `weigh_typicalEntry_shouldStayWithinCalibratedBound`: the typical entry lands between 300–560 B (the old 832 B fails).
  - `weigh_deepPojoOverNodeBudget_shouldPriceAtLeastTheFloor`: over-budget graphs cost at least `OVER_BUDGET_FLOOR`.
  - `weigh_wideContainer_shouldVisitOnlyTheSamplePrefix`: a self-counting `AbstractList` proves a wide container is read at most `CONTAINER_SAMPLE_SIZE` times — this test failed against the first fix (enhanced-for read 9 elements) and drove the iterator guard.
  - `weigh_wideContainer_shouldScaleWithElementCount`: estimates for 1 000 and 10 000 elements stay proportional, guarding against a degenerate constant sample.
  - `weigh_sampledContainer_shouldStayProportionalToPayload`: 1 000 × `byte[1KB]` lands within 1.5–3× of the real payload, pinning the safety factor.
  - `weigh_randomAccessListWithClusteredPayload_shouldSeeTheTail`: a payload clustered outside the first slots must be seen by the spread sample (the prefix-sampling implementation fails).
- The pre-existing precision cases (POJO deep-walk differential > 10 MB, both sides of `GRAPH_NODE_BUDGET`) still pass.
- All numbers above come from standalone probes: JOL 0.17 + JDK 21 + compressed oops, replicating Zeta's variable-expiration `Expiry` and container model, comparing `weigh(key, value)` against `GraphLayout.parseInstance(value).totalSize()`.
- `mvn.cmd -pl common test`: **2128 tests, 0 failures, 0 errors, 1 skipped — BUILD SUCCESS**.
