# Regionalized Key-Space Monitoring Rejected (Worker Detection Stays Per Exact Key)

The kernel-inspired optimization report (§13.1) proposes replacing the Worker's per-exact-key monitoring with a DAMON-style **prefix region tree**: regions seeded from `@Preload`/rules/observed prefixes, HeavyKeeper over region IDs, hot regions split and drilled down to exact keys, cold regions merged and reported, and the report payload shrunk from O(keys) to O(regions). The report rates it P3/high cost and sets its own prerequisite — an independent ADR plus a benchmark comparison of detection-accuracy loss. We re-verified every premise against the code and measured the costs the redesign targets. **The region tree is rejected**: DAMON's cost model rests on the monitor owning the sampled address space, and Zeta's Worker does not own the key space. The report's own companion ③ (score-watermark admission, `damos_adjust_quota`'s "truncate by score, not by count") is promoted from companion-of-a-rejected-design to the standalone accepted direction for bounding Worker per-key state — and it lands *after* a measurement baseline that does not exist yet.

## Status

rejected (2026-09-29) — §13.1's region tree; its companion ③ split out as the accepted direction, companion ④ split out as an independent item.

## Context

### 1. The load-bearing premise is missing: the monitor does not own the probe

DAMON's regionalization is affordable because the kernel **owns the target address space**: `__damon_va_prepare_access_check` picks `damon_rand(start, end)` inside each region and clears that address's access bit, and the next interval tests whether it came back (`vaddr.c:360-501`). Monitoring cost is O(regions) because the monitor can *interrogate* the target.

Zeta's Worker cannot. Its only inbound data channel is `ReportConsumer.onReport(ReportMessage)` (`worker/.../ingest/ReportConsumer.java:177-246`), carrying `Map<String, Long>` — the complete set of keys the **App** chose to send. The App counts exactly, unconditionally, per key (`KeyReporterImpl.reportToWorker` → `WaveCounter.count(key, TOPK_INCR)`, `CentralDispatcher.report`), with no per-key sampling, throttling or probabilistic drop anywhere. There is **no Worker→App data-request message type**; Worker→App traffic is HOT/COOL broadcasts, heartbeat and rules/config gossip only. ADR-0078:13 already rejected the sibling DAMON knob for exactly this structural reason: "the Worker is *push-driven* (RabbitMQ → `SlidingWindowDetector`); there is no polling period to stretch".

The report itself declares companion ① (single-sample-point two-phase protocol) indispensable — "缺了任何一个，区域化都不成立". Zeta has no isomorphic capability; making the App perform the sampling inverts the reporting contract (the App would have to know the region tree and deliberately report one key per region per window).

### 2. Cache-key heat has no prefix locality

DAMON's regions are contiguous address ranges, and its documented precision/size trade-off is tolerable because hot working sets are spatially clustered — a struct, an array, a hot function's data. Cache-key frequency is **not a smooth function of the key prefix**: the hot object is one specific item or user, and `user:123456`'s prefix neighbours are uncorrelated with it. Preserving resolution therefore forces the region down to ~one key — at which point the region tree carries the per-key cost it was introduced to remove. The report concedes the single-hot-key-in-a-region accuracy loss but treats it as an edge case; for cache keys it is the normal case.

### 3. The costs, measured

Probe run for this evaluation (JDK 21.0.11, Windows, single-threaded, JOL 0.17 for retained sizes — indicative, not JMH; the repo's own convention is to quote machine context). Sources, classpath and known limitations: `sandbox/regionalized-keyspace-probe/`.

| Quantity | Measured | Note |
|---|---|---|
| `SlidingWindowDetector` retained per key | **418.5 B** | exactly linear at 200k / 400k keys; of which the doubled slice buffer is 2 × 16 × 8 B = 256 B (`slices=10` aligns up to 16, `CONFIG.md:321`) |
| `DefaultEvaluator` per-key state (exclusive) | **~259 B** | CV history ring + EMA cell |
| `ZetaBayesianSM.KeyState` | **0 for never-hot keys** | `evaluate` returns `NONE` before materializing state (`state == null && !isHotThisWindow`, `ZetaBayesianSM.java:457-469`) — the codebase already admits expensive state lazily |
| **Worker total per reported key** | **≈ 678 B** | 10M distinct keys ≈ 6.8 GB ⇒ the report's "千万个精确 key" premise is right that today's shape cannot scale. Eviction is by **idle time only** (no cardinality cap): `EvictStaleTask` passes the short tier `cold-evict-interval-ms` (5 min default) to the detector, the evaluator and the state machine's COLD tier; only CONFIRMED_HOT/PRE_COOLING state keeps the 20-minute `evict-interval-ms` retention. |
| `Evaluator.evaluate(key, count, ratio)` | **1 723 / 1 762 ns per key** (cold fill / sustained) | ≈ 0.57 M keys/s single-threaded; ≈ 4.5 M keys/s at 8 consumers with contention ignored. **Memory saturates long before CPU.** |
| Report wire cost | **26 B/key** at 24-char keys (18/26/34/50 for 16/24/32/48) | exactly `1 + keyLen + 1`: ADR-0074's varint framing is already at the structural floor |
| Region node (CHM<String, 7-field stats>, 1M entries) | **165.6 B/region** | a constant-factor (~3×) win at region ≈ key, not an order of magnitude |
| `indexOf(':') + substring` | **30.3 ns/op and 56 B allocated per op** | per-access prefix aggregation on the App read path would be a multi-fold regression on a path the codebase measures at ~10 ns/op and has rejected 1-2 ns changes on (ADR-0043/0048); region aggregation is only affordable at the tide boundary |

Nothing here says the memory saving is illusory: replacing cold keys' ~678 B/key with ~166 B/region is real. It says the saving is **bounded by keys-per-region**, which is the same knob that costs detection accuracy.

### 4. Downstream semantics are per exact key, and the exactness invariants are load-bearing

- The decision unit is one exact key: `ZetaDecision.hot/cool(key, snapshot)`, broadcast as raw key bytes (`WorkerBroadcaster.broadcastHot/broadcastCool`), applied to that exact entry by `DefaultWorkerDecisionHandler.handleHot/handleCool`. No region-level decision type or handler exists, and unknown message types are WARN-dropped (ADR-0071).
- Reports are sharded **per exact key** (`ringManager.routeNode(key, isAlive)`), so a prefix region spans Workers; cross-Worker aggregation is explicitly out of scope for the existing estimator, which "only sees traffic routed to this Worker shard".
- ADR-0054/0057 make the promotion boundary the **exact** k-th largest — "proven, not sampled", byte-identical to the reference path on every reachable input — and ADR-0057 warns that "an approximate kth would re-introduce the exact systematic-bias class ADR-0054 removed". ADR-0038/0064 declare counting bounds (worst ≈2.1e-5/op, 0.01% stress bounds, "counting is byte-identical"). A region aggregate feeding any of these requires that bias analysis to be redone from scratch, and sampling one key per region per window is a far larger error than any bound those ADRs cover.
- The Worker's region view would be biased by App-side filters it cannot see: `ALLOW_NO_REPORT` rules, caller `skipReport` flags, and WaveCounter's soft cap that **drops new cold keys** past ~110k distinct keys per tide (`WaveCounter.java:1655-1668`).

### 5. The named seed sources do not reach the Worker

- **`@Preload` cannot seed anything Worker-side.** The annotation declares exact keys only (`keys[]` plus one SpEL-resolved `keyExpr`, `Preload.java:66-89`) and inflates the **App-local** sketch via `Zeta#notifyLocalDetectorDirect` — it never enters the report path.
- **Rules are prefix-capable but are the wrong seed and the wrong shape.** `RuleType.PREFIX` exists (a trailing `*` auto-converts), yet the App-side `RuleMatcher` and the Worker-side FastLane rule set are separate, independently versioned systems, only the latter lives on the Worker, rule size is assumed small and uncapped, and ADR-0065 already **rejected** building a structural index over rules because first-match-wins ordering decides semantics.
- **"Observed prefixes" has no implementing mechanism.** `ZetaLoaderRegistry` performs real longest-prefix matching (ADR-0070) but ships empty, is user-populated and serves the value-loading plane only; no prefix-observation counting path exists.
- There is no `region`/`zone`/`namespace` configuration anywhere in the tree, and no prior art: `drill`, `multi-resolution`, `coarse-to-fine` return zero matches repo-wide.

### 6. The prerequisite the report set for itself is unmet and currently unmeetable

§13.1 demands "独立 ADR + 基准（benchmark 模块）对照检测精度损失". `benchmark/` holds three JMH throughput benchmarks (`WaveCounterBenchmark`, `LogThrottleBenchmark`, `DefaultWeigherBenchmark`) and **no ground-truth accuracy metric of any kind**; no layer emits recall@K / precision / F1 / FPR; there is no prefix-structured workload generator (Java has one Zipf generator inside `WaveCounterSimulatorTest`; the Python sandbox has paired-seed methodology but no prefix/region structure); and the decision plane is unobservable — no HOT/COOL counters, no per-state gauges, no detection-latency timer, and the only Worker detection gauge (`zeta.worker.tracked.keys`) reads `ZetaBayesianSM.getTrackedKeys()`, which by construction excludes never-hot keys and hides the dominant `SlidingWindowDetector` map. **The last gap is closed by this ADR (four meters — see Decision); the others are not.**

## Decision

- **Rejected: the prefix region tree** — its region-ID sketch, merge/split dynamics, drill-down, region-level decisions and cold-region merging. Companions ① (single-sample-point protocol) and ② (merge-threshold doubling) fall with it: ① has no isomorphic Zeta capability (§1), ② has no state to guard once there is no region count.
- **Adopted as the direction for bounding Worker per-key state: companion ③, score-watermark admission** — "按分数截断而非按个数截断" (`damos_adjust_quota`). The Worker keeps per-exact-key decisions; a fixed-memory sketch over all reported keys gates the materialization of expensive per-key state behind a score watermark, so memory is bounded by the **admitted** key count rather than by cardinality, trading no resolution. It reuses in-repo assets rather than new architecture: the HeavyKeeper sketch with its documented precision model (ADR-0014/0026/0073), the exact k-th selection (ADR-0054/0057), and the lazy-state precedent already inside `ZetaBayesianSM.evaluate`.
  - Landing points compose rather than collide. ADR-0079:49 pre-commits the same water-mark semantics to the App-side `loadCacheEntry` choke point (distance gate / quota gate / negative budget as three independent evidence chains). The Worker-side admission is the second landing of the same idea; the two land independently.
  - **Not specified here.** It needs its own decision record (watermark source, admitted-key capacity, eviction policy, and its interaction with ADR-0078's rule that the window is the state machine's semantic time unit) and it lands only after the measurement baseline below.
- **Adopted: the measurement baseline, first.** Before any detection-architecture change: (a) expose `zeta.worker.detector.keys` (the per-key window map — the real memory driver; `getActiveKeyCount()` already exists but is unregistered), the `zeta.worker.decisions.hot` / `.cool` counters and `zeta.worker.report.eval` (monotonic per-report-batch evaluation latency) — shipped with this ADR; (b) a ground-truth scoring harness (recall@K / precision / detection latency) and a prefix-structured workload generator, built on the existing paired-seed sandbox methodology, remain open before any comparative claim is possible. A detection-architecture proposal without (b) cannot be judged, which is the state §13.1 was in.
- **Adopted: companion ④ (evidence token packing) as an independent small item**, orthogonal to this rejection — it lands on the CacheEntry/`DecisionStamp`/L2 value channel (`DecisionStamp` already carries `decisionVersion/nodeId/epoch`), not on the Worker's monitoring object.
- **Not pursued: "13.1-lite"** (flush-time App-side prefix aggregation into a flat region map, no Worker tree). Kept as a conditional item only if a workload is shown in which heat is genuinely prefix-correlated (a whole namespace going hot at once), because it costs a wire-format change (ADR-0074 versioning, Workers-first rolling upgrade) while per-access prefix extraction is forbidden on the hot path by §3.

## Considered Options

- **Adopt the region tree at a coarse region size** — rejected: benefit and precision are the same knob (§2, §3).
- **Adopt the region tree with drill-down, on the argument that only the cold majority needs coarsening** — rejected on cost/benefit, not on arithmetic: the memory saving is real, but the identical saving is obtainable with a *hash*-partitioned sketch plus a watermark at a fraction of the complexity — no prefix semantics, no merge/split controller, no second time base, no cross-shard aggregation, no new decision type, no wire change, and no accuracy loss. The region tree's unique addition is prefix readability, and the decision path has no consumer for it (decisions are per exact key and per exact entry, §4).
- **Do nothing (leave per-key state unbounded)** — rejected: ~678 B/key with no cardinality cap (eviction is by idle time only) means a high-cardinality scan can OOM a Worker; watermark admission is the bounded answer.
- **Build the accuracy harness and re-litigate the region tree with numbers** — deferred, not refused: this ADR records the harness as the standing prerequisite. Nothing in this evaluation suggests the outcome would flip, because the region tree's benefit is bounded by a knob whose cost is the accuracy metric itself.

## Consequences

1. §13.1 of the kernel-inspired report is rewritten as a rejected direction; the score-watermark companion is promoted to a standalone item and the report's implementation order is updated in the same change.
2. Worker per-key state remains unbounded until the admission item lands. Capacity planning must use ≈678 B/key × keys reported within `cold-evict-interval-ms` (5 min default — the tier the detector, the evaluator and the COLD state tier all use; only CONFIRMED_HOT/PRE_COOLING state is retained for the full 20-minute `evict-interval-ms`), now observable via `zeta.worker.detector.keys`.
3. Four meters ship with this ADR (one gauge, two emission counters, one timer — `zeta.worker.detector.keys`, `.decisions.hot`, `.decisions.cool`, `.report.eval`), giving the first baseline a future detection-architecture proposal can be measured against — including, notably, the memory driver that was previously invisible.
4. The comparison §13.1 demands (detection-accuracy loss) is still unbuildable: no ground-truth scoring harness and no prefix-structured workload generator exist. Recorded here as a prerequisite rather than a nice-to-have.
5. `@Preload`, rules and the loader registry are confirmed **not** to be a Worker-side region seed. Anyone revisiting the region idea must first answer two questions this evaluation could not: *who samples the key space*, and *what consumes a region-level decision*.
