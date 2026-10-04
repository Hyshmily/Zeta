# Dependency Footprint of the Published Starter (guava / lucene-core / bucket4j)

`common/` is published to Maven Central, so every compile-scope dependency is a footprint and version-conflict surface carried by all downstream applications. This ADR audits the three heavy non-optional dependencies, records which are justified, which are removed (guava), and which are kept with an explicit re-calibration gate (lucene-core), so the "why is this dependency here" question has a documented answer that outlives the review that raised it.

## Status

accepted, partially implemented (2026-10-04: guava removed from common compile scope; lucene-core and bucket4j kept with documented constraints)

## Context

The 2026-10-04 architecture review (§F2) measured the actual usage of the three heavy compile-scope dependencies in `common/pom.xml`:

| Dependency | Declared | Actual usage (verified by grep) | Used by |
|---|---|---|---|
| `com.google.guava:guava:32.1.3-jre` | compile (non-optional) | 2 call sites, hash functions only | `HeavyKeeper.fingerprint` (`Hashing.murmur3_128()` — sketch slot fingerprint, process-local only, never on the wire), `ConsistentHashRing.hash` (`Hashing.murmur3_32_fixed()` — shard routing) |
| `org.apache.lucene:lucene-core:9.12.0` (pinned over the BOM-managed 10.5.0) | compile (non-optional) | 1 class | `DefaultWeigher` (`RamUsageEstimator` deep size measurement, ADR-0015 amendment + ADR-0069 calibration) |
| `com.bucket4j:bucket4j_jdk17-core:8.19.0` | compile (non-optional) | 1 class | `CacheExtensionAspect` (`Bandwidth`/`Bucket` for `@Intercept` rate limiting) |

Plus one indirect consumer: `worker/` (unpublished) imports `com.google.common.util.concurrent.Striped` in `ZetaBayesianSM` for per-key locks, inheriting guava transitively through `zeta`.

The risks of the status quo are: (a) ~6 MB of transitive jar weight per downstream application; (b) guava and lucene are the two most conflict-prone third-party libraries in the wider Java ecosystem (Lucene-ecosystem and Elasticsearch-client applications especially), and Zeta participates in Maven nearest-wins mediation with a **pinned** lucene version that diverges from the BOM it ships under; (c) any future replacement of `RamUsageEstimator` invalidates the ADR-0069 weigher calibration evidence chain unless the replacement is behavior-identical.

## Decision

- **guava: removed from `common` compile scope.** Both hash call sites move to vendored, bit-identical implementations in `util/FastMath` (following the ADR-0073 consolidation precedent that merged `FastRangeUtil` into `FastMath`): `murmur3_32Fixed` for the ring (canonical MurmurHash3_x86_32 — guava's `_fixed` variant is the canonical algorithm, its zero-padded tail included) and `murmur3_128` (lower 64-bit half) for the HeavyKeeper fingerprint. Bit-identity with guava is a **hard requirement**, not a nicety: `ConsistentHashRing` positions must agree across a mixed-version rolling upgrade (old Zeta hashing with guava, new Zeta hashing with the vendor). Proof is a differential corpus test that keeps guava at **test scope** permanently and asserts equality over ASCII/UTF-8/multibyte/emoji/length-sweep corpora plus hardcoded golden vectors, so the guard survives even if guava later leaves the test classpath. `worker/` declares guava directly (`Striped` stays — the alternative touches the 1342-line state machine for zero published-artifact benefit; worker is not published). Consequence: downstream starters stop carrying guava entirely; ADR-0005's "pinned hash" invariant survives because the hash values do not change.
- **lucene-core: kept, pinned at 9.12.0, with a re-calibration gate.** `RamUsageEstimator` is the measured foundation of the ADR-0069 weigher calibration (sampled-container pricing, 4096-node walk budget, per-entry overhead constants); replacing or upgrading it silently invalidates that evidence. The pin over the BOM's 10.5.0 is deliberate until a re-calibration run (`DefaultWeigherBenchmark` + the ADR-0069 test chain) is executed on the target version. Any proposal to vendor a size estimator or bump the pin must re-run that chain first — this is the "measurement evidence first" rule applied to dependencies.
- **bucket4j: kept.** One call site, a real user-facing feature (`@Intercept` rate limiting, ADR-0023 three-layer model), JDK17-native line, and the replacement cost (a hand-rolled token bucket with the same semantics) buys ~0.4 MB of footprint relief — not worth the new code to own. Recorded here so the choice is conscious and revisitable.

## Considered Options

- **Vendor a `RamUsageEstimator` replacement to drop lucene-core**: rejected for now — the estimator's object-graph walk semantics (ref counting, nested arrays, cycle handling) are exactly what ADR-0069 calibrated against; a reimplementation is a new evidence chain, not a refactor. Revisit only if lucene version conflicts are reported by real downstream users.
- **Make lucene-core/bucket4j `<optional>` with graceful degradation**: rejected — `DefaultWeigher` is on the L1 write path whenever `max-weight > 0`, and the aspect rate limiter on the `@Intercept` path; optional-izing would move a startup-visible configuration into a runtime classpath surprise for exactly the users who enabled the feature.
- **Drop guava by keeping it optional**: rejected — an optional dependency that one code path needs is a `NoClassDefFoundError` in production for users who exclude it; hash functions are either always available or the code does not compile.
- **Vendor guava's `Striped` into worker too**: rejected — `Striped` is used inside `ZetaBayesianSM`'s per-key locking; touching it violates the review's "don't touch measured-complexity cores without evidence" rule for zero downstream benefit (worker is never published).

## Consequences

1. Downstream applications no longer receive guava transitively from `zeta`; the differential test is the permanent equivalence guard.
2. `worker` gains a direct guava declaration — its artifact changes nothing (it already shipped with guava transitively).
3. The lucene pin divergence (9.12.0 vs BOM 10.5.0) is now a documented, deliberate state with an explicit upgrade procedure, not an accident.
4. Future hash-function or size-estimator changes must reference this ADR and re-run the respective evidence chains (ADR-0069 calibration / ADR-0005 hash stability).
