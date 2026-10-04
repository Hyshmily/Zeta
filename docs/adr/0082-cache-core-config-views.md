# Cache-Core Configuration Views (Cutting the cache→autoconfigure Package Cycle)

The five cache-core classes that read runtime configuration (`HotKeyCache`, `TtlPolicy`, `ExpireManagerImpl`, `CircuitBreakerImpl`, `RefaultAdmission`) imported `autoconfigure.ZetaProperties` directly, making `cache → autoconfigure` the only real package cycle inside `common` (the 2026-10-04 architecture review, finding F1). We cut the cycle with narrow read-only configuration **view interfaces owned by the cache layer** and implemented by `ZetaProperties` at the assembly layer, rather than the constructor-time snapshot the review originally sketched — because verification proved the live-read semantics are documented behavior with a locking test, and a snapshot would have silently broken them.

## Status

accepted, implemented (2026-10-04)

## Context

**The cycle.** `HotKeyCache`, `TtlPolicy`, `RefaultAdmission`, `TtlPolicy`-holder `ExpireManagerImpl`, and `CircuitBreakerImpl` all imported `io.github.hyshmily.zeta.autoconfigure.ZetaProperties` to read `zeta.local.*` values, while the autoconfiguration layer creates and wires those same classes — `cache ⇄ autoconfigure` in the package import graph. Functionally harmless for a single-jar starter, but the reverse edge leaks the physical location of configuration into the orchestration core, and it is the first thing that breaks if the cache core is ever built in a non-Spring host or split module.

**Why the review's snapshot plan was voided (the verification gate).** The plan said: extract the values the cache core consumes into a record, inject at construction — *unless* live-reload semantics turn out to be load-bearing. They are, in three independent ways:

1. **Documented contract.** `TtlPolicy`'s class Javadoc states the underlying configuration "is read on every call (same as the pre-extraction behaviour), so runtime configuration updates keep working".
2. **A locking test.** `ZetaCacheTest.tag_shouldNormalizeKeyWhenStripQueryEnabled` calls `ttlConfig.getCacheKey().setStripQuery(true)` *after* the `HotKeyCache` under test is constructed, then asserts `tag()` honors the new value — a live-reload assertion by construction.
3. **Mixed-granularity reads.** `HotKeyCache` reads the *raw* `getNullValueTtlSeconds()` (`0` → `0 ms`), while `TtlPolicy` reads `effectiveNullTtlMs()` (`0` → `Long.MAX_VALUE`); `CircuitBreakerImpl` reads all eleven nested getters live inside decision methods (`isEnabled()` toggles fast paths at runtime). A snapshot would have had to reproduce per-field liveness decisions — a behavior surface, not a boundary cleanup.

**Full read-point inventory** (verified by grep before the change): `TtlPolicy` — five `effective*TtlMs()` reads; `ExpireManagerImpl` — `getTtlJitterRatio()` at construction, the rest delegated to `TtlPolicy`; `HotKeyCache` — raw `getNullValueTtlSeconds()` and `getCacheKey().isStripQuery()`; `CircuitBreakerImpl` — every `ZetaProperties.CircuitBreaker` getter live; `RefaultAdmission.from` — mode enum (name-mapped), reject TTL, capacity entries, max-size fallback, shadow bits, all construction-time.

## Decision

- **Ports owned by the cache layer, adapter at the assembly layer.** New `@Internal` view interfaces in `cache.cachesupport`, method names mirroring the existing accessors so `ZetaProperties` implements them without logic moves:
  - `CacheCoreSettings` — the TTL family (`effectiveHardTtlMs`, `effectiveSoftTtlMs`, `effectiveHotHardTtlMs`, `effectiveHotSoftTtlMs`, `effectiveNullTtlMs`), the raw `getNullValueTtlSeconds()`, `getTtlJitterRatio()`, and `isStripQuery()` (the one bridge: it aggregates the nested cache-key block). Consumed by `TtlPolicy`, `ExpireManagerImpl`, `HotKeyCache`.
  - `CircuitBreakerSettings` — the eleven breaker getters. Implemented by the nested `ZetaProperties.CircuitBreaker` class; consumed by `CircuitBreakerImpl`.
  - `RefaultAdmission.Settings` (nested in the class it configures) — mode (pre-mapped to the core's own `Mode` enum via the existing `valueOf(name)` translation), reject TTL, capacity entries, shadow bits, max size. Implemented by the nested `ZetaProperties.CacheConfig`; consumed by `RefaultAdmission.from`.
- **Live semantics preserved byte-for-byte.** The views are the live bound bean; every read goes through the same getters as before. No read is snapshotted, no behavior changes, and the locking test keeps passing unmodified.
- **One-way dependency.** All five `import ...autoconfigure.ZetaProperties` lines are gone; the only remaining direction is autoconfigure → cache (which already existed). `grep -r autoconfigure common/src/main/java/.../cache/` returns zero.
- **Constructor signatures widen, callers don't change.** Parameters change from concrete types to the views; every construction site (assembly beans, tests passing `new ZetaProperties()`) keeps compiling because `ZetaProperties` implements the views. Test code required **zero** edits.
- **Not recorded as a formal SPI.** These are internal boundary views (`@Internal`), not user extension points; they exist to express direction, not to invite alternate implementations. A future config snapshot can still be built *on top of* them without re-cutting the boundary.

## Considered Options

- **Constructor-time snapshot record** (the review's original sketch): rejected — voided by the verification gate above (documented contract + locking test + per-field liveness).
- **Move `ZetaProperties` out of `autoconfigure`**: rejected — the class is public API (`zeta.local.*` binding bean); a package move is a binary-breaking change for the published artifact and needs a deprecate-then-move cycle for a boundary-only benefit.
- **Keep the cycle and document it**: rejected — the fix cost (three small interfaces, zero behavior change, zero test churn) is lower than the maintenance cost of the only real cycle in the codebase, and `RefaultAdmission`'s javadoc already carried "autoconfigure-free core" language, showing the direction was pre-intended.
- **Guava-style functional views (`BooleanSupplier` etc. per read)**: rejected — loses self-documentation and per-method javadoc for no gain over a named interface.

## Consequences

1. `cache` no longer imports `autoconfigure`; the package graph of `common` is now acyclic in its main line (the remaining root-facade hub cycle — fluentAPI/annotationsupporter referencing `Zeta` — is a separate, deliberate UX shape, out of scope here).
2. `ZetaProperties` gains three `implements` clauses and two bridge methods (`isStripQuery`, `CacheConfig.mode()`); its binding behavior, validation, and metadata are untouched.
3. Any future reader of the cache core sees the exact configuration surface it consumes, enumerated in one interface — the review's "boundary expressed by the code itself" goal.
4. If the cache core is ever extracted from the Spring host, only the assembly-side implementations move; the core and its tests are already free of the assembly package.
