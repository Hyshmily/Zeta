# L1 Slot Type Closure

The L1 Caffeine cache was typed `Cache<String, Object>` with slots holding either a
`CacheEntry` or a bare user value (a leftover from before value wrapping was
introduced), forcing 47 `instanceof CacheEntry` discriminations across the read,
write, sync, and refresh paths. We closed the slot type to
`Cache<String, CacheEntry>`: every L1 read sees a `CacheEntry` or nothing, the 47
discriminations are deleted, and a bare write is unrepresentable (the compiler
rejects it). Shipped as a breaking change with explicit waiver: self-supplied
`Cache` beans and `ZetaCacheCustomizer` implementations follow the new generic.

## Status

implemented (2026-10-06). `HotKeyCache`, `LocalPromotion`, `EntryLifecycle`,
`VersionGuard`, `RefaultAdmission`, both decision handlers, the background
refresher, and all autoconfigure wiring carry `Cache<String, CacheEntry>`;
`unwrapForStats`/`unwrapRawValue` lost their bare-value branches;
`EntryLifecycle.decisionOf` takes a `CacheEntry`.

## Considered Options

- **Keep `Object` + centralize unwrapping in one helper.** Rejected: the
  discriminations survive, only relocated — the deletion test fails (deleting the
  helper resurrects complexity in N callers instead of vanishing it).
- **Sealed `L1Slot(CacheEntry|Raw)` union.** Rejected: a single real shape needs
  no polymorphism; the union adds an allocation per read for zero modeling gain.

## Consequences

1. Breaking: custom `Cache` beans change generic; `ZetaCacheCustomizer.customize`
   takes `Caffeine<String, CacheEntry>`. The `hotLocalCache` bean builder stays
   `Caffeine<Object, Object>` until the typed `expireAfter` pins it (Caffeine's
   narrowing setters mutate in place — no setting is lost).
2. Tests seed L1 with `CacheEntry` only; the three bare-value tolerance tests
   were rewritten as NORMAL-entry equivalents.
3. `ZetaEndpoint`'s `heavyKeeperConfig` keeps its `instanceof HeavyKeeper`: a
   genuine implementation dispatch, unrelated to slot typing.
