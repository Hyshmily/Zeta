# Read/Write/Invalidate Policy Split

`CachePolicy` is one 8-knob object (`hardTtlMs`/`softTtlMs`, `reader`,
`nullCaching`, `stalePolicy`, `reportEnabled`, `failOnError`,
`skipBroadcast`) shared by three orthogonal call families — reads, writes,
and invalidations. Each family honors a different subset and silently ignores
the rest, so every method's javadoc carries a bespoke "Honored / Ignored"
matrix, and callers who set a non-honored knob believe it applies when it
does not. This has already produced real bugs: `ZetaWriteCommand` grew
`assertNoUnusedTtl` purely to fail fast on silently-dropped TTLs — the patch
is the evidence that the shape is wrong.

## Status

implemented (2026-10-06). Shipped atomically in the breaking turn:
`model.ReadPolicy` / `model.WritePolicy` / `model.InvalidatePolicy` replace
`CachePolicy` (deleted) across the facade, engine, annotation layer, registry
specs and fluent API; batch engine paths stay on resolved primitives (a single
policy cannot express per-key readers without reintroducing an ignored field —
see the batch note in `HotKeyCache.batchGet`); `ZetaCacheContext` transports a
`Snapshot(ReadPolicy, skipBroadcast)` record; the CAS quartet and the
`Item`-returning observers are deleted in the same turn; `getLocalCache` is
replaced by `snapshotValues` / `localKeysWithPrefix` (fixing the sentinel-NPE
in the old collector-based snapshot). Originally proposed 2026-10-05 as a
design-only record pending the 2.0 window; executed with explicit waiver of
compatibility constraints.

## Decision

Split `CachePolicy` into three closed records along the read/write/delete
axes, each carrying exactly the knobs its family honors:

- **`ReadPolicy`** `{reader, hardTtlMs, softTtlMs, nullCaching, stalePolicy,
  failOnError, reportEnabled}` — `get`/`getWithSoftExpire`/`computeIfAbsent`
  families, batch variants, `peekAndTag`, `registerRefresh` ticks.
- **`WritePolicy`** `{hardTtlMs, softTtlMs, skipBroadcast}` —
  `putThrough`/`putLocal`/`putIfAbsent`-style inserts.
- **`InvalidatePolicy`** `{skipBroadcast}` — `invalidate`/`invalidateAll`/
  `invalidateAfterPut` families.

Records, not builders: the knob sets are small, fixed, and fully validated at
construction. Passing a read knob to a write becomes a compile error instead
of a silent no-op. `CachePolicy` is deleted in the same turn (no deprecation
bridge — see Status: executed atomically with explicit waiver of compatibility
constraints).

Migration sketch per family (all mechanical):

| Today | 2.0 |
|---|---|
| `get(k, CachePolicy.of(reader, hard, soft, …))` | `get(k, ReadPolicy.of(reader).withTtl(hard, soft)…)` |
| `putThrough(k, v, w, CachePolicy.of(hard, soft))` | `putThrough(k, v, w, WritePolicy.of(hard, soft))` |
| `invalidate(k, CachePolicy…withSkipBroadcast(b))` | `invalidate(k, InvalidatePolicy…)` |
| `ZetaReadQuery` (reader + fallback chain + TTL) | thins to `ReadPolicy` sugar; the fallback loop moves to caller composition |
| `ZetaWriteCommand` (already fail-fast on unused TTLs) | thins to `WritePolicy` sugar; `assertNoUnusedTtl` deleted with its cause |

## Considered Options

- **Keep one policy + runtime validation (fail-fast on ignored knobs).**
  Rejected: the `assertNoUnusedTtl` precedent shows this is whack-a-mole —
  every new method needs its own assertion table, and the table duplicates
  the honored/ignored matrix instead of removing it. Validation also fires at
  runtime; the record split fires at compile time.
- **One builder per family (six builders total with the spec builder).**
  Rejected: builders pay off for large optional-knob spaces; each family has
  1–7 knobs with no conditional validation between them. Records are
  smaller, immutable, and self-documenting.
- **Split incrementally (new overloads beside the old ones).** Rejected:
  two policy systems coexisting doubles the honored/ignored documentation
  and the conversion code at every seam. The split ships atomically in the
  breaking release, with the old type deprecated one minor before.

## Consequences

1. Breaking: every `CachePolicy`-taking overload changes signature. As recorded
   in Status, the split shipped atomically with explicit waiver (no 1.1.x
   bridge period); consumers migrate per the table above.
2. The per-method "Honored / Ignored" javadoc sections disappear — a policy
   type that cannot express an inapplicable knob needs no such section.
3. `ZetaLoadingSpec.toPolicy` becomes `toReadPolicy`, the single spec→policy
   exit; `CachePolicy.of(reader, …)` dies with the old type.
4. CAS removal (deprecated 1.1.58 for local-only coherence bypass) and the
   `Item`/`getLocalCache` removals (deprecation windows served) land in the
   same major, so 2.0 carries the full API cleanup in one migration.
