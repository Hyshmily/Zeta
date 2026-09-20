# Centralized Cache Value Compression via ExpireManager

Cache value compression (`Lz4CacheCompressor`) was previously applied by individual callers at the `HotKeyCache` level. This meant that `CacheSyncListener`, `WorkerListener`, and `ExpireManagerImpl` internal paths stored values uncompressed.

**Decision:** `CacheCompressor` is now injected into `ExpireManagerImpl`. All four `createBuilder()` overloads call `compressor.wrap(value)` before storing the value in the `CacheEntry`. A new method `replaceEntryValue(CacheEntry, Object)` wraps and replaces the value in an existing entry. All callers (`HotKeyCache`, `WorkerListener`, `CacheSyncListener`) now go through these methods, ensuring every value entering a `CacheEntry` is compressed.

**Benefits:**
- Single responsibility: compression is handled at one layer (the CacheEntry factory), not scattered across five files.
- Automatic coverage: new CacheEntry creation paths (null-value sentinels, async refresh, putLocal) no longer need explicit `compressor.wrap()` calls.
- Consistent memory usage: large strings are always compressed in L1 regardless of entry source.

**Trade-off:** `ExpireManagerImpl` is now coupled to `CacheCompressor`, mixing TTL management with compression. This is acceptable because both are L1 cache concerns and `CacheCompressor.NONE` keeps the no-op path free of runtime overhead.

## Amendment 2026-08-29 — Codec hardening

Four refinements to the L1 value format, all covered by `Lz4CacheCompressorTest` / `DefaultWeigherTest`:

1. **Incompressible fallback.** The compressed form is stored only when it is strictly smaller than the flag-prefixed raw form (`len + 5 < raw.length + 1`); otherwise the value is stored raw (`0x00`/`0x03`). Incompressible payloads (already-encrypted / already-compressed values) stop paying per-hit decompression for zero memory benefit — and the compressed form can never exceed the raw one anymore.
2. **Scratch residency cap.** The per-thread compression scratch buffer stops growing at 1 MiB (`SCRATCH_MAX_BYTES`); larger values use a per-call buffer, so one huge value cannot pin its worst-case buffer in every executor thread for the process lifetime.
3. **Unknown codec flag → `IOException`.** `unwrap` no longer returns an unrecognized-flag byte[] verbatim. The compressed format exists only inside this JVM's L1 — Redis L2 stores caller-serialized values (`StringRedisTemplate`), `SyncMessage` carries keys/versions but never values, and every L1 write funnels through `ExpireManager.createBuilder` — so an unknown flag can only be corruption, never a foreign format or a rolling-upgrade artifact. Throwing routes the entry into the existing invalidate-and-reload path (`HotKeyCache.unwrapValue`), which heals it in one read; the previous behavior silently leaked a wrongly-typed (and flag-prefixed, i.e. not even correct-as-bytes) value on every hit.
4. **Weigher graph-walk budget.** `DefaultWeigher`'s jol `GraphLayout` deep-measurement (an unbounded reflection walk on the miss path; a value holding an application context or class loader would stall every write) is replaced by a cycle-safe, node-budgeted reference-graph walk (4096 nodes; over-budget graphs priced at 2× the walked size — conservative, since an over-estimate costs an early eviction while an under-estimate costs unbounded memory). The now-unused jol-core dependency is removed from `common/pom.xml`.

**Deliberately not changed:** the decompression length-header bound stays at 100 MB (`MAX_DECOMPRESSED_BYTES`). It guards against a corrupt header allocating huge memory before LZ4 validates a byte, but tightening it — or gating it on the currently-unenforced `zeta.local.cache.max-value-size` — would turn large-but-legitimate values into permanent invalidate → reload → fail-again loops. With refinement 3 in place, corruption already self-heals in one read, so the bound remains the generous third line of defense.
