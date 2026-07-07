# Centralized Cache Value Compression via ExpireManager

Cache value compression (`Lz4CacheCompressor`) was previously applied by individual callers at the `HotKeyCache` level. This meant that `CacheSyncListener`, `WorkerListener`, and `ExpireManagerImpl` internal paths stored values uncompressed.

**Decision:** `CacheCompressor` is now injected into `ExpireManagerImpl`. All four `createBuilder()` overloads call `compressor.wrap(value)` before storing the value in the `CacheEntry`. A new method `replaceEntryValue(CacheEntry, Object)` wraps and replaces the value in an existing entry. All callers (`HotKeyCache`, `WorkerListener`, `CacheSyncListener`) now go through these methods, ensuring every value entering a `CacheEntry` is compressed.

**Benefits:**
- Single responsibility: compression is handled at one layer (the CacheEntry factory), not scattered across five files.
- Automatic coverage: new CacheEntry creation paths (null-value sentinels, async refresh, putLocal) no longer need explicit `compressor.wrap()` calls.
- Consistent memory usage: large strings are always compressed in L1 regardless of entry source.

**Trade-off:** `ExpireManagerImpl` is now coupled to `CacheCompressor`, mixing TTL management with compression. This is acceptable because both are L1 cache concerns and `CacheCompressor.NONE` keeps the no-op path free of runtime overhead.
