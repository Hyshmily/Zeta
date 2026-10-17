# Value-Carrying PUT Broadcast

## Context

Every `putThrough` / `refresh(broadcastToPeers=true)` write broadcast a
version-only `REFRESH`: the AMQP body was the bare cache key, and every
receiving instance had to resolve the value for itself via
`loadAuthoritative` — one reload per peer per write (O(N) read amplification
against the data source). On the default deployment there is no Redis value
channel, so that reload returns nothing and the receiver degraded to a local
invalidation (ADR-0031): the next read was a guaranteed miss, re-loading
through the application reader. The effective convergence path for a peer was
"REFRESH → invalidate → miss → reader reload", the write's propagation
latency was bounded below by the sync buffer's 500ms flush window, and peers
whose loaders are non-deterministic (timestamps, jitter) converged to
*different* values.

## Status

Decided (2026-10-12). Implemented: `SyncMessage.TYPE_PUT`,
`CacheSyncPublisher.serializePutPayload/parsePutPayload/broadcastPut`,
`CentralDispatcher.sendPut`, `BroadcastBuffer.recordPut`, payload typing, and
the receiver `DefaultSyncDecisionHandler.handlePut`.

## Decision

1. **One buffer, one delay budget.** PUT travels through the same
   `BroadcastBuffer` as REFRESH — same-key writes still merge to the newest
   version, the flush still hands the batch to the dedicated send executor
   (ADR-0037). No separate direct-send path: a short-lived design that
   bypassed the buffer was reverted, because the merge window (and the
   executor isolation it feeds) remains the only thing keeping a hot-key
   rewrite storm from becoming a 1:1 message storm and a write-thread stall.
   The buffer's `VersionInfo` now carries a serialized payload alongside the
   version; the merge is unchanged (newest version wins, payload travels with
   its version), and a total-pending-payload-bytes cap (default 64MB) forces
   a flush the same way the entry-count cap does, so buffered values stay
   bounded under write bursts.
2. **Latency is the flush window.** `zeta.sync.flush-delay-ms` drops 500 →
   **50** and `zeta.sync.max-defer-ms` 2000 → **500**: the window IS the
   sync plane's propagation latency, and with per-key merge still in place a
   small window keeps storm protection while making peers see writes in tens
   of milliseconds.
3. **Writer-side snapshot, typed round-trip.** `serializePutPayload` runs on
   the writer thread (the transaction-commit thread; pure CPU, no network
   I/O — ADR-0037 forbids only the broker call there) and freezes the value
   into immutable bytes, so later mutation of the caller's object cannot
   change what peers receive. The payload mapper uses Jackson default typing
   with `EVERYTHING` + `WRAPPER_ARRAY`: final types (`Long`, `BigDecimal`,
   `byte[]`) carry their type marker too — with the default non-final typing
   a `Long` would round-trip as an `Integer` and a business read would fail
   with `ClassCastException`.
4. **Size bound with graceful degradation.** A payload over
   `zeta.sync.put-max-bytes` (default 32KB), or a value Jackson cannot
   serialize, degrades to a value-less REFRESH in the same buffer — those
   keys keep the old reload path, whose loader round-trip preserves type
   fidelity that direct delivery could not have provided anyway.
5. **Wire shape.** The PUT body is `{"key":...,"value":<typed value>}`; the
   real cache key also rides in the new `cacheKey` AMQP header. Receivers use
   the header as the ordered-dispatch key (so a key's PUT/REFRESH/INVALIDATE
   share one per-key worker), filter their own PUTs like self-REFRESH
   (ADR-0067), and `SyncMessage.from` treats PUT as a payload type (no
   bare-key validity check).
6. **Receiver semantics mirror REFRESH.** `handlePut` applies the carried
   value under the same strict-newer guard the REFRESH receiver uses — an
   **equal** version applies (ADR-0066; the writer is authoritative at the
   version it allocated, which also heals ADR-0033's probe-after-read
   over-stamp) — plus the invalidation watermark check (an older in-flight
   PUT cannot re-warm a key an INVALIDATE removed). The apply is
   refresh-in-place: value + dataVersion, TTLs and Worker decision metadata
   preserved, expiry re-armed; an absent key gets a fresh default-TTL NORMAL
   entry. No batch-tail skip: a PUT apply is a pure in-memory compute with no
   I/O to amortize. A key that parses with an unreadable value (e.g. a
   rolling upgrade removed the value's class) degrades to a versioned
   INVALIDATE — same guard, self-heals on the next read.

## Considered Options

- **Delete the buffer, send directly** (briefly adopted, then reverted).
  Rejected: without the merge window a hot-key rewrite storm sends one AMQP
  message per write, and without the buffer's swap-then-send shape the send
  either blocks the transaction-commit thread (the exact failure ADR-0037
  removed) or pays one executor task per write with no bounding.
- **Broadcast the whole `CacheEntry`.** Rejected: the receiver's local entry
  may be Worker-managed (HOT/COOL) with `decisionVersion`/`decisionEpoch` and
  extended TTLs (ADR-0021/0024 depend on them surviving); a full overwrite
  would discard decision metadata. Only value + dataVersion are applied.
- **Non-final default typing (`As.PROPERTY`).** Rejected: final scalar types
  lose their type marker and drift on deserialization (`Long` → `Integer`).
- **Trust-the-writer without size bound.** Rejected: unbounded values would
  dominate AMQP frame sizes, the sync dispatcher's KB-weighted pending
  budget, and the buffer's memory footprint.

## Consequences

1. One write costs one buffered, merged, value-carrying message instead of
   N−1 peer reloads; peers converge to the writer's value (single writer
   authority) within ~50ms instead of ≥500ms plus a reload.
2. Broker traffic grows by the serialized value size for every broadcast
   write within the 32KB bound; larger values keep the REFRESH path.
3. The default-typing mapper accepts the application's own class surface —
   the sync plane is a same-application fanout (foreign-app messages are
   dropped by the `appName` filter, ADR-0068), so the trust boundary equals
   the application's own broker trust.
4. Observed counters are unchanged (`zeta.stall.broadcast_storm.stopped.total`
   still attributes send failures and saturation drops); put/refresh share
   the flush WARN.
5. Values intended for broadcast must be Jackson round-trippable under
   default typing; anything else silently degrades to REFRESH (debug-log
   visible), which is the documented contract rather than an error.
