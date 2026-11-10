# Design Decisions

Why Zeta is built the way it is: the invariants each mechanism protects, the trade-off that
justifies it, and the class that owns it. Read the relevant topic before changing a mechanism —
most entries exist because the obvious alternative was tried and produced a specific failure.

This file replaces the former `docs/adr/` directory. A decision that was later reversed keeps its
failure record with the current mechanism — what the old shape did, the symptom it produced, and
why the new shape closes it — and the rejected alternative is recorded once under
[Ruled out](#ruled-out) with the reason it lost, so the same idea is not proposed twice.

**How code refers to it.** Comments in the source point at a topic instead of restating the
rationale (`design decisions: version comparison`). A comment should state the local contract —
what the method guarantees, what a caller must not do. The _why_ belongs here.

**Every decision records the bug it prevents.** An entry states the current mechanism, the concrete
failure an omission or a previous shape produced, why the current shape closes it, and — when a
change reverses a shipped shape — how the previous shape worked. "For robustness" is not a reason;
the failure is the observable symptom (a stuck hot key, a drain thread burning a core, a demotion
that reaches the wire after the promotion it belonged to). A source comment stays in the present
tense; this file carries the failure history and the before/after.

**Where a decision is documented.** Two tiers, used in this order:

1. **Local comment** — the default. A contract that only the neighbouring code needs stays beside it.
2. **This file** — a decision that spans mechanisms, changes a guarantee, or would be re-litigated
   without its rationale.

The former third tier (one topic document per component under `docs/design/`) is folded in here:
[counting engine](#counting-engine-wavecounter) and [Worker
cooling](#worker-cooling) are long sections, but they are still one component's reasoning, and
splitting them across files left two places to forget. A section may be long; what it must not be is
a second document with its own conventions.

**Conventions.** "App" is an application instance embedding `zeta` (L1 Caffeine + local TopK +
reporter). "Worker" is the standalone `zeta-worker` cluster. "L1" is the single Caffeine table
owned by one App instance; there is no L2. _Hot_ paths run once per served read; _cold_ paths run
per write, per broadcast or per administrative call.

---

## System shape

**The read path never waits for the cluster.** Detection, reporting and sync are
fire-and-forget. A read touches L1, appends to one buffered counter, and at most one loader
call. Worker availability changes TTL policy, never the correctness of a read. This is the
property every other decision is measured against.

**The Worker is the only authority that can cool a key down.** The App may promote on its own
— a provisional speed-up — but only the Worker may demote, through a COOL decision or by
dying (Decision Validity). Otherwise a local heuristic could silently override a global one.

**Every cross-instance guarantee is eventually consistent and bounded by TTL.** No consensus,
no publisher confirms, no cross-instance dedup state. Each mechanism states its own
inconsistency window and backstop; none of them claims more than it delivers.

**Two modules, one dependency direction.** `common` (`zeta`) is the starter: facade, cache
engine, detection sketch, sync, reporter, annotations, actuator. `worker` is a
standalone Spring Boot app: ingest, sliding window, rule resolution, decision broadcast,
heartbeat. `benchmark` is a JMH harness that is never published and never runs in
CI; its AMQP dependency is compile-only, because `DropPublisher` extends `ReportPublisher`, whose
constructor exposes `RabbitTemplate`, and it never opens a broker connection. `common` never
imports `worker`. The Worker depends on `common` for the shared model
(`CacheEntry`, `ZetaDecision`, `VersionGuard`, message types), not for the cache engine. Every
contract that must be visible to both sides lives in `common`; the Worker holds its implementation.

**The dependency set is the product.** Caffeine, Spring, Jackson, AMQP (optional).
Redis, an external registry, a distributed lock and a rule store were all removed; nothing in
the data plane needs a second stateful service.

---

## Configuration surface

**Spring metadata is hand-maintained and must match the properties the code reads.** Each module's
`META-INF/additional-spring-configuration-metadata.json` is the IDE-facing contract for properties
Boot cannot infer from a plain getter/setter: name, type and default must agree with the
`@ConfigurationProperties` field — or, for a condition-only gate, the `@ConditionalOnProperty`
annotation — and with `docs/CONFIG.md` / `docs/CONFIG.zh.md`. A live property missing from the JSON
is invisible to completion; an entry left behind by a removed property is a phantom knob, which is
why a field no code reads is deleted rather than documented. The 2026-10 cleanup deleted entries for
properties that no longer exist (Redis, lock, SRE, `shard-count`) and added the missing
`detect-interval-ms`, `node-id`, `put-allowed-packages`, `max-tracked-keys` and `epoch-file` entries.
Owner: `ZetaProperties`, `WorkerProperties`, `CacheSyncProperties`, `WorkerListenerProperties`.

---

## Cache table

**One table; the annotation layer encodes namespaces into the physical key.** A key is stored and
addressed exactly as the caller wrote it on the programmatic facade, which has no notion of a
namespace. The annotation verbs do have one — `cache` is required — and the interceptor encodes
it: `cache + '\u0000' + key`.

**Why the annotations namespace.** Two cache names may hold the same logical key without seeing
each other's value. Sharing the raw key would make
`@ZetaCached(cache = "users") User get(Long id)` and
`@ZetaCached(cache = "orders") Order get(Long id)` collide on `"1"`, so one method could serve
the other's value — a runtime type error that TTL convergence turns into a permanent flip-flop
instead of a visible failure. Telling callers to hand-prefix
(`key = "#name + ':' + #id"`) pushes a silent-corruption mode onto every user.

**What it costs, and what it does not change.** One concatenation per annotated operation, which
also defeats the access counter's same-key-instance fast path for that call; the programmatic
facade pays nothing. The table stays single — one size bound, one sweep, one `INVALIDATE_ALL` —
so `zeta.local.cache.max-size` bounds the union. `clearAll(options)` is a table flush; the
engine's scoped clear (`HotKeyCache.clearPrefix`, reachable from `Zeta.clearPrefix` /
`Zeta.clearNamespace` and from an `INVALIDATE_PREFIX` received on the sync plane) sweeps one
namespace and broadcasts `INVALIDATE_PREFIX` instead of `INVALIDATE_ALL`. No annotation verb exposes an all-entries form:
it would need a facade method the three verbs deliberately do not have, and the verb set stays
the annotation API surface. `unsafe().raw()` shows the encoded keys.

**Scoped clears need a scoped watermark.** A full clear advances one table-wide stamp and every
pre-clear write loses to it; reusing that for a namespace clear would refuse in-flight fills of
_all_ namespaces — the over-eviction the scope exists to prevent. `EntryLifecycle` therefore also
keeps a small copy-on-write snapshot of `prefix -> clearStamp` **plus the highest stamp ever
recorded**. The max is what keeps the write path cheap once a namespace clear has aged past the
in-flight window: a write above it cannot match any live prefix, so one volatile read and one long
compare replace the walk — permanently, not only while the map happens to be empty. Only a version
low enough to match a live prefix pays the short `startsWith` walk. The map stays honest about what
is live: a stamp the table-wide watermark covers is dropped on the way in and pruned when a clear
advances past it (the guard tests the table-wide watermark first, so such an entry can never match
again), at the 64-entry cap the older entries first shed the ones older than the tombstone TTL —
except a stamp the recording call itself adds or raises, which is that call's own in-flight window
and whose wall clock may lag a peer's — and only if the cap is still exceeded does the whole live
set fold into the table-wide watermark at the _highest_ stamp involved and the map reset — folding
the max is what lets the reset keep every evicted prefix's protection, and refusing more, never
less, stays the safe direction. Pruning keeps
the max (it gates only whether the walk runs, never whether a write is refused), so the fast path
survives the clear that removed the last prefix. The guard consults both levels, so the effective
watermark for a key is the higher of the table-wide stamp and the stamp of any prefix it matches:
a write has to outrank both to land. Neither the snapshot nor a per-prefix lookup is part of the
`EntryLifecycle` surface — `recordPrefixClear`/`recordPrefixClears` and `staleAgainstInvalidation`
are the whole contract, which leaves the encoding of the watermarks free to change.

**A clear is best-effort termination, not a barrier.** The sweep removes what the table's weakly
consistent key-set iteration yields. A fill whose write guard already passed and whose install lands
after the iterator has traversed its bin is never seen, so it survives until its own hard TTL — the
same convergence bound a peer that missed the broadcast has. The anti-resurrection guarantee is the
watermark, not the sweep: the watermark refuses every write that starts after it, and the sweep only
reclaims what the iteration can see. Closing the window would need a lock on every write, paid on the
hot path for a stale entry that already dies at its TTL. Owner: `HotKeyCache.sweep`.

**The prefix is validated at the engine entry, not inside the watermark table.** `clearPrefix`
rejects a null, empty or blank prefix. A null one would otherwise degrade into the shared table-wide
clear; an empty one matches every key while the watermark table records nothing and the publisher
drops the broadcast, so it is a full sweep with no anti-resurrection guarantee; a blank one is
recorded locally but dropped by every peer's wire validation, leaving the two planes disagreeing
about whether a clear happened. Owner: `HotKeyCache.clearPrefix`.

Ruled out: a table-wide flush for a namespace clear. It is wrong for the same reason the
namespaces exist — clearing `users` would evict every `orders` entry and, worse, advance a
watermark that blocks their fills. The single-table argument only ever justified one _storage_
structure, not one eviction scope.

Boundary: `HotKeyCache.table()`, `HotKeyCache.clearPrefix`, the annotation namespace encoding,
`EntryLifecycle.recordPrefixClear`/`recordPrefixClears`.

---

## Entry model

**Entries are immutable, and `EntryDraft` is the only construction path.** `CacheEntry` has no
builder and no setters: the constructor is package-private for the draft, and every creation or
mutation goes through `newEntry()`/`editEntry()`/`of()`. The entry carries the value plus
everything the TTL regime needs: `dataVersion`, one nullable `DecisionStamp` (decision version,
Worker node ID, epoch and decision kind), `keyState`, four TTL durations (hard/soft,
current/normal baseline), two absolute expire timestamps and the value-birth second.
`EntryLifecycle.newEntry()` and
`editEntry(entry)` supply the TTL arithmetic, so no caller can build an entry whose timestamps
disagree with its durations. Collaborators that need the arithmetic take it from the lifecycle
once at construction (`TtlPolicy` is stateless), instead of reaching through
`entryLifecycle.ttlPolicy()` on every call.

**Effective heat and the Worker lease are orthogonal.** `keyState` answers only which TTL regime is
active. The nullable `DecisionStamp` is the Worker lease: version, node ID, epoch and whether the
last decision was HOT or COOL. A COOL lease can remain on an entry whose effective state is NORMAL
(a completed cool-down) or HOT (a degraded-mode local promotion). `isWorkerManaged()` is therefore true for a COOL entry carrying a lease and for a HOT entry whose
carrier is a HOT decision — not for any HOT/COOL state; `isElevated()` alone selects the TTL regime
for a value write, and `isLocalOverrideHot()` marks the one shape that recovers when the health gate
returns.

**One decision reference, one packed metadata word.** A Worker decision is one nullable
`DecisionStamp` — version, node ID, full-width epoch and kind — instead of three fields that are
zero on every local entry. The 2-bit state, the 30-bit birth second and the 32-bit hard TTL share one
`long`; the other three TTLs stay packed `int`s. Nine fields total 64 bytes with compressed class
pointers (12-byte header, four `long`s, three `int`s, two references), so a million resident entries
carry 16 MB less than the 80-byte shape and the object's payload is one 64-byte line's worth of
bytes rather than two lines' worth. The stamp is
immutable and stored by reference: `EntryDraft.of(entry)` carries it without copying, and a
Worker-managed entry reuses the record the decision handler built. A local entry pays four bytes of
`null`; a Worker-managed entry pays one 32-byte record, bounded by the TopK set rather than by the
table. The epoch left the packed word with the version and node ID, so it is a full `long` again and
the old 56-bit range check is gone.

**Packing is load-bearing.** The metadata word keeps the key-state code in the low two bits for a
zero-shift hot-path decode, the value-birth second in the next 30 bits (≈ 34 years of monotonic
uptime before the lease-staleness bound would see a wrapped stamp) and the encoded hard TTL in the
high 32 bits. A TTL duration is one `int`: bit 31 selects seconds, bits [0, 31) carry the magnitude.
Values up to 2^31−1 ms (≈ 24.8 days) round-trip exactly; longer ones are stored in whole seconds
rounded **up**, so the decoded duration never falls below the request and
`encode(decode(x)) == encode(x)` — a no-op `EntryDraft.of(entry).build()` must not change the packed
fields that `equals`/`hashCode` compare. `Long.MAX_VALUE` (permanent) is a dedicated sentinel that
no real seconds encoding can collide with (its magnitude is non-zero). A hard duration of `0` means
permanent in the draft contract, and every hard-TTL setter (`ttl`/`ttlDurations`) stores the
explicit `Long.MAX_VALUE` sentinel instead of leaving the zero for the entry to decode: a stored
zero reads as the smallest possible TTL to every consumer that scales from the duration (the
refresh-failure lease floor is the live one) while the entry's deadline is permanent. Only
`normalHardTtlMs == 0` keeps its "unset baseline" meaning, so a later transition still resolves it
to the configured default. The absolute timestamps stay full `long`s and are the actual enforcement
point — durations are nominal values used when rebuilding an entry after a state transition. The
birth second is monotonic seconds-since-boot, not wall time: it bounds lease staleness and must not
move when NTP steps the clock. Its source clamps to `>= 1`, because `0` is the "unknown" sentinel:
a value loaded during the JVM's first second must still be eligible for the staleness cap.

**One atomic mutation primitive.** `EntryMutations.apply` owns the "judge the resident, rebuild or
remove it, report what the bin lock saw" shape for every in-place rewrite: Decision-Validity
demotion, local promotion, the sync PUT/REFRESH apply and the versioned invalidate. The mutation
returns the input instance for "no change", a new instance for a rewrite, or `null` for a removal;
the call always returns the pre/post images it observed inside the compute — the pair is
identical for a no-op — so no caller carries a mutable `boolean[]`/`CacheEntry[]` out of the
compute and no caller has to infer "unchanged" from a null return. The mutation runs on an absent
key too, which is what lets an invalidate record its tombstone in the same compute as the
removal. The read-path reconciliation serves the reported `current` directly: a concurrent
invalidate or eviction is then a miss, never the snapshot read before the lock (the previous
"null means unchanged" return could not tell those two apart and served a removed entry).

**`null` is never a cached value and never a cache hit.** No null sentinel, no negative caching,
`put(key, null)` throws, and a loader returning `null` writes no entry. Caching absence would
conflate "absent" with "not yet loaded", force every reader to distinguish two empties, and
interact badly with admission and versioning.

Owner: `CacheEntry`, `EntryDraft`, `EntryMutations`, `EntryLifecycle`.

---

## TTL model

**Two tiers (hard/soft) × two regimes (normal/hot).**

| State  | Hard TTL (eviction)     | Soft TTL (stale-while-revalidate) |
| ------ | ----------------------- | --------------------------------- |
| NORMAL | `hard-ttl-ms` (5 min)   | `soft-ttl-ms` (30 s)              |
| HOT    | `hot-hard-ttl-ms` (1 h) | `hot-soft-ttl-ms` (5 min)         |

**One field per TTL knob.** Spring Boot's binder overlays a configured value onto the field's
initializer, so a second `default-*` field plus an `effective*()` branch adds nothing but makes an
explicit `0` indistinguishable from "unset". For soft TTL that was a real defect: `soft-ttl-ms=0`
(disable soft expiry) fell back to the 30 s default. `hard-ttl-ms`, `soft-ttl-ms`,
`hot-hard-ttl-ms` and `hot-soft-ttl-ms` now carry both the default and the configured value; a
per-call/per-method `0` still means "use the configured value" in `TtlPolicy`. At configuration
level `hard-ttl-ms=0` means permanent (no hard expiry) and `soft-ttl-ms=0` disables soft expiry.
Owner: `ZetaProperties`, `TtlPolicy`.

**`Long.MAX_VALUE` means permanent.** Pure logical expiry is expressed as a permanent hard TTL,
which is a supported Caffeine `Expiry` value.

**The miss path resolves a per-call `0` before the entry draft.** `HotKeyCache.loadAndCache` resolves both
overrides through `TtlPolicy` before `loadCacheEntry` builds the entry, so `GetPolicy.of(0, 0, …)`
stores the configured values. Passing the raw `0` down would reach `EntryDraft`, where a zero
duration means permanent — a load with no explicit TTL would never expire. Owner:
`HotKeyCache.loadAndCache` (single and batch), `loadCacheEntry`.

**A hot TTL is a floor, and the permanent sentinel wins on either side.** The effective hot TTL is
`max(normal baseline, configured hot TTL)`, so promotion can never shorten an entry's life. At the
resolved entry layer (after the public override boundary), hard `0` is permanent and soft `0` is
disabled; either sentinel wins over the other side's finite value. Treating `0` as "use the default"
there turned a permanently configured normal entry into a finite hot one and let a permanent hot
policy be overridden by an ordinary TTL. `resolveEffectiveHotHard`/`resolveEffectiveHotSoft` are
therefore called with the already-resolved normal baseline, not with a raw override. Owner:
`TtlPolicy`, `EntryLifecycleImpl.writeValue`, `LocalPromotion`.

**Normal baselines survive transitions.** `normalHardTtlMs`/`normalSoftTtlMs` are recorded at
creation, so a COOL decision or a demotion restores the original baseline instead of inventing
one. A permanent normal hard TTL (`0` in configuration) is stored as the explicit
`Long.MAX_VALUE` sentinel, so COOL/demotion cannot mistake it for an unknown baseline and fall
back to the 120 s protection floor. A normal soft TTL of `0` means disabled and stays disabled
through a COOL rewrite; only an absent hard baseline uses the protection floor. A demotion to a
permanent baseline clears the deadline to `Long.MAX_VALUE` as well as the stored duration: keeping
the HOT deadline while the duration said "never expire" left an entry that was permanent in its
metadata but finite in fact. A COOL rewrite of a soft-`MAX` baseline keeps the soft deadline
permanent instead of capping it at the current HOT deadline.

**Jitter applies to every expiry timestamp** (±5% by default) to break synchronized expiry
across instances. `zeta.local.ttl-jitter-ratio=0` disables it.

**A disabled soft TTL never triggers a stale branch.** `softExpireAtMs == 0` means disabled.
The earlier `<= 0` reading armed a background refresh on _every_ hit — exactly the load storm
the "disabled" setting exists to prevent. An elevated entry's value write keeps its own soft
duration; a stored `0` (the `hot-soft-ttl-ms=0` regime) is not re-enabled by the caller's normal
soft TTL.

Owner: `TtlPolicy` (pure arithmetic), `ZetaProperties` (live configuration).

---

## Expiry and refresh

**Reads serve stale data and refresh behind them.** `StalePolicy.SOFT_REFRESH` (the default on
load-capable paths) serves the entry and arms one background refresh; a successful refresh
re-arms the soft window while the hard TTL stays the absolute bound. `REVALIDATE` blocks: the
reader runs on the calling thread and its value is returned, while the stale entry stays in L1
so a failed load can still serve it and the reload preserves the version identity. `RETURN`
serves as-is.

**Soft-expire dedup is a lock-free marker read.** Deciding whether a refresh is
already in flight is one `ConcurrentHashMap.containsKey`: presence is the state,
and the marker's identity is what makes the completion-time `remove(key, marker)`
ABA-safe. Only the read that actually arms a task takes a limiter permit and installs
the marker with `putIfAbsent`. The task is created outside the map, so a synchronous
executor cannot run its completion callback inside a map update. An earlier
`compute`-based design took the CHM bin lock and allocated a lambda on _every_
soft-expired hit while the refresh was in flight — precisely the slow-source
window in which the read path must stay cheap. A completed marker is removed by
its own task in the same `finally` that releases the permit; a read that observes
it in that window dedups and the next soft-expired read re-arms. There is no
separate `inFlight` bit to clear first: a marker that is already past its task
would otherwise send one more read through a permit acquisition it is guaranteed
to lose at `putIfAbsent`.

**Refresh housekeeping is not a caller read.** The refresh snapshot and the
staleness probe go through `asMap()` (`get`), not `getIfPresent`: Caffeine's map
view skips the hit/miss counters while still honouring physical expiry, so the
internal refresh path cannot count a second hit for a soft-expired read or
inflate the L1 hit rate. The `staleDebt` view uses `containsKey`, which also
leaves the access-order queue untouched; a diagnostics scrape must not make a
failing key look hotter and harder to evict.

**The failure lease never shortens a healthy budget.**

```
remaining = max(0, hardExpireAtMs - now)
floor     = min(120s, hardTtlMs)          // follows the entry's own scale
lease     = max(remaining, floor)
hardExpireAtMs = now + lease
softExpireAtMs = now + lease / 2           // second half = retry window
```

The softer timestamp is the lease midpoint so the entry is soft-expired but hard-valid during
the second half — the next read in that window re-arms the refresh. For a _granted_ lease,
`soft == hard` would close the retry window entirely: the read path checks hard expiry before
soft expiry, so the entry would die at lease end without ever retrying. At the floor the window
is 60 s, above the 30 s refresh timeout. A _refused_ lease deliberately does the opposite: it
closes the window at the hard deadline so a failing source is not retried on every read.

**Lease bookkeeping stays outside the bin lock.** The decision — identity guard,
staleness re-check, TTL arithmetic — runs inside the per-key `compute`; the
`leasedEpisodes` update and the grant counter happen after it. `leasedEpisodes` is
an insertion-ordered map whose monitor is taken only by writers and by the
`staleDebt` snapshot, so taking it under Caffeine's bin lock would let a scrape
stall L1 writers on that bin. The cost is a moment of observability skew: the
entry can carry the extended TTL just before the counter and the view catch up.
A volatile episode count gates the common path: a successful refresh with no
lease outstanding does not acquire the map monitor at all.

**One ticket is the whole refresh lifecycle.** `RefreshTicket` is simultaneously
the `refreshes` reservation token, the creation-time snapshot
(`dataVersion`, `DecisionStamp`, key state, hard expiry/TTL) and the
`whenComplete` callback. Its identity is the ABA guard for the completion-time
removal, and it carries the lease settlement (`NONE`/`GRANTED`/`CAPPED`/
`CLOSED`/`EXPIRED`) written inside the failure compute. A dedicated outcome
field stays clear: reusing a snapshot field for the settlement would make the
same value mean two things across phases and invite a reader to use it after the
overwrite. The shape removes the per-refresh marker, snapshot record, capturing
lambda and four single-element result arrays.

**The lease is refused when it cannot help.** It is skipped for entries older than 30 minutes
(`birthStampSec`), for classifier-rejected failures (fatal `Error`s and app-supplied definitive
errors), and when the hard TTL cannot fit a retry window above the refresh timeout. A refused
lease does not leave the entry soft-expired while hard-valid: the staleness cap and the
too-short-TTL case move the soft deadline up to the existing hard deadline, so the value stays
servable until its already-scheduled exit but no read re-arms the failing refresh in between.
The classifier case hard-expires the entry (both deadlines in the past): a definitive failure
must not keep serving known-stale data. It applies only while the entry is still the same
logical entry (`dataVersion` **and** `hardExpireAtMs` unchanged) and no invalidation tombstone at or
above the ticket's snapshot stamp exists: a peer `INVALIDATE` can close the soft window or remove
the entry while the refresh is in flight, and a failure lease must not reopen the retry window that
invalidation just condemned. A concurrent write, promotion
or broadcast that granted fresh TTLs is never shortened. Permanent entries are never leased.
There is no knob — the alternative, hard expiry during an outage, converts a source outage into
a stampede.

**A clean refresh that returns no value closes the retry window.** A successful background
lookup that yields `null` (the source deleted the row, or the admission predicate declined the
value) is not a failure and writes no new entry, but it must not leave the stale entry
soft-expired: the reservation is removed in the completion callback, so every subsequent read
would re-arm another refresh and hammer the source for the rest of the hard TTL. The empty
result therefore moves the soft deadline up to the existing hard deadline (permanent entries
included) and clears the key's lease episode; the value stays servable until its scheduled exit,
and the next explicit write or hard expiry is the recovery point. Owner:
`DefaultBackgroundRefresher.closeRetryWindowAfterEmptyRefresh`.

**Hard TTL is therefore not an absolute exit while the reader keeps failing.** The staleness
bound is per-lease: 30-minute cap plus the entry's own scale. `BackgroundRefresher` exposes
`refreshFailureLeased` and a bounded `staleDebt` view so the bound is auditable rather than
asserted. The view keeps its oldest 1024 episodes and refuses new keys at the cap: evicting the
eldest would hide exactly the longest-running debt it exists to show. A refusal has no counter
of its own: it is visible as a failure that did not raise the grant count, and every refusal
reason leads to the same response — fix the source, or accept the entry expiring. _Ruled out:_
leaving a refused lease soft-expired while hard-valid — the next read re-arms the failing
refresh immediately, turning the remainder of the hard window into a retry loop.

Owner: `DefaultBackgroundRefresher`, `EntryLifecycleImpl`, `TtlPolicy`.

---

## Read path

**One clock sample, one L1 lookup, one outcome per key.** `get` → `readThrough` →
(`readThrough` hit | `loadAndCache`) → `reconcileHit`:

1. `getIfPresent`; if present, judge hard expiry against a single `now`. A hard-expired entry
   is removed and the read becomes a miss.
2. On a hit: classify the entry once (`classifyHit` → `HitAction`), then serve directly,
   reconcile state (Decision-Validity demotion, local promotion, soft-expiry refresh
   arming), or reload. The access is recorded only when the read actually serves (see
   _One access record per served read_ below). Reconciliation re-verifies inside its own
   atomic compute and removes a resident whose hard deadline passed at the same `now`, so the
   post-image is live without a second eviction pass and a stale entry reference is safe.
3. On a miss: `SingleFlight.load` runs the reader once per key and returns a `LoaderOutcome`.
4. The leader stamps, admits and stores with HOT or NORMAL TTL inside its `leaderCommit`, before
   the shared future completes; every caller then records its own access and returns the value.

**Hard-expiry eviction returns only a live entry.** `EntryLifecycleImpl.evictIfExpired` judges
the caller's snapshot and, when a concurrent write replaced it inside the same compute, judges the
replacement against the same `now`: the method returns `null` or an entry that is not logically
expired. Without that second judgment a peek, a breaker-open stale serve or a reconciled hit could
hand back the replacement after its own hard deadline — the hard TTL contract belongs to the
resident entry, not to the snapshot the caller happened to read, and a stale reference must not
exempt its successor. Callers therefore never re-check the returned reference for expiry;
`classifyHit`'s loop re-classifies a replacement's state, not its lifetime. Owner:
`EntryLifecycleImpl`, `HotKeyCache`.

**Internal probes are stats- and policy-quiet.** `HotKeyCache.isHot`,
`refreshSoftExpire`'s residency confirmation, `resolveNullAnswer`'s stale probe and
`LocalPromotion.promote`'s admission probe are maintenance, not caller reads. They use
`QuietCache` (`policy().getIfPresentQuietly`, falling back to `asMap().get` for a custom `Cache`
whose `policy()` is unsupported), so they neither increment `cache.gets` nor feed Caffeine's read
buffer and W-TinyLFU. `Cache.getIfPresent` does both; `asMap().get` is only stats-quiet — it still
records the node in the read buffer. The previous shape used `getIfPresent`, which let a diagnostics
scrape inflate the hit count and made maintenance reads look like traffic to the eviction policy.
Owner: `QuietCache`, `HotKeyCache`, `LocalPromotion`.

**Both read paths classify a probe the same way.** `classifyHit` resolves a resident entry into a
`HitAction`: `EXPIRED` (evicted under the identity guard, then reload), `RELOAD` (soft-expired under
`REVALIDATE`; the entry stays resident and the read loads), `NONE` (no state rewrite; a `refresh`
flag separately asks for soft-expiry arming), `DEMOTE` (an invalid Worker decision), `REASSERT` (a
local override HOT entry whose COOL lease must return after recovery), or `PROMOTE` (a
degraded-mode COOL or NORMAL-with-COOL-lease entry the local TopK still tracks). The batch fast path
is the plain-serve outcome of that same classification rather than a parallel set of predicates, so
the single-key and batch paths cannot disagree on when a hit is a hit. The action is consumed
directly by `reconcileHit`, so the classification's predicates, the TopK membership probe and the
promotion lookup are not repeated during reconciliation. A fresh locally promoted HOT entry and a
COOL entry that cannot be promoted both classify as a plain serve, so the common hot hit allocates
no read context. The hard-expiry eviction is identity-guarded and never touches the dedup flight:
expiry is not a mutation.

**The hit path arms a refresh without re-reading L1.** `SOFT_REFRESH` used to look the key up a second
time on every hit just to learn the entry's post-reconciliation state. The soft-expiry test is a
comparison on the entry the caller already holds, and it is false for the overwhelming majority of
hits, so the arming now gates on it first and only a genuinely soft-expired hit confirms residency
(the refresh re-warms, so it must not fire for an entry a concurrent invalidate removed). A plain hit
therefore pays exactly one table lookup.

**The batch slow branch arms its reader before it knows the entry is stale.** A batch enters that
branch for any entry that needs reconciliation — a stale Worker decision, a degraded COOL or
NORMAL-with-COOL-lease promotion, or a soft-expired entry — so the reader closure is built for every
slow-branch `SOFT_REFRESH` hit instead
of only an already-stale one. The probe cannot see the race that makes this necessary: a peer
invalidation can close a Worker-managed entry's soft window to `now - 1` between the probe and
`reconcileHit()`, and the Decision-Validity demotion that follows keeps that earlier deadline. Arming on
the probe's snapshot would then skip the refresh. A fresh entry that needs neither correction returns
before this point, so the common batch hot hit still allocates neither a context nor a closure.

**Only the load leader commits to L1.** A merged load hands the same value to every waiter, so N
waiters writing it meant N `compute` calls serialising on one Caffeine bin to produce an identical
entry — the guard then refused all but the first as "resident at least as new". `SingleFlight.load`
therefore takes the caller's store as a `leaderCommit` callback and runs it on the leader's thread
_inside_ the load. The role is implicit: only the elected leader reaches the callback, so no ticket
type, no `isFollower`/`isCurrent` pair and no post-commit `remove(key, ticket)` are needed.

Outside a transaction the commit still runs before the shared future completes, so a woken follower
cannot observe an empty L1 and start a second load. Inside a transaction a caller-affine load is
isolated: the reader still runs on the calling thread, but no flight is published and no L1 store
is registered. A caller-affine loader can read the transaction's own uncommitted writes, so a
follower that joined its flight would observe a value that may still roll back; deferring only the
store still leaked that value through the completed future, and the pre-read stamp cannot be
ordered against the same transaction's deferred invalidations without letting a stale fill outrank
a concurrent write. The transactional call gets its value; a later committed read warms L1. A
non-caller-affine reader runs on the load executor, outside the transaction, so its value is
committed data and its store may run inline. Owner: `HotKeyCache.loadAndCache`,
`SingleFlight.loadIsolated`.

**Six load outcomes, never conflated.** `LOADED`, `SOURCE_EMPTY`, `FAILED`,
`SUPPRESSED_BREAKER`, `SUPPRESSED_TIMEOUT`, `SUPPRESSED_EXECUTOR`. The dedup layer never
throws; `HotKeyCache.resolveLoaded` / `resolveSuppressed` / `resolveFailed` alone turn a status
into a value or an exception. Collapsing them was a real bug: six `Optional.empty()` shapes flowed
through one type, so a transient suppression and a real failure resolved identically.

**A failed load is never a miss; only a null result is.** `SOURCE_EMPTY` (the reader returned
`null`) is the one outcome that resolves to a miss. `FAILED` rethrows its cause. `SUPPRESSED_TIMEOUT`
and `SUPPRESSED_EXECUTOR` throw `ZetaSuppressedException`, because the reader never ran and the
caller must be able to tell that from "no data". `SUPPRESSED_BREAKER` first tries the stale
fallback and trusts the verdict that suppressed it — the breaker is not probed a second time, so a
recovery that lands between admission and resolution cannot turn a degradable suppression into an
exception — and only throws when nothing is servable. There is no per-call switch: a caller that
wants an empty answer reads a key whose loader returns `null`, not one whose source failed.

**A batch closes before it throws.** `getAll` processes every key first and rethrows the first
failure with the rest attached as suppressed (an identical throwable is never attached twice).
Throwing mid-batch would strand the followers that joined this leader's load, leaving every one
of them with a value this node never wrote.

**Stale-first beats fail-fast.** A breaker-open load degrades instead of throwing: with a
hard-valid stale entry the read serves it and records the access like any other; with nothing
servable it returns `null`, i.e. a miss. A hard-expired entry is never served as stale and is
evicted on the spot, dedup slot included — the fallback is the one path that inspects the entry
with no reader behind it, so it is also the natural cleanup point.

**`Error` always propagates, and an interrupt degrades.** A fatal error is never swallowable;
an `InterruptedException` cause resolves as a miss instead of throwing. Only the thread that
ran the reader restores the flag (the caller-runs leader); a waiter that merely joins the failed
future must stay clean — re-interrupting every waiter turned one interrupted load into N
spuriously interrupted business threads.

**TTL suppliers are lazy** — evaluated at most once per call, and only when an entry is created,
promoted by the current call (a load or write that stamps HOT), or a refresh is armed.
Event-driven promotion has no call, so it resolves the hot TTLs from the entry's normal baseline
and the configured hot defaults, evaluating no supplier. Never on a plain NORMAL hit, so SpEL-based
TTLs cost nothing on the hot path.

**One read context survives a reconciliation that reloads.** A reconciliation can consume the
memoized TTLs and still find the entry hard-expired; the fallback load reuses that same context, so
one call never evaluates a TTL supplier twice. A Worker-HOT hit that needs demotion also needs the
context even when the snapshot is fresh: a peer write can close the in-force entry's soft window
while the read holds the older snapshot, and the demotion preserves that deadline, so the refresh is
judged on the entry in force rather than the snapshot.

**Batch reads resolve TTLs once**, take one clock sample, and submit all misses to
`SingleFlight` in one pass. Keys with no value are omitted from the result map (missing key =
miss). Admission still applies per value.

**Hit/miss statistics are request-level.** A key served from L1 without invoking the reader is
a hit; a key that reaches the loader is a miss. Raw Caffeine counters are available through
`unsafe().raw().stats()` and are not the same number.

**One access record per served read.** The access is recorded where the read is served: a plain
serve hit in `servePlain`, a reconciled hit in `reconcileHit`, a completed load in `loadAndCache`,
a breaker-open stale serve in the fallback itself.
The post-reconciliation TOCTOU re-check therefore records nothing when it finds the rewritten
entry expired — the single-key path corrects the hit it counted at entry and degrades to a
reload (which records), and the batch path counts the miss and loads once. Recording before the
re-check charged a hit for a read whose entry died under the rewrites and, on the batch path,
counted the key a second time when the reload recorded again: a read that expired mid-flight is
a miss that happened to be fast, not a hit that returned nothing. The stale-serve record keeps
the hot-key signal alive through an outage window — a key hot enough to be read throughout the
outage must not stop looking hot exactly when the Worker needs to know.

**Batch reads dedup their keys.** A repeated key is probed, counted and loaded once, so one
logical read cannot be counted twice or start a second load for a key already in flight.

Owner: `HotKeyCache` (`readThrough`, `servePlain`, `reconcileHit`, `classifyHit`, `loadAndCache`,
`resolveLoaded`, `resolveSuppressed`, `resolveFailed`), `SingleFlightImpl`.

---

## Write path

**Bump before mutate.** `put(key, value)` is local-only by default: there is no business writer
inside Zeta, so "mutate the source, then update the cache" is the caller's orchestration.
`PutPolicy.synced()` allocates a stamp, applies locally, then broadcasts a value-carrying
`PUT`; peers apply it under the version guard without reloading.

**Transaction-aware by default.** Inside an active transaction the write or evict waits for
commit, and a rollback drops it. `InvalidatePolicy.immediate(true)` is the escape hatch that
runs the removal on the calling thread (the `@ZetaInvalidate(beforeInvocation = true)` contract).

**Batch writes share one policy, one transaction boundary and one version block per chunk.**
`putAll` validates the whole map before any write, resolves one `PutPolicy`, checks the effective
hard TTL once, and delegates to `HotKeyCache.putAll`. The engine freezes the map when the store is
deferred, then reuses `forEachStampBlock` to reserve one contiguous stamp block per chunk before
applying each row through the shared write tail. A concurrent single write is ordered against the
whole block — older than every chunk stamp or newer than every one — instead of landing between two
keys of the same intended batch. The batch is deliberately **not atomic**: a failure after some
applies leaves those keys written, and a synced batch still sends one PUT per key because the wire
has no batch PUT message. The previous shape was a caller loop of `put`: N independent stamp
allocations and N afterCommit registrations, with a concurrent write free to interleave between
keys. Owner: `DefaultZeta.putAll`, `HotKeyCache.putAll`.

**A synced write validates the effective hard TTL, not the raw override.** `PutPolicy.of(0, …, true)`
means "use the configured default", and `zeta.local.hard-ttl-ms=0` means permanent. Checking only
the raw value therefore let the default `@ZetaPut(sync=true)` broadcast a permanent entry: peers
drop an entry only through its hard TTL, so every peer pinned the stale value forever. `DefaultZeta`
resolves the effective TTL through `HotKeyCache.resolveSyncableHardTtl` and rejects the write before
the cache call; `refreshAll`, which is always synced, applies the same check to its default. The
previous shape checked `Long.MAX_VALUE` on the raw override only. Owner: `DefaultZeta`,
`HotKeyCache.resolveSyncableHardTtl`.

**Elevated entries get value-only updates, from every write shape.** A local write, a load or a
peer PUT/REFRESH on a HOT/COOL entry updates the value, data stamp and birth stamp and re-arms only
the soft window, capped by the existing hard deadline. The active regime and its hard bound survive;
so do the decision lease and its kind. A NORMAL value update re-arms both normal deadlines. This is
one tail — `EntryLifecycle.writeValue` — used by `putLocal`, `putThrough`, `refresh`, the load path
and `DefaultSyncContext`; the receiver no longer re-arms a HOT/COOL hard deadline that the
sender preserved. A logically expired resident is treated as absent: it gets a fresh lifetime
instead of inheriting its dead deadline. Overwriting an elevated regime would leave an entry whose
TTLs the Worker can never repair, because the Worker only speaks again when the key changes phase.

**`refresh` allocates the stamp before loading.** A concurrent synced write during the load
therefore holds the newer stamp and this refresh's apply is skipped instead of regressing it. A
`null` loader result skips the refresh entirely — a refresh that learned nothing must not evict
the value it was asked to renew.

**A refused write neither lands nor broadcasts.** The write-through apply runs behind
`writeRefused` — the tombstone barrier plus the expiry-aware resident guard (design decisions:
version comparison) — and the cluster PUT fires only when `EntryMutations.apply` reports an
installed entry. A refused write (a strictly newer resident, or a tombstone from an
INVALIDATE/CLEAR that landed between the stamp allocation and the compute) would otherwise ship a
value this node has already superseded — or invalidated — to peers that may not hold the newer
entry, re-warming data the local tombstone just removed. The applied/refused distinction is the
same one the load path makes before touching the dedup entry.

**A read fill is not a write-through.** The load path takes a pre-read stamp, stores
version-guarded, preserves an elevated TTL regime and sends no broadcast. A fill that loses
the comparison leaves the newer entry alone; the winning write ran its own invalidation.

Owner: `HotKeyCache` (`putThrough`, `putLocal`, `refresh`, `writeThroughApply`, `writeRefused`,
`buildEntryOnWrite`), `EntryMutations`, `TransactionSupport`.

---

## Invalidation

**Bump, drop, then broadcast.** Allocate the stamp, remove from L1 **and** from the
SingleFlight dedup cache, then send `INVALIDATE`. Allocating after the removal reopens a window
in which a racing reload is stamped with the old version and then pinned by the invalidate's
higher version.

**REMOTE-only invalidation follows the same after-commit contract.** A remote-only call allocates
its stamp and broadcasts inside `TransactionSupport.run(immediate, …)`, and the batched form freezes
the caller's key list before deferral. A rollback therefore sends no INVALIDATE/clear at all. The
previous shape broadcast immediately, so a rollback leaked a peer removal; a peer could then
reload the pre-commit value and keep it until its hard TTL, with no compensating message after the
commit. `immediate=true` still runs on the calling thread. `InvalidatePolicy` is the single policy
surface: `HotKeyCache.invalidate`/`clear`/`clearPrefix` read its scope, sync flag and immediate flag
directly, so the facade has no `REMOTE` branch and a local-only call cannot accidentally select the
broadcast path through a second spelling of the same boolean. Owner: `DefaultZeta`,
`HotKeyCache`.

**Batch the wire, not the guard.** A collection invalidation takes one stamp per key (same
bump-first order) and packs `[key, version]` pairs into `INVALIDATE_BATCH` messages of at most
1000 entries. Receivers decompose into per-key tasks on the ordered dispatcher, so a batch entry
still orders against that key's PUT/REFRESH. The per-chunk versions are reserved as **one
contiguous clock block before any drop** (design decisions: version stamps), so a single write
racing the batch is superseded by it or supersedes it as a whole — the per-key guard never sees a
half-interleaved order.

**One clear message + one watermark.** `clearAll()` allocates one `clearStamp`, sweeps entries
at or below it (a strictly newer concurrent write survives) and broadcasts one
`INVALIDATE_ALL`. Cost is independent of how many keys the instance holds, and an instance
holding none still converges the cluster.

**A clear also kills the pre-clear dedup flights.** The watermark refuses a pre-clear fill, but
a read that starts _after_ the clear must not join a load that began before it: joining would
serve the pre-clear value and skip the reader even though the clear already returned. A
table-wide clear invalidates every flight; a scoped clear invalidates only the flights under its
prefix (`SingleFlight.invalidatePrefix`), and the sync receiver applies the same kill when it
records `INVALIDATE_ALL`/`INVALIDATE_PREFIX`. Unrelated namespaces keep their in-flight loads,
which is why the scoped path is not a blanket `invalidateAll`. The kill happens **once**, before the
sweep, not per removed key: the watermark recorded first already refuses every pre-clear fill, so a
per-key `invalidate` inside the sweep would only drop flights created by reads that started after
the clear — forcing a duplicate load for every key the sweep touches. Owner: `HotKeyCache.doClear`,
`DefaultSyncContext.applyClear`, `SingleFlightImpl`.

**The watermark guards every write path.** The per-key tombstones and the clear watermark live in
`EntryLifecycle`, and peer applies, load fills, background refreshes, Worker preloads and the
write-through apply (`putThrough` / `refresh` → `buildEntryOnWrite`) all consult the same
`staleAgainstInvalidation` check. Without it a slow read that started before the
invalidation finds L1 absent (nothing to version-guard against) and re-populates the value the
removal just dropped. The write-through apply is the path exposed by construction — it allocates
its stamp, then (for `refresh`) loads, then computes — so the barrier is what stops an INVALIDATE
landing inside that window from being undone by the apply; it was the last write path to carry the
check (2026-10).

**Two volatile reads, no ordering hazard.** `staleAgainstInvalidation` reads the table-wide
watermark and then the prefix snapshot — two independent volatile reads with no happens-before edge
between them, so one pass can observe the old watermark and a post-clear snapshot. That is safe
because the check runs inside the same per-key `compute` as the store, and a clear records its
watermark _before_ it sweeps: a fill that acquires the key after the sweep must observe the new
watermark (the sweep's lock release publishes the earlier volatile write), and one that acquires it
before the sweep is removed by that sweep, because its stamp is at or below the clear stamp.
Reading the prefix snapshot first would not add a guarantee the compute already holds.

**Tombstone expiry runs on the lifecycle clock.** The per-key tombstone cache uses `TimeSource`'s
monotonic axis as its Caffeine ticker, so a tombstone ages with every other deadline in the class —
and tests advance it through the offset hook — instead of a private `System.nanoTime` axis.

The writer side is the other half, and it is easy to lose: **every** removal must record —
`HotKeyCache.dropAndBroadcastInvalidate`, `executeBatchInvalidate`, `clear`, and the sync
receiver's `removeEntry`/`handleClearAll`. A local-only invalidation or clear allocates a stamp
purely to tombstone (it is never broadcast), because "nobody else
was told" says nothing about the reads already in flight on this node. A guard that reads state
no writer fills is indistinguishable from no guard at all until the race fires.

**Sends leave the caller; saturation drops and counts.** INVALIDATE publications run on the
send executor; a saturated executor drops the message and increments
`BroadcastBuffer.invalidatesDropped` rather than blocking the caller. Peers converge via the next
write or their hard TTL.

**A local clear never broadcasts.** `clearAll(InvalidatePolicy.localOnly())` clears one JVM, not
the cluster — peers converge at their own entry TTL; `clearAll` with the default scope is the
converging form. The former `flushLocalEmergency` verb was this exact operation
(`Scope.LOCAL` + `immediate`) wearing a second name, and was folded into `clearAll`
(2026-10); `unsafe().raw()` remains the escape hatch for anything the verbs do not cover.

Owner: `HotKeyCache` (`invalidate`, `dropAndBroadcastInvalidate`, `sweep`, `doClear`),
`BroadcastBuffer`, `DefaultSyncContext`.

---

## Hot state

**Local promotion is a provisional speed-up.** `NORMAL → HOT` happens off the read path: a key
newly admitted to the local TopK offers its name to a bounded queue drained by the shared
scheduler, which rewrites a resident NORMAL entry to HOT with the configured hot TTLs. A plain
NORMAL hit therefore pays no TopK membership probe and no health check. It cannot race the
Worker because the Worker never emits NORMAL. The drain samples TopK membership before the
rewrite; the compute re-checks residency, promotable state and logical expiry under the bin lock
but not membership — TopK ownership is advisory, so a sample that goes stale before the rewrite
lands promotes a key the sketch has just expelled.

The queue is bounded and drops on saturation; a load or local write that finds the key already in
the TopK stamps HOT directly, which repairs both a dropped event and an entry created after the
admission. `zeta.scheduling.enabled=false` removes the promoter, so NORMAL entries then promote
only through that load/write check or a Worker decision.

**A COOL lease gates local promotion even after the state moved to NORMAL.** A successful
soft-expire refresh rewrites a COOL entry to NORMAL while keeping its decision lease. Checking only
`keyState == COOL` would therefore let a healthy cluster locally promote a key the Worker had just
cooled. The promotion gate reads the lease kind: a COOL lease permits local promotion only while the
health gate fails. A promotion that happens during degradation keeps the lease and sets
`isLocalOverrideHot`; on recovery the read path reasserts a live COOL lease (or clears a dead
Worker's lease), so the entry cannot stay HOT indefinitely under an old COOL decision.

**A promotion extends life; it never resurrects or truncates it.** The rewrite re-checks logical
expiry inside its `compute`, because the admission event runs off the read path and the resident
may have crossed its hard deadline since it was queued: an expired entry is removed, never
re-stamped with a fresh deadline. The lock-free pre-check that skips a live already-promoted
resident still lets an expired snapshot through to that compute, so the optimization never
bypasses the removal. The attempt reports the pre/post images the compute observed — unchanged
when the rewrite was skipped, `current == null` when the resident was removed as expired —
because the read that triggered it must treat the removal as a miss instead of serving the value
the compute just evicted. The read path uses the reported post-image for its TOCTOU expiry check,
so a rewrite costs no second table lookup. The hot TTL is
resolved from the resident's normal
baseline and the call's override — `max(override, normalHardTtl, configured hot default)` for hard,
`max(override, normalSoftTtl, configured hot default)` for soft — so an entry written with a
longer explicit TTL keeps that lifetime when it is lifted. Event-driven promotion passes no
override; the read-path COOL promotion passes the call's TTL overrides through the same floor.

**`COOL → HOT` only under degradation.** While the cluster health gate passes, a COOL decision
is authoritative; only when it fails does the local TopK assume authority.

**Decision Validity.** A Worker decision is valid only while its issuing incarnation is alive.
The stamp carries the issuing Worker's node ID, epoch and kind; the read path resolves the record in
one map lookup and checks the same epoch and the shared liveness judgment. A local entry (no
stamp) is never incarnation-invalid, and a local override HOT carrying a COOL lease is not a
Worker-owned HOT: its recovery is the reassertion rule above, not Decision-Validity demotion. When the judgment fails, the next read rewrites the entry
in place — `NORMAL`, normal TTLs, decision stamp
cleared, value preserved and still served. A resident whose hard deadline has already passed is
removed in the same compute instead of rewritten: without that removal the rewritten entry keeps
its past deadline, and because `reconcileHit` no longer runs a second expiry pass, the read path
would serve and access-record an entry the hard TTL had already condemned. The rewrite
caps the entry's remaining life at the
normal TTL rather than restarting it: the value keeps its age, so a demotion may shorten the
deadline the dead Worker's decision granted but never extends the entry's own (a demotion is not
a refresh). The rewrite is built **inside** the per-key `compute`, not before it: a dead Worker's
HOT set is read concurrently, so a pre-built candidate hands every losing reader one full
draft-plus-entry allocation to discard. The callback is single-threaded per key, so only the winner
builds. _Ruled out:_ building it outside the lock to shorten the critical section — the rewrite is
pure in-memory work (config read, jitter, two clock reads, one copy), and against a demotion storm
the nanoseconds saved inside the bin lock are outweighed by N−1 wasted allocations. The liveness
check reads the health record's own freshness window, so validity can lag a dead Worker by at
most the effective heartbeat timeout. No scan: only entries actually read are visited; unread
ones converge at the hot hard TTL.

**No decision lease: a deadline nothing renews would only hurt.** A Worker-HOT entry carries no
expiry beyond its decision stamp and hot TTLs. The Worker sends each decision exactly once
(design decisions: sync messages), so a lease deadline would demote every HOT entry one
lease-length after its promotion while the Worker still considered the key hot, and the key could
only be re-promoted after a full cool-down and re-heat. What bounds an orphaned HOT entry is the
incarnation check: a dead or restarted Worker fails it on the next read, and an entry nobody reads
converges at the hot hard TTL. _Ruled out:_ a lease plus a sampling sweep against a Worker that
sends no keepalives.

**Demotion is re-queued, not re-promoted in the same read.** Promotion judges the pre-demotion
snapshot; after a demotion the entry is NORMAL, and if the local TopK still contains the key it is
offered to the promoter queue again so the off-path tick restores HOT. Clearing the stamp is what
prevents demote/promote oscillation. The re-queue is wired once as a demoted sink on the lifecycle
(`EntryLifecycle.setDemotedSink`) instead of being duplicated at every demotion call site — the
sink fires exactly once per successful demotion.

**A HOT promotion never rewrites newer data.** `decisionVersion` and `dataVersion` are orthogonal,
so the Worker plane's version guard cannot see a concurrent put / peer PUT / refresh that landed
during the promotion's authoritative load. `DefaultWorkerContext` therefore judges the
data plane inside the compute: the loaded value is written only when it is provably fresh —
resident `dataVersion` at or below the pre-read value stamp and no invalidation tombstone crossed
since it. Otherwise the promotion applies decision metadata only (state, decision stamp, hot TTLs)
and the live resident value and version survive. `editEntry` preserves the resident's
`dataVersion`, so an unguarded overwrite would wear the newer version like fresh data and lock the
stale value in against every later sync for the whole hot hard TTL — the one shape the tombstone
and resident guards cannot catch, because it carries a legitimate version.

**A logically expired resident is absent, not a veto.** The read path does not serve a resident
whose hard deadline passed, so the promotion must not treat it as a live pre-image. When this call
loaded a fresh value, `settleHot` creates a new HOT entry with `createHot` and a fresh
`normalHardBaseline`; when it did not, the promotion is skipped. The previous shape ran the
metadata-only branch on the dead entry: a clock-skewed dead entry carrying a higher `dataVersion`
rejected the freshly loaded value and fired `VALUE_NOT_FOUND`, and the accepted case edited the
dead entry in place, carrying its already-expired baseline into the next COOL downgrade. The
version guard still runs first, because a dead entry's decision stamp must keep rejecting a
same-node replayed older decision.

The tombstone probe is part of the value-rewrite guard, not of the metadata-only path. A
no-loader deployment (and an L1-fallback promotion) has no pre-read stamp to compare, so the probe
would run at version `0` and report "invalidated during load" for every key with a live per-key
tombstone — even though the resident is inside the bin lock and is exactly what the read path
serves. Such a promotion updates only the decision reference, state and TTLs and must not consult
the invalidation table at all; otherwise the entry stays NORMAL forever. A refused promotion still
fires exactly one terminal `onHotSkipped`: `VERSION_STALE` for the inner DCL guard, and
`VALUE_NOT_FOUND` for a tombstone-refused create or an absent/value-less/logically expired
resident. Owner: `DefaultWorkerContext.settleHot`, `promoteExisting`.

**A loader-failure fallback keeps the resident's deadlines.** When the cluster loader fails or
returns nothing, the HOT decision promotes the resident L1 value only in metadata; applying the
hot hard/soft TTLs would reset both deadlines to `now + hot TTL`, making stale data look freshly
loaded and suppressing its next soft refresh for the whole hot window. The metadata-only rewrite
therefore leaves the existing durations and expiry timestamps untouched. A deployment with no
loader is different: the resident is the authoritative source, so its promotion does adopt the
hot TTLs. Owner: `DefaultWorkerContext.promoteExisting`.

**The create stamp is the value's stamp, not the decision's, and a fallback cannot create.**
One slot carries the stamp through both phases of `resolveHot`/`settleHot`: the pre-read stamp for a fresh load,
and the resident value's own `dataVersion` when the load fell back to L1. A fallback is an existing
value, never a new one, so signing it with the pre-read stamp would let it outrank a tombstone
recorded above its own version and resurrect a value a concurrent invalidation had already
condemned. The create path therefore runs only when this call loaded a fresh value. If an L1
fallback's resident disappears between the fallback read and the compute, the promotion is skipped
as `VALUE_NOT_FOUND`. Before that guard, a hard-expired resident removed by a concurrent read in
that window was re-created from the fallback value under fresh HOT TTLs and a new `birthStampSec`,
resurrecting data the hard TTL had already condemned for a full hot lifetime. Owner:
`DefaultWorkerContext.resolveHot`, `createHot`.

**Every decision is sent once.** `WorkerBroadcaster` publishes a HOT or COOL exactly once, with no
repeat chain and no periodic re-emission, and a failed or dropped send is not replayed (design
decisions: sync messages). A lost COOL leaves the entry at its HOT TTLs and a
lost HOT leaves it at its normal TTL; both are bounded by the hot hard TTL, by the Decision-Validity
demotion when the Worker incarnation goes away, and by the local TopK reclaiming TTL authority —
per-key retries were judged not worth their machinery. The receiver's version guard still makes a
late duplicate harmless, so the send path stays idempotent even without retries.

**A COOL run is reduced, a HOT run is not.** The ordered dispatcher grants same-key batches;
`DefaultWorkerContext.reduceCool` folds a run of COOL decisions from one sender node
to the last decision that survives the version guard, firing `onCoolSkipped` for the covered
messages. COOL is pure in-memory (no loader, no create or remove), so only the final decision can
affect the state. The fold is restricted to a single `decisionNodeId`: the ghost-COOL guard tests
the resident HOT's owner, so an earlier same-owner COOL can legitimately demote the entry and
disable the guard for a later cross-node COOL; dropping it could leave the entry HOT. Mixed-node
runs fall back to sequential applies. A run of one COOL goes straight to `policy.cool`: no
`subList`, no reduction, and no second `requireType` for a batch that cannot change. `reduceCool`
validates types and decision-node identity in one pass. HOT is never reduced: a later HOT may load
a different value, and if its load fails the earlier HOT's created L1 entry is its only fallback.
Owner: `DefaultWorkerContext.reduceCool`, `WorkerListener.dispatchBatch`.

Owner: `LocalPromotion`, `EntryLifecycleImpl.demoteIfRemoteNodeIdInvalid`, `CacheKeysPolicy`,
`DefaultWorkerContext`, `ZetaSchedulingConfiguration`.

---

## Access counting

**Every served read counts once.** `AccessCounter` owns a single `WaveCounter`. Each tide
feeds the reporter's batch; once `detect-interval-ms` (500 ms) has elapsed, or a tide reaches the
accumulator's key cap, the residual accumulator and that tide go into `HotKeyDetector.ingest`.
Before this, the read path appended to two counters with
two hashes, two thread-local lookups, two capacity decisions and two independent reservoir caps
that could disagree — one pipeline admitting a key the other dropped.

**Tag-mode routing belongs to the counter.** `AccessCounter.record(cacheKey, TagMode)` owns the
four-way switch (`RECORD`, `DETECT_ONLY`, `REPORT_ONLY`, `SILENT`), and `HotKeyCache.recordAccess`
calls it directly. The previous shape decomposed the mode into two booleans in the dispatcher and
rebuilt the enum's meaning with nested `if`/`else` — a new mode could silently fall through to the
merged read path, and the routing rule lived away from the class whose documentation describes it.
Owner: `AccessCounter.record`, `HotKeyCache.recordAccess`.

**The tide period and the sketch cadence are two clocks.** The tide base starts at
`report-interval-ms` and the reporter's feed-loop tuner moves it inside
`[report-interval-min-ms, report-interval-max-ms]` (50–1000 ms); the sketch cadence is the fixed
`detect-interval-ms` the accumulator checks on every tide. Detection must not follow the reporting
control loop — a tuner that slows reporting under pressure would otherwise slow TopK admission with
it. The configured interval is a floor, not a ceiling: a tide slower than it stretches the ingest
with it.

**The sketch kernel owns key validity.** `HeavyKeeper` throws on a `null` key and treats the empty
string as a legitimate one; `HotKeyDetector` is the cache-side edge that maps `null`/blank to
`AddResult.cold()`. The tide's batch ingest skips a null key instead of discarding the whole
flush, while the public batch API still fails fast like the single-key path. Keeping the policy in
one place is what lets `hotkeydetector` drop its dependency on `cache.CacheKeysPolicy`.

**The accumulator stays bounded under a scan.** The detector-side map's 16,384-key ceiling is a
flush trigger: crossing it hands the merged batch to the sketch immediately, and a single tide
that alone reaches the ceiling is handed over directly instead of being merged (the reporter
reservoir can carry up to 100,000 keys). The post-merge check is deliberate — a stable hot set
overlaps almost completely, so a pre-merge sum would flush every other tide at twice the sketch
update cost — and it lets one merge overshoot by at most the snapshot's own size. A flush replaces
the map only when the flushed batch exceeds the burst watermark, instead of only calling
`clear()`: `HashMap.clear()` retains its `Node[]` table, so a scan burst would otherwise pin its
peak capacity for the process lifetime, while replacing at the initial capacity made steady-state
cardinality rehash every cadence. The initial table is sized for its entry target at the default
load factor, and a small batch just clears the reused map.

**The detector is not a `TopK` bean.** It used to implement the interface while delegating every
call to the `HeavyKeeper` bean, so `List<TopK>` injection saw the same sketch twice and decayed it
twice per tick. Queries now inject `TopK`; writes enter through the detector, and it also owns
the lenient membership probe `contains(...)`, but it deliberately does not implement the interface.

**Per-key tag modes leave the merged tide.** The tide cannot split per key, so
`DETECT_ONLY` adds synchronously to the sketch and `REPORT_ONLY` publishes a single-key batch.
Both are rare, opt-in paths.

**No reporter ⇒ detector-only counter.** The tide base becomes the detector's cadence and the
report leg is skipped, so App-only deployments keep buffered counting. The cadence is still not a
fixed batch: the tide pacer compresses a burst toward 50 ms to drain the reservoir, so
`mergeIntoSketches` keeps the accumulator as the sketch-cadence bridge. _Ruled out:_
direct-ingesting a no-reporter tide — it would hand the sketch up to 10× the configured ingest rate
exactly during the scan the accumulator exists to batch.

**The sketch ingest is additive, so a due tide merges before it flushes.** The flush decision runs
before any merge: a snapshot that alone reaches the key cap is handed to the sketch separately from
the residual, because merging a 100k-key scan would drive the reused map's table capacity. Every
other due tide first merges into the accumulator and then flushes one combined batch — the two
views overlap almost completely on a stable hot set, so ingesting them separately paid the sketch's
hash and update twice for the same keys every cadence. An empty accumulator ingests the snapshot
directly, so the merge copy is skipped when there is nothing to combine. The accumulator is sized
for its entry target at the default load factor, and a flush releases its table only above a burst
watermark instead of at the initial capacity: steady-state cardinality must not rehash every
cadence, while one scan burst still cannot pin the peak capacity for the process lifetime.
Shutdown drains the residual accumulator instead of dropping it (the counter's own one-tide loss
contract covers only the reservoir).

Owner: `AccessCounter`, `HotKeyDetector`, `KeyReporter`.

---

## Sketch

**The sketch may approximate counts; it must not invent traffic.** An owned estimate: a row
contributes to a key's estimate only while the slot's fingerprint matches that key. A colliding
fingerprint contributes **zero** unless the slot changes hands (size-aware takeover or the
displacement lottery) — the occupant's decayed count is never handed to the newcomer. Before
this rule a cold key sharing a slot inherited someone else's traffic and could enter the TopK
on it.

**The sketch is the single source of truth for membership counts.** A member's count is
re-synchronised from its sketch slot on every decay cycle (`probeSketch(node.fp)`), not halved
independently. Slot sums are a true sliding window, so a key whose traffic stopped reaches zero
exactly when the ring rotates past it; halving instead took `log2(peak / floor)` cycles — a burst
of 1e6 stayed above the `minCount` floor for 17 cycles after its traffic had already left the
window, keeping `minMemberCount` inflated and locking genuinely hot newcomers out. The read is
**lock-free on purpose**: `decayMembership` holds `admissionLock`, and taking a stripe lock there
would invert the class's stripe → admission order. When every row has been taken over
(`probeSketch` returns `-1`) the member halves instead of dropping — a collision can blind the
sketch to a key that is still busy, and that must cost one cycle, not the seat.

**Seats are released when they cannot come back.** A member whose count falls below `minCount`
loses its seat (floor 1, so `minCount=0` still expels): below the admission threshold it can
never re-enter, and keeping the seat starves a better candidate and depresses the O(1)
fast-reject floor.

**Collision decay distributes its decrement with a carried remainder.** Each window's share is
`floor((wv * decays + carry) / cur)` and the modulo becomes the next window's carry, so the shares
sum to exactly `decays`. Independent per-window flooring subtracted nothing at all whenever
`decays` was smaller than the number of non-empty windows — with `decays=1` spread over three
windows every share floored to zero, and the collision decayed nothing.

**Fading locks the compound slot update, not the individual counter.** A slot's fingerprint,
running sum and window ring are read and written together; individual `int` writes are atomic but
a combination from two instants is not a consistent slot. Per-stripe `synchronized` guards the
compound update and beat `ReentrantLock`/`StampedLock` on this critical section. Fading runs
every 20 s; admission takes the stripes before the admission lock, and the order never reverses.

**Membership counts use max-merge with a deadband.** A hit adds its batch to the sketch before the
raise, so the stored count was behind on every hit and the strict compare-and-raise executed a
locked CAS each time. The write is now skipped until growth reaches `max(16, cur / 32)` — the
stored count only feeds ordering and the eviction floor, both tolerant of a few percent of lag —
while a member near `minMemberCount` still raises eagerly, because there the lag decides its seat.
The `LongAccumulator` experiment remains reverted: `reset()` silently drops concurrent
`accumulate()` calls, and a plain `LongAdder` cannot express max-merge.

**Slot state is one flat `int[]`**, `[fingerprint, slotSum, w0 .. wN]` per slot. The three values
were separate arrays, so every probe of every row reached three unrelated addresses; at
`width=50_000 × depth=5` each of those is a cache miss, paid per row per add. One slot now sits in
a handful of adjacent ints.

**`WaveCounter` is a counter, not a policy layer.** Its heat-aware routing, tide pacing and
promotion-floor governance are deliberately complex. The full protocol — hot/cold routing, the
exact-versus-approximate precision split, the promotion boundary and governor, the tide pacer, the
memory recycling — is in [Counting engine (WaveCounter)](#counting-engine-wavecounter). The boundary
is the design: count a key, receive batched snapshots. Nothing outside depends on its routing, its
tide state or its promotion policy, and that must stay true.

**One item shape, one count slot, one window index.** `Item` is the single `(key, count)` value
for the membership snapshot, the expelled queue and the public ranking; a private snapshot record
per copy would only add a mapping step. A member's count is a volatile `int` mutated through a
`VarHandle` CAS, not an `AtomicInteger`: the atomic object is a second heap allocation per member
for a value that is already `int`-capped. `activeWindow` is an `int` that `fading()` advances and
every updater reads, so the hot path pays no `AtomicLong` dereference and no `& windowMask`; the
ring index is always already masked. Admission resolves the minimum as a `Node` reference under
`admissionLock`; every removal runs under the same lock, so the reference cannot detach between the
scan and the eviction CAS. A rejected admission returns the shared `AddResult.cold()` — the
per-key record would carry a key no caller may use.

Owner: `HeavyKeeper`, `HotKeyDetector`, `WaveCounter`, `AccessCounter`.

---

## Load protection

**The breaker is per key, sliding-window, and deliberately asymmetric.** `CLOSED` trips to
`OPEN` when the failure rate crosses the threshold; `OPEN` moves to `HALF_OPEN` after the
timeout; `HALF_OPEN` needs `consecutiveSuccessThreshold` consecutive probe successes to close,
and any probe failure reopens. Counting consecutive successes is what stops a single lucky probe
from closing a still-broken source.

**Exception filtering is by assignable type, and exclusion wins.** `excludeExceptions` never
trips the breaker; `includeExceptions` is the only set that can (empty means all). A thrown
exception matches when any configured type is assignable from its class, a superclass, or a
directly declared interface — so subclasses and interface implementations count. The whole cause
chain is walked (depth-capped), because a wrapped exception must still match its underlying
cause. Names are resolved once and cached, including the resolves-to-empty outcome, so a
mistyped class does not re-run `Class.forName` per classification.

**Sharding follows the annotation namespace.** `CircuitBreakerRegistry.prefixOf` reads the segment
before the namespace delimiter (`U+0000`) first, then the conventional `:` datasource prefix, and
falls back to one shared bucket. Without the delimiter check every annotated key — whose physical
form is `cache\u0000logicalKey` — shared the default shard whenever the logical key held no colon,
so one slow `@ZetaCached` namespace tripped every other namespace with it; when the logical key did
hold one (the default-key encoding, a composite id), the colon split inside the logical key and
scattered one namespace's failures across high-cardinality shards, preventing the breaker from
tripping and evicting state. Owner: `CircuitBreakerRegistry`.

**The breaker machine is one immutable snapshot.** `CLOSED → OPEN`, `HALF_OPEN → OPEN`,
`HALF_OPEN → CLOSED` and the wedged-episode reset each swap one `Epoch` — state, episode
generation, live probe slots, consecutive successes, the episode clock and the probe-credit
multiplier — with a single compare-and-set, so a failure storm trips once, logs once and stamps the
cooldown once instead of letting every failing thread re-decide. Admission is one retry loop over
that snapshot: a lost CAS means another thread advanced the machine, and the answer is re-derived
from the new epoch. Rejecting on a lost CAS was wrong because the state it lost to may be a
`CLOSED` that had just recovered from a successful probe — the caller then threw
`ZetaSuppressedException` against a healthy breaker. The credit belongs to the same
snapshot: a failed episode's penalty and a recovered episode's reward are applied by the transition
CAS that publishes the outcome, so no reader sees a new state with the previous episode's quota. The
packing is not a micro-optimisation. Those values used to live in separate fields, and every ordering
between them was a window somebody could observe:
a retired episode still advertising free slots (ghost probes admitted into the episode that had
just failed, one recovery failure charged as two, a `CLOSED` publication overwritten by the
reopen it raced), an `OPEN` state whose timer had not been written yet, and an episode-reset
right that a losing CAS could leak — after which no thread could ever open an episode again, so
the breaker stayed `OPEN` until the process restarted. One word means a reader can never see a
state without the quota, credit and timer that belong to it, and a probe stamp is live only while
its generation still matches: a straggler from an older episode settles into nothing.
`HALF_OPEN → CLOSED` still clears the window before publishing CLOSED — an evaluation must never
see CLOSED with pre-recovery buckets, or the breaker re-trips the instant it heals — and the probe
slot release, the winning success and the transition are one CAS: no transient HALF_OPEN with a
free slot is published between the threshold check and the close, so a ghost probe cannot enter a
recovery that has already met its threshold and an old probe failure cannot reopen it. Only a
settlement that meets the threshold closes, and the close is generation-guarded: a concurrent
success that merely continues the streak does not close. The window clear runs before the closing
CAS, so a closer that loses that CAS after another closer published CLOSED can clear the fresh
window once, costing at most a delayed re-trip; the opposite order would instead expose CLOSED with
pre-recovery buckets, which is worse. A HALF_OPEN episode whose probes stop settling is force-retired
after `singleTestIntervalMs`, and the same CAS
starts the replacement episode owning one slot (a hung probe cannot pin the breaker; slow-but-alive
probes keep settling and keep the episode alive). Rate evaluation stays per-failure — a collapsing
gate would skip the very failure that completes a same-millisecond burst past the threshold, and
bursts are exactly how a small-volume window trips — while the transition CAS bounds a storm to one
decision and one log line. Disabling the breaker force-closes it to CLOSED with the episode retired
and the window cleared, so stale buckets cannot flap it OPEN/CLOSED while disabled. Exception
filters resolve eagerly at construction through the context classloader (Spring `ClassUtils`),
removing the first-classification race a lazy resolve had and surviving layered-classloader
deployments.

**`isOpen` is the fast-fail state; `isDegraded` is the read path's gate.** A HALF_OPEN breaker
admits only its probe quota and rejects the surplus, so `isOpen == false` while those surplus loads
are being refused. The read path therefore asks `isDegraded` (OPEN, or HALF_OPEN), and a rejected
probe, a failed probe and a null recovery answer all take the stale fallback instead of throwing.
`isOpen` remains the exact OPEN predicate for callers that need the fast-fail state itself. Owner:
`CircuitBreaker`, `CircuitBreakerRegistry`, `SingleFlightImpl`.

**The window is a clock-driven ring, not a scheduled one.** A settlement stamps its bucket with the
aligned window index (`now / slideMs`) and zeroes that bucket when the index moved on, so the window
advances with traffic and needs no task, no `start()`/`close()` lifecycle and no scheduler to share.
The earlier shape rotated one bucket per `slideMs` from a background timer, which dragged in three
pieces of machinery — a registry-wide scheduler, a lifecycle, and a versioned watermark to detect a
rotation that raced an evaluation — and made the trip decision depend on that timer running. A
stalled rotation froze the window for every shard sharing the thread while the breaker stayed
`CLOSED`, i.e. exactly when it was needed. Reading the clock on the write path cannot stall.

**The trip gate is the volume floor, and that is the whole gate.** Tripping needs
`fail / (success + fail) > threshold` over `requestVolumeThreshold` requests, so a failure count at
or below `requestVolumeThreshold × threshold` cannot trip at any success count. That bound needs no
cross-window state, so it cannot go stale. The previous gate widened it with
`threshold / (1 - threshold) × success` taken from the last evaluation's total; the extra term only
pays off in a very-high-success window, and it cost a packed 16/48-bit watermark that had to be
maintained across every rotation and reset — the maintenance itself became the defect (see _Ruled
out_). Each bucket is one atomic read of a packed word, so the pair it contributes always comes from
one slice; a rotation between two buckets is inherent and bounded by the ring width.

**Probe credit is asymmetric, and the episode is the unit.** A failed recovery episode multiplies
the `HALF_OPEN` probe quota by 0.8; a recovered one by 1.1, both floored at 0.25 and capped at 1.0.
Per <em>episode</em>, never per probe: rewarding each probe success (×1.25) against one penalty per
failed episode made a failing episode net-positive — five successes and a failure _raised_ the
quota — so flapping widened it instead of tightening it. The ratio pair is deliberately not
reciprocal (0.8 × 1.1 ≠ 1): a source whose recovery episodes fail more than about one time in three
keeps tightening toward single-probe episodes, and one that recovers cleanly climbs back to full
aggression over a handful of episodes. The penalty or reward is computed into the same `Epoch` swap
that publishes the episode outcome, so the credit can never lag the state that earned it.

**Bucket contention is one packed word, not a monitor per slice.** The window index (24 bits), the
failure count (20 bits) and the success count (20 bits) live in one `long` in an
`AtomicLongArray`. The rotate-zero-increment is one compare-and-set, so a settlement that sampled
an older slice either lands before the rotation or is discarded, never erased by it; the rate scan
takes one atomic read per bucket and gets the index and both counters from the same slice. A
counter saturates at 1,048,575 settlements per slice, far above any plausible per-slice volume, and
cannot overflow into its neighbour. The 24-bit window index wraps after ≈ 4.7 hours at the minimum
1 ms slice and ≈ 194 days at the default 1 s slice. A settlement treats only a slot at most one ring
ahead as an out-of-order sample; any larger forward distance is a bucket behind the caller — the
previous slice or a bucket idle past half the id range — and is rotated to the caller's slice. The
residual ambiguity is the wrap itself: a bucket within one ring width before 2^24 slices is
discarded for at most that many settlements, and a bucket exactly 2^24 slices old contributes its
stale counts once before the next settlement rotates it. The
monitor per bucket was wrong about its own contention: the buckets are time slices, not thread
shards, so every concurrent settlement of the live slice targets the same monitor and the other nine
spread nothing; the rate scan additionally took ten locks. The packed word also removes the ten
`Bucket` objects and their per-object headers. _Ruled out:_ the monitor ring (2026-11, see _Ruled
out_).

Owner: `CircuitBreakerImpl`, `CircuitBreakerRegistry`, `ZetaProperties.CircuitBreaker`.

---

## WaveCounter: buffering boundary

**`WaveCounter` is a counter with a delivery contract, not a policy layer.** It routes keys by
heat into two paths: hot keys merge into a per-writer local map (no shared contention, bulk-
merged into the shared table every 128 increments) and cold keys write directly to a lock-free
shared table. One snapshot is delivered per tide; the tide pacer shortens the cycle under bursts
and stretches it when idle.

**The boundary is the design, and it must not leak.** Callers count a key and receive batched
snapshots. Nothing outside depends on the routing decision, the tide state, the promotion floor,
or the promotion-boundary selection. Its internals — heat-aware routing, the promotion-floor
governor, quickselect boundary selection, the beacon decay — are deliberately complex and are
not a refactoring target. Keep new policy out of it; if a caller needs to know _why_ a key was
promoted, that belongs in `HotKeyDetector`/`AccessCounter`, not in the counter.

Owner: `WaveCounter`, `AccessCounter`, `HotKeyDetector`.

---

## Counting engine (WaveCounter)

The App-side routing counter: how a count reaches the shared table, what the precision budget is per
path, and why the promotion boundary is the exact k-th largest rather than a threshold.

### The problem it solves

N writer threads increment per-key counters at high frequency; a periodic snapshot of distinct-key
counts must reach a downstream consumer. The workload is skewed — a few keys carry most of the
traffic — and the key universe is largely stable across cycles. The design budgets four resources:
aggregate throughput (multi-million ops/s at 16 threads), memory (bounded and flat under churn),
delivery latency (bounded), and a _documented_ precision model.

A single shared table would serialize every writer on the hot keys; per-writer buffering for every
key would multiply memory and delivery work. The answer is to partition the budget by heat.

### Heat-aware routing

Each count goes into one of two paths, both backed by the same `ConcurrentHashMap`:

**Hot path — writer-local, zero shared access.** Each writer merges hot keys into its own private
open-addressing map. The map is bulk-merged into the shared table once it holds `opMaxCount`
**distinct keys** (the trigger counts claimed slots, not increments — a workload with fewer hot
keys than the batch size never self-discharges and is drained by the next tide instead: fewer
merges, one-tide latency, the intended trade).

Per-op cost on this path:

- a pointer-equal last-key cache short-circuit, consulted _before_ the routing beacon, so a
  repeated-key op — the common hot pattern — never pays the beacon read;
- on a cache miss, the beacon test plus a `Ceils#drainStamp` read and a post-check (the lock
  rationale below);
- an atomic per-slot add (`AtomicLongArray.getAndAdd`, the price of the exact take-and-merge
  protocol).

The per-map `ReentrantLock` is paid only while a drain is actually in flight — µs-scale, at most
once per writer per tide.

**Cold path — direct lock-free increment.** The cheapest possible write, at the cost of a
documented approximate snapshot window (see _Correctness model_).

Promotion from cold to hot is a per-cycle decision: every delivery scans the snapshot and promotes
the keys that earn the top `hotLimit` slots, selected from that cycle's counts and floored at
`PROMOTION_FLOOR`. From the next cycle on, the key takes the exact hot path. Membership is
time-sampled: the counting beacon decays on every other promoted tide, so a key that keeps earning
the boundary stays hot while a drifted-away key leaves within four non-empty tides. **Promotion
gates performance only, never correctness.**

### Precision budget

Exactness is expensive in proportion to the time a writer holds a shared structure. The budget is
partitioned by heat: the hot path pays for exactness (in-flight merges are waited out), the cold
path pays for throughput (a bounded approximate window of ~1e-5/op). Sustained hot keys are
promoted and then take the exact path, so the approximation applies only to traffic that does not
justify exactness.

### Parameters are measured decisions, not operator knobs

**`opMaxCount = 128`** — the local batch size, the knee of a 64/128 sweep at 16 threads: small
enough that hot local data reaches the shared table within one tide, large enough to amortize the
shared-table add (~0.3 ns/op at 16 threads).

**The promotion boundary is the exact k-th largest count, not a threshold.** The top `hotLimit`
keys of the cycle earn the hot slots. A log2-bucket histogram of the snapshot locates the boundary
bucket (one O(n) pass with a leading-zeros log per key); the boundary value is selected inside that
bucket by quickselect, never a power-of-two edge. This is scale-free by construction: a relative
hot spot qualifies at any traffic volume, which a fixed absolute threshold cannot do (a low-volume
cycle's top key measured 0 promotions at 80 counts). `PROMOTION_FLOOR` (10) keeps noise keys
(1–9 counts/cycle) out of the hot set. A snapshot below `MIN_PROMOTION_KEYS` (16) distinct keys
skips promotion entirely — Caffeine's min-signal discipline; the decay reclaims any slots anyway,
so this guards against wasted work, not permanent pollution.

When the exact k-th largest cuts a tie-band wider than the remaining slots, promotion is
incumbent-first: renewing members are re-promoted before any newcomer. The pre-decay beacon state —
the only last-tide membership memory — is captured in `incumbentIdx` before the halving decay zeroes
it, so the capacity break never evicts a renewing key and the hot set stays stable under flat
distributions where every promotion would otherwise be a coin flip. Selection and split are
routing-only and cost zero per op; the deliverer pays the bucket pass, the quickselect and the
tie/blocked pass on scan tides (O(bucket) expected) plus the incumbent split on overflowing tides.
A stable-workload shortcut reuses the previous tide's exact boundary as a sweep filter, so
selection runs over the ~`hotLimit`-key list instead of the bucket whenever that list provably
holds the k-th largest; the flat-distribution worst case (a boundary bucket the size of the
snapshot) degrades to O(hotLimit) on stable workloads.

**`hotLimit = 1024`** caps the active promoted set so pathological traffic cannot grow it without
bound. The routing beacon is the compact 2+2 role-evidence array of `hotLimit × 32` rooms (≈16 KB
at 1024 — no key strings retained, so cold keys stay GC-eligible), with a ~0.37% false-positive
rate at full capacity. A false positive only routes a cold key onto the bounded hot path, whose
counts converge in the same table. The beacon is read on every count, so its size has a
cache-level throughput effect: 1024 (16 KB, L1/L2-resident) measured consistently fastest on a
16-thread sweep. The "capacity starves" penalty for limits below the hot-key count did not
materialize — the cold path carries un-promoted hot keys without measurable loss.

**The promotion floor is a renew-disambiguated governor (`MoonsTidalForce`).** The absolute floor
(10) is only a seed: the floor never filters above the histogram boundary. Healthy tides with keys
blocked behind the floor drop it toward the boundary so the renewing keys qualify. Distress — where
occupied hot slots earn less per slot than cold keys earn per key (the density-ratio evidence) —
arms a bounded raise-walk that filters the stale tail. An empty hot set collapses the floor to the
seed.

The raise is an evidence-based, reversible probe (goal verdict, budgeted undo, retry backoff): an
unconditional step would ratchet the floor past the whole distribution on rotating hot sets and
oscillate forever under key churn. Veto-return, audit clock and saturation release are defensive
machinery; the histogram boundary stays scale-free. Two volume-gated regime switches extend it: a
quiet regime (volume < `QUIET_VOLUME`, empty set) drops the floor to `QUIET_FLOOR` so any key routes
hot, and a raised floor blocking the whole incoming distribution at high volume collapses in one
tide (the flood collapse) instead of the wrong-direction raise-walk plus a slow decay-driven
collapse. The volume EWMA is folded once per non-empty tide by the deliverer; both switches are
wall-clock-debounced. Three refinements bound the probe machine's worst cases: a raise CONFIRM that
lands over-filtering corrects to the boundary immediately; a confirmed raise shields the re-arm for
`CONFIRM_SHIELD` tides (no alternating-workload ratchet); the walk budget binds on both verdict
branches.

**`deliverIntervalMs` (500 ms standalone default; `report-interval-ms` when the reporter wires the
counter), adaptive.** Delivery latency is bounded by one adaptive interval: a
tide that delivered ≥ 20,000 distinct keys re-schedules at 50 ms, scaling linearly back to the base
as the backlog shrinks — trading burst-detection latency against delivery overhead, self-damping as
the reservoir drains. The ramp is driven by `TidePacer`: the raw backlog is folded into a smoothed
reference with fast-attack (bursts shorten the cycle immediately) / slow-release (the fast cadence
drains over ~5 tides instead of snapping back) asymmetry and a still band (in-band jitter moves
nothing). Consecutive empty tides stretch the cadence up to 2× the base — the empty-tide ladder, so
idle cycles stop paying quiescence, decay and wakeups at the base rate — reset by any non-empty
tide. An external cadence owner (the reporter's feed-loop tuner in apply mode) bypasses the ramp
entirely: its base is the non-empty delay verbatim, because the loop scores a rate onto the input it
applied and two controllers cannot own the same actuator. The ladder still applies; ownership never
returns (design decisions: reporting).

**`SNAPSHOT_QUIESCENCE = 1 ms`** is the price of the cold approximate window. It is paid when the
shared table received any write this cycle: `coldWriteSeen` is set by every insert path — cold
first-inserts and every merge (read-gated, so only the first mark of a cycle pays) — and captured
and cleared at the swap. Entries inserted by any path (including hot-path drains) are visible to
cold hit-writers, whose adds need the same window, so only a cycle with no shared-table writes
skips it — and its old table is empty anyway. The wait parks instead of spinning, so the cost is a
yielded core for most of the window, not CPU burn.

### Correctness model

**The hot path is exact.** Per-slot counts are atomic (`AtomicLongArray`): the writer's update is a
`getAndAdd`, and every drain (tide sweep, batch discharge, tag-driven reconcile) takes the whole
value with `getAndSet(0)`. The two are serialized per slot, so a slot is never torn and a "taken"
slot reads exactly 0. A racing update that lands on a taken slot observes the 0 return and recovers
its delta exactly via `Ceils#recoverZero` — the prior value is provably already in the shared table
— so the hot path is never lost and never double-counted; the residual is precisely the un-merged
delta.

The shared-table reference is captured under `reservoirGate`, and the in-flight slot is reserved
atomically with the capture, so the deliverer waiting for `mergesInFlight` to reach zero can never
snapshot a table that a captured merge still targets: no in-flight hot add can be stranded. A
non-atomic read-modify-write loses exactly one delta when a take lands between the read and the
write-back (~3e-6/op on the tryLockSkip stress); the atomic protocol closes it.

**The cold path is approximate.** A cold writer that captured the table reference just before the
tide swap may write into the old table after the snapshot. The 1 ms quiescence window reduces this
to a preemption of > 1 ms — measured on deliver-racing and slow-consumer stress: typical loss 0,
worst observed ≈2.1e-5/op. The window is skipped only on cycles with no shared-table writes, whose
old table is empty anyway. With the window paid, residual loss requires a preemption > 1 ms; a
miss-path writer preempted across the swap re-targets the new table via its `computeIfAbsent`
re-read. Sustained hot keys are promoted and then take the exact path.

### Delivery

A self-rescheduling flusher — a one-shot tide that re-arms itself at a backlog-adaptive delay —
merges every writer's hot local map, swaps the shared table, waits for in-flight merges plus the
gated quiescence window, snapshots the old table into a single map and delivers it once per cycle.
The snapshot carries the measured swap-to-swap window, and the consumer receives it before the
promotion pass: O(n) promotion work cannot add to the delivery latency the consumer sees, and the
next delay is computed after delivery, so an apply-mode tuner's new base arms the very next tide.
Dead writers' registry entries are removed, and their residual local counts are merged first.

### Memory

The shared table holds exactly the live key set (no per-batch duplication); each writer's hot local
map is bounded by `opMaxCount`, and the hot set by `hotLimit`. Cold- and hot-path `LongAdder`s are
recycled across cycles from the previous tide's drained table (`ebbReservoir`), so a stable key
universe allocates no adders per cycle. The pool is bounded by one cycle's key universe and held
behind a single `SoftReference`: under memory pressure the GC may clear it, degrading reuse to
fresh allocations for that cycle — an optimization, never a correctness contract.

Per-writer `Ceils` maps follow the same discipline: dead writers' drained maps are pooled for the
next writer (`ceilPool`, capped at `CEIL_POOL_CAP`), so per-request-thread deployments allocate a
local map at most once per pooled wave instead of once per request. Memory therefore tracks the
concurrent hot-writing thread count (~7 KB per live thread that ever counted a hot key), not
cumulative churn — a large but live writer pool retains one map per thread until the thread dies
and the tide reaps it.

### What not to change

- The hot path's take/merge protocol is the correctness argument; a "simpler" read-modify-write
  reintroduces a measurable delta loss.
- `opMaxCount`, `hotLimit` and the quiescence window are measured knees. Changing one is a
  measurement task, not a style edit.
- Promotion, the beacon and the floor governor are routing decisions. They must never be consulted
  for a correctness decision — the counts in the shared table are the truth.

---

## Worker pipeline

**One detection path.** `ReportConsumer` validates and fences a report batch, then feeds every
`(key, count)` to `DetectionPipeline`: the shared `SlidingWindowDetector` adds the count, the rule
resolver is consulted only when the window sum reaches the promotion floor (or an exact rule's own
threshold), and `HotKeyRegistry` owns the HOT seat, its decision version and the cool-down wheel.
`DecisionDispatcher` drains HOT/COOL decisions through the single `WorkerBroadcaster` funnel.
`SamplingReclaimer` bounds the window table, `RuleSet`/`RuleResolver` own the rules, and the
wheel owns cool-down scheduling. A rule's `hotCount`/`coolCount` and the window sum are the
decision inputs; there is no per-key Bayesian state, momentum history or CV ring between the window
and the decision.
**The admission bound is one locked pass.** `SamplingReclaimer.tryAdmit` owns the cap, the
rejected/evicted counters, the relief signal, the saturation window and the Clock-style sweep
hand. A new key at the cap takes the sample lock with `tryLock`, walks up to 32 table entries and
applies the caller's eviction predicate inside that section; the predicate re-checks the candidate
under the guard that serializes it with mutations and removes it only when idle. The predicate's
first test is a lock-free read of the candidate's `lastSlice`: an active window is rejected without
taking the entry monitor or the map bin lock, so a sample over a table full of hot keys cannot
serialize 32 unrelated report threads on their own entries. Only a stale candidate takes its own
monitor, where the idle bound is re-checked and the tombstone is written; the mapping is then
removed with an identity-matched `windows.remove(key, window)` after the monitor has been released,
so the monitor stays a leaf and no `compute` lambda is allocated per victim. The predicate reports
the tombstone, not the removal, as its verdict: a `remove` that returns false means a concurrent add
already replaced the mapping with a fresh window, while the seat this pass tombstoned is the seat
its own admission consumes. One pass both decides and reclaims, so the caller supplies no victim
list. The lock order is reclaimer → entry monitor, then the CHM bin lock after the monitor is
released; no path holds the monitor while taking a bin lock, and none takes the reclaimer lock from
inside a bin lock, so the two orders cannot deadlock. Because two passes cannot overlap, the newest pass is the only evidence and
the saturation verdict needs no epoch. A pass that removes nothing sets the 50 ms saturation window
for the arrivals behind it — but only while the seat ledger is still at the cap: the periodic sweep
does not take the sample lock, so it can release seats during the walk, and a pass over the
survivors must not report a saturation that no longer holds. That same empty pass arms the relief
signal for the accelerated sweep: without it, a trickle of new keys slower than one per 50 ms keeps
spending the hard headroom while the periodic scan stays at its slow cadence, until the ledger is
exhausted. Below the hard bound, the pass's own admission reserves one hard-headroom seat **before**
the walk: the thread that paid for the walk is not the one admission the ledger should refuse while
its contenders pass, and the window it just opened is what suppresses those contenders. A pass that
removes a seat clears the window and admits; every reclaimed seat returns to the ledger because the
admission already owns its reservation. A contended admission that bypasses the lock therefore
cannot take the seat this pass reserved; the seats the pass frees are handed back to the ledger for
the contenders. At the hard bound there is no seat to reserve, so the pass runs without one and
admits only from what it reclaims. The predicate
must therefore return true only when this pass tombstoned the candidate and released its seat. The
sweep hand is weakly consistent and can still yield an entry another path already removed; treating
that ghost as a reclaim would inflate `capacity.evictions` and spend the hard-headroom budget on a
seat this pass did not free. The hand
resumes where the previous pass stopped and is bounded by `min(sampleSize, map.size())`; an
exhausted hand is rebuilt at the head, and the periodic sweep drops it because a CHM iterator pins
the table array. A contended admission (`tryLock` fails) proceeds unless the ledger is at the hard
limit: the holder may be releasing the seat it needs, and "no pass ran" is not evidence that
nothing is idle. The detector's `LongAdder` rejection counter spreads flood-time writes across
cells, the relief/saturation flags are read before they are written because the sweep and the
per-seat admission would otherwise bounce one cache line to store a value they already hold, and
the rejection line's expensive `sum()` and varargs are built only when DEBUG is on. Owner: `SamplingReclaimer`, `SlidingWindowDetector`.

**The soft cap has a real hard headroom: a seat ledger.** A completed pass that reclaims one seat
clears the saturation window, and the admissions that lost its `tryLock` proceed; that is the right
default because the holder may be releasing the very seats they need. But if every round finds one
newly idle window, those contended admissions add more than the pass released, and a pre-check of
`ConcurrentHashMap.size()` cannot stop them: many threads see the same under-limit value and all
create. `SamplingReclaimer` therefore reserves one atomic seat per window before creation; the
reservation is consumed by the created window or released when a race finds an existing one, and
removal returns the seat under the entry monitor. The soft cap plus `max(8, 1%)` is the ledger's
hard limit, so the table cannot grow past it under any interleaving. The pass's own admission is
covered by one reservation taken before the walk: a bypassing admission cannot take that seat, and
every seat the pass reclaims is returned to the ledger. A key that already holds a HOT
registry seat also takes a seat without the bound — a hot window's count feeds the LOW_QPS
cool-down, and losing it would make a live hot key look quiet; the overshoot is bounded by
`max-active-hot-keys`. The ledger initializes from a prefilled table for test/embedding callers.
Owner: `SamplingReclaimer`, `SlidingWindowDetector`.

**The failure this closes (the sampler refused after paying for the walk).** The pass used to sample
first and call `reserve` only after releasing the lock. While it walked, contenders that lost
`tryLock` reserved the remaining hard headroom; if the walk found nothing idle, the sampler's own
reservation then failed at the full ledger. The one thread that had just scanned the table was the
one admission refused, which made the cap look arbitrary under contention. Reserving the seat
before the walk closes the window; each reclaimed seat is returned, so the reservation is the only
seat the sampler can consume. Owner: `SamplingReclaimer.tryAdmit`.

**The fast path reserves below the cap, never from the headroom.** The below-cap admission used the
same hard-limit CAS as the sampling path. A burst that all observed the ledger one seat below the
soft cap therefore spent the entire hard headroom without running one pass; the next arrival was
rejected from a table that could still hold reclaimable idle windows. The fast path now CASes only
below the cap: an arrival that loses the last below-cap seat falls through to the sampling gate, or
to its saturation window, so headroom is spent only by an admission that reaches the sampling path —
it walks the table, or loses the `tryLock` to the pass already walking it.
The sampling admission still reserves its hard-headroom seat before the walk. Owner:
`SamplingReclaimer.tryAdmit`, `SamplingReclaimer.tryReserveBelowCap`.

**The tombstone is the seat state, and the seat crosses the compute as a primitive.** A
`KeyWindow` owns its seat for exactly the interval in which `lastSlice != EVICTED`; eviction sets the
tombstone and returns the seat in the same entry-monitor section, and a second pass that finds the
tombstone cannot release it again. A report that sampled the old tombstone reference therefore
creates its replacement through the same admission gate as a first touch: the old seat is already
back in the ledger, and a direct hard-headroom reservation would bypass the cap sample and leave
reclaimable idle windows tracked. No separate `seatHeld` flag is needed: a boolean after the
24-byte `header + int[] + long` layout would round the entry up to 32 bytes, costing 8 MB at the
default one-million tracked-key cap. The admission verdict is likewise a `boolean`: the reclaimer
reserves the seat before `windows.compute`, and the remapping function — which the bin lock runs
exactly once — consumes it when it creates a window and calls `releaseSeat()` when a concurrent
admission already published a live one. That removes the two short-lived objects (the reservation
and its `AtomicBoolean`) that the admission path used to allocate per new key while keeping the
seat ownership explicit in the call's stack frame. The admission gate reads only the ledger; the
map size is read once per sampling pass, where the walk needs its bound. Owner:
`SlidingWindowDetector`, `SamplingReclaimer`.

**The failure this closes (tombstone reservation).** Before the tombstone branch entered
`tryAdmitAtCapacity`, it called `tryReserve` directly. A report that sampled an evicted window —
a normal race with the evictor, not a rare one — then bypassed the soft-cap sample and the
saturation window: above the soft cap it consumed hard headroom while reclaimable idle windows
stayed tracked, and a flood of such reports could push the table to the hard limit without ever
running the pass that would have freed seats. The previous branch reasoned that a tombstone's seat
was already returned, so the replacement was "a normal seat"; a normal seat is exactly a sampled
admission.

**The hard bound still gets one uncontended pass.** At the hard limit the seat ledger has no
reservation to hand out, but an idle window can still be reclaimed. `tryAdmit` therefore takes the
sample lock with `tryLock` once before rejecting: a pass that frees a seat admits the caller, a pass
that finds nothing opens the saturation window, and a contender that loses the lock rejects. The
pass holds no reservation at that point, so a contender that reclaims the same seat first can still
refuse the thread that walked the table; that is the price of the bound, and the 50 ms saturation
window keeps the walk from repeating. Owner: `SamplingReclaimer.tryAdmit`.

**The failure this closes (hard-limit lockout).** `tryAdmit` used to reject on
`usedSeats >= hardLimit` before touching the sample lock, on the premise that the periodic sweep was
the only recovery path there. With the default 30 s sweep cadence, a table at the hard bound but
holding windows that had just gone idle refused every new key for up to half a minute, even though
one bounded walk would have released a seat. The hard bound is still a true bound — the fallback
never reserves above it, and a failed `tryLock` rejects — but an admission is no longer blind to
reclaimable seats. Owner: `SamplingReclaimer.tryAdmit`.

**The repair re-links a lost check; the wheel is the only evaluator.** A scheduled check is taken
within the hand-off latency of its deadline, so re-linking inside that window would move a node
that is merely late and wake its owner for no reason. A check must therefore be overdue by a full
grace period (one second) before the sweep touches it. The sweep then calls `reschedule`, which
links a lost node into the due list and wakes the wheel owner; the owner applies the real verdict
and the real cool reason. A failed evaluation or a missed link is late by one sweep interval, so the
grace costs nothing the sweep cadence does not already bound. Re-linking rather than evaluating
keeps one answer to "who decides a cool-down": a second evaluator could move `nextCheckAt` twice and
report the wrong cause. A wheel owner that dies leaves its buckets without an advancing thread;
this pass cannot replace the owner, and the App's hot hard TTL bounds the keys it held. Owner:
`HotKeyRegistry.repair`, `TimerWheel.reschedule`.

**A cool-down deadline saturates instead of wrapping.** `HotKeyRule` and its published
`RuleBinding` compute every deadline with a saturating add, so a very large `cool-after` or
`low-qps-for` stays at `Long.MAX_VALUE` instead of wrapping negative and firing immediately. The
repair scan compares the deadline against `now - REPAIR_GRACE_MS` rather than adding the grace to
the deadline, so a saturated deadline stays future instead of looking overdue. Owner:
`HotKeyRule`, `RuleBinding`, `HotKeyRegistry`.

**A due check is evaluated once, under the stripe lock.** The wheel detaches an entry and calls
`evaluate`; between the detach and the stripe lock, a report can re-arm the same entry. `evaluate`
re-reads `nextCheckAt` after the identity check and re-links the fresh deadline when it is still in
the future. The entry is `UNSCHEDULED` at that point, so the call is a schedule, not a second bucket
move. The lateness sample is taken under the same stripe lock, from the deadline `evaluate` is about
to replace, so a concurrent re-arm cannot move the entry between the check and the metric. Owner:
`HotKeyRegistry.evaluate`.

**The LOW_QPS dwell clock is a real timestamp; the sentinels are outside the clock.** `belowSince`
stores the instant the key first dropped below the band verbatim, including zero and negative
monotonic coordinates. "Not below" is `Long.MAX_VALUE` and "retired" is `Long.MIN_VALUE`; neither is
a monotonic instant, so a key promoted before the epoch can start its dwell at `-9 900` and cool
after one second instead of waiting for the clock to reach a positive sentinel. A `now` of zero is
equally real. The retired tombstone stays internal to `HE`: the public `belowSince()`
accessor folds it into `NOT_BELOW`, so a diagnostic reader makes one volatile read and can subtract
without knowing the private sentinel; the registry's under-lock path still compares the raw field.
Owner: `HE`, `HotKeyRegistry.evaluate`.

**A shutdown stops the inner batch, not only the outer loop.** A wheel advance can leave a large
detached batch, so the owner checks `running` inside the drain as well as around the wait. A
per-entry evaluation failure is caught inside the drain, so one poisoned check cannot skip the rest
of the batch or force an extra wheel advance; the entry is unscheduled and overdue, and the repair
pass re-links it for the wheel to retry. The
constructor also tears down already-started owners if a later thread cannot start, because a
constructor failure never reaches `@PreDestroy`. Owner: `CoolingScheduler`.

---

## Heartbeat and health

**Epoch is node-local and file-persisted.** A per-Worker restart counter persisted to
`zeta.worker.epoch-file` (default `${java.io.tmpdir}/zeta-epoch-<workerId>`), so a restarted
Worker always has a higher epoch than its previous incarnation. The read-modify-write runs under an
OS file lock as well as the in-JVM monitor: during a rolling update two Pods briefly share the same
`hostPath`, and without the lock both read the same previous value and mint the same epoch — after
which each incarnation's decisions look like the other's and are silently skipped. A lock the
platform cannot provide, or one this JVM already holds through another channel
(`OverlappingFileLockException`, e.g. a DevTools reload or a second context), degrades to the
in-JVM monitor with a WARN: the monitor already serializes this class's own calls. The lock lives
on a stable sidecar `.lock` file, not on the epoch file: the epoch file is replaced by rename, so a
lock on its old inode would not serialize a process that opens the new one. Never compare epochs
across Workers. The file is machine-owned state: exactly one binary `long` (8 bytes), no text, no
comments, no parser. A file whose size is not exactly 8 bytes reads as absent and is replaced on
the next start, so a text-format leftover still yields a strictly increasing epoch from
`max(previous + 1, wall-clock floor)`. The floor is re-sampled after the OS lock, so time spent
waiting for another incarnation cannot mint an epoch below the clock, and a corrupt
`Long.MAX_VALUE` saturates instead of wrapping back to the floor. The write-failure fallback floors
at the value it just tried to write (`max(previous + 1, wall clock)`), so a clock that stepped
backwards cannot mint an epoch at or below its predecessor when the replacement file cannot be
written. Owner: `WorkerEpochStore`.

**The failure this closes (torn eight-byte overwrite).** The old write happened at offset zero of
the existing file and truncated to eight bytes. A crash in the middle could leave the file at
exactly eight bytes containing a mix of the old and new values, so the size check could not see the
damage and the next start read a corrupted epoch. The replacement is now a same-directory temporary
file, forced to disk and renamed over the target, so a crash leaves either the complete old value
or the complete new one. Owner: `WorkerEpochStore.writeEpoch`.

**The epoch file is not replaceable by a clock.** `System.nanoTime()` resets with the JVM, and
`TimeSource.monotonicMillis()` is `BOOT_WALL_MS + uptime`, so at the instant the epoch is minted it
equals the wall clock — and that instant is the only sample: `workerEpochCounter` is a write-once
`AtomicLong`, so a monotonic axis would protect elapsed-time arithmetic the epoch never performs.
Monotone across restarts is not a clock property at all; it is the persisted value. Without the
file the floor is bare `currentTimeMillis() * 1000` — the default `${java.io.tmpdir}` path is wiped
with the container, so this is the default posture unless a volume is mounted — and a value already
broadcast stays high after a clock rollback, a VM snapshot restore or a host that earlier ran ahead,
so restarting stops being a remedy: every start re-derives the same low value. The App drops the
lower heartbeat, reports carry the stale epoch and are fenced by `ReportConsumer`, decisions fail
`isCurrentIncarnation`, and the record ages out of the liveness window — the restarted Worker is
effectively dead to the cluster. With the file, `max(previous + 1, floor)` makes restart the remedy
again. _Ruled out:_ a monotonic clock
(resets per boot, and is anchored to the wall clock at the one instant that matters); comparing
against "the last epoch" without persisting it (that value lives in the App's `HealthView` and in
queued report headers, neither readable at boot, and under a shared file it may exceed anything this
node ever minted); allocating epochs from Redis or etcd (one stateful dependency).

**The heartbeat carries state, not a bare ping.** Each heartbeat message carries the sender's
`epoch` and the `ready` cold-start flag, so one periodic message serves restart detection and the
health gate on the same cadence. A payload-free ping would need a second exchange and a second
schedule for facts the App reads together. The loaded rule set's fingerprint stays on the local
detection endpoint: no component compares hashes across heartbeats, and the endpoint already
answers the operator's question next to the rules it describes. The producer is a pure message
builder; `WorkerScheduling` owns its cadence together with every other periodic task, so no
producer starts work during bean construction. The heartbeat task starts immediately and runs at a
fixed rate: the first **successful** beat anchors the ready grace without losing one interval, and
an AMQP send's duration does not accumulate into the next period. Anchoring after the send is what
keeps a broker outage at startup from consuming the grace before the Worker can receive reports; a
failed attempt leaves the anchor unset, so the next successful beat starts the clock. The repair
sweep keeps a fixed delay because its run time is work the next cadence should not absorb. Owner: `WorkerHeartbeatProducer`,
`WorkerHeartbeatMessage`, `WorkerAutoConfiguration.WorkerScheduling`.

**A heartbeat is tenant-scoped at the receiver.** The topic exchange and the `heartbeat.*`
binding are global, so every App on a shared broker would receive every Worker's heartbeat and
put a foreign Worker into its ring. The heartbeat therefore carries the sender's `zeta.app-name`
alongside epoch and readiness; the App's heartbeat listener applies the same `AppIsolationFilter`
as the sync and decision planes before recording it. A heartbeat without the header (a
pre-isolation Worker during a rolling upgrade) is still processed. Owner:
`WorkerHeartbeatProducer`, `ZetaAmqpAutoConfiguration.WorkerListenerConfiguration`.

**Health is one third of observed Workers.** Healthy when at least `ceil(observedWorkers/3)`
are alive, floored at 1. `observedWorkers` is every Worker id ever seen in a heartbeat record:
records are added on the first heartbeat and replaced on a newer epoch. `HealthView.removeRecord`
exists for an external caller that has confirmed a Worker dead, but no production path calls it, so
the denominator never shrinks on its own and a transient blip cannot collapse the threshold.
`zeta.local.heartbeat.min-alive-workers` overrides it with an absolute count. A single surviving
Worker is deliberately healthy: report traffic is compressed and batched, so one Worker can serve
the cluster, and degrading on every partial failure would be worse. The override is folded into
the one `HealthViewImpl` factory behind the shared bean, and every consumer injects that bean
rather than building a private view: a locally built view would miss heartbeat updates and
silently disagree with the rest of the process. Owner:
`ZetaFacadeAutoConfiguration.defaultHealthView`, `ZetaFacadeAutoConfiguration.clusterHealthView`.

**The heartbeat fast path allocates, logs and invalidates nothing.** A same-epoch heartbeat
mutates the existing record in place, so no record or cache entry is created, and the
cluster-health cache is invalidated only when the Worker's liveness verdict actually flips.
The cached verdict is one packed `long`: bit 0 is the verdict, bits 1..42 the biased
computation instant, bits 43..63 the invalidation generation. Invalidation advances the
generation and clears the low bits, so a publish racing it cannot match the word it read;
the rare invalidation path allocates nothing. A join or re-join is logged after the record
lock is released, while the joined/re-joined decision is taken under it, which keeps the
report exactly-once under racing heartbeats. Owner: `HealthViewImpl`.

**The heartbeat clock sample is taken under the record lock.** Two heartbeats can arrive out of
scheduler order; sampling `monotonicMillis` before acquiring `recordLock` let the older sample
win the lock second and move `lastAnyHeartbeatTime` backwards, and a same-epoch record could be
overwritten with an older `ready`/arrival pair. The sample is therefore the first statement inside
the monitor, so the record mutation and the global watermark share one ordered instant. Owner:
`HealthViewImpl.onHeartbeat`.

**One record word carries readiness and evidence.** A per-Worker record packs `ready` into
the sign bit of one volatile `long` and the last alive-evidence monotonic instant into the
magnitude: non-negative means ready and holds the timestamp, negative means not ready and
holds `~timestamp`. One volatile read in `isAlive` settles both, so a reader can never pair
a stale flag with a newer timestamp, and the former separate arrival timestamp is gone —
the alive-evidence instant is the arrival instant. Owner:
`HealthViewImpl.WorkerHealthRecord`.

**Liveness windows adapt per Worker (Jacobson/Karels).** The effective timeout for a Worker is
the SRTT + 4×RTTVAR estimate over its heartbeat inter-arrival — the Linux `tcp_rtt_estimator`
RTO shape — clamped into `[adaptive-floor-ms, timeout-ms]`. The static `timeout-ms` demotes from
a flat threshold to the warm-up window (first 8 samples) and the ceiling; RTTVAR carries a
200 ms floor so the estimate cannot collapse, and the shrink branch decays with 1/8 gain (the
Eifel refinement) so one fast beat does not deflate the window. Only heartbeat arrivals feed the
estimator. Net effect: dead-Worker detection tightens from the full static window to a few heartbeat
intervals while jittery Workers get proportionally wider windows instead of flapping. The
estimator lives in the record itself; its published word is negative during warm-up (a
countdown from `-8` to `-1`) and non-negative once the derived timeout is trusted, so one
volatile read answers both "is it warmed up" and "what is it". Every consumer of liveness
(ring routing, health gate, Decision-Validity probes) shares the same per-record judgment.
Owner: `HealthViewImpl.WorkerHealthRecord`.

**The Worker's configuration is an auto-configuration, not a scanned `@Configuration`.** It lives
under the Worker application's scan base package, so a plain `@Configuration` was picked up by
component scanning before `@EnableAutoConfiguration`'s deferred imports registered the common
module. Its `@ConditionalOnMissingBean` factories were therefore decided against an incomplete bean
set — the fallback report converter could win over the common converter, and any future overlapping
bean would fail with a definition-override error. The class is now `@AutoConfiguration` ordered after
`ZetaAmqpAutoConfiguration` and is listed in the Worker's `AutoConfiguration.imports`; Spring Boot's
auto-configuration exclude filter keeps component scanning from registering it. Owner:
`WorkerAutoConfiguration`, `WorkerApplication`.

**The planes are isolated.** Heartbeats travel on a TopicExchange with ack NONE and a prefetched
queue; decisions use MANUAL ack with a small prefetch. Different loss semantics must not share a
queue. The control-plane exchange is declared from the publishing template before that
connection is handed to a publisher, because Boot's `RabbitAdmin` declares lazily and the first
publish would otherwise win the race on a cold broker. Do not register a second `RabbitAdmin`:
Boot's is `@ConditionalOnMissingBean` on the `AmqpAdmin` return type, so a second admin silently
switches it off and leaves every exchange, queue and binding undeclared. Thread pools are isolated
for the same reason: heartbeat emission runs on a dedicated single-thread `workerControlScheduler`,
while `hotKeyScheduler` keeps the periodic sweeps, whose walks over the window table and the HOT
registry can occupy a thread for tens of milliseconds.

**A data-plane configuration is gated by a data-plane factory, not by any `ConnectionFactory`.**
The `@Primary` heartbeat factory is itself a `ConnectionFactory`; a plain
`@ConditionalOnBean(ConnectionFactory.class)` therefore activates report and sync configurations
in a control-plane-only context and then fails while resolving the data-plane factory. The
condition and the resolver share one name lookup that excludes
`zetaHeartbeatConnectionFactory`. Owner: `ZetaAmqpAutoConfiguration`.

**The Worker's decision broadcaster owns a data-plane template.** `WorkerAutoConfiguration` defines
`zetaWorkerBroadcastRabbitTemplate` over `ZetaAmqpAutoConfiguration.dataPlaneConnectionFactory` and
injects it by qualifier; `ReportConfiguration` is disabled whenever `zeta.worker.enabled=true`, so a
Worker never builds an unfed `KeyReporter` and report/sync templates cannot collide with the
broadcast template. The previous shape injected `RabbitTemplate` unqualified. When a second Zeta
template existed (report enabled, or sync enabled) the context failed at startup with
`NoUniqueBeanDefinitionException`; when only the heartbeat template existed, HOT/COOL broadcasts
shared the control-plane connection, and broker flow control on a burst could block the heartbeat
channel and make the App evict a healthy Worker from the ring. `@Primary` stays on the heartbeat
factory for Boot's own template/`RabbitAdmin`; every Zeta data-plane bean resolves its factory
explicitly. Owner: `WorkerAutoConfiguration`, `ZetaAmqpAutoConfiguration`.

---

## Worker detection (standalone server)

**The promotion floor gates only the rules it can gate.** `RuleSet` derives `minHotCount` over the
default rule and every non-`EXACT` rule in its constructor, together with the precedence sort and
the fingerprint, so the three derived facts cannot disagree. Effective `EXACT` rules are indexed by
key in the published snapshot and consulted before the floor; a shadowed exact rule — one whose key
is already matched by a higher-priority rule — is omitted from the index because `resolve` would
never return it. The index is rebuilt from the published set's own rule instances, not from the
bindings: an unchanged rule keeps its previous `HotKeyRule` object, so an identity test against the
binding would drop that exact rule from the index after any other rule changed, and its keys would
silently fall back to the default rule's threshold. Without the split, one 10-QPS exact rule lowered
the floor for the entire key space: every warm key above 10 QPS paid a resolution and the bounded
match cache churned. An exact key below the floor pays one index lookup and its own threshold
decides; below that threshold the report is rejected before the match cache, so cold traffic for a
special key cannot occupy a cache slot or run a scan. An eligible exact key is returned straight from the index as its binding, not
inserted into the bounded match cache: the index is already O(1), and the entry would only evict a
prefix/regex answer that needs the cache. `resolveIfEligible` applies the governing binding's own
`hotCount` and counts exactly one match when it clears, for every rule type: `matches` is "reports
that cleared a rule's promotion threshold", not a cache miss and not a once-per-publication flag.
The cache and the exact index decide only where the binding comes from. The rule set, its match
cache, its exact index, its bindings and its non-`EXACT` scan array are one `Snapshot` behind a
single volatile reference; its maps and array are immutable, so a reader cannot mix a new rule set
with an old cache or an old counter. Owner: `DetectionSettings`, `RuleSet`, `RuleResolver`,
`DetectionPipeline`.

**Rule equality compares fields, not object identity or allocated text.** A `HotKeyRule` carries a
compiled `Pattern`, and `Pattern` does not override `equals`; two rule sets built from the same
configuration therefore compared unequal once any rule was `REGEX`, and `RuleSet.equals` (which
delegates to its rules) could never recognise an unchanged publication. `HotKeyRule.equals` now
compares every behavioural field and the configuration order directly, excluding the `Pattern` that
`type` and `pattern` already determine, so equality allocates no canonical string. The length-
prefixed `definition()` remains the fingerprint's canonical text: a delimiter inside an
operator-supplied id or pattern cannot make two different rules collide. Owner: `HotKeyRule`,
`RuleSet`.

**The tracked-key cap is priced at startup.** A tracked window is a doubled ring of
`2 × alignedSlices` longs plus a window object, a map node and the key string — roughly 350–400 B at
the default 16 slices. `DetectionSettings.warn` multiplies the configured cap by
`2 × alignedSlices × 8 + 96` bytes and logs a WARN when that exceeds half the max heap. The failure
it prevents is a 1M default on a 512 MB container entering a GC death spiral before the hard cap ever
rejects a key: the cap is a bound, not proof that the deployment fits. Owner:
`DetectionSettings`, `docs/CONFIG.md`.

**The per-rule match count is a counter, not a contended field.**
`RuleResolver.Snapshot.eligible` records one match for every report that clears a rule's threshold,
and eight report-consumer threads do that concurrently. When the HOT seats are full, every candidate
key's report lands on the same default rule, so a single shared `volatile long` made all of them
contend on one cache line. `RuleCounters.matches` is a Micrometer `Counter` whose underlying adder
spreads those writes, and promotions and cool-downs use the same kind of counter; `aborts` stays a
`LongAdder` because it is endpoint-only. The previous shape used a `volatile long` for `matches` too,
leaving the highest-concurrency counter on the lowest-concurrency update path. The `Counter` keeps
the adder's write spread and the endpoint on the same total the scrape reads. Owner: `RuleCounters`,
`WorkerDetectionMetrics`.

**A scrape reads one rule view, and a cache miss scans only rules that can win.**
`RuleSet.allRules()` is the single default-first list; the fingerprint, the resolver's binding map
and the endpoint all walk it instead of rebuilding "default plus rules". `RuleResolver.publish`
derives the ordered `RuleBinding` list once and a flat `RuleBinding[]` of the non-`EXACT` rules; the
match cache's loader scans that array and returns the default binding directly when nothing
matches. An `EXACT` rule is excluded from the scan because an effective one was already consulted
through the exact index before the cache, and a shadowed one can never be the first match for any
key — the rule that shadows it also matches its pattern. `RuleResolver.snapshot()` returns the same
`Snapshot` from a single volatile read, so a concurrent publication cannot pair a new floor with
counters it just retired, and a scrape allocates no view object. `WindowGeometry.toWindowCount` /
`toEffectiveQps` are the only QPS↔window-count conversions, and `HotKeyRule.firstDue` / `nextDue` /
`describeCool` define the cool-down arithmetic; `RuleBinding` keeps the promoted rule and the
counters of its id together, so the cooling check reads one entry object and never re-resolves the
rule. No caller keeps a private `Math.round`, integer division or `instanceof` chain. Owner: `RuleSet`, `RuleResolver`,
`WindowGeometry`, `HotKeyRule`, `RuleBinding`, `HotKeyEndpoint`.

**A promotion captures a rule binding, and a publication reuses what did not change.** The resolver
pairs each published rule with a `RuleBinding`: the immutable rule and the `RuleCounters` of its
id. The entry stores that binding, so the report path and the cooling check read the retained
rule's thresholds by field access instead of resolving the rule through the published set, and a
promotion, an abort and a cool-down advance the counters through the captured handles — no second
lookup through the rule set, the active-id set and the metrics maps, and no per-call lambda. Counters
are keyed by
id and survive a publication that retains the id; an unchanged rule keeps its binding (definition
equality is textual, so a recompiled regex keeps it too), so a threshold edit to one rule does not
rebuild the rest. An identical fingerprint **and an equal rule set** short-circuit the publication,
keeping the match cache; the fingerprint alone is a 64-bit filter, not an equality proof, so a
collision cannot suppress a real configuration change. Meters for newly added ids are registered
only after the whole snapshot is built, so a
publication that fails while deriving the exact index or the scan array leaves the previous snapshot
and its meter set untouched; binding while building left an orphan series for an id no published
rule owned. A removed id is retired: a late cool-down still finishes against its
captured rule, but its update lands on counters whose meters the publication already removed, so no
meter series is recreated. The endpoint reports the current rule set, so a retired rule's counters
are intentionally not carried forward. An id duplicated across the default and the precedence list is
rejected by `RuleSet.of`; otherwise two bindings would collapse onto one counter set, so a promotion
under one rule would advance the other's counters and the endpoint would attribute both to one id.
Owner: `RuleResolver`, `RuleBinding`, `WorkerDetectionMetrics`.

**Rule metadata is not bit-packed, and `HE` is not pooled.** A rule set is an
operator-authored constant (tens of entries), so packing priority, order, match type and strategy
into a `long` would save under a kilobyte while making startup validation and diagnostics opaque;
the compactness that pays is the per-check and per-report path, not the configuration. `HE`
is allocated per promotion and discarded on cool-down: its reference identity is the ABA guard
behind `hotKeys.remove(key, entry)` and the wheel's stale-event discard, so pooling it would let a
delayed event mutate a successor. The hot-path win comes from retaining the rule in `RuleBinding` —
thresholds are final-field reads, not resolver lookups — and from scanning only non-`EXACT` rules.
Owner: `HotKeyRule`, `RuleBinding`, `HotKeyRegistry`.

**A live hot key's window is admitted at the tracked-key cap.** The already-hot report path calls
`recordCountAtSlice`, which bypasses the new-key admission gate and the hard ledger: the key already
holds a registry seat, so losing its count would make a live hot key look quiet to
`COOL_FOR_LOW_QPS` and could cool it while traffic is still arriving. The overshoot above the hard
limit is bounded by the HOT seat count, and the tracked-key cap was already an approximate admission
bound. A brand-new key at the cap still reclaims an idle window first and is rejected only when the
sample finds none. Owner: `SlidingWindowDetector`.

**A resident window advances under its own monitor, not the CHM bin lock.** A slice crossing writes
only the absolute slice number, so re-entering `windows.compute` rehashes the key and takes the bin
lock once per slice for an entry that is already present. The entry's monitor serializes that update
with eviction: the evictor re-checks the idle bound under the same monitor, writes the
`lastSlice = Long.MIN_VALUE` tombstone, releases the monitor, and only then removes the mapping with
an identity-matched `windows.remove`. Only the call that wrote the tombstone reports the removal, so
a concurrent periodic sweep that tombstoned the same window first cannot make a second caller
release the seat. An add that sampled the old reference sees the tombstone and re-creates through
the admission gate and `compute`; an add that refreshed the slice first makes the evictor keep the
window. Creation is the only path that nests: `windows.compute` runs its remapping function under
the bin lock and may take the entry monitor inside it, while eviction never holds the monitor while
taking a bin lock, so the two orders cannot deadlock. Owner: `SlidingWindowDetector`.

**A slice number of zero is a real coordinate.** The window is created with
`lastSlice = now / sliceMs`, so zero — a test clock or a negative `nanoTime` origin — is a previous
write, not "never written". Only the `Long.MIN_VALUE` tombstone is special: no timestamp this
process can observe divides down to it, so it is distinguishable from every real slice. A negative
slice is a real coordinate too, so every comparison is between two absolute slice numbers, not a
truncated elapsed-time quotient: slices `-2` and `0` are two slices apart. Owner:
`SlidingWindowDetector`.

**The slice-gated fast path accepts one bounded loss.** A thread that samples `now` and then stalls
for longer than the eviction threshold between its map read and its slot write can land on an entry
the evictor removed; the count is lost, never attributed to another key. Closing that window would
require the add under the entry monitor (and, for a replacement, the bin lock), which is exactly the
cost the fast path removes. Eviction thresholds are minutes and a stall that long is already outside
the report path's latency assumption; the aggregate is a statistical signal, so one lost batch does
not move a threshold. Owner: `SlidingWindowDetector.addCountAtSlice`.

**One packed slot carries the count and its absolute slice.** A slot is a `long`: high 32 bits are
the low bits of the slice that wrote it (the tag), low 32 bits the saturating count. The tag is what
lets the add stay lock-free. A writer can sample slice `s`, stall through a later writer's
advance and full rotation, and then land in the slot slice `s + 2 * windowSize` reuses; without a
tag that stale count would be summed as current traffic two windows later. With the tag, the reader
checks the slice it expects and ignores the slot, and the later writer observes the old tag and
starts its count at zero. The stalled writer's CAS retry is the mirror case: when the witness tag is
ahead of its own slice, the slot has already been reused by a newer rotation, so the add drops
itself instead of replacing the newer count — its own slice is outside every reader's range, so
nothing visible is lost, while overwriting would erase a write the current window still sums. The
tag aliases only after `2^32` slices (about 49.7 days at 1 ms), far
beyond the maximum report-path stall; the doubled ring already guarantees a live slot is not reused
inside the `windowSize + 1` summation range. A parallel tag array would need two independent
updates, so the packed word is the smaller and the atomic shape. Owner: `SlidingWindowDetector`.

_Ruled out:_ zeroing the slots a rotation leaves behind under the entry monitor. It wrote up to
`windowSize` slots on a slow-path add and still could not distinguish a writer that stalled across
the clear from a current write; the later write survived and was read as traffic two rotations
later. The packed absolute-slice tag makes the write self-identifying instead. Owner:
`SlidingWindowDetector`.

**The window slots use opaque access outside the atomic add.** A slice is an independent
statistical counter: the summation needs per-slot atomicity and coherence, not a happens-before
edge between slots. Volatile mode paid an acquire barrier per slot on ARM64; opaque reads keep the
same single-slot guarantee, and a summation that races an add sees either value — exactly the
statistical tolerance the window already documents. The add keeps a CAS loop because the tag and
the count must change together and a `getAndAdd` followed by a MAX repair is not atomic: a
concurrent adder can observe the wrapped value before the repair lands. The slot starts at zero and
every write is positive, so only the upper bound is clamped; `compareAndExchange` returns the value
it observed, so a lost race reuses the witness instead of issuing another volatile read. The write
summation walks `windowSize + 1` slots and includes a slot only when its tag matches the slice it
would represent; a read-only sum ends at the key's last write and drops the empty slices after it,
so it walks at most that same span and never the silence that separates the key from now. The read
samples the clock before reading `lastSlice`: a write that lands after that sample makes the range
end at the newer write, while the reverse order could pair an old slice with a new clock reading and
return zero for a key still receiving traffic. Owner: `SlidingWindowDetector`.

**The window stores the absolute slice, and the report batch samples the clock once.** `KeyWindow`
holds `volatile long lastSlice` instead of a millisecond timestamp. `ReportConsumer` samples
`TimeSource.monotonicMillis()` once per message and derives the slice from it once;
`DetectionPipeline` passes the instant to `promote` and the slice to
`addCountAtSlice`/`recordCountAtSlice`, so a 500-key batch pays one clock read and one `floorDiv`,
not 500 of each. Every later comparison is integer arithmetic on the slice number:
`lastSlice < nowSlice` is the fast-path gate, `nowSlice & lengthMask` is the add index, `nowSlice -
lastSlice > windowSize` is the idle test, and the sum compares each slot's tag with the absolute
slice it expects. `evictStale(staleAfterMs)` converts the duration once per scan
(`staleAfterMs / sliceMs`) and the scan itself only subtracts; the retention is therefore quantized
to whole slices, at most one slice early or late, while every real retention (minutes) is orders of
magnitude larger. The cap-relief scan calls `evictIdle()` instead of converting a duration: it uses
the same slice-aligned idle predicate as the read path and the capacity sampler, so a guard slice a
reader still covers is never reclaimed. The batch sample may be older than a concurrent adder's
sample; the add keeps its own slice tag, and an add that fell behind the whole window is dropped
rather than moved onto the newest slice. Floor division, not truncation, keeps a timestamp before zero
in its own slice: `-1` and `-101` are two slices apart at `sliceMs = 100`, not one. Every write
walks the `windowSize + 1` slots; the ring is too short for a cached sum to be worth the staleness
it would introduce. A non-positive count writes no slot and admits no key: it returns before any
bookkeeping, and `addCountAtSlice` returns the key's window sum at the caller's batch slice — a
read, not a write; re-sampling the clock there would let an older batch sample read as a silent
window. `evictStale` rejects a negative duration instead of letting a negative slice bound match
every live window. Zero and
negative reports are a caller contract violation; ignoring them is what makes the slot invariant
hold — a slot starts empty, every write is positive, and an overflowing add saturates at
`Integer.MAX_VALUE`. Owner: `SlidingWindowDetector`, `DetectionPipeline`, `ReportConsumer`.

**The failure this closes (expired traffic moved to the newest slice).** `recordCount` used to take
`max(nowSlice, window.lastSlice)` as the write slice. A thread that sampled an old slice and then
stalled past the window had its count injected into the current newest slice, inflating the QPS the
rules saw and keeping a dead key's window fresh. The add now writes its real slice when it is still
inside the window and drops itself when it is not; the stored tag already handles a recycled slot,
and `addCountAtSlice` still sums to the later of the two slices so a concurrent advance is reflected.
Owner: `SlidingWindowDetector.recordCount`.

---

## Worker cooling

**The schedule is a hierarchical timer wheel whose node is the HOT entry.** Five levels of
`64 / 64 / 32 / 4 / 1` buckets with spans of `1 ms / 64 ms / 4096 ms / 131072 ms / 524288 ms`
(the bucket layout follows Caffeine's `TimerWheel`, in milliseconds because the Worker's clocks and
the `cooling.lateness` metric are millisecond-based). Placement uses the XOR prefix of the deadline
and the wheel clock: a deadline goes to the finest level whose enclosing span differs from the
current clock's. Every level therefore holds a disjoint, strictly later span than the level below,
so the first occupied bucket in level order is the earliest, and a cascade can only descend. When
that bucket's turn arrives, still-future entries move into a finer level, so the last hop always
lands on the 1 ms bucket and fires on the exact millisecond. A deadline beyond the horizon stays in
the top bucket and is re-evaluated on its rare rotations, which covers hour-scale `cool-after`
values without another level. _Ruled out:_ the relative-duration rule (the coarsest level whose
span covers the remaining delay). It mixes a duration test with an absolute tick index, so a
deadline 4095 ms out could land in the 64 ms level's current slot; the wheel then woke one
level-tick later only to cascade it, and `nextDelay` needed `peekAhead` because a coarser level
could hold an earlier deadline. Owner: `TimerWheel`.

**The wheel adds no per-seat object and no deadline field.** The node is the `HE`: two
references and one `int` link state, with `nextCheckAt` reused as the timer's variable time and the
field that held the validation token reused as the link state. The wheel's two links and state word
grew the entry by sixteen bytes. The `int` state word fills the object header's padding, so only
the two links add size. Removing that `int` would not give the four bytes back — the JVM
would insert the same alignment padding before the first `long` — while the link state would share
a word with `belowSince` and make a stripe-lock write and the owner's unlocked hand-off contend on
one CAS. The
value-based `CoolCheck` (32 B) and its queue reference slot (4–8 B) disappear, so the per-seat
footprint drops by roughly 30 B and a re-arm allocates nothing. One entry holds at most one scheduled check — a re-arm moves the node, a cool-down unlinks
it — so the queue's cap, depth counter, rejection metric and stale-check amplification all disappear
with it. Owner: `HE`, `TimerWheel`.

**The wheel state is written under the wheel lock, except the registry's hand-off.** `schedule`,
`reschedule` and `deschedule` move the node and set its state under the wheel lock; `advance`
marks a due node `DETACHED` while it moves it to the pending list. `pollDetached` only unlinks the
node from the owner's private batch and leaves it `DETACHED`; `HotKeyRegistry.evaluate` clears it
to `UNSCHEDULED` under the key's stripe lock before it evaluates or re-arms the entry. The field is
`volatile` so that one-way transition is published to a concurrent `reschedule`/`deschedule` that
reads it under the wheel lock. Every consumer of the state also holds the key's stripe lock (the
evaluation, cool and abort paths), and the batch is owned by the advancing thread, so the node's
links are never read by another thread while the hand-off runs. That stripe ownership is also what
lets `deschedule` check the volatile state before taking the wheel lock: the only paths that set
`SCHEDULED` run under the same stripe, so an entry seen unscheduled cannot become scheduled before
the lock is taken. A fired entry still reads as `DETACHED` after the poll, so a concurrent repair
defers to the owner; the evaluation then clears the state and the common cool-down path returns
without acquiring the wheel lock; the in-lock check remains authoritative. Owner: `HE`,
`TimerWheel`, `HotKeyRegistry.evaluate`.

**The failure this closes (repair stealing the hand-off).** `pollDetached` used to clear the node to
`UNSCHEDULED` before the registry took the stripe lock. A repair that acquired the stripe in that
window saw an unscheduled node, re-linked it into `pending`, and marked it `DETACHED` again; the
owner's evaluation then set the fresh deadline, called `reschedule`, and the `DETACHED` check
skipped the link. The node sat in `pending` with a future deadline, producing a spurious wake and a
second evaluation instead of one scheduled check. Keeping the hand-off `DETACHED` until the stripe
lock makes the two writers serial: repair either sees the detached node and defers, or runs before
the poll. Owner: `TimerWheel.pollDetached`, `HotKeyRegistry.evaluate`, `HotKeyRegistry.repair`.

**A stripe lock may take its wheel's lock; the wheel never takes a stripe lock.** Scheduling,
re-arming and descheduling happen inside the stripe section (stripe → wheel). `advance` detaches due
entries under the wheel lock and the owning thread evaluates them after releasing it, so the wheel
lock is never held while a stripe lock is taken. A detached entry is `DETACHED`: a concurrent
re-arm is deferred (the owner re-evaluates it and links the fresh deadline), and a concurrent
cool-down is discarded by the map identity check. Owner: `HotKeyRegistry`, `TimerWheel`.

**The wheels are striped by key, one advancing thread each.** A wheel's links and clock are
single-owner by construction, so `detection.cooling.threads` sizes a striped set of independent
wheels rather than a pool of pollers: a key is assigned by its hash, and different wheels advance
in parallel with no shared queue. Each wheel also owns its wakeup semaphore, released when a
schedule lands earlier than the thread's current sleep. The wait drains the permits under the wheel
lock before computing the next delay: a permit released while the owner was awake is already
reflected in that delay, and consuming it would only return immediately and burn a loop iteration.
A schedule arriving after the drain releases a fresh permit, so nothing is lost. Owner:
`CoolingWheels`, `CoolingScheduler`.

**A wake signal is a state, not a count.** `wakeAtMs` is `Long.MIN_VALUE` while the owner is awake
(or a signal is already pending), the current sleep target while it sleeps, and `Long.MAX_VALUE`
when the wheel is idle. Zero and negative monotonic instants are real — `TimeSource` is allowed to
sit below zero, and the wheel's own tests pin that — so the awake sentinel lies outside the clock
domain instead of colliding with a valid deadline. `advance` sets it to `Long.MIN_VALUE` as soon as
the owner takes over, and every schedule that would have released the semaphore first checks that
word. A mass-heat burst therefore collapses to one release: the first schedule releases and
publishes the sentinel, and the rest leave the permit alone instead of paying a CAS that the
owner's next `drainPermits` would throw away. The sentinel also covers the window between
`advance` and `awaitNext`, where the owner is about to recompute its target anyway.
Owner: `TimerWheel`.

**The failure this closes (negative sleep target).** The awake sentinel was `0`, and
`wakeIfAsleep` released only when `wakeAtMs > 0`. A wheel sleeping toward a negative deadline — a
valid monotonic instant before the epoch — therefore ignored a schedule for an earlier entry: the
entry was linked into the wheel, but the owner stayed parked until the old deadline. The check ran
late by up to the previous sleep, not at its scheduled millisecond. `Long.MIN_VALUE` cannot be a
clock instant, so the state test no longer overlaps the value domain. Owner: `TimerWheel`.

**The sleep is calibrated after the batch.** `advance` samples the wheel clock before the due
entries are evaluated, so `nextDelay` is relative to the batch start. `awaitNext` converts that
delay to a real deadline and sleeps only the remaining wall time; sleeping the full delay from the
later instant added the batch's evaluation time to every wait, and `wakeAtMs` described an instant
later than the actual wake, hiding a schedule that landed between them. The sleep target and the
wake state remain the same word. Owner: `TimerWheel`.

**A cascade starts after the previous tick.** On levels 0–3, the slot the previous advance stopped
on was consumed then and is empty by construction: `bucketIndex` never places a deadline in the
slot the wheel is currently consuming. Re-scanning it only pays a sentinel transfer, so the loop
starts at `previousTicks + 1` and scans `min(delta, buckets.length)` slots. Level 4 has one bucket,
so the mask wraps the start to 0 and its single slot is scanned. Owner: `TimerWheel`.

**Occupied bucket bits replace the per-level sentinel walk.** Each level has at most 64 buckets, so
one `long` per level records which slots hold a list. A link sets its bit; a cascade clears the bit
of the bucket it drains; an unlink cannot cheaply reach its bucket, so its bit is cleared lazily by
`nextDelay` when the sentinel is found empty. Because a level never sets a bit at or above its
bucket count, the search is two trailing-zero counts — slots from the current one to the end, then
the wrap to the head — with no mask and no `length == 64` special case. The stale bit is a bounded,
self-healing cost, while a missing bit would hide a deadline, which is why clearing is conservative.
`nextDelay` therefore finds the first occupied slot from the current tick without a pointer walk
over the empty buckets. Owner: `TimerWheel`.

**One wheel lock per due batch.** `advance` moves every due entry into `pending` under the wheel
lock and then appends `pending` to the tail of the owner's private `draining` list (a
`list_splice_tail_init`-shaped append, six reference writes and a sentinel reset). The advancing
thread drains `draining` without the lock: those nodes are `DETACHED`, so a concurrent re-arm or cool
leaves their links alone, and only the owner traverses them. A burst of N due checks therefore pays
one lock acquisition instead of N+1. Appending rather than swapping is what keeps a batch an
evaluation failed to finish at the head: the next advance's entries land behind it in arrival order,
so the following poll drains the remainder and the fresh entries in one traversal instead of
bouncing the remainder back through `pending` for an extra round. `nextDelay` returns zero while
either list is non-empty, so the owner retries an unfinished batch before sleeping toward the next
bucket; without that check, a batch left by a failed evaluation could sleep until the next occupied
bucket, or indefinitely with an otherwise empty wheel. The cascade itself resets the drained
bucket's sentinel and walks
the forward chain once, re-linking each node directly: no
scratch list, no `transfer`, and no per-node `unlink` whose four pointer writes would be overwritten
by the re-link immediately afterwards. Owner: `TimerWheel`, `CoolingScheduler`.

**The registry resolves the wheel from the key's hash at every evaluation.** `HotKeyRegistry.evaluate(entry)` computes the spread hash once and uses it for both the stripe lock and `CoolingWheels.select`, so a fired check costs one hash — the `String` caches it after the first report — and one array index. The scheduler therefore passes only the entry it drained: which thread fired a check is not part of the evaluation's contract, and the registry's shape does not depend on a caller happening to hold a wheel. `promote`, `abort` and `repair` resolve the hash the same way. Owner: `HotKeyRegistry`, `CoolingWheels`, `CoolingScheduler`.

_Ruled out:_ an `evaluate(entry, wheel, forced)` overload that let the scheduler hand over the wheel it was already advancing. The evaluation still hashed the key to find the stripe, so the parameter saved exactly one array index while adding a second public overload, a private four-argument variant, and a `wheel` argument threaded through `rearm`, `coolLocked` and `removeAndRelease`; the comment that claimed it avoided a re-hash described a saving the code never made. Storing the wheel on the entry was rejected for the same reason it always was: the extra reference grows every seat by eight bytes to save one modulo. Owner: `HotKeyRegistry`.

**A due check fires on its millisecond, and the removed machinery is the reason to use the wheel.**
A scratch benchmark (JDK 26, Epsilon GC, 10k/100k hot keys re-armed every second, 1 ms simulated
clock; the production `TimerWheel` linked against the worker classes) measured ~50 ns per
fire+re-arm with no allocation, versus ~150 ns and 40 B for the `DelayQueue<CoolCheck>` and
~137 ns for an intrusive indexed heap at 10k keys. At the default 10k checks/s all three are below
0.2 % of one core, so the choice is not the constant factor: the wheel deletes the check value, the
queue heap, the admission cap and the stale-check path, and its cost does not grow with the seat
count. Owner: `TimerWheel`.

---

## Decision delivery

**One funnel.** `WorkerBroadcaster` is the only writer to the broadcast exchange; the detection
pipeline and the cool-down scheduler submit through `DecisionDispatcher`, whose single drain thread
publishes through it. That is why the decision counters live there — emitted and dropped alike: the
funnel is the only place that sees whether a send reached the cluster, and counting at call sites
would count the same decision twice. The funnel distinguishes an actual send from an in-memory
suppression (`Delivery.SENT` / `ELIDED` / `SUPERSEDED` / `EXPIRED` / `FAILED`), so "it returned true" no longer
means "it reached the broker". `SENT` means the AMQP client accepted the publish call; no publisher
confirm is requested, so a broker acknowledgement is a stronger claim than the Worker can make.

**The decision version is allocated where the transition happens.** A decision's version is taken
by `HotKeyRegistry` inside the per-key lock that performed the transition, not by the broadcaster
at send time. Submission order is not transition order: a report-consumer or wheel thread commits the
transition (and its wheel re-arm) under the lock, then queues the decision, and the dispatcher's drain
thread sends it. Versioning at the send site let the thread that reached the wire first
win, so an older decision could overwrite a newer one on every App — the Worker believing the key
cold while the cluster kept it hot until the hard TTL. With the version fixed at the transition, a
decision that arrives with a version **at or below** the last one **accepted** for that key is
simply dropped at the funnel: an **equal** version is an elided replay of the state the cluster
already holds (`ELIDED`), while a **strictly lower** one was overtaken by a newer accepted decision
(`SUPERSEDED`); neither is a send failure. The two cases are distinguished because the caller must
not return a seat for the first: the newer transition owns the key's current seat, so the
dispatcher's rollback is identity-guarded for `SUPERSEDED` and skipped entirely for `ELIDED`.
Accepted includes a decision elided by the 100 ms type debounce: it never travels the wire, but it
is the state this Worker holds, and leaving it out of the accepted record let an older COOL queued
ahead of it through — the Apps demoted a key the Worker had just re-promoted, and nothing else
would have corrected it. A HOT dropped after its offer but before the wire marks the accepted record
cold without lowering its version, so the lifecycle's later COOL is elided as a demotion of a key
this Worker never told the cluster was hot, while a stale decision below the preserved watermark is
still superseded. A COOL with no accepted record at all is not a ghost: it is
the only demotion signal a locally promoted App can receive, and an earlier lifecycle's COOL may
have been lost, so it is sent. A cool-down COOL gets its version the same way: allocated under the
same per-key lock that removes the entry, so a re-promotion that lands afterwards outranks it.

**Only a HOT is coalesced against a queued decision, and against the registry's current
promotion.** The two queues can hold a COOL that has not been drained and a HOT from a
re-promotion of the same key. `DecisionDispatcher` keeps the newest queued **Pending** per key; an
older **HOT** with a strictly newer decision already pending is dropped under the bin lock _before_
the offer, counted as superseded and charged neither a queue slot nor a token. The identity-guarded
abort needs no accepted-record read: a newer decision proves a later transition for the key already
ran, so the entry this HOT created is gone or replaced. The registry is the second, independent
proof, applied by the drain: a report thread can stall between the promotion and its submission
while the key cools — or cools and re-promotes — and the demotion's own decision may be refused,
expired or failed, so neither the pending map nor the accepted record is guaranteed to remember it.
A HOT whose `version` no longer owns the key's entry is therefore dropped before the send (and at a
token-starved head), exactly like one behind a newer pending decision; without that probe the stale
promotion would be broadcast and nothing would ever cool the App but its hard TTL.
The registry does not track whether the dispatcher queued a promotion. A COOL for a promotion that
cooled before its submission is sent as a ghost; the window is the microsecond-scale gap between
the promotion and its submission, and a ghost COOL is a no-op for an App that is already cold. The
tracking required a per-entry flag and a late-HOT mark to cover the same window, and the flag still
missed the App's local promotion — a locally promoted App is hot without any Worker HOT, so the
funnel's empty record is not proof the cluster is cold. The drain keeps only the two facts the
funnel cannot observe: a newer pending decision (`SUPERSEDED_BY_PENDING`) and the registry's
currency probe.
The value in the per-key record is the queue item itself: a displaced item is marked through a bit
in its own hand-off state, so the drain reads one field instead of hashing the key a second time,
and the map does not box a `long` per submission. The bit is assembled under the bin lock and
published by the one plain write that precedes the offer. The item is invisible before that offer,
so a bit written there cannot race the drain and needs no atomic OR; a later `supersede` on an
already-visible item uses one. The state is a plain `volatile int` updated through a `VarHandle`
rather than an `AtomicInteger` (the item stays one small allocation). The bits are orthogonal: a
`QUEUED`/`OWNED` lifecycle pair and `SUPERSEDED_BY_PENDING`, so taking ownership is one
`getAndBitwiseOr(OWNED)` with no retry loop, and a concurrent `supersede` cannot invalidate it.
Claiming for the drain and withdrawing at shutdown are the same take: both only decide who reports
the terminal outcome. The queue offer and the map publication happen inside one per-key `compute`:
publishing after a separate offer left a window in which an older HOT was queued but not yet in the
map, so a newer COOL saw no predecessor, sent a demotion the funnel should have elided, and the
older HOT could still drain. The bin lock serializes submissions for the key, so a newer decision always sees the
older item before it can replace it; a refused offer returns the previous record untouched, so a
decision that never entered a queue cannot suppress one already on its way. The entry is removed
when the newest decision reaches a terminal outcome, so the map is bounded by the two queues. The
drain's removal is identity-matched, so it never erases a newer item, and the superseded bit lets
an older item skip the map removal entirely. An equal-version duplicate is still allowed into the
queue — both copies may be enqueued — but the map keeps the copy offered last, so a newer decision
still finds a predecessor until the last copy settles; the first copy's identity-matched removal
simply misses. The submitter needs no post-publication re-check: the
drain's remove cannot pass the bin lock that the offering compute holds, so it either runs after
the publication and removes the item, or the item was already displaced by a newer submission.

**A COOL is not elided on the strength of a promotion that never reached a queue.** The registry
commits a promotion under the stripe lock, then the report thread submits the HOT after unlocking,
so a cool-down can remove the entry before the submission. The COOL is sent anyway: the App may have
promoted locally, and the previous lifecycle's COOL may have been lost. Eliding it because the
promotion never entered a queue left such a key hot until the App's hard TTL, with no later decision
to correct it; the proof is narrower than the App's state and buys only one message. Owner:
`HotKeyRegistry.coolLocked`, `DecisionDispatcher.submit`.

**A HOT dropped after the offer records a cold lifecycle.** A key can cool immediately after the
offer; the drain's registry probe then drops the queued HOT as superseded, and the COOL may be
published after the pending map has forgotten it. The drain records a cold accepted record when a
queued HOT is superseded before the wire and no accepted record exists for the key. A later COOL
with no predecessor sees that record and is elided instead of spending a cool token and a wire round
trip; without it, every HOT dropped before the wire was followed by a redundant demotion of a key
this lifecycle never told the cluster was hot. `rememberCold` installs a cold record only when the
key has none; an existing COLD record already elides the lifecycle's COOL, and an `UNKNOWN` record
stays unknown because a newer failed HOT still owes a corrective COOL. `forgetCold` only clears the
cold knowledge, leaving the version and send anchor in place, so older decisions stay superseded
while a corrective COOL can still travel. The record is not marked cold when the superseding
decision is a COOL, because that COOL is the corrective demotion itself. A failed or expired HOT
keeps the conservative correction: its COOL is still sent, because the cluster may hold a state from
an earlier lifecycle whose COOL was lost; the same clear runs there. A hot record is never demoted: it keeps the ordering watermark. Owner:
`DecisionDispatcher.publish`, `DecisionDispatcher.applyOutcome`,
`WorkerBroadcaster.rememberCold`, `WorkerBroadcaster.forgetCold`.

**The failure this closes (version watermark regression).** `forgetCold` used to remove a cold
accepted record when a newer HOT failed, and the superseded-HOT path then installed a fresh cold
tombstone at version `0`. The key's accepted version fell from the last COOL to zero, so a delayed
HOT below the old watermark was no longer superseded and could be re-sent; the send anchor was lost
too, so the 100 ms debounce restarted. The record now keeps `(version, state, lastSentAtMs)` with
the state changing independently: COLD -> UNKNOWN clears only the "cluster is known cold" fact.
`rememberCold` never reverses that clearing, so a stale superseded HOT cannot elide the corrective
COOL of a newer failed HOT. Owner: `WorkerBroadcaster.AcceptedDecision`.

**A queued HOT never suppresses a COOL.** Suppressing the demotion on the evidence of a HOT that is
merely queued was a split-brain bug: the HOT can still expire, fail its publish or be discarded at
shutdown, and then the Worker has released the seat while every App keeps the key hot until its
hard TTL, with no later decision left to correct it. The COOL may be dropped only when the
broadcaster's accepted record already covers it — proof that a newer decision actually reached the
funnel. A COOL that merely wins over a still-pending HOT is not elided: that HOT may still fail,
and the demotion is then the conservative correction for whatever state the cluster already holds.
The superseded HOT also skips its cold record when that COOL is still pending, so the demotion is
sent whichever lane drains first. A hot accepted record never expires, so the first case cannot
degrade into a redundant send as a long hot episode ages. The cost is a demote-then-promote pair
only when the COOL is drained before its re-promotion is submitted; once the HOT is pending, the
drain lets it be accepted first (see the next paragraph) and the COOL is dropped as superseded.
Version order still converges every App to the Worker's final state, and a pair is cheaper than a
permanently split cluster. Owner: `DecisionDispatcher`, `WorkerBroadcaster.send`.

**The synchronous refusal's rollback belongs to the dispatcher.** `DetectionPipeline` hands the
`HE` to `DecisionDispatcher.submit(HE)`, which calls the abort when the queue offer is refused.
The pipeline never interprets a boolean and never calls `registry.abort` itself: a caller that
ignored the return value leaked a seat — eventually filling the seat cap and silently disabling
detection. The drain's own terminal-drop rollbacks are unchanged; only the synchronous branch moved
to the component that accepts or refuses the offer. Owner: `DecisionDispatcher.submit(HE)`,
`DetectionPipeline.onReport`.

**A superseded COOL yields one round to its newer HOT.** The drain normally sends COOL before HOT,
because a mass-heat wave must not delay the demotions that free seats. A COOL that is already
superseded by a newer pending HOT is not one of those needed demotions: the Worker's newest state is
promotion, and the accepted record would drop the COOL as soon as that HOT is accepted. Draining the
COOL first would therefore demote every App and immediately re-promote it in the same round. When
the cool head is superseded, the newest pending decision for its key is that HOT, and the hot lane
has a token, the cool head yields once. The HOT is accepted first; the next pass finds the COOL
below the accepted version and removes it without spending a cool token. The yield is bounded to
one round per head: if the HOT is deeper than the hot lane's burst, a second round sends the COOL
instead of letting one key block every unrelated demotion behind it. If the HOT expires or fails,
its pending entry disappears and the COOL is sent on a later round as the correction. Owner:
`DecisionDispatcher.drain`, `DecisionDispatcher.hasNewerHotPending`.

**The failure this closes (COOL-first churn).** The previous drain loop always ran the cool lane to
its burst before touching HOT. With COOL(v10) queued behind a re-promotion HOT(v11), that sent the
demotion first and the promotion in the same round. Every App evicted the key and rebuilt it for a
state that never changed, and the Worker paid a cool token for a decision the accepted record would
have dropped one instant later. The fix is deliberately narrow: only a COOL that is already
superseded by the newest pending HOT yields, and only when the HOT lane has a token, so a normal
COOL wave still drains first and a starved HOT lane cannot hold the demotions behind it.

**The funnel's in-memory state machine also clears the token-starved head, and it owns the age
bound.** The pending record ends when a decision reaches a terminal outcome, so a COOL drained
before an older HOT in the other queue left nothing behind to say the HOT was obsolete. The HOT then
sat at its head through every token-starved round — holding back every decision behind it — and was
dropped by the funnel only once a token arrived. The drain therefore calls `WorkerBroadcaster.classify` on any head the lane cannot send — an empty
bucket, or a COOL that must yield to its newer HOT — before the token gate, and commits it with
`commit` only after the queue removal and ownership take succeed. That method is the same
classify-plus-elide state machine the send path uses: an exact-version replay and a same-type
decision inside the debounce window are `ELIDED`, a strictly lower version is `SUPERSEDED`, and a
COOL for a key the cluster already holds cold is elided and advances the accepted record. Both
paths apply the
queue's age bound in the same pass, _after_ the state machine: only a decision that would
otherwise be sent can be `EXPIRED`, so an obsolete decision the funnel would elide is never reported
as expired — the old order checked age before classification on the send path and polluted the
expired meter (and its WARN) with decisions that were already no-ops. Every one of those outcomes
leaves the head without a token, and each uses the same meter bucket on both paths — an exact
duplicate is elided, not misreported as superseded. The send path now makes the same one pass: one
accepted-decision lookup and one clock sample classify the decision and apply the bound, and an
`EXPIRED` HOT returns its seat through the unguarded abort because classification proved no accepted
record at or above its version exists. The head check exists for the starved case; the send path
pays no extra lookup to perform the same classification. `acceptedVersion` remains for a pre-funnel
rollback with no funnel outcome of its own — the shutdown drain and an unexpected round failure.
Owner: `DecisionDispatcher.discardHeadIfTerminal`, `WorkerBroadcaster.classify` / `commit`.

**Shutdown spends only the budget that exists at that instant.** The drain spent a round's burst,
then refilled and continued, so a broker send slower than one token interval kept minting
replacement tokens and the thread drained the whole backlog until the join timeout — exactly the
opposite of the documented shutdown contract. `refill` is now a no-op once `running` is false; the
remaining queue is counted as dropped and HOT seats are returned synchronously. A submission that
passed the pre-compute check but waited for the per-key bin lock while shutdown began is refused
inside `publish` before it can touch its predecessor; the submitter reports the drop synchronously.
A drain-round failure after shutdown breaks to the final drain instead of sleeping through the join.
Owner: `DecisionDispatcher.refill`, `DecisionDispatcher.publish`, `DecisionDispatcher.drainLoop`.

**A lane is ready when it can act, not when no lane is starved.** The old `tokenStarved()` OR-ed
both non-empty lanes: a hot lane starving while the cool lane held tokens made `awaitWork` wait,
and on shutdown it returned false and dropped sendable COOLs. `hasReadyWork` asks the per-lane
question instead — a non-empty lane with a token, or a head the funnel would settle without one.
The terminal test mirrors the drain: a HOT with a newer pending decision or a released promotion,
and any head `WorkerBroadcaster.classify` reports as expired, elided or superseded. A cool head with
a token always counts; the next round sends it or yields once to a newer HOT the hot lane can send
in the same round, so `!hasNewerHotPending` must not turn a ready demotion into a false "nothing to
do". The check is repeated after the permit drain: a submission that makes a head terminal in that
window releases a permit this round would discard, and without the second check the terminal head
waited for a token it did not need. A round also reports the terminal items it processed,
not only the sends: a lane carrying more than one work-budget of elided, superseded or expired
items used to return `sent == 0` and park on a token those items never spend, leaving the rest of
the backlog to wait for the next refill. Owner: `DecisionDispatcher.drain`,
`DecisionDispatcher.awaitWork`, `DecisionDispatcher.hasReadyWork`.

**A COOL is elided when the cluster is already cold.** The two queues can hold a HOT that never
left the process (no hot token) and the COOL of the same lifecycle. The COOL is not superseded —
nothing newer is queued — but it carries no state change: the last decision the cluster accepted
for the key is already a COOL, so every App is cold and demoting it again spends a cool token and
a network round trip. `WorkerBroadcaster` elides it, advancing the accepted version and keeping the
send timestamp. A COOL after a HOT that failed or expired before the wire is still sent: the
previous lifecycle's COOL may have been lost, and that send is the corrective demote. A queued HOT
that was superseded before the wire is the exception: the drain records the cold state, so the
demotion is elided by the same accepted-record rule. Only the accepted record can tell the cases
apart, which is why the decision is made there and not from the queue contents. An elision stores an
immutable replacement that advances the version and keeps the send timestamp. Owner:
`WorkerBroadcaster.send`.

**The funnel answers every per-key question from one accepted-decision read.** Both `send` and
`classify` read the key's `AcceptedDecision` once and decide the supersede, duplicate,
redundant-COOL, debounce and send outcomes through the shared `decide`; `apply` is the one place an
elision advances the accepted version. The token-starved head therefore pays one accepted-decision
lookup per head, and the send path pays one per decision, not one per concern. The classification
carries that record into `commit`, so the two-phase head step performs exactly one read: a second
read could observe a Caffeine expiry the first did not and regress the accepted version to an older
decision. The old shape probed the cache
separately for "any accepted
record" and "the accepted version" before and after the send, so one decision paid up to three
Caffeine lookups and could still discard the exact fact — whether the accepted version was equal to
or strictly above the decision's — that decides whether a HOT seat may be returned. `Delivery` has
five outcomes; `NONE` is rejected at `DecisionDispatcher.submit` and never acquires a delivery
value. Owner: `WorkerBroadcaster`, `DecisionDispatcher`.

**The accepted record is time-bounded while cold, state-bounded while hot, never size-bounded.**
The record keeps one `long` version, one `long` send instant and a three-state cluster fact (HOT,
COLD, UNKNOWN); every replacement is immutable. A cold or unknown record expires
after `2 * broadcast.max-decision-age-ms`, and the cache sets no `maximumSize`. A decision admitted
at `t` is dropped at `t + maxAge` at the latest, so its key's cold record — created no later than
the newest accepted decision for that key — is still present when the older decision reaches the
funnel. A size cap would evict a live record under key churn and let an older COOL overtake a newer
HOT; tying the cold bound to the queue age makes the guarantee explicit. A record whose last
accepted decision is HOT has no time bound: it keeps the ordering and debounce watermark across a
hot episode that can outlive any queue age by design (`COOL_FOR_LOW_QPS` keeps a busy seat
indefinitely, `COOL_FOR_EXPIRE` can be configured in minutes). Expiring it would let a stale HOT or
an old COOL be re-sent; the App's version guard makes the replay harmless, but the send is wasted.
A COOL with no record is still sent as the correction. The record turns cold when the COOL is
accepted, and the cold bound resumes. The cold premise is that the producer submits a decision within
`max-decision-age-ms` of the transition that allocated its version; a producer stalled longer than
twice the bound is outside the report path's latency contract and is the same bounded loss as a
stalled window add. Owner: `WorkerBroadcaster.AcceptedDecision`,
`WorkerBroadcaster.AcceptedDecisionExpiry`.

**The failure this closes (wasted replay after a long hot episode).** Before the expiry became
state-dependent, every accepted record used `expireAfterWrite(2 * max-decision-age-ms)` (60 s at
the default). A key could stay hot for 65 s; its record expired; a stale HOT or an old COOL then
found an empty record and was re-sent. The App's version guard makes the replay harmless, and a
COOL with no record is now sent as the correction, so an expired hot record costs a redundant send,
not a stuck key. The state-dependent expiry keeps the watermark for the whole episode and avoids
that send. The cost is a record per key the cluster may still hold hot, bounded by the HOT
population plus lost first COOLs, not by the queue.

**Bounded buffer, drop on saturation.** `DecisionDispatcher` decouples ingestion from sending:
a mass-heat event must not stall the AMQP consumer behind synchronous publishes. One drain thread
serializes the sends (ordering is carried by the decision version, not by dequeue order), a bounded
`ArrayBlockingQueue` stops a sustained disconnect from growing without limit, and the handoff is one
value object plus one `offer` — no task object per decision and no `RejectedExecutionException`
built on the saturation path. A refused enqueue is counted by the buffer; a failed send is counted
by the broadcaster, and the per-type drop meter carries the volume while one DEBUG line per failed
round names the cause classes. Both producers — the report consumer and the cool-down scheduler —
submit through one `DecisionDispatcher`, which owns the buffered-versus-inline choice, so neither
carries a second send path. The detection-plane meters
are a non-null collaborator: a meter-less construction is a no-op instance rather than a `null`
every caller has to branch on. The constructor validates capacities, rates, bursts and the age bound
before it starts the drain thread, and requires every collaborator. A zero burst with a positive
rate would make `hasReadyWork` report work while `drain` cannot enter its loop — a 100% CPU spin —
and a null collaborator would otherwise leave an orphan thread behind the failed construction.
Owner: `DecisionDispatcher`.

**A failed COOL round does not spend a HOT on the same outage.** The drain sends COOL first, and a
broker failure there is global: pulling a HOT only to lose it in the same outage is a second drop
for no progress. The failure branch therefore skips the HOT drain, backs off 10 ms even when earlier
sends in the round succeeded, and exits immediately at shutdown. A queued item has a claimed /
withdrawn hand-off between `submit` and the final drain: whoever wins the CAS owns the terminal
outcome, so `submit` cannot report success for an item the shutdown drain silently discarded.

**An unexpected drain-round failure does not kill the delivery thread.** The broadcast's own
failure handling covers broker errors, but a runtime failure from a meter, a future collaborator or
a poisoned decision would otherwise escape the loop while `running` stayed true: submissions would
keep filling the queues, every HOT would be aborted on refusal and every COOL lost. The round is
caught and logged with its stack, the in-flight decision is charged to
`zeta.worker.decisions.dropped` for its type, the HOT returns its seat, and the loop retries after
the send-failure backoff. An exception after the funnel returned an outcome does not count a second
outcome or roll back a delivered promotion. Owner: `DecisionDispatcher`.

**A suppressed or expired decision costs no send budget.** The drain consumes a token, and a burst
slot, only when the funnel answers `SENT`. A decision elided by the 100 ms debounce, dropped as
superseded, or expired by `max-decision-age-ms` never touches the wire; charging it against
`max-hot-broadcasts-per-second` / `max-cool-broadcasts-per-second` or against the per-round burst
made a queue of stale versions starve the decisions that would actually be delivered. An expired
queue head is removed before the token gate, so a starved bucket cannot delay the HOT seat
hand-back, and the funnel's in-memory state machine clears a duplicate, redundant COOL or debounced
same-type head for the same reason: none of those can ever reach the wire, so leaving one at the
head would block a sendable decision behind it while the bucket refills. Terminal items do consume
a per-round work slot: a continuously refilled lane of elided heads would otherwise hold the single
drain thread inside one lane and delay the other lane indefinitely. The work bound is a fixed
per-round item budget, separate from the lane's send burst; it does not consume a send token or a
token-bucket budget. The idle drain blocks on a
semaphore released by every successful submission (and by
shutdown) instead of sleeping 10 ms per empty round; a bucket-starved round still waits for the
next refill while the Worker is running. A starved round first drains the permits accumulated by the
queue entries it cannot send. Each of those entries released a permit on submission, so without the
clear the timed acquire returns immediately for every backlog entry and the drain spins through
refill, head classification and expiry checks until the backlog's permits run out — a startup burst
or a starved lane turns into a CPU burn. A submission after the clear releases a fresh permit and
wakes the wait; one before it is either visible in a queue with a token, making the wait bound zero,
or is bound by the same token gate. The wait bound is zero for a queue that already holds a token
and a head: only the other queue is starved in that round, and charging this head its full age
bound would let the drain sleep through a sendable decision — the case that matters after a
`sent > 0` round has already drained the semaphore permits. At shutdown the drain is woken
immediately, spends the tokens already available, and breaks on the next starvation instead of
waiting for a refill; a
failed send while `running == false` also exits the retry loop, so the thread cannot spin until the
join timeout — the remainder is counted by the final drain.

**The failure this closes (starved semaphore).** Before the starved branch cleared its permits, a
startup backlog of 20,000 decisions left 20,000 permits behind. Every queued entry releases one on
submission, so `tryAcquire` returned immediately for each while the bucket still had no token; the
drain thread looped through refill, head classification and expiry checks until the permits ran out,
burning CPU for a burst that could never send. The fix is `drainPermits()` immediately before the
bounded wait. It is safe because a submission after the clear releases a fresh permit, while one
before it is visible in a queue whose token makes `nanosToNextEvent()` zero or is behind the same
token gate. A new submission during a starved period still wakes the wait once; that is the signal
that some other lane may be sendable, not a stale backlog permit.

**The token bucket's ceiling is the larger of one second's rate and the per-round burst.** A bucket
capped at `rate` makes a configured `burst > rate` unreachable: the round stops at the rate even
though the operator asked for the larger burst, and the setting silently does nothing. The ceiling is
therefore `max(rate, burst)`, which keeps the usual one-second allowance for every default
(`burst <= rate`) and makes a larger configured burst effective; the sustained refill rate and the
queue capacity remain the real bounds. Owner: `DecisionDispatcher.Lane`.

**A HOT that never reaches the broker returns its seat.** A decision is still sent once and never
retried, but the Worker's seat is not a published App state. If a HOT is refused at saturation,
expires in the queue, fails its publish, or is discarded by the shutdown drain, the promotion is
undone through `HotKeyRegistry.abort` — the same path a synchronous enqueue refusal
already used. A HOT superseded by a newer COOL needs no hand-back: that COOL's transition already
removed the entry and released the seat, so the identity-guarded abort is a no-op and the seat
ledger counts the COOL, not an abort. The abort is identity-guarded by the entry's `version`, so a later
promotion or COOL for the same key is untouched; a HOT that did reach the broker but was reported as
failed self-corrects on the next report's newer version. A duplicate submission whose exact version
already reached the funnel is not an undelivered promotion, so the accepted-version check leaves its
seat alone. Without the hand-back, `COOL_FOR_LOW_QPS` holds the entry
forever: its window stays above the cool band, every cooling check re-arms it, and the
already-hot fast path suppresses the next promotion — a permanent seat leak. The rollback is not a
retry loop; while a broker outage lasts, reports can re-promote, and the bounded hot queue is the
bound. Owner: `DecisionDispatcher`, `HotKeyRegistry`.

**Eviction goes through the same buffer.** The COOL the wheel produces is enqueued like every other
decision, so the cooling thread performs no AMQP I/O: a broker stall costs one failed `offer`, not a
blocked scheduler thread. The repair pass only re-links overdue checks; it neither evaluates nor
sends. The version is allocated under the per-key lock that removed the entry, so a re-promotion
that overtakes the queued COOL outranks it and the broadcaster drops the stale one. A refused
enqueue or a failed send is counted, the state stays removed — nothing restores it, no retry is
scheduled — and the entry converges through the App's hot hard TTL.

**A refused COOL is applied inline, never dropped.** The App-side `KeyDispatcher` can
refuse a decision (per-key queue full, global budget, closed). A refused HOT stays dropped — the
Worker never re-sends it, and inlining it would run the cluster-loader load on the AMQP consumer
thread. A COOL has no re-send either, and dropping it would pin the entry at its HOT TTL until hard
expiry, so the App applies it inline (`WorkerListener.onDispatchRejected`
applies it via `WorkerContext.coolNow` — the same in-memory compute as
`policy.cool`, hooks skipped so the consumer thread never waits on user code). Rejections are counted (`zeta.worker.decision.cool.rejected.total`,
`/actuator/hotkey` → `coolDispatchRejections`): a saturation signal, not data loss.

---

## Reporting

**Startup is a state, not a boolean.** `KeyReporterImpl` uses `NEW → STARTING → RUNNING` with
`FAILED` retryable and `STOPPED` terminal. A dispatcher that fails to start is shut down (both
consumer pool and watchdog timer), the state becomes `FAILED` instead of leaving
`started=true`, and a later `start()` may retry. A stop racing `STARTING` wins: the freshly built
dispatcher is closed rather than published. This keeps an optional reporter honest — a failed
leg is visible and recoverable, not a silent drop on every publish.

**The routing epoch is captured with the ring.** One flush reconciles the ring and reads every
alive node's epoch into the same `Admission`; per-key batches use that captured epoch, never a
post-routing `epochOf`. A Worker restart therefore drops the whole batch at the receiver's
incarnation check instead of re-basing pre-restart counts onto the new epoch. The epoch read is
fused with the liveness+epoch probe (`isCurrentIncarnation`): a Worker that restarted into its
cold-start window between the alive-set snapshot and the epoch read is omitted, so pre-restart
counts are not stamped with an incarnation that is not ready to serve. When every alive Worker is
still in that cold-start window the map is empty, and the flush is censored exactly like a
no-alive-Worker cycle: it is dropped (and counted in the dead-Worker ledger) instead of routing each
key through a predicate that can never match. Routing then uses the
captured epoch map as its liveness predicate: a concurrent ring rebuild may expose a new owner
without a captured epoch, and the key walks past it to a captured node instead of being dropped as
unroutable. Owner: `KeyReporterImpl`.

**Every timed-out, interrupted or shutdown-withdrawn batch is visible in both ledgers.** The queue
path counts a timed-out or shutdown-withdrawn batch in `DispatcherStats.dropped`; the path that
exhausts the shared offer budget while routing the remaining targets must do the same, or the public
dropped count silently under-reports exactly the stalls the dispatcher exists to expose.
`recordDropped` is the one increment for a batch the routing layer never offered. An interrupt
during the offer is a shutdown signal, but the batch is still lost, so `enqueue` advances the same
ledger before returning. The key-level counters
(unroutable, no alive Worker, queue-refused) are exposed as `RoutingStats` instead of living only
in log lines. Owner: `KeyReporterImpl.routeAndEnqueue`, `KeyReporter.RoutingStats`.

**The feed loop scores the unrounded projection.** Rounding the projected batch before the
target ratio loses the whole signal for a small target: 4.6 keys against a target of 5 becomes 5,
scores exactly `10000 bp`, and the loop stops moving. `scoreBp` therefore consumes the raw
`rate × interval` double, while the published `batchSize` remains a rounded display value (zero
for a completely idle window, not a floored one). The floor was also why a target of one could
never back off: every idle window looked on-target. Owner: `ReportFeedLoop.onFlushCompleted`.

**The bounded queue is the only backpressure.** Reports are aggregated by
`AccessCounter` and published in batches through a bounded dispatch queue; consumer threads publish
synchronously, so a broker that stops draining propagates back through the queue and nowhere else.
The queue's default (1024 batches) covers one 5 s staleness window at ~200 batches/s, and a batch
older than 5 s is discarded on dequeue: the backlog is bounded in count and in age together, and a
transient stall is absorbed rather than shed. A full queue blocks the offering thread for at most
`queue-offer-timeout-ms` **for the whole flush**, not once per routed Worker, and then drops every
remaining shard. The previous shape paid the timeout per target: on an N-worker ring one tide could
block for N × 100 ms and delay the local sketch merge behind it, letting the cold table fill and
drop new keys. The detector merge now runs before the reporter leg for the same reason — a remote
stall must not become a local-detection stall. Owner: `KeyReporterImpl.Dispatcher`,
`AccessCounter.onTide`.

**Reports enter through the report exchange; decisions leave through the decision exchange.**
`zeta.reportToWorker.exchange` is the only ingress for App reports, and each Worker binds its shard
queue `zeta.reportToWorker.<appName>.<workerNodeId>` to it. `zeta.send.exchange` is the separate
Worker→App decision channel. Both are `DirectExchange` beans, so every binding and injection names
the one it means (`@Qualifier` and name-scoped `@ConditionalOnMissingBean`); a type-only lookup can
select the decision exchange and leave the report queue with consumers but no ingress. Owner:
`WorkerAutoConfiguration.reportBinding`.

**The publish timeout is a permanent per-consumer slot, not a timer entry per batch.** Consumer
threads are fixed, so each owns one watchdog slot for its lifetime. A publish arms the slot
with one synchronized deadline write; the sole timer task scans the slots every 100 ms and
interrupts an expired one; the finally disarms and clears the interrupt. The handshake under
the slot monitor means an interrupt either happens before `disarm` returns or not at all, so a
cancelled timer task can never leave a stale interrupt for the next `take()`. The hot publish
path allocates nothing and never touches the timer's heap; a five-second publish just pays a
short overrun past its budget. Owner: `KeyReporterImpl.Dispatcher.ConsumerWatchdogSlot`.

**Dropping is the designed outcome.** A dropped batch is lost: `WaveCounter` swaps its table per
tide, so the next tide carries new counts, not the dropped ones — a sustained drop biases every
key's measured rate down by the drop ratio. That is accepted because the queue absorbs the
transient case, and a permanent stall would lose the reports anyway. Nothing retries a dropped
batch, and no publisher confirm is requested. Owner: `KeyReporterImpl`.

**Flush cadence is tuned by a feed loop in rate space.** The reporter's base interval is a
`damon_feed_loop_next_input` port, but its plant input and its observable are different units: the
input is the flush interval, the observable the arrival rate `batch / measured window`. The loop
projects that rate onto its own input and scores the projection against
`report-interval-target-batch` in basis points (`10000` = on target). Scoring the raw batch instead
makes `shadow` wind up — a shadow loop never applies its input, so every window is measured at the
configured interval while the loop believes the cadence is its own, and the integral diverges. In
`on` the measured window _is_ the input, so the projection collapses back to the observed batch:
one law, both modes. The rate is a first-order low-pass in wall-clock time (`τ = 250 ms`,
`α = 1 − e^(−Δt/τ)`), weighted by the time since the last accepted sample, so a short window earns
proportionally less weight and a long gap ages the old estimate out instead of averaging across
scales. An empty tide _is_ sampled, at rate zero: the flush happened and nothing arrived. Only
cycles censored by the delivery path (no alive Worker) are withheld.

**The step is the plant's exact inverse.** `next = last × goal / score`: a 1.9× batch asks for
1/1.9 of the input and a 20× batch for 1/20, with no proportional-to-floor cliff. A `±500 bp`
deadband holds the input through Poisson jitter; outside it the input moves at least 1 ms, so
integer-millisecond truncation cannot stall the loop; one step is rate limited to `[0.5x, 2x]`; and
the result is clamped into `[report-interval-min-ms, report-interval-max-ms]`. The floor is a
parameter because the kernel's `10000` is a microsecond value — taken literally it would be a
10-second floor for millisecond intervals. Censored cycles (no alive Worker) never
feed the loop: traffic existed but never reached routing. An empty snapshot is not censored — zero
is the measurement. _Ruled out:_ treating empty snapshots as censored. With SHADOW's fixed window
every accepted sample is at least `1/W0`, so the smoothed rate can never fall below one key per
window; SHADOW then pins at `targetBatch × W0` and ON never backs off from the high-traffic
cadence. The upstream `WaveCounter` delivers the empty window (and only `destroy` suppresses a
final empty batch).

**The feed loop owns the cadence in apply mode.** The first `adjustDeliverIntervalMs` call disables
the pacer's backlog-pressure compression for good: a controller that scores a rate onto its own
input needs the window it measures to be the input it applied, and two controllers cannot own one
actuator. The empty-tide ladder stays (an idle cycle still stretches to 2× the tuned base).
`shadow` keeps the pacer, so the default deployment behaviour is unchanged, and ownership never
reverts — the tuner is configured once at startup. _Ruled out:_ letting `TidePacer` and the tuner
both move the base — the measured window would stop being the applied input, which is exactly the
open-loop error the rate-space design removes; scoring raw batch sizes in `shadow` (winds up);
keeping the kernel's proportional-to-floor step (sawtooth). Owner: `ReportFeedLoop`.

**Cadence quantizes the detection window.** `addCountAtSlice` places a whole reported batch in the slice
of its arrival and `ReportMessage` has no span, so the Worker cannot spread a batch across the
interval it accumulated over. A 1 s window at an 800 ms cadence holds one or two batches — the
second can carry up to 800 ms of extra traffic, ~1.6× a steady one-second count — while the 50 ms
default holds ~20 and tracks closely. The window sum is a count-per-window estimate, not a
count-per-second measurement: when a threshold must mean counts per second, keep
`report-interval-max-ms` well below `zeta.worker.detection.window.duration`. Owner:
`SlidingWindowDetector`, `ReportFeedLoop`.

**The 100–800 ms operating point.** The example keeps every non-empty cycle inside the default 1 s
Worker window while the loop trades cadence for batch size:

```yaml
zeta:
  local:
    report-interval-ms: 200 # seed; keep it inside [min, max]
    report-interval-min-ms: 100
    report-interval-max-ms: 800 # idle worst case: 2 × 800 = 1600 ms, under 2× the 1 s window
    report-interval-target-batch: 512
    report-interval-tuning: on
```

`report-interval-ms` is the fixed base in `off`, the base the loop observes but does not move in
`shadow`, and the starting point in `on`. `min-ms` bounds the message overhead of dense traffic,
`max-ms` the first-report lag (doubled after an idle cycle), and `target-batch` is the projected
batch the loop converges to. The 800 ms ceiling still admits two batches in a 1 s window — the
overcount described above — so a threshold that must mean counts per second should keep `max-ms`
well below the window rather than merely at it.

**Reports are fenced by the target Worker's incarnation.** `ReportMessage.targetEpoch` captures
the target's epoch when the batch is routed, and `ReportConsumer` drops any report whose epoch
differs from its own (including `HealthView.UNKNOWN_EPOCH`). The durable report queue and the
App's bounded dispatcher both outlive a restart, so replaying pre-restart windows into a fresh
detector would confirm HOT from old traffic. The batch is stamped at routing time, not publish
time — otherwise a batch already in the dispatcher would be re-based onto the new incarnation.
The epoch is node-local and monotone at boot (`max(previous + 1, wall clock)`), so a restarted
Worker always has a strictly newer value. _Ruled out:_ purging the report queue at startup (misses
batches already held by the App dispatcher and races the listener container); wall-clock staleness
as the primary defence (cross-host skew, which is why the existing filter defaults off); relying on
the queue TTL alone (bounds the window but does not prevent a replay).

**Logs are plain SLF4J, split by category.** A per-operation failure — a failed send, a loader
exception, a batch that failed to apply — logs at WARN/ERROR on every occurrence, with the
throwable attached. A designed degradation or a load rejection — an executor refusing a batch, a
per-key queue full, a ring probe exhausting its cycle, an epoch-fenced drop, a malformed wire
body — logs at DEBUG, because the cumulative counter (or meter) is the operational signal and a
per-message line under a systemic condition is a log flood rather than diagnosis. No call site
throttles.

Owner: `KeyReporter`, `ReportFeedLoop`, `ReportPublisher`,
`ReportConsumer`.

---

## Version stamps

**One space: positive, monotone, node-local.** `dataVersion` comes from `StampClock`; one CAS
returns a value strictly above the previous stamp and never below the current wall-clock window.
Sequence overflow rolls the stamp into the next millisecond arithmetically instead of blocking, so
allocation has no clock-tick ceiling and never waits for the physical clock. At the very end of the
41-bit timestamp field — a 2094 problem, or a clock poisoned near that boundary — allocation
refuses with `IllegalStateException` instead of shifting the sign bit: one silently negative
version would be persisted and propagated cluster-wide, and a 63-bit positive space cannot offer
"unbounded allocation", "always positive" and "strictly increasing" at once. The bit layout is the
shared stamp layout (`1|41|2|8|12`, custom epoch), used for `dataVersion` values, so every version
compares numerically and decodes with `StampClock.timestampOf`. Message trace IDs are time-sortable
but order nothing, so they are minted from the cached `TimeSource` wall clock instead of the stamp
clock: a send never pays a CAS against the shared data-version clock and never burns a sequence of the
4096/ms window, which would compound the cross-node ratchet documented below.
_Ruled out:_ keeping a second Snowflake generator solely to mint message trace IDs — it duplicated
the same layout, epoch and node resolution behind a separate bean and a second lock-backed
allocation path, for a value that orders nothing.

**The nodeId-above-sequence layout is kept despite the cross-node window cost.** With the node id
above the sequence, a lower-id node that observes a higher-id peer's same-millisecond stamp
cannot sort above it in that window and rolls one millisecond forward — under sustained
bidirectional write storms the logical clock outruns physical time at roughly one millisecond per
raced write. Swapping the layout (`ts|seq|node`) would erase the ratchet, but it also erases the
property `nextStamps(count)` depends on: one CAS reserving a contiguous integer range
`[base, base + count − 1]` with the node bits constant across the block. The batched-invalidation
reservation is a correctness mechanism; the ratchet is a latency-and-skew tax that only binds
above roughly a thousand cross-node round trips per second, is bounded by the forward-drift
guard, and is now visible on the skew gauge. `getBase` expresses the whole choice as one
arithmetic tail: the target window is `max(floor timestamp, now)`, the block starts at
`max(ownBase, cur + 1)`, and a block that would cross the sequence ceiling moves to the next
millisecond. `ownBase` is always rebuilt from this clock's node bits, never from the floor's, so a
peer's identity can never be minted into this node's stamp. Owner: `MonotonicStampClock.getBase`.

**A local write is authoritative at its stamp, not over later ones.** `putLocal` allocates a
stamp and then competes in the same `compute` as every other write: it refuses to store when an
invalidation tombstone already outranks that stamp, or when a strictly newer resident exists. A
write descheduled between allocation and apply is causally older than a concurrent write and must
not rewind it — "local" describes the broadcast scope, not an ordering privilege.

**Node identity is configured, not just random.** The 22 low bits split into a 10-bit node id and
a 12-bit sequence, and a fresh window restarts the sequence at 0. Two nodes that resolve to the
same node id therefore mint identical stamps whenever they write in the same millisecond;
`shouldSkipForSync` reads equality as "already current" and skips both directions, so neither
peer applies the other's write until a later millisecond or TTL expiry. The default derives the
node id as a stable CRC32 hash of the instance id and logs it once when first resolved;
`zeta.local.node-id` (`0..1023`) makes the identity explicit and unique per instance. Every sync
message carries the sender's id in `originNodeId`, letting the receiver WARN once when a peer
shares its id. Instance id, JVM id and node id live in one `NodeIdentity` holder so queue naming,
message origins and stamp allocation read the same resolved identity; the derived node id is
cached per instance id, so a late `zeta.local.instance-id` override cannot leave a stale
derivation behind. The effective identity is one immutable `Identity` record — instance id and
the pre-boxed `Long` — fully resolved together and replaced as a unit under the class monitor,
so one volatile read carries a consistent snapshot and a reader can never pair a new instance id
with an old derivation or observe a half-resolved pair. The pre-boxed `Long` is the node id's
own instance, kept so the AMQP header path does not allocate one per message (`Long.valueOf` only
caches -128..127); `getNodeId()` unboxes that same instance. The two override flags are static
fields guarded by the class monitor, not part of the read snapshot: only the setters read them.
A redundant `setInstanceOverride(null)` (no override was ever set) is a no-op: the auto-detected
instance id and its derived node id survive instead of being invalidated and rebuilt, and the
"override cleared" line is not emitted for a state that never existed. Clearing a real override
invalidates the derivation unless an explicit node id owns it. The JVM id is exposure-only — no
stamp or trace ID embeds it.
Owners: `NodeIdentity`, `CacheSyncPublisher`, `CacheSyncListener`.

_Ruled out:_ allocating node ids by registering with the Worker cluster at startup. It couples
App initialization to the Worker plane and leaves the id ambiguous when an App starts during a
Worker partition; a configured id (or a unique instance id) is local, visible in the startup log,
and has no availability dependency.

**The clock merges observed stamps, within a forward-drift bound — and reports acceptance.**
`observe(stamp)` raises a local floor to the highest accepted stamp and returns whether the stamp
was accepted. Without the merge, a node whose wall clock lags loses every conflict permanently.
The merge is bounded by `zeta.local.clock-max-forward-drift-ms`
(default 60s): a stamp that leads local physical time by more than that is refused (DEBUG line) and does not raise the floor. This is the anti-drift rule mature HLC implementations apply
(uhlc defaults to 500 ms); Zeta's bound is looser because its LWW contract already tolerates
bounded skew, and the guard targets broken clocks rather than ordinary NTP drift.

The refusal is a contract the receiver must honour, not a silent no-op. A refused stamp is by
definition above every stamp this node will mint until wall time catches up, so persisting it as
a `dataVersion` blinds every later local write to that key — the version guard rejects each one
for the whole lead duration. The sync receiver (`CacheSyncListener`) therefore re-stamps a
refused message with a freshly allocated local stamp before dispatching it: the mutation still
applies, the ordering claim against a broken-clock peer degrades to arrival order (the only order
left), and the node's monotonicity invariant holds end to end. The skew gauge reads the highest
peer timestamp ever observed — refused outliers included — so the one anomaly the gauge exists
for is visible instead of reporting a healthy 0. _Ruled out:_ merging without a bound — one peer
with a clock hours ahead raised every node's floor, and every local write stayed numerically
below it until physical time caught up. _Ruled out:_ persisting the refused version next to an
unraised floor — that combination is the write-blindness above.

**Batched allocation reserves one contiguous block.** `StampClock.nextStamps(count)` reserves
`[base, base + count − 1]` with a single CAS and returns the base. No concurrent allocation —
single or range — can land inside the block, so a concurrent single write is ordered against a
whole batch at once: its stamp is either older than every batch stamp or newer than every batch
stamp, never interleaved between two of the batch's keys. That is what closes the partial-apply
window a per-key allocation chain left open when a single write raced a batched invalidation
(owner: `HotKeyCache.executeBatchInvalidate`, `allocateStampBlocks`). The range must fit one
sequence window (`MAX_ATOMIC_RANGE` = 4096, the layout's 12-bit sequence field, node bits constant
across the block — this keeps the block disjoint from every other node's window); callers chunk by
that bound, and a wire batch (`MAX_BATCH_ENTRIES` = 1000) always fits one call. Count outside
`[1, MAX_ATOMIC_RANGE]` is rejected — a silent clamp would break the exclusive-reservation
guarantee the caller relies on.

**A stamp clock is not a duration clock.** Versions use the wall-clock-derived, merged clock;
TTLs, soft expiry and `birthStampSec` use the per-JVM monotonic clock. Collapsing them lets an
NTP step rewrite entry lifetimes.

**The cached wall clock is monotonic and bounded.** `TimeSource` refreshes the wall clock on a 5 ms
daemon tick. A backward step never moves the cached value (expiry deltas stay non-negative), and a
forward leap larger than the elapsed real time plus a 5 s tolerance is refused and slewed at twice
real-time rate until it reaches the wall clock. Without that bound, a transient bad leap that NTP
later reverts pins the cached clock in the future for the length of the leap, and every absolute
timestamp and stamp minted in the window carries the bogus future; without the >1× rate, a genuine
step (VM resume, a first NTP correction) leaves the cache permanently behind the wall clock — the
slew advances exactly as fast as the gap grows, so it never closes. The rate is capped at the wall
clock itself, so a bogus leap still costs at most twice the seconds it lasts before NTP reverts it. Reads during the restart backoff keep the same
monotonic floor, and the ticker is reference-counted per application context — it stops only when
the last context in the JVM is destroyed, because the clock is JVM-global and one context teardown
must not degrade the others to a native clock read per call.

**The ticker state lives in the sign of the cached value.** Epoch milliseconds are positive, so
`currentMillis > 0` is the running state and `~currentMillis` is the stopped state's raw monotonic
floor. A running read is therefore one volatile load and no second offset read: the ticker stores
`wallNow + wallOffset`, while the stopped fallback adds the offset to
`max(floor, System.currentTimeMillis())`. `start()` and `stopTicker()` encode the transition under
the class monitor; the ticker publishes each tick with a CAS, so a tick that raced a stop fails
instead of resurrecting the running sign. The test offset adjusts a running cached value by the
delta and leaves a stopped floor raw, which keeps the existing time-jump tests exact. _Ruled out:_
a separate `running` flag — it made every read pay a second volatile load and let the flag and the
cached value describe different instants.

**An interrupted ticker re-arms in place; the stop join runs outside the monitor.** An unexpected
interrupt of the ticker's sleep is transient — the thread is alive — so it marks the clock stopped
(readers fall back to the wall clock), waits the restart backoff and republishes the running sign on
the same thread instead of destroying and recreating an OS thread. The ownership check happens
before the backoff: `markStopped()` returns `false` when `clockThread` is no longer this thread, and
both `resumeAfterBackoff()` and `restartWithBackoff()` return immediately in that case. Before that
return, a `stopTicker()` that cleared the slot (or a replacement `start()`) let the superseded
ticker sleep a full second in `resumeAfterBackoff()` even though `markStopped()` had only no-op'd;
`stopTicker()`'s `join(200)` then burned its full timeout. Conversely, `stopTicker()` performs the
state transition under the class monitor but the
interrupt and the bounded `join(200)` outside it: a ticker entering `markStopped()` while the
joiner holds the monitor would otherwise stall the joiner for the whole timeout. `start()` treats
an installed `clockThread` as "a ticker already owns the slot", so a registration arriving during
the backoff does not create a second OS thread; the dying ticker re-arms itself, and the exception
path clears the slot before starting its replacement. The lifecycle owner count is incremented
and `start()` is invoked in one monitor section (the increment and start cannot interleave with a
teardown), and the count is decremented under the monitor with the stop re-checked against it, so
a context registering during another's teardown keeps the ticker alive. Owner: `TimeSource`.

**Read-path stamps are taken before the value read.** A pre-read stamp is a lower bound ("this
value is at least as new as this instant"), so any write allocated after the load began outranks
it. A post-read stamp is an upper bound and would pin an over-stamped entry until its TTL.

**`0` means unknown and makes the weakest claim.** It is not "version zero". An entry with no
stamp accepts any real stamp; an incoming message with a non-positive stamp is dropped, because
a missing header must never be read as "version 0" and applied over real data.

**`decisionVersion` is a separate space.** A per-Worker `AtomicLong`, allocated at the transition
inside `HotKeyRegistry`'s per-key lock, not at send time, so version order is transition order and
a failed send still burns a version (the sequence has holes). It is never compared with
`dataVersion`.

Owner: `MonotonicStampClock`, `StampClock`, `NodeIdentity`, `VersionGuard`, `CacheEntry`.

---

## Version comparison

**Two policies over one numeric rule.** `VersionGuard` owns every comparison.

Data plane (absent → accept; incoming is positive — the wire boundary dropped every non-positive
stamp; resident `<= 0` → accept; otherwise numeric):

- `shouldSkipForSync`: `resident >= incoming` skips. Used by the load/populate store guard,
  where the comparison is against an entry the caller itself just produced and equality means
  "already current".
- `shouldSkipForRefresh`: `resident > incoming` skips. Used by the REFRESH receiver, the
  INVALIDATE receiver and the writer's own apply. An equal-version apply is either the
  over-stamped load being healed or a peer's copy of the same write (idempotent), while skipping
  pins the stale value until the next write or the whole TTL.

The cache-level pre-check reads through `Cache.asMap().get`, not `getIfPresent`: a sync-plane
first pass is not a caller read, so it must not record a hit/miss or promote the key in Caffeine's
access-order queue. The entry-level overload is called directly, with no capturing predicate.
REFRESH skips the pre-check when no cluster loader is configured: there is no loader I/O to avoid,
and the fallback's compute applies the same version guard inside the bin lock. The decision
plane's `resolveHot`/`coolApply` snapshots follow the same rule: a Worker broadcast must not inflate
the caller statistics or mark a cold key as recently used in TinyLFU.

Decision plane (`shouldSkipForWorker` for COOL, `shouldSkipForHot` for HOT): absent → accept;
different `nodeId` → accept on arrival (cross-Worker versions are incomparable); same node,
higher epoch → accept (restart); same node, lower epoch → skip (stale incarnation); same node
and epoch → skip when `resident decisionVersion >= incoming`. A non-positive `decisionVersion` is
unorderable and is dropped at `WorkerDecision.from`, before the body decode and the dispatcher: a
malformed broadcast must never reach the "different node" acceptance path and create or overwrite a
decision. The guard therefore assumes a positive incoming stamp and does not re-check it. The HOT
variant accepts an equal decision when the resident is `NORMAL`: a local
lifecycle transition keeps the decision stamp, so replaying the same decision must re-promote
instead of being skipped forever.

**Cross-Worker order is arrival order.** Epoch and `decisionVersion` are per-Worker counters
(epoch is seeded from that Worker's wall clock at boot, `decisionVersion` starts at 0), so they
are not comparable across Workers. Comparing them across Workers silences a surviving Worker
after a rolling restart — the restarted Worker stamps a higher epoch, and every later decision
from an older-epoch Worker is dropped until the entry expires. The owning Worker re-broadcasts
its decision, so a stale out-of-order decision is corrected by the next broadcast. **The
ghost-COOL guard is COOL-only and lives in `DefaultWorkerContext.isStaleCrossNodeCool`**:
it drops a cross-Worker COOL aimed at an in-force HOT whose owner is still the **current, alive
incarnation** (`isCurrentIncarnation(owner, entryEpoch)` — the same probe the read path uses) while
the ring still routes the key to that owner. The ring is reconciled from that health view before
the ownership probe: a published snapshot that predates the heartbeat making the owner live would
still route to the old owner and bypass the guard while a new Worker joins. It fails open on any
missing signal and never touches HOT, so it cannot reintroduce the "mute the survivor" failure it
complements. A restarted owner no longer protects the previous incarnation's HOT: the COOL applies
and the entry cools normally.

Owner: `VersionGuard`, `DefaultWorkerContext`.

---

## Sync messages

**Six types, one meaning each.**

| Message             | Carries                                         | Sent when                                                        | Receiver effect                                            |
| ------------------- | ----------------------------------------------- | ---------------------------------------------------------------- | ---------------------------------------------------------- |
| `PUT`               | key (header), version, typed value bytes (body) | synced write                                                     | version-guarded apply, no reload                           |
| `REFRESH`           | key, version                                    | synced refresh; PUT fallback when the value cannot ride the wire | reload through the cluster loader, else guarded invalidate |
| `INVALIDATE`        | key, version                                    | invalidation                                                     | version-guarded removal                                    |
| `INVALIDATE_BATCH`  | up to 1000 (key, version) pairs                 | collection invalidation                                          | per-key removal on the ordered dispatcher                  |
| `INVALIDATE_ALL`    | clear stamp                                     | `clearAll()`                                                     | sweep entries at or below the stamp + watermark            |
| `INVALIDATE_PREFIX` | key prefix, clear stamp                         | a scoped namespace clear received from a peer                    | sweep entries under the prefix + scoped watermark          |

**The type is decoded once into `SyncType`.** The AMQP type header becomes an enum at the
listener boundary; self-ignore, clear-family and jitter requirements are flags on that enum, so
the listener, the dispatcher and the handler never re-compare header strings. An unknown or
missing type is dropped at parse (DEBUG line in the listener) and never enters the
origin filter, the collision probe, the body decoder or the ordered dispatcher; `SyncMessage.from`
stays a pure data decoder. The origin-instance header is read once per delivery and shared by the
self-filter and the node-id collision diagnostic. A lenient `readStringHeader` helper owns the
String-vs-`toString` conversion for the app-name, origin-instance and PUT cache-key headers, so
the same conversion is not re-derived at each site. Owner: `SyncType`, `SyncMessage.from`,
`CacheSyncListener`, `AmqpMessageReader`.

**The decision plane uses the same decoded-type shape.** `WorkerDecision` carries one
`WorkerType` (HOT/COOL, parsed once from the header) and one `DecisionStamp` that also
stores the corresponding `DecisionKind`; no handler has to infer a lease type from the effective
state. The listener reads the parsed flags for admission —
`isControl()` for the global-budget exemption, `needsJitter()` for the start-jitter park — and the
handler compares the stamp directly through `VersionGuard`, so no branch re-parses a string and no
handler rebuilds a record per message. The wire headers are unchanged: the enum name is the
`HEADER_TYPE` value. A blank cache-key body is rejected at the decoder, matching `SyncMessage`, so
an unservable key never reaches the cluster loader. Owner: `WorkerType`, `WorkerDecision`,
`WorkerListener`, `VersionGuard`.

**The loader read and the hook tail have one owner per plane pair.** `DecisionPlaneSupport` holds
the optional cluster-loader read (clean miss, declined admission and loader failure all answer
`null`) and the hook loop that isolates a broken hook. Each `fire*` method checks its list for
emptiness before building the capturing callback, so the default no-hook deployment allocates
nothing on the apply path. Owner: `DecisionPlaneSupport`, `DefaultSyncContext`,
`DefaultWorkerContext`.

**The batch frame is decoded before the dispatcher, and the PUT payload is typed.** `SyncMessage`
carries a nullable `byte[] payload` only for `PUT`; `INVALIDATE_BATCH` is a wire frame, not a
message, and `SyncMessage.parseBatch` materialises each accepted key/version pair directly as the
`INVALIDATE` sub-message the listener submits. The record therefore never holds an untyped
`attachment`, the dispatch layer never sees the batch type, and a refused peer stamp is the only
case that rebuilds a sub-message. The parser still consumes one entry at a time so a malformed
element cannot shift the token stream for its neighbours. Owner: `SyncMessage`,
`CacheSyncListener.process`.

**The batch parser consumes exactly one entry at a time.** A malformed element must not shift
the token stream for its neighbours. `skipEntry` starts from the parser's current token: an entry
that ended early (`[]`, `["key"]`) is already at its `END_ARRAY` and must return without consuming
the next entry, while a nested start is passed through `skipChildren` before looking for the
entry's closing bracket. The previous `skipEntry` always advanced first, so an empty entry
swallowed the following element (and an empty final entry swallowed the root) and a nested element
left the parser inside the malformed entry; one bad entry discarded the whole batch. Owner:
`SyncMessage.skipEntry`.

**Same-version priority is INVALIDATE > REFRESH/PUT, in arrival order too.** The publisher's
`BroadcastClaim` already treats an equal-version INVALIDATE as an upgrade over a REFRESH. The batch
fold must use the same rule: a later equal-version REFRESH must not cover an INVALIDATE of the same
version, or the final state depends on message arrival order. A covered INVALIDATE is retained as
the batch's highest one; when the winning value message falls back to an eviction, that covered
removal still fires `afterInvalidate` (the value fallback carries out its removal), in addition to
the winning message's `onRefreshSkipped`. Owner: `Batch.reduce`, `CacheSyncListener.dispatchBatch`.

**A clear batch has no winner; its shape is the classification.** The clear family shares one
dispatcher channel so all table sweeps serialize, and every clear watermark covers a different key
set. `Batch` is sealed into `Batch.Value` (a non-clear batch reduced to one effective message) and
`Batch.Clear` (every table-wide and prefix clear, applied in one sweep). `CacheSyncListener`
routes on the batch type, not on a synthesized winner, so the old `outranksClear` reduction and
the meaningless clear-batch `winner` field are gone. `Batch.Value` still takes a single-message
fast path without allocating a covered list, and `allowSoftExpire()` is derived from the winner
and the covered invalidate rather than stored as a second field that could drift. Owner: `Batch`,
`CacheSyncListener.dispatchBatch`, `DefaultSyncContext.clear`.

**Semantics, stated once.** At-most-once for every sync/decision message (ack before update). A
crash between ack and update loses one message; the next write or the TTL heals it.
Fire-and-forget: no publisher confirms, because all three publish paths are state-oriented and
the next cycle re-sends. Best-effort ordering, with the version guard as the only authority —
the deferred buffer and the direct send paths were never guaranteed to order against each other.

**Origin filtering covers value-bearing and clear types.** The fanout delivers a sender's own
message back; a self-REFRESH would equal-apply and, with no value channel, invalidate the entry the
writer just wrote, and a self-PUT would replay the value the sender just stored. Clears
(`INVALIDATE_ALL`, `INVALIDATE_PREFIX`, `INVALIDATE_BATCH`) are dropped for the same reason: the
sender recorded the watermark and swept its own L1 before publishing, so the loopback would repeat
a full-table scan (or one scan per batch entry) over a cache the clear just emptied. Self-
INVALIDATE is still processed (it heals a racing reload stamped with an older version), and
messages without an origin header are processed (rolling upgrade).

**Foreign-app isolation: routing key for decisions, header for sync.** The decision exchange
(`zeta.send.exchange`) is a **direct** exchange and every instance queue binds under
`send.<appName>`, so a shared broker delivers one application's HOT/COOL only to that
application's queues. It was a fanout, which ignores the routing key: every app queue on the
broker received every decision of every tenant and the `appName` header filter dropped the
foreign ones inside the JVM — one tenant's mass-heating wave became a broker-side fan-out to the
whole broker plus a deserialize-and-discard on every unrelated instance, a cost that grew with
the tenant count rather than with the emitting tenant's traffic. A direct exchange keeps the
fan-out (every queue bound to one key still gets a copy), so nothing about the intra-application
contract changes. The sync plane (`zeta.sync.exchange`) is still a fanout whose exchange name is
a global constant, so there the header check remains the isolation mechanism: a message is
dropped only when the sender declared an `appName` header that differs from the local
`zeta.app-name`; a missing header (pre-0068 sender, rolling upgrade) and a blank local name are
always processed. The decision receiver keeps the same check as its own boundary — a queue bound
to the wrong key (an operator mistake, or one left over from the fanout topology) must not
promote keys in another application's cache. Drops are counted and surfaced as
`foreignAppDrops`/`lastForeignApp`; the drop is logged at DEBUG because a misconfigured app name
silently discards all traffic while the process looks healthy.

**The REFRESH receiver does not share the read path's SingleFlight.** It looks tempting: the
receiver runs on the ordered dispatcher, so a direct `clusterLoader.load` holds that thread for as
long as the source takes. But the dedup layer's contract is _reusable success_ — a completed
future is replayed for its whole window — and the receiver stamps whatever it gets with the
**message's** version. A reused load would therefore claim a version it was never read at, which
is the unsafe direction (the read path's stamp is a pre-read lower bound by construction).
Bounding this call needs a no-reuse load, not a shared one. Ruled out: routing the REFRESH
receiver through `SingleFlight`; a `DefaultSyncContextTest` case pins the value-less
fallback that a reused future silently broke.

**Collection invalidation is batched, and a zero payload cap disables value PUTs.** An
`invalidate(Collection)` allocates one contiguous version block per `MAX_BATCH_ENTRIES` chunk and
sends one `INVALIDATE_BATCH` message instead of N single-key broadcasts; a `REMOTE` scope uses the
same wire shape through `HotKeyCache.invalidate(Collection, InvalidatePolicy)` without dropping the
local copy or recording tombstones. The decoded batch exposes the highest entry version as the message version,
so the receiver merges one clock floor and re-stamps a refused batch with one local stamp.
`zeta.sync.put-max-bytes=0` means no payload can fit: a synced write degrades
to a value-less REFRESH immediately, without paying the Jackson serialization pass for a value that
would be discarded.

**The PUT payload codec is one receive/transmit component, not the publisher's parser.** The
polymorphic mapper, the size cap and the bounded write buffer lived inside `CacheSyncPublisher`, so
`DefaultSyncContext` had to inject the outbound publisher (and transitively `RabbitTemplate`)
just to decode an inbound body. `PutPayloadCodec` now owns the mapper, `encode` and `decode`, and
both sides share one bean; the publisher keeps only AMQP publishing and the dedup window. The
bounded stream overrides `write(int)` and `write(byte[], int, int)` only: `writeBytes(byte[])`
delegates to the latter, so the old extra override ran the cap check twice per write. Failure
symptom of the old split: a test or deployment without a publisher could not build the receive
context, and the whitelist had two independently constructed mappers to keep in sync. Owner:
`PutPayloadCodec`, `CacheSyncPublisher`, `DefaultSyncContext`.

**A value-less REFRESH cannot keep a known-stale value.** The peer's write already happened, so
the local copy is stale by definition. With no cluster loader the receiver force-expires the soft
window of a Worker-managed entry — the next read reloads through the application reader while the
Worker decision and its extended TTLs survive — and removes an ordinary entry outright. Preserving
the value until the HOT hard TTL (the previous shape) served a known-stale value for up to an
hour. _Ruled out:_ removing the Worker-managed entry too — that discarded the decision stamp and
made the next Worker decision start from scratch.

**A fallback eviction is not a peer INVALIDATE.** When a REFRESH loads nothing, a PUT payload is
unreadable, or the loader refuses a peer value, the receiver still evicts the resident under the
message's version and tombstone — but it does so through the internal eviction path, which fires
no `afterInvalidate` for that value message. The message itself then fires exactly one terminal
`onRefreshSkipped`. Firing both hooks for one message (the former shape) broke the hook contract
and told observers that a peer had explicitly invalidated a key when it had merely failed to
deliver a value. The one exception is a covered INVALIDATE already folded out of the same batch:
if this fallback actually removes the entry, that invalidation's removal is now observable and
its `afterInvalidate` fires alongside the refresh skip. Owner:
`DefaultSyncContext.evict`.

**A failed deduped send drops its claim instead of restoring the displaced version.** The
claim previously rolled back to the version it displaced — but that version's own send may
equally have failed or still be in flight, so the rollback recorded an unsent version as sent
for the whole dedup window and a retry of it was silently skipped. Dropping the claim at worst
re-sends one idempotent broadcast (the plane's at-most-once contract); keeping a phantom claim
can lose the mutation for the window. Owner: `CacheSyncPublisher.sendDeduped`.

Owner: `CacheSyncPublisher`, `CacheSyncListener`, `DefaultSyncContext`, `SyncMessage`,
`AmqpMessageReader.AppIsolationFilter`.

---

## Dedup lifecycle

**SingleFlight is dedup, not a cache.** The leader retires its flight as soon as the load
settles — before it completes the shared future — so a later miss re-runs the reader instead of
replaying a completed future, and a waiter woken by the completion that retries cannot re-elect
the settled flight. The table deduplicates concurrent loads only; there is no completed-result
window to serve stale data.

**Every L1 mutation or removal kills an in-flight load.** A load admitted before a write or
removal must not commit its pre-mutation value, so every path that removes or overwrites L1
calls `SingleFlight.invalidate(key)`: invalidate (single/batch), the sync receiver's applied
removals, `putLocal`, `putThrough`, and the clear paths (`invalidateAll` / `invalidatePrefix`).
**If you add a mutation site, wire it here too.** Hard expiry is not a mutation: a load already in flight is
fresh data, and the version guards reject anything older, so the read path's logical-expiry
branches do not invalidate. Invalidating there would let every concurrent reader that saw the
same expired entry kill the load the previous reader had just started — the cache-stampede
failure the dedup layer exists to prevent.

**The leader owns the whole flight.** It admits under the breaker, runs the reader, commits,
retires its flight and completes the future — retiring first is what closes the retry window a
woken waiter would otherwise see. Breaker settlement stays on the thread that ran the reader
(inline, before the completion; on the executor, inside the task), except a timeout, which settles
from the timing thread so a wedged reader cannot keep the flight out of the breaker; and
`putThrough`, `putLocal`
and `refresh` invalidate the key's dedup entry _after_ their L1 apply through the shared
`writeThroughApply` tail — before its optional broadcast, so a failed send cannot leave the
pre-write future replayable (commit first, then drop). The read path needs no post-commit
removal step because the commit happens inside the load.

**A timeout is a settlement, not a detached future.** The interrupting timeout and the reader's
normal return compete for one settled-bit CAS on the flight. The timeout path settles the breaker as a
failure before it completes the shared future, so a reader that ignores its interrupt and blocks in
uninterruptible I/O still counts against the breaker and a waiter woken by the timeout sees the
updated state. A worker that returns after the timeout loses the CAS: its late value never reaches
L1 and never becomes a breaker success. The success path settles the breaker before completing the
future for the same reason — a woken waiter must never observe the flight before the breaker
outcome it belongs to. Reader failures are tagged with a private `ReaderFailure` (a
`CompletionException`): only the dedup layer's own timeout and submission rejection stay bare, so
a reader-thrown `RejectedExecutionException` or `TimeoutException` is a `FAILED` load, not a
suppression. The join-side classifier unwraps exactly the join envelope, never the reader's own
cause chain. Owner: `SingleFlightImpl`.

**A killed load and a completed load leave the table differently.** `SingleFlightImpl` entries are
flights that are themselves the shared `CompletableFuture`, carrying a write-path kill bit in the
same word as the settlement bit and the breaker stamp: `invalidate`/`invalidateAll` set the kill bit
(and leave the waiters their future to complete), while the leader's own removal is bookkeeping
that never looks like a kill. The leader reads the kill bit before committing, so a load superseded by
a local write, a peer apply or a clear serves its waiters but never re-populates L1. Breaker
settlement is leader-only and happens on the thread that performed the downstream call, so a
mid-batch election failure cannot strand a HALF_OPEN probe and the batch path needs no cleanup
pass.

**One flight object carries the whole election.** `InFlight` extends `CompletableFuture`, and one
volatile `long` packs the 61-bit breaker stamp, the settled bit, the backlog bit and the kill bit.
The flight is created inside `computeIfAbsent` after breaker admission, so a rejected load allocates
only the small holder that publishes the candidate back to this call, never a discarded future. A
caller that finds an existing flight allocates no flight, only the small election result. Leadership
is decided by identity with the candidate this call's mapping function created: the call that
inserted the flight is the leader, and every other call — including a same-thread re-entry that
reuses the flight — is a follower. A claim bit set after insertion cannot make that distinction: a
follower descheduled between insertion and claim could steal the role, which would run a
`callerRunsLoader` reader on the wrong thread and lose the caller's transaction and MDC, and the
creator's own re-entry would see the bit and run the reader a second time. The flight records the id
of the thread running its reader; a follower that finds that id equal to its own throws
`IllegalStateException` instead of joining a future only it can complete, converting the old
self-join (a permanent block under caller-runs, a budget-long stall on the executor) into a fast
failure. A follower whose leader was refused by the executor resolves to the same
`SUPPRESSED_EXECUTOR` outcome as the leader, not to a raw rejection.

**Shutdown fails every live flight.** Spring destroys `SingleFlight` before
`interruptibleLoadExecutor`, but the executor's `shutdownNow` drains queued tasks without completing
their `CompletableFuture`. Without a close hook a caller already joining one of those flights blocked
forever, `pendingLoads` never returned, and a HALF_OPEN probe slot stayed reserved. `close()` sets a
closed flag, removes every live flight, releases its backlog slot once when the load has already
claimed one, settles the breaker as abandoned and completes the future with
`RejectedExecutionException`, which the join classifier maps to `SUPPRESSED_EXECUTOR`. A load that
elects after the flag is set takes the same suppression, on both the single-key and batch paths. The
election's own shutdown withdrawal also completes the inserted candidate exceptionally, not only
removes it and releases the probe: a follower could observe that candidate between the insertion
and the removal and join a future no leader would ever finish. The previous shape had no close
path. Owner: `SingleFlightImpl`.

**One dedup table carries one payload shape.** The single-key and batch read paths both submit
`VersionedValue` carriers. They share one dedup table, so a single-key read and a batch read
racing the same missing key elect one leader and the other joins its future — with heterogeneous
payloads the joiner either threw `ClassCastException` or nested the carrier inside a fresh
carrier and cached that; sharing the carrier also gives the follower the leader's own pre-read
stamp (the stamp is the value's lower bound, not the joiner's later allocation). Owner:
`HotKeyCache.loadAndCache` (batch).

Owner: `SingleFlightImpl`, `HotKeyCache` (`dropLocal`, `dropAndBroadcastInvalidate`,
`putThrough`, `putLocal`, `refresh`).

---

## Buffering and ordering

**The sync buffer owns the invalidation send and its causal cancellation.** All peer broadcasts leave
through `BroadcastBuffer`: PUT/REFRESH through the deferred map, the INVALIDATE family through the
immediate send. An INVALIDATE at version `v` cancels every buffered PUT/REFRESH for the key at or
below `v` before the message leaves. The cancellation used to live in four `CentralDispatcher`
methods, i.e. it was a caller obligation: a new invalidation shape that forgot the eviction shipped
a buffered PUT after the invalidation that superseded it and re-warmed deleted data on the peers
(or, with the invalidation dropped, pinned it until the peer's hard TTL). Folding the send into the
buffer makes the rule impossible to skip, and the publisher and send executor also have one owner.
Owner: `BroadcastBuffer`.

**Deferred sends merge by version.** `BroadcastBuffer` defers REFRESH/PUT for a short window and
flushes on a dedicated single-threaded bounded executor. Deferral batches a burst of writes into
fewer sends and moves the send off the caller. The pending map merges by version: within one
space the strictly newer allocation wins, so an out-of-order record can never regress the
pending version and silently drop the newer write's broadcast. It also holds the swap read side
across the merge, so a record racing a flush swap is never stranded outside both maps.
Cancellation follows the same stamp: a pending record is removed only while its live version is
at or below the invalidation's, and the removal re-reads the live entry atomically, so a record
merged after the sweep observed it is still judged by its version rather than its object
identity.

The schedule fast path also leaves an armed task in place when it is already due
(`getDelay <= 0`), not only when the record arrives inside the deferral window. Once the
max-defer window has elapsed, a burst of concurrent records must not queue on `scheduleLock`
only to discover the flush is already pending; that is exactly the write rate at which the
buffer is busiest. Owner: `BroadcastBuffer.ensureFlushScheduled`.

The approximate size/payload counters are decremented in the same bin compute that removes an
evicted entry, and evictions and records both hold the swap read side while a flush reset holds
the write side. An empty map therefore cannot look over its cap, and a record that loses its entry
to a flush swap finds size zero instead of arming a deferral window for nothing. A PUT/REFRESH
record also carries a currentness predicate evaluated under that read side twice — before the merge
and again after it. The two stores form a Dekker pair with the invalidation path, which records its
tombstone before evicting pending entries: either the invalidation sees the record and removes it,
or the record sees the tombstone and removes itself. A single pre-merge check left the window open,
because both sides hold the shared read side and neither waits for the other.

**A flushed snapshot is chunked into independent lists.** The send executor receives at most
`SEND_CHUNK_SIZE` (1,024) entries per submission, and each chunk is its own `ArrayList` over the
swapped map's entries — never a `subList` view. A view keeps the parent array, and therefore every
entry and value payload of the whole snapshot, reachable until the last chunk has run; that is
exactly the state a backed-up send executor holds longest. Independent chunks let each slice be
collected as soon as its send returns. Chunks are submitted in iteration order, but per-key
convergence is version-guarded and order-independent.

**The batch is the unit of work, and it is reduced before anything is applied.** The receive side
does not preserve an origin's send order: `zeta.sync.concurrent-consumers` defaults to three AMQP
consumers that deserialize and submit concurrently, and the publisher uses separate channels per
message type, so even one origin's `[REFRESH v10, PUT v5]` can reach the dispatcher with the PUT
first. `KeyDispatcher` therefore hands a key's whole granted batch to
`Batch.reduce`, and the handler keeps only its highest-version message: the
final state of a key is decided by that one message, so every message it covers would be applied
and immediately overwritten. Reducing first means a superseded PUT never pays its payload
deserialization, a superseded REFRESH never runs the loader, and a superseded INVALIDATE never
takes a cache-bin lock. A covered PUT/REFRESH still reports one terminal `onRefreshSkipped`; a
covered INVALIDATE that produced no removal is dropped silently, because a removal that never
happened has no applied/skipped outcome to report. One exception carries the covered removal into
the winner: when the winner's load yields nothing (or is refused by `isCacheable`) and the batch
contained an explicit INVALIDATE, the fallback hard-removes rather than soft-expiring a
Worker-managed entry — serving the pre-invalidation value would contradict an instruction the
same batch already carried — and the actual removal fires the covered INVALIDATE's
`afterInvalidate`.

**A closed dispatcher does not re-classify an appended task.** The submission path checks
`closed` inside the per-key bin lock, but a task appended to an existing worker may be granted
and executed by that worker before the submitter's post-compute `closed` re-check. Returning
`false` then made `CacheSyncListener` replay the already-applied PUT/REFRESH as a same-version
INVALIDATE, which the equal-version guard accepts and which erased the value the worker had just
written. A task appended to an existing worker therefore reports acceptance even when shutdown
wins the race; only a worker this call created (which never started) returns `false` and lets the
caller take its local fallback. Shutdown is allowed to drop a granted batch.

The batch carries no state of its own: no per-batch context, no deferral slot, no end-of-batch
callback. The list _is_ the state, and it dies with the call.

_Ruled out._ (A) Apply every message on arrival and let the version guards reject the stale ones:
correct, but the covered messages still pay their I/O and bin locks — the cost this reduction
exists to remove. (B) A per-batch deferral slot plus an end-of-batch callback: a deferral slot only
ever held the REFRESH (or the clear) that a later message would have covered anyway, and it needed
a context object, a role check on every handler method and a `finally` in the listener to settle
abandoned batches. (C) Sort inside `KeyDispatcher`: it is a generic ordered executor
shared with `WorkerListener` and would have to learn message types and override rules. (D) Compact
the same-key queue at enqueue time: the task a submission would replace may already be inside the
granted batch.

**Every clear shares one dispatch channel.** `INVALIDATE_ALL` and `INVALIDATE_PREFIX` dispatch
under one non-String key, because their cost is the L1 table itself: concurrent sweeps of
unrelated namespaces only competed for the same table and starved the per-key tasks. One channel
also makes them coalescable: the batch merges every clear's stamp into its accumulator and then
runs a single `entrySet` pass, judging each resident from the value the iterator already holds and
using `EntryMutations.mutate` for the guarded removal (no separate `getIfPresent` before the bin
lock, no per-key outcome array). Each rule keeps its own stamp: collapsing a scoped clear
into the table-wide one, or two prefixes into their maximum, would evict entries written
legitimately after the lower-stamped clear. Ordering against per-key work is unaffected either
way — anti-resurrection is carried by `EntryLifecycle`'s watermarks, not by the dispatcher.
Owner: `CacheSyncListener` (`CLEAR_DISPATCH_KEY`), `DefaultSyncContext.applyClear`.

**A refusal is applied inline, on the consumer thread.** The sync plane acks before the ordered
dispatcher runs, so a message the dispatcher refuses (per-key queue full, global budget exhausted,
listener closing) would otherwise leave a stale L1 entry for its whole hard TTL. The fallback
therefore applies it immediately: a clear as a local sweep, anything else as an O(1) in-memory
invalidation. Neither touches the cluster loader, so the thread that must keep acking cannot
stall. Owner: `CacheSyncListener.onDispatchRejected`.

**A twice-refused submission retires its worker.** If the executor refuses both the immediate
submission and the delayed re-drive, the key's `KeyWorker` is removed and its queued weight
discharged. A later submission then creates a fresh worker instead of enqueueing behind a cycle
that will never run. No scheduling flag is needed: the atomic compute reports whether this call
created the worker, and only the creator drives the first cycle; every later cycle is re-driven by
`run` after it settles. Owner: `KeyDispatcher.submit`, `abandon`.

**The first batch is pre-granted; a queued tail is folded into it at the first cycle.**
`KeyWorker` is created with its first task already in a `Grant` (task list plus budget weight),
so a lone first task pays no grant `compute` before the consumer call. If submissions arrived
while the start-jitter park held it, the first cycle folds that queued tail into the same grant
under the bin lock, preserving the old coalescing (one consumer call, one sweep, one loader
read); a lone task still skips the compute. Every later batch is drained at the end of the
previous cycle, so a sustained key costs one `compute` per cycle instead of two. The `Grant`
reference is cleared as soon as the cycle takes it: keeping it through the consumer's synchronous
load and the next park pinned every payload (a PUT body can be 32 KB) in the dispatcher. The
worker also implements `Runnable`, so a cycle submission allocates neither a closure nor a
wrapper. `close()` resets the global unit counter with the map that held the queued weight; an
in-flight `run` therefore skips its `finally` subtraction when `closed` is set, otherwise the
reset was immediately driven negative by the batch it had already granted. Owner:
`KeyDispatcher.KeyWorker`, `run`, `close`.

**Per-key FIFO with batched consumption.** `KeyDispatcher` runs up to
`maxTasksPerCycle` same-key tasks per submission and yields a busy key so other keys progress.
Queue management is one atomic `compute` per operation (no orphan-queue race by construction);
weight is `1 + bodyBytes/1024` per task, bounded per key and against one global unit budget, and
an over-cap submission is rejected at the door rather than queued and later discarded. Control
submissions (a COOL, a clear) skip the global budget: an O(1) message that repairs the state a
payload storm created must not be locked out by it. A granted batch is handed to the consumer as
one list — the boundary the reduction above works on.

**No I/O while holding a Caffeine bin lock.** The load runs outside the mapping function;
`compute` only stores the already-loaded value. A loader or broker call inside `compute` would
hold a per-bin lock across network latency and serialize every key hashing to that bin. This is
why the read path takes its stamp before the load and commits afterwards.

**Packing the pending size and payload counters into one `AtomicLong` — rejected.** The two
counters are read once per record and reset together, so one word looks tempting, but a payload
delta can be negative: adding it into the low field borrows from the count, and the correct
field-wise update needs a CAS retry loop. That replaces two uncontended `addAndGet`s (one of them
usually skipped — a version-only REFRESH has no byte delta) with a retry loop on a single cache
line under exactly the write burst the buffer exists to absorb. The pair stays two counters; the
size reset is already one write each under the swap's write side (2026-11). Owner:
`BroadcastBuffer`.

Owner: `BroadcastBuffer`, `KeyDispatcher`, `DefaultSyncContext`.

---

## Topology

**Consistent hashing with a cheap key hash and a 64-bit node seed.** 32-bit Murmur3 finalizer
over the key's `String.hashCode` (cached, allocation-free), 500 virtual nodes per Worker, one
Worker per key. The ring is rebuilt from heartbeats; a dead Worker's keys re-hash to the
survivors with no rebalancing protocol. The build mixes the node identity and vnode index
directly — no per-vnode String, TreeMap entry or boxed Integer — packs `(hash, nodeIdx)` into one
`long[]`, sorts once, and keeps the first entry of each equal-hash run while splitting into the
published `int[]`. The physical-node seed is a 64-bit FNV-1a hash of the full ID avalanched by
`fmix64`, advanced with a golden-ratio stride; the previous shape used one 32-bit
`String.hashCode` value XORed with `mix32(i)`, so two IDs that collided in the 32-bit polynomial
hash shared every virtual position and one node was erased by the collision dedup, while an ID
whose hash equaled `mix32(i)` produced the fixed `mix32(0)` position for every such node. The
lower node index still wins a residual 32-bit position collision, so the winner is deterministic
across instances and a node's virtual positions never move when another node joins or leaves:
the sort is the dedup, with no open-addressing set and no re-salting. The residual is a fixed
≈0.2% ordering skew at that position, no correctness impact. The key hash itself
stays 32-bit: it is on the routing hot path, where MD5/SHA-256 would cost an order of magnitude
more. _Ruled out:_ re-salting a colliding vnode into the next free slot (it made a surviving
node's positions depend on which other nodes happened to be present, breaking the
minimal-disruption contract) and dropping the loser after bounded retries (it silently lost
virtual nodes). _Ruled out:_ Maglev (O(1) lookup, but a rebuild reshuffles more than a ring's
minimal-disruption guarantee, no equivalent of the `isAlive` probe walk, and the lookup is a
small part of the reporter's flush path) and Rendezvous/HRW (revisit only if routing CPU or
distribution ever measurably hurts — zero-state and strictly minimal-disruption if that day
comes).

**Routing reuses the health-view liveness judgment.** `rebuildIfPossible` reconciles the ring
against `getAliveWorkerIds`; `locateNode(key, healthView)` samples the monotonic clock once per
call and probes each physical node by ID against that instant instead of sampling per hop. The
reporter uses the snapshot-only `locateNode(key)`: it routes on the ring's current published
snapshot and never re-probes liveness per key. A node dying after the reconcile still receives the
batch, and the consumer discards it through the incarnation check. A concurrent rebuild between the
admission's alive-set capture and routing can send a key to a node whose epoch was not captured;
that key is dropped as unroutable rather than re-based onto the new epoch. Both are the same
bounded report loss the alive-set predicate already had. The
predicate and health-view overloads remain for callers that route against a state they did not
just reconcile (the ghost-COOL guard, which reconciles the ring from its health view immediately
before the ownership probe). The common fast path (the start node is alive) allocates
nothing; a walk that starts on a dead node allocates one `long[]` bitmap indexed by physical
node, so each distinct node is probed at most once regardless of cluster size. Owner:
`ConsistentHashRingImpl`, `HealthViewImpl`, `KeyReporterImpl`.

**Inspection reconciles before listing.** `/actuator/hotkeyring` calls `rebuildIfPossible` on
every read. The reporter tide is the routing rebuild, but an operator asking for topology must not
see a Worker that died since the last flush; the endpoint is the observability path and owns the
refresh that makes its answer current. Owner: `RingEndpoint`.

**One freshness judgment.** The ring, the health gate and Decision Validity all use
`readyToServe && !stale && heartbeat < effectiveTimeout` — the configured static window
(default 10 s), or the per-Worker adaptive estimate once warmed up. Using different judgments
would create windows where reports route to a new owner while L1 still trusts the old one.

**Graceful degradation triggers only on a failed health gate.** COOL entries become locally
promotable, and the reporter keeps accepting counts while its limiter backpressures. Recovery is automatic on the next heartbeat; Worker
decisions override local promotions by arrival order.

Owner: `ConsistentHashRing`, `HealthView`.

---

## Locking

**Two domains, one acquisition order.** (1) HeavyKeeper sketch stripe monitor, (2) HeavyKeeper
admission lock. A path inside a higher-order lock must never acquire a lower-order one. Admission
and fading take (1) then (2); the cache and the window table use `ConcurrentMap.compute` and take
neither. This is the only lock-ordering rule in the codebase, and it is what makes the components
composable without a global lock.

**Cache-level atomicity uses no explicit locks.** Every read-modify-write goes through
`ConcurrentMap.compute`, which gives per-key atomicity and keeps the fast path lock-free for
readers. The version guard is re-checked inside the compute, so a decision made on a stale
reference cannot overwrite a concurrent write.

Owner: `HeavyKeeper`, `HotKeyCache`.

---

## Executors

**Pool roles are not interchangeable.**

| Pool                        | Work                                      | Saturation policy     |
| --------------------------- | ----------------------------------------- | --------------------- |
| `hotKeyExecutor`            | bulk refresh fan-out, isolated cache work | `CALLER_RUNS` allowed |
| `interruptibleLoadExecutor` | read-miss loads and background refresh    | abort                 |
| `zetaSendExecutor`          | broadcast sends                           | abort                 |

`CALLER_RUNS` is honoured **only** on `hotKeyExecutor`. On the load pool the caller would block
inside SingleFlight's mapping function while holding a Caffeine bin lock, and `orTimeout`'s
interrupt would land on the business thread. On the send pool the caller is the commit thread,
so a saturated broker would stall transaction commit — the exact failure the deferral exists to
prevent. Configuring `CALLER_RUNS` there is ignored with one startup WARN, never silently
downgraded.

**A load timeout is an execution budget, not a queue deadline.** `InterruptingAsync` arms the
timeout when a pool thread starts the task, so queue wait is not charged against the reader.
Anchoring the clock at submission made batch semantics depend on pool depth: the tail of a
`getAll` on a busy pool resolved `SUPPRESSED_TIMEOUT` without its reader ever running. Queue
saturation is an admission problem and must surface as one, not disguised as a data-source
timeout. A task that never starts never times out; callers that need a bound on queue wait own
admission control. A non-positive budget is refused at submission (`timeout > 0`), so a zero
budget cannot complete the future inline and then still run the supplier. The task future
overrides `completeExceptionally` and `cancel` instead of
registering a `whenComplete` consumer: a bare `TimeoutException` is the budget's own completion and
interrupts the recorded thread, while a supplier failure arrives wrapped in `CompletionException`
and is not mistaken for it. The timeout completes the future **before** interrupting: a reader that
catches the interrupt can otherwise return and win the race with `complete(value)`, turning a budget
timeout into a loaded value. One guarded `state` reference carries the phase — queued supplier →
running thread → null — so the interrupt and the task exit are mutually exclusive, the supplier is
released when `run()` takes it or a cancellation clears the phase, and each task costs one object
instead of three fields, and the budget is stored once in nanoseconds so the time unit is not
retained as a second field. Owner: `InterruptingAsync`, `SingleFlightImpl`.

**The refresh semaphore must stay below the load pool.** Background refreshes and read-miss
loads share `interruptibleLoadExecutor`, so a `refresh-max-pools` value at or above the pool's
max size bounds nothing: refreshes take every thread and read-miss loads queue behind them. The
default derives half the pool (`refresh-max-pools = 0` → `max(1, maxPoolSize / 2)`), and a
configured value above the pool is clamped to the pool with one WARN — the pool, not the
semaphore, is the real bound. The earlier default (100 against a 32-thread pool) read as a limit
while providing none. Owner: `ZetaProperties.effectiveRefreshMaxPools`, `ZetaAutoConfiguration`.

**`StandardThreadExecutor` uses Tomcat's core→max→queue→reject ordering.** The JDK's
core→queue→max ordering lets a bounded queue starve the growth the pool was sized for. Its
`submittedTasksCount` admission accounting is symmetric on every path that never reaches
`afterExecute`: rejection (including a handler that throws an `Error` — the compensation is a
`finally`), `remove`, `purge` (each cancelled queued `Future` is removed and counted), and
`shutdownNow`. `setMaximumPoolSize` re-derives the admission ceiling; the queue reads
`getMaximumPoolSize()` from the executor on every thread-creation decision, so runtime resizing
needs no mirror field to keep in sync.

**The shutdown recheck compensates once, and `purge` is overridden to compensate.** The JDK's
`execute` offers the task, then on a shutdown recheck calls the virtual `remove(command)` and, if
that report says the task was still queued, calls `reject(command)`. Without a marker, `remove`
decremented the submission and the internal rejection handler decremented it again — one
submission could drive the counter negative (or at least permanently under-count it). The marker
is written by a `remove` that won while the pool is shutting down; only the JDK's recheck hands
that same task to the rejection handler, which consumes the marker before invoking user code, so a
`CallerRunsPolicy` task that removes another queued task still balances it. An <em>external</em>
`remove(task)` during shutdown writes the same marker, so every `execute` first drops any marker
already on the thread when the pool is shut down: the JDK's own marker is written later, inside
`super.execute`, so it cannot be mistaken for a stale one, and a re-submitted instance is still
compensated exactly once. A normal submission never touches a `ThreadLocal`: it only pays an
`isShutdown()` read of the pool word. The JDK's `purge()` removes
cancelled futures through the queue iterator and never calls `remove(Runnable)`, so the previous
code leaked one admission slot per purged future while the design document claimed the path was
symmetric. The override removes cancelled queued futures and subtracts only the removals the purge
actually won; a worker or a concurrent purge that won the race owns the other compensation. Owner:
`StandardThreadExecutor.execute`, `remove`, `purge`, `InternalForceQueueHandler`.

**The admission ceiling saturates.** `queueCapacity + maximumPoolSize` is computed as a `long` and
capped at `Integer.MAX_VALUE`, and a negative capacity is rejected. A plain `int` sum overflowed to
a negative ceiling for a large `queueCapacity`, and the over-cap pre-check then rejected every
submission. Owner: `StandardThreadExecutor`.

**Force-and-shutdown share one lifecycle lock.** `StandardExecutorQueue.force` checks
`isShutdown()` and offers in one critical section; `shutdownNow` takes the same lock around the
STOP transition and drain. Without it, a forced task could slip into the queue between the check
and the drain, never run, and leave its admission count permanently incremented. Owner:
`StandardExecutorQueue`, `StandardThreadExecutor.shutdownNow`.

**`offer()` decides from lock-free counters first.** `ThreadPoolExecutor.getPoolSize()` takes the
pool's single `mainLock`, so calling it on every submission serialises the lock-free transfer
queue behind one monitor. The queue therefore short-circuits twice without it: fewer outstanding
tasks than core threads means an idle core thread (queue), and a fixed-size pool (`core == max`)
has no growth step to force (queue). In the elastic range it first tries a direct transfer to a
worker already waiting in `take()`/`poll()`: a successful transfer proves the queue was empty and
`submitted <= poolSize`, so the size read could not have changed the decision from "queue" to
"grow" — it only removes the `mainLock` acquisition. Only a failed transfer reads the pool size,
where the answer can actually change the decision. Owner: `StandardExecutorQueue.offer`.

**The internal force is a boolean, not an exception.** `StandardExecutorQueue.force` returns
`false` when the pool is shutting down; the executor's only caller is its own rejection handler,
which would immediately swallow a `RejectedExecutionException`. A normal return removes the
exception's stack capture from the shutdown path and leaves the shutdown check in one place.
The queue is still attached exactly once; an unattached `force` also returns `false` rather than
throwing.

**`ZetaExceptionHandler` is the single failure sink** for executor tasks (thread-local →
inheritable → default → WARN-log fallback) and never throws. It resolves the handler exactly once
and passes the call-site context through `ExceptionHandler#handleException(String, Throwable)` —
a default method, so a single-arg lambda remains a valid handler — and a handler that throws is
reported at ERROR with the original throwable attached as a suppressed exception: a broken
monitoring SDK must not make the task failure disappear from diagnostics. Inheritable handlers are
copied at thread-creation time by the thread that spawns the worker, not registered pool-wide;
`setDefaultExceptionHandler` is the process-wide path. Handlers must not throw: they run on paths
that are already handling a failure. `ExceptionHandler` and `ZetaExceptionHandler` are supported
extension points, not `@Internal`: the documented Micrometer/alerting use requires application
code to implement and register them.

Owner: `ZetaAutoConfiguration`, `StandardThreadExecutor`, `ZetaExceptionHandler`.

---

## Scheduling

**Fixed-rate work is phase-anchored.** `SafeScheduledExecutorService` keeps the cadence fixed to
the original phase; a run that overshoots skips every missed slot and lands on the next slot of the
original grid — no drift, no catch-up burst, and a slow run does not permanently shift the phase. `scheduleWithFixedDelay` is completion-relative. A task exception is routed to
the handler chain and never kills the cadence; `remove(originalCommand)` stops a periodic chain
by the original task. The chain's future reports `isDone()` only at terminal end — cancelled, or
ended by executor shutdown — never per-link: judging the current link would read done between a
link finishing and the next being scheduled, flickering on every healthy chain. `get()` reports the
chain, not the link: a link that was already running when `cancel()` arrived completes normally,
so the link's own `get()` would return a value while `isCancelled()` is true. The future therefore
re-checks the chain's cancellation after the link settles and throws `CancellationException`, as
the `Future` contract requires. Owner: `SafePeriodicTask.get`.

**A chain publishes its next link before a worker can see it.** `SafeScheduledExecutorService`
overrides `ScheduledThreadPoolExecutor.decorateTask` and installs the chain's `current` reference
there: the JDK calls that hook synchronously before `delayedExecute` enqueues the wrapper, so the
link is visible through `current` before any worker can run it. Before the hook existed,
`scheduleLink` assigned `current = schedule(...)` after the call returned. A zero-delay link could
start and schedule its successor before that assignment, and the scheduling thread then overwrote
`current` with the completed link — cancellation, `getDelay()` and `get()` all targeted the dead
link while the live successor waited in the delay queue. The first-run window had the inverse
symptom: `get()` could see `current == null` and throw "task was never scheduled" after the command
had already started. `scheduleLink` no longer assigns the return value, and a rejected initial
schedule clears the hook-installed wrapper instead of leaving `get()` to wait on a task the queue
never accepted. Owner: `SafeScheduledExecutorService.decorateTask`, `SafePeriodicTask.scheduleLink`.

**A chain's lifecycle is one 2-bit code, and its mode is the sign of the gap.**
`SafePeriodicTask` keeps `ACTIVE`, `TERMINATED`, `CANCELLED_NO_INTERRUPT` and
`CANCELLED_INTERRUPT` in one volatile `int` mutated by a `VarHandle` CAS, so `isDone()` is one
read, `isCancelled()` is a bit test, and `cancel()` needs no monitor: the cancellation and the
interrupt request publish together. The compensating cancel in `scheduleLink` reads the same code,
so a `cancel(true)` that raced a freshly queued link is never downgraded. `gapNanos` is positive
for a phase-anchored rate and negative for a completion-relative delay, which removes the separate
`rateMode` boolean; `scheduleNext` uses the sign to choose the formula. _Ruled out:_ a separate
atomic or lock for the cancel pair — three booleans plus a monitor was more state and more
acquisition for a transition that happens once per chain.

**Shutdown terminates every chain, immediately.** A chain waits for its next run as an ordinary
one-shot delayed task, which the JDK keeps queued at `shutdown()` (it only cancels true periodic
tasks) and then runs once after the full period. `shutdown()`/`shutdownNow()` therefore mark every
registered chain terminal, cancel its queued link and clear the registry: `isDone()` turns true at
the shutdown call, the command does not run again, and a chain object is not retained by a drained
queue. A run already in progress is allowed to finish; only the next link is cancelled.

**Control plane and data plane do not share a scheduler.** On the Worker, heartbeat emission
runs on a dedicated single-thread `workerControlScheduler`; `hotKeyScheduler` keeps the data-plane
work that performs synchronous AMQP sends (stale-eviction broadcasts). Sharing one pool let a broker stall plus a
mass-cooling wave occupy every thread, starving the heartbeat and making Apps evict a healthy
Worker from the ring — the connection-level isolation already in place was defeated at the thread
level.

**A maintenance tick is registered only when its collaborator exists.** The optional references
are resolved once when the configuration is constructed; absent L1 table, engine or TopK means
the corresponding `scheduleWithFixedDelay` call is never made, and a present tick never re-tests
the absence or unpacks a container handle on the scheduler thread. The expelled drain is a
memory-protection pass, not an event stream: production calls `clear()` on each queue (no batch
list, no per-item copy), and only DEBUG drains a bounded sample for the summary line.

Owner: `SafeScheduledExecutorService`, `ZetaSchedulingConfiguration`, `WorkerAutoConfiguration`.

---

## Annotations

**Three verbs, one annotation each; no Spring Cache abstraction.** `@ZetaCached`, `@ZetaPut` and
`@ZetaInvalidate` are the declarative form of `get`, `put` and `invalidate`. They replaced a
Spring Cache adapter — a `CacheManager`/`Cache` pair plus one advice implementing both Spring's
operation model and six companion annotations (`@CacheTtl`, `@Intercept`, `@Fallback`, `@Preload`,
`@Tag`, `@SkipSync`) — after the 2026-10 evaluation that narrowed the project's scope. The
companion layer was not one feature but a second product: rate limiting and concurrency isolation
(`@Intercept` with Bucket4j), exception-to-value degradation (`@Fallback`), a "preload" that
inflated sketch counts instead of warming values (`@Preload`), and a tagging sugar equal to one
facade call (`@Tag`). Each either duplicated a facade/caller capability or contradicted a stated
invariant: local interception made the local TopK an authority in the steady state, and its
`peek`-based path could serve a soft-expired value indefinitely because no loader was involved.
The adapter also could not be made correct on its own terms — the key was resolved twice in two
SpEL dialects, a plain `@Cacheable` had no loader (so a soft-expired hit could not arm a refresh),
and `@Caching`'s advice nesting could shadow a write with the read's marker.

**The method body is the loader.** A read passes the invocation as the engine's loader, so a
soft-expired entry serves stale while one background refresh runs, concurrent same-key misses
merge through SingleFlight, and the pre-read stamp is sampled by the engine inside the load. A
body exception returns a `BodyFailure` sentinel from the loader and is rethrown by the caller: a
business failure must not be counted as a data-source failure (that would drive the breaker open)
and must not be converted into a value. `null` is never stored. On the refresh leg the interceptor
throws the failure to the engine instead of returning the sentinel: there is no caller to rethrow
to, and the engine's failure path (WARN plus lease-on-failure) must see a real failure — a sentinel
the admission filter declines looks like "no new value", which silences the failure and skips the
lease, re-running the body on every stale read throughout the outage.

**An explicit key encodes one value exactly like one default-key argument.** An absent key over a
single-argument method encodes the canonical `DefaultKey` form (`[s5:alice]`), while `key = "#id"`
used to concatenate the raw value (`alice`), so a query and an invalidation written against the
same argument addressed different entries and the removal silently missed. The same split made
`key = "#root.args"` wrap the whole argument list in an `aO[...]` part instead of using the
multi-argument default form. `KeyPlan` now routes every explicitly selected value through
`DefaultKey.encode(namespace, new Object[] { value })`; a whole `#root.args` is recognized from the
parsed AST and uses the argument-list encoding, so it matches an absent key for the same call.
Arrays are still encoded by content, not by the identity hash of `Object.toString()`; a varargs
tail stays off the single-argument fast path so the value lookup can return the whole array (or the
expanded arguments) intact. The lookup normalizes every shape to a component-typed array: a packed
tail is returned as-is, an expanded tail is copied with `java.lang.reflect.Array` (so a
`String...` always yields `String[]`, never an `Object[]`), one expanded value is wrapped rather
than returned as a scalar, and zero expanded values yield an empty array instead of `null`. The
same normalizer packs the arguments before a reflective background reload, because
`Method.invoke` requires exactly `parameterCount` arguments with the final element an array.
Owner: `ZetaCacheInterceptor.KeyPlan`, `CompiledExpression.wholeRootArgs`,
`ZetaEvaluationContext.packArguments`, `DefaultKey`.

**The plan binds to the most-specific implementation method.** A JDK proxy reports the interface
`Method`, which may carry no parameter-name metadata and has no body, so both key discovery and a
refresh need the implementation method. `AopUtils.getMostSpecificMethod` therefore runs once per
candidate and is handed to plan resolution, so the same `Method` serves the annotation probe and
the cached plan instead of being looked up again inside the mapping function; a call that reaches
the interceptor without a prior pointcut match (a manually built proxy) resolves it on the first
call only. Its result is stored in the plan: SpEL binds `#p0`/`#a0`/parameter names from it,
`#root.method` is it, and a background refresh invokes it.

**The expression's parameter names come from the method that declares the annotation.** A key is
written against the interface or base method that carries `@ZetaCached`/`@ZetaPut`/
`@ZetaInvalidate`, so that method owns the names: an interface `load(String userId)` and an
implementation `load(String id)` must not make `key = "#userId"` fail startup as an unknown
variable. `parameterNames` therefore prefers the most-specific annotated view, then the
implementation method when the declaring method has no metadata, then any remaining view. The
plan is held in a per-user-class table, so one interface method with two implementations resolves
two plans. When the reported method and the most-specific method differ, the same plan instance is
stored under every view that contributed it.

**Resolution validates the whole method, not the last view.** A method can be reached through the
implementation, a generic bridge, a generic interface and several unrelated interfaces, and
`Class.getDeclaredMethods()` order is unspecified. The scan therefore groups every candidate method
by its most-specific method first and resolves the group once. A verb read directly from the
implementation method wins over the same verb on a view, because it is the caller's explicit
override; every other verb is read from the views with `getMergedAnnotation`, which resolves only
the view's own annotations. Verbs declared on two different interfaces still conflict, a generic
interface's verb still conflicts with the implementation's, and two unrelated interfaces declaring
the same verb with different attributes fail as ambiguous. `findMergedAnnotation` on the
implementation method cannot make that decision: its type-hierarchy search returns the first
interface's annotation as soon as one interface declares the verb, so the second interface's
different value is never seen. Direct lookups also keep the scan linear in the candidate views
instead of paying one hierarchy search per view. Resolving while scanning let the first view install
a plan and every later view bind to it without re-checking the verb — a `@ZetaCached` on an
interface silently became a `@ZetaPut` plan, or the startup conflict check disappeared entirely.
The direct probe is gated on a declared annotation (direct or composed): `getMergedAnnotation` on
the implementation also searches overridden declarations, so checking merely `declaredAnnotations
.length > 0` let an unrelated annotation such as `@Deprecated` on the implementation return an
interface verb as the "direct" one and hide the conflict. Owner:
`ZetaCacheInterceptor.computeValue`, `planFor`, `declaresAnnotation`.

**Inheritance dominance is a partial order, so every maximal view is compared.** Grouping by the
most-specific implementation method still leaves the superclass methods and every interface method
that the implementation inherits. Treating them as unrelated peers made a perfectly valid
`interface → base implementation → grandchild override` hierarchy fail startup: the base class
declared one value, the interface declared another, and the no-annotation grandchild made both
visible at once. The verb scan therefore filters the annotated views to the maximal ones under
declaring-type assignability, then compares their attributes: a class dominates its superclass and
the interfaces it implements, a sub-interface dominates its super-interface, and a dominated view
is that override rather than a peer. Filtering to maximal views, not folding into one running
winner, is load-bearing: with `SubInterface` extending `SuperInterface` and an unrelated
`OtherInterface`, scan order could visit `SuperInterface`, then `OtherInterface` (equal to the
current winner, so ignored), then `SubInterface` (which dominates the winner and replaces it),
hiding the real conflict between `SubInterface` and `OtherInterface`. The view set is also
deduplicated with `Method` equality because the class-plus-interface walk reaches one method
through several paths. Owner: `ZetaCacheInterceptor.findVerb`, `dominates`.

**The invocation's receiver is the unwrapped target object, not the proxy.** Spring's
`MethodInvocation.getThis()` already returns the target (the proxy is on
`ProxyMethodInvocation.getProxy()`), so the interceptor uses it directly as `#root.target` and as
the reflective-refresh receiver. A target that is itself an `Advised` proxy is unwrapped once in
`invoke` before the verb runs; every evaluation context and the reflective refresh reuse that
receiver, and a non-singleton `TargetSource` keeps the proxy it cannot unwrap. The unwrap stops at
the first object that still implements the reported method's declaring type: a Spring Data
repository proxy's singleton target is `SimpleJpaRepository`, which does not implement the
repository interface. Unwrapping through it made the plan lookup miss every repository method
(silent cache bypass) and made a background refresh reflectively invoke a method the target class
does not declare. The plan key follows the same rule: `planClass` returns the ultimate target class
only when it is assignable to the method's declaring type, otherwise the declaring type itself, so
the interface's plan table answers. When a proxy class reports a method its target class does not
implement, `plan` also falls back to the method's declaring-type table before giving up. Owner:
`ZetaCacheInterceptor.findUltimateTargetObject`, `planClass`, `plan`.

**Resolution is the fail-fast boundary.** The advisor's pointcut calls the interceptor's plan
resolution while Spring decides whether to proxy a bean, so a configuration error surfaces at bean
creation instead of the first request. Resolution validates the whole verb: exactly one of the three
annotations (a second is rejected instead of silently shadowing the first), a non-blank namespace, a
value return type (`void` and `Void` cannot produce a cacheable value), every configured expression
parsed with its role and method in the failure message, every positional `#pN`/`#aN` reference in
range, a method parameter that uses a reserved binding name (`root`, `this`, `result`, canonical
`pN`/`aN`) rejected when the method compiles an expression, and every other variable name known to
the context when parameter names are available — checked by walking the parsed AST once, which also
collects direct variable assignments (`#x = ...`), so a padded expression is still checked, a
`'#p1'` literal is not a reference, and the collected variable set is stored on the
`CompiledExpression` for the later `references` checks. An assignment declares its left-hand
variable from the point it executes: `(#x = 1) + #x` is legal, while `#x = #x + 1` and
`#x + (#x = 1)` are rejected as reads before assignment (the target itself is a write, not a
read). A reserved name cannot be an assignment target: `#result = ...` would write a map the
context answers from its bound result first, so the write could never be observed. Property and
index assignments (`#user.name = ...`) declare nothing, so the right-hand side and both sides of a
property assignment are still validated. A declared parameter name is legal unless it is reserved.
`#result` is admitted exactly where the caller declares it bound — a read key/TTL, a pre-invocation
key, and a `void`/`Void` invalidation do not bind a result. A permanent synced `@ZetaPut` is
rejected at resolution too. The alternative — compile or degrade per call — turns a typo into a
runtime log storm, lets a misconfigured cache address wrong keys until someone reads the logs, and
can run a business side effect before a write configuration error surfaces.

**An unresolved placeholder is a configuration error, not a namespace or a SpEL expression.**
`cache = "${users.cache}"` resolved with no `ConfigurableBeanFactory` (or with one that leaves the
token intact) used to become the physical namespace `"${users.cache}\0"` and silently miss every
real `users` entry. The namespace validator rejects any remaining `${` token. The TTL attributes
now do the same instead of handing `${my.ttl}` to the SpEL parser, where `$` is a legal identifier
and `{my.ttl}` an inline list, so the failure named neither the placeholder nor the missing bean
factory and surfaced as an EL1041E syntax error. One check covers `cache`, `hardTtl` and
`softTtl`; `key` and `unless` remain pure SpEL because `${...}` there is not an embedded value.
Owner: `ZetaCacheInterceptor.namespace`, `resolveTtlPlaceholders`.

**Numeric TTL literals follow one range rule.** Unary signs are part of the literal: `+60000L` and
`+1e3` fold to constants exactly like `60000L`/`1e3`, rather than becoming a dynamic `OpPlus`
evaluated on every write. A finite real literal outside the `long` range (`1e20`) fails startup
with the same "exceeds the long range" error as its digits-only spelling; only a runtime
expression result is clamped to `Long.MAX_VALUE`. The floating bound is `value >= 2^63`, not
`value > Long.MAX_VALUE`: IEEE-754 rounds `Long.MAX_VALUE` up to `2^63`, so the comparison let
exactly `9223372036854775808e0` through and the narrowing cast saturated to the permanent sentinel.
Unary negation widens an `Integer` to `long` before negating (the old `-i` left
`Integer.MIN_VALUE` negative and clamped it to the default TTL), and a negated `Long.MIN_VALUE`
folds through `BigDecimal`, where the out-of-range magnitude is rejected like any other literal.
Owner: `ZetaCacheInterceptor.Ttl`, `strictFloatingLiteral`, `constantNumber`.

**The loader snapshots the argument array it may reuse.** A soft refresh keeps the `ReadLoader`
after the foreground call returned, so the loader's `target` and `args` are the refresh's only
input. The argument array and every array element are copied at construction (including a varargs
tail), so a caller that mutates its own array cannot change what the background reload invokes; a
non-array mutable argument is still shared, which is why a method whose body depends on mutable or
request-scoped state must not declare a soft TTL. The one-shot advice chain is still released on
return, because the refresh never proceeds. Owner: `ZetaCacheInterceptor.ReadLoader`.

The class's table is a `ClassValue<Map<Method, Plan>>` keyed by the user class — `ClassUtils.getUserClass`
normalises a CGLIB proxy to the class the pointcut also resolves — so a CGLIB bean's pointcut
question and first invocation share one resolution instead of compiling every expression twice. The
invocation asks the `ClassValue` with the invocation's target class; a CGLIB proxy class delegates
to the user class's immutable table once, so the hit path performs one class-value read and one map
lookup with no key allocation, no `getUserClass` name probe and no reflection. Every view of an
annotated group is stored under its own `Method`, so a method from any implemented interface answers
with the table lookup alone; a method reported by a proxy class the user class does not declare
falls back to the declaring type's table (the nested interface-proxy case) and then to the
most-specific user-class method. `AopUtils.canApply` stops at the first matching
method, and a JDK proxy matches each method lazily on its first call, so the first pointcut question
about a class — not the first match — scans every candidate method of that class once.
`ClassValue.computeValue` builds the table in a local map and returns it only after the whole scan
succeeds; a throwing scan records no value, so a malformed second verb fails bean creation and the
class is retried instead of being half-validated. The entry is attached to the `Class` itself, so it
is released with the classloader instead of being pinned by a long-lived map keyed on `Class`, and
a later question about a view of the class is one lookup — present means a verb method, absent means
an unannotated method — with no annotation scan and no most-specific lookup; only a method reported
by a proxy class the user class does not declare can still take the fallback. The pointcut and the
scan filter static, private and non-bridge synthetic methods.

**Advice order brackets the transaction.** `ZetaCacheAdvisor.ADVICE_ORDER` is
`LOWEST_PRECEDENCE - 1`: inside method security (order 100), outside the default transaction
interceptor. The body runs inside the transaction because the read's loader proceeds into it,
while a write or an after-the-fact invalidation runs only after the transaction completed — a
rollback can never leave a cache side effect. The read fill follows the same rule: the loader can
read the transaction's own writes, but the engine registers the L1 store on `afterCommit`, so a
rollback leaves no entry behind. A key expression that does not read `#result` is
resolved before the body, outside the transaction, so a null argument or a null key fails before the
business side effect. A synced dynamic hard TTL is the only TTL evaluated before the body, because
a permanent value cannot be broadcast; the evaluated value is reused by the write. Every other TTL,
and `unless`, waits for the body's return (and, for a TTL, for the result to be non-null and not
vetoed). Only an expression that actually reads `#result` needs the returned value.
`@ZetaInvalidate(beforeInvocation = true)` runs before the transaction begins, which is also why it
bypasses after-commit deferral. A custom transaction order that places the transaction outside makes
the facade defer the write itself, so both configurations stay correct.

**No host AOP setup required.** An advisor bean is applied only when an auto-proxy creator exists,
and Spring Boot installs one only when AspectJ is on the classpath or another `@Enable*` feature
registers one. Relying on that would make the three annotations silently inert in a plain
`spring-boot-starter` application, so `ZetaAnnotationAutoConfiguration` registers the same
infrastructure-only creator Spring's own `@EnableCaching`/`@EnableTransactionManagement` register
(idempotently, upgrading rather than duplicating an existing one) and declares its advisor
`ROLE_INFRASTRUCTURE` so that creator considers it.

**A background refresh is a separate loader seam, never a resumed advice chain.**
`MethodInvocation.proceed()` is single-shot and not thread-safe, so it serves only the first,
synchronous load on the invocation thread — which is why the body keeps its transaction, security
context and MDC. The loader owns that one-shot chain: `load` claims it at most once, and the
foreground read clears it in a `finally` when it returns. A refresh never touches the chain, so a
refresh task that retains the loader after the caller returned (bounded by the engine's refresh
timeout) pins neither the proxy chain, the target and original advice state nor any evaluation
state. A later refresh always invokes the most-specific target method reflectively, outside the
transaction interceptor and without method security. That is true even when the refresh executor
runs the task inline on the caller's own thread (`CallerRunsPolicy`, a synchronous test executor).
The engine therefore hands a loader two entry points — `CacheLoader.load` for a foreground miss and
`CacheLoader.reload` for a refresh (the default delegates to `load`) — and the annotation
interceptor implements `reload` as the reflective call. A refresh failure is thrown back
through `reload`, so the engine's WARN and lease-on-failure path applies exactly as for a plain
loader. The refreshed value is written to the local L1 only; a method whose body depends on
request-scoped state must not be given a soft TTL.

**One context per phase, shared inside it.** A context is built from the call's immutable inputs —
the unwrapped target, the resolved class, the implementation method, the arguments and the
parameter names discovered once at plan resolution. The pre-body key and the synced dynamic hard
TTL share one context; they cannot observe a result. On a write, one post-body context binds
`#result` and serves `unless`, a result-dependent key and every TTL evaluated at write time, so the
same call does not rebuild a context for each expression. A read's TTL suppliers are lazier: the
engine may evaluate one later, on a refresh thread or inline on the caller's thread, so each
supplier builds its own context. Sharing one context between the two suppliers would let a variable
assigned by one expression leak into the other and would race when the engine evaluates them on
different threads. No context is cached across phases, so an off-thread evaluation
cannot read a half-released foreground binding and the interceptor needs no owner-thread
comparison, no release step for evaluation state, and no volatile state machine. The plan still
claims the method's reflective access once at resolution, unconditionally: under JPMS a public
member is not necessarily accessible when the package is opened but not exported, and setting the
accessible flag also lets `invoke` skip the caller-sensitive access check, so a refresh pays the
invocation only.

**The context shares reflection caches within one interceptor and keeps its own lazy assignment
map.** `ZetaEvaluationContext` implements `EvaluationContext` directly and returns one
thread-safe `ReflectivePropertyAccessor` and one method/constructor resolver set from a `Support`
object, so the accessor's reader/writer/type-descriptor caches survive the phase that first
resolved a property. The support is created by `ZetaCacheInterceptor`, not a static field: those
caches strongly reference `Class`/`Method`/`Field`/`TypeDescriptor`, and the type locator captures a
classloader. A JVM-wide static set pinned application classes across a DevTools restart or a second
Spring context and could resolve `T(com.app.Type)` against the wrong loader. Its assignment map is
created on the first `setVariable`, and a variable assigned `null` is kept as a binding instead of
being removed, so a later `#name` reads the assigned `null` rather than falling through to the
argument of the same name. A `StandardEvaluationContext` also allocates a variable map in its
constructor and accepts eight setter calls for delegates this context already owns as constants;
implementing the interface keeps the per-phase object to the fields it actually reads. The root is
built once per call and reused by every phase; a constant-key hit never builds one at all. The
three-argument `of` creates a fresh default support for direct callers and tests; the interceptor
uses the four-argument form with its own support.

Owner: `ZetaCacheInterceptor`, `ZetaEvaluationContext`, `CacheLoader`, `ZetaCacheAdvisor`,
`ZetaAnnotationAutoConfiguration`.

---

## Annotation reads

**A soft-expired hit serves stale on the annotation path.** Because every annotated read hands the
body to `get` as a loader, `SOFT_REFRESH` applies: the entry is served while one background
refresh runs behind the caller. The refresh goes through the loader's `reload` entry point — the
interceptor's reflective invocation, never `proceed()` — so the foreground load and the refresh stay
separate even when the executor runs the refresh inline on the calling thread (see _Annotations_).
A call without a loader (`HotKeyCache.peek`) cannot arm that refresh; it is a raw L1 probe, so it
serves whatever value is resident.

A business failure travels as the `BodyFailure` sentinel, so the annotation layer keeps it
distinct from a source failure, and an engine-internal failure fails fast instead of degrading to
a silent miss — a wiring fault in the cache engine should surface, not return "no data" from a
source that answered.

Owner: `ZetaCacheInterceptor`, `CacheLoader`, `HotKeyCache.get`.

---

## Annotation attributes

**Shared attributes: `cache` and `key`.** `cache` is required and encoded with the delimiter, so
there is no default namespace to fall into silently. `key` is SpEL evaluated once per call in
`ZetaEvaluationContext` (an `EvaluationContext` implementation with shared reflective delegates,
`#p0`/`#a0`/parameter-name bindings read straight from the most-specific method's arguments,
`#root.*`, and the `@beanName` resolver). The
attribute is stripped before compilation, so surrounding whitespace never changes the fast path.
A key whose whole parsed AST is one variable reference — `#pN`, `#aN` or a bare parameter name — is
recognized at resolution and reads `args[N]` directly, skipping SpEL for the common single-id key;
a text scan cannot do that without misreading `'#p1'` or `#p01`. A bare parameter name is looked up
in the discovered parameter list; a canonical `#pN`/`#aN` is an index. `#root`, `#this` and
`#result` keep the expression path. A non-canonical name such as `#p01`, `#accountId` or `#page` is
only ever a variable name, never an index reinterpretation.

Empty means the canonical `DefaultKey` encoding of the arguments: `[]` for none, otherwise typed,
self-delimiting parts — `s<len>:<text>` for a `String`, `c<len>:<text>` for a `Character`,
`q<len>:<text>` for any other `CharSequence`, `o<len>:<toString>` for any other object,
`B:`/`S:`/`I:`/`J:`/`F:`/`D:`/`N:`/`E:`/`Z:` for the boxed primitive scalars and the two `Big*`
types (no length prefix: their text cannot contain `,` or `]`), and `a<component>[...]` recursively
for arrays. A primitive array loops over its concrete type and tags each element with its scalar tag
(`int[]{1}` → `aI[I:1]`), so it neither boxes nor calls `Array.get`; a reference array keeps the `aO`
tag and the recursive path. Argument-boundary collisions, no-argument vs single-argument collisions,
array identity hashing and `String "1"` vs `Integer 1` vs `Long 1` vs `BigDecimal("1")` (or `'1'`,
or a `StringBuilder`) therefore cannot happen. The scalar type tests put the common final classes
(`String`, `Integer`, `Long`, `Boolean`) first: a final-class check is a class-pointer comparison,
while an interface check such as `CharSequence` reads the secondary supertype table, so the rarer
interface shape is tested after the wrapper classes. A directly self-referential array is rejected
immediately; an indirect cycle is rejected as soon as a nested reference array repeats an ancestor on
the current recursion path, so a branching cycle cannot expand exponentially before the depth cap.
The 32-level cap remains for a cycle longer than the ancestor path and for acyclic nesting, so a
malformed argument graph fails the call with a configuration error instead of overflowing the stack.
The ancestor chain is allocated only when a top-level reference array actually contains a nested
reference array, keeping a one-dimensional `Object[]` on the zero-allocation path. Two limits
remain and are documented rather than hidden: a non-array object is rendered by `toString()`, and
every reference array carries the same `aO` component tag, so different component classes whose
element encodings coincide (`String[]{"a"}` vs `Object[]{"a"}`) share a key — a value type or array
shape that must not collide needs an explicit key. A null top-level argument or a resolved null key
is a configuration error that fails the call rather than addressing the default key; the one
exception is an after-the-body invalidation, where a result-dependent key that resolves to `null` is
a no-op removal. A no-argument default key and a whole-literal key expression are precomputed as the
full physical key at resolution, so a steady-state hit performs no SpEL and no namespace
concatenation.

A read resolves its key and TTLs before the body, so `#result` is unavailable to them; declaring it
there is rejected at resolution, not evaluated to `null`. A pre-invocation invalidation has the same
restriction. Detection walks the parsed SpEL AST, so `key = "'#result'"` remains a legal literal key
and a property named `result` is not mistaken for the variable, while a whitespace-padded expression
is still checked. One walk collects the variable set and detects a user assignment. `#result` is
available wherever the expression may resolve after the body: `@ZetaCached.unless`, a `@ZetaPut`
key or TTL that reads it, and an after-the-body `@ZetaInvalidate`. A `@ZetaPut` key that does not
read it resolves before the body; its TTLs are evaluated when the write happens, except for the
synced dynamic hard TTL preflight. The post-body context holds the body result in a field with a
bound flag, so a bound `null` is distinguishable from an unbound `#result`, and a method parameter
named `result` is rejected at resolution, so argument binding cannot overwrite it.

The legal variable set is also static when parameter names are available: `#root`, `#this`,
`#result` only where the caller declares it bound, every in-range `#pN`/`#aN` and every parameter
name. `root`, `this`, `result` and canonical `pN`/`aN` are reserved: a method parameter with one of
those names is rejected at resolution as soon as the method compiles an expression, so a parameter
can never shadow a binding and the fast path and the context agree without a precedence rule. A
misspelled `#reslt` is rejected at resolution instead of evaluating to `null` and silently disabling
an `unless` or zeroing a TTL. An expression that assigns its own variables (`#x = ...`) is exempt
from the unknown-name check because its variable set is no longer static; a declared parameter name
is legal unless it is reserved.

**`${...}` placeholders are resolved once, at plan resolution.** `cache`, `hardTtl` and `softTtl` run
through the bean factory's embedded-value resolver while the plan is built, so a property-driven
namespace or TTL follows the same rules as bean configuration and costs nothing per call. `key` and
`unless` are deliberately excluded: they are SpEL, where `${...}` has no meaning, and rewriting
their text would corrupt a legal literal.

**`#root` is an explicit five-property object, not the target bean.** Rooting at the bean would
silently break every property below it, and an expression that fails there resolves a different key
(or throws) than the caller wrote. `ZetaEvaluationContext.Root` exposes `methodName`, `method`,
`args`, `target` and `targetClass`. The three plain properties use Lombok-generated getters;
`methodName` and `args` are hand-written because one derives from `method` and the other returns a
defensive copy. SpEL resolves `#root.x` to `getX()`, so a missing or misnamed accessor fails only at
evaluation time, on the one expression that uses it. `#root.args` is a defensive copy; the empty
argument array is immutable and is returned as-is.

**`unless` is the store veto on both storing verbs.** On `@ZetaCached` it runs inside the loader's
`isCacheable` after the body returned the value; on `@ZetaPut` it runs after the body returned and
before the `put`. Either way it evaluates at most once per load with `#result` bound; an expression
that throws at evaluation time fails open (the value is served/written) and logs at DEBUG,
because a broken veto must not turn every read into a miss or stop every write. A malformed
expression is rejected at resolution instead.

**TTL attributes are classified at resolution.** Surrounding whitespace is stripped; an optionally
signed integer literal folds to a constant — a negative value clamps to `0`, the configured
default — and to one prebuilt `LongSupplier` per attribute; a digits-only literal outside the
`long` range is a startup error with its role and method, not a fallback to SpEL (SpEL parses an
unsuffixed decimal through `Integer.parseInt`, so the fallback only produced an obscure parse
failure). A whole numeric SpEL literal (`60000L`, `1e3`) is read from the AST root and folded the
same way, through the same clamp, so it also takes the permanent-synced startup check instead of
degrading to a runtime evaluation. When both TTLs are constants the whole policy instance is
prebuilt too: `GetPolicy` for a read and `PutPolicy` for a write. A plain annotated read or write
therefore allocates no TTL supplier and no policy record. The permanent-synced write is deliberately
excluded from the write prebuild so the `@ZetaPut` plan builder (`writePlan`) raises its specific
startup failure instead of `PutPolicy.of`'s generic one. Anything else is compiled once and evaluated
only when the engine builds, promotes, renews or soft-refreshes an entry — never on a plain hit. On
a write, every TTL is evaluated after the body, once the result is non-null and not vetoed, except a
synced dynamic hard TTL: that one is evaluated before the body and its value reused, because a
permanent-synced configuration error must not follow the business side effect. A malformed
expression or an unknown variable fails resolution; a negative
result or a runtime evaluation failure degrades to the configured default (the runtime failure is
logged at DEBUG), because a runtime failure must not fail a read. A `BigInteger` or
`BigDecimal` result larger than `Long.MAX_VALUE` clamps to the permanent sentinel instead of keeping
its low 64 bits and wrapping negative.

**Write attributes.** A `@ZetaPut` key that does not read `#result` resolves before the body runs;
one that reads it resolves after the return — `key = "#result.id"` is the generated-id case. `unless`
always resolves after the body because it decides whether that value is stored. TTLs are evaluated
when the write happens, after the result is non-null and not vetoed, except a synced dynamic hard
TTL, which is evaluated before the body and reused: the split is what lets a null argument, a null
key or a permanent-synced hard TTL fail before the business side effect instead of after the
transaction committed. `sync` decides whether the write
converges across instances (fire-and-forget fan-out, TTL-bounded); the default is `true`, because a
write is an author's decision and peers should hear about it. A permanent hard TTL cannot be synced
— peers could only drop it through its TTL, so it would pin a stale value on every peer; the facade
rejects that combination through one `PutPolicy.requireSyncable` rule. A constant permanent TTL with
`sync = true` is rejected at plan resolution; a result-independent dynamic hard TTL is checked as
soon as it is evaluated, before the body; only a `#result`-dependent hard TTL is knowable after the
body, where the facade rejects it at the write.

**Invalidation attributes.** `beforeInvocation` decides the ordering; `scope` decides the blast
radius (`LOCAL`, `REMOTE`, `BOTH`). The interceptor maps them to one prebuilt
`InvalidatePolicy(scope, immediate = beforeInvocation)` per plan: "before the body" and "after
commit" cannot both hold, so a pre-invocation removal must run immediately. The default
after-the-body form resolves a key that does not read `#result` before the body and a key that does
read it after the return; the pre-invocation form resolves before the body runs, where `#result`
does not exist. A key that reads `#result` is evaluated by SpEL: a null result is a null
dereference, so a key that must tolerate a nullable return writes `#result?.id` — safe navigation
yields a null key, which is a no-op removal. An expression that handles null itself
(`#result != null ? #result.id : #fallbackId`) still runs and can remove its fallback key. A key
that resolves to `null` is a no-op; a null key from a non-result expression remains a configuration
error.

**Detection.** An annotated read counts exactly once: the read is one facade `get` call, which
counts the served hit or the load — never both. A miss fill is the same load, so nothing is
counted twice. A failed load still counts one access: the value came back as the `BodyFailure`
sentinel, which the engine sees as a loaded value before the admission filter declines it, and the
request did reach the key. Only a write takes the write-through broadcast path.

**`null` and exceptions are values on no path.** A `null` result writes no entry and the next
read re-invokes the body; a body exception propagates unchanged and stores nothing. The loader's
`BodyFailure` sentinel is never admitted.

**No cross-call thread-local policy.** Options, the key and the plan are call arguments; the body
runs on the caller thread (`callerRunsLoader`), so a nested cached method cannot observe an outer
method's policy and a method may hop threads without smuggling state. Each evaluation phase
builds its own context from immutable inputs, and a write or invalidation keeps its resolved key in
a local, so nothing per-call outlives the phase that created it. The read loader carries the
immutable inputs plus the one-shot chain, which `load` claims at most once and the foreground read
releases; a retained refresh loader therefore carries no advice chain and no evaluation state. No
static or `ThreadLocal` state carries policy across calls; a refresh still goes through
`CacheLoader.reload` (see _Annotations_).

Owner: `ZetaCacheInterceptor`, `ZetaEvaluationContext`, `CompiledExpression`, `DefaultKey`,
`SingleFlight`.

---

## API surface

**Three data verbs, one probe, one raw escape hatch.**

```java
V       get(key, loader|reader, GetPolicy)      // read-through; null = miss
Map<K,V> getAll(keys, reader, GetPolicy)        // batch; missing keys omitted
void    put(key, value, PutPolicy)              // local by default; synced() = cluster write
void    invalidate(key|keys, InvalidatePolicy)  // sync on by default
T       peek(key)                                // L1 only, no load, no detection
void    clearAll(InvalidatePolicy)              // one INVALIDATE_ALL; Scope.LOCAL = this JVM only
void    refresh(key, supplier, PutPolicy)       // version-before-load reload
```

**Options are closed per family.** `GetPolicy` carries only read knobs, `PutPolicy` only write
knobs, `InvalidatePolicy` only invalidation knobs. A mis-scoped knob is a compile error instead
of a silent no-op. `null` options mean `defaults()`.

**No boolean parameter pairs.** `TagMode` replaced `(skipDetection, skipReport)`;
`InvalidatePolicy.Scope` replaced `(local, remote)` booleans. Call sites read as intent.

**No second cache API.** The former builder/handle layer was deleted: it had one caller (its own
test), duplicated the options records, and offered runtime `Assert`s instead of type safety.
One-off configuration is a `GetPolicy` constant.

**`unsafe().raw()` is an escape hatch, not a peer API.** It exposes the live Caffeine table for
eviction introspection, bulk scans and diagnostics. A raw write skips version allocation,
broadcast, admission and the decision stamp; the caller owns every guarantee it skips. One of
those guarantees the caller cannot uphold alone: a raw removal cannot forget the SingleFlight
dedup entry (the engine wires the forget into every mutation site — design decisions: dedup
lifecycle), so `unsafe().forgetDedup(keys)` hands them the same tool. That is the shape of any
future addition to the view: a contract-preserving companion, never a new capability.

**`stats()` is the one aggregate the raw table cannot provide** (request-level hit/miss).
Everything else is a raw-table call.

**The local-TopK view returns the detector's `Item`.** `detector().localTopKeys(limit)` answers with
`List<Item>` — the same `(key, count)` record `TopK#list()`/`listTopN()` produce — instead of a
parallel facade record. `Item` is already part of the public `TopK` contract and of the actuator
response, so a duplicate shape hid nothing and cost a per-call mapping plus a second type to keep in
step. `listTopN` builds a detached list, so the snapshot belongs to the caller. _Ruled out:_ a
facade-local view record per observation surface; with one shape the copy is pure overhead.

**Admission governs the L1 store, not the read.** `CacheLoader.isCacheable` is
consulted wherever the engine is about to store a value it obtained on the loader's behalf: the read path (`HotKeyCache`), Worker HOT
warm-up, a peer `REFRESH` reload (both through the cluster loader's admitted load) and a peer `PUT`'s carried value
(`DefaultSyncContext.applyValue`, whose value arrives on the wire rather than
through a loader). An explicit `put` is the caller's own write and is never filtered, and the
predicate receives no key, so a per-key rule cannot be expressed there. A declined value is answered as "no authoritative value", so each plane takes its
existing fallback — HOT keeps its current L1 entry or skips with `VALUE_NOT_FOUND`, REFRESH degrades
to local invalidation — and a value the application declared non-cacheable can never be pinned for a
HOT hard TTL by a cluster decision. The predicate must stay cheap and context-free because it now
runs on AMQP listener and ordered-dispatcher threads, not only on the read path's load thread; a
REFRESH reload is checked once at the loader call and again at the shared apply gate, which is a
no-op precisely because the predicate is required to be side-effect free. _Ruled out:_ splitting the SPI into a local loader (with admission) and a cluster loader (without).
Admission is not a data-source property, so the split would codify the bypass instead of closing it;
and both planes must read one loader because a second data source would let a peer write land with a
value the read path would never have produced, breaking version comparison.

**Worker-only mode fails fast.** Data-plane methods throw `ZetaModeException`;
`isCacheAvailable()` lets callers gate. Silent no-ops hid wiring errors.

**TTL packing round-trips through one unit switch.** The unit-bit / 31-bit-magnitude `int` is
the stored form and `@EqualsAndHashCode` compares it, so `decode` followed by `encode` has to
return the same `int` or a no-op `EntryDraft.of(entry).build()` would produce an entry whose
getters all match but which is unequal to the original. The seconds transition therefore rounds
**up**: a duration just above the millisecond cap decodes slightly longer instead of falling back
into milliseconds, which keeps the decoded duration from ever shrinking below the request and
lands the exact second count back in the seconds unit. `TTL_ENCODE_INFINITE` is
`Integer.MIN_VALUE`, a magnitude no seconds encoding can produce, so the sentinel cannot collide
with a real duration; the accepted range closes at 2^31−1 seconds (≈ 68 years).

**`CacheEntry.toString` never prints the value.** Metadata identifies an entry; the value is an
arbitrary business object that would be expanded — and its PII leaked — by whichever log line,
exception message or debugger dump happened to format the entry.

**`ZetaDecision` stays public and enforces its null contract.** It is the value a decision handler
returns and the wire payload the App applies, so `@Internal` would contradict the extension point.
The Javadoc has always stated that `type` and `cacheKey` are never `null`; the compact
constructor enforces it instead of leaving the promise to convention, which also covers the
factories.

**`hitRate` is derived, never supplied.** The `ZetaCacheStats` canonical constructor recomputes
it from `hitCount`/`missCount`, so a caller cannot publish a rate that disagrees with its own
counters or forget the divide-by-zero case; the five-argument form stays available for
serialization and tests, and its `hitRate` argument is overwritten rather than trusted.

**`GetPolicy.defaults()` carries the concrete policy; `null` is for advice layers.**
`defaults()` materializes `SOFT_REFRESH` because a facade call that passes no options must still
arm the background refresh, and the batch path decides whether to build its reader closure by
comparing the policy directly. `GetPolicy.of(..., null)` keeps the tri-state: the engine
normalizes `null` to `SOFT_REFRESH` at its own boundary (`ReadContext`), so an advice layer can
merge "caller said nothing" with its own configuration without the engine inventing a second
default.

**Policies decide; contexts are the guarded write path.** `WorkerPolicy` exposes
`hot(WorkerDecision, HotValue)` returning a `HotAction` and `cool(WorkerDecision)` returning a
boolean. The framework calls `WorkerContext.resolveHot`, then the policy, then
`WorkerContext.settleHot`; a `Skip` (or a refused COOL) still fires exactly one terminal hook,
so a policy cannot forget to settle a decision. `SyncPolicy` mirrors it with `value`,
`invalidate` and `clear` over the sealed `Batch.Value` / `Batch.Clear`, and `SyncContext` owns
the guards, clear watermarks and dedup kills. Per-key ordering, batch reduction and hook
dispatch are framework-owned; a policy never implements them. The context is not a user
extension point: the autoconfiguration wires the default one and only the policy is a
`@ConditionalOnMissingBean`. The split is what keeps a custom policy from re-deriving the
second version check: `resolveHot` is a lock-free optimization, while `settleHot` re-checks
inside the entry compute. Failure it prevents: a policy that trusts a pre-check can overwrite a
newer decision or resurrect a value removed while its load was in flight. Owner:
`WorkerPolicy`, `SyncPolicy`, `WorkerContext`, `SyncContext`, `DefaultWorkerContext`,
`DefaultSyncContext`.

**Validation happens before any cache work, at one boundary.** A blank single key throws
`IllegalArgumentException`, a null callback `NullPointerException`, a negative TTL
`IllegalArgumentException`. Collection arguments are validated element by element at the same
boundary and **filtered instead of rejected**: `getAll`, `invalidate(Collection)` and
`forgetDedup(String...)` drop the null/blank elements (order and duplicates preserved) before the
engine is called, and an all-invalid collection is a no-op. The asymmetry is deliberate — a batch
assembled from a query result legitimately contains no-key rows, while a blank single key is a
direct caller error. `CacheKeysPolicy` owns both halves of the rule (`invalidCacheKey`,
`validKeys`); the engine assumes valid keys on every path, single-key and collection alike.

---

## Failure behaviour

**Expensive log arguments are built only when the line is enabled.** A call site whose arguments
cost real work (a UTF-8 body decode plus truncation, a `LongAdder.sum()` walk) gates them on
`log.isDebugEnabled()`; otherwise a storm of failures that only produces DEBUG lines still pays the
decode on every message. The line itself is plain SLF4J — no admission window owns the level.

**A static misconfiguration warns once; transient malformed input logs at DEBUG.** A node-id
collision is a deployment error that cannot heal without a restart, so one WARN plus the cumulative
counter is the actionable signal; repeating it on every inbound message would only be noise. A
malformed batch body is transient (a foreign or hostile peer) and repeats for every message under a
broken sender, so it logs at DEBUG — a single early occurrence must not silence the rest of the
process lifetime.

| Failure                             | Effect                                                                                                 | Recovery                                                                                                 |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------- |
| RabbitMQ down (App)                 | reports/sync sends fail or drop, counted; reads unaffected                                             | connection recovery; next tide/write re-sends                                                            |
| RabbitMQ down (Worker)              | decisions buffered then dropped on saturation, counted; App state not rolled back                      | key converges through the App's hot hard TTL / local TopK authority; undelivered HOTs return their seats |
| Worker cluster below health gate    | local TopK promotes COOL; reporter keeps counting                                                      | next heartbeat restores Worker authority                                                                 |
| One Worker dead                     | its keys re-hash to survivors                                                                          | ring rebuild from heartbeats                                                                             |
| Worker restarted                    | higher epoch accepted; old decisions demoted on next read                                              | next broadcasts re-stamp entries                                                                         |
| Source failing (background refresh) | stale value leased (never shortened, 30 min cap)                                                       | first successful refresh returns to normal                                                               |
| Source failing (synchronous read)   | the reader's cause is rethrown; a breaker-open load degrades to a hard-valid stale entry, else a miss  | the source recovers; the breaker closes and loads resume                                                 |
| L1 eviction / hard expiry           | next read reloads through SingleFlight                                                                 | normal                                                                                                   |
| Clock skew between nodes            | stamps merged on observe up to `clock-max-forward-drift-ms`; convergence bounded by skew + TTL         | later writes / TTL                                                                                       |
| Send executor saturated             | message dropped, counter incremented                                                                   | peers converge by next write or TTL                                                                      |
| Worker key flood                    | idle window reclaimed first; rejected past `max-tracked-keys` only when the sample finds none, counted | next scan admits (accelerated to the window length)                                                      |

---

## Metrics

**Monotonic totals are counters; live or decaying values are `Gauge`.** A lifetime count (decisions
emitted, queue drops, sync failures, rejected checks) must carry counter semantics so Prometheus
applies `_total` and rate() works. A `Counter` is incremented at the event; a `FunctionCounter` reads
a total the component already keeps (the detector's eviction and rejection tallies, the HOT rejected
tally). A value that can move down — current
queue depth, in-flight batches, lease debt, clock skew, `zeta.topk.total` (decay-adjusted recent
traffic, not a lifetime count) — stays a gauge. Registering a monotonic total as a gauge gives
dashboards a flat line and breaks `rate()`. Owner: `ZetaMicrometerAutoConfiguration`,
`WorkerDetectionMetrics`.

An unstarted component reads as `0` on its counters and `-1` on the live gauges that deliberately
expose absence. A counter never returns `-1`, because a negative sample is not a valid counter
value.

**Registration is completed after singleton creation.** A `MeterBinder` can be initialized during
the early reporter/access-counter cycle, before the cache, detector or reporter it reads exists;
its `ObjectProvider` checks then register nothing and every `zeta.*` series silently disappears.
A late `SmartInitializingSingleton` re-runs the same idempotent registrations after all singletons
are ready, and skips a registry that already has a `zeta.*` meter so the normal path does not log
duplicate-registration warnings. Owner: `ZetaMicrometerAutoConfiguration`.

**A per-rule total is the Micrometer counter, and the meter registry owns it.** `RuleCounters`
holds three `Counter` handles registered when the rule is published and one endpoint-only `LongAdder`
for aborts. The `Counter` is the single total: a rule event increments the meter and the endpoint
reads `count()` from the same handle, so a delta cannot disagree, and a late cool-down after the id is
retired increments a detached meter that is never scraped again. Micrometer's registry holds the meter
strongly, so no state object has to be kept reachable. Registration happens only after the whole
snapshot is built, so a publication that fails while deriving its indexes registers nothing. Owner:
`WorkerDetectionMetrics`, `RuleResolver`, `RuleBinding`, `RuleCounters`.

_Ruled out:_ the `volatile long` fields updated through `VarHandle.getAndAdd` with a
`FunctionCounter` reading each field. That shape was correct — the published binding retained the
counters object the meter held weakly — but it required three VarHandles, a hand-written weak-state
contract, and a second read path for the endpoint. The `Counter` removes that contract and keeps the
endpoint on the same value the scrape sees. The cost is that a meter-less construction (the
`WorkerDetectionMetrics(null)` used by tests and embedded callers) reads zero for the three rule
counters; the endpoint's aborts stays a `LongAdder` and is unaffected. Owner:
`WorkerDetectionMetrics`, `RuleCounters`.

---

## Class layout

**Internal seam types keep one interface and one implementation.** `HealthView`,
`ConsistentHashRing`, `CircuitBreaker`, `EntryLifecycle`, `SingleFlight` and
`BackgroundRefresher` each pair an interface with one implementation in the same feature package: the
interface is the seam Spring's `ObjectProvider` / `@ConditionalOnMissingBean` resolution and the
tests program against, the implementation carries the state. The pair costs one dispatch hop and a
second Javadoc copy, which is why only seam types get one — a helper with one call site is a private
method, not a file. _Ruled out:_ folding the manager into the ring (`RingManager` absorbed by
`ConsistentHashRing` 2026-10): the ring now reconciles itself from the `HealthView` alive set
and routes with the health view as the liveness source, so the manager was a middle man over the
ring's own API. _Ruled out:_ the SRE adaptive limiter (`SreRateLimiter`, 2026-10): probabilistic
HOT-promotion throttling added a gate, an accounting path and five properties for backpressure
the report path already applies through its bounded queue.

**The consumer-plane configuration is one interface with two implementations.**
`AmqpConsumerProperties` carries everything the two AMQP listener planes build their container and
jitter scheduler from — exchange and per-instance queue names, consumer concurrency, prefetch,
scheduler pool size and auto-startup — so `SyncConfiguration` and `WorkerListenerConfiguration`
share one builder and a change to the container contract cannot drift between them.
`CacheSyncProperties` and `WorkerListenerProperties` keep their plane-specific knobs and implement
the interface. Owner: `AmqpConsumerProperties`, `ZetaAmqpAutoConfiguration`.

**Published strategy SPIs stay interfaces.** `SyncPolicy` and `WorkerPolicy` are
documented in the README as caller-replaceable: the interface's default methods run the standard
pipeline through `SyncContext` / `WorkerContext`, and a caller overrides only the
decision kind whose policy changes. The contexts are the framework-owned write paths; the policy
is the published seam. The same applies to `SingleFlight`, `CacheLoader`, `TopK`, `SyncHook`, `WorkerDecisionHook`
and the message records — each has a real second implementation (test doubles, benchmark stubs or
caller-supplied loaders).

**Small helpers live with their only caller.** The TTL-jitter arithmetic lives in `TtlPolicy`; a
helper with one call site is a method with a name, not a file.

**AMQP message plumbing is one type.** `AmqpMessageReader` holds the numeric-header/body readers
and the nested `AppIsolationFilter` used by both listeners. The old split into
`util.AmqpMessageReader` and `sync.AppIsolationFilter` bought nothing: both are message plumbing
with no independent lifecycle, and the duplicated foreign-app predicate had already diverged
(`equals` vs `contentEquals`). One type keeps the predicate, drop counter and DEBUG log line
shared. _Ruled out:_ keeping the two helpers in separate files.

**Admission stamps, not thread-local probes.** `circuitbreaker` binds a HALF_OPEN probe
reservation to the returned stamp; the caller settles exactly one of success/failure/abandoned
with it. This keeps batch loads (one thread, many keys) and nested cache calls from consuming
each other's probe quota. Probe credit is asymmetric and applied per episode — a failed recovery
episode multiplies the next quota by 0.8 (floor 0.25), a recovered one earns 1.1 up to 1.0 — so a
source whose episodes keep failing converges to single-probe episodes while a healthy one climbs
back to the full budget.

**Invalidation tombstones are bounded and versioned.** `EntryLifecycle` records the highest
invalidation version per key for a bounded retention (10k keys / 10 minutes by default,
`zeta.local.cache.tombstone-*`) plus a table-wide clear watermark. A fill at or below either is
dropped, closing the window where an absent entry lets a read-miss resurrect a value an
`INVALIDATE` just removed. The per-key boundary is inclusive, not strictly newer: two nodes can
mint the same millisecond stamp (the clock carries no node ID for ordering), so a delayed
same-version PUT after an INVALIDATE would otherwise re-install the removed value. The defaults
outlive any realistic in-flight read by orders of
magnitude; the knobs exist so a mass-invalidation workload can trade guard window for guard
memory instead of letting the size cap start evicting recent tombstones.

**The ring is rebuilt, never patched.** `ConsistentHashRingImpl` publishes a fresh `(hashes, nodes)` snapshot
through one `AtomicReference`; readers binary-search it and walk at most one full cycle looking
for a live node. A live-set change rebuilds; an unchanged set only republishes nothing. The hash
is the pinned 32-bit mixed hash — changing it reshuffles every key.

Owner: `circuitbreaker`, `EntryLifecycle`, `HealthView`, `ConsistentHashRing`,
`ReportFeedLoop`, `TtlPolicy`, `AmqpMessageReader`.

---

## Ruled out

Each entry was implemented, measured or reasoned about, then removed. Do not re-propose one
without new evidence.

**Expiring a hot accepted record at twice the queue age.** The age bound orders decisions that are
still queued; it says nothing about how long the cluster can hold a hot key. A hot record that
expired after the bound turned a later COOL into a "ghost" demotion with no predecessor, so the
funnel elided it and every App stayed hot until its hard TTL. A hot record is now retained until a
COOL is accepted; only the cold record keeps the queue-age bound. Owner:
`WorkerBroadcaster.AcceptedDecisionExpiry`.

**Reserving a tombstone replacement directly from the hard ledger.** A window sampled as
`EVICTED` has already returned its seat, so replacing it is a new admission. Reserving directly
bypassed the soft-cap sample and the saturation window: an admission could consume hard headroom
while reclaimable idle windows stayed tracked, and the table could drift toward the hard limit
without ever running the pass that would have freed them. The tombstone now enters the same
`tryAdmit` gate as a first touch. Owner: `SlidingWindowDetector`.

**Releasing the capacity-sample lock before removing the victims.** The two-phase shape needed an
epoch to decide which sample's verdict was newest, because removals ran after the lock and two
walks could finish out of order; it also allocated an `ArrayList` of victims on every cap
admission. The single locked pass removes the ordering question and the allocation; `tryLock`
keeps the admission non-blocking, and the hard headroom bounds the longer hold. Owner:
`SamplingReclaimer`.

**Packing a window's slice number into its slot array.** A `ConcurrentHashMap<String, int[]>` whose
last slot holds a 32-bit slice number relative to the process origin would remove the `KeyWindow`
object — about 24 bytes per tracked key, 24 MB at the one-million-key ceiling — and one pointer
dereference per report. It buys that with an overflow horizon: a signed 32-bit relative slice spans
2^31 slices, which is 24.8 days at the 1 ms slice `WindowGeometry` accepts, so a long-running
process would wrap and read live windows as stale (or worse). Forcing slices to at least 10 ms
would stretch the horizon to 248 days, but that is a configuration restriction imposed to save
24 MB at the design's maximum cardinality, and the dereference it removes is one field read on a
path already dominated by the map lookup. `KeyWindow` stays. Owner: `SlidingWindowDetector`.

**Pre-evaluating every result-independent `@ZetaPut` TTL before the body.** The only TTL evaluation
that can reject a write is a synced hard TTL resolving to permanent; `evaluateTtl` degrades every other
runtime failure to the configured default. Evaluating the rest before the body spent a SpEL context
and an evaluation on writes whose body returned `null` or whose `unless` vetoed the value, and moved
result-independent TTLs that read mutable target state to the pre-body snapshot. The interceptor now
preflights only the synced dynamic hard TTL and evaluates the rest in the shared post-body context.
Owner: `ZetaCacheInterceptor`.

**Allowing a business parameter to shadow `#result`, `#pN` or `#aN`.** The precedence rules had to
be duplicated in `CompiledExpression`, the key fast path and `ZetaEvaluationContext`: a parameter
named `p0` changed the positional binding, a parameter named `result` changed the body result, and
the three sites had to agree on which one won. Those names are reserved instead; a method that uses
one is rejected at resolution, and the context holds `#result` in a field that argument binding
cannot reach. Owner: `CompiledExpression`, `ZetaEvaluationContext`, `ZetaCacheInterceptor`.

**Relying on `MethodBasedEvaluationContext` for lazy parameter binding.** Its lazy load is
all-or-nothing: the first lookup of any name it cannot resolve runs `lazyLoadArguments`, which calls
the parameter-name discoverer again (Spring does not cache it, and `getParameters()` clones the
`Parameter` array) and writes `3N` entries into the context's variable map before the lookup
returns. A context is built per evaluation phase, so every expression that read a variable paid that
once. A bound `null` result was also indistinguishable from an unbound variable, so reading
`#result` on a null-returning method triggered the same load. `ZetaEvaluationContext` implements
`EvaluationContext` directly with shared reflective delegates and resolves a name against the
pre-discovered parameter names and the argument array — an assigned variable keeps precedence
through a lazily created assignment map that retains an assigned `null` as a binding — and
`#result` is a field plus a bound flag — so a lookup is O(1) in the references actually evaluated,
performs no reflection and writes no entry.
Parameter names are discovered once per method at plan resolution and reused for every expression
and every call. Owner: `ZetaEvaluationContext`, `CompiledExpression`, `ZetaCacheInterceptor`.

**MVCC version chains in L1.** A per-key version chain plus a read-visibility rule would not remove
the read-path predicates: the cache's unit of consistency is one key, `ConcurrentMap.compute`
already publishes a new immutable `CacheEntry` atomically, and `dataVersion`/`decisionVersion`
already order writers. MVCC's core guarantee — a reader pinned to an older snapshot — is the
opposite of the cache contract, which must never serve a value an applied invalidation has
superseded. A chain also adds a per-entry pointer, multi-version retention and a reclamation rule
(refcount/epoch) on the hottest path, while the batch read deliberately promises no multi-key
snapshot. The scattered predicates were consolidated instead: `classifyHit` for reads and
`writeRefused` for writes. Owner: `HotKeyCache`, `EntryMutations`.

**A four-tier TTL encoding (2-bit unit + 30-bit mantissa).** It bought a range (~122k years) no
cache uses and charged for it with a unit ladder, a reserved mantissa to keep the permanent
sentinel unambiguous, and a ceiling-rounding argument repeated at every tier. The one-switch form
— bit 31 selects seconds, bits [0, 31) carry the magnitude, `Integer.MIN_VALUE` is permanent —
keeps the same four bytes, covers 68 years, and reduces idempotence to "a decoded second count
lands in the seconds unit again" (`CacheEntry.encodeTtl`).

**A Lombok `@Builder` beside `EntryDraft`.** Two construction APIs for the same entry let a
caller supply raw timestamps that disagreed with the durations, bypassing the TTL arithmetic and
the draft's expiry computation. `EntryDraft` is the only path now; `CacheEntry`'s constructor is
package-private, and test fixtures build through the draft.

**The health-view slot table.** It gave each Worker a dense slot and carried the slot in the
decision stamp so the Decision-Validity probe could skip one map lookup. An isolated, clock-free
JMH probe put the slot form at ~3.5 ns against ~8.3 ns for the map-plus-array fallback; with the
monotonic clock read both forms pay (~45–50 ns), which dominates the difference. A slotless
design also drops the array from the fallback, leaving a net advantage of a few nanoseconds at
the cost of a second index, a second table, a stamp field and three `HealthView` methods. At 1M
HOT probes/s the table bought well under 0.5% of a core. Removed; `DecisionStamp` carries only
node, epoch and version, and the probe is one `records.get` plus the shared liveness judgment
(2026-11).

**`HeavyKeeper.warm` (persisted-snapshot warm-up).** The sketch-seeding contract was correct
(membership + sketch, outside the admission lock), but no production path ever restored a
persisted snapshot — the only callers were tests and benchmarks. Deleted with the `TopK.warm` /
`TopK.estimatedCount` surface; a future warm-up should re-seed through `addDirect(Map)` and
re-derive the sketch-seeding discipline it had (2026-10).

**A rolling-window QPS counter (`util.window.RollingWindow`).** It fed `@Intercept(QPS)`, which counted accesses in 10 × 100 ms buckets: a burst split across a bucket boundary could stay under the threshold, and the fixed bucket count both capped burst tolerance and spent per-key memory on sub-intervals. It was replaced by a per-key Bucket4j token bucket in `InterceptGate` (capacity = `threshold`, greedy refill) — and then the whole annotation family went away in the 2026-10 redesign (design decisions: annotations): throttling and concurrency isolation are not a hot-key cache's job, and the "cluster threshold" was really per-instance. Both classes and their tests are gone (2026-10).

**Redis for versions, rules, locks or epochs.** Four roles, one dependency, and the degraded
dual-space arbitration was itself the source of a read-your-writes defect (`0` meant both
"version zero" and "no version"). Replaced by the node-local stamp clock with a merge floor, rules
published from the Worker's own configuration, and a file-backed epoch. No compatibility layer.

**Gossiping a runtime rule snapshot, versioned by the startup wall clock.** A snapshot carried on
the heartbeat and ordered by a wall-clock timestamp lets a restart or a scale-out outrank — and
therefore overwrite — cluster-wide changes made earlier, because the fresh process's clock is
always the newest; a rolling restart then silently resets the cluster to its YAML values. Rules
stay local to each Worker: the published snapshot is one process's `RuleResolver.publish`, and
cross-Worker consistency is observed through the `rulesHash` each detection endpoint reports,
never negotiated.

**A vendored 550-line math utility (`FastMath`).** It bundled PELT decay tables, Murmur3
x86_32/x64_128 ports, FastRange and power-of-two alignment. Once hash bit-identity stopped being
a requirement, every remaining call site has a JDK or six-line replacement:
`Integer.highestOneBit` (alignment), `Math.exp` (decay), the inline
`(hash & 0xFFFFFFFFL) * range >>> 32` (bucket mapping) and `Hashes.mix32` (avalanche over the
cached `String.hashCode`). The dependency set is unchanged — no Guava.

**A distributed lock (`LockProvider`/`RedisLockProvider`).** One implementation, no second
provider, no in-tree caller. Cross-instance locking is the application's problem.

**A rule subsystem (blacklist/whitelist with `ZetaBlockedException`).** No in-tree caller; the
three annotation verbs cover the declarative surface, and admission decisions belong to the
caller. A rule store, evaluator and sync protocol did not pay for themselves.

**An L2, a regionalized keyspace, or a logical hot limit.** A second tier or a per-region key
space adds a consistency problem Zeta does not need; L1 + TTL convergence is the product.

**A sharded local table (`zeta.local.shard-count`).** The property was bound, documented and
advertised as partitioning L1, but no code read it: `HotKeyCache` owns one Caffeine table and the
annotation layer encodes namespaces into the key. It implied a partitioning feature that does not
exist; removed with its metadata entry and CONFIG rows. Owner: `ZetaProperties`.

**Per-namespace tables and a `name::key` physical key.** Two disjoint namespaces for the same
logical cache name, a mis-routing bug for keys containing `::`, and two extra concatenations and
hashes per named hit. The engine instead keeps one table and encodes the namespace with a NUL
delimiter (design decisions: cache table).

**Named-cache handles (`Zeta.newBuilder()` → `ZetaCache`).** Redundant option schema, one caller,
fake compile-time guarantees.

**An element filter inside `HotKeyCache`'s collection methods.** It put one key rule in two
layers, each free to drift: the facade rejected a blank single key while the engine silently
dropped a blank batch element, and nothing said which layer owned the answer. Validation is a
boundary concern (design decisions: api surface), so `DefaultZeta` now filters every collection
argument through `CacheKeysPolicy.validKeys` and the engine assumes valid keys everywhere. Cost:
one reference-array copy per collection call, paid once at the boundary instead of a per-element
check inside the engine's own loop.

**Value compression / a value codec in L1.** Per-hit decompression for values that are often
incompressible, plus a second place where the stored form differed from the caller's value.
Values are stored as passed.

**Null caching, negative caching, a null sentinel.** Conflates "absent" with "cached absence",
forces every reader to distinguish two empties, and interacts badly with admission and
versioning.

**`LongAccumulator` for TopK membership counts.** `reset()` drops concurrent accumulates; a CAS
re-synchronisation loop does not.

**`try/finally` dedup invalidation on the load path.** Destroys the dedup cache on every
successful load and forces the next caller to re-run the supplier; success ages out through the
dedup TTL instead.

**Publisher confirms.** All three publish paths are state-oriented; confirms add latency and
channel pressure for no durability gain.

**A damped admission limiter on the report path (`BbrRateLimiter`).** It admitted a flush against a
Little-Law estimate (`maxPass × minRt / bucketDuration`), a damped baseline, a cubic position ratio,
CPU derating and a downstream-yield step. The estimate measures the publish concurrency the reporter
already achieves, not the capacity of the publish pool: at the default cadence and millisecond
publishes it reads below 1, so `minInFlight = ring.nodeCount()` decided every admission and the
window, bucket, CPU and cooldown knobs changed none. That floor is one flush's fan-out, not the
consumer count, so a slow broker capped in-flight below the number of publishing threads and whole
flush cycles were dropped that the queue would have absorbed. The bounded queue already bounds the
backlog in count (`queue-capacity`) and in age (5 s staleness). Removed with its interface, six
properties and eight meters. _Ruled out, too:_ the separate sampled CPU monitor
(`SystemLoadMonitor`) that once fed the derate — a 500 ms poller, an EMA, a `zeta.cpu.load` gauge and
two tuning knobs for a signal that was never on the admission path's critical resource. Owner:
`KeyReporterImpl`, `ZetaProperties`.

**A second `RabbitAdmin` to pre-declare exchanges.** Boot's admin is `@ConditionalOnMissingBean`
on the `AmqpAdmin` return type, so a second admin silently switches it off and leaves every
exchange, queue and binding undeclared. The control-plane exchange is declared from the
publishing template instead.

**`CALLER_RUNS` on the load or send pool.** A correctness bug, not back-pressure.

**Majority-quorum cluster health.** The denominator was never wired, so the documented behaviour
and the real behaviour disagreed. One third of observed Workers is the honest contract.

**Blocking the stamp clock on sequence overflow.** It serialized every write on one monitor and
produced p99 spikes on cold-start paths. Rolling the stamp forward arithmetically is monotone and
wait-free.

**Inter-bucket padding in the breaker failure counter.** `failureCounts` used a 128-byte stride
(`long[buckets × 16]`) to give each time bucket its own cache lines, but every concurrent failure
write targets the one live bucket — padding cannot reduce that contention, while it taxed every
rate scan with a cache miss per bucket and cost 16× the memory. The only cross-bucket writer is
the rotation's reset of the bucket it enters, too rare to justify the layout. Replaced by a plain
contiguous `long[]` (2026-10). Owner: `CircuitBreakerImpl`.

**A monitor per breaker bucket.** Each time slice carried its own `Bucket` monitor, on the premise
that ten buckets spread settlement contention across the window. They do not: the buckets are clock
slices, not thread shards, so every concurrent settlement of the live slice targets the same
monitor while the other nine sit read-only until the clock reaches them. The monitor also made the
rate scan take one lock per bucket. Replaced by one packed `window | failures | successes` word in
an `AtomicLongArray`: the rotation is one CAS, the scan is one atomic read per bucket, and the ten
objects disappear (2026-11, design decisions: load protection). Owner: `CircuitBreakerImpl`.

**Folding the breaker's `Epoch` into one `long`.** The six fields fit the bit budget only after
truncating the generation and the timestamp precision, and the premise was that CLOSED traffic pays
for them. It does not: the CLOSED path only reads the immutable reference, and a new `Epoch` is
allocated a handful of times per HALF_OPEN episode — a few objects per recovery, not per request.
Trading timestamp wraparound and generation truncation for those allocations is a negative-return
optimisation, so the reference stays (2026-11). Owner: `CircuitBreakerImpl`.

**A versioned success watermark in front of the breaker's success sweep.** The trip gate packed a
16-bit window version above a 48-bit success total so a steady-failure window could skip the
`buckets × cells` sum, and the write-back compared-and-swapped it under a version check. Every
rotation and every reset had to bump that version, and the CAS compared the _shifted_ version
against the _raw_ packed value: it could only succeed while the stored total was zero, so the
second evaluation of any window that had seen a success spun forever on the settling thread. The
rotation calls `evaluateThreshold` last, on the same shared thread, so the spin also trapped the
only thread that could have advanced the version — a permanent livelock of the failing request and
of every shard's window, with the breaker still `CLOSED`. Deleted rather than repaired: the volume
floor alone is an exact gate, needs no cross-window state, and leaves nothing to keep in sync
(2026-10). Owner: `CircuitBreakerImpl`.

**A separate `lastHalfOpenAttempt` gate, and episode retirement by zeroing the quota.** Two fields
and a packed `probeState` for one state machine, and every gap between them was reachable. The
`OPEN → HALF_OPEN` right was a flag reset by each episode exit, so a thread that won the flag and
then lost the quota CAS left it set while the state went `OPEN` — no path resets the flag in
`OPEN`, so every later admission failed the same CAS and the breaker never probed again (a
permanent `OPEN` until restart). Retiring an episode as `(generation + 1, slots = 0)` was
indistinguishable from a fresh episode to any thread that read the packed quota before the state
write landed: it admitted a probe into the episode that had just failed, which could reopen again
and pay the credit penalty a second time, or close the breaker just before the reopening thread
overwrote it. Both windows are gone once state, generation, quota, credit and timer are one immutable
`Epoch` swapped by one CAS (2026-10, design decisions: load protection). Owner:
`CircuitBreakerImpl`.

**One merged per-key state object across the window table and the HOT registry.** Tempting on
paper — one map lookup and one monitor per report instead of a window probe plus a registry entry —
and rejected on the evidence: the two tables have different shapes, lifetimes and admission
policies. The detector's entry is a `2W`-slot `int[]` whose slice crossing is serialized on the
entry monitor while its adds stay lock-free; the registry entry is the lifecycle record guarded by
a stripe and linked into the timer wheel. Merging would move the detector's add under the
registry's monitor, which is the one lock the design keeps out of the report path. Owner:
`SlidingWindowDetector`, `HotKeyRegistry`.

**Micro-batching decisions into one AMQP message.** `DecisionDispatcher` sends one decision per
`rabbitTemplate.send`. Draining with `queue.drainTo(batch)` and aggregating a chunk into one message
would cut frames and syscalls by one to two orders of magnitude during a mass-heat event — and it
would also break the unit the rest of the plane is built on: the per-decision result is what the
dropped-decision counter and the supersede check key on, and the receiver's `decisionVersion >=`
guard is per key. A batch message would need per-entry results, a new wire type, and receiver-side
per-entry ordering discipline — a protocol change to save syscalls on a thread that is idle between
bursts. The bounded single drain thread is the cheaper bound. Revisit if a profile shows the send
syscalls, not the decision pipeline, dominating a mass-heat burst. Owner: `DecisionDispatcher`,
`WorkerBroadcaster`.

**Repeating a COOL to improve its odds.** A chained COOL repeat (first repeat scheduled on send,
each fired step scheduling the next, stopping when a newer decision supersedes the key) was the
delivery guarantee for the one decision a lost send could pin: a dropped COOL leaves the entry at
its hot TTL until hard expiry, while a dropped HOT self-heals on the next promotion. Measuring it
against the machinery — a scheduler task per live episode, a suppression check against the
broadcaster's accepted record, and a re-promotion hook to cancel the chain — the
backstop it duplicated was cheaper: the App's hot hard TTL, and the local TopK taking TTL authority
while the Worker is unhealthy. A mass-cooling wave also turned the chain into load: 20k COOLs
within one scan queued 80k delayed tasks, each live for up to two minutes even after its key
re-heated. Removed; the pipeline is one send per decision (design decisions: sync messages).
Owner: `WorkerBroadcaster`.

**Leaving a dropped HOT's seat held.** The previous fire-and-forget contract stopped at the
dispatcher: a HOT that expired, failed to publish, or was discarded at shutdown left its registry
entry in place, on the assumption that a HOT self-heals on the next promotion. It does not under
`COOL_FOR_LOW_QPS`: the entry's window stays above the cool band, every cooling check re-arms it,
and the already-hot fast path suppresses the next promotion, so the seat leaks until the cap fills
and detection silently stops. The replacement is not a retry — it is a synchronous hand-back of a
promotion that never reached any App, identity-guarded by the entry's `version`; a delivered HOT that
was reported as failed converges through the next report's newer version. Owner:
`DecisionDispatcher`, `HotKeyRegistry`.

**A parallel `lastAccessTime` map beside the detector's window map.** The timestamp lived in its
own map, so `addCount`'s timestamp write and `evictStale`'s removal shared no lock: the evictor
needed a conditional timestamp-remove plus an orphan sweep for a timestamp whose window it had
already dropped, and a reordered pair could still lose an add — the counts landed on a window no
reader could see. Keeping the timestamp in the same `KeyWindow` entry gives both operations one
monitor: a removal that wins leaves the add to re-create a fresh window, and an add that wins fails
the evictor's re-check. The sweep also drops its `ArrayList` candidate snapshot —
`ConcurrentHashMap.forEach` is weakly consistent and its `compute` may remove, so one in-place pass
is safe. Owner: `SlidingWindowDetector`.

**Packing `HE.wheelState` into an existing word, or storing its timestamps as `int`
offsets.** Neither saves memory. The layout is one cache line — 12-byte header, the `int` state
filling the alignment gap, four `long`s and four references — and removing the `int` would leave
the same padding, not shrink the entry; `wheelState` is also guarded by the wheel lock while
`nextCheckAt` is guarded by a registry stripe, so folding them into one word would join two lock
domains. An `int`-millisecond offset from `promotedAt` overflows after `2^31 - 1 ms` (about
24.85 days), which a long-lived hot key reaches routinely. Owner: `HE`, `TimerWheel`.

**A `-1` tombstone without an entry monitor.** A marker alone cannot close the fast-path lost-add
race: a thread reads `lastAccessTime` in the gate and writes its slot after, so a retirement
between those two steps is still invisible. That race remains a documented bounded loss. The
current design uses the tombstone for a different race, introduced by the entry-monitor path: a
slice-crossing add that sampled a reference before eviction now updates that reference directly
instead of re-entering `windows.compute`, so without a marker it could write into a detached buffer
after the mapping was replaced. Under the entry monitor, the sentinel is the only way the add can
tell that its reference is retired; readers guard it (`windowSum` returns zero, `isStale` and the
removal path treat it as removable), and the sentinel rule has one owner. The value is
`Long.MIN_VALUE`, not `-1`: a negative monotonic timestamp is a real coordinate. A fresh window
needs no marker: its sum walk reads zeroes except for the slot just written. Owner:
`SlidingWindowDetector`.

**Packing the broadcaster's decision type into the accepted version's sign bit.** With compressed
class pointers the accepted record is 12-byte header + 4-byte type + two `long`s = 32 bytes; two
`long`s alone are 12 + 4 padding + 16 = 32 as well, so the packing saves no bytes and buys a mask
on every read. The accepted record is an immutable snapshot, so an elision stores a replacement
that advances the version and keeps the send timestamp; the write refreshes the state-dependent
expiry — twice the queue age while cold, until a COOL is accepted while hot — so the newest version
outlives every queued decision that could read it. Owner:
`WorkerBroadcaster`.

**A hand-written TTAS probe around `Semaphore.tryAcquire`.** `Semaphore`'s non-fair acquire already
reads `available`, subtracts `acquires` and only then attempts the CAS; wrapping it in
`availablePermits() > 0` duplicates the same volatile read and narrows the race, it does not remove
the atomic. Owner: `HotKeyRegistry`.

**Blanket-skipping an after-the-body invalidation when the body returned `null`, and a shape-based
exception for a `#result` dereference.** The skip avoided a `#result.id` null dereference but also
skipped every expression with its own fallback (`#result != null ? #result.id : #fallbackId`), so a
legal removal never ran. The AST-shaped tolerance that replaced it — `#result.id` was a no-op on a
null result while `'prefix:' + #result.id` threw — made the contract depend on syntax and matched
SpEL message codes inside the interceptor. The key is now evaluated by SpEL with no special case:
`#result?.id` yields a null key (a no-op removal), a plain `#result.id` on a null result fails, and a
fallback expression still runs. Owner: `ZetaCacheInterceptor`, `CompiledExpression`.

**Falling back to SpEL for a digits-only TTL literal that overflows `long`.** The fallback assumed
SpEL could hold the number, but an unsuffixed decimal is parsed through `Integer.parseInt`, so the
result was an obscure startup parse failure instead of a clear range error. The literal is rejected
at resolution now, and a dynamic `BigInteger`/`BigDecimal` result is clamped to `Long.MAX_VALUE`
rather than keeping its low 64 bits. Owner: `ZetaCacheInterceptor`.

**Replacing the registry stripes with `synchronized (entry)`.** The stripes are not only mutual
exclusion for an existing entry; they are the lock that makes `promote`'s check-then-insert and
`cool`'s check-then-remove safe before any entry exists. Removing them would make promotion use
`putIfAbsent` and force the decision version to be allocated _after_ publication (otherwise a
promotion that stalls before the insert can mint a lower version than a cool of the previous
lifecycle and lose at every App), which turns the entry's `version` into a
post-publication volatile write. The entry's own tombstone already removes the `hotKeys.get`
identity re-check from the un-cooled evaluation path; the stripe stays for the pre-insertion
transition. Owner: `HotKeyRegistry`.

**Packing a decision version and type into one long.** The two fields sit next to a `long`
timestamp in a 32-byte object under compressed oops (12-byte header + 8-byte version + 4-byte type
reference + 8-byte timestamp = 32), so packing them into one long does not cross a size class; the
decode is paid on every read of the record, and the type is a shared enum reference that the JIT
already keeps in a register. The record's population is bounded by the cold queue age plus the keys
the cluster may still hold hot, and the packing saves no bytes per record, so the claimed 800 KB is
not a measured saving. Owner: `WorkerBroadcaster.AcceptedDecision`.

**Folding a HOT run the way a COOL run is folded.** COOL is pure in-memory, so only its last
decision can affect the state. A HOT creates an entry through the cluster loader, and a later HOT
whose load fails uses the earlier HOT's created entry as its only fallback; reducing the run would
turn a source outage that still had one successful load into a `VALUE_NOT_FOUND` skip. Distinct
versions therefore stay sequential; duplicate rebroadcasts are already deduplicated by the DCL
pre-check after the first apply, before any loader call. Owner: `WorkerListener.dispatchBatch`,
`DefaultWorkerContext`.

**Folding `closed` and the pending budget into one `AtomicLong`.** The budget deliberately uses a
striped `LongAdder`: every submission adds to it, and `sum()` with the two or three AMQP consumer
threads that submit is a base-field read, not a cell walk. A single word would put every submitter
on one cache line and make settlement vulnerable to a borrow that clears the closed bit when the
budget is discharged after `close()`. The separate volatile `closed` read is cheaper than that
risk, and it is also the guard that stops a granted batch's `finally` from subtracting after
`close()` has reset the counter. Owner: `KeyDispatcher`.

**Encoding the periodic chain's terminal states in its `current` reference.** A terminal enum
would remove the `int state`, but `TERMINATED` (executor shutdown) is not a cancellation:
`isCancelled()` must stay false while `get()` returns the last link's result. Collapsing both into
"terminal singleton" either violates the `Future` contract in the other direction or needs a
second distinction anyway. The 2-bit code plus the compensating cancel in `scheduleLink` already
publishes cancellation and the interrupt request in one CAS. Owner: `SafePeriodicTask`.

**Passing `InvalidatePolicy` into `HotKeyCache` to merge the local/remote method pairs.** This was
ruled out while the facade's `Scope.REMOTE` branch was two lines per verb and the engine's
local/remote halves had separate entry points. The duplicated branch was the failure: four facade
methods re-derived the same boolean two ways (`opts.sync()` in two, `opts.scope().remote()` in the
other two) and the separate `*Remote` methods let the two spellings drift; a Worker-only empty
batch also had an inconsistent mode-check order. The policy now travels as one validated value,
the engine owns the remote-only contract (allocate a stamp, broadcast, no local tombstone and no
sweep), and the facade only normalizes and forwards. Owner: `DefaultZeta`, `HotKeyCache`.

**A packed-state log throttle (`util.LogThrottle`).** One `AtomicLong` carried the next allowed
monotonic millisecond and a saturating tally, so every hot path emitted one WARN per 10 s window
and pushed the remaining occurrences to DEBUG with a "suppressed N" tail. It silenced exactly the
lines an incident needs, made the emitted level depend on the occurrence rate rather than on the
event, turned a log statement into a compare-and-set on a shared cache line, and owned a
`TimeSource.monotonicMillisSinceBoot` dependency that existed for no other caller. The counters
and meters already carry the true volume, so the per-occurrence lines are back: genuine failures
log at WARN/ERROR with the throwable, designed degradations and load rejections (executor
saturation, queue full, ring probe exhaustion, epoch-fenced and foreign-app drops) log at DEBUG.
The packed layout, the suppression tally, the `needsArguments` seam, the custom 60 s heartbeat
window and the monotonic-since-boot clock are gone with the class. Owner: every former call site.

**`HE.promotionSubmitted` and the displaced-HOT bit.** The flag proved that a promotion had not
entered a queue, so a COOL that displaced it could be elided as a ghost; a late HOT also marked a
pending COOL as its displacement. It was removed: the proof is narrower than the App's state — a
locally promoted App is hot without any Worker HOT — so the elision suppressed the only demotion
signal a locally promoted or previously hot key could receive. The window is now covered by sending
the COOL, one idempotent message; the queued-HOT ghost is still suppressed by the cold record the
drain installs when it drops a HOT before the wire. The cost is a wasted demote-then-promote pair
in the microsecond gap between a promotion and its submission. Owner: `DecisionDispatcher`,
`WorkerBroadcaster`.
