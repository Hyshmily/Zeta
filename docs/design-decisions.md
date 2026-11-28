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

**Every decision settled by measurement records its data.** A benchmark, a sweep, a stress run or a
trace that picked a value leaves its table here, beside the paragraph that states the decision: the
observable and its unit, the setup that makes the number mean something (hardware, JVM and GC,
threads, heap, harness, workload shape, duration), every point of the sweep rather than the winner,
and the knee the data selected. An adjective without the table underneath — "consistently fastest",
"reduced excess by 51%" — cannot be re-run, cannot be told apart from a noise floor, and silently
outlives the code it described. Where `README.md` publishes the same measurement, cite that block
instead of keeping a second copy; two tables drift. Data that contradicts a shipped value is stated
as a finding, not dropped.

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

**The AMQP prefetch window is sized for O(1) acknowledgement, not for processing time.** Both
listeners acknowledge a delivery as soon as it is queued on the ordered dispatcher — no loader, no
user callback runs on the consumer thread — so the prefetch count only bounds in-flight work. At 5,
a consumer could hold five unacknowledged deliveries and then wait a full broker round trip for the
next batch: on a 2–5 ms RTT link that capped a node at a few hundred messages per second while its
dispatcher sat idle, and a burst larger than the window queued at the broker instead of in memory.
The base default is 64; the Worker listener raises it to 100 (decisions are a few bytes) and the
sync listener lowers it to 32 because one delivery can carry a `put-max-bytes` payload, so 32
in-flight PUTs bound the window at roughly 3 MB instead of 10 MB. Owner: `AmqpConsumerProperties`,
`WorkerListenerProperties`, `CacheSyncProperties`.

**Bean Validation constraints match the field's type and the cross-field relation.** Floating-point
knobs use `@DecimalMin` / `@DecimalMax`; `@Min` / `@Max` do not support `double`, and with
`spring-boot-starter-validation` on the classpath Hibernate Validator throws
`UnexpectedTypeException` while binding `ZetaProperties`, so the application fails to start even
though the default value is valid. Positive integer limits use `@Min(1)`. The circuit breaker's
`windowTimeMs >= windowBuckets` is an `@AssertTrue` because a per-field range cannot express the
relation; without it the invalid pair survives binding and crashes at the first
`SlidingWindowBreaker` construction, long after the operator could connect it to the configuration.
Owner: `ZetaProperties`.

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
keeps a **single immutable watermark snapshot**: the table-wide clear stamp, the copy-on-write
`prefix -> clearStamp` map **and the highest live prefix stamp**, published by one volatile
reference. The table stamp and the prefix map must publish together: read as two independent
volatiles, a write could observe the new table stamp with the post-clear empty map (or the old
table stamp with an already-pruned map) and admit a stamp both watermarks refuse — a deleted key
re-entering L1. The max is what keeps the write path cheap once a namespace clear has aged past
the in-flight window: a write above it cannot match any live prefix, so one volatile read and one
long compare replace the walk. Only a version low enough to match a live prefix pays the short
`startsWith` walk. The map stays honest about what is live: a stamp the table-wide watermark covers
is dropped on the way in and pruned when a clear advances past it (the guard tests the table-wide
watermark first, so such an entry can never match again), at the 64-entry cap the older entries
first shed the ones older than the tombstone TTL — except a stamp the recording call itself adds or
raises, which is that call's own in-flight window and whose wall clock may lag a peer's — and only
if the cap is still exceeded does the whole live set fold into the table-wide watermark at the
_highest_ stamp involved and the map reset: the raised global watermark keeps every evicted
prefix's protection, and refusing more, never less, stays the safe direction. The max is recomputed
from the surviving map on every prune and age-eviction (it gates only whether the walk runs, never
whether a write is refused), so a removed prefix does not force later writes through a walk that
cannot match; an empty map short-circuits the walk. The guard consults both levels, so the
effective watermark for a key is the higher of the table-wide stamp and the stamp of any prefix it
matches: a write has to outrank both to land. Neither the snapshot nor a per-prefix lookup is part
of the `EntryLifecycle` surface — `recordPrefixClear`/`recordPrefixClears` and
`staleAgainstInvalidation` are the whole contract, which leaves the encoding of the watermarks free
to change.

**A clear is best-effort termination, not a barrier.** The sweep removes what the table's weakly
consistent key-set iteration yields. A fill whose write guard already passed and whose install lands
after the iterator has traversed its bin is never seen, so it survives until its own hard TTL — the
same convergence bound a peer that missed the broadcast has. The anti-resurrection guarantee is the
watermark, not the sweep: the watermark refuses every write that starts after it, and the sweep only
reclaims what the iteration can see. Closing the window would need a lock on every write, paid on the
hot path for a stale entry that already dies at its TTL. Owner: `HotKeyCache.sweep`.

**The sweep commits its count per key, and a failed sweep does not kill the drain.**
`DefaultSyncContext.sweep` reuses one outcome holder across the entire entry loop — one allocation
per sweep, not per key — and resets it before each key's mutation, because a contended
`computeIfPresent` can re-run that function against a different image and must not carry the
previous key's result into this key's count. It distinguishes a live removal (counted) from an
already-expired one (swept, its warm task cancelled, not counted), and one `liveResident` call per
attempt supplies both the guard and that distinction, so an entry the clock crosses mid-attempt
cannot be judged live and expired in the same pass. The deferred drain catches a `RuntimeException`
from the sweep, reports it through `ZetaExceptionHandler`, and continues with the next coalesced
marks: the watermarks are already recorded, so the anti-resurrection guard holds and the remaining
entries converge through their hard TTL; without the guard an unexpected sweep failure escaped on
the sweep executor and dropped the rest of that drain cycle. Owner: `DefaultSyncContext.sweep`,
`drainSweep`.

**The prefix is validated at the facade boundary, not inside the watermark table.** `Zeta.clearPrefix`
rejects a null, empty or blank prefix before the engine is called; the engine's own `clearPrefix`
therefore assumes a physical prefix (the facade is the only production caller). A null one would
otherwise degrade into the shared table-wide clear; an empty one matches every key while the
watermark table records nothing and the publisher drops the broadcast, so it is a full sweep with
no anti-resurrection guarantee; a blank one is recorded locally but dropped by every peer's wire
validation, leaving the two planes disagreeing about whether a clear happened.
Owner: `DefaultZeta.clearPrefix`, `HotKeyCache.clearPrefix`.

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
mutation goes through `newEntry()`/`editEntry()`/`of()`. The entry carries exactly what the data
plane owns: the value, its `dataVersion`, the caller's resolved hard/soft durations and the two
absolute deadlines. `EntryLifecycle.newEntry()` and `editEntry(entry)` supply the TTL arithmetic,
so no caller can build an entry whose timestamps disagree with its durations. Collaborators that
need the arithmetic take it from the lifecycle once at construction (`TtlPolicy` is stateless),
instead of reaching through `entryLifecycle.ttlPolicy()` on every call.

**No heat state on the entry.** A heat lease is a `HeatLease` in `HeatDirectory`, keyed by the
physical key; it never enters the L1 value. Storing a decision next to a value would make every
rewrite site carry heat metadata forward, and one missed copy would silently change what HOT or
COOL means. The separation costs one extra map lookup on the paths that need cluster heat
(prewarm, keep-warm, the decision handler); a plain read touches none of it. The 2.0 model removed
the per-entry `DecisionStamp`, the three-state code and the packed state bits; the entry is now six
fields, and no code path has to reconcile a state machine with a value.

**Packing is load-bearing.** Each TTL duration is one `int`: bit 31 selects seconds, bits [0, 31)
carry the magnitude. Values up to 2^31−1 ms (≈ 24.8 days) round-trip exactly; longer ones are
stored in whole seconds rounded **up**, so the decoded duration never falls below the request and
`encode(decode(x)) == encode(x)` — a no-op `EntryDraft.of(entry).build()` must not change the packed
fields that `equals`/`hashCode` compare. `Long.MAX_VALUE` (permanent) is a dedicated sentinel that
no real seconds encoding can collide with, and it is the _only_ permanent representation: every
hard duration reaching a draft is resolved, so a `0` there is a caller bug the setter rejects
rather than a second spelling of "forever". A blank draft starts at `Long.MAX_VALUE` for both hard
fields, which keeps "a draft that never declared a TTL" total without inventing an unset state. The
absolute timestamps stay full `long`s and are the enforcement point — durations are nominal values
used when a fresh value re-arms the entry.

**One atomic mutation primitive.** `EntryMutations.apply` owns the "judge the resident, rebuild or
remove it, report what the bin lock saw" shape for every in-place rewrite: the sync PUT/REFRESH
apply, the refresh settlement, the versioned invalidate, the write-through apply and the load fill.
The mutation returns the input instance for "no change", a new instance for a rewrite, or `null` for
a removal; the call always returns the pre/post images it observed inside the compute — the pair is
identical for a no-op — so no caller carries a mutable `boolean[]`/`CacheEntry[]` out of the compute
and no caller has to infer "unchanged" from a null return. The mutation runs on an absent key too,
which is what lets an invalidate record its tombstone in the same compute as the removal.

**The post-image is the only "did it apply?" answer.** Identity, not the data version, separates a
write that landed from one the guard refused: a refusal returns the resident instance it was handed,
and that instance may already carry the incoming stamp — two nodes minting the same stamp in one
millisecond after a node-id collision, or a tombstone recorded at exactly that stamp. Comparing
versions then read a refusal as an application, so the refused write dropped the key's in-flight
flight (the next miss re-ran the source instead of joining it), re-armed renewal for a value nobody
stored, and broadcast the refused value to peers. Owner: `HotKeyCache.writeThroughApply`,
`HotKeyCache.loadCacheEntry`.

**`null` is never a cached value and never a cache hit.** No null sentinel, no negative caching,
`put(key, null)` throws, and a loader returning `null` writes no entry. Caching absence would
conflate "absent" with "not yet loaded", force every reader to distinguish two empties, and
interact badly with admission and versioning.

Owner: `CacheEntry`, `EntryDraft`, `EntryMutations`, `EntryLifecycle`.

---

## TTL model

**Two tiers, one lifetime.** Every resident entry has exactly one hard and one soft declaration:

| Tier | Purpose                                                               |
| ---- | --------------------------------------------------------------------- |
| Hard | Caffeine eviction and the absolute bound: never served after it       |
| Soft | Stale-while-revalidate: after it a read may refresh behind the caller |

There is no second, heat-dependent row. `hard-ttl-ms` and `soft-ttl-ms` are the only TTL knobs a
deployment has, and the caller's annotation/option declaration is the only per-key override. A HOT
or COOL decision never supplies a duration.

**One vocabulary, three values, every layer.** `0` inherits the configured default; `-1` disables a
soft window (a hard TTL has no "off" state, so there it inherits like `0`); every positive
declaration is trusted verbatim, `Long.MAX_VALUE` included, and means "never expires". Any other
negative value is a configuration error and is rejected where it was written: an annotation literal
at plan time, an option record at construction, a property at binding. A context-free expression is
folded by the same rule, not by its numeric sign: `softTtl = "-1L"` is the sentinel, while a fold
that clamped every negative to `0` turned it into "inherit" and silently re-armed the background
refresh the caller had disabled — the SpEL long suffix (`-1L`) bypassed the integer-literal path and
reached the clamp as a negative constant. Owner: `CachePlan.Ttl` (classification and folding),
`TtlPolicy` (resolution).

That single sentence replaced a shape in which the same number meant opposite things per layer.
`hard-ttl-ms: 0` was permanent in configuration while `hardTtl = "0"` meant "inherit" on an
annotation, and "disable the soft refresh" was an undocumented `-1` that the property layer
rejected outright. Two real defects came out of it. A method declaring `softTtl = "-1"` on a
deployment with a global `soft-ttl-ms` stored `0` as its soft baseline, and the (now removed)
promotion path read a stored `0` as "unset" and substituted the global value
(`stored > 0 ? stored : effectiveSoftTtlMs()`): the exact background refresh the caller had
switched off came back the moment the key became hot. And because the global soft default is itself
`0`, "disabled" and "never configured" were indistinguishable at every downstream ternary. The rule
makes each of those branches unnecessary rather than correct: nothing downstream resolves a
default, so nothing can resurrect a disabled window.

**Configuration resolves once, in its getters.** `ZetaProperties` is the outermost declaration
layer, so `getHardTtlMs()` / `getSoftTtlMs()` map a non-positive configured value to the built-in
default (`300_000`, `0`) and return a duration every consumer can act on. That is the one place
configuration applies "0 means default"; every other layer resolves a _declaration_
(`TtlPolicy.resolveHardTtl` / `resolveSoftTtl`) and then passes the result down. Two owners of that
reading — a field default and an `effective*()` branch — could not tell an explicit `0` from an
unset field, which is exactly how `soft-ttl-ms=0` used to fall back to a 30-second default.
Owner: `ZetaProperties`, `TtlPolicy`.

**The entry stores resolved durations only.** `CacheEntry` never holds an unresolved TTL: hard is
positive (`Long.MAX_VALUE` = permanent), soft is `0` (disabled) or positive (`Long.MAX_VALUE` =
never stale). `EntryDraft` rejects a non-positive hard duration and `CacheEntry` re-checks it in the
constructor, so "0 = permanent" no longer exists inside L1 and "0 = unset" does not exist at all.
Every downstream consumer — `resolveRefreshSoftTtl`, `resolveSyncableHardTtl`, the refresh
settlement, the keep-warm renewal — uses the stored value directly. This is what makes a disabled
soft window durable: inside the entry `0` is a decision, not a gap, and a consumer that substituted
a default would be changing the caller's contract. Owner: `CacheEntry`, `EntryDraft`, `TtlPolicy`.

**`Long.MAX_VALUE` means permanent (hard) or never stale (soft).** Pure logical expiry is expressed
as a permanent hard TTL, which is a supported Caffeine `Expiry` value; a permanent soft window
keeps an entry servable forever while the hard deadline still bounds its life. Both are ordinary
declarations now — an explicit value, not a side effect of writing `0`. Owner: `TtlPolicy`,
`EntryDraft.ExpiryArithmetic`.

**The saturation decision belongs to the clock sample, not the sum.** `EntryDraft.SYSTEM` clamps to
`Long.MAX_VALUE` only when the sample is already within the TTL of it
(`now > Long.MAX_VALUE - ttlMs`). The monotonic axis is not guaranteed non-negative — a test clock
or a negative-offset base hands out negative instants — and deciding on `now + ttlMs < 0` read an
ordinary future deadline as a wrap: at `now = -10_000` a 1 s TTL was clamped to `Long.MAX_VALUE`, so
the entry became permanent instead of expiring one second later, and no later declaration could undo
that for the life of the process. `TtlPolicy.expireAtFromNow` always decided this way; the draft's
plain arithmetic now shares the rule instead of re-deriving it. Owner: `EntryDraft.SYSTEM`,
`TtlPolicy.expireAtFromNow`.

**A TTL must be representable before it reaches L1.** The packed entry stores a hard/soft duration
as a unit bit plus a 31-bit magnitude, so the largest finite duration is `2^31 - 1` seconds
(about 68 years). A value above that but below `Long.MAX_VALUE` is rejected at the option, annotation
and draft boundaries through `CacheEntry.requireDeclaredTtl` / `requireEncodableTtl`; otherwise the
failure surfaced inside an L1 `compute` or a transaction's after-commit hook, after the business side
effect had already run. The annotation planner folds a constant TTL at bean creation and rejects the
out-of-range value there; a dynamic expression that evaluates out of range throws with the attribute
role and method instead of reaching L1 or degrading to the configured default; the facade's
`ResolvedTtls` validates both axes before
it opens a transaction. Owner: `GetPolicy.ttlSupplier`, `CachePlan.Ttl.of`,
`DefaultZeta.ResolvedTtls`, `EntryDraft`, `CacheEntry`.

**An expiry deadline saturates; it never wraps.** `TtlPolicy.expireAtFromNow` computes
`now + max(1, ttl + jitter)` and clamps to `Long.MAX_VALUE` when the jittered duration or the final
sum would overflow. `hardExpireAt` / `softExpireAt` are public arithmetic: the packed entry rejects
an unencodable finite duration at the entry boundary, but a direct caller (or `EntryDraft.SYSTEM`)
can still pass a `long` near the maximum. A plain addition of such a TTL and its positive jitter
wrapped negative, and every expiry predicate (`isLogicallyExpired`, `isSoftExpired`) reads a
negative deadline as already expired: the entry was born expired, so the very next read missed and
re-invoked the reader even though the write had succeeded. Saturation keeps the deadline in the
future, the only safe direction for a nominal TTL. Owner: `TtlPolicy.expireAtFromNow`.

**A declaration is resolved before the entry draft, never inside it.** `HotKeyCache.loadAndCache`
resolves both overrides through `TtlPolicy` before `loadCacheEntry` builds the entry, so
`GetPolicy.of(0, …)` and `GetPolicy.of(-1, -1, …)` store the configured durations. The draft now
rejects an unresolved `0` hard duration instead of reinterpreting it: the previous
"`0` = permanent" mapping meant a load that never set a hard TTL would never expire, and a caller
that passed a raw declaration by mistake got the opposite of the default. Owner:
`HotKeyCache.loadAndCache` (single and batch), `loadCacheEntry`, `EntryDraft`.

**The caller's declaration is the only lifetime.** A load, a write and a refresh all mint the
entry's deadlines from the declaration they were given; a HOT or COOL decision changes only a lease
in `HeatDirectory`. Detection answers "which key is hot"; the caller owns "how stale may this data
be", and the framework cannot price that trade for every namespace. Before 2.0 a per-namespace hot
TTL policy could extend a HOT entry's windows (the default `PRESERVE` did not, but the mechanism
and its declaration existed), which made the hottest keys the ones most likely to outlive their
declared bound. The 2.0 model deletes the mechanism rather than defaulting it off: a policy that
can extend a deadline is a second owner of the caller's freshness contract, and every rewrite site
that had to carry the old "normal baseline" forward was a place where that contract could be lost.
Owner: `TtlPolicy`, `HotKeyCache`, `DefaultBackgroundRefresher`.

**A namespace TTL contract exists for loads that have no invocation.** `TtlContract` holds the
namespace's resolved hard/soft baseline and a `dynamicTtl` flag; `NamespacePolicyRegistry` records
it at plan time. A cold prewarm has no invocation whose TTL expression it could evaluate, so a
namespace with a dynamic TTL (a SpEL TTL, or two methods whose declarations disagree) is marked
non-prewarmable instead of being loaded under the global default. Keep-warm does not need the
contract: it re-arms the resident's own stored durations. Owner: `TtlContract`,
`NamespacePolicyRegistry`, `PlanResolver`.

**A value write re-declares the lifetime.** The caller's resolved TTLs become the entry's hard and
soft durations, and both windows re-arm from `now`. A new value has no older declaration to
preserve, and carrying the previous deadline forward would let a value outlive the lifetime its
owner just replaced — while ignoring a _shorter_ TTL would stop a caller from tightening a bound.
Owner: `EntryLifecycleImpl.writeValue`, `HotKeyCache.putThrough`, `HotKeyCache.executeRefreshNow`.

**Jitter only subtracts.** Every minted deadline is `now + ttl - offset`, with
`offset ∈ [0, ttl × ratio]`. The staggering that jitter exists for (no synchronized expiry across
instances) survives unchanged, while a declared `hardTtl = 5min` can no longer be observed as
5min15s. A symmetric offset made the declared bound a suggestion: the caller's ceiling was exceeded
by up to 5%. `zeta.local.ttl-jitter-ratio=0` disables it. Owner: `TtlPolicy.jitterOffset`.

**A disabled soft TTL never triggers a stale branch.** `softExpireAtMs == 0` means disabled.
The earlier `<= 0` reading armed a background refresh on _every_ hit — exactly the load storm
the "disabled" setting exists to prevent. `soft-ttl-ms` defaults to `0`: a background refresh for
an annotated read is a reflective call without the invocation's transaction, security or MDC
context, so it is a deployment decision, not a default. `softTtl = "-1"` is the declaration that
disables it, `0` inherits the configured default, and both reach the entry as `0` — which every
consumer now reads as "off" rather than "unset".

Owner: `TtlPolicy` (pure arithmetic), `ZetaProperties` (live configuration), `CachePlan.Ttl`.

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

**Refresh housekeeping is not a caller read.** The refresh snapshot goes through
`QuietCache.getIfPresent` (`policy().getIfPresentQuietly`, falling back to `asMap().get` for a
replacement `Cache` without a policy view): Caffeine's quiet probe skips the hit/miss counters,
honours physical expiry and keeps the node out of the read buffer, so the internal refresh path
cannot count a second hit for a soft-expired read, inflate the L1 hit rate, or make a maintenance
probe look like traffic to W-TinyLFU.

**One ticket is the whole refresh lifecycle.** `RefreshTicket` is simultaneously the `refreshes`
reservation token, the creation-time snapshot (`dataVersion`, hard expiry, the entry's hard/soft
durations and the caller's refresh soft TTL) and the `whenComplete` callback. Its identity is the
ABA guard for the completion-time removal, and the snapshot is the identity guard every settlement
path compares against. The same ticket serves a soft refresh and a hard renewal through one atomic
mode: `SOFT → (SETTLING_SOFT | HARD)`. A soft settlement claims `SOFT → SETTLING_SOFT` before it
writes back; `renewHard` claims `SOFT → HARD` before the load settles. Exactly one wins, so a
renewal can never be told "applied" by a soft write-back that does not re-arm the hard deadline.
The loser shares the ticket's future, which completes only after the soft write-back landed, and
reports `Skipped.SOFT_REFRESH`. That ordering is the contract: `KeepWarm` reschedules from the
resident inside that future's completion callback, and an already-completed refusal ran the
callback while `settleLoaded(loaded, false)` had not written back yet — `liveResident` still saw
the softly expired entry, `computeDelayMs` returned `-1`, and the key lost its renewal task for the
rest of the hard TTL, which is the stampede the scheduler exists to prevent.
The mode and the fixed from-soft flag share one int (`FLAG_HARD`, `FLAG_SETTLING`,
`FLAG_FROM_SOFT`), and the completion word holds `null | future | outcome`. A pure soft ticket never
allocates a future: `renewHard` installs one with a CAS and the settlement publishes the outcome
with `getAndSet`, completing that future when it is present; a later upgrade reads the stored
outcome instead. Previously the ticket used a `volatile boolean hardRequested`: a soft settlement
could read `false`, a renewal could set it `true` and receive the shared future, and the soft
write-back then completed that future as applied — while the hard deadline was never extended. The
key expired at its original deadline even though the renewal reported success. Owner:
`DefaultBackgroundRefresher.RefreshTicket`, `KeepWarm.fire`.

**A settlement replaces a resident; it never creates one.** A ticket is reserved only while the
resident is live, so its snapshot (`dataVersion`, hard deadline) always describes an entry that
existed when the refresh was requested. A key evicted or invalidated before the write-back has no
resident left, and a settlement that rebuilt one would resurrect a value Caffeine had already
dropped for capacity — with a fresh full hard window the caller never granted. A ticket for an
already absent or hard-expired resident is therefore refused before the permit is taken: its
settlement could only answer `ABSENT_OR_EXPIRED` or `NEWER_VALUE`, so a permit and a source query
would be spent on a write that is mathematically impossible. The same snapshot orders the
completion checks: a tombstone outranks the value, a newer resident outranks an older load, and a
soft settlement gives up on a resident whose hard deadline has passed, because a soft refresh does
not re-arm that deadline and the write-back would store an entry the next maintenance pass evicts
and the next read treats as absent. A hard renewal applies to any remaining resident — including
one that crossed its hard deadline while the load ran — because the ticket only exists if the
entry was live when the renewal was requested, and re-arming both windows is the natural
expiry-and-reload the caller paid for one source query earlier. Before this shape one check
(`entry == null || logically expired`) treated soft and hard alike: a hard renewal that straddled
the deadline discarded the value it had just loaded, exactly when a slow source most needs the
renewal, so every following read fell through to the source, while a soft refresh _rebuilt_ an
evicted entry and reset the full hard TTL, extending a bound the caller had declared absolute.
Owner: `DefaultBackgroundRefresher.submitTicket`,
`DefaultBackgroundRefresher.RefreshTicket.settleLoaded`.

**A failed or empty refresh closes the retry window and never extends a deadline.** The entry
keeps serving until the hard TTL its owner declared; what the settlement removes is the reason to
retry, by moving the soft deadline up to that hard deadline. The identity guard covers the same
logical entry (`dataVersion` and `hardExpireAtMs` unchanged), and a peer `INVALIDATE` at or above
the ticket's snapshot stamp wins: a condemned entry keeps the window the invalidation closed. Without the close, every read re-arms the failing
source for the rest of the hard window — a retry loop that looks like a load storm, not like
degradation. With it, the next explicit write or the hard expiry is the recovery point, and
"bounded staleness" is a property of the declared TTL rather than of a lease the framework invented.
Owner: `DefaultBackgroundRefresher.RefreshTicket.closeRetryWindow`.

**A refresh re-declares the lifetime from the resident's own durations.** `refresh` / `refreshAll`
use the resident's stored hard/soft durations and fall back to the configured defaults only for an
absent or hard-expired key; the incoming value is new, so its deadlines are minted from that
declaration. A background (soft) refresh re-arms only the soft window, leaving the caller's hard
bound absolute. The version is allocated before the resident probe and the load, so a concurrent
write is either visible to the probe or holds a newer stamp and refuses the refresh. A
`CacheLoader` refresh uses `reload` and `isCacheable`, so an after-commit or scheduler refresh
cannot resume caller-bound state and a declined value never enters L1. Owner:
`HotKeyCache.executeRefreshNow`, `EntryLifecycleImpl.writeValue`.

---

## Read path

**One clock sample, one L1 lookup per read.** `get` → `readThrough` (a hit) or `loadAndCache`
(a miss):

1. `getIfPresent`; if present, judge hard expiry against a single `now`. A hard-expired entry
   is removed under the identity guard and the read becomes a miss.
2. On a hit: serve, or — when the soft window has passed — either block on a reload
   (`REVALIDATE`) or arm one background refresh (`SOFT_REFRESH`) and serve the stale value. There
   is no state machine to reconcile on this path. The access is recorded only when the read
   actually serves (see _One access record per served read_ below).
3. On a miss: `SingleFlight.load` runs the reader once per key and returns a `LoaderOutcome`.
4. The load leader stamps, admits and stores inside its `leaderCommit`, before the shared future
   completes; every caller then records its own access and returns the value.

**One loader abstraction, adapted only when a read needs it.** `HotKeyCache` still exposes the
supplier and loader entry points, but internally `ReadContext` holds one `CacheLoader`; a plain
supplier is adapted to it inside the lazy context factory, so a plain hit allocates neither the
context nor the adapter. The key travels as a parameter to `loadValue` instead of
being stored in every context; the batch path adapts its `Function` once per slow hit and reuses it
for the refresh. A scheduled refresh captures the context's `loader()` reference, never the
context, so a queued task cannot pin the call's TTL suppliers. This removes the
`reader`/`loader`/`loaderKey` branch split; the batch adapter
remains one allocation per slow hit, and the common hit path adds no work. The failure this
prevents: the reader and loader shapes had to be kept in lockstep for `reload`/`isCacheable`, and a
divergence would serve or store from the wrong source. Owner: `ReadContext`,
`HotKeyCache.readContext`.

**Hard-expiry eviction returns only a live entry.** `EntryLifecycleImpl.evictIfExpired` judges
the caller's snapshot and, when a concurrent write replaced it inside the same compute, judges the
replacement against the same `now`: the method returns `null` or an entry that is not logically
expired. Without that second judgment a peek or a degraded stale serve could hand back the
replacement after its own hard deadline — the hard TTL contract belongs to the resident entry, not
to the snapshot the caller happened to read, and a stale reference must not exempt its successor.
Callers therefore never re-check the returned reference for expiry. Owner:
`EntryLifecycleImpl`, `HotKeyCache`.

**Internal probes are stats- and policy-quiet.** `refreshSoftExpire`'s residency confirmation and
`resolveNullAnswer`'s stale probe are maintenance, not caller reads. They use `QuietCache`
(`policy().getIfPresentQuietly`, falling back to `asMap().get` for a custom `Cache` whose
`policy()` is unsupported), so they neither increment `cache.gets` nor feed Caffeine's read buffer
and W-TinyLFU. `Cache.getIfPresent` does both; `asMap().get` is only stats-quiet — it still records
the node in the read buffer. A diagnostics scrape that used `getIfPresent` inflated the hit count
and made maintenance reads look like traffic to the eviction policy. Owner: `QuietCache`,
`HotKeyCache`.

**The single-key and batch paths share the stale-policy decision.** Both probe once, judge hard
expiry against one `now`, and apply the same `StalePolicy`: `RETURN` serves, `REVALIDATE` reloads
on the calling thread, and `SOFT_REFRESH` serves while arming one background refresh. A
soft-expired batch hit reuses the single-key refresh helper, so the two paths cannot disagree on
when a hit is a hit. The hard-expiry eviction is identity-guarded and never touches the dedup
flight: expiry is not a mutation.

**The hit path arms a refresh without a second L1 read in the common case.** The soft-expiry test
is a comparison on the entry the caller already holds, and it is false for the overwhelming
majority of hits, so arming gates on it first; only a genuinely soft-expired hit confirms
residency. A plain hit therefore pays exactly one table lookup.

**The batch slow branch confirms residency before arming.** A batch enters the slow branch for any
soft-expired hit, so the refresh must re-check that the resident still exists and is still stale:
the arm path re-reads it through `QuietCache` and re-tests the deadline on that post-eviction
snapshot, because a concurrent invalidate or a peer soft-close can change the answer between the
probe and the arming. A fresh entry returns before this point, so the common batch hit still
allocates neither a context nor a closure. The context is built once per call, on the first slow hit
or the first miss, and shared for the rest of the batch with the same lazy TTL suppliers the
single-key path uses. The batch used to pre-resolve both suppliers at entry and build one
`ReadContext.ofBatch` — plus one `CacheLoader` adapter — per soft-expired key, so a batch of pure
hits evaluated SpEL-backed TTL expressions it never needed and N stale keys allocated N contexts.
One factory, `ReadContext.of`, keeps the memoization and the supplier count identical on both paths.

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

**A suppression is never a miss; a failed load under an open breaker degrades only with a value.**
`SOURCE_EMPTY` (the reader returned `null`) is the one outcome that is a genuine miss. Every
`SUPPRESSED_*` outcome means the reader never ran: it first tries the hard-valid stale fallback and
trusts the verdict that suppressed it — the breaker is not probed a second time, so a recovery that
lands between admission and resolution cannot turn a degradable suppression into an exception — and
throws `ZetaSuppressedException` when nothing is servable. `FAILED` rethrows its cause, except that
an open breaker first tries the same stale entry and serves it when one is hard-valid; with nothing
servable the original failure propagates. Falling through to `null` there made an outage
indistinguishable from `SOURCE_EMPTY`, so an upstream could read a source failure as "record
absent" (a 404) instead of an error. A batch applies the same contract per key and rethrows the
first failure after processing the rest. There is no per-call switch: a caller that wants an empty
answer reads a key whose loader returns `null`, not one whose source was suppressed or failed.

**A batch closes before it throws.** `getAll` processes every key first and rethrows the first
failure with the rest attached as suppressed (an identical throwable is never attached twice).
Throwing mid-batch would strand the followers that joined this leader's load, leaving every one
of them with a value this node never wrote.

**A null reader is a cache-only read on both engine paths.** The single-key engine read already
accepted a null reader as "serve L1 and the breaker-open stale fallback, load nothing"; the batch
path now applies the same rule instead of building `k -> reader.apply(k)` and failing with an NPE
on a soft-expired hit or a miss. The facade still rejects a null reader at its own boundary; this
is the engine's internal contract, kept symmetric so a direct `HotKeyCache` caller cannot observe a
different answer from the two forms. Owner: `HotKeyCache.batchHit`, `loadAndCache`.

**Stale-first beats fail-fast, but a suppression is never a miss.** A breaker-open **failed** load
degrades instead of throwing only when a hard-valid stale entry exists: the read serves it and
records the access like any other. With nothing servable the original failure propagates, exactly
as a **suppressed** load throws `ZetaSuppressedException` with nothing servable — a protected
source can masquerade as neither an absent one nor a successful empty read. A hard-expired entry
is never served as stale and is evicted on the spot, dedup slot included — the fallback is the one
path that inspects the entry with no reader behind it, so it is also the natural cleanup point.

**`Error` always propagates, and an interrupt degrades.** A fatal error is never swallowable; an
`InterruptedException` cause resolves as a miss instead of throwing. Only the thread that observed
the interrupt restores the flag: the caller-affine leader (`runInline`) and the isolated loader in
its own catch. A follower of an executor load, and every waiter on a failed flight, must stay
clean — re-interrupting every join turned one interrupted reader into N spuriously interrupted
business threads whose next JDBC/RPC/lock call threw.

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

**One access record per served read.** The access is recorded wherever the value is actually served:
the plain hit in `readThrough`, the batch fast path in `batchGet`, the completed load in
`resolveLoaded`, and the degraded stale serve in `serveStaleEntry`. A read whose entry expired
mid-flight is a miss that happened to be fast, not a hit that returned nothing: the batch path
counts the miss and the reload records the hit once. The stale-serve record keeps the hot-key
signal alive through an outage window — a key hot enough to be read throughout the outage must not
stop looking hot exactly when the Worker needs to know.

**Batch reads dedup their keys.** A repeated key is probed, counted and loaded once, so one
logical read cannot be counted twice or start a second load for a key already in flight.

Owner: `HotKeyCache` (`readThrough`, `batchGet`, `loadAndCache`, `resolveLoaded`,
`resolveSuppressed`, `resolveFailed`, `serveStaleEntry`), `LoadFlights`.

---

## Write path

**Bump before mutate.** `put(key, value)` is local-only by default: there is no business writer
inside Zeta, so "mutate the source, then update the cache" is the caller's orchestration.
`PutPolicy.synced()` allocates a stamp, applies locally, then broadcasts a value-carrying
`PUT`; peers apply it under the version guard without reloading.

**Transaction-aware by default.** Inside an active transaction the write or evict waits for
commit, and a rollback drops it. `InvalidatePolicy.immediate(true)` is the escape hatch that
runs the removal on the calling thread (the `@ZetaInvalidate(beforeInvocation = true)` contract).
A task that runs inside `afterCommit` must not defer again: Spring snapshots its synchronization
list before invoking the callbacks, while `TransactionSynchronizationManager.isSynchronizationActive()`
stays true until cleanup, so a callback registered from inside one is never visited and disappears
without an error. `TransactionSupport` therefore tracks the after-commit depth per thread and runs
nested writes and invalidations inline. A callback registered by a different synchronization's
`afterCommit` callback is likewise absent from that snapshot; its own `afterCompletion` runs it when
the transaction committed, instead of discarding it. The failure this closes is a committed write
whose cache update is silently dropped, leaving L1 stale until its hard TTL; the previous shape
checked only `isSynchronizationActive()` and lost both the nested task and the listener's task.

**A synchronization is not a transaction.** `isSynchronizationActive()` alone is also true for the
empty transaction status Spring creates when `SUPPORTS`, `NOT_SUPPORTED` or `NEVER` runs without an
outer transaction under the default `SYNCHRONIZATION_ALWAYS` policy, while the data source is in
auto-commit. Deferring on that flag moved a cache write, eviction or clear to an `afterCommit` a
rollback never reaches, so a method that threw after its SQL had committed left L1 serving the old
value for the whole hard TTL; on the read side the same false positive made every caller-affine miss
look uncommitted, so the load was isolated and the key never entered L1. Deferral now also requires
`isActualTransactionActive()`, and the read-side question became its own predicate,
`isReadWriteTransactionActive()`, which deliberately does **not** require a synchronization: with
`SYNCHRONIZATION_NEVER` a real transaction has none, and a reader on that thread still observes the
transaction's own uncommitted writes. Both keep the after-commit depth guard, because
`isActualTransactionActive()` stays true until `cleanupAfterCompletion`, after the callbacks have
run. Owner: `TransactionSupport`, `HotKeyCache.loadAndCache`.

A cache write issued from a _different_ synchronization's `afterCompletion(STATUS_COMMITTED)` is
still too late: Spring snapshots the callback list before invoking it, and the synchronization
registered there is never visited. Such a hook must run its operation immediately
(`InvalidatePolicy.immediate(true)`) or after the transaction has fully completed. Owner:
`TransactionSupport`.

**Batch writes share one policy, one transaction boundary and one version block per chunk.**
`putAll` resolves one `PutPolicy`, checks the effective hard TTL once, then copies the entries into
an insertion-ordered snapshot while it validates them, and delegates that snapshot to
`HotKeyCache.putAll`. Copying closes the race where a concurrent mutation of the caller's map injects
an unchecked key or value between the validation pass and the engine's traversal. Option validation
precedes the empty-map short-circuit: an invalid TTL or an unsyncable permanent write is a caller
error even when there is nothing to write. When the store is deferred, the engine freezes the map and
reserves one contiguous stamp block per chunk before applying each row through the shared write tail.
A concurrent single write is ordered against the whole block — older than every chunk stamp or newer
than every one — instead of landing between two keys of the same intended batch. The batch is
deliberately **not atomic**: a failure after some applies leaves those keys written, and a synced
batch still sends one PUT per key because the wire has no batch PUT message. The previous shape was
a caller loop of `put`: N independent stamp allocations and N afterCommit registrations, with a
concurrent write free to interleave between keys. Owner: `DefaultZeta.putAll`, `HotKeyCache.putAll`.

**A synced write validates the effective hard TTL, not the raw override.** `PutPolicy.of(0, …, true)`
means "use the configured default", and a configured `zeta.local.hard-ttl-ms` of `Long.MAX_VALUE`
means permanent. Checking only
the raw value therefore let the default `@ZetaPut(sync=true)` broadcast a permanent entry: peers
drop an entry only through its hard TTL, so every peer pinned the stale value forever. `DefaultZeta`
resolves the effective TTL through `HotKeyCache.resolveSyncableHardTtl` and rejects the write before
the cache call. `refreshAll` resolves its shared `PutPolicy` once — local-only by default,
`PutPolicy.synced()` when the caller asks for the broadcast — and applies the same check to that
resolved policy even for an empty map. `refreshAll` also validates every key and loader into an
immutable snapshot before submitting anything, so a concurrent map mutation cannot inject an
unvalidated entry between the validation and dispatch passes. The previous shape checked
`Long.MAX_VALUE` on the raw override only. Owner: `DefaultZeta`, `HotKeyCache.resolveSyncableHardTtl`.

**Elevated entries get value-only updates, from every write shape.** A local write, a load or a
peer PUT/REFRESH on a HOT/COOL entry updates the value, data stamp and birth stamp and re-arms only
the soft window, capped by the existing hard deadline. The active regime, its hard bound and the
stored normal hard/soft baselines all survive; so do the decision lease and its kind. A NORMAL
value update re-arms both normal deadlines. This is one tail — `EntryLifecycle.writeValue` — used
by `putLocal`, `putThrough`, `refresh`, the load path and `DefaultSyncContext`; the receiver no
longer re-arms a HOT/COOL hard deadline that the sender preserved. A logically expired resident is
treated as absent: it gets a fresh lifetime instead of inheriting its dead deadline. Overwriting an
elevated regime would leave an entry whose TTLs the Worker can never repair, because the Worker only
speaks again when the key changes phase. A `put` therefore changes TTLs only while the entry is
NORMAL; on an elevated entry it is value-only as well, because the state machine owns both the
active window and the baseline it will be demoted to.

**`refresh` allocates the stamp before loading.** A concurrent synced write during the load
therefore holds the newer stamp and this refresh's apply is skipped instead of regressing it. A
`null` loader result skips the refresh entirely — a refresh that learned nothing must not evict
the value it was asked to renew.

**A refused write neither lands, nor broadcasts, nor retires dedup.** The write-through apply runs
behind `writeRefused` — the tombstone barrier plus the expiry-aware resident guard (design decisions:
version comparison) — and the cluster PUT fires only when `EntryMutations.apply` reports a changed
post-image. A refused write (a strictly newer resident, a tombstone from an INVALIDATE/CLEAR that
landed between the stamp allocation and the compute, or a resident already carrying the same stamp)
would otherwise ship a value this node has already superseded — or invalidated — to peers that may
not hold the newer entry, re-warming data the local tombstone just removed. The dedup removal is
gated by the same post-image test: the failing write's stamp was superseded, so the newer write or
invalidation already dropped the flight it outranked, and a flight created afterwards may be the
load filling L1 with the newer value. Unconditionally invalidating that flight made its fresh value
skip L1 and sent every subsequent read back to the source. Owner: `HotKeyCache.writeThroughApply`.

**A read fill is not a write-through.** The load path takes a pre-read stamp, stores
version-guarded, preserves an elevated TTL regime and sends no broadcast. A fill that loses
the comparison leaves the newer entry alone and offers nothing to the renewal scheduler: the
post-image test sees the refusal, so a read whose pre-read stamp is already resident cannot
re-arm a renewal window for a value it never stored. The winning write ran its own invalidation.

**A local invalidation removes only the entry it outranks.** `dropLocal` records the tombstone
first, then removes through `computeIfPresent` and keeps a resident whose data version is newer than
the invalidation stamp; the scheduled warm task is cancelled only when nothing survives. A plain
`invalidate` removed whatever was resident, so a concurrent write that landed between the tombstone
and the removal was deleted even though its own apply had already succeeded: L1 then lacked a value
peers hold and its warm task was cancelled, until the next read refilled it. The guard is the same
rule `sweep` applies per key. Owner: `HotKeyCache.dropLocal`.

Owner: `HotKeyCache` (`putThrough`, `putLocal`, `refresh`, `writeThroughApply`, `writeRefused`,
`entryForWrite`), `EntryMutations`, `TransactionSupport`.

---

## Invalidation

**Bump, drop, then broadcast.** Allocate the stamp, remove from L1 **and** from the
SingleFlight dedup cache, then send `INVALIDATE`. Allocating after the removal reopens a window
in which a racing reload is stamped with the old version and then pinned by the invalidate's
higher version.

**An expired resident cannot veto a tombstone.** The per-key guard in
`DefaultSyncContext.evict` compares only a live resident. A logically expired entry with a higher
data stamp must not skip the invalidation: the read path already treats it as absent, and
`HotKeyCache.writeRefused` will let a fill that started before the invalidation land once that
entry is expired. Recording the tombstone at the invalidation's lower stamp blocks the older fill;
a fresh fill receives a stamp above the dead entry and is unaffected. The previous guard compared
the raw resident, so a hard-expired higher-version entry vetoed the tombstone and left that older
fill unblocked. `sweep` records the clear watermark before its walk, so its live-resident guard is
cleanup rather than the protection. Owner: `DefaultSyncContext.evict`, `sweep`,
`HotKeyCache.writeRefused`.

**A covered stamp cannot condemn anything, and must not kill the key's flight.** `evict` recorded
the tombstone and invalidated the key's SingleFlight unconditionally. A late peer INVALIDATE — a
network-reordered message, or a degradation that lost its race — for a stamp an earlier removal
already covered therefore re-entered the bin with no live resident, rewrote a tombstone that already
covered it and dropped the dedup flight a read had started **after** that removal, so a load
carrying a newer stamp returned its value but was silently prevented from filling L1. `evict` now
settles that case as `EVICT_STALE_TOMBSTONE` without touching the flight and without rewriting the
tombstone. The short-circuit is keyed on the live resident rather than on the tombstone alone: a
resident that is merely soft-expired still has to be hard-removed when the same stamp arrives again.
`applyInvalidate` mirrors the guard by consulting the tombstone only when no live resident exists —
with one, the version comparison already orders the event and a tombstone at or above it cannot
exist. Owner: `DefaultSyncContext.evict`, `applyInvalidate`.

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
`DefaultSyncContext.applyClearBatch`, `LoadFlights`.

**The watermark guards every write path.** The per-key tombstones and the clear watermark live in
`EntryLifecycle`, and peer applies, load fills, background refreshes, Worker preloads and the
write-through apply (`putThrough` / `refresh` → `entryForWrite`) all consult the same
`staleAgainstInvalidation` check. Without it a slow read that started before the
invalidation finds L1 absent (nothing to version-guard against) and re-populates the value the
removal just dropped. The write-through apply is the path exposed by construction — it allocates
its stamp, then (for `refresh`) loads, then computes — so the barrier is what stops an INVALIDATE
landing inside that window from being undone by the apply; it was the last write path to carry the
check (2026-10).

**The prefix fast-path bound is the live max, not a historical one.** The prefix snapshot's
maximum stamp gates the `startsWith` walk: a write above it cannot match any live prefix. Pruning
and age-eviction recompute it from the surviving map instead of keeping the highest stamp ever
recorded. Keeping the historical max made every write between the live max and a removed prefix's
stamp walk a table that could not match it; the bound gates only whether the walk runs, never
whether a write is refused, so recomputing it is safe. Owner:
`EntryLifecycleImpl.recordPrefixClears`, `recordClear`.

**One snapshot, not two volatile reads.** `staleAgainstInvalidation` reads the table-wide
watermark and the prefix snapshot from a single volatile reference, so it can never pair the new
table stamp with a post-clear empty prefix map (or an old table stamp with an already-pruned map)
and admit a write both watermarks refuse. The earlier shape read two independent volatiles and
argued the per-key `compute` made the pair safe: it does not. A writer publishes the table stamp
and then empties the prefix map; a reader that sampled the old stamp first and the empty map second
sees a state that never existed and lets a stale fill resurrect a deleted entry. The single
snapshot also removes one volatile read from the write hot path. Owner: `EntryLifecycleImpl`,
`staleAgainstInvalidation`, `recordClear`, `recordPrefixClears`.

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

## Heat plane

**Heat is a lease, not an entry state.** A HOT/COOL decision installs or revokes a `HeatLease` in
`HeatDirectory`; the data plane never observes it. The 2.0 model removed the per-entry
`DecisionStamp`, the NORMAL/HOT/COOL state machine and the hot TTL rewrite. Carrying a decision on
the value forced every load, write, refresh and peer apply to preserve it, and one missed copy
silently changed which lifetime a key ran under — the failure that let a 5-minute declaration serve
for an hour once traffic rose. A lease keeps that contract in one table, so the read path has no
heat branch at all and a plain hit pays one L1 lookup. Owner: `HeatDirectory`, `HeatView`,
`HotKeyCache`.

**The lease table owns decision ordering.** `HeatDirectory.grant`/`revoke` run one per-key
`compute` that compares the incoming `DecisionStamp` under `VersionGuard.shouldSkipForWorker`
(same node: epoch first, then version; a different or anonymous node: arrival order, because both
counters are node-local). A grant over a live HOT lease — a repeat broadcast, or another live
Worker taking the shard — is idempotent and reports `newlyHot = false`, which lets a listener
distinguish a first grant from a repeat broadcast. A grant over a cold key, or over a lease whose
incarnation is gone, reports `newlyHot = true`: the key was not hot while its owner was gone
(`isClusterHot` reports a dead lease as cold), so the takeover is a first grant again. A lease whose
issuing incarnation is gone is not hot: `isClusterHot` identity-removes it and reports the end, so
a dead Worker cannot keep a key prewarmed. Owner: `HeatDirectory`, `VersionGuard`.

**A decision from a dead incarnation never installs a lease.** `grant` first checks the incoming
stamp against the health view, exactly as `isClusterHot` checks a resident one. Without that check,
a HOT left in the queue when a Worker restarts or dies installed a lease and returned
`newlyHot = true`; `prewarmIfAbsent` then called `isClusterHot`, which pruned the lease and queued
`onCool(INCARNATION_DEAD)` on the notification executor while the dispatcher thread was already
running `HotKeyListener.onHot`. The two callbacks could arrive cool-first, leaving the caller's hot
state permanently set for a key no Worker owns. A skipped incoming decision is reported through
`DecisionSkipReason.INCARNATION_DEAD` and never reaches the handler or the listeners. Owner:
`HeatDirectory.grant`, `WorkerListener.applyDecision`.

**A COOL leaves a tombstone, not an absence.** `revoke` writes a short-lived COOL record
(`zeta.local.warm.cool-tombstone-ms`, default 60 s) because version ordering needs a resident
stamp: deleting the record would make a late HOT below the COOL's version look like a first grant
and resurrect the lease. The tombstone expires only after the Worker's in-flight decision window,
measured from its grant time; a `compute` that returns the resident instance must not re-arm the
full window on every duplicate COOL. A cross-node COOL is refused while a different, live
incarnation still owns the key on the consistent-hash ring — for a HOT lease **and** for a COOL
tombstone, because overwriting another owner's tombstone would let an older HOT from that owner
pass the version guard and resurrect. A COOL from an already-dead incarnation never revokes a live
lease: only a same-node, same-epoch HOT lease is pair-closed with `INCARNATION_DEAD`, the one case
where the dead COOL is the last chance to emit the matching `onCool`. The ring rebuild runs before
the compute so the bin lock never walks the health view. The rebuild triggers when either the
resident owner or the sender is missing from the local ring: a Worker that just joined can take a
shard before this node's ring observes the membership change, and checking only the owner rejected
that legitimate COOL as a ghost. Owner: `HeatDirectory.revoke`, `HeatDirectory.isStaleCrossNodeCool`,
`HeatDirectory.reconcileRingForCrossNodeCool`.

**A COOL or a takeover over a dead incarnation still ends the lease, on the dispatcher thread.**
When the resident HOT lease belongs to a Worker incarnation that is already gone — or to a newer
epoch of the same node — the COOL still replaces the record with its tombstone, and `revoke`
returns the ended lease with `INCARNATION_DEAD`. A HOT that replaces the same dead lease returns it
in `LeaseOutcome.endedLease`. Caffeine sees either replacement as `REPLACED` and never fires its
removal listener, so without the returned lease the business side that received `onHot` for the
dead grant would never receive the matching `onCool` — a permanent leak in the caller's hot-state
bookkeeping after a Worker crash. `WorkerListener` emits the end synchronously and in order: before
the new grant's `onHot` for a HOT, and alongside the COOL's own handler call for a COOL. The
previous shape handed the end to the passive-end executor, and that queue could run after the
dispatcher had already delivered the new grant's `onHot(newlyHot = true)`; a listener keyed by
"currently hot" then deleted the fresh state. Keeping both callbacks on the per-key dispatcher
thread makes the order a property of one call instead of two pools racing. Owner:
`HeatDirectory.grant`, `HeatDirectory.revoke`, `WorkerListener.applyDecision`.

**COOL revokes spending, never the entry.** A COOL decision ends the cluster's permission to spend
a background load; it does not evict the resident or shorten a deadline. Evicting on COOL would
throw away a valid value and send every node's next read back to the source exactly when the
cluster decided the key is cooling; shortening a deadline would let a detection signal override
the caller's freshness contract. It does drop a renewal task whose keep-warm trigger no longer
accepts any live heat signal (`HotKeyContext.cancelKeepWarmIfCold`): a cluster-only key that
received a COOL, or whose Worker died and was passively pruned, must not keep renewing without a
real access, while a key still locally hot for `LOCAL`/`ANY` keeps its task. The lease-end observer
`HotKeyListener.onCool` fires only when the COOL actually ended a HOT lease
(`LeaseOutcome.endedLease`), so a COOL for a key this node never held — or one already past its
lease — does not trigger spurious lease-end cleanup. `HotKeyHandler.onCool` is the decision hook
and runs for every applied COOL, including one that only wrote a tombstone: it reports "the cluster
no longer considers the key hot", and a handler that cleans up its own `onHot` state must track
whether it created one. A lease pruned by `isClusterHot`, size eviction or tombstone expiry is
reported through the passive executor and cancels the same cold renewal.
Owner: `WorkerListener.applyDecision`, `HotKeyHandler.onCool`, `HotKeyContext.cancelKeepWarmIfCold`,
`KeepWarm.cancelIfCold`, `HeatDirectory`.

**A repeated HOT is not a new heat event.** `WorkerListener` puts `newlyHot` from the lease table on
the decision's context, and the default handler prewarms only when `isNewlyHot()` is true. A repeat
broadcast or a shard takeover over a live lease must not spend a load: with `keepWarm = OFF` it
bypassed the namespace's opt-out, and for a key the source returns as absent it re-ran the loader on
every broadcast. The listener hook `HotKeyListener.onHot(decision, newlyHot)` still observes every
applied HOT, so business state that needs the decision itself keeps full visibility. Owner:
`WorkerListener.invokeHandler`, `HotKeyHandler.onHot`.

**Keep-warm is a local hard renewal.** `KeepWarm` schedules one task per key and, when it fires,
reloads through the loader the namespace policy resolves at that moment and stores a fresh value
under the entry's own declared hard/soft durations; it never extends the old value's deadline and
never broadcasts. The task is armed from the earlier of the two deadlines, minus a lead and jitter.
The lead is capped at `min(lead-max-ms, window / 4)` with a floor of `min(1 s, that cap)`, so the
lead plus its jitter consumes less than 3/8 of the window; the earlier fixed 1 s floor turned a
one-second window into a renewal every 200 ms (the old `window - lead - jitter` went negative and
was clamped to the floor). The task is skipped when the window is below `min-window-ms` or the
deadline is permanent. At fire time the task re-checks the resident: the entry must still exist,
still carry the task's `dataVersion`, still be logically valid, and still match the keep-warm
trigger, so a write, a peer value or an invalidation that landed while the task waited wins.
Re-resolving the loader is what makes a runtime policy update take effect instead of
renewing through the loader captured when the task was armed; a synchronous rejection from
`renewHard` releases the task slot so the key can be re-armed. A resident whose `dataVersion`
moved without a scheduler offer (a peer write or a soft refresh) is re-armed against the new
resident instead of dropping renewal, and a peer-driven removal cancels the key's task. Owner:
`KeepWarm`, `DefaultBackgroundRefresher.renewHard`, `DefaultSyncContext`.

**A write carries read evidence forward only within one renewal window.** A value write on a key
with an armed task starts a new cycle bound to the new version, but it is not a read: the task
copies the previous cycle's `lastReadMonoMs` and keeps that cycle's `armedAtMonoMs`, so the
inherited read still satisfies the gate. Starting the new cycle at `now` would make the read
predate it and the key would be dropped as "no local read" merely because a write replaced the
entry first. The copy expires after one renewal window (`nowMono - lastReadMonoMs <= delayMs`):
without that bound a steady write stream — a peer PUT every few seconds — kept a read from hours
earlier satisfying the gate forever, so a key this instance had long stopped serving renewed on
every write. The read tide also compares the resident's `dataVersion` with the task's; a soft
refresh or a peer write can install a shorter window while the old task still waits for the old
deadline, and re-arming from the tide is the only point that sees the new version early enough.
Read evidence only ever moves forward: `WarmTask.recordRead` max-merges it under a `VarHandle` CAS,
because one tide carries a single sample for the whole batch while a task replaced mid-walk is
already armed by a newer cycle — writing the batch's older sample over that newer read pushed the
evidence back before `armedAtMonoMs`, and the next fire dropped a key that had just been served.
Owner: `KeepWarm.scheduleWithPolicy`, `KeepWarm.onReads`, `KeepWarm.WarmTask`.

**A cancelled cycle leaves nothing behind.** A task can be dequeued before its cancellation lands,
so `cancel(false)` returns `false` and the body still runs; the task therefore carries a
`cancelled` flag that `fire` reads, and every rescheduling point re-checks ownership with
`tasks.remove(key, task)` instead of removing blindly. Rescheduling from the completion of a task
the application already cancelled or replaced armed a new cycle for it — a "zombie renewal" that
kept a deleted or cooled key loading. The re-arm signal is `WarmSignal.RENEW`, never `READ`: a
renewal settling is not evidence that this instance served the key, and feeding the completion
back as a read let a cluster-only key renew forever with no reader. The scheduler runs with
`removeOnCancelPolicy`, because every write, peer PUT, soft refresh and invalidation replaces the
task, and a cancelled future otherwise stays in the delay queue with its task and key until the
original deadline. A task is also dropped when the new version declares a lifetime that must not
be renewed (permanent, or a window below `min-window-ms`), when the resident is gone, and when the
namespace no longer matches keep-warm; a task whose bound version merely expired is kept, because
its renewal may be in flight at that instant and `onReads` cannot tell the two apart.
`destroy()` clears the map and then cancels unconditionally: an arm racing it may find the entry
already cleared, so a `remove(key, task)`-guarded cancel would skip on exactly the interleaving that
needs it and leave the future and its captured task in the delay queue for the rest of the window.
Owner: `KeepWarm.fire`, `KeepWarm.onReads`, `KeepWarm.scheduleWithPolicy`, `KeepWarm.WarmTask`,
`KeepWarm.destroy`.

**A cluster-triggered renewal needs a real read.** A cluster lease alone is not evidence that this
instance serves the key: every instance receives the same HOT broadcast, and a lost COOL is never
replayed. A task armed by `WarmSignal.HEAT` therefore starts with no read evidence and fires only
if a read for that key arrived after the task was armed (`lastReadMonoMs >= armedAtMonoMs`, fed by
`AccessCounter`'s tide through `ReadObserver`). Local-TopK membership is its own evidence, because
the detector counted real reads. Without the gate, a node that received HOT for a key it never
serves would reload it until the lease ended — one background load per idle node, multiplied by
every hot key. Owner: `KeepWarm.fire`, `KeepWarm.onReads`, `AccessCounter`.

**Detection never pays for a framework load.** `HotKeyCache.warmAsync` and the keep-warm renewal
call the loader directly and store through the guarded write path; they record no access, so a
prewarm or a renewal cannot manufacture heat on the node that performed it. The prewarm path is
non-blocking: the prewarm caller submits the reader and attaches to the flight's completion stage
instead of waiting on an executor thread the reader may need. The prewarm trigger (`HeatTrigger`)
only decides whether a load may be spent; it is not an access record. Owner:
`HotKeyCache.warmAsync`, `SingleFlight.loadAsync`, `HotKeyContext`. This is also why the context
exposes actions and heat queries but never the namespace loader: a caller that could call
`loader.reload` itself would run the load on the dispatcher thread, outside SingleFlight and the
version guard.

**The renewal is value-only, the loader is namespace-scoped.** `WarmRegistry` declares a prewarm
trigger, a keep-warm trigger and an optional namespace loader; `NamespacePolicyRegistry` resolves
them with YAML override > programmatic > annotation > global precedence. Keep-warm is independent
of the soft TTL: a hard-only namespace can renew, and a positive soft window alone does not
authorize spending. Coupling the two made a soft-TTL edit silently switch renewals on, so a
namespace that wants one declares `keepWarm` (or inherits the global trigger).
`TtlContract.dynamicTtl` blocks prewarm only: a guessed lifetime is worse than a miss, while a
renewal can always use the resident's own stored durations. Owner: `NamespacePolicyRegistry`,
`WarmRegistry`, `TtlContract`.

**Every decision is sent once, and a same-incarnation batch is reduced.** `WorkerBroadcaster`
publishes each HOT or COOL exactly once; the receiver's version guard makes a late duplicate
harmless, so no per-key retry chain exists. `WorkerListener.dispatchBatch` first checks whether the
whole batch comes from one Worker incarnation — node id **and** epoch. Only then can it fold, and it
folds against the same live-stamp rule
`grant` uses: a lease whose incarnation is gone contributes no stamp, so a decision from the new
incarnation is not dropped as stale by the dead one's version. A same-type run keeps the last
decision; a COOL takes the place of the HOT before it **and of the COOL before that HOT**, leaving
at most one COOL followed by one HOT. Dropping only the HOT left `[COOL(v2), COOL(v4)]` from
`[COOL(v2), HOT(v3), COOL(v4)]`, and the second COOL found the tombstone the first had written:
the business `onCool` ran twice for one hot cycle while the receiver neither prewarmed a key it was
about to cool nor ran `onHot` three times for three versions. The covered decision is reported as
`COVERED_BY_NEWER_DECISION`; the version-stale one as `VERSION_STALE`. The fold lives directly in
`dispatchBatch` as two locals (`cool`, `hot`) because its result shape is fixed at
`∅ / [COOL] / [HOT] / [COOL, HOT]`; a list and its remove/set branches allocated on every batch for
a value that never grows past two. Mixed-node, mixed-epoch or anonymous runs are applied sequentially, because
both the version comparison and the ghost-COOL guard depend on which incarnation owns the resident;
a batch spanning a restart carries two version spaces that both restart at 1. Folding
`[HOT(v2, epoch 1), COOL(v1, epoch 2)]` kept the COOL as the newer decision and reported the HOT as
covered, so the key stayed cold although the restarted Worker had just granted it — the new
incarnation's own HOT was lost to a version space that had already ended. Only a batch from one node
id **and** one epoch is comparable; every other batch reaches `HeatDirectory`'s incarnation and
version guards decision by decision.
A dispatch-refused COOL is applied inline on the consumer thread (no loader, no user handler),
while a refused HOT is dropped: its prewarm must not run on the AMQP consumer thread. The listener
fan-out also checks `listeners.isEmpty()` before allocating a hook lambda, so a deployment with no
observer pays nothing per decision. Owner: `WorkerListener.dispatchBatch`,
`HeatDirectory.liveStampOf`.

## Access counting

**Every served read counts once.** `AccessCounter` owns a single `WaveCounter`. Each tide
feeds the reporter's batch; once `detect-interval-ms` (500 ms) has elapsed, or a tide reaches the
accumulator's key cap, the residual accumulator and that tide go into `HotKeyDetector.ingest`.
Before this, the read path appended to two counters with
two hashes, two thread-local lookups, two capacity decisions and two independent reservoir caps
that could disagree — one pipeline admitting a key the other dropped.

**Tag-mode routing belongs to the counter.** `AccessCounter.record(cacheKey, count, TagMode)` owns the
four-way switch (`RECORD`, `DETECT_ONLY`, `REPORT_ONLY`, `SILENT`), and `HotKeyCache.recordAccess`
calls it directly. The previous shape decomposed the mode into two booleans in the dispatcher and
rebuilt the enum's meaning with nested `if`/`else` — a new mode could silently fall through to the
merged read path, and the routing rule lived away from the class whose documentation describes it.
Owner: `AccessCounter.record`, `HotKeyCache.recordAccess`.

**A tag batch is validated once and routed as one shape.** `Zeta.tagAll(Map, TagMode)` copies while
validating (non-blank key, positive count, non-null mode) and hands one snapshot to
`AccessCounter.recordAll`; the collection overload builds that snapshot with duplicate keys summed.
`RECORD` iterates the shared reservoir, `DETECT_ONLY` hands the map to `ingest`, and `REPORT_ONLY`
publishes it as one batch. Single-key tagging uses the same mode switch, so the two entry points
cannot diverge in routing. Failure it prevents: a batch that drops a bad row (or a zero count) and
applies the rest, N direct sketch updates or N single-key reports from one caller batch, and a
concurrent map mutation injecting an unchecked row after validation. The former public surface
exposed three non-intersecting overloads (`tag(key)`, `tag(key, mode)`, `tag(key, count)`) with no
batch form; a caller needing both axes had to pick one. Owner: `Zeta.tagAll`, `DefaultZeta.tagAll`,
`AccessCounter.recordAll`.

**The tide period and the sketch cadence are two clocks.** The tide base starts at
`report-interval-ms` and the reporter's feed-loop tuner moves it inside
`[report-interval-min-ms, report-interval-max-ms]` (50–1000 ms); the sketch cadence is the fixed
`detect-interval-ms` the accumulator checks on every tide. Detection must not follow the reporting
control loop — a tuner that slows reporting under pressure would otherwise slow TopK admission with
it. The configured interval is a floor, not a ceiling: a tide slower than it stretches the ingest
with it.

**Without a reporter the tide already is the sketch cadence.** The detector accumulator exists to
bridge a 50 ms tide to a 500 ms ingest, so when no reporter is wired the fresh tide goes straight to
`HotKeyDetector.ingest` and no accumulator is allocated. Keeping the buffer turned every tide into a
full `HashMap.merge` copy of up to 16,383 keys plus a deferred 1 ms flush task, because the cadence
check compared wall-clock samples: a tide that fired 1 ms early was "not due", merged the whole
snapshot into the residual and re-armed a flush one millisecond later — then the next cycle landed
1 ms early again and repeated it. The local mode now pays one ingest per tide and no second map, no
copy and no extra scheduled task. Owner: `AccessCounter.mergeIntoSketches`, `AccessCounter`.

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

**An empty tide is not an ingest.** The reporter's feed loop samples zero-arrival windows, so
`onTide` receives empty snapshots. `mergeIntoSketches` must return before `flushNow` when the
snapshot is empty: advancing `lastDetectorFlushMs` without handing anything to the sketch makes the
first real traffic after an idle period wait out a full detector interval. Only a non-empty residual
is drained at the boundary. Owner: `AccessCounter.mergeIntoSketches`.

**A cancelled detector flush may already be running.** `ScheduledFuture.cancel(false)` cannot
interrupt a task that has started, so a tide's `flushNow()` can cancel an armed flush that is
already waiting for the accumulator monitor, drain the accumulator itself, and later arm a new
task. Without an ownership token the old task then clears the new task's handle and advances the
cadence anchor from an empty accumulator — the replacement becomes uncancellable and the next real
batch waits a full interval. Every arm and every cancellation advances `detectorFlushGen`; the
scheduled task captures its generation and returns immediately if the field has moved on. Owner:
`AccessCounter.flushDetector`, `scheduleDetectorFlush`, `cancelPendingDetectorFlush`.

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

**Every sketch row needs an independent hash.** Row zero uses the key's mixed fingerprint; each
later row re-avalanches it with `Hashes.mix32(fingerprint + row * 0x9e3779b9)`. XORing the
fingerprint with a row constant is fully correlated: XOR and the power-of-two mask commute, so two
keys colliding in row zero collided in every row, and the depth-`d` collision bound collapsed to a
single row. The same correlation survived the FastRange path whenever the two fingerprints shared
their high bits. Owner: `HeavyKeeper.bucketIndex`.

**The sketch may approximate counts; it must not invent traffic.** An owned estimate: a row
contributes to a key's estimate only while the slot's fingerprint matches that key. A colliding
fingerprint contributes **zero** unless the slot changes hands (size-aware takeover or the
displacement lottery) — the occupant's decayed count is never handed to the newcomer. Before
this rule a cold key sharing a slot inherited someone else's traffic and could enter the TopK
on it.

**A slot payload publishes after its fingerprint detaches.** `probeSketch` is deliberately
lock-free while every writer holds the slot's stripe lock, so it can read a slot mid-update. Both
handover paths therefore park the fingerprint on the reserved `0` switching sentinel before they
move the sum, publish the sum with release ordering, and publish the fingerprint last; the probe
reads the fingerprint, the sum, then the fingerprint again and discards the sample when the owner
changed. Without the detach a probe could match the old fingerprint and then read the incoming
key's sum, so a member whose traffic had rolled out of the window "borrowed" the new owner's count
and kept its seat through the decay cycle. `fingerprint()` maps a zero hash to `1`, reserving `0`
as the sentinel. Owner: `HeavyKeeper.probeSketch`, `updateEmptySlot`, `takeoverSlot`,
`applyIncrement`, `decayCollisionSlot`, `rotateSketchWindows`.

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

**Collision decay is exponential in the occupant's count.** Each incoming unit removes one occupant
unit with probability `decay^cur`, sampled as `Binomial(delta, decay^cur)` and capped at
`min(delta, cur)`. A hot slot therefore resists cold churn: at the default `decay=0.92` a
one-unit collision against a 1,000-count slot has probability `0.92^1000`, effectively zero, while
a small count still turns over quickly. Before this rule the collision sampled
`Binomial(cur, decay)` per collision — a constant per-unit survival probability independent of the
occupant's count. One `delta=1` cold key hitting a 10,000-count slot removed about 8% of the count;
under a high-cardinality cold scan the count collapsed geometrically and the real hot key was
evicted from the TopK. The exponential form is the HeavyKeeper protection the sketch exists to
provide. Owner: `HeavyKeeper.decayCollisionSlot`, `collisionDecays`, `decayProbability`.

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

**A stripe owns a contiguous block of slots.** The monitor of slot `i` is
`lockStripes[i >>> stripeShift]`, with the shift derived from the geometry so a block covers ~16
slots and the monitor count is clamped to 64…2048. Interleaving by `i & lockMask` put _adjacent_
slots under different monitors, and one 64-byte line holds two or three 24–40-byte slots: two
threads updating neighbours ping-ponged that line between cores while sharing no data at all, and
`fading` swept the table strided by the monitor count — 2048 passes at a ~80 KB jump, touching
every page 2048 times and defeating the prefetcher. With contiguous blocks a monitor's slots cover
whole lines, so a boundary line is the only one two monitors can share, and the sweep is one
ascending pass over `table`. The shift is one integer op on the hot path, and hash uniformity is
unchanged because `bucketIndex` already spreads the fingerprints. Owner: `HeavyKeeper.lockStripes`,
`stripeShift`, `addToSketch`, `rotateSketchWindows`.

**Membership counts use max-merge with a deadband.** A hit adds its batch to the sketch before the
raise, so the stored count was behind on every hit and the strict compare-and-raise executed a
locked CAS each time. The write is now skipped until growth reaches `max(16, cur / 32)` — the
stored count only feeds ordering and the eviction floor, both tolerant of a few percent of lag —
while a member near `minMemberCount` still raises eagerly, because there the lag decides its seat.
The `LongAccumulator` experiment remains reverted: `reset()` silently drops concurrent
`accumulate()` calls, and a plain `LongAdder` cannot express max-merge.

**A detached member is tombstoned, not merely removed.** The lock-free raise path resolves its
`Node` before the sketch update, so a concurrent eviction can remove that node in the window. Both
removal sites write the reserved negative `DETACHED` value into the node's count: the admission
eviction with a `getAndSet`, the decay drop with a CAS loop that keeps a concurrent raise instead of
overwriting it. A raise that observes the tombstone returns `false`, and `admit` falls through to
the admission lock, re-admitting the key and firing the admission observer. Without the tombstone a
stale node reported `isHotKey = true` while `contains(key)` was already false, and a decay-time
raise could be silently discarded by the removal. The decay drop CASes and unlinks inside the sweep
itself: every structural write to `members` already runs under `admissionLock`, so wrapping it in a
map `compute` bought nothing but a second hash lookup, a captured lambda and a temporary victim
list. Owner: `HeavyKeeper.raiseCount`, `decayMembership`, `evictMinAndPut`.

**The admission observer runs after the admission lock is released.** `notifyAdmitted` reaches
`KeepWarm.onLocalAdmitted`, which reads the L1 table and schedules a renewal; both take locks this
class does not own (a Caffeine bin, the scheduler queue). Invoking the observer inside
`admissionLock` made every admitting read wait on those resources and inverted the class's stated
lock order. `admit` now records whether the key newly entered and fires the observer after the
`finally` unlock; the TopK state is already authoritative, so an observer that runs a few
instructions later cannot admit a stale member. Owner: `HeavyKeeper.admit`.

**Admission resolves the victim and the runner-up in one O(k) pass, then re-validates the victim
under the admission lock.** `findTwoSmallest` returns the minimum, the count it saw and the
second-smallest count in one walk, because the newcomer can only be compared against the count the
scan observed (member counts are raised concurrently) and the runner-up becomes the new
`minMemberCount`. A second walk would pay two full O(k) scans under `admissionLock` per admitted
key. If a lock-free raise lifted the minimum between the scan and the eviction, `isStaleMinimum`
re-resolves the pair instead of evicting a key that is no longer the coldest; without that re-check
a hot member could be evicted while the running minimum stayed wrong for the rest of the cycle.
Every departure, write-path eviction and decay drop alike, goes through
`offerExpelled` — from `evictMinAndPut` or the inline drop in `decayMembership` — so
`AddResult.expelledKey()` and the expelled queue never miss a seat loss. Owner:
`HeavyKeeper.admitOrEvict`, `findTwoSmallest`, `isStaleMinimum`, `evictMinAndPut`,
`decayMembership`.

**The promotion floor is a fast reject, zero while the set is not full.** `minMemberCount` is a
volatile lower bound of the coldest seat, so a candidate at or below it cannot win a seat and is
refused without taking `admissionLock`; the authoritative re-check still runs under the lock. While
`members.size() < k` the floor is kept at `0`, because every fresh candidate is admissible and a
non-zero floor would leave the Top-K permanently unfilled. Ties are refused (`<=`, not `<`): a
candidate equal to the coldest member would evict it only to be evicted by the next equal burst,
a churn no ranking benefit pays for. `ADMITTED_SILENT` lets a batch ingest skip the per-key
`AddResult` allocation without losing the admission notification. Owner: `HeavyKeeper.admit`,
`admitOrEvict`, `decayMembership`, `ingestBatch`.

**A ranking snapshot is taken before sorting.** Each member's count is read once into its `Item`
before the sort. Member counts are mutated concurrently (the lock-free max-raise in `admit`, the CAS
re-synchronisation in `decayMembership`), and a comparator that re-read a mutable counter mid-sort
could feed TimSort an inconsistent total order and abort with `Comparison method violates its
general contract!` once membership exceeds TimSort's `MIN_MERGE` of 32. Sorting the immutable
snapshot is total-order-consistent by construction. A node already carrying the `DETACHED` tombstone
is skipped rather than clamped to zero: it left the Top-K inside the window this weakly consistent
walk is traversing, and clamping published it as `Item(key, 0)` — a zero-count ghost in `list()` and
`listTopN()` for a key no longer in the ranking. The truncation clears the tail of the sorted list
in place instead of copying a sub-range. Owner: `HeavyKeeper.snapshotItemsSorted`.

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
trips the breaker; `includeExceptions` is the only set that can. A thrown exception matches when any
configured type is assignable from its class, a superclass, or a directly declared interface — so
subclasses and interface implementations count. The whole cause chain is walked (depth-capped),
because a wrapped exception must still match its underlying cause. Names are resolved once and
cached, so a mistyped class does not re-run `Class.forName` per classification.

**An unresolvable include list matches nothing.** The two zero-class outcomes are different
answers: no list configured admits every exception as a failure, while a configured list whose
names all failed to load (`ClassNotFoundException`, `LinkageError` — a typo, or an optional driver
outside the classpath) admits none. Both used to collapse into one empty array, so one misspelled
name silently inverted the operator's whitelist into "every business exception trips the breaker"
and the shard opened on the first one. The list therefore resolves to `null` when unconfigured and
to an empty array when configured-but-empty, and a partial resolution keeps the usual whitelist rule:
an exception matching no resolved name is not a failure. Owner:
`SlidingWindowBreaker.resolveIncludeClasses`, `isIgnorableException`.

**Sharding follows the annotation namespace.** `CircuitBreakerRegistry.prefixOf` reads the segment
before the namespace delimiter (`U+0000`) first, then the conventional `:` datasource prefix, and
falls back to one shared bucket. Without the delimiter check every annotated key — whose physical
form is `cache\u0000logicalKey` — shared the default shard whenever the logical key held no colon,
so one slow `@ZetaCached` namespace tripped every other namespace with it; when the logical key did
hold one (the default-key encoding, a composite id), the colon split inside the logical key and
scattered one namespace's failures across high-cardinality shards, preventing the breaker from
tripping and evicting state. Owner: `CircuitBreakerRegistry`.

**Window validation and the exception filters are resolved once per configuration.** Every new
prefix built a `SlidingWindowBreaker`, and each construction re-ran `validateConfig` and resolved
every configured exception name with `ClassUtils.forName` — a classloader lookup and an array
allocation per shard, one `WARN` per shard for each unloadable optional driver class, and a bad
window that surfaced only on the first miss which created a shard. `CircuitBreakerRegistry` now
builds one `SlidingWindowBreaker.Spec` at startup and hands it to every shard, so a bad window fails
the context instead of a request. `enabled` and the thresholds stay live on the configuration; the
window geometry was always baked into a shard's bucket array at construction, so a runtime change
never reached an existing shard either. Owner: `CircuitBreakerRegistry`, `SlidingWindowBreaker.Spec`.

**The breaker machine is one immutable snapshot.** `CLOSED → OPEN`, `HALF_OPEN → OPEN`,
`HALF_OPEN → CLOSED` and the wedged-episode reset each swap one `Epoch` — state, episode
generation, live probe slots, consecutive successes, the episode clock, the probe-credit
multiplier, the monotone window offset and the CLOSED window floor — with a single
compare-and-set, so a failure storm trips once, logs once and stamps the
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
`HALF_OPEN → CLOSED` publishes the window floor in the same CAS as CLOSED: the epoch advances
`windowOffset` and records `closedWindowIndex`, so an evaluation never counts a pre-close bucket
even when the clock slice did not move. The probe slot release, the winning success and the
transition are one CAS: no transient HALF_OPEN with a free slot is published between the threshold
check and the close, so a ghost probe cannot enter a recovery that has already met its threshold and
an old probe failure cannot reopen it. Only a settlement that meets the threshold closes, and the
close is generation-guarded: a concurrent success that merely continues the streak does not close. A
closer that loses that CAS publishes nothing, so it cannot erase a winner's fresh counts; the floor
is ignored once half a ring has elapsed because the 24-bit index has wrapped. A HALF_OPEN episode
whose probes stop settling is force-retired after `singleTestIntervalMs`, and the same CAS
starts the replacement episode owning one slot (a hung probe cannot pin the breaker; slow-but-alive
probes keep settling and keep the episode alive). Every admitted probe refreshes the last-activity
anchor, so a burst that starts after an idle episode is not mistaken for a wedged one: the timer
measures the last admission or settlement, not the episode's first probe. Without that refresh, the
first burst after a quiet period reset the episode and retired the probes that had just started,
resetting the consecutive-success streak and delaying recovery. Rate evaluation stays per-failure — a collapsing
gate would skip the very failure that completes a same-millisecond burst past the threshold, and
bursts are exactly how a small-volume window trips — while the transition CAS bounds a storm to one
decision and one log line. Disabling the breaker force-closes it to CLOSED with the episode retired
and the window floor advanced, so stale buckets cannot flap it OPEN/CLOSED while disabled. Exception
filters resolve eagerly at construction through the context classloader (Spring `ClassUtils`),
removing the first-classification race a lazy resolve had and surviving layered-classloader
deployments.

**`isOpen` is the fast-fail state; `isDegraded` is the read path's gate.** A HALF_OPEN breaker
admits only its probe quota and rejects the surplus, so `isOpen == false` while those surplus loads
are being refused. The read path therefore asks `isDegraded` (OPEN, or HALF_OPEN), and a rejected
probe, a failed probe and a null recovery answer all take the stale fallback instead of throwing.
`isOpen` remains the exact OPEN predicate for callers that need the fast-fail state itself. Owner:
`CircuitBreaker`, `CircuitBreakerRegistry`, `LoadFlights`.

**The window is a clock-driven ring, not a scheduled one.** A settlement stamps its bucket with the
aligned effective window index (`now / slideMs + epoch.windowOffset`) and zeroes that bucket when
the index moved on, so the window
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
1 ms slice and ≈ 194 days at the default 1 s slice. A settlement treats a slot advanced by at most one window as an out-of-order sample and discards
it. A larger forward distance is ambiguous: the slot may be an old slice that wrapped (rotate it),
or a live slot ahead of a settlement whose thread stalled past the window. For the lower half of
that distance the cold path samples the live window once and discards the settlement when the slot
still belongs to the live window set, so a paused writer cannot rotate a live bucket back to an old
window and erase the counters it accumulated; the hot path pays nothing for this check. Any slot
behind the caller is the previous slice or a bucket idle past half the id range and is rotated to
the caller's slice. The residual ambiguity is the wrap itself: a bucket within one ring width
before 2^24 slices is discarded for at most that many settlements, and a bucket exactly 2^24 slices
old contributes its stale counts once before the next settlement rotates it. Owner:
`SlidingWindowBreaker.record`. The
monitor per bucket was wrong about its own contention: the buckets are time slices, not thread
shards, so every concurrent settlement of the live slice targets the same monitor and the other nine
spread nothing; the rate scan additionally took ten locks. The packed word also removes the ten
`Bucket` objects and their per-object headers. _Ruled out:_ the monitor ring (2026-11, see _Ruled
out_).

**The CLOSED window floor publishes with the state, not as an array reset.** Every CLOSED
transition bumps a monotone `windowOffset` in the same `Epoch` CAS and records the resulting
`closedWindowIndex` plus `closedAtMs`. A bucket stamped before the close is therefore behind the
floor even when the clock slice did not move, so `record` and `evaluateThreshold` ignore it without
rewriting the array. This removes the old `resetBuckets()` loop and, more importantly, its race: a
thread that lost the CLOSED CAS could still be walking the array and erase counts a winner had
already written. A loser now publishes nothing. The floor is ignored once `now - closedAtMs`
reaches half a ring, because the 24-bit index has wrapped and the normal forward-distance test is
authoritative again. Owner: `SlidingWindowBreaker.record`, `evaluateThreshold`, `Epoch.closed`.

Owner: `SlidingWindowBreaker`, `CircuitBreakerRegistry`, `ZetaProperties.CircuitBreaker`.

---

## WaveCounter: buffering boundary

**`WaveCounter` is a counter with a delivery contract, not a policy layer.** It routes keys by
heat into two paths: hot keys merge into a per-writer local map (no shared contention, bulk-
merged into the shared table every 128 distinct keys) and cold keys write directly to a lock-free
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
  repeated-key op — the common hot pattern — never pays the beacon read. The test is reference
  equality (`key == lastKey`), not `String.equals`: a content-equal but distinct instance takes the
  full probe, which is exactly the string walk the shortcut exists to avoid. The full path refreshes
  the cache after a successful existing-slot add as well as after a claim; a drain never clears a
  slot or its key, so the cache can only go stale through a bulk capacity reset, which clears it;
- on a cache miss, the beacon test plus one plain load of the slot's count and one release store of
  the new value (the single-writer protocol below) — no lock, no CAS, no atomic read-modify-write;
- a monotonic sequence publish (a release store of a thread-private counter), which is the only
  cross-thread ordering the drain needs.

The per-map `ReentrantLock` is paid only while the writer itself discharges a full batch, and the
deliverer probes it with `tryLock` — a busy writer skips its turn rather than blocking the tide.

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

**Construction is the validation boundary.** `batchConsumer` is non-null, `opMaxCount` and
`hotLimit` are range-checked, and `capacityLimit` (the configured cap plus its 10% headroom, or
`Long.MAX_VALUE` when unbounded) is final so a published counter cannot expose a half-set cap to a
writer.

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
the only last-tide membership memory — is captured in `splitIdx` before the halving decay zeroes
it, so the capacity break never evicts a renewing key and the hot set stays stable under flat
distributions where every promotion would otherwise be a coin flip. One array carries both sides:
incumbents fill from index 0 upward, newcomers from the top downward, and the two fronts cannot
cross because every qualifying key lands on exactly one side. Selection and split are
routing-only and cost zero per op; the deliverer pays the bucket pass, the quickselect and the
tie/blocked pass on scan tides (O(bucket) expected) plus the incumbent split on overflowing tides.
A stable-workload shortcut reuses the previous tide's exact boundary as a sweep filter, so
selection runs over the ~`hotLimit`-key list instead of the bucket whenever that list provably
holds the k-th largest. Two gates bound the shortcut's cost: a previous boundary of 1 is not
collected at all (it would admit nearly the whole snapshot), and a list materially larger than the
view the bucket path would scan is declined — a stale low boundary must not turn a few-hundred-key
selection into an O(snapshot) one.

**The boundary is an exact selected count, not a quantized bucket edge.** A 64-sub-bucket
quantization of the bucket systematically **underestimated** the true cutoff (`kth >= 2^b`), and on
non-overflowing tides the refinement was skipped, so the governor's "floor above boundary" stale
evidence, the flood signature, the admit condition and the veto band were all computed against a
lower boundary than the honest cutoff. The result was over-probing healthy states, inflated excess,
over-filtering, flicker and admit latency. The exact selection costs a deliverer-side three-way
quickselect over a compact value buffer (`selectValues`), never over an indirect index view: the
scan is sequential, the packed (value, hash) pairing survives, and the three-way post-condition
yields the qualifying count directly (`n - lt`), so the second pass over the view is gone. A
deterministic pseudo-random median of three blunts adversarial orderings that defeat a fixed middle
pivot; equal-value bands (flat workloads) collapse in one pass instead of degrading to O(n²).
Owner: `WaveCounter.estimateBoundary`, `selectBoundary`, `selectKthPartition`, `selectValues`,
`aboveKthIdx`.

The histogram is 63 buckets because a positive `long` has at most 63 significant bits:
`63 - numberOfLeadingZeros(v)` lands in `[0, 62]`, so every bucket is reachable and `1L << bucket`
stays positive — the old 64th bucket was unreachable and its clamp branches were dead. Bucket 0 is
exactly the count 1, so its cutoff and qualifying count are known without a scan.

**`hotLimit = 1024`** caps the active promoted set so pathological traffic cannot grow it without
bound. The non-overflow scan enforces the same cap against the slots that survived the decay: its
selection bound is only the current tide's qualifying count, so 900 surviving incumbents plus a
1024-key burst used to activate every newcomer and read ~1924 active rooms. When `remain` reaches
`hotLimit`, only an existing beacon member may renew; a newcomer is skipped without entering the
renewal numerator. The routing beacon is the compact 2+2 role-evidence array of `hotLimit × 32` rooms (≈16 KB
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
quiet regime (rate < `QUIET_RATE_PER_SEC`, empty set) drops the floor to `QUIET_FLOOR` so any key
routes hot, and a raised floor blocking the whole incoming distribution at high volume collapses in
one tide (the flood collapse) instead of the wrong-direction raise-walk plus a slow decay-driven
collapse. Entering and leaving the quiet bypass are both complete regime switches
(`endWalk`, `state.reset`, `positions.reset`, `rates.reset`; the entry then sets `QUIET_FLOOR`):
the walk/retreat state and the anchors were measured against the other volume regime, and a
one-sided reset strands them. Restoring only the floor on re-engage leaves an in-flight walk armed
against the quiet floor's base: its crash undo drags the floor back to `QUIET_FLOOR` — below
`PROMOTION_FLOOR` — and its `retreatTarget` points at a sub-seed position, so the governor spends
tides climbing out of a floor it must never hold. The volume EWMA is folded once per non-empty
tide by the deliverer in counts per second (`snapshotSum * 1000 / windowMs`), not in raw per-window
counts: the cadence is adaptive and externally adjustable, so dividing a window-count EWMA by the
current window mixed units (a 500 ms sample under a 50 ms interval over-ranked the rate ~5.5x, and
a stable 50 ms interval under a 1000/s stream read as quiet). Both switches are wall-clock-debounced. Three refinements bound the probe
machine's worst cases: a raise CONFIRM that lands over-filtering corrects to the boundary
immediately; a confirmed raise shields the re-arm for `CONFIRM_SHIELD` tides (no
alternating-workload ratchet); the walk budget binds on both verdict branches.

**A release walk prices its stride from the smoothed renewal against its own crash bar.** The
release descends by `clamp(round(STEP_INITIAL * (smoothed - bar) / noise), 1, STRIDE_MAX)`, with
`bar = baseRenewal - vetoMargin()` and `noise = max(MIN_TIDAL_EVIDENCE, margin)`. At the arm the
stride is the initial probe step; approaching the bar it shrinks to 1 so the verdict samples the
decision zone finely; below the bar it creeps while crash persistence accumulates. A
bold-driver/fixed-step decay plunged to the seed before the crash verdict could fire and overshot
the earners on the raise side; replacing it reduced measured excess by 51%. Owner:
`MoonsTidalForce.releaseStride`, `releaseBar`, `handleReleaseWalkWalking`.

**A stage-0 retreat reads the live renewal and pivots when the descent is falsified.** The veto
holds for `PROBE_STREAK` tides and samples renewal; recovery re-plants the anchor at the held
position, persistent shortfall deepens the target. The stage-0 descent reads the live renewal on
every stride: a rung that earns the target parks there and re-plants the anchor (mid-descent
recovery), while under-earning the descent's own start for `SLOPE_STREAK` tides falsifies the down
direction and pivots to a raise. A blind budgeted descent walked past the first healthy rung toward
a target that was never re-measured, discarding a correct newer position. Owner:
`MoonsTidalForce.retreat`, `plant`.

**A regime collapse resets the governor in one tide but preserves priced ladder state.** An empty
hot set above the seed, or a raised floor blocking the whole high-volume distribution (the P2 flood
signature), proves the floor is stale. Resetting immediately avoids the wrong-direction raise walk
(parked by the renewal == 0 gate) plus the slow four-tide decay and the empty-set collapse.
In-flight walks and retreats are cancelled so their budgeted undo cannot drag the floor back;
distress/veto history, the audit clock, the anchor and the renewal/volume references belong to the
old regime and are reset. A walk killed by the collapse is priced FAILED before the wipe, and any
already-priced ladder state survives, so the backoff throttle keeps an oscillation loop bounded;
`floodLock` deliberately survives, because clearing it would re-arm into the same flood. Owner:
`MoonsTidalForce.collapse`, `emptyCollapse`, `floodCollapse`.

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

**The hot path is exact without an atomic on the slot.** Each writer owns its slots: the owner is
the only thread that stores a key or a count. Every increment — a fresh claim or an existing-slot
add — stores the count and then publishes a monotonically increasing sequence with release
ordering. A drain reads the sequence with acquire, scans the occupancy bitmap, merges
`counts[i] - drained[i]` into the shared table, and advances only its private `drained[]` baseline;
it never clears a slot, never rewrites a key, and never touches the last-key cache.

Two interleavings cover the race. A count stored before the drain's sequence read is either seen by
the slot scan (its bitmap bit was published before the read) or missed there (the bitmap read
predates the bit) and then picked up by the next round, because the sequence moved. A count stored
after the read may still be observed by the scan — then the next round's difference is zero,
because the baseline advanced to the value it observed. Never lost, never double-counted.

The previous shape made every increment an atomic read-modify-write (`AtomicLongArray.getAndAdd`)
and every drain a take (`getAndSet(0)`), with a "0 return" recovery for a racing add that landed on
a taken slot. That is exact, but it puts a lock-prefixed instruction on the hottest path (`LOCK
XADD` on x86, `LDADDAL` on AArch64) and forces the writer to re-hash, re-probe and re-claim every
slot after each tide, because the drain clears the map wholesale. The single-writer shape removes
both: the hot path is a plain load plus a release store, and a slot survives a drain, so a
steady-state hot key keeps its slot and its last-key cache across tides.

**A claim's publication order is key → count → tag → bitmap bit → sequence.** The bitmap bit and
the sequence are release stores and the drain reads both with acquire, so a visible bit implies a
complete entry and a visible sequence implies every count it covers. The tag is thread-private —
only the owning writer probes it — so it needs no fence of its own; it exists to make the probe
cheap, not to publish. The old tag-first ordering (tag as a cross-thread publication word) was the
fence for a tag-driven recovery sweep that no longer exists: a drain that never clears a slot has
nothing to recover.

The shared-table reference is captured under `reservoirGate`, and an in-flight slot is reserved on
the captured reservoir atomically with the capture, so the deliverer waiting for that reservoir's
counter to reach zero can never snapshot a table that a captured merge still targets: no in-flight
hot add can be stranded, and a merge into the fresh table cannot extend the old table's settle-wait.

**The cold path is approximate.** A cold writer that captured the table reference just before the
tide swap may write into the old table after the snapshot. The 1 ms quiescence window reduces this
to a preemption of > 1 ms — measured on deliver-racing and slow-consumer stress: typical loss 0,
worst observed ≈2.1e-5/op. The window is skipped only on cycles with no shared-table writes, whose
old table is empty anyway. With the window paid, residual loss requires a preemption > 1 ms; a
miss-path writer preempted across the swap re-targets the new table via its `putIfAbsent` re-read.
Sustained hot keys are promoted and then take the exact path.

### Delivery

A self-rescheduling flusher — a one-shot tide that re-arms itself at a backlog-adaptive delay —
merges every writer's hot local map, swaps the shared table, waits for in-flight merges plus the
gated quiescence window, snapshots the old table into a single map and delivers it once per cycle.
The snapshot carries the measured swap-to-swap window, and the consumer receives it before the
promotion pass: O(n) promotion work cannot add to the delivery latency the consumer sees, and the
next delay is computed after delivery, so an apply-mode tuner's new base arms the very next tide.
Dead writers' registry entries are removed, and their residual local counts are merged first; a
writer that dies between that drain and the liveness re-check is drained once more before its map is
recycled, because the final batch may have landed after the drain released the map. The deliverer's
own `tryDrainInto` does not reserve a reservoir slot: the settle-wait runs after that drain in the
same thread, so the two contended atomics per live writer per tide bought no ordering the call graph
did not already provide. A recycled map whose lock is still held (the owner died between the lock
acquisition and the protected block) is discarded instead of blocking the deliverer on a lock that
can never be released.

**The running future is cleared before the tide re-arms, and the body is serialized with destroy.**
The one-shot future that is currently running stays `isDone() == false` until `run()` returns. If a
scheduler fires it early, re-arming without clearing would leave the field pointing at the running
task, so `tide()` clears `pendingTide` under `tideScheduleGate` before its body and re-arms in the
`finally`; `scheduleTide` is its only other writer. A `tideExecutionLock` (outer to
`reservoirGate`, the per-map `Ceils.lock` and the schedule gate) serializes the body against
`destroy()`'s final drain. `destroy()` cancels the pending future, shuts down an owned scheduler,
then takes that lock before its final merge/snapshot/cleanup, so the consumer is never invoked
concurrently and a tide that starts after shutdown returns without delivering. Because the lock is
the only context in which the reservoir reference is replaced, `destroy()` reads the table once for
its whole registry sweep and drains every writer into it — no per-writer capture/reserve, no gate
traffic, no in-flight churn on the teardown path. `scheduleTide` re-checks the shutdown flag inside
`tideScheduleGate`: a reschedule racing destroy could otherwise pass the outer check, wait for the
gate while destroy cancels the pending future, and arm a fresh one after the cancel.
`acquireLocal` re-checks shutdown after registering and removes its registry and ThreadLocal entries
if destroy already cleared them; a late registration would otherwise pin a dead writer thread and
its map.

### Memory

The shared table holds exactly the live key set (no per-batch duplication); each writer's hot local
map is bounded by `opMaxCount`, and the hot set by `hotLimit`. A first insert allocates one
`LongAdder`; a hit — the dominant case, including the dozens of writers draining the same hot key
after a swap — takes the existing cell with a plain `table.get` and only adds to it.

**The drained table is not recycled.** The previous shape published the previous cycle's table
behind a single `SoftReference` and reused its adders on the next cycle's first inserts. Every reuse
paid a `SoftReference` dereference, a `ConcurrentHashMap.remove` on the old table (a second hash, a
bin lock and an `addCount(-1)`), and a `reset()` of a `Cell[]` that may have inflated under
contention; and the whole previous table — one cycle's key universe — stayed strongly reachable
across the cycle, so it survived into the old generation and was scanned as a soft reference at
every STW. The saving was one ~32-byte adder per first insert. Fresh allocation is cheaper than the
O(new keys) contended removals plus the retained old cycle, and it removes the soft-reference
scan from the GC's critical path.

Per-writer `Ceils` maps follow a separate discipline: dead writers' drained maps are pooled for the
next writer (`ceilPool`, capped at `CEIL_POOL_CAP`), so per-request-thread deployments allocate a
local map at most once per pooled wave instead of once per request. The pool's size slot is reserved
before the offer (`incrementAndGet`, rolled back when over cap), so a concurrent `acquireLocal`
poll cannot drive the counter negative or admit a map past the cap. Memory therefore tracks the
concurrent hot-writing thread count (~9 KB per live thread that ever counted a hot key — the slots,
the occupancy bitmap and the drain baseline), not cumulative churn — a large but live writer pool
retains one map per thread until the thread dies and the tide reaps it.

The distinct-key counter (`approximateSize`, an `AtomicLong` in its own object) is deliberately
outside the `WaveCounter` header cache line: every first insert into the shared table performs an
atomic add, and keeping it adjacent to `shutdown`/`reservoir`/`capacityLimit` made every cold insert
invalidate the line the hot path reads. The deliverer's reused scan arrays are also lifecycle-managed
per tide: `allValues`/`allHashes` are the only views pre-sized to the distinct-key count, the
filtered index views (`candidateIdx`, `aboveKthIdx`) grow on demand — pre-sizing them to the whole
snapshot would commit a cardinality's worth of memory for lists that hold ~`hotLimit` entries — and
a shrink allocates a fresh array instead of copying a prefix that the next sweep overwrites. The
occupancy bitmap, not the plain `size` field, is the deliverer's pending-work truth (published with
release, read with acquire); the writer's sequence is the O(1) skip test in front of it.

**Capacity drops are counted, not logged per key.** The cold path's soft cap increments
`coldCapacityDrops` and returns; a per-drop DEBUG line made a high-cardinality flood pay log
formatting and output on the miss path. The cumulative total is exposed as
`WaveCounter.coldCapacityDrops()` — a growing value with stable traffic means `capacity` is
undersized — and stays separate from the reporter's dead-worker, unroutable, routing-queue and
dispatcher counters.

### What not to change

- The hot path's single-writer protocol is the correctness argument: the owner is the only writer
  of a slot's key and count, and the drain derives its residual from its private `drained[]`
  baseline. Letting a drain clear a slot (or take a count) re-opens the writer/teardown race the
  ownership split closes; making the add an atomic read-modify-write re-introduces a lock-prefixed
  instruction and a per-tide re-claim on the hottest path.
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

**The Worker's lifecycle phases follow the report path.** Spring starts `SmartLifecycle` beans
lowest phase first and stops them highest first, and components sharing a phase have no defined
order. The shipped ladder is `DecisionDispatcher` (`MAX_VALUE - 20`) → `CoolingScheduler`
(`MAX_VALUE - 10`) → report listener container (`MAX_VALUE - 5`) → `WorkerScheduling`
(`DEFAULT_PHASE`, `MAX_VALUE`). Starting therefore readies the final drain, the cool-down wheels and
the report consumer before the heartbeat runs; stopping reverses it, so `WorkerScheduling` cancels
the heartbeat before the consumer drains in-flight reports, and the consumer stops before the last
COOL or the final drain. A default-phase consumer and `WorkerScheduling` in the same phase made that
order a race in both directions: on shutdown the heartbeat kept advertising `ready = true` while
the listener container was draining, delaying the App's view of the Worker leaving the ring, and on
startup a `ready-grace-ms = 0` heartbeat could declare readiness before the consumer had begun. The
report listener adopts `MAX_VALUE - 5` through its container factory, and Spring AMQP's
`RabbitListenerEndpointRegistry` takes the same phase when it holds one custom-phase container
factory. Owner: `WorkerAutoConfiguration.reportListenerContainerFactory`,
`WorkerAutoConfiguration.WorkerScheduling`, `CoolingScheduler.getPhase`,
`DecisionDispatcher.getPhase`.

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
transfers the **first** seat it reclaims to its own admission before a lock-free contender can see
it; every later reclaimed seat is handed back. The predicate
must therefore return true only when this pass tombstoned the candidate and returned its seat — to
the ledger below the hard bound, or to its own admission at the hard bound. The
sweep hand is weakly consistent and can still yield an entry another path already removed; treating
that ghost as a reclaim would inflate `capacity.evictions` and spend the hard-headroom budget on a
seat this pass did not free. The hand
resumes where the previous pass stopped and is bounded by `min(sampleSize, map.size())`; an
exhausted hand is rebuilt at the head. A CHM iterator pins the table array it was created over, so
the periodic sweep drops the hand with a volatile clear rather than a `tryLock`: the sweep runs
exactly when admissions contend the sample lock, and a contended drop left the old array pinned
for the whole sweep interval, the one case the release exists for. The sampler reads the field
into a local once per pass, not once per step: a clear between two field reads would restart the
walk at the head and could skip the tail, and a dereference of the cleared field would NPE. The
clear therefore never touches a pass in progress. A contended admission (`tryLock` fails)
proceeds unless the ledger is at the hard limit: the holder may be releasing the seat it needs,
and "no pass ran" is not evidence that nothing is idle. A reservation that reaches the lock after
another pass opened the saturation window skips the walk and admits: the window is the newest
evidence, and repeating the scan only burns the sampler's hold. The detector's `LongAdder` rejection counter spreads flood-time writes across
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
hard limit, so the table cannot grow past it under any interleaving. Below the hard bound the pass's
own admission is covered by one reservation taken before the walk: a bypassing admission cannot take
that seat, and every seat the pass reclaims is returned to the ledger. At the hard bound no
reservation exists, so the pass keeps the first reclaimed seat for its own admission and returns
only the later ones. A key that already holds a HOT
registry seat also takes a seat without the bound — a hot window's count feeds the LOW_QPS
cool-down, and losing it would make a live hot key look quiet; the overshoot is bounded by
`max-active-hot-keys`. The ledger initializes from a prefilled table for test/embedding callers.
Owner: `SamplingReclaimer`, `SlidingWindowDetector`.

**The failure this closes (the sampler refused after paying for the walk).** The pass used to sample
first and call `reserve` only after releasing the lock. While it walked, contenders that lost
`tryLock` reserved the remaining hard headroom; if the walk found nothing idle, the sampler's own
reservation then failed at the full ledger. The one thread that had just scanned the table was the
one admission refused, which made the cap look arbitrary under contention. Reserving the seat
before the walk closes that window below the hard bound; each reclaimed seat is returned, so the
reservation is the only seat the sampler can consume. At the hard bound no reservation exists, so
the sampler keeps the first reclaimed seat instead of returning it to the ledger. Owner:
`SamplingReclaimer.tryAdmit`.

**The fast path reserves below the cap, never from the headroom.** The below-cap admission used the
same hard-limit CAS as the sampling path. A burst that all observed the ledger one seat below the
soft cap therefore spent the entire hard headroom without running one pass; the next arrival was
rejected from a table that could still hold reclaimable idle windows. The fast path now CASes only
below the cap: an arrival that loses the last below-cap seat falls through to the sampling gate, or
to its saturation window, so headroom is spent only by an admission that reaches the sampling path —
it walks the table, or loses the `tryLock` to the pass already walking it.
The sampling admission still reserves its hard-headroom seat before the walk. Owner:
`SamplingReclaimer.tryAdmit`, `SamplingReclaimer.tryReserveIfBelowCap`.

**The tombstone is the seat state, and the seat crosses the compute as a primitive.** A
`KeyWindow` owns its seat for exactly the interval in which `lastSlice != EVICTED`; eviction sets the
tombstone and returns the seat in the same entry-monitor section, and a second pass that finds the
tombstone cannot release it again. A report that sampled the old tombstone reference therefore
creates its replacement through the same admission gate as a first touch: the old seat is already
back in the ledger, and a direct hard-headroom reservation would bypass the cap sample and leave
reclaimable idle windows tracked. No separate `seatHeld` flag is needed: a boolean after the
24-byte `header + int[] + long` layout would round the entry up to 32 bytes, costing 8 MB at the
default one-million tracked-key cap. The admission verdict is likewise a `boolean`: the reclaimer
reserves the seat before the publication, and `publishOrGetExisting` consumes it when it installs a window
and calls `releaseSeat()` when a concurrent admission already published a live one. That removes the
two short-lived objects (the reservation and its `AtomicBoolean`) that the admission path used to
allocate per new key while keeping the seat ownership explicit in the call's stack frame. A
publication that throws after the reservation — an OOM in the new window or the map node — returns
the seat in the same call before rethrowing; without that, every failed admission permanently
shrinks the ledger and the cap starts rejecting while the table is still below it. The admission
gate reads only the ledger; the map size is read once per sampling pass, where the walk needs its
bound. Owner: `SlidingWindowDetector`, `SamplingReclaimer`.

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
pass transfers the first seat it reclaims to its own admission before any lock-free reservation can
observe it, so a contender cannot take the seat the walker paid for; later reclaimed seats are
returned to the ledger. The 50 ms saturation window keeps a pass that found nothing from repeating.
Owner: `SamplingReclaimer.tryAdmit`.

**The failure this closes (hard-limit lockout).** `tryAdmit` used to reject on
`usedSeats >= hardLimit` before touching the sample lock, on the premise that the periodic sweep was
the only recovery path there. With the default 30 s sweep cadence, a table at the hard bound but
holding windows that had just gone idle refused every new key for up to half a minute, even though
one bounded walk would have released a seat. The hard bound is still a true bound — the fallback
never reserves above it, and a failed `tryLock` rejects — but an admission is no longer blind to
reclaimable seats. Owner: `SamplingReclaimer.tryAdmit`.

**The repair re-links only a lost check; the wheel is the only evaluator.** A check still linked in
a wheel is late, not lost: the wheel will detach it when it catches up. An `UNSCHEDULED` node is
different — no wheel owner holds it, because a failed evaluation cleared the `DETACHED` hand-off
and left the deadline in the past — so the sweep re-links it on the next pass. The sweep then calls
`reschedule`, which links a lost node into the due list and wakes the wheel owner; the owner applies
the real verdict and the real cool reason. A node the wheel has already handed to its owner is
`DETACHED`: the scan leaves it alone and does not count it as repaired, because the owner's
evaluation is about to re-link the fresh deadline. Re-linking rather than evaluating keeps one
answer to "who decides a cool-down": a second evaluator could move `nextCheckAt` twice and report
the wrong cause. A wheel owner that dies leaves its buckets without an advancing thread; this pass
cannot replace the owner, and the App's hot hard TTL bounds the keys it held. Owner:
`HotKeyRegistry.repair`, `TimerWheel.reschedule`.

**The failure the linked-node repair closed (sweep amplifying a lagging wheel).** The scan used to
re-link every `SCHEDULED` node whose deadline was more than a second past `now`, on the premise
that a late node was as good as lost. Under a GC pause or a large wheel batch, `nextCheckAt` is
past `now` while `wheel.clockMs` has not reached it yet: the node is still in a future bucket and
the wheel is already catching up. The sweep then took every entry's stripe lock and the wheel lock
to unlink and re-insert it into the _same_ bucket, contending with the very thread making progress
and stretching the pause. The predicate now asks the only question the registry can answer without
the wheel clock: does this entry have an owner at all (`wheelState == UNSCHEDULED`)? The scan also
signals every wheel once, so a missed wake from the lagging batch is recovered without moving any
still-owned node. Owner: `HotKeyRegistry.needsRepair`, `CoolingWheels.signalAll`.

**A cool-down deadline saturates instead of wrapping.** `HotKeyRule` and its published
`RuleBinding` compute every deadline with a saturating add, so a very large `cool-after` or
`low-qps-for` stays at `Long.MAX_VALUE` instead of wrapping negative and firing immediately. The
repair scan reads only the entry's wheel state — it never subtracts the deadline to decide whether
to repair — so a saturated deadline stays future. The endpoint keeps the saturation instead of
subtracting the monotonic sample. Owner: `HotKeyRule`, `RuleBinding`, `HotKeyEndpoint.keyState`.

**A due check is evaluated once, under the stripe lock.** The wheel detaches an entry and calls
`evaluate`; between the detach and the stripe lock, a report can re-arm the same entry. `evaluate`
re-reads `nextCheckAt` after the identity check and re-links the fresh deadline when it is still in
the future. The entry is `UNSCHEDULED` at that point, so the call is a schedule, not a second bucket
move. The lateness sample is taken under the same stripe lock, from the deadline `evaluate` is about
to replace, so a concurrent re-arm cannot move the entry between the check and the metric. Owner:
`HotKeyRegistry.evaluate`.

**A re-arm publishes both entry fields only when the wheel accepts the node.** `evaluate` computes
the new deadline and the dwell start before mutating the entry, and `rearm` snapshots both fields
and restores them if `TimerWheel.reschedule` throws. Without that, a failed move left the node
unscheduled with `nextCheckAt` already moved into the future, so the repair predicate saw a healthy
long timer and never re-linked it: the key stayed HOT until its App hard TTL, and a `QPS_RECOVERED`
window left a stale `belowSince` behind. The failed node now remains overdue, which is exactly what
the repair pass looks for. Owner: `HotKeyRegistry.evaluate`, `HotKeyRegistry.rearmNextProbe`.

**A promoted entry is scheduled before it is visible.** `promoteLocked` links the entry into its
wheel before `hotKeys.put`, so a repair sweep that iterates the map lock-free can never observe a
promoted node with no owner. The stripe lock already serializes the two writers, so the order only
removes the window from the scan's pre-check; a promotion that throws after the link is retired and
unlinked by the same catch. Owner: `HotKeyRegistry.promoteLocked`.

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
per-entry `Throwable` is caught at the evaluation and submission boundaries, so one poisoned check
cannot skip the rest of the batch or kill the wheel owner; a failed evaluation stays unscheduled
for the repair pass, while a failed submission loses only that COOL. The separate catches keep the
log truthful: a COOL that was already cooled and released before its submission failed is not
"overdue for repair", and the entry is retired. An `InterruptedException` observed while `running`
is logged and the loop continues: the only legitimate interrupt is shutdown, which clears `running`
first, so treating every interrupt as terminal let a stray interrupt permanently kill one wheel's
owner and leave every key on it unscheduled until the App's hard TTL. The shutdown joins share one
deadline, so the teardown wait is one second rather than one second per wheel. The constructor also
tears down already-started owners if a later thread cannot start, because a constructor failure
never reaches `@PreDestroy`. Owner: `CoolingScheduler`.

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
failed attempt leaves the anchor unset, so the next successful beat starts the clock. The anchor is
written with one CAS from the unset sentinel, so two concurrent senders cannot replace the first
accepted instant with a later sample and stretch the grace period. The repair
sweep keeps a fixed delay because its run time is work the next cadence should not absorb. Owner: `WorkerHeartbeatProducer`,
`WorkerHeartbeatMessage`, `WorkerAutoConfiguration.WorkerScheduling`.

**A heartbeat is tenant-scoped at the receiver.** The topic exchange and the `heartbeat.*`
binding are global, so every App on a shared broker would receive every Worker's heartbeat and
put a foreign Worker into its ring. The heartbeat therefore carries the sender's `zeta.app-name`
alongside epoch and readiness; the App's heartbeat listener applies the same `AppIsolationFilter`
as the sync and decision planes before recording it. A heartbeat without the header (a
pre-isolation Worker during a rolling upgrade) is still processed. Owner:
`WorkerHeartbeatProducer`, `ZetaAmqpAutoConfiguration.WorkerListenerConfiguration`.

**A heartbeat's text headers use the shared string reader.** RabbitMQ may deliver a string header
as a `LongString`; an `instanceof String` check on the node id fell back to the body, which may be
empty, and dropped a live Worker. The type discriminator has the same failure mode: `TYPE.equals`
against a `LongString` is false, so every heartbeat is dropped before the health update and the
ghost-COOL guard sees every Worker as dead. `WorkerHeartbeatMessage.from` reads both the node id
and `HEADER_TYPE` through `AmqpMessageReader.readStringHeader`, the same text conversion the sync
and decision planes use, and still falls back to the body when the node id is missing or blank.
The record also rejects a null worker id at construction, so `toMessage()` cannot NPE on a
partially built heartbeat. Owner: `WorkerHeartbeatMessage.from`.

**Health is one third of observed Workers.** Healthy when at least `ceil(observedWorkers/3)`
are alive, floored at 1. `observedWorkers` is every Worker id ever seen in a heartbeat record:
records are added on the first heartbeat and replaced on a newer epoch. `HealthView.removeRecord`
exists for an external caller that has confirmed a Worker dead, but no production path calls it, so
the denominator never shrinks on its own and a transient blip cannot collapse the threshold.
`zeta.local.heartbeat.min-alive-workers` overrides it with an absolute count. A single surviving
Worker is deliberately healthy: report traffic is compressed and batched, so one Worker can serve
the cluster, and degrading on every partial failure would be worse. The override is folded into
the one `ClusterHealth` factory behind the shared bean, and every consumer injects that bean
rather than building a private view: a locally built view would miss heartbeat updates and
silently disagree with the rest of the process. Owner:
`ZetaFacadeAutoConfiguration.defaultHealthView`, `ZetaFacadeAutoConfiguration.clusterHealthView`.

**A non-positive heartbeat window is a configuration error, not an always-dead cluster.**
`ClusterHealth` rejects `heartbeatTimeoutMs <= 0` and a negative health-cache TTL at construction:
a zero window makes `now - lastHeartbeat < timeout` false for every record, so every Worker is
declared dead the instant it arrives and the cluster silently degrades forever. The endpoint has no
synthetic zero-timeout view: without a wired health view it reports the current ring snapshot and
does not rebuild it, so an empty liveness set can never empty a live ring. Owner: `ClusterHealth`,
`RingEndpoint`.

**The heartbeat fast path allocates, logs and invalidates nothing.** A same-epoch heartbeat
mutates the existing record in place, so no record or cache entry is created, and the
cluster-health cache is invalidated only when the Worker's liveness verdict actually flips.

**An outage-length gap never feeds the adaptive window.** Only an inter-arrival interval shorter
than the static `heartbeatTimeoutMs` updates the Jacobson/Karels estimate. A longer gap means the
Worker was already considered dead by the static window; using it as a latency sample would pin the
adaptive timeout at the ceiling and keep a healthy recovery marked dead (RFC 6298 isolates exactly
these samples). The static ceiling is passed to the record for the filter, not the tightened
effective timeout, so ordinary jitter can never exclude a legitimate sample. Owner:
`ClusterHealth.WorkerHealthRecord.onHeartbeatSample`.
The cached verdict is one packed `long`: bit 0 is the verdict, bits 1..42 the biased
computation instant, bits 43..63 the invalidation generation. Invalidation advances the
generation and clears the low bits, so a publish racing it cannot match the word it read;
the rare invalidation path allocates nothing. A join or re-join is logged after the record
lock is released, while the joined/re-joined decision is taken under it, which keeps the
report exactly-once under racing heartbeats. Owner: `ClusterHealth`.

**The heartbeat clock sample is taken under the record lock.** Two heartbeats can arrive out of
scheduler order; sampling `monotonicMillis` before acquiring `recordLock` let the older sample
win the lock second and move `lastAnyHeartbeatTime` backwards, and a same-epoch record could be
overwritten with an older `ready`/arrival pair. The sample is therefore the first statement inside
the monitor, so the record mutation and the global watermark share one ordered instant. Owner:
`ClusterHealth.onHeartbeat`.

**One record word carries readiness and evidence.** A per-Worker record packs `ready` into
the sign bit of one volatile `long` and the last alive-evidence monotonic instant into the
magnitude: non-negative means ready and holds the timestamp, negative means not ready and
holds `~timestamp`. One volatile read in `isAlive` settles both, so a reader can never pair
a stale flag with a newer timestamp, and the former separate arrival timestamp is gone —
the alive-evidence instant is the arrival instant. Owner:
`ClusterHealth.WorkerHealthRecord`.

**Liveness windows adapt per Worker (Jacobson/Karels).** The effective timeout for a Worker is
the SRTT + RTTVAR estimate over its heartbeat inter-arrival — the Linux `tcp_rtt_estimator`
RTO shape — clamped into `[adaptive-floor-ms, timeout-ms]`. RTTVAR is stored **4× scaled**
(`rttVar4xMs`, like the kernel's `mdev_us`), so the estimate adds it once and the `|err|` term
enters with RFC 6298's β = 1/4; storing it unscaled while also multiplying by 4 in the estimate
made the window `SRTT + 16·E|err|`, so a few milliseconds of jitter pushed it to the static
ceiling and the adaptation bought nothing. The static `timeout-ms` demotes from a flat threshold
to the warm-up window (first 8 samples) and the ceiling; the RTTVAR floor is 4 × 200 ms and the
shrink branch decays with 1/8 gain (the Eifel refinement) so one fast beat does not deflate the
window. Only heartbeat arrivals feed the estimator. Net effect: dead-Worker detection tightens
from the full static window to a few heartbeat intervals while jittery Workers get proportionally
wider windows instead of flapping. The estimator lives in the record itself; its published word is
negative during warm-up (a countdown from `-8` to `-1`) and non-negative once the derived timeout
is trusted, so one volatile read answers both "is it warmed up" and "what is it". Every consumer
of liveness (ring routing, health gate, incarnation checks) shares the same per-record
judgment. Owner: `ClusterHealth.WorkerHealthRecord`.

**The Worker's configuration is an auto-configuration, not a scanned `@Configuration`.** It lives
under the Worker application's scan base package, so a plain `@Configuration` was picked up by
component scanning before `@EnableAutoConfiguration`'s deferred imports registered the common
module. Its `@ConditionalOnMissingBean` factories were therefore decided against an incomplete bean
set — the fallback report converter could win over the common converter, and any future overlapping
bean would fail with a definition-override error. The class is now `@AutoConfiguration` ordered after
`ZetaAmqpAutoConfiguration` and is listed in the Worker's `AutoConfiguration.imports`; Spring Boot's
auto-configuration exclude filter keeps component scanning from registering it. Owner:
`WorkerAutoConfiguration`, `WorkerApplication`.

**The Worker's identity is explicit, not ambient.** `zeta.worker.node-id` is resolved before
`INSTANCE_ID`, `zeta.local.instance-id` and the auto-generated fallback, and it names both the
report queue (`zeta.reportToWorker.<app-name>.<node-id>`) and the default epoch file. Relying on
`INSTANCE_ID` alone left the Worker with a random id whenever the deployment did not inject it:
every restart changed the epoch-file name (so the persisted epoch was lost and Apps treated the
process as a fresh incarnation) and created another 7-day `x-expires` report queue, accumulating
orphans. The random fallback still exists for a local run, but it logs a WARN naming the three
stable sources. Owner: `WorkerProperties.nodeId`, `WorkerAutoConfiguration.workerIdentity`.

**The planes are isolated.** Heartbeats travel on a TopicExchange with ack NONE and a prefetched
queue; decisions use MANUAL ack with a small prefetch. Different loss semantics must not share a
queue. The control-plane exchange is declared from the publishing template before that
connection is handed to a publisher, because Boot's `RabbitAdmin` declares lazily and the first
publish would otherwise win the race on a cold broker. Do not register a second `RabbitAdmin`
bean: Boot's is `@ConditionalOnMissingBean` on the `AmqpAdmin` return type, so a second admin
silently switches it off and leaves every exchange, queue and binding undeclared. Thread pools are
isolated for the same reason: heartbeat emission runs on a dedicated single-thread
`workerControlScheduler`, while `hotKeyScheduler` keeps the periodic sweeps, whose walks over the
window table and the HOT registry can occupy a thread for tens of milliseconds.

**The heartbeat topology is redeclared on every control-plane connect.** The queue is
non-durable and auto-delete, so RabbitMQ removes it when the last consumer — the control-plane
consumer — disconnects. The data-plane `RabbitAdmin` only runs its declarations on a data-plane
connection event; a control-plane-only network blip leaves the data-plane connection alive, so it
never repairs the queue and the reconnected consumer loops on `404 NOT_FOUND`. A dedicated
connection listener, created with the heartbeat container, declares the exchange, queue and
binding on each new control-plane connection. It is not an `AmqpAdmin` bean, so Boot's
data-plane admin stays enabled. Owner: `ZetaAmqpAutoConfiguration.declareOnControlPlane`.

**The control-plane factory inherits the data plane's cluster addresses.** Both factories are
built from the same `RabbitProperties`, but the wrapper's address list and shuffle mode belong to
Boot's `CachingConnectionFactoryConfigurer`, not to the underlying driver factory that
`RabbitConnectionFactoryBeanConfigurer` configures. Building the wrapper by hand therefore kept
only the first host and lost `spring.rabbitmq.addresses` failover. The heartbeat factory applies
the Boot configurer when present (and mirrors the addresses and shuffle mode from the properties
when it is not), plus every `ConnectionFactoryCustomizer`, so a node failover behaves identically
on the control plane. Owner: `ZetaAmqpAutoConfiguration.heartbeatConnectionFactory`.

**The control-plane connection is a holder, not a primary `ConnectionFactory`.** The heartbeat
`CachingConnectionFactory` is wrapped in `ControlPlaneConnectionFactory`, a type that is
deliberately not a `ConnectionFactory`. A `@Primary` `ConnectionFactory` captured every unqualified
host injection — Boot's `RabbitTemplate`, `RabbitAdmin`, `@RabbitListener` container factories and
any application bean — and routed the host's business MQ traffic and queue declarations over the
heartbeat connection. A business channel stall or broker flow control then blocked the heartbeat
too, destroying the physical isolation the two factories exist for. The holder cannot join those
injection points or Boot's `@ConditionalOnSingleCandidate` decision; Zeta's heartbeat template and
listener container receive it by qualifier and unwrap `connectionFactory()`, and the holder
delegates `InitializingBean`/`DisposableBean` so the wrapped factory is still initialized and
destroyed with the context. Owner: `ZetaAmqpAutoConfiguration.ControlPlaneConnectionFactory`.

**A data-plane configuration is gated by a data-plane factory, not by any `ConnectionFactory`.**
The control-plane holder is not a `ConnectionFactory`, so a plain
`@ConditionalOnBean(ConnectionFactory.class)` no longer matches it; the condition and the resolver
still share one name lookup that excludes the reserved `zetaHeartbeatConnectionFactory` name, which
keeps a legacy or user-supplied factory under that name out of the data plane. Owner:
`ZetaAmqpAutoConfiguration`.

**The Worker's decision broadcaster owns a data-plane template.** `WorkerAutoConfiguration` defines
`zetaWorkerBroadcastRabbitTemplate` over `ZetaAmqpAutoConfiguration.dataPlaneConnectionFactory` and
injects it by qualifier; `ReportConfiguration`, `SyncConfiguration` and `WorkerListenerConfiguration`
are disabled whenever `zeta.worker.enabled=true`, so a Worker never builds an unfed `KeyReporter`
or a context that requires the app-side L1 table, and report/sync templates cannot collide with the
broadcast template. The worker-mode guard is part of every app-plane nested configuration, not
only the report one: a shared config that leaves sync or the worker listener enabled must not fail
a Worker's startup with a missing `Cache`/`EntryLifecycle` bean. The previous shape injected `RabbitTemplate` unqualified. When a second Zeta
template existed (report enabled, or sync enabled) the context failed at startup with
`NoUniqueBeanDefinitionException`; when only the heartbeat template existed, HOT/COOL broadcasts
shared the control-plane connection, and broker flow control on a burst could block the heartbeat
channel and make the App evict a healthy Worker from the ring. The control-plane factory is a
holder rather than a type candidate, so Boot's own template and `RabbitAdmin` keep the data-plane
factory. Owner: `WorkerAutoConfiguration`, `ZetaAmqpAutoConfiguration`.

**Seat conservation and entry identity are the registry's two map invariants.**
`permits.availablePermits() + hotKeys.size() == capacity` at every quiescent point: a promotion
acquires a permit before it inserts, and only the call whose two-argument
`hotKeys.remove(key, entry)` returns `true` releases the permit. The two-argument remove is the
single commit point; a one-argument remove would let a swept check and a fired check — both holding
the entry reference — each release the same seat, inflating available permits and admitting more
than `max-active-hot-keys`. The wheel schedules the entry object itself, and every removal retires
it with the `HE.RETIRED` tombstone under the stripe lock, so an evaluation reads the entry's own
tombstone instead of hashing the key for a second map lookup. A cooled-and-re-promoted key has a
different entry, so a stale wheel event is discarded without a token and without a second release;
without the tombstone it could cool the successor (a ghost COOL) or return a seat twice. The wheel
node is descheduled after the permit is released, so an exception in a later step cannot leak the
seat. `abort` applies the same identity rule through a lock-free `isCurrent` read: versions are
monotonic, an entry's version is final, and a version's insert is visible before any abort of that
version, so a version that is not current can never become current again. Most delivery-path aborts
are no-ops for a superseded or already-cooled key, and those no longer take the stripe lock. Owner:
`HotKeyRegistry.promoteLocked`, `HotKeyRegistry.removeAndRelease`,
`HotKeyRegistry.markTerminal`, `HE.retire`.

---

## Worker detection (standalone server)

**The detector aggregates a sliding window, not a fixed-window counter.** A fixed-window counter
resets on each boundary, so a burst that straddles two windows is split between them and can stay
below a threshold in both halves even though it is one continuous hot episode.
`SlidingWindowDetector` sums the `windowSize` slices ending at the slice a write just touched (plus
one guard slice), so every sample sees the same trailing interval and the same traffic crosses the
threshold regardless of the millisecond it is sampled. The failure this prevents is a genuinely hot
key that is never promoted because its burst happened to straddle a reset boundary, leaving its load
uncached. Owner: `SlidingWindowDetector`.

**The promotion floor gates only the rules it can gate.** `RuleSet` derives `minHotCount` over the
default rule and every non-`EXACT` rule in its constructor, together with the precedence sort and
the fingerprint, so the three derived facts cannot disagree. Effective `EXACT` rules are indexed by
key in the published snapshot and consulted before the floor; a shadowed exact rule — one whose key
is already matched by a higher-priority rule — is omitted from the index because `resolve` would
never return it. The index scans the published set's own rule instances to decide effectiveness,
then reads the binding that was just built for the winning id; a rebuilt binding therefore holds the
same rule instance the scan tested. Without the split, one 10-QPS exact rule lowered
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

**A report below every rule's threshold is rejected by one comparison.** The snapshot precomputes
`globalFloor` — the lowest `hotCount` of the rules resolution can actually reach, which is
`minHotCount` floored again by the indexed `EXACT` rules — and rejects a below-floor report before
the exact index and before the match cache. The floor is only a shortcut: an exact rule whose
threshold sits below `minHotCount` still promotes its own key at its own value, and every other key
still meets `minHotCount` before a binding is resolved. The failure this prevents is cost, not
correctness: in a long-tail distribution the bulk of reports belong to keys nowhere near promotion,
and each of them used to pay an `exactRules` lookup (an empty-map dispatch when no exact rule is
configured), a `ruleSet` dereference for the floor and a binding threshold test before being
rejected. The report path resolves the key against three maps — `hotKeys`, `windows` and
`exactRules` — and a below-floor report now needs only the first two. With the floor first it pays
one `long` comparison and leaves, and a deployment with no `EXACT` rule configured holds the index as
`null` instead of an empty map, so the branch costs no dispatch at all. Owner:
`RuleResolver.Snapshot`.

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

**A duration that overflows milliseconds is a configuration error, not an exception.**
`Duration.toMillis()` throws `ArithmeticException` for a seconds value near `Long.MAX_VALUE`. The
validator converts inside its error sink, so the operator sees the property name alongside every
other violation instead of a bare arithmetic stack trace escaping the "collect all errors, then
fail" contract. Owner: `DetectionSettings.Validator.millis`.

**A null configuration group joins the startup error list.** An empty YAML block
(`zeta.worker.detection:` or `zeta.worker.detection.window:`) binds the group to `null`. The
validator checks `detection` and `detection.window` before dereferencing them, so the operator gets
`detection.window must not be null` alongside every other collected violation instead of a bare
`NullPointerException` from `resolveGeometry` that hides the rest. A null `detection` has no
sibling block left to report and fails with the same single-error message shape. Owner:
`DetectionSettings.Validator.validate`.

**A saturated cool-down deadline stays saturated in the endpoint.** `HotKeyRule` saturates a
deadline at `Long.MAX_VALUE`. The diagnostic subtracts the monotonic sample, and that sample may be
negative (the tests shift the axis, and zero/negative instants are real); `Long.MAX_VALUE - now`
would then wrap negative and report a live check as long overdue. The endpoint keeps the saturated
value instead of subtracting. Owner: `HotKeyEndpoint.keyState`.

**The per-rule totals are `LongAdder`s; Micrometer meters are a reporting side channel.**
`RuleResolver.Snapshot.eligible` records one match for every report that clears a rule's threshold,
and eight report-consumer threads do that concurrently. When the HOT seats are full, every candidate
key's report lands on the same default rule, so a single shared `volatile long` made all of them
contend on one cache line. Each of `matches`, `promotions` and `cools` is a `LongAdder`: it spreads
those writes, and the endpoint and the `RuleBinding` read the cumulative `sum()`. The same increments
also feed the three Micrometer `Counter`s so a scrape still sees the series, but the meter is never
the total. Owner: `RuleCounters`, `WorkerDetectionMetrics`.

**The failure this closes (a step registry reset the endpoint to zero).** `matches`, `promotions`
and `cools` used to read `Counter.count()`. On a step registry (OTLP, Datadog, StatsD, Influx,
CloudWatch) that value is the increment of the last step window, not a cumulative total, and on a
meter-less construction — the empty `CompositeMeterRegistry` fallback — every `increment()` is a
no-op and the value is always zero. The endpoint then reported `matches/promotions/cools` as zero
while `aborts` (already a `LongAdder`) was correct, and the seat invariant
`promotions == cools + aborts + active` could not hold. The adders make the endpoint independent of
the registry backend; the meters remain for dashboards that want the series. Owner:
`RuleCounters.matches`, `RuleCounters.promotions`, `RuleCounters.cools`, `HotKeyEndpoint`.

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
rule. No caller keeps a private `Math.round`, integer division or `instanceof` chain. A snapshot
whose scan array is empty — the default rule alone, or a default plus only `EXACT` rules — allocates
no match cache: every key resolves to the same default binding, so the cache would only add a hash
probe to the hot path and retain up to `4 * maxActiveHotKeys` key strings to answer a constant.
Owner: `RuleSet`, `RuleResolver`,
`WindowGeometry`, `HotKeyRule`, `RuleBinding`, `HotKeyEndpoint`.

**A promotion captures a rule binding; a publication rebuilds bindings and preserves counters.**
The resolver pairs each published rule with a fresh `RuleBinding`: the immutable rule and the
`RuleCounters` of its id. The entry stores that binding, so the report path and the cooling check
read the retained rule's thresholds by field access instead of resolving the rule through the
published set, and a promotion, an abort and a cool-down advance the counters through the captured
handles — no second lookup through the rule set, the active-id set and the metrics maps, and no
per-call lambda. Counters are keyed by id and survive a publication that retains the id, so a
threshold edit to one rule keeps every surviving id's statistics. Rebuilding every binding is what
makes `binding.rule()` the rule instance in the published snapshot even when a recompiled regex or
a reordered rule has an unchanged definition text; the binding is 24 bytes and publication is cold.
An identical fingerprint **and an equal rule set** short-circuit the publication,
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
delayed event mutate a successor. `HE` is also not bit-packed: `belowSince` and `nextCheckAt` each
need the whole `long` domain because both carry sentinels outside any real monotonic instant, and
the endpoint and the repair scan read them lock-free, so packing would trade a few bytes per seat
for mask-and-sentinel decoding on those readers. The hot-path win comes from retaining the rule in `RuleBinding` —
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
only the absolute slice number, so re-entering the map rehashes the key and takes the bin lock once
per slice for an entry that is already present. The entry's monitor serializes that update
with eviction: the evictor re-checks the idle bound under the same monitor, writes the
`lastSlice = Long.MIN_VALUE` tombstone, releases the monitor, and only then removes the mapping with
an identity-matched `windows.remove`. Only the call that wrote the tombstone reports the removal, so
a concurrent periodic sweep that tombstoned the same window first cannot make a second caller
release the seat. An add that sampled the old reference sees the tombstone and re-creates through
the admission gate; an add that refreshed the slice first makes the evictor keep the window.
Creation takes no bin lock beyond the atomic install: `putIfAbsent` publishes the fresh ring on an
empty bin's lock-free CAS path, the slice advance for an existing window runs outside the map
entirely, and a tombstoned mapping is swapped with `replace`, so the nesting order is entry monitor
→ bin lock and creation holds no monitor at all. Owner: `SlidingWindowDetector`.

**The failure this closes (a first touch under a per-key bin lock).** Creation used to run inside
`windows.compute`. On an empty bin that made the JDK allocate a `ReservationNode` and take its
monitor, because `compute` cannot fall back to a lock-free insert; it also allocated a capturing
lambda per call, cleared the new ring while holding the bin lock, and the existing-window branch took
the entry monitor inside that bin lock. Under a high-cardinality burst — first touches are exactly
what a cold-key flood is — every one of those first touches paid two objects, one lock and a nested
monitor for a mapping only it was going to touch. `putIfAbsent` plus `replace` keeps the atomicity
that matters (exactly one window is installed, and the losers refund their seat) and removes the
nesting; the only cost is that two admissions racing the same brand-new key may both allocate a
ring, and the loser's is dropped immediately. Owner: `SlidingWindowDetector.publishOrGetExisting`.

**A fresh window is published with its first count already in its slot.** `KeyWindow` takes the
ring slot and the initial count in its constructor and writes the packed word before
`putIfAbsent`/`replace` publishes the window. The map's publication orders that write before any
reader or adder that observes the window, so the fresh install needs no post-publication
`compareAndExchange`, and a concurrent adder finds the count already in place and accumulates on
it. The admitting call returns the `delta` it installed: the add is linearizable at the
publication, and a concurrent batch added afterwards may be included in another reader's sum
without making this call's answer wrong. The previous shape published an all-zero ring, then
called the CAS-based `addToSlot` for the slot its own thread had just created, and walked
`windowSize + 1` slots to re-derive a sum that was exactly the count it had just written. The
constructor call now removes that CAS, the 17-slot walk and the two post-publication `lastSlice`
reads from every new-key install.

| per fresh install (default 16 slices)             | before | after |
| ------------------------------------------------- | ------ | ----- |
| first-slot `compareAndExchange`                   | 1      | 0     |
| post-publication `lastSlice` volatile reads       | 2      | 0     |
| opaque slot loads (`addToSlot` 1 + `sumRange` 17) | 18     | 0     |

The shape was checked with a Python differential model across `windowSize` 1, 2, 16 and 64, 20,000
random steps per size (negative, zero and positive slices, late writes, `Integer.MAX_VALUE`
saturation and eviction). The returned sum matched the previous implementation in every step, and
all 12,451 fresh window installs skipped the post-publication CAS and the ring walk. A resident
same-slice add reads
`lastSlice` once for the gate and once for the sum instead of three times, because the
monitor-advance path returns the slice it just wrote. Owner:
`SlidingWindowDetector.KeyWindow`, `SlidingWindowDetector.publishOrGetExisting`.

**A slice number of zero is a real coordinate.** The window is created with
`lastSlice = now / sliceMs`, so zero — a test clock or a negative `nanoTime` origin — is a previous
write, not "never written". Only the `Long.MIN_VALUE` tombstone is special: no timestamp this
process can observe divides down to it, so it is distinguishable from every real slice. A negative
slice is a real coordinate too, so every comparison is between two absolute slice numbers, not a
truncated elapsed-time quotient: slices `-2` and `0` are two slices apart. Owner:
`SlidingWindowDetector`.

**The slice-gated fast path reads `lastSlice` once and accepts one bounded loss.** The fast path
reads `lastSlice` once to choose between the fast add and the monitor refresh, then writes its own
slice. An eviction that tombstones the entry between that read and the slot CAS loses this batch.
The evictor can only target a window its later clock already sees as idle: the default retention is
minutes and a positive value is validated at or above the window, while the capacity sample uses the
window itself. The lost batch is therefore outside every readable range and would not change a sum
even if it were re-admitted; the next report recreates the window through the admission gate. The
previous shape re-read `lastSlice` after the add gate and rebuilt an observed tombstone; that
recovered only the same outside-window batch while putting a second volatile read on every resident
add. An explicit `retention: 0` reduces the idle bound to one slice and widens the race to a batch
still inside the summation range, which is the operator's explicit memory bound and still only one
lost batch. Closing the remaining window entirely would require the add under the entry monitor
(and, for a replacement, the bin lock), exactly the cost the fast path removes. The batch is a
statistical sample, so one lost batch does not move a threshold. Owner:
`SlidingWindowDetector.recordCount`.

**One packed slot carries the count and its absolute slice.** A slot is a `long`: high 32 bits are
the low bits of the slice that wrote it (the tag), low 32 bits the saturating count. The tag is what
lets the add stay lock-free. A writer can sample slice `s`, stall through a later writer's
advance and full rotation, and then land in the slot slice `s + 2 * windowSize` reuses; without a
tag that stale count would be summed as current traffic two windows later. With the tag, the reader
checks the slice it expects and ignores the slot, and the later writer observes the old tag and
starts its count at zero. The stalled writer's CAS retry is the mirror case: when the witness tag is
ahead of its own slice, the slot has already been reused by a newer rotation, so the add drops
itself instead of replacing the newer count — its own slice is outside every reader's range, so
nothing visible is lost, while overwriting would erase a write the current window still sums. A slot
already saturated at `Integer.MAX_VALUE` for the caller's own slice returns before the CAS: the
packed word would be identical, and under a super-hot key every consumer thread would otherwise
contend for the same cache line only to write it back. The tag aliases only after `2^32` slices
(about 49.7 days at 1 ms), but the signed 32-bit difference cannot distinguish a newer rotation
from a tag written between `2^31` and `2^32` slices ago — both subtract to a positive "ahead". The
add resolves the ambiguity on its cold path with the window's 64-bit `lastSlice`: a newer rotation
is at least `2 * windowSize` slices ahead and its writer advanced `lastSlice` before the slot CAS,
while an add that observes `lastSlice - addSlice <= windowSize` proves no newer rotation can have
reused the slot and replaces the half-cycle-old tag from zero. An acquire fence after the slot read
makes the advanced `lastSlice` visible. Without it, a long-lived key whose slot went untouched for
`2^31` slices (about 24.85 days at 1 ms) dropped every write to that slot for the next `2^31`
slices, so its window sum ran low and a hot key could stay unpromoted or be cooled while live. The
doubled ring already guarantees a live slot is not reused inside the `windowSize + 1` summation
range. A parallel tag array would need two independent
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
would represent; the walk runs forward from the oldest slice in at most two contiguous runs, so the
hardware prefetcher sees ascending indexes and the loop pays no per-slot mask. A read-only sum ends
at the key's last write and drops the empty slices after it, so it walks at most that same span and
never the silence that separates the key from now. The read
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
(`max(1, staleAfterMs / sliceMs)`) and the scan itself only subtracts; the retention is therefore
quantized to whole slices, at most one slice early or late, while every real retention (minutes) is
orders of magnitude larger. The cap-relief scan calls `evictIdle()` instead of converting a duration: it uses
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
every live window, and floors the quantized bound at one slice. The floor gives a report sampled in
the previous slice one boundary of grace; a zero bound would reclaim that window before its
in-flight add lands. The previous shape used the raw quotient, so `evictStale(0)` evicted on the
very next boundary. Zero and
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

**The detection endpoint's optional parameter keeps `org.springframework.lang.Nullable`.** Every
other type in the Worker is annotated with JSpecify, and the endpoint's `key` is the one exception
that must stay on the framework annotation: on Spring Boot 3.x, Actuator decides whether an
`@ReadOperation` parameter is optional by looking for `org.springframework.lang.Nullable`, and it
does not recognise the JSpecify annotation. Boot 4.0 removes that support in the other direction —
its release notes and migration guide accept `OptionalParameter` or `org.jspecify.annotations.Nullable`
and no longer honour `org.springframework.lang.Nullable` — so the annotation follows the runtime the
module is built against (`spring-boot-dependencies` 3.5.x here), and a migration is a Boot-4 upgrade
step, not a local cleanup. The failure this prevents is a diagnosis-by-null: with the wrong
annotation a parameterless `GET` looks optional in the source, and Actuator treats it as required at
runtime, so every no-argument scrape fails with a missing-parameter error and the class still
compiles. Owner: `HotKeyEndpoint.info`.

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
values without another level. Each level's `occupied` word is conservative in the useful direction:
a bit is set only when a deadline lands in that slot and cleared by the cascade that scans it, and a
node that leaves early only leaves a stale bit behind. `advance` therefore skips a level whose word
is zero — the cascade could only clear bits that are already clear — instead of walking up to
`buckets.length` empty sentinels on every idle wake-up.
_Ruled out:_ the relative-duration rule (the coarsest level whose
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
marks a due node `DETACHED` while it moves it to the pending list. `pollAndDetached` only unlinks the
node from the owner's private batch and leaves it `DETACHED`; `HotKeyRegistry.evaluate` clears it
to `UNSCHEDULED` under the key's stripe lock before it evaluates or re-arms the entry. The field is
`volatile` so that one-way transition is published to a concurrent `reschedule`/`deschedule` that
reads it under the wheel lock. Every consumer of the state also holds the key's stripe lock (the
evaluation, cool and abort paths), and the batch is owned by the advancing thread, so the node's
links are never read by another thread while the hand-off runs. That stripe ownership is also what
lets `deschedule` check the volatile state before taking the wheel lock: the only paths that set
`SCHEDULED` run under the same stripe, so an entry seen unscheduled cannot become scheduled before
the lock is taken. A fired entry still reads as `DETACHED` after the poll, so a concurrent repair
defers to the owner and does not count it as repaired; the evaluation then clears the state and the
common cool-down path returns without acquiring the wheel lock; the in-lock check remains
authoritative. Owner: `HE`, `TimerWheel`, `HotKeyRegistry.evaluate`.

**The failure this closes (repair stealing the hand-off).** `pollAndDetached` used to clear the node to
`UNSCHEDULED` before the registry took the stripe lock. A repair that acquired the stripe in that
window saw an unscheduled node, re-linked it into `pending`, and marked it `DETACHED` again; the
owner's evaluation then set the fresh deadline, called `reschedule`, and the `DETACHED` check
skipped the link. The node sat in `pending` with a future deadline, producing a spurious wake and a
second evaluation instead of one scheduled check. Keeping the hand-off `DETACHED` until the stripe
lock makes the two writers serial: repair either sees the detached node and defers, or runs before
the poll. Owner: `TimerWheel.pollAndDetached`, `HotKeyRegistry.evaluate`, `HotKeyRegistry.repair`.

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

**The startup projection warns at 200,000 cool-down checks per second per wheel.** A wheel insert and fire is O(1) and allocation-free; the per-check cost is the registry evaluation plus the wheel link. At 200k checks/s one wheel still spends only a few percent of a core, so crossing the projection means the deployment is an order of magnitude beyond the design point, not that the wheel is full. `DetectionSettings.warn` divides `max-active-hot-keys` by the LOW_QPS check interval to project checks/s and compares it with `200_000 × coolingThreads`; the warning names `detection.cooling.threads` as the remedy. Without the warning, a configuration that projects millions of checks/s silently saturates the wheel owners' cores, delays cool-downs, and lets hot keys outlive their window until the App's hard TTL. Owner: `DetectionSettings.warn`.

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

**Only the AMQP call can turn SENT into FAILED.** The accepted-record update and the sent meter run
after the publish call. Neither is exception-guarded: `Cache.put` is infallible for a valid key and
value, and a Micrometer counter under a live registry does not throw, so a catch could only turn a
diagnostics error into a delivery failure — the dispatcher would then abort a promotion the cluster
already received, and no later cool-down could be produced from that removed entry, leaving the
cluster hot until the App's hard TTL. A publish failure itself keeps the last confirmed version and
wire type and clears only the confirmation/debounce anchor: the ordering watermark must survive a
send the broker may or may not have received, while the next same-type decision must not be
debounced by that uncertain send. A missing record still makes a later COOL conservative (it is
sent), and a missing meter is a diagnostics loss, not a correctness failure. If a future collaborator
can throw, the dispatcher's round catch owns it and returns the HOT seat; `emit` owns only the broker
call. Owner: `WorkerBroadcaster.emit`.

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
An elided decision advances the version watermark but never changes the recorded wire state: it never
travelled the wire, so it cannot make a HOT key cold or a COOL key hot. Leaving the elided version out
of the watermark let an older COOL queued ahead of it through — the Apps demoted a key the Worker had
just re-promoted, and nothing else would have corrected it. A decision that never reached `emit`
(dropped at submission, superseded, or expired) does not touch the accepted record; it cannot have
changed the cluster's state. A send failure keeps the last confirmed version and wire type and clears
the confirmation/debounce anchor, so a stale decision below the watermark stays suppressed while a
newer decision travels. A COOL with no accepted record at all is not a ghost: it is the
only demotion signal a locally promoted App can receive, and an earlier lifecycle's COOL may have
been lost, so it is sent. A cool-down COOL gets its version the same way: allocated under the same
per-key lock that removes the entry, so a re-promotion that lands afterwards outranks it.

**A HOT is coalesced against a queued decision and the registry's current promotion; an older COOL
is always retained as the fallback.** The two queues can hold a COOL that has not been drained and
a HOT from a re-promotion of the same key. `DecisionDispatcher` keeps the newest queued **Pending**
per key; an older **HOT** with a strictly newer decision already pending is dropped under the bin
lock _before_ the offer, counted as superseded and charged neither a queue slot nor a token. An
older **COOL** is queued behind the newer decision without taking ownership of the map: if the newer
decision reaches the wire, the version watermark drops the COOL as `SUPERSEDED`; if the newer send
fails, the COOL is still there as the corrective demotion. The one-slot cost buys the fallback. The identity-guarded
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
`QUEUED`/`OWNED` lifecycle pair, `SUPERSEDED_BY_PENDING` and `YIELDED_ONCE`, so taking ownership is
one `getAndBitwiseOr(OWNED)` with no retry loop, and a concurrent `supersede` cannot invalidate it.
The cool head's one-round yield mark is the item's own `YIELDED_ONCE` bit, not a dispatcher field:
a processed head is collectible as soon as it leaves the queue instead of staying pinned until a
different cool head takes its place, and the drain has no field to clear on every exit path.
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

**The cool yield reads the pending map, not the supersede bit.** An equal-version duplicate
displaces the per-key pending value without marking the copy it displaced. Marking that copy as
superseded would make a HOT's rollback abort the very promotion the last copy is about to deliver,
so the bit intentionally covers only a strictly newer transition. The cool head's yield therefore
reads `queued.get(key)` and compares versions directly; requiring `SUPERSEDED_BY_PENDING` on the
head left it unmarked when a duplicate displaced the map value, and a later HOT(v2) behind
COOL(v1) + duplicate COOL(v1) was invisible to the yield. The drain sent the first COOL before the
HOT, demoting and re-promoting every App in the same round for a state that never changed. The bit
remains the fast path for `forget` and the pre-funnel HOT drop, where the registry probe is the
correctness backstop. Owner: `DecisionDispatcher.hasNewerHotPending`, `DecisionDispatcher.offer`.

**A COOL is not elided on the strength of a promotion that never reached a queue.** The registry
commits a promotion under the stripe lock, then the report thread submits the HOT after unlocking,
so a cool-down can remove the entry before the submission. The COOL is sent anyway: the App may have
promoted locally, and the previous lifecycle's COOL may have been lost. Eliding it because the
promotion never entered a queue left such a key hot until the App's hard TTL, with no later decision
to correct it; the proof is narrower than the App's state and buys only one message. Owner:
`HotKeyRegistry.markTerminal`, `DecisionDispatcher.submit`.

**The accepted record is wire truth, and only the drain thread writes it.** `WorkerBroadcaster`
keeps `(version, sentAtMs, updatedAtMs, flags)` per key: `flags` packs the last confirmed wire type, a
confirmation bit and the long-retention bit; `sentAtMs` is the monotonic send instant and
`updatedAtMs` the instant of the record's own write. `emit` writes
the record only after a HOT/COOL send returns; `settle`
advances `version` for an elided decision and leaves the recorded HOT/COOL state alone, because that
decision never touched the broker; the one exception is the long retention, which a debounced HOT
takes back. An expired COOL gave it up because the Worker stopped being hot, but a re-promotion
inside the 100 ms window is a real HOT seat that simply never travelled, so the record must not
expire after `2 * maxDecisionAgeMs` while the cluster still sees HOT. A same-version replay does not
restore it — a replay says nothing new about the key. A send failure keeps the last confirmed
version and
wire type but clears the confirmation bit, because the broker may or may not have received the
failure. `decide` therefore suppresses a decision
only by the version watermark (`ELIDED` for an equal version, `SUPERSEDED` for a lower one) and by
the 100 ms same-type debounce anchored at the last real send. A COOL is never suppressed because
the last wire state is COOL: a COOL after a COOL outside the debounce window travels. A HOT that
never reached `emit` — dropped at submission, superseded, or expired — does not mutate the accepted
record, because it cannot have changed the cluster's state. This is what makes the accepted
record single-threaded: `DecisionDispatcher.submit` and `applyOutcome` no longer touch the funnel's
state, and `discardHeadIfTerminal`/`commit` need no CAS or retry because `classify` and `commit` run
on the same drain thread with no interleaving writer.

**The failure this closes (a failed COOL erased the HOT watermark).** A publish failure used to
invalidate the whole record. With HOT(10) confirmed and COOL(11) failing, the record vanished, so an
older COOL at or below version 10 still queued found no watermark and was sent after the Worker had
already moved past it; the App's version guard rejects it only if that App saw HOT(10), which the
failed send may have dropped. Keeping version 10 and the HOT wire type, and clearing only the
confirmation bit, suppresses the stale decision while still letting COOL(12) travel immediately.
Owner: `WorkerBroadcaster.emit`, `WorkerBroadcaster.AcceptedDecision`.

**The failure this closes (cross-thread cold state).** The previous shape stored a `COLD`/`UNKNOWN`
record and let the submission thread call `clearCold` when it dropped an older HOT. That created two
races. First, the drain could read `COLD(v0)`, decide a newer COOL was redundant, the submission
could turn the record into `UNKNOWN`, and the drain's `settle` could then write back a stale
`COLD(vN)` derived from the old read. Second, the drain could finish the elision before the clear
happened at all; the clear then saw a record at the COOL's newer version, the version guard refused
it, and the COOL was already lost. A Pending side-channel or a per-key CAS closes only the first
window, not the second, and the token-starved `commit` path has already removed and claimed the item
when a retry would need to send it without a token. The cold state was also self-contradictory:
`clearCold` replaced a `COLD(v2)` with `UNKNOWN(v2)`, and a following `markCold` was a `putIfAbsent`
no-op, so the HOT that was merely superseded destroyed the cold knowledge instead of preserving it.
It was largely unnecessary too: `coolLane` drains before `hotLane`, and a pending COOL already made
`markCold` skip, so the tombstone only covered the microsecond window where a HOT was superseded
before the COOL was submitted. Removing the cold state deletes both races, the `UNKNOWN` state and
the submission-thread mutation. Owner: `WorkerBroadcaster`, `DecisionDispatcher`.

**The cost is one extra COOL for a dropped-HOT/local-promotion lifecycle, not a redundant send.**
If the last wire state was COOL and the next lifecycle's HOT never reached `emit`, the next COOL is
sent instead of elided. That is exactly the case where an App may have promoted locally after the
previous COOL, so the message is the only demotion signal; the old cold record was betting against
that local promotion. The rate is bounded by HOTs that fail before the wire and by the 100 ms
same-type debounce, and each extra COOL consumes one cool token and one fan-out publish. No memory
is added; the accepted record is a `long` version plus one `int` metadata word (24 bytes with
compressed oops) and the cold tombstone entries disappear.

_Ruled out:_ keeping a real `COLD` record and suppressing the next COOL (the pure wire-truth variant).
It is race-free once only the drain thread writes it, but a real `COLD` record only proves the
Worker's last sent decision was a COOL; the App can have promoted locally after that, and a HOT that
never reached `emit` cannot prove otherwise. Suppressing the next COOL would leave that App hot until
its hard TTL with no later decision to correct it. If local promotion is ever removed or made
observable on the wire, the cold suppression can return; until then, the version watermark and the
100 ms debounce are the only safe suppressions. Owner: `WorkerBroadcaster.AcceptedDecision`,
`WorkerBroadcaster.decide`.

**A queued HOT never suppresses a COOL.** Suppressing the demotion on the evidence of a HOT that is
merely queued was a split-brain bug: the HOT can still expire, fail its publish or be discarded at
shutdown, and then the Worker has released the seat while every App keeps the key hot until its
hard TTL, with no later decision left to correct it. At the funnel a COOL may be dropped only when
the broadcaster's accepted record already covers it — proof that a newer decision actually reached
the funnel. Submission never drops a COOL: it is queued behind the newer decision as the fallback
demotion, and the version watermark suppresses it if the newer decision reaches the wire. A COOL
that merely wins over a still-pending HOT is not elided: that HOT may still fail, and the demotion is
then the conservative correction for whatever state the cluster already holds. A hot accepted record
never expires, so the first case cannot
degrade into a redundant send as a long hot episode ages. The cost is a demote-then-promote pair
only when the COOL is drained before its re-promotion is submitted; once the HOT is pending, the
drain lets it be accepted first (see the next paragraph) and the COOL is dropped as superseded.
Version order still converges every App to the Worker's final state, and a pair is cheaper than a
permanently split cluster. Owner: `DecisionDispatcher`, `WorkerBroadcaster.send`.

**The synchronous refusal's rollback belongs to the dispatcher.** `DetectionPipeline` hands the
`HE` to `DecisionDispatcher.submit(HE)`, which calls the abort when the queue offer is refused or
when the submission itself throws before the caller can own an outcome. The pipeline never
interprets a boolean and never calls `registry.abort` itself: a caller that ignored the return value
leaked a seat — eventually filling the seat cap and silently disabling detection. An unexpected
`RuntimeException` or `Error` after the promotion committed but before the queue accepted it had
the same effect, and under `COOL_FOR_LOW_QPS` a key that keeps receiving traffic never cools, so
the leaked seat was permanent. The abort is identity-guarded, so the exception path and the refused
path cannot release the same seat twice. The drain's own terminal-drop rollbacks are unchanged;
only the synchronous branch belongs to the component that accepts or refuses the offer. Owner:
`DecisionDispatcher.submit(HE)`, `DetectionPipeline.onReport`.

**A superseded COOL yields one round to its newer HOT.** The drain normally sends COOL before HOT,
because a mass-heat wave must not delay the demotions that free seats. A COOL that is already
superseded by a newer pending HOT is not one of those needed demotions: the Worker's newest state is
promotion, and the accepted record would drop the COOL as soon as that HOT is accepted. Draining the
COOL first would therefore demote every App and immediately re-promote it in the same round. When
the cool head is superseded, the newest pending decision for its key is that HOT, and the hot lane
has a token, the cool head yields once. The HOT is accepted first; the next pass finds the COOL
below the accepted version and removes it without spending a cool token. The yield is bounded to
one round per head by the item's own `YIELDED_ONCE` bit: if the HOT is deeper than the hot lane's
burst, a second round sends the COOL instead of letting one key block every unrelated demotion
behind it. If the HOT expires or fails,
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
decision inside the debounce window are `ELIDED`, and a strictly lower version is `SUPERSEDED`.
An elision advances only the version watermark; it never changes the recorded wire state. Both
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
inside `offer` before it can touch its predecessor; the submitter reports the drop synchronously.
A drain-round failure after shutdown breaks to the final drain instead of sleeping through the join.
Owner: `DecisionDispatcher.refill`, `DecisionDispatcher.offer`, `DecisionDispatcher.drainLoop`.

**A lane is ready when it can act, not when no lane is starved.** A readiness test that OR-ed both
lanes makes a hot lane starving while the cool lane held tokens park the drain, and on shutdown it
drops sendable COOLs. `readyToWork` asks the per-lane question instead — a non-empty lane with a
token, or a head the funnel would settle without one. The terminal test mirrors the drain: a HOT with
a newer pending decision or a released promotion, and any head `WorkerBroadcaster.classify` reports as
expired, elided or superseded. A cool head with a token always counts; the next round sends it or
yields once to a newer HOT the hot lane can send in the same round, so `!hasNewerHotPending` must not
turn a ready demotion into a false "nothing to do". The heads come from one peek per lane and are
reused for the readiness test and for the wait bound, so a starved round probes the head once instead
of three times. A round also reports the terminal items it processed, not only the sends: a lane
carrying more than one work-budget of elided, superseded or expired items used to return
`sent == 0` and park on a token those items never spend, leaving the rest of the backlog to wait for
the next refill. Owner: `DecisionDispatcher.drain`, `DecisionDispatcher.awaitWork`,
`DecisionDispatcher.readyToWork`.

**A submission wakes the parked drain only when it satisfies a wait condition.** `awaitWork`
publishes `wakeMask` — one bit per fact only a submission can supply: a HOT offered to a lane it saw
as empty, a COOL likewise, and "a queued HOT was displaced, so the hot head may now be terminal
without any lane becoming non-empty" — and then re-checks the lanes and the head, so a submission
that ran before the mask was visible is caught instead of being lost. Only a HOT coalesces on a
pending supersede: a displaced COOL is still the fallback demotion, so it keeps its slot and a COOL
supersede sets no wake bit. A submission keeps a plain field read on its own path: while the drain
is draining or backing off the mask is `0`, so `submit` finds none of its bits wanted and touches no
semaphore. A submission that does match claims the mask with one CAS and releases; the claim clears
the mask, so a burst of submissions produces exactly one wakeup. The failure this prevents is a wake
storm in exactly the state the token bucket exists for. At the default 500 HOT broadcasts/s one token
arrives every 2 ms, so during a heat wave the drain is parked almost the whole time — and `submit`
used to release the semaphore on every successful offer, including the offers that were neither the
head nor able to invalidate it. Each useless release woke the drain out of
`tryAcquire(nanosToNextEvent())`; the round it then ran could send nothing, probed the head three
times through the funnel state machine and peeked each lane three times, and parked again —
permanently, because the backlog kept refilling. The same releases made every report thread contend
the AQS state during a healthy drain, where `drainLoop`'s own `drainPermits()` discarded the permits
one instant later. Owner: `DecisionDispatcher.submit`, `DecisionDispatcher.signalIfNeeded`,
`DecisionDispatcher.awaitWork`.

**The post-mask re-check includes the hot head.** A submitter can supersede the head after the
readiness test but before the mask is published; it then reads a still-zero mask and releases no
permit, while the drain re-reads the same now-superseded head, sees no lane change and sleeps.
`awaitWork` therefore re-runs `supersededHot(hotHead)` after publishing the mask. The submitter marks
the head through the volatile state word before it reads the mask, and the drain writes the mask
before it reads that word, so the two accesses form a Dekker pair: at least one side observes the
other, and the head is either dropped by this round or the semaphore is released. The same check
covers the registry probe: a cool-down or rollback can release the head's promotion in that window
without queuing a decision, because the COOL may be refused by a full lane. Without the check, a
token-starved HOT head made terminal in that window blocked every decision behind it until the next
token refill or its age bound, because the readiness test had already run and the lane never became
non-empty. Owner: `DecisionDispatcher.offer`, `DecisionDispatcher.awaitWork`.

**The last wire state never suppresses a newer COOL.** The two queues can hold a HOT that never
left the process and the COOL of the same lifecycle. The COOL is newer than that HOT, so no pending
decision supersedes it, and the accepted record may still say the last wire state was COOL. That is
not proof the cluster is cold: the App can have promoted locally after that COOL, and a HOT that
never reached the wire cannot prove otherwise. The COOL is therefore sent once the 100 ms same-type
window has passed; only the version watermark and the debounce suppress it. The decision is made
from the accepted record, not from the queue contents, because a queued HOT may still fail. Owner:
`WorkerBroadcaster.decide`, `DecisionDispatcher.drain`.

**The funnel answers every per-key question from one accepted-decision read.** Both `send` and
`classify` read the key's `AcceptedDecision` once and decide the supersede, duplicate, debounce and
send outcomes through the shared `decide`; `settle` is the one place an elision advances the accepted
version. The token-starved head therefore pays one accepted-decision
lookup per head, and the send path pays one per decision, not one per concern. The classification
carries that record into `commit`, so the two-phase head step performs exactly one read: a second
read could observe a ledger expiry the first did not and regress the accepted version to an older
decision. The old shape probed the ledger
separately for "any accepted
record" and "the accepted version" before and after the send, so one decision paid up to three
lookups and could still discard the exact fact — whether the accepted version was equal to
or strictly above the decision's — that decides whether a HOT seat may be returned. `Delivery` has
five outcomes; `NONE` is rejected at `DecisionDispatcher.submit` and never acquires a delivery
value. Owner: `WorkerBroadcaster`, `DecisionDispatcher`.

**The accepted record is time-bounded while COOL, long-lived while HOT, and size-bounded.** The
record keeps one `long` version, one `long` send instant, one `long` write instant and one `int` flag
word (wire type, confirmation, long-retention); every replacement is immutable. `sentAtMs` is a full
monotonic instant: the previous 30-bit field wrapped every 12.4 days, and a real decision inside the
100 ms alias window at the wrap was wrongly elided even though the record itself lived for years.
`sentAtMs` anchors the debounce and `updatedAtMs` anchors the COOL retention, and both are samples of
`TimeSource.monotonicMillis()` — the same clock as the queue age bound, the cool-down wheel and every
other deadline in the Worker. The ledger is two insertion-ordered tiers: a record whose last wire
state is HOT lives in the `hot` tier and has no time bound, because `COOL_FOR_LOW_QPS` can keep a busy
seat indefinitely and `COOL_FOR_EXPIRE` is configurable in minutes; a COOL-state, unconfirmed or
retention-limited record lives in the `cool` tier and expires after `2 * max-decision-age-ms`, which
is strictly longer than the residency of any decision that can still be compared against it. Because
the drain thread's samples are non-decreasing and every write re-inserts at the tail, the `cool` tier
is ordered by `updatedAtMs`, so expiry is a head walk that stops at the first live record. The tier
caches `oldestCoolAtMs`, a lower bound on the head's write instant while the tier is non-empty: an
access creates the head iterator only once that bound reaches the retention, and a walk that stops at
a live record refreshes the bound to that record's exact instant. Removing or moving the head leaves the
bound older than the new head, so it can cost one redundant walk but cannot hide an expired record.
The failure this prevents is a per-send allocation: `send` reads the key (`get`) and then writes the
outcome (`put`) at the same instant, so the unconditional head walk allocated two `LinkedEntryIterator`s
per broadcast even when the tier was minutes from expiry, and the churn scaled with the broadcast
rate. A synthetic 50,000-step randomized `get`/`put` run (single thread, `maxEntries` 20, retention
100 ms, negative monotonic start) exercised the head walk 50,000 times before and 890 times after —
a shape check, not a throughput measurement. The tier move probes `hot` first and only then `cool`,
because a key lives in exactly one tier, so a HOT update pays one hash probe instead of two. The cap —
`max(65_536, 4 × (max-active-hot-keys + hot-queue-capacity + cool-queue-capacity))`, 120 000 at the
defaults — is the backstop for a COOL dropped at queue saturation, which never reaches the funnel and
so never updates the record. Owner: `WorkerBroadcaster.AcceptedStore`, `DecisionDispatcher.submit`.

**The cap evicts the oldest COOL record before any HOT record.** A cap is unavoidable — the
saturation case above leaves one record per key and no update ever arrives to release it — but
_which_ record it takes is not: the `hot` tier is touched only once the `cool` tier is empty. The
failure this prevents is the reverse order. Under a recency-and-frequency policy, a key that stays hot
for hours under `COOL_FOR_LOW_QPS` produces no new decision at all, so its record is the least
recently touched entry in the ledger, while a COOL wave from short-lived keys produces thousands of
the most recent ones; the cap then evicts exactly the record that still matters. `acceptedVersion`
answers `0` for a key the cluster was told is hot, and the next pre-funnel rollback — the shutdown
drain or an unexpected round failure — compares a version against nothing and aborts a live HOT seat
on every App. Evicting a `cool` record instead costs one redundant send, which the App's version guard
absorbs. The key's previous record is removed before the cap is applied, so an in-place update never
costs another key its record, and the cap is applied before the insert, so the record being written
can never be the one it evicts: with a cap of one, inserting first would evict the COOL record that
was just written. `WorkerBroadcasterTest#aCoolFloodCannotEvictASilentHotRecord` pins the property at
the default order of magnitude: 100 silent HOT records all keep their watermark while 50 000 COOL
records flow through a 1 000-entry ledger. Owner: `WorkerBroadcaster.AcceptedStore`.

_Ruled out:_ `Caffeine` with `maximumSize` plus a state-dependent `Expiry`. It states the same
retention, but its eviction order belongs to W-TinyLFU: the policy sees lookups and a frequency
sketch, not the `hotRetained` bit or a remaining lifetime, so under pressure it prefers recently
touched COOL records over a silent HOT one — exactly the reversal above. It also brought a second
clock (its own `Ticker` over `System.nanoTime()`, unrelated to `TimeSource`) and, for a map only the
drain thread ever touches, a `ConcurrentHashMap` with a read/write buffer, a frequency sketch sized to
the cap and an expiry wheel with its own maintenance lock. Two `LinkedHashMap`s under the same
single-threaded contract express the policy exactly, and the retention becomes what it is — a tier —
instead of an interval a policy may override. Owner: `WorkerBroadcaster`.

**The failure this closes (wasted replay after a long hot episode).** An accepted record used to be
retained for a flat `2 * max-decision-age-ms` whatever its wire state. A key could stay hot for 65 s;
its record expired; a stale HOT or an old COOL then found an empty record and was re-sent. The App's
version guard makes the replay harmless, and a COOL with no record is sent as the correction, so an
expired hot record costs a redundant send, not a stuck key; the `hot` tier keeps the watermark for the
whole episode and avoids that send. The cost is a record per key the cluster may still hold hot,
bounded by the HOT population plus lost first COOLs, not by the queue.

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
rate would make `readyToWork` report work while `drain` cannot enter its loop — a 100% CPU spin —
and a null collaborator would otherwise leave an orphan thread behind the failed construction.
Owner: `DecisionDispatcher`.

**A failed COOL round does not spend a HOT on the same outage.** The drain sends COOL first, and a
broker failure there is global: pulling a HOT only to lose it in the same outage is a second drop
for no progress. The failure branch therefore skips the HOT drain, backs off 10 ms even when earlier
sends in the round succeeded, and exits immediately at shutdown. A queued item has a claimed /
withdrawn hand-off between `submit` and the final drain: whoever wins the CAS owns the terminal
outcome, so `submit` cannot report success for an item the shutdown drain silently discarded.

**An unexpected drain-round failure does not kill the delivery thread.** The broadcast's own
failure handling covers broker errors, but a runtime failure from a future collaborator or a poisoned
decision would otherwise escape the loop while `running` stayed true: submissions would keep filling
the queues, every HOT would be aborted on refusal and every COOL lost. The round is caught and logged
with its stack, the in-flight decision is charged to `zeta.worker.decisions.dropped` for its type,
the HOT returns its seat, and the loop retries after the send-failure backoff. An exception after the
funnel returned an outcome does not count a second outcome or roll back a delivered promotion. Owner:
`DecisionDispatcher`.

**A suppressed or expired decision costs no send budget.** The drain consumes a token, and a burst
slot, only when the funnel answers `SENT`. A decision elided by the 100 ms debounce, dropped as
superseded, or expired by `max-decision-age-ms` never touches the wire; charging it against
`max-hot-broadcasts-per-second` / `max-cool-broadcasts-per-second` or against the per-round burst
made a queue of stale versions starve the decisions that would actually be delivered. An expired
queue head is removed before the token gate, so a starved bucket cannot delay the HOT seat
hand-back, and the funnel's in-memory state machine clears a duplicate or debounced same-type head
for the same reason: none of those can ever reach the wire, so leaving one at the head would block
a sendable decision behind it while the bucket refills. Terminal items do consume
a per-round work slot: a continuously refilled lane of elided heads would otherwise hold the single
drain thread inside one lane and delay the other lane indefinitely. The work bound is a fixed
per-round item budget, separate from the lane's send burst; it does not consume a send token or a
token-bucket budget. The idle drain blocks on a
semaphore instead of sleeping 10 ms per empty round; the release comes only from a submission that
satisfies a published wait condition (see the wake entry) or from shutdown, so a backlog that cannot
be sent accumulates no permits to spin through. A bucket-starved round still waits for the
next refill or the head's age bound while the Worker is running. The wait bound is zero for a lane
that already holds a token and a head: only the other lane is starved in that round, and charging
this head its full age bound would let the drain sleep through a sendable decision. At shutdown the
drain is woken
immediately, spends the tokens already available, and breaks on the next starvation instead of
waiting for a refill; a
failed send while `running == false` also exits the retry loop, so the thread cannot spin until the
join timeout — the remainder is counted by the final drain.

**The failure this closes (a permit per backlog entry).** Every successful submission used to release
the semaphore, so a startup backlog of 20,000 decisions left 20,000 permits behind. `tryAcquire` then
returned immediately for each of them while the bucket still had no token, and the drain thread
looped through refill, head classification and expiry checks until those permits ran out, burning CPU
for a burst that could never send. Clearing the permits immediately before the wait treated the
symptom; publishing the wait conditions removes the cause, because a submission that cannot satisfy
one of them releases nothing at all, and one that can clears the mask before releasing, so a burst of
any size leaves at most one permit behind. Owner: `DecisionDispatcher.signalIfNeeded`.

**The wait bound uses saturating arithmetic.** An expiry of `Long.MAX_VALUE` (the unbounded sentinel)
minus a negative monotonic sample overflows to a negative remainder; clamping that with
`max(0, diff)` turns an effectively infinite wait into zero, and a token-starved lane then calls
`tryAcquire(0)` in a loop until the next refill, pinning one core. The remaining milliseconds are
saturated at both ends before the conversion to nanoseconds. Owner:
`DecisionDispatcher.Lane.saturatedRemainingMs`.

**The token bucket's ceiling is the larger of one second's rate and the per-round burst.** A bucket
capped at `rate` makes a configured `burst > rate` unreachable: the round stops at the rate even
though the operator asked for the larger burst, and the setting silently does nothing. The ceiling is
therefore `max(rate, burst)`, which keeps the usual one-second allowance for every default
(`burst <= rate`) and makes a larger configured burst effective; the sustained refill rate and the
queue capacity remain the real bounds. A token is spent as soon as the funnel reports `SENT`, before
`applyOutcome` runs, so a metrics or rollback exception cannot refund a decision that already reached
the wire. Owner: `DecisionDispatcher.Lane`, `DecisionDispatcher.drain`.

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
applies it via `HeatDirectory.revoke` — the same in-memory compute the dispatched path uses; hooks
are skipped so the consumer thread never waits on user code). Rejections are counted (`zeta.worker.decision.cool.rejected.total`,
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
in log lines. A publisher call that throws is added to `DispatcherStats.publishFailed` and exposed
as `reportPublishFailedCount` / `zeta.reporter.publish.failed.total`: before that, a broker outage
only produced a WARN per batch while the queue still had room, so the stall was invisible to every
counter until the queue overflowed. Owner: `KeyReporterImpl.routeAndEnqueue`,
`KeyReporterImpl.consumeLoop`, `KeyReporter.DispatcherStats`, `KeyReporter.RoutingStats`.

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
stall must not become a local-detection stall. A zero timeout is a **mode**, not a spent budget:
it issues a non-blocking `offer` for every shard and never enters the deadline check, so an empty
queue accepts immediately instead of dropping the whole flush. Owner: `KeyReporterImpl.Dispatcher`,
`KeyReporterImpl.routeAndEnqueue`, `AccessCounter.onTide`.

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

**A consumer survives a throwing health probe.** The incarnation check runs on the consumer thread
before every publish. The consumer pool is fixed and tasks arrive through `submit`, whose exception
lands in an unobserved `Future`; a `RuntimeException` from the health view therefore killed the
thread permanently and silently shrank the report plane. The probe is now inside a guard that
counts the batch as dropped, logs once per failed batch, and continues the loop. Queue capacity,
consumer count and the offer budget are validated in the constructor so a zero-sized pool or a
negative budget fails bean creation instead of `start()`.

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
for the whole lead duration. The sync receiver therefore re-stamps a refused message with a
freshly allocated local stamp when the mutation is **applied**, not when it is consumed: the
AMQP listener (`CacheSyncListener`) only merges the observed stamp, and `DefaultSyncContext`
allocates the replacement on the per-key serialized dispatcher after `Batch.reduce`. A
consumer-thread allocation can invert two same-key messages: thread A allocates the lower stamp
for v2 and is preempted before `submit`, thread B allocates the higher stamp for v1 and submits
first, and the dispatcher applies v1 at the higher stamp and drops the newer v2 as stale — the
old value survives until the next write or TTL. Deferring the allocation makes the replacement
order equal the apply order, so the mutation the key applies last always holds the highest local
stamp. The reduction still compares the peer versions, so a covered message never allocates; a
refused clear batch resolves only its surviving watermarks. The mutation still applies, the
ordering claim against a broken-clock peer degrades to dispatcher arrival order (the only order
left), and the node's monotonicity invariant holds end to end. The skew gauge samples the newest
refused outlier for a bounded retention window (60 s): a single corrupt future stamp must be
visible to the operator without pinning the gauge at a multi-year lead long after the peer stopped
sending. The previous all-time maximum made one bad message a permanent alert; accepted stamps
still feed the gauge through the local floor. _Ruled out:_ merging without a bound — one peer
with a clock hours ahead raised every node's floor, and every local write stayed numerically
below it until physical time caught up. _Ruled out:_ persisting the refused version next to an
unraised floor — that combination is the write-blindness above.

**Batched allocation reserves one contiguous block.** `StampClock.nextStamps(count)` reserves
`[base, base + count − 1]` with a single CAS and returns the base. No concurrent allocation —
single or range — can land inside the block, so a concurrent single write is ordered against a
whole batch at once: its stamp is either older than every batch stamp or newer than every batch
stamp, never interleaved between two of the batch's keys. That is what closes the partial-apply
window a per-key allocation chain left open when a single write raced a batched invalidation
(owner: `HotKeyCache.executeBatchInvalidate`). The range must fit one
sequence window (`MAX_ATOMIC_RANGE` = 4096, the layout's 12-bit sequence field, node bits constant
across the block — this keeps the block disjoint from every other node's window); callers chunk by
that bound, and a wire batch (`MAX_BATCH_ENTRIES` = 1000) always fits one call. Count outside
`[1, MAX_ATOMIC_RANGE]` is rejected — a silent clamp would break the exclusive-reservation
guarantee the caller relies on.

A batch **read** does not reserve a block: each key takes its stamp inside its own load. The block
belongs to a batch the caller applies as one decision (an invalidation set, a `putAll` snapshot),
where interleaving would split the decision. A read has no such decision — its stamps only have to
be newer than any invalidation that preceded the load, and a block reserved before the reads start
gave a key a stamp minted ahead of a concurrent invalidation, so a value read after that
invalidation was refused write-back as stale and the key missed L1 with the data already in hand.
Reserving it also allocated a `HashMap` entry and a boxed `Long` per key, most of them for keys that
became followers or were never loaded. Owner: `HotKeyCache.loadAndCache` (batch).

**A stamp clock is not a duration clock, and a deadline is not a stamp.** Versions use the
wall-clock-derived, merged clock. Absolute TTL deadlines (`hardExpireAtMs` / `softExpireAtMs`) are
minted and judged on the monotonic axis: `TtlPolicy.hardExpireAt` / `softExpireAt` take their
sample from `TimeSource.monotonicMillis()`, and `isLogicallyExpired` / `isSoftExpired` compare
against the same axis. A deadline never leaves the node that minted it — entries are process-local,
`SyncMessage` carries `(id, key, type, version, payload)` and no deadline, and nothing persists an
entry — so the wall clock buys TTL nothing. Keeping the two axes separate is what stops a
stamp-space rollback from rewriting entry lifetimes and a clock step from reordering versions.
`EntryView` hands the same instants to `SyncPolicy`/`SyncHook`/`HotKeyContext`, so they are
documented there as node-local monotonic milliseconds rather than timestamps. Owner: `TtlPolicy`,
`EntryDraft.SYSTEM`, `ZetaAutoConfiguration.remainingHardTtlNanos`, `EntryView`,
`EntryLifecycle.evictIfExpired`.

_Ruled out:_ minting deadlines from the cached wall clock (`TimeSource.currentTimeMillis()`, the
shape this replaced). It made every TTL a function of the machine's clock adjustments: a forward
step larger than the elapsed real time plus the 5 s tolerance is slewed at twice real time, so
while the slew runs a declared 10-minute TTL elapses in as little as 5 minutes of real time — and
`remainingHardTtlNanos` fed that slewed remaining time to Caffeine, whose own ticker counts
`nanoTime`, so the scheduled eviction and the entry's declared lifetime disagreed on top of it. The
failure is invisible to tests and to a healthy JVM, because both axes are anchored to the same
instant at JVM start and diverge only when the wall clock is adjusted — exactly the production
condition the axis choice exists for. The one property wall-clock deadlines do have, comparability
across nodes and restarts, is a property no Zeta entry needs. Two regression tests pin each half:
`TimeJumpTest#ttlDeadline_shouldBeMintedOnTheMonotonicAxis` (a deadline minted under a wall offset
still lands on the monotonic window) and `TimeJumpTest#wallJumpAfterTheBuild_shouldNotExpireAStoredEntry`
(a step after the entry exists cannot expire it).

**The cached wall clock is monotonic and bounded.** `TimeSource` refreshes the wall clock on a 5 ms
daemon tick. A backward step never moves the cached value (a stamp window stays non-negative), and a
forward leap larger than the elapsed real time plus a 5 s tolerance is refused and slewed at twice
real-time rate until it reaches the wall clock. Without that bound, a transient bad leap that NTP
later reverts pins the cached clock in the future for the length of the leap, and every stamp and
trace ID minted in the window carries the bogus future; without the >1× rate, a genuine
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
instead of resurrecting the running sign. Both `publishRunning()` and the ticker clamp the
published value to at least `1`, so a synthetic negative offset cannot flip the sign, publish the
stopped encoding, and make the ticker exit on its next iteration. The test offset adjusts a running
cached value by the delta and leaves a stopped floor raw, which keeps the existing time-jump tests
exact. _Ruled out:_ a separate `running` flag — it made every read pay a second volatile load and
let the flag and the cached value describe different instants.

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
path clears the slot before starting its replacement. Thread creation, handler installation, slot
assignment, `publishRunning()` and `start()` share one try: a throwable before the thread was
installed resets the slot and the stopped sign, so a failed start cannot leave a positive cached
value with no ticker and freeze every read. The lifecycle owner count is incremented
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

**A refresh applies against its creation-time snapshot, never against the incoming stamp.**
`DefaultBackgroundRefresher.RefreshTicket.settleLoaded` accepts a refresh when the resident entry's
`dataVersion` is not strictly above the ticket's snapshot; the comparison baseline is the value
read when the refresh was registered, not the stamp the loader returned. An incoming-stamp
comparison is wrong in both directions. With no concurrent write the resident stamp equals the
incoming stamp, so an equal-skip rule (`shouldSkipForSync`) discards every ordinary refresh: the
soft window is never extended, every soft-expired read re-arms another refresh, and
stale-while-revalidate degrades into a blocking reload per read during the whole hard TTL. With
equal-apply (`shouldSkipForRefresh`), a stamp sampled after a concurrent writer re-applies the
stale value over the writer's fresh entry. Only the creation-time snapshot distinguishes
"nothing changed" from "a newer write landed in flight", so an ordinary refresh extends the TTLs
while a concurrent writer's value survives. Owner: `DefaultBackgroundRefresher.RefreshTicket.settleLoaded`,
`DefaultBackgroundRefresher.submitTicket`.

The cache-level pre-check reads through `QuietCache.getIfPresent`, not `Cache.getIfPresent`: a
sync-plane first pass is not a caller read, so it must not record a hit/miss, promote the key in
Caffeine's access-order queue, or feed the frequency sketch that decides eviction. The entry-level
overload is called directly, with no capturing predicate.
REFRESH skips the pre-check when no cluster loader is configured: there is no loader I/O to avoid,
and the fallback's compute applies the same version guard inside the bin lock. The decision plane
never touches an L1 entry, so a Worker broadcast cannot inflate the caller statistics or mark a
cold key as recently used in TinyLFU.

Decision plane (`shouldSkipForWorker`, the one guard for both HOT and COOL): absent → accept;
a missing/blank node id on either side → accept on arrival (an anonymous decision is not provably
from the same Worker, so epoch and version are incomparable); different `nodeId` → accept on
arrival (cross-Worker versions are incomparable); same node,
higher epoch → accept (restart); same node, lower epoch → skip (stale incarnation); same node
and epoch → skip when `resident decisionVersion >= incoming`. A non-positive `decisionVersion` is
unorderable and is dropped at `WorkerDecision.from`, before the body decode and the dispatcher: a
malformed broadcast must never reach the "different node" acceptance path and create or overwrite a
decision. The guard therefore assumes a positive incoming stamp and does not re-check it; the
public entry points do reject a null stamp up front, so an absent resident cannot silently make the
contract depend on whether a resident happens to exist. An equal same-node/same-epoch decision is
skipped for both kinds: the lease table stores the latest decision stamp, so a replay is already
represented and re-applying it would only duplicate the business callback. The earlier per-entry
`NORMAL`/`HOT`/`COOL` state needed a HOT-only exception — a local transition kept the stamp, so an
equal HOT had to re-promote the entry instead of being skipped forever — and that exception
disappeared with the entry state.

**Cross-Worker order is arrival order.** Epoch and `decisionVersion` are per-Worker counters
(epoch is seeded from that Worker's wall clock at boot, `decisionVersion` starts at 0), so they
are not comparable across Workers. Comparing them across Workers silences a surviving Worker
after a rolling restart — the restarted Worker stamps a higher epoch, and every later decision
from an older-epoch Worker is dropped until the newer Worker's lease ends. The owning Worker
re-broadcasts its decision, so a stale out-of-order decision is corrected by the next broadcast.
**The ghost-COOL guard is COOL-only and lives in `HeatDirectory.isStaleCrossNodeCool`**: it
applies only to COOL decisions, and it drops a cross-Worker COOL aimed at a live resident (a HOT
lease or another owner's COOL tombstone) whose owner is still the **current, alive incarnation**
(`isCurrentIncarnation(owner, lease.stamp().decisionEpoch())` — the same probe
`HeatDirectory.isClusterHot` uses) while
the ring still routes the key to that owner. The ring is reconciled from that health view in the
lock-free pre-check before the ownership probe: a published snapshot that predates the heartbeat
making the owner live would still route to the old owner and bypass the guard while a new Worker
joins. The re-check inside the entry compute never rebuilds — a health-view walk under Caffeine's
bin lock would serialize every key in that bin behind it — so an owner still absent from the
published snapshot there fails open and the COOL applies. It fails open on any missing signal and
never touches a value decision, so it cannot reintroduce the "mute the survivor" failure it
complements. A restarted owner no longer protects the previous incarnation's HOT: the COOL applies
and revokes the lease.

Owner: `VersionGuard`, `HeatDirectory`.

---

## Sync messages

**Six types, one meaning each.**

| Message             | Carries                                         | Sent when                                                        | Receiver effect                                                           |
| ------------------- | ----------------------------------------------- | ---------------------------------------------------------------- | ------------------------------------------------------------------------- |
| `PUT`               | key (header), version, typed value bytes (body) | synced write                                                     | version-guarded apply, no reload                                          |
| `REFRESH`           | key, version                                    | synced refresh; PUT fallback when the value cannot ride the wire | reload a resident (or prewarm-eligible cold key), else guarded invalidate |
| `INVALIDATE`        | key, version                                    | invalidation                                                     | version-guarded removal                                                   |
| `INVALIDATE_BATCH`  | up to 1000 (key, version) pairs                 | collection invalidation                                          | per-key removal on the ordered dispatcher                                 |
| `INVALIDATE_ALL`    | clear stamp                                     | `clearAll()`                                                     | sweep entries at or below the stamp + watermark                           |
| `INVALIDATE_PREFIX` | key prefix, clear stamp                         | a scoped namespace clear received from a peer                    | sweep entries under the prefix + scoped watermark                         |

**The type is decoded once into `SyncType`.** The AMQP type header becomes an enum at the
listener boundary; self-ignore, clear-family and jitter requirements are predicates on that enum,
so the listener, the dispatcher and the handler never re-compare header strings. An unknown or
missing type is dropped at parse (DEBUG line in the listener) and never enters the
origin filter, the collision probe, the body decoder or the ordered dispatcher; `SyncMessage.from`
stays a pure data decoder. The origin-instance header is read once per delivery and shared by the
self-filter and the node-id collision diagnostic. A lenient `readStringHeader` helper owns the
String-vs-`toString` conversion for the app-name, origin-instance and PUT cache-key headers, so
the same conversion is not re-derived at each site. Owner: `SyncType`, `SyncMessage.from`,
`CacheSyncListener`, `AmqpMessageReader`.

**The decision plane uses the same decoded-type shape.** `WorkerDecision` carries one
`DecisionStamp`, whose `DecisionKind` (HOT/COOL) is parsed once from the header and is the only
operation field; a second type field could disagree with the lease it describes. No handler has to
infer a lease type from the effective state. The listener reads the parsed flags for admission —
`isControl()` for the global-budget exemption, `needsJitter()` for the start-jitter park — and the
handler compares the stamp directly through `VersionGuard`, so no branch re-parses a string and no
handler rebuilds a record per message. The wire headers are unchanged: the enum name is the
`HEADER_TYPE` value. A blank cache-key body is rejected at the decoder, matching `SyncMessage`, so
an unservable key never reaches the cluster loader. Owner: `DecisionKind`, `WorkerDecision`,
`WorkerListener`, `VersionGuard`.

**The loader read, entry snapshot and hook tail have one owner per plane pair.**
`DecisionPlaneSupport` holds the optional cluster-loader read — `LoadResult` keeps the
declined-vs-missing distinction, while a clean miss and a loader failure carry a null value — the
shared `EntryView` snapshot and the hook loop that isolates a broken hook. The caller checks its
list for emptiness before building the capturing callback, so the default no-hook deployment
allocates nothing on the apply path. Owner: `DecisionPlaneSupport`, `DefaultSyncContext`,
`WorkerListener`.

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

**Same-version priority is INVALIDATE > PUT > REFRESH, in arrival order too.** The publisher's
`BroadcastClaim` already treats an equal-version INVALIDATE as an upgrade over a REFRESH. The batch
fold must use the same rule: a later equal-version REFRESH must not cover an INVALIDATE of the same
version, or the final state depends on message arrival order. At the same version a PUT outranks a
REFRESH: the PUT carries the value at that data stamp and avoids the loader read a REFRESH would
trigger; a PUT whose payload cannot be decoded still degrades to a versioned eviction. Previously
an equal-version PUT/REFRESH pair was resolved by arrival order, so a REFRESH that landed first
discarded the value the PUT already carried. `BroadcastClaim` covers PUT too: a PUT claims its
version so an equal/lower REFRESH is suppressed, while an equal-version INVALIDATE still replaces
the claim. Every covered INVALIDATE is retained in the covered list, so each reports
`onSkipped(COVERED_BY_NEWER_EVENT)` exactly once; the highest one is also kept as the batch's
fallback removal. When the winning value message falls back to an eviction, the value event
reports `afterValueDegraded`. When the policy refuses the winner before it writes, or the
policy call fails, that highest covered INVALIDATE evicts at its own version instead of being
lost with the refused winner. Previously the reduce kept only the highest covered INVALIDATE
and reported it from selected apply branches: lower covered INVALIDATEs never received a
terminal callback, and a policy-refused PUT/REFRESH left the covered removal unapplied — the
resident kept serving pre-invalidation data. Owner: `Batch.reduce`, `DefaultSyncContext`,
`CacheSyncPublisher.claim`.

**A value event is idempotent at an equal stamp.** A PUT or REFRESH whose version the resident
already carries is settled as a skip, before any payload decode or loader call. The stamp identifies
the write, so a redelivery (a consumer reconnect, a broker retry) has nothing newer to contribute:
re-applying it re-fetched the source and restarted the entry's hard and soft TTLs, turning one
at-least-once delivery into extra source load and an unbounded lifetime extension. It also made the
node-id collision warning untrue: two instances sharing a 10-bit node id mint equal stamps, and with
an equal stamp applying, each node overwrote its own value with the other's — the two sides swapped
data instead of both skipping it. INVALIDATE and the clear sweep keep the opposite rule (an equal
stamp applies) because a removal at the same version outranks a value by
`SyncType.priority()`, and a sweep must remove an entry stamped exactly at the clear. A version-less
resident (`dataVersion == 0`) still accepts any stamp on both paths. Owner:
`DefaultSyncContext.staleReason`, `writePeerValue`, `VersionGuard.shouldSkipForSync`.

**A peer REFRESH load leaves the ordering threads.** `DefaultSyncContext` submits the blocking
reload to a dedicated refresh executor and keeps PUT decode, INVALIDATE and the clear family on the
ordered dispatcher. A REFRESH storm or a slow source previously occupied every scheduler thread
with synchronous `loader.load` calls; the O(1) INVALIDATE/PUT/CLEAR tasks queued behind them, so a
cross-node invalidation could miss its whole hard TTL window and a saturated dispatcher forced the
consumer-thread fallback. The refresh slot keeps one active load per key and one newest parked
replacement: equal/lower versions are reported as covered, a parked higher version runs after the
active one, and the version/tombstone guards are re-checked before the write. A superseded task does
**not** evict on the spot: its covered stamp is merged into the slot (`activeCovered` for the active
task, `nextCovered` for the parked one) and handed to whichever task settles last, which reports the
highest stamp it satisfied — the write version, the evicted version, or zero when a replacement is
parked. Evicting at supersede time removed the live resident before its replacement had loaded, so
the replacement saw an absent key: a dynamic-TTL or prewarm-gated namespace refused the cold load
(`TTL_NOT_PREWARMABLE` / `ABSENT_NOT_PREWARMABLE`) and the refresh that was meant to renew the key
deleted it instead, while an admitted cold load took the namespace defaults
instead of the resident's declared TTLs. Dropping the eviction without the carry is the
opposite failure: a replacement the policy skips, or one whose load fails, leaves a value the batched
INVALIDATE already condemned, and a soft-expire degradation leaves it serving as stale. The carry
survives two edges that used to lose it. A slot that closes with no replacement left applies the
removal it still owed before it disappears: `pollNextRefresh` discarded `activeCovered` whenever
`next` was null, so when the settling task left a soft-expired resident behind — its own
degradation removed nothing — and a batch had merged an INVALIDATE into the slot while it ran, that
removal vanished and deleted data kept serving out of L1 until its hard TTL. And a value event
refused by the invalidation tombstone applies its covered INVALIDATE before reporting the skip: the
tombstone refuses the _write_, not the removal the same batch carried, so `[INVALIDATE(v10),
PUT(v11)]` arriving under a tombstone at `v12` left the `v5` resident in place while the same
INVALIDATE on its own hard-removed it. Both paths now follow one rule: the settle that owns the key
satisfies the covered stamp, and a settle superseded by a parked task hands the obligation over
instead of evicting. Every degradation branch — `settle` returning `INVALIDATE`, a value the loader
declines, an un-prewarmable resident, a throw inside the drain — checks supersession first for that
reason. The load itself always uses `CacheLoader.reload`: a peer REFRESH runs off the caller's stack
whether or not a resident exists, so a loader that overrides `reload` to shed caller-bound state (a
transaction, a security context, an MDC) must not be bypassed for the cold case. When the executor
refuses, both the active task and its parked replacement degrade to a local versioned invalidation
(`REFRESH_REJECTED`), so backpressure costs freshness rather than correctness. A submission failure
that is not a plain rejection, or an `Error` escaping the drain, retires the slot and degrades the
still-running task plus its parked replacement (`REFRESH_FAILED`); leaving the slot behind would make
every later refresh for that key look superseded and be dropped. The drain clears its task cursor
immediately after `executeRefreshTask` returns, before polling for a replacement: the task has
already received its terminal callback (applied, skipped or degraded), and a poll that threw used to
re-enter the cleanup with that settled task still in hand, so the same message was degraded a second
time — `afterValueDegraded` fired twice for one delivery and the second eviction could undo a value a
later message had just written. The pool is the
bounded `peerRefreshExecutor`, sized from the refresh concurrency bound and separate from the
interruptible load pool, so a peer burst cannot park a local read-miss load. Owner:
`DefaultSyncContext.scheduleAsyncRefresh`, `drainAsyncRefresh`, `KeyDispatcher`,
`ZetaAutoConfiguration.peerRefreshExecutor`.

**A cold REFRESH is a prewarm, not a write mirror.** A peer write of a key this node never served
previously loaded it on every instance: an N-way source amplification and an L1 pollution, because
the key was not hot here. `applyValue` now reloads an absent (or logically expired) key only when
the namespace's prewarm trigger accepts the heat signal (`heatView.isHot` when there is no policy
table). A cold key outside the trigger records a versioned invalidation instead — zero I/O, zero
entry, with the tombstone and dedup-flight kill preventing a concurrent old read from refilling it.
A PUT still travels because it carries the value; it is subject to the same dynamic-TTL check.
The gate holds again inside the deferred task: a REFRESH admitted while its entry was live can reach
the loader after that entry expired or was evicted, so `executeRefreshTask` re-runs the same refusal
before the blocking load. Deciding once at enqueue time meant a dynamic-TTL namespace paid one
blocking source call per queued task and then discarded the value at `writePeerValue`, while a
static-TTL namespace with prewarm off wrote the cold value into L1 — the one outcome the gate exists
to prevent.
Owner: `DefaultSyncContext.canColdRefresh`, `coldRefusal`, `applyValue`, `executeRefreshTask`.

**A dynamic-TTL namespace has no cold baseline.** `NamespacePolicyRegistry` marks a namespace with
a per-call TTL as non-prewarmable and publishes the global default only as a placeholder.
`writePeerValue` previously applied that default to an absent entry, silently turning a short
runtime TTL into the long global one. Both the admission path and the atomic write now refuse a
cold value when the resolved contract is not prewarmable (`TTL_NOT_PREWARMABLE`), degrading to the
versioned invalidation; a resident keeps its own stored durations, which is the only correct
lifetime for a peer value. Owner: `DefaultSyncContext.applyValue`, `writePeerValue`,
`NamespacePolicyRegistry.registerAnnotatedNamespace`.

**Control invalidations bypass the payload budget.** `INVALIDATE`, `INVALIDATE_ALL` and
`INVALIDATE_PREFIX` are O(1) control messages, like a COOL. They carried a payload-sized global
weight and, under a 32 KB PUT storm, were refused at submission — skipping the single-key batch
fold and the `SyncPolicy`/`SyncHook` path entirely. `SyncType` now marks them `CONTROL`; the
listener submits them through `KeyDispatcher.submitControl`, which keeps their per-key ordering and
per-key bound but exempts them from the global memory budget. A refused control message still
falls back to the no-hook inline invalidation. Owner: `SyncType.isControl`,
`CacheSyncListener.submit`, `KeyDispatcher.submitControl`.

**The soft-expiry fallback follows any heat, not only a Worker lease.** A failed or degraded
REFRESH may close only the soft window of a live resident and keep the hard-valid value as a
fallback. Requiring `isClusterHot` disabled that fallback in a deployment without the Worker plane
or for a purely local hot key, so the next read paid a full reload even though the instance was
still serving the key. `evict` now tests `HeatView.isHot`: a local TopK hit is enough to keep the
value, and a key with no heat signal is removed. Owner: `DefaultSyncContext.evict`.

**A clear batch has no winner; its shape is the classification.** The clear family shares one
dispatcher channel so all table sweeps serialize, and every clear watermark covers a different key
set. `Batch` is sealed into `Batch.Value` (a non-clear batch reduced to one effective message) and
`Batch.Clear` (every table-wide and prefix clear, applied in one sweep). `CacheSyncListener`
routes on the batch type, not on a synthesized winner, so the old `outranksClear` reduction and
the meaningless clear-batch `winner` field are gone. `Batch.Value` still takes a single-message
fast path without allocating a covered list, and `allowSoftExpire()` is derived from the winner
and the covered invalidate rather than stored as a second field that could drift. Owner: `Batch`,
`CacheSyncListener.dispatchBatch`, `DefaultSyncContext.applyClearBatch`.

**Semantics, stated once.** At-most-once for every sync/decision message (ack before update). A
crash between ack and update loses one message; the next write or the TTL heals it.
Fire-and-forget: no publisher confirms, because all three publish paths are state-oriented and
the next cycle re-sends. Best-effort ordering, with the version guard as the only authority —
the deferred buffer and the direct send paths were never guaranteed to order against each other.

**Origin filtering drops every echo of a sender's own fanout broadcast.** The fanout delivers a
sender's own message back, and the sender already applied the mutation locally: a self-REFRESH would
equal-apply the value it just loaded, a self-PUT would replay the value it just stored, and a
self-clear would repeat a full-table sweep over a cache it just emptied. The single-key and batched
INVALIDATEs are no different, and were the two that used to be processed: with `Scope.REMOTE` that
deleted the local entry the policy promises to leave untouched, and right after a `dropLocal` it
re-invalidated the key's flight — the one a read had already started on a higher stamp, whose fill
was then discarded. The loopback was kept to heal an invalidate-vs-reload race, which the tombstone
already closes: `dropLocal` records it before the bin-locked removal, and every L1 write path
re-checks `staleAgainstInvalidation` inside the same `asMap().compute`, so an older fill either
loses the lock and is removed or observes the tombstone and is refused. Messages without an origin
header are still processed (rolling upgrade). Owner: `CacheSyncListener.onMessage`.

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
same wire shape through `HotKeyCache.invalidate(Collection, InvalidatePolicy)`, where the send path
leaves the local copy alone, while the fanout loopback still applies the per-key INVALIDATEs exactly
as a single-key self-INVALIDATE does. The decoded batch exposes the highest entry version as the
message version, so the receiver can merge the sender's clock upper bound once. It must not
re-stamp a refused batch with one local stamp: each entry is observed on its own, and only an entry
whose own version breaches the forward-drift bound receives a fresh local stamp — allocated when
the reduced per-key batch is applied, never on the unordered consumer thread. The frame-wide
replacement raised an older, accepted entry past a newer resident and evicted a live value — a
collection invalidation in one key's range could delete data the per-key guard should have kept.
The frame probe only reads the anti-drift verdict and allocates no throwaway stamp; the reduced
entry that is actually applied allocates exactly one. A refused clear batch resolves its
watermarks in ascending peer order before applying the table-wide coverage, so a replacement
stamp cannot invert an older full clear above a newer prefix clear. The `afterClear` events carry
those resolved watermarks, not the refused peer stamps, so a hook and the sweep observe the same
version.
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
context, and the whitelist had two independently constructed mappers to keep in sync. The whitelist is a custom `PolymorphicTypeValidator`, not a package-prefix
builder. Its two hooks split the work by what each one can see. `validateSubClassName` receives only
a type id — Jackson truncates it at the first `<` before the call, and skips `validateSubType`
entirely when the name verdict is `ALLOWED` — so an admitted raw name answers `INDETERMINATE`
instead. That forces every resolved type through `validateSubType`, which is the authority:
it admits a primitive (an `int[]`/`byte[]` component has no class name in any set), recurses through
an array's component type, checks the raw class against the explicit set plus segment-bounded
configured prefixes (`com.example.dto` cannot admit the sibling `com.example.dtoOther`), and then
recurses through every generic argument. `java.lang` and `java.util` are therefore never
whole-package rules. The set admits the boxed primitives, `String`, the two mutable `CharSequence`s,
the standard containers, `UUID`/`Date`/`Currency`/`Locale`/`BitSet`/`Optional`, and the JDK
collection views a DTO field can carry (`Arrays$ArrayList`, the `ImmutableCollections$*` and
`Collections$*` forms); it does not admit `Thread`, `ProcessBuilder`, `Timer` or
`java.util.logging.FileHandler`. The framework's own package is narrowed to
`io.github.hyshmily.zeta.model.*`; `java.time` and `java.math` stay prefixes because they hold value
types only. Three failure shapes motivated the exact set. `BasicPolymorphicTypeValidator.allowIfSubTypeIsArray()`
matched `clazz.isArray()` and admitted the array form of any class, so `["[Lcom.evil.Gadget;", [...]]`
reached a component type that no rule ever judged; the name hook now strips the dimensions and judges
the component before the array class is resolved. A whole-package prefix admitted types a PUT can
never mean — a logging handler is a well-formed `java.util.logging` class with a no-arg constructor.
And an allowed raw class carried its type arguments past every rule: `["java.util.ArrayList<com.evil.Gadget>", [...]]`
answered the name hook with the truncated `java.util.ArrayList`, which is allowlisted, and the
`ALLOWED` verdict meant the constructed parameter type was never presented to a validator — Jackson
then bound attacker-shaped values to a class no rule had judged (a regression test pins the decode to
`null`; with the old `ALLOWED` verdict the same payload decodes an `ArrayList` of that class).
Answering `INDETERMINATE` and recursing is what closes it; a recursive check that only runs inside
`validateSubType` closes nothing on its own, because that hook is not reached for an allowed name.
Default typing is `NON_FINAL` (the deprecated `EVERYTHING` is removed in Jackson 3) over an
`Object`-declared root: `writerFor(Object.class)` / `readerFor(Object.class)` keep the wrapper for a
final runtime value such as `Long`, `Instant`, `byte[]` or a record, which a final declared type would
write natively. The mapper also discovers the Jackson modules on the application classpath
(`findAndAddModules`): the allowlist admits `java.time`, and those values need their serializers or
every `Instant`/`LocalDate` PUT degrades to REFRESH. Module discovery adds serializers, not type ids,
so it does not widen this surface. The root normalizer compares exact classes: a `Map` subclass from a
framework or a double-brace initializer is copied to `LinkedHashMap` (same for `List`/`Set`, including
`EnumSet`/`EnumMap`, which become a `LinkedHashSet`/`LinkedHashMap` and therefore write their enum
keys as declared `Object` values) instead of being encoded under a type id the receiver must refuse;
a `LinkedList` root is already a standard container and keeps its type.
Owner: `PutPayloadCodec`,
`CacheSyncPublisher`, `DefaultSyncContext`.

**A value-less REFRESH cannot keep a known-stale value.** The peer's write already happened, so
the local copy is stale by definition. With no cluster loader the receiver force-expires the soft
window of a Worker-managed entry — the next read reloads through the application reader while the
Worker decision and its extended TTLs survive — and removes an ordinary entry outright. Preserving
the value until the HOT hard TTL (the previous shape) served a known-stale value for up to an
hour. _Ruled out:_ removing the Worker-managed entry too — that discarded the decision stamp and
made the next Worker decision start from scratch.

**A fallback eviction is not a peer INVALIDATE.** When a REFRESH loads nothing, a PUT payload is
unreadable, or the loader refuses a peer value, the receiver still evicts the resident under the
message's version and tombstone — but it reports the value event as `afterValueDegraded`, not
`afterInvalidate`. That hook means "a peer INVALIDATE was applied"; firing it for a failed value
delivery told observers a peer had explicitly invalidated a key when it had merely failed to
deliver a value, and left the value event with no callback that named its cause. A covered
INVALIDATE folded out of the same batch lost the reduction, so it too is reported as
`onSkipped(COVERED_BY_NEWER_EVENT)`; only the winning event carries the removal effect. The
loader's `isCacheable` is a caller-supplied SPI: an exception is treated as a refusal and takes
the same versioned invalidation path, so it cannot escape the applier and skip the terminal hook
or the covered-INVALIDATE fallback. A loader exception used to be swallowed by `loadValue` and
reported as `VALUE_NOT_FOUND`, so a source outage looked like an absent row and `REFRESH_FAILED`
was unreachable on the peer path; the load result now carries a separate failure sentinel and the
two outcomes reach `afterValueDegraded` with their own cause. Owner:
`DefaultSyncContext.degradeToInvalidate`, `DefaultSyncContext.admitsValue`,
`DefaultSyncContext.loadValue`, `SyncHook`.

**A failed deduped send drops its claim instead of restoring the displaced version.** The
claim previously rolled back to the version it displaced — but that version's own send may
equally have failed or still be in flight, so the rollback recorded an unsent version as sent
for the whole dedup window and a retry of it was silently skipped. Dropping the claim at worst
re-sends one idempotent broadcast (the plane's at-most-once contract); keeping a phantom claim
can lose the mutation for the window. Owner: `CacheSyncPublisher.publishDeduped`.

Owner: `CacheSyncPublisher`, `CacheSyncListener`, `DefaultSyncContext`, `SyncMessage`,
`AmqpMessageReader.AppIsolationFilter`.

---

**A structurally malformed batch is dropped whole, not recovered entry by entry.** The batch body
is a transport envelope of `[key, version]` pairs. The parser now stops at the first structural
error (a non-array element, a non-string key, a non-integer version, a trailing token) and returns
`null`. The previous shape carried a `skipEntry` state machine that consumed a broken element's
remaining tokens and continued with the next one, so one malformed entry cost only itself. That
recovery was reachable only from a sender that emits invalid JSON or a version-skewed writer; the
batch is an invalidate tombstone set, and a dropped batch is re-covered by the next write,
periodic broadcast or hard TTL — applying a partially understood tombstone set is the worse
failure. Owner: `SyncMessage.parseBatchBody`.

**Put payload normalization is shallow and the allowlist is package-based.** A root JDK immutable
collection or view (`List.of`, `Set.of`, `Arrays.asList`, a `Collections` wrapper) is copied into
`ArrayList`/`LinkedHashSet`/`LinkedHashMap` before encoding, because those runtime classes have no
public constructor on the receiving side; a nested immutable collection inside a DTO is not
rewritten and may degrade that PUT to a versioned INVALIDATE. The previous shape normalized every
nested container through a custom `TypeIdResolver` and enforced a per-class container rule. The
copy is bounded before it happens: under `WRAPPER_ARRAY` a list or set needs at least `2 * size +
23` bytes and a map at least `5 * size + 23`, so a root container above `put-max-bytes / 2` (or
`/ 5` for a map) cannot fit whatever its contents, and `encode` refuses it without materializing
the copy. Without that check a cached multi-megabyte view was duplicated on the caller's write
thread — a GC spike per write — only for Jackson to abort 32 KiB into the serialization.
The package allowlist (`java.lang`, `java.util`, `java.time`, `java.math`, `zeta.model`, configured
packages) still gates deserialization, so an arbitrary classpath type cannot be instantiated from
a broker-writable frame. Owner: `PutPayloadCodec.normalizeRoot`, `encode`, `buildMapper`.

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
**If you add a mutation site, wire it here too.** The write-through tail drops the entry only when
its store applied: a refused write was superseded or tombstoned, so the newer mutation already
retired the flight it outranked, and invalidating unconditionally would kill a flight created
afterwards for the newer value. Hard expiry is not a mutation: a load already in flight is
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
outcome it belongs to. Reader failures are tagged with a private `ReaderFailure`, a plain
`RuntimeException` rather than a `CompletionException`: `CompletableFuture.get()` unwraps a
`CompletionException` before wrapping it in `ExecutionException`, which would strip the tag a
joining follower needs, and a reader-thrown `TimeoutException` would then be misread as the dedup
layer's own budget timeout. Only the dedup layer's own timeout and submission rejection stay bare,
so a reader-thrown `RejectedExecutionException` or `TimeoutException` is a `FAILED` load, not a
suppression. The join-side classifier unwraps exactly the join envelope, never the reader's own
cause chain. Owner: `LoadFlights`.

**A follower joins interruptibly; the reader's thread owns the interrupt flag.** The flight is a
`CompletableFuture`, and `join()` ignores interrupts — a follower of an inline leader (no timeout
budget) could not respond to shutdown or request cancellation. The join therefore uses `get()`:
an interrupt cancels the wait with `FAILED(InterruptedException)` and restores _that follower's_
flag, while the reader's own thread restores its flag only where it observed the interrupt (the
inline leader and the isolated loader). A waiter must never inherit an executor thread's interrupt
for a failure it merely observed. Owner: `LoadFlights.settle`, `runInline`, `loadIsolated`,
`HotKeyCache.resolveFailed`.

**A killed load and a completed load leave the table differently.** `LoadFlights` entries are
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
The field is mutable and `volatile` because every transition goes through a `VarHandle`:
`findVarHandle` on a `final` field yields a handle whose write modes — `compareAndSet`,
`getAndBitwiseOr`, `getAndBitwiseAnd` — throw `UnsupportedOperationException` on the first load, and
the plain reads behind `isInvalidated()`/`stamp()` need the same visibility as the writes. The
backlog and settlement bits are set and cleared with one `getAndBitwiseAnd`/`getAndBitwiseOr` each,
which returns the prior word: a compare-and-set loop bought nothing for a single-word update and
turned a contended settle into a retry storm.
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

**A batch elects each distinct key exactly once.** `LoadFlights.load(Iterable, ...)` skips a key
already present in its `elections` map before calling `elect`, so a duplicated key reuses its first
flight and outcome, the reader runs once, and one logical read is counted once. Electing per
occurrence would consume a second stamped `HALF_OPEN` probe in the window where the first election's
flight had already failed out of the dedup table: the second `elect` pays `tryAcquireStamp` before
the batch map can discard it, and no leader ever settles that stamp. The probe slot stays reserved
for the rest of the episode, shrinking the recovery quota; enough duplicate batches can pin recovery
at the one-probe floor and make a recovered source look permanently degraded. `computeIfAbsent` also
performs breaker admission inside the atomic insertion, so a caller that loses the election never
consumes a probe at all. Owner: `LoadFlights.load`, `LoadFlights.elect`.

**A settled flight is never finalized twice.** `finishFlight` returns immediately for a flight that
is already complete. The only way to reach it twice is a cancellation callback: `close()` and the
detach hook settle the flight first and cancel its task second, so the cancellation re-enters the
completion handler on the settling thread. That thread can be inside Caffeine's eviction
notification — a size or expiry eviction calls the listener from within the removal it is performing
for that key — so the re-entry would remove the very mapping being evicted, and a leader timeout
that arrives after the shutdown already answered the waiters with a suppression would still
increment the leader-timeout counter, the signal operators read as source degradation. A
single-caller tail keeps the flight's state, its backlog slot, its breaker outcome and its metrics
owned by whoever finalized it. Owner: `LoadFlights.finishFlight`, `submit`, `detach`.

**Submission cancels last.** The leader's `submit` re-checks the closed flag after handing the task
to the executor, and it retires the flight before cancelling the task — the same order `close()` and
the detach hook use. Cancelling first let the synchronous cancellation callback win the settlement
CAS, so a coordinated shutdown was classified as `FAILED(CancellationException)` for the caller and
recorded as a breaker failure instead of a released probe. Owner: `LoadFlights.submit`.

**A full clear invalidates flight by flight.** `invalidateAll` removes each key's flight through
`invalidate(String)` instead of one table-wide clear. Caffeine reports the explicit removal of a live
flight through the removal listener, never the eviction listener, so a table-wide clear dropped the
flight it removed from the table (and every joiner's future) without invalidating it or detaching
it, and the flight reached neither `detachedActive` nor `close()`'s snapshot. A load elected
concurrently with the clear — after the per-key traversal passed its key — was then owned by nobody:
`shutdownNow` discarded its queued task without completing the future and every thread joining it
blocked forever, with a HALF_OPEN probe still reserved. Per-key removal also leaves a load elected
after the traversal running, which matches `invalidatePrefix`: its stamp outranks the clear's, so its
fill is legitimate. Owner: `LoadFlights.invalidateAll`, `invalidate`.

**Shutdown fails every live flight.** Spring destroys `SingleFlight` before
`interruptibleLoadExecutor`, but the executor's `shutdownNow` drains queued tasks without completing
their `CompletableFuture`. Without a close hook a caller already joining one of those flights blocked
forever, `pendingLoads` never returned, and a HALF_OPEN probe slot stayed reserved. `close()` sets a
closed flag, removes every live flight, releases its backlog slot once when the load has already
claimed one, settles the breaker as abandoned and completes the future with
`RejectedExecutionException`, which the join classifier maps to `SUPPRESSED_EXECUTOR`. A load that
elects after the flag is set takes the same suppression, on both the single-key and batch paths. A
flight that leaves the table between the sweep's two snapshots — explicit invalidation or an eviction
notification racing `close()` — is retired by the detach hook's own `closed` check: without it the
flight sat in neither collection, and the executor's `shutdownNow` discarded its queued task without
completing the future, so every thread already joining it blocked forever. A task the executor starts
after its flight was settled exits before calling the reader: the flight is done, so the value could
never be committed, and running the reader only spent a downstream query during shutdown. The
election's own shutdown withdrawal also completes the inserted candidate exceptionally, not only
removes it and releases the probe: a follower could observe that candidate between the insertion
and the removal and join a future no leader would ever finish. The previous shape had no close
path. Owner: `LoadFlights`.

**One dedup table carries one payload shape.** The single-key and batch read paths both submit
`VersionedValue` carriers. They share one dedup table, so a single-key read and a batch read
racing the same missing key elect one leader and the other joins its future — with heterogeneous
payloads the joiner either threw `ClassCastException` or nested the carrier inside a fresh
carrier and cached that; sharing the carrier also gives the follower the leader's own pre-read
stamp (the stamp is the value's lower bound, not the joiner's later allocation). Owner:
`HotKeyCache.loadAndCache` (batch).

Owner: `LoadFlights`, `HotKeyCache` (`dropLocal`, `dropAndBroadcastInvalidate`,
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
(`getDelay <= 0`), not only when the record arrives inside the deferral window. A burst of
concurrent records must not queue on `scheduleLock` only to discover the flush is already pending;
that is exactly the write rate at which the buffer is busiest. Owner:
`BroadcastBuffer.ensureFlushScheduled`.

**The deferral window never slides, so it needs no cap.** The record that arms the flush fixes its
fire time; later records inside the window merge into the pending map and leave the armed task
alone. A `max-defer-ms` ceiling on top of that was unreachable in the shipped configuration
(`flush-delay-ms <= max-defer-ms`, so the armed flush fires no later than the cap) and wrong in the
reverse one, where a single record still waited the whole `flush-delay-ms`. It was not free
either: every `record` paid a `TimeSource.monotonicMillis()` call and a volatile read to maintain
an anchor that only the unreachable branch consumed. The property is gone, and the pending-count
and payload-byte caps remain the only reasons to expedite an armed flush.

**Crossing a cap re-arms the one pending flush; it never queues a task per record.**
`BroadcastBuffer.record` calls `ensureFlushScheduled(true)` after the merge and reuses the armed
future when one exists, so a burst across `MAX_PENDING_ENTRIES` or the payload-byte cap costs at
most one extra schedule. Calling `scheduler.execute(flushAndReset)` for every over-cap record
enqueued an unbounded number of tasks into the shared scheduler's queue; the reporter flush tick
and the sketch decay share that pool and must run before the queue can drain, so a write burst
delayed detection and reporting exactly when the buffer was busiest. Owner:
`BroadcastBuffer.record`, `BroadcastBuffer.ensureFlushScheduled`.

The approximate size/payload counters are decremented in the same bin compute that removes an
evicted entry, and evictions and records both hold the swap read side while a flush reset holds
the write side. An empty map therefore cannot look over its cap, and a record that loses its entry
to a flush swap finds size zero instead of arming a deferral window for nothing. A PUT/REFRESH
record also carries a currentness predicate evaluated under that read side twice — before the merge
and again after it. The two stores form a Dekker pair with the invalidation path, which records its
tombstone before evicting pending entries: either the invalidation sees the record and removes it,
or the record sees the tombstone and removes itself. A single pre-merge check left the window open,
because both sides hold the shared read side and neither waits for the other. A value-carrying PUT
also checks the predicate once before serialization: an already-dead payload is not built, while
the two in-lock checks remain the correctness pair.

**A REMOTE invalidation cannot cancel a PUT that has not entered the buffer yet.** REMOTE
deliberately records no local tombstone and leaves the local entry untouched, so the currentness
predicate cannot see it; a PUT serialized before the invalidation can still be merged after the
cancel sweep. A peer that received the INVALIDATE rejects the older PUT through its own per-key
tombstone; a peer that missed it converges through the at-most-once contract and its hard TTL.
Closing the window would require a second bounded per-key cancel watermark inside the buffer, or a
local L1 tombstone that changes REMOTE's local-fill semantics; the residual is bounded by the peer
version guard, so both were ruled out. Owner: `BroadcastBuffer.put`, `HotKeyCache.invalidate`.

**Shutdown drains the last deferral window synchronously.** `BroadcastBuffer` is destroyed before
`zetaSendExecutor`, whose own destroy drains for a bounded window; an asynchronous handoff on the
way out could still race that budget. `destroy()` swaps the pending map and sends inline, so the
last window (up to `flush-delay-ms`) is not lost on restart. A broker failure during that drain still
drops the batch — the buffer is lossy and the sync plane is at-most-once. Owner:
`BroadcastBuffer.destroy`.

**A flushed snapshot is chunked into detached carrier lists.** The send executor receives at most
`SEND_CHUNK_SIZE` (1,024) entries per submission, and each chunk is its own `ArrayList` of
`SendItem(key, VersionInfo)` records — never a `subList` view and never the swapped map's
`Map.Entry` objects. A view keeps the parent array, and a `ConcurrentHashMap` entry keeps a field
reference to its whole map, so either would let one surviving chunk pin every entry and payload of
the snapshot until the last chunk ran; the detached carrier releases the map and each sent chunk
as soon as its send returns. Chunks are submitted in iteration order, but per-key convergence is
version-guarded and order-independent. Owner: `BroadcastBuffer.submitSendChunks`,
`BroadcastBuffer.SendItem`.

**A broker send failure is counted where the batch is sent.** `CacheSyncPublisher.publishSync`
returns `false` on a broker error instead of logging one WARN per message, and
`BroadcastBuffer.send` sums the failures and emits one WARN per executor chunk. Before this rule
the publisher swallowed each exception and logged it with a full stack trace, while the public
broadcast methods returned `void`; a 1,024-key chunk produced 1,024 WARNs, and
`BroadcastBuffer.sendFailureCounter` stayed at zero because the catch around the void calls could
never fire. The `zeta.stall.broadcast_storm.stopped.total` meter therefore missed a real broker
outage while the log flooded. Invalidation publishes are one message each and log one WARN on
failure; their volume counter stays in the publisher. Owner: `CacheSyncPublisher.publishSync`,
`publishDeduped`, `BroadcastBuffer.send`.

**A refused send chunk strands only itself.** `BroadcastBuffer.submitSendChunks` splits the swapped
snapshot into independent `ArrayList` chunks of at most `SEND_CHUNK_SIZE` entries and submits each
chunk separately; when the send executor rejects a submission, `submitSendChunk` drops that chunk,
adds its size to `saturationDrops`, and every other chunk still ships. Submitting the whole snapshot
as one task made a single rejection — a saturated pool, a shutdown race, or a custom executor
throwing on submission — strand every pending PUT/REFRESH of the flush, up to
`MAX_PENDING_ENTRIES` records, so peers converged only through their hard TTL even though the
buffer had successfully flushed. Per-key convergence stays version-guarded and order-independent,
so chunk order adds no ordering requirement. Owner: `BroadcastBuffer.submitSendChunks`,
`BroadcastBuffer.submitSendChunk`.

**The batch is the unit of work, and it is reduced before anything is applied.** The receive side
does not preserve an origin's send order: `zeta.sync.concurrent-consumers` defaults to three AMQP
consumers that deserialize and submit concurrently, and the publisher uses separate channels per
message type, so even one origin's `[REFRESH v10, PUT v5]` can reach the dispatcher with the PUT
first. `KeyDispatcher` therefore hands a key's whole granted batch to
`Batch.reduce`, and the handler keeps only its highest-version message: the
final state of a key is decided by that one message, so every message it covers would be applied
and immediately overwritten. Reducing first means a superseded PUT never pays its payload
deserialization, a superseded REFRESH never runs the loader, and a superseded INVALIDATE never
takes a cache-bin lock. Every covered PUT/REFRESH/INVALIDATE still reports one terminal
`onSkipped(COVERED_BY_NEWER_EVENT)`. The highest covered INVALIDATE is also the fallback removal:
when the winner's load yields nothing, that removal is the winning value event's degraded effect
(`afterValueDegraded`); when the policy refuses or fails the winner before a write, the covered
removal applies at its own version. Its effective version is resolved together with the winner's,
covered first in ascending order, before the first policy call: resolving it lazily after a refusal
could mint a stamp above a local write that landed while the winner was being decided, and the
fallback would then evict that newer write. The fallback still hard-removes a Worker-managed entry
when the batch carried an explicit INVALIDATE — serving the pre-invalidation value would contradict
an instruction the same batch already carried. Previously the reduce kept only the highest
covered INVALIDATE outside `covered`: lower covered INVALIDATEs lost their terminal callback,
and a refused value winner left the covered removal unapplied.

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
using `computeIfPresent` for the guarded removal (no separate `getIfPresent` before the bin lock, no
per-key `Applied` or flag allocation: one reusable outcome holder, reset before each key's mutation,
records the committed result because a contended compute can retry). Each rule keeps its own stamp:
collapsing a scoped clear
into the table-wide one, or two prefixes into their maximum, would evict entries written
legitimately after the lower-stamped clear. Ordering against per-key work is unaffected either
way — anti-resurrection is carried by `EntryLifecycle`'s watermarks, not by the dispatcher.
Owner: `CacheSyncListener` (`CLEAR_DISPATCH_KEY`), `DefaultSyncContext.applyClearBatch`.

**A refusal is applied on the consumer thread, without hooks.** The sync plane acks before the
ordered dispatcher runs, so a message the dispatcher refuses (per-key queue full, global budget
exhausted, listener closing) would otherwise leave a stale L1 entry for its whole hard TTL. The
fallback therefore applies it immediately: a clear records its watermarks and kills the pre-clear
flights, then hands the table walk to the sweep executor (inline only when that executor refuses);
anything else is an O(1) in-memory invalidation. Neither touches the cluster loader or a user hook,
so the thread that must keep acking cannot stall. The clear's affected-entry count is committed
after each key's `compute` returns, because a contended bin can retry the remapping function; a
counter incremented inside it over-reports `afterClear`. Only an entry that was still live counts: an
entry past its hard deadline is removed and its warm task cancelled, but the clear did not take a
servable value away, and counting it inflated `afterClear` by every expired resident the sweep walked
past. The refusal path is one call — `fallbackEvict(SyncMessage)` — instead of rebuilding a batch,
a `Batch.Clear`, a `TreeSet` of stamps and a `ClearMarks` for a single message. Owner:
`CacheSyncListener.onDispatchRejected`, `DefaultSyncContext.fallbackEvict` /
`dispatchClearFallback`, `sweep`.

**A refused first drive publishes no worker; a refused re-drive retires it.** `submit` hands the
new worker to the executor inside the same per-key `compute` that would publish it, so a concurrent
submit cannot append behind a cycle the executor has not accepted. A refused first drive leaves no
map entry and returns `false`, letting the caller apply its inline fallback. A refused re-drive is
submitted from inside the retire compute: it drains the next grant, and on rejection discards that
grant plus the queued tail, removes the worker and logs at WARN, so the key can be rescheduled. The
previous shape published the worker before `executor.execute`, so a submit racing the start window
could enqueue, return `true`, and lose its task when the creator's start was refused and `abandon`
discarded both. No scheduling flag is needed. The executor must not run a submitted task inline
(no `CallerRunsPolicy` or direct executor): the first submission and every re-drive hold the key's
bin lock, and an inline run would re-enter that key's `compute`. Owner: `KeyDispatcher.submit`,
`startWorker`, `submitCycle`, `run`.

**A close that wins after the executor accepts the first cycle still retires the worker.** `close()`
sweeps the map without the key's bin lock, so a worker accepted by the executor inside `submit`'s
compute can be published after that sweep. `startWorker` re-reads `closed` after the executor
call, and `submit` re-checks after the compute: a worker this call created is removed again and its
parked future is cancelled. Before both checks the worker stayed in the map with an uncancelled
jitter future after `close()` cleared it — the PUT payload remained pinned for the rest of the
jitter delay while the message had already been acked and its local invalidation fallback was
skipped. Owner: `KeyDispatcher.submit`, `startWorker`, `close`.

**Only an unclaimed first cycle is reported refused.** Retirement is unconditional on the map and
the parked future, but the boolean it reports also decides whether the caller applies its inline
fallback, and that fallback is destructive: it records a tombstone at the message's version and
removes the resident, so applying it over a batch that already ran erases the value the batch just
wrote and then refuses the redelivery at the same version. `submit` therefore refuses only by winning
`abortFirst` while `KeyWorker.firstState` is still pending; the worker thread claims that same state
(`claimFirst`) before it consumes its pre-granted batch. Every exit that drops a cycle — the top
`closed` gate, the merge compute, the second `closed` check — sits before the claim and leaves it
pending, so the refusing caller is right that the batch will not run; once the claim is won nothing
drops the batch and the caller is told the truth. A plain `started` flag cannot carry this: the
writer sets it after the reader has already loaded a stale value, so one interleaving still reports
`false` for a batch that runs. Owner: `KeyDispatcher.submit`, `startWorker`, `run`,
`KeyWorker.claimFirst`/`abortFirst`.

**The first batch is pre-granted; a queued tail is folded into it at the first cycle.**
`KeyWorker` is created with its first task already in a `Grant` (task list plus budget weight),
so a lone first task pays no grant `compute` before the consumer call. If submissions arrived
while the start-jitter park held it, the first cycle folds that queued tail into the same grant
under the bin lock, preserving the old coalescing (one consumer call, one sweep, one loader
read); a lone task still skips the compute. The `volatile hasQueued` flag is written under the
same bin lock and read by `run` outside it, so the first cycle can skip the merge compute without
touching the `ArrayDeque` off-lock; the deque itself never escapes the lock. The merge also clears
the worker's `first` flag under that lock, so a concurrent enqueue landing between the merge and
`run`'s volatile write cannot count the already-granted first task a second time against the
per-key bound. The previous helper read the non-thread-safe deque outside the bin lock: a stale
`false` skipped the merge and left a jitter-window burst for a later cycle. Every later batch is
drained and re-submitted at the end of
the previous cycle, inside the retire compute, so a sustained key costs one `compute` per cycle
instead of two and the executor's acceptance is atomic with the grant's publication. The first
cycle's `ScheduledFuture` is stored on the worker and cancelled by `close()`; the constructor
enables remove-on-cancel on a `ScheduledThreadPoolExecutor` and `close()` purges as a fallback, so a
parked worker does not pin its payload for the remaining jitter delay. An enqueue that completes
`maxTasksPerCycle` does not cancel the park to drive the batch early: the park is bounded by
`jitterMs` (50 ms by default), so a full first batch waits out the remainder and the first cycle
still folds the queued tail into one grant (up to `maxTasksPerCycle`). The previous shape cancelled
the parked cycle and submitted it immediately; a refused immediate submission then had no driver
left for the worker,
so it discarded the grant and every queued task — including the messages whose `submit` had already
returned `true`, which therefore never received their one terminal callback. Only the task that
completed the batch reached the caller's rejection fallback. The `Grant` reference is cleared as soon
as the cycle takes it: keeping it through the consumer's synchronous load and the next park pinned every payload (a
PUT body can be 32 KB) in the dispatcher. The worker also implements `Runnable`,
so a cycle submission allocates neither a closure nor a wrapper. `close()` resets the global unit
counter with the map that held the queued weight; an in-flight `run` therefore skips its `finally`
subtraction when `closed` is set. The check and the subtraction are not atomic, so a settlement can
still race the reset; that value is unobservable because no admission reads the counter after close
(see _Ruled out_). Owner: `KeyDispatcher.KeyWorker`, `run`, `close`.

**Per-key FIFO with batched consumption.** `KeyDispatcher` runs up to
`maxTasksPerCycle` same-key tasks per submission and yields a busy key so other keys progress.
Queue management is one atomic `compute` per operation (no orphan-queue race by construction);
weight is `1 + bodyBytes/1024` per task, bounded per key and against one global unit budget, and
an over-cap submission is rejected at the door rather than queued and later discarded. Control
submissions (a COOL, a clear) skip the global budget: an O(1) message that repairs the state a
payload storm created must not be locked out by it. A granted batch is handed to the consumer as
one list — the boundary the reduction above works on. The constructor rejects a non-positive
per-key bound, per-cycle cap or global pending budget, and `submit` rejects a null key/task before
admission: a zero cap grants an empty batch forever, a zero bound refuses every task behind the
pre-granted first one, and a zero budget silently refuses every non-control task. The budget check
subtracts the task weight from the cap instead of adding it to the sum, so a configured budget near
`Long.MAX_VALUE` cannot overflow into admission. Both the first-merge compute and the retire compute
return the map's current worker unchanged when it is no longer this worker, so a successor is never
deleted as a side effect of the old cycle finishing. A consumer failure is reported through
`ZetaExceptionHandler`; a fatal `Error` retires the key (dropping its queued tail) before
propagating, and an `InterruptedException` found in the cause chain restores the thread's interrupt
flag.

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
against the health view's alive Workers and returns the unchanged published set on a lock-free,
allocation-free fast path when membership did not move: `matchesAliveWorkers` compares the records
against the snapshot's own set in one walk, instead of materialising a `HashSet` per call only to
throw it away after one `equals`. Only a real change takes the rebuild monitor, and the alive set is
re-read there — never reused from the fast-path probe — so a caller that waited cannot publish or
return a set sampled before another writer's newer snapshot. `locateNode(key,
healthView)` samples the monotonic clock once per call and probes each physical node by ID against
that instant instead of sampling per hop. The reporter starts on the snapshot-only
`locateNode(key)`: it routes on the ring's current published snapshot and never re-probes liveness
for the common case. A node dying after the reconcile still receives the batch, and the consumer
discards it through the incarnation check. If a concurrent rebuild between the admission's
alive-set capture and routing moves the key to a node whose epoch was not captured, the reporter
re-runs the admitted-target walk to keep the counts on a captured node; only a key with no
captured node left is dropped as unroutable. The predicate and health-view overloads remain for
callers that route against a state they did not just reconcile (the ghost-COOL guard, which
reconciles the ring from its health view in its lock-free pre-check, immediately before the
ownership probe). The common fast
path (the start node is alive) allocates nothing; a walk that starts on a dead node allocates one
`long[]` bitmap indexed by physical node, so each distinct node is probed at most once regardless
of cluster size. Owner: `HashRing`, `ClusterHealth`, `KeyReporterImpl`.

**Inspection reconciles before listing when a health view is wired.** `/actuator/hotkeyring` calls
`rebuildIfPossible` on every read when the endpoint has a health view. The reporter tide is the
routing rebuild, but an operator asking for topology must not see a Worker that died since the last
flush; the endpoint is the observability path and owns the refresh that makes its answer current.
Without a wired view it reports the current published snapshot without rebuilding: an empty
liveness set would empty a ring the endpoint does not own. Owner: `RingEndpoint`.

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

**`refreshAll` joins the tasks it accepted before rethrowing.** Submission and execution are two
failure domains: a rejection, or an `Error` thrown by the executor while the fan-out is still being
submitted, must not let the call return while keys it already dispatched are still refreshing in
the background, or the caller's failure path observes a cache mid-batch and a retry races the
abandoned batch. The loop records the admission failure, stops on `RejectedExecutionException` or a
fatal `Error`, and a `finally` joins every accepted future before the first failure is rethrown;
the rest are attached as suppressed, and a failed attachment is ignored so diagnostics cannot
replace the primary cause. The previous shape caught only `RuntimeException`, so a submission-time
`Error` skipped the join entirely and returned with accepted tasks still live.

A single-entry batch runs inline on the calling thread: a task, a queue hand-off and a future would
add latency without parallelism. A fan-out task that catches an `InterruptedException` from a
sneaky-throwing supplier restores the interrupt bit before recording the failure, so the pool thread
does not silently lose a cancellation signal while the batch keeps settling.

**The fan-out runs at the transaction boundary, not on the caller's thread.** A transaction-bound
`refreshAll` defers the whole batch through `TransactionSupport.run(false, …)`: a fan-out task runs
on a pool thread with no transaction context, so dispatching before commit would publish and
broadcast a value the transaction can still roll back. Validation and the loader snapshot stay
eager (they are caller errors, not per-key failures); outside a transaction the same wrapper runs
inline, and the after-commit depth guard keeps each per-key refresh from deferring a second time.
Owner: `DefaultZeta.refreshAll`.

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
retained as a second field. Owner: `InterruptingAsync`, `LoadFlights`.

**The refresh semaphore must stay below the load pool.** Background refreshes and read-miss
loads share `interruptibleLoadExecutor`, so a `refresh-max-pools` value at or above the pool's
max size bounds nothing: refreshes take every thread and read-miss loads queue behind them. The
default derives half the effective load-pool maximum (`refresh-max-pools = 0` →
`max(1, loadMax / 2)`), and a configured value above it is clamped with one WARN — the pool, not the
semaphore, is the real bound. The earlier default (100 against a 32-thread pool) read as a limit
while providing none. Owner: `ZetaProperties.effectiveRefreshMaxPools`, `ZetaAutoConfiguration`.

**The load and send pools have their own size knobs.** Both inherit the shared
`executor-*-size` defaults, but a slow data source needs a wider load pool while the send pool's
queue capacity is the bound on RabbitMQ channel consumption. Sharing one pair meant widening the
load pool also enlarged the send pool and pushed more channels at the broker; the
`load-executor-*` and `send-executor-*` overrides keep the two decisions independent. `0` is the
inheritance sentinel, and each effective pair is validated before a pool is constructed. Owner:
`ZetaProperties`, `ZetaAutoConfiguration.newPool`.

**The send pool drains before it is forced down.** An immediate `shutdownNow()` on context close
discards queued INVALIDATE/PUT broadcasts that were submitted after a committed transaction — the
other nodes keep stale entries until the hard TTL for no reason. `zetaSendExecutor` therefore uses
`ElasticThreadPool.shutdownGracefully()`: stop admission, await termination for three seconds, and
only then force the remainder. The budget is short enough that a wedged broker cannot delay
shutdown indefinitely. Owner: `ElasticThreadPool.shutdownGracefully`,
`ZetaAutoConfiguration.zetaSendExecutor`.

**`ElasticThreadPool` uses Tomcat's core→max→queue→reject ordering.** The JDK's
core→queue→max ordering lets a bounded queue starve the growth the pool was sized for. Its
`submittedTasksCount` admission accounting is symmetric on every path that never reaches
`afterExecute`: rejection (including a handler that throws an `Error` — the compensation is a
`finally`), a non-rejection failure such as a throwing `ThreadFactory` or `Thread.start` failing,
`remove`, `purge` (each cancelled queued `Future` is removed and counted), and `shutdownNow`.
`setMaximumPoolSize` re-derives the admission ceiling; the queue reads `getMaximumPoolSize()` from
the executor on every thread-creation decision, so runtime resizing needs no mirror field to keep
in sync.

**The submission counter is balanced by the one site that owns each path.** A submission can die in
three places, and exactly one of them subtracts it. The internal rejection handler subtracts _after_
the user handler returns: a returning handler (`CallerRunsPolicy` — which
no longer runs inline once the pool is shutting down — or a discard policy) leaves the counter
balanced, while a throwing one (Abort, or an `Error` escaping an inline run) never reaches that
line and `execute`'s finally rolls the same submission back; a task the handler forced into the
queue is neither, and the later `afterExecute` balances it. `remove` subtracts a task it actually
dequeued, because that task can never reach `afterExecute` — except in
`ThreadPoolExecutor.execute`'s shutdown recheck, which calls the virtual `remove(command)` only to
`reject(command)` on the same thread and hands that decrement to the handler above. Without the
exception one submission was subtracted twice, and the counter under-counted its in-flight tasks on
every removal in the shutdown window. The recheck is recognised by its caller frame, and only after
`isShutdown()` already answered true — the JDK calls `remove` from `execute` in no other state — so
the walk runs on the shutdown path alone. The previous shape kept a per-submission `ExecFrame` in a
`ThreadLocal`: a frame object plus a `ThreadLocalMap.Entry` allocated, a `remove()` executed and a
stale-entry expunge run on every ordinary submission, for a discriminator needed only while the
pool is closing. Owner: `ElasticThreadPool.execute`, `remove`, `InternalForceQueueHandler`.

**`purge` iterates the queue instead of snapshotting it.** The JDK's `purge()` removes cancelled
futures through the queue iterator and never calls `remove(Runnable)`, so the override must
compensate them itself. It walks the queue's weakly consistent iterator — no `toArray` array
allocated on every call, including the empty and the nothing-cancelled cases — and subtracts only
the removals `queue.remove` actually won; a worker or a concurrent purge that won the race owns the
other compensation. The JDK iterator's own `remove()` stays unused: it returns `void`, so it cannot
report which side won the unlink, and decrementing on it could under-count a task a worker had
already taken. Owner: `ElasticThreadPool.purge`.

**The admission ceiling saturates.** `queueCapacity + maximumPoolSize` is computed as a `long` and
capped at `Integer.MAX_VALUE`, and a negative capacity is rejected. A plain `int` sum overflowed to
a negative ceiling for a large `queueCapacity`, and the over-cap pre-check then rejected every
submission. Owner: `ElasticThreadPool`.

**Force-and-shutdown share one lifecycle lock, and a forced task gets a consumer.** `ElasticTaskQueue.force`
checks `isShutdown()` and offers in one critical section; `shutdown()` and `shutdownNow()` take the
same lock around the STOP/SHUTDOWN transition. Without the `shutdown()` override, a forced task
could slip into the queue after the last worker exited and before the pool reached `TERMINATED`, never
run, and leave its admission count permanently incremented. After a successful insert, `force`
starts a core worker when no worker exists; if the pool has no core thread to start and no live
worker, it removes the task and returns `false` so the caller's handler runs instead of leaving an
undrainable queue behind. Owner: `ElasticTaskQueue.force`, `ElasticThreadPool.shutdown`.

**`offer()` decides from lock-free counters first.** `ThreadPoolExecutor.getPoolSize()` takes the
pool's single `mainLock`, so calling it on every submission serialises the lock-free transfer
queue behind one monitor. The queue therefore short-circuits twice without it: fewer outstanding
tasks than core threads means an idle core thread (queue), and a fixed-size pool (`core == max`)
has no growth step to force (queue). In the elastic range it first tries a direct transfer to a
worker already waiting in `take()`/`poll()`: a successful transfer proves the queue was empty and
`submitted <= poolSize`, so the size read could not have changed the decision from "queue" to
"grow" — it only removes the `mainLock` acquisition. Only a failed transfer reads the pool size,
where the answer can actually change the decision. Owner: `ElasticTaskQueue.offer`.

**The internal force is a boolean, not an exception.** `ElasticTaskQueue.force` returns
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

Owner: `ZetaAutoConfiguration`, `ElasticThreadPool`, `ZetaExceptionHandler`.

---

## Scheduling

**Fixed-rate work is phase-anchored.** `PeriodicScheduler` keeps the cadence fixed to
the original phase; a run that overshoots skips every missed slot and lands on the next slot of the
original grid — no drift, no catch-up burst, and a slow run does not permanently shift the phase. `scheduleWithFixedDelay` is completion-relative. A task exception is routed to
the handler chain and never kills the cadence; `remove(originalCommand)` stops a periodic chain
by the original task. The chain's future reports `isDone()` only at terminal end — cancelled, or
ended by executor shutdown — never per-link: judging the current link would read done between a
link finishing and the next being scheduled, flickering on every healthy chain. `get()` waits for
that terminal state: a healthy chain has no per-run result, so it blocks until cancellation or
shutdown, exactly like the JDK's periodic future. The previous shape delegated to the current
link's `get()`, so it returned `null` while `isDone()` was still false; a generic `Future`
consumer — a timeout wait, `invokeAll`, a test harness — read that as completion and acted on a
live chain. A caller that needs "wait for one run" owns a latch in the command, not the chain
future. Owner: `PeriodicChain.get`.

**A chain publishes its next link before a worker can see it.** `PeriodicScheduler`
overrides `ScheduledThreadPoolExecutor.decorateTask` and installs the chain's `current` reference
there: the JDK calls that hook synchronously before `delayedExecute` enqueues the wrapper, so the
link is visible through `current` before any worker can run it. Before the hook existed,
`scheduleLink` assigned `current = schedule(...)` after the call returned. A zero-delay link could
start and schedule its successor before that assignment, and the scheduling thread then overwrote
`current` with the completed link — cancellation and `getDelay()` targeted the dead link while the
live successor waited in the delay queue. `scheduleLink` no longer assigns the return value, and a
rejected initial schedule clears the hook-installed wrapper instead of leaving `getDelay()` on a
link the queue never accepted. Owner: `PeriodicScheduler.decorateTask`,
`PeriodicChain.scheduleLink`.

**A chain's lifecycle is one 2-bit code, and its mode is the sign of the gap.**
`PeriodicChain` keeps `ACTIVE`, `TERMINATED`, `CANCELLED_NO_INTERRUPT` and
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
queue. A run already in progress is allowed to finish; only the next link is cancelled. A
`closing` flag is set before the registry sweep and re-checked by `scheduleLink`, so a registration
that races shutdown either lands in the sweep or refuses to queue its first link; neither path can
leave a chain queued past the shutdown call.

**Control plane and data plane do not share a scheduler.** On the Worker, heartbeat emission
runs on a dedicated single-thread `workerControlScheduler`; `hotKeyScheduler` keeps the data-plane
work that performs synchronous AMQP sends (stale-eviction broadcasts). Sharing one pool let a broker stall plus a
mass-cooling wave occupy every thread, starving the heartbeat and making Apps evict a healthy
Worker from the ring — the connection-level isolation already in place was defeated at the thread
level.

**A maintenance tick is registered only when its collaborator exists.** The optional references
are resolved once when the configuration is constructed; absent L1 table, engine or TopK means
the corresponding `scheduleWithFixedDelay` call is never made, and a present tick never re-tests
the absence or unpacks a container handle on the scheduler thread. The returned futures are kept
and cancelled in `@PreDestroy`, so a host-supplied `hotKeyScheduler` that outlives the context
cannot keep running against a destroyed cache, reporter or sketch. The expelled drain is a
memory-protection pass, not an event stream: production calls `clear()` on each queue (no batch
list, no per-item copy), and only DEBUG drains a bounded sample for the summary line.

Each tick runs behind one guard that forwards a throwable to `ZetaExceptionHandler` — the single
failure sink every Zeta executor uses — instead of logging a private ERROR and swallowing it. The
guard exists for a substituted plain `ScheduledExecutorService`, whose JDK implementation cancels a
fixed-delay task after its first uncaught exception; on the shared `PeriodicScheduler` the chain's
own wrapper already reports there, so the guard never fires. A private catch happened to be the
only owner of the failure: the configured handler chain — the documented Micrometer/alerting hook —
never saw a maintenance failure, and an operator who registered a handler still had to grep for a
second log format to find it.

Owner: `PeriodicScheduler`, `ZetaSchedulingConfiguration`, `WorkerAutoConfiguration`.

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
to, and the engine's failure path (WARN plus a closed retry window) must see a real failure — a
sentinel the admission filter declines looks like "no new value", which silences the failure and
re-runs the body on every stale read throughout the outage.

**The refresh bypass is keyed by the resolved plan, not by a method signature.** A reflective
refresh that can only invoke a proxy arms `ThreadLocal<RefreshBypass>` so the inner advice call
reaches the body instead of serving the entry being replaced. The old marker stored the target and
two `Method` views and compared them with `BridgeMethodResolver` plus an exact parameter-type
array. An interface never generates a bridge, and a multi-level generic hierarchy
(`Base<T> → Mid<N extends Number> → Leaf(Long)`) can make the proxy's reported view and the plan's
most-specific view unrelated under that comparison; the bypass then missed, the refresh re-entered
the advice and served the stale value. Every view of one method group already shares one
`CachePlan` reference in the plan table, so the marker stores `(proxy, plan)` and normally answers
with one identity comparison; a plan from a second table — a warm spec scanned for the interface
while the call resolved the implementation table — compares both `specific` methods through
`PlanResolver.sameMethod`, which resolves each against the proxy's ultimate target class. Owner:
`ZetaCacheInterceptor.invoke`, `RefreshBypass`, `PlanResolver.sameMethod`,
`AnnotatedWarmLoaderBinder.AnnotatedWarmLoader`.

**A failed scan publishes no warm loader and no half-registered contract.** `readPlan`/`writePlan`
collect their namespace contract into the scan's `BuiltPlans` instead of registering it, and
`scanRoots` publishes the batch only after every group of that class has resolved and
`publishWarmSpecs` has accepted the warm specs: a later method's condition, key, TTL or
permanent-sync failure used to leave an earlier method's TTLs and triggers in the registry, so a
namespace no plan served still carried a TTL regime and prewarm trigger. `publishWarmSpecs`
validates the whole batch against the existing map under one lock and only then writes it into an
insertion-ordered map: a second warmed method that conflicts with another class used to be stored
before the conflict surfaced, and concurrent scans could interleave two partial publications. The
registry's trigger merge runs before its TTL write for the same reason — a conflicting trigger must
not record the namespace's baseline. The scan’s key-format declarations are staged the same way:
`NamespaceKeys.Staging` records what each namespace resolved to and publishes nothing until
`scanRoots` has accepted every group and every warm spec, so a method whose key expression fails
after its namespace was registered no longer leaves that namespace in the registry with a format
no plan uses — `MethodResolution.encoding` used to write `NamespaceKeys.byName` directly during
the build. The commit re-resolves each staged entry under the registry lock and fails when a
concurrent registration moved a pinned namespace’s format. The publication is atomic for a scan
that fails validation, not for a registry rejection inside the batch: `registerAnnotatedNamespace` still writes contract by
contract, so a conflicting trigger on the second of three contracts leaves the first behind. Owner:
`PlanResolver.scanRoots`, `PlanResolver.BuiltPlans`, `PlanResolver.PendingContract`,
`PlanResolver.publishWarmSpecs`, `NamespaceKeys.Staging`,
`NamespacePolicyRegistry.registerAnnotatedNamespace`.

**An `eachKey` single argument avoids the SpEL context.** A `key = "#ids"` invalidation with
`eachKey = true` used to build `ZetaEvaluationContext`, its root and a `TypedValue` on every call
just to read one argument. `Invalidate.Operation` records the argument slot when the expression is
a whole variable (and not a varargs tail), so resolution reads `args[index]` directly; only an
expression with a receiver or `#result` still needs a context. Expansion also returns a one-element
`List.of(...)` without allocating a `LinkedHashSet`, iterates a reference array directly, and a
one-element `Set` skips the temporary encoded-part list and sort. `Key` no longer copies an erased
array into its declared component type: every reference array encodes as `aO`, and PLAIN forbids
array keys, so the copy changed no output byte. Owner: `CachePlan.Invalidate.Operation`,
`CachePlan.Key`, `DefaultKey.collection`.

**The plan's hot-path carriers are monomorphic.** `Key`, `Ttl`, `CompiledExpression` and an
invalidation `Operation` are final classes, not sealed hierarchies of one-field records. An L1 hit
calls `key.resolve`, a dynamic TTL calls `evaluate`, and a guard calls `evalAdmission` or
`evalVeto`; with four
implementations each, C2 passed its polymorphic-inline threshold and the calls stayed megamorphic.
One final class per carrier keeps the call site monomorphic, removes the empty `Absent`/constant
record shapes, and makes the `Read` flag word unnecessary (`optional` is a field, `hasUnless()` is
`!unless.absent()`). The three verbs stay records: `Read`, `Write` and `Invalidate` are genuinely
orthogonal states, and merging them would replace the sealed check with nullable fields. Owner:
`CachePlan`, `CompiledExpression`.

**An explicit key encodes one value exactly like one default-key argument.** An absent key over a
single-argument method encodes the canonical `DefaultKey` form (`[s5:alice]`), while `key = "#id"`
used to concatenate the raw value (`alice`), so a query and an invalidation written against the
same argument addressed different entries and the removal silently missed. The same split made
`key = "#root.args"` wrap the whole argument list in an `aO[...]` part instead of using the
multi-argument default form. `Key` routes every explicitly selected value through
`DefaultKey.encodeSingle(namespace, value)`; a whole `#root.args` is recognized from the
parsed AST and uses the argument-list encoding, so it matches an absent key for the same call.
`Key` is one final class with four exclusive kinds — a constant physical key, one argument, the
argument list, or a SpEL expression — so a constant key with an argument index or a retained
expression on a constant is not representable; the kind is chosen once at resolution. A
context-free expression is folded to a constant physical key there, and a constant `null` fails
startup instead of throwing on every call. A single final class keeps the hit-path `resolve` call
monomorphic rather than splitting it across four record implementations.
Arrays are still encoded by content, not by the identity hash of `Object.toString()`; a varargs
tail stays off the single-argument fast path so the value lookup can return the whole array (or the
expanded arguments) intact. The lookup normalizes every shape to a component-typed array: a packed
tail is returned as-is, an expanded tail is copied with `java.lang.reflect.Array` (so a
`String...` always yields `String[]`, never an `Object[]`), one expanded value is wrapped rather
than returned as a scalar, and zero expanded values yield an empty array instead of `null`. A
generic interface erases `T...` to `Object[]`, so a call through that view hands the implementation
an `Object[]` container even when it declares `String...`; that container is copied element by
element into the declared component type before the key is evaluated or a refresh is packed. The
same normalizer adapts every non-varargs array parameter (`T[]` erases the same way), not only the
last one, and the adaptation is recursive: a `String[][]` parameter accepts an `Object[]` of
`Object[]` rows. Without that copy a generic `find(T[] ids)` left the first parameter as
`Object[]`, so the reflective refresh failed with `IllegalArgumentException: argument type
mismatch`, and a `String[]...` tail failed earlier in `Array.set` with `ArrayStoreException`. A
nested tail counts as packed when every element is already the declared row type or is an
`Object[]` row that the same recursive copy adapts; a row of a primitive-array component type can
only arrive erased as an `Object[]`, so asking whether that row's own component type was primitive
read a packed `int[]...` tail as one element and `Array.set` failed with
`IllegalArgumentException`. The same normalizer packs the arguments before a reflective background
reload, because `Method.invoke` requires exactly `parameterCount` arguments with every array
element assignable to its declared component type. A custom `MethodInvocation` may report fewer arguments than the method
declares; the fixed-slot loop stops at `args.length`, so a multi-parameter varargs method cannot
throw `ArrayIndexOutOfBoundsException` before a refresh, while the missing fixed slots stay `null`
and a missing tail becomes an empty array. A reported tail that is not a plain `Object[]` is one
element of the declared component type, so a single empty row of a `String[]...` parameter stays one
row instead of collapsing to zero: the erased packed shape is exactly `Object[]`, while any other
array class is a row that `Array.set` was about to store. `DefaultKey` sizes the builder for a single `String`
value from its text length (the tag, the worst-case decimal length prefix and the brackets), so a
UUID key does not grow and copy the builder on every hit; the single-argument `encode` path uses the
same estimate, while the other scalars fit the 24-byte default (a `Long` is the widest). A
multi-argument key is sized from the same facts — prefix, one allowance per argument and the full
text length of every `CharSequence` — because the fixed 32-byte slack made a two-id key copy the
prefix and the first id on the first append; an under-estimate costs one `StringBuilder` growth,
never correctness. Owner:
`CachePlan.Key`, `CompiledExpression.wholeRootArgs`, `ParameterBindings.pack`,
`ParameterBindings.varargsTail`, `ParameterBindings.adaptArgument`, `DefaultKey`.

**The plan binds to the group's most-specific method.** A JDK proxy reports the interface
`Method`, which may carry no parameter-name metadata and has no body, so `#root.method` and a
refresh need the group's most-specific method rather than the reported view.
`BridgeMethodResolver.getMostSpecificMethod` therefore runs once per candidate, followed by
`BridgeMethodResolver.findBridgedMethod`: a generic parent interface's erased view binds to the
child override that actually carries the verb, so a parent-interface call and a child-interface
call share one plan instead of splitting into an annotated child group and an unannotated bridge
group. The guard belongs to the scan root, not to the resolved method's declaring type:
`getMostSpecificMethod` answers with whatever that root dispatches to, and for a body inherited from
a superclass that implements no view that is the superclass's method — `UserServiceImpl extends
BaseService implements UserApi` resolves `UserApi.find` to `BaseService.find`. Rejecting a result
whose declaring class does not implement the view kept the interface method instead, so
`#root.method`, the reflective refresh and the self-warm spec named a method with no body, and which
view won depended on the traversal order. The resolved method is handed to plan resolution, so the
same `Method` serves the annotation probe and the cached plan instead of being looked up again
inside the mapping function;
a call that reaches the interceptor without a prior pointcut match (a manually built proxy)
resolves it on the first call only. Its result is stored in the plan: `#root.method` is it, a
background refresh invokes it, and it is the fallback source of parameter names; the names
themselves come from the annotation-declaring view (next paragraph). A JDK proxy class borrows its
business interfaces' plan table: `Proxy.isProxyClass` routes the scan to them, so the proxy's
generated final methods (which carry neither annotations nor bridge flags) never become the
resolution key and one interface is not compiled twice; the proxy's own entry shares the
interface's immutable plan map. Owner: `PlanResolver.classPlans`, `PlanResolver.proxyPlanTable`.

**The expression's parameter names come from the method that declares the annotation.** A key is
written against the interface or base method that carries `@ZetaCached`/`@ZetaPut`/
`@ZetaInvalidate`, so that method owns the names: an interface `load(String userId)` and an
implementation `load(String id)` must not make `key = "#userId"` fail startup as an unknown
variable. `parameterNames` therefore prefers the view that declares the verb annotation, then the
most-specific method when the declaring view has no metadata, then any remaining view. The
plan is held in a per-user-class table, so one interface method with two implementations resolves
two plans. When the reported method and the most-specific method differ, the same plan instance is
stored under every view that contributed it.

**Resolution validates the whole method, not the last view.** A method can be reached through the
implementation, a generic bridge, a generic interface and several unrelated interfaces, and
`Class.getDeclaredMethods()` order is unspecified. The scan therefore groups every candidate method
by its bridge-resolved most-specific method first and resolves the group once. Interface-declared
specifics are folded by signature before that grouping: an interface generates no synthetic bridge,
so two super-interfaces that both declare `String m(String)` keep two distinct `Method` objects even
when a child interface extends both without redeclaring the method. The fold keeps the most specific
declaration of that signature: a sub-interface dominates its parent, a covariant return type
dominates the wider one, and an exact tie keeps the first view. Without that rule two unrelated
interfaces declaring `Object m(String)` and `String m(String)` made the plan's `specific`, and
therefore `#root.method`, depend on interface order, so a key written as
`#root.method.declaringClass.simpleName` addressed two different entries for the same call. The
previous shape grouped on the raw `Method`, so `@ZetaCached` on one sibling and `@ZetaPut` on the
other never met in one `resolveVerb` call and the startup conflict check disappeared; when only one
sibling carried the verb, the other view was absent from the plan table and a call through it
silently bypassed the cache. Owner: `ZetaCacheInterceptor.resolveProxyPlan`,
`PlanResolver.groupViewsBySignature`. A compiler-generated
bridge is never a parameter-name source: it can carry synthetic metadata and it dominates the
interface it bridges, so including it in the fallback could outrank the real interface names with
`arg0` and break `#userId` bindings. The fallback skips every bridge and scans only source views. A verb on the
implementation method still wins over the same verb on a view when the implementation dominates
that view, because the maximal-view comparison drops the dominated declaration; an inherited
superclass method and a newly implemented interface are unrelated, so their attributes must agree.
Every verb is read with `MergedAnnotations.from(method, SearchStrategy.DIRECT)`, not a
type-hierarchy search. A method that declares no annotation at all is skipped before that scan: a
composed annotation is itself declared on the method, so the merged view cannot carry a verb there,
and the scan would otherwise run for every unannotated method of a cached bean and each of its
inherited views. The three verb annotations are `@Target({METHOD, ANNOTATION_TYPE})`, so a
team can define a composed annotation such as `@UserCache`; `MergedAnnotations` resolves it and
applies its `@AliasFor` overrides. `DIRECT` searches the method itself and its meta-annotations but
not its declaring type's supertypes: the existing view loop already visits every interface and
superclass, so a hierarchy search there would return the first interface's annotation and hide the
second interface's different attributes — exactly the conflict the maximal-view comparison must see.
A missing composed-annotation path was a silent cache bypass: a method annotated with a team-local
`@UserCache` carried no direct verb, the pointcut did not match, and every call ran the body with no
warning. Repeated invalidations are one `Verb` carrying a list; a read or write is one annotation.
`Verb` is one record whose declaration payload is the annotation (or the invalidation list), so the
three shapes share a single type without a sealed hierarchy of forwarding records, and a bridge is
still skipped before the walk. A bridge is skipped before the walk, so it cannot lend
its bridged method's verb to the bridge view. Owner: `PlanResolver.classPlans`, `buildPlansForGroups`,
`declaredVerb`, `Verb`.

**A specialized signature without a bridge is folded into the parent view.** `javac` emits a bridge
when a class implements a generic interface, but a child interface that specializes its parent’s
type variable only redeclares the method: `UserRepository extends CrudRepository<User, Long>` with
`Optional<User> findById(Long id)` produces no `findById(Object)` bridging the two. Grouping by the
erased signature therefore split `findById(Object)` from `findById(Long)` into an unplanned parent
group and a planned child group, so a call through `CrudRepository` served the body with no cache
entry, and an invalidation declared on the parent view skipped the removal entirely — the silent
form of a stale entry, invisible until the TTL. `resolveGenericInterfaceOverride` resolves the
parent method’s parameter types against each interface between its declaring type and the root
with `ResolvableType.forMethodParameter`, then folds a declaration whose erased parameter types
match the resolved ones into the group. It runs after, not instead of, the most-specific lookup: an
intermediate interface that only redeclares the erased signature must not end the walk early. A
method whose parameters hold no type variable answers without allocating a `ResolvableType`, and a
candidate that resolves back to the erased signature is skipped, so `foo(T)` and `foo(String)` in
one interface never merge. Owner: `PlanResolver.getSpecificMethod`,
`PlanResolver.resolveGenericInterfaceOverride`.

**Class defaults are an attribute-wise fallback, and only applicable roots contribute.** `@ZetaConfig`
supplies the namespace, the two TTLs, the warming triggers and the key format, and a method
attribute always overrides it. A class root merges its superclass chain nearest first, then the
interfaces that carry the group’s views: a class implementing two unrelated business interfaces
must not let the first interface’s `@ZetaConfig(cache = "a")` lend its namespace to a method
declared on the second interface, whose own `cache = "b"` would otherwise be ignored. A method only
the class declares has no interface view, so every implemented interface contributes under the
class’s own attributes. Within the interface graph only maximal declarations compete — a
sub-interface dominates its parents — and either one maximal declaration answers or several that
resolve to the same effective configuration do; two that set the same attribute to different values
fail resolution instead of taking the first one the scan reaches, which is the same ambiguity rule
as the verb comparison. Within one declaration the annotations merge attribute by attribute, nearest
first: a subclass may set only `hardTtl` and inherit the parent’s `cache`, instead of
`MergedAnnotations.get` returning the child’s single annotation with an empty `cache`. Starting at
the declaring class lost a default on a subclass: an inherited `@ZetaCached(key = "#p0")` on
`BaseService` resolved against `BaseService`, never saw `@ZetaConfig(cache = "orders")` on
`OrderServiceImpl`, and bean creation failed with "cache name must not be blank"; a child repository
interface extending an annotated parent lost it the same way on a JDK proxy. A blank `cache` after
the fallback still fails startup; the method annotation’s `cache` is empty-only, so a method that
forgot both names cannot silently address a namespace. `"0"` remains an explicit "configured
default" and does not inherit the class TTL. A single `TYPE_HIERARCHY` scan of the bean class
answered all of this in one lookup but could not tell which type contributed an attribute, so two
sibling interfaces were folded into one configuration:
`CompositeService implements UserReader, OrderReader` wrote orders into the users namespace and gave
the user method the orders soft TTL, and the same class behind a JDK proxy answered differently.
The resolver therefore reads each type’s `DIRECT` declaration and walks the graph itself. Owner:
`PlanResolver.resolveConfig`, `PlanResolver.resolveInterfaceConfig`,
`PlanResolver.readDirectConfig`.

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
hiding the real conflict between `SubInterface` and `OtherInterface`. A short-circuit that let an
annotated most-specific method win outright hid the same ambiguity for an inherited superclass
method: a subclass that kept its parent's `@ZetaCached` and newly implemented an interface with a
different namespace never compared the two, so the interface's declaration was silently ignored.
The view set is also deduplicated with `Method` equality because the class-plus-interface walk
reaches one method through several paths. Owner: `PlanResolver.resolveVerb`, `dominates`.

**The invocation's receiver is the target object, not the proxy.** Spring's
`MethodInvocation.getThis()` is the target the call was made on; `ProxyMethodInvocation.getProxy()`
exposes the proxy. `ZetaCacheInterceptor.resolveProxyPlan` unwraps a receiver that is itself an
`Advised` proxy before the verb runs; every evaluation context and the reflective refresh reuse
that receiver, except where it cannot reach the method's declaring view and the proxy can — the
case the next paragraph owns. The unwrap stops at
the first object that still implements every proxied interface **assignable to the reported
method's declaring type**, not merely the declaring type itself: a Spring Data repository proxy's
singleton target is `SimpleJpaRepository`, which implements `CrudRepository` but not the child
`UserRepository` that carries the annotation. Unwrapping through it made a parent-interface call
key the plan lookup on `CrudRepository` and silently bypass the cache, and made a background
refresh reflectively invoke a method the target class does not declare. An introduced (mixin)
interface is unrelated to the method's declaring type and is deliberately ignored: the target
never implements a `@DeclareParents`/`IntroductionAdvisor` view, so requiring it kept the
interceptor on a nested proxy and made every refresh re-enter the advice chain — the refresh
served the stale hit instead of running the body. A proxied child interface that _extends_ the
declaring type is still required, so the Spring Data guard above survives. A plain receiver is
answered from its published `PlanTable` without allocating an invocation-plan object; the proxy
path (`ZetaCacheInterceptor.resolveProxyPlan`) unwraps the receiver once, replaces a receiver that
does not implement the reported method's declaring view with the proxy that does, derives the
authoritative target class, looks up the plan table, and only then falls back to the proxy's
business interfaces. That replacement is what keeps a soft refresh on the chain the caller used:
Spring's `ReflectiveMethodInvocation.getThis()` is the **target**, so a Spring Data repository's
`SimpleJpaRepository` receiver reaches a child-interface query through a same-named erased
declaration (`findById(Object)`). Judging the receiver "invocable" because of that erased
declaration kept it as the refresh target, and every soft refresh then invoked the base
implementation directly — silently dropping the `@Query`, `@EntityGraph` and `@Lock` the repository
proxy applies — while the self-warm loader for the same method already went through the proxy.
Only a receiver that has no proxy behind it keeps serving the refresh itself (a custom
`MethodInvocation` without `getProxy()`, or a target that implements the declaring view); a
receiver that is the body does not pay for a proxy round trip. The fallback dispatches with
`targetClass` corrected to the annotated child view, so `#root.targetClass` agrees with the plan
even when the raw target implements only the parent, and it carries the same receiver rule: when the
plan answers only from the proxy's table, the interface view it was built from becomes the refresh
target too, because the receiver that made the primary lookup miss is exactly the erased base
implementation — a repository's `findById(Object)` — whose body is not the one the caller ran. A
non-static `TargetSource` is never asked for
a target by the resolver (`getSingletonTarget` answers `null`), so the refresh re-invokes the
receiver Spring already resolved for this call instead of taking a second lease on the source.
Calling a retained proxy re-enters this advice chain for the key being refreshed unless the call is
marked: a prototype or request-scoped bean served its own stale L1 hit as the "refreshed" value, or
blocked on a per-key refresh lock. The reflective call therefore arms the bypass marker for exactly
that proxy and plan, so only the cache read advice is skipped and the rest of the chain runs as it
does for a caller. A JDK proxy that is not an `Advised` proxy has no user class to name: its most specific interface view becomes the target class, so
`#root.targetClass` and the plan table never surface the generated `$ProxyN` class. Without that
view, a key built from `#root.targetClass.simpleName` changed with every generated proxy class, so
two calls through the same interface addressed different entries, and `#root.targetClass` exposed an
internal generated class to the expression. The proxy path carries the unwrapped receiver used by
`#root.target` and a reflective refresh. Every receiver goes through the same pipeline:
`unwrapTarget` returns a plain object unchanged, and a receiver that is itself an `Advised` proxy is
unwrapped to the object the body should run on. When the target-class table misses, the lookup also
tries the method's declaring-type table before giving up. Owner: `ZetaCacheInterceptor.resolveProxyPlan`,
`PlanResolver.cachedTable`, `unwrapTarget`, `resolveTargetClass`, `lookupPlanForMethod`;
`ZetaCacheInterceptor.ReadLoader.invokeTarget`.

**A repository hit answers from the declaring interface’s table.** A Spring Data call is the one
steady shape whose receiver cannot answer it: `getThis()` is `SimpleJpaRepository`, or Boot’s inner
proxy, while the plan is published under the child interface, so every L1 hit unwrapped the
receiver, derived the authoritative class from its interfaces and allocated a
`ResolvedInvocation`. The interceptor answers that shape from
`cachedTable(method.getDeclaringClass())`, guarded by
`mostSpecificInterface(proxy.getInterfaces(), declaring) == declaring`: a proxy that also exposes a
child interface must be resolved against the child, whose `@ZetaConfig` is what the caller’s chain
was built for, and taking the plan from the parent’s table wrote into the parent’s namespace when
`ChildRepo extends BaseRepo` answered `baseNS` instead of `childNS`. Two layers are covered: a plain
target that does not implement the view dispatches on the proxy, and an inner `Advised` JDK proxy
dispatches on itself only when its own singleton target is plain and does not implement the view —
otherwise the receiver is an ordinary nested bean whose chain the slow path must still unwrap.
Owner: `ZetaCacheInterceptor.invoke`, `ZetaCacheInterceptor.repositoryPlan`.

**A final method no proxy can override fails resolution.** A CGLIB proxy cannot override a
`final` method, so `@ZetaCached`, `@ZetaPut` or `@ZetaInvalidate` on a public final method of a
concrete class was accepted at startup and then silently ignored at runtime: the method still ran,
just never through the cache. `PlanResolver.requireInterceptable` rejects that shape at proxy
creation unless an interface view declares the same method; that view lets a JDK proxy intercept,
while a CGLIB proxy still cannot. The check is tied to the interface view rather than the configured
proxy mode because the resolver does not own that mode, and an annotation on a method no interface
exposes is un-interceptable in either mode. When an interface view does exist, resolution logs a
WARN naming the CGLIB risk and continues: a JDK-proxy application is valid, and the resolver cannot
see `spring.aop.proxy-target-class` to decide for it. Owner: `PlanResolver.buildPlanForGroup`,
`requireInterceptable`.

**Resolution is the fail-fast boundary.** The advisor's pointcut calls the interceptor's plan
resolution while Spring decides whether to proxy a bean, so a configuration error surfaces at bean
creation instead of the first request. Resolution validates the whole verb: exactly one of the three
annotations (a second is rejected instead of silently shadowing the first), a non-blank namespace, a
value return type (`void` and `Void` cannot produce a cacheable value), every configured expression
parsed with its role and method in the failure message, every positional `#pN`/`#aN` reference in
range, the slot table reserving every discovered name that spells a positional alias (so `#pN` can
only ever address parameter `N`, and an out-of-range alias fails with the range diagnostic instead
of answering with the parameter that happens to carry that name) — `root`/`this`/`result`
parameters stay legal because SpEL and the bound result resolve ahead of the slot table and the
table skips those names — and every other variable name known to
the context — checked by walking the parsed AST once, which also rejects every assignment or
increment/decrement (`#x = ...`, `#user.name = ...`, `#x++`, `#x--`), so a padded expression is
still checked, a `'#p1'` literal is not a reference, and the AST facts the invocation path
needs — whether the tree is context-free, whether it reads `#result`, whether it reads any other
invocation input, and whether it is exactly `#root.args` — are collected in that same walk and
packed into one `flags` word on the `CompiledExpression`. A named
variable such as `#userId` is rejected when no parameter names were discovered: the runtime context
is built from that same array, so it could only resolve `null`, and letting it through made an
`unless` or TTL silently degrade instead of failing startup. The message points at `-parameters` or
`#p0`. A SpEL function call (`#fn(...)`) is rejected for the same reason: no functions are
registered on the context, so every evaluation could only throw, and `unless`/TTL evaluation
swallows that throw as a DEBUG log. A cache expression is read-only: an accepted `++`/`--` would
mutate the caller's argument object on every evaluation (`#root.args[0].age++` did exactly that),
and an accepted assignment could change the state observed by the next phase. An indexer, selection
or projection without an explicit receiver applies to `#root` and is therefore dynamic, never
folded as a constant against an empty context; constant folding is reserved for nodes that cannot
read the invocation. Rejecting all of these at resolution keeps the key, TTL and `unless`
evaluations pure functions of the invocation; a declared parameter name is legal unless it is
reserved.
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
A resolver failure is wrapped with the attribute role and the method, so a missing property names
the annotation that owns it. The SpEL parser and type locator use the bean factory's classloader
when one is available, so `T(...)` and compiled expressions resolve against the same loader as the
rest of the context. Owner: `PlanResolver.MethodResolution.cacheName`,
`PlanResolver.resolvePlaceholders`.

**Numeric TTL literals follow one range rule.** A digits-only literal (optionally signed) is
parsed as a `long`; a value outside the `long` range fails startup with a role-and-method error.
Every other spelling is SpEL: a context-free expression such as `60000L`, `+60000L` or
`60 * 1000` is evaluated at resolution and folded to a constant. A floating-point result
(`Double`/`Float`) — such as `1e3`, `1e20` or `9223372036854775808e0` — or `null` fails startup as
"must be an integral number"; a constant evaluation failure such as `1 / 0` is a startup error with
the role and method, not a per-call exception. A negative constant result also fails startup: SpEL
evaluates integer arithmetic in 32 bits, so `30 * 24 * 3600 * 1000` wrapped negative and the old
clamp silently degraded a 30-day TTL to the configured default; a signed literal keeps its explicit
clamp to `0`. The earlier floating-literal folder compared against
`2^63` instead of `Long.MAX_VALUE`: IEEE-754 rounds `Long.MAX_VALUE` up to `2^63`, so exactly
`9223372036854775808e0` passed and the narrowing cast saturated to the permanent sentinel. Rejecting
every `Double`/`Float` result removes that boundary instead of re-deriving it. A dynamic expression is evaluated lazily and fails instead of degrading: a negative result other
than `-1` throws with the role and method, a floating-point result is rejected the same way as a
floating-point literal, a `BigInteger` or `BigDecimal` that is not exactly representable as a
`long` throws instead of keeping its low 64 bits, and a runtime evaluation failure propagates.
The runtime path used to
call `longValue()` on a `Double`, so `#ratio * 1000` truncated silently and a floating division by
zero produced the permanent sentinel: a read cached forever, or a synced `@ZetaPut` rejected by the
permanent-sync check after the body had already committed. `Ttl` is a sealed interface: its
`Constant` record carries only the folded millis and its `Dynamic` record only the compiled
expression, role and method; both implement `LongSupplier` so a constant still costs one object and
a dynamic attribute carries no unused field. Owner: `CachePlan.Ttl`.

**A plan attribute has only the fields its shape can use.** `CachePlan.Key` and `CachePlan.Ttl` are
sealed interfaces with one record per shape, matching the `Read`/`Write`/`Invalidate` plan themselves.
The single-class shape left every kind branch reading a different subset of fields while the rest
stayed `null` or `-1`; a caller that missed the `kind()` guard could decode a constant key with an
unrelated namespace or index `args[-1]` instead of failing where the key was resolved. Each record now
holds only the fields that shape uses, and the binder's decoder selection names the record type
rather than an enum discriminator. Owner: `CachePlan.Key`, `CachePlan.Ttl`,
`AnnotatedWarmLoaderBinder.buildDecoder`.

**Constant folding is a whitelist, not a "reads no variable" test.** `CompiledExpression` records a
`pureConstant` fact while it walks the AST: no variable, no `@bean`, and no type, constructor,
member, function, selection or projection node. Literals, operators and an indexer on a literal
receiver still fold, so `'global'` becomes a physical key and `60 * 1000` a TTL, while
`T(Config).ttl()` and `T(TenantHolder).get()` stay lazy: a type reference is a runtime call even
when it reads no invocation state. An unquoted inline-map key is the one member node that does not
read the invocation: `{ACTIVE: false}` parses the key as a `PropertyOrFieldReference`, but the
evaluator reads its name and never resolves it against `#root`, and Spring treats such a map as a
constant. The walk skips exactly the even-position `PropertyOrFieldReference` children of an
`InlineMap`, so `unless = "{ACTIVE: false, DRAFT: true}[#result.status]"` keeps folding the map and
a context-free map-valued key stays context-free instead of building a `ZetaEvaluationContext` per
call. Values and computed keys are still walked, so `{a: #p0}` and `{T(...).KEY: 1}` stay dynamic.
That distinction is what lets resolution evaluate the fold
without turning a per-call lookup into a startup constant. Folding a key restores self-warming for
a PLAIN zero-argument method, which could only spell its key as `'global'` and was rejected as a
non-invertible expression; folding a TTL is what makes `prebuiltPolicy` reachable for the
arithmetic spellings a deployment actually writes. A folded key that evaluates to `null`, and a
constant expression that throws, fail startup with the attribute role and method instead of
surfacing on the first request. Owner: `CompiledExpression.AstInspector`, `CachePlan.Key`,
`CachePlan.Ttl`.

**The loader snapshots the argument array it may reuse.** A soft refresh keeps the `ReadLoader`
after the foreground call returned, so the loader's `target` and `args` are the refresh's only
input; the reflective method comes from `PlanResolver.invocableMethods`, a per-resolver cache keyed
by `(method, target class)` that every refresh of that pair shares. An argument list with no array
is stored as-is:
the invocation owns that array and the caller
cannot mutate it. When an argument is itself an array, the read snapshots the argument array and
every array reachable from it once the `condition` admits the call — a rejected call never reaches
the key or the loader, so it must not pay the copy — and binds the loader to that snapshot; the
condition and the key keep reading the invocation's own array, so both see one view. The loader
does not share the pre-body context in that case: that context is bound to the array the body
receives, so a post-body `unless` or a lazily evaluated TTL would read whatever the body wrote into
it while a refresh of the same key read the snapshot. The traversal inspects a
reference array element by element instead of trusting its declared component type: an
`Object[]`/`Object...` has component type `Object` even when its elements are arrays, so a
component-type test copied only the outer level and a refresh observed the caller's later mutation
of a nested `byte[]`. Each source array is cloned once and recorded by identity before its elements
are visited, so a repeated array resolves to its existing clone and a self-referential graph such
as `Object[][] a; a[0] = a` keeps its cycle instead of overflowing the stack or expanding without
bound; a flat argument list, and a reference array whose elements are not arrays, allocate no
identity map. The snapshot is taken on the cache-admitted path before the body, not in `load`: a
soft-expired hit arms a refresh through the loader without ever calling `load`, so deferring the
copy would let that refresh observe a post-call mutation. A non-array mutable argument is still
shared, which is why a method whose body
depends on mutable or request-scoped state must not declare a soft TTL. The one-shot advice chain
field holds only that chain and is thread-confined to the invocation thread: the foreground `load`
claims it once and clears the field, and `release()` clears it before `get` returns, because a
retained loader must never keep the chain (and its proxy state) reachable after the call; a refresh
never reads or resumes it. The loader also caches the invocation thread's one
`ZetaEvaluationContext` and clears it on release: the pre-body key or condition, the lazy TTL
suppliers and the post-body `unless` share it, so a miss that configures all three builds one
context instead of three. A refresh thread never reads that field and builds its own context, so a
retained loader still holds no evaluation state. `release()` also clears the invocation thread
itself. The loader is retained for the entry’s whole TTL, and a final `Thread` field kept the
thread that served the first miss reachable for as long as the entry lived; on a Java 21 virtual
thread that is the finished `VirtualThread`, its `ThreadLocalMap` and its context class loader, one
per key, so a long-TTL deployment retained millions of dead threads. The field is non-volatile and
written only by that thread: another thread can only ever observe the owner or `null`, never itself,
so the identity test alone separates the foreground thread from every other one, and the owner
observes its own clear by program order. Owner:
`ZetaCacheInterceptor.ReadLoader`, `ParameterBindings.snapshot`.

**A custom `MethodInvocation` may report a null argument array.** Spring's
`ReflectiveMethodInvocation` reports an empty array, but the AOP Alliance interface does not require
it, and the key plans, the evaluation root and `InvocationArguments.snapshot` all read `args.length`. The
interceptor normalizes `null` to the shared empty array at its entry, the one boundary that owns the
argument contract, so a test stub or a third-party invocation cannot fail a call with an NPE before
the key or the body runs. Owner: `ZetaCacheInterceptor.invoke`.

The class's table is an instance-scoped `ConcurrentHashMap<Class<?>, PlanTable>`, where each
`PlanTable` holds the user class plus its immutable `Map<Method, CachePlan>`. It is keyed by every
class asked about; `ClassUtils.getUserClass` normalises a CGLIB proxy once, when the proxy's entry
is registered, so a CGLIB bean's pointcut question and first invocation share one resolution
instead of compiling every expression twice, and the hit path never calls `getUserClass`. The
resolver owns the table instead of attaching it through a `ClassValue`: a plan retains application
`Method`s and compiled SpEL, and a parent-loader framework interface key (`CrudRepository`, a shared
callback) would keep that value on a `Class` that outlives the context, pinning the application
classloader. The map is released with the Spring context. A
JDK proxy class is routed to its business interfaces before any scan: `Proxy.isProxyClass` takes
the maximal proxied interfaces (infrastructure interfaces such as `SpringProxy`, `Advised` and
`DecoratingProxy` excluded), a single one lends its table to the proxy, and several are scanned
together with each view resolved against the interface hierarchy. The proxy class's own generated
methods carry neither the annotations nor the bridge flags, so scanning them split a generic parent
interface method from its annotated child override and left the parent view unplanned — a silent
cache bypass for a Spring Data repository called through `CrudRepository`. The invocation reads the
map with the invocation's target class; a CGLIB proxy class delegates
to the user class's immutable table once, so the hit path performs one map read and one map
lookup with no key allocation, no `getUserClass` name probe and no reflection. Every view of an
annotated group is stored under its own `Method`, so a method from any implemented interface answers
with the table lookup alone; a method reported by a proxy class the user class does not declare
falls back to the declaring type's table (the nested interface-proxy case) and then to the
most-specific user-class method. `AopUtils.canApply` stops at the first matching
method, and a JDK proxy matches each method lazily on its first call, so the first pointcut question
about a class — not the first match — scans every candidate method of that class once. The pointcut
also carries a `ClassFilter`: Spring otherwise asks the resolver about every method of every bean,
including JDK infrastructure, `AopInfrastructureBean` implementations and third-party framework
classes, and each question permanently caches that class's (usually empty) table. The filter skips
`java.*`, `javax.*`, `jakarta.*`, Spring/AOP infrastructure and `sun.*`/`jdk.*` packages, but
admits every JDK proxy class first: a Spring Data repository's generated proxy lives in a framework
package while its business interfaces carry the annotations. `org.springframework.data.*` is exempt
from the Spring negative for the same reason one layer down: `SimpleJpaRepository` and its peers are
the target a single-layer proxy or a `RepositoryProxyPostProcessor` chain is built over, and
filtering them out disabled the advice silently while every method of the business interface
carried a verb. A CGLIB proxy keeps its user class's package name, so it is not filtered.
The scan builds the table in a local map and publishes it only after the whole scan succeeds with
`putIfAbsent`; a throwing scan publishes nothing, so a malformed second verb fails bean creation and
the class is retried instead of being half-validated, while a concurrent duplicate scan is harmless
because the tables are immutable and the first one wins. A later question about a view of the class
is one lookup — present means a verb method, absent means an unannotated method — with no annotation
scan and no most-specific lookup; only a method reported by a proxy class the user class does not
declare can still take the fallback. The pointcut and the
scan filter `Object` methods (both `Object`-declared and overrides of `toString`/`hashCode`/`equals`),
static, private and non-bridge synthetic methods. A final method is not filtered: `requireInterceptable`
validates it, because an interface view can still make it interceptable. The interceptor already
skips `Object`-declared calls at invocation; matching them in the pointcut would create a proxy whose
annotated `toString`/`hashCode` is silently never cached.

**Advice order brackets the transaction.** `ZetaCacheAdvisor.ADVICE_ORDER` is
`LOWEST_PRECEDENCE - 1`: inside method security (order 100), outside the default transaction
interceptor. The body runs inside the transaction because the read's loader proceeds into it,
while a write or an after-the-fact invalidation runs only after the transaction completed — a
rollback can never leave a cache side effect. The read fill keeps the same boundary by never storing a value that may
include the transaction's own uncommitted writes: a caller-affine read inside a read-write
transaction goes through `loadIsolated` and commits nothing at all, while the shared and batch paths
read on the dedup executor (committed data) and store inline under the pre-read stamp. Deferring that
store to `afterCommit` was both ineffective and unsafe: the leader runs on the dedup pool, where no
transaction context exists, so the hook only fired when a test or a direct executor put the leader on
the business thread — and there the deferred pre-read stamp could outrank an invalidation the same
transaction registered after it. A key expression that does not read `#result` is
resolved before the body, outside the transaction, so a null argument or a null key fails before the
business side effect. A synced dynamic hard TTL is the only TTL evaluated before the body, because
a permanent value cannot be broadcast; the evaluated value is reused by the write. Every other TTL,
and `unless`, waits for the body's return (and, for a TTL, for the result to be non-null and not
vetoed). Only an expression that actually reads `#result` needs the returned value. A
result-dependent `@ZetaPut` key that resolves to null skips the write instead of failing after the
body: the side effect has already committed, so a null key (for example `#result?.id` on a generated
id that is absent) behaves like `unless = true`, and the caller keeps its value without the cache
recording it. The previous shape used `resolve` and threw `IllegalStateException` after the
transaction committed, turning a successful write into a 500.
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
state. A later refresh invokes the group's most-specific method reflectively, outside the
transaction interceptor and without method security. When the target implements only the method's
parent interface, that method is erased on the target (Spring Data: `SimpleJpaRepository
.findById(Object)` for `CrudRepository<T, ID>`). The plan carries the widest bridge signature the
declaring class generated; an interface never generates a bridge, so a repository child interface
has none. A three-level generic hierarchy generates a `Number` and an `Object` bridge, and selecting
the first one made the bridge lookup depend on `Class.getDeclaredMethods()` order, so the widest is
the stable signature carrier. `TargetMethodResolver.invocable` therefore tries the bridge, then
walks the plan method's declaring-type supertypes for the erased declaration on a common parent the
target actually implements, resolves it with `BridgeMethodResolver.getMostSpecificMethod`, and
claims reflective access on the selected method. The walk skips static and private declarations and
chooses the most specific accepting overload: a same-signature static helper otherwise ran instead
of the body, and an `Object`/`CharSequence` overload pair could resolve to `Object` depending on JVM
order. It visits the whole reachable supertype graph and compares every candidate declaration
before returning: a diamond where `LongRepo extends RootRepo<Long>, NumericRepo<Long>` reaches
`RootRepo.find(Object)` first and used to stop there, hiding the more specific
`NumericRepo.find(Number)`; the result now depends on specificity, not interface declaration order.
`invocable` rejects a static method that `getMostSpecificMethod` finds on the target
for the same reason. The fallback to the reported interface method claims its reflective access
before the visibility test, so an implementation in a closed package can still refresh through an
open package-private/interface view. Without the erased walk a soft TTL on `UserRepository.findById(String)`
refreshed every time with "object is not an instance of declaring class", so the stale value was
leased until its hard TTL instead of being replaced — the previous shape trusted `erasedBridge`
alone and kept the specialized interface method. Without the access claim the same refresh failed
with `IllegalAccessException` when the implementation class or its package was not exported to the
caller's module. That is true even when the refresh executor runs the task inline on the caller's
own thread (`CallerRunsPolicy`, a synchronous test executor). `TargetMethodResolver.invocable`
claims access and then checks `canAccess(target)`: when the plan's implementation method cannot be
opened but the reported interface method can be invoked, the interface method is used instead.
Invoking the public interface method still dispatches to the implementation, so a package-private
implementation in a module that does not open its package no longer turns every refresh into an
`IllegalAccessException`. The supertype walk records each visited type, so a diamond of repository
interfaces is scanned once per resolution instead of once per path. The invocable cache is keyed by
the target class and, per entry, by the plan's specific `Method` (or the `(specific, reported)` pair
when a different view reaches the same plan), so a refresh lookup compares two methods instead of
recursively hashing the whole `CachePlan.Read` and its compiled expressions.

**A refresh that must invoke the advice proxy bypasses that call's cache read.** A repository
fallback or a scoped proxy whose target cannot invoke the annotated view is reflectively invoked on
the proxy itself. That call re-enters this advice chain; the fresh `zeta.get` then serves the
soft-expired entry the refresh is trying to replace (or, on a miss, joins its own SingleFlight
load). The interceptor arms a `ThreadLocal<RefreshBypass>` for exactly that reflective call,
matched by proxy identity and a bridge-tolerant method signature. The first matching `invoke`
resumes the chain without resolving the key, reading the cache or allocating a loader; the caller
restores the previous marker in a `finally`, so a body's nested call to a different cache method
keeps its normal semantics. A marker keyed by method equality alone was insufficient: a JDK
repository reports a parent/bridge view while the plan invokes a more specific one.

The engine therefore hands a loader two entry points — `CacheLoader.load` for a foreground miss and
`CacheLoader.reload` for a refresh (the default delegates to `load`) — and the annotation
interceptor implements `reload` as the reflective call. A refresh failure is thrown back
through `reload`, so the engine's WARN and closed-retry-window path applies exactly as for a plain
loader. The refreshed value is written to the local L1 only; a method whose body depends on
request-scoped state must not be given a soft TTL. The foreground side is a synchronous contract:
when `CacheLoader.load` runs for a caller-runs read, it completes on the calling thread before
`Zeta.get` returns, and a same-key follower's loader is never invoked at all. The annotation loader
therefore releases its one-shot chain in a `finally` after `get` returns; that clear cannot race a
first load that is still running. A future engine that moves the first load off the caller's thread
must change this contract and the loader's ownership model, not just the executor.

**One context per foreground call, reused across its phases.** A context is built from the call's
immutable inputs — the unwrapped target, the resolved class, the group's most-specific method, the
arguments and the plan's parameter slot table. The pre-body key and the
synced dynamic hard TTL share one context; they cannot observe a result. On a write, the pre-body
context is reused when it exists; otherwise one post-body context binds `#result` and serves
`unless`, a result-dependent key and every TTL evaluated at write time, so the same call does not
rebuild a context for each expression. A read loader caches the invocation thread's one context:
the pre-body key or condition, the lazy TTL suppliers and the post-body `unless` share it, and
`release()` clears the owner-thread marker and the field before `get` returns. A refresh thread, or
a supplier the engine evaluates on another thread, compares the owner thread and builds its own
context. A refresh that runs inline on the owner thread after release (a caller-runs executor, a
direct executor, or a pooled web thread reused by a later request) must not re-populate the field
either: the cleared marker turns that path into a stack-local context, so a retained loader cannot
pin an evaluation context and its bound business value forever. The same thread check gates
`load`: `proceed()` is resumed only on the invocation thread; a load reported elsewhere runs the
body reflectively rather than corrupting the invocation's transaction/security context. No context
is cached beyond the call; the pre-body context serves the post-body phase only because the two
phases never overlap, so the owner-thread plus released check and the release itself are the whole
state machine and an off-thread evaluation cannot read a half-released foreground binding. The
plan claims the method's reflective access once at resolution, unconditionally: under JPMS a public
member is not necessarily accessible when the package is opened but not exported, and setting the
accessible flag also lets `invoke` skip the caller-sensitive access check; a refresh that invokes
the plan's method therefore pays the invocation only, and `TargetMethodResolver.invocable` claims
access on a fallback method when it has to select one.

**The context shares reflection caches within one interceptor and stores no variable state.**
`ZetaEvaluationContext` implements `EvaluationContext` directly and returns one
thread-safe `ReflectivePropertyAccessor(false)` and one method/constructor resolver set from an
`ExpressionSupport` object, so the accessor's reader/type-descriptor caches survive the phase that
first resolved a property. Writes are disabled at the accessor itself, matching the read-only
contract and avoiding the writer cache; `setVariable` rejects assignments as before. The support is created by `PlanResolver` (the interceptor's plan
source), not a static field:
those caches strongly reference `Class`/`Method`/`Field`/`TypeDescriptor`, and the type locator
captures a classloader. A JVM-wide static set pinned application classes across a DevTools restart or
a second Spring context and could resolve `T(com.app.Type)` against the wrong loader; the support
uses the bean factory's classloader when one is available. `setVariable` rejects every write and the
context keeps no assignment map, so a cache expression cannot introduce state that a later phase
observes. A `StandardEvaluationContext` also allocates a variable map in its constructor and accepts
eight setter calls for delegates this context already owns as constants; implementing the interface
keeps the per-phase object to the fields it actually reads. The root is built once per foreground
call and reused by the phases that share it; a constant-key hit never builds one at all, and a
read's TTL supplier evaluated after release or on another thread builds a fresh root and context.
`create` always takes
that plan source's support; a direct caller or test builds one with `ExpressionSupport.defaults()`.

A context-free expression evaluates against `ExpressionSupport.emptyContext()`: the same read-only
accessors, converters and type locator, no variable map, no bean resolver, no write path. It must
belong to the same support as the phase contexts rather than to a static field, for the three
reasons above; it is also what makes a constant fold trustworthy, because `T(app.Type)` in a folded
key resolves against the bean factory's loader while an expression such as
`T(System).props.clear()` has no writable accessor to reach.

Owner: `ZetaCacheInterceptor`, `PlanResolver`, `CachePlan`, `ZetaEvaluationContext`, `CacheLoader`,
`ZetaCacheAdvisor`, `ZetaAnnotationAutoConfiguration`.

---

**The annotation layer is resolved declaratively; the extra machinery is gone.** The layer now
uses Spring's `MergedAnnotations` (`TYPE_HIERARCHY`), `BridgeMethodResolver` and
`AopUtils.selectInvocableMethod` for annotation lookup, bridge folding and reflective target
selection. The previous shape re-implemented interface dominance, maximal-interface pruning,
method-view grouping, PLAIN namespace type inference and a SpEL AST constant folder; those
mechanisms answered questions Spring already answers, and each had a hand-written edge case that
the framework rule does not need. What remains is the part a framework does not provide: the
unknown-variable check at resolution (a misspelt `#userId` must fail startup, not resolve to
`null`), the TTL negative/range guard (a mistyped or 32-bit-overflowed TTL must not silently
become the default), the `sync=true` permanent preflight (a business write must not commit before
the configuration error surfaces), and the maximal-view verb conflict check (two independent
interfaces that disagree on one method are a configuration error, not a scan-order choice).

The removals and their replacements:

| Removed                                                        | Replaced by                                                                                    | Failure mode accepted                                                                                                                                    |
| -------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `InvocationArguments` (288 lines)                              | `ParameterBindings.pack`/`varargsTail`/`adaptArgument`/`snapshot`                              | None: the varargs/erased-array shapes are still adapted for `Method.invoke` and `#pN`; the cycle-preserving snapshot keeps its `IdentityHashMap`.        |
| `TargetMethodResolver` (337 lines)                             | `PlanResolver.targetMethod` + `findErasedMethod`, memoized per resolver                        | None: the reflective target is resolved once per `(method, target class)`; a second resolver re-resolves it.                                             |
| `CacheFailure`/`CacheFailurePolicy`/`CacheVerb` (199 lines)    | `log.warn` in `ZetaCacheInterceptor.write` and `CachePlan.Invalidate.Operation.applyAfterBody` | A committed write or an after-body removal logs and returns; a failed before-body removal still throws. No pluggable failure policy.                     |
| SpEL AST scope simulation / assignment checks                  | runtime evaluation, `ExpressionSupport.emptyContext()` for context-free expressions            | An assignment fails at evaluation (`setVariable` rejects it) instead of at parse; a non-boolean condition fails at the first call instead of at startup. |
| PLAIN key-type locking (`@ZetaConfig.keyType`, `requiresType`) | the format prints the value text; the caller owns type identity                                | `Long 42` and `String "42"` share one PLAIN entry by design.                                                                                             |
| final-method startup check                                     | Spring's proxy limitation, documented                                                          | A `final` method on a concrete class is not interceptable by CGLIB; the annotation is inert.                                                             |
| sibling `@ZetaConfig` merge/conflict                           | nearest declaration wins                                                                       | Two independent interfaces that set the same `@ZetaConfig` attribute differently no longer fail startup.                                                 |

Owner: `PlanResolver`, `CachePlan`, `ZetaCacheInterceptor`, `ParameterBindings`,
`CompiledExpression`, `KeyEncoding`, `DefaultKey`.

## Annotation reads

**The annotation read is caller-affine.** `CachePlan.readPolicy` sets
`GetPolicy.callerRunsLoader = true`, so the elected leader of a first miss runs
`MethodInvocation.proceed()` on the invocation thread: the body keeps its transaction, security
context and MDC. `SingleFlight` still deduplicates a same-key follower onto the leader's future;
the executor never owns the advice chain. A transaction-aware caller is isolated further: the
reader runs on its thread with no published flight (design decisions: read path). The
`ReadLoader` reflective fallback exists only for a load reported from another thread (a custom
`SingleFlight`/executor) and for background refresh, where `proceed()` is unavailable by
contract. Owner: `CachePlan.readPolicy`, `ZetaCacheInterceptor.ReadLoader`,
`HotKeyCache.loadAndCache`.

**The batch read honours the same thread affinity.** `getAll` resolves the TTL overrides once for
the whole call and rebuilt the caller's policy through `GetPolicy.of`, which hard-coded
`callerRunsLoader = false`. A caller that asked for a caller-affine reader got every miss dispatched
to the load executor — losing its transaction, security context and MDC — and a batch read inside a
read-write transaction wrote values that a concurrent reader could observe before the transaction
decided. The rebuild now carries the flag; `HotKeyCache.loadAndCache(List…)` loads each miss on the
calling thread, one key at a time, with same-key deduplication intact, and isolates the load inside a
read-write transaction instead of storing it. Owner: `DefaultZeta.getAll`, `GetPolicy.of`,
`HotKeyCache.loadAndCache`.

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
`#p0`/`#a0`/parameter-name bindings read straight from the invocation arguments (the names come
from the annotation-declaring view, falling back to the most-specific method), `#root.*`, and the
`@beanName` resolver). The
attribute is stripped before compilation, so surrounding whitespace never changes the fast path.
A key whose whole parsed AST is one variable reference — `#pN`, `#aN` or a bare parameter name — is
recognized at resolution and reads `args[N]` directly, skipping SpEL for the common single-id key;
a text scan cannot do that without misreading `'#p1'` or `#p01`. A bare parameter name is looked up
in the discovered parameter list; a canonical `#pN`/`#aN` is an index. The fast path carries the
declared array type for a non-varargs array parameter and normalizes an erased `Object[]` before
encoding, so it sees the same shape as the SpEL path; a missing reported argument returns the same
null key as the table lookup instead of indexing past the array. A varargs tail stays on the SpEL
path so its packed and expanded shapes both survive. `#root`, `#this` and `#result` keep the
expression path. An expression that reads none of that state — `T(TenantContext).current()`, or
`T(java.lang.Math).max(1, 2)` — is resolved against the resolver's read-only empty context, so a
key of that shape builds no `Root`, `TypedValue` or `ZetaEvaluationContext` per call; only an
expression that references a variable, the root, a bean or an implicit root member keeps the phase
context. The empty context carries the same type locator, converters and resolvers, so the value
and the failure mode are identical. A non-canonical name such as `#p01`, `#accountId` or `#page` is
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
interface shape is tested after the wrapper classes. A directly self-referential array or collection is
rejected immediately; an indirect cycle is rejected as soon as a nested container repeats an ancestor
on the current recursion path, so a branching cycle cannot expand exponentially before the depth cap.
The 32-level cap remains for a cycle longer than the ancestor path and for acyclic nesting, so a
malformed argument graph fails the call with a configuration error instead of overflowing the stack.
A container with more than 1,024 elements is rejected for the same reason: a multi-megabyte
`byte[]` would otherwise expand into a key several times its size and reach Caffeine, the sketch
and the sync wire. The cap is enforced by the same failing call for an array and a collection,
before any element is encoded. The ancestor chain is allocated only when a top-level container
actually contains a nested container, keeping a flat array or collection on the zero-allocation
path. Two limits
remain and are documented rather than hidden: a non-array object is rendered by `toString()`, and
every reference array carries the same `aO` component tag, so different component classes whose
element encodings coincide (`String[]{"a"}` vs `Object[]{"a"}`) share a key — a value type or array
shape that must not collide needs an explicit key. A null top-level argument or a resolved null key
is a configuration error that fails the call rather than addressing the default key; the exception
is a selected-argument or expression key resolved after the body, where a null is a no-op. A
whole-argument-list key still rejects a null argument in either phase, because a null argument
cannot be encoded. A no-argument default key, a no-argument whole `#root.args`, and a context-free
key expression are precomputed as the full physical key at resolution, so a steady-state hit
performs no SpEL and no namespace concatenation.

A read resolves its key and TTLs before the body, so `#result` is unavailable to them; declaring it
there is rejected at resolution, not evaluated to `null`. A pre-invocation invalidation has the same
restriction. Detection walks the parsed SpEL AST, so `key = "'#result'"` remains a legal literal key
and a property named `result` is not mistaken for the variable, while a whitespace-padded expression
is still checked. One walk collects the variable set and rejects every assignment or
increment/decrement. An indexer, selection or projection without an explicit receiver applies to
`#root`, so it is dynamic and is never folded as a constant against an empty context. Inside a
selection or projection the condition is evaluated once per element with that element as the active
context object, so a bare property and `#this` there address the element, not the invocation: a
self-warm `unless = "#result.items.?[active].isEmpty()"` is a legal result-only guard rather than a
rejected caller dependency. `#root`, `#pN` and `@bean` remain invocation/bean reads even inside the
element scope, and the selection itself still applies to whatever precedes it. `#result` is
available wherever the expression may resolve after the body: `@ZetaCached.unless`, a `@ZetaPut`
key or TTL that reads it, and an after-the-body `@ZetaInvalidate`. A `@ZetaPut` key that does not
read it resolves before the body; its TTLs are evaluated when the write happens, except for the
synced dynamic hard TTL preflight. The post-body context holds the body result in a field with a
bound flag, so a bound `null` is distinguishable from an unbound `#result`, and `lookupVariable`
answers the bound result ahead of the parameter table, so a parameter named `result` is legal and
cannot overwrite it.

The legal variable set is also static when parameter names are available: `#root`, `#this`,
`#result` only where the caller declares it bound, every in-range `#pN`/`#aN` and every parameter
name. When parameter names are unavailable, a named variable is rejected rather than admitted: the
runtime context uses the same array and could only resolve it to `null`, which would silently
disable an `unless` or zero a TTL; the failure names `-parameters` and the `#p0` alternative.
`pN`/`aN` are generated positional bindings: the slot table rejects a parameter name that would
point at a different argument, while a parameter named for its own slot resolves identically.
`root`, `this` and `result` are bound ahead of the table — SpEL's root/active object and the
context's result field — so a parameter with one of those names stays legal, is omitted from the
slot table, and the reserved name never resolves to it. A
misspelled `#reslt` is rejected at resolution instead of evaluating to `null` and silently disabling
an `unless` or zeroing a TTL. A SpEL function call is rejected for the same reason: no functions are
registered, so it could only throw at evaluation time. Assignments and increments/decrements are
rejected outright, so the variable set stays static and no expression can introduce state for a
later phase.

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
before the `put`. Either way it evaluates at most once per load with `#result` bound; a `null`
answer does not veto and any other answer is coerced to a Boolean, while an expression that throws
propagates to the caller: a broken veto is a configuration error, not a silent "store everything".
A malformed expression is rejected at resolution instead.

**TTL attributes are classified at resolution.** Surrounding whitespace is stripped; an optionally
signed integer literal folds to a constant — a negative value clamps to `0`, the configured
default — and the constant value supplies its own `LongSupplier`; a digits-only literal outside the
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
result other than `-1`, a floating-point result, or a `BigInteger`/`BigDecimal` that is not exactly
representable as a `long` fails evaluation with the role and method instead of silently becoming
the configured default. Exactness is what makes the `-1` sentinel trustworthy: `longValue()` turned
`-18446744073709551617` into `-1`, a soft window the caller never disabled, and a value beyond
`Long.MAX_VALUE` into a positive lifetime.

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
after-the-body form resolves a conditional removal after the return: a present `condition` is
evaluated there and, when it passes, gates key resolution. An unconditional removal resolves a key
that does not read `#result` before the body, so a null argument fails before the business side
effect; a key that reads `#result` waits for the return. The pre-invocation form resolves before
the body runs, where `#result` does not exist. A key that reads `#result` is evaluated by SpEL: a
null result is a null
dereference, so a key that must tolerate a nullable return writes `#result?.id` — safe navigation
yields a null key, which is a no-op removal. A non-null result whose selected property is null also
yields a null key and is skipped. An expression that handles null itself
(`#result != null ? #result.id : #fallbackId`) still runs and can remove its fallback key. A key
resolved before the body must not be null: that is a configuration error, because failing then
still precedes the business side effect. A key resolved after the body — whether it reads
`#result` or was gated by a condition — treats a null key, and an `eachKey` collection that is null
or empty, as no removal: the side effect has already committed, so a null argument must not turn a
successful call into an error.

**A constant guard is type-checked at resolution.** A `condition` or `unless` whose SpEL tree is
context-free is evaluated once while the plan is built, and its result must be `null` or a
`Boolean`. The runtime asks SpEL for a `Boolean`; a constant of another type is admitted only by
the converter's incidental rules or throws on every call, and the guard's catch turns that throw
into a silent skip or store at DEBUG. A constant `condition` then permanently bypassed the cache
and a constant `unless` permanently stored, paying one exception per call and never naming the
attribute. Evaluating the constant at resolution makes its meaning explicit — a String `'true'` or
a number `1` is a configuration error, not a conversion. The evaluated value is stored on the
compiled expression, so a constant guard is a field read at runtime, not a per-call SpEL
evaluation, and a constant `unless` builds no evaluation context at all. A write's `condition` is
checked before its key/TTL pre-context is built, so a constant-false gate with a context-reading key
also pays no context allocation. Constant `null` stays
legal because it is a defined "skip" answer on both attributes. Owner:
`PlanResolver.MethodResolution.compiled`, `PlanResolver.requireBooleanConstant`,
`CompiledExpression`.

**`condition` is an admission guard, evaluated in the operation's phase.** A read or write evaluates
it before the body, so `#result` is unavailable; a false result runs the body without touching the
cache or resolving the key. An invalidation evaluates it in the same phase as the removal: before
the body when `beforeInvocation = true`, after the return otherwise, where `#result` is the unwrapped
domain value. The rule is what lets `condition = "#id != null"` skip a cache whose default key
rejects a null argument, without weakening the key contract. A `null` result is treated as `false`
and any other result is coerced to a Boolean; a runtime failure propagates to the caller. The
direction is what matters, and it is the opposite of a store veto: a broken guard must not admit a
call the caller meant to bypass, and a broken veto must not store a value the caller meant to
reject. A malformed expression still fails startup. A context-free expression is evaluated without an
invocation context: `CompiledExpression` answers it from the support's empty context rather than the
phase context. Building no context is the optimization, and the read-only empty context keeps it
sound: routing `condition = "true"` through a context overload made SpEL reject a null context, the
fail-closed catch turned that into `false`, and every call silently bypassed the cache.
Owner: `PlanResolver.MethodResolution.compiled`,
`ZetaCacheInterceptor.read`.

**`Optional` is unwrapped at the loader boundary, never stored as a wrapper.** A read loader returns
`Optional.orElse(null)` to the engine, so a hit that produced `Optional.of(user)` and a raw
`User` read of the same namespace address one domain value; the caller-facing result is always
rewrapped with `Optional.ofNullable(value)`. The previous "an Optional is not wrapped twice" guard
returned the cached inner value directly, so a method returning `Optional<Optional<T>>` produced
the wrong shape on every path; always wrapping preserves the nesting (the loader still unwraps
exactly one layer). `Optional.empty()` is a null result and is never stored. A write unwraps before `unless`, a
result-dependent key and every TTL, so `#result` is the domain object; the original wrapper is
returned to the caller. Without that boundary, a method returning `Optional<User>` stored the
wrapper while a sibling method stored `User`, and the first read through the wrong shape threw
`ClassCastException`; over the sync channel the wrapper also failed `NotSerializableException`.
Owner: `ZetaCacheInterceptor.read`, `ReadLoader.load`, `ReadLoader.reload`.

**Repeated invalidations are one verb with a list, not mixed cache operations.** A method may carry
several `@ZetaInvalidate` declarations, each with its own namespace, key, condition and policy. A
child declaration replaces the inherited set; two unrelated maximal views are one declaration only
when their lists are equal, order included. The earlier set comparison accepted the same
invalidations in opposite orders and then executed whichever list the interface scan reached
first, so swapping the `implements` order changed the removal order without a startup error. Each
operation keeps its own phase — whether its target resolves before the body, and whether its
condition and target wait for the result — resolved once at plan time from the policy's `immediate`
flag and the target's `#result` dependency, so one declaration's skipped condition or empty batch can
neither suppress nor
re-resolve another's removal. Every pre-body target is resolved before the first immediate removal
runs: a later declaration's null argument or bad key used to fail after an earlier namespace had
already been cleared, while the body never executed, and the caller could not tell which removals
had happened. A target resolves itself into one executable removal that carries its
namespace, so the failure policy receives the namespace the plan resolved instead of a substring
re-derived from the physical key; a null or empty target resolved after the body is no work at all.
A post-body resolution failure on one declaration is collected while the remaining declarations
still run, then rethrown (additional failures suppressed): the body already committed, so one bad
`#result` expression must not leave every other namespace stale. Pre-body targets keep the
fail-before-the-body rule: they are all resolved before the first immediate removal runs.
`eachKey` expands an array or `Iterable` into one
physical key per distinct non-null element through `DefaultKey.encodeSingle`, so a batch delete
removes the same per-id entries a single-id read created and a repeated id is one removal; an empty
collection is a no-op, more than 1024 elements fails instead of iterating an unbounded/lazy source,
and a top-level null is a
configuration error before the body and a no-op after it. The type that reaches a PLAIN namespace is
the _element_ type: an array contributes its component type and an `Iterable` its resolved generic
argument, not the container. Passing `List.class`/`Long[].class` to the scalar check rejected every
legal `eachKey` invalidation in a PLAIN namespace at startup. The startup proof rejects only a key
whose declared type is statically unable to be an array or `Iterable`: a primitive, or a final
class that implements neither. `Object`, a generic erasure and an interface such as `Serializable`
may hold a collection at runtime, so rejecting them turned a legal batch invalidation into a
startup failure. `allEntries` clears exactly one namespace
through `clearNamespace`, never the whole table; it is mutually exclusive with `key` and `eachKey`.
The namespace-clear branch has no key expression, so it must not ask an inference helper to read
one: the old call passed `null` into a single-variable probe that dereferenced it, and every
`@ZetaInvalidate(allEntries = true)` method failed bean creation with an NPE.
A collection key without `eachKey` logs a startup WARN because the likely mistake leaves every
per-element entry stale until TTL. Owner: `PlanResolver.Verb`, `CachePlan.Invalidate`,
`CachePlan.Invalidate.Operation`, `ZetaCacheInterceptor.invalidate`.

**A cache-side failure is split by phase and by cause.** A read engine failure propagates: the
loader may already have run, so a fallback cannot safely repeat the body, and a local engine failure
is a wiring bug rather than a source outage. A `put`’s transient failure, and an after-the-body
removal’s, is logged at WARN and swallowed, because the business write already committed and an
engine failure must not turn it into a 500. A pre-invocation removal/clear failure is rethrown,
because continuing the body would write with a cache that could not be invalidated and widen the
dirty-read window. Caller/configuration errors are not cache-side failures: an unencodable TTL, a
permanent synced write whose hard TTL resolved from the global default, or a Worker-only
`ZetaModeException` is rethrown by `ZetaCacheInterceptor.write`. The removed policy logged them at
WARN and returned, telling the caller a synced write succeeded while the facade refused it. The
result-dependent permanent synced TTL is the only rejection that can be knowable after the body
alone; a constant TTL is rejected at resolution and a result-independent dynamic one before the
body, so every such rejection surfaces at the point it became knowable. An after-body removal splits
by cause, not by phase: a transient engine failure leaves the committed business result standing,
while `IllegalArgumentException` and `ZetaModeException` are rethrown exactly as `@ZetaPut`
rethrows them. A declaration that can never run — a `scope = REMOTE` invalidation in a
local-only deployment — used to be logged and skipped, so the caller saw success, the entry
stayed stale for its whole TTL, and `beforeInvocation = true` on the same method threw: one
annotation shape had two contracts for the same configuration error. Owner:
`ZetaCacheInterceptor.write`, `CachePlan.Write.policy`, `PutPolicy.requireSyncable`,
`CachePlan.Invalidate.Operation.applyAfterBody`, `ZetaCacheInterceptor.invalidate`.

**The advisor holds only the plan source, never the facade.** `ZetaCacheAdvisor` extends
`AbstractBeanFactoryPointcutAdvisor` and resolves the advice bean by name when the first advised bean
is proxied; the interceptor bean is lazy. Creating the advisor during BeanPostProcessor registration
therefore does not instantiate `Zeta` or its engine, so those dependencies remain eligible for every
BeanPostProcessor instead of triggering the "not eligible for auto-proxying" warning. The pointcut
still owns startup validation through `PlanResolver`, which is lightweight and depends only on the
bean factory. Owner: `ZetaCacheAdvisor`, `ZetaAnnotationAutoConfiguration`.

**A collection key is encoded by content; a `Set` is canonicalised.** A `List` or other `Iterable`
uses `l[...]` in iteration order; a `Set` uses `t[...]` with its encoded elements sorted, so
`Set.of("a", "b")` produces the same physical key on every JVM despite the implementation's
per-JVM hash salt, and a peer invalidation cannot silently miss. A `List` and a `Set` with the same
elements remain distinct shapes. A collection above 1024 elements, a self-referential collection, or
depth beyond the existing cap fails the call with a configuration error; before this branch a
`List` fell through to `o<len>:toString`, so `("foo, bar", "baz")` and `("foo", "bar, baz")` inside
an inline list could collide and a self-reference overflowed the stack. The depth cap, the ancestor
check and the lazy chain allocation live in two shared helpers used by both the collection and the
array encoder, so the two recursion shapes cannot drift. Owner:
`DefaultKey.collection`, `DefaultKey.array`, `DefaultKey.enterContainer`, `DefaultKey.ensureAncestors`,
`DefaultKey.part`.

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
method's policy and a method may hop threads without smuggling state. Each evaluation phase that
needs a context builds it from immutable inputs; a write may reuse the pre-body context after the
body returned, a read loader caches the invocation thread's context for the call's phases and
clears it on release, and the phases never overlap. A write or invalidation keeps its resolved key
in a local, so nothing per-call outlives the call. The read loader carries the
immutable inputs plus the one-shot chain, which `load` claims at most once and the foreground read
releases after `get` returns; a caller-runs load completes on the calling thread before that return,
so release cannot clear a chain a first load is still using. A retained refresh loader therefore
carries no advice chain and no evaluation state. No
static or `ThreadLocal` state carries policy across calls; a refresh still goes through
`CacheLoader.reload` (see _Annotations_).

Owner: `ZetaCacheInterceptor`, `ZetaEvaluationContext`, `CompiledExpression`, `DefaultKey`,
`SingleFlight`.

---

## Key formats and background warming

**One namespace has one physical key format.** `@ZetaConfig(keyFormat = PLAIN)` (or the global
`zeta.local.key-format`) selects between the canonical TYPED encoding
(`cache + '\u0000' + DefaultKey`) and a human-readable PLAIN encoding (`cache + ':' + value`). The
format is a namespace fact, not a method option: the same logical key must never be addressable
under two physical strings. `KeyFormat.Option` keeps `INHERIT` expressible on an annotation, while
`KeyFormat` is the resolved runtime value; the split mirrors `HeatTrigger.Option`/`HeatTrigger` and
keeps an unresolved value out of every hot path.

The decoder's scalar type is resolved against the class the plan table was scanned for
(`ResolvableType.forMethodParameter(method, index, beanClass)`), never read from the erased
declaration: a method inherited from `BaseService<ID>` declares `Object` after erasure, so a
`selfWarmLoader` decoder built from the method object alone failed startup with `unsupported
reversible scalar key type: java.lang.Object` even though the implementation fixed `ID` to `Long`.
The same resolution feeds the decoder for the single-argument and argument-list shapes;
`WarmSpec.buildDecoder` therefore applies the type arguments of the scanned class, and the method
object alone cannot answer.

A method that declares a different `cache` does not inherit the class's `keyFormat`; a method that
repeats the class's resolved cache name does, because the two declarations name the same namespace.
Treating the repeated name as "another namespace" made
`@ZetaConfig(cache = "users", keyFormat = PLAIN)` plus `@ZetaCached(cache = "users")` fail as a
PLAIN-vs-TYPED conflict at startup. A default key is rejected at resolution because the
colon-delimited form cannot invert one, so every key-bearing method in a PLAIN namespace names one
explicit single-value key. The format prints the value's text and locks no type: `Long 42` and
`String "42"` share one `users:42` entry by design, and the caller owns that consequence. Owner:
`PlanResolver.MethodResolution.encoding`, `NamespaceKeys`, `KeyEncoding`.

**Encoding belongs to the plan; inversion belongs to the warm loader.** `PlanResolver` builds a
`KeyEncoding` once and every `CachePlan.Key` inlines it, so a read allocates no registry lookup.
`KeyEncoding.Decoder` is bound to the single warmed method and parses a physical key back to that
method's arguments. Two methods in one TYPED namespace can carry different key types, so a
namespace-wide decoder would decode one method's key with another method's type and silently skip
every prewarm. This is why `ResolvedNamespacePolicy` stores no universal codec.

`DefaultKey` gives `UUID` a `U:` tag instead of its `toString()` under the generic `o:` fallback;
the nested `KeyEncoding.ScalarType` table inverts the scalar tags and is the typed planner's
decoder. Floating-point keys are deliberately not reversible: `-0.0` and `0.0` share an encoding
and NaN has no useful identity for a cache key. TYPED keys carry `U:`, integral scalars, `Boolean`,
`Character`, `BigInteger` and `BigDecimal`; anything else needs an explicit codec.

**A warmed method is an explicit capability, not an inference.** `@ZetaCached(selfWarmLoader =
true)` authorizes the framework to bind that method as its namespace's background loader. The
declaration is validated while the plan is built: no `condition`, `unless` may read only `#result`,
and the key must be exactly invertible to the method's argument list: a constant key over a
zero-argument method, one selected parameter at index 0 of a one-argument method, or the default
argument list. A constant key on a method with parameters, a `SingleArgument(1)` that would leave
another parameter null, or any SpEL expression is rejected: `#req.userId` cannot be inverted to the
`UserRequest` parameter, so a background refresh would call the method with a mismatched argument
and throw `IllegalArgumentException` on every renewal. The loader invokes the unwrapped target and
reuses the interceptor's `REFRESH_BYPASS`; retaining the proxy would re-enter the cache advice and
serve the entry being replaced.

**A warmed method's advice is reported, not judged.** `WarmSpec.of` runs during pointcut matching,
while the bean is still being proxied, so it sees the `Method` and nothing else: a transaction, a
method-security interceptor or a programmatic advisor is invisible until the container has built the
final proxy. `AnnotatedWarmLoaderBinder.bind` therefore inspects that proxy after all singletons
exist, filters each `PointcutAdvisor` by its own `ClassFilter` and `MethodMatcher` (a runtime matcher
counts as a match), skips the cache advice, Spring's plumbing (invocation exposure, exception
translation, a repository's query executor, another cache layer) and logs one WARN naming the method,
every remaining advice that applies to it, and whether the load runs on the unwrapped target or
re-enters the proxy. That last answer is `PlanResolver.isProxy(target)`, not `target == bean`: a
double proxy’s inner proxy is both the resolved bean and a chain to re-enter, and reporting it as
the unwrapped target sent a reader looking for an advisor that was in fact still on the call path.
The same predicate gates the refresh bypass, so binder and interceptor cannot disagree about what a
proxy is. The two paths fail differently: detached, a `@PreAuthorize` check is skipped,
and a body that reads the `SecurityContext` can fill a shared key with another tenant's data;
through a retained proxy (a repository, a scoped bean) the same check runs with no authentication
and throws on every load; a read-only transaction disappears when detached, so a read that routes to
a replica on the caller's thread reads the primary in the background. It is not a startup failure
because the same shapes are the ordinary ones — `@Transactional(readOnly = true)` on a read service
whose repository opens its own transaction, a Spring Data repository carrying a query executor and a
read-only transaction advisor on every query method — and rejecting them would reject loads that
return the right value.
Owner: `AnnotatedWarmLoaderBinder.warnUnhonouredAdvice`, `PlanResolver.proxyChain`.

The check covers the declaration that becomes a loader. A soft-TTL refresh reaches the same
reflective call with no loader behind it, which is why the read-path contract already requires such
a method to be free of transaction and request-scoped state: the refresh cannot be validated away
without rejecting a deployment-wide `zeta.local.soft-ttl-ms`.

_Ruled out:_ a fail-fast classifier with an allow-list and a strict switch. The allow-list would
have to track Spring Data and Spring Security releases, and a default of "reject" fails an
application that upgraded without touching its configuration — the opposite error from the one it
prevents. _Ruled out:_ an `AdvisorSafetyClassifier` SPI; the WARN already names the exact advisor a
deployment would have to remove, and an extension point would freeze that classification as public
API. _Ruled out:_ a public "is this a background refresh" probe over `REFRESH_BYPASS` — the marker is
thread-confined and exists only when the proxy is retained, so on the detached path, the one with no
advice at all, there is nothing to ask; propagating the caller's `SecurityContext` or
`TenantContext` into a loader retained for the whole TTL window would instead put one user's
identity behind a shared key. A method that needs that state registers a `ZetaWarmConfigurer` loader
and decodes the tenant from the key.

**The warm binder looks up the scanned bean class, not the declaring class.** The plan table is
built for the bean's user class, and that class is recorded on the warm spec. Two subclasses may
inherit one annotated base method and distinguish themselves through their own `@ZetaConfig`; looking
the method's declaring class up matched both beans and failed startup with "matches several beans".
The record is also what keeps the two subclasses' namespaces separate. A multi-interface JDK proxy
is the mirror case: its scan roots are interfaces, so the first root can be unrelated to the
method's declaring type. The binder then needs a root assignable from that declaring type (falling
back to the declaring type itself); using the unrelated first root matched another bean and failed
with the same "matches several beans" message. When several roots are assignable the most specific
one wins: with `BaseRepo` listed before `ChildRepo`, taking the first root read `@ZetaConfig` from
the parent and erased a `Long` id to `Object`, silently losing the child's namespace and key type.
A JDK proxy implements only its business
interfaces, so a bean whose plan was scanned for the implementation is not an instance of that
class and the type query returns nothing at all; the binder falls back to the container's Spring
proxies and matches the one whose ultimate target class is the scanned class. A type-query answer is
not automatically the right bean either: both a parent and a child bean are assignable to the
parent's scanned class, so `getBeanNamesForType(ParentServiceImpl)` returned two names and startup
failed with "matches several beans", and when the parent was a proxy the query returned only the
child — the parent's warm loader then silently loaded through the child's bean and namespace. Every
candidate from either source is therefore classified by its ultimate target class, an exact match
wins over an inherited one, and the proxy scan runs only when no candidate matched exactly. The
proxy scan is one pass per binding cycle: `bindAll` builds the `Advised` list once and every
specification that no exact match answered reuses it, because the uncached shape ran
`getBeanNamesForType` plus one `getBean` per proxy for every self-warming method, and a container
with hundreds of transaction-managed beans paid that product at startup. A candidate that is a
not-yet-created `@Lazy` bean is skipped by both sources: the type query reports its name from the
bean definition without instantiating it, so calling `getBean(name)` merely to classify the target
class forced the lazy singleton into existence. Binding runs after every non-lazy singleton exists,
so a name missing from the singleton registry at that point can only be a lazy bean; the parent's
own spec hit this through a lazy subclass that the type query reports as assignable. The same
lookup feeds `#root.targetClass`, which must name the class the body runs on: the target class is
`PlanResolver.resolveTargetClass`’s answer — an unwrapped bean’s user class, or the business
interface when the unwrapped target is itself a JDK proxy, which is exactly Boot’s double
repository proxy — and a bean that was never proxied contributes the scanned class. Reading it as
`target == bean ? scanned : ClassUtils.getUserClass(target)` did not unwrap the inner `$Proxy`, so
the advice filters were evaluated against a generated class (the WARN was skipped), the message
claimed the load ran on the unwrapped target while it re-entered the chain, and `#root.targetClass`
exposed the proxy class to `unless`. A proxy's generated class carries neither the annotations nor
the type variables. Owner:
`AnnotatedWarmLoaderBinder.findBean`, `AnnotatedWarmLoaderBinder.WarmSpec`,
`PlanResolver.selectBeanClass`.

**The interface and implementation views of one warmed method are one declaration.** A JDK proxy
scan resolves `UserService.find`; the concrete scan of the bean resolves the
`UserServiceImpl.find` that overrides it. Both carry `selfWarmLoader = true`, and comparing bean
classes alone reported "declares more than one selfWarmLoader method" and failed startup for a
correct configuration. The publisher recognises the dominance — the interface view's most-specific
method on the concrete class is the concrete spec's method — and keeps the concrete view, because
its bean class is the one the binder can find. The dominance is resolved against the bean class, not
between the two declaring types: when `UserServiceImpl extends MidService extends BaseService` and
`BaseService` implements no view at all, the body the bean dispatches to is `BaseService.find`,
which is unrelated to `UserService` by inheritance, so comparing the owners directly reported two
loaders for one annotated method. Resolving both specs' methods through the bean class answers with
that body for either view, and it is also the method the reflective warm load and the plan's
`#root.method` must name. The bean classes must be related first, though:
`UserService`/`UserServiceImpl` are, but two sibling subclasses that inherit one annotated base
method and share a namespace are not. Their specs carry the same `Method` object, so a method-only
comparison merged them and silently kept whichever bean was scanned first, warming the namespace
through one subclass on every key the other produced. Two unrelated bean classes now fail startup
whether or not they resolve to the same method. A parent and a child concrete class are not two
views of one bean either: both are scanned user classes, so they are two beans, and a child that
inherits the parent's annotated method and namespace produced two specs whose only difference was
the bean class. The assignability test accepted them as one loader and the more specific spec
silently replaced the parent's, so every key the parent produced was reloaded through the child
instance — or the other way round, depending on scan order. Only an interface and its
implementation, which a single bean can present at once, are one declaration. A scan validates its
own list before publishing anything, so a scan that fails on a later method leaves no
half-registered namespace behind. Owner: `PlanResolver.publishWarmSpecs`,
`PlanResolver.sameLoader`.

**A keyless clear names no format, and INHERIT follows a configured one.** `@ZetaInvalidate` has no
`keyFormat`, and its `@ZetaConfig` belongs to its own bean, so an invalidation that clears another
bean’s PLAIN namespace can only pass `INHERIT`. Resolving `INHERIT` to the global TYPED default
made that clear pin the namespace and fail startup with `conflicting key format: PLAIN vs TYPED`;
resolving it to "whatever is registered" instead made startup depend on bean order. The registry
therefore records where an entry came from. A declaration that names a format — an explicit
`keyFormat` option, or an `INHERIT` that is key-bearing — pins it (`ANNOTATED`); a declaration that
names a format programmatically pins it too and is marked `CONFIGURED`; a table-wide clear has no
key to encode and registers the namespace as `UNPINNED`, carrying the default format so
`physicalPrefixOf` still answers, while any later key-bearing declaration may replace that format.
A key-bearing `INHERIT` follows a `CONFIGURED` format and otherwise pins the global default, so two
annotations still cannot disagree because of scan order. A programmatic registration that names a
format over an annotation’s pinned one still fails: one namespace has one physical encoding, and a
format that silently re-encoded a live namespace would strand every existing entry. Owner:
`NamespaceKeys.resolveAnnotated`, `NamespaceKeys.resolveProgrammatic`,
`PlanResolver.MethodResolution.namespace`.

**One namespace has at most one background loader.** A namespace with several read methods of
different key shapes cannot name one canonical loader; startup rejects `selfWarmLoader` there and
asks for a `ZetaWarmConfigurer` raw loader or a namespace split. A namespace whose only key is an
untyped expression is also rejected, because no decoder can prove which value a broadcast carries.
The same rule applies to a programmatic registration: a second loader for an already-bound
namespace fails instead of overwriting the first.

**`ZetaWarmConfigurer` is the one programmatic entry.** It runs after all singletons exist, merges
its declarations with the annotation contracts, validates the capability graph, and freezes it. It
is the one registration path: the facade's `warmPolicies()` is a read-only view, because a runtime
write could overwrite a `selfWarmLoader` or a configurer registration after validation and before
freeze. `namespace(name, keyType)` derives the physical prefix from the resolved format, so a caller
never writes `'\u0000'` or `':'`. Omitting `format(...)` inherits the annotation's format (or the
global default when no annotation declared the namespace); a hard-coded `TYPED` default turned a
PLAIN namespace into a `PLAIN vs TYPED` startup failure for a caller who only wanted to add a
loader. `namespaceRaw` is the escape for a loader that parses the physical key itself, and it may
use either `loader(Function)` or `cacheLoader(CacheLoader)`: both builders write the same loader
field, and a raw registration is not key-bearing, so a PLAIN raw namespace does not have to lock a
scalar type it never decodes. Prewarm, keep-warm and a peer REFRESH all resolve the same namespace
loader, so a loader registered once serves all three paths instead of leaving the sync plane on a
separate global bean.

**Global defaults are OFF.** Registering a loader does not opt a namespace into spending
background loads. `prewarm` and `keepWarm` are independent: `softTtl` controls the read-path
stale-while-revalidate window and never switches renewal on, and a hard-only namespace can renew
just as a soft-windowed one can. A namespace that enables either trigger without a loader, or
enables prewarm under a dynamic/conflicting TTL, fails startup with the namespace and the policy
source in the message. YAML may change the activation of a known namespace, but it never declares a
loader or a TTL; an override naming a namespace no declaration registered fails startup, because an
emergency stop must not be silently swallowed by a typo.

**The policy table speaks physical prefixes only.** Every internal table of
`NamespacePolicyRegistry` — YAML overrides, annotation declarations, programmatic declarations —
is keyed by the physical prefix a key resolves to (`"users:"` or `"users\u0000"`). A
`overrides.prefixes` entry is stored as `prefix + ':'`, exactly what `CacheKeysPolicy.physicalPrefixOf`
returns for a programmatic key; the earlier bare form (`"order"`) never matched that lookup and
startup reported a legitimate override as an unknown namespace. Only a `overrides.caches` entry,
which names a logical cache before its format is known, is registered under both delimiters. A
`prefixes` entry is exact-match: accepting its alternate delimiter at freeze let
`prefixes.orders: OFF` pass startup for a TYPED `orders\u0000` namespace and then never match the
runtime key — the operator's emergency stop was silently ineffective. One key space also removes
the bare-name fallback and the second table probe from the hot path. A name declared under both
`caches` and `prefixes` fails startup: `caches` owns both delimiter forms, so the `prefixes` write
could only overwrite the `:` half and leave one namespace with two trigger sets, one per format;
the freeze accepted that split because the `\u0000` twin still carried its `dualRegistered`
alternate.

**Freeze compiles the policy graph once.** `validateAndFreeze()` resolves every known namespace into
one immutable prefix map plus a default policy, then rejects any later registration. The map, the
default policy and the any-keep-warm aggregate are published as one immutable `FrozenSnapshot`, so
a reader cannot see a new map beside an old aggregate. Before the
snapshot, each `resolve(key)` split the key, looked through three concurrent maps with fallbacks and
allocated a fresh `TtlContract`/`ResolvedNamespacePolicy` pair — on the decision, prewarm and
scheduler paths. A frozen `resolve` is one prefix read and one map lookup with no temporary policy.
The TTL comparison also runs on _effective_ values (`TtlPolicy.resolveHardTtl`/`resolveSoftTtl`):
`hardTtl = ""` and an explicit value equal to the global default used to be compared raw, which
marked the namespace `dynamic` and silently disabled cold prewarm. A bean first proxied after the
freeze (`@Lazy`, prototype, a later context) still builds and runs its plan, but its late contract
publication is ignored with a warning instead of failing bean creation; that namespace is simply not
background-warmed. That tolerance is the annotation's alone, because its declaring bean can
legitimately outlive the freeze. A programmatic registration or a fallback loader after the freeze
throws: the snapshot is published, so the declaration could only be dropped while
`WarmRegistry.Registration.register()` returned normally and removed it from the pending set — the
owner would believe a namespace is warmed, and the operator would see no error and no coverage.
Owner: `NamespacePolicyRegistry.validateAndFreeze`, `resolve`, `registerAnnotatedNamespace`,
`registerProgrammaticNamespace`, `setFallbackLoader`.

**One warm startup order.** `WarmRegistryFinalizer` runs configurers, then the annotated warm-loader
binders, then `NamespaceKeys.validateOnStartup`, then the policy freeze, and only then attaches
`KeepWarm` to the cache, access counter and detector. Two independent
`SmartInitializingSingleton` beans previously ran in registration order, so the renewal scheduler
could observe the registry before `bindAll()` and the freeze had completed; one initializer makes
the order a property of the code. The same merge removed `ZetaWarmPolicyCustomizer`, whose
registration ran while the facade bean was created and bypassed both the freeze and the PLAIN
prefix rule. Owner: `WarmRegistryFinalizer`, `ZetaWarmConfigurer`, `WarmRegistry`.

**The plan resolver resolves its registries after BeanPostProcessor registration.**
`zetaPlanResolver` and `ZetaCacheAdvisor` are infrastructure-role beans, and the advisor is
instantiated while the auto-proxy creator initializes — before `registerBeanPostProcessors`. A
constructor that resolved `NamespacePolicyRegistry` and `NamespaceKeys` there pulled
`ZetaAutoConfiguration`, `ZetaProperties`, the namespace registry and the user's cluster
`CacheLoader` (through `clusterLoaderProvider.getIfAvailable()`) into existence ahead of their
post-processing; Spring reports each of them as "not eligible for getting processed by all
BeanPostProcessors", and they stay without the post-processors registered later. The resolver
holds two suppliers and resolves each once, on the first plan that publishes a namespace contract
or format — a bean-creation callback rather than an infrastructure one. Owner: `PlanResolver`,
`ZetaAnnotationAutoConfiguration.zetaPlanResolver`.

Owner: `KeyFormat`, `KeyFormat.Option`, `NamespaceKeys`, `KeyEncoding`, `KeyEncoding.Decoder`,
`AnnotatedWarmLoaderBinder`, `ZetaWarmConfigurer`, `NamespacePolicyRegistry`, `WarmRegistry`,
`WarmRegistryFinalizer`.

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
void    refresh(key, reader|loader, boolean sync) // value-only renewal; false = local
void    refreshAll(loaders, boolean sync)       // batch form of refresh
```

**Options are closed per family.** `GetPolicy` carries only read knobs, `PutPolicy` only write
knobs, `InvalidatePolicy` only invalidation knobs. A mis-scoped knob is a compile error instead
of a silent no-op. A `null` options argument selects the verb's no-options default; `refresh` takes
no options record at all — its `boolean sync` flag is explicit, and `false` is the local default.

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
existing fallback — HOT keeps its current L1 entry or skips with `NO_PROMOTABLE_VALUE`, REFRESH
degrades to local invalidation with `VALUE_DECLINED_BY_LOADER` — and a value the application
declared non-cacheable can never be pinned for a
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

**Policies decide; the framework applies and reports.** The heat plane has one replaceable
strategy: `HotKeyHandler.onHot`/`onCool(HotKeyContext)` returns nothing and receives only guarded
operations plus the applied decision's read-only facts, so a handler can request a prewarm or
re-judge a renewal but cannot write an entry or a deadline directly; `HotKeyListener` observes
applied and skipped decisions. The sync
plane keeps `SyncPolicy.admit`/`settle`, receiving a read-only `EntryView` and the clean
`SyncEvent`, never a write context, a `Batch` or a `CacheEntry` it can mutate. The framework calls
the policy for the effective value event (or each granted clear) only, then applies under the
atomic version/tombstone guards and fires exactly one terminal hook — including `POLICY_REJECTED`
and `POLICY_FAILED` for a refusal or an exception. A `null` policy result is a broken policy: it
logs one WARN and settles as `POLICY_FAILED`. The previous shape handed a policy a mutable write
context; a policy that forgot `context.skip()` left a covered message without its terminal
callback, and one that called `context.value` twice could double-apply a value. Owner:
`HotKeyHandler`, `HotKeyListener`, `HotKeyContext`, `SyncPolicy`, `DefaultSyncContext`, `EntryView`,
`SyncEvent`.

**The AMQP consumer thread never runs a user hook or a table sweep.** A refused sync message
is applied by `DefaultSyncContext.fallbackEvict`: the INVALIDATE path is an O(1)
tombstone + removal, the CLEAR path records the watermarks and kills the pre-clear flights,
then hands the table sweep to the applier's sweep executor (coalesced; inline only when the
executor refuses during shutdown/saturation). The policy and `SyncHook` run only on the
ordered dispatcher. The previous fallback called the hook-bearing `invalidate` and the
sweeping `clear` on the consumer thread; a slow `SyncHook` delayed `basicAck` until the
heartbeat window closed and the broker dropped the channel, and a rejected clear locked the
consumer behind an O(N) walk. Owner: `CacheSyncListener.onDispatchRejected`,
`DefaultSyncContext`, `SyncHook`.

**A rejected clear is still swept, not just watermarked.** The clear watermark is a write-path
guard: `staleAgainstInvalidation` refuses later fills, but the read path does not consult it.
Recording only the watermark would leave every resident entry servable until its hard TTL.
The consumer-thread fallback therefore records the marks, kills the covered flights and
schedules the sweep; the watermark alone is never treated as an invalidation of existing
entries. Owner: `DefaultSyncContext.fallbackEvict`, `EntryLifecycleImpl.staleAgainstInvalidation`,
`HotKeyCache` read path.

**A PUT claims the dedup slot.** `broadcastPut` records the same `(version, type)` claim as a
REFRESH/INVALIDATE before publishing. A same- or lower-version REFRESH is suppressed because
the value is already on the wire; an equal-version INVALIDATE still replaces the claim. A
failed PUT releases the claim, so a later REFRESH is not lost. Without the claim, a REFRESH at
the PUT's exact version passed the receiver's equal-version rule (`shouldSkipForRefresh` applies
an equal stamp) and forced every peer through the cluster loader for a value it already had. Every
deduped send validates the key and a positive version before claiming, so a message the receiver
drops as unorderable cannot occupy the slot and suppress a later valid retry.
Owner: `CacheSyncPublisher.publishDeduped`, `BroadcastClaim`.

**An oversized batch invalidation is chunked, never dropped.** `broadcastInvalidateBatch`
filters invalid entries, splits the rest at `SyncMessage.MAX_BATCH_ENTRIES` and falls back to
per-key INVALIDATEs when one frame cannot be encoded. The previous shape returned silently for
more than 1000 entries, leaving every peer stale until the hard TTL; the normal facade already
chunked, so the drop only hit direct publisher callers and future call sites. Owner:
`CacheSyncPublisher.broadcastInvalidateBatch`, `SyncMessage.encodeBatchBody`.

**Deployment identity is an instance value, not a JVM-global override.** `NodeIdentity` is a
record Bean resolved per application context; queue names, publisher/listener origin headers
and the stamp clock read that Bean. `AmqpConsumerProperties` exposes only the configured queue
prefix; the auto-configuration composes `{prefix}:{instanceId}`. The previous static
`setInstanceOverride`/`getInstanceId` made two contexts in one JVM share one queue name: the
fanout then became competing consumers and the self-filter dropped the other context's
messages. A context with no configured id still auto-detects one per context.

**The auto-detected instance id carries a per-resolution random suffix.** The fallback is
`{server.port|SERVER_PORT|instance}-{HOSTNAME|local}-{8 hex}`. The host and port are readability,
not identity: `server.port` is absent from the system properties when it lives in
`application.yml`, every container on a host sees the same `HOSTNAME`, and every context in one JVM
sees the same process UUID. Any of those pairs made two instances resolve the same id, which then
gave them the same queue name (competing consumers, each losing most invalidation messages) and the
same derived 10-bit node id (equal stamps in one millisecond). A random suffix per call is the only
component that is unique by construction. Owner: `NodeIdentity.autoInstanceId`,
`ZetaFacadeAutoConfiguration`, `WorkerAutoConfiguration`.

**The ghost-COOL ring reconcile is lock-free-only.** The heartbeat listener reconciles the ring
on membership changes. `isStaleCrossNodeCool` reconciles inline only in the lock-free pre-check,
and only when the entry's owner is absent from the published ring, closing the race where a newly
joined owner arrives before the rebuild. The in-compute re-check reads the same published
snapshot and fails open when the owner is absent: moving `rebuildIfPossible` under the bin lock
serialized every key hashing to that bin behind the health-view walk, so one unlucky concurrent
COOL could stall unrelated reads and writes for the whole probe. The refusal reason is captured
inside the entry compute, so one COOL performs one ownership probe and emits one ghost line even
when a concurrent replacement made the outer snapshot stale. Owner:
`HeatDirectory.isStaleCrossNodeCool`, `HashRing.rebuildIfPossible`, the heartbeat
listener.

**Validation happens before any cache work, at one boundary.** A blank single key throws
`IllegalArgumentException`, a null callback `NullPointerException`, a negative TTL or top-K limit
`IllegalArgumentException`. Collection arguments are validated element by element at the same
boundary and **filtered instead of rejected**: `getAll` and `invalidate(Collection)` drop the
null/blank elements (order and duplicates preserved) before the engine is called, and an
all-invalid collection is a no-op. `forgetDedup(String...)` and `putAll` reject instead: a raw
dedup mutation names exact slots and a batch write is caller intent, so neither may be partially
applied. The asymmetry is deliberate — a batch assembled from a query result legitimately contains
no-key rows, while a blank single key is a direct caller error. `CacheKeysPolicy` owns both halves
of the rule (`invalidCacheKey`, `validKeys`); the engine assumes valid keys on every path,
single-key and collection alike.

A read policy's TTL suppliers are lazy, so the facade cannot evaluate them up front without losing
the hit-path optimization. `get` therefore validates a TTL where it is first needed:
`ReadContext` rejects a negative supplier result at evaluation. The batch `getAll` has no lazy
per-key path, so `DefaultZeta` resolves the shared TTLs once and rejects a negative value before the
engine call, even when the key collection is empty. Owner: `DefaultZeta.getAll`, `ReadContext`.

**The decision context carries the event facts; the callback signature carries nothing else.**
`HotKeyHandler.onHot`/`onCool` take one argument. `HotKeyContext` exposes `decision()` and
`isNewlyHot()` beside the guarded operations, so a new per-decision fact is a getter rather than a
fourth parameter on a method every handler overrides. A context is created per invocation and never
memoized per key: the facts belong to one applied decision, and a cached instance would answer a
later handler with the previous event's `newlyHot`. `isNewlyHot()` is `false` on a COOL context,
which is truthful — a COOL grants nothing. The previous shape passed
`(decision, context, newlyHot)`: a repeat HOT and a first grant reached the handler through a
positional boolean every override had to restate, and the next fact would have broken every
override again. Owner: `HotKeyContext`, `HotKeyContext.Factory`, `HotKeyHandler`,
`WorkerListener.invokeHandler`.

**A cancellation is a re-judgment, not a switch.** `HotKeyContext.cancelKeepWarmIfCold` and its two
internal callers — `WorkerListener`'s passive lease end and its dispatch-rejected COOL — delegate to
`WarmScheduler.cancelIfCold`, which resolves the namespace policy and drops the task only when the
keep-warm trigger no longer accepts any live heat signal. The unconditional `cancelKeepWarm()` is
gone: it held only until the next read, write or admission re-armed the task from that same
trigger, so a handler that believed it had stopped the renewal watched the next tide restore it.
Deleting it also removed a context allocation from those two paths, which hold a lease or a
rejected decision and never needed the guarded operations. Owner: `HotKeyContext`,
`HotKeyHandler`, `WarmScheduler.cancelIfCold`, `KeepWarm.cancelIfCold`,
`WorkerListener.cancelColdRenewal`.

**Eligibility is answered by the action, not by a sibling predicate.** `HotKeyContext` exposes
`armKeepWarm(): boolean` and `prewarmIfAbsent(): CompletableFuture<Boolean>` instead of
`canPrewarm()`/`shouldKeepWarm()`/`isTtlPrewarmable()` predicates. The action already re-checks the
namespace policy, so a companion predicate would be a second copy of the same gates and would drift
from the copy that decides; a predicate that answers "yes" while the action refuses is worse than
no predicate at all. The same rule retired the side effect that made `prewarmIfAbsent()` arm a
renewal for a resident key: an action named for one job now does only that job, and the default
`onHot` branches on `hasLiveEntry()`. Previously a handler calling `prewarmIfAbsent()` on an absent
key could neither observe nor prevent a renewal armed for a resident one, and "renew only" had to be
expressed through an action named "prewarm". Owner: `HotKeyContext`, `HotKeyHandler.onHot`,
`WarmScheduler.scheduleIfEligible`.

**A namespace is compared by its logical name.** `CacheKeysPolicy.logicalNamespaceOf` drops the
encoding delimiter, so `users\u0000u-1` (TYPED) and `users:u-1` (PLAIN) both answer `users`;
`HotKeyContext.namespace()` and `SyncEvent.namespace()` delegate to it, and a policy writes
`"users".equals(...)` without knowing which layer produced the key.
`physicalPrefixOf` keeps the delimiter because a registry keyed by physical prefix needs it, and the
two forms must not be mixed: a comparison against the physical form answers `false` for every
annotation key. The prefix
ends at the first delimiter of either kind: a `PLAIN` value containing U+0000 (`users:a\u0000b`)
and a key starting with a delimiter (`\u0000a:b`) previously resolved to a namespace containing the
delimiter, so policy lookup and circuit-breaker sharding put the key in the wrong bucket.
`namespaceEnd` now scans once and stops at the first delimiter. A third accessor,
`namespaceOf`, answered TYPED with the delimiter and PLAIN without it — neither a logical name nor a
physical prefix — and its Javadoc still claimed the TTL registry resolved through it long after the
registry moved to `physicalPrefixOf`; nothing in the library called it, so it is gone rather than
left as a trap for the next caller. `SyncEvent.affectsNamespace`
covers all three event shapes — a key matches by name, a table-wide
clear matches everything, and a prefix clear ranges over a namespace when the prefix is a raw
prefix of the name or continues past it at a delimiter boundary, so a clear of `users\u0000u-`
never reads as reaching `user`. Owner: `CacheKeysPolicy`, `SyncEvent`, `HotKeyContext`.

**An admission that fits the event never leaves a stale entry.** `INVALIDATE_LOCAL` on an INVALIDATE
used to be settled as `POLICY_FAILED` and skip the eviction, while the same answer on a clear was
accepted as `PROCEED`. A policy that answered "invalidate locally" for a whole namespace therefore
left the peer's stale value resident on every INVALIDATE, and the only trace was a skip reason that
looked like a broken policy. The event already removes the local copy, so the answer now proceeds
on INVALIDATE and the clear family alike; only a value event degrades. `POLICY_FAILED` keeps its
real meaning: an unactionable `SyncValueAction` or a throwing policy. Owner:
`DefaultSyncContext.applyInvalidate`, `SyncAdmission`.

**A warm declaration that was never published fails startup.** `DefaultWarmRegistry` tracks every
builder it hands out and `assertFullyRegistered` rejects the ones still pending before the policy
registry freezes. Previously a configurer that forgot `.register()` dropped the whole declaration
silently: the namespace kept the annotation defaults, no trigger fired, and nothing failed. The
same builder now also accepts a triggers-only registration when another declaration (an annotation
`selfWarmLoader`) supplies the loader, so one configurer can manage triggers without restating the
value path; the freeze-time validation still rejects an active trigger with no loader anywhere.
Owner: `DefaultWarmRegistry`, `WarmRegistryFinalizer`.

**A primitive key type is boxed when the loader is built.** `namespace("users", long.class)` is
legal Java — `long.class` has type `Class<Long>` — and `ScalarType.require` accepts primitives,
but `long.class.cast(Long.valueOf(42))` threw `ClassCastException` on every background load:
compilation and startup passed and the first prewarm failed. `DefaultWarmRegistry.adaptTyped`
resolves the primitive to its wrapper once, so the decoder's boxed argument casts cleanly at zero
runtime cost. Owner: `DefaultWarmRegistry.adaptTyped`, `DefaultWarmRegistry.decodeKey`.

**No internal type appears in a public signature.** `HotKeyContextFactory` moved out of
`HotKeyContext`, so the extension interface no longer publishes a factory over `HeatView`, the
cache and the policy registry; `WarmRegistry.create` moved to the `@Internal`
`DefaultWarmRegistry`; `HeatTrigger.matches(HeatView, String)` became
`HeatView.matches(HeatTrigger, String)`, which keeps the public `HeatTrigger` free of the internal
heat view while `matches(boolean, boolean)` remains the composition primitive a caller can use with
values it already has. `TtlContract` lost its `@Internal` because `ResolvedNamespacePolicy` publishes
it through `Zeta.warmPolicies()`, while `SyncMessage` and `Batch` gained it because they are
transport and reduction shapes the policy never sees. Owner: `HotKeyContextFactory`,
`DefaultWarmRegistry`, `HeatView`, `TtlContract`, `SyncMessage`, `Batch`.

**A named predicate replaces a repeated pair.** `EntryView.isLive(view)` is the null-safe
"present and not past its hard deadline" test, and `SyncType.isValue()`/`SyncEvent.isValue()` is the
one definition of the value path. Both replaced idioms that call sites copied by hand — the null
check was easy to drop, and `type() == PUT || type() == REFRESH` is exactly the pair a policy
forgets on INVALIDATE. Owner: `EntryView`, `SyncType`, `SyncEvent`.

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

**The AMQP ack runs outside the processing catch.** `AmqpConsume.ackOrNack` wraps only the
plane's `body.run()` in the failure catch and calls `channel.basicAck(tag, false)` after it.
An ack failure is a channel failure, not a processing failure: it must propagate to the Spring
listener container, which owns channel recovery, and it must not be converted into a nack of a
delivery whose body already ran. The rejected shape kept `basicAck` inside the try, so an
`IOException` or a runtime `AlreadyClosedException` from the ack entered the catch: the
delivery was logged as a `{} processing failed` line with the body and the cause, and
`basicNack` was then attempted on the same broken channel. The observable symptoms were a
misleading ERROR that blamed the payload for a channel loss, and a delivery that had already
been applied being settled as failed (discarded or dead-lettered) instead of acked; when the
nack also threw, the nack's exception replaced the ack's, so the container saw the wrong
failure and could not attribute it to the channel. Owner: `AmqpConsume.ackOrNack`,
`CacheSyncListener.onMessage`, `WorkerListener.onMessage`.

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
queue depth, in-flight batches, lease debt, clock skew, `zeta.topk.count` (decay-adjusted recent
traffic, not a lifetime count) — stays a gauge. Registering a monotonic total as a gauge gives
dashboards a flat line and breaks `rate()`. Owner: `ZetaMicrometerAutoConfiguration`,
`WorkerDetectionMetrics`.

An unstarted component reads as `0` on its counters and `-1` on the live gauges that deliberately
expose absence. A counter never returns `-1`, because a negative sample is not a valid counter
value.

**A gauge whose state object is a binder-created method reference must opt into a strong
reference.** Micrometer's `Gauge` holds its state object weakly by default. The queue-depth and
wheel-count suppliers are method references created in `WorkerDetectionMetrics.bindMeters` (from
`WorkerAutoConfiguration.workerDetectionMeterBinder`), and no field owns them; a young collection can
therefore clear the supplier and the gauge starts reading `NaN`. `bindMeters` registers
`zeta.worker.broadcast.hot.depth`, `zeta.worker.broadcast.cool.depth` and
`zeta.worker.cooling.scheduled` with `.strongReference(true)`. The captured dispatcher and wheels are
application-lifetime beans, so the strong reference extends nothing that would otherwise be
collectible. Without it, a queue-depth alert disappears exactly when the process is under memory
pressure and the backlog is most likely. Owner: `WorkerAutoConfiguration.workerDetectionMeterBinder`,
`WorkerDetectionMetrics.bindMeters`.

**Registration is completed after singleton creation, through the registered binder beans.** A
`MeterBinder` can be initialized during the early reporter/access-counter cycle, before the cache,
detector or reporter it reads exists; its `ObjectProvider` checks then register nothing and every
`zeta.*` series silently disappears. A late `SmartInitializingSingleton` resolves the
`hotKeyCaffeineMetrics` and `hotKeyCustomMetrics` beans by name and binds those instances again
after all singletons are ready; Micrometer returns the existing meter for an identical id, so a
successful early bind is harmless. The previous shape called the `@Bean` factory methods directly
(bypassing a user override under the same name) and skipped the re-bind whenever any `zeta.*` meter
already existed, so one early meter — `zeta.version.clock.skew` or `zeta.worker.alive` — silently
suppressed every late series. Owner: `ZetaMicrometerAutoConfiguration.zetaLateMetrics`.

**A per-rule total is a `LongAdder`, and the Micrometer meter is a side channel.** `RuleCounters`
holds four adders (`matches`, `promotions`, `cools`, `aborts`) plus three `Counter` handles
registered when the rule is published. A rule event increments its adder and, when bound, the meter;
the `RuleBinding` and the endpoint read the adder. A late cool-down after the id is retired still
lands on the adder (the binding keeps its counters), while the removed meter is never scraped again.
Registration happens only after the whole snapshot is built, so a publication that fails while
deriving its indexes registers nothing. Owner: `WorkerDetectionMetrics`, `RuleResolver`,
`RuleBinding`, `RuleCounters`.

_Ruled out:_ reading `Counter.count()` as the total. It is a per-step delta on a step registry and
a permanent zero without a registry, so the endpoint could report zero matches/promotions/cools
while `aborts` was non-zero and the seat ledger invariant broke. The adders keep the total correct
for every backend; the cost is one extra add per rule event. Owner: `RuleCounters`.

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

**The consumer-plane configuration is one abstract field base.** `AmqpConsumerProperties` carries
everything the two AMQP listener planes build their container and jitter scheduler from — exchange,
queue prefix, consumer concurrency, prefetch, scheduler pool size and auto-startup — so
`SyncConfiguration` and `WorkerListenerConfiguration` share one builder and a change to the
container contract cannot drift between them. The per-instance queue name is composed by the
auto-configuration from that prefix and the injected `NodeIdentity`, so the configuration object
never derives identity. `CacheSyncProperties` and `WorkerListenerProperties` extend the abstract
base, set only their exchange, queue prefix and plane-specific defaults, and keep their own knobs.
A separate read-only interface mirrored the same nine getters and had no second implementer; the
abstract class is the one shape both the binder and the auto-configuration need. Owner:
`AmqpConsumerProperties`, `ZetaAmqpAutoConfiguration`.

**Published strategy SPIs stay interfaces.** `SyncPolicy` and `HotKeyHandler` are
documented in the README as caller-replaceable: the interface's default methods are the standard
actions (run the framework's admit/settle pipeline, prewarm on HOT and do nothing on COOL), and a
`DefaultSyncContext` and the default `HotKeyContext` are the framework-owned write paths; the
policy/handler is the published seam. The same applies to `SingleFlight`, `CacheLoader`, `TopK`,
`SyncHook`, `HotKeyListener` and the message records — each has a real second implementation (test
doubles, benchmark stubs or caller-supplied loaders).

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

**The ring is rebuilt, never patched.** `HashRing` publishes a fresh `(hashes, nodes)` snapshot
through one `AtomicReference`; readers binary-search it and walk at most one full cycle looking
for a live node. A live-set change rebuilds; an unchanged set only republishes nothing. The hash
is the pinned 32-bit mixed hash — changing it reshuffles every key.

Owner: `circuitbreaker`, `EntryLifecycle`, `HealthView`, `ConsistentHashRing`,
`ReportFeedLoop`, `TtlPolicy`, `AmqpMessageReader`.

---

## Ruled out

Each entry was implemented, measured or reasoned about, then removed. Do not re-propose one
without new evidence.

**A policy cache for the accepted ledger.** `Caffeine` with `maximumSize` and a state-dependent
`Expiry` held the per-key wire records. Both facts it expressed were right — the cap for a COOL lost
at queue saturation, the state-dependent retention — but the _order_ it evicted in was not its to
choose: W-TinyLFU ranks by recency and a frequency sketch, and a key that stays hot for hours under
`COOL_FOR_LOW_QPS` produces no lookups at all, so a wave of short-lived COOL keys evicted precisely
the record proving the cluster had been told HOT. The ledger is now two insertion-ordered tiers owned
by the drain thread, with the cap taking the oldest COOL record first (see the cap entry under
decision delivery). The `Expiry` was also the reason the class held a second clock — Caffeine's own
`Ticker` over `System.nanoTime()` — for a deadline set that otherwise shares
`TimeSource.monotonicMillis()`. Owner: `WorkerBroadcaster.AcceptedStore`.

**One semaphore release per successful submission.** `submit` released the submission semaphore on
every successful offer, and the drain discarded the surplus permits at the end of each round. During a
heat wave the drain is parked between tokens (one per 2 ms at the default 500 HOT broadcasts/s), so
every offer of a decision that was neither the head nor able to invalidate one still woke it: the
round could send nothing, probed the head three times and parked again, and the report threads
contended the AQS state for permits the drain then threw away. The wait conditions are now published
in a 3-bit `wakeMask` (`WAKE_HOT`, `WAKE_COOL`, `WAKE_SUPERSEDE`, the last one for a displaced HOT
only), so a submission wakes the drain only when it supplies one of them and a burst collapses into
one wakeup (see the wake entry under decision delivery). Owner:
`DecisionDispatcher.signalIfNeeded`.

**Lease-on-failure (background-refresh keep-alive).** On every failed refresh the engine extended
the hard deadline to `now + max(remaining, 120 s)`, capped at a 30-minute entry age, re-opened the
retry window at the lease midpoint, and consulted a `LeaseClassifier` to refuse definitive errors.
It made a declared 5-minute hard TTL a 30-minute one on exactly the keys whose source was
unhealthy, and it turned a business exception raised by the loader into a renewal of data the
business had already deleted; the classifier, the staleness cap, the lease table and its meters all
existed to bound a promise the framework should not have made. The replacement keeps the
operational property without the extension: a failed or empty refresh closes the retry window at
the existing hard deadline, so the failing source is not re-hit per read and the entry still exits
when its owner said it would. _Ruled out:_ keeping the lease behind a flag — "bounded staleness of
up to 30 minutes" has to be expressed by the caller's TTL, or the number in the annotation means
nothing.

**A per-namespace Hot TTL policy (`HotTtlPolicy.PRESERVE` / `EXTEND_*`).** It let a HOT decision
rewrite an entry's windows from a "normal baseline" carried on the entry, so the same key could be
served for an hour under a five-minute declaration once traffic rose — the hottest keys were the
stalest data. It also made every load, write, refresh, peer apply and promotion responsible for
carrying four durations and the baseline forward; one missed copy changed the caller's freshness
contract silently. The 2.0 model removed the policy, the per-entry decision stamp, the state machine
and the dual baseline together, and made the caller's declaration the only lifetime. _Ruled out:_
keeping the mechanism behind a default of `PRESERVE` — a second owner of the deadline that is
merely switched off is still a second owner, and every rewrite site still had to preserve it.
Owner: `TtlPolicy`, `CacheEntry`.

**Two independent volatile watermark reads.** The claim was that the per-key `compute` made the
pair safe, because a clear records its table watermark before the sweep and a fill either observes
the watermark or is removed by the sweep. It does not: the prefix map is pruned in the same write,
so a reader that sampled the old table stamp and then the post-clear empty map observed a state
that never existed and admitted a stamp both watermarks refused — a deleted entry resurrected in
L1. The table stamp and the prefix map now publish as one immutable snapshot behind a single
volatile reference, which also removes one volatile read from the write hot path. Owner:
`EntryLifecycleImpl`.

**`CompletableFuture.join()` for a follower.** It ignores interrupts, so a follower of an inline
leader — the path with no timeout budget — could not respond to shutdown or request cancellation.
The join uses `get()` and restores only the waiting thread's flag; the reader's flag is restored
by the thread that observed the interrupt. The reader-failure tag is a plain `RuntimeException`
for the same reason: `get()` unwraps a `CompletionException` and would lose the tag. Owner:
`LoadFlights.settle`, `runInline`, `loadIsolated`.

**A `cacheNull` annotation flag with a `NullValue` sentinel.** Caching a null as an internal
marker would have broken the engine-wide "null is never stored" contract for one annotation
namespace, leaked the sentinel into the model package and sync decoding, and made a programmatic
`zeta.get` on the same physical key return an opaque enum. Negative caching is a domain decision
with a TTL and a business meaning; the supported paths are a real absence value with a short TTL
or an explicit programmatic `put`, not an annotation switch. Owner: `ZetaCacheInterceptor`, `CachePlan`.

**The built-in timed refresh.** `Zeta.refresh().registerRefresh` scheduled a `get` tick at
`softTtl × 1.1`: every cycle recorded a phantom access, it could not express a value-only renewal,
and it duplicated scheduling and lifecycle control the caller already owns. The replacement is the
caller's own scheduler plus `refresh`. The cost is explicit: a JDK `ScheduledExecutorService`
cancels a fixed-delay chain on the first uncaught loader exception, so a scheduled refresh must
catch `Throwable` (`PeriodicScheduler` stays internal). Owner: `Zeta.refresh`.

**Expiring a hot accepted record at twice the queue age.** The age bound orders decisions that are
still queued; it says nothing about how long the cluster can hold a hot key. With the record expired,
a stale HOT or an old COOL found an empty record and was re-sent: the App's version guard made the
replay harmless, but the send was wasted and the long hot episode lost its ordering and debounce
watermark. A record whose last wire state is HOT is retained until a COOL reaches the wire, a COOL
publish fails or the COOL expires; only a COOL-state record keeps the queue-age bound. The expiry
distinguishes the wire-state HOT bit from the retention bit, so a lost COOL can shorten retention
without lying about the last state that reached the broker. Owner:
`WorkerBroadcaster.AcceptedDecision`.

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
that can reject a write is a synced hard TTL resolving to permanent; `CachePlan.Ttl.evaluate` degrades
every other runtime failure to the configured default. Evaluating the rest before the body spent a SpEL
context and an evaluation on writes whose body returned `null` or whose `unless` vetoed the value, and
moved result-independent TTLs that read mutable target state to the pre-body snapshot. The interceptor
now preflights only the synced dynamic hard TTL and evaluates the rest in the shared post-body context.
Owner: `ZetaCacheInterceptor`, `CachePlan.Ttl`.

**Allowing a business parameter to shadow a different argument's `#pN`/`#aN`.** A parameter named
`p0` at index 1 would overwrite the generated positional slot in `ParameterBindings`, so the key
fast path and the SpEL lookup would disagree. Writing the aliases last only fixed the in-range
case: on `find(String p1)` — one parameter, named `p1` — no alias `#p1` exists, so the discovered
name survived and `#p1` silently addressed slot 0, while the framework's own contract says an
out-of-range `#pN` fails startup. Every discovered name that parses as a positional alias is
therefore never entered in the slot table: `#pN`/`#aN` always address parameter `N`, an out-of-range
alias is rejected by the variable check with its range diagnostic, and a parameter named for its own
slot resolves identically. A method whose parameters are literally named `p1`/`p0` still resolves
those aliases positionally. Owner: `ParameterBindings.of`, `ParameterBindings`,
`CompiledExpression.argumentIndex`.

**Rejecting a parameter named `result`, `root` or `this` at resolution.** The first shape treated
all four context names alike and failed an ordinary `save(TaskResult result, String bizId)` even
when the expression only read `#bizId`. It was unnecessary: SpEL binds `#root`/`#this` ahead of the
argument table, and `ZetaEvaluationContext.lookupVariable` returns a bound `#result` before the
table, so a parameter with one of those names can never shadow the reserved binding. The key fast
path also had to learn the same rule — `ParameterBindings` omits the three context names and
`CompiledExpression.argumentIndex(variable)` returns `-1` for them — otherwise
`@ZetaPut(key = "#result")` with a parameter named `result` would have addressed the argument
instead of the body result. A parameter named `result`
is therefore legal and is reachable only through `#pN`/`#aN`; `pN`/`aN` remain reserved. Owner:
`ParameterBindings.of`, `CompiledExpression.argumentIndex`, `CachePlan.Key`.

**Relying on `MethodBasedEvaluationContext` for lazy parameter binding.** Its lazy load is
all-or-nothing: the first lookup of any name it cannot resolve runs `lazyLoadArguments`, which calls
the parameter-name discoverer again (Spring does not cache it, and `getParameters()` clones the
`Parameter` array) and writes `3N` entries into the context's variable map before the lookup
returns. A context is built per evaluation phase, so every expression that read a variable paid that
once. A bound `null` result was also indistinguishable from an unbound variable, so reading
`#result` on a null-returning method triggered the same load. `ZetaEvaluationContext` implements
`EvaluationContext` directly with shared reflective delegates and resolves a name against the plan's
precomputed `ParameterBindings` — one map read, no scan of the parameter names and no
`Method.getParameterTypes()` clone — while `#result` is a field plus a bound flag, so a lookup
allocates nothing, performs no reflection and writes no entry. `setVariable` rejects every write, so
no expression can introduce state for a later phase. The slot table is built once per plan and
shared by every expression and every call. Owner: `ZetaEvaluationContext`, `ParameterBindings`,
`CompiledExpression`, `PlanResolver`, `CachePlan.Key`.

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
decision stamp so the (then current) Decision-Validity probe could skip one map lookup. An isolated,
clock-free JMH probe put the slot form at ~3.5 ns against ~8.3 ns for the map-plus-array fallback;
with the monotonic clock read both forms pay (~45–50 ns), which dominates the difference. A
slotless design also drops the array from the fallback, leaving a net advantage of a few
nanoseconds at the cost of a second index, a second table, a stamp field and three `HealthView`
methods. At 1M HOT probes/s the table bought well under 0.5% of a core. Removed; `DecisionStamp`
carries only
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
contiguous `long[]` (2026-10). Owner: `SlidingWindowBreaker`.

**A monitor per breaker bucket.** Each time slice carried its own `Bucket` monitor, on the premise
that ten buckets spread settlement contention across the window. They do not: the buckets are clock
slices, not thread shards, so every concurrent settlement of the live slice targets the same
monitor while the other nine sit read-only until the clock reaches them. The monitor also made the
rate scan take one lock per bucket. Replaced by one packed `window | failures | successes` word in
an `AtomicLongArray`: the rotation is one CAS, the scan is one atomic read per bucket, and the ten
objects disappear (2026-11, design decisions: load protection). Owner: `SlidingWindowBreaker`.

**Folding the breaker's `Epoch` into one `long`.** All nine fields fit the bit budget only after
truncating the generation and the timestamp precision, and the premise was that CLOSED traffic pays
for them. It does not: the CLOSED path only reads the immutable reference, and a new `Epoch` is
allocated a handful of times per HALF_OPEN episode — a few objects per recovery, not per request.
Trading timestamp wraparound and generation truncation for those allocations is a negative-return
optimisation, so the reference stays (2026-11). Owner: `SlidingWindowBreaker`.

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
(2026-10). Owner: `SlidingWindowBreaker`.

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
`SlidingWindowBreaker`.

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
24.85 days), which a long-lived hot key reaches routinely. The torn two-read check in
`needsRepair` is fixed by loading `wheelState` into a local; that is the whole consistency benefit
packing would buy, without joining the wheel and stripe domains. Owner: `HE`, `TimerWheel`,
`HotKeyRegistry.needsRepair`.

**Applying a self-INVALIDATE echo (removed).** `SyncType.ignoresSelf()` exempted the single-key and
batched INVALIDATE from the loopback filter, on the theory that re-applying the removal on the
sender healed a reload that started before the invalidation was stamped. It did not: `dropLocal`
records the tombstone before its bin-locked removal and every write path re-checks that tombstone
inside the same `asMap().compute`, so the older fill is either removed or refused without the echo.
What the echo did do was break `InvalidatePolicy.Scope.REMOTE`, which promises to leave local L1
untouched: the sender's own broadcast came back and deleted the entry it had just refused to delete.
It also re-invalidated the key's SingleFlight at a stamp the sender had already covered, discarding
the fill of a read that started afterwards, and fired `SyncPolicy.admit`/`SyncHook.afterInvalidate`
on the sender for an event it had already applied. The filter now drops every self-originated
message after app isolation, and `ignoresSelf` is gone. Owner: `CacheSyncListener.onMessage`.

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
class pointers the accepted record is a 12-byte header + 8-byte version + 8-byte send instant +
4-byte flags = 32 bytes; the flags word already carries the wire type, the confirmation bit and the
long-retention bit, and the instant is a full `long` so a 12.4-day wrap cannot alias a real
decision. Packing the type into the version's sign bit would save the same four bytes the flags
word costs and buy a mask on every read while still needing a separate timestamp field. The accepted
record is an immutable snapshot, so an elision stores a replacement that advances the version and
keeps the flags and instant; the write refreshes the state-dependent expiry — twice the queue age
while cold, until a COOL is accepted, fails or expires while hot — so the newest version outlives
every queued decision that could read it. Owner: `WorkerBroadcaster`.

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
fallback expression still runs. The after-the-body `@ZetaPut` key follows the same rule: a
result-dependent key that resolves to null skips the write, because the body's side effect already
committed. Owner: `ZetaCacheInterceptor`, `CompiledExpression`.

**Falling back to SpEL for a digits-only TTL literal that overflows `long`.** The fallback assumed
SpEL could hold the number, but an unsuffixed decimal is parsed through `Integer.parseInt`, so the
result was an obscure startup parse failure instead of a clear range error. The literal is rejected
at resolution now, and a dynamic `BigInteger`/`BigDecimal` result is clamped to `Long.MAX_VALUE`
rather than keeping its low 64 bits. Owner: `CachePlan.Ttl`.

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

**Folding a HOT run exactly like a COOL run.** _Reversed._ The claim was that a later HOT whose
load fails needs the earlier HOT's created entry as its only fallback, so same-node HOT runs had to
stay sequential. That fallback is the resident entry, not the earlier decision: reducing the run
never evicts a value, so a later load failure still serves the entry the first HOT created. Keeping
every HOT instead ran the cluster loader and the business `onHot` three times for three versions,
and a same-batch HOT followed by a COOL prewarmed a key the COOL immediately revoked.
`WorkerListener.dispatchBatch` now keeps the last decision of a same-node run that survives the
version guard, folding the run into its two locals; mixed-node or anonymous runs stay sequential.
Owner: `WorkerListener.dispatchBatch`.

**Folding `closed` and the pending budget into one `AtomicLong`.** The budget deliberately uses a
striped `LongAdder`: every submission adds to it, and `sum()` with the two or three AMQP consumer
threads that submit is a base-field read, not a cell walk. A single word would put every submitter
on one cache line and make settlement vulnerable to a borrow that clears the closed bit when the
budget is discharged after `close()`. The separate volatile `closed` read is cheaper than that
risk, and it is also the guard that stops a granted batch's `finally` from subtracting after
`close()` has reset the counter. A settlement that read `closed` just before the reset can still
drive the counter negative, but only after close: no admission reads it then. Exact conservation
would need an `AtomicLong` for every submitter or an atomic grant claim on every cycle, trading the
hot-path design for a value nobody observes. Owner: `KeyDispatcher`.

**Encoding the periodic chain's terminal states in its `current` reference.** A terminal enum
would remove the `int state`, but `TERMINATED` (executor shutdown) is not a cancellation:
`isCancelled()` must stay false for a shutdown-terminated chain whose `get()` returns `null`, not a
`CancellationException`. Collapsing both into "terminal singleton" either violates the `Future`
contract in the other direction or needs a second distinction anyway. The 2-bit code plus the
compensating cancel in `scheduleLink` already publishes cancellation and the interrupt request in
one CAS. Owner: `PeriodicChain`.

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
the COOL, one idempotent message; a HOT dropped before the wire does not change the accepted record,
so the later COOL still travels. The cost is a wasted demote-then-promote pair in the microsecond gap
between a promotion and its submission. Owner: `DecisionDispatcher`, `WorkerBroadcaster`.

**Running a peer REFRESH load on the ordered dispatcher.** _Reversed._ The dispatcher pool was
sized for O(1) applies, so putting the blocking `loader.load` on it made a slow source or a
REFRESH storm occupy every scheduler thread; INVALIDATE, PUT and CLEAR sat behind the load and a
cross-node invalidation could miss its hard-TTL window. The fallback then moved refused messages
onto the AMQP consumer thread, exactly the path the dispatcher exists to keep free. The dedicated
refresh executor keeps one active load and one parked replacement per key; the cost is a second
pool and a per-key slot table, and the benefit is that backpressure on loads cannot delay a
removal. Owner: `DefaultSyncContext`.

**Reloading every cold key a peer names.** _Reversed._ A REFRESH broadcast reaches every instance,
so loading an absent key unconditionally multiplied the source read by the cluster size and wrote
entries no instance had served, evicting the local working set. A cold key is now a prewarm
decision: it reloads only when the namespace's prewarm trigger accepts the heat signal, otherwise
it records a versioned tombstone (zero I/O). The cost is that a key whose prewarm is off stays
absent on a peer that never read it, which is exactly the declared policy. Owner:
`DefaultSyncContext.canColdRefresh`.

**Applying the global TTL to a cold entry in a dynamic-TTL namespace.** _Reversed._ The registry
publishes the global default as a placeholder because a per-call TTL has no static baseline; writing
that placeholder turned a short dynamic TTL into the long global one and violated the caller's
declaration. The cold write is now refused (`TTL_NOT_PREWARMABLE`) and degrades to invalidation;
a resident keeps its stored durations. Owner: `DefaultSyncContext.writePeerValue`.

**Cancelling every renewal on COOL, or none.** Both shapes are wrong: cancelling all drops a
locally hot `keepWarm = LOCAL` task, while cancelling none leaves a cluster-only key renewing after
the COOL that revoked its permission and after a dead Worker's lease was pruned. The hook cancels
only when `keepWarm.matches(heatView, key)` is false; `OFF` is the empty mask and short-circuits
without a heat probe. Owner: `HotKeyContext.cancelKeepWarmIfCold`, `KeepWarm.cancelIfCold`.

**Copying an erased array argument before encoding a single key.** _Ruled out._ `SingleArgument`
adapted a generic `T[]` (erased to `Object[]`) into the declared component type on every hit.
`DefaultKey` tags every reference array as `aO` regardless of its runtime component type, and a
PLAIN namespace rejects array keys, so the copy changed no output byte. The SpEL path keeps the
adapter because `Method.invoke` still needs the declared array shape; the key fast path does not,
and the marker-only `Arrays` import and `expectedArrayType` are gone. Owner: `CachePlan.Key`.

**Judging a refresh receiver by its erased declaration.** _Ruled out._ A receiver that did not
implement the reported method's declaring interface was still kept as the refresh target when its
class exposed a same-name, same-arity erased declaration, on the theory that the erased call is the
body the plan expects. For a Spring Data repository it is not: the receiver is `SimpleJpaRepository`,
the erased declaration is the unused base implementation, and the body is the repository proxy's
query executor. Every soft refresh therefore invoked `findById(Object)` reflectively and silently
dropped the `@Query`, `@EntityGraph` and `@Lock` the foreground call honoured, while the self-warm
loader for the same method already went through the proxy. The interceptor now replaces any
receiver that does not implement the declaring view with the proxy that does, and the reflective
call arms the cache-bypass marker so the proxy's chain runs without re-entering the read advice. A
receiver with no proxy behind it — a custom `MethodInvocation` without `getProxy()` — still serves
the refresh itself. Owner: `ZetaCacheInterceptor.resolveProxyPlan`,
`ZetaCacheInterceptor.invokeBypassingCache`.

**Pinning a keyless clear to the global format.** _Ruled out._ Every annotated declaration
resolved `INHERIT` to the global default and recorded that answer, which kept two annotations
order-independent but made a table-wide clear claim a format it never used: an admin method clearing
a PLAIN namespace could not name it without a `@ZetaConfig` of its own, so startup failed with
`conflicting key format: PLAIN vs TYPED`, and a programmatic `PLAIN` registration made every
default-named read of that namespace fail. Treating the clear as "unknown" only moved the failure to
whichever declaration was scanned first. The registry now separates the format a declaration names
from the existence of the namespace, so a clear registers `UNPINNED` and a key-bearing declaration
pins. Owner: `NamespaceKeys`.

**Swallowing an after-the-body invalidation failure by phase alone.** _Ruled out._ The rule was that
the body already committed, so no removal failure could reach the caller. It also turned a
configuration error into a WARN: a `scope = REMOTE` invalidation in a local-only deployment, or an
unencodable key, removed nothing and reported success, leaving the entry stale for its whole TTL —
while the same mistake with `beforeInvocation = true` threw. The removal now distinguishes cause
exactly as `@ZetaPut` does: engine failures stay WARN, caller/configuration rejections propagate.
Owner: `CachePlan.Invalidate.Operation.applyAfterBody`, `ZetaCacheInterceptor.invalidate`.

**A flat-array delivery snapshot (`String[]` + `long[]`).** _Ruled out._ Building the tide snapshot
as two primitive arrays and widening `WaveCounter.BatchConsumer` from `Map<String, Long>` to that
pair would remove the per-tide `HashMap` — one `HashMap$Node` per key plus a boxed `Long` outside the
cache window. The consumer chain, however, ends at `KeyReporterImpl.publishBatch(Map, long)`:
admission reads `counts.isEmpty()`, `shardCounts` re-shards into a `Map<String, Map<String, Long>>`
per Worker, and the dispatcher's report message needs exactly those per-key pairs. A flat snapshot
would be expanded into those maps one call later, so the allocation would move from the deliverer's
`tideWatcher` to the reporter's shard pass — the same objects, a wider public reporting contract,
and no end-to-end reduction. The snapshot stays a `Map`; the memory reductions that do reach the
whole path (the adder-pool removal, the compact selection buffer, the merged scan arrays) are in
place. Owner: `WaveCounter.tideWatcher`, `AccessCounter.onTide`, `KeyReporter.publishBatch`.
