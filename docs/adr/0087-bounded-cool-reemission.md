# Bounded COOL Re-Emission (Same-Version Repeats)

ADR-0024 made HOT decisions self-healing (periodic rebroadcast, default every
10s) and deliberately left COOL as single-send: cooling is edge-triggered, so a
steady-state COOL rebroadcast would be constant traffic, and a lost COOL fails
in the lenient direction (the entry keeps serving, just with a longer TTL).

That left one loss path with no healing at all. A COOL lost in transit
(ADR-0007 fire-and-forget, `WorkerBroadcastBuffer` saturation drop, dispatcher
saturation drop on the App side — all documented, all real) pins the entry HOT
until its hard TTL (default 1h). The staleness bound is therefore not the
heartbeat cycle, as ADR-0066's consequences section implies, but the hot hard
TTL. Cross-Worker last-writer-wins (ADR-0085) and the App-side dispatcher
dropping COOL without retry compound it: a stale HOT replay arriving after the
lost COOL is accepted unconditionally, and nothing ever corrects it.

We decided on bounded same-version re-emission: each successful COOL broadcast
schedules `coolRebroadcastTimes` repeats (default 2, range [0, 5]), spaced one
`rebroadcast-interval-ms` apart, carrying the **original** decision version —
never a fresh allocation. Same-version is what makes repeats safe: where the
first send landed, the repeat is an equal-version skip; where a newer HOT
landed (cool → reheat inside the repeat window), the repeat loses the version
comparison instead of demoting a legitimately hot entry; where the first send
was lost, the repeat applies and heals. A fresh version would invert the second
case and invent a demotion. Repeats bypass the 100ms send dedup entirely
(neither checked nor populated); the ≥1s spacing already exceeds the debounce
window, and populating it could elide a genuine new decision.

Cooling stays edge-triggered — repeats fire per cool episode, not per interval —
so the traffic cost is two extra sends per episode, which cannot storm. `0`
restores the exact pre-ADR-0087 single-send behaviour. A rejected repeat
(saturated/shut-down scheduler) is a DEBUG log, converging to the old bound.

Rejected: periodic COOL rebroadcast symmetric to HOT (unbounded traffic for an
event that fires once per episode — the reason ADR-0024 excluded COOL in the
first place); back-to-back double-send (shares fate with the first send under
the same outage, and collides with the 100ms dedup); version-bumped repeats
(invents demotions, above). Rejected also: fixing this App-side by shortening
the hot hard TTL — that punishes every HOT entry for a transport loss.

Consequence: `zeta.worker.decisions.cool` now counts emissions including
repeats (same rule as HOT rebroadcasts, which are counted per send at the
funnel). The worst-case single-loss window drops from the hot hard TTL to
`coolRebroadcastTimes × rebroadcast-interval-ms` (default ~20s). The
`WorkerBroadcaster` gained a constructor overload; the legacy constructor
disables repeats, and a `null` scheduler does the same.
