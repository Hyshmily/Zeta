# WaveCounter Tide Controls (Consolidated ADR-0038..0041)

The WaveCounter's tide-path decisions were spread across four documents (0038 adaptive controls, 0039 flag-gated quiescence, 0040 promotion-floor admit-on-block, 0041 beacon best-effort visibility). They are consolidated here into a single record; each section is a self-contained decision that was originally accepted on its own.

## 0038 — Adaptive Controls: Smoothed Tide Cadence, Empty-Tide Ladder, Probe-Governed Promotion Floor

WaveCounter's adaptive knobs — the backlog-driven tide cadence and the absolute promotion floor — were static or raw-signal laws that ping-ponged and ratcheted. We adopted three WindowClimber designs: (1) the cadence ramp now runs on a fast-attack / slow-release EWMA reference with a still band (burst latency unchanged, the 50↔500ms ping-pong gone); (2) consecutive empty tides stretch the cadence up to 2× the base, reset by any non-empty tide; (3) the promotion floor (10) is now a probe-governed controller: it raises only while keys are measurably blocked behind it, retreats to its probe base when a raise fails to recover the hot set's renewal rate, and audits back down under sustained health or saturation. All three were validated on simulated sequences before wiring in.

### Status

accepted

### Context

`tide()` (WaveCounter.java) re-schedules itself at a delay computed from the RAW delivered key count: ≥ 20,000 keys → 50ms, scaling linearly back to 500ms. A burst tide followed by a quiet tide therefore snaps 50 → 500 → 50 (ping-pong), and the single-tide raw signal moves the ramp on every jitter. The promotion pass admitted keys at `max(histogram boundary, PROMOTION_FLOOR = 10)`; the floor was a fixed constant, so noise keys (1-9 counts/cycle) were excluded forever and marginal keys were admitted forever, with no feedback about whether the hot set's slots were actually earning their keep.

The reporter contract (KeyReporter → 100k capacity reservoir) bounds burst detection latency: a tide must not stretch so far that a burst overflows the reservoir before delivery. The hot set is routing-only (a false/weak promotion costs performance, never count correctness — ADR-0020 class doc), so adaptive moves on the floor are safe to probe.

The fixed floor's failure modes, established by probe: unconditional raising (the naive "renewal low → raise") starves low-traffic workloads (the floor chases to 256 while no key can qualify — nothing promotes, renewal stays 0, infinite churn); a raise-only controller ratchets (a recovered workload keeps an elevated floor forever).

### Decision

**TidePacer (cadence).** The raw backlog is folded into a smoothed reference: a backlog ABOVE the reference by more than the still band (5% of 20,000 = 1,000 keys) folds in FULLY at once (fast attack — a burst shortens the cycle to 50ms on the very next tide, burst latency unchanged); a backlog BELOW folds at the EMA rate (0.2/tide, ~5-tide memory — the fast cadence drains the reservoir instead of snapping back); moves within the band move nothing (deadband — jitter cannot toggle the ramp). Consecutive EMPTY tides stretch the next cadence by a power of two, capped at 2× the base (the empty-tide ladder — idle cycles stop paying the 1ms quiescence, decay sweep and scheduler wakeup at the base rate; a 2× cap keeps any burst within two base intervals of detection); any non-empty tide resets the streak.

**PromotionGovernor (floor).** Each promoted tide measures `renewal` = active hot slots whose key earned at least the threshold, divided by active slots (probed: healthy single-key set 1.0, one-drifter pair 0.5, fully drifted 0.0). Below the target (0.5) the set is distressed; the raise direction is a bounded RAISE-WALK armed only on distress + the within-sample density-ratio signal (hot slots earning less per slot than cold keys earn per key — the frozen-set signal), confirming durably (the set must hold the target across `TIDAL_CRASH_PERSISTENCE` tides) and undone on persistence (ADR-0045; the unconditional raise was procyclical — it starved low-traffic workloads and ratcheted). The blocked-keys band is renewal-disambiguated: under health it is genuinely blocked hot keys and the floor drops toward the boundary (admit-on-block); under distress it is the self-healing stale tail of the 2-tide membership memory and is never admitted. If distress survives 4 tides above the veto anchor (the position the last confirmed walk settled on), the floor retreats to the anchor in 8 budgeted strides (veto-return, anchor-priced). Under health, the floor is released: saturation (active slots ≥ 90% of hotLimit) or a still-health audit (8 tides without movement) arms a bounded release WALK — frozen base, anchor-memory goal verdict, budgeted undo on crash, retry backoff — and movement now decays the audit run instead of zeroing it (ADR-0045). Clamped to [10, 256]. The histogram boundary stays scale-free; the governor only moves the noise filter.

**Validation-before-wiring.** The design was first prototyped as simulated sequences in a temporary probe test (fast-attack/slow-release trajectories, renewal in real tides via reflection, governor law with synthetic sequences); the failing cases (starve-churn, ratchet) shaped the final law. The probe was then deleted and the permanent assertions moved to `WaveCounterAdaptiveTest`.

### Considered Options

- **Raw-law cadence (status quo):** single-tide snap 50↔500ms; jitter moves the ramp. Rejected — the EWMA pair costs ~3 ops/tide and removes the oscillation.
- **Symmetric EWMA (no attack/release asymmetry):** a burst's fast cadence arrives only after ~5 tides (smoothed 0 → 4k → …) — burst detection latency regresses. Rejected; the asymmetry preserves the latency the reporter contract needs.
- **Skip empty tides entirely (longer stretch, e.g. 4-8×):** more idle power saved, but burst detection drifts to multiple base intervals and the beacon decay (2-tide memory) stretches in wall-clock. Rejected — 2× is the documented trade-off point.
- **Unconditional distress-raise:** probe showed the floor chases to 256 and starves low-traffic workloads (nothing ever qualifies). Rejected — the blockedKeys gate is load-bearing.
- **Raise-only governor (no audit release):** a recovered workload keeps an elevated floor forever (ratchet). Rejected — the audit releases it.
- **Anchor with smoothed claim + veto margins (full WindowClimber goal metric):** more machinery (Rates, deviation-priced margins, anchor planting) for a signal (renewal) that is coarse (0/0.5/1.0 in small hot sets); the probe showed the veto churns when no anchor can plant. Rejected — the probe-and-retreat form delivers the same protection more simply.

### Consequences

1. **Burst detection latency unchanged** (fast attack); the cadence recovers over ~5 tides instead of one (slower release of the fast cadence after a burst — deliberate, drains the reservoir).
2. **Idle cost drops**: after a stretch, empty cycles run at 2× the base interval; the quiescence window, decay sweep and scheduler wakeup are paid half as often. Worst-case detection of a burst arriving in the stretch is 2 × base interval (1,000ms at defaults). Since 2026-08-12 sub-minimum tides (snapshots below `MIN_PROMOTION_KEYS` distinct keys) also skip the decay sweep entirely — the halving decay without the re-seeding promotion scan would strip the whole hot set within two tides (evidence 2→1→0) on small workloads, silently routing every key down the cold path (adversarial audit H2).
3. **Promotion is slightly adaptive**: under drift with blocked noise keys the floor probes up (max 256), re-promotion of a returning hot key is unaffected while its count exceeds the floor, and the audit releases the ratchet within ~9 healthy tides. All moves are routing-only.
4. **Renewal is counted in the promotion scan** (member keys at/above threshold, post-promotion to avoid the fresh-beacon startup artifact); at hotLimit the scan breaks early and the signals are partial — the governor is suppressed in that state anyway.
5. **New tests**: `WaveCounterAdaptiveTest` (10 tests: pacer attack/release/band/ladder, governor raise-gate/retreat/audit/saturation/clamps, end-to-end drift-probe-raises-then-audits-back). Full hotkeydetector suite: 142 tests green.

## 0039 — Flag-Gated Quiescence and Cold-Insert Simplification

The tide's 1ms cold-write quiescence window was paid on every cycle because an empty old table cannot be told apart from "no writer in flight". Since 2026-08-09 the window is gated by a `coldWriteSeen` flag — set by cold-path first-inserts before their writes (read-gated: the flag is monotonic within a cycle, so only the first store of a cycle pays; the read-then-store race is benign and the store-before-tide-gate-acquire ordering is unchanged), captured and cleared by the deliverer at the table swap — so cycles with no cold traffic skip the window entirely (pure-hot and idle cycles save the 1ms park and the yielded core), while cycles with cold traffic pay the same parked wait as before. The residual loss window when skipped shrinks to the ns-scale gap between a writer's reference read and its flag store; a miss-path writer preempted across the swap re-targets the NEW table (its `computeIfAbsent` re-reads the `reservoir` field), so the flag cannot lose a write by itself. The 2026-08-07 "skip on empty" attempt was reverted the same day because it traded a documented loss bound for ~1ms of busy-spin without any proof; the flag gate is finer — it skips only when PROVEN that no cold writer is in flight.

### Status

accepted

### Context

The cold approximate window's bound was "a preemption > 1ms loses the write" (measured typical loss 0, worst ≈2.1e-5/op, 2026-08-06). The old code paid the 1ms park-then-spin window on EVERY tide — including pure-hot cycles where no cold writer existed — because an empty old table is indistinguishable from a preempted writer (the information gap). The cost: 1ms added delivery latency per cycle and a parked core, bounded but constant.

The gate's soundness argument: every cold write into the current table was preceded this cycle by a first-insert (the table is swapped wholesale per tide, so a hit implies an insert this cycle — which set the flag). A dropped key (capacity guard) never writes, so it must not set the flag — the guard returns before the store. A writer whose flag lands after the swap targets the new table via its `computeIfAbsent` re-read. The only un-rescued case is a writer preempted inside the ns-scale gap between its `get()` and the flag store, whose write lands in `old` after the snapshot — inside the documented approximate semantics.

Alternative considered and rejected: re-introducing a per-op in-flight counter for cold writers (closes the gap exactly) — measured ~15ns/op on the hot path when tried previously (2026-08-08), which is precisely the per-op cost the routing design exists to avoid.

The same change drops the redundant `containsKey` re-check from the capacity guard (the get() above already proves absence, except for the µs boundary race where another thread inserts between the two reads — inside the documented "approximate (racy size check)" semantics). This keeps the dominant capacity-drop path at one get + one atomic read.

### Consequences

- Cycles with no cold traffic (pure-hot or idle) skip the 1ms window; the residual loss model is documented in the class Javadoc as the ns-scale get-to-flag gap.
- Hot-path exactness is unchanged: the `mergesInFlight` settle is still paid unconditionally — verified by the hot-racing stress test (exact 0 loss with tides racing at 5ms while 8 writers hammer 32 promoted keys) and the probe's 100.0000% delivery sanity.
- The gate exposed a latent TOCTOU on the hot path: `discharge` captured the table reference under the gate but bumped `mergesInFlight` afterwards, so a discharge preempted between capture and bump could land its merge in the swapped-out table after the snapshot — previously masked by the always-paid 1ms window (the hot-racing stress lost 32/160k ops with the window skipped). The reservation now happens atomically with the capture inside `reservoirGate` (monitor release-acquire makes it visible to the tide's settle-wait), making the documented "no in-flight hot add can be stranded" claim true unconditionally.
- `clear()` resets the flag (a full reset; an in-flight writer racing clear() is ns-scale and its counts are lost by clear()'s definition anyway). The same pass zeroes `approximateSize`, which clear() previously left stale — a stale size made the capacity guard drop the first post-clear insert (surfaced by the quiescence-gate test). Since 2026-08-10 the pass also zeroes the routing beacon, so no stale hot route survives a full reset (the halving decay's 2-tide memory otherwise), and hot-path first-inserts bump `approximateSize` exactly like the cold path (`waveTo`'s `putIfAbsent` winner) — closing a systematic under-count of up to `hotLimit` resident hot keys per cycle that weakened the cold capacity bound.
- Measured parity on all five probe workloads (2026-08-09, copy-vs-original head-to-head): 1.0x ± run-to-run noise; churn workload regained parity only after the null-abort `computeIfAbsent` variant (capacity check inside the mapping function) was rejected — that variant paid the bin lock per dropped key and measured 5x slower on churn.
- New tests: `WaveCounterQuiescenceGateTest` (flag set/clear matrix, hot-racing exactness, cold racing with idle gaps within the 0.01% bound).

### Amendment 2026-08-11 — Every Insert Path Marks the Flag

The gate's soundness argument assumed "every cold write into the current table was preceded this cycle by a first-insert — which set the flag". The assumption was false for hot-path inserts: `waveTo` (writer discharges and tide phase-1 drains), `reconcile` and `recoverZero` all inserted entries via `mergeKey` WITHOUT marking the flag. A cold hit-writer that added to such an entry therefore had no 1ms bound on its loss — the tide skipped the window on a cycle whose only table writes were hot drains, and the hit-writer's add raced the snapshot with only preemption luck as its bound (violating the documented "preemption > 1ms" model).

The flag is now marked by EVERY insert path: `mergeKey` marks before its `putIfAbsent` makes the entry visible (covering the tide's phase-1 drains and `drainDead`), and the writer-side gate paths (`discharge`, `reconcile`, `recoverZero`) additionally mark inside their `reservoirGate` capture — a swap that races an in-flight merge must capture the mark, because the entries may land in `old` after the swap and a mergeKey-only mark would land after the capture. The skip now applies only to cycles with NO shared-table writes, whose old table is empty anyway (nothing to lose); a cycle with table writes pays the same 1ms parked wait. Cost: one read-gated volatile store per merge batch — negligible. The measured worst loss (~2.1e-5/op) is expected to shrink; the 0.01% stress bounds are unchanged.

## 0040 — Promotion Floor: Admit-on-Block Replaces Raise-on-Blocked

The probe-governed promotion floor from the 0038 section above raised while keys were blocked behind it — the procyclical direction. Blocked keys are exactly the renewing keys the histogram boundary would admit; excluding them starves the hot set (probed: rotating-hot-set workloads ran the floor past the whole distribution and emptied the hot set, churn workloads oscillated it forever). The governor now DROPS the floor to the boundary when distress has keys blocked behind it (admit-on-block), and collapses an empty hot set to the seed. Supersedes the PromotionGovernor half of the 0038 section; the TidePacer half is unchanged.

### Status

accepted

### Considered Options

- **bc-style sharded cold path (16 hash slices):** measured equal-or-worse across five cold-path workloads (0.48-1.14x of the single table) — hash routing spreads every thread across every slice, so the aggregate working set and its cache footprint are unchanged; dead table entries themselves were measured harmless to sparse hits. Rejected on data.
- **Keep raise-on-blocked (status quo):** probed to ratchet (rotation) and oscillate (churn). Rejected.
- **Unconditional admit (drop the floor on any distress):** low-traffic workloads would churn the floor pointlessly; the blockedKeys gate keeps the move evidence-driven. Rejected.

### Consequences

1. The floor never filters above the histogram boundary: the boundary plus the seed (10) do all noise filtering; the governor only recovers over-filtered states.
2. Veto-return, audit clock and saturation release remain as defensive machinery (unit-tested via synthetic boundary-raised states) but are unreachable from the real call path.
3. Hot-path utilization is preserved under rotating and churn workloads (probed: floor stays at the seed, hot set stays populated, phase leaders keep promotion).

## 0041 — Routing Beacon: Best-Effort-Visibility Plain Reads Replace Volatile Reads

The counting beacon (`long[]`, 4 bits per room, k=2) was read via `AtomicLongArray.get` — a volatile load — on EVERY `count()` op, including the highest-frequency hot-key ops. The beacon is routing-only (a stale read merely sends a hot key down the always-correct cold path), so writers now read it via plain (non-volatile) array loads: visibility of deliverer-side promotion updates is best-effort (hardware cache coherence bounds it in practice; immediate on x86 TSO), and correctness never depends on when an update is seen. The deliverer remains the array's only writer (plain read-modify-write, no CAS), and even a hypothetical torn 64-bit load is harmless — garbage evidence reads as a false negative and routes cold.

### Status

accepted

### Considered Options

- **Keep `AtomicLongArray` (volatile reads):** each hot-path count pays two volatile loads; the JMM visibility guarantee is real but the beacon has no correctness dependency on it. Rejected — the guarantee is free to drop.
- **Plain reads + a per-tide epoch fence (e.g. bump `coldWriteSeen` after beacon writes):** restores a JMM-guaranteed publish point at the cost of an extra store on the deliverer side and no per-op savings — the writers' plain loads still lack an ordering guarantee to that fence. Rejected — the epoch buys nothing that coherence does not already provide in practice.

### Consequences

1. A writer may keep routing a promoted key down the cold path for a while after its promotion (bounded by coherence in practice, unbounded in the JMM). Cold-path counting is always correct, and the key is re-promoted every tide it stays hot.
2. Hot-path per-op cost drops from two volatile loads to two plain loads on the highest-frequency ops (the promoted hot keys).
3. HotSpot guarantees aligned 64-bit loads/stores are non-tearing in practice; a torn read on any other VM is still routing-only.
