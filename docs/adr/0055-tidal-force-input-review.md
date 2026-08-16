# Tidal-Force Input Contract Review (no parameter additions; dead `snapshotSum` removed)

After the ADR-0054 exact-boundary iteration, the deliverer-side signals available to the `MoonsTidalForce` governor became more numerous and more precise. This ADR records the systematic review of the governor's input contract (`TideReading` + `onVolume`) and its internal branches: three candidate signal changes were sandbox-validated and rejected with evidence, one dead field was removed, and a construction-level mutual exclusivity is documented so future readers do not re-propose `overflow`.

## Status

accepted

## Context

The review question: with the promotion boundary now exact (ADR-0054), which input parameters of the tidal-force correction need to be added, removed or changed, and which internal branches need optimization, expansion or deletion, to improve correction precision? The full parameter/branch audit, the experiments and the reachability probe are recorded in the sandbox (`REVIEW-0055-tidal-force.md`, `scratch_tidal_review.py`, `scratch_probe_overflow.py`, `scratch_review_campaign3/4.log`, zeta-tidal-sim).

## Decision

**1. No parameter additions; the 8 consumed `TideReading` fields stay.** Each consumed field drives at least one reachable branch: `renewal` (walk verdicts, veto, R3 shift, distress/healthy split), `remain` (empty collapse, P1 quiet, saturation), `hotLimit` (saturation gate), `blockedKeys` (admit, raise ARM gate, flood signature), `boundary` (admit, confirm-admit, `floor > boundary` evidence), `hotColdRatio` (ARM gate, raise step gate, ADR-0053 stride price), `distinct` and `intervalMs` (flood rate scale, quiet/re-engage debounce). The volume itself stays on the `onVolume` EWMA channel; it is deliberately NOT part of the reading.

**2. Dead field removed: `TideReading.snapshotSum`.** The 9th record field was never read inside `onTide` (8/9 accessors consumed; the volume lives in the `vol` EWMA folded by `onVolume`). The record now carries exactly the signals the governor consumes — the "signal dropped here fails the compile" seam is honest again.

**3. Rejected with sandbox evidence (300-seed paired expanded corpora, baseline = shipped config):**

- **flood_raw** — the P2 flood signature prices its rate from the raw `snapshotSum / intervalMs` instead of `vol * 1000 / intervalMs` (the EWMA lags a regime jump 1-2 tides): floods −0.073 (18/300 improved, p < 0.0001) but SIX harm axes significantly worse on the SAME seeds — collapses +0.043 (p = 0.0002), excess +0.0087 (p = 0.0044), raised_frac +0.0015 (p = 0.0001), harmful over-filter +0.0008 (p = 0.0063), admit latency +0.030 (p = 0.0312), walk duration +0.116 (p < 0.0001). Firing inside the EWMA-lag window catches transient renewal-0 states the other signature gates cannot disambiguate — the same failure mode the ADR-0051 §4 flood-gate-2500 rejection documented.
- **flood_max** — `max(vol, raw)`: exactly zero delta on every axis at 300 seeds. Whenever the signature's other gates hold, `raw` never outruns `vol` in a fire-relevant state — a genuine flood state takes 1-2 tides to develop (renewal 0 + blocked band + floor above boundary), by which time the EWMA has folded the volume. The EWMA lag is load-bearing noise filtering, not a defect.
- **release_gate / `overflow`** — the audit/saturation release walk gated off while the exact cutoff overflowed (tie band wider than capacity): exactly zero delta at 150 and 300 seeds, and a reachability probe over 15,693 governor tides found the target state (raised floor + overflow + healthy saturated set) **0 times** — a raised floor meeting an overflow regime is always distressed (the old members squat below the new kth). Additionally, `blockedKeys > 0` and `overflow` are **mutually exclusive by construction** in `selectBoundary`: `kth >= floor` ⟺ `overflow` possible, `blockedKeys = 0`; `kth < floor` ⟺ `blockedKeys` possible, `overflow = false`. The ARM/admit/flood gates can never need overflow disambiguation, which is why the parameter has no consumer.

**4. Branches.** No dead branches exist in the governor (the veto and RLS-CRASH paths are unreachable in the e2e corpus but covered by the gov_fuzz synthetic-signal scenarios, ADR-0045 Part II). The P1 quiet-regime fall-through (floor == `QUIET_FLOOR`, renewal 0) is NOT short-circuited: its `foldRenewal` is the load-bearing R3 re-seed after a quiet regime (a zero-renewal fold makes the returning burst read as a ≥ 0.05 shift and re-learns the references). The flood rate/signature double evaluation on the distress path (once in `floodCollapse`, once in the ARM gate) is kept as the self-contained design — hoisting it into `onTide` was considered and declined during review (2026-08-16).

## Consequences

- `WaveCounter.TideReading` drops the 9th field; `computeReading` no longer packs `snapshotSum` into the reading (the local stays, as the density-ratio numerator).
- Java tests updated: `WaveCounterAdaptiveTest`'s reflective helpers construct the 8-field record; the volume seed for the P1/P2 gates is folded via `onVolume` exactly as the production call order requires.
- Sandbox mirror updated: the sim's `on_tide` drops `snapshot_sum` (pipeline call, `gov_fuzz.gen_reading`, the synthetic scenarios in `fuzz_sandbox.py`/`fuzz_m4.py` and the two scratch scripts); `on_volume` and the promote-local `snapshot_sum` stay.
- Behavior-neutral: 91 `WaveCounter*Test` tests green; all 10 hand scenarios byte-identical; gov_fuzz 4 configs × 40,000 decisions hold every invariant.
- The 2026-08-16 repo rewind attempt (working tree replacing `WaveCounter.java` with the ADR-0045/0046-era governor and deleting ADR-0049/0051/0052/0053/0054) was discarded on request and backed up to `zeta-tidal-sim/backup_rewind_staged.diff` before the hard reset to `hotkey/master` (86cc347).
