# 0065 — Snapshot-Stamped Rule Decision Memo

## Status

Accepted (2026-09-12)

## Context

`RuleMatcherImpl.evaluateRule` ran on every cache `get` / `getWithSoftExpire` (via `HotKeyCache.preGuard`) and again on the load path (`afterGuard`), each time walking the full `Rule[]` snapshot in insertion order. The per-rule cost is trivial for EXACT/PREFIX (`equals`/`startsWith`) but a real regex-engine execution for WILDCARD/REGEX, so a regex-heavy rule set could spend microseconds per get re-evaluating rules that almost never match — measurable against a read path whose other steps are tens of nanoseconds.

## Decision

Evaluation results are memoized per key in a small Caffeine cache (`decisionCache`, maximumSize 10k, expireAfterWrite 30s). Each entry carries the **exact snapshot array reference** the action was computed from, not a version number: a reader hits only while `entry.snapshot == rulesSnapshot`. Every mutation builds and swaps a fresh array (append, copy, `replaceRules`, `EMPTY_RULES`), so a new/removed/replaced/cleared/synced rule set invalidates all previous entries by reference inequality on their next read — no invalidation call, no invalidation storm after a change, and a load raced across a mutation is rejected on first read instead of lingering. An empty rule set short-circuits to `ALLOW` without touching the memo (zero-config deployments keep the old ~1-2ns cost), and a `null` key skips the memo entirely, preserving the historical scan semantics (NPE from a matching EXACT rule, `ALLOW` when no rules exist).

The snapshot-reference stamp was chosen over the `rulesVersion` stamp because `replaceRules` swaps the snapshot **without** bumping the version (it is `syncRules`' internal helper and a test surface), so a version stamp would have served stale decisions for a full TTL epoch after a direct `replaceRules` call. The one shared reference, `EMPTY_RULES`, is safe for reuse: zero rules can only ever produce `ALLOW`, so a later empty set cannot resurrect a wrong action. Mutation-ordering discipline (snapshot swap before version bump) becomes irrelevant — correctness no longer depends on it.

## Considered Options

- **`rulesVersion`-stamped entries** — rejected: `replaceRules` does not bump the version; also required a version-before-snapshot read-ordering argument for stamp safety.
- **Invalidate-on-mutation (the Worker fast-lane `matchCache` pattern)** — rejected for this site: every rule change would trigger a thundering re-scan of every hot key, and an invalidation racing a concurrent loader can re-insert a stale value that then lives to the TTL. (`FastLaneRuleManagerImpl` keeps `invalidateAll` — its rules live in a mutable CHM+COWAL with no snapshot to stamp.)
- **Structural index (EXACT → `HashSet`, PREFIX → first-char buckets, WILDCARD/REGEX residual scan)** — rejected: the REGEX residual must still be scanned unconditionally (order decides), so it does not help the exact case that hurts most; more code for less benefit.

## Consequences

- Rule decisions on the read path cost one lock-free map probe regardless of rule count or pattern types; `preGuard` + `afterGuard` double evaluation degenerates to two probes.
- A rule change is honored on the **very next evaluation** — BLOCK immediacy is unchanged (pinned by tests: add / remove / removeRulesByAction / removeRule(int) / clearRules / replaceRules / syncRules all assert next-evaluation visibility, plus a writer-vs-reader convergence test).
- In-place `Rule.setPattern`/`setType` mutation (discouraged; bypasses `RuleMatcher`, never propagates cluster-wide anyway) swaps no array and bumps no version — its effect may be delayed by up to the 30s TTL. This is the only staleness window and it is documented on the memo and on `Rule`.
- Memo memory is bounded: 10k keys × (key + snapshot reference + action); all entries of an epoch share one snapshot reference.
