# Versioned Rule Merge for Cross-Instance Consistency

Zeta's `RuleMatcher.persistAndBroadcastRules()` used an XOR pattern (`ifPresentOrElse`) — either persisted the full rule set to Redis OR broadcast via AMQP, never both. With multiple App instances independently modifying rules, a last-writer-wins race on the shared Redis key `hotkey:rules` caused concurrent rule additions from different instances to silently overwrite each other.

## Decision

Introduce a third version space (`rulesVersion`, `AtomicLong`), make `persistAndBroadcastRules()` always both-write (Redis + AMQP), and change `syncRules()` from full-replacement to merge-by-pattern. A Lua script guards the Redis write (`version > currentVersion`), and merge-on-receive preserves local-only rules while accepting incoming pattern conflicts.

## Considered Options

- **XOR pattern (status quo):** Simple but broken under concurrent multi-instance rule changes. Rejected.
- **Lua-based atomic merge:** Read current rules from Redis, merge in script, write merged result. Rejected because JSON merge in Lua is complex and fragile.
- **Diff-based operations (add/delete per rule):** Semantically clean but requires tombstone tracking for deletes and significantly more AMQP traffic. Overkill for admin operations.
- **Distributed lock around rule changes:** Introduces a new failure mode (lock holder crashes) and latency. Inconsistent with the project's fire-and-forget philosophy.

## Consequences

- Deletions are **eventually consistent** (per ADR-0013): a concurrently added rule may temporarily reappear after a peer deletes it, until the next merge cycle.
- Both-write guarantees that a freshly started instance with no broadcast history still gets the latest rules from Redis on first access.
- The Lua CAS guards against stale-write races but does not prevent concurrent additions from different instances — merge-on-receive handles that.
- `rulesVersion` space is per-rule-set (global), not per-rule. A single `AtomicLong` suffices because rule changes are administrative operations (low frequency, small payload).
- Backward compatible: if a peer broadcasts the old array-only format (`String[]` of patterns with implicit BLOCK action), the listener detects it via `instanceof` check and wraps into the versioned envelope.
