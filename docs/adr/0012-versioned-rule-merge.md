# Versioned Rule Merge for Cross-Instance Consistency

`RuleMatcher.persistAndBroadcastRules()` used an XOR pattern (`ifPresentOrElse`) — either persisted the full rule set to Redis OR broadcast via AMQP, never both. With multiple App instances independently modifying rules, a last-writer-wins race on the shared Redis key `hotkey:rules` caused concurrent rule additions from different instances to silently overwrite each other. We introduced a third version space (`rulesVersion`, `AtomicLong`), made `persistAndBroadcastRules()` always both-write (Redis + AMQP), and changed `syncRules()` from full-replacement to merge-by-pattern. A Lua script guards the Redis write (`version > currentVersion`), and merge-on-receive preserves local-only rules while accepting incoming pattern conflicts. Deletions are eventually consistent (ADR-0006) — a concurrently added rule may temporarily re-appear after a peer deletes it.

## Considered Options

- **XOR pattern (status quo):** Simple but broken under concurrent multi-instance rule changes. Rejected.
- **Lua-based atomic merge:** Read current rules from Redis, merge in script, write merged result. Rejected because JSON merge in Lua is complex and fragile.
- **Diff-based operations (add/delete per rule):** Semantically clean but requires tombstone tracking for deletes and significantly more AMQP traffic. Overkill for admin operations.
- **Distributed lock around rule changes:** Introduces a new failure mode (lock holder crashes) and latency. Inconsistent with the project's fire-and-forget philosophy.
