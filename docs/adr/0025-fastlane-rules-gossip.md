# Fast-Lane Rules Gossip with Versioned Full-Set Merge

Fast-lane rules (ADR-0016 fast-lane bypass) were Worker-local state: the `FastLaneEndpoint` CRUD wrote only to the receiving Worker's `FastLaneRuleManagerImpl`. Under consistent-hash sharding each key is owned by exactly one Worker, so a rule POSTed to worker-1 never affected keys routed to worker-2 — the same fast-lane rule set produced divergent behavior across shards, and a restarted Worker came back with only its static YAML rules.

## Decision

Propagate fast-lane rules Worker-to-Worker using the same decentralized-gossip philosophy as ADR-0003 (state-machine config via heartbeat, no config store) and the versioned-merge semantics of ADR-0012.

1. **New message type `FastLaneRulesMessage`** (`TYPE = "FASTLANE_RULES"`): headers carry `nodeId`, snowflake `messageId`, and `fastlaneRulesVersion` (long, the mutating Worker's `System.currentTimeMillis()`); the body carries the full rule set as JSON.
2. **Channel:** published on the heartbeat exchange with routing key `fastlane.rules`, over the heartbeat-dedicated (control-plane) connection. Deliberately *not* under `heartbeat.*`: the App-side heartbeat queues bind `heartbeat.*`, and rules are Worker-internal — a separate key keeps them off every App queue. Each Worker's config queue adds a second binding (`fastlane.rules`) to the same heartbeat exchange.
3. **Versioned full-set merge (LWW):** `FastLaneRuleManager` gains `getRulesVersion()` and `replaceAll(rules, version)`. A receiver applies a message only when its version is newer; on a same-millisecond tie the message from the lexicographically larger `nodeId` wins, making simultaneous two-Worker edits converge deterministically. Local CRUD sets the version to the current wall-clock; YAML-loaded initial rules start at version 0 so any gossiped set overrides them, while an untouched cluster (all v0) is self-consistent.
4. **Dual-trigger broadcast:** immediate full-set broadcast after every endpoint mutation, plus a periodic full-set rebroadcast every 60 s (`zeta.worker.fast-lane.gossip-interval-ms`). A fresh or partitioned Worker converges within one interval. Rule sets are small (tens of entries → a few KB), so full-set transmission is cheaper and simpler than delta sync.
5. **Endpoint hardening:** blank `keyPattern` or `threshold <= 0` now returns 400 instead of an NPE-driven 500.

**Operational discipline (documented, same assumption as ADR-0012):** rule changes should be performed against a single Worker at a time. Concurrent independent edits to different Workers resolve by wall-clock LWW; clock skew across Workers can let a genuinely newer edit lose — acceptable because rule changes are rare, operator-driven events, and the periodic rebroadcast bounds divergence to one interval after the last edit.

## Alternatives Considered

- **Redis-backed rule store with polling:** rejected — introduces an external config store and a hard dependency on Redis in the detection path, contradicting ADR-0003's decentralized-gossip philosophy. Redis is already optional for epoch init (file fallback).
- **Extend `WorkerHeartbeatMessage` with the rule set:** rejected — the heartbeat is a fixed-header, zero-deserialization message; embedding a variable-length JSON rule set breaks that design and would also fan rules out to every App (heartbeats bind `heartbeat.*` on the App side too).
- **Delta operations (add/remove per rule):** rejected — reordering and missed messages make deltas fragile; full-set LWW is idempotent and self-healing, and rule sets are small.
- **Version = per-Worker `AtomicLong`:** rejected — not comparable across Workers (same problem as decisionVersion pre-epoch, see ADR-0010 rule 6). Wall-clock timestamps are cluster-comparable; the nodeId tie-break handles same-millisecond collisions.

## Consequences

1. `FastLaneRuleManager` gains two methods (interface `default` implementations preserve source compatibility for any external implementations).
2. `WorkerConfigNegotiator` now demultiplexes two message types on the config queue (heartbeat config vs fast-lane rules); its previous 3-arg constructor delegates for compatibility.
3. One extra binding on the Worker config queue; zero topology change on the App side.
4. Rules converge cluster-wide within `gossip-interval-ms` (60 s default) in the worst case (missed immediate broadcast), and within milliseconds in the normal case.
5. A Worker that was down and returns with stale YAML rules (v0) adopts the cluster's gossiped set on the first received message — self-healing on rejoin.
