# Wire-Protocol Versioning

Five AMQP message shapes and two binary codecs previously evolved without a
shared rule: `Rule` JSON already ignored unknown fields, but fast-lane gossip
and the report JSON path dropped whole messages on any added field. We adopt
additive-only evolution — JSON ignores unknown fields (fail-open),
header-framed messages grow by header addition with typed defaults, and binary
codecs keep reject-on-newer (fail-closed, self-healed by the next flush or
reload).

## Status

implemented (2026-10-06). `CompactAwareReportMessageConverter.forwardCompatible`
owns the report JSON delegate used by both bean sites; `FastLaneRulesMessage`
mapper ignores unknown properties; behaviors pinned by `WireCompatTest`
(common) and `FastLaneRulesMessageTest.from_shouldIgnoreUnknownRuleFields`
(worker).

## Considered Options

- **Per-message schema versions with rejection.** Rejected: hard failures
  contradict the fire-and-forget, self-healing design (ADR-0007/0013) — a
  tolerable field difference would become a dropped decision.
- **Version negotiation/advertisement.** Rejected: no request channel exists
  (the Worker only receives pushed reports); negotiation has no transport.

## Consequences

1. Upgrade order: report/fast-lane/gossip JSON field additions — any order;
   compact binary bump — Workers first, Apps after full rollout (ADR-0074);
   header additions — any order.
2. Iron rules: headers additive-only with typed defaults; JSON additive-only,
   every zeta-owned mapper sets `FAIL_ON_UNKNOWN_PROPERTIES=false`; binary
   bumps are Workers-first events.
3. Newer-version compact bodies are discarded (logged by the container), not
   misparsed — reports self-heal on the next 50ms flush. No new gauge
   (explicitly deferred).
