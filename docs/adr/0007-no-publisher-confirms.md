# No Publisher Confirms on AMQP Publish Paths

Three core publish paths intentionally omit RabbitMQ publisher confirms:

| Path | What's lost | Self-healing mechanism |
|------|-------------|------------------------|
| `ReportPublisher` | A batch of access counts | Next flush cycle resends within `reportIntervalMs` (default 1s) |
| `WorkerBroadcaster` | A HOT/COOL decision | Next evaluation window re-broadcasts; clients converge via `decisionVersion` ordering |
| `CacheSyncPublisher` | An invalidate/refresh message | All cache entries have Caffeine `expireAfterWrite` as final consistency backstop |

None of the three paths are command-oriented (fire-and-forget would be dangerous). All are state-oriented: loss is ephemeral, and the next periodic publish cycle restores consistency within bounded time.

| Dimension | Evaluation |
|-----------|------------|
| **Hard to reverse** | Adding confirms later is easy (change `RabbitTemplate` settings + add `ConfirmCallback`). |
| **Surprising without context** | Yes — operators familiar with AMQP will notice the lack of confirms and may consider it a bug. |
| **Result of a real trade-off** | Confirms add latency (each publish waits for broker acknowledgment), increase channel pressure (unconfirmed messages accumulate), and complicate error handling (re-publish logic). For state-oriented broadcasts where loss is self-healing, the throughput benefit outweighs the reliability cost. |

## Decision

Do not enable publisher confirms. Accept ephemeral message loss.

## Related

- ADR 0004 (Ack-Before-Update) — listeners ack before cache write, complementing at-most-once delivery
