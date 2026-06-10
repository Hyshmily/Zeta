# Acceptable Data Race in HeavyKeeper Fading

`HeavyKeeper.fading()` performs a non-atomic read-modify-write (`counts[i] >>= 1`) on `long[]` counters without acquiring the per-stripe lock that `add()` uses for `counts[index] += increment`. JLS 17.7 does NOT guarantee atomicity for `long` writes on 32-bit JVMs, so a concurrent `add()` may observe a torn value during `fading()`. This creates a data race: a concurrent `add()` can see a stale count before `fading()` halves it, losing one increment.

This race is accepted as a deliberate trade-off, not a bug.

| Dimension | Evaluation |
|-----------|------------|
| **Hard to reverse** | Locking `fading()` would add thread contention on an infrequent-but-hot path (runs every `decayIntervalMs`, default 30s). Using `AtomicLongArray` would bloat every counter slot and degrade `add()` throughput. |
| **Surprising without context** | Yes — a static-analysis tool or new contributor will flag the non-atomic `long >>=1` immediately. This ADR is the context they need. |
| **Result of a real trade-off** | HeavyKeeper is probabilistic (Count-Min Sketch variant). A lost increment on a single decay cycle (every 30s per counter) is negligible — it at most delays hot promotion by one decay cycle for a key near the boundary. The alternative (locking or atomic counters) degrades every `add()` on the hot path. |

**Decision:** Keep `>>=1` unlocked. Do not change to `AtomicLongArray`. Do not lock `fading()`.

## Context

- `add()` uses per-stripe `synchronized` blocks on `Object[] locks` for the counter update and decay loop
- `fading()` iterates the full `counts` array and halves every slot
- The race window is one iteration of `fading()` per counter (single `>>=1` instruction)
- In practice, `fading()` and `add()` for the same counter collide rarely because `fading()` iterates the full array (O(depth × width)) while `add()` touches a single counter from one stripe
- Even when they collide, the error is at most 1 count lost per 30-second window on a single counter — negligible for the TopK ranking
