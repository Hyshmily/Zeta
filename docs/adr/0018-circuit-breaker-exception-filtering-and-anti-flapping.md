# Exception Filtering and Anti-Flapping for Circuit Breaker

The `SingleFlight.load()` circuit breaker (wrapping remote cache-load suppliers) was extended with two features inspired by the `neural-circuitbreaker` project: exception filtering via `includeExceptions`/`excludeExceptions`, and consecutive success counting in the HALF_OPEN state to prevent flapping.

## Motivation

The original circuit breaker had two blind spots:

1. **Every exception is a failure.** A database timeout and an `IllegalArgumentException` from malformed user input were treated identically. The latter is a client error that should never trip the breaker — but doing so would silently degrade all subsequent requests for unrelated keys.

2. **Single success closes the breaker.** In the original design, any successful probe in OPEN state immediately transitioned to CLOSED via `AtomicBoolean.compareAndSet(true, false)`. A single lucky probe during a transient recovery window would close the breaker, only for it to open again on the very next request — causing OPEN/CLOSE oscillation.

## Decision

### Exception Filtering

The breaker now accepts an `onFailure(Throwable t)` overload. The throwable's class name (and its `cause` chain) is checked against two optional lists:

- **`excludeExceptions`** (whitelist): matching exceptions are treated as success — they increment the success bucket and never count toward the failure rate.
- **`includeExceptions`** (blacklist): when non-empty, ONLY matching exceptions trigger failure counting; all others are treated as success.

When both lists are empty (default), every exception counts as failure — preserving backward compatibility.

### Consecutive Success Counting (Anti-Flapping)

The state machine was changed from a binary `AtomicBoolean` to a three-value `CircuitBreakerState` enum (`CLOSED`/`OPEN`/`HALF_OPEN`). The half-open to closed transition now requires `consecutiveSuccessThreshold` consecutive successes:

```
HALF_OPEN → success → consecutiveSuccessCounter++
  └─ counter >= threshold → CLOSED (reset all buckets)
  └─ any failure → OPEN (reset counter)
```

The HALF_OPEN state is entered from OPEN after `singleTestIntervalMs` — at which point `consecutiveSuccessCounter` is reset to 0. This mirrors the `neural-circuitbreaker` pattern: the `openHalf()` method resets the counter before any probe is allowed.

### State Enum

`CircuitBreakerState` replaces the raw `AtomicBoolean open` field. The interface exposes `getState()` for diagnostics and monitoring. The enum provides three states:
- `CLOSED` — normal operation, sliding-window failure tracking
- `OPEN` — fast-fail, only probe requests allowed after timeout
- `HALF_OPEN` — probe state, consecutive success counting

## Considered Options

- **Exponential backoff for retry interval** — considered but not implemented because `singleTestIntervalMs` already provides a configurable retry cadence. Exponential backoff would add complexity without measurable benefit for the cache-load use case (suppliers are typically idempotent and fast).
- **Counting all exceptions equally** (status quo) — rejected because it conflates system-failure exceptions (connection timeout) with client-error exceptions (bad argument). The former should trip the breaker; the latter should not.
- **Single success closes** (status quo) — rejected because empirical observation showed that transient blips (network jitter, connection pool exhaustion) cause rapid OPEN/CLOSE oscillation. Consecutive success threshold provides natural hysteresis.

## Consequences

1. **Exception lists are class-name strings, not classes.** Using `String` avoids classloading issues when the exception class is not on the app classpath at configuration time. The trade-off is no compile-time safety — a typo in the class name silently disables filtering for that entry.

2. **`onFailure(Throwable)` vs `onFailure()`.** The `onFailure(Throwable)` overload delegates to `onFailure()` after the ignorable check. Callers that cannot provide the exception (e.g., timeout paths in `CompletableFuture`) continue using the no-arg version — the filtering is a best-effort enhancement, not a required API change.

3. **`consecutiveSuccessThreshold` defaults to 3.** This was chosen as a balance between stability (preventing flapping) and recovery speed (three probes at `singleTestIntervalMs` = 15s worst case to recover). Users with fast-recovery suppliers may lower this to 1 (the original behavior, no flapping protection).

4. **Backward compatibility.** The no-arg `onFailure()` retains the original semantics (always counts as failure). Existing tests pass without modification. The `CircuitBreakerState` enum is a new type — code that only checks `isOpen()` is unaffected.
