# Idempotent Transaction Processor

A small Java 17 + Maven implementation of an idempotent, order-aware transaction processor. It uses in-memory
state, so it runs locally with no external infrastructure.

## Run
```bash
mvn test                                   # unit + concurrency tests
mvn package
java -jar target/idempotent-transaction-service-1.0.0.jar
# or: mvn exec:java
```
The demo batch in `Main` mirrors `sample-input.json`.

## Behavior at a glance
| Situation | Result |
|---|---|
| New, valid, next-in-sequence | `PROCESSED` |
| Same `transactionId`, same payload (any `requestId`) | `DUPLICATE`, no effect, stored status untouched |
| Same `transactionId`, different payload | `FAILED` (conflict), original untouched |
| `requestId` already used by a different transaction | `FAILED` |
| Sequence ahead of expected | `RETRY_PENDING`, auto-processed when the gap fills |
| Sequence already consumed / already reserved / more than 1000 ahead | `FAILED` |
| Insufficient funds / currency mismatch | `FAILED`; the sequence is consumed so later items proceed |
| Transient failure | retried with bounded exponential backoff + jitter |
| Retries exhausted | `FAILED`, no effect; a redelivery of the same payload is processed again |
| Invalid input | `FAILED`, nothing recorded |

## Design choices
- **Idempotency key:** `transactionId`, with a SHA-256 fingerprint of the business fields (length-prefixed, so field values cannot collide across delimiters).
- **Concurrency:** one lock guards short critical sections only. Retries and backoff run outside it. A retrying transaction stays `PROCESSING` and owns its sequence slot, so a concurrent duplicate cannot double-apply and another transaction cannot take the slot.
- **Ordering:** per-account `TreeMap` of parked/in-flight sequences. Draining is O(log n) per item and runs through the same retry/commit path as direct submissions.
- **Reporting:** `process()` returns the outcome at call time; outcomes reached later (drained items) go to a `transitionListener`, and `statusCounts()` gives the live view.
- **Validation:** ID charset/length, ISO 4217 currency, decimal places per currency, bounded magnitude (guards against hostile exponents), one currency per account.
- **Logging:** IDs only (no account or customer data), control characters neutralized.

## Limits of this implementation
State is lost on restart and locking is JVM-local. See `DESIGN.md` for the production design (durable store with unique constraints, queue + outbox, DLQ, metrics, auth).

## Verification note
Run `mvn test` in your own environment to confirm. Output of the demo is deterministic apart from backoff timing.
