# Design Note — Idempotent Background Transaction Processor

## 1. Problem and assumptions
Requests may be duplicated, arrive out of order, fail transiently, or race each other. The demo models account credits/debits.
- A sequence is monotonic per account and starts at 1.
- Amounts are positive, bounded, and respect the currency's decimal places; a debit cannot make a balance negative.
- An account holds one currency; a transaction in another currency is rejected rather than summed.
- REVERSAL is modeled as a credit. A production reversal must reference and reverse a specific original transaction.

## 2. Idempotency
Business key: `transactionId`. A SHA-256 fingerprint of the business fields distinguishes a replay from an ID collision.
- Replay (same ID and payload) → `DUPLICATE`; the message states the original's current status. Stored state is never modified by a replay.
- Same ID, different payload → `FAILED` conflict.
- A `requestId` already bound to a different transaction → `FAILED`.
- Registration happens atomically with the claim, so a duplicate arriving while the original is retrying is a `DUPLICATE`, never a second effect.

## 3. Ordering
Each account has a `TreeMap<sequence, entry>` holding parked (`RETRY_PENDING`) and in-flight (`PROCESSING`) items.
- Sequence > expected → parked. Sequence < expected → stale `FAILED`. A sequence already held by another transaction → `FAILED` (otherwise the loser would be stranded forever).
- Gaps larger than 1000 are rejected to bound memory.
- After a commit, the next parked item is claimed (marked `PROCESSING` under the lock, so only one thread drains it) and executed through the normal retry path.
- **A sequence is consumed by any terminal business outcome**, including rejection for insufficient funds. Otherwise one rejected item would block the account forever. Exhausted transient retries are not terminal: the slot is released and a redelivery is processed.
- Known limitation: if the item at the head of the line is never redelivered, successors stay parked. Production needs a gap timeout, an alert, and an operator/skip procedure.

## 4. Retry and consistency
`RetryPolicy` provides bounded exponential backoff with full jitter. Retries happen outside the lock, so one slow transaction never blocks other accounts. Balance, ledger state, sequence, and status change together in a single critical section. Failure injection is behind `TransientFaultInjector`, keeping simulation out of business logic.
This gives atomicity within one JVM only; it is not a substitute for database transactions.

## 5. Lifecycle and observability
Statuses: RECEIVED, PROCESSING, PROCESSED, DUPLICATE, FAILED, RETRY_PENDING.
- `process()` returns the outcome at call time. Later outcomes (drained items) are pushed to a `transitionListener`.
- `statusCounts()` is a live snapshot, so reporting is not blind to drained items.
- Logs carry transaction ID, status, attempts and reason only; IDs are sanitized against log injection.

## 6. Test plan (all automated)
Idempotency: replay, replay doesn't overwrite status, conflict keeps original, scale-equivalent amounts, request-ID reuse.
Ordering: park-then-drain, listener notification, stale, same-slot clash, gap limit, rejected item doesn't block successors.
Business rules: insufficient funds, sequence consumed on rejection, currency mismatch.
Validation: negative amount, null, bad currency, excess decimals, hostile magnitude, control characters, bad retry limit.
Retries: transient retry once, exhaustion then redelivery, exhaustion doesn't strand a parked successor.
Concurrency: duplicate during in-flight retry, 32 concurrent identical submissions (exactly one effect), 200 shuffled concurrent sequences (all applied).

## 7. Production hardening
1. Durable store: unique constraints on `transactionId` and `requestId`; commit idempotency record, ledger entry and balance in one ACID transaction; per-account row lock or optimistic version.
2. Durable queue (Kafka/RabbitMQ), acknowledge only after commit; partition by account to preserve order.
3. Persist parked items and drive reprocessing from storage rather than in-process threads.
4. Transactional outbox for downstream effects; dead-letter queue and replay tooling.
5. Metrics (processed/duplicate/retry/failure rates, parked count and age, queue lag), tracing, alerts on stuck sequence gaps.
6. AuthN/AuthZ, request size and rate limits, safe error responses.
7. Crash-recovery, load and integration tests; periodic reconciliation.
8. Lock striping per account if a single lock becomes a bottleneck (profile first).
9. Agree reversal, currency and rounding semantics with the business owner.
