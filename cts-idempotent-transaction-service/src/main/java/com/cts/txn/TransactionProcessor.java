package com.cts.txn;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * In-memory idempotent, order-aware transaction processor.
 *
 * <p>Concurrency model: all shared state is guarded by one lock, held only for short, non-blocking
 * critical sections. Retry/backoff happens <em>outside</em> the lock. While a transaction is retrying it
 * stays registered as PROCESSING and owns its sequence slot, so a concurrent duplicate is rejected as a
 * DUPLICATE rather than applied twice, and a different transaction cannot steal the slot.
 *
 * <p>Sequence semantics: a sequence number is consumed once its transaction reaches a terminal business
 * outcome (PROCESSED or a business rejection such as insufficient funds), so one rejected item never
 * blocks the rest of the account's stream. Exhausted transient retries are <em>not</em> terminal: the
 * slot is released and a redelivery of the same payload is processed again.
 *
 * <p>Production caveat: state is JVM-local and volatile; see DESIGN.md for the durable-store design.
 */
public class TransactionProcessor {
    private static final Logger LOG = Logger.getLogger(TransactionProcessor.class.getName());

    /** Largest accepted distance between the expected and received sequence; bounds pending memory. */
    static final int MAX_SEQUENCE_GAP = 1_000;

    private final RetryPolicy retryPolicy;
    private final TransientFaultInjector faults;
    private final Consumer<ProcessingResult> transitionListener;

    private final Object lock = new Object();
    private final Map<String, Entry> entries = new HashMap<>();        // transactionId -> record
    private final Map<String, String> requestOwners = new HashMap<>(); // requestId -> transactionId
    private final Map<String, Ledger> ledgers = new HashMap<>();       // accountId -> ledger

    public TransactionProcessor(int maxAttempts) {
        this(RetryPolicy.immediate(maxAttempts), TransientFaultInjector.FROM_TRANSACTION_FLAG, result -> { });
    }

    /**
     * @param transitionListener notified of every outcome reached <em>after</em> {@link #process} already
     *                           returned for that transaction (for example a pending item drained once its
     *                           gap is filled). Invoked on the draining thread, outside the lock.
     */
    public TransactionProcessor(RetryPolicy retryPolicy, TransientFaultInjector faults,
                                Consumer<ProcessingResult> transitionListener) {
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
        this.faults = Objects.requireNonNull(faults, "faults");
        this.transitionListener = Objects.requireNonNull(transitionListener, "transitionListener");
    }

    // ------------------------------------------------------------------ public API

    public ProcessingResult process(Transaction tx) {
        String invalid = TransactionValidator.validate(tx);
        if (invalid != null) {
            // Identity of an invalid request is untrustworthy, so nothing is recorded for it.
            return log(new ProcessingResult(tx == null ? null : tx.transactionId(), TransactionStatus.FAILED, invalid, 0));
        }

        Claim claim = claim(tx);
        if (claim.entry == null) return log(claim.immediate);

        ProcessingResult mine = log(execute(claim.entry));
        drain(tx.accountId());
        return mine;
    }

    public BigDecimal balance(String accountId) {
        synchronized (lock) {
            Ledger ledger = ledgers.get(accountId);
            return ledger == null ? BigDecimal.ZERO : ledger.balance;
        }
    }

    /** Current lifecycle status of a transaction that was accepted for processing, or {@code null}. */
    public TransactionStatus status(String transactionId) {
        synchronized (lock) {
            Entry entry = entries.get(transactionId);
            return entry == null ? null : entry.status;
        }
    }

    /** Snapshot of how many known transactions are in each status (reflects drained items, unlike per-call results). */
    public Map<TransactionStatus, Long> statusCounts() {
        Map<TransactionStatus, Long> counts = new EnumMap<>(TransactionStatus.class);
        for (TransactionStatus s : TransactionStatus.values()) counts.put(s, 0L);
        synchronized (lock) {
            entries.values().forEach(e -> counts.merge(e.status, 1L, Long::sum));
        }
        return counts;
    }

    /** Number of transactions currently parked waiting for a missing sequence number. */
    public int pendingCount() {
        synchronized (lock) {
            return (int) entries.values().stream().filter(e -> e.status == TransactionStatus.RETRY_PENDING).count();
        }
    }

    public ProcessingSummary summary(java.util.List<ProcessingResult> results) {
        return ProcessingSummary.from(results);
    }

    // ------------------------------------------------------------------ phase 1: claim

    /** Decides, atomically, whether this call is rejected, parked, or becomes the single executor. */
    private Claim claim(Transaction tx) {
        String id = tx.transactionId();
        String fingerprint = Fingerprints.of(tx);

        synchronized (lock) {
            Entry existing = entries.get(id);
            if (existing != null) {
                if (!existing.fingerprint.equals(fingerprint))
                    return Claim.rejected(id, TransactionStatus.FAILED, "Transaction ID reused with different payload");
                if (!existing.retryable)
                    return Claim.rejected(id, TransactionStatus.DUPLICATE,
                            "Duplicate of transaction already in status " + existing.status + "; no effect applied");
                entries.remove(id); // earlier attempt exhausted transient retries: this redelivery re-runs it
            }

            String owner = requestOwners.get(tx.requestId());
            if (owner != null && !owner.equals(id))
                return Claim.rejected(id, TransactionStatus.FAILED, "Request ID already used by a different transaction");

            Ledger ledger = ledgers.computeIfAbsent(tx.accountId(), k -> new Ledger());
            int seq = tx.sequenceNumber();
            int expected = ledger.nextSequence;

            if (seq < expected) {
                Entry stale = register(tx, fingerprint);
                stale.status = TransactionStatus.FAILED;
                stale.message = "Stale sequence: expected " + expected;
                return Claim.rejected(id, stale.status, stale.message);
            }
            if ((long) seq - expected > MAX_SEQUENCE_GAP)
                return Claim.rejected(id, TransactionStatus.FAILED,
                        "Sequence too far ahead: expected " + expected + " but received " + seq);
            if (ledger.slots.containsKey(seq))
                return Claim.rejected(id, TransactionStatus.FAILED,
                        "Sequence " + seq + " is already reserved by another transaction");

            Entry entry = register(tx, fingerprint);
            ledger.slots.put(seq, entry);
            if (seq > expected) {
                entry.status = TransactionStatus.RETRY_PENDING;
                entry.message = "Sequence gap: expected " + expected + " but received " + seq;
                return Claim.rejected(id, entry.status, entry.message);
            }
            entry.status = TransactionStatus.PROCESSING;
            return Claim.execute(entry);
        }
    }

    private Entry register(Transaction tx, String fingerprint) {
        Entry entry = new Entry(tx, fingerprint);
        entries.put(tx.transactionId(), entry);
        requestOwners.put(tx.requestId(), tx.transactionId());
        return entry;
    }

    // ------------------------------------------------------------------ phase 2: execute (retries outside the lock)

    private ProcessingResult execute(Entry entry) {
        Transaction tx = entry.tx;
        int max = retryPolicy.maxAttempts();
        for (int attempt = 1; attempt <= max; attempt++) {
            try {
                faults.beforeCommit(tx, attempt);
            } catch (TransientFailureException e) {
                LOG.log(Level.WARNING, "Transient failure: id={0}, attempt={1}/{2}",
                        new Object[] {safe(tx.transactionId()), attempt, max});
                if (attempt == max) return releaseAfterExhaustedRetries(entry, attempt);
                if (!pause(retryPolicy.delayBefore(attempt + 1))) return releaseAfterExhaustedRetries(entry, attempt);
                continue;
            }
            return commit(entry, attempt);
        }
        return releaseAfterExhaustedRetries(entry, max); // unreachable: maxAttempts >= 1
    }

    private ProcessingResult commit(Entry entry, int attempt) {
        Transaction tx = entry.tx;
        synchronized (lock) {
            Ledger ledger = ledgers.get(tx.accountId());
            String rejection = null;
            if (ledger.currency != null && !ledger.currency.equals(tx.currency())) {
                rejection = "Currency mismatch: account is held in " + ledger.currency;
            } else {
                BigDecimal delta = tx.transactionType() == TransactionType.DEBIT ? tx.amount().negate() : tx.amount();
                BigDecimal updated = ledger.balance.add(delta);
                if (updated.signum() < 0) {
                    rejection = "Insufficient balance";
                } else {
                    ledger.balance = updated;
                    ledger.currency = tx.currency();
                }
            }
            // Balance, ledger state, sequence and status change together under one lock hold.
            ledger.slots.remove(tx.sequenceNumber());
            ledger.nextSequence = tx.sequenceNumber() + 1;
            entry.attempts = attempt;
            entry.status = rejection == null ? TransactionStatus.PROCESSED : TransactionStatus.FAILED;
            entry.message = rejection != null ? rejection
                    : attempt > 1 ? "Processed after retry" : "Processed successfully";
            return entry.toResult();
        }
    }

    private ProcessingResult releaseAfterExhaustedRetries(Entry entry, int attempts) {
        synchronized (lock) {
            entry.status = TransactionStatus.FAILED;
            entry.message = "Retry limit exhausted";
            entry.attempts = attempts;
            entry.retryable = true;
            Ledger ledger = ledgers.get(entry.tx.accountId());
            ledger.slots.remove(entry.tx.sequenceNumber(), entry);
            return entry.toResult();
        }
    }

    private boolean pause(Duration delay) {
        if (delay.isZero()) return true;
        try {
            Thread.sleep(delay.toMillis());
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    // ------------------------------------------------------------------ phase 3: drain parked successors

    /**
     * Runs parked transactions that have become next in line. Each goes through the same retry/commit path
     * as a direct submission. Claiming marks the entry PROCESSING under the lock, so only one thread ever
     * drains a given item.
     */
    private void drain(String accountId) {
        Entry next;
        while ((next = claimNextReady(accountId)) != null) {
            ProcessingResult result = log(execute(next));
            try {
                transitionListener.accept(result);
            } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "Transition listener failed for id={0}", safe(result.transactionId()));
            }
        }
    }

    private Entry claimNextReady(String accountId) {
        synchronized (lock) {
            Ledger ledger = ledgers.get(accountId);
            Entry candidate = ledger.slots.get(ledger.nextSequence);
            if (candidate == null || candidate.status != TransactionStatus.RETRY_PENDING) return null;
            candidate.status = TransactionStatus.PROCESSING;
            return candidate;
        }
    }

    // ------------------------------------------------------------------ internals

    private ProcessingResult log(ProcessingResult r) {
        LOG.log(Level.INFO, "Transaction outcome: id={0}, status={1}, attempts={2}, reason={3}",
                new Object[] {safe(r.transactionId()), r.status(), r.attempts(), r.message()});
        return r;
    }

    /** Neutralizes control characters and bounds length so untrusted IDs cannot forge log lines. */
    private static String safe(String value) {
        if (value == null) return "null";
        String cleaned = value.replaceAll("\\p{Cntrl}", "?");
        return cleaned.length() > 64 ? cleaned.substring(0, 64) + "..." : cleaned;
    }

    /** Mutable per-transaction record; every field except {@code tx}/{@code fingerprint} is guarded by {@code lock}. */
    private static final class Entry {
        final Transaction tx;
        final String fingerprint;
        TransactionStatus status = TransactionStatus.RECEIVED;
        String message = "Received";
        int attempts;
        boolean retryable;

        Entry(Transaction tx, String fingerprint) {
            this.tx = tx;
            this.fingerprint = fingerprint;
        }

        ProcessingResult toResult() {
            return new ProcessingResult(tx.transactionId(), status, message, attempts);
        }
    }

    /** Per-account state. {@code slots} holds parked and in-flight transactions keyed by sequence number. */
    private static final class Ledger {
        BigDecimal balance = BigDecimal.ZERO;
        String currency;
        int nextSequence = 1;
        final TreeMap<Integer, Entry> slots = new TreeMap<>();
    }

    /** Outcome of the claim phase: either an immediate answer, or the entry this caller must execute. */
    private record Claim(ProcessingResult immediate, Entry entry) {
        static Claim rejected(String id, TransactionStatus status, String message) {
            return new Claim(new ProcessingResult(id, status, message, 0), null);
        }

        static Claim execute(Entry entry) {
            return new Claim(null, entry);
        }
    }
}
