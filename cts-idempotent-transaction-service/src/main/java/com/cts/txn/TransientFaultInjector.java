package com.cts.txn;

/**
 * Seam for simulating (or in production, surfacing) transient failures just before the ledger commit.
 * Keeps failure simulation out of the business logic so it can be swapped in tests.
 */
@FunctionalInterface
public interface TransientFaultInjector {

    void beforeCommit(Transaction tx, int attempt) throws TransientFailureException;

    /** Never fails. */
    TransientFaultInjector NONE = (tx, attempt) -> { };

    /** Honors {@link Transaction#simulateTransientFailure()}: fails the first attempt only. */
    TransientFaultInjector FROM_TRANSACTION_FLAG = (tx, attempt) -> {
        if (tx.simulateTransientFailure() && attempt == 1) {
            throw new TransientFailureException("Simulated transient failure");
        }
    };
}
