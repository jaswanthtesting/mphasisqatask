package com.cts.txn;

/** Signals a recoverable infrastructure-style failure; the operation is safe to retry. */
public class TransientFailureException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public TransientFailureException(String message) {
        super(message);
    }
}
