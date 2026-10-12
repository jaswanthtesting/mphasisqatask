package com.cts.txn;

public enum TransactionStatus {
    RECEIVED, PROCESSING, PROCESSED, DUPLICATE, FAILED, RETRY_PENDING
}
