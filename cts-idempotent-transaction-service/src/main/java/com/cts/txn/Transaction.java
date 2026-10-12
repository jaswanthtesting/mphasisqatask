package com.cts.txn;

import java.math.BigDecimal;
import java.time.Instant;

public record Transaction(
        String transactionId,
        String requestId,
        String accountId,
        int sequenceNumber,
        TransactionType transactionType,
        BigDecimal amount,
        String currency,
        Instant eventTime,
        boolean simulateTransientFailure) {}
