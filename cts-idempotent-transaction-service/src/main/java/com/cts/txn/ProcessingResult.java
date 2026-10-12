package com.cts.txn;

public record ProcessingResult(String transactionId, TransactionStatus status, String message, int attempts) {}
