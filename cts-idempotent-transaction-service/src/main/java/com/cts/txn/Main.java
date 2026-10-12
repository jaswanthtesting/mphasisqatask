package com.cts.txn;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Demo runner. The batch mirrors sample-input.json. */
public class Main {
    private static final Instant EVENT_TIME = Instant.parse("2026-10-11T10:00:00Z");

    public static void main(String[] args) {
        List<ProcessingResult> lateTransitions = new ArrayList<>();
        TransactionProcessor processor = new TransactionProcessor(
                new RetryPolicy(3, Duration.ofMillis(50), Duration.ofMillis(500)),
                TransientFaultInjector.FROM_TRANSACTION_FLAG,
                lateTransitions::add);

        List<Transaction> batch = List.of(
                tx("TXN-2002", "REQ-2", "ACC-201", 2, TransactionType.DEBIT, "40", false),
                tx("TXN-2001", "REQ-1", "ACC-201", 1, TransactionType.CREDIT, "100", false),
                tx("TXN-2002", "REQ-2-DUP", "ACC-201", 2, TransactionType.DEBIT, "40", false),
                tx("TXN-2003", "REQ-3", "ACC-201", 3, TransactionType.CREDIT, "25", false),
                tx("TXN-2004", "REQ-4", "ACC-202", 1, TransactionType.DEBIT, "-10", false),
                tx("TXN-2005", "REQ-5", "ACC-203", 1, TransactionType.CREDIT, "500", true));

        List<ProcessingResult> results = new ArrayList<>();
        for (Transaction tx : batch) results.add(processor.process(tx));

        System.out.println("\n--- Result returned for each submission ---");
        results.forEach(System.out::println);

        System.out.println("\n--- Outcomes reached later (pending items drained) ---");
        lateTransitions.forEach(System.out::println);

        System.out.println("\n--- Final status of all accepted transactions ---");
        processor.statusCounts().forEach((status, count) -> System.out.println(status + "=" + count));

        System.out.println("\nBalances: ACC-201=" + processor.balance("ACC-201")
                + ", ACC-202=" + processor.balance("ACC-202")
                + ", ACC-203=" + processor.balance("ACC-203"));
    }

    private static Transaction tx(String id, String requestId, String account, int seq,
                                  TransactionType type, String amount, boolean fail) {
        return new Transaction(id, requestId, account, seq, type, new BigDecimal(amount), "USD", EVENT_TIME, fail);
    }
}
