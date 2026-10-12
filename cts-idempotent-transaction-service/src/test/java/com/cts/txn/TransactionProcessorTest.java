package com.cts.txn;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.cts.txn.TransactionStatus.*;
import static com.cts.txn.TransactionType.CREDIT;
import static com.cts.txn.TransactionType.DEBIT;
import static org.junit.jupiter.api.Assertions.*;

class TransactionProcessorTest {
    private static final Instant T0 = Instant.parse("2026-10-11T10:00:00Z");
    private TransactionProcessor processor;

    @BeforeEach void setup() { processor = new TransactionProcessor(3); }

    private static Transaction tx(String id, String req, String account, int seq, TransactionType type,
                                  String amount, boolean fail) {
        return tx(id, req, account, seq, type, amount, "USD", fail);
    }

    private static Transaction tx(String id, String req, String account, int seq, TransactionType type,
                                  String amount, String currency, boolean fail) {
        return new Transaction(id, req, account, seq, type, new BigDecimal(amount), currency, T0, fail);
    }

    private static void assertBalance(String expected, BigDecimal actual) {
        assertTrue(new BigDecimal(expected).compareTo(actual) == 0, "expected balance " + expected + " but was " + actual);
    }

    // ---- idempotency

    @Test void duplicateDoesNotApplyBalanceTwice() {
        var first = processor.process(tx("T1", "R1", "A1", 1, CREDIT, "100", false));
        var duplicate = processor.process(tx("T1", "R2", "A1", 1, CREDIT, "100", false));
        assertEquals(PROCESSED, first.status());
        assertEquals(DUPLICATE, duplicate.status());
        assertBalance("100", processor.balance("A1"));
    }

    @Test void duplicateReplayDoesNotOverwriteStoredStatus() {
        processor.process(tx("T1", "R1", "A1", 1, CREDIT, "100", false));
        processor.process(tx("T1", "R2", "A1", 1, CREDIT, "100", false));
        assertEquals(PROCESSED, processor.status("T1"));
    }

    @Test void sameTransactionIdWithDifferentPayloadIsConflictAndKeepsOriginal() {
        processor.process(tx("T1", "R1", "A1", 1, CREDIT, "50", false));
        var result = processor.process(tx("T1", "R2", "A1", 1, CREDIT, "70", false));
        assertEquals(FAILED, result.status());
        assertEquals(PROCESSED, processor.status("T1"));
        assertBalance("50", processor.balance("A1"));
    }

    @Test void equivalentAmountScaleIsTreatedAsSamePayload() {
        processor.process(tx("T1", "R1", "A1", 1, CREDIT, "50", false));
        var replay = processor.process(tx("T1", "R2", "A1", 1, CREDIT, "50.00", false));
        assertEquals(DUPLICATE, replay.status());
    }

    @Test void requestIdReusedForDifferentTransactionIsRejected() {
        processor.process(tx("T1", "R1", "A1", 1, CREDIT, "50", false));
        var result = processor.process(tx("T2", "R1", "A1", 2, CREDIT, "10", false));
        assertEquals(FAILED, result.status());
        assertNull(processor.status("T2"));
        assertBalance("50", processor.balance("A1"));
    }

    // ---- ordering

    @Test void futureSequenceWaitsThenProcessesWhenGapArrives() {
        var future = processor.process(tx("T2", "R2", "A1", 2, DEBIT, "20", false));
        assertEquals(RETRY_PENDING, future.status());
        assertEquals(1, processor.pendingCount());
        var first = processor.process(tx("T1", "R1", "A1", 1, CREDIT, "100", false));
        assertEquals(PROCESSED, first.status());
        assertEquals(PROCESSED, processor.status("T2"));
        assertEquals(0, processor.pendingCount());
        assertBalance("80", processor.balance("A1"));
    }

    @Test void drainedTransitionsAreReportedToListener() {
        List<ProcessingResult> late = new CopyOnWriteArrayList<>();
        var p = new TransactionProcessor(RetryPolicy.immediate(3), TransientFaultInjector.NONE, late::add);
        p.process(tx("T3", "R3", "A1", 3, CREDIT, "5", false));
        p.process(tx("T2", "R2", "A1", 2, CREDIT, "5", false));
        p.process(tx("T1", "R1", "A1", 1, CREDIT, "5", false));
        assertEquals(2, late.size());
        assertEquals("T2", late.get(0).transactionId());
        assertEquals("T3", late.get(1).transactionId());
        assertBalance("15", p.balance("A1"));
    }

    @Test void staleSequenceIsRejected() {
        processor.process(tx("T1", "R1", "A1", 1, CREDIT, "10", false));
        var result = processor.process(tx("T9", "R9", "A1", 1, CREDIT, "10", false));
        assertEquals(FAILED, result.status());
        assertBalance("10", processor.balance("A1"));
    }

    @Test void secondTransactionForSameParkedSequenceIsRejected() {
        processor.process(tx("T2a", "R2a", "A1", 2, CREDIT, "10", false));
        var clash = processor.process(tx("T2b", "R2b", "A1", 2, CREDIT, "99", false));
        assertEquals(FAILED, clash.status());
        processor.process(tx("T1", "R1", "A1", 1, CREDIT, "1", false));
        assertEquals(0, processor.pendingCount()); // nothing stranded
        assertBalance("11", processor.balance("A1"));
    }

    @Test void absurdSequenceGapIsRejectedWithoutParking() {
        var result = processor.process(tx("T1", "R1", "A1", TransactionProcessor.MAX_SEQUENCE_GAP + 2, CREDIT, "1", false));
        assertEquals(FAILED, result.status());
        assertEquals(0, processor.pendingCount());
    }

    // ---- business rules

    @Test void insufficientBalanceDoesNotDebit() {
        var result = processor.process(tx("T1", "R1", "A1", 1, DEBIT, "50", false));
        assertEquals(FAILED, result.status());
        assertBalance("0", processor.balance("A1"));
    }

    @Test void businessRejectionConsumesSequenceSoStreamKeepsMoving() {
        processor.process(tx("T1", "R1", "A1", 1, DEBIT, "50", false)); // rejected
        var next = processor.process(tx("T2", "R2", "A1", 2, CREDIT, "30", false));
        assertEquals(PROCESSED, next.status());
        assertBalance("30", processor.balance("A1"));
    }

    @Test void rejectedParkedItemDoesNotBlockItsSuccessors() {
        processor.process(tx("T2", "R2", "A1", 2, DEBIT, "500", false));  // will be rejected when drained
        processor.process(tx("T3", "R3", "A1", 3, CREDIT, "7", false));
        processor.process(tx("T1", "R1", "A1", 1, CREDIT, "100", false));
        assertEquals(FAILED, processor.status("T2"));
        assertEquals(PROCESSED, processor.status("T3"));
        assertBalance("107", processor.balance("A1"));
    }

    @Test void currencyMismatchIsRejectedNotSummed() {
        processor.process(tx("T1", "R1", "A1", 1, CREDIT, "100", "USD", false));
        var result = processor.process(tx("T2", "R2", "A1", 2, CREDIT, "100", "EUR", false));
        assertEquals(FAILED, result.status());
        assertBalance("100", processor.balance("A1"));
    }

    // ---- validation

    @Test void invalidNegativeAmountIsRejectedAndIsolated() {
        var result = processor.process(tx("T1", "R1", "A1", 1, CREDIT, "-10", false));
        assertEquals(FAILED, result.status());
        assertBalance("0", processor.balance("A1"));
        assertNull(processor.status("T1"));
    }

    @Test void nullTransactionIsRejected() {
        assertEquals(FAILED, processor.process(null).status());
    }

    @Test void invalidCurrencyIsRejected() {
        assertEquals(FAILED, processor.process(tx("T1", "R1", "A1", 1, CREDIT, "1", "ZZZ", false)).status());
        assertEquals(FAILED, processor.process(tx("T2", "R2", "A1", 1, CREDIT, "1", "usd", false)).status());
    }

    @Test void tooManyDecimalPlacesForCurrencyIsRejected() {
        assertEquals(FAILED, processor.process(tx("T1", "R1", "A1", 1, CREDIT, "1.001", false)).status());
        assertEquals(PROCESSED, processor.process(tx("T2", "R2", "A1", 1, CREDIT, "1.25", false)).status());
    }

    @Test void hostileAmountMagnitudeIsRejectedCheaply() {
        assertEquals(FAILED, processor.process(tx("T1", "R1", "A1", 1, CREDIT, "1E+999999999", false)).status());
    }

    @Test void controlCharactersInIdsAreRejected() {
        assertEquals(FAILED, processor.process(tx("T1\nFORGED", "R1", "A1", 1, CREDIT, "1", false)).status());
    }

    @Test void constructorRejectsBadRetryLimit() {
        assertThrows(IllegalArgumentException.class, () -> new TransactionProcessor(0));
    }

    // ---- retries

    @Test void transientFailureRetriesWithoutDuplicateEffect() {
        var result = processor.process(tx("T1", "R1", "A1", 1, CREDIT, "50", true));
        assertEquals(PROCESSED, result.status());
        assertEquals(2, result.attempts());
        assertBalance("50", processor.balance("A1"));
    }

    @Test void exhaustedRetriesFailWithoutEffectAndRedeliveryIsProcessed() {
        AtomicInteger calls = new AtomicInteger();
        var p = new TransactionProcessor(RetryPolicy.immediate(3), (t, attempt) -> {
            if (calls.incrementAndGet() <= 3) throw new TransientFailureException("downstream unavailable");
        }, r -> { });

        var failed = p.process(tx("T1", "R1", "A1", 1, CREDIT, "50", false));
        assertEquals(FAILED, failed.status());
        assertEquals(3, failed.attempts());
        assertBalance("0", p.balance("A1"));

        var redelivered = p.process(tx("T1", "R1", "A1", 1, CREDIT, "50", false));
        assertEquals(PROCESSED, redelivered.status());
        assertBalance("50", p.balance("A1"));

        assertEquals(DUPLICATE, p.process(tx("T1", "R1", "A1", 1, CREDIT, "50", false)).status());
        assertBalance("50", p.balance("A1"));
    }

    @Test void exhaustedRetryDoesNotStrandParkedSuccessor() {
        AtomicInteger calls = new AtomicInteger();
        var p = new TransactionProcessor(RetryPolicy.immediate(2), (t, attempt) -> {
            if (calls.incrementAndGet() <= 2) throw new TransientFailureException("down");
        }, r -> { });
        p.process(tx("T2", "R2", "A1", 2, CREDIT, "5", false));
        assertEquals(FAILED, p.process(tx("T1", "R1", "A1", 1, CREDIT, "10", false)).status());
        assertEquals(RETRY_PENDING, p.status("T2")); // still parked, not lost
        assertEquals(PROCESSED, p.process(tx("T1", "R1b", "A1", 1, CREDIT, "10", false)).status());
        assertEquals(PROCESSED, p.status("T2"));
        assertBalance("15", p.balance("A1"));
    }

    // ---- concurrency

    @Test void duplicateArrivingMidRetryIsNotReprocessed() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        var p = new TransactionProcessor(RetryPolicy.immediate(3), (t, attempt) -> {
            entered.countDown();
            try {
                assertTrue(release.await(5, TimeUnit.SECONDS), "test timed out");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, r -> { });
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<ProcessingResult> first = pool.submit(() -> p.process(tx("T1", "R1", "A1", 1, CREDIT, "100", false)));
            assertTrue(entered.await(5, TimeUnit.SECONDS));

            var duplicate = p.process(tx("T1", "R2", "A1", 1, CREDIT, "100", false));
            assertEquals(DUPLICATE, duplicate.status());
            assertEquals(PROCESSING, p.status("T1"));
            var thief = p.process(tx("T-other", "R3", "A1", 1, CREDIT, "999", false));
            assertEquals(FAILED, thief.status()); // cannot steal the in-flight sequence slot

            release.countDown();
            assertEquals(PROCESSED, first.get(5, TimeUnit.SECONDS).status());
            assertBalance("100", p.balance("A1"));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test void concurrentSubmissionsOfSameTransactionApplyExactlyOnce() throws Exception {
        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<ProcessingResult>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            String requestId = "R" + i;
            futures.add(pool.submit(() -> {
                start.await();
                return processor.process(tx("T1", requestId, "A1", 1, CREDIT, "100", false));
            }));
        }
        start.countDown();
        int processed = 0, duplicates = 0;
        for (Future<ProcessingResult> f : futures) {
            TransactionStatus s = f.get(10, TimeUnit.SECONDS).status();
            if (s == PROCESSED) processed++;
            if (s == DUPLICATE) duplicates++;
        }
        pool.shutdownNow();
        assertEquals(1, processed);
        assertEquals(threads - 1, duplicates);
        assertBalance("100", processor.balance("A1"));
    }

    @Test void shuffledConcurrentSequencesAllApplyInOrder() throws Exception {
        int count = 200;
        List<Integer> order = new ArrayList<>();
        for (int i = 1; i <= count; i++) order.add(i);
        Collections.shuffle(order, new java.util.Random(42));

        ExecutorService pool = Executors.newFixedThreadPool(16);
        List<Future<ProcessingResult>> futures = new ArrayList<>();
        for (int seq : order) {
            futures.add(pool.submit(() -> processor.process(tx("T" + seq, "R" + seq, "A1", seq, CREDIT, "1", false))));
        }
        for (Future<ProcessingResult> f : futures) f.get(10, TimeUnit.SECONDS);
        pool.shutdownNow();

        assertBalance(Integer.toString(count), processor.balance("A1"));
        assertEquals(count, processor.statusCounts().get(PROCESSED));
        assertEquals(0, processor.pendingCount());
    }

    // ---- reporting

    @Test void summaryIsStablyOrderedAndStatusCountsReflectDrainedItems() {
        var results = new ArrayList<ProcessingResult>();
        results.add(processor.process(tx("T2", "R2", "A1", 2, CREDIT, "5", false)));
        results.add(processor.process(tx("T1", "R1", "A1", 1, CREDIT, "5", false)));
        var summary = processor.summary(results);
        assertEquals(List.of(RECEIVED, PROCESSING, PROCESSED, DUPLICATE, FAILED, RETRY_PENDING),
                new ArrayList<>(summary.counts().keySet()));
        assertEquals(1L, summary.counts().get(RETRY_PENDING)); // per-call view: T2 was parked when it returned
        assertEquals(2L, processor.statusCounts().get(PROCESSED)); // live view: T2 has since been processed
    }
}
