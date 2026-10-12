package com.cts.txn;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public record ProcessingSummary(List<ProcessingResult> results, Map<TransactionStatus, Long> counts) {
    public static ProcessingSummary from(List<ProcessingResult> results) {
        Map<TransactionStatus, Long> counts = new EnumMap<>(TransactionStatus.class);
        for (TransactionStatus status : TransactionStatus.values()) counts.put(status, 0L);
        results.forEach(r -> counts.merge(r.status(), 1L, Long::sum));
        // unmodifiableMap keeps the enum ordering; Map.copyOf would make iteration order arbitrary.
        return new ProcessingSummary(List.copyOf(results), Collections.unmodifiableMap(counts));
    }
}
