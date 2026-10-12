package com.cts.txn;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/** Bounded exponential backoff with full jitter. */
public record RetryPolicy(int maxAttempts, Duration baseDelay, Duration maxDelay) {

    public RetryPolicy {
        if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts must be >= 1");
        if (baseDelay == null || baseDelay.isNegative()) throw new IllegalArgumentException("baseDelay must be >= 0");
        if (maxDelay == null || maxDelay.compareTo(baseDelay) < 0)
            throw new IllegalArgumentException("maxDelay must be >= baseDelay");
    }

    /** Retries without waiting; useful for tests and the simulated demo. */
    public static RetryPolicy immediate(int maxAttempts) {
        return new RetryPolicy(maxAttempts, Duration.ZERO, Duration.ZERO);
    }

    /** Delay to wait before the given (2nd or later) attempt: random in [0, min(maxDelay, base * 2^(n-2))]. */
    public Duration delayBefore(int attempt) {
        if (attempt < 2 || maxDelay.isZero()) return Duration.ZERO;
        long cap = Math.min(maxDelay.toMillis(), baseDelay.toMillis() << Math.min(attempt - 2, 20));
        return Duration.ofMillis(cap <= 0 ? 0 : ThreadLocalRandom.current().nextLong(cap + 1));
    }
}
