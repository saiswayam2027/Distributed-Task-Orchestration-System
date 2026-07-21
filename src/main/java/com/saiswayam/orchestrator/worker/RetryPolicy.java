package com.saiswayam.orchestrator.worker;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff with full jitter (AWS-recommended strategy).
 * delay = random(0, min(cap, base * 2^attempt))
 *
 * Jitter matters: without it, a burst of tasks that fail together
 * retries together, hammering the downstream dependency in waves
 * (the "thundering herd" problem).
 */
public final class RetryPolicy {

    private final Duration base;
    private final Duration cap;

    public RetryPolicy(Duration base, Duration cap) {
        this.base = base;
        this.cap = cap;
    }

    public static RetryPolicy defaults() {
        return new RetryPolicy(Duration.ofSeconds(2), Duration.ofMinutes(5));
    }

    public Instant nextEligibleAt(int attempt) {
        long exp = (long) (base.toMillis() * Math.pow(2, attempt));
        long capped = Math.min(cap.toMillis(), exp);
        long jittered = ThreadLocalRandom.current().nextLong(0, Math.max(1, capped));
        return Instant.now().plusMillis(jittered);
    }
}
