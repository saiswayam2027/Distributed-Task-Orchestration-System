package com.saiswayam.orchestrator;

import com.saiswayam.orchestrator.worker.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class RetryPolicyTest {

    @Test
    void backoffNeverExceedsCap() {
        RetryPolicy policy = new RetryPolicy(Duration.ofSeconds(1), Duration.ofSeconds(10));
        Instant before = Instant.now();
        // attempt 20 would be 2^20 seconds without a cap
        Instant next = policy.nextEligibleAt(20);
        long delayMs = next.toEpochMilli() - before.toEpochMilli();
        assertTrue(delayMs <= 10_500, "delay should be capped near 10s, was " + delayMs + "ms");
    }

    @Test
    void jitterProducesVariedDelays() {
        RetryPolicy policy = new RetryPolicy(Duration.ofSeconds(2), Duration.ofMinutes(5));
        long first = policy.nextEligibleAt(5).toEpochMilli();
        boolean varied = false;
        for (int i = 0; i < 20; i++) {
            if (policy.nextEligibleAt(5).toEpochMilli() != first) { varied = true; break; }
        }
        assertTrue(varied, "full jitter should produce different delays across calls");
    }

    @Test
    void earlyAttemptsRetryQuickly() {
        RetryPolicy policy = new RetryPolicy(Duration.ofSeconds(2), Duration.ofMinutes(5));
        Instant next = policy.nextEligibleAt(0);
        long delayMs = next.toEpochMilli() - Instant.now().toEpochMilli();
        assertTrue(delayMs <= 2_100, "attempt 0 should retry within ~2s, was " + delayMs + "ms");
    }
}
