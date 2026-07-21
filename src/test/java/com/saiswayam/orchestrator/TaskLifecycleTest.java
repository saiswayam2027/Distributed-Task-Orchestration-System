package com.saiswayam.orchestrator;

import com.saiswayam.orchestrator.model.Task;
import com.saiswayam.orchestrator.model.TaskPriority;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class TaskLifecycleTest {

    @Test
    void newTaskHasZeroAttemptsAndIsImmediatelyEligible() {
        Task t = Task.of("echo", "{}", TaskPriority.HIGH, 3);
        assertEquals(0, t.getAttempt());
        assertFalse(t.retriesExhausted());
        assertFalse(t.getNotBefore().isAfter(Instant.now()));
    }

    @Test
    void retriesExhaustAfterMaxAttempts() {
        Task t = Task.of("flaky", "{}", TaskPriority.MEDIUM, 2);
        t.recordFailure("boom", Instant.now());
        assertFalse(t.retriesExhausted()); // attempt 1 of 2
        t.recordFailure("boom", Instant.now());
        assertTrue(t.retriesExhausted());  // attempt 2 of 2
    }

    @Test
    void failureSchedulesFutureEligibility() {
        Task t = Task.of("flaky", "{}", TaskPriority.MEDIUM, 3);
        Instant future = Instant.now().plusSeconds(30);
        t.recordFailure("timeout", future);
        assertEquals(future, t.getNotBefore());
        assertEquals("timeout", t.getLastError());
    }

    @Test
    void nullPriorityDefaultsToMedium() {
        Task t = Task.of("echo", "{}", null, 3);
        assertEquals(TaskPriority.MEDIUM, t.getPriority());
    }
}
