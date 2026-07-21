package com.saiswayam.orchestrator.model;

/**
 * Lifecycle of a task:
 * PENDING -> RUNNING -> COMPLETED
 *                    -> FAILED (retryable) -> PENDING (re-queued with backoff)
 *                    -> DEAD (exhausted retries, moved to dead-letter queue)
 */
public enum TaskStatus {
    PENDING, RUNNING, COMPLETED, FAILED, DEAD
}
