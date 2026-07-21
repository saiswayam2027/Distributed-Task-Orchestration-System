package com.saiswayam.orchestrator.model;

import java.time.Instant;
import java.util.UUID;

/**
 * Task envelope that travels through the queue.
 * Serialized to JSON when stored in Redis.
 */
public class Task {

    private String id;
    private String type;          // logical task type, e.g. "send_email", "resize_image"
    private String payload;       // JSON payload for the handler
    private TaskPriority priority;
    private int attempt;          // 0-based attempt counter
    private int maxRetries;
    private Instant createdAt;
    private Instant notBefore;    // for delayed / backoff scheduling
    private String lastError;

    public Task() { }             // for Jackson

    public static Task of(String type, String payload, TaskPriority priority, int maxRetries) {
        Task t = new Task();
        t.id = UUID.randomUUID().toString();
        t.type = type;
        t.payload = payload;
        t.priority = priority == null ? TaskPriority.MEDIUM : priority;
        t.attempt = 0;
        t.maxRetries = maxRetries;
        t.createdAt = Instant.now();
        t.notBefore = Instant.now();
        return t;
    }

    public boolean retriesExhausted() {
        return attempt >= maxRetries;
    }

    public void recordFailure(String error, Instant nextEligibleAt) {
        this.attempt++;
        this.lastError = error;
        this.notBefore = nextEligibleAt;
    }

    // getters / setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public TaskPriority getPriority() { return priority; }
    public void setPriority(TaskPriority priority) { this.priority = priority; }
    public int getAttempt() { return attempt; }
    public void setAttempt(int attempt) { this.attempt = attempt; }
    public int getMaxRetries() { return maxRetries; }
    public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public Instant getNotBefore() { return notBefore; }
    public void setNotBefore(Instant notBefore) { this.notBefore = notBefore; }
    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }
}
