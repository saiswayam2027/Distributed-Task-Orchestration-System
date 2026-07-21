package com.saiswayam.orchestrator.persistence;

import com.saiswayam.orchestrator.model.TaskPriority;
import com.saiswayam.orchestrator.model.TaskStatus;
import jakarta.persistence.*;

import java.time.Instant;

/** Durable audit record of every task's lifecycle, queryable after Redis entries are gone. */
@Entity
@Table(name = "task_records", indexes = {
        @Index(name = "idx_task_status", columnList = "status"),
        @Index(name = "idx_task_type", columnList = "type")
})
public class TaskRecord {

    @Id
    @Column(length = 36)
    private String id;

    @Column(nullable = false)
    private String type;

    @Column(columnDefinition = "text")
    private String payload;

    @Enumerated(EnumType.STRING)
    private TaskPriority priority;

    @Enumerated(EnumType.STRING)
    private TaskStatus status;

    private int attempt;
    private String workerId;

    @Column(columnDefinition = "text")
    private String lastError;

    private Instant createdAt;
    private Instant updatedAt;

    public TaskRecord() { }

    // getters/setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public TaskPriority getPriority() { return priority; }
    public void setPriority(TaskPriority priority) { this.priority = priority; }
    public TaskStatus getStatus() { return status; }
    public void setStatus(TaskStatus status) { this.status = status; }
    public int getAttempt() { return attempt; }
    public void setAttempt(int attempt) { this.attempt = attempt; }
    public String getWorkerId() { return workerId; }
    public void setWorkerId(String workerId) { this.workerId = workerId; }
    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
}
