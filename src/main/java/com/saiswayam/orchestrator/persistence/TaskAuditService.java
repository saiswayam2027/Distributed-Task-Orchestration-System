package com.saiswayam.orchestrator.persistence;

import com.saiswayam.orchestrator.model.Task;
import com.saiswayam.orchestrator.model.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * Writes the durable audit trail. Deliberately best-effort: an audit failure
 * must never fail the task itself (queue state in Redis remains the source
 * of truth for execution; Postgres is the system of record for history).
 */
@Service
public class TaskAuditService {

    private static final Logger log = LoggerFactory.getLogger(TaskAuditService.class);

    private final TaskRecordRepository repo;

    public TaskAuditService(TaskRecordRepository repo) { this.repo = repo; }

    @Transactional
    public void recordSubmitted(Task task) {
        try {
            TaskRecord r = new TaskRecord();
            r.setId(task.getId());
            r.setType(task.getType());
            r.setPayload(task.getPayload());
            r.setPriority(task.getPriority());
            r.setStatus(TaskStatus.PENDING);
            r.setAttempt(0);
            r.setCreatedAt(task.getCreatedAt());
            r.setUpdatedAt(Instant.now());
            repo.save(r);
        } catch (Exception e) {
            log.error("Audit write failed for submit of {}", task.getId(), e);
        }
    }

    @Transactional
    public void markRunning(Task task, String workerId) {
        try {
            repo.findById(task.getId()).ifPresent(r -> {
                r.setStatus(TaskStatus.RUNNING);
                r.setWorkerId(workerId);
                r.setAttempt(task.getAttempt());
                r.setUpdatedAt(Instant.now());
                repo.save(r);
            });
        } catch (Exception e) {
            log.error("Audit write failed (running) for {}", task.getId(), e);
        }
    }

    @Transactional
    public void markStatus(Task task, TaskStatus status, String error) {
        try {
            repo.findById(task.getId()).ifPresent(r -> {
                r.setStatus(status);
                r.setAttempt(task.getAttempt());
                r.setLastError(error);
                r.setUpdatedAt(Instant.now());
                repo.save(r);
            });
        } catch (Exception e) {
            log.error("Audit write failed ({}) for {}", status, task.getId(), e);
        }
    }
}
