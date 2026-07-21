package com.saiswayam.orchestrator.api;

import com.saiswayam.orchestrator.model.Task;
import com.saiswayam.orchestrator.model.TaskStatus;
import com.saiswayam.orchestrator.persistence.TaskRecord;
import com.saiswayam.orchestrator.persistence.TaskRecordRepository;
import com.saiswayam.orchestrator.persistence.TaskAuditService;
import com.saiswayam.orchestrator.queue.RedisTaskQueue;
import com.saiswayam.orchestrator.worker.TaskHandlerRegistry;
import jakarta.validation.Valid;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/tasks")
public class TaskController {

    private final RedisTaskQueue queue;
    private final TaskAuditService audit;
    private final TaskRecordRepository records;
    private final TaskHandlerRegistry registry;

    public TaskController(RedisTaskQueue queue, TaskAuditService audit,
                          TaskRecordRepository records, TaskHandlerRegistry registry) {
        this.queue = queue;
        this.audit = audit;
        this.records = records;
        this.registry = registry;
    }

    @PostMapping
    public ResponseEntity<Map<String, String>> submit(@Valid @RequestBody SubmitTaskRequest req) {
        if (!registry.supports(req.getType())) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "Unknown task type: " + req.getType()));
        }
        Task task = Task.of(req.getType(), req.getPayload(), req.getPriority(), req.getMaxRetries());
        if (req.getDelaySeconds() > 0) {
            task.setNotBefore(Instant.now().plusSeconds(req.getDelaySeconds()));
        }
        audit.recordSubmitted(task);
        queue.enqueue(task);
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of("taskId", task.getId(), "status", "PENDING"));
    }

    @GetMapping("/{id}")
    public ResponseEntity<TaskRecord> get(@PathVariable String id) {
        return records.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping
    public List<TaskRecord> recent(@RequestParam(defaultValue = "50") int limit,
                                   @RequestParam(required = false) TaskStatus status) {
        PageRequest page = PageRequest.of(0, Math.min(limit, 200));
        return status == null
                ? records.findAllByOrderByUpdatedAtDesc(page)
                : records.findByStatusOrderByUpdatedAtDesc(status, page);
    }
}
