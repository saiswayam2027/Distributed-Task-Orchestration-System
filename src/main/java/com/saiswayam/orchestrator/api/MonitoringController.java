package com.saiswayam.orchestrator.api;

import com.saiswayam.orchestrator.model.TaskStatus;
import com.saiswayam.orchestrator.persistence.TaskRecordRepository;
import com.saiswayam.orchestrator.queue.RedisTaskQueue;
import com.saiswayam.orchestrator.worker.WorkerPool;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/monitor")
public class MonitoringController {

    private final WorkerPool pool;
    private final RedisTaskQueue queue;
    private final TaskRecordRepository records;

    public MonitoringController(WorkerPool pool, RedisTaskQueue queue, TaskRecordRepository records) {
        this.pool = pool;
        this.queue = queue;
        this.records = records;
    }

    @GetMapping
    public Map<String, Object> snapshot() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("timestamp", Instant.now());
        out.put("poolRunning", pool.isRunning());
        out.put("workers", pool.heartbeats());
        out.put("queue", Map.of(
                "pending", queue.pendingCount(),
                "inflight", queue.inflightCount(),
                "deadLetter", queue.deadLetterCount()));
        out.put("counters", Map.of(
                "completed", pool.completed(),
                "failedAttempts", pool.failed()));
        out.put("history", Map.of(
                "completed", records.countByStatus(TaskStatus.COMPLETED),
                "dead", records.countByStatus(TaskStatus.DEAD)));
        return out;
    }
}
