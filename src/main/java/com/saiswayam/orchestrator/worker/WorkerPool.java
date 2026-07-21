package com.saiswayam.orchestrator.worker;

import com.saiswayam.orchestrator.model.Task;
import com.saiswayam.orchestrator.model.TaskStatus;
import com.saiswayam.orchestrator.persistence.TaskAuditService;
import com.saiswayam.orchestrator.queue.RedisTaskQueue;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Pull-based worker pool.
 *
 * Each worker thread loops: dequeue -> execute -> ack / nack.
 * Pull (rather than push) means backpressure is automatic: a busy pool
 * simply stops pulling, and tasks wait safely in Redis.
 *
 * Concurrency notes (interview gold):
 *  - `running` is an AtomicBoolean, not a plain boolean: worker threads and the
 *    shutdown hook read/write it from different threads, so we need visibility
 *    guarantees (a plain boolean could be cached in a register/core forever).
 *  - Per-worker heartbeats are written to a ConcurrentHashMap; the monitoring
 *    endpoint reads them without locking.
 *  - Graceful shutdown: stop pulling, then await in-progress tasks with a
 *    deadline. Anything not finished will re-appear via the visibility-timeout
 *    reaper — this is why crash-safety and graceful shutdown share one mechanism.
 */
@Component
public class WorkerPool {

    private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);

    private final RedisTaskQueue queue;
    private final TaskHandlerRegistry registry;
    private final TaskAuditService audit;
    private final RetryPolicy retryPolicy = RetryPolicy.defaults();

    private final int workerCount;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Map<String, Instant> heartbeats = new ConcurrentHashMap<>();
    private final AtomicLong completedCount = new AtomicLong();
    private final AtomicLong failedCount = new AtomicLong();

    private ExecutorService executor;

    public WorkerPool(RedisTaskQueue queue,
                      TaskHandlerRegistry registry,
                      TaskAuditService audit,
                      @Value("${orchestrator.workers:4}") int workerCount) {
        this.queue = queue;
        this.registry = registry;
        this.audit = audit;
        this.workerCount = workerCount;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (!running.compareAndSet(false, true)) return;
        executor = Executors.newFixedThreadPool(workerCount, r -> {
            Thread t = new Thread(r);
            t.setDaemon(false);
            return t;
        });
        for (int i = 0; i < workerCount; i++) {
            final String workerId = "worker-" + i;
            executor.submit(() -> workerLoop(workerId));
        }
        log.info("Worker pool started with {} workers", workerCount);
    }

    private void workerLoop(String workerId) {
        Thread.currentThread().setName(workerId);
        while (running.get()) {
            heartbeats.put(workerId, Instant.now());
            Optional<Task> maybeTask;
            try {
                maybeTask = queue.dequeue();
            } catch (Exception e) {
                log.error("[{}] dequeue failed (Redis down?); backing off", workerId, e);
                sleepQuietly(1000);
                continue;
            }
            if (maybeTask.isEmpty()) {
                sleepQuietly(200); // idle poll interval
                continue;
            }
            process(workerId, maybeTask.get());
        }
        heartbeats.remove(workerId);
        log.info("[{}] stopped", workerId);
    }

    private void process(String workerId, Task task) {
        audit.markRunning(task, workerId);
        try {
            registry.get(task.getType()).handle(task);
            queue.ack(task);
            audit.markStatus(task, TaskStatus.COMPLETED, null);
            completedCount.incrementAndGet();
            log.info("[{}] completed task {} (type={}, attempt={})",
                    workerId, task.getId(), task.getType(), task.getAttempt());
        } catch (Exception e) {
            failedCount.incrementAndGet();
            handleFailure(workerId, task, e);
        }
    }

    private void handleFailure(String workerId, Task task, Exception e) {
        String error = e.getClass().getSimpleName() + ": " + e.getMessage();
        if (task.retriesExhausted()) {
            queue.moveToDeadLetter(task);
            audit.markStatus(task, TaskStatus.DEAD, error);
            log.error("[{}] task {} DEAD after {} attempts", workerId, task.getId(), task.getAttempt() + 1);
        } else {
            Instant next = retryPolicy.nextEligibleAt(task.getAttempt());
            task.recordFailure(error, next);
            queue.nackRetry(task);
            audit.markStatus(task, TaskStatus.FAILED, error);
            log.warn("[{}] task {} failed (attempt {}), retrying at {}: {}",
                    workerId, task.getId(), task.getAttempt(), next, error);
        }
    }

    @PreDestroy
    public void shutdown() {
        if (!running.compareAndSet(true, false)) return;
        log.info("Shutting down worker pool gracefully...");
        executor.shutdown();
        try {
            if (!executor.awaitTermination(20, TimeUnit.SECONDS)) {
                log.warn("Workers did not finish in time; forcing shutdown. "
                        + "In-flight tasks will be recovered by the visibility-timeout reaper.");
                executor.shutdownNow();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }

    private static void sleepQuietly(long millis) {
        try { Thread.sleep(millis); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    // --- monitoring accessors ---
    public Map<String, Instant> heartbeats() { return Map.copyOf(heartbeats); }
    public long completed() { return completedCount.get(); }
    public long failed() { return failedCount.get(); }
    public boolean isRunning() { return running.get(); }
}
