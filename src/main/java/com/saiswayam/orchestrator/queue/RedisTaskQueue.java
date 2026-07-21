package com.saiswayam.orchestrator.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.saiswayam.orchestrator.model.Task;
import com.saiswayam.orchestrator.model.TaskPriority;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.ZAddParams;
import redis.clients.jedis.resps.Tuple;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Distributed queue with AT-LEAST-ONCE delivery, implemented on Redis sorted sets.
 *
 * Design (SQS-style visibility timeout pattern):
 *
 *  - queue:pending:{priority}  -> ZSET scored by "notBefore" epoch millis.
 *                                 A task is eligible for dequeue only when its score <= now.
 *                                 Delayed tasks and backoff-retried tasks just get a future score.
 *
 *  - queue:inflight            -> ZSET scored by "visibility deadline" (now + visibilityTimeout).
 *                                 When a worker dequeues a task, it is atomically MOVED here.
 *                                 If the worker crashes and never ACKs, the reaper finds the
 *                                 expired entry and moves it back to pending. This is what makes
 *                                 delivery at-least-once instead of at-most-once.
 *
 *  - queue:dead                -> LIST holding tasks whose retries are exhausted (dead-letter queue).
 *
 *  - task:{id}                 -> JSON body of the task (single source of truth for the envelope).
 *
 * Atomicity: dequeue uses a Lua script so that "pop from pending + push to inflight"
 * is a single atomic step. Two workers can never receive the same task simultaneously.
 */
@Component
public class RedisTaskQueue {

    private static final Logger log = LoggerFactory.getLogger(RedisTaskQueue.class);

    static final String PENDING_PREFIX = "queue:pending:";
    static final String INFLIGHT_KEY = "queue:inflight";
    static final String DEAD_KEY = "queue:dead";
    static final String TASK_PREFIX = "task:";

    /**
     * KEYS[1] = pending zset, KEYS[2] = inflight zset
     * ARGV[1] = now millis, ARGV[2] = visibility deadline millis
     * Pops the lowest-scored eligible task id and moves it to inflight atomically.
     */
    private static final String DEQUEUE_LUA = """
            local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 1)
            if #ids == 0 then return nil end
            local id = ids[1]
            redis.call('ZREM', KEYS[1], id)
            redis.call('ZADD', KEYS[2], ARGV[2], id)
            return id
            """;

    private final JedisPooled jedis;
    private final ObjectMapper mapper;
    private final long visibilityTimeoutMillis;

    public RedisTaskQueue(JedisPooled jedis, ObjectMapper mapper) {
        this.jedis = jedis;
        this.mapper = mapper;
        this.visibilityTimeoutMillis = 30_000; // 30s to process before re-delivery
    }

    /** Enqueue a task. If notBefore is in the future, it acts as a delayed task. */
    public void enqueue(Task task) {
        try {
            String json = mapper.writeValueAsString(task);
            jedis.set(TASK_PREFIX + task.getId(), json);
            jedis.zadd(pendingKey(task.getPriority()),
                    task.getNotBefore().toEpochMilli(),
                    task.getId(),
                    ZAddParams.zAddParams());
            log.debug("Enqueued task {} type={} priority={}", task.getId(), task.getType(), task.getPriority());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to enqueue task " + task.getId(), e);
        }
    }

    /**
     * Try to dequeue one eligible task, scanning priorities HIGH -> LOW.
     * Returns empty if no task is currently eligible.
     */
    public Optional<Task> dequeue() {
        long now = Instant.now().toEpochMilli();
        long deadline = now + visibilityTimeoutMillis;
        for (TaskPriority p : TaskPriority.values()) {
            Object result = jedis.eval(DEQUEUE_LUA,
                    List.of(pendingKey(p), INFLIGHT_KEY),
                    List.of(String.valueOf(now), String.valueOf(deadline)));
            if (result != null) {
                String id = result.toString();
                String json = jedis.get(TASK_PREFIX + id);
                if (json == null) {
                    // Body vanished (shouldn't happen); drop the orphan inflight entry.
                    jedis.zrem(INFLIGHT_KEY, id);
                    continue;
                }
                try {
                    return Optional.of(mapper.readValue(json, Task.class));
                } catch (Exception e) {
                    log.error("Corrupt task body for {}; sending to DLQ", id, e);
                    jedis.zrem(INFLIGHT_KEY, id);
                    jedis.rpush(DEAD_KEY, json);
                }
            }
        }
        return Optional.empty();
    }

    /** Worker finished successfully: remove from inflight and delete the body. */
    public void ack(Task task) {
        jedis.zrem(INFLIGHT_KEY, task.getId());
        jedis.del(TASK_PREFIX + task.getId());
    }

    /** Worker failed but retries remain: update body, move back to pending with backoff. */
    public void nackRetry(Task task) {
        try {
            jedis.set(TASK_PREFIX + task.getId(), mapper.writeValueAsString(task));
            jedis.zrem(INFLIGHT_KEY, task.getId());
            jedis.zadd(pendingKey(task.getPriority()), task.getNotBefore().toEpochMilli(), task.getId());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to nack task " + task.getId(), e);
        }
    }

    /** Retries exhausted: move to dead-letter queue for manual inspection. */
    public void moveToDeadLetter(Task task) {
        try {
            jedis.zrem(INFLIGHT_KEY, task.getId());
            jedis.rpush(DEAD_KEY, mapper.writeValueAsString(task));
            jedis.del(TASK_PREFIX + task.getId());
            log.warn("Task {} moved to dead-letter queue after {} attempts. Last error: {}",
                    task.getId(), task.getAttempt(), task.getLastError());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to dead-letter task " + task.getId(), e);
        }
    }

    /**
     * Reaper: find inflight tasks whose visibility deadline has passed (worker crashed
     * or is too slow) and make them visible again. Called on a schedule.
     * Returns how many tasks were recovered.
     */
    public int requeueExpiredInflight() {
        long now = Instant.now().toEpochMilli();
        List<Tuple> expired = jedis.zrangeByScoreWithScores(INFLIGHT_KEY, 0, now);
        int recovered = 0;
        for (Tuple t : expired) {
            String id = t.getElement();
            String json = jedis.get(TASK_PREFIX + id);
            if (jedis.zrem(INFLIGHT_KEY, id) == 0) continue; // another reaper instance won the race
            if (json == null) continue;
            try {
                Task task = mapper.readValue(json, Task.class);
                jedis.zadd(pendingKey(task.getPriority()), now, id);
                recovered++;
                log.warn("Recovered abandoned task {} (worker likely crashed)", id);
            } catch (Exception e) {
                jedis.rpush(DEAD_KEY, json);
            }
        }
        return recovered;
    }

    public long pendingCount() {
        long total = 0;
        for (TaskPriority p : TaskPriority.values()) total += jedis.zcard(pendingKey(p));
        return total;
    }

    public long inflightCount() { return jedis.zcard(INFLIGHT_KEY); }

    public long deadLetterCount() { return jedis.llen(DEAD_KEY); }

    private String pendingKey(TaskPriority p) {
        return PENDING_PREFIX + p.name().toLowerCase();
    }
}
