package com.saiswayam.orchestrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.saiswayam.orchestrator.model.Task;
import com.saiswayam.orchestrator.model.TaskPriority;
import com.saiswayam.orchestrator.queue.RedisTaskQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.JedisPooled;

import java.time.Instant;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests against a real Redis (Testcontainers).
 * Verifies the core delivery guarantees of the queue.
 */
@Testcontainers
class RedisTaskQueueIntegrationTest {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    RedisTaskQueue queue;
    JedisPooled jedis;

    @BeforeEach
    void setUp() {
        jedis = new JedisPooled(redis.getHost(), redis.getMappedPort(6379));
        jedis.flushAll();
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        mapper.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        queue = new RedisTaskQueue(jedis, mapper);
    }

    @Test
    void enqueueThenDequeueReturnsSameTask() {
        Task t = Task.of("echo", "hello", TaskPriority.MEDIUM, 3);
        queue.enqueue(t);

        Optional<Task> got = queue.dequeue();
        assertTrue(got.isPresent());
        assertEquals(t.getId(), got.get().getId());
        assertEquals(1, queue.inflightCount());
        assertEquals(0, queue.pendingCount());
    }

    @Test
    void higherPriorityDequeuedFirst() {
        queue.enqueue(Task.of("echo", "low", TaskPriority.LOW, 3));
        Task high = Task.of("echo", "high", TaskPriority.HIGH, 3);
        queue.enqueue(high);

        assertEquals(high.getId(), queue.dequeue().orElseThrow().getId());
    }

    @Test
    void delayedTaskNotVisibleUntilEligible() {
        Task t = Task.of("echo", "later", TaskPriority.HIGH, 3);
        t.setNotBefore(Instant.now().plusSeconds(60));
        queue.enqueue(t);

        assertTrue(queue.dequeue().isEmpty(), "delayed task must not be dequeued early");
        assertEquals(1, queue.pendingCount());
    }

    @Test
    void ackRemovesTaskCompletely() {
        Task t = Task.of("echo", "x", TaskPriority.MEDIUM, 3);
        queue.enqueue(t);
        Task got = queue.dequeue().orElseThrow();
        queue.ack(got);

        assertEquals(0, queue.inflightCount());
        assertEquals(0, queue.pendingCount());
    }

    @Test
    void noDuplicateDeliveryUnderConcurrentDequeue() throws Exception {
        int taskCount = 200;
        for (int i = 0; i < taskCount; i++) {
            queue.enqueue(Task.of("echo", "t" + i, TaskPriority.MEDIUM, 3));
        }

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        Set<String> seen = ConcurrentHashMap.newKeySet();
        CountDownLatch done = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    while (true) {
                        Optional<Task> t = queue.dequeue();
                        if (t.isEmpty()) break;
                        // add() returns false if the id was already seen -> duplicate delivery
                        assertTrue(seen.add(t.get().getId()), "duplicate delivery detected!");
                    }
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(done.await(30, TimeUnit.SECONDS));
        pool.shutdown();

        assertEquals(taskCount, seen.size(), "every task delivered exactly once under contention");
    }

    @Test
    void crashedWorkerTaskIsRecoveredByReaper() {
        Task t = Task.of("echo", "will-be-abandoned", TaskPriority.MEDIUM, 3);
        queue.enqueue(t);
        queue.dequeue().orElseThrow(); // worker "takes" the task and then "crashes" (never acks)

        // Simulate visibility timeout expiry by rewinding the inflight score
        jedis.zadd("queue:inflight", Instant.now().minusSeconds(60).toEpochMilli(), t.getId());

        int recovered = queue.requeueExpiredInflight();
        assertEquals(1, recovered);
        assertEquals(1, queue.pendingCount(), "abandoned task must be visible again");

        // And it can be dequeued again -> at-least-once delivery
        assertEquals(t.getId(), queue.dequeue().orElseThrow().getId());
    }

    @Test
    void exhaustedTaskGoesToDeadLetterQueue() {
        Task t = Task.of("doomed", "x", TaskPriority.MEDIUM, 1);
        queue.enqueue(t);
        Task got = queue.dequeue().orElseThrow();
        got.recordFailure("boom", Instant.now());
        assertTrue(got.retriesExhausted());

        queue.moveToDeadLetter(got);
        assertEquals(1, queue.deadLetterCount());
        assertEquals(0, queue.inflightCount());
    }
}
