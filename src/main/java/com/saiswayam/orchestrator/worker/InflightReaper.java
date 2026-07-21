package com.saiswayam.orchestrator.worker;

import com.saiswayam.orchestrator.queue.RedisTaskQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodically returns abandoned inflight tasks (crashed/slow workers) to the
 * pending queue. Combined with the visibility timeout in RedisTaskQueue, this
 * gives the system its at-least-once delivery guarantee.
 */
@Component
public class InflightReaper {

    private static final Logger log = LoggerFactory.getLogger(InflightReaper.class);

    private final RedisTaskQueue queue;

    public InflightReaper(RedisTaskQueue queue) { this.queue = queue; }

    @Scheduled(fixedDelayString = "${orchestrator.reaper-interval-ms:10000}")
    public void reap() {
        int recovered = queue.requeueExpiredInflight();
        if (recovered > 0) {
            log.warn("Reaper recovered {} abandoned task(s)", recovered);
        }
    }
}
