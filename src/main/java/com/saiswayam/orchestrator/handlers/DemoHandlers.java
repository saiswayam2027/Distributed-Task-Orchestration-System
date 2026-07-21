package com.saiswayam.orchestrator.handlers;

import com.saiswayam.orchestrator.model.Task;
import com.saiswayam.orchestrator.worker.TaskHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Demo task handlers to exercise the system.
 * In a real deployment you'd register handlers for actual work
 * (send email, generate report, call external API, etc).
 */
@Configuration
public class DemoHandlers {

    private static final Logger log = LoggerFactory.getLogger(DemoHandlers.class);

    /** Simulates work by sleeping 0.5-2s. Always succeeds. */
    @Bean
    TaskHandler sleepHandler() {
        return new TaskHandler() {
            @Override public String type() { return "sleep"; }
            @Override public void handle(Task task) throws Exception {
                Thread.sleep(ThreadLocalRandom.current().nextLong(500, 2000));
            }
        };
    }

    /** Fails ~50% of the time — demonstrates retry with backoff and DLQ. */
    @Bean
    TaskHandler flakyHandler() {
        return new TaskHandler() {
            @Override public String type() { return "flaky"; }
            @Override public void handle(Task task) throws Exception {
                Thread.sleep(300);
                if (ThreadLocalRandom.current().nextBoolean()) {
                    throw new RuntimeException("Simulated transient failure");
                }
            }
        };
    }

    /** Always fails — every task of this type ends in the dead-letter queue. */
    @Bean
    TaskHandler doomedHandler() {
        return new TaskHandler() {
            @Override public String type() { return "doomed"; }
            @Override public void handle(Task task) {
                throw new IllegalStateException("This task type always fails");
            }
        };
    }

    /** Logs its payload. Useful as a smoke test. */
    @Bean
    TaskHandler echoHandler() {
        return new TaskHandler() {
            @Override public String type() { return "echo"; }
            @Override public void handle(Task task) {
                log.info("ECHO: {}", task.getPayload());
            }
        };
    }
}
