package com.saiswayam.orchestrator.worker;

import com.saiswayam.orchestrator.model.Task;

/** A handler for one logical task type. Register implementations as Spring beans. */
public interface TaskHandler {
    /** The task type this handler serves, e.g. "send_email". */
    String type();

    /** Execute the task. Throw any exception to signal failure (will be retried). */
    void handle(Task task) throws Exception;
}
