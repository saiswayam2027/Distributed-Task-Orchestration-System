package com.saiswayam.orchestrator.worker;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Maps task type -> handler. Fails fast at startup on duplicate types. */
@Component
public class TaskHandlerRegistry {

    private final Map<String, TaskHandler> handlers;

    public TaskHandlerRegistry(List<TaskHandler> discovered) {
        this.handlers = discovered.stream()
                .collect(Collectors.toUnmodifiableMap(TaskHandler::type, Function.identity()));
    }

    public TaskHandler get(String type) {
        TaskHandler h = handlers.get(type);
        if (h == null) throw new IllegalArgumentException("No handler registered for task type: " + type);
        return h;
    }

    public boolean supports(String type) { return handlers.containsKey(type); }
}
