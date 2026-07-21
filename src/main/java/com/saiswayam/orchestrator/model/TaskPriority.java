package com.saiswayam.orchestrator.model;

public enum TaskPriority {
    HIGH(0), MEDIUM(1), LOW(2);

    private final int level;

    TaskPriority(int level) { this.level = level; }

    public int level() { return level; }
}
