package com.saiswayam.orchestrator.api;

import com.saiswayam.orchestrator.model.TaskPriority;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public class SubmitTaskRequest {

    @NotBlank
    private String type;

    private String payload;

    private TaskPriority priority = TaskPriority.MEDIUM;

    @Min(0) @Max(10)
    private int maxRetries = 3;

    /** Optional delay in seconds before the task becomes eligible. */
    @Min(0)
    private long delaySeconds = 0;

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public TaskPriority getPriority() { return priority; }
    public void setPriority(TaskPriority priority) { this.priority = priority; }
    public int getMaxRetries() { return maxRetries; }
    public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }
    public long getDelaySeconds() { return delaySeconds; }
    public void setDelaySeconds(long delaySeconds) { this.delaySeconds = delaySeconds; }
}
