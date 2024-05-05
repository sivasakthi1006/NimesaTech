package com.nimesa.careers.multithreading_assignment;

public class TaskRequest {
    private String id;
    private String submittedBy;
    private int priority;
    private TaskType type;
    private int size;

    public TaskRequest(String id, String submittedBy, int priority, TaskType type, int size) {
        this.id = id;
        this.submittedBy = submittedBy;
        this.priority = priority;
        this.type = type;
        this.size = size;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSubmittedBy() {
        return submittedBy;
    }

    public void setSubmittedBy(String submittedBy) {
        this.submittedBy = submittedBy;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public TaskType getType() {
        return type;
    }

    public void setType(TaskType type) {
        this.type = type;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
