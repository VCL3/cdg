package com.intrence.cdg.task;

import java.sql.Timestamp;

public class TaskRun {
    
    public static final String ID = "id";
    public static final String TASK_RULE_ID = "task_rule_id";
    public static final String STATUS = "status";
    public static final String STATE_SNAPSHOT = "state_snapshot";
    public static final String CREATED_AT = "created_at";
    public static final String UPDATED_AT = "updated_at";
    public static final String COUNT = "count";
    public static final String PUBLISHED_COUNT = "published_count";

    private final int id;
    private final int taskRuleId;
    private final int count;
    private final int published_count;
    private String stateSnapshot;
    private String status;
    private final Timestamp createdAt;
    private final Timestamp updatedAt;


    public TaskRun(Builder builder) {
        this.id = builder.id;
        this.taskRuleId = builder.taskRuleId;
        this.count = builder.count;
        this.published_count = builder.published_count;
        this.stateSnapshot = builder.stateSnapshot;
        this.status = builder.status;
        this.createdAt = builder.createdAt;
        this.updatedAt = builder.updatedAt;
    }

    public int getId() {
        return id;
    }

    public int getTaskRuleId() {
        return taskRuleId;
    }

    public int getCount() {
        return count;
    }

    public int getPublishedCount() {
        return count;
    }

    public String getStateSnapshot() {
        return stateSnapshot;
    }

    public String getStatus() {
        return status;
    }

    public Timestamp getCreatedAt() {
        return createdAt;
    }

    public Timestamp getUpdatedAt() {
        return updatedAt;
    }

    public void setStateSnapshot(String state) {
        this.stateSnapshot = state;
    }

    public static class Builder {
        private int id;
        private int taskRuleId;
        private int count;
        private int published_count;
        private String stateSnapshot;
        private String status;
        private Timestamp createdAt;
        private Timestamp updatedAt;

        public Builder() { }

        public Builder id(int id) {
            this.id = id;
            return this;
        }

        public Builder taskRuleId(int id) {
            this.taskRuleId = id;
            return this;
        }

        public Builder count(int count) {
            this.count = count;
            return this;
        }

        public Builder published_count(int published_count) {
            this.published_count = published_count;
            return this;
        }

        public Builder stateSnapshot(String snapshot) {
            this.stateSnapshot = snapshot;
            return this;
        }

        public Builder status(String status) {
            this.status = status;
            return this;
        }
        
        public Builder createdAt(Timestamp createdAt) {
            this.createdAt = createdAt;
            return this;
        }
        
        public Builder updatedAt(Timestamp updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }
        
        public TaskRun build() {
            return new TaskRun(this);
        }
    }

}
