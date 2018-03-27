package com.intrence.cdg.task;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.sql.Timestamp;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize(builder = TaskBatchRequest.Builder.class)
public class TaskBatchRequest {
    public static final String ID = "id";
    public static final String SOURCE = "source";
    public static final String STATUS = "status";
    public static final String CREATED_AT = "created_at";
    public static final String UPDATED_AT = "updated_at";
    public static final String FETCH_REQUEST = "fetch_request";

    private final Integer id;
    private final String status;
    private final Timestamp createdAt;
    private final Timestamp updatedAt;
    private final String fetchRequestJsonString;
    private final String source;

    public TaskBatchRequest(final Builder builder) {
        this.id = builder.id;
        this.status = builder.status;
        this.createdAt = builder.createdAt;
        this.updatedAt = builder.updatedAt;
        this.fetchRequestJsonString = builder.fetchRequestJsonString;
        this.source = builder.source;
    }

    @JsonProperty(UPDATED_AT)
    public Timestamp getUpdatedAt() {
        return this.updatedAt;
    }

    @JsonProperty(CREATED_AT)
    public Timestamp getCreatorAt() {
        return this.createdAt;
    }

    @JsonProperty(ID)
    public int getId() {
        return this.id;
    }

    @JsonProperty(STATUS)
    public String getStatus() {
        return this.status;
    }

    @JsonProperty(SOURCE)
    public String getSource() {
        return this.source;
    }

    @JsonProperty(FETCH_REQUEST)
    public String getFetchRequestJsonString() {
        return this.fetchRequestJsonString;
    }

    public enum Status {
        DRAFT,
        FINISHED,
        ERROR;

        public static Status fromString(String str) {
            for (Status status : Status.values()) {
                if (status.toString().equalsIgnoreCase(str)) {
                    return status;
                }
            }
            throw new IllegalArgumentException(String.format("Status string %s is not valid", str));
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Builder {

        private int id;
        private String status;
        private Timestamp createdAt;
        private Timestamp updatedAt;
        private String fetchRequestJsonString;
        private String source;

        public Builder() {
        }

        public Builder(TaskBatchRequest taskBatchRequest) {
            this.id = taskBatchRequest.id;
            this.status = taskBatchRequest.status;
            this.createdAt = taskBatchRequest.createdAt;
            this.updatedAt = taskBatchRequest.updatedAt;
            this.fetchRequestJsonString = taskBatchRequest.fetchRequestJsonString;
            this.source = taskBatchRequest.source;
        }

        @JsonSetter(ID)
        public Builder id(int id) {
            this.id = id;
            return this;
        }

        @JsonSetter(SOURCE)
        public Builder source(String source) {
            this.source = source;
            return this;
        }

        @JsonSetter(CREATED_AT)
        public Builder createdAt(Timestamp createdAt) {
            this.createdAt = createdAt;
            return this;
        }

        @JsonSetter(UPDATED_AT)
        public Builder updatedAt(Timestamp updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }

        @JsonSetter(FETCH_REQUEST)
        public Builder fetchRequestJsonString(String fetchRequestJsonString) {
            this.fetchRequestJsonString = fetchRequestJsonString;
            return this;
        }

        @JsonSetter(STATUS)
        public Builder status(String status) {
            this.status = status;
            return this;
        }

        public TaskBatchRequest build() {
            return new TaskBatchRequest(this);
        }

    }
}
