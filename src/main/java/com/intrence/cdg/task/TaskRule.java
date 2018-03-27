package com.intrence.cdg.task;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.util.ISO8601DateFormat;
import com.intrence.models.model.SearchParams;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize(builder = TaskRule.Builder.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskRule {

    public static final String CLIENT_ID = "client_id";
    public static final String CREATOR_ID = "creator_id";
    public static final String TASK_BUCKET = "task_bucket";
    public static final String COUNTRY = "country";
    public static final String AUTO_START = "auto_start";
    public static final String TYPE = "type";

    public static final String ID = "id";
    public static final String LAST_UPDATED_BY = "last_updated_by";
    public static final String MAX_RECORDS = "max_records";
    public static final String SOURCE = "source";
    public static final String STATUS = "status";
    public static final String SEARCH_PARAMS = "search_params";
    public static final String RECURRENCE = "recurrence";
    public static final String NEXT_SCHEDULE_AT = "next_schedule_at";
    public static final String CREATED_AT = "created_at";
    public static final String UPDATED_AT = "updated_at";
    private static final int DEFAULT_MAX_RECORDS = 10;

    private final Integer id;
    private final String type;
    private final Status status;
    private final String source;
    private final String taskBucket;
    private final String country;
    private final Boolean autoStart;
    private final String last_updated_by;
    private final String creator_id;
    private final String client_id;
    private final Integer max_records;
    private final SearchParams searchParams;
    private final RuleRecurrence recurrence;
    private final Timestamp nextScheduleAt;
    private final Timestamp createdAt;
    private final Timestamp updatedAt;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false).setDateFormat(new ISO8601DateFormat());
    private static final Logger LOGGER = Logger.getLogger(TaskRule.class);

    public TaskRule(Builder builder) {

        if (StringUtils.isBlank(builder.source) || StringUtils.isBlank(builder.type)) {
            throw new IllegalArgumentException("Fields source and type are required by TaskRule, can't be null!");
        }
        this.id = builder.id;
        this.type = builder.type;
        this.source = builder.source;
        this.taskBucket = builder.taskBucket;
        this.country = builder.country;
        this.autoStart = builder.autoStart == null ? false : builder.autoStart;
        this.last_updated_by = builder.last_updated_by;
        this.creator_id = builder.creator_id;
        this.client_id = builder.client_id;
        this.max_records = computeMaxRecords(builder.max_records);
        this.status = builder.status == null ? Status.DRAFT : builder.status;
        this.searchParams = builder.searchParams;
        this.recurrence = builder.recurrence;
        this.nextScheduleAt = builder.nextScheduleAt;
        this.createdAt = builder.createdAt;
        this.updatedAt = builder.updatedAt;
    }
    
    private int computeMaxRecords(Integer builderMaxRecords) {
        if (builderMaxRecords == null) {
            return DEFAULT_MAX_RECORDS;
        } else if (builderMaxRecords < 0) {
            return Integer.MAX_VALUE;
        } else {
            return builderMaxRecords;
        }
    }

    @JsonProperty(ID)
    public Integer getId() {
        return id;
    }

    @JsonProperty(TASK_BUCKET)
    public String getTaskBucket() {
        return this.taskBucket;
    }

    @JsonProperty(AUTO_START)
    public Boolean getAutoStart() {
        return this.autoStart != null ? this.autoStart : false;
    }

    @JsonProperty(COUNTRY)
    public String getCountry() {
        return this.country;
    }

    @JsonProperty(SOURCE)
    public String getSource() {
        return this.source;
    }

    @JsonProperty(LAST_UPDATED_BY)
    public String getLastUpdatedBy() {
        return this.last_updated_by;
    }

    @JsonProperty(CREATOR_ID)
    public String getCreatorId() {
        return this.creator_id;
    }

    @JsonProperty(CLIENT_ID)
    public String getClientId() {
        return this.client_id;
    }

    @JsonProperty(MAX_RECORDS)
    public Integer getMaxRecords() {
        return this.max_records;
    }

    @JsonProperty(STATUS)
    public Status getStatus() {
        return status;
    }

    @JsonProperty(TYPE)
    public String getType() {
        return this.type;
    }

    @JsonProperty(SEARCH_PARAMS)
    public SearchParams getSearchParams() {
        return searchParams;
    }

    @JsonProperty(CREATED_AT)
    public Timestamp getCreatedAt() {
        return createdAt;
    }

    @JsonProperty(UPDATED_AT)
    public Timestamp getUpdatedAt() {
        return updatedAt;
    }

    @JsonProperty(RECURRENCE)
    public RuleRecurrence getRecurrence() {
        return recurrence;
    }

    @JsonProperty(NEXT_SCHEDULE_AT)
    public Timestamp getNextScheduleAt() {
        return nextScheduleAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TaskRule that = (TaskRule) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (status != null ? !status.equals(that.status) : that.status != null) return false;
        if (source != null ? !source.equals(that.source) : that.source!= null) return false;
        if (max_records != null ? !max_records.equals(that.max_records) : that.max_records != null) return false;
        if (searchParams != null ? !searchParams.equals(that.searchParams) : that.searchParams != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        if (createdAt != null ? !createdAt.equals(that.createdAt) : that.createdAt != null) return false;
        if (updatedAt != null ? !updatedAt.equals(that.updatedAt) : that.updatedAt != null) return false;
        if (recurrence != null ? !recurrence.equals(that.recurrence) : that.recurrence != null) return false;
        if (nextScheduleAt != null ? !nextScheduleAt.equals(that.nextScheduleAt) : that.nextScheduleAt != null) return false;
        if (creator_id != null ? !creator_id.equals(that.creator_id) : that.creator_id != null) return false;
        if (client_id != null ? !client_id.equals(that.client_id) : that.client_id != null) return false;
        if (taskBucket != null ? !taskBucket.equals(that.taskBucket) : that.taskBucket != null) return false;
        if (country != null ? !country.equals(that.country) : that.country != null) return false;
        if (autoStart != null ? !autoStart.equals(that.autoStart) : that.autoStart != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (source!=null ? source.hashCode() : 0);
        result = 31 * result + (max_records != null ? max_records.hashCode() : 0);
        result = 31 * result + (status != null ? status.hashCode() : 0);
        result = 31 * result + (searchParams != null ? searchParams.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (nextScheduleAt != null ? nextScheduleAt.hashCode() : 0);
        result = 31 * result + (createdAt != null ? createdAt.hashCode() : 0);
        result = 31 * result + (updatedAt != null ? updatedAt.hashCode() : 0);
        result = 31 * result + (recurrence != null ? recurrence.hashCode() : 0);
        result = 31 * result + (source != null ? source.hashCode() : 0);
        result = 31 * result + (creator_id != null ? creator_id.hashCode() : 0);
        result = 31 * result + (client_id != null ? client_id.hashCode() : 0);
        result = 31 * result + (taskBucket != null ? taskBucket.hashCode() : 0);
        result = 31 * result + (country != null ? country.hashCode() : 0);
        result = 31 * result + (autoStart != null ? autoStart.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return String.format("{ id: %s, last_updated_by: %s, source: %s, type: %s, max_records: %s, status: %s, searchParams:%s, recurrence: %s, next_schedule_at: %s, "
                + "created_at: %s, updated_at: %s }", id, last_updated_by, source, type, max_records, status, searchParams, recurrence, nextScheduleAt, createdAt, updatedAt);
    }

    public boolean equals_client_metadata(Object o) {

        if(this==o) return true;
        if(o == null || getClass() != o.getClass()) return false;

        TaskRule that = (TaskRule) o;
        if (max_records != null ? !max_records.equals(that.max_records) : that.max_records != null) return false;
        if (searchParams != null ? !searchParams.equals(that.searchParams) : that.searchParams != null) return false;
        if (autoStart != null ? !autoStart.equals(that.autoStart) : that.autoStart != null) return false;
        return true;
    }

    public Timestamp getNextSchedule(long current) {
        if (recurrence == null) {
            return null;
        } else if(recurrence.useCronJob()){
            return recurrence.getCronExpression().nextValidTime();
        }

        long next ;
        switch (recurrence.getUnit()) {
            case days:
                next = current + (recurrence.getFrequency() * 24 * 60 * 60 * 1000);
                break;
            case hours:
                next = current + (recurrence.getFrequency() * 60 * 60 * 1000);
                break;
            case minutes:
                next = current + (recurrence.getFrequency() * 60 * 1000);
                break;
            default:
                return null;
        }

        return new Timestamp(next);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Builder {
        private Integer id;
        private String type;
        private String source;
        private String last_updated_by;
        private String creator_id;
        private String client_id;
        private String taskBucket;
        private String country;
        private Boolean autoStart;
        private Integer max_records;
        private Status status;
        private SearchParams searchParams;
        private RuleRecurrence recurrence;
        private Timestamp nextScheduleAt;
        private Timestamp createdAt;
        private Timestamp updatedAt;

        public Builder() {
        }

        public Builder(TaskRule rule) {
            this.type = rule.type;
            this.id = rule.id;
            this.source = rule.source;
            this.taskBucket = rule.taskBucket;
            this.country = rule.country;
            this.autoStart = rule.autoStart;
            this.last_updated_by = rule.last_updated_by;
            this.creator_id = rule.creator_id;
            this.client_id = rule.client_id;
            this.max_records = rule.max_records;
            this.nextScheduleAt = rule.nextScheduleAt;
            this.recurrence = rule.recurrence;
            this.createdAt = rule.createdAt;
            this.updatedAt = rule.updatedAt;
            this.searchParams = rule.searchParams;
            this.status = rule.status;
        }

        @JsonSetter(ID)
        public Builder id(Integer id) {
            this.id = id;
            return this;
        }

        @JsonSetter(TYPE)
        public Builder type(String type) {
            this.type = type;
            return this;
        }

        @JsonSetter(SOURCE)
        public Builder source(String source) {
            this.source = source;
            return this;
        }

        @JsonSetter(TASK_BUCKET)
        public Builder taskBucket(String taskBucket) {
            this.taskBucket = taskBucket;
            return this;
        }

        @JsonSetter(COUNTRY)
        public Builder country(String country) {
            this.country = country;
            return this;
        }

        @JsonSetter(AUTO_START)
        public Builder autoStart(Boolean autoStart) {
            this.autoStart = autoStart;
            return this;
        }

        @JsonSetter(LAST_UPDATED_BY)
        public Builder lastUpdatedBy(String last_updated_by) {
            this.last_updated_by = last_updated_by;
            return this;
        }

        @JsonSetter(CREATOR_ID)
        public Builder creatorId(String creator_id) {
            this.creator_id = creator_id;
            return this;
        }

        @JsonSetter(CLIENT_ID)
        public Builder clientId(String client_id) {
            this.client_id = client_id;
            return this;
        }

        @JsonSetter(MAX_RECORDS)
        public Builder maxRecords(Integer max_records) {
            this.max_records = max_records;
            return this;
        }

        public Builder status(Status status) {
            this.status = status;
            return this;
        }

        @JsonSetter(SEARCH_PARAMS)
        public Builder searchParamsMap(SearchParams searchParams) {
            this.searchParams = searchParams;
            return this;
        }

        @JsonSetter(RECURRENCE)
        public Builder recurrence(RuleRecurrence recurrence) {
            this.recurrence = recurrence;
            return this;
        }

        @JsonSetter(NEXT_SCHEDULE_AT)
        public Builder nextScheduleAt(Timestamp nextScheduleAt) {
            this.nextScheduleAt = nextScheduleAt;
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

        public TaskRule build() {
            return new TaskRule(this);
        }
    }

    /**
     * Task Status
     */
    public enum Status {
        DRAFT, RUNNABLE, RESUME, RUNNING, PAUSED, CANCELLED, FINISHED, ERROR;

        public static Status fromString(String str) {
            for (Status status : Status.values()) {
                if (status.toString().equalsIgnoreCase(str)) {
                    return status;
                }
            }
            throw new IllegalArgumentException(String.format("Status string %s is not valid", str));
        }
    }

    /*
        TaskRule Serialization-deserialization methods
     */
    public static TaskRule fromJson(String json) throws IllegalArgumentException, IOException {
        try {
            return OBJECT_MAPPER.readValue(json, TaskRule.class);
        } catch (IOException e) {
            LOGGER.error("Could not deserialize Json string to TaskRule.", e);
            throw new IllegalArgumentException("Could not deserialize Json string to TaskRule. " + e.getMessage());
        }
    }

    public String toJson() throws IllegalArgumentException {
        try {
            return OBJECT_MAPPER.writeValueAsString(this);
        } catch (IOException e) {
            LOGGER.error("Could not serialize TaskRule to Json string. ", e);
            throw new IllegalArgumentException("Could not serialize TaskRule to Json string. " + e.getMessage());
        }
    }

    public static String toJsonArray(List<TaskRule> rules) throws IllegalArgumentException {
        try {
            return OBJECT_MAPPER.writeValueAsString(rules);
        } catch (IOException e) {
            LOGGER.error("Could not serialize TaskRules to Json string. ", e);
            throw new IllegalArgumentException("Could not serialize TaskRules to Json string."+ e.getMessage());
        }
    }
}