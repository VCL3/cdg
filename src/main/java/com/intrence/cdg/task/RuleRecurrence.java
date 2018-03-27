package com.intrence.cdg.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RuleRecurrence {
    
    private static final String TIME_UNIT = "time_unit";
    private static final String FREQUENCY = "frequency";
    private static final String CRON_EXPRESSION = "cron_job";
            
    TimeUnit unit;
    int frequency;
    boolean useCronJob;
    CronJobExpression cronJobExpression;
    
    @JsonCreator
    public RuleRecurrence(@JsonProperty(TIME_UNIT) TimeUnit unit, @JsonProperty(FREQUENCY) int frequency,
                          @JsonProperty(CRON_EXPRESSION) CronJobExpression cronJobExpression) {
        this.unit = unit;
        this.frequency = frequency;
        this.cronJobExpression = cronJobExpression;
        if(cronJobExpression != null){
            this.useCronJob = Boolean.TRUE;
        }
    }
    
    @JsonProperty(TIME_UNIT)
    public TimeUnit getUnit() {
        return this.unit;
    }
    
    @JsonProperty(FREQUENCY)
    public int getFrequency() {
        return this.frequency;
    }
    
    public boolean useCronJob() {
        return useCronJob;
    }

    @JsonProperty(CRON_EXPRESSION)
    public CronJobExpression getCronExpression() {
        return cronJobExpression;
    }

    @Override
    public int hashCode() {
        int result = unit != null ? unit.hashCode() : 0;
        result = 31 * result + frequency;        
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        RuleRecurrence that = (RuleRecurrence) o;

        if (this.unit != null ? !this.unit.equals(that.unit) : that.unit != null)
            return false;
        if (this.frequency != that.frequency)
            return false;        

        return true;
    }

    @Override
    public String toString() {
        if (useCronJob) {
            return "{ \"time_unit\": " + getString(unit) + ",\"frequency\": " + frequency
                    + "\"cron_job\": {\"expression\" : " + cronJobExpression.getExpression() + ",\"time_zone\":"
                    + cronJobExpression.getTimeZone() + "}}";
        }
        return "{ \"time_unit\": " + this.unit.toString() + ",\"frequency\": " + this.frequency + "}";
    }
    
    private String getString(TimeUnit unitToString) {
        if(unitToString != null)
            return unitToString.toString();
        return "";
    }

    public static enum TimeUnit {
        days, hours, minutes;
        
        public TimeUnit fromString(String value) {
            for (TimeUnit unit : TimeUnit.values()) {
                if (value.equalsIgnoreCase(unit.toString())) {
                    return unit;
                }
            }
            
            throw new IllegalArgumentException("the value " + value + " for recurrence.timeunit passed is not valid");
        }
    }
    
    
}
