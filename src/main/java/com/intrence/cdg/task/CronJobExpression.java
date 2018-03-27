/**
 *
 */
package com.intrence.cdg.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.log4j.Logger;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.Trigger;

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * @author smankala
 *
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class CronJobExpression {

    private static final String EXPRESSION = "expression";
    private static final String TIME_ZONE = "time_zone";

    private static final Logger LOGGER = Logger.getLogger(CronJobExpression.class);

    private String expression;
    private String timeZone;
    private CronExpression quartzCronExpression;
    private Trigger trigger;

    @JsonCreator
    public CronJobExpression(@JsonProperty(EXPRESSION) String expression, @JsonProperty(TIME_ZONE) String timeZone) {
        this.expression = expression;
        try {
            this.quartzCronExpression = new CronExpression(expression);
            this.timeZone = timeZone;
            if (timeZone != null && quartzCronExpression != null) {
                trigger = CronScheduleBuilder.cronSchedule(quartzCronExpression)
                        .inTimeZone(TimeZone.getTimeZone(timeZone)).build();
            } else if (quartzCronExpression != null) {
                trigger = CronScheduleBuilder.cronSchedule(quartzCronExpression).inTimeZone(TimeZone.getDefault())
                        .build();
                this.timeZone = TimeZone.getDefault().getID();
            }
        } catch (ParseException e) {
            LOGGER.error("Invalid cron expression.", e);
            throw new IllegalArgumentException("Invalid cron expression." + e.getMessage());
        }
    }

    @JsonProperty(EXPRESSION)
    public String getExpression() {
        return expression;
    }

    @JsonProperty(TIME_ZONE)
    public String getTimeZone() {
        return timeZone;
    }

    public Timestamp nextValidTime() {
        Calendar c = new GregorianCalendar(TimeZone.getTimeZone(timeZone));
        return new Timestamp(trigger.getFireTimeAfter(c.getTime()).getTime());

    }

}
