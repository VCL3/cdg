package com.intrence.cdg.task;

import com.intrence.cdg.SerializableModelTest;
import com.intrence.cdg.util.Constants;
import com.intrence.models.model.SearchParams;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;

public class TaskRuleTest extends SerializableModelTest<TaskRule> {

    @Override
    protected TaskRule getTestInstance() {
        SearchParams factualSearchParams = new SearchParams.Builder()
                .country("US")
                .build();

        return new TaskRule.Builder()
                .id(20)
                .source("factual")
                .lastUpdatedBy("test_client")
                .nextScheduleAt(new Timestamp(System.currentTimeMillis()))
                .type(Constants.REFRESH_OPERATION)
                .autoStart(false)
                .searchParamsMap(factualSearchParams)
                .build();
    }

    @Override
    protected Class<TaskRule> getType() {
        return TaskRule.class;
    }

    @Test (expected = IllegalArgumentException.class)
    public void testBadTaskParamValue() {
        SearchParams factualSearchParams = new SearchParams.Builder()
                .country("DD")
                .build();
        TaskRule rule = new TaskRule.Builder()
                .source("factual")
                .type(Constants.REFRESH_OPERATION)
                .lastUpdatedBy("test_client")
                .searchParamsMap(factualSearchParams)
                .autoStart(false)
            .build();
    }

    @Test
    public void testCorrectTaskParam() {
        SearchParams factualSearchParams = new SearchParams.Builder()
                .country("US")
                .build();
        TaskRule rule = new TaskRule.Builder().source("factual")
                .type(Constants.REFRESH_OPERATION)
                .lastUpdatedBy("test_client")
                .autoStart(false)
                .searchParamsMap(factualSearchParams)
                .build();
        Assert.assertEquals(rule.getSearchParams(), factualSearchParams);
    }

    @Test
    public void testDeserializedRuleTimestamps() {
        Long currentTimeMillis = 1461355747682L;
        Timestamp currentTimestamp = new Timestamp(currentTimeMillis);
        TaskRule rule = new TaskRule.Builder().source("factual")
                .type(Constants.REFRESH_OPERATION)
                .lastUpdatedBy("test_client")
                .nextScheduleAt(currentTimestamp)
                .autoStart(false)
                .build();
        JSONObject jObject = new JSONObject(rule.toJson());
        Assert.assertEquals(jObject.get(TaskRule.NEXT_SCHEDULE_AT),"2016-04-22T20:09:07Z");
    }


}
