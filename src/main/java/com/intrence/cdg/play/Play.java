/**
 * Created by wliu on 11/9/17.
 */
package com.intrence.cdg.play;

import com.intrence.cdg.frontier.FetchRequestFrontier;
import com.intrence.cdg.frontier.RedisFetchReqFrontier;
import com.intrence.cdg.net.WebFetcher;
import com.intrence.cdg.persistence.PostgresQueryService;
import com.intrence.cdg.task.SimpleTask;
import com.intrence.cdg.task.TaskRule;
import com.intrence.cdg.util.Constants;
import com.intrence.config.ConfigProvider;
import com.intrence.config.configloader.ConfigMapLoader;
import com.intrence.config.configloader.YamlFileConfigMapLoader;

import java.sql.Timestamp;

public class Play {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting CDG Play");

//        TaskRule taskRule = new TaskRule.Builder()
//                .id(2)
//                .source("farfetch")
//                .lastUpdatedBy("test_client")
//                .nextScheduleAt(new Timestamp(System.currentTimeMillis()))
//                .type(Constants.REFRESH_OPERATION)
//                .autoStart(false)
//                .clientId("test_client")
//                .status(TaskRule.Status.RUNNABLE)
//                .build();

        TaskRule totokaeloTaskRule = new TaskRule.Builder()
                .id(10)
                .source("totokaelo")
                .lastUpdatedBy("test_client")
                .nextScheduleAt(new Timestamp(System.currentTimeMillis()))
                .type(Constants.REFRESH_OPERATION)
                .autoStart(false)
                .clientId("test_client")
                .status(TaskRule.Status.RUNNABLE)
                .build();

        PostgresQueryService postgresQueryService = PostgresQueryService.getInstance();
//        postgresQueryService.addRule(totokaeloTaskRule);

        FetchRequestFrontier fetchRequestFrontier = new RedisFetchReqFrontier();
        ((RedisFetchReqFrontier) fetchRequestFrontier).init();

        ConfigMapLoader configMapLoader = new YamlFileConfigMapLoader(Constants.CDG_SOURCES_CONFIG_LOCALHOST_PATH);
        WebFetcher webFetcher = new WebFetcher(ConfigProvider.getConfig(), configMapLoader.getConfigMap());
        webFetcher.initWebFetcher();

        SimpleTask simpleTask = new SimpleTask(totokaeloTaskRule, postgresQueryService, fetchRequestFrontier, webFetcher, 1);
        simpleTask.start();

        System.out.println("Ending CDG Play");
    }

}