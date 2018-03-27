package com.intrence.cdg.task;

import com.intrence.config.ConfigProvider;
import com.intrence.config.collection.ConfigMap;
import com.intrence.cdg.net.FetchRequest;
import com.intrence.cdg.exception.CdgBackendException;
import com.intrence.cdg.exception.ThresholdReachedException;
import com.intrence.cdg.frontier.FetchRequestFrontier;
import com.intrence.cdg.net.WebFetcher;
import com.intrence.cdg.parser.ParserFactory;
import com.intrence.cdg.persistence.PostgresQueryService;
import com.intrence.cdg.url.SeedUrlProvider;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

/**
 * Abstract class for task execution that calls implementations of abstract methods to process fetch reqs.
 * - additionally, it also takes care of updating task-rule and run metadata during task execution.
 */
public abstract class Task {

    private static final ConfigMap CONFIG_MAP = ConfigProvider.getConfig();
    private static final Logger LOGGER = Logger.getLogger(Task.class);

    protected ExecutorService singleThreadTaskRunner = Executors.newFixedThreadPool(1);
    private ExecutorService fetchReqProcessingThreads;

    private Integer BAD_RESPONSE_RATE_DELAY = CONFIG_MAP.getInteger("bad_response_rate_delay", 60000);
    private final Integer ALLOWED_CONSECUTIVE_BAD_RESPONSES = 3;
    private static final double BAD_RESPONSE_RATE = 0.2;
    private static final int MAX_RETRIES = 3;

    protected Integer parallelism;
    protected TaskRule rule;
    protected TaskRun run;

    protected PostgresQueryService postgresQueryService;
    protected FetchRequestFrontier fetchReqFrontier;
    protected WebFetcher webFetcher;
    protected SeedUrlProvider seedUrlProvider;

    protected Task(TaskRule rule,
                   PostgresQueryService postgresQueryService,
                   FetchRequestFrontier fetchReqFrontier,
                   WebFetcher webFetcher,
                   Integer parallelism) {
        this.rule = rule;    
        this.postgresQueryService = postgresQueryService;
        this.fetchReqFrontier = fetchReqFrontier;
        this.webFetcher = webFetcher;
        this.parallelism = parallelism;
    }

    public void start() {
        fetchReqProcessingThreads = Executors.newFixedThreadPool(this.parallelism);
        singleThreadTaskRunner.execute(new TasksRunnable());
    }

    public void stop() {
        fetchReqProcessingThreads.shutdownNow();
        singleThreadTaskRunner.shutdownNow();
    }

    /**
     * Getters and Setters
     */
    public TaskRule getRule() {
        return this.rule;
    }

    public TaskRun getRun() {
        return this.run;
    }

    public void setRun(TaskRun run) {
        this.run = run;
    }

    /**
     * Abstract Methods
     */
    // number of fetch reqs to process per batch
    public abstract int numberOfFetchReqsPerBatch();

    // get raw data from source, parse, transform and persist, return failed requests
    public abstract Set<FetchRequest> processFetchRequests(Set<FetchRequest> fetchReqs) throws InterruptedException,
            CdgBackendException, ThresholdReachedException;

    // Create seed urls and append to the fetchReqFrontier to process
    public void queueSeedRequests() throws Exception {
        seedUrlProvider = ParserFactory.createSeedUrlProvider(rule.getSource());
        Set<FetchRequest> seedUrls = seedUrlProvider.buildSeedUrls(rule.getSearchParams());
        fetchReqFrontier.addAll(rule.getId(), seedUrls);
    }

    protected void persistRawData(JSONObject jsonObject) {
        // persist raw data
    }

    /**
     * Logging
     */
    protected void infoLog(String message) {
        LOGGER.info(formatMessage(message));
    }

    protected void errorLog(String message, Exception e) {        
        LOGGER.error(formatMessage(message), e);
    }
    
    protected void warnLog(String message, Exception e) {
        LOGGER.warn(formatMessage(message), e);
    }
    
    protected void warnLog(String message) {
        LOGGER.warn(formatMessage(message));
    }
    
    private String formatMessage(String message) {
        return String.format("taskRuleId=%s threadName=%s taskType=%s Source=%s ",
                rule.getId(), Thread.currentThread(), rule.getType(), rule.getSource()) + message;
    }


    /**
     * Task Runnable Implementation
     */
    protected class TasksRunnable implements Runnable {

        @Override
        public void run() {

            // topK fetchReqs to be processed per batch
            int k = numberOfFetchReqsPerBatch();

            // Setup retry and fail records
            Map<FetchRequest, Integer> failedRequestsRetryMap = new HashMap();
            Set<FetchRequest> allFailedRequests = new HashSet<>();
            Set<FetchRequest> topKReqs = new HashSet<>();

            try {
                // task-run table is for recording history of runs, every time a rule is run, a new row is created in task_run
                run = postgresQueryService.createTaskRun(rule);

                // If rule is in RUNNABLE state, queue seed reqs and then mark the state to RUNNING
                if (TaskRule.Status.RUNNABLE.equals(rule.getStatus())) {
                    infoLog("Event=QueueSeedReqs");
                    queueSeedRequests();
                    // For recurring tasks, reset urls fetched count
                    postgresQueryService.saveUrlsFetchedCount(rule.getId(), 0);
                }

                int urlsFetched = postgresQueryService.getUrlsFetchedCount(rule.getId());
                int badResponseOccurrence = 0;

                // Mark the TaskRule state to RUNNING
                TaskRule.Builder ruleBuilder = new TaskRule.Builder(rule);
                ruleBuilder.status(TaskRule.Status.RUNNING);
                rule = new TaskRule(ruleBuilder);
                postgresQueryService.updateRule(rule);
                postgresQueryService.updateTaskRunStatus(run.getId(), TaskRule.Status.RUNNING.toString());

                // ----- FETCHING ----- //
                // while the frontier queue has some fetch reqs, keep processing them and update task_run
                while ((topKReqs = fetchReqFrontier.getTopK(rule.getId(), k)) != null && !topKReqs.isEmpty()) {

                    checkThreadInterruption();

                    infoLog(String.format("Event=ProcessTopKBatch k=%s", topKReqs.size()));

                    //divide topK among threads and submit for processing
                    List<Set<FetchRequest>> subsets = subset(topKReqs, parallelism);
                    List<Future<Set<FetchRequest>>> futures = new ArrayList<>();
                    for (Set<FetchRequest> subset : subsets) {                        
                        Future<Set<FetchRequest>> future = fetchReqProcessingThreads.submit( () -> {
                            try{
                                // it returns only failed Requests
                                return processFetchRequests(subset);
                            } catch (ThresholdReachedException e) {
                                throw e;
                            } catch (InterruptedException e) {
                                warnLog("interrupted exception happened from processing threads",e);
                                // returning the entire subset in case of InterruptedException is just
                                // a precautionary measure for other threads to take it up instead of
                                // deleting it from queue.
                                return subset;
                            }
                        } );
                        futures.add(future);
                    }

                    // wait for processing to finish and gather result
                    for (Future<Set<FetchRequest>> future: futures) {
                        try {
                            Set<FetchRequest> failedRequests = future.get();
                            // Accumulate all failed request from all the threads
                            allFailedRequests.addAll(failedRequests);
                        }
                        // InterruptedException from all processing threads would be consumed and ignored here,
                        //  - the topK loop above would eventually find out that the task is cancelled/interrupted and bail out in next iteration
                        //  - this way it is cleaner as it waits for all processing threads to finish / get interrupted and eventually exit                            
                        // Any unknown runtime-exceptions are thrown out immediately which will result in marking the task as ERROR
                        catch (InterruptedException e) {
                            warnLog("interrupted exception happened from processing threads",e);
                        } catch(ExecutionException e){
                            Throwable rootException = ExceptionUtils.getRootCause(e);
                            if (rootException instanceof InterruptedException) {
                                warnLog("interrupted exception happened from processings d threads",e);
                            } else {
                                throw e;
                            }
                        }
                    }

                    checkThreadInterruption();
                    postgresQueryService.updateTaskRun(run.getId());
                    
                    // Sleep if bad responseCodes > 20% of total requests                    
                    badResponseOccurrence = sleepIfHighBadResponse(topKReqs.size(), badResponseOccurrence,
                            allFailedRequests.size());
                    urlsFetched += (topKReqs.size() - allFailedRequests.size());

                    // Update retry count for each failed request, retry MAX_RETRIES(3) times
                    // Drop the failed request if it either fails after MAX_RETRIES or passed during future retries
                    updateFailedRequests(failedRequestsRetryMap, allFailedRequests, topKReqs);

                    // clear the set for reuse
                    allFailedRequests.clear();

                    // Save urlsFetched count in postgres
                    postgresQueryService.saveUrlsFetchedCount(rule.getId(), urlsFetched);                    
                    //check UrlsFetched against max_records for remaining urls to be fetched                    
                    if (urlsFetched >= rule.getMaxRecords()) {
                        infoLog(String.format("Fetched max no of urls provided, urlsFetched=%d", urlsFetched));
                        break;
                    } else {
                        k = (rule.getMaxRecords() - urlsFetched) > k ? k : rule.getMaxRecords() - urlsFetched;
                    }

                    //Todo: Handle response codes like 416 (range not satisfiable) etc ?

                }

                // Finish as no task in fetchReqFrontier
                postgresQueryService.markTaskRuleFinished(rule.getId());
                postgresQueryService.updateTaskRunStatus(run.getId(), TaskRule.Status.FINISHED.toString() );
                fetchReqFrontier.deleteKey(rule.getId());

            } catch (Exception e) {
                Throwable rootException = ExceptionUtils.getRootCause(e);

                if (rootException instanceof ThresholdReachedException) {
                    postgresQueryService.markTaskRuleFinished(rule.getId());
                    postgresQueryService.updateTaskRunStatus(run.getId(), TaskRule.Status.FINISHED.toString() );
                    fetchReqFrontier.deleteKey(rule.getId());
                    warnLog(rootException.getMessage(), e);
                } else if (e instanceof CancellationException || e instanceof RejectedExecutionException
                        || e instanceof InterruptedException) {
                    // task-run is cancelled either via user request or webapp shutdown
                    // - either case, the task status is unchanged and leave it to user request api to change the status if need be
                    // - this also aids in resuming the task when left in running state
                    warnLog("Task run is cancelled, leaving the status unchanged", e);                    
                } else {
                    errorLog("Exception=TaskExecutionException", e);
                    postgresQueryService.updateTaskRuleStatus(rule.getId(), TaskRule.Status.ERROR);
                    postgresQueryService.updateTaskRunStatus(run.getId(), TaskRule.Status.ERROR.toString() );
                    // Delete all successful requests from the queue, in case of exception thrown
                    // due to too many bad responses. This is needed if same task is resumed in future,
                    // then only non-successful urls from the redis should be tried
                    topKReqs.removeAll(allFailedRequests);
                    fetchReqFrontier.deleteAll(rule.getId(), topKReqs);
                }
            }
        }

        private void checkThreadInterruption() throws InterruptedException {
            if (Thread.currentThread().interrupted()) {
                infoLog("Task.java Task stop flag activated, throwing InterruptedException");
                throw new InterruptedException();
            }
        }
	}

    /**
     * This method updates the failedRequestRetryMap with the latest retry count and
     * do the set manipulation to clean this map appropriately. Also, deleted all
     * successful requests from Redis queue after this set manipulation.
     *
     * @param failedRequestsRetryMap used to update the failed requests with the retry count
     * @param failedRequests set of failed request in the current set of topK url fetch
     * @param topKRequests list of topK url fetch request
     */
    public void updateFailedRequests(Map<FetchRequest, Integer> failedRequestsRetryMap,
            Set<FetchRequest> failedRequests, Set<FetchRequest> topKRequests) {
        Set<FetchRequest> successfulRequests = new HashSet<>();
        for (FetchRequest fetchRequest : topKRequests) {
            if (failedRequests.contains(fetchRequest)) {
                int retryCount = failedRequestsRetryMap.getOrDefault(fetchRequest, 0);
                if (++retryCount < MAX_RETRIES) {
                    failedRequestsRetryMap.put(fetchRequest, retryCount);
                    infoLog(String.format("Failed request=%s, retry_count=%d", fetchRequest, retryCount));
                } else {
                    failedRequestsRetryMap.remove(fetchRequest);
                    // Delete all request which have reached their max. retries
                    fetchReqFrontier.delete(rule.getId(), fetchRequest);
                    warnLog(String.format("Dropping request url=%s, exhausted max_retries=%d", fetchRequest,
                            MAX_RETRIES));
                }
            } else {
                successfulRequests.add(fetchRequest);
                // remove request which was not a part of failedRequests, but got
                // succeeded in their current retries (if any)
                failedRequestsRetryMap.remove(fetchRequest);
            }
        }

        // Delete all successful requests from the queue
        fetchReqFrontier.deleteAll(rule.getId(), successfulRequests);
    }

    protected int sleepIfHighBadResponse(int k, int badResponseOccurrence, int badResponseCount) throws Exception {

        if( badResponseCount > k * BAD_RESPONSE_RATE) {
            badResponseOccurrence++;

            if( badResponseOccurrence > ALLOWED_CONSECUTIVE_BAD_RESPONSES ) {
                throw new Exception(String.format("Too many bad responses=%4.2f%%, marking taskRule status='ERROR'",
                        (double)badResponseCount*100/k));
            }
            LOGGER.info(String.format("Sleeping for time=%d due to high bad response rate for source=%s, ruleId=%d",
                    BAD_RESPONSE_RATE_DELAY*badResponseOccurrence, rule.getSource(), rule.getId()));
            Thread.sleep(BAD_RESPONSE_RATE_DELAY*badResponseOccurrence);
            return badResponseOccurrence;
        }
        return 0;
    }

    // Divide top k fetch request based on number of threads
    protected List<Set<FetchRequest>> subset(Set<FetchRequest> set, int parallelism) {

        List<Set<FetchRequest>> subsets = new ArrayList<>();
        int splitSize = (int) Math.ceil((double) set.size() / (double) parallelism);

        Set<FetchRequest> subset = new HashSet<>();
        int i = 0;
        for (FetchRequest fetchReq : set) {
            if (i == splitSize) {
                subsets.add(subset);
                i = 0;
                subset = new HashSet<>();
            }

            subset.add(fetchReq);
            i++;
        }

        if (!subset.isEmpty()) {
            subsets.add(subset);
        }
        return subsets;
    }
}
