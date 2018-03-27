package com.intrence.cdg.task;

import com.intrence.cdg.net.FetchRequest;
import com.intrence.cdg.exception.CdgBackendException;
import com.intrence.cdg.exception.ThresholdReachedException;
import com.intrence.cdg.frontier.FetchRequestFrontier;
import com.intrence.cdg.net.RequestResponse;
import com.intrence.cdg.net.WebFetcher;
import com.intrence.cdg.parser.BaseParser;
import com.intrence.cdg.parser.ParserFactory;
import com.intrence.cdg.persistence.PostgresQueryService;
import com.intrence.cdg.util.Constants;
import com.intrence.models.model.DataPoint;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Set;


public class SimpleTask extends Task {

    public static int K_PER_BATCH = 10;

    public SimpleTask(TaskRule rule,
                      PostgresQueryService postgresQueryService,
                      FetchRequestFrontier fetchReqFrontier,
                      WebFetcher webFetcher,
                      Integer parallelism) {
        super(rule, postgresQueryService, fetchReqFrontier, webFetcher, parallelism);
    }

    /**
     * Abstract Methods
     */
    @Override
    public int numberOfFetchReqsPerBatch() {
        return K_PER_BATCH;
    }

    @Override
    public Set<FetchRequest> processFetchRequests(Set<FetchRequest> fetchReqs) throws InterruptedException, CdgBackendException, ThresholdReachedException {

        Set<FetchRequest> failedRequests = new HashSet<>();

        for (FetchRequest request: fetchReqs) {
            if (Thread.currentThread().interrupted()) {
                infoLog("Task stop flag activated, throwing InterruptedException");                
                throw new InterruptedException();
            }

            try {

                long searchStart = System.currentTimeMillis();
                RequestResponse response;

                try {
                    infoLog(String.format("Fetching data from the url=%s",request.getWorkRequest()));

                    // TaskRule Type "stream"
                    if (Constants.STREAM_OPERATION.equals(rule.getType())) {
                        response = webFetcher.getStreamResponse(this.rule.getSource(), request);
                    } else {
                        response = webFetcher.getResponse(this.rule.getSource(), request);
                    }

                } catch (Exception ex) {
                    errorLog(String.format("Exception=WebFetcherException error while getting response for url=%s, " +
                            "time_taken=%d", request.getWorkRequest(), System.currentTimeMillis()-searchStart), ex);
                    failedRequests.add(request);
                    continue;
                }

                long searchEnd = System.currentTimeMillis();
                infoLog(String.format("Event=WebFetcherResponse received response for the url=%s, " +
                                "time_taken=%d, http_status=%d, redirected_url=%s", request.getWorkRequest(),
                        System.currentTimeMillis()-searchStart, response.getStatusCode(), response.getRedirectedUrl()));
                
                long parseTime = 0;
                int statusCode = response.getStatusCode();

                if (Constants.HTTP_STATUS_CODES_TO_RETRY.contains(statusCode)) {
                    failedRequests.add(request); 
                    continue;
                }

                // Successful Responses
                if (statusCode >= 200 && statusCode < 300) {
                    Set<FetchRequest> extractedReqs = handleSuccessfulRequests(response);
                    if (extractedReqs != null) {
                        scheduleExtractedRequests(extractedReqs);
                    }
                    parseTime = System.currentTimeMillis() - searchEnd;
                } else if (Constants.HTTP_REDIRECTION_STATUS_CODES.contains(response.getStatusCode())) {
                    if (response.getResponse() != null) {
                        FetchRequest redirectedReq = handleRedirectedRequests(response);
                        if (redirectedReq != null) {
                            Set<FetchRequest> reqs = new HashSet<>();
                            reqs.add(redirectedReq);
                            scheduleExtractedRequests(reqs);
                        }
                    }
                }

                long end = System.currentTimeMillis();
                infoLog(String.format("Event=FetchReqProcessing crawl_time=%s, place_parse_time=%s," +
                        "total_time=%s, url=%s ", searchEnd - searchStart, parseTime,
                        end - searchStart, request.getWorkRequest()));

            } catch (ThresholdReachedException ex) {
                throw ex;
            } catch (Exception ex){
                failedRequests.add(request);
                errorLog(String.format("Exception=FetchReqProcessing for the url=%s", request.getWorkRequest()),ex);
            }
        }
        return failedRequests;
    }

    
    protected void scheduleExtractedRequests(Set<FetchRequest> reqs) throws CdgBackendException {
        fetchReqFrontier.addAll(this.rule.getId(),reqs);
    }

    protected void persistRawData(RequestResponse requestResponse){
        JSONObject json = new JSONObject();
        json.put("data", requestResponse.getResponse());
        json.put("url", requestResponse.getRequest().getWorkRequest());
        persistRawData(json);
    }

    protected Set<FetchRequest> handleSuccessfulRequests(RequestResponse response) throws Exception {

        // Get the proper parser
        BaseParser parser = (BaseParser) ParserFactory.createParser(rule.getSource(), response.getResponse(), response.getRequest());

        // Persist data
//        if (parser instanceof ProductParser) {
//            persistRawData(response);
//        }

        Set<DataPoint> dataPoints = parser.extractEntities();
        if (dataPoints != null && dataPoints.size() > 0) {
            System.out.println(dataPoints);
//            postgresQueryService.updateCountTaskRun(dataPoints.size(), 1, run.getId());
        }

        return parser.extractLinks();
    }

    protected FetchRequest handleRedirectedRequests(RequestResponse response){
        // Check if we need to enqueue this redirected url for different api sources. e.g. factual
        infoLog(String.format("Event=RedirectionEvent  -  actualUrl=%s  -  redirectedUrl=%s",
                response.getRequest().getWorkRequest(), response.getRedirectedUrl()));
        return new FetchRequest(response.getRedirectedUrl(),1);
    }

}
