/**
 * Created by wliu on 11/8/17.
 */
package com.intrence.cdg.net;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.intrence.cdg.crawler.NormalizeURL;
import com.intrence.models.util.JsonHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.HttpGet;

import java.io.IOException;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class FetchRequest implements Comparable {

    String workRequest; //example: urls to crawl, factual_id to fetch etc.
    int priority;
    String methodType;
    String httpBody;
    String httpResponse;

    @JsonProperty
    Map<String,String> inputParamters;
    public FetchRequest() {}

    public FetchRequest(Map<String,String> inputParamters) {
        this.inputParamters = inputParamters;
    }

    public FetchRequest(String workRequest, int priority) {
        this(workRequest, priority, HttpGet.METHOD_NAME, null);
    }

    public FetchRequest(String workRequest, int priority, String methodType, String httpBody) {
        if (StringUtils.isEmpty(workRequest)) {
            throw new IllegalArgumentException("Cannot construct fetchReq with empty/null workRequest");
        }

        try {
            if(workRequest.startsWith("http"))
                this.workRequest = NormalizeURL.normalize(workRequest);
            else
                this.workRequest = workRequest;
        }catch (Exception ex){
            throw new RuntimeException(ex);
        }

        this.priority = priority;
        this.methodType = methodType;
        if(StringUtils.isNotBlank(httpBody)) {
            this.httpBody = httpBody;
        }
    }

    public FetchRequest(String workRequest, int priority, String methodType, String httpBody,Map<String,String> inputParamters) {
        this(workRequest, priority, methodType, httpBody);
        this.inputParamters = inputParamters;
    }

    public FetchRequest(String workRequest, int priority, String methodType, String httpBody,Map<String,String> inputParamters, String httpResponse) {
        this(workRequest, priority, methodType, httpBody, inputParamters);
        this.httpResponse = httpResponse;
    }

    public String getWorkRequest() {
        return workRequest;
    }

    public int getPriority() {
        return this.priority;
    }

    public String getInputParameter(String key){
        if(inputParamters != null) {
            return inputParamters.get(key);
        }
        return null;
    }

    @Override
    public String toString() {
        return "FetchRequest{" +
                "workRequest='" + workRequest + '\'' +
                ", priority=" + priority +
                ", methodType='" + methodType + '\'' +
                ", httpBody='" + httpBody + '\'' +
                ", httpResponse='" + httpResponse + '\'' +
                ", inputParamters=" + inputParamters +
                '}';
    }

    @Override
    public int compareTo(Object o) {
        if (this == o) return 0;
        if (o == null || getClass() != o.getClass()) throw new IllegalArgumentException("Object not comparable with FetchRequest");

        FetchRequest that = (FetchRequest) o;
        if (this.priority < that.priority) {
            return -1;
        } else if (this.priority > that.priority) {
            return 1;
        } else {
            return this.workRequest.compareTo(that.workRequest);
        }
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FetchRequest that = (FetchRequest) o;

        if (workRequest != null ? !workRequest.equals(that.workRequest) : that.workRequest != null) return false;
        if (methodType != null ? !methodType.equals(that.methodType) : that.methodType != null) return false;
        if (httpBody != null ? !httpBody.equals(that.httpBody) : that.httpBody != null) return false;
        if (httpResponse != null ? !httpResponse.equals(that.httpResponse) : that.httpResponse != null) return false;
        if (inputParamters != null ? !inputParamters.equals(that.inputParamters) : that.inputParamters != null) return false;
        if (priority != that.priority) return false;



        return true;
    }

    @Override
    public int hashCode() {
        int result = workRequest != null ? workRequest.hashCode() : 0;
        result = 31 * result + (methodType != null ? methodType.hashCode() : 0);
        result = 31 * result + (httpBody != null ? httpBody.hashCode() : 0);
        result = 31 * result + (httpResponse != null ? httpResponse.hashCode() : 0);
        result = 31 * result + (inputParamters != null ? inputParamters.hashCode() : 0);
        result = 31 * result + priority;
        return result;
    }
    public String getMethodType() {
        return methodType;
    }

    public String getHttpBody() {
        return httpBody;
    }

    public String getHttpResponse() {
        return httpResponse;
    }


    public String toJson() throws JsonProcessingException {
        return JsonHandler.getInstance().convertObjectToJsonString(this);
    }

    public static FetchRequest fromJson(String jsonString) {
        try {
            return JsonHandler.getInstance().convertJsonStringToObject(jsonString, FetchRequest.class);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

}

