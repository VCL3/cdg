package com.intrence.cdg.net;

import com.intrence.cdg.streamer.provider.StreamProvider;
import org.apache.http.Header;
import org.apache.http.HttpStatus;

import java.util.HashMap;
import java.util.Map;

public class RequestResponse {
    private FetchRequest request;
    private String response;
    private int statusCode;
    private String redirectedUrl;
    private StreamProvider streamProvider;
    private Map<String,String> headerMap = new HashMap<>();

    public RequestResponse(FetchRequest request, String response, Integer statusCode){
        this.request = request;
        this.response = response;
        this.statusCode = statusCode;
    }

    public RequestResponse(FetchRequest request, StreamProvider streamProvider, Integer statusCode){
        this.request = request;
        this.streamProvider = streamProvider;
        this.statusCode = statusCode;
    }

    public RequestResponse(FetchRequest request, String response, int statusCode, String redirectUrl) {
        this(request,response,statusCode);
        this.redirectedUrl = redirectUrl;
    }

    public RequestResponse(final FetchRequest fetchReq, final String response, final int statusCode, final Header[] responseHeaders) {
        this(fetchReq,response,statusCode);
        for (Header header : responseHeaders) {
            headerMap.put(header.getName(),header.getValue());
        }
    }


    public FetchRequest getRequest() {
        return request;
    }

    public String getResponse() {
        return response;
    }

    public StreamProvider getStreamProvider() {
        return streamProvider;
    }

    public Integer getStatusCode(){
        return statusCode;
    }

    public String getRedirectedUrl() {
        return redirectedUrl;
    }

    public Map<String, String> getHeaderMap() {
        return headerMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RequestResponse that = (RequestResponse) o;

        if (request != null ? !request.equals(that.request) : that.request != null) return false;
        if (response != null ? !response.equals(that.response) : that.response != null) return false;
        if (streamProvider != null ? !streamProvider.equals(that.streamProvider) : that.streamProvider != null) return false;
        if (statusCode != that.statusCode) return false;
        if (redirectedUrl != null ? !redirectedUrl.equals(that.redirectedUrl) : that.redirectedUrl != null) return false;
        if (headerMap != null ? !headerMap.equals(that.headerMap) : that.headerMap != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = request != null ? request.hashCode() : 0;
        result = 31 * result + (response != null ? response.hashCode() : 0);
        result = 31 * result + (streamProvider != null ? streamProvider.hashCode() : 0);
        result = 31 * result + statusCode;
        result = 31 * result + (redirectedUrl != null ? redirectedUrl.hashCode() : 0);
        result = 31 * result + (headerMap != null ? headerMap.hashCode() : 0);
        return result;
    }

    public boolean isSuccess(){
        if(statusCode >= HttpStatus.SC_OK && statusCode < HttpStatus.SC_MULTIPLE_CHOICES){
            return true;
        }
        return false;
    }
}
