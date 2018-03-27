package com.intrence.cdg.crawler;

import com.intrence.cdg.net.FetchRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;

public class NextPageUrlBuilder {
    private final static Logger LOGGER = Logger.getLogger(NextPageUrlBuilder.class);
    public static final String KEY = "nextPageUrlBuilder";
    public static final String EXTRACT_FROM_RESPONSE_KEY = "extractFromResponse";
    public static final String EXTRACT_FROM_RESPONSE_KEY_PATTERN = "%"+EXTRACT_FROM_RESPONSE_KEY +"%";
    public static final String NEXT_PAGE_URL_KEY = "nextPageUrl";
    public static final String PRIORITY_KEY = "priority";    
    
    private static final ObjectMapper MAPPER = (new ObjectMapper()).registerModule(new JodaModule());
    
    private final String extractFromResponsePath;
    private final String nextPageUrl;
    private final int priority;
    
    public NextPageUrlBuilder(String extractFromResponsePath, String nextPageUrl, int priority) {
        if ( StringUtils.isBlank(extractFromResponsePath) ||  StringUtils.isBlank(nextPageUrl) ) {
            throw new IllegalArgumentException("extractFromResponse or nextPageUrl are blank in the config");
        }
        this.extractFromResponsePath = extractFromResponsePath;
        this.nextPageUrl = nextPageUrl;
        this.priority = priority;
    }
    
    public FetchRequest build(String responseContent) {
        try {
            JsonNode root = MAPPER.readTree(responseContent);
            JsonNode extractedFromResponse = root.at(extractFromResponsePath);
            if (extractedFromResponse == null || extractedFromResponse.isMissingNode() || extractedFromResponse.isNull()) {
                return null;
            }
            String url = this.nextPageUrl.replaceAll(EXTRACT_FROM_RESPONSE_KEY_PATTERN, extractedFromResponse.asText());
            return new FetchRequest(url, this.priority);
        } catch (Exception e) {
            LOGGER.error("Could not build next page url from response content", e);
            return null;
        }
    }

}
