/**
 * Created by wliu on 11/8/17.
 */
package com.intrence.cdg.parser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.intrence.cdg.net.FetchRequest;
import com.intrence.cdg.crawler.CrawlConfig;
import com.intrence.cdg.crawler.CrawlablePage;
import com.intrence.cdg.extractor.Extractor;
import com.intrence.cdg.net.ParserWebFetcher;
import com.intrence.cdg.util.Constants;
import com.intrence.cdg.url.UrlHelper;
import com.intrence.models.model.DataPoint;
import com.intrence.models.model.MetaData;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;

public abstract class BaseParser<N> implements Parser<DataPoint>{

    /** Static */
    private final static Logger LOGGER = Logger.getLogger(BaseParser.class);

    protected static final String ANCESTOR_PATH = "ancestor";

    protected static final String URL_EXTRACTOR_PATTERN = "(?s)<a.*?href=\"(.*?)\"";
    protected static final String NUMBER_PATTERN = ".*?(\\d+[\\d\\,\\.]*)[\\s\\S]*?$";

    protected static final String REF_LINK = "ref_link#";
    public static final String METADATA_BUILDER_VERSION = "1.0.0";

    /** Field */
    protected String source;
    protected FetchRequest fetchReq;
    protected String originUrl;
    protected URL originalUrl;
    protected String currentPageType;

    protected JsonNode rules;
    protected Extractor<N> extractor;

    /** Constructor */
    public BaseParser(String source, JsonNode rules, FetchRequest req, Extractor<N> extractor) {

        this.source = source;
        this.rules = rules;
        this.fetchReq = req;
        this.originUrl = req.getWorkRequest();

        this.currentPageType = getPageTypeFromUrl(this.originUrl);
        if (this.currentPageType == null) {
            throw new IllegalArgumentException(String.format("pageType could not be determined for source=%s , url=%s", source, originUrl));
        }

        if (extractor == null) {
            throw new IllegalArgumentException(String.format("Extractor cannot be null for source=%s , url=%s", source, originUrl));
        }
        this.extractor = extractor;

        try {
            if (!(this.originUrl.contains("api-refresh") || this.originUrl.contains("offline-crawl:"))) {
                this.originalUrl = new URL(this.originUrl);
            }
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(String.format("Malformed URL for source=%s , url=%s", source, originUrl), e);
        }
    }

    /** Interface Methods */
    @Override
    public Set<FetchRequest> extractLinks() {
        return extractor.extractLinks(currentPageType, URL_EXTRACTOR_PATTERN);
    }

    public Set<FetchRequest> extractLinks(String extractionPattern) {
        return extractor.extractLinks(currentPageType, extractionPattern);
    }

    @Override
    public Set<DataPoint> extractEntities() {
        if (rules == null) {
            LOGGER.warn(String.format("Given url=%s is not an entity page to parse data point info", fetchReq.getWorkRequest()));
            return new HashSet<>();
        }
        return extractor.extractEntities(this::extractEntity);
    }

    protected abstract DataPoint extractEntity(N contentTree);

    @Override
    public Set<String> extractEntitiesAsJson() {
        Set<DataPoint> dataPoints = extractEntities();
        Set<String> dataPointJsons = new HashSet<>();
        for (DataPoint dataPoint: dataPoints) {
            try {
                dataPointJsons.add(dataPoint.toJson());
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Exception=ParserException could not convert place to json", e);
            }
        }
        return dataPointJsons;
    }

    public String getSourceToPublish() {
        return rules.get("m3_source_id").asText();
    }

    /**
     * This method needs to be implemented by respective parsers to provide context based url for dynamically fetch the
     * web-content from the ParserWebFetcher
     *
     * @param key is the label name to populate the respective urls
     * @return the url to fetch the web-content
     */
    protected abstract String getUrl(String key);

    /** Getters */
    public JsonNode getRulesNode() {
        return rules;
    }

    public Extractor<N> getExtractor() {
        return extractor;
    }

    public String getCurrentPageType() {
        return currentPageType;
    }

    /**
     * Given URL string, checks whether it is crawlable as per the config provided and returns fetch request from the url
     * if it crawlable else it would return null
     * @param url
     * @return FetchRequest formed from the given url, if the url is allowed to crawl
     */
    public FetchRequest checkIfCrawlable(String url){
        return UrlHelper.checkAndGetCrawlableRequest(source, url);
    }

    /**
     * Utility method to get the priority associated with a given page type
     *
     * @param pageTypeName name of the page type
     * @return a priority associated with the requested page in the crawlable graph
     */
    public int getPagePriority(String pageTypeName) {
        CrawlConfig crawlConfig = ParserFactory.getCrawlConfig(source);
        CrawlablePage crawlablePage = crawlConfig.getCrawlablePages().get(pageTypeName);
        if (crawlablePage == null) {
            throw new IllegalArgumentException(
                    String.format("Request PageType=%s is not found for the source=%s", pageTypeName, source));
        }
        return crawlablePage.getPriority();
    }


    protected MetaData buildMetaData() {
        return new MetaData.Builder(METADATA_BUILDER_VERSION, getSourceToPublish(), originUrl).build();
    }

    protected String formatUrl(String urlStr, URL originalUrl) throws MalformedURLException {
        return UrlHelper.formatUrl(urlStr, originalUrl.toString());
    }

    protected String getPageTypeFromUrl(String url){
        return "seed";
//        return UrlHelper.getPageTypeFromUrl(source, url);
    }

    protected boolean isUrlMatched(String urlPattern, String urlStr) {
        return UrlHelper.isUrlMatched(urlPattern, urlStr);
    }

    protected boolean isUrlMatched(Set<String> urlPatterns, String urlStr){
        return UrlHelper.isUrlMatched(urlPatterns, urlStr);
    }

    protected String getRule(String ruleKey) {
        return getRule(rules, ruleKey);
    }

    protected String getRule(JsonNode rules, String ruleKey) {
        String rule = null;
        if (checkFieldExistenceInRulesAndWarn(rules, ruleKey)) {

            rule = rules.get(ruleKey).asText();
        }
        return rule;
    }

    protected boolean checkFieldExistenceInRulesAndWarn(JsonNode rules, String fieldName) {
        if(rules.has(fieldName))
            return true;

        LOGGER.warn(String.format("Event=ParserMissingField field=%s is missing from json rules for the source=%s, url=%s", fieldName,
                source, originUrl));

        return false;
    }

    /**
     * Helper method to retrieve hours and minutes data given a time string
     * @param time ex: "12:00 AM" or "11:45pm" or "15:09" or "11AM" or "2am"
     * @return [12,0] or [23,45] or [15,9] or [11,0] or [2, 0]for above examples
     */
    public String extractHoursAndMins(String time) throws IllegalArgumentException {
        String regex = ".*?(\\d\\d?)(:(\\d\\d?))?.*";
        Matcher matcher = getStringMatcher(regex, time);
        if (matcher.find() && matcher.groupCount() > 0) {
            Integer hour = Integer.parseInt(matcher.group(1));
            Integer mins = 0;
            if (!StringUtils.isBlank(matcher.group(3)))
                mins = Integer.parseInt(matcher.group(3));
            if (hour > 24 || mins > 60)
                throw new IllegalArgumentException("Invalid time string is provided : " + time);
            if (hour < 12 && time.toLowerCase().contains("pm")) {
                hour = hour + 12;
            } else if (hour > 11 && time.toLowerCase().contains("am")) {
                hour = hour - 12;
            }
            return hour + ":" + mins;
        }
        throw new IllegalArgumentException("Invalid time string is provided : " + time);
    }

    protected Matcher getStringMatcher(String regex, String text){
        return UrlHelper.getStringMatcher(regex, text);
    }

    protected LinkedHashSet<String> extractFields(String rule, N node) {
        List<String> fields = extractor.extractFields(rule, node);
        if (CollectionUtils.isEmpty(fields)) {
            fields = new ArrayList<>();
            String fieldString = extractor.extractField(rule, node);

            if (!StringUtils.isBlank(fieldString)) {
                if (fieldString.contains(Constants.COMMA)) {
                    fields.addAll(splitAndCollect(fieldString, Constants.COMMA));
                } else {
                    fields.add(fieldString);
                }
            }
        }
        return CollectionUtils.isEmpty(fields) ? null : new LinkedHashSet<>(fields);
    }

    protected LinkedHashSet<String> splitAndCollect(String targetString, String splitter) {

        LinkedHashSet<String> targetSet = new LinkedHashSet<String>();
        String[] targetList = targetString.split(splitter);

        for(String target: targetList) {
            targetSet.add(target);
        }

        return targetSet;
    }


    /**
     * This method is to get the Http content from the provided url using ParserWebFetcher. It is called when ref_link#
     * tag is present in the parser json rule.
     *
     * @param source name of the crawling site
     * @param url of the crawling site
     * @return the TagNode or JsonNode of the fetched web content
     * @throws Exception
     */
    protected N getSubContent(String source, String url) throws Exception {
        String content = ParserWebFetcher.getInstance().getResponse(source, url);
        return extractor.getContentTree(content);
    }

    /**
     * This process the start and end time (represented in HH:MM format) and return a pair of time after all
     * validations.
     *
     * @param startTime openening time
     * @param endTime closing time
     * @return pair of time represent the open hours
     * @throws IllegalArgumentException in case provided inputs are proper
     */
    protected Pair<String, String> processStartEndTime(String startTime, String endTime)
            throws IllegalArgumentException {
        startTime = extractHoursAndMins(startTime);
        endTime = extractHoursAndMins(endTime);
        // Corner cases check to represent the hour
        if (startTime.startsWith("24")) {
            startTime = "00:" + endTime.split(":")[1];
        }
        if (endTime.startsWith("24")) {
            endTime = "00:" + endTime.split(":")[1];
        }
        return Pair.of(startTime, endTime);
    }


    protected String getNumeric(String fieldWithNumericData) {
        if (StringUtils.isNotBlank(fieldWithNumericData)) {
            fieldWithNumericData = fieldWithNumericData.replaceAll(Constants.COMMA, Constants.EMPTY_STRING);

            Matcher matcher = getStringMatcher(NUMBER_PATTERN, fieldWithNumericData);
            if (matcher != null && matcher.find() && matcher.groupCount() > 0) {
                return matcher.group(1).replace(Constants.COMMA, Constants.EMPTY_STRING);
            }
        }
        return null;
    }

    protected Float getFloatValue(String floatString) {
        floatString = getNumeric(floatString);
        return StringUtils.isNotBlank(floatString) ? Float.parseFloat(floatString) : null;
    }

    protected Double getDoubleValue(String doubleString) {
        doubleString = getNumeric(doubleString); // this is just for a check to meet criteria for string to contain only numbers
        return StringUtils.isNotBlank(doubleString) ? Double.parseDouble(doubleString) : null;
    }

    protected Integer getIntegerValue(String integerString) {
        integerString = getNumeric(integerString);
        return StringUtils.isNotBlank(integerString) ? Integer.parseInt(integerString) : null;
    }

    protected String removeMultipleSpaces(String fieldWitMultipleSpaces) {
        if(fieldWitMultipleSpaces != null) {
            return fieldWitMultipleSpaces.replaceAll("\\s{2,}", " ");
        }
        return null;
    }

    /**
     * This method is to parse the price value in float.
     * @param data string data which extracted from json node for price. ex : 79,50 €
     * @return the float value of price. ex : 79.50
     */
    protected Float getFloatPriceValue(String data) {
        if (StringUtils.isNotBlank(data)) {
            data = data.replace(Constants.DOT, Constants.EMPTY_STRING);
            data = data.replace(Constants.COMMA, Constants.DOT);

            Matcher matcher = getStringMatcher(NUMBER_PATTERN, data);
            if (matcher != null && matcher.find() && matcher.groupCount() > 0) {
                return Float.valueOf(matcher.group(1));
            }
        }
        return null;
    }
}
