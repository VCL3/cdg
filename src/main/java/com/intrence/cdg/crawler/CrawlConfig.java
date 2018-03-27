package com.intrence.cdg.crawler;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.intrence.cdg.util.Constants;
import com.intrence.cdg.util.JsonNodeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.HttpPost;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class CrawlConfig {

    private IntegrationType            type;
    private Set<String>                seedUrls;
    private Map<String, CrawlablePage> crawlablePages;

    private Map<String, String>      paramsFormat;
    private Map<String, Set<String>> crawlGraph;
    private NextPageUrlBuilder       nextPageUrlBuilder;
    private String                   methodType;
    private String                   body;
    private boolean                  isBatchrequest;

    private static final String SEED_PAGE_TYPE = "seed";

    // This set will have all task integration types, which don't require crawler config.
    public static final Set<String> OFFLINE_TYPES = ImmutableSet.of(IntegrationType.ftp.name(),
                                                                    IntegrationType.soap.name());

    private CrawlConfig() { }

    private CrawlConfig(Builder builder) {
        this.type = builder.type;
        this.seedUrls = builder.seedUrls;
        this.crawlablePages = builder.crawlablePages;
        this.paramsFormat = builder.paramsFormat;
        this.crawlGraph = builder.crawlGraph;
        this.nextPageUrlBuilder = builder.nextPageUrlBuilder;
        this.methodType = builder.methodType;
        this.body = builder.body;

        this.isBatchrequest = builder.isBatchrequest;

    }

    public Set<String> getSeedUrls() {
        return seedUrls;
    }


    public boolean isBatchrequest() {
        return isBatchrequest;
    }

    public IntegrationType getType() {
        return this.type;
    }

    public Map<String, CrawlablePage> getCrawlablePages() {
        return this.crawlablePages;
    }

    public Map<String, Set<String>> getCrawlGraph() {
        return this.crawlGraph;
    }

    public NextPageUrlBuilder getNextPageUrlBuilder() {
        return this.nextPageUrlBuilder;
    }

    public Map<String, String> getParamsFormat() {
        return this.paramsFormat;
    }

    public String getMethodType() {
        return this.methodType;
    }

    public String getBody() {
        return this.body;
    }

    public static CrawlConfig fromJson(String json) throws JsonProcessingException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode config = mapper.readTree(json);

        String type = JsonNodeUtil.getTextual(config, "/type");

        Builder builder = new Builder();
        builder.type = IntegrationType.valueOf(type);

        if (type.equals(IntegrationType.soap.name())) {
            builder.paramsFormat = extractParamsFormat(config);
            return builder.build();
        }

        builder.isBatchrequest = isBatch(config);
        if (!isOfflineType(type) && !builder.isBatchrequest) {
            builder.seedUrls = extractSeedUrls(config);
        }

        builder.crawlablePages = extractCrawlablePages(config);
        builder.paramsFormat = extractParamsFormat(config);
        builder.crawlGraph = extractCrawlGraph(config);
        builder.methodType = extractSeedMethodType(config);

        if (StringUtils.isNotBlank(builder.methodType) && builder.methodType.equalsIgnoreCase(HttpPost.METHOD_NAME)) {
            builder.body = extractSeedBody(config);
        }

        //check if a pageType part of crawlGraph config does not have crawlablePages definition
        for (Entry<String, Set<String>> entry : builder.crawlGraph.entrySet()) {
            if (!SEED_PAGE_TYPE.equals(entry.getKey()) && !builder.crawlablePages.containsKey(entry.getKey())) {
                throw new IllegalArgumentException("No page_types definition provided for " + entry.getKey());
            }

            for (String pageType : entry.getValue()) {
                if (!builder.crawlablePages.containsKey(pageType) && !NextPageUrlBuilder.KEY.equals(pageType)) {
                    throw new IllegalArgumentException("No page_types definition provided for " + pageType);
                }
            }
        }

        builder.nextPageUrlBuilder = extractNextPageUrlBuilder(config);

        return builder.build();
    }

    private static String extractSeedBody(final JsonNode config) {
        return JsonNodeUtil.getTextual(config, Constants.BODY);
    }

    public static boolean isBatch(JsonNode config) {
        String batchedRequest = JsonNodeUtil.getTextual(config, Constants.BATCHED_REQUESTS, "");
        return batchedRequest.equalsIgnoreCase("true");
    }

    public static boolean isOfflineType(String type) {
        return OFFLINE_TYPES.contains(type);
    }

    public static boolean isSoapType(String type) {
        if (IntegrationType.soap.toString().equalsIgnoreCase(type)) { return true; }
        return false;
    }

    protected static NextPageUrlBuilder extractNextPageUrlBuilder(JsonNode config) {
        JsonNode nextPageUrlConfig = config.get(NextPageUrlBuilder.KEY);
        if (nextPageUrlConfig == null || nextPageUrlConfig.isMissingNode()) {
            return null;
        }
        String responseJsonPath = JsonNodeUtil.getTextual(nextPageUrlConfig,
                                                          "/" + NextPageUrlBuilder.EXTRACT_FROM_RESPONSE_KEY);
        String nextPageUrl = JsonNodeUtil.getTextual(nextPageUrlConfig, "/" + NextPageUrlBuilder.NEXT_PAGE_URL_KEY);
        int priority = JsonNodeUtil.getNumber(nextPageUrlConfig, NextPageUrlBuilder.PRIORITY_KEY);
        return new NextPageUrlBuilder(responseJsonPath, nextPageUrl, priority);
    }

    protected static Set<String> extractSeedUrls(JsonNode config) {
        Set<String> seeds = new HashSet<>();
        JsonNode seedUrlConfig = JsonNodeUtil.getArray(config, Constants.SEED_URLS);

        for (JsonNode seed : seedUrlConfig) {
            if (!seed.isTextual()) {
                throw new IllegalArgumentException("seed_urls crawler config is not an array of string");
            }
            seeds.add(seed.asText());
        }

        if (seeds.isEmpty()) {
            throw new IllegalArgumentException("seed_urls cannot be empty");
        }

        return ImmutableSet.copyOf(seeds);
    }

    protected static String extractSeedMethodType(JsonNode config) {
        JsonNode node = config.at(Constants.METHOD_TYPE);
        if (node == null || node.isMissingNode() || !node.isTextual()) {
            return "";
        }
        return node.asText();
    }

    protected static Map<String, CrawlablePage> extractCrawlablePages(JsonNode config) {
        Map<String, CrawlablePage> map = new HashMap<>();
        JsonNode pagesNode = JsonNodeUtil.getArray(config, Constants.PAGE_TYPES);

        for (JsonNode pageNode : pagesNode) {
            int priority = JsonNodeUtil.getNumber(pageNode, "priority");
            String pageType = JsonNodeUtil.getTextual(pageNode, "/pageType");
            Set<String> pagePatterns = new HashSet<>();
            for (JsonNode pattern : JsonNodeUtil.getArray(pageNode, "/pattern")) {
                pagePatterns.add(pattern.asText());
            }
            map.put(pageType, new CrawlablePage(pageType, pagePatterns, priority));
        }

        return ImmutableMap.copyOf(map);
    }

    protected static Map<String, String> extractParamsFormat(JsonNode config) {
        JsonNode formatNode = config.get("params_format");
        if (formatNode == null || formatNode.isMissingNode()) {
            return ImmutableMap.of();
        }

        Map<String, String> paramsFormatMap = new HashMap<>();
        Iterator<String> iterator = formatNode.fieldNames();
        while (iterator.hasNext()) {
            String param = iterator.next();
            paramsFormatMap.put(param, formatNode.get(param).textValue());
        }

        return ImmutableMap.copyOf(paramsFormatMap);
    }

    protected static Map<String, Set<String>> extractCrawlGraph(JsonNode config) {
        JsonNode graphNode = config.get("graph");
        if (graphNode == null || graphNode.isMissingNode()) {
            return ImmutableMap.of();
        }

        Map<String, Set<String>> graph = new HashMap<>();
        Iterator<Entry<String, JsonNode>> it = graphNode.fields();

        while (it.hasNext()) {
            Entry<String, JsonNode> entry = it.next();
            String source = entry.getKey();
            if (!entry.getValue().isArray()) {
                throw new IllegalArgumentException("crawler config graph is not an adjacency list of pageTypes");
            }
            for (JsonNode dest : entry.getValue()) {
                if (!dest.isTextual()) {
                    throw new IllegalArgumentException("crawler config graph is not an adjacency list of pageTypes");
                }

                Set<String> destinations = graph.getOrDefault(source, new HashSet<String>());
                destinations.add(dest.asText());
                graph.put(source, destinations);
            }
        }

        if (graph.isEmpty()) {
            throw new IllegalArgumentException("crawler config graph is not an adjacency list of pageTypes");
        }

        if (!graph.containsKey(SEED_PAGE_TYPE)) {
            throw new IllegalArgumentException("crawler config graph does not define seed key");
        }

        return ImmutableMap.copyOf(graph);
    }

    public boolean isInterestingLink(String urlString) {
        Set<String> pageTypes = crawlablePages.keySet();
        for (String pageType : pageTypes) {
            Set<String> urlPatterns = crawlablePages.get(pageType).getUrlPatterns();
            if (urlPatterns != null) {
                for (String urlPattern : urlPatterns) {
                    if (urlString.matches(urlPattern)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }

        CrawlConfig that = (CrawlConfig) o;
        if (type != null ? !type.equals(that.type) : that.type != null) { return false; }
        if (seedUrls != null ? !seedUrls.equals(that.seedUrls) : that.seedUrls != null) { return false; }
        if (crawlablePages != null ? !crawlablePages.equals(that.crawlablePages) : that.crawlablePages != null) {
            return false;
        }
        if (paramsFormat != null ? !paramsFormat.equals(that.paramsFormat) : that.paramsFormat != null) {
            return false;
        }
        if (crawlGraph != null ? !crawlGraph.equals(that.crawlGraph) : that.crawlGraph != null) { return false; }
        if (methodType != null ? !methodType.equals(that.methodType) : that.methodType != null) { return false; }
        if (body != null ? !body.equals(that.body) : that.body != null) { return false; }
        return true;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (seedUrls != null ? seedUrls.hashCode() : 0);
        result = 31 * result + (crawlablePages != null ? crawlablePages.hashCode() : 0);
        result = 31 * result + (paramsFormat != null ? paramsFormat.hashCode() : 0);
        result = 31 * result + (crawlGraph != null ? crawlGraph.hashCode() : 0);
        result = 31 * result + (methodType != null ? methodType.hashCode() : 0);
        result = 31 * result + (body != null ? body.hashCode() : 0);
        return result;
    }

    private static class Builder {
        private IntegrationType            type;
        private Set<String>                seedUrls;
        private Map<String, CrawlablePage> crawlablePages;

        private Map<String, String>      paramsFormat;
        private Map<String, Set<String>> crawlGraph;
        private NextPageUrlBuilder       nextPageUrlBuilder;
        private String                   methodType;
        private String                   body;
        private boolean                  isBatchrequest;

        public CrawlConfig build() {
            return new CrawlConfig(this);
        }
    }
}
