package com.intrence.cdg.extractor;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.intrence.cdg.crawler.CrawlConfig;
import com.intrence.cdg.crawler.NextPageUrlBuilder;
import com.intrence.cdg.net.FetchRequest;
import com.intrence.cdg.parser.ParserFactory;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class JsonExtractor extends BaseExtractor<JsonNode> {

    private static final ObjectMapper MAPPER = (new ObjectMapper()).registerModule(new JodaModule());

    static {
        //This is to allow some JSON object which contain html strings
        MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
    }

    private JsonNode jsonTree;

    public JsonExtractor(String source, String content, String url) {

        super(source, content, url);
        createContentTree(content);
    }

    @Override
    public JsonNode getContentTree() {
        return jsonTree;
    }

    @Override
    public <T> Set<T> extractEntities(Function<JsonNode, T> extractEntityFunction) {

        return extractEntities(jsonTree, extractEntityFunction);
    }

    @Override
    public Set<FetchRequest> extractLinks(String pageType, String extractionPattern) {
        CrawlConfig crawlConfig = ParserFactory.getCrawlConfig(source);

        //if no outlink configured return empty set
        if (crawlConfig.getCrawlablePages() == null) {
            throw new RuntimeException("something wrong, no crawlable pages defined in crawlConfig");
        }

        Set<FetchRequest> newReqs = new HashSet<>();
        if(crawlConfig.getCrawlGraph() == null || crawlConfig.getCrawlGraph().get(pageType) == null){
            return newReqs;
        }
        for (String destination : crawlConfig.getCrawlGraph().get(pageType)) {
            if (NextPageUrlBuilder.KEY.equals(destination)) {
                FetchRequest nextPageUrl = crawlConfig.getNextPageUrlBuilder().build(content);
                if (nextPageUrl != null) {
                    newReqs.add(nextPageUrl);
                }
            }
        }

        return newReqs;
    }

    @Override
    public String extractField(String rule, JsonNode jsonTree) {
        JsonNode node = extractFieldObject(rule, jsonTree);
        return extractText(node);
    }

    /*
    Will work only for Json arrays having values as String and Numbers. ["133", 4, 5.5]
    It will not extract values from array of Json like [{..}, {..}]
     */
    @Override
    public List<String> extractFields(String rule, JsonNode jsonTree) {

        ArrayList<String> fields = new ArrayList<String>();
        JsonNode valueNode = extractFieldObject(rule, jsonTree);

        if (valueNode.isArray()) {
            for (JsonNode node : valueNode) {
                String field = extractText(node);
                if (!StringUtils.isBlank(field)) {
                    fields.add(field);
                }
            }
        }

        return fields;
    }

    @Override
    public JsonNode extractFieldObject(String rule, JsonNode jsonTree) {
        JsonNode node = jsonTree.at(rule);
        return node.isMissingNode() ? MissingNode.getInstance() : node;
    }

    @Override
    public JsonNode[] extractFieldObjects(String rule, JsonNode jsonTree) {

        JsonNode valueNode = extractFieldObject(rule, jsonTree);
        JsonNode[] fieldObjects = null;

        if (valueNode.isArray()) {
            fieldObjects = new JsonNode[valueNode.size()];
            int index = 0;
            for (JsonNode node : valueNode) {
                fieldObjects[index++] = node;
            }
        }

        return ArrayUtils.isEmpty(fieldObjects) ? new JsonNode[0] : fieldObjects;
    }

    @Override
    public List<Map<String, String>> extractRepeatedFields(JsonNode repeatedRules, JsonNode jsonTree) {
        throw new NotImplementedException("Not implemented for JSONExtractor");
    }

    private void createContentTree(String content) {
        if (jsonTree != null) {
            return;
        }
        jsonTree = getContentTree(content);
    }

    private String extractText(JsonNode node) {

        if (node.isMissingNode() || StringUtils.isEmpty(node.asText())) {
            return null;
        }
        return node.asText().trim();
    }

    private <T> Set<T> extractEntities(JsonNode node, Function<JsonNode, T> extractEntityFunction) {
        Set<T> entities = new HashSet<>();

        T entity = extractEntityFunction.apply(node);
        if (entity != null) {
            entities.add(entity);
            return entities;
        } else if (node.size() > 0) {
            for (JsonNode innerNode : node) {
                entities.addAll(extractEntities(innerNode, extractEntityFunction));
            }
        }

        return entities;
    }

    @Override
    public JsonNode getContentTree(String content) {
        try {
            return MAPPER.readTree(content);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format("Exception in initializing jsonTree for source=%s", source), e);
        }
    }

}
