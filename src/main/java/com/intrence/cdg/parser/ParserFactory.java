/**
 * Created by wliu on 11/8/17.
 */
package com.intrence.cdg.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.intrence.cdg.extractor.HtmlExtractor;
import com.intrence.cdg.net.FetchRequest;
import com.intrence.cdg.crawler.CrawlConfig;
import com.intrence.cdg.crawler.IntegrationType;
import com.intrence.cdg.url.ApiSeedUrlProvider;
import com.intrence.cdg.url.CrawlConfigProvider;
import com.intrence.cdg.url.FarfetchSeedUrlProvider;
import com.intrence.cdg.url.SeedUrlProvider;
import com.intrence.cdg.util.Constants;
import org.apache.log4j.Logger;
import org.htmlcleaner.TagNode;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParserFactory {

    private static final Logger LOGGER = Logger.getLogger(ParserFactory.class);

    private static final String CONFIG_PATH = "com/intrence/config/sources/%s.json";
    private static Map<String, List<JsonNode>> parserConfigMap = new HashMap<>();
    private static Map<String, CrawlConfig> crawlConfigMap = new HashMap<>();

    // Factory method to give out Parser instance based on url
    public static <T> Parser createParser(String source, T content, FetchRequest fetchRequest) throws Exception {

        // If DNE in parserConfigMap then load
        if (!parserConfigMap.containsKey(source)) {
            loadConfigsForSource(source);
        }

        JsonNode rules = null;
        for (JsonNode parsingRule : parserConfigMap.get(source)) {
            if (isUrlMatched(parsingRule.at(Constants.ESCAPED_URL_PATTERN), fetchRequest.getWorkRequest())) {
                rules = parsingRule;
                break;
            }
        }

        JsonNode parsingRules = null;

        if (rules == null) {
            //Client might have requested for City/Category Page to extract links from parse.
            rules = parserConfigMap.get(source).get(0);
        } else {
            parsingRules = rules.at(Constants.ESCAPED_RULES);
        }
        String parserClass = rules.at(Constants.ESCAPED_PARSER_CLASS).asText();

        try {
            Class<?> cl = Class.forName(parserClass);
            Constructor<?> cons = cl.getConstructor(String.class, JsonNode.class, FetchRequest.class, content.getClass());

            return (Parser) cons.newInstance(source, parsingRules, fetchRequest, content);
        } catch (Exception e) {
            throw new RuntimeException("Exception=ParserException could not instantiate parser class even though config" +
                    " is available, check parser config for correctness", e);
        }
    }

    public static CrawlConfig getCrawlConfig(String source) {
        if (!crawlConfigMap.containsKey(source)) {
            loadConfigsForSource(source);
        }
        return crawlConfigMap.get(source);
    }

    public static JsonNode getParserConfig(String source, String url){
        if (!parserConfigMap.containsKey(source)) {
            loadConfigsForSource(source);
        }

        for(JsonNode parsingRule : parserConfigMap.get(source)){
            if(isUrlMatched(parsingRule.at(Constants.ESCAPED_URL_PATTERN), url)) {
                return parsingRule;
            }
        }
        return null;
    }

    public static SeedUrlProvider createSoapSeedProvider(CrawlConfig crawlerConfig, String source) {
        return null;
//        if(source.contains(SoapProvidersEnum.AFFILIATE_WINDOW.toString())){
//            return new AffiliateWindowProvider(crawlerConfig);
//        }
//        throw new IllegalArgumentException("Unknown soap-type, could not initialize SeedUrlProvider for the source " + source);
    }

    public static SeedUrlProvider createSeedUrlProvider(String source) {
        CrawlConfig crawlerConfig = getCrawlConfig(source);
        switch (source) {
            case Constants.FARFETCH_ID:
                return new FarfetchSeedUrlProvider(source);
        }
        if (IntegrationType.api.equals(crawlerConfig.getType())) {
            return new ApiSeedUrlProvider(crawlerConfig);
        } else if (IntegrationType.crawl.equals(crawlerConfig.getType()) || IntegrationType.ftp.equals(crawlerConfig.getType())) {
            return new CrawlConfigProvider(crawlerConfig);
        } else if (IntegrationType.soap.equals(crawlerConfig.getType())){
            return createSoapSeedProvider(crawlerConfig, source);
        }
        throw new IllegalArgumentException("Unknown integration-type, could not initialize SeedUrlProvider for the " +
                "source " + source);
    }

    private static boolean isUrlMatched(JsonNode patterns, String url){
        if (patterns == null || url == null) {
            return false;
        }
        if (patterns.isArray()) {
            Iterator<JsonNode> it = patterns.elements();
            while (it.hasNext()) {
                String pattern = it.next().asText();
                if (matchText(pattern, url))
                    return true;
            }
        } else {
            return matchText(patterns.asText(), url);
        }
        return false;
    }

    public static boolean matchText(String pattern, String text) {
        Pattern regexPattern = Pattern.compile(pattern);
        Matcher matcher  = regexPattern.matcher(text);
        return matcher.find();
    }

    // Load configuration from json file
    private static synchronized void loadConfigsForSource(String source) {
        // If DNE in parserConfigMap then load
        if (!parserConfigMap.containsKey(source)) {
            try {
                StringBuilder jsonData = new StringBuilder();
                InputStream i = ParserFactory.class.getClassLoader().getResourceAsStream(String.format(CONFIG_PATH, source));
                BufferedReader r = new BufferedReader(new InputStreamReader(i));

                String line;
                while ((line = r.readLine()) != null) {
                    jsonData.append(line);
                }
                i.close();

                ObjectMapper mapper = (new ObjectMapper()).registerModule(new JodaModule());
                JsonNode config = mapper.readTree(jsonData.toString());
                JsonNode sourceName = config.at(Constants.ESCAPED_SOURCE);
                LOGGER.debug("Config for source" + sourceName.toString() + "is : " + jsonData.toString());
                if (!sourceName.isMissingNode() && sourceName.isTextual() && sourceName.asText() != null) {
                    loadCrawlConfig(config);
                    loadParsingConfig(config);
                } else {
                    String error = String.format("Exception=ConfigFactoryError source name is missing " + "from the config for the source=%s", source);
                    logAndThrowException(error);
                }
            } catch (Exception ex) {
                String error = String.format("Exception=ConfigFactoryError Error loading configs for the source=%s",source);
                logAndThrowException(error, ex);
            }
        }
    }

    protected static void loadCrawlConfig(JsonNode config) throws Exception{
        LOGGER.debug("loading crawl config");
        JsonNode crawlConfigOriginal = config.get(Constants.CRAWLER);
        validateCrawlConfig(config);
        ObjectNode crawlConfig = (ObjectNode) crawlConfigOriginal;
        crawlConfig.put(Constants.SOURCE, config.get(Constants.SOURCE).asText());
        crawlConfigMap.put(config.get(Constants.SOURCE).asText(), CrawlConfig.fromJson(crawlConfig.toString()));
    }

    private static void loadParsingConfig(JsonNode config) throws Exception {
        LOGGER.debug("loading parser config");
        JsonNode crawlConfig = config.at(Constants.ESCAPED_CRAWLER);
        List<JsonNode> nodes;
        if (CrawlConfig.isSoapType(crawlConfig.get(Constants.TYPE).asText())){
            nodes = extractRulesForSoapRequests(config);
        }
        else {
            validateParserConfig(config);
            nodes = extractRulesByUrlPattern(config.at(Constants.PARSER));
        }
        if(nodes.isEmpty()) {
            logAndThrowException(String.format("Exception=ConfigFactoryError  No valid parsing config found for the source=%s",
                    config.get(Constants.SOURCE).asText()));
        }
        parserConfigMap.put(config.get(Constants.SOURCE).asText(), nodes);
    }

    private static List<JsonNode> extractRulesForSoapRequests(final JsonNode config) {
        List<JsonNode> nodes = new ArrayList<>();
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        nodes.add(node.put(Constants.PARSER_CLASS, config.at(Constants.PARSER).get(Constants.PARSER_CLASS).asText()));
        return nodes;
    }

    private static void validateCrawlConfig(JsonNode config) {
        JsonNode crawlConfig = config.at(Constants.ESCAPED_CRAWLER);
        JsonNode pageTypes = crawlConfig.at(Constants.PAGE_TYPES);

        if(crawlConfig.isMissingNode()) {
            logAndThrowException(String.format("Exception=ConfigFactoryError CrawlConfigMissing for source=%s",
                    config.get(Constants.SOURCE).asText()));
        }
        if(crawlConfig.at(Constants.ESCAPED_TYPE).isMissingNode()) {
            logAndThrowException(String.format("Exception=ConfigFactoryError source type information missing " +
                    "for source=%s", config.get(Constants.SOURCE).asText()));
        }

        if (CrawlConfig.isOfflineType(crawlConfig.at(Constants.ESCAPED_TYPE).asText())) {
            return;
        }

        if(crawlConfig.at(Constants.SEED_URLS).isMissingNode() && !crawlConfig.at(Constants.BATCHED_REQUESTS).asBoolean()  ) {
            logAndThrowException(String.format("Exception=ConfigFactoryError SeedUrlsMissing for source=%s",
                    config.get(Constants.SOURCE).asText()));
        }

        for (JsonNode page : pageTypes) {
            if (page.at(Constants.PAGE_TYPE).isMissingNode()) {
                logAndThrowException(String.format("Exception=ConfigFactoryError pageType information is missing from pages" +
                        "for source=%s", config.get(Constants.SOURCE).asText()));
            }
        }
    }

    private static void validateParserConfig(JsonNode config) {
        JsonNode parserConfig = config.at(Constants.PARSER);
        if (parserConfig.isMissingNode())
            logAndThrowException(String.format("Exception=ConfigFactoryError ParsingRulesMissing for source=%s",
                    config.get(Constants.SOURCE).asText()));

        JsonNode parserClass = parserConfig.at(Constants.ESCAPED_PARSER_CLASS);
        if (parserClass.isMissingNode())
            logAndThrowException(String.format("Exception=ConfigFactoryError parserClass for source=%s is missing",
                    config.get(Constants.SOURCE).asText()));

        JsonNode rulesByPattern = parserConfig.at(Constants.RULES_BY_URL_PATTERN);
        if (rulesByPattern.isMissingNode()) {
            rulesByPattern = parserConfig.at(Constants.RULES_BY_OFFLINE);
            if (rulesByPattern.isMissingNode()) {
                logAndThrowException(String.format("Exception=ConfigFactoryError ParsingRulesMissing for source=%s",
                        config.get(Constants.SOURCE).asText()));
            }
        }

        if(!rulesByPattern.isArray())
            logAndThrowException(String.format("Exception=ConfigFactoryError ParsingRules must be in array format for " +
                    "source=%s", config.get(Constants.SOURCE).asText()));

        Iterator<JsonNode> it = rulesByPattern.elements();
        while (it.hasNext()){
            JsonNode rules = it.next();
            JsonNode urlPattern = rules.at(Constants.ESCAPED_URL_PATTERN);
            if(urlPattern.isMissingNode() && parserConfig.at(Constants.RULES_BY_OFFLINE).isMissingNode()) {
                logAndThrowException(String.format("Exception=ConfigFactoryError  UrlPattern of the rules for the " +
                        "source=%s are missing", config.get(Constants.SOURCE).asText()));
            }

            JsonNode parsingRules = rules.at(Constants.ESCAPED_RULES);
            if(parsingRules.isMissingNode()) {
                logAndThrowException(String.format("Exception=ConfigFactoryError rules for the " +
                        "source=%s are missing", config.get(Constants.SOURCE).asText()));
            }

        }
    }
    private static List<JsonNode> extractRulesByUrlPattern(JsonNode config){

        List<JsonNode> nodes = new ArrayList<>();
        JsonNode rulesByPattern = config.at(Constants.RULES_BY_URL_PATTERN);

        if (rulesByPattern.isMissingNode()) {
            rulesByPattern = config.at(Constants.RULES_BY_OFFLINE);
        }

        Iterator<JsonNode> it = rulesByPattern.elements();
        while (it.hasNext()) {
            JsonNode rules = it.next();
            JsonNode urlPattern = rules.at(Constants.ESCAPED_URL_PATTERN);
            JsonNode parsingRules = rules.at(Constants.ESCAPED_RULES);

            ObjectNode node = JsonNodeFactory.instance.objectNode();
            node.put(Constants.PARSER_CLASS, config.get(Constants.PARSER_CLASS).asText());
            node.set(Constants.RULES, parsingRules);
            node.set(Constants.URL_PATTERN, urlPattern);

            if (config.has(Constants.EXTRACTOR_CLASS)) {
                node.put(Constants.EXTRACTOR_CLASS, config.get(Constants.EXTRACTOR_CLASS).asText());
            }
            nodes.add(node);
        }
        return nodes;
    }

    private static void logAndThrowException(String error){
        LOGGER.error(error);
        throw new RuntimeException(error);
    }

    private static void logAndThrowException(String errorMsg, Throwable ex){
        LOGGER.error(errorMsg, ex);
        throw new RuntimeException(errorMsg, ex);
    }
}
